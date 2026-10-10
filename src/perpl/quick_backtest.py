"""Replay 10-07 real data on a merged timeline by _recv_ts with the new params (4x + one-way +15%)"""
import json
import logging
import sys

sys.path.insert(0, ".")
from strategy_engine import FundingArbStrategy, StrategyParams

# Params loaded from config file (not hard-coded): test case = 1000U collateral / 4x / notional 4000 / spot 4000
params = StrategyParams.from_file("config/strategy_test.json")
print(f"Params(config): leverage={params.leverage}x collateral={params.collateral_usd} notional={params.perp_notional} "
      f"spot={params.spot_usd} trigger=+{params.deviation_trigger*100:.0f}%")

s = FundingArbStrategy(params)

TARGET = "10"  # MON (mainnet market id 10)
# Read funding frames, record (recv_ts, rate) sequence
funding_events = []  # [(recv_ts, rate)]
for line in open("data/2026-10-07/funding.jsonl", encoding="utf-8"):
    fr = json.loads(line)
    d = fr.get("d") or {}
    if TARGET in d and "rate" in d[TARGET]:
        funding_events.append((fr.get("_recv_ts", 0), d[TARGET]["rate"]))
funding_events.sort()

# Read market_state frames
mstate = []
for line in open("data/2026-10-07/market_state.jsonl", encoding="utf-8"):
    fr = json.loads(line)
    d = fr.get("d") or {}
    if TARGET in d:
        mstate.append(fr)
mstate.sort(key=lambda x: x.get("_recv_ts", 0))

print(f"funding events: {len(funding_events)}, market_state frames: {len(mstate)}")

# Merge timeline: maintain the current funding rate, mark a settlement event when a funding frame arrives
fi = 0
cur_rate = 0.0
last_fund_ts = -1
events = 0
for fr in mstate:
    rts = fr.get("_recv_ts", 0)
    # Process all funding frames that have arrived
    while fi < len(funding_events) and funding_events[fi][0] <= rts:
        cur_rate = funding_events[fi][1] / 1e6   # scaled -> decimal (40 -> 0.00004)
        fi += 1
    st = fr["d"][TARGET]
    mrk = st.get("mrk") or 0
    orl = st.get("orl") or 0
    ts = fr.get("at", {}).get("t", 0)
    if not (mrk and orl):
        continue
    # Funding settlement event determination: cur_rate just updated and >30min since the last event
    is_fund = False
    if fi > 0 and funding_events[fi-1][0] == rts:
        pass  # same-frame arrival does not count
    # Simplified: funding_events timestamps are settlement points, use the at.t of the latest funding to determine
    # Switch to funding frame count determination: mark when fi increases
    # Here funding_events[fi-1] is used as the "currently effective funding", check whether it is a new settlement
    if fi > 0 and funding_events[fi-1][0] != last_fund_ts:
        last_fund_ts = funding_events[fi-1][0]
        is_fund = True
    dec = s.update_market(TARGET, cur_rate, orl, mrk, ts, is_funding_event=is_fund)
    if dec:
        print(f"  [{ts}] {dec.action}: {dec.reason}")
        events += 1

print(f"\nDecision count: {events}, state: {s.state}")
if s.pos:
    print(f"  perp_notional={s.pos.perp_size} spot={s.pos.spot_size} "
          f"cumulative_funding={s.pos.funding_collected:.4f} events={s.pos.funding_events}")
print("✅ Replay complete")
