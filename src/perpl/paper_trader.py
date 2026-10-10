"""Paper trading: replay real collector data to drive the strategy engine

- Input: funding.jsonl + market_state.jsonl persisted by the collector (real data)
- Flow: replay by time -> feed the strategy engine -> collect decision records
- Output: console summary + JSON decision records (for demo / later connection to real execution)

Usage:
    python paper_trader.py                    # use the latest day's data in data/
    python paper_trader.py --date 2026-10-07  # specify a date
"""
import argparse
import asyncio
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, ".")
from perpl_config import CollectorConfig, MT, MAINNET, MARKETS_MAINNET
from strategy_engine import FundingArbStrategy, StrategyParams, State

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("perpl.paper")


def load_day_data(data_dir: str, day: str, markets: dict):
    """Load one day's funding + market_state data (event stream sorted by time)"""
    day_dir = Path(data_dir) / day
    events = []
    if (day_dir / "funding.jsonl").exists():
        for line in (day_dir / "funding.jsonl").read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = rec.get("_recv_ts", 0)
            for mid, f in (rec.get("d") or {}).items():
                events.append((ts, "funding", mid, f))
    if (day_dir / "market_state.jsonl").exists():
        for line in (day_dir / "market_state.jsonl").read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = rec.get("_recv_ts", 0)
            for mid, s in (rec.get("d") or {}).items():
                events.append((ts, "state", mid, s))
    events.sort(key=lambda x: x[0])
    return events


def run_paper(data_dir: str, day: str, params: StrategyParams):
    events = load_day_data(data_dir, day, MARKETS_MAINNET)
    if not events:
        log.error("No data: %s/%s (run the collector first)", data_dir, day)
        return

    # market id -> symbol
    id2sym = {v: k for k, v in MARKETS_MAINNET.items()}
    # One strategy instance per market (simulate independent multi-market decisions; real version allocates by capital)
    strategies = {}
    state_cache = {}    # mid -> latest state
    funding_cache = {}  # mid -> latest funding

    decisions = []
    ts0 = events[0][0]

    for ts, kind, mid, data in events:
        sym = id2sym.get(int(mid), f"m{mid}")
        if kind == "state":
            state_cache[mid] = data
        elif kind == "funding":
            funding_cache[mid] = data

        # Only feed the strategy once both state and funding are ready for that market
        if mid in state_cache and mid in funding_cache:
            s = state_cache[mid]
            f = funding_cache[mid]
            if sym not in strategies:
                strategies[sym] = FundingArbStrategy(params)
            st = strategies[sym]
            rate = f.get("rate", 0)
            oracle = s.get("orl", 0)
            mark = s.get("mrk", 0)
            # rate unit inferred as Micros: divide by 1e6 to get decimal rate
            rate_frac = rate / 1_000_000 if rate else 0.0
            # Only funding events (funding frame arrival) count as settlement; state frames only update price
            is_funding = (kind == "funding")
            d = st.update_market(sym, rate_frac, oracle, mark, ts, is_funding_event=is_funding)
            if d:
                decisions.append(asdict_safe(d))

    # ── Summary ──────────────────────────────
    print(f"\n{'='*60}")
    print(f"Paper trading results | data: {day} | events: {len(events)}")
    print(f"{'='*60}")
    for sym, st in strategies.items():
        s = st.summary()
        pos = s.get("position")
        print(f"\n[{sym}] state={s['state']} decisions={s['decision_count']}")
        if pos:
            print(f"  Position: entry_price={pos['entry_price']} funding_events={pos['funding_events']} "
                  f"cumulative_funding={pos['funding_collected']:.2f}")
        # This market's decisions
        mkt_decisions = [d for d in decisions if d["market"] == sym]
        for d in mkt_decisions:
            print(f"  {fmt_time(d['ts'])} [{d['action']}] {d['reason'][:60]}")

    # Save decision records
    out = Path(data_dir) / f"paper_decisions_{day}.json"
    out.write_text(json.dumps(decisions, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nDecision records saved: {out} ({len(decisions)} total)")


def asdict_safe(d):
    d = d.__dict__.copy()
    if d.get("pos"):
        d["pos"] = {k: (float(v) if isinstance(v, (int, float)) else v) for k, v in d["pos"].items()}
    d["ts"] = int(d["ts"])
    return d


def fmt_time(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="Paper trading: real data replay")
    parser.add_argument("--date", type=str, default="", help="date YYYY-MM-DD (default latest)")
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--capital", type=float, default=10000.0)
    args = parser.parse_args()

    # Find the most recent day with data
    if not args.date:
        days = sorted([p.name for p in Path(args.data_dir).iterdir() if p.is_dir()])
        if not days:
            log.error("data directory has no data")
            return
        args.date = days[-1]

    params = StrategyParams(capital_usd=args.capital)
    run_paper(args.data_dir, args.date, params)


if __name__ == "__main__":
    main()
