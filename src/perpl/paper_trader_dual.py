"""Dual-leg paper trading: strategy decisions -> Perpl perpetual leg + Kuru spot leg ledger

Decided on 2026-10-08 (Option A):
- Perpl perpetual leg: real testnet execution (this script = simulation layer, verifying decision + ledger linkage)
- Kuru spot leg: ledger simulation (delta-neutral math is real, order logic is real, but no real orders placed)
- Purpose: verify the "strategy signal -> dual-leg linkage -> ledger record" closed loop, for later connection to the real execution layer

Usage:
    python paper_trader_dual.py --date 2026-10-07
    python paper_trader_dual.py                    # default to the latest day
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, ".")
from perpl_config import MARKETS_MAINNET
from strategy_engine import FundingArbStrategy, StrategyParams
from kuru_ledger import KuruSpotLedger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("perpl.paper_dual")


def load_day_data(data_dir: str, day: str):
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


def run_dual(data_dir: str, day: str, params: StrategyParams):
    events = load_day_data(data_dir, day)
    if not events:
        log.error("No data: %s/%s", data_dir, day)
        return

    id2sym = {v: k for k, v in MARKETS_MAINNET.items()}
    strategies = {}
    ledgers = {}          # market -> KuruSpotLedger
    state_cache = {}
    funding_cache = {}

    decisions = []
    ledger_ops = []       # Kuru ledger operation log (for demonstration)

    for ts, kind, mid, data in events:
        sym = id2sym.get(int(mid), f"m{mid}")
        if kind == "state":
            state_cache[mid] = data
        elif kind == "funding":
            funding_cache[mid] = data

        if mid in state_cache and mid in funding_cache:
            s = state_cache[mid]
            f = funding_cache[mid]
            if sym not in strategies:
                strategies[sym] = FundingArbStrategy(params)
                ledgers[sym] = KuruSpotLedger(spot_usd=params.spot_usd)
            st = strategies[sym]
            ledger = ledgers[sym]
            rate = f.get("rate", 0)
            oracle = s.get("orl", 0)
            mark = s.get("mrk", 0)
            rate_frac = rate / 1_000_000 if rate else 0.0
            is_funding = (kind == "funding")

            # Update the Kuru ledger mark price on every frame (unrealized PnL computation)
            if mark:
                ledger.update_price(sym, mark, ts)

            d = st.update_market(sym, rate_frac, oracle, mark, ts, is_funding_event=is_funding)
            if d:
                decisions.append(asdict_safe(d))
                # Decision -> Kuru ledger linkage
                if d.action == "OPEN":
                    entry = ledger.open_spot(sym, d.price, d.ts, notional_usd=params.spot_usd)
                    ledger_ops.append({"ts": d.ts, "market": sym, "action": "OPEN",
                                       "price": d.price, "notional": params.spot_usd,
                                       "amount": entry.amount})
                elif d.action == "CLOSE":
                    entry = ledger.close_spot(sym, d.price, d.ts)
                    if entry:
                        ledger_ops.append({"ts": d.ts, "market": sym, "action": "CLOSE",
                                           "price": d.price, "notional": entry.notional_usd,
                                           "amount": entry.amount})

    # ── Summary ──────────────────────────────
    print(f"\n{'='*70}")
    print(f"Dual-leg paper trading | data: {day} | events: {len(events)} | params: {params.leverage}x {params.collateral_usd}U notional {params.perp_notional}U spot {params.spot_usd}U")
    print(f"{'='*70}")
    for sym, st in strategies.items():
        s = st.summary()
        pos = s.get("position")
        ls = ledgers[sym].snapshot(sym)
        print(f"\n[{sym}] strategy_state={s['state']} decisions={s['decision_count']} | Kuru_ledger={ls['status']} "
              f"position {ls['amount']:.0f} MON realized {ls['realized_pnl']:.2f}U unrealized {ls['unrealized_pnl']:.2f}U")
        if pos:
            print(f"  Perpetual: entry_price={pos['entry_price']} funding_events={pos['funding_events']} "
                  f"cumulative_funding={pos['funding_collected']:.2f}")
        mkt_decisions = [d for d in decisions if d["market"] == sym]
        for d in mkt_decisions[-3:]:  # latest 3
            print(f"  {fmt_time(d['ts'])} [{d['action']}] {d['reason'][:60]}")

    # Kuru ledger overview
    print(f"\n{'─'*70}")
    print("Kuru spot leg ledger overview (simulated):")
    for sym, ledger in ledgers.items():
        summ = ledger.summary()
        print(f"  [{sym}] realized {summ['total_realized_pnl']:.4f}U | unrealized {summ['total_unrealized_pnl']:.4f}U")

    # Save output
    out = Path(data_dir) / f"paper_dual_{day}.json"
    out.write_text(json.dumps({
        "params": params.__dict__,
        "decisions": decisions,
        "kuru_ledger_ops": ledger_ops,
        "kuru_ledger_summary": {sym: ledgers[sym].summary() for sym in ledgers},
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nDual-leg paper trading records saved: {out}")


def asdict_safe(d):
    d = d.__dict__.copy()
    if d.get("pos"):
        d["pos"] = {k: (float(v) if isinstance(v, (int, float)) else v) for k, v in d["pos"].items()}
    d["ts"] = int(d["ts"])
    return d


def fmt_time(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="Dual-leg paper trading: strategy + Kuru ledger")
    parser.add_argument("--date", type=str, default="", help="date YYYY-MM-DD (default latest)")
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--config", type=str, default="config/strategy_test.json")
    args = parser.parse_args()

    if not args.date:
        days = sorted([p.name for p in Path(args.data_dir).iterdir() if p.is_dir()])
        if not days:
            log.error("data directory has no data")
            return
        args.date = days[-1]

    params = StrategyParams.from_file(args.config)
    print(f"Params: leverage={params.leverage}x collateral={params.collateral_usd}U "
          f"notional={params.perp_notional}U spot={params.spot_usd}U")
    run_dual(args.data_dir, args.date, params)


if __name__ == "__main__":
    main()
