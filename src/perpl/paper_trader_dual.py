"""双腿模拟盘（Dual-leg Paper Trading）：策略决策 → Perpl 永续腿 + Kuru 现货腿账本

老铁 2026-10-08 拍板（选项 A）：
- Perpl 永续腿：测试网真实执行（本脚本 = 模拟层，验证决策与账本联动）
- Kuru 现货腿：账本模拟（delta 中性计算真实、下单逻辑真实、不下真实订单）
- 目的：验证"策略信号 → 双腿联动 → 账本记录"闭环，供后续接真实执行层

用法：
    python paper_trader_dual.py --date 2026-10-07
    python paper_trader_dual.py                    # 默认最新一天
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
    """加载某天 funding + market_state 数据（按时间排序的事件流）"""
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
        log.error("没有数据: %s/%s", data_dir, day)
        return

    id2sym = {v: k for k, v in MARKETS_MAINNET.items()}
    strategies = {}
    ledgers = {}          # market -> KuruSpotLedger
    state_cache = {}
    funding_cache = {}

    decisions = []
    ledger_ops = []       # Kuru 账本操作流水（供演示）

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

            # 每帧先更新 Kuru 账本市价（浮盈计算）
            if mark:
                ledger.update_price(sym, mark, ts)

            d = st.update_market(sym, rate_frac, oracle, mark, ts, is_funding_event=is_funding)
            if d:
                decisions.append(asdict_safe(d))
                # 决策 → Kuru 账本联动
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

    # ── 汇总 ──────────────────────────────
    print(f"\n{'='*70}")
    print(f"双腿模拟盘 | 数据: {day} | 事件数: {len(events)} | 参数: {params.leverage}x {params.collateral_usd}U 名义{params.perp_notional}U 现货{params.spot_usd}U")
    print(f"{'='*70}")
    for sym, st in strategies.items():
        s = st.summary()
        pos = s.get("position")
        ls = ledgers[sym].snapshot(sym)
        print(f"\n[{sym}] 策略状态={s['state']} 决策数={s['decision_count']} | Kuru账本={ls['status']} "
              f"持仓{ls['amount']:.0f} MON 已实现{ls['realized_pnl']:.2f}U 浮盈{ls['unrealized_pnl']:.2f}U")
        if pos:
            print(f"  永续: 入场价={pos['entry_price']} funding事件={pos['funding_events']} "
                  f"累计funding={pos['funding_collected']:.2f}")
        mkt_decisions = [d for d in decisions if d["market"] == sym]
        for d in mkt_decisions[-3:]:  # 最近 3 条
            print(f"  {fmt_time(d['ts'])} [{d['action']}] {d['reason'][:60]}")

    # Kuru 账本总览
    print(f"\n{'─'*70}")
    print("Kuru 现货腿账本总览（模拟）:")
    for sym, ledger in ledgers.items():
        summ = ledger.summary()
        print(f"  [{sym}] 已实现 {summ['total_realized_pnl']:.4f}U | 浮动 {summ['total_unrealized_pnl']:.4f}U")

    # 保存输出
    out = Path(data_dir) / f"paper_dual_{day}.json"
    out.write_text(json.dumps({
        "params": params.__dict__,
        "decisions": decisions,
        "kuru_ledger_ops": ledger_ops,
        "kuru_ledger_summary": {sym: ledgers[sym].summary() for sym in ledgers},
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\n双腿模拟盘记录已保存: {out}")


def asdict_safe(d):
    d = d.__dict__.copy()
    if d.get("pos"):
        d["pos"] = {k: (float(v) if isinstance(v, (int, float)) else v) for k, v in d["pos"].items()}
    d["ts"] = int(d["ts"])
    return d


def fmt_time(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="双腿模拟盘：策略 + Kuru 账本")
    parser.add_argument("--date", type=str, default="", help="日期 YYYY-MM-DD（默认最新）")
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--config", type=str, default="config/strategy_test.json")
    args = parser.parse_args()

    if not args.date:
        days = sorted([p.name for p in Path(args.data_dir).iterdir() if p.is_dir()])
        if not days:
            log.error("data 目录没有数据")
            return
        args.date = days[-1]

    params = StrategyParams.from_file(args.config)
    print(f"参数: 杠杆={params.leverage}x 保证金={params.collateral_usd}U "
          f"名义={params.perp_notional}U 现货={params.spot_usd}U")
    run_dual(args.data_dir, args.date, params)


if __name__ == "__main__":
    main()
