"""模拟盘（Paper Trading）：用采集器真实数据回放驱动策略引擎

- 输入：采集器落盘的 funding.jsonl + market_state.jsonl（真实数据）
- 流程：按时间回放 → 喂策略引擎 → 收集决策记录
- 输出：控制台摘要 + JSON 决策记录（供演示/后续接真实执行）

用法：
    python paper_trader.py                    # 用 data/ 最新一天数据
    python paper_trader.py --date 2026-10-07  # 指定日期
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
    """加载某天的 funding + market_state 数据（按时间排序的事件流）"""
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
        log.error("没有数据: %s/%s （先跑采集器）", data_dir, day)
        return

    # 市场 id -> 符号
    id2sym = {v: k for k, v in MARKETS_MAINNET.items()}
    # 每市场一个策略实例（模拟多市场独立决策；真实版按资金分配）
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

        # 该市场 state 和 funding 都齐了才喂策略
        if mid in state_cache and mid in funding_cache:
            s = state_cache[mid]
            f = funding_cache[mid]
            if sym not in strategies:
                strategies[sym] = FundingArbStrategy(params)
            st = strategies[sym]
            rate = f.get("rate", 0)
            oracle = s.get("orl", 0)
            mark = s.get("mrk", 0)
            # rate 单位推断 Micros：除以 1e6 得小数费率
            rate_frac = rate / 1_000_000 if rate else 0.0
            # funding 事件（funding 帧到达）才算结算；state 帧只更新价格
            is_funding = (kind == "funding")
            d = st.update_market(sym, rate_frac, oracle, mark, ts, is_funding_event=is_funding)
            if d:
                decisions.append(asdict_safe(d))

    # ── 汇总 ──────────────────────────────
    print(f"\n{'='*60}")
    print(f"模拟盘结果 | 数据: {day} | 事件数: {len(events)}")
    print(f"{'='*60}")
    for sym, st in strategies.items():
        s = st.summary()
        pos = s.get("position")
        print(f"\n[{sym}] 状态={s['state']} 决策数={s['decision_count']}")
        if pos:
            print(f"  持仓: 入场价={pos['entry_price']} funding事件={pos['funding_events']} "
                  f"累计funding={pos['funding_collected']:.2f}")
        # 该市场的决策
        mkt_decisions = [d for d in decisions if d["market"] == sym]
        for d in mkt_decisions:
            print(f"  {fmt_time(d['ts'])} [{d['action']}] {d['reason'][:60]}")

    # 保存决策记录
    out = Path(data_dir) / f"paper_decisions_{day}.json"
    out.write_text(json.dumps(decisions, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\n决策记录已保存: {out} (共 {len(decisions)} 条)")


def asdict_safe(d):
    d = d.__dict__.copy()
    if d.get("pos"):
        d["pos"] = {k: (float(v) if isinstance(v, (int, float)) else v) for k, v in d["pos"].items()}
    d["ts"] = int(d["ts"])
    return d


def fmt_time(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="模拟盘：真实数据回放")
    parser.add_argument("--date", type=str, default="", help="日期 YYYY-MM-DD（默认取最新）")
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--capital", type=float, default=10000.0)
    args = parser.parse_args()

    # 找最新有数据的日期
    if not args.date:
        days = sorted([p.name for p in Path(args.data_dir).iterdir() if p.is_dir()])
        if not days:
            log.error("data 目录没有数据")
            return
        args.date = days[-1]

    params = StrategyParams(capital_usd=args.capital)
    run_paper(args.data_dir, args.date, params)


if __name__ == "__main__":
    main()
