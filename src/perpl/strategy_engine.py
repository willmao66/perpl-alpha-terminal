"""Perpl Alpha Terminal - 策略引擎（Phase 2 核心）

低频持仓型 funding arb（老铁 2026-10-07/10-08 拍板）：
- 方向：funding > 0 深升水时 Perpl 永续空 + Kuru 现货多（单向，delta 中性）
- 持仓：跨多个 43min 结算周期吃费率（低频，不做高频收割）
- 资金：10000U = 2000U 永续保证金(4x, 8000U 名义) + 8000U 现货对冲（10-08 拍板）
- 风控：单向 +15%（仅做空亏损方向=价格上涨 +15% → 双腿平仓重开；下跌盈利方向不设阈值）
- 附加：费率转负/跌破阈值 → 平仓走人（不头铁死拿）

数据输入：采集器实时 funding + market state（或模拟回放）
决策输出：JSON 决策记录（供模拟盘/执行层消费）
"""
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger("perpl.strategy")


# ── 参数配置（可调）──────────────────────────────
@dataclass
class StrategyParams:
    # 信号参数
    funding_threshold: float = 0.00002      # rate > 2bp/次 才考虑开仓（待单位验证后校准）
    premium_min_bps: float = 0.5            # 实时溢价 ≥ 0.5bp 确认（防 rate 与溢价背离）
    # 风控参数（老铁 2026-10-08 拍板：单向 +15%，仅做空亏损方向=价格上涨触发）
    leverage: float = 4.0                   # 4x 杠杆
    deviation_trigger: float = 0.15         # 仅做空方向（价格上涨）+15% → 双腿平仓重开
    funding_exit_threshold: float = 0.0     # 持仓中费率跌破此值 → 平仓（转负/无利可图就走）
    # 资金参数（10000U 口径，老铁 10-08 拍板）
    collateral_usd: float = 2000.0          # 永续保证金 2000U
    spot_usd: float = 8000.0                # 现货对冲 8000U
    # 数据源
    data_dir: str = "data"                  # 采集器落盘目录（模拟回放用）


# ── 状态机 ──────────────────────────────────────
class State:
    FLAT = "FLAT"              # 空仓，等机会
    HEDGED = "HEDGED"          # 持仓中（Perpl 空 + Kuru 多）
    TRIGGERED = "TRIGGERED"    # 偏离超限，等待平仓确认
    REBALANCING = "REBALANCING"  # 平仓重开中


@dataclass
class Position:
    market: str = ""
    entry_price: float = 0.0       # 入场价（oracle/mark）
    entry_time: int = 0            # 入场时间戳 ms
    perp_size: float = 0.0         # Perpl 永续空头名义
    spot_size: float = 0.0         # Kuru 现货多头名义
    funding_collected: float = 0.0 # 累计吃到的 funding（模拟）
    last_funding_rate: float = 0.0
    funding_events: int = 0        # 吃到的结算次数


@dataclass
class Decision:
    """策略决策输出（模拟盘/执行层消费）"""
    ts: int
    action: str                     # OPEN / CLOSE / HOLD / NOOP
    market: str
    reason: str
    price: float
    funding_rate: float
    premium_bps: float
    pos: Optional[dict] = None      # 当前持仓快照
    params_snapshot: Optional[dict] = None


class FundingArbStrategy:
    """funding arb 策略引擎（低频持仓型）"""

    def __init__(self, params: Optional[StrategyParams] = None):
        self.p = params or StrategyParams()
        self.state = State.FLAT
        self.pos: Optional[Position] = None
        self.decisions: List[Decision] = []
        self._last_signals: Dict[str, dict] = {}   # market -> 最新信号

    # ── 信号更新（由数据源驱动）──────────────────
    def update_market(self, market: str, funding_rate: float,
                      oracle_price: float, mark_price: float, ts: int,
                      is_funding_event: bool = False) -> Optional[Decision]:
        """每收到一帧市场数据调用。is_funding_event=True 表示这是 funding 结算事件。
        返回决策（可能为 None = 无动作）"""
        premium_bps = (mark_price - oracle_price) / oracle_price * 10000 if oracle_price else 0
        sig = {
            "ts": ts, "funding_rate": funding_rate, "oracle": oracle_price,
            "mark": mark_price, "premium_bps": premium_bps,
        }
        self._last_signals[market] = sig

        # 状态机流转
        if self.state == State.FLAT:
            return self._eval_open(market, sig)
        elif self.state == State.HEDGED:
            return self._eval_hold(market, sig, is_funding_event)
        elif self.state == State.TRIGGERED:
            return self._eval_reopen(market, sig)
        return None

    # ── 开仓评估（FLAT → HEDGED）────────────────
    def _eval_open(self, market: str, sig: dict) -> Optional[Decision]:
        fr = sig["funding_rate"]
        if fr < self.p.funding_threshold:
            return None
        # 溢价确认：rate 高但已贴水 → 费率将回落，不追（背离保护）
        if sig["premium_bps"] < self.p.premium_min_bps:
            return None
        # 开仓
        self.state = State.HEDGED
        self.pos = Position(
            market=market,
            entry_price=sig["mark"],
            entry_time=sig["ts"],
            perp_size=self.p.collateral_usd * self.p.leverage,  # 2000×4 = 8000 名义
            spot_size=self.p.spot_usd,                          # 8000 现货对冲
            last_funding_rate=fr,
        )
        d = Decision(sig["ts"], "OPEN", market, f"funding={fr:.6f} premium={sig['premium_bps']:.2f}bp 深升水开仓",
                     sig["mark"], fr, sig["premium_bps"], asdict(self.pos))
        self.decisions.append(d)
        return d

    # ── 持仓监控（HEDGED）───────────────────────
    def _eval_hold(self, market: str, sig: dict, is_funding_event: bool = False) -> Optional[Decision]:
        assert self.pos is not None
        # 1) funding 累计：仅在 funding 结算事件时累计（不是每帧 market_state）
        if is_funding_event:
            self.pos.funding_collected += self.pos.perp_size * sig["funding_rate"]
            self.pos.funding_events += 1
            self.pos.last_funding_rate = sig["funding_rate"]

        # 1.5) 费率转负平仓：持仓中 funding 跌破阈值 → 平仓走人（不头铁死拿）
        if is_funding_event and sig["funding_rate"] < self.p.funding_exit_threshold:
            self.state = State.FLAT
            d = Decision(sig["ts"], "CLOSE", market,
                         f"费率转负 ({sig['funding_rate']:.6f})，平仓走人（累计funding={self.pos.funding_collected:.2f}）",
                         sig["mark"], sig["funding_rate"], sig["premium_bps"], asdict(self.pos))
            self.pos = None
            self.decisions.append(d)
            return d

        # 2) 偏离检测（单向 +15%）：仅做空亏损方向 = 价格上涨。下跌是盈利方向不设阈值
        #    做空 MON：价格上涨 → 永续腿亏损，+15% 即双腿平仓重开（校准新起点）
        dev_up = (sig["mark"] - self.pos.entry_price) / self.pos.entry_price
        if dev_up >= self.p.deviation_trigger:
            self.state = State.TRIGGERED
            d = Decision(sig["ts"], "CLOSE", market,
                         f"价格上涨 {dev_up*100:.1f}% ≥ +15%（做空亏损方向），双腿平仓重开（校准新起点）",
                         sig["mark"], sig["funding_rate"], sig["premium_bps"], asdict(self.pos))
            self.decisions.append(d)
            return d
        return None

    # ── 平仓后重开评估（TRIGGERED → HEDGED/FLAT）─
    def _eval_reopen(self, market: str, sig: dict) -> Optional[Decision]:
        """平仓后立即重开（MVP）：回到新入场价原点。v2：若 funding 已回落则不重开"""
        fr = sig["funding_rate"]
        if fr < self.p.funding_threshold or sig["premium_bps"] < self.p.premium_min_bps:
            # 机会已消失 → 回空仓
            self.state = State.FLAT
            self.pos = None
            return Decision(sig["ts"], "NOOP", market,
                            "触发平仓后机会消失（funding 回落），回空仓等待",
                            sig["mark"], fr, sig["premium_bps"], None)
        # 重开
        self.state = State.HEDGED
        self.pos = Position(
            market=market, entry_price=sig["mark"], entry_time=sig["ts"],
            perp_size=self.p.collateral_usd * self.p.leverage,  # 8000 名义
            spot_size=self.p.spot_usd,                          # 8000 现货对冲
            last_funding_rate=fr,
        )
        d = Decision(sig["ts"], "OPEN", market,
                     f"偏离后重开（新基准 {sig['mark']}）", sig["mark"], fr,
                     sig["premium_bps"], asdict(self.pos))
        self.decisions.append(d)
        return d

    # ── 状态查询 ────────────────────────────────
    def summary(self) -> dict:
        return {
            "state": self.state,
            "position": asdict(self.pos) if self.pos else None,
            "decision_count": len(self.decisions),
            "last_signal": self._last_signals,
        }
