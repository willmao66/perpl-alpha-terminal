"""Kuru 现货腿账本模拟（Phase 2）—— 测试网无流动性/无测试币，改为记账模式

背景（老铁 2026-10-08 拍板，选项 A）：
- Perpl 永续腿：测试网**真实执行**（有 txid，真实链上活动证据）
- Kuru 现货腿：**账本模拟**（delta 中性计算真实、下单逻辑真实、但不下真实订单）
- 原因：Kuru 测试网 MON-USDC 订单簿空盘 + AMM vault 空（vaultAskOrderSize=0）、
        Kuru 测试 USDC 不可 mint、Circle 水龙头 USDC Kuru 不认 → 无法真实成交
- 参赛叙事：说清楚设计意图 —— 双腿策略完整，Kuru 腿因测试网无流动性以模拟模式演示

职责：
- 维护虚拟现货持仓（delta 中性对冲 = Perpl 永续名义 / mark price = 应买 MON 数量）
- 记账价 = Perpl mark price（同一标的，永续与现货价格一致，无需单独拉 Kuru 价格）
- 记录每笔开/平仓操作（虚拟成交价、数量、时间戳）—— 供演示"信号→记账→持仓"时间线
- 计算现货腿浮动盈亏 + 已实现盈亏（配合永续腿 funding 收益，评估策略完整性）
"""
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger("perpl.kuru_ledger")


@dataclass
class SpotEntry:
    """一次现货开仓记录（虚拟）"""
    market: str
    ts: int                # 时间戳 ms
    price: float           # 虚拟成交价（Perpl mark price）
    notional_usd: float    # 现货名义（对冲金额）
    amount: float          # 应买 MON 数量 = notional / price
    action: str = "OPEN"   # OPEN / CLOSE


@dataclass
class KuruLedgerState:
    """Kuru 现货账本状态"""
    market: str = ""
    status: str = "FLAT"             # FLAT / HEDGED
    cost_basis: float = 0.0          # 持仓成本价（均价）
    amount: float = 0.0              # 虚拟持仓数量（MON）
    notional: float = 0.0            # 持仓名义
    last_price: float = 0.0          # 最新市价（Perpl mark）
    unrealized_pnl: float = 0.0      # 浮动盈亏（USD）
    realized_pnl: float = 0.0        # 已实现盈亏（USD）
    opened_at: int = 0               # 开仓时间戳
    entries: List[dict] = field(default_factory=list)  # 操作流水


class KuruSpotLedger:
    """Kuru 现货腿账本模拟器

    用法：
        ledger = KuruSpotLedger(spot_usd=params.spot_usd)
        ledger.open_spot("MON", mark_price, ts)   # 永续开空时同步开现货多
        ledger.update_price("MON", mark_price, ts) # 每帧市价更新（算浮动盈亏）
        ledger.close_spot("MON", mark_price, ts)   # 永续平仓时同步平现货
        snapshot = ledger.snapshot()               # 账本快照（JSON 输出）
    """

    def __init__(self, spot_usd: float = 0.0, min_record_amount: float = 1e-9):
        self.spot_usd = spot_usd          # 默认现货对冲金额（0 = 不自动配置）
        self.min_record_amount = min_record_amount
        self._states: Dict[str, KuruLedgerState] = {}
        self._ledger_file: Optional[str] = None   # 落盘路径（可选）

    # ── 核心操作 ────────────────────────────
    def open_spot(self, market: str, price: float, ts: int,
                  notional_usd: Optional[float] = None) -> SpotEntry:
        """开现货多（对冲 Perpl 永续空）。price = Perpl mark price（虚拟成交价）"""
        notional = notional_usd if notional_usd is not None else self.spot_usd
        amount = notional / price if price > 0 else 0.0

        st = self._states.get(market)
        if st is None:
            st = KuruLedgerState(market=market)
            self._states[market] = st
        if st.status == "HEDGED":
            log.warning("[%s] 已持仓，忽略重复开仓（如需加仓走 rebalance）", market)
            return SpotEntry(market, ts, price, notional, amount, "SKIP")

        st.status = "HEDGED"
        st.cost_basis = price
        st.amount = amount
        st.notional = notional
        st.last_price = price
        st.opened_at = ts
        entry = SpotEntry(market, ts, price, notional, amount, "OPEN")
        st.entries.append(asdict(entry))
        log.info("[%s] 现货腿开仓(模拟): %.4f MON @ %s (名义 %.2f U, delta 中性对冲)",
                 market, amount, price, notional)
        self._flush()
        return entry

    def close_spot(self, market: str, price: float, ts: int) -> Optional[SpotEntry]:
        """平现货多（配合永续腿平仓）。记录已实现盈亏。"""
        st = self._states.get(market)
        if st is None or st.status != "HEDGED":
            log.warning("[%s] 无现货持仓可平", market)
            return None

        # 已实现盈亏 = (卖出价 - 成本价) × 数量（现货多，卖出平仓）
        realized = (price - st.cost_basis) * st.amount
        st.realized_pnl += realized
        entry = SpotEntry(market, ts, price, st.notional, st.amount, "CLOSE")
        st.entries.append(asdict(entry))
        log.info("[%s] 现货腿平仓(模拟): %.4f MON @ %s (已实现 %.4f U)",
                 market, st.amount, price, realized)

        # 清零（含浮动盈亏——平仓后不再有持仓，浮盈归零，避免与已实现重复计算）
        st.status = "FLAT"
        st.cost_basis = 0.0
        st.amount = 0.0
        st.notional = 0.0
        st.unrealized_pnl = 0.0
        self._flush()
        return entry

    # ── 市价更新（算浮动盈亏）────────────────
    def update_price(self, market: str, price: float, ts: int) -> Optional[dict]:
        """每帧市价更新：更新 last_price + 浮动盈亏（现货多：浮盈 = (价-成本)×量）"""
        st = self._states.get(market)
        if st is None:
            return None
        st.last_price = price
        if st.status == "HEDGED" and st.amount > 0:
            st.unrealized_pnl = (price - st.cost_basis) * st.amount
        return {"market": market, "price": price, "unrealized_pnl": st.unrealized_pnl}

    # ── 快照 / 落盘 ─────────────────────────
    def snapshot(self, market: Optional[str] = None) -> dict:
        """账本快照（全部市场或指定市场）。空状态也返回完整字段（避免调用方 KeyError）"""
        if market:
            st = self._states.get(market)
            if st:
                return asdict(st)
            # 空状态：返回完整字段的默认快照
            return asdict(KuruLedgerState(market=market))
        return {m: asdict(s) for m, s in self._states.items()}

    def set_ledger_file(self, path: str) -> None:
        """设置账本落盘文件（每次操作后自动写）"""
        self._ledger_file = path

    def _flush(self) -> None:
        if self._ledger_file:
            with open(self._ledger_file, "w", encoding="utf-8") as f:
                json.dump(self.snapshot(), f, ensure_ascii=False, indent=1)

    # ── 汇总（配合永续腿评估策略完整性）────────
    def summary(self) -> dict:
        """全账本汇总：各市场状态 + 已实现/浮动盈亏 + 操作流水条数"""
        total_realized = sum(s.realized_pnl for s in self._states.values())
        total_unrealized = sum(s.unrealized_pnl for s in self._states.values())
        return {
            "markets": list(self._states.keys()),
            "total_realized_pnl": round(total_realized, 4),
            "total_unrealized_pnl": round(total_unrealized, 4),
            "total_pnl": round(total_realized + total_unrealized, 4),
            "detail": {m: asdict(s) for m, s in self._states.items()},
        }


# ── 演示用的简单跑批：喂一串 (price, ts) 开/平 ──
def demo():
    """快速演示：模拟一个市场开仓→吃价→平仓，展示账本输出"""
    ledger = KuruSpotLedger(spot_usd=4000.0)
    # 模拟 MON: 开仓(0.02) → 价微涨(0.0202) → 平仓(0.0202)
    ledger.open_spot("MON", 0.02, 1700000000000)
    ledger.update_price("MON", 0.0202, 1700000060000)
    snap1 = ledger.snapshot("MON")
    ledger.close_spot("MON", 0.0202, 1700000120000)
    summ = ledger.summary()
    print(json.dumps({"open_snapshot": snap1, "summary": summ}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    demo()
