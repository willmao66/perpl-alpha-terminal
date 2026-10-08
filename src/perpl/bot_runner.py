"""Alpha Terminal Bot Runner（Phase 2 桥接层）—— 策略决策 → Perpl 真实执行 + Kuru 账本记账

老铁 2026-10-08 拍板（选项 A，Kuru 腿账本模拟）：
- Perpl 永续腿：测试网**真实执行**（有 txid，真实链上活动证据）
- Kuru 现货腿：**账本模拟**（delta 中性计算真实、下单逻辑真实、不下真实订单）

桥接流程（双轨）：
    market-data WS（实时 funding + market-state）
        ↓ 每帧
    策略引擎（FundingArbStrategy，MON 低频持仓 funding arb）
        ↓ 决策 OPEN / CLOSE
    [OPEN]  Perpl open_short（测试网真实下单）→ 等成交确认 → Kuru ledger.open_spot（记账）
    [CLOSE] Perpl close_short（测试网真实平仓）→ 等平仓确认 → Kuru ledger.close_spot（记账）

用法：
    python bot_runner.py                # 测试网真实执行（API key 在 config_local.py，gitignored）
    python bot_runner.py --dry-run      # 纯模拟（不连 trading WS，本地验证桥接逻辑）
    python bot_runner.py --market MON   # 指定市场（默认 MON）
"""
import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

sys.path.insert(0, ".")
from perpl_config import CollectorConfig, MAINNET, TESTNET, MARKETS_MAINNET, MARKETS_TESTNET
from perpl_ws_client import PerplWSClient
from strategy_engine import FundingArbStrategy, StrategyParams
from kuru_ledger import KuruSpotLedger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("perpl.bot")


# ── 市场符号 ↔ ID ─────────────────────────────
def sym_to_mid(sym: str, network: str) -> int:
    src = MARKETS_MAINNET if network == "mainnet" else MARKETS_TESTNET
    if sym not in src:
        raise ValueError(f"未知市场 {sym}（可用: {list(src.keys())}）")
    return src[sym]


def mid_to_sym(mid: int, network: str) -> str:
    src = MARKETS_MAINNET if network == "mainnet" else MARKETS_TESTNET
    for k, v in src.items():
        if v == mid:
            return k
    return f"m{mid}"


class AlphaTerminalBot:
    """桥接层：策略信号 → Perpl 真实执行 + Kuru 账本记账（双轨）"""

    def __init__(self, params: StrategyParams, network: str = "testnet",
                 market: str = "MON", dry_run: bool = False,
                 log_dir: str = "data"):
        self.p = params
        self.network = network
        self.market = market
        self.market_id = sym_to_mid(market, network)
        self.dry_run = dry_run
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.net = TESTNET if network == "testnet" else MAINNET
        self.strategy = FundingArbStrategy(params)
        self.ledger = KuruSpotLedger(spot_usd=params.spot_usd)

        # 市场缩放（REST 拉取）
        self.price_dec: int = 5     # MON 默认
        self.size_dec: int = 0
        self._load_market_config()

        # 数据源 / 执行（lazy）
        self.market_ws: Optional[PerplWSClient] = None
        self.trader = None          # PerplTradingClient（dry_run 时不建）

        # 运行时状态
        self._state: Optional[dict] = None    # 最新 market-state（scaled）
        self._funding: Optional[dict] = None  # 最新 funding（scaled）
        self._head: int = 0                   # trading WS 最新 head block
        self._run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ops_log = []                    # 操作流水（决策+订单+账本）
        self._pending_confirm: Dict[int, asyncio.Event] = {}  # rq -> event
        self._order_state: Dict[int, dict] = {}               # rq -> {expect: open/close, market_id}

    # ── 初始化 ────────────────────────────────
    def _load_market_config(self) -> None:
        """从 REST /api/v1/pub/context 拉市场缩放参数"""
        import requests
        try:
            # rest_base 已含 /api（如 https://testnet.perpl.xyz/api），直接拼 v1
            r = requests.get(f"{self.net['rest_base']}/v1/pub/context", timeout=15)
            d = r.json()
            for m in d.get("markets", []):
                if m.get("id") == self.market_id:
                    cfg = m.get("config", {})
                    self.price_dec = cfg.get("price_decimals", 5)
                    self.size_dec = cfg.get("size_decimals", 0)
                    log.info("市场 %s: price_dec=%s size_dec=%s", self.market,
                             self.price_dec, self.size_dec)
                    return
            log.warning("REST 未找到市场 %s 配置，用默认 price_dec=5 size_dec=0", self.market)
        except Exception as e:
            log.warning("拉市场配置失败（用默认值）: %s", e)

    def _scale_price(self, raw: int) -> float:
        return raw / (10 ** self.price_dec)

    # ── 启动 ──────────────────────────────────
    async def start(self) -> None:
        # 1) 连接 trading WS（真实执行模式）
        if not self.dry_run:
            from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_WS
            from perpl_trader import PerplAuth, PerplTradingClient
            auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
            self.trader = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading",
                                             chain_id=PERPL_CHAIN_ID)
            await self.trader.connect()
            await self.trader._read_until_snapshots(timeout=15)
            self._head = self.trader.wallet.get("at", {}).get("b", 0)
            log.info("trading WS 就绪: 账户=%s head=%s 持仓=%s",
                     self.trader._account_id, self._head, len(self.trader.positions))
            # 恢复已有持仓 → 同步策略状态（崩溃恢复）
            await self._recover_positions()
        else:
            log.info("dry-run 模式：不连 trading WS，Perpl 下单为模拟")

        # 2) 连接 market-data WS
        cfg = CollectorConfig(network=self.network, markets=[self.market],
                              subscribe_orderbook=False, subscribe_trades=False,
                              subscribe_candles=False)
        self.market_ws = PerplWSClient(cfg, self._on_market_frame)
        log.info("market-data WS 将连接: %s", self.market_ws.ws_url)

    async def _recover_positions(self) -> None:
        """重启恢复：若测试网已有持仓，同步策略状态为 HEDGED"""
        for mid, pos in self.trader.positions.items():
            s = pos.get("s", 0)
            if s > 0:
                sym = mid_to_sym(int(mid), self.network)
                ep_scaled = pos.get("ep", 0)
                ep = self._scale_price(ep_scaled)
                # 手动置策略为 HEDGED（用真实持仓信息）
                from strategy_engine import Position, State
                self.strategy.state = State.HEDGED
                self.strategy.pos = Position(
                    market=sym, entry_price=ep, entry_time=int(time.time() * 1000),
                    perp_size=self.p.perp_notional, spot_size=self.p.spot_usd,
                )
                # Kuru 账本同步
                self.ledger.open_spot(sym, ep, int(time.time() * 1000),
                                      notional_usd=self.p.spot_usd)
                log.info("恢复持仓: %s %s MON @ %s（策略置 HEDGED）", sym, s, ep)

    # ── 数据流处理 ────────────────────────────
    async def _on_market_frame(self, frame: dict) -> None:
        """market-data WS 回调：更新缓存 + 喂策略 + 触发执行"""
        mt = frame.get("mt")
        # 9 = MARKET_STATE_UPDATE, 10 = MARKET_FUNDING_UPDATE
        if mt == 9:
            d = frame.get("d") or {}
            if str(self.market_id) in d:
                self._state = d[str(self.market_id)]
        elif mt == 10:
            d = frame.get("d") or {}
            if str(self.market_id) in d:
                self._funding = d[str(self.market_id)]
        else:
            return

        # state + funding 齐了才喂策略
        if self._state is None or self._funding is None:
            return
        await self._process_signal(is_funding=(mt == 10))

    async def _process_signal(self, is_funding: bool) -> None:
        """喂策略引擎，处理决策"""
        s = self._state
        f = self._funding
        mark = self._scale_price(s.get("mrk", 0))
        oracle = self._scale_price(s.get("orl", 0))
        rate_frac = (f.get("rate", 0) or 0) / 1_000_000
        ts = int(time.time() * 1000)

        # Kuru 账本市价更新（浮盈）
        self.ledger.update_price(self.market, mark, ts)

        dec = self.strategy.update_market(self.market, rate_frac, oracle, mark, ts,
                                          is_funding_event=is_funding)
        if dec and dec.action in ("OPEN", "CLOSE"):
            await self._execute(dec)

    # ── 执行 ──────────────────────────────────
    async def _execute(self, dec) -> None:
        """执行决策：Perpl 真实下单（dry-run 模拟）+ Kuru 账本记账"""
        mark = dec.price
        if dec.action == "OPEN":
            # size = 名义 / mark（MON size_dec=0 → 整数 MON）
            size = max(1, round(self.p.perp_notional / mark))
            log.info(">>> [OPEN] %s 永续开空 %s MON @ %s (名义 %sU) 杠杆%sx",
                     self.market, size, mark, self.p.perp_notional, self.p.leverage)
            if not self.dry_run:
                lv = int(self.p.leverage * 100)
                ok = await self._place_and_confirm(2, size, lv, expect="open")
                if not ok:
                    log.error("开仓未确认，跳过 Kuru 记账（保持一致性）")
                    return
            self.ledger.open_spot(self.market, mark, dec.ts, notional_usd=self.p.spot_usd)
            self._ops_log.append({"ts": dec.ts, "action": "OPEN", "market": self.market,
                                  "size": size, "price": mark, "notional": self.p.spot_usd,
                                  "dry_run": self.dry_run})

        elif dec.action == "CLOSE":
            # 平当前真实持仓（从 trader.positions 拿 size）
            size = 0
            if self.trader is not None:
                pos = self.trader.positions.get(self.market_id, {})
                size = pos.get("s", 0)
            if size <= 0:
                log.warning(">>> [CLOSE] 无真实持仓可平，仅 Kuru 账本平仓")
            else:
                log.info(">>> [CLOSE] %s 永续平空 %s MON @ %s", self.market, size, mark)
                if not self.dry_run:
                    lv = int(self.p.leverage * 100)
                    ok = await self._place_and_confirm(4, size, lv, expect="close")
                    if not ok:
                        log.error("平仓未确认，跳过 Kuru 记账")
                        return
            self.ledger.close_spot(self.market, mark, dec.ts)
            self._ops_log.append({"ts": dec.ts, "action": "CLOSE", "market": self.market,
                                  "size": size, "price": mark, "dry_run": self.dry_run})
        self._flush_ops()

    async def _place_and_confirm(self, order_type: int, size: int, lv: int,
                                 expect: str, timeout: float = 30.0) -> bool:
        """下单 + 等确认（持仓更新 或 订单失败帧）

        成功 = 收到 PositionsUpdate 匹配市场
        失败 = 收到 StatusResponse 错误 / OrdersUpdate st=7 (Failed) 携带拒绝原因
        """
        # 用最新 head 做 lb（+20 blocks 缓冲）
        lb = self._head + 20
        await self.trader.place_order(self.market_id, order_type, size,
                                      leverage_hundredths=lv, last_block=lb)
        # 等持仓更新事件
        event = asyncio.Event()
        self._pending_confirm[self.market_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.error("等待持仓确认超时 (%ss)——可能订单簿无对手方或订单滞留", timeout)
            return False
        finally:
            self._pending_confirm.pop(self.market_id, None)
        return True

    async def _on_trading_frame(self, frame: dict) -> None:
        """trading WS 帧：更新 head + 处理订单状态/持仓确认"""
        at = frame.get("at") or {}
        if at.get("b"):
            self._head = at["b"]
        mt = frame.get("mt")
        # 3 = STATUS_RESPONSE：网关层校验结果（code!=0 即拒绝）
        if mt == 3:
            st = frame.get("status") or {}
            code = st.get("code")
            if code != 0:
                err = st.get("error", "unknown")
                log.error("订单被网关拒绝: code=%s error=%s", code, err)
                # 快速失败：触发确认事件（fail 由调用方超时处理）
                # 注意：这里没有 rq 映射，只能唤醒所有等待者
                for mid, ev in list(self._pending_confirm.items()):
                    ev.set()
        # 24 = ORDERS_UPDATE：订单状态（st=7 Failed 带 sr 拒绝原因）
        elif mt == 24:
            d = frame.get("d")
            if isinstance(d, list):
                for o in d:
                    if isinstance(o, dict) and o.get("st") == 7:
                        sr = o.get("sr")
                        log.error("订单失败: sr=%s (reject reason)，mkt=%s size=%s",
                                  sr, o.get("mkt"), o.get("s"))
                        mid = o.get("mkt")
                        if mid is not None and mid in self._pending_confirm:
                            self._pending_confirm[mid].set()
        # 26/27 = POSITIONS_SNAPSHOT/UPDATE：持仓变化确认
        elif mt in (26, 27):
            d = frame.get("d")
            if isinstance(d, list):
                for pos in d:
                    if isinstance(pos, dict) and pos.get("mkt") is not None:
                        mid = int(pos["mkt"])
                        if mid == self.market_id and mid in self._pending_confirm:
                            log.info("持仓更新确认: mkt=%s size=%s st=%s sr=%s",
                                     mid, pos.get("s"), pos.get("st"), pos.get("sr"))
                            self._pending_confirm[mid].set()

    # ── 日志 / 落盘 ───────────────────────────
    def _flush_ops(self) -> None:
        out = self.log_dir / f"bot_ops_{self._run_id}.jsonl"
        with open(out, "a", encoding="utf-8") as f:
            for op in self._ops_log:
                f.write(json.dumps(op, ensure_ascii=False) + "\n")
        self._ops_log = []

    def _save_state(self) -> None:
        out = self.log_dir / f"bot_state_{self._run_id}.json"
        out.write_text(json.dumps({
            "run_id": self._run_id,
            "network": self.network,
            "market": self.market,
            "dry_run": self.dry_run,
            "params": self.p.__dict__,
            "strategy": self.strategy.summary(),
            "kuru_ledger": self.ledger.summary(),
            "head": self._head,
        }, ensure_ascii=False, indent=1), encoding="utf-8")

    async def _state_saver(self, interval: float = 60.0) -> None:
        """定期保存状态（systemd 常驻时也能拿到最新 funding 累积）"""
        while True:
            await asyncio.sleep(interval)
            try:
                self._save_state()
                log.debug("状态已定期保存 (funding_events=%s, collected=%s)",
                          self.strategy.pos.funding_events if self.strategy.pos else 0,
                          self.strategy.pos.funding_collected if self.strategy.pos else 0)
            except Exception as e:
                log.error("定期保存状态失败: %s", e)

    # ── 主循环 ────────────────────────────────
    async def run(self) -> None:
        """并行跑 market-data WS + trading WS 监听"""
        await self.start()
        tasks = []
        tasks.append(asyncio.create_task(self.market_ws.run()))
        if self.trader is not None:
            tasks.append(asyncio.create_task(self._trading_listen()))
        # 定期保存状态（systemd 常驻）
        tasks.append(asyncio.create_task(self._state_saver(interval=60.0)))
        log.info("Bot 运行中（%s, market=%s, dry_run=%s）...", self.network, self.market, self.dry_run)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Bot 停止")
            self._save_state()

    async def _trading_listen(self) -> None:
        """监听 trading WS 帧（复用 PerplTradingClient.run 的循环逻辑）"""
        try:
            async for raw in self.trader._ws:
                frame = json.loads(raw)
                await self._on_trading_frame(frame)
                # 同步持仓到 client.positions（开平仓后真实持仓）
                mt = frame.get("mt")
                if mt in (26, 27):
                    d = frame.get("d")
                    if isinstance(d, list):
                        for pos in d:
                            if isinstance(pos, dict) and pos.get("mkt") is not None:
                                self.trader.positions[int(pos["mkt"])] = pos
        except Exception as e:
            log.error("trading 监听中断: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Alpha Terminal Bot Runner（桥接层）")
    parser.add_argument("--market", type=str, default="MON")
    parser.add_argument("--network", type=str, default="testnet", choices=["testnet", "mainnet"])
    parser.add_argument("--dry-run", action="store_true", help="纯模拟模式（不连 trading WS）")
    parser.add_argument("--config", type=str, default="config/strategy_test.json")
    parser.add_argument("--timeout", type=int, default=0, help="运行秒数（0=无限）")
    args = parser.parse_args()

    params = StrategyParams.from_file(args.config)
    log.info("参数: %sx 保证金=%sU 名义=%sU 现货=%sU",
             params.leverage, params.collateral_usd, params.perp_notional, params.spot_usd)

    bot = AlphaTerminalBot(params, network=args.network, market=args.market,
                           dry_run=args.dry_run)

    async def _main():
        await bot.start()
        # 指定超时
        if args.timeout > 0:
            task = asyncio.create_task(bot.run())
            await asyncio.sleep(args.timeout)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            bot._save_state()
            log.info("运行 %ss 结束，状态已保存", args.timeout)
        else:
            await bot.run()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        log.info("手动中断")
        bot._save_state()


if __name__ == "__main__":
    main()
