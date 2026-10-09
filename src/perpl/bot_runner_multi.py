"""Alpha Terminal Bot Runner（多市场轮动版，Phase 2.5）—— 全自动 funding arb

老铁 2026-10-09 拍板的风控要求（全自动化机器人必须）：
1. 空单亏损方向（价格上涨）+15% → 双腿平仓重开（校准新起点）
2. funding 转负或 < 5 micro → 立即平仓，扫描全市场寻找可做标的（费率低就撤，找更肥的）

核心升级 vs bot_runner.py（单市场）：
- 订阅全部市场（market-state/funding 流本来就是全市场广播）
- FLAT 时：扫描所有市场，选 funding 最高 + premium 达标的开仓
- HEDGED 时：只监控持仓市场（费率低/转负/15%偏离 → 平仓）→ 回 FLAT 自动重扫换标的

用法：
    python bot_runner_multi.py                    # 测试网真实执行（API key 在 config_local.py）
    python bot_runner_multi.py --dry-run          # 纯模拟
    python bot_runner_multi.py --config config/vps_eth.json
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
from strategy_engine import FundingArbStrategy, StrategyParams, State
from kuru_ledger import KuruSpotLedger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("perpl.bot.multi")


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


# 候选市场：已命名市场 + 已知有流动性的数字市场（测试网）
CANDIDATE_MARKETS = ["BTC", "ETH", "SOL", "MON", "ZEC"]


class AlphaTerminalBotMulti:
    """多市场轮动：FLAT 扫描全市场选最优，HEDGED 监控持仓市场"""

    def __init__(self, params: StrategyParams, network: str = "testnet",
                 dry_run: bool = False, log_dir: str = "data"):
        self.p = params
        self.network = network
        self.dry_run = dry_run
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.net = TESTNET if network == "testnet" else MAINNET
        self.market_src = MARKETS_TESTNET if network == "testnet" else MARKETS_MAINNET
        self.strategy = FundingArbStrategy(params)
        self.ledger = KuruSpotLedger(spot_usd=params.spot_usd)

        # 候选市场 ID（已命名 + 数字市场探测）
        self.candidate_ids = [sym_to_mid(s, network) for s in CANDIDATE_MARKETS]
        self.candidate_ids += [m for m in self._probe_numeric_markets() if m not in self.candidate_ids]

        # 市场缩放（REST 拉取，多市场字典）
        self.price_dec: Dict[int, int] = {}
        self.size_dec: Dict[int, int] = {}
        self._load_market_configs()

        # 数据源 / 执行（lazy）
        self.market_ws: Optional[PerplWSClient] = None
        self.trader = None

        # 运行时状态（多市场缓存）
        self._states: Dict[str, dict] = {}       # mid -> state（scaled）
        self._fundings: Dict[str, dict] = {}     # mid -> funding（scaled）
        self._last_funding_blocks: Dict[str, int] = {}  # mid -> at.b
        self._head: int = 0
        self._run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ops_log = []
        self._pending_confirm: Dict[int, asyncio.Event] = {}
        self._order_state: Dict[int, dict] = {}

    def _probe_numeric_markets(self) -> list:
        """探测测试网数字市场（从 REST context 拉市场列表，选 funding 可能为正的）"""
        import requests
        try:
            r = requests.get(f"{self.net['rest_base']}/v1/pub/context", timeout=15)
            d = r.json()
            mids = [m.get("id") for m in d.get("markets", [])]
            return [m for m in mids if m not in self.market_src.values()]
        except Exception as e:
            log.warning("探测数字市场失败: %s", e)
            return []

    def _load_market_configs(self) -> None:
        """从 REST context 拉所有候选市场缩放参数"""
        import requests
        try:
            r = requests.get(f"{self.net['rest_base']}/v1/pub/context", timeout=15)
            d = r.json()
            for m in d.get("markets", []):
                mid = m.get("id")
                if mid in self.candidate_ids:
                    cfg = m.get("config", {})
                    self.price_dec[mid] = cfg.get("price_decimals", 5)
                    self.size_dec[mid] = cfg.get("size_decimals", 0)
            log.info("市场配置加载: %d 个候选市场", len(self.price_dec))
        except Exception as e:
            log.warning("拉市场配置失败: %s", e)

    def _scale_price(self, mid: int, raw: int) -> float:
        return raw / (10 ** self.price_dec.get(mid, 5))

    # ── 启动 ──────────────────────────────────
    async def start(self) -> None:
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
            await self._recover_positions()
        else:
            log.info("dry-run 模式：不连 trading WS")

        # 订阅全部候选市场（market-state/funding 流全市场广播，无需逐个订阅）
        cfg = CollectorConfig(network=self.network, markets=CANDIDATE_MARKETS,
                              subscribe_orderbook=False, subscribe_trades=False,
                              subscribe_candles=False)
        self.market_ws = PerplWSClient(cfg, self._on_market_frame)
        log.info("market-data WS 将连接: %s", self.market_ws.ws_url)

    async def _recover_positions(self) -> None:
        """重启恢复：读链上真实持仓，同步策略为 HEDGED"""
        for mid, pos in self.trader.positions.items():
            s = pos.get("s", 0)
            if s > 0:
                sym = mid_to_sym(int(mid), self.network)
                ep = self._scale_price(int(mid), pos.get("ep", 0))
                from strategy_engine import Position
                self.strategy.state = State.HEDGED
                self.strategy.pos = Position(
                    market=sym, entry_price=ep, entry_time=int(time.time() * 1000),
                    perp_size=self.p.perp_notional, spot_size=self.p.spot_usd,
                )
                self.ledger.open_spot(sym, ep, int(time.time() * 1000),
                                      notional_usd=self.p.spot_usd)
                log.info("恢复持仓: %s (%s) @ %s（策略置 HEDGED）", sym, mid, ep)

    # ── 数据流处理（全市场）──────────────────
    async def _on_market_frame(self, frame: dict) -> None:
        mt = frame.get("mt")
        if mt == 9:  # MARKET_STATE_UPDATE（全市场广播）
            d = frame.get("d") or {}
            for mid, st in d.items():
                self._states[mid] = st
            # 每帧都尝试决策（FLAT 扫描 / HEDGED 监控），state 更新是最高频信号
            await self._process_signal_all(is_funding=False)
        elif mt == 10:  # MARKET_FUNDING_UPDATE（全市场广播）
            d = frame.get("d") or {}
            settled = []
            for mid, f in d.items():
                self._fundings[mid] = f
                # 结算检测（每市场独立 at.b）
                fb = (f.get("at") or {}).get("b")
                is_settle = False
                if fb is not None:
                    if mid in self._last_funding_blocks:
                        is_settle = (fb != self._last_funding_blocks[mid])
                    self._last_funding_blocks[mid] = fb
                if is_settle:
                    settled.append(mid)
            if settled:
                await self._process_signal_all(is_funding=True, settled_markets=settled)
        # 其他帧忽略

    async def _process_signal_all(self, is_funding: bool, settled_markets: Optional[list] = None) -> None:
        """多市场决策入口：
        - HEDGED：只监控持仓市场（结算事件才累计/平仓判断）
        - FLAT：扫描全市场选最优（每帧都尝试）
        """
        if self.strategy.state == State.HEDGED:
            pos = self.strategy.pos
            if not pos:
                return
            pos_mid = self._market_to_mid(pos.market)
            # 持仓市场的结算事件才处理（费率累计 + 平仓判断）
            if is_funding and settled_markets:
                if str(pos_mid) in settled_markets:
                    await self._process_signal(str(pos_mid), is_funding=True)
            elif not is_funding:
                # 非结算帧也监控价格偏离（15% 风控）
                if str(pos_mid) in self._states:
                    await self._process_signal(str(pos_mid), is_funding=False)
        else:
            # FLAT：每帧扫描选最优
            await self._scan_and_open()

    # ── 决策（多市场）─────────────────────────
    async def _process_signal(self, mid: str, is_funding: bool) -> None:
        """多市场决策：
        - HEDGED：只监控持仓市场（费率低/转负/15%偏离 → 平仓）
        - FLAT：扫描全市场选最优 → 开仓
        """
        s = self._states.get(mid)
        f = self._fundings.get(mid)
        if not s or not f:
            return
        mid_i = int(mid)
        mark = self._scale_price(mid_i, s.get("mrk", 0))
        oracle = self._scale_price(mid_i, s.get("orl", 0))
        rate_frac = (f.get("rate", 0) or 0) / 1_000_000
        ts = int(time.time() * 1000)

        # 更新账本市价（持仓市场的浮盈）
        if self.strategy.pos and self.strategy.pos.market == mid_to_sym(mid_i, self.network):
            self.ledger.update_price(mid_to_sym(mid_i, self.network), mark, ts)

        if self.strategy.state == State.HEDGED:
            # 只监控持仓市场
            pos_mid = self._market_to_mid(self.strategy.pos.market)
            if pos_mid != mid_i:
                return
            dec = self.strategy.update_market(mid_to_sym(mid_i, self.network), rate_frac,
                                              oracle, mark, ts, is_funding_event=is_funding)
            if dec and dec.action == "CLOSE":
                log.info("决策: %s | reason: %s", dec.action, dec.reason)
                await self._execute_close(dec, mid_i)
        else:
            # FLAT：扫描全市场选最优
            await self._scan_and_open()

    def _market_to_mid(self, sym: str) -> int:
        if sym in self.market_src:
            return self.market_src[sym]
        if sym.startswith("m"):
            return int(sym[1:])
        return -1

    async def _scan_and_open(self) -> None:
        """FLAT：扫描所有缓存市场，选 funding 最高 + premium 达标 → 开仓"""
        best_mid = None
        best_score = -1
        best_info = None
        for mid, s in self._states.items():
            f = self._fundings.get(mid)
            if not f:
                continue
            mid_i = int(mid)
            mark = self._scale_price(mid_i, s.get("mrk", 0))
            oracle = self._scale_price(mid_i, s.get("orl", 0))
            rate_frac = (f.get("rate", 0) or 0) / 1_000_000
            premium_bps = (mark - oracle) / oracle * 10000 if oracle else 0
            # 开仓条件：funding ≥ 阈值 + premium ≥ 最小
            if rate_frac < self.p.funding_threshold:
                continue
            if premium_bps < self.p.premium_min_bps:
                continue
            # 评分：funding 为主，premium 辅助
            score = rate_frac * 10000 + premium_bps / 100
            if score > best_score:
                best_score = score
                best_mid = mid
                best_info = (rate_frac, premium_bps, mark, oracle)

        if best_mid is None:
            return  # 无达标市场，继续等
        sym = mid_to_sym(int(best_mid), self.network)
        rate_frac, premium_bps, mark, oracle = best_info
        log.info(">>> [SCAN] 选中 %s (funding=%s premium=%.2fbp mark=%.5f)", sym, rate_frac, premium_bps, mark)
        # 喂策略开仓
        dec = self.strategy.update_market(sym, rate_frac, oracle, mark,
                                          int(time.time() * 1000), is_funding_event=False)
        if dec and dec.action == "OPEN":
            log.info("决策: %s | reason: %s", dec.action, dec.reason)
            await self._execute_open(dec, int(best_mid))

    # ── 执行 ──────────────────────────────────
    async def _execute_open(self, dec, mid_i: int) -> None:
        mark = dec.price
        size_dec = self.size_dec.get(mid_i, 0)
        size = max(1, round(self.p.perp_notional / mark * (10 ** size_dec)))
        sym = dec.market
        log.info(">>> [OPEN] %s 永续开空 %s @ %s (名义 %sU) 杠杆%sx | %s",
                 sym, size, mark, self.p.perp_notional, self.p.leverage, dec.reason)
        if not self.dry_run:
            lv = int(self.p.leverage * 100)
            ok = await self._place_and_confirm(mid_i, 2, size, lv, expect="open")
            if not ok:
                log.error("开仓未确认，跳过 Kuru 记账（保持一致性）")
                return
        self.ledger.open_spot(sym, mark, dec.ts, notional_usd=self.p.spot_usd)
        self._ops_log.append({"ts": dec.ts, "action": "OPEN", "market": sym,
                              "size": size, "price": mark, "notional": self.p.spot_usd,
                              "reason": dec.reason, "dry_run": self.dry_run})
        self._flush_ops()

    async def _execute_close(self, dec, mid_i: int) -> None:
        mark = dec.price
        sym = dec.market
        size = 0
        if self.trader is not None:
            pos = self.trader.positions.get(mid_i, {})
            size = pos.get("s", 0)
        if size <= 0:
            log.warning(">>> [CLOSE] %s 无真实持仓可平，仅 Kuru 账本平仓 | %s", sym, dec.reason)
        else:
            log.info(">>> [CLOSE] %s 永续平空 %s @ %s | %s", sym, size, mark, dec.reason)
            if not self.dry_run:
                lv = int(self.p.leverage * 100)
                ok = await self._place_and_confirm(mid_i, 4, size, lv, expect="close")
                if not ok:
                    log.error("平仓未确认，跳过 Kuru 记账")
                    return
        self.ledger.close_spot(sym, mark, dec.ts)
        self._ops_log.append({"ts": dec.ts, "action": "CLOSE", "market": sym,
                              "size": size, "price": mark, "reason": dec.reason,
                              "dry_run": self.dry_run})
        self._flush_ops()

    async def _place_and_confirm(self, market_id: int, order_type: int, size: int, lv: int,
                                 expect: str, timeout: float = 30.0) -> bool:
        lb = self._head + 20
        await self.trader.place_order(market_id, order_type, size,
                                      leverage_hundredths=lv, last_block=lb)
        event = asyncio.Event()
        self._pending_confirm[market_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.error("等待持仓确认超时 (%ss)——可能订单簿无对手方或订单滞留", timeout)
            return False
        finally:
            self._pending_confirm.pop(market_id, None)
        return True

    async def _on_trading_frame(self, frame: dict) -> None:
        at = frame.get("at") or {}
        if at.get("b"):
            self._head = at["b"]
        mt = frame.get("mt")
        if mt == 3:
            st = frame.get("status") or {}
            code = st.get("code")
            if code != 0:
                err = st.get("error", "unknown")
                log.error("订单被网关拒绝: code=%s error=%s", code, err)
                for mid, ev in list(self._pending_confirm.items()):
                    ev.set()
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
        elif mt in (26, 27):
            d = frame.get("d")
            if isinstance(d, list):
                for pos in d:
                    if isinstance(pos, dict) and pos.get("mkt") is not None:
                        mid = int(pos["mkt"])
                        if mid in self._pending_confirm:
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
        pos = self.strategy.pos
        out = self.log_dir / f"bot_state_{self._run_id}.json"
        out.write_text(json.dumps({
            "run_id": self._run_id,
            "network": self.network,
            "market": pos.market if pos else "FLAT",
            "dry_run": self.dry_run,
            "params": self.p.__dict__,
            "strategy": self.strategy.summary(),
            "kuru_ledger": self.ledger.summary(),
            "head": self._head,
            "markets_watched": len(self._states),
        }, ensure_ascii=False, indent=1), encoding="utf-8")

    async def _state_saver(self, interval: float = 60.0) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                self._save_state()
                pos = self.strategy.pos
                log.info("状态已定期保存 (market=%s events=%s collected=%s)",
                         pos.market if pos else "FLAT",
                         pos.funding_events if pos else 0,
                         pos.funding_collected if pos else 0)
            except Exception as e:
                log.error("定期保存状态失败: %s", e)

    # ── 主循环 ────────────────────────────────
    async def run(self) -> None:
        await self.start()
        tasks = []
        tasks.append(asyncio.create_task(self.market_ws.run()))
        if self.trader is not None:
            tasks.append(asyncio.create_task(self._trading_listen()))
        tasks.append(asyncio.create_task(self._state_saver(interval=60.0)))
        log.info("Bot 运行中（%s, 多市场轮动, dry_run=%s）...", self.network, self.dry_run)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Bot 停止")
            self._save_state()

    async def _trading_listen(self) -> None:
        try:
            async for raw in self.trader._ws:
                frame = json.loads(raw)
                await self._on_trading_frame(frame)
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
    parser = argparse.ArgumentParser(description="Alpha Terminal Bot Runner（多市场轮动）")
    parser.add_argument("--network", type=str, default="testnet", choices=["testnet", "mainnet"])
    parser.add_argument("--dry-run", action="store_true", help="纯模拟模式（不连 trading WS）")
    parser.add_argument("--config", type=str, default="config/strategy_test.json")
    parser.add_argument("--timeout", type=int, default=0, help="运行秒数（0=无限）")
    args = parser.parse_args()

    params = StrategyParams.from_file(args.config)
    log.info("参数: %sx 保证金=%sU 名义=%sU 现货=%sU",
             params.leverage, params.collateral_usd, params.perp_notional, params.spot_usd)

    bot = AlphaTerminalBotMulti(params, network=args.network, dry_run=args.dry_run)

    async def _main():
        await bot.start()
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
