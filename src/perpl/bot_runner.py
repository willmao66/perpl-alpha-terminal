"""Alpha Terminal Bot Runner (Phase 2 bridge layer) - strategy decisions -> Perpl real execution + Kuru ledger

Decided on 2026-10-08 (Option A, Kuru leg simulated in ledger):
- Perpl perpetual leg: **real execution** on testnet (has txid, real on-chain activity evidence)
- Kuru spot leg: **ledger simulation** (delta-neutral math is real, order logic is real, but no real orders placed)

Bridge flow (dual-track):
    market-data WS (real-time funding + market-state)
        | every frame
    strategy engine (FundingArbStrategy, MON low-frequency hold funding arb)
        | decision OPEN / CLOSE
    [OPEN]  Perpl open_short (real testnet order) -> wait for fill confirmation -> Kuru ledger.open_spot (record)
    [CLOSE] Perpl close_short (real testnet close) -> wait for close confirmation -> Kuru ledger.close_spot (record)

Usage:
    python bot_runner.py                # real testnet execution (API key in config_local.py, gitignored)
    python bot_runner.py --dry-run      # pure simulation (no trading WS, local verification of bridge logic)
    python bot_runner.py --market MON   # specify market (default MON)
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


# ── Market symbol ↔ ID ─────────────────────────────
def sym_to_mid(sym: str, network: str) -> int:
    src = MARKETS_MAINNET if network == "mainnet" else MARKETS_TESTNET
    if sym not in src:
        raise ValueError(f"Unknown market {sym} (available: {list(src.keys())})")
    return src[sym]


def mid_to_sym(mid: int, network: str) -> str:
    src = MARKETS_MAINNET if network == "mainnet" else MARKETS_TESTNET
    for k, v in src.items():
        if v == mid:
            return k
    return f"m{mid}"


class AlphaTerminalBot:
    """Bridge layer: strategy signals -> Perpl real execution + Kuru ledger (dual-track)"""

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

        # Market scaling (fetched via REST)
        self.price_dec: int = 5     # MON default
        self.size_dec: int = 0
        self._load_market_config()

        # Data sources / execution (lazy)
        self.market_ws: Optional[PerplWSClient] = None
        self.trader = None          # PerplTradingClient (not created in dry_run mode)

        # Runtime state
        self._state: Optional[dict] = None    # latest market-state (scaled)
        self._funding: Optional[dict] = None  # latest funding (scaled)
        self._head: int = 0                   # latest head block of trading WS
        self._last_funding_block: Optional[int] = None  # last settlement block (at.b, detects real settlement events)
        self._run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ops_log = []                    # operation log (decisions + orders + ledger)
        self._pending_confirm: Dict[int, asyncio.Event] = {}  # rq -> event
        self._order_state: Dict[int, dict] = {}               # rq -> {expect: open/close, market_id}

    # ── Initialization ────────────────────────────
    def _load_market_config(self) -> None:
        """Fetch market scaling params from REST /api/v1/pub/context"""
        import requests
        try:
            # rest_base already includes /api (e.g. https://testnet.perpl.xyz/api), just append v1
            r = requests.get(f"{self.net['rest_base']}/v1/pub/context", timeout=15)
            d = r.json()
            for m in d.get("markets", []):
                if m.get("id") == self.market_id:
                    cfg = m.get("config", {})
                    self.price_dec = cfg.get("price_decimals", 5)
                    self.size_dec = cfg.get("size_decimals", 0)
                    log.info("Market %s: price_dec=%s size_dec=%s", self.market,
                             self.price_dec, self.size_dec)
                    return
            log.warning("REST did not find config for market %s, using defaults price_dec=5 size_dec=0", self.market)
        except Exception as e:
            log.warning("Failed to fetch market config (using defaults): %s", e)

    def _scale_price(self, raw: int) -> float:
        return raw / (10 ** self.price_dec)

    # ── Startup ──────────────────────────────────
    async def start(self) -> None:
        # 1) Connect trading WS (real execution mode)
        if not self.dry_run:
            from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_WS
            from perpl_trader import PerplAuth, PerplTradingClient
            auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
            self.trader = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading",
                                             chain_id=PERPL_CHAIN_ID)
            await self.trader.connect()
            await self.trader._read_until_snapshots(timeout=15)
            self._head = self.trader.wallet.get("at", {}).get("b", 0)
            log.info("trading WS ready: account=%s head=%s positions=%s",
                     self.trader._account_id, self._head, len(self.trader.positions))
            # Recover existing positions -> sync strategy state (crash recovery)
            await self._recover_positions()
        else:
            log.info("dry-run mode: no trading WS connection, Perpl orders are simulated")

        # 2) Connect market-data WS
        cfg = CollectorConfig(network=self.network, markets=[self.market],
                              subscribe_orderbook=False, subscribe_trades=False,
                              subscribe_candles=False)
        self.market_ws = PerplWSClient(cfg, self._on_market_frame)
        log.info("market-data WS will connect: %s", self.market_ws.ws_url)

    async def _recover_positions(self) -> None:
        """Restart recovery: if testnet already has positions, sync strategy state to HEDGED"""
        for mid, pos in self.trader.positions.items():
            s = pos.get("s", 0)
            if s > 0:
                sym = mid_to_sym(int(mid), self.network)
                ep_scaled = pos.get("ep", 0)
                ep = self._scale_price(ep_scaled)
                # Manually set strategy to HEDGED (using real position info)
                from strategy_engine import Position, State
                self.strategy.state = State.HEDGED
                self.strategy.pos = Position(
                    market=sym, entry_price=ep, entry_time=int(time.time() * 1000),
                    perp_size=self.p.perp_notional, spot_size=self.p.spot_usd,
                )
                # Sync Kuru ledger
                self.ledger.open_spot(sym, ep, int(time.time() * 1000),
                                      notional_usd=self.p.spot_usd)
                log.info("Recovered position: %s %s MON @ %s (strategy set to HEDGED)", sym, s, ep)

    # ── Data stream handling ────────────────────────
    async def _on_market_frame(self, frame: dict) -> None:
        """market-data WS callback: update cache + feed strategy + trigger execution"""
        mt = frame.get("mt")
        # 9 = MARKET_STATE_UPDATE, 10 = MARKET_FUNDING_UPDATE
        if mt == 9:
            d = frame.get("d") or {}
            if str(self.market_id) in d:
                self._state = d[str(self.market_id)]
        elif mt == 10:
            d = frame.get("d") or {}
            if str(self.market_id) in d:
                f = d[str(self.market_id)]
                # Real settlement detection: funding event block (at.b) change = new settlement period
                fb = (f.get("at") or {}).get("b")
                is_settle = False
                if fb is not None:
                    if self._last_funding_block is None:
                        # First frame: record baseline, do not accumulate (avoid treating startup snapshot as settlement)
                        is_settle = False
                    elif fb != self._last_funding_block:
                        is_settle = True
                    self._last_funding_block = fb
                self._funding = f
                # Feed only when state is ready (is_funding_event=True only for settlement events)
                if self._state is not None:
                    await self._process_signal(is_funding=is_settle)
                return
        else:
            return

        # Feed strategy only when both state and funding are ready
        if self._state is None or self._funding is None:
            return
        await self._process_signal(is_funding=False)

    async def _process_signal(self, is_funding: bool) -> None:
        """Feed the strategy engine and process the decision"""
        s = self._state
        f = self._funding
        mark = self._scale_price(s.get("mrk", 0))
        oracle = self._scale_price(s.get("orl", 0))
        rate_frac = (f.get("rate", 0) or 0) / 1_000_000
        ts = int(time.time() * 1000)

        # Kuru ledger mark price update (unrealized PnL)
        self.ledger.update_price(self.market, mark, ts)

        dec = self.strategy.update_market(self.market, rate_frac, oracle, mark, ts,
                                          is_funding_event=is_funding)
        if dec and dec.action in ("OPEN", "CLOSE"):
            log.info("Decision: %s | reason: %s", dec.action, dec.reason)
            await self._execute(dec)

    # ── Execution ──────────────────────────────
    async def _execute(self, dec) -> None:
        """Execute decision: Perpl real order (dry-run simulated) + Kuru ledger entry"""
        mark = dec.price
        if dec.action == "OPEN":
            # size = notional / mark x 10^size_dec (scaled int, sent on-chain directly by place_order)
            # MON size_dec=0 -> integer MON; ETH size_dec=3 -> 0.001 ETH granularity
            size = max(1, round(self.p.perp_notional / mark * (10 ** self.size_dec)))
            log.info(">>> [OPEN] %s perpetual short %s %s @ %s (notional %sU) leverage %sx | %s",
                     self.market, size, self.market, mark, self.p.perp_notional, self.p.leverage, dec.reason)
            if not self.dry_run:
                lv = int(self.p.leverage * 100)
                ok = await self._place_and_confirm(2, size, lv, expect="open")
                if not ok:
                    log.error("Open not confirmed, skipping Kuru ledger entry (keep consistency)")
                    return
            self.ledger.open_spot(self.market, mark, dec.ts, notional_usd=self.p.spot_usd)
            self._ops_log.append({"ts": dec.ts, "action": "OPEN", "market": self.market,
                                  "size": size, "price": mark, "notional": self.p.spot_usd,
                                  "reason": dec.reason, "dry_run": self.dry_run})

        elif dec.action == "CLOSE":
            # Close current real position (get size from trader.positions)
            size = 0
            if self.trader is not None:
                pos = self.trader.positions.get(self.market_id, {})
                size = pos.get("s", 0)
            if size <= 0:
                log.warning(">>> [CLOSE] No real position to close, only Kuru ledger close | %s", dec.reason)
            else:
                log.info(">>> [CLOSE] %s perpetual close short %s MON @ %s | %s", self.market, size, mark, dec.reason)
                if not self.dry_run:
                    lv = int(self.p.leverage * 100)
                    ok = await self._place_and_confirm(4, size, lv, expect="close")
                    if not ok:
                        log.error("Close not confirmed, skipping Kuru ledger entry")
                        return
            self.ledger.close_spot(self.market, mark, dec.ts)
            self._ops_log.append({"ts": dec.ts, "action": "CLOSE", "market": self.market,
                                  "size": size, "price": mark, "reason": dec.reason,
                                  "dry_run": self.dry_run})
        self._flush_ops()

    async def _place_and_confirm(self, order_type: int, size: int, lv: int,
                                 expect: str, timeout: float = 30.0) -> bool:
        """Place order + wait for confirmation (position update or order failure frame)

        Success = received PositionsUpdate matching the market
        Failure = received StatusResponse error / OrdersUpdate st=7 (Failed) with reject reason
        """
        # Use latest head as lb (+20 blocks buffer)
        lb = self._head + 20
        await self.trader.place_order(self.market_id, order_type, size,
                                      leverage_hundredths=lv, last_block=lb)
        # Wait for position update event
        event = asyncio.Event()
        self._pending_confirm[self.market_id] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.error("Timeout waiting for position confirmation (%ss) - order book may have no counterparty or the order is stuck", timeout)
            return False
        finally:
            self._pending_confirm.pop(self.market_id, None)
        return True

    async def _on_trading_frame(self, frame: dict) -> None:
        """trading WS frame: update head + handle order status/position confirmation"""
        at = frame.get("at") or {}
        if at.get("b"):
            self._head = at["b"]
        mt = frame.get("mt")
        # 3 = STATUS_RESPONSE: gateway validation result (code!=0 means rejected)
        if mt == 3:
            st = frame.get("status") or {}
            code = st.get("code")
            if code != 0:
                err = st.get("error", "unknown")
                log.error("Order rejected by gateway: code=%s error=%s", code, err)
                # Fail fast: trigger confirmation events (failures handled by caller timeout)
                # Note: no rq mapping here, wake up all waiters
                for mid, ev in list(self._pending_confirm.items()):
                    ev.set()
        # 24 = ORDERS_UPDATE: order status (st=7 Failed with sr reject reason)
        elif mt == 24:
            d = frame.get("d")
            if isinstance(d, list):
                for o in d:
                    if isinstance(o, dict) and o.get("st") == 7:
                        sr = o.get("sr")
                        log.error("Order failed: sr=%s (reject reason), mkt=%s size=%s",
                                  sr, o.get("mkt"), o.get("s"))
                        mid = o.get("mkt")
                        if mid is not None and mid in self._pending_confirm:
                            self._pending_confirm[mid].set()
        # 26/27 = POSITIONS_SNAPSHOT/UPDATE: position change confirmation
        elif mt in (26, 27):
            d = frame.get("d")
            if isinstance(d, list):
                for pos in d:
                    if isinstance(pos, dict) and pos.get("mkt") is not None:
                        mid = int(pos["mkt"])
                        if mid == self.market_id and mid in self._pending_confirm:
                            log.info("Position update confirmed: mkt=%s size=%s st=%s sr=%s",
                                     mid, pos.get("s"), pos.get("st"), pos.get("sr"))
                            self._pending_confirm[mid].set()

    # ── Logging / persistence ───────────────────────
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
        """Periodically save state (so latest funding accumulation is available when running as a systemd daemon)"""
        while True:
            await asyncio.sleep(interval)
            try:
                self._save_state()
                log.debug("State periodically saved (funding_events=%s, collected=%s)",
                          self.strategy.pos.funding_events if self.strategy.pos else 0,
                          self.strategy.pos.funding_collected if self.strategy.pos else 0)
            except Exception as e:
                log.error("Failed to periodically save state: %s", e)

    # ── Main loop ──────────────────────────────
    async def run(self) -> None:
        """Run market-data WS + trading WS listeners in parallel"""
        await self.start()
        tasks = []
        tasks.append(asyncio.create_task(self.market_ws.run()))
        if self.trader is not None:
            tasks.append(asyncio.create_task(self._trading_listen()))
        # Periodically save state (systemd daemon)
        tasks.append(asyncio.create_task(self._state_saver(interval=60.0)))
        log.info("Bot running (%s, market=%s, dry_run=%s)...", self.network, self.market, self.dry_run)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Bot stopped")
            self._save_state()

    async def _trading_listen(self) -> None:
        """Listen to trading WS frames (reuse the loop logic from PerplTradingClient.run)"""
        try:
            async for raw in self.trader._ws:
                frame = json.loads(raw)
                await self._on_trading_frame(frame)
                # Sync positions to client.positions (real positions after open/close)
                mt = frame.get("mt")
                if mt in (26, 27):
                    d = frame.get("d")
                    if isinstance(d, list):
                        for pos in d:
                            if isinstance(pos, dict) and pos.get("mkt") is not None:
                                self.trader.positions[int(pos["mkt"])] = pos
        except Exception as e:
            log.error("Trading listener interrupted: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Alpha Terminal Bot Runner (bridge layer)")
    parser.add_argument("--market", type=str, default="MON")
    parser.add_argument("--network", type=str, default="testnet", choices=["testnet", "mainnet"])
    parser.add_argument("--dry-run", action="store_true", help="pure simulation mode (no trading WS connection)")
    parser.add_argument("--config", type=str, default="config/strategy_test.json")
    parser.add_argument("--timeout", type=int, default=0, help="run seconds (0=infinite)")
    args = parser.parse_args()

    params = StrategyParams.from_file(args.config)
    log.info("Params: %sx collateral=%sU notional=%sU spot=%sU",
             params.leverage, params.collateral_usd, params.perp_notional, params.spot_usd)

    bot = AlphaTerminalBot(params, network=args.network, market=args.market,
                           dry_run=args.dry_run)

    async def _main():
        await bot.start()
        # Specified timeout
        if args.timeout > 0:
            task = asyncio.create_task(bot.run())
            await asyncio.sleep(args.timeout)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            bot._save_state()
            log.info("Run finished after %ss, state saved", args.timeout)
        else:
            await bot.run()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        log.info("Manual interrupt")
        bot._save_state()


if __name__ == "__main__":
    main()
