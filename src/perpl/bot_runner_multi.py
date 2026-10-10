"""Alpha Terminal Bot Runner (multi-market rotation version, Phase 2.5) - fully automated funding arb

Risk-control requirements decided on 2026-10-09 (mandatory for fully automated bots):
1. Short-side adverse move (price rise) +15% -> close both legs and reopen (recalibrate to a new baseline)
2. funding turns negative or < 5 micro -> close immediately, scan all markets for a new target (leave when rates drop, find fatter ones)

Key upgrades vs bot_runner.py (single market):
- Subscribe to all markets (market-state/funding streams are already broadcast market-wide)
- When FLAT: scan all markets, open on the one with the highest funding + qualifying premium
- When HEDGED: only monitor the held market (rate low / negative / 15% deviation -> close) -> back to FLAT, auto-rescan and switch targets

Usage:
    python bot_runner_multi.py                    # real testnet execution (API key in config_local.py)
    python bot_runner_multi.py --dry-run          # pure simulation
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


# Candidate markets: named markets + numeric markets with known liquidity (testnet)
CANDIDATE_MARKETS = ["BTC", "ETH", "SOL", "MON", "ZEC"]


class AlphaTerminalBotMulti:
    """Multi-market rotation: when FLAT scan all markets and pick the best, when HEDGED monitor the held market"""

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

        # Candidate market IDs (named + numeric market probe)
        self.candidate_ids = [sym_to_mid(s, network) for s in CANDIDATE_MARKETS]
        self.candidate_ids += [m for m in self._probe_numeric_markets() if m not in self.candidate_ids]

        # Market scaling (fetched via REST, per-market dict)
        self.price_dec: Dict[int, int] = {}
        self.size_dec: Dict[int, int] = {}
        self._load_market_configs()

        # Data sources / execution (lazy)
        self.market_ws: Optional[PerplWSClient] = None
        self.trader = None

        # Runtime state (multi-market cache)
        self._states: Dict[str, dict] = {}       # mid -> state (scaled)
        self._fundings: Dict[str, dict] = {}     # mid -> funding (scaled)
        self._last_funding_blocks: Dict[str, int] = {}  # mid -> at.b
        self._head: int = 0
        self._run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ops_log = []
        self._pending_confirm: Dict[int, asyncio.Event] = {}
        self._order_state: Dict[int, dict] = {}

    def _probe_numeric_markets(self) -> list:
        """Probe numeric testnet markets (fetch market list from REST context, pick ones that may have positive funding)"""
        import requests
        try:
            r = requests.get(f"{self.net['rest_base']}/v1/pub/context", timeout=15)
            d = r.json()
            mids = [m.get("id") for m in d.get("markets", [])]
            return [m for m in mids if m not in self.market_src.values()]
        except Exception as e:
            log.warning("Failed to probe numeric markets: %s", e)
            return []

    def _load_market_configs(self) -> None:
        """Fetch scaling params for all candidate markets from REST context"""
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
            log.info("Market config loaded: %d candidate markets", len(self.price_dec))
        except Exception as e:
            log.warning("Failed to fetch market config: %s", e)

    def _scale_price(self, mid: int, raw: int) -> float:
        return raw / (10 ** self.price_dec.get(mid, 5))

    # ── Startup ──────────────────────────────────
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
            log.info("trading WS ready: account=%s head=%s positions=%s",
                     self.trader._account_id, self._head, len(self.trader.positions))
            await self._recover_positions()
        else:
            log.info("dry-run mode: no trading WS connection")

        # Subscribe to all candidate markets (market-state/funding streams are broadcast market-wide, no per-market subscription needed)
        cfg = CollectorConfig(network=self.network, markets=CANDIDATE_MARKETS,
                              subscribe_orderbook=False, subscribe_trades=False,
                              subscribe_candles=False)
        self.market_ws = PerplWSClient(cfg, self._on_market_frame)
        log.info("market-data WS will connect: %s", self.market_ws.ws_url)

    async def _recover_positions(self) -> None:
        """Restart recovery: read real on-chain positions and sync strategy to HEDGED"""
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
                log.info("Recovered position: %s (%s) @ %s (strategy set to HEDGED)", sym, mid, ep)

    # ── Data stream handling (all markets) ───────────
    async def _on_market_frame(self, frame: dict) -> None:
        mt = frame.get("mt")
        if mt == 9:  # MARKET_STATE_UPDATE (market-wide broadcast)
            d = frame.get("d") or {}
            for mid, st in d.items():
                self._states[mid] = st
            # Try to decide on every frame (FLAT scan / HEDGED monitor); state update is the highest-frequency signal
            await self._process_signal_all(is_funding=False)
        elif mt == 10:  # MARKET_FUNDING_UPDATE (market-wide broadcast)
            d = frame.get("d") or {}
            settled = []
            for mid, f in d.items():
                self._fundings[mid] = f
                # Settlement detection (per-market independent at.b)
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
        # Ignore other frame types

    async def _process_signal_all(self, is_funding: bool, settled_markets: Optional[list] = None) -> None:
        """Multi-market decision entry point:
        - HEDGED: only monitor the held market (only settlement events trigger accumulation/close checks)
        - FLAT: scan all markets and pick the best (tried on every frame)
        """
        if self.strategy.state == State.HEDGED:
            pos = self.strategy.pos
            if not pos:
                return
            pos_mid = self._market_to_mid(pos.market)
            # Only handle settlement events for the held market (rate accumulation + close checks)
            if is_funding and settled_markets:
                if str(pos_mid) in settled_markets:
                    await self._process_signal(str(pos_mid), is_funding=True)
            elif not is_funding:
                # Also monitor price deviation on non-settlement frames (15% risk control)
                if str(pos_mid) in self._states:
                    await self._process_signal(str(pos_mid), is_funding=False)
        else:
            # FLAT: scan every frame and pick the best
            await self._scan_and_open()

    # ── Decision (multi-market) ───────────────────────
    async def _process_signal(self, mid: str, is_funding: bool) -> None:
        """Multi-market decision:
        - HEDGED: only monitor the held market (rate low / negative / 15% deviation -> close)
        - FLAT: scan all markets and pick the best -> open
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

        # Update ledger mark price (unrealized PnL of the held market)
        if self.strategy.pos and self.strategy.pos.market == mid_to_sym(mid_i, self.network):
            self.ledger.update_price(mid_to_sym(mid_i, self.network), mark, ts)

        if self.strategy.state == State.HEDGED:
            # Only monitor the held market
            pos_mid = self._market_to_mid(self.strategy.pos.market)
            if pos_mid != mid_i:
                return
            dec = self.strategy.update_market(mid_to_sym(mid_i, self.network), rate_frac,
                                              oracle, mark, ts, is_funding_event=is_funding)
            if dec and dec.action == "CLOSE":
                log.info("Decision: %s | reason: %s", dec.action, dec.reason)
                await self._execute_close(dec, mid_i)
        else:
            # FLAT: scan all markets and pick the best
            await self._scan_and_open()

    def _market_to_mid(self, sym: str) -> int:
        if sym in self.market_src:
            return self.market_src[sym]
        if sym.startswith("m"):
            return int(sym[1:])
        return -1

    async def _scan_and_open(self) -> None:
        """FLAT: scan all cached markets, pick the one with the highest funding + qualifying premium -> open"""
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
            # Open conditions: funding >= threshold + premium >= minimum
            if rate_frac < self.p.funding_threshold:
                continue
            if premium_bps < self.p.premium_min_bps:
                continue
            # Scoring: funding primary, premium secondary
            score = rate_frac * 10000 + premium_bps / 100
            if score > best_score:
                best_score = score
                best_mid = mid
                best_info = (rate_frac, premium_bps, mark, oracle)

        if best_mid is None:
            return  # no qualifying market, keep waiting
        sym = mid_to_sym(int(best_mid), self.network)
        rate_frac, premium_bps, mark, oracle = best_info
        log.info(">>> [SCAN] selected %s (funding=%s premium=%.2fbp mark=%.5f)", sym, rate_frac, premium_bps, mark)
        # Feed the strategy to open
        dec = self.strategy.update_market(sym, rate_frac, oracle, mark,
                                          int(time.time() * 1000), is_funding_event=False)
        if dec and dec.action == "OPEN":
            log.info("Decision: %s | reason: %s", dec.action, dec.reason)
            await self._execute_open(dec, int(best_mid))

    # ── Execution ──────────────────────────────
    async def _execute_open(self, dec, mid_i: int) -> None:
        mark = dec.price
        size_dec = self.size_dec.get(mid_i, 0)
        size = max(1, round(self.p.perp_notional / mark * (10 ** size_dec)))
        sym = dec.market
        log.info(">>> [OPEN] %s perpetual short %s @ %s (notional %sU) leverage %sx | %s",
                 sym, size, mark, self.p.perp_notional, self.p.leverage, dec.reason)
        if not self.dry_run:
            lv = int(self.p.leverage * 100)
            ok = await self._place_and_confirm(mid_i, 2, size, lv, expect="open")
            if not ok:
                log.error("Open not confirmed, skipping Kuru ledger entry (keep consistency)")
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
            log.warning(">>> [CLOSE] %s no real position to close, only Kuru ledger close | %s", sym, dec.reason)
        else:
            log.info(">>> [CLOSE] %s perpetual close short %s @ %s | %s", sym, size, mark, dec.reason)
            if not self.dry_run:
                lv = int(self.p.leverage * 100)
                ok = await self._place_and_confirm(mid_i, 4, size, lv, expect="close")
                if not ok:
                    log.error("Close not confirmed, skipping Kuru ledger entry")
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
            log.error("Timeout waiting for position confirmation (%ss) - order book may have no counterparty or the order is stuck", timeout)
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
                log.error("Order rejected by gateway: code=%s error=%s", code, err)
                for mid, ev in list(self._pending_confirm.items()):
                    ev.set()
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
        elif mt in (26, 27):
            d = frame.get("d")
            if isinstance(d, list):
                for pos in d:
                    if isinstance(pos, dict) and pos.get("mkt") is not None:
                        mid = int(pos["mkt"])
                        if mid in self._pending_confirm:
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
                log.info("State periodically saved (market=%s events=%s collected=%s)",
                         pos.market if pos else "FLAT",
                         pos.funding_events if pos else 0,
                         pos.funding_collected if pos else 0)
            except Exception as e:
                log.error("Failed to periodically save state: %s", e)

    # ── Main loop ──────────────────────────────
    async def run(self) -> None:
        await self.start()
        tasks = []
        tasks.append(asyncio.create_task(self.market_ws.run()))
        if self.trader is not None:
            tasks.append(asyncio.create_task(self._trading_listen()))
        tasks.append(asyncio.create_task(self._state_saver(interval=60.0)))
        log.info("Bot running (%s, multi-market rotation, dry_run=%s)...", self.network, self.dry_run)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Bot stopped")
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
            log.error("Trading listener interrupted: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Alpha Terminal Bot Runner (multi-market rotation)")
    parser.add_argument("--network", type=str, default="testnet", choices=["testnet", "mainnet"])
    parser.add_argument("--dry-run", action="store_true", help="pure simulation mode (no trading WS connection)")
    parser.add_argument("--config", type=str, default="config/strategy_test.json")
    parser.add_argument("--timeout", type=int, default=0, help="run seconds (0=infinite)")
    args = parser.parse_args()

    params = StrategyParams.from_file(args.config)
    log.info("Params: %sx collateral=%sU notional=%sU spot=%sU",
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
