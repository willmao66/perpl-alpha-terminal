"""Perpl data collector - time capsule prototype

Phase 1 goal: continuously collect Perpl market data and persist it (self-use data layer).
- Data source: Perpl official WS (unauthenticated market-data)
- Persistence: JSONL, per-day directories, per-stream-type files
- Added value: every message gets a `_recv_ts` (local receive timestamp, a dimension official data does not have)

Usage:
    python perpl_collector.py            # mainnet default
    python perpl_collector.py --testnet  # testnet
    python perpl_collector.py --markets BTC,ETH
"""
import argparse
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from perpl_config import CollectorConfig, MT
from perpl_ws_client import PerplWSClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("perpl.collector")


class PerplCollector:
    """Collection main loop: receive frame -> timestamp -> persist JSONL"""

    def __init__(self, config: CollectorConfig):
        self.cfg = config
        self.data_root = Path(config.data_dir)
        self.data_root.mkdir(parents=True, exist_ok=True)
        # Currently open write handles: (date, stream type, market) -> file
        self._handles: Dict[tuple, object] = {}
        self._stats: Dict[str, int] = {}
        self._start = time.time()

    # ── File management ───────────────────────
    def _file_for(self, stream: str, frame: dict) -> Optional[object]:
        """Decide the persistence file by (date, stream type, market)"""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        mt = frame.get("mt")
        # Reverse-lookup the stream from sid (saved at subscription time)
        sid = frame.get("sid")
        if sid is not None:
            stream = self._sid_map.get(sid, stream)

        # Classification: market-state / funding are chain-level, the rest are market-level
        if mt == MT["MARKET_STATE_UPDATE"]:
            key, fname = ("market_state",), "market_state.jsonl"
        elif mt == MT["MARKET_FUNDING_UPDATE"]:
            key, fname = ("funding",), "funding.jsonl"
        elif mt in (MT["L2_BOOK_SNAPSHOT"], MT["L2_BOOK_UPDATE"]):
            mkt = self._market_from_sid(sid)
            key, fname = ("orderbook", mkt), f"orderbook_{mkt}.jsonl"
        elif mt in (MT["TRADES_SNAPSHOT"], MT["TRADES_UPDATE"]):
            mkt = self._market_from_sid(sid)
            key, fname = ("trades", mkt), f"trades_{mkt}.jsonl"
        elif mt in (MT["CANDLES_SNAPSHOT"], MT["CANDLES_UPDATE"]):
            mkt = self._market_from_sid(sid)
            key, fname = ("candles", mkt), f"candles_{mkt}.jsonl"
        else:
            return None

        handle_key = (now,) + key
        if handle_key not in self._handles:
            day_dir = self.data_root / now
            day_dir.mkdir(parents=True, exist_ok=True)
            f = open(day_dir / fname, "a", encoding="utf-8")
            self._handles[handle_key] = f
        return self._handles[handle_key]

    def _market_from_sid(self, sid: Optional[int]) -> str:
        """sid -> market symbol (reverse-lookup the subscribed stream name order-book@1 -> BTC)"""
        if sid is None:
            return "?"
        stream = self._sid_map.get(sid, "")
        # stream looks like order-book@1 or candles@1*60
        try:
            mkt_id = int(stream.split("@")[1].split("*")[0])
        except (IndexError, ValueError):
            return "?"
        inv = {v: k for k, v in self.cfg.market_ids().items()}
        return inv.get(mkt_id, f"m{mkt_id}")

    # ── Message handling ──────────────────────
    async def on_message(self, frame: dict) -> None:
        f = self._file_for("", frame)
        if f is None:
            return
        # Attach the local receive timestamp (the time capsule core added value)
        record = {"_recv_ts": int(time.time() * 1000), **frame}
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        mt = frame.get("mt")
        self._stats[mt] = self._stats.get(mt, 0) + 1

    # ── Main loop ─────────────────────────────
    async def run(self) -> None:
        client = PerplWSClient(self.cfg, self.on_message)
        # Expose sid_map to the collector for reverse lookup
        self._sid_map = client._sid_map
        log.info("Collection started | network=%s markets=%s persist=%s",
                 self.cfg.network, list(self.cfg.market_ids().keys()), self.data_root)
        stats_task = asyncio.create_task(self._stats_reporter())
        try:
            await client.run()
        finally:
            stats_task.cancel()
            self._close_all()

    async def _stats_reporter(self) -> None:
        """Log stats every 60s"""
        while True:
            await asyncio.sleep(60)
            elapsed = time.time() - self._start
            total = sum(self._stats.values())
            rate = total / elapsed if elapsed > 0 else 0
            log.info("Stats | total_msgs=%d rate=%.1f msg/s | %s",
                     total, rate, {MT_REV.get(k, k): v for k, v in sorted(self._stats.items())})

    def _close_all(self) -> None:
        for f in self._handles.values():
            f.close()
        self._handles.clear()


MT_REV = {v: k for k, v in MT.items()}


def main():
    parser = argparse.ArgumentParser(description="Perpl data collector")
    parser.add_argument("--testnet", action="store_true", help="use testnet")
    parser.add_argument("--markets", type=str, default="", help="market list, comma-separated, e.g. BTC,ETH")
    parser.add_argument("--data-dir", type=str, default="data", help="persistence directory")
    parser.add_argument("--no-orderbook", action="store_true", help="do not subscribe to order book")
    parser.add_argument("--no-trades", action="store_true", help="do not subscribe to trades")
    parser.add_argument("--no-candles", action="store_true", help="do not subscribe to candles")
    args = parser.parse_args()

    cfg = CollectorConfig(
        network="testnet" if args.testnet else "mainnet",
        markets=[m.strip() for m in args.markets.split(",") if m.strip()] or ["BTC", "MON", "ETH", "SOL", "HYPE", "ZEC"],
        data_dir=args.data_dir,
        subscribe_orderbook=not args.no_orderbook,
        subscribe_trades=not args.no_trades,
        subscribe_candles=not args.no_candles,
    )
    collector = PerplCollector(cfg)
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        log.info("Manual stop")


if __name__ == "__main__":
    main()
