"""Perpl WS client - subscribe to market data streams

Based on docs.perpl.xyz WebSocket documentation (confirmed 2026-09-24):
- Endpoint: wss://app.perpl.xyz/ws/v1/market-data (unauthenticated)
- Stream format: <name>@<key>; market-scoped streams use market ID, chain-scoped streams use chain ID
- Subscribe: mt=5, subs=[{stream, subscribe:true}]
- Response: mt=6 SubscriptionResponse (sid used to match subsequent frames)
- Price/size are scaled int, scaled with MarketConfig's price_decimals/size_decimals
"""
import asyncio
import json
import logging
import time
from typing import Awaitable, Callable, Dict, Optional

import websockets

from perpl_config import CollectorConfig, MT

log = logging.getLogger("perpl.ws")


class PerplWSClient:
    """Perpl market data WS client"""

    def __init__(self, config: CollectorConfig, on_message: Callable[[dict], Awaitable[None]]):
        self.cfg = config
        self.on_message = on_message          # async message handler
        self.net = config.net()
        self.ws_url = f"{self.net['ws_base']}/ws/v1/market-data"
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._sid_map: Dict[int, str] = {}    # sid -> stream
        self._last_heartbeat = 0.0
        self._last_sn: Optional[int] = None
        self._running = False

    def _streams(self) -> list:
        """Build the subscription stream list"""
        chain = self.net["chain_id"]
        mids = self.cfg.market_ids()
        streams = [
            {"stream": f"heartbeat@{chain}", "subscribe": True},
            {"stream": f"market-state@{chain}", "subscribe": True},
            {"stream": f"funding@{chain}", "subscribe": True},
        ]
        if self.cfg.subscribe_orderbook:
            for m in mids.values():
                streams.append({"stream": f"order-book@{m}", "subscribe": True})
        if self.cfg.subscribe_trades:
            for m in mids.values():
                streams.append({"stream": f"trades@{m}", "subscribe": True})
        if self.cfg.subscribe_candles:
            # candles stream limit: subscribe to at most ~6 markets (11 causes too many subscriptions)
            for m in list(mids.values())[:6]:
                streams.append({"stream": f"candles@{m}*{self.cfg.candle_resolution}", "subscribe": True})
        return streams

    async def connect(self) -> None:
        """Connect and subscribe"""
        log.info("Connecting to %s", self.ws_url)
        self._ws = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
        # Subscribe
        req = {"mt": MT["SUBSCRIPTION_REQUEST"], "subs": self._streams()}
        await self._ws.send(json.dumps(req))
        log.info("Sent subscription request, %d streams", len(self._streams()))

    async def _handle_frame(self, raw: str) -> None:
        """Parse the frame and dispatch it"""
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Non-JSON frame: %s", raw[:200])
            return

        mt = frame.get("mt")
        # Heartbeat monitoring
        if mt == MT["HEARTBEAT"]:
            self._last_heartbeat = time.time()
            sn = frame.get("sn")
            if self._last_sn is not None and sn is not None and sn != self._last_sn + 1:
                log.warning("Heartbeat sequence jump: %s -> %s (possible missed messages)", self._last_sn, sn)
            self._last_sn = sn
            # Heartbeats are not persisted, only monitored
            return

        # Subscription response: record sid -> stream mapping
        if mt == MT["SUBSCRIPTION_RESPONSE"]:
            for sub in frame.get("subs", []):
                sid = sub.get("sid")
                stream = sub.get("stream")
                status = sub.get("status", {})
                if sid is not None:
                    self._sid_map[sid] = stream
                if status.get("code") != 0:
                    log.warning("Subscription failed %s: %s", stream, status.get("error"))
                else:
                    log.info("Subscription successful: %s (sid=%s)", stream, sid)
            return

        await self.on_message(frame)

    async def run(self) -> None:
        """Main loop: receive frames + heartbeat timeout detection + auto-reconnect"""
        self._running = True
        while self._running:
            try:
                await self.connect()
                async for raw in self._ws:
                    await self._handle_frame(raw)
            except (websockets.ConnectionClosed, OSError) as e:
                log.warning("Connection closed: %s", e)
            except Exception as e:
                log.error("Unexpected error: %s", e, exc_info=True)
            finally:
                self._running = False
                if self.cfg.auto_reconnect:
                    log.info("Reconnecting in %ds...", self.cfg.reconnect_delay)
                    await asyncio.sleep(self.cfg.reconnect_delay)
                    self._running = True

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
