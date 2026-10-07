"""Perpl WS 客户端 - 订阅市场数据流

基于 docs.perpl.xyz WebSocket 文档（2026-09-24 确认）：
- 端点: wss://app.perpl.xyz/ws/v1/market-data（免认证）
- 流格式: <name>@<key>；market-scoped 流用市场 ID，chain-scoped 流用链 ID
- 订阅: mt=5, subs=[{stream, subscribe:true}]
- 响应: mt=6 SubscriptionResponse（sid 用于匹配后续帧）
- 价格/尺寸为 scaled int，用 MarketConfig 的 price_decimals/size_decimals 缩放
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
    """Perpl 市场数据 WS 客户端"""

    def __init__(self, config: CollectorConfig, on_message: Callable[[dict], Awaitable[None]]):
        self.cfg = config
        self.on_message = on_message          # 异步消息处理器
        self.net = config.net()
        self.ws_url = f"{self.net['ws_base']}/ws/v1/market-data"
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._sid_map: Dict[int, str] = {}    # sid -> stream
        self._last_heartbeat = 0.0
        self._last_sn: Optional[int] = None
        self._running = False

    def _streams(self) -> list:
        """构造订阅流列表"""
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
            for m in mids.values():
                streams.append({"stream": f"candles@{m}*{self.cfg.candle_resolution}", "subscribe": True})
        return streams

    async def connect(self) -> None:
        """连接并订阅"""
        log.info("连接 %s", self.ws_url)
        self._ws = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
        # 订阅
        req = {"mt": MT["SUBSCRIPTION_REQUEST"], "subs": self._streams()}
        await self._ws.send(json.dumps(req))
        log.info("已发送订阅请求，%d 个流", len(self._streams()))

    async def _handle_frame(self, raw: str) -> None:
        """解析帧并分发"""
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("非 JSON 帧: %s", raw[:200])
            return

        mt = frame.get("mt")
        # 心跳监控
        if mt == MT["HEARTBEAT"]:
            self._last_heartbeat = time.time()
            sn = frame.get("sn")
            if self._last_sn is not None and sn is not None and sn != self._last_sn + 1:
                log.warning("心跳序列跳变: %s -> %s（可能丢消息）", self._last_sn, sn)
            self._last_sn = sn
            # 心跳不落盘，仅监控
            return

        # 订阅响应：记录 sid -> stream 映射
        if mt == MT["SUBSCRIPTION_RESPONSE"]:
            for sub in frame.get("subs", []):
                sid = sub.get("sid")
                stream = sub.get("stream")
                status = sub.get("status", {})
                if sid is not None:
                    self._sid_map[sid] = stream
                if status.get("code") != 0:
                    log.warning("订阅失败 %s: %s", stream, status.get("error"))
                else:
                    log.info("订阅成功: %s (sid=%s)", stream, sid)
            return

        await self.on_message(frame)

    async def run(self) -> None:
        """主循环：收帧 + 心跳超时检测 + 自动重连"""
        self._running = True
        while self._running:
            try:
                await self.connect()
                async for raw in self._ws:
                    await self._handle_frame(raw)
            except (websockets.ConnectionClosed, OSError) as e:
                log.warning("连接断开: %s", e)
            except Exception as e:
                log.error("未预期错误: %s", e, exc_info=True)
            finally:
                self._running = False
                if self.cfg.auto_reconnect:
                    log.info("%ds 后重连...", self.cfg.reconnect_delay)
                    await asyncio.sleep(self.cfg.reconnect_delay)
                    self._running = True

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
