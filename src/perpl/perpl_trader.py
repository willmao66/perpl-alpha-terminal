"""Perpl 执行层（Phase 2）—— 官方 Trading WebSocket + Ed25519 API key 签名

基于 docs.perpl.xyz（2026-10-07 确认）：
- 认证：ApiKeySignIn (mt=29)，canonical = chain_id + "trading-ws-signin" + ts + nonce
- 下单：OrderRequest (mt=22)，t=2 OpenShort / t=4 CloseShort / t=6 IncreasePositionCollateral
- 幂等：rq 严格递增（seed from account.lfr），防重复下单
- 杠杆：hundredths（3x = 300）
- 价格/尺寸：scaled int（price_decimals / size_decimals）
- 金额：AUSD 6 decimals

⚠️ 只读 API key 不能下单（403）；下单需 trade-scope key。
⚠️ 提现/转账永不通过 API key 允许。
"""
import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import time
from typing import Optional

import websockets
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from perpl_config import CollectorConfig

log = logging.getLogger("perpl.trader")

# WS 消息类型（trading）
MT = {
    "STATUS_RESPONSE": 3,
    "ORDER_REQUEST": 22,
    "ORDERS_SNAPSHOT": 23,
    "FILLS_UPDATE": 25,
    "POSITIONS_SNAPSHOT": 26,
    "POSITIONS_UPDATE": 27,
    "API_KEY_SIGN_IN": 29,
    "WALLET_SNAPSHOT": 19,
    "ACCOUNT_UPDATE": 21,
}

# 订单类型
ORDER_TYPE = {
    "OPEN_LONG": 1,
    "OPEN_SHORT": 2,
    "CLOSE_LONG": 3,
    "CLOSE_SHORT": 4,
    "CANCEL": 5,
    "INCREASE_COLLATERAL": 6,
    "CHANGE": 7,
}

# 订单标志
ORDER_FLAGS = {"GTC": 0, "POST_ONLY": 1, "FOK": 2, "IOC": 4}


class PerplAuth:
    """Ed25519 API key 签名（REST + WS）"""

    def __init__(self, api_key: str, private_key_hex: str, chain_id: int = 143):
        self.api_key = api_key
        self.private_key = Ed25519PrivateKey.from_private_bytes(
            bytes.fromhex(private_key_hex.replace("0x", ""))
        )
        self.chain_id = chain_id

    def _sign(self, canonical: str) -> str:
        sig = self.private_key.sign(canonical.encode("utf-8"))
        return base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")

    def ws_signin_frame(self) -> dict:
        """构造 ApiKeySignIn 帧（mt=29）"""
        ts = str(int(time.time() * 1000))
        nonce = secrets.token_bytes(16)
        nonce_b64 = base64.urlsafe_b64encode(nonce).rstrip(b"=").decode("ascii")
        canonical = "\n".join([str(self.chain_id), "trading-ws-signin", ts, nonce_b64])
        return {
            "mt": MT["API_KEY_SIGN_IN"],
            "chain_id": self.chain_id,
            "api_key": self.api_key,
            "timestamp": ts,
            "nonce": nonce_b64,
            "signature": self._sign(canonical),
        }

    def rest_headers(self, method: str, target: str, body: str = "") -> dict:
        """REST 请求签名 headers"""
        ts = str(int(time.time() * 1000))
        nonce = secrets.token_bytes(16)
        nonce_b64 = base64.urlsafe_b64encode(nonce).rstrip(b"=").decode("ascii")
        body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        canonical = "\n".join([str(self.chain_id), method, target, ts, nonce_b64, body_hash])
        return {
            "X-API-Key": self.api_key,
            "X-API-Timestamp": ts,
            "X-API-Nonce": nonce_b64,
            "X-API-Signature": self._sign(canonical),
        }


class PerplTradingClient:
    """Perpl trading WS 客户端（下单/查持仓）"""

    def __init__(self, auth: PerplAuth, ws_url: Optional[str] = None, chain_id: int = 143):
        self.auth = auth
        self.ws_url = ws_url or f"wss://app.perpl.xyz/ws/v1/trading"
        self.chain_id = chain_id
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._account_id: Optional[int] = None
        self._last_rq: int = 0
        self.positions: dict = {}   # market_id -> position
        self.wallet: dict = {}

    # ── 连接与认证 ──────────────────────────
    async def connect(self) -> None:
        self._ws = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
        await self._ws.send(json.dumps(self.auth.ws_signin_frame()))
        log.info("已发送 ApiKeySignIn")

    async def _read_until_snapshots(self, timeout: float = 15.0) -> None:
        """读取初始三个快照（wallet/orders/positions），seed rq"""
        deadline = time.time() + timeout
        got_positions = False
        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue
            frame = json.loads(raw)
            mt = frame.get("mt")
            if mt == MT["WALLET_SNAPSHOT"]:
                self.wallet = frame
                # 账户在 as[] 数组里：取 id + lfr（rq 种子）
                for acc in frame.get("as", []):
                    if isinstance(acc, dict):
                        lfr = acc.get("lfr", 0) or 0
                        self._last_rq = max(self._last_rq, lfr)
                        aid = acc.get("id")
                        if aid:
                            self._account_id = aid
                log.info("WalletSnapshot: addr=%s account=%s last_rq=%s",
                         frame.get("addr"), self._account_id, self._last_rq)
            elif mt == MT["POSITIONS_SNAPSHOT"]:
                d = frame.get("d")
                if isinstance(d, list):
                    for pos in d:
                        if isinstance(pos, dict) and pos.get("mkt") is not None:
                            self.positions[pos["mkt"]] = pos
                elif isinstance(d, dict):
                    self.positions.update(d)
                got_positions = True
                log.info("PositionsSnapshot: %d 个持仓", len(self.positions))
            elif mt == MT["ORDERS_SNAPSHOT"]:
                pass  # 初始订单快照，暂不处理
            if self._account_id is not None and got_positions:
                break

    # ── 下单 ────────────────────────────────
    async def place_order(self, market_id: int, order_type: int, size: int,
                          leverage_hundredths: int = 300, price_scaled: int = 0,
                          flags: int = 0, tif: Optional[int] = None,
                          last_block: int = 0, slippage_bps: Optional[int] = None,
                          amount: Optional[str] = None) -> dict:
        """发送 OrderRequest。size/price 为 scaled int。
        返回 StatusResponse（网关是否接受），非订单结果。"""
        if self._ws is None:
            raise RuntimeError("未连接")
        if self._account_id is None:
            raise RuntimeError("账户未初始化（需要 on-chain exchange account）")

        self._last_rq += 1
        frame = {
            "mt": MT["ORDER_REQUEST"],
            "sn": self._last_rq,          # 唯一，回显为 cid
            "rq": self._last_rq,          # 幂等键，严格递增
            "mkt": market_id,
            "acc": self._account_id,
            "t": order_type,
            "p": price_scaled,
            "s": size,
            "fl": flags,
            "lv": leverage_hundredths,
            "lb": last_block,
        }
        if tif is not None:
            frame["tif"] = tif
        if slippage_bps is not None:
            frame["ms"] = slippage_bps
        if amount is not None:
            frame["a"] = amount

        await self._ws.send(json.dumps(frame))
        log.info("OrderRequest 已发送: type=%s mkt=%s size=%s rq=%s",
                 order_type, market_id, size, self._last_rq)
        return frame

    # ── 策略封装（我们用的）─────────────────
    async def open_short(self, market_id: int, size_scaled: int,
                         leverage_hundredths: int = 300,
                         market_price_scaled: int = 0) -> dict:
        """开空（funding arb 永续腿）"""
        return await self.place_order(market_id, ORDER_TYPE["OPEN_SHORT"], size_scaled,
                                      leverage_hundredths, market_price_scaled)

    async def close_short(self, market_id: int, size_scaled: int) -> dict:
        """平空（触发风控/费率回落时）"""
        return await self.place_order(market_id, ORDER_TYPE["CLOSE_SHORT"], size_scaled)

    async def add_collateral(self, market_id: int, amount_ausd: str) -> dict:
        """加保证金（爆拉时补 Perpl 仓位保证金）amount 为 AUSD decimal string"""
        return await self.place_order(market_id, ORDER_TYPE["INCREASE_COLLATERAL"], 0,
                                      amount=amount_ausd)

    # ── 主循环 ──────────────────────────────
    async def run(self, on_update=None):
        """连接 + 认证 + 持续监听持仓/账户更新"""
        await self.connect()
        await self._read_until_snapshots()
        log.info("trading WS 就绪，监听中...")
        async for raw in self._ws:
            frame = json.loads(raw)
            mt = frame.get("mt")
            if mt == MT["POSITIONS_UPDATE"] or mt == MT["POSITIONS_SNAPSHOT"]:
                d = frame.get("d")
                if isinstance(d, list):  # PositionsUpdate d 是数组
                    for pos in d:
                        if isinstance(pos, dict) and pos.get("mkt") is not None:
                            self.positions[pos["mkt"]] = pos
                elif isinstance(d, dict):
                    self.positions.update(d)
            if on_update:
                await on_update(frame)

    async def close(self) -> None:
        if self._ws:
            await self._ws.close()
