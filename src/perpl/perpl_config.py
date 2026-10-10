"""Perpl Alpha Terminal - configuration module

Source: docs.perpl.xyz (fetched and confirmed 2026-09-24)
"""
from dataclasses import dataclass, field
from typing import Dict, List

# ── Network params (confirmed from official docs) ──────────
MAINNET = {
    "chain_id": 143,
    "rest_base": "https://app.perpl.xyz/api",
    "ws_base": "wss://app.perpl.xyz",
    "rpc": "https://rpc.monad.xyz",
    "exchange_contract": "0x34B6552d57a35a1D042CcAe1951BD1C370112a6F",
    "ausd_token": "0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a",
}

TESTNET = {
    "chain_id": 10143,
    "rest_base": "https://testnet.perpl.xyz/api",
    "ws_base": "wss://testnet.perpl.xyz",
    "rpc": "https://testnet-rpc.monad.xyz",
    "exchange_contract": "0x1964C32f0bE608E7D29302AFF5E61268E72080cc",
    "ausd_token": "0xa9012a055bd4e0eDfF8Ce09f960291C09D5322dC",
}

# ── Market IDs (mainnet, confirmed from official docs) ─────
MARKETS_MAINNET: Dict[str, int] = {
    "BTC": 1,
    "MON": 10,
    "ETH": 20,
    "SOL": 31,
    "HYPE": 40,
    "ZEC": 50,
}

MARKETS_TESTNET: Dict[str, int] = {
    "BTC": 16,
    "ETH": 32,
    "SOL": 48,
    "MON": 64,
    "ZEC": 256,
}

# ── Candle resolution (seconds) ───────────────────────
CANDLE_RESOLUTIONS = [60, 300, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400]

# ── WS message types (confirmed from official docs) ───
MT = {
    "PING": 1,
    "PONG": 2,
    "STATUS_RESPONSE": 3,
    "SUBSCRIPTION_REQUEST": 5,
    "SUBSCRIPTION_RESPONSE": 6,
    "GAS_PRICE_UPDATE": 7,
    "MARKET_CONFIG_UPDATE": 8,
    "MARKET_STATE_UPDATE": 9,
    "MARKET_FUNDING_UPDATE": 10,
    "CANDLES_SNAPSHOT": 11,
    "CANDLES_UPDATE": 12,
    "L2_BOOK_SNAPSHOT": 15,
    "L2_BOOK_UPDATE": 16,
    "TRADES_SNAPSHOT": 17,
    "TRADES_UPDATE": 18,
    "HEARTBEAT": 100,
}

# ── WS rate limits (from official docs) ───────────────
WS_MAX_MSGS_PER_SEC = 50      # ~50 msg/s per connection
WS_MAX_CONNS_PER_IP = 5       # ~5 connections per IP


@dataclass
class CollectorConfig:
    """Collector configuration"""
    network: str = "mainnet"                     # mainnet / testnet
    markets: List[str] = field(default_factory=lambda: ["BTC", "MON", "ETH", "SOL", "HYPE", "ZEC"])
    subscribe_orderbook: bool = True             # order book
    subscribe_trades: bool = True                # trades
    subscribe_candles: bool = True               # candles
    candle_resolution: int = 60                  # 1m
    data_dir: str = "data"                       # persistence directory
    heartbeat_timeout: int = 30                  # heartbeat timeout seconds
    auto_reconnect: bool = True                  # auto reconnect
    reconnect_delay: int = 5                     # reconnect wait seconds

    def net(self) -> dict:
        return MAINNET if self.network == "mainnet" else TESTNET

    def market_ids(self) -> Dict[str, int]:
        src = MARKETS_MAINNET if self.network == "mainnet" else MARKETS_TESTNET
        return {k: v for k, v in src.items() if k in self.markets}
