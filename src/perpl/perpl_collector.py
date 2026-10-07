"""Perpl 数据采集器 - 时间胶囊雏形

第一期目标：持续采集 Perpl 市场数据并落盘（自用数据层）。
- 数据源：Perpl 官方 WS（免认证 market-data）
- 落盘：JSONL，按天分目录，按流类型分文件
- 增值：每条消息附加 `_recv_ts`（本地接收时间戳，官方数据没有的维度）

用法：
    python perpl_collector.py            # mainnet 默认
    python perpl_collector.py --testnet  # 测试网
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
    """采集主循环：收帧 -> 打时间戳 -> 落盘 JSONL"""

    def __init__(self, config: CollectorConfig):
        self.cfg = config
        self.data_root = Path(config.data_dir)
        self.data_root.mkdir(parents=True, exist_ok=True)
        # 当前打开的写入句柄: (日期, 流类型, 市场) -> file
        self._handles: Dict[tuple, object] = {}
        self._stats: Dict[str, int] = {}
        self._start = time.time()

    # ── 文件管理 ──────────────────────────────
    def _file_for(self, stream: str, frame: dict) -> Optional[object]:
        """按 (日期, 流类型, 市场) 决定落盘文件"""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        mt = frame.get("mt")
        # 从 sid 映射反查 stream（订阅时已存）
        sid = frame.get("sid")
        if sid is not None:
            stream = self._sid_map.get(sid, stream)

        # 分类：market-state / funding 是 chain 级，其余 market 级
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
        """sid -> 市场符号（反查订阅流名 order-book@1 -> BTC）"""
        if sid is None:
            return "?"
        stream = self._sid_map.get(sid, "")
        # stream 形如 order-book@1 或 candles@1*60
        try:
            mkt_id = int(stream.split("@")[1].split("*")[0])
        except (IndexError, ValueError):
            return "?"
        inv = {v: k for k, v in self.cfg.market_ids().items()}
        return inv.get(mkt_id, f"m{mkt_id}")

    # ── 消息处理 ──────────────────────────────
    async def on_message(self, frame: dict) -> None:
        f = self._file_for("", frame)
        if f is None:
            return
        # 附加本地接收时间戳（时间胶囊核心增值）
        record = {"_recv_ts": int(time.time() * 1000), **frame}
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        mt = frame.get("mt")
        self._stats[mt] = self._stats.get(mt, 0) + 1

    # ── 主循环 ────────────────────────────────
    async def run(self) -> None:
        client = PerplWSClient(self.cfg, self.on_message)
        # 暴露 sid_map 给 collector 反查
        self._sid_map = client._sid_map
        log.info("采集启动 | 网络=%s 市场=%s 落盘=%s",
                 self.cfg.network, list(self.cfg.market_ids().keys()), self.data_root)
        stats_task = asyncio.create_task(self._stats_reporter())
        try:
            await client.run()
        finally:
            stats_task.cancel()
            self._close_all()

    async def _stats_reporter(self) -> None:
        """每 60s 打一次统计"""
        while True:
            await asyncio.sleep(60)
            elapsed = time.time() - self._start
            total = sum(self._stats.values())
            rate = total / elapsed if elapsed > 0 else 0
            log.info("统计 | 总消息=%d 速率=%.1f msg/s | %s",
                     total, rate, {MT_REV.get(k, k): v for k, v in sorted(self._stats.items())})

    def _close_all(self) -> None:
        for f in self._handles.values():
            f.close()
        self._handles.clear()


MT_REV = {v: k for k, v in MT.items()}


def main():
    parser = argparse.ArgumentParser(description="Perpl 数据采集器")
    parser.add_argument("--testnet", action="store_true", help="使用测试网")
    parser.add_argument("--markets", type=str, default="", help="市场列表，逗号分隔，如 BTC,ETH")
    parser.add_argument("--data-dir", type=str, default="data", help="落盘目录")
    parser.add_argument("--no-orderbook", action="store_true", help="不订阅订单簿")
    parser.add_argument("--no-trades", action="store_true", help="不订阅成交")
    parser.add_argument("--no-candles", action="store_true", help="不订阅K线")
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
        log.info("手动停止")


if __name__ == "__main__":
    main()
