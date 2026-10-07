"""拉取当前 6 个市场的 funding 快照 + 年化，看升水机会"""
import asyncio
import json
import sys

sys.path.insert(0, ".")
from perpl_config import CollectorConfig, MT
from perpl_ws_client import PerplWSClient


async def main():
    cfg = CollectorConfig(markets=["BTC", "MON", "ETH", "SOL", "HYPE", "ZEC"])
    snapshot = {}
    funding = {}

    async def on_msg(frame):
        mt = frame.get("mt")
        if mt == MT["MARKET_STATE_UPDATE"]:
            for mid, st in (frame.get("d") or {}).items():
                snapshot[mid] = st
        elif mt == MT["MARKET_FUNDING_UPDATE"]:
            for mid, f in (frame.get("d") or {}).items():
                funding[mid] = f

    client = PerplWSClient(cfg, on_msg)
    task = asyncio.create_task(client.run())
    await asyncio.sleep(6)
    await client.stop()
    task.cancel()

    inv = {v: k for k, v in cfg.market_ids().items()}
    print("MKT    funding_rate   年化%      mark价       OI")
    print("-" * 62)
    for mid in sorted(snapshot.keys(), key=lambda x: int(x)):
        sym = inv.get(mid, mid)
        f = funding.get(mid, {})
        s = snapshot.get(mid, {})
        rate = f.get("rate", 0)
        # rate 单位需要确认：之前样例 BTC rate=-40, idx=836498，看起来是万分之几？
        # Perpl funding rate 可能是 scaled int（除以 1e6 或类似），这里先原样显示 + 假设万分比
        annual_pct = rate * 24 * 365 / 10000  # 假设 rate 单位是 0.01% (bps*0.01?)
        mark = s.get("mrk", 0)
        oi = s.get("oi", 0)
        print(f"{sym:<6} {rate:<14} {annual_pct:<10.2f} {mark:<12} {oi:<12}")


asyncio.run(main())
