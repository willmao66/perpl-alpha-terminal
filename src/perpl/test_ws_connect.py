"""Quick connectivity test: connect to Perpl WS for 8 seconds, print frame-type stats + a sample of each type"""
import asyncio
import json
import sys
import time

sys.path.insert(0, ".")
from perpl_config import CollectorConfig, MT
from perpl_ws_client import PerplWSClient

MT_REV = {v: k for k, v in MT.items()}

async def test():
    cfg = CollectorConfig(markets=["BTC", "ETH"], data_dir="data_test")
    counts = {}
    samples = {}
    first_ts = None

    async def on_msg(frame):
        nonlocal first_ts
        if first_ts is None:
            first_ts = time.time()
        mt = frame.get("mt")
        counts[mt] = counts.get(mt, 0) + 1
        if mt not in samples:
            samples[mt] = frame

    client = PerplWSClient(cfg, on_msg)
    task = asyncio.create_task(client.run())
    await asyncio.sleep(8)
    await client.stop()
    task.cancel()

    print(f"\n=== Received {sum(counts.values())} frames in 8 seconds ===")
    for mt, n in sorted(counts.items()):
        name = MT_REV.get(mt, f"mt{mt}")
        print(f"  {name:<28} x{n}")
    print("\n=== Samples ===")
    for mt, frame in samples.items():
        name = MT_REV.get(mt, f"mt{mt}")
        preview = json.dumps(frame, ensure_ascii=False)
        print(f"\n[{name}] {preview[:300]}")

asyncio.run(test())
