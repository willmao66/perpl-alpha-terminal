"""调试：查看 WalletSnapshot 完整结构 + 找正确 REST 端点"""
import asyncio
import json
import sys

sys.path.insert(0, ".")
from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_WS
from perpl_trader import PerplAuth, PerplTradingClient


async def main():
    auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
    client = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading", chain_id=PERPL_CHAIN_ID)
    await client.connect()
    # 手动读帧，打印前几个完整结构
    for i in range(6):
        try:
            raw = await asyncio.wait_for(client._ws.recv(), timeout=10)
            frame = json.loads(raw)
            print(f"\n=== 帧 {i} (mt={frame.get('mt')}) ===")
            print(json.dumps(frame, indent=1, ensure_ascii=False)[:1200])
            if frame.get("mt") == 26:  # PositionsSnapshot
                break
        except asyncio.TimeoutError:
            print("timeout")
            break
    await client.close()


asyncio.run(main())
