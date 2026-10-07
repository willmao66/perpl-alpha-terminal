"""查 trading WS 账户完整字段（确认 fr/fw/scope 相关）"""
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
    # 打印前 8 帧完整内容（含 wallet/account）
    for i in range(8):
        try:
            raw = await asyncio.wait_for(client._ws.recv(), timeout=8)
            frame = json.loads(raw)
            mt = frame.get("mt")
            print(f"\n=== mt={mt} ===")
            print(json.dumps(frame, ensure_ascii=False)[:2000])
            if mt == 21:  # AccountUpdate
                break
        except asyncio.TimeoutError:
            break
    await client.close()


asyncio.run(main())
