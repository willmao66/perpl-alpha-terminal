"""Print raw PositionsSnapshot frames + account balance"""
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
    for i in range(6):
        raw = await asyncio.wait_for(client._ws.recv(), timeout=10)
        fr = json.loads(raw)
        mt = fr.get("mt")
        if mt in (19, 26, 23):
            print(f"mt={mt}: {json.dumps(fr, ensure_ascii=False)}")
        if mt == 26:
            break
    await client.close()


asyncio.run(main())
