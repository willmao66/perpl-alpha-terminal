"""Query current positions and close them all (flatten to zero)"""
import asyncio
import json
import sys

sys.path.insert(0, ".")
from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_WS
from perpl_trader import PerplAuth, PerplTradingClient

MON_MARKET = 64


async def main():
    auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
    client = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading", chain_id=PERPL_CHAIN_ID)
    await client.connect()
    await client._read_until_snapshots(timeout=10)
    head = client.wallet.get("at", {}).get("b", 0)
    print(f"Account: {client._account_id} head: {head}")
    print(f"Initial positions: {client.positions}")

    # Close all MON positions
    pos = client.positions.get(MON_MARKET)
    if pos and pos.get("s", 0) > 0:
        size = pos["s"]
        print(f"\n>>> Close remaining {size} MON")
        await client.place_order(MON_MARKET, 4, size, leverage_hundredths=300, last_block=head + 20)
        for i in range(20):
            try:
                raw = await asyncio.wait_for(client._ws.recv(), timeout=8)
                fr = json.loads(raw)
                if fr.get("mt") == 27:
                    d = fr.get("d", [])
                    for p in d if isinstance(d, list) else []:
                        if p.get("mkt") == MON_MARKET:
                            print(f"  PositionsUpdate: size={p.get('s')} st={p.get('st')} sr={p.get('sr')} cpnl={p.get('cpnl')}")
                            if p.get("s", 0) == 0:
                                print("  ✅ Position cleared")
            except asyncio.TimeoutError:
                break
    else:
        print("No positions to close")

    await client.close()


asyncio.run(main())
