"""Full testnet verification: open short MON -> confirm position -> close short -> confirm flatten

Verify the full flow (after One-Click Trading is enabled):
1. Connect trading WS + sign-in
2. Pull MON price + head
3. Open short 10 MON (3x)
4. Wait for fill + confirm position
5. Close position
6. Confirm flatten
"""
import asyncio
import json
import sys

sys.path.insert(0, ".")
from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_WS
from perpl_trader import PerplAuth, PerplTradingClient

MON_MARKET = 64


async def get_mon_price():
    import websockets
    async with websockets.connect("wss://testnet.perpl.xyz/ws/v1/market-data") as ws:
        await ws.send(json.dumps({
            "mt": 5, "subs": [{"stream": "market-state@10143", "subscribe": True}]
        }))
        for _ in range(5):
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            frame = json.loads(raw)
            if frame.get("mt") == 9:
                st = (frame.get("d") or {}).get(str(MON_MARKET))
                if st:
                    return st
    return None


async def main():
    print("=== Full testnet verification: open short -> position -> close ===")
    st = await get_mon_price()
    if not st:
        print("❌ Cannot get MON price")
        return
    print(f"MON mark: {st.get('mrk')} = {st.get('mrk')/1e5:.5f}")

    auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
    client = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading", chain_id=PERPL_CHAIN_ID)
    await client.connect()
    await client._read_until_snapshots(timeout=10)
    head = client.wallet.get("at", {}).get("b", 0)
    print(f"Account: {client._account_id} head: {head}")

    # ── Open short ──
    size = 10
    lb = head + 20
    print(f"\n>>> Open short {size} MON (3x)")
    await client.place_order(MON_MARKET, 2, size, leverage_hundredths=300, last_block=lb)

    # Listen until a position appears
    pos_seen = False
    for i in range(20):
        try:
            raw = await asyncio.wait_for(client._ws.recv(), timeout=8)
            fr = json.loads(raw)
            mt = fr.get("mt")
            if mt == 27:
                d = fr.get("d", [])
                for p in d if isinstance(d, list) else []:
                    if p.get("mkt") == MON_MARKET and p.get("s", 0) > 0:
                        print(f"  ✅ Position opened: size={p['s']} ep={p.get('ep')} sd={p.get('sd')} st={p.get('st')} sr={p.get('sr')}")
                        pos_seen = True
        except asyncio.TimeoutError:
            break
    if not pos_seen:
        print("  ⚠️ No position detected")

    # ── Close position ──
    print(f"\n>>> Close {size} MON")
    await client.place_order(MON_MARKET, 4, size, leverage_hundredths=300, last_block=lb + 100)

    closed = False
    for i in range(20):
        try:
            raw = await asyncio.wait_for(client._ws.recv(), timeout=8)
            fr = json.loads(raw)
            mt = fr.get("mt")
            if mt == 27:
                d = fr.get("d", [])
                for p in d if isinstance(d, list) else []:
                    if p.get("mkt") == MON_MARKET and p.get("s", 0) == 0:
                        print(f"  ✅ Position closed: st={p.get('st')} sr={p.get('sr')}")
                        closed = True
        except asyncio.TimeoutError:
            break
    if not closed:
        print("  ⚠️ Close not confirmed (may already be closed but not captured)")

    await client.close()
    print("\n✅ Full verification complete")


asyncio.run(main())
