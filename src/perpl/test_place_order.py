"""Full testnet flow verification: connect -> place order (small MON short) -> confirm -> close

Flow:
1. Connect trading WS + sign-in
2. Pull MON current price (market state, from REST or market data)
3. Send OpenShort order (MON market=64)
4. Wait for StatusResponse + OrdersUpdate/Fills confirmation
5. Send CloseShort to close position
6. Confirm flatten

⚠️ Testnet, uses virtual funds, safe.
"""
import asyncio
import json
import sys
import time

sys.path.insert(0, ".")
from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_WS
from perpl_trader import PerplAuth, PerplTradingClient

MON_MARKET = 64   # testnet MON


async def get_mon_price():
    """Pull MON current mark price from the market-data WS (scaled)"""
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


async def get_head_block():
    """Get the latest head from REST context (state.at.b, live)"""
    import urllib.request
    req = urllib.request.Request("https://testnet.perpl.xyz/api/v1/pub/context")
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.loads(r.read().decode())
    for m in d.get("markets", []):
        if m.get("id") == MON_MARKET:
            return m.get("state", {}).get("at", {}).get("b")
    return None


async def main():
    print("=== Full testnet order flow ===")
    # 1. Pull MON price
    st = await get_mon_price()
    if not st:
        print("❌ Cannot get MON price")
        return
    mark = st.get("mrk", 0)
    print(f"MON mark price (scaled, price_dec=5): {mark} = {mark/1e5:.5f}")

    # 2. Connect trading WS
    auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
    client = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading", chain_id=PERPL_CHAIN_ID)
    await client.connect()
    await client._read_until_snapshots(timeout=10)
    print(f"Account ID: {client._account_id}, balance: {client.wallet.get('as', [{}])[0].get('b')} raw")
    # WalletSnapshot at.b = the latest head as the server sees it (live)
    head = client.wallet.get("at", {}).get("b", 0)
    print(f"Server head (WalletSnapshot at.b): {head}")
    print(f"Current positions: {client.positions}")

    # 3. Open short order: lb = server head + order_ttl_blocks(20)
    size = 10
    lb = head + 20
    print(f"\n>>> Open short {size} MON (market {MON_MARKET}), leverage 3x, lb={lb}")
    frame = await client.place_order(
        market_id=MON_MARKET,
        order_type=2,          # OpenShort
        size=size,
        leverage_hundredths=300,  # 3x
        price_scaled=0,          # market price
        flags=0,                 # GTC
        last_block=lb,
    )
    print(f"Order frame: {json.dumps(frame, ensure_ascii=False)}")

    # 4. Wait for confirmation (StatusResponse + Orders/Fills/Positions updates)
    print("\nWaiting for fill confirmation...")
    for i in range(15):
        try:
            raw = await asyncio.wait_for(client._ws.recv(), timeout=8)
            fr = json.loads(raw)
            mt = fr.get("mt")
            if mt == 3:
                st_code = fr.get("status", {}).get("code")
                err = fr.get("status", {}).get("error", "")
                print(f"  StatusResponse: code={st_code} {err}")
            elif mt == 24:
                print(f"  OrdersUpdate: {json.dumps(fr.get('d', []), ensure_ascii=False)[:200]}")
            elif mt == 25:
                print(f"  FillsUpdate: {json.dumps(fr.get('d', []), ensure_ascii=False)[:200]}")
            elif mt == 26:
                print(f"  PositionsSnapshot: {fr.get('d')}")
            elif mt == 27:
                print(f"  PositionsUpdate: {fr.get('d')}")
            if mt in (24, 25, 27):
                pass  # keep listening for a while
        except asyncio.TimeoutError:
            break

    print(f"\nCurrent positions: {client.positions}")

    # 5. Close position
    if client.positions:
        print("\n>>> Close position")
        await client.place_order(
            market_id=MON_MARKET,
            order_type=4,        # CloseShort
            size=size,
            leverage_hundredths=300,
            price_scaled=0,
            flags=0,
            last_block=lb,
        )
        for i in range(10):
            try:
                raw = await asyncio.wait_for(client._ws.recv(), timeout=8)
                fr = json.loads(raw)
                mt = fr.get("mt")
                if mt == 27:
                    print(f"  PositionsUpdate after close: {fr.get('d')}")
                    break
            except asyncio.TimeoutError:
                break
    else:
        print("⚠️ No position held, skipping close")

    await client.close()
    print("\n✅ Test complete")


asyncio.run(main())
