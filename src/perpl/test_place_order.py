"""测试网全流程验证：连接 → 下单(MON 小额开空) → 确认 → 平仓

流程：
1. 连接 trading WS + sign-in
2. 拉 MON 当前价（market state，从 REST 或市场数据）
3. 发 OpenShort 单（MON market=64）
4. 等 StatusResponse + OrdersUpdate/Fills 确认
5. 发 CloseShort 平仓
6. 确认清仓

⚠️ 测试网，用虚拟资金，安全。
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
    """从市场数据 WS 拉 MON 当前 mark 价（scaled）"""
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
    """从 REST context 拿最新 head（state.at.b 实时）"""
    import urllib.request
    req = urllib.request.Request("https://testnet.perpl.xyz/api/v1/pub/context")
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.loads(r.read().decode())
    for m in d.get("markets", []):
        if m.get("id") == MON_MARKET:
            return m.get("state", {}).get("at", {}).get("b")
    return None


async def main():
    print("=== 测试网下单全流程 ===")
    # 1. 拉 MON 价格
    st = await get_mon_price()
    if not st:
        print("❌ 拿不到 MON 价格")
        return
    mark = st.get("mrk", 0)
    print(f"MON mark 价 (scaled, price_dec=5): {mark} = {mark/1e5:.5f}")

    # 2. 连接 trading WS
    auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
    client = PerplTradingClient(auth, ws_url=f"{PERPL_WS}/ws/v1/trading", chain_id=PERPL_CHAIN_ID)
    await client.connect()
    await client._read_until_snapshots(timeout=10)
    print(f"账户 ID: {client._account_id}, 余额: {client.wallet.get('as', [{}])[0].get('b')} raw")
    # WalletSnapshot 的 at.b = 服务器认为的最新 head（实时）
    head = client.wallet.get("at", {}).get("b", 0)
    print(f"服务器 head (WalletSnapshot at.b): {head}")
    print(f"已持仓: {client.positions}")

    # 3. 开空单：lb = 服务器 head + order_ttl_blocks(20)
    size = 10
    lb = head + 20
    print(f"\n>>> 开空 {size} MON (market {MON_MARKET}), 杠杆 3x, lb={lb}")
    frame = await client.place_order(
        market_id=MON_MARKET,
        order_type=2,          # OpenShort
        size=size,
        leverage_hundredths=300,  # 3x
        price_scaled=0,          # 市价
        flags=0,                 # GTC
        last_block=lb,
    )
    print(f"下单帧: {json.dumps(frame, ensure_ascii=False)}")

    # 4. 等确认（StatusResponse + Orders/Fills/Positions 更新）
    print("\n等待成交确认...")
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
                pass  # 继续监听一段时间
        except asyncio.TimeoutError:
            break

    print(f"\n当前持仓: {client.positions}")

    # 5. 平仓
    if client.positions:
        print("\n>>> 平仓")
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
                    print(f"  平仓后 PositionsUpdate: {fr.get('d')}")
                    break
            except asyncio.TimeoutError:
                break
    else:
        print("⚠️ 未持仓，跳过平仓")

    await client.close()
    print("\n✅ 测试完成")


asyncio.run(main())
