"""检查各市场 funding 结算时间是否错开 + 各市场费率对比"""
import requests
import json
from datetime import datetime, timezone

r = requests.get("https://app.perpl.xyz/api/v1/pub/context", timeout=15)
d = r.json()
markets = d.get("markets", [])

print(f"{'MKT':<6}{'interval_sec':<14}{'funding_at.b':<14}{'rate':<8}{'orl':<12}{'mrk':<12}{'premium_bps':<12}")
print("-" * 78)
for m in sorted(markets, key=lambda x: x.get("id", 0)):
    name = m.get("name", "")
    if not name:
        continue
    f = m.get("funding", {})
    s = m.get("state", {})
    cfg = m.get("config", {})
    interval = m.get("funding_interval_sec", "?")
    at_b = f.get("at", {}).get("b", "?")
    rate = f.get("rate", 0)
    orl = s.get("orl", 0)
    mrk = s.get("mrk", 0)
    # 溢价 bps（永续 vs oracle）
    premium_bps = round((mrk - orl) / orl * 10000, 2) if orl else 0
    print(f"{name:<6}{interval:<14}{at_b:<14}{rate:<8}{orl:<12}{mrk:<12}{premium_bps:<12}")

# 汇总：funding at.b 是否有多个不同值（判断是否错开）
at_blocks = set()
for m in markets:
    f = m.get("funding", {})
    at_b = f.get("at", {}).get("b")
    if at_b:
        at_blocks.add(at_b)
print(f"\n不同 funding 结算区块数: {len(at_blocks)}")
print(f"结算区块: {sorted(at_blocks)}")
