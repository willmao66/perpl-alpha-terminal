"""Check whether funding settlement times across markets are staggered + compare funding rates across markets"""
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
    # premium bps (perpetual vs oracle)
    premium_bps = round((mrk - orl) / orl * 10000, 2) if orl else 0
    print(f"{name:<6}{interval:<14}{at_b:<14}{rate:<8}{orl:<12}{mrk:<12}{premium_bps:<12}")

# Summary: whether funding at.b has multiple distinct values (to see if they're staggered)
at_blocks = set()
for m in markets:
    f = m.get("funding", {})
    at_b = f.get("at", {}).get("b")
    if at_b:
        at_blocks.add(at_b)
print(f"\nDistinct funding settlement blocks: {len(at_blocks)}")
print(f"Settlement blocks: {sorted(at_blocks)}")
