"""查 API key 信息和 scope（read/trade）"""
import json
import sys
import urllib.request

sys.path.insert(0, ".")
from config_local import PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID, PERPL_REST
from perpl_trader import PerplAuth


def signed_get(path):
    auth = PerplAuth(PERPL_API_KEY, PERPL_API_KEY_SECRET, PERPL_CHAIN_ID)
    headers = auth.rest_headers("GET", path)
    req = urllib.request.Request(PERPL_REST + path, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:500]


for path in ["/v1/api-keys", "/v1/api-key", "/v1/auth/api-keys", "/v1/keys"]:
    status, body = signed_get(path)
    print(f"GET {path} -> {status}")
    if status == 200:
        print(f"  {body[:800]}")
        break
