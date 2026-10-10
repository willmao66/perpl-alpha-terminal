"""Pull testnet MON/BTC/ETH market parameters"""
import requests

r = requests.get("https://testnet.perpl.xyz/api/v1/pub/context", timeout=15)
d = r.json()
for m in d.get("markets", []):
    if m.get("id") in (64, 16, 32):
        cfg = m.get("config", {})
        print(f"market {m.get('id')} {m.get('name')}: price_dec={cfg.get('price_decimals')} "
              f"size_dec={cfg.get('size_decimals')} init_margin={cfg.get('initial_margin')} "
              f"maker_fee={cfg.get('maker_fee')} taker_fee={cfg.get('taker_fee')}")
        print(f"  min_posting={cfg.get('min_posting_amount')} min_settle={cfg.get('min_settle_amount')} "
              f"is_open={cfg.get('is_open')}")
print(f"min_account_open: {d.get('min_account_open_amount')}")
