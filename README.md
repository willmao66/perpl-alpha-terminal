# Perpl Alpha Terminal

**A production-grade quantitative trading terminal for Perpl perpetuals on Monad — automated carry strategies (funding arbitrage / basis) with a protocol & wallet-level risk analytics layer.**

Built for [Monad Metropolis Hackathon 2026](https://monad.xyz/metropolis) · Track: **Onchain Finance & Trading**

---

## Why Monad? Why Perpl + Kuru?

Monad is the only chain where a **fully on-chain perpetual CLOB (Perpl)** and a **fully on-chain spot CLOB (Kuru)** coexist — the two legs required for delta-neutral **funding arbitrage** live on the same chain, composable and programmable in a single trust domain.

- **300ms blocks / 600ms finality / ~$0.0001 per order** → on-chain market making is economically viable for the first time
- **Perpl** = isolated-margin perpetuals CLOB (fully on-chain matching, no sequencer)
- **Kuru** = spot CLOB + hybrid AMM (the natural spot leg for funding arb)
- **The play**: short perps on Perpl (collect funding) + long spot on Kuru (hedge delta) → earn funding rate while market-neutral

This is not a "port a DEX" project — it's the **missing quant infrastructure** for Monad's early funding market, where thin liquidity means wider rate differentials.

## Current Status (Build in Progress)

**Phase 1 — Data layer (DONE, running):**
- Real-time Perpl market data collector over official WebSocket (public, no auth)
- Streams: market state (price/OI/TVL), funding rate, L2 order book, trades, candles
- JSONL persistence with local receive timestamps (history that Perpl does not archive)
- Heartbeat gap detection + auto-reconnect, designed for 24/7 VPS operation
- Verified live: 65 frames / 8s from mainnet (Sep 24, 2026)

**Phase 2 — Strategy & execution (IN PROGRESS):**
- Funding arb signal engine (funding premium / OI change / order-book imbalance)
- Delta-neutral position management (Perpl perp leg + Kuru spot leg)
- EVM contract-direct execution (no API rate limits; gas ≈ $0.0001)

**Phase 3 — Analytics terminal (planned):**
- Protocol-level + wallet-level risk dashboard (dark UI, real-time)

## Repository Layout

```
src/perpl/
├─ perpl_config.py        # Network / market parameters (mainnet + testnet)
├─ perpl_ws_client.py     # WebSocket client (subscribe / frames / heartbeat / reconnect)
├─ perpl_collector.py     # Collector main loop (timestamp + JSONL persistence)
└─ test_ws_connect.py     # Connectivity smoke test
```

## Quick Start

```bash
# Setup (Windows note: set UTF-8 env)
python -m venv .venv
$env:PYTHONUTF8="1"; $env:PYTHONIOENCODING="utf-8"
.\.venv\Scripts\python -m pip install -r requirements.txt

# Collect live market data (mainnet)
.\.venv\Scripts\python perpl_collector.py --markets BTC,ETH --data-dir data

# Testnet
.\.venv\Scripts\python perpl_collector.py --testnet --data-dir data_test
```

## Key Facts (verified from official docs)

- WS endpoint: `wss://app.perpl.xyz/ws/v1/market-data` (no auth)
- Market IDs (mainnet): BTC=1, MON=10, ETH=20, SOL=31, HYPE=40, ZEC=50
- Rate limits: ~50 msg/s per connection, ~5 connections/IP
- Perpl archives **no historical order-book / funding / OI** — only real-time streams + K-line REST. Historical microstructure must be self-collected. That's what this terminal does.

## Roadmap

- [x] Phase 1: Perpl data collector (live)
- [ ] Phase 2: Funding arb strategy + delta-neutral execution (Perpl + Kuru legs)
- [ ] Phase 3: Analytics / risk dashboard
- [ ] Phase 4: HyperLiquid adapter (cross-venue funding comparison)

## License

MIT
