# Perpl Alpha Terminal

**A production-grade quantitative trading terminal for Perpl perpetuals on Monad — automated carry strategies (funding arbitrage / basis) with delta-neutral spot hedging.**

Built for [Monad Metropolis Hackathon 2026](https://monad.xyz/metropolis) · Track: **Onchain Finance & Trading** · Bounties: **Perpl API · Perpl Analytics** (+ Agora, Kuru×2, Nansen, MetaMask)

> **Live product**: a bot is running 24/7 on a VPS against the **Perpl testnet**, placing real on-chain orders (verifiable txids) and collecting funding. See [Deployment](#deployment) and the **[live run report](docs/run-report-2026-10-09-1010.md)** (16.4h unattended run, 23 funding settlements).

---

## Why Monad? Why Perpl + Kuru?

Monad is the only chain where a **fully on-chain perpetual CLOB (Perpl)** and a **fully on-chain spot CLOB (Kuru)** coexist — the two legs required for delta-neutral **funding arbitrage** live on the same chain, composable and programmable in a single trust domain.

- **300ms blocks / 600ms finality / ~$0.0001 per order** → on-chain order books are economically viable for the first time
- **Perpl** = isolated-margin perpetuals CLOB (fully on-chain matching, no sequencer)
- **Kuru** = spot CLOB + hybrid AMM (the natural spot leg for funding arb)
- **The play**: short perps on Perpl (collect funding) + long spot on Kuru (hedge delta) → earn the funding rate while market-neutral

This is not a "port a DEX" project — it's the **missing quant infrastructure** for Monad's early funding market, where thin liquidity means wider rate differentials. On chains where Perpl/Kuru are the only venues, this data and execution layer is only-on-Monad by construction.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Data layer (live, 24/7)                                     │
│  Perpl WebSocket (market-state / funding / orderbook /       │
│  trades / candles) → JSONL persistence (history Perpl        │
│  does not archive)                                           │
└──────────────────────────┬──────────────────────────────────┘
                           │ feeds
┌──────────────────────────▼──────────────────────────────────┐
│  Strategy engine (funding arb)                               │
│  deep-premium detection → OPEN (short perp)                  │
│  funding turns negative / price deviates → CLOSE             │
└──────────────────────────┬──────────────────────────────────┘
                           │ decisions
┌──────────────────────────▼──────────────────────────────────┐
│  Execution layer (testnet, live)                             │
│  Perpl trading WS (Ed25519-signed) → real on-chain orders    │
│  Kuru spot leg (ledger-simulated on testnet — no liquidity)  │
│  → delta-neutral bookkeeping                                 │
└─────────────────────────────────────────────────────────────┘
```

## Current Status

**Phase 1 — Data layer: DONE & running (mainnet collector)**
- Real-time Perpl market data over official WebSocket (public, no auth)
- Streams: market-state (price/OI/TVL), funding, L2 order book, trades, candles
- JSONL persistence with local receive timestamps + heartbeat gap detection + auto-reconnect
- Verified: 2.5h continuous capture (102.9MB), funding updates observed every ~43min

**Phase 2 — Strategy & execution: DONE (testnet live)**
- Funding arb signal engine: deep-premium → open short; funding negative → close
- **Real on-chain execution on testnet**: Ed25519-signed orders via Perpl trading WS (txids on Monad testnet explorer), position confirmations, close cycle verified
- Kuru spot leg: ledger-simulated (testnet has no market liquidity — verified empirically; see below)
- Dual-leg paper trading verified against 2.5h of real funding data (7 markets triggered, correct funding accumulation)

**Phase 3 — Analytics terminal: PLANNED**
- Protocol-level + wallet-level risk dashboard (dark UI, real-time)

### Testnet liquidity reality (why Kuru is ledger-simulated)

We verified empirically: Perpl/Kuru **testnet** has essentially no liquidity (empty order book + empty AMM vault). The Perpl perp leg therefore runs with a small notional (~$20) that the thin book can absorb — enough to prove the full execution path with real txids. The Kuru spot leg is ledger-simulated (delta-neutral accounting at the Perpl mark price). On **mainnet** both legs execute for real; the architecture is identical.

---

## Deployment (Live product)

Runs on a VPS (Ubuntu 24.04) as a systemd service against Perpl **testnet**:

```bash
# on the VPS
git clone git@github.com:willmao66/perpl-alpha-terminal.git
cd src/perpl
python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt
# place config_local.py (API key from testnet.perpl.xyz/apikeys — gitignored)

# systemd unit (/etc/systemd/system/perpl-alpha-terminal.service)
[Unit]
Description=Perpl Alpha Terminal Bot (funding arb, testnet)
After=network-online.target
[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/perpl-alpha-terminal/src/perpl
ExecStart=/home/ubuntu/perpl-alpha-terminal/src/perpl/.venv/bin/python bot_runner.py --market MON --config config/bridge_test.json
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target

sudo systemctl enable --now perpl-alpha-terminal
journalctl -u perpl-alpha-terminal -f   # watch it trade
```

On startup the bot detects existing positions and resumes (no duplicate opens). WS auto-reconnects; systemd restarts on crash.

The service now runs the **multi-market rotation** bot (`bot_runner_multi.py`): it scans all candidate markets, opens the one with the best funding+premium score, holds while funding is healthy, and auto-rotates when a risk exit fires (funding < 5 micro / turns negative / +15% price deviation).

---

## Live Run Results

**16.4-hour unattended run** (2026-10-09 19:09 → 2026-10-10 11:36, Perpl testnet) — full report: [docs/run-report-2026-10-09-1010.md](docs/run-report-2026-10-09-1010.md)

| Metric | Value |
|--------|-------|
| Runtime (no restart) | 16.4 hours |
| Markets scanned | 15 |
| Auto-selected market | **ZEC** (only deep-premium venue; all others had funding ≤ 0) |
| Position | Short 16 ZEC @ 1221.999 (20U notional, 4x) |
| Funding settlements | **23** (~43 min cadence) |
| Funding collected | **+0.0122 USDC** |
| Risk exits | 0 (rate held at 30 micro, above the 5-micro exit threshold) |
| Implied annualized yield (holding-period) | ≈ **32.6%** |

This demonstrates the complete unattended loop: **auto-scan → auto-select → real on-chain order → funding collection → risk monitoring → auto-rotate**. On-chain position confirmed (sr=21), no crashes, no manual intervention.

---

## Repository Layout

```
src/perpl/
├─ perpl_config.py        # Network / market parameters (mainnet + testnet)
├─ perpl_ws_client.py     # WebSocket client (subscribe / frames / heartbeat / reconnect)
├─ perpl_collector.py     # Collector main loop (JSONL persistence)
├─ strategy_engine.py     # Funding arb signal engine + state machine
├─ kuru_ledger.py         # Kuru spot leg (ledger simulation, delta-neutral)
├─ bot_runner.py          # AlphaTerminalBot — data → strategy → execution bridge
├─ bot_runner_multi.py    # Multi-market rotation bot (FLAT scan → open best → risk exit → rotate)
├─ paper_trader.py        # Single-leg paper backtest
├─ paper_trader_dual.py   # Dual-leg paper backtest (perp + spot ledger)
├─ perpl_trader.py        # Perpl trading WS client (Ed25519 auth, order placement)
├─ config/                # Strategy params (JSON-driven, not hardcoded)
│  ├─ strategy_test.json  #   test scenario: 1000U / 4x / 4000U notional
│  ├─ bridge_test.json    #   VPS live: 5U / 4x (testnet thin book)
│  └─ vps_eth.json        #   VPS multi-market: 5U / 4x / 5micro exit
└─ requirements.txt
```

## Quick Start

```bash
python -m venv .venv
# Windows: $env:PYTHONUTF8="1"; $env:PYTHONIOENCODING="utf-8"
.\.venv\Scripts\python -m pip install -r requirements.txt

# Collect live market data (mainnet)
.\.venv\Scripts\python perpl_collector.py --markets BTC,ETH --data-dir data

# Backtest against collected data (dual-leg)
.\.venv\Scripts\python paper_trader_dual.py --date 2026-10-07

# Dry-run the live bot (no orders)
.\.venv\Scripts\python bot_runner.py --dry-run --market MON --config config/bridge_test.json

# Live (testnet, requires config_local.py with API key)
.\.venv\Scripts\python bot_runner.py --market MON --config config/bridge_test.json
```

## Key Facts (verified from official docs)

- WS market-data: `wss://app.perpl.xyz/ws/v1/market-data` (public); trading WS: `wss://app.perpl.xyz/ws/v1/trading` (Ed25519-signed)
- Market IDs (mainnet): BTC=1, MON=10, ETH=20, SOL=31, HYPE=40, ZEC=50
- Rate limits: ~50 msg/s per connection, ~5 connections/IP
- Perpl archives **no historical order-book / funding / OI** — only real-time streams. Historical microstructure must be self-collected. That's what this terminal does.
- One-Click Trading must be enabled in the Perpl UI before API orders are forwarded on-chain

## Roadmap

- [x] Phase 1: Perpl data collector (live, mainnet)
- [x] Phase 2: Funding arb strategy + delta-neutral execution (testnet live, txids)
- [ ] Phase 3: Analytics / risk dashboard
- [ ] Phase 4: HyperLiquid adapter (cross-venue funding comparison)

## License

MIT
