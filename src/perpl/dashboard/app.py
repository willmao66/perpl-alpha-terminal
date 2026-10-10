"""Perpl Alpha Terminal Dashboard API (read-only)

- Read-only: no write/operation endpoints (operations go through CLI, see repo README)
- Data source: files persisted by the running bot (bot_state_*.json / bot_ops_*.jsonl)
- Background task: every 60s reads the latest bot_state, appends to history.jsonl (PnL curve)

API:
    GET /api/status   -> run status / position / pnl summary
    GET /api/history  -> pnl time series (curve)
    GET /api/ops      -> execution / risk log
"""
import asyncio
import glob
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("perpl.dash")

# ── config ─────────────────────────────────────
DATA_DIR = Path(os.environ.get("PERPL_DATA_DIR", "/home/ubuntu/perpl-alpha-terminal/src/perpl/data"))
HISTORY_FILE = DATA_DIR / "history.jsonl"
HISTORY_INTERVAL = float(os.environ.get("PERPL_HISTORY_INTERVAL", "60"))  # seconds
STATIC_DIR = Path(os.environ.get("PERPL_STATIC_DIR", Path(__file__).parent))

app = FastAPI(title="Perpl Alpha Terminal Dashboard", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── data reading ─────────────────────────────────
def latest_state_file() -> Optional[Path]:
    files = sorted(glob.glob(str(DATA_DIR / "bot_state_*.json")))
    if not files:
        return None
    # most recently modified (60s periodic overwrite)
    return max((Path(f) for f in files), key=lambda p: p.stat().st_mtime)


def read_latest_state() -> Optional[dict]:
    f = latest_state_file()
    if not f:
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("failed to read state file %s: %s", f.name, e)
        return None


def read_ops() -> list:
    """Read all bot_ops_*.jsonl, sorted by time"""
    ops = []
    for f in sorted(glob.glob(str(DATA_DIR / "bot_ops_*.jsonl"))):
        try:
            for line in f and Path(f).read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    ops.append(json.loads(line))
        except Exception as e:
            log.warning("failed to read ops %s: %s", f, e)
    ops.sort(key=lambda x: x.get("ts", 0))
    return ops


def read_history() -> list:
    if not HISTORY_FILE.exists():
        return []
    rows = []
    try:
        for line in HISTORY_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    except Exception as e:
        log.warning("failed to read history: %s", e)
    return rows


def ts_to_str(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M") if ts else ""


# ── summary computation ─────────────────────────────────
def compute_status(state: Optional[dict]) -> dict:
    if not state:
        return {"online": False, "error": "no bot state file found"}

    strategy = state.get("strategy", {})
    pos = strategy.get("position")
    ledger = state.get("kuru_ledger", {})

    # uptime: infer from run_id (YYYYMMDD_HHMMSS)
    run_id = state.get("run_id", "")
    started_ts = None
    try:
        started_ts = datetime.strptime(run_id, "%Y%m%d_%H%M%S").timestamp() * 1000
    except Exception:
        pass

    funding_collected = pos.get("funding_collected", 0) if pos else 0
    funding_events = pos.get("funding_events", 0) if pos else 0
    last_rate = pos.get("last_funding_rate", 0) if pos else 0

    # annualized estimate: funding_collected / notional / hours * 8760
    annualized = 0.0
    notional = state.get("params", {}).get("collateral_usd", 0) * state.get("params", {}).get("leverage", 0)
    if started_ts and funding_collected > 0 and notional > 0:
        hours = max((datetime.now().timestamp() * 1000 - started_ts) / 3600000, 1 / 3600)
        annualized = funding_collected / notional / hours * 8760 * 100  # %

    return {
        "online": True,
        "run_id": run_id,
        "network": state.get("network"),
        "market": state.get("market"),
        "dry_run": state.get("dry_run"),
        "started_ts": started_ts,
        "started_at": ts_to_str(int(started_ts)) if started_ts else "",
        "uptime_hours": round((datetime.now().timestamp() * 1000 - started_ts) / 3600000, 1) if started_ts else 0,
        "strategy_state": strategy.get("state"),
        "position": {
            "market": pos.get("market"),
            "entry_price": pos.get("entry_price"),
            "perp_size": pos.get("perp_size"),
            "spot_size": pos.get("spot_size"),
            "last_funding_rate": last_rate,
            "funding_collected": funding_collected,
            "funding_events": funding_events,
            "unrealized_pnl": ledger.get("total_unrealized_pnl", 0),
            "realized_pnl": ledger.get("total_realized_pnl", 0),
        } if pos else None,
        "ledger": {
            "total_realized_pnl": ledger.get("total_realized_pnl", 0),
            "total_unrealized_pnl": ledger.get("total_unrealized_pnl", 0),
            "total_pnl": ledger.get("total_pnl", 0),
            "markets": ledger.get("markets", []),
        },
        "params": {
            "collateral_usd": state.get("params", {}).get("collateral_usd"),
            "leverage": state.get("params", {}).get("leverage"),
            "notional": notional,
            "funding_threshold": state.get("params", {}).get("funding_threshold"),
            "funding_exit_threshold": state.get("params", {}).get("funding_exit_threshold"),
        },
        "annualized_pct": round(annualized, 1),
        "markets_watched": state.get("markets_watched"),
        "head": state.get("head"),
        "updated_at": ts_to_str(int(datetime.now().timestamp() * 1000)),
    }


def compute_history() -> dict:
    rows = read_history()
    return {
        "points": [
            {
                "ts": r.get("ts"),
                "time": ts_to_str(r.get("ts", 0)),
                "funding_collected": r.get("funding_collected", 0),
                "funding_events": r.get("funding_events", 0),
                "market": r.get("market"),
                "mark": r.get("mark"),
                "annualized_pct": r.get("annualized_pct", 0),
            }
            for r in rows
        ]
    }


# ── background task: history accumulation ───────────────────
async def history_saver():
    log.info("history accumulation task started (every %ss)", HISTORY_INTERVAL)
    while True:
        try:
            state = read_latest_state()
            if state:
                strategy = state.get("strategy", {})
                pos = strategy.get("position")
                row = {
                    "ts": int(datetime.now().timestamp() * 1000),
                    "market": state.get("market"),
                    "strategy_state": strategy.get("state"),
                    "funding_collected": pos.get("funding_collected", 0) if pos else 0,
                    "funding_events": pos.get("funding_events", 0) if pos else 0,
                    "last_funding_rate": pos.get("last_funding_rate", 0) if pos else 0,
                    "mark": pos.get("entry_price", 0) if pos else 0,
                    "annualized_pct": compute_status(state).get("annualized_pct", 0),
                }
                with open(HISTORY_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            log.error("history accumulation failed: %s", e)
        await asyncio.sleep(HISTORY_INTERVAL)


@app.on_event("startup")
async def startup():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    asyncio.create_task(history_saver())


# ── API ──────────────────────────────────────
@app.get("/api/status")
async def api_status():
    return compute_status(read_latest_state())


@app.get("/api/history")
async def api_history():
    return compute_history()


@app.get("/api/ops")
async def api_ops(limit: int = 100):
    ops = read_ops()
    # newest first, take the most recent `limit` entries
    ops.reverse()
    return {"total": len(ops), "ops": ops[:limit]}


# static frontend (mount is optional when /perpl/ is served directly by nginx; kept for local dev)
if (STATIC_DIR / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
