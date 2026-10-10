"""Perpl Alpha Terminal - Strategy Engine (Phase 2 core)

Low-frequency hold funding arb (decided on 2026-10-07/10-08):
- Direction: when funding > 0 deep premium, Perpl perpetual short + Kuru spot long (one-way, delta neutral)
- Hold: capture funding across multiple 43min settlement periods (low frequency, no high-frequency harvesting)
- Capital: 10000U = 2000U perpetual collateral (4x, 8000U notional) + 8000U spot hedge (decided on 10-08)
- Risk: one-way +15% (only the short-loss direction = price rise +15% -> close both legs and reopen; no threshold on the profit direction = price fall)
- Extra: when rate turns negative / drops below threshold -> close and exit (do not stubbornly hold)

Data input: collector real-time funding + market state (or simulated replay)
Decision output: JSON decision records (for paper trading / execution layer consumption)
"""
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger("perpl.strategy")


# ── Parameter config (configurable, not hard-coded) ──
@dataclass
class StrategyParams:
    # Signal params
    funding_threshold: float = 0.00002      # consider opening only when rate > 2bp per event
    premium_min_bps: float = 0.5            # real-time premium >= 0.5bp to confirm (prevent rate/premium divergence)
    # Risk-control params (decided on 2026-10-08: one-way +15%, only the short-loss direction = price rise triggers)
    leverage: float = 4.0                   # leverage multiplier (configurable)
    deviation_trigger: float = 0.15         # short-only direction (price rise) +15% -> close both legs and reopen
    funding_exit_threshold: float = 0.0     # exit when the rate while holding drops below this (leave when negative/unprofitable)
    # Capital params (test case: 1000U collateral / 4x / notional 4000U / spot hedge 4000U, decided on 10-08)
    collateral_usd: float = 1000.0          # perpetual collateral
    spot_usd: float = 0.0                   # spot hedge (0 = auto = notional = collateral x leverage)
    # Data source
    data_dir: str = "data"                  # collector data directory (for simulated replay)

    def __post_init__(self):
        """When spot_usd is not explicitly configured, auto-set to notional position (collateral x leverage) to guarantee delta neutrality"""
        if not self.spot_usd:
            self.spot_usd = self.collateral_usd * self.leverage

    @property
    def perp_notional(self) -> float:
        """Perpetual notional position = collateral x leverage"""
        return self.collateral_usd * self.leverage

    @classmethod
    def from_file(cls, path: str) -> "StrategyParams":
        """Load params from a JSON config file (project/tool usage, params not hard-coded)"""
        import json
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})


# ── State machine ──────────────────────────────────
class State:
    FLAT = "FLAT"              # flat, waiting for an opportunity
    HEDGED = "HEDGED"          # holding (Perpl short + Kuru long)
    TRIGGERED = "TRIGGERED"    # deviation exceeded, waiting for close confirmation
    REBALANCING = "REBALANCING"  # closing and reopening


@dataclass
class Position:
    market: str = ""
    entry_price: float = 0.0       # entry price (oracle/mark)
    entry_time: int = 0            # entry timestamp ms
    perp_size: float = 0.0         # Perpl perpetual short notional
    spot_size: float = 0.0         # Kuru spot long notional
    funding_collected: float = 0.0 # cumulative funding captured (simulated)
    last_funding_rate: float = 0.0
    funding_events: int = 0        # number of settlements captured


@dataclass
class Decision:
    """Strategy decision output (consumed by paper trading / execution layer)"""
    ts: int
    action: str                     # OPEN / CLOSE / HOLD / NOOP
    market: str
    reason: str
    price: float
    funding_rate: float
    premium_bps: float
    pos: Optional[dict] = None      # current position snapshot
    params_snapshot: Optional[dict] = None


class FundingArbStrategy:
    """Funding arb strategy engine (low-frequency hold)"""

    def __init__(self, params: Optional[StrategyParams] = None):
        self.p = params or StrategyParams()
        self.state = State.FLAT
        self.pos: Optional[Position] = None
        self.decisions: List[Decision] = []
        self._last_signals: Dict[str, dict] = {}   # market -> latest signal

    # ── Signal update (driven by data source) ──────
    def update_market(self, market: str, funding_rate: float,
                      oracle_price: float, mark_price: float, ts: int,
                      is_funding_event: bool = False) -> Optional[Decision]:
        """Called on every market data frame. is_funding_event=True means this is a funding settlement event.
        Returns a decision (None = no action)"""
        premium_bps = (mark_price - oracle_price) / oracle_price * 10000 if oracle_price else 0
        sig = {
            "ts": ts, "funding_rate": funding_rate, "oracle": oracle_price,
            "mark": mark_price, "premium_bps": premium_bps,
        }
        self._last_signals[market] = sig

        # State machine transition
        if self.state == State.FLAT:
            return self._eval_open(market, sig)
        elif self.state == State.HEDGED:
            return self._eval_hold(market, sig, is_funding_event)
        elif self.state == State.TRIGGERED:
            return self._eval_reopen(market, sig)
        return None

    # ── Open evaluation (FLAT -> HEDGED) ────────
    def _eval_open(self, market: str, sig: dict) -> Optional[Decision]:
        fr = sig["funding_rate"]
        if fr < self.p.funding_threshold:
            return None
        # Premium confirmation: rate high but already at discount -> rate will fall, do not chase (divergence protection)
        if sig["premium_bps"] < self.p.premium_min_bps:
            return None
        # Open
        self.state = State.HEDGED
        self.pos = Position(
            market=market,
            entry_price=sig["mark"],
            entry_time=sig["ts"],
            perp_size=self.p.perp_notional,   # notional = collateral x leverage
            spot_size=self.p.spot_usd,        # spot hedge
            last_funding_rate=fr,
        )
        d = Decision(sig["ts"], "OPEN", market, f"funding={fr:.6f} premium={sig['premium_bps']:.2f}bp deep premium open",
                     sig["mark"], fr, sig["premium_bps"], asdict(self.pos))
        self.decisions.append(d)
        return d

    # ── Position monitoring (HEDGED) ───────────
    def _eval_hold(self, market: str, sig: dict, is_funding_event: bool = False) -> Optional[Decision]:
        assert self.pos is not None
        # 1) Funding accumulation: only accumulate on funding settlement events (not every market_state frame)
        if is_funding_event:
            self.pos.funding_collected += self.pos.perp_size * sig["funding_rate"]
            self.pos.funding_events += 1
            self.pos.last_funding_rate = sig["funding_rate"]

        # 1.5) Close on negative rate: while holding, if funding drops below the threshold -> close and exit (do not stubbornly hold)
        if is_funding_event and sig["funding_rate"] < self.p.funding_exit_threshold:
            self.state = State.FLAT
            d = Decision(sig["ts"], "CLOSE", market,
                         f"Rate turned negative ({sig['funding_rate']:.6f}), closing (cumulative funding={self.pos.funding_collected:.2f})",
                         sig["mark"], sig["funding_rate"], sig["premium_bps"], asdict(self.pos))
            self.pos = None
            self.decisions.append(d)
            return d

        # 2) Deviation detection (one-way +15%): only the short-loss direction = price rise. No threshold on the price-fall (profit) direction
        #    Short MON: price rise -> perpetual leg loss, at +15% close both legs and reopen (recalibrate to a new baseline)
        dev_up = (sig["mark"] - self.pos.entry_price) / self.pos.entry_price
        if dev_up >= self.p.deviation_trigger:
            self.state = State.TRIGGERED
            d = Decision(sig["ts"], "CLOSE", market,
                         f"Price rose {dev_up*100:.1f}% >= +15% (short-loss direction), closing both legs and reopening (recalibrate baseline)",
                         sig["mark"], sig["funding_rate"], sig["premium_bps"], asdict(self.pos))
            self.decisions.append(d)
            return d
        return None

    # ── Reopen evaluation after close (TRIGGERED -> HEDGED/FLAT) ─
    def _eval_reopen(self, market: str, sig: dict) -> Optional[Decision]:
        """Reopen immediately after close (MVP): return to the new entry price baseline. v2: if funding has already fallen, do not reopen"""
        fr = sig["funding_rate"]
        if fr < self.p.funding_threshold or sig["premium_bps"] < self.p.premium_min_bps:
            # Opportunity has disappeared -> back to flat
            self.state = State.FLAT
            self.pos = None
            return Decision(sig["ts"], "NOOP", market,
                            "Opportunity disappeared after trigger close (funding fell), back to flat and wait",
                            sig["mark"], fr, sig["premium_bps"], None)
        # Reopen
        self.state = State.HEDGED
        self.pos = Position(
            market=market, entry_price=sig["mark"], entry_time=sig["ts"],
            perp_size=self.p.perp_notional,   # notional = collateral x leverage
            spot_size=self.p.spot_usd,        # spot hedge
            last_funding_rate=fr,
        )
        d = Decision(sig["ts"], "OPEN", market,
                     f"Reopen after deviation (new baseline {sig['mark']})", sig["mark"], fr,
                     sig["premium_bps"], asdict(self.pos))
        self.decisions.append(d)
        return d

    # ── State query ───────────────────────────
    def summary(self) -> dict:
        return {
            "state": self.state,
            "position": asdict(self.pos) if self.pos else None,
            "decision_count": len(self.decisions),
            "last_signal": self._last_signals,
        }
