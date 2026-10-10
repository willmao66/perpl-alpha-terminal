"""Kuru spot leg ledger simulation (Phase 2) - testnet has no liquidity/test tokens, so switched to ledger mode

Background (decided on 2026-10-08, Option A):
- Perpl perpetual leg: **real execution** on testnet (has txid, real on-chain activity evidence)
- Kuru spot leg: **ledger simulation** (delta-neutral math is real, order logic is real, but no real orders placed)
- Reason: Kuru testnet MON-USDC order book empty + AMM vault empty (vaultAskOrderSize=0),
        Kuru testnet USDC cannot be minted, Circle faucet USDC not accepted by Kuru -> cannot fill in reality
- Competition narrative: make the design intent clear - the two-leg strategy is complete, the Kuru leg is
  demonstrated in simulation mode due to lack of testnet liquidity

Responsibilities:
- Maintain a virtual spot position (delta-neutral hedge = Perpl perpetual notional / mark price = MON quantity to buy)
- Record price = Perpl mark price (same underlying, perpetual and spot prices consistent, no need to separately fetch Kuru price)
- Record every open/close operation (virtual fill price, quantity, timestamp) - to demonstrate the "signal -> ledger -> position" timeline
- Calculate spot leg unrealized PnL + realized PnL (combined with perpetual leg funding income, evaluate strategy completeness)
"""
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger("perpl.kuru_ledger")


@dataclass
class SpotEntry:
    """A single spot open record (virtual)"""
    market: str
    ts: int                # timestamp ms
    price: float           # virtual fill price (Perpl mark price)
    notional_usd: float    # spot notional (hedge amount)
    amount: float          # MON quantity to buy = notional / price
    action: str = "OPEN"   # OPEN / CLOSE


@dataclass
class KuruLedgerState:
    """Kuru spot ledger state"""
    market: str = ""
    status: str = "FLAT"             # FLAT / HEDGED
    cost_basis: float = 0.0          # position cost basis (average price)
    amount: float = 0.0              # virtual position quantity (MON)
    notional: float = 0.0            # position notional
    last_price: float = 0.0          # latest market price (Perpl mark)
    unrealized_pnl: float = 0.0      # unrealized PnL (USD)
    realized_pnl: float = 0.0        # realized PnL (USD)
    opened_at: int = 0               # open timestamp
    entries: List[dict] = field(default_factory=list)  # operation log


class KuruSpotLedger:
    """Kuru spot leg ledger simulator

    Usage:
        ledger = KuruSpotLedger(spot_usd=params.spot_usd)
        ledger.open_spot("MON", mark_price, ts)   # open spot long in sync when the perpetual short opens
        ledger.update_price("MON", mark_price, ts) # per-frame market price update (compute unrealized PnL)
        ledger.close_spot("MON", mark_price, ts)   # close spot in sync when the perpetual leg closes
        snapshot = ledger.snapshot()               # ledger snapshot (JSON output)
    """

    def __init__(self, spot_usd: float = 0.0, min_record_amount: float = 1e-9):
        self.spot_usd = spot_usd          # default spot hedge amount (0 = not auto-configured)
        self.min_record_amount = min_record_amount
        self._states: Dict[str, KuruLedgerState] = {}
        self._ledger_file: Optional[str] = None   # persistence path (optional)

    # ── Core operations ──────────────────────────
    def open_spot(self, market: str, price: float, ts: int,
                  notional_usd: Optional[float] = None) -> SpotEntry:
        """Open a spot long (hedge the Perpl perpetual short). price = Perpl mark price (virtual fill price)"""
        notional = notional_usd if notional_usd is not None else self.spot_usd
        amount = notional / price if price > 0 else 0.0

        st = self._states.get(market)
        if st is None:
            st = KuruLedgerState(market=market)
            self._states[market] = st
        if st.status == "HEDGED":
            log.warning("[%s] Already holding, ignoring duplicate open (use rebalance to add)", market)
            return SpotEntry(market, ts, price, notional, amount, "SKIP")

        st.status = "HEDGED"
        st.cost_basis = price
        st.amount = amount
        st.notional = notional
        st.last_price = price
        st.opened_at = ts
        entry = SpotEntry(market, ts, price, notional, amount, "OPEN")
        st.entries.append(asdict(entry))
        log.info("[%s] Spot leg open (simulated): %.4f MON @ %s (notional %.2f U, delta-neutral hedge)",
                 market, amount, price, notional)
        self._flush()
        return entry

    def close_spot(self, market: str, price: float, ts: int) -> Optional[SpotEntry]:
        """Close the spot long (in sync with perpetual leg close). Records realized PnL."""
        st = self._states.get(market)
        if st is None or st.status != "HEDGED":
            log.warning("[%s] No spot position to close", market)
            return None

        # Realized PnL = (sell price - cost basis) x quantity (spot long, close by selling)
        realized = (price - st.cost_basis) * st.amount
        st.realized_pnl += realized
        entry = SpotEntry(market, ts, price, st.notional, st.amount, "CLOSE")
        st.entries.append(asdict(entry))
        log.info("[%s] Spot leg close (simulated): %.4f MON @ %s (realized %.4f U)",
                 market, st.amount, price, realized)

        # Reset to zero (including unrealized PnL - after close there is no position, floating PnL returns to zero, avoid double counting with realized)
        st.status = "FLAT"
        st.cost_basis = 0.0
        st.amount = 0.0
        st.notional = 0.0
        st.unrealized_pnl = 0.0
        self._flush()
        return entry

    # ── Market price update (compute unrealized PnL) ──
    def update_price(self, market: str, price: float, ts: int) -> Optional[dict]:
        """Per-frame market price update: update last_price + unrealized PnL (spot long: unrealized = (price - cost) x qty)"""
        st = self._states.get(market)
        if st is None:
            return None
        st.last_price = price
        if st.status == "HEDGED" and st.amount > 0:
            st.unrealized_pnl = (price - st.cost_basis) * st.amount
        return {"market": market, "price": price, "unrealized_pnl": st.unrealized_pnl}

    # ── Snapshot / persistence ────────────────
    def snapshot(self, market: Optional[str] = None) -> dict:
        """Ledger snapshot (all markets or a specific one). Empty state also returns full fields (avoid caller KeyError)"""
        if market:
            st = self._states.get(market)
            if st:
                return asdict(st)
            # Empty state: return a default snapshot with full fields
            return asdict(KuruLedgerState(market=market))
        return {m: asdict(s) for m, s in self._states.items()}

    def set_ledger_file(self, path: str) -> None:
        """Set the ledger persistence file (auto-written after every operation)"""
        self._ledger_file = path

    def _flush(self) -> None:
        if self._ledger_file:
            with open(self._ledger_file, "w", encoding="utf-8") as f:
                json.dump(self.snapshot(), f, ensure_ascii=False, indent=1)

    # ── Summary (combined with perpetual leg to evaluate strategy completeness) ──
    def summary(self) -> dict:
        """Full ledger summary: per-market state + realized/unrealized PnL + operation log entry count"""
        total_realized = sum(s.realized_pnl for s in self._states.values())
        total_unrealized = sum(s.unrealized_pnl for s in self._states.values())
        return {
            "markets": list(self._states.keys()),
            "total_realized_pnl": round(total_realized, 4),
            "total_unrealized_pnl": round(total_unrealized, 4),
            "total_pnl": round(total_realized + total_unrealized, 4),
            "detail": {m: asdict(s) for m, s in self._states.items()},
        }


# ── Simple demo batch for demonstration: feed a series of (price, ts) opens/closes ──
def demo():
    """Quick demo: simulate opening a market position -> price moves -> close, show the ledger output"""
    ledger = KuruSpotLedger(spot_usd=4000.0)
    # Simulate MON: open (0.02) -> price rises slightly (0.0202) -> close (0.0202)
    ledger.open_spot("MON", 0.02, 1700000000000)
    ledger.update_price("MON", 0.0202, 1700000060000)
    snap1 = ledger.snapshot("MON")
    ledger.close_spot("MON", 0.0202, 1700000120000)
    summ = ledger.summary()
    print(json.dumps({"open_snapshot": snap1, "summary": summ}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    demo()
