"""Stateful sliding-window feature engine (5m/1h/24h/7d/30d).

Includes cage-native fields ported from demojam for CTR / structuring / wire / TITO.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, Set

from casino_common.models import FeatureVector


WINDOWS = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}

CTR = 10_000.0
STRUCTURING_LOW = 8_000.0
STRUCTURING_HIGH = 9_999.0


@dataclass
class TxEvent:
    ts: datetime
    amount: float
    tx_type: str
    payment_method: str = ""
    location: str = ""
    is_wager: bool = False
    is_chip: bool = False
    is_cage: bool = False
    is_cash_in: bool = False
    is_cash_out: bool = False
    is_wire_in: bool = False
    is_just_below: bool = False
    geo_km: float = 0.0


@dataclass
class PlayerState:
    events: Deque[TxEvent] = field(default_factory=deque)
    shared_device_count: float = 1.0
    account_age_days: float = 30.0
    is_pep: float = 0.0
    watchlist_hit: float = 0.0


class FeatureEngine:
    def __init__(self) -> None:
        self._players: Dict[str, PlayerState] = defaultdict(PlayerState)

    def update_static(self, player_id: str, **kwargs: float) -> None:
        st = self._players[player_id]
        for k, v in kwargs.items():
            if hasattr(st, k):
                setattr(st, k, float(v))

    def ingest(self, player_id: str, event: dict[str, Any]) -> FeatureVector:
        st = self._players[player_id]
        ts = _parse_ts(event.get("timestamp"))
        amount = float(event.get("amount") or 0)
        tx_type = str(event.get("transaction_type") or event.get("event_type") or "UNKNOWN").upper()
        payment = str(event.get("payment_method") or event.get("channel") or "").upper()
        location = str(event.get("location") or event.get("cage_id") or "")
        is_cash = tx_type in ("CASH_IN", "CASH_OUT") or payment in ("CASH", "CAGE")
        et = TxEvent(
            ts=ts,
            amount=amount,
            tx_type=tx_type,
            payment_method=payment or tx_type,
            location=location,
            is_wager=tx_type in ("WAGER",) or event.get("event_type") == "WAGER",
            is_chip=bool(event.get("is_chip") or "CHIP" in tx_type),
            is_cage=bool(event.get("is_cage") or "CAGE" in tx_type or payment == "CAGE"),
            is_cash_in=tx_type in ("CASH_IN", "DEPOSIT", "BUY_IN") and is_cash,
            is_cash_out=tx_type in ("CASH_OUT", "WITHDRAWAL") and is_cash,
            is_wire_in=tx_type == "WIRE_IN" or payment == "WIRE",
            is_just_below=is_cash and STRUCTURING_LOW <= amount <= STRUCTURING_HIGH,
            geo_km=float(event.get("geo_distance_km") or 0),
        )
        # Treat generic cage deposits as cash-in for CTR features when channel is CAGE
        if tx_type in ("DEPOSIT", "BUY_IN") and payment in ("CASH", "CAGE", ""):
            if payment in ("CASH", "CAGE") or not payment:
                et.is_cash_in = et.is_cash_in or payment in ("CASH", "CAGE")
        if tx_type in ("WITHDRAWAL", "CASH_OUT") and payment in ("CASH", "CAGE"):
            et.is_cash_out = True

        st.events.append(et)
        self._prune(st, ts)

        if event.get("shared_device_count") is not None:
            st.shared_device_count = float(event["shared_device_count"])
        if event.get("is_pep") is not None:
            st.is_pep = 1.0 if event.get("is_pep") else 0.0
        if event.get("watchlist_hit") is not None:
            st.watchlist_hit = 1.0 if event.get("watchlist_hit") else 0.0

        windows = {name: self._window_features(st, ts, delta) for name, delta in WINDOWS.items()}
        fv = FeatureVector(
            player_id=player_id,
            transaction_id=event.get("transaction_id"),
            event_id=str(event.get("event_id") or event.get("transaction_id") or ts.isoformat()),
            correlation_id=str(event.get("correlation_id") or player_id),
            timestamp=ts,
            window_5m=windows["5m"],
            window_1h=windows["1h"],
            window_24h=windows["24h"],
            window_7d=windows["7d"],
            window_30d=windows["30d"],
            static={
                "shared_device_count": st.shared_device_count,
                "account_age_days": st.account_age_days,
                "is_pep": st.is_pep,
                "watchlist_hit": st.watchlist_hit,
                "avg_tx_amount": windows["30d"].get("avg_tx_amount", 0.0),
            },
        )
        return fv

    def _prune(self, st: PlayerState, now: datetime) -> None:
        cutoff = now - WINDOWS["30d"]
        while st.events and st.events[0].ts < cutoff:
            st.events.popleft()

    def _window_features(self, st: PlayerState, now: datetime, delta: timedelta) -> dict[str, float]:
        start = now - delta
        txs = [e for e in st.events if e.ts >= start]
        deposits = [e for e in txs if e.tx_type in ("DEPOSIT", "BUY_IN", "CASH_IN")]
        withdrawals = [e for e in txs if e.tx_type in ("WITHDRAWAL", "CASH_OUT")]
        wagers = [e for e in txs if e.is_wager]
        chips = [e for e in txs if e.is_chip]
        cages = [e for e in txs if e.is_cage]
        amounts = [e.amount for e in txs]
        cash_in = [e for e in txs if e.is_cash_in]
        cash_out = [e for e in txs if e.is_cash_out]
        wire_in = [e for e in txs if e.is_wire_in]
        just_below = [e for e in txs if e.is_just_below]
        methods: Set[str] = {e.payment_method for e in txs if e.payment_method}
        # Cage visit = unique location + hour bucket
        visit_keys = {
            f"{e.location}|{e.ts.strftime('%Y%m%d%H')}"
            for e in txs
            if e.location or e.is_cage or e.payment_method in ("CAGE", "CASH")
        }
        cash_in_sum = float(sum(e.amount for e in cash_in))
        wire_in_sum = float(sum(e.amount for e in wire_in))
        wager_sum = float(sum(e.amount for e in wagers))
        buyin = max(cash_in_sum, wire_in_sum, float(sum(e.amount for e in deposits)), 0.0)
        return {
            "tx_count": float(len(txs)),
            "tx_sum": float(sum(amounts)),
            "avg_tx_amount": float(sum(amounts) / len(amounts)) if amounts else 0.0,
            "max_tx_amount": float(max(amounts)) if amounts else 0.0,
            "deposit_count": float(len(deposits)),
            "deposit_sum": float(sum(e.amount for e in deposits)),
            "withdrawal_count": float(len(withdrawals)),
            "withdrawal_sum": float(sum(e.amount for e in withdrawals)),
            "wager_count": float(len(wagers)),
            "wager_sum": wager_sum,
            "chip_sum": float(sum(e.amount for e in chips)),
            "cage_sum": float(sum(e.amount for e in cages)),
            "geo_distance_km": float(max((e.geo_km for e in txs), default=0.0)),
            "unique_channels": float(len(methods) or 1),
            "cash_in_sum": cash_in_sum,
            "cash_out_sum": float(sum(e.amount for e in cash_out)),
            "wire_in_sum": wire_in_sum,
            "just_below_10k_count": float(len(just_below)),
            "cage_visits": float(len(visit_keys)),
            "unique_payment_methods": float(len(methods)),
            "wager_to_buyin_ratio": float(wager_sum / buyin) if buyin > 0 else 1.0,
        }


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.now(timezone.utc)
