"""In-memory rolling feature engine (stateful per player)."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque

from .models import EnrichedEvent, PlayerFeatures

CASH_IN = {"CASH_IN", "BUY_IN", "CHIP_BUY", "WIRE_IN"}
CASH_OUT = {"CASH_OUT", "CHIP_REDEEM", "WIRE_OUT"}
CAGE = {"CASH_IN", "CASH_OUT", "WIRE_IN", "WIRE_OUT", "TITO_REDEEM"}
WAGER = {"GAMING_WAGER", "BLACKJACK", "SLOTS", "ROULETTE", "POKER", "WAGER"}


class FeatureEngine:
    def __init__(self) -> None:
        self._events: dict[str, Deque[EnrichedEvent]] = defaultdict(lambda: deque(maxlen=2000))

    def update(self, event: EnrichedEvent) -> PlayerFeatures:
        self._events[event.player_id].append(event)
        return self.snapshot(event.player_id, event.created_at)

    def snapshot(self, player_id: str, now: datetime | None = None) -> PlayerFeatures:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        window_1h = now - timedelta(hours=1)
        window_24h = now - timedelta(hours=24)
        window_30d = now - timedelta(days=30)

        feat = PlayerFeatures(player_id=player_id)
        locations: set[str] = set()
        methods: set[str] = set()
        cage_times: set[str] = set()

        for ev in self._events[player_id]:
            ts = ev.created_at
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts < window_30d:
                continue
            amt = float(ev.amount or 0)
            kind = ev.canonical_type
            feat.month_volume += amt

            if ts < window_24h:
                continue

            if kind in CASH_IN:
                feat.cash_in_24h += amt
            if kind in CASH_OUT:
                feat.cash_out_24h += amt
            if kind in {"WIRE_IN"}:
                feat.wire_in_24h += amt
            if kind in {"WIRE_OUT"}:
                feat.wire_out_24h += amt
            if kind in WAGER or kind in {"BLACKJACK", "SLOTS", "ROULETTE", "POKER"}:
                feat.total_wagered_24h += amt
            feat.txn_count_24h += 1
            if ev.location:
                locations.add(ev.location)
            if ev.payment_method:
                methods.add(ev.payment_method)
            if kind in CAGE and ev.location:
                cage_times.add(ev.location + ts.strftime("%Y%m%d%H"))
            if 8000 <= amt < 10000 and kind in CASH_OUT | CASH_IN:
                feat.just_below_10k_count_24h += 1

            if ts >= window_1h:
                feat.txn_count_1h += 1
                feat.max_single_1h = max(feat.max_single_1h, amt)
                if kind in CASH_IN:
                    feat.cash_in_1h += amt
                if kind in CASH_OUT:
                    feat.cash_out_1h += amt

        feat.unique_locations_24h = len(locations)
        feat.unique_payment_methods_24h = len(methods)
        feat.cage_visits_24h = max(len(cage_times), feat.just_below_10k_count_24h)
        return feat

    def to_dict(self, feat: PlayerFeatures) -> dict:
        return feat.model_dump()
