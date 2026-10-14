"""Shared numeric feature vector for CPU tabular models (XGBoost)."""

from __future__ import annotations

import math

from .models import EnrichedEvent, PlayerFeatures, PlayerProfile

FEATURE_NAMES = [
    "amount",
    "amount_log",
    "is_cash",
    "is_wire",
    "cash_in_24h",
    "cash_out_24h",
    "txn_count_1h",
    "txn_count_24h",
    "max_single_1h",
    "cage_visits_24h",
    "total_wagered_24h",
    "wager_to_buyin_ratio",
    "unique_locations_24h",
    "unique_payment_methods_24h",
    "just_below_10k_count_24h",
    "wire_in_24h",
    "wire_out_24h",
    "month_volume",
    "pep_flag",
    "watchlist_hit",
    "prior_alerts",
    "kyc_risk_ordinal",
]


def _kyc_ordinal(rating: str | None) -> int:
    return {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}.get((rating or "LOW").upper(), 0)


def vectorize(
    event: EnrichedEvent | None,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
    *,
    amount: float | None = None,
    canonical_type: str = "",
) -> list[float]:
    amt = float(amount if amount is not None else (event.amount if event else 0) or 0)
    kind = (canonical_type or (event.canonical_type if event else "") or "").upper()
    ratio = features.wager_to_buyin_ratio
    pep = bool(profile.pep_flag if profile else (event.pep_flag if event else False))
    watch = bool(profile.watchlist_hit if profile else (event.watchlist_hit if event else False))
    prior = int(profile.prior_alerts if profile else (event.prior_alerts if event else 0))
    kyc = (profile.kyc_risk_rating if profile else (event.kyc_risk_rating if event else "LOW"))
    return [
        amt,
        math.log10(max(amt, 1.0)),
        1.0 if kind in {"CASH_IN", "CASH_OUT"} or "CASH" in kind else 0.0,
        1.0 if "WIRE" in kind else 0.0,
        float(features.cash_in_24h),
        float(features.cash_out_24h),
        float(features.txn_count_1h),
        float(features.txn_count_24h),
        float(features.max_single_1h),
        float(features.cage_visits_24h),
        float(features.total_wagered_24h),
        float(ratio if ratio is not None else 0.0),
        float(features.unique_locations_24h),
        float(features.unique_payment_methods_24h),
        float(features.just_below_10k_count_24h),
        float(features.wire_in_24h),
        float(features.wire_out_24h),
        float(features.month_volume),
        1.0 if pep else 0.0,
        1.0 if watch else 0.0,
        float(prior),
        float(_kyc_ordinal(kyc)),
    ]
