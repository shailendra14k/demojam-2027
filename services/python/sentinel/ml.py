"""Monolith helpers: score via ``AMLModel`` (B feature schema + model/artifacts)."""

from __future__ import annotations

import logging

from .aml_model import AMLModel
from .models import EnrichedEvent, PlayerFeatures, PlayerProfile

log = logging.getLogger("sentinel.ml")

_model: AMLModel | None = None


def _get_model() -> AMLModel:
    global _model
    if _model is None:
        _model = AMLModel()
        try:
            _model.load()
        except FileNotFoundError as exc:
            log.warning("%s — heuristic fallback", exc)
    return _model


def _to_flat(
    event: EnrichedEvent | None,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> dict[str, float]:
    """Map A PlayerFeatures → B FEATURE_COLUMNS names for the shared joblib."""
    pep = bool(profile.pep_flag if profile else (event.pep_flag if event else False))
    watch = bool(profile.watchlist_hit if profile else (event.watchlist_hit if event else False))
    ratio = features.wager_to_buyin_ratio
    return {
        "w1h_tx_count": float(features.txn_count_1h),
        "w24h_tx_count": float(features.txn_count_24h),
        "w24h_deposit_sum": float(features.cash_in_24h),
        "w24h_cash_in_sum": float(features.cash_in_24h),
        "w24h_withdrawal_sum": float(features.cash_out_24h),
        "w24h_cash_out_sum": float(features.cash_out_24h),
        "w24h_wager_sum": float(features.total_wagered_24h),
        "w24h_cage_visits": float(features.cage_visits_24h),
        "w24h_just_below_10k_count": float(features.just_below_10k_count_24h),
        "w24h_unique_payment_methods": float(features.unique_payment_methods_24h),
        "w24h_wire_in_sum": float(features.wire_in_24h),
        "w30d_tx_sum": float(features.month_volume),
        "static_is_pep": 1.0 if pep else 0.0,
        "static_watchlist_hit": 1.0 if watch else 0.0,
        "static_shared_device_count": 1.0,
        # keep ratio signal in wager sum already; unused cols default 0 in AMLModel
        "_wager_ratio": float(ratio if ratio is not None else 0.0),
    }


def score_xgboost(
    event: EnrichedEvent,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> tuple[int, list[dict]]:
    model = _get_model()
    if model.model is None:
        return _heuristic(event, features, profile)
    out = model.predict(_to_flat(event, features, profile))
    score = int(max(1, min(99, round(float(out["score"]) * 100))))
    factors = [
        {"feature": c["feature"], "value": c.get("value"), "model": "xgboost"}
        for c in (out.get("shap_top") or [])[:4]
    ]
    return score, factors


def score_anomaly(
    event: EnrichedEvent,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> tuple[int, list[dict]]:
    model = _get_model()
    if model.iforest is None and model.model is None:
        return _heuristic(event, features, profile)
    out = model.predict(_to_flat(event, features, profile))
    if "iforest_score" in out:
        score = int(max(1, min(99, round(float(out["iforest_score"]) * 100))))
        factors = [{"feature": "isolation_forest", "detail": f"score={out['iforest_score']:.3f}", "model": "iforest"}]
    else:
        score = int(max(1, min(99, round(float(out["score"]) * 100))))
        factors = [{"feature": "xgboost_proxy", "detail": "no iforest artifact", "model": "xgboost"}]
    if features.just_below_10k_count_24h >= 2:
        factors.append({"feature": "just_below_10k_count_24h", "detail": str(features.just_below_10k_count_24h)})
    ratio = features.wager_to_buyin_ratio
    if ratio is not None and ratio < 0.15:
        factors.append({"feature": "wager_to_buyin_ratio", "detail": f"{ratio:.2f}"})
    return score, factors


def _heuristic(
    event: EnrichedEvent,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> tuple[int, list[dict]]:
    parts: list[tuple[str, float]] = []
    amt = float(event.amount or 0)
    parts.append(("amount_log", min(25.0, max(0.0, (amt ** 0.5) / 8))))
    if features.cash_in_24h > 0:
        ratio = features.total_wagered_24h / features.cash_in_24h
        parts.append(("cash_in_vs_gaming", 22.0 if ratio < 0.15 else 6.0 if ratio < 0.4 else 1.0))
    else:
        parts.append(("cash_in_vs_gaming", 2.0))
    parts.append(
        ("cage_velocity", 18.0 if features.cage_visits_24h >= 5 else 10.0 if features.cage_visits_24h >= 3 else 2.0)
    )
    parts.append(
        (
            "just_below_threshold",
            20.0 if features.just_below_10k_count_24h >= 3 else 12.0 if features.just_below_10k_count_24h >= 1 else 0.0,
        )
    )
    raw = sum(v for _, v in parts)
    score = int(max(4, min(99, raw)))
    top = sorted(parts, key=lambda x: x[1], reverse=True)[:4]
    factors = [{"feature": k, "contribution": round(v / max(raw, 1), 3), "model": "heuristic"} for k, v in top]
    return score, factors
