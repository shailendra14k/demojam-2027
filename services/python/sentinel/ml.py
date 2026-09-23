"""CPU predictive models: XGBoost + Isolation Forest.

No GPU required. Train with: python -m sentinel.train_models
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

from .config import settings
from .models import EnrichedEvent, PlayerFeatures, PlayerProfile
from .vectorize import FEATURE_NAMES, vectorize

log = logging.getLogger("sentinel.ml")

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
_xgb = None
_iforest = None
_importances: list[tuple[str, float]] = []


def _load_xgboost():
    global _xgb, _importances
    if _xgb is not None:
        return _xgb
    for name in ("aml_xgboost.joblib", "aml_gbt.joblib"):
        path = ARTIFACTS / name
        if not path.exists():
            continue
        import joblib

        payload = joblib.load(path)
        _xgb = payload["model"] if isinstance(payload, dict) else payload
        meta_path = ARTIFACTS / "meta.json"
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            _importances = [(n, float(v)) for n, v in meta.get("top_features", [])]
        if not _importances and hasattr(_xgb, "feature_importances_"):
            _importances = sorted(
                zip(FEATURE_NAMES, [float(x) for x in _xgb.feature_importances_]),
                key=lambda x: x[1],
                reverse=True,
            )
        log.info("Loaded XGBoost AML classifier %s", path)
        return _xgb
    log.warning("XGBoost artifact missing under %s — using heuristic fallback", ARTIFACTS)
    return None


def _load_iforest():
    global _iforest
    if _iforest is not None:
        return _iforest
    path = ARTIFACTS / "aml_iforest.joblib"
    if not path.exists():
        log.warning("Isolation Forest artifact missing at %s", path)
        return None
    import joblib

    _iforest = joblib.load(path)
    log.info("Loaded Isolation Forest %s", path)
    return _iforest


def score_xgboost(
    event: EnrichedEvent,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> tuple[int, list[dict]]:
    """Supervised AML probability → 0–100 (OpenShift AI ONNX, else local joblib)."""
    vec = vectorize(event, features, profile)
    proba = None
    source = "xgboost"
    if settings.xgb_infer_url:
        from . import onnx_infer

        remote = onnx_infer.predict_proba(settings.xgb_infer_url, vec)
        if remote is not None:
            proba = float(remote.reshape(-1)[1] if remote.size > 1 else remote.reshape(-1)[0])
            source = "xgboost-onnx"
    if proba is None:
        model = _load_xgboost()
        if model is None:
            return _heuristic(event, features, profile)
        proba = float(model.predict_proba(np.asarray([vec], dtype=np.float32))[0, 1])

    score = int(max(1, min(99, round(proba * 100))))
    ranked = _importances[:4] or list(zip(FEATURE_NAMES, vec))
    factors = []
    for name, _imp in ranked[:4]:
        idx = FEATURE_NAMES.index(name) if name in FEATURE_NAMES else -1
        val = vec[idx] if idx >= 0 else None
        factors.append({"feature": name, "value": val, "model": source})
    return score, factors


def score_anomaly(
    event: EnrichedEvent,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> tuple[int, list[dict]]:
    """Isolation Forest → 0–100 risk (CPU, unsupervised)."""
    vec = vectorize(event, features, profile)
    model = _load_iforest()
    if model is None:
        return _heuristic(event, features, profile)

    decision = float(model.decision_function(np.asarray([vec], dtype=np.float32))[0])
    score = int(max(1, min(99, round(50 - decision * 90))))
    factors = [
        {"feature": "isolation_forest", "detail": f"decision_function={decision:.3f}", "model": "iforest"}
    ]
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
