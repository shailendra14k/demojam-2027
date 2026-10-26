"""Risk aggregator: weighted rules + ML + behavioral + optional KYC → risk level."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from casino_common.models import (
    BehavioralScore,
    MLPrediction,
    RiskAssessment,
    RiskLevel,
    RuleResult,
)


def _weights() -> dict[str, float]:
    raw = {
        "rules": float(os.getenv("RISK_WEIGHT_RULES", "0.50")),
        "ml": float(os.getenv("RISK_WEIGHT_ML", "0.25")),
        "behavioral": float(os.getenv("RISK_WEIGHT_BEHAVIORAL", "0.15")),
        "kyc": float(os.getenv("RISK_WEIGHT_KYC", "0.10")),
    }
    total = sum(raw.values()) or 1.0
    return {k: v / total for k, v in raw.items()}


def kyc_score_from_features(features: Optional[dict[str, float]]) -> float:
    """0–1 KYC risk from static watchlist / PEP / shared device signals."""
    if not features:
        return 0.0
    score = 0.0
    if features.get("static_watchlist_hit", 0):
        score = max(score, 0.95)
    if features.get("static_is_pep", 0):
        score = max(score, 0.75)
    shared = features.get("static_shared_device_count", 0)
    if shared >= 3:
        score = max(score, min(0.7, 0.4 + 0.1 * (shared - 2)))
    return float(min(1.0, score))


def score_to_level(score: float) -> RiskLevel:
    if score >= 0.85:
        return RiskLevel.CRITICAL
    if score >= 0.65:
        return RiskLevel.HIGH
    if score >= 0.40:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def aggregate(
    rules: RuleResult,
    ml: Optional[MLPrediction] = None,
    behavioral: Optional[BehavioralScore] = None,
    features: Optional[dict[str, float]] = None,
    kyc_score: Optional[float] = None,
) -> RiskAssessment:
    w = _weights()
    rules_score = float(rules.aggregate_score)
    ml_score = float(ml.score) if ml else 0.0
    beh_score = float(behavioral.score) if behavioral else 0.0
    kyc = float(kyc_score) if kyc_score is not None else kyc_score_from_features(features)
    if kyc <= 0:
        for hit in rules.hits:
            if hit.rule_id == "AML-015":
                kyc = max(kyc, 0.95 if "Watchlist" in hit.rule_name else 0.78)
    final = (
        w["rules"] * rules_score
        + w["ml"] * ml_score
        + w["behavioral"] * beh_score
        + w.get("kyc", 0.0) * kyc
    )
    if len(rules.hits) >= 2:
        final = min(1.0, final + 0.05 * (len(rules.hits) - 1))
    # Watchlist floor (demojam-style)
    if features and features.get("static_watchlist_hit", 0):
        final = max(final, 0.90)

    explanations: list[str] = []
    for hit in rules.hits:
        explanations.append(
            f"{hit.rule_id} ({hit.rule_name}): {hit.description} "
            f"[score={hit.score:.2f}; evidence={hit.evidence}]"
        )
    if ml:
        top = ", ".join(f"{s['feature']}={s['value']:.2f}" for s in (ml.shap_top or [])[:3])
        explanations.append(
            f"ML model {ml.model_name} v{ml.model_version} scored {ml.score:.2f} ({ml.label}); "
            f"top drivers: {top}"
        )
    if behavioral:
        explanations.append(
            f"Behavioral adapter '{behavioral.adapter}' scored {behavioral.score:.2f}; "
            f"flags={behavioral.anomaly_flags}"
        )
    if kyc > 0:
        explanations.append(f"KYC enrichment scored {kyc:.2f} (watchlist/PEP/shared-device)")
    if not explanations:
        explanations.append(
            f"No rule hits; composite score {final:.2f} from weighted ML/behavioral/KYC signals only."
        )

    return RiskAssessment(
        event_id=rules.event_id,
        player_id=rules.player_id,
        transaction_id=rules.transaction_id,
        correlation_id=rules.correlation_id,
        timestamp=datetime.now(timezone.utc),
        rules_score=rules_score,
        ml_score=ml_score,
        behavioral_score=beh_score,
        final_score=round(final, 4),
        risk_level=score_to_level(final),
        rule_hits=rules.hits,
        explanations=explanations,
        weights={**w, "kyc_score": kyc},
        amount=getattr(rules, "amount", None),
        transaction_type=getattr(rules, "transaction_type", None) or "TRANSACTION",
        currency=getattr(rules, "currency", None) or "USD",
        location=getattr(rules, "location", None) or "Casino floor",
        payment_method=getattr(rules, "payment_method", None) or "UNKNOWN",
    )
