"""Risk aggregation + explainability (architecture box 6)."""

from __future__ import annotations

from .config import settings
from .models import ScoreBreakdown


def tier_for(score: int) -> str:
    if score >= 85:
        return "CRITICAL"
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    return "LOW"


def aggregate(
    rule_score: int,
    ml_score: int,
    behavioral_score: int,
    kyc_score: int,
    matched_rules: list[str],
    factors: list[dict],
) -> ScoreBreakdown:
    final = int(
        round(
            settings.weight_rules * rule_score
            + settings.weight_xgboost * ml_score
            + settings.weight_foundation * behavioral_score
            + settings.weight_kyc * kyc_score
        )
    )
    final = max(0, min(100, final))
    if "WATCHLIST_MATCH" in matched_rules:
        final = max(final, 90)
    explanation = explain(final, matched_rules, factors, rule_score, ml_score, behavioral_score, kyc_score)
    return ScoreBreakdown(
        rules=rule_score,
        xgboost=ml_score,
        behavioral=behavioral_score,
        kyc=kyc_score,
        final=final,
        tier=tier_for(final),
        matched_rules=matched_rules,
        explanation=explanation,
        factors=factors[:8],
    )


def kyc_score(watchlist: bool, pep: bool, rating: str, prior_alerts: int) -> int:
    base = {"LOW": 10, "MEDIUM": 35, "HIGH": 62, "CRITICAL": 88}.get((rating or "LOW").upper(), 10)
    if pep:
        base = max(base, 70)
    if watchlist:
        base = max(base, 92)
    base = min(100, base + min(15, prior_alerts * 4))
    return base


def explain(
    final: int,
    matched: list[str],
    factors: list[dict],
    rules: int,
    ml: int,
    behavioral: int,
    kyc: int,
) -> str:
    if not matched:
        return (
            f"No AML rule fired. Composite score {final} "
            f"(rules {rules}, foundation model {behavioral}, XGBoost {ml}, KYC {kyc})."
        )
    lead = ", ".join(matched[:3])
    bits = [f for f in factors if f.get("detail")][:2]
    extra = "; ".join(str(b.get("detail")) for b in bits)
    return (
        f"Triggered {lead}. Final AML risk {final}/100 "
        f"(rules {rules} ×30%, foundation model {behavioral} ×35%, "
        f"XGBoost {ml} ×20%, KYC {kyc} ×15%). {extra}"
    ).strip()
