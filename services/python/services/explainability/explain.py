"""Programmatic explainability — never 'AI says high risk'."""

from __future__ import annotations

from typing import Any

from casino_common.models import RiskAssessment


def explain(assessment: RiskAssessment) -> dict[str, Any]:
    bullets = []
    for e in assessment.explanations:
        bullets.append(e)
    narrative = (
        f"Player {assessment.player_id} received risk level {assessment.risk_level.value} "
        f"with composite score {assessment.final_score:.2f}. "
        f"Weights: rules={assessment.weights.get('rules')}, "
        f"ml={assessment.weights.get('ml')}, "
        f"behavioral={assessment.weights.get('behavioral')}."
    )
    return {
        "event_id": assessment.event_id,
        "player_id": assessment.player_id,
        "risk_level": assessment.risk_level.value,
        "final_score": assessment.final_score,
        "narrative": narrative,
        "factor_breakdown": {
            "rules_score": assessment.rules_score,
            "ml_score": assessment.ml_score,
            "behavioral_score": assessment.behavioral_score,
        },
        "rule_hits": [h.model_dump() for h in assessment.rule_hits],
        "bullets": bullets,
        "disclaimer": (
            "Explanation is derived from deterministic AML rules, feature contributions, "
            "and model scores — not a free-form generative statement."
        ),
    }
