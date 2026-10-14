"""Human-readable AML detection labels for alerts and live events."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

RULE_AI_LABELS: dict[str, str] = {
    "AML-001": "AI Detected Structuring / Smurfing",
    "AML-002": "AI Detected Money Laundering",
    "AML-003": "AI Detected Quick Cash In & Out",
    "AML-004": "AI Detected Chip-Cage Layering",
    "AML-005": "AI Detected Shared Device",
    "AML-006": "AI Detected Impossible Travel",
    "AML-007": "AI Detected Behavioral Deviation",
    "AML-008": "AI Detected High-Value Spike",
    "AML-009": "AI Detected CTR Threshold",
    "AML-010": "AI Detected Cage Structuring",
    "AML-011": "AI Detected High Velocity",
    "AML-012": "AI Detected Multi-Instrument",
    "AML-013": "AI Detected Rapid Wire Laundering",
    "AML-014": "AI Detected Large TITO",
    "AML-015": "AI Detected Watchlist / PEP",
}

# Legacy / catalog ids used in mock paths
_LEGACY_RULE_LABELS: dict[str, str] = {
    "STRUCTURING": RULE_AI_LABELS["AML-001"],
    "MINIMAL_PLAY": RULE_AI_LABELS["AML-002"],
    "RAPID_WITHDRAWAL": RULE_AI_LABELS["AML-003"],
    "CTR_THRESHOLD": RULE_AI_LABELS["AML-009"],
    "RAPID_WIRE": RULE_AI_LABELS["AML-013"],
    "WATCHLIST_MATCH": RULE_AI_LABELS["AML-015"],
    "HIGH_VELOCITY": RULE_AI_LABELS["AML-011"],
    "MULTI_INSTRUMENT": RULE_AI_LABELS["AML-012"],
}

_GENERIC_TITLE_PREFIXES = ("AML CRITICAL", "AML HIGH", "AML MEDIUM", "AML LOW", "AML ")


def _rule_id(hit: Any) -> str:
    if isinstance(hit, str):
        return hit.strip()
    if isinstance(hit, Mapping):
        return str(hit.get("rule_id") or hit.get("id") or "").strip()
    return str(getattr(hit, "rule_id", "") or "").strip()


def _collect_rule_ids(rule_hits: Sequence[Any] | None, matched_rules: Sequence[Any] | None = None) -> list[str]:
    ids: list[str] = []
    for hit in rule_hits or []:
        rid = _rule_id(hit)
        if rid:
            ids.append(rid)
    for raw in matched_rules or []:
        rid = _rule_id(raw)
        if rid and rid not in ids:
            ids.append(rid)
    return ids


def _label_for_rule_id(rule_id: str) -> str | None:
    if rule_id in RULE_AI_LABELS:
        return RULE_AI_LABELS[rule_id]
    upper = rule_id.upper()
    if upper in _LEGACY_RULE_LABELS:
        return _LEGACY_RULE_LABELS[upper]
    if upper.startswith("AML-") and upper in RULE_AI_LABELS:
        return RULE_AI_LABELS[upper]
    return None


def _is_generic_title(title: str | None) -> bool:
    if not title:
        return True
    t = title.strip()
    if not t or t.upper() == "AML":
        return True
    if t.upper().startswith(_GENERIC_TITLE_PREFIXES):
        return True
    if t in RULE_AI_LABELS or t in _LEGACY_RULE_LABELS:
        return True
    if _label_for_rule_id(t):
        return True
    return False


def alert_display_label(
    *,
    rule_hits: Sequence[Any] | None = None,
    matched_rules: Sequence[Any] | None = None,
    risk_level: str | None = None,
    title: str | None = None,
    summary: str | None = None,
    ml_score: float | None = None,
    behavioral_score: float | None = None,
) -> str:
    """Primary dashboard / investigation headline for an alert or scored event."""
    if title and not _is_generic_title(title):
        return title.strip()

    rule_ids = _collect_rule_ids(rule_hits, matched_rules)
    for rid in rule_ids:
        label = _label_for_rule_id(rid)
        if label:
            return label

    if behavioral_score is not None and behavioral_score >= 0.65:
        return "AI Detected Behavioral Anomaly"
    if ml_score is not None and ml_score >= 0.65:
        return "AI Detected ML Fraud Pattern"

    if summary:
        low = summary.lower()
        if "structuring" in low or "smurf" in low:
            return RULE_AI_LABELS["AML-001"]
        if "minimal" in low and ("gaming" in low or "play" in low):
            return RULE_AI_LABELS["AML-002"]
        if "rapid" in low and ("withdraw" in low or "cash" in low):
            return RULE_AI_LABELS["AML-003"]
        if "wire" in low:
            return RULE_AI_LABELS["AML-013"]
        if "watchlist" in low or "pep" in low:
            return RULE_AI_LABELS["AML-015"]

    level = (risk_level or "HIGH").upper()
    if level == "CRITICAL":
        return "AI Detected Critical AML Activity"
    if level == "HIGH":
        return "AI Detected Elevated AML Risk"
    return "AI Detected Suspicious Activity"


def event_display_label(item: Mapping[str, Any]) -> str:
    """Label for live monitor / dashboard rows built from pipeline events."""
    risk = item.get("risk") or {}
    rules = item.get("rules") or {}
    hits = rules.get("hits") if isinstance(rules, Mapping) else None
    rule_hits = risk.get("rule_hits") or hits or []
    ml = item.get("ml") or {}
    beh = item.get("behavioral") or {}
    try:
        ml_s = float(ml.get("score")) if ml.get("score") is not None else None
    except (TypeError, ValueError):
        ml_s = None
    try:
        beh_s = float(beh.get("score")) if beh.get("score") is not None else None
    except (TypeError, ValueError):
        beh_s = None
    return alert_display_label(
        rule_hits=rule_hits,
        risk_level=str(risk.get("risk_level") or ""),
        ml_score=ml_s,
        behavioral_score=beh_s,
    )
