"""OpenJev decision layer for AML alert triage.

Detection stays with Rules + XGBoost + TFM. OpenJev only decides disposition:
  escalate | need_info | close

OpenJev (https://github.com/razorback16/openjev) speaks the System One wire API
(``POST /v1/systemone``). Defaults target open-source OpenJev — not paid TypeSafe Jev.

Modes (env ``JEV_MODE``):
  * ``mock`` — deterministic heuristics from rule hits / risk score
  * ``live`` — OpenJev HTTP (Codiv hosted or in-cluster ``openjev`` Service)
  * ``off`` — skip decision enrichment

Endpoints (``TYPESAFE_BASE_URL`` kept for SDK-compatible env names):
  * Codiv (hosted OpenJev, free tier): ``https://api.codiv.ai`` + ``TYPESAFE_API_KEY=sk-codiv-…``
  * In-cluster Verdict CPU: ``http://openjev:8080`` + ``TYPESAFE_API_KEY=local``
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("casino_common.jev")

DISPOSITION_TO_STATUS = {
    "escalate": "ESCALATED",
    "need_info": "INVESTIGATING",
    "close": "CLOSED",
}

_STRUCTURING_RULES = {"AML-001", "AML-010", "STRUCTURING"}
_RAPID_RULES = {"AML-002", "AML-003", "AML-013", "MINIMAL_PLAY", "RAPID_WITHDRAWAL", "RAPID_WIRE"}
_SHELL_RULES = {"AML-005", "AML-012"}
_WATCHLIST_RULES = {"AML-015", "AML-009", "WATCHLIST_MATCH", "CTR_THRESHOLD"}
_PURPOSE_RULES = {"AML-002", "AML-007", "AML-008"}


def jev_mode() -> str:
    return (os.getenv("JEV_MODE") or "mock").strip().lower()


def openjev_base_url() -> str:
    """System One base URL (no trailing slash). Prefer OPENJEV_BASE_URL."""
    return (
        os.getenv("OPENJEV_BASE_URL")
        or os.getenv("TYPESAFE_BASE_URL")
        or "https://api.codiv.ai"
    ).rstrip("/")


def openjev_model() -> str:
    return (os.getenv("JEV_MODEL") or os.getenv("OPENJEV_MODEL") or "openjev-latest").strip()


def _api_key() -> str:
    return (
        os.getenv("TYPESAFE_API_KEY")
        or os.getenv("JEV_API_KEY")
        or os.getenv("CODIV_API_KEY")
        or ""
    ).strip()


def _is_local_openjev(base: str) -> bool:
    host = base.lower()
    return (
        "openjev" in host
        or "127.0.0.1" in host
        or "localhost" in host
        or ".svc" in host
    )


def _rule_ids(alert: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for hit in alert.get("rule_hits") or []:
        if isinstance(hit, str):
            ids.add(hit.strip().upper())
        elif isinstance(hit, dict):
            rid = str(hit.get("rule_id") or hit.get("id") or "").strip().upper()
            if rid:
                ids.add(rid)
    for raw in alert.get("matched_rules") or []:
        if isinstance(raw, str):
            ids.add(raw.strip().upper())
        elif isinstance(raw, dict):
            rid = str(raw.get("rule_id") or raw.get("id") or "").strip().upper()
            if rid:
                ids.add(rid)
    return ids


def build_state(alert: dict[str, Any]) -> dict[str, Any]:
    """Compact case file Jev (or mock) evaluates."""
    rules = sorted(_rule_ids(alert))
    return {
        "alert_id": alert.get("alert_id"),
        "player_id": alert.get("player_id"),
        "transaction_id": alert.get("transaction_id"),
        "detection_score": float(alert.get("score") or 0),
        "pipeline_risk_level": str(alert.get("risk_level") or "LOW").upper(),
        "status": alert.get("status"),
        "title": alert.get("title") or alert.get("summary") or "",
        "summary": alert.get("summary") or "",
        "rule_ids": rules,
        "explanations": list(alert.get("explanations") or [])[:8],
        "notes": "Casino AML alert from Rules+XGBoost+TFM pipeline. Decide triage disposition only.",
    }


def _aml_questions(state: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fast System One questions for in-cluster Verdict CPU (~5–15s).

    Full multi-noul rubrics take 60–90s on CPU and trip OpenShift route timeouts.
    Verdict often returns near-uniform probs; we still call it, then reconcile
    with AML policy heuristics so escalate / need_info / close stay justified.
    """
    rules = ", ".join((state or {}).get("rule_ids") or []) or "none"
    score = (state or {}).get("detection_score")
    level = (state or {}).get("pipeline_risk_level") or "UNKNOWN"
    hint = (
        f"Alert context: pipeline_risk={level}, detection_score={score}, "
        f"matched_rules=[{rules}]. "
        "escalate for watchlist/PEP/CTR/structuring/rapid cash-out/critical risk; "
        "need_info for medium or incomplete evidence; "
        "close ONLY when risk is LOW, score is low, and there are no AML rule hits."
    )
    return {
        "disposition": {
            "type": "choice",
            "instructions": hint,
            "criteria": {
                "escalate": (
                    "Escalate to compliance / SAR path — watchlist, PEP, sanctions, "
                    "CTR breach, structuring/smurfing, rapid withdrawal/wire, or critical risk"
                ),
                "need_info": (
                    "Keep investigating — medium risk, behavioral anomaly, or incomplete evidence"
                ),
                "close": (
                    "Close as not suspicious — ONLY low score, LOW pipeline risk, and no rule hits"
                ),
            },
        },
    }


def _mock_decide(state: dict[str, Any]) -> dict[str, Any]:
    rules = {str(r).upper() for r in (state.get("rule_ids") or [])}
    score = float(state.get("detection_score") or 0)
    pipeline_level = str(state.get("pipeline_risk_level") or "LOW").upper()

    structuring = 0.92 if rules & _STRUCTURING_RULES else (0.25 if score >= 0.7 else 0.08)
    rapid = 0.9 if rules & _RAPID_RULES else 0.12
    shell = 0.88 if rules & _SHELL_RULES else 0.1
    watchlist = 0.95 if rules & _WATCHLIST_RULES else 0.05
    purpose = 0.85 if rules & _PURPOSE_RULES else (0.4 if rapid > 0.5 else 0.15)

    # Map pipeline level + score → Score rubric 0..3
    level_map = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    risk_idx = level_map.get(pipeline_level, 1)
    if score >= 0.85:
        risk_idx = max(risk_idx, 3)
    elif score >= 0.65:
        risk_idx = max(risk_idx, 2)
    elif score >= 0.4:
        risk_idx = max(risk_idx, 1)

    red_flag_count = sum(
        1 for v in (structuring, rapid, shell, watchlist, purpose) if v >= 0.55
    )
    active_flags = [
        name
        for name, val in (
            ("structuring/smurfing", structuring),
            ("rapid pass-through", rapid),
            ("shared/shell device", shell),
            ("watchlist/CTR", watchlist),
            ("purpose mismatch", purpose),
        )
        if val >= 0.55
    ]
    # Escalate on watchlist/CTR, critical pipeline, or a clear typology + elevated score.
    if (
        watchlist >= 0.7
        or risk_idx >= 3
        or (red_flag_count >= 1 and (risk_idx >= 2 or score >= 0.65))
        or (red_flag_count >= 2 and score >= 0.55)
    ):
        disposition = "escalate"
        conf = 0.86
        why = (
            f"Escalate: {', '.join(active_flags) or 'elevated typology'} with "
            f"pipeline={pipeline_level} score={score:.2f}"
        )
    elif score < 0.4 and red_flag_count == 0 and risk_idx <= 0:
        disposition = "close"
        conf = 0.75
        why = (
            f"Close: no AML rule red-flags, pipeline={pipeline_level}, "
            f"score={score:.2f} looks benign"
        )
    else:
        disposition = "need_info"
        conf = 0.7
        why = (
            f"Need info: evidence incomplete or medium risk "
            f"(flags={', '.join(active_flags) or 'none'}, "
            f"pipeline={pipeline_level}, score={score:.2f})"
        )

    legend = ["Low", "Medium", "High", "Critical"]
    return {
        "mode": "mock",
        "model": "jev-mock",
        "decided_at": datetime.now(timezone.utc).isoformat(),
        "state_summary": {
            "player_id": state.get("player_id"),
            "rule_ids": state.get("rule_ids"),
            "detection_score": score,
        },
        "red_flags": {
            "structuring": round(structuring, 4),
            "rapid_pass_through": round(rapid, 4),
            "shell_or_shared": round(shell, 4),
            "watchlist_or_ctr": round(watchlist, 4),
            "purpose_mismatch": round(purpose, 4),
        },
        "risk_level": legend[risk_idx],
        "risk_score_index": risk_idx,
        "disposition": disposition,
        "disposition_confidence": conf,
        "suggested_status": DISPOSITION_TO_STATUS[disposition],
        "rationale": why,
        "policy_applied": True,
    }


def _parse_live_answers(raw: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    answers = raw.get("answers") or raw.get("response") or raw
    if not isinstance(answers, dict):
        answers = {}

    def _noul(key: str) -> float:
        node = answers.get(key) or {}
        if isinstance(node, (int, float)):
            return float(node)
        return float(node.get("noul") or 0)

    risk_node = answers.get("risk_level") or {}
    if isinstance(risk_node, dict):
        risk_idx = int(round(float(risk_node.get("score") or 0)))
        conf_risk = float(risk_node.get("confidence") or 0)
    else:
        risk_idx = 0
        conf_risk = 0.0
    risk_idx = max(0, min(3, risk_idx))
    legend = ["Low", "Medium", "High", "Critical"]

    disp_node = answers.get("disposition") or {}
    disposition = str(
        disp_node.get("choice") if isinstance(disp_node, dict) else disp_node or "need_info"
    ).lower()
    if disposition not in DISPOSITION_TO_STATUS:
        disposition = "need_info"
    conf = float(disp_node.get("confidence") or conf_risk or 0.5) if isinstance(disp_node, dict) else 0.5

    provider = "openjev"
    return {
        "mode": "openjev",
        "provider": provider,
        "model": openjev_model(),
        "decided_at": datetime.now(timezone.utc).isoformat(),
        "state_summary": {
            "player_id": state.get("player_id"),
            "rule_ids": state.get("rule_ids"),
            "detection_score": state.get("detection_score"),
        },
        "red_flags": {
            "structuring": round(_noul("structuring"), 4),
            "rapid_pass_through": round(_noul("rapid_pass_through"), 4),
            "shell_or_shared": round(_noul("shell_or_shared"), 4),
            "watchlist_or_ctr": round(_noul("watchlist_or_ctr"), 4),
            "purpose_mismatch": round(_noul("purpose_mismatch"), 4),
        },
        "risk_level": legend[risk_idx],
        "risk_score_index": risk_idx,
        "disposition": disposition,
        "disposition_confidence": round(conf, 4),
        "suggested_status": DISPOSITION_TO_STATUS[disposition],
        "rationale": f"OpenJev: disposition={disposition} confidence={conf:.2f}",
        "raw_usage": raw.get("usage"),
    }


def _needs_live_refresh(decision: Any) -> bool:
    """True when decision is missing, provisional, or a pre-policy OpenJev close."""
    if not isinstance(decision, dict) or not decision:
        return True
    mode = str(decision.get("mode") or "").lower()
    if mode in ("", "mock", "mock_fallback", "pending"):
        return True
    # Re-decide old Verdict-only closes that lacked AML policy reconciliation.
    if not decision.get("policy_applied"):
        return True
    conf = float(decision.get("disposition_confidence") or 0)
    if mode == "openjev" and conf < 0.15:
        return True
    return False


def _prob_margin(probs: dict[str, Any] | None) -> float:
    if not probs:
        return 0.0
    vals = sorted((float(v) for v in probs.values()), reverse=True)
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    return vals[0] - vals[1]


def _reconcile_disposition(
    live: dict[str, Any],
    heuristic: dict[str, Any],
    *,
    raw_disp: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Blend OpenJev output with AML policy so dispositions stay justified.

    In-cluster Verdict often returns near-uniform probs biased to ``close``
    (confidence ~0.02). When the model is weak or unsafe, keep OpenJev
    telemetry but apply heuristic escalate / need_info / close policy.
    """
    out = dict(live)
    h_disp = str(heuristic.get("disposition") or "need_info").lower()
    l_disp = str(live.get("disposition") or "need_info").lower()
    conf = float(live.get("disposition_confidence") or 0)
    probs = {}
    if isinstance(raw_disp, dict):
        probs = {str(k): float(v) for k, v in (raw_disp.get("probabilities") or {}).items()}
    margin = _prob_margin(probs)

    min_conf = float(os.getenv("JEV_TRUST_MIN_CONF", "0.25"))
    min_margin = float(os.getenv("JEV_TRUST_MIN_MARGIN", "0.12"))
    trust_live = (
        conf >= min_conf
        and margin >= min_margin
        and l_disp in DISPOSITION_TO_STATUS
        # Verdict is biased to "close"; never accept close unless policy also says close.
        and not (l_disp == "close" and h_disp != "close")
    )

    out["red_flags"] = heuristic.get("red_flags") or out.get("red_flags")
    out["risk_level"] = heuristic.get("risk_level") or out.get("risk_level")
    out["risk_score_index"] = heuristic.get("risk_score_index", out.get("risk_score_index"))
    out["openjev_raw"] = {
        "disposition": l_disp,
        "confidence": conf,
        "probabilities": probs or None,
    }
    out["policy_applied"] = True
    out["provider"] = "openjev"

    if trust_live:
        out["mode"] = "openjev"
        out["disposition"] = l_disp
        out["disposition_confidence"] = round(conf, 4)
        out["suggested_status"] = DISPOSITION_TO_STATUS[l_disp]
        out["rationale"] = (
            f"OpenJev chose {l_disp} (conf={conf:.2f}, margin={margin:.2f}). "
            f"Policy check agrees with pipeline={heuristic.get('risk_level')}."
        )
        return out

    # Policy wins — still surface that OpenJev was consulted.
    out["mode"] = "hybrid"
    out["disposition"] = h_disp
    out["disposition_confidence"] = float(heuristic.get("disposition_confidence") or 0.7)
    out["suggested_status"] = DISPOSITION_TO_STATUS[h_disp]
    out["model"] = f"{out.get('model') or openjev_model()}+policy"
    out["rationale"] = (
        f"{heuristic.get('rationale')}. "
        f"OpenJev raw={l_disp} (conf={conf:.2f}, margin={margin:.2f}) was too weak / unsafe; "
        f"applied AML disposition policy."
    )
    return out


def _live_decide(state: dict[str, Any]) -> dict[str, Any]:
    """Call OpenJev (or any System One–compatible server), then apply AML policy."""
    base = openjev_base_url()
    api_key = _api_key()
    # Heuristic red flags + disposition policy stay local for a justified triage.
    heuristic = _mock_decide(state)
    # In-cluster OpenJev accepts a placeholder bearer; Codiv requires sk-codiv-…
    if not api_key:
        if _is_local_openjev(base):
            api_key = "local"
        else:
            log.warning(
                "JEV_MODE=live but no API key (set TYPESAFE_API_KEY / CODIV_API_KEY) — falling back to mock"
            )
            out = dict(heuristic)
            out["mode"] = "mock_fallback"
            out["rationale"] = "OpenJev API key missing; " + out["rationale"]
            return out

    model = openjev_model()
    # In-cluster Verdict image may only expose verdict-1.4 — allow override via JEV_MODEL.
    if _is_local_openjev(base) and model in ("openjev-latest", "jev-latest", "jev-preview"):
        model = os.getenv("OPENJEV_INCLUSTER_MODEL", "verdict-1.4")

    url = f"{base}/v1/systemone"
    payload = {
        "model": model,
        "state": state,
        "questions": _aml_questions(state),
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Verdict CPU is ~5–15s for disposition-only; keep headroom under route limits.
    timeout = float(os.getenv("JEV_TIMEOUT_SEC", "60"))

    try:
        import httpx

        with httpx.Client(timeout=timeout) as client:
            resp = client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        answers = data.get("answers") if isinstance(data, dict) else {}
        out = _parse_live_answers(data if isinstance(data, dict) else {}, state)
        raw_disp = answers.get("disposition") if isinstance(answers, dict) else None
        out["endpoint"] = base
        out["model"] = model
        return _reconcile_disposition(
            out, heuristic, raw_disp=raw_disp if isinstance(raw_disp, dict) else None
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("OpenJev call failed (%s); using mock", exc)
        out = dict(heuristic)
        out["mode"] = "mock_fallback"
        out["rationale"] = f"openjev error: {exc}; " + out["rationale"]
        return out


def decide_alert(alert: dict[str, Any], *, force: bool = False) -> dict[str, Any] | None:
    """Return an OpenJev decision dict, or None when disabled."""
    mode = jev_mode()
    if mode in ("off", "disabled", "false", "0"):
        return None
    existing = alert.get("jev_decision")
    if existing and not force and not _needs_live_refresh(existing):
        return existing
    # force / mock / pending → refresh when live is configured
    if existing and not force and mode not in ("live", "openjev"):
        return existing

    state = build_state(alert)
    if mode in ("live", "openjev"):
        return _live_decide(state)
    return _mock_decide(state)


def attach_decision(alert: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    """Mutate alert with ``jev_decision`` when enabled."""
    decision = decide_alert(alert, force=force)
    if decision is None:
        return alert
    alert["jev_decision"] = decision
    alert["jev_disposition"] = decision.get("disposition")
    alert["jev_suggested_status"] = decision.get("suggested_status")
    return alert
