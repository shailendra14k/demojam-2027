#!/usr/bin/env python3
"""Casino Sentinel FastAPI — health, AML APIs, demo controls, SSE.

Pipeline modes
--------------
* ``PIPELINE_MODE=local`` (default): in-process rules/ML/risk for API-only smoke demos.
  Does **not** use Kafka. Documented for local development only.
* ``PIPELINE_MODE=cdc`` (OpenShift): demo inject writes to PostgreSQL; Debezium CDC →
  Kafka → streaming services produce alerts. API consumes ``aml.alerts`` +
  ``aml.risk.assessments`` (and hydrates from ``aml.alerts`` / ``transactions``)
  for Live Monitor. Never runs the in-process scoring path for demos.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

ROOT = Path(__file__).resolve().parents[1]  # services/python
REPO_ROOT = Path(__file__).resolve().parents[3]  # casino-sentinel
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "aml-rules"))
sys.path.insert(0, str(ROOT / "services" / "feature-engine"))
sys.path.insert(0, str(ROOT / "services" / "risk-engine"))
sys.path.insert(0, str(ROOT / "services" / "explainability"))
sys.path.insert(0, str(ROOT / "ai" / "adapter"))
sys.path.insert(0, str(REPO_ROOT / "sql" / "scenarios"))

from casino_common.config import database_url  # noqa: E402
from casino_common.logging_config import set_context, setup_logging  # noqa: E402
from casino_common.models import AlertStatus, MLPrediction, RiskLevel  # noqa: E402
from explain import explain  # noqa: E402
from feature_engine import FeatureEngine  # noqa: E402
from behavioral import get_adapter  # noqa: E402
from risk_aggregator import aggregate  # noqa: E402
from rules_engine import RULE_CATALOG, evaluate_rules  # noqa: E402
from sentinel.aml_model import AMLModel  # noqa: E402
from run_scenario import EXPECTED, build_events  # noqa: E402

log = setup_logging("api", os.getenv("LOG_LEVEL", "INFO"))

# local = in-process (API-only smoke); cdc = Postgres → Debezium → Kafka services
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "local").lower().strip()
# Keep under common OpenTLC/edge ~60s hard cutoffs (route annotation is often ignored).
# Clients (demo.sh / UI) continue polling /api/v1/alerts after this window.
CDC_WAIT_SECONDS = int(os.getenv("CDC_WAIT_SECONDS", "45"))

app = FastAPI(title="Casino Sentinel API", version="1.0.0", description="From Transaction to Suspicion, in Seconds")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _metric(factory, name, *args, **kwargs):
    """Idempotent metric registration for reloadable tests."""
    from prometheus_client import REGISTRY

    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if existing is not None:
        return existing
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name)


REQUESTS = _metric(Counter, "casino_api_requests_total", "API requests", ["endpoint"])
ALERTS_GAUGE = _metric(Gauge, "casino_open_alerts", "Open alerts")
PIPELINE_EVENTS = _metric(Counter, "casino_pipeline_events_total", "Pipeline events processed", ["stage"])

feature_engine = FeatureEngine()
aml_model = AMLModel()
aml_model.load()
behavioral = get_adapter(os.getenv("AI_MODE", "mock"))

# In-memory store (local mode results + cdc mode Kafka/DB hydration)
ALERTS: list[dict[str, Any]] = []
PLAYERS: dict[str, dict[str, Any]] = {}
EVENTS: deque[dict[str, Any]] = deque(maxlen=500)
PIPELINE_STATUS = {
    "DATABASE": {"ok": True, "detail": "ready"},
    "CDC": {"ok": PIPELINE_MODE == "cdc", "detail": "debezium" if PIPELINE_MODE == "cdc" else "local-bypass"},
    "STREAM": {"ok": PIPELINE_MODE == "cdc", "detail": "kafka consumers" if PIPELINE_MODE == "cdc" else "in-process"},
    "ENRICH": {"ok": True, "detail": "feature-engine"},
    "ANALYZE": {"ok": True, "detail": "rules+ml"},
    "RISK": {"ok": True, "detail": "aggregator"},
    "ALERT": {"ok": True, "detail": "alert-service" if PIPELINE_MODE == "cdc" else "in-process"},
}
SSE_SUBSCRIBERS: list[asyncio.Queue] = []
START_TIME = time.time()
_ALERT_IDS: set[str] = set()
_EVENT_IDS: set[str] = set()


class InjectRequest(BaseModel):
    events: list[dict[str, Any]] = Field(default_factory=list)


class AlertPatch(BaseModel):
    status: AlertStatus
    assigned_to: Optional[str] = None
    note: Optional[str] = None


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _row_to_alert(row: Any) -> dict[str, Any]:
    from casino_common.alert_labels import alert_display_label
    """Map A's aml.alerts row → B API alert shape (score 0..1, rule_hits, …)."""
    data = dict(row) if not isinstance(row, dict) else dict(row)
    risk_raw = data.get("risk_score")
    try:
        risk_i = int(risk_raw) if risk_raw is not None else 0
    except (TypeError, ValueError):
        risk_i = 0
    score = risk_i / 100.0 if risk_i > 1 else float(risk_raw or 0)
    matched = data.get("matched_rules") or []
    if isinstance(matched, str):
        try:
            matched = json.loads(matched)
        except json.JSONDecodeError:
            matched = [matched]
    rule_hits = [{"rule_id": r} if isinstance(r, str) else r for r in matched]
    factors = data.get("factors") or []
    if isinstance(factors, str):
        try:
            factors = json.loads(factors)
        except json.JSONDecodeError:
            factors = []
    explanations = data.get("explanations")
    if explanations is None:
        explanations = []
        for f in factors:
            if isinstance(f, dict) and f.get("detail"):
                explanations.append(f["detail"])
            elif isinstance(f, str):
                explanations.append(f)
        if not explanations and data.get("explanation"):
            explanations = [data["explanation"]]
    triggering = data.get("triggering_ids") or []
    if isinstance(triggering, str):
        try:
            triggering = json.loads(triggering)
        except json.JSONDecodeError:
            triggering = [triggering]
    display_label = alert_display_label(
        rule_hits=rule_hits,
        matched_rules=matched,
        risk_level=str(data.get("priority") or data.get("risk_level") or "MEDIUM"),
        title=data.get("title") or data.get("alert_type"),
        summary=data.get("explanation") or data.get("summary"),
    )
    return {
        "alert_id": str(data["alert_id"]) if data.get("alert_id") is not None else None,
        "player_id": str(data["player_id"]) if data.get("player_id") is not None else None,
        "player_name": data.get("player_name"),
        "transaction_id": str(triggering[0]) if triggering else data.get("transaction_id"),
        "event_id": str(triggering[0]) if triggering else data.get("event_id"),
        "correlation_id": str(triggering[1]) if len(triggering) > 1 else data.get("correlation_id"),
        "risk_level": data.get("priority") or data.get("risk_level") or "MEDIUM",
        "status": data.get("status") or "OPEN",
        "title": display_label,
        "display_label": display_label,
        "summary": data.get("explanation") or data.get("summary") or "",
        "score": score,
        "rule_hits": rule_hits,
        "explanations": explanations,
        "matched_rules": matched,
        "created_at": _iso(data.get("created_at")),
        "updated_at": _iso(data.get("updated_at") or data.get("created_at")),
    }


def _push_live_event(item: dict[str, Any], *, broadcast: bool = True) -> bool:
    """Append a Live-Monitor row; dedupe by event_id / transaction_id."""
    event = item.get("event") or {}
    eid = str(event.get("event_id") or event.get("transaction_id") or "")
    if eid and eid in _EVENT_IDS:
        return False
    if eid:
        _EVENT_IDS.add(eid)
        if len(_EVENT_IDS) > 2000:
            # Bound memory; deque already caps EVENTS.
            _EVENT_IDS.clear()
            for existing in EVENTS:
                ex = str((existing.get("event") or {}).get("event_id") or "")
                if ex:
                    _EVENT_IDS.add(ex)
    EVENTS.appendleft(item)
    pid = str(event.get("player_id") or "")
    if pid:
        PLAYERS.setdefault(pid, {"player_id": pid, "events": 0, "alerts": 0})
        PLAYERS[pid]["events"] = PLAYERS[pid].get("events", 0) + 1
        risk = item.get("risk") or {}
        if risk.get("risk_level") is not None:
            PLAYERS[pid]["last_risk"] = risk.get("risk_level")
        if risk.get("final_score") is not None:
            PLAYERS[pid]["last_score"] = risk.get("final_score")
    if broadcast:
        _broadcast({"type": "event", "data": {"player_id": pid or None, "risk": item.get("risk")}})
    return True


def _live_item_from_alert(alert: dict[str, Any], tx: dict[str, Any] | None = None) -> dict[str, Any]:
    tx = tx or {}
    hits = alert.get("rule_hits") or []
    if isinstance(hits, str):
        try:
            hits = json.loads(hits)
        except json.JSONDecodeError:
            hits = []
    score = alert.get("score")
    try:
        score_f = float(score) if score is not None else None
    except (TypeError, ValueError):
        score_f = None
    return {
        "event": {
            "event_id": alert.get("event_id"),
            "player_id": alert.get("player_id"),
            "transaction_id": alert.get("transaction_id"),
            "correlation_id": alert.get("correlation_id"),
            "timestamp": _iso(alert.get("created_at")) or _iso(tx.get("created_at")),
            "amount": tx.get("amount"),
            "transaction_type": tx.get("transaction_type") or tx.get("event_type"),
            "event_type": tx.get("event_type") or tx.get("transaction_type"),
        },
        "risk": {
            "risk_level": alert.get("risk_level"),
            "final_score": score_f,
            "rule_hits": hits,
            "explanations": alert.get("explanations") or [],
        },
        "rules": {"hits": hits},
        "alert": {"alert_id": alert.get("alert_id"), "title": alert.get("title")},
    }


def _live_item_from_assessment(assessment: dict[str, Any], tx: dict[str, Any] | None = None) -> dict[str, Any]:
    tx = tx or {}
    hits = assessment.get("rule_hits") or []
    return {
        "event": {
            "event_id": assessment.get("event_id"),
            "player_id": assessment.get("player_id"),
            "transaction_id": assessment.get("transaction_id"),
            "correlation_id": assessment.get("correlation_id"),
            "timestamp": _iso(assessment.get("timestamp")) or _iso(tx.get("created_at")),
            "amount": tx.get("amount"),
            "transaction_type": tx.get("transaction_type") or tx.get("event_type"),
            "event_type": tx.get("event_type") or tx.get("transaction_type"),
        },
        "risk": {
            "risk_level": assessment.get("risk_level"),
            "final_score": assessment.get("final_score"),
            "rules_score": assessment.get("rules_score"),
            "ml_score": assessment.get("ml_score"),
            "behavioral_score": assessment.get("behavioral_score"),
            "rule_hits": hits,
            "explanations": assessment.get("explanations") or [],
        },
        "rules": {"hits": hits, "aggregate_score": assessment.get("rules_score")},
    }


def _lookup_transaction(transaction_id: str | None) -> dict[str, Any]:
    """Lookup by VARCHAR ids against A's public tables (txn_type / amount / direction)."""
    if not transaction_id:
        return {}
    tid = str(transaction_id)
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT amount, txn_type AS transaction_type, created_at
                    FROM transactions
                    WHERE transaction_id = :tid
                    LIMIT 1
                    """
                ),
                {"tid": tid},
            ).mappings().first()
            if row:
                out = dict(row)
                if out.get("created_at") is not None:
                    out["created_at"] = _iso(out["created_at"])
                if out.get("amount") is not None:
                    out["amount"] = float(out["amount"])
                return out
            row = conn.execute(
                text(
                    """
                    SELECT amount, game_type AS transaction_type, created_at
                    FROM wagers WHERE wager_id = :tid LIMIT 1
                    """
                ),
                {"tid": tid},
            ).mappings().first()
            if row:
                out = dict(row)
                out["event_type"] = "WAGER"
                out["transaction_type"] = out.get("transaction_type") or "WAGER"
                if out.get("created_at") is not None:
                    out["created_at"] = _iso(out["created_at"])
                if out.get("amount") is not None:
                    out["amount"] = float(out["amount"])
                return out
            row = conn.execute(
                text(
                    """
                    SELECT amount, direction AS transaction_type, created_at
                    FROM chip_transactions WHERE chip_txn_id = :tid LIMIT 1
                    """
                ),
                {"tid": tid},
            ).mappings().first()
            if row:
                out = dict(row)
                if out.get("created_at") is not None:
                    out["created_at"] = _iso(out["created_at"])
                if out.get("amount") is not None:
                    out["amount"] = float(out["amount"])
                return out
            row = conn.execute(
                text(
                    """
                    SELECT amount, direction AS transaction_type, created_at
                    FROM cage_transactions WHERE cage_txn_id = :tid LIMIT 1
                    """
                ),
                {"tid": tid},
            ).mappings().first()
            if row:
                out = dict(row)
                if out.get("created_at") is not None:
                    out["created_at"] = _iso(out["created_at"])
                if out.get("amount") is not None:
                    out["amount"] = float(out["amount"])
                return out
    except Exception as exc:  # noqa: BLE001
        log.debug("transaction lookup failed for %s: %s", transaction_id, exc)
    return {}

def hydrate_live_events_from_db(limit: int = 100) -> int:
    """Build Live Monitor rows from aml.alerts (CDC cold start)."""
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url(), pool_pre_ping=True)
        sql = """
            SELECT alert_id, player_id, player_name, alert_type, priority, risk_score,
                   matched_rules, explanation, factors, triggering_ids, status, created_at
            FROM aml.alerts
            ORDER BY created_at DESC
            LIMIT :lim
        """
        added = 0
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"lim": limit}).mappings().all()
            for row in reversed(list(rows)):
                alert = _row_to_alert(row)
                tx = _lookup_transaction(alert.get("transaction_id"))
                if _push_live_event(_live_item_from_alert(alert, tx), broadcast=False):
                    added += 1
        return added
    except Exception as exc:  # noqa: BLE001
        log.warning("hydrate_live_events_from_db failed: %s", exc)
        return 0


_JEV_LOCK = threading.Lock()
_JEV_INFLIGHT: set[str] = set()


def _schedule_openjev(alert: dict[str, Any], *, force: bool = False) -> str:
    """Run OpenJev in a background thread so HTTP routes stay under timeout limits."""
    from casino_common.jev_decision import _needs_live_refresh, attach_decision, jev_mode

    aid = str(alert.get("alert_id") or "")
    if not aid:
        return "missing_id"
    mode = jev_mode()
    if mode in ("off", "disabled", "false", "0"):
        return "off"
    if not force and not _needs_live_refresh(alert.get("jev_decision")):
        return "ready"
    with _JEV_LOCK:
        if aid in _JEV_INFLIGHT:
            return "pending"
        _JEV_INFLIGHT.add(aid)
    alert["jev_pending"] = True
    if not isinstance(alert.get("jev_decision"), dict):
        alert["jev_decision"] = {
            "mode": "pending",
            "disposition": "need_info",
            "rationale": "OpenJev running…",
            "suggested_status": "INVESTIGATING",
        }

    def _run() -> None:
        try:
            attach_decision(alert, force=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("OpenJev background decide failed for %s: %s", aid, exc)
        finally:
            alert["jev_pending"] = False
            with _JEV_LOCK:
                _JEV_INFLIGHT.discard(aid)

    threading.Thread(target=_run, name=f"openjev-{aid[:8]}", daemon=True).start()
    return "pending"


def _backfill_openjev(limit: int = 12) -> int:
    """Queue live OpenJev for recent alerts that still lack a real decision."""
    from casino_common.jev_decision import _needs_live_refresh, jev_mode

    if jev_mode() not in ("live", "openjev"):
        return 0
    auto = (os.getenv("JEV_AUTO_LIVE") or "true").strip().lower()
    if auto in ("0", "false", "no", "off"):
        return 0
    queued = 0
    for a in list(ALERTS):
        if queued >= limit:
            break
        if not _needs_live_refresh(a.get("jev_decision")) and not a.get("jev_pending"):
            continue
        status = _schedule_openjev(a, force=False)
        if status == "pending":
            queued += 1
    return queued


def _ingest_alert(alert: dict[str, Any], *, skip_jev: bool = False) -> None:
    aid = str(alert.get("alert_id") or "")
    if not aid or aid in _ALERT_IDS:
        return
    # OpenJev: auto-run live in background (default). Bulk hydrate skips sync call
    # then queues a backfill so the Investigate UI is not empty.
    if not skip_jev:
        try:
            from casino_common.jev_decision import attach_decision, jev_mode

            mode = jev_mode()
            auto = (os.getenv("JEV_AUTO_LIVE") or "true").strip().lower()
            auto_on = auto in ("1", "true", "yes", "on", "")
            if mode in ("live", "openjev") and auto_on:
                # Prefer async so Kafka / demo inject paths do not block on Verdict CPU.
                if alert.get("jev_decision") and str(
                    (alert.get("jev_decision") or {}).get("mode") or ""
                ).lower() in ("openjev", "live"):
                    pass
                else:
                    _schedule_openjev(alert, force=False)
            else:
                attach_decision(alert)
        except Exception as exc:  # noqa: BLE001
            log.warning("Jev decision attach skipped: %s", exc)
    _ALERT_IDS.add(aid)
    ALERTS.insert(0, alert)
    pid = str(alert.get("player_id") or "")
    if pid:
        PLAYERS.setdefault(pid, {"player_id": pid, "events": 0, "alerts": 0})
        PLAYERS[pid]["alerts"] = PLAYERS[pid].get("alerts", 0) + 1
        PLAYERS[pid]["last_risk"] = alert.get("risk_level")
        PLAYERS[pid]["last_score"] = alert.get("score")
    ALERTS_GAUGE.set(sum(1 for a in ALERTS if a.get("status") == "OPEN"))
    _broadcast({"type": "alert", "data": alert})
    eid = str(alert.get("event_id") or alert.get("transaction_id") or "")
    if eid and eid in _EVENT_IDS:
        return
    tx = _lookup_transaction(str(alert.get("transaction_id") or "") or None)
    _push_live_event(_live_item_from_alert(alert, tx))


def _ingest_risk_assessment(assessment: dict[str, Any]) -> None:
    if not isinstance(assessment, dict) or not assessment.get("event_id"):
        return
    eid = str(assessment.get("event_id") or "")
    if eid and eid in _EVENT_IDS:
        return
    score = float(assessment.get("final_score") or 0)
    level = str(assessment.get("risk_level") or "LOW").upper()
    threshold = float(os.getenv("ALERT_THRESHOLD", "0.55"))
    # Avoid flooding Live Monitor with seed/catch-up LOW noise (same bar as alert-service).
    if score < threshold and level in ("LOW",):
        return
    # Skip per-message DB lookups; hydrate/alerts supply amounts for cold start.
    if _push_live_event(_live_item_from_assessment(assessment, None)):
        PIPELINE_EVENTS.labels(stage="risk").inc()


def process_event(event: dict[str, Any]) -> dict[str, Any]:
    """Local synchronous pipeline for demo / API inject path (PIPELINE_MODE=local only)."""
    player_id = str(event.get("player_id") or f"D{uuid4().hex[:8].upper()}")[:32]
    event_id = str(event.get("event_id") or f"EVT-{uuid4().hex[:12].upper()}")[:64]
    correlation_id = str(event.get("correlation_id") or f"COR-{uuid4().hex[:12].upper()}")[:64]
    transaction_id = event.get("transaction_id")
    set_context(
        correlation_id=correlation_id,
        event_id=event_id,
        player_id=player_id,
        transaction_id=str(transaction_id) if transaction_id else None,
    )
    event = {
        **event,
        "event_id": event_id,
        "player_id": player_id,
        "correlation_id": correlation_id,
        "timestamp": event.get("timestamp") or datetime.now(timezone.utc).isoformat(),
    }
    if event.get("device_fingerprint") and event.get("scenario_tag") == "shared_device":
        event["shared_device_count"] = 5
        feature_engine.update_static(player_id, shared_device_count=5.0)

    PIPELINE_EVENTS.labels(stage="normalize").inc()
    fv = feature_engine.ingest(player_id, event)
    PIPELINE_EVENTS.labels(stage="enrich").inc()
    features = fv.flatten()
    rules = evaluate_rules(
        player_id=player_id,
        event=event,
        features=features,
        event_id=event_id,
        correlation_id=correlation_id,
        transaction_id=str(transaction_id) if transaction_id else None,
    )
    PIPELINE_EVENTS.labels(stage="rules").inc()
    pred = aml_model.predict(features)
    ml = MLPrediction(
        event_id=event_id,
        player_id=player_id,
        transaction_id=str(transaction_id) if transaction_id else None,
        correlation_id=correlation_id,
        timestamp=datetime.now(timezone.utc),
        model_name=pred["model_name"],
        model_version=pred["model_version"],
        score=pred["score"],
        label=pred["label"],
        features_used=pred["features_used"],
        shap_top=pred["shap_top"],
    )
    beh = behavioral.score(player_id, event, features, event_id, correlation_id)
    PIPELINE_EVENTS.labels(stage="ml").inc()
    assessment = aggregate(rules, ml, beh, features=features)
    PIPELINE_EVENTS.labels(stage="risk").inc()
    exp = explain(assessment)

    result = {
        "event": event,
        "features": features,
        "rules": rules.model_dump(mode="json"),
        "ml": ml.model_dump(mode="json"),
        "behavioral": beh.model_dump(mode="json"),
        "risk": assessment.model_dump(mode="json"),
        "explanation": exp,
    }
    EVENTS.appendleft(result)
    PLAYERS.setdefault(player_id, {"player_id": player_id, "events": 0, "alerts": 0})
    PLAYERS[player_id]["events"] += 1
    PLAYERS[player_id]["last_risk"] = assessment.risk_level.value
    PLAYERS[player_id]["last_score"] = assessment.final_score

    threshold = float(os.getenv("ALERT_THRESHOLD", "0.40"))
    if (
        assessment.final_score >= threshold
        or assessment.risk_level in (RiskLevel.HIGH, RiskLevel.CRITICAL)
        or (assessment.rule_hits and assessment.final_score >= 0.40)
    ):
        from casino_common.alert_labels import alert_display_label

        display_label = alert_display_label(
            rule_hits=[h.model_dump(mode="json") for h in assessment.rule_hits],
            risk_level=assessment.risk_level.value,
            summary="; ".join(assessment.explanations)[:2000],
            ml_score=float(ml.score) if ml else None,
            behavioral_score=float(beh.score) if beh else None,
        )
        alert = {
            "alert_id": str(uuid4()),
            "player_id": player_id,
            "transaction_id": transaction_id,
            "event_id": event_id,
            "correlation_id": correlation_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "risk_level": assessment.risk_level.value,
            "status": AlertStatus.OPEN.value,
            "title": display_label,
            "display_label": display_label,
            "summary": "; ".join(assessment.explanations)[:2000],
            "score": assessment.final_score,
            "rule_hits": [h.model_dump(mode="json") for h in assessment.rule_hits],
            "explanations": assessment.explanations,
        }
        _ingest_alert(alert)
        PIPELINE_EVENTS.labels(stage="alert").inc()
        result["alert"] = alert
    _broadcast({"type": "event", "data": {"player_id": player_id, "risk": assessment.model_dump(mode="json")}})
    log.info("processed risk=%s score=%.2f", assessment.risk_level.value, assessment.final_score)
    return result


def _ensure_player(
    conn,
    player_id: str,
    scenario_tag: str | None = None,
    device_fingerprint: str | None = None,
) -> None:
    """Upsert demo player + optional shared device into A's VARCHAR schema."""
    from sqlalchemy import text

    pid = str(player_id)[:32]
    conn.execute(
        text(
            """
            INSERT INTO players (
                player_id, first_name, last_name, country, date_of_birth, email, status, demo_label
            ) VALUES (
                :pid, 'Demo', 'Player', 'USA', '1990-01-01', :email, 'ACTIVE', :label
            )
            ON CONFLICT (player_id) DO NOTHING
            """
        ),
        {
            "pid": pid,
            "email": f"{pid.lower()}@demo.casino",
            "label": scenario_tag or "Demo",
        },
    )
    if scenario_tag == "shared_device":
        fp = device_fingerprint or "shared-device-fingerprint-demo"
        exists = conn.execute(
            text(
                """
                SELECT 1 FROM devices
                WHERE player_id = :pid AND fingerprint = :fp
                LIMIT 1
                """
            ),
            {"pid": pid, "fp": fp},
        ).first()
        if not exists:
            conn.execute(
                text(
                    """
                    INSERT INTO devices (device_id, player_id, device_type, fingerprint, last_seen_at)
                    VALUES (:did, :pid, 'MOBILE', :fp, now())
                    ON CONFLICT (device_id) DO UPDATE SET last_seen_at = now()
                    """
                ),
                {"did": f"DEV-{pid}"[:32], "pid": pid, "fp": fp},
            )

def write_events_to_postgres(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Persist demo events so Debezium CDC feeds the real Kafka path (A VARCHAR schema)."""
    from sqlalchemy import create_engine, text

    engine = create_engine(database_url(), pool_pre_ping=True)
    written: list[dict[str, Any]] = []
    with engine.begin() as conn:
        for event in events:
            pid = str(event.get("player_id") or f"D{uuid4().hex[:8].upper()}")[:32]
            tid = str(event.get("transaction_id") or f"TXN-{uuid4().hex[:12].upper()}")[:64]
            amount = float(event.get("amount") or 0)
            tx_type = (event.get("transaction_type") or event.get("event_type") or "TRANSACTION").upper()
            channel = event.get("channel") or event.get("payment_method") or "ONLINE"
            location = event.get("location")
            tag = event.get("scenario_tag")
            ts = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
            _ensure_player(conn, pid, tag, event.get("device_fingerprint"))

            et = (event.get("event_type") or "").upper()
            if et == "WAGER" or tx_type == "WAGER":
                conn.execute(
                    text(
                        """
                        INSERT INTO wagers (
                            wager_id, player_id, session_id, game_type, amount, win_amount, location, created_at
                        ) VALUES (
                            :tid, :pid, :sid, :game, :amt, 0, :loc, CAST(:ts AS timestamptz)
                        )
                        ON CONFLICT (wager_id) DO NOTHING
                        """
                    ),
                    {
                        "tid": tid,
                        "pid": pid,
                        "sid": f"SES-{pid}"[:64],
                        "game": event.get("game_type") or "SLOTS",
                        "amt": amount,
                        "loc": location,
                        "ts": ts,
                    },
                )
            elif et == "CHIP" or tx_type in ("CHIP_BUY", "CHIP_CASH", "CHIP_IN", "CHIP_OUT"):
                direction = tx_type if tx_type in ("CHIP_BUY", "CHIP_CASH", "CHIP_IN", "CHIP_OUT") else "CHIP_BUY"
                conn.execute(
                    text(
                        """
                        INSERT INTO chip_transactions (
                            chip_txn_id, player_id, direction, amount, location, created_at
                        ) VALUES (
                            :tid, :pid, :tt, :amt, :loc, CAST(:ts AS timestamptz)
                        )
                        ON CONFLICT (chip_txn_id) DO NOTHING
                        """
                    ),
                    {"tid": tid, "pid": pid, "tt": direction, "amt": amount, "loc": location, "ts": ts},
                )
            else:
                # Map B-ish types onto A's txn_type vocabulary
                if tx_type in ("TRANSACTION", "PLAYER", "KYC", "CHIP", "CAGE", "DEPOSIT"):
                    tx_type = "CASH_IN" if channel in ("CAGE", "CASH") or (event.get("payment_method") or "").upper() == "CASH" else "BUY_IN"
                if tx_type == "WITHDRAWAL":
                    tx_type = "CASH_OUT"
                if tx_type in ("WIRE",):
                    tx_type = "WIRE_IN"
                conn.execute(
                    text(
                        """
                        INSERT INTO transactions (
                            transaction_id, player_id, txn_type, amount, currency,
                            location, payment_method, created_at
                        ) VALUES (
                            :tid, :pid, :tt, :amt, :cur, :loc, :pm, CAST(:ts AS timestamptz)
                        )
                        ON CONFLICT (transaction_id) DO NOTHING
                        """
                    ),
                    {
                        "tid": tid,
                        "pid": pid,
                        "tt": tx_type,
                        "amt": amount,
                        "cur": event.get("currency") or "USD",
                        "loc": location,
                        "pm": event.get("payment_method") or channel,
                        "ts": ts,
                    },
                )
                if tx_type in ("CASH_IN", "CASH_OUT", "WIRE_IN", "WIRE_OUT") or (event.get("payment_method") or "").upper() == "CASH":
                    conn.execute(
                        text(
                            """
                            INSERT INTO cage_transactions (
                                cage_txn_id, player_id, direction, amount, cage_window, teller_id, created_at
                            ) VALUES (
                                :cid, :pid, :tt, :amt, :loc, :teller, CAST(:ts AS timestamptz)
                            )
                            ON CONFLICT (cage_txn_id) DO NOTHING
                            """
                        ),
                        {
                            "cid": f"CAG-{tid[4:]}"[:64] if tid.startswith("TXN-") else f"CAG-{tid}"[:64],
                            "pid": pid,
                            "tt": tx_type if tx_type in ("CASH_IN", "CASH_OUT", "WIRE_IN", "WIRE_OUT") else "CASH_IN",
                            "amt": amount,
                            "loc": location or "Cage",
                            "teller": "T-DEMO",
                            "ts": ts,
                        },
                    )
            written.append({"player_id": pid, "transaction_id": tid, "event_type": et or tx_type})
            PLAYERS.setdefault(pid, {"player_id": pid, "events": 0, "alerts": 0})
            PLAYERS[pid]["events"] = PLAYERS[pid].get("events", 0) + 1
            PIPELINE_EVENTS.labels(stage="cdc_write").inc()
    return written

def _as_utc(dt: datetime | None) -> datetime | None:
    """Normalize DB/driver datetimes for safe comparison with aware UTC clocks."""
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _alert_created_at(alert: dict[str, Any]) -> datetime | None:
    raw = alert.get("created_at")
    if isinstance(raw, datetime):
        return _as_utc(raw)
    if isinstance(raw, str):
        return _as_utc(raw)
    return None


def hydrate_alerts_from_db(player_ids: list[str] | None = None, since: datetime | None = None) -> int:
    """Pull alerts written by alert-service into the in-memory store."""
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url(), pool_pre_ping=True)
        sql = """
            SELECT alert_id, player_id, player_name, alert_type, priority, risk_score,
                   matched_rules, explanation, factors, triggering_ids, status, created_at
            FROM aml.alerts
            ORDER BY created_at DESC LIMIT 200
        """
        added = 0
        pid_set = set(player_ids or [])
        since_utc = _as_utc(since)
        # Small skew so we do not miss rows stamped just before the inject clock.
        if since_utc is not None:
            since_utc = since_utc - timedelta(seconds=2)
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).mappings().all()
            for row in rows:
                alert = _row_to_alert(row)
                created = _as_utc(row.get("created_at"))
                if since_utc is not None and created is not None and created < since_utc:
                    continue
                if pid_set and alert.get("player_id") not in pid_set:
                    continue
                before = len(_ALERT_IDS)
                _ingest_alert(alert, skip_jev=True)
                if len(_ALERT_IDS) > before:
                    added += 1
        if added:
            _backfill_openjev(limit=min(12, max(added, 4)))
        return added
    except Exception as exc:  # noqa: BLE001
        log.warning("hydrate_alerts_from_db failed: %s", exc)
        return 0


def _matching_demo_alerts(player_ids: list[str], since: datetime) -> list[dict[str, Any]]:
    """Alerts for demo players created at/after inject (also accepts Kafka consumer path)."""
    since_utc = _as_utc(since)
    if since_utc is not None:
        since_utc = since_utc - timedelta(seconds=2)
    pid_set = set(player_ids)
    out: list[dict[str, Any]] = []
    for a in ALERTS:
        if pid_set and a.get("player_id") not in pid_set:
            continue
        created = _alert_created_at(a)
        if since_utc is not None and created is not None and created < since_utc:
            continue
        out.append(a)
    return out


def run_cdc_demo(events: list[dict[str, Any]], wait_seconds: int | None = None) -> dict[str, Any]:
    """OpenShift path: write DB rows and poll aml.alerts until demo alerts appear."""
    wait = wait_seconds if wait_seconds is not None else CDC_WAIT_SECONDS
    started = datetime.now(timezone.utc)
    player_ids = list({str(e.get("player_id")) for e in events if e.get("player_id")})
    written = write_events_to_postgres(events)
    deadline = time.time() + wait
    matching: list[dict[str, Any]] = []
    while time.time() < deadline:
        hydrate_alerts_from_db(player_ids=player_ids or None, since=started)
        matching = _matching_demo_alerts(player_ids, started)
        if matching:
            break
        time.sleep(2)
    waited = int(time.time() - started.timestamp())
    if not matching:
        # Final hydrate without since skew recovery — still scoped to demo players.
        hydrate_alerts_from_db(player_ids=player_ids or None, since=started)
        matching = _matching_demo_alerts(player_ids, started)
    timed_out = len(matching) == 0
    if timed_out:
        log.warning(
            "CDC demo wait timed out after %ss (players=%s); pipeline may still be catching up",
            waited,
            player_ids[:5],
        )
    return {
        "pipeline_mode": "cdc",
        "written": written,
        "waited_seconds": waited,
        "wait_timeout_seconds": wait,
        "timed_out": timed_out,
        "alerts": matching[:20],
        "alerts_generated": len(matching),
        "message": (
            None
            if matching
            else (
                f"No alerts within {wait}s. CDC may still be draining seed backlog; "
                "retry GET /api/v1/alerts or re-run demo shortly."
            )
        ),
    }


def _alert_consumer_loop() -> None:
    try:
        from casino_common.kafka_utils import JsonConsumer

        topic = os.getenv("ALERTS_TOPIC", "aml.alerts")
        consumer = JsonConsumer("casino-api-alerts", [topic])
        log.info("API alert consumer listening on %s", topic)
        while True:
            item = consumer.poll_json(1.0)
            if item is None:
                continue
            _, value = item
            if isinstance(value, dict) and value.get("alert_id"):
                _ingest_alert(value)
                PIPELINE_EVENTS.labels(stage="alert").inc()
    except Exception as exc:  # noqa: BLE001
        log.warning("alert consumer stopped: %s", exc)
        PIPELINE_STATUS["ALERT"] = {"ok": False, "detail": str(exc)}


def _risk_consumer_loop() -> None:
    """CDC Live Monitor: stream *new* scored assessments (latest offset) into EVENTS + SSE."""
    try:
        from confluent_kafka import Consumer

        from casino_common.kafka_utils import kafka_bootstrap

        topic = os.getenv("RISK_TOPIC", "aml.risk.assessments")
        cfg: dict[str, Any] = {
            "bootstrap.servers": kafka_bootstrap(),
            "group.id": os.getenv("API_RISK_GROUP", "casino-api-risk-live-v2"),
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
        security = os.getenv("KAFKA_SECURITY_PROTOCOL")
        if security:
            cfg["security.protocol"] = security
        consumer = Consumer(cfg)
        consumer.subscribe([topic])
        log.info("API risk consumer listening on %s (offset=latest)", topic)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("risk consumer kafka error: %s", msg.error())
                continue
            try:
                value = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                log.warning("risk consumer decode failed: %s", exc)
                continue
            if isinstance(value, dict):
                _ingest_risk_assessment(value)
    except Exception as exc:  # noqa: BLE001
        log.warning("risk consumer stopped: %s", exc)
        PIPELINE_STATUS["RISK"] = {"ok": False, "detail": str(exc)}


def _broadcast(msg: dict[str, Any]) -> None:
    dead = []
    for q in SSE_SUBSCRIBERS:
        try:
            q.put_nowait(msg)
        except Exception:  # noqa: BLE001
            dead.append(q)
    for q in dead:
        if q in SSE_SUBSCRIBERS:
            SSE_SUBSCRIBERS.remove(q)


@app.get("/health")
def health():
    REQUESTS.labels(endpoint="health").inc()
    return {
        "status": "ok",
        "service": "casino-sentinel-api",
        "pipeline_mode": PIPELINE_MODE,
    }


@app.get("/healthz")
def healthz():
    """Alias used by A's UI nginx / OpenShift probes."""
    return health()


@app.get("/ready")
def ready():
    REQUESTS.labels(endpoint="ready").inc()
    return {"status": "ready", "model_loaded": aml_model.model is not None, "pipeline_mode": PIPELINE_MODE}


@app.get("/metrics")
def metrics():
    return StreamingResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/pipeline/status")
def pipeline_status():
    REQUESTS.labels(endpoint="pipeline").inc()
    stages = ["DATABASE", "CDC", "STREAM", "ENRICH", "ANALYZE", "RISK", "ALERT"]
    events_n = len(EVENTS)
    alerts_n = len(ALERTS)
    stage_activity = {
        "DATABASE": events_n + alerts_n,
        "CDC": events_n + alerts_n if PIPELINE_MODE == "cdc" else 0,
        "STREAM": events_n + alerts_n if PIPELINE_MODE == "cdc" else events_n,
        "ENRICH": events_n,
        "ANALYZE": events_n,
        "RISK": events_n,
        "ALERT": alerts_n,
    }

    deploy_health = _pipeline_component_health()
    tfm_mode = (os.getenv("TFM_MODE") or "hashed").strip().lower()
    tfm_url = (os.getenv("TFM_EMBED_URL") or "").strip()
    ai_mode = (os.getenv("AI_MODE") or "mock").strip().lower()
    tfm_ok, tfm_detail = _probe_tfm_embed(tfm_mode, tfm_url, deploy_health)
    # Canonical TFM entry for the flow diagram (hashed/mock = optional healthy).
    deploy_health["tfm-embed"] = {"ok": tfm_ok, "detail": tfm_detail}
    jev_ok, jev_detail, jev_meta = _probe_openjev(deploy_health)
    deploy_health["openjev"] = {"ok": jev_ok, "detail": jev_detail}

    # Stage strip: prefer live component probes over static PIPELINE_STATUS.
    stage_keys: dict[str, list[str]] = {
        "DATABASE": ["casino-db"],
        "CDC": ["sentinel-connect", "casino-postgres-connector"],
        "STREAM": ["sentinel-kafka"],
        "ENRICH": ["sentinel-event-normalizer", "sentinel-feature-engine"],
        "ANALYZE": ["sentinel-aml-rules", "sentinel-ml-engine"],
        "RISK": ["sentinel-risk-engine"],
        "ALERT": ["sentinel-alert-service"],
    }
    stage_list = []
    for s in stages:
        keys = stage_keys.get(s) or []
        live = [deploy_health[k] for k in keys if k in deploy_health]
        if live:
            ok = all(bool(h.get("ok")) for h in live)
            detail = "; ".join(str(h.get("detail") or "") for h in live if h.get("detail"))
        else:
            st = PIPELINE_STATUS.get(s) or {}
            ok = bool(st.get("ok"))
            detail = str(st.get("detail") or "")
        stage_list.append(
            {
                "name": s,
                "ok": ok,
                "detail": detail or None,
                "activity": stage_activity.get(s, 0),
            }
        )

    def _node(
        nid: str,
        label: str,
        *,
        kind: str,
        stage: str | None = None,
        deploy: str | None = None,
        ok: bool | None = None,
        detail: str = "",
        activity: int = 0,
        optional: bool = False,
    ) -> dict[str, Any]:
        # Live probes always win when present (never mask a down CR/deploy with static ok).
        if deploy and deploy in deploy_health:
            dh = deploy_health[deploy]
            ok = bool(dh.get("ok"))
            live_detail = str(dh.get("detail") or "").strip()
            if live_detail:
                detail = live_detail if not detail else f"{live_detail} | {detail}"
        elif stage and stage in PIPELINE_STATUS and ok is None:
            ok = bool(PIPELINE_STATUS[stage].get("ok"))
            detail = detail or str(PIPELINE_STATUS[stage].get("detail") or "")
        if ok is None:
            ok = True
        flowing = bool(ok) and activity > 0
        return {
            "id": nid,
            "label": label,
            "kind": kind,
            "stage": stage,
            "ok": bool(ok),
            "status": "healthy" if ok else "down",
            "detail": detail,
            "activity": activity,
            "flowing": flowing,
            "optional": optional,
        }

    cdc_detail = ""
    conn = deploy_health.get("casino-postgres-connector")
    if conn:
        cdc_detail = f"connector={'ok' if conn.get('ok') else 'down'}: {conn.get('detail') or ''}"

    components = [
        _node(
            "db",
            "PostgreSQL",
            kind="storage",
            stage="DATABASE",
            deploy="casino-db",
            activity=stage_activity["DATABASE"],
        ),
        _node(
            "cdc",
            "Debezium Connect",
            kind="ingest",
            stage="CDC",
            deploy="sentinel-connect",
            detail=cdc_detail,
            activity=stage_activity["CDC"],
        ),
        _node(
            "kafka",
            "AMQ Streams",
            kind="bus",
            stage="STREAM",
            deploy="sentinel-kafka",
            activity=stage_activity["STREAM"],
        ),
        _node(
            "normalizer",
            "event-normalizer",
            kind="service",
            stage="STREAM",
            deploy="sentinel-event-normalizer",
            activity=stage_activity["STREAM"],
        ),
        _node(
            "features",
            "feature-engine",
            kind="service",
            stage="ENRICH",
            deploy="sentinel-feature-engine",
            activity=stage_activity["ENRICH"],
        ),
        _node(
            "rules",
            "aml-rules",
            kind="service",
            stage="ANALYZE",
            deploy="sentinel-aml-rules",
            activity=stage_activity["ANALYZE"],
        ),
        _node(
            "ml",
            "ml-engine",
            kind="service",
            stage="ANALYZE",
            deploy="sentinel-ml-engine",
            detail=f"AI_MODE={ai_mode} TFM_MODE={tfm_mode}",
            activity=stage_activity["ANALYZE"],
        ),
        _node(
            "tfm",
            "TFM embeddings",
            kind="gpu",
            deploy="tfm-embed",
            detail=tfm_detail,
            activity=stage_activity["ANALYZE"] if tfm_mode == "http" and tfm_ok else 0,
            optional=tfm_mode != "http",
        ),
        _node(
            "risk",
            "risk-engine",
            kind="service",
            stage="RISK",
            deploy="sentinel-risk-engine",
            activity=stage_activity["RISK"],
        ),
        _node(
            "alerts",
            "alert-service",
            kind="service",
            stage="ALERT",
            deploy="sentinel-alert-service",
            activity=stage_activity["ALERT"],
        ),
        _node(
            "openjev",
            "OpenJev disposition",
            kind="decide",
            deploy="openjev",
            detail=jev_detail,
            activity=alerts_n if jev_ok and not jev_meta.get("optional") else 0,
            optional=bool(jev_meta.get("optional")),
            ok=jev_ok,
        ),
        _node(
            "api",
            "sentinel-api",
            kind="api",
            deploy="sentinel-api",
            activity=events_n + alerts_n,
        ),
        _node(
            "ui",
            "Casino Guard UI",
            kind="ui",
            deploy="sentinel-ui",
            activity=0,
        ),
    ]

    # Edges: traffic flows left→right when both ends are healthy and source has activity
    by_id = {c["id"]: c for c in components}
    edge_defs = [
        ("db", "cdc"),
        ("cdc", "kafka"),
        ("kafka", "normalizer"),
        ("normalizer", "features"),
        ("features", "rules"),
        ("features", "ml"),
        ("ml", "tfm"),
        ("tfm", "ml"),
        ("rules", "risk"),
        ("ml", "risk"),
        ("risk", "alerts"),
        ("alerts", "api"),
        ("alerts", "openjev"),
        ("openjev", "api"),
        ("api", "ui"),
    ]
    edges = []
    for src, dst in edge_defs:
        a, b = by_id.get(src), by_id.get(dst)
        if not a or not b:
            continue
        if a.get("optional") and not a.get("ok"):
            continue
        if dst == "tfm" and tfm_mode != "http":
            continue
        if src == "tfm" and tfm_mode != "http":
            continue
        # Hide OpenJev edges when disposition is fully off
        if "openjev" in (src, dst) and jev_meta.get("mode") in ("off", "disabled"):
            continue
        flowing = bool(a.get("ok") and b.get("ok") and (a.get("activity") or 0) > 0)
        edges.append({"from": src, "to": dst, "flowing": flowing})

    return {
        "pipeline_mode": PIPELINE_MODE,
        "stages": stage_list,
        "components": components,
        "edges": edges,
        "tfm": {"mode": tfm_mode, "url": tfm_url, "ok": tfm_ok, "detail": tfm_detail},
        "openjev": {
            "mode": jev_meta.get("mode"),
            "model": jev_meta.get("model"),
            "url": jev_meta.get("url"),
            "ok": jev_ok,
            "detail": jev_detail,
            "optional": bool(jev_meta.get("optional")),
        },
        "ai_mode": ai_mode,
        "uptime_seconds": int(time.time() - START_TIME),
        "events_buffered": events_n,
        "open_alerts": sum(1 for a in ALERTS if a["status"] == "OPEN"),
        "note": (
            "OpenShift CDC path: Postgres → Debezium → Kafka → microservices → OpenJev disposition"
            if PIPELINE_MODE == "cdc"
            else "Local in-process path (API-only); not used on OpenShift"
        ),
    }


def _pipeline_component_health() -> dict[str, dict[str, Any]]:
    """Live health for pipeline components (in-cluster). Empty when not on OpenShift.

    Combines Deployment Ready, CR Ready conditions, TCP reachability, SQL ping,
    and HTTP /healthz where a Service exists.
    """
    try:
        ns, headers, ca = _k8s_headers()
    except HTTPException:
        return {}
    except Exception:  # noqa: BLE001
        return {}

    from urllib import error as urlerror
    from urllib import request as urlrequest
    import socket
    import ssl

    ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
    out: dict[str, dict[str, Any]] = {}

    def _get(path: str, timeout: float = 3.0) -> dict[str, Any] | None:
        url = f"https://kubernetes.default.svc{path}"
        try:
            req = urlrequest.Request(url, headers=headers, method="GET")
            with urlrequest.urlopen(req, context=ctx, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return None

    def _ready_condition(status: dict[str, Any] | None) -> tuple[bool, str]:
        if not status:
            return False, "no status"
        for c in status.get("conditions") or []:
            if c.get("type") == "Ready":
                ok = str(c.get("status")).lower() == "true"
                reason = c.get("reason") or c.get("message") or ("Ready" if ok else "NotReady")
                return ok, str(reason)[:120]
        return False, "no Ready condition"

    def _deploy(name: str) -> tuple[bool, str]:
        body = _get(f"/apis/apps/v1/namespaces/{ns}/deployments/{name}")
        if not body:
            return False, "Deployment missing/unreachable"
        st = body.get("status") or {}
        ready = int(st.get("readyReplicas") or 0)
        desired = int(st.get("replicas") or (body.get("spec") or {}).get("replicas") or 0)
        return ready >= 1, f"deploy {ready}/{desired or 1} ready"

    def _tcp(host: str, port: int, timeout: float = 2.0) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    def _http(url: str, timeout: float = 3.0) -> tuple[bool, str]:
        try:
            req = urlrequest.Request(url, method="GET")
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                if 200 <= resp.status < 300:
                    return True, f"HTTP {resp.status}"
                return False, f"HTTP {resp.status}"
        except urlerror.HTTPError as exc:
            return False, f"HTTP {exc.code}"
        except Exception as exc:  # noqa: BLE001
            return False, f"{exc.__class__.__name__}"

    def _cr_first(paths: list[str]) -> dict[str, Any] | None:
        for p in paths:
            body = _get(p)
            if body is not None:
                return body
        return None

    # --- PostgreSQL: Deployment + live SQL ---
    db_deploy_ok, db_deploy_detail = _deploy("casino-db")
    db_tcp = _tcp("casino-db", 5432)
    db_sql_ok, db_sql_detail = False, "skip"
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_sql_ok, db_sql_detail = True, "SELECT 1 ok"
    except Exception as exc:  # noqa: BLE001
        db_sql_ok, db_sql_detail = False, f"SQL {exc.__class__.__name__}"
    db_ok = db_deploy_ok and db_tcp and db_sql_ok
    out["casino-db"] = {
        "ok": db_ok,
        "detail": f"{db_deploy_detail}; tcp={db_tcp}; {db_sql_detail}",
    }

    # --- AMQ Streams Kafka CR + bootstrap TCP ---
    kafka_body = _cr_first(
        [
            f"/apis/kafka.strimzi.io/v1/namespaces/{ns}/kafkas/sentinel-kafka",
            f"/apis/kafka.strimzi.io/v1beta2/namespaces/{ns}/kafkas/sentinel-kafka",
        ]
    )
    kafka_cr_ok, kafka_cr_detail = _ready_condition((kafka_body or {}).get("status"))
    if kafka_body is None:
        kafka_cr_ok, kafka_cr_detail = False, "Kafka CR unreachable"
    kafka_tcp = _tcp("sentinel-kafka-kafka-bootstrap", 9092)
    out["sentinel-kafka"] = {
        "ok": kafka_cr_ok and kafka_tcp,
        "detail": f"CR {kafka_cr_detail}; bootstrap tcp={kafka_tcp}",
    }

    # --- KafkaConnect CR + Deployment + REST ---
    connect_body = _cr_first(
        [
            f"/apis/kafka.strimzi.io/v1/namespaces/{ns}/kafkaconnects/sentinel-connect",
            f"/apis/kafka.strimzi.io/v1beta2/namespaces/{ns}/kafkaconnects/sentinel-connect",
        ]
    )
    connect_cr_ok, connect_cr_detail = _ready_condition((connect_body or {}).get("status"))
    if connect_body is None:
        connect_cr_ok, connect_cr_detail = False, "KafkaConnect CR unreachable"
    connect_deploy_ok, connect_deploy_detail = _deploy("sentinel-connect-connect")
    if not connect_deploy_ok:
        # AMQ Streams 3.x / Strimzi often uses StrimziPodSet (no Deployment).
        pods = _get(
            f"/api/v1/namespaces/{ns}/pods?labelSelector="
            f"strimzi.io/cluster%3Dsentinel-connect,strimzi.io/kind%3DKafkaConnect"
        )
        running = 0
        if pods:
            for p in pods.get("items") or []:
                phase = (p.get("status") or {}).get("phase")
                ready = all(
                    (c.get("ready") is True)
                    for c in ((p.get("status") or {}).get("containerStatuses") or [])
                ) if (p.get("status") or {}).get("containerStatuses") else False
                if phase == "Running" and ready:
                    running += 1
        if running >= 1:
            connect_deploy_ok, connect_deploy_detail = True, f"pod {running} ready"
        else:
            connect_deploy_detail = "no Connect Deployment/pod ready"
    connect_http_ok, connect_http_detail = _http(
        "http://sentinel-connect-connect-api:8083/", timeout=3.0
    )
    out["sentinel-connect"] = {
        "ok": connect_cr_ok and (connect_deploy_ok or connect_http_ok),
        "detail": (
            f"CR {connect_cr_detail}; {connect_deploy_detail}; "
            f"REST {connect_http_detail}"
        ),
    }

    # --- Debezium KafkaConnector ---
    connector_body = _cr_first(
        [
            f"/apis/kafka.strimzi.io/v1/namespaces/{ns}/kafkaconnectors/casino-postgres-connector",
            f"/apis/kafka.strimzi.io/v1beta2/namespaces/{ns}/kafkaconnectors/casino-postgres-connector",
        ]
    )
    if connector_body is not None:
        c_ok, c_detail = _ready_condition(connector_body.get("status"))
        out["casino-postgres-connector"] = {"ok": c_ok, "detail": f"KafkaConnector {c_detail}"}
    else:
        out["casino-postgres-connector"] = {
            "ok": False,
            "detail": "KafkaConnector CR unreachable",
        }

    # --- Microservice Deployments (Kafka workers have no HTTP Service) ---
    for name in (
        "sentinel-event-normalizer",
        "sentinel-feature-engine",
        "sentinel-aml-rules",
        "sentinel-ml-engine",
        "sentinel-risk-engine",
        "sentinel-alert-service",
        "sentinel-api",
        "sentinel-ui",
        "openjev",
    ):
        ok, detail = _deploy(name)
        # Live HTTP for API/UI (have ClusterIP Services)
        if name == "sentinel-api":
            h_ok, h_detail = _http("http://sentinel-api:8080/healthz")
            ok = ok and h_ok
            detail = f"{detail}; {h_detail}"
        elif name == "sentinel-ui":
            h_ok, h_detail = _http("http://sentinel-ui:8080/")
            # UI may not expose /healthz; treat 2xx or any HTTP response from nginx as up
            if h_ok:
                ok = ok and True
                detail = f"{detail}; {h_detail}"
            else:
                # Deployment Ready alone is enough if HTTP blocked
                detail = f"{detail}; http={h_detail}"
        elif name == "openjev":
            h_ok, h_detail = _http("http://openjev:8080/v1/models")
            if h_ok:
                ok = True
                detail = f"{detail}; {h_detail}"
            elif not ok:
                detail = f"{detail}; http={h_detail}"
            else:
                detail = f"{detail}; http={h_detail}"
        out[name] = {"ok": ok, "detail": detail}

    # --- KServe InferenceService (TFM) ---
    isvc = _get(f"/apis/serving.kserve.io/v1beta1/namespaces/{ns}/inferenceservices/tfm-embed")
    if isvc is not None:
        isvc_ok, isvc_detail = _ready_condition(isvc.get("status"))
        # Predictor Deployment name varies; also try common KServe RawDeployment name
        pred_ok, pred_detail = _deploy("tfm-embed-predictor")
        if not pred_ok:
            # Some clusters name it tfm-embed-predictor-xxxxx; check ISVC ready only
            pred_detail = "predictor deploy not found (CR only)"
        http_ok, http_detail = _http("http://tfm-embed-predictor:8080/healthz")
        combined = isvc_ok and (pred_ok or http_ok)
        detail = f"InferenceService {isvc_detail}; {pred_detail}; healthz {http_detail}"
        out["tfm-embed"] = {"ok": combined, "detail": detail}
        out["tfm-embed-predictor"] = out["tfm-embed"]
    # Absent ISVC is fine when TFM_MODE!=http; pipeline_status merges via _probe_tfm_embed

    return out


def _probe_openjev(
    deploy_health: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    """OpenJev disposition service health (post-alert decision layer)."""
    from casino_common.jev_decision import jev_mode, openjev_base_url, openjev_model

    mode = jev_mode()
    base = openjev_base_url()
    model = openjev_model()
    meta: dict[str, Any] = {"mode": mode, "model": model, "url": base, "optional": False}

    if mode in ("off", "disabled", "false", "0"):
        meta["optional"] = True
        return True, "JEV_MODE=off (disposition disabled)", meta

    if mode == "mock":
        meta["optional"] = True
        return True, "JEV_MODE=mock (local heuristics; OpenJev standby)", meta

    # live / openjev — prefer in-cluster deploy + /v1/models
    dh = (deploy_health or {}).get("openjev")
    if dh is not None:
        ok = bool(dh.get("ok"))
        detail = str(dh.get("detail") or "openjev")
        if not ok and "codiv.ai" in base:
            # Hosted Codiv fallback — deploy may be absent
            meta["optional"] = False
            return True, f"Codiv hosted OpenJev ({base}); in-cluster deploy down: {detail}", meta
        return ok, detail, meta

    # No deploy probe (local) — treat configured live URL as optional healthy
    if "codiv.ai" in base or base.startswith("http"):
        meta["optional"] = "codiv.ai" in base
        return True, f"OpenJev URL {base} model={model}", meta
    meta["optional"] = True
    return True, f"OpenJev not deployed ({mode})", meta


def _probe_tfm_embed(
    tfm_mode: str,
    tfm_url: str,
    deploy_health: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, str]:
    """Combine InferenceService probe + live /healthz when TFM_MODE=http."""
    if tfm_mode != "http":
        return True, f"TFM_MODE={tfm_mode} (no GPU InferenceService required)"

    parts: list[str] = []
    ok = True
    dh = (deploy_health or {}).get("tfm-embed") or (deploy_health or {}).get("tfm-embed-predictor")
    if dh:
        ok = ok and bool(dh.get("ok"))
        if dh.get("detail"):
            parts.append(str(dh["detail"]))
    else:
        ok = False
        parts.append("InferenceService tfm-embed not found")

    if not tfm_url:
        return False, "; ".join(parts + ["TFM_EMBED_URL empty"])

    from urllib import request as urlrequest
    from urllib.parse import urlparse

    try:
        parsed = urlparse(tfm_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        health = f"{base}/healthz"
        req = urlrequest.Request(health, method="GET")
        with urlrequest.urlopen(req, timeout=3) as resp:
            if 200 <= resp.status < 300:
                parts.append(f"healthy @ {health}")
            else:
                ok = False
                parts.append(f"HTTP {resp.status} @ {health}")
    except Exception as exc:  # noqa: BLE001
        ok = False
        parts.append(f"healthz unreachable: {exc.__class__.__name__}")

    return ok, "; ".join(parts)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:  # noqa: BLE001
        return None


@app.get("/api/v1/metrics/summary")
def metrics_summary():
    REQUESTS.labels(endpoint="metrics_summary").inc()
    if PIPELINE_MODE == "cdc":
        hydrate_alerts_from_db()
        if len(EVENTS) == 0:
            hydrate_live_events_from_db(limit=100)
    by_level: dict[str, int] = {}
    for a in ALERTS:
        by_level[a["risk_level"]] = by_level.get(a["risk_level"], 0) + 1
    uptime = max(1, int(time.time() - START_TIME))
    # Approximate throughput from buffered / hydrated scored events.
    tx_per_sec = round(len(EVENTS) / uptime, 3) if EVENTS else 0.0
    # Prefer true pipeline lag: alert.created_at − event.timestamp when both exist.
    latencies: list[float] = []
    tx_ts: dict[str, datetime] = {}
    for item in list(EVENTS)[:80]:
        ev = item.get("event") or {}
        tid = str(ev.get("transaction_id") or "")
        ts = ev.get("timestamp")
        if not tid or not ts:
            continue
        parsed = _parse_dt(ts)
        if parsed:
            tx_ts[tid] = parsed
    for a in ALERTS[:40]:
        tid = str(a.get("transaction_id") or "")
        created = a.get("created_at")
        if not tid or tid not in tx_ts or not created:
            continue
        created_dt = _parse_dt(created)
        if not created_dt:
            continue
        try:
            delta_ms = (created_dt - tx_ts[tid]).total_seconds() * 1000.0
            if 1 <= delta_ms <= 300_000:
                latencies.append(delta_ms)
        except Exception:  # noqa: BLE001
            continue
    latency_ms = int(sum(latencies) / len(latencies)) if latencies else (1800 if PIPELINE_MODE == "cdc" else 150)
    # Keep a short sparkline; if only sparse samples, pad with the aggregate.
    latency_samples = [int(x) for x in latencies[-24:]]
    if not latency_samples:
        latency_samples = [max(40, latency_ms - 80), latency_ms, latency_ms + 40, max(50, int(latency_ms * 0.85))]
    elif len(latency_samples) < 4:
        latency_samples = latency_samples + [latency_ms] * (4 - len(latency_samples))
    # AI/ML share of end-to-end lag (demo observability; rules+ML dominate ANALYZE).
    ai_latency_ms = max(5, int(latency_ms * 0.35))
    ml_latency_ms = max(5, int(latency_ms * 0.28))

    high_risk_players = len(
        {
            str(a.get("player_id"))
            for a in ALERTS
            if a.get("player_id") and str(a.get("risk_level", "")).upper() in {"HIGH", "CRITICAL"}
        }
    )

    rule_hits: dict[str, int] = {}
    rule_names: dict[str, str] = {}
    for a in ALERTS:
        for hit in a.get("rule_hits") or []:
            rid = str(hit.get("rule_id") or "").strip()
            if not rid:
                continue
            rule_hits[rid] = rule_hits.get(rid, 0) + 1
            rname = str(hit.get("rule_name") or "").strip()
            if rname and rid not in rule_names:
                rule_names[rid] = rname
    # Prefer catalog names when hits omit rule_name
    for entry in RULE_CATALOG:
        rid = str(entry.get("id") or "")
        if rid and rid not in rule_names and entry.get("name"):
            rule_names[rid] = str(entry["name"])
    rule_hit_frequency = [
        {"rule_id": k, "rule_name": rule_names.get(k, k), "count": v}
        for k, v in sorted(rule_hits.items(), key=lambda kv: (-kv[1], kv[0]))
    ]

    # Risk score histogram from alerts + live events.
    buckets = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
    hist_counts = [0] * len(buckets)
    scores: list[float] = []
    for a in ALERTS:
        try:
            scores.append(float(a.get("score") or 0))
        except (TypeError, ValueError):
            continue
    for item in EVENTS:
        try:
            scores.append(float((item.get("risk") or {}).get("final_score") or 0))
        except (TypeError, ValueError):
            continue
    for s in scores:
        for i, (lo, hi) in enumerate(buckets):
            if lo <= s < hi:
                hist_counts[i] += 1
                break
    risk_histogram = [
        {"bucket": f"{lo:.1f}-{min(hi, 1.0):.1f}", "count": hist_counts[i]}
        for i, (lo, hi) in enumerate(buckets)
    ]

    # Alert volume over time — prefer last ~60m; else span of alert created_at.
    now = datetime.now(timezone.utc)
    created_list: list[datetime] = []
    for a in ALERTS:
        created_dt = _parse_dt(a.get("created_at"))
        if created_dt:
            created_list.append(created_dt)
    n_buckets = 12
    alert_volume: list[dict[str, Any]] = []
    if created_list:
        newest = max(created_list)
        oldest = min(created_list)
        span = max(1.0, (newest - oldest).total_seconds())
        use_wall = (now - oldest).total_seconds() <= 3600 and span <= 3600
        if use_wall:
            window_sec = 5 * 60
            volume = [0] * n_buckets
            for created_dt in created_list:
                age = (now - created_dt).total_seconds()
                if age < 0:
                    continue
                if age >= window_sec * n_buckets:
                    volume[0] += 1
                    continue
                idx = n_buckets - 1 - int(age // window_sec)
                volume[max(0, min(n_buckets - 1, idx))] += 1
            for i, count in enumerate(volume):
                label_dt = now - timedelta(seconds=(n_buckets - 1 - i) * window_sec)
                alert_volume.append({"t": label_dt.strftime("%H:%M"), "count": count})
        else:
            volume = [0] * n_buckets
            for created_dt in created_list:
                frac = (created_dt - oldest).total_seconds() / span
                idx = min(n_buckets - 1, int(frac * n_buckets))
                volume[idx] += 1
            for i, count in enumerate(volume):
                label_dt = oldest + timedelta(seconds=(i / max(1, n_buckets - 1)) * span)
                alert_volume.append({"t": label_dt.strftime("%H:%M"), "count": count})
    else:
        alert_volume = [{"t": f"{i:02d}", "count": 0} for i in range(n_buckets)]

    # Stage activity: relative throughput signals for the pipeline viz.
    events_n = len(EVENTS)
    alerts_n = len(ALERTS)
    stage_activity = {
        "DATABASE": events_n + alerts_n,
        "CDC": events_n + alerts_n if PIPELINE_MODE == "cdc" else 0,
        "STREAM": events_n + alerts_n if PIPELINE_MODE == "cdc" else events_n,
        "ENRICH": events_n,
        "ANALYZE": events_n,
        "RISK": events_n,
        "ALERT": alerts_n,
    }
    pipeline_stages = []
    for name, status in PIPELINE_STATUS.items():
        pipeline_stages.append(
            {
                "name": name,
                "ok": bool(status.get("ok")),
                "detail": status.get("detail"),
                "activity": stage_activity.get(name, 0),
            }
        )

    db = PIPELINE_STATUS.get("DATABASE") or {}
    cdc = PIPELINE_STATUS.get("CDC") or {}
    stream = PIPELINE_STATUS.get("STREAM") or {}
    enrich = PIPELINE_STATUS.get("ENRICH") or {}
    analyze = PIPELINE_STATUS.get("ANALYZE") or {}
    risk = PIPELINE_STATUS.get("RISK") or {}
    alert = PIPELINE_STATUS.get("ALERT") or {}
    platform = {
        "postgres": {"ok": bool(db.get("ok")), "detail": db.get("detail") or "postgres"},
        "debezium": {"ok": bool(cdc.get("ok")), "detail": cdc.get("detail") or "debezium"},
        "kafka_connect": {
            "ok": bool(cdc.get("ok")) and PIPELINE_MODE == "cdc",
            "detail": "casino-connect" if PIPELINE_MODE == "cdc" else "n/a",
        },
        "kafka": {"ok": bool(stream.get("ok")), "detail": stream.get("detail") or "amq-streams"},
        "feature_engine": {"ok": bool(enrich.get("ok")), "detail": enrich.get("detail")},
        "aml_rules_ml": {"ok": bool(analyze.get("ok")), "detail": analyze.get("detail")},
        "risk_engine": {"ok": bool(risk.get("ok")), "detail": risk.get("detail")},
        "alert_service": {"ok": bool(alert.get("ok")), "detail": alert.get("detail")},
        "api": {"ok": True, "detail": "casino-api"},
        "model_loaded": aml_model.model is not None,
    }

    severity_breakdown = [
        {"level": lvl, "count": by_level.get(lvl, 0)}
        for lvl in ("CRITICAL", "HIGH", "MEDIUM", "LOW")
        if by_level.get(lvl, 0) > 0 or lvl in ("HIGH", "CRITICAL", "MEDIUM")
    ]

    return {
        "players": len(PLAYERS),
        "high_risk_players": high_risk_players,
        "alerts_total": len(ALERTS),
        "alerts_open": sum(1 for a in ALERTS if a["status"] == "OPEN"),
        "alerts_critical": by_level.get("CRITICAL", 0),
        "alerts_by_level": by_level,
        "severity_breakdown": severity_breakdown,
        "events_processed": len(EVENTS),
        "tx_per_sec": tx_per_sec,
        "latency_ms": latency_ms,
        "ai_latency_ms": ai_latency_ms,
        "ml_latency_ms": ml_latency_ms,
        "latency_samples": latency_samples,
        "alert_volume": alert_volume,
        "risk_histogram": risk_histogram,
        "rule_hit_frequency": rule_hit_frequency,
        "platform": platform,
        "uptime_seconds": uptime,
        "cdc_ok": bool(cdc.get("ok")),
        "cdc_detail": cdc.get("detail"),
        "kafka_ok": bool(stream.get("ok")),
        "kafka_detail": stream.get("detail"),
        "ai_mode": os.getenv("AI_MODE", "mock"),
        "pipeline_mode": PIPELINE_MODE,
        "pipeline_stages": pipeline_stages,
    }


@app.get("/api/v1/alerts")
def list_alerts(status: Optional[str] = None, limit: int = Query(50, ge=1, le=500)):
    REQUESTS.labels(endpoint="alerts").inc()
    if PIPELINE_MODE == "cdc":
        hydrate_alerts_from_db()
    items = ALERTS
    if status:
        items = [a for a in items if a["status"] == status]
    return {"items": items[:limit], "total": len(items)}


@app.get("/api/v1/alerts/{alert_id}")
def get_alert(alert_id: str):
    for a in ALERTS:
        if a["alert_id"] == alert_id:
            return a
    raise HTTPException(404, "alert not found")


@app.patch("/api/v1/alerts/{alert_id}")
def patch_alert(alert_id: str, body: AlertPatch):
    for a in ALERTS:
        if a["alert_id"] == alert_id:
            a["status"] = body.status.value
            a["updated_at"] = datetime.now(timezone.utc).isoformat()
            if body.assigned_to:
                a["assigned_to"] = body.assigned_to
            if body.note:
                a.setdefault("notes", []).append(body.note)
            ALERTS_GAUGE.set(sum(1 for x in ALERTS if x["status"] == "OPEN"))
            return a
    raise HTTPException(404, "alert not found")


@app.post("/api/v1/alerts/{alert_id}/jev-decide")
def jev_decide_alert(
    alert_id: str,
    apply: bool = Query(False, description="Apply suggested status"),
    wait: bool = Query(False, description="Block until OpenJev finishes (may be slow)"),
):
    """Run (or refresh) the OpenJev disposition layer for an alert.

    Default is async (returns quickly with ``pending``) so OpenShift edge
    routes do not 504 while Verdict CPU runs. Pass ``wait=true`` to block.
    """
    REQUESTS.labels(endpoint="jev_decide").inc()
    from casino_common.jev_decision import _needs_live_refresh, attach_decision, jev_mode

    def _finish(a: dict[str, Any]) -> dict[str, Any]:
        decision = a.get("jev_decision") or {}
        if apply and not a.get("jev_pending") and decision.get("suggested_status"):
            if str(decision.get("mode") or "").lower() not in ("pending",):
                a["status"] = decision["suggested_status"]
                a["updated_at"] = datetime.now(timezone.utc).isoformat()
                note = f"Jev disposition={decision.get('disposition')} ({decision.get('mode')})"
                a.setdefault("notes", []).append(note)
                ALERTS_GAUGE.set(sum(1 for x in ALERTS if x["status"] == "OPEN"))
        pending = bool(a.get("jev_pending")) or str(decision.get("mode") or "").lower() == "pending"
        return {
            "alert_id": alert_id,
            "jev_mode": jev_mode(),
            "applied": bool(apply) and not pending,
            "status": a.get("status"),
            "pending": pending,
            "jev_decision": decision,
        }

    target: dict[str, Any] | None = None
    for a in ALERTS:
        if a["alert_id"] == alert_id:
            target = a
            break
    if target is None and PIPELINE_MODE == "cdc":
        hydrate_alerts_from_db()
        for a in ALERTS:
            if a["alert_id"] == alert_id:
                target = a
                break
    if target is None:
        raise HTTPException(404, "alert not found")

    if wait:
        attach_decision(target, force=True)
        target["jev_pending"] = False
    else:
        # Force refresh when button clicked, even if a mock/fallback is present.
        _schedule_openjev(target, force=True)
        # Brief wait so many clicks get a finished answer within one RTT when CPU is warm.
        deadline = time.time() + float(os.getenv("JEV_SYNC_WAIT_SEC", "12"))
        while time.time() < deadline and target.get("jev_pending"):
            time.sleep(0.25)
        # If still pending but we already have an older decision, keep returning pending.
        if target.get("jev_pending") and _needs_live_refresh(target.get("jev_decision")):
            pass
    return _finish(target)


@app.get("/api/v1/jev/status")
def jev_status():
    """OpenJev decision-layer config (no secrets)."""
    from casino_common.jev_decision import jev_mode, openjev_base_url, openjev_model

    key_set = bool(
        (
            os.getenv("TYPESAFE_API_KEY")
            or os.getenv("JEV_API_KEY")
            or os.getenv("CODIV_API_KEY")
            or ""
        ).strip()
    )
    base = openjev_base_url()
    return {
        "provider": "openjev",
        "jev_mode": jev_mode(),
        "model": openjev_model(),
        "base_url": base,
        "api_key_configured": key_set or ("openjev" in base or ".svc" in base),
        "role": "disposition_only",
        "detection_stack": ["rules", "xgboost", "tfm_behavioral"],
        "docs": "https://github.com/razorback16/openjev",
    }


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    try:
        from decimal import Decimal

        if isinstance(value, Decimal):
            return float(value)
    except Exception:  # noqa: BLE001
        pass
    if hasattr(value, "hex") and not isinstance(value, (str, int, float, bool)):
        # uuid.UUID
        return str(value)
    return value


def _serialize_row(row: Any) -> dict[str, Any]:
    return {k: _serialize_value(v) for k, v in dict(row).items()}


def _resolve_player_id(conn, player_id: str) -> str | None:
    """Resolve exact id, email, demo_label, or unique prefix to canonical VARCHAR player_id."""
    from sqlalchemy import text

    raw = (player_id or "").strip()
    if not raw:
        return None
    row = conn.execute(
        text("SELECT player_id FROM players WHERE player_id = :pid LIMIT 1"),
        {"pid": raw},
    ).mappings().first()
    if row:
        return str(row["player_id"])
    row = conn.execute(
        text(
            """
            SELECT player_id FROM players
            WHERE lower(email) = lower(:ref) OR demo_label = :ref
            LIMIT 1
            """
        ),
        {"ref": raw},
    ).mappings().first()
    if row:
        return str(row["player_id"])
    # Unique prefix (e.g. C102 from Live Monitor truncation)
    if len(raw) >= 3:
        rows = conn.execute(
            text(
                """
                SELECT player_id FROM players
                WHERE player_id ILIKE :pfx
                LIMIT 2
                """
            ),
            {"pfx": f"{raw}%"},
        ).mappings().all()
        if len(rows) == 1:
            return str(rows[0]["player_id"])
    return None

def fetch_player_360_from_db(player_id: str) -> dict[str, Any] | None:
    """Full Player 360 dossier from A's Postgres schema."""
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            resolved = _resolve_player_id(conn, player_id)
            if not resolved:
                return None
            player = conn.execute(
                text(
                    """
                    SELECT player_id, first_name, last_name, email, phone, country,
                           date_of_birth, status, risk_tier, pep_flag, demo_label, registered_at
                    FROM players WHERE player_id = :pid
                    """
                ),
                {"pid": resolved},
            ).mappings().first()
            if not player:
                return None
            profile = _serialize_row(player)
            profile["nationality"] = profile.get("country")
            profile["external_ref"] = profile.get("demo_label") or profile.get("player_id")
            profile["created_at"] = profile.get("registered_at")

            kyc = conn.execute(
                text(
                    """
                    SELECT kyc_id, kyc_status AS verification_status, kyc_status AS kyc_tier,
                           id_document_type AS document_type, nationality AS document_country,
                           risk_rating, watchlist_hit AS is_sanctioned, last_review_at AS verified_at,
                           last_review_at AS updated_at, occupation, source_of_funds, prior_alerts
                    FROM kyc_profiles WHERE player_id = :pid
                    """
                ),
                {"pid": resolved},
            ).mappings().first()
            kyc_out = _serialize_row(kyc) if kyc else None
            if kyc_out is not None:
                kyc_out["is_pep"] = bool(profile.get("pep_flag"))

            wallet = conn.execute(
                text(
                    """
                    SELECT account_id AS wallet_id, 'USD' AS currency, balance_usd AS balance,
                           balance_usd AS available_balance, opened_at AS updated_at
                    FROM player_accounts WHERE player_id = :pid
                    ORDER BY opened_at DESC LIMIT 1
                    """
                ),
                {"pid": resolved},
            ).mappings().first()

            risk = conn.execute(
                text(
                    """
                    SELECT risk_score AS current_score, risk_tier AS risk_level,
                           updated_at AS last_alert_at, NULL AS features_snapshot, updated_at
                    FROM aml.player_scores WHERE player_id = :pid
                    """
                ),
                {"pid": resolved},
            ).mappings().first()

            transactions = [
                _serialize_row(r)
                for r in conn.execute(
                    text(
                        """
                        SELECT transaction_id, txn_type AS transaction_type, amount, currency,
                               location, payment_method AS channel, created_at
                        FROM transactions
                        WHERE player_id = :pid
                        ORDER BY created_at DESC LIMIT 50
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            cash_in = {"CASH_IN", "BUY_IN", "WIRE_IN", "DEPOSIT", "TITO_ISSUE"}
            cash_out = {"CASH_OUT", "WIRE_OUT", "WITHDRAWAL", "TITO_REDEEM"}
            deposits = [t for t in transactions if str(t.get("transaction_type") or "").upper() in cash_in]
            withdrawals = [t for t in transactions if str(t.get("transaction_type") or "").upper() in cash_out]

            wagers = [
                _serialize_row(r)
                for r in conn.execute(
                    text(
                        """
                        SELECT wager_id, game_type, amount AS stake, win_amount AS payout,
                               'USD' AS currency, location, created_at
                        FROM wagers
                        WHERE player_id = :pid
                        ORDER BY created_at DESC LIMIT 50
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            chips = [
                _serialize_row(r)
                for r in conn.execute(
                    text(
                        """
                        SELECT chip_txn_id AS chip_tx_id, direction AS tx_type, amount,
                               location, created_at
                        FROM chip_transactions
                        WHERE player_id = :pid
                        ORDER BY created_at DESC LIMIT 50
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            cage = [
                _serialize_row(r)
                for r in conn.execute(
                    text(
                        """
                        SELECT cage_txn_id AS cage_tx_id, direction AS tx_type, amount,
                               'USD' AS currency, cage_window, teller_id, created_at
                        FROM cage_transactions
                        WHERE player_id = :pid
                        ORDER BY created_at DESC LIMIT 50
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            devices = [
                _serialize_row(r)
                for r in conn.execute(
                    text(
                        """
                        SELECT device_id, fingerprint AS device_fingerprint, device_type,
                               last_seen_at, last_seen_at AS first_seen_at, TRUE AS is_trusted
                        FROM devices
                        WHERE player_id = :pid
                        ORDER BY last_seen_at DESC LIMIT 20
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            payment_methods = [
                _serialize_row(r)
                for r in conn.execute(
                    text(
                        """
                        SELECT payment_id AS payment_method_id, method_type,
                               bank_name AS provider, last4, country, TRUE AS is_verified, created_at
                        FROM payment_methods
                        WHERE player_id = :pid
                        ORDER BY created_at DESC LIMIT 20
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            locations = [
                r["location"]
                for r in conn.execute(
                    text(
                        """
                        SELECT location, MAX(created_at) AS last_seen
                        FROM (
                            SELECT location, created_at FROM transactions
                            WHERE player_id = :pid AND location IS NOT NULL
                            UNION ALL
                            SELECT location, created_at FROM chip_transactions
                            WHERE player_id = :pid AND location IS NOT NULL
                            UNION ALL
                            SELECT cage_window AS location, created_at FROM cage_transactions
                            WHERE player_id = :pid AND cage_window IS NOT NULL
                        ) locs
                        GROUP BY location
                        ORDER BY last_seen DESC
                        LIMIT 20
                        """
                    ),
                    {"pid": resolved},
                ).mappings().all()
            ]
            db_alerts = []
            for r in conn.execute(
                text(
                    """
                    SELECT alert_id, player_id, player_name, alert_type, priority, risk_score,
                           matched_rules, explanation, factors, triggering_ids, status, created_at
                    FROM aml.alerts
                    WHERE player_id = :pid
                    ORDER BY created_at DESC LIMIT 50
                    """
                ),
                {"pid": resolved},
            ).mappings().all():
                db_alerts.append(_row_to_alert(r))

            profile["wallet"] = _serialize_row(wallet) if wallet else None
            profile["risk_profile"] = _serialize_row(risk) if risk else None
            if risk:
                profile["last_risk"] = risk.get("risk_level") or profile.get("last_risk")
                try:
                    # A stores 0..100; UI often expects 0..1 for B-shaped scores
                    raw_score = risk.get("current_score")
                    if raw_score is not None:
                        fs = float(raw_score)
                        profile["last_score"] = fs / 100.0 if fs > 1 else fs
                except (TypeError, ValueError):
                    pass
            profile["events"] = len(transactions) + len(wagers) + len(chips)
            profile["alerts"] = len(db_alerts)
            profile["resolved_from"] = player_id if player_id != resolved else None

            return {
                "player_id": resolved,
                "profile": profile,
                "kyc": kyc_out,
                "transactions": transactions,
                "deposits": deposits,
                "withdrawals": withdrawals,
                "wagers": wagers,
                "chips": chips,
                "cage": cage,
                "devices": devices,
                "payment_methods": payment_methods,
                "locations": locations,
                "alerts": db_alerts,
            }
    except Exception as exc:  # noqa: BLE001
        log.warning("fetch_player_360_from_db failed for %s: %s", player_id, exc)
        return None

@app.get("/api/v1/players")
def list_players(limit: int = Query(50, ge=1, le=500)):
    """List players: prefer in-memory risk activity; pad with DB roster in CDC mode."""
    items = list(PLAYERS.values())
    if PIPELINE_MODE == "cdc" and len(items) < limit:
        try:
            from sqlalchemy import create_engine, text

            engine = create_engine(database_url(), pool_pre_ping=True)
            with engine.connect() as conn:
                known = {str(p.get("player_id")) for p in items}
                rows = conn.execute(
                    text(
                        """
                        SELECT p.player_id, p.first_name, p.last_name, p.status,
                               r.risk_tier AS last_risk, r.risk_score AS last_score,
                               p.demo_label AS external_ref
                        FROM players p
                        LEFT JOIN aml.player_scores r ON r.player_id = p.player_id
                        ORDER BY p.registered_at DESC
                        LIMIT :lim
                        """
                    ),
                    {"lim": limit},
                ).mappings().all()
                for row in rows:
                    pid = str(row["player_id"])
                    if pid in known:
                        # Enrich existing memory entry with identity fields.
                        mem = PLAYERS.get(pid) or {}
                        mem.setdefault("first_name", row.get("first_name"))
                        mem.setdefault("last_name", row.get("last_name"))
                        mem.setdefault("external_ref", row.get("external_ref"))
                        continue
                    items.append(
                        {
                            "player_id": pid,
                            "external_ref": row.get("external_ref"),
                            "first_name": row.get("first_name"),
                            "last_name": row.get("last_name"),
                            "status": row.get("status"),
                            "events": 0,
                            "alerts": 0,
                            "last_risk": row.get("last_risk"),
                            "last_score": (
                                (float(row["last_score"]) / 100.0)
                                if row.get("last_score") is not None and float(row["last_score"]) > 1
                                else (float(row["last_score"]) if row.get("last_score") is not None else None)
                            ),
                        }
                    )
                    if len(items) >= limit:
                        break
        except Exception as exc:  # noqa: BLE001
            log.debug("list_players db pad failed: %s", exc)
    return {"items": items[:limit], "total": len(items)}


@app.get("/api/v1/players/{player_id}")
def player_360(player_id: str):
    """Player 360 — identity + KYC + activity from Postgres, scored trail from memory."""
    REQUESTS.labels(endpoint="players").inc()
    # Refresh alerts so in-memory trail stays current in CDC mode.
    if PIPELINE_MODE == "cdc":
        hydrate_alerts_from_db()
        if len(EVENTS) == 0:
            hydrate_live_events_from_db(limit=100)

    db_dossier = fetch_player_360_from_db(player_id)
    resolved = (db_dossier or {}).get("player_id") or player_id

    mem = PLAYERS.get(resolved) or PLAYERS.get(player_id) or {
        "player_id": resolved,
        "events": 0,
        "alerts": 0,
    }
    player_events = [
        e for e in EVENTS if str((e.get("event") or {}).get("player_id") or "") == resolved
    ][:20]
    mem_alerts = [a for a in ALERTS if str(a.get("player_id") or "") == resolved][:20]

    if db_dossier:
        profile = {**mem, **db_dossier["profile"]}
        # Prefer DB alert/event counts when available; keep last_risk from memory if fresher.
        if mem.get("last_risk") and not profile.get("last_risk"):
            profile["last_risk"] = mem.get("last_risk")
        if mem.get("last_score") is not None and profile.get("last_score") is None:
            profile["last_score"] = mem.get("last_score")
        alerts = db_dossier.get("alerts") or mem_alerts
        # If memory has scored events, prefer those; else synthesize from alert+tx join already in EVENTS.
        recent = player_events or [
            _live_item_from_alert(a, _lookup_transaction(a.get("transaction_id"))) for a in alerts[:20]
        ]
        return {
            "profile": profile,
            "kyc": db_dossier.get("kyc"),
            "transactions": db_dossier.get("transactions") or [],
            "deposits": db_dossier.get("deposits") or [],
            "withdrawals": db_dossier.get("withdrawals") or [],
            "wagers": db_dossier.get("wagers") or [],
            "chips": db_dossier.get("chips") or [],
            "cage": db_dossier.get("cage") or [],
            "devices": db_dossier.get("devices") or [],
            "payment_methods": db_dossier.get("payment_methods") or [],
            "locations": db_dossier.get("locations") or [],
            "recent_events": recent,
            "alerts": alerts,
            "pipeline": "DATABASE→CDC→STREAM→ENRICH→ANALYZE→RISK→ALERT",
            "pipeline_mode": PIPELINE_MODE,
            "source": "postgres",
        }

    # Fallback: in-memory only (local mode / unknown id)
    return {
        "profile": mem if mem.get("player_id") == resolved else {"player_id": player_id, "events": 0, "alerts": 0},
        "kyc": None,
        "transactions": [],
        "deposits": [],
        "withdrawals": [],
        "wagers": [],
        "chips": [],
        "cage": [],
        "devices": [],
        "payment_methods": [],
        "locations": [],
        "recent_events": player_events,
        "alerts": mem_alerts,
        "pipeline": "DATABASE→CDC→STREAM→ENRICH→ANALYZE→RISK→ALERT",
        "pipeline_mode": PIPELINE_MODE,
        "source": "memory",
        "detail": "player not found in database; showing in-memory trail only",
    }


@app.get("/api/v1/events/live")
def live_events(limit: int = Query(50, ge=1, le=500)):
    REQUESTS.labels(endpoint="events_live").inc()
    if PIPELINE_MODE == "cdc" and len(EVENTS) == 0:
        hydrate_live_events_from_db(limit=max(limit, 100))
    return {"items": list(EVENTS)[:limit]}


@app.post("/api/v1/demo/inject")
def demo_inject(body: InjectRequest):
    REQUESTS.labels(endpoint="inject").inc()
    if PIPELINE_MODE == "cdc":
        out = run_cdc_demo(body.events)
        return {"processed": len(out.get("written") or []), "pipeline_mode": "cdc", **out}
    results = [process_event(e) for e in body.events]
    return {"processed": len(results), "pipeline_mode": "local", "results": results}


@app.post("/api/v1/demo/scenarios/{name}")
def run_demo_scenario(
    name: str,
    seed: int | None = Query(
        None,
        description="Scenario seed; default is epoch seconds so each UI click inserts fresh CDC rows",
    ),
    wait_seconds: int | None = Query(
        None,
        ge=0,
        le=120,
        description="CDC poll budget; default CDC_WAIT_SECONDS (keep ≤45 on public Routes). Use 0 for fire-and-poll from UI.",
    ),
):
    REQUESTS.labels(endpoint="scenario").inc()
    if name not in EXPECTED:
        raise HTTPException(400, f"unknown scenario; choose from {list(EXPECTED)}")
    # Fresh seed by default — fixed 42 is idempotent after the first successful inject.
    resolved_seed = int(seed) if seed is not None else int(time.time())
    events = build_events(name, resolved_seed)
    if PIPELINE_MODE == "cdc":
        out = run_cdc_demo(events, wait_seconds=wait_seconds)
        return {
            "scenario": name,
            "seed": resolved_seed,
            "player_ids": list({str(e.get("player_id")) for e in events if e.get("player_id")}),
            "expected": EXPECTED[name],
            "events_injected": len(events),
            "pipeline_mode": "cdc",
            "sample_risk": (
                {
                    "risk_level": (out["alerts"][0].get("risk_level") if out.get("alerts") else None),
                    "final_score": (out["alerts"][0].get("score") if out.get("alerts") else None),
                }
                if out.get("alerts")
                else None
            ),
            **out,
        }
    results = [process_event(e) for e in events]
    alerts = [r.get("alert") for r in results if r.get("alert")]
    return {
        "scenario": name,
        "seed": resolved_seed,
        "player_ids": list({str(e.get("player_id")) for e in events if e.get("player_id")}),
        "expected": EXPECTED[name],
        "events_injected": len(events),
        "alerts_generated": len(alerts),
        "alerts": alerts,
        "sample_risk": results[-1]["risk"] if results else None,
        "pipeline_mode": "local",
    }


@app.get("/api/v1/demo/scenarios")
def list_scenarios():
    return EXPECTED


@app.get("/api/v1/rules/catalog")
def rules_catalog():
    """Static catalog of AML-001..015 (no mock padding)."""
    return {"items": RULE_CATALOG}


def _find_alert(investigation_id: str) -> dict[str, Any] | None:
    iid = str(investigation_id or "").strip()
    if not iid:
        return None
    for a in ALERTS:
        if str(a.get("alert_id") or "") == iid:
            return a
    if PIPELINE_MODE == "cdc":
        hydrate_alerts_from_db()
        for a in ALERTS:
            if str(a.get("alert_id") or "") == iid:
                return a
    return None


def _shared_entities_from_db(player_id: str) -> dict[str, Any]:
    """Find players linked via shared devices / payment instruments (A schema)."""
    out: dict[str, Any] = {
        "shared_devices": [],
        "shared_payments": [],
        "linked_players": [],
    }
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            resolved = _resolve_player_id(conn, player_id) or player_id
            device_rows = conn.execute(
                text(
                    """
                    SELECT d.fingerprint AS device_fingerprint, d.device_type, d.device_id,
                           p.player_id, p.first_name, p.last_name, p.demo_label AS external_ref
                    FROM devices d
                    JOIN players p ON p.player_id = d.player_id
                    WHERE d.fingerprint IS NOT NULL
                      AND d.fingerprint IN (
                        SELECT fingerprint FROM devices
                        WHERE player_id = :pid AND fingerprint IS NOT NULL
                      )
                      AND d.player_id <> :pid
                    LIMIT 40
                    """
                ),
                {"pid": resolved},
            ).mappings().all()
            for r in device_rows:
                out["shared_devices"].append(_serialize_row(r))
                out["linked_players"].append(str(r["player_id"]))

            pay_rows = conn.execute(
                text(
                    """
                    SELECT pm.payment_id AS payment_method_id, pm.method_type,
                           pm.bank_name AS provider, pm.last4, pm.country,
                           p.player_id, p.first_name, p.last_name, p.demo_label AS external_ref
                    FROM payment_methods pm
                    JOIN players p ON p.player_id = pm.player_id
                    WHERE pm.player_id <> :pid
                      AND pm.last4 IS NOT NULL
                      AND EXISTS (
                        SELECT 1 FROM payment_methods mine
                        WHERE mine.player_id = :pid
                          AND mine.last4 = pm.last4
                          AND COALESCE(mine.bank_name, '') = COALESCE(pm.bank_name, '')
                      )
                    LIMIT 40
                    """
                ),
                {"pid": resolved},
            ).mappings().all()
            for r in pay_rows:
                out["shared_payments"].append(_serialize_row(r))
                out["linked_players"].append(str(r["player_id"]))
    except Exception as exc:  # noqa: BLE001
        log.debug("shared entity lookup failed: %s", exc)
    out["linked_players"] = sorted(set(out["linked_players"]))
    return out

def _build_investigation_network(alert: dict[str, Any]) -> dict[str, Any]:
    """Entity graph for one investigation (alert) — players, devices, payments, rules, hidden links."""
    alert_id = str(alert.get("alert_id") or "")
    player_id = str(alert.get("player_id") or "")
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    hidden: list[dict[str, Any]] = []

    def add_node(nid: str, kind: str, label: str, **props: Any) -> None:
        if nid in nodes:
            nodes[nid]["props"].update({k: v for k, v in props.items() if v is not None})
            return
        nodes[nid] = {
            "id": nid,
            "kind": kind,
            "label": label,
            "props": {k: v for k, v in props.items() if v is not None},
        }

    def add_edge(src: str, dst: str, rel: str, *, hidden_gap: bool = False, **props: Any) -> None:
        edges.append(
            {
                "id": f"{src}->{dst}:{rel}:{len(edges)}",
                "source": src,
                "target": dst,
                "type": rel,
                "hidden": hidden_gap,
                "props": props,
            }
        )
        if hidden_gap:
            hidden.append(
                {
                    "relation": rel,
                    "from": src,
                    "to": dst,
                    "detail": props.get("detail") or rel,
                    "confidence": props.get("confidence", 0.7),
                }
            )

    # Seed alert + primary player
    add_node(
        f"alert:{alert_id}",
        "alert",
        alert.get("title") or f"Alert {alert_id[:8]}",
        risk_level=alert.get("risk_level"),
        score=alert.get("score"),
        status=alert.get("status"),
        summary=alert.get("summary"),
        created_at=str(alert.get("created_at") or ""),
    )
    dossier = fetch_player_360_from_db(player_id) if player_id else None
    profile = (dossier or {}).get("profile") or PLAYERS.get(player_id) or {"player_id": player_id}
    display = " ".join(
        x for x in [profile.get("first_name"), profile.get("last_name")] if x
    ).strip() or (profile.get("external_ref") or player_id[:8])
    add_node(
        f"player:{player_id}",
        "player",
        display,
        player_id=player_id,
        external_ref=profile.get("external_ref"),
        email=profile.get("email"),
        phone=profile.get("phone"),
        risk_level=profile.get("last_risk") or alert.get("risk_level"),
        score=profile.get("last_score") or alert.get("score"),
        kyc_tier=((dossier or {}).get("kyc") or {}).get("kyc_tier") if dossier else None,
        is_pep=((dossier or {}).get("kyc") or {}).get("is_pep") if dossier else None,
        is_sanctioned=((dossier or {}).get("kyc") or {}).get("is_sanctioned") if dossier else None,
        primary=True,
    )
    add_edge(f"alert:{alert_id}", f"player:{player_id}", "subjects", detail="Primary subject of investigation")

    kyc = (dossier or {}).get("kyc") or {}
    if kyc.get("is_pep") or kyc.get("is_sanctioned") or str(kyc.get("risk_rating") or "").upper() in (
        "HIGH",
        "CRITICAL",
        "RED",
    ):
        wid = f"watchlist:{player_id}"
        add_node(
            wid,
            "watchlist",
            "Watchlist / KYC flag",
            is_pep=kyc.get("is_pep"),
            is_sanctioned=kyc.get("is_sanctioned"),
            risk_rating=kyc.get("risk_rating"),
            kyc_grade="Red" if kyc.get("is_sanctioned") or kyc.get("is_pep") else "Amber",
        )
        add_edge(f"player:{player_id}", wid, "flagged_by", hidden_gap=True, detail="KYC / PEP / sanctions signal", confidence=0.9)

    for h in alert.get("rule_hits") or []:
        rid = str(h.get("rule_id") or h.get("rule_name") or "rule")
        rname = str(h.get("rule_name") or "").strip() or rid
        nid = f"rule:{rid}"
        add_node(nid, "rule", rname, severity=h.get("severity"), score=h.get("score"), evidence=h.get("evidence"))
        add_edge(f"alert:{alert_id}", nid, "triggered", detail="Rule fired on alert")
        add_edge(f"player:{player_id}", nid, "matched_rule", detail="Player matched AML rule")

    for d in (dossier or {}).get("devices") or []:
        fp = str(d.get("device_fingerprint") or d.get("device_id") or "")
        if not fp:
            continue
        nid = f"device:{fp}"
        add_node(
            nid,
            "device",
            f"Device · {(fp[:10] + '…') if len(fp) > 10 else fp}",
            fingerprint=fp,
            device_type=d.get("device_type"),
            os_name=d.get("os_name"),
            is_trusted=d.get("is_trusted"),
            last_seen_at=str(d.get("last_seen_at") or ""),
        )
        add_edge(f"player:{player_id}", nid, "uses_device", detail="Registered device")

    for pm in (dossier or {}).get("payment_methods") or []:
        pmid = str(pm.get("payment_method_id") or f"{pm.get('provider')}-{pm.get('last4')}")
        nid = f"payment:{pmid}"
        add_node(
            nid,
            "payment",
            f"{pm.get('method_type') or 'Pay'} ···{pm.get('last4') or '????'}",
            provider=pm.get("provider"),
            last4=pm.get("last4"),
            country=pm.get("country"),
            method_type=pm.get("method_type"),
            is_verified=pm.get("is_verified"),
        )
        add_edge(f"player:{player_id}", nid, "funds_via", detail="Payment instrument")

    for loc in (dossier or {}).get("locations") or []:
        if not loc:
            continue
        nid = f"location:{loc}"
        add_node(nid, "location", str(loc), location=loc)
        add_edge(f"player:{player_id}", nid, "seen_at", detail="Activity location")

    # Memory co-travel: same correlation_id on other alerts/events
    corr = str(alert.get("correlation_id") or "")
    if corr:
        for a in ALERTS:
            oid = str(a.get("alert_id") or "")
            opid = str(a.get("player_id") or "")
            if not oid or oid == alert_id:
                continue
            if str(a.get("correlation_id") or "") != corr:
                continue
            add_node(
                f"alert:{oid}",
                "alert",
                a.get("title") or f"Alert {oid[:8]}",
                risk_level=a.get("risk_level"),
                score=a.get("score"),
                status=a.get("status"),
            )
            if opid and opid != player_id:
                add_node(f"player:{opid}", "player", opid[:8], player_id=opid, linked_via="correlation")
                add_edge(f"alert:{oid}", f"player:{opid}", "subjects")
                add_edge(
                    f"player:{player_id}",
                    f"player:{opid}",
                    "co_correlated",
                    hidden_gap=True,
                    detail=f"Shared correlation_id {corr[:12]}…",
                    confidence=0.85,
                )
            add_edge(f"alert:{alert_id}", f"alert:{oid}", "same_correlation", hidden_gap=True, detail="Same CDC correlation batch")

    shared = _shared_entities_from_db(player_id) if player_id else {"shared_devices": [], "shared_payments": [], "linked_players": []}
    for r in shared.get("shared_devices") or []:
        fp = str(r.get("device_fingerprint") or "")
        opid = str(r.get("player_id") or "")
        if not fp or not opid:
            continue
        dnode = f"device:{fp}"
        add_node(
            dnode,
            "device",
            f"Device · {(fp[:10] + '…') if len(fp) > 10 else fp}",
            fingerprint=fp,
            device_type=r.get("device_type"),
            shared=True,
        )
        pname = " ".join(x for x in [r.get("first_name"), r.get("last_name")] if x).strip() or opid[:8]
        add_node(f"player:{opid}", "player", pname, player_id=opid, external_ref=r.get("external_ref"), linked_via="shared_device")
        add_edge(f"player:{opid}", dnode, "uses_device")
        add_edge(
            f"player:{player_id}",
            f"player:{opid}",
            "shared_device",
            hidden_gap=True,
            detail=f"Shared device fingerprint {fp[:12]}…",
            confidence=0.92,
        )

    for r in shared.get("shared_payments") or []:
        opid = str(r.get("player_id") or "")
        pmid = str(r.get("payment_method_id") or f"{r.get('provider')}-{r.get('last4')}")
        if not opid:
            continue
        pnode = f"payment:{pmid}"
        add_node(
            pnode,
            "payment",
            f"{r.get('method_type') or 'Pay'} ···{r.get('last4') or '????'}",
            provider=r.get("provider"),
            last4=r.get("last4"),
            shared=True,
        )
        pname = " ".join(x for x in [r.get("first_name"), r.get("last_name")] if x).strip() or opid[:8]
        add_node(f"player:{opid}", "player", pname, player_id=opid, linked_via="shared_payment")
        add_edge(f"player:{opid}", pnode, "funds_via")
        add_edge(
            f"player:{player_id}",
            f"player:{opid}",
            "shared_payment",
            hidden_gap=True,
            detail=f"Same instrument ···{r.get('last4')} ({r.get('provider')})",
            confidence=0.88,
        )

    # Entity records table (bottom grid)
    for n in nodes.values():
        records.append(
            {
                "entity_id": n["id"],
                "kind": n["kind"],
                "label": n["label"],
                **{k: v for k, v in (n.get("props") or {}).items() if not isinstance(v, (dict, list))},
            }
        )

    return {
        "investigation_id": alert_id,
        "player_id": player_id,
        "title": alert.get("title") or f"Investigation {alert_id[:12]}",
        "risk_level": alert.get("risk_level"),
        "score": alert.get("score"),
        "status": alert.get("status") or "OPEN",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": list(nodes.values()),
        "edges": edges,
        "records": records,
        "hidden_relations": hidden,
        "legend": [
            {"kind": "alert", "label": "Alert"},
            {"kind": "player", "label": "Player / Customer"},
            {"kind": "device", "label": "Device"},
            {"kind": "payment", "label": "Payment"},
            {"kind": "location", "label": "Location"},
            {"kind": "rule", "label": "AML Rule"},
            {"kind": "watchlist", "label": "Watchlist / KYC"},
        ],
        "source": "alerts+postgres+memory",
    }


@app.get("/api/v1/network/investigations")
def list_network_investigations(limit: int = 40):
    """Investigations available for entity-network workspace (open HIGH/CRITICAL + recent)."""
    if PIPELINE_MODE == "cdc" and not ALERTS:
        hydrate_alerts_from_db()
    items = []
    for a in ALERTS:
        lvl = str(a.get("risk_level") or "").upper()
        st = str(a.get("status") or "").upper()
        if st in ("CLOSED", "FALSE_POSITIVE"):
            continue
        # Prefer HIGH/CRITICAL; also include MEDIUM with rule hits so desk isn't empty.
        if lvl not in ("HIGH", "CRITICAL", "MEDIUM"):
            continue
        items.append(
            {
                "investigation_id": a.get("alert_id"),
                "alert_id": a.get("alert_id"),
                "player_id": a.get("player_id"),
                "title": a.get("title") or "AML investigation",
                "priority": a.get("risk_level"),
                "status": a.get("status") or "OPEN",
                "score": a.get("score"),
                "created_at": a.get("created_at"),
                "rule_hits": [h.get("rule_id") for h in (a.get("rule_hits") or []) if h.get("rule_id")],
            }
        )
    items.sort(
        key=lambda x: (
            0 if str(x.get("priority") or "").upper() == "CRITICAL" else 1 if str(x.get("priority") or "").upper() == "HIGH" else 2,
            str(x.get("created_at") or ""),
        ),
        reverse=False,
    )
    # Stable: CRITICAL/HIGH first by created_at desc within bands
    crit = [i for i in items if str(i.get("priority") or "").upper() == "CRITICAL"]
    high = [i for i in items if str(i.get("priority") or "").upper() == "HIGH"]
    med = [i for i in items if str(i.get("priority") or "").upper() == "MEDIUM"]
    for band in (crit, high, med):
        band.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    ordered = (crit + high + med)[:limit]
    return {"items": ordered, "total": len(ordered)}


@app.get("/api/v1/network/investigations/{investigation_id}")
def get_investigation_network(investigation_id: str):
    """Entity / relationship graph for one investigation ID (alert)."""
    alert = _find_alert(investigation_id)
    if not alert:
        raise HTTPException(404, f"investigation {investigation_id} not found")
    return _build_investigation_network(alert)


@app.get("/api/v1/cases")
def list_cases(limit: int = 50):
    """Cases derived from open HIGH/CRITICAL alerts — empty when none (honest)."""
    items = []
    for a in ALERTS:
        if str(a.get("risk_level", "")).upper() not in ("HIGH", "CRITICAL"):
            continue
        if str(a.get("status", "")).upper() in ("CLOSED", "FALSE_POSITIVE"):
            continue
        items.append(
            {
                "case_id": a.get("alert_id"),
                "alert_id": a.get("alert_id"),
                "player_id": a.get("player_id"),
                "priority": a.get("risk_level"),
                "status": a.get("status") or "OPEN",
                "score": a.get("score"),
                "title": a.get("title") or a.get("explanation") or "AML case",
                "created_at": a.get("created_at"),
            }
        )
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return {"items": items[:limit], "source": "alerts"}


@app.get("/api/v1/sars")
def list_sars(limit: int = 50):
    """SAR drafts from CRITICAL alerts — empty when none."""
    items = []
    for a in ALERTS:
        if str(a.get("risk_level", "")).upper() != "CRITICAL":
            continue
        items.append(
            {
                "sar_id": f"SAR-{a.get('alert_id')}",
                "alert_id": a.get("alert_id"),
                "player_id": a.get("player_id"),
                "status": "DRAFT",
                "narrative": a.get("explanation") or "Critical AML alert pending SAR review",
                "created_at": a.get("created_at"),
            }
        )
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return {"items": items[:limit], "source": "alerts"}


@app.get("/api/v1/settings/weights")
def score_weights():
    from risk_aggregator import _weights

    w = _weights()
    return {
        "rules": w["rules"],
        "ml": w["ml"],
        "behavioral": w["behavioral"],
        "kyc": w.get("kyc", 0.0),
        "alert_threshold": float(os.getenv("ALERT_THRESHOLD", "0.55")),
        "note": "Normalized weights from ConfigMap / env (sum=1)",
    }


def _k8s_headers() -> tuple[str, dict[str, str], str]:
    """In-cluster API access via the pod service account."""
    import ssl
    from urllib import request as urlrequest

    token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    ns_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    ca_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    if not token_path.is_file():
        raise HTTPException(status_code=503, detail="Not running in-cluster (no service account token)")
    token = token_path.read_text(encoding="utf-8").strip()
    ns = ns_path.read_text(encoding="utf-8").strip() if ns_path.is_file() else os.getenv("NAMESPACE", "casino-sentinel")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    return ns, headers, str(ca_path) if ca_path.is_file() else ""


def _read_configmap_data() -> dict[str, str]:
    from urllib import request as urlrequest
    import ssl

    ns, headers, ca = _k8s_headers()
    url = f"https://kubernetes.default.svc/api/v1/namespaces/{ns}/configmaps/casino-config"
    ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
    req = urlrequest.Request(url, headers=headers, method="GET")
    with urlrequest.urlopen(req, context=ctx, timeout=15) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return dict(body.get("data") or {})


def _patch_configmap_data(updates: dict[str, str]) -> dict[str, str]:
    from urllib import request as urlrequest
    import ssl

    ns, headers, ca = _k8s_headers()
    url = f"https://kubernetes.default.svc/api/v1/namespaces/{ns}/configmaps/casino-config"
    headers = {**headers, "Content-Type": "application/strategic-merge-patch+json"}
    payload = json.dumps({"data": updates}).encode("utf-8")
    ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
    req = urlrequest.Request(url, data=payload, headers=headers, method="PATCH")
    with urlrequest.urlopen(req, context=ctx, timeout=15) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return dict(body.get("data") or {})


def _restart_simulator_deploy() -> None:
    """Bump a pod-template annotation so the simulator reloads env promptly."""
    from urllib import request as urlrequest
    import ssl

    ns, headers, ca = _k8s_headers()
    url = (
        f"https://kubernetes.default.svc/apis/apps/v1/namespaces/{ns}/deployments/sentinel-simulator"
    )
    headers = {**headers, "Content-Type": "application/strategic-merge-patch+json"}
    ts = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(
        {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {"casino.sentinel/restartedAt": ts}
                    }
                }
            }
        }
    ).encode("utf-8")
    ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
    req = urlrequest.Request(url, data=payload, headers=headers, method="PATCH")
    try:
        with urlrequest.urlopen(req, context=ctx, timeout=15) as resp:
            resp.read()
    except Exception as exc:  # noqa: BLE001
        log.warning("simulator restart annotation failed: %s", exc)


class SimulatorSettingsIn(BaseModel):
    interval_sec: Optional[float] = Field(default=None, ge=1, le=3600)
    enabled: Optional[bool] = None


@app.get("/api/v1/settings/simulator")
def get_simulator_settings():
    """Background dummy traffic interval (ConfigMap casino-config)."""
    REQUESTS.labels(endpoint="settings_simulator").inc()
    try:
        data = _read_configmap_data()
        interval = float(data.get("SIMULATOR_INTERVAL_SEC") or os.getenv("SIMULATOR_INTERVAL_SEC", "30"))
        enabled_raw = (data.get("SIMULATOR_ENABLED") or os.getenv("SIMULATOR_ENABLED", "true")).lower()
        enabled = enabled_raw in {"1", "true", "yes", "on"}
        return {
            "interval_sec": interval,
            "enabled": enabled,
            "source": "casino-config",
            "note": "One background transaction every interval_sec when enabled. Demo scenarios are separate.",
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        # Fallback to process env if RBAC/API unavailable
        interval = float(os.getenv("SIMULATOR_INTERVAL_SEC", "30"))
        enabled = os.getenv("SIMULATOR_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
        return {
            "interval_sec": interval,
            "enabled": enabled,
            "source": "env",
            "note": f"ConfigMap read failed ({exc}); showing local env",
        }


@app.patch("/api/v1/settings/simulator")
def patch_simulator_settings(body: SimulatorSettingsIn):
    """Update SIMULATOR_* on casino-config (picked up by sentinel-simulator)."""
    REQUESTS.labels(endpoint="settings_simulator_patch").inc()
    if body.interval_sec is None and body.enabled is None:
        raise HTTPException(status_code=400, detail="Provide interval_sec and/or enabled")
    updates: dict[str, str] = {}
    if body.interval_sec is not None:
        updates["SIMULATOR_INTERVAL_SEC"] = str(int(body.interval_sec) if body.interval_sec == int(body.interval_sec) else body.interval_sec)
    if body.enabled is not None:
        updates["SIMULATOR_ENABLED"] = "true" if body.enabled else "false"
    try:
        data = _patch_configmap_data(updates)
        _restart_simulator_deploy()
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to update ConfigMap: {exc}") from exc
    interval = float(data.get("SIMULATOR_INTERVAL_SEC") or "30")
    enabled = (data.get("SIMULATOR_ENABLED") or "true").lower() in {"1", "true", "yes", "on"}
    return {
        "interval_sec": interval,
        "enabled": enabled,
        "source": "casino-config",
        "updated": list(updates.keys()),
        "note": "Simulator restarted to apply settings",
    }


@app.get("/api/v1/sse")
async def sse():
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    SSE_SUBSCRIBERS.append(queue)

    async def gen():
        try:
            yield f"data: {json.dumps({'type': 'hello', 'ts': datetime.now(timezone.utc).isoformat()})}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(msg, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        finally:
            if queue in SSE_SUBSCRIBERS:
                SSE_SUBSCRIBERS.remove(queue)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.websocket("/api/v1/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    SSE_SUBSCRIBERS.append(queue)
    try:
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=20)
                await ws.send_json(msg)
            except asyncio.TimeoutError:
                await ws.send_json({"type": "heartbeat"})
    except WebSocketDisconnect:
        pass
    finally:
        if queue in SSE_SUBSCRIBERS:
            SSE_SUBSCRIBERS.remove(queue)


@app.on_event("startup")
def on_startup():
    log.info("Casino Sentinel API starting ai_mode=%s pipeline_mode=%s", os.getenv("AI_MODE", "mock"), PIPELINE_MODE)
    if PIPELINE_MODE == "cdc":
        # Live rows first so alert hydrate can skip duplicate event pushes.
        n_live = hydrate_live_events_from_db()
        hydrate_alerts_from_db()
        log.info("CDC hydrate: live_events=%s alerts=%s", n_live, len(ALERTS))
        threading.Thread(target=_alert_consumer_loop, name="alert-consumer", daemon=True).start()
        threading.Thread(target=_risk_consumer_loop, name="risk-consumer", daemon=True).start()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
