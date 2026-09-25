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
    return {
        "alert_id": str(data["alert_id"]) if data.get("alert_id") is not None else None,
        "player_id": str(data["player_id"]) if data.get("player_id") is not None else None,
        "player_name": data.get("player_name"),
        "transaction_id": str(triggering[0]) if triggering else data.get("transaction_id"),
        "event_id": str(triggering[0]) if triggering else data.get("event_id"),
        "correlation_id": str(triggering[1]) if len(triggering) > 1 else data.get("correlation_id"),
        "risk_level": data.get("priority") or data.get("risk_level") or "MEDIUM",
        "status": data.get("status") or "OPEN",
        "title": data.get("alert_type") or data.get("title") or "AML",
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


def _ingest_alert(alert: dict[str, Any]) -> None:
    aid = str(alert.get("alert_id") or "")
    if not aid or aid in _ALERT_IDS:
        return
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
            "title": f"AML {assessment.risk_level.value}",
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
                _ingest_alert(alert)
                if len(_ALERT_IDS) > before:
                    added += 1
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
    return {
        "pipeline_mode": PIPELINE_MODE,
        "stages": [{"name": s, **PIPELINE_STATUS[s]} for s in stages],
        "uptime_seconds": int(time.time() - START_TIME),
        "events_buffered": len(EVENTS),
        "open_alerts": sum(1 for a in ALERTS if a["status"] == "OPEN"),
        "note": (
            "OpenShift CDC path: Postgres → Debezium → Kafka services"
            if PIPELINE_MODE == "cdc"
            else "Local in-process path (API-only); not used on OpenShift"
        ),
    }


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
    for a in ALERTS:
        for hit in a.get("rule_hits") or []:
            rid = str(hit.get("rule_id") or "").strip()
            if rid:
                rule_hits[rid] = rule_hits.get(rid, 0) + 1
    rule_hit_frequency = [
        {"rule_id": k, "count": v}
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
        nid = f"rule:{rid}"
        add_node(nid, "rule", f"{rid} · {h.get('rule_name') or rid}", severity=h.get("severity"), score=h.get("score"), evidence=h.get("evidence"))
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
