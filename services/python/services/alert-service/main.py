#!/usr/bin/env python3
"""Alert service: risk assessments → aml.alerts + Postgres persistence."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from casino_common.config import database_url  # noqa: E402
from casino_common.kafka_utils import JsonConsumer, JsonProducer, run_loop  # noqa: E402
from casino_common.logging_config import set_context, setup_logging  # noqa: E402
from casino_common.alert_labels import alert_display_label  # noqa: E402
from casino_common.models import Alert, AlertStatus, RiskLevel  # noqa: E402

log = setup_logging("alert-service", os.getenv("LOG_LEVEL", "INFO"))
IN_TOPIC = os.getenv("RISK_TOPIC", "aml.risk.assessments")
OUT_TOPIC = os.getenv("ALERTS_TOPIC", "aml.alerts")
DLQ = os.getenv("DLQ_TOPIC", "casino.dlq")
THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "0.55"))
producer = JsonProducer("alert-service")

# Suppress INFO spam while replaying historical LOW assessments during catch-up.
_below_threshold_count = 0
_seen_event_ids: set[str] = set()
_SEEN_EVENT_CAP = 50_000


def _alert_id_for(event_id: str | None) -> str:
    """Deterministic id so catch-up replays upsert idempotently."""
    if event_id:
        return str(uuid5(NAMESPACE_URL, f"casino-sentinel-alert:{event_id}"))
    return str(uuid4())


def persist_alert(alert: Alert) -> bool:
    """Insert alert into A's aml.alerts (+ aml.player_scores). Returns True when inserted."""
    try:
        from sqlalchemy import create_engine, text

        # B scores are 0..1; A's risk_score column is INTEGER 0..100
        risk_score = int(round(float(alert.score) * 100)) if float(alert.score) <= 1.0 else int(round(float(alert.score)))
        risk_score = max(0, min(100, risk_score))
        matched = [getattr(h, "rule_id", None) or (h.get("rule_id") if isinstance(h, dict) else str(h)) for h in (alert.rule_hits or [])]
        matched = [m for m in matched if m]
        factors = []
        for e in alert.explanations or []:
            factors.append({"detail": e} if isinstance(e, str) else e)
        triggering = [x for x in [alert.transaction_id, alert.event_id, alert.correlation_id] if x]
        alert_type = matched[0] if matched else (alert.title or "AML")[:64]

        engine = create_engine(database_url(), pool_pre_ping=True)
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO aml.alerts (
                        alert_id, player_id, player_name, alert_type, priority, risk_score,
                        rule_score, ml_score, behavioral_score, kyc_score, matched_rules,
                        explanation, factors, triggering_ids, status, created_at
                    ) VALUES (
                        :alert_id, :player_id, :player_name, :alert_type, :priority, :risk_score,
                        :rule_score, :ml_score, :behavioral_score, :kyc_score, :matched_rules,
                        :explanation, CAST(:factors AS jsonb), :triggering_ids, :status,
                        CAST(:created_at AS timestamptz)
                    )
                    ON CONFLICT (alert_id) DO NOTHING
                    """
                ),
                {
                    "alert_id": alert.alert_id,
                    "player_id": alert.player_id,
                    "player_name": None,
                    "alert_type": alert_type,
                    "priority": alert.risk_level.value,
                    "risk_score": risk_score,
                    "rule_score": None,
                    "ml_score": None,
                    "behavioral_score": None,
                    "kyc_score": None,
                    "matched_rules": matched,
                    "explanation": alert.summary or alert.title or "",
                    "factors": json.dumps(factors),
                    "triggering_ids": triggering,
                    "status": alert.status.value if hasattr(alert.status, "value") else str(alert.status),
                    "created_at": alert.created_at.isoformat(),
                },
            )
            inserted = (result.rowcount or 0) > 0
            if not inserted:
                return False
            # A has aml.player_scores (not player_risk_profiles)
            try:
                conn.execute(
                    text(
                        """
                        INSERT INTO aml.player_scores (
                            player_id, player_name, country, risk_score, risk_tier,
                            month_volume, explanation, updated_at
                        ) VALUES (
                            :player_id, NULL, NULL, :risk_score, :risk_tier,
                            0, :explanation, CAST(:ts AS timestamptz)
                        )
                        ON CONFLICT (player_id) DO UPDATE SET
                            risk_score = EXCLUDED.risk_score,
                            risk_tier = EXCLUDED.risk_tier,
                            explanation = EXCLUDED.explanation,
                            updated_at = EXCLUDED.updated_at
                        """
                    ),
                    {
                        "player_id": alert.player_id,
                        "risk_score": risk_score,
                        "risk_tier": alert.risk_level.value,
                        "explanation": alert.summary or alert.title or "",
                        "ts": alert.created_at.isoformat(),
                    },
                )
            except Exception as score_exc:  # noqa: BLE001
                log.warning("aml.player_scores upsert skipped: %s", score_exc)
            return True
    except Exception as exc:  # noqa: BLE001
        log.warning("persist_alert failed (continuing): %s", exc)
        return False


def handle(key, value):
    global _below_threshold_count

    score = float(value.get("final_score") or 0)
    level = value.get("risk_level", "LOW")
    event_id = value.get("event_id")

    if score < THRESHOLD and level in ("LOW",):
        _below_threshold_count += 1
        # Debug per-event; summary every 500 to avoid catch-up log floods.
        log.debug("below threshold score=%.2f event_id=%s", score, event_id)
        if _below_threshold_count % 500 == 0:
            log.info("skipped %d below-threshold assessments (catch-up)", _below_threshold_count)
        return

    if event_id and event_id in _seen_event_ids:
        log.debug("skip duplicate event_id=%s", event_id)
        return

    now = datetime.now(timezone.utc)
    rules = value.get("rule_hits") or []
    title = alert_display_label(
        rule_hits=rules,
        risk_level=level,
        summary="; ".join(value.get("explanations") or [])[:500] or None,
    )
    alert = Alert(
        alert_id=_alert_id_for(str(event_id) if event_id else None),
        player_id=value["player_id"],
        transaction_id=value.get("transaction_id"),
        event_id=event_id,
        correlation_id=value.get("correlation_id"),
        created_at=now,
        updated_at=now,
        risk_level=RiskLevel(level),
        status=AlertStatus.OPEN,
        title=title[:255],
        summary="; ".join(value.get("explanations") or [title])[:2000],
        score=score,
        rule_hits=rules,
        explanations=value.get("explanations") or [],
    )
    set_context(
        correlation_id=alert.correlation_id,
        event_id=alert.event_id,
        player_id=alert.player_id,
        transaction_id=alert.transaction_id,
    )
    inserted = persist_alert(alert)
    if event_id:
        if len(_seen_event_ids) >= _SEEN_EVENT_CAP:
            _seen_event_ids.clear()
        _seen_event_ids.add(str(event_id))
    if not inserted:
        log.debug("alert already present alert_id=%s event_id=%s", alert.alert_id, event_id)
        return
    payload = alert.model_dump(mode="json")
    try:
        from casino_common.jev_decision import attach_decision

        attach_decision(payload)
    except Exception as jev_exc:  # noqa: BLE001
        log.warning("Jev decision skipped: %s", jev_exc)
    producer.produce(OUT_TOPIC, payload, key=alert.player_id)
    producer.flush()
    disp = (payload.get("jev_decision") or {}).get("disposition")
    log.info(
        "alert created level=%s score=%.2f jev=%s",
        alert.risk_level.value,
        alert.score,
        disp or "—",
    )


def main():
    consumer = JsonConsumer("alert-service", [IN_TOPIC])
    dlq = JsonProducer("alert-service-dlq")
    log.info("alert-service threshold=%.2f", THRESHOLD)
    run_loop(consumer, handle, dlq, DLQ, "alert-service")


if __name__ == "__main__":
    main()
