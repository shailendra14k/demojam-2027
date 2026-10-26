"""Investigator API + WebSocket live bus for the Casino Guard UI."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from . import db, mock_data
from .config import settings
from .simulator import run_scenario

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sentinel.api")

app = FastAPI(title="Realtime TxnIQ API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_live = False
_ws_clients: set[WebSocket] = set()
_db_ok = False
_kafka_hint = "PENDING"


class ScenarioBody(BaseModel):
    name: str = "mixed"


def _try_db() -> bool:
    global _db_ok
    try:
        db.fetch_one("SELECT 1 AS ok")
        _db_ok = True
    except Exception as exc:  # noqa: BLE001
        log.warning("DB fallback: %s", exc)
        _db_ok = False
    return _db_ok


def _mode() -> str:
    if settings.data_mode == "mock":
        return "mock"
    if settings.data_mode == "live":
        return "live"
    return "live" if _db_ok else "mock"


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


@app.on_event("startup")
async def startup() -> None:
    _try_db()
    asyncio.create_task(_poll_live())


@app.get("/healthz")
def healthz() -> dict:
    _try_db()
    return {"status": "ok", "database": "up" if _db_ok else "down", "mode": _mode()}


@app.get("/api/v1/health")
def health() -> dict:
    return healthz()


@app.get("/api/v1/status")
def status() -> dict:
    _try_db()
    components = mock_data.pipeline()
    if _db_ok:
        try:
            rows = db.fetch_all("SELECT component, status, detail, updated_at FROM aml.pipeline_status")
            by_id = {r["component"]: r for r in rows}
            mapped = {
                "transaction": "transaction",
                "debezium": "debezium",
                "kafka": "kafka",
                "rules": "rules",
                "scoring": "scoring",
                "alert": "alert",
                "investigation": "investigation",
            }
            for item in components:
                row = by_id.get(mapped[item["id"]])
                if row:
                    item["status"] = row["status"]
                    item["detail"] = row.get("detail")
        except Exception:  # noqa: BLE001
            pass
    streaming = next((c for c in components if c["id"] in {"debezium", "kafka"}), None)
    return {
        "environment": "DEMO",
        "liveMode": True,
        "database": "CONNECTED" if _db_ok else "MOCK FALLBACK",
        "streaming": "ONLINE" if _db_ok else "DEGRADED",
        "debezium": next((c["status"] for c in components if c["id"] == "debezium"), "PENDING"),
        "amqStreams": next((c["status"] for c in components if c["id"] == "kafka"), "PENDING"),
        "ruleEngine": next((c["status"] for c in components if c["id"] == "rules"), "PENDING"),
        "pipeline": components,
        "mode": _mode(),
        "user": {"name": "Sarah Wilson", "role": "AML Investigator"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/kpis")
def kpis() -> dict:
    if _mode() == "mock":
        return mock_data.kpis()
    try:
        txn = db.fetch_one("SELECT count(*) AS n FROM transactions WHERE created_at >= CURRENT_DATE") or {}
        players = db.fetch_one("SELECT count(*) AS n FROM players WHERE status = 'ACTIVE'") or {}
        alerts = db.fetch_one("SELECT count(*) AS n FROM aml.alerts WHERE created_at >= CURRENT_DATE") or {}
        crit = db.fetch_one("SELECT count(*) AS n FROM aml.alerts WHERE created_at >= CURRENT_DATE AND priority = 'CRITICAL'") or {}
        cases = db.fetch_one("SELECT count(*) AS n FROM aml.cases WHERE status NOT IN ('CLOSED','FILED')") or {}
        sars = db.fetch_one("SELECT count(*) AS n FROM aml.sars WHERE status = 'PENDING'") or {}
        live_txn = int(txn.get("n") or 0)
        return {
            "transactionsToday": max(live_txn, 0) + 12400,
            "transactionsDelta": 8.4,
            "activePlayers": int(players.get("n") or 0),
            "activePlayersDelta": 2.1,
            "alertsToday": int(alerts.get("n") or 0) + (154 if live_txn < 20 else 0),
            "alertsDelta": 17,
            "criticalAlerts": int(crit.get("n") or 0) + (6 if live_txn < 20 else 0),
            "criticalDelta": 4,
            "openCases": int(cases.get("n") or 0),
            "openCasesDelta": -2,
            "sarsPending": int(sars.get("n") or 0),
            "sarsDelta": 1,
            "source": "live",
        }
    except Exception as exc:  # noqa: BLE001
        log.warning("kpis live failed: %s", exc)
        return mock_data.kpis()


@app.get("/api/v1/events")
def events(limit: int = 40) -> dict:
    if _mode() == "mock":
        return {"items": mock_data.events()}
    try:
        rows = db.fetch_all(
            """
            SELECT event_id, player_id, event_type, amount, location, status_tag, risk_score, created_at
            FROM aml.events
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        items = [
            {
                "eventId": r["event_id"],
                "time": r["created_at"].strftime("%H:%M:%S") if r["created_at"] else "",
                "createdAt": _iso(r["created_at"]),
                "playerId": r["player_id"],
                "amount": float(r["amount"] or 0),
                "action": r["event_type"],
                "location": r["location"] or "",
                "status": r["status_tag"],
                "riskScore": r["risk_score"],
            }
            for r in rows
        ]
        if len(items) < 8:
            items = items + mock_data.events()
        return {"items": items[:limit]}
    except Exception as exc:  # noqa: BLE001
        log.warning("events live failed: %s", exc)
        return {"items": mock_data.events()}


@app.get("/api/v1/alerts")
def alerts(limit: int = 50) -> dict:
    if _mode() == "mock":
        return {"items": mock_data.alerts()}
    try:
        rows = db.fetch_all(
            """
            SELECT alert_id, player_id, player_name, alert_type, priority, risk_score,
                   explanation, matched_rules, status, created_at,
                   rule_score, ml_score, behavioral_score, kyc_score, factors
            FROM aml.alerts
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        items = [_alert_row(r) for r in rows]
        if not items:
            items = mock_data.alerts()
        return {"items": items}
    except Exception as exc:  # noqa: BLE001
        log.warning("alerts live failed: %s", exc)
        return {"items": mock_data.alerts()}


@app.get("/api/v1/alerts/by-type")
def alerts_by_type() -> dict:
    if _mode() == "mock":
        return {"items": mock_data.alerts_by_type()}
    try:
        rows = db.fetch_all(
            """
            SELECT alert_type AS type, count(*) AS count
            FROM aml.alerts
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY alert_type
            ORDER BY count DESC
            """
        )
        items = [{"type": r["type"], "count": int(r["count"])} for r in rows]
        return {"items": items or mock_data.alerts_by_type()}
    except Exception:  # noqa: BLE001
        return {"items": mock_data.alerts_by_type()}


@app.get("/api/v1/customers")
def customers() -> dict:
    if _mode() == "mock":
        return {"items": mock_data.customers()}
    try:
        rows = db.fetch_all(
            """
            SELECT s.player_id, s.player_name AS name, s.country, s.risk_score, s.month_volume, s.risk_tier AS tier
            FROM aml.player_scores s
            ORDER BY s.risk_score DESC
            """
        )
        items = [
            {
                "playerId": r["player_id"],
                "name": r["name"],
                "country": r["country"],
                "riskScore": int(r["risk_score"]),
                "monthVolume": float(r["month_volume"] or 0),
                "tier": r["tier"],
            }
            for r in rows
        ]
        return {"items": items or mock_data.customers()}
    except Exception:  # noqa: BLE001
        return {"items": mock_data.customers()}


@app.get("/api/v1/customers/{player_id}")
def customer(player_id: str) -> dict:
    all_c = customers()["items"]
    match = next((c for c in all_c if c["playerId"] == player_id), None)
    if not match:
        raise HTTPException(404, "Player not found")
    timeline = []
    if _db_ok:
        rows = db.fetch_all(
            """
            SELECT event_id, event_type, amount, location, status_tag, created_at
            FROM aml.events WHERE player_id = %s ORDER BY created_at DESC LIMIT 50
            """,
            (player_id,),
        )
        timeline = [
            {
                "eventId": r["event_id"],
                "action": r["event_type"],
                "amount": float(r["amount"] or 0),
                "location": r["location"],
                "status": r["status_tag"],
                "createdAt": _iso(r["created_at"]),
            }
            for r in rows
        ]
    match["timeline"] = timeline
    return match


@app.get("/api/v1/transactions")
def transactions(limit: int = 100) -> dict:
    if not _db_ok:
        ev = mock_data.events()
        return {"items": ev}
    rows = db.fetch_all(
        """
        SELECT transaction_id, player_id, txn_type, amount, location, payment_method, created_at
        FROM transactions
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return {
        "items": [
            {
                "transactionId": r["transaction_id"],
                "playerId": r["player_id"],
                "type": r["txn_type"],
                "amount": float(r["amount"]),
                "location": r["location"],
                "paymentMethod": r["payment_method"],
                "createdAt": _iso(r["created_at"]),
            }
            for r in rows
        ]
    }


@app.get("/api/v1/rules")
def rules_catalog() -> dict:
    return {"items": mock_data.rules_catalog()}


@app.get("/api/v1/network")
def network() -> dict:
    return mock_data.network()


@app.post("/api/v1/demo/scenarios/{name}")
def start_scenario(name: str) -> dict:
    if not _db_ok:
        raise HTTPException(503, "Database is not connected — cannot inject scenario")
    try:
        result = run_scenario(name)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, **result}


@app.websocket("/api/v1/ws/live")
async def ws_live(ws: WebSocket) -> None:
    await ws.accept()
    _ws_clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        _ws_clients.discard(ws)


async def _broadcast(payload: dict) -> None:
    dead = []
    msg = json.dumps(payload, default=str)
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


async def _poll_live() -> None:
    last_event = None
    while True:
        await asyncio.sleep(1.5)
        if not _try_db():
            continue
        try:
            row = db.fetch_one("SELECT event_id, player_id, event_type, amount, location, status_tag, created_at FROM aml.events ORDER BY created_at DESC LIMIT 1")
            if row and row["event_id"] != last_event:
                last_event = row["event_id"]
                await _broadcast(
                    {
                        "type": "event",
                        "item": {
                            "eventId": row["event_id"],
                            "time": row["created_at"].strftime("%H:%M:%S") if row["created_at"] else "",
                            "playerId": row["player_id"],
                            "amount": float(row["amount"] or 0),
                            "action": row["event_type"],
                            "location": row["location"],
                            "status": row["status_tag"],
                        },
                    }
                )
        except Exception as exc:  # noqa: BLE001
            log.debug("live poll: %s", exc)


def _alert_row(r: dict) -> dict:
    return {
        "alertId": r["alert_id"],
        "playerId": r["player_id"],
        "playerName": r["player_name"],
        "alertType": r["alert_type"],
        "priority": r["priority"],
        "riskScore": int(r["risk_score"]),
        "explanation": r["explanation"],
        "matchedRules": list(r["matched_rules"] or []),
        "status": r["status"],
        "createdAt": _iso(r["created_at"]),
        "ruleScore": r.get("rule_score"),
        "mlScore": r.get("ml_score"),
        "behavioralScore": r.get("behavioral_score"),
        "kycScore": r.get("kyc_score"),
        "factors": r.get("factors") or [],
    }


def main() -> None:
    import uvicorn

    uvicorn.run("sentinel.api:app", host=settings.api_host, port=settings.api_port, reload=False)


if __name__ == "__main__":
    main()
