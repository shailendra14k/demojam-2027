#!/usr/bin/env python3
"""CDC event normalizer: casino.cdc.* → casino.events.normalized (+ DLQ)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from casino_common.kafka_utils import JsonConsumer, JsonProducer, run_loop  # noqa: E402
from casino_common.logging_config import set_context, setup_logging  # noqa: E402
from casino_common.models import NormalizedEvent  # noqa: E402

log = setup_logging("event-normalizer", os.getenv("LOG_LEVEL", "INFO"))

CDC_TOPICS = [
    t.strip()
    for t in os.getenv(
        "CDC_TOPICS",
        "casino.cdc.players,casino.cdc.transactions,casino.cdc.wagers,"
        "casino.cdc.chips,casino.cdc.cage,casino.cdc.kyc,"
        "casino.cdc.player_accounts,casino.cdc.gaming_sessions,"
        "casino.cdc.payment_methods,casino.cdc.devices",
    ).split(",")
    if t.strip()
]
OUT_TOPIC = os.getenv("NORMALIZED_TOPIC", "casino.events.normalized")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "casino.dlq")


def unwrap_debezium(payload: dict) -> dict:
    """Support raw Debezium envelope or already-flattened events."""
    if "payload" in payload and isinstance(payload["payload"], dict):
        p = payload["payload"]
        after = p.get("after") or p.get("before") or {}
        op = p.get("op", "u")
        source = p.get("source", {})
        return {
            "op": op,
            "table": source.get("table") or payload.get("table"),
            "ts_ms": p.get("ts_ms"),
            "after": after,
            "raw": payload,
        }
    if "after" in payload:
        return {
            "op": payload.get("op", "c"),
            "table": payload.get("table"),
            "ts_ms": payload.get("ts_ms"),
            "after": payload.get("after") or {},
            "raw": payload,
        }
    return {"op": "c", "table": payload.get("table"), "ts_ms": None, "after": payload, "raw": payload}


def normalize(key: str | None, value: dict) -> NormalizedEvent:
    unwrapped = unwrap_debezium(value)
    after = unwrapped["after"] or {}
    table = (unwrapped.get("table") or "").lower()
    op = unwrapped.get("op", "c")
    if op == "d":
        raise ValueError("delete events are ignored for AML demo path")

    player_id = after.get("player_id")
    transaction_id = (
        after.get("transaction_id")
        or after.get("wager_id")
        or after.get("chip_txn_id")
        or after.get("chip_tx_id")
        or after.get("cage_txn_id")
        or after.get("cage_tx_id")
    )
    amount = after.get("amount") or after.get("stake")
    ts_raw = after.get("created_at") or after.get("updated_at") or after.get("verified_at") or after.get("registered_at")
    if ts_raw:
        timestamp = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
    elif unwrapped.get("ts_ms"):
        timestamp = datetime.fromtimestamp(int(unwrapped["ts_ms"]) / 1000, tz=timezone.utc)
    else:
        timestamp = datetime.now(timezone.utc)

    event_type = {
        "players": "PLAYER",
        "transactions": "TRANSACTION",
        "wagers": "WAGER",
        "chip_transactions": "CHIP",
        "cage_transactions": "CAGE",
        "kyc_profiles": "KYC",
        "devices": "DEVICE",
        "payment_methods": "PAYMENT",
        "player_accounts": "ACCOUNT",
        "gaming_sessions": "SESSION",
    }.get(table, table.upper() or "UNKNOWN")

    if event_type == "TRANSACTION":
        # A schema uses txn_type; accept B aliases too
        event_type = str(
            after.get("txn_type") or after.get("transaction_type") or "TRANSACTION"
        ).upper()
    elif event_type == "CHIP":
        event_type = str(after.get("direction") or after.get("tx_type") or "CHIP").upper()
    elif event_type == "CAGE":
        event_type = str(after.get("direction") or after.get("tx_type") or "CAGE").upper()

    correlation_id = str(after.get("correlation_id") or uuid4())
    event = NormalizedEvent(
        event_id=str(uuid4()),
        event_type=event_type,
        event_source=f"debezium:{table}",
        player_id=str(player_id) if player_id else None,
        transaction_id=str(transaction_id) if transaction_id else None,
        correlation_id=correlation_id,
        timestamp=timestamp,
        amount=float(amount) if amount is not None else None,
        currency=str(after.get("currency") or "USD"),
        channel=after.get("channel") or after.get("payment_method"),
        device_id=str(after["device_id"]) if after.get("device_id") else None,
        payment_method_id=str(after["payment_id"])
        if after.get("payment_id")
        else (str(after["payment_method_id"]) if after.get("payment_method_id") else None),
        location=after.get("location") or after.get("cage_window") or after.get("table_id"),
        raw=unwrapped["raw"],
        metadata={
            "table": table,
            "op": op,
            "scenario_tag": after.get("scenario_tag") or after.get("demo_label"),
            "txn_type": after.get("txn_type") or after.get("transaction_type") or after.get("direction"),
            "payment_method": after.get("payment_method"),
            "fingerprint": after.get("fingerprint") or after.get("device_fingerprint"),
            "pep_flag": after.get("pep_flag"),
            "watchlist_hit": after.get("watchlist_hit"),
        },
    )
    set_context(
        correlation_id=event.correlation_id,
        event_id=event.event_id,
        player_id=event.player_id,
        transaction_id=event.transaction_id,
    )
    return event


def main() -> None:
    consumer = JsonConsumer("event-normalizer", CDC_TOPICS)
    producer = JsonProducer("event-normalizer")
    dlq = JsonProducer("event-normalizer-dlq")

    def handler(key, value):
        try:
            event = normalize(key, value)
            if not event.player_id and event.event_type not in ("PLAYER",):
                raise ValueError("missing player_id")
            producer.produce(
                OUT_TOPIC,
                event.model_dump(mode="json"),
                key=event.player_id or event.event_id,
            )
            producer.flush()
            log.info("normalized event type=%s", event.event_type)
        except Exception as exc:  # noqa: BLE001
            log.exception("normalize failed")
            dlq.produce(
                DLQ_TOPIC,
                {"service": "event-normalizer", "error": str(exc), "payload": value},
                key=key,
            )
            dlq.flush()

    log.info("starting normalizer topics=%s", CDC_TOPICS)
    run_loop(consumer, handler, dlq, DLQ_TOPIC, "event-normalizer")


if __name__ == "__main__":
    main()
