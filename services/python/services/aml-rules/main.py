#!/usr/bin/env python3
"""AML rules consumer: enriched → aml.rules.hits."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from casino_common.kafka_utils import JsonConsumer, JsonProducer, run_loop  # noqa: E402
from casino_common.logging_config import set_context, setup_logging  # noqa: E402
from rules_engine import evaluate_rules  # noqa: E402

log = setup_logging("aml-rules", os.getenv("LOG_LEVEL", "INFO"))
IN_TOPIC = os.getenv("ENRICHED_TOPIC", "casino.events.enriched")
OUT_TOPIC = os.getenv("RULES_TOPIC", "aml.rules.hits")
DLQ = os.getenv("DLQ_TOPIC", "casino.dlq")
producer = JsonProducer("aml-rules")


def handle(key, value):
    player_id = value.get("player_id") or key
    set_context(
        correlation_id=value.get("correlation_id"),
        event_id=value.get("event_id"),
        player_id=player_id,
        transaction_id=value.get("transaction_id"),
    )
    result = evaluate_rules(
        player_id=player_id,
        event=value,
        features=value.get("features") or {},
        event_id=value.get("event_id"),
        correlation_id=value.get("correlation_id"),
        transaction_id=value.get("transaction_id"),
    )
    producer.produce(OUT_TOPIC, result.model_dump(mode="json"), key=player_id)
    producer.flush()
    log.info("rules hits=%d score=%.2f", len(result.hits), result.aggregate_score)


def main():
    consumer = JsonConsumer("aml-rules", [IN_TOPIC])
    dlq = JsonProducer("aml-rules-dlq")
    run_loop(consumer, handle, dlq, DLQ, "aml-rules")


if __name__ == "__main__":
    main()
