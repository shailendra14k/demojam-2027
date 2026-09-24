#!/usr/bin/env python3
"""Risk engine joins rules + ml + behavioral → aml.risk.assessments."""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(ROOT / "services" / "explainability"))

from casino_common.kafka_utils import JsonConsumer, JsonProducer  # noqa: E402
from casino_common.logging_config import set_context, setup_logging  # noqa: E402
from casino_common.models import BehavioralScore, MLPrediction, RuleResult  # noqa: E402
from explain import explain  # noqa: E402
from risk_aggregator import aggregate  # noqa: E402

log = setup_logging("risk-engine", os.getenv("LOG_LEVEL", "INFO"))
RULES_TOPIC = os.getenv("RULES_TOPIC", "aml.rules.hits")
ML_TOPIC = os.getenv("ML_TOPIC", "aml.ml.scores")
BEH_TOPIC = os.getenv("BEHAVIORAL_TOPIC", "aml.behavioral.scores")
OUT_TOPIC = os.getenv("RISK_TOPIC", "aml.risk.assessments")
EXPLAIN_TOPIC = os.getenv("EXPLAIN_TOPIC", "aml.explanations")
DLQ = os.getenv("DLQ_TOPIC", "casino.dlq")

producer = JsonProducer("risk-engine")
_lock = threading.Lock()
_buffer: dict[str, dict[str, Any]] = {}


def _key(msg: dict) -> str:
    return f"{msg.get('event_id')}:{msg.get('player_id')}"


def _maybe_emit(buf_key: str) -> None:
    with _lock:
        item = _buffer.get(buf_key)
        if not item:
            return
        if not item.get("rules"):
            return
        # Allow ML/behavioral to be optional briefly
        age = time.time() - item["ts"]
        if age < 2 and (not item.get("ml") or not item.get("beh")):
            return
        rules = RuleResult.model_validate(item["rules"])
        ml = MLPrediction.model_validate(item["ml"]) if item.get("ml") else None
        beh = BehavioralScore.model_validate(item["beh"]) if item.get("beh") else None
        assessment = aggregate(rules, ml, beh)
        exp = explain(assessment)
        producer.produce(OUT_TOPIC, assessment.model_dump(mode="json"), key=assessment.player_id)
        producer.produce(EXPLAIN_TOPIC, exp, key=assessment.player_id)
        producer.flush()
        del _buffer[buf_key]
        set_context(
            correlation_id=assessment.correlation_id,
            event_id=assessment.event_id,
            player_id=assessment.player_id,
            transaction_id=assessment.transaction_id,
        )
        log.info("risk=%s score=%.2f", assessment.risk_level.value, assessment.final_score)


def consume_topic(topic: str, kind: str) -> None:
    consumer = JsonConsumer(f"risk-engine-{kind}", [topic])
    while True:
        item = consumer.poll_json()
        if item is None:
            # flush aged partials
            with _lock:
                keys = list(_buffer.keys())
            for k in keys:
                _maybe_emit(k)
            continue
        _, value = item
        bk = _key(value)
        with _lock:
            slot = _buffer.setdefault(bk, {"ts": time.time()})
            slot[kind] = value
            slot["ts"] = slot.get("ts", time.time())
        _maybe_emit(bk)


def main():
    threads = [
        threading.Thread(target=consume_topic, args=(RULES_TOPIC, "rules"), daemon=True),
        threading.Thread(target=consume_topic, args=(ML_TOPIC, "ml"), daemon=True),
        threading.Thread(target=consume_topic, args=(BEH_TOPIC, "beh"), daemon=True),
    ]
    for t in threads:
        t.start()
    log.info("risk-engine started")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
