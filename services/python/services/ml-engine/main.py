#!/usr/bin/env python3
"""ML engine: features/enriched → aml.ml.scores."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ai" / "adapter"))

from casino_common.kafka_utils import JsonConsumer, JsonProducer, run_loop  # noqa: E402
from casino_common.logging_config import set_context, setup_logging  # noqa: E402
from casino_common.models import BehavioralScore, MLPrediction  # noqa: E402
from behavioral import get_adapter  # noqa: E402
from sentinel.aml_model import AMLModel  # noqa: E402

log = setup_logging("ml-engine", os.getenv("LOG_LEVEL", "INFO"))
IN_TOPIC = os.getenv("ENRICHED_TOPIC", "casino.events.enriched")
ML_TOPIC = os.getenv("ML_TOPIC", "aml.ml.scores")
BEH_TOPIC = os.getenv("BEHAVIORAL_TOPIC", "aml.behavioral.scores")
DLQ = os.getenv("DLQ_TOPIC", "casino.dlq")

model = AMLModel()
model.load()
adapter = get_adapter(os.getenv("AI_MODE", "mock"))
producer = JsonProducer("ml-engine")


def handle(key, value):
    player_id = value.get("player_id") or key
    set_context(
        correlation_id=value.get("correlation_id"),
        event_id=value.get("event_id"),
        player_id=player_id,
        transaction_id=value.get("transaction_id"),
    )
    features = value.get("features") or {}
    pred = model.predict(features)
    ml = MLPrediction(
        event_id=value.get("event_id"),
        player_id=player_id,
        transaction_id=value.get("transaction_id"),
        correlation_id=value.get("correlation_id"),
        timestamp=datetime.now(timezone.utc),
        model_name=pred["model_name"],
        model_version=pred["model_version"],
        score=pred["score"],
        label=pred["label"],
        features_used=pred["features_used"],
        shap_top=pred["shap_top"],
    )
    beh = adapter.score(
        player_id=player_id,
        event=value,
        features=features,
        event_id=value.get("event_id"),
        correlation_id=value.get("correlation_id"),
    )
    producer.produce(ML_TOPIC, ml.model_dump(mode="json"), key=player_id)
    producer.produce(BEH_TOPIC, beh.model_dump(mode="json"), key=player_id)
    producer.flush()
    log.info("ml=%.2f behavioral=%.2f", ml.score, beh.score)


def main():
    consumer = JsonConsumer("ml-engine", [IN_TOPIC])
    dlq = JsonProducer("ml-engine-dlq")
    run_loop(consumer, handle, dlq, DLQ, "ml-engine")


if __name__ == "__main__":
    main()
