"""Kafka producer/consumer helpers with DLQ support."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "sentinel-kafka-kafka-bootstrap:9092")


def _base_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "bootstrap.servers": kafka_bootstrap(),
    }
    security = os.getenv("KAFKA_SECURITY_PROTOCOL")
    if security:
        cfg["security.protocol"] = security
    return cfg


class JsonProducer:
    def __init__(self, client_id: str) -> None:
        self._client_id = client_id
        self._producer = None

    def _ensure(self):
        if self._producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("confluent-kafka is required") from exc
            cfg = _base_config()
            cfg["client.id"] = self._client_id
            cfg["acks"] = "all"
            self._producer = Producer(cfg)
        return self._producer

    def produce(self, topic: str, value: dict[str, Any], key: Optional[str] = None) -> None:
        producer = self._ensure()
        payload = json.dumps(value, default=str).encode("utf-8")
        k = key.encode("utf-8") if key else None
        producer.produce(topic, value=payload, key=k)
        producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        if self._producer is not None:
            self._producer.flush(timeout)


class JsonConsumer:
    def __init__(self, group_id: str, topics: list[str]) -> None:
        self.group_id = group_id
        self.topics = topics
        self._consumer = None
        self.last_topic: Optional[str] = None

    def _ensure(self):
        if self._consumer is None:
            try:
                from confluent_kafka import Consumer
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("confluent-kafka is required") from exc
            cfg = _base_config()
            cfg.update(
                {
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True,
                }
            )
            self._consumer = Consumer(cfg)
            self._consumer.subscribe(self.topics)
        return self._consumer

    def poll_json(self, timeout: float = 1.0) -> Optional[tuple[Optional[str], dict[str, Any]]]:
        consumer = self._ensure()
        msg = consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            logger.warning("kafka consumer error: %s", msg.error())
            return None
        self.last_topic = msg.topic()
        key = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))
        # Help normalizers when Debezium ExtractNewRecordState drops envelope metadata
        if isinstance(value, dict) and "table" not in value and self.last_topic:
            # casino.cdc.public.transactions → transactions
            parts = self.last_topic.split(".")
            if len(parts) >= 1:
                value.setdefault("_kafka_topic", self.last_topic)
                value.setdefault("table", parts[-1])
        return key, value

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()


def run_loop(
    consumer: JsonConsumer,
    handler: Callable[[Optional[str], dict[str, Any]], None],
    dlq_producer: Optional[JsonProducer] = None,
    dlq_topic: str = "casino.dlq",
    service: str = "unknown",
) -> None:
    while True:
        try:
            item = consumer.poll_json()
            if item is None:
                continue
            key, value = item
            handler(key, value)
        except Exception as exc:  # noqa: BLE001
            logger.exception("handler failure in %s", service)
            if dlq_producer is not None:
                dlq_producer.produce(
                    dlq_topic,
                    {
                        "service": service,
                        "error": str(exc),
                        "payload": getattr(exc, "payload", None),
                    },
                    key=service,
                )
                dlq_producer.flush()
