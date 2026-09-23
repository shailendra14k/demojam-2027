from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Iterable

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from .config import settings

log = logging.getLogger("sentinel.kafka")

TOPICS = [
    "casino.cdc.players",
    "casino.cdc.transactions",
    "casino.cdc.wagers",
    "casino.cdc.chips",
    "casino.cdc.cage",
    "casino.cdc.kyc",
    "casino.cdc.player_accounts",
    "casino.cdc.gaming_sessions",
    "casino.cdc.payment_methods",
    "casino.cdc.devices",
    "casino.events.normalized",
    "casino.events.enriched",
    "aml.features",
    "aml.scores.rules",
    "aml.scores.ml",
    "aml.scores.behavioral",
    "aml.scores.kyc",
    "aml.alerts",
    "aml.player.risk_profiles",
]


def wait_for_kafka(retries: int = 60, delay: float = 3.0) -> None:
    last = None
    for i in range(retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=settings.kafka_bootstrap,
                client_id=f"{settings.kafka_client_id}-admin",
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )
            admin.list_topics()
            admin.close()
            log.info("Kafka is reachable at %s", settings.kafka_bootstrap)
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            log.warning("Kafka not ready (%s/%s): %s", i + 1, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Kafka never became ready: {last}")


def ensure_topics(names: Iterable[str] = TOPICS, partitions: int = 3, rf: int = 1) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap,
        client_id=f"{settings.kafka_client_id}-admin",
    )
    existing = set(admin.list_topics())
    new = [
        NewTopic(name=n, num_partitions=partitions, replication_factor=rf)
        for n in names
        if n not in existing
    ]
    if new:
        try:
            admin.create_topics(new)
            log.info("Created topics: %s", [t.name for t in new])
        except TopicAlreadyExistsError:
            pass
        except Exception as exc:  # noqa: BLE001
            log.warning("create_topics: %s", exc)
    admin.close()


def producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        client_id=settings.kafka_client_id,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        acks="all",
        retries=8,
        linger_ms=20,
    )


def consumer(topics: list[str], group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap,
        client_id=f"{settings.kafka_client_id}-{group_id}",
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=_deserialize,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        consumer_timeout_ms=1000,
    )


def _deserialize(raw: bytes | None) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"_raw": raw.decode("utf-8", errors="replace")}


def publish(prod: KafkaProducer, topic: str, key: str, value: dict) -> None:
    if prod is None or not hasattr(prod, "send"):
        return
    try:
        prod.send(topic, key=key, value=value)
    except Exception as exc:  # noqa: BLE001
        log.warning("publish %s failed: %s", topic, exc)


def consume_forever(
    topics: list[str],
    group_id: str,
    handler: Callable[[str, Any], None],
) -> None:
    while True:
        try:
            cons = consumer(topics, group_id)
            log.info("Consuming %s as %s", topics, group_id)
            while True:
                records = cons.poll(timeout_ms=1000)
                for tp, batch in records.items():
                    for rec in batch:
                        if rec.value is None:
                            continue
                        handler(tp.topic, rec.value)
        except Exception as exc:  # noqa: BLE001
            log.exception("Consumer loop error: %s", exc)
            time.sleep(3)
