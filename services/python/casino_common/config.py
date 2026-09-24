"""Shared configuration helpers."""

from __future__ import annotations

import os
from functools import lru_cache


@lru_cache
def settings() -> dict:
    return {
        "postgres_host": os.getenv("POSTGRES_HOST", "casino-db"),
        "postgres_port": int(os.getenv("POSTGRES_PORT", "5432")),
        "postgres_db": os.getenv("POSTGRES_DB", "casino"),
        "postgres_user": os.getenv("POSTGRES_USER", "casino"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD", "casino"),
        "kafka_bootstrap": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "sentinel-kafka-kafka-bootstrap:9092"
        ),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "ai_mode": os.getenv("AI_MODE", "mock"),  # mock|cpu|nvidia
        "risk_weight_rules": float(os.getenv("RISK_WEIGHT_RULES", "0.45")),
        "risk_weight_ml": float(os.getenv("RISK_WEIGHT_ML", "0.35")),
        "risk_weight_behavioral": float(os.getenv("RISK_WEIGHT_BEHAVIORAL", "0.20")),
        "alert_threshold": float(os.getenv("ALERT_THRESHOLD", "0.55")),
    }


def database_url() -> str:
    """Prefer DATABASE_URL when set; otherwise build from POSTGRES_* defaults (casino-db)."""
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        # SQLAlchemy expects a driver prefix for psycopg2
        if explicit.startswith("postgresql://"):
            return explicit.replace("postgresql://", "postgresql+psycopg2://", 1)
        if explicit.startswith("postgres://"):
            return explicit.replace("postgres://", "postgresql+psycopg2://", 1)
        return explicit
    s = settings()
    return (
        f"postgresql+psycopg2://{s['postgres_user']}:{s['postgres_password']}"
        f"@{s['postgres_host']}:{s['postgres_port']}/{s['postgres_db']}"
    )
