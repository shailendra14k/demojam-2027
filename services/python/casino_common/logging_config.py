"""Structured JSON logging for Casino Sentinel services."""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional

correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)
event_id_var: ContextVar[Optional[str]] = ContextVar("event_id", default=None)
player_id_var: ContextVar[Optional[str]] = ContextVar("player_id", default=None)
transaction_id_var: ContextVar[Optional[str]] = ContextVar("transaction_id", default=None)


def new_correlation_id() -> str:
    cid = str(uuid.uuid4())
    correlation_id_var.set(cid)
    return cid


def set_context(
    *,
    correlation_id: Optional[str] = None,
    event_id: Optional[str] = None,
    player_id: Optional[str] = None,
    transaction_id: Optional[str] = None,
) -> None:
    if correlation_id is not None:
        correlation_id_var.set(correlation_id)
    if event_id is not None:
        event_id_var.set(event_id)
    if player_id is not None:
        player_id_var.set(player_id)
    if transaction_id is not None:
        transaction_id_var.set(transaction_id)


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "message": record.getMessage(),
            "correlation_id": correlation_id_var.get() or getattr(record, "correlation_id", None),
            "event_id": event_id_var.get() or getattr(record, "event_id", None),
            "player_id": player_id_var.get() or getattr(record, "player_id", None),
            "transaction_id": transaction_id_var.get() or getattr(record, "transaction_id", None),
            "logger": record.name,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key in ("extra_fields",):
            extra = getattr(record, key, None)
            if isinstance(extra, dict):
                payload.update(extra)
        return json.dumps(payload, default=str)


def setup_logging(service: str, level: str = "INFO") -> logging.Logger:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logging.getLogger(service)
