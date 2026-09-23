"""Casino → NVIDIA Transaction Foundation Model tokenizer.

Mirrors the blueprint domain tokenizer in
https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model
(src/tokenizer): amount bins, hour/dow/month, merchant/type hash, time-delta,
identity. Each transaction becomes a short token sequence (~12 tokens).

The official pipeline uses RAPIDS cuDF on GPU. This adapter is CPU so the
AML demo runs without an A100; swap in their FinancialTokenizerPipeline when
the NeMo container is attached (TFM_CHECKPOINT).
"""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque

from .models import EnrichedEvent

EMBED_DIM = 512
VOCAB_SIZE = 6251  # blueprint vocabulary size
PAD_ID = 0
MAX_SEQ_TXNS = 48
TOKENS_PER_TXN = 12

_history: dict[str, Deque[EnrichedEvent]] = defaultdict(lambda: deque(maxlen=MAX_SEQ_TXNS))

# Amount bins similar to the blueprint (log-ish dollar buckets)
_AMOUNT_BINS = [
    0, 5, 10, 20, 40, 80, 150, 300, 600, 1200, 2500, 5000,
    8000, 9000, 10000, 15000, 25000, 50000, 100000,
]


def observe(event: EnrichedEvent) -> None:
    _history[event.player_id].append(event)


def sequence_for(player_id: str) -> list[EnrichedEvent]:
    return list(_history[player_id])


def _bin_amount(amount: float) -> int:
    for i, edge in enumerate(_AMOUNT_BINS):
        if amount <= edge:
            return 10 + i
    return 10 + len(_AMOUNT_BINS)


def _hash_token(key: str, namespace: int) -> int:
    h = 2166136261
    for ch in f"{namespace}:{key}":
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return 1 + (h % (VOCAB_SIZE - 2))


def tokenize_event(event: EnrichedEvent, prev: EnrichedEvent | None) -> list[int]:
    ts = event.created_at
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta_min = 0
    if prev is not None:
        p = prev.created_at
        if p.tzinfo is None:
            p = p.replace(tzinfo=timezone.utc)
        delta_min = max(0, int((ts - p).total_seconds() / 60))
    delta_bin = min(delta_min // 15, 96)
    return [
        _bin_amount(float(event.amount or 0)),
        _hash_token(event.canonical_type, 1),
        _hash_token(event.payment_method or "UNK", 2),
        _hash_token(event.location or "UNK", 3),
        200 + ts.hour,
        230 + ts.weekday(),
        240 + ts.month,
        260 + delta_bin,
        _hash_token(event.player_id, 4),
        _hash_token(event.source_table, 5),
        1 if float(event.amount or 0) >= 8000 else 2,
        3 if "WIRE" in (event.canonical_type or "").upper() else 4,
    ]


def tokenize_history(player_id: str) -> list[int]:
    seq = sequence_for(player_id)
    ids: list[int] = []
    prev = None
    for ev in seq:
        ids.extend(tokenize_event(ev, prev))
        prev = ev
    if not ids:
        ids = [PAD_ID]
    return ids[-MAX_SEQ_TXNS * TOKENS_PER_TXN :]
