"""NVIDIA Transaction Foundation Model adapter.

Blueprint: https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model

Official path (GPU, NeMo 25.09.01+):
  tokenize → Llama decoder (~29M, hidden 512) → last-token pooling → 512-d embedding
  → concatenate with tabular features → XGBoost (notebook 05)

Casino Sentinel wiring:
  TFM_MODE=hashed     CPU demo (default). Same 512-d last-token contract.
  TFM_MODE=checkpoint Load HuggingFace AutoModelForCausalLM from TFM_CHECKPOINT
                      (models/decoder-foundation-model after git lfs pull).
  TFM_MODE=http       POST sequences to TFM_EMBED_URL (sidecar in the NeMo container).
"""

from __future__ import annotations

import json
import logging
from urllib import request

import numpy as np

from .config import settings
from .models import EnrichedEvent, PlayerFeatures, PlayerProfile
from .tfm_tokenizer import EMBED_DIM, PAD_ID, observe, tokenize_history

log = logging.getLogger("sentinel.tfm")

_hf_model = None
_normal_centroid: np.ndarray | None = None
_centroid_n = 0


def _hashed_last_token_embedding(token_ids: list[int]) -> np.ndarray:
    """CPU stand-in for last-token pooling (blueprint decoder_inference.py)."""
    vec = np.zeros(EMBED_DIM, dtype=np.float32)
    last = np.zeros(EMBED_DIM, dtype=np.float32)
    for tid in token_ids:
        rng = np.random.default_rng(int(tid) + 17)
        e = rng.standard_normal(EMBED_DIM).astype(np.float32)
        n = float(np.linalg.norm(e)) + 1e-9
        e /= n
        vec = 0.88 * vec + 0.12 * e
        last = e
    # Last non-pad token dominates — same idea as HuggingFaceDecoderInference pooling=last_token
    pooled = 0.35 * vec + 0.65 * last
    n = float(np.linalg.norm(pooled)) + 1e-9
    return pooled / n


def _load_hf():
    global _hf_model
    if _hf_model is not None:
        return _hf_model
    path = settings.tfm_checkpoint
    if not path:
        return None
    try:
        import torch
        from transformers import AutoModelForCausalLM
    except ImportError:
        log.warning("transformers/torch not installed; cannot load TFM checkpoint")
        return None

    class _Pad:
        pad_token_id = PAD_ID

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = AutoModelForCausalLM.from_pretrained(path)
    model.to(device)
    model.eval()
    _hf_model = (model, device, _Pad())
    log.info("Loaded NVIDIA TFM checkpoint from %s on %s", path, device)
    return _hf_model


def _embed_checkpoint(token_ids: list[int]) -> np.ndarray | None:
    loaded = _load_hf()
    if loaded is None:
        return None
    model, device, tok = loaded
    import torch

    ids = torch.tensor([token_ids], dtype=torch.long, device=device)
    mask = (ids != tok.pad_token_id).long()
    with torch.no_grad():
        out = model(input_ids=ids, attention_mask=mask, output_hidden_states=True)
        hidden = out.hidden_states[-1]
        last = int(mask.sum().item()) - 1
        last = max(0, last)
        emb = hidden[0, last, :].float().cpu().numpy()
    n = float(np.linalg.norm(emb)) + 1e-9
    return (emb / n).astype(np.float32)


def _embed_http(token_ids: list[int]) -> np.ndarray | None:
    url = settings.tfm_embed_url
    if not url:
        return None
    payload = json.dumps({"input_ids": [token_ids], "pooling": "last_token"}).encode("utf-8")
    req = request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=5) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    rows = body.get("embeddings") or body.get("data")
    if not rows:
        return None
    emb = np.asarray(rows[0], dtype=np.float32).reshape(-1)
    if emb.size < EMBED_DIM:
        emb = np.pad(emb, (0, EMBED_DIM - emb.size))
    emb = emb[:EMBED_DIM]
    n = float(np.linalg.norm(emb)) + 1e-9
    return emb / n


def embed_player(player_id: str) -> tuple[np.ndarray, str]:
    token_ids = tokenize_history(player_id)
    mode = (settings.tfm_mode or "hashed").lower()
    if mode == "checkpoint":
        emb = _embed_checkpoint(token_ids)
        if emb is not None:
            return emb, "checkpoint"
    if mode == "http":
        try:
            emb = _embed_http(token_ids)
            if emb is not None:
                return emb, "http"
        except Exception as exc:  # noqa: BLE001
            log.warning("TFM HTTP embed failed: %s", exc)
    return _hashed_last_token_embedding(token_ids), "hashed"


def _update_centroid(emb: np.ndarray, is_suspicious: bool) -> None:
    global _normal_centroid, _centroid_n
    if is_suspicious:
        return
    if _normal_centroid is None:
        _normal_centroid = emb.copy()
        _centroid_n = 1
        return
    _centroid_n += 1
    _normal_centroid += (emb - _normal_centroid) / _centroid_n


def score_tfm(
    event: EnrichedEvent,
    features: PlayerFeatures | None = None,
    profile: PlayerProfile | None = None,
) -> tuple[int, list[dict], np.ndarray]:
    observe(event)
    emb, backend = embed_player(event.player_id)
    hist_len = len(tokenize_history(event.player_id))

    look_suspicious = False
    if features:
        look_suspicious = features.just_below_10k_count_24h >= 2 or (
            features.wager_to_buyin_ratio is not None and features.wager_to_buyin_ratio < 0.1
        )
    _update_centroid(emb, look_suspicious)

    if _normal_centroid is not None:
        sim = float(np.dot(emb, _normal_centroid) / (np.linalg.norm(_normal_centroid) + 1e-9))
        dist = max(0.0, 1.0 - sim)
    else:
        dist = float(np.std(emb))

    # Sequence surprise: high amount + short gaps encoded in embedding energy
    energy = float(np.mean(np.abs(emb[:32])))
    score = int(max(1, min(99, round(dist * 70 + energy * 80))))
    if features and features.just_below_10k_count_24h >= 2:
        score = min(99, score + 12)
    if profile and (profile.watchlist_hit or profile.pep_flag):
        score = min(99, score + 8)

    factors = [
        {
            "feature": "nvidia_tfm",
            "model": "transaction-foundation-model",
            "backend": backend,
            "embedding_dim": EMBED_DIM,
            "pooling": "last_token",
            "tokens": hist_len,
            "cosine_distance": round(dist, 4),
            "blueprint": "https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model",
        }
    ]
    return score, factors, emb
