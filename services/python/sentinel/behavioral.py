"""NVIDIA Transaction Foundation Model sequence score (architecture box 5).

GPU / HTTP: HuggingFace decoder via ``sentinel.tfm`` (TFM_MODE=http + TFM_EMBED_URL).
Hashed fallback when TFM_MODE=hashed (demo without GPU).
"""

from __future__ import annotations

from . import tfm
from .models import EnrichedEvent, PlayerFeatures, PlayerProfile


def score_behavioral(
    event: EnrichedEvent,
    features: PlayerFeatures | None = None,
    profile: PlayerProfile | None = None,
) -> tuple[int, list[dict]]:
    if features is None:
        features = PlayerFeatures(player_id=event.player_id)
    tfm_score, tfm_factors, _emb = tfm.score_tfm(event, features, profile)
    return max(1, min(99, int(tfm_score))), tfm_factors
