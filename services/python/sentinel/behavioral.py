"""NVIDIA Transaction Foundation Model sequence score (architecture box 5).

GPU: HuggingFace decoder from the official blueprint checkpoint.
CPU: 512-d last-token hashed embedding with the same tokenizer contract,
     blended with Isolation Forest so the demo still runs without an A100.
"""

from __future__ import annotations

from . import ml, tfm
from .models import EnrichedEvent, PlayerFeatures, PlayerProfile


def score_behavioral(
    event: EnrichedEvent,
    features: PlayerFeatures | None = None,
    profile: PlayerProfile | None = None,
) -> tuple[int, list[dict]]:
    if features is None:
        features = PlayerFeatures(player_id=event.player_id)
    tfm_score, tfm_factors, _emb = tfm.score_tfm(event, features, profile)
    iso_score, iso_factors = ml.score_anomaly(event, features, profile)
    # Blueprint notebook 05: embeddings add lift on top of tabular models.
    # 35% architecture slot = TFM embedding anomaly, with Isolation Forest as CPU assist.
    blended = int(round(0.65 * tfm_score + 0.35 * iso_score))
    return max(1, min(99, blended)), tfm_factors + iso_factors
