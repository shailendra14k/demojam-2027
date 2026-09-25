"""Behavioral model adapter: mock | cpu | nvidia."""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from casino_common.models import BehavioralScore

log = logging.getLogger("behavioral-adapter")

_DEFAULT_IFOREST = (
    Path(__file__).resolve().parents[2] / "ml" / "models" / "aml_iforest.joblib"
)


class BehavioralModelAdapter(ABC):
    name: str

    @abstractmethod
    def score(self, player_id: str, event: dict[str, Any], features: dict[str, float],
              event_id: str, correlation_id: str) -> BehavioralScore:
        raise NotImplementedError


class MockBehavioralAdapter(BehavioralModelAdapter):
    name = "mock"

    def score(self, player_id: str, event: dict[str, Any], features: dict[str, float],
              event_id: str, correlation_id: str) -> BehavioralScore:
        amount = float(event.get("amount") or 0)
        baseline = features.get("w30d_avg_tx_amount") or features.get("static_avg_tx_amount") or 100.0
        ratio = amount / max(baseline, 1.0)
        flags = []
        score = min(1.0, max(0.0, (ratio - 1.0) / 20.0))
        if ratio >= 10:
            flags.append("amount_spike")
            score = max(score, 0.75)
        if features.get("static_shared_device_count", 0) >= 3:
            flags.append("shared_device")
            score = max(score, 0.6)
        if features.get("w1h_withdrawal_sum", 0) > 0 and features.get("w1h_deposit_sum", 0) > 0:
            flags.append("rapid_turnover")
            score = max(score, 0.55)
        return BehavioralScore(
            event_id=event_id,
            player_id=player_id,
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            adapter=self.name,
            score=score,
            anomaly_flags=flags,
            explanation={
                "method": "deterministic heuristic mock",
                "amount_to_baseline_ratio": ratio,
                "flags": flags,
            },
        )


class CpuBehavioralAdapter(BehavioralModelAdapter):
    """CPU IsolationForest fit on synthetic *normal* casino feature vectors (not N(0,1) noise).

    Prefers a baked ``aml_iforest.joblib`` (joblib, demojam-style) when present;
    otherwise fits in-memory on synthetic normals.
    """

    name = "cpu"

    def __init__(self, model_path: Path | None = None) -> None:
        self._model = None
        self._model_path = Path(
            model_path or os.getenv("IFOREST_MODEL_PATH", str(_DEFAULT_IFOREST))
        )

    def _ensure(self):
        if self._model is not None:
            return self._model

        import joblib
        from sklearn.ensemble import IsolationForest
        import numpy as np

        if self._model_path.exists():
            self._model = joblib.load(self._model_path)
            log.info("loaded IsolationForest from %s", self._model_path)
            return self._model

        rng = np.random.default_rng(42)
        n = 800
        # Columns: 5m_sum, 1h_sum, dep24, wd24, wager24, avg30d, shared_dev, amount/1k
        X = np.column_stack(
            [
                rng.uniform(0, 2000, n),
                rng.uniform(0, 8000, n),
                rng.uniform(200, 5000, n),
                rng.uniform(0, 3000, n),
                rng.uniform(100, 6000, n),
                rng.uniform(50, 800, n),
                np.ones(n),
                rng.uniform(0.05, 5.0, n),
            ]
        )
        self._model = IsolationForest(n_estimators=80, contamination=0.08, random_state=42)
        self._model.fit(X)
        try:
            self._model_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(self._model, self._model_path)
            log.info("fitted IsolationForest and saved %s", self._model_path)
        except OSError as exc:
            log.debug("could not persist IsolationForest (%s); keeping in-memory", exc)
        return self._model

    def score(self, player_id: str, event: dict[str, Any], features: dict[str, float],
              event_id: str, correlation_id: str) -> BehavioralScore:
        import numpy as np

        model = self._ensure()
        vec = np.array([[
            features.get("w5m_tx_sum", 0),
            features.get("w1h_tx_sum", 0),
            features.get("w24h_deposit_sum", 0),
            features.get("w24h_withdrawal_sum", 0),
            features.get("w24h_wager_sum", 0),
            features.get("w30d_avg_tx_amount", 0),
            features.get("static_shared_device_count", 0),
            float(event.get("amount") or 0) / 1000.0,
        ]])
        raw = float(model.decision_function(vec)[0])
        score = float(min(1.0, max(0.0, 0.5 - raw)))
        flags = ["isolation_forest_anomaly"] if score >= 0.6 else []
        return BehavioralScore(
            event_id=event_id,
            player_id=player_id,
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            adapter=self.name,
            score=score,
            anomaly_flags=flags,
            explanation={
                "method": "sklearn IsolationForest on synthetic normal casino features",
                "decision_function": raw,
            },
        )


class NvidiaBehavioralAdapter(BehavioralModelAdapter):
    """NVIDIA Transaction Foundation Model via ``sentinel.tfm`` (TFM_MODE / TFM_EMBED_URL).

    Prefer ConfigMap keys ``TFM_EMBED_URL`` + ``TFM_MODE=http`` (GPU ``tfm-embed`` service).
    Legacy ``NVIDIA_TFM_ENDPOINT`` is accepted as a URL alias only (no API key required).
    """

    name = "nvidia"

    def __init__(self) -> None:
        self.embed_url = (
            os.getenv("TFM_EMBED_URL") or os.getenv("NVIDIA_TFM_ENDPOINT") or ""
        ).strip()
        self._fallback = MockBehavioralAdapter()
        if self.embed_url and (os.getenv("TFM_MODE") or "").lower() in ("", "hashed"):
            # URL set but mode left at default — force HTTP so score_tfm hits the GPU service.
            os.environ["TFM_MODE"] = "http"
            try:
                from sentinel.config import settings as _settings

                _settings.tfm_mode = "http"
                if not _settings.tfm_embed_url:
                    _settings.tfm_embed_url = self.embed_url
            except Exception:  # noqa: BLE001
                pass
            log.info("TFM_EMBED_URL set; overriding TFM_MODE=http for nvidia adapter")

    def score(self, player_id: str, event: dict[str, Any], features: dict[str, float],
              event_id: str, correlation_id: str) -> BehavioralScore:
        if not self.embed_url and (os.getenv("TFM_MODE") or "hashed").lower() == "hashed":
            fb = self._fallback.score(player_id, event, features, event_id, correlation_id)
            fb.adapter = "nvidia-fallback-mock"
            fb.explanation["note"] = "TFM_EMBED_URL not set; using mock fallback"
            return fb
        try:
            from sentinel import tfm as sentinel_tfm
            from sentinel.models import EnrichedEvent, PlayerFeatures

            created_raw = event.get("created_at") or event.get("timestamp")
            if isinstance(created_raw, datetime):
                created_at = created_raw if created_raw.tzinfo else created_raw.replace(tzinfo=timezone.utc)
            elif isinstance(created_raw, str) and created_raw:
                created_at = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            else:
                created_at = datetime.now(timezone.utc)

            enriched = EnrichedEvent(
                event_id=str(event_id or event.get("event_id") or "unknown"),
                player_id=str(player_id),
                source_table=str(event.get("source_table") or event.get("event_source") or "transactions"),
                canonical_type=str(
                    event.get("canonical_type") or event.get("event_type") or "UNKNOWN"
                ),
                amount=float(event.get("amount") or 0),
                currency=str(event.get("currency") or "USD"),
                location=str(event.get("location") or ""),
                payment_method=str(
                    event.get("payment_method")
                    or event.get("payment_method_id")
                    or event.get("channel")
                    or ""
                ),
                created_at=created_at,
                raw=event.get("raw") if isinstance(event.get("raw"), dict) else {},
                pep_flag=bool(event.get("is_pep") or event.get("pep_flag")),
                watchlist_hit=bool(event.get("watchlist_hit")),
            )
            pf = PlayerFeatures(
                player_id=str(player_id),
                cash_in_24h=float(
                    features.get("w24h_deposit_sum") or features.get("cash_in_24h") or 0
                ),
                total_wagered_24h=float(
                    features.get("w24h_wager_sum") or features.get("total_wagered_24h") or 0
                ),
                just_below_10k_count_24h=int(
                    features.get("just_below_10k_count_24h")
                    or features.get("w24h_just_below_10k")
                    or 0
                ),
            )
            score_100, factors, _emb = sentinel_tfm.score_tfm(enriched, pf, None)
            backend = "unknown"
            if factors and isinstance(factors[0], dict):
                backend = str(factors[0].get("backend") or "unknown")
            score = max(0.0, min(1.0, float(score_100) / 100.0))
            flags = ["nvidia_tfm"] if score >= 0.55 else []
            if backend == "hashed" and self.embed_url:
                flags.append("tfm_http_fallback_hashed")
            return BehavioralScore(
                event_id=str(event_id or enriched.event_id),
                player_id=str(player_id),
                correlation_id=str(correlation_id or ""),
                timestamp=datetime.now(timezone.utc),
                adapter=self.name,
                score=score,
                anomaly_flags=flags,
                explanation={
                    "method": "sentinel.tfm",
                    "backend": backend,
                    "tfm_score_100": score_100,
                    "factors": factors,
                },
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("nvidia TFM score failed: %s", exc)
            fb = self._fallback.score(player_id, event, features, event_id, correlation_id)
            fb.adapter = "nvidia-error-fallback"
            fb.explanation["error"] = str(exc)
            return fb


def get_adapter(mode: str | None = None) -> BehavioralModelAdapter:
    mode = (mode or os.getenv("AI_MODE", "mock")).lower()
    if mode == "cpu":
        return CpuBehavioralAdapter()
    if mode == "nvidia" or mode == "gpu":
        return NvidiaBehavioralAdapter()
    return MockBehavioralAdapter()
