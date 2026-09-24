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
    """Optional NVIDIA Transaction Foundation Model adapter (not required for demo)."""

    name = "nvidia"

    def __init__(self) -> None:
        self.endpoint = os.getenv("NVIDIA_TFM_ENDPOINT", "")
        self.api_key = os.getenv("NVIDIA_API_KEY", "")
        self._fallback = MockBehavioralAdapter()

    def score(self, player_id: str, event: dict[str, Any], features: dict[str, float],
              event_id: str, correlation_id: str) -> BehavioralScore:
        if not self.endpoint or not self.api_key:
            fb = self._fallback.score(player_id, event, features, event_id, correlation_id)
            fb.adapter = "nvidia-fallback-mock"
            fb.explanation["note"] = "NVIDIA endpoint not configured; using mock fallback"
            return fb
        # Placeholder HTTP call — real TFM wiring is environment-specific
        try:
            import urllib.request

            payload = {
                "player_id": player_id,
                "event": event,
                "features": features,
            }
            req = urllib.request.Request(
                self.endpoint,
                data=__import__("json").dumps(payload).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = __import__("json").loads(resp.read().decode())
            return BehavioralScore(
                event_id=event_id,
                player_id=player_id,
                correlation_id=correlation_id,
                timestamp=datetime.now(timezone.utc),
                adapter=self.name,
                score=float(data.get("score", 0.5)),
                anomaly_flags=list(data.get("flags", [])),
                explanation={"method": "NVIDIA TFM", "raw": data},
            )
        except Exception as exc:  # noqa: BLE001
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
