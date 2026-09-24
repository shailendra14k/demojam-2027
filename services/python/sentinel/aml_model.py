"""Online AML scoring — load joblib artifacts and predict (no training).

Artifacts are produced offline by ``model/train_models.py`` and baked into the
``sentinel-python`` image under ``model/artifacts/``.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from pathlib import Path
from typing import Any, Optional

import joblib
import numpy as np

from .feature_schema import FEATURE_COLUMNS

log = logging.getLogger("sentinel.aml_model")


def _default_model_dir() -> Path:
    env = os.getenv("MODEL_DIR")
    if env:
        return Path(env)
    here = Path(__file__).resolve().parent
    # Container: /opt/app-root/src/sentinel → ../model/artifacts
    # Repo:      .../services/python/sentinel → ../../../model/artifacts
    for cand in (
        here.parent / "model" / "artifacts",
        here.parents[2] / "model" / "artifacts",
    ):
        if (cand / "aml_xgb.joblib").exists() or (cand / "aml_xgboost.joblib").exists():
            return cand
    return here.parent / "model" / "artifacts"


class AMLModel:
    """Load + predict only. Training lives in ``model/train_models.py``."""

    def __init__(self, model_dir: Optional[Path] = None) -> None:
        self.model_dir = Path(model_dir or _default_model_dir())
        self.model = None
        self.iforest = None
        self.feature_names = list(FEATURE_COLUMNS)
        self.version = "2.0.0"
        self.name = "casino-aml-xgb"
        self.infer_url = os.getenv("XGB_INFER_URL", "").strip()

    def _xgb_candidates(self) -> list[Path]:
        return [
            self.model_dir / "aml_xgb.joblib",
            self.model_dir / "aml_xgboost.joblib",
            self.model_dir / "aml_xgb.pkl",
        ]

    def _iforest_path(self) -> Path:
        return self.model_dir / "aml_iforest.joblib"

    def _meta_path(self) -> Path:
        for name in ("meta.json", "aml_xgb_meta.json"):
            p = self.model_dir / name
            if p.exists():
                return p
        return self.model_dir / "meta.json"

    def load(self) -> None:
        path = next((p for p in self._xgb_candidates() if p.exists()), None)
        if path is None:
            raise FileNotFoundError(
                f"No XGBoost artifact in {self.model_dir}. "
                "Run: cd model && python train_models.py"
            )
        raw = joblib.load(path)
        if isinstance(raw, dict) and "model" in raw:
            self.model = raw["model"]
            names = raw.get("feature_names") or raw.get("features")
            if names:
                self.feature_names = list(names)
        else:
            self.model = raw

        ifp = self._iforest_path()
        if ifp.exists():
            self.iforest = joblib.load(ifp)

        meta_path = self._meta_path()
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                self.version = str(meta.get("version") or meta.get("model") or self.version)
                if meta.get("feature_names"):
                    self.feature_names = list(meta["feature_names"])
            except Exception as exc:  # noqa: BLE001
                log.debug("meta read failed: %s", exc)

        log.info("loaded AML model from %s (features=%d)", path, len(self.feature_names))

    def vectorize(self, flat_features: dict[str, float]) -> np.ndarray:
        return np.array(
            [[float(flat_features.get(c, 0.0)) for c in self.feature_names]],
            dtype=float,
        )

    def _predict_remote(self, flat_features: dict[str, float]) -> Optional[dict[str, Any]]:
        if not self.infer_url:
            return None
        try:
            body = {
                "inputs": [
                    {
                        "name": "input",
                        "shape": [1, len(self.feature_names)],
                        "datatype": "FP32",
                        "data": self.vectorize(flat_features)[0].tolist(),
                    }
                ]
            }
            req = urllib.request.Request(
                self.infer_url,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            outs = data.get("outputs") or data.get("predictions") or []
            if outs and isinstance(outs[0], dict) and "data" in outs[0]:
                arr = outs[0]["data"]
                proba = float(arr[-1] if len(arr) > 1 else arr[0])
            elif outs and isinstance(outs[0], (int, float)):
                proba = float(outs[0])
            else:
                return None
            return {
                "score": proba,
                "label": "SUSPICIOUS" if proba >= 0.5 else "BENIGN",
                "model_name": self.name + "-remote",
                "model_version": self.version,
                "features_used": self.feature_names,
                "shap_top": [],
                "source": "xgb_infer_url",
            }
        except Exception as exc:  # noqa: BLE001
            log.warning("XGB_INFER_URL failed (%s); falling back to local model", exc)
            return None

    def predict(self, flat_features: dict[str, float]) -> dict[str, Any]:
        remote = self._predict_remote(flat_features)
        if remote is not None:
            return remote
        if self.model is None:
            self.load()
        assert self.model is not None
        x = self.vectorize(flat_features)
        proba = float(self.model.predict_proba(x)[0, 1])
        label = "SUSPICIOUS" if proba >= 0.5 else "BENIGN"
        importances = getattr(self.model, "feature_importances_", np.ones(len(self.feature_names)))
        contrib = []
        for i, col in enumerate(self.feature_names):
            contrib.append(
                {
                    "feature": col,
                    "value": float(x[0, i]),
                    "importance": float(importances[i]) if i < len(importances) else 0.0,
                    "contribution": float(importances[i] * abs(x[0, i])) if i < len(importances) else 0.0,
                }
            )
        contrib.sort(key=lambda c: c["contribution"], reverse=True)
        out: dict[str, Any] = {
            "score": proba,
            "label": label,
            "model_name": self.name,
            "model_version": self.version,
            "features_used": self.feature_names,
            "shap_top": contrib[:5],
            "source": "local",
        }
        if self.iforest is not None:
            # decision_function: higher = more normal; map to 0..1 anomaly-ish score
            raw = float(self.iforest.decision_function(x)[0])
            out["iforest_score"] = float(1.0 / (1.0 + np.exp(raw)))
        return out
