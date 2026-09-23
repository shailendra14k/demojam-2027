"""Score via OpenShift AI (KServe V2 / OVMS REST). Fallback is local joblib."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

import numpy as np

log = logging.getLogger("sentinel.onnx_infer")


def predict_proba(url: str, vec: list[float], timeout: float = 3.0) -> np.ndarray | None:
    """Return [p_legit, p_aml] or None on failure."""
    url = (url or "").strip()
    if not url:
        return None
    x = np.asarray(vec, dtype=np.float32).reshape(1, -1)
    try:
        if "/v2/" in url or url.rstrip("/").endswith("/infer"):
            return _v2(url, x, timeout)
        return _ovms_v1(url, x, timeout)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc:
        log.warning("ONNX infer at %s failed: %s", url, exc)
        return None


def _v2(url: str, x: np.ndarray, timeout: float) -> np.ndarray:
    body = {
        "inputs": [
            {
                "name": "input",
                "shape": list(x.shape),
                "datatype": "FP32",
                "data": x.reshape(-1).tolist(),
            }
        ]
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    data = payload["outputs"][0]["data"]
    shape = payload["outputs"][0].get("shape") or [1, 2]
    return np.asarray(data, dtype=np.float32).reshape(shape)[0]


def _ovms_v1(url: str, x: np.ndarray, timeout: float) -> np.ndarray:
    req = urllib.request.Request(
        url,
        data=json.dumps({"inputs": x.tolist()}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    arr = payload.get("outputs") or payload.get("predictions") or payload
    return np.asarray(arr, dtype=np.float32).reshape(-1, 2)[0]
