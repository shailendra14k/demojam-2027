#!/usr/bin/env python3
"""Embedding HTTP server for the NVIDIA Transaction Foundation Model.

Used by OpenShift AI InferenceService (ConfigMap tfm-embed-script) via
`demojam-2027/deploy.sh --rhoai`. Checkpoint lives beside this file:

  model/decoder-foundation-model/   # from nvidia-tfm/clone-nvidia-tfm.sh

  PORT=8080 TFM_CHECKPOINT=/mnt/models/decoder-foundation-model \\
    python serve_embeddings.py

Casino Sentinel consumes via TFM_MODE=http + TFM_EMBED_URL=.../v1/embeddings.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np
import uvicorn

CHECKPOINT = os.environ.get(
    "TFM_CHECKPOINT",
    str(Path(__file__).resolve().parent / "decoder-foundation-model"),
)
PAD_ID = int(os.environ.get("TFM_PAD_ID", "0"))

app = FastAPI(title="NVIDIA TFM embeddings", version="1.0.0")
_bundle = None


class EmbedRequest(BaseModel):
    input_ids: list[list[int]]
    pooling: str = "last_token"


def _load():
    global _bundle
    if _bundle is not None:
        return _bundle
    import torch
    from transformers import AutoModelForCausalLM

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = AutoModelForCausalLM.from_pretrained(CHECKPOINT)
    model.to(device)
    model.eval()
    _bundle = (model, device)
    return _bundle


@app.get("/healthz")
def healthz():
    return {"status": "ok", "checkpoint": CHECKPOINT}


@app.post("/v1/embeddings")
def embeddings(body: EmbedRequest):
    import torch

    model, device = _load()
    rows = []
    for seq in body.input_ids:
        ids = torch.tensor([seq], dtype=torch.long, device=device)
        mask = (ids != PAD_ID).long()
        with torch.no_grad():
            out = model(input_ids=ids, attention_mask=mask, output_hidden_states=True)
            hidden = out.hidden_states[-1]
            if body.pooling == "mean":
                m = mask.unsqueeze(-1).float()
                vec = (hidden * m).sum(dim=1) / m.sum(dim=1).clamp(min=1)
            else:
                last = int(mask.sum().item()) - 1
                last = max(0, last)
                vec = hidden[0, last, :]
        arr = vec.squeeze().float().cpu().numpy()
        n = float(np.linalg.norm(arr)) + 1e-9
        rows.append((arr / n).tolist())
    return {"embeddings": rows, "dim": len(rows[0]) if rows else 512, "pooling": body.pooling}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8090")))
