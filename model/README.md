# AML tabular models — train offline only

Feature schema matches live ``feature-engine`` flatten() columns
(``w5m_*``, ``w24h_*``, ``static_*``). Scoring on OpenShift uses
``sentinel.aml_model`` (load/predict) — this folder is never imported at runtime.

Behavioral scoring uses **NVIDIA TFM** (`AI_MODE=nvidia`), not Isolation Forest.

```bash
cd demojam-2027/model
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python train_models.py
python train_models.py --csv artifacts/aml_train_synthetic.csv
```

Then rebuild ``sentinel-python`` so ``aml_xgb.joblib`` is baked into the image.

TFM embedding image (GPU / ``--rhoai``): sources live under
``decoder-foundation-model/`` (gitignored — Containerfile, serve_embeddings.py, weights).
Default deploy image is ``quay.io/shailendra14k/casino-tfm-embed:latest``.
Rebuild (opt-in; not part of default ``build-push-images.sh``):
``../scripts/build-push-images.sh --tfm`` → ``:latest``.

| File | Role |
|---|---|
| `train_models.py` | Fit XGBoost (offline) |
| `requirements.txt` | Train deps |
| `artifacts/aml_xgb.joblib` | Classifier payload `{model, feature_names, kind}` |
| `artifacts/meta.json` | Metrics + feature list |
| `decoder-foundation-model/` | Local TFM bake context (gitignored) |
