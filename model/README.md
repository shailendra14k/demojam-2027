# AML tabular models — train offline only

Feature schema matches live ``feature-engine`` flatten() columns
(``w5m_*``, ``w24h_*``, ``static_*``). Scoring on OpenShift uses
``sentinel.aml_model`` (load/predict) — this folder is never imported at runtime.

```bash
cd Casino-useCase/casino-sentinel/model
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python train_models.py
python train_models.py --csv /path/to/aml_train.csv
```

Then rebuild ``sentinel-python`` so artifacts are baked into the image.

| File | Role |
|---|---|
| `train_models.py` | Fit XGBoost + Isolation Forest (offline) |
| `requirements.txt` | Train deps |
| `artifacts/aml_xgb.joblib` | Classifier payload `{model, feature_names}` |
| `artifacts/aml_iforest.joblib` | Isolation Forest |
| `artifacts/meta.json` | Metrics + feature list |
