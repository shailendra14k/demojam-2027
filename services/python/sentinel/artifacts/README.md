Trained CPU artifacts (do not edit by hand). Retrain with:

    python -m sentinel.train_models

- `aml_xgboost.joblib` — XGBoost `XGBClassifier` (`tree_method=hist`, CPU)
- `aml_gbt.joblib` — same payload (compat alias)
- `aml_iforest.joblib` — Isolation Forest
- `meta.json` — ROC/PR and top features
