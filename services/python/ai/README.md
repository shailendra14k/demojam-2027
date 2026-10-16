# Casino Sentinel AI adapters

This package provides a pluggable **BehavioralModelAdapter**:

| Mode | Class | GPU required | Notes |
|------|-------|--------------|-------|
| `mock` | `MockBehavioralAdapter` | No | Deterministic heuristics for demos |
| `cpu` | `CpuBehavioralAdapter` | No | **Deprecated** — maps to mock (Isolation Forest removed) |
| `nvidia` / `gpu` | `NvidiaBehavioralAdapter` | Optional | Uses `sentinel.tfm` with `TFM_MODE=http` + `TFM_EMBED_URL` (GPU `tfm-embed`); falls back to mock if unset |

Set `AI_MODE=mock|nvidia` via ConfigMap `casino-config` (patched by `./deploy.sh --rhoai`). Do **not** put these in `.env` — `.env` is only cluster login. For GPU cutover, `--rhoai` sets `TFM_MODE=http` and `TFM_EMBED_URL` (see `nvidia-tfm/OCP-1GPU.md`).

Tabular ML is XGBoost (`aml_xgb.joblib`). Behavioral scoring is NVIDIA TFM when `AI_MODE=nvidia`.
