# Casino Sentinel AI adapters

This package provides a pluggable **BehavioralModelAdapter**:

| Mode | Class | GPU required | Notes |
|------|-------|--------------|-------|
| `mock` | `MockBehavioralAdapter` | No | Deterministic heuristics for demos |
| `cpu` | `CpuBehavioralAdapter` | No | sklearn IsolationForest |
| `nvidia` / `gpu` | `NvidiaBehavioralAdapter` | Optional | Uses `sentinel.tfm` with `TFM_MODE=http` + `TFM_EMBED_URL` (GPU `tfm-embed`); falls back to mock if unset |

Set `AI_MODE=mock|cpu|nvidia` via ConfigMap / `.env`. For GPU cutover also set `TFM_MODE=http` and `TFM_EMBED_URL` (see `nvidia-tfm/OCP-1GPU.md`).

NVIDIA Transaction Foundation Model integration is **optional**. The PoC is fully functional without it.
