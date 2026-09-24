# Casino Sentinel AI adapters

This package provides a pluggable **BehavioralModelAdapter**:

| Mode | Class | GPU required | Notes |
|------|-------|--------------|-------|
| `mock` | `MockBehavioralAdapter` | No | Deterministic heuristics for demos |
| `cpu` | `CpuBehavioralAdapter` | No | sklearn IsolationForest |
| `nvidia` / `gpu` | `NvidiaBehavioralAdapter` | Optional | Calls `NVIDIA_TFM_ENDPOINT` if set; otherwise falls back to mock |

Set `AI_MODE=mock|cpu|nvidia` via ConfigMap / `.env`.

NVIDIA Transaction Foundation Model integration is **optional**. The PoC is fully functional without it.
