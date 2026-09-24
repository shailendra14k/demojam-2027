# Casino Sentinel

Microservice AML demo on OpenShift: PostgreSQL → Debezium → AMQ Streams → streaming services → investigator UI.

## Fresh deploy

```bash
cd casino-sentinel
cp env.example env          # set OCP_API and OCP_TOKEN; do not commit env
# Optional: retrain before image build
#   cd model && python train_models.py && cd ..
chmod +x deploy.sh destroy.sh demo.sh smoke-test.sh status.sh
./deploy.sh --env ./env --insecure
# If AMQ Streams / Console operators are not on the cluster yet:
# ./deploy.sh --env ./env --insecure --install-operators
```

Requires `model/artifacts/*.joblib` (checked in or produced by `model/train_models.py`) — baked into `sentinel-python`.

## Architecture

```
Postgres (casino-db)
  → Debezium (sentinel-connect)
  → Kafka (sentinel-kafka)
  → sentinel-event-normalizer
  → sentinel-feature-engine
  → sentinel-aml-rules + sentinel-ml-engine (+ TFM / AI_MODE)
  → sentinel-risk-engine
  → sentinel-alert-service
  → sentinel-api + sentinel-ui
```

XGBoost / IsolationForest: train offline in `model/` (`python train_models.py`), bake `model/artifacts/*.joblib` into `sentinel-python`. Runtime scoring is `sentinel.aml_model` (load/predict only).  
NVIDIA TFM: set `TFM_MODE` / `TFM_EMBED_URL` on ConfigMap `casino-config` (see `nvidia-tfm/OCP-1GPU.md`).

| Env | Use |
|---|---|
| `TFM_MODE=hashed` | Default CPU stand-in embedding |
| `TFM_MODE=http` | GPU infer service (`TFM_EMBED_URL`) |
| `AI_MODE=mock\|cpu\|nvidia` | Behavioral adapter mode (ml-engine) |

## After deploy (optional helpers)

These are **not** called by `deploy.sh`. Use them once the cluster is up (`oc` logged in).

| Script | Use |
|---|---|
| `./status.sh` | Quick cluster check: pods, Kafka/Connect, routes, API `/health` + pipeline status |
| `./smoke-test.sh` | E2E: API health → inject scenario → wait/poll until an OPEN alert appears |
| `./demo.sh --scenario structuring` | Same inject path as smoke, tuned for live demos (polls alerts, prints summary) |

```bash
./status.sh
./smoke-test.sh
./demo.sh --scenario structuring          # or rapid_wire / watchlist / …
./demo.sh --scenario structuring --seed 42   # reproducible (idempotent after first run)
```

UI path: open Casino Guard → Demo Controls → Start Scenario (same scenarios).

## Files `deploy.sh` uses

### Scripts

| File | Role |
|---|---|
| `deploy.sh` | Install |
| `destroy.sh` | Tear down |
| `env.example` | Copy to `env` |
| `demo.sh` / `smoke-test.sh` / `status.sh` | Post-deploy only (see above) |

### SQL

| File | Role |
|---|---|
| `sql/postgresql-cfg/logical.conf` | ConfigMap `casino-db-cfg` |
| `sql/01-init.sql` | Schema + CDC publication |
| `sql/02-seed.sql` | Demo players |
| `sql/scenarios/` | Demo scenario payloads (also baked into python image) |

### Manifests (order)

| Step | File | Creates |
|---|---|---|
| 0 | `manifests/00-base/serviceaccount.yaml` | SA |
| 0b | `manifests/00-base/config.yaml` | `casino-config` |
| 0c | `manifests/07-security/network-rbac.yaml` | NetPol + Role |
| 1 | `manifests/01-postgres/postgres.yaml` | `casino-db` |
| 2 | `manifests/02-kafka/kafka.yaml` | `sentinel-kafka` (KRaft 4.2) |
| 3 | `manifests/05-builds/python.yaml` | BuildConfig `sentinel-python` |
| 4 | `manifests/05-builds/ui.yaml` | BuildConfig `sentinel-ui` |
| 5 | `manifests/02-kafka/kafka-user-*.yaml` | console + services users |
| 6 | `manifests/03-console/console.yaml` | Streams Console |
| 7 | `manifests/03-topics/topics.yaml` | CDC + pipeline topics |
| 8–9 | `manifests/04-debezium/*` | Connect + connector |
| 10 | `manifests/06-apps/services.yaml` | microservices + API + simulator |
| 11 | `manifests/06-apps/ui.yaml` | UI + Routes |
| 12 | `manifests/07-monitoring/monitoring.yaml` | Grafana stub |

### Images

| Directory | Image | Runs as |
|---|---|---|
| repo root (`services/python/Containerfile` + `model/artifacts/`) | `sentinel-python:latest` | all python services + API |
| `ui/` (`ui/Containerfile`) | `sentinel-ui:latest` | `sentinel-ui` |

## Flags

| Flag | Effect |
|---|---|
| `--install-operators` | OLM Subscriptions for AMQ Streams + Console |
| `--skip-console` | Skip Streams Console |
| `--skip-builds` | Reuse existing ImageStreams |
| `--insecure` | Skip API TLS verify |
| `--dry-run` | Print only |

```bash
./destroy.sh --env ./env --insecure
./destroy.sh --env ./env --insecure --delete-namespace
```

`casino-sentinel-merged` (B) was absorbed and deleted; see `MERGE_NOTES.md`.
