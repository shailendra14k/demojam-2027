# Casino Sentinel

Microservice AML demo on OpenShift: PostgreSQL → Debezium → AMQ Streams → streaming services → investigator UI.

## Architecture (what this repo implements)

```
Casino systems  →  PostgreSQL (casino-db, CDC tables)
                      │
                      ▼
              Debezium (sentinel-connect)
                      │
                      ▼
              AMQ Streams (sentinel-kafka)
                casino.cdc.*
                      │
                      ▼
         Pipeline (normalize → features)
                casino.events.normalized
                aml.features
                      │
        ┌─────────────┼──────────────┬──────────────┐
        ▼             ▼              ▼              ▼
   Rules engine   XGBoost         NVIDIA TFM     KYC score
   sentinel-      sentinel-       512-d embed    (risk engine)
   aml-rules      ml-engine       TFM_MODE=http     │
   (45%)          joblib CPU      / hashed          │
                  (35%)           (20% behavioral)  (10%)
                      │
                      ▼
           Risk aggregation 0–100  (sentinel-risk-engine)
           Low / Medium / High / Critical
           + human-readable explanation
                      │
                      ▼
           AML alerts + cases  (sentinel-alert-service → Postgres)
                      │
                      ▼
           Casino Guard React UI  (sentinel-api + sentinel-ui)
```

Score weights (ConfigMap `casino-config`): **Rules 45% · XGBoost (ML) 35% · TFM / behavioral 20% · KYC 10%**.

With `./deploy.sh --rhoai`, `sentinel-ml-engine` tokenizes on CPU and calls the GPU InferenceService  
`http://tfm-embed-predictor.<ns>.svc:8080/v1/embeddings` (`TFM_MODE=http`, `AI_MODE=nvidia`).


| Model / signal | Role                                           | How                                                                                         |
| -------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **Rules**      | Deterministic compliance (CTR, structuring, …) | `sentinel-aml-rules`                                                                        |
| **XGBoost**    | Supervised AML classifier                      | `model/artifacts/*.joblib` via `sentinel.aml_model` in `sentinel-ml-engine` (CPU)           |
| **NVIDIA TFM** | Behavioral sequence embeddings                 | `AI_MODE=nvidia` + `TFM_MODE=http` (GPU); Isolation Forest removed                          |
| **NVIDIA TFM** | 512-d transaction embeddings                   | Default `TFM_MODE=hashed`; GPU: OpenShift AI + `casino-tfm-embed:latest` |


Official TFM blueprint: [NVIDIA-AI-Blueprints/transaction-foundation-model](https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model) —


| Env               | Use                                    |
| ----------------- | -------------------------------------- |
| `TFM_MODE=hashed` | Default CPU stand-in embedding         |
| `TFM_MODE=http`   | GPU InferenceService (`TFM_EMBED_URL`) |
| `AI_MODE=mock     | cpu                                    |




## Build app images (Quay)

`sentinel-python` and `sentinel-ui` are **not** built on the cluster. Build them on a laptop/CI and push to Quay. The image tag is the **short git commit** of `demojam-2027/` (same tag `deploy.sh` pulls).

Prerequisites: `podman` or `docker`, Quay write access, `model/artifacts/*.joblib` present (checked in or from `model/train_models.py`).

```bash
cd demojam-2027

# 1. Log in to Quay
podman login quay.io
# or: docker login quay.io

# 2. Optional: retrain before baking XGBoost into the python image
# cd model && python train_models.py && cd ..

# 3. Build + push both images (linux/amd64)
chmod +x scripts/build-push-images.sh
./scripts/build-push-images.sh
```

Pushes:

| Image | Tag |
| ----- | --- |
| `quay.io/shailendra14k/sentinel-python` | `<short-sha>` (e.g. `3cd20ed`) |
| `quay.io/shailendra14k/sentinel-ui` | `<short-sha>` |
| `quay.io/shailendra14k/casino-tfm-embed` | only with `--tfm` (use `TAG=latest` for deploy default) |

Default `./scripts/build-push-images.sh` builds **python + ui only** (not TFM).

Useful variants:

```bash
./scripts/build-push-images.sh --python-only   # only sentinel-python
./scripts/build-push-images.sh --ui-only       # only sentinel-ui
TAG=latest ./scripts/build-push-images.sh --tfm       # also casino-tfm-embed (opt-in)
TAG=latest ./scripts/build-push-images.sh --tfm-only  # only casino-tfm-embed
TAG=mytag ./scripts/build-push-images.sh              # override python/ui tag
```

## Fresh deploy (CPU default)

```bash
cd demojam-2027
cp env.example .env         # set OCP_API and OCP_TOKEN; do not commit .env

# Build + push first (see above) if this commit’s images are not on Quay yet
./scripts/build-push-images.sh

chmod +x deploy.sh destroy.sh demo.sh
./deploy.sh --env ./.env --insecure
# If AMQ Streams / Console operators are not on the cluster yet:
# ./deploy.sh --env ./.env --insecure --install-operators
```

`deploy.sh` pulls `quay.io/shailendra14k/sentinel-{python,ui}:<short-sha>` for the current commit (override with `SENTINEL_PYTHON_IMAGE` / `SENTINEL_UI_IMAGE`).

Defaults: `AI_MODE=mock`, `TFM_MODE=hashed` (no GPU).

## Fresh deploy with NVIDIA TFM (1 GPU / OpenShift AI)

No TFM training for a normal `--rhoai` deploy. Default image:
`quay.io/shailendra14k/casino-tfm-embed:latest` (NeMo + script + weights; no PVC).

Rebuild is **opt-in** (not part of default `build-push-images.sh`). Sources live under
`model/decoder-foundation-model/` (gitignored: Containerfile, `serve_embeddings.py`, weights):

```bash
cd demojam-2027
TAG=latest ./scripts/build-push-images.sh --tfm   # or --tfm-only
```

Cluster needs: GPU Operator, OpenShift AI single-model serving (KServe).  
**NGC /** `NGC_API_KEY` **is not required**.

```bash
cd demojam-2027
# .env: OCP_API + OCP_TOKEN only
./deploy.sh --env ./.env --insecure --rhoai --install-operators
```

`--rhoai` checks OpenShift AI (DSC `kserve=Managed`, CRDs), frees the single GPU, applies
`manifests/08-openshift-ai/*` with `casino-tfm-embed:latest`, and sets `TFM_MODE=http` /
`AI_MODE=nvidia` on `casino-config`. Set `SKIP_FREE_GPU=true` in `.env` to leave other GPU models running.


| Path                              | Role                                                                 |
| --------------------------------- | -------------------------------------------------------------------- |
| `model/artifacts/aml_xgb.joblib`  | XGBoost (CPU, in `sentinel-python`); TFM is separate                 |
| `model/decoder-foundation-model/` | Local TFM bake dir (gitignored): Containerfile, script, weights      |




## After deploy (optional helpers)

These are **not** called by `deploy.sh`. Use them once the cluster is up (`oc` logged in).


| Script                             | Use                                                                               |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `./status.sh`                      | Quick cluster check: pods, Kafka/Connect, routes, API `/health` + pipeline status |
| `./smoke-test.sh`                  | E2E: API health → inject scenario → wait/poll until an OPEN alert appears         |
| `./demo.sh --scenario structuring` | Same inject path as smoke, tuned for live demos (polls alerts, prints summary)    |


```bash
./status.sh
./smoke-test.sh
./demo.sh --scenario structuring          # or rapid_wire / watchlist / …
./demo.sh --scenario structuring --seed 42   # reproducible (idempotent after first run)
```

UI path: open Casino Guard → Demo Controls → Start Scenario (same scenarios).

## Files `deploy.sh` uses



### Scripts


| File                                      | Role                                                                               |
| ----------------------------------------- | ---------------------------------------------------------------------------------- |
| `deploy.sh`                               | Install                                                                            |
| `destroy.sh`                              | Tear down                                                                          |
| `env.example`                             | Copy to `.env` (`OCP_API`, `OCP_TOKEN`; `NGC_API_KEY` not required — NeMo on Quay) |
| `demo.sh` / `smoke-test.sh` / `status.sh` | Post-deploy only (see above)                                                       |




### SQL


| File                              | Role                                                  |
| --------------------------------- | ----------------------------------------------------- |
| `sql/postgresql-cfg/logical.conf` | ConfigMap `casino-db-cfg`                             |
| `sql/01-init.sql`                 | Schema + CDC publication                              |
| `sql/02-seed.sql`                 | Demo players                                          |
| `sql/scenarios/`                  | Demo scenario payloads (also baked into python image) |




### Manifests (order)


| Step | File                                      | Creates                                                               |
| ---- | ----------------------------------------- | --------------------------------------------------------------------- |
| 0    | `manifests/00-base/serviceaccount.yaml`   | SA                                                                    |
| 0b   | `manifests/00-base/config.yaml`           | `casino-config`                                                       |
| 0c   | `manifests/07-security/network-rbac.yaml` | NetPol + Role                                                         |
| 1    | `manifests/01-postgres/postgres.yaml`     | `casino-db`                                                           |
| 2    | `manifests/02-kafka/kafka.yaml`           | `sentinel-kafka` (KRaft 4.2)                                          |
| 3    | `manifests/02-kafka/kafka-user-*.yaml`    | console + services users                                              |
| 4    | `manifests/03-console/console.yaml`       | Streams Console                                                       |
| 5    | `manifests/03-topics/topics.yaml`         | CDC + pipeline topics                                                 |
| 6–7  | `manifests/04-debezium/*`                 | Connect + connector                                                   |
| 8    | `manifests/06-apps/services.yaml`         | microservices + API + simulator (Quay `sentinel-python:<sha>`)        |
| 9    | `manifests/06-apps/ui.yaml`               | UI + Routes (Quay `sentinel-ui:<sha>`)                                |
| 10   | `manifests/07-monitoring/monitoring.yaml` | Grafana stub                                                          |
| 11   | `manifests/08-openshift-ai/*`             | **With** `--rhoai`**:** ServingRuntime + InferenceService `tfm-embed` |




### Images


| Directory                                                        | Image                                                      | Runs as                            |
| ---------------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------- |
| repo root (`services/python/Containerfile` + `model/artifacts/`) | `quay.io/shailendra14k/sentinel-python:<short-sha>`        | all python services + API          |
| `ui/` (`ui/Containerfile`)                                       | `quay.io/shailendra14k/sentinel-ui:<short-sha>`            | `sentinel-ui`                      |
| `model/decoder-foundation-model/` (gitignored bake context)      | `quay.io/shailendra14k/casino-tfm-embed:latest`            | InferenceService `tfm-embed` (GPU) |

See **Build app images (Quay)** above (`--tfm` is opt-in for casino-tfm-embed).

## Flags


| Flag                  | Effect                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| `--install-operators` | OLM Subscriptions for AMQ Streams + Console                                                               |
| `--skip-console`      | Skip Streams Console                                                                                      |
| `--rhoai`             | Free GPU → TFM on OpenShift AI (`casino-tfm-embed` + `08-openshift-ai/*`); **no** `NGC_API_KEY` / PVC     |
| `--insecure`          | Skip API TLS verify                                                                                       |
| `--dry-run`           | Print only                                                                                                |


```bash
./destroy.sh --env ./.env --insecure
./destroy.sh --env ./.env --insecure --delete-namespace
```

`casino-sentinel-merged` (B) was absorbed and deleted; see `MERGE_NOTES.md`.