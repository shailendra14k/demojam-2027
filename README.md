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


| Model / signal       | Role                                           | How                                                                                         |
| -------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **Rules**            | Deterministic compliance (CTR, structuring, …) | `sentinel-aml-rules`                                                                        |
| **XGBoost**          | Supervised AML classifier                      | `model/artifacts/*.joblib` via `sentinel.aml_model` in `sentinel-ml-engine` (CPU)           |
| **Isolation Forest** | Unsupervised anomaly                           | Same joblib artifacts; blended into ML / behavioral path                                    |
| **NVIDIA TFM**       | 512-d transaction embeddings                   | Default `TFM_MODE=hashed`; GPU: OpenShift AI InferenceService + `model/serve_embeddings.py` |


Official TFM blueprint: [NVIDIA-AI-Blueprints/transaction-foundation-model](https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model) — clone/sync with `../nvidia-tfm/clone-nvidia-tfm.sh`. See `[../nvidia-tfm/OCP-1GPU.md](../nvidia-tfm/OCP-1GPU.md)`.


| Env                       | Use                                    |
| ------------------------- | -------------------------------------- |
| `TFM_MODE=hashed`         | Default CPU stand-in embedding         |
| `TFM_MODE=http`           | GPU InferenceService (`TFM_EMBED_URL`) |
| `AI_MODE=mock|cpu|nvidia` | Behavioral adapter mode (ml-engine)    |




## Fresh deploy (CPU default)

```bash
cd demojam-2027
cp env.example .env         # set OCP_API and OCP_TOKEN; do not commit .env
# Optional: retrain before image build
#   cd model && python train_models.py && cd ..
chmod +x deploy.sh destroy.sh demo.sh smoke-test.sh status.sh
./deploy.sh --env ./.env --insecure
# If AMQ Streams / Console operators are not on the cluster yet:
# ./deploy.sh --env ./.env --insecure --install-operators
```

Requires `model/artifacts/*.joblib` (checked in or produced by `model/train_models.py`) — baked into `sentinel-python`.

Defaults: `AI_MODE=mock`, `TFM_MODE=hashed` (no GPU).

## Fresh deploy with NVIDIA TFM (1 GPU / OpenShift AI)

Laptop prep (once) — see `[../nvidia-tfm/OCP-1GPU.md](../nvidia-tfm/OCP-1GPU.md)`:

```bash
# from demoJam/ — clones/updates nvidia-tfm/upstream/ (pristine), COPIES checkpoint into demojam-2027
brew install git-lfs && git lfs install   # if needed
./nvidia-tfm/clone-nvidia-tfm.sh         # → demojam-2027/model/decoder-foundation-model/
# New NVIDIA release later? Re-run clone script (Step 1b in OCP-1GPU.md), then --rhoai again.
```

Cluster needs: GPU Operator, OpenShift AI single-model serving (KServe), NGC key.

```bash
cd demojam-2027
# .env must include NGC_API_KEY=nvapi-...  (creates ngc-secret)
./deploy.sh --env ./.env --insecure --rhoai --install-operators
```

`--rhoai` first checks OpenShift AI (DSC `kserve=Managed`, waits for ServingRuntime/InferenceService CRDs), frees the single GPU (stops other InferenceServices such as `llama-32-3b-instruct`, scales down other GPU Deployments), then creates PVC `tfm-checkpoint`, loads the checkpoint, mounts `model/serve_embeddings.py`, applies `manifests/08-openshift-ai/*`, and sets `TFM_MODE=http` / `AI_MODE=nvidia` on `casino-config`. Set `SKIP_FREE_GPU=true` in `.env` to leave other GPU models running.


| Path                              | Role                                                  |
| --------------------------------- | ----------------------------------------------------- |
| `model/artifacts/*.joblib`        | XGBoost / IsolationForest (CPU, in `sentinel-python`) |
| `model/decoder-foundation-model/` | NVIDIA TFM weights (PVC; not baked into python image) |
| `model/serve_embeddings.py`       | Embedding HTTP server for InferenceService            |




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


| File                                      | Role                                                            |
| ----------------------------------------- | --------------------------------------------------------------- |
| `deploy.sh`                               | Install                                                         |
| `destroy.sh`                              | Tear down                                                       |
| `env.example`                             | Copy to `.env` (`OCP_API`, `OCP_TOKEN`, optional `NGC_API_KEY`) |
| `demo.sh` / `smoke-test.sh` / `status.sh` | Post-deploy only (see above)                                    |




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
| 3    | `manifests/05-builds/python.yaml`         | BuildConfig `sentinel-python`                                         |
| 4    | `manifests/05-builds/ui.yaml`             | BuildConfig `sentinel-ui`                                             |
| 5    | `manifests/02-kafka/kafka-user-*.yaml`    | console + services users                                              |
| 6    | `manifests/03-console/console.yaml`       | Streams Console                                                       |
| 7    | `manifests/03-topics/topics.yaml`         | CDC + pipeline topics                                                 |
| 8–9  | `manifests/04-debezium/*`                 | Connect + connector                                                   |
| 10   | `manifests/06-apps/services.yaml`         | microservices + API + simulator                                       |
| 11   | `manifests/06-apps/ui.yaml`               | UI + Routes                                                           |
| 12   | `manifests/07-monitoring/monitoring.yaml` | Grafana stub                                                          |
| 13   | `manifests/08-openshift-ai/*`             | **With** `--rhoai`**:** ServingRuntime + InferenceService `tfm-embed` |




### Images


| Directory                                                        | Image                        | Runs as                   |
| ---------------------------------------------------------------- | ---------------------------- | ------------------------- |
| repo root (`services/python/Containerfile` + `model/artifacts/`) | `sentinel-python:latest`     | all python services + API |
| `ui/` (`ui/Containerfile`)                                       | `sentinel-ui:latest`         | `sentinel-ui`             |
| NeMo NGC (via ServingRuntime)                                    | InferenceService `tfm-embed` | TFM embeddings (1 GPU)    |




## Flags


| Flag                  | Effect                                                                                               |
| --------------------- | ---------------------------------------------------------------------------------------------------- |
| `--install-operators` | OLM Subscriptions for AMQ Streams + Console                                                          |
| `--skip-console`      | Skip Streams Console                                                                                 |
| `--skip-builds`       | Reuse existing ImageStreams                                                                          |
| `--rhoai`             | Free GPU → TFM on OpenShift AI (PVC + ngc-secret + `08-openshift-ai/*`); needs clone + `NGC_API_KEY` |
| `--insecure`          | Skip API TLS verify                                                                                  |
| `--dry-run`           | Print only                                                                                           |


```bash
./destroy.sh --env ./.env --insecure
./destroy.sh --env ./.env --insecure --delete-namespace
```

`casino-sentinel-merged` (B) was absorbed and deleted; see `MERGE_NOTES.md`.