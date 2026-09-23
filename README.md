# Casino Sentinel

One command: PostgreSQL → Debezium → AMQ Streams → Streams Console → AML pipeline (joblib XGBoost) → Casino Guard UI.

```bash
cp env.example env          # set OCP_API and OCP_TOKEN; do not commit env
chmod +x deploy.sh destroy.sh
./deploy.sh --env ./env --insecure
```

XGBoost runs **inside** `sentinel-python` from `sentinel/artifacts/aml_xgboost.joblib`. There is no OpenShift AI / `--rhoai` path.

## Files `deploy.sh` uses

### Scripts

| File | Role |
|---|---|
| `deploy.sh` | Install |
| `destroy.sh` | Tear down |
| `env.example` | Copy to `env` |

### SQL

| File | Object |
|---|---|
| `sql/postgresql-cfg/logical.conf` | ConfigMap `casino-db-cfg` |
| `sql/01-init.sql` | Schema + CDC publication |
| `sql/02-seed.sql` | Demo players |

### Manifests (order)

| Step | File | Creates |
|---|---|---|
| 1 | `manifests/01-postgres/postgres.yaml` | `casino-db` |
| 2 | `manifests/02-kafka/kafka.yaml` | `sentinel-kafka` (KRaft 4.2) |
| 3 | `manifests/05-builds/python.yaml` | BuildConfig `sentinel-python` |
| 4 | `manifests/05-builds/ui.yaml` | BuildConfig `sentinel-ui` |
| 5 | `manifests/02-kafka/kafka-user-console.yaml` | `console-user` (SCRAM) |
| 6 | `manifests/03-console/console.yaml` | Streams Console |
| 7 | `manifests/03-topics/topics.yaml` | CDC + AML topics |
| 8 | `manifests/04-debezium/imagestream.yaml` | `sentinel-debezium-connect` |
| 9 | `manifests/04-debezium/kafka-connect.yaml` | `sentinel-connect` |
| 10 | `manifests/06-apps/services.yaml` | pipeline, simulator, API |
| 11 | `manifests/06-apps/ui.yaml` | UI + Route |
| 12 | `manifests/04-debezium/postgres-connector.yaml` | Debezium connector |

`--install-operators` also applies `manifests/01-operators/*.yaml`.

### Images built from

| Directory | Image | Runs as |
|---|---|---|
| `services/python/` | `sentinel-python:latest` | `sentinel-pipeline`, `sentinel-api`, `sentinel-simulator` |
| `ui/` | `sentinel-ui:latest` | `sentinel-ui` |

Joblib is baked into `sentinel-python` at steps 3–4 and used by `sentinel-pipeline` at step 10.

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
