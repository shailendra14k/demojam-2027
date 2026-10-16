# Casino Sentinel

Microservice AML demo on OpenShift: PostgreSQL → Debezium → AMQ Streams → streaming services → investigator UI.

---

## Deploy

### Step 1 — Create the lab

Order the Red Hat OpenShift AI 3 demo lab:

[https://catalog.demo.redhat.com/catalog/babylon-catalog-prod?item=babylon-catalog-prod/published.openshift-ai-v3.prod&utm_source=webapp&utm_medium=share-link](https://catalog.demo.redhat.com/catalog/babylon-catalog-prod?item=babylon-catalog-prod/published.openshift-ai-v3.prod&utm_source=webapp&utm_medium=share-link)

When the lab is ready, copy the API URL and token.

### Step 2 — Deploy

```bash
git clone https://github.com/shailendra14k/demojam-2027.git
cd demojam-2027
chmod +x deploy.sh

./deploy.sh --token=sha256~... --server=https://api.<cluster>:6443
```

No TFM training. Inference uses `quay.io/shailendra14k/casino-tfm-embed:latest` (script + weights baked in)

---



## Destroy

```bash
./destroy.sh --token=sha256~... --server=https://api.<cluster>:6443
```

Defaults: insecure login, **delete namespace** `casino-sentinel`, and **uninstall** AMQ Streams + Streams Console operators.  
Opt out: `--keep-namespace`, `--keep-operators`, `--secure`.

---



## Architecture

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

By default (TFM on), `sentinel-ml-engine` tokenizes on CPU and calls the GPU InferenceService  
`http://tfm-embed-predictor.<ns>.svc:8080/v1/embeddings` (`TFM_MODE=http`, `AI_MODE=nvidia`).


| Model / signal | Role                                           | How                                                                               |
| -------------- | ---------------------------------------------- | --------------------------------------------------------------------------------- |
| **Rules**      | Deterministic compliance (CTR, structuring, …) | `sentinel-aml-rules`                                                              |
| **XGBoost**    | Supervised AML classifier                      | `model/artifacts/*.joblib` via `sentinel.aml_model` in `sentinel-ml-engine` (CPU) |
| **NVIDIA TFM** | 512-d transaction embeddings                   | Default `TFM_MODE=hashed` (CPU); GPU: `TFM_MODE=http` + `casino-tfm-embed:latest` |


Official TFM blueprint: [NVIDIA-AI-Blueprints/transaction-foundation-model](https://github.com/NVIDIA-AI-Blueprints/transaction-foundation-model).


| Env               | Use                                    |
| ----------------- | -------------------------------------- |
| `TFM_MODE=hashed` | Default CPU stand-in embedding         |
| `TFM_MODE=http`   | GPU InferenceService (`TFM_EMBED_URL`) |
| `AI_MODE=mock`    | Mock scoring (`--no-rhoai`)            |
| `AI_MODE=nvidia`  | Default demo; TFM HTTP embeddings      |


---



## Reference



### Image sources


| Directory                                                        | Image                                           | Runs as                            |
| ---------------------------------------------------------------- | ----------------------------------------------- | ---------------------------------- |
| repo root (`services/python/Containerfile` + `model/artifacts/`) | `quay.io/shailendra14k/sentinel-python:latest`  | all python services + API          |
| `ui/` (`ui/Containerfile`)                                       | `quay.io/shailendra14k/sentinel-ui:latest`      | `sentinel-ui`                      |
| `model/decoder-foundation-model/` (gitignored)                   | `quay.io/shailendra14k/casino-tfm-embed:latest` | InferenceService `tfm-embed` (GPU) |




### SQL


| File                              | Role                                                  |
| --------------------------------- | ----------------------------------------------------- |
| `sql/postgresql-cfg/logical.conf` | ConfigMap `casino-db-cfg`                             |
| `sql/01-init.sql`                 | Schema + CDC publication                              |
| `sql/02-seed.sql`                 | Demo players                                          |
| `sql/scenarios/`                  | Demo scenario payloads (also baked into python image) |




### Manifests (order)

Order matches `deploy.sh` (not directory sort). Optional steps noted.


| #   | When                                   | File                                                            | Creates                                               |
| --- | -------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| 1   | default (skip with `--skip-operators`) | `manifests/01-operators/amq-streams-subscription.yaml`          | AMQ Streams operator (OLM)                            |
| 2   | default (skip with `--skip-operators`) | `manifests/01-operators/console-operator-subscription.yaml`     | Streams Console operator (OLM)                        |
| 3   | always                                 | `manifests/00-base/serviceaccount.yaml`                         | ServiceAccount                                        |
| 4   | always                                 | `manifests/00-base/config.yaml`                                 | ConfigMap `casino-config`                             |
| 5   | always                                 | `manifests/07-security/network-rbac.yaml`                       | NetworkPolicy + Role                                  |
| 6   | always                                 | `manifests/01-postgres/postgres.yaml`                           | Deployment `casino-db`                                |
| —   | always (SQL, not a manifest)           | `sql/01-init.sql` + `sql/02-seed.sql`                           | Schema, CDC publication, seed                         |
| 7   | always                                 | `manifests/02-kafka/kafka.yaml`                                 | Kafka `sentinel-kafka` (KRaft)                        |
| 8   | always                                 | `manifests/02-kafka/kafka-user-console.yaml`                    | KafkaUser `console-user`                              |
| 9   | always                                 | `manifests/02-kafka/kafka-user-services.yaml`                   | KafkaUser `casino-services`                           |
| 10  | unless `--skip-console`                | `manifests/03-console/console.yaml`                             | Streams Console                                       |
| 11  | always                                 | `manifests/03-topics/topics.yaml`                               | CDC + pipeline topics                                 |
| 12  | always                                 | `manifests/04-debezium/imagestream.yaml`                        | ImageStream for Connect build                         |
| 13  | always                                 | `manifests/04-debezium/kafka-connect.yaml`                      | KafkaConnect `sentinel-connect`                       |
| 14  | always                                 | `manifests/06-apps/services.yaml`                               | Pipeline + API + simulator (`sentinel-python:latest`) |
| 15  | always                                 | `manifests/06-apps/ui.yaml`                                     | UI + Routes (`sentinel-ui:latest`)                    |
| 16  | always (best-effort)                   | `manifests/07-monitoring/monitoring.yaml`                       | Grafana stub                                          |
| 17  | after Connect Ready                    | `manifests/04-debezium/postgres-connector.yaml`                 | KafkaConnector (Postgres CDC)                         |
| 18  | default (skip with `--no-rhoai`)       | `manifests/08-openshift-ai/servingruntime-casino-tfm-nemo.yaml` | ServingRuntime `casino-tfm-nemo`                      |
| 19  | default (skip with `--no-rhoai`)       | `manifests/08-openshift-ai/inferenceservice-tfm-embed.yaml`     | InferenceService `tfm-embed`                          |


Notes:

- Debezium **connector** (step 17) is applied **after** apps, once Kafka Connect is Ready — not with the Connect CR.
- App images come from Quay (`:latest`); there is no `manifests/05-builds/` for python/ui.

---



## [OPTIONAL] Build app images (Quay)

Only when you change application or UI code.  
Prerequisites: `podman` or `docker`, Quay write access, `model/artifacts/*.joblib` present.

```bash
cd demojam-2027

podman login quay.io          # or: docker login quay.io

# Optional: retrain before baking XGBoost into the python image
# cd model && python train_models.py && cd ..

chmod +x scripts/build-push-images.sh
./scripts/build-push-images.sh          # → sentinel-python:latest + sentinel-ui:latest
./scripts/build-push-images.sh --tfm    # also casino-tfm-embed:latest
```



## Deploy logs

<details open>
<summary><strong>Deploy run output</strong> (collapse / expand — long log scrolls inside the box)</summary>

```text
➜  demojam-2027 git:(main) ✗ ./deploy.sh --token=sha256~... --server=https://api.ocp.<name>.opentlc.com:6443

[21:56:52] Logging in to https://api.ocp.<name>.opentlc.com:6443
  OK  Logged in as admin

[21:56:57] Ensuring namespace casino-sentinel
namespace/casino-sentinel created
namespace/casino-sentinel labeled

[21:57:01] Installing AMQ Streams and Streams Console operators (cluster-wide)
subscription.operators.coreos.com/amq-streams created
subscription.operators.coreos.com/amq-streams-console created

[21:57:07] Waiting for operator CSV matching /amqstreams/ in openshift-operators
  OK  CSV amqstreams.v3.2.1-14 Succeeded

[21:57:23] Waiting for operator CSV matching /amq-streams-console|console-operator/ in openshift-operators
  OK  CSV amq-streams-console.v3.2.1-10 Succeeded
serviceaccount/casino-sentinel created
configmap/casino-config created
networkpolicy.networking.k8s.io/casino-default-deny-ingress created
networkpolicy.networking.k8s.io/casino-allow-same-namespace created
networkpolicy.networking.k8s.io/casino-allow-api created
networkpolicy.networking.k8s.io/casino-allow-router-ui created
networkpolicy.networking.k8s.io/casino-allow-router-console created
role.rbac.authorization.k8s.io/casino-sentinel created
rolebinding.rbac.authorization.k8s.io/casino-sentinel created

[21:58:00] Creating SQL ConfigMaps
configmap/casino-db-cfg created
configmap/casino-db-init created
  OK  ConfigMaps casino-db-cfg / casino-db-init
secret/casino-db created
persistentvolumeclaim/casino-db created
service/casino-db created
deployment.apps/casino-db created

[21:58:13] Waiting for deployment/casino-db
Waiting for deployment "casino-db" rollout to finish: 0 of 1 updated replicas are available...
deployment "casino-db" successfully rolled out

[21:58:34] Applying schema + seed
CREATE EXTENSION
DO
GRANT
GRANT
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE INDEX
CREATE INDEX
CREATE INDEX
GRANT
ALTER DEFAULT PRIVILEGES
DO
CREATE SCHEMA
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
INSERT 0 7
CREATE INDEX
CREATE INDEX
CREATE INDEX
GRANT
GRANT
ALTER DEFAULT PRIVILEGES
GRANT
ALTER DEFAULT PRIVILEGES
INSERT 0 12
INSERT 0 12
INSERT 0 12
INSERT 0 9
INSERT 0 3
INSERT 0 8
INSERT 0 2
INSERT 0 3
INSERT 0 6
INSERT 0 4
INSERT 0 2
  OK  Database initialized
kafkanodepool.kafka.strimzi.io/dual created
kafka.kafka.strimzi.io/sentinel-kafka created
  OK  App images from Quay (no sentinel-python / sentinel-ui BuildConfig)

[21:59:00]   python: quay.io/shailendra14k/sentinel-python:latest

[21:59:00]   ui:     quay.io/shailendra14k/sentinel-ui:latest

[21:59:00] Waiting for kafka/sentinel-kafka Ready
kafka.kafka.strimzi.io/sentinel-kafka condition met
kafkauser.kafka.strimzi.io/console-user created
kafkauser.kafka.strimzi.io/casino-services created
kafkauser.kafka.strimzi.io/console-user condition met
kafkauser.kafka.strimzi.io/casino-services condition met
  OK  Cluster domain: apps.ocp.xmlr4.sandbox840.opentlc.com

[22:00:12] Deploying Streams Console at sentinel-console.apps.ocp.xmlr4.sandbox840.opentlc.com
kafkauser.kafka.strimzi.io/console-user unchanged
kafkauser.kafka.strimzi.io/console-user condition met
console.console.streamshub.github.com/sentinel-console created

[22:00:18] Waiting for consoles.console.streamshub.github.com/sentinel-console Ready
console.console.streamshub.github.com/sentinel-console condition met
  OK  Console URL: https://sentinel-console.apps.ocp.xmlr4.sandbox840.opentlc.com
kafkatopic.kafka.strimzi.io/casino-cdc-players created
kafkatopic.kafka.strimzi.io/casino-cdc-transactions created
kafkatopic.kafka.strimzi.io/casino-cdc-wagers created
kafkatopic.kafka.strimzi.io/casino-cdc-chips created
kafkatopic.kafka.strimzi.io/casino-cdc-cage created
kafkatopic.kafka.strimzi.io/casino-cdc-kyc created
kafkatopic.kafka.strimzi.io/casino-cdc-player-accounts created
kafkatopic.kafka.strimzi.io/casino-cdc-gaming-sessions created
kafkatopic.kafka.strimzi.io/casino-cdc-payment-methods created
kafkatopic.kafka.strimzi.io/casino-cdc-devices created
kafkatopic.kafka.strimzi.io/casino-events-normalized created
kafkatopic.kafka.strimzi.io/casino-events-enriched created
kafkatopic.kafka.strimzi.io/aml-features created
kafkatopic.kafka.strimzi.io/aml-rules-hits created
kafkatopic.kafka.strimzi.io/aml-ml-scores created
kafkatopic.kafka.strimzi.io/aml-behavioral-scores created
kafkatopic.kafka.strimzi.io/aml-risk-assessments created
kafkatopic.kafka.strimzi.io/aml-explanations created
kafkatopic.kafka.strimzi.io/aml-alerts created
kafkatopic.kafka.strimzi.io/aml-player-risk-profiles created
kafkatopic.kafka.strimzi.io/casino-dlq created
kafkatopic.kafka.strimzi.io/debezium-heartbeat-casino-cdc created
imagestream.image.openshift.io/sentinel-debezium-connect created
kafkaconnect.kafka.strimzi.io/sentinel-connect created

[22:00:58] App images: python=quay.io/shailendra14k/sentinel-python:latest  ui=quay.io/shailendra14k/sentinel-ui:latest
deployment.apps/sentinel-event-normalizer created
deployment.apps/sentinel-feature-engine created
deployment.apps/sentinel-aml-rules created
deployment.apps/sentinel-ml-engine created
deployment.apps/sentinel-risk-engine created
deployment.apps/sentinel-alert-service created
deployment.apps/sentinel-api created
service/sentinel-api created
deployment.apps/sentinel-simulator created
deployment.apps/sentinel-ui created
service/sentinel-ui created
route.route.openshift.io/sentinel-ui created
route.route.openshift.io/sentinel-api created
configmap/casino-grafana-dashboard created

[22:01:16] Waiting for deployment/sentinel-event-normalizer
deployment "sentinel-event-normalizer" successfully rolled out

[22:01:18] Waiting for deployment/sentinel-feature-engine
deployment "sentinel-feature-engine" successfully rolled out

[22:01:20] Waiting for deployment/sentinel-aml-rules
deployment "sentinel-aml-rules" successfully rolled out

[22:01:21] Waiting for deployment/sentinel-ml-engine
deployment "sentinel-ml-engine" successfully rolled out

[22:01:23] Waiting for deployment/sentinel-risk-engine
deployment "sentinel-risk-engine" successfully rolled out

[22:01:24] Waiting for deployment/sentinel-alert-service
deployment "sentinel-alert-service" successfully rolled out

[22:01:26] Waiting for deployment/sentinel-api
deployment "sentinel-api" successfully rolled out

[22:01:28] Waiting for deployment/sentinel-simulator
deployment "sentinel-simulator" successfully rolled out

[22:01:29] Waiting for deployment/sentinel-ui
deployment "sentinel-ui" successfully rolled out

[22:01:31] Waiting for Kafka Connect image build (can take several minutes)

[22:01:31] Waiting for kafkaconnect/sentinel-connect Ready
kafkaconnect.kafka.strimzi.io/sentinel-connect condition met
kafkaconnector.kafka.strimzi.io/casino-postgres-connector created

[22:02:20] Waiting for kafkaconnector/casino-postgres-connector Ready
kafkaconnector.kafka.strimzi.io/casino-postgres-connector condition met

[22:02:22] Deploying OpenShift AI TFM ServingRuntime + InferenceService (--rhoai)

[22:02:22]   TFM image: quay.io/shailendra14k/casino-tfm-embed:latest

[22:02:22] Checking OpenShift AI / KServe readiness (ServingRuntime + InferenceService CRDs)
  OK  DataScienceCluster: default-dsc
  OK  DSC default-dsc kserve.managementState=Managed

[22:02:25] Waiting up to 600s for KServe CRDs…
  OK  CRDs ready: ServingRuntime + InferenceService
  OK  DataScienceCluster/default-dsc Ready=True

[22:02:28] Freeing the single cluster GPU for TFM (stop other GPU model deployments first)
  OK  Deploy my-first-model/llama-32-3b-instruct-predictor already scaled to 0

[22:02:34] Waiting for GPU to become free for tfm-embed…
  OK  GPU is free (no competing workloads)
  OK  Skipping ngc-secret (NeMo image is on Quay; NGC_API_KEY not required)
servingruntime.serving.kserve.io/casino-tfm-nemo created
inferenceservice.serving.kserve.io/tfm-embed created
  OK  Hardware profile annotations: gpu-profile (redhat-ods-applications)
  OK  ServingRuntime image: quay.io/shailendra14k/casino-tfm-embed:latest

[22:02:45] Waiting for InferenceService/tfm-embed Ready (casino-tfm-embed pull can take several minutes)
inferenceservice.serving.kserve.io/tfm-embed condition met
configmap/casino-config patched

[22:03:23] Restarting sentinel-ml-engine + sentinel-api to pick up TFM_MODE=http / AI_MODE=nvidia
deployment.apps/sentinel-ml-engine restarted
deployment.apps/sentinel-api restarted
deployment "sentinel-ml-engine" successfully rolled out
Waiting for deployment "sentinel-api" rollout to finish: 1 old replicas are pending termination...
Waiting for deployment "sentinel-api" rollout to finish: 1 old replicas are pending termination...
deployment "sentinel-api" successfully rolled out
  OK  RHOAI TFM cutover: TFM_EMBED_URL=http://tfm-embed-predictor.casino-sentinel.svc:8080/v1/embeddings (ml-engine + api restarted)

[22:03:38] Deployment summary

NAME                                    READY   WARNINGS   KAFKA VERSION   METADATA VERSION
kafka.kafka.strimzi.io/sentinel-kafka   True               4.2.0           4.2-IV1

NAME                                  DESIRED REPLICAS   ROLES                     NODEIDS
kafkanodepool.kafka.strimzi.io/dual   1                  ["controller","broker"]   [0]

NAME                                         CLUSTER          AUTHENTICATION   AUTHORIZATION   READY
kafkauser.kafka.strimzi.io/casino-services   sentinel-kafka   scram-sha-512    simple          True
kafkauser.kafka.strimzi.io/console-user      sentinel-kafka   scram-sha-512    simple          True

NAME                                             DESIRED REPLICAS   READY
kafkaconnect.kafka.strimzi.io/sentinel-connect   1                  True

NAME                                                        CLUSTER            CONNECTOR CLASS                                      MAX TASKS   READY
kafkaconnector.kafka.strimzi.io/casino-postgres-connector   sentinel-connect   io.debezium.connector.postgresql.PostgresConnector   1           True

NAME                                                     READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/casino-db                                1/1     1            1           5m28s
deployment.apps/sentinel-alert-service                   1/1     1            1           2m36s
deployment.apps/sentinel-aml-rules                       1/1     1            1           2m38s
deployment.apps/sentinel-api                             1/1     1            1           2m35s
deployment.apps/sentinel-console-console-deployment      1/1     1            1           3m21s
deployment.apps/sentinel-console-prometheus-deployment   1/1     1            1           3m22s
deployment.apps/sentinel-event-normalizer                1/1     1            1           2m41s
deployment.apps/sentinel-feature-engine                  1/1     1            1           2m39s
deployment.apps/sentinel-kafka-entity-operator           1/1     1            1           4m2s
deployment.apps/sentinel-ml-engine                       1/1     1            1           2m37s
deployment.apps/sentinel-risk-engine                     1/1     1            1           2m36s
deployment.apps/sentinel-simulator                       0/0     0            0           2m33s
deployment.apps/sentinel-ui                              1/1     1            1           2m30s
deployment.apps/tfm-embed-predictor                      1/1     1            1           56s

NAME                                          TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                        AGE
service/casino-db                             ClusterIP   172.30.252.162   <none>        5432/TCP                                       5m30s
service/sentinel-api                          ClusterIP   172.30.92.68     <none>        8080/TCP                                       2m35s
service/sentinel-connect-connect              ClusterIP   None             <none>        8083/TCP                                       2m28s
service/sentinel-connect-connect-api          ClusterIP   172.30.52.207    <none>        8083/TCP                                       2m28s
service/sentinel-console-console-service      ClusterIP   172.30.242.96    <none>        80/TCP                                         3m23s
service/sentinel-console-prometheus-service   ClusterIP   172.30.40.74     <none>        9090/TCP                                       3m21s
service/sentinel-kafka-kafka-bootstrap        ClusterIP   172.30.202.246   <none>        9091/TCP,9092/TCP,9094/TCP                     4m40s
service/sentinel-kafka-kafka-brokers          ClusterIP   None             <none>        9090/TCP,9091/TCP,8443/TCP,9092/TCP,9094/TCP   4m40s
service/sentinel-ui                           ClusterIP   172.30.240.175   <none>        8080/TCP                                       2m30s
service/tfm-embed-predictor                   ClusterIP   None             <none>        80/TCP                                         57s

NAME                                                              HOST/PORT                                                            PATH   SERVICES                           PORT    TERMINATION     WILDCARD
route.route.openshift.io/sentinel-api                             sentinel-api-casino-sentinel.apps.ocp.xmlr4.sandbox840.opentlc.com          sentinel-api                       http    edge/Redirect   None
route.route.openshift.io/sentinel-console-console-ingress-xnh2k   sentinel-console.apps.ocp.xmlr4.sandbox840.opentlc.com                      sentinel-console-console-service   <all>   edge/Redirect   None
route.route.openshift.io/sentinel-ui                              sentinel-ui-casino-sentinel.apps.ocp.xmlr4.sandbox840.opentlc.com           sentinel-ui                        http    edge/Redirect   None

NAME                                                 STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   VOLUMEATTRIBUTESCLASS   AGE
persistentvolumeclaim/casino-db                      Bound    pvc-eaa8988b-0b12-4931-a52f-d3a6a4a8c931   10Gi       RWO            gp3-csi        <unset>                 5m31s
persistentvolumeclaim/data-0-sentinel-kafka-dual-0   Bound    pvc-ff48357f-69fa-4861-ae94-da8168ed5acf   20Gi       RWO            gp3-csi        <unset>                 4m40s

Casino Guard UI:  https://sentinel-ui-casino-sentinel.apps.ocp.xmlr4.sandbox840.opentlc.com
Streams Console:  https://sentinel-console.apps.ocp.xmlr4.sandbox840.opentlc.com
Kafka bootstrap:  sentinel-kafka-kafka-bootstrap.casino-sentinel.svc:9092
Postgres:         casino-db.casino-sentinel.svc:5432  (user casino / casino)
XGBoost:          joblib in sentinel-python (sentinel-ml-engine)
Pipeline:         normalizer → features → rules+ml → risk → alerts
Images:           quay.io/shailendra14k/sentinel-python:latest / quay.io/shailendra14k/sentinel-ui:latest
TFM:              TFM_MODE/AI_MODE on casino-config (default hashed/mock). GPU: TFM_MODE=http + TFM_EMBED_URL
RHOAI TFM:        quay.io/shailendra14k/casino-tfm-embed:latest (ServingRuntime casino-tfm-nemo + InferenceService tfm-embed)
TFM_EMBED_URL:    http://tfm-embed-predictor.casino-sentinel.svc:8080/v1/embeddings
NAME                                               DISABLED   MODELTYPE    CONTAINERS         AGE
servingruntime.serving.kserve.io/casino-tfm-nemo              casino-tfm   kserve-container   62s

NAME                                           URL                                                            READY   PREV   LATEST   PREVROLLEDOUTREVISION   LATESTREADYREVISION   AGE
inferenceservice.serving.kserve.io/tfm-embed   http://tfm-embed-predictor.casino-sentinel.svc.cluster.local   True                                                                  59s

Demo: open the UI → Demo Controls / Start Scenario (structuring / rapid-wire / mixed)

[22:03:45] Done.


```

</details>





## Destroy logs

<details open>
<summary><strong>Destroy run output</strong> (collapse / expand — long log scrolls inside the box)</summary>

```text


./destroy.sh --ocp_api=https://api.ocp.<name>.opentlc.com:6443 --ocp_token=sha256~....

[21:48:57] Logging in to https://api.ocp.<name>.opentlc.com:6443

[21:49:01] Deleting Casino Sentinel resources in casino-sentinel
inferenceservice.serving.kserve.io "tfm-embed" deleted
servingruntime.serving.kserve.io "casino-tfm-nemo" deleted
console.console.streamshub.github.com "sentinel-console" deleted
kafkauser.kafka.strimzi.io "casino-services" deleted
kafkauser.kafka.strimzi.io "console-user" deleted
kafkaconnector.kafka.strimzi.io "casino-postgres-connector" deleted
kafkaconnect.kafka.strimzi.io "sentinel-connect" deleted
kafkatopic.kafka.strimzi.io "aml-alerts" deleted
kafkatopic.kafka.strimzi.io "aml-behavioral-scores" deleted
kafkatopic.kafka.strimzi.io "aml-explanations" deleted
kafkatopic.kafka.strimzi.io "aml-features" deleted
kafkatopic.kafka.strimzi.io "aml-ml-scores" deleted
kafkatopic.kafka.strimzi.io "aml-player-risk-profiles" deleted
kafkatopic.kafka.strimzi.io "aml-risk-assessments" deleted
kafkatopic.kafka.strimzi.io "aml-rules-hits" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-cage" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-chips" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-devices" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-gaming-sessions" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-kyc" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-payment-methods" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-player-accounts" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-players" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-transactions" deleted
kafkatopic.kafka.strimzi.io "casino-cdc-wagers" deleted
kafkatopic.kafka.strimzi.io "casino-dlq" deleted
kafkatopic.kafka.strimzi.io "casino-events-enriched" deleted
kafkatopic.kafka.strimzi.io "casino-events-normalized" deleted
kafkatopic.kafka.strimzi.io "debezium-heartbeat-casino-cdc" deleted
kafka.kafka.strimzi.io "sentinel-kafka" deleted
kafkanodepool.kafka.strimzi.io "dual" deleted
pod "tfm-embed-predictor-59cbdf86c6-4x9t7" deleted
deployment.apps "casino-db" deleted
deployment.apps "sentinel-alert-service" deleted
deployment.apps "sentinel-aml-rules" deleted
deployment.apps "sentinel-api" deleted
deployment.apps "sentinel-event-normalizer" deleted
deployment.apps "sentinel-feature-engine" deleted
deployment.apps "sentinel-ml-engine" deleted
deployment.apps "sentinel-risk-engine" deleted
deployment.apps "sentinel-simulator" deleted
deployment.apps "sentinel-ui" deleted
service "casino-db" deleted
service "sentinel-api" deleted
service "sentinel-ui" deleted
route.route.openshift.io "sentinel-api" deleted
route.route.openshift.io "sentinel-ui" deleted
imagestream.image.openshift.io "sentinel-debezium-connect" deleted
persistentvolumeclaim "casino-db" deleted
configmap "casino-config" deleted
configmap "casino-grafana-dashboard" deleted
  OK  Namespace resources deleted (or ignored if missing)

[21:49:49] Deleting namespace casino-sentinel
namespace "casino-sentinel" deleted
  OK  Namespace casino-sentinel delete requested

[21:49:50] Uninstalling operator Subscription amq-streams-console (openshift-operators)
subscription.operators.coreos.com "amq-streams-console" deleted
clusterserviceversion.operators.coreos.com "amq-streams-console.v3.2.1-10" deleted
  OK  Removed Subscription amq-streams-console + CSV amq-streams-console.v3.2.1-10

[21:49:54] Uninstalling operator Subscription amq-streams (openshift-operators)
subscription.operators.coreos.com "amq-streams" deleted
clusterserviceversion.operators.coreos.com "amqstreams.v3.2.1-14" deleted
  OK  Removed Subscription amq-streams + CSV amqstreams.v3.2.1-14

[21:49:58] Done.
Destroy complete (namespace_delete=true operators_removed=true).

```

</details>


