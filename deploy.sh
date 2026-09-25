#!/usr/bin/env bash
# Deploy Casino Sentinel (Casino Guard AML demo) to OpenShift.
# Usage:
#   ./deploy.sh --api https://api.cluster:6443 --token sha256~...
#   ./deploy.sh --env ./env

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFESTS="${SCRIPT_DIR}/manifests"

OCP_API="${OCP_API:-}"
OCP_TOKEN="${OCP_TOKEN:-}"
NAMESPACE="${NAMESPACE:-casino-sentinel}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-}"
INSECURE="${INSECURE:-false}"
SKIP_OPERATORS=true
SKIP_CONSOLE=false
SKIP_BUILDS=false
DEPLOY_RHOAI=false
DRY_RUN=false
ENV_FILE=""

log()  { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '  OK  %s\n' "$*"; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Deploy Casino Sentinel (PostgreSQL + AMQ Streams + Streams Console + Debezium + AML pipeline + Casino Guard UI).

Required:
  --api URL            OpenShift API server
  --token TOKEN        oc token (oc whoami -t)
  --env FILE           Load OCP_API / OCP_TOKEN / NAMESPACE from a file

Optional:
  --namespace NAME     Project (default: casino-sentinel)
  --insecure           Skip TLS verify
  --install-operators  Install AMQ Streams + Streams Console operators via OLM
  --skip-console       Do not deploy Streams Console
  --skip-builds        Reuse existing ImageStream tags
  --rhoai              Deploy TFM on OpenShift AI (ServingRuntime + InferenceService)
  --dry-run            Print commands only
  -h, --help

Default (./deploy.sh --env ./env --insecure):
  PostgreSQL + AMQ Streams + Streams Console + Debezium + microservice AML pipeline + UI.
  XGBoost scores in sentinel-ml-engine from joblib (no OpenShift AI).
  TFM_MODE=hashed / AI_MODE=mock on casino-config.

With --rhoai (OpenShift AI KServe + 1 GPU + NGC):
  Prerequisite: ../nvidia-tfm/clone-nvidia-tfm.sh  → model/decoder-foundation-model/
  Creates PVC tfm-checkpoint + loads weights, ngc-secret (NGC_API_KEY),
  ConfigMap tfm-embed-script from model/serve_embeddings.py,
  applies manifests/08-openshift-ai/*, sets TFM_MODE=http + AI_MODE=nvidia.
  See ../nvidia-tfm/OCP-1GPU.md
EOF
}

need_oc() {
  command -v oc >/dev/null 2>&1 || die "oc CLI not found."
}

apply() {
  local file="$1"
  if [[ "${DRY_RUN}" == true ]]; then
    echo "DRY-RUN oc apply -f ${file}"
    return 0
  fi
  oc apply -f "${file}"
}

login() {
  [[ -n "${OCP_API}" ]]   || die "OpenShift API URL is required (--api or OCP_API)."
  [[ -n "${OCP_TOKEN}" ]] || die "OpenShift token is required (--token or OCP_TOKEN)."
  log "Logging in to ${OCP_API}"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  if [[ "${INSECURE}" == true || "${INSECURE}" == "true" ]]; then
    oc login --server="${OCP_API}" --token="${OCP_TOKEN}" --insecure-skip-tls-verify=true >/dev/null
  else
    oc login --server="${OCP_API}" --token="${OCP_TOKEN}" >/dev/null
  fi
  ok "Logged in as $(oc whoami)"
}

ensure_namespace() {
  log "Ensuring namespace ${NAMESPACE}"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  if oc get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    ok "Namespace exists"
  else
    oc create namespace "${NAMESPACE}"
    oc label namespace "${NAMESPACE}" app.kubernetes.io/part-of=casino-sentinel --overwrite
  fi
  oc project "${NAMESPACE}" >/dev/null
}

create_sql_configmaps() {
  log "Creating SQL ConfigMaps"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  oc create configmap casino-db-cfg \
    --from-file="${SCRIPT_DIR}/sql/postgresql-cfg/logical.conf" \
    -n "${NAMESPACE}" --dry-run=client -o yaml | oc apply -f -
  oc create configmap casino-db-init \
    --from-file=01-init.sql="${SCRIPT_DIR}/sql/01-init.sql" \
    --from-file=02-seed.sql="${SCRIPT_DIR}/sql/02-seed.sql" \
    -n "${NAMESPACE}" --dry-run=client -o yaml | oc apply -f -
  ok "ConfigMaps casino-db-cfg / casino-db-init"
}

wait_deploy() {
  local name="$1"
  local timeout="${2:-300}"
  log "Waiting for deployment/${name}"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  oc rollout status "deployment/${name}" -n "${NAMESPACE}" --timeout="${timeout}s"
}

wait_cr() {
  local kind="$1"
  local name="$2"
  local timeout="${3:-600}"
  log "Waiting for ${kind}/${name} Ready"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  oc wait "${kind}/${name}" -n "${NAMESPACE}" --for=condition=Ready --timeout="${timeout}s"
}

wait_csv() {
  local grep_pat="$1"
  local ns="${2:-openshift-operators}"
  local timeout="${3:-300}"
  log "Waiting for operator CSV matching /${grep_pat}/ in ${ns}"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  local elapsed=0
  local csv=""
  while (( elapsed < timeout )); do
    csv="$(oc get csv -n "${ns}" --no-headers 2>/dev/null | awk -v g="${grep_pat}" 'tolower($1) ~ g {print $1; exit}')"
    if [[ -n "${csv}" ]]; then
      if oc wait "csv/${csv}" -n "${ns}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=30s >/dev/null 2>&1; then
        ok "CSV ${csv} Succeeded"
        return 0
      fi
    fi
    sleep 10
    elapsed=$((elapsed + 10))
  done
  die "Timed out waiting for operator CSV /${grep_pat}/ in ${ns}"
}

detect_domain() {
  if [[ -n "${CLUSTER_DOMAIN}" ]]; then
    return 0
  fi
  if [[ "${DRY_RUN}" == true ]]; then
    CLUSTER_DOMAIN="apps.example.com"
    return 0
  fi
  CLUSTER_DOMAIN="$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}' 2>/dev/null || true)"
  [[ -n "${CLUSTER_DOMAIN}" ]] || die "Could not detect cluster apps domain. Set CLUSTER_DOMAIN in env."
  ok "Cluster domain: ${CLUSTER_DOMAIN}"
}

run_sql() {
  local file="$1"
  local dest="/tmp/$(basename "${file}")"
  if [[ "${DRY_RUN}" == true ]]; then
    echo "DRY-RUN apply ${file} via psql"
    return 0
  fi
  oc cp "${file}" "${NAMESPACE}/$(oc get pod -n "${NAMESPACE}" -l app=casino-db -o jsonpath='{.items[0].metadata.name}'):${dest}"
  oc exec -n "${NAMESPACE}" deploy/casino-db -- \
    env PGPASSWORD=casinoadmin psql -h 127.0.0.1 -U postgres -d casino -v ON_ERROR_STOP=0 -f "${dest}"
}

deploy_operators() {
  if [[ "${SKIP_OPERATORS}" == true ]]; then
    log "Skipping operators (AMQ Streams + Console assumed installed)"
    return 0
  fi
  log "Installing AMQ Streams and Streams Console operators (cluster-wide)"
  apply "${MANIFESTS}/01-operators/amq-streams-subscription.yaml"
  apply "${MANIFESTS}/01-operators/console-operator-subscription.yaml"
  wait_csv "amqstreams" openshift-operators 420
  wait_csv "amq-streams-console|console-operator" openshift-operators 420
}

deploy_console() {
  if [[ "${SKIP_CONSOLE}" == true ]]; then
    log "Skipping Streams Console"
    return 0
  fi
  detect_domain
  log "Deploying Streams Console at sentinel-console.${CLUSTER_DOMAIN}"
  if [[ "${DRY_RUN}" == true ]]; then
    echo "DRY-RUN Console hostname=sentinel-console.${CLUSTER_DOMAIN} namespace=${NAMESPACE}"
    return 0
  fi
  apply "${MANIFESTS}/02-kafka/kafka-user-console.yaml"
  oc wait kafkauser/console-user -n "${NAMESPACE}" --for=condition=Ready --timeout=180s || true
  sed -e "s/__CLUSTER_DOMAIN__/${CLUSTER_DOMAIN}/g" \
      -e "s/__NAMESPACE__/${NAMESPACE}/g" \
      "${MANIFESTS}/03-console/console.yaml" | oc apply -n "${NAMESPACE}" -f -
  wait_cr consoles.console.streamshub.github.com sentinel-console 300 || true
  ok "Console URL: https://sentinel-console.${CLUSTER_DOMAIN}"
}

# OpenShift AI TFM: PVC + NGC secret + script ConfigMap + ServingRuntime + InferenceService.
# Laptop pre-req: ../nvidia-tfm/clone-nvidia-tfm.sh → model/decoder-foundation-model/ (see OCP-1GPU.md).
ensure_ngc_secret() {
  if oc get secret ngc-secret -n "${NAMESPACE}" >/dev/null 2>&1; then
    ok "Secret ngc-secret exists"
    return 0
  fi
  local key="${NGC_API_KEY:-}"
  [[ -n "${key}" ]] || die "Secret ngc-secret missing. Set NGC_API_KEY in env (see nvidia-tfm/OCP-1GPU.md) or: oc create secret docker-registry ngc-secret ..."
  log "Creating ngc-secret from NGC_API_KEY"
  oc create secret docker-registry ngc-secret \
    --docker-server=nvcr.io \
    --docker-username='$oauthtoken' \
    --docker-password="${key}" \
    -n "${NAMESPACE}"
  oc secrets link default ngc-secret --for=pull -n "${NAMESPACE}" || true
  ok "Secret ngc-secret created"
}

ensure_tfm_checkpoint_pvc() {
  local ckpt_dir="${SCRIPT_DIR}/model/decoder-foundation-model"
  local safetensors="${ckpt_dir}/model-00001-of-00001.safetensors"
  [[ -d "${ckpt_dir}" ]] || die "Missing ${ckpt_dir}. Run: ../nvidia-tfm/clone-nvidia-tfm.sh (see nvidia-tfm/OCP-1GPU.md)"
  [[ -f "${safetensors}" ]] || die "Missing ${safetensors}. Run git lfs pull via clone-nvidia-tfm.sh"
  local sz
  sz="$(wc -c < "${safetensors}" | tr -d ' ')"
  [[ "${sz}" -gt 1000000 ]] || die "${safetensors} looks like an LFS pointer (${sz} bytes). Re-run clone-nvidia-tfm.sh"

  if ! oc get pvc tfm-checkpoint -n "${NAMESPACE}" >/dev/null 2>&1; then
    log "Creating PVC tfm-checkpoint"
    oc apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: tfm-checkpoint
  namespace: ${NAMESPACE}
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 2Gi
EOF
  else
    ok "PVC tfm-checkpoint exists"
  fi

  log "Loading decoder-foundation-model onto PVC tfm-checkpoint"
  oc delete pod tfm-ckpt-load -n "${NAMESPACE}" --ignore-not-found || true
  oc run tfm-ckpt-load -n "${NAMESPACE}" --restart=Never \
    --image=registry.access.redhat.com/ubi9/ubi-minimal \
    --overrides="{\"spec\":{\"containers\":[{\"name\":\"load\",\"image\":\"registry.access.redhat.com/ubi9/ubi-minimal\",\"command\":[\"sleep\",\"3600\"],\"volumeMounts\":[{\"name\":\"ckpt\",\"mountPath\":\"/models\"}]}],\"volumes\":[{\"name\":\"ckpt\",\"persistentVolumeClaim\":{\"claimName\":\"tfm-checkpoint\"}}]}}"
  oc wait -n "${NAMESPACE}" pod/tfm-ckpt-load --for=condition=Ready --timeout=180s
  oc exec -n "${NAMESPACE}" tfm-ckpt-load -- microdnf install -y tar gzip
  tar -C "${SCRIPT_DIR}/model" -czf /tmp/decoder-foundation-model.tgz decoder-foundation-model
  oc cp /tmp/decoder-foundation-model.tgz "${NAMESPACE}/tfm-ckpt-load:/models/decoder-foundation-model.tgz"
  oc exec -n "${NAMESPACE}" tfm-ckpt-load -- tar -C /models -xzf /models/decoder-foundation-model.tgz
  oc delete pod tfm-ckpt-load -n "${NAMESPACE}" --ignore-not-found || true
  rm -f /tmp/decoder-foundation-model.tgz
  ok "Checkpoint loaded onto PVC"
}

deploy_rhoai() {
  if [[ "${DEPLOY_RHOAI}" != true ]]; then
    return 0
  fi

  local rhoai_dir="${MANIFESTS}/08-openshift-ai"
  local script_py="${SCRIPT_DIR}/model/serve_embeddings.py"
  local embed_url="http://tfm-embed-predictor.${NAMESPACE}.svc:8080/v1/embeddings"

  log "Deploying OpenShift AI TFM ServingRuntime + InferenceService (--rhoai)"
  [[ -d "${rhoai_dir}" ]] || die "Missing ${rhoai_dir}"
  [[ -f "${script_py}" ]] || die "Missing ${script_py}"

  if [[ "${DRY_RUN}" == true ]]; then
    echo "DRY-RUN ensure ngc-secret + PVC tfm-checkpoint from model/decoder-foundation-model"
    echo "DRY-RUN ConfigMap tfm-embed-script from ${script_py}"
    echo "DRY-RUN oc apply ${rhoai_dir}/*.yaml (namespace=${NAMESPACE})"
    echo "DRY-RUN patch casino-config TFM_MODE=http TFM_EMBED_URL=${embed_url} AI_MODE=nvidia"
    return 0
  fi

  if ! oc api-resources --api-group=serving.kserve.io 2>/dev/null | grep -q ServingRuntime; then
    die "OpenShift AI / KServe CRDs not found (ServingRuntime). Enable RHOAI single-model serving first."
  fi

  ensure_ngc_secret
  ensure_tfm_checkpoint_pvc

  oc create configmap tfm-embed-script \
    -n "${NAMESPACE}" \
    --from-file=serve_embeddings.py="${script_py}" \
    --dry-run=client -o yaml | oc apply -f -
  ok "ConfigMap tfm-embed-script"

  if oc get deploy tfm-embed -n "${NAMESPACE}" >/dev/null 2>&1; then
    log "Deleting legacy deploy/tfm-embed (InferenceService only)"
    oc delete deploy,svc tfm-embed -n "${NAMESPACE}" --ignore-not-found || true
  fi

  # Dashboard: HARDWARE_PROFILE_NAME must match Settings → Hardware profiles (e.g. gpu-profile)
  local hwp_name="${HARDWARE_PROFILE_NAME:-gpu-profile}"
  local hwp_ns="${HARDWARE_PROFILE_NAMESPACE:-redhat-ods-applications}"

  sed "s/namespace: casino-sentinel/namespace: ${NAMESPACE}/g" \
    "${rhoai_dir}/servingruntime-casino-tfm-nemo.yaml" | oc apply -f -

  sed -e "s/namespace: casino-sentinel/namespace: ${NAMESPACE}/g" \
      -e "s/opendatahub.io\/hardware-profile-name: .*/opendatahub.io\/hardware-profile-name: ${hwp_name}/" \
      -e "s/opendatahub.io\/hardware-profile-namespace: .*/opendatahub.io\/hardware-profile-namespace: ${hwp_ns}/" \
    "${rhoai_dir}/inferenceservice-tfm-embed.yaml" | oc apply -f -
  ok "Hardware profile annotations: ${hwp_name} (${hwp_ns})"

  log "Waiting for InferenceService/tfm-embed Ready (NeMo image pull can take several minutes)"
  oc wait inferenceservice/tfm-embed -n "${NAMESPACE}" --for=condition=Ready --timeout=900s || true

  oc patch configmap casino-config -n "${NAMESPACE}" --type merge -p "{
    \"data\": {
      \"TFM_MODE\": \"http\",
      \"TFM_EMBED_URL\": \"${embed_url}\",
      \"TFM_CHECKPOINT\": \"\",
      \"AI_MODE\": \"nvidia\"
    }
  }"
  oc rollout restart deploy/sentinel-ml-engine -n "${NAMESPACE}" || true
  oc rollout status deploy/sentinel-ml-engine -n "${NAMESPACE}" --timeout=180s || true
  ok "RHOAI TFM cutover: TFM_EMBED_URL=${embed_url}"
}

start_build() {
  local name="$1"
  local dir="$2"
  log "Binary build ${name} from ${dir}"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  oc start-build "${name}" -n "${NAMESPACE}" --from-dir="${dir}" --follow --wait
}

print_summary() {
  [[ "${DRY_RUN}" == true ]] && return 0
  log "Deployment summary"
  echo
  oc get kafka,kafkanodepool,kafkauser,kafkaconnect,kafkaconnector,deploy,svc,route,pvc -n "${NAMESPACE}" 2>/dev/null || true
  echo
  local host
  host="$(oc get route sentinel-ui -n "${NAMESPACE}" -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  if [[ -n "${host}" ]]; then
    echo "Casino Guard UI:  https://${host}"
  fi
  if [[ "${SKIP_CONSOLE}" != true && -n "${CLUSTER_DOMAIN}" ]]; then
    echo "Streams Console:  https://sentinel-console.${CLUSTER_DOMAIN}"
  fi
  echo "Kafka bootstrap:  sentinel-kafka-kafka-bootstrap.${NAMESPACE}.svc:9092"
  echo "Postgres:         casino-db.${NAMESPACE}.svc:5432  (user casino / casino)"
  echo "XGBoost:          joblib in sentinel-python (sentinel-ml-engine)"
  echo "Pipeline:         normalizer → features → rules+ml → risk → alerts"
  echo "TFM:              TFM_MODE/AI_MODE on casino-config (default hashed/mock). GPU: TFM_MODE=http + TFM_EMBED_URL"
  if [[ "${DEPLOY_RHOAI}" == true ]]; then
    echo "RHOAI TFM:        ServingRuntime casino-tfm-nemo + InferenceService tfm-embed"
    echo "TFM_EMBED_URL:    http://tfm-embed-predictor.${NAMESPACE}.svc:8080/v1/embeddings"
    oc get servingruntime,inferenceservice -n "${NAMESPACE}" 2>/dev/null || true
  fi
  echo
  echo "Demo: open the UI → Demo Controls / Start Scenario (structuring / rapid-wire / mixed)"
}

# Load --env first
_pre=("${@}")
for ((i=0; i<${#_pre[@]}; i++)); do
  if [[ "${_pre[$i]}" == "--env" ]]; then
    ENV_FILE="${_pre[$((i+1))]}"
    [[ -f "${ENV_FILE}" ]] || die "Env file not found: ${ENV_FILE}"
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
    break
  fi
done

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api)                OCP_API="$2"; shift 2 ;;
    --token)              OCP_TOKEN="$2"; shift 2 ;;
    --namespace)          NAMESPACE="$2"; shift 2 ;;
    --env)                ENV_FILE="$2"; shift 2 ;;
    --insecure)           INSECURE=true; shift ;;
    --skip-operators)     SKIP_OPERATORS=true; shift ;;
    --install-operators)  SKIP_OPERATORS=false; shift ;;
    --skip-console)       SKIP_CONSOLE=true; shift ;;
    --skip-builds)        SKIP_BUILDS=true; shift ;;
    --rhoai)              DEPLOY_RHOAI=true; shift ;;
    --dry-run)            DRY_RUN=true; shift ;;
    -h|--help)            usage; exit 0 ;;
    *)                    die "Unknown argument: $1" ;;
  esac
done

need_oc
login
ensure_namespace
deploy_operators
apply "${MANIFESTS}/00-base/serviceaccount.yaml"
apply "${MANIFESTS}/00-base/config.yaml"
apply "${MANIFESTS}/07-security/network-rbac.yaml"
create_sql_configmaps
apply "${MANIFESTS}/01-postgres/postgres.yaml"
wait_deploy casino-db 300
log "Applying schema + seed"
run_sql "${SCRIPT_DIR}/sql/01-init.sql"
run_sql "${SCRIPT_DIR}/sql/02-seed.sql"
ok "Database initialized"

apply "${MANIFESTS}/02-kafka/kafka.yaml"
apply "${MANIFESTS}/05-builds/python.yaml"
apply "${MANIFESTS}/05-builds/ui.yaml"
if [[ "${SKIP_BUILDS}" != true ]]; then
  start_build sentinel-python "${SCRIPT_DIR}"
  start_build sentinel-ui "${SCRIPT_DIR}/ui"
fi

wait_cr kafka sentinel-kafka 720 || true
apply "${MANIFESTS}/02-kafka/kafka-user-console.yaml"
apply "${MANIFESTS}/02-kafka/kafka-user-services.yaml"
if [[ "${DRY_RUN}" != true ]]; then
  oc wait kafkauser/console-user -n "${NAMESPACE}" --for=condition=Ready --timeout=180s || true
  oc wait kafkauser/casino-services -n "${NAMESPACE}" --for=condition=Ready --timeout=180s || true
fi
deploy_console
apply "${MANIFESTS}/03-topics/topics.yaml"
apply "${MANIFESTS}/04-debezium/imagestream.yaml"
apply "${MANIFESTS}/04-debezium/kafka-connect.yaml"

apply "${MANIFESTS}/06-apps/services.yaml"
apply "${MANIFESTS}/06-apps/ui.yaml"
apply "${MANIFESTS}/07-monitoring/monitoring.yaml" || true
for d in \
  sentinel-event-normalizer \
  sentinel-feature-engine \
  sentinel-aml-rules \
  sentinel-ml-engine \
  sentinel-risk-engine \
  sentinel-alert-service \
  sentinel-api \
  sentinel-simulator \
  sentinel-ui
do
  wait_deploy "$d" 240 || true
done

log "Waiting for Kafka Connect image build (can take several minutes)"
wait_cr kafkaconnect sentinel-connect 900 || true
apply "${MANIFESTS}/04-debezium/postgres-connector.yaml"
wait_cr kafkaconnector casino-postgres-connector 300 || true

deploy_rhoai

print_summary
log "Done."