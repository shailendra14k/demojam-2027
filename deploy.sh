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
  --dry-run            Print commands only
  -h, --help

Default (./deploy.sh --env ./env --insecure):
  PostgreSQL + AMQ Streams + Streams Console + Debezium + pipeline + UI.
  XGBoost scores in-pipeline from joblib (no OpenShift AI).
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
  echo "XGBoost:          joblib inside sentinel-python (sentinel-pipeline)"
  echo
  echo "Demo: open the UI → Start Scenario (structuring / rapid-wire / mixed)"
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
    --dry-run)            DRY_RUN=true; shift ;;
    -h|--help)            usage; exit 0 ;;
    *)                    die "Unknown argument: $1" ;;
  esac
done

need_oc
login
ensure_namespace
deploy_operators
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
  start_build sentinel-python "${SCRIPT_DIR}/services/python"
  start_build sentinel-ui "${SCRIPT_DIR}/ui"
fi

wait_cr kafka sentinel-kafka 720 || true
apply "${MANIFESTS}/02-kafka/kafka-user-console.yaml"
if [[ "${DRY_RUN}" != true ]]; then
  oc wait kafkauser/console-user -n "${NAMESPACE}" --for=condition=Ready --timeout=180s || true
fi
deploy_console
apply "${MANIFESTS}/03-topics/topics.yaml"
apply "${MANIFESTS}/04-debezium/imagestream.yaml"
apply "${MANIFESTS}/04-debezium/kafka-connect.yaml"

apply "${MANIFESTS}/06-apps/services.yaml"
apply "${MANIFESTS}/06-apps/ui.yaml"
wait_deploy sentinel-api 240 || true
wait_deploy sentinel-pipeline 240 || true
wait_deploy sentinel-simulator 240 || true
wait_deploy sentinel-ui 240 || true

log "Waiting for Kafka Connect image build (can take several minutes)"
wait_cr kafkaconnect sentinel-connect 900 || true
apply "${MANIFESTS}/04-debezium/postgres-connector.yaml"
wait_cr kafkaconnector casino-postgres-connector 300 || true

print_summary
log "Done."
