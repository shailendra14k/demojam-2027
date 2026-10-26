#!/usr/bin/env bash
# Deploy Casino Sentinel (Casino Guard AML demo) to OpenShift.
# Demo default (= old: --env ./.env --insecure --rhoai --install-operators):
#   ./deploy.sh --token=sha256~... --server=https://api...:6443

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFESTS="${SCRIPT_DIR}/manifests"

OCP_API="${OCP_API:-}"
OCP_TOKEN="${OCP_TOKEN:-}"
NAMESPACE="${NAMESPACE:-casino-sentinel}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-}"
# Demo defaults: insecure TLS, install operators, deploy TFM (--rhoai).
INSECURE="${INSECURE:-true}"
SKIP_OPERATORS=false
SKIP_CONSOLE=false
DEPLOY_RHOAI=true
DRY_RUN=false
ENV_FILE=""
# App images: Quay tags default to :latest.
REGISTRY="${REGISTRY:-quay.io/shailendra14k}"

log()  { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '  OK  %s\n' "$*"; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# Resolve Quay image refs. Manifests use __SENTINEL_*_IMAGE__.
resolve_app_images() {
  PYTHON_IMAGE_REF="${SENTINEL_PYTHON_IMAGE:-${REGISTRY}/sentinel-python:latest}"
  UI_IMAGE_REF="${SENTINEL_UI_IMAGE:-${REGISTRY}/sentinel-ui:latest}"
  TFM_IMAGE_REF="${SENTINEL_TFM_IMAGE:-${REGISTRY}/casino-tfm-embed:latest}"
}

usage() {
  cat <<'EOF'
Deploy Casino Sentinel (demo default: insecure + install operators + OpenShift AI TFM).

Required:
  --token=TOKEN         oc token (oc whoami -t)
  --server=URL          OpenShift API server

Aliases: --token TOKEN / --ocp_token=TOKEN
         --server URL / --ocp_api=URL / --api URL

Example (full demo stack):
  ./deploy.sh --token=sha256~... --server=https://api.cluster:6443

Optional:
  --namespace NAME      Project (default: casino-sentinel)
  --skip-operators      Do not install AMQ Streams / Console operators
  --skip-console        Do not deploy Streams Console
  --no-rhoai            Skip TFM InferenceService (CPU hashed / AI_MODE=mock)
  --secure              Require API TLS verify (default is --insecure)
  --env FILE            Optional: load vars from a file (not required for demo)
  --dry-run             Print commands only
  -h, --help

Defaults pull Quay :latest images (sentinel-python, sentinel-ui, casino-tfm-embed).
Override: SENTINEL_PYTHON_IMAGE=... SENTINEL_UI_IMAGE=... SENTINEL_TFM_IMAGE=...
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

apply_apps() {
  resolve_app_images
  log "App images: python=${PYTHON_IMAGE_REF}  ui=${UI_IMAGE_REF}"
  if [[ "${DRY_RUN}" == true ]]; then
    echo "DRY-RUN apply apps"
    return 0
  fi
  sed -e "s|__SENTINEL_PYTHON_IMAGE__|${PYTHON_IMAGE_REF}|g" \
    "${MANIFESTS}/06-apps/services.yaml" | oc apply -f -
  sed -e "s|__SENTINEL_UI_IMAGE__|${UI_IMAGE_REF}|g" \
    "${MANIFESTS}/06-apps/ui.yaml" | oc apply -f -
}

login() {
  [[ -n "${OCP_API}" ]]   || die "OpenShift API URL is required (--server= or OCP_API)."
  [[ -n "${OCP_TOKEN}" ]] || die "OpenShift token is required (--token= or OCP_TOKEN)."
  log "Logging in to ${OCP_API}"
  if [[ "${DRY_RUN}" == true ]]; then
    return 0
  fi
  if [[ "${INSECURE}" == true || "${INSECURE}" == "true" ]]; then
    oc login --token="${OCP_TOKEN}" --server="${OCP_API}" --insecure-skip-tls-verify=true >/dev/null
  else
    oc login --token="${OCP_TOKEN}" --server="${OCP_API}" >/dev/null
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

# OpenShift AI TFM: casino-tfm-embed image (NeMo + script + weights) ServingRuntime + InferenceService.
# Laptop pre-req for rebuild: model/decoder-foundation-model/ weights (gitignored; clone-nvidia-tfm.sh).
# Bake sources: model/casino-tfm-embed/{Containerfile,serve_embeddings.py} (tracked).

# Fresh 1-GPU labs often ship with llama-32-3b-instruct (or similar) already on the GPU.
# Stop other GPU InferenceServices / Deployments before scheduling tfm-embed.
free_single_gpu() {
  if [[ "${SKIP_FREE_GPU:-false}" == "true" || "${SKIP_FREE_GPU:-}" == "1" ]]; then
    log "SKIP_FREE_GPU set — not stopping other GPU workloads"
    return 0
  fi

  log "Freeing the single cluster GPU for TFM (stop other GPU model deployments first)"

  # 1) OpenShift AI / KServe InferenceServices (e.g. llama-32-3b-instruct in my-first-model)
  if oc api-resources --api-group=serving.kserve.io 2>/dev/null | grep -q InferenceService; then
    local ns name stopped
    stopped=0
    while read -r ns name; do
      [[ -z "${ns}" || -z "${name}" ]] && continue
      if [[ "${ns}" == "${NAMESPACE}" && "${name}" == "tfm-embed" ]]; then
        continue
      fi
      # Already stopped?
      local stop_ann
      stop_ann="$(oc get inferenceservice "${name}" -n "${ns}" -o jsonpath='{.metadata.annotations.serving\.kserve\.io/stop}' 2>/dev/null || true)"
      if [[ "${stop_ann}" == "true" ]]; then
        ok "InferenceService ${ns}/${name} already stopped"
        continue
      fi
      log "Stopping InferenceService ${ns}/${name} (serving.kserve.io/stop=true)"
      oc annotate inferenceservice "${name}" -n "${ns}" \
        serving.kserve.io/stop=true --overwrite >/dev/null
      stopped=$((stopped + 1))
    done < <(oc get inferenceservice -A --no-headers 2>/dev/null | awk '{print $1, $2}')
    [[ "${stopped}" -gt 0 ]] && ok "Stopped ${stopped} InferenceService(s) to free GPU"
  fi

  # 2) Deployments that request nvidia.com/gpu (skip GPU Operator / device plugin / our predictor)
  local line dns dname gpu
  while IFS=$'\t' read -r dns dname gpu; do
    [[ -z "${dns}" || -z "${dname}" ]] && continue
    [[ -z "${gpu}" || "${gpu}" == "<none>" || "${gpu}" == "0" ]] && continue
    case "${dns}" in
      nvidia-gpu-operator|gpu-operator*|openshift-*|redhat-ods-operator) continue ;;
    esac
    case "${dname}" in
      *device-plugin*|*gpu-feature-discovery*|*dcgm*|*mig-manager*|*nvidia-driver*|*node-feature*) continue ;;
      tfm-embed-predictor|tfm-embed) continue ;;
    esac
    if [[ "${dns}" == "${NAMESPACE}" && "${dname}" == tfm-embed* ]]; then
      continue
    fi
    local replicas
    replicas="$(oc get deploy "${dname}" -n "${dns}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
    if [[ "${replicas}" == "0" ]]; then
      ok "Deploy ${dns}/${dname} already scaled to 0"
      continue
    fi
    log "Scaling Deploy ${dns}/${dname} → 0 (held nvidia.com/gpu=${gpu})"
    oc scale deploy "${dname}" -n "${dns}" --replicas=0 >/dev/null || true
  done < <(
    oc get deploy -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\t"}{range .spec.template.spec.containers[*]}{.resources.requests.nvidia\.com/gpu}{" "}{end}{"\n"}{end}' 2>/dev/null \
      | sed 's/[[:space:]]*$//'
  )

  # 3) Wait until no non-TFM pods still bind nvidia.com/gpu (up to ~3 minutes)
  log "Waiting for GPU to become free for tfm-embed…"
  local i holders
  for i in $(seq 1 36); do
    holders="$(
      oc get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\t"}{.status.phase}{"\t"}{range .spec.containers[*]}{.resources.requests.nvidia\.com/gpu}{" "}{end}{"\n"}{end}' 2>/dev/null \
        | awk -F'\t' '
            $4 ~ /[1-9]/ {
              if ($1 ~ /nvidia-gpu-operator|gpu-operator/ ) next
              if ($2 ~ /^tfm-embed/) next
              if ($3 == "Succeeded" || $3 == "Failed") next
              print $1 "/" $2 " (" $3 ")"
            }'
    )"
    if [[ -z "${holders}" ]]; then
      ok "GPU is free (no competing workloads)"
      return 0
    fi
    if (( i == 1 || i % 6 == 0 )); then
      log "Still waiting; GPU held by: $(echo "${holders}" | tr '\n' ' ')"
    fi
    sleep 5
  done
  log "WARNING: GPU may still be busy — tfm-embed can stay Pending. Holders:"
  echo "${holders}"
}

ensure_ngc_secret() {
  # Optional / legacy: NeMo runtime image is quay.io/shailendra14k/nemo:25.09.01 (public).
  # NGC pull secret is not required for the demo. Kept for optional nvcr.io pulls.
  if oc get secret ngc-secret -n "${NAMESPACE}" >/dev/null 2>&1; then
    ok "Secret ngc-secret exists (unused by Quay NeMo mirror)"
    return 0
  fi
  local key="${NGC_API_KEY:-}"
  if [[ -z "${key}" ]]; then
    ok "Skipping ngc-secret (NeMo image is on Quay; NGC_API_KEY not required)"
    return 0
  fi
  log "Creating optional ngc-secret from NGC_API_KEY (legacy nvcr.io pulls)"
  oc create secret docker-registry ngc-secret \
    --docker-server=nvcr.io \
    --docker-username='$oauthtoken' \
    --docker-password="${key}" \
    -n "${NAMESPACE}"
  oc secrets link default ngc-secret --for=pull -n "${NAMESPACE}" || true
  ok "Secret ngc-secret created"
}



# Verify OpenShift AI single-model serving (KServe) is ready before applying TFM manifests.
# Fresh demo clusters often have the operator installed before CRDs finish registering.
ensure_rhoai_kserve() {
  local wait_secs="${RHOAI_WAIT_TIMEOUT:-600}"
  local auto_enable="${RHOAI_ENABLE_KSERVE:-true}"
  local dsc=""
  local state=""
  local i

  log "Checking OpenShift AI / KServe readiness (ServingRuntime + InferenceService CRDs)"

  # Prefer named DSC used on RHOAI labs; else first available
  if oc get dsc default-dsc >/dev/null 2>&1; then
    dsc="default-dsc"
  else
    dsc="$(oc get dsc -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  fi

  if [[ -z "${dsc}" ]]; then
    die "No DataScienceCluster found. Install Red Hat OpenShift AI from OperatorHub, wait for DSC, then re-run --rhoai.
  Check: oc get dsc,dscinitialization -A
  Docs: ../nvidia-tfm/OCP-1GPU.md"
  fi
  ok "DataScienceCluster: ${dsc}"

  state="$(oc get dsc "${dsc}" -o jsonpath='{.spec.components.kserve.managementState}' 2>/dev/null || true)"
  if [[ -z "${state}" ]]; then
    # Older DSC shapes / API drift — still try waiting for CRDs
    log "Could not read dsc.spec.components.kserve.managementState (continuing with CRD wait)"
  elif [[ "${state}" != "Managed" ]]; then
    log "DSC ${dsc} kserve.managementState=${state} (need Managed for single-model serving)"
    if [[ "${auto_enable}" == "true" || "${auto_enable}" == "1" ]]; then
      log "Enabling KServe: patching dsc/${dsc} kserve.managementState=Managed"
      oc patch dsc "${dsc}" --type merge \
        -p '{"spec":{"components":{"kserve":{"managementState":"Managed"}}}}' \
        || die "Failed to patch dsc/${dsc}. Enable KServe in the OpenShift AI console or patch manually."
      ok "Patched dsc/${dsc} → kserve Managed"
    else
      die "KServe is not Managed (state=${state}). Set RHOAI_ENABLE_KSERVE=true or patch:
  oc patch dsc ${dsc} --type merge -p '{\"spec\":{\"components\":{\"kserve\":{\"managementState\":\"Managed\"}}}}'"
    fi
  else
    ok "DSC ${dsc} kserve.managementState=Managed"
  fi

  # Wait for CRDs (operator may still be installing controllers on a fresh cluster)
  log "Waiting up to ${wait_secs}s for KServe CRDs…"
  for ((i = 0; i < wait_secs; i += 5)); do
    if oc get crd servingruntimes.serving.kserve.io >/dev/null 2>&1 \
       && oc get crd inferenceservices.serving.kserve.io >/dev/null 2>&1; then
      ok "CRDs ready: ServingRuntime + InferenceService"
      # Best-effort: DSC Ready (do not fail hard — CRDs are enough to apply manifests)
      local ready
      ready="$(oc get dsc "${dsc}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
      if [[ "${ready}" == "True" ]]; then
        ok "DataScienceCluster/${dsc} Ready=True"
      else
        log "DSC Ready=${ready:-unknown} (proceeding — CRDs exist)"
      fi
      return 0
    fi
    if (( i % 30 == 0 )); then
      log "Still waiting for servingruntimes.serving.kserve.io / inferenceservices… (${i}s)"
    fi
    sleep 5
  done

  die "Timed out after ${wait_secs}s waiting for KServe CRDs.
  oc get crd servingruntimes.serving.kserve.io inferenceservices.serving.kserve.io
  oc get dsc ${dsc} -o yaml | grep -A8 kserve
  oc get pods -n redhat-ods-operator
  Increase wait: RHOAI_WAIT_TIMEOUT=900 ./deploy.sh --token=... --server=..."
}

deploy_rhoai() {
  if [[ "${DEPLOY_RHOAI}" != true ]]; then
    return 0
  fi

  local rhoai_dir="${MANIFESTS}/08-openshift-ai"
  local embed_url="http://tfm-embed-predictor.${NAMESPACE}.svc:8080/v1/embeddings"

  resolve_app_images
  log "Deploying OpenShift AI TFM ServingRuntime + InferenceService (--rhoai)"
  log "  TFM image: ${TFM_IMAGE_REF}"
  [[ -d "${rhoai_dir}" ]] || die "Missing ${rhoai_dir}"

  if [[ "${DRY_RUN}" == true ]]; then
    echo "DRY-RUN ensure_rhoai_kserve (DSC kserve=Managed + wait for CRDs)"
    echo "DRY-RUN free_single_gpu (stop llama / other GPU InferenceServices + scale GPU deploys)"
    echo "DRY-RUN ServingRuntime image=${TFM_IMAGE_REF} (no PVC / ConfigMap)"
    echo "DRY-RUN oc apply ${rhoai_dir}/*.yaml (namespace=${NAMESPACE})"
    echo "DRY-RUN patch casino-config TFM_MODE=http TFM_EMBED_URL=${embed_url} AI_MODE=nvidia"
    echo "DRY-RUN rollout restart deploy/sentinel-ml-engine deploy/sentinel-api"
    return 0
  fi

  ensure_rhoai_kserve
  free_single_gpu
  ensure_ngc_secret

  if oc get deploy tfm-embed -n "${NAMESPACE}" >/dev/null 2>&1; then
    log "Deleting legacy deploy/tfm-embed (InferenceService only)"
    oc delete deploy,svc tfm-embed -n "${NAMESPACE}" --ignore-not-found || true
  fi

  # Dashboard: HARDWARE_PROFILE_NAME must match Settings → Hardware profiles (e.g. gpu-profile)
  local hwp_name="${HARDWARE_PROFILE_NAME:-gpu-profile}"
  local hwp_ns="${HARDWARE_PROFILE_NAMESPACE:-redhat-ods-applications}"

  sed -e "s/namespace: casino-sentinel/namespace: ${NAMESPACE}/g" \
      -e "s|__SENTINEL_TFM_IMAGE__|${TFM_IMAGE_REF}|g" \
    "${rhoai_dir}/servingruntime-casino-tfm-nemo.yaml" | oc apply -f -

  sed -e "s/namespace: casino-sentinel/namespace: ${NAMESPACE}/g" \
      -e "s/opendatahub.io\/hardware-profile-name: .*/opendatahub.io\/hardware-profile-name: ${hwp_name}/" \
      -e "s/opendatahub.io\/hardware-profile-namespace: .*/opendatahub.io\/hardware-profile-namespace: ${hwp_ns}/" \
    "${rhoai_dir}/inferenceservice-tfm-embed.yaml" | oc apply -f -
  ok "Hardware profile annotations: ${hwp_name} (${hwp_ns})"
  ok "ServingRuntime image: ${TFM_IMAGE_REF}"

  log "Waiting for InferenceService/tfm-embed Ready (casino-tfm-embed pull can take several minutes)"
  oc wait inferenceservice/tfm-embed -n "${NAMESPACE}" --for=condition=Ready --timeout=900s || true

  oc patch configmap casino-config -n "${NAMESPACE}" --type merge -p "{
    \"data\": {
      \"TFM_MODE\": \"http\",
      \"TFM_EMBED_URL\": \"${embed_url}\",
      \"TFM_CHECKPOINT\": \"\",
      \"AI_MODE\": \"nvidia\"
    }
  }"
  # ConfigMap env is only read at process start — restart consumers of TFM_* / AI_MODE.
  log "Restarting sentinel-ml-engine + sentinel-api to pick up TFM_MODE=http / AI_MODE=nvidia"
  oc rollout restart deploy/sentinel-ml-engine deploy/sentinel-api -n "${NAMESPACE}" || true
  oc rollout status deploy/sentinel-ml-engine -n "${NAMESPACE}" --timeout=180s || true
  oc rollout status deploy/sentinel-api -n "${NAMESPACE}" --timeout=180s || true
  ok "RHOAI TFM cutover: TFM_EMBED_URL=${embed_url} (ml-engine + api restarted)"
}

print_summary() {
  [[ "${DRY_RUN}" == true ]] && return 0
  resolve_app_images
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
  echo "Images:           ${PYTHON_IMAGE_REF} / ${UI_IMAGE_REF}"
  echo "TFM:              TFM_MODE/AI_MODE on casino-config (default hashed/mock). GPU: TFM_MODE=http + TFM_EMBED_URL"
  if [[ "${DEPLOY_RHOAI}" == true ]]; then
    echo "RHOAI TFM:        ${TFM_IMAGE_REF} (ServingRuntime casino-tfm-nemo + InferenceService tfm-embed)"
    echo "TFM_EMBED_URL:    http://tfm-embed-predictor.${NAMESPACE}.svc:8080/v1/embeddings"
    oc get servingruntime,inferenceservice -n "${NAMESPACE}" 2>/dev/null || true
  fi
  echo
  echo "Demo: open the UI → Demo Controls / Start Scenario (structuring / rapid-wire / mixed)"
}

# Optional --env FILE first (advanced); demo path uses --server / --token only.
_pre=()
(($#)) && _pre=("$@")
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
    --server=*)           OCP_API="${1#*=}"; shift ;;
    --token=*)            OCP_TOKEN="${1#*=}"; shift ;;
    --ocp_api=*)          OCP_API="${1#*=}"; shift ;;
    --ocp_token=*)        OCP_TOKEN="${1#*=}"; shift ;;
    --server|--ocp_api|--api)     OCP_API="$2"; shift 2 ;;
    --token|--ocp_token)          OCP_TOKEN="$2"; shift 2 ;;
    --namespace)          NAMESPACE="$2"; shift 2 ;;
    --env)                ENV_FILE="$2"; shift 2 ;;
    --insecure)           INSECURE=true; shift ;;
    --secure)             INSECURE=false; shift ;;
    --skip-operators)     SKIP_OPERATORS=true; shift ;;
    --install-operators)  SKIP_OPERATORS=false; shift ;;
    --skip-console)       SKIP_CONSOLE=true; shift ;;
    --rhoai)              DEPLOY_RHOAI=true; shift ;;
    --no-rhoai)           DEPLOY_RHOAI=false; shift ;;
    --dry-run)            DRY_RUN=true; shift ;;
    -h|--help)            usage; exit 0 ;;
    *)                    die "Unknown argument: $1" ;;
  esac
done

[[ -n "${OCP_API}" ]]   || die "Missing --server=URL (required). See --help"
[[ -n "${OCP_TOKEN}" ]] || die "Missing --token=TOKEN (required). See --help"

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
resolve_app_images
ok "App images from Quay (no sentinel-python / sentinel-ui BuildConfig)"
log "  python: ${PYTHON_IMAGE_REF}"
log "  ui:     ${UI_IMAGE_REF}"

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

apply_apps
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