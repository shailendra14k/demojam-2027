#!/usr/bin/env bash
# Tear down Casino Sentinel (demo default: delete namespace + uninstall AMQ Streams operators).
#   ./destroy.sh --token=sha256~... --server=https://api...:6443

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OCP_API="${OCP_API:-}"
OCP_TOKEN="${OCP_TOKEN:-}"
NAMESPACE="${NAMESPACE:-casino-sentinel}"
INSECURE="${INSECURE:-true}"
DELETE_NAMESPACE=true
UNINSTALL_OPERATORS=true
DRY_RUN=false
ENV_FILE=""

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()  { printf '  OK  %s\n' "$*"; }

usage() {
  cat <<'EOF'
Destroy Casino Sentinel demo stack.

Required:
  --token=TOKEN         oc token (oc whoami -t)
  --server=URL          OpenShift API server

Aliases: --token TOKEN / --ocp_token=TOKEN
         --server URL / --ocp_api=URL / --api URL

Example:
  ./destroy.sh --token=sha256~... --server=https://api.cluster:6443

Defaults:
  - insecure TLS skip
  - delete namespace (all demo resources)
  - uninstall AMQ Streams + Streams Console operator Subscriptions (openshift-operators)

Optional:
  --namespace NAME         Project (default: casino-sentinel)
  --keep-namespace         Leave the namespace (delete resources inside only)
  --keep-operators         Do not remove AMQ Streams / Console Subscriptions
  --secure                 Require API TLS verify
  --env FILE               Optional vars file (not required)
  --dry-run                Print actions only
  -h, --help
EOF
}

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
    --delete-namespace)   DELETE_NAMESPACE=true; shift ;;
    --keep-namespace)     DELETE_NAMESPACE=false; shift ;;
    --keep-operators)     UNINSTALL_OPERATORS=false; shift ;;
    --dry-run)            DRY_RUN=true; shift ;;
    -h|--help)            usage; exit 0 ;;
    *)                    die "Unknown argument: $1" ;;
  esac
done

[[ -n "${OCP_API}" ]]   || die "Missing --server=URL (required). See --help"
[[ -n "${OCP_TOKEN}" ]] || die "Missing --token=TOKEN (required). See --help"
command -v oc >/dev/null 2>&1 || die "oc CLI not found."

if [[ "${DRY_RUN}" == true ]]; then
  echo "DRY-RUN login ${OCP_API} (insecure=${INSECURE})"
  echo "DRY-RUN delete resources in ${NAMESPACE} (delete_namespace=${DELETE_NAMESPACE})"
  echo "DRY-RUN uninstall operators=${UNINSTALL_OPERATORS}"
  exit 0
fi

log "Logging in to ${OCP_API}"
if [[ "${INSECURE}" == true || "${INSECURE}" == "true" ]]; then
  oc login --token="${OCP_TOKEN}" --server="${OCP_API}" --insecure-skip-tls-verify=true >/dev/null
else
  oc login --token="${OCP_TOKEN}" --server="${OCP_API}" >/dev/null
fi

log "Deleting Casino Sentinel resources in ${NAMESPACE}"
oc delete inferenceservice --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete servingruntime --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete consoles.console.streamshub.github.com --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete kafkauser,kafkaconnector,kafkaconnect,kafkatopic,kafka,kafkanodepool --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete deploy,svc,route,bc,is,job,pod -l app.kubernetes.io/part-of=casino-sentinel -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete deploy,svc,route,bc,is,job --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete pvc --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete configmap,secret -l app.kubernetes.io/part-of=casino-sentinel -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
ok "Namespace resources deleted (or ignored if missing)"

if [[ "${DELETE_NAMESPACE}" == true ]]; then
  log "Deleting namespace ${NAMESPACE}"
  oc delete namespace "${NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null || true
  ok "Namespace ${NAMESPACE} delete requested"
else
  log "Namespace ${NAMESPACE} kept (--keep-namespace)"
fi

uninstall_operator() {
  local sub_name="$1"
  local ns="${2:-openshift-operators}"
  log "Uninstalling operator Subscription ${sub_name} (${ns})"
  local csv
  csv="$(oc get subscription "${sub_name}" -n "${ns}" -o jsonpath='{.status.installedCSV}' 2>/dev/null || true)"
  oc delete subscription "${sub_name}" -n "${ns}" --ignore-not-found 2>/dev/null || true
  if [[ -n "${csv}" ]]; then
    oc delete csv "${csv}" -n "${ns}" --ignore-not-found 2>/dev/null || true
    ok "Removed Subscription ${sub_name} + CSV ${csv}"
  else
    oc get csv -n "${ns}" -o name 2>/dev/null \
      | grep -E "amqstreams|amq-streams" \
      | xargs -r oc delete -n "${ns}" --ignore-not-found 2>/dev/null || true
    ok "Removed Subscription ${sub_name} (CSV best-effort)"
  fi
}

if [[ "${UNINSTALL_OPERATORS}" == true ]]; then
  uninstall_operator amq-streams-console openshift-operators
  uninstall_operator amq-streams openshift-operators
else
  log "Operators kept (--keep-operators)"
fi

log "Done."
echo "Destroy complete (namespace_delete=${DELETE_NAMESPACE} operators_removed=${UNINSTALL_OPERATORS})."
