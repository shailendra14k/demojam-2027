#!/usr/bin/env bash
# Tear down Casino Sentinel. Leaves the AMQ Streams operator installed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OCP_API="${OCP_API:-}"
OCP_TOKEN="${OCP_TOKEN:-}"
NAMESPACE="${NAMESPACE:-casino-sentinel}"
INSECURE="${INSECURE:-false}"
DELETE_NAMESPACE=false
ENV_FILE=""

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

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
    --api)               OCP_API="$2"; shift 2 ;;
    --token)             OCP_TOKEN="$2"; shift 2 ;;
    --namespace)         NAMESPACE="$2"; shift 2 ;;
    --env)               ENV_FILE="$2"; shift 2 ;;
    --insecure)          INSECURE=true; shift ;;
    --delete-namespace)  DELETE_NAMESPACE=true; shift ;;
    -h|--help)
      echo "Usage: ./destroy.sh --env ./env [--delete-namespace]"
      exit 0
      ;;
    *) die "Unknown argument: $1" ;;
  esac
done

[[ -n "${OCP_API}" && -n "${OCP_TOKEN}" ]] || die "Need --api/--token or --env"
if [[ "${INSECURE}" == true || "${INSECURE}" == "true" ]]; then
  oc login --server="${OCP_API}" --token="${OCP_TOKEN}" --insecure-skip-tls-verify=true >/dev/null
else
  oc login --server="${OCP_API}" --token="${OCP_TOKEN}" >/dev/null
fi

echo "Deleting Casino Sentinel resources in ${NAMESPACE}"
oc delete consoles.console.streamshub.github.com --all -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
oc delete kafkauser,kafkaconnector,kafkaconnect,kafkatopic,kafka,kafkanodepool --all -n "${NAMESPACE}" --ignore-not-found
oc delete deploy,svc,route,bc,is,pvc,secret,configmap -l app.kubernetes.io/part-of=casino-sentinel -n "${NAMESPACE}" --ignore-not-found || true
oc delete deploy,svc,route,bc,is --all -n "${NAMESPACE}" --ignore-not-found
oc delete pvc --all -n "${NAMESPACE}" --ignore-not-found

if [[ "${DELETE_NAMESPACE}" == true ]]; then
  oc delete namespace "${NAMESPACE}" --ignore-not-found
  echo "Namespace ${NAMESPACE} deleted"
else
  echo "Namespace ${NAMESPACE} kept (add --delete-namespace to remove it)"
fi
