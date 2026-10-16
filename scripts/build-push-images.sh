#!/usr/bin/env bash
# Build Casino Sentinel images (linux/amd64) and push to Quay.
# Tag defaults to the short git commit of demojam-2027/ (override with TAG= or --tag).
#
# Prerequisites: podman or docker, logged into Quay
#   podman login quay.io
#
# Usage:
#   ./scripts/build-push-images.sh              # python + ui only (does NOT build TFM)
#   ./scripts/build-push-images.sh --python-only
#   ./scripts/build-push-images.sh --ui-only
#   ./scripts/build-push-images.sh --tfm        # also casino-tfm-embed (opt-in; NeMo + weights)
#   ./scripts/build-push-images.sh --tfm-only   # only casino-tfm-embed
#   TAG=latest ./scripts/build-push-images.sh --tfm-only
#   REGISTRY=quay.io/myorg ./scripts/build-push-images.sh
#
# TFM sources: model/decoder-foundation-model/{Containerfile,serve_embeddings.py,weights}
# (gitignored). Deploy --rhoai defaults to casino-tfm-embed:latest.
#
# Then deploy:
#   ./deploy.sh --env ./.env --insecure
#   ./deploy.sh --env ./.env --insecure --rhoai

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REGISTRY="${REGISTRY:-quay.io/shailendra14k}"
PLATFORM="${PLATFORM:-linux/amd64}"
BUILD_PYTHON=true
BUILD_UI=true
BUILD_TFM=false

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '==> %s\n' "$*"; }

short_commit() {
  git -C "${ROOT}" rev-parse --short HEAD 2>/dev/null \
    || die "Cannot resolve short commit (run from a git checkout of demojam-2027)"
}

TAG="${TAG:-$(short_commit)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-only) BUILD_UI=false; BUILD_TFM=false; shift ;;
    --ui-only)     BUILD_PYTHON=false; BUILD_TFM=false; shift ;;
    --tfm)         BUILD_TFM=true; shift ;;
    --tfm-only)    BUILD_PYTHON=false; BUILD_UI=false; BUILD_TFM=true; shift ;;
    --registry)    REGISTRY="$2"; shift 2 ;;
    --tag)         TAG="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,22p' "$0"
      exit 0
      ;;
    *) die "Unknown argument: $1" ;;
  esac
done

if command -v podman >/dev/null 2>&1; then
  CTR=podman
elif command -v docker >/dev/null 2>&1; then
  CTR=docker
else
  die "Need podman or docker"
fi

PYTHON_IMAGE="${REGISTRY}/sentinel-python:${TAG}"
UI_IMAGE="${REGISTRY}/sentinel-ui:${TAG}"
TFM_IMAGE="${REGISTRY}/casino-tfm-embed:${TAG}"

build_push() {
  local context="$1" file="$2" image="$3"
  log "Building ${image} (${PLATFORM}) from ${file}"
  if [[ "${CTR}" == podman ]]; then
    podman build --platform="${PLATFORM}" -f "${file}" -t "${image}" "${context}"
    log "Pushing ${image}"
    podman push "${image}"
  else
    docker buildx build --platform="${PLATFORM}" -f "${file}" -t "${image}" --push "${context}"
  fi
  printf 'OK  %s\n' "${image}"
}

[[ "${BUILD_PYTHON}" == true ]] && \
  build_push "${ROOT}" "${ROOT}/services/python/Containerfile" "${PYTHON_IMAGE}"

[[ "${BUILD_UI}" == true ]] && \
  build_push "${ROOT}/ui" "${ROOT}/ui/Containerfile" "${UI_IMAGE}"

if [[ "${BUILD_TFM}" == true ]]; then
  TFM_DIR="${ROOT}/model/decoder-foundation-model"
  CKPT="${TFM_DIR}/model-00001-of-00001.safetensors"
  [[ -f "${TFM_DIR}/Containerfile" ]] || die "Missing ${TFM_DIR}/Containerfile"
  [[ -f "${TFM_DIR}/serve_embeddings.py" ]] || die "Missing ${TFM_DIR}/serve_embeddings.py"
  [[ -f "${CKPT}" ]] || die "Missing ${CKPT} (needed to bake casino-tfm-embed)"
  sz="$(wc -c < "${CKPT}" | tr -d ' ')"
  [[ "${sz}" -gt 1000000 ]] || die "${CKPT} looks truncated (${sz} bytes)"
  build_push "${TFM_DIR}" "${TFM_DIR}/Containerfile" "${TFM_IMAGE}"
fi

echo
echo "Pushed (tag=${TAG}):"
[[ "${BUILD_PYTHON}" == true ]] && echo "  ${PYTHON_IMAGE}"
[[ "${BUILD_UI}" == true ]] && echo "  ${UI_IMAGE}"
[[ "${BUILD_TFM}" == true ]] && echo "  ${TFM_IMAGE}"
echo
echo "Deploy with the same commit (deploy.sh defaults to this short SHA):"
echo "  ./deploy.sh --env ./.env --insecure"
if [[ "${BUILD_TFM}" == true ]]; then
  echo "  ./deploy.sh --env ./.env --insecure --rhoai"
  echo "Or pin TFM image:"
  echo "  SENTINEL_TFM_IMAGE=${TFM_IMAGE} ./deploy.sh --env ./.env --insecure --rhoai"
fi
echo "Or pin app images:"
echo "  SENTINEL_PYTHON_IMAGE=${PYTHON_IMAGE} SENTINEL_UI_IMAGE=${UI_IMAGE} \\"
echo "    ./deploy.sh --env ./.env --insecure"
