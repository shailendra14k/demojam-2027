#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="casino-sentinel"
SCENARIO="structuring"
API_URL="${API_URL:-}"
# Fresh seed by default so CDC inserts always happen.
# Fixed --seed 42 is idempotent after the first successful run (same transaction UUIDs).
SEED="${SEED:-}"
# Client-side poll after API wait (covers residual CDC lag + empty edge replies).
POLL_SECONDS="${DEMO_POLL_SECONDS:-90}"
# Keep under ~60s edge cutoffs; API default is also 45.
WAIT_SECONDS="${DEMO_WAIT_SECONDS:-45}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --api-url) API_URL="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: ./demo.sh [--scenario name] [--project name] [--api-url url] [--seed N]"
      echo "Scenarios: normal structuring minimal_gaming rapid_withdrawal shared_device behavioral_deviation"
      echo "Seed: defaults to epoch seconds (fresh CDC inserts). Use --seed 42 for reproducible/idempotent runs."
      exit 0 ;;
    *) shift ;;
  esac
done
SEED="${SEED:-$(date +%s)}"

PUBLIC_API_URL=""
if [[ -z "$API_URL" ]] && command -v oc >/dev/null 2>&1 && oc whoami >/dev/null 2>&1; then
  HOST=$(oc -n "$PROJECT" get route sentinel-api -o jsonpath='{.spec.host}' 2>/dev/null || true)
  if [[ -n "$HOST" ]]; then
    PUBLIC_API_URL="https://${HOST}"
    API_URL="$PUBLIC_API_URL"
  fi
fi
API_URL="${API_URL:-http://127.0.0.1:8080}"

PF_PID=""
cleanup_pf() {
  if [[ -n "$PF_PID" ]] && kill -0 "$PF_PID" 2>/dev/null; then
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
  fi
}
trap cleanup_pf EXIT

ensure_api_reachable() {
  local url="$1"
  local code
  code=$(curl -sk --max-time 10 -o /dev/null -w "%{http_code}" "$url/health" || true)
  [[ "$code" == "200" ]]
}

poll_open_alerts() {
  local url="$1"
  local budget="$2"
  local deadline=$((SECONDS + budget))
  local alerts_json=""
  while (( SECONDS < deadline )); do
    alerts_json=$(curl -sk --max-time 15 "$url/api/v1/alerts?status=OPEN" || true)
    if echo "$alerts_json" | python3 -c "import sys,json; d=json.load(sys.stdin); items=d.get('items') or d.get('alerts') or (d if isinstance(d,list) else []); raise SystemExit(0 if items else 1)" 2>/dev/null; then
      echo "$alerts_json"
      return 0
    fi
    sleep 3
  done
  return 1
}

# Prefer public route; fall back to port-forward only if route health fails.
if [[ -n "$PUBLIC_API_URL" ]] && ! ensure_api_reachable "$PUBLIC_API_URL"; then
  echo "WARN: public API route unhealthy — falling back to oc port-forward"
  oc -n "$PROJECT" port-forward svc/sentinel-api 18000:8080 >/dev/null 2>&1 &
  PF_PID=$!
  sleep 2
  API_URL="http://127.0.0.1:18000"
  if ! ensure_api_reachable "$API_URL"; then
    echo "ERROR: API unreachable via route and port-forward"
    exit 1
  fi
fi

SCENARIO_URL="$API_URL/api/v1/demo/scenarios/${SCENARIO}?seed=${SEED}&wait_seconds=${WAIT_SECONDS}"
echo "Running scenario='$SCENARIO' seed=$SEED wait=${WAIT_SECONDS}s via $API_URL"
SCENARIO_OUT=$(curl -sk --max-time $((WAIT_SECONDS + 20)) -X POST "$SCENARIO_URL" || true)

if [[ -z "$SCENARIO_OUT" ]]; then
  echo "WARN: empty reply (edge may have cut the connection). DB write usually already happened — polling alerts…"
  if ALERTS_JSON=$(poll_open_alerts "$API_URL" "$POLL_SECONDS"); then
    echo '{"alerts_generated":1,"timed_out":true,"message":"empty HTTP body; alerts observed via poll"}' | python3 -m json.tool
    echo ""
    echo "Open alerts:"
    echo "$ALERTS_JSON" | python3 -m json.tool | head -80
    exit 0
  fi
  if [[ -n "$PUBLIC_API_URL" && "$API_URL" == "$PUBLIC_API_URL" ]] && command -v oc >/dev/null 2>&1; then
    echo "Falling back to oc port-forward…"
    oc -n "$PROJECT" port-forward svc/sentinel-api 18000:8080 >/dev/null 2>&1 &
    PF_PID=$!
    sleep 2
    API_URL="http://127.0.0.1:18000"
    SEED=$(date +%s)
    SCENARIO_URL="$API_URL/api/v1/demo/scenarios/${SCENARIO}?seed=${SEED}&wait_seconds=${WAIT_SECONDS}"
    echo "Retrying scenario='$SCENARIO' seed=$SEED via $API_URL"
    SCENARIO_OUT=$(curl -sk --max-time $((WAIT_SECONDS + 20)) -X POST "$SCENARIO_URL" || true)
  fi
fi

if [[ -z "$SCENARIO_OUT" ]]; then
  echo "ERROR: empty scenario response and no alerts observed"
  exit 1
fi
echo "$SCENARIO_OUT" | python3 -m json.tool
ALERTS_N=$(echo "$SCENARIO_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d.get('alerts_generated') or 0))" 2>/dev/null || echo 0)
if [[ "$ALERTS_N" -eq 0 ]]; then
  echo ""
  echo "API returned alerts_generated=0 — polling /api/v1/alerts for up to ${POLL_SECONDS}s…"
  if ALERTS_JSON=$(poll_open_alerts "$API_URL" "$POLL_SECONDS"); then
    echo "Alerts appeared:"
    echo "$ALERTS_JSON" | python3 -m json.tool | head -80
    exit 0
  fi
  echo "WARN: no alerts within ${POLL_SECONDS}s after scenario (CDC may still be catching up)."
  exit 1
fi
echo ""
echo "Open alerts:"
curl -sk "$API_URL/api/v1/alerts?status=OPEN" | python3 -m json.tool | head -80
