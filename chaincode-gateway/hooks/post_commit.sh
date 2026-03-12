#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CHANNEL="${FABRIC_CHANNEL:-}"
CHAINCODE="${CHAINCODE_NAME:-}"
BASE_URL="${MARKETPLACE_BASE_URL:-http://localhost:8085/api}"
TOKEN="${MARKETPLACE_JWT:-}"
ORG_OVERRIDE="${MARKETPLACE_ORG_OVERRIDE:-}"
RESTART_CMD="${MARKETPLACE_RELOAD_CMD:-}"

ARGS=(
  "--base-url" "${BASE_URL}"
  "--output-dir" "${REPO_ROOT}/marketplace"
)

if [[ -n "${CHANNEL}" ]]; then
  ARGS+=("--channel" "${CHANNEL}")
fi
if [[ -n "${CHAINCODE}" ]]; then
  ARGS+=("--chaincode" "${CHAINCODE}")
fi
if [[ -n "${TOKEN}" ]]; then
  ARGS+=("--token" "${TOKEN}")
fi

if [[ -n "${ORG_OVERRIDE}" ]]; then
  ARGS+=("--org" "${ORG_OVERRIDE}")
fi

if [[ -n "${RESTART_CMD}" ]]; then
  ARGS+=("--restart-cmd" "${RESTART_CMD}")
fi

python3 "${REPO_ROOT}/scripts/marketplace_sync.py" "${ARGS[@]}"
