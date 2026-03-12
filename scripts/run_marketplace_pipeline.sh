#!/usr/bin/env bash
# Convenience script to execute the marketplace synchronisation followed by the
# smoke tests. Environment variables can be used to customise behaviour.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CHANNEL_ENV="${FABRIC_CHANNEL:-}"
CHAINCODE_ENV="${CHAINCODE_NAME:-}"
BASE_URL="${MARKETPLACE_BASE_URL:-http://localhost:8085/api}"
TOKEN="${MARKETPLACE_JWT:-}"
ORG_OVERRIDE="${MARKETPLACE_ORG_OVERRIDE:-}"
RESTART_CMD="${MARKETPLACE_RELOAD_CMD:-}"
RUN_SMOKE_TESTS="${RUN_SMOKE_TESTS:-1}"
PATIENT_KEY="${SMOKE_PATIENT_KEY:-patient:387c97eb-cf0b-5cdb-96d3-38d8408922bc}"
BLUEPRINT_LINT_FILES="${BLUEPRINT_LINT_FILES:-}"
PIPELINE_A1_3_EVIDENCE_DIR="${PIPELINE_A1_3_EVIDENCE_DIR:-}"

# Optional overrides for mutation payloads
CREATE_PROTO="${SMOKE_CREATE_PROTO:-123}"
UPDATE_PROTO="${SMOKE_UPDATE_PROTO:-11856}"
DOCTOR_HASH="${SMOKE_DOCTOR_HASH:-doctor:bd2f37f3-00e8-5ebc-aa98-652bef83858fd}"
PATIENT_HASH_UPDATE="${SMOKE_PATIENT_HASH:-patient:9d6f96f4-e796-5a56-a010-06812033b03bd}"
DELETE_REF="${SMOKE_DELETE_REF:-exam::11856}"

if [[ -z "${BLUEPRINT_LINT_FILES}" ]]; then
  echo "[ERROR] BLUEPRINT_LINT_FILES não informado." >&2
  echo "O gate de blueprint é pré-condição obrigatória antes do runbook." >&2
  echo "Exemplo: BLUEPRINT_LINT_FILES='path/a.yaml path/b.json'" >&2
  exit 2
fi

if [[ -z "${PIPELINE_A1_3_EVIDENCE_DIR}" ]]; then
  echo "[ERROR] PIPELINE_A1_3_EVIDENCE_DIR não informado." >&2
  echo "O gate do pipeline A1.3 é pré-condição obrigatória antes do runbook." >&2
  echo "Exemplo: PIPELINE_A1_3_EVIDENCE_DIR='/tmp/pipeline-a1.3/run-123'" >&2
  exit 2
fi

echo "==> Running mandatory blueprint lint gate"
# shellcheck disable=SC2206
BLUEPRINT_FILES_ARRAY=(${BLUEPRINT_LINT_FILES})
"${REPO_ROOT}/scripts/validate_blueprints_ci.sh" "${BLUEPRINT_FILES_ARRAY[@]}"

echo "==> Running mandatory A1.3 pipeline evidence gate"
"${REPO_ROOT}/scripts/validate_pipeline_a1_3_ci.sh" "${PIPELINE_A1_3_EVIDENCE_DIR}"

echo "==> Running marketplace sync"
SYNC_ARGS=(
  "--base-url" "${BASE_URL}"
  "--output-dir" "${REPO_ROOT}/marketplace"
)

if [[ -n "${CHANNEL_ENV}" ]]; then
  SYNC_ARGS+=("--channel" "${CHANNEL_ENV}")
fi
if [[ -n "${CHAINCODE_ENV}" ]]; then
  SYNC_ARGS+=("--chaincode" "${CHAINCODE_ENV}")
fi
if [[ -n "${TOKEN}" ]]; then
  SYNC_ARGS+=("--token" "${TOKEN}")
fi
if [[ -n "${ORG_OVERRIDE}" ]]; then
  SYNC_ARGS+=("--org" "${ORG_OVERRIDE}")
fi
if [[ -n "${RESTART_CMD}" ]]; then
  SYNC_ARGS+=("--restart-cmd" "${RESTART_CMD}")
fi

python3 "${REPO_ROOT}/scripts/marketplace_sync.py" "${SYNC_ARGS[@]}"

if [[ "${RUN_SMOKE_TESTS}" == "0" ]]; then
  echo "SMOKE TESTS SKIPPED (RUN_SMOKE_TESTS=0)"
  exit 0
fi

echo "==> Running API smoke tests"
SMOKE_ARGS=(
  "--base-url" "${BASE_URL}"
  "--patient-key" "${PATIENT_KEY}"
  "--create-num-protocolo" "${CREATE_PROTO}"
  "--update-num-protocolo" "${UPDATE_PROTO}"
  "--doctor-hash" "${DOCTOR_HASH}"
  "--patient-hash" "${PATIENT_HASH_UPDATE}"
  "--delete-asset-ref" "${DELETE_REF}"
)

# quando múltiplas combinações existem, escolher a primeira gerada
DEFAULT_MANIFEST=$(ls "${REPO_ROOT}"/marketplace/*__*_templates.json 2>/dev/null | head -n 1)
if [[ -z "${DEFAULT_MANIFEST}" ]]; then
  echo "Nenhum manifesto encontrado em marketplace/. Os testes não podem prosseguir." >&2
  exit 1
fi
SMOKE_CHANNEL=$(basename "${DEFAULT_MANIFEST}" | sed 's/_templates\.json$//' | cut -d'_' -f1)
SMOKE_CHAINCODE=$(basename "${DEFAULT_MANIFEST}" | sed 's/_templates\.json$//' | cut -d'_' -f3-)
SMOKE_CHAINCODE="${SMOKE_CHAINCODE#__}"
SMOKE_MANIFEST="${DEFAULT_MANIFEST/_templates.json/.json}"

SMOKE_ARGS+=("--manifest" "${SMOKE_MANIFEST}")
SMOKE_ARGS+=("--channel" "${SMOKE_CHANNEL}")
SMOKE_ARGS+=("--chaincode" "${SMOKE_CHAINCODE}")

if [[ -n "${CHANNEL_ENV}" ]]; then
  SMOKE_ARGS+=("--channel" "${CHANNEL_ENV}")
fi
if [[ -n "${CHAINCODE_ENV}" ]]; then
  SMOKE_ARGS+=("--chaincode" "${CHAINCODE_ENV}")
fi
if [[ -n "${TOKEN}" ]]; then
  SMOKE_ARGS+=("--token" "${TOKEN}")
fi
if [[ -n "${ORG_OVERRIDE}" ]]; then
  SMOKE_ARGS+=("--org" "${ORG_OVERRIDE}")
fi

python3 "${REPO_ROOT}/scripts/api_smoke_test.py" "${SMOKE_ARGS[@]}"
