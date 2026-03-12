#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Uso: $0 <pipeline_evidence_dir>" >&2
  exit 2
fi

EVIDENCE_DIR="$1"
PIPELINE_REPORT_FILE="${EVIDENCE_DIR}/pipeline-report.json"
STAGE_REPORTS_DIR="${EVIDENCE_DIR}/stage-reports"
INVENTORY_FINAL_FILE="${EVIDENCE_DIR}/inventory-final.json"
HISTORY_FILE="${EVIDENCE_DIR}/history.jsonl"
DECISION_TRACE_FILE="${EVIDENCE_DIR}/decision-trace.jsonl"

if [[ ! -d "${EVIDENCE_DIR}" ]]; then
  echo "[ERROR] Diretório de evidências do pipeline não encontrado: ${EVIDENCE_DIR}" >&2
  exit 1
fi

if [[ ! -s "${PIPELINE_REPORT_FILE}" ]]; then
  echo "[ERROR] pipeline-report.json ausente ou vazio em ${EVIDENCE_DIR}" >&2
  exit 1
fi
if [[ ! -d "${STAGE_REPORTS_DIR}" ]]; then
  echo "[ERROR] stage-reports/ ausente em ${EVIDENCE_DIR}" >&2
  exit 1
fi
if [[ ! -s "${INVENTORY_FINAL_FILE}" ]]; then
  echo "[ERROR] inventory-final.json ausente ou vazio em ${EVIDENCE_DIR}" >&2
  exit 1
fi
if [[ ! -s "${HISTORY_FILE}" ]]; then
  echo "[ERROR] history.jsonl ausente ou vazio em ${EVIDENCE_DIR}" >&2
  exit 1
fi

for stage_report in prepare-report.json provision-report.json configure-report.json verify-report.json; do
  if [[ ! -s "${STAGE_REPORTS_DIR}/${stage_report}" ]]; then
    echo "[ERROR] stage-reports/${stage_report} ausente ou vazio." >&2
    exit 1
  fi
done

if [[ ! -s "${DECISION_TRACE_FILE}" ]]; then
  echo "[ERROR] decision-trace.jsonl ausente ou vazio em ${EVIDENCE_DIR}" >&2
  exit 1
fi

for crypto_artifact in \
  "configure/crypto-inventory.json" \
  "configure/crypto-rotation-report.json" \
  "configure/crypto-revocation-report.json"; do
  if [[ ! -s "${EVIDENCE_DIR}/${crypto_artifact}" ]]; then
    echo "[ERROR] evidência criptográfica mínima ausente: ${crypto_artifact}" >&2
    exit 1
  fi
done

python3 - <<PY
import json
from pathlib import Path

pipeline_report = Path(${PIPELINE_REPORT_FILE@Q})
history_file = Path(${HISTORY_FILE@Q})
decision_trace_file = Path(${DECISION_TRACE_FILE@Q})

report = json.loads(pipeline_report.read_text(encoding="utf-8"))
stage_reports_dir = Path(${STAGE_REPORTS_DIR@Q})
inventory_final_file = Path(${INVENTORY_FINAL_FILE@Q})

prepare_report = json.loads((stage_reports_dir / "prepare-report.json").read_text(encoding="utf-8"))
provision_report = json.loads((stage_reports_dir / "provision-report.json").read_text(encoding="utf-8"))
configure_report = json.loads((stage_reports_dir / "configure-report.json").read_text(encoding="utf-8"))
verify_report = json.loads((stage_reports_dir / "verify-report.json").read_text(encoding="utf-8"))
inventory_final = json.loads(inventory_final_file.read_text(encoding="utf-8"))

required_fields = [
    "run_id",
    "change_id",
  "valid",
  "errors",
  "warnings",
  "hints",
    "fingerprint_sha256",
    "decision",
    "decision_reasons",
    "evidence_valid",
    "required_artifacts",
    "generated_at",
]
missing = [field for field in required_fields if field not in report]
if missing:
    raise SystemExit("pipeline-report.json sem contrato mínimo obrigatório: " + ", ".join(missing))

if not isinstance(report.get("decision_reasons"), list):
    raise SystemExit("Campo decision_reasons deve ser lista.")
if not isinstance(report.get("errors"), list):
  raise SystemExit("Campo errors deve ser lista.")
if not isinstance(report.get("warnings"), list):
  raise SystemExit("Campo warnings deve ser lista.")
if not isinstance(report.get("hints"), list):
  raise SystemExit("Campo hints deve ser lista.")

if report.get("decision") != "allow":
    raise SystemExit("Gate do pipeline bloqueado: decision != allow.")

if report.get("evidence_valid") is not True:
    raise SystemExit("Gate do pipeline bloqueado: evidence_valid != true.")
if report.get("valid") is not True:
  raise SystemExit("Gate do pipeline bloqueado: valid != true.")

prepare_summary = prepare_report.get("precondition_summary") if isinstance(prepare_report.get("precondition_summary"), dict) else {}
if prepare_summary.get("crypto_preconditions_valid") is not True:
  raise SystemExit("Gate do pipeline bloqueado: prepare sem pré-condições criptográficas válidas.")

provision_crypto_preconditions = provision_report.get("crypto_preconditions") if isinstance(provision_report.get("crypto_preconditions"), dict) else {}
if provision_crypto_preconditions.get("valid") is not True:
  raise SystemExit("Gate do pipeline bloqueado: provision sem pré-condições criptográficas válidas.")

configure_crypto_preconditions = configure_report.get("crypto_preconditions") if isinstance(configure_report.get("crypto_preconditions"), dict) else {}
if configure_crypto_preconditions.get("valid") is not True:
  raise SystemExit("Gate do pipeline bloqueado: configure sem pré-condições criptográficas válidas.")

for section in ("crypto_materialization", "crypto_inventory", "crypto_rotation", "crypto_revocation"):
  if not isinstance(configure_report.get(section), dict):
    raise SystemExit(f"configure-report.json sem seção criptográfica obrigatória: {section}")

if not isinstance(verify_report.get("crypto_inventory_consistency"), list):
  raise SystemExit("verify-report.json sem crypto_inventory_consistency.")
if not isinstance(verify_report.get("crypto_revocation_consistency"), list):
  raise SystemExit("verify-report.json sem crypto_revocation_consistency.")

inventory_payload = inventory_final.get("inventory") if isinstance(inventory_final.get("inventory"), dict) else {}
crypto_inventory = inventory_payload.get("crypto_inventory") if isinstance(inventory_payload.get("crypto_inventory"), dict) else {}
required_crypto_fields = [
  "contract_version",
  "run_id",
  "change_id",
  "blueprint_fingerprint",
  "resolved_schema_version",
  "inventory_fingerprint",
  "entities",
  "links",
]
missing_crypto_fields = [field for field in required_crypto_fields if field not in crypto_inventory]
if missing_crypto_fields:
  raise SystemExit(
    "inventory-final.json sem contrato mínimo de crypto_inventory: " + ", ".join(missing_crypto_fields)
  )

if str(crypto_inventory.get("contract_version", "")).strip() != "1.0.0":
  raise SystemExit("Regressão de contrato: crypto_inventory.contract_version != 1.0.0")

if len(str(crypto_inventory.get("inventory_fingerprint", "")).strip()) != 64:
  raise SystemExit("Regressão de contrato: crypto_inventory.inventory_fingerprint inválido.")

entities = crypto_inventory.get("entities") if isinstance(crypto_inventory.get("entities"), dict) else {}
for entity in ("cas", "certificates", "keys", "bundles"):
  if not isinstance(entities.get(entity), list):
    raise SystemExit(f"Regressão de contrato: crypto_inventory.entities.{entity} ausente/ inválido.")

history_lines = [line.strip() for line in history_file.read_text(encoding="utf-8").splitlines() if line.strip()]
if not history_lines:
    raise SystemExit("history.jsonl sem entradas.")

trace_lines = [line.strip() for line in decision_trace_file.read_text(encoding="utf-8").splitlines() if line.strip()]
if not trace_lines:
    raise SystemExit("decision-trace.jsonl sem entradas.")

last_history = json.loads(history_lines[-1])
last_trace = json.loads(trace_lines[-1])

for required in ("timestamp_utc", "run_id", "change_id", "fingerprint_sha256", "decision", "decision_reasons"):
    if required not in last_history:
        raise SystemExit(f"Última entrada de history.jsonl sem campo obrigatório: {required}")
    if required not in last_trace:
        raise SystemExit(f"Última entrada de decision-trace.jsonl sem campo obrigatório: {required}")

if last_history.get("decision") != "allow" or last_trace.get("decision") != "allow":
    raise SystemExit("Última decisão de trilha indica bloqueio.")
PY

echo "==> Pipeline A1.3 gate concluído com sucesso"
echo "==> Evidências válidas em: ${EVIDENCE_DIR}"
