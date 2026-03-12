#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

ALLOW_MIGRATION="${BLUEPRINT_LINT_MIGRATE:-0}"
OUTPUT_DIR="${BLUEPRINT_LINT_OUTPUT_DIR:-${REPO_ROOT}/.artifacts/blueprint-lint}"
mkdir -p "${OUTPUT_DIR}"
CHANGE_ID="${BLUEPRINT_CHANGE_ID:-bp-$(date -u +%Y%m%dT%H%M%SZ)}"
HISTORY_FILE="${OUTPUT_DIR}/history.jsonl"
CHANGE_ID_SAFE="$(printf '%s' "${CHANGE_ID}" | tr -c 'A-Za-z0-9._-' '_')"
RUN_FILE="${OUTPUT_DIR}/run-${CHANGE_ID_SAFE}.jsonl"
SUMMARY_FILE="${OUTPUT_DIR}/latest-summary.json"
DECISION_TRACE_FILE="${OUTPUT_DIR}/decision-trace.jsonl"

touch "${HISTORY_FILE}"
touch "${DECISION_TRACE_FILE}"
: > "${RUN_FILE}"

if [[ "$#" -eq 0 ]]; then
  echo "Uso: $0 <blueprint1.(yaml|yml|json)> [blueprint2 ...]" >&2
  echo "Dica: configure BLUEPRINT_LINT_FILES no pipeline para passar os arquivos." >&2
  exit 2
fi

echo "==> Blueprint lint gate (files=$# migrate=${ALLOW_MIGRATION})"
echo "==> change_id=${CHANGE_ID}"

any_failed=0
for input_path in "$@"; do
  if [[ ! -f "${input_path}" ]]; then
    echo "[ERROR] Blueprint não encontrado: ${input_path}" >&2
    any_failed=1
    continue
  fi

  base_name="$(basename "${input_path}")"
  normalized_out="${OUTPUT_DIR}/${base_name}.normalized.json"
  report_out="${OUTPUT_DIR}/${base_name}.report.json"

  cmd=(
    "${PYTHON_BIN}"
    "${REPO_ROOT}/scripts/blueprint_lint.py"
    "--blueprint" "${input_path}"
    "--output" "text"
    "--normalized-out" "${normalized_out}"
    "--report-out" "${report_out}"
  )

  if [[ "${ALLOW_MIGRATION}" == "1" ]]; then
    cmd+=("--migrate")
  fi

  echo "--> Validando ${input_path}"
  lint_status="ok"
  if ! "${cmd[@]}"; then
    any_failed=1
    lint_status="failed"
  fi

  if [[ -s "${report_out}" && -s "${normalized_out}" ]]; then
    entry_file="${OUTPUT_DIR}/.${base_name}.entry.json"
    if ! "${PYTHON_BIN}" - <<PY > "${entry_file}"
import json
from datetime import datetime, timezone

report_path = ${report_out@Q}
input_path = ${input_path@Q}
normalized_path = ${normalized_out@Q}
change_id = ${CHANGE_ID@Q}
lint_status = ${lint_status@Q}

with open(report_path, "r", encoding="utf-8") as fp:
    report = json.load(fp)

required_report_fields = [
    "valid",
    "errors",
    "warnings",
    "hints",
    "normalized_orgs",
    "normalized_channels",
    "normalized_nodes",
    "normalized_policies",
    "normalized_environment_profile",
    "normalized_identity_baseline",
    "schema_runtime",
    "resolved_schema_version",
    "fingerprint_sha256",
]
missing_report_fields = [field for field in required_report_fields if field not in report]
if missing_report_fields:
    raise SystemExit(
        "Relatório de lint sem contrato mínimo obrigatório: "
        + ", ".join(missing_report_fields)
    )

for bucket in ("errors", "warnings", "hints"):
    if not isinstance(report.get(bucket), list):
        raise SystemExit(f"Campo '{bucket}' do relatório deve ser lista.")

if not isinstance(report.get("valid"), bool):
    raise SystemExit("Campo 'valid' do relatório deve ser booleano.")

def _build_reproducible_reasons(report_obj):
  reasons = []
  errors = report_obj.get("errors", []) if isinstance(report_obj.get("errors"), list) else []
  if errors:
    by_code = {}
    for issue in errors:
      if not isinstance(issue, dict):
        continue
      code = str(issue.get("code", "unknown_error"))
      by_code[code] = by_code.get(code, 0) + 1
    for code in sorted(by_code):
      reasons.append(
        {
          "type": "lint_error_code",
          "code": code,
          "count": by_code[code],
          "message": f"{by_code[code]} ocorrência(s) de '{code}'.",
        }
      )
  else:
    reasons.append(
      {
        "type": "lint_passed",
        "code": "no_error_issues",
        "count": 0,
        "message": "Relatório sem erros de lint.",
      }
    )
  return reasons

with open(normalized_path, "r", encoding="utf-8") as fp:
    normalized = json.load(fp)

required_normalized_fields = [
    "orgs",
    "channels",
    "nodes",
    "policies",
    "environment_profile",
    "identity_baseline",
    "fingerprint_sha256",
    "current_schema_version",
    "resolved_schema_version",
]
missing_normalized_fields = [field for field in required_normalized_fields if field not in normalized]
if missing_normalized_fields:
    raise SystemExit(
        "Blueprint normalizado sem contrato mínimo obrigatório: "
        + ", ".join(missing_normalized_fields)
    )

entry = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "change_id": change_id,
    "blueprint_path": input_path,
    "status": lint_status,
    "valid": bool(report.get("valid", False)),
    "schema_name": report.get("schema_name", ""),
    "schema_version": report.get("schema_version", ""),
    "resolved_schema_version": report.get("resolved_schema_version", ""),
    "fingerprint_sha256": report.get("fingerprint_sha256", ""),
    "report_file": report_path,
    "normalized_file": normalized_path,
    "decision": "allow" if bool(report.get("valid", False)) else "block",
    "decision_reasons": _build_reproducible_reasons(report),
}

print(json.dumps(entry, ensure_ascii=False))
PY
    then
      echo "[ERROR] Falha ao registrar histórico para ${input_path}" >&2
      any_failed=1
    elif ! cat "${entry_file}" >> "${HISTORY_FILE}" || ! cat "${entry_file}" >> "${RUN_FILE}" || ! cat "${entry_file}" >> "${DECISION_TRACE_FILE}"; then
      echo "[ERROR] Falha ao persistir evidência de histórico para ${input_path}" >&2
      any_failed=1
    fi
    rm -f "${entry_file}"
  else
    echo "[ERROR] Evidências obrigatórias não geradas para ${input_path}:" >&2
    echo "  - report: ${report_out}" >&2
    echo "  - normalized: ${normalized_out}" >&2
    any_failed=1
  fi
done

if ! "${PYTHON_BIN}" - <<PY > "${SUMMARY_FILE}"
import json
from datetime import datetime, timezone

run_file = ${RUN_FILE@Q}
change_id = ${CHANGE_ID@Q}
history_file = ${HISTORY_FILE@Q}
decision_trace_file = ${DECISION_TRACE_FILE@Q}
output_dir = ${OUTPUT_DIR@Q}
allow_migration = ${ALLOW_MIGRATION@Q}
total_files = ${#@}
failed = ${any_failed}

entries = []
with open(run_file, "r", encoding="utf-8") as fp:
    for line in fp:
        line = line.strip()
        if not line:
            continue
        entries.append(json.loads(line))

summary = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "change_id": change_id,
    "status": "failed" if failed else "ok",
  "decision": "block" if failed else "allow",
    "migrate_enabled": allow_migration == "1",
    "total_files": int(total_files),
    "validated_entries": len(entries),
    "valid_entries": sum(1 for entry in entries if bool(entry.get("valid", False))),
    "invalid_entries": sum(1 for entry in entries if not bool(entry.get("valid", False))),
    "history_file": history_file,
  "decision_trace_file": decision_trace_file,
    "run_file": run_file,
    "output_dir": output_dir,
  "decision_reasons": [
    {
      "type": "lint_gate_failure",
      "code": "blueprint_invalid_or_gate_error",
      "message": "Pelo menos um blueprint inválido ou erro operacional no gate.",
    }
  ] if failed else [
    {
      "type": "lint_gate_passed",
      "code": "all_blueprints_valid",
      "message": "Todos os blueprints validados com sucesso.",
    }
  ],
    "entries": entries,
}

print(json.dumps(summary, indent=2, ensure_ascii=False))
PY
then
  echo "[ERROR] Falha ao gerar resumo consolidado do gate." >&2
  any_failed=1
fi

if [[ "${any_failed}" -ne 0 ]]; then
  echo "[ERROR] Blueprint lint gate falhou." >&2
  echo "==> Resumo consolidado: ${SUMMARY_FILE}" >&2
  exit 1
fi

echo "==> Blueprint lint gate concluído com sucesso"
echo "==> Evidências disponíveis em: ${OUTPUT_DIR}"
echo "==> Histórico: ${HISTORY_FILE}"
echo "==> Resumo consolidado: ${SUMMARY_FILE}"
