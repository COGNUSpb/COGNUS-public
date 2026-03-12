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

ALLOW_MIGRATION="${ORG_RUNTIME_MANIFEST_MIGRATE:-0}"
PERSIST_ENABLED="${ORG_RUNTIME_MANIFEST_PERSIST:-1}"
OUTPUT_DIR="${WP_A2_1_GATE_OUTPUT_DIR:-${REPO_ROOT}/.artifacts/wp-a2-1-gate}"
PERSIST_ROOT="${WP_A2_1_PERSIST_ROOT:-${OUTPUT_DIR}/state-store}"
CHANGE_ID="${WP_A2_1_CHANGE_ID:-a2-1-$(date -u +%Y%m%dT%H%M%SZ)}"

HISTORY_FILE="${OUTPUT_DIR}/history.jsonl"
DECISION_TRACE_FILE="${OUTPUT_DIR}/decision-trace.jsonl"
CHANGE_ID_SAFE="$(printf '%s' "${CHANGE_ID}" | tr -c 'A-Za-z0-9._-' '_')"
RUN_FILE="${OUTPUT_DIR}/run-${CHANGE_ID_SAFE}.jsonl"
SUMMARY_FILE="${OUTPUT_DIR}/latest-summary.json"
TEST_MATRIX_FILE="${OUTPUT_DIR}/test-matrix-summary.json"
FULL_SUITE_LOG="${OUTPUT_DIR}/org-runtime-manifest-unittest.log"

mkdir -p "${OUTPUT_DIR}"
touch "${HISTORY_FILE}" "${DECISION_TRACE_FILE}"
: > "${RUN_FILE}"

if [[ "$#" -eq 0 ]]; then
  echo "Uso: $0 <manifesto1.(yaml|yml|json)> [manifesto2 ...]" >&2
  echo "Dica: passe pelo menos um OrgRuntimeManifest válido para o gate." >&2
  exit 2
fi

echo "==> WP A2.1 gate (files=$# migrate=${ALLOW_MIGRATION} persist=${PERSIST_ENABLED})"
echo "==> change_id=${CHANGE_ID}"

any_failed=0

for input_path in "$@"; do
  if [[ ! -f "${input_path}" ]]; then
    echo "[ERROR] Manifesto nao encontrado: ${input_path}" >&2
    any_failed=1
    continue
  fi

  base_name="$(basename "${input_path}")"
  normalized_out="${OUTPUT_DIR}/${base_name}.normalized.json"
  report_out="${OUTPUT_DIR}/${base_name}.report.json"

  cmd=(
    "${PYTHON_BIN}"
    "${REPO_ROOT}/scripts/org_runtime_manifest_lint.py"
    "--manifest" "${input_path}"
    "--output" "text"
    "--normalized-out" "${normalized_out}"
    "--report-out" "${report_out}"
  )
  if [[ "${ALLOW_MIGRATION}" == "1" ]]; then
    cmd+=("--migrate")
  fi
  if [[ "${PERSIST_ENABLED}" == "1" ]]; then
    cmd+=("--persist-root" "${PERSIST_ROOT}" "--persist-actor" "wp-a2.1-gate")
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
import re
from datetime import datetime, timezone

report_path = ${report_out@Q}
normalized_path = ${normalized_out@Q}
input_path = ${input_path@Q}
change_id = ${CHANGE_ID@Q}
lint_status = ${lint_status@Q}

with open(report_path, "r", encoding="utf-8") as fp:
    report = json.load(fp)

with open(normalized_path, "r", encoding="utf-8") as fp:
    normalized = json.load(fp)

required_report_fields = [
    "valid",
    "errors",
    "warnings",
    "hints",
    "manifest_runtime_version",
    "manifest_name",
    "manifest_version",
    "resolved_manifest_version",
    "migration_applied",
    "migrated_from_manifest_version",
    "change_id",
    "run_id",
    "org_id",
    "org_label",
    "domain",
    "environment_profile_ref",
    "source_blueprint_fingerprint",
    "source_blueprint_version",
    "orchestrator_context",
    "normalized_source_blueprint_scope",
    "normalized_org_identity",
    "normalized_hosts",
    "normalized_components",
    "normalized_chaincode_runtimes",
    "manifest_fingerprint",
    "fingerprint_sha256",
    "issue_catalog",
    "schema_policy",
    "issues",
]
missing_report_fields = [field for field in required_report_fields if field not in report]
if missing_report_fields:
    raise SystemExit(
        "Relatorio sem contrato minimo obrigatorio: " + ", ".join(missing_report_fields)
    )

if not isinstance(report.get("valid"), bool):
    raise SystemExit("Campo 'valid' do relatorio deve ser booleano.")
for bucket in ("errors", "warnings", "hints", "issues"):
    if not isinstance(report.get(bucket), list):
        raise SystemExit(f"Campo '{bucket}' do relatorio deve ser lista.")

fingerprint = str(report.get("manifest_fingerprint", "")).strip().lower()
fingerprint_alias = str(report.get("fingerprint_sha256", "")).strip().lower()
if fingerprint != fingerprint_alias:
    raise SystemExit("manifest_fingerprint diverge de fingerprint_sha256.")
if not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
    raise SystemExit("manifest_fingerprint deve ser SHA-256 canonico.")

required_normalized_fields = [
    "manifest_name",
    "manifest_version",
    "resolved_manifest_version",
    "manifest_runtime_version",
    "generated_at",
    "change_id",
    "run_id",
    "org_id",
    "org_label",
    "domain",
    "environment_profile_ref",
    "source_blueprint_fingerprint",
    "source_blueprint_version",
    "orchestrator_context",
    "source_blueprint_scope",
    "org_identity",
    "hosts",
    "components",
    "chaincode_runtimes",
    "manifest_fingerprint",
    "fingerprint_sha256",
]
missing_normalized_fields = [field for field in required_normalized_fields if field not in normalized]
if missing_normalized_fields:
    raise SystemExit(
        "Manifesto normalizado sem contrato minimo obrigatorio: "
        + ", ".join(missing_normalized_fields)
    )
if str(normalized.get("manifest_fingerprint", "")).strip().lower() != fingerprint:
    raise SystemExit("manifest_fingerprint do normalizado diverge do relatorio.")

def build_reasons(report_obj):
    errors = report_obj.get("errors", [])
    if not isinstance(errors, list) or not errors:
        return [{"type": "lint_passed", "code": "no_error_issues", "count": 0}]
    by_code = {}
    for issue in errors:
        if not isinstance(issue, dict):
            continue
        code = str(issue.get("code", "unknown_error")).strip()
        if not code:
            continue
        by_code[code] = by_code.get(code, 0) + 1
    return [
        {"type": "lint_error_code", "code": code, "count": by_code[code]}
        for code in sorted(by_code)
    ]

entry = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "gate_change_id": change_id,
    "manifest_path": input_path,
    "status": lint_status,
    "valid": bool(report.get("valid", False)),
    "decision": "allow" if bool(report.get("valid", False)) else "block",
    "decision_reasons": build_reasons(report),
    "manifest_change_id": str(report.get("change_id", "")).strip(),
    "manifest_run_id": str(report.get("run_id", "")).strip(),
    "manifest_fingerprint": fingerprint,
    "report_file": report_path,
    "normalized_file": normalized_path,
}
print(json.dumps(entry, ensure_ascii=False))
PY
    then
      echo "[ERROR] Falha ao validar contrato de evidencia para ${input_path}" >&2
      any_failed=1
    elif ! cat "${entry_file}" >> "${HISTORY_FILE}" || ! cat "${entry_file}" >> "${RUN_FILE}" || ! cat "${entry_file}" >> "${DECISION_TRACE_FILE}"; then
      echo "[ERROR] Falha ao persistir historico para ${input_path}" >&2
      any_failed=1
    fi
    rm -f "${entry_file}"
  else
    echo "[ERROR] Evidencias obrigatorias ausentes para ${input_path}:" >&2
    echo "  - report: ${report_out}" >&2
    echo "  - normalized: ${normalized_out}" >&2
    any_failed=1
  fi
done

if ! "${PYTHON_BIN}" - <<PY > "${TEST_MATRIX_FILE}"
import io
import json
import unittest
from datetime import datetime, timezone

tests = [
    ("positive_minimum", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_valid_manifest_is_accepted"),
    ("positive_expansion", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_manifest_expansion_peer1_orderer1_is_accepted"),
    ("negative_cardinality", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_minimum_cardinality_is_enforced"),
    ("negative_invalid_naming", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_component_name_convention_is_enforced"),
    ("negative_host_port_conflict", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_host_port_conflict_is_rejected"),
    ("negative_component_without_host", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_component_host_reference_must_exist"),
    ("negative_runtime_without_peer", "automation.tests.test_org_runtime_manifest.OrgRuntimeManifestValidationTests.test_chaincode_runtime_target_peer_must_exist"),
]

results = []
all_ok = True
for criterion, test_name in tests:
    suite = unittest.defaultTestLoader.loadTestsFromName(test_name)
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    passed = result.wasSuccessful()
    all_ok = all_ok and passed
    results.append(
        {
            "criterion": criterion,
            "test_name": test_name,
            "passed": passed,
            "failures": len(result.failures),
            "errors": len(result.errors),
            "output": stream.getvalue().strip(),
        }
    )

payload = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "all_passed": all_ok,
    "results": results,
}
print(json.dumps(payload, indent=2, ensure_ascii=False))
raise SystemExit(0 if all_ok else 1)
PY
then
  echo "[ERROR] Matriz de testes obrigatoria do WP A2.1 falhou." >&2
  any_failed=1
fi

if ! "${PYTHON_BIN}" -m unittest automation.tests.test_org_runtime_manifest -v > "${FULL_SUITE_LOG}" 2>&1; then
  echo "[ERROR] Suite completa automation.tests.test_org_runtime_manifest falhou." >&2
  any_failed=1
fi

if ! "${PYTHON_BIN}" - <<PY > "${SUMMARY_FILE}"
import json
from datetime import datetime, timezone

run_file = ${RUN_FILE@Q}
history_file = ${HISTORY_FILE@Q}
decision_trace_file = ${DECISION_TRACE_FILE@Q}
test_matrix_file = ${TEST_MATRIX_FILE@Q}
full_suite_log = ${FULL_SUITE_LOG@Q}
change_id = ${CHANGE_ID@Q}
output_dir = ${OUTPUT_DIR@Q}
allow_migration = ${ALLOW_MIGRATION@Q}
persist_enabled = ${PERSIST_ENABLED@Q}
failed = ${any_failed}

entries = []
with open(run_file, "r", encoding="utf-8") as fp:
    for line in fp:
        line = line.strip()
        if not line:
            continue
        entries.append(json.loads(line))

try:
    with open(test_matrix_file, "r", encoding="utf-8") as fp:
        matrix = json.load(fp)
except Exception:
    matrix = {"all_passed": False, "results": []}

summary = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "change_id": change_id,
    "status": "failed" if failed else "ok",
    "decision": "block" if failed else "allow",
    "migrate_enabled": allow_migration == "1",
    "persist_enabled": persist_enabled == "1",
    "validated_manifests": len(entries),
    "valid_manifests": sum(1 for entry in entries if bool(entry.get("valid", False))),
    "invalid_manifests": sum(1 for entry in entries if not bool(entry.get("valid", False))),
    "required_evidence": {
        "structured_validation_output": bool(entries) and all(bool(entry.get("report_file")) for entry in entries),
        "normalized_manifest_output": bool(entries) and all(bool(entry.get("normalized_file")) for entry in entries),
        "manifest_fingerprint_present": bool(entries) and all(len(str(entry.get("manifest_fingerprint", ""))) == 64 for entry in entries),
        "automated_suite_passed": bool(matrix.get("all_passed", False)) and failed == 0,
    },
    "test_matrix": matrix,
    "history_file": history_file,
    "decision_trace_file": decision_trace_file,
    "run_file": run_file,
    "full_suite_log": full_suite_log,
    "output_dir": output_dir,
    "entries": entries,
}
print(json.dumps(summary, indent=2, ensure_ascii=False))
PY
then
  echo "[ERROR] Falha ao gerar resumo consolidado do WP A2.1." >&2
  any_failed=1
fi

if [[ "${any_failed}" -ne 0 ]]; then
  echo "[ERROR] Gate WP A2.1 falhou." >&2
  echo "==> Resumo consolidado: ${SUMMARY_FILE}" >&2
  exit 1
fi

echo "==> Gate WP A2.1 concluido com sucesso"
echo "==> Evidencias disponiveis em: ${OUTPUT_DIR}"
echo "==> Historico: ${HISTORY_FILE}"
echo "==> Resumo consolidado: ${SUMMARY_FILE}"
