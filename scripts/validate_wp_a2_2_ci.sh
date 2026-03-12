#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Uso: $0 <provision_evidence_dir>" >&2
  exit 2
fi

EVIDENCE_DIR="$1"
if [[ ! -d "${EVIDENCE_DIR}" ]]; then
  echo "[ERROR][a2_2_missing_evidence_dir] Diretorio de evidencias nao encontrado: ${EVIDENCE_DIR}" >&2
  exit 1
fi

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

OUTPUT_DIR="${WP_A2_2_GATE_OUTPUT_DIR:-${REPO_ROOT}/.artifacts/wp-a2-2-gate}"
CHANGE_ID="${WP_A2_2_CHANGE_ID:-a2-2-$(date -u +%Y%m%dT%H%M%SZ)}"
SKIP_MATRIX="${WP_A2_2_GATE_SKIP_MATRIX:-0}"

HISTORY_FILE="${OUTPUT_DIR}/history.jsonl"
DECISION_TRACE_FILE="${OUTPUT_DIR}/decision-trace.jsonl"
CHANGE_ID_SAFE="$(printf '%s' "${CHANGE_ID}" | tr -c 'A-Za-z0-9._-' '_')"
RUN_FILE="${OUTPUT_DIR}/run-${CHANGE_ID_SAFE}.jsonl"
SUMMARY_FILE="${OUTPUT_DIR}/latest-summary.json"
VALIDATION_RESULT_FILE="${OUTPUT_DIR}/validation-result.json"
TEST_MATRIX_FILE="${OUTPUT_DIR}/test-matrix-summary.json"
FULL_SUITE_LOG="${OUTPUT_DIR}/a2-2-unittest.log"

mkdir -p "${OUTPUT_DIR}"
touch "${HISTORY_FILE}" "${DECISION_TRACE_FILE}"
: > "${RUN_FILE}"

for artifact in \
  "provision-plan.json" \
  "reconcile-report.json" \
  "inventory-final.json" \
  "stage-reports.json" \
  "verify-report.json" \
  "ssh-execution-log.json" \
  "a2-audit-summary.json"; do
  if [[ ! -s "${EVIDENCE_DIR}/${artifact}" ]]; then
    echo "[ERROR][a2_2_missing_artifact] Evidencia obrigatoria ausente ou vazia: ${artifact}" >&2
    exit 1
  fi
done

echo "==> WP A2.2 gate (change_id=${CHANGE_ID})"
echo "==> evidence_dir=${EVIDENCE_DIR}"

any_failed=0
validation_ok=0
matrix_passed=0
full_suite_passed=0

if "${PYTHON_BIN}" - <<PY > "${VALIDATION_RESULT_FILE}"
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


def fail(code: str, message: str) -> None:
    print(f"[ERROR][{code}] {message}", file=sys.stderr)
    raise SystemExit(1)


def load_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail("a2_2_invalid_json", f"Falha ao ler JSON de {path}: {exc}")
    if not isinstance(payload, dict):
        fail("a2_2_invalid_json_root", f"Raiz JSON invalida em {path}: esperado objeto.")
    return payload


def is_sha256(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value).strip().lower()))


def normalized_host_ids(value):
    if isinstance(value, list):
        return sorted({str(item).strip() for item in value if str(item).strip()})
    text = str(value).strip()
    return [text] if text else []


evidence_dir = Path(${EVIDENCE_DIR@Q})
gate_change_id = ${CHANGE_ID@Q}

files = {
    "provision_plan": evidence_dir / "provision-plan.json",
    "reconcile_report": evidence_dir / "reconcile-report.json",
    "inventory_final": evidence_dir / "inventory-final.json",
    "stage_reports": evidence_dir / "stage-reports.json",
    "verify_report": evidence_dir / "verify-report.json",
    "ssh_execution_log": evidence_dir / "ssh-execution-log.json",
    "audit_summary": evidence_dir / "a2-audit-summary.json",
}
payloads = {key: load_json(path) for key, path in files.items()}

root_run_ids = {}
root_change_ids = {}
for key, payload in payloads.items():
    root_run_id = str(payload.get("run_id", "")).strip()
    root_change_id = str(payload.get("change_id", "")).strip()
    if not root_run_id:
        fail("a2_2_missing_root_run_id", f"{key} sem run_id.")
    if not root_change_id:
        fail("a2_2_missing_root_change_id", f"{key} sem change_id.")
    root_run_ids[key] = root_run_id
    root_change_ids[key] = root_change_id

unique_run_ids = sorted(set(root_run_ids.values()))
unique_change_ids = sorted(set(root_change_ids.values()))
if len(unique_run_ids) != 1:
    fail("a2_2_root_run_id_mismatch", f"run_id divergente entre artefatos: {root_run_ids}")
if len(unique_change_ids) != 1:
    fail("a2_2_root_change_id_mismatch", f"change_id divergente entre artefatos: {root_change_ids}")

run_id = unique_run_ids[0]
change_id = unique_change_ids[0]

correlations = {}
for key, payload in payloads.items():
    correlation = payload.get("correlation")
    if not isinstance(correlation, dict):
        fail("a2_2_missing_correlation", f"{key} sem bloco correlation.")
    corr_run_id = str(correlation.get("run_id", "")).strip()
    corr_change_id = str(correlation.get("change_id", "")).strip()
    manifest_fingerprint = str(correlation.get("manifest_fingerprint", "")).strip().lower()
    source_blueprint_fingerprint = str(correlation.get("source_blueprint_fingerprint", "")).strip().lower()
    host_ids = normalized_host_ids(
        correlation.get("host_id", correlation.get("host_ids", []))
    )
    if corr_run_id != run_id or corr_change_id != change_id:
        fail(
            "a2_2_correlation_mismatch",
            f"{key} com correlation divergente do escopo run/change oficial.",
        )
    if not is_sha256(manifest_fingerprint):
        fail("a2_2_invalid_manifest_fingerprint", f"{key} sem manifest_fingerprint SHA-256 valido.")
    if not is_sha256(source_blueprint_fingerprint):
        fail("a2_2_invalid_source_blueprint_fingerprint", f"{key} sem source_blueprint_fingerprint SHA-256 valido.")
    if not host_ids:
        fail("a2_2_missing_correlation_hosts", f"{key} sem host_id/host_ids em correlation.")
    correlations[key] = {
        "manifest_fingerprint": manifest_fingerprint,
        "source_blueprint_fingerprint": source_blueprint_fingerprint,
        "host_ids": host_ids,
    }

manifest_fingerprints = {value["manifest_fingerprint"] for value in correlations.values()}
source_blueprint_fingerprints = {value["source_blueprint_fingerprint"] for value in correlations.values()}
if len(manifest_fingerprints) != 1:
    fail("a2_2_manifest_fingerprint_mismatch", "manifest_fingerprint divergente entre artefatos.")
if len(source_blueprint_fingerprints) != 1:
    fail("a2_2_source_blueprint_fingerprint_mismatch", "source_blueprint_fingerprint divergente entre artefatos.")

manifest_fingerprint = next(iter(manifest_fingerprints))
source_blueprint_fingerprint = next(iter(source_blueprint_fingerprints))
host_ids = sorted({host for value in correlations.values() for host in value["host_ids"]})

reconcile_wrapper = payloads["reconcile_report"]
reconcile_report = reconcile_wrapper.get("reconcile_report")
if not isinstance(reconcile_report, dict):
    fail("a2_2_invalid_reconcile_report", "reconcile-report.json sem objeto reconcile_report.")
for required in ("divergence_summary", "action_summary", "reconcile_actions", "reconcile_action_count", "manifest_fingerprint"):
    if required not in reconcile_report:
        fail("a2_2_reconcile_contract_missing_field", f"reconcile-report sem campo obrigatorio: {required}")
if not isinstance(reconcile_report.get("divergence_summary"), dict):
    fail("a2_2_reconcile_invalid_divergence_summary", "divergence_summary deve ser objeto.")
if not isinstance(reconcile_report.get("action_summary"), dict):
    fail("a2_2_reconcile_invalid_action_summary", "action_summary deve ser objeto.")
actions = reconcile_report.get("reconcile_actions")
if not isinstance(actions, list):
    fail("a2_2_reconcile_invalid_actions", "reconcile_actions deve ser lista.")
reconcile_action_count = int(reconcile_report.get("reconcile_action_count", -1) or -1)
if reconcile_action_count != len(actions):
    fail(
        "a2_2_reconcile_action_count_mismatch",
        f"reconcile_action_count ({reconcile_action_count}) diverge de len(reconcile_actions) ({len(actions)}).",
    )
if str(reconcile_report.get("manifest_fingerprint", "")).strip().lower() != manifest_fingerprint:
    fail("a2_2_reconcile_manifest_fingerprint_mismatch", "Manifest fingerprint divergente em reconcile-report.")
computed_action_counts = {}
for action in actions:
    if not isinstance(action, dict):
        continue
    action_name = str(action.get("action", "")).strip().lower()
    if action_name:
        computed_action_counts[action_name] = computed_action_counts.get(action_name, 0) + 1
for action_name, count in sorted(computed_action_counts.items()):
    reported = int((reconcile_report.get("action_summary", {}) or {}).get(action_name, 0) or 0)
    if reported != count:
        fail(
            "a2_2_reconcile_action_summary_mismatch",
            f"action_summary.{action_name}={reported} diverge de contagem observada {count}.",
        )

inventory_wrapper = payloads["inventory_final"]
inventory = inventory_wrapper.get("inventory")
if not isinstance(inventory, dict):
    fail("a2_2_inventory_missing", "inventory-final sem objeto inventory.")
inventory_verify = inventory.get("verify_report")
if not isinstance(inventory_verify, dict):
    fail("a2_2_inventory_missing_verify_report", "inventory-final sem verify_report interno.")
if str(inventory_verify.get("decision", "")).strip().lower() != "allow" or bool(inventory_verify.get("blocked", True)):
    fail("a2_2_inventory_not_converged", "verify_report interno do inventory-final sem decision=allow.")
inventory_audit = inventory.get("audit_trail")
if not isinstance(inventory_audit, dict):
    fail("a2_2_inventory_missing_audit_trail", "inventory-final sem audit_trail interno.")
if bool(inventory_audit.get("evidence_valid", False)) is not True:
    fail("a2_2_inventory_not_converged", "inventory-final sem audit_trail.evidence_valid=true.")
hosts = inventory.get("hosts")
if not isinstance(hosts, list) or not hosts:
    fail("a2_2_inventory_missing_hosts", "inventory-final sem hosts convergidos.")
required_not_running = []
for host in hosts:
    if not isinstance(host, dict):
        continue
    host_ref = str(host.get("host_ref", "")).strip()
    for component in host.get("components", []) or []:
        if not isinstance(component, dict):
            continue
        desired_state = str(component.get("desired_state", "")).strip().lower()
        if desired_state != "required":
            continue
        status = str(component.get("status", "")).strip().lower()
        if status != "running":
            component_ref = str(component.get("component_id", "")).strip() or str(component.get("name", "")).strip()
            required_not_running.append(f"{host_ref}:{component_ref}:{status or 'unknown'}")
if required_not_running:
    fail(
        "a2_2_required_component_not_converged",
        "Componentes required sem status running: " + ", ".join(sorted(required_not_running)),
    )

verify_report = payloads["verify_report"]
if str(verify_report.get("decision", "")).strip().lower() != "allow" or bool(verify_report.get("blocked", True)):
    fail("a2_2_verify_decision_block", "verify-report sem decision=allow.")
verify_reason_codes = verify_report.get("reason_codes")
if not isinstance(verify_reason_codes, list):
    fail("a2_2_verify_reason_codes_invalid", "verify-report.reason_codes deve ser lista.")

stage_reports = payloads["stage_reports"]
stage_reports_payload = stage_reports.get("reports")
if not isinstance(stage_reports_payload, dict):
    fail("a2_2_stage_reports_invalid", "stage-reports sem objeto reports.")
stage_verify = stage_reports_payload.get("verify")
if not isinstance(stage_verify, dict):
    fail("a2_2_stage_reports_missing_verify", "stage-reports sem reports.verify.")
if str(stage_verify.get("decision", "")).strip().lower() != "allow":
    fail("a2_2_stage_reports_verify_blocked", "stage-reports.reports.verify sem decision=allow.")

ssh_wrapper = payloads["ssh_execution_log"]
ssh_log = ssh_wrapper.get("ssh_execution_log")
if not isinstance(ssh_log, dict):
    fail("a2_2_ssh_log_missing", "ssh-execution-log sem objeto ssh_execution_log.")
if str(ssh_log.get("run_id", "")).strip() != run_id or str(ssh_log.get("change_id", "")).strip() != change_id:
    fail("a2_2_ssh_log_correlation_mismatch", "ssh_execution_log com run/change divergente.")
ssh_summary = ssh_log.get("summary")
if not isinstance(ssh_summary, dict):
    fail("a2_2_ssh_log_summary_missing", "ssh_execution_log sem summary.")
units_detailed = ssh_log.get("units_detailed")
if not isinstance(units_detailed, list):
    fail("a2_2_ssh_log_units_missing", "ssh_execution_log sem units_detailed.")
unit_count = int(ssh_summary.get("unit_count", -1) or -1)
if unit_count != len(units_detailed):
    fail(
        "a2_2_ssh_log_unit_count_mismatch",
        f"summary.unit_count ({unit_count}) diverge de len(units_detailed) ({len(units_detailed)}).",
    )
for idx, unit in enumerate(units_detailed):
    if not isinstance(unit, dict):
        continue
    if str(unit.get("run_id", "")).strip() != run_id or str(unit.get("change_id", "")).strip() != change_id:
        fail("a2_2_ssh_log_correlation_mismatch", f"Unidade SSH {idx} com run/change divergente.")
    for required in ("host_id", "component_id", "operation", "status"):
        if not str(unit.get(required, "")).strip():
            fail("a2_2_ssh_log_unit_missing_field", f"Unidade SSH {idx} sem campo obrigatorio: {required}.")

audit_summary = payloads["audit_summary"]
if str(audit_summary.get("decision", "")).strip().lower() != "allow":
    fail("a2_2_audit_decision_block", "a2-audit-summary sem decision=allow.")
if bool(audit_summary.get("evidence_valid", False)) is not True:
    fail("a2_2_audit_evidence_invalid", "a2-audit-summary sem evidence_valid=true.")
required_artifacts = audit_summary.get("required_artifacts")
if not isinstance(required_artifacts, dict):
    fail("a2_2_audit_missing_required_artifacts", "a2-audit-summary sem required_artifacts.")
expected_artifacts = {
    "provision_plan",
    "reconcile_report",
    "inventory_final",
    "stage_reports",
    "verify_report",
    "ssh_execution_log",
}
if set(required_artifacts.keys()) != expected_artifacts:
    fail(
        "a2_2_audit_required_artifacts_mismatch",
        f"required_artifacts divergente. Esperado={sorted(expected_artifacts)} obtido={sorted(required_artifacts.keys())}",
    )
missing_artifacts = audit_summary.get("missing_artifacts", [])
if not isinstance(missing_artifacts, list):
    fail("a2_2_audit_missing_artifacts_invalid", "a2-audit-summary.missing_artifacts deve ser lista.")
if missing_artifacts:
    fail("a2_2_audit_missing_artifacts", "a2-audit-summary reporta artefatos faltantes.")
artifact_paths = audit_summary.get("artifacts")
if not isinstance(artifact_paths, dict):
    fail("a2_2_audit_artifacts_invalid", "a2-audit-summary sem objeto artifacts.")
for key in sorted(expected_artifacts):
    path = Path(str(artifact_paths.get(key, "")).strip())
    if not path.exists():
        fail("a2_2_audit_artifact_path_missing", f"Caminho do artefato {key} ausente: {path}")

entry = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "gate_change_id": gate_change_id,
    "evidence_dir": str(evidence_dir),
    "run_id": run_id,
    "change_id": change_id,
    "manifest_fingerprint": manifest_fingerprint,
    "source_blueprint_fingerprint": source_blueprint_fingerprint,
    "host_ids": host_ids,
    "decision": "allow",
    "decision_reasons": [
        {
            "code": "a2_2_evidence_contract_valid",
            "message": "Evidencias de provision/reconcile/verify correlacionadas e convergidas.",
        }
    ],
    "required_evidence": {
        "reconcile_report_consistent": True,
        "inventory_final_converged": True,
        "ssh_logs_correlated": True,
    },
}
print(json.dumps(entry, ensure_ascii=False))
PY
then
  validation_ok=1
  if ! cat "${VALIDATION_RESULT_FILE}" >> "${HISTORY_FILE}" || ! cat "${VALIDATION_RESULT_FILE}" >> "${RUN_FILE}" || ! cat "${VALIDATION_RESULT_FILE}" >> "${DECISION_TRACE_FILE}"; then
    echo "[ERROR][a2_2_persist_history_failed] Falha ao persistir historico/trace do gate A2.2." >&2
    any_failed=1
  fi
else
  echo "[ERROR][a2_2_validation_failed] Validacao contratual das evidencias A2.2 falhou." >&2
  any_failed=1
fi

if [[ "${validation_ok}" -eq 1 && "${SKIP_MATRIX}" != "1" ]]; then
  if "${PYTHON_BIN}" - <<PY > "${TEST_MATRIX_FILE}"
import io
import json
import unittest
from datetime import datetime, timezone

tests = [
    (
        "positive_baseline_minimum_creation",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a2_materializes_fabric_base_components_with_postchecks",
    ),
    (
        "positive_idempotent_reexecution_without_drift",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_new_run_same_change_applies_only_diff_actions",
    ),
    (
        "positive_reconcile_after_manual_service_stop",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_reconciles_after_manual_service_stop",
    ),
    (
        "negative_host_unavailable",
        "automation.tests.test_provisioning_ssh_executor.ProvisioningSshExecutorIntegrationTests.test_provision_stage_blocks_on_host_unavailable_ssh_failure",
    ),
    (
        "negative_invalid_credentials",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_blocks_when_connection_profile_ref_is_not_in_registry",
    ),
    (
        "negative_port_conflict",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_blocks_when_port_conflict_detected",
    ),
    (
        "negative_manifest_divergent_from_host",
        "automation.tests.test_pipeline_a2_2_reconcile_engine.PipelineA22ReconcileEngineTests.test_reconciliation_classifies_divergences_and_policies",
    ),
    (
        "negative_required_component_not_converged",
        "automation.tests.test_provisioning_ssh_executor.ProvisioningSshExecutorIntegrationTests.test_provision_stage_blocks_on_definitive_ssh_failure",
    ),
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
    matrix_passed=1
  else
    echo "[ERROR][a2_2_test_matrix_failed] Matriz obrigatoria de testes do WP A2.2 falhou." >&2
    any_failed=1
  fi
elif [[ "${SKIP_MATRIX}" == "1" ]]; then
  cat > "${TEST_MATRIX_FILE}" <<JSON
{
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "all_passed": false,
  "skipped": true,
  "reason": "WP_A2_2_GATE_SKIP_MATRIX=1"
}
JSON
else
  cat > "${TEST_MATRIX_FILE}" <<JSON
{
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "all_passed": false,
  "skipped": true,
  "reason": "validation_failed"
}
JSON
fi

if [[ "${validation_ok}" -eq 1 && "${matrix_passed}" -eq 1 ]]; then
  if "${PYTHON_BIN}" -m unittest \
    automation.tests.test_pipeline_provision \
    automation.tests.test_provisioning_ssh_executor \
    automation.tests.test_pipeline_a2_2_reconcile_engine \
    automation.tests.test_pipeline_a2_2_observed_state \
    automation.tests.test_pipeline_a2_2_provision_plan \
    automation.tests.test_pipeline_a2_2_entry_gate \
    automation.tests.test_pipeline_a2_backend_flow \
    -v > "${FULL_SUITE_LOG}" 2>&1; then
    full_suite_passed=1
  else
    echo "[ERROR][a2_2_full_suite_failed] Suite automatizada A2.2 falhou." >&2
    any_failed=1
  fi
else
  : > "${FULL_SUITE_LOG}"
fi

if ! "${PYTHON_BIN}" - <<PY > "${SUMMARY_FILE}"
import json
from datetime import datetime, timezone

run_file = ${RUN_FILE@Q}
validation_result_file = ${VALIDATION_RESULT_FILE@Q}
test_matrix_file = ${TEST_MATRIX_FILE@Q}
history_file = ${HISTORY_FILE@Q}
decision_trace_file = ${DECISION_TRACE_FILE@Q}
full_suite_log = ${FULL_SUITE_LOG@Q}
change_id = ${CHANGE_ID@Q}
evidence_dir = ${EVIDENCE_DIR@Q}
output_dir = ${OUTPUT_DIR@Q}
validation_ok = bool(${validation_ok})
matrix_passed = bool(${matrix_passed})
full_suite_passed = bool(${full_suite_passed})
failed = int(${any_failed})
skip_matrix = ${SKIP_MATRIX@Q}

entries = []
with open(run_file, "r", encoding="utf-8") as fp:
    for line in fp:
        line = line.strip()
        if not line:
            continue
        entries.append(json.loads(line))

validation_result = {}
try:
    with open(validation_result_file, "r", encoding="utf-8") as fp:
        validation_result = json.load(fp)
except Exception:
    validation_result = {}

test_matrix = {}
try:
    with open(test_matrix_file, "r", encoding="utf-8") as fp:
        test_matrix = json.load(fp)
except Exception:
    test_matrix = {"all_passed": False}

required_evidence = {
    "reconcile_report_consistent": bool((validation_result.get("required_evidence") or {}).get("reconcile_report_consistent", False)),
    "inventory_final_converged": bool((validation_result.get("required_evidence") or {}).get("inventory_final_converged", False)),
    "ssh_logs_correlated": bool((validation_result.get("required_evidence") or {}).get("ssh_logs_correlated", False)),
    "automated_suite_passed": bool(matrix_passed and full_suite_passed),
}

summary = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "change_id": change_id,
    "status": "failed" if failed else "ok",
    "decision": "block" if failed else "allow",
    "validation_ok": validation_ok,
    "matrix_passed": matrix_passed,
    "full_suite_passed": full_suite_passed,
    "skip_matrix": skip_matrix == "1",
    "evidence_dir": evidence_dir,
    "required_evidence": required_evidence,
    "validation_result": validation_result,
    "test_matrix": test_matrix,
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
  echo "[ERROR][a2_2_summary_failed] Falha ao gerar resumo consolidado do gate A2.2." >&2
  any_failed=1
fi

if [[ "${any_failed}" -ne 0 ]]; then
  echo "[ERROR][a2_2_gate_failed] Gate WP A2.2 falhou." >&2
  echo "==> Resumo consolidado: ${SUMMARY_FILE}" >&2
  exit 1
fi

echo "==> Gate WP A2.2 concluido com sucesso"
echo "==> Resumo consolidado: ${SUMMARY_FILE}"
echo "==> Historico: ${HISTORY_FILE}"
