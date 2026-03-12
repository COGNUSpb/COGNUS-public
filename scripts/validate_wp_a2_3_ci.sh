#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Uso: $0 <provision_evidence_dir>" >&2
  exit 2
fi

EVIDENCE_DIR="$1"
if [[ ! -d "${EVIDENCE_DIR}" ]]; then
  echo "[ERROR][a2_3_missing_evidence_dir] Diretorio de evidencias nao encontrado: ${EVIDENCE_DIR}" >&2
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

OUTPUT_DIR="${WP_A2_3_GATE_OUTPUT_DIR:-${REPO_ROOT}/.artifacts/wp-a2-3-gate}"
CHANGE_ID="${WP_A2_3_CHANGE_ID:-a2-3-$(date -u +%Y%m%dT%H%M%SZ)}"
SKIP_MATRIX="${WP_A2_3_GATE_SKIP_MATRIX:-0}"

HISTORY_FILE="${OUTPUT_DIR}/history.jsonl"
DECISION_TRACE_FILE="${OUTPUT_DIR}/decision-trace.jsonl"
CHANGE_ID_SAFE="$(printf '%s' "${CHANGE_ID}" | tr -c 'A-Za-z0-9._-' '_')"
RUN_FILE="${OUTPUT_DIR}/run-${CHANGE_ID_SAFE}.jsonl"
SUMMARY_FILE="${OUTPUT_DIR}/latest-summary.json"
ACCEPTANCE_SUMMARY_FILE="${OUTPUT_DIR}/a2-3-acceptance-summary.json"
READINESS_CHECKLIST_FILE="${OUTPUT_DIR}/a2-3-readiness-checklist.json"
HANDOFF_FILE="${OUTPUT_DIR}/a2-3-handoff-a2-4-a2-5.json"
VALIDATION_RESULT_FILE="${OUTPUT_DIR}/validation-result.json"
TEST_MATRIX_FILE="${OUTPUT_DIR}/test-matrix-summary.json"
FULL_SUITE_LOG="${OUTPUT_DIR}/a2-3-unittest.log"

mkdir -p "${OUTPUT_DIR}"
touch "${HISTORY_FILE}" "${DECISION_TRACE_FILE}"
: > "${RUN_FILE}"

for artifact in \
  "provision-report.json" \
  "runtime-inventory.json" \
  "inventory-final.json" \
  "runtime-plan.json" \
  "runtime-template-contract.json" \
  "runtime-bundle-manifest.json" \
  "runtime-bootstrap-report.json" \
  "runtime-verify-report.json" \
  "runtime-reconcile-report.json"; do
  if [[ ! -s "${EVIDENCE_DIR}/${artifact}" ]]; then
    echo "[ERROR][a2_3_missing_artifact] Evidencia obrigatoria ausente ou vazia: ${artifact}" >&2
    exit 1
  fi
done

echo "==> WP A2.3 gate (change_id=${CHANGE_ID})"
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
        fail("a2_3_invalid_json", f"Falha ao ler JSON de {path}: {exc}")
    if not isinstance(payload, dict):
        fail("a2_3_invalid_json_root", f"Raiz JSON invalida em {path}: esperado objeto.")
    return payload


def is_sha256(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value).strip().lower()))


def get_correlation(payload: dict, payload_name: str) -> dict:
    corr = payload.get("correlation")
    if isinstance(corr, dict):
        return corr
    fallback = {
        "run_id": payload.get("run_id", ""),
        "change_id": payload.get("change_id", ""),
        "manifest_fingerprint": payload.get("manifest_fingerprint", ""),
        "source_blueprint_fingerprint": payload.get("source_blueprint_fingerprint", ""),
    }
    if all(str(fallback.get(key, "")).strip() for key in ("manifest_fingerprint", "source_blueprint_fingerprint")):
        return fallback
    fail("a2_3_missing_correlation", f"{payload_name} sem correlation e sem fingerprints top-level equivalentes.")


def validate_run_change(payload_name: str, payload: dict, run_id: str, change_id: str) -> None:
    payload_run_id = str(payload.get("run_id", "")).strip()
    payload_change_id = str(payload.get("change_id", "")).strip()
    if payload_run_id and payload_run_id != run_id:
        fail("a2_3_run_id_mismatch", f"{payload_name} com run_id divergente do escopo.")
    if payload_change_id and payload_change_id != change_id:
        fail("a2_3_change_id_mismatch", f"{payload_name} com change_id divergente do escopo.")


evidence_dir = Path(${EVIDENCE_DIR@Q})
gate_change_id = ${CHANGE_ID@Q}

files = {
    "provision_report": evidence_dir / "provision-report.json",
    "runtime_inventory": evidence_dir / "runtime-inventory.json",
    "inventory_final": evidence_dir / "inventory-final.json",
    "runtime_plan": evidence_dir / "runtime-plan.json",
    "runtime_template_contract": evidence_dir / "runtime-template-contract.json",
    "runtime_bundle_manifest": evidence_dir / "runtime-bundle-manifest.json",
    "runtime_bootstrap_report": evidence_dir / "runtime-bootstrap-report.json",
    "runtime_verify_report": evidence_dir / "runtime-verify-report.json",
    "runtime_reconcile_report": evidence_dir / "runtime-reconcile-report.json",
}
payloads = {key: load_json(path) for key, path in files.items()}

run_ids = set()
change_ids = set()
for key, payload in payloads.items():
    raw_run = str(payload.get("run_id", "")).strip()
    raw_change = str(payload.get("change_id", "")).strip()
    if raw_run:
        run_ids.add(raw_run)
    if raw_change:
        change_ids.add(raw_change)

if len(run_ids) != 1 or len(change_ids) != 1:
    fail("a2_3_root_correlation_mismatch", f"run_id/change_id divergente entre artefatos: run={sorted(run_ids)} change={sorted(change_ids)}")

run_id = next(iter(run_ids))
change_id = next(iter(change_ids))

for key, payload in payloads.items():
    validate_run_change(key, payload, run_id, change_id)

corr_targets = {}
for key in ("runtime_plan", "runtime_template_contract", "runtime_bundle_manifest", "runtime_bootstrap_report", "runtime_verify_report", "runtime_reconcile_report"):
    corr = get_correlation(payloads[key], key)
    corr_run_id = str(corr.get("run_id", "")).strip()
    corr_change_id = str(corr.get("change_id", "")).strip()
    if corr_run_id and corr_run_id != run_id:
        fail("a2_3_correlation_mismatch", f"{key} com correlation.run_id divergente.")
    if corr_change_id and corr_change_id != change_id:
        fail("a2_3_correlation_mismatch", f"{key} com correlation.change_id divergente.")
    manifest_fp = str(corr.get("manifest_fingerprint", "")).strip().lower()
    source_fp = str(corr.get("source_blueprint_fingerprint", "")).strip().lower()
    if not is_sha256(manifest_fp):
        fail("a2_3_invalid_manifest_fingerprint", f"{key} sem manifest_fingerprint SHA-256 valido.")
    if not is_sha256(source_fp):
        fail("a2_3_invalid_source_blueprint_fingerprint", f"{key} sem source_blueprint_fingerprint SHA-256 valido.")
    corr_targets[key] = {"manifest_fingerprint": manifest_fp, "source_blueprint_fingerprint": source_fp}

if len({item["manifest_fingerprint"] for item in corr_targets.values()}) != 1:
    fail("a2_3_manifest_fingerprint_mismatch", "manifest_fingerprint divergente entre artefatos de runtime.")
if len({item["source_blueprint_fingerprint"] for item in corr_targets.values()}) != 1:
    fail("a2_3_source_blueprint_fingerprint_mismatch", "source_blueprint_fingerprint divergente entre artefatos de runtime.")

runtime_plan = payloads["runtime_plan"]
plan_entries = runtime_plan.get("entries")
if not isinstance(plan_entries, list) or not plan_entries:
    fail("a2_3_runtime_plan_empty", "runtime-plan sem entries elegiveis.")
if not is_sha256(str(runtime_plan.get("runtime_plan_fingerprint", "")).strip().lower()):
    fail("a2_3_runtime_plan_fingerprint_invalid", "runtime_plan_fingerprint invalido.")

runtime_template_contract = payloads["runtime_template_contract"]
template_catalog = runtime_template_contract.get("catalog") if isinstance(runtime_template_contract.get("catalog"), dict) else {}
if str(template_catalog.get("contract_version", "")).strip() == "":
    fail("a2_3_runtime_template_contract_missing", "runtime-template-contract sem catalog.contract_version.")
if not is_sha256(str(runtime_template_contract.get("contract_fingerprint", "")).strip().lower()):
    fail("a2_3_runtime_template_contract_fingerprint_invalid", "contract_fingerprint invalido.")

runtime_bundle_manifest = payloads["runtime_bundle_manifest"]
if not is_sha256(str(runtime_bundle_manifest.get("runtime_bundle_fingerprint", "")).strip().lower()):
    fail("a2_3_runtime_bundle_fingerprint_invalid", "runtime_bundle_fingerprint invalido.")

runtime_bootstrap_report = payloads["runtime_bootstrap_report"]
bootstrap_summary = runtime_bootstrap_report.get("summary")
if not isinstance(bootstrap_summary, dict):
    fail("a2_3_runtime_bootstrap_summary_invalid", "runtime-bootstrap-report sem summary valido.")
if int(bootstrap_summary.get("unit_count", 0) or 0) <= 0:
    fail("a2_3_runtime_bootstrap_unit_count_invalid", "runtime-bootstrap-report sem unit_count>0.")
if int(bootstrap_summary.get("failed_units", 0) or 0) != 0:
    fail("a2_3_runtime_bootstrap_failed_units", "runtime-bootstrap-report com failed_units>0.")
if not is_sha256(str(runtime_bootstrap_report.get("plan_fingerprint", "")).strip().lower()):
    fail("a2_3_runtime_bootstrap_plan_fingerprint_invalid", "runtime-bootstrap-report sem plan_fingerprint valido.")

runtime_verify_report = payloads["runtime_verify_report"]
runtime_verify_summary = runtime_verify_report.get("summary")
if not isinstance(runtime_verify_summary, dict):
    fail("a2_3_runtime_verify_summary_invalid", "runtime-verify-report sem summary valido.")
if int(runtime_verify_summary.get("runtime_count", 0) or 0) <= 0:
    fail("a2_3_runtime_verify_runtime_count_invalid", "runtime-verify-report sem runtime_count>0.")
if int(runtime_verify_summary.get("failed", 0) or 0) != 0:
    fail("a2_3_runtime_verify_failed", "runtime-verify-report com failed>0.")
if str(runtime_verify_report.get("executor_id", "")).strip().lower() != "provisioning-ssh-executor":
    fail("a2_3_runtime_verify_executor_invalid", "runtime-verify-report sem executor oficial.")

runtime_reconcile_report = payloads["runtime_reconcile_report"]
runtime_reconcile_summary = runtime_reconcile_report.get("summary")
if not isinstance(runtime_reconcile_summary, dict):
    fail("a2_3_runtime_reconcile_summary_invalid", "runtime-reconcile-report sem summary valido.")
if bool(runtime_reconcile_report.get("blocked", True)):
    fail("a2_3_runtime_not_converged", "runtime-reconcile-report bloqueado (runtime nao convergido).")
if int(runtime_reconcile_summary.get("required_non_converged_count", 0) or 0) != 0:
    fail("a2_3_runtime_required_not_converged", "required_non_converged_count deve ser 0.")
if not is_sha256(str(runtime_reconcile_report.get("runtime_reconcile_fingerprint", "")).strip().lower()):
    fail("a2_3_runtime_reconcile_fingerprint_invalid", "runtime_reconcile_fingerprint invalido.")

inventory_final = payloads["inventory_final"]
runtime_rows = inventory_final.get("chaincode_runtime_inventory")
if not isinstance(runtime_rows, list) or not runtime_rows:
    fail("a2_3_inventory_missing_runtime_rows", "inventory-final sem chaincode_runtime_inventory.")
for index, row in enumerate(runtime_rows):
    if not isinstance(row, dict):
        fail("a2_3_inventory_runtime_row_invalid", f"Linha de runtime invalida em inventory-final: {index}")
    runtime_name = str(row.get("name", "")).strip()
    if not runtime_name.startswith("dev-peer"):
        fail("a2_3_inventory_runtime_name_invalid", f"Linha runtime sem naming dev-peer em index {index}.")
    if not is_sha256(str(row.get("runtime_fingerprint", "")).strip().lower()):
        fail("a2_3_inventory_runtime_fingerprint_invalid", f"runtime_fingerprint invalido em index {index}.")
    if str(row.get("status", "")).strip().lower() != "running":
        fail("a2_3_inventory_runtime_not_running", f"Runtime nao-running em index {index}.")

provision_report = payloads["provision_report"]
for field in ("chaincode_runtime_plan", "chaincode_runtime_template_contract", "chaincode_runtime_bootstrap", "chaincode_runtime_verify", "chaincode_runtime_reconcile"):
    if not isinstance(provision_report.get(field), dict):
        fail("a2_3_provision_report_runtime_section_missing", f"provision-report sem secao obrigatoria: {field}")

entry = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "gate_change_id": gate_change_id,
    "evidence_dir": str(evidence_dir),
    "run_id": run_id,
    "change_id": change_id,
    "manifest_fingerprint": next(iter({item["manifest_fingerprint"] for item in corr_targets.values()})),
    "source_blueprint_fingerprint": next(iter({item["source_blueprint_fingerprint"] for item in corr_targets.values()})),
    "decision": "allow",
    "decision_reasons": [
        {
            "code": "a2_3_runtime_evidence_contract_valid",
            "message": "Evidencias minimas de runtime estao consistentes e convergidas.",
        }
    ],
    "required_evidence": {
        "runtime_plan_consistent": True,
        "runtime_bundle_integrity": True,
        "runtime_bootstrap_verified": True,
        "runtime_reconcile_converged": True,
        "runtime_inventory_complete": True,
    },
}
print(json.dumps(entry, ensure_ascii=False))
PY
then
  validation_ok=1
  if ! cat "${VALIDATION_RESULT_FILE}" >> "${HISTORY_FILE}" || ! cat "${VALIDATION_RESULT_FILE}" >> "${RUN_FILE}" || ! cat "${VALIDATION_RESULT_FILE}" >> "${DECISION_TRACE_FILE}"; then
    echo "[ERROR][a2_3_persist_history_failed] Falha ao persistir historico/trace do gate A2.3." >&2
    any_failed=1
  fi
else
  echo "[ERROR][a2_3_validation_failed] Validacao contratual das evidencias A2.3 falhou." >&2
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
        "positive_minimum_runtime_bootstrap_per_channel",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_executes_runtime_bootstrap_with_attempt_contract",
    ),
    (
        "positive_multiple_runtimes_per_peer",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_bootstraps_multiple_runtimes_for_same_peer",
    ),
    (
        "positive_idempotent_reexecution_without_drift",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_reexecution_is_idempotent_without_runtime_drift",
    ),
    (
        "negative_template_invalid",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_template_contract_blocks_missing_required_parameters",
    ),
    (
        "negative_peer_channel_out_of_scope",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_runtime_plan_is_deterministic_and_filters_ineligible",
    ),
    (
        "negative_naming_or_hash_conflict",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_blocks_when_runtime_name_conflicts_between_bindings",
    ),
    (
        "negative_host_unavailable",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_blocks_on_runtime_bootstrap_retry_exhausted_when_host_unavailable",
    ),
    (
        "negative_runtime_not_converged_after_retry",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_provision_a23_blocks_on_runtime_reconcile_not_converged_required",
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
    echo "[ERROR][a2_3_test_matrix_failed] Matriz obrigatoria de testes do WP A2.3 falhou." >&2
    any_failed=1
  fi
elif [[ "${SKIP_MATRIX}" == "1" ]]; then
  cat > "${TEST_MATRIX_FILE}" <<JSON
{
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "all_passed": false,
  "skipped": true,
  "reason": "WP_A2_3_GATE_SKIP_MATRIX=1"
}
JSON
    echo "[ERROR][a2_3_matrix_skipped_not_allowed] Matriz obrigatoria nao pode ser ignorada no gate A2.3." >&2
    any_failed=1
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
    automation.tests.test_pipeline_a2_backend_flow \
    automation.tests.test_pipeline_provision \
    automation.tests.test_pipeline_a2_3_entry_gate \
    -v > "${FULL_SUITE_LOG}" 2>&1; then
    full_suite_passed=1
  else
    echo "[ERROR][a2_3_full_suite_failed] Suite automatizada A2.3 falhou." >&2
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
readiness_checklist_file = ${READINESS_CHECKLIST_FILE@Q}
handoff_file = ${HANDOFF_FILE@Q}
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
    "runtime_plan_consistent": bool((validation_result.get("required_evidence") or {}).get("runtime_plan_consistent", False)),
    "runtime_bundle_integrity": bool((validation_result.get("required_evidence") or {}).get("runtime_bundle_integrity", False)),
    "runtime_bootstrap_verified": bool((validation_result.get("required_evidence") or {}).get("runtime_bootstrap_verified", False)),
    "runtime_reconcile_converged": bool((validation_result.get("required_evidence") or {}).get("runtime_reconcile_converged", False)),
    "runtime_inventory_complete": bool((validation_result.get("required_evidence") or {}).get("runtime_inventory_complete", False)),
    "automated_suite_passed": bool(matrix_passed and full_suite_passed),
}

decision_reasons = []
if not validation_ok:
    decision_reasons.append(
        {
            "code": "a2_3_validation_contract_failed",
            "message": "Evidencias minimas de runtime nao atenderam ao contrato tecnico do gate A2.3.",
        }
    )
if not matrix_passed:
    if skip_matrix == "1":
        decision_reasons.append(
            {
                "code": "a2_3_matrix_skipped_not_allowed",
                "message": "A matriz obrigatoria de testes A2.3 nao pode ser ignorada.",
            }
        )
    else:
        decision_reasons.append(
            {
                "code": "a2_3_required_test_matrix_failed",
                "message": "A matriz obrigatoria de testes A2.3 falhou.",
            }
        )
if validation_ok and matrix_passed and not full_suite_passed:
    decision_reasons.append(
        {
            "code": "a2_3_full_suite_failed",
            "message": "A suite automatizada A2.3 falhou na validacao final.",
        }
    )
if not decision_reasons and failed:
    decision_reasons.append(
        {
            "code": "a2_3_gate_execution_failed",
            "message": "Falha interna no gate A2.3 durante consolidacao de evidencias.",
        }
    )
if not decision_reasons:
    decision_reasons.append(
        {
            "code": "a2_3_acceptance_allow",
            "message": "Criterios obrigatorios do gate A2.3 atendidos com evidencias oficiais convergidas.",
        }
    )

readiness_checklist = {
    "runtime_converged": bool(required_evidence["runtime_reconcile_converged"] and required_evidence["runtime_bootstrap_verified"]),
    "final_inventory_complete": bool(required_evidence["runtime_inventory_complete"]),
    "reproducible_diagnostics": bool(validation_ok and bool(test_matrix.get("results")) and matrix_passed),
    "official_artifacts_integrity": bool(required_evidence["runtime_plan_consistent"] and required_evidence["runtime_bundle_integrity"]),
}
readiness_checklist["ready_for_a2_4_a2_5"] = bool(
    readiness_checklist["runtime_converged"]
    and readiness_checklist["final_inventory_complete"]
    and readiness_checklist["reproducible_diagnostics"]
    and readiness_checklist["official_artifacts_integrity"]
    and required_evidence["automated_suite_passed"]
)

handoff_dependencies = [
    {
        "dependency_id": "a2_3_runtime_convergence_baseline",
        "target_wp": "A2.4",
        "description": "Evolucao incremental de topologia exige baseline de runtime convergida e inventario oficial consistente.",
        "required_check_ids": ["runtime_converged", "final_inventory_complete", "official_artifacts_integrity"],
    },
    {
        "dependency_id": "a2_3_incremental_reconcile_contract",
        "target_wp": "A2.4",
        "description": "Expansao incremental requer reconciliação e diagnostico reproduzivel para aplicacao de diffs sem ruptura operacional.",
        "required_check_ids": ["runtime_converged", "reproducible_diagnostics"],
    },
    {
        "dependency_id": "a2_3_runtime_topology_inventory_projection",
        "target_wp": "A2.5",
        "description": "Jornada UX de topologia runtime depende de inventario final completo e artefatos oficiais integros por execucao.",
        "required_check_ids": ["final_inventory_complete", "official_artifacts_integrity"],
    },
    {
        "dependency_id": "a2_3_acceptance_decision_traceability",
        "target_wp": "A2.5",
        "description": "UX de aceite e auditoria depende de decisao allow|block e motivos deterministicos por execucao.",
        "required_check_ids": ["reproducible_diagnostics"],
    },
]

for dependency in handoff_dependencies:
    required_checks = dependency.get("required_check_ids") or []
    dependency["satisfied"] = all(bool(readiness_checklist.get(check_id, False)) for check_id in required_checks)

summary = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "change_id": change_id,
    "status": "failed" if failed else "ok",
    "decision": "block" if failed else "allow",
    "decision_reasons": decision_reasons,
    "validation_ok": validation_ok,
    "matrix_passed": matrix_passed,
    "full_suite_passed": full_suite_passed,
    "skip_matrix": skip_matrix == "1",
    "evidence_dir": evidence_dir,
    "required_evidence": required_evidence,
    "readiness_checklist": readiness_checklist,
    "validation_result": validation_result,
    "test_matrix": test_matrix,
    "history_file": history_file,
    "decision_trace_file": decision_trace_file,
    "run_file": run_file,
    "full_suite_log": full_suite_log,
    "output_dir": output_dir,
    "entries": entries,
}

with open(readiness_checklist_file, "w", encoding="utf-8") as fp:
    json.dump(
        {
            "generated_at_utc": summary["generated_at_utc"],
            "change_id": change_id,
            "decision": summary["decision"],
            "decision_reasons": decision_reasons,
            "checklist": readiness_checklist,
            "required_evidence": required_evidence,
            "validation_ok": validation_ok,
            "matrix_passed": matrix_passed,
            "full_suite_passed": full_suite_passed,
        },
        fp,
        indent=2,
        ensure_ascii=False,
    )

handoff_payload = {
    "generated_at_utc": summary["generated_at_utc"],
    "source_wp": "A2.3",
    "target_wps": ["A2.4", "A2.5"],
    "change_id": change_id,
    "run_id": str(validation_result.get("run_id", "")).strip(),
    "manifest_fingerprint": str(validation_result.get("manifest_fingerprint", "")).strip().lower(),
    "source_blueprint_fingerprint": str(validation_result.get("source_blueprint_fingerprint", "")).strip().lower(),
    "handoff_decision": "allow" if readiness_checklist["ready_for_a2_4_a2_5"] and not failed else "block",
    "handoff_reasons": decision_reasons,
    "dependencies": handoff_dependencies,
    "readiness_checklist_ref": readiness_checklist_file,
}

with open(handoff_file, "w", encoding="utf-8") as fp:
    json.dump(handoff_payload, fp, indent=2, ensure_ascii=False)

print(json.dumps(summary, indent=2, ensure_ascii=False))
PY
then
  echo "[ERROR][a2_3_summary_failed] Falha ao gerar resumo consolidado do gate A2.3." >&2
  any_failed=1
fi

if ! cp "${SUMMARY_FILE}" "${ACCEPTANCE_SUMMARY_FILE}"; then
  echo "[ERROR][a2_3_acceptance_summary_failed] Falha ao publicar resumo de aceite A2.3." >&2
  any_failed=1
fi

if [[ "${any_failed}" -ne 0 ]]; then
  echo "[ERROR][a2_3_gate_failed] Gate WP A2.3 falhou." >&2
  echo "==> Resumo consolidado: ${SUMMARY_FILE}" >&2
  exit 1
fi

echo "==> Gate WP A2.3 concluido com sucesso"
echo "==> Resumo consolidado: ${SUMMARY_FILE}"
echo "==> Resumo de aceite A2.3: ${ACCEPTANCE_SUMMARY_FILE}"
echo "==> Checklist de pronto A2.3: ${READINESS_CHECKLIST_FILE}"
echo "==> Handoff A2.3 -> A2.4/A2.5: ${HANDOFF_FILE}"
echo "==> Historico: ${HISTORY_FILE}"
