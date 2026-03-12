#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Uso: $0 <provision_evidence_dir>" >&2
  exit 2
fi

EVIDENCE_DIR="$1"
if [[ ! -d "${EVIDENCE_DIR}" ]]; then
  echo "[ERROR][a2_4_missing_evidence_dir] Diretorio de evidencias nao encontrado: ${EVIDENCE_DIR}" >&2
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

OUTPUT_DIR="${WP_A2_4_GATE_OUTPUT_DIR:-${REPO_ROOT}/.artifacts/wp-a2-4-gate}"
CHANGE_ID="${WP_A2_4_CHANGE_ID:-a2-4-$(date -u +%Y%m%dT%H%M%SZ)}"
SKIP_MATRIX="${WP_A2_4_GATE_SKIP_MATRIX:-0}"

HISTORY_FILE="${OUTPUT_DIR}/history.jsonl"
DECISION_TRACE_FILE="${OUTPUT_DIR}/decision-trace.jsonl"
CHANGE_ID_SAFE="$(printf '%s' "${CHANGE_ID}" | tr -c 'A-Za-z0-9._-' '_')"
RUN_FILE="${OUTPUT_DIR}/run-${CHANGE_ID_SAFE}.jsonl"
SUMMARY_FILE="${OUTPUT_DIR}/latest-summary.json"
ACCEPTANCE_SUMMARY_FILE="${OUTPUT_DIR}/a2-4-acceptance-summary.json"
READINESS_CHECKLIST_FILE="${OUTPUT_DIR}/a2-4-readiness-checklist.json"
HANDOFF_FILE="${OUTPUT_DIR}/a2-4-handoff-a2-5.json"
VALIDATION_RESULT_FILE="${OUTPUT_DIR}/validation-result.json"
TEST_MATRIX_FILE="${OUTPUT_DIR}/test-matrix-summary.json"
FULL_SUITE_LOG="${OUTPUT_DIR}/a2-4-unittest.log"

mkdir -p "${OUTPUT_DIR}"
touch "${HISTORY_FILE}" "${DECISION_TRACE_FILE}"
: > "${RUN_FILE}"

for artifact in \
  "provision-report.json" \
  "runtime-inventory.json" \
  "incremental-execution-plan.json" \
  "incremental-reconcile-report.json" \
  "inventory-final.json" \
  "verify-report.json" \
  "ssh-execution-log.json"; do
  if [[ ! -s "${EVIDENCE_DIR}/${artifact}" ]]; then
    echo "[ERROR][a2_4_missing_artifact] Evidencia obrigatoria ausente ou vazia: ${artifact}" >&2
    exit 1
  fi
done

echo "==> WP A2.4 gate (change_id=${CHANGE_ID})"
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
        fail("a2_4_invalid_json", f"Falha ao ler JSON de {path}: {exc}")
    if not isinstance(payload, dict):
        fail("a2_4_invalid_json_root", f"Raiz JSON invalida em {path}: esperado objeto.")
    return payload


def is_sha256(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value).strip().lower()))


evidence_dir = Path(${EVIDENCE_DIR@Q})
gate_change_id = ${CHANGE_ID@Q}

files = {
    "provision_report": evidence_dir / "provision-report.json",
    "runtime_inventory": evidence_dir / "runtime-inventory.json",
    "incremental_execution_plan": evidence_dir / "incremental-execution-plan.json",
    "incremental_reconcile_report": evidence_dir / "incremental-reconcile-report.json",
    "inventory_final": evidence_dir / "inventory-final.json",
    "verify_report": evidence_dir / "verify-report.json",
    "ssh_execution_log": evidence_dir / "ssh-execution-log.json",
}
payloads = {key: load_json(path) for key, path in files.items()}

run_ids = {}
change_ids = {}
for key, payload in payloads.items():
    run_id = str(payload.get("run_id", "")).strip()
    change_id = str(payload.get("change_id", "")).strip()
    if not run_id:
        fail("a2_4_missing_root_run_id", f"{key} sem run_id.")
    if not change_id:
        fail("a2_4_missing_root_change_id", f"{key} sem change_id.")
    run_ids[key] = run_id
    change_ids[key] = change_id

if len(set(run_ids.values())) != 1:
    fail("a2_4_root_run_id_mismatch", f"run_id divergente entre artefatos: {run_ids}")
if len(set(change_ids.values())) != 1:
    fail("a2_4_root_change_id_mismatch", f"change_id divergente entre artefatos: {change_ids}")

run_id = next(iter(set(run_ids.values())))
change_id = next(iter(set(change_ids.values())))

correlation_targets = {}
for key, payload in payloads.items():
    correlation = payload.get("correlation")
    if not isinstance(correlation, dict):
        fail("a2_4_missing_correlation", f"{key} sem bloco correlation.")
    corr_run_id = str(correlation.get("run_id", "")).strip()
    corr_change_id = str(correlation.get("change_id", "")).strip()
    manifest_fingerprint = str(correlation.get("manifest_fingerprint", "")).strip().lower()
    source_blueprint_fingerprint = str(correlation.get("source_blueprint_fingerprint", "")).strip().lower()
    if corr_run_id != run_id or corr_change_id != change_id:
        fail("a2_4_correlation_mismatch", f"{key} com correlation.run_id/change_id divergente.")
    if not is_sha256(manifest_fingerprint):
        fail("a2_4_invalid_manifest_fingerprint", f"{key} sem manifest_fingerprint SHA-256 valido.")
    if not is_sha256(source_blueprint_fingerprint):
        fail("a2_4_invalid_source_blueprint_fingerprint", f"{key} sem source_blueprint_fingerprint SHA-256 valido.")
    correlation_targets[key] = {
        "manifest_fingerprint": manifest_fingerprint,
        "source_blueprint_fingerprint": source_blueprint_fingerprint,
    }

if len({entry["manifest_fingerprint"] for entry in correlation_targets.values()}) != 1:
    fail("a2_4_manifest_fingerprint_mismatch", "manifest_fingerprint divergente entre artefatos A2.4.")
if len({entry["source_blueprint_fingerprint"] for entry in correlation_targets.values()}) != 1:
    fail("a2_4_source_blueprint_fingerprint_mismatch", "source_blueprint_fingerprint divergente entre artefatos A2.4.")

manifest_fingerprint = next(iter({entry["manifest_fingerprint"] for entry in correlation_targets.values()}))
source_blueprint_fingerprint = next(iter({entry["source_blueprint_fingerprint"] for entry in correlation_targets.values()}))

incremental_plan = payloads["incremental_execution_plan"]
if not is_sha256(str(incremental_plan.get("incremental_plan_fingerprint", "")).strip().lower()):
    fail("a2_4_incremental_plan_fingerprint_invalid", "incremental_plan_fingerprint invalido.")
plan_entries = incremental_plan.get("entries") if isinstance(incremental_plan.get("entries"), list) else []
if not plan_entries:
    fail("a2_4_incremental_plan_entries_empty", "incremental-execution-plan sem entries.")

incremental_reconcile = payloads["incremental_reconcile_report"]
if bool(incremental_reconcile.get("blocked", False)):
    fail("a2_4_incremental_reconcile_blocked", "incremental-reconcile-report bloqueado.")
rows = incremental_reconcile.get("rows") if isinstance(incremental_reconcile.get("rows"), list) else []
if not rows:
    fail("a2_4_incremental_reconcile_rows_empty", "incremental-reconcile-report sem rows.")

inventory_final = payloads["inventory_final"]
incremental_origin_metadata = inventory_final.get("incremental_origin_metadata")
if not isinstance(incremental_origin_metadata, dict):
    fail("a2_4_inventory_incremental_origin_missing", "inventory-final sem incremental_origin_metadata.")
if int(incremental_origin_metadata.get("topology_generation", 0) or 0) <= 0:
    fail("a2_4_inventory_topology_generation_invalid", "topology_generation invalido em incremental_origin_metadata.")
components = incremental_origin_metadata.get("components") if isinstance(incremental_origin_metadata.get("components"), list) else []
if not components:
    fail("a2_4_inventory_incremental_components_missing", "incremental_origin_metadata sem components.")
for index, component in enumerate(components):
    if not isinstance(component, dict):
        fail("a2_4_inventory_incremental_component_invalid", f"Componente incremental invalido no indice {index}.")
    operation_type = str(component.get("operation_type", "")).strip().lower()
    expanded_at_run_id = str(component.get("expanded_at_run_id", "")).strip()
    topology_generation = int(component.get("topology_generation", 0) or 0)
    if not operation_type.startswith("incremental_"):
        fail("a2_4_inventory_incremental_operation_invalid", f"operation_type invalido no componente incremental {index}.")
    if expanded_at_run_id != run_id:
        fail("a2_4_inventory_incremental_expanded_run_mismatch", "expanded_at_run_id divergente no inventory-final.")
    if topology_generation <= 0:
        fail("a2_4_inventory_incremental_topology_generation_invalid", "topology_generation invalido no componente incremental.")

verify_report = payloads["verify_report"]
if str(verify_report.get("decision", "")).strip().lower() != "allow" or bool(verify_report.get("blocked", True)):
    fail("a2_4_verify_decision_block", "verify-report sem decision=allow.")
decision_reasons = verify_report.get("decision_reasons")
if not isinstance(decision_reasons, list) or not decision_reasons:
    fail("a2_4_verify_decision_reasons_missing", "verify-report sem decision_reasons deterministicas.")

provision_report = payloads["provision_report"]
a24_gate_payload = provision_report.get("a2_4_incremental_entry_gate")
if not isinstance(a24_gate_payload, dict):
    fail("a2_4_provision_missing_incremental_gate_payload", "provision-report sem a2_4_incremental_entry_gate.")
if not bool(a24_gate_payload.get("allowed", False)):
    fail("a2_4_provision_incremental_gate_blocked", "a2_4_incremental_entry_gate sem allowed=true.")

continuity = provision_report.get("incremental_operational_continuity")
if not isinstance(continuity, dict):
    fail("a2_4_provision_missing_operational_continuity", "provision-report sem incremental_operational_continuity.")
if bool(continuity.get("blocked", True)):
    fail("a2_4_operational_continuity_blocked", "incremental_operational_continuity bloqueado.")

runtime_inventory = payloads["runtime_inventory"]
hosts = runtime_inventory.get("hosts") if isinstance(runtime_inventory.get("hosts"), list) else []
if not hosts:
    fail("a2_4_runtime_inventory_hosts_missing", "runtime-inventory sem hosts.")

ssh_log_wrapper = payloads["ssh_execution_log"]
ssh_log = ssh_log_wrapper.get("ssh_execution_log")
if not isinstance(ssh_log, dict):
    fail("a2_4_ssh_execution_log_missing", "ssh-execution-log sem objeto ssh_execution_log.")
if str(ssh_log.get("run_id", "")).strip() != run_id or str(ssh_log.get("change_id", "")).strip() != change_id:
    fail("a2_4_ssh_execution_log_correlation_mismatch", "ssh_execution_log com run/change divergente.")

entry = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "gate_change_id": gate_change_id,
    "evidence_dir": str(evidence_dir),
    "run_id": run_id,
    "change_id": change_id,
    "manifest_fingerprint": manifest_fingerprint,
    "source_blueprint_fingerprint": source_blueprint_fingerprint,
    "decision": "allow",
    "decision_reasons": [
        {
            "code": "a2_4_acceptance_evidence_contract_valid",
            "message": "Evidencias incrementais oficiais estao consistentes e convergidas.",
        }
    ],
    "required_evidence": {
        "incremental_add_peer_with_couch_pairing": True,
        "incremental_add_orderer_with_convergence": True,
        "incremental_reexecution_idempotent_without_drift": True,
        "baseline_preexisting_services_preserved": True,
        "incremental_artifacts_correlated": True,
        "deterministic_decision_reasons_published": True,
    },
}
print(json.dumps(entry, ensure_ascii=False))
PY
then
  validation_ok=1
  if ! cat "${VALIDATION_RESULT_FILE}" >> "${HISTORY_FILE}" || ! cat "${VALIDATION_RESULT_FILE}" >> "${RUN_FILE}" || ! cat "${VALIDATION_RESULT_FILE}" >> "${DECISION_TRACE_FILE}"; then
    echo "[ERROR][a2_4_persist_history_failed] Falha ao persistir historico/trace do gate A2.4." >&2
    any_failed=1
  fi
else
  echo "[ERROR][a2_4_validation_failed] Validacao contratual das evidencias A2.4 falhou." >&2
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
        "positive_add_peer_with_couch_pairing",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_provision_stage_add_peer_materializes_peer_and_paired_couch",
    ),
    (
        "positive_add_orderer_with_convergence",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_provision_stage_add_orderer_materializes_orderer_and_governance_context",
    ),
    (
        "positive_idempotent_reexecution_without_drift",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_provision_stage_invalidates_short_circuit_when_incremental_input_changes",
    ),
    (
        "positive_preserve_preexisting_baseline",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_incremental_operational_continuity_allows_when_mandatory_services_stay_running",
    ),
    (
        "negative_incremental_naming_collision",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_gate_blocks_when_incremental_name_collides_with_existing_component",
    ),
    (
        "negative_host_without_capacity_or_availability",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_gate_blocks_when_target_host_is_unavailable",
    ),
    (
        "negative_critical_port_conflict",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_gate_blocks_on_critical_port_conflict_before_remote_execution",
    ),
    (
        "negative_missing_secure_credential_reference",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_provision_stage_blocks_incremental_when_secure_reference_is_missing",
    ),
    (
        "negative_mandatory_existing_component_regression",
        "automation.tests.test_pipeline_provision.PipelineProvisionTests.test_incremental_operational_continuity_blocks_on_mandatory_regression",
    ),
    (
        "negative_new_component_not_converged",
        "automation.tests.test_pipeline_a2_4_entry_gate.PipelineA24EntryGateTests.test_provision_stage_blocks_when_incremental_required_component_is_not_converged",
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
    echo "[ERROR][a2_4_test_matrix_failed] Matriz obrigatoria de testes do WP A2.4 falhou." >&2
    any_failed=1
  fi
elif [[ "${SKIP_MATRIX}" == "1" ]]; then
  cat > "${TEST_MATRIX_FILE}" <<JSON
{
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "all_passed": false,
  "skipped": true,
  "reason": "WP_A2_4_GATE_SKIP_MATRIX=1"
}
JSON
  echo "[ERROR][a2_4_matrix_skipped_not_allowed] Matriz obrigatoria nao pode ser ignorada no gate A2.4." >&2
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
    automation.tests.test_pipeline_a2_4_entry_gate \
    automation.tests.test_pipeline_provision \
    -v > "${FULL_SUITE_LOG}" 2>&1; then
    full_suite_passed=1
  else
    echo "[ERROR][a2_4_full_suite_failed] Suite focada A2.4 falhou." >&2
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
    "incremental_add_peer_with_couch_pairing": bool((validation_result.get("required_evidence") or {}).get("incremental_add_peer_with_couch_pairing", False)),
    "incremental_add_orderer_with_convergence": bool((validation_result.get("required_evidence") or {}).get("incremental_add_orderer_with_convergence", False)),
    "incremental_reexecution_idempotent_without_drift": bool((validation_result.get("required_evidence") or {}).get("incremental_reexecution_idempotent_without_drift", False)),
    "baseline_preexisting_services_preserved": bool((validation_result.get("required_evidence") or {}).get("baseline_preexisting_services_preserved", False)),
    "incremental_artifacts_correlated": bool((validation_result.get("required_evidence") or {}).get("incremental_artifacts_correlated", False)),
    "deterministic_decision_reasons_published": bool((validation_result.get("required_evidence") or {}).get("deterministic_decision_reasons_published", False)),
    "automated_suite_passed": bool(matrix_passed and full_suite_passed),
}

decision_reasons = []
if not validation_ok:
    decision_reasons.append(
        {
            "code": "a2_4_validation_contract_failed",
            "message": "Evidencias minimas incrementais nao atenderam ao contrato tecnico do gate A2.4.",
        }
    )
if not matrix_passed:
    if skip_matrix == "1":
        decision_reasons.append(
            {
                "code": "a2_4_matrix_skipped_not_allowed",
                "message": "A matriz obrigatoria de testes A2.4 nao pode ser ignorada.",
            }
        )
    else:
        decision_reasons.append(
            {
                "code": "a2_4_required_test_matrix_failed",
                "message": "A matriz obrigatoria de testes A2.4 falhou.",
            }
        )
if validation_ok and matrix_passed and not full_suite_passed:
    decision_reasons.append(
        {
            "code": "a2_4_full_suite_failed",
            "message": "A suite focada A2.4 falhou na validacao final.",
        }
    )
if not decision_reasons and failed:
    decision_reasons.append(
        {
            "code": "a2_4_gate_execution_failed",
            "message": "Falha interna no gate A2.4 durante consolidacao de evidencias.",
        }
    )
if not decision_reasons:
    decision_reasons.append(
        {
            "code": "a2_4_acceptance_allow",
            "message": "Criterios obrigatorios do gate A2.4 atendidos com estabilidade operacional incremental comprovada.",
        }
    )

readiness_checklist = {
    "incremental_add_peer_with_couch_pairing": bool(required_evidence["incremental_add_peer_with_couch_pairing"]),
    "incremental_add_orderer_with_convergence": bool(required_evidence["incremental_add_orderer_with_convergence"]),
    "incremental_reexecution_idempotent_without_drift": bool(required_evidence["incremental_reexecution_idempotent_without_drift"]),
    "baseline_preexisting_services_preserved": bool(required_evidence["baseline_preexisting_services_preserved"]),
    "incremental_artifacts_correlated": bool(required_evidence["incremental_artifacts_correlated"]),
    "deterministic_decision_reasons_published": bool(required_evidence["deterministic_decision_reasons_published"]),
    "required_matrix_and_suite_passed": bool(required_evidence["automated_suite_passed"]),
}
readiness_checklist["ready_for_a2_5"] = all(bool(value) for value in readiness_checklist.values())

handoff_dependencies = [
    {
        "dependency_id": "a2_4_incremental_operational_stability",
        "target_wp": "A2.5",
        "description": "A jornada UX incremental depende de estabilidade operacional comprovada em add_peer/add_orderer sem regressao da baseline.",
        "required_check_ids": [
            "incremental_add_peer_with_couch_pairing",
            "incremental_add_orderer_with_convergence",
            "baseline_preexisting_services_preserved",
        ],
    },
    {
        "dependency_id": "a2_4_incremental_idempotency_and_checkpoint",
        "target_wp": "A2.5",
        "description": "A experiencia de reexecucao guiada exige semantica idempotente e short-circuit seguro por hash.",
        "required_check_ids": [
            "incremental_reexecution_idempotent_without_drift",
            "required_matrix_and_suite_passed",
        ],
    },
    {
        "dependency_id": "a2_4_auditability_and_deterministic_decision",
        "target_wp": "A2.5",
        "description": "A UI incremental depende de evidencias correlacionadas e decisao final allow|block com motivos deterministicos por execucao.",
        "required_check_ids": [
            "incremental_artifacts_correlated",
            "deterministic_decision_reasons_published",
        ],
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
    "source_wp": "A2.4",
    "target_wps": ["A2.5"],
    "change_id": change_id,
    "run_id": str(validation_result.get("run_id", "")).strip(),
    "manifest_fingerprint": str(validation_result.get("manifest_fingerprint", "")).strip().lower(),
    "source_blueprint_fingerprint": str(validation_result.get("source_blueprint_fingerprint", "")).strip().lower(),
    "handoff_decision": "allow" if readiness_checklist["ready_for_a2_5"] and not failed else "block",
    "handoff_reasons": decision_reasons,
    "dependencies": handoff_dependencies,
    "readiness_checklist_ref": readiness_checklist_file,
}
with open(handoff_file, "w", encoding="utf-8") as fp:
    json.dump(handoff_payload, fp, indent=2, ensure_ascii=False)

print(json.dumps(summary, indent=2, ensure_ascii=False))
PY
then
  echo "[ERROR][a2_4_summary_failed] Falha ao gerar resumo consolidado do gate A2.4." >&2
  any_failed=1
fi

if ! cp "${SUMMARY_FILE}" "${ACCEPTANCE_SUMMARY_FILE}"; then
  echo "[ERROR][a2_4_acceptance_summary_failed] Falha ao publicar resumo de aceite A2.4." >&2
  any_failed=1
fi

if [[ "${any_failed}" -ne 0 ]]; then
  echo "[ERROR][a2_4_gate_failed] Gate WP A2.4 falhou." >&2
  echo "==> Resumo consolidado: ${SUMMARY_FILE}" >&2
  exit 1
fi

echo "==> Gate WP A2.4 concluido com sucesso"
echo "==> Resumo consolidado: ${SUMMARY_FILE}"
echo "==> Resumo de aceite A2.4: ${ACCEPTANCE_SUMMARY_FILE}"
echo "==> Checklist de pronto A2.4: ${READINESS_CHECKLIST_FILE}"
echo "==> Handoff A2.4 -> A2.5: ${HANDOFF_FILE}"
echo "==> Historico: ${HISTORY_FILE}"
