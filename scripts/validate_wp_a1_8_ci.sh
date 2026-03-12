#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 5 ]]; then
  echo "Uso: $0 <pipeline_evidence_dir> <wp_a1_5_json> <wp_a1_6_json> <wp_a1_7_json> <wp_a1_8_json>" >&2
  exit 2
fi

PIPELINE_EVIDENCE_DIR="$1"
WP_A1_5_FILE="$2"
WP_A1_6_FILE="$3"
WP_A1_7_FILE="$4"
WP_A1_8_FILE="$5"

PIPELINE_REPORT_FILE="${PIPELINE_EVIDENCE_DIR}/pipeline-report.json"
STAGE_REPORTS_DIR="${PIPELINE_EVIDENCE_DIR}/stage-reports"
INVENTORY_FINAL_FILE="${PIPELINE_EVIDENCE_DIR}/inventory-final.json"
CRYPTO_INVENTORY_FILE="${PIPELINE_EVIDENCE_DIR}/configure/crypto-inventory.json"
HISTORY_FILE="${PIPELINE_EVIDENCE_DIR}/history.jsonl"
DECISION_TRACE_FILE="${PIPELINE_EVIDENCE_DIR}/decision-trace.jsonl"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ ! -d "${PIPELINE_EVIDENCE_DIR}" ]]; then
  echo "[ERROR][a1_8_missing_pipeline_evidence_dir] Diretório de evidências do pipeline não encontrado: ${PIPELINE_EVIDENCE_DIR}" >&2
  exit 1
fi

if [[ ! -s "${PIPELINE_REPORT_FILE}" ]]; then
    echo "[ERROR][a1_8_missing_pipeline_report] pipeline-report.json ausente ou vazio em ${PIPELINE_EVIDENCE_DIR}" >&2
    exit 1
fi
if [[ ! -d "${STAGE_REPORTS_DIR}" ]]; then
    echo "[ERROR][a1_8_missing_stage_reports] stage-reports/ ausente em ${PIPELINE_EVIDENCE_DIR}" >&2
    exit 1
fi
if [[ ! -s "${INVENTORY_FINAL_FILE}" ]]; then
    echo "[ERROR][a1_8_missing_inventory_final] inventory-final.json ausente ou vazio em ${PIPELINE_EVIDENCE_DIR}" >&2
    exit 1
fi
if [[ ! -s "${CRYPTO_INVENTORY_FILE}" ]]; then
    echo "[ERROR][a1_8_missing_crypto_inventory] configure/crypto-inventory.json ausente ou vazio em ${PIPELINE_EVIDENCE_DIR}" >&2
    exit 1
fi
if [[ ! -s "${HISTORY_FILE}" ]]; then
    echo "[ERROR][a1_8_missing_history] history.jsonl ausente ou vazio em ${PIPELINE_EVIDENCE_DIR}" >&2
    exit 1
fi
if [[ ! -s "${DECISION_TRACE_FILE}" ]]; then
    echo "[ERROR][a1_8_missing_decision_trace] decision-trace.jsonl ausente ou vazio em ${PIPELINE_EVIDENCE_DIR}" >&2
    exit 1
fi

for stage_report in prepare-report.json provision-report.json configure-report.json verify-report.json; do
    if [[ ! -s "${STAGE_REPORTS_DIR}/${stage_report}" ]]; then
        echo "[ERROR][a1_8_missing_stage_report] stage-reports/${stage_report} ausente ou vazio em ${PIPELINE_EVIDENCE_DIR}" >&2
        exit 1
    fi
done

for file in "${WP_A1_5_FILE}" "${WP_A1_6_FILE}" "${WP_A1_7_FILE}" "${WP_A1_8_FILE}"; do
  if [[ ! -s "${file}" ]]; then
    echo "[ERROR][a1_8_missing_acceptance_artifact] Evidência de aceite ausente ou vazia: ${file}" >&2
    exit 1
  fi
done

echo "==> Running mandatory A1.3 pipeline gate before A1.8 closure"
"${REPO_ROOT}/scripts/validate_pipeline_a1_3_ci.sh" "${PIPELINE_EVIDENCE_DIR}"

python3 - <<PY
import json
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path


def fail(code: str, message: str) -> None:
    print(f"[ERROR][{code}] {message}", file=sys.stderr)
    raise SystemExit(1)


def load_json(path_text: str):
    path = Path(path_text)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail("a1_8_invalid_json", f"Falha ao ler JSON de {path}: {exc}")
    if not isinstance(payload, dict):
        fail("a1_8_invalid_json_root", f"Raiz JSON inválida em {path}: esperado objeto.")
    return payload


def is_utc_iso_8601(timestamp_text: str) -> bool:
    if not isinstance(timestamp_text, str) or not timestamp_text.endswith("Z"):
        return False
    try:
        datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


pipeline_dir = Path(${PIPELINE_EVIDENCE_DIR@Q})
repo_root = Path(${REPO_ROOT@Q})
pipeline_report_path = pipeline_dir / "pipeline-report.json"
if not pipeline_report_path.exists():
    fail("a1_8_missing_pipeline_report", f"pipeline-report.json ausente em {pipeline_dir}")

pipeline_report = load_json(str(pipeline_report_path))
stage_reports_dir = pipeline_dir / "stage-reports"
prepare_report = load_json(str(stage_reports_dir / "prepare-report.json"))
provision_report = load_json(str(stage_reports_dir / "provision-report.json"))
configure_report = load_json(str(stage_reports_dir / "configure-report.json"))
verify_report = load_json(str(stage_reports_dir / "verify-report.json"))
inventory_final = load_json(str(pipeline_dir / "inventory-final.json"))
crypto_inventory = load_json(str(pipeline_dir / "configure" / "crypto-inventory.json"))

history_path = pipeline_dir / "history.jsonl"
decision_trace_path = pipeline_dir / "decision-trace.jsonl"
history_lines = [line.strip() for line in history_path.read_text(encoding="utf-8").splitlines() if line.strip()]
trace_lines = [line.strip() for line in decision_trace_path.read_text(encoding="utf-8").splitlines() if line.strip()]
if not history_lines:
    fail("a1_8_missing_history_entries", "history.jsonl sem entradas válidas")
if not trace_lines:
    fail("a1_8_missing_decision_trace_entries", "decision-trace.jsonl sem entradas válidas")

last_history = json.loads(history_lines[-1])
last_trace = json.loads(trace_lines[-1])

for required in ("run_id", "change_id", "fingerprint_sha256", "decision", "evidence_valid", "valid"):
    if required not in pipeline_report:
        fail("a1_8_pipeline_contract_missing_field", f"pipeline-report sem campo obrigatório: {required}")

if pipeline_report.get("decision") != "allow":
    fail("a1_8_pipeline_decision_block", "pipeline-report indica decision != allow")
if pipeline_report.get("evidence_valid") is not True:
    fail("a1_8_pipeline_evidence_invalid", "pipeline-report indica evidence_valid != true")
if pipeline_report.get("valid") is not True:
    fail("a1_8_pipeline_valid_false", "pipeline-report indica valid != true")

stage_statuses = pipeline_report.get("stage_statuses") if isinstance(pipeline_report.get("stage_statuses"), dict) else {}
for stage in ("prepare", "provision", "configure", "verify"):
    if stage not in stage_statuses:
        fail("a1_8_stage_status_missing", f"pipeline-report sem status da etapa: {stage}")

verify_stage_status = str(stage_statuses.get("verify", "")).strip().lower()
if verify_stage_status in {"failed", "blocked", "running"}:
    fail("a1_8_verify_stage_inconsistent", "pipeline-report indica estado inconsistente de verify para aceite final")

verify_verdict = str(verify_report.get("verdict", "")).strip().lower()
if verify_verdict not in {"success", "partial"}:
    fail("a1_8_verify_verdict_inconsistent", "verify-report sem veredito consistente para aceite final")

for required in ("run_id", "change_id", "fingerprint_sha256"):
    if str(inventory_final.get(required, "")).strip() != str(pipeline_report.get(required, "")).strip():
        fail("a1_8_inventory_correlation_mismatch", f"inventory-final divergente do pipeline-report em {required}")

for required in ("timestamp_utc", "run_id", "change_id", "fingerprint_sha256", "decision"):
    if required not in last_history:
        fail("a1_8_history_contract_missing_field", f"history.jsonl sem campo obrigatório: {required}")
    if required not in last_trace:
        fail("a1_8_decision_trace_contract_missing_field", f"decision-trace.jsonl sem campo obrigatório: {required}")

if str(last_history.get("run_id", "")).strip() != str(pipeline_report.get("run_id", "")).strip():
    fail("a1_8_history_run_mismatch", "history.jsonl divergente do run_id oficial")
if str(last_trace.get("run_id", "")).strip() != str(pipeline_report.get("run_id", "")).strip():
    fail("a1_8_decision_trace_run_mismatch", "decision-trace.jsonl divergente do run_id oficial")
if str(last_history.get("decision", "")).strip() != "allow":
    fail("a1_8_history_decision_mismatch", "history.jsonl final sem decision=allow")
if str(last_trace.get("decision", "")).strip() != "allow":
    fail("a1_8_decision_trace_decision_mismatch", "decision-trace final sem decision=allow")

a15 = load_json(${WP_A1_5_FILE@Q})
a16 = load_json(${WP_A1_6_FILE@Q})
a17 = load_json(${WP_A1_7_FILE@Q})
a18 = load_json(${WP_A1_8_FILE@Q})


def assert_wp(payload, wp_code: str, code_prefix: str) -> None:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    if metadata.get("wp") != wp_code:
        fail(f"{code_prefix}_wrong_wp", f"metadata.wp esperado {wp_code}, obtido {metadata.get('wp')!r}")
    if summary.get("accepted") is not True:
        fail(f"{code_prefix}_not_accepted", f"summary.accepted != true para {wp_code}")


assert_wp(a15, "A1.5", "a1_8_a15")
assert_wp(a16, "A1.6", "a1_8_a16")
assert_wp(a17, "A1.7", "a1_8_a17")
assert_wp(a18, "A1.8", "a1_8_a18")

a17_metadata = a17.get("metadata") if isinstance(a17.get("metadata"), dict) else {}
if str(a17_metadata.get("item", "")).strip() != "A1.7.9":
    fail("a1_8_a17_item_mismatch", "metadata.item de A1.7 deve ser A1.7.9")

a17_policy = a17.get("policy") if isinstance(a17.get("policy"), dict) else {}
if bool(a17_policy.get("blueprint_local_mode_enabled")):
    fail("a1_8_policy_local_mode_enabled", "Blueprint local mode deve estar desabilitado para fechamento operacional A1")
if bool(a17_policy.get("runbook_local_mode_enabled")):
    fail("a1_8_policy_local_mode_enabled", "Runbook local mode deve estar desabilitado para fechamento operacional A1")

a17_flow = a17.get("e2e_official_flow") if isinstance(a17.get("e2e_official_flow"), dict) else {}
if str(a17_flow.get("provider_key", "")).strip() != "external-linux":
    fail("a1_8_provider_scope_mismatch", "A1.7 precisa comprovar provider_key=external-linux")
host_proof = a17_flow.get("host_activity_proof") if isinstance(a17_flow.get("host_activity_proof"), dict) else {}
if not str(host_proof.get("ssh_probe_command", "")).strip():
    fail("a1_8_missing_host_activity_proof", "A1.7 sem prova de atividade real no host alvo")

a18_metadata = a18.get("metadata") if isinstance(a18.get("metadata"), dict) else {}
if str(a18_metadata.get("item", "")).strip() != "A1.8.final":
    fail("a1_8_item_mismatch", "metadata.item de A1.8 deve ser A1.8.final")

required_out_of_scope = {
    "Core do Orquestrador",
    "Gateway ccapi, Lifecycle e Guardrails",
    "Console Operacional e Visualização de Rede",
    "Governança, IAM e Segurança End-to-End",
    "Observabilidade Multi-Escala e SRE",
    "Marketplace, Templates e Integrações",
    "Validação, Benchmark/Chaos e Readiness",
}

scope = a18.get("scope") if isinstance(a18.get("scope"), dict) else {}
out_of_scope_items = set(scope.get("out_of_scope", [])) if isinstance(scope.get("out_of_scope"), list) else set()
missing_out_of_scope = sorted(required_out_of_scope - out_of_scope_items)
if missing_out_of_scope:
    fail(
        "a1_8_out_of_scope_missing",
        "Itens fora de escopo obrigatórios ausentes em A1.8: " + ", ".join(missing_out_of_scope),
    )

out_of_scope_allocations = scope.get("out_of_scope_allocations") if isinstance(scope.get("out_of_scope_allocations"), list) else []
if len(out_of_scope_allocations) != len(required_out_of_scope):
    fail(
        "a1_8_out_of_scope_allocation_missing",
        "Mapeamento de destino por épico ausente/incompleto para itens fora de escopo de A1.8",
    )

allocation_items = set()
for allocation in out_of_scope_allocations:
    if not isinstance(allocation, dict):
        fail("a1_8_out_of_scope_allocation_invalid", "Entrada inválida em out_of_scope_allocations")
    item = str(allocation.get("item", "")).strip()
    target_epic = str(allocation.get("target_epic", "")).strip()
    if not item or not target_epic:
        fail("a1_8_out_of_scope_allocation_invalid", "Item fora de escopo sem target_epic explícito")
    if item not in required_out_of_scope:
        fail("a1_8_out_of_scope_allocation_unknown_item", f"Item de alocação fora de escopo desconhecido: {item}")
    allocation_items.add(item)

missing_allocations = sorted(required_out_of_scope - allocation_items)
if missing_allocations:
    fail(
        "a1_8_out_of_scope_allocation_missing",
        "Itens fora de escopo sem destino de épico: " + ", ".join(missing_allocations),
    )

wp_closure = a18.get("wp_closure") if isinstance(a18.get("wp_closure"), list) else []
closure_map = {}
for row in wp_closure:
    if isinstance(row, dict):
        wp = str(row.get("wp", "")).strip()
        status = str(row.get("status", "")).strip().lower()
        if wp:
            closure_map[wp] = status

for wp in ("A1.1", "A1.2", "A1.3", "A1.4", "A1.5", "A1.6", "A1.7"):
    if closure_map.get(wp) != "concluido":
        fail("a1_8_wp_closure_incomplete", f"{wp} sem status concluido no fechamento A1.8")

baseline = a18.get("final_baseline") if isinstance(a18.get("final_baseline"), dict) else {}
if str(baseline.get("provider_key", "")).strip() != "external-linux":
    fail("a1_8_baseline_provider_mismatch", "final_baseline.provider_key deve ser external-linux")
if str(baseline.get("operational_mode", "")).strip() != "official":
    fail("a1_8_baseline_mode_mismatch", "final_baseline.operational_mode deve ser official")
if bool(baseline.get("fallback_local_allowed")):
    fail("a1_8_baseline_fallback_forbidden", "final_baseline.fallback_local_allowed deve ser false")

correlation = baseline.get("correlation") if isinstance(baseline.get("correlation"), dict) else {}
run_id = str(correlation.get("run_id", "")).strip()
change_id = str(correlation.get("change_id", "")).strip()
fingerprint = str(correlation.get("fingerprint_sha256", "")).strip()

if not run_id or not change_id or len(fingerprint) != 64:
    fail("a1_8_missing_correlation_fields", "Correlação obrigatória de baseline incompleta em A1.8")

if run_id != str(pipeline_report.get("run_id", "")).strip():
    fail("a1_8_run_id_mismatch", "run_id de A1.8 difere do pipeline-report oficial")
if change_id != str(pipeline_report.get("change_id", "")).strip():
    fail("a1_8_change_id_mismatch", "change_id de A1.8 difere do pipeline-report oficial")
if fingerprint != str(pipeline_report.get("fingerprint_sha256", "")).strip():
    fail("a1_8_fingerprint_mismatch", "fingerprint_sha256 de A1.8 difere do pipeline-report oficial")

checks = a18.get("checks") if isinstance(a18.get("checks"), list) else []
if not checks:
    fail("a1_8_missing_checks", "A1.8 sem checks de aceite final")

if not all(isinstance(check, dict) and check.get("passed") is True for check in checks):
    fail("a1_8_failed_check", "A1.8 contém check reprovado")

closure_dossier = a18.get("closure_dossier") if isinstance(a18.get("closure_dossier"), dict) else {}
if not closure_dossier:
    fail("a1_8_missing_closure_dossier", "A1.8 sem closure_dossier obrigatório")

technical_artifact = (
    closure_dossier.get("technical_acceptance_artifact")
    if isinstance(closure_dossier.get("technical_acceptance_artifact"), dict)
    else {}
)
if str(technical_artifact.get("id", "")).strip() != "a1-delivery-1-closure-dossier":
    fail("a1_8_closure_dossier_artifact_id_invalid", "closure_dossier.technical_acceptance_artifact.id inválido")
artifact_generated_at = str(technical_artifact.get("generated_at_utc", "")).strip()
if not is_utc_iso_8601(artifact_generated_at):
    fail("a1_8_closure_dossier_timestamp_invalid", "closure_dossier com generated_at_utc fora de UTC ISO-8601")

test_trail = closure_dossier.get("test_trail") if isinstance(closure_dossier.get("test_trail"), dict) else {}
backend_tests = test_trail.get("backend") if isinstance(test_trail.get("backend"), list) else []
frontend_tests = test_trail.get("frontend") if isinstance(test_trail.get("frontend"), list) else []
if not backend_tests:
    fail("a1_8_missing_backend_test_trail", "closure_dossier sem trilha de testes backend")
if not frontend_tests:
    fail("a1_8_missing_frontend_test_trail", "closure_dossier sem trilha de testes frontend")

final_snapshot = (
    closure_dossier.get("final_inventory_snapshot")
    if isinstance(closure_dossier.get("final_inventory_snapshot"), dict)
    else {}
)
operational_snapshot = final_snapshot.get("operational") if isinstance(final_snapshot.get("operational"), dict) else {}
cryptographic_snapshot = final_snapshot.get("cryptographic") if isinstance(final_snapshot.get("cryptographic"), dict) else {}
if str(operational_snapshot.get("source", "")).strip() != "inventory-final.json":
    fail("a1_8_operational_snapshot_source_invalid", "Snapshot operacional deve referenciar inventory-final.json")
if str(cryptographic_snapshot.get("source", "")).strip() != "configure/crypto-inventory.json":
    fail("a1_8_crypto_snapshot_source_invalid", "Snapshot criptográfico deve referenciar configure/crypto-inventory.json")

immutability = closure_dossier.get("immutability") if isinstance(closure_dossier.get("immutability"), dict) else {}
if immutability.get("immutable_post_closure") is not True:
    fail("a1_8_immutability_not_enforced", "closure_dossier deve marcar immutable_post_closure=true")

signature = (
    closure_dossier.get("final_scope_conformity_checklist")
    if isinstance(closure_dossier.get("final_scope_conformity_checklist"), dict)
    else {}
)
if signature.get("signed") is not True:
    fail("a1_8_missing_technical_signature", "Checklist final sem assinatura técnica")
if signature.get("scope_conformity") is not True:
    fail("a1_8_scope_conformity_not_signed", "Checklist final sem conformidade explícita de escopo")
signature_signed_at = str(signature.get("signed_at_utc", "")).strip()
if not is_utc_iso_8601(signature_signed_at):
    fail("a1_8_signature_timestamp_invalid", "signed_at_utc da assinatura técnica fora de UTC ISO-8601")
if not str(signature.get("signed_by", "")).strip():
    fail("a1_8_signature_signatory_missing", "Checklist final sem signed_by")

transition_a1_to_a2 = a18.get("transition_a1_to_a2") if isinstance(a18.get("transition_a1_to_a2"), dict) else {}
if not transition_a1_to_a2:
    fail("a1_8_transition_missing", "A1.8 sem bloco transition_a1_to_a2")

if str(transition_a1_to_a2.get("target_epic", "")).strip() != "A2":
    fail("a1_8_transition_target_epic_invalid", "transition_a1_to_a2.target_epic deve ser A2")

transition_generated_at = str(transition_a1_to_a2.get("handoff_generated_at_utc", "")).strip()
if transition_generated_at and not is_utc_iso_8601(transition_generated_at):
    fail("a1_8_transition_timestamp_invalid", "transition_a1_to_a2.handoff_generated_at_utc fora de UTC ISO-8601")

transition_correlation = (
    transition_a1_to_a2.get("correlation")
    if isinstance(transition_a1_to_a2.get("correlation"), dict)
    else {}
)
if str(transition_correlation.get("run_id", "")).strip() != run_id:
    fail("a1_8_transition_run_id_mismatch", "transition_a1_to_a2.correlation.run_id divergente")
if str(transition_correlation.get("change_id", "")).strip() != change_id:
    fail("a1_8_transition_change_id_mismatch", "transition_a1_to_a2.correlation.change_id divergente")
if str(transition_correlation.get("fingerprint_sha256", "")).strip() != fingerprint:
    fail("a1_8_transition_fingerprint_mismatch", "transition_a1_to_a2.correlation.fingerprint_sha256 divergente")

consumed_prerequisites = (
    transition_a1_to_a2.get("consumed_prerequisites")
    if isinstance(transition_a1_to_a2.get("consumed_prerequisites"), list)
    else []
)
if not consumed_prerequisites:
    fail("a1_8_transition_prerequisites_missing", "transition_a1_to_a2 sem consumed_prerequisites")

for row in consumed_prerequisites:
    if not isinstance(row, dict):
        fail("a1_8_transition_prerequisite_invalid", "Entrada inválida em consumed_prerequisites")
    if not str(row.get("id", "")).strip():
        fail("a1_8_transition_prerequisite_invalid", "Pré-requisito sem id")
    if not str(row.get("target_epic", "")).strip() or not str(row.get("target_module", "")).strip():
        fail("a1_8_transition_prerequisite_target_missing", "Pré-requisito sem target_epic/target_module")
    if not str(row.get("source_module", "")).strip():
        fail("a1_8_transition_prerequisite_source_missing", "Pré-requisito sem source_module")

technical_dependencies = (
    transition_a1_to_a2.get("technical_dependencies")
    if isinstance(transition_a1_to_a2.get("technical_dependencies"), list)
    else []
)
if not technical_dependencies:
    fail("a1_8_transition_dependencies_missing", "transition_a1_to_a2 sem technical_dependencies")

for row in technical_dependencies:
    if not isinstance(row, dict):
        fail("a1_8_transition_dependency_invalid", "Entrada inválida em technical_dependencies")
    if not str(row.get("id", "")).strip():
        fail("a1_8_transition_dependency_invalid", "Dependência técnica sem id")
    if not str(row.get("target_epic", "")).strip() or not str(row.get("target_module", "")).strip():
        fail("a1_8_transition_dependency_target_missing", "Dependência técnica sem target_epic/target_module")

residual_risks = (
    transition_a1_to_a2.get("residual_risks")
    if isinstance(transition_a1_to_a2.get("residual_risks"), list)
    else []
)
if not residual_risks:
    fail("a1_8_transition_residual_risks_missing", "transition_a1_to_a2 sem residual_risks")

for risk in residual_risks:
    if not isinstance(risk, dict):
        fail("a1_8_transition_residual_risk_invalid", "Entrada inválida em residual_risks")
    if not str(risk.get("id", "")).strip():
        fail("a1_8_transition_residual_risk_invalid", "Risco residual sem id")
    if not str(risk.get("owner", "")).strip():
        fail("a1_8_transition_residual_risk_owner_missing", "Risco residual sem owner")
    if not str(risk.get("mitigation", "")).strip():
        fail("a1_8_transition_residual_risk_mitigation_missing", "Risco residual sem mitigation")
    if not str(risk.get("target_epic", "")).strip() or not str(risk.get("target_module", "")).strip():
        fail("a1_8_transition_residual_risk_target_missing", "Risco residual sem target_epic/target_module")

handoff_boundary = (
    transition_a1_to_a2.get("handoff_boundary")
    if isinstance(transition_a1_to_a2.get("handoff_boundary"), dict)
    else {}
)
if handoff_boundary.get("scope_reopen_forbidden") is not True:
    fail("a1_8_transition_scope_reopen_forbidden_missing", "handoff_boundary deve bloquear reabertura de escopo")
reopened_scope_items = (
    handoff_boundary.get("reopened_scope_items")
    if isinstance(handoff_boundary.get("reopened_scope_items"), list)
    else None
)
if reopened_scope_items is None:
    fail("a1_8_transition_reopened_scope_invalid", "handoff_boundary.reopened_scope_items inválido")
if reopened_scope_items:
    fail("a1_8_transition_scope_reopened", "handoff de A1->A2 não pode reabrir escopo de A1.8")

cross_document_conformity = (
    a18.get("cross_document_conformity") if isinstance(a18.get("cross_document_conformity"), dict) else {}
)
if not cross_document_conformity:
    fail("a1_8_cross_document_conformity_missing", "A1.8 sem bloco cross_document_conformity")

if str(cross_document_conformity.get("status", "")).strip() != "conform":
    fail("a1_8_cross_document_status_invalid", "cross_document_conformity.status deve ser conform")

cross_blocking_policy = (
    cross_document_conformity.get("blocking_policy")
    if isinstance(cross_document_conformity.get("blocking_policy"), dict)
    else {}
)
if cross_blocking_policy.get("semantic_divergence_blocks_acceptance") is not True:
    fail(
        "a1_8_cross_document_blocking_policy_missing",
        "cross_document_conformity deve declarar bloqueio semântico como regra obrigatória",
    )

status_alignment = (
    cross_document_conformity.get("status_alignment")
    if isinstance(cross_document_conformity.get("status_alignment"), list)
    else []
)
if len(status_alignment) < 3:
    fail("a1_8_cross_document_status_alignment_missing", "cross_document_conformity.status_alignment incompleto")

required_status_docs = {
    "docs/entregas/README.md",
    "docs/entregas/entrega-1.md",
    "docs/entregas/roadmap-epicos.md",
}
declared_status_docs = set()
for row in status_alignment:
    if not isinstance(row, dict):
        fail("a1_8_cross_document_status_alignment_invalid", "Entrada inválida em status_alignment")
    doc_ref = str(row.get("doc_ref", "")).strip()
    expected_status = str(row.get("expected_status", "")).strip().lower()
    if not doc_ref:
        fail("a1_8_cross_document_status_alignment_invalid", "status_alignment sem doc_ref")
    if expected_status != "encerrado":
        fail("a1_8_cross_document_status_mismatch", f"status esperado inválido para {doc_ref}: {expected_status}")
    declared_status_docs.add(doc_ref)

missing_status_docs = sorted(required_status_docs - declared_status_docs)
if missing_status_docs:
    fail(
        "a1_8_cross_document_status_alignment_missing",
        "Docs obrigatórios ausentes em status_alignment: " + ", ".join(missing_status_docs),
    )

canonical_terminology = (
    cross_document_conformity.get("canonical_terminology")
    if isinstance(cross_document_conformity.get("canonical_terminology"), dict)
    else {}
)
if str(canonical_terminology.get("provider_scope_term", "")).strip() != "external-linux":
    fail("a1_8_cross_document_term_mismatch", "provider_scope_term deve ser external-linux")
if str(canonical_terminology.get("pipeline_flow_term", "")).strip() != "prepare -> provision -> configure -> verify":
    fail(
        "a1_8_cross_document_term_mismatch",
        "pipeline_flow_term deve ser exatamente prepare -> provision -> configure -> verify",
    )

e1_operational_flow = (
    cross_document_conformity.get("e1_operational_flow")
    if isinstance(cross_document_conformity.get("e1_operational_flow"), dict)
    else {}
)
if e1_operational_flow.get("unique_description_enforced") is not True:
    fail("a1_8_cross_document_flow_not_unique", "e1_operational_flow deve marcar unique_description_enforced=true")
if str(e1_operational_flow.get("source_of_truth", "")).strip() != "docs/entregas/README.md#11":
    fail("a1_8_cross_document_flow_source_invalid", "source_of_truth do fluxo E1 deve apontar para docs/entregas/README.md#11")
canonical_steps = e1_operational_flow.get("canonical_steps") if isinstance(e1_operational_flow.get("canonical_steps"), list) else []
required_steps = {
    "infra_ssh_preflight",
    "organization_and_network_registration",
    "nodes_and_execution_mapping",
    "business_group_channels_and_install",
    "api_publication_by_channel_chaincode",
    "incremental_expansion_post_creation",
}
if set(canonical_steps) != required_steps:
    fail("a1_8_cross_document_flow_steps_invalid", "canonical_steps do fluxo E1 está incompleto ou divergente")

containerization_by_epic = (
    cross_document_conformity.get("containerization_by_epic")
    if isinstance(cross_document_conformity.get("containerization_by_epic"), dict)
    else {}
)
if containerization_by_epic.get("explicit_by_epic") is not True:
    fail("a1_8_cross_document_containerization_not_explicit", "containerization_by_epic.explicit_by_epic deve ser true")
epics = containerization_by_epic.get("epics") if isinstance(containerization_by_epic.get("epics"), list) else []
expected_epics = ["A1", "A2", "B1", "B2", "C1", "C2", "D1", "E1"]
if epics != expected_epics:
    fail("a1_8_cross_document_containerization_epics_invalid", "Lista de épicos da containerização está divergente")

architecture_coherence = (
    cross_document_conformity.get("architecture_coherence")
    if isinstance(cross_document_conformity.get("architecture_coherence"), dict)
    else {}
)
high_level_refs = architecture_coherence.get("high_level_refs") if isinstance(architecture_coherence.get("high_level_refs"), list) else []
low_level_refs = architecture_coherence.get("low_level_refs") if isinstance(architecture_coherence.get("low_level_refs"), list) else []
required_high_level_refs = {
    "docs/orchestrator-architecture.puml",
    "docs/orchestrator-conceptual.puml",
    "docs/orchestrator-overview-macro.puml",
}
required_low_level_refs = {"docs/primary-work-article/low-level-architecture.puml"}
if set(high_level_refs) != required_high_level_refs:
    fail("a1_8_cross_document_arch_refs_invalid", "high_level_refs divergente do contrato obrigatório")
if set(low_level_refs) != required_low_level_refs:
    fail("a1_8_cross_document_arch_refs_invalid", "low_level_refs divergente do contrato obrigatório")

scope_boundaries = (
    cross_document_conformity.get("scope_boundaries")
    if isinstance(cross_document_conformity.get("scope_boundaries"), dict)
    else {}
)
included_boundaries = scope_boundaries.get("included") if isinstance(scope_boundaries.get("included"), list) else []
excluded_boundaries = scope_boundaries.get("excluded") if isinstance(scope_boundaries.get("excluded"), list) else []
if len(included_boundaries) < 4:
    fail("a1_8_cross_document_scope_boundary_included_missing", "scope_boundaries.included incompleto")
if set(excluded_boundaries) != required_out_of_scope:
    fail("a1_8_cross_document_scope_boundary_excluded_mismatch", "scope_boundaries.excluded divergente do fora de escopo oficial")

container_operational_matrix = (
    a18.get("container_operational_matrix") if isinstance(a18.get("container_operational_matrix"), dict) else {}
)
if not container_operational_matrix:
    fail("a1_8_container_matrix_missing", "A1.8 sem bloco container_operational_matrix")

if str(container_operational_matrix.get("status", "")).strip() != "executable":
    fail("a1_8_container_matrix_status_invalid", "container_operational_matrix.status deve ser executable")
if str(container_operational_matrix.get("source_of_truth", "")).strip() != "docs/entregas/roadmap-epicos.md":
    fail("a1_8_container_matrix_source_invalid", "container_operational_matrix.source_of_truth inválido")

container_blocking_policy = (
    container_operational_matrix.get("blocking_policy")
    if isinstance(container_operational_matrix.get("blocking_policy"), dict)
    else {}
)
if container_blocking_policy.get("implicit_strategic_containers_forbidden") is not True:
    fail(
        "a1_8_container_matrix_implicit_not_blocked",
        "Matriz de containers deve bloquear containers estratégicos implícitos",
    )
if container_blocking_policy.get("ambiguous_responsibility_overlap_forbidden") is not True:
    fail(
        "a1_8_container_matrix_overlap_not_blocked",
        "Matriz de containers deve bloquear sobreposição ambígua de responsabilidade",
    )
if container_blocking_policy.get("progressive_vm_convergence_required") is not True:
    fail(
        "a1_8_container_matrix_convergence_not_required",
        "Matriz de containers deve exigir convergência progressiva para external-linux",
    )

epic_distribution = (
    container_operational_matrix.get("epic_distribution")
    if isinstance(container_operational_matrix.get("epic_distribution"), list)
    else []
)
expected_epic_order = ["A2", "B1", "B2", "C1", "C2", "D1", "E1"]
if [str(row.get("epic", "")).strip() if isinstance(row, dict) else "" for row in epic_distribution] != expected_epic_order:
    fail("a1_8_container_matrix_epic_order_invalid", "epic_distribution deve seguir A2,B1,B2,C1,C2,D1,E1")

expected_matrix = {
    "A2": {
        "orchestrator": [
            "opssc-api-engine",
            "opssc-workflow-engine",
            "opssc-event-bus",
            "opssc-control-db",
            "opssc-state-store",
        ],
        "external_linux": ["nenhum"],
    },
    "B1": {
        "orchestrator": [
            "ccapi-gateway",
            "lifecycle-operator",
            "lifecycle-policy-engine",
            "lifecycle-webhook-dispatcher",
        ],
        "external_linux": ["chaincode-runtime-managed-by-peers", "channel-local-aux-services-when-applicable"],
    },
    "B2": {
        "orchestrator": [
            "iam-provider",
            "policy-decision-point",
            "offchain-authz-broker",
            "access-audit-service",
        ],
        "external_linux": ["nenhum"],
    },
    "C1": {
        "orchestrator": [
            "metrics-collector",
            "logs-aggregator",
            "traces-collector",
            "alertmanager-slo",
            "observability-dashboard",
        ],
        "external_linux": ["node-observer-agent-when-adopted"],
    },
    "C2": {
        "orchestrator": [
            "ops-console-backend",
            "ops-topology-service",
            "ops-timeline-service",
            "ops-runbook-catalog-service",
        ],
        "external_linux": ["nenhum"],
    },
    "D1": {
        "orchestrator": [
            "marketplace-catalog-api",
            "marketplace-sync-worker",
            "marketplace-recommendation-service",
            "marketplace-governance-service",
        ],
        "external_linux": ["nenhum"],
    },
    "E1": {
        "orchestrator": ["e2e-test-runner", "benchmark-runner", "chaos-runner", "readiness-gate-service"],
        "external_linux": ["chaos-target-agent-when-applicable"],
    },
}

all_orchestrator_containers = []
for row in epic_distribution:
    if not isinstance(row, dict):
        fail("a1_8_container_matrix_distribution_invalid", "Entrada inválida em epic_distribution")
    epic = str(row.get("epic", "")).strip()
    if epic not in expected_matrix:
        fail("a1_8_container_matrix_unknown_epic", f"Épico inesperado na matriz de containers: {epic}")

    orchestrator_containers = row.get("orchestrator_containers") if isinstance(row.get("orchestrator_containers"), list) else []
    external_linux_containers = (
        row.get("external_linux_host_containers") if isinstance(row.get("external_linux_host_containers"), list) else []
    )

    if orchestrator_containers != expected_matrix[epic]["orchestrator"]:
        fail("a1_8_container_matrix_orchestrator_divergence", f"Containers do orquestrador divergentes no épico {epic}")
    if external_linux_containers != expected_matrix[epic]["external_linux"]:
        fail("a1_8_container_matrix_external_linux_divergence", f"Containers de host external-linux divergentes no épico {epic}")

    if not str(row.get("wave", "")).strip():
        fail("a1_8_container_matrix_wave_missing", f"Épico {epic} sem wave")

    operational_purpose = row.get("operational_purpose") if isinstance(row.get("operational_purpose"), dict) else {}
    if not str(operational_purpose.get("orchestrator", "")).strip():
        fail("a1_8_container_matrix_purpose_missing", f"Épico {epic} sem finalidade operacional do orquestrador")
    if not str(operational_purpose.get("external_linux_host", "")).strip():
        fail("a1_8_container_matrix_purpose_missing", f"Épico {epic} sem finalidade operacional do host external-linux")

    traceability = row.get("traceability") if isinstance(row.get("traceability"), dict) else {}
    requirements_trace = traceability.get("requirements") if isinstance(traceability.get("requirements"), list) else []
    architecture_layers = (
        traceability.get("architecture_layers") if isinstance(traceability.get("architecture_layers"), list) else []
    )
    if not requirements_trace:
        fail("a1_8_container_matrix_traceability_missing", f"Épico {epic} sem rastreabilidade de requisitos")
    if not architecture_layers:
        fail("a1_8_container_matrix_traceability_missing", f"Épico {epic} sem rastreabilidade arquitetural")

    all_orchestrator_containers.extend(orchestrator_containers)

if len(all_orchestrator_containers) != len(set(all_orchestrator_containers)):
    fail(
        "a1_8_container_matrix_ambiguous_overlap",
        "Container estratégico duplicado entre épicos (sobreposição ambígua de responsabilidade)",
    )

progressive_vm_convergence = (
    container_operational_matrix.get("progressive_vm_convergence")
    if isinstance(container_operational_matrix.get("progressive_vm_convergence"), dict)
    else {}
)
if str(progressive_vm_convergence.get("reference_environment", "")).strip() != "external-linux":
    fail("a1_8_container_matrix_reference_env_invalid", "Convergência progressiva deve usar reference_environment=external-linux")

wave_checkpoints = (
    progressive_vm_convergence.get("wave_checkpoints")
    if isinstance(progressive_vm_convergence.get("wave_checkpoints"), list)
    else []
)
expected_checkpoint_levels = {
    "A2": 0,
    "B1": 1,
    "B2": 1,
    "C1": 2,
    "C2": 2,
    "D1": 2,
    "E1": 4,
}
if [str(cp.get("epic", "")).strip() if isinstance(cp, dict) else "" for cp in wave_checkpoints] != expected_epic_order:
    fail("a1_8_container_matrix_checkpoint_order_invalid", "wave_checkpoints deve seguir ordem A2,B1,B2,C1,C2,D1,E1")

last_level = -1
for checkpoint in wave_checkpoints:
    if not isinstance(checkpoint, dict):
        fail("a1_8_container_matrix_checkpoint_invalid", "Entrada inválida em wave_checkpoints")
    epic = str(checkpoint.get("epic", "")).strip()
    level = checkpoint.get("convergence_level")
    if not isinstance(level, int):
        fail("a1_8_container_matrix_checkpoint_level_invalid", f"convergence_level inválido para {epic}")
    if expected_checkpoint_levels.get(epic) != level:
        fail("a1_8_container_matrix_checkpoint_level_invalid", f"convergence_level inesperado para {epic}")
    if level < last_level:
        fail("a1_8_container_matrix_checkpoint_non_progressive", "convergence_level não pode regredir entre ondas")
    if not str(checkpoint.get("verification_focus", "")).strip():
        fail("a1_8_container_matrix_checkpoint_focus_missing", f"wave checkpoint sem verification_focus no épico {epic}")
    last_level = level

final_expected_components = (
    progressive_vm_convergence.get("final_expected_components")
    if isinstance(progressive_vm_convergence.get("final_expected_components"), list)
    else []
)
required_final_components = {
    "ca",
    "orderer",
    "peer",
    "couchdb",
    "ccapi",
    "networkapi",
    "webclient-or-forwarder",
    "prometheus",
    "grafana",
    "exporters-or-agents",
}
if set(final_expected_components) != required_final_components:
    fail("a1_8_container_matrix_final_components_invalid", "final_expected_components divergente do estado final obrigatório")

verification_artifacts = (
    progressive_vm_convergence.get("verification_artifacts")
    if isinstance(progressive_vm_convergence.get("verification_artifacts"), list)
    else []
)
if verification_artifacts != ["inventory-final.json", "stage-reports/*", "verify-report.json"]:
    fail("a1_8_container_matrix_verification_artifacts_invalid", "verification_artifacts divergente do contrato obrigatório")

a1_operational_allocation = (
    a18.get("a1_operational_allocation") if isinstance(a18.get("a1_operational_allocation"), dict) else {}
)
if not a1_operational_allocation:
    fail("a1_8_a1_operational_allocation_missing", "A1.8 sem bloco a1_operational_allocation")

if str(a1_operational_allocation.get("status", "")).strip() != "enforced":
    fail("a1_8_a1_operational_allocation_status_invalid", "a1_operational_allocation.status deve ser enforced")

a1_allocation_convention = (
    a1_operational_allocation.get("convention")
    if isinstance(a1_operational_allocation.get("convention"), dict)
    else {}
)
if a1_allocation_convention.get("explicit_none_required_when_no_container_in_domain") is not True:
    fail(
        "a1_8_a1_operational_allocation_convention_invalid",
        "Convenção do A1 deve exigir declaração explícita de nenhum quando não houver container no domínio",
    )
if a1_allocation_convention.get("development_co_location_allowed_with_logical_separation") is not True:
    fail(
        "a1_8_a1_operational_allocation_convention_invalid",
        "Convenção do A1 deve explicitar co-localização apenas lógica em desenvolvimento",
    )

a1_mandatory_containers = (
    a1_operational_allocation.get("mandatory_a1_containers")
    if isinstance(a1_operational_allocation.get("mandatory_a1_containers"), dict)
    else {}
)
a1_orchestrator_containers = (
    a1_mandatory_containers.get("orchestrator_system_services")
    if isinstance(a1_mandatory_containers.get("orchestrator_system_services"), list)
    else []
)
expected_a1_orchestrator_containers = [
    "runbook-api-engine",
    "provisioning-ssh-executor",
    "pipeline-evidence-store",
]
if a1_orchestrator_containers != expected_a1_orchestrator_containers:
    fail(
        "a1_8_a1_operational_allocation_orchestrator_invalid",
        "Containers obrigatórios do domínio orquestrador no A1 estão divergentes",
    )

a1_external_linux_requirements = (
    a1_mandatory_containers.get("external_linux_host_requirements")
    if isinstance(a1_mandatory_containers.get("external_linux_host_requirements"), list)
    else []
)
expected_a1_external_linux_requirements = [
    "runtime-base-per-active-inventoried-host",
    "fabric-ca-minimum-one-per-org-with-ca-role",
    "fabric-orderer-minimum-one-per-org-with-orderer-role",
    "fabric-peer-minimum-one-per-channel-member-org",
    "couchdb-one-per-peer-when-external-state-db-required",
]
if a1_external_linux_requirements != expected_a1_external_linux_requirements:
    fail(
        "a1_8_a1_operational_allocation_external_linux_invalid",
        "Requisitos obrigatórios do domínio external-linux no A1 estão divergentes",
    )

a1_frontend_evolution = (
    a1_operational_allocation.get("frontend_evolution")
    if isinstance(a1_operational_allocation.get("frontend_evolution"), dict)
    else {}
)
a1_existing_screens = (
    a1_frontend_evolution.get("existing_incremented_screens")
    if isinstance(a1_frontend_evolution.get("existing_incremented_screens"), list)
    else []
)
expected_a1_existing_screens = [
    "ProvisioningInfrastructurePage",
    "ProvisioningBlueprintPage",
    "ProvisioningRunbookPage",
    "ProvisioningInventoryPage",
]
if a1_existing_screens != expected_a1_existing_screens:
    fail("a1_8_a1_operational_allocation_frontend_invalid", "Lista de telas incrementadas do A1 está divergente")

a1_new_modules = (
    a1_frontend_evolution.get("new_screens_or_modules")
    if isinstance(a1_frontend_evolution.get("new_screens_or_modules"), list)
    else []
)
expected_a1_new_modules = [
    "ProvisioningTechnicalHubPage",
    "ProvisioningReadinessCard",
    "ScreenReadinessBanner",
]
if a1_new_modules != expected_a1_new_modules:
    fail("a1_8_a1_operational_allocation_frontend_invalid", "Lista de módulos novos do A1 está divergente")

a1_out_of_scope_topic = (
    a1_operational_allocation.get("out_of_scope_for_a1_8_topic")
    if isinstance(a1_operational_allocation.get("out_of_scope_for_a1_8_topic"), list)
    else []
)
expected_a1_out_of_scope_topic = [
    "gateway-ccapi",
    "lifecycle-governado-chaincode",
    "console-operacional-avancado",
    "iam-end-to-end",
    "observabilidade-multi-escala",
]
if a1_out_of_scope_topic != expected_a1_out_of_scope_topic:
    fail("a1_8_a1_operational_allocation_out_of_scope_invalid", "Fora de escopo obrigatório do A1 está divergente")

a1_minimum_evidence = (
    a1_operational_allocation.get("minimum_container_acceptance_evidence")
    if isinstance(a1_operational_allocation.get("minimum_container_acceptance_evidence"), list)
    else []
)
expected_a1_minimum_evidence = [
    "inventory-final-with-name-role-host-ports-status",
    "stage-reports-with-change-id-and-run-id-correlation",
    "verify-report-with-health-checks-per-critical-component",
]
if a1_minimum_evidence != expected_a1_minimum_evidence:
    fail("a1_8_a1_operational_allocation_evidence_invalid", "Evidência mínima obrigatória do A1 está divergente")

final_acceptance_criteria = (
    a18.get("final_acceptance_criteria") if isinstance(a18.get("final_acceptance_criteria"), dict) else {}
)
if not final_acceptance_criteria:
    fail("a1_8_final_acceptance_criteria_missing", "A1.8 sem bloco final_acceptance_criteria")

if str(final_acceptance_criteria.get("status", "")).strip() != "accepted":
    fail("a1_8_final_acceptance_status_invalid", "final_acceptance_criteria.status deve ser accepted")

minimum_coverage = (
    final_acceptance_criteria.get("minimum_coverage")
    if isinstance(final_acceptance_criteria.get("minimum_coverage"), dict)
    else {}
)
if minimum_coverage.get("functional_and_contractual_validation_official_flow_a1") is not True:
    fail("a1_8_final_acceptance_coverage_missing", "Cobertura funcional/contratual do fluxo oficial A1 é obrigatória")
if minimum_coverage.get("minimum_evidence_and_auditable_decision_trace_validation") is not True:
    fail("a1_8_final_acceptance_coverage_missing", "Cobertura de evidência mínima e trilha auditável é obrigatória")
if minimum_coverage.get("documentation_and_container_distribution_validation") is not True:
    fail("a1_8_final_acceptance_coverage_missing", "Cobertura de documentação e distribuição de containers é obrigatória")

mandatory_rules = (
    final_acceptance_criteria.get("mandatory_rules")
    if isinstance(final_acceptance_criteria.get("mandatory_rules"), dict)
    else {}
)
if mandatory_rules.get("contract_regression_breaks_ci") is not True:
    fail("a1_8_final_acceptance_regression_rule_missing", "Regressão contratual deve quebrar CI no aceite final")
if mandatory_rules.get("no_local_or_degraded_success_path") is not True:
    fail("a1_8_final_acceptance_local_success_forbidden", "Aceite final não pode permitir sucesso local/degradado")
if mandatory_rules.get("acceptance_requires_items_1_to_6_completed") is not True:
    fail("a1_8_final_acceptance_items_rule_missing", "Aceite final deve exigir conclusão dos itens 1 a 6")

required_items = (
    final_acceptance_criteria.get("required_items_1_to_6")
    if isinstance(final_acceptance_criteria.get("required_items_1_to_6"), dict)
    else {}
)
required_item_keys = {
    "item_1_scope_and_baseline_consolidation",
    "item_2_blocking_ci_gate",
    "item_3_closure_evidence_package",
    "item_4_a1_to_a2_transition_readiness",
    "item_5_cross_document_conformity",
    "item_6_executable_container_matrix",
}
if set(required_items.keys()) != required_item_keys:
    fail("a1_8_final_acceptance_items_contract_invalid", "Contrato required_items_1_to_6 divergente")
if not all(required_items.get(key) is True for key in sorted(required_item_keys)):
    fail("a1_8_final_acceptance_items_incomplete", "Aceite final inválido: itens 1 a 6 não estão todos concluídos")

required_check_ids = (
    final_acceptance_criteria.get("required_check_ids")
    if isinstance(final_acceptance_criteria.get("required_check_ids"), list)
    else []
)
expected_required_check_ids = [
    "wp_closure_a1_1_a1_7",
    "official_flow_no_local_fallback",
    "deterministic_contract_freeze",
    "traceability_ids_present",
    "delivery_1_closure_dossier_complete",
    "transition_checklist_ready",
    "cross_document_conformity",
    "container_operational_matrix_executable",
    "a1_operational_allocation_non_isolated",
]
if required_check_ids != expected_required_check_ids:
    fail("a1_8_final_acceptance_required_checks_invalid", "required_check_ids divergente do contrato obrigatório")

check_pass_map = {}
for check in checks:
    if isinstance(check, dict):
        check_id = str(check.get("id", "")).strip()
        if check_id:
            check_pass_map[check_id] = bool(check.get("passed") is True)

missing_required_checks = [check_id for check_id in expected_required_check_ids if check_id not in check_pass_map]
if missing_required_checks:
    fail(
        "a1_8_final_acceptance_required_checks_missing",
        "Checks obrigatórios ausentes no pacote A1.8: " + ", ".join(missing_required_checks),
    )

failed_required_checks = [check_id for check_id in expected_required_check_ids if check_pass_map.get(check_id) is not True]
if failed_required_checks:
    fail(
        "a1_8_final_acceptance_required_checks_failed",
        "Checks obrigatórios reprovados para aceite final: " + ", ".join(failed_required_checks),
    )

if str(final_acceptance_criteria.get("expected_result", "")).strip() != "WP A1.8 encerrado com evidência técnica definitiva.":
    fail("a1_8_final_acceptance_expected_result_invalid", "expected_result do aceite final divergente")


def load_text(path: Path) -> str:
    if not path.exists():
        fail("a1_8_cross_document_file_missing", f"Documento obrigatório não encontrado: {path}")
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:
        fail("a1_8_cross_document_file_read_error", f"Falha ao ler {path}: {exc}")
    return ""


readme_path = repo_root / "docs" / "entregas" / "README.md"
entrega1_path = repo_root / "docs" / "entregas" / "entrega-1.md"
roadmap_path = repo_root / "docs" / "entregas" / "roadmap-epicos.md"

readme_text = load_text(readme_path)
entrega1_text = load_text(entrega1_path)
roadmap_text = load_text(roadmap_path)

if "Fluxo operacional alvo da Entrega 1" not in readme_text:
    fail("a1_8_cross_document_readme_flow_missing", "README sem seção canônica do fluxo operacional de E1")
if "Status da Entrega 1: **encerrada**" not in readme_text:
    fail("a1_8_cross_document_readme_status_mismatch", "README sem status encerrado da Entrega 1")
if "prepare -> provision -> configure -> verify" not in readme_text:
    fail("a1_8_cross_document_readme_flow_term_mismatch", "README sem semântica única do fluxo operacional E1")
if "external-linux" not in readme_text:
    fail("a1_8_cross_document_readme_scope_mismatch", "README sem termo external-linux")

if "Situação: encerrada" not in entrega1_text:
    fail("a1_8_cross_document_entrega1_status_mismatch", "entrega-1.md sem status encerrada")
if "Fora de escopo" not in entrega1_text:
    fail("a1_8_cross_document_entrega1_scope_boundary_missing", "entrega-1.md sem fronteira explícita de escopo")
if "external-linux" not in entrega1_text:
    fail("a1_8_cross_document_entrega1_scope_mismatch", "entrega-1.md sem semântica external-linux")

if "A1.8 concluído (definitivo)" not in roadmap_text:
    fail("a1_8_cross_document_roadmap_status_mismatch", "roadmap sem fechamento definitivo de A1.8")
if "Conformidade documental cruzada" not in roadmap_text:
    fail("a1_8_cross_document_roadmap_conformity_missing", "roadmap sem item de conformidade documental cruzada")

required_container_name_markers = [
    "opssc-api-engine",
    "opssc-workflow-engine",
    "ccapi-gateway",
    "lifecycle-operator",
    "iam-provider",
    "policy-decision-point",
    "metrics-collector",
    "logs-aggregator",
    "ops-console-backend",
    "ops-topology-service",
    "marketplace-catalog-api",
    "marketplace-sync-worker",
    "e2e-test-runner",
    "benchmark-runner",
    "chaos-runner",
    "readiness-gate-service",
    "chaos-target-agent",
]
for marker in required_container_name_markers:
    if marker not in roadmap_text:
        fail("a1_8_container_matrix_roadmap_marker_missing", f"roadmap sem container obrigatório da matriz executável: {marker}")

required_container_markers = [
    "Containers obrigatórios no Épico A1:",
    "Containers previstos neste épico (A2):",
    "Containers previstos neste épico (B1):",
    "Containers previstos neste épico (B2):",
    "Containers previstos neste épico (C1):",
    "Containers previstos neste épico (C2):",
    "Containers previstos neste épico (D1):",
    "Containers previstos neste épico (E1):",
]
for marker in required_container_markers:
    if marker not in roadmap_text:
        fail("a1_8_cross_document_containerization_marker_missing", f"roadmap sem marcador de containers obrigatório: {marker}")

for arch_ref in sorted(required_high_level_refs | required_low_level_refs):
    arch_path = repo_root / arch_ref
    arch_text = load_text(arch_path)
    if "Orquestrador" not in arch_text or "Fabric" not in arch_text:
        fail("a1_8_cross_document_arch_semantic_mismatch", f"Semântica arquitetural incompleta em {arch_ref}")

low_level_arch_text = load_text(repo_root / "docs" / "primary-work-article" / "low-level-architecture.puml")
if "Camada 2 — Plano de Controle (OPSSC)" not in low_level_arch_text:
    fail("a1_8_cross_document_low_level_opssc_missing", "Arquitetura de baixo nível sem referência explícita ao OPSSC")
if "Camada 3 — Orquestração e APIs" not in low_level_arch_text:
    fail("a1_8_cross_document_low_level_orchestration_missing", "Arquitetura de baixo nível sem camada de orquestração")

for required in ("run_id", "change_id"):
    if str(crypto_inventory.get(required, "")).strip() != str(pipeline_report.get(required, "")).strip():
        fail("a1_8_crypto_inventory_correlation_mismatch", f"crypto-inventory divergente do pipeline-report em {required}")

crypto_fingerprint = str(
    crypto_inventory.get("fingerprint_sha256")
    or crypto_inventory.get("blueprint_fingerprint")
    or crypto_inventory.get("inventory_fingerprint")
    or ""
).strip()
if crypto_fingerprint != str(pipeline_report.get("fingerprint_sha256", "")).strip():
    fail(
        "a1_8_crypto_inventory_correlation_mismatch",
        "crypto-inventory divergente do pipeline-report em fingerprint_sha256/blueprint_fingerprint",
    )

summary_file = pipeline_dir / "a1-epic-closure-summary.json"
closure_dossier_file = pipeline_dir / "a1-delivery-1-closure-dossier.json"
transition_handoff_file = pipeline_dir / "a1-a2-transition-handoff.json"
cross_document_conformity_file = pipeline_dir / "a1-cross-document-conformity.json"
container_operational_matrix_file = pipeline_dir / "a1-container-operational-matrix.json"
final_acceptance_criteria_file = pipeline_dir / "a1-final-acceptance-criteria.json"
summary_payload = {
    "gate": "wp-a1.8-final-acceptance",
    "gate_version": "1.0.0",
    "decision": "allow",
    "validated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "run_id": run_id,
    "change_id": change_id,
    "fingerprint_sha256": fingerprint,
    "evidence_presence": {
        "pipeline_report": True,
        "stage_reports": True,
        "inventory_final": True,
        "history": True,
        "decision_trace": True,
    },
    "wp_acceptance": {
        "A1.5": True,
        "A1.6": True,
        "A1.7": True,
        "A1.8": True,
    },
    "consistency": {
        "pipeline_verify_status": verify_stage_status,
        "verify_verdict": verify_verdict,
        "history_decision": str(last_history.get("decision", "")).strip(),
        "decision_trace_decision": str(last_trace.get("decision", "")).strip(),
        "a1_to_a2_scope_reopen_forbidden": handoff_boundary.get("scope_reopen_forbidden") is True,
        "cross_document_conformity": cross_document_conformity.get("status") == "conform",
        "container_matrix_executable": container_operational_matrix.get("status") == "executable",
        "final_acceptance_status": final_acceptance_criteria.get("status") == "accepted",
    },
    "validated_files": {
        "pipeline_report": str(pipeline_report_path),
        "stage_reports_dir": str(stage_reports_dir),
        "inventory_final": str(pipeline_dir / "inventory-final.json"),
        "history": str(history_path),
        "decision_trace": str(decision_trace_path),
        "wp_a1_5": ${WP_A1_5_FILE@Q},
        "wp_a1_6": ${WP_A1_6_FILE@Q},
        "wp_a1_7": ${WP_A1_7_FILE@Q},
        "wp_a1_8": ${WP_A1_8_FILE@Q},
    },
}
summary_file.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

closure_dossier_payload = {
    "artifact": {
        "id": "a1-delivery-1-closure-dossier",
        "version": "1.0.0",
        "immutable": True,
        "generated_at_utc": artifact_generated_at,
        "generated_by_gate": "scripts/validate_wp_a1_8_ci.sh",
    },
    "correlation": {
        "run_id": run_id,
        "change_id": change_id,
        "fingerprint_sha256": fingerprint,
    },
    "official_execution_references": {
        "pipeline_report": str(pipeline_report_path),
        "stage_reports_dir": str(stage_reports_dir),
        "inventory_final": str(pipeline_dir / "inventory-final.json"),
        "crypto_inventory": str(pipeline_dir / "configure" / "crypto-inventory.json"),
        "history": str(history_path),
        "decision_trace": str(decision_trace_path),
        "gate_a1_3": "scripts/validate_pipeline_a1_3_ci.sh",
        "gate_a1_8": "scripts/validate_wp_a1_8_ci.sh",
    },
    "test_trail": {
        "backend": backend_tests,
        "frontend": frontend_tests,
    },
    "inventory_snapshot": {
        "operational": {
            "source": "inventory-final.json",
            "run_id": str(inventory_final.get("run_id", "")).strip(),
            "change_id": str(inventory_final.get("change_id", "")).strip(),
            "fingerprint_sha256": str(inventory_final.get("fingerprint_sha256", "")).strip(),
            "entity_counters": inventory_final.get("entity_counters", {}),
        },
        "cryptographic": {
            "source": "configure/crypto-inventory.json",
            "contract_version": str(crypto_inventory.get("contract_version", "")).strip(),
            "run_id": str(crypto_inventory.get("run_id", "")).strip(),
            "change_id": str(crypto_inventory.get("change_id", "")).strip(),
            "fingerprint_sha256": crypto_fingerprint,
            "entity_counters": crypto_inventory.get("entity_counters", {}),
        },
    },
    "immutability": {
        "post_closure_locked": True,
        "policy": "content_hash_locked",
    },
    "technical_scope_signature": {
        "signed": True,
        "signed_by": str(signature.get("signed_by", "")).strip(),
        "signed_at_utc": signature_signed_at,
        "scope_conformity": True,
        "statement": str(signature.get("statement", "")).strip(),
    },
    "handoff_checklist": {
        "ready_for_audit": True,
        "ready_for_handoff": True,
        "scope_reopen_forbidden": True,
    },
}

payload_for_hash = json.dumps(closure_dossier_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
content_sha256 = hashlib.sha256(payload_for_hash).hexdigest()
closure_dossier_payload["artifact"]["content_sha256"] = content_sha256

closure_dossier_text = json.dumps(closure_dossier_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
if closure_dossier_file.exists():
    existing_payload = load_json(str(closure_dossier_file))
    existing_hash = str(existing_payload.get("artifact", {}).get("content_sha256", "")).strip()
    if existing_hash != content_sha256:
        fail(
            "a1_8_immutable_dossier_changed",
            "Artefato imutável de encerramento já existe com hash divergente; alteração pós-fechamento não permitida",
        )
else:
    closure_dossier_file.write_text(closure_dossier_text, encoding="utf-8")

transition_handoff_payload = {
    "artifact": {
        "id": "a1-a2-transition-handoff",
        "version": "1.0.0",
        "immutable": True,
        "generated_at_utc": transition_generated_at or artifact_generated_at,
        "generated_by_gate": "scripts/validate_wp_a1_8_ci.sh",
    },
    "handoff": {
        "source_epic": "A1",
        "target_epic": "A2",
        "scope_reopen_forbidden": True,
    },
    "correlation": {
        "run_id": run_id,
        "change_id": change_id,
        "fingerprint_sha256": fingerprint,
    },
    "consumed_prerequisites": consumed_prerequisites,
    "technical_dependencies": technical_dependencies,
    "residual_risks": residual_risks,
    "handoff_boundary": handoff_boundary,
}

transition_hash_input = json.dumps(
    transition_handoff_payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
transition_content_sha256 = hashlib.sha256(transition_hash_input).hexdigest()
transition_handoff_payload["artifact"]["content_sha256"] = transition_content_sha256

transition_text = json.dumps(transition_handoff_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
if transition_handoff_file.exists():
    existing_transition = load_json(str(transition_handoff_file))
    existing_transition_hash = str(existing_transition.get("artifact", {}).get("content_sha256", "")).strip()
    if existing_transition_hash != transition_content_sha256:
        fail(
            "a1_8_immutable_transition_handoff_changed",
            "Artefato imutável de handoff A1->A2 já existe com hash divergente",
        )
else:
    transition_handoff_file.write_text(transition_text, encoding="utf-8")

cross_document_conformity_payload = {
    "artifact": {
        "id": "a1-cross-document-conformity",
        "version": "1.0.0",
        "immutable": True,
        "generated_at_utc": artifact_generated_at,
        "generated_by_gate": "scripts/validate_wp_a1_8_ci.sh",
    },
    "correlation": {
        "run_id": run_id,
        "change_id": change_id,
        "fingerprint_sha256": fingerprint,
    },
    "canonical_terminology": canonical_terminology,
    "status_alignment": status_alignment,
    "architecture_coherence": architecture_coherence,
    "scope_boundaries": scope_boundaries,
    "e1_operational_flow": e1_operational_flow,
    "containerization_by_epic": containerization_by_epic,
    "validated_documents": {
        "readme_entregas": str(readme_path),
        "entrega_1": str(entrega1_path),
        "roadmap_epicos": str(roadmap_path),
    },
}

cross_document_hash_input = json.dumps(
    cross_document_conformity_payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
cross_document_content_sha256 = hashlib.sha256(cross_document_hash_input).hexdigest()
cross_document_conformity_payload["artifact"]["content_sha256"] = cross_document_content_sha256

cross_document_text = json.dumps(cross_document_conformity_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
if cross_document_conformity_file.exists():
    existing_conformity = load_json(str(cross_document_conformity_file))
    existing_conformity_hash = str(existing_conformity.get("artifact", {}).get("content_sha256", "")).strip()
    if existing_conformity_hash != cross_document_content_sha256:
        fail(
            "a1_8_immutable_cross_document_conformity_changed",
            "Artefato imutável de conformidade documental já existe com hash divergente",
        )
else:
    cross_document_conformity_file.write_text(cross_document_text, encoding="utf-8")

container_operational_matrix_payload = {
    "artifact": {
        "id": "a1-container-operational-matrix",
        "version": "1.0.0",
        "immutable": True,
        "generated_at_utc": artifact_generated_at,
        "generated_by_gate": "scripts/validate_wp_a1_8_ci.sh",
    },
    "correlation": {
        "run_id": run_id,
        "change_id": change_id,
        "fingerprint_sha256": fingerprint,
    },
    "matrix": container_operational_matrix,
    "validation_summary": {
        "epic_order": expected_epic_order,
        "strategic_containers_unique": len(all_orchestrator_containers) == len(set(all_orchestrator_containers)),
        "progressive_vm_convergence": True,
    },
}

container_matrix_hash_input = json.dumps(
    container_operational_matrix_payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
container_matrix_content_sha256 = hashlib.sha256(container_matrix_hash_input).hexdigest()
container_operational_matrix_payload["artifact"]["content_sha256"] = container_matrix_content_sha256

container_matrix_text = json.dumps(container_operational_matrix_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
if container_operational_matrix_file.exists():
    existing_container_matrix = load_json(str(container_operational_matrix_file))
    existing_container_matrix_hash = str(existing_container_matrix.get("artifact", {}).get("content_sha256", "")).strip()
    if existing_container_matrix_hash != container_matrix_content_sha256:
        fail(
            "a1_8_immutable_container_matrix_changed",
            "Artefato imutável da matriz operacional de containers já existe com hash divergente",
        )
else:
    container_operational_matrix_file.write_text(container_matrix_text, encoding="utf-8")

final_acceptance_criteria_payload = {
    "artifact": {
        "id": "a1-final-acceptance-criteria",
        "version": "1.0.0",
        "immutable": True,
        "generated_at_utc": artifact_generated_at,
        "generated_by_gate": "scripts/validate_wp_a1_8_ci.sh",
    },
    "correlation": {
        "run_id": run_id,
        "change_id": change_id,
        "fingerprint_sha256": fingerprint,
    },
    "criteria": final_acceptance_criteria,
    "required_checks_validated": expected_required_check_ids,
}

final_acceptance_hash_input = json.dumps(
    final_acceptance_criteria_payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
final_acceptance_content_sha256 = hashlib.sha256(final_acceptance_hash_input).hexdigest()
final_acceptance_criteria_payload["artifact"]["content_sha256"] = final_acceptance_content_sha256

final_acceptance_text = json.dumps(final_acceptance_criteria_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
if final_acceptance_criteria_file.exists():
    existing_final_acceptance = load_json(str(final_acceptance_criteria_file))
    existing_final_acceptance_hash = str(existing_final_acceptance.get("artifact", {}).get("content_sha256", "")).strip()
    if existing_final_acceptance_hash != final_acceptance_content_sha256:
        fail(
            "a1_8_immutable_final_acceptance_changed",
            "Artefato imutável de critérios finais já existe com hash divergente",
        )
else:
    final_acceptance_criteria_file.write_text(final_acceptance_text, encoding="utf-8")

summary = {
    "decision": "allow",
    "run_id": run_id,
    "change_id": change_id,
    "fingerprint_sha256": fingerprint,
    "summary_file": str(summary_file),
    "closure_dossier_file": str(closure_dossier_file),
    "closure_dossier_sha256": content_sha256,
    "transition_handoff_file": str(transition_handoff_file),
    "transition_handoff_sha256": transition_content_sha256,
    "cross_document_conformity_file": str(cross_document_conformity_file),
    "cross_document_conformity_sha256": cross_document_content_sha256,
    "container_operational_matrix_file": str(container_operational_matrix_file),
    "container_operational_matrix_sha256": container_matrix_content_sha256,
    "final_acceptance_criteria_file": str(final_acceptance_criteria_file),
    "final_acceptance_criteria_sha256": final_acceptance_content_sha256,
    "validated_files": [
        ${WP_A1_5_FILE@Q},
        ${WP_A1_6_FILE@Q},
        ${WP_A1_7_FILE@Q},
        ${WP_A1_8_FILE@Q},
    ],
}
print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
PY

echo "==> Gate final A1.8 concluído com sucesso"
echo "==> Encerramento técnico do Épico 1 validado com evidência auditável"
