from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Dict, List, Optional, Tuple

from .blueprint_schema import BlueprintValidationResult
from .org_runtime_manifest import OrgRuntimeManifestStateStore
from .pipeline_contract import PipelineRun, utc_now_iso
from .pipeline_observability import EvidenceConsolidationResult, consolidate_run_evidence
from .pipeline_prepare import PrepareExecutionResult, run_prepare_stage
from .provision_types import ProvisionExecutionResult
from .pipeline_provision import run_provision_stage
from .pipeline_state_store import PipelineStateStore, StageCheckpoint, payload_sha256
from .provisioning_ssh_executor import ProvisioningSshExecutor, SshExecutionPolicy


VERIFY_REPORT_FILENAME = "verify-report.json"
RECONCILE_REPORT_FILENAME = "reconcile-report.json"
PIPELINE_REPORT_FILENAME = "pipeline-report.json"
VERIFY_INVENTORY_FILENAME = "inventory-final.json"
READ_STATE_FILENAME = "technical-read-state.json"


@dataclass(frozen=True)
class A2VerifyExecutionResult:
    verify_report: Dict[str, Any]
    pipeline_report: Dict[str, Any]
    inventory_final: Dict[str, Any]
    technical_read_state: Dict[str, Any]
    blocked: bool
    artifacts: Dict[str, str]
    checkpoint: Optional[StageCheckpoint] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verify_report": self.verify_report,
            "pipeline_report": self.pipeline_report,
            "inventory_final": self.inventory_final,
            "technical_read_state": self.technical_read_state,
            "blocked": self.blocked,
            "artifacts": self.artifacts,
            "checkpoint": self.checkpoint.to_dict() if self.checkpoint else None,
        }


@dataclass(frozen=True)
class A2ReconcileExecutionResult:
    reconcile_report: Dict[str, Any]
    blocked: bool
    artifacts: Dict[str, str]
    checkpoint: Optional[StageCheckpoint] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reconcile_report": self.reconcile_report,
            "blocked": self.blocked,
            "artifacts": self.artifacts,
            "checkpoint": self.checkpoint.to_dict() if self.checkpoint else None,
        }


@dataclass(frozen=True)
class A2BackendFlowExecutionResult:
    sequence: List[str]
    blocked: bool
    blocked_stage: str
    decision: str
    stage_statuses: Dict[str, str]
    prepare_result: Optional[PrepareExecutionResult]
    provision_result: Optional[ProvisionExecutionResult]
    reconcile_result: Optional[A2ReconcileExecutionResult]
    verify_result: Optional[A2VerifyExecutionResult]
    technical_read_state: Dict[str, Any]
    artifacts_by_stage: Dict[str, Dict[str, str]]
    evidence: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sequence": list(self.sequence),
            "blocked": self.blocked,
            "blocked_stage": self.blocked_stage,
            "decision": self.decision,
            "stage_statuses": dict(self.stage_statuses),
            "prepare_result": self.prepare_result.to_dict() if self.prepare_result else None,
            "provision_result": self.provision_result.to_dict() if self.provision_result else None,
            "reconcile_result": self.reconcile_result.to_dict() if self.reconcile_result else None,
            "verify_result": self.verify_result.to_dict() if self.verify_result else None,
            "technical_read_state": dict(self.technical_read_state),
            "artifacts_by_stage": {
                str(stage): {str(key): str(value) for key, value in sorted(artifacts.items())}
                for stage, artifacts in sorted(self.artifacts_by_stage.items())
                if isinstance(artifacts, dict)
            },
            "evidence": dict(self.evidence),
        }


def _sorted_issue_dicts(items: Any) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    for item in (items if isinstance(items, list) else []):
        if not isinstance(item, dict):
            continue
        payloads.append(
            {
                str(key): value
                for key, value in sorted(item.items(), key=lambda pair: str(pair[0]))
            }
        )
    return sorted(
        payloads,
        key=lambda item: (
            str(item.get("level", "")).strip().lower(),
            str(item.get("code", "")).strip().lower(),
            str(item.get("path", "")).strip().lower(),
            str(item.get("message", "")).strip().lower(),
        ),
    )


def _decision_reasons_from_provision(
    *,
    provision_result: ProvisionExecutionResult,
    issues: List[Dict[str, Any]],
) -> Tuple[str, List[Dict[str, str]]]:
    provision_report = provision_result.provision_report if isinstance(provision_result.provision_report, dict) else {}
    provision_verify = (
        provision_report.get("verify_report")
        if isinstance(provision_report.get("verify_report"), dict)
        else {}
    )
    has_errors = any(str(item.get("level", "")).strip().lower() == "error" for item in issues)
    candidate_decision = str(provision_verify.get("decision", "")).strip().lower()
    if candidate_decision not in {"allow", "block"}:
        candidate_decision = "block" if provision_result.blocked or has_errors else "allow"
    elif candidate_decision == "allow" and (provision_result.blocked or has_errors):
        candidate_decision = "block"

    reasons: List[Dict[str, str]] = []
    raw_reasons = provision_verify.get("decision_reasons")
    for reason in (raw_reasons if isinstance(raw_reasons, list) else []):
        if not isinstance(reason, dict):
            continue
        code = str(reason.get("code", "")).strip().lower()
        message = str(reason.get("message", "")).strip()
        if not code:
            continue
        reasons.append(
            {
                "code": code,
                "message": message or f"Motivo técnico reproduzível: {code}",
            }
        )

    if reasons:
        unique = {(item["code"], item["message"]): item for item in reasons}
        normalized = sorted(unique.values(), key=lambda item: (item["code"], item["message"]))
        if candidate_decision == "block":
            existing_codes = {item["code"] for item in normalized}
            blocking_codes = sorted(
                {
                    str(item.get("code", "")).strip().lower()
                    for item in issues
                    if str(item.get("level", "")).strip().lower() == "error" and str(item.get("code", "")).strip()
                }
            )
            for code in blocking_codes:
                if code in existing_codes:
                    continue
                normalized.append(
                    {
                        "code": code,
                        "message": f"Motivo técnico reproduzível: {code}",
                    }
                )
            normalized = sorted(normalized, key=lambda item: (item["code"], item["message"]))
        return candidate_decision, normalized

    if candidate_decision == "allow":
        return "allow", [
            {
                "code": "a2_verify_no_blocking_errors",
                "message": "Provisionamento convergiu sem erros bloqueantes para verificação A2.",
            }
        ]

    error_codes = sorted(
        {
            str(item.get("code", "")).strip().lower()
            for item in issues
            if str(item.get("level", "")).strip().lower() == "error" and str(item.get("code", "")).strip()
        }
    )
    if not error_codes:
        error_codes = ["a2_verify_provision_blocked"]
    return "block", [
        {
            "code": code,
            "message": f"Motivo técnico reproduzível: {code}",
        }
        for code in error_codes
    ]


def _recommendations_from_issues(issues: List[Dict[str, Any]]) -> List[str]:
    recommendations = {
        str(item.get("acao_recomendada", "")).strip()
        for item in issues
        if str(item.get("acao_recomendada", "")).strip()
    }
    if not recommendations:
        recommendations = {
            str(item.get("recommendation", "")).strip()
            for item in issues
            if str(item.get("recommendation", "")).strip()
        }
    return sorted(recommendations)


def _pipeline_stage_statuses(
    *,
    stage_statuses: Dict[str, str],
    reconcile_blocked: bool,
    verify_blocked: bool,
) -> Dict[str, str]:
    normalized = {
        "prepare": str(stage_statuses.get("prepare", "pending")).strip().lower() or "pending",
        "provision": str(stage_statuses.get("provision", "pending")).strip().lower() or "pending",
        "reconcile": "failed" if reconcile_blocked else (str(stage_statuses.get("reconcile", "pending")).strip().lower() or "pending"),
        "configure": str(stage_statuses.get("configure", "pending")).strip().lower() or "pending",
        "verify": "failed" if verify_blocked else "completed",
    }
    return normalized


def _runtime_official_target_names(
    *,
    provision_report: Dict[str, Any],
    runtime_inventory: Dict[str, Any],
) -> List[str]:
    targets = set()
    plan = provision_report.get("chaincode_runtime_plan") if isinstance(provision_report.get("chaincode_runtime_plan"), dict) else {}
    for entry in (plan.get("entries") if isinstance(plan.get("entries"), list) else []):
        if not isinstance(entry, dict):
            continue
        runtime_name = str(entry.get("runtime_name", "")).strip()
        if runtime_name.startswith("dev-peer"):
            targets.add(runtime_name)

    inventory_rows = (
        runtime_inventory.get("chaincode_runtime_inventory")
        if isinstance(runtime_inventory.get("chaincode_runtime_inventory"), list)
        else []
    )
    for row in inventory_rows:
        if not isinstance(row, dict):
            continue
        runtime_name = str(row.get("name", "")).strip()
        if runtime_name.startswith("dev-peer"):
            targets.add(runtime_name)
    return sorted(targets)


def _runtime_official_verify_issues(
    *,
    run: PipelineRun,
    provision_report: Dict[str, Any],
    runtime_inventory: Dict[str, Any],
) -> List[Dict[str, Any]]:
    target_runtime_names = _runtime_official_target_names(
        provision_report=provision_report,
        runtime_inventory=runtime_inventory,
    )
    if not target_runtime_names:
        return []

    issues: List[Dict[str, Any]] = []
    runtime_verify = (
        provision_report.get("chaincode_runtime_verify")
        if isinstance(provision_report.get("chaincode_runtime_verify"), dict)
        else {}
    )
    runtime_reconcile = (
        provision_report.get("chaincode_runtime_reconcile")
        if isinstance(provision_report.get("chaincode_runtime_reconcile"), dict)
        else {}
    )

    if not runtime_verify:
        issues.append(
            {
                "level": "error",
                "code": "a2_3_verify_runtime_official_verify_missing",
                "path": "provision_report.chaincode_runtime_verify",
                "message": "Verify A2.3 bloqueado: evidência oficial chaincode_runtime_verify ausente para runtime dev-peer.",
                "acao_recomendada": "Reexecutar provision para gerar runtime-verify-report.json oficial do backend.",
            }
        )
    if not runtime_reconcile:
        issues.append(
            {
                "level": "error",
                "code": "a2_3_verify_runtime_official_reconcile_missing",
                "path": "provision_report.chaincode_runtime_reconcile",
                "message": "Verify A2.3 bloqueado: evidência oficial chaincode_runtime_reconcile ausente para runtime dev-peer.",
                "acao_recomendada": "Reexecutar provision para gerar runtime-reconcile-report.json oficial do backend.",
            }
        )
    if issues:
        return issues

    verify_corr = runtime_verify.get("correlation") if isinstance(runtime_verify.get("correlation"), dict) else {}
    reconcile_corr = runtime_reconcile.get("correlation") if isinstance(runtime_reconcile.get("correlation"), dict) else {}
    for payload_name, correlation in (("chaincode_runtime_verify", verify_corr), ("chaincode_runtime_reconcile", reconcile_corr)):
        corr_run_id = str(correlation.get("run_id", "")).strip()
        corr_change_id = str(correlation.get("change_id", "")).strip()
        if corr_run_id != run.run_id or corr_change_id != run.change_id:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_3_verify_runtime_correlation_mismatch",
                    "path": f"provision_report.{payload_name}.correlation",
                    "message": (
                        "Verify A2.3 bloqueado: correlação oficial divergente no payload de runtime "
                        f"('{payload_name}')."
                    ),
                    "acao_recomendada": "Regenerar evidências oficiais de runtime para o run_id/change_id atual.",
                }
            )

    executor_id = str(runtime_verify.get("executor_id", "")).strip().lower()
    if executor_id != "provisioning-ssh-executor":
        issues.append(
            {
                "level": "error",
                "code": "a2_3_verify_runtime_official_executor_invalid",
                "path": "provision_report.chaincode_runtime_verify.executor_id",
                "message": "Verify A2.3 bloqueado: chaincode_runtime_verify sem executor oficial de backend.",
                "acao_recomendada": "Executar verify de runtime via provisioning-ssh-executor oficial.",
            }
        )

    verify_rows = runtime_verify.get("rows") if isinstance(runtime_verify.get("rows"), list) else []
    reconcile_rows = runtime_reconcile.get("rows") if isinstance(runtime_reconcile.get("rows"), list) else []
    verify_by_runtime = {
        str(row.get("runtime_name", "")).strip(): row
        for row in verify_rows
        if isinstance(row, dict) and str(row.get("runtime_name", "")).strip()
    }
    reconcile_by_runtime = {
        str(row.get("runtime_name", "")).strip(): row
        for row in reconcile_rows
        if isinstance(row, dict) and str(row.get("runtime_name", "")).strip()
    }

    for runtime_name in target_runtime_names:
        verify_row = verify_by_runtime.get(runtime_name)
        if verify_row is None:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_3_verify_runtime_official_row_missing",
                    "path": "provision_report.chaincode_runtime_verify.rows",
                    "runtime_name": runtime_name,
                    "message": f"Verify A2.3 bloqueado: runtime '{runtime_name}' sem linha oficial no chaincode_runtime_verify.",
                    "acao_recomendada": "Reexecutar verify de runtime e publicar rows oficiais completas.",
                }
            )
        elif not bool(verify_row.get("ok", False)):
            issues.append(
                {
                    "level": "error",
                    "code": "a2_3_verify_runtime_not_verified",
                    "path": "provision_report.chaincode_runtime_verify.rows",
                    "runtime_name": runtime_name,
                    "message": f"Verify A2.3 bloqueado: runtime '{runtime_name}' não validado como ok no backend oficial.",
                    "acao_recomendada": "Corrigir falha de runtime e reexecutar bootstrap/verify oficial.",
                }
            )

        reconcile_row = reconcile_by_runtime.get(runtime_name)
        if reconcile_row is None:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_3_verify_runtime_reconcile_row_missing",
                    "path": "provision_report.chaincode_runtime_reconcile.rows",
                    "runtime_name": runtime_name,
                    "message": f"Verify A2.3 bloqueado: runtime '{runtime_name}' sem linha oficial no chaincode_runtime_reconcile.",
                    "acao_recomendada": "Reexecutar reconciliação de runtime para materializar decisão oficial por item.",
                }
            )
        elif not bool(reconcile_row.get("converged", False)):
            issues.append(
                {
                    "level": "error",
                    "code": "a2_3_verify_runtime_not_converged",
                    "path": "provision_report.chaincode_runtime_reconcile.rows",
                    "runtime_name": runtime_name,
                    "message": f"Verify A2.3 bloqueado: runtime '{runtime_name}' segue não convergido no backend oficial.",
                    "acao_recomendada": "Aplicar reconcile_actions oficiais e repetir verify de runtime.",
                }
            )

    return issues


def _official_stage_failure_issues(
    *,
    stage_statuses: Dict[str, str],
) -> List[Dict[str, Any]]:
    stage_code_map = {
        "prepare": "a2_official_stage_prepare_failed",
        "provision": "a2_official_stage_provision_failed",
        "reconcile": "a2_official_stage_reconcile_failed",
    }
    issues: List[Dict[str, Any]] = []
    for stage_name in ("prepare", "provision", "reconcile"):
        status = str(stage_statuses.get(stage_name, "")).strip().lower()
        if status != "failed":
            continue
        issues.append(
            {
                "level": "error",
                "code": stage_code_map[stage_name],
                "path": f"stage_statuses.{stage_name}",
                "message": (
                    "Falha em etapa oficial do pipeline A2 detectada durante verify "
                    f"('{stage_name}')."
                ),
                "acao_recomendada": (
                    "Corrigir falha oficial da etapa e reexecutar o pipeline A2 sem fallback local/degradado."
                ),
            }
        )
    return issues


def _incremental_read_projection(
    *,
    provision_report: Dict[str, Any],
    runtime_inventory: Dict[str, Any],
) -> Dict[str, Any]:
    incremental_plan = (
        provision_report.get("incremental_execution_plan")
        if isinstance(provision_report.get("incremental_execution_plan"), dict)
        else {}
    )
    incremental_reconcile = (
        provision_report.get("incremental_reconcile_report")
        if isinstance(provision_report.get("incremental_reconcile_report"), dict)
        else {}
    )
    inventory_incremental_origin = (
        runtime_inventory.get("incremental_origin_metadata")
        if isinstance(runtime_inventory.get("incremental_origin_metadata"), dict)
        else {}
    )
    plan_entries = incremental_plan.get("entries") if isinstance(incremental_plan.get("entries"), list) else []
    reconcile_rows = (
        incremental_reconcile.get("rows")
        if isinstance(incremental_reconcile.get("rows"), list)
        else []
    )
    operations = sorted(
        {
            str(entry.get("operation_type", "")).strip().lower()
            for entry in plan_entries
            if isinstance(entry, dict) and str(entry.get("operation_type", "")).strip()
        }
    )
    return {
        "active": bool(incremental_plan or incremental_reconcile or inventory_incremental_origin),
        "source_of_truth": "official_backend_artifacts",
        "fallback_mode_allowed": False,
        "operations": operations,
        "execution_plan_fingerprint": str(incremental_plan.get("incremental_plan_fingerprint", "")).strip(),
        "reconcile_fingerprint": str(incremental_reconcile.get("fingerprint", "")).strip(),
        "topology_generation": inventory_incremental_origin.get("topology_generation"),
        "expanded_component_count": len(
            [
                item
                for item in (
                    inventory_incremental_origin.get("components")
                    if isinstance(inventory_incremental_origin.get("components"), list)
                    else []
                )
                if isinstance(item, dict)
            ]
        ),
        "plan_entry_count": len([item for item in plan_entries if isinstance(item, dict)]),
        "reconcile_row_count": len([item for item in reconcile_rows if isinstance(item, dict)]),
    }


def _verify_payload_from_provision(
    *,
    run: PipelineRun,
    stage_statuses: Dict[str, str],
    provision_result: ProvisionExecutionResult,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    provision_report = provision_result.provision_report if isinstance(provision_result.provision_report, dict) else {}
    runtime_inventory = provision_result.runtime_inventory if isinstance(provision_result.runtime_inventory, dict) else {}
    issues = _sorted_issue_dicts(provision_report.get("issues"))
    runtime_official_issues = _runtime_official_verify_issues(
        run=run,
        provision_report=provision_report,
        runtime_inventory=runtime_inventory,
    )
    stage_failure_issues = _official_stage_failure_issues(stage_statuses=stage_statuses)
    if stage_failure_issues:
        issues.extend(_sorted_issue_dicts(stage_failure_issues))
        issues = _sorted_issue_dicts(issues)
    if runtime_official_issues:
        issues.extend(_sorted_issue_dicts(runtime_official_issues))
        issues = _sorted_issue_dicts(issues)
    decision, decision_reasons = _decision_reasons_from_provision(provision_result=provision_result, issues=issues)
    blocked = decision == "block"

    error_codes = sorted(
        {
            str(item.get("code", "")).strip().lower()
            for item in issues
            if str(item.get("level", "")).strip().lower() == "error" and str(item.get("code", "")).strip()
        }
    )
    warning_codes = sorted(
        {
            str(item.get("code", "")).strip().lower()
            for item in issues
            if str(item.get("level", "")).strip().lower() == "warning" and str(item.get("code", "")).strip()
        }
    )
    recommendations = _recommendations_from_issues(issues)
    correlation = (
        provision_report.get("correlation")
        if isinstance(provision_report.get("correlation"), dict)
        else {}
    )
    stage_reports = (
        provision_report.get("stage_reports")
        if isinstance(provision_report.get("stage_reports"), dict)
        else {}
    )

    verify_stage_statuses = _pipeline_stage_statuses(
        stage_statuses=stage_statuses,
        reconcile_blocked=(str(stage_statuses.get("reconcile", "")).strip().lower() == "failed"),
        verify_blocked=blocked,
    )
    verdict = "blocked" if blocked else ("partial" if warning_codes else "success")

    verify_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "verify",
        "flow": "prepare->provision->reconcile->verify",
        "blocked": blocked,
        "verdict": verdict,
        "decision": decision,
        "decision_reasons": decision_reasons,
        "summary": {
            "errors": len(error_codes),
            "warnings": len(warning_codes),
            "issue_count": len(issues),
        },
        "issues": issues,
        "recommendations": recommendations,
        "source": {
            "source_stage": "provision",
            "source_report": "provision_report.verify_report",
            "provision_blocked": bool(provision_result.blocked),
            "runtime_segment_source": "provision_report.chaincode_runtime_verify + provision_report.chaincode_runtime_reconcile",
            "runtime_source_of_truth": "backend_official_artifacts_only",
        },
        "runtime_official_state": {
            "required": bool(_runtime_official_target_names(provision_report=provision_report, runtime_inventory=runtime_inventory)),
            "target_runtime_count": len(_runtime_official_target_names(provision_report=provision_report, runtime_inventory=runtime_inventory)),
            "issues": _sorted_issue_dicts(runtime_official_issues),
        },
        "official_stage_failures": {
            "failed_stages": sorted(
                [
                    stage_name
                    for stage_name in ("prepare", "provision", "reconcile")
                    if str(stage_statuses.get(stage_name, "")).strip().lower() == "failed"
                ]
            ),
            "issues": _sorted_issue_dicts(stage_failure_issues),
        },
        "correlation": correlation,
        "generated_at": utc_now_iso(),
    }

    pipeline_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "blueprint_fingerprint": run.blueprint_fingerprint,
        "resolved_schema_version": run.resolved_schema_version,
        "stage": "verify",
        "pipeline_flow": "prepare -> provision -> reconcile -> verify",
        "final_result": verdict,
        "decision": decision,
        "decision_reasons": decision_reasons,
        "errors": error_codes,
        "warnings": warning_codes,
        "hints": recommendations,
        "blocking_reason_codes": error_codes if blocked else [],
        "stage_statuses": verify_stage_statuses,
        "stage_reports": stage_reports,
        "generated_at": utc_now_iso(),
    }

    inventory_final = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "verify",
        "source": "provision-runtime-inventory",
        "verify_verdict": verdict,
        "inventory": runtime_inventory,
        "correlation": correlation,
        "generated_at": utc_now_iso(),
    }

    technical_read_state = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "verify",
        "read_only": True,
        "projection_type": "a2_4_pipeline_runtime_projection",
        "source_of_truth": "pipeline_state_store",
        "pipeline_flow": "prepare -> provision -> reconcile -> verify",
        "stage_statuses": verify_stage_statuses,
        "decision": decision,
        "decision_reasons": decision_reasons,
        "verify_verdict": verdict,
        "summary": verify_report["summary"],
        "incremental_runtime_state": _incremental_read_projection(
            provision_report=provision_report,
            runtime_inventory=runtime_inventory,
        ),
        "correlation": correlation,
        "stage_artifacts": {},
        "generated_at": utc_now_iso(),
    }

    return verify_report, pipeline_report, inventory_final, technical_read_state


def _write_or_load_verify_state(
    *,
    run: PipelineRun,
    provision_result: ProvisionExecutionResult,
    stage_statuses: Dict[str, str],
    state_store: Optional[PipelineStateStore],
    attempt: int,
    executor: str,
) -> A2VerifyExecutionResult:
    verify_report, pipeline_report, inventory_final, read_state = _verify_payload_from_provision(
        run=run,
        stage_statuses=stage_statuses,
        provision_result=provision_result,
    )
    verify_input_payload = {
        "provision_report": provision_result.provision_report,
        "runtime_inventory": provision_result.runtime_inventory,
        "stage_statuses": stage_statuses,
    }
    verify_input_hash = payload_sha256(verify_input_payload)

    if state_store is None:
        return A2VerifyExecutionResult(
            verify_report=verify_report,
            pipeline_report=pipeline_report,
            inventory_final=inventory_final,
            technical_read_state=read_state,
            blocked=bool(verify_report.get("blocked", False)),
            artifacts={},
            checkpoint=None,
        )

    stage_dir = state_store.stage_artifacts_dir(run.run_id, "verify")
    verify_path = stage_dir / VERIFY_REPORT_FILENAME
    pipeline_path = stage_dir / PIPELINE_REPORT_FILENAME
    inventory_path = stage_dir / VERIFY_INVENTORY_FILENAME
    read_state_path = stage_dir / READ_STATE_FILENAME

    checkpoint = state_store.load_checkpoint(run.run_id, "verify", run.idempotency_key("verify"))
    if (
        checkpoint is not None
        and checkpoint.stage_status == "completed"
        and checkpoint.input_hash == verify_input_hash
        and verify_path.exists()
        and pipeline_path.exists()
        and inventory_path.exists()
    ):
        loaded_verify = json.loads(verify_path.read_text(encoding="utf-8"))
        loaded_pipeline = json.loads(pipeline_path.read_text(encoding="utf-8"))
        loaded_inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        loaded_read_state = (
            json.loads(read_state_path.read_text(encoding="utf-8"))
            if read_state_path.exists()
            else {}
        )
        loaded_output_hash = payload_sha256(
            {
                "verify_report": loaded_verify,
                "pipeline_report": loaded_pipeline,
                "inventory_final": loaded_inventory,
                "technical_read_state": loaded_read_state,
            }
        )
        if checkpoint.output_hash != loaded_output_hash:
            checkpoint = None
        else:
            loaded_verify["reexecution"] = "skipped_completed_checkpoint"
            loaded_pipeline["reexecution"] = "skipped_completed_checkpoint"
            if isinstance(loaded_read_state, dict):
                loaded_read_state["reexecution"] = "skipped_completed_checkpoint"

            artifacts = {
                "verify_report": str(verify_path),
                "pipeline_report": str(pipeline_path),
                "inventory_final": str(inventory_path),
            }
            if read_state_path.exists():
                artifacts["technical_read_state"] = str(read_state_path)

            return A2VerifyExecutionResult(
                verify_report=loaded_verify if isinstance(loaded_verify, dict) else verify_report,
                pipeline_report=loaded_pipeline if isinstance(loaded_pipeline, dict) else pipeline_report,
                inventory_final=loaded_inventory if isinstance(loaded_inventory, dict) else inventory_final,
                technical_read_state=loaded_read_state if isinstance(loaded_read_state, dict) else read_state,
                blocked=bool((loaded_verify or {}).get("blocked", False)),
                artifacts=artifacts,
                checkpoint=checkpoint,
            )

    verify_artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="verify",
        artifact_name=VERIFY_REPORT_FILENAME,
        content=json.dumps(verify_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )
    pipeline_artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="verify",
        artifact_name=PIPELINE_REPORT_FILENAME,
        content=json.dumps(pipeline_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )
    inventory_artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="verify",
        artifact_name=VERIFY_INVENTORY_FILENAME,
        content=json.dumps(inventory_final, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )

    artifacts = {
        "verify_report": str(verify_artifact),
        "pipeline_report": str(pipeline_artifact),
        "inventory_final": str(inventory_artifact),
    }
    read_state["stage_artifacts"] = dict(artifacts)
    read_state_artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="verify",
        artifact_name=READ_STATE_FILENAME,
        content=json.dumps(read_state, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )
    artifacts["technical_read_state"] = str(read_state_artifact)

    state_store.persist_run_state(run)
    persisted_checkpoint = state_store.persist_stage_checkpoint(
        run=run,
        stage="verify",
        stage_status="failed" if bool(verify_report.get("blocked", False)) else "completed",
        input_hash=verify_input_hash,
        output_hash=payload_sha256(
            {
                "verify_report": verify_report,
                "pipeline_report": pipeline_report,
                "inventory_final": inventory_final,
                "technical_read_state": read_state,
            }
        ),
        attempt=max(int(attempt), 1),
        executor=executor,
        timestamp_utc=utc_now_iso(),
    )

    return A2VerifyExecutionResult(
        verify_report=verify_report,
        pipeline_report=pipeline_report,
        inventory_final=inventory_final,
        technical_read_state=read_state,
        blocked=bool(verify_report.get("blocked", False)),
        artifacts=artifacts,
        checkpoint=persisted_checkpoint,
    )


def _reconcile_payload_from_provision(
    *,
    run: PipelineRun,
    provision_result: ProvisionExecutionResult,
) -> Dict[str, Any]:
    provision_report = provision_result.provision_report if isinstance(provision_result.provision_report, dict) else {}
    runtime_reconcile = (
        provision_report.get("chaincode_runtime_reconcile")
        if isinstance(provision_report.get("chaincode_runtime_reconcile"), dict)
        else {}
    )
    summary = runtime_reconcile.get("summary") if isinstance(runtime_reconcile.get("summary"), dict) else {}
    rows = runtime_reconcile.get("rows") if isinstance(runtime_reconcile.get("rows"), list) else []
    row_count = len([item for item in rows if isinstance(item, dict)])
    blocked = bool(runtime_reconcile.get("blocked", False) or provision_result.blocked)
    return {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "reconcile",
        "flow": "prepare->provision->reconcile->verify",
        "source": "provision_report.chaincode_runtime_reconcile",
        "blocked": blocked,
        "summary": dict(summary),
        "row_count": row_count,
        "generated_at": utc_now_iso(),
    }


def _write_or_load_reconcile_state(
    *,
    run: PipelineRun,
    provision_result: ProvisionExecutionResult,
    state_store: Optional[PipelineStateStore],
    attempt: int,
    executor: str,
) -> A2ReconcileExecutionResult:
    reconcile_report = _reconcile_payload_from_provision(run=run, provision_result=provision_result)
    reconcile_input_payload = {
        "provision_report": provision_result.provision_report,
        "runtime_inventory": provision_result.runtime_inventory,
        "runtime_reconcile": (
            provision_result.provision_report.get("chaincode_runtime_reconcile")
            if isinstance(provision_result.provision_report, dict)
            else {}
        ),
    }
    reconcile_input_hash = payload_sha256(reconcile_input_payload)

    if state_store is None:
        return A2ReconcileExecutionResult(
            reconcile_report=reconcile_report,
            blocked=bool(reconcile_report.get("blocked", False)),
            artifacts={},
            checkpoint=None,
        )

    stage_dir = state_store.stage_artifacts_dir(run.run_id, "reconcile")
    reconcile_path = stage_dir / RECONCILE_REPORT_FILENAME
    reconcile_output_hash = payload_sha256({"reconcile_report": reconcile_report})
    if reconcile_path.exists():
        loaded = json.loads(reconcile_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            loaded_input_hash = str(loaded.get("input_hash", "")).strip().lower()
            loaded_output_hash = str(loaded.get("output_hash", "")).strip().lower()
            loaded_status = str(loaded.get("status", "")).strip().lower()
            if (
                loaded_status == "completed"
                and loaded_input_hash == reconcile_input_hash
                and loaded_output_hash == reconcile_output_hash
            ):
                loaded["reexecution"] = "skipped_completed_checkpoint"
                return A2ReconcileExecutionResult(
                    reconcile_report=loaded,
                    blocked=bool(loaded.get("blocked", False)),
                    artifacts={"reconcile_report": str(reconcile_path)},
                    checkpoint=None,
                )

    reconcile_report["status"] = "failed" if bool(reconcile_report.get("blocked", False)) else "completed"
    reconcile_report["input_hash"] = reconcile_input_hash
    reconcile_report["output_hash"] = reconcile_output_hash

    reconcile_artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="reconcile",
        artifact_name=RECONCILE_REPORT_FILENAME,
        content=json.dumps(reconcile_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )
    state_store.persist_run_state(run)
    return A2ReconcileExecutionResult(
        reconcile_report=reconcile_report,
        blocked=bool(reconcile_report.get("blocked", False)),
        artifacts={"reconcile_report": str(reconcile_artifact)},
        checkpoint=None,
    )

    return A2VerifyExecutionResult(
        verify_report=verify_report,
        pipeline_report=pipeline_report,
        inventory_final=inventory_final,
        technical_read_state=read_state,
        blocked=bool(verify_report.get("blocked", False)),
        artifacts=artifacts,
        checkpoint=persisted_checkpoint,
    )


def _persist_skipped_configure_stage(
    *,
    run: PipelineRun,
    state_store: Optional[PipelineStateStore],
    attempt: int,
    executor: str,
) -> Dict[str, str]:
    if state_store is None:
        return {}

    report_payload = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "configure",
        "blocked": False,
        "skipped": True,
        "skip_reason": "a2_2_backend_flow_no_configure_stage",
        "ready_for_verify": True,
        "issues": [],
        "generated_at": utc_now_iso(),
    }
    report_artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="configure",
        artifact_name="configure-report.json",
        content=json.dumps(report_payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )
    state_store.persist_stage_checkpoint(
        run=run,
        stage="configure",
        stage_status="skipped",
        input_hash=payload_sha256(
            {
                "skip_reason": report_payload["skip_reason"],
                "run_id": run.run_id,
                "change_id": run.change_id,
            }
        ),
        output_hash=payload_sha256(report_payload),
        attempt=max(int(attempt), 1),
        executor=executor,
        timestamp_utc=utc_now_iso(),
    )
    return {"configure_report": str(report_artifact)}


def _read_state_for_blocked_prepare(
    *,
    run: PipelineRun,
    prepare_result: PrepareExecutionResult,
    stage_statuses: Dict[str, str],
) -> Dict[str, Any]:
    issues = _sorted_issue_dicts(prepare_result.prepare_report.get("issues"))
    error_codes = sorted(
        {
            str(item.get("code", "")).strip().lower()
            for item in issues
            if str(item.get("level", "")).strip().lower() == "error" and str(item.get("code", "")).strip()
        }
    )
    decision_reasons = [
        {
            "code": code,
            "message": f"Motivo técnico reproduzível: {code}",
        }
        for code in (error_codes or ["a2_prepare_blocked"])
    ]
    return {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "prepare",
        "read_only": True,
        "projection_type": "a2_4_pipeline_runtime_projection",
        "source_of_truth": "pipeline_state_store",
        "pipeline_flow": "prepare -> provision -> reconcile -> verify",
        "stage_statuses": dict(stage_statuses),
        "decision": "block",
        "decision_reasons": decision_reasons,
        "verify_verdict": "blocked",
        "summary": {
            "errors": len(error_codes),
            "warnings": 0,
            "issue_count": len(issues),
        },
        "issues": issues,
        "incremental_runtime_state": {
            "active": False,
            "source_of_truth": "official_backend_artifacts",
            "fallback_mode_allowed": False,
            "operations": [],
            "execution_plan_fingerprint": "",
            "reconcile_fingerprint": "",
            "topology_generation": None,
            "expanded_component_count": 0,
            "plan_entry_count": 0,
            "reconcile_row_count": 0,
        },
        "generated_at": utc_now_iso(),
    }


def _persist_prepare_read_state(
    *,
    run: PipelineRun,
    state_store: Optional[PipelineStateStore],
    read_state: Dict[str, Any],
) -> Dict[str, str]:
    if state_store is None:
        return {}
    artifact = state_store.write_artifact(
        run_id=run.run_id,
        stage="prepare",
        artifact_name=READ_STATE_FILENAME,
        content=json.dumps(read_state, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
    )
    return {"technical_read_state": str(artifact)}


def _sync_run_projection(
    *,
    run: PipelineRun,
    stage_statuses: Dict[str, str],
    blocked: bool,
    terminal_stage: str,
) -> None:
    for stage in ("prepare", "provision", "reconcile", "configure", "verify"):
        run.stage_statuses[stage] = str(stage_statuses.get(stage, "pending")).strip().lower() or "pending"
    run.stage = str(terminal_stage).strip().lower() or "prepare"
    run.blocked = bool(blocked)
    run.status = run._resolve_pipeline_status()
    if run._is_terminal_pipeline_state() and not run.finished_at:
        run.finished_at = utc_now_iso()


def _consolidate_evidence_if_possible(
    *,
    run: PipelineRun,
    state_store: Optional[PipelineStateStore],
) -> Dict[str, Any]:
    if state_store is None:
        return {}
    consolidation: EvidenceConsolidationResult = consolidate_run_evidence(state_store=state_store, run=run)
    return {
        "decision": consolidation.decision,
        "decision_reasons": consolidation.decision_reasons,
        "evidence_valid": consolidation.evidence_valid,
        "pipeline_report_path": consolidation.pipeline_report_path,
        "stage_reports_dir": consolidation.stage_reports_dir,
        "inventory_final_path": consolidation.inventory_final_path,
        "history_path": consolidation.history_path,
        "decision_trace_path": consolidation.decision_trace_path,
    }


def run_a2_backend_flow(
    *,
    run: PipelineRun,
    blueprint_validation: BlueprintValidationResult,
    runtime_snapshot: Optional[Dict[str, Dict[str, Any]]] = None,
    runtime_state: Optional[Dict[str, Dict[str, Any]]] = None,
    state_store: Optional[PipelineStateStore] = None,
    org_runtime_manifest_report: Optional[Dict[str, Any]] = None,
    org_manifest_state_store: Optional[OrgRuntimeManifestStateStore] = None,
    enforce_org_runtime_manifest_gate: bool = False,
    enforce_chaincode_runtime_entry_gate: bool = False,
    enforce_a2_4_incremental_entry_gate: bool = False,
    a2_2_converged_inventory: Optional[Dict[str, Any]] = None,
    a2_3_handoff: Optional[Dict[str, Any]] = None,
    a2_3_readiness_checklist: Optional[Dict[str, Any]] = None,
    a2_3_converged_inventory: Optional[Dict[str, Any]] = None,
    topology_change_intent: Optional[Dict[str, Any]] = None,
    enforce_credential_reference_validation: bool = False,
    connection_profile_registry: Any = None,
    secret_vault_registry: Any = None,
    ssh_executor: Optional[ProvisioningSshExecutor] = None,
    ssh_execution_policy: SshExecutionPolicy = SshExecutionPolicy(),
    attempt: int = 1,
    prepare_executor: str = "prepare-engine",
    provision_executor: str = "provision-engine",
    verify_executor: str = "a2-verify-engine",
    reconcile_executor: str = "a2-reconcile-engine",
    configure_skip_executor: str = "a2-configure-skip-engine",
    consolidate_evidence: bool = True,
) -> A2BackendFlowExecutionResult:
    if not blueprint_validation.valid:
        raise ValueError("Fluxo A2.2 exige blueprint validado (valid=true).")
    if attempt <= 0:
        raise ValueError("attempt deve ser maior que 0.")

    sequence: List[str] = []
    artifacts_by_stage: Dict[str, Dict[str, str]] = {}
    stage_statuses: Dict[str, str] = {
        "prepare": "pending",
        "provision": "pending",
        "reconcile": "pending",
        "configure": "pending",
        "verify": "pending",
    }

    prepare_result = run_prepare_stage(
        run=run,
        blueprint_validation=blueprint_validation,
        runtime_snapshot=runtime_snapshot,
        state_store=state_store,
        executor=prepare_executor,
    )
    sequence.append("prepare")
    artifacts_by_stage["prepare"] = dict(prepare_result.artifacts)
    if prepare_result.blocked:
        stage_statuses["prepare"] = "failed"
        stage_statuses["provision"] = "skipped"
        stage_statuses["reconcile"] = "skipped"
        stage_statuses["configure"] = "skipped"
        stage_statuses["verify"] = "skipped"
        read_state = _read_state_for_blocked_prepare(
            run=run,
            prepare_result=prepare_result,
            stage_statuses=stage_statuses,
        )
        artifacts_by_stage["prepare"].update(
            _persist_prepare_read_state(
                run=run,
                state_store=state_store,
                read_state=read_state,
            )
        )
        _sync_run_projection(
            run=run,
            stage_statuses=stage_statuses,
            blocked=True,
            terminal_stage="prepare",
        )
        if state_store is not None:
            state_store.persist_run_state(run)

        return A2BackendFlowExecutionResult(
            sequence=sequence,
            blocked=True,
            blocked_stage="prepare",
            decision="block",
            stage_statuses=stage_statuses,
            prepare_result=prepare_result,
            provision_result=None,
            reconcile_result=None,
            verify_result=None,
            technical_read_state=read_state,
            artifacts_by_stage=artifacts_by_stage,
            evidence={},
        )

    stage_statuses["prepare"] = "completed"

    provision_result = run_provision_stage(
        run=run,
        blueprint_validation=blueprint_validation,
        execution_plan=prepare_result.execution_plan,
        runtime_state=runtime_state,
        state_store=state_store,
        org_runtime_manifest_report=org_runtime_manifest_report,
        org_manifest_state_store=org_manifest_state_store,
        enforce_org_runtime_manifest_gate=enforce_org_runtime_manifest_gate,
        enforce_chaincode_runtime_entry_gate=enforce_chaincode_runtime_entry_gate,
        enforce_a2_4_incremental_entry_gate=enforce_a2_4_incremental_entry_gate,
        a2_2_converged_inventory=a2_2_converged_inventory,
        a2_3_handoff=a2_3_handoff,
        a2_3_readiness_checklist=a2_3_readiness_checklist,
        a2_3_converged_inventory=a2_3_converged_inventory,
        topology_change_intent=topology_change_intent,
        enforce_credential_reference_validation=enforce_credential_reference_validation,
        connection_profile_registry=connection_profile_registry,
        secret_vault_registry=secret_vault_registry,
        ssh_executor=ssh_executor,
        ssh_execution_policy=ssh_execution_policy,
        executor=provision_executor,
        attempt=attempt,
    )
    sequence.append("provision")
    artifacts_by_stage["provision"] = dict(provision_result.artifacts)
    stage_statuses["provision"] = "failed" if provision_result.blocked else "completed"

    reconcile_result = _write_or_load_reconcile_state(
        run=run,
        provision_result=provision_result,
        state_store=state_store,
        attempt=attempt,
        executor=reconcile_executor,
    )
    sequence.append("reconcile")
    artifacts_by_stage["reconcile"] = dict(reconcile_result.artifacts)
    stage_statuses["reconcile"] = "failed" if reconcile_result.blocked else "completed"

    stage_statuses["configure"] = "skipped"
    artifacts_by_stage["configure"] = _persist_skipped_configure_stage(
        run=run,
        state_store=state_store,
        attempt=attempt,
        executor=configure_skip_executor,
    )

    verify_result = _write_or_load_verify_state(
        run=run,
        provision_result=provision_result,
        stage_statuses=stage_statuses,
        state_store=state_store,
        attempt=attempt,
        executor=verify_executor,
    )
    sequence.append("verify")
    artifacts_by_stage["verify"] = dict(verify_result.artifacts)
    stage_statuses["verify"] = "failed" if verify_result.blocked else "completed"
    decision = "block" if verify_result.blocked else "allow"
    blocked_stage = ""
    if provision_result.blocked:
        blocked_stage = "provision"
    elif reconcile_result.blocked:
        blocked_stage = "reconcile"
    elif verify_result.blocked:
        blocked_stage = "verify"

    _sync_run_projection(
        run=run,
        stage_statuses=stage_statuses,
        blocked=verify_result.blocked,
        terminal_stage="verify",
    )
    if state_store is not None:
        state_store.persist_run_state(run)

    evidence = (
        _consolidate_evidence_if_possible(run=run, state_store=state_store)
        if consolidate_evidence
        else {}
    )

    return A2BackendFlowExecutionResult(
        sequence=sequence,
        blocked=verify_result.blocked,
        blocked_stage=blocked_stage,
        decision=decision,
        stage_statuses=stage_statuses,
        prepare_result=prepare_result,
        provision_result=provision_result,
        reconcile_result=reconcile_result,
        verify_result=verify_result,
        technical_read_state=verify_result.technical_read_state,
        artifacts_by_stage=artifacts_by_stage,
        evidence=evidence,
    )


def load_a2_backend_flow_read_state(
    *,
    run_id: str,
    state_store: PipelineStateStore,
) -> Dict[str, Any]:
    if not str(run_id).strip():
        raise ValueError("run_id é obrigatório para leitura do estado técnico A2.")

    verify_path = state_store.stage_artifacts_dir(str(run_id).strip(), "verify") / READ_STATE_FILENAME
    prepare_path = state_store.stage_artifacts_dir(str(run_id).strip(), "prepare") / READ_STATE_FILENAME
    candidate_paths = [verify_path, prepare_path]
    for path in candidate_paths:
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Payload inválido para estado técnico A2 em '{path}'.")
        return payload

    raise FileNotFoundError(
        f"Estado técnico A2 não encontrado para run_id '{run_id}' (verify/prepare/{READ_STATE_FILENAME})."
    )
