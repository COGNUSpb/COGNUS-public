from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .pipeline_contract import PIPELINE_STAGE_ORDER, PipelineRun, utc_now_iso
from .pipeline_state_store import PipelineStateStore


TRANSIENT_ERROR_CODES = {
    "prepare_ssh_access_failed",
    "provision_critical_resource_locked",
    "provision_host_failure",
    "provision_ssh_executor_transient_failure",
    "configure_host_failure",
}

DEFINITIVE_ERROR_CODES = {
    "host_runtime_snapshot_missing",
    "prepare_permissions_failed",
    "prepare_workdir_not_writable",
    "prepare_memory_insufficient",
    "prepare_disk_insufficient",
    "prepare_ports_unavailable",
    "provision_port_conflict",
    "provision_host_runtime_incompatible",
    "provision_ssh_executor_definitive_failure",
    "configure_component_not_applied",
    "configure_component_endpoint_missing",
    "verify_precondition_configure_not_completed",
    "verify_component_missing",
    "verify_component_endpoint_missing",
    "verify_channel_member_peer_unreachable",
    "verify_inventory_node_mismatch",
}


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 30.0
    timeout_seconds: float = 0.0
    sleep_during_backoff: bool = False


@dataclass(frozen=True)
class RetryAttemptRecord:
    attempt: int
    started_at: str
    finished_at: str
    duration_ms: int
    result: str
    error_type: str
    error_codes: List[str]
    cause: str
    backoff_seconds: float
    compensation_applied: bool
    compensation_actions: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompensationResult:
    applied: bool
    actions: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StageResilienceResult:
    stage: str
    resumed_from_checkpoint: bool
    attempts: List[RetryAttemptRecord]
    final_status: str
    final_error_type: str
    final_error_codes: List[str]
    final_cause: str
    final_result: Any
    compensation_history: List[CompensationResult]
    retry_report_artifact: str = ""
    attempts_artifact: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage,
            "resumed_from_checkpoint": self.resumed_from_checkpoint,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "final_status": self.final_status,
            "final_error_type": self.final_error_type,
            "final_error_codes": list(self.final_error_codes),
            "final_cause": self.final_cause,
            "final_result": _to_json_compatible(self.final_result),
            "compensation_history": [item.to_dict() for item in self.compensation_history],
            "retry_report_artifact": self.retry_report_artifact,
            "attempts_artifact": self.attempts_artifact,
        }


def _validate_retry_policy(policy: RetryPolicy) -> None:
    if policy.max_attempts <= 0:
        raise ValueError("max_attempts deve ser maior que 0.")
    if policy.base_backoff_seconds < 0:
        raise ValueError("base_backoff_seconds não pode ser negativo.")
    if policy.backoff_multiplier < 1:
        raise ValueError("backoff_multiplier deve ser >= 1.")
    if policy.max_backoff_seconds < 0:
        raise ValueError("max_backoff_seconds não pode ser negativo.")
    if policy.timeout_seconds < 0:
        raise ValueError("timeout_seconds não pode ser negativo.")


def _to_json_compatible(value: Any) -> Any:
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _to_json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(item) for item in value]
    return value


def _stage_report_from_result(stage: str, result: Any) -> Dict[str, Any]:
    if isinstance(result, dict):
        candidate = result.get(f"{stage}_report")
        if isinstance(candidate, dict):
            return candidate
        return result

    attr_name = f"{stage}_report"
    if hasattr(result, attr_name):
        candidate = getattr(result, attr_name)
        if isinstance(candidate, dict):
            return candidate

    if hasattr(result, "to_dict") and callable(result.to_dict):
        payload = result.to_dict()
        if isinstance(payload, dict):
            candidate = payload.get(f"{stage}_report")
            if isinstance(candidate, dict):
                return candidate
            return payload

    return {}


def _stage_blocked_from_result(stage: str, result: Any, report: Dict[str, Any]) -> bool:
    if isinstance(report.get("blocked"), bool):
        return bool(report.get("blocked"))
    if isinstance(result, dict) and isinstance(result.get("blocked"), bool):
        return bool(result.get("blocked"))
    if hasattr(result, "blocked"):
        return bool(getattr(result, "blocked"))
    if stage == "verify":
        verdict = str(report.get("verdict", "")).strip().lower()
        return verdict == "blocked"
    return False


def _error_codes_and_cause(report: Dict[str, Any]) -> Tuple[List[str], str]:
    issues = report.get("issues", []) if isinstance(report, dict) else []
    if not isinstance(issues, list):
        issues = []

    error_codes: List[str] = []
    cause = ""
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        level = str(issue.get("level", "")).strip().lower()
        if level != "error":
            continue
        code = str(issue.get("code", "")).strip()
        message = str(issue.get("message", "")).strip()
        if code:
            error_codes.append(code)
        if not cause and message:
            cause = message

    return sorted(set(error_codes)), cause


def _classify_error_type(error_codes: Sequence[str], blocked: bool) -> str:
    normalized_codes = {str(code).strip() for code in error_codes if str(code).strip()}
    if not normalized_codes and not blocked:
        return "none"

    if normalized_codes & DEFINITIVE_ERROR_CODES:
        return "definitive"
    if normalized_codes & TRANSIENT_ERROR_CODES:
        return "transient"

    if any("lock" in code for code in normalized_codes):
        return "transient"
    return "definitive"


def _backoff_seconds(policy: RetryPolicy, attempt: int) -> float:
    if attempt <= 0:
        raise ValueError("attempt deve ser maior que 0 para cálculo de backoff.")
    computed = policy.base_backoff_seconds * (policy.backoff_multiplier ** (attempt - 1))
    bounded = min(computed, policy.max_backoff_seconds)
    return round(max(bounded, 0.0), 6)


def _append_jsonl_entry(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def apply_stage_compensation(
    *,
    stage: str,
    runtime_state: Optional[Dict[str, Dict[str, Any]]] = None,
) -> CompensationResult:
    normalized_stage = str(stage).strip().lower()
    state = runtime_state if isinstance(runtime_state, dict) else {}
    actions: List[str] = []

    if normalized_stage == "provision":
        for host_ref in sorted(state.keys()):
            host_state = state.get(host_ref)
            if not isinstance(host_state, dict):
                continue

            installed_packages = sorted({str(item).strip() for item in host_state.get("installed_packages", []) if str(item).strip()})
            directories = sorted({str(item).strip() for item in host_state.get("directories", []) if str(item).strip()})
            volumes = sorted({str(item).strip() for item in host_state.get("volumes", []) if str(item).strip()})
            nodes = host_state.get("nodes", {})
            nodes = dict(nodes) if isinstance(nodes, dict) else {}

            for node_id, node_payload in list(nodes.items()):
                if not isinstance(node_payload, dict):
                    nodes.pop(node_id, None)
                    actions.append(f"removed_invalid_node:{host_ref}:{node_id}")
                    continue
                ports = sorted({int(port) for port in (node_payload.get("ports") or []) if str(port).isdigit()})
                if node_payload.get("ports") != ports:
                    node_payload["ports"] = ports
                    actions.append(f"normalized_node_ports:{host_ref}:{node_id}")

            allocated_ports = host_state.get("allocated_ports", {})
            allocated_ports = dict(allocated_ports) if isinstance(allocated_ports, dict) else {}
            valid_node_ids = set(str(node_id) for node_id in nodes.keys())
            for port, owner in list(allocated_ports.items()):
                owner_id = str(owner).strip()
                if owner_id and owner_id not in valid_node_ids:
                    allocated_ports.pop(port, None)
                    actions.append(f"removed_orphan_port:{host_ref}:{port}")

            host_state["installed_packages"] = installed_packages
            host_state["directories"] = directories
            host_state["volumes"] = volumes
            host_state["nodes"] = dict(sorted(nodes.items(), key=lambda item: str(item[0])))
            host_state["allocated_ports"] = dict(
                sorted(
                    {
                        int(port): str(owner).strip()
                        for port, owner in allocated_ports.items()
                        if str(port).isdigit() and str(owner).strip()
                    }.items(),
                    key=lambda item: item[0],
                )
            )

    elif normalized_stage == "configure":
        for host_ref in sorted(state.keys()):
            host_state = state.get(host_ref)
            if not isinstance(host_state, dict):
                continue

            nodes = host_state.get("nodes", {})
            node_ids = set(str(node_id) for node_id in nodes.keys()) if isinstance(nodes, dict) else set()
            configured_nodes = host_state.get("configured_nodes", {})
            configured_nodes = dict(configured_nodes) if isinstance(configured_nodes, dict) else {}

            for node_id in list(configured_nodes.keys()):
                if node_ids and str(node_id) not in node_ids:
                    configured_nodes.pop(node_id, None)
                    actions.append(f"removed_orphan_configured_node:{host_ref}:{node_id}")

            host_state["configured_nodes"] = dict(sorted(configured_nodes.items(), key=lambda item: str(item[0])))

    else:
        actions.append(f"noop_compensation:{normalized_stage}")

    meaningful = [action for action in actions if not action.startswith("noop_compensation:")]
    return CompensationResult(applied=bool(meaningful), actions=actions)


def execute_stage_with_retry(
    *,
    run: PipelineRun,
    stage: str,
    execute_attempt: Callable[[int], Any],
    retry_policy: RetryPolicy = RetryPolicy(),
    state_store: Optional[PipelineStateStore] = None,
    runtime_state: Optional[Dict[str, Dict[str, Any]]] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> StageResilienceResult:
    normalized_stage = str(stage).strip().lower()
    if normalized_stage not in PIPELINE_STAGE_ORDER:
        raise ValueError(f"Etapa inválida '{stage}'.")
    _validate_retry_policy(retry_policy)

    attempts: List[RetryAttemptRecord] = []
    compensations: List[CompensationResult] = []
    final_result: Any = None
    final_status = "failed"
    final_error_type = "definitive"
    final_error_codes: List[str] = []
    final_cause = ""
    resumed_from_checkpoint = False

    attempts_artifact = ""
    retry_report_artifact = ""
    attempts_log_path = ""

    if state_store is not None:
        completed = state_store.load_checkpoint(run.run_id, normalized_stage, run.idempotency_key(normalized_stage))
        if completed is not None and completed.stage_status == "completed":
            resumed_from_checkpoint = True
            return StageResilienceResult(
                stage=normalized_stage,
                resumed_from_checkpoint=True,
                attempts=[],
                final_status="completed",
                final_error_type="none",
                final_error_codes=[],
                final_cause="resumed_completed_checkpoint",
                final_result=None,
                compensation_history=[],
            )

        attempts_log_path = str(state_store.stage_artifacts_dir(run.run_id, normalized_stage) / "retry-attempts.jsonl")
        attempts_artifact = attempts_log_path

    for attempt in range(1, retry_policy.max_attempts + 1):
        started_at = utc_now_iso()
        monotonic_start = time.perf_counter()
        error_codes: List[str] = []
        cause = ""
        blocked = False

        try:
            stage_result = execute_attempt(attempt)
            final_result = stage_result
            report = _stage_report_from_result(normalized_stage, stage_result)
            blocked = _stage_blocked_from_result(normalized_stage, stage_result, report)
            error_codes, cause = _error_codes_and_cause(report)
        except Exception as error:  # noqa: BLE001
            stage_result = None
            final_result = None
            blocked = True
            error_codes = [f"{normalized_stage}_unhandled_exception"]
            cause = str(error).strip() or error.__class__.__name__

        duration_ms = int((time.perf_counter() - monotonic_start) * 1000)
        timed_out = bool(retry_policy.timeout_seconds > 0 and (duration_ms / 1000.0) > retry_policy.timeout_seconds)
        if timed_out:
            blocked = True
            timeout_code = f"{normalized_stage}_timeout"
            error_codes = sorted(set(list(error_codes) + [timeout_code]))
            if not cause:
                cause = (
                    f"Tempo limite excedido na etapa '{normalized_stage}' "
                    f"(duration={duration_ms}ms, timeout={retry_policy.timeout_seconds}s)."
                )

        error_type = _classify_error_type(error_codes, blocked)
        if timed_out:
            error_type = "transient"
        success = not blocked and not error_codes

        compensation = CompensationResult(applied=False, actions=[])
        scheduled_backoff = 0.0
        attempt_result = "success"
        if not success:
            should_retry = error_type == "transient" and attempt < retry_policy.max_attempts
            if should_retry:
                compensation = apply_stage_compensation(stage=normalized_stage, runtime_state=runtime_state)
                compensations.append(compensation)
                scheduled_backoff = _backoff_seconds(retry_policy, attempt)
                attempt_result = "retry"
                if retry_policy.sleep_during_backoff and scheduled_backoff > 0:
                    sleep_fn(scheduled_backoff)
            else:
                attempt_result = "blocked" if blocked else "failed"

        finished_at = utc_now_iso()
        record = RetryAttemptRecord(
            attempt=attempt,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=max(duration_ms, 0),
            result=attempt_result,
            error_type=error_type,
            error_codes=sorted(set(error_codes)),
            cause=cause,
            backoff_seconds=scheduled_backoff,
            compensation_applied=compensation.applied,
            compensation_actions=list(compensation.actions),
        )
        attempts.append(record)

        if attempts_log_path:
            _append_jsonl_entry(
                attempts_log_path,
                {
                    "run_id": run.run_id,
                    "change_id": run.change_id,
                    "stage": normalized_stage,
                    "recorded_at": utc_now_iso(),
                    **record.to_dict(),
                },
            )

        if success:
            final_status = "completed"
            final_error_type = "none"
            final_error_codes = []
            final_cause = ""
            break

        final_status = "blocked" if blocked else "failed"
        final_error_type = error_type
        final_error_codes = sorted(set(error_codes))
        final_cause = cause

        if attempt_result != "retry":
            break

    if state_store is not None:
        retry_report = {
            "run_id": run.run_id,
            "change_id": run.change_id,
            "stage": normalized_stage,
            "resumed_from_checkpoint": resumed_from_checkpoint,
            "final_status": final_status,
            "final_error_type": final_error_type,
            "final_error_codes": final_error_codes,
            "final_cause": final_cause,
            "attempts": [attempt.to_dict() for attempt in attempts],
            "compensations": [compensation.to_dict() for compensation in compensations],
            "generated_at": utc_now_iso(),
        }
        retry_report_path = state_store.write_artifact(
            run_id=run.run_id,
            stage=normalized_stage,
            artifact_name="retry-report.json",
            content=json.dumps(retry_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        retry_report_artifact = str(retry_report_path)

    return StageResilienceResult(
        stage=normalized_stage,
        resumed_from_checkpoint=resumed_from_checkpoint,
        attempts=attempts,
        final_status=final_status,
        final_error_type=final_error_type,
        final_error_codes=final_error_codes,
        final_cause=final_cause,
        final_result=final_result,
        compensation_history=compensations,
        retry_report_artifact=retry_report_artifact,
        attempts_artifact=attempts_artifact,
    )


def resume_pipeline_from_checkpoints(*, run: PipelineRun, state_store: PipelineStateStore) -> PipelineRun:
    return state_store.resume_run_from_checkpoints(run)
