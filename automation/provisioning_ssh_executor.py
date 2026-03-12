from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .pipeline_contract import PipelineRun, utc_now_iso
from .pipeline_state_store import PipelineStateStore


ALLOWED_SSH_ERROR_CLASSIFICATIONS = {"none", "transient", "definitive"}
_ALLOWED_OVERRIDE_CLASSIFICATIONS = {"transient", "definitive"}
_TRANSIENT_EXIT_CODES = {124, 130, 137, 143, 255}
_TRANSIENT_ERROR_MARKERS = (
    "timed out",
    "timeout",
    "connection reset",
    "temporarily unavailable",
    "resource busy",
    "try again",
    "eagain",
    "econnreset",
    "network is unreachable",
)
_REDACTED = "***REDACTED***"
_SENSITIVE_KEY_MARKERS = (
    "password",
    "passwd",
    "pwd",
    "token",
    "secret",
    "private_key",
    "private-key",
    "credential",
    "authorization",
    "api_key",
    "api-key",
    "client_secret",
    "access_key",
    "secret_key",
)
_SENSITIVE_ASSIGNMENT_REGEX = re.compile(
    r"(?i)(\b[a-z0-9_.-]*(?:password|passwd|pwd|token|secret|api[_-]?key|private[_-]?key|credential|authorization|client_secret|access_key|secret_key)[a-z0-9_.-]*\b\s*(?:=|:)\s*)(\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_BEARER_REGEX = re.compile(r"(?i)\b(bearer)\s+([a-z0-9._=\-+/]+)")
_URL_CREDENTIALS_REGEX = re.compile(r"(?i)([a-z][a-z0-9+.\-]*://[^/\s:@]+:)([^@/\s]+)(@)")
_FABRIC_CA_BOOTSTRAP_REGEX = re.compile(r"(?i)(\s-b\s+[^:\s]+:)([^\s]+)")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _safe_segment(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(value).strip())
    return normalized or "unknown"


def _is_sensitive_key(value: str) -> bool:
    normalized = str(value).strip().lower().replace("-", "_")
    if not normalized:
        return False
    return any(marker in normalized for marker in _SENSITIVE_KEY_MARKERS)


def sanitize_sensitive_text(value: Any) -> str:
    text = str(value)
    if not text:
        return ""

    redacted = text
    redacted = _URL_CREDENTIALS_REGEX.sub(rf"\1{_REDACTED}\3", redacted)
    redacted = _BEARER_REGEX.sub(rf"\1 {_REDACTED}", redacted)
    redacted = _FABRIC_CA_BOOTSTRAP_REGEX.sub(rf"\1{_REDACTED}", redacted)
    redacted = _SENSITIVE_ASSIGNMENT_REGEX.sub(rf"\1{_REDACTED}", redacted)
    return redacted


def _redacted_payload_value(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return _REDACTED
    return f"{_REDACTED}:{_sha256_text(text)[:12]}"


def sanitize_sensitive_payload(value: Any, *, key_hint: str = "") -> Any:
    if isinstance(value, dict):
        sanitized: Dict[str, Any] = {}
        for key, item in sorted(value.items(), key=lambda current: str(current[0])):
            key_text = str(key)
            if _is_sensitive_key(key_text):
                sanitized[key_text] = _redacted_payload_value(item)
            else:
                sanitized[key_text] = sanitize_sensitive_payload(item, key_hint=key_text)
        return sanitized

    if isinstance(value, list):
        return [sanitize_sensitive_payload(item, key_hint=key_hint) for item in value]

    if isinstance(value, tuple):
        return [sanitize_sensitive_payload(item, key_hint=key_hint) for item in value]

    if isinstance(value, str):
        if _is_sensitive_key(key_hint):
            return _redacted_payload_value(value)
        return sanitize_sensitive_text(value)

    if _is_sensitive_key(key_hint):
        return _redacted_payload_value(value)
    return value


def _backoff_seconds(
    *,
    base_backoff_seconds: float,
    backoff_multiplier: float,
    max_backoff_seconds: float,
    attempt: int,
) -> float:
    if attempt <= 0:
        raise ValueError("attempt deve ser maior que 0 para cálculo de backoff.")
    computed = base_backoff_seconds * (backoff_multiplier ** (attempt - 1))
    bounded = min(computed, max_backoff_seconds)
    return round(max(bounded, 0.0), 6)


def build_ssh_unit_idempotency_key(
    *,
    run: PipelineRun,
    host_id: str,
    component_id: str,
    operation: str,
    component_signature: str = "",
) -> str:
    normalized_host = str(host_id).strip()
    normalized_component = str(component_id).strip()
    normalized_operation = str(operation).strip().lower()
    normalized_signature = str(component_signature).strip().lower()
    if not normalized_host:
        raise ValueError("host_id é obrigatório para idempotency_key do executor SSH.")
    if not normalized_component:
        raise ValueError("component_id é obrigatório para idempotency_key do executor SSH.")
    if not normalized_operation:
        raise ValueError("operation é obrigatório para idempotency_key do executor SSH.")

    basis = (
        f"{run.idempotency_key('provision')}|{normalized_host}|{normalized_component}|"
        f"{normalized_operation}|{normalized_signature}"
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SshExecutionPolicy:
    max_attempts: int = 3
    timeout_seconds: float = 30.0
    base_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 30.0
    sleep_during_backoff: bool = False


@dataclass(frozen=True)
class SshCommandRequest:
    run_id: str
    change_id: str
    host_id: str
    component_id: str
    operation: str
    idempotency_key: str
    command: str
    timeout_seconds: float
    attempt: int
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class SshCommandResponse:
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    duration_ms: int = 0
    classification: str = ""


@dataclass(frozen=True)
class SshCommandAttempt:
    attempt: int
    started_at: str
    finished_at: str
    exit_code: int
    stdout_digest: str
    stderr_digest: str
    timeout: bool
    classification: str
    backoff_seconds: float
    duration_ms: int
    error_code: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SshCommandAttempt":
        return cls(
            attempt=int(payload.get("attempt", 0)),
            started_at=str(payload.get("started_at", "")).strip(),
            finished_at=str(payload.get("finished_at", "")).strip(),
            exit_code=int(payload.get("exit_code", 0)),
            stdout_digest=str(payload.get("stdout_digest", "")).strip().lower(),
            stderr_digest=str(payload.get("stderr_digest", "")).strip().lower(),
            timeout=bool(payload.get("timeout", False)),
            classification=str(payload.get("classification", "")).strip().lower(),
            backoff_seconds=float(payload.get("backoff_seconds", 0.0) or 0.0),
            duration_ms=max(int(payload.get("duration_ms", 0) or 0), 0),
            error_code=str(payload.get("error_code", "")).strip().lower(),
        )


@dataclass(frozen=True)
class SshExecutionUnitResult:
    run_id: str
    change_id: str
    host_id: str
    component_id: str
    operation: str
    idempotency_key: str
    command: str
    command_digest: str
    status: str
    classification: str
    final_exit_code: int
    attempts: List[SshCommandAttempt]
    reused: bool
    timeout_seconds: float
    artifact_path: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "change_id": self.change_id,
            "host_id": self.host_id,
            "component_id": self.component_id,
            "operation": self.operation,
            "idempotency_key": self.idempotency_key,
            "command": self.command,
            "command_digest": self.command_digest,
            "status": self.status,
            "classification": self.classification,
            "final_exit_code": self.final_exit_code,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "reused": self.reused,
            "timeout_seconds": self.timeout_seconds,
            "artifact_path": self.artifact_path,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SshExecutionUnitResult":
        attempts = payload.get("attempts", [])
        if not isinstance(attempts, list):
            attempts = []
        return cls(
            run_id=str(payload.get("run_id", "")).strip(),
            change_id=str(payload.get("change_id", "")).strip(),
            host_id=str(payload.get("host_id", "")).strip(),
            component_id=str(payload.get("component_id", "")).strip(),
            operation=str(payload.get("operation", "")).strip().lower(),
            idempotency_key=str(payload.get("idempotency_key", "")).strip().lower(),
            command=str(payload.get("command", "")).strip(),
            command_digest=(
                str(payload.get("command_digest", "")).strip().lower()
                or _sha256_text(str(payload.get("command", "")).strip())
            ),
            status=str(payload.get("status", "")).strip().lower(),
            classification=str(payload.get("classification", "")).strip().lower(),
            final_exit_code=int(payload.get("final_exit_code", 0)),
            attempts=[SshCommandAttempt.from_dict(item) for item in attempts if isinstance(item, dict)],
            reused=bool(payload.get("reused", False)),
            timeout_seconds=float(payload.get("timeout_seconds", 0.0) or 0.0),
            artifact_path=str(payload.get("artifact_path", "")).strip(),
            metadata=(
                sanitize_sensitive_payload(payload.get("metadata", {}))
                if isinstance(payload.get("metadata"), dict)
                else {}
            ),
        )


@dataclass(frozen=True)
class SshExecutionCapturedOutput:
    result: SshExecutionUnitResult
    stdout: str
    stderr: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "result": self.result.to_dict(),
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


def _default_command_runner(request: SshCommandRequest) -> SshCommandResponse:
    return SshCommandResponse(
        exit_code=0,
        stdout=f"ssh-executed:{request.host_id}:{request.component_id}:{request.operation}",
        stderr="",
        timed_out=False,
    )


def _normalize_classification(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in _ALLOWED_OVERRIDE_CLASSIFICATIONS:
        return normalized
    return ""


def _classify_failure(*, exit_code: int, stderr: str, timed_out: bool) -> Tuple[str, str]:
    if timed_out:
        return "transient", "ssh_timeout"
    if exit_code == 0:
        return "none", ""

    stderr_normalized = str(stderr).strip().lower()
    if exit_code in _TRANSIENT_EXIT_CODES:
        return "transient", "ssh_transport_transient"
    if any(marker in stderr_normalized for marker in _TRANSIENT_ERROR_MARKERS):
        return "transient", "ssh_transport_transient"
    return "definitive", "ssh_command_failed"


class ProvisioningSshExecutor:
    def __init__(
        self,
        *,
        state_store: Optional[PipelineStateStore] = None,
        stage: str = "provision",
        policy: SshExecutionPolicy = SshExecutionPolicy(),
        command_runner: Optional[Callable[[SshCommandRequest], SshCommandResponse]] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._state_store = state_store
        self._stage = str(stage).strip().lower() or "provision"
        self._policy = policy
        self._command_runner = command_runner or _default_command_runner
        self._sleep_fn = sleep_fn
        self._validate_policy(policy)

    @staticmethod
    def _validate_policy(policy: SshExecutionPolicy) -> None:
        if policy.max_attempts <= 0:
            raise ValueError("SshExecutionPolicy.max_attempts deve ser maior que 0.")
        if policy.timeout_seconds < 0:
            raise ValueError("SshExecutionPolicy.timeout_seconds não pode ser negativo.")
        if policy.base_backoff_seconds < 0:
            raise ValueError("SshExecutionPolicy.base_backoff_seconds não pode ser negativo.")
        if policy.backoff_multiplier < 1:
            raise ValueError("SshExecutionPolicy.backoff_multiplier deve ser >= 1.")
        if policy.max_backoff_seconds < 0:
            raise ValueError("SshExecutionPolicy.max_backoff_seconds não pode ser negativo.")

    def _unit_artifact_name(self, *, host_id: str, component_id: str, idempotency_key: str) -> str:
        return (
            "ssh-executor/"
            f"{_safe_segment(host_id)}/{_safe_segment(component_id)}/"
            f"{str(idempotency_key).strip().lower()}.json"
        )

    def _unit_artifact_path(self, *, run_id: str, host_id: str, component_id: str, idempotency_key: str) -> Optional[Path]:
        if self._state_store is None:
            return None
        artifact_name = self._unit_artifact_name(
            host_id=host_id,
            component_id=component_id,
            idempotency_key=idempotency_key,
        )
        return self._state_store.stage_artifacts_dir(run_id, self._stage) / artifact_name

    def _load_completed_unit(
        self,
        *,
        run: PipelineRun,
        host_id: str,
        component_id: str,
        idempotency_key: str,
    ) -> Optional[SshExecutionUnitResult]:
        artifact_path = self._unit_artifact_path(
            run_id=run.run_id,
            host_id=host_id,
            component_id=component_id,
            idempotency_key=idempotency_key,
        )
        if artifact_path is None or not artifact_path.exists():
            return None
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        existing = SshExecutionUnitResult.from_dict(payload)
        if existing.status != "completed":
            return None
        return SshExecutionUnitResult.from_dict(
            {
                **existing.to_dict(),
                "reused": True,
                "artifact_path": str(artifact_path),
            }
        )

    def _persist_result(self, *, result: SshExecutionUnitResult) -> str:
        if self._state_store is None:
            return ""
        artifact_name = self._unit_artifact_name(
            host_id=result.host_id,
            component_id=result.component_id,
            idempotency_key=result.idempotency_key,
        )
        payload = json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        path = self._state_store.write_artifact(
            run_id=result.run_id,
            stage=self._stage,
            artifact_name=artifact_name,
            content=payload,
        )
        return str(path)

    def _execute_unit_with_output(
        self,
        *,
        run: PipelineRun,
        host_id: str,
        component_id: str,
        operation: str,
        idempotency_key: str,
        command: str,
        timeout_seconds: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SshExecutionCapturedOutput:
        normalized_host = str(host_id).strip()
        normalized_component = str(component_id).strip()
        normalized_operation = str(operation).strip().lower()
        normalized_key = str(idempotency_key).strip().lower()
        normalized_command = str(command).strip()
        resolved_timeout = self._policy.timeout_seconds if timeout_seconds is None else max(float(timeout_seconds), 0.0)
        payload_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        sanitized_command = sanitize_sensitive_text(normalized_command)
        sanitized_metadata = sanitize_sensitive_payload(payload_metadata)

        if not normalized_host:
            raise ValueError("host_id é obrigatório para execução SSH.")
        if not normalized_component:
            raise ValueError("component_id é obrigatório para execução SSH.")
        if not normalized_operation:
            raise ValueError("operation é obrigatório para execução SSH.")
        if not normalized_key:
            raise ValueError("idempotency_key é obrigatório para execução SSH.")
        if not normalized_command:
            raise ValueError("command é obrigatório para execução SSH.")

        reused = self._load_completed_unit(
            run=run,
            host_id=normalized_host,
            component_id=normalized_component,
            idempotency_key=normalized_key,
        )
        if reused is not None:
            return SshExecutionCapturedOutput(
                result=reused,
                stdout="",
                stderr="",
            )

        attempts: List[SshCommandAttempt] = []
        status = "failed"
        final_classification = "definitive"
        final_exit_code = 1
        final_stdout = ""
        final_stderr = ""

        for attempt in range(1, self._policy.max_attempts + 1):
            started_at = utc_now_iso()
            monotonic_start = time.perf_counter()

            stdout = ""
            stderr = ""
            timed_out = False
            exit_code = 1
            override_classification = ""

            request = SshCommandRequest(
                run_id=run.run_id,
                change_id=run.change_id,
                host_id=normalized_host,
                component_id=normalized_component,
                operation=normalized_operation,
                idempotency_key=normalized_key,
                command=normalized_command,
                timeout_seconds=resolved_timeout,
                attempt=attempt,
                metadata=payload_metadata,
            )

            try:
                response = self._command_runner(request)
                if not isinstance(response, SshCommandResponse):
                    response = SshCommandResponse(
                        exit_code=0,
                        stdout=str(response),
                        stderr="",
                        timed_out=False,
                        duration_ms=0,
                    )
                exit_code = int(response.exit_code)
                stdout = str(response.stdout)
                stderr = str(response.stderr)
                timed_out = bool(response.timed_out)
                override_classification = _normalize_classification(response.classification)
                recorded_duration_ms = max(int(response.duration_ms or 0), 0)
            except TimeoutError as error:  # noqa: PERF203
                exit_code = 124
                stdout = ""
                stderr = str(error).strip() or "SSH timeout."
                timed_out = True
                recorded_duration_ms = 0
            except Exception as error:  # noqa: BLE001, PERF203
                exit_code = 1
                stdout = ""
                stderr = str(error).strip() or error.__class__.__name__
                timed_out = False
                recorded_duration_ms = 0

            duration_ms = max(int((time.perf_counter() - monotonic_start) * 1000), 0)
            if recorded_duration_ms > 0:
                duration_ms = recorded_duration_ms
            if resolved_timeout > 0 and (duration_ms / 1000.0) > resolved_timeout:
                timed_out = True
                if exit_code == 0:
                    exit_code = 124
                if not stderr:
                    stderr = (
                        f"SSH timeout excedido para componente '{normalized_component}' "
                        f"(timeout={resolved_timeout}s, duration={duration_ms}ms)."
                    )

            classification, error_code = _classify_failure(
                exit_code=exit_code,
                stderr=stderr,
                timed_out=timed_out,
            )
            if override_classification:
                classification = override_classification
                error_code = "ssh_transport_transient" if classification == "transient" else "ssh_command_failed"

            backoff_seconds = 0.0
            success = exit_code == 0 and not timed_out
            if success:
                status = "completed"
                final_classification = "none"
                final_exit_code = 0
            else:
                status = "failed"
                final_classification = classification
                final_exit_code = exit_code
                if classification == "transient" and attempt < self._policy.max_attempts:
                    backoff_seconds = _backoff_seconds(
                        base_backoff_seconds=self._policy.base_backoff_seconds,
                        backoff_multiplier=self._policy.backoff_multiplier,
                        max_backoff_seconds=self._policy.max_backoff_seconds,
                        attempt=attempt,
                    )

            finished_at = utc_now_iso()
            attempts.append(
                SshCommandAttempt(
                    attempt=attempt,
                    started_at=started_at,
                    finished_at=finished_at,
                    exit_code=exit_code,
                    stdout_digest=_sha256_text(stdout),
                    stderr_digest=_sha256_text(stderr),
                    timeout=timed_out,
                    classification=classification if not success else "none",
                    backoff_seconds=backoff_seconds,
                    duration_ms=duration_ms,
                    error_code=error_code if not success else "",
                )
            )
            final_stdout = stdout
            final_stderr = stderr

            if success:
                break

            if classification != "transient" or attempt >= self._policy.max_attempts:
                break
            if self._policy.sleep_during_backoff and backoff_seconds > 0:
                self._sleep_fn(backoff_seconds)

        result = SshExecutionUnitResult(
            run_id=run.run_id,
            change_id=run.change_id,
            host_id=normalized_host,
            component_id=normalized_component,
            operation=normalized_operation,
            idempotency_key=normalized_key,
            command=sanitized_command,
            command_digest=_sha256_text(normalized_command),
            status=status,
            classification=final_classification,
            final_exit_code=final_exit_code,
            attempts=attempts,
            reused=False,
            timeout_seconds=resolved_timeout,
            artifact_path="",
            metadata=sanitized_metadata,
        )
        artifact_path = self._persist_result(result=result)
        if artifact_path:
            result = SshExecutionUnitResult.from_dict(
                {
                    **result.to_dict(),
                    "artifact_path": artifact_path,
                }
            )
        return SshExecutionCapturedOutput(
            result=result,
            stdout=final_stdout,
            stderr=final_stderr,
        )

    def execute_unit(
        self,
        *,
        run: PipelineRun,
        host_id: str,
        component_id: str,
        operation: str,
        idempotency_key: str,
        command: str,
        timeout_seconds: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SshExecutionUnitResult:
        capture = self._execute_unit_with_output(
            run=run,
            host_id=host_id,
            component_id=component_id,
            operation=operation,
            idempotency_key=idempotency_key,
            command=command,
            timeout_seconds=timeout_seconds,
            metadata=metadata,
        )
        return capture.result

    def execute_unit_with_output(
        self,
        *,
        run: PipelineRun,
        host_id: str,
        component_id: str,
        operation: str,
        idempotency_key: str,
        command: str,
        timeout_seconds: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SshExecutionCapturedOutput:
        return self._execute_unit_with_output(
            run=run,
            host_id=host_id,
            component_id=component_id,
            operation=operation,
            idempotency_key=idempotency_key,
            command=command,
            timeout_seconds=timeout_seconds,
            metadata=metadata,
        )

    def collect_container_mount_evidence(
        self,
        *,
        run: PipelineRun,
        host_id: str,
        component_id: str,
        container_name: str,
        artifact_name: Optional[str] = None,
        tail_lines: int = 100,
    ) -> Optional[str]:
        """Collect `docker inspect` (Mounts) and recent `docker logs` for a container
        via SSH and persist a textual evidence report in the state store.

        Returns the artifact path string when persisted, or None on failure/unsupported.
        """
        if self._state_store is None:
            return None

        try:
            vol_cmd = f"docker inspect --format '{{{{json .Mounts}}}}' {container_name}"
            logs_cmd = f"docker logs --tail {int(tail_lines)} {container_name}"

            # Use execute_unit_with_output so the SSH executor records unit artifacts too
            vol_res = self.execute_unit_with_output(
                run=run,
                host_id=host_id,
                component_id=component_id,
                operation=f"volume_check:{container_name}",
                idempotency_key=hashlib.sha256(f"{run.run_id}|{host_id}|{component_id}|volume_check|{container_name}".encode("utf-8")).hexdigest(),
                command=vol_cmd,
                timeout_seconds=15.0,
            )

            logs_res = self.execute_unit_with_output(
                run=run,
                host_id=host_id,
                component_id=component_id,
                operation=f"logs_check:{container_name}",
                idempotency_key=hashlib.sha256(f"{run.run_id}|{host_id}|{component_id}|logs_check|{container_name}".encode("utf-8")).hexdigest(),
                command=logs_cmd,
                timeout_seconds=15.0,
            )

            vol_stdout = str(getattr(vol_res, "stdout", "") or "").strip()
            vol_stderr = str(getattr(vol_res, "stderr", "") or "").strip()
            logs_stdout = str(getattr(logs_res, "stdout", "") or "").strip()
            logs_stderr = str(getattr(logs_res, "stderr", "") or "").strip()

            report = (
                "Volume Mounts:\n"
                + (vol_stdout or "(no output)")
                + "\n\nContainer Logs:\n"
                + (logs_stdout or "(no output)")
                + "\n\nErrors:\n"
                + (vol_stderr or "(no stderr)")
                + "\n"
                + (logs_stderr or "(no stderr)")
            )

            name = artifact_name or "gateway-volume-mount-report.txt"
            try:
                path = self._state_store.write_text_artifact(run.run_id, self._stage, name, report)
                return str(path)
            except Exception:
                return None
        except Exception:
            return None
