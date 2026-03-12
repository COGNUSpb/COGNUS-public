from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

from .pipeline_contract import ALLOWED_STAGE_STATUSES, PIPELINE_STAGE_ORDER, PipelineRun, utc_now_iso


_SHA256_REGEX_CHARS = set("0123456789abcdef")


def _is_sha256(value: str) -> bool:
    normalized = str(value).strip().lower()
    return len(normalized) == 64 and set(normalized).issubset(_SHA256_REGEX_CHARS)


def payload_sha256(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".bin", dir=str(path.parent))
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


@dataclass(frozen=True)
class StageCheckpoint:
    run_id: str
    change_id: str
    stage: str
    stage_status: str
    idempotency_key: str
    input_hash: str
    output_hash: str
    attempt: int
    executor: str
    timestamp_utc: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "StageCheckpoint":
        return cls(
            run_id=str(payload.get("run_id", "")).strip(),
            change_id=str(payload.get("change_id", "")).strip(),
            stage=str(payload.get("stage", "")).strip(),
            stage_status=str(payload.get("stage_status", "")).strip(),
            idempotency_key=str(payload.get("idempotency_key", "")).strip().lower(),
            input_hash=str(payload.get("input_hash", "")).strip().lower(),
            output_hash=str(payload.get("output_hash", "")).strip().lower(),
            attempt=int(payload.get("attempt", 0)),
            executor=str(payload.get("executor", "")).strip(),
            timestamp_utc=str(payload.get("timestamp_utc", "")).strip(),
        )


class CriticalResourceLock:
    def __init__(self, path: Path, file_handle: Any) -> None:
        self.path = path
        self.file_handle = file_handle

    def release(self) -> None:
        fcntl.flock(self.file_handle.fileno(), fcntl.LOCK_UN)
        self.file_handle.close()


class PipelineStateStore:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.control_dir = self.root_dir / "control"
        self.artifacts_dir = self.root_dir / "artifacts"
        self.checkpoints_dir = self.control_dir / "checkpoints"
        self.runs_dir = self.control_dir / "runs"
        self.locks_dir = self.control_dir / "locks"

        self.control_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.locks_dir.mkdir(parents=True, exist_ok=True)

    def stage_checkpoint_path(self, run_id: str, stage: str, idempotency_key: str) -> Path:
        return self.checkpoints_dir / run_id / stage / f"{idempotency_key}.json"

    def run_control_state_path(self, run_id: str) -> Path:
        return self.runs_dir / f"{run_id}.json"

    def stage_artifacts_dir(self, run_id: str, stage: str) -> Path:
        path = self.artifacts_dir / run_id / stage
        path.mkdir(parents=True, exist_ok=True)
        return path

    def persist_run_state(self, run: PipelineRun) -> Path:
        target = self.run_control_state_path(run.run_id)
        payload = json.dumps(run.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
        _atomic_write_text(target, payload)
        return target

    def load_run_state(self, run_id: str) -> Optional[Dict[str, Any]]:
        target = self.run_control_state_path(run_id)
        if not target.exists():
            return None
        return json.loads(target.read_text(encoding="utf-8"))

    def write_artifact(self, run_id: str, stage: str, artifact_name: str, content: bytes) -> Path:
        normalized_name = str(artifact_name).strip()
        if not normalized_name:
            raise ValueError("artifact_name é obrigatório.")
        target = self.stage_artifacts_dir(run_id, stage) / normalized_name
        _atomic_write_bytes(target, content)
        return target

    def write_text_artifact(self, run_id: str, stage: str, artifact_name: str, content: str) -> Path:
        return self.write_artifact(run_id, stage, artifact_name, (content or "").encode("utf-8"))

    def write_json_artifact(self, run_id: str, stage: str, artifact_name: str, payload: Any) -> Path:
        normalized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        return self.write_text_artifact(run_id, stage, artifact_name, normalized)

    def artifact_exists(self, run_id: str, stage: str, artifact_name: str) -> bool:
        target = self.stage_artifacts_dir(run_id, stage) / str(artifact_name).strip()
        return target.exists()

    def list_stage_artifacts(self, run_id: str, stage: str) -> List[str]:
        d = self.stage_artifacts_dir(run_id, stage)
        return [p.name for p in sorted(d.glob("*"))] if d.exists() else []

    def append_json_array_artifact(self, run_id: str, stage: str, artifact_name: str, entry: Any) -> Path:
        """Append an entry to a JSON array artifact. If artifact doesn't exist, create it.

        This is used to accumulate lists of decisions/evidence (eg. workarounds/tech-debt).
        The artifact content will be a JSON array of objects.
        """
        target = self.stage_artifacts_dir(run_id, stage) / str(artifact_name).strip()
        existing: List[Any] = []
        if target.exists():
            try:
                existing_payload = json.loads(target.read_text(encoding="utf-8"))
                if isinstance(existing_payload, list):
                    existing = existing_payload
            except Exception:
                existing = []

        # Append sanitized entry (prefer dict for readability)
        to_append = entry if not isinstance(entry, (str, bytes)) else {"note": entry}
        existing.append(to_append)

        normalized = json.dumps(existing, ensure_ascii=False, indent=2, sort_keys=True)
        _atomic_write_text(target, normalized)
        return target

    def load_checkpoint(self, run_id: str, stage: str, idempotency_key: str) -> Optional[StageCheckpoint]:
        target = self.stage_checkpoint_path(run_id, stage, idempotency_key)
        if not target.exists():
            return None
        payload = json.loads(target.read_text(encoding="utf-8"))
        return StageCheckpoint.from_dict(payload)

    def persist_stage_checkpoint(
        self,
        *,
        run: PipelineRun,
        stage: str,
        stage_status: str,
        input_hash: str,
        output_hash: str,
        attempt: int,
        executor: str,
        timestamp_utc: Optional[str] = None,
    ) -> StageCheckpoint:
        normalized_stage = str(stage).strip().lower()
        normalized_status = str(stage_status).strip().lower()
        normalized_executor = str(executor).strip()
        normalized_input_hash = str(input_hash).strip().lower()
        normalized_output_hash = str(output_hash).strip().lower()

        if normalized_stage not in PIPELINE_STAGE_ORDER:
            raise ValueError(f"Etapa inválida '{stage}'.")
        if normalized_status not in ALLOWED_STAGE_STATUSES:
            raise ValueError(f"stage_status inválido '{stage_status}'.")
        if not _is_sha256(normalized_input_hash):
            raise ValueError("input_hash deve ser SHA-256 canônico.")
        if not _is_sha256(normalized_output_hash):
            raise ValueError("output_hash deve ser SHA-256 canônico.")
        if attempt <= 0:
            raise ValueError("attempt deve ser maior que 0.")
        if not normalized_executor:
            raise ValueError("executor é obrigatório.")

        resolved_timestamp = (timestamp_utc or utc_now_iso()).strip()
        if not resolved_timestamp.endswith("Z"):
            raise ValueError("timestamp_utc deve estar em UTC ISO-8601.")

        idempotency_key = run.idempotency_key(normalized_stage)
        target = self.stage_checkpoint_path(run.run_id, normalized_stage, idempotency_key)

        existing = self.load_checkpoint(run.run_id, normalized_stage, idempotency_key)
        if existing is not None:
            if existing.stage_status == "completed":
                return existing
            if attempt < existing.attempt:
                raise ValueError(
                    f"attempt inválido para checkpoint existente: novo={attempt}, atual={existing.attempt}."
                )

        checkpoint = StageCheckpoint(
            run_id=run.run_id,
            change_id=run.change_id,
            stage=normalized_stage,
            stage_status=normalized_status,
            idempotency_key=idempotency_key,
            input_hash=normalized_input_hash,
            output_hash=normalized_output_hash,
            attempt=attempt,
            executor=normalized_executor,
            timestamp_utc=resolved_timestamp,
        )

        payload = json.dumps(checkpoint.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
        _atomic_write_text(target, payload)
        return checkpoint

    def completed_stages_for_run(self, run: PipelineRun) -> Dict[str, StageCheckpoint]:
        completed: Dict[str, StageCheckpoint] = {}
        for stage in PIPELINE_STAGE_ORDER:
            key = run.idempotency_key(stage)
            checkpoint = self.load_checkpoint(run.run_id, stage, key)
            if checkpoint is not None and checkpoint.stage_status == "completed":
                completed[stage] = checkpoint
        return completed

    def resume_run_from_checkpoints(self, run: PipelineRun) -> PipelineRun:
        completed = self.completed_stages_for_run(run)
        for stage in PIPELINE_STAGE_ORDER:
            run.stage_statuses[stage] = "completed" if stage in completed else "pending"

        first_pending = next(
            (stage for stage in PIPELINE_STAGE_ORDER if run.stage_statuses[stage] != "completed"),
            None,
        )
        run.stage = first_pending or PIPELINE_STAGE_ORDER[-1]
        run.blocked = False
        run.status = run._resolve_pipeline_status()

        if run.status == "success":
            timestamps = [completed[stage].timestamp_utc for stage in PIPELINE_STAGE_ORDER if stage in completed]
            if timestamps:
                run.finished_at = max(timestamps)

        return run

    def acquire_critical_resource_lock(
        self,
        resource_type: str,
        resource_id: str,
        *,
        blocking: bool = False,
    ) -> CriticalResourceLock:
        normalized_type = str(resource_type).strip().lower()
        normalized_id = str(resource_id).strip().lower()
        if not normalized_type or not normalized_id:
            raise ValueError("resource_type e resource_id são obrigatórios para lock.")

        lock_path = self.locks_dir / f"{normalized_type}-{normalized_id}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        file_handle = open(lock_path, "a+", encoding="utf-8")

        operation = fcntl.LOCK_EX
        if not blocking:
            operation |= fcntl.LOCK_NB

        try:
            fcntl.flock(file_handle.fileno(), operation)
        except BlockingIOError as error:
            file_handle.close()
            raise RuntimeError(
                f"Recurso crítico em uso: {normalized_type}:{normalized_id}."
            ) from error

        return CriticalResourceLock(path=lock_path, file_handle=file_handle)

    @contextmanager
    def stage_resource_locks(
        self,
        resources: Iterable[Tuple[str, str]],
        *,
        blocking: bool = False,
    ) -> Generator[List[CriticalResourceLock], None, None]:
        normalized_resources = sorted(
            {
                (str(resource_type).strip().lower(), str(resource_id).strip().lower())
                for resource_type, resource_id in resources
                if str(resource_type).strip() and str(resource_id).strip()
            }
        )

        acquired: List[CriticalResourceLock] = []
        try:
            for resource_type, resource_id in normalized_resources:
                acquired.append(
                    self.acquire_critical_resource_lock(
                        resource_type,
                        resource_id,
                        blocking=blocking,
                    )
                )
            yield acquired
        finally:
            for lock in reversed(acquired):
                lock.release()
