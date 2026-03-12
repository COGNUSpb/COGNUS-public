from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import os
import re
from typing import Any, Dict, List, Optional, Tuple


PIPELINE_STAGE_ORDER: Tuple[str, ...] = ("prepare", "provision", "configure", "verify")
ALLOWED_STAGE_STATUSES = {"pending", "running", "completed", "failed", "skipped"}
ALLOWED_PIPELINE_RESULTS = {"success", "partial", "failed", "blocked"}

_ISO8601_UTC_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3,6})?Z$")
_SHA256_REGEX = re.compile(r"^[a-f0-9]{64}$")
_TRANSITIONS: Dict[str, Tuple[str, ...]] = {
    "pending": ("running", "skipped"),
    "running": ("completed", "failed"),
    "completed": (),
    "failed": (),
    "skipped": (),
}


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_change_id(explicit_change_id: Optional[str] = None) -> str:
    candidate = explicit_change_id if explicit_change_id is not None else os.getenv("BLUEPRINT_CHANGE_ID", "")
    normalized = str(candidate).strip()
    if not normalized:
        raise ValueError("change_id é obrigatório (parâmetro explícito ou variável BLUEPRINT_CHANGE_ID).")
    return normalized


def deterministic_run_id(change_id: str, blueprint_fingerprint: str, resolved_schema_version: str) -> str:
    basis = f"{change_id}|{blueprint_fingerprint}|{resolved_schema_version}".encode("utf-8")
    digest = hashlib.sha256(basis).hexdigest()
    return f"run-{digest[:24]}"


def _validate_iso8601_utc(value: str, field_name: str) -> None:
    if not _ISO8601_UTC_REGEX.fullmatch(value):
        raise ValueError(f"{field_name} deve estar em UTC ISO-8601 (ex.: 2026-02-16T10:00:00Z).")


@dataclass(frozen=True)
class StageContract:
    expected_inputs: Tuple[str, ...]
    expected_outputs: Tuple[str, ...]
    depends_on: Tuple[str, ...]
    completion_criteria: Tuple[str, ...]

    def to_dict(self) -> Dict[str, List[str]]:
        return {
            "expected_inputs": list(self.expected_inputs),
            "expected_outputs": list(self.expected_outputs),
            "depends_on": list(self.depends_on),
            "completion_criteria": list(self.completion_criteria),
        }


DEFAULT_STAGE_CONTRACTS: Dict[str, StageContract] = {
    "prepare": StageContract(
        expected_inputs=(
            "normalized-blueprint.json",
            "blueprint-validation-report.json",
            "environment-profile",
        ),
        expected_outputs=("execution-plan.json", "prepare-report.json"),
        depends_on=(),
        completion_criteria=(
            "preconditions_validated",
            "execution_plan_materialized",
        ),
    ),
    "provision": StageContract(
        expected_inputs=("execution-plan.json", "prepare-report.json"),
        expected_outputs=("runtime-inventory.json", "provision-report.json"),
        depends_on=("prepare",),
        completion_criteria=(
            "runtime_dependencies_ready",
            "host_node_mapping_applied",
        ),
    ),
    "configure": StageContract(
        expected_inputs=("runtime-inventory.json", "provision-report.json"),
        expected_outputs=(
            "connection-profiles.json",
            "network-manifests.json",
            "configure-report.json",
        ),
        depends_on=("prepare", "provision"),
        completion_criteria=(
            "baseline_configuration_applied",
            "critical_components_validated",
        ),
    ),
    "verify": StageContract(
        expected_inputs=("configure-report.json", "network-manifests.json"),
        expected_outputs=("verify-report.json", "pipeline-report.json"),
        depends_on=("prepare", "provision", "configure"),
        completion_criteria=(
            "topology_health_checks_passed",
            "inventory_consistency_validated",
        ),
    ),
}


@dataclass
class PipelineRun:
    run_id: str
    change_id: str
    blueprint_fingerprint: str
    resolved_schema_version: str
    stage: str
    status: str
    started_at: str
    finished_at: str = ""
    stage_statuses: Dict[str, str] = field(default_factory=dict)
    stage_contracts: Dict[str, StageContract] = field(default_factory=dict)
    blocked: bool = False

    @classmethod
    def new(
        cls,
        *,
        blueprint_fingerprint: str,
        resolved_schema_version: str,
        change_id: Optional[str] = None,
        run_id: Optional[str] = None,
        started_at: Optional[str] = None,
    ) -> "PipelineRun":
        resolved_change_id = resolve_change_id(change_id)
        fingerprint = str(blueprint_fingerprint).strip().lower()
        if not _SHA256_REGEX.fullmatch(fingerprint):
            raise ValueError("blueprint_fingerprint deve ser SHA-256 canônico (64 hex minúsculo).")

        schema_version = str(resolved_schema_version).strip()
        if not schema_version:
            raise ValueError("resolved_schema_version é obrigatório.")

        start = started_at or utc_now_iso()
        _validate_iso8601_utc(start, "started_at")

        normalized_run_id = run_id or deterministic_run_id(resolved_change_id, fingerprint, schema_version)
        statuses = {stage: "pending" for stage in PIPELINE_STAGE_ORDER}
        contracts = {stage: DEFAULT_STAGE_CONTRACTS[stage] for stage in PIPELINE_STAGE_ORDER}

        return cls(
            run_id=normalized_run_id,
            change_id=resolved_change_id,
            blueprint_fingerprint=fingerprint,
            resolved_schema_version=schema_version,
            stage="prepare",
            status="partial",
            started_at=start,
            finished_at="",
            stage_statuses=statuses,
            stage_contracts=contracts,
            blocked=False,
        )

    def transition_stage(
        self,
        stage: str,
        new_stage_status: str,
        *,
        blocked: bool = False,
        finished_at: Optional[str] = None,
    ) -> None:
        normalized_stage = str(stage).strip().lower()
        normalized_new_status = str(new_stage_status).strip().lower()

        if normalized_stage not in self.stage_statuses:
            raise ValueError(f"Etapa inválida '{stage}'.")
        if normalized_new_status not in ALLOWED_STAGE_STATUSES:
            raise ValueError(f"Status de etapa inválido '{new_stage_status}'.")

        current = self.stage_statuses[normalized_stage]
        allowed = _TRANSITIONS[current]
        if normalized_new_status not in allowed:
            raise ValueError(
                f"Transição inválida para etapa '{normalized_stage}': {current} -> {normalized_new_status}."
            )

        self.stage_statuses[normalized_stage] = normalized_new_status
        self.stage = normalized_stage
        if blocked:
            self.blocked = True

        self.status = self._resolve_pipeline_status()

        if self._is_terminal_pipeline_state():
            ended = finished_at or utc_now_iso()
            _validate_iso8601_utc(ended, "finished_at")
            self.finished_at = ended

    def _resolve_pipeline_status(self) -> str:
        if self.blocked:
            return "blocked"

        values = tuple(self.stage_statuses[stage] for stage in PIPELINE_STAGE_ORDER)
        if any(value == "failed" for value in values):
            return "failed"

        if all(value in {"completed", "skipped"} for value in values):
            if all(value == "completed" for value in values):
                return "success"
            return "partial"

        return "partial"

    def _is_terminal_pipeline_state(self) -> bool:
        if self.status in {"failed", "blocked"}:
            return True
        return all(
            self.stage_statuses[stage] in {"completed", "failed", "skipped"}
            for stage in PIPELINE_STAGE_ORDER
        )

    def idempotency_key(self, stage: str) -> str:
        normalized_stage = str(stage).strip().lower()
        if normalized_stage not in self.stage_statuses:
            raise ValueError(f"Etapa inválida '{stage}' para idempotency_key.")

        basis = (
            f"{self.change_id}|{self.blueprint_fingerprint}|{self.resolved_schema_version}|{normalized_stage}"
        ).encode("utf-8")
        return hashlib.sha256(basis).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        ordered_stage_statuses = {
            stage: self.stage_statuses.get(stage, "pending")
            for stage in PIPELINE_STAGE_ORDER
        }
        ordered_stage_contracts = {
            stage: self.stage_contracts[stage].to_dict()
            for stage in PIPELINE_STAGE_ORDER
        }

        report = {
            "run_id": self.run_id,
            "change_id": self.change_id,
            "blueprint_fingerprint": self.blueprint_fingerprint,
            "resolved_schema_version": self.resolved_schema_version,
            "stage": self.stage,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stage_statuses": ordered_stage_statuses,
            "stage_contracts": ordered_stage_contracts,
            "blocked": self.blocked,
        }

        return report
