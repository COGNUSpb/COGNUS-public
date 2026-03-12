from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any, Dict, List, Optional, Tuple

from .blueprint_schema import BlueprintValidationResult
from .pipeline_contract import PIPELINE_STAGE_ORDER, PipelineRun, utc_now_iso
from .pipeline_state_store import PipelineStateStore, payload_sha256


MANDATORY_RUNTIME_CHECKS = (
    "ssh_access",
    "permissions_ok",
    "workdir_writable",
    "memory_ok",
    "disk_ok",
    "ports_ok",
)


@dataclass(frozen=True)
class PrepareIssue:
    level: str
    code: str
    path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class HostPreconditionResult:
    host_ref: str
    checks: Dict[str, bool]
    required_memory_mb: int
    available_memory_mb: int
    required_disk_gb: int
    available_disk_gb: int
    required_ports: List[int]
    occupied_ports: List[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host_ref": self.host_ref,
            "checks": dict(sorted(self.checks.items())),
            "required_memory_mb": self.required_memory_mb,
            "available_memory_mb": self.available_memory_mb,
            "required_disk_gb": self.required_disk_gb,
            "available_disk_gb": self.available_disk_gb,
            "required_ports": sorted(self.required_ports),
            "occupied_ports": sorted(self.occupied_ports),
        }


@dataclass(frozen=True)
class PrepareExecutionResult:
    execution_plan: Dict[str, Any]
    prepare_report: Dict[str, Any]
    blocked: bool
    artifacts: Dict[str, str]
    issues: List[PrepareIssue]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_plan": self.execution_plan,
            "prepare_report": self.prepare_report,
            "blocked": self.blocked,
            "artifacts": self.artifacts,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def _sort_issues(issues: List[PrepareIssue]) -> List[PrepareIssue]:
    return sorted(issues, key=lambda item: (item.level, item.code, item.path, item.message))


def _normalize_runtime_snapshot(runtime_snapshot: Optional[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    normalized: Dict[str, Dict[str, Any]] = {}
    for host_ref, snapshot in (runtime_snapshot or {}).items():
        key = str(host_ref).strip()
        if not key:
            continue
        normalized[key] = dict(snapshot or {})
    return normalized


def _group_nodes_by_host(normalized_nodes: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for node in sorted(normalized_nodes, key=lambda current: (str(current.get("host_ref", "")), str(current.get("node_id", "")))):
        host_ref = str(node.get("host_ref", "")).strip()
        if not host_ref:
            continue
        grouped.setdefault(host_ref, []).append(node)
    return grouped


def _resolve_required_resources(nodes: List[Dict[str, Any]]) -> Tuple[int, int, List[int]]:
    total_memory = 0
    total_disk = 0
    required_ports: List[int] = []
    for node in nodes:
        resources = node.get("resources") or {}
        total_memory += int(resources.get("memory_mb", 0) or 0)
        total_disk += int(resources.get("disk_gb", 0) or 0)
        for port in node.get("ports") or []:
            try:
                required_ports.append(int(port))
            except (TypeError, ValueError):
                continue
    return total_memory, total_disk, sorted(set(required_ports))


def _build_host_preconditions(
    *,
    host_ref: str,
    host_nodes: List[Dict[str, Any]],
    runtime_snapshot: Dict[str, Dict[str, Any]],
    issues: List[PrepareIssue],
) -> HostPreconditionResult:
    snapshot = runtime_snapshot.get(host_ref)
    if snapshot is None:
        issues.append(
            PrepareIssue(
                level="error",
                code="host_runtime_snapshot_missing",
                path=f"runtime_snapshot.{host_ref}",
                message=f"Snapshot de runtime ausente para host '{host_ref}'.",
            )
        )
        snapshot = {}

    required_memory, required_disk, required_ports = _resolve_required_resources(host_nodes)
    available_memory = int(snapshot.get("available_memory_mb", 0) or 0)
    available_disk = int(snapshot.get("available_disk_gb", 0) or 0)
    occupied_ports = sorted({int(port) for port in snapshot.get("occupied_ports", []) if str(port).isdigit()})

    checks = {
        "ssh_access": bool(snapshot.get("ssh_access", False)),
        "permissions_ok": bool(snapshot.get("permissions_ok", False)),
        "workdir_writable": bool(snapshot.get("workdir_writable", False)),
        "memory_ok": available_memory >= required_memory,
        "disk_ok": available_disk >= required_disk,
        "ports_ok": not any(port in occupied_ports for port in required_ports),
    }

    if not checks["ssh_access"]:
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_ssh_access_failed",
                path=f"runtime_snapshot.{host_ref}.ssh_access",
                message=f"Host '{host_ref}' sem acesso SSH para etapa prepare.",
            )
        )
    if not checks["permissions_ok"]:
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_permissions_failed",
                path=f"runtime_snapshot.{host_ref}.permissions_ok",
                message=f"Host '{host_ref}' sem permissões mínimas para execução.",
            )
        )
    if not checks["workdir_writable"]:
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_workdir_not_writable",
                path=f"runtime_snapshot.{host_ref}.workdir_writable",
                message=f"Diretório de trabalho não gravável em host '{host_ref}'.",
            )
        )
    if not checks["memory_ok"]:
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_memory_insufficient",
                path=f"runtime_snapshot.{host_ref}.available_memory_mb",
                message=(
                    f"Memória insuficiente em '{host_ref}' (required={required_memory}MB, "
                    f"available={available_memory}MB)."
                ),
            )
        )
    if not checks["disk_ok"]:
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_disk_insufficient",
                path=f"runtime_snapshot.{host_ref}.available_disk_gb",
                message=(
                    f"Disco insuficiente em '{host_ref}' (required={required_disk}GB, "
                    f"available={available_disk}GB)."
                ),
            )
        )
    if not checks["ports_ok"]:
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_ports_unavailable",
                path=f"runtime_snapshot.{host_ref}.occupied_ports",
                message=(
                    f"Portas já ocupadas em '{host_ref}' para o plano prepare: "
                    f"{[port for port in required_ports if port in occupied_ports]}"
                ),
            )
        )

    return HostPreconditionResult(
        host_ref=host_ref,
        checks=checks,
        required_memory_mb=required_memory,
        available_memory_mb=available_memory,
        required_disk_gb=required_disk,
        available_disk_gb=available_disk,
        required_ports=required_ports,
        occupied_ports=occupied_ports,
    )


def _materialize_initial_manifests(
    normalized_nodes: List[Dict[str, Any]],
    normalized_channels: List[Dict[str, Any]],
    normalized_orgs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    node_manifests = []
    for node in sorted(normalized_nodes, key=lambda current: (str(current.get("host_ref", "")), str(current.get("node_id", "")))):
        node_id = str(node.get("node_id", "")).strip()
        host_ref = str(node.get("host_ref", "")).strip()
        org_id = str(node.get("org_id", "")).strip()
        node_type = str(node.get("node_type", "")).strip()
        node_manifests.append(
            {
                "manifest_id": f"manifest-{node_id}",
                "template_ref": f"templates/{node_type}-runtime.yaml",
                "host_ref": host_ref,
                "org_id": org_id,
                "node_id": node_id,
                "node_type": node_type,
                "ports": sorted(int(port) for port in node.get("ports") or []),
            }
        )

    channel_bootstrap = []
    for channel in sorted(normalized_channels, key=lambda current: str(current.get("channel_id", ""))):
        channel_bootstrap.append(
            {
                "channel_id": channel.get("channel_id"),
                "members": sorted(channel.get("members") or []),
                "type": channel.get("type"),
            }
        )

    org_bootstrap = []
    for org in sorted(normalized_orgs, key=lambda current: str(current.get("org_id", ""))):
        org_bootstrap.append(
            {
                "org_id": org.get("org_id"),
                "msp_id": org.get("msp_id"),
                "domain": org.get("domain"),
            }
        )

    return {
        "node_manifests": node_manifests,
        "channel_bootstrap": channel_bootstrap,
        "org_bootstrap": org_bootstrap,
    }


def _build_crypto_preconditions(
    blueprint: BlueprintValidationResult,
) -> Dict[str, Any]:
    identity_baseline = (
        blueprint.normalized_identity_baseline
        if isinstance(blueprint.normalized_identity_baseline, dict)
        else {}
    )
    baseline_version = str(identity_baseline.get("baseline_version", "")).strip()
    schema_ref = str(identity_baseline.get("schema_ref", "")).strip()
    trust_domains = [
        item
        for item in (identity_baseline.get("trust_domains") or [])
        if isinstance(item, dict)
    ]
    org_crypto_profiles = [
        item
        for item in (identity_baseline.get("org_crypto_profiles") or [])
        if isinstance(item, dict)
    ]

    required_org_ids = sorted(
        {
            str(item.get("org_id", "")).strip().lower()
            for item in blueprint.normalized_orgs
            if str(item.get("org_id", "")).strip()
        }
    )
    profile_by_org = {
        str(item.get("org_id", "")).strip().lower(): item
        for item in org_crypto_profiles
        if str(item.get("org_id", "")).strip()
    }
    missing_org_profiles = sorted(org_id for org_id in required_org_ids if org_id not in profile_by_org)

    profile_error_codes: List[str] = []
    for org_id in sorted(profile_by_org.keys()):
        profile = profile_by_org[org_id]
        storage = profile.get("storage") if isinstance(profile.get("storage"), dict) else {}
        ca_profile = profile.get("ca") if isinstance(profile.get("ca"), dict) else {}
        tls_ca_profile = profile.get("tls_ca") if isinstance(profile.get("tls_ca"), dict) else {}

        if not str(profile.get("ca_profile", "")).strip() or not str(profile.get("tls_ca_profile", "")).strip():
            profile_error_codes.append(f"org_profile_name_missing:{org_id}")
        if not str(storage.get("private_key_store", "")).strip():
            profile_error_codes.append(f"org_private_key_store_missing:{org_id}")
        if not str(storage.get("ca_material_path", "")).strip() or not str(storage.get("tls_ca_material_path", "")).strip():
            profile_error_codes.append(f"org_material_path_missing:{org_id}")

        for key in ("algorithm", "key_size", "validity_days", "rotation_days"):
            if key not in ca_profile:
                profile_error_codes.append(f"org_ca_{key}_missing:{org_id}")
            if key not in tls_ca_profile:
                profile_error_codes.append(f"org_tls_ca_{key}_missing:{org_id}")

    validation_errors: List[str] = []
    if not baseline_version:
        validation_errors.append("identity_baseline_version_missing")
    if not schema_ref:
        validation_errors.append("identity_baseline_schema_ref_missing")
    if missing_org_profiles:
        validation_errors.append("identity_org_crypto_profiles_missing")
    validation_errors.extend(sorted(set(profile_error_codes)))

    return {
        "valid": len(validation_errors) == 0,
        "baseline_version": baseline_version,
        "schema_ref": schema_ref,
        "trust_domain_count": len(trust_domains),
        "org_profile_count": len(profile_by_org),
        "required_org_ids": required_org_ids,
        "missing_org_profiles": missing_org_profiles,
        "validation_errors": sorted(validation_errors),
    }


def _build_execution_plan(
    *,
    run: PipelineRun,
    blueprint: BlueprintValidationResult,
    host_checks: List[HostPreconditionResult],
    crypto_preconditions: Dict[str, Any],
) -> Dict[str, Any]:
    nodes_by_host = _group_nodes_by_host(blueprint.normalized_nodes)
    manifests = _materialize_initial_manifests(
        blueprint.normalized_nodes,
        blueprint.normalized_channels,
        blueprint.normalized_orgs,
    )

    host_plan = []
    for host_ref in sorted(nodes_by_host.keys()):
        nodes = nodes_by_host[host_ref]
        host_plan.append(
            {
                "host_ref": host_ref,
                "nodes": [
                    {
                        "node_id": node.get("node_id"),
                        "org_id": node.get("org_id"),
                        "node_type": node.get("node_type"),
                        "ports": sorted(int(port) for port in node.get("ports") or []),
                    }
                    for node in nodes
                ],
            }
        )

    return {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "blueprint_fingerprint": run.blueprint_fingerprint,
        "resolved_schema_version": run.resolved_schema_version,
        "stage": "prepare",
        "stage_order": list(PIPELINE_STAGE_ORDER),
        "environment_profile": blueprint.normalized_environment_profile,
        "host_plan": host_plan,
        "preconditions": [check.to_dict() for check in sorted(host_checks, key=lambda current: current.host_ref)],
        "crypto_preconditions": crypto_preconditions,
        "manifests": manifests,
        "generated_at": utc_now_iso(),
    }


def run_prepare_stage(
    *,
    run: PipelineRun,
    blueprint_validation: BlueprintValidationResult,
    runtime_snapshot: Optional[Dict[str, Dict[str, Any]]] = None,
    state_store: Optional[PipelineStateStore] = None,
    executor: str = "prepare-engine",
) -> PrepareExecutionResult:
    if not blueprint_validation.valid:
        raise ValueError("prepare exige blueprint validado pelo gate A1.2 (valid=True).")
    if any(issue.level == "error" for issue in blueprint_validation.issues):
        raise ValueError("prepare requer ausência de erros no resultado de validação do blueprint.")

    normalized_runtime = _normalize_runtime_snapshot(runtime_snapshot)
    nodes_by_host = _group_nodes_by_host(blueprint_validation.normalized_nodes)
    issues: List[PrepareIssue] = []
    host_checks: List[HostPreconditionResult] = []

    for host_ref in sorted(nodes_by_host.keys()):
        host_checks.append(
            _build_host_preconditions(
                host_ref=host_ref,
                host_nodes=nodes_by_host[host_ref],
                runtime_snapshot=normalized_runtime,
                issues=issues,
            )
        )

    issues = _sort_issues(issues)
    crypto_preconditions = _build_crypto_preconditions(blueprint_validation)
    if not bool(crypto_preconditions.get("valid", False)):
        issues.append(
            PrepareIssue(
                level="error",
                code="prepare_crypto_preconditions_invalid",
                path="identity_baseline",
                message=(
                    "Pré-condições criptográficas inválidas para iniciar provision/configure: "
                    f"{crypto_preconditions.get('validation_errors', [])}"
                ),
            )
        )
        issues = _sort_issues(issues)
    blocked = any(issue.level == "error" for issue in issues)

    execution_plan = _build_execution_plan(
        run=run,
        blueprint=blueprint_validation,
        host_checks=host_checks,
        crypto_preconditions=crypto_preconditions,
    )
    precondition_summary = {
        "required_checks": list(MANDATORY_RUNTIME_CHECKS),
        "passed": not blocked,
        "failed_codes": [issue.code for issue in issues if issue.level == "error"],
        "crypto_preconditions_valid": bool(crypto_preconditions.get("valid", False)),
    }

    prepare_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "prepare",
        "blocked": blocked,
        "ready_for_provision": not blocked,
        "precondition_summary": precondition_summary,
        "crypto_preconditions": crypto_preconditions,
        "issues": [issue.to_dict() for issue in issues],
        "generated_at": utc_now_iso(),
    }

    artifacts: Dict[str, str] = {}
    if state_store is not None:
        execution_plan_bytes = json.dumps(
            execution_plan,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        prepare_report_bytes = json.dumps(
            prepare_report,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")

        plan_artifact = state_store.write_artifact(
            run_id=run.run_id,
            stage="prepare",
            artifact_name="execution-plan.json",
            content=execution_plan_bytes,
        )
        report_artifact = state_store.write_artifact(
            run_id=run.run_id,
            stage="prepare",
            artifact_name="prepare-report.json",
            content=prepare_report_bytes,
        )
        artifacts = {
            "execution_plan": str(plan_artifact),
            "prepare_report": str(report_artifact),
        }

        state_store.persist_run_state(run)
        state_store.persist_stage_checkpoint(
            run=run,
            stage="prepare",
            stage_status="failed" if blocked else "completed",
            input_hash=payload_sha256(
                {
                    "runtime_snapshot": normalized_runtime,
                    "blueprint_fingerprint": run.blueprint_fingerprint,
                    "resolved_schema_version": run.resolved_schema_version,
                }
            ),
            output_hash=payload_sha256(
                {
                    "execution_plan": execution_plan,
                    "prepare_report": prepare_report,
                }
            ),
            attempt=1,
            executor=executor,
            timestamp_utc=utc_now_iso(),
        )

    return PrepareExecutionResult(
        execution_plan=execution_plan,
        prepare_report=prepare_report,
        blocked=blocked,
        artifacts=artifacts,
        issues=issues,
    )
