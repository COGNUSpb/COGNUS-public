from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from .org_runtime_manifest import OrgRuntimeManifestStateStore
from .pipeline_contract import PipelineRun


_SHA256_CHARS = set("0123456789abcdef")
_A22_REQUIRED_COMPONENT_TYPES = {"peer", "couch", "orderer", "ca", "api_gateway", "network_api"}
_A24_INCREMENTAL_COMPONENT_TYPES = {"peer", "orderer"}


def _is_sha256(value: str) -> bool:
    normalized = str(value).strip().lower()
    return len(normalized) == 64 and set(normalized).issubset(_SHA256_CHARS)


@dataclass(frozen=True)
class A2ProvisionEntryGateIssue:
    level: str
    code: str
    path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class A2ProvisionEntryGateResult:
    allowed: bool
    blocked: bool
    change_id: str
    run_id: str
    org_id: str
    environment_profile_ref: str
    source_blueprint_fingerprint: str
    manifest_fingerprint: str
    persisted: bool
    persisted_manifest_path: str
    issues: List[A2ProvisionEntryGateIssue]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "blocked": self.blocked,
            "change_id": self.change_id,
            "run_id": self.run_id,
            "org_id": self.org_id,
            "environment_profile_ref": self.environment_profile_ref,
            "source_blueprint_fingerprint": self.source_blueprint_fingerprint,
            "manifest_fingerprint": self.manifest_fingerprint,
            "persisted": self.persisted,
            "persisted_manifest_path": self.persisted_manifest_path,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class A2ChaincodeRuntimeEntryGateIssue:
    level: str
    code: str
    path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class A2ChaincodeRuntimeEntryGateResult:
    allowed: bool
    blocked: bool
    change_id: str
    run_id: str
    org_id: str
    source_blueprint_fingerprint: str
    manifest_fingerprint: str
    manifest_persisted: bool
    persisted_manifest_path: str
    baseline_converged: bool
    converged_inventory_fingerprint: str
    chaincode_runtime_count: int
    issues: List[A2ChaincodeRuntimeEntryGateIssue]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "blocked": self.blocked,
            "change_id": self.change_id,
            "run_id": self.run_id,
            "org_id": self.org_id,
            "source_blueprint_fingerprint": self.source_blueprint_fingerprint,
            "manifest_fingerprint": self.manifest_fingerprint,
            "manifest_persisted": self.manifest_persisted,
            "persisted_manifest_path": self.persisted_manifest_path,
            "baseline_converged": self.baseline_converged,
            "converged_inventory_fingerprint": self.converged_inventory_fingerprint,
            "chaincode_runtime_count": self.chaincode_runtime_count,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class A2IncrementalTopologyEntryGateIssue:
    level: str
    code: str
    path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class A2IncrementalTopologyEntryGateResult:
    allowed: bool
    blocked: bool
    change_id: str
    run_id: str
    org_id: str
    source_blueprint_fingerprint: str
    manifest_fingerprint: str
    handoff_decision: str
    topology_change_intent_fingerprint: str
    topology_change_operations: List[str]
    incremental_component_allocations: List[Dict[str, str]]
    incremental_allocation_fingerprint: str
    incremental_placement_decisions: List[Dict[str, Any]]
    incremental_placement_fingerprint: str
    readiness_ready_for_a2_4_a2_5: bool
    baseline_converged_for_org: bool
    converged_inventory_fingerprint: str
    issues: List[A2IncrementalTopologyEntryGateIssue]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "blocked": self.blocked,
            "change_id": self.change_id,
            "run_id": self.run_id,
            "org_id": self.org_id,
            "source_blueprint_fingerprint": self.source_blueprint_fingerprint,
            "manifest_fingerprint": self.manifest_fingerprint,
            "handoff_decision": self.handoff_decision,
            "topology_change_intent_fingerprint": self.topology_change_intent_fingerprint,
            "topology_change_operations": list(self.topology_change_operations),
            "incremental_component_allocations": [dict(item) for item in self.incremental_component_allocations],
            "incremental_allocation_fingerprint": self.incremental_allocation_fingerprint,
            "incremental_placement_decisions": [dict(item) for item in self.incremental_placement_decisions],
            "incremental_placement_fingerprint": self.incremental_placement_fingerprint,
            "readiness_ready_for_a2_4_a2_5": self.readiness_ready_for_a2_4_a2_5,
            "baseline_converged_for_org": self.baseline_converged_for_org,
            "converged_inventory_fingerprint": self.converged_inventory_fingerprint,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def _sort_issues(issues: List[A2ProvisionEntryGateIssue]) -> List[A2ProvisionEntryGateIssue]:
    return sorted(issues, key=lambda issue: (issue.level, issue.code, issue.path, issue.message))


def _sort_runtime_issues(
    issues: List[A2ChaincodeRuntimeEntryGateIssue],
) -> List[A2ChaincodeRuntimeEntryGateIssue]:
    return sorted(issues, key=lambda issue: (issue.level, issue.code, issue.path, issue.message))


def _sort_incremental_issues(
    issues: List[A2IncrementalTopologyEntryGateIssue],
) -> List[A2IncrementalTopologyEntryGateIssue]:
    return sorted(issues, key=lambda issue: (issue.level, issue.code, issue.path, issue.message))


def _component_active(component: Dict[str, Any]) -> bool:
    desired_state = str(component.get("desired_state", "")).strip().lower()
    return desired_state != "planned"


def _manifest_required_component_keys(
    report: Dict[str, Any],
) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {component_type: set() for component_type in _A22_REQUIRED_COMPONENT_TYPES}
    components = report.get("normalized_components")
    if not isinstance(components, list):
        return result
    for component in components:
        if not isinstance(component, dict) or not _component_active(component):
            continue
        component_type = str(component.get("component_type", "")).strip().lower()
        if component_type not in _A22_REQUIRED_COMPONENT_TYPES:
            continue
        key = str(component.get("name", "")).strip() or str(component.get("component_id", "")).strip()
        if key:
            result[component_type].add(key)
    return result


def _fingerprint_payload(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_incremental_issues(
    base_issues: List[A2ProvisionEntryGateIssue],
) -> List[A2IncrementalTopologyEntryGateIssue]:
    return [
        A2IncrementalTopologyEntryGateIssue(
            level=issue.level,
            code=issue.code,
            path=issue.path,
            message=issue.message,
        )
        for issue in base_issues
    ]


def _required_boolean_check(
    *,
    checklist: Dict[str, Any],
    key: str,
    path: str,
    issues: List[A2IncrementalTopologyEntryGateIssue],
) -> bool:
    if key not in checklist:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_readiness_check_missing",
                path=path,
                message=f"Checklist de prontidão A2.3 não contém a chave obrigatória '{key}'.",
            )
        )
        return False

    value = checklist.get(key)
    if not isinstance(value, bool):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_readiness_check_invalid_type",
                path=path,
                message=f"Checklist de prontidão A2.3 deve usar booleano para '{key}'.",
            )
        )
        return False

    if not value:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_readiness_check_not_satisfied",
                path=path,
                message=f"Checklist de prontidão A2.3 bloqueia entrada A2.4: '{key}'=false.",
            )
        )
    return bool(value)


def _extract_a23_checklist_payload(
    readiness_checklist: Any,
    issues: List[A2IncrementalTopologyEntryGateIssue],
) -> Dict[str, bool]:
    if not isinstance(readiness_checklist, dict):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_readiness_payload_not_object",
                path="a2_3_readiness_checklist",
                message="Gate A2.4 exige payload de checklist de prontidão A2.3 em formato objeto.",
            )
        )
        return {}

    checklist_raw = readiness_checklist.get("checklist")
    checklist = checklist_raw if isinstance(checklist_raw, dict) else readiness_checklist
    if not isinstance(checklist, dict):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_readiness_checklist_not_object",
                path="a2_3_readiness_checklist.checklist",
                message="Checklist de prontidão A2.3 deve ser objeto.",
            )
        )
        return {}

    required_keys = (
        "runtime_converged",
        "final_inventory_complete",
        "reproducible_diagnostics",
        "official_artifacts_integrity",
        "ready_for_a2_4_a2_5",
    )
    normalized: Dict[str, bool] = {}
    for key in required_keys:
        normalized[key] = _required_boolean_check(
            checklist=checklist,
            key=key,
            path=f"a2_3_readiness_checklist.checklist.{key}",
            issues=issues,
        )
    return normalized


def _validate_a23_handoff_payload(
    *,
    handoff: Any,
    run: PipelineRun,
    manifest_fingerprint: str,
    source_blueprint_fingerprint: str,
    issues: List[A2IncrementalTopologyEntryGateIssue],
) -> Dict[str, str]:
    if not isinstance(handoff, dict):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_not_object",
                path="a2_3_handoff",
                message="Gate A2.4 exige handoff A2.3→A2.4/A2.5 em formato objeto.",
            )
        )
        return {"handoff_decision": "", "change_id": "", "run_id": "", "manifest_fingerprint": "", "source_blueprint_fingerprint": ""}

    decision = str(handoff.get("handoff_decision", "")).strip().lower()
    if decision != "allow":
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_decision_blocked",
                path="a2_3_handoff.handoff_decision",
                message="Gate A2.4 exige handoff_decision='allow' no handoff A2.3.",
            )
        )

    source_wp = str(handoff.get("source_wp", "")).strip().upper()
    if source_wp and source_wp != "A2.3":
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_source_wp_invalid",
                path="a2_3_handoff.source_wp",
                message="Handoff A2.3 inválido: source_wp deve ser 'A2.3'.",
            )
        )

    target_wps = handoff.get("target_wps")
    if not isinstance(target_wps, list) or "A2.4" not in {str(item).strip().upper() for item in target_wps}:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_target_wp_missing",
                path="a2_3_handoff.target_wps",
                message="Handoff A2.3 deve declarar A2.4 em target_wps.",
            )
        )

    dependencies = handoff.get("dependencies")
    if isinstance(dependencies, list):
        for index, item in enumerate(dependencies):
            if not isinstance(item, dict):
                continue
            target_wp = str(item.get("target_wp", "")).strip().upper()
            if target_wp == "A2.4" and not bool(item.get("satisfied", False)):
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_handoff_dependency_not_satisfied",
                        path=f"a2_3_handoff.dependencies[{index}].satisfied",
                        message="Dependência de handoff para A2.4 marcada como não satisfeita.",
                    )
                )

    handoff_change_id = str(handoff.get("change_id", "")).strip()
    handoff_run_id = str(handoff.get("run_id", "")).strip()
    handoff_manifest = str(handoff.get("manifest_fingerprint", "")).strip().lower()
    handoff_blueprint = str(handoff.get("source_blueprint_fingerprint", "")).strip().lower()

    if handoff_change_id != run.change_id:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_change_id_mismatch",
                path="a2_3_handoff.change_id",
                message="change_id do handoff A2.3 diverge do contexto do run atual.",
            )
        )
    if handoff_run_id != run.run_id:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_run_id_mismatch",
                path="a2_3_handoff.run_id",
                message="run_id do handoff A2.3 diverge do contexto do run atual.",
            )
        )
    if handoff_manifest != manifest_fingerprint:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_manifest_fingerprint_mismatch",
                path="a2_3_handoff.manifest_fingerprint",
                message="manifest_fingerprint do handoff A2.3 diverge do OrgRuntimeManifest validado.",
            )
        )
    if handoff_blueprint != source_blueprint_fingerprint:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_handoff_blueprint_fingerprint_mismatch",
                path="a2_3_handoff.source_blueprint_fingerprint",
                message="source_blueprint_fingerprint do handoff A2.3 diverge do blueprint do run atual.",
            )
        )

    return {
        "handoff_decision": decision,
        "change_id": handoff_change_id,
        "run_id": handoff_run_id,
        "manifest_fingerprint": handoff_manifest,
        "source_blueprint_fingerprint": handoff_blueprint,
    }


def _validate_a23_converged_inventory_for_org(
    *,
    converged_inventory: Any,
    target_org_id: str,
    issues: List[A2IncrementalTopologyEntryGateIssue],
) -> Dict[str, Any]:
    if not isinstance(converged_inventory, dict):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_a23_converged_inventory_missing",
                path="a2_3_converged_inventory",
                message="Gate A2.4 exige inventário convergido de runtime proveniente do A2.3.",
            )
        )
        return {"baseline_converged": False, "fingerprint": ""}

    normalized_target_org = str(target_org_id).strip().lower()
    rows = converged_inventory.get("chaincode_runtime_inventory")
    runtime_rows = rows if isinstance(rows, list) else []

    converged_for_org = False
    for index, row in enumerate(runtime_rows):
        if not isinstance(row, dict):
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_a23_runtime_row_invalid",
                    path=f"a2_3_converged_inventory.chaincode_runtime_inventory[{index}]",
                    message="Cada item de chaincode_runtime_inventory deve ser objeto.",
                )
            )
            continue
        org_id = str(row.get("org_id", "")).strip().lower()
        status = str(row.get("status", "")).strip().lower()
        if org_id == normalized_target_org and status == "running":
            converged_for_org = True

    if not converged_for_org:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_a23_baseline_not_converged_for_org",
                path="a2_3_converged_inventory.chaincode_runtime_inventory",
                message=(
                    "Baseline A2.3 não convergida para org alvo: nenhum runtime running encontrado "
                    f"para org_id='{normalized_target_org}'."
                ),
            )
        )

    return {
        "baseline_converged": converged_for_org,
        "fingerprint": _fingerprint_payload(converged_inventory),
    }


def _validate_topology_change_intent_contract(
    *,
    topology_change_intent: Any,
    target_org_id: str,
    report: Dict[str, Any],
    converged_inventory: Any,
    issues: List[A2IncrementalTopologyEntryGateIssue],
) -> Dict[str, Any]:
    def _normalize_domain(value: str) -> str:
        normalized = str(value).strip().lower()
        while ".." in normalized:
            normalized = normalized.replace("..", ".")
        return normalized.strip(".")

    def _expected_name(component_type: str, index: int, org_token: str, domain: str) -> str:
        return f"{component_type}{index}.{org_token}.{domain}"

    def _extract_name_index(
        *,
        name: str,
        component_type: str,
        org_token: str,
        domain: str,
    ) -> Optional[int]:
        pattern = re.compile(
            rf"^{re.escape(component_type)}(\d+)\.{re.escape(org_token)}\.{re.escape(domain)}$"
        )
        match = pattern.fullmatch(str(name).strip().lower())
        if match is None:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    def _existing_names_by_type() -> Dict[str, Set[str]]:
        result: Dict[str, Set[str]] = {component_type: set() for component_type in _A24_INCREMENTAL_COMPONENT_TYPES}

        components = report.get("normalized_components")
        if isinstance(components, list):
            for component in components:
                if not isinstance(component, dict):
                    continue
                component_type = str(component.get("component_type", "")).strip().lower()
                if component_type not in _A24_INCREMENTAL_COMPONENT_TYPES:
                    continue
                if not _component_active(component):
                    continue
                name = str(component.get("name", "")).strip().lower()
                if name:
                    result[component_type].add(name)

        if isinstance(converged_inventory, dict):
            hosts = converged_inventory.get("hosts")
            if isinstance(hosts, list):
                for host in hosts:
                    if not isinstance(host, dict):
                        continue
                    host_components = host.get("components")
                    if not isinstance(host_components, list):
                        continue
                    for component in host_components:
                        if not isinstance(component, dict):
                            continue
                        component_type = str(component.get("component_type", "")).strip().lower()
                        if component_type not in _A24_INCREMENTAL_COMPONENT_TYPES:
                            continue
                        status = str(component.get("status", "")).strip().lower()
                        if status and status != "running":
                            continue
                        name = str(component.get("name", "")).strip().lower()
                        if name:
                            result[component_type].add(name)
        return result

    if not isinstance(topology_change_intent, dict):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_not_object",
                path="topology_change_intent",
                message="Gate A2.4 exige topology_change_intent em formato objeto.",
            )
        )
        return {"fingerprint": "", "operations": []}

    intent_org_id = str(topology_change_intent.get("org_id", "")).strip().lower()
    if not intent_org_id:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_org_id_missing",
                path="topology_change_intent.org_id",
                message="topology_change_intent deve incluir org_id.",
            )
        )
    elif target_org_id and intent_org_id != target_org_id:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_org_id_mismatch",
                path="topology_change_intent.org_id",
                message="org_id do topology_change_intent diverge da organização alvo do manifesto.",
            )
        )

    raw_operations = topology_change_intent.get("operations")
    if not isinstance(raw_operations, list) or not raw_operations:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_operations_missing",
                path="topology_change_intent.operations",
                message="topology_change_intent deve declarar operations explícitas.",
            )
        )
        raw_operations = []

    normalized_operations: List[str] = []
    allowed_operations = {"add_peer", "add_orderer"}
    for index, operation in enumerate(raw_operations):
        if isinstance(operation, dict):
            operation_name = str(
                operation.get("operation")
                or operation.get("operation_type")
                or operation.get("type")
                or ""
            ).strip().lower()
        else:
            operation_name = str(operation).strip().lower()

        if not operation_name:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_topology_change_intent_operation_invalid",
                    path=f"topology_change_intent.operations[{index}]",
                    message="Cada operação incremental deve informar operation_type explícito.",
                )
            )
            continue
        if operation_name not in allowed_operations:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_topology_change_intent_operation_not_allowed",
                    path=f"topology_change_intent.operations[{index}]",
                    message=(
                        "Operação incremental fora do conjunto permitido "
                        "(add_peer, add_orderer)."
                    ),
                )
            )
            continue
        normalized_operations.append(operation_name)

    target_hosts = topology_change_intent.get("target_hosts")
    if not isinstance(target_hosts, list) or not target_hosts:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_target_hosts_missing",
                path="topology_change_intent.target_hosts",
                message="topology_change_intent deve incluir target_hosts não vazio.",
            )
        )
        target_hosts = []
    normalized_target_hosts = sorted(
        {
            str(host).strip()
            for host in target_hosts
            if str(host).strip()
        }
    )
    if not normalized_target_hosts:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_target_hosts_invalid",
                path="topology_change_intent.target_hosts",
                message="target_hosts deve conter ao menos um host_id válido.",
            )
        )

    requested_components = topology_change_intent.get("requested_components")
    if not isinstance(requested_components, list) or not requested_components:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_requested_components_missing",
                path="topology_change_intent.requested_components",
                message="topology_change_intent deve incluir requested_components não vazio.",
            )
        )
        requested_components = []

    destructive_actions = {"remove", "delete", "replace", "destroy", "stop"}
    org_token = str(target_org_id).strip().lower()
    domain = _normalize_domain(str(report.get("domain", "")).strip())
    existing_names = _existing_names_by_type()

    deactivated_component_names_raw = topology_change_intent.get("deactivated_component_names")
    deactivated_component_names = {
        str(item).strip().lower()
        for item in (deactivated_component_names_raw if isinstance(deactivated_component_names_raw, list) else [])
        if str(item).strip()
    }
    allow_reuse_deactivated_names = bool(topology_change_intent.get("allow_reuse_deactivated_names", False))

    component_default_ports: Dict[str, List[int]] = {
        "peer": [7051, 17051],
        "orderer": [7050, 9443, 17050],
    }
    component_default_resources: Dict[str, Dict[str, int]] = {
        "peer": {"cpu": 2, "memory_mb": 2048, "disk_gb": 30},
        "orderer": {"cpu": 2, "memory_mb": 2048, "disk_gb": 40},
    }

    def _normalize_ports(raw_ports: Any, *, component_type: str) -> List[int]:
        ports: List[int] = []
        if isinstance(raw_ports, list):
            for value in raw_ports:
                try:
                    parsed = int(str(value).strip())
                except (TypeError, ValueError):
                    continue
                if 1 <= parsed <= 65535:
                    ports.append(parsed)
        if ports:
            return sorted(set(ports))
        return list(component_default_ports.get(component_type, []))

    def _normalize_resources(raw_resources: Any, *, component_type: str) -> Dict[str, int]:
        base = dict(component_default_resources.get(component_type, {"cpu": 1, "memory_mb": 512, "disk_gb": 10}))
        if not isinstance(raw_resources, dict):
            return base
        for key in ("cpu", "memory_mb", "disk_gb"):
            try:
                parsed = int(str(raw_resources.get(key)).strip())
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                base[key] = parsed
        return base

    hosts_index: Dict[str, Dict[str, Any]] = {}
    for host in report.get("normalized_hosts", []):
        if not isinstance(host, dict):
            continue
        host_id = str(host.get("host_id", "")).strip()
        if host_id:
            hosts_index[host_id] = host

    host_validations: Dict[str, Dict[str, Any]] = {}
    raw_host_validations = topology_change_intent.get("host_validations")
    if isinstance(raw_host_validations, dict):
        for host_key, payload in raw_host_validations.items():
            host_id = str(host_key).strip()
            if host_id and isinstance(payload, dict):
                host_validations[host_id] = payload
    elif isinstance(raw_host_validations, list):
        for item in raw_host_validations:
            if not isinstance(item, dict):
                continue
            host_id = str(item.get("host_id", "")).strip()
            if host_id:
                host_validations[host_id] = item

    normalized_host_validations: Dict[str, Dict[str, Any]] = {}
    for host_id in normalized_target_hosts:
        host_validation = host_validations.get(host_id)
        if not isinstance(host_validation, dict):
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_target_host_validation_missing",
                    path="topology_change_intent.host_validations",
                    message=(
                        "Validação de capacidade/credencial ausente para host alvo "
                        f"'{host_id}'."
                    ),
                )
            )
            host_validation = {}

        available_raw = host_validation.get("host_available", host_validation.get("available"))
        credentials_raw = host_validation.get("credentials_valid")
        available = bool(available_raw) if isinstance(available_raw, bool) else False
        credentials_valid = bool(credentials_raw) if isinstance(credentials_raw, bool) else False

        if host_id not in hosts_index:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_target_host_not_found",
                    path="topology_change_intent.target_hosts",
                    message=f"Host alvo '{host_id}' não existe no OrgRuntimeManifest.",
                )
            )

        connection_profile_ref = ""
        if host_id in hosts_index:
            connection_profile_ref = str(hosts_index[host_id].get("connection_profile_ref", "")).strip()
            if not connection_profile_ref:
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_incremental_target_host_connection_profile_missing",
                        path=f"org_runtime_manifest_report.normalized_hosts[{host_id}].connection_profile_ref",
                        message=(
                            "Host alvo sem connection_profile_ref válido para execução incremental "
                            f"('{host_id}')."
                        ),
                    )
                )

        if not available:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_target_host_unavailable",
                    path=f"topology_change_intent.host_validations.{host_id}.host_available",
                    message=f"Host alvo '{host_id}' indisponível para expansão incremental.",
                )
            )
        if not credentials_valid:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_target_host_credentials_invalid",
                    path=f"topology_change_intent.host_validations.{host_id}.credentials_valid",
                    message=(
                        "Credencial inválida ou não verificada para host alvo "
                        f"'{host_id}'."
                    ),
                )
            )

        available_ports = _normalize_ports(host_validation.get("available_ports", []), component_type="peer")
        available_resources = _normalize_resources(host_validation.get("available_resources"), component_type="peer")
        normalized_host_validations[host_id] = {
            "host_available": available,
            "credentials_valid": credentials_valid,
            "available_ports": set(available_ports),
            "available_resources": dict(available_resources),
        }

    existing_ports_by_host: Dict[str, Set[int]] = {host_id: set() for host_id in hosts_index}
    existing_resource_usage_by_host: Dict[str, Dict[str, int]] = {
        host_id: {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
        for host_id in hosts_index
    }
    placement_load_by_host: Dict[str, Dict[str, int]] = {
        host_id: {"peer": 0, "orderer": 0, "total": 0}
        for host_id in hosts_index
    }
    for component in report.get("normalized_components", []):
        if not isinstance(component, dict) or not _component_active(component):
            continue
        host_id = str(component.get("host_id", "")).strip()
        if host_id not in hosts_index:
            continue
        component_type = str(component.get("component_type", "")).strip().lower()
        ports = _normalize_ports(component.get("ports", []), component_type=component_type)
        existing_ports_by_host.setdefault(host_id, set()).update(ports)
        resources = _normalize_resources(component.get("resources", {}), component_type=component_type)
        usage = existing_resource_usage_by_host.setdefault(host_id, {"cpu": 0, "memory_mb": 0, "disk_gb": 0})
        usage["cpu"] += int(resources.get("cpu", 0))
        usage["memory_mb"] += int(resources.get("memory_mb", 0))
        usage["disk_gb"] += int(resources.get("disk_gb", 0))
        if component_type in _A24_INCREMENTAL_COMPONENT_TYPES:
            load = placement_load_by_host.setdefault(host_id, {"peer": 0, "orderer": 0, "total": 0})
            load[component_type] += 1
            load["total"] += 1

    normalized_requested_components: List[Dict[str, Any]] = []
    allocations: List[Dict[str, str]] = []
    placement_decisions: List[Dict[str, Any]] = []
    reserved_names: Set[str] = set()
    reserved_ports_by_host: Dict[str, Set[int]] = {host_id: set() for host_id in hosts_index}
    reserved_resources_by_host: Dict[str, Dict[str, int]] = {
        host_id: {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
        for host_id in hosts_index
    }
    for index, component in enumerate(requested_components):
        if not isinstance(component, dict):
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_topology_change_intent_requested_component_invalid",
                    path=f"topology_change_intent.requested_components[{index}]",
                    message="Cada requested_component deve ser objeto.",
                )
            )
            continue

        component_type = str(component.get("component_type", "")).strip().lower()
        if component_type not in {"peer", "orderer"}:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_topology_change_intent_component_type_not_allowed",
                    path=f"topology_change_intent.requested_components[{index}].component_type",
                    message="requested_components aceita apenas component_type peer/orderer para A2.4.",
                )
            )

        mutation_action = str(
            component.get("action")
            or component.get("operation")
            or component.get("mutation")
            or "add"
        ).strip().lower()
        if mutation_action in destructive_actions:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_topology_change_intent_destructive_change_forbidden",
                    path=f"topology_change_intent.requested_components[{index}]",
                    message=(
                        "topology_change_intent não pode introduzir remoção implícita "
                        "ou alteração destrutiva de componentes convergidos."
                    ),
                )
            )

        requested_name = str(component.get("name", "")).strip().lower()
        host_id = str(component.get("host_id", "")).strip()

        normalized_requested_components.append(
            {
                "component_type": component_type,
                "name": requested_name,
                "host_id": host_id,
                "action": mutation_action,
                "ports": _normalize_ports(component.get("ports", []), component_type=component_type),
                "resources": _normalize_resources(component.get("resources", {}), component_type=component_type),
            }
        )

    sorted_requested = sorted(
        [
            item
            for item in normalized_requested_components
            if str(item.get("component_type", "")) in _A24_INCREMENTAL_COMPONENT_TYPES
            and str(item.get("action", "")) not in destructive_actions
        ],
        key=lambda item: (
            str(item.get("component_type", "")),
            str(item.get("host_id", "")),
            str(item.get("name", "")),
            str(item.get("action", "")),
            json.dumps(item.get("ports", []), ensure_ascii=False, sort_keys=True),
            json.dumps(item.get("resources", {}), ensure_ascii=False, sort_keys=True),
        ),
    )

    for allocation_index, requested in enumerate(sorted_requested):
        component_type = str(requested.get("component_type", "")).strip().lower()
        requested_name = str(requested.get("name", "")).strip().lower()
        host_id = str(requested.get("host_id", "")).strip()
        requested_ports = _normalize_ports(requested.get("ports", []), component_type=component_type)
        requested_resources = _normalize_resources(requested.get("resources", {}), component_type=component_type)

        if not org_token or not domain:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_naming_context_missing",
                    path="org_runtime_manifest_report.org_id|domain",
                    message=(
                        "Não foi possível calcular naming incremental determinístico: "
                        "org_id/domain ausentes no contexto do manifesto."
                    ),
                )
            )
            continue

        base_used_names = set(existing_names.get(component_type, set())) | set(reserved_names)

        base_occupied_indexes = {
            value
            for value in (
                _extract_name_index(
                    name=name,
                    component_type=component_type,
                    org_token=org_token,
                    domain=domain,
                )
                for name in base_used_names
            )
            if value is not None
        }
        base_next_free_index = 0
        while base_next_free_index in base_occupied_indexes:
            base_next_free_index += 1
        base_expected_name = _expected_name(component_type, base_next_free_index, org_token, domain)

        if (
            not requested_name
            and not allow_reuse_deactivated_names
            and base_expected_name in deactivated_component_names
        ):
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_component_name_reuse_without_policy",
                    path=f"topology_change_intent.requested_components[{allocation_index}].name",
                    message=(
                        "Naming incremental ambíguo: próximo índice livre recai sobre nome "
                        f"desativado '{base_expected_name}' sem política explícita de reuso."
                    ),
                )
            )
            continue

        used_names = set(base_used_names)
        if not allow_reuse_deactivated_names:
            used_names.update(deactivated_component_names)

        occupied_indexes = {
            value
            for value in (
                _extract_name_index(
                    name=name,
                    component_type=component_type,
                    org_token=org_token,
                    domain=domain,
                )
                for name in used_names
            )
            if value is not None
        }
        next_free_index = 0
        while next_free_index in occupied_indexes:
            next_free_index += 1
        expected_name = _expected_name(component_type, next_free_index, org_token, domain)

        if requested_name:
            requested_index = _extract_name_index(
                name=requested_name,
                component_type=component_type,
                org_token=org_token,
                domain=domain,
            )
            if requested_index is None:
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_incremental_component_name_invalid",
                        path=f"topology_change_intent.requested_components[{allocation_index}].name",
                        message=(
                            "Nome incremental inválido para componente "
                            f"'{component_type}'; esperado padrão '{component_type}{{n}}.{org_token}.{domain}'."
                        ),
                    )
                )
                continue
            chosen_name = requested_name
            chosen_index = requested_index
            if chosen_name in used_names:
                code = "a2_4_incremental_component_name_collision"
                if chosen_name in deactivated_component_names and not allow_reuse_deactivated_names:
                    code = "a2_4_incremental_component_name_reuse_without_policy"
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code=code,
                        path=f"topology_change_intent.requested_components[{allocation_index}].name",
                        message=(
                            "Colisão/reuso ambíguo de naming incremental para componente "
                            f"'{component_type}' com nome '{chosen_name}'."
                        ),
                    )
                )
                continue
            if chosen_name != expected_name:
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_incremental_component_name_not_next_free",
                        path=f"topology_change_intent.requested_components[{allocation_index}].name",
                        message=(
                            "Naming incremental deve usar próximo índice livre. "
                            f"Esperado '{expected_name}', recebido '{chosen_name}'."
                        ),
                    )
                )
                continue
        else:
            chosen_name = expected_name
            chosen_index = next_free_index

        candidate_hosts = [host_id] if host_id else list(normalized_target_hosts)
        if host_id and host_id not in normalized_target_hosts:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_component_host_outside_target_hosts",
                    path=f"topology_change_intent.requested_components[{allocation_index}].host_id",
                    message=(
                        f"Host '{host_id}' fora do escopo target_hosts para componente incremental "
                        f"'{component_type}'."
                    ),
                )
            )
            continue

        eligible_hosts: List[Tuple[int, int, str]] = []
        for candidate_host in sorted({value for value in candidate_hosts if str(value).strip()}):
            host_validation = normalized_host_validations.get(candidate_host, {})
            if candidate_host not in hosts_index:
                continue
            if not bool(host_validation.get("host_available", False)):
                continue
            if not bool(host_validation.get("credentials_valid", False)):
                continue

            used_ports = set(existing_ports_by_host.get(candidate_host, set())) | set(
                reserved_ports_by_host.get(candidate_host, set())
            )
            conflicting_ports = sorted(set(requested_ports) & used_ports)
            if conflicting_ports:
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_incremental_host_port_conflict",
                        path=f"topology_change_intent.requested_components[{allocation_index}].ports",
                        message=(
                            f"Conflito de portas crítico no host '{candidate_host}' para componente "
                            f"'{component_type}': {conflicting_ports}."
                        ),
                    )
                )
                continue

            available_ports = set(host_validation.get("available_ports", set()))
            if available_ports and not set(requested_ports).issubset(available_ports):
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_incremental_host_port_unavailable",
                        path=f"topology_change_intent.requested_components[{allocation_index}].ports",
                        message=(
                            f"Portas requeridas não disponíveis no host '{candidate_host}' para "
                            f"'{component_type}'."
                        ),
                    )
                )
                continue

            available_resources = _normalize_resources(
                host_validation.get("available_resources", {}),
                component_type=component_type,
            )
            current_usage = existing_resource_usage_by_host.get(
                candidate_host,
                {"cpu": 0, "memory_mb": 0, "disk_gb": 0},
            )
            reserved_usage = reserved_resources_by_host.get(
                candidate_host,
                {"cpu": 0, "memory_mb": 0, "disk_gb": 0},
            )
            projected_usage = {
                key: int(current_usage.get(key, 0)) + int(reserved_usage.get(key, 0)) + int(requested_resources.get(key, 0))
                for key in ("cpu", "memory_mb", "disk_gb")
            }
            if any(projected_usage[key] > int(available_resources.get(key, 0)) for key in ("cpu", "memory_mb", "disk_gb")):
                issues.append(
                    A2IncrementalTopologyEntryGateIssue(
                        level="error",
                        code="a2_4_incremental_host_capacity_insufficient",
                        path=f"topology_change_intent.requested_components[{allocation_index}].resources",
                        message=(
                            f"Capacidade insuficiente no host '{candidate_host}' para componente "
                            f"'{component_type}'."
                        ),
                    )
                )
                continue

            load = placement_load_by_host.get(candidate_host, {"peer": 0, "orderer": 0, "total": 0})
            eligible_hosts.append((int(load.get(component_type, 0)), int(load.get("total", 0)), candidate_host))

        if not eligible_hosts:
            issues.append(
                A2IncrementalTopologyEntryGateIssue(
                    level="error",
                    code="a2_4_incremental_placement_no_eligible_host",
                    path=f"topology_change_intent.requested_components[{allocation_index}].host_id",
                    message=(
                        "Nenhum host elegível para placement incremental após validar disponibilidade, "
                        "credencial, portas e capacidade."
                    ),
                )
            )
            continue

        eligible_hosts.sort(key=lambda item: (item[0], item[1], item[2]))
        selected_host = eligible_hosts[0][2]
        reason = "requested_host_respected" if host_id else "least_loaded_distribution_policy"
        constraints_applied = [
            "target_hosts_scope",
            "connection_profile_ref_valid",
            "host_available",
            "credentials_valid",
            "ports_available",
            "resources_available",
            "baseline_preserved",
        ]
        if not host_id:
            constraints_applied.append("deterministic_least_loaded")

        reserved_names.add(chosen_name)
        reserved_ports_by_host.setdefault(selected_host, set()).update(requested_ports)
        reserved_usage = reserved_resources_by_host.setdefault(
            selected_host,
            {"cpu": 0, "memory_mb": 0, "disk_gb": 0},
        )
        reserved_usage["cpu"] += int(requested_resources.get("cpu", 0))
        reserved_usage["memory_mb"] += int(requested_resources.get("memory_mb", 0))
        reserved_usage["disk_gb"] += int(requested_resources.get("disk_gb", 0))

        load = placement_load_by_host.setdefault(selected_host, {"peer": 0, "orderer": 0, "total": 0})
        load[component_type] += 1
        load["total"] += 1

        allocations.append(
            {
                "component_type": component_type,
                "host_id": selected_host,
                "allocated_name": chosen_name,
                "allocated_index": str(chosen_index),
            }
        )
        placement_decisions.append(
            {
                "component_type": component_type,
                "allocated_name": chosen_name,
                "host_id": selected_host,
                "reason": reason,
                "constraints_applied": sorted(set(constraints_applied)),
            }
        )

    if bool(topology_change_intent.get("introduces_removal", False)) or bool(
        topology_change_intent.get("introduces_destructive_change", False)
    ):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_destructive_change_forbidden",
                path="topology_change_intent",
                message=(
                    "topology_change_intent não pode introduzir remoção implícita "
                    "ou alteração destrutiva de componentes convergidos."
                ),
            )
        )

    change_reason = str(topology_change_intent.get("change_reason", "")).strip()
    if not change_reason:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_change_reason_missing",
                path="topology_change_intent.change_reason",
                message="topology_change_intent deve informar change_reason.",
            )
        )

    dependencies = topology_change_intent.get("dependencies")
    if not isinstance(dependencies, dict):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_dependencies_missing",
                path="topology_change_intent.dependencies",
                message=(
                    "topology_change_intent deve registrar dependências de execução "
                    "(requires_channel_updates, requires_orderer_consenters_update)."
                ),
            )
        )
        dependencies = {}

    requires_channel_updates = dependencies.get("requires_channel_updates")
    requires_orderer_consenters_update = dependencies.get("requires_orderer_consenters_update")
    if not isinstance(requires_channel_updates, bool):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_dependency_invalid",
                path="topology_change_intent.dependencies.requires_channel_updates",
                message="requires_channel_updates deve ser booleano.",
            )
        )
        requires_channel_updates = False
    if not isinstance(requires_orderer_consenters_update, bool):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_topology_change_intent_dependency_invalid",
                path="topology_change_intent.dependencies.requires_orderer_consenters_update",
                message="requires_orderer_consenters_update deve ser booleano.",
            )
        )
        requires_orderer_consenters_update = False

    normalized_payload = {
        "org_id": intent_org_id,
        "operations": sorted(set(normalized_operations)),
        "target_hosts": normalized_target_hosts,
        "requested_components": sorted(
            normalized_requested_components,
            key=lambda item: (
                str(item.get("component_type", "")),
                str(item.get("host_id", "")),
                str(item.get("name", "")),
                str(item.get("action", "")),
            ),
        ),
        "change_reason": change_reason,
        "dependencies": {
            "requires_channel_updates": bool(requires_channel_updates),
            "requires_orderer_consenters_update": bool(requires_orderer_consenters_update),
        },
        "incremental_allocations": sorted(
            allocations,
            key=lambda item: (
                str(item.get("component_type", "")),
                int(str(item.get("allocated_index", "0")) if str(item.get("allocated_index", "")).isdigit() else 0),
                str(item.get("allocated_name", "")),
                str(item.get("host_id", "")),
            ),
        ),
        "placement_decisions": sorted(
            placement_decisions,
            key=lambda item: (
                str(item.get("component_type", "")),
                str(item.get("allocated_name", "")),
                str(item.get("host_id", "")),
            ),
        ),
    }
    return {
        "fingerprint": _fingerprint_payload(normalized_payload),
        "operations": list(normalized_payload["operations"]),
        "allocations": [dict(item) for item in normalized_payload["incremental_allocations"]],
        "allocation_fingerprint": _fingerprint_payload(normalized_payload["incremental_allocations"]),
        "placement_decisions": [dict(item) for item in normalized_payload["placement_decisions"]],
        "placement_fingerprint": _fingerprint_payload(normalized_payload["placement_decisions"]),
        "normalized": normalized_payload,
    }


def _validate_chaincode_runtime_contract(
    report: Dict[str, Any],
    issues: List[A2ChaincodeRuntimeEntryGateIssue],
) -> int:
    runtimes = report.get("normalized_chaincode_runtimes")
    if not isinstance(runtimes, list) or not runtimes:
        issues.append(
            A2ChaincodeRuntimeEntryGateIssue(
                level="error",
                code="a2_3_manifest_chaincode_runtimes_missing",
                path="normalized_chaincode_runtimes",
                message=(
                    "Gate A2.3 exige normalized_chaincode_runtimes não vazio no relatório do OrgRuntimeManifest."
                ),
            )
        )
        return 0

    required_fields = (
        "channel_id",
        "chaincode_id",
        "version",
        "target_peer",
        "package_id_hash",
        "runtime_name",
    )
    valid_count = 0
    for index, runtime in enumerate(runtimes):
        if not isinstance(runtime, dict):
            issues.append(
                A2ChaincodeRuntimeEntryGateIssue(
                    level="error",
                    code="a2_3_manifest_chaincode_runtime_invalid",
                    path=f"normalized_chaincode_runtimes[{index}]",
                    message="Cada runtime normalizado deve ser objeto.",
                )
            )
            continue
        missing = [field for field in required_fields if not str(runtime.get(field, "")).strip()]
        if missing:
            issues.append(
                A2ChaincodeRuntimeEntryGateIssue(
                    level="error",
                    code="a2_3_manifest_chaincode_runtime_missing_field",
                    path=f"normalized_chaincode_runtimes[{index}]",
                    message=(
                        "Runtime normalizado inválido: campos obrigatórios ausentes "
                        f"{missing}."
                    ),
                )
            )
            continue
        valid_count += 1
    return valid_count


def _validate_a22_converged_inventory(
    *,
    converged_inventory: Any,
    report: Dict[str, Any],
    issues: List[A2ChaincodeRuntimeEntryGateIssue],
) -> Dict[str, Any]:
    if not isinstance(converged_inventory, dict):
        issues.append(
            A2ChaincodeRuntimeEntryGateIssue(
                level="error",
                code="a2_3_previous_inventory_missing",
                path="a2_2_converged_inventory",
                message=(
                    "Gate A2.3 exige inventário convergido da execução anterior (A2.2) para validar baseline."
                ),
            )
        )
        return {"baseline_converged": False, "fingerprint": "", "observed": {}}

    hosts = converged_inventory.get("hosts")
    if not isinstance(hosts, list) or not hosts:
        issues.append(
            A2ChaincodeRuntimeEntryGateIssue(
                level="error",
                code="a2_3_previous_inventory_hosts_missing",
                path="a2_2_converged_inventory.hosts",
                message="Inventário convergido anterior deve incluir hosts não vazios.",
            )
        )
        return {
            "baseline_converged": False,
            "fingerprint": _fingerprint_payload(converged_inventory),
            "observed": {},
        }

    observed: Dict[str, Set[str]] = {component_type: set() for component_type in _A22_REQUIRED_COMPONENT_TYPES}
    for host_index, host in enumerate(hosts):
        if not isinstance(host, dict):
            issues.append(
                A2ChaincodeRuntimeEntryGateIssue(
                    level="error",
                    code="a2_3_previous_inventory_host_invalid",
                    path=f"a2_2_converged_inventory.hosts[{host_index}]",
                    message="Cada host do inventário convergido deve ser objeto.",
                )
            )
            continue
        components = host.get("components")
        if not isinstance(components, list):
            continue
        for component in components:
            if not isinstance(component, dict):
                continue
            component_type = str(component.get("component_type", "")).strip().lower()
            if component_type not in _A22_REQUIRED_COMPONENT_TYPES:
                continue
            status = str(component.get("status", "")).strip().lower()
            if status != "running":
                continue
            key = str(component.get("name", "")).strip() or str(component.get("component_id", "")).strip()
            if key:
                observed[component_type].add(key)

    baseline_converged = True
    for component_type in sorted(_A22_REQUIRED_COMPONENT_TYPES):
        if not observed.get(component_type):
            baseline_converged = False
            issues.append(
                A2ChaincodeRuntimeEntryGateIssue(
                    level="error",
                    code="a2_3_a22_baseline_component_not_converged",
                    path=f"a2_2_converged_inventory.components[{component_type}]",
                    message=(
                        "Baseline A2.2 não convergida para componente mandatório "
                        f"'{component_type}' (status running não encontrado)."
                    ),
                )
            )

    expected = _manifest_required_component_keys(report)
    for component_type in sorted(_A22_REQUIRED_COMPONENT_TYPES):
        expected_keys = expected.get(component_type, set())
        if not expected_keys:
            continue
        missing = sorted(expected_keys - observed.get(component_type, set()))
        if missing:
            baseline_converged = False
            issues.append(
                A2ChaincodeRuntimeEntryGateIssue(
                    level="error",
                    code="a2_3_manifest_inventory_divergence",
                    path=f"a2_2_converged_inventory.components[{component_type}]",
                    message=(
                        "Inventário convergido anterior diverge do manifesto para componente "
                        f"'{component_type}'; itens ausentes: {missing}."
                    ),
                )
            )

    return {
        "baseline_converged": baseline_converged,
        "fingerprint": _fingerprint_payload(converged_inventory),
        "observed": observed,
    }


def _required_string(
    report: Dict[str, Any],
    field_name: str,
    *,
    path: str,
    issues: List[A2ProvisionEntryGateIssue],
) -> str:
    value = str(report.get(field_name, "")).strip()
    if not value:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_missing_required_field",
                path=path,
                message=f"Campo obrigatório ausente no relatório do OrgRuntimeManifest: '{field_name}'.",
            )
        )
    return value


def _normalize_dict(report: Any, issues: List[A2ProvisionEntryGateIssue]) -> Dict[str, Any]:
    if isinstance(report, dict):
        return dict(report)
    issues.append(
        A2ProvisionEntryGateIssue(
            level="error",
            code="a2_2_manifest_report_not_object",
            path="org_runtime_manifest_report",
            message="Relatório do OrgRuntimeManifest deve ser objeto.",
        )
    )
    return {}


def _validate_minimum_runtime_contract(
    report: Dict[str, Any],
    issues: List[A2ProvisionEntryGateIssue],
) -> None:
    hosts = report.get("normalized_hosts")
    components = report.get("normalized_components")

    if not isinstance(hosts, list) or not hosts:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_hosts_missing",
                path="normalized_hosts",
                message="Gate A2.2 requer normalized_hosts não vazio no relatório do manifesto.",
            )
        )
        hosts = []
    if not isinstance(components, list) or not components:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_components_missing",
                path="normalized_components",
                message="Gate A2.2 requer normalized_components não vazio no relatório do manifesto.",
            )
        )
        components = []

    host_ids: Set[str] = set()
    for index, host in enumerate(hosts):
        if not isinstance(host, dict):
            issues.append(
                A2ProvisionEntryGateIssue(
                    level="error",
                    code="a2_2_manifest_host_invalid",
                    path=f"normalized_hosts[{index}]",
                    message="Cada host normalizado deve ser objeto.",
                )
            )
            continue
        host_id = str(host.get("host_id", "")).strip()
        if not host_id:
            issues.append(
                A2ProvisionEntryGateIssue(
                    level="error",
                    code="a2_2_manifest_host_missing_host_id",
                    path=f"normalized_hosts[{index}].host_id",
                    message="host_id é obrigatório em normalized_hosts.",
                )
            )
            continue
        host_ids.add(host_id)

    for index, component in enumerate(components):
        if not isinstance(component, dict):
            issues.append(
                A2ProvisionEntryGateIssue(
                    level="error",
                    code="a2_2_manifest_component_invalid",
                    path=f"normalized_components[{index}]",
                    message="Cada componente normalizado deve ser objeto.",
                )
            )
            continue
        host_id = str(component.get("host_id", "")).strip()
        if not host_id:
            issues.append(
                A2ProvisionEntryGateIssue(
                    level="error",
                    code="a2_2_manifest_component_missing_host_id",
                    path=f"normalized_components[{index}].host_id",
                    message="host_id é obrigatório em normalized_components.",
                )
            )
            continue
        if host_id not in host_ids:
            issues.append(
                A2ProvisionEntryGateIssue(
                    level="error",
                    code="a2_2_manifest_component_host_not_found",
                    path=f"normalized_components[{index}].host_id",
                    message=(
                        f"Componente referencia host_id '{host_id}' inexistente em normalized_hosts."
                    ),
                )
            )


def evaluate_a2_provision_entry_gate(
    *,
    run: PipelineRun,
    org_runtime_manifest_report: Any,
    manifest_state_store: Optional[OrgRuntimeManifestStateStore],
    require_persistence: bool = True,
) -> A2ProvisionEntryGateResult:
    issues: List[A2ProvisionEntryGateIssue] = []
    report = _normalize_dict(org_runtime_manifest_report, issues)

    if not bool(report.get("valid", False)):
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_not_valid",
                path="valid",
                message="Gate A2.2 exige OrgRuntimeManifest validado com valid=true no WP A2.1.",
            )
        )

    change_id = _required_string(
        report,
        "change_id",
        path="change_id",
        issues=issues,
    )
    run_id = _required_string(
        report,
        "run_id",
        path="run_id",
        issues=issues,
    )
    org_id = _required_string(
        report,
        "org_id",
        path="org_id",
        issues=issues,
    )
    environment_profile_ref = _required_string(
        report,
        "environment_profile_ref",
        path="environment_profile_ref",
        issues=issues,
    )
    source_blueprint_fingerprint = _required_string(
        report,
        "source_blueprint_fingerprint",
        path="source_blueprint_fingerprint",
        issues=issues,
    ).lower()

    manifest_fingerprint = str(report.get("manifest_fingerprint", "")).strip().lower()
    fingerprint_sha256 = str(report.get("fingerprint_sha256", "")).strip().lower()
    if not _is_sha256(manifest_fingerprint):
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_fingerprint_invalid",
                path="manifest_fingerprint",
                message="manifest_fingerprint deve ser SHA-256 canônico (64 hex minúsculo).",
            )
        )
    if fingerprint_sha256 != manifest_fingerprint:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_fingerprint_mismatch",
                path="fingerprint_sha256",
                message="fingerprint_sha256 diverge de manifest_fingerprint no relatório do manifesto.",
            )
        )

    if change_id and change_id != run.change_id:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_change_id_mismatch",
                path="change_id",
                message=(
                    f"change_id do manifesto ('{change_id}') diverge do contexto da execução ('{run.change_id}')."
                ),
            )
        )
    if run_id and run_id != run.run_id:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_run_id_mismatch",
                path="run_id",
                message=f"run_id do manifesto ('{run_id}') diverge do contexto da execução ('{run.run_id}').",
            )
        )
    if source_blueprint_fingerprint and source_blueprint_fingerprint != run.blueprint_fingerprint:
        issues.append(
            A2ProvisionEntryGateIssue(
                level="error",
                code="a2_2_manifest_blueprint_fingerprint_mismatch",
                path="source_blueprint_fingerprint",
                message=(
                    "source_blueprint_fingerprint do manifesto diverge do blueprint aplicado no run atual."
                ),
            )
        )

    _validate_minimum_runtime_contract(report, issues)

    persisted = False
    persisted_manifest_path = ""
    if require_persistence:
        if manifest_state_store is None:
            issues.append(
                A2ProvisionEntryGateIssue(
                    level="error",
                    code="a2_2_manifest_persistence_store_missing",
                    path="manifest_state_store",
                    message="Gate A2.2 exige OrgRuntimeManifestStateStore para validar persistência.",
                )
            )
        elif change_id and run_id and _is_sha256(manifest_fingerprint):
            try:
                persisted_manifest_path = str(
                    manifest_state_store.manifest_version_path(
                        change_id=change_id,
                        run_id=run_id,
                        manifest_fingerprint=manifest_fingerprint,
                    )
                )
                persisted_payload = manifest_state_store.load_manifest_version(
                    change_id=change_id,
                    run_id=run_id,
                    manifest_fingerprint=manifest_fingerprint,
                )
            except ValueError as error:
                persisted_payload = None
                issues.append(
                    A2ProvisionEntryGateIssue(
                        level="error",
                        code="a2_2_manifest_persistence_lookup_invalid",
                        path="manifest_state_store.lookup",
                        message=f"Falha ao resolver persistência do manifesto: {error}",
                    )
                )

            if not isinstance(persisted_payload, dict):
                issues.append(
                    A2ProvisionEntryGateIssue(
                        level="error",
                        code="a2_2_manifest_not_persisted",
                        path="manifest_state_store.version",
                        message=(
                            "Manifesto não encontrado no state store para "
                            f"change_id='{change_id}', run_id='{run_id}', fingerprint='{manifest_fingerprint}'."
                        ),
                    )
                )
            else:
                persisted = True
                persisted_change_id = str(persisted_payload.get("change_id", "")).strip()
                persisted_run_id = str(persisted_payload.get("run_id", "")).strip()
                persisted_fingerprint = str(
                    persisted_payload.get("manifest_fingerprint", "")
                ).strip().lower()
                persisted_blueprint_fingerprint = str(
                    persisted_payload.get("source_blueprint_fingerprint", "")
                ).strip().lower()

                if (
                    persisted_change_id != change_id
                    or persisted_run_id != run_id
                    or persisted_fingerprint != manifest_fingerprint
                    or persisted_blueprint_fingerprint != source_blueprint_fingerprint
                ):
                    issues.append(
                        A2ProvisionEntryGateIssue(
                            level="error",
                            code="a2_2_persisted_manifest_mismatch",
                            path="manifest_state_store.version",
                            message=(
                                "Registro persistido do manifesto diverge dos metadados/correlação esperados "
                                "do relatório validado."
                            ),
                        )
                    )

    issues = _sort_issues(issues)
    blocked = any(issue.level == "error" for issue in issues)
    return A2ProvisionEntryGateResult(
        allowed=not blocked,
        blocked=blocked,
        change_id=change_id,
        run_id=run_id,
        org_id=org_id,
        environment_profile_ref=environment_profile_ref,
        source_blueprint_fingerprint=source_blueprint_fingerprint,
        manifest_fingerprint=manifest_fingerprint,
        persisted=persisted,
        persisted_manifest_path=persisted_manifest_path,
        issues=issues,
    )


def evaluate_a2_chaincode_runtime_entry_gate(
    *,
    run: PipelineRun,
    org_runtime_manifest_report: Any,
    manifest_state_store: Optional[OrgRuntimeManifestStateStore],
    a2_2_converged_inventory: Any,
    require_persistence: bool = True,
) -> A2ChaincodeRuntimeEntryGateResult:
    base_gate = evaluate_a2_provision_entry_gate(
        run=run,
        org_runtime_manifest_report=org_runtime_manifest_report,
        manifest_state_store=manifest_state_store,
        require_persistence=require_persistence,
    )
    report = dict(org_runtime_manifest_report) if isinstance(org_runtime_manifest_report, dict) else {}

    runtime_issues: List[A2ChaincodeRuntimeEntryGateIssue] = [
        A2ChaincodeRuntimeEntryGateIssue(
            level=issue.level,
            code=issue.code,
            path=issue.path,
            message=issue.message,
        )
        for issue in base_gate.issues
    ]

    runtime_count = _validate_chaincode_runtime_contract(report, runtime_issues)
    baseline_payload = _validate_a22_converged_inventory(
        converged_inventory=a2_2_converged_inventory,
        report=report,
        issues=runtime_issues,
    )

    runtime_issues = _sort_runtime_issues(runtime_issues)
    blocked = any(issue.level == "error" for issue in runtime_issues)

    return A2ChaincodeRuntimeEntryGateResult(
        allowed=not blocked,
        blocked=blocked,
        change_id=base_gate.change_id,
        run_id=base_gate.run_id,
        org_id=base_gate.org_id,
        source_blueprint_fingerprint=base_gate.source_blueprint_fingerprint,
        manifest_fingerprint=base_gate.manifest_fingerprint,
        manifest_persisted=base_gate.persisted,
        persisted_manifest_path=base_gate.persisted_manifest_path,
        baseline_converged=bool(baseline_payload.get("baseline_converged", False)),
        converged_inventory_fingerprint=str(baseline_payload.get("fingerprint", "")).strip().lower(),
        chaincode_runtime_count=runtime_count,
        issues=runtime_issues,
    )


def evaluate_a2_incremental_topology_entry_gate(
    *,
    run: PipelineRun,
    org_runtime_manifest_report: Any,
    manifest_state_store: Optional[OrgRuntimeManifestStateStore],
    a2_3_handoff: Any,
    a2_3_readiness_checklist: Any,
    a2_3_converged_inventory: Any,
    topology_change_intent: Any = None,
    require_persistence: bool = True,
) -> A2IncrementalTopologyEntryGateResult:
    base_gate = evaluate_a2_provision_entry_gate(
        run=run,
        org_runtime_manifest_report=org_runtime_manifest_report,
        manifest_state_store=manifest_state_store,
        require_persistence=require_persistence,
    )

    report = dict(org_runtime_manifest_report) if isinstance(org_runtime_manifest_report, dict) else {}
    issues: List[A2IncrementalTopologyEntryGateIssue] = _normalize_incremental_issues(base_gate.issues)

    checklist = _extract_a23_checklist_payload(a2_3_readiness_checklist, issues)
    readiness_ready = bool(checklist.get("ready_for_a2_4_a2_5", False))

    handoff_payload = _validate_a23_handoff_payload(
        handoff=a2_3_handoff,
        run=run,
        manifest_fingerprint=base_gate.manifest_fingerprint,
        source_blueprint_fingerprint=base_gate.source_blueprint_fingerprint,
        issues=issues,
    )

    readiness_payload = a2_3_readiness_checklist if isinstance(a2_3_readiness_checklist, dict) else {}
    readiness_change_id = str(readiness_payload.get("change_id", "")).strip()
    if readiness_change_id and readiness_change_id != run.change_id:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_readiness_change_id_mismatch",
                path="a2_3_readiness_checklist.change_id",
                message="change_id do checklist de prontidão A2.3 diverge do contexto do run atual.",
            )
        )

    manifest_run_id = str(report.get("run_id", "")).strip()
    manifest_change_id = str(report.get("change_id", "")).strip()
    manifest_blueprint = str(report.get("source_blueprint_fingerprint", "")).strip().lower()
    if manifest_run_id and manifest_run_id != handoff_payload.get("run_id", ""):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_correlation_run_id_mismatch",
                path="run_id",
                message="Correlação inválida entre manifesto e handoff A2.3 (run_id divergente).",
            )
        )
    if manifest_change_id and manifest_change_id != handoff_payload.get("change_id", ""):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_correlation_change_id_mismatch",
                path="change_id",
                message="Correlação inválida entre manifesto e handoff A2.3 (change_id divergente).",
            )
        )
    if base_gate.manifest_fingerprint and base_gate.manifest_fingerprint != handoff_payload.get("manifest_fingerprint", ""):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_correlation_manifest_fingerprint_mismatch",
                path="manifest_fingerprint",
                message="Correlação inválida entre manifesto e handoff A2.3 (manifest_fingerprint divergente).",
            )
        )
    if manifest_blueprint and manifest_blueprint != handoff_payload.get("source_blueprint_fingerprint", ""):
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_correlation_source_blueprint_fingerprint_mismatch",
                path="source_blueprint_fingerprint",
                message=(
                    "Correlação inválida entre manifesto e handoff A2.3 "
                    "(source_blueprint_fingerprint divergente)."
                ),
            )
        )

    target_org_id = str(report.get("org_id", "")).strip().lower()
    if not target_org_id:
        issues.append(
            A2IncrementalTopologyEntryGateIssue(
                level="error",
                code="a2_4_target_org_missing",
                path="org_runtime_manifest_report.org_id",
                message="Gate A2.4 exige org_id no OrgRuntimeManifest para validar baseline de org alvo.",
            )
        )

    baseline_payload = _validate_a23_converged_inventory_for_org(
        converged_inventory=a2_3_converged_inventory,
        target_org_id=target_org_id,
        issues=issues,
    )

    topology_change_payload = _validate_topology_change_intent_contract(
        topology_change_intent=topology_change_intent,
        target_org_id=target_org_id,
        report=report,
        converged_inventory=a2_3_converged_inventory,
        issues=issues,
    )

    issues = _sort_incremental_issues(issues)
    blocked = any(issue.level == "error" for issue in issues)
    return A2IncrementalTopologyEntryGateResult(
        allowed=not blocked,
        blocked=blocked,
        change_id=base_gate.change_id,
        run_id=base_gate.run_id,
        org_id=target_org_id,
        source_blueprint_fingerprint=base_gate.source_blueprint_fingerprint,
        manifest_fingerprint=base_gate.manifest_fingerprint,
        handoff_decision=handoff_payload.get("handoff_decision", ""),
        topology_change_intent_fingerprint=str(topology_change_payload.get("fingerprint", "")).strip().lower(),
        topology_change_operations=[
            str(item).strip().lower()
            for item in topology_change_payload.get("operations", [])
            if str(item).strip()
        ],
        incremental_component_allocations=[
            {
                "component_type": str(item.get("component_type", "")).strip().lower(),
                "host_id": str(item.get("host_id", "")).strip(),
                "allocated_name": str(item.get("allocated_name", "")).strip().lower(),
                "allocated_index": str(item.get("allocated_index", "")).strip(),
            }
            for item in topology_change_payload.get("allocations", [])
            if isinstance(item, dict)
        ],
        incremental_allocation_fingerprint=str(
            topology_change_payload.get("allocation_fingerprint", "")
        ).strip().lower(),
        incremental_placement_decisions=[
            {
                "component_type": str(item.get("component_type", "")).strip().lower(),
                "allocated_name": str(item.get("allocated_name", "")).strip().lower(),
                "host_id": str(item.get("host_id", "")).strip(),
                "reason": str(item.get("reason", "")).strip(),
                "constraints_applied": [
                    str(constraint).strip()
                    for constraint in item.get("constraints_applied", [])
                    if str(constraint).strip()
                ],
            }
            for item in topology_change_payload.get("placement_decisions", [])
            if isinstance(item, dict)
        ],
        incremental_placement_fingerprint=str(
            topology_change_payload.get("placement_fingerprint", "")
        ).strip().lower(),
        readiness_ready_for_a2_4_a2_5=readiness_ready,
        baseline_converged_for_org=bool(baseline_payload.get("baseline_converged", False)),
        converged_inventory_fingerprint=str(baseline_payload.get("fingerprint", "")).strip().lower(),
        issues=issues,
    )

