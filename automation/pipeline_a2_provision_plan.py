from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .pipeline_contract import PipelineRun, utc_now_iso
from .pipeline_state_store import payload_sha256


ALLOWED_PROVISION_ACTIONS = {"create", "update", "start", "noop", "verify"}
_RUNNING_STATUSES = {"running", "up", "healthy"}
_STOPPED_STATUSES = {"created", "exited", "stopped", "dead", "paused"}


def _sorted_unique_ports(values: Any) -> List[int]:
    if not isinstance(values, (list, tuple, set)):
        return []
    ports = set()
    for value in values:
        try:
            port = int(value)
        except (TypeError, ValueError):
            continue
        if port > 0:
            ports.add(port)
    return sorted(ports)


def _normalize_resources(raw: Any) -> Dict[str, int]:
    if not isinstance(raw, dict):
        return {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
    normalized = {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
    for key in ("cpu", "memory_mb", "disk_gb"):
        try:
            parsed = int(raw.get(key, 0))
        except (TypeError, ValueError):
            parsed = 0
        normalized[key] = parsed if parsed > 0 else 0
    return normalized


def _normalize_service_context(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    org_id = str(raw.get("org_id", "")).strip().lower()
    channel_ids = sorted(
        {
            str(item).strip().lower()
            for item in (raw.get("channel_ids") or [])
            if str(item).strip()
        }
    )
    chaincode_ids = sorted(
        {
            str(item).strip().lower()
            for item in (raw.get("chaincode_ids") or [])
            if str(item).strip()
        }
    )
    if not org_id and not channel_ids and not chaincode_ids:
        return {}
    return {
        "org_id": org_id,
        "channel_ids": channel_ids,
        "chaincode_ids": chaincode_ids,
    }


def _component_sort_key(component: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(component.get("host_id", "")).strip(),
        str(component.get("component_type", "")).strip().lower(),
        str(component.get("component_id", "")).strip(),
        str(component.get("name", "")).strip(),
    )


def _normalize_observed_components(runtime_state: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_host: Dict[str, List[Dict[str, Any]]] = {}
    for host_id, host_state in runtime_state.items():
        host_key = str(host_id).strip()
        if not host_key:
            continue
        host_payload = host_state if isinstance(host_state, dict) else {}

        observed: List[Dict[str, Any]] = []
        raw_components = host_payload.get("components", [])
        if isinstance(raw_components, list):
            for item in raw_components:
                if not isinstance(item, dict):
                    continue
                observed.append(
                    {
                        "host_id": str(item.get("host_id", "")).strip() or host_key,
                        "component_id": str(item.get("component_id", "")).strip(),
                        "component_type": str(item.get("component_type", "")).strip().lower(),
                        "name": str(item.get("name", "")).strip(),
                        "image": str(item.get("image", "")).strip(),
                        "ports": _sorted_unique_ports(item.get("ports", [])),
                        "status": str(item.get("status", "")).strip().lower(),
                    }
                )

        # Fallback para estado legado do pipeline (nodes), mantendo compatibilidade.
        raw_nodes = host_payload.get("nodes", {})
        if isinstance(raw_nodes, dict):
            for node_id, node in sorted(raw_nodes.items(), key=lambda pair: str(pair[0])):
                if not isinstance(node, dict):
                    continue
                observed.append(
                    {
                        "host_id": host_key,
                        "component_id": str(node_id).strip(),
                        "component_type": str(node.get("node_type", "")).strip().lower(),
                        "name": str(node_id).strip(),
                        "image": "",
                        "ports": _sorted_unique_ports(node.get("ports", [])),
                        "status": "running",
                    }
                )

        by_host[host_key] = sorted(observed, key=_component_sort_key)
    return by_host


def _observed_index_by_host(observed_by_host: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    index_by_host: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for host_id, components in observed_by_host.items():
        component_index: Dict[str, Dict[str, Any]] = {}
        name_index: Dict[str, Dict[str, Any]] = {}
        for component in components:
            component_id = str(component.get("component_id", "")).strip()
            name = str(component.get("name", "")).strip()
            if component_id and component_id not in component_index:
                component_index[component_id] = component
            if name and name not in name_index:
                name_index[name] = component
        index_by_host[host_id] = {**name_index, **component_index}
    return index_by_host


def _drift_flags(desired: Dict[str, Any], observed: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    desired_type = str(desired.get("component_type", "")).strip().lower()
    observed_type = str(observed.get("component_type", "")).strip().lower()
    if desired_type and observed_type and desired_type != observed_type:
        flags.append("type_drift")

    desired_image = str(desired.get("image", "")).strip()
    observed_image = str(observed.get("image", "")).strip()
    if desired_image and observed_image and desired_image != observed_image:
        flags.append("image_drift")

    desired_ports = _sorted_unique_ports(desired.get("ports", []))
    observed_ports = _sorted_unique_ports(observed.get("ports", []))
    if desired_ports != observed_ports:
        flags.append("port_drift")

    return sorted(set(flags))


def _derive_action(
    *,
    desired: Dict[str, Any],
    observed: Optional[Dict[str, Any]],
) -> Tuple[str, str, List[str]]:
    desired_state = str(desired.get("desired_state", "")).strip().lower()
    if desired_state == "planned":
        return "verify", "component_planned", []

    if observed is None:
        return "create", "component_missing", []

    drift = _drift_flags(desired, observed)
    if drift:
        return "update", "configuration_drift", drift

    status = str(observed.get("status", "")).strip().lower()
    if status in _RUNNING_STATUSES:
        return "noop", "already_running", []
    if status in _STOPPED_STATUSES:
        return "start", "stopped_component", []
    return "verify", "status_unknown", []


def _normalize_manifest_components(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    components_raw = report.get("normalized_components", [])
    if not isinstance(components_raw, list):
        return []
    components: List[Dict[str, Any]] = []
    for item in components_raw:
        if not isinstance(item, dict):
            continue
        components.append(
            {
                "host_id": str(item.get("host_id", "")).strip(),
                "component_type": str(item.get("component_type", "")).strip().lower(),
                "component_id": str(item.get("component_id", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "image": str(item.get("image", "")).strip(),
                "ports": _sorted_unique_ports(item.get("ports", [])),
                "env_profile": str(item.get("env_profile", "")).strip().lower(),
                "storage_profile": str(item.get("storage_profile", "")).strip(),
                "resources": _normalize_resources(item.get("resources", {})),
                "service_context": _normalize_service_context(item.get("service_context", {})),
                "desired_state": str(item.get("desired_state", "")).strip().lower(),
                "criticality": str(item.get("criticality", "")).strip().lower(),
            }
        )
    return sorted(components, key=_component_sort_key)


def _fallback_components_from_prepare_plan(execution_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    host_plan = execution_plan.get("host_plan", [])
    if not isinstance(host_plan, list):
        return []
    components: List[Dict[str, Any]] = []
    for host_entry in host_plan:
        if not isinstance(host_entry, dict):
            continue
        host_id = str(host_entry.get("host_ref", "")).strip()
        if not host_id:
            continue
        for node in host_entry.get("nodes", []) or []:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("node_id", "")).strip()
            node_type = str(node.get("node_type", "")).strip().lower()
            if not node_id:
                continue
            components.append(
                {
                    "host_id": host_id,
                    "component_type": node_type,
                    "component_id": node_id,
                    "name": node_id,
                    "image": "",
                    "ports": _sorted_unique_ports(node.get("ports", [])),
                    "env_profile": "",
                    "storage_profile": str(node.get("storage_profile", "")).strip(),
                    "resources": _normalize_resources(node.get("resources", {})),
                    "service_context": {},
                    "desired_state": "required",
                    "criticality": "critical",
                }
            )
    return sorted(components, key=_component_sort_key)


def _action_summary(entries: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    summary = {action: 0 for action in sorted(ALLOWED_PROVISION_ACTIONS)}
    for entry in entries:
        action = str(entry.get("action", "")).strip().lower()
        if action in summary:
            summary[action] += 1
    return summary


def materialize_provision_execution_plan(
    *,
    run: PipelineRun,
    execution_plan: Dict[str, Any],
    org_runtime_manifest_report: Optional[Dict[str, Any]],
    normalized_runtime_state: Dict[str, Dict[str, Any]],
    reconciliation_plan: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    report = (
        dict(org_runtime_manifest_report)
        if isinstance(org_runtime_manifest_report, dict)
        else {}
    )
    reconciliation = (
        dict(reconciliation_plan)
        if isinstance(reconciliation_plan, dict)
        else {}
    )
    reconcile_actions_raw = reconciliation.get("reconcile_actions", [])
    if isinstance(reconcile_actions_raw, list) and reconcile_actions_raw:
        entries: List[Dict[str, Any]] = []
        for action_item in reconcile_actions_raw:
            if not isinstance(action_item, dict):
                continue
            if str(action_item.get("target_kind", "")).strip().lower() != "desired_component":
                continue

            desired = action_item.get("desired", {})
            observed = action_item.get("observed", {})
            if not isinstance(desired, dict):
                desired = {}
            if not isinstance(observed, dict):
                observed = {}

            host_id = str(action_item.get("host_id", "")).strip()
            component_type = str(action_item.get("component_type", "")).strip().lower()
            component_id = str(action_item.get("component_id", "")).strip()
            component_name = str(action_item.get("name", "")).strip()
            desired_state = str(action_item.get("desired_state", "")).strip().lower()
            criticality = str(action_item.get("criticality", "")).strip().lower()
            action = str(action_item.get("action", "")).strip().lower()
            if action not in ALLOWED_PROVISION_ACTIONS:
                action = "verify"

            divergences = action_item.get("divergences", [])
            if not isinstance(divergences, list):
                divergences = []
            drift_flags = sorted(
                str(item).strip().lower()
                for item in divergences
                if str(item).strip().lower() in {"type_drift", "image_drift", "port_drift", "env_drift", "host_drift"}
            )
            action_reason = str(action_item.get("cause", "")).strip() or "reconciliation_policy"
            current_status = str(observed.get("status", "")).strip().lower()
            image = str(desired.get("image", "")).strip()
            ports = _sorted_unique_ports(desired.get("ports", []))
            env_profile = str(desired.get("env_profile", "")).strip().lower()
            storage_profile = str(desired.get("storage_profile", "")).strip()
            resources = _normalize_resources(desired.get("resources", {}))
            service_context = _normalize_service_context(desired.get("service_context", {}))
            desired_signature = payload_sha256(
                {
                    "host_id": host_id,
                    "component_type": component_type,
                    "component_id": component_id,
                    "name": component_name,
                    "image": image,
                    "ports": ports,
                    "desired_state": desired_state,
                    "env_profile": env_profile,
                    "storage_profile": storage_profile,
                    "resources": resources,
                    "service_context": service_context,
                }
            )
            observed_signature = (
                payload_sha256(
                    {
                        "host_id": str(observed.get("host_id", "")).strip(),
                        "component_type": component_type,
                        "component_id": component_id,
                        "name": component_name,
                        "image": str(observed.get("image", "")).strip(),
                        "ports": _sorted_unique_ports(observed.get("ports", [])),
                        "status": current_status,
                        "env_profile": str(observed.get("env_profile", "")).strip().lower(),
                    }
                )
                if observed
                else ""
            )
            entries.append(
                {
                    "host_id": host_id,
                    "component_type": component_type,
                    "component_id": component_id,
                    "name": component_name,
                    "desired_state": desired_state,
                    "criticality": criticality,
                    "action": action,
                    "action_reason": action_reason,
                    "drift_flags": drift_flags,
                    "current_status": current_status,
                    "image": image,
                    "ports": ports,
                    "env_profile": env_profile,
                    "storage_profile": storage_profile,
                    "resources": resources,
                    "service_context": service_context,
                    "desired_signature": desired_signature,
                    "observed_signature": observed_signature,
                }
            )

        entries = sorted(entries, key=lambda item: (
            str(item.get("host_id", "")),
            str(item.get("component_type", "")),
            str(item.get("component_id", "")),
            str(item.get("name", "")),
        ))
        canonical_plan_payload = {
            "run_id": run.run_id,
            "change_id": run.change_id,
            "plan_source": "reconciliation_plan",
            "manifest_fingerprint": str(report.get("manifest_fingerprint", "")).strip().lower(),
            "source_blueprint_fingerprint": str(report.get("source_blueprint_fingerprint", "")).strip().lower(),
            "entries": entries,
        }
        plan_fingerprint = payload_sha256(canonical_plan_payload)
        resolved_generated_at = str(execution_plan.get("generated_at", "")).strip() or run.started_at or utc_now_iso()
        return {
            **canonical_plan_payload,
            "plan_fingerprint": plan_fingerprint,
            "entry_count": len(entries),
            "action_summary": _action_summary(entries),
            "generated_at": resolved_generated_at,
        }

    manifest_components = _normalize_manifest_components(report)
    if manifest_components:
        desired_components = manifest_components
        plan_source = "org_runtime_manifest"
    else:
        desired_components = _fallback_components_from_prepare_plan(execution_plan)
        plan_source = "prepare_execution_plan"

    observed_by_host = _normalize_observed_components(normalized_runtime_state)
    observed_index = _observed_index_by_host(observed_by_host)

    entries: List[Dict[str, Any]] = []
    for desired in desired_components:
        host_id = str(desired.get("host_id", "")).strip()
        host_observed = observed_index.get(host_id, {})
        component_id = str(desired.get("component_id", "")).strip()
        component_name = str(desired.get("name", "")).strip()
        observed = host_observed.get(component_id) or host_observed.get(component_name)

        action, reason, drift_flags = _derive_action(
            desired=desired,
            observed=observed,
        )
        entry = {
            "host_id": host_id,
            "component_type": str(desired.get("component_type", "")).strip().lower(),
            "component_id": component_id,
            "name": component_name,
            "desired_state": str(desired.get("desired_state", "")).strip().lower(),
            "criticality": str(desired.get("criticality", "")).strip().lower(),
            "action": action,
            "action_reason": reason,
            "drift_flags": drift_flags,
            "image": str(desired.get("image", "")).strip(),
            "ports": _sorted_unique_ports(desired.get("ports", [])),
            "env_profile": str(desired.get("env_profile", "")).strip().lower(),
            "storage_profile": str(desired.get("storage_profile", "")).strip(),
            "resources": _normalize_resources(desired.get("resources", {})),
            "service_context": _normalize_service_context(desired.get("service_context", {})),
            "current_status": (
                str((observed or {}).get("status", "")).strip().lower()
                if isinstance(observed, dict)
                else ""
            ),
            "desired_signature": payload_sha256(
                {
                    "host_id": host_id,
                    "component_type": str(desired.get("component_type", "")).strip().lower(),
                    "component_id": component_id,
                    "name": component_name,
                    "image": str(desired.get("image", "")).strip(),
                    "ports": _sorted_unique_ports(desired.get("ports", [])),
                    "desired_state": str(desired.get("desired_state", "")).strip().lower(),
                    "env_profile": str(desired.get("env_profile", "")).strip().lower(),
                    "storage_profile": str(desired.get("storage_profile", "")).strip(),
                    "resources": _normalize_resources(desired.get("resources", {})),
                    "service_context": _normalize_service_context(desired.get("service_context", {})),
                }
            ),
            "observed_signature": (
                payload_sha256(
                    {
                        "host_id": str((observed or {}).get("host_id", "")).strip(),
                        "component_type": str((observed or {}).get("component_type", "")).strip().lower(),
                        "component_id": str((observed or {}).get("component_id", "")).strip(),
                        "name": str((observed or {}).get("name", "")).strip(),
                        "image": str((observed or {}).get("image", "")).strip(),
                        "ports": _sorted_unique_ports((observed or {}).get("ports", [])),
                        "status": str((observed or {}).get("status", "")).strip().lower(),
                    }
                )
                if isinstance(observed, dict)
                else ""
            ),
        }
        entries.append(entry)

    entries = sorted(entries, key=lambda item: (
        str(item.get("host_id", "")),
        str(item.get("component_type", "")),
        str(item.get("component_id", "")),
        str(item.get("name", "")),
    ))
    canonical_plan_payload = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "plan_source": plan_source,
        "manifest_fingerprint": str(report.get("manifest_fingerprint", "")).strip().lower(),
        "source_blueprint_fingerprint": str(report.get("source_blueprint_fingerprint", "")).strip().lower(),
        "entries": entries,
    }
    plan_fingerprint = payload_sha256(canonical_plan_payload)
    resolved_generated_at = str(execution_plan.get("generated_at", "")).strip() or run.started_at or utc_now_iso()
    return {
        **canonical_plan_payload,
        "plan_fingerprint": plan_fingerprint,
        "entry_count": len(entries),
        "action_summary": _action_summary(entries),
        "generated_at": resolved_generated_at,
    }


@dataclass(frozen=True)
class ProvisionExecutionPlanResult:
    plan: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def materialize_incremental_execution_plan(
    *,
    run: PipelineRun,
    execution_plan: Dict[str, Any],
    org_runtime_manifest_report: Optional[Dict[str, Any]],
    incremental_gate_result: Optional[Dict[str, Any]],
    topology_change_intent: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    report = dict(org_runtime_manifest_report) if isinstance(org_runtime_manifest_report, dict) else {}
    gate = dict(incremental_gate_result) if isinstance(incremental_gate_result, dict) else {}
    intent = dict(topology_change_intent) if isinstance(topology_change_intent, dict) else {}

    org_id = str(report.get("org_id", "")).strip().lower() or str(gate.get("org_id", "")).strip().lower()
    org_token = org_id or "org"

    existing_components: List[Dict[str, Any]] = []
    existing_name_index: Dict[str, Dict[str, Any]] = {}
    existing_id_index: Dict[str, Dict[str, Any]] = {}
    existing_couch_name_index: Dict[str, Dict[str, Any]] = {}
    existing_couch_id_index: Dict[str, Dict[str, Any]] = {}
    for item in report.get("normalized_components", []):
        if not isinstance(item, dict):
            continue
        desired_state = str(item.get("desired_state", "")).strip().lower()
        component_type = str(item.get("component_type", "")).strip().lower()
        if desired_state == "planned" or component_type not in {"peer", "orderer", "couch"}:
            continue
        normalized = {
            "host_id": str(item.get("host_id", "")).strip(),
            "component_type": component_type,
            "component_id": str(item.get("component_id", "")).strip(),
            "name": str(item.get("name", "")).strip().lower(),
            "image": str(item.get("image", "")).strip(),
            "ports": _sorted_unique_ports(item.get("ports", [])),
            "env_profile": str(item.get("env_profile", "")).strip().lower(),
            "storage_profile": str(item.get("storage_profile", "")).strip(),
            "resources": _normalize_resources(item.get("resources", {})),
            "desired_state": "required",
            "criticality": str(item.get("criticality", "")).strip().lower() or "critical",
        }
        existing_components.append(normalized)
        if normalized["name"]:
            existing_name_index.setdefault(normalized["name"], normalized)
        if normalized["component_id"]:
            existing_id_index.setdefault(normalized["component_id"], normalized)
        if component_type == "couch":
            if normalized["name"]:
                existing_couch_name_index.setdefault(normalized["name"], normalized)
            if normalized["component_id"]:
                existing_couch_id_index.setdefault(normalized["component_id"], normalized)

    operations = {
        str(item).strip().lower()
        for item in (gate.get("topology_change_operations") or [])
        if str(item).strip()
    }
    for item in intent.get("operations", []):
        if not isinstance(item, dict):
            continue
        operation = str(item.get("operation", "")).strip().lower()
        if operation:
            operations.add(operation)
    add_peer_requested = "add_peer" in operations
    add_orderer_requested = "add_orderer" in operations

    normalized_org_identity = report.get("normalized_org_identity") if isinstance(report.get("normalized_org_identity"), dict) else {}
    expected_org_msp_id = str(normalized_org_identity.get("msp_id", "")).strip()
    expected_org_tls_ca_profile_ref = str(normalized_org_identity.get("tls_ca_profile_ref", "")).strip().lower()

    intent_dependencies = intent.get("dependencies") if isinstance(intent.get("dependencies"), dict) else {}
    requires_orderer_consenters_update = bool(intent_dependencies.get("requires_orderer_consenters_update", False))
    requires_channel_updates = bool(intent_dependencies.get("requires_channel_updates", False))

    normalized_source_blueprint_scope = (
        report.get("normalized_source_blueprint_scope")
        if isinstance(report.get("normalized_source_blueprint_scope"), dict)
        else (
            report.get("source_blueprint_scope") if isinstance(report.get("source_blueprint_scope"), dict) else {}
        )
    )
    org_scope_channel_ids = sorted(
        {
            str(item).strip().lower()
            for item in (normalized_source_blueprint_scope.get("channels") or [])
            if str(item).strip()
        }
    )

    channel_association = (
        intent.get("channel_association")
        if isinstance(intent.get("channel_association"), dict)
        else {}
    )
    requested_channel_ids = sorted(
        {
            str(item).strip().lower()
            for item in (channel_association.get("requested_channel_ids") or [])
            if str(item).strip()
        }
    )
    channel_ids_for_projection = requested_channel_ids or list(org_scope_channel_ids)
    out_of_scope_channel_ids = sorted(
        {
            item
            for item in channel_ids_for_projection
            if item not in set(org_scope_channel_ids)
        }
    )

    component_channel_targets_map: Dict[str, List[str]] = {}
    for target in (channel_association.get("component_channel_targets") or []):
        if not isinstance(target, dict):
            continue
        component_name = str(target.get("component_name", "")).strip().lower()
        component_id = str(target.get("component_id", "")).strip().lower()
        target_key = component_name or component_id
        if not target_key:
            continue
        target_channel_ids = sorted(
            {
                str(item).strip().lower()
                for item in (target.get("channel_ids") or [])
                if str(item).strip()
            }
        )
        if target_channel_ids:
            component_channel_targets_map[target_key] = target_channel_ids

    channel_step_status_raw = (
        channel_association.get("channel_step_status")
        if isinstance(channel_association.get("channel_step_status"), dict)
        else {}
    )
    channel_step_status: Dict[str, str] = {}
    for key, value in channel_step_status_raw.items():
        normalized_key = str(key).strip().lower()
        normalized_value = str(value).strip().lower()
        if not normalized_key:
            continue
        if normalized_value not in {"completed", "pending", "planned", "not_required", "n/a"}:
            normalized_value = "pending"
        channel_step_status[normalized_key] = normalized_value

    ordering_service_governance = (
        intent.get("ordering_service_governance")
        if isinstance(intent.get("ordering_service_governance"), dict)
        else {}
    )
    ordering_quorum_guardrail = (
        ordering_service_governance.get("quorum_guardrail")
        if isinstance(ordering_service_governance.get("quorum_guardrail"), dict)
        else {}
    )
    consenter_update_prepared = bool(ordering_service_governance.get("consenter_update_prepared", False))
    policy_update_prepared = bool(ordering_service_governance.get("policy_update_prepared", False))

    requested_by_name: Dict[str, Dict[str, Any]] = {}
    for item in intent.get("requested_components", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip().lower()
        if name:
            requested_by_name[name] = dict(item)

    drift_evidence: Dict[str, Dict[str, Any]] = {}
    drift_raw = intent.get("drift_evidence")
    if isinstance(drift_raw, dict):
        for key, payload in drift_raw.items():
            drift_key = str(key).strip().lower()
            if drift_key and isinstance(payload, dict):
                drift_evidence[drift_key] = dict(payload)
    elif isinstance(drift_raw, list):
        for payload in drift_raw:
            if not isinstance(payload, dict):
                continue
            drift_key = (
                str(payload.get("name", "")).strip().lower()
                or str(payload.get("component_id", "")).strip().lower()
            )
            if drift_key:
                drift_evidence[drift_key] = dict(payload)

    default_image = {
        "peer": "hyperledger/fabric-peer:2.5",
        "orderer": "hyperledger/fabric-orderer:2.5",
        "couch": "couchdb:3.3",
    }
    default_ports = {
        "peer": [7051, 17051],
        "orderer": [7050, 9443, 17050],
        "couch": [5984],
    }
    default_storage = {
        "peer": "peer-persistent",
        "orderer": "orderer-persistent",
        "couch": "couch-persistent",
    }
    default_resources = {
        "peer": {"cpu": 2, "memory_mb": 2048, "disk_gb": 30},
        "orderer": {"cpu": 2, "memory_mb": 2048, "disk_gb": 40},
        "couch": {"cpu": 1, "memory_mb": 1024, "disk_gb": 20},
    }

    manifest_domain = str(report.get("domain", "")).strip().lower()
    if not manifest_domain:
        first_named_component = next(
            (
                str(item.get("name", "")).strip().lower()
                for item in existing_components
                if str(item.get("name", "")).strip()
            ),
            "",
        )
        if "." in first_named_component:
            manifest_domain = first_named_component.split(".", 1)[1]

    issues: List[Dict[str, Any]] = []
    incremental_components: List[Dict[str, Any]] = []
    incremental_peer_couch_pairs: List[Dict[str, Any]] = []
    incremental_orderer_governance_updates: List[Dict[str, Any]] = []
    incremental_channel_association_projections: List[Dict[str, Any]] = []
    incremental_channel_step_dependencies: List[Dict[str, Any]] = []
    seen_component_ids: Set[str] = set()
    allocations = gate.get("incremental_component_allocations", []) if isinstance(gate.get("incremental_component_allocations", []), list) else []
    existing_active_orderer_count = sum(
        1
        for item in existing_components
        if str(item.get("component_type", "")).strip().lower() == "orderer"
    )
    incremental_orderer_allocations = [
        item
        for item in allocations
        if isinstance(item, dict)
        and str(item.get("component_type", "")).strip().lower() == "orderer"
    ]
    projected_active_orderer_count = existing_active_orderer_count + len(incremental_orderer_allocations)
    baseline_quorum = (existing_active_orderer_count // 2) + 1 if existing_active_orderer_count > 0 else 0
    projected_quorum = (projected_active_orderer_count // 2) + 1 if projected_active_orderer_count > 0 else 0
    raw_guardrail_min = ordering_quorum_guardrail.get("min_active_orderers")
    guardrail_min_active_orderers = 0
    try:
        guardrail_min_active_orderers = int(str(raw_guardrail_min).strip())
    except (TypeError, ValueError):
        guardrail_min_active_orderers = 0

    if add_orderer_requested:
        if guardrail_min_active_orderers <= 0:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_orderer_quorum_guardrail_invalid",
                    "path": "topology_change_intent.ordering_service_governance.quorum_guardrail.min_active_orderers",
                    "message": (
                        "add_orderer bloqueado: quorum_guardrail.min_active_orderers deve ser inteiro positivo."
                    ),
                }
            )
        elif baseline_quorum > 0 and guardrail_min_active_orderers < baseline_quorum:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_orderer_quorum_guardrail_invalid",
                    "path": "topology_change_intent.ordering_service_governance.quorum_guardrail.min_active_orderers",
                    "message": (
                        "add_orderer bloqueado: guardrail de quorum abaixo do mínimo da baseline "
                        f"(guardrail={guardrail_min_active_orderers}, baseline_quorum={baseline_quorum})."
                    ),
                }
            )
        elif existing_active_orderer_count < guardrail_min_active_orderers:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_orderer_quorum_guardrail_violation",
                    "path": "topology_change_intent.ordering_service_governance.quorum_guardrail.min_active_orderers",
                    "message": (
                        "add_orderer bloqueado: baseline atual já viola guardrail de disponibilidade do ordering service "
                        f"(active_orderers={existing_active_orderer_count}, guardrail={guardrail_min_active_orderers})."
                    ),
                }
            )

        if requires_orderer_consenters_update and not consenter_update_prepared:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_orderer_consenters_update_not_prepared",
                    "path": "topology_change_intent.ordering_service_governance.consenter_update_prepared",
                    "message": (
                        "add_orderer bloqueado: atualização de consenters requerida sem preparação de governança."
                    ),
                }
            )
        if requires_orderer_consenters_update and not policy_update_prepared:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_orderer_policy_update_not_prepared",
                    "path": "topology_change_intent.ordering_service_governance.policy_update_prepared",
                    "message": (
                        "add_orderer bloqueado: atualização de políticas requerida sem preparação de governança."
                    ),
                }
            )
    if (add_peer_requested or add_orderer_requested) and not org_scope_channel_ids:
        issues.append(
            {
                "level": "error",
                "code": "a2_4_channel_association_scope_missing",
                "path": "org_runtime_manifest_report.normalized_source_blueprint_scope.channels",
                "message": (
                    "Expansão incremental bloqueada: escopo de canais da organização ausente no manifesto normalizado."
                ),
            }
        )
    if out_of_scope_channel_ids:
        issues.append(
            {
                "level": "error",
                "code": "a2_4_channel_association_out_of_scope",
                "path": "topology_change_intent.channel_association.requested_channel_ids",
                "message": (
                    "Expansão incremental bloqueada: associação de canal fora do escopo da organização "
                    f"(requested={out_of_scope_channel_ids}, scope={org_scope_channel_ids})."
                ),
            }
        )
    for allocation in allocations:
        if not isinstance(allocation, dict):
            continue
        component_type = str(allocation.get("component_type", "")).strip().lower()
        if component_type not in {"peer", "orderer"}:
            continue
        allocated_name = str(allocation.get("allocated_name", "")).strip().lower()
        host_id = str(allocation.get("host_id", "")).strip()
        allocated_index_text = str(allocation.get("allocated_index", "")).strip()
        allocated_index = int(allocated_index_text) if allocated_index_text.isdigit() else 0
        component_id = f"{component_type}{allocated_index}-{org_token}"

        requested_payload = requested_by_name.get(allocated_name, {})
        ports = _sorted_unique_ports(requested_payload.get("ports", [])) or list(default_ports.get(component_type, []))
        resources = _normalize_resources(requested_payload.get("resources", {}))
        if not any(resources.values()):
            resources = dict(default_resources.get(component_type, {"cpu": 1, "memory_mb": 512, "disk_gb": 10}))

        existing_component = existing_name_index.get(allocated_name) or existing_id_index.get(component_id)
        has_drift_evidence = allocated_name in drift_evidence or component_id in drift_evidence
        if existing_component is not None and not has_drift_evidence:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_incremental_plan_recreate_converged_without_drift",
                    "path": "a2_4_incremental_entry_gate.incremental_component_allocations",
                    "message": (
                        "Plano incremental não pode recriar componente já convergido sem evidência "
                        f"de drift ('{allocated_name}')."
                    ),
                }
            )
            continue

        if component_id in seen_component_ids:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_incremental_plan_component_id_collision",
                    "path": "a2_4_incremental_entry_gate.incremental_component_allocations",
                    "message": f"component_id incremental duplicado detectado: '{component_id}'.",
                }
            )
            continue
        seen_component_ids.add(component_id)

        incremental_components.append(
            {
                "host_id": host_id,
                "component_type": component_type,
                "component_id": component_id,
                "name": allocated_name,
                "image": str(requested_payload.get("image", "")).strip() or default_image.get(component_type, ""),
                "ports": ports,
                "env_profile": str(requested_payload.get("env_profile", "")).strip().lower() or "dev",
                "storage_profile": str(requested_payload.get("storage_profile", "")).strip() or default_storage.get(component_type, ""),
                "resources": resources,
                "desired_state": "required",
                "criticality": "critical",
                "operation_type": "incremental_add",
                "drift_evidence": drift_evidence.get(allocated_name) or drift_evidence.get(component_id) or {},
                "existing_with_drift": bool(existing_component is not None and has_drift_evidence),
                "incremental_pairing_mode": "n/a",
            }
        )

        if component_type == "orderer" and add_orderer_requested:
            connectivity = (
                requested_payload.get("tls_msp_connectivity")
                if isinstance(requested_payload.get("tls_msp_connectivity"), dict)
                else {}
            )
            connectivity_msp_id = str(connectivity.get("msp_id", "")).strip() or expected_org_msp_id
            connectivity_tls_ca_profile_ref = (
                str(connectivity.get("tls_ca_profile_ref", "")).strip().lower()
                or expected_org_tls_ca_profile_ref
            )
            connectivity_tls_cert_ref = str(connectivity.get("tls_cert_ref", "")).strip()
            connectivity_tls_key_ref = str(connectivity.get("tls_key_ref", "")).strip()
            connectivity_msp_signcert_ref = (
                str(connectivity.get("msp_signcert_ref", "")).strip()
                or str(connectivity.get("msp_cert_ref", "")).strip()
            )
            connectivity_validated = bool(connectivity.get("validated", False))

            if not connectivity_msp_id:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_precondition_missing",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.msp_id",
                        "message": "add_orderer bloqueado: msp_id obrigatório para conectividade TLS/MSP.",
                    }
                )
            elif expected_org_msp_id and connectivity_msp_id != expected_org_msp_id:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_precondition_mismatch",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.msp_id",
                        "message": (
                            "add_orderer bloqueado: msp_id incompatível com identidade da organização "
                            f"(expected='{expected_org_msp_id}', received='{connectivity_msp_id}')."
                        ),
                    }
                )

            if not connectivity_tls_ca_profile_ref:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_precondition_missing",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.tls_ca_profile_ref",
                        "message": "add_orderer bloqueado: tls_ca_profile_ref obrigatório para conectividade TLS/MSP.",
                    }
                )
            if not connectivity_tls_cert_ref:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_precondition_missing",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.tls_cert_ref",
                        "message": "add_orderer bloqueado: tls_cert_ref obrigatório para conectividade TLS/MSP.",
                    }
                )
            if not connectivity_tls_key_ref:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_precondition_missing",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.tls_key_ref",
                        "message": "add_orderer bloqueado: tls_key_ref obrigatório para conectividade TLS/MSP.",
                    }
                )
            if not connectivity_msp_signcert_ref:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_precondition_missing",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.msp_signcert_ref",
                        "message": "add_orderer bloqueado: msp_signcert_ref obrigatório para conectividade TLS/MSP.",
                    }
                )
            if not connectivity_validated:
                issues.append(
                    {
                        "level": "error",
                        "code": "a2_4_add_orderer_tls_msp_connectivity_not_validated",
                        "path": "topology_change_intent.requested_components.tls_msp_connectivity.validated",
                        "message": "add_orderer bloqueado: validação TLS/MSP não confirmada no intent incremental.",
                    }
                )

            governance_payload = {
                "requires_channel_updates": requires_channel_updates,
                "requires_orderer_consenters_update": requires_orderer_consenters_update,
                "consenter_update_prepared": consenter_update_prepared,
                "policy_update_prepared": policy_update_prepared,
                "quorum_guardrail_min_active_orderers": guardrail_min_active_orderers,
                "baseline_active_orderer_count": existing_active_orderer_count,
                "projected_active_orderer_count": projected_active_orderer_count,
                "baseline_quorum": baseline_quorum,
                "projected_quorum": projected_quorum,
            }
            incremental_orderer_governance_updates.append(
                {
                    "orderer_component_id": component_id,
                    "orderer_name": allocated_name,
                    "host_id": host_id,
                    "tls_msp_connectivity": {
                        "msp_id": connectivity_msp_id,
                        "tls_ca_profile_ref": connectivity_tls_ca_profile_ref,
                        "tls_cert_ref": connectivity_tls_cert_ref,
                        "tls_key_ref": connectivity_tls_key_ref,
                        "msp_signcert_ref": connectivity_msp_signcert_ref,
                        "validated": connectivity_validated,
                    },
                    "ordering_service_governance": governance_payload,
                }
            )

        if component_type != "peer" or not add_peer_requested:
            continue

        peer_match = re.match(r"^peer(\d+)\.", allocated_name)
        if peer_match is None:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_peer_couch_pairing_invalid_peer_name",
                    "path": "a2_4_incremental_entry_gate.incremental_component_allocations",
                    "message": (
                        "Pareamento add_peer bloqueado: nome do peer incremental fora do padrão "
                        f"determinístico para derivação do CouchDB correspondente ('{allocated_name}')."
                    ),
                }
            )
            continue

        peer_index = int(peer_match.group(1))
        couch_name = f"couch{peer_index}.{org_token}.{manifest_domain}" if manifest_domain else f"couch{peer_index}-{org_token}"
        couch_component_id = f"couch{peer_index}-{org_token}"
        expected_couch_host = host_id
        couch_port = 5984 + (peer_index * 1000)
        requested_couch_payload = requested_by_name.get(couch_name, {})
        couch_ports = _sorted_unique_ports(requested_couch_payload.get("ports", [])) or [couch_port]
        couch_resources = _normalize_resources(requested_couch_payload.get("resources", {}))
        if not any(couch_resources.values()):
            couch_resources = dict(default_resources.get("couch", {"cpu": 1, "memory_mb": 1024, "disk_gb": 20}))

        existing_couch = existing_couch_name_index.get(couch_name) or existing_couch_id_index.get(couch_component_id)
        if existing_couch is not None and str(existing_couch.get("host_id", "")).strip() != expected_couch_host:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_peer_couch_pairing_incompatible",
                    "path": "a2_4_incremental_entry_gate.incremental_component_allocations",
                    "message": (
                        "add_peer bloqueado: CouchDB correspondente encontrado em host incompatível "
                        f"(peer_host='{expected_couch_host}', couch_host='{existing_couch.get('host_id', '')}', couch='{couch_name}')."
                    ),
                }
            )
            continue

        if couch_component_id in seen_component_ids:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_add_peer_couch_component_id_collision",
                    "path": "a2_4_incremental_entry_gate.incremental_component_allocations",
                    "message": f"component_id incremental de couch duplicado detectado: '{couch_component_id}'.",
                }
            )
            continue
        seen_component_ids.add(couch_component_id)

        couch_pair_mode = "reuse"
        if existing_couch is None:
            couch_pair_mode = "create"

        incremental_components.append(
            {
                "host_id": expected_couch_host,
                "component_type": "couch",
                "component_id": couch_component_id,
                "name": couch_name,
                "image": str(requested_couch_payload.get("image", "")).strip() or default_image.get("couch", ""),
                "ports": couch_ports,
                "env_profile": str(requested_couch_payload.get("env_profile", "")).strip().lower() or "dev",
                "storage_profile": str(requested_couch_payload.get("storage_profile", "")).strip() or default_storage.get("couch", ""),
                "resources": couch_resources,
                "desired_state": "required",
                "criticality": "critical",
                "operation_type": (
                    "incremental_add_peer_couch_reuse"
                    if couch_pair_mode == "reuse"
                    else "incremental_add_peer_couch_create"
                ),
                "drift_evidence": {},
                "existing_with_drift": False,
                "incremental_pairing_mode": couch_pair_mode,
                "paired_peer_component_id": component_id,
                "paired_peer_name": allocated_name,
            }
        )
        incremental_peer_couch_pairs.append(
            {
                "peer_component_id": component_id,
                "peer_name": allocated_name,
                "couch_component_id": couch_component_id,
                "couch_name": couch_name,
                "host_id": expected_couch_host,
                "pairing_mode": couch_pair_mode,
            }
        )

    projection_default_step_status = "pending" if requires_channel_updates else "completed"
    for component in sorted(incremental_components, key=_component_sort_key):
        component_type = str(component.get("component_type", "")).strip().lower()
        if component_type not in {"peer", "orderer"}:
            continue
        component_name = str(component.get("name", "")).strip().lower()
        component_id = str(component.get("component_id", "")).strip().lower()
        component_channels = (
            component_channel_targets_map.get(component_name)
            or component_channel_targets_map.get(component_id)
            or list(channel_ids_for_projection)
        )
        component_out_of_scope = sorted(
            {
                channel_id
                for channel_id in component_channels
                if channel_id not in set(org_scope_channel_ids)
            }
        )
        if component_out_of_scope:
            issues.append(
                {
                    "level": "error",
                    "code": "a2_4_channel_association_out_of_scope",
                    "path": "topology_change_intent.channel_association.component_channel_targets",
                    "message": (
                        "Expansão incremental bloqueada: componente incremental com canal fora do escopo da organização "
                        f"(component='{component_name or component_id}', channels={component_out_of_scope})."
                    ),
                }
            )
            continue

        required_steps = ["join", "anchor"] if component_type == "peer" else ["config-update"]
        critical_steps = {"join", "config-update"}
        for channel_id in sorted({str(item).strip().lower() for item in component_channels if str(item).strip()}):
            association_status = "ready"
            pending_critical_step_ids: List[str] = []
            for step_name in required_steps:
                step_key = f"{component_name}|{channel_id}|{step_name}"
                step_status = channel_step_status.get(step_key, projection_default_step_status)
                dependency = {
                    "component_id": component_id,
                    "component_name": component_name,
                    "component_type": component_type,
                    "channel_id": channel_id,
                    "step": step_name,
                    "status": step_status,
                    "critical": step_name in critical_steps,
                    "requires_channel_updates": requires_channel_updates,
                    "baseline_state": "converged_preserved",
                    "pending_additional_configuration": step_status in {"pending", "planned"},
                }
                incremental_channel_step_dependencies.append(dependency)
                if dependency["critical"] and dependency["pending_additional_configuration"]:
                    pending_critical_step_ids.append(step_name)
                    association_status = "pending_critical_configuration"
                elif dependency["pending_additional_configuration"] and association_status == "ready":
                    association_status = "pending_additional_configuration"

            incremental_channel_association_projections.append(
                {
                    "component_id": component_id,
                    "component_name": component_name,
                    "component_type": component_type,
                    "host_id": str(component.get("host_id", "")).strip(),
                    "channel_id": channel_id,
                    "association_scope": "org_channel_scope",
                    "baseline_state": "converged_preserved",
                    "association_status": association_status,
                    "critical_configuration_pending": bool(pending_critical_step_ids),
                    "pending_critical_steps": sorted(set(pending_critical_step_ids)),
                }
            )

    pending_critical_channel_steps = [
        item
        for item in incremental_channel_step_dependencies
        if isinstance(item, dict)
        and bool(item.get("critical", False))
        and bool(item.get("pending_additional_configuration", False))
    ]
    if pending_critical_channel_steps:
        issues.append(
            {
                "level": "error",
                "code": "a2_4_channel_critical_configuration_pending",
                "path": "incremental_execution_plan.incremental_channel_step_dependencies",
                "message": (
                    "Expansão incremental bloqueada: pendência crítica de configuração de canal detectada "
                    "(join/config-update) sem convergência, evitando estado de sucesso aparente."
                ),
            }
        )

    entries: List[Dict[str, Any]] = []
    for component in sorted(existing_components, key=_component_sort_key):
        for action_order, action in enumerate(("noop", "verify"), start=1):
            entries.append(
                {
                    **component,
                    "operation_type": "baseline_existing",
                    "action": action,
                    "action_order": action_order,
                    "action_reason": "preserve_converged_baseline",
                    "drift_flags": [],
                    "current_status": "running",
                    "desired_signature": payload_sha256(
                        {
                            "host_id": component["host_id"],
                            "component_type": component["component_type"],
                            "component_id": component["component_id"],
                            "name": component["name"],
                            "ports": component["ports"],
                            "resources": component["resources"],
                            "action": action,
                        }
                    ),
                    "observed_signature": "",
                }
            )

    for component in sorted(incremental_components, key=_component_sort_key):
        action_sequence = ("create", "start", "verify")
        if str(component.get("incremental_pairing_mode", "")).strip().lower() == "reuse":
            action_sequence = ("noop", "verify")
        elif bool(component.get("existing_with_drift", False)):
            action_sequence = ("update", "start", "verify")
        for action_order, action in enumerate(action_sequence, start=1):
            entries.append(
                {
                    **{k: v for k, v in component.items() if k not in {"existing_with_drift", "drift_evidence"}},
                    "action": action,
                    "action_order": action_order,
                    "action_reason": "incremental_expand_topology",
                    "drift_flags": ["drift_evidence"] if bool(component.get("existing_with_drift", False)) else [],
                    "current_status": "",
                    "desired_signature": payload_sha256(
                        {
                            "host_id": component["host_id"],
                            "component_type": component["component_type"],
                            "component_id": component["component_id"],
                            "name": component["name"],
                            "ports": component["ports"],
                            "resources": component["resources"],
                            "action": action,
                            "operation_type": "incremental_add",
                        }
                    ),
                    "observed_signature": "",
                }
            )

    entries = sorted(
        entries,
        key=lambda item: (
            str(item.get("host_id", "")),
            str(item.get("component_type", "")),
            str(item.get("component_id", "")),
            str(item.get("name", "")),
            int(item.get("action_order", 0) or 0),
        ),
    )

    incremental_replay_fingerprint = payload_sha256(
        {
            "execution_plan": execution_plan,
            "topology_change_intent_fingerprint": str(gate.get("topology_change_intent_fingerprint", "")).strip().lower(),
            "incremental_allocation_fingerprint": str(gate.get("incremental_allocation_fingerprint", "")).strip().lower(),
            "incremental_placement_fingerprint": str(gate.get("incremental_placement_fingerprint", "")).strip().lower(),
            "topology_change_intent": intent,
        }
    )

    canonical_payload = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "plan_source": "a2_4_incremental_topology",
        "manifest_fingerprint": str(report.get("manifest_fingerprint", "")).strip().lower(),
        "source_blueprint_fingerprint": str(report.get("source_blueprint_fingerprint", "")).strip().lower(),
        "topology_change_intent_fingerprint": str(gate.get("topology_change_intent_fingerprint", "")).strip().lower(),
        "incremental_allocation_fingerprint": str(gate.get("incremental_allocation_fingerprint", "")).strip().lower(),
        "incremental_placement_fingerprint": str(gate.get("incremental_placement_fingerprint", "")).strip().lower(),
        "incremental_replay_fingerprint": incremental_replay_fingerprint,
        "channel_association_fingerprint": payload_sha256(
            {
                "org_scope_channel_ids": org_scope_channel_ids,
                "requested_channel_ids": requested_channel_ids,
                "projections": incremental_channel_association_projections,
                "dependencies": incremental_channel_step_dependencies,
            }
        ),
        "entries": entries,
    }
    incremental_plan_fingerprint = payload_sha256(canonical_payload)
    resolved_generated_at = str(execution_plan.get("generated_at", "")).strip() or run.started_at or utc_now_iso()

    return {
        **canonical_payload,
        "incremental_plan_fingerprint": incremental_plan_fingerprint,
        "entry_count": len(entries),
        "action_summary": _action_summary(entries),
        "issue_count": len(issues),
        "issues": sorted(
            issues,
            key=lambda item: (
                str(item.get("level", "")),
                str(item.get("code", "")),
                str(item.get("path", "")),
                str(item.get("message", "")),
            ),
        ),
        "incremental_peer_couch_pairs": sorted(
            [dict(item) for item in incremental_peer_couch_pairs if isinstance(item, dict)],
            key=lambda item: (
                str(item.get("host_id", "")),
                str(item.get("peer_component_id", "")),
                str(item.get("couch_component_id", "")),
            ),
        ),
        "incremental_orderer_governance_updates": sorted(
            [dict(item) for item in incremental_orderer_governance_updates if isinstance(item, dict)],
            key=lambda item: (
                str(item.get("host_id", "")),
                str(item.get("orderer_component_id", "")),
                str(item.get("orderer_name", "")),
            ),
        ),
        "incremental_channel_association_projections": sorted(
            [dict(item) for item in incremental_channel_association_projections if isinstance(item, dict)],
            key=lambda item: (
                str(item.get("component_type", "")),
                str(item.get("component_id", "")),
                str(item.get("channel_id", "")),
                str(item.get("host_id", "")),
            ),
        ),
        "incremental_channel_step_dependencies": sorted(
            [dict(item) for item in incremental_channel_step_dependencies if isinstance(item, dict)],
            key=lambda item: (
                str(item.get("component_type", "")),
                str(item.get("component_id", "")),
                str(item.get("channel_id", "")),
                str(item.get("step", "")),
            ),
        ),
        "channel_association_scope": {
            "org_scope_channel_ids": list(org_scope_channel_ids),
            "requested_channel_ids": list(requested_channel_ids),
            "projected_channel_ids": list(channel_ids_for_projection),
            "out_of_scope_channel_ids": list(out_of_scope_channel_ids),
        },
        "blocked": any(str(item.get("level", "")).strip().lower() == "error" for item in issues),
        "generated_at": resolved_generated_at,
    }
