from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from .pipeline_contract import PipelineRun, utc_now_iso


_STOPPED_STATUSES = {"created", "exited", "stopped", "dead", "paused"}
_ENFORCED_STATES = {"required"}
_OBSERVED_STATES = {"planned", "optional"}
_ALLOWED_DIVERGENCES = {
    "missing",
    "stopped",
    "image_drift",
    "port_drift",
    "env_drift",
    "host_drift",
    "orphan",
}


def _sorted_unique_ports(value: Any) -> List[int]:
    if not isinstance(value, (list, tuple, set)):
        return []
    ports = set()
    for item in value:
        try:
            parsed = int(item)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            ports.add(parsed)
    return sorted(ports)


def _normalize_resources(value: Any) -> Dict[str, int]:
    if not isinstance(value, dict):
        return {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
    normalized = {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
    for key in ("cpu", "memory_mb", "disk_gb"):
        try:
            parsed = int(value.get(key, 0))
        except (TypeError, ValueError):
            parsed = 0
        normalized[key] = parsed if parsed > 0 else 0
    return normalized


def _normalize_service_context(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    org_id = str(value.get("org_id", "")).strip().lower()
    channel_ids = sorted(
        {
            str(item).strip().lower()
            for item in (value.get("channel_ids") or [])
            if str(item).strip()
        }
    )
    chaincode_ids = sorted(
        {
            str(item).strip().lower()
            for item in (value.get("chaincode_ids") or [])
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


def _normalize_desired_components(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = report.get("normalized_components", [])
    if not isinstance(raw, list):
        return []
    normalized = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "component_id": str(item.get("component_id", "")).strip(),
                "component_type": str(item.get("component_type", "")).strip().lower(),
                "host_id": str(item.get("host_id", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "image": str(item.get("image", "")).strip(),
                "ports": _sorted_unique_ports(item.get("ports", [])),
                "env_profile": str(item.get("env_profile", "")).strip().lower(),
                "storage_profile": str(item.get("storage_profile", "")).strip(),
                "resources": _normalize_resources(item.get("resources", {})),
                "service_context": _normalize_service_context(item.get("service_context", {})),
                "desired_state": str(item.get("desired_state", "")).strip().lower(),
                "criticality": str(item.get("criticality", "")).strip().lower(),
                "reconcile_hints": (
                    dict(item.get("reconcile_hints", {}))
                    if isinstance(item.get("reconcile_hints"), dict)
                    else {}
                ),
            }
        )
    return sorted(
        normalized,
        key=lambda item: (
            str(item.get("host_id", "")),
            str(item.get("component_type", "")),
            str(item.get("component_id", "")),
            str(item.get("name", "")),
        ),
    )


def _normalize_observed_components(observed_state_baseline: Dict[str, Any]) -> List[Dict[str, Any]]:
    hosts = observed_state_baseline.get("hosts", [])
    if not isinstance(hosts, list):
        return []
    observed: List[Dict[str, Any]] = []
    for host in hosts:
        if not isinstance(host, dict):
            continue
        host_id = str(host.get("host_id", "")).strip()
        for component in host.get("observed_components", []) or []:
            if not isinstance(component, dict):
                continue
            observed.append(
                {
                    "host_id": str(component.get("host_id", "")).strip() or host_id,
                    "component_id": str(component.get("component_id", "")).strip(),
                    "component_type": str(component.get("component_type", "")).strip().lower(),
                    "name": str(component.get("name", "")).strip(),
                    "image": str(component.get("image", "")).strip(),
                    "ports": _sorted_unique_ports(component.get("ports", [])),
                    "status": str(component.get("status", "")).strip().lower(),
                    "env_profile": str(component.get("env_profile", "")).strip().lower(),
                    "container_id": str(component.get("container_id", "")).strip(),
                }
            )
    return sorted(
        observed,
        key=lambda item: (
            str(item.get("host_id", "")),
            str(item.get("component_type", "")),
            str(item.get("name", "")),
            str(item.get("component_id", "")),
        ),
    )


def _desired_key(component: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(component.get("component_type", "")).strip().lower(),
        str(component.get("name", "")).strip().lower(),
    )


def _observed_key(component: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(component.get("component_type", "")).strip().lower(),
        str(component.get("name", "")).strip().lower(),
    )


def _pick_observed_for_desired(
    desired: Dict[str, Any],
    observed_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    key = _desired_key(desired)
    candidates = list(observed_by_key.get(key, []))
    if not candidates:
        return None
    desired_host = str(desired.get("host_id", "")).strip()
    same_host = [
        item
        for item in candidates
        if str(item.get("host_id", "")).strip() == desired_host
    ]
    selected_pool = same_host or candidates
    selected = sorted(
        selected_pool,
        key=lambda item: (
            str(item.get("host_id", "")),
            str(item.get("container_id", "")),
            str(item.get("component_id", "")),
        ),
    )[0]
    return selected


def _divergences_for_pair(
    *,
    desired: Dict[str, Any],
    observed: Optional[Dict[str, Any]],
) -> List[str]:
    if observed is None:
        return ["missing"]

    divergences: List[str] = []
    desired_host = str(desired.get("host_id", "")).strip()
    observed_host = str(observed.get("host_id", "")).strip()
    if desired_host and observed_host and desired_host != observed_host:
        divergences.append("host_drift")

    desired_image = str(desired.get("image", "")).strip()
    observed_image = str(observed.get("image", "")).strip()
    if desired_image and observed_image and desired_image != observed_image:
        divergences.append("image_drift")

    desired_ports = _sorted_unique_ports(desired.get("ports", []))
    observed_ports = _sorted_unique_ports(observed.get("ports", []))
    if desired_ports != observed_ports:
        divergences.append("port_drift")

    desired_env = str(desired.get("env_profile", "")).strip().lower()
    observed_env = str(observed.get("env_profile", "")).strip().lower()
    if desired_env and observed_env and desired_env != observed_env:
        divergences.append("env_drift")

    observed_status = str(observed.get("status", "")).strip().lower()
    if observed_status in _STOPPED_STATUSES:
        divergences.append("stopped")

    return sorted(set(item for item in divergences if item in _ALLOWED_DIVERGENCES))


def _policy_decision_and_action(
    *,
    desired_state: str,
    divergences: List[str],
    observed: Optional[Dict[str, Any]],
) -> Tuple[str, str, str]:
    normalized_state = str(desired_state).strip().lower()
    normalized_divergences = sorted(set(str(item).strip().lower() for item in divergences if str(item).strip()))

    if normalized_state in _OBSERVED_STATES:
        if "missing" in normalized_divergences:
            return (
                "observe",
                "noop",
                "Componente nao obrigatorio ausente; politica planned/optional nao exige convergencia ativa.",
            )
        if normalized_divergences:
            return (
                "defer",
                "verify",
                "Componente planned/optional com drift observado; reconciliacao automatica adiada por politica.",
            )
        return (
            "observe",
            "noop",
            "Componente planned/optional sem divergencia relevante.",
        )

    if "missing" in normalized_divergences:
        return (
            "enforce",
            "create",
            "Componente obrigatorio ausente no host alvo.",
        )
    if any(item in normalized_divergences for item in ("host_drift", "image_drift", "port_drift", "env_drift")):
        return (
            "enforce",
            "update",
            "Componente obrigatorio com drift de configuracao/topologia.",
        )
    if "stopped" in normalized_divergences:
        return (
            "enforce",
            "start",
            "Componente obrigatorio presente porem parado.",
        )
    if observed is None:
        return (
            "enforce",
            "create",
            "Componente obrigatorio sem observacao valida.",
        )
    return (
        "enforce",
        "noop",
        "Componente obrigatorio convergente com estado desejado.",
    )


def _recommended_action(policy_decision: str, action: str, divergences: List[str]) -> str:
    divergence_text = ", ".join(divergences) if divergences else "none"
    if policy_decision == "observe":
        return f"Monitorar componente; sem acao obrigatoria imediata (divergences={divergence_text})."
    if policy_decision == "defer":
        return (
            "Submeter drift para aprovacao operacional/manual antes de aplicar alteracoes "
            f"(divergences={divergence_text})."
        )
    if action == "create":
        return "Provisionar componente no host desejado e registrar inventario atualizado."
    if action == "update":
        return "Aplicar reconciliacao de configuracao (imagem/portas/env/host) de forma idempotente."
    if action == "start":
        return "Iniciar componente parado e validar saude apos start."
    if action == "noop":
        return "Nenhuma acao corretiva necessaria."
    return "Executar verificacao tecnica do componente e revisar divergencias."


def _orphan_action(observed: Dict[str, Any]) -> Dict[str, Any]:
    host_id = str(observed.get("host_id", "")).strip()
    component_id = str(observed.get("component_id", "")).strip()
    component_type = str(observed.get("component_type", "")).strip().lower()
    name = str(observed.get("name", "")).strip()
    return {
        "target_kind": "orphan_component",
        "host_id": host_id,
        "component_id": component_id or name,
        "component_type": component_type,
        "name": name,
        "desired_state": "orphan",
        "criticality": "supporting",
        "divergences": ["orphan"],
        "policy_decision": "defer",
        "action": "verify",
        "cause": "Componente observado nao declarado no manifesto desejado.",
        "recommended_action": (
            "Avaliar remoção controlada ou incorporação no manifesto antes de nova execução."
        ),
        "observed": {
            "host_id": host_id,
            "status": str(observed.get("status", "")).strip().lower(),
            "image": str(observed.get("image", "")).strip(),
            "ports": _sorted_unique_ports(observed.get("ports", [])),
            "env_profile": str(observed.get("env_profile", "")).strip().lower(),
        },
    }


@dataclass(frozen=True)
class ReconciliationPlanResult:
    plan: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_reconciliation_plan(
    *,
    run: PipelineRun,
    org_runtime_manifest_report: Optional[Dict[str, Any]],
    observed_state_baseline: Dict[str, Any],
    execution_generated_at: str,
) -> Dict[str, Any]:
    report = (
        dict(org_runtime_manifest_report)
        if isinstance(org_runtime_manifest_report, dict)
        else {}
    )
    desired_components = _normalize_desired_components(report)
    observed_components = _normalize_observed_components(observed_state_baseline)
    observed_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for item in observed_components:
        observed_by_key.setdefault(_observed_key(item), []).append(item)

    matched_observed_signatures = set()
    reconcile_actions: List[Dict[str, Any]] = []
    divergence_summary = {code: 0 for code in sorted(_ALLOWED_DIVERGENCES)}

    for desired in desired_components:
        observed = _pick_observed_for_desired(desired, observed_by_key)
        if observed is not None:
            signature = (
                str(observed.get("host_id", "")).strip(),
                str(observed.get("component_type", "")).strip().lower(),
                str(observed.get("name", "")).strip().lower(),
                str(observed.get("container_id", "")).strip(),
            )
            matched_observed_signatures.add(signature)

        divergences = _divergences_for_pair(
            desired=desired,
            observed=observed,
        )
        for divergence in divergences:
            if divergence in divergence_summary:
                divergence_summary[divergence] += 1

        desired_state = str(desired.get("desired_state", "")).strip().lower()
        policy_decision, action, cause = _policy_decision_and_action(
            desired_state=desired_state,
            divergences=divergences,
            observed=observed,
        )
        reconcile_actions.append(
            {
                "target_kind": "desired_component",
                "host_id": str(desired.get("host_id", "")).strip(),
                "component_id": str(desired.get("component_id", "")).strip(),
                "component_type": str(desired.get("component_type", "")).strip().lower(),
                "name": str(desired.get("name", "")).strip(),
                "desired_state": desired_state,
                "criticality": str(desired.get("criticality", "")).strip().lower(),
                "divergences": divergences,
                "policy_decision": policy_decision,
                "action": action,
                "cause": cause,
                "recommended_action": _recommended_action(policy_decision, action, divergences),
                "desired": {
                    "host_id": str(desired.get("host_id", "")).strip(),
                    "image": str(desired.get("image", "")).strip(),
                    "ports": _sorted_unique_ports(desired.get("ports", [])),
                    "env_profile": str(desired.get("env_profile", "")).strip().lower(),
                    "storage_profile": str(desired.get("storage_profile", "")).strip(),
                    "resources": _normalize_resources(desired.get("resources", {})),
                    "service_context": _normalize_service_context(desired.get("service_context", {})),
                    "reconcile_hints": (
                        dict(desired.get("reconcile_hints", {}))
                        if isinstance(desired.get("reconcile_hints"), dict)
                        else {}
                    ),
                },
                "observed": (
                    {
                        "host_id": str(observed.get("host_id", "")).strip(),
                        "status": str(observed.get("status", "")).strip().lower(),
                        "image": str(observed.get("image", "")).strip(),
                        "ports": _sorted_unique_ports(observed.get("ports", [])),
                        "env_profile": str(observed.get("env_profile", "")).strip().lower(),
                    }
                    if isinstance(observed, dict)
                    else None
                ),
            }
        )

    orphan_actions: List[Dict[str, Any]] = []
    for observed in observed_components:
        signature = (
            str(observed.get("host_id", "")).strip(),
            str(observed.get("component_type", "")).strip().lower(),
            str(observed.get("name", "")).strip().lower(),
            str(observed.get("container_id", "")).strip(),
        )
        if signature in matched_observed_signatures:
            continue
        divergence_summary["orphan"] += 1
        orphan_actions.append(_orphan_action(observed))

    reconcile_actions = sorted(
        reconcile_actions + orphan_actions,
        key=lambda item: (
            str(item.get("target_kind", "")),
            str(item.get("host_id", "")),
            str(item.get("component_type", "")),
            str(item.get("component_id", "")),
            str(item.get("name", "")),
        ),
    )
    action_summary: Dict[str, int] = {}
    for action_item in reconcile_actions:
        action = str(action_item.get("action", "")).strip().lower()
        if not action:
            continue
        action_summary[action] = action_summary.get(action, 0) + 1

    resolved_generated_at = str(execution_generated_at).strip() or run.started_at or utc_now_iso()
    return {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "manifest_fingerprint": str(report.get("manifest_fingerprint", "")).strip().lower(),
        "source_blueprint_fingerprint": str(report.get("source_blueprint_fingerprint", "")).strip().lower(),
        "policy": {
            "enforced_states": sorted(_ENFORCED_STATES),
            "observed_states": sorted(_OBSERVED_STATES),
            "orphan_policy": "defer",
        },
        "desired_component_count": len(desired_components),
        "observed_component_count": len(observed_components),
        "reconcile_action_count": len(reconcile_actions),
        "divergence_summary": divergence_summary,
        "action_summary": dict(sorted(action_summary.items())),
        "reconcile_actions": reconcile_actions,
        "generated_at": resolved_generated_at,
    }
