from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional, Set, Tuple

from .blueprint_schema import BlueprintValidationResult
from .pipeline_contract import PipelineRun, utc_now_iso
from .pipeline_state_store import PipelineStateStore, StageCheckpoint, payload_sha256


@dataclass(frozen=True)
class VerifyIssue:
    level: str
    code: str
    path: str
    message: str
    recommendation: str
    critical: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VerifyExecutionResult:
    verify_report: Dict[str, Any]
    pipeline_report: Dict[str, Any]
    blocked: bool
    verdict: str
    artifacts: Dict[str, str]
    issues: List[VerifyIssue]
    checkpoint: Optional[StageCheckpoint] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verify_report": self.verify_report,
            "pipeline_report": self.pipeline_report,
            "blocked": self.blocked,
            "verdict": self.verdict,
            "artifacts": self.artifacts,
            "issues": [issue.to_dict() for issue in self.issues],
            "checkpoint": self.checkpoint.to_dict() if self.checkpoint else None,
        }


def _sort_issues(issues: List[VerifyIssue]) -> List[VerifyIssue]:
    return sorted(
        issues,
        key=lambda item: (
            item.level,
            item.code,
            item.path,
            item.message,
            item.recommendation,
            int(item.critical),
        ),
    )


def _configure_completed_checkpoint(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
) -> Optional[StageCheckpoint]:
    key = run.idempotency_key("configure")
    checkpoint = state_store.load_checkpoint(run.run_id, "configure", key)
    if checkpoint and checkpoint.stage_status == "completed":
        return checkpoint
    return None


def _verify_completed_checkpoint(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
) -> Optional[StageCheckpoint]:
    key = run.idempotency_key("verify")
    checkpoint = state_store.load_checkpoint(run.run_id, "verify", key)
    if checkpoint and checkpoint.stage_status == "completed":
        return checkpoint
    return None


def _node_index_from_blueprint(blueprint_validation: BlueprintValidationResult) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for node in blueprint_validation.normalized_nodes:
        node_id = str((node or {}).get("node_id", "")).strip()
        if node_id:
            index[node_id] = node
    return index


def _inventory_index_by_node(technical_inventory: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for host in technical_inventory.get("hosts", []) or []:
        host_ref = str((host or {}).get("host_ref", "")).strip()
        for component in (host or {}).get("configured_components", []) or []:
            node_id = str((component or {}).get("node_id", "")).strip()
            if not node_id:
                continue
            payload = dict(component or {})
            payload["host_ref"] = host_ref
            index[node_id] = payload
    return index


def _profiles_by_org(connection_profiles: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    profiles = connection_profiles.get("profiles", {})
    if isinstance(profiles, dict):
        return {str(org_id): dict(profile or {}) for org_id, profile in profiles.items()}
    return {}


def _parse_utc_iso(timestamp: str) -> Optional[datetime]:
    value = str(timestamp).strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _health_checks(
    *,
    expected_nodes: Dict[str, Dict[str, Any]],
    inventory_nodes: Dict[str, Dict[str, Any]],
    issues: List[VerifyIssue],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    for node_id in sorted(expected_nodes.keys()):
        expected = expected_nodes[node_id]
        component = inventory_nodes.get(node_id)
        node_type = str(expected.get("node_type", "")).strip()
        host_ref = str(expected.get("host_ref", "")).strip()

        if component is None:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_component_missing",
                    path=f"technical_inventory.hosts[].configured_components.{node_id}",
                    message=f"Componente esperado '{node_id}' não encontrado no inventário técnico final.",
                    recommendation="Executar configure novamente e validar persistência do inventário técnico.",
                    critical=True,
                )
            )
            checks.append(
                {
                    "node_id": node_id,
                    "node_type": node_type,
                    "host_ref": host_ref,
                    "status": "unhealthy",
                    "reason": "missing_component",
                }
            )
            continue

        endpoint = str(component.get("endpoint", "")).strip()
        healthy = bool(endpoint)
        if not healthy:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_component_endpoint_missing",
                    path=f"technical_inventory.hosts[].configured_components.{node_id}.endpoint",
                    message=f"Componente '{node_id}' sem endpoint efetivo para conectividade.",
                    recommendation="Reaplicar baseline em configure para materializar endpoint do componente.",
                    critical=True,
                )
            )

        checks.append(
            {
                "node_id": node_id,
                "node_type": node_type,
                "host_ref": str(component.get("host_ref", host_ref)).strip(),
                "status": "healthy" if healthy else "unhealthy",
                "reason": "ok" if healthy else "endpoint_missing",
                "endpoint": endpoint,
            }
        )
    return checks


def _connectivity_checks(
    *,
    blueprint_validation: BlueprintValidationResult,
    profiles: Dict[str, Dict[str, Any]],
    issues: List[VerifyIssue],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    for channel in sorted(
        blueprint_validation.normalized_channels,
        key=lambda item: str((item or {}).get("channel_id", "")),
    ):
        channel_id = str((channel or {}).get("channel_id", "")).strip()
        members = sorted(str(member).strip() for member in ((channel or {}).get("members") or []) if str(member).strip())
        for org_id in members:
            profile = profiles.get(org_id, {})
            peers = profile.get("peers", []) if isinstance(profile, dict) else []
            reachable = any(str((peer or {}).get("endpoint", "")).strip() for peer in peers)
            if not reachable:
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_channel_member_peer_unreachable",
                        path=f"channels.{channel_id}.members.{org_id}",
                        message=(
                            f"Canal '{channel_id}' possui membro '{org_id}' sem peer alcançável no profile de conexão."
                        ),
                        recommendation="Validar peers da organização e regenerar connection profiles na etapa configure.",
                        critical=True,
                    )
                )

            checks.append(
                {
                    "channel_id": channel_id,
                    "org_id": org_id,
                    "status": "reachable" if reachable else "unreachable",
                    "reason": "ok" if reachable else "no_peer_endpoint",
                }
            )
    return checks


def _inventory_consistency_checks(
    *,
    expected_nodes: Dict[str, Dict[str, Any]],
    inventory_nodes: Dict[str, Dict[str, Any]],
    issues: List[VerifyIssue],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    expected_node_ids = set(expected_nodes.keys())
    inventory_node_ids = set(inventory_nodes.keys())

    for node_id in sorted(expected_node_ids):
        expected = expected_nodes[node_id]
        found = inventory_nodes.get(node_id)
        if not found:
            checks.append(
                {
                    "node_id": node_id,
                    "status": "inconsistent",
                    "reason": "missing_in_inventory",
                }
            )
            continue

        expected_host = str(expected.get("host_ref", "")).strip()
        expected_org = str(expected.get("org_id", "")).strip()
        expected_type = str(expected.get("node_type", "")).strip()
        expected_ports = sorted(int(port) for port in (expected.get("ports") or []) if str(port).isdigit())

        found_host = str(found.get("host_ref", "")).strip()
        found_org = str(found.get("org_id", "")).strip()
        found_type = str(found.get("node_type", "")).strip()
        found_ports = sorted(int(port) for port in (found.get("ports") or []) if str(port).isdigit())

        consistent = (
            expected_host == found_host
            and expected_org == found_org
            and expected_type == found_type
            and expected_ports == found_ports
        )
        if not consistent:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_inventory_node_mismatch",
                    path=f"technical_inventory.hosts[].configured_components.{node_id}",
                    message=(
                        f"Componente '{node_id}' diverge do blueprint normalizado (host/org/tipo/portas)."
                    ),
                    recommendation="Sincronizar inventário técnico com blueprint e reaplicar configure antes de avançar.",
                    critical=True,
                )
            )

        checks.append(
            {
                "node_id": node_id,
                "status": "consistent" if consistent else "inconsistent",
                "reason": "ok" if consistent else "topology_mismatch",
            }
        )

    for extra_node in sorted(inventory_node_ids - expected_node_ids):
        issues.append(
            VerifyIssue(
                level="warning",
                code="verify_inventory_extra_node",
                path=f"technical_inventory.hosts[].configured_components.{extra_node}",
                message=f"Inventário final contém nó extra '{extra_node}' não declarado no blueprint.",
                recommendation="Revisar inventário e remover componentes órfãos para manter rastreabilidade.",
                critical=False,
            )
        )
        checks.append(
            {
                "node_id": extra_node,
                "status": "inconsistent",
                "reason": "extra_inventory_node",
            }
        )

    return sorted(checks, key=lambda item: (item.get("node_id", ""), item.get("status", ""), item.get("reason", "")))


def _crypto_inventory_consistency_checks(
    *,
    run: PipelineRun,
    blueprint_validation: BlueprintValidationResult,
    technical_inventory: Dict[str, Any],
    inventory_nodes: Dict[str, Dict[str, Any]],
    issues: List[VerifyIssue],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    crypto_inventory = (
        technical_inventory.get("crypto_inventory")
        if isinstance(technical_inventory.get("crypto_inventory"), dict)
        else {}
    )

    if not crypto_inventory:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_inventory_missing",
                path="technical_inventory.crypto_inventory",
                message="Inventário criptográfico canônico ausente no inventário técnico final.",
                recommendation="Executar configure para gerar crypto-inventory.json antes de avançar.",
                critical=True,
            )
        )
        return [{"status": "inconsistent", "reason": "missing_crypto_inventory"}]

    if str(crypto_inventory.get("run_id", "")).strip() != run.run_id:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_inventory_run_mismatch",
                path="technical_inventory.crypto_inventory.run_id",
                message="crypto_inventory.run_id diverge do run aplicado.",
                recommendation="Regenerar crypto-inventory.json para o run corrente.",
                critical=True,
            )
        )
    if str(crypto_inventory.get("change_id", "")).strip() != run.change_id:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_inventory_change_mismatch",
                path="technical_inventory.crypto_inventory.change_id",
                message="crypto_inventory.change_id diverge do contexto de execução.",
                recommendation="Garantir correlação do inventário com o change_id ativo.",
                critical=True,
            )
        )
    if str(crypto_inventory.get("blueprint_fingerprint", "")).strip() != run.blueprint_fingerprint:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_inventory_fingerprint_mismatch",
                path="technical_inventory.crypto_inventory.blueprint_fingerprint",
                message="crypto_inventory.blueprint_fingerprint diverge do blueprint aplicado.",
                recommendation="Reexecutar configure/verify com o mesmo blueprint normalizado.",
                critical=True,
            )
        )
    if str(crypto_inventory.get("resolved_schema_version", "")).strip() != run.resolved_schema_version:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_inventory_schema_mismatch",
                path="technical_inventory.crypto_inventory.resolved_schema_version",
                message="crypto_inventory.resolved_schema_version diverge da versão resolvida do run.",
                recommendation="Regerar inventário criptográfico para a versão de schema ativa.",
                critical=True,
            )
        )

    entities = crypto_inventory.get("entities") if isinstance(crypto_inventory.get("entities"), dict) else {}
    entity_indices: Dict[str, Set[str]] = {}
    certificate_by_id: Dict[str, Dict[str, Any]] = {}
    for entity_name in ("cas", "certificates", "keys", "bundles"):
        raw_items = entities.get(entity_name) if isinstance(entities.get(entity_name), list) else []
        stable_ids: Set[str] = set()
        for idx, item in enumerate(raw_items):
            if not isinstance(item, dict):
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_crypto_inventory_entity_invalid",
                        path=f"technical_inventory.crypto_inventory.entities.{entity_name}[{idx}]",
                        message=f"Entidade inválida em crypto_inventory.entities.{entity_name}.",
                        recommendation="Corrigir serialização do inventário criptográfico.",
                        critical=True,
                    )
                )
                continue
            stable_id = str(item.get("stable_id", "")).strip()
            fingerprint = str(item.get("fingerprint", "")).strip()
            if not stable_id:
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_crypto_inventory_stable_id_missing",
                        path=f"technical_inventory.crypto_inventory.entities.{entity_name}[{idx}].stable_id",
                        message=f"stable_id ausente em entidade {entity_name}.",
                        recommendation="Garantir identificador estável por item do inventário.",
                        critical=True,
                    )
                )
            elif stable_id in stable_ids:
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_crypto_inventory_stable_id_duplicated",
                        path=f"technical_inventory.crypto_inventory.entities.{entity_name}[{idx}].stable_id",
                        message=f"stable_id duplicado detectado em {entity_name}: '{stable_id}'.",
                        recommendation="Eliminar duplicidade lógica no inventário criptográfico.",
                        critical=True,
                    )
                )
            else:
                stable_ids.add(stable_id)
                if entity_name == "certificates":
                    certificate_by_id[stable_id] = item

            if len(fingerprint) != 64:
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_crypto_inventory_fingerprint_invalid",
                        path=f"technical_inventory.crypto_inventory.entities.{entity_name}[{idx}].fingerprint",
                        message=f"Fingerprint inválido em {entity_name} para stable_id '{stable_id}'.",
                        recommendation="Regerar fingerprints determinísticos com SHA-256.",
                        critical=True,
                    )
                )
        entity_indices[entity_name] = stable_ids

    links = crypto_inventory.get("links") if isinstance(crypto_inventory.get("links"), dict) else {}
    node_links = links.get("nodes") if isinstance(links.get("nodes"), list) else []
    node_link_by_node: Dict[str, Dict[str, Any]] = {}
    for item in node_links:
        if not isinstance(item, dict):
            continue
        node_id = str(item.get("node_id", "")).strip()
        if node_id:
            node_link_by_node[node_id] = item

    manifests_by_node: Dict[str, Set[str]] = {}
    for host in technical_inventory.get("hosts", []) or []:
        host_ref = str((host or {}).get("host_ref", "")).strip()
        for manifest in (host or {}).get("msp_tls_manifests", []) or []:
            node_id = str((manifest or {}).get("node_id", "")).strip()
            manifest_hash = str((manifest or {}).get("manifest_hash", "")).strip()
            if not (host_ref and node_id and manifest_hash):
                continue
            manifests_by_node.setdefault(node_id, set()).add(manifest_hash)

    for node_id in sorted(inventory_nodes.keys()):
        component = inventory_nodes[node_id]
        link = node_link_by_node.get(node_id)
        if not isinstance(link, dict):
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_inventory_node_link_missing",
                    path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}",
                    message=f"Vínculo de nó ausente no crypto_inventory para '{node_id}'.",
                    recommendation="Regerar links por nó no inventário criptográfico canônico.",
                    critical=True,
                )
            )
            checks.append({"node_id": node_id, "status": "inconsistent", "reason": "missing_node_link"})
            continue

        expected_host = str(component.get("host_ref", "")).strip()
        expected_org = str(component.get("org_id", "")).strip().lower()
        expected_type = str(component.get("node_type", "")).strip().lower()

        link_host = str(link.get("host_ref", "")).strip()
        link_org = str(link.get("org_id", "")).strip().lower()
        link_type = str(link.get("node_type", "")).strip().lower()
        if (expected_host != link_host) or (expected_org != link_org) or (expected_type != link_type):
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_inventory_node_link_mismatch",
                    path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}",
                    message=f"Vínculo de nó divergente no crypto_inventory para '{node_id}'.",
                    recommendation="Sincronizar links por nó com o estado aplicado em configure.",
                    critical=True,
                )
            )

        expected_manifest_hashes = sorted(manifests_by_node.get(node_id, set()))
        found_manifest_hashes = sorted(
            {
                str(item).strip()
                for item in (link.get("manifest_hashes") or [])
                if str(item).strip()
            }
        )
        if expected_manifest_hashes != found_manifest_hashes:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_inventory_manifest_mismatch",
                    path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}.manifest_hashes",
                    message=(
                        f"Manifestos do crypto_inventory divergem do estado aplicado para '{node_id}'."
                    ),
                    recommendation="Regerar crypto_inventory após materialização MSP/TLS convergente.",
                    critical=True,
                )
            )

        missing_refs: List[str] = []
        for cert_id in (link.get("certificate_ids") or []):
            cert_key = str(cert_id).strip()
            if cert_key and cert_key not in entity_indices.get("certificates", set()):
                missing_refs.append(f"certificate:{cert_key}")
        eligible_certificate_ids = [
            str(item).strip()
            for item in (link.get("eligible_certificate_ids") or [])
            if str(item).strip()
        ]
        revoked_certificate_ids = [
            str(item).strip()
            for item in (link.get("revoked_certificate_ids") or [])
            if str(item).strip()
        ]
        overlap_ids = sorted(set(eligible_certificate_ids).intersection(set(revoked_certificate_ids)))
        if overlap_ids:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_inventory_revocation_conflict",
                    path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}",
                    message=(
                        f"Conflito entre certificados elegíveis/revogados para '{node_id}': {overlap_ids}."
                    ),
                    recommendation="Remover certificado revogado da lista elegível antes de liberar execução.",
                    critical=True,
                )
            )
        for cert_key in eligible_certificate_ids:
            cert_payload = certificate_by_id.get(cert_key, {})
            status = str(cert_payload.get("status", "")).strip().lower()
            revoked = bool(cert_payload.get("revoked", False)) or status in {"revoked", "compromised"}
            eligible = bool(cert_payload.get("eligible_for_validation", False))
            if cert_key not in entity_indices.get("certificates", set()):
                missing_refs.append(f"eligible_certificate:{cert_key}")
            elif revoked or (status != "active") or not eligible:
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_crypto_inventory_revoked_eligible",
                        path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}.eligible_certificate_ids",
                        message=(
                            f"Certificado não elegível presente em eligible_certificate_ids para '{node_id}': '{cert_key}'."
                        ),
                        recommendation="Garantir que certificados revogados não permaneçam elegíveis para validação.",
                        critical=True,
                    )
                )
        for cert_key in revoked_certificate_ids:
            cert_payload = certificate_by_id.get(cert_key, {})
            status = str(cert_payload.get("status", "")).strip().lower()
            revoked = bool(cert_payload.get("revoked", False)) or status in {"revoked", "compromised"}
            if cert_key not in entity_indices.get("certificates", set()):
                missing_refs.append(f"revoked_certificate:{cert_key}")
            elif not revoked:
                issues.append(
                    VerifyIssue(
                        level="error",
                        code="verify_crypto_inventory_revoked_list_inconsistent",
                        path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}.revoked_certificate_ids",
                        message=(
                            f"Certificado listado como revogado sem estado revogado para '{node_id}': '{cert_key}'."
                        ),
                        recommendation="Sincronizar status de certificados com listas de elegibilidade/revogação.",
                        critical=True,
                    )
                )
        for key_id in (link.get("key_ids") or []):
            key_key = str(key_id).strip()
            if key_key and key_key not in entity_indices.get("keys", set()):
                missing_refs.append(f"key:{key_key}")
        for bundle_id in (link.get("bundle_ids") or []):
            bundle_key = str(bundle_id).strip()
            if bundle_key and bundle_key not in entity_indices.get("bundles", set()):
                missing_refs.append(f"bundle:{bundle_key}")
        if missing_refs:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_inventory_reference_missing",
                    path=f"technical_inventory.crypto_inventory.links.nodes.{node_id}",
                    message=f"Referências ausentes no inventário canônico para '{node_id}': {sorted(missing_refs)}.",
                    recommendation="Corrigir consistência entre entities e links no crypto_inventory.",
                    critical=True,
                )
            )

        checks.append(
            {
                "node_id": node_id,
                "status": "consistent" if expected_manifest_hashes == found_manifest_hashes else "inconsistent",
                "reason": "ok" if expected_manifest_hashes == found_manifest_hashes else "manifest_mismatch",
            }
        )

    expected_channel_links: Set[Tuple[str, str]] = set()
    for channel in blueprint_validation.normalized_channels:
        channel_id = str((channel or {}).get("channel_id", "")).strip().lower()
        for org_id in ((channel or {}).get("members") or []):
            member = str(org_id).strip().lower()
            if channel_id and member:
                expected_channel_links.add((channel_id, member))

    found_channel_links: Set[Tuple[str, str]] = set()
    for entry in (links.get("channels") or []):
        if not isinstance(entry, dict):
            continue
        channel_id = str(entry.get("channel_id", "")).strip().lower()
        org_id = str(entry.get("org_id", "")).strip().lower()
        if channel_id and org_id:
            found_channel_links.add((channel_id, org_id))
    for channel_id, org_id in sorted(expected_channel_links - found_channel_links):
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_inventory_channel_link_missing",
                path=f"technical_inventory.crypto_inventory.links.channels.{channel_id}.{org_id}",
                message=f"Vínculo de canal ausente no crypto_inventory: canal '{channel_id}', org '{org_id}'.",
                recommendation="Sincronizar links de canal no inventário criptográfico.",
                critical=True,
            )
        )

    return sorted(checks, key=lambda item: (item.get("node_id", ""), item.get("status", ""), item.get("reason", "")))


def _crypto_revocation_consistency_checks(
    *,
    configure_report: Dict[str, Any],
    network_manifests: Dict[str, Any],
    technical_inventory: Dict[str, Any],
    issues: List[VerifyIssue],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    report_payload = (
        configure_report.get("crypto_revocation")
        if isinstance(configure_report.get("crypto_revocation"), dict)
        else {}
    )
    if not report_payload:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_revocation_report_missing",
                path="configure_report.crypto_revocation",
                message="Resumo de revogação/CRL ausente no configure_report.",
                recommendation="Executar configure com suporte de revogação A1.4.7 habilitado.",
                critical=True,
            )
        )
        return [{"status": "inconsistent", "reason": "missing_revocation_report"}]

    decision_trace = [item for item in (report_payload.get("decision_trace") or []) if isinstance(item, dict)]
    if not decision_trace:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_revocation_decision_trace_missing",
                path="configure_report.crypto_revocation.decision_trace",
                message="Trilha de decisão de revogação ausente.",
                recommendation="Registrar trilha determinística de bloqueio/liberação para revogação/CRL.",
                critical=True,
            )
        )
    for idx, item in enumerate(decision_trace):
        rule_code = str(item.get("rule_code", "")).strip()
        decision = str(item.get("decision", "")).strip().lower()
        technical_reason = str(item.get("technical_reason", "")).strip()
        evidence_hash = str(item.get("evidence_hash", "")).strip()
        if not (rule_code and decision in {"allowed", "blocked"} and technical_reason and len(evidence_hash) == 64):
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_revocation_decision_trace_invalid",
                    path=f"configure_report.crypto_revocation.decision_trace[{idx}]",
                    message="Entrada inválida na trilha de decisão de revogação.",
                    recommendation="Fornecer rule_code/decision/technical_reason/evidence_hash válidos e reproduzíveis.",
                    critical=True,
                )
            )

    propagation_failures = [item for item in (report_payload.get("propagation_failures") or []) if isinstance(item, dict)]
    if propagation_failures:
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_crypto_crl_propagation_failed",
                path="configure_report.crypto_revocation.propagation_failures",
                message="Falha de propagação de CRL registrada para etapa sensível.",
                recommendation="Conter incidente, corrigir propagação CRL e reexecutar configure/verify.",
                critical=True,
            )
        )

    manifests_revocation = (
        network_manifests.get("crypto_revocation_impacts")
        if isinstance(network_manifests.get("crypto_revocation_impacts"), dict)
        else {}
    )
    org_crls = [item for item in (report_payload.get("org_crls") or []) if isinstance(item, dict)]
    manifest_org_crls = [item for item in (manifests_revocation.get("org_crls") or []) if isinstance(item, dict)]
    manifest_crl_index = {
        (str(item.get("org_id", "")).strip().lower(), str(item.get("crl_hash", "")).strip()): item
        for item in manifest_org_crls
        if str(item.get("org_id", "")).strip() and str(item.get("crl_hash", "")).strip()
    }

    host_manifests_by_org: Dict[str, int] = {}
    host_manifests_with_crl_by_org: Dict[str, int] = {}
    latest_manifest_crl_issued_at_by_org: Dict[str, datetime] = {}
    for host in technical_inventory.get("hosts", []) or []:
        for manifest in (host or {}).get("msp_tls_manifests", []) or []:
            org_id = str((manifest or {}).get("org_id", "")).strip().lower()
            if not org_id:
                continue
            host_manifests_by_org[org_id] = host_manifests_by_org.get(org_id, 0) + 1
            revocation = manifest.get("revocation") if isinstance(manifest.get("revocation"), dict) else {}
            if str(revocation.get("crl_hash", "")).strip():
                host_manifests_with_crl_by_org[org_id] = host_manifests_with_crl_by_org.get(org_id, 0) + 1
            issued_at = _parse_utc_iso(str(revocation.get("issued_at", "")))
            if issued_at is not None:
                current = latest_manifest_crl_issued_at_by_org.get(org_id)
                if current is None or issued_at > current:
                    latest_manifest_crl_issued_at_by_org[org_id] = issued_at

    for item in org_crls:
        org_id = str(item.get("org_id", "")).strip().lower()
        crl_hash = str(item.get("crl_hash", "")).strip()
        if not (org_id and crl_hash):
            continue
        manifest_entry = manifest_crl_index.get((org_id, crl_hash))
        manifests_total = int(host_manifests_by_org.get(org_id, 0))
        manifests_with_crl = int(host_manifests_with_crl_by_org.get(org_id, 0))
        report_issued_at = _parse_utc_iso(str(item.get("issued_at", "")))
        manifest_latest_issued_at = latest_manifest_crl_issued_at_by_org.get(org_id)
        crl_outdated = (
            report_issued_at is not None
            and manifest_latest_issued_at is not None
            and manifest_latest_issued_at < report_issued_at
        )
        consistent = bool(manifest_entry) and (manifests_total == 0 or manifests_with_crl >= manifests_total)
        if not consistent:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_crl_manifest_incomplete",
                    path=f"network_manifests.crypto_revocation_impacts.org_crls.{org_id}",
                    message=f"Propagação CRL incompleta para organização '{org_id}'.",
                    recommendation="Propagar CRL para todos os manifests MSP/TLS e sincronizar network manifests.",
                    critical=True,
                )
            )
        if crl_outdated:
            issues.append(
                VerifyIssue(
                    level="error",
                    code="verify_crypto_crl_outdated",
                    path=f"technical_inventory.hosts[].msp_tls_manifests[].revocation.issued_at.{org_id}",
                    message=f"CRL desatualizada em manifests para organização '{org_id}'.",
                    recommendation="Repropagar a CRL mais recente para MSP/gateway antes de liberar execução.",
                    critical=True,
                )
            )
        checks.append(
            {
                "org_id": org_id,
                "crl_hash": crl_hash,
                "status": "consistent" if (consistent and not crl_outdated) else "inconsistent",
                "reason": (
                    "ok" if (consistent and not crl_outdated)
                    else ("crl_outdated" if crl_outdated else "crl_propagation_incomplete")
                ),
            }
        )

    if not checks:
        checks.append({"org_id": "*", "status": "consistent", "reason": "no_revocation_events"})

    return sorted(checks, key=lambda item: (item.get("org_id", ""), item.get("status", ""), item.get("reason", "")))


def _verdict_from_issues(
    *,
    issues: List[VerifyIssue],
    precondition_blocked: bool,
    partial_mode: bool,
) -> str:
    if precondition_blocked:
        return "blocked"

    errors = [issue for issue in issues if issue.level == "error"]
    warnings = [issue for issue in issues if issue.level == "warning"]
    if errors:
        return "failed"
    if warnings:
        return "partial"
    if partial_mode:
        return "partial"
    return "success"


def _deterministic_recommendations(issues: List[VerifyIssue]) -> List[str]:
    return sorted({issue.recommendation for issue in issues if str(issue.recommendation).strip()})


def run_verify_stage(
    *,
    run: PipelineRun,
    blueprint_validation: BlueprintValidationResult,
    connection_profiles: Dict[str, Any],
    network_manifests: Dict[str, Any],
    technical_inventory: Dict[str, Any],
    configure_report: Dict[str, Any],
    allow_partial_verification: bool = False,
    partial_verification_reason: str = "",
    state_store: Optional[PipelineStateStore] = None,
    executor: str = "verify-engine",
    attempt: int = 1,
) -> VerifyExecutionResult:
    if not blueprint_validation.valid:
        raise ValueError("verify exige blueprint válido (gate A1.2).")
    if attempt <= 0:
        raise ValueError("attempt deve ser maior que 0.")
    if str(configure_report.get("stage", "")).strip().lower() != "configure":
        raise ValueError("configure_report inválido: stage esperado é 'configure'.")

    if state_store is not None:
        completed_checkpoint = _verify_completed_checkpoint(state_store=state_store, run=run)
        if completed_checkpoint is not None:
            artifacts: Dict[str, str] = {}
            verify_path = state_store.stage_artifacts_dir(run.run_id, "verify") / "verify-report.json"
            pipeline_path = state_store.stage_artifacts_dir(run.run_id, "verify") / "pipeline-report.json"

            verify_report = {
                "run_id": run.run_id,
                "change_id": run.change_id,
                "stage": "verify",
                "blocked": False,
                "verdict": "success",
                "reexecution": "skipped_completed_checkpoint",
                "issues": [],
                "generated_at": utc_now_iso(),
            }
            pipeline_report = {
                "run_id": run.run_id,
                "change_id": run.change_id,
                "stage": "verify",
                "final_result": "success",
                "generated_at": utc_now_iso(),
            }

            if verify_path.exists():
                verify_report = json.loads(verify_path.read_text(encoding="utf-8"))
                verify_report["reexecution"] = "skipped_completed_checkpoint"
                artifacts["verify_report"] = str(verify_path)
            if pipeline_path.exists():
                pipeline_report = json.loads(pipeline_path.read_text(encoding="utf-8"))
                pipeline_report["reexecution"] = "skipped_completed_checkpoint"
                artifacts["pipeline_report"] = str(pipeline_path)

            verdict = str(verify_report.get("verdict", "success")).strip().lower() or "success"
            blocked = verdict == "blocked"
            return VerifyExecutionResult(
                verify_report=verify_report,
                pipeline_report=pipeline_report,
                blocked=blocked,
                verdict=verdict,
                artifacts=artifacts,
                issues=[],
                checkpoint=completed_checkpoint,
            )

    issues: List[VerifyIssue] = []
    expected_nodes = _node_index_from_blueprint(blueprint_validation)
    inventory_nodes = _inventory_index_by_node(technical_inventory)
    profiles = _profiles_by_org(connection_profiles)

    configure_ready = bool(configure_report.get("ready_for_verify", False)) and not bool(configure_report.get("blocked", False))
    precondition_blocked = False
    if not configure_ready and not allow_partial_verification:
        precondition_blocked = True
        issues.append(
            VerifyIssue(
                level="error",
                code="verify_precondition_configure_not_completed",
                path="configure_report.ready_for_verify",
                message="Etapa verify bloqueada: configure não concluído e política de verificação parcial não habilitada.",
                recommendation="Concluir configure com sucesso ou habilitar política explícita de verificação parcial.",
                critical=True,
            )
        )
    elif not configure_ready and allow_partial_verification:
        reason = str(partial_verification_reason).strip() or "policy_explicit_partial_verification"
        issues.append(
            VerifyIssue(
                level="warning",
                code="verify_partial_policy_applied",
                path="configure_report.ready_for_verify",
                message=(
                    "Verificação parcial habilitada por política explícita com configure não concluído integralmente."
                ),
                recommendation=f"Registrar justificativa operacional da verificação parcial: {reason}.",
                critical=False,
            )
        )

    health_checks: List[Dict[str, Any]] = []
    connectivity_checks: List[Dict[str, Any]] = []
    inventory_consistency: List[Dict[str, Any]] = []
    crypto_inventory_consistency: List[Dict[str, Any]] = []
    crypto_revocation_consistency: List[Dict[str, Any]] = []
    if not precondition_blocked:
        health_checks = _health_checks(
            expected_nodes=expected_nodes,
            inventory_nodes=inventory_nodes,
            issues=issues,
        )
        connectivity_checks = _connectivity_checks(
            blueprint_validation=blueprint_validation,
            profiles=profiles,
            issues=issues,
        )
        inventory_consistency = _inventory_consistency_checks(
            expected_nodes=expected_nodes,
            inventory_nodes=inventory_nodes,
            issues=issues,
        )
        crypto_inventory_consistency = _crypto_inventory_consistency_checks(
            run=run,
            blueprint_validation=blueprint_validation,
            technical_inventory=technical_inventory,
            inventory_nodes=inventory_nodes,
            issues=issues,
        )
        crypto_revocation_consistency = _crypto_revocation_consistency_checks(
            configure_report=configure_report,
            network_manifests=network_manifests,
            technical_inventory=technical_inventory,
            issues=issues,
        )

    issues = _sort_issues(issues)
    verdict = _verdict_from_issues(
        issues=issues,
        precondition_blocked=precondition_blocked,
        partial_mode=allow_partial_verification,
    )
    blocked = verdict == "blocked"

    error_count = sum(1 for issue in issues if issue.level == "error")
    warning_count = sum(1 for issue in issues if issue.level == "warning")
    critical_error_count = sum(1 for issue in issues if issue.level == "error" and issue.critical)

    recommendations = _deterministic_recommendations(issues)
    verify_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "verify",
        "blocked": blocked,
        "verdict": verdict,
        "partial_verification": {
            "enabled": allow_partial_verification,
            "reason": str(partial_verification_reason).strip(),
        },
        "summary": {
            "errors": error_count,
            "warnings": warning_count,
            "critical_errors": critical_error_count,
        },
        "health_checks": sorted(
            health_checks,
            key=lambda item: (item.get("node_id", ""), item.get("status", ""), item.get("reason", "")),
        ),
        "connectivity_checks": sorted(
            connectivity_checks,
            key=lambda item: (item.get("channel_id", ""), item.get("org_id", ""), item.get("status", "")),
        ),
        "inventory_consistency": inventory_consistency,
        "crypto_inventory_consistency": crypto_inventory_consistency,
        "crypto_revocation_consistency": crypto_revocation_consistency,
        "issues": [issue.to_dict() for issue in issues],
        "recommendations": recommendations,
        "generated_at": utc_now_iso(),
    }

    final_stage_statuses = {
        stage: run.stage_statuses.get(stage, "pending")
        for stage in ("prepare", "provision", "configure", "verify")
    }
    final_stage_statuses["verify"] = "failed" if verdict in {"failed", "blocked"} else "completed"

    pipeline_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "blueprint_fingerprint": run.blueprint_fingerprint,
        "resolved_schema_version": run.resolved_schema_version,
        "stage": "verify",
        "final_result": verdict,
        "stage_statuses": final_stage_statuses,
        "blocking_reason_codes": sorted({issue.code for issue in issues if issue.level == "error"}),
        "stage_reports": {
            "configure": {
                "blocked": bool(configure_report.get("blocked", False)),
                "ready_for_verify": bool(configure_report.get("ready_for_verify", False)),
                "issue_count": len(configure_report.get("issues", []) or []),
            },
            "verify": {
                "verdict": verdict,
                "errors": error_count,
                "warnings": warning_count,
                "critical_errors": critical_error_count,
            },
        },
        "generated_at": utc_now_iso(),
    }

    artifacts: Dict[str, str] = {}
    checkpoint: Optional[StageCheckpoint] = None
    if state_store is not None:
        if not allow_partial_verification and _configure_completed_checkpoint(state_store=state_store, run=run) is None:
            raise ValueError("verify exige checkpoint completed da etapa configure (ou política parcial explícita).")

        verify_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="verify",
            artifact_name="verify-report.json",
            content=json.dumps(verify_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        pipeline_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="verify",
            artifact_name="pipeline-report.json",
            content=json.dumps(pipeline_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        artifacts = {
            "verify_report": str(verify_path),
            "pipeline_report": str(pipeline_path),
        }

        state_store.persist_run_state(run)
        checkpoint = state_store.persist_stage_checkpoint(
            run=run,
            stage="verify",
            stage_status="failed" if verdict in {"failed", "blocked"} else "completed",
            input_hash=payload_sha256(
                {
                    "connection_profiles": connection_profiles,
                    "network_manifests": network_manifests,
                    "technical_inventory": technical_inventory,
                    "configure_report": configure_report,
                    "allow_partial_verification": allow_partial_verification,
                    "partial_verification_reason": str(partial_verification_reason).strip(),
                }
            ),
            output_hash=payload_sha256(
                {
                    "verify_report": verify_report,
                    "pipeline_report": pipeline_report,
                }
            ),
            attempt=attempt,
            executor=executor,
            timestamp_utc=utc_now_iso(),
        )

    return VerifyExecutionResult(
        verify_report=verify_report,
        pipeline_report=pipeline_report,
        blocked=blocked,
        verdict=verdict,
        artifacts=artifacts,
        issues=issues,
        checkpoint=checkpoint,
    )
