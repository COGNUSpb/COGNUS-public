from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import unicodedata
from urllib.parse import quote, urlparse

from .pipeline_contract import utc_now_iso

import yaml


CURRENT_MANIFEST_VERSION = "1.0.0"
PERSISTENCE_CONTRACT_VERSION = "1.0.0"

ALLOWED_COMPONENT_TYPES = {
    "peer",
    "orderer",
    "ca",
    "couch",
    "api_gateway",
    "network_api",
    "cc_webclient",
}
REQUIRED_COMPONENT_TYPES = {
    "peer",
    "orderer",
    "ca",
    "couch",
    "api_gateway",
    "network_api",
}
ALLOWED_ORG_ROLES = {"peer", "orderer", "ca"}
ALLOWED_CA_MODES = {"internal", "external"}
ALLOWED_DESIRED_STATES = {"planned", "required", "optional"}
ALLOWED_CRITICALITIES = {"critical", "supporting"}

SEMVER_REGEX = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:[-+][0-9A-Za-z.-]+)?$"
)
ISO8601_UTC_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3,6})?Z$"
)
SHA256_REGEX = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
MSP_ID_REGEX = re.compile(r"^[A-Za-z][A-Za-z0-9]{1,62}MSP$")
CHANNEL_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
CHAINCODE_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
CHAINCODE_VERSION_REGEX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
DNS_LABEL_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
PROFILE_REF_REGEX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
IMAGE_REF_REGEX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:-]*(?:@sha256:[0-9a-f]{64})?$")
MANIFEST_STORAGE_TOKEN_REGEX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

CHAINCODE_RUNTIME_NAME_REGEX = re.compile(
    r"^dev-peer(?P<peer_idx>\d+)\.(?P<org>[a-z0-9-]+)\.(?P<domain>[a-z0-9.-]+?)-"
    r"(?P<chaincode>[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)_"
    r"(?P<version>[A-Za-z0-9._-]+)-(?P<hash>[0-9a-f]{64})$"
)

_ISSUE_LEVEL_ORDER = {"error": 0, "warning": 1, "hint": 2}
ISSUE_CATALOG_VERSION = "1.0.0"
ISSUE_DIAGNOSTIC_FIELDS = ("path", "causa", "impacto", "acao_recomendada")
ISSUE_SEVERITY_LEVELS = ("error", "warning")

_ERROR_ISSUE_CODES = frozenset(
    {
        "blueprint_scope_domain_mismatch",
        "blueprint_scope_org_mismatch",
        "blueprint_scope_roles_mismatch",
        "cardinality_below_minimum",
        "chaincode_runtime_chaincode_mismatch",
        "chaincode_runtime_channel_not_in_scope",
        "chaincode_runtime_domain_mismatch",
        "chaincode_runtime_hash_mismatch",
        "chaincode_runtime_name_not_deterministic",
        "chaincode_runtime_org_mismatch",
        "chaincode_runtime_target_peer_not_active",
        "chaincode_runtime_version_mismatch",
        "component_host_not_found",
        "component_service_context_chaincode_not_declared",
        "component_service_context_channel_not_in_scope",
        "component_service_context_org_mismatch",
        "couch_peer_pairing_invalid",
        "duplicate_chaincode_runtime_binding",
        "duplicate_chaincode_runtime_name",
        "duplicate_component_id",
        "duplicate_component_name",
        "duplicate_component_port",
        "duplicate_host_id",
        "host_port_conflict",
        "incompatible_org_role",
        "invalid_chaincode_runtime_chaincode_id",
        "invalid_chaincode_runtime_channel",
        "invalid_chaincode_runtime_name",
        "invalid_chaincode_runtime_object",
        "invalid_chaincode_runtime_package_hash",
        "invalid_chaincode_runtime_target_peer",
        "invalid_chaincode_runtime_version",
        "invalid_chaincode_runtimes_block",
        "invalid_component_criticality",
        "invalid_component_desired_state",
        "invalid_component_env_profile",
        "invalid_component_host_id",
        "invalid_component_id",
        "invalid_component_image",
        "invalid_component_image_whitespace",
        "invalid_component_name",
        "invalid_component_object",
        "invalid_component_port_value",
        "invalid_component_ports",
        "invalid_component_resource_value",
        "invalid_component_resources",
        "invalid_component_service_context",
        "invalid_component_service_context_chaincode_item",
        "invalid_component_service_context_chaincode_value",
        "invalid_component_service_context_chaincodes",
        "invalid_component_service_context_channel_item",
        "invalid_component_service_context_channel_value",
        "invalid_component_service_context_channels",
        "invalid_component_service_context_org_id",
        "invalid_component_storage_profile",
        "invalid_component_type",
        "invalid_components_block",
        "invalid_domain",
        "invalid_generated_at",
        "invalid_host_address",
        "invalid_host_connection_profile_ref",
        "invalid_host_docker_endpoint",
        "invalid_host_id",
        "invalid_host_object",
        "invalid_host_ssh_port",
        "invalid_hosts_block",
        "invalid_manifest_version",
        "manifest_schema_migration_path_not_found",
        "manifest_schema_migration_required",
        "manifest_version_major_ahead",
        "manifest_version_newer_than_runtime",
        "invalid_org_id",
        "invalid_org_identity_ca_mode",
        "invalid_org_identity_ca_profile_ref",
        "invalid_org_identity_msp_id",
        "invalid_org_identity_tls_ca_profile_ref",
        "invalid_org_naming_token",
        "invalid_role_item",
        "invalid_role_value",
        "invalid_roles_block",
        "invalid_source_blueprint_fingerprint",
        "invalid_source_blueprint_scope_channel_item",
        "invalid_source_blueprint_scope_channel_value",
        "invalid_source_blueprint_scope_channels",
        "invalid_source_blueprint_scope_domain",
        "invalid_source_blueprint_scope_org_id",
        "invalid_source_blueprint_version",
        "manifest_missing_required",
        "manifest_not_object",
        "missing_external_ca_profile_ref",
        "missing_external_tls_ca_profile_ref",
        "missing_org_identity",
        "missing_source_blueprint_scope",
        "org_identity_msp_org_mismatch",
        "org_naming_token_mismatch",
        # Codigo guarda-chuva para categorizacao de problemas de runtime de chaincode.
        "invalid_chaincode_runtime",
    }
)

_WARNING_ISSUE_CODES = frozenset(
    {
        "invalid_component_reconcile_hints",
        "invalid_host_labels",
        "manifest_schema_migration_applied",
        "manifest_version_older_compatible",
    }
)

ISSUE_CODE_SEVERITY = {
    **{code: "error" for code in _ERROR_ISSUE_CODES},
    **{code: "warning" for code in _WARNING_ISSUE_CODES},
}


def get_org_runtime_manifest_issue_catalog() -> Dict[str, Any]:
    return {
        "catalog_version": ISSUE_CATALOG_VERSION,
        "severity_levels": list(ISSUE_SEVERITY_LEVELS),
        "diagnostic_payload_fields": list(ISSUE_DIAGNOSTIC_FIELDS),
        "codes": [
            {"code": code, "severity": ISSUE_CODE_SEVERITY[code]}
            for code in sorted(ISSUE_CODE_SEVERITY)
        ],
    }


def get_org_runtime_manifest_schema_policy() -> Dict[str, Any]:
    return {
        "runtime_manifest_version": CURRENT_MANIFEST_VERSION,
        "semver_contract": "MAJOR.MINOR.PATCH",
        "compatibility_rules": [
            {"when": "same_version", "result": "accept"},
            {"when": "same_major_older", "result": "accept_with_warning_backward_compatible"},
            {"when": "same_major_newer", "result": "reject_runtime_update_required"},
            {"when": "major_ahead", "result": "reject_major_not_supported"},
            {"when": "major_behind", "result": "migration_required"},
        ],
        "migration_paths": [
            {"from": source, "to": target}
            for source, target in sorted(MANIFEST_SCHEMA_MIGRATIONS.keys())
        ],
    }


def _normalize_issue_against_catalog(issue: "ManifestIssue") -> "ManifestIssue":
    expected_level = ISSUE_CODE_SEVERITY.get(issue.code)
    if expected_level and issue.level != expected_level:
        return ManifestIssue(
            level=expected_level,
            code=issue.code,
            path=issue.path,
            message=issue.message,
            cause=issue.cause,
            impact=issue.impact,
            recommendation=issue.recommendation,
        )
    return issue


@dataclass(frozen=True)
class ManifestIssue:
    level: str
    code: str
    path: str
    message: str
    cause: str = ""
    impact: str = ""
    recommendation: str = ""

    def to_dict(self) -> Dict[str, str]:
        payload = asdict(self)
        compact_payload = {key: value for key, value in payload.items() if value != ""}
        # Alias canonicos em PT-BR para contrato de diagnostico do A2.
        if self.cause:
            compact_payload["causa"] = self.cause
        if self.impact:
            compact_payload["impacto"] = self.impact
        if self.recommendation:
            compact_payload["acao_recomendada"] = self.recommendation
        # Alias explicito para consumidores que usam `severity`.
        compact_payload["severity"] = self.level
        return compact_payload


@dataclass(frozen=True)
class OrgRuntimeManifestValidationResult:
    valid: bool
    manifest_runtime_version: str
    manifest_name: str
    manifest_version: str
    resolved_manifest_version: str
    migration_applied: bool
    migrated_from_manifest_version: str
    generated_at: str
    change_id: str
    run_id: str
    org_id: str
    org_label: str
    domain: str
    environment_profile_ref: str
    source_blueprint_fingerprint: str
    source_blueprint_version: str
    orchestrator_context: str
    normalized_source_blueprint_scope: Dict[str, Any]
    normalized_org_identity: Dict[str, Any]
    normalized_hosts: List[Dict[str, Any]]
    normalized_components: List[Dict[str, Any]]
    normalized_chaincode_runtimes: List[Dict[str, Any]]
    manifest_fingerprint: str
    fingerprint_sha256: str
    issues: List[ManifestIssue]

    def to_dict(self) -> Dict[str, Any]:
        issue_dicts = [issue.to_dict() for issue in self.issues]
        errors = [issue for issue in issue_dicts if issue.get("level") == "error"]
        warnings = [issue for issue in issue_dicts if issue.get("level") == "warning"]
        hints = [issue for issue in issue_dicts if issue.get("level") == "hint"]
        return {
            "valid": self.valid,
            "errors": errors,
            "warnings": warnings,
            "hints": hints,
            "manifest_runtime_version": self.manifest_runtime_version,
            "manifest_name": self.manifest_name,
            "manifest_version": self.manifest_version,
            "resolved_manifest_version": self.resolved_manifest_version,
            "migration_applied": self.migration_applied,
            "migrated_from_manifest_version": self.migrated_from_manifest_version,
            "generated_at": self.generated_at,
            "change_id": self.change_id,
            "run_id": self.run_id,
            "org_id": self.org_id,
            "org_label": self.org_label,
            "domain": self.domain,
            "environment_profile_ref": self.environment_profile_ref,
            "source_blueprint_fingerprint": self.source_blueprint_fingerprint,
            "source_blueprint_version": self.source_blueprint_version,
            "orchestrator_context": self.orchestrator_context,
            "normalized_source_blueprint_scope": self.normalized_source_blueprint_scope,
            "normalized_org_identity": self.normalized_org_identity,
            "normalized_hosts": self.normalized_hosts,
            "normalized_components": self.normalized_components,
            "normalized_chaincode_runtimes": self.normalized_chaincode_runtimes,
            "manifest_fingerprint": self.manifest_fingerprint,
            "fingerprint_sha256": self.fingerprint_sha256,
            "issue_catalog": get_org_runtime_manifest_issue_catalog(),
            "schema_policy": get_org_runtime_manifest_schema_policy(),
            "issues": issue_dicts,
        }


def _sort_issues_deterministically(issues: List[ManifestIssue]) -> List[ManifestIssue]:
    normalized_issues = [_normalize_issue_against_catalog(issue) for issue in issues]
    return sorted(
        normalized_issues,
        key=lambda issue: (
            _ISSUE_LEVEL_ORDER.get(issue.level, 99),
            issue.path,
            issue.code,
            issue.message,
            issue.cause,
            issue.impact,
            issue.recommendation,
        ),
    )


def _is_valid_domain(value: str) -> bool:
    if not value or len(value) > 253:
        return False
    domain = value[:-1] if value.endswith(".") else value
    labels = domain.split(".")
    if len(labels) < 2:
        return False
    return all(DNS_LABEL_REGEX.fullmatch(label) for label in labels)


def _is_msp_consistent_with_org(org_id: str, msp_id: str) -> bool:
    if not org_id or not msp_id:
        return True
    org_key = re.sub(r"[^a-z0-9]", "", org_id.lower())
    msp_lower = msp_id.lower()
    if not msp_lower.endswith("msp"):
        return False
    msp_key = re.sub(r"[^a-z0-9]", "", msp_lower[:-3])
    return org_key == msp_key


def _ascii_fold(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(
        character for character in normalized if not unicodedata.combining(character)
    )
    return without_marks.encode("ascii", "ignore").decode("ascii")


def _normalize_domain(value: str) -> str:
    ascii_value = _ascii_fold(value)
    normalized = ascii_value.strip().lower().strip(".")
    normalized = re.sub(r"\.+", ".", normalized)
    return normalized


def _is_valid_host_address(value: str) -> bool:
    if not value:
        return False
    candidate = value.strip()
    if not candidate:
        return False
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1]
    try:
        ipaddress.ip_address(candidate)
        return True
    except ValueError:
        pass
    domain_candidate = candidate.lower()
    if _is_valid_domain(domain_candidate):
        return True
    if "." not in domain_candidate:
        return bool(DNS_LABEL_REGEX.fullmatch(domain_candidate))
    return False


def _is_valid_docker_endpoint(value: str) -> bool:
    if not value:
        return False
    try:
        parsed = urlparse(value.strip())
    except ValueError:
        return False
    scheme = parsed.scheme.lower()
    if scheme not in {"tcp", "unix", "ssh"}:
        return False
    if parsed.query or parsed.fragment:
        return False
    if scheme == "unix":
        return bool(parsed.path and parsed.path.startswith("/")) and not parsed.netloc
    host = parsed.hostname
    if not host or not _is_valid_host_address(host):
        return False
    try:
        port = parsed.port
    except ValueError:
        return False
    return bool(port and 1 <= port <= 65535)


def _normalize_ascii_token(value: str) -> str:
    ascii_value = _ascii_fold(value)
    normalized = re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized


def _normalize_org_label_upper(value: str) -> str:
    ascii_value = _ascii_fold(value)
    normalized = re.sub(r"[^A-Za-z0-9-]+", "-", ascii_value.upper()).strip("-")
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized


def _ensure_required_string(
    manifest: Dict[str, Any],
    key: str,
    issues: List[ManifestIssue],
    path: str,
) -> str:
    raw = manifest.get(key)
    if isinstance(raw, str):
        value = raw.strip()
        if value:
            return value
    issues.append(
        ManifestIssue(
            level="error",
            code="manifest_missing_required",
            path=path,
            message=f"Campo obrigatorio '{key}' ausente ou invalido.",
            cause=f"Campo '{key}' nao informado como string valida.",
            impact="Manifesto nao pode ser processado de forma deterministica.",
            recommendation=f"Preencher '{key}' com valor textual nao vazio.",
        )
    )
    return ""


def _to_positive_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
        return parsed if parsed > 0 else None
    return None


def _canonical_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_fingerprint(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _coerce_semver_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _parse_semver(version_str: str) -> Optional[Tuple[int, int, int]]:
    if not SEMVER_REGEX.fullmatch(version_str):
        return None
    core = version_str.split("-", 1)[0].split("+", 1)[0]
    major, minor, patch = core.split(".")
    return int(major), int(minor), int(patch)


def _migrate_manifest_0_9_0_to_1_0_0(manifest: Dict[str, Any]) -> Dict[str, Any]:
    migrated = deepcopy(manifest)
    # Compatibilidade retroativa: aliases do esquema inicial.
    if "identity" in migrated and "org_identity" not in migrated:
        migrated["org_identity"] = migrated.pop("identity")
    if "source_scope" in migrated and "source_blueprint_scope" not in migrated:
        migrated["source_blueprint_scope"] = migrated.pop("source_scope")
    migrated["manifest_version"] = "1.0.0"
    return migrated


MANIFEST_SCHEMA_MIGRATIONS = {
    ("0.9.0", "1.0.0"): _migrate_manifest_0_9_0_to_1_0_0,
}


def _apply_manifest_schema_migration_chain(
    manifest: Dict[str, Any],
    source_version: str,
    target_version: str,
) -> Optional[Dict[str, Any]]:
    if source_version == target_version:
        return deepcopy(manifest)

    current = source_version
    migrated = deepcopy(manifest)
    safety_counter = 0
    while current != target_version and safety_counter < 8:
        safety_counter += 1
        next_step = None
        migrator = None
        for (src, dst), candidate in MANIFEST_SCHEMA_MIGRATIONS.items():
            if src == current:
                next_step = dst
                migrator = candidate
                break
        if not migrator or not next_step:
            return None
        migrated = migrator(migrated)
        current = next_step

    if current != target_version:
        return None
    migrated["manifest_version"] = target_version
    return migrated


def _resolve_manifest_schema_compatibility(
    manifest: Dict[str, Any],
    allow_schema_migration: bool,
    issues: List[ManifestIssue],
) -> Tuple[Dict[str, Any], str, bool, str]:
    raw_version = _coerce_semver_text(manifest.get("manifest_version"))
    if not raw_version:
        return deepcopy(manifest), "", False, ""

    parsed = _parse_semver(raw_version)
    current_parsed = _parse_semver(CURRENT_MANIFEST_VERSION)
    if not parsed or not current_parsed:
        return deepcopy(manifest), raw_version, False, ""

    if parsed == current_parsed:
        return deepcopy(manifest), raw_version, False, ""

    if parsed[0] == current_parsed[0]:
        if parsed > current_parsed:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="manifest_version_newer_than_runtime",
                    path="manifest_version",
                    message=(
                        f"manifest_version {raw_version} e mais nova que a suportada "
                        f"({CURRENT_MANIFEST_VERSION}). Atualize o runtime."
                    ),
                    cause="Versao declarada excede o runtime atual no mesmo major.",
                    impact="Compatibilidade de parsing/validacao nao e garantida.",
                    recommendation="Atualizar runtime ou reduzir manifest_version para versao suportada.",
                )
            )
            return deepcopy(manifest), raw_version, False, ""

        issues.append(
            ManifestIssue(
                level="warning",
                code="manifest_version_older_compatible",
                path="manifest_version",
                message=(
                    f"manifest_version {raw_version} e retrocompativel com runtime {CURRENT_MANIFEST_VERSION}."
                ),
                cause="Manifesto usa versao anterior no mesmo major.",
                impact="Fluxo segue valido com contrato de backward compatibility.",
                recommendation="Planejar upgrade gradual para CURRENT_MANIFEST_VERSION.",
            )
        )
        return deepcopy(manifest), raw_version, False, ""

    if parsed[0] > current_parsed[0]:
        issues.append(
            ManifestIssue(
                level="error",
                code="manifest_version_major_ahead",
                path="manifest_version",
                message=(
                    f"Major {parsed[0]} de manifest_version nao suportado por este runtime "
                    f"({CURRENT_MANIFEST_VERSION})."
                ),
                cause="Manifesto de major superior ao runtime.",
                impact="Contrato pode ter breaking changes nao reconhecidas.",
                recommendation="Atualizar runtime para o major suportado ou ajustar manifest_version.",
            )
        )
        return deepcopy(manifest), raw_version, False, ""

    if not allow_schema_migration:
        issues.append(
            ManifestIssue(
                level="error",
                code="manifest_schema_migration_required",
                path="manifest_version",
                message=(
                    f"manifest_version {raw_version} requer migracao para {CURRENT_MANIFEST_VERSION}. "
                    "Habilite allow_schema_migration."
                ),
                cause="Manifesto de major anterior exige rotina de migracao.",
                impact="Risco de campos/contratos legados incompatíveis com o runtime atual.",
                recommendation="Executar validacao com allow_schema_migration=True.",
            )
        )
        return deepcopy(manifest), raw_version, False, ""

    migrated = _apply_manifest_schema_migration_chain(
        manifest,
        raw_version,
        CURRENT_MANIFEST_VERSION,
    )
    if migrated is None:
        issues.append(
            ManifestIssue(
                level="error",
                code="manifest_schema_migration_path_not_found",
                path="manifest_version",
                message=(
                    f"Sem rotina de migracao de {raw_version} para {CURRENT_MANIFEST_VERSION}."
                ),
                cause="Cadeia de migracao nao declarada no runtime.",
                impact="Manifesto legado nao pode ser promovido com seguranca.",
                recommendation="Adicionar rotina de migracao intermediaria ou atualizar manifesto manualmente.",
            )
        )
        return deepcopy(manifest), raw_version, False, ""

    issues.append(
        ManifestIssue(
            level="warning",
            code="manifest_schema_migration_applied",
            path="manifest_version",
            message=f"Migracao aplicada: {raw_version} -> {CURRENT_MANIFEST_VERSION}.",
            cause="Manifesto legado convertido para schema suportado.",
            impact="Rastreabilidade deve considerar versao de origem e versao resolvida.",
            recommendation="Registrar migrated_from_manifest_version para auditoria.",
        )
    )
    return migrated, CURRENT_MANIFEST_VERSION, True, raw_version


def _normalize_roles(raw_roles: Any, path: str, issues: List[ManifestIssue]) -> List[str]:
    if not isinstance(raw_roles, list) or not raw_roles:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_roles_block",
                path=path,
                message=f"{path} deve ser lista nao vazia.",
                cause=f"Papeis em '{path}' nao foram informados corretamente.",
                impact="Nao e possivel validar compatibilidade de componentes.",
                recommendation="Informar roles contendo peer/orderer/ca conforme topologia.",
            )
        )
        return []

    normalized: List[str] = []
    seen: Set[str] = set()
    for idx, role in enumerate(raw_roles):
        role_path = f"{path}[{idx}]"
        if not isinstance(role, str) or not role.strip():
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_role_item",
                    path=role_path,
                    message=f"Cada item de {path} deve ser string nao vazia.",
                    cause="Item de role invalido.",
                    impact="Roles ficam inconsistentes para validacao cruzada.",
                    recommendation="Corrigir o item de role para valor textual valido.",
                )
            )
            continue
        role_name = role.strip().lower()
        if role_name not in ALLOWED_ORG_ROLES:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_role_value",
                    path=role_path,
                    message=f"Role invalida em {path}.",
                    cause=f"Role '{role}' nao pertence ao conjunto permitido.",
                    impact="Manifesto fica fora do contrato A2.",
                    recommendation="Usar apenas: peer, orderer, ca.",
                )
            )
            continue
        if role_name not in seen:
            normalized.append(role_name)
            seen.add(role_name)
    return normalized


def _validate_component_name(
    component_type: str,
    name: str,
    domain_lower: str,
    org_token: str,
    org_label_upper: str,
    issues: List[ManifestIssue],
    path: str,
) -> None:
    lower_name = name.lower()

    def _invalid(expected: str) -> None:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_component_name",
                path=path,
                message=f"Nome de componente invalido para tipo '{component_type}'.",
                cause=f"Nome '{name}' nao segue o padrao esperado.",
                impact="Reconciliacao e incremento deterministico ficam comprometidos.",
                recommendation=f"Aplicar padrao: {expected}",
            )
        )

    if component_type in {"peer", "orderer", "couch"}:
        if not org_token:
            _invalid(f"{component_type}{{n}}.{{organization}}.{domain_lower}")
            return
        expected_prefix = component_type
        pattern = re.compile(rf"^{expected_prefix}\d+\.{re.escape(org_token)}\.{re.escape(domain_lower)}$")
        if not pattern.fullmatch(lower_name):
            _invalid(f"{component_type}{{n}}.{{organization}}.{domain_lower}")
        return

    if component_type == "api_gateway":
        if not org_token:
            _invalid(f"apiGateway{{n}}.{{organization}}.{domain_lower}")
            return
        pattern = re.compile(rf"^apigateway\d+\.{re.escape(org_token)}\.{re.escape(domain_lower)}$")
        if not pattern.fullmatch(lower_name):
            _invalid(f"apiGateway{{n}}.{{organization}}.{domain_lower}")
        return

    if component_type == "cc_webclient":
        if not org_token:
            _invalid(f"cc-webclient{{n}}.{{organization}}.{domain_lower}")
            return
        pattern = re.compile(rf"^cc-webclient\d+\.{re.escape(org_token)}\.{re.escape(domain_lower)}$")
        if not pattern.fullmatch(lower_name):
            _invalid(f"cc-webclient{{n}}.{{organization}}.{domain_lower}")
        return

    if component_type == "ca":
        expected = f"ca_{org_label_upper}"
        if lower_name != expected.lower():
            _invalid(expected)
        return

    if component_type == "network_api":
        expected = f"netapi_{org_label_upper}"
        if lower_name != expected.lower():
            _invalid(expected)
        return

    _invalid("<component_type>_naming_pattern")


def _validate_manifest_metadata(manifest: Dict[str, Any], issues: List[ManifestIssue]) -> Dict[str, str]:
    metadata = {
        "manifest_name": _ensure_required_string(manifest, "manifest_name", issues, "manifest_name"),
        "manifest_version": _ensure_required_string(
            manifest, "manifest_version", issues, "manifest_version"
        ),
        "generated_at": _ensure_required_string(manifest, "generated_at", issues, "generated_at"),
        "change_id": _ensure_required_string(manifest, "change_id", issues, "change_id"),
        "run_id": _ensure_required_string(manifest, "run_id", issues, "run_id"),
        "org_id": _ensure_required_string(manifest, "org_id", issues, "org_id"),
        "org_label": _ensure_required_string(manifest, "org_label", issues, "org_label"),
        "domain": _normalize_domain(
            _ensure_required_string(manifest, "domain", issues, "domain")
        ),
        "environment_profile_ref": _ensure_required_string(
            manifest, "environment_profile_ref", issues, "environment_profile_ref"
        ),
        "source_blueprint_fingerprint": _ensure_required_string(
            manifest,
            "source_blueprint_fingerprint",
            issues,
            "source_blueprint_fingerprint",
        ).lower(),
        "source_blueprint_version": _ensure_required_string(
            manifest, "source_blueprint_version", issues, "source_blueprint_version"
        ),
        "orchestrator_context": _ensure_required_string(
            manifest, "orchestrator_context", issues, "orchestrator_context"
        ),
    }

    if metadata["manifest_version"] and not SEMVER_REGEX.fullmatch(metadata["manifest_version"]):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_manifest_version",
                path="manifest_version",
                message="manifest_version deve seguir SemVer (MAJOR.MINOR.PATCH).",
                cause="Versao do manifesto fora do padrao definido.",
                impact="Compatibilidade de schema nao pode ser garantida.",
                recommendation="Usar valor SemVer valido, ex.: 1.0.0.",
            )
        )

    if metadata["source_blueprint_version"] and not SEMVER_REGEX.fullmatch(
        metadata["source_blueprint_version"]
    ):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_source_blueprint_version",
                path="source_blueprint_version",
                message="source_blueprint_version deve seguir SemVer.",
                cause="Versao do blueprint de origem invalida.",
                impact="Rastreabilidade de origem fica inconsistente.",
                recommendation="Informar versao do blueprint em SemVer.",
            )
        )

    if metadata["generated_at"] and not ISO8601_UTC_REGEX.fullmatch(metadata["generated_at"]):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_generated_at",
                path="generated_at",
                message="generated_at deve ser timestamp UTC ISO-8601.",
                cause="Timestamp de geracao fora do formato UTC esperado.",
                impact="Ordenacao temporal e auditoria podem divergir.",
                recommendation="Usar formato como 2026-02-22T12:30:00Z.",
            )
        )

    if metadata["source_blueprint_fingerprint"] and not SHA256_REGEX.fullmatch(
        metadata["source_blueprint_fingerprint"]
    ):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_source_blueprint_fingerprint",
                path="source_blueprint_fingerprint",
                message="source_blueprint_fingerprint deve ser SHA-256 canonico (64 hex minusculo).",
                cause="Fingerprint de origem fora do formato padrao.",
                impact="Correlacao blueprint->manifest pode quebrar.",
                recommendation="Informar hash SHA-256 valido com 64 caracteres hexadecimais minusculos.",
            )
        )

    if metadata["org_id"] and not IDENTIFIER_REGEX.fullmatch(metadata["org_id"]):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_id",
                path="org_id",
                message="org_id invalido para contrato A2.",
                cause="org_id fora do padrao de identificador.",
                impact="Naming e correlacao entre artefatos podem colidir.",
                recommendation="Usar org_id em minusculo, com hifen opcional.",
            )
        )

    org_id_token = _normalize_ascii_token(metadata["org_id"])
    org_label_token = _normalize_ascii_token(metadata["org_label"])
    if org_id_token and org_label_token and org_id_token != org_label_token:
        issues.append(
            ManifestIssue(
                level="error",
                code="org_naming_token_mismatch",
                path="org_label",
                message="org_label nao normaliza para o mesmo token de org_id.",
                cause=(
                    f"org_id token='{org_id_token}' e org_label token='{org_label_token}' "
                    "apos normalizacao de case/acentuacao/separadores."
                ),
                impact="Naming de componentes pode ficar ambiguo para incremento e reconciliacao.",
                recommendation="Alinhar org_label para normalizar no mesmo token de org_id.",
            )
        )

    if metadata["domain"] and not _is_valid_domain(metadata["domain"]):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_domain",
                path="domain",
                message="domain deve ser FQDN valido.",
                cause="Dominio informado nao atende regras DNS.",
                impact="Nomes de componentes e roteamento ficam invalidos.",
                recommendation="Informar dominio completo, ex.: org.example.com.",
            )
        )

    return metadata


def _validate_org_identity(
    manifest: Dict[str, Any],
    metadata_org_id: str,
    issues: List[ManifestIssue],
) -> Tuple[Dict[str, Any], List[str]]:
    path = "org_identity"
    raw = manifest.get("org_identity")
    if not isinstance(raw, dict):
        issues.append(
            ManifestIssue(
                level="error",
                code="missing_org_identity",
                path=path,
                message="org_identity e obrigatorio e deve ser objeto.",
                cause="Bloco de identidade da organizacao nao foi definido corretamente.",
                impact="Nao ha base para validar papeis e componentes.",
                recommendation="Definir org_identity com msp_id, roles e ca_mode.",
            )
        )
        return {}, []

    msp_id = _ensure_required_string(raw, "msp_id", issues, f"{path}.msp_id")
    if msp_id and not MSP_ID_REGEX.fullmatch(msp_id):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_identity_msp_id",
                path=f"{path}.msp_id",
                message="msp_id invalido para contrato Fabric.",
                cause="Formato MSP fora do padrao esperado.",
                impact="Identidade da organizacao nao pode ser correlacionada com a rede.",
                recommendation="Usar padrao <Nome>MSP, ex.: InfUfgMSP.",
            )
        )
    if msp_id and metadata_org_id and not _is_msp_consistent_with_org(metadata_org_id, msp_id):
        issues.append(
            ManifestIssue(
                level="error",
                code="org_identity_msp_org_mismatch",
                path=f"{path}.msp_id",
                message="msp_id nao e consistente com org_id do manifesto.",
                cause=(
                    f"msp_id '{msp_id}' nao corresponde ao org_id '{metadata_org_id}' "
                    "apos normalizacao."
                ),
                impact="Escopo da org pode divergir entre blueprint e runtime manifest.",
                recommendation="Ajustar msp_id para corresponder ao org_id (ex.: InfUfgMSP para inf-ufg).",
            )
        )

    roles = _normalize_roles(raw.get("roles"), f"{path}.roles", issues)

    ca_mode = _ensure_required_string(raw, "ca_mode", issues, f"{path}.ca_mode").lower()
    if ca_mode and ca_mode not in ALLOWED_CA_MODES:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_identity_ca_mode",
                path=f"{path}.ca_mode",
                message="ca_mode invalido.",
                cause=f"Valor '{ca_mode}' nao pertence ao conjunto permitido.",
                impact="Provisionamento de CA pode divergir do modo de operacao.",
                recommendation="Usar ca_mode 'internal' ou 'external'.",
            )
        )

    ca_profile_raw = raw.get("ca_profile_ref", "")
    ca_profile_ref = ca_profile_raw.strip() if isinstance(ca_profile_raw, str) else ""
    if ca_profile_raw is not None and not isinstance(ca_profile_raw, str):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_identity_ca_profile_ref",
                path=f"{path}.ca_profile_ref",
                message="ca_profile_ref deve ser string quando informado.",
                cause="Referencia de perfil de CA fora do formato textual.",
                impact="Proveniencia tecnica da autoridade certificadora fica inconsistente.",
                recommendation="Informar ca_profile_ref como string textual.",
            )
        )
    if ca_profile_ref and not PROFILE_REF_REGEX.fullmatch(ca_profile_ref):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_identity_ca_profile_ref",
                path=f"{path}.ca_profile_ref",
                message="ca_profile_ref possui formato invalido.",
                cause=f"Valor '{ca_profile_ref}' fora do padrao permitido.",
                impact="Correlacao com perfil tecnico de CA pode falhar.",
                recommendation="Usar referencia alfanumerica com '-', '_', '/', '.'.",
            )
        )

    tls_ca_profile_raw = raw.get("tls_ca_profile_ref", "")
    tls_ca_profile_ref = tls_ca_profile_raw.strip() if isinstance(tls_ca_profile_raw, str) else ""
    if tls_ca_profile_raw is not None and not isinstance(tls_ca_profile_raw, str):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_identity_tls_ca_profile_ref",
                path=f"{path}.tls_ca_profile_ref",
                message="tls_ca_profile_ref deve ser string quando informado.",
                cause="Referencia de perfil TLS CA fora do formato textual.",
                impact="Rastreabilidade da autoridade TLS fica inconsistente.",
                recommendation="Informar tls_ca_profile_ref como string textual.",
            )
        )
    if tls_ca_profile_ref and not PROFILE_REF_REGEX.fullmatch(tls_ca_profile_ref):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_identity_tls_ca_profile_ref",
                path=f"{path}.tls_ca_profile_ref",
                message="tls_ca_profile_ref possui formato invalido.",
                cause=f"Valor '{tls_ca_profile_ref}' fora do padrao permitido.",
                impact="Correlacao com perfil tecnico de TLS CA pode falhar.",
                recommendation="Usar referencia alfanumerica com '-', '_', '/', '.'.",
            )
        )

    if ca_mode == "external" and not ca_profile_ref:
        issues.append(
            ManifestIssue(
                level="error",
                code="missing_external_ca_profile_ref",
                path=f"{path}.ca_profile_ref",
                message="ca_profile_ref e obrigatorio quando ca_mode=external.",
                cause="Modo external sem referencia explicita de autoridade CA.",
                impact="Provisionamento externo nao consegue resolver perfil de CA.",
                recommendation="Informar ca_profile_ref valido para o modo external.",
            )
        )
    if ca_mode == "external" and not tls_ca_profile_ref:
        issues.append(
            ManifestIssue(
                level="error",
                code="missing_external_tls_ca_profile_ref",
                path=f"{path}.tls_ca_profile_ref",
                message="tls_ca_profile_ref e obrigatorio quando ca_mode=external.",
                cause="Modo external sem referencia explicita de autoridade TLS CA.",
                impact="Cadeia TLS pode ficar sem base de trust oficial no provisionamento.",
                recommendation="Informar tls_ca_profile_ref valido para o modo external.",
            )
        )

    normalized = {
        "msp_id": msp_id,
        "roles": sorted(roles),
        "ca_mode": ca_mode,
        "ca_profile_ref": ca_profile_ref,
        "tls_ca_profile_ref": tls_ca_profile_ref,
    }
    return normalized, roles


def _validate_source_blueprint_scope(
    manifest: Dict[str, Any],
    metadata: Dict[str, str],
    normalized_org_identity: Dict[str, Any],
    issues: List[ManifestIssue],
) -> Tuple[Dict[str, Any], Set[str]]:
    path = "source_blueprint_scope"
    raw_scope = manifest.get("source_blueprint_scope")
    if not isinstance(raw_scope, dict):
        issues.append(
            ManifestIssue(
                level="error",
                code="missing_source_blueprint_scope",
                path=path,
                message="source_blueprint_scope e obrigatorio e deve ser objeto.",
                cause="Escopo publicado do blueprint nao foi fornecido no manifesto.",
                impact="Nao e possivel validar coerencia org/domain/roles/canais com origem publicada.",
                recommendation=(
                    "Incluir source_blueprint_scope com org_id, domain, roles e channels "
                    "derivados do blueprint publicado."
                ),
            )
        )
        return {}, set()

    scope_org_id = _ensure_required_string(raw_scope, "org_id", issues, f"{path}.org_id")
    if scope_org_id and not IDENTIFIER_REGEX.fullmatch(scope_org_id):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_source_blueprint_scope_org_id",
                path=f"{path}.org_id",
                message="source_blueprint_scope.org_id invalido.",
                cause=f"Valor '{scope_org_id}' fora do padrao de identificador.",
                impact="Correlacao entre manifesto e blueprint fica ambigua.",
                recommendation="Usar org_id em minusculo com hifen opcional.",
            )
        )

    scope_domain = _normalize_domain(
        _ensure_required_string(raw_scope, "domain", issues, f"{path}.domain")
    )
    if scope_domain and not _is_valid_domain(scope_domain):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_source_blueprint_scope_domain",
                path=f"{path}.domain",
                message="source_blueprint_scope.domain deve ser FQDN valido.",
                cause="Dominio no escopo do blueprint nao atende regras DNS.",
                impact="Verificacao de naming por dominio fica inconsistente.",
                recommendation="Informar domain completo, ex.: inf.ufg.br.",
            )
        )

    scope_roles = _normalize_roles(raw_scope.get("roles"), f"{path}.roles", issues)

    raw_channels = raw_scope.get("channels")
    normalized_channels: List[str] = []
    seen_channels: Set[str] = set()
    if not isinstance(raw_channels, list) or not raw_channels:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_source_blueprint_scope_channels",
                path=f"{path}.channels",
                message="source_blueprint_scope.channels deve ser lista nao vazia.",
                cause="Associacao de canais da org nao foi declarada no escopo publicado.",
                impact="Runtimes de chaincode nao podem ser validados contra canais permitidos.",
                recommendation="Informar channels com os channel_ids associados a org no blueprint.",
            )
        )
    else:
        for idx, raw_channel in enumerate(raw_channels):
            channel_path = f"{path}.channels[{idx}]"
            if not isinstance(raw_channel, str) or not raw_channel.strip():
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="invalid_source_blueprint_scope_channel_item",
                        path=channel_path,
                        message="Cada channel em source_blueprint_scope.channels deve ser string nao vazia.",
                        cause="Item de canal invalido no escopo publicado.",
                        impact="Associacao de canais pode ficar parcial ou ambigua.",
                        recommendation="Corrigir o item para channel_id textual valido.",
                    )
                )
                continue
            channel_id = raw_channel.strip().lower()
            if not CHANNEL_ID_REGEX.fullmatch(channel_id):
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="invalid_source_blueprint_scope_channel_value",
                        path=channel_path,
                        message="channel_id invalido em source_blueprint_scope.channels.",
                        cause=f"Valor '{raw_channel}' fora do padrao de channel_id.",
                        impact="Comparacao de associacao de canais nao confiavel.",
                        recommendation="Usar channel_id em minusculo com hifen opcional.",
                    )
                )
                continue
            if channel_id not in seen_channels:
                seen_channels.add(channel_id)
                normalized_channels.append(channel_id)

    metadata_org_id = metadata.get("org_id", "")
    metadata_domain = metadata.get("domain", "")
    if scope_org_id and metadata_org_id and scope_org_id != metadata_org_id:
        issues.append(
            ManifestIssue(
                level="error",
                code="blueprint_scope_org_mismatch",
                path=f"{path}.org_id",
                message="source_blueprint_scope.org_id diverge de org_id do manifesto.",
                cause=f"Escopo org_id='{scope_org_id}' e manifesto org_id='{metadata_org_id}'.",
                impact="Manifesto pode ter sido montado para organizacao diferente do blueprint.",
                recommendation="Sincronizar org_id do manifesto com org_id do escopo publicado.",
            )
        )
    if scope_domain and metadata_domain and scope_domain != metadata_domain:
        issues.append(
            ManifestIssue(
                level="error",
                code="blueprint_scope_domain_mismatch",
                path=f"{path}.domain",
                message="source_blueprint_scope.domain diverge de domain do manifesto.",
                cause=f"Escopo domain='{scope_domain}' e manifesto domain='{metadata_domain}'.",
                impact="Naming de componentes e roteamento por dominio podem divergir.",
                recommendation="Alinhar domain do manifesto ao dominio do escopo publicado.",
            )
        )

    manifest_roles = set(normalized_org_identity.get("roles", []))
    if scope_roles and manifest_roles and set(scope_roles) != manifest_roles:
        issues.append(
            ManifestIssue(
                level="error",
                code="blueprint_scope_roles_mismatch",
                path=f"{path}.roles",
                message="source_blueprint_scope.roles diverge de org_identity.roles.",
                cause=(
                    f"Roles do escopo={sorted(set(scope_roles))} e "
                    f"roles do manifesto={sorted(manifest_roles)}."
                ),
                impact="Validacao de compatibilidade de componentes por papel pode ficar inconsistente.",
                recommendation="Manter roles do escopo e do manifesto estritamente alinhados.",
            )
        )

    normalized_scope = {
        "org_id": scope_org_id,
        "domain": scope_domain,
        "roles": sorted(set(scope_roles)),
        "channels": sorted(normalized_channels),
    }
    return normalized_scope, set(normalized_channels)


def _validate_hosts(
    manifest: Dict[str, Any],
    issues: List[ManifestIssue],
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    path = "hosts"
    raw_hosts = manifest.get("hosts")
    if not isinstance(raw_hosts, list) or not raw_hosts:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_hosts_block",
                path=path,
                message="hosts deve ser lista nao vazia.",
                cause="Manifesto sem host de execucao definido.",
                impact="Nao ha alvo para provisionamento via SSH.",
                recommendation="Definir ao menos um host com host_id e conectividade.",
            )
        )
        return [], set()

    normalized_hosts: List[Dict[str, Any]] = []
    seen_host_ids: Set[str] = set()
    valid_host_ids: Set[str] = set()

    for idx, raw_host in enumerate(raw_hosts):
        host_path = f"{path}[{idx}]"
        if not isinstance(raw_host, dict):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_host_object",
                    path=host_path,
                    message="Cada item de hosts deve ser objeto.",
                    cause="Entrada de host em formato invalido.",
                    impact="Mapeamento host->componentes fica indefinido.",
                    recommendation="Corrigir item para objeto com campos obrigatorios.",
                )
            )
            continue

        host_id = _ensure_required_string(raw_host, "host_id", issues, f"{host_path}.host_id")
        host_address = _ensure_required_string(
            raw_host, "host_address", issues, f"{host_path}.host_address"
        )
        docker_endpoint = _ensure_required_string(
            raw_host, "docker_endpoint", issues, f"{host_path}.docker_endpoint"
        )
        connection_profile_ref = _ensure_required_string(
            raw_host,
            "connection_profile_ref",
            issues,
            f"{host_path}.connection_profile_ref",
        )

        if host_id and not IDENTIFIER_REGEX.fullmatch(host_id):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_host_id",
                    path=f"{host_path}.host_id",
                    message="host_id invalido.",
                    cause=f"Valor '{host_id}' fora do padrao de identificador.",
                    impact="Mapeamento host->componentes fica ambiguo para reconciliacao.",
                    recommendation="Usar host_id em minusculo com hifen opcional.",
                )
            )

        if host_address and not _is_valid_host_address(host_address):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_host_address",
                    path=f"{host_path}.host_address",
                    message="host_address deve ser IP ou hostname/FQDN valido.",
                    cause=f"Endereco '{host_address}' fora do padrao aceito.",
                    impact="Executor nao consegue resolver o host de provisionamento.",
                    recommendation="Informar host_address com IPv4/IPv6 ou hostname valido.",
                )
            )

        if docker_endpoint and not _is_valid_docker_endpoint(docker_endpoint):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_host_docker_endpoint",
                    path=f"{host_path}.docker_endpoint",
                    message="docker_endpoint invalido.",
                    cause=(
                        "Endpoint Docker fora do formato suportado "
                        "(tcp://host:port, ssh://host:port, unix:///path)."
                    ),
                    impact="Provisionamento nao consegue operar Docker remoto/local neste host.",
                    recommendation="Ajustar docker_endpoint para schema suportado e porta valida.",
                )
            )

        if connection_profile_ref and not PROFILE_REF_REGEX.fullmatch(connection_profile_ref):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_host_connection_profile_ref",
                    path=f"{host_path}.connection_profile_ref",
                    message="connection_profile_ref possui formato invalido.",
                    cause=f"Valor '{connection_profile_ref}' fora do padrao permitido.",
                    impact="Nao e possivel correlacionar host com perfil de conexao oficial.",
                    recommendation="Usar referencia alfanumerica com '-', '_', '/', '.'.",
                )
            )

        ssh_port = _to_positive_int(raw_host.get("ssh_port"))
        if ssh_port is None or ssh_port > 65535:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_host_ssh_port",
                    path=f"{host_path}.ssh_port",
                    message="ssh_port deve ser inteiro entre 1 e 65535.",
                    cause="Porta SSH do host fora de faixa valida.",
                    impact="Executor SSH nao conseguira conectar no host alvo.",
                    recommendation="Informar porta SSH valida no intervalo permitido.",
                )
            )
            ssh_port = 0

        if host_id:
            host_id_key = host_id.lower()
            if host_id_key in seen_host_ids:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="duplicate_host_id",
                        path=f"{host_path}.host_id",
                        message=f"host_id '{host_id}' duplicado.",
                        cause="Dois hosts com mesmo identificador logico.",
                        impact="Componentes podem ser mapeados para host incorreto.",
                        recommendation="Garantir host_id unico no manifesto.",
                    )
                )
            else:
                seen_host_ids.add(host_id_key)
                valid_host_ids.add(host_id_key)

        labels = raw_host.get("labels", {})
        normalized_labels: Dict[str, str] = {}
        if isinstance(labels, dict):
            for label_key, label_value in sorted(labels.items(), key=lambda kv: str(kv[0])):
                key_text = str(label_key).strip()
                value_text = str(label_value).strip()
                if key_text:
                    normalized_labels[key_text] = value_text
        elif labels is not None:
            issues.append(
                ManifestIssue(
                    level="warning",
                    code="invalid_host_labels",
                    path=f"{host_path}.labels",
                    message="labels ignorado por nao ser objeto.",
                    cause="Bloco labels em formato nao suportado.",
                    impact="Metadados de host nao serao aplicados.",
                    recommendation="Usar labels como objeto chave->valor.",
                )
            )

        normalized_hosts.append(
            {
                "host_id": host_id,
                "host_address": host_address,
                "ssh_port": ssh_port,
                "docker_endpoint": docker_endpoint,
                "connection_profile_ref": connection_profile_ref,
                "labels": normalized_labels,
            }
        )

    normalized_hosts.sort(key=lambda item: item.get("host_id", ""))
    return normalized_hosts, valid_host_ids


def _validate_components(
    manifest: Dict[str, Any],
    domain_lower: str,
    org_id: str,
    org_label: str,
    org_roles: Iterable[str],
    scoped_roles: Iterable[str],
    scoped_channels: Set[str],
    valid_host_ids: Set[str],
    issues: List[ManifestIssue],
) -> Tuple[List[Dict[str, Any]], Dict[str, int], Dict[str, Any]]:
    path = "components"
    raw_components = manifest.get("components")
    if not isinstance(raw_components, list) or not raw_components:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_components_block",
                path=path,
                message="components deve ser lista nao vazia.",
                cause="Manifesto sem componentes de runtime definidos.",
                impact="Nao e possivel atingir baseline minima do A2.",
                recommendation="Definir componentes peer/orderer/ca/couch/api_gateway/network_api.",
            )
        )
        return [], {}, {
            "peer_component_ids": set(),
            "peer_component_names": set(),
            "peer_id_to_name": {},
            "peer_name_to_id": {},
            "active_peer_component_ids": set(),
            "active_peer_component_names": set(),
            "active_peer_id_to_name": {},
            "active_peer_name_to_id": {},
        }

    org_id_token = _normalize_ascii_token(org_id)
    org_label_token = _normalize_ascii_token(org_label)
    org_token = org_id_token or org_label_token
    if not org_token:
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_org_naming_token",
                path="org_id",
                message="Nao foi possivel derivar token canonico da organizacao para naming.",
                cause="org_id/org_label sem caracteres validos apos normalizacao.",
                impact="Validacao deterministica de naming fica indisponivel.",
                recommendation="Ajustar org_id/org_label com caracteres alfanumericos validos.",
            )
        )
    org_label_upper = _normalize_org_label_upper(org_label)
    normalized_components: List[Dict[str, Any]] = []
    counts = {component_type: 0 for component_type in ALLOWED_COMPONENT_TYPES}
    active_counts = {component_type: 0 for component_type in ALLOWED_COMPONENT_TYPES}
    seen_component_ids: Set[str] = set()
    seen_component_names: Set[str] = set()
    host_port_index: Dict[Tuple[str, int], str] = {}
    org_roles_set = {role.lower() for role in org_roles}
    scoped_roles_set = {role.lower() for role in scoped_roles}
    if scoped_roles_set:
        role_context = "source_blueprint_scope.roles"
        effective_roles_set = scoped_roles_set
    else:
        role_context = "org_identity.roles"
        effective_roles_set = org_roles_set
    peer_component_ids: Set[str] = set()
    peer_component_names: Set[str] = set()
    peer_id_to_name: Dict[str, str] = {}
    peer_name_to_id: Dict[str, str] = {}
    active_peer_component_ids: Set[str] = set()
    active_peer_component_names: Set[str] = set()
    active_peer_id_to_name: Dict[str, str] = {}
    active_peer_name_to_id: Dict[str, str] = {}
    declared_runtime_chaincodes: Set[str] = set()
    raw_declared_runtimes = manifest.get("chaincode_runtimes", [])
    if isinstance(raw_declared_runtimes, list):
        for raw_runtime in raw_declared_runtimes:
            if not isinstance(raw_runtime, dict):
                continue
            raw_chaincode = raw_runtime.get("chaincode_id")
            if not isinstance(raw_chaincode, str):
                continue
            chaincode_id = raw_chaincode.strip().lower()
            if CHAINCODE_ID_REGEX.fullmatch(chaincode_id):
                declared_runtime_chaincodes.add(chaincode_id)

    for idx, raw_component in enumerate(raw_components):
        component_path = f"{path}[{idx}]"
        if not isinstance(raw_component, dict):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_object",
                    path=component_path,
                    message="Cada item de components deve ser objeto.",
                    cause="Entrada de componente fora do formato esperado.",
                    impact="Componente nao pode ser provisionado nem reconciliado.",
                    recommendation="Corrigir item para objeto com campos obrigatorios.",
                )
            )
            continue

        component_id = _ensure_required_string(
            raw_component, "component_id", issues, f"{component_path}.component_id"
        )
        if component_id and not IDENTIFIER_REGEX.fullmatch(component_id):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_id",
                    path=f"{component_path}.component_id",
                    message="component_id invalido.",
                    cause=f"Valor '{component_id}' fora do padrao de identificador.",
                    impact="Incremento e reconciliacao podem colidir.",
                    recommendation="Usar component_id em minusculo com hifen opcional.",
                )
            )
        component_id_key = component_id.lower()
        if component_id_key and component_id_key in seen_component_ids:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="duplicate_component_id",
                    path=f"{component_path}.component_id",
                    message=f"component_id '{component_id}' duplicado.",
                    cause="Dois componentes compartilham o mesmo identificador.",
                    impact="Nao ha enderecamento univoco de componentes.",
                    recommendation="Garantir component_id unico no manifesto.",
                )
            )
        elif component_id_key:
            seen_component_ids.add(component_id_key)

        component_type = _ensure_required_string(
            raw_component, "component_type", issues, f"{component_path}.component_type"
        ).lower()
        if component_type and component_type not in ALLOWED_COMPONENT_TYPES:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_type",
                    path=f"{component_path}.component_type",
                    message=f"component_type '{component_type}' invalido.",
                    cause="Tipo de componente fora do contrato A2.",
                    impact="Provisionador nao sabe como tratar o componente.",
                    recommendation=(
                        "Usar tipos permitidos: peer, orderer, ca, couch, api_gateway, "
                        "network_api, cc_webclient."
                    ),
                )
            )
            component_type = ""
        if component_type:
            counts[component_type] += 1

        host_id = _ensure_required_string(raw_component, "host_id", issues, f"{component_path}.host_id")
        if host_id and not IDENTIFIER_REGEX.fullmatch(host_id):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_host_id",
                    path=f"{component_path}.host_id",
                    message="host_id do componente invalido.",
                    cause=f"Valor '{host_id}' fora do padrao de identificador.",
                    impact="Mapeamento de componente para host fica ambiguo.",
                    recommendation="Usar host_id em minusculo com hifen opcional.",
                )
            )
        if host_id and host_id.lower() not in valid_host_ids:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="component_host_not_found",
                    path=f"{component_path}.host_id",
                    message=f"host_id '{host_id}' nao existe em hosts[].",
                    cause="Componente aponta para host inexistente no manifesto.",
                    impact="Execucao SSH nao consegue alocar o componente no destino correto.",
                    recommendation="Usar host_id valido previamente definido em hosts[].",
                )
            )

        name = _ensure_required_string(raw_component, "name", issues, f"{component_path}.name")
        image = _ensure_required_string(raw_component, "image", issues, f"{component_path}.image")
        env_profile = _ensure_required_string(
            raw_component, "env_profile", issues, f"{component_path}.env_profile"
        )
        storage_profile = _ensure_required_string(
            raw_component, "storage_profile", issues, f"{component_path}.storage_profile"
        )

        if name:
            name_key = name.lower()
            if name_key in seen_component_names:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="duplicate_component_name",
                        path=f"{component_path}.name",
                        message=f"name '{name}' duplicado em components[].",
                        cause="Dois componentes compartilham o mesmo nome operacional.",
                        impact="Reconciliacao por nome pode atuar no componente errado.",
                        recommendation="Garantir names unicos no manifesto.",
                    )
                )
            else:
                seen_component_names.add(name_key)

        if image and not IMAGE_REF_REGEX.fullmatch(image):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_image",
                    path=f"{component_path}.image",
                    message="image possui formato invalido.",
                    cause=f"Imagem '{image}' fora do padrao de referencia de container.",
                    impact="Provisionador pode falhar ao resolver/puxar a imagem.",
                    recommendation=(
                        "Usar referencia valida, ex.: repo/image:tag ou repo/image@sha256:<hash>."
                    ),
                )
            )
        if image and any(char.isspace() for char in image):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_image_whitespace",
                    path=f"{component_path}.image",
                    message="image nao pode conter espacos em branco.",
                    cause="Referencia de imagem com espacos.",
                    impact="Comando de runtime nao consegue interpretar a imagem corretamente.",
                    recommendation="Remover espacos e usar formato canonico de image reference.",
                )
            )

        if env_profile and not PROFILE_REF_REGEX.fullmatch(env_profile):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_env_profile",
                    path=f"{component_path}.env_profile",
                    message="env_profile possui formato invalido.",
                    cause=f"Valor '{env_profile}' fora do padrao permitido.",
                    impact="Provisionador nao consegue correlacionar perfil de ambiente.",
                    recommendation="Usar referencia alfanumerica com '-', '_', '/', '.'.",
                )
            )

        if storage_profile and not PROFILE_REF_REGEX.fullmatch(storage_profile):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_storage_profile",
                    path=f"{component_path}.storage_profile",
                    message="storage_profile possui formato invalido.",
                    cause=f"Valor '{storage_profile}' fora do padrao permitido.",
                    impact="Reconciliador nao consegue aplicar perfil de armazenamento correto.",
                    recommendation="Usar referencia alfanumerica com '-', '_', '/', '.'.",
                )
            )

        desired_state = str(raw_component.get("desired_state", "")).strip().lower()
        if not desired_state:
            desired_state = "required" if component_type in REQUIRED_COMPONENT_TYPES else "optional"
        if desired_state not in ALLOWED_DESIRED_STATES:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_desired_state",
                    path=f"{component_path}.desired_state",
                    message="desired_state invalido.",
                    cause=f"Valor '{desired_state}' fora do conjunto permitido.",
                    impact="Reconciliacao pode adotar estrategia incorreta.",
                    recommendation="Usar desired_state planned|required|optional.",
                )
            )
            desired_state = "planned"

        criticality = str(raw_component.get("criticality", "")).strip().lower()
        if not criticality:
            criticality = "critical" if component_type in REQUIRED_COMPONENT_TYPES else "supporting"
        if criticality not in ALLOWED_CRITICALITIES:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_criticality",
                    path=f"{component_path}.criticality",
                    message="criticality invalida.",
                    cause=f"Valor '{criticality}' fora do conjunto permitido.",
                    impact="Priorizacao de reconciliacao pode ficar incorreta.",
                    recommendation="Usar criticality critical|supporting.",
                )
            )
            criticality = "supporting"

        if component_type:
            _validate_component_name(
                component_type=component_type,
                name=name,
                domain_lower=domain_lower,
                org_token=org_token,
                org_label_upper=org_label_upper,
                issues=issues,
                path=f"{component_path}.name",
            )

        required_role_by_type = {
            "peer": "peer",
            "couch": "peer",
            "api_gateway": "peer",
            "network_api": "peer",
            "cc_webclient": "peer",
            "orderer": "orderer",
            "ca": "ca",
        }
        required_role = required_role_by_type.get(component_type)
        if required_role and required_role not in effective_roles_set:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="incompatible_org_role",
                    path=f"{component_path}.component_type",
                    message=f"Componente '{component_type}' exige role {required_role} na organizacao.",
                    cause=f"Role {required_role} ausente em {role_context}.",
                    impact="Topologia desejada diverge das capacidades declaradas da org.",
                    recommendation=f"Incluir role {required_role} ou remover componente incompativel.",
                )
            )

        raw_service_context = raw_component.get("service_context")
        normalized_service_context: Dict[str, Any] = {}
        if component_type in {"api_gateway", "network_api"} and raw_service_context is not None:
            context_path = f"{component_path}.service_context"
            if not isinstance(raw_service_context, dict):
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="invalid_component_service_context",
                        path=context_path,
                        message="service_context deve ser objeto para api_gateway/network_api.",
                        cause="Campo service_context fora do formato esperado.",
                        impact="Nao e possivel validar escopo org/canal/chaincode da API.",
                        recommendation="Definir service_context como objeto com org_id, channel_ids e chaincode_ids.",
                    )
                )
            else:
                service_org_id = _ensure_required_string(
                    raw_service_context,
                    "org_id",
                    issues,
                    f"{context_path}.org_id",
                ).lower()
                if service_org_id and not IDENTIFIER_REGEX.fullmatch(service_org_id):
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="invalid_component_service_context_org_id",
                            path=f"{context_path}.org_id",
                            message="org_id invalido em service_context.",
                            cause=f"Valor '{service_org_id}' fora do padrao de identificador.",
                            impact="Contexto da API pode apontar para org inexistente.",
                            recommendation="Usar org_id em minusculo com hifen opcional.",
                        )
                    )
                if service_org_id and org_id and service_org_id != org_id:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="component_service_context_org_mismatch",
                            path=f"{context_path}.org_id",
                            message="service_context.org_id diverge de org_id do manifesto.",
                            cause=f"service_context.org_id='{service_org_id}' e org_id='{org_id}'.",
                            impact="Provisionamento da API pode ser direcionado para org incorreta.",
                            recommendation="Alinhar service_context.org_id ao org_id do manifesto.",
                        )
                    )

                raw_context_channels = raw_service_context.get("channel_ids")
                context_channels: List[str] = []
                if not isinstance(raw_context_channels, list) or not raw_context_channels:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="invalid_component_service_context_channels",
                            path=f"{context_path}.channel_ids",
                            message="channel_ids deve ser lista nao vazia em service_context.",
                            cause="API sem declaracao valida de canais de contexto.",
                            impact="Gateway/API de rede pode operar fora do escopo previsto.",
                            recommendation="Informar channel_ids validos associados a organizacao.",
                        )
                    )
                else:
                    seen_context_channels: Set[str] = set()
                    for channel_idx, raw_channel in enumerate(raw_context_channels):
                        channel_path = f"{context_path}.channel_ids[{channel_idx}]"
                        if not isinstance(raw_channel, str) or not raw_channel.strip():
                            issues.append(
                                ManifestIssue(
                                    level="error",
                                    code="invalid_component_service_context_channel_item",
                                    path=channel_path,
                                    message="Cada channel_id em service_context deve ser string nao vazia.",
                                    cause="Item de canal invalido no contexto da API.",
                                    impact="Escopo de canal da API fica ambiguo.",
                                    recommendation="Corrigir channel_id para valor textual valido.",
                                )
                            )
                            continue
                        channel_id = raw_channel.strip().lower()
                        if not CHANNEL_ID_REGEX.fullmatch(channel_id):
                            issues.append(
                                ManifestIssue(
                                    level="error",
                                    code="invalid_component_service_context_channel_value",
                                    path=channel_path,
                                    message="channel_id invalido em service_context.",
                                    cause=f"Valor '{raw_channel}' fora do padrao de channel_id.",
                                    impact="API pode ser configurada com canal invalido.",
                                    recommendation="Usar channel_id em minusculo com hifen opcional.",
                                )
                            )
                            continue
                        if scoped_channels and channel_id not in scoped_channels:
                            issues.append(
                                ManifestIssue(
                                    level="error",
                                    code="component_service_context_channel_not_in_scope",
                                    path=channel_path,
                                    message=f"channel_id '{channel_id}' fora de source_blueprint_scope.channels.",
                                    cause="Contexto da API referencia canal nao associado a organizacao.",
                                    impact="API pode ser provisionada para canal indevido.",
                                    recommendation="Usar apenas channel_ids presentes no escopo publicado.",
                                )
                            )
                        if channel_id not in seen_context_channels:
                            context_channels.append(channel_id)
                            seen_context_channels.add(channel_id)

                raw_context_chaincodes = raw_service_context.get("chaincode_ids")
                context_chaincodes: List[str] = []
                if not isinstance(raw_context_chaincodes, list) or not raw_context_chaincodes:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="invalid_component_service_context_chaincodes",
                            path=f"{context_path}.chaincode_ids",
                            message="chaincode_ids deve ser lista nao vazia em service_context.",
                            cause="API sem declaracao valida de chaincodes de contexto.",
                            impact="Gateway/API de rede pode operar sem escopo de chaincode definido.",
                            recommendation="Informar chaincode_ids validos para o contexto da API.",
                        )
                    )
                else:
                    seen_context_chaincodes: Set[str] = set()
                    for chaincode_idx, raw_chaincode in enumerate(raw_context_chaincodes):
                        chaincode_path = f"{context_path}.chaincode_ids[{chaincode_idx}]"
                        if not isinstance(raw_chaincode, str) or not raw_chaincode.strip():
                            issues.append(
                                ManifestIssue(
                                    level="error",
                                    code="invalid_component_service_context_chaincode_item",
                                    path=chaincode_path,
                                    message="Cada chaincode_id em service_context deve ser string nao vazia.",
                                    cause="Item de chaincode invalido no contexto da API.",
                                    impact="Escopo de chaincode da API fica ambiguo.",
                                    recommendation="Corrigir chaincode_id para valor textual valido.",
                                )
                            )
                            continue
                        chaincode_id = raw_chaincode.strip().lower()
                        if not CHAINCODE_ID_REGEX.fullmatch(chaincode_id):
                            issues.append(
                                ManifestIssue(
                                    level="error",
                                    code="invalid_component_service_context_chaincode_value",
                                    path=chaincode_path,
                                    message="chaincode_id invalido em service_context.",
                                    cause=f"Valor '{raw_chaincode}' fora do padrao de chaincode_id.",
                                    impact="API pode ser configurada com chaincode invalido.",
                                    recommendation="Usar chaincode_id em minusculo com hifen opcional.",
                                )
                            )
                            continue
                        if declared_runtime_chaincodes and chaincode_id not in declared_runtime_chaincodes:
                            issues.append(
                                ManifestIssue(
                                    level="error",
                                    code="component_service_context_chaincode_not_declared",
                                    path=chaincode_path,
                                    message=f"chaincode_id '{chaincode_id}' nao consta em chaincode_runtimes.",
                                    cause="Contexto da API referencia chaincode sem runtime declarado.",
                                    impact="API pode ser provisionada com binding invalido de chaincode.",
                                    recommendation="Declarar runtime correspondente em chaincode_runtimes.",
                                )
                            )
                        if chaincode_id not in seen_context_chaincodes:
                            context_chaincodes.append(chaincode_id)
                            seen_context_chaincodes.add(chaincode_id)

                normalized_service_context = {
                    "org_id": service_org_id,
                    "channel_ids": sorted(context_channels),
                    "chaincode_ids": sorted(context_chaincodes),
                }

        raw_ports = raw_component.get("ports")
        normalized_ports: List[int] = []
        seen_local_ports: Set[int] = set()
        if not isinstance(raw_ports, list) or not raw_ports:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_ports",
                    path=f"{component_path}.ports",
                    message="ports deve ser lista nao vazia de inteiros entre 1 e 65535.",
                    cause="Componente sem portas validas para exposicao operacional.",
                    impact="Nao e possivel conectar ou validar saude do componente.",
                    recommendation="Informar lista de portas numericas validas.",
                )
            )
        else:
            for port_idx, raw_port in enumerate(raw_ports):
                port_path = f"{component_path}.ports[{port_idx}]"
                parsed_port = _to_positive_int(raw_port)
                if parsed_port is None or parsed_port > 65535:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="invalid_component_port_value",
                            path=port_path,
                            message="Porta invalida em components[].ports.",
                            cause=f"Valor '{raw_port}' fora da faixa permitida.",
                            impact="Publicacao de portas fica inconsistente.",
                            recommendation="Usar porta inteira no intervalo 1..65535.",
                        )
                    )
                    continue
                if parsed_port in seen_local_ports:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="duplicate_component_port",
                            path=port_path,
                            message="Porta duplicada no mesmo componente.",
                            cause=f"Porta {parsed_port} repetida em ports[].",
                            impact="Declaracao de rede do componente fica redundante/invalida.",
                            recommendation="Remover portas duplicadas dentro do componente.",
                        )
                    )
                    continue
                seen_local_ports.add(parsed_port)
                normalized_ports.append(parsed_port)

                if host_id and desired_state != "planned":
                    host_port_key = (host_id.lower(), parsed_port)
                    previous_component = host_port_index.get(host_port_key)
                    if previous_component and previous_component != component_id:
                        issues.append(
                            ManifestIssue(
                                level="error",
                                code="host_port_conflict",
                                path=port_path,
                                message=(
                                    f"Conflito de porta ativa no host '{host_id}': porta {parsed_port} "
                                    f"ja usada por '{previous_component}'."
                                ),
                                cause=(
                                    "Dois componentes ativos mapeados para mesma porta "
                                    "no mesmo host."
                                ),
                                impact="Provisionamento pode falhar ou sobrescrever binding existente.",
                                recommendation=(
                                    "Ajustar portas de componentes ativos para evitar colisao no host."
                                ),
                            )
                        )
                    else:
                        host_port_index[host_port_key] = component_id

        resources_raw = raw_component.get("resources")
        normalized_resources = {"cpu": 0, "memory_mb": 0, "disk_gb": 0}
        if not isinstance(resources_raw, dict):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_component_resources",
                    path=f"{component_path}.resources",
                    message="resources deve ser objeto com cpu, memory_mb e disk_gb (>0).",
                    cause="Recursos do componente nao foram declarados corretamente.",
                    impact="Planejamento de capacidade e reconciliacao ficam incompletos.",
                    recommendation="Informar resources com cpu/memory_mb/disk_gb positivos.",
                )
            )
        else:
            for resource_name in ("cpu", "memory_mb", "disk_gb"):
                resource_value = _to_positive_int(resources_raw.get(resource_name))
                if resource_value is None:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="invalid_component_resource_value",
                            path=f"{component_path}.resources.{resource_name}",
                            message=f"resources.{resource_name} deve ser inteiro > 0.",
                            cause=f"Valor invalido para recurso '{resource_name}'.",
                            impact="Dimensionamento do componente fica inconsistente.",
                            recommendation=f"Informar resources.{resource_name} como inteiro positivo.",
                        )
                    )
                    resource_value = 0
                normalized_resources[resource_name] = resource_value

        reconcile_hints_raw = raw_component.get("reconcile_hints", {})
        normalized_reconcile_hints: Dict[str, Any] = {}
        if isinstance(reconcile_hints_raw, dict):
            for hint_key, hint_value in sorted(reconcile_hints_raw.items(), key=lambda kv: str(kv[0])):
                key_text = str(hint_key).strip()
                if key_text:
                    normalized_reconcile_hints[key_text] = hint_value
        elif reconcile_hints_raw is not None:
            issues.append(
                ManifestIssue(
                    level="warning",
                    code="invalid_component_reconcile_hints",
                    path=f"{component_path}.reconcile_hints",
                    message="reconcile_hints ignorado por nao ser objeto.",
                    cause="Campo de hints em formato nao suportado.",
                    impact="Reconciliador perde pistas auxiliares de convergencia.",
                    recommendation="Usar reconcile_hints como objeto chave->valor.",
                )
            )

        normalized_component = {
            "component_id": component_id,
            "component_type": component_type,
            "host_id": host_id,
            "name": name,
            "image": image,
            "ports": sorted(normalized_ports),
            "env_profile": env_profile,
            "storage_profile": storage_profile,
            "resources": normalized_resources,
            "desired_state": desired_state,
            "criticality": criticality,
            "service_context": normalized_service_context,
            "reconcile_hints": normalized_reconcile_hints,
        }
        normalized_components.append(normalized_component)

        if component_type and desired_state != "planned":
            active_counts[component_type] += 1

        if component_type == "peer":
            component_id_lower = component_id.lower() if component_id else ""
            name_lower = name.lower() if name else ""
            if component_id:
                peer_component_ids.add(component_id_lower)
            if name:
                peer_component_names.add(name_lower)
            if component_id and name:
                peer_id_to_name[component_id_lower] = name_lower
                peer_name_to_id[name_lower] = component_id_lower
            if desired_state != "planned":
                if component_id:
                    active_peer_component_ids.add(component_id_lower)
                if name:
                    active_peer_component_names.add(name_lower)
                if component_id and name:
                    active_peer_id_to_name[component_id_lower] = name_lower
                    active_peer_name_to_id[name_lower] = component_id_lower

    for required_type in sorted(REQUIRED_COMPONENT_TYPES):
        if active_counts.get(required_type, 0) < 1:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="cardinality_below_minimum",
                    path="components",
                    message=f"Cardinalidade minima ativa nao atendida para '{required_type}'.",
                    cause=(
                        f"Quantidade ativa de '{required_type}' abaixo de 1 "
                        f"(ativos={active_counts.get(required_type, 0)}, total={counts.get(required_type, 0)})."
                    ),
                    impact="Organizacao nao atinge baseline operacional minima do A2.",
                    recommendation=(
                        f"Incluir ao menos um componente ativo do tipo '{required_type}' "
                        "(desired_state diferente de planned)."
                    ),
                )
            )

    if active_counts.get("couch", 0) < active_counts.get("peer", 0):
        issues.append(
            ManifestIssue(
                level="error",
                code="couch_peer_pairing_invalid",
                path="components",
                message="Cardinalidade ativa couch x peer invalida (couch < peer).",
                cause=(
                    "Nao existe ao menos um couch ativo para cada peer ativo provisionado "
                    f"(peers ativos={active_counts.get('peer', 0)}, couches ativos={active_counts.get('couch', 0)})."
                ),
                impact="State database padrao do chaincode pode ficar incompleta.",
                recommendation=(
                    "Adicionar componentes couch ativos para cobrir todos os peers ativos "
                    "(desired_state diferente de planned)."
                ),
            )
        )

    normalized_components.sort(key=lambda item: item.get("component_id", ""))
    peer_registry = {
        "peer_component_ids": peer_component_ids,
        "peer_component_names": peer_component_names,
        "peer_id_to_name": peer_id_to_name,
        "peer_name_to_id": peer_name_to_id,
        "active_peer_component_ids": active_peer_component_ids,
        "active_peer_component_names": active_peer_component_names,
        "active_peer_id_to_name": active_peer_id_to_name,
        "active_peer_name_to_id": active_peer_name_to_id,
    }
    return normalized_components, counts, peer_registry


def _validate_chaincode_runtimes(
    manifest: Dict[str, Any],
    peer_registry: Dict[str, Any],
    scoped_channels: Set[str],
    domain_lower: str,
    org_token: str,
    issues: List[ManifestIssue],
) -> List[Dict[str, Any]]:
    path = "chaincode_runtimes"
    raw_runtimes = manifest.get("chaincode_runtimes", [])
    if raw_runtimes is None:
        raw_runtimes = []
    if not isinstance(raw_runtimes, list):
        issues.append(
            ManifestIssue(
                level="error",
                code="invalid_chaincode_runtimes_block",
                path=path,
                message="chaincode_runtimes deve ser lista.",
                cause="Bloco de runtime de chaincodes fora do formato esperado.",
                impact="Nao e possivel validar bootstrap de runtime do A2.",
                recommendation="Definir chaincode_runtimes como lista de objetos.",
            )
        )
        return []

    normalized_runtimes: List[Dict[str, Any]] = []
    peer_ids = peer_registry.get("peer_component_ids", set())
    peer_names = peer_registry.get("peer_component_names", set())
    peer_id_to_name = peer_registry.get("peer_id_to_name", {})
    peer_name_to_id = peer_registry.get("peer_name_to_id", {})
    active_peer_ids = peer_registry.get("active_peer_component_ids", set())
    active_peer_names = peer_registry.get("active_peer_component_names", set())
    seen_runtime_names: Set[str] = set()
    seen_runtime_bindings: Set[Tuple[str, str, str, str]] = set()

    for idx, raw_runtime in enumerate(raw_runtimes):
        runtime_path = f"{path}[{idx}]"
        if not isinstance(raw_runtime, dict):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_object",
                    path=runtime_path,
                    message="Cada item de chaincode_runtimes deve ser objeto.",
                    cause="Entrada de runtime em formato invalido.",
                    impact="Runtime de chaincode nao pode ser reconciliado.",
                    recommendation="Corrigir item para objeto com campos obrigatorios.",
                )
            )
            continue

        channel_id = _ensure_required_string(
            raw_runtime, "channel_id", issues, f"{runtime_path}.channel_id"
        ).lower()
        if channel_id and not CHANNEL_ID_REGEX.fullmatch(channel_id):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_channel",
                    path=f"{runtime_path}.channel_id",
                    message="channel_id invalido em chaincode_runtimes.",
                    cause=f"Valor '{channel_id}' fora do padrao de canal.",
                    impact="Vinculo runtime->canal fica inconsistente.",
                    recommendation="Usar channel_id em minusculo com hifen opcional.",
                )
            )
        if channel_id and scoped_channels and channel_id not in scoped_channels:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="chaincode_runtime_channel_not_in_scope",
                    path=f"{runtime_path}.channel_id",
                    message=f"channel_id '{channel_id}' nao pertence a source_blueprint_scope.channels.",
                    cause="Runtime de chaincode aponta para canal fora do escopo publicado da org.",
                    impact="Provisionamento pode tentar bootstrap em canal nao associado a organizacao.",
                    recommendation="Usar channel_id presente em source_blueprint_scope.channels.",
                )
            )

        chaincode_id = _ensure_required_string(
            raw_runtime, "chaincode_id", issues, f"{runtime_path}.chaincode_id"
        ).lower()
        if chaincode_id and not CHAINCODE_ID_REGEX.fullmatch(chaincode_id):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_chaincode_id",
                    path=f"{runtime_path}.chaincode_id",
                    message="chaincode_id invalido em chaincode_runtimes.",
                    cause=f"Valor '{chaincode_id}' fora do padrao esperado.",
                    impact="Roteamento runtime->chaincode fica ambiguo.",
                    recommendation="Usar chaincode_id em minusculo com hifen opcional.",
                )
            )

        version = _ensure_required_string(raw_runtime, "version", issues, f"{runtime_path}.version")
        if version and not CHAINCODE_VERSION_REGEX.fullmatch(version):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_version",
                    path=f"{runtime_path}.version",
                    message="version invalida em chaincode_runtimes.",
                    cause=f"Versao '{version}' fora do padrao permitido.",
                    impact="Naming do runtime de chaincode pode divergir do esperado.",
                    recommendation="Usar versao com caracteres [A-Za-z0-9._-].",
                )
            )

        target_peer = _ensure_required_string(
            raw_runtime, "target_peer", issues, f"{runtime_path}.target_peer"
        )
        target_peer_key = target_peer.lower()
        resolved_target_peer_name = ""
        canonical_target_peer_id = ""
        if target_peer_key in peer_id_to_name:
            resolved_target_peer_name = str(peer_id_to_name.get(target_peer_key, "")).lower()
            canonical_target_peer_id = target_peer_key
        elif target_peer_key in peer_names:
            resolved_target_peer_name = target_peer_key
            canonical_target_peer_id = str(peer_name_to_id.get(target_peer_key, "")).lower()
        target_peer_exists = bool(target_peer_key and (target_peer_key in peer_ids or target_peer_key in peer_names))
        target_peer_active = bool(
            target_peer_key
            and (
                target_peer_key in active_peer_ids
                or target_peer_key in active_peer_names
                or (canonical_target_peer_id and canonical_target_peer_id in active_peer_ids)
            )
        )
        if target_peer_key and not target_peer_exists:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_target_peer",
                    path=f"{runtime_path}.target_peer",
                    message=f"target_peer '{target_peer}' nao encontrado entre peers do manifesto.",
                    cause="Runtime de chaincode aponta para peer inexistente.",
                    impact="Bootstrap do runtime falha por ausencia do peer alvo.",
                    recommendation="Usar target_peer por component_id ou name de peer existente.",
                )
            )
        elif target_peer_key and not target_peer_active:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="chaincode_runtime_target_peer_not_active",
                    path=f"{runtime_path}.target_peer",
                    message=f"target_peer '{target_peer}' existe, mas nao esta ativo para runtime.",
                    cause="Runtime de chaincode aponta para peer com desired_state=planned.",
                    impact="Bootstrap de runtime pode ocorrer em peer fora da topologia ativa.",
                    recommendation="Usar target_peer de peer ativo (desired_state diferente de planned).",
                )
            )

        package_id_hash = _ensure_required_string(
            raw_runtime,
            "package_id_hash",
            issues,
            f"{runtime_path}.package_id_hash",
        ).lower()
        if package_id_hash and not SHA256_REGEX.fullmatch(package_id_hash):
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_package_hash",
                    path=f"{runtime_path}.package_id_hash",
                    message="package_id_hash deve ser SHA-256 canonico (64 hex minusculo).",
                    cause="Hash do pacote de chaincode fora do padrao.",
                    impact="Runtime nao pode ser correlacionado com pacote instalado.",
                    recommendation="Informar package_id_hash com 64 caracteres hexadecimais minusculos.",
                )
            )

        runtime_name = _ensure_required_string(
            raw_runtime, "runtime_name", issues, f"{runtime_path}.runtime_name"
        )
        runtime_name_lower = runtime_name.lower()
        runtime_match = CHAINCODE_RUNTIME_NAME_REGEX.fullmatch(runtime_name)
        if runtime_name and not runtime_match:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="invalid_chaincode_runtime_name",
                    path=f"{runtime_path}.runtime_name",
                    message="runtime_name invalido para padrao dev-peer{n}.",
                    cause=f"Nome '{runtime_name}' fora da convencao estabelecida no A2.",
                    impact="Inventario e reconciliacao de runtime perdem previsibilidade.",
                    recommendation=(
                        "Usar padrao dev-peer{n}.{organization}.{domain}-{chaincode}_{version}-{package_id_hash}."
                    ),
                )
            )

        runtime_hash_segment = runtime_name_lower.rsplit("-", 1)[-1] if runtime_name else ""
        if runtime_match:
            runtime_org = runtime_match.group("org")
            runtime_domain = runtime_match.group("domain")
            runtime_chaincode = runtime_match.group("chaincode")
            runtime_version = runtime_match.group("version")
            runtime_hash_segment = runtime_match.group("hash")

            if org_token and runtime_org != org_token:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="chaincode_runtime_org_mismatch",
                        path=f"{runtime_path}.runtime_name",
                        message="runtime_name referencia organization divergente do manifesto.",
                        cause=f"Organization em runtime_name='{runtime_org}', esperado='{org_token}'.",
                        impact="Reconciliacao de runtime por organizacao pode apontar para peer incorreto.",
                        recommendation="Gerar runtime_name com token de organizacao canonico do manifesto.",
                    )
                )

            if domain_lower and runtime_domain != domain_lower:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="chaincode_runtime_domain_mismatch",
                        path=f"{runtime_path}.runtime_name",
                        message="runtime_name referencia dominio divergente do manifesto.",
                        cause=f"Dominio em runtime_name='{runtime_domain}', esperado='{domain_lower}'.",
                        impact="Provisionamento pode tentar reconciliar runtime fora do dominio da org.",
                        recommendation="Gerar runtime_name com domain canonico do manifesto.",
                    )
                )

            if chaincode_id and runtime_chaincode != chaincode_id:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="chaincode_runtime_chaincode_mismatch",
                        path=f"{runtime_path}.runtime_name",
                        message="runtime_name referencia chaincode divergente de chaincode_id.",
                        cause=f"Chaincode em runtime_name='{runtime_chaincode}', chaincode_id='{chaincode_id}'.",
                        impact="Runtime pode ser reconciliado para chaincode diferente do declarado.",
                        recommendation="Sincronizar segmento chaincode de runtime_name com chaincode_id.",
                    )
                )

            if version and runtime_version != version:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="chaincode_runtime_version_mismatch",
                        path=f"{runtime_path}.runtime_name",
                        message="runtime_name referencia versao divergente de version.",
                        cause=f"Versao em runtime_name='{runtime_version}', version='{version}'.",
                        impact="Auditoria de release de chaincode fica inconsistente.",
                        recommendation="Sincronizar segmento de versao de runtime_name com version.",
                    )
                )

            if (
                resolved_target_peer_name
                and chaincode_id
                and version
                and package_id_hash
            ):
                expected_runtime_name = (
                    f"dev-{resolved_target_peer_name}-{chaincode_id}_{version}-{package_id_hash}"
                )
                if runtime_name != expected_runtime_name:
                    issues.append(
                        ManifestIssue(
                            level="error",
                            code="chaincode_runtime_name_not_deterministic",
                            path=f"{runtime_path}.runtime_name",
                            message="runtime_name nao corresponde ao padrao deterministico esperado.",
                            cause=(
                                f"Runtime recebido='{runtime_name}' e esperado='{expected_runtime_name}' "
                                "a partir de target_peer/chaincode/version/hash."
                            ),
                            impact="Incremento e reconciliacao de runtimes podem ficar ambiguos.",
                            recommendation="Gerar runtime_name deterministico conforme contrato A2.",
                        )
                    )

        if runtime_hash_segment and package_id_hash and runtime_hash_segment != package_id_hash:
            issues.append(
                ManifestIssue(
                    level="error",
                    code="chaincode_runtime_hash_mismatch",
                    path=f"{runtime_path}.runtime_name",
                    message="Hash final de runtime_name difere de package_id_hash.",
                    cause="Nome do runtime referencia pacote diferente do declarado.",
                    impact="Auditoria de runtime e pacote instalado fica inconsistente.",
                    recommendation="Sincronizar hash de runtime_name com package_id_hash.",
                )
            )

        runtime_name_key = runtime_name.lower()
        if runtime_name_key:
            if runtime_name_key in seen_runtime_names:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="duplicate_chaincode_runtime_name",
                        path=f"{runtime_path}.runtime_name",
                        message=f"runtime_name '{runtime_name}' duplicado em chaincode_runtimes.",
                        cause="Dois runtimes compartilham o mesmo nome operacional.",
                        impact="Bootstrap/reconciliacao de runtime pode atuar em container ambiguo.",
                        recommendation="Garantir runtime_name unico por runtime declarado.",
                    )
                )
            else:
                seen_runtime_names.add(runtime_name_key)

        canonical_binding_peer = canonical_target_peer_id or target_peer_key
        if channel_id and chaincode_id and version and canonical_binding_peer:
            binding_key = (channel_id, chaincode_id, version, canonical_binding_peer)
            if binding_key in seen_runtime_bindings:
                issues.append(
                    ManifestIssue(
                        level="error",
                        code="duplicate_chaincode_runtime_binding",
                        path=runtime_path,
                        message="Binding channel/chaincode/version/target_peer duplicado em chaincode_runtimes.",
                        cause=(
                            f"Duplicidade para ({channel_id}, {chaincode_id}, "
                            f"{version}, {canonical_binding_peer})."
                        ),
                        impact="Contrato de bootstrap fica ambiguo para runtime do chaincode.",
                        recommendation="Remover runtime duplicado ou ajustar binding para alvo distinto.",
                    )
                )
            else:
                seen_runtime_bindings.add(binding_key)

        normalized_runtimes.append(
            {
                "channel_id": channel_id,
                "chaincode_id": chaincode_id,
                "version": version,
                "target_peer": target_peer,
                "package_id_hash": package_id_hash,
                "runtime_name": runtime_name,
            }
        )

    normalized_runtimes.sort(
        key=lambda item: (
            item.get("channel_id", ""),
            item.get("chaincode_id", ""),
            item.get("runtime_name", ""),
        )
    )
    return normalized_runtimes


def _build_fingerprint_payload(
    metadata: Dict[str, str],
    source_blueprint_scope: Dict[str, Any],
    org_identity: Dict[str, Any],
    hosts: List[Dict[str, Any]],
    components: List[Dict[str, Any]],
    chaincode_runtimes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "manifest_name": metadata.get("manifest_name", ""),
        "manifest_version": metadata.get("manifest_version", ""),
        "generated_at": metadata.get("generated_at", ""),
        "change_id": metadata.get("change_id", ""),
        "run_id": metadata.get("run_id", ""),
        "org_id": metadata.get("org_id", ""),
        "org_label": metadata.get("org_label", ""),
        "domain": metadata.get("domain", ""),
        "environment_profile_ref": metadata.get("environment_profile_ref", ""),
        "source_blueprint_fingerprint": metadata.get("source_blueprint_fingerprint", ""),
        "source_blueprint_version": metadata.get("source_blueprint_version", ""),
        "orchestrator_context": metadata.get("orchestrator_context", ""),
        "source_blueprint_scope": source_blueprint_scope,
        "org_identity": org_identity,
        "hosts": hosts,
        "components": components,
        "chaincode_runtimes": chaincode_runtimes,
    }


def validate_org_runtime_manifest_block(
    manifest: Dict[str, Any],
    *,
    allow_schema_migration: bool = False,
) -> OrgRuntimeManifestValidationResult:
    issues: List[ManifestIssue] = []
    manifest_payload = manifest if isinstance(manifest, dict) else {}
    if not isinstance(manifest, dict):
        issues.append(
            ManifestIssue(
                level="error",
                code="manifest_not_object",
                path="manifest",
                message="Manifesto deve ser objeto JSON/YAML.",
                cause="Entrada recebida nao e objeto no nivel raiz.",
                impact="Validacao do OrgRuntimeManifest nao pode ser iniciada.",
                recommendation="Enviar manifesto como objeto JSON/YAML.",
            )
        )

    resolved_manifest_payload, resolved_manifest_version, migration_applied, migrated_from = (
        _resolve_manifest_schema_compatibility(
            manifest_payload,
            allow_schema_migration,
            issues,
        )
    )

    metadata = _validate_manifest_metadata(resolved_manifest_payload, issues)
    org_identity, org_roles = _validate_org_identity(
        resolved_manifest_payload,
        metadata_org_id=metadata.get("org_id", ""),
        issues=issues,
    )
    source_blueprint_scope, scoped_channels = _validate_source_blueprint_scope(
        resolved_manifest_payload,
        metadata=metadata,
        normalized_org_identity=org_identity,
        issues=issues,
    )
    hosts, valid_host_ids = _validate_hosts(resolved_manifest_payload, issues)

    components, _component_counts, peer_registry = _validate_components(
        manifest=resolved_manifest_payload,
        domain_lower=metadata.get("domain", ""),
        org_id=metadata.get("org_id", ""),
        org_label=metadata.get("org_label", ""),
        org_roles=org_roles,
        scoped_roles=source_blueprint_scope.get("roles", []),
        scoped_channels=scoped_channels,
        valid_host_ids=valid_host_ids,
        issues=issues,
    )
    runtime_org_token = _normalize_ascii_token(metadata.get("org_id", "")) or _normalize_ascii_token(
        metadata.get("org_label", "")
    )
    chaincode_runtimes = _validate_chaincode_runtimes(
        manifest=resolved_manifest_payload,
        peer_registry=peer_registry,
        scoped_channels=scoped_channels,
        domain_lower=metadata.get("domain", ""),
        org_token=runtime_org_token,
        issues=issues,
    )

    fingerprint_payload = _build_fingerprint_payload(
        metadata=metadata,
        source_blueprint_scope=source_blueprint_scope,
        org_identity=org_identity,
        hosts=hosts,
        components=components,
        chaincode_runtimes=chaincode_runtimes,
    )
    fingerprint = _compute_fingerprint(fingerprint_payload)

    sorted_issues = _sort_issues_deterministically(issues)
    valid = not any(issue.level == "error" for issue in sorted_issues)

    return OrgRuntimeManifestValidationResult(
        valid=valid,
        manifest_runtime_version=CURRENT_MANIFEST_VERSION,
        manifest_name=metadata.get("manifest_name", ""),
        manifest_version=metadata.get("manifest_version", ""),
        resolved_manifest_version=resolved_manifest_version or metadata.get("manifest_version", ""),
        migration_applied=migration_applied,
        migrated_from_manifest_version=migrated_from,
        generated_at=metadata.get("generated_at", ""),
        change_id=metadata.get("change_id", ""),
        run_id=metadata.get("run_id", ""),
        org_id=metadata.get("org_id", ""),
        org_label=metadata.get("org_label", ""),
        domain=metadata.get("domain", ""),
        environment_profile_ref=metadata.get("environment_profile_ref", ""),
        source_blueprint_fingerprint=metadata.get("source_blueprint_fingerprint", ""),
        source_blueprint_version=metadata.get("source_blueprint_version", ""),
        orchestrator_context=metadata.get("orchestrator_context", ""),
        normalized_source_blueprint_scope=source_blueprint_scope,
        normalized_org_identity=org_identity,
        normalized_hosts=hosts,
        normalized_components=components,
        normalized_chaincode_runtimes=chaincode_runtimes,
        manifest_fingerprint=fingerprint,
        fingerprint_sha256=fingerprint,
        issues=sorted_issues,
    )


def load_org_runtime_manifest(file_path: Path) -> Dict[str, Any]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Manifesto nao encontrado: {file_path}")
    raw_text = file_path.read_text(encoding="utf-8")
    suffix = file_path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(raw_text)
    elif suffix == ".json":
        data = json.loads(raw_text)
    else:
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            data = yaml.safe_load(raw_text)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError("Manifesto deve ser objeto no nivel raiz.")
    return data


def validate_org_runtime_manifest_file(
    file_path: Path,
    *,
    allow_schema_migration: bool = False,
) -> OrgRuntimeManifestValidationResult:
    manifest = load_org_runtime_manifest(file_path)
    return validate_org_runtime_manifest_block(
        manifest,
        allow_schema_migration=allow_schema_migration,
    )


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


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line)
        handle.write("\n")


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    entries: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def _storage_path_segment(value: str, *, field_name: str) -> str:
    token = str(value).strip()
    if not token:
        raise ValueError(f"{field_name} e obrigatorio para persistencia do manifesto.")
    if not MANIFEST_STORAGE_TOKEN_REGEX.fullmatch(token):
        raise ValueError(
            f"{field_name} invalido para persistencia (permitido: [A-Za-z0-9._:-], max 128)."
        )
    return quote(token, safe="")


def _build_normalized_manifest_from_validation(
    validation: OrgRuntimeManifestValidationResult,
) -> Dict[str, Any]:
    return {
        "manifest_name": validation.manifest_name,
        "manifest_version": validation.manifest_version,
        "resolved_manifest_version": validation.resolved_manifest_version,
        "manifest_runtime_version": validation.manifest_runtime_version,
        "migration_applied": validation.migration_applied,
        "migrated_from_manifest_version": validation.migrated_from_manifest_version,
        "generated_at": validation.generated_at,
        "change_id": validation.change_id,
        "run_id": validation.run_id,
        "org_id": validation.org_id,
        "org_label": validation.org_label,
        "domain": validation.domain,
        "environment_profile_ref": validation.environment_profile_ref,
        "source_blueprint_fingerprint": validation.source_blueprint_fingerprint,
        "source_blueprint_version": validation.source_blueprint_version,
        "orchestrator_context": validation.orchestrator_context,
        "source_blueprint_scope": validation.normalized_source_blueprint_scope,
        "org_identity": validation.normalized_org_identity,
        "hosts": validation.normalized_hosts,
        "components": validation.normalized_components,
        "chaincode_runtimes": validation.normalized_chaincode_runtimes,
        "manifest_fingerprint": validation.manifest_fingerprint,
        "fingerprint_sha256": validation.fingerprint_sha256,
    }


@dataclass(frozen=True)
class OrgRuntimeManifestPersistenceResult:
    stored: bool
    immutable: bool
    version_created: bool
    manifest_fingerprint: str
    change_id: str
    run_id: str
    manifest_version: str
    resolved_manifest_version: str
    migration_applied: bool
    migrated_from_manifest_version: str
    source_blueprint_fingerprint: str
    manifest_path: str
    history_path: str
    run_index_path: str
    persisted_at: str

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["fingerprint_sha256"] = self.manifest_fingerprint
        return payload


class OrgRuntimeManifestStateStore:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.base_dir = self.root_dir / "org-runtime-manifests"
        self.versions_dir = self.base_dir / "versions"
        self.history_dir = self.base_dir / "history"
        self.run_index_dir = self.base_dir / "run-index"

        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        self.run_index_dir.mkdir(parents=True, exist_ok=True)

    def manifest_version_path(
        self,
        *,
        change_id: str,
        run_id: str,
        manifest_fingerprint: str,
    ) -> Path:
        normalized_change_id = _storage_path_segment(change_id, field_name="change_id")
        normalized_run_id = _storage_path_segment(run_id, field_name="run_id")
        normalized_fingerprint = str(manifest_fingerprint).strip().lower()
        if not SHA256_REGEX.fullmatch(normalized_fingerprint):
            raise ValueError("manifest_fingerprint deve ser SHA-256 canonico.")
        return (
            self.versions_dir
            / normalized_change_id
            / normalized_run_id
            / f"{normalized_fingerprint}.json"
        )

    def manifest_history_path(self, *, change_id: str, run_id: str) -> Path:
        normalized_change_id = _storage_path_segment(change_id, field_name="change_id")
        normalized_run_id = _storage_path_segment(run_id, field_name="run_id")
        return self.history_dir / normalized_change_id / normalized_run_id / "history.jsonl"

    def run_index_path(self, *, run_id: str) -> Path:
        normalized_run_id = _storage_path_segment(run_id, field_name="run_id")
        return self.run_index_dir / f"{normalized_run_id}.json"

    def persist_manifest(
        self,
        manifest: Dict[str, Any],
        *,
        allow_schema_migration: bool = False,
        actor: str = "org-runtime-manifest-store",
        persisted_at: Optional[str] = None,
    ) -> OrgRuntimeManifestPersistenceResult:
        validation = validate_org_runtime_manifest_block(
            manifest,
            allow_schema_migration=allow_schema_migration,
        )
        return self.persist_validation_result(
            validation,
            actor=actor,
            persisted_at=persisted_at,
        )

    def persist_validation_result(
        self,
        validation: OrgRuntimeManifestValidationResult,
        *,
        actor: str = "org-runtime-manifest-store",
        persisted_at: Optional[str] = None,
    ) -> OrgRuntimeManifestPersistenceResult:
        if not validation.valid:
            raise ValueError("Manifesto invalido nao pode ser persistido.")

        manifest_fingerprint = str(validation.manifest_fingerprint).strip().lower()
        if not SHA256_REGEX.fullmatch(manifest_fingerprint):
            raise ValueError("manifest_fingerprint invalido no relatorio de validacao.")

        resolved_actor = str(actor).strip() or "org-runtime-manifest-store"
        resolved_timestamp = str(persisted_at or utc_now_iso()).strip()
        if not ISO8601_UTC_REGEX.fullmatch(resolved_timestamp):
            raise ValueError("persisted_at deve estar em UTC ISO-8601.")

        version_path = self.manifest_version_path(
            change_id=validation.change_id,
            run_id=validation.run_id,
            manifest_fingerprint=manifest_fingerprint,
        )
        history_path = self.manifest_history_path(
            change_id=validation.change_id,
            run_id=validation.run_id,
        )
        run_index_path = self.run_index_path(run_id=validation.run_id)

        normalized_manifest = _build_normalized_manifest_from_validation(validation)
        version_payload = {
            "contract_version": PERSISTENCE_CONTRACT_VERSION,
            "persisted_at": resolved_timestamp,
            "actor": resolved_actor,
            "change_id": validation.change_id,
            "run_id": validation.run_id,
            "source_blueprint_fingerprint": validation.source_blueprint_fingerprint,
            "manifest_fingerprint": manifest_fingerprint,
            "fingerprint_sha256": manifest_fingerprint,
            "manifest_version": validation.manifest_version,
            "resolved_manifest_version": validation.resolved_manifest_version,
            "manifest_runtime_version": validation.manifest_runtime_version,
            "migration_applied": validation.migration_applied,
            "migrated_from_manifest_version": validation.migrated_from_manifest_version,
            "schema_policy": get_org_runtime_manifest_schema_policy(),
            "manifest": normalized_manifest,
        }
        serialized_version_payload = json.dumps(
            version_payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

        version_created = False
        if version_path.exists():
            try:
                existing_payload = json.loads(version_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as error:
                raise RuntimeError(
                    "Violacao de imutabilidade: versao existente do manifesto esta corrompida."
                ) from error

            if not isinstance(existing_payload, dict):
                raise RuntimeError(
                    "Violacao de imutabilidade: versao existente do manifesto invalida."
                )
            existing_manifest = (
                existing_payload.get("manifest")
                if isinstance(existing_payload.get("manifest"), dict)
                else {}
            )
            immutable_snapshot_matches = (
                str(existing_payload.get("change_id", "")).strip() == validation.change_id
                and str(existing_payload.get("run_id", "")).strip() == validation.run_id
                and str(existing_payload.get("manifest_fingerprint", "")).strip().lower()
                == manifest_fingerprint
                and str(existing_payload.get("source_blueprint_fingerprint", "")).strip().lower()
                == validation.source_blueprint_fingerprint
                and existing_manifest == normalized_manifest
            )
            if not immutable_snapshot_matches:
                raise RuntimeError(
                    "Violacao de imutabilidade: versao existente do manifesto diverge do snapshot canônico."
                )
        else:
            _atomic_write_text(version_path, serialized_version_payload)
            version_created = True

        history_entries = _read_jsonl(history_path)
        known_fingerprints = {
            str(entry.get("manifest_fingerprint", "")).strip().lower()
            for entry in history_entries
        }
        if manifest_fingerprint not in known_fingerprints:
            _append_jsonl(
                history_path,
                {
                    "timestamp_utc": resolved_timestamp,
                    "change_id": validation.change_id,
                    "run_id": validation.run_id,
                    "source_blueprint_fingerprint": validation.source_blueprint_fingerprint,
                    "manifest_fingerprint": manifest_fingerprint,
                    "fingerprint_sha256": manifest_fingerprint,
                    "manifest_version": validation.manifest_version,
                    "resolved_manifest_version": validation.resolved_manifest_version,
                    "migration_applied": validation.migration_applied,
                    "migrated_from_manifest_version": validation.migrated_from_manifest_version,
                    "manifest_path": str(version_path),
                    "actor": resolved_actor,
                },
            )
            history_entries = _read_jsonl(history_path)

        run_index_payload: Dict[str, Any] = {
            "contract_version": PERSISTENCE_CONTRACT_VERSION,
            "run_id": validation.run_id,
            "change_id": validation.change_id,
            "source_blueprint_fingerprint": validation.source_blueprint_fingerprint,
            "latest_manifest_fingerprint": manifest_fingerprint,
            "manifest_fingerprints": sorted(
                {
                    str(entry.get("manifest_fingerprint", "")).strip().lower()
                    for entry in history_entries
                    if SHA256_REGEX.fullmatch(
                        str(entry.get("manifest_fingerprint", "")).strip().lower()
                    )
                }
            ),
            "history_path": str(history_path),
            "updated_at": resolved_timestamp,
        }
        if run_index_path.exists():
            try:
                existing_index = json.loads(run_index_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing_index = {}
            if isinstance(existing_index, dict):
                existing_change_id = str(existing_index.get("change_id", "")).strip()
                if existing_change_id and existing_change_id != validation.change_id:
                    raise RuntimeError(
                        "Inconsistencia de audit trail: run_id ja associado a outro change_id."
                    )
        _atomic_write_text(
            run_index_path,
            json.dumps(run_index_payload, ensure_ascii=False, indent=2, sort_keys=True),
        )

        return OrgRuntimeManifestPersistenceResult(
            stored=True,
            immutable=True,
            version_created=version_created,
            manifest_fingerprint=manifest_fingerprint,
            change_id=validation.change_id,
            run_id=validation.run_id,
            manifest_version=validation.manifest_version,
            resolved_manifest_version=validation.resolved_manifest_version,
            migration_applied=validation.migration_applied,
            migrated_from_manifest_version=validation.migrated_from_manifest_version,
            source_blueprint_fingerprint=validation.source_blueprint_fingerprint,
            manifest_path=str(version_path),
            history_path=str(history_path),
            run_index_path=str(run_index_path),
            persisted_at=resolved_timestamp,
        )

    def load_manifest_version(
        self,
        *,
        change_id: str,
        run_id: str,
        manifest_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        target = self.manifest_version_path(
            change_id=change_id,
            run_id=run_id,
            manifest_fingerprint=manifest_fingerprint,
        )
        if not target.exists():
            return None
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def load_manifest_history(self, *, change_id: str, run_id: str) -> List[Dict[str, Any]]:
        return _read_jsonl(self.manifest_history_path(change_id=change_id, run_id=run_id))

    def load_run_index(self, *, run_id: str) -> Optional[Dict[str, Any]]:
        target = self.run_index_path(run_id=run_id)
        if not target.exists():
            return None
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None
