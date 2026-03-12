from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml


ALLOWED_ORG_ROLES = {"peer", "orderer", "ca"}
ALLOWED_CHANNEL_TYPES = {"ops", "reggov", "business"}
ALLOWED_NODE_TYPES = {"peer", "orderer", "ca"}
ALLOWED_POLICY_SCOPES = {"network", "org", "channel", "operation"}
ALLOWED_DEPLOYMENT_STACKS = {"docker", "k8s", "hybrid"}
ALLOWED_CRYPTO_ALGORITHMS = {"ecdsa", "rsa"}
ALLOWED_SECRET_POLICIES = {"state_store_only", "state_store_encrypted", "vault_only"}
AUTHORIZED_PRIVATE_KEY_STORE_PREFIX = "/var/cognus/secure-store/"
CURRENT_SCHEMA_VERSION = "1.0.0"

ORG_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$")
MSP_ID_REGEX = re.compile(r"^[A-Za-z][A-Za-z0-9]{1,62}MSP$")
CHANNEL_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
NODE_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
POLICY_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
PROFILE_ID_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
DNS_LABEL_REGEX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
SEMVER_REGEX = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:[-+][0-9A-Za-z.-]+)?$")
ISO8601_UTC_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3,6})?Z$"
)
RESERVED_IDENTIFIER_VALUES = {"admin", "default", "localhost", "none", "null", "root"}

MIN_NODE_RESOURCES_BY_STAGE: Dict[str, Dict[str, Dict[str, int]]] = {
    "dev": {
        "peer": {"cpu": 1, "memory_mb": 1024, "disk_gb": 10},
        "orderer": {"cpu": 1, "memory_mb": 1024, "disk_gb": 20},
        "ca": {"cpu": 1, "memory_mb": 512, "disk_gb": 5},
    },
    "hml": {
        "peer": {"cpu": 2, "memory_mb": 2048, "disk_gb": 50},
        "orderer": {"cpu": 2, "memory_mb": 2048, "disk_gb": 50},
        "ca": {"cpu": 1, "memory_mb": 1024, "disk_gb": 10},
    },
    "prod": {
        "peer": {"cpu": 2, "memory_mb": 4096, "disk_gb": 100},
        "orderer": {"cpu": 2, "memory_mb": 4096, "disk_gb": 100},
        "ca": {"cpu": 1, "memory_mb": 2048, "disk_gb": 20},
    },
}

NON_PERSISTENT_STORAGE_TOKENS = {"tmp", "temp", "ephemeral", "memory", "ramdisk"}


@dataclass(frozen=True)
class LintIssue:
    level: str
    code: str
    path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class OrgsValidationResult:
    valid: bool
    normalized_orgs: List[Dict[str, Any]]
    issues: List[LintIssue]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "normalized_orgs": self.normalized_orgs,
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class BlueprintValidationResult:
    valid: bool
    current_schema_version: str
    resolved_schema_version: str
    migration_applied: bool
    migrated_from_schema_version: str
    schema_name: str
    schema_version: str
    blueprint_version: str
    created_at: str
    updated_at: str
    normalized_orgs: List[Dict[str, Any]]
    normalized_channels: List[Dict[str, Any]]
    normalized_nodes: List[Dict[str, Any]]
    normalized_policies: List[Dict[str, Any]]
    normalized_environment_profile: Dict[str, Any]
    normalized_identity_baseline: Dict[str, Any]
    fingerprint_sha256: str
    issues: List[LintIssue]

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
            "current_schema_version": self.current_schema_version,
            "schema_runtime": self.current_schema_version,
            "resolved_schema_version": self.resolved_schema_version,
            "migration_applied": self.migration_applied,
            "migrated_from_schema_version": self.migrated_from_schema_version,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "blueprint_version": self.blueprint_version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "normalized_orgs": self.normalized_orgs,
            "normalized_channels": self.normalized_channels,
            "normalized_nodes": self.normalized_nodes,
            "normalized_policies": self.normalized_policies,
            "normalized_environment_profile": self.normalized_environment_profile,
            "normalized_identity_baseline": self.normalized_identity_baseline,
            "fingerprint_sha256": self.fingerprint_sha256,
            "issues": issue_dicts,
        }


_ISSUE_LEVEL_ORDER = {"error": 0, "warning": 1, "hint": 2}


def _sort_issues_deterministically(issues: List[LintIssue]) -> List[LintIssue]:
    return sorted(
        issues,
        key=lambda issue: (
            _ISSUE_LEVEL_ORDER.get(issue.level, 99),
            issue.path,
            issue.code,
            issue.message,
        ),
    )


def _is_valid_domain(value: str) -> bool:
    if not value or len(value) > 253:
        return False
    if value.endswith("."):
        value = value[:-1]
    labels = value.split(".")
    if len(labels) < 2:
        return False
    return all(DNS_LABEL_REGEX.fullmatch(label) for label in labels)


def _identifier_normalization_key(value: str) -> str:
    return re.sub(r"[-_.]", "", value.strip().lower())


def _is_msp_consistent_with_org(org_id: str, msp_id: str) -> bool:
    if not org_id or not msp_id:
        return True
    org_key = re.sub(r"[^a-z0-9]", "", org_id.lower())
    msp_lower = msp_id.lower()
    if not msp_lower.endswith("msp"):
        return False
    msp_key = re.sub(r"[^a-z0-9]", "", msp_lower[:-3])
    return org_key == msp_key


def _ensure_str(field_name: str, value: Any, issues: List[LintIssue], path: str) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    issues.append(
        LintIssue(
            level="error",
            code="required_string",
            path=path,
            message=f"Campo obrigatório '{field_name}' ausente ou inválido.",
        )
    )
    return ""


def _validate_schema_metadata(blueprint: Dict[str, Any], issues: List[LintIssue]) -> Dict[str, str]:
    def _metadata_text(field_name: str, value: Any, path: str) -> str:
        if isinstance(value, datetime):
            iso = value.isoformat()
            if iso.endswith("+00:00"):
                iso = iso[:-6] + "Z"
            return iso
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, (int, float)):
            return str(value)
        return _ensure_str(field_name, value, issues, path)

    schema_name = _metadata_text("schema_name", blueprint.get("schema_name"), "schema_name")
    schema_version = _metadata_text("schema_version", blueprint.get("schema_version"), "schema_version")
    blueprint_version = _metadata_text(
        "blueprint_version", blueprint.get("blueprint_version"), "blueprint_version"
    )
    created_at = _metadata_text("created_at", blueprint.get("created_at"), "created_at")
    updated_at = _metadata_text("updated_at", blueprint.get("updated_at"), "updated_at")

    if schema_version and not SEMVER_REGEX.fullmatch(schema_version):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_schema_version",
                path="schema_version",
                message="schema_version deve seguir SemVer (MAJOR.MINOR.PATCH).",
            )
        )
    if blueprint_version and not SEMVER_REGEX.fullmatch(blueprint_version):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_blueprint_version",
                path="blueprint_version",
                message="blueprint_version deve seguir SemVer (MAJOR.MINOR.PATCH).",
            )
        )
    if created_at and not ISO8601_UTC_REGEX.fullmatch(created_at):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_created_at",
                path="created_at",
                message="created_at deve ser timestamp UTC ISO-8601 (ex.: 2026-02-15T10:00:00Z).",
            )
        )
    if updated_at and not ISO8601_UTC_REGEX.fullmatch(updated_at):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_updated_at",
                path="updated_at",
                message="updated_at deve ser timestamp UTC ISO-8601 (ex.: 2026-02-15T10:00:00Z).",
            )
        )

    return {
        "schema_name": schema_name,
        "schema_version": schema_version,
        "blueprint_version": blueprint_version,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _coerce_schema_version(value: Any) -> str:
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


def _migrate_schema_0_9_0_to_1_0_0(blueprint: Dict[str, Any]) -> Dict[str, Any]:
    migrated = deepcopy(blueprint)
    if "environment_profile" not in migrated and "profile" in migrated:
        migrated["environment_profile"] = migrated.pop("profile")
    migrated["schema_version"] = "1.0.0"
    return migrated


SCHEMA_MIGRATIONS = {
    ("0.9.0", "1.0.0"): _migrate_schema_0_9_0_to_1_0_0,
}


def _apply_schema_migration_chain(
    blueprint: Dict[str, Any],
    source_version: str,
    target_version: str,
) -> Optional[Dict[str, Any]]:
    if source_version == target_version:
        return deepcopy(blueprint)

    current = source_version
    migrated = deepcopy(blueprint)
    safety_counter = 0
    while current != target_version and safety_counter < 8:
        safety_counter += 1
        next_step = None
        migrator = None
        for (src, dst), candidate in SCHEMA_MIGRATIONS.items():
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
    migrated["schema_version"] = target_version
    return migrated


def _resolve_schema_compatibility(
    blueprint: Dict[str, Any],
    allow_schema_migration: bool,
    issues: List[LintIssue],
) -> Tuple[Dict[str, Any], str, bool, str]:
    raw_version = _coerce_schema_version(blueprint.get("schema_version"))
    if not raw_version:
        return deepcopy(blueprint), "", False, ""

    parsed = _parse_semver(raw_version)
    current_parsed = _parse_semver(CURRENT_SCHEMA_VERSION)
    if not parsed or not current_parsed:
        return deepcopy(blueprint), raw_version, False, ""

    if parsed == current_parsed:
        return deepcopy(blueprint), raw_version, False, ""

    if parsed[0] == current_parsed[0]:
        if parsed > current_parsed:
            issues.append(
                LintIssue(
                    level="error",
                    code="schema_version_newer_than_runtime",
                    path="schema_version",
                    message=(
                        f"schema_version {raw_version} é mais nova que a suportada "
                        f"({CURRENT_SCHEMA_VERSION}). Atualize o validador."
                    ),
                )
            )
            return deepcopy(blueprint), raw_version, False, ""

        issues.append(
            LintIssue(
                level="hint",
                code="schema_version_older_compatible",
                path="schema_version",
                message=(
                    f"schema_version {raw_version} é compatível com runtime {CURRENT_SCHEMA_VERSION}."
                ),
            )
        )
        return deepcopy(blueprint), raw_version, False, ""

    if parsed[0] > current_parsed[0]:
        issues.append(
            LintIssue(
                level="error",
                code="schema_version_major_ahead",
                path="schema_version",
                message=(
                    f"Major {parsed[0]} não suportado por este runtime ({CURRENT_SCHEMA_VERSION})."
                ),
            )
        )
        return deepcopy(blueprint), raw_version, False, ""

    if not allow_schema_migration:
        issues.append(
            LintIssue(
                level="error",
                code="schema_migration_required",
                path="schema_version",
                message=(
                    f"schema_version {raw_version} requer migração para {CURRENT_SCHEMA_VERSION}. "
                    "Execute o lint com --migrate."
                ),
            )
        )
        return deepcopy(blueprint), raw_version, False, ""

    migrated = _apply_schema_migration_chain(blueprint, raw_version, CURRENT_SCHEMA_VERSION)
    if migrated is None:
        issues.append(
            LintIssue(
                level="error",
                code="schema_migration_path_not_found",
                path="schema_version",
                message=(
                    f"Sem rotina de migração de {raw_version} para {CURRENT_SCHEMA_VERSION}."
                ),
            )
        )
        return deepcopy(blueprint), raw_version, False, ""

    issues.append(
        LintIssue(
            level="hint",
            code="schema_migration_applied",
            path="schema_version",
            message=f"Migração aplicada: {raw_version} -> {CURRENT_SCHEMA_VERSION}.",
        )
    )
    return migrated, CURRENT_SCHEMA_VERSION, True, raw_version


def _normalize_roles(raw_roles: Any, path: str, issues: List[LintIssue]) -> List[str]:
    if not isinstance(raw_roles, list) or not raw_roles:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_roles",
                path=path,
                message="Campo 'roles' deve ser uma lista não vazia.",
            )
        )
        return []

    normalized: List[str] = []
    seen = set()
    for idx, role in enumerate(raw_roles):
        role_path = f"{path}[{idx}]"
        if not isinstance(role, str):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_role_type",
                    path=role_path,
                    message="Role deve ser string.",
                )
            )
            continue
        role_name = role.strip().lower()
        if role_name not in ALLOWED_ORG_ROLES:
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_role_value",
                    path=role_path,
                    message=(
                        f"Role '{role}' inválida. Valores permitidos: "
                        f"{', '.join(sorted(ALLOWED_ORG_ROLES))}."
                    ),
                )
            )
            continue
        if role_name not in seen:
            normalized.append(role_name)
            seen.add(role_name)
    return normalized


def _normalize_identity(raw_identity: Any, org_path: str, issues: List[LintIssue]) -> Dict[str, Any]:
    path = f"{org_path}.identity"
    if not isinstance(raw_identity, dict):
        issues.append(
            LintIssue(
                level="error",
                code="missing_identity",
                path=path,
                message=(
                    "Campo 'identity' é obrigatório e deve conter "
                    "ca_profile, tls_ca_profile, msp_path, association_policies, "
                    "node_identity_policy e admin_identity_policy."
                ),
            )
        )
        return {}

    ca_profile = _ensure_str("ca_profile", raw_identity.get("ca_profile"), issues, f"{path}.ca_profile")
    tls_ca_profile = _ensure_str(
        "tls_ca_profile", raw_identity.get("tls_ca_profile"), issues, f"{path}.tls_ca_profile"
    )
    msp_path = _ensure_str("msp_path", raw_identity.get("msp_path"), issues, f"{path}.msp_path")

    assoc_raw = raw_identity.get("association_policies")
    association_policies: List[str] = []
    if not isinstance(assoc_raw, list) or not assoc_raw:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_association_policies",
                path=f"{path}.association_policies",
                message="Campo 'association_policies' deve ser lista não vazia.",
            )
        )
    else:
        for idx, item in enumerate(assoc_raw):
            policy_path = f"{path}.association_policies[{idx}]"
            if not isinstance(item, str) or not item.strip():
                issues.append(
                    LintIssue(
                        level="error",
                        code="invalid_association_policy_item",
                        path=policy_path,
                        message="Cada item de 'association_policies' deve ser string não vazia.",
                    )
                )
                continue
            association_policies.append(item.strip())

    def _normalize_identity_policy(policy_name: str, raw_policy: Any, policy_path: str) -> Dict[str, Any]:
        if not isinstance(raw_policy, dict):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_identity_policy_object",
                    path=policy_path,
                    message=f"{policy_name} deve ser objeto com allowed_roles não vazio.",
                )
            )
            return {"allowed_roles": []}

        allowed_roles_raw = raw_policy.get("allowed_roles")
        normalized_allowed_roles: List[str] = []
        seen_allowed_roles = set()
        if not isinstance(allowed_roles_raw, list) or not allowed_roles_raw:
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_identity_policy_allowed_roles",
                    path=f"{policy_path}.allowed_roles",
                    message=f"{policy_name}.allowed_roles deve ser lista não vazia.",
                )
            )
        else:
            for idx, role in enumerate(allowed_roles_raw):
                role_path = f"{policy_path}.allowed_roles[{idx}]"
                if not isinstance(role, str) or not role.strip():
                    issues.append(
                        LintIssue(
                            level="error",
                            code="invalid_identity_policy_role_type",
                            path=role_path,
                            message="Role da política de identidade deve ser string não vazia.",
                        )
                    )
                    continue
                role_name = role.strip().lower()
                if role_name not in ALLOWED_ORG_ROLES and role_name != "admin":
                    issues.append(
                        LintIssue(
                            level="error",
                            code="invalid_identity_policy_role_value",
                            path=role_path,
                            message="Role inválida na política de identidade. Use peer, orderer, ca ou admin.",
                        )
                    )
                    continue
                if role_name not in seen_allowed_roles:
                    normalized_allowed_roles.append(role_name)
                    seen_allowed_roles.add(role_name)

        return {
            "allowed_roles": sorted(normalized_allowed_roles),
        }

    node_identity_policy = _normalize_identity_policy(
        "node_identity_policy",
        raw_identity.get("node_identity_policy"),
        f"{path}.node_identity_policy",
    )
    admin_identity_policy = _normalize_identity_policy(
        "admin_identity_policy",
        raw_identity.get("admin_identity_policy"),
        f"{path}.admin_identity_policy",
    )

    if "admin" not in set(admin_identity_policy.get("allowed_roles", [])):
        issues.append(
            LintIssue(
                level="error",
                code="admin_identity_policy_missing_admin_role",
                path=f"{path}.admin_identity_policy.allowed_roles",
                message="admin_identity_policy deve incluir role 'admin'.",
            )
        )

    if ca_profile and tls_ca_profile and ca_profile.strip().lower() == tls_ca_profile.strip().lower():
        issues.append(
            LintIssue(
                level="error",
                code="ambiguous_org_ca_tls_profile",
                path=path,
                message=(
                    "ca_profile e tls_ca_profile não podem ser iguais para evitar "
                    "ambiguidade de trust domain."
                ),
            )
        )

    if msp_path and not msp_path.startswith("/"):
        issues.append(
            LintIssue(
                level="warning",
                code="msp_path_not_absolute",
                path=f"{path}.msp_path",
                message="msp_path deveria ser absoluto para evitar ambiguidades de runtime.",
            )
        )

    return {
        "ca_profile": ca_profile,
        "tls_ca_profile": tls_ca_profile,
        "msp_path": msp_path,
        "association_policies": association_policies,
        "node_identity_policy": node_identity_policy,
        "admin_identity_policy": admin_identity_policy,
    }


def _validate_identity_policy_role_compatibility(
    normalized_orgs: List[Dict[str, Any]],
    issues: List[LintIssue],
) -> None:
    for idx, org in enumerate(normalized_orgs):
        roles = set(org.get("roles") or [])
        identity = org.get("identity") if isinstance(org.get("identity"), dict) else {}
        node_policy = (
            identity.get("node_identity_policy")
            if isinstance(identity.get("node_identity_policy"), dict)
            else {}
        )
        node_allowed_roles = set(node_policy.get("allowed_roles") or [])

        if "peer" in roles and "peer" not in node_allowed_roles:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_identity_policy_incompatible_peer_role",
                    path=f"orgs[{idx}].identity.node_identity_policy.allowed_roles",
                    message="Org com role 'peer' deve possuir node_identity_policy compatível com 'peer'.",
                )
            )

        if "orderer" in roles and "orderer" not in node_allowed_roles:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_identity_policy_incompatible_orderer_role",
                    path=f"orgs[{idx}].identity.node_identity_policy.allowed_roles",
                    message=(
                        "Org com role 'orderer' deve possuir node_identity_policy compatível com 'orderer'."
                    ),
                )
            )


def _validate_identity_trust_domain_ambiguity(
    normalized_orgs: List[Dict[str, Any]],
    issues: List[LintIssue],
) -> None:
    ca_profiles: Dict[str, int] = {}
    tls_profiles: Dict[str, int] = {}

    for idx, org in enumerate(normalized_orgs):
        identity = org.get("identity") if isinstance(org.get("identity"), dict) else {}
        ca_profile = str(identity.get("ca_profile", "")).strip().lower()
        tls_ca_profile = str(identity.get("tls_ca_profile", "")).strip().lower()
        if ca_profile:
            ca_profiles.setdefault(ca_profile, idx)
        if tls_ca_profile:
            tls_profiles.setdefault(tls_ca_profile, idx)

    overlapping = sorted(set(ca_profiles).intersection(set(tls_profiles)))
    for profile in overlapping:
        ca_idx = ca_profiles[profile]
        tls_idx = tls_profiles[profile]
        issues.append(
            LintIssue(
                level="error",
                code="trust_domain_ca_tls_ambiguity",
                path="identity_baseline.trust_domains",
                message=(
                    f"Perfil '{profile}' aparece em domínios de assinatura e TLS "
                    f"(orgs[{ca_idx}] e orgs[{tls_idx}])."
                ),
            )
        )


def _normalize_ca_tls_crypto_parameters(
    raw_params: Any,
    path: str,
    profile_kind: str,
    issues: List[LintIssue],
) -> Dict[str, Any]:
    if not isinstance(raw_params, dict):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_params",
                path=path,
                message=f"{profile_kind} deve ser objeto com algorithm, key_size, validity_days e rotation_days.",
            )
        )
        return {
            "algorithm": "",
            "key_size": 0,
            "validity_days": 0,
            "rotation_days": 0,
        }

    algorithm = str(raw_params.get("algorithm", "")).strip().lower()
    key_size = raw_params.get("key_size")
    validity_days = raw_params.get("validity_days")
    rotation_days = raw_params.get("rotation_days")

    if algorithm not in ALLOWED_CRYPTO_ALGORITHMS:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_algorithm",
                path=f"{path}.algorithm",
                message="algorithm deve ser um de: ecdsa, rsa.",
            )
        )

    if not isinstance(key_size, int) or key_size <= 0:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_key_size",
                path=f"{path}.key_size",
                message="key_size deve ser inteiro positivo.",
            )
        )
    elif algorithm == "ecdsa" and key_size not in {256, 384, 521}:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_key_size_for_ecdsa",
                path=f"{path}.key_size",
                message="Para ecdsa, key_size deve ser 256, 384 ou 521.",
            )
        )
    elif algorithm == "rsa" and key_size < 2048:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_key_size_for_rsa",
                path=f"{path}.key_size",
                message="Para rsa, key_size deve ser >= 2048.",
            )
        )

    if not isinstance(validity_days, int) or validity_days <= 0:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_validity",
                path=f"{path}.validity_days",
                message="validity_days deve ser inteiro positivo.",
            )
        )

    if not isinstance(rotation_days, int) or rotation_days <= 0:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_rotation",
                path=f"{path}.rotation_days",
                message="rotation_days deve ser inteiro positivo.",
            )
        )
    elif isinstance(validity_days, int) and validity_days > 0 and rotation_days >= validity_days:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_rotation_window",
                path=f"{path}.rotation_days",
                message="rotation_days deve ser menor que validity_days.",
            )
        )

    return {
        "algorithm": algorithm,
        "key_size": key_size if isinstance(key_size, int) else 0,
        "validity_days": validity_days if isinstance(validity_days, int) else 0,
        "rotation_days": rotation_days if isinstance(rotation_days, int) else 0,
    }


def _normalize_org_crypto_profile(
    raw_profile: Any,
    profile_path: str,
    org_contract: Dict[str, Any],
    issues: List[LintIssue],
) -> Dict[str, Any]:
    if not isinstance(raw_profile, dict):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_object",
                path=profile_path,
                message="Cada item de org_crypto_profiles deve ser objeto.",
            )
        )
        return {}

    org_id = str(raw_profile.get("org_id", "")).strip().lower()
    ca_profile = _ensure_str("ca_profile", raw_profile.get("ca_profile"), issues, f"{profile_path}.ca_profile")
    tls_ca_profile = _ensure_str(
        "tls_ca_profile",
        raw_profile.get("tls_ca_profile"),
        issues,
        f"{profile_path}.tls_ca_profile",
    )

    ca_params = _normalize_ca_tls_crypto_parameters(
        raw_profile.get("ca"),
        f"{profile_path}.ca",
        "ca",
        issues,
    )
    tls_ca_params = _normalize_ca_tls_crypto_parameters(
        raw_profile.get("tls_ca"),
        f"{profile_path}.tls_ca",
        "tls_ca",
        issues,
    )

    storage_raw = raw_profile.get("storage")
    if not isinstance(storage_raw, dict):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_storage",
                path=f"{profile_path}.storage",
                message=(
                    "storage deve definir ca_material_path, tls_ca_material_path, permissions, "
                    "secret_policy, private_key_store, ca_credential_ref e tls_ca_credential_ref."
                ),
            )
        )
        storage_raw = {}

    ca_material_path = _ensure_str(
        "ca_material_path",
        storage_raw.get("ca_material_path"),
        issues,
        f"{profile_path}.storage.ca_material_path",
    )
    tls_ca_material_path = _ensure_str(
        "tls_ca_material_path",
        storage_raw.get("tls_ca_material_path"),
        issues,
        f"{profile_path}.storage.tls_ca_material_path",
    )
    permissions = _ensure_str(
        "permissions",
        storage_raw.get("permissions"),
        issues,
        f"{profile_path}.storage.permissions",
    )
    secret_policy = _ensure_str(
        "secret_policy",
        storage_raw.get("secret_policy"),
        issues,
        f"{profile_path}.storage.secret_policy",
    ).lower()
    private_key_store = _ensure_str(
        "private_key_store",
        storage_raw.get("private_key_store"),
        issues,
        f"{profile_path}.storage.private_key_store",
    )
    ca_credential_ref = _ensure_str(
        "ca_credential_ref",
        storage_raw.get("ca_credential_ref"),
        issues,
        f"{profile_path}.storage.ca_credential_ref",
    )
    tls_ca_credential_ref = _ensure_str(
        "tls_ca_credential_ref",
        storage_raw.get("tls_ca_credential_ref"),
        issues,
        f"{profile_path}.storage.tls_ca_credential_ref",
    )

    if ca_material_path and tls_ca_material_path and ca_material_path == tls_ca_material_path:
        issues.append(
            LintIssue(
                level="error",
                code="org_crypto_profile_storage_not_separated",
                path=f"{profile_path}.storage",
                message="CA e TLS-CA devem usar paths de storage distintos.",
            )
        )

    if ca_credential_ref and tls_ca_credential_ref and ca_credential_ref == tls_ca_credential_ref:
        issues.append(
            LintIssue(
                level="error",
                code="org_crypto_profile_credentials_not_separated",
                path=f"{profile_path}.storage",
                message="CA e TLS-CA devem usar credenciais distintas.",
            )
        )

    if permissions and permissions not in {"0700", "0750"}:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_permissions",
                path=f"{profile_path}.storage.permissions",
                message="permissions deve ser 0700 ou 0750.",
            )
        )

    if secret_policy and secret_policy not in ALLOWED_SECRET_POLICIES:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_crypto_profile_secret_policy",
                path=f"{profile_path}.storage.secret_policy",
                message="secret_policy inválida para baseline de segurança.",
            )
        )

    if private_key_store and not private_key_store.startswith(AUTHORIZED_PRIVATE_KEY_STORE_PREFIX):
        issues.append(
            LintIssue(
                level="error",
                code="org_crypto_profile_private_key_store_not_authorized",
                path=f"{profile_path}.storage.private_key_store",
                message=(
                    "private_key_store deve usar storage autorizado do pipeline "
                    f"({AUTHORIZED_PRIVATE_KEY_STORE_PREFIX}...)."
                ),
            )
        )

    expected_ca_profile = str(org_contract.get("ca_profile", ""))
    expected_tls_profile = str(org_contract.get("tls_ca_profile", ""))
    if ca_profile and expected_ca_profile and ca_profile != expected_ca_profile:
        issues.append(
            LintIssue(
                level="error",
                code="org_crypto_profile_ca_profile_mismatch",
                path=f"{profile_path}.ca_profile",
                message=(
                    f"ca_profile ({ca_profile}) difere do contrato da org ({expected_ca_profile})."
                ),
            )
        )
    if tls_ca_profile and expected_tls_profile and tls_ca_profile != expected_tls_profile:
        issues.append(
            LintIssue(
                level="error",
                code="org_crypto_profile_tls_profile_mismatch",
                path=f"{profile_path}.tls_ca_profile",
                message=(
                    f"tls_ca_profile ({tls_ca_profile}) difere do contrato da org ({expected_tls_profile})."
                ),
            )
        )

    return {
        "org_id": org_id,
        "ca_profile": ca_profile,
        "tls_ca_profile": tls_ca_profile,
        "ca": ca_params,
        "tls_ca": tls_ca_params,
        "storage": {
            "ca_material_path": ca_material_path,
            "tls_ca_material_path": tls_ca_material_path,
            "permissions": permissions,
            "secret_policy": secret_policy,
            "private_key_store": private_key_store,
            "ca_credential_ref": ca_credential_ref,
            "tls_ca_credential_ref": tls_ca_credential_ref,
        },
    }


def validate_identity_baseline_block(
    blueprint: Dict[str, Any],
    normalized_orgs: List[Dict[str, Any]],
    schema_version: str,
    issues: List[LintIssue],
) -> Tuple[Dict[str, Any], List[LintIssue]]:
    local_issues = list(issues)

    raw_baseline = blueprint.get("identity_baseline")
    if not isinstance(raw_baseline, dict):
        local_issues.append(
            LintIssue(
                level="error",
                code="missing_identity_baseline_block",
                path="identity_baseline",
                message="Bloco 'identity_baseline' é obrigatório e deve ser um objeto.",
            )
        )
        return {}, local_issues

    baseline_name = _ensure_str(
        "baseline_name",
        raw_baseline.get("baseline_name"),
        local_issues,
        "identity_baseline.baseline_name",
    )
    baseline_version = _ensure_str(
        "baseline_version",
        raw_baseline.get("baseline_version"),
        local_issues,
        "identity_baseline.baseline_version",
    )
    schema_ref = _ensure_str(
        "schema_ref",
        raw_baseline.get("schema_ref"),
        local_issues,
        "identity_baseline.schema_ref",
    )

    if baseline_version and not SEMVER_REGEX.fullmatch(baseline_version):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_identity_baseline_version",
                path="identity_baseline.baseline_version",
                message="identity_baseline.baseline_version deve seguir SemVer.",
            )
        )

    if schema_ref and not SEMVER_REGEX.fullmatch(schema_ref):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_identity_baseline_schema_ref",
                path="identity_baseline.schema_ref",
                message="identity_baseline.schema_ref deve seguir SemVer.",
            )
        )
    elif schema_ref and schema_version and schema_ref != schema_version:
        local_issues.append(
            LintIssue(
                level="warning",
                code="identity_baseline_schema_ref_mismatch",
                path="identity_baseline.schema_ref",
                message=(
                    f"schema_ref ({schema_ref}) difere de schema_version ({schema_version})."
                ),
            )
        )

    _validate_identity_policy_role_compatibility(normalized_orgs, local_issues)
    _validate_identity_trust_domain_ambiguity(normalized_orgs, local_issues)

    org_contracts = []
    orderer_trust_domain = []
    tls_profiles = set()
    for org in sorted(normalized_orgs, key=lambda current: str(current.get("org_id", ""))):
        org_id = str(org.get("org_id", ""))
        msp_id = str(org.get("msp_id", ""))
        roles = sorted(list(set(org.get("roles") or [])))
        identity = org.get("identity") if isinstance(org.get("identity"), dict) else {}
        ca_profile = str(identity.get("ca_profile", ""))
        tls_ca_profile = str(identity.get("tls_ca_profile", ""))
        tls_profiles.add(tls_ca_profile)
        contract = {
            "org_id": org_id,
            "msp_id": msp_id,
            "roles": roles,
            "ca_profile": ca_profile,
            "tls_ca_profile": tls_ca_profile,
            "node_identity_policy": identity.get("node_identity_policy", {"allowed_roles": []}),
            "admin_identity_policy": identity.get("admin_identity_policy", {"allowed_roles": []}),
        }
        org_contracts.append(contract)
        if "orderer" in roles:
            orderer_trust_domain.append(
                {
                    "org_id": org_id,
                    "msp_id": msp_id,
                    "ca_profile": ca_profile,
                    "tls_ca_profile": tls_ca_profile,
                }
            )

    trust_domains = {
        "org_msp": [
            {
                "org_id": item["org_id"],
                "msp_id": item["msp_id"],
                "ca_profile": item["ca_profile"],
                "tls_ca_profile": item["tls_ca_profile"],
            }
            for item in org_contracts
        ],
        "orderer_msp": sorted(orderer_trust_domain, key=lambda current: current["org_id"]),
        "tls_trust_bundles": [
            {
                "bundle_id": "global-tls-trust",
                "tls_ca_profiles": sorted(profile for profile in tls_profiles if profile),
            }
        ],
    }

    org_contract_index = {
        str(item.get("org_id", "")).lower(): item
        for item in org_contracts
        if str(item.get("org_id", "")).strip()
    }

    raw_crypto_profiles = raw_baseline.get("org_crypto_profiles")
    if not isinstance(raw_crypto_profiles, list) or not raw_crypto_profiles:
        local_issues.append(
            LintIssue(
                level="error",
                code="missing_org_crypto_profiles",
                path="identity_baseline.org_crypto_profiles",
                message="identity_baseline.org_crypto_profiles deve ser lista não vazia (1 por org).",
            )
        )
        raw_crypto_profiles = []

    normalized_crypto_profiles: List[Dict[str, Any]] = []
    seen_profile_orgs: Dict[str, int] = {}
    for idx, raw_profile in enumerate(raw_crypto_profiles):
        profile_path = f"identity_baseline.org_crypto_profiles[{idx}]"
        if not isinstance(raw_profile, dict):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_org_crypto_profile_object",
                    path=profile_path,
                    message="Cada item de org_crypto_profiles deve ser objeto.",
                )
            )
            continue

        org_id = _ensure_str("org_id", raw_profile.get("org_id"), local_issues, f"{profile_path}.org_id").lower()
        if not org_id:
            continue

        if org_id in seen_profile_orgs:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="duplicate_org_crypto_profile",
                    path=f"{profile_path}.org_id",
                    message=f"org_crypto_profile duplicado para org_id '{org_id}'.",
                )
            )
            continue
        seen_profile_orgs[org_id] = idx

        org_contract = org_contract_index.get(org_id)
        if org_contract is None:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="org_crypto_profile_org_not_found",
                    path=f"{profile_path}.org_id",
                    message=f"org_id '{org_id}' não existe em orgs[].",
                )
            )
            continue

        normalized_profile = _normalize_org_crypto_profile(
            raw_profile,
            profile_path,
            org_contract,
            local_issues,
        )
        if normalized_profile:
            normalized_crypto_profiles.append(normalized_profile)

    for org_id in sorted(org_contract_index.keys()):
        if org_id not in seen_profile_orgs:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="missing_org_crypto_profile",
                    path="identity_baseline.org_crypto_profiles",
                    message=f"Perfil criptográfico obrigatório ausente para org '{org_id}'.",
                )
            )

    normalized_crypto_profiles = sorted(normalized_crypto_profiles, key=lambda item: item.get("org_id", ""))

    certificate_baseline = []
    for org in sorted(org_contracts, key=lambda item: str(item.get("org_id", ""))):
        domain = ""
        for org_obj in normalized_orgs:
            if str(org_obj.get("org_id", "")) == str(org.get("org_id", "")):
                domain = str(org_obj.get("domain", ""))
                break
        if not domain:
            continue
        certificate_baseline.append(
            {
                "org_id": org.get("org_id", ""),
                "domain": domain,
                "root_ca_subject": f"CN=RootCA.{domain}",
                "intermediate_ca_subject": f"CN=IntermediateCA.{domain}",
                "tls_root_ca_subject": f"CN=TLSRootCA.{domain}",
                "tls_intermediate_ca_subject": f"CN=TLSIntermediateCA.{domain}",
            }
        )

    normalized_baseline = {
        "baseline_name": baseline_name,
        "baseline_version": baseline_version,
        "schema_ref": schema_ref,
        "org_identities": org_contracts,
        "trust_domains": trust_domains,
        "org_crypto_profiles": normalized_crypto_profiles,
        "certificate_baseline": certificate_baseline,
    }

    return normalized_baseline, local_issues


def _validate_uniqueness(
    orgs: Iterable[Dict[str, Any]],
    issues: List[LintIssue],
) -> None:
    seen_org_ids: Dict[str, int] = {}
    seen_org_id_normalized: Dict[str, int] = {}
    seen_msp_ids: Dict[str, int] = {}
    seen_domains: Dict[str, int] = {}

    for idx, org in enumerate(orgs):
        org_id = org.get("org_id", "")
        msp_id = org.get("msp_id", "")
        domain = org.get("domain", "")

        if org_id:
            org_key = org_id.lower()
            if org_key in seen_org_ids:
                issues.append(
                    LintIssue(
                        level="error",
                        code="duplicate_org_id",
                        path=f"orgs[{idx}].org_id",
                        message=f"org_id duplicado com orgs[{seen_org_ids[org_key]}].",
                    )
                )
            else:
                seen_org_ids[org_key] = idx

            org_normalized_key = _identifier_normalization_key(org_id)
            if org_normalized_key in seen_org_id_normalized and seen_org_id_normalized[org_normalized_key] != idx:
                issues.append(
                    LintIssue(
                        level="error",
                        code="ambiguous_org_id_normalization",
                        path=f"orgs[{idx}].org_id",
                        message=(
                            f"org_id '{org_id}' colide por normalização com "
                            f"orgs[{seen_org_id_normalized[org_normalized_key]}].org_id."
                        ),
                    )
                )
            else:
                seen_org_id_normalized[org_normalized_key] = idx

        if msp_id:
            msp_key = msp_id.lower()
            if msp_key in seen_msp_ids:
                issues.append(
                    LintIssue(
                        level="error",
                        code="duplicate_msp_id",
                        path=f"orgs[{idx}].msp_id",
                        message=f"msp_id duplicado com orgs[{seen_msp_ids[msp_key]}].",
                    )
                )
            else:
                seen_msp_ids[msp_key] = idx

        if domain:
            domain_key = domain.lower()
            if domain_key in seen_domains:
                issues.append(
                    LintIssue(
                        level="error",
                        code="duplicate_domain",
                        path=f"orgs[{idx}].domain",
                        message=f"domain duplicado com orgs[{seen_domains[domain_key]}].",
                    )
                )
            else:
                seen_domains[domain_key] = idx


def _validate_org_naming(org: Dict[str, Any], idx: int, issues: List[LintIssue]) -> None:
    org_id = org.get("org_id", "")
    msp_id = org.get("msp_id", "")
    domain = org.get("domain", "")

    if org_id and not ORG_ID_REGEX.fullmatch(org_id):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_org_id_format",
                path=f"orgs[{idx}].org_id",
                message=(
                    f"org_id '{org_id}' inválido. Esperado: lowercase alfanumérico com hífen "
                    "(ex.: org-a)."
                ),
            )
        )

    if org_id and org_id in RESERVED_IDENTIFIER_VALUES:
        issues.append(
            LintIssue(
                level="error",
                code="reserved_org_id",
                path=f"orgs[{idx}].org_id",
                message=f"org_id '{org_id}' é reservado/não portável entre ambientes.",
            )
        )

    if msp_id and not MSP_ID_REGEX.fullmatch(msp_id):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_msp_id_format",
                path=f"orgs[{idx}].msp_id",
                message="msp_id inválido. Exemplo válido: Org1MSP.",
            )
        )

    if org_id and msp_id and not _is_msp_consistent_with_org(org_id, msp_id):
        issues.append(
            LintIssue(
                level="error",
                code="org_msp_id_inconsistent",
                path=f"orgs[{idx}].msp_id",
                message=(
                    f"msp_id '{msp_id}' inconsistente com org_id '{org_id}'. "
                    "Esperado prefixo equivalente ao org_id seguido de 'MSP'."
                ),
            )
        )

    if domain and not _is_valid_domain(domain):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_domain_format",
                path=f"orgs[{idx}].domain",
                message="domain inválido para padrão DNS/Fabric.",
            )
        )


def _validate_minimum_role_by_topology(
    normalized_orgs: List[Dict[str, Any]],
    channels: Any,
    issues: List[LintIssue],
) -> None:
    if not isinstance(channels, list):
        return

    org_index: Dict[str, Tuple[int, Dict[str, Any]]] = {
        str(org["org_id"]).lower(): (idx, org)
        for idx, org in enumerate(normalized_orgs)
        if org.get("org_id")
    }
    org_channel_membership: Dict[str, List[int]] = {}

    for channel_idx, channel in enumerate(channels):
        if not isinstance(channel, dict):
            continue
        members = channel.get("members")
        if not isinstance(members, list):
            continue
        for member_idx, org_id in enumerate(members):
            if not isinstance(org_id, str) or not org_id.strip():
                continue
            org_key = org_id.strip().lower()
            org_channel_membership.setdefault(org_key, []).append(channel_idx)

    for org_key, channel_positions in org_channel_membership.items():
        indexed = org_index.get(org_key)
        if not indexed:
            continue
        org_idx, org = indexed
        roles = set(org.get("roles") or [])
        if "peer" not in roles:
            joined = ", ".join(str(index) for index in channel_positions)
            issues.append(
                LintIssue(
                    level="error",
                    code="org_without_peer_in_channel",
                    path=f"orgs[{org_idx}].roles",
                    message=(
                        f"Organização '{org.get('org_id')}' participa de canais ({joined}) "
                        "mas não possui role 'peer'."
                    ),
                )
            )


def _normalize_channel_members(
    raw_members: Any,
    channel_path: str,
    issues: List[LintIssue],
) -> List[str]:
    path = f"{channel_path}.members"
    if not isinstance(raw_members, list) or not raw_members:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_channel_members",
                path=path,
                message="Campo 'members' deve ser lista não vazia.",
            )
        )
        return []

    normalized: List[str] = []
    seen = set()
    for idx, member in enumerate(raw_members):
        member_path = f"{path}[{idx}]"
        if not isinstance(member, str) or not member.strip():
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_channel_member_type",
                    path=member_path,
                    message="Cada membro do canal deve ser org_id string não vazio.",
                )
            )
            continue
        org_id = member.strip().lower()
        if org_id not in seen:
            normalized.append(org_id)
            seen.add(org_id)
    return normalized


def _normalize_anchor_peers(
    raw_anchor_peers: Any,
    channel_path: str,
    issues: List[LintIssue],
) -> List[Dict[str, str]]:
    path = f"{channel_path}.anchor_peers"
    if not isinstance(raw_anchor_peers, list) or not raw_anchor_peers:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_anchor_peers",
                path=path,
                message="Campo 'anchor_peers' deve ser lista não vazia.",
            )
        )
        return []

    normalized: List[Dict[str, str]] = []
    seen = set()
    for idx, anchor in enumerate(raw_anchor_peers):
        anchor_path = f"{path}[{idx}]"
        if not isinstance(anchor, dict):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_anchor_peer_object",
                    path=anchor_path,
                    message="Cada anchor peer deve ser objeto com org_id e peer_ref.",
                )
            )
            continue
        org_id = _ensure_str("org_id", anchor.get("org_id"), issues, f"{anchor_path}.org_id").lower()
        peer_ref = _ensure_str("peer_ref", anchor.get("peer_ref"), issues, f"{anchor_path}.peer_ref")
        key = (org_id, peer_ref)
        if org_id and peer_ref and key not in seen:
            normalized.append({"org_id": org_id, "peer_ref": peer_ref})
            seen.add(key)
    return normalized


def _normalize_capabilities(
    raw_capabilities: Any,
    channel_path: str,
    issues: List[LintIssue],
) -> Dict[str, Any]:
    path = f"{channel_path}.capabilities"
    if not isinstance(raw_capabilities, dict) or not raw_capabilities:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_channel_capabilities",
                path=path,
                message="Campo 'capabilities' deve ser objeto não vazio.",
            )
        )
        return {}
    return raw_capabilities


def _normalize_policy_base(
    raw_policy_base: Any,
    channel_path: str,
    issues: List[LintIssue],
) -> Dict[str, Any]:
    path = f"{channel_path}.policy_base"
    if not isinstance(raw_policy_base, dict) or not raw_policy_base:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_channel_policy_base",
                path=path,
                message="Campo 'policy_base' deve ser objeto não vazio.",
            )
        )
        return {}

    endorsement = raw_policy_base.get("endorsement")
    acl_profile = raw_policy_base.get("acl_profile")
    if not isinstance(endorsement, str) or not endorsement.strip():
        issues.append(
            LintIssue(
                level="error",
                code="invalid_channel_endorsement_policy",
                path=f"{path}.endorsement",
                message="Campo 'endorsement' em policy_base deve ser string não vazia.",
            )
        )
    if not isinstance(acl_profile, str) or not acl_profile.strip():
        issues.append(
            LintIssue(
                level="error",
                code="invalid_channel_acl_profile",
                path=f"{path}.acl_profile",
                message="Campo 'acl_profile' em policy_base deve ser string não vazia.",
            )
        )
    return {
        "endorsement": (endorsement or "").strip() if isinstance(endorsement, str) else "",
        "acl_profile": (acl_profile or "").strip() if isinstance(acl_profile, str) else "",
    }


def _validate_channel_uniqueness(channels: List[Dict[str, Any]], issues: List[LintIssue]) -> None:
    seen: Dict[str, int] = {}
    seen_normalized: Dict[str, int] = {}
    for idx, channel in enumerate(channels):
        channel_id = channel.get("channel_id", "")
        if not channel_id:
            continue
        key = channel_id.lower()
        if key in seen:
            issues.append(
                LintIssue(
                    level="error",
                    code="duplicate_channel_id",
                    path=f"channels[{idx}].channel_id",
                    message=f"channel_id duplicado com channels[{seen[key]}].",
                )
            )
        else:
            seen[key] = idx

        normalized_key = _identifier_normalization_key(channel_id)
        if normalized_key in seen_normalized and seen_normalized[normalized_key] != idx:
            issues.append(
                LintIssue(
                    level="error",
                    code="ambiguous_channel_id_normalization",
                    path=f"channels[{idx}].channel_id",
                    message=(
                        f"channel_id '{channel_id}' colide por normalização com "
                        f"channels[{seen_normalized[normalized_key]}].channel_id."
                    ),
                )
            )
        else:
            seen_normalized[normalized_key] = idx


def _validate_channels_cross_references(
    channels: List[Dict[str, Any]],
    normalized_orgs: List[Dict[str, Any]],
    issues: List[LintIssue],
) -> None:
    org_index: Dict[str, Tuple[int, Dict[str, Any]]] = {
        str(org["org_id"]).lower(): (idx, org)
        for idx, org in enumerate(normalized_orgs)
        if org.get("org_id")
    }

    for channel_idx, channel in enumerate(channels):
        members = channel.get("members") or []
        if len(members) < 2:
            issues.append(
                LintIssue(
                    level="warning",
                    code="single_org_channel",
                    path=f"channels[{channel_idx}].members",
                    message="Canal com menos de 2 organizações; permitido para dev, não recomendado para consórcio.",
                )
            )

        member_set = set(members)
        for member_idx, org_id in enumerate(members):
            indexed = org_index.get(org_id)
            if not indexed:
                issues.append(
                    LintIssue(
                        level="error",
                        code="channel_member_org_not_found",
                        path=f"channels[{channel_idx}].members[{member_idx}]",
                        message=f"Organização '{org_id}' não existe em orgs[].",
                    )
                )
                continue
            _, org = indexed
            roles = set(org.get("roles") or [])
            if "peer" not in roles:
                issues.append(
                    LintIssue(
                        level="error",
                        code="channel_member_without_peer_role",
                        path=f"channels[{channel_idx}].members[{member_idx}]",
                        message=f"Organização '{org_id}' participa do canal sem role 'peer'.",
                    )
                )

        for anchor_idx, anchor in enumerate(channel.get("anchor_peers") or []):
            org_id = anchor.get("org_id", "").lower()
            if org_id not in member_set:
                issues.append(
                    LintIssue(
                        level="error",
                        code="anchor_peer_org_not_in_members",
                        path=f"channels[{channel_idx}].anchor_peers[{anchor_idx}].org_id",
                        message=f"Anchor peer referencia org '{org_id}' fora de members.",
                    )
                )


def validate_channels_block(
    blueprint: Dict[str, Any],
    normalized_orgs: List[Dict[str, Any]],
    issues: Optional[List[LintIssue]] = None,
) -> Tuple[List[Dict[str, Any]], List[LintIssue]]:
    local_issues: List[LintIssue] = [] if issues is None else issues
    raw_channels = blueprint.get("channels")
    if raw_channels is None:
        local_issues.append(
            LintIssue(
                level="warning",
                code="missing_channels_block",
                path="channels",
                message="Bloco 'channels' não definido; validações de topologia de canal não aplicadas.",
            )
        )
        return [], local_issues

    if not isinstance(raw_channels, list):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_channels_block",
                path="channels",
                message="Bloco 'channels' deve ser uma lista.",
            )
        )
        return [], local_issues

    normalized_channels: List[Dict[str, Any]] = []
    for idx, raw_channel in enumerate(raw_channels):
        channel_path = f"channels[{idx}]"
        if not isinstance(raw_channel, dict):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_channel_object",
                    path=channel_path,
                    message="Cada item de channels[] deve ser objeto.",
                )
            )
            continue

        raw_channel_id = _ensure_str(
            "channel_id", raw_channel.get("channel_id"), local_issues, f"{channel_path}.channel_id"
        )
        channel_id = raw_channel_id.lower()
        channel_type = _ensure_str(
            "type", raw_channel.get("type"), local_issues, f"{channel_path}.type"
        ).lower()

        if channel_id and not CHANNEL_ID_REGEX.fullmatch(channel_id):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_channel_id_format",
                    path=f"{channel_path}.channel_id",
                    message=(
                        f"channel_id '{raw_channel_id}' inválido. Esperado: lowercase alfanumérico com hífen "
                        "(ex.: ops-channel)."
                    ),
                )
            )
        if channel_id and channel_id in RESERVED_IDENTIFIER_VALUES:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="reserved_channel_id",
                    path=f"{channel_path}.channel_id",
                    message=f"channel_id '{channel_id}' é reservado/não portável entre ambientes.",
                )
            )
        if channel_type and channel_type not in ALLOWED_CHANNEL_TYPES:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_channel_type",
                    path=f"{channel_path}.type",
                    message=(
                        f"type inválido '{channel_type}'. Valores permitidos: "
                        f"{', '.join(sorted(ALLOWED_CHANNEL_TYPES))}."
                    ),
                )
            )

        members = _normalize_channel_members(raw_channel.get("members"), channel_path, local_issues)
        anchor_peers = _normalize_anchor_peers(
            raw_channel.get("anchor_peers"), channel_path, local_issues
        )
        capabilities = _normalize_capabilities(
            raw_channel.get("capabilities"), channel_path, local_issues
        )
        policy_base = _normalize_policy_base(
            raw_channel.get("policy_base"), channel_path, local_issues
        )

        normalized_channels.append(
            {
                "channel_id": channel_id,
                "type": channel_type,
                "members": members,
                "anchor_peers": anchor_peers,
                "capabilities": capabilities,
                "policy_base": policy_base,
            }
        )

    _validate_channel_uniqueness(normalized_channels, local_issues)
    _validate_channels_cross_references(normalized_channels, normalized_orgs, local_issues)
    return normalized_channels, local_issues


def _normalize_ports(raw_ports: Any, node_path: str, issues: List[LintIssue]) -> List[int]:
    path = f"{node_path}.ports"
    if not isinstance(raw_ports, list) or not raw_ports:
        issues.append(
            LintIssue(
                level="error",
                code="invalid_node_ports",
                path=path,
                message="Campo 'ports' deve ser lista não vazia de portas TCP.",
            )
        )
        return []

    normalized: List[int] = []
    seen = set()
    for idx, value in enumerate(raw_ports):
        item_path = f"{path}[{idx}]"
        if not isinstance(value, int):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_port_type",
                    path=item_path,
                    message="Porta deve ser número inteiro.",
                )
            )
            continue
        if value < 1 or value > 65535:
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_port_range",
                    path=item_path,
                    message="Porta fora da faixa válida (1..65535).",
                )
            )
            continue
        if value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def _normalize_resources(raw_resources: Any, node_path: str, issues: List[LintIssue]) -> Dict[str, int]:
    path = f"{node_path}.resources"
    if not isinstance(raw_resources, dict):
        issues.append(
            LintIssue(
                level="error",
                code="missing_node_resources",
                path=path,
                message="Campo 'resources' é obrigatório.",
            )
        )
        return {}

    normalized: Dict[str, int] = {}
    for key in ("cpu", "memory_mb", "disk_gb"):
        value = raw_resources.get(key)
        key_path = f"{path}.{key}"
        if not isinstance(value, int) or value <= 0:
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_resource_value",
                    path=key_path,
                    message=f"Recurso '{key}' deve ser inteiro > 0.",
                )
            )
            continue
        normalized[key] = value
    return normalized


def _validate_node_uniqueness(nodes: List[Dict[str, Any]], issues: List[LintIssue]) -> None:
    seen_node_ids: Dict[str, int] = {}
    seen_node_id_normalized: Dict[str, int] = {}
    host_ports: Dict[str, Dict[int, int]] = {}

    for idx, node in enumerate(nodes):
        node_id = node.get("node_id", "")
        host_ref = node.get("host_ref", "")
        ports = node.get("ports", [])

        if node_id:
            key = node_id.lower()
            if key in seen_node_ids:
                issues.append(
                    LintIssue(
                        level="error",
                        code="duplicate_node_id",
                        path=f"nodes[{idx}].node_id",
                        message=f"node_id duplicado com nodes[{seen_node_ids[key]}].",
                    )
                )
            else:
                seen_node_ids[key] = idx

            normalized_key = _identifier_normalization_key(node_id)
            if normalized_key in seen_node_id_normalized and seen_node_id_normalized[normalized_key] != idx:
                issues.append(
                    LintIssue(
                        level="error",
                        code="ambiguous_node_id_normalization",
                        path=f"nodes[{idx}].node_id",
                        message=(
                            f"node_id '{node_id}' colide por normalização com "
                            f"nodes[{seen_node_id_normalized[normalized_key]}].node_id."
                        ),
                    )
                )
            else:
                seen_node_id_normalized[normalized_key] = idx

        if not host_ref:
            continue
        host_key = host_ref.lower()
        host_ports.setdefault(host_key, {})
        for port in ports:
            if port in host_ports[host_key]:
                issues.append(
                    LintIssue(
                        level="error",
                        code="duplicate_host_port",
                        path=f"nodes[{idx}].ports",
                        message=(
                            f"Porta {port} em host '{host_ref}' já usada em "
                            f"nodes[{host_ports[host_key][port]}]."
                        ),
                    )
                )
            else:
                host_ports[host_key][port] = idx


def _is_runtime_eligible_node(node: Dict[str, Any]) -> bool:
    node_id = node.get("node_id")
    org_id = node.get("org_id")
    node_type = node.get("node_type")
    host_ref = node.get("host_ref")
    ports = node.get("ports")
    resources = node.get("resources")

    if not isinstance(node_id, str) or not node_id.strip():
        return False
    if not isinstance(org_id, str) or not org_id.strip():
        return False
    if not isinstance(node_type, str) or node_type not in ALLOWED_NODE_TYPES:
        return False
    if not isinstance(host_ref, str) or not host_ref.strip():
        return False
    if not isinstance(ports, list) or not ports:
        return False
    if not all(isinstance(port, int) and 1 <= port <= 65535 for port in ports):
        return False
    if not isinstance(resources, dict):
        return False

    cpu = resources.get("cpu")
    memory_mb = resources.get("memory_mb")
    disk_gb = resources.get("disk_gb")
    return (
        isinstance(cpu, int)
        and cpu > 0
        and isinstance(memory_mb, int)
        and memory_mb > 0
        and isinstance(disk_gb, int)
        and disk_gb > 0
    )


def _validate_resource_profile_minimums(
    nodes: List[Dict[str, Any]],
    stage: str,
    issues: List[LintIssue],
) -> None:
    if stage not in MIN_NODE_RESOURCES_BY_STAGE:
        return

    stage_requirements = MIN_NODE_RESOURCES_BY_STAGE[stage]
    for idx, node in enumerate(nodes):
        node_type = str(node.get("node_type", "")).strip().lower()
        required = stage_requirements.get(node_type)
        if not required:
            continue

        resources = node.get("resources") or {}
        for resource_key, minimum in required.items():
            current = resources.get(resource_key)
            if isinstance(current, int) and current < minimum:
                issues.append(
                    LintIssue(
                        level="error",
                        code="node_resources_below_minimum",
                        path=f"nodes[{idx}].resources.{resource_key}",
                        message=(
                            f"Recurso '{resource_key}' do node tipo '{node_type}' em stage '{stage}' "
                            f"está abaixo do mínimo ({current} < {minimum})."
                        ),
                    )
                )


def _validate_host_resource_oversubscription(
    nodes: List[Dict[str, Any]],
    constraints: Dict[str, Any],
    issues: List[LintIssue],
) -> None:
    max_cpu_per_host = constraints.get("max_cpu_per_host")
    max_memory_mb_per_host = constraints.get("max_memory_mb_per_host")
    max_disk_gb_per_host = constraints.get("max_disk_gb_per_host")

    if not any(
        isinstance(value, int) and value > 0
        for value in (max_cpu_per_host, max_memory_mb_per_host, max_disk_gb_per_host)
    ):
        return

    host_totals: Dict[str, Dict[str, int]] = {}
    for node in nodes:
        host_ref = str(node.get("host_ref", "")).strip().lower()
        if not host_ref:
            continue
        resources = node.get("resources") or {}
        host_totals.setdefault(host_ref, {"cpu": 0, "memory_mb": 0, "disk_gb": 0})
        for resource_key in ("cpu", "memory_mb", "disk_gb"):
            value = resources.get(resource_key)
            if isinstance(value, int) and value > 0:
                host_totals[host_ref][resource_key] += value

    for host_ref, totals in host_totals.items():
        if isinstance(max_cpu_per_host, int) and max_cpu_per_host > 0 and totals["cpu"] > max_cpu_per_host:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_host_cpu_oversubscription",
                    path="nodes",
                    message=(
                        f"Host '{host_ref}' excede max_cpu_per_host "
                        f"({totals['cpu']} > {max_cpu_per_host})."
                    ),
                )
            )
        if (
            isinstance(max_memory_mb_per_host, int)
            and max_memory_mb_per_host > 0
            and totals["memory_mb"] > max_memory_mb_per_host
        ):
            issues.append(
                LintIssue(
                    level="error",
                    code="node_host_memory_oversubscription",
                    path="nodes",
                    message=(
                        f"Host '{host_ref}' excede max_memory_mb_per_host "
                        f"({totals['memory_mb']} > {max_memory_mb_per_host})."
                    ),
                )
            )
        if isinstance(max_disk_gb_per_host, int) and max_disk_gb_per_host > 0 and totals["disk_gb"] > max_disk_gb_per_host:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_host_disk_oversubscription",
                    path="nodes",
                    message=(
                        f"Host '{host_ref}' excede max_disk_gb_per_host "
                        f"({totals['disk_gb']} > {max_disk_gb_per_host})."
                    ),
                )
            )


def _validate_storage_profile_by_stage(
    nodes: List[Dict[str, Any]],
    stage: str,
    issues: List[LintIssue],
) -> None:
    if stage != "prod":
        return

    for idx, node in enumerate(nodes):
        storage_profile = str(node.get("storage_profile", "")).strip().lower()
        if not storage_profile:
            continue
        if any(token in storage_profile for token in NON_PERSISTENT_STORAGE_TOKENS):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_storage_profile_for_stage",
                    path=f"nodes[{idx}].storage_profile",
                    message=(
                        f"storage_profile '{storage_profile}' não é permitido em stage 'prod' "
                        "por risco de não persistência."
                    ),
                )
            )


def _validate_nodes_cross_references(
    nodes: List[Dict[str, Any]],
    normalized_orgs: List[Dict[str, Any]],
    normalized_channels: List[Dict[str, Any]],
    environment_profile: Dict[str, Any],
    issues: List[LintIssue],
) -> None:
    org_index: Dict[str, Tuple[int, Dict[str, Any]]] = {
        str(org["org_id"]).lower(): (idx, org)
        for idx, org in enumerate(normalized_orgs)
        if org.get("org_id")
    }

    eligible_nodes_by_org_and_type: Dict[Tuple[str, str], int] = {}
    eligible_nodes_by_type: Dict[str, int] = {}
    distinct_hosts_by_type: Dict[str, set[str]] = {}

    for idx, node in enumerate(nodes):
        org_id = node.get("org_id", "")
        node_type = node.get("node_type", "")

        org_ref = org_index.get(org_id)
        if not org_ref:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_org_not_found",
                    path=f"nodes[{idx}].org_id",
                    message=f"Organização '{org_id}' não existe em orgs[].",
                )
            )
            continue

        _, org = org_ref
        roles = set(org.get("roles") or [])
        if node_type and node_type not in roles:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_type_not_allowed_for_org",
                    path=f"nodes[{idx}].node_type",
                    message=(
                        f"node_type '{node_type}' não permitido para org '{org_id}' "
                        f"com roles {sorted(roles)}."
                    ),
                )
            )

        if _is_runtime_eligible_node(node):
            key = (org_id, node_type)
            eligible_nodes_by_org_and_type[key] = eligible_nodes_by_org_and_type.get(key, 0) + 1
            eligible_nodes_by_type[node_type] = eligible_nodes_by_type.get(node_type, 0) + 1

            host_ref = str(node.get("host_ref", "")).strip().lower()
            if host_ref:
                distinct_hosts_by_type.setdefault(node_type, set()).add(host_ref)

    stage = str(environment_profile.get("stage", "")).strip().lower()

    if normalized_channels and eligible_nodes_by_type.get("peer", 0) < 1:
        issues.append(
            LintIssue(
                level="error",
                code="missing_peer_nodes_for_topology",
                path="nodes",
                message=(
                    "Topologia inválida: channels[] definido sem nenhum node peer elegível "
                    "(com host_ref, ports e resources válidos)."
                ),
            )
        )

    if stage in {"hml", "prod"} and eligible_nodes_by_type.get("peer", 0) < 2:
        issues.append(
            LintIssue(
                level="error",
                code="insufficient_peer_cardinality_for_stage",
                path="nodes",
                message=(
                    f"Topologia em stage '{stage}' exige ao menos 2 peers elegíveis; "
                    f"encontrado: {eligible_nodes_by_type.get('peer', 0)}."
                ),
            )
        )

    if stage == "prod" and 0 < eligible_nodes_by_type.get("orderer", 0) < 3:
        issues.append(
            LintIssue(
                level="error",
                code="insufficient_orderer_cardinality_for_stage",
                path="nodes",
                message=(
                    "Topologia em stage 'prod' com ordering service exige ao menos 3 orderers "
                    f"elegíveis; encontrado: {eligible_nodes_by_type.get('orderer', 0)}."
                ),
            )
        )

    if stage == "prod" and eligible_nodes_by_type.get("orderer", 0) >= 3:
        orderer_hosts = distinct_hosts_by_type.get("orderer", set())
        if len(orderer_hosts) < 3:
            issues.append(
                LintIssue(
                    level="warning",
                    code="insufficient_orderer_host_distribution",
                    path="nodes",
                    message=(
                        "Orderers elegíveis em produção devem preferencialmente estar distribuídos "
                        f"em 3 hosts distintos; encontrado: {len(orderer_hosts)}."
                    ),
                )
            )

    for org_idx, org in enumerate(normalized_orgs):
        org_id = str(org.get("org_id", "")).strip().lower()
        roles = set(org.get("roles") or [])
        if not org_id:
            continue

        if "orderer" in roles and eligible_nodes_by_org_and_type.get((org_id, "orderer"), 0) < 1:
            issues.append(
                LintIssue(
                    level="error",
                    code="orderer_org_without_orderer_node",
                    path=f"orgs[{org_idx}].roles",
                    message=(
                        f"Organização '{org_id}' possui role 'orderer', mas não possui node "
                        "orderer elegível em nodes[]."
                    ),
                )
            )

    for channel_idx, channel in enumerate(normalized_channels):
        for member in channel.get("members") or []:
            if eligible_nodes_by_org_and_type.get((member, "peer"), 0) < 1:
                issues.append(
                    LintIssue(
                        level="error",
                        code="channel_member_without_peer_node",
                        path=f"channels[{channel_idx}].members",
                        message=(
                            f"Organização '{member}' participa do canal, mas não possui node peer "
                            "elegível em nodes[] (host_ref, ports e resources válidos)."
                        ),
                    )
                )

    constraints = environment_profile.get("infra_constraints")
    if isinstance(constraints, dict):
        max_cpu = constraints.get("max_cpu")
        max_memory_mb = constraints.get("max_memory_mb")
        max_disk_gb = constraints.get("max_disk_gb")
        for idx, node in enumerate(nodes):
            resources = node.get("resources") or {}
            cpu = resources.get("cpu")
            memory_mb = resources.get("memory_mb")
            disk_gb = resources.get("disk_gb")
            if isinstance(max_cpu, int) and isinstance(cpu, int) and cpu > max_cpu:
                issues.append(
                    LintIssue(
                        level="error",
                        code="node_cpu_exceeds_profile",
                        path=f"nodes[{idx}].resources.cpu",
                        message=f"CPU ({cpu}) excede max_cpu ({max_cpu}) do environment_profile.",
                    )
                )
            if isinstance(max_memory_mb, int) and isinstance(memory_mb, int) and memory_mb > max_memory_mb:
                issues.append(
                    LintIssue(
                        level="error",
                        code="node_memory_exceeds_profile",
                        path=f"nodes[{idx}].resources.memory_mb",
                        message=(
                            f"memory_mb ({memory_mb}) excede max_memory_mb ({max_memory_mb}) "
                            "do environment_profile."
                        ),
                    )
                )
            if isinstance(max_disk_gb, int) and isinstance(disk_gb, int) and disk_gb > max_disk_gb:
                issues.append(
                    LintIssue(
                        level="error",
                        code="node_disk_exceeds_profile",
                        path=f"nodes[{idx}].resources.disk_gb",
                        message=f"disk_gb ({disk_gb}) excede max_disk_gb ({max_disk_gb}) do environment_profile.",
                    )
                )

        _validate_host_resource_oversubscription(nodes, constraints, issues)

    _validate_resource_profile_minimums(nodes, stage, issues)
    _validate_storage_profile_by_stage(nodes, stage, issues)

    deployment_stack = str(environment_profile.get("deployment_stack", "")).strip().lower()
    compatibility = environment_profile.get("stack_compatibility") or []
    compatible_stacks = {
        str(value).strip().lower()
        for value in compatibility
        if isinstance(value, str) and value.strip()
    }

    used_node_stacks: set[str] = set()

    for idx, node in enumerate(nodes):
        node_stack = str(node.get("stack", "")).strip().lower()
        if node_stack and node_stack not in ALLOWED_DEPLOYMENT_STACKS:
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_node_stack",
                    path=f"nodes[{idx}].stack",
                    message=(
                        f"Stack '{node_stack}' inválida para node. Valores permitidos: "
                        f"{', '.join(sorted(ALLOWED_DEPLOYMENT_STACKS))}."
                    ),
                )
            )

        if node_stack and node_stack not in compatible_stacks:
            issues.append(
                LintIssue(
                    level="error",
                    code="node_stack_not_compatible",
                    path=f"nodes[{idx}].stack",
                    message=(
                        f"Stack '{node_stack}' do node não está em stack_compatibility "
                        f"({sorted(compatible_stacks)})."
                    ),
                )
            )

        if node_stack in ALLOWED_DEPLOYMENT_STACKS:
            used_node_stacks.add(node_stack)

        if deployment_stack in {"docker", "k8s"}:
            if not node_stack:
                issues.append(
                    LintIssue(
                        level="warning",
                        code="missing_node_stack",
                        path=f"nodes[{idx}].stack",
                        message=(
                            "Node sem stack explícita; para deployment_stack fixo recomenda-se "
                            "declarar nodes[].stack."
                        ),
                    )
                )
            elif node_stack != deployment_stack:
                issues.append(
                    LintIssue(
                        level="error",
                        code="node_stack_mismatch_selected",
                        path=f"nodes[{idx}].stack",
                        message=(
                            f"Node usa stack '{node_stack}' mas deployment_stack é '{deployment_stack}'."
                        ),
                    )
                )

        if deployment_stack == "hybrid" and not node_stack:
            issues.append(
                LintIssue(
                    level="warning",
                    code="missing_node_stack_hybrid",
                    path=f"nodes[{idx}].stack",
                    message=(
                        "Node sem stack explícita em deployment_stack 'hybrid'; "
                        "declare nodes[].stack para roteamento determinístico."
                    ),
                )
            )

    if deployment_stack == "hybrid":
        selected_compatible = {stack for stack in compatible_stacks if stack in {"docker", "k8s"}}
        if len(selected_compatible) < 2:
            issues.append(
                LintIssue(
                    level="error",
                    code="hybrid_requires_multi_stack_compatibility",
                    path="environment_profile.stack_compatibility",
                    message=(
                        "deployment_stack 'hybrid' exige stack_compatibility com ao menos "
                        "'docker' e 'k8s'."
                    ),
                )
            )

        if used_node_stacks and len(used_node_stacks.intersection({"docker", "k8s"})) < 2:
            issues.append(
                LintIssue(
                    level="warning",
                    code="hybrid_single_runtime_detected",
                    path="nodes",
                    message=(
                        "deployment_stack 'hybrid' declarado, mas os nodes atuais usam apenas "
                        "uma stack efetiva; valide se o perfil deveria ser fixo."
                    ),
                )
            )


def validate_nodes_block(
    blueprint: Dict[str, Any],
    normalized_orgs: List[Dict[str, Any]],
    normalized_channels: List[Dict[str, Any]],
    normalized_environment_profile: Dict[str, Any],
    issues: Optional[List[LintIssue]] = None,
) -> Tuple[List[Dict[str, Any]], List[LintIssue]]:
    local_issues: List[LintIssue] = [] if issues is None else issues
    raw_nodes = blueprint.get("nodes")
    if raw_nodes is None:
        local_issues.append(
            LintIssue(
                level="warning",
                code="missing_nodes_block",
                path="nodes",
                message="Bloco 'nodes' não definido; validações de provisão por host não aplicadas.",
            )
        )
        return [], local_issues

    if not isinstance(raw_nodes, list):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_nodes_block",
                path="nodes",
                message="Bloco 'nodes' deve ser uma lista.",
            )
        )
        return [], local_issues

    normalized_nodes: List[Dict[str, Any]] = []
    for idx, raw_node in enumerate(raw_nodes):
        node_path = f"nodes[{idx}]"
        if not isinstance(raw_node, dict):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_node_object",
                    path=node_path,
                    message="Cada item de nodes[] deve ser objeto.",
                )
            )
            continue

        raw_node_id = _ensure_str("node_id", raw_node.get("node_id"), local_issues, f"{node_path}.node_id")
        node_id = raw_node_id.lower()
        org_id = _ensure_str("org_id", raw_node.get("org_id"), local_issues, f"{node_path}.org_id").lower()
        node_type = _ensure_str(
            "node_type", raw_node.get("node_type"), local_issues, f"{node_path}.node_type"
        ).lower()
        host_ref = _ensure_str(
            "host_ref", raw_node.get("host_ref"), local_issues, f"{node_path}.host_ref"
        ).lower()
        storage_profile = _ensure_str(
            "storage_profile", raw_node.get("storage_profile"), local_issues, f"{node_path}.storage_profile"
        )
        ports = _normalize_ports(raw_node.get("ports"), node_path, local_issues)
        resources = _normalize_resources(raw_node.get("resources"), node_path, local_issues)

        if node_id and not NODE_ID_REGEX.fullmatch(node_id):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_node_id_format",
                    path=f"{node_path}.node_id",
                    message=(
                        f"node_id '{raw_node_id}' inválido. Esperado: lowercase alfanumérico com hífen "
                        "(ex.: peer0-orga)."
                    ),
                )
            )
        if node_id and node_id in RESERVED_IDENTIFIER_VALUES:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="reserved_node_id",
                    path=f"{node_path}.node_id",
                    message=f"node_id '{node_id}' é reservado/não portável entre ambientes.",
                )
            )

        if node_type and node_type not in ALLOWED_NODE_TYPES:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_node_type",
                    path=f"{node_path}.node_type",
                    message=(
                        f"node_type inválido '{node_type}'. Valores permitidos: "
                        f"{', '.join(sorted(ALLOWED_NODE_TYPES))}."
                    ),
                )
            )

        normalized_nodes.append(
            {
                "node_id": node_id,
                "org_id": org_id,
                "node_type": node_type,
                "host_ref": host_ref,
                "ports": ports,
                "storage_profile": storage_profile,
                "stack": (
                    str(raw_node.get("stack", "")).strip().lower()
                    if isinstance(raw_node.get("stack", ""), str)
                    else ""
                ),
                "resources": resources,
            }
        )

    _validate_node_uniqueness(normalized_nodes, local_issues)
    _validate_nodes_cross_references(
        normalized_nodes,
        normalized_orgs,
        normalized_channels,
        normalized_environment_profile,
        local_issues,
    )
    return normalized_nodes, local_issues


def validate_environment_profile_block(
    blueprint: Dict[str, Any],
    issues: Optional[List[LintIssue]] = None,
) -> Tuple[Dict[str, Any], List[LintIssue]]:
    local_issues: List[LintIssue] = [] if issues is None else issues
    raw_profile = blueprint.get("environment_profile")
    if raw_profile is None:
        local_issues.append(
            LintIssue(
                level="warning",
                code="missing_environment_profile",
                path="environment_profile",
                message="Bloco 'environment_profile' não definido; presets de ambiente não serão aplicados.",
            )
        )
        return {}, local_issues

    if not isinstance(raw_profile, dict):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_environment_profile",
                path="environment_profile",
                message="environment_profile deve ser objeto.",
            )
        )
        return {}, local_issues

    normalized = {
        "profile_id": _ensure_str(
            "profile_id", raw_profile.get("profile_id"), local_issues, "environment_profile.profile_id"
        ).lower(),
        "stage": _ensure_str("stage", raw_profile.get("stage"), local_issues, "environment_profile.stage").lower(),
        "infra_constraints": raw_profile.get("infra_constraints") if isinstance(raw_profile.get("infra_constraints"), dict) else {},
        "deployment_stack": _ensure_str(
            "deployment_stack",
            raw_profile.get("deployment_stack"),
            local_issues,
            "environment_profile.deployment_stack",
        ).lower(),
        "stack_compatibility": raw_profile.get("stack_compatibility") if isinstance(raw_profile.get("stack_compatibility"), list) else [],
        "security_baseline": _ensure_str(
            "security_baseline",
            raw_profile.get("security_baseline", "baseline"),
            local_issues,
            "environment_profile.security_baseline",
        ),
        "observability_level": _ensure_str(
            "observability_level",
            raw_profile.get("observability_level", "standard"),
            local_issues,
            "environment_profile.observability_level",
        ),
        "cost_class": _ensure_str(
            "cost_class",
            raw_profile.get("cost_class", "default"),
            local_issues,
            "environment_profile.cost_class",
        ),
    }

    if normalized["stage"] and normalized["stage"] not in {"dev", "hml", "prod"}:
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_environment_stage",
                path="environment_profile.stage",
                message="stage deve ser um de: dev, hml, prod.",
            )
        )

    profile_id = normalized["profile_id"]
    if profile_id and not PROFILE_ID_REGEX.fullmatch(profile_id):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_profile_id_format",
                path="environment_profile.profile_id",
                message=(
                    f"profile_id '{profile_id}' inválido. Esperado: lowercase alfanumérico com hífen "
                    "(ex.: dev-small)."
                ),
            )
        )
    if profile_id and profile_id in RESERVED_IDENTIFIER_VALUES:
        local_issues.append(
            LintIssue(
                level="error",
                code="reserved_profile_id",
                path="environment_profile.profile_id",
                message=f"profile_id '{profile_id}' é reservado/não portável entre ambientes.",
            )
        )

    deployment_stack = normalized["deployment_stack"]
    if deployment_stack and deployment_stack not in ALLOWED_DEPLOYMENT_STACKS:
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_deployment_stack",
                path="environment_profile.deployment_stack",
                message=(
                    f"deployment_stack inválido '{deployment_stack}'. Valores: "
                    f"{', '.join(sorted(ALLOWED_DEPLOYMENT_STACKS))}."
                ),
            )
        )

    raw_compatibility = normalized["stack_compatibility"]
    normalized_compatibility: List[str] = []
    seen = set()
    if not raw_compatibility:
        local_issues.append(
            LintIssue(
                level="error",
                code="missing_stack_compatibility",
                path="environment_profile.stack_compatibility",
                message="stack_compatibility é obrigatório para compatibilidade explícita de stack.",
            )
        )
    else:
        for idx, value in enumerate(raw_compatibility):
            value_path = f"environment_profile.stack_compatibility[{idx}]"
            if not isinstance(value, str) or not value.strip():
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="invalid_stack_compatibility_item",
                        path=value_path,
                        message="Cada item de stack_compatibility deve ser string não vazia.",
                    )
                )
                continue
            stack = value.strip().lower()
            if stack not in ALLOWED_DEPLOYMENT_STACKS:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="invalid_stack_compatibility_value",
                        path=value_path,
                        message=(
                            f"Stack '{stack}' inválida em compatibilidade. Valores: "
                            f"{', '.join(sorted(ALLOWED_DEPLOYMENT_STACKS))}."
                        ),
                    )
                )
                continue
            if stack not in seen:
                normalized_compatibility.append(stack)
                seen.add(stack)
    normalized["stack_compatibility"] = normalized_compatibility

    if deployment_stack and normalized_compatibility and deployment_stack not in normalized_compatibility:
        local_issues.append(
            LintIssue(
                level="error",
                code="deployment_stack_not_compatible",
                path="environment_profile.deployment_stack",
                message=(
                    f"deployment_stack '{deployment_stack}' não está em stack_compatibility "
                    f"({normalized_compatibility})."
                ),
            )
        )

    constraints = normalized["infra_constraints"]
    if not constraints:
        local_issues.append(
            LintIssue(
                level="warning",
                code="missing_infra_constraints",
                path="environment_profile.infra_constraints",
                message="infra_constraints ausente; validações de limite de recursos ficarão parciais.",
            )
        )
    else:
        for key in (
            "max_cpu",
            "max_memory_mb",
            "max_disk_gb",
            "max_cpu_per_host",
            "max_memory_mb_per_host",
            "max_disk_gb_per_host",
        ):
            value = constraints.get(key)
            if value is not None and (not isinstance(value, int) or value <= 0):
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="invalid_environment_constraint",
                        path=f"environment_profile.infra_constraints.{key}",
                        message=f"{key} deve ser inteiro > 0 quando informado.",
                    )
                )

    return normalized, local_issues


def _normalize_policy_rules(raw_rules: Any, policy_path: str, issues: List[LintIssue]) -> Dict[str, List[str]]:
    path = f"{policy_path}.rules"
    if not isinstance(raw_rules, dict):
        issues.append(
            LintIssue(
                level="error",
                code="invalid_policy_rules",
                path=path,
                message="rules deve ser objeto com allow/deny.",
            )
        )
        return {"allow": [], "deny": []}

    result: Dict[str, List[str]] = {"allow": [], "deny": []}
    for key in ("allow", "deny"):
        raw_list = raw_rules.get(key, [])
        if not isinstance(raw_list, list):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_policy_rule_list",
                    path=f"{path}.{key}",
                    message=f"rules.{key} deve ser lista.",
                )
            )
            continue
        dedup: List[str] = []
        seen = set()
        for idx, value in enumerate(raw_list):
            item_path = f"{path}.{key}[{idx}]"
            if not isinstance(value, str) or not value.strip():
                issues.append(
                    LintIssue(
                        level="error",
                        code="invalid_policy_rule_item",
                        path=item_path,
                        message="Regra deve ser string não vazia.",
                    )
                )
                continue
            item = value.strip()
            lowered = item.lower()
            if lowered not in seen:
                dedup.append(item)
                seen.add(lowered)
        result[key] = dedup

    allow_set = {value.lower() for value in result["allow"]}
    deny_set = {value.lower() for value in result["deny"]}
    overlaps = sorted(allow_set.intersection(deny_set))
    if overlaps:
        issues.append(
            LintIssue(
                level="error",
                code="conflicting_policy_rules",
                path=path,
                message=f"Conflito allow/deny para: {', '.join(overlaps)}.",
            )
        )
    return result


def validate_policies_block(
    blueprint: Dict[str, Any],
    normalized_orgs: List[Dict[str, Any]],
    normalized_channels: List[Dict[str, Any]],
    schema_version: str,
    issues: Optional[List[LintIssue]] = None,
) -> Tuple[List[Dict[str, Any]], List[LintIssue]]:
    local_issues: List[LintIssue] = [] if issues is None else issues
    raw_policies = blueprint.get("policies")
    if raw_policies is None:
        local_issues.append(
            LintIssue(
                level="warning",
                code="missing_policies_block",
                path="policies",
                message="Bloco 'policies' não definido; guardrails de governança ficarão parciais.",
            )
        )
        return [], local_issues

    if not isinstance(raw_policies, list):
        local_issues.append(
            LintIssue(
                level="error",
                code="invalid_policies_block",
                path="policies",
                message="policies deve ser lista.",
            )
        )
        return [], local_issues

    org_ids = {str(org.get("org_id", "")).lower() for org in normalized_orgs if org.get("org_id")}
    channel_ids = {
        str(channel.get("channel_id", "")).lower()
        for channel in normalized_channels
        if channel.get("channel_id")
    }

    normalized_policies: List[Dict[str, Any]] = []
    seen_policy_ids: Dict[str, int] = {}
    for idx, raw_policy in enumerate(raw_policies):
        policy_path = f"policies[{idx}]"
        if not isinstance(raw_policy, dict):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_policy_object",
                    path=policy_path,
                    message="Cada item de policies[] deve ser objeto.",
                )
            )
            continue

        policy_id = _ensure_str(
            "policy_id", raw_policy.get("policy_id"), local_issues, f"{policy_path}.policy_id"
        ).lower()
        scope = _ensure_str("scope", raw_policy.get("scope"), local_issues, f"{policy_path}.scope").lower()
        policy_version = _ensure_str(
            "policy_version", raw_policy.get("policy_version"), local_issues, f"{policy_path}.policy_version"
        )
        schema_ref = _ensure_str(
            "schema_ref", raw_policy.get("schema_ref", schema_version), local_issues, f"{policy_path}.schema_ref"
        )
        approvals = raw_policy.get("approvals") if isinstance(raw_policy.get("approvals"), dict) else {}
        constraints = raw_policy.get("constraints") if isinstance(raw_policy.get("constraints"), dict) else {}
        target = raw_policy.get("target") if isinstance(raw_policy.get("target"), dict) else {}
        rules = _normalize_policy_rules(raw_policy.get("rules"), policy_path, local_issues)

        if policy_id:
            key = policy_id.lower()
            if key in seen_policy_ids:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="duplicate_policy_id",
                        path=f"{policy_path}.policy_id",
                        message=f"policy_id duplicado com policies[{seen_policy_ids[key]}].",
                    )
                )
            else:
                seen_policy_ids[key] = idx

            if not POLICY_ID_REGEX.fullmatch(policy_id):
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="invalid_policy_id_format",
                        path=f"{policy_path}.policy_id",
                        message=(
                            f"policy_id '{policy_id}' inválido. Esperado: lowercase alfanumérico com hífen "
                            "(ex.: policy-channel-ops)."
                        ),
                    )
                )
            if policy_id in RESERVED_IDENTIFIER_VALUES:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="reserved_policy_id",
                        path=f"{policy_path}.policy_id",
                        message=f"policy_id '{policy_id}' é reservado/não portável entre ambientes.",
                    )
                )

        if scope and scope not in ALLOWED_POLICY_SCOPES:
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_policy_scope",
                    path=f"{policy_path}.scope",
                    message=(
                        f"scope inválido '{scope}'. Valores permitidos: "
                        f"{', '.join(sorted(ALLOWED_POLICY_SCOPES))}."
                    ),
                )
            )

        if policy_version and not SEMVER_REGEX.fullmatch(policy_version):
            local_issues.append(
                LintIssue(
                    level="error",
                    code="invalid_policy_version",
                    path=f"{policy_path}.policy_version",
                    message="policy_version deve seguir SemVer (MAJOR.MINOR.PATCH).",
                )
            )

        if schema_ref and schema_version and schema_ref != schema_version:
            local_issues.append(
                LintIssue(
                    level="warning",
                    code="policy_schema_ref_mismatch",
                    path=f"{policy_path}.schema_ref",
                    message=(
                        f"schema_ref ({schema_ref}) difere de schema_version ({schema_version})."
                    ),
                )
            )

        if scope == "org":
            org_id = str(target.get("org_id", "")).strip().lower()
            if not org_id:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="missing_policy_org_target",
                        path=f"{policy_path}.target.org_id",
                        message="scope 'org' exige target.org_id.",
                    )
                )
            elif org_id not in org_ids:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="policy_org_target_not_found",
                        path=f"{policy_path}.target.org_id",
                        message=f"target.org_id '{org_id}' não existe em orgs[].",
                    )
                )

        if scope == "channel":
            channel_id = str(target.get("channel_id", "")).strip().lower()
            if not channel_id:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="missing_policy_channel_target",
                        path=f"{policy_path}.target.channel_id",
                        message="scope 'channel' exige target.channel_id.",
                    )
                )
            elif channel_id not in channel_ids:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="policy_channel_target_not_found",
                        path=f"{policy_path}.target.channel_id",
                        message=f"target.channel_id '{channel_id}' não existe em channels[].",
                    )
                )

        if scope == "operation":
            operation = str(target.get("operation", "")).strip()
            if not operation:
                local_issues.append(
                    LintIssue(
                        level="error",
                        code="missing_policy_operation_target",
                        path=f"{policy_path}.target.operation",
                        message="scope 'operation' exige target.operation.",
                    )
                )

        if not approvals:
            local_issues.append(
                LintIssue(
                    level="warning",
                    code="missing_policy_approvals",
                    path=f"{policy_path}.approvals",
                    message="Approvals ausente; política sem quórum explícito.",
                )
            )

        normalized_policies.append(
            {
                "policy_id": policy_id,
                "scope": scope,
                "policy_version": policy_version,
                "schema_ref": schema_ref,
                "rules": rules,
                "approvals": approvals,
                "constraints": constraints,
                "target": target,
            }
        )

    return normalized_policies, local_issues


def _build_fingerprint_payload(
    metadata: Dict[str, str],
    orgs: List[Dict[str, Any]],
    channels: List[Dict[str, Any]],
    nodes: List[Dict[str, Any]],
    policies: List[Dict[str, Any]],
    environment_profile: Dict[str, Any],
    identity_baseline: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "schema_name": metadata.get("schema_name", ""),
        "schema_version": metadata.get("schema_version", ""),
        "blueprint_version": metadata.get("blueprint_version", ""),
        "orgs": orgs,
        "channels": channels,
        "nodes": nodes,
        "policies": policies,
        "environment_profile": environment_profile,
        "identity_baseline": identity_baseline,
    }


def _compute_blueprint_fingerprint(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_orgs_block(blueprint: Dict[str, Any]) -> OrgsValidationResult:
    issues: List[LintIssue] = []

    raw_orgs = blueprint.get("orgs")
    if not isinstance(raw_orgs, list):
        issues.append(
            LintIssue(
                level="error",
                code="missing_orgs_block",
                path="orgs",
                message="Bloco 'orgs' é obrigatório e deve ser uma lista.",
            )
        )
        return OrgsValidationResult(valid=False, normalized_orgs=[], issues=issues)

    if not raw_orgs:
        issues.append(
            LintIssue(
                level="error",
                code="empty_orgs_block",
                path="orgs",
                message="Bloco 'orgs' não pode ser vazio.",
            )
        )

    normalized_orgs: List[Dict[str, Any]] = []
    for idx, raw_org in enumerate(raw_orgs):
        org_path = f"orgs[{idx}]"
        if not isinstance(raw_org, dict):
            issues.append(
                LintIssue(
                    level="error",
                    code="invalid_org_object",
                    path=org_path,
                    message="Cada item de orgs[] deve ser um objeto.",
                )
            )
            continue

        org_id = _ensure_str("org_id", raw_org.get("org_id"), issues, f"{org_path}.org_id").lower()
        display_name = _ensure_str(
            "display_name", raw_org.get("display_name"), issues, f"{org_path}.display_name"
        )
        msp_id = _ensure_str("msp_id", raw_org.get("msp_id"), issues, f"{org_path}.msp_id")
        domain = _ensure_str("domain", raw_org.get("domain"), issues, f"{org_path}.domain").lower()
        roles = _normalize_roles(raw_org.get("roles"), f"{org_path}.roles", issues)
        identity = _normalize_identity(raw_org.get("identity"), org_path, issues)

        normalized = {
            "org_id": org_id,
            "display_name": display_name,
            "msp_id": msp_id,
            "domain": domain,
            "roles": roles,
            "identity": identity,
        }
        normalized_orgs.append(normalized)

    _validate_uniqueness(normalized_orgs, issues)
    for idx, org in enumerate(normalized_orgs):
        _validate_org_naming(org, idx, issues)
    _validate_minimum_role_by_topology(normalized_orgs, blueprint.get("channels"), issues)

    sorted_issues = _sort_issues_deterministically(issues)
    has_errors = any(issue.level == "error" for issue in sorted_issues)
    return OrgsValidationResult(valid=not has_errors, normalized_orgs=normalized_orgs, issues=sorted_issues)


def load_blueprint(path: Path) -> Dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix == ".json":
        loaded = json.loads(raw_text)
    elif suffix in {".yaml", ".yml"}:
        loaded = yaml.safe_load(raw_text)
    else:
        raise ValueError("Formato não suportado. Use .json, .yaml ou .yml")

    if not isinstance(loaded, dict):
        raise ValueError("Blueprint deve ser um objeto no nível raiz.")
    return loaded


def validate_orgs_file(path: Path) -> OrgsValidationResult:
    blueprint = load_blueprint(path)
    return validate_orgs_block(blueprint)


def validate_blueprint_block(
    blueprint: Dict[str, Any],
    *,
    allow_schema_migration: bool = False,
) -> BlueprintValidationResult:
    merged_issues: List[LintIssue] = []

    resolved_blueprint, resolved_schema_version, migration_applied, migrated_from = _resolve_schema_compatibility(
        blueprint,
        allow_schema_migration,
        merged_issues,
    )

    metadata = _validate_schema_metadata(resolved_blueprint, merged_issues)

    fatal_schema_codes = {
        "schema_migration_required",
        "schema_migration_path_not_found",
        "schema_version_newer_than_runtime",
        "schema_version_major_ahead",
    }
    if any(issue.level == "error" and issue.code in fatal_schema_codes for issue in merged_issues):
        empty_profile: Dict[str, Any] = {}
        fingerprint_payload = _build_fingerprint_payload(
            metadata,
            [],
            [],
            [],
            [],
            empty_profile,
            {},
        )
        sorted_issues = _sort_issues_deterministically(merged_issues)
        return BlueprintValidationResult(
            valid=False,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            resolved_schema_version=resolved_schema_version,
            migration_applied=migration_applied,
            migrated_from_schema_version=migrated_from,
            schema_name=metadata.get("schema_name", ""),
            schema_version=metadata.get("schema_version", ""),
            blueprint_version=metadata.get("blueprint_version", ""),
            created_at=metadata.get("created_at", ""),
            updated_at=metadata.get("updated_at", ""),
            normalized_orgs=[],
            normalized_channels=[],
            normalized_nodes=[],
            normalized_policies=[],
            normalized_environment_profile=empty_profile,
            normalized_identity_baseline={},
            fingerprint_sha256=_compute_blueprint_fingerprint(fingerprint_payload),
            issues=sorted_issues,
        )

    org_result = validate_orgs_block(resolved_blueprint)
    merged_issues.extend(org_result.issues)

    normalized_environment_profile, merged_issues = validate_environment_profile_block(
        resolved_blueprint,
        merged_issues,
    )

    normalized_channels, merged_issues = validate_channels_block(
        resolved_blueprint,
        org_result.normalized_orgs,
        merged_issues,
    )
    normalized_nodes, merged_issues = validate_nodes_block(
        resolved_blueprint,
        org_result.normalized_orgs,
        normalized_channels,
        normalized_environment_profile,
        merged_issues,
    )

    normalized_policies, merged_issues = validate_policies_block(
        resolved_blueprint,
        org_result.normalized_orgs,
        normalized_channels,
        metadata.get("schema_version", ""),
        merged_issues,
    )

    normalized_identity_baseline, merged_issues = validate_identity_baseline_block(
        resolved_blueprint,
        org_result.normalized_orgs,
        metadata.get("schema_version", ""),
        merged_issues,
    )

    fingerprint_payload = _build_fingerprint_payload(
        metadata,
        org_result.normalized_orgs,
        normalized_channels,
        normalized_nodes,
        normalized_policies,
        normalized_environment_profile,
        normalized_identity_baseline,
    )
    fingerprint_sha256 = _compute_blueprint_fingerprint(fingerprint_payload)

    sorted_issues = _sort_issues_deterministically(merged_issues)
    has_errors = any(issue.level == "error" for issue in sorted_issues)
    return BlueprintValidationResult(
        valid=not has_errors,
        current_schema_version=CURRENT_SCHEMA_VERSION,
        resolved_schema_version=resolved_schema_version,
        migration_applied=migration_applied,
        migrated_from_schema_version=migrated_from,
        schema_name=metadata.get("schema_name", ""),
        schema_version=metadata.get("schema_version", ""),
        blueprint_version=metadata.get("blueprint_version", ""),
        created_at=metadata.get("created_at", ""),
        updated_at=metadata.get("updated_at", ""),
        normalized_orgs=org_result.normalized_orgs,
        normalized_channels=normalized_channels,
        normalized_nodes=normalized_nodes,
        normalized_policies=normalized_policies,
        normalized_environment_profile=normalized_environment_profile,
        normalized_identity_baseline=normalized_identity_baseline,
        fingerprint_sha256=fingerprint_sha256,
        issues=sorted_issues,
    )


def validate_blueprint_file(
    path: Path,
    *,
    allow_schema_migration: bool = False,
) -> BlueprintValidationResult:
    blueprint = load_blueprint(path)
    return validate_blueprint_block(blueprint, allow_schema_migration=allow_schema_migration)


def summarize_issues(issues: List[LintIssue]) -> Tuple[int, int, int]:
    errors = sum(1 for issue in issues if issue.level == "error")
    warnings = sum(1 for issue in issues if issue.level == "warning")
    hints = sum(1 for issue in issues if issue.level == "hint")
    return errors, warnings, hints
