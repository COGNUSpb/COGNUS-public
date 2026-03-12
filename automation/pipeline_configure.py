from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Dict, List, Optional, Set, Tuple

from .blueprint_schema import BlueprintValidationResult
from .pipeline_contract import PipelineRun, utc_now_iso
from .pipeline_state_store import PipelineStateStore, StageCheckpoint, payload_sha256


REQUIRED_MSP_LAYOUT_DIRS = {
    "cacerts",
    "intermediatecerts",
    "tlscacerts",
    "signcerts",
    "keystore",
}
CRYPTO_INVENTORY_CONTRACT_VERSION = "1.0.0"
CRYPTO_ROTATION_POLICY_BY_STAGE = {
    "dev": {
        "default_validity_days": 365,
        "renewal_window_days": 30,
        "critical_alert_days": 7,
        "transition_overlap_days": 14,
    },
    "hml": {
        "default_validity_days": 540,
        "renewal_window_days": 60,
        "critical_alert_days": 15,
        "transition_overlap_days": 21,
    },
    "prod": {
        "default_validity_days": 825,
        "renewal_window_days": 90,
        "critical_alert_days": 30,
        "transition_overlap_days": 30,
    },
}


@dataclass(frozen=True)
class ConfigureIssue:
    level: str
    code: str
    path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ConfigureExecutionResult:
    connection_profiles: Dict[str, Any]
    network_manifests: Dict[str, Any]
    technical_inventory: Dict[str, Any]
    configure_report: Dict[str, Any]
    blocked: bool
    artifacts: Dict[str, str]
    issues: List[ConfigureIssue]
    checkpoint: Optional[StageCheckpoint] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connection_profiles": self.connection_profiles,
            "network_manifests": self.network_manifests,
            "technical_inventory": self.technical_inventory,
            "configure_report": self.configure_report,
            "blocked": self.blocked,
            "artifacts": self.artifacts,
            "issues": [issue.to_dict() for issue in self.issues],
            "checkpoint": self.checkpoint.to_dict() if self.checkpoint else None,
        }


def _sort_issues(issues: List[ConfigureIssue]) -> List[ConfigureIssue]:
    return sorted(issues, key=lambda item: (item.level, item.code, item.path, item.message))


def _normalize_runtime_state(runtime_state: Optional[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    source = runtime_state if isinstance(runtime_state, dict) else {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for host_ref in list(source.keys()):
        key = str(host_ref).strip()
        if not key:
            continue
        host_state = source.get(host_ref)
        if not isinstance(host_state, dict):
            host_state = {}
            source[host_ref] = host_state

        if "nodes" not in host_state or not isinstance(host_state.get("nodes"), dict):
            host_state["nodes"] = {}
        if "configured_nodes" not in host_state or not isinstance(host_state.get("configured_nodes"), dict):
            host_state["configured_nodes"] = {}

        normalized[key] = host_state
    return normalized


def _runtime_nodes_by_host(runtime_inventory: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for host in runtime_inventory.get("hosts", []) or []:
        host_ref = str((host or {}).get("host_ref", "")).strip()
        if not host_ref:
            continue
        nodes = list((host or {}).get("nodes") or [])
        grouped[host_ref] = sorted(nodes, key=lambda node: str((node or {}).get("node_id", "")))
    return grouped


def _runtime_msp_tls_manifests_by_host(runtime_inventory: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for host in runtime_inventory.get("hosts", []) or []:
        host_ref = str((host or {}).get("host_ref", "")).strip()
        if not host_ref:
            continue
        manifests = [
            dict(item)
            for item in ((host or {}).get("msp_tls_manifests") or [])
            if isinstance(item, dict)
        ]
        grouped[host_ref] = sorted(
            manifests,
            key=lambda item: (
                str(item.get("org_id", "")),
                str(item.get("node_id", "")),
                str(item.get("node_type", "")),
            ),
        )
    return grouped


def _runtime_crypto_services_by_host(runtime_inventory: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for host in runtime_inventory.get("hosts", []) or []:
        host_ref = str((host or {}).get("host_ref", "")).strip()
        if not host_ref:
            continue
        services = [
            json.loads(json.dumps(item, ensure_ascii=False, sort_keys=True))
            for item in ((host or {}).get("crypto_services") or [])
            if isinstance(item, dict)
        ]
        grouped[host_ref] = sorted(
            services,
            key=lambda item: (
                str(item.get("org_id", "")),
                str(item.get("status", "")),
            ),
        )
    return grouped


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


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rotation_policy(stage: str) -> Dict[str, int]:
    stage_key = str(stage).strip().lower()
    policy = CRYPTO_ROTATION_POLICY_BY_STAGE.get(stage_key) or CRYPTO_ROTATION_POLICY_BY_STAGE["dev"]
    return {
        "default_validity_days": int(policy.get("default_validity_days", 365)),
        "renewal_window_days": int(policy.get("renewal_window_days", 30)),
        "critical_alert_days": int(policy.get("critical_alert_days", 7)),
        "transition_overlap_days": int(policy.get("transition_overlap_days", 14)),
    }


def _days_until_expiry(not_after: str, now: datetime) -> Optional[int]:
    parsed = _parse_utc_iso(not_after)
    if parsed is None:
        return None
    return int((parsed - now).total_seconds() // 86400)


def _cert_hash(payload: Dict[str, Any]) -> str:
    return payload_sha256(
        {
            "identity_id": payload.get("identity_id"),
            "serial": payload.get("serial"),
            "subject": payload.get("subject"),
            "issuer": payload.get("issuer"),
            "not_before": payload.get("not_before"),
            "not_after": payload.get("not_after"),
            "status": payload.get("status"),
            "algorithm": payload.get("algorithm"),
            "key_size": payload.get("key_size"),
            "issuance_version": payload.get("issuance_version"),
        }
    )


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y", "sim"}


def _revocation_signal(identity: Dict[str, Any]) -> Tuple[bool, bool, str]:
    status = str(identity.get("status", "")).strip().lower()
    compromise_detected = (
        status == "compromised"
        or _is_truthy(identity.get("compromise_reported"))
        or _is_truthy(identity.get("key_compromised"))
        or _is_truthy(identity.get("certificate_compromised"))
    )
    revocation_requested = (
        status in {"revoked", "compromised"}
        or _is_truthy(identity.get("revocation_requested"))
        or compromise_detected
    )
    reason = (
        str(identity.get("revocation_reason", "")).strip()
        or str(identity.get("compromise_reason", "")).strip()
        or ("key_or_certificate_compromise" if compromise_detected else "manual_revocation_request")
    )
    return revocation_requested, compromise_detected, reason


def _manifest_index_by_node(
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for host_ref in sorted(msp_tls_manifests_by_host.keys()):
        for manifest in msp_tls_manifests_by_host.get(host_ref, []):
            node_id = str((manifest or {}).get("node_id", "")).strip()
            if not node_id:
                continue
            index[node_id] = manifest
    return index


def _apply_crypto_validity_rotation_policy(
    *,
    run: PipelineRun,
    stage: str,
    changes: List[Dict[str, Any]],
    crypto_services_by_host: Dict[str, List[Dict[str, Any]]],
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
    connection_profiles: Dict[str, Any],
    issues: List[ConfigureIssue],
) -> Dict[str, Any]:
    policy = _rotation_policy(stage)
    now = _parse_utc_iso(str(getattr(run, "started_at", "")))
    if now is None:
        now = _parse_utc_iso(utc_now_iso()) or datetime.now(timezone.utc)

    high_risk_change_detected = bool(changes)
    identity_types_for_rotation = {"peer", "orderer", "client"}

    manifest_index = _manifest_index_by_node(msp_tls_manifests_by_host)
    rotation_events: List[Dict[str, Any]] = []
    ca_rotation_events: List[Dict[str, Any]] = []
    manifest_impacts: List[Dict[str, Any]] = []
    profile_impacts: List[Dict[str, Any]] = []
    critical_expiry_blocks: List[Dict[str, Any]] = []
    renewal_warnings: List[Dict[str, Any]] = []

    for host_ref in sorted(crypto_services_by_host.keys()):
        for service in crypto_services_by_host.get(host_ref, []):
            org_id = str(service.get("org_id", "")).strip().lower()
            if not org_id:
                continue

            identities = [item for item in (service.get("issued_identities") or []) if isinstance(item, dict)]
            updated_identities: List[Dict[str, Any]] = []
            org_has_rotation = False

            for identity in identities:
                identity_type = str(identity.get("identity_type", "")).strip().lower()
                status = str(identity.get("status", "")).strip().lower()
                if identity_type not in identity_types_for_rotation or status != "active":
                    updated_identities.append(identity)
                    continue

                days_to_expiry = _days_until_expiry(str(identity.get("not_after", "")), now)
                if days_to_expiry is None:
                    updated_identities.append(identity)
                    continue

                if days_to_expiry <= policy["critical_alert_days"] and high_risk_change_detected:
                    critical_expiry_blocks.append(
                        {
                            "org_id": org_id,
                            "host_ref": host_ref,
                            "identity_id": str(identity.get("identity_id", "")).strip(),
                            "node_id": str(identity.get("node_id", "")).strip(),
                            "days_to_expiry": days_to_expiry,
                        }
                    )
                    issues.append(
                        ConfigureIssue(
                            level="error",
                            code="configure_crypto_critical_expiry_block",
                            path=f"runtime_inventory.hosts.{host_ref}.crypto_services.{org_id}.issued_identities",
                            message=(
                                f"Certificado crítico próximo ao vencimento ({days_to_expiry} dias) bloqueia mudança de risco alto."
                            ),
                        )
                    )

                if days_to_expiry > policy["renewal_window_days"]:
                    updated_identities.append(identity)
                    continue

                renewal_warnings.append(
                    {
                        "org_id": org_id,
                        "host_ref": host_ref,
                        "identity_id": str(identity.get("identity_id", "")).strip(),
                        "days_to_expiry": days_to_expiry,
                    }
                )

                old_payload = dict(identity)
                old_payload["status"] = "superseded"
                old_payload["superseded_at"] = _iso_utc(now)

                old_not_before = _parse_utc_iso(str(identity.get("not_before", "")))
                old_not_after = _parse_utc_iso(str(identity.get("not_after", "")))
                if old_not_before is not None and old_not_after is not None and old_not_after > old_not_before:
                    validity_days = int((old_not_after - old_not_before).total_seconds() // 86400)
                    validity_days = max(validity_days, 1)
                else:
                    validity_days = policy["default_validity_days"]

                new_not_before = _iso_utc(now)
                new_not_after = _iso_utc(now + timedelta(days=validity_days))
                previous_version = (
                    int(identity.get("issuance_version", 0))
                    if str(identity.get("issuance_version", "")).isdigit()
                    else 0
                )
                new_version = previous_version + 1 if previous_version > 0 else 1

                identity_id = str(identity.get("identity_id", "")).strip().lower()
                new_serial = payload_sha256(
                    {
                        "identity_id": identity_id,
                        "old_serial": str(identity.get("serial", "")).strip(),
                        "new_version": new_version,
                        "run_id": run.run_id,
                    }
                )[:32]
                new_payload = {
                    **dict(identity),
                    "status": "active",
                    "serial": new_serial,
                    "not_before": new_not_before,
                    "not_after": new_not_after,
                    "issuance_version": new_version,
                    "enrollment_type": "rotation",
                    "parent_serial": str(identity.get("serial", "")).strip(),
                    "rotation_recorded_at": _iso_utc(now),
                }

                transition_overlap_until = _iso_utc(
                    now + timedelta(days=max(policy["transition_overlap_days"], 1))
                )
                transition_plan = {
                    "mode": "overlap",
                    "overlap_until": transition_overlap_until,
                    "old_serial": str(identity.get("serial", "")).strip(),
                    "new_serial": new_serial,
                }

                event_id = payload_sha256(
                    {
                        "org_id": org_id,
                        "identity_id": identity_id,
                        "old_serial": str(identity.get("serial", "")).strip(),
                        "new_serial": new_serial,
                        "run_id": run.run_id,
                    }
                )[:24]
                event = {
                    "event_id": event_id,
                    "event_type": "certificate_rotation",
                    "org_id": org_id,
                    "host_ref": host_ref,
                    "identity_id": identity_id,
                    "identity_type": identity_type,
                    "node_id": str(identity.get("node_id", "")).strip(),
                    "recorded_at": _iso_utc(now),
                    "pre_hash": _cert_hash(identity),
                    "post_hash": _cert_hash(new_payload),
                    "transition_plan": transition_plan,
                }
                rotation_events.append(event)

                node_id = str(identity.get("node_id", "")).strip()
                if node_id:
                    manifest = manifest_index.get(node_id)
                    if isinstance(manifest, dict):
                        old_manifest_hash = str(manifest.get("manifest_hash", "")).strip()
                        manifest_events = [
                            item for item in (manifest.get("rotation_events") or []) if isinstance(item, dict)
                        ]
                        manifest_events.append(
                            {
                                "event_id": event_id,
                                "identity_id": identity_id,
                                "pre_hash": event["pre_hash"],
                                "post_hash": event["post_hash"],
                                "recorded_at": event["recorded_at"],
                                "transition_plan": transition_plan,
                            }
                        )
                        manifest["rotation_events"] = sorted(
                            manifest_events,
                            key=lambda item: (
                                str(item.get("event_id", "")),
                                str(item.get("identity_id", "")),
                            ),
                        )
                        payload_for_hash = {
                            key: value
                            for key, value in manifest.items()
                            if key != "manifest_hash"
                        }
                        manifest["manifest_hash"] = payload_sha256(payload_for_hash)
                        manifest_impacts.append(
                            {
                                "event_id": event_id,
                                "node_id": node_id,
                                "host_ref": host_ref,
                                "pre_manifest_hash": old_manifest_hash,
                                "post_manifest_hash": manifest.get("manifest_hash", ""),
                            }
                        )

                profile_impacts.append(
                    {
                        "event_id": event_id,
                        "org_id": org_id,
                        "node_id": node_id,
                        "identity_type": identity_type,
                        "recorded_at": event["recorded_at"],
                    }
                )

                updated_identities.append(old_payload)
                updated_identities.append(new_payload)
                org_has_rotation = True

            if org_has_rotation:
                for ca_kind in ("ca", "tls_ca"):
                    ca_payload = service.get(ca_kind) if isinstance(service.get(ca_kind), dict) else {}
                    if not ca_payload:
                        continue
                    root_subject = str(((ca_payload.get("root_certificate") or {}).get("subject") or "")).strip()
                    current_intermediate = dict(ca_payload.get("intermediate_certificate") or {})
                    intermediate_subject = str(current_intermediate.get("subject", "")).strip()
                    if not (root_subject and intermediate_subject):
                        issues.append(
                            ConfigureIssue(
                                level="error",
                                code="configure_crypto_rotation_transition_invalid",
                                path=f"runtime_inventory.hosts.{host_ref}.crypto_services.{org_id}.{ca_kind}",
                                message="Rotação de CA intermediária inválida: cadeia ativa sem plano de transição seguro.",
                            )
                        )
                        continue

                    new_intermediate_subject = f"{intermediate_subject}.r{run.run_id[:8]}"
                    pre_hash = payload_sha256(current_intermediate)
                    post_payload = {
                        "subject": new_intermediate_subject,
                        "issuer": root_subject,
                        "rotated_at": _iso_utc(now),
                    }
                    post_hash = payload_sha256(post_payload)
                    ca_payload["intermediate_certificate"] = post_payload
                    ca_rotation_events.append(
                        {
                            "event_id": payload_sha256(
                                {
                                    "org_id": org_id,
                                    "ca_kind": ca_kind,
                                    "pre_hash": pre_hash,
                                    "post_hash": post_hash,
                                    "run_id": run.run_id,
                                }
                            )[:24],
                            "event_type": "ca_intermediate_rotation",
                            "org_id": org_id,
                            "host_ref": host_ref,
                            "ca_kind": ca_kind,
                            "recorded_at": _iso_utc(now),
                            "pre_hash": pre_hash,
                            "post_hash": post_hash,
                            "transition_plan": {
                                "mode": "overlap",
                                "overlap_until": _iso_utc(
                                    now + timedelta(days=max(policy["transition_overlap_days"], 1))
                                ),
                                "previous_subject": intermediate_subject,
                                "new_subject": new_intermediate_subject,
                            },
                        }
                    )

            service["issued_identities"] = sorted(
                updated_identities,
                key=lambda item: (
                    str(item.get("identity_id", "")),
                    str(item.get("status", "")),
                    int(item.get("issuance_version", 0)) if str(item.get("issuance_version", "")).isdigit() else 0,
                ),
            )

    if renewal_warnings:
        issues.append(
            ConfigureIssue(
                level="warning",
                code="configure_crypto_rotation_recommended",
                path="runtime_inventory.hosts[].crypto_services[].issued_identities",
                message="Certificados em janela de renovação detectados; rotação controlada registrada.",
            )
        )

    rotation_report = {
        "stage_profile": str(stage).strip().lower(),
        "policy": policy,
        "high_risk_change_detected": high_risk_change_detected,
        "critical_expiry_blocks": sorted(
            critical_expiry_blocks,
            key=lambda item: (item.get("org_id", ""), item.get("identity_id", ""), int(item.get("days_to_expiry", 0))),
        ),
        "rotation_events": sorted(
            rotation_events,
            key=lambda item: (item.get("org_id", ""), item.get("identity_id", ""), item.get("event_id", "")),
        ),
        "ca_rotation_events": sorted(
            ca_rotation_events,
            key=lambda item: (item.get("org_id", ""), item.get("ca_kind", ""), item.get("event_id", "")),
        ),
        "manifest_impacts": sorted(
            manifest_impacts,
            key=lambda item: (item.get("node_id", ""), item.get("event_id", "")),
        ),
        "profile_impacts": sorted(
            profile_impacts,
            key=lambda item: (item.get("org_id", ""), item.get("node_id", ""), item.get("event_id", "")),
        ),
        "recorded_at": _iso_utc(now),
    }

    connection_profiles["crypto_rotation_impacts"] = {
        "rotation_event_count": len(rotation_report["rotation_events"]),
        "ca_rotation_event_count": len(rotation_report["ca_rotation_events"]),
        "impacts": rotation_report["profile_impacts"],
    }

    return rotation_report


def _apply_crypto_revocation_policy(
    *,
    run: PipelineRun,
    stage: str,
    crypto_services_by_host: Dict[str, List[Dict[str, Any]]],
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
    connection_profiles: Dict[str, Any],
    issues: List[ConfigureIssue],
) -> Dict[str, Any]:
    now = _parse_utc_iso(str(getattr(run, "started_at", "")))
    if now is None:
        now = _parse_utc_iso(utc_now_iso()) or datetime.now(timezone.utc)

    manifests_by_org: Dict[str, List[Dict[str, Any]]] = {}
    for host_ref in sorted(msp_tls_manifests_by_host.keys()):
        for manifest in msp_tls_manifests_by_host.get(host_ref, []):
            org_id = str((manifest or {}).get("org_id", "")).strip().lower()
            node_id = str((manifest or {}).get("node_id", "")).strip()
            if not (org_id and node_id):
                continue
            manifests_by_org.setdefault(org_id, []).append(manifest)

    revocation_events: List[Dict[str, Any]] = []
    org_crls: List[Dict[str, Any]] = []
    manifest_impacts: List[Dict[str, Any]] = []
    gateway_impacts: List[Dict[str, Any]] = []
    incident_response: List[Dict[str, Any]] = []
    propagation_failures: List[Dict[str, Any]] = []
    decision_trace: List[Dict[str, Any]] = []

    for host_ref in sorted(crypto_services_by_host.keys()):
        for service in crypto_services_by_host.get(host_ref, []):
            org_id = str(service.get("org_id", "")).strip().lower()
            if not org_id:
                continue

            simulate_crl_failure = _is_truthy(service.get("simulate_crl_propagation_failure"))
            identities = [item for item in (service.get("issued_identities") or []) if isinstance(item, dict)]
            updated_identities: List[Dict[str, Any]] = []
            org_events: List[Dict[str, Any]] = []

            for identity in identities:
                revocation_requested, compromise_detected, reason = _revocation_signal(identity)
                if not revocation_requested:
                    updated_identities.append(identity)
                    continue

                identity_id = str(identity.get("identity_id", "")).strip().lower()
                serial = str(identity.get("serial", "")).strip().lower()
                if not identity_id:
                    updated_identities.append(identity)
                    continue

                revocation_id = payload_sha256(
                    {
                        "org_id": org_id,
                        "identity_id": identity_id,
                        "serial": serial,
                        "reason": reason,
                        "run_id": run.run_id,
                    }
                )[:24]
                revoked_payload = {
                    **dict(identity),
                    "status": "revoked",
                    "revocation_id": revocation_id,
                    "revoked_at": _iso_utc(now),
                    "revocation_reason": reason,
                    "revocation_scope": "certificate",
                    "compromise_detected": compromise_detected,
                    "eligible_for_validation": False,
                }
                updated_identities.append(revoked_payload)

                event = {
                    "event_id": revocation_id,
                    "event_type": "certificate_revocation",
                    "org_id": org_id,
                    "host_ref": host_ref,
                    "identity_id": identity_id,
                    "node_id": str(identity.get("node_id", "")).strip(),
                    "serial": serial,
                    "compromise_detected": compromise_detected,
                    "technical_reason": reason,
                    "recorded_at": _iso_utc(now),
                    "pre_hash": _cert_hash(identity),
                    "post_hash": _cert_hash(revoked_payload),
                }
                org_events.append(event)
                revocation_events.append(event)

                incident_response.append(
                    {
                        "incident_id": payload_sha256(
                            {
                                "event_id": revocation_id,
                                "org_id": org_id,
                                "identity_id": identity_id,
                                "run_id": run.run_id,
                            }
                        )[:24],
                        "org_id": org_id,
                        "identity_id": identity_id,
                        "response_type": "crypto_compromise_containment",
                        "actions": [
                            "mark_certificate_revoked",
                            "publish_org_crl",
                            "propagate_crl_to_msp_and_gateway",
                            "require_identity_reissuance",
                        ],
                        "recorded_at": _iso_utc(now),
                        "evidence_hash": payload_sha256(event),
                    }
                )

            service["issued_identities"] = sorted(
                updated_identities,
                key=lambda item: (
                    str(item.get("identity_id", "")),
                    str(item.get("status", "")),
                    int(item.get("issuance_version", 0)) if str(item.get("issuance_version", "")).isdigit() else 0,
                ),
            )

            if not org_events:
                continue

            revoked_serials = sorted({str(item.get("serial", "")).strip().lower() for item in org_events if str(item.get("serial", "")).strip()})
            issuance_version_by_identity = {
                str(identity.get("identity_id", "")).strip().lower(): (
                    int(identity.get("issuance_version", 0))
                    if str(identity.get("issuance_version", "")).isdigit()
                    else 0
                )
                for identity in updated_identities
                if str(identity.get("identity_id", "")).strip()
            }
            revoked_certificate_ids = sorted(
                {
                    f"cert:{str(item.get('identity_id', '')).strip().lower()}:v"
                    f"{issuance_version_by_identity.get(str(item.get('identity_id', '')).strip().lower(), 0)}"
                    for item in org_events
                    if str(item.get("identity_id", "")).strip()
                }
            )
            crl_payload = {
                "org_id": org_id,
                "stage": str(stage).strip().lower(),
                "run_id": run.run_id,
                "revocation_event_ids": sorted(str(item.get("event_id", "")).strip() for item in org_events),
                "revoked_serials": revoked_serials,
                "issued_at": _iso_utc(now),
            }
            crl_hash = payload_sha256(crl_payload)
            crl_id = f"crl:{org_id}:{crl_hash[:16]}"
            org_crls.append(
                {
                    "org_id": org_id,
                    "crl_id": crl_id,
                    "crl_hash": crl_hash,
                    "issued_at": _iso_utc(now),
                    "event_count": len(org_events),
                    "revoked_serials": revoked_serials,
                    "revoked_certificate_ids": revoked_certificate_ids,
                }
            )

            org_manifests = manifests_by_org.get(org_id, [])
            propagated_nodes: Set[str] = set()
            if not simulate_crl_failure:
                for manifest in org_manifests:
                    node_id = str(manifest.get("node_id", "")).strip()
                    if not node_id:
                        continue
                    pre_manifest_hash = str(manifest.get("manifest_hash", "")).strip()
                    manifest["revocation"] = {
                        "org_id": org_id,
                        "crl_id": crl_id,
                        "crl_hash": crl_hash,
                        "revoked_serials": revoked_serials,
                        "issued_at": _iso_utc(now),
                        "source": "configure.crypto_revocation",
                    }
                    manifest["trust_revocation_hashes"] = sorted(
                        {
                            *{
                                str(item).strip()
                                for item in (manifest.get("trust_revocation_hashes") or [])
                                if str(item).strip()
                            },
                            crl_hash,
                        }
                    )
                    payload_for_hash = {key: value for key, value in manifest.items() if key != "manifest_hash"}
                    manifest["manifest_hash"] = payload_sha256(payload_for_hash)
                    manifest_impacts.append(
                        {
                            "org_id": org_id,
                            "node_id": node_id,
                            "host_ref": str(manifest.get("host_ref", "")).strip() or host_ref,
                            "crl_id": crl_id,
                            "pre_manifest_hash": pre_manifest_hash,
                            "post_manifest_hash": manifest.get("manifest_hash", ""),
                        }
                    )
                    propagated_nodes.add(node_id)

            profiles = connection_profiles.get("profiles") if isinstance(connection_profiles.get("profiles"), dict) else {}
            profile = profiles.get(org_id) if isinstance(profiles, dict) else None
            if isinstance(profile, dict):
                profile["revocation"] = {
                    "org_id": org_id,
                    "crl_id": crl_id,
                    "crl_hash": crl_hash,
                    "revoked_serials": revoked_serials,
                    "recorded_at": _iso_utc(now),
                }
                gateway_impacts.append(
                    {
                        "org_id": org_id,
                        "profile_ref": f"connection_profiles.profiles.{org_id}",
                        "crl_id": crl_id,
                        "crl_hash": crl_hash,
                        "recorded_at": _iso_utc(now),
                    }
                )

            failure_reasons: List[str] = []
            if not org_manifests:
                failure_reasons.append("missing_org_manifests_for_crl")
            if simulate_crl_failure:
                failure_reasons.append("simulated_crl_propagation_failure")
            elif len(propagated_nodes) < len({str((item or {}).get("node_id", "")).strip() for item in org_manifests if str((item or {}).get("node_id", "")).strip()}):
                failure_reasons.append("incomplete_crl_propagation")

            decision = "blocked" if failure_reasons else "allowed"
            decision_payload = {
                "org_id": org_id,
                "decision": decision,
                "failure_reasons": sorted(failure_reasons),
                "crl_hash": crl_hash,
                "event_count": len(org_events),
                "propagated_nodes": sorted(propagated_nodes),
            }
            decision_trace.append(
                {
                    "decision_id": payload_sha256(
                        {
                            "org_id": org_id,
                            "decision": decision,
                            "run_id": run.run_id,
                            "crl_hash": crl_hash,
                        }
                    )[:24],
                    "rule_code": "revocation_crl_propagation_guard",
                    "decision": decision,
                    "org_id": org_id,
                    "technical_reason": (
                        "CRL propagada para dependências críticas." if not failure_reasons
                        else f"Falha de propagação de CRL: {', '.join(sorted(failure_reasons))}."
                    ),
                    "evidence_hash": payload_sha256(decision_payload),
                    "recorded_at": _iso_utc(now),
                }
            )

            if failure_reasons:
                propagation_failures.append(
                    {
                        "org_id": org_id,
                        "crl_id": crl_id,
                        "failure_reasons": sorted(failure_reasons),
                        "recorded_at": _iso_utc(now),
                    }
                )
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_crypto_crl_propagation_failed",
                        path=f"runtime_inventory.hosts.{host_ref}.crypto_services.{org_id}",
                        message=(
                            "Falha de propagação de CRL para componentes dependentes; etapa sensível bloqueada."
                        ),
                    )
                )

    if revocation_events:
        issues.append(
            ConfigureIssue(
                level="warning",
                code="configure_crypto_revocation_registered",
                path="runtime_inventory.hosts[].crypto_services[].issued_identities",
                message="Eventos de revogação registrados com emissão de CRL por organização.",
            )
        )

    if not decision_trace:
        default_payload = {
            "rule_code": "revocation_crl_propagation_guard",
            "decision": "allowed",
            "technical_reason": "Nenhum evento de revogação detectado para o run.",
            "run_id": run.run_id,
        }
        decision_trace.append(
            {
                "decision_id": payload_sha256(default_payload)[:24],
                "rule_code": "revocation_crl_propagation_guard",
                "decision": "allowed",
                "org_id": "*",
                "technical_reason": "Nenhum evento de revogação detectado para o run.",
                "evidence_hash": payload_sha256(default_payload),
                "recorded_at": _iso_utc(now),
            }
        )

    connection_profiles["crypto_revocation_impacts"] = {
        "revocation_event_count": len(revocation_events),
        "org_crl_count": len(org_crls),
        "gateway_impacts": sorted(
            gateway_impacts,
            key=lambda item: (item.get("org_id", ""), item.get("crl_id", "")),
        ),
    }

    return {
        "stage_profile": str(stage).strip().lower(),
        "revocation_events": sorted(
            revocation_events,
            key=lambda item: (item.get("org_id", ""), item.get("identity_id", ""), item.get("event_id", "")),
        ),
        "org_crls": sorted(org_crls, key=lambda item: (item.get("org_id", ""), item.get("crl_id", ""))),
        "manifest_impacts": sorted(
            manifest_impacts,
            key=lambda item: (item.get("org_id", ""), item.get("node_id", ""), item.get("crl_id", "")),
        ),
        "gateway_impacts": sorted(
            gateway_impacts,
            key=lambda item: (item.get("org_id", ""), item.get("crl_id", "")),
        ),
        "incident_response": sorted(
            incident_response,
            key=lambda item: (item.get("org_id", ""), item.get("identity_id", ""), item.get("incident_id", "")),
        ),
        "propagation_failures": sorted(
            propagation_failures,
            key=lambda item: (item.get("org_id", ""), item.get("crl_id", "")),
        ),
        "decision_trace": sorted(
            decision_trace,
            key=lambda item: (item.get("org_id", ""), item.get("decision", ""), item.get("decision_id", "")),
        ),
        "recorded_at": _iso_utc(now),
    }


def _build_org_index(blueprint_validation: BlueprintValidationResult) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for org in blueprint_validation.normalized_orgs:
        org_id = str(org.get("org_id", "")).strip()
        if org_id:
            index[org_id] = org
    return index


def _build_connection_profiles(
    *,
    org_index: Dict[str, Dict[str, Any]],
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    by_org_nodes: Dict[str, List[Dict[str, Any]]] = {}
    for host_ref, nodes in nodes_by_host.items():
        for node in nodes:
            org_id = str((node or {}).get("org_id", "")).strip()
            if not org_id:
                continue
            ports = sorted(int(port) for port in ((node or {}).get("ports") or []) if str(port).isdigit())
            endpoint = f"{host_ref}:{ports[0]}" if ports else host_ref
            by_org_nodes.setdefault(org_id, []).append(
                {
                    "node_id": node.get("node_id"),
                    "node_type": node.get("node_type"),
                    "host_ref": host_ref,
                    "endpoint": endpoint,
                    "ports": ports,
                }
            )

    profiles: Dict[str, Any] = {}
    for org_id in sorted(org_index.keys()):
        org = org_index[org_id]
        nodes = sorted(by_org_nodes.get(org_id, []), key=lambda node: str(node.get("node_id", "")))
        profile = {
            "org_id": org_id,
            "msp_id": org.get("msp_id"),
            "domain": org.get("domain"),
            "peers": [node for node in nodes if node.get("node_type") == "peer"],
            "orderers": [node for node in nodes if node.get("node_type") == "orderer"],
            "cas": [node for node in nodes if node.get("node_type") == "ca"],
            "generated_at": utc_now_iso(),
        }
        profiles[org_id] = profile
    return profiles


def _build_network_manifests(
    *,
    blueprint_validation: BlueprintValidationResult,
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
    crypto_inventory: Dict[str, Any],
    rotation_report: Dict[str, Any],
    revocation_report: Dict[str, Any],
) -> Dict[str, Any]:
    host_manifests: List[Dict[str, Any]] = []
    for host_ref in sorted(nodes_by_host.keys()):
        host_nodes = nodes_by_host[host_ref]
        host_manifests.append(
            {
                "host_ref": host_ref,
                "component_count": len(host_nodes),
                "components": [
                    {
                        "node_id": node.get("node_id"),
                        "org_id": node.get("org_id"),
                        "node_type": node.get("node_type"),
                        "ports": sorted(int(port) for port in ((node or {}).get("ports") or []) if str(port).isdigit()),
                    }
                    for node in host_nodes
                ],
            }
        )

    return {
        "run_id": blueprint_validation.fingerprint_sha256,
        "channels": sorted(
            [
                {
                    "channel_id": channel.get("channel_id"),
                    "members": sorted(channel.get("members") or []),
                    "type": channel.get("type"),
                }
                for channel in blueprint_validation.normalized_channels
            ],
            key=lambda channel: str(channel.get("channel_id", "")),
        ),
        "hosts": host_manifests,
        "crypto_manifests": [
            {
                "host_ref": host_ref,
                "entries": [
                    {
                        "org_id": item.get("org_id"),
                        "node_id": item.get("node_id"),
                        "node_type": item.get("node_type"),
                        "endpoint": item.get("endpoint"),
                        "manifest_hash": item.get("manifest_hash"),
                        "trust_bundles": sorted(item.get("trust_bundles") or []),
                        "trust_revocation_hashes": sorted(item.get("trust_revocation_hashes") or []),
                        "revocation": item.get("revocation", {}),
                        "validation": item.get("validation", {}),
                    }
                    for item in msp_tls_manifests_by_host.get(host_ref, [])
                ],
            }
            for host_ref in sorted(msp_tls_manifests_by_host.keys())
        ],
        "crypto_inventory_ref": {
            "contract_version": crypto_inventory.get("contract_version"),
            "inventory_fingerprint": crypto_inventory.get("inventory_fingerprint"),
            "entity_counts": dict((crypto_inventory.get("summary") or {}).get("entity_counts") or {}),
        },
        "crypto_rotation_impacts": {
            "rotation_event_count": len((rotation_report.get("rotation_events") or [])),
            "ca_rotation_event_count": len((rotation_report.get("ca_rotation_events") or [])),
            "manifest_impacts": [
                item
                for item in (rotation_report.get("manifest_impacts") or [])
                if isinstance(item, dict)
            ],
            "recorded_at": rotation_report.get("recorded_at", ""),
        },
        "crypto_revocation_impacts": {
            "revocation_event_count": len((revocation_report.get("revocation_events") or [])),
            "org_crl_count": len((revocation_report.get("org_crls") or [])),
            "org_crls": [
                item
                for item in (revocation_report.get("org_crls") or [])
                if isinstance(item, dict)
            ],
            "manifest_impacts": [
                item
                for item in (revocation_report.get("manifest_impacts") or [])
                if isinstance(item, dict)
            ],
            "decision_trace": [
                item
                for item in (revocation_report.get("decision_trace") or [])
                if isinstance(item, dict)
            ],
            "recorded_at": revocation_report.get("recorded_at", ""),
        },
        "generated_at": utc_now_iso(),
    }


def _upsert_entity(entity_index: Dict[str, Dict[str, Any]], entity: Dict[str, Any]) -> None:
    stable_id = str(entity.get("stable_id", "")).strip()
    if not stable_id:
        return
    if stable_id not in entity_index:
        entity_index[stable_id] = entity


def _build_crypto_inventory(
    *,
    run: PipelineRun,
    blueprint_validation: BlueprintValidationResult,
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
    crypto_services_by_host: Dict[str, List[Dict[str, Any]]],
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    cas_index: Dict[str, Dict[str, Any]] = {}
    certificates_index: Dict[str, Dict[str, Any]] = {}
    keys_index: Dict[str, Dict[str, Any]] = {}
    bundles_index: Dict[str, Dict[str, Any]] = {}

    org_links_index: Dict[str, Dict[str, Any]] = {}
    node_links_index: Dict[str, Dict[str, Any]] = {}
    channel_links_index: Dict[str, Dict[str, Any]] = {}

    node_ids_by_org: Dict[str, Set[str]] = {}
    for host_ref in sorted(nodes_by_host.keys()):
        for node in nodes_by_host[host_ref]:
            org_id = str((node or {}).get("org_id", "")).strip().lower()
            node_id = str((node or {}).get("node_id", "")).strip()
            if org_id and node_id:
                node_ids_by_org.setdefault(org_id, set()).add(node_id)

    for host_ref in sorted(crypto_services_by_host.keys()):
        for service in crypto_services_by_host.get(host_ref, []):
            org_id = str(service.get("org_id", "")).strip().lower()
            if not org_id:
                continue
            org_link_id = f"org-link:{org_id}"
            org_links_index[org_link_id] = {
                "stable_id": org_link_id,
                "org_id": org_id,
                "host_refs": sorted(
                    {
                        *set(org_links_index.get(org_link_id, {}).get("host_refs") or []),
                        host_ref,
                    }
                ),
                "node_ids": sorted(node_ids_by_org.get(org_id, set())),
                "ca_ids": sorted(
                    {
                        item["stable_id"]
                        for item in cas_index.values()
                        if str(item.get("org_id", "")).strip().lower() == org_id
                    }
                ),
            }

            for ca_kind in ("ca", "tls_ca"):
                ca_payload = service.get(ca_kind) if isinstance(service.get(ca_kind), dict) else {}
                if not ca_payload:
                    continue
                stable_id = f"ca:{org_id}:{ca_kind}"
                fingerprint = payload_sha256(
                    {
                        "stable_id": stable_id,
                        "profile": ca_payload.get("profile"),
                        "algorithm": ca_payload.get("algorithm"),
                        "key_size": ca_payload.get("key_size"),
                        "validity_days": ca_payload.get("validity_days"),
                        "rotation_days": ca_payload.get("rotation_days"),
                        "root_subject": ((ca_payload.get("root_certificate") or {}).get("subject")),
                        "intermediate_subject": ((ca_payload.get("intermediate_certificate") or {}).get("subject")),
                    }
                )
                _upsert_entity(
                    cas_index,
                    {
                        "stable_id": stable_id,
                        "fingerprint": fingerprint,
                        "org_id": org_id,
                        "host_ref": host_ref,
                        "ca_type": "signing" if ca_kind == "ca" else "tls",
                        "status": str(service.get("status", "initialized")).strip().lower() or "initialized",
                        "algorithm": ca_payload.get("algorithm"),
                        "key_size": ca_payload.get("key_size"),
                        "validity_days": ca_payload.get("validity_days"),
                        "rotation_days": ca_payload.get("rotation_days"),
                        "origin": {
                            "profile": ca_payload.get("profile"),
                            "process_name": ca_payload.get("process_name"),
                            "storage_path": ca_payload.get("storage_path"),
                            "credential_ref": ca_payload.get("credential_ref"),
                        },
                        "certificates": {
                            "root": dict(ca_payload.get("root_certificate") or {}),
                            "intermediate": dict(ca_payload.get("intermediate_certificate") or {}),
                        },
                    },
                )

            identities = [item for item in (service.get("issued_identities") or []) if isinstance(item, dict)]
            for identity in identities:
                identity_id = str(identity.get("identity_id", "")).strip().lower()
                serial = str(identity.get("serial", "")).strip().lower()
                issuance_version = int(identity.get("issuance_version", 0)) if str(identity.get("issuance_version", "")).isdigit() else 0
                if not identity_id:
                    continue
                stable_id = f"cert:{identity_id}:v{issuance_version}"
                fingerprint = payload_sha256(
                    {
                        "stable_id": stable_id,
                        "serial": serial,
                        "subject": identity.get("subject"),
                        "issuer": identity.get("issuer"),
                        "not_before": identity.get("not_before"),
                        "not_after": identity.get("not_after"),
                        "algorithm": identity.get("algorithm"),
                        "key_size": identity.get("key_size"),
                    }
                )
                status = str(identity.get("status", "")).strip().lower()
                revoked = status in {"revoked", "compromised"}
                _upsert_entity(
                    certificates_index,
                    {
                        "stable_id": stable_id,
                        "fingerprint": fingerprint,
                        "org_id": org_id,
                        "node_id": str(identity.get("node_id", "")).strip(),
                        "identity_type": identity.get("identity_type"),
                        "status": status,
                        "revoked": revoked,
                        "eligible_for_validation": (status == "active") and not revoked,
                        "revoked_at": identity.get("revoked_at"),
                        "revocation_reason": identity.get("revocation_reason"),
                        "subject": identity.get("subject"),
                        "issuer": identity.get("issuer"),
                        "serial": serial,
                        "not_before": identity.get("not_before"),
                        "not_after": identity.get("not_after"),
                        "algorithm": identity.get("algorithm"),
                        "key_size": identity.get("key_size"),
                        "origin": {
                            "issuance_source": "provision.ca.enrollment",
                            "ca_stable_id": f"ca:{org_id}:ca",
                        },
                    },
                )

    for host_ref in sorted(msp_tls_manifests_by_host.keys()):
        for manifest in msp_tls_manifests_by_host.get(host_ref, []):
            org_id = str(manifest.get("org_id", "")).strip().lower()
            node_id = str(manifest.get("node_id", "")).strip()
            node_type = str(manifest.get("node_type", "")).strip().lower()
            if not (org_id and node_id):
                continue

            manifests_hashes = sorted(
                {
                    str(manifest.get("manifest_hash", "")).strip()
                }
                - {""}
            )
            artifacts = [item for item in (manifest.get("artifacts") or []) if isinstance(item, dict)]

            node_key_ids: Set[str] = set()
            node_bundle_ids: Set[str] = set()
            node_cert_ids: Set[str] = set(
                item["stable_id"]
                for item in certificates_index.values()
                if str(item.get("org_id", "")).strip().lower() == org_id
                and str(item.get("node_id", "")).strip() == node_id
            )
            node_eligible_cert_ids: Set[str] = set(
                item["stable_id"]
                for item in certificates_index.values()
                if str(item.get("org_id", "")).strip().lower() == org_id
                and str(item.get("node_id", "")).strip() == node_id
                and bool(item.get("eligible_for_validation", False))
            )
            node_revoked_cert_ids: Set[str] = set(
                item["stable_id"]
                for item in certificates_index.values()
                if str(item.get("org_id", "")).strip().lower() == org_id
                and str(item.get("node_id", "")).strip() == node_id
                and bool(item.get("revoked", False))
            )

            for artifact in artifacts:
                path = str(artifact.get("path", "")).strip()
                kind = str(artifact.get("kind", "")).strip().lower()
                content_hash = str(artifact.get("content_hash", "")).strip()
                if not (path and kind and content_hash):
                    continue

                if "key" in kind:
                    key_id = f"key:{org_id}:{node_id}:{path}".lower()
                    node_key_ids.add(key_id)
                    _upsert_entity(
                        keys_index,
                        {
                            "stable_id": key_id,
                            "fingerprint": payload_sha256(
                                {
                                    "stable_id": key_id,
                                    "path": path,
                                    "content_hash": content_hash,
                                    "kind": kind,
                                }
                            ),
                            "org_id": org_id,
                            "node_id": node_id,
                            "node_type": node_type,
                            "status": "active",
                            "path": path,
                            "algorithm": "ecdsa",
                            "origin": {
                                "source": "provision.msp_tls.artifact",
                                "artifact_kind": kind,
                                "artifact_hash": content_hash,
                            },
                        },
                    )

            for bundle_hash in sorted(
                {
                    str(item).strip()
                    for item in (manifest.get("trust_bundles") or [])
                    if str(item).strip()
                }
            ):
                bundle_id = f"bundle:{org_id}:{node_id}:{bundle_hash[:16]}".lower()
                node_bundle_ids.add(bundle_id)
                _upsert_entity(
                    bundles_index,
                    {
                        "stable_id": bundle_id,
                        "fingerprint": bundle_hash,
                        "org_id": org_id,
                        "node_id": node_id,
                        "node_type": node_type,
                        "status": "active",
                        "origin": {
                            "source": "provision.msp_tls.manifest",
                            "manifest_hash": manifests_hashes[0] if manifests_hashes else "",
                        },
                    },
                )

            node_link_id = f"node-link:{org_id}:{node_id}".lower()
            node_links_index[node_link_id] = {
                "stable_id": node_link_id,
                "org_id": org_id,
                "node_id": node_id,
                "node_type": node_type,
                "host_ref": host_ref,
                "manifest_hashes": manifests_hashes,
                "ca_ids": sorted(
                    {
                        f"ca:{org_id}:ca",
                        f"ca:{org_id}:tls_ca",
                    }
                ),
                "certificate_ids": sorted(node_cert_ids),
                "eligible_certificate_ids": sorted(node_eligible_cert_ids),
                "revoked_certificate_ids": sorted(node_revoked_cert_ids),
                "key_ids": sorted(node_key_ids),
                "bundle_ids": sorted(node_bundle_ids),
            }

    for channel in sorted(
        blueprint_validation.normalized_channels,
        key=lambda item: str((item or {}).get("channel_id", "")),
    ):
        channel_id = str((channel or {}).get("channel_id", "")).strip().lower()
        if not channel_id:
            continue
        members = sorted(
            {
                str(item).strip().lower()
                for item in ((channel or {}).get("members") or [])
                if str(item).strip()
            }
        )
        for org_id in members:
            link_id = f"channel-link:{channel_id}:{org_id}"
            channel_links_index[link_id] = {
                "stable_id": link_id,
                "channel_id": channel_id,
                "org_id": org_id,
                "node_ids": sorted(node_ids_by_org.get(org_id, set())),
                "status": "linked" if node_ids_by_org.get(org_id, set()) else "pending",
            }

    for org_link_id in sorted(org_links_index.keys()):
        org_id = str(org_links_index[org_link_id].get("org_id", "")).strip().lower()
        org_links_index[org_link_id]["ca_ids"] = sorted(
            {
                item["stable_id"]
                for item in cas_index.values()
                if str(item.get("org_id", "")).strip().lower() == org_id
            }
        )

    entities = {
        "cas": sorted(cas_index.values(), key=lambda item: str(item.get("stable_id", ""))),
        "certificates": sorted(certificates_index.values(), key=lambda item: str(item.get("stable_id", ""))),
        "keys": sorted(keys_index.values(), key=lambda item: str(item.get("stable_id", ""))),
        "bundles": sorted(bundles_index.values(), key=lambda item: str(item.get("stable_id", ""))),
    }
    links = {
        "orgs": sorted(org_links_index.values(), key=lambda item: str(item.get("stable_id", ""))),
        "nodes": sorted(node_links_index.values(), key=lambda item: str(item.get("stable_id", ""))),
        "channels": sorted(channel_links_index.values(), key=lambda item: str(item.get("stable_id", ""))),
    }

    inventory_payload = {
        "contract_version": CRYPTO_INVENTORY_CONTRACT_VERSION,
        "run_id": run.run_id,
        "change_id": run.change_id,
        "blueprint_fingerprint": run.blueprint_fingerprint,
        "resolved_schema_version": run.resolved_schema_version,
        "serialization": {
            "format": "json",
            "deterministic_sort_keys": True,
            "deterministic_lists": True,
        },
        "entities": entities,
        "links": links,
        "summary": {
            "entity_counts": {
                "cas": len(entities["cas"]),
                "certificates": len(entities["certificates"]),
                "keys": len(entities["keys"]),
                "bundles": len(entities["bundles"]),
            },
            "link_counts": {
                "orgs": len(links["orgs"]),
                "nodes": len(links["nodes"]),
                "channels": len(links["channels"]),
            },
        },
    }
    return {
        **inventory_payload,
        "inventory_fingerprint": payload_sha256(inventory_payload),
        "generated_at": utc_now_iso(),
    }


def _apply_convergent_configuration(
    *,
    runtime_state: Dict[str, Dict[str, Any]],
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
    issues: List[ConfigureIssue],
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []
    for host_ref in sorted(nodes_by_host.keys()):
        host_state = runtime_state.setdefault(host_ref, {"nodes": {}, "configured_nodes": {}})
        if bool(host_state.get("fail_configure", False)):
            issues.append(
                ConfigureIssue(
                    level="error",
                    code="configure_host_failure",
                    path=f"runtime_state.{host_ref}",
                    message=f"Falha injetada de configuração no host '{host_ref}'.",
                )
            )
            continue
        configured_nodes = host_state.setdefault("configured_nodes", {})
        for node in nodes_by_host[host_ref]:
            node_id = str((node or {}).get("node_id", "")).strip()
            node_type = str((node or {}).get("node_type", "")).strip()
            org_id = str((node or {}).get("org_id", "")).strip()
            ports = sorted(int(port) for port in ((node or {}).get("ports") or []) if str(port).isdigit())
            endpoint = f"{host_ref}:{ports[0]}" if ports else host_ref
            target = {
                "node_id": node_id,
                "node_type": node_type,
                "org_id": org_id,
                "host_ref": host_ref,
                "ports": ports,
                "endpoint": endpoint,
                "identity_profile": f"identity/{org_id}/{node_type}",
                "connectivity_profile": f"connectivity/{host_ref}/{node_id}",
            }
            current = configured_nodes.get(node_id)
            if current != target:
                configured_nodes[node_id] = target
                evidence_hash = payload_sha256(target)
                changes.append(
                    {
                        "host_ref": host_ref,
                        "org_id": org_id,
                        "node_id": node_id,
                        "node_type": node_type,
                        "effective_parameters": target,
                        "change_hash": evidence_hash,
                    }
                )
    return changes


def _validate_critical_components(
    *,
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
    runtime_state: Dict[str, Dict[str, Any]],
    issues: List[ConfigureIssue],
) -> None:
    critical_types = {"ca", "peer", "orderer"}
    for host_ref in sorted(nodes_by_host.keys()):
        configured_nodes = runtime_state.get(host_ref, {}).get("configured_nodes", {})
        for node in nodes_by_host[host_ref]:
            node_id = str((node or {}).get("node_id", "")).strip()
            node_type = str((node or {}).get("node_type", "")).strip()
            if node_type not in critical_types:
                continue
            current = configured_nodes.get(node_id)
            if not current:
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_component_not_applied",
                        path=f"runtime_state.{host_ref}.configured_nodes.{node_id}",
                        message=f"Configuração não aplicada para componente crítico '{node_id}'.",
                    )
                )
                continue
            if not str(current.get("endpoint", "")).strip():
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_component_endpoint_missing",
                        path=f"runtime_state.{host_ref}.configured_nodes.{node_id}.endpoint",
                        message=f"Endpoint ausente para componente crítico '{node_id}'.",
                    )
                )


def _build_technical_inventory(
    *,
    runtime_state: Dict[str, Dict[str, Any]],
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
    crypto_inventory: Dict[str, Any],
) -> Dict[str, Any]:
    hosts: List[Dict[str, Any]] = []
    for host_ref in sorted(nodes_by_host.keys()):
        configured_nodes = runtime_state.get(host_ref, {}).get("configured_nodes", {})
        hosts.append(
            {
                "host_ref": host_ref,
                "configured_components": [
                    configured_nodes[node_id]
                    for node_id in sorted(configured_nodes.keys())
                ],
                "msp_tls_manifests": msp_tls_manifests_by_host.get(host_ref, []),
            }
        )
    return {
        "hosts": hosts,
        "crypto_inventory": crypto_inventory,
        "generated_at": utc_now_iso(),
    }


def _validate_msp_tls_materialization(
    *,
    nodes_by_host: Dict[str, List[Dict[str, Any]]],
    msp_tls_manifests_by_host: Dict[str, List[Dict[str, Any]]],
    issues: List[ConfigureIssue],
) -> None:
    for host_ref in sorted(nodes_by_host.keys()):
        manifest_by_node = {
            str(item.get("node_id", "")).strip(): item
            for item in msp_tls_manifests_by_host.get(host_ref, [])
            if str(item.get("node_id", "")).strip()
        }
        for node in nodes_by_host[host_ref]:
            node_id = str((node or {}).get("node_id", "")).strip()
            node_type = str((node or {}).get("node_type", "")).strip().lower()
            if node_type not in {"peer", "orderer", "ca"}:
                continue
            manifest = manifest_by_node.get(node_id)
            if not isinstance(manifest, dict):
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_msp_tls_manifest_missing",
                        path=f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}",
                        message=f"Manifesto MSP/TLS ausente para endpoint crítico '{node_id}'.",
                    )
                )
                continue

            manifest_hash = str(manifest.get("manifest_hash", "")).strip()
            if len(manifest_hash) != 64:
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_msp_tls_manifest_hash_invalid",
                        path=f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}.manifest_hash",
                        message=f"manifest_hash inválido para endpoint '{node_id}'.",
                    )
                )

            validation = manifest.get("validation") if isinstance(manifest.get("validation"), dict) else {}
            if not bool(validation.get("chain_valid", False)):
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_msp_tls_chain_invalid",
                        path=f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}.validation.chain_valid",
                        message=f"Cadeia MSP/TLS inválida para endpoint '{node_id}'.",
                    )
                )
            if not bool(validation.get("key_cert_match", False)):
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_msp_tls_key_cert_mismatch",
                        path=f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}.validation.key_cert_match",
                        message=f"Correspondência chave/certificado inválida para endpoint '{node_id}'.",
                    )
                )

            layout = manifest.get("msp_layout") if isinstance(manifest.get("msp_layout"), dict) else {}
            directories = {
                str(item).strip().split("/")[-1]
                for item in (layout.get("directories") or [])
                if str(item).strip()
            }
            if not REQUIRED_MSP_LAYOUT_DIRS.issubset(directories):
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_msp_layout_incomplete",
                        path=f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}.msp_layout.directories",
                        message=f"Layout MSP incompleto para endpoint '{node_id}'.",
                    )
                )

            artifacts = [item for item in (manifest.get("artifacts") or []) if isinstance(item, dict)]
            if not artifacts:
                issues.append(
                    ConfigureIssue(
                        level="error",
                        code="configure_msp_tls_artifacts_missing",
                        path=f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}.artifacts",
                        message=f"Lista de artefatos MSP/TLS ausente para endpoint '{node_id}'.",
                    )
                )
                continue
            for idx, artifact in enumerate(artifacts):
                artifact_hash = str(artifact.get("content_hash", "")).strip()
                if len(artifact_hash) != 64:
                    issues.append(
                        ConfigureIssue(
                            level="error",
                            code="configure_msp_tls_artifact_hash_invalid",
                            path=(
                                f"runtime_inventory.hosts.{host_ref}.msp_tls_manifests.{node_id}."
                                f"artifacts[{idx}].content_hash"
                            ),
                            message=f"Hash inválido em artefato MSP/TLS de '{node_id}'.",
                        )
                    )


def _provision_completed_checkpoint(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
) -> bool:
    key = run.idempotency_key("provision")
    checkpoint = state_store.load_checkpoint(run.run_id, "provision", key)
    return bool(checkpoint and checkpoint.stage_status == "completed")


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


def run_configure_stage(
    *,
    run: PipelineRun,
    blueprint_validation: BlueprintValidationResult,
    runtime_inventory: Dict[str, Any],
    runtime_state: Optional[Dict[str, Dict[str, Any]]] = None,
    state_store: Optional[PipelineStateStore] = None,
    executor: str = "configure-engine",
    attempt: int = 1,
) -> ConfigureExecutionResult:
    if not blueprint_validation.valid:
        raise ValueError("configure exige blueprint válido (gate A1.2).")
    if str(runtime_inventory.get("stage", "")).strip().lower() != "provision":
        raise ValueError("runtime_inventory inválido: stage esperado é 'provision'.")
    if attempt <= 0:
        raise ValueError("attempt deve ser maior que 0.")

    crypto_preconditions = (
        runtime_inventory.get("crypto_preconditions")
        if isinstance(runtime_inventory.get("crypto_preconditions"), dict)
        else {}
    )
    if not crypto_preconditions or not bool(crypto_preconditions.get("valid", False)):
        raise ValueError("configure exige pré-condições criptográficas válidas originadas do prepare.")

    if state_store is not None:
        if not _provision_completed_checkpoint(state_store=state_store, run=run):
            raise ValueError("configure exige checkpoint completed da etapa provision.")
        completed_checkpoint = _configure_completed_checkpoint(state_store=state_store, run=run)
        if completed_checkpoint is not None:
            artifacts: Dict[str, str] = {}
            profiles_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "connection-profiles.json"
            manifests_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "network-manifests.json"
            inventory_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "technical-inventory.json"
            report_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "configure-report.json"
            crypto_inventory_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "crypto-inventory.json"
            crypto_rotation_report_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "crypto-rotation-report.json"
            crypto_revocation_report_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "crypto-revocation-report.json"

            connection_profiles = {}
            network_manifests = {}
            technical_inventory = {}
            crypto_inventory = {}
            configure_report = {
                "run_id": run.run_id,
                "change_id": run.change_id,
                "stage": "configure",
                "blocked": False,
                "reexecution": "skipped_completed_checkpoint",
                "issues": [],
                "generated_at": utc_now_iso(),
            }

            if profiles_path.exists():
                connection_profiles = json.loads(profiles_path.read_text(encoding="utf-8"))
                artifacts["connection_profiles"] = str(profiles_path)
            if manifests_path.exists():
                network_manifests = json.loads(manifests_path.read_text(encoding="utf-8"))
                artifacts["network_manifests"] = str(manifests_path)
            if inventory_path.exists():
                technical_inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
                artifacts["technical_inventory"] = str(inventory_path)
            if crypto_inventory_path.exists():
                crypto_inventory = json.loads(crypto_inventory_path.read_text(encoding="utf-8"))
                artifacts["crypto_inventory"] = str(crypto_inventory_path)
                if isinstance(technical_inventory, dict) and not isinstance(technical_inventory.get("crypto_inventory"), dict):
                    technical_inventory["crypto_inventory"] = crypto_inventory
            if crypto_rotation_report_path.exists():
                artifacts["crypto_rotation_report"] = str(crypto_rotation_report_path)
            if crypto_revocation_report_path.exists():
                artifacts["crypto_revocation_report"] = str(crypto_revocation_report_path)
            if report_path.exists():
                configure_report = json.loads(report_path.read_text(encoding="utf-8"))
                artifacts["configure_report"] = str(report_path)
            configure_report["reexecution"] = "skipped_completed_checkpoint"

            return ConfigureExecutionResult(
                connection_profiles=connection_profiles,
                network_manifests=network_manifests,
                technical_inventory=technical_inventory,
                configure_report=configure_report,
                blocked=False,
                artifacts=artifacts,
                issues=[],
                checkpoint=completed_checkpoint,
            )

    normalized_runtime_state = _normalize_runtime_state(runtime_state)
    nodes_by_host = _runtime_nodes_by_host(runtime_inventory)
    msp_tls_manifests_by_host = _runtime_msp_tls_manifests_by_host(runtime_inventory)
    crypto_services_by_host = _runtime_crypto_services_by_host(runtime_inventory)
    org_index = _build_org_index(blueprint_validation)

    issues: List[ConfigureIssue] = []
    changes = _apply_convergent_configuration(
        runtime_state=normalized_runtime_state,
        nodes_by_host=nodes_by_host,
        issues=issues,
    )
    _validate_critical_components(
        nodes_by_host=nodes_by_host,
        runtime_state=normalized_runtime_state,
        issues=issues,
    )
    _validate_msp_tls_materialization(
        nodes_by_host=nodes_by_host,
        msp_tls_manifests_by_host=msp_tls_manifests_by_host,
        issues=issues,
    )

    connection_profiles = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "profiles": _build_connection_profiles(org_index=org_index, nodes_by_host=nodes_by_host),
        "generated_at": utc_now_iso(),
    }

    environment_profile = blueprint_validation.normalized_environment_profile
    stage = str((environment_profile or {}).get("stage", "dev")).strip().lower()
    rotation_report = _apply_crypto_validity_rotation_policy(
        run=run,
        stage=stage,
        changes=changes,
        crypto_services_by_host=crypto_services_by_host,
        msp_tls_manifests_by_host=msp_tls_manifests_by_host,
        connection_profiles=connection_profiles,
        issues=issues,
    )
    revocation_report = _apply_crypto_revocation_policy(
        run=run,
        stage=stage,
        crypto_services_by_host=crypto_services_by_host,
        msp_tls_manifests_by_host=msp_tls_manifests_by_host,
        connection_profiles=connection_profiles,
        issues=issues,
    )

    crypto_inventory = _build_crypto_inventory(
        run=run,
        blueprint_validation=blueprint_validation,
        nodes_by_host=nodes_by_host,
        crypto_services_by_host=crypto_services_by_host,
        msp_tls_manifests_by_host=msp_tls_manifests_by_host,
    )
    network_manifests = _build_network_manifests(
        blueprint_validation=blueprint_validation,
        nodes_by_host=nodes_by_host,
        msp_tls_manifests_by_host=msp_tls_manifests_by_host,
        crypto_inventory=crypto_inventory,
        rotation_report=rotation_report,
        revocation_report=revocation_report,
    )
    technical_inventory = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        **_build_technical_inventory(
            runtime_state=normalized_runtime_state,
            nodes_by_host=nodes_by_host,
            msp_tls_manifests_by_host=msp_tls_manifests_by_host,
            crypto_inventory=crypto_inventory,
        ),
    }

    issues = _sort_issues(issues)
    blocked = any(issue.level == "error" for issue in issues)

    configure_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "configure",
        "blocked": blocked,
        "ready_for_verify": not blocked,
        "crypto_preconditions": {
            "valid": bool(crypto_preconditions.get("valid", False)),
            "baseline_version": crypto_preconditions.get("baseline_version", ""),
            "schema_ref": crypto_preconditions.get("schema_ref", ""),
            "trust_domain_count": int(crypto_preconditions.get("trust_domain_count", 0) or 0),
            "org_profile_count": int(crypto_preconditions.get("org_profile_count", 0) or 0),
        },
        "effective_parameter_changes": sorted(
            changes,
            key=lambda item: (item.get("host_ref", ""), item.get("org_id", ""), item.get("node_id", "")),
        ),
        "crypto_materialization": {
            "manifest_count": sum(
                1
                for host_ref in sorted(msp_tls_manifests_by_host.keys())
                for _ in msp_tls_manifests_by_host.get(host_ref, [])
            ),
            "manifest_hashes": sorted(
                {
                    str(item.get("manifest_hash", ""))
                    for host_ref in sorted(msp_tls_manifests_by_host.keys())
                    for item in msp_tls_manifests_by_host.get(host_ref, [])
                    if str(item.get("manifest_hash", "")).strip()
                }
            ),
        },
        "crypto_inventory": {
            "contract_version": crypto_inventory.get("contract_version"),
            "inventory_fingerprint": crypto_inventory.get("inventory_fingerprint"),
            "entity_counts": dict((crypto_inventory.get("summary") or {}).get("entity_counts") or {}),
        },
        "crypto_rotation": {
            "stage_profile": rotation_report.get("stage_profile"),
            "policy": rotation_report.get("policy", {}),
            "rotation_event_count": len(rotation_report.get("rotation_events") or []),
            "ca_rotation_event_count": len(rotation_report.get("ca_rotation_events") or []),
            "critical_expiry_blocks": [
                item for item in (rotation_report.get("critical_expiry_blocks") or []) if isinstance(item, dict)
            ],
            "manifest_impacts": [
                item for item in (rotation_report.get("manifest_impacts") or []) if isinstance(item, dict)
            ],
            "profile_impacts": [
                item for item in (rotation_report.get("profile_impacts") or []) if isinstance(item, dict)
            ],
            "recorded_at": rotation_report.get("recorded_at", ""),
        },
        "crypto_revocation": {
            "stage_profile": revocation_report.get("stage_profile"),
            "revocation_event_count": len(revocation_report.get("revocation_events") or []),
            "org_crl_count": len(revocation_report.get("org_crls") or []),
            "org_crls": [
                item for item in (revocation_report.get("org_crls") or []) if isinstance(item, dict)
            ],
            "manifest_impacts": [
                item for item in (revocation_report.get("manifest_impacts") or []) if isinstance(item, dict)
            ],
            "gateway_impacts": [
                item for item in (revocation_report.get("gateway_impacts") or []) if isinstance(item, dict)
            ],
            "incident_response": [
                item for item in (revocation_report.get("incident_response") or []) if isinstance(item, dict)
            ],
            "propagation_failures": [
                item for item in (revocation_report.get("propagation_failures") or []) if isinstance(item, dict)
            ],
            "decision_trace": [
                item for item in (revocation_report.get("decision_trace") or []) if isinstance(item, dict)
            ],
            "recorded_at": revocation_report.get("recorded_at", ""),
        },
        "issues": [issue.to_dict() for issue in issues],
        "generated_at": utc_now_iso(),
    }

    artifacts: Dict[str, str] = {}
    checkpoint: Optional[StageCheckpoint] = None
    if state_store is not None:
        profiles_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="connection-profiles.json",
            content=json.dumps(connection_profiles, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        manifests_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="network-manifests.json",
            content=json.dumps(network_manifests, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        inventory_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="technical-inventory.json",
            content=json.dumps(technical_inventory, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        crypto_inventory_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="crypto-inventory.json",
            content=json.dumps(crypto_inventory, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        crypto_rotation_report_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="crypto-rotation-report.json",
            content=json.dumps(rotation_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        crypto_revocation_report_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="crypto-revocation-report.json",
            content=json.dumps(revocation_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        report_path = state_store.write_artifact(
            run_id=run.run_id,
            stage="configure",
            artifact_name="configure-report.json",
            content=json.dumps(configure_report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        )
        artifacts = {
            "connection_profiles": str(profiles_path),
            "network_manifests": str(manifests_path),
            "technical_inventory": str(inventory_path),
            "crypto_inventory": str(crypto_inventory_path),
            "crypto_rotation_report": str(crypto_rotation_report_path),
            "crypto_revocation_report": str(crypto_revocation_report_path),
            "configure_report": str(report_path),
        }

        state_store.persist_run_state(run)
        checkpoint = state_store.persist_stage_checkpoint(
            run=run,
            stage="configure",
            stage_status="failed" if blocked else "completed",
            input_hash=payload_sha256(
                {
                    "runtime_inventory": runtime_inventory,
                    "runtime_state": normalized_runtime_state,
                    "blueprint_fingerprint": run.blueprint_fingerprint,
                }
            ),
            output_hash=payload_sha256(
                {
                    "connection_profiles": connection_profiles,
                    "network_manifests": network_manifests,
                    "technical_inventory": technical_inventory,
                    "crypto_inventory": crypto_inventory,
                    "crypto_rotation_report": rotation_report,
                    "crypto_revocation_report": revocation_report,
                    "configure_report": configure_report,
                }
            ),
            attempt=attempt,
            executor=executor,
            timestamp_utc=utc_now_iso(),
        )

    return ConfigureExecutionResult(
        connection_profiles=connection_profiles,
        network_manifests=network_manifests,
        technical_inventory=technical_inventory,
        configure_report=configure_report,
        blocked=blocked,
        artifacts=artifacts,
        issues=issues,
        checkpoint=checkpoint,
    )
