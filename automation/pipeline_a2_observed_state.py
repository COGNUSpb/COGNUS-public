from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from .pipeline_contract import PipelineRun, utc_now_iso
from .pipeline_state_store import payload_sha256
from .provisioning_ssh_executor import (
    ProvisioningSshExecutor,
    SshExecutionCapturedOutput,
    build_ssh_unit_idempotency_key,
    sanitize_sensitive_text,
)


_DOCKER_DISCOVERY_COMMANDS: Tuple[Tuple[str, str], ...] = (
    ("docker_ps_a", "docker ps -a --format '{{json .}}'"),
    ("docker_inspect", "docker inspect $(docker ps -aq)"),
    ("docker_network_ls", "docker network ls --format '{{json .}}'"),
    ("docker_volume_ls", "docker volume ls --format '{{json .}}'"),
)

_COMPONENT_NAME_PATTERNS: Tuple[Tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"^peer\d+\."), "peer"),
    (re.compile(r"^orderer\d+\."), "orderer"),
    (re.compile(r"^couch\d+\."), "couch"),
    (re.compile(r"^apigateway\d+\."), "api_gateway"),
    (re.compile(r"^ca_"), "ca"),
    (re.compile(r"^netapi_"), "network_api"),
    (re.compile(r"^cc-webclient\d+\."), "cc_webclient"),
    (re.compile(r"^dev-peer\d+\."), "chaincode_runtime"),
)


def _normalize_container_name(value: str) -> str:
    normalized = str(value).strip()
    while normalized.startswith("/"):
        normalized = normalized[1:]
    return normalized


def _normalize_status(value: str) -> str:
    normalized = str(value).strip().lower()
    if not normalized:
        return "unknown"
    if normalized.startswith("up"):
        return "running"
    if normalized.startswith("exited"):
        return "exited"
    if normalized.startswith("created"):
        return "created"
    if normalized.startswith("paused"):
        return "paused"
    if normalized.startswith("dead"):
        return "dead"
    return normalized.split(" ")[0]


def _parse_host_ports(value: str) -> List[int]:
    ports = set()
    text = str(value).strip()
    if not text:
        return []
    for match in re.findall(r":(\d+)->", text):
        try:
            port = int(match)
        except ValueError:
            continue
        if port > 0:
            ports.add(port)
    return sorted(ports)


def _parse_json_lines(stdout: str) -> Tuple[List[Dict[str, Any]], bool]:
    text = str(stdout).strip()
    if not text:
        return [], True
    try:
        payload = json.loads(text)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)], True
        if isinstance(payload, dict):
            return [payload], True
    except json.JSONDecodeError:
        pass

    parsed: List[Dict[str, Any]] = []
    parse_failed = False
    for raw_line in text.splitlines():
        line = str(raw_line).strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            parse_failed = True
            continue
        if isinstance(item, dict):
            parsed.append(item)
        else:
            parse_failed = True
    return parsed, not parse_failed


def _parse_inspect(stdout: str) -> Tuple[List[Dict[str, Any]], bool]:
    text = str(stdout).strip()
    if not text:
        return [], True
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [], False
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], True
    if isinstance(payload, dict):
        return [payload], True
    return [], False


def _name_and_id_indexes(
    desired_components: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    by_host_name: Dict[str, Dict[str, str]] = {}
    by_host_component_id: Dict[str, Dict[str, str]] = {}
    for component in desired_components:
        if not isinstance(component, dict):
            continue
        host_id = str(component.get("host_id", "")).strip()
        name = _normalize_container_name(component.get("name", ""))
        component_id = str(component.get("component_id", "")).strip()
        component_type = str(component.get("component_type", "")).strip().lower()
        if not host_id or not component_type:
            continue
        if name:
            by_host_name.setdefault(host_id, {})[name.lower()] = component_type
        if component_id:
            by_host_component_id.setdefault(host_id, {})[component_id.lower()] = component_type
    return by_host_name, by_host_component_id


def _infer_component_type(
    *,
    host_id: str,
    name: str,
    component_id: str,
    by_host_name: Dict[str, Dict[str, str]],
    by_host_component_id: Dict[str, Dict[str, str]],
) -> str:
    normalized_name = _normalize_container_name(name).lower()
    normalized_component_id = str(component_id).strip().lower()

    from_name = by_host_name.get(host_id, {}).get(normalized_name, "")
    if from_name:
        return from_name
    from_component_id = by_host_component_id.get(host_id, {}).get(normalized_component_id, "")
    if from_component_id:
        return from_component_id

    for pattern, inferred in _COMPONENT_NAME_PATTERNS:
        if pattern.match(normalized_name):
            return inferred
    return "unknown"


def _extract_inspect_networks(inspect_payload: Dict[str, Any]) -> List[str]:
    network_settings = (
        inspect_payload.get("NetworkSettings")
        if isinstance(inspect_payload.get("NetworkSettings"), dict)
        else {}
    )
    networks = network_settings.get("Networks", {})
    if not isinstance(networks, dict):
        return []
    return sorted(str(name).strip() for name in networks.keys() if str(name).strip())


def _extract_inspect_mounts(inspect_payload: Dict[str, Any]) -> List[str]:
    mounts = inspect_payload.get("Mounts", [])
    if not isinstance(mounts, list):
        return []
    values = set()
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        source = str(mount.get("Source", "")).strip()
        destination = str(mount.get("Destination", "")).strip()
        if source:
            values.add(source)
        if destination:
            values.add(destination)
    return sorted(values)


def _extract_inspect_ports(inspect_payload: Dict[str, Any]) -> List[int]:
    network_settings = (
        inspect_payload.get("NetworkSettings")
        if isinstance(inspect_payload.get("NetworkSettings"), dict)
        else {}
    )
    ports_raw = network_settings.get("Ports", {})
    if not isinstance(ports_raw, dict):
        return []
    ports = set()
    for mappings in ports_raw.values():
        if not isinstance(mappings, list):
            continue
        for mapping in mappings:
            if not isinstance(mapping, dict):
                continue
            host_port = str(mapping.get("HostPort", "")).strip()
            if not host_port.isdigit():
                continue
            ports.add(int(host_port))
    return sorted(ports)


def _extract_inspect_env_profile(inspect_payload: Dict[str, Any]) -> str:
    config = (
        inspect_payload.get("Config")
        if isinstance(inspect_payload.get("Config"), dict)
        else {}
    )
    labels = config.get("Labels", {})
    if isinstance(labels, dict):
        for key in (
            "env_profile",
            "cognus.env_profile",
            "environment_profile",
            "org.env_profile",
        ):
            candidate = str(labels.get(key, "")).strip().lower()
            if candidate:
                return candidate

    env_list = config.get("Env", [])
    if isinstance(env_list, list):
        for item in env_list:
            text = str(item).strip()
            if not text or "=" not in text:
                continue
            env_key, env_value = text.split("=", 1)
            key_normalized = env_key.strip().upper()
            if key_normalized in {"ENV_PROFILE", "ORG_ENV_PROFILE", "COGNUS_ENV_PROFILE", "ENV"}:
                candidate = env_value.strip().lower()
                if candidate:
                    return candidate
    return ""


def _string_from_cmd_parts(value: Any) -> str:
    if isinstance(value, list):
        return " ".join(str(part).strip() for part in value if str(part).strip())
    if isinstance(value, str):
        return value.strip()
    return ""


def _normalize_observed_components_for_host(
    *,
    host_id: str,
    docker_ps_rows: List[Dict[str, Any]],
    docker_inspect_rows: List[Dict[str, Any]],
    by_host_name: Dict[str, Dict[str, str]],
    by_host_component_id: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    inspect_by_name: Dict[str, Dict[str, Any]] = {}
    inspect_by_id: Dict[str, Dict[str, Any]] = {}
    for item in docker_inspect_rows:
        if not isinstance(item, dict):
            continue
        name = _normalize_container_name(item.get("Name", ""))
        container_id = str(item.get("Id", "")).strip()
        if name:
            inspect_by_name[name.lower()] = item
        if container_id:
            inspect_by_id[container_id.lower()] = item

    observed_index: Dict[str, Dict[str, Any]] = {}
    for row in docker_ps_rows:
        if not isinstance(row, dict):
            continue
        name = _normalize_container_name(row.get("Names", ""))
        if not name:
            continue
        short_id = str(row.get("ID", "")).strip()
        inspect_payload = inspect_by_name.get(name.lower())
        if inspect_payload is None and short_id:
            inspect_payload = inspect_by_id.get(short_id.lower())

        image = str(row.get("Image", "")).strip()
        command = str(row.get("Command", "")).strip().strip("\"'")
        status = _normalize_status(row.get("Status", ""))
        ports = set(_parse_host_ports(row.get("Ports", "")))
        networks = []
        volumes = []
        container_id = short_id
        if isinstance(inspect_payload, dict):
            if not image:
                config = inspect_payload.get("Config", {})
                if isinstance(config, dict):
                    image = str(config.get("Image", "")).strip()
            if not command:
                config = inspect_payload.get("Config", {})
                if isinstance(config, dict):
                    command = _string_from_cmd_parts(config.get("Cmd"))
            state_payload = inspect_payload.get("State", {})
            if isinstance(state_payload, dict) and str(state_payload.get("Status", "")).strip():
                status = _normalize_status(state_payload.get("Status", ""))
            ports.update(_extract_inspect_ports(inspect_payload))
            networks = _extract_inspect_networks(inspect_payload)
            volumes = _extract_inspect_mounts(inspect_payload)
            container_id = str(inspect_payload.get("Id", "")).strip() or container_id
            env_profile = _extract_inspect_env_profile(inspect_payload)
        else:
            env_profile = ""

        component_type = _infer_component_type(
            host_id=host_id,
            name=name,
            component_id=name,
            by_host_name=by_host_name,
            by_host_component_id=by_host_component_id,
        )
        component_key = f"{component_type}:{name.lower()}"
        observed_index[component_key] = {
            "component_key": component_key,
            "host_id": host_id,
            "name": name,
            "component_type": component_type,
            "component_id": name,
            "image": image,
            "command": sanitize_sensitive_text(command),
            "ports": sorted(ports),
            "status": status,
            "networks": networks,
            "volumes": volumes,
            "env_profile": env_profile,
            "container_id": container_id,
            "source": "docker_ps_inspect",
        }

    ordered = sorted(
        observed_index.values(),
        key=lambda item: (
            str(item.get("component_type", "")),
            str(item.get("name", "")),
        ),
    )
    return ordered


def _discover_command_report(
    *,
    capture: SshExecutionCapturedOutput,
    command_id: str,
    command: str,
) -> Dict[str, Any]:
    result = capture.result
    attempts = list(result.attempts)
    last_attempt = attempts[-1] if attempts else None
    return {
        "command_id": command_id,
        "command": sanitize_sensitive_text(command),
        "status": result.status,
        "classification": result.classification,
        "final_exit_code": result.final_exit_code,
        "attempt_count": len(attempts),
        "timeout_attempt_count": sum(1 for item in attempts if bool(item.timeout)),
        "stdout_digest": (last_attempt.stdout_digest if last_attempt is not None else ""),
        "stderr_digest": (last_attempt.stderr_digest if last_attempt is not None else ""),
        "idempotency_key": result.idempotency_key,
        "artifact_path": result.artifact_path,
        "reused": result.reused,
    }


@dataclass(frozen=True)
class ObservedStateBaselineResult:
    baseline: Dict[str, Any]
    observed_components_by_host: Dict[str, List[Dict[str, Any]]]
    parseable_hosts: List[str]
    issues: List[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def collect_observed_state_baseline(
    *,
    run: PipelineRun,
    host_ids: List[str],
    ssh_executor: ProvisioningSshExecutor,
    execution_generated_at: str,
    manifest_fingerprint: str,
    source_blueprint_fingerprint: str,
    desired_components: Optional[List[Dict[str, Any]]] = None,
) -> ObservedStateBaselineResult:
    resolved_manifest_fingerprint = str(manifest_fingerprint).strip().lower()
    resolved_blueprint_fingerprint = str(source_blueprint_fingerprint).strip().lower()
    resolved_generated_at = str(execution_generated_at).strip() or run.started_at or utc_now_iso()

    host_ids_sorted = sorted(str(item).strip() for item in host_ids if str(item).strip())
    desired = [
        dict(item)
        for item in (desired_components or [])
        if isinstance(item, dict)
    ]
    by_host_name, by_host_component_id = _name_and_id_indexes(desired)

    host_reports: List[Dict[str, Any]] = []
    observed_by_host: Dict[str, List[Dict[str, Any]]] = {}
    parseable_hosts: List[str] = []
    issues: List[Dict[str, str]] = []

    for host_id in host_ids_sorted:
        command_reports: List[Dict[str, Any]] = []
        docker_ps_rows: List[Dict[str, Any]] = []
        docker_inspect_rows: List[Dict[str, Any]] = []
        networks: List[str] = []
        volumes: List[str] = []
        docker_ps_parseable = False
        docker_inspect_parseable = False

        for command_id, command in _DOCKER_DISCOVERY_COMMANDS:
            discovery_component_id = f"host-discovery-{command_id}"
            idempotency_key = build_ssh_unit_idempotency_key(
                run=run,
                host_id=host_id,
                component_id=discovery_component_id,
                operation="verify",
                component_signature=f"{command_id}|{resolved_manifest_fingerprint}",
            )
            capture = ssh_executor.execute_unit_with_output(
                run=run,
                host_id=host_id,
                component_id=discovery_component_id,
                operation="verify",
                idempotency_key=idempotency_key,
                command=command,
                metadata={"discovery": "observed_state_baseline", "command_id": command_id},
            )
            command_reports.append(
                _discover_command_report(
                    capture=capture,
                    command_id=command_id,
                    command=command,
                )
            )

            stdout = capture.stdout
            if command_id == "docker_ps_a":
                docker_ps_rows, docker_ps_parseable = _parse_json_lines(stdout)
            elif command_id == "docker_inspect":
                docker_inspect_rows, docker_inspect_parseable = _parse_inspect(stdout)
            elif command_id == "docker_network_ls":
                network_rows, network_parseable = _parse_json_lines(stdout)
                if network_parseable:
                    networks = sorted(
                        str(item.get("Name", "")).strip()
                        for item in network_rows
                        if str(item.get("Name", "")).strip()
                    )
            elif command_id == "docker_volume_ls":
                volume_rows, volume_parseable = _parse_json_lines(stdout)
                if volume_parseable:
                    volumes = sorted(
                        str(item.get("Name", "")).strip()
                        for item in volume_rows
                        if str(item.get("Name", "")).strip()
                    )

        observed_components = _normalize_observed_components_for_host(
            host_id=host_id,
            docker_ps_rows=docker_ps_rows,
            docker_inspect_rows=docker_inspect_rows,
            by_host_name=by_host_name,
            by_host_component_id=by_host_component_id,
        )
        if docker_ps_parseable:
            parseable_hosts.append(host_id)
        else:
            issues.append(
                {
                    "level": "warning",
                    "code": "provision_observed_state_not_parseable",
                    "path": f"observed_state_baseline.hosts.{host_id}.docker_ps_a",
                    "message": (
                        f"Snapshot observado do host '{host_id}' não foi parseável a partir de 'docker ps -a'. "
                        "Reconciliação manteve fallback para estado conhecido."
                    ),
                }
            )
        if not docker_inspect_parseable:
            issues.append(
                {
                    "level": "warning",
                    "code": "provision_observed_inspect_not_parseable",
                    "path": f"observed_state_baseline.hosts.{host_id}.docker_inspect",
                    "message": (
                        f"Saída de 'docker inspect' do host '{host_id}' não parseável. "
                        "Campos avançados (networks/volumes/ports) podem estar incompletos."
                    ),
                }
            )

        observed_by_host[host_id] = observed_components
        host_report = {
            "host_id": host_id,
            "discovery_source": "ssh",
            "manifest_fingerprint": resolved_manifest_fingerprint,
            "commands": command_reports,
            "docker_ps_parseable": docker_ps_parseable,
            "docker_inspect_parseable": docker_inspect_parseable,
            "observed_components": observed_components,
            "observed_component_count": len(observed_components),
            "observed_components_fingerprint": payload_sha256(observed_components),
            "networks": networks,
            "volumes": volumes,
            "generated_at": resolved_generated_at,
        }
        host_reports.append(host_report)

    host_reports = sorted(host_reports, key=lambda item: str(item.get("host_id", "")))
    baseline = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "manifest_fingerprint": resolved_manifest_fingerprint,
        "source_blueprint_fingerprint": resolved_blueprint_fingerprint,
        "host_count": len(host_reports),
        "hosts": host_reports,
        "summary": {
            "parseable_host_count": len(parseable_hosts),
            "observed_component_total": sum(
                int(host.get("observed_component_count", 0) or 0)
                for host in host_reports
            ),
        },
        "generated_at": resolved_generated_at,
    }
    return ObservedStateBaselineResult(
        baseline=baseline,
        observed_components_by_host=observed_by_host,
        parseable_hosts=sorted(parseable_hosts),
        issues=sorted(
            issues,
            key=lambda item: (
                str(item.get("level", "")),
                str(item.get("code", "")),
                str(item.get("path", "")),
            ),
        ),
    )
