#
# SPDX-License-Identifier: Apache-2.0
#
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import base64
import errno
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile
from tempfile import gettempdir
from threading import RLock
import uuid

try:
    import docker as docker_sdk
except Exception:  # pragma: no cover
    docker_sdk = None

from drf_yasg.utils import swagger_auto_schema
from rest_framework import status, viewsets
from rest_framework import status as drf_status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from api.common import err, ok
from api.utils.common import with_common_response
from .serializers import (
    RunbookCatalogEnvelopeSerializer,
    RunbookPreflightEnvelopeSerializer,
    RunbookPreflightRequestSerializer,
    RunbookOperateRequestSerializer,
    RunbookRuntimeInspectionEnvelopeSerializer,
    RunbookStartRequestSerializer,
    RunbookStateEnvelopeSerializer,
)

LOG = logging.getLogger(__name__)
RUNBOOK_CONTRACT_VERSION = "a1.7-runbook-api.v1"
RUNBOOK_PROVIDER_KEY = "external-linux"
RUNBOOK_LEGACY_STORE_FILE = Path(gettempdir()) / "cognus_runbook_store.json"
RUNBOOK_STORE_FILE = Path(
    str(
        os.getenv(
            "RUNBOOK_STORE_FILE",
            "/opt/cello/runbook-store/cognus_runbook_store.json",
        )
    ).strip()
    or "/opt/cello/runbook-store/cognus_runbook_store.json"
)
RUNBOOK_STORE_LOCK = RLock()
RUNBOOK_STORE_SCHEMA_VERSION = "a1.7-runbook-store.v1"
RUNBOOK_SSH_TIMEOUT_SECONDS = int(os.getenv("RUNBOOK_SSH_TIMEOUT_SECONDS", "180"))
RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS = int(
    os.getenv("RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS", "180")
)
RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS = max(
    RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS,
    RUNBOOK_SSH_TIMEOUT_SECONDS + 3,
    600,
)
RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS = int(
    os.getenv("RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS", "90")
)
RUNBOOK_SSH_KNOWN_HOSTS_FILE = str(
    os.getenv("RUNBOOK_SSH_KNOWN_HOSTS_FILE", "/tmp/cognus_known_hosts")
).strip() or "/tmp/cognus_known_hosts"
RUNBOOK_SSH_CONTROLMASTER_ENABLED = (
    str(os.getenv("RUNBOOK_SSH_CONTROLMASTER_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_SSH_CONTROL_PERSIST_SECONDS = max(
    0,
    int(str(os.getenv("RUNBOOK_SSH_CONTROL_PERSIST_SECONDS", "180")).strip() or "0"),
)
RUNBOOK_SSH_CONTROL_PATH_DIR = str(
    os.getenv(
        "RUNBOOK_SSH_CONTROL_PATH_DIR",
        str(Path(gettempdir()) / "cognus_ssh_mux"),
    )
).strip() or str(Path(gettempdir()) / "cognus_ssh_mux")
RUNBOOK_SSH_TRANSIENT_RETRY_ATTEMPTS = max(
    0,
    int(str(os.getenv("RUNBOOK_SSH_TRANSIENT_RETRY_ATTEMPTS", "1")).strip() or "1"),
)
RUNBOOK_SSH_TRANSIENT_RETRY_DELAY_SECONDS = max(
    0,
    int(
        str(os.getenv("RUNBOOK_SSH_TRANSIENT_RETRY_DELAY_SECONDS", "2")).strip()
        or "2"
    ),
)
RUNBOOK_SSH_INLINE_COMMAND_MAX_BYTES = max(
    1024,
    int(str(os.getenv("RUNBOOK_SSH_INLINE_COMMAND_MAX_BYTES", "8192")).strip() or "8192"),
)
RUNBOOK_RUNTIME_NODE_TYPE_ALIASES = {
    "api_gateway": "apigateway",
    "api-gateway": "apigateway",
    "api gateway": "apigateway",
    "network_api": "netapi",
    "network-api": "netapi",
    "network api": "netapi",
}


def _normalize_runtime_component_node_type(node_type):
    normalized = str(node_type or "").strip().lower()
    if not normalized:
        return ""
    return RUNBOOK_RUNTIME_NODE_TYPE_ALIASES.get(normalized, normalized)


def _coerce_exit_code(value, default=1):
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL = (
    str(
        os.getenv(
            "RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL",
            "cognus/chaincode-gateway:latest",
        )
    ).strip()
    or "cognus/chaincode-gateway:latest"
)
RUNBOOK_APIGATEWAY_LEGACY_IMAGE_TOKENS = ("ccapi-go",)
if any(
    token in RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL.lower()
    for token in RUNBOOK_APIGATEWAY_LEGACY_IMAGE_TOKENS
):
    RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL = "cognus/chaincode-gateway:latest"
RUNBOOK_NETAPI_RUNTIME_IMAGE_CANONICAL = (
    str(
        os.getenv(
            "RUNBOOK_NETAPI_RUNTIME_IMAGE_CANONICAL",
            "cognus/chaincode-gateway:latest",
        )
    ).strip()
    or "cognus/chaincode-gateway:latest"
)

_RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE = None
_RUNBOOK_CHAINCODE_PACKAGE_B64_CACHE = {}
_RUNBOOK_HOST_REPO_ROOTS_CACHE = None
RUNBOOK_APIGATEWAY_INLINE_GATEWAY_INDEX_MAX_BYTES = max(
    0,
    int(
        str(
            os.getenv("RUNBOOK_APIGATEWAY_INLINE_GATEWAY_INDEX_MAX_BYTES", "4096")
        ).strip()
        or "4096"
    ),
)


def _load_runbook_chaincode_gateway_index_source():
    global _RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE
    if (
        _RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE is not None
        and str(_RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE).strip()
    ):
        return _RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE

    try:
        current = Path(__file__).resolve()
        candidates = [current.parent / "assets" / "chaincode-gateway-index.js"]
        for parent in current.parents:
            candidates.append(parent / "chaincode-gateway" / "index.js")
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                _RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE = candidate.read_text(
                    encoding="utf-8"
                )
                return _RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE
    except Exception:
        pass

    return ""


def _discover_host_repo_roots_from_mounts():
    global _RUNBOOK_HOST_REPO_ROOTS_CACHE
    if isinstance(_RUNBOOK_HOST_REPO_ROOTS_CACHE, list):
        return list(_RUNBOOK_HOST_REPO_ROOTS_CACHE)

    discovered_roots = []
    container_id = str(os.getenv("HOSTNAME", "") or "").strip()
    if not container_id:
        _RUNBOOK_HOST_REPO_ROOTS_CACHE = []
        return []

    try:
        inspect_result = subprocess.run(
            ["docker", "inspect", container_id],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if inspect_result.returncode == 0:
            inspect_payload = json.loads(str(inspect_result.stdout or "[]"))
            mount_rows = (
                inspect_payload[0].get("Mounts", [])
                if isinstance(inspect_payload, list) and inspect_payload
                else []
            )
            for mount_row in mount_rows if isinstance(mount_rows, list) else []:
                if not isinstance(mount_row, dict):
                    continue
                source = str(mount_row.get("Source", "") or "").strip()
                if not source:
                    continue
                normalized_source = source.replace("\\", "/")
                if "/cello-v2/" in normalized_source:
                    root_candidate = source[: normalized_source.index("/cello-v2/")]
                elif normalized_source.endswith("/cello-v2"):
                    root_candidate = source[: -len("/cello-v2")]
                else:
                    continue
                root_candidate = str(root_candidate or "").strip()
                if root_candidate and root_candidate not in discovered_roots:
                    discovered_roots.append(root_candidate)
    except Exception:
        discovered_roots = []

    _RUNBOOK_HOST_REPO_ROOTS_CACHE = list(discovered_roots)
    return list(discovered_roots)


def _normalize_runtime_image_reference(node_type, runtime_image, default_image=""):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    normalized_runtime_image = str(runtime_image or "").strip()
    if not normalized_runtime_image:
        normalized_runtime_image = str(default_image or "").strip()
    if not normalized_runtime_image:
        return ""

    lowered_runtime_image = normalized_runtime_image.lower()
    if any(token in lowered_runtime_image for token in RUNBOOK_APIGATEWAY_LEGACY_IMAGE_TOKENS):
        return RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL

    if normalized_node_type == "apigateway":
        if lowered_runtime_image in {
            "chaincode-gateway:latest",
            "cognus/chaincode-gateway:latest",
        }:
            return RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL
    if normalized_node_type == "orderer":
        if lowered_runtime_image in {
            "hyperledger/fabric-orderer:2.5",
            "fabric-orderer:2.5",
        }:
            return "hyperledger/fabric-orderer:2.5.12"
    if normalized_node_type == "netapi":
        if "networkapi" in lowered_runtime_image:
            return RUNBOOK_NETAPI_RUNTIME_IMAGE_CANONICAL
    return normalized_runtime_image


RUNBOOK_BASE_NODE_IMAGE = str(os.getenv("RUNBOOK_BASE_NODE_IMAGE", "alpine:3.20")).strip() or "alpine:3.20"
RUNBOOK_RUNTIME_IMAGE_BY_NODE_TYPE = {
    "peer": str(os.getenv("RUNBOOK_IMAGE_PEER", "hyperledger/fabric-peer:2.5")).strip()
    or "hyperledger/fabric-peer:2.5",
    "orderer": str(
        os.getenv("RUNBOOK_IMAGE_ORDERER", "hyperledger/fabric-orderer:2.5")
    ).strip()
    or "hyperledger/fabric-orderer:2.5",
    "ca": str(os.getenv("RUNBOOK_IMAGE_CA", "hyperledger/fabric-ca:1.5")).strip()
    or "hyperledger/fabric-ca:1.5",
    "couch": str(os.getenv("RUNBOOK_IMAGE_COUCH", "couchdb:3.2.2")).strip()
    or "couchdb:3.2.2",
    "apigateway": str(
        os.getenv(
            "RUNBOOK_IMAGE_APIGATEWAY",
            RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL,
        )
    ).strip()
    or RUNBOOK_APIGATEWAY_RUNTIME_IMAGE_CANONICAL,
    "netapi": str(
        os.getenv("RUNBOOK_IMAGE_NETAPI", RUNBOOK_NETAPI_RUNTIME_IMAGE_CANONICAL)
    ).strip()
    or RUNBOOK_NETAPI_RUNTIME_IMAGE_CANONICAL,
    "chaincode": str(
        os.getenv("RUNBOOK_IMAGE_CHAINCODE", "hyperledger/fabric-ccenv:2.5")
    ).strip()
    or "hyperledger/fabric-ccenv:2.5",
}
for _node_type, _runtime_image in list(RUNBOOK_RUNTIME_IMAGE_BY_NODE_TYPE.items()):
    RUNBOOK_RUNTIME_IMAGE_BY_NODE_TYPE[_node_type] = _normalize_runtime_image_reference(
        _node_type,
        _runtime_image,
        RUNBOOK_BASE_NODE_IMAGE,
    )
RUNBOOK_SUPPORTED_NODE_TYPES = tuple(RUNBOOK_RUNTIME_IMAGE_BY_NODE_TYPE.keys())
RUNBOOK_DEFAULT_ENABLED_NODE_TYPES = RUNBOOK_SUPPORTED_NODE_TYPES


def _resolve_enabled_runtime_node_types():
    configured_value = str(
        os.getenv(
            "RUNBOOK_ENABLED_NODE_TYPES",
            ",".join(RUNBOOK_DEFAULT_ENABLED_NODE_TYPES),
        )
    ).strip()
    if not configured_value:
        return RUNBOOK_DEFAULT_ENABLED_NODE_TYPES

    normalized_tokens = []
    for token in configured_value.split(","):
        normalized_token = _normalize_runtime_component_node_type(token)
        if not normalized_token:
            continue
        if normalized_token == "*":
            return RUNBOOK_SUPPORTED_NODE_TYPES
        normalized_tokens.append(normalized_token)

    enabled_types = tuple(
        node_type
        for node_type in RUNBOOK_SUPPORTED_NODE_TYPES
        if node_type in set(normalized_tokens)
    )
    if enabled_types:
        return enabled_types
    return RUNBOOK_DEFAULT_ENABLED_NODE_TYPES


RUNBOOK_ENABLED_NODE_TYPES = _resolve_enabled_runtime_node_types()
RUNBOOK_RUNTIME_HOST_MAPPING_AUTOEXPAND_ENABLED = (
    str(os.getenv("RUNBOOK_RUNTIME_HOST_MAPPING_AUTOEXPAND_ENABLED", "true"))
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_RUNTIME_IMAGE_FORBIDDEN_PREFIXES = tuple(
    prefix
    for prefix in [
        str(prefix or "").strip().lower()
        for prefix in str(
            os.getenv("RUNBOOK_RUNTIME_IMAGE_FORBIDDEN_PREFIXES", "goledger/,goleder/")
        ).split(",")
    ]
    if prefix
)
RUNBOOK_RUNTIME_IMAGE_REQUIRED_PREFIX_BY_NODE_TYPE = {
    "apigateway": str(
        os.getenv("RUNBOOK_REQUIRED_IMAGE_PREFIX_APIGATEWAY", "cognus/")
    ).strip().lower(),
    "netapi": str(
        os.getenv("RUNBOOK_REQUIRED_IMAGE_PREFIX_NETAPI", "cognus/")
    ).strip().lower(),
}
RUNBOOK_RUNTIME_IMAGE_LOCAL_ALIAS_BY_NODE_TYPE = {
    "apigateway": tuple(
        alias
        for alias in [
            str(alias or "").strip()
            for alias in str(
                os.getenv(
                    "RUNBOOK_LOCAL_IMAGE_ALIASES_APIGATEWAY",
                    "chaincode-gateway:latest,cognus/chaincode-gateway:latest",
                )
            ).split(",")
        ]
        if alias
    ),
    "netapi": tuple(
        alias
        for alias in [
            str(alias or "").strip()
            for alias in str(
                os.getenv(
                    "RUNBOOK_LOCAL_IMAGE_ALIASES_NETAPI",
                    "networkapi:latest,netapi:latest,cognus/netapi:latest,"
                    "chaincode-gateway:latest,cognus/chaincode-gateway:latest",
                )
            ).split(",")
        ]
        if alias
    ),
}
RUNBOOK_RUNTIME_IMAGE_SOURCE_DIR_CANDIDATES_BY_NODE_TYPE = {
    "apigateway": tuple(
        source_dir
        for source_dir in [
            str(source_dir or "").strip()
            for source_dir in str(
                os.getenv(
                    "RUNBOOK_RUNTIME_IMAGE_SOURCE_DIRS_APIGATEWAY",
                    "$HOME/UFG-Fabric-Orchestrator-Blockchain/chaincode-gateway,"
                    "/workspace/root-chaincode-gateway,"
                    "/workspace/chaincode-gateway,"
                    "./chaincode-gateway",
                )
            ).split(",")
        ]
        if source_dir
    ),
    "netapi": tuple(
        source_dir
        for source_dir in [
            str(source_dir or "").strip()
            for source_dir in str(
                os.getenv(
                    "RUNBOOK_RUNTIME_IMAGE_SOURCE_DIRS_NETAPI",
                    "$HOME/UFG-Fabric-Orchestrator-Blockchain/chaincode-gateway,"
                    "/workspace/root-chaincode-gateway,"
                    "/workspace/chaincode-gateway,"
                    "./chaincode-gateway",
                )
            ).split(",")
        ]
        if source_dir
    ),
}
RUNBOOK_RUNTIME_PLACEHOLDER_IMAGE_REGEX = re.compile(
    r"^(alpine|busybox|debian|ubuntu)(:|$)", re.IGNORECASE
)
RUNBOOK_BASE_NETWORK_NAME = str(os.getenv("RUNBOOK_BASE_NETWORK_NAME", "cognus-runbook-net")).strip() or "cognus-runbook-net"
RUNBOOK_BASE_CONTAINER_PREFIX = str(os.getenv("RUNBOOK_BASE_CONTAINER_PREFIX", "cognusrb")).strip() or "cognusrb"
RUNBOOK_ZERO_PROVISION_CLEAN_ENABLED = (
    str(os.getenv("RUNBOOK_ZERO_PROVISION_CLEAN_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_PROVISION_RUNTIME_STAGGER_SECONDS = max(
    0,
    int(str(os.getenv("RUNBOOK_PROVISION_RUNTIME_STAGGER_SECONDS", "2")).strip() or "0"),
)
RUNBOOK_RUNTIME_STABILITY_WINDOW_SECONDS = max(
    0,
    int(str(os.getenv("RUNBOOK_RUNTIME_STABILITY_WINDOW_SECONDS", "4")).strip() or "0"),
)
RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS = max(
    5,
    int(
        str(os.getenv("RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS", "20")).strip()
        or "20"
    ),
)
RUNBOOK_RUNTIME_HOST_IMAGE_WARMUP_ENABLED = (
    str(os.getenv("RUNBOOK_RUNTIME_HOST_IMAGE_WARMUP_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_RUNTIME_HOST_IMAGE_WARMUP_MAX_WORKERS = max(
    1,
    int(
        str(os.getenv("RUNBOOK_RUNTIME_HOST_IMAGE_WARMUP_MAX_WORKERS", "3")).strip()
        or "3"
    ),
)
RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED = (
    str(os.getenv("RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)


def _resolve_runtime_image_seed_enabled_node_types():
    configured_value = str(
        os.getenv(
            "RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED_NODE_TYPES",
            "peer,orderer,ca,couch,apigateway,netapi,chaincode",
        )
    ).strip()
    if not configured_value:
        return tuple()

    normalized_tokens = []
    for token in configured_value.split(","):
        normalized_token = _normalize_runtime_component_node_type(token)
        if not normalized_token:
            continue
        if normalized_token == "*":
            return RUNBOOK_SUPPORTED_NODE_TYPES
        normalized_tokens.append(normalized_token)

    enabled_types = tuple(
        node_type
        for node_type in RUNBOOK_SUPPORTED_NODE_TYPES
        if node_type in set(normalized_tokens)
    )
    return enabled_types


RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED_NODE_TYPES = (
    _resolve_runtime_image_seed_enabled_node_types()
)


def _resolve_runtime_image_preseed_node_types():
    configured_value = str(
        os.getenv("RUNBOOK_RUNTIME_IMAGE_PRESEED_NODE_TYPES", "apigateway,netapi,chaincode")
    ).strip()
    if not configured_value:
        return tuple()

    normalized_tokens = []
    for token in configured_value.split(","):
        normalized_token = _normalize_runtime_component_node_type(token)
        if not normalized_token:
            continue
        if normalized_token == "*":
            return RUNBOOK_SUPPORTED_NODE_TYPES
        normalized_tokens.append(normalized_token)

    enabled_types = tuple(
        node_type
        for node_type in RUNBOOK_SUPPORTED_NODE_TYPES
        if node_type in set(normalized_tokens)
    )
    return enabled_types


RUNBOOK_RUNTIME_IMAGE_PRESEED_NODE_TYPES = _resolve_runtime_image_preseed_node_types()
RUNBOOK_RUNTIME_IMAGE_PRESEED_REQUIRED = str(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_PRESEED_REQUIRED", "0")
).strip().lower() in ("1", "true", "yes", "on")
RUNBOOK_RUNTIME_IMAGE_PRESEED_PULL_TIMEOUT_SECONDS = int(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_PRESEED_PULL_TIMEOUT_SECONDS", "210")
)
RUNBOOK_RUNTIME_IMAGE_SEED_BASE_TIMEOUT_SECONDS = int(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_SEED_BASE_TIMEOUT_SECONDS", "900")
)
RUNBOOK_RUNTIME_IMAGE_SEED_MAX_TIMEOUT_SECONDS = int(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_SEED_MAX_TIMEOUT_SECONDS", "3600")
)
RUNBOOK_RUNTIME_IMAGE_SEED_BYTES_PER_SECOND = int(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_SEED_BYTES_PER_SECOND", str(1 * 1024 * 1024))
)
RUNBOOK_RUNTIME_IMAGE_PRESEED_RETRY_COOLDOWN_SECONDS = int(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_PRESEED_RETRY_COOLDOWN_SECONDS", "180")
)
RUNBOOK_RUNTIME_IMAGE_PRESEED_MAX_ATTEMPTS_PER_RUN = int(
    os.getenv("RUNBOOK_RUNTIME_IMAGE_PRESEED_MAX_ATTEMPTS_PER_RUN", "2")
)


def _resolve_runtime_image_preseed_pull_first_node_types():
    configured_value = str(
        os.getenv(
            "RUNBOOK_RUNTIME_IMAGE_PRESEED_PULL_FIRST_NODE_TYPES",
            "apigateway,netapi,chaincode",
        )
    ).strip()
    if not configured_value:
        return tuple()

    normalized_tokens = []
    for token in configured_value.split(","):
        normalized_token = _normalize_runtime_component_node_type(token)
        if not normalized_token:
            continue
        if normalized_token == "*":
            return RUNBOOK_SUPPORTED_NODE_TYPES
        normalized_tokens.append(normalized_token)

    enabled_types = tuple(
        node_type
        for node_type in RUNBOOK_SUPPORTED_NODE_TYPES
        if node_type in set(normalized_tokens)
    )
    return enabled_types


RUNBOOK_RUNTIME_IMAGE_PRESEED_PULL_FIRST_NODE_TYPES = (
    _resolve_runtime_image_preseed_pull_first_node_types()
)
RUNBOOK_MSP_MATERIALIZE_INLINE_MAX_BYTES = max(
    0,
    int(str(os.getenv("RUNBOOK_MSP_MATERIALIZE_INLINE_MAX_BYTES", "4096")).strip() or "4096"),
)
RUNBOOK_RUNTIME_CPU_LIMIT_BY_NODE_TYPE = {
    "peer": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_PEER", "0.75")).strip(),
    "orderer": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_ORDERER", "0.75")).strip(),
    "ca": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_CA", "0.50")).strip(),
    "couch": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_COUCH", "0.50")).strip(),
    "apigateway": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_APIGATEWAY", "0.50")).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_NETAPI", "0.50")).strip(),
    "chaincode": str(os.getenv("RUNBOOK_RUNTIME_CPU_LIMIT_CHAINCODE", "0.25")).strip(),
}
RUNBOOK_RUNTIME_MEMORY_LIMIT_BY_NODE_TYPE = {
    "peer": str(os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_PEER", "768m")).strip(),
    "orderer": str(os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_ORDERER", "768m")).strip(),
    "ca": str(os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_CA", "384m")).strip(),
    "couch": str(os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_COUCH", "512m")).strip(),
    "apigateway": str(
        os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_APIGATEWAY", "384m")
    ).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_NETAPI", "384m")).strip(),
    "chaincode": str(os.getenv("RUNBOOK_RUNTIME_MEMORY_LIMIT_CHAINCODE", "256m")).strip(),
}
RUNBOOK_RUNTIME_MIN_AVAILABLE_MEMORY_MB_BY_NODE_TYPE = {
    "peer": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_PEER", "1024")).strip(),
    "orderer": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_ORDERER", "1024")).strip(),
    "ca": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_CA", "768")).strip(),
    "couch": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_COUCH", "768")).strip(),
    "apigateway": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_APIGATEWAY", "768")).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_NETAPI", "768")).strip(),
    "chaincode": str(os.getenv("RUNBOOK_RUNTIME_MIN_MEMORY_MB_CHAINCODE", "512")).strip(),
}
RUNBOOK_RUNTIME_PIDS_LIMIT_BY_NODE_TYPE = {
    "peer": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_PEER", "512")).strip(),
    "orderer": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_ORDERER", "512")).strip(),
    "ca": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_CA", "256")).strip(),
    "couch": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_COUCH", "256")).strip(),
    "apigateway": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_APIGATEWAY", "256")).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_NETAPI", "256")).strip(),
    "chaincode": str(os.getenv("RUNBOOK_RUNTIME_PIDS_LIMIT_CHAINCODE", "128")).strip(),
}
RUNBOOK_ORDERER_DEV_TLS_CA_B64 = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNIekNDQWNXZ0F3SUJBZ0lVSVg3dU1HWHBmN0ZxVmZBQUR0ZGxjd2JCbDVFd0NnWUlLb1pJemowRUF3SXcKWFRFTE1Ba0dBMVVFQmhNQ1FsSXhDekFKQmdOVkJBZ01Ba2RQTVJJd0VBWURWUVFLREFsVFlXMXdiR1ZQY21jeApEakFNQmdOVkJBc01CV0ZrYldsdU1SMHdHd1lEVlFRRERCUlRZVzF3YkdWUGNtY3RiM0prWlhKbGNpMWpZVEFlCkZ3MHlOakF6TURFd05EUXlNVGhhRncwek5qQXlNamN3TkRReU1UaGFNRjB4Q3pBSkJnTlZCQVlUQWtKU01Rc3cKQ1FZRFZRUUlEQUpIVHpFU01CQUdBMVVFQ2d3SlUyRnRjR3hsVDNKbk1RNHdEQVlEVlFRTERBVmhaRzFwYmpFZApNQnNHQTFVRUF3d1VVMkZ0Y0d4bFQzSm5MVzl5WkdWeVpYSXRZMkV3V1RBVEJnY3Foa2pPUFFJQkJnZ3Foa2pPClBRTUJCd05DQUFRZXBmZFFxSkJRMUlYMVFtYWFIL3MwbHRTaFRYUXZ3c2o3cHJiWVJPU3c3MVVXRUhRNFNMYXQKc2dkVnU1N0I4YUxFbzhNc2M2WHdZaUE4bmtES2JaMWJvMk13WVRBZEJnTlZIUTRFRmdRVVpNbDBTaWlGT3lRVQptN3NsZGlGb1VCT1cyeXd3SHdZRFZSMGpCQmd3Rm9BVVpNbDBTaWlGT3lRVW03c2xkaUZvVUJPVzJ5d3dEd1lEClZSMFRBUUgvQkFVd0F3RUIvekFPQmdOVkhROEJBZjhFQkFNQ0FRWXdDZ1lJS29aSXpqMEVBd0lEU0FBd1JRSWcKQzYwaVVWTWdyTEVpSmpaQ2U2RlNIbUEvY1BqYVV2UFB1Q0x5VEFFSVpOd0NJUURRZk9kZm9jV1lkMEF6R085VwpON2dZT0VSKzcvWUZzNzRHd2FnSDBhVTVIQT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
RUNBOOK_ORDERER_DEV_TLS_CERT_B64 = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNhakNDQWcrZ0F3SUJBZ0lVZXQ3UmJrNGhpbkxXdmlsTDl2MXZHSEtGV29Zd0NnWUlLb1pJemowRUF3SXcKWFRFTE1Ba0dBMVVFQmhNQ1FsSXhDekFKQmdOVkJBZ01Ba2RQTVJJd0VBWURWUVFLREFsVFlXMXdiR1ZQY21jeApEakFNQmdOVkJBc01CV0ZrYldsdU1SMHdHd1lEVlFRRERCUlRZVzF3YkdWUGNtY3RiM0prWlhKbGNpMWpZVEFlCkZ3MHlOakF6TURFd05EUXlNVGhhRncwek5qQXlNamN3TkRReU1UaGFNRm94Q3pBSkJnTlZCQVlUQWtKU01Rc3cKQ1FZRFZRUUlEQUpIVHpFU01CQUdBMVVFQ2d3SlUyRnRjR3hsVDNKbk1RNHdEQVlEVlFRTERBVmhaRzFwYmpFYQpNQmdHQTFVRUF3d1JVMkZ0Y0d4bFQzSm5MVzl5WkdWeVpYSXdXVEFUQmdjcWhrak9QUUlCQmdncWhrak9QUU1CCkJ3TkNBQVE5Skh4K3k0WVdzZG00SVAzeEVSem8zZXl0M0R2dnNxY1ZZYkR2Zlp6QWxxa2Q4aFBCUUpzSlRxVEkKMndQeDF3NVhwczI5NExNbzJWc0dSVmtJWnl2RW80R3ZNSUdzTUF3R0ExVWRFd0VCL3dRQ01BQXdEZ1lEVlIwUApBUUgvQkFRREFnV2dNQjBHQTFVZEpRUVdNQlFHQ0NzR0FRVUZCd01CQmdnckJnRUZCUWNEQWpBdEJnTlZIUkVFCkpqQWtnaEZUWVcxd2JHVlBjbWN0YjNKa1pYSmxjb0lKYkc5allXeG9iM04waHdSL0FBQUJNQjBHQTFVZERnUVcKQkJUZ2ZrbTlpeUFSdkFMSDZSNy93cUV0RGRhN0FEQWZCZ05WSFNNRUdEQVdnQlJreVhSS0tJVTdKQlNidXlWMgpJV2hRRTViYkxEQUtCZ2dxaGtqT1BRUURBZ05KQURCR0FpRUFpM2twYVkzamRHSnNRU0x3aTQwSWFFR0ZuZmZrCkZ4SEpXaXc0c2ZmN3hya0NJUUN0WkdCWmFiQk42WlZKTFZ1V1k4MTcyd3BlT2thN3lqR01CbUs1ZUZlZTVnPT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo="
RUNBOOK_ORDERER_DEV_TLS_KEY_B64 = "LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSUkvV1RTTUp5NlV0aldValpnUlJLa0laY3RGZkV5VmNyd0RYMjcxdFFnRFVvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFUFNSOGZzdUdGckhadUNEOThSRWM2TjNzcmR3Nzc3S25GV0d3NzMyY3dKYXBIZklUd1VDYgpDVTZreU5zRDhkY09WNmJOdmVDektObGJCa1ZaQ0djcnhBPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo="
RUNBOOK_ORDERER_DEV_MSP_CONFIG_B64 = "Tm9kZU9VczoKICBFbmFibGU6IHRydWUKICBDbGllbnRPVUlkZW50aWZpZXI6CiAgICBDZXJ0aWZpY2F0ZTogY2FjZXJ0cy9jYWNlcnQucGVtCiAgICBPcmdhbml6YXRpb25hbFVuaXRJZGVudGlmaWVyOiBjbGllbnQKICBQZWVyT1VJZGVudGlmaWVyOgogICAgQ2VydGlmaWNhdGU6IGNhY2VydHMvY2FjZXJ0LnBlbQogICAgT3JnYW5pemF0aW9uYWxVbml0SWRlbnRpZmllcjogcGVlcgogIEFkbWluT1VJZGVudGlmaWVyOgogICAgQ2VydGlmaWNhdGU6IGNhY2VydHMvY2FjZXJ0LnBlbQogICAgT3JnYW5pemF0aW9uYWxVbml0SWRlbnRpZmllcjogYWRtaW4KICBPcmRlcmVyT1VJZGVudGlmaWVyOgogICAgQ2VydGlmaWNhdGU6IGNhY2VydHMvY2FjZXJ0LnBlbQogICAgT3JnYW5pemF0aW9uYWxVbml0SWRlbnRpZmllcjogYWRtaW4K"
RUNBOOK_CONFIGTX_FALLBACK_B64 = base64.b64encode(
        """
Organizations:
    - &SampleOrg
        Name: SampleOrg
        ID: SampleOrg
        MSPDir: /etc/hyperledger/fabric/msp
        Policies:
            Readers:
                Type: Signature
                Rule: "OR('SampleOrg.member')"
            Writers:
                Type: Signature
                Rule: "OR('SampleOrg.member')"
            Admins:
                Type: Signature
                Rule: "OR('SampleOrg.admin')"
            Endorsement:
                Type: Signature
                Rule: "OR('SampleOrg.member')"
Capabilities:
    Channel: &ChannelCapabilities
        V2_0: true
    Orderer: &OrdererCapabilities
        V2_0: true
    Application: &ApplicationCapabilities
        V2_5: true
Application: &ApplicationDefaults
    Policies:
        Readers:
            Type: ImplicitMeta
            Rule: "ANY Readers"
        Writers:
            Type: ImplicitMeta
            Rule: "ANY Writers"
        Admins:
            Type: ImplicitMeta
            Rule: "MAJORITY Admins"
        Endorsement:
            Type: ImplicitMeta
            Rule: "MAJORITY Endorsement"
    Organizations:
    Capabilities: *ApplicationCapabilities
Orderer: &OrdererDefaults
    OrdererType: etcdraft
    Addresses:
        - SampleOrg-orderer:7050
    EtcdRaft:
        Consenters:
            - Host: SampleOrg-orderer
              Port: 7050
              ClientTLSCert: /etc/hyperledger/fabric/tls/server.crt
              ServerTLSCert: /etc/hyperledger/fabric/tls/server.crt
    BatchTimeout: 2s
    BatchSize:
        MaxMessageCount: 10
        AbsoluteMaxBytes: 99 MB
        PreferredMaxBytes: 512 KB
    Policies:
        Readers:
            Type: ImplicitMeta
            Rule: "ANY Readers"
        Writers:
            Type: ImplicitMeta
            Rule: "ANY Writers"
        Admins:
            Type: ImplicitMeta
            Rule: "MAJORITY Admins"
        BlockValidation:
            Type: ImplicitMeta
            Rule: "ANY Writers"
    Organizations:
        - *SampleOrg
    Capabilities: *OrdererCapabilities
Channel: &ChannelDefaults
    Policies:
        Readers:
            Type: ImplicitMeta
            Rule: "ANY Readers"
        Writers:
            Type: ImplicitMeta
            Rule: "ANY Writers"
        Admins:
            Type: ImplicitMeta
            Rule: "MAJORITY Admins"
    Capabilities: *ChannelCapabilities
Profiles:
    SampleSingleMSPChannel:
        <<: *ChannelDefaults
        Orderer:
            <<: *OrdererDefaults
            Organizations:
                - *SampleOrg
        Application:
            <<: *ApplicationDefaults
            Organizations:
                - *SampleOrg
    SampleSingleMSPSolo:
        <<: *ChannelDefaults
        Orderer:
            <<: *OrdererDefaults
            OrdererType: solo
            Organizations:
                - *SampleOrg
        Application:
            <<: *ApplicationDefaults
            Organizations:
                - *SampleOrg
    SampleDevModeSolo:
        <<: *ChannelDefaults
        Orderer:
            <<: *OrdererDefaults
            OrdererType: solo
            Organizations:
                - *SampleOrg
        Application:
            <<: *ApplicationDefaults
            Organizations:
                - *SampleOrg
""".strip().encode("utf-8")
).decode("ascii")
def _build_runbook_orderer_dev_tls_bootstrap():
    # Keep the resulting shell command single-line so shlex.split preserves it.
    return (
        "sh -c 'set -e; "
        "if lsof -i :7050 2>/dev/null | grep LISTEN; then echo bootstrap: port 7050 busy, attempting cleanup >&2; docker ps -a --format \"{{.ID}} {{.Names}}\" | grep orderer | awk \"{print $1}\" | xargs -r docker rm -f; lsof -i :7050 2>/dev/null | awk \"NR>1 {print $2}\" | xargs -r kill -9; sleep 2; fi; "
        "export ORDERER_GENERAL_LISTENPORT=${ORDERER_GENERAL_LISTENPORT:-7050}; "
        "export ORDERER_GENERAL_CLUSTER_LISTENPORT=${ORDERER_GENERAL_CLUSTER_LISTENPORT:-7051}; "
        "if [ \"$ORDERER_GENERAL_CLUSTER_LISTENPORT\" = \"$ORDERER_GENERAL_LISTENPORT\" ]; then export ORDERER_GENERAL_CLUSTER_LISTENPORT=$((ORDERER_GENERAL_LISTENPORT + 1)); fi; "
        "export ORDERER_GENERAL_LOCALMSPID=${ORDERER_GENERAL_LOCALMSPID:-Org1MSP}; "
        "export ORDERER_GENERAL_LOCALMSPDIR=${ORDERER_GENERAL_LOCALMSPDIR:-/etc/hyperledger/fabric/msp}; "
        "TLS_DIR=/etc/hyperledger/fabric/tls; "
        "MSP_DIR=${ORDERER_GENERAL_LOCALMSPDIR:-/etc/hyperledger/fabric/msp}; "
        "SRC_MSP=/var/lib/cognus/msp; "
        "PROD_DIR=/var/hyperledger/production/orderer; "
        "COGNUS_SAMPLE_MSP=0; "
        "COGNUS_HOST_MSP_READY=0; "
        "mkdir -p \"$TLS_DIR\" \"$MSP_DIR/cacerts\" \"$MSP_DIR/signcerts\" \"$MSP_DIR/keystore\" \"$MSP_DIR/admincerts\" \"$MSP_DIR/tlscacerts\"; "
        "COGNUS_ORDERER_RESET_LEDGER=${COGNUS_ORDERER_RESET_LEDGER:-0}; "
        "COGNUS_ORDERER_LEDGER_PRESENT=0; "
        "if [ -d \"$PROD_DIR/chains\" ] && find \"$PROD_DIR/chains\" -mindepth 1 2>/dev/null | head -n 1 | grep -q .; then COGNUS_ORDERER_LEDGER_PRESENT=1; fi; "
        "if [ \"$COGNUS_ORDERER_LEDGER_PRESENT\" = \"0\" ] && [ -d \"$PROD_DIR/etcdraft\" ] && find \"$PROD_DIR/etcdraft\" -mindepth 1 2>/dev/null | head -n 1 | grep -q .; then COGNUS_ORDERER_LEDGER_PRESENT=1; fi; "
        "if [ \"$COGNUS_ORDERER_RESET_LEDGER\" = \"1\" ]; then rm -rf \"$PROD_DIR/chains\" \"$PROD_DIR/pendingops\" \"$PROD_DIR/etcdraft\" >/dev/null 2>&1 || true; fi; "
        "if [ \"$COGNUS_ORDERER_LEDGER_PRESENT\" = \"1\" ] && [ \"$COGNUS_ORDERER_RESET_LEDGER\" != \"1\" ]; then echo bootstrap: preserving existing orderer ledger >&2; fi; "
        "mkdir -p \"$PROD_DIR/chains\" \"$PROD_DIR/pendingops\" \"$PROD_DIR/etcdraft\" >/dev/null 2>&1 || true; "
        "if [ -d \"$SRC_MSP\" ]; then cp -a \"$SRC_MSP/.\" \"$MSP_DIR/\" >/dev/null 2>&1 || true; fi; "
        "if [ -s \"$SRC_MSP/signcerts/cert.pem\" ] && [ -s \"$SRC_MSP/keystore/key.pem\" ]; then COGNUS_HOST_MSP_READY=1; fi; "
        "if [ \"$COGNUS_HOST_MSP_READY\" != \"1\" ]; then "
        "rm -rf \"$MSP_DIR/signcerts\" \"$MSP_DIR/keystore\" \"$MSP_DIR/admincerts\" \"$MSP_DIR/cacerts\" \"$MSP_DIR/tlscacerts\" \"$MSP_DIR/tlsintermediatecerts\" >/dev/null 2>&1 || true; "
        "mkdir -p \"$MSP_DIR/cacerts\" \"$MSP_DIR/signcerts\" \"$MSP_DIR/keystore\" \"$MSP_DIR/admincerts\" \"$MSP_DIR/tlscacerts\" >/dev/null 2>&1 || true; "
        "rm -f \"$MSP_DIR/config.yaml\" \"$TLS_DIR/ca.crt\" \"$TLS_DIR/server.crt\" \"$TLS_DIR/server.key\" >/dev/null 2>&1 || true; "
        "COGNUS_SAMPLE_MSP=1; "
        "fi; "
        "if [ -f \"$SRC_MSP/.cognus-sample-msp\" ] || [ -f \"$MSP_DIR/.cognus-sample-msp\" ]; then COGNUS_SAMPLE_MSP=1; fi; "
        "if [ ! -s \"$MSP_DIR/signcerts/cert.pem\" ]; then s=$(find \"$MSP_DIR/signcerts\" -type f 2>/dev/null | head -n 1); [ -n \"$s\" ] && cp \"$s\" \"$MSP_DIR/signcerts/cert.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$MSP_DIR/keystore/key.pem\" ]; then k=$(find \"$MSP_DIR/keystore\" -type f 2>/dev/null | head -n 1); [ -n \"$k\" ] && cp \"$k\" \"$MSP_DIR/keystore/key.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$MSP_DIR/config.yaml\" ]; then printf %s \"" + RUNBOOK_ORDERER_DEV_MSP_CONFIG_B64 + "\" | base64 -d > \"$MSP_DIR/config.yaml\"; fi; "
        "if [ ! -s \"$TLS_DIR/ca.crt\" ]; then printf %s \"" + RUNBOOK_ORDERER_DEV_TLS_CA_B64 + "\" | base64 -d > \"$TLS_DIR/ca.crt\"; fi; "
        "if [ ! -s \"$TLS_DIR/server.crt\" ]; then printf %s \"" + RUNBOOK_ORDERER_DEV_TLS_CERT_B64 + "\" | base64 -d > \"$TLS_DIR/server.crt\"; fi; "
        "if [ ! -s \"$TLS_DIR/server.key\" ]; then printf %s \"" + RUNBOOK_ORDERER_DEV_TLS_KEY_B64 + "\" | base64 -d > \"$TLS_DIR/server.key\"; fi; "
        "if [ ! -s \"$MSP_DIR/cacerts/cacert.pem\" ]; then c=$(find \"$MSP_DIR/cacerts\" \"$MSP_DIR/tlscacerts\" -type f 2>/dev/null | head -n 1); if [ -n \"$c\" ]; then cp \"$c\" \"$MSP_DIR/cacerts/cacert.pem\" >/dev/null 2>&1 || true; else cp \"$TLS_DIR/ca.crt\" \"$MSP_DIR/cacerts/cacert.pem\" >/dev/null 2>&1 || true; fi; fi; "
        "if [ ! -s \"$MSP_DIR/tlscacerts/tlsca.pem\" ]; then c=$(find \"$MSP_DIR/tlscacerts\" \"$MSP_DIR/cacerts\" -type f 2>/dev/null | head -n 1); if [ -n \"$c\" ]; then cp \"$c\" \"$MSP_DIR/tlscacerts/tlsca.pem\" >/dev/null 2>&1 || true; else cp \"$TLS_DIR/ca.crt\" \"$MSP_DIR/tlscacerts/tlsca.pem\" >/dev/null 2>&1 || true; fi; fi; "
        "if [ ! -s \"$MSP_DIR/signcerts/cert.pem\" ]; then cp \"$TLS_DIR/server.crt\" \"$MSP_DIR/signcerts/cert.pem\" >/dev/null 2>&1 || true; COGNUS_SAMPLE_MSP=1; fi; "
        "if [ ! -s \"$MSP_DIR/keystore/key.pem\" ]; then cp \"$TLS_DIR/server.key\" \"$MSP_DIR/keystore/key.pem\" >/dev/null 2>&1 || true; COGNUS_SAMPLE_MSP=1; fi; "
        "if [ ! -s \"$MSP_DIR/admincerts/admincert.pem\" ] && [ -s \"$MSP_DIR/signcerts/cert.pem\" ]; then cp \"$MSP_DIR/signcerts/cert.pem\" \"$MSP_DIR/admincerts/admincert.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ \"$COGNUS_SAMPLE_MSP\" = \"1\" ]; then touch \"$MSP_DIR/.cognus-sample-msp\" >/dev/null 2>&1 || true; touch \"$SRC_MSP/.cognus-sample-msp\" >/dev/null 2>&1 || true; export ORDERER_GENERAL_LOCALMSPID=SampleOrg; fi; "
        "chmod 600 \"$TLS_DIR/server.key\" \"$MSP_DIR/keystore/key.pem\" >/dev/null 2>&1 || true; "
        "chmod 644 \"$TLS_DIR/server.crt\" \"$TLS_DIR/ca.crt\" \"$MSP_DIR/config.yaml\" \"$MSP_DIR/signcerts/cert.pem\" \"$MSP_DIR/cacerts/cacert.pem\" \"$MSP_DIR/admincerts/admincert.pem\" \"$MSP_DIR/tlscacerts/tlsca.pem\" >/dev/null 2>&1 || true; "
        "for f in \"$MSP_DIR/signcerts/cert.pem\" \"$MSP_DIR/cacerts/cacert.pem\" \"$MSP_DIR/keystore/key.pem\" \"$MSP_DIR/admincerts/admincert.pem\" \"$MSP_DIR/tlscacerts/tlsca.pem\" \"$MSP_DIR/config.yaml\" \"$TLS_DIR/ca.crt\" \"$TLS_DIR/server.crt\" \"$TLS_DIR/server.key\"; do test -s \"$f\" || { echo bootstrap: empty file $f >&2; exit 27; }; done; "
        "wc -c \"$MSP_DIR/signcerts/cert.pem\" \"$MSP_DIR/cacerts/cacert.pem\" \"$MSP_DIR/keystore/key.pem\" \"$MSP_DIR/admincerts/admincert.pem\" \"$MSP_DIR/tlscacerts/tlsca.pem\" \"$MSP_DIR/config.yaml\" \"$TLS_DIR/ca.crt\" \"$TLS_DIR/server.crt\" \"$TLS_DIR/server.key\"; "
        "exec orderer'"
    )


def _build_runbook_peer_dev_msp_bootstrap():
    return (
        "sh -c 'set -e; "
        "export CORE_PEER_MSPCONFIGPATH=${CORE_PEER_MSPCONFIGPATH:-/etc/hyperledger/fabric/msp}; "
        "PEER_MSP_DIR=${CORE_PEER_MSPCONFIGPATH:-/etc/hyperledger/fabric/msp}; "
        "mkdir -p \"$PEER_MSP_DIR/signcerts\" \"$PEER_MSP_DIR/keystore\" \"$PEER_MSP_DIR/admincerts\" \"$PEER_MSP_DIR/cacerts\" \"$PEER_MSP_DIR/tlscacerts\" >/dev/null 2>&1 || true; "
        "if [ ! -s \"$PEER_MSP_DIR/signcerts/cert.pem\" ] && [ -s \"$PEER_MSP_DIR/signcerts/peer.pem\" ]; then cp \"$PEER_MSP_DIR/signcerts/peer.pem\" \"$PEER_MSP_DIR/signcerts/cert.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$PEER_MSP_DIR/keystore/key.pem\" ]; then k=$(find \"$PEER_MSP_DIR/keystore\" -type f 2>/dev/null | head -n 1); [ -n \"$k\" ] && cp \"$k\" \"$PEER_MSP_DIR/keystore/key.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$PEER_MSP_DIR/admincerts/admincert.pem\" ] && [ -s \"$PEER_MSP_DIR/signcerts/cert.pem\" ]; then cp \"$PEER_MSP_DIR/signcerts/cert.pem\" \"$PEER_MSP_DIR/admincerts/admincert.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ -f \"$PEER_MSP_DIR/.cognus-sample-msp\" ] || [ -f /var/lib/cognus/msp/.cognus-sample-msp ] || [ -s \"$PEER_MSP_DIR/signcerts/peer.pem\" ] || [ -s /var/lib/cognus/msp/signcerts/peer.pem ]; then export CORE_PEER_LOCALMSPID=SampleOrg; fi; "
        "exec peer node start'"
    )


# Dev fallback to self-generate TLS/MSP when no artifacts were mounted (uses embedded self-signed materials, avoids openssl dependency in the image).
# Important: keep this command single-line and free of heredocs so shlex.split in _resolve_runtime_docker_run_command_override_args does not strip required newlines.
RUNBOOK_PEER_DEV_MSP_BOOTSTRAP = _build_runbook_peer_dev_msp_bootstrap()
RUNBOOK_ORDERER_DEV_TLS_BOOTSTRAP = _build_runbook_orderer_dev_tls_bootstrap()
RUNBOOK_RUNTIME_COMMAND_OVERRIDE_BY_NODE_TYPE = {
    "peer": (
        str(os.getenv("RUNBOOK_RUNTIME_COMMAND_PEER", "")).strip()
        or RUNBOOK_PEER_DEV_MSP_BOOTSTRAP
    ),
    "orderer": (
        str(os.getenv("RUNBOOK_RUNTIME_COMMAND_ORDERER", "")).strip()
        or RUNBOOK_ORDERER_DEV_TLS_BOOTSTRAP
    ),
    "ca": (
        str(os.getenv("RUNBOOK_RUNTIME_COMMAND_CA", "")).strip()
        or "sh -c 'mkdir -p /etc/hyperledger/fabric-ca-server && fabric-ca-server start -b admin:adminpw'"
    ),
    "couch": str(os.getenv("RUNBOOK_RUNTIME_COMMAND_COUCH", "")).strip(),
    "apigateway": str(os.getenv("RUNBOOK_RUNTIME_COMMAND_APIGATEWAY", "")).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_COMMAND_NETAPI", "")).strip(),
    "chaincode": str(
        os.getenv("RUNBOOK_RUNTIME_COMMAND_CHAINCODE", "tail -f /dev/null")
    ).strip(),
}
RUNBOOK_RUNTIME_COMMAND_OVERRIDE_MAX_BYTES = max(
    1024,
    int(str(os.getenv("RUNBOOK_RUNTIME_COMMAND_OVERRIDE_MAX_BYTES", "16384")).strip() or "16384"),
)
RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE = {
    "apigateway": str(
        os.getenv("RUNBOOK_RUNTIME_CONTAINER_PORT_APIGATEWAY", "8085")
    ).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_CONTAINER_PORT_NETAPI", "3000")).strip(),
}
RUNBOOK_RUNTIME_DEFAULT_HOST_PORT_BY_NODE_TYPE = {
    "apigateway": str(os.getenv("RUNBOOK_RUNTIME_HOST_PORT_APIGATEWAY", "8443")).strip(),
    "netapi": str(os.getenv("RUNBOOK_RUNTIME_HOST_PORT_NETAPI", "3000")).strip(),
}
RUNBOOK_RUNTIME_HOST_CRYPTO_ROOT = (
    str(os.getenv("RUNBOOK_RUNTIME_HOST_CRYPTO_ROOT", "/tmp/cognus/crypto")).strip()
    or "/tmp/cognus/crypto"
)
RUNBOOK_CHAINCODE_LIFECYCLE_GUARD_ENABLED = (
    str(os.getenv("RUNBOOK_CHAINCODE_LIFECYCLE_GUARD_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_CHAINCODE_CHANNEL_AUTO_REPAIR_ENABLED = (
    str(os.getenv("RUNBOOK_CHAINCODE_CHANNEL_AUTO_REPAIR_ENABLED", "true"))
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_VERIFY_CONSISTENCY_STRICT = (
    str(os.getenv("RUNBOOK_VERIFY_CONSISTENCY_STRICT", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_CHAINCODE_AUTODEFINE_ENABLED = (
    str(os.getenv("RUNBOOK_CHAINCODE_AUTODEFINE_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_CHAINCODE_AUTODEFINE_VERSION = (
    str(os.getenv("RUNBOOK_CHAINCODE_AUTODEFINE_VERSION", "1.0")).strip() or "1.0"
)
RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE = (
    str(os.getenv("RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE", "1")).strip() or "1"
)
RUNBOOK_CHAINCODE_RUNTIME_MODE = (
    str(os.getenv("RUNBOOK_CHAINCODE_RUNTIME_MODE", "auto")).strip().lower()
    or "auto"
)
RUNBOOK_CHAINCODE_CCAAS_SERVICE_PORT = max(
    1,
    int(str(os.getenv("RUNBOOK_CHAINCODE_CCAAS_SERVICE_PORT", "9999")).strip() or "9999"),
)
RUNBOOK_CHAINCODE_CCAAS_DIAL_TIMEOUT = (
    str(os.getenv("RUNBOOK_CHAINCODE_CCAAS_DIAL_TIMEOUT", "10s")).strip() or "10s"
)
RUNBOOK_CHAINCODE_CCAAS_READY_WAIT_SECONDS = max(
    1,
    int(
        str(os.getenv("RUNBOOK_CHAINCODE_CCAAS_READY_WAIT_SECONDS", "20")).strip()
        or "20"
    ),
)
RUNBOOK_CHAINCODE_CCAAS_READY_STABLE_POLLS = max(
    1,
    int(
        str(os.getenv("RUNBOOK_CHAINCODE_CCAAS_READY_STABLE_POLLS", "2")).strip()
        or "2"
    ),
)
RUNBOOK_CHAINCODE_CCAAS_BUILD_GO_IMAGE = (
    str(os.getenv("RUNBOOK_CHAINCODE_CCAAS_BUILD_GO_IMAGE", "golang:1.22")).strip()
    or "golang:1.22"
)
RUNBOOK_CHAINCODE_CCAAS_RUNTIME_BASE_IMAGE = (
    str(
        os.getenv(
            "RUNBOOK_CHAINCODE_CCAAS_RUNTIME_BASE_IMAGE",
            "alpine:3.20",
        )
    ).strip()
    or "alpine:3.20"
)
RUNBOOK_ORDERER_FORCE_CHANNEL_PARTICIPATION = (
    str(os.getenv("RUNBOOK_ORDERER_FORCE_CHANNEL_PARTICIPATION", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_ORDERER_BOOTSTRAP_METHOD = (
    str(os.getenv("RUNBOOK_ORDERER_BOOTSTRAP_METHOD", "none")).strip() or "none"
)
RUNBOOK_ORDERER_CHANNEL_PARTICIPATION_ENABLED = (
    str(os.getenv("RUNBOOK_ORDERER_CHANNEL_PARTICIPATION_ENABLED", "true")).strip().lower()
    in {"1", "true", "yes", "on"}
)
RUNBOOK_ORDERER_TLS_HOST_OVERRIDE = (
    str(os.getenv("RUNBOOK_ORDERER_TLS_HOST_OVERRIDE", "localhost")).strip()
    or "localhost"
)
RUNBOOK_ORDERER_LISTEN_ADDRESS = (
    str(os.getenv("RUNBOOK_ORDERER_LISTEN_ADDRESS", "0.0.0.0")).strip() or "0.0.0.0"
)
RUNBOOK_COUCHDB_ADMIN_USER = (
    str(os.getenv("RUNBOOK_COUCHDB_ADMIN_USER", "couchdb")).strip() or "couchdb"
)
RUNBOOK_COUCHDB_ADMIN_PASSWORD = (
    str(os.getenv("RUNBOOK_COUCHDB_ADMIN_PASSWORD", "couchdb")).strip() or "couchdb"
)
RUNBOOK_ALLOWED_ROLES_BY_ACTION = {
    "catalog": {"admin", "operator", "user"},
    "preflight": {"admin", "operator"},
    "start": {"admin", "operator"},
    "operate": {"admin", "operator"},
    "status": {"admin", "operator", "user"},
    "runtime-inspection": {"admin", "operator", "user"},
}

MINIMUM_RUN_ARTIFACT_KEYS = [
    "pipeline-report:pipeline-report.json",
    "inventory-final:inventory-final.json",
    "history:history.jsonl",
    "decision-trace:decision-trace.jsonl",
]

A2A_ENTRY_REQUIRED_ARTIFACT_KEYS = [
    "inventory-final",
    "verify-report",
    "stage-reports",
    "ssh-execution-log",
]

A2A_RUNTIME_TELEMETRY_CONTRACT_VERSION = "a2a-runtime-telemetry.v1"
A2A_ORGANIZATION_READ_MODEL_CONTRACT_VERSION = "a2a-organization-read-model.v1"
A2A_RUNTIME_INSPECTION_CONTRACT_VERSION = "a2a-runtime-inspection-cache.v1"
A2A_RUNTIME_INSPECTION_SUPPORTED_SCOPES = (
    "docker_inspect",
    "docker_logs",
    "environment",
    "ports",
    "mounts",
)
A2A_RUNTIME_INSPECTION_DEFAULT_TTL_SECONDS = max(
    15,
    int(str(os.getenv("RUNBOOK_RUNTIME_INSPECTION_DEFAULT_TTL_SECONDS", "120") or "120").strip() or 120),
)
A2A_RUNTIME_INSPECTION_LOG_TTL_SECONDS = max(
    15,
    int(str(os.getenv("RUNBOOK_RUNTIME_INSPECTION_LOG_TTL_SECONDS", "60") or "60").strip() or 60),
)
A2A_RUNTIME_INSPECTION_LOG_TAIL_LINES = max(
    20,
    int(str(os.getenv("RUNBOOK_RUNTIME_INSPECTION_LOG_TAIL_LINES", "120") or "120").strip() or 120),
)
A2A_RUNTIME_INSPECTION_TRAIL_LIMIT = max(
    10,
    int(str(os.getenv("RUNBOOK_RUNTIME_INSPECTION_TRAIL_LIMIT", "80") or "80").strip() or 80),
)
A2A_RUNTIME_DEFAULT_SCOPE_BY_COMPONENT_TYPE = {
    "peer": "required",
    "orderer": "required",
    "ca": "required",
    "couch": "required",
    "api_gateway": "required",
    "network_api": "required",
    "chaincode_runtime": "optional",
}
A2A_RUNTIME_DEFAULT_CRITICALITY_BY_COMPONENT_TYPE = {
    "peer": "critical",
    "orderer": "critical",
    "ca": "critical",
    "couch": "supporting",
    "api_gateway": "supporting",
    "network_api": "supporting",
    "chaincode_runtime": "supporting",
}
A2A_RUNTIME_DEFAULT_CONTAINER_PORTS_BY_COMPONENT_TYPE = {
    "peer": 7051,
    "orderer": 7050,
    "ca": 7054,
    "couch": 5984,
}
RUNTIME_TELEMETRY_SENSITIVE_ENV_KEY_REGEX = re.compile(
    r"(password|secret|token|private[_-]?key|credential|passphrase)",
    re.IGNORECASE,
)
RUNTIME_INSPECTION_SENSITIVE_ASSIGNMENT_REGEX = re.compile(
    r"((?:password|secret|token|private[_-]?key|credential|passphrase)[a-z0-9_\-]*\s*=\s*)([^\s,;]+)",
    re.IGNORECASE,
)
RUNTIME_INSPECTION_SENSITIVE_FLAG_REGEX = re.compile(
    r"((?:--(?:password|secret|token|passphrase|private-key|credential)[=\s]+))([^\s]+)",
    re.IGNORECASE,
)
RUNTIME_INSPECTION_IMAGE_PATTERN_BY_NODE_TYPE = {
    "peer": "fabric-peer",
    "orderer": "fabric-orderer",
    "ca": "fabric-ca|fabric-ca-server",
    "couch": "couchdb|couch",
    "apigateway": "chaincode-gateway|ccapi|apigateway",
    "netapi": "networkapi|netapi|chaincode-gateway",
}

RUNBOOK_EVENT_LEVELS = {"info", "warning", "error"}
RUNBOOK_EVENT_CLASSIFICATION_BY_LEVEL = {
    "info": "informational",
    "warning": "transient",
    "error": "critical",
}

SECURE_REFERENCE_PREFIX_REGEX = re.compile(r"^(vault|secret|ref|ssm|kms|env|keyring):\/\/[^\s]+$", re.IGNORECASE)
SECURE_REFERENCE_SHORT_REF_REGEX = re.compile(r"^ref:[a-z0-9][a-z0-9/_-]*$", re.IGNORECASE)
LOCAL_FILE_REFERENCE_REGEX = re.compile(r"^local-file:[^\s]+$", re.IGNORECASE)
RUNBOOK_EVENT_CAUSE_BY_LEVEL = {
    "info": "Evento operacional informativo emitido pelo backend oficial.",
    "warning": "Condicao transitoria identificada no pipeline oficial.",
    "error": "Falha tecnica detectada pelo pipeline oficial.",
}
RUNBOOK_EVENT_IMPACT_BY_LEVEL = {
    "info": "Sem impacto bloqueante para continuidade operacional.",
    "warning": "Pode impactar continuidade da etapa atual se nao tratado.",
    "error": "Bloqueia continuidade da execucao ate correcao da causa.",
}
RUNBOOK_EVENT_RECOMMENDED_ACTION_BY_LEVEL = {
    "info": "Sem acao imediata.",
    "warning": "Analisar evidencias da etapa e confirmar estabilidade antes de avancar.",
    "error": "Corrigir causa tecnica reportada e executar retry da etapa/checkpoint.",
}


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_stage(stage_key, stage_order, checkpoint_labels):
    checkpoints = []
    for checkpoint_index, checkpoint_label in enumerate(checkpoint_labels):
        checkpoint_key = "{}.{}".format(stage_key, checkpoint_label.lower())
        checkpoints.append(
            {
                "key": checkpoint_key,
                "label": checkpoint_label,
                "order": checkpoint_index + 1,
                "status": "pending",
                "started_at_utc": "",
                "completed_at_utc": "",
            }
        )

    return {
        "key": stage_key,
        "label": stage_key,
        "order": stage_order,
        "status": "pending",
        "checkpoints": checkpoints,
        "failure": None,
    }


def _build_runbook_stages():
    return [
        _build_stage("prepare", 1, ["preflight", "connectivity"]),
        _build_stage("provision", 2, ["hosts", "runtime"]),
        _build_stage("configure", 3, ["artifacts", "policies"]),
        _build_stage("verify", 4, ["consistency", "evidence"]),
    ]


def _migrate_legacy_runbook_store_if_needed():
    current_store_file = Path(RUNBOOK_STORE_FILE)
    legacy_store_file = Path(RUNBOOK_LEGACY_STORE_FILE)
    if current_store_file == legacy_store_file:
        return
    if current_store_file.exists() or not legacy_store_file.exists():
        return

    try:
        current_store_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(str(legacy_store_file), str(current_store_file))
        LOG.info(
            "Runbook store migrated from legacy path %s to durable path %s",
            legacy_store_file,
            current_store_file,
        )
    except Exception:
        LOG.warning(
            "Failed to migrate legacy runbook store from %s to %s",
            legacy_store_file,
            current_store_file,
            exc_info=True,
        )


def _load_store():
    def _default_store():
        return {
            "store_version": RUNBOOK_STORE_SCHEMA_VERSION,
            "runs": {},
            "checkpoints": {},
            "artifacts": {},
            "resource_locks": {},
            "access_audit": [],
        }

    with RUNBOOK_STORE_LOCK:
        _migrate_legacy_runbook_store_if_needed()
        if not RUNBOOK_STORE_FILE.exists():
            return _default_store()

        raw_payload = RUNBOOK_STORE_FILE.read_text(encoding="utf-8")
        if not raw_payload.strip():
            return _default_store()

        parsed_payload = json.loads(raw_payload)
        if not isinstance(parsed_payload, dict):
            return _default_store()

        runs = parsed_payload.get("runs", {})
        if not isinstance(runs, dict):
            runs = {}

        checkpoints = parsed_payload.get("checkpoints", {})
        if not isinstance(checkpoints, dict):
            checkpoints = {}

        artifacts = parsed_payload.get("artifacts", {})
        if not isinstance(artifacts, dict):
            artifacts = {}

        resource_locks = parsed_payload.get("resource_locks", {})
        if not isinstance(resource_locks, dict):
            resource_locks = {}

        access_audit = parsed_payload.get("access_audit", [])
        if not isinstance(access_audit, list):
            access_audit = []

        return {
            "store_version": parsed_payload.get(
                "store_version", RUNBOOK_STORE_SCHEMA_VERSION
            ),
            "runs": runs,
            "checkpoints": checkpoints,
            "artifacts": artifacts,
            "resource_locks": resource_locks,
            "access_audit": access_audit,
        }


def _save_store(store_payload):
    with RUNBOOK_STORE_LOCK:
        _migrate_legacy_runbook_store_if_needed()
        RUNBOOK_STORE_FILE.parent.mkdir(parents=True, exist_ok=True)
        serialized_payload = json.dumps(store_payload, ensure_ascii=True, indent=2)
        temp_file = RUNBOOK_STORE_FILE.with_suffix(
            ".{}.tmp".format(uuid.uuid4().hex)
        )
        temp_file.write_text(serialized_payload, encoding="utf-8")
        os.replace(str(temp_file), str(RUNBOOK_STORE_FILE))


def _sha256_payload(payload):
    canonical_payload = json.dumps(
        payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _redact_sensitive_host_fields(host_row):
    if not isinstance(host_row, dict):
        return {}

    sensitive_keys = {
        "private_key",
        "private_key_pem",
        "ssh_private_key",
        "ssh_key",
        "password",
        "passphrase",
        "private_key_ref",
        "vault_ref",
        "secret_ref",
        "credential_ref",
        "token_ref",
        "credential_payload",
    }
    redacted = {}
    for key, value in host_row.items():
        if key in sensitive_keys:
            redacted[key] = "[REDACTED]"
            continue
        redacted[key] = value
    return redacted


def _sanitize_host_mapping_collection(host_mapping):
    if not isinstance(host_mapping, list):
        return []
    sanitized_rows = []
    for host_row in host_mapping:
        sanitized_host_row = _redact_sensitive_host_fields(host_row)
        if not isinstance(sanitized_host_row, dict):
            sanitized_rows.append(sanitized_host_row)
            continue
        normalized_host_row, _ = _normalize_host_ssh_target(sanitized_host_row)
        sanitized_rows.append(normalized_host_row)
    return sanitized_rows


def _sanitize_machine_credentials_collection(machine_credentials):
    if not isinstance(machine_credentials, list):
        return []
    sanitized_rows = []
    for credential_row in machine_credentials:
        identity_file = _materialize_identity_file(credential_row)
        sanitized_row = _redact_sensitive_host_fields(credential_row)
        if not isinstance(sanitized_row, dict):
            sanitized_rows.append(sanitized_row)
            continue
        sanitized_rows.append(
            {
                "machine_id": str(sanitized_row.get("machine_id", "") or "").strip(),
                "credential_ref": str(sanitized_row.get("credential_ref", "") or "").strip(),
                "credential_fingerprint": str(
                    sanitized_row.get("credential_fingerprint", "") or ""
                ).strip(),
                "reuse_confirmed": bool(sanitized_row.get("reuse_confirmed", False)),
                "identity_file": identity_file,
            }
        )
    return sanitized_rows


def _resolve_machine_id_from_host_row(host_row):
    if not isinstance(host_row, dict):
        return ""

    return str(
        host_row.get("host_ref")
        or host_row.get("node_id")
        or host_row.get("host_address")
        or ""
    ).strip()


def _is_secure_credential_reference(value):
    normalized = str(value or "").strip()
    if not normalized:
        return False
    if " " in normalized:
        return False
    if SECURE_REFERENCE_PREFIX_REGEX.match(normalized):
        return True
    if SECURE_REFERENCE_SHORT_REF_REGEX.match(normalized):
        return True
    return bool(LOCAL_FILE_REFERENCE_REGEX.match(normalized))


def _materialize_identity_file(machine_credential):
    if not isinstance(machine_credential, dict):
        return ""

    existing_identity_file = str(
        machine_credential.get("identity_file", "") or ""
    ).strip()
    if existing_identity_file and Path(existing_identity_file).is_file():
        return existing_identity_file

    credential_ref = str(machine_credential.get("credential_ref", "") or "").strip()
    credential_payload = machine_credential.get("credential_payload")
    if not credential_ref or not _is_secure_credential_reference(credential_ref):
        return ""

    if not credential_ref.lower().startswith("local-file:"):
        return ""

    payload_text = str(credential_payload or "").strip()
    if payload_text.lower().startswith("data:") and "," in payload_text:
        payload_text = payload_text.split(",", 1)[1].strip()
    if not payload_text:
        return ""

    try:
        decoded_bytes = base64.b64decode(payload_text, validate=True)
        decoded_text = decoded_bytes.decode("utf-8-sig")
    except Exception:
        decoded_text = payload_text

    normalized_key_text = str(decoded_text or "").replace("\r\n", "\n").replace("\r", "\n")

    key_dir = Path(gettempdir()) / "cognus_ssh_keys"
    key_dir.mkdir(parents=True, exist_ok=True)

    machine_id = str(machine_credential.get("machine_id", "") or uuid.uuid4().hex[:12]).strip()
    identity_path = key_dir / f"{machine_id}.pem"
    identity_path.write_text(normalized_key_text.strip() + "\n", encoding="utf-8")
    try:
        os.chmod(identity_path, 0o600)
    except Exception:
        LOG.warning("Failed to chmod identity file for %s", machine_id)

    return str(identity_path)


def _ensure_machine_credential_identity_file(credential_by_machine, machine_id):
    normalized_machine_id = str(machine_id or "").strip()
    if not normalized_machine_id:
        return ""

    credential_row = credential_by_machine.get(normalized_machine_id, {})
    if not isinstance(credential_row, dict):
        return ""

    identity_file = str(credential_row.get("identity_file", "") or "").strip()
    if identity_file and Path(identity_file).is_file():
        return identity_file

    identity_file = _materialize_identity_file(credential_row)
    if identity_file:
        updated_row = dict(credential_row)
        updated_row["identity_file"] = identity_file
        credential_by_machine[normalized_machine_id] = updated_row
    return str(identity_file or "").strip()


def _build_ssh_command(host_address, ssh_user, ssh_port, remote_command, identity_file=""):
    command = [
        "ssh",
        "-p",
        str(ssh_port),
    ]

    normalized_identity_file = str(identity_file or "").strip()
    if normalized_identity_file:
        # Force SSH to use only the uploaded identity and avoid agent/config side effects.
        command.extend(
            [
                "-i",
                normalized_identity_file,
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "PreferredAuthentications=publickey",
                "-o",
                "PasswordAuthentication=no",
                "-o",
                "KbdInteractiveAuthentication=no",
            ]
        )

    control_path = ""
    if RUNBOOK_SSH_CONTROLMASTER_ENABLED and RUNBOOK_SSH_CONTROL_PERSIST_SECONDS > 0:
        control_material = "|".join(
            [
                str(ssh_user or "").strip().lower(),
                str(host_address or "").strip().lower(),
                str(ssh_port or "").strip(),
                str(normalized_identity_file or "").strip(),
            ]
        )
        control_token = hashlib.sha256(control_material.encode("utf-8")).hexdigest()[:24]
        control_dir = Path(RUNBOOK_SSH_CONTROL_PATH_DIR)
        try:
            control_dir.mkdir(parents=True, exist_ok=True)
            control_path = str(control_dir / f"mux-{control_token}")
        except Exception:
            control_path = ""

    command.extend(
        [
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "UserKnownHostsFile={}".format(RUNBOOK_SSH_KNOWN_HOSTS_FILE),
            "-o",
            "ConnectTimeout={}".format(RUNBOOK_SSH_TIMEOUT_SECONDS),
        ]
    )
    if control_path:
        command.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                "ControlPersist={}s".format(RUNBOOK_SSH_CONTROL_PERSIST_SECONDS),
                "-o",
                "ControlPath={}".format(control_path),
            ]
        )
    command.extend(
        [
            "{}@{}".format(ssh_user, host_address),
            remote_command,
        ]
    )
    return command


def _build_ssh_stdin_wrapper_command():
    return (
        "COGNUS_SSH_STDIN_SCRIPT=$(mktemp /tmp/cognus-ssh-script.XXXXXX) || exit 127; "
        "cat > \"$COGNUS_SSH_STDIN_SCRIPT\" || { COGNUS_SSH_STDIN_RC=$?; rm -f \"$COGNUS_SSH_STDIN_SCRIPT\"; exit $COGNUS_SSH_STDIN_RC; }; "
        "COGNUS_SSH_SCRIPT_SHELL=sh; "
        "if command -v bash >/dev/null 2>&1; then COGNUS_SSH_SCRIPT_SHELL=bash; fi; "
        "\"$COGNUS_SSH_SCRIPT_SHELL\" \"$COGNUS_SSH_STDIN_SCRIPT\"; "
        "COGNUS_SSH_STDIN_RC=$?; "
        "rm -f \"$COGNUS_SSH_STDIN_SCRIPT\"; "
        "exit $COGNUS_SSH_STDIN_RC"
    )


def _should_stream_remote_command_via_stdin(remote_command):
    command_text = str(remote_command or "")
    try:
        command_size = len(command_text.encode("utf-8"))
    except Exception:
        command_size = len(command_text)
    return command_size > RUNBOOK_SSH_INLINE_COMMAND_MAX_BYTES


def _resolve_checkpoint_ssh_timeout(stage_key, checkpoint_key):
    ssh_timeout = RUNBOOK_SSH_TIMEOUT_SECONDS + 3
    if stage_key == "provision" and checkpoint_key == "provision.runtime":
        ssh_timeout = max(ssh_timeout, RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS)
    if stage_key == "configure" and checkpoint_key == "configure.artifacts":
        ssh_timeout = max(ssh_timeout, RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS)
    return ssh_timeout


def _runtime_image_seed_enabled_for_node_type(node_type):
    if not RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED:
        return False
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    if not normalized_node_type:
        return False
    if not RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED_NODE_TYPES:
        return False
    return normalized_node_type in RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED_NODE_TYPES


def _get_local_docker_sdk_client():
    if docker_sdk is None:
        return None
    try:
        client = docker_sdk.from_env()
        client.ping()
        return client
    except Exception:
        return None


def _docker_cli_available():
    return bool(shutil.which("docker"))


def _resolve_local_runtime_image_size_bytes(runtime_image):
    normalized_runtime_image = str(runtime_image or "").strip()
    if not normalized_runtime_image:
        return 0
    if _docker_cli_available():
        try:
            inspect_result = subprocess.run(
                ["docker", "image", "inspect", "--format", "{{.Size}}", normalized_runtime_image],
                capture_output=True,
                text=True,
                timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 5,
            )
        except Exception:
            inspect_result = None
        if inspect_result and inspect_result.returncode == 0:
            try:
                return max(
                    0,
                    int(
                        str(inspect_result.stdout or "")
                        .strip()
                        .splitlines()[-1]
                        .strip()
                        or "0"
                    ),
                )
            except (TypeError, ValueError, IndexError):
                pass

    docker_client = _get_local_docker_sdk_client()
    if docker_client is None:
        return 0
    try:
        image = docker_client.images.get(normalized_runtime_image)
        image_attrs = getattr(image, "attrs", {}) or {}
        return max(0, int(image_attrs.get("Size") or 0))
    except Exception:
        return 0


def _resolve_runtime_image_seed_timeout_seconds(runtime_image):
    runtime_image_size_bytes = _resolve_local_runtime_image_size_bytes(runtime_image)
    base_timeout_seconds = max(
        RUNBOOK_RUNTIME_IMAGE_SEED_BASE_TIMEOUT_SECONDS,
        RUNBOOK_SSH_TIMEOUT_SECONDS + 120,
        180,
    )
    max_timeout_seconds = max(
        base_timeout_seconds,
        RUNBOOK_RUNTIME_IMAGE_SEED_MAX_TIMEOUT_SECONDS,
    )
    min_bytes_per_second = max(256 * 1024, RUNBOOK_RUNTIME_IMAGE_SEED_BYTES_PER_SECOND)

    if runtime_image_size_bytes <= 0:
        return base_timeout_seconds, 0

    estimated_timeout = int(runtime_image_size_bytes / float(min_bytes_per_second)) + 120
    resolved_timeout = min(max(base_timeout_seconds, estimated_timeout), max_timeout_seconds)
    return resolved_timeout, runtime_image_size_bytes


def _ensure_local_runtime_image_available(node_type, runtime_image):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    normalized_runtime_image = str(runtime_image or "").strip()
    if not normalized_runtime_image:
        return {
            "status": "failed",
            "reason": "runtime_image_empty",
        }

    def _inspect_local_image(image_name):
        if _docker_cli_available():
            try:
                inspect_result = subprocess.run(
                    ["docker", "image", "inspect", image_name],
                    capture_output=True,
                    text=True,
                    timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 3,
                )
            except Exception as inspect_error:
                return {
                    "available": False,
                    "reason": "inspect_failed",
                    "stderr": str(inspect_error)[:512],
                    "exit_code": 1,
                }
            return {
                "available": inspect_result.returncode == 0,
                "reason": "inspect_ok" if inspect_result.returncode == 0 else "inspect_missing",
                "stderr": str(inspect_result.stderr or "").strip()[:512],
                "exit_code": inspect_result.returncode,
            }

        docker_client = _get_local_docker_sdk_client()
        if docker_client is None:
            return {
                "available": False,
                "reason": "docker_client_unavailable",
                "stderr": "docker_cli_missing_and_sdk_unavailable",
                "exit_code": 1,
            }
        try:
            docker_client.images.get(image_name)
        except Exception as inspect_error:
            return {
                "available": False,
                "reason": "inspect_missing",
                "stderr": str(inspect_error)[:512],
                "exit_code": 1,
            }
        return {
            "available": True,
            "reason": "inspect_ok",
            "stderr": "",
            "exit_code": 0,
        }

    def _pull_local_image(image_name):
        if _docker_cli_available():
            try:
                pull_result = subprocess.run(
                    ["docker", "pull", image_name],
                    capture_output=True,
                    text=True,
                    timeout=max(RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS + 30, 120),
                )
            except subprocess.TimeoutExpired:
                return {
                    "pulled": False,
                    "reason": "pull_timeout",
                    "stderr": "",
                    "exit_code": 124,
                }
            except Exception as pull_error:
                return {
                    "pulled": False,
                    "reason": "pull_failed",
                    "stderr": str(pull_error)[:512],
                    "exit_code": 1,
                }
            return {
                "pulled": pull_result.returncode == 0,
                "reason": "pull_ok" if pull_result.returncode == 0 else "pull_failed",
                "stderr": str(pull_result.stderr or "").strip()[:512],
                "exit_code": pull_result.returncode,
            }

        docker_client = _get_local_docker_sdk_client()
        if docker_client is None:
            return {
                "pulled": False,
                "reason": "docker_client_unavailable",
                "stderr": "docker_cli_missing_and_sdk_unavailable",
                "exit_code": 1,
            }
        try:
            docker_client.images.pull(image_name)
        except Exception as pull_error:
            return {
                "pulled": False,
                "reason": "pull_failed",
                "stderr": str(pull_error)[:512],
                "exit_code": 1,
            }
        return {
            "pulled": True,
            "reason": "pull_ok",
            "stderr": "",
            "exit_code": 0,
        }

    initial_probe = _inspect_local_image(normalized_runtime_image)
    if initial_probe.get("available", False):
        return {
            "status": "ready",
            "reason": "runtime_image_available_locally",
            "runtime_image": normalized_runtime_image,
        }

    alias_pull_attempts = []
    for alias_image in _resolve_runtime_image_local_alias_candidates(
        normalized_node_type, normalized_runtime_image
    ):
        alias_probe = _inspect_local_image(alias_image)
        alias_was_pulled = False
        if not alias_probe.get("available", False):
            alias_pull_result = _pull_local_image(alias_image)
            alias_pull_attempts.append(
                {
                    "alias_image": alias_image,
                    "pull_result": alias_pull_result,
                }
            )
            if alias_pull_result.get("pulled", False):
                alias_was_pulled = True
                alias_probe = _inspect_local_image(alias_image)

        if not alias_probe.get("available", False):
            continue

        tag_ok = False
        if _docker_cli_available():
            try:
                tag_result = subprocess.run(
                    ["docker", "tag", alias_image, normalized_runtime_image],
                    capture_output=True,
                    text=True,
                    timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 3,
                )
                tag_ok = tag_result.returncode == 0
            except Exception:
                tag_ok = False
        else:
            docker_client = _get_local_docker_sdk_client()
            if docker_client is not None:
                try:
                    alias_obj = docker_client.images.get(alias_image)
                    if ":" in normalized_runtime_image:
                        repo, tag = normalized_runtime_image.rsplit(":", 1)
                    else:
                        repo, tag = normalized_runtime_image, "latest"
                    tag_ok = bool(alias_obj.tag(repository=repo, tag=tag))
                except Exception:
                    tag_ok = False

        if tag_ok:
            tagged_probe = _inspect_local_image(normalized_runtime_image)
            if tagged_probe.get("available", False):
                return {
                    "status": "ready",
                    "reason": (
                        "runtime_image_aliased_after_alias_pull"
                        if alias_was_pulled
                        else "runtime_image_aliased_locally"
                    ),
                    "runtime_image": normalized_runtime_image,
                    "alias_image": alias_image,
                    "alias_pull_attempts": alias_pull_attempts,
                }

    pull_result = _pull_local_image(normalized_runtime_image)
    if pull_result.get("pulled", False):
        pulled_probe = _inspect_local_image(normalized_runtime_image)
        if pulled_probe.get("available", False):
            return {
                "status": "ready",
                "reason": "runtime_image_pulled_locally",
                "runtime_image": normalized_runtime_image,
            }

    source_dir_candidates = RUNBOOK_RUNTIME_IMAGE_SOURCE_DIR_CANDIDATES_BY_NODE_TYPE.get(
        normalized_node_type, tuple()
    )
    for source_dir in source_dir_candidates:
        normalized_source_dir = os.path.expandvars(
            os.path.expanduser(str(source_dir or "").strip())
        )
        if not normalized_source_dir:
            continue
        source_path = Path(normalized_source_dir)
        if not source_path.exists() or not source_path.is_dir():
            continue
        if not (source_path / "Dockerfile").exists():
            continue

        if _docker_cli_available():
            try:
                build_result = subprocess.run(
                    ["docker", "build", "-t", normalized_runtime_image, str(source_path)],
                    capture_output=True,
                    text=True,
                    timeout=max(300, RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS * 4),
                )
            except Exception:
                continue

            if build_result.returncode != 0:
                continue
        else:
            docker_client = _get_local_docker_sdk_client()
            if docker_client is None:
                continue
            try:
                docker_client.images.build(
                    path=str(source_path),
                    tag=normalized_runtime_image,
                    rm=True,
                    pull=False,
                )
            except Exception:
                continue

        built_probe = _inspect_local_image(normalized_runtime_image)
        if built_probe.get("available", False):
            return {
                "status": "ready",
                "reason": "runtime_image_built_locally",
                "runtime_image": normalized_runtime_image,
                "source_dir": str(source_path),
            }

    return {
        "status": "failed",
        "reason": "runtime_image_unavailable_locally",
        "runtime_image": normalized_runtime_image,
        "probe": initial_probe,
        "alias_pull_attempts": alias_pull_attempts,
        "pull_result": pull_result,
    }


def _pull_runtime_image_via_ssh(
    runtime_image,
    node_type,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
    timeout_seconds=0,
):
    normalized_runtime_image = str(runtime_image or "").strip()
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    if not normalized_runtime_image:
        return {
            "attempted": False,
            "status": "skipped",
            "reason": "runtime_image_empty",
        }

    pull_timeout_seconds = max(
        60,
        int(timeout_seconds or RUNBOOK_RUNTIME_IMAGE_PRESEED_PULL_TIMEOUT_SECONDS),
    )
    candidate_results = []
    pull_candidates = _resolve_runtime_image_pull_candidates(
        normalized_node_type,
        normalized_runtime_image,
    )
    for pull_candidate in pull_candidates:
        remote_pull_command = (
            "set -e; "
            "if command -v timeout >/dev/null 2>&1; then "
            "timeout {pull_timeout}s docker pull {runtime_image} >/dev/null 2>&1; "
            "else "
            "docker pull {runtime_image} >/dev/null 2>&1; "
            "fi"
        ).format(
            pull_timeout=pull_timeout_seconds,
            runtime_image=shlex.quote(pull_candidate),
        )
        pull_result = _run_remote_command(
            host_address=host_address,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            remote_command=remote_pull_command,
            identity_file=identity_file,
            timeout=pull_timeout_seconds + 20,
        )
        candidate_row = {
            "pull_image": pull_candidate,
            "stdout": str(pull_result.get("stdout", ""))[:512],
            "stderr": str(pull_result.get("stderr", ""))[:512],
            "exit_code": pull_result.get("returncode", 1),
            "timed_out": bool(pull_result.get("timed_out", False)),
        }
        if pull_result.get("timed_out", False):
            candidate_row["status"] = "timeout"
            candidate_results.append(candidate_row)
            continue

        if pull_result.get("returncode", 1) != 0:
            candidate_row["status"] = "failed"
            candidate_results.append(candidate_row)
            continue

        candidate_row["status"] = "pulled"
        if pull_candidate != normalized_runtime_image:
            tag_result = _run_remote_command(
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                remote_command="docker tag {src} {dst}".format(
                    src=shlex.quote(pull_candidate),
                    dst=shlex.quote(normalized_runtime_image),
                ),
                identity_file=identity_file,
                timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 20,
            )
            candidate_row["tag_result"] = {
                "stdout": str(tag_result.get("stdout", ""))[:512],
                "stderr": str(tag_result.get("stderr", ""))[:512],
                "exit_code": tag_result.get("returncode", 1),
                "timed_out": bool(tag_result.get("timed_out", False)),
            }
            if (
                tag_result.get("timed_out", False)
                or tag_result.get("returncode", 1) != 0
            ):
                candidate_row["status"] = "tag_failed"
                candidate_results.append(candidate_row)
                continue

        candidate_results.append(candidate_row)
        remote_image_probe = _probe_remote_runtime_image(
            host_address=host_address,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            runtime_image=normalized_runtime_image,
            identity_file=identity_file,
        )
        if remote_image_probe.get("available", False):
            return {
                "attempted": True,
                "status": "pulled",
                "reason": "runtime_image_pulled_remotely",
                "runtime_image": normalized_runtime_image,
                "pulled_image": pull_candidate,
                "pull_timeout_seconds": pull_timeout_seconds,
                "candidate_results": candidate_results,
            }

    if any(row.get("status") == "timeout" for row in candidate_results):
        return {
            "attempted": True,
            "status": "failed",
            "reason": "runtime_image_remote_pull_timeout",
            "runtime_image": normalized_runtime_image,
            "pull_timeout_seconds": pull_timeout_seconds,
            "candidate_results": candidate_results,
        }
    return {
        "attempted": True,
        "status": "failed",
        "reason": "runtime_image_remote_pull_failed",
        "runtime_image": normalized_runtime_image,
        "pull_timeout_seconds": pull_timeout_seconds,
        "candidate_results": candidate_results,
    }


def _warm_runtime_image_on_host(
    runtime_image,
    node_type,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    normalized_runtime_image = str(runtime_image or "").strip()
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    if not normalized_runtime_image:
        return {
            "attempted": False,
            "status": "failed",
            "reason": "runtime_image_empty",
        }

    if _runtime_image_warmup_prefers_local_seed(
        normalized_node_type,
        normalized_runtime_image,
    ):
        local_image_ready = _ensure_local_runtime_image_available(
            node_type=normalized_node_type,
            runtime_image=normalized_runtime_image,
        )
        if str(local_image_ready.get("status", "")).strip().lower() == "ready":
            seed_result = _seed_runtime_image_via_ssh(
                runtime_image=normalized_runtime_image,
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                identity_file=identity_file,
            )
            seeded_probe = _probe_remote_runtime_image(
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                runtime_image=normalized_runtime_image,
                identity_file=identity_file,
            )
            if seeded_probe.get("available", False):
                return {
                    "attempted": True,
                    "status": "seeded",
                    "reason": "runtime_image_seeded_remotely",
                    "runtime_image": normalized_runtime_image,
                    "seed_result": seed_result,
                    "local_image_ready": local_image_ready,
                }

    return _pull_runtime_image_via_ssh(
        runtime_image=normalized_runtime_image,
        node_type=normalized_node_type,
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        identity_file=identity_file,
    )


def _seed_runtime_image_via_ssh(
    runtime_image,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
    seed_timeout_seconds=0,
):
    normalized_runtime_image = str(runtime_image or "").strip()
    if not normalized_runtime_image:
        return {
            "attempted": False,
            "status": "skipped",
            "reason": "runtime_image_empty",
        }

    docker_client = None
    if _docker_cli_available():
        try:
            local_image_probe = subprocess.run(
                ["docker", "image", "inspect", normalized_runtime_image],
                capture_output=True,
                text=True,
                timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 3,
            )
        except Exception as probe_error:
            return {
                "attempted": False,
                "status": "skipped",
                "reason": "runtime_image_probe_failed",
                "stderr": str(probe_error)[:512],
            }

        if local_image_probe.returncode != 0:
            return {
                "attempted": False,
                "status": "skipped",
                "reason": "runtime_image_not_available_locally",
                "stderr": str(local_image_probe.stderr or "").strip()[:512],
            }
    else:
        docker_client = _get_local_docker_sdk_client()
        if docker_client is None:
            return {
                "attempted": False,
                "status": "skipped",
                "reason": "runtime_image_probe_failed",
                "stderr": "docker_cli_missing_and_sdk_unavailable",
            }
        try:
            docker_client.images.get(normalized_runtime_image)
        except Exception as probe_error:
            return {
                "attempted": False,
                "status": "skipped",
                "reason": "runtime_image_not_available_locally",
                "stderr": str(probe_error)[:512],
            }

    resolved_seed_timeout_seconds, runtime_image_size_bytes = (
        _resolve_runtime_image_seed_timeout_seconds(normalized_runtime_image)
    )
    if seed_timeout_seconds:
        resolved_seed_timeout_seconds = max(
            resolved_seed_timeout_seconds,
            int(seed_timeout_seconds),
        )

    load_command = _build_ssh_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command="docker load >/dev/null 2>&1",
        identity_file=identity_file,
    )
    transfer_mode = "stream"
    try:
        if _docker_cli_available():
            transfer_pipeline = "{} | {}".format(
                shlex.join(["docker", "image", "save", normalized_runtime_image]),
                shlex.join(load_command),
            )
            transfer_result = subprocess.run(
                ["/bin/bash", "-lc", transfer_pipeline],
                capture_output=True,
                text=True,
                timeout=resolved_seed_timeout_seconds,
            )
        else:
            transfer_mode = "stream-sdk"
            remote_process = subprocess.Popen(
                load_command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            image_obj = docker_client.images.get(normalized_runtime_image)
            for tar_chunk in image_obj.save(named=True):
                if not tar_chunk:
                    continue
                if remote_process.stdin is not None:
                    remote_process.stdin.write(tar_chunk)
            if remote_process.stdin is not None:
                remote_process.stdin.close()
            remote_stdout, remote_stderr = remote_process.communicate(
                timeout=resolved_seed_timeout_seconds
            )
            transfer_result = subprocess.CompletedProcess(
                args=load_command,
                returncode=remote_process.returncode,
                stdout=(remote_stdout or b"").decode("utf-8", errors="replace"),
                stderr=(remote_stderr or b"").decode("utf-8", errors="replace"),
            )
    except subprocess.TimeoutExpired:
        return {
            "attempted": True,
            "status": "failed",
            "reason": "runtime_image_seed_timeout",
            "runtime_image": normalized_runtime_image,
            "runtime_image_size_bytes": runtime_image_size_bytes,
            "seed_timeout_seconds": resolved_seed_timeout_seconds,
            "transfer_mode": transfer_mode,
        }
    except Exception as transfer_error:
        return {
            "attempted": True,
            "status": "failed",
            "reason": "runtime_image_seed_command_failed",
            "runtime_image": normalized_runtime_image,
            "runtime_image_size_bytes": runtime_image_size_bytes,
            "seed_timeout_seconds": resolved_seed_timeout_seconds,
            "transfer_mode": transfer_mode,
            "stderr": str(transfer_error)[:512],
        }

    return {
        "attempted": True,
        "status": "seeded" if transfer_result.returncode == 0 else "failed",
        "runtime_image": normalized_runtime_image,
        "runtime_image_size_bytes": runtime_image_size_bytes,
        "seed_timeout_seconds": resolved_seed_timeout_seconds,
        "transfer_mode": transfer_mode,
        "exit_code": transfer_result.returncode,
        "stdout": str(transfer_result.stdout or "").strip()[:512],
        "stderr": str(transfer_result.stderr or "").strip()[:512],
    }


def _validate_machine_credentials_security(host_mapping, machine_credentials):
    if not isinstance(host_mapping, list) or len(host_mapping) == 0:
        return _build_runbook_error_payload(
            "runbook_host_mapping_required",
            "Host mapping oficial obrigatorio para validar credenciais por maquina.",
        )

    if not isinstance(machine_credentials, list) or len(machine_credentials) == 0:
        return _build_runbook_error_payload(
            "runbook_machine_credentials_required",
            "machine_credentials obrigatorio para vinculo deterministico de credencial SSH por maquina.",
        )

    credential_by_machine = {}
    reuse_index = {}
    for index, credential_row in enumerate(machine_credentials):
        if not isinstance(credential_row, dict):
            return _build_runbook_error_payload(
                "runbook_machine_credentials_invalid",
                "Entrada de machine_credentials invalida.",
                {"credential_index": index},
            )

        machine_id = str(credential_row.get("machine_id", "") or "").strip()
        if not machine_id:
            return _build_runbook_error_payload(
                "runbook_machine_credentials_invalid",
                "machine_id obrigatorio em machine_credentials.",
                {"credential_index": index},
            )

        if machine_id in credential_by_machine:
            return _build_runbook_error_payload(
                "runbook_machine_credentials_duplicate",
                "machine_id duplicado em machine_credentials.",
                {"machine_id": machine_id},
            )

        credential_ref = str(credential_row.get("credential_ref", "") or "").strip()
        credential_fingerprint = str(
            credential_row.get("credential_fingerprint", "") or ""
        ).strip()
        credential_payload = str(credential_row.get("credential_payload", "") or "").strip()
        reuse_confirmed = bool(credential_row.get("reuse_confirmed", False))

        if not credential_ref and not credential_fingerprint:
            return _build_runbook_error_payload(
                "runbook_machine_credentials_missing_binding",
                "credential_ref ou credential_fingerprint obrigatorio por machine_id.",
                {"machine_id": machine_id},
            )

        if re.search(r"-----BEGIN\s+[^-]*PRIVATE KEY-----", credential_ref, re.IGNORECASE):
            return _build_runbook_error_payload(
                "runbook_sensitive_credential_forbidden",
                "Payload operacional nao pode transportar chave privada em texto puro.",
                {"machine_id": machine_id, "forbidden_field": "credential_ref"},
            )

        if credential_ref:
            if not _is_secure_credential_reference(credential_ref):
                return _build_runbook_error_payload(
                    "runbook_machine_credentials_invalid_reference",
                    "credential_ref fora do padrao seguro permitido.",
                    {"machine_id": machine_id},
                )
            if credential_ref.lower().startswith("local-file:") and not credential_payload:
                return _build_runbook_error_payload(
                    "runbook_machine_credentials_payload_required",
                    "credential_payload em base64 e obrigatorio para referencias local-file.",
                    {"machine_id": machine_id},
                )

        credential_by_machine[machine_id] = {
            "machine_id": machine_id,
            "credential_ref": credential_ref,
            "credential_fingerprint": credential_fingerprint,
            "credential_payload": credential_payload,
            "reuse_confirmed": reuse_confirmed,
        }

        reuse_key = credential_ref or credential_fingerprint
        if reuse_key:
            if reuse_key not in reuse_index:
                reuse_index[reuse_key] = []
            reuse_index[reuse_key].append(
                {"machine_id": machine_id, "reuse_confirmed": reuse_confirmed}
            )

    for host_index, host_row in enumerate(host_mapping):
        machine_id = _resolve_machine_id_from_host_row(host_row)
        if not machine_id:
            return _build_runbook_error_payload(
                "runbook_machine_credentials_invalid",
                "Nao foi possivel resolver machine_id a partir do host_mapping.",
                {"host_index": host_index},
            )
        if machine_id not in credential_by_machine:
            return _build_runbook_error_payload(
                "runbook_machine_credentials_missing_binding",
                "machine_credentials ausente para host do preflight/start oficial.",
                {"machine_id": machine_id, "host_index": host_index},
            )

    for reuse_rows in reuse_index.values():
        if len(reuse_rows) <= 1:
            continue
        for reuse_row in reuse_rows:
            if not reuse_row.get("reuse_confirmed", False):
                return _build_runbook_error_payload(
                    "runbook_machine_credentials_reuse_not_confirmed",
                    "Reuso de credencial SSH requer confirmacao explicita por machine_id.",
                    {"machine_id": reuse_row.get("machine_id", "")},
                )

    return None


def _extract_ssh_target_parts(raw_ssh_user):
    raw_value = str(raw_ssh_user or "").strip()
    if not raw_value:
        return None

    if raw_value.lower().startswith("ssh ") is False:
        return None

    try:
        tokens = shlex.split(raw_value)
    except ValueError:
        tokens = raw_value.split()

    if len(tokens) == 0 or tokens[0].lower() != "ssh":
        return None

    extracted_user = ""
    extracted_host = ""
    extracted_port = None
    target_token = ""
    index = 1
    options_with_argument = {"-i", "-o", "-F", "-J", "-L", "-R", "-S", "-W", "-w", "-b", "-c", "-D", "-E", "-e", "-I", "-m", "-Q", "-X", "-Y"}

    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            index += 1
            break

        if token == "-p" and index + 1 < len(tokens):
            extracted_port = tokens[index + 1]
            index += 2
            continue
        if token.startswith("-p") and token != "-p":
            extracted_port = token[2:]
            index += 1
            continue

        if token == "-l" and index + 1 < len(tokens):
            extracted_user = tokens[index + 1]
            index += 2
            continue
        if token.startswith("-l") and token != "-l":
            extracted_user = token[2:]
            index += 1
            continue

        if token in options_with_argument and index + 1 < len(tokens):
            index += 2
            continue

        if token.startswith("-"):
            index += 1
            continue

        target_token = token
        break

    if target_token:
        if "@" in target_token:
            split_user, split_host = target_token.split("@", 1)
            if split_user.strip():
                extracted_user = split_user.strip()
            if split_host.strip():
                extracted_host = split_host.strip()
        else:
            extracted_user = extracted_user or target_token.strip()

    return {
        "ssh_user": extracted_user,
        "host_address": extracted_host,
        "ssh_port": extracted_port,
    }


def _normalize_host_ssh_target(host_row):
    if not isinstance(host_row, dict):
        return {}, _build_runbook_error_payload(
            "runbook_host_mapping_invalid",
            "Entrada de host mapping invalida para execucao real.",
        )

    normalized_row = dict(host_row)
    host_address = str(normalized_row.get("host_address", "") or "").strip()
    raw_ssh_user = str(normalized_row.get("ssh_user", "") or "").strip()

    ssh_port = normalized_row.get("ssh_port", 22)
    try:
        ssh_port = int(ssh_port or 22)
    except (TypeError, ValueError):
        ssh_port = 22

    if ssh_port < 1 or ssh_port > 65535:
        ssh_port = 22

    if "@" in raw_ssh_user and " " not in raw_ssh_user:
        maybe_user, maybe_host = raw_ssh_user.split("@", 1)
        if maybe_user.strip() and maybe_host.strip():
            raw_ssh_user = maybe_user.strip()
            if not host_address:
                host_address = maybe_host.strip()

    extracted_parts = _extract_ssh_target_parts(raw_ssh_user)
    if isinstance(extracted_parts, dict):
        extracted_user = str(extracted_parts.get("ssh_user", "") or "").strip()
        extracted_host = str(extracted_parts.get("host_address", "") or "").strip()
        extracted_port = extracted_parts.get("ssh_port")

        if extracted_user:
            raw_ssh_user = extracted_user
        if extracted_host and not host_address:
            host_address = extracted_host
        if extracted_port is not None:
            try:
                parsed_port = int(str(extracted_port).strip())
                if 1 <= parsed_port <= 65535:
                    ssh_port = parsed_port
            except (TypeError, ValueError):
                pass

    normalized_row["host_address"] = host_address
    normalized_row["ssh_user"] = raw_ssh_user
    normalized_row["ssh_port"] = ssh_port

    if not raw_ssh_user or any(char.isspace() for char in raw_ssh_user):
        return normalized_row, _build_runbook_error_payload(
            "runbook_host_mapping_invalid",
            "ssh_user invalido no host mapping oficial.",
            {
                "host_ref": str(normalized_row.get("host_ref", "") or "").strip(),
                "ssh_user": str(normalized_row.get("ssh_user", "") or "").strip(),
            },
        )

    if not host_address:
        return normalized_row, _build_runbook_error_payload(
            "runbook_host_mapping_invalid",
            "host_address obrigatorio no host mapping oficial.",
            {
                "host_ref": str(normalized_row.get("host_ref", "") or "").strip(),
            },
        )

    return normalized_row, None


def _validate_host_mapping_security(host_mapping):
    if not isinstance(host_mapping, list) or len(host_mapping) == 0:
        return _build_runbook_error_payload(
            "runbook_host_mapping_required",
            "Host mapping oficial obrigatorio para execucao real via SSH.",
        )

    sensitive_keys = {
        "private_key",
        "private_key_pem",
        "ssh_private_key",
        "ssh_key",
        "password",
        "passphrase",
    }
    for index, host_row in enumerate(host_mapping):
        if not isinstance(host_row, dict):
            return _build_runbook_error_payload(
                "runbook_host_mapping_invalid",
                "Entrada de host mapping invalida para execucao real.",
                {"host_index": index},
            )

        for sensitive_key in sensitive_keys:
            sensitive_value = host_row.get(sensitive_key)
            if isinstance(sensitive_value, str) and sensitive_value.strip():
                return _build_runbook_error_payload(
                    "runbook_sensitive_credential_forbidden",
                    "Payload operacional nao pode transportar credencial sensivel.",
                    {
                        "host_index": index,
                        "forbidden_field": sensitive_key,
                    },
                )

        if not str(host_row.get("host_address", "")).strip():
            return _build_runbook_error_payload(
                "runbook_host_mapping_invalid",
                "host_address obrigatorio no host mapping oficial.",
                {"host_index": index},
            )
        if not str(host_row.get("ssh_user", "")).strip():
            return _build_runbook_error_payload(
                "runbook_host_mapping_invalid",
                "ssh_user obrigatorio no host mapping oficial.",
                {"host_index": index},
            )

    return None


def _is_runtime_node_type_enabled(node_type):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    if not normalized_node_type:
        return True
    return normalized_node_type in RUNBOOK_ENABLED_NODE_TYPES


def _filter_enabled_runtime_host_mapping(host_mapping):
    if not isinstance(host_mapping, list):
        return []

    filtered_rows = []
    for host_row in host_mapping:
        if not isinstance(host_row, dict):
            filtered_rows.append(host_row)
            continue

        normalized_node_type = _normalize_runtime_component_node_type(
            host_row.get("node_type", "")
        )
        if normalized_node_type and normalized_node_type not in RUNBOOK_ENABLED_NODE_TYPES:
            continue

        normalized_host_row = dict(host_row)
        if normalized_node_type:
            normalized_host_row["node_type"] = normalized_node_type
        filtered_rows.append(normalized_host_row)
    return filtered_rows


def _resolve_runtime_image_for_node_type(node_type):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    runtime_image = str(
        RUNBOOK_RUNTIME_IMAGE_BY_NODE_TYPE.get(normalized_node_type, "")
    ).strip()
    if runtime_image:
        return _normalize_runtime_image_reference(
            normalized_node_type,
            runtime_image,
            RUNBOOK_BASE_NODE_IMAGE,
        )
    return _normalize_runtime_image_reference(
        normalized_node_type,
        RUNBOOK_BASE_NODE_IMAGE,
        RUNBOOK_BASE_NODE_IMAGE,
    )


def _runtime_image_is_placeholder(runtime_image):
    normalized_runtime_image = str(runtime_image or "").strip().lower()
    if not normalized_runtime_image:
        return True
    return bool(RUNBOOK_RUNTIME_PLACEHOLDER_IMAGE_REGEX.match(normalized_runtime_image))


def _runtime_image_has_forbidden_prefix(runtime_image):
    normalized_runtime_image = str(runtime_image or "").strip().lower()
    if not normalized_runtime_image:
        return False
    for forbidden_prefix in RUNBOOK_RUNTIME_IMAGE_FORBIDDEN_PREFIXES:
        if normalized_runtime_image.startswith(forbidden_prefix):
            return True
    return False


def _runtime_image_matches_required_prefix(node_type, runtime_image):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    required_prefix = str(
        RUNBOOK_RUNTIME_IMAGE_REQUIRED_PREFIX_BY_NODE_TYPE.get(normalized_node_type, "")
    ).strip().lower()
    if not required_prefix:
        return True
    normalized_runtime_image = str(runtime_image or "").strip().lower()
    if normalized_runtime_image.startswith(required_prefix):
        return True

    required_token = required_prefix.strip("/")
    if not required_token:
        return True

    if normalized_runtime_image.startswith("{}/".format(required_token)):
        return True
    if "/{}/".format(required_token) in normalized_runtime_image:
        return True
    return False


def _validate_runtime_image_catalog_alignment(host_mapping):
    if not isinstance(host_mapping, list):
        return None

    violations = []
    for host_row in host_mapping:
        if not isinstance(host_row, dict):
            continue

        node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        runtime_image = str(host_row.get("runtime_image", "") or "").strip()
        if not runtime_image:
            runtime_image = _resolve_runtime_image_for_node_type(node_type)
        runtime_image = _normalize_runtime_image_reference(
            node_type,
            runtime_image,
            _resolve_runtime_image_for_node_type(node_type),
        )

        if _runtime_image_has_forbidden_prefix(runtime_image):
            violations.append(
                {
                    "reason": "forbidden_runtime_registry_prefix",
                    "node_type": node_type,
                    "node_id": str(host_row.get("node_id", "") or "").strip(),
                    "org_id": str(host_row.get("org_id", "") or "").strip(),
                    "host_ref": str(host_row.get("host_ref", "") or "").strip(),
                    "runtime_image": runtime_image,
                }
            )
            continue

        if not _runtime_image_matches_required_prefix(node_type, runtime_image):
            violations.append(
                {
                    "reason": "required_runtime_registry_prefix_mismatch",
                    "node_type": node_type,
                    "node_id": str(host_row.get("node_id", "") or "").strip(),
                    "org_id": str(host_row.get("org_id", "") or "").strip(),
                    "host_ref": str(host_row.get("host_ref", "") or "").strip(),
                    "runtime_image": runtime_image,
                    "required_prefix": RUNBOOK_RUNTIME_IMAGE_REQUIRED_PREFIX_BY_NODE_TYPE.get(
                        node_type, ""
                    ),
                }
            )

    if violations:
        return _build_runbook_error_payload(
            "runbook_runtime_image_catalog_violation",
            "Catalogo de imagens invalido para o runtime oficial do COGNUS.",
            {
                "violations": violations,
            },
        )
    return None


def _resolve_runtime_image_local_alias_candidates(node_type, runtime_image):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    requested_image = str(runtime_image or "").strip()
    seen = set()
    candidates = []

    for candidate in RUNBOOK_RUNTIME_IMAGE_LOCAL_ALIAS_BY_NODE_TYPE.get(
        normalized_node_type, tuple()
    ):
        normalized_candidate = str(candidate or "").strip()
        if (
            not normalized_candidate
            or normalized_candidate == requested_image
            or normalized_candidate in seen
        ):
            continue
        seen.add(normalized_candidate)
        candidates.append(normalized_candidate)
    return candidates


def _resolve_runtime_image_pull_candidates(node_type, runtime_image):
    requested_image = str(runtime_image or "").strip()
    candidates = []
    if requested_image:
        candidates.append(requested_image)
    candidates.extend(
        _resolve_runtime_image_local_alias_candidates(node_type, requested_image)
    )
    deduplicated = []
    seen = set()
    for candidate in candidates:
        normalized_candidate = str(candidate or "").strip()
        if not normalized_candidate or normalized_candidate in seen:
            continue
        seen.add(normalized_candidate)
        deduplicated.append(normalized_candidate)
    return deduplicated


def _build_runtime_image_source_fallback_step(node_type, runtime_image):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    source_dir_candidates = RUNBOOK_RUNTIME_IMAGE_SOURCE_DIR_CANDIDATES_BY_NODE_TYPE.get(
        normalized_node_type, tuple()
    )
    if not source_dir_candidates:
        return ""

    quoted_runtime_image = shlex.quote(str(runtime_image or "").strip())
    if not quoted_runtime_image:
        return ""

    quoted_source_dirs = " ".join(
        '"{}"'.format(str(source_dir).replace('"', '\\"'))
        for source_dir in source_dir_candidates
    )
    if not quoted_source_dirs:
        return ""

    return (
        "(for runtime_src_dir in {source_dirs}; do "
        "[ -f \"$runtime_src_dir/Dockerfile\" ] || continue; "
        "[ -f \"$runtime_src_dir/package.json\" ] || continue; "
        "docker build -t {runtime} \"$runtime_src_dir\" >/dev/null 2>&1 && break; "
        "done; "
        "docker image inspect {runtime} >/dev/null 2>&1)"
    ).format(
        source_dirs=quoted_source_dirs,
        runtime=quoted_runtime_image,
    )


def _build_runtime_image_bootstrap_command(node_type, runtime_image):
    normalized_runtime_image = str(runtime_image or "").strip()
    if not normalized_runtime_image:
        return ""

    quoted_runtime_image = shlex.quote(normalized_runtime_image)
    missing_message = shlex.quote(
        "COGNUS_RUNTIME_IMAGE_MISSING:{}".format(normalized_runtime_image)
    )

    alias_steps = []
    for alias_image in _resolve_runtime_image_local_alias_candidates(
        node_type, normalized_runtime_image
    ):
        quoted_alias_image = shlex.quote(alias_image)
        alias_steps.append(
            "(docker image inspect {alias} >/dev/null 2>&1 && "
            "docker tag {alias} {runtime} >/dev/null 2>&1)".format(
                alias=quoted_alias_image,
                runtime=quoted_runtime_image,
            )
        )

    bootstrap_candidates = list(alias_steps)
    bootstrap_candidates.append(
        "docker pull {runtime} >/dev/null 2>&1".format(runtime=quoted_runtime_image)
    )
    bootstrap_chain = " || ".join(bootstrap_candidates)

    return (
        "docker image inspect {runtime} >/dev/null 2>&1 || "
        "{bootstrap_chain} || {{ printf '%s\\n' {missing_message} >&2; exit 125; }}; "
    ).format(
        runtime=quoted_runtime_image,
        bootstrap_chain=bootstrap_chain,
        missing_message=missing_message,
    )


def _enrich_runtime_host_mapping(host_mapping):
    if not isinstance(host_mapping, list):
        return []

    enriched_rows = []
    for host_row in host_mapping:
        if not isinstance(host_row, dict):
            enriched_rows.append(host_row)
            continue
        enriched_row = dict(host_row)
        normalized_node_type = _normalize_runtime_component_node_type(
            enriched_row.get("node_type", "")
        )
        if normalized_node_type:
            enriched_row["node_type"] = normalized_node_type
        runtime_image = str(enriched_row.get("runtime_image", "") or "").strip()
        if not runtime_image:
            runtime_image = _resolve_runtime_image_for_node_type(
                normalized_node_type
            )
        runtime_image = _normalize_runtime_image_reference(
            normalized_node_type,
            runtime_image,
            _resolve_runtime_image_for_node_type(normalized_node_type),
        )
        enriched_row["runtime_image"] = runtime_image
        enriched_rows.append(enriched_row)

    return enriched_rows


def _resolve_runtime_default_node_id(node_type, org_id):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    normalized_org_id = str(org_id or "").strip().lower() or "org"
    if normalized_node_type == "peer":
        return "peer0-{}".format(normalized_org_id)
    if normalized_node_type == "orderer":
        return "orderer0-{}".format(normalized_org_id)
    if normalized_node_type == "ca":
        return "ca0-{}".format(normalized_org_id)
    if normalized_node_type == "couch":
        return "couch-{}".format(normalized_org_id)
    if normalized_node_type == "apigateway":
        return "apigateway-{}".format(normalized_org_id)
    if normalized_node_type == "netapi":
        return "netapi-{}".format(normalized_org_id)
    if normalized_node_type == "chaincode":
        return "chaincode-{}".format(normalized_org_id)
    return "{}-{}".format(normalized_node_type or "node", normalized_org_id)


def _autoexpand_runtime_host_mapping(host_mapping, topology_catalog=None):
    if not RUNBOOK_RUNTIME_HOST_MAPPING_AUTOEXPAND_ENABLED:
        return host_mapping if isinstance(host_mapping, list) else []
    if not isinstance(host_mapping, list):
        return []

    expanded_rows = [dict(row) if isinstance(row, dict) else row for row in host_mapping]
    host_rows = [row for row in expanded_rows if isinstance(row, dict)]
    if len(host_rows) == 0:
        return expanded_rows

    topology_catalog = topology_catalog if isinstance(topology_catalog, dict) else {}
    organizations = topology_catalog.get("organizations", [])
    if not isinstance(organizations, list):
        organizations = []

    org_ids = set()
    for row in host_rows:
        org_id = str(row.get("org_id", "") or "").strip().lower()
        if org_id:
            org_ids.add(org_id)
    for organization in organizations:
        if not isinstance(organization, dict):
            continue
        org_id = str(
            organization.get("org_id")
            or organization.get("org_name")
            or organization.get("org_key")
            or ""
        ).strip().lower()
        if org_id:
            org_ids.add(org_id)

    if len(org_ids) == 0:
        fallback_org_id = str(host_rows[0].get("org_id", "") or "").strip().lower() or "org"
        org_ids.add(fallback_org_id)

    def _resolve_organization(org_id):
        normalized_org_id = str(org_id or "").strip().lower()
        for organization in organizations:
            if not isinstance(organization, dict):
                continue
            candidate_org_id = str(
                organization.get("org_id")
                or organization.get("org_name")
                or organization.get("org_key")
                or ""
            ).strip().lower()
            if candidate_org_id and candidate_org_id == normalized_org_id:
                return organization
        return {}

    def _resolve_base_row(org_id):
        normalized_org_id = str(org_id or "").strip().lower()
        for row in host_rows:
            row_org_id = str(row.get("org_id", "") or "").strip().lower()
            if normalized_org_id and row_org_id == normalized_org_id:
                return row
        return host_rows[0]

    seen_pairs = set()
    for row in host_rows:
        node_type = _normalize_runtime_component_node_type(row.get("node_type", ""))
        org_id = str(row.get("org_id", "") or "").strip().lower()
        if node_type and org_id:
            seen_pairs.add((org_id, node_type))

    for org_id in sorted(org_ids):
        base_row = _resolve_base_row(org_id)
        if not isinstance(base_row, dict):
            continue

        organization = _resolve_organization(org_id)
        service_host_mapping = organization.get("service_host_mapping", {})
        if not isinstance(service_host_mapping, dict):
            service_host_mapping = {}

        host_ref_by_type = {
            "peer": str(service_host_mapping.get("peer") or "").strip(),
            "orderer": str(service_host_mapping.get("orderer") or "").strip(),
            "ca": str(service_host_mapping.get("ca") or "").strip(),
            "couch": str(service_host_mapping.get("couch") or "").strip(),
            "apigateway": str(
                service_host_mapping.get("apiGateway")
                or service_host_mapping.get("api_gateway")
                or ""
            ).strip(),
            "netapi": str(
                service_host_mapping.get("netapi")
                or service_host_mapping.get("netApi")
                or service_host_mapping.get("networkApi")
                or ""
            ).strip(),
            "chaincode": str(service_host_mapping.get("peer") or "").strip(),
        }

        for node_type in RUNBOOK_ENABLED_NODE_TYPES:
            normalized_node_type = _normalize_runtime_component_node_type(node_type)
            pair_key = (org_id, normalized_node_type)
            if pair_key in seen_pairs:
                continue

            resolved_host_ref = host_ref_by_type.get(normalized_node_type, "")
            source_row = None
            if resolved_host_ref:
                for row in host_rows:
                    if str(row.get("host_ref", "") or "").strip() == resolved_host_ref:
                        source_row = row
                        break
            if source_row is None:
                source_row = base_row

            new_row = dict(source_row)
            new_row["org_id"] = org_id
            new_row["node_type"] = normalized_node_type
            if resolved_host_ref:
                new_row["host_ref"] = resolved_host_ref
            new_row["node_id"] = _resolve_runtime_default_node_id(normalized_node_type, org_id)
            new_row["runtime_image"] = _resolve_runtime_image_for_node_type(
                normalized_node_type
            )

            expanded_rows.append(new_row)
            host_rows.append(new_row)
            seen_pairs.add(pair_key)

    return expanded_rows


def _normalize_org_alias_token(value):
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    return re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")


def _build_topology_org_alias_registry(topology_catalog):
    if not isinstance(topology_catalog, dict):
        return {}

    organizations = topology_catalog.get("organizations", [])
    if not isinstance(organizations, list):
        return {}

    alias_registry = {}

    def _register_alias(alias_value, canonical_org_id):
        alias = str(alias_value or "").strip()
        canonical = str(canonical_org_id or "").strip()
        if not alias or not canonical:
            return
        alias_registry[alias.lower()] = canonical
        alias_token = _normalize_org_alias_token(alias)
        if alias_token:
            alias_registry[alias_token] = canonical

    for organization in organizations:
        if not isinstance(organization, dict):
            continue

        canonical_org_id = str(
            organization.get("org_id")
            or organization.get("org_name")
            or organization.get("org_key")
            or ""
        ).strip()
        if not canonical_org_id:
            continue

        _register_alias(canonical_org_id, canonical_org_id)
        _register_alias(organization.get("org_name"), canonical_org_id)
        _register_alias(organization.get("org_key"), canonical_org_id)
        _register_alias(organization.get("label"), canonical_org_id)
        _register_alias(organization.get("org_label"), canonical_org_id)
        _register_alias(organization.get("domain"), canonical_org_id)

    return alias_registry


def _resolve_canonical_org_id(org_id, alias_registry):
    normalized_org_id = str(org_id or "").strip()
    if not normalized_org_id:
        return ""

    if not isinstance(alias_registry, dict):
        return normalized_org_id

    direct_alias = alias_registry.get(normalized_org_id.lower())
    if direct_alias:
        return direct_alias

    token_alias = alias_registry.get(_normalize_org_alias_token(normalized_org_id))
    if token_alias:
        return token_alias

    return normalized_org_id


def _extract_topology_runtime_expectations(topology_catalog):
    if not isinstance(topology_catalog, dict):
        return {}

    organizations = topology_catalog.get("organizations", [])
    if not isinstance(organizations, list):
        return {}

    expected = {}
    chaincodes_by_channel = {}

    def _add_expected_component(org_id, node_type, host_ref, component=""):
        normalized_org_id = str(org_id or "").strip()
        normalized_node_type = _normalize_runtime_component_node_type(node_type)
        normalized_host_ref = str(host_ref or "").strip()
        if not _is_runtime_node_type_enabled(normalized_node_type):
            return
        if not normalized_org_id or not normalized_node_type or not normalized_host_ref:
            return

        key = "{}|{}".format(normalized_org_id.lower(), normalized_node_type)
        if key not in expected:
            expected[key] = {
                "org_id": normalized_org_id,
                "node_type": normalized_node_type,
                "required_count": 0,
                "required_host_refs": set(),
                "components": set(),
            }
        expected[key]["required_count"] += 1
        expected[key]["required_host_refs"].add(normalized_host_ref)
        if component:
            expected[key]["components"].add(str(component).strip())

    def _add_expected_from_nodes(org_id, node_type, nodes):
        if not isinstance(nodes, list):
            return
        for node in nodes:
            if not isinstance(node, dict):
                continue
            host_ref = str(node.get("host_ref") or "").strip()
            component_name = str(
                node.get("node_id") or node.get("name") or node_type
            ).strip()
            _add_expected_component(
                org_id,
                node_type,
                host_ref,
                component=component_name,
            )

    def _normalize_channel_key(channel_id):
        return str(channel_id or "").strip().lower()

    def _register_channel_chaincode(channel_id, chaincode_id):
        normalized_channel_key = _normalize_channel_key(channel_id)
        normalized_chaincode_id = str(chaincode_id or "").strip()
        if not normalized_channel_key or not normalized_chaincode_id:
            return
        if normalized_channel_key not in chaincodes_by_channel:
            chaincodes_by_channel[normalized_channel_key] = set()
        chaincodes_by_channel[normalized_channel_key].add(normalized_chaincode_id)

    def _ingest_channel_collection(channels):
        if not isinstance(channels, list):
            return
        for channel in channels:
            if not isinstance(channel, dict):
                continue
            channel_id = str(
                channel.get("channel_id")
                or channel.get("name")
                or channel.get("id")
                or ""
            ).strip()
            if not channel_id:
                continue
            for chaincode_id in channel.get("chaincodes", []):
                _register_channel_chaincode(channel_id, chaincode_id)

    _ingest_channel_collection(topology_catalog.get("channels", []))
    for business_group in topology_catalog.get("business_groups", []):
        if not isinstance(business_group, dict):
            continue
        _ingest_channel_collection(business_group.get("channels", []))

    for chaincode_row in topology_catalog.get("chaincodes", []):
        if not isinstance(chaincode_row, dict):
            continue
        _register_channel_chaincode(
            chaincode_row.get("channel_id", ""),
            chaincode_row.get("chaincode_id", ""),
        )

    def _collect_organization_api_components(organization):
        api_rows = organization.get("apis", [])
        if not isinstance(api_rows, list):
            api_rows = []

        api_components = {}
        for api_index, api_row in enumerate(api_rows):
            if not isinstance(api_row, dict):
                continue
            api_id = str(api_row.get("api_id", "") or "").strip()
            channel_id = str(api_row.get("channel_id", "") or "").strip()
            chaincode_id = str(api_row.get("chaincode_id", "") or "").strip()
            route_path = str(api_row.get("route_path", "") or "").strip()
            component_key = "|".join(
                [api_id.lower(), channel_id.lower(), chaincode_id.lower(), route_path.lower()]
            )
            if not component_key.strip("|"):
                component_key = "api-index-{}".format(api_index + 1)
            if component_key in api_components:
                continue
            api_components[component_key] = {
                "api_id": api_id,
                "channel_id": channel_id,
                "chaincode_id": chaincode_id,
                "route_path": route_path,
            }

        return sorted(
            api_components.values(),
            key=lambda api_component: (
                str(api_component.get("api_id", "")).lower(),
                str(api_component.get("channel_id", "")).lower(),
                str(api_component.get("chaincode_id", "")).lower(),
                str(api_component.get("route_path", "")).lower(),
            ),
        )

    for organization in organizations:
        if not isinstance(organization, dict):
            continue

        org_id = str(
            organization.get("org_id") or organization.get("org_name") or ""
        ).strip()
        if not org_id:
            continue

        service_host_mapping = organization.get("service_host_mapping", {})
        if not isinstance(service_host_mapping, dict):
            service_host_mapping = {}

        _add_expected_from_nodes(org_id, "peer", organization.get("peers", []))
        _add_expected_from_nodes(org_id, "orderer", organization.get("orderers", []))
        _add_expected_from_nodes(org_id, "ca", organization.get("cas", []))

        _add_expected_component(
            org_id,
            "couch",
            service_host_mapping.get("couch", ""),
            component="couch",
        )
        apigateway_host_ref = (
            service_host_mapping.get("apiGateway")
            or service_host_mapping.get("api_gateway")
            or ""
        )
        api_components = _collect_organization_api_components(organization)
        api_component_name = "apiGateway"
        if len(api_components) > 0:
            api_component_name = "apiGateway:{}apis".format(len(api_components))
        _add_expected_component(
            org_id,
            "apigateway",
            apigateway_host_ref,
            component=api_component_name,
        )
        _add_expected_component(
            org_id,
            "netapi",
            service_host_mapping.get("netapi")
            or service_host_mapping.get("netApi")
            or service_host_mapping.get("networkApi")
            or "",
            component="netapi",
        )

        chaincodes = organization.get("chaincodes", [])
        if not isinstance(chaincodes, list):
            chaincodes = []
        organization_channels = set()
        organization_chaincode_components = {}
        scoped_chaincode_tokens = set()

        def _register_organization_chaincode(channel_id, chaincode_id):
            normalized_channel_id = str(channel_id or "").strip()
            normalized_chaincode_id = str(chaincode_id or "").strip()
            if not normalized_chaincode_id:
                return
            component_key = "{}|{}".format(
                normalized_channel_id.lower(),
                normalized_chaincode_id.lower(),
            )
            if component_key in organization_chaincode_components:
                return
            organization_chaincode_components[component_key] = {
                "channel_id": normalized_channel_id,
                "chaincode_id": normalized_chaincode_id,
            }
            if normalized_channel_id:
                scoped_chaincode_tokens.add(normalized_chaincode_id.lower())

        organization_channels_rows = organization.get("channels", [])
        if not isinstance(organization_channels_rows, list):
            organization_channels_rows = []
        for organization_channel in organization_channels_rows:
            if isinstance(organization_channel, dict):
                organization_channel_id = str(
                    organization_channel.get("channel_id")
                    or organization_channel.get("name")
                    or organization_channel.get("id")
                    or ""
                ).strip()
                if not organization_channel_id:
                    continue
                organization_channels.add(organization_channel_id)
                for chaincode_id in organization_channel.get("chaincodes", []):
                    _register_organization_chaincode(organization_channel_id, chaincode_id)
                continue

            organization_channel_id = str(organization_channel or "").strip()
            if organization_channel_id:
                organization_channels.add(organization_channel_id)

        for api_component in api_components:
            api_channel_id = str(api_component.get("channel_id", "") or "").strip()
            if api_channel_id:
                organization_channels.add(api_channel_id)
            _register_organization_chaincode(
                api_channel_id,
                api_component.get("chaincode_id", ""),
            )

        for organization_channel_id in organization_channels:
            for chaincode_id in chaincodes_by_channel.get(
                _normalize_channel_key(organization_channel_id), set()
            ):
                _register_organization_chaincode(organization_channel_id, chaincode_id)

        for chaincode_id in chaincodes:
            normalized_chaincode_id = str(chaincode_id or "").strip()
            if not normalized_chaincode_id:
                continue
            if normalized_chaincode_id.lower() in scoped_chaincode_tokens:
                continue
            _register_organization_chaincode("", normalized_chaincode_id)

        chaincode_host_ref = str(
            service_host_mapping.get("peer")
            or organization.get("peer_host_ref")
            or service_host_mapping.get("netapi")
            or ""
        ).strip()
        for chaincode_component in sorted(
            organization_chaincode_components.values(),
            key=lambda row: (
                str(row.get("channel_id", "")).lower(),
                str(row.get("chaincode_id", "")).lower(),
            ),
        ):
            normalized_chaincode_id = str(
                chaincode_component.get("chaincode_id", "") or ""
            ).strip()
            normalized_channel_id = str(
                chaincode_component.get("channel_id", "") or ""
            ).strip()
            if not normalized_chaincode_id:
                continue
            component_name = "chaincode:{}".format(normalized_chaincode_id)
            if normalized_channel_id:
                component_name = "chaincode:{}:{}".format(
                    normalized_channel_id,
                    normalized_chaincode_id,
                )
            _add_expected_component(
                org_id,
                "chaincode",
                chaincode_host_ref,
                component=component_name,
            )

    return expected


def _validate_topology_runtime_host_mapping_coverage(topology_catalog, host_mapping):
    expected = _extract_topology_runtime_expectations(topology_catalog)
    if not expected:
        return None

    alias_registry = _build_topology_org_alias_registry(topology_catalog)
    actual = {}
    for host_row in host_mapping if isinstance(host_mapping, list) else []:
        if not isinstance(host_row, dict):
            continue
        org_id = _resolve_canonical_org_id(host_row.get("org_id", ""), alias_registry)
        node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        host_ref = str(host_row.get("host_ref", "") or "").strip()
        if not org_id or not node_type:
            continue

        key = "{}|{}".format(org_id.lower(), node_type)
        if key not in actual:
            actual[key] = {
                "count": 0,
                "host_refs": set(),
            }
        actual[key]["count"] += 1
        if host_ref:
            actual[key]["host_refs"].add(host_ref)

    missing_components = []
    for key, expected_row in expected.items():
        actual_row = actual.get(key, {"count": 0, "host_refs": set()})
        required_count = int(expected_row.get("required_count", 0) or 0)
        actual_count = int(actual_row.get("count", 0) or 0)
        required_host_refs = expected_row.get("required_host_refs", set()) or set()
        actual_host_refs = actual_row.get("host_refs", set()) or set()

        if actual_count < required_count:
            missing_components.append(
                {
                    "org_id": expected_row.get("org_id", ""),
                    "node_type": expected_row.get("node_type", ""),
                    "required_count": required_count,
                    "actual_count": actual_count,
                    "required_host_refs": sorted(list(required_host_refs)),
                    "actual_host_refs": sorted(list(actual_host_refs)),
                    "components": sorted(list(expected_row.get("components", set()) or set())),
                    "reason": "missing_component_rows",
                }
            )
            continue

        if required_host_refs and not required_host_refs.issubset(actual_host_refs):
            missing_components.append(
                {
                    "org_id": expected_row.get("org_id", ""),
                    "node_type": expected_row.get("node_type", ""),
                    "required_count": required_count,
                    "actual_count": actual_count,
                    "required_host_refs": sorted(list(required_host_refs)),
                    "actual_host_refs": sorted(list(actual_host_refs)),
                    "components": sorted(list(expected_row.get("components", set()) or set())),
                    "reason": "component_host_ref_mismatch",
                }
            )

    if missing_components:
        return _build_runbook_error_payload(
            "runbook_topology_runtime_mapping_incomplete",
            "Host mapping incompleto para runtime oficial conforme topologia declarada.",
            {"missing_components": missing_components},
        )

    return None


def _normalize_actor_role(user):
    if not user:
        return "user"

    role_value = getattr(user, "role", "")
    if role_value is None:
        role_value = ""

    if isinstance(role_value, int):
        if role_value == 0:
            return "admin"
        if role_value == 1:
            return "operator"
        return "user"

    normalized = str(role_value).strip().lower()
    if normalized in {"admin", "operator", "user"}:
        return normalized
    if normalized in {"0", "1", "2"}:
        return {"0": "admin", "1": "operator", "2": "user"}[normalized]

    return "user"


def _resolve_actor_context(request):
    user = getattr(request, "user", None)
    organization = getattr(user, "organization", None)
    organization_id = ""
    organization_name = ""
    if organization is not None:
        organization_id = str(getattr(organization, "id", "") or "").strip()
        organization_name = str(getattr(organization, "name", "") or "").strip().lower()

    return {
        "username": str(getattr(user, "username", "system") or "system").strip() or "system",
        "email": str(getattr(user, "email", "") or "").strip().lower(),
        "role": _normalize_actor_role(user),
        "organization_id": organization_id,
        "organization_name": organization_name,
    }


def _is_actor_authorized(action_name, actor_role):
    allowed_roles = RUNBOOK_ALLOWED_ROLES_BY_ACTION.get(action_name, set())
    return actor_role in allowed_roles


def _is_valid_change_context(change_id):
    return bool(str(change_id or "").strip())


def _run_contains_actor_scope(run_state, actor_context):
    actor_role = actor_context.get("role", "user")
    if actor_role == "admin":
        return True

    actor_username = str(actor_context.get("username", "") or "").strip().lower()
    actor_email = str(actor_context.get("email", "") or "").strip().lower()

    actor_scope = run_state.get("actor_scope", {})
    if isinstance(actor_scope, dict):
        scoped_identities = {
            str(actor_scope.get("username", "") or "").strip().lower(),
            str(actor_scope.get("email", "") or "").strip().lower(),
        }
        scoped_identities.discard("")
        if actor_username and actor_username in scoped_identities:
            return True
        if actor_email and actor_email in scoped_identities:
            return True

    for event in run_state.get("events", []):
        if not isinstance(event, dict):
            continue
        event_actor_user = str(event.get("actor_user", "") or "").strip().lower()
        if actor_username and event_actor_user == actor_username:
            return True

    actor_org_name = actor_context.get("organization_name", "")
    actor_org_id = actor_context.get("organization_id", "")
    if not actor_org_name and not actor_org_id:
        return False

    for host_row in run_state.get("host_mapping", []):
        if not isinstance(host_row, dict):
            continue
        host_org = str(host_row.get("org_id", "") or "").strip().lower()
        if host_org and host_org in {actor_org_name, actor_org_id.lower()}:
            return True

    return False


def _validate_run_context_for_critical_commands(run_state):
    if str(run_state.get("provider_key", "") or "").strip() != RUNBOOK_PROVIDER_KEY:
        return _build_runbook_error_payload(
            "runbook_scope_invalid",
            "Escopo operacional invalido: apenas external-linux e permitido.",
            {
                "provider_key": run_state.get("provider_key", ""),
                "required_provider": RUNBOOK_PROVIDER_KEY,
            },
        )
    if not _is_valid_change_context(run_state.get("change_id", "")):
        return _build_runbook_error_payload(
            "runbook_change_context_required",
            "change_id obrigatorio para comandos criticos do runbook.",
            {
                "provider_key": run_state.get("provider_key", ""),
            },
        )
    return None


def _append_access_audit(
    store_payload,
    action,
    actor_context,
    authorized,
    decision_code,
    reason,
    run_id="",
    change_id="",
    provider_key="",
):
    audit_rows = store_payload.setdefault("access_audit", [])
    if not isinstance(audit_rows, list):
        audit_rows = []

    audit_rows.append(
        {
            "timestamp_utc": _utc_now(),
            "action": action,
            "authorized": bool(authorized),
            "decision_code": decision_code,
            "reason": reason,
            "run_id": str(run_id or "").strip(),
            "change_id": str(change_id or "").strip(),
            "provider_key": str(provider_key or "").strip(),
            "actor_user": actor_context.get("username", "system"),
            "actor_role": actor_context.get("role", "user"),
            "actor_organization_id": actor_context.get("organization_id", ""),
            "actor_organization_name": actor_context.get("organization_name", ""),
        }
    )
    store_payload["access_audit"] = audit_rows[-1000:]


def _build_runbook_catalog_entry(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    run_id = str(safe_run_state.get("run_id", "") or "").strip()
    if not run_id:
        return None

    topology_catalog = safe_run_state.get("topology_catalog", {})
    if not isinstance(topology_catalog, dict):
        topology_catalog = {}

    host_mapping = _sanitize_host_mapping_collection(safe_run_state.get("host_mapping", []))
    machine_credentials = _sanitize_machine_credentials_collection(
        safe_run_state.get("machine_credentials", [])
    )
    updated_at = str(
        safe_run_state.get("updated_at_utc")
        or safe_run_state.get("completed_at_utc")
        or safe_run_state.get("started_at_utc")
        or ""
    ).strip()

    return {
        "key": run_id,
        "runId": run_id,
        "changeId": str(safe_run_state.get("change_id", "") or "").strip(),
        "status": str(safe_run_state.get("status", "") or "").strip().lower(),
        "finishedAt": str(safe_run_state.get("completed_at_utc", "") or "").strip(),
        "capturedAt": updated_at,
        "context": {
            "providerKey": str(safe_run_state.get("provider_key", "") or "").strip(),
            "environmentProfile": str(
                safe_run_state.get("environment_profile", "") or ""
            ).strip(),
            "hostCount": len(host_mapping),
            "organizationCount": len(
                topology_catalog.get("organizations", [])
                if isinstance(topology_catalog.get("organizations", []), list)
                else []
            ),
            "nodeCount": len(host_mapping),
            "apiCount": len(
                safe_run_state.get("api_registry", [])
                if isinstance(safe_run_state.get("api_registry", []), list)
                else []
            ),
            "incrementalCount": len(
                safe_run_state.get("incremental_expansions", [])
                if isinstance(safe_run_state.get("incremental_expansions", []), list)
                else []
            ),
            "organizations": [],
            "topology": topology_catalog,
            "host_mapping": host_mapping,
            "machine_credentials": machine_credentials,
            "handoff_fingerprint": str(
                safe_run_state.get("handoff_fingerprint", "") or ""
            ).strip(),
        },
    }


def _resource_keys_for_stage(run_state, stage_key):
    resource_keys = []
    for host in run_state.get("host_mapping", []):
        if not isinstance(host, dict):
            continue
        host_address = str(host.get("host_address", "")).strip().lower()
        org_id = str(host.get("org_id", "")).strip().lower()
        if host_address:
            resource_keys.append("host:{}".format(host_address))
        if org_id:
            resource_keys.append("org:{}".format(org_id))

    for api_row in run_state.get("api_registry", []):
        if not isinstance(api_row, dict):
            continue
        channel_id = str(api_row.get("channel_id", "")).strip().lower()
        if channel_id:
            resource_keys.append("channel:{}".format(channel_id))

    resource_keys.append("stage:{}:{}".format(run_state.get("run_id", ""), stage_key))
    return sorted(set([key for key in resource_keys if key]))


def _acquire_stage_resource_locks(store_payload, run_state, stage_key):
    lock_store = store_payload.setdefault("resource_locks", {})
    run_id = run_state.get("run_id", "")
    resource_keys = _resource_keys_for_stage(run_state, stage_key)
    conflicting_key = None
    for resource_key in resource_keys:
        lock_row = lock_store.get(resource_key)
        if not isinstance(lock_row, dict):
            continue
        if lock_row.get("run_id", "") != run_id:
            conflicting_key = resource_key
            break

    if conflicting_key:
        return _build_runbook_error_payload(
            "runbook_resource_lock_conflict",
            "Conflito de lock em recurso critico para execucao concorrente.",
            {
                "resource_key": conflicting_key,
                "stage": stage_key,
            },
        )

    now_utc = _utc_now()
    for resource_key in resource_keys:
        lock_store[resource_key] = {
            "run_id": run_id,
            "change_id": run_state.get("change_id", ""),
            "stage": stage_key,
            "acquired_at_utc": now_utc,
        }

    return None


def _release_stage_resource_locks(store_payload, run_state, stage_key=None):
    lock_store = store_payload.setdefault("resource_locks", {})
    run_id = run_state.get("run_id", "")
    release_keys = []
    for resource_key, lock_row in lock_store.items():
        if not isinstance(lock_row, dict):
            continue
        if lock_row.get("run_id", "") != run_id:
            continue
        if stage_key and lock_row.get("stage", "") != stage_key:
            continue
        release_keys.append(resource_key)

    for resource_key in release_keys:
        lock_store.pop(resource_key, None)


def _sanitize_container_token(value, fallback="node"):
    normalized = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    normalized = normalized.strip("-")
    if not normalized:
        normalized = fallback
    return normalized[:40]


def _resolve_runbook_container_name(run_state, host_row):
    change_token = _sanitize_container_token(run_state.get("change_id", ""), "change")
    run_token = _sanitize_container_token(run_state.get("run_id", ""), "run")
    node_token = _sanitize_container_token(
        host_row.get("node_id", "")
        or host_row.get("host_ref", "")
        or host_row.get("host_address", ""),
        "node",
    )
    container_name = "{}-{}-{}-{}".format(
        _sanitize_container_token(RUNBOOK_BASE_CONTAINER_PREFIX, "cognusrb"),
        change_token,
        run_token,
        node_token,
    )
    if len(container_name) <= 63:
        return container_name

    digest = hashlib.sha256(container_name.encode("utf-8")).hexdigest()[:10]
    compact_prefix = _sanitize_container_token(RUNBOOK_BASE_CONTAINER_PREFIX, "cognusrb")[:12]
    compact_change = change_token[:12]
    compact_run = run_token[:12]
    compact_node = node_token[:12]
    compact_name = "{}-{}-{}-{}-{}".format(
        compact_prefix,
        compact_change,
        compact_run,
        compact_node,
        digest,
    )
    return compact_name[:63]


def _parse_port_value(raw_port):
    try:
        parsed_port = int(str(raw_port or "").strip())
    except (TypeError, ValueError):
        return 0
    if parsed_port < 1 or parsed_port > 65535:
        return 0
    return parsed_port


def _resolve_runtime_exposed_host_port(run_state, host_row):
    if not isinstance(host_row, dict):
        return 0

    direct_port = _parse_port_value(
        host_row.get("runtime_port")
        or host_row.get("exposure_port")
        or host_row.get("service_port")
        or host_row.get("port")
    )
    if direct_port > 0:
        return direct_port

    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type not in RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE:
        return 0

    topology_catalog = run_state.get("topology_catalog", {})
    organizations = topology_catalog.get("organizations", [])
    if not isinstance(organizations, list):
        organizations = []

    alias_registry = _build_topology_org_alias_registry(topology_catalog)
    host_org_id = _resolve_canonical_org_id(host_row.get("org_id", ""), alias_registry)
    normalized_host_org_id = str(host_org_id or "").strip().lower()

    for organization in organizations:
        if not isinstance(organization, dict):
            continue

        organization_id = str(
            organization.get("org_id")
            or organization.get("org_name")
            or organization.get("org_key")
            or ""
        ).strip()
        organization_id = _resolve_canonical_org_id(organization_id, alias_registry)
        if normalized_host_org_id and organization_id.lower() != normalized_host_org_id:
            continue

        service_parameters = organization.get("service_parameters", {})
        if not isinstance(service_parameters, dict):
            service_parameters = {}

        if node_type == "apigateway":
            api_gateway_parameters = service_parameters.get("apiGateway", {})
            if not isinstance(api_gateway_parameters, dict):
                api_gateway_parameters = {}
            resolved_port = _parse_port_value(api_gateway_parameters.get("port"))
            if resolved_port > 0:
                return resolved_port

        if node_type == "netapi":
            netapi_parameters = service_parameters.get("netapi", {})
            if not isinstance(netapi_parameters, dict):
                netapi_parameters = {}
            resolved_port = _parse_port_value(netapi_parameters.get("port"))
            if resolved_port > 0:
                return resolved_port

    return _parse_port_value(
        RUNBOOK_RUNTIME_DEFAULT_HOST_PORT_BY_NODE_TYPE.get(node_type, "")
    )


def _infer_runtime_organization_msp_id(*candidates):
    for candidate in candidates:
        normalized_candidate = str(candidate or "").strip()
        if not normalized_candidate:
            continue
        if re.fullmatch(r"[A-Za-z0-9]+MSP", normalized_candidate):
            return normalized_candidate

        token_source = re.sub(r"[^A-Za-z0-9]+", " ", normalized_candidate).strip()
        if not token_source:
            continue

        msp_tokens = []
        for token in token_source.split():
            if not token:
                continue
            msp_tokens.append("{}{}".format(token[:1].upper(), token[1:]))
        if msp_tokens:
            return "{}MSP".format("".join(msp_tokens))
    return ""


def _resolve_topology_organization_msp_id(organization, host_row=None):
    safe_organization = organization if isinstance(organization, dict) else {}
    safe_host_row = host_row if isinstance(host_row, dict) else {}
    resolved_msp_id = _infer_runtime_organization_msp_id(
        safe_organization.get("msp_id"),
        safe_organization.get("org_msp_id"),
        safe_organization.get("mspId"),
        safe_organization.get("org_id"),
        safe_organization.get("org_name"),
        safe_organization.get("org_key"),
        safe_organization.get("label"),
        safe_organization.get("org_label"),
        safe_host_row.get("msp_id"),
        safe_host_row.get("org_msp_id"),
        safe_host_row.get("mspId"),
        safe_host_row.get("org_id"),
        safe_host_row.get("org_name"),
        safe_host_row.get("org_label"),
        safe_host_row.get("peer_localMspId"),
        safe_host_row.get("peer_local_msp_id"),
    )
    if resolved_msp_id:
        return resolved_msp_id
    return "Org1MSP"


def _resolve_topology_organization_for_host(run_state, host_row):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return {}

    topology_catalog = run_state.get("topology_catalog", {})
    if not isinstance(topology_catalog, dict):
        return {}

    organizations = topology_catalog.get("organizations", [])
    if not isinstance(organizations, list) or len(organizations) == 0:
        return {}

    alias_registry = _build_topology_org_alias_registry(topology_catalog)
    host_org_id = _resolve_canonical_org_id(host_row.get("org_id", ""), alias_registry)
    normalized_host_org_id = str(host_org_id or "").strip().lower()

    if normalized_host_org_id:
        for organization in organizations:
            if not isinstance(organization, dict):
                continue
            organization_id = str(
                organization.get("org_id")
                or organization.get("org_name")
                or organization.get("org_key")
                or ""
            ).strip()
            organization_id = _resolve_canonical_org_id(organization_id, alias_registry)
            if organization_id.lower() == normalized_host_org_id:
                resolved_organization = dict(organization)
                resolved_msp_id = _resolve_topology_organization_msp_id(
                    resolved_organization,
                    host_row,
                )
                if resolved_msp_id and not resolved_organization.get("msp_id"):
                    resolved_organization["msp_id"] = resolved_msp_id
                return resolved_organization

    if len(organizations) == 1 and isinstance(organizations[0], dict):
        resolved_organization = dict(organizations[0])
        resolved_msp_id = _resolve_topology_organization_msp_id(
            resolved_organization,
            host_row,
        )
        if resolved_msp_id and not resolved_organization.get("msp_id"):
            resolved_organization["msp_id"] = resolved_msp_id
        return resolved_organization
    return {}


def _resolve_plain_runtime_value(candidates, fallback=""):
    resolved_value = str(fallback or "").strip()
    for candidate in candidates:
        normalized_candidate = str(candidate or "").strip()
        if not normalized_candidate:
            continue
        if _is_secure_credential_reference(normalized_candidate):
            continue
        resolved_value = normalized_candidate
        break
    return resolved_value


def _resolve_couchdb_runtime_credentials(run_state, host_row):
    host_row = host_row if isinstance(host_row, dict) else {}
    admin_user = _resolve_plain_runtime_value(
        [
            host_row.get("couchdb_admin_user"),
            host_row.get("couch_admin_user"),
            host_row.get("runtime_admin_user"),
        ],
        fallback=RUNBOOK_COUCHDB_ADMIN_USER,
    )
    admin_password = _resolve_plain_runtime_value(
        [
            host_row.get("couchdb_admin_password"),
            host_row.get("couch_admin_password"),
            host_row.get("runtime_admin_password"),
        ],
        fallback=RUNBOOK_COUCHDB_ADMIN_PASSWORD,
    )

    organization = _resolve_topology_organization_for_host(run_state, host_row)
    service_parameters = organization.get("service_parameters", {})
    if not isinstance(service_parameters, dict):
        service_parameters = {}
    couch_parameters = service_parameters.get("couch", {})
    if not isinstance(couch_parameters, dict):
        couch_parameters = {}

    admin_user = _resolve_plain_runtime_value(
        [couch_parameters.get("admin_user")],
        fallback=admin_user,
    )
    admin_password = _resolve_plain_runtime_value(
        [
            couch_parameters.get("admin_password"),
            couch_parameters.get("password"),
            couch_parameters.get("admin_password_plain"),
        ],
        fallback=admin_password,
    )
    return {"admin_user": admin_user, "admin_password": admin_password}


def _resolve_runtime_docker_environment_args(run_state, host_row):
    if not isinstance(host_row, dict):
        return ""

    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    environment_pairs = []
    organization = _resolve_topology_organization_for_host(run_state, host_row)
    organization_msp_id = _resolve_topology_organization_msp_id(
        organization,
        host_row,
    )

    if node_type == "peer":
        if organization_msp_id:
            environment_pairs.append(("CORE_PEER_LOCALMSPID", organization_msp_id))
        environment_pairs.append(("CORE_PEER_MSPCONFIGPATH", "/etc/hyperledger/fabric/msp"))
        couch_host_row = _resolve_runtime_host_row_for_node_type(
            run_state,
            host_row,
            "couch",
        )
        couch_container = _resolve_runtime_container_name_for_node_type(
            run_state,
            host_row,
            "couch",
        )
        couch_credentials = _resolve_couchdb_runtime_credentials(
            run_state,
            couch_host_row if isinstance(couch_host_row, dict) else host_row,
        )
        if couch_container:
            environment_pairs.extend(
                [
                    ("CORE_LEDGER_STATE_STATEDATABASE", "CouchDB"),
                    (
                        "CORE_LEDGER_STATE_COUCHDBCONFIG_COUCHDBADDRESS",
                        "{}:5984".format(couch_container),
                    ),
                    (
                        "CORE_LEDGER_STATE_COUCHDBCONFIG_USERNAME",
                        couch_credentials.get("admin_user", ""),
                    ),
                    (
                        "CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD",
                        couch_credentials.get("admin_password", ""),
                    ),
                ]
            )

    if node_type == "orderer":
        if organization_msp_id:
            environment_pairs.append(("ORDERER_GENERAL_LOCALMSPID", organization_msp_id))
        environment_pairs.append(("ORDERER_GENERAL_LOCALMSPDIR", "/etc/hyperledger/fabric/msp"))
        if RUNBOOK_ORDERER_LISTEN_ADDRESS:
            environment_pairs.append(
                ("ORDERER_GENERAL_LISTENADDRESS", RUNBOOK_ORDERER_LISTEN_ADDRESS)
            )
        tls_enabled = False
        for env_key, env_val in host_row.get("environment", []):
            if env_key == "ORDERER_GENERAL_TLS_ENABLED" and str(env_val).strip().lower() in {"1", "true", "yes", "on"}:
                tls_enabled = True
        # Se TLS não foi explicitamente habilitado no host mapping, mantém transporte em plaintext
        # para alinhar com o peer dev padrão (CORE_PEER_TLS_ENABLED=false).
        if not tls_enabled:
            environment_pairs.append(("ORDERER_GENERAL_TLS_ENABLED", "false"))
            environment_pairs.append(("ORDERER_CHANNELPARTICIPATION_ENABLED", "true"))
            environment_pairs.append(("ORDERER_GENERAL_LISTENADDRESS", RUNBOOK_ORDERER_LISTEN_ADDRESS))
            environment_pairs.append(("ORDERER_GENERAL_BOOTSTRAPMETHOD", "none"))
            environment_pairs.append(("ORDERER_GENERAL_GENESISFILE", ""))
            environment_pairs.append(("ORDERER_GENERAL_GENESISMETHOD", "none"))
            environment_pairs.append(("ORDERER_GENERAL_BOOTSTRAPFILE", ""))
            environment_pairs.append(("ORDERER_GENERAL_TLS_PRIVATEKEY", "/etc/hyperledger/fabric/tls/server.key"))
            environment_pairs.append(("ORDERER_GENERAL_TLS_CERTIFICATE", "/etc/hyperledger/fabric/tls/server.crt"))
            environment_pairs.append(("ORDERER_GENERAL_TLS_ROOTCAS", "/etc/hyperledger/fabric/tls/ca.crt"))
            environment_pairs.append(("ORDERER_GENERAL_TLS_CLIENTAUTHREQUIRED", "false"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_LISTENADDRESS", RUNBOOK_ORDERER_LISTEN_ADDRESS))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_LISTENPORT", "7051"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_SERVERCERTIFICATE", "/etc/hyperledger/fabric/tls/server.crt"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_SERVERPRIVATEKEY", "/etc/hyperledger/fabric/tls/server.key"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_CLIENTCERTIFICATE", "/etc/hyperledger/fabric/tls/server.crt"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_CLIENTPRIVATEKEY", "/etc/hyperledger/fabric/tls/server.key"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_ROOTCAS", "/etc/hyperledger/fabric/tls/ca.crt"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_DIALTIMEOUT", "5s"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_RPCTIMEOUT", "7s"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_REPLICATIONBUFFERSIZE", "20971520"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_REPLICATIONPULLTIMEOUT", "5s"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_REPLICATIONRETRYTIMEOUT", "5s"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_REPLICATIONBACKGROUNDREFRESHINTERVAL", "5m0s"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_REPLICATIONMAXRETRIES", "12"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_SENDBUFFERSIZE", "100"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_CERTEXPIRATIONWARNINGTHRESHOLD", "168h0m0s"))
            environment_pairs.append(("ORDERER_GENERAL_CLUSTER_TLSHANDSHAKETIMESHIFT", "0s"))
        else:
            if RUNBOOK_ORDERER_FORCE_CHANNEL_PARTICIPATION:
                environment_pairs.append(
                    ("ORDERER_GENERAL_BOOTSTRAPMETHOD", RUNBOOK_ORDERER_BOOTSTRAP_METHOD)
                )
                environment_pairs.append(
                    (
                        "ORDERER_CHANNELPARTICIPATION_ENABLED",
                        "true" if RUNBOOK_ORDERER_CHANNEL_PARTICIPATION_ENABLED else "false",
                    )
                )

    if node_type == "couch":
        couch_credentials = _resolve_couchdb_runtime_credentials(run_state, host_row)
        environment_pairs.append(("COUCHDB_USER", couch_credentials.get("admin_user", "")))
        environment_pairs.append(
            ("COUCHDB_PASSWORD", couch_credentials.get("admin_password", ""))
        )

    if node_type in ("apigateway", "netapi"):
        org_id = str(
            host_row.get("org_id")
            or organization.get("org_id")
            or organization.get("org_name")
            or ""
        ).strip().lower()
        if org_id:
            environment_pairs.append(("DEFAULT_ORG", org_id))
        gateway_port = "8085" if node_type == "apigateway" else "3000"
        gateway_data_mount = (
            "/app/data"
            if node_type == "apigateway"
            else "/workspace/chaincode-gateway/data"
        )
        environment_pairs.extend(
            [
                ("PORT", gateway_port),
                ("IDENTITY_CONFIG", f"{gateway_data_mount}/identities.json"),
                ("CONNECTION_PROFILE", f"{gateway_data_mount}/connection.json"),
                ("CCP_PATH", f"{gateway_data_mount}/connection.json"),
                ("WALLET_PATH", f"{gateway_data_mount}/wallet"),
                (
                    "NODE_EXTRA_CA_CERTS",
                    f"{gateway_data_mount}/msp/tlscacerts/tlsca-cert.pem",
                ),
            ]
        )

    environment_args = []
    for environment_key, environment_value in environment_pairs:
        normalized_key = str(environment_key or "").strip()
        normalized_value = str(environment_value or "").strip()
        if not normalized_key or not normalized_value:
            continue
        environment_args.append(
            "--env {}={}".format(normalized_key, shlex.quote(normalized_value))
        )

    return "{} ".format(" ".join(environment_args)) if environment_args else ""


def _resolve_runtime_docker_volume_args(run_state, host_row):
    """Resolve host->container volume mounts useful for runtime (identities, data).
    Returns a string with `--volume` args (including trailing space) or empty string.
    """
    if not isinstance(host_row, dict):
        return ""

    # Try to find organization identity locations from topology catalog
    org = _resolve_topology_organization_for_host(run_state, host_row)
    if not isinstance(org, dict):
        org = {}

    identity = org.get("identity") if isinstance(org.get("identity"), dict) else {}

    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))

    host_identity_path = _resolve_runtime_host_identity_path(
        run_state,
        host_row,
        organization=org,
        identity=identity,
    )

    mounts = []
    if host_identity_path:
        # Peer CLI/server logic expects the effective MSP under /etc/hyperledger/fabric/msp.
        # Keep /var/lib/cognus/msp mirrored as well because other repair paths inspect it.
        if node_type == "peer":
            mounts.append((host_identity_path, "/var/lib/cognus/msp"))
            mounts.append((host_identity_path, "/etc/hyperledger/fabric/msp"))
        else:
            mounts.append((host_identity_path, "/var/lib/cognus/msp"))

    # Also allow mounting a prepared chaincode-gateway data dir if provided.
    # For apigateway we always enforce /app/data persistence and derive a deterministic
    # host path when not explicitly configured.
    host_gateway_data = str(host_row.get("gateway_data_path") or "").strip()
    if node_type in ("apigateway", "netapi") and not host_gateway_data:
        node_token = _sanitize_container_token(
            host_row.get("node_id", "")
            or host_row.get("host_ref", "")
            or host_row.get("host_address", ""),
            "gateway",
        )
        host_gateway_data = f"/tmp/cognus/runtime/apigateway/{node_token}"
    if host_gateway_data:
        gateway_mount = "/app/data" if node_type == "apigateway" else "/workspace/chaincode-gateway/data"
        mounts.append((host_gateway_data, gateway_mount))

    if node_type == "peer":
        mounts.append(("/var/run/docker.sock", "/var/run/docker.sock"))

    if not mounts:
        return ""

    volume_args = []
    for host_path, container_path in mounts:
        # sanitize and quote
        host_q = shlex.quote(host_path)
        container_q = shlex.quote(container_path)
        volume_args.append(f"--volume {host_q}:{container_q}")

    return "{} ".format(" ".join(volume_args))


def _resolve_apigateway_runtime_host_dir(run_state, host_row):
    if not isinstance(host_row, dict):
        return ""
    explicit = str(host_row.get("gateway_data_path") or "").strip()
    if explicit:
        return explicit
    node_token = _sanitize_container_token(
        host_row.get("node_id", "")
        or host_row.get("host_ref", "")
        or host_row.get("host_address", ""),
        "gateway",
    )
    return f"/tmp/cognus/runtime/apigateway/{node_token}"


def _collect_apigateway_org_scope(run_state, host_row):
    organization = _resolve_topology_organization_for_host(run_state, host_row)
    org_id = str(
        host_row.get("org_id")
        or organization.get("org_id")
        or organization.get("org_name")
        or ""
    ).strip().lower()
    channel_ids = []
    channel_seen = set()
    for channel_row in organization.get("channels", []) if isinstance(organization.get("channels", []), list) else []:
        channel_id = ""
        if isinstance(channel_row, dict):
            channel_id = str(
                channel_row.get("channel_id")
                or channel_row.get("name")
                or channel_row.get("id")
                or ""
            ).strip().lower()
        else:
            channel_id = str(channel_row or "").strip().lower()
        if channel_id and channel_id not in channel_seen:
            channel_seen.add(channel_id)
            channel_ids.append(channel_id)

    chaincode_ids = []
    chaincode_seen = set()
    for chaincode_id in organization.get("chaincodes", []) if isinstance(organization.get("chaincodes", []), list) else []:
        normalized = str(chaincode_id or "").strip().lower()
        if normalized and normalized not in chaincode_seen:
            chaincode_seen.add(normalized)
            chaincode_ids.append(normalized)

    apis = organization.get("apis", []) if isinstance(organization.get("apis"), list) else []
    for api_row in apis:
        if not isinstance(api_row, dict):
            continue
        api_channel = str(api_row.get("channel_id", "") or "").strip().lower()
        if api_channel and api_channel not in channel_seen:
            channel_seen.add(api_channel)
            channel_ids.append(api_channel)
        api_chaincode = str(api_row.get("chaincode_id", "") or "").strip().lower()
        if api_chaincode and api_chaincode not in chaincode_seen:
            chaincode_seen.add(api_chaincode)
            chaincode_ids.append(api_chaincode)

    def _append_channel_candidate(candidate):
        if isinstance(candidate, dict):
            normalized = str(
                candidate.get("channel_id")
                or candidate.get("name")
                or candidate.get("id")
                or ""
            ).strip().lower()
        else:
            normalized = str(candidate or "").strip().lower()
        if normalized and normalized not in channel_seen:
            channel_seen.add(normalized)
            channel_ids.append(normalized)

    def _append_chaincode_candidate(candidate):
        if isinstance(candidate, dict):
            normalized = str(
                candidate.get("chaincode_id")
                or candidate.get("name")
                or candidate.get("id")
                or ""
            ).strip().lower()
        else:
            normalized = str(candidate or "").strip().lower()
        if normalized and normalized not in chaincode_seen:
            chaincode_seen.add(normalized)
            chaincode_ids.append(normalized)

    def _apply_service_context_scope(service_context):
        if not isinstance(service_context, dict):
            return

        for key in ("channel_ids", "channels", "channel"):
            value = service_context.get(key)
            if isinstance(value, list):
                for item in value:
                    _append_channel_candidate(item)
            elif value:
                _append_channel_candidate(value)

        for key in ("chaincode_ids", "chaincodes", "chaincode"):
            value = service_context.get(key)
            if isinstance(value, list):
                for item in value:
                    _append_chaincode_candidate(item)
            elif value:
                _append_chaincode_candidate(value)

        service_apis = service_context.get("apis", [])
        if isinstance(service_apis, list):
            for api_row in service_apis:
                if not isinstance(api_row, dict):
                    continue
                _append_channel_candidate(api_row.get("channel_id") or api_row.get("channel"))
                _append_chaincode_candidate(api_row.get("chaincode_id") or api_row.get("chaincode"))

    _apply_service_context_scope(host_row.get("service_context"))
    _apply_service_context_scope(run_state.get("service_context"))

    provision_execution_plan = run_state.get("provision_execution_plan", {})
    entries = provision_execution_plan.get("entries", []) if isinstance(provision_execution_plan, dict) else []
    host_node_id = str(host_row.get("node_id") or "").strip().lower()
    host_org_id = str(org_id or "").strip().lower()
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict):
            continue
        service_context = entry.get("service_context")
        if not isinstance(service_context, dict):
            continue

        entry_runtime_name = str(entry.get("runtime_name") or "").strip().lower()
        entry_org_id = str(service_context.get("org_id") or "").strip().lower()

        if host_node_id and entry_runtime_name and entry_runtime_name != host_node_id:
            continue
        if host_org_id and entry_org_id and entry_org_id != host_org_id:
            continue

        _apply_service_context_scope(service_context)

    explicit_org_channels = set(channel_ids)
    for metadata_row in _collect_runbook_chaincode_metadata_rows(run_state):
        if not isinstance(metadata_row, dict):
            continue
        metadata_channel_id = str(
            metadata_row.get("channel_id")
            or metadata_row.get("channel")
            or metadata_row.get("target_channel")
            or ""
        ).strip().lower()
        if (
            explicit_org_channels
            and metadata_channel_id
            and metadata_channel_id not in explicit_org_channels
        ):
            continue
        _append_channel_candidate(metadata_channel_id)
        _append_chaincode_candidate(metadata_row.get("chaincode_id", ""))

    return org_id, channel_ids, chaincode_ids, organization


def _normalize_chaincode_metadata_token(value):
    return str(value or "").strip()


def _normalize_chaincode_metadata_key(value):
    return _normalize_chaincode_metadata_token(value).lower()


def _strip_chaincode_archive_suffix(file_name):
    normalized_file_name = os.path.basename(str(file_name or "").strip())
    lowered_file_name = normalized_file_name.lower()
    for suffix in (".tar.gz", ".tgz", ".tar"):
        if lowered_file_name.endswith(suffix):
            return normalized_file_name[: -len(suffix)]
    return normalized_file_name


def _extract_chaincode_version_from_archive_name(file_name):
    stem = _strip_chaincode_archive_suffix(file_name)
    if not stem:
        return ""
    version_match = re.search(r"(?:[_-]|^)(v?\d+(?:\.\d+){0,3})$", stem, re.IGNORECASE)
    if not version_match:
        return ""
    return str(version_match.group(1) or "").lstrip("vV").strip()


RUNBOOK_CHAINCODE_ENV_SUFFIXES = {
    "dev",
    "hml",
    "hom",
    "prod",
    "prd",
    "qa",
    "test",
    "tst",
    "stage",
    "staging",
    "sandbox",
    "sbx",
}
RUNBOOK_CHAINCODE_ALIAS_IGNORED_TOKENS = {
    "cc",
    "chaincode",
    *RUNBOOK_CHAINCODE_ENV_SUFFIXES,
}


def _strip_chaincode_environment_suffix(value):
    normalized_value = _normalize_chaincode_metadata_key(value)
    if not normalized_value:
        return ""
    tokens = [
        token
        for token in re.split(r"[-_]+", normalized_value)
        if str(token or "").strip()
    ]
    while tokens and tokens[-1] in RUNBOOK_CHAINCODE_ENV_SUFFIXES:
        tokens.pop()
    return "-".join(tokens)


def _collect_chaincode_metadata_aliases(*values, include_fragments=True):
    aliases = []

    def _append_alias(raw_alias):
        normalized_alias = _normalize_chaincode_metadata_key(raw_alias)
        if normalized_alias and normalized_alias not in aliases:
            aliases.append(normalized_alias)

    for value in values:
        normalized_value = _normalize_chaincode_metadata_token(value)
        if not normalized_value:
            continue
        normalized_stem = _strip_chaincode_archive_suffix(normalized_value)
        envless_alias = _strip_chaincode_environment_suffix(normalized_stem)
        _append_alias(normalized_value)
        _append_alias(normalized_stem)
        _append_alias(envless_alias)

    if include_fragments:
        for alias in list(aliases):
            for token in re.split(r"[-_]+", alias):
                normalized_token = _normalize_chaincode_metadata_key(token)
                if (
                    normalized_token
                    and len(normalized_token) >= 4
                    and normalized_token not in RUNBOOK_CHAINCODE_ALIAS_IGNORED_TOKENS
                ):
                    _append_alias(normalized_token)

    return aliases


def _coerce_runbook_chaincode_metadata_row(row):
    if not isinstance(row, dict):
        return {}

    channel_id = _normalize_chaincode_metadata_token(
        row.get("channel_id")
        or row.get("channel")
        or row.get("target_channel")
        or row.get("channel_name")
        or row.get("channelId")
    )
    chaincode_id = _normalize_chaincode_metadata_token(
        row.get("chaincode_id")
        or row.get("chaincode")
        or row.get("chaincode_name")
        or row.get("chaincodeId")
        or row.get("name")
        or row.get("id")
    )
    package_pattern = _normalize_chaincode_metadata_token(
        row.get("package_pattern") or row.get("packagePattern")
    )
    package_file_name = _normalize_chaincode_metadata_token(
        row.get("package_file_name")
        or row.get("packageFileName")
        or row.get("package_file")
        or row.get("packageFile")
        or row.get("artifact_file_name")
        or row.get("artifactFileName")
    )
    artifact_ref = _normalize_chaincode_metadata_token(
        row.get("artifact_ref")
        or row.get("artifactRef")
        or row.get("artifact_path")
        or row.get("artifactPath")
        or row.get("package_ref")
        or row.get("packageRef")
        or row.get("package_path")
        or row.get("packagePath")
    )
    source_ref = _normalize_chaincode_metadata_token(
        row.get("source_ref")
        or row.get("sourceRef")
        or row.get("source_path")
        or row.get("sourcePath")
        or row.get("source_dir")
        or row.get("sourceDir")
    )
    version = _normalize_chaincode_metadata_token(
        row.get("chaincode_version")
        or row.get("chaincodeVersion")
        or row.get("version")
    )
    sequence = _normalize_chaincode_metadata_token(
        row.get("chaincode_sequence")
        or row.get("chaincodeSequence")
        or row.get("sequence")
    )
    runtime_mode = _normalize_chaincode_metadata_key(
        row.get("runtime_mode") or row.get("runtimeMode")
    )
    if runtime_mode not in {"auto", "legacy", "ccaas"}:
        runtime_mode = ""

    if not version:
        version = _extract_chaincode_version_from_archive_name(package_file_name)

    return {
        "channel_id": channel_id,
        "chaincode_id": chaincode_id,
        "package_pattern": package_pattern,
        "package_file_name": package_file_name,
        "artifact_ref": artifact_ref,
        "source_ref": source_ref,
        "version": version,
        "sequence": sequence,
        "runtime_mode": runtime_mode,
    }


def _collect_runbook_chaincode_metadata_rows(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    collected_rows = []

    def _append_rows(rows):
        for row in rows if isinstance(rows, list) else []:
            normalized_row = _coerce_runbook_chaincode_metadata_row(row)
            if any(
                str(normalized_row.get(key, "") or "").strip()
                for key in (
                    "channel_id",
                    "chaincode_id",
                    "package_pattern",
                    "package_file_name",
                    "artifact_ref",
                    "source_ref",
                    "version",
                    "sequence",
                    "runtime_mode",
                )
            ):
                collected_rows.append(normalized_row)

    topology_catalog = safe_run_state.get("topology_catalog", {})
    if isinstance(topology_catalog, dict):
        _append_rows(topology_catalog.get("chaincodes", []))

    handoff_payload = safe_run_state.get("handoff_payload", {})
    if not isinstance(handoff_payload, dict):
        handoff_payload = {}

    network_payload = handoff_payload.get("network", {})
    if isinstance(network_payload, dict):
        _append_rows(network_payload.get("chaincodes", []))
        _append_rows(network_payload.get("chaincodes_install", []))

    guided_blueprint_draft = handoff_payload.get("guided_blueprint_draft", {})
    if isinstance(guided_blueprint_draft, dict):
        guided_network_payload = guided_blueprint_draft.get("network", {})
        if isinstance(guided_network_payload, dict):
            _append_rows(guided_network_payload.get("chaincodes", []))
            _append_rows(guided_network_payload.get("chaincodes_install", []))

    return collected_rows


def _resolve_chaincode_artifact_metadata(run_state, channel_id, chaincode_id):
    normalized_channel_id = _normalize_chaincode_metadata_key(channel_id)
    normalized_chaincode_id = _normalize_chaincode_metadata_key(chaincode_id)
    search_terms = []
    candidate_files = []
    artifact_refs = []
    source_refs = []
    resolved_version = str(RUNBOOK_CHAINCODE_AUTODEFINE_VERSION or "").strip()
    resolved_sequence = str(RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE or "").strip()
    resolved_runtime_mode = ""

    def _append_unique(target_list, value):
        normalized_value = _normalize_chaincode_metadata_token(value)
        if not normalized_value:
            return
        if normalized_value not in target_list:
            target_list.append(normalized_value)

    def _entry_matches(entry):
        if not isinstance(entry, dict):
            return False
        entry_channel_id = _normalize_chaincode_metadata_key(entry.get("channel_id", ""))
        if normalized_channel_id and entry_channel_id and entry_channel_id != normalized_channel_id:
            return False

        requested_aliases = set(
            _collect_chaincode_metadata_aliases(
                normalized_chaincode_id,
                include_fragments=False,
            )
        )

        entry_chaincode_key = _normalize_chaincode_metadata_key(entry.get("chaincode_id", ""))
        entry_package_pattern_key = _normalize_chaincode_metadata_key(
            entry.get("package_pattern", "")
        )
        package_file_name = _normalize_chaincode_metadata_token(
            entry.get("package_file_name", "")
        )
        package_file_key = _normalize_chaincode_metadata_key(package_file_name)
        package_stem_key = _normalize_chaincode_metadata_key(
            _strip_chaincode_archive_suffix(package_file_name)
        )

        entry_aliases = set(
            _collect_chaincode_metadata_aliases(
                entry.get("chaincode_id", ""),
                package_file_name,
                include_fragments=False,
            )
        )
        if requested_aliases.intersection(entry_aliases):
            return True
        for requested_alias in requested_aliases:
            if requested_alias and (
                requested_alias in package_file_key
                or requested_alias in package_stem_key
            ):
                return True
        return False

    for alias in _collect_chaincode_metadata_aliases(normalized_chaincode_id):
        _append_unique(search_terms, alias)
    metadata_rows = _collect_runbook_chaincode_metadata_rows(run_state)
    for metadata_row in metadata_rows:
        if not _entry_matches(metadata_row):
            continue

        for alias in _collect_chaincode_metadata_aliases(
            metadata_row.get("chaincode_id", ""),
            metadata_row.get("package_file_name", ""),
        ):
            _append_unique(search_terms, alias)
        _append_unique(candidate_files, metadata_row.get("package_file_name", ""))
        _append_unique(artifact_refs, metadata_row.get("artifact_ref", ""))
        _append_unique(source_refs, metadata_row.get("source_ref", ""))

        row_version = _normalize_chaincode_metadata_token(metadata_row.get("version", ""))
        if row_version:
            resolved_version = row_version
        row_sequence = _normalize_chaincode_metadata_token(metadata_row.get("sequence", ""))
        if row_sequence:
            resolved_sequence = row_sequence
        row_runtime_mode = _normalize_chaincode_metadata_key(
            metadata_row.get("runtime_mode", "")
        )
        if row_runtime_mode in {"auto", "legacy", "ccaas"}:
            resolved_runtime_mode = row_runtime_mode

    if not resolved_version:
        resolved_version = str(RUNBOOK_CHAINCODE_AUTODEFINE_VERSION or "").strip()
    if not resolved_sequence:
        resolved_sequence = str(RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE or "").strip()

    if not candidate_files and resolved_version:
        for alias in _collect_chaincode_metadata_aliases(
            normalized_chaincode_id,
            include_fragments=False,
        ):
            _append_unique(candidate_files, "{}_{}.tar.gz".format(alias, resolved_version))

    return {
        "chaincode_id": normalized_chaincode_id,
        "channel_id": normalized_channel_id,
        "search_terms": search_terms,
        "candidate_files": candidate_files,
        "artifact_refs": artifact_refs,
        "source_refs": source_refs,
        "version": resolved_version,
        "sequence": resolved_sequence,
        "runtime_mode": resolved_runtime_mode,
    }


def _normalize_chaincode_local_binding_reference(value):
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return ""
    if normalized_value.lower().startswith("local-file:"):
        normalized_value = normalized_value.split(":", 1)[1].strip()
    elif "://" in normalized_value:
        return ""
    normalized_value = os.path.expandvars(os.path.expanduser(normalized_value))
    if not normalized_value or not os.path.isabs(normalized_value):
        return ""
    return normalized_value


def _collect_explicit_chaincode_binding_paths(chaincode_artifact_metadata, field_name):
    if not isinstance(chaincode_artifact_metadata, dict):
        return []

    normalized_field_name = str(field_name or "").strip()
    if not normalized_field_name:
        return []

    collected_paths = []
    for raw_value in chaincode_artifact_metadata.get(normalized_field_name, []) or []:
        normalized_path = _normalize_chaincode_local_binding_reference(raw_value)
        if normalized_path and normalized_path not in collected_paths:
            collected_paths.append(normalized_path)
    return collected_paths


def _chaincode_artifact_metadata_has_explicit_binding(chaincode_artifact_metadata):
    return bool(
        _collect_explicit_chaincode_binding_paths(
            chaincode_artifact_metadata,
            "artifact_refs",
        )
        or _collect_explicit_chaincode_binding_paths(
            chaincode_artifact_metadata,
            "source_refs",
        )
    )


def _chaincode_artifact_metadata_has_explicit_artifact_binding(chaincode_artifact_metadata):
    return bool(
        _collect_explicit_chaincode_binding_paths(
            chaincode_artifact_metadata,
            "artifact_refs",
        )
    )


def _chaincode_artifact_metadata_declares_binding(chaincode_artifact_metadata):
    if not isinstance(chaincode_artifact_metadata, dict):
        return False

    for field_name in ("artifact_refs", "source_refs"):
        raw_values = chaincode_artifact_metadata.get(field_name, [])
        if not isinstance(raw_values, list):
            continue
        if any(_normalize_chaincode_metadata_token(value) for value in raw_values):
            return True
    return False


def _collect_chaincode_identity_aliases_from_metadata(
    chaincode_artifact_metadata,
    include_fragments=True,
):
    if not isinstance(chaincode_artifact_metadata, dict):
        return []

    raw_values = []
    chaincode_id = _normalize_chaincode_metadata_token(
        chaincode_artifact_metadata.get("chaincode_id", "")
    )
    if chaincode_id:
        raw_values.append(chaincode_id)
    else:
        raw_values.extend(chaincode_artifact_metadata.get("candidate_files", []))

    aliases = []
    for raw_value in raw_values:
        for alias in _collect_chaincode_metadata_aliases(
            raw_value,
            include_fragments=include_fragments,
        ):
            if alias and alias not in aliases:
                aliases.append(alias)
    return aliases


def _read_chaincode_archive_metadata_label(archive_path):
    safe_archive_path = str(archive_path or "").strip()
    if not safe_archive_path:
        return ""
    try:
        with tarfile.open(safe_archive_path, "r:*") as archive:
            metadata_member = archive.extractfile("metadata.json")
            if metadata_member is None:
                return ""
            payload = metadata_member.read()
            if not payload:
                return ""
            metadata = json.loads(payload.decode("utf-8", errors="replace"))
            if not isinstance(metadata, dict):
                return ""
            return _normalize_chaincode_metadata_token(metadata.get("label", ""))
    except Exception:
        return ""


def _chaincode_archive_matches_expected_identity(
    archive_path,
    chaincode_artifact_metadata,
):
    if _chaincode_artifact_metadata_has_explicit_artifact_binding(
        chaincode_artifact_metadata
    ):
        return True

    expected_aliases = set(
        _collect_chaincode_identity_aliases_from_metadata(
            chaincode_artifact_metadata,
            include_fragments=False,
        )
    )
    if not expected_aliases:
        return True

    archive_label = _read_chaincode_archive_metadata_label(archive_path)
    if not archive_label:
        return False

    archive_aliases = set(
        _collect_chaincode_metadata_aliases(
            archive_label,
            include_fragments=False,
        )
    )
    if expected_aliases.intersection(archive_aliases):
        return True

    normalized_label = _normalize_chaincode_metadata_key(archive_label)
    return any(
        expected_alias and expected_alias in normalized_label
        for expected_alias in expected_aliases
    )


def _sanitize_docker_tag_token(value, fallback="tag"):
    normalized = re.sub(r"[^a-z0-9_.-]+", "-", str(value or "").strip().lower())
    normalized = normalized.strip(".-")
    if not normalized:
        normalized = fallback
    return normalized[:64]


def _resolve_chaincode_runtime_mode(chaincode_artifact_metadata):
    if not isinstance(chaincode_artifact_metadata, dict):
        return "legacy"

    declared_runtime_mode = _normalize_chaincode_metadata_key(
        chaincode_artifact_metadata.get("runtime_mode", "")
    )
    if declared_runtime_mode in {"legacy", "ccaas"}:
        return declared_runtime_mode

    configured_runtime_mode = str(RUNBOOK_CHAINCODE_RUNTIME_MODE or "").strip().lower()
    if configured_runtime_mode in {"legacy", "ccaas"}:
        return configured_runtime_mode

    if _chaincode_artifact_metadata_has_explicit_binding(chaincode_artifact_metadata):
        return "ccaas"
    return "legacy"


def _resolve_chaincode_ccaas_runtime_identity(
    run_state,
    host_row,
    channel_id,
    chaincode_id,
    chaincode_artifact_metadata,
):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    safe_host_row = host_row if isinstance(host_row, dict) else {}
    safe_metadata = (
        chaincode_artifact_metadata if isinstance(chaincode_artifact_metadata, dict) else {}
    )

    normalized_channel_id = str(channel_id or "").strip().lower()
    normalized_chaincode_id = str(chaincode_id or "").strip().lower()
    version = (
        _normalize_chaincode_metadata_token(safe_metadata.get("version", ""))
        or str(RUNBOOK_CHAINCODE_AUTODEFINE_VERSION or "").strip()
        or "1.0"
    )

    org_token = _sanitize_container_token(safe_host_row.get("org_id", ""), "org")
    channel_token = _sanitize_container_token(normalized_channel_id, "channel")
    chaincode_token = _sanitize_container_token(normalized_chaincode_id, "chaincode")
    version_token = _sanitize_docker_tag_token(version, "v")
    digest_material = json.dumps(
        {
            "org_id": str(safe_host_row.get("org_id", "") or "").strip().lower(),
            "channel_id": normalized_channel_id,
            "chaincode_id": normalized_chaincode_id,
            "version": version,
            "artifact_refs": list(safe_metadata.get("artifact_refs", []) or []),
            "source_refs": list(safe_metadata.get("source_refs", []) or []),
            "candidate_files": list(safe_metadata.get("candidate_files", []) or []),
        },
        sort_keys=True,
    )
    identity_digest = hashlib.sha256(digest_material.encode("utf-8")).hexdigest()[:12]

    base_container_name = "cognus-ccaas-{}-{}-{}-{}-{}".format(
        org_token[:10],
        channel_token[:12],
        chaincode_token[:12],
        version_token[:12],
        identity_digest,
    )
    container_name = base_container_name[:63].rstrip("-") or base_container_name[:63]

    image_repo = "cognus/ccaas-{}-{}-{}".format(
        org_token[:12],
        channel_token[:18],
        chaincode_token[:18],
    ).rstrip("-")
    image_name = "{}:{}-{}".format(
        image_repo[:120],
        version_token[:40],
        identity_digest,
    )
    external_label = "{}_{}".format(normalized_chaincode_id, version)
    work_root = "/tmp/cognus/ccaas/{}".format(container_name)
    external_package_name = "{}-external.tgz".format(container_name)
    external_package_host_path = "{}/{}".format(work_root, external_package_name)
    external_package_peer_path = "/tmp/cognus/{}".format(external_package_name)
    service_address = "{}:{}".format(
        container_name,
        RUNBOOK_CHAINCODE_CCAAS_SERVICE_PORT,
    )

    return {
        "container_name": container_name,
        "image_name": image_name,
        "external_label": external_label,
        "work_root": work_root,
        "external_package_name": external_package_name,
        "external_package_host_path": external_package_host_path,
        "external_package_peer_path": external_package_peer_path,
        "service_address": service_address,
        "service_port": str(RUNBOOK_CHAINCODE_CCAAS_SERVICE_PORT),
        "version": version,
        "digest": identity_digest,
    }


def _build_chaincode_ccaas_adapter_source():
    return """package main

import (
\t"errors"
\t"os"
\t"strings"

\t"github.com/hyperledger/fabric-chaincode-go/shim"
)

func startCognusCCAAS(cc shim.Chaincode) error {
\tccid := firstNonEmpty(os.Getenv("CHAINCODE_ID"), os.Getenv("CORE_CHAINCODE_ID_NAME"))
\tif ccid == "" {
\t\treturn errors.New("missing chaincode package id for CCAAS runtime")
\t}

\taddress := firstNonEmpty(os.Getenv("CHAINCODE_SERVER_ADDRESS"), "0.0.0.0:9999")
\tserver := &shim.ChaincodeServer{
\t\tCCID:    ccid,
\t\tAddress: address,
\t\tCC:      cc,
\t\tTLSProps: shim.TLSProperties{
\t\t\tDisabled: true,
\t\t},
\t}
\treturn server.Start()
}

func firstNonEmpty(values ...string) string {
\tfor _, value := range values {
\t\ttrimmed := strings.TrimSpace(value)
\t\tif trimmed != "" {
\t\t\treturn trimmed
\t\t}
\t}
\treturn ""
}
"""


def _build_chaincode_ccaas_dockerfile_source():
    return """ARG COGNUS_GO_IMAGE=golang:1.22
ARG COGNUS_RUNTIME_BASE_IMAGE=alpine:3.20

FROM ${COGNUS_GO_IMAGE} AS builder
WORKDIR /workspace/src
COPY app/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/chaincode .

FROM ${COGNUS_RUNTIME_BASE_IMAGE}
WORKDIR /home/chaincode
COPY --from=builder /out/chaincode /usr/local/bin/chaincode
ENV CHAINCODE_SERVER_ADDRESS=0.0.0.0:9999
ENTRYPOINT ["/usr/local/bin/chaincode"]
"""


def _build_chaincode_ccaas_lifecycle_check(
    run_state,
    host_row,
    channel_id,
    chaincode_id,
    chaincode_artifact_metadata,
    lifecycle_failure_clause,
):
    identity = _resolve_chaincode_ccaas_runtime_identity(
        run_state,
        host_row,
        channel_id,
        chaincode_id,
        chaincode_artifact_metadata,
    )
    chaincode_package_lookup_step = _build_chaincode_package_lookup_step(
        "COGNUS_CC_PACKAGE_HOST",
        chaincode_artifact_metadata.get("search_terms", []),
        chaincode_artifact_metadata.get("candidate_files", []),
    )
    chaincode_package_id_lookup_step = _build_chaincode_package_id_lookup_step(
        "COGNUS_CC_PACKAGE_ID",
        "COGNUS_CC_QUERYINSTALLED",
        chaincode_artifact_metadata.get("search_terms", []),
    )

    adapter_b64 = base64.b64encode(
        _build_chaincode_ccaas_adapter_source().encode("utf-8")
    ).decode("ascii")
    dockerfile_b64 = base64.b64encode(
        _build_chaincode_ccaas_dockerfile_source().encode("utf-8")
    ).decode("ascii")
    orderer_host_override_q = shlex.quote(RUNBOOK_ORDERER_TLS_HOST_OVERRIDE)
    chaincode_version_q = shlex.quote(
        str(identity.get("version", RUNBOOK_CHAINCODE_AUTODEFINE_VERSION) or "")
    )
    chaincode_sequence_q = shlex.quote(
        _normalize_chaincode_metadata_token(
            chaincode_artifact_metadata.get(
                "sequence", RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE
            )
        )
        or str(RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE or "").strip()
    )
    work_root_q = shlex.quote(identity["work_root"])
    external_package_host_path_q = shlex.quote(identity["external_package_host_path"])
    external_package_peer_path_q = shlex.quote(identity["external_package_peer_path"])
    service_address_q = shlex.quote(identity["service_address"])
    external_label_q = shlex.quote(identity["external_label"])
    container_name_q = shlex.quote(identity["container_name"])
    image_name_q = shlex.quote(identity["image_name"])
    chaincode_q = shlex.quote(str(chaincode_id or "").strip().lower())
    channel_q = shlex.quote(str(channel_id or "").strip().lower())
    adapter_b64_q = shlex.quote(adapter_b64)
    dockerfile_b64_q = shlex.quote(dockerfile_b64)
    go_image_q = shlex.quote(RUNBOOK_CHAINCODE_CCAAS_BUILD_GO_IMAGE)
    runtime_base_image_q = shlex.quote(RUNBOOK_CHAINCODE_CCAAS_RUNTIME_BASE_IMAGE)
    service_port_q = shlex.quote(str(RUNBOOK_CHAINCODE_CCAAS_SERVICE_PORT))
    dial_timeout_q = shlex.quote(RUNBOOK_CHAINCODE_CCAAS_DIAL_TIMEOUT)
    ready_wait_seconds_q = shlex.quote(str(RUNBOOK_CHAINCODE_CCAAS_READY_WAIT_SECONDS))
    ready_stable_polls_q = shlex.quote(str(RUNBOOK_CHAINCODE_CCAAS_READY_STABLE_POLLS))
    run_id_q = shlex.quote(str(run_state.get("run_id", "") or "").strip())
    change_id_q = shlex.quote(str(run_state.get("change_id", "") or "").strip())
    org_id_q = shlex.quote(str(host_row.get("org_id", "") or "").strip().lower())
    needle_q = shlex.quote("Name: {}".format(str(chaincode_id or "").strip().lower()))

    return "".join(
        [
            chaincode_package_lookup_step,
            "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && "
            "! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && "
            "! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then ".format(
                channel=channel_q,
                needle=needle_q,
            ),
            "COGNUS_CCAAS_WORKROOT={work_root}; ".format(work_root=work_root_q),
            "COGNUS_CCAAS_CONTAINER={container_name}; ".format(
                container_name=container_name_q
            ),
            "COGNUS_CCAAS_IMAGE={image_name}; ".format(image_name=image_name_q),
            "COGNUS_CCAAS_LABEL={external_label}; ".format(
                external_label=external_label_q
            ),
            "COGNUS_CCAAS_SERVICE_ADDRESS={service_address}; ".format(
                service_address=service_address_q
            ),
            "COGNUS_CCAAS_SERVICE_PORT={service_port}; ".format(
                service_port=service_port_q
            ),
            "COGNUS_CCAAS_EXTERNAL_PACKAGE_HOST={external_package_host_path}; ".format(
                external_package_host_path=external_package_host_path_q
            ),
            "COGNUS_CCAAS_EXTERNAL_PACKAGE_PEER={external_package_peer_path}; ".format(
                external_package_peer_path=external_package_peer_path_q
            ),
            "rm -rf \"$COGNUS_CCAAS_WORKROOT\" >/dev/null 2>&1 || true; ",
            "mkdir -p \"$COGNUS_CCAAS_WORKROOT/original\" \"$COGNUS_CCAAS_WORKROOT/extracted\" \"$COGNUS_CCAAS_WORKROOT/app\" \"$COGNUS_CCAAS_WORKROOT/external-src\" >/dev/null 2>&1 || true; ",
            "if [ -z \"$COGNUS_CC_PACKAGE_HOST\" ] || [ ! -s \"$COGNUS_CC_PACKAGE_HOST\" ]; then printf '%s%s\\n' 'COGNUS_CC_BOOTSTRAP_PKG_MISSING:' {chaincode}; ".format(
                chaincode=chaincode_q
            ),
            lifecycle_failure_clause,
            "fi; ",
            "cp \"$COGNUS_CC_PACKAGE_HOST\" \"$COGNUS_CCAAS_WORKROOT/original/package.tgz\" >/dev/null 2>&1 || true; ",
            "if [ ! -s \"$COGNUS_CCAAS_WORKROOT/original/package.tgz\" ]; then printf '%s\\n' COGNUS_CCAAS_SOURCE_PACKAGE_MISSING:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "tar -xzf \"$COGNUS_CCAAS_WORKROOT/original/package.tgz\" -C \"$COGNUS_CCAAS_WORKROOT/original\" >/dev/null 2>&1 || true; ",
            "if [ -s \"$COGNUS_CCAAS_WORKROOT/original/code.tar.gz\" ]; then tar -xzf \"$COGNUS_CCAAS_WORKROOT/original/code.tar.gz\" -C \"$COGNUS_CCAAS_WORKROOT/extracted\" >/dev/null 2>&1 || true; fi; ",
            "COGNUS_CCAAS_SOURCE_APP=$COGNUS_CCAAS_WORKROOT/extracted; ",
            "if [ -s \"$COGNUS_CCAAS_WORKROOT/extracted/src/go.mod\" ]; then COGNUS_CCAAS_SOURCE_APP=$COGNUS_CCAAS_WORKROOT/extracted/src; fi; ",
            "if [ ! -s \"$COGNUS_CCAAS_SOURCE_APP/go.mod\" ]; then printf '%s\\n' COGNUS_CCAAS_GO_MODULE_MISSING:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "cp -a \"$COGNUS_CCAAS_SOURCE_APP/.\" \"$COGNUS_CCAAS_WORKROOT/app/\" >/dev/null 2>&1 || true; ",
            "find \"$COGNUS_CCAAS_WORKROOT/app\" -type f \\( -name '*:Zone.Identifier' -o -name '.DS_Store' -o -name 'Thumbs.db' \\) -delete >/dev/null 2>&1 || true; ",
            "find \"$COGNUS_CCAAS_WORKROOT/app\" -depth -type d -name '__MACOSX' -exec rm -rf '{}' + >/dev/null 2>&1 || true; ",
            "COGNUS_CCAAS_MAIN_TARGET=$(find \"$COGNUS_CCAAS_WORKROOT/app\" -type f -name '*.go' 2>/dev/null | while read -r f; do grep -F 'shim.Start(' \"$f\" >/dev/null 2>&1 && { echo \"$f\"; break; }; done | head -n 1); ",
            "if [ -z \"$COGNUS_CCAAS_MAIN_TARGET\" ]; then printf '%s\\n' COGNUS_CCAAS_SHIM_START_MISSING:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "sed -i 's/shim\\.Start(/startCognusCCAAS(/g' \"$COGNUS_CCAAS_MAIN_TARGET\" >/dev/null 2>&1 || true; ",
            "printf %s {adapter_b64} | base64 -d > \"$COGNUS_CCAAS_WORKROOT/app/cognus_ccaas_adapter.go\"; ".format(
                adapter_b64=adapter_b64_q
            ),
            "printf %s {dockerfile_b64} | base64 -d > \"$COGNUS_CCAAS_WORKROOT/Dockerfile\"; ".format(
                dockerfile_b64=dockerfile_b64_q
            ),
            "printf '{{\"address\":\"%s\",\"dial_timeout\":\"%s\",\"tls_required\":false}}' \"$COGNUS_CCAAS_SERVICE_ADDRESS\" {dial_timeout} > \"$COGNUS_CCAAS_WORKROOT/external-src/connection.json\"; ".format(
                dial_timeout=dial_timeout_q
            ),
            "tar -czf \"$COGNUS_CCAAS_WORKROOT/code.tar.gz\" -C \"$COGNUS_CCAAS_WORKROOT/external-src\" connection.json >/dev/null 2>&1 || true; ",
            "printf '{\"path\":\"\",\"type\":\"ccaas\",\"label\":\"%s\"}' \"$COGNUS_CCAAS_LABEL\" > \"$COGNUS_CCAAS_WORKROOT/metadata.json\"; ",
            "tar -czf \"$COGNUS_CCAAS_EXTERNAL_PACKAGE_HOST\" -C \"$COGNUS_CCAAS_WORKROOT\" metadata.json code.tar.gz >/dev/null 2>&1 || true; ",
            "if [ ! -s \"$COGNUS_CCAAS_EXTERNAL_PACKAGE_HOST\" ]; then printf '%s\\n' COGNUS_CCAAS_EXTERNAL_PACKAGE_MISSING:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "docker cp \"$COGNUS_CCAAS_EXTERNAL_PACKAGE_HOST\" \"$COGNUS_PEER_CONTAINER:$COGNUS_CCAAS_EXTERNAL_PACKAGE_PEER\" >/dev/null 2>&1 || true; ",
            "COGNUS_CC_PACKAGE_ID=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode calculatepackageid '$COGNUS_CCAAS_EXTERNAL_PACKAGE_PEER' 2>/dev/null | tr -d '\\r\\n'\" 2>/dev/null || true); ",
            "COGNUS_CC_INSTALL_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode install \"$COGNUS_CCAAS_EXTERNAL_PACKAGE_PEER\" 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" peer lifecycle chaincode install \"$COGNUS_CCAAS_EXTERNAL_PACKAGE_PEER\" 2>&1 || true); ",
            "COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode queryinstalled 2>/dev/null || true); ",
            "if ! printf '%s\\n' \"$COGNUS_CC_QUERYINSTALLED\" | grep -qi 'Package ID:'; then COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode queryinstalled 2>/dev/null || true\" 2>/dev/null || true); fi; ",
            "if ! printf '%s\\n' \"$COGNUS_CC_QUERYINSTALLED\" | grep -qi 'Package ID:'; then COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'peer lifecycle chaincode queryinstalled 2>/dev/null || true' 2>/dev/null || true); fi; ",
            "if [ -z \"$COGNUS_CC_PACKAGE_ID\" ]; then ",
            chaincode_package_id_lookup_step,
            "fi; ",
            "if [ -z \"$COGNUS_CC_PACKAGE_ID\" ]; then printf '%s\\n' COGNUS_CCAAS_PACKAGE_ID_MISSING:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "if ! printf '%s\\n' \"$COGNUS_CC_QUERYINSTALLED\" | grep -F \"$COGNUS_CC_PACKAGE_ID\" >/dev/null 2>&1; then COGNUS_CC_INSTALL_LAST=$(printf '%s' \"$COGNUS_CC_INSTALL_OUT\" | tr '\\n' '|' | cut -c1-380 || true); [ -n \"$COGNUS_CC_INSTALL_LAST\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_INSTALL_DIAG:' \"$COGNUS_CC_INSTALL_LAST\" >&2 || true; printf '%s\\n' COGNUS_CCAAS_PACKAGE_NOT_INSTALLED:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "docker image inspect {go_image} >/dev/null 2>&1 || docker pull {go_image} >/dev/null 2>&1 || true; ".format(
                go_image=go_image_q
            ),
            "docker image inspect {runtime_base_image} >/dev/null 2>&1 || docker pull {runtime_base_image} >/dev/null 2>&1 || true; ".format(
                runtime_base_image=runtime_base_image_q
            ),
            "COGNUS_CCAAS_BUILD_LOG=$COGNUS_CCAAS_WORKROOT/docker-build.log; ",
            "docker build --build-arg COGNUS_GO_IMAGE={go_image} --build-arg COGNUS_RUNTIME_BASE_IMAGE={runtime_base_image} -t \"$COGNUS_CCAAS_IMAGE\" \"$COGNUS_CCAAS_WORKROOT\" >\"$COGNUS_CCAAS_BUILD_LOG\" 2>&1 || true; ".format(
                go_image=go_image_q,
                runtime_base_image=runtime_base_image_q,
            ),
            "if ! docker image inspect \"$COGNUS_CCAAS_IMAGE\" >/dev/null 2>&1; then COGNUS_CCAAS_BUILD_LAST=$(tail -n 5 \"$COGNUS_CCAAS_BUILD_LOG\" 2>/dev/null | tr '\\n' '|' | cut -c1-320 || true); [ -n \"$COGNUS_CCAAS_BUILD_LAST\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_BUILD_LOG:' \"$COGNUS_CCAAS_BUILD_LAST\" >&2 || true; printf '%s\\n' COGNUS_CCAAS_BUILD_FAILED:{channel}:{chaincode} >&2; ".format(
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            lifecycle_failure_clause,
            "fi; ",
            "docker rm -f \"$COGNUS_CCAAS_CONTAINER\" >/dev/null 2>&1 || true; ",
            "docker run -d --name \"$COGNUS_CCAAS_CONTAINER\" --network {network} --restart unless-stopped --label cognus.run_id={run_id} --label cognus.change_id={change_id} --label cognus.node_type=chaincode --label cognus.runtime_mode=ccaas --label cognus.org_id={org_id} --label cognus.channel_id={channel_id} --label cognus.chaincode_id={chaincode_id} -e CHAINCODE_SERVER_ADDRESS=0.0.0.0:{service_port} -e CHAINCODE_ID=\"$COGNUS_CC_PACKAGE_ID\" -e CORE_CHAINCODE_ID_NAME=\"$COGNUS_CC_PACKAGE_ID\" \"$COGNUS_CCAAS_IMAGE\" >/dev/null 2>&1 || true; ".format(
                network=shlex.quote(RUNBOOK_BASE_NETWORK_NAME),
                run_id=run_id_q,
                change_id=change_id_q,
                org_id=org_id_q,
                channel_id=channel_q,
                chaincode_id=chaincode_q,
                service_port=service_port_q,
            ),
            "COGNUS_CCAAS_RUNTIME_STATUS=''; ",
            "COGNUS_CCAAS_RUNTIME_EXIT=''; ",
            "COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=0; ",
            "for COGNUS_CCAAS_READY_ATTEMPT in $(seq 1 {ready_wait_seconds}); do ".format(
                ready_wait_seconds=ready_wait_seconds_q
            ),
            "COGNUS_CCAAS_RUNTIME_STATUS=$(docker inspect -f '{{{{.State.Status}}}}' \"$COGNUS_CCAAS_CONTAINER\" 2>/dev/null || true); ",
            "COGNUS_CCAAS_RUNTIME_EXIT=$(docker inspect -f '{{{{.State.ExitCode}}}}' \"$COGNUS_CCAAS_CONTAINER\" 2>/dev/null || true); ",
            "if [ \"$COGNUS_CCAAS_RUNTIME_STATUS\" = running ]; then COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=$((COGNUS_CCAAS_RUNTIME_RUNNING_STREAK + 1)); else COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=0; fi; ",
            "if [ \"$COGNUS_CCAAS_RUNTIME_RUNNING_STREAK\" -ge {ready_stable_polls} ]; then break; fi; ".format(
                ready_stable_polls=ready_stable_polls_q
            ),
            "sleep 1; ",
            "done; ",
            "if [ \"$COGNUS_CCAAS_RUNTIME_RUNNING_STREAK\" -lt {ready_stable_polls} ]; then COGNUS_CCAAS_RUNTIME_LOG=$(docker logs --tail 20 \"$COGNUS_CCAAS_CONTAINER\" 2>&1 | tr '\\n' '|' | cut -c1-320 || true); [ -n \"$COGNUS_CCAAS_RUNTIME_LOG\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_LOG:' \"$COGNUS_CCAAS_RUNTIME_LOG\" >&2 || true; [ -n \"$COGNUS_CCAAS_RUNTIME_STATUS\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_STATUS:' \"$COGNUS_CCAAS_RUNTIME_STATUS\" >&2 || true; [ -n \"$COGNUS_CCAAS_RUNTIME_EXIT\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_EXIT:' \"$COGNUS_CCAAS_RUNTIME_EXIT\" >&2 || true; printf '%s\\n' COGNUS_CCAAS_RUNTIME_PRECHECK_PENDING:{channel}:{chaincode} >&2; fi; ".format(
                ready_stable_polls=ready_stable_polls_q,
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            "COGNUS_CC_ORDERER_FLAGS=''; ",
            "COGNUS_CC_ORDERER_HOST_OVERRIDES=''; ",
            "if [ \"${{COGNUS_ORDERER_TLS_ENABLED:-0}}\" = \"1\" ] && docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CA'\" >/dev/null 2>&1; then COGNUS_CC_ORDERER_FLAGS=\"--tls --cafile $COGNUS_PEER_CA\"; COGNUS_CC_ORDERER_HOST_OVERRIDES=\"{host_override} $COGNUS_ORDERER_CONTAINER localhost\"; fi; ".format(
                host_override=orderer_host_override_q
            ),
            "for COGNUS_CC_ORDERER_EP in \"$COGNUS_ORDERER_ENDPOINT\" \"$COGNUS_ORDERER_ENDPOINT_ALT\"; do ",
            "[ -n \"$COGNUS_CC_ORDERER_EP\" ] || continue; ",
            "if [ -n \"$COGNUS_CC_ORDERER_HOST_OVERRIDES\" ]; then ",
            "for COGNUS_CC_HOST_OVERRIDE in $COGNUS_CC_ORDERER_HOST_OVERRIDES; do ",
            "for COGNUS_CC_ATTEMPT in 1 2 3; do ",
            "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" $COGNUS_CC_ORDERER_FLAGS --ordererTLSHostnameOverride \"$COGNUS_CC_HOST_OVERRIDE\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); ".format(
                channel=channel_q,
                chaincode=chaincode_q,
                chaincode_version=chaincode_version_q,
                chaincode_sequence=chaincode_sequence_q,
            ),
            "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" $COGNUS_CC_ORDERER_FLAGS --ordererTLSHostnameOverride \"$COGNUS_CC_HOST_OVERRIDE\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); ".format(
                channel=channel_q,
                chaincode=chaincode_q,
                chaincode_version=chaincode_version_q,
                chaincode_sequence=chaincode_sequence_q,
            ),
            "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 3; fi; ".format(
                channel=channel_q,
                needle=needle_q,
            ),
            "sleep 2; ",
            "done; ",
            "done; ",
            "else ",
            "for COGNUS_CC_ATTEMPT in 1 2 3; do ",
            "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); ".format(
                channel=channel_q,
                chaincode=chaincode_q,
                chaincode_version=chaincode_version_q,
                chaincode_sequence=chaincode_sequence_q,
            ),
            "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); ".format(
                channel=channel_q,
                chaincode=chaincode_q,
                chaincode_version=chaincode_version_q,
                chaincode_sequence=chaincode_sequence_q,
            ),
            "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 2; fi; ".format(
                channel=channel_q,
                needle=needle_q,
            ),
            "sleep 2; ",
            "done; ",
            "fi; ",
            "done; ",
            "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then COGNUS_CCAAS_RUNTIME_LOG=$(docker logs --tail 20 \"$COGNUS_CCAAS_CONTAINER\" 2>&1 | tr '\\n' '|' | cut -c1-320 || true); [ -n \"$COGNUS_CCAAS_RUNTIME_LOG\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_LOG:' \"$COGNUS_CCAAS_RUNTIME_LOG\" >&2 || true; ".format(
                channel=channel_q,
                needle=needle_q,
            ),
            lifecycle_failure_clause,
            "fi; ",
            "COGNUS_CCAAS_RUNTIME_STATUS=''; ",
            "COGNUS_CCAAS_RUNTIME_EXIT=''; ",
            "COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=0; ",
            "for COGNUS_CCAAS_READY_ATTEMPT in $(seq 1 {ready_wait_seconds}); do ".format(
                ready_wait_seconds=ready_wait_seconds_q
            ),
            "COGNUS_CCAAS_RUNTIME_STATUS=$(docker inspect -f '{{{{.State.Status}}}}' \"$COGNUS_CCAAS_CONTAINER\" 2>/dev/null || true); ",
            "COGNUS_CCAAS_RUNTIME_EXIT=$(docker inspect -f '{{{{.State.ExitCode}}}}' \"$COGNUS_CCAAS_CONTAINER\" 2>/dev/null || true); ",
            "if [ \"$COGNUS_CCAAS_RUNTIME_STATUS\" = running ]; then COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=$((COGNUS_CCAAS_RUNTIME_RUNNING_STREAK + 1)); else COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=0; fi; ",
            "if [ \"$COGNUS_CCAAS_RUNTIME_RUNNING_STREAK\" -ge {ready_stable_polls} ]; then break; fi; ".format(
                ready_stable_polls=ready_stable_polls_q
            ),
            "sleep 1; ",
            "done; ",
            "if [ \"$COGNUS_CCAAS_RUNTIME_RUNNING_STREAK\" -lt {ready_stable_polls} ]; then COGNUS_CCAAS_RUNTIME_LOG=$(docker logs --tail 20 \"$COGNUS_CCAAS_CONTAINER\" 2>&1 | tr '\\n' '|' | cut -c1-320 || true); [ -n \"$COGNUS_CCAAS_RUNTIME_LOG\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_LOG:' \"$COGNUS_CCAAS_RUNTIME_LOG\" >&2 || true; [ -n \"$COGNUS_CCAAS_RUNTIME_STATUS\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_STATUS:' \"$COGNUS_CCAAS_RUNTIME_STATUS\" >&2 || true; [ -n \"$COGNUS_CCAAS_RUNTIME_EXIT\" ] && printf '%s%s\\n' 'COGNUS_CCAAS_RUNTIME_EXIT:' \"$COGNUS_CCAAS_RUNTIME_EXIT\" >&2 || true; printf '%s\\n' COGNUS_CCAAS_RUNTIME_POSTCHECK_PENDING:{channel}:{chaincode} >&2; fi; ".format(
                ready_stable_polls=ready_stable_polls_q,
                channel=str(channel_id or "").strip().lower(),
                chaincode=str(chaincode_id or "").strip().lower(),
            ),
            "fi; ",
        ]
    )


def _resolve_local_chaincode_artifact_path(chaincode_artifact_metadata):
    if not isinstance(chaincode_artifact_metadata, dict):
        return ""

    for candidate_path in _collect_explicit_chaincode_binding_paths(
        chaincode_artifact_metadata,
        "artifact_refs",
    ):
        candidate = Path(candidate_path)
        try:
            if (
                candidate.stat().st_size > 0
                and _chaincode_archive_matches_expected_identity(
                    candidate,
                    chaincode_artifact_metadata,
                )
            ):
                return str(candidate)
        except Exception:
            continue
    return ""


def _resolve_local_chaincode_source_dir(chaincode_artifact_metadata):
    if not isinstance(chaincode_artifact_metadata, dict):
        return ""

    def _resolve_packaging_root(source_dir):
        source_path = Path(str(source_dir or "").strip())
        candidate_roots = [source_path, source_path / "chaincode"]
        scored_roots = []
        for candidate_root in candidate_roots:
            if not candidate_root.is_dir():
                continue
            score = 0
            if (candidate_root / "go.mod").is_file():
                score += 100
            if (candidate_root / "package.json").is_file():
                score += 100
            if (candidate_root / "main.go").is_file():
                score += 40
            if (candidate_root / "txList.go").is_file():
                score += 20
            if score <= 0:
                continue
            scored_roots.append((score, len(str(candidate_root)), str(candidate_root)))
        if not scored_roots:
            return ""
        scored_roots.sort(key=lambda row: (-row[0], row[1], row[2]))
        return scored_roots[0][2]

    for source_dir in _collect_explicit_chaincode_binding_paths(
        chaincode_artifact_metadata,
        "source_refs",
    ):
        packaging_root = _resolve_packaging_root(source_dir)
        if packaging_root:
            return packaging_root
    return ""


def _infer_chaincode_language_from_source_dir(source_dir):
    source_path = Path(str(source_dir or "").strip())
    if not source_path.is_dir():
        return "golang"
    if (source_path / "go.mod").is_file():
        return "golang"
    try:
        if any(source_path.rglob("*.go")):
            return "golang"
    except Exception:
        pass
    if (source_path / "package.json").is_file():
        return "node"
    return "golang"


def _build_chaincode_source_sanitize_step(source_root):
    safe_source_root = shlex.quote(str(source_root or "").strip() or "/workspace/src")
    return (
        "find {root} -type f \\( -name '*:Zone.Identifier' -o -name '.DS_Store' -o -name 'Thumbs.db' \\) -delete >/dev/null 2>&1 || true; "
        "find {root} -depth -type d -name '__MACOSX' -exec rm -rf '{{}}' + >/dev/null 2>&1 || true"
    ).format(root=safe_source_root)


def _iter_host_chaincode_source_candidates(chaincode_artifact_metadata):
    for source_dir in _collect_explicit_chaincode_binding_paths(
        chaincode_artifact_metadata,
        "source_refs",
    ):
        yield source_dir


def _copy_host_chaincode_archive_from_explicit_bindings(
    chaincode_artifact_metadata,
    artifact_path,
):
    explicit_artifact_paths = _collect_explicit_chaincode_binding_paths(
        chaincode_artifact_metadata,
        "artifact_refs",
    )

    if not explicit_artifact_paths:
        return ""

    helper_image = "alpine:3.20"
    try:
        inspect_result = subprocess.run(
            ["docker", "image", "inspect", helper_image],
            capture_output=True,
            text=True,
            timeout=max(RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS, 30),
        )
        if inspect_result.returncode != 0:
            subprocess.run(
                ["docker", "pull", helper_image],
                capture_output=True,
                text=True,
                timeout=max(RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS, 60),
            )
    except Exception:
        return ""

    for explicit_artifact_path in explicit_artifact_paths:
        explicit_artifact_file = Path(explicit_artifact_path)
        try:
            if (
                explicit_artifact_file.is_file()
                and explicit_artifact_file.stat().st_size > 0
                and _chaincode_archive_matches_expected_identity(
                    explicit_artifact_file,
                    chaincode_artifact_metadata,
                )
            ):
                return str(explicit_artifact_file)
        except Exception:
            pass

        host_parent_dir = str(explicit_artifact_file.parent or "").strip()
        archive_name = explicit_artifact_file.name
        if not host_parent_dir or not archive_name:
            continue
        helper_name = "cognus-cchost-{}".format(
            hashlib.sha256(
                "{}|{}".format(explicit_artifact_path, artifact_path).encode("utf-8")
            ).hexdigest()[:12]
        )
        helper_created = False
        try:
            subprocess.run(
                ["docker", "rm", "-f", helper_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            create_result = subprocess.run(
                [
                    "docker",
                    "create",
                    "--name",
                    helper_name,
                    "-v",
                    "{}:/workspace/artifacts:ro".format(host_parent_dir),
                    helper_image,
                    "sh",
                    "-lc",
                    "sleep 600",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if create_result.returncode != 0:
                continue
            helper_created = True
            subprocess.run(
                ["docker", "start", helper_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            copy_result = subprocess.run(
                [
                    "docker",
                    "cp",
                    "{}:/workspace/artifacts/{}".format(helper_name, archive_name),
                    str(artifact_path),
                ],
                capture_output=True,
                text=True,
                timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 60),
            )
            if copy_result.returncode != 0:
                continue
            try:
                if (
                    artifact_path.is_file()
                    and artifact_path.stat().st_size > 0
                    and _chaincode_archive_matches_expected_identity(
                        artifact_path,
                        chaincode_artifact_metadata,
                    )
                ):
                    return str(artifact_path)
                artifact_path.unlink(missing_ok=True)
            except Exception:
                continue
        except Exception:
            continue
        finally:
            if helper_created:
                try:
                    subprocess.run(
                        ["docker", "rm", "-f", helper_name],
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                except Exception:
                    pass

    return ""


def _run_chaincode_packaging_helper_command(command, chaincode_id):
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 60),
    )
    if result.returncode != 0:
        LOG.warning(
            "Failed to package chaincode %s: %s",
            chaincode_id,
            str(result.stderr or "").strip()[:256],
        )
    return result


def _build_local_chaincode_artifact_from_host_source(
    chaincode_artifact_metadata,
    artifact_path,
    helper_image,
    helper_label,
    package_file_name,
):
    chaincode_id = _normalize_chaincode_metadata_token(
        chaincode_artifact_metadata.get("chaincode_id", "")
    )
    if not chaincode_id:
        return ""

    copied_archive_path = _copy_host_chaincode_archive_from_explicit_bindings(
        chaincode_artifact_metadata,
        artifact_path,
    )
    if copied_archive_path:
        return copied_archive_path

    helper_output = "/workspace/out/{}".format(os.path.basename(package_file_name))
    for host_source_dir in _iter_host_chaincode_source_candidates(
        chaincode_artifact_metadata
    ):
        helper_lang = _infer_chaincode_language_from_source_dir(host_source_dir)
        helper_material = "|".join(
            [
                chaincode_id.lower(),
                str(host_source_dir),
                str(package_file_name),
            ]
        )
        helper_name = "cognus-ccpkg-{}".format(
            hashlib.sha256(helper_material.encode("utf-8")).hexdigest()[:12]
        )
        helper_created = False
        try:
            subprocess.run(
                ["docker", "rm", "-f", helper_name],
                capture_output=True,
                text=True,
                timeout=30,
            )
            create_result = subprocess.run(
                [
                    "docker",
                    "create",
                    "--name",
                    helper_name,
                    "-v",
                    "{}:/workspace/src:ro".format(host_source_dir),
                    helper_image,
                    "sh",
                    "-lc",
                    "sleep 600",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if create_result.returncode != 0:
                continue
            helper_created = True

            for command in [
                ["docker", "start", helper_name],
                [
                    "docker",
                    "exec",
                    helper_name,
                    "sh",
                    "-lc",
                    "mkdir -p /workspace/out /workspace/buildsrc && "
                    "cp -R /workspace/src/. /workspace/buildsrc/ >/dev/null 2>&1 && "
                    "{sanitize} && "
                    "peer lifecycle chaincode package {output} --path /workspace/buildsrc --lang {lang} --label {label}".format(
                        sanitize=_build_chaincode_source_sanitize_step("/workspace/buildsrc"),
                        output=shlex.quote(helper_output),
                        lang=shlex.quote(helper_lang),
                        label=shlex.quote(helper_label),
                    ),
                ],
                ["docker", "cp", "{}:{}".format(helper_name, helper_output), str(artifact_path)],
            ]:
                command_result = _run_chaincode_packaging_helper_command(
                    command,
                    chaincode_id,
                )
                if command_result.returncode != 0:
                    break
            else:
                if (
                    artifact_path.is_file()
                    and artifact_path.stat().st_size > 0
                    and _chaincode_archive_matches_expected_identity(
                        artifact_path,
                        chaincode_artifact_metadata,
                    )
                ):
                    return str(artifact_path)
                try:
                    artifact_path.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception as exc:
            LOG.warning(
                "Failed to package chaincode %s from host source %s: %s",
                chaincode_id,
                host_source_dir,
                exc,
            )
        finally:
            if helper_created:
                try:
                    subprocess.run(
                        ["docker", "rm", "-f", helper_name],
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                except Exception:
                    pass

    return ""


def _build_local_chaincode_artifact_from_source(chaincode_artifact_metadata):
    chaincode_id = _normalize_chaincode_metadata_token(
        chaincode_artifact_metadata.get("chaincode_id", "")
    )
    if not chaincode_id:
        return ""

    version = _normalize_chaincode_metadata_token(
        chaincode_artifact_metadata.get("version", "")
    ) or str(RUNBOOK_CHAINCODE_AUTODEFINE_VERSION or "").strip() or "1.0"
    candidate_files = [
        str(os.path.basename(candidate_file or "")).strip()
        for candidate_file in chaincode_artifact_metadata.get("candidate_files", [])
        if str(candidate_file or "").strip()
    ]
    package_file_name = candidate_files[0] if candidate_files else ""
    if not package_file_name:
        package_file_name = "{}_{}.tar.gz".format(chaincode_id, version)
    if not package_file_name.lower().endswith((".tar.gz", ".tgz", ".tar")):
        package_file_name = "{}.tar.gz".format(package_file_name)

    artifact_dir = Path(gettempdir()) / "cognus_chaincode_autobuild" / _sanitize_container_token(
        chaincode_id, "chaincode"
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / os.path.basename(package_file_name)
    try:
        if (
            artifact_path.is_file()
            and artifact_path.stat().st_size > 0
            and _chaincode_archive_matches_expected_identity(
                artifact_path,
                chaincode_artifact_metadata,
            )
        ):
            return str(artifact_path)
        artifact_path.unlink(missing_ok=True)
    except Exception:
        pass

    helper_image = "hyperledger/fabric-tools:2.5"
    helper_label = "{}_{}".format(chaincode_id, version)
    helper_output = "/workspace/out/{}".format(os.path.basename(package_file_name))
    source_dir = _resolve_local_chaincode_source_dir(chaincode_artifact_metadata)
    if not source_dir:
        return _build_local_chaincode_artifact_from_host_source(
            chaincode_artifact_metadata,
            artifact_path,
            helper_image,
            helper_label,
            package_file_name,
        )
    helper_material = "|".join(
        [
            chaincode_id.lower(),
            str(version),
            str(source_dir),
            str(package_file_name),
        ]
    )
    helper_name = "cognus-ccpkg-{}".format(
        hashlib.sha256(helper_material.encode("utf-8")).hexdigest()[:12]
    )
    helper_lang = _infer_chaincode_language_from_source_dir(source_dir)

    helper_created = False
    try:
        inspect_result = subprocess.run(
            ["docker", "image", "inspect", helper_image],
            capture_output=True,
            text=True,
            timeout=max(RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS, 30),
        )
        if inspect_result.returncode != 0:
            pull_result = subprocess.run(
                ["docker", "pull", helper_image],
                capture_output=True,
                text=True,
                timeout=max(RUNBOOK_RUNTIME_PULL_TIMEOUT_SECONDS, 60),
            )
            if pull_result.returncode != 0:
                LOG.warning(
                    "Failed to pull helper image for chaincode packaging %s: %s",
                    chaincode_id,
                    str(pull_result.stderr or "").strip()[:256],
                )
                return _build_local_chaincode_artifact_from_host_source(
                    chaincode_artifact_metadata,
                    artifact_path,
                    helper_image,
                    helper_label,
                    package_file_name,
                )

        subprocess.run(
            ["docker", "rm", "-f", helper_name],
            capture_output=True,
            text=True,
            timeout=30,
        )
        create_result = subprocess.run(
            ["docker", "create", "--name", helper_name, helper_image, "sh", "-lc", "sleep 600"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if create_result.returncode != 0:
            LOG.warning(
                "Failed to create chaincode packaging helper for %s: %s",
                chaincode_id,
                str(create_result.stderr or "").strip()[:256],
            )
            return _build_local_chaincode_artifact_from_host_source(
                chaincode_artifact_metadata,
                artifact_path,
                helper_image,
                helper_label,
                package_file_name,
            )
        helper_created = True

        for command in [
            ["docker", "start", helper_name],
            ["docker", "exec", helper_name, "sh", "-lc", "mkdir -p /workspace/src /workspace/out"],
            ["docker", "cp", "{}/.".format(str(source_dir).rstrip("/")), "{}:/workspace/src/".format(helper_name)],
            [
                "docker",
                "exec",
                helper_name,
                "sh",
                "-lc",
                "{sanitize} && peer lifecycle chaincode package {output} --path /workspace/src --lang {lang} --label {label}".format(
                    sanitize=_build_chaincode_source_sanitize_step("/workspace/src"),
                    output=shlex.quote(helper_output),
                    lang=shlex.quote(helper_lang),
                    label=shlex.quote(helper_label),
                ),
            ],
            ["docker", "cp", "{}:{}".format(helper_name, helper_output), str(artifact_path)],
        ]:
            command_result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 60),
            )
            if command_result.returncode != 0:
                LOG.warning(
                    "Failed to package chaincode %s from %s: %s",
                    chaincode_id,
                    source_dir,
                    str(command_result.stderr or "").strip()[:256],
                )
                return _build_local_chaincode_artifact_from_host_source(
                    chaincode_artifact_metadata,
                    artifact_path,
                    helper_image,
                    helper_label,
                    package_file_name,
                )

        if (
            artifact_path.is_file()
            and artifact_path.stat().st_size > 0
            and _chaincode_archive_matches_expected_identity(
                artifact_path,
                chaincode_artifact_metadata,
            )
        ):
            return str(artifact_path)
        try:
            artifact_path.unlink(missing_ok=True)
        except Exception:
            pass
    except Exception as exc:
        LOG.warning("Failed to autobuild chaincode package for %s: %s", chaincode_id, exc)
    finally:
        if helper_created:
            try:
                subprocess.run(
                    ["docker", "rm", "-f", helper_name],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
            except Exception:
                pass

    return _build_local_chaincode_artifact_from_host_source(
        chaincode_artifact_metadata,
        artifact_path,
        helper_image,
        helper_label,
        package_file_name,
    )


def _resolve_or_build_local_chaincode_artifact(chaincode_artifact_metadata):
    if not _chaincode_artifact_metadata_declares_binding(chaincode_artifact_metadata):
        return {
            "available": False,
            "mode": "binding_missing",
            "local_path": "",
        }

    if not _chaincode_artifact_metadata_has_explicit_binding(chaincode_artifact_metadata):
        return {
            "available": False,
            "mode": "binding_invalid",
            "local_path": "",
        }

    local_artifact_path = _resolve_local_chaincode_artifact_path(
        chaincode_artifact_metadata
    )
    if local_artifact_path:
        return {
            "available": True,
            "mode": "existing",
            "local_path": local_artifact_path,
        }

    built_artifact_path = _build_local_chaincode_artifact_from_source(
        chaincode_artifact_metadata
    )
    if built_artifact_path:
        return {
            "available": True,
            "mode": "autobuild",
            "local_path": built_artifact_path,
        }

    return {
        "available": False,
        "mode": "binding_unavailable",
        "local_path": "",
    }


def _upload_local_file_to_remote(
    host_address,
    ssh_user,
    ssh_port,
    local_path,
    remote_path,
    identity_file="",
    timeout=None,
):
    normalized_local_path = str(local_path or "").strip()
    normalized_remote_path = str(remote_path or "").strip()
    if not normalized_local_path or not normalized_remote_path:
        return {
            "returncode": 1,
            "stdout": "",
            "stderr": "missing_local_or_remote_path",
            "timed_out": False,
        }

    local_file = Path(normalized_local_path)
    if not local_file.is_file():
        return {
            "returncode": 1,
            "stdout": "",
            "stderr": "local_file_missing",
            "timed_out": False,
        }

    remote_dir = os.path.dirname(normalized_remote_path) or "/tmp"
    ssh_args = _build_ssh_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command=(
            "umask 077; mkdir -p {remote_dir} >/dev/null 2>&1 || true; "
            "cat > {remote_path}; chmod 600 {remote_path} >/dev/null 2>&1 || true"
        ).format(
            remote_dir=shlex.quote(remote_dir),
            remote_path=shlex.quote(normalized_remote_path),
        ),
        identity_file=identity_file,
    )

    try:
        payload = local_file.read_bytes()
        result = subprocess.run(
            ssh_args,
            input=payload,
            capture_output=True,
            timeout=timeout or max(RUNBOOK_SSH_TIMEOUT_SECONDS, 30),
        )
        return {
            "returncode": result.returncode,
            "stdout": bytes(result.stdout or b"").decode("utf-8", errors="replace").strip(),
            "stderr": bytes(result.stderr or b"").decode("utf-8", errors="replace").strip(),
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "returncode": 124,
            "stdout": bytes(exc.stdout or b"").decode("utf-8", errors="replace").strip(),
            "stderr": bytes(exc.stderr or b"").decode("utf-8", errors="replace").strip(),
            "timed_out": True,
        }
    except FileNotFoundError as exc:
        return {
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
            "timed_out": False,
        }
    except OSError as exc:
        return {
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
            "timed_out": False,
        }


def _seed_remote_chaincode_packages_for_host(
    run_state,
    host_row,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
    timeout=None,
):
    scope = _collect_gateway_channel_chaincode_pairs(run_state, host_row)
    pairs = [
        pair
        for pair in scope.get("pairs", [])
        if isinstance(pair, dict)
        and str(pair.get("channel_id", "") or "").strip()
        and str(pair.get("chaincode_id", "") or "").strip()
    ]
    if not pairs:
        return {
            "available": False,
            "rows": [],
            "missing_pairs": [],
            "details": {"reason": "no_channel_chaincode_pairs"},
        }

    cache_store = run_state.setdefault("remote_chaincode_package_seed_cache", {})
    if not isinstance(cache_store, dict):
        cache_store = {}
        run_state["remote_chaincode_package_seed_cache"] = cache_store

    rows = []
    missing_pairs = []
    for pair in pairs:
        channel_id = str(pair.get("channel_id", "") or "").strip().lower()
        chaincode_id = str(pair.get("chaincode_id", "") or "").strip().lower()
        chaincode_artifact_metadata = _resolve_chaincode_artifact_metadata(
            run_state,
            channel_id,
            chaincode_id,
        )
        local_artifact = _resolve_or_build_local_chaincode_artifact(
            chaincode_artifact_metadata
        )
        if not bool(local_artifact.get("available", False)):
            missing_pairs.append(
                {
                    "channel_id": channel_id,
                    "chaincode_id": chaincode_id,
                    "mode": str(local_artifact.get("mode", "missing") or "missing"),
                }
            )
            continue

        local_path = str(local_artifact.get("local_path", "") or "").strip()
        local_file = Path(local_path)
        try:
            file_digest = hashlib.sha256(local_file.read_bytes()).hexdigest()[:12]
        except Exception:
            file_digest = hashlib.sha256(local_path.encode("utf-8")).hexdigest()[:12]
        remote_name = "{}_{}-{}".format(
            _sanitize_container_token(chaincode_id, "chaincode"),
            file_digest,
            _sanitize_container_token(local_file.name, "package"),
        )
        remote_name = "{}.tar.gz".format(remote_name[:180].rstrip("-")) if not remote_name.endswith((".tar.gz", ".tgz", ".tar")) else remote_name
        remote_path = "/tmp/cognus/chaincode-stage/{}".format(remote_name)
        cache_key = "{}|{}|{}|{}".format(
            str(host_address or "").strip().lower(),
            channel_id,
            chaincode_id,
            file_digest,
        )

        cached_row = cache_store.get(cache_key, {})
        if (
            isinstance(cached_row, dict)
            and str(cached_row.get("remote_path", "") or "").strip() == remote_path
            and str(cached_row.get("local_path", "") or "").strip() == local_path
        ):
            rows.append(
                {
                    "channel_id": channel_id,
                    "chaincode_id": chaincode_id,
                    "mode": str(local_artifact.get("mode", "existing") or "existing"),
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "upload_returncode": 0,
                    "cached": True,
                }
            )
            continue

        upload_result = _upload_local_file_to_remote(
            host_address=host_address,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            local_path=local_path,
            remote_path=remote_path,
            identity_file=identity_file,
            timeout=timeout or max(RUNBOOK_SSH_TIMEOUT_SECONDS, 30),
        )
        row = {
            "channel_id": channel_id,
            "chaincode_id": chaincode_id,
            "mode": str(local_artifact.get("mode", "existing") or "existing"),
            "local_path": local_path,
            "remote_path": remote_path,
            "upload_returncode": _coerce_exit_code(
                upload_result.get("returncode", 1),
                default=1,
            ),
            "upload_stdout": str(upload_result.get("stdout", "") or "")[:256],
            "upload_stderr": str(upload_result.get("stderr", "") or "")[:256],
            "cached": False,
        }
        if row["upload_returncode"] == 0:
            cache_store[cache_key] = {
                "remote_path": remote_path,
                "local_path": local_path,
                "updated_at_utc": _utc_now(),
            }
        rows.append(row)

    return {
        "available": bool(rows) or bool(missing_pairs),
        "rows": rows,
        "missing_pairs": missing_pairs,
    }


def _build_remote_chaincode_package_seed_failure_payload(
    run_state,
    stage_key,
    checkpoint_key,
    host_ref,
    host_address,
    org_id,
    node_id,
    node_type,
    remote_chaincode_package_seed,
):
    seed_result = (
        remote_chaincode_package_seed
        if isinstance(remote_chaincode_package_seed, dict)
        else {}
    )
    missing_pairs = seed_result.get("missing_pairs", [])
    failed_rows = [
        row
        for row in seed_result.get("rows", [])
        if isinstance(row, dict)
        and _coerce_exit_code(row.get("upload_returncode", 1), default=1) != 0
    ]
    common_details = {
        "run_id": run_state.get("run_id", "") if isinstance(run_state, dict) else "",
        "stage": stage_key,
        "checkpoint": checkpoint_key,
        "host_ref": host_ref,
        "host_address": host_address,
        "org_id": org_id,
        "node_id": node_id,
        "node_type": node_type,
        "remote_chaincode_package_seed": seed_result,
    }
    if missing_pairs:
        return _build_runbook_error_payload(
            "runbook_chaincode_artifact_unavailable",
            "Binding explicito de chaincode ausente, invalido ou indisponivel para seed remoto.",
            {
                **common_details,
                "missing_pairs": missing_pairs,
            },
        )
    if failed_rows:
        return _build_runbook_error_payload(
            "runbook_chaincode_package_seed_failed",
            "Upload do pacote de chaincode para o host remoto falhou antes do checkpoint operacional.",
            {
                **common_details,
                "failed_rows": failed_rows,
            },
        )
    return None


def _build_shell_word_list(values):
    words = [
        shlex.quote(str(value or "").strip())
        for value in values
        if str(value or "").strip()
    ]
    if not words:
        return "''"
    return " ".join(words)


def _build_chaincode_package_lookup_step(result_var, search_terms, candidate_files):
    normalized_result_var = str(result_var or "").strip()
    if not normalized_result_var:
        return ""
    candidate_files_words = _build_shell_word_list(candidate_files)
    search_terms_words = _build_shell_word_list(search_terms)
    return (
        "{result_var}=''; "
        "for COGNUS_CC_ROOT in /tmp/cognus /tmp /var/cognus \"${{HOME}}/.cognus\" \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain\"; do "
        "[ -d \"$COGNUS_CC_ROOT\" ] || continue; "
        "for COGNUS_CC_FILE in {candidate_files_words}; do "
        "[ -n \"$COGNUS_CC_FILE\" ] || continue; "
        "COGNUS_CC_CANDIDATE=$(find \"$COGNUS_CC_ROOT\" -maxdepth 8 -type f -name \"$COGNUS_CC_FILE\" 2>/dev/null | head -n 1); "
        "if [ -n \"$COGNUS_CC_CANDIDATE\" ] && [ -s \"$COGNUS_CC_CANDIDATE\" ]; then "
        "COGNUS_CC_STAGE_ROOT=/tmp/cognus/chaincode-stage; "
        "mkdir -p \"$COGNUS_CC_STAGE_ROOT\" >/dev/null 2>&1 || true; "
        "COGNUS_CC_STAGE_SAFE=$(basename \"$(dirname \"$COGNUS_CC_CANDIDATE\")\" | tr '/:' '__'); "
        "COGNUS_CC_STAGE=\"$COGNUS_CC_STAGE_ROOT/$COGNUS_CC_STAGE_SAFE-$(basename \"$COGNUS_CC_CANDIDATE\" | tr '/:' '__')\"; "
        "rm -f \"$COGNUS_CC_STAGE\" >/dev/null 2>&1 || true; "
        "if cat \"$COGNUS_CC_CANDIDATE\" > \"$COGNUS_CC_STAGE\" 2>/dev/null || sudo -n cat \"$COGNUS_CC_CANDIDATE\" > \"$COGNUS_CC_STAGE\" 2>/dev/null; then chmod 600 \"$COGNUS_CC_STAGE\" >/dev/null 2>&1 || true; fi; "
        "if [ -s \"$COGNUS_CC_STAGE\" ]; then {result_var}=$COGNUS_CC_STAGE; else {result_var}=$COGNUS_CC_CANDIDATE; fi; "
        "break 2; "
        "fi; "
        "done; "
        "for COGNUS_CC_TERM in {search_terms_words}; do "
        "[ -n \"$COGNUS_CC_TERM\" ] || continue; "
        "COGNUS_CC_CANDIDATE=$(find \"$COGNUS_CC_ROOT\" -maxdepth 8 -type f \\( -name \"*$COGNUS_CC_TERM*.tar.gz\" -o -name \"*$COGNUS_CC_TERM*.tgz\" -o -name \"$COGNUS_CC_TERM*.tar.gz\" -o -name \"$COGNUS_CC_TERM*.tgz\" \\) 2>/dev/null | head -n 1); "
        "if [ -n \"$COGNUS_CC_CANDIDATE\" ] && [ -s \"$COGNUS_CC_CANDIDATE\" ]; then "
        "COGNUS_CC_STAGE_ROOT=/tmp/cognus/chaincode-stage; "
        "mkdir -p \"$COGNUS_CC_STAGE_ROOT\" >/dev/null 2>&1 || true; "
        "COGNUS_CC_STAGE_SAFE=$(basename \"$(dirname \"$COGNUS_CC_CANDIDATE\")\" | tr '/:' '__'); "
        "COGNUS_CC_STAGE=\"$COGNUS_CC_STAGE_ROOT/$COGNUS_CC_STAGE_SAFE-$(basename \"$COGNUS_CC_CANDIDATE\" | tr '/:' '__')\"; "
        "rm -f \"$COGNUS_CC_STAGE\" >/dev/null 2>&1 || true; "
        "if cat \"$COGNUS_CC_CANDIDATE\" > \"$COGNUS_CC_STAGE\" 2>/dev/null || sudo -n cat \"$COGNUS_CC_CANDIDATE\" > \"$COGNUS_CC_STAGE\" 2>/dev/null; then chmod 600 \"$COGNUS_CC_STAGE\" >/dev/null 2>&1 || true; fi; "
        "if [ -s \"$COGNUS_CC_STAGE\" ]; then {result_var}=$COGNUS_CC_STAGE; else {result_var}=$COGNUS_CC_CANDIDATE; fi; "
        "break 2; "
        "fi; "
        "done; "
        "done; "
    ).format(
        result_var=normalized_result_var,
        candidate_files_words=candidate_files_words,
        search_terms_words=search_terms_words,
    )


def _build_chaincode_package_id_lookup_step(result_var, queryinstalled_var, search_terms):
    normalized_result_var = str(result_var or "").strip()
    normalized_queryinstalled_var = str(queryinstalled_var or "").strip()
    if not normalized_result_var or not normalized_queryinstalled_var:
        return ""
    search_terms_words = _build_shell_word_list(search_terms)
    queryinstalled_var_ref = "${{{}}}".format(normalized_queryinstalled_var)
    result_var_ref = "${{{}}}".format(normalized_result_var)
    return (
        "{result_var}=''; "
        "for COGNUS_CC_TERM in {search_terms_words}; do "
        "[ -n \"$COGNUS_CC_TERM\" ] || continue; "
        "{result_var}=$(printf '%s\\n' \"{queryinstalled_var_ref}\" | grep -i 'Package ID:' | grep -i -- \"$COGNUS_CC_TERM\" | head -n 1 | cut -d: -f2- | cut -d, -f1 | tr -d ' \\r\\n\\t' || true); "
        "if [ -n \"{result_var_ref}\" ]; then break; fi; "
        "done; "
        "if [ -z \"{result_var_ref}\" ]; then {result_var}=$(printf '%s\\n' \"{queryinstalled_var_ref}\" | grep -i 'Package ID:' | head -n 1 | cut -d: -f2- | cut -d, -f1 | tr -d ' \\r\\n\\t' || true); fi; "
    ).format(
        result_var=normalized_result_var,
        result_var_ref=result_var_ref,
        queryinstalled_var_ref=queryinstalled_var_ref,
        search_terms_words=search_terms_words,
    )


def _build_configtx_profile_autodetect_step(channel_id, config_dir_var_name="cfg_dir"):
    normalized_config_dir_var_name = str(config_dir_var_name or "cfg_dir").strip() or "cfg_dir"
    config_dir_ref = "${}".format(normalized_config_dir_var_name)
    return (
        "COGNUS_PROFILE_LIST=$(awk '"
        "BEGIN{{in_profiles=0}} "
        "/^[[:space:]]*Profiles:[[:space:]]*$/ {{in_profiles=1; next}} "
        "in_profiles && /^[^[:space:]][^:]*:[[:space:]]*$/ {{exit}} "
        "in_profiles && /^[[:space:]][[:space:]][A-Za-z0-9_.-]+:[[:space:]]*$/ {{line=$0; sub(/^[[:space:]]+/, \"\", line); sub(/:[[:space:]]*$/, \"\", line); print line}}"
        "' \"{config_dir_ref}/configtx.yaml\" 2>/dev/null | tr '\\n' ' '); "
        "if [ -z \"$COGNUS_PROFILE_LIST\" ]; then COGNUS_PROFILE_LIST='SampleSingleMSPChannel SampleAppChannelEtcdRaft SampleDevModeEtcdRaft SampleSingleMSPSolo SampleDevModeSolo SampleSingleMSPKafka SampleDevModeKafka'; fi; "
        "COGNUS_TX_PROFILE=''; "
        "for alt_tx_profile in $COGNUS_PROFILE_LIST; do "
        "FABRIC_CFG_PATH=\"{config_dir_ref}\" configtxgen -inspectProfile \"$alt_tx_profile\" >/dev/null 2>&1 || continue; "
        "FABRIC_CFG_PATH=\"{config_dir_ref}\" configtxgen -profile \"$alt_tx_profile\" -channelID {channel} -outputCreateChannelTx /tmp/cognus/channel.tx >/dev/null 2>&1 && COGNUS_TX_PROFILE=\"$alt_tx_profile\" && break; "
        "done; "
        "COGNUS_BLOCK_PROFILE=''; "
        "COGNUS_BLOCK_PROBE=\"{config_dir_ref}/.cognus-block-probe\"; "
        "rm -f \"$COGNUS_BLOCK_PROBE\" >/dev/null 2>&1 || true; "
        "for alt_block_profile in $COGNUS_PROFILE_LIST; do "
        "rm -f \"$COGNUS_BLOCK_PROBE\" >/dev/null 2>&1 || true; "
        "FABRIC_CFG_PATH=\"{config_dir_ref}\" configtxgen -profile \"$alt_block_profile\" -channelID {channel} -outputBlock \"$COGNUS_BLOCK_PROBE\" >/dev/null 2>&1 && COGNUS_BLOCK_PROFILE=\"$alt_block_profile\" && break; "
        "done; "
        "rm -f \"$COGNUS_BLOCK_PROBE\" >/dev/null 2>&1 || true; "
        "if [ -n \"$COGNUS_BLOCK_PROFILE\" ]; then FABRIC_CFG_PATH=\"{config_dir_ref}\" configtxgen -profile \"$COGNUS_BLOCK_PROFILE\" -channelID {channel} -outputBlock /tmp/cognus/channel.block >/dev/null 2>&1; fi; "
        "if [ -n \"$COGNUS_TX_PROFILE\" ]; then FABRIC_CFG_PATH=\"{config_dir_ref}\" configtxgen -profile \"$COGNUS_TX_PROFILE\" -channelID {channel} -outputCreateChannelTx /tmp/cognus/channel.tx >/dev/null 2>&1 || true; fi; "
    ).format(
        channel=shlex.quote(str(channel_id or "").strip().lower()),
        config_dir_ref=config_dir_ref,
    )


def _build_configtx_block_direct_fallback_step(channel_id):
    return (
        "COGNUS_DIRECT_BLOCK_PROFILE=''; "
        "rm -f \"$COGNUS_TMP_ROOT/channel.block.direct.out\" \"$COGNUS_TMP_ROOT/channel.block.direct.err\" >/dev/null 2>&1 || true; "
        "for COGNUS_DIRECT_BLOCK_CANDIDATE in SampleSingleMSPChannel SampleSingleMSPSolo SampleDevModeSolo; do "
        "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -e COGNUS_DIRECT_BLOCK_CANDIDATE=\"$COGNUS_DIRECT_BLOCK_CANDIDATE\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc "
        "\"cfg=/tmp/cognus/direct-cfg; rm -rf \\\"\\$cfg\\\"; mkdir -p \\\"\\$cfg\\\"; cp /tmp/cognus/configtx.yaml \\\"\\$cfg/configtx.yaml\\\"; "
        "if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; "
        "elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; fi; "
        "FABRIC_CFG_PATH=\\\"\\$cfg\\\" configtxgen -profile \\\"\\$COGNUS_DIRECT_BLOCK_CANDIDATE\\\" -channelID {channel} -outputBlock /tmp/cognus/channel.block "
        ">/tmp/cognus/channel.block.direct.out 2>/tmp/cognus/channel.block.direct.err && chmod 666 /tmp/cognus/channel.block >/dev/null 2>&1 || true\" "
        ">/dev/null 2>&1 || true; "
        "if [ -s \"$COGNUS_HOST_BLOCK\" ]; then COGNUS_DIRECT_BLOCK_PROFILE=$COGNUS_DIRECT_BLOCK_CANDIDATE; break; fi; "
        "done; "
        "if [ -s \"$COGNUS_HOST_BLOCK\" ] && [ -n \"$COGNUS_DIRECT_BLOCK_PROFILE\" ]; then "
        "printf '%s\\n' COGNUS_CHANNEL_BLOCK_RECOVERED_DIRECT:{channel}:$COGNUS_DIRECT_BLOCK_PROFILE >&2; "
        "fi; "
        "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_TMP_ROOT/channel.block.direct.err\" ]; then "
        "COGNUS_DIRECT_BLOCK_LAST=$(tail -n 1 \"$COGNUS_TMP_ROOT/channel.block.direct.err\" | tr -d '\\r' | cut -c1-220); "
        "[ -n \"$COGNUS_DIRECT_BLOCK_LAST\" ] && printf '%s\\n' COGNUS_CHANNEL_BLOCK_DIRECT_DIAG:$COGNUS_DIRECT_BLOCK_LAST >&2; "
        "fi; "
    ).format(channel=shlex.quote(str(channel_id or "").strip().lower()))


def _build_channel_block_expected_channel_guard_step(
    channel_id,
    host_block_var_name="COGNUS_HOST_BLOCK",
    tmp_root_var_name="COGNUS_TMP_ROOT",
):
    normalized_channel_id = str(channel_id or "").strip().lower()
    host_block_ref = "${}".format(
        str(host_block_var_name or "COGNUS_HOST_BLOCK").strip() or "COGNUS_HOST_BLOCK"
    )
    tmp_root_ref = "${}".format(
        str(tmp_root_var_name or "COGNUS_TMP_ROOT").strip() or "COGNUS_TMP_ROOT"
    )
    return (
        "if [ -s \"{host_block_ref}\" ]; then "
        "COGNUS_BLOCK_EXPECTED_CHANNEL={channel}; "
        "COGNUS_BLOCK_CHANNEL_GUARD_JSON={tmp_root_ref}/channel.block.channel.guard.json; "
        "COGNUS_BLOCK_CHANNEL_GUARD_ERR={tmp_root_ref}/channel.block.channel.guard.err; "
        "rm -f \"$COGNUS_BLOCK_CHANNEL_GUARD_JSON\" \"$COGNUS_BLOCK_CHANNEL_GUARD_ERR\" >/dev/null 2>&1 || true; "
        "docker run --rm -v \"{tmp_root_ref}\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc "
        "'configtxgen -inspectBlock /tmp/cognus/channel.block >/tmp/cognus/channel.block.channel.guard.json 2>/tmp/cognus/channel.block.channel.guard.err || true' "
        ">/dev/null 2>&1 || true; "
        "if [ ! -s \"$COGNUS_BLOCK_CHANNEL_GUARD_JSON\" ] || ! grep -F '\"channel_id\": \"{channel}\"' \"$COGNUS_BLOCK_CHANNEL_GUARD_JSON\" >/dev/null 2>&1; then "
        "COGNUS_BLOCK_ACTUAL_CHANNEL=$(grep -oE '\"channel_id\"[[:space:]]*:[[:space:]]*\"[^\"]+\"' \"$COGNUS_BLOCK_CHANNEL_GUARD_JSON\" 2>/dev/null | head -n 1 | sed -E 's/.*\"channel_id\"[[:space:]]*:[[:space:]]*\"([^\"]+)\"/\\1/' | tr -d '\\r\\n' || true); "
        "if [ -n \"$COGNUS_BLOCK_ACTUAL_CHANNEL\" ] && [ \"$COGNUS_BLOCK_ACTUAL_CHANNEL\" != \"$COGNUS_BLOCK_EXPECTED_CHANNEL\" ]; then "
        "printf '%s\\n' COGNUS_CHANNEL_BLOCK_CHANNEL_MISMATCH:{channel}:$COGNUS_BLOCK_ACTUAL_CHANNEL >&2; "
        "fi; "
        "rm -f \"{host_block_ref}\" \"$COGNUS_BLOCK_CHANNEL_GUARD_JSON\" \"$COGNUS_BLOCK_CHANNEL_GUARD_ERR\" >/dev/null 2>&1 || true; "
        "fi; "
        "fi; "
    ).format(
        channel=shlex.quote(normalized_channel_id),
        host_block_ref=host_block_ref,
        tmp_root_ref=tmp_root_ref,
    )


def _default_ccapi_search_payload():
    return {
        "query": {
            "selector": {
                "@assetType": "person",
            },
            "limit": 10,
            "bookmark": "",
        },
        "resolve": True,
    }


def _collect_gateway_channel_chaincode_pairs(run_state, host_row):
    org_id, channel_ids, chaincode_ids, organization = _collect_apigateway_org_scope(
        run_state,
        host_row,
    )
    api_registry = run_state.get("api_registry", []) if isinstance(run_state, dict) else []
    normalized_org_id = str(org_id or "").strip().lower()
    pairs = []
    seen = set()
    route_paths_by_pair = {}

    def _append_pair(channel_id, chaincode_id, route_path=""):
        normalized_channel_id = str(channel_id or "").strip().lower()
        normalized_chaincode_id = str(chaincode_id or "").strip().lower()
        normalized_route_path = str(route_path or "").strip()
        if not normalized_channel_id or not normalized_chaincode_id:
            return
        pair_key = (normalized_channel_id, normalized_chaincode_id)
        if normalized_route_path and pair_key not in route_paths_by_pair:
            route_paths_by_pair[pair_key] = normalized_route_path
        key = (
            normalized_channel_id,
            normalized_chaincode_id,
            normalized_route_path.lower(),
        )
        if key in seen:
            return
        seen.add(key)
        pairs.append(
            {
                "channel_id": normalized_channel_id,
                "chaincode_id": normalized_chaincode_id,
                "route_path": normalized_route_path,
            }
        )

    organization_apis = organization.get("apis", []) if isinstance(organization, dict) else []
    if not isinstance(organization_apis, list):
        organization_apis = []
    for api_row in organization_apis:
        if not isinstance(api_row, dict):
            continue
        _append_pair(
            api_row.get("channel_id", ""),
            api_row.get("chaincode_id", ""),
            api_row.get("route_path", ""),
        )

    for api_row in api_registry if isinstance(api_registry, list) else []:
        if not isinstance(api_row, dict):
            continue
        api_org_id = str(
            api_row.get("org_id")
            or api_row.get("org_name")
            or api_row.get("organization")
            or ""
        ).strip().lower()
        if normalized_org_id and api_org_id and api_org_id != normalized_org_id:
            continue
        _append_pair(
            api_row.get("channel_id", ""),
            api_row.get("chaincode_id", ""),
            api_row.get("route_path", ""),
        )

    organization_channels = organization.get("channels", []) if isinstance(organization, dict) else []
    if isinstance(organization_channels, list):
        for channel_row in organization_channels:
            if not isinstance(channel_row, dict):
                continue
            channel_id = (
                channel_row.get("channel_id")
                or channel_row.get("name")
                or channel_row.get("id")
                or ""
            )
            for chaincode_id in channel_row.get("chaincodes", []) if isinstance(channel_row.get("chaincodes"), list) else []:
                _append_pair(channel_id, chaincode_id)

    scoped_channel_set = {
        str(channel_id or "").strip().lower()
        for channel_id in channel_ids
        if str(channel_id or "").strip()
    }
    for metadata_row in _collect_runbook_chaincode_metadata_rows(run_state):
        if not isinstance(metadata_row, dict):
            continue
        channel_id = str(metadata_row.get("channel_id", "") or "").strip().lower()
        chaincode_id = str(metadata_row.get("chaincode_id", "") or "").strip().lower()
        if not channel_id or not chaincode_id:
            continue
        if scoped_channel_set and channel_id not in scoped_channel_set:
            continue
        _append_pair(
            channel_id,
            chaincode_id,
            route_paths_by_pair.get((channel_id, chaincode_id), ""),
        )

    if not pairs:
        for channel_id in channel_ids:
            for chaincode_id in chaincode_ids:
                _append_pair(channel_id, chaincode_id)

    return {
        "org_id": normalized_org_id,
        "channels": [str(channel_id or "").strip().lower() for channel_id in channel_ids if str(channel_id or "").strip()],
        "chaincodes": [str(chaincode_id or "").strip().lower() for chaincode_id in chaincode_ids if str(chaincode_id or "").strip()],
        "pairs": pairs,
        "organization": organization,
    }


def _collect_runbook_chaincode_scope_pairs(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    pairs = []
    seen = set()

    def _append_pair(channel_id, chaincode_id):
        normalized_channel_id = str(channel_id or "").strip().lower()
        normalized_chaincode_id = str(chaincode_id or "").strip().lower()
        if not normalized_channel_id or not normalized_chaincode_id:
            return
        pair_key = (normalized_channel_id, normalized_chaincode_id)
        if pair_key in seen:
            return
        seen.add(pair_key)
        pairs.append(
            {
                "channel_id": normalized_channel_id,
                "chaincode_id": normalized_chaincode_id,
            }
        )

    host_mapping = safe_run_state.get("host_mapping", [])
    for host_row in host_mapping if isinstance(host_mapping, list) else []:
        if not isinstance(host_row, dict):
            continue
        node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        if node_type not in ("apigateway", "netapi"):
            continue
        scope = _collect_gateway_channel_chaincode_pairs(safe_run_state, host_row)
        for pair in scope.get("pairs", []) if isinstance(scope, dict) else []:
            if not isinstance(pair, dict):
                continue
            _append_pair(pair.get("channel_id", ""), pair.get("chaincode_id", ""))

    for metadata_row in _collect_runbook_chaincode_metadata_rows(safe_run_state):
        if not isinstance(metadata_row, dict):
            continue
        _append_pair(
            metadata_row.get("channel_id", ""),
            metadata_row.get("chaincode_id", ""),
        )

    return pairs


def _collect_runbook_chaincode_binding_issues(run_state):
    issues = []
    for pair in _collect_runbook_chaincode_scope_pairs(run_state):
        channel_id = str(pair.get("channel_id", "") or "").strip().lower()
        chaincode_id = str(pair.get("chaincode_id", "") or "").strip().lower()
        if not channel_id or not chaincode_id:
            continue

        chaincode_artifact_metadata = _resolve_chaincode_artifact_metadata(
            run_state,
            channel_id,
            chaincode_id,
        )
        raw_artifact_refs = [
            _normalize_chaincode_metadata_token(value)
            for value in chaincode_artifact_metadata.get("artifact_refs", [])
            if _normalize_chaincode_metadata_token(value)
        ]
        raw_source_refs = [
            _normalize_chaincode_metadata_token(value)
            for value in chaincode_artifact_metadata.get("source_refs", [])
            if _normalize_chaincode_metadata_token(value)
        ]
        resolved_artifact_path = _resolve_local_chaincode_artifact_path(
            chaincode_artifact_metadata
        )
        resolved_source_dir = _resolve_local_chaincode_source_dir(
            chaincode_artifact_metadata
        )

        issue_reason = ""
        if not _chaincode_artifact_metadata_declares_binding(chaincode_artifact_metadata):
            issue_reason = "binding_missing"
        elif resolved_artifact_path or resolved_source_dir:
            issue_reason = ""
        elif not _chaincode_artifact_metadata_has_explicit_binding(
            chaincode_artifact_metadata
        ):
            issue_reason = "binding_invalid"
        else:
            issue_reason = "binding_unavailable"

        if not issue_reason:
            continue

        issues.append(
            {
                "channel_id": channel_id,
                "chaincode_id": chaincode_id,
                "reason": issue_reason,
                "artifact_refs": raw_artifact_refs,
                "source_refs": raw_source_refs,
                "resolved_artifact_path": resolved_artifact_path,
                "resolved_source_dir": resolved_source_dir,
            }
        )
    return issues


def _gateway_verify_candidate_identity(host_row):
    safe_host_row = host_row if isinstance(host_row, dict) else {}
    return (
        _normalize_runtime_component_node_type(safe_host_row.get("node_type", "")),
        str(safe_host_row.get("node_id", "") or "").strip().lower(),
        str(safe_host_row.get("host_ref", "") or "").strip().lower(),
        str(safe_host_row.get("host_address", "") or "").strip().lower(),
    )


def _is_primary_gateway_verify_host(run_state, host_row):
    safe_host_row = host_row if isinstance(host_row, dict) else {}
    node_type = _normalize_runtime_component_node_type(safe_host_row.get("node_type", ""))
    if node_type not in ("apigateway", "netapi"):
        return True
    if not isinstance(run_state, dict):
        return node_type == "apigateway"

    current_scope = _collect_gateway_channel_chaincode_pairs(run_state, safe_host_row)
    current_org_id = str(current_scope.get("org_id", "") or "").strip().lower()
    if not current_org_id:
        return node_type == "apigateway"

    candidates = []
    for candidate in run_state.get("host_mapping", []) if isinstance(run_state.get("host_mapping", []), list) else []:
        if not isinstance(candidate, dict):
            continue
        candidate_type = _normalize_runtime_component_node_type(candidate.get("node_type", ""))
        if candidate_type not in ("apigateway", "netapi"):
            continue
        candidate_scope = _collect_gateway_channel_chaincode_pairs(run_state, candidate)
        candidate_org_id = str(candidate_scope.get("org_id", "") or "").strip().lower()
        if candidate_org_id != current_org_id:
            continue
        candidates.append(candidate)

    if not candidates:
        return node_type == "apigateway"

    candidates.sort(
        key=lambda row: (
            0 if _normalize_runtime_component_node_type(row.get("node_type", "")) == "apigateway" else 1,
            str(row.get("host_ref", "") or "").strip().lower(),
            str(row.get("node_id", "") or "").strip().lower(),
            str(row.get("host_address", "") or "").strip().lower(),
        )
    )
    return _gateway_verify_candidate_identity(candidates[0]) == _gateway_verify_candidate_identity(
        safe_host_row
    )


def _resolve_runtime_container_name_for_node_type(run_state, host_row, target_node_type):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return ""
    host_mapping = run_state.get("host_mapping", [])
    if not isinstance(host_mapping, list):
        host_mapping = []

    alias_registry = _build_topology_org_alias_registry(run_state.get("topology_catalog", {}))
    host_org = _resolve_canonical_org_id(host_row.get("org_id", ""), alias_registry)
    normalized_host_org = str(host_org or "").strip().lower()

    first_candidate = {}
    for candidate in host_mapping:
        if not isinstance(candidate, dict):
            continue
        candidate_type = _normalize_runtime_component_node_type(candidate.get("node_type", ""))
        if candidate_type != target_node_type:
            continue
        if not first_candidate:
            first_candidate = candidate
        candidate_org = _resolve_canonical_org_id(candidate.get("org_id", ""), alias_registry)
        if normalized_host_org and str(candidate_org or "").strip().lower() == normalized_host_org:
            return _resolve_runbook_container_name(run_state, candidate)

    if first_candidate:
        return _resolve_runbook_container_name(run_state, first_candidate)
    return ""


def _resolve_runtime_host_row_for_node_type(run_state, host_row, target_node_type):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return {}
    host_mapping = run_state.get("host_mapping", [])
    if not isinstance(host_mapping, list):
        host_mapping = []

    alias_registry = _build_topology_org_alias_registry(run_state.get("topology_catalog", {}))
    host_org = _resolve_canonical_org_id(host_row.get("org_id", ""), alias_registry)
    normalized_host_org = str(host_org or "").strip().lower()

    first_candidate = {}
    for candidate in host_mapping:
        if not isinstance(candidate, dict):
            continue
        candidate_type = _normalize_runtime_component_node_type(candidate.get("node_type", ""))
        if candidate_type != target_node_type:
            continue
        if not first_candidate:
            first_candidate = candidate
        candidate_org = _resolve_canonical_org_id(candidate.get("org_id", ""), alias_registry)
        if normalized_host_org and str(candidate_org or "").strip().lower() == normalized_host_org:
            return candidate

    if first_candidate:
        return first_candidate
    return {}


def _resolve_runtime_node_tls_expected(run_state, host_row, target_node_type):
    normalized_target_type = _normalize_runtime_component_node_type(target_node_type)
    candidate_row = _resolve_runtime_host_row_for_node_type(
        run_state,
        host_row,
        normalized_target_type,
    )
    environment_rows = candidate_row.get("environment", []) if isinstance(candidate_row, dict) else []
    if not isinstance(environment_rows, list):
        environment_rows = []

    def _truthy(value):
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    if normalized_target_type == "peer":
        for env_entry in environment_rows:
            if not isinstance(env_entry, (list, tuple)) or len(env_entry) < 2:
                continue
            env_key = str(env_entry[0] or "").strip().upper()
            env_val = env_entry[1]
            if env_key == "CORE_PEER_TLS_ENABLED":
                return _truthy(env_val)
        return False

    if normalized_target_type == "orderer":
        for env_entry in environment_rows:
            if not isinstance(env_entry, (list, tuple)) or len(env_entry) < 2:
                continue
            env_key = str(env_entry[0] or "").strip().upper()
            env_val = env_entry[1]
            if env_key == "ORDERER_GENERAL_TLS_ENABLED":
                return _truthy(env_val)
        return False

    return False


def _resolve_runtime_host_env_value(run_state, host_row, target_node_type, env_key):
    normalized_target_type = _normalize_runtime_component_node_type(target_node_type)
    candidate_row = _resolve_runtime_host_row_for_node_type(
        run_state,
        host_row,
        normalized_target_type,
    )
    environment_rows = candidate_row.get("environment", []) if isinstance(candidate_row, dict) else []
    if not isinstance(environment_rows, list):
        environment_rows = []

    normalized_env_key = str(env_key or "").strip().upper()
    if not normalized_env_key:
        return ""

    for env_entry in environment_rows:
        if not isinstance(env_entry, (list, tuple)) or len(env_entry) < 2:
            continue
        entry_key = str(env_entry[0] or "").strip().upper()
        if entry_key != normalized_env_key:
            continue
        return str(env_entry[1] or "").strip()
    return ""


def _build_apigateway_bootstrap_payload(run_state, host_row, data_mount="/app/data"):
    org_id, channel_ids, chaincode_ids, organization = _collect_apigateway_org_scope(run_state, host_row)
    if not org_id:
        return {}

    peer_local_mspid = _resolve_runtime_host_env_value(
        run_state,
        host_row,
        "peer",
        "CORE_PEER_LOCALMSPID",
    )
    organization_msp_id = str(organization.get("msp_id") or "").strip()
    peer_local_mspid = str(peer_local_mspid or "").strip()
    msp_id = organization_msp_id or peer_local_mspid or "Org1MSP"
    peer_container = _resolve_runtime_container_name_for_node_type(run_state, host_row, "peer")
    orderer_container = _resolve_runtime_container_name_for_node_type(run_state, host_row, "orderer")
    peer_tls_enabled = _resolve_runtime_node_tls_expected(run_state, host_row, "peer")
    orderer_tls_enabled = _resolve_runtime_node_tls_expected(run_state, host_row, "orderer")

    peer_payload = {}
    if peer_container:
        peer_entry = {
            "url": f"{'grpcs' if peer_tls_enabled else 'grpc'}://{peer_container}:7051",
        }
        if peer_tls_enabled:
            peer_entry["tlsCACerts"] = {"path": "/app/data/msp/tlscacerts/tlsca-cert.pem"}
        peer_payload[peer_container] = peer_entry

    orderer_payload = {}
    if orderer_container:
        orderer_entry = {
            "url": f"{'grpcs' if orderer_tls_enabled else 'grpc'}://{orderer_container}:7050",
        }
        if orderer_tls_enabled:
            orderer_entry["tlsCACerts"] = {"path": "/app/data/orderer-tlsca.pem"}
            orderer_entry["grpcOptions"] = {
                "ssl-target-name-override": RUNBOOK_ORDERER_TLS_HOST_OVERRIDE,
                "hostnameOverride": RUNBOOK_ORDERER_TLS_HOST_OVERRIDE,
            }
        orderer_payload[orderer_container] = orderer_entry

    channels_payload = {}
    for channel_id in channel_ids:
        row = {}
        if peer_container:
            row["peers"] = {peer_container: {}}
        if orderer_container:
            row["orderers"] = [orderer_container]
        channels_payload[channel_id] = row

    normalized_data_mount = str(data_mount or "/app/data").strip() or "/app/data"
    identities_entry = {
        "mspId": msp_id,
        "ccpPath": f"{normalized_data_mount}/connection.json",
        "connectionProfilePath": f"{normalized_data_mount}/connection.json",
        "certPath": f"{normalized_data_mount}/msp/signcerts/cert.pem",
        "keyPath": f"{normalized_data_mount}/msp/keystore/key.pem",
        "cryptoPath": f"{normalized_data_mount}/msp",
        "channels": channel_ids,
        "chaincodes": chaincode_ids,
        "discoveryEnabled": False,
        "discoveryAsLocalhost": False,
    }

    return {
        "connection_profile": {
            "name": "cognus-fabric-runtime",
            "version": "1.0.0",
            "client": {
                "organization": org_id,
                "connection": {"timeout": {"peer": {"endorser": "300"}}},
            },
            "organizations": {
                org_id: {
                    "mspid": msp_id,
                    "peers": [peer_container] if peer_container else [],
                }
            },
            "peers": peer_payload,
            "orderers": orderer_payload,
            "channels": channels_payload,
        },
        "identities": {
            "selectionContract": {
                "queryParam": "org",
                "header": "x-fabric-org",
                "requireExplicitOrgWhenAmbiguous": True,
            },
            "defaultOrg": org_id,
            "organizations": {org_id: identities_entry},
            "default": dict(identities_entry),
        },
    }


def _resolve_apigateway_runtime_bootstrap_step(run_state, host_row):
    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type not in ("apigateway", "netapi"):
        return ""

    host_dir = _resolve_apigateway_runtime_host_dir(run_state, host_row)
    if not host_dir:
        return "printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:no_host_dir >&2; "

    data_mount = "/app/data" if node_type == "apigateway" else "/workspace/chaincode-gateway/data"
    payload = _build_apigateway_bootstrap_payload(run_state, host_row, data_mount=data_mount)
    identities_payload = payload.get("identities") if isinstance(payload.get("identities"), dict) else {}
    organizations_payload = identities_payload.get("organizations") if isinstance(identities_payload.get("organizations"), dict) else {}
    if not organizations_payload:
        return "printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:empty_organizations >&2; "

    connection_json = json.dumps(
        payload.get("connection_profile", {}),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    identities_json = json.dumps(
        identities_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    declared_msp_id = str(
        identities_payload.get("default", {}).get("mspId", "")
        if isinstance(identities_payload.get("default", {}), dict)
        else ""
    ).strip()
    connection_b64 = base64.b64encode(connection_json.encode("utf-8")).decode("ascii")
    identities_b64 = base64.b64encode(identities_json.encode("utf-8")).decode("ascii")
    gateway_index_source = _load_runbook_chaincode_gateway_index_source()
    gateway_index_b64 = (
        base64.b64encode(gateway_index_source.encode("utf-8")).decode("ascii")
        if gateway_index_source
        else ""
    )
    if (
        gateway_index_b64
        and RUNBOOK_APIGATEWAY_INLINE_GATEWAY_INDEX_MAX_BYTES > 0
        and len(gateway_index_b64.encode("utf-8"))
        > RUNBOOK_APIGATEWAY_INLINE_GATEWAY_INDEX_MAX_BYTES
    ):
        gateway_index_b64 = ""

    host_dir_q = shlex.quote(host_dir)
    connection_path_q = shlex.quote(f"{host_dir}/connection.json")
    identities_path_q = shlex.quote(f"{host_dir}/identities.json")
    gateway_index_path_q = shlex.quote(f"{host_dir}/gateway-index.js")
    wallet_dir_q = shlex.quote(f"{host_dir}/wallet")
    msp_dir_q = shlex.quote(f"{host_dir}/msp")
    msp_tlsca_dir_q = shlex.quote(f"{host_dir}/msp/tlscacerts")
    orderer_tlsca_path_q = shlex.quote(f"{host_dir}/orderer-tlsca.pem")
    msp_identity_q = shlex.quote("/var/lib/cognus/msp")
    _, scoped_channel_ids, _, _ = _collect_apigateway_org_scope(run_state, host_row)
    primary_channel_id = (
        str(scoped_channel_ids[0] or "").strip().lower()
        if isinstance(scoped_channel_ids, list) and scoped_channel_ids
        else ""
    )
    cfg_msp_dir_q = shlex.quote(
        f"/tmp/cognus-run-{str(run_state.get('run_id', '') or '').strip()}-{primary_channel_id}/cfg-gen/msp"
    )
    cfg_msp_cert_q = shlex.quote(
        f"/tmp/cognus-run-{str(run_state.get('run_id', '') or '').strip()}-{primary_channel_id}/cfg-gen/msp/signcerts/cert.pem"
    )
    cfg_msp_key_q = shlex.quote(
        f"/tmp/cognus-run-{str(run_state.get('run_id', '') or '').strip()}-{primary_channel_id}/cfg-gen/msp/keystore/key.pem"
    )
    declared_msp_id_q = shlex.quote(declared_msp_id)
    apigateway_org_id = str(host_row.get("org_id", "") or "").strip().lower()
    org_crypto_root_q = shlex.quote(
        f"/var/cognus/crypto/{apigateway_org_id}" if apigateway_org_id else "/var/cognus/crypto"
    )
    crypto_root_q = shlex.quote("/var/cognus/crypto")
    gateway_index_write_step = ""
    if gateway_index_b64:
        gateway_index_write_step = (
            f"printf %s {shlex.quote(gateway_index_b64)} | base64 -d > {gateway_index_path_q}; "
            f"chmod 0644 {gateway_index_path_q} || true; "
        )

    peer_container = _resolve_runtime_container_name_for_node_type(run_state, host_row, "peer")
    peer_container_q = shlex.quote(peer_container)
    peer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "peer")
    peer_node_id_q = shlex.quote(str(peer_host_row.get("node_id", "") or "").strip())

    orderer_container = _resolve_runtime_container_name_for_node_type(run_state, host_row, "orderer")
    orderer_container_q = shlex.quote(orderer_container)
    run_id_q = shlex.quote(str(run_state.get("run_id", "") or "").strip())
    orderer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "orderer")
    orderer_node_id_q = shlex.quote(str(orderer_host_row.get("node_id", "") or "").strip())
    fallback_tlsca_path_q = shlex.quote(f"{host_dir}/msp/tlscacerts/tlsca-cert.pem")

    orderer_tlsca_materialize_step = (
        "COGNUS_APIGW_PEER_CONTAINER={peer}; "
        "if [ -z \"$COGNUS_APIGW_PEER_CONTAINER\" ] || ! docker inspect \"$COGNUS_APIGW_PEER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_APIGW_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --filter label=cognus.node_id={peer_node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_APIGW_PEER_CONTAINER\" ]; then "
        "COGNUS_APIGW_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -n \"$COGNUS_APIGW_PEER_CONTAINER\" ]; then "
        "docker cp \"$COGNUS_APIGW_PEER_CONTAINER:/etc/hyperledger/fabric/msp/.\" {msp_dir}/ >/dev/null 2>&1 || true; "
        "fi; "
        "COGNUS_APIGW_ORDERER_CONTAINER={orderer}; "
        "if [ -z \"$COGNUS_APIGW_ORDERER_CONTAINER\" ] || ! docker inspect \"$COGNUS_APIGW_ORDERER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_APIGW_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=orderer --filter label=cognus.node_id={orderer_node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_APIGW_ORDERER_CONTAINER\" ]; then "
        "COGNUS_APIGW_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=orderer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "rm -f {orderer_tlsca_path} >/dev/null 2>&1 || true; "
        "if [ -n \"$COGNUS_APIGW_ORDERER_CONTAINER\" ]; then "
        "docker cp \"$COGNUS_APIGW_ORDERER_CONTAINER:/etc/hyperledger/fabric/tls/ca.crt\" {orderer_tlsca_path} >/dev/null 2>&1 || true; "
        "fi; "
        "if [ ! -s {orderer_tlsca_path} ] && [ -s {fallback_tlsca_path} ]; then cp {fallback_tlsca_path} {orderer_tlsca_path} >/dev/null 2>&1 || true; fi; "
    ).format(
        peer=peer_container_q,
        peer_node_id=peer_node_id_q,
        orderer=orderer_container_q,
        run_id=run_id_q,
        orderer_node_id=orderer_node_id_q,
        msp_dir=msp_dir_q,
        orderer_tlsca_path=orderer_tlsca_path_q,
        fallback_tlsca_path=fallback_tlsca_path_q,
    )

    bootstrap_fast_materialize_step = (
        f"{orderer_tlsca_materialize_step}"
        "if [ ! -s \"$APIGW_MSP_DIR/signcerts/cert.pem\" ] && [ -s \"$APIGW_MSP_DIR/signcerts/peer.pem\" ]; then cp \"$APIGW_MSP_DIR/signcerts/peer.pem\" \"$APIGW_MSP_DIR/signcerts/cert.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$APIGW_MSP_DIR/keystore/key.pem\" ]; then apigw_key_candidate=$(find \"$APIGW_MSP_DIR/keystore\" -type f 2>/dev/null | head -n 1); [ -n \"$apigw_key_candidate\" ] && cp \"$apigw_key_candidate\" \"$APIGW_MSP_DIR/keystore/key.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$APIGW_MSP_DIR/tlscacerts/tlsca-cert.pem\" ]; then apigw_tlsca_seed=$(find \"$APIGW_MSP_DIR/tlscacerts\" \"$APIGW_MSP_DIR/cacerts\" -type f \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); [ -n \"$apigw_tlsca_seed\" ] && cp \"$apigw_tlsca_seed\" \"$APIGW_MSP_DIR/tlscacerts/tlsca-cert.pem\" >/dev/null 2>&1 || true; fi; "
    )

    bootstrap_slow_resolution_step = (
        "if [ ! -s \"$APIGW_MSP_DIR/signcerts/cert.pem\" ] || [ ! -s \"$APIGW_MSP_DIR/keystore/key.pem\" ] || [ ! -s \"$APIGW_MSP_DIR/tlscacerts/tlsca-cert.pem\" ]; then "
        "apigw_cert_src=$(find \"$APIGW_MSP_DIR\" -type f -readable \\( -path '*/signcerts/*' -o -path '*/admincerts/*' \\) \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_cert_src\" ] || apigw_cert_src=$(find \"$APIGW_MSP_DIR\" -type f -readable \\( -name '*-cert.pem' -o -name 'cert.pem' -o -name '*.pem' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_cert_src\" ] || apigw_cert_src=$(find \"$APIGW_ORG_CRYPTO_DIR\" -type f -readable \\( -path '*/msp/signcerts/*' -o -path '*/msp/admincerts/*' \\) \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_cert_src\" ] || apigw_cert_src=$(find \"$APIGW_ORG_CRYPTO_DIR\" -type f -readable \\( -name '*-cert.pem' -o -name 'cert.pem' -o -name '*.pem' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_cert_src\" ] || apigw_cert_src=$(find \"$APIGW_CRYPTO_ROOT\" -type f -readable \\( -path '*/msp/signcerts/*' -o -path '*/msp/admincerts/*' -o -name '*-cert.pem' -o -name 'cert.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "if [ -z \"$apigw_cert_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_cert_src=$(sudo -n find \"$APIGW_ORG_CRYPTO_DIR\" -type f \\( -path '*/msp/signcerts/*' -o -path '*/msp/admincerts/*' -o -name '*-cert.pem' -o -name 'cert.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_cert_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_cert_src=$(sudo -n find \"$APIGW_CRYPTO_ROOT\" -type f \\( -path '*/msp/signcerts/*' -o -path '*/msp/admincerts/*' -o -name '*-cert.pem' -o -name 'cert.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_cert_src\" ] && command -v docker >/dev/null 2>&1 && docker image inspect alpine:3.20 >/dev/null 2>&1 && [ -d \"$APIGW_CRYPTO_ROOT\" ]; then apigw_cert_src=$(docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20 sh -lc \"find /hostcrypto -type f \\\\( -path '*/msp/signcerts/*' -o -path '*/msp/admincerts/*' -o -name '*-cert.pem' -o -name 'cert.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\\\) 2>/dev/null | head -n 1\" | sed 's#^/hostcrypto#/var/cognus/crypto#'); fi; "
        "[ -n \"$apigw_cert_src\" ] || { printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_cert >&2; }; "
        "apigw_cert_dst=\"$APIGW_MSP_DIR/signcerts/cert.pem\"; "
        "if [ \"$apigw_cert_src\" != \"$apigw_cert_dst\" ]; then cp \"$apigw_cert_src\" \"$apigw_cert_dst\" >/dev/null 2>&1 || (test -r \"$apigw_cert_src\" && cat \"$apigw_cert_src\" > \"$apigw_cert_dst\" 2>/dev/null) || (command -v sudo >/dev/null 2>&1 && sudo -n cat \"$apigw_cert_src\" > \"$apigw_cert_dst\" 2>/dev/null) || (command -v docker >/dev/null 2>&1 && docker image inspect alpine:3.20 >/dev/null 2>&1 && [ -d \"$APIGW_CRYPTO_ROOT\" ] && apigw_cert_rel=\"${apigw_cert_src#/var/cognus/crypto/}\" && [ \"$apigw_cert_rel\" != \"$apigw_cert_src\" ] && docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20 sh -lc \"cat /hostcrypto/$apigw_cert_rel\" > \"$apigw_cert_dst\") || { printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:copy_cert_failed >&2; }; fi; "
        "apigw_key_src=$(find \"$APIGW_MSP_DIR\" -type f -readable -path '*/keystore/*' 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_key_src\" ] || apigw_key_src=$(find \"$APIGW_MSP_DIR\" -type f -readable \\( -name '*_sk' -o -name '*-key.pem' -o -name 'key.pem' -o -name '*.key' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_key_src\" ] || apigw_key_src=$(find \"$APIGW_ORG_CRYPTO_DIR\" -type f -readable -path '*/msp/keystore/*' 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_key_src\" ] || apigw_key_src=$(find \"$APIGW_ORG_CRYPTO_DIR\" -type f -readable \\( -name '*_sk' -o -name '*-key.pem' -o -name 'key.pem' -o -name '*.key' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_key_src\" ] || apigw_key_src=$(find \"$APIGW_CRYPTO_ROOT\" -type f -readable -path '*/msp/keystore/*' 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_key_src\" ] || apigw_key_src=$(find \"$APIGW_CRYPTO_ROOT\" -type f -readable \\( -name '*_sk' -o -name '*-key.pem' -o -name 'key.pem' -o -name '*.key' \\) 2>/dev/null | head -n 1); "
        "if [ -z \"$apigw_key_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_key_src=$(sudo -n find \"$APIGW_ORG_CRYPTO_DIR\" -type f -path '*/msp/keystore/*' 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_key_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_key_src=$(sudo -n find \"$APIGW_ORG_CRYPTO_DIR\" -type f \\( -name '*_sk' -o -name '*-key.pem' -o -name 'key.pem' -o -name '*.key' \\) 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_key_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_key_src=$(sudo -n find \"$APIGW_CRYPTO_ROOT\" -type f -path '*/msp/keystore/*' 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_key_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_key_src=$(sudo -n find \"$APIGW_CRYPTO_ROOT\" -type f \\( -name '*_sk' -o -name '*-key.pem' -o -name 'key.pem' -o -name '*.key' \\) 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_key_src\" ] && command -v docker >/dev/null 2>&1 && docker image inspect alpine:3.20 >/dev/null 2>&1 && [ -d \"$APIGW_CRYPTO_ROOT\" ]; then apigw_key_src=$(docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20 sh -lc \"find /hostcrypto -type f \\\\( -path '*/msp/keystore/*' -o -name '*_sk' -o -name '*-key.pem' -o -name 'key.pem' -o -name '*.key' \\\\) 2>/dev/null | head -n 1\" | sed 's#^/hostcrypto#/var/cognus/crypto#'); fi; "
        "[ -n \"$apigw_key_src\" ] || { printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_key >&2; }; "
        "apigw_key_dst=\"$APIGW_MSP_DIR/keystore/key.pem\"; "
        "if [ \"$apigw_key_src\" != \"$apigw_key_dst\" ]; then cp \"$apigw_key_src\" \"$apigw_key_dst\" >/dev/null 2>&1 || (test -r \"$apigw_key_src\" && cat \"$apigw_key_src\" > \"$apigw_key_dst\" 2>/dev/null) || (command -v sudo >/dev/null 2>&1 && sudo -n cat \"$apigw_key_src\" > \"$apigw_key_dst\" 2>/dev/null) || (command -v docker >/dev/null 2>&1 && docker image inspect alpine:3.20 >/dev/null 2>&1 && [ -d \"$APIGW_CRYPTO_ROOT\" ] && apigw_key_rel=\"${apigw_key_src#/var/cognus/crypto/}\" && [ \"$apigw_key_rel\" != \"$apigw_key_src\" ] && docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20 sh -lc \"cat /hostcrypto/$apigw_key_rel\" > \"$apigw_key_dst\") || { printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:copy_key_failed >&2; }; fi; "
        "apigw_tlsca_src=$(find \"$APIGW_MSP_DIR\" -type f -readable -path '*/tlscacerts/*' \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_tlsca_src\" ] || apigw_tlsca_src=$(find \"$APIGW_MSP_DIR\" -type f -readable -path '*/cacerts/*' \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_tlsca_src\" ] || apigw_tlsca_src=$(find \"$APIGW_ORG_CRYPTO_DIR\" -type f -readable -path '*/msp/tlscacerts/*' \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_tlsca_src\" ] || apigw_tlsca_src=$(find \"$APIGW_ORG_CRYPTO_DIR\" -type f -readable -path '*/msp/cacerts/*' \\( -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "[ -n \"$apigw_tlsca_src\" ] || apigw_tlsca_src=$(find \"$APIGW_CRYPTO_ROOT\" -type f -readable \\( -path '*/msp/tlscacerts/*' -o -path '*/msp/cacerts/*' -o -name '*tlsca*.pem' -o -name '*-ca.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); "
        "if [ -z \"$apigw_tlsca_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_tlsca_src=$(sudo -n find \"$APIGW_ORG_CRYPTO_DIR\" -type f \\( -path '*/msp/tlscacerts/*' -o -path '*/msp/cacerts/*' -o -name '*tlsca*.pem' -o -name '*-ca.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_tlsca_src\" ] && command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then apigw_tlsca_src=$(sudo -n find \"$APIGW_CRYPTO_ROOT\" -type f \\( -path '*/msp/tlscacerts/*' -o -path '*/msp/cacerts/*' -o -name '*tlsca*.pem' -o -name '*-ca.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\) 2>/dev/null | head -n 1); fi; "
        "if [ -z \"$apigw_tlsca_src\" ] && command -v docker >/dev/null 2>&1 && docker image inspect alpine:3.20 >/dev/null 2>&1 && [ -d \"$APIGW_CRYPTO_ROOT\" ]; then apigw_tlsca_src=$(docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20 sh -lc \"find /hostcrypto -type f \\\\( -path '*/msp/tlscacerts/*' -o -path '*/msp/cacerts/*' -o -name '*tlsca*.pem' -o -name '*-ca.pem' -o -name '*.pem' -o -name '*.crt' -o -name '*.cert' \\\\) 2>/dev/null | head -n 1\" | sed 's#^/hostcrypto#/var/cognus/crypto#'); fi; "
        "[ -n \"$apigw_tlsca_src\" ] || { printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_tlsca >&2; }; "
        "apigw_tlsca_dst=\"$APIGW_MSP_DIR/tlscacerts/tlsca-cert.pem\"; "
        "if [ \"$apigw_tlsca_src\" != \"$apigw_tlsca_dst\" ]; then cp \"$apigw_tlsca_src\" \"$apigw_tlsca_dst\" >/dev/null 2>&1 || (test -r \"$apigw_tlsca_src\" && cat \"$apigw_tlsca_src\" > \"$apigw_tlsca_dst\" 2>/dev/null) || (command -v sudo >/dev/null 2>&1 && sudo -n cat \"$apigw_tlsca_src\" > \"$apigw_tlsca_dst\" 2>/dev/null) || (command -v docker >/dev/null 2>&1 && docker image inspect alpine:3.20 >/dev/null 2>&1 && [ -d \"$APIGW_CRYPTO_ROOT\" ] && apigw_tlsca_rel=\"${apigw_tlsca_src#/var/cognus/crypto/}\" && [ \"$apigw_tlsca_rel\" != \"$apigw_tlsca_src\" ] && docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20 sh -lc \"cat /hostcrypto/$apigw_tlsca_rel\" > \"$apigw_tlsca_dst\") || { printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:copy_tlsca_failed >&2; }; fi; "
        "fi; "
    )

    return (
        f"mkdir -p {host_dir_q} {wallet_dir_q} {msp_tlsca_dir_q} >/dev/null 2>&1 || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:mkdir_failed >&2; }}; "
        f"APIGW_ORG_CRYPTO_DIR={org_crypto_root_q}; "
        f"APIGW_CRYPTO_ROOT={crypto_root_q}; "
        f"APIGW_MSP_DIR={msp_dir_q}; "
        f"if [ -d {msp_identity_q} ]; then cp -a {msp_identity_q}/. {msp_dir_q}/ 2>/dev/null || true; fi; "
        f"if [ -d {cfg_msp_dir_q} ] && [ -s {cfg_msp_cert_q} ] && [ -s {cfg_msp_key_q} ]; then cp -a {cfg_msp_dir_q}/. {msp_dir_q}/ 2>/dev/null || true; fi; "
        f"mkdir -p \"$APIGW_MSP_DIR/signcerts\" \"$APIGW_MSP_DIR/keystore\" \"$APIGW_MSP_DIR/tlscacerts\" >/dev/null 2>&1 || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:msp_dir_prepare_failed >&2; }}; "
        f"{bootstrap_fast_materialize_step}"
        f"{bootstrap_slow_resolution_step}"
        f"printf %s {shlex.quote(connection_b64)} | base64 -d > {connection_path_q}; "
        f"printf %s {shlex.quote(identities_b64)} | base64 -d > {identities_path_q}; "
        f"{gateway_index_write_step}"
        f"APIGW_DECLARED_MSP_ID={declared_msp_id_q}; "
        f"APIGW_EFFECTIVE_MSP_ID=\"$APIGW_DECLARED_MSP_ID\"; "
        f"if [ -f \"$APIGW_MSP_DIR/.cognus-sample-msp\" ] || [ -s \"$APIGW_MSP_DIR/signcerts/peer.pem\" ]; then APIGW_EFFECTIVE_MSP_ID=SampleOrg; fi; "
        f"if [ \"$APIGW_EFFECTIVE_MSP_ID\" = sampleorg ]; then APIGW_EFFECTIVE_MSP_ID=SampleOrg; fi; "
        f"if [ -n \"$APIGW_DECLARED_MSP_ID\" ] && [ -n \"$APIGW_EFFECTIVE_MSP_ID\" ] && [ \"$APIGW_EFFECTIVE_MSP_ID\" != \"$APIGW_DECLARED_MSP_ID\" ]; then "
        f"sed -i \"s#\\\"mspId\\\":\\\"$APIGW_DECLARED_MSP_ID\\\"#\\\"mspId\\\":\\\"$APIGW_EFFECTIVE_MSP_ID\\\"#g\" {identities_path_q} >/dev/null 2>&1 || true; "
        f"sed -i \"s#\\\"mspid\\\":\\\"$APIGW_DECLARED_MSP_ID\\\"#\\\"mspid\\\":\\\"$APIGW_EFFECTIVE_MSP_ID\\\"#g\" {connection_path_q} >/dev/null 2>&1 || true; "
        f"fi; "
        f"test -s {shlex.quote(f'{host_dir}/msp/signcerts/cert.pem')} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:final_cert_missing >&2; }}; "
        f"test -s {shlex.quote(f'{host_dir}/msp/keystore/key.pem')} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:final_key_missing >&2; }}; "
        f"test -s {shlex.quote(f'{host_dir}/msp/tlscacerts/tlsca-cert.pem')} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:final_tlsca_missing >&2; }}; "
        f"test -s {orderer_tlsca_path_q} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:orderer_tlsca_missing >&2; }}; "
        f"test -s {identities_path_q} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:empty_file >&2; }}; "
        f"grep -q '\"organizations\"' {identities_path_q} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_organizations >&2; }}; "
        f"grep -q '\"mspId\"' {identities_path_q} || {{ printf '%s\\n' COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_mspid >&2; }}; "
        f"chmod 0644 {connection_path_q} {identities_path_q} || true; "
        f"chmod 0770 {wallet_dir_q} >/dev/null 2>&1 || true; "
    )


def _resolve_apigateway_runtime_hotpatch_step(run_state, host_row, container_name):
    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type not in ("apigateway", "netapi"):
        return ""

    host_dir = _resolve_apigateway_runtime_host_dir(run_state, host_row)
    if not host_dir:
        return ""

    host_gateway_index_q = shlex.quote(f"{host_dir}/gateway-index.js")
    container_q = shlex.quote(str(container_name or "").strip())
    if not container_q:
        return ""

    return (
        f"if [ -s {host_gateway_index_q} ]; then "
        f"docker cp {host_gateway_index_q} {container_q}:/app/index.js >/dev/null 2>&1 || true; "
        f"docker exec {container_q} /bin/sh -lc 'node --check /app/index.js >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
        f"docker container restart {container_q} >/dev/null 2>&1 || true; "
        f"fi; "
    )


def _resolve_runtime_docker_publish_args(run_state, host_row):
    if not isinstance(host_row, dict):
        return ""

    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type not in RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE:
        return ""

    host_port = _resolve_runtime_exposed_host_port(run_state, host_row)
    if host_port <= 0:
        return ""

    container_port = _parse_port_value(
        RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE.get(node_type, "")
    )
    if container_port <= 0:
        return ""

    return "--publish {}:{} ".format(host_port, container_port)


def _resolve_runtime_host_port_preflight_step(run_state, host_row):
    if not isinstance(host_row, dict):
        return ""

    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    host_port = _resolve_runtime_exposed_host_port(run_state, host_row)
    container_port = _parse_port_value(
        RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE.get(node_type, "")
    )

    if host_port <= 0 or container_port <= 0:
        return ""

    if node_type not in ("apigateway", "netapi"):
        return ""

    host_port_q = shlex.quote(str(host_port))
    return (
        "if command -v lsof >/dev/null 2>&1 && lsof -i :{host_port} 2>/dev/null | grep LISTEN >/dev/null 2>&1; then "
        "docker ps --format '{{{{.ID}}}} {{{{.Ports}}}}' | grep -E '(^|, )0.0.0.0:{host_port}->|(^|, ):::{host_port}->' | awk '{{{{print $1}}}}' | xargs -r docker rm -f >/dev/null 2>&1 || true; "
        "lsof -i :{host_port} 2>/dev/null | awk 'NR>1 {{{{print $2}}}}' | xargs -r kill -9 >/dev/null 2>&1 || true; "
        "sleep 1; "
        "fi; "
    ).format(host_port=host_port_q)


def _resolve_runtime_docker_run_resource_args(node_type):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    cpu_limit = str(
        RUNBOOK_RUNTIME_CPU_LIMIT_BY_NODE_TYPE.get(normalized_node_type, "")
    ).strip()
    memory_limit = str(
        RUNBOOK_RUNTIME_MEMORY_LIMIT_BY_NODE_TYPE.get(normalized_node_type, "")
    ).strip()
    pids_limit = str(
        RUNBOOK_RUNTIME_PIDS_LIMIT_BY_NODE_TYPE.get(normalized_node_type, "")
    ).strip()

    run_args = []
    if cpu_limit:
        run_args.append("--cpus {}".format(shlex.quote(cpu_limit)))
    if memory_limit:
        run_args.append("--memory {}".format(shlex.quote(memory_limit)))
    if pids_limit:
        run_args.append("--pids-limit {}".format(shlex.quote(pids_limit)))

    # Avoid unbounded container logs filling disk and destabilizing low-resource hosts.
    run_args.extend(
        [
            "--log-driver json-file",
            "--log-opt max-size=25m",
            "--log-opt max-file=3",
        ]
    )
    return "{} ".format(" ".join(run_args)) if run_args else ""


def _resolve_runtime_docker_run_command_override_args(node_type):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    command_override = str(
        RUNBOOK_RUNTIME_COMMAND_OVERRIDE_BY_NODE_TYPE.get(normalized_node_type, "")
    ).strip()
    if not command_override:
        return ""

    builtin_bootstrap_override = False
    if normalized_node_type == "peer":
        builtin_bootstrap_override = command_override == RUNBOOK_PEER_DEV_MSP_BOOTSTRAP
    elif normalized_node_type == "orderer":
        builtin_bootstrap_override = (
            command_override == RUNBOOK_ORDERER_DEV_TLS_BOOTSTRAP
        )

    if (
        len(command_override.encode("utf-8")) > RUNBOOK_RUNTIME_COMMAND_OVERRIDE_MAX_BYTES
        and not builtin_bootstrap_override
    ):
        return ""

    try:
        command_tokens = shlex.split(command_override)
    except ValueError:
        command_tokens = [command_override]

    if len(command_tokens) == 0:
        return ""
    return " " + " ".join(shlex.quote(token) for token in command_tokens)


def _resolve_runtime_provision_stagger_step():
    if RUNBOOK_PROVISION_RUNTIME_STAGGER_SECONDS <= 0:
        return ""
    return "sleep {}; ".format(RUNBOOK_PROVISION_RUNTIME_STAGGER_SECONDS)


def _resolve_runtime_host_identity_path(
    run_state,
    host_row,
    organization=None,
    identity=None,
):
    safe_host_row = host_row if isinstance(host_row, dict) else {}
    safe_organization = (
        organization
        if isinstance(organization, dict)
        else _resolve_topology_organization_for_host(run_state, safe_host_row)
    )
    safe_identity = (
        identity
        if isinstance(identity, dict)
        else (
            safe_organization.get("identity")
            if isinstance(safe_organization.get("identity"), dict)
            else {}
        )
    )

    host_identity_path = str(
        safe_host_row.get("runtime_identity_path")
        or safe_host_row.get("identity_path")
        or safe_identity.get("cryptoPath")
        or safe_identity.get("crypto_path")
        or safe_identity.get("msp_path")
        or ""
    ).strip()
    if host_identity_path:
        return host_identity_path

    org_id_guess = str(safe_host_row.get("org_id") or "").strip().lower()
    node_id_guess = str(safe_host_row.get("node_id") or "").strip()
    if org_id_guess and node_id_guess:
        return "{}/{}/{}/msp".format(
            RUNBOOK_RUNTIME_HOST_CRYPTO_ROOT.rstrip("/"),
            org_id_guess,
            node_id_guess,
        )
    return ""


def _resolve_runtime_peer_fallback_msp_seed_step(host_row):
    safe_host_row = host_row if isinstance(host_row, dict) else {}
    if _normalize_runtime_component_node_type(safe_host_row.get("node_type", "")) != "peer":
        return ""

    msp_root = _resolve_runtime_host_identity_path({}, safe_host_row)
    if not msp_root:
        return ""
    signcert_path = shlex.quote(f"{msp_root}/signcerts/cert.pem")
    peer_signcert_path = shlex.quote(f"{msp_root}/signcerts/peer.pem")
    cacert_path = shlex.quote(f"{msp_root}/cacerts/cacert.pem")
    tlscacert_path = shlex.quote(f"{msp_root}/tlscacerts/tlsca.pem")
    key_path = shlex.quote(f"{msp_root}/keystore/key.pem")
    admincert_path = shlex.quote(f"{msp_root}/admincerts/admincert.pem")
    config_path = shlex.quote(f"{msp_root}/config.yaml")
    msp_root_q = shlex.quote(msp_root)
    cert_b64_q = shlex.quote(RUNBOOK_ORDERER_DEV_TLS_CERT_B64)
    key_b64_q = shlex.quote(RUNBOOK_ORDERER_DEV_TLS_KEY_B64)
    ca_b64_q = shlex.quote(RUNBOOK_ORDERER_DEV_TLS_CA_B64)
    config_b64_q = shlex.quote(RUNBOOK_ORDERER_DEV_MSP_CONFIG_B64)

    return (
        "if [ ! -s {signcert_path} ] && [ ! -s {peer_signcert_path} ]; then "
        "mkdir -p {msp_root_q} {msp_root_q}/cacerts {msp_root_q}/signcerts {msp_root_q}/keystore {msp_root_q}/admincerts {msp_root_q}/tlscacerts >/dev/null 2>&1 || true; "
        "printf %s {ca_b64_q} | base64 -d > {cacert_path} 2>/dev/null || true; "
        "printf %s {ca_b64_q} | base64 -d > {tlscacert_path} 2>/dev/null || true; "
        "printf %s {cert_b64_q} | base64 -d > {signcert_path} 2>/dev/null || true; "
        "cp {signcert_path} {peer_signcert_path} >/dev/null 2>&1 || true; "
        "printf %s {key_b64_q} | base64 -d > {key_path} 2>/dev/null || true; "
        "printf %s {config_b64_q} | base64 -d > {config_path} 2>/dev/null || true; "
        "cp {signcert_path} {admincert_path} >/dev/null 2>&1 || true; "
        "touch {msp_root_q}/.cognus-sample-msp >/dev/null 2>&1 || true; "
        "chmod 600 {key_path} >/dev/null 2>&1 || true; "
        "chmod 644 {signcert_path} {peer_signcert_path} {cacert_path} {tlscacert_path} {admincert_path} {config_path} >/dev/null 2>&1 || true; "
        "fi; "
    ).format(
        signcert_path=signcert_path,
        peer_signcert_path=peer_signcert_path,
        cacert_path=cacert_path,
        tlscacert_path=tlscacert_path,
        key_path=key_path,
        admincert_path=admincert_path,
        config_path=config_path,
        msp_root_q=msp_root_q,
        cert_b64_q=cert_b64_q,
        key_b64_q=key_b64_q,
        ca_b64_q=ca_b64_q,
        config_b64_q=config_b64_q,
    )


def _resolve_zero_provision_cleanup_step(run_state, host_row):
    if not RUNBOOK_ZERO_PROVISION_CLEAN_ENABLED:
        return ""
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return ""

    run_id = _sanitize_container_token(
        str(run_state.get("run_id", "") or "").strip(),
        "run",
    )
    host_scope = _sanitize_container_token(
        str(
            host_row.get("host_ref")
            or host_row.get("host_address")
            or host_row.get("node_id")
            or ""
        ).strip(),
        "host",
    )

    marker_dir = "${{HOME:-/tmp}}/.cognus/{}/zero-provision/{}".format(
        run_id,
        host_scope,
    )
    network_q = shlex.quote(RUNBOOK_BASE_NETWORK_NAME)

    return (
        "COGNUS_ZERO_MARKER_DIR=\"{marker_dir}\"; "
        "COGNUS_ZERO_MARKER=\"$COGNUS_ZERO_MARKER_DIR/clean.done\"; "
        "if [ ! -f \"$COGNUS_ZERO_MARKER\" ]; then "
        "mkdir -p \"$COGNUS_ZERO_MARKER_DIR\" >/dev/null 2>&1 || true; "
        "for COGNUS_NODE_TYPE in peer orderer ca couch apigateway netapi chaincode; do "
        "docker ps -aq --filter \"label=cognus.node_type=$COGNUS_NODE_TYPE\" | xargs -r docker rm -f >/dev/null 2>&1 || true; "
        "done; "
        "docker ps -aq --format '{{{{.ID}}}} {{{{.Names}}}}' | grep -E 'orderer|peer|fabric-ca|couch|apigateway|netapi|chaincode|cognusrb' | awk '{{{{print $1}}}}' | xargs -r docker rm -f >/dev/null 2>&1 || true; "
        "docker network rm {network} >/dev/null 2>&1 || true; "
        "docker volume ls -q | grep -E 'cognus|fabric|cello' | xargs -r docker volume rm -f >/dev/null 2>&1 || true; "
        "for COGNUS_RM_PATH in /var/hyperledger/production /var/cognus /var/lib/cognus /tmp/cognus; do "
        "sudo -n rm -rf \"$COGNUS_RM_PATH\" >/dev/null 2>&1 || rm -rf \"$COGNUS_RM_PATH\" >/dev/null 2>&1 || true; "
        "done; "
        "find /tmp -maxdepth 1 -type d -name 'cognus-run-*' -exec rm -rf {{{{}}}} + >/dev/null 2>&1 || true; "
        "date -u +%Y-%m-%dT%H:%M:%SZ >\"$COGNUS_ZERO_MARKER\" 2>/dev/null || touch \"$COGNUS_ZERO_MARKER\"; "
        "fi; "
    ).format(
        marker_dir=marker_dir,
        network=network_q,
    )


def _resolve_runtime_memory_guard_step(node_type):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    required_memory_mb_raw = str(
        RUNBOOK_RUNTIME_MIN_AVAILABLE_MEMORY_MB_BY_NODE_TYPE.get(
            normalized_node_type, ""
        )
    ).strip()
    try:
        required_memory_mb = int(required_memory_mb_raw or 0)
    except (TypeError, ValueError):
        required_memory_mb = 0
    if required_memory_mb <= 0:
        return ""

    required_memory_kb = required_memory_mb * 1024
    marker_prefix = shlex.quote(
        "COGNUS_RUNTIME_MEMORY_LOW:{}:{}:".format(
            normalized_node_type or "node",
            required_memory_mb,
        )
    )
    return (
        "mem_avail_kb=$(awk '/MemAvailable:/ {{print $2}}' /proc/meminfo 2>/dev/null || echo 0); "
        "if [ \"$mem_avail_kb\" -lt {required_memory_kb} ]; then "
        "printf '%s%s\\n' {marker_prefix} \"$mem_avail_kb\" >&2; "
        "exit 137; "
        "fi; "
    ).format(
        required_memory_kb=required_memory_kb,
        marker_prefix=marker_prefix,
    )


def _resolve_runtime_state_guard_step(container):
    wait_step = ""
    if RUNBOOK_RUNTIME_STABILITY_WINDOW_SECONDS > 0:
        wait_step = "sleep {}; ".format(RUNBOOK_RUNTIME_STABILITY_WINDOW_SECONDS)
    return (
        "runtime_status=$(docker inspect -f '{{{{.State.Status}}}}' {container} 2>/dev/null || echo unknown); "
        "if [ \"$runtime_status\" != running ]; then "
        "printf '%s%s\\n' 'COGNUS_RUNTIME_STATUS_INVALID:' \"$runtime_status\" >&2; "
        "exit 125; "
        "fi; "
        "restart_count_before=$(docker inspect -f '{{{{.RestartCount}}}}' {container} 2>/dev/null || echo 0); "
        "{wait_step}"
        "runtime_status_after=$(docker inspect -f '{{{{.State.Status}}}}' {container} 2>/dev/null || echo unknown); "
        "restart_count_after=$(docker inspect -f '{{{{.RestartCount}}}}' {container} 2>/dev/null || echo 0); "
        "if [ \"$runtime_status_after\" != running ]; then "
        "printf '%s%s\\n' 'COGNUS_RUNTIME_STATUS_INVALID:' \"$runtime_status_after\" >&2; "
        "exit 125; "
        "fi; "
        "if [ \"$restart_count_before\" != \"$restart_count_after\" ]; then "
        "printf '%s%s:%s\\n' 'COGNUS_RUNTIME_RESTART_LOOP:' \"$restart_count_before\" \"$restart_count_after\" >&2; "
        "exit 125; "
        "fi; "
    ).format(container=container, wait_step=wait_step)


def _resolve_runtime_topology_guard_step(run_state, host_row):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return ""

    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type not in ("apigateway", "netapi"):
        return ""

    host_mapping = run_state.get("host_mapping", [])
    if not isinstance(host_mapping, list) or not host_mapping:
        return ""

    current_host_ref = str(host_row.get("host_ref", "") or "").strip().lower()
    current_host_address = str(host_row.get("host_address", "") or "").strip().lower()
    current_org = str(host_row.get("org_id", "") or "").strip().lower()

    enabled_node_types = set(
        _normalize_runtime_component_node_type(item)
        for item in (run_state.get("enabled_runtime_node_types") or [])
        if str(item or "").strip()
    )
    if not enabled_node_types:
        enabled_node_types = {
            "peer",
            "orderer",
            "ca",
            "couch",
            "apigateway",
            "netapi",
        }

    expected_rows = []
    seen_node_ids = set()
    for candidate in host_mapping:
        if not isinstance(candidate, dict):
            continue

        candidate_type = _normalize_runtime_component_node_type(candidate.get("node_type", ""))
        if not candidate_type or candidate_type == "chaincode":
            continue
        if candidate_type not in enabled_node_types:
            continue

        candidate_org = str(candidate.get("org_id", "") or "").strip().lower()
        if current_org and candidate_org and candidate_org != current_org:
            continue

        candidate_host_ref = str(candidate.get("host_ref", "") or "").strip().lower()
        candidate_host_address = str(candidate.get("host_address", "") or "").strip().lower()
        same_host = False
        if current_host_ref and candidate_host_ref and candidate_host_ref == current_host_ref:
            same_host = True
        elif (
            current_host_address
            and candidate_host_address
            and candidate_host_address == current_host_address
        ):
            same_host = True
        elif not current_host_ref and not current_host_address:
            same_host = True
        if not same_host:
            continue

        candidate_node_id = str(candidate.get("node_id", "") or "").strip()
        candidate_key = candidate_node_id or "{}:{}".format(
            candidate_type,
            str(candidate.get("host_ref", "") or "").strip(),
        )
        if candidate_key in seen_node_ids:
            continue
        seen_node_ids.add(candidate_key)
        expected_rows.append(candidate)

    if not expected_rows:
        return ""

    run_id_q = shlex.quote(str(run_state.get("run_id", "") or "").strip())
    guard_steps = [
        "COGNUS_TOPOLOGY_GUARD_RUN_ID={run_id}; ".format(run_id=run_id_q),
    ]
    for index, candidate in enumerate(expected_rows):
        candidate_type = _normalize_runtime_component_node_type(candidate.get("node_type", ""))
        candidate_node_id = str(candidate.get("node_id", "") or "").strip()
        if not candidate_type or not candidate_node_id:
            continue

        expected_container_name = _resolve_runbook_container_name(run_state, candidate)
        guard_steps.append(
            (
                "COGNUS_EXPECTED_CONTAINER_{idx}={container}; "
                "if [ -z \"$COGNUS_EXPECTED_CONTAINER_{idx}\" ] || ! docker inspect \"$COGNUS_EXPECTED_CONTAINER_{idx}\" >/dev/null 2>&1; then "
                "COGNUS_EXPECTED_CONTAINER_{idx}=$(docker ps -a --filter label=cognus.run_id=$COGNUS_TOPOLOGY_GUARD_RUN_ID --filter label=cognus.node_id={node_id} --format '{{{{.Names}}}}' | head -n 1); "
                "fi; "
                "if [ -z \"$COGNUS_EXPECTED_CONTAINER_{idx}\" ] || ! docker inspect \"$COGNUS_EXPECTED_CONTAINER_{idx}\" >/dev/null 2>&1; then "
                "printf '%s\\n' COGNUS_RUNTIME_TOPOLOGY_MISSING:{node_type}:{node_id_plain} >&2; "
                "exit 125; "
                "fi; "
                "if ! docker ps --format '{{{{.Names}}}}' | grep -Fx \"$COGNUS_EXPECTED_CONTAINER_{idx}\" >/dev/null 2>&1; then "
                "docker start \"$COGNUS_EXPECTED_CONTAINER_{idx}\" >/dev/null 2>&1 || true; "
                "fi; "
                "COGNUS_EXPECTED_STATUS_{idx}=$(docker inspect -f '{{{{.State.Status}}}}' \"$COGNUS_EXPECTED_CONTAINER_{idx}\" 2>/dev/null || echo unknown); "
                "if [ \"$COGNUS_EXPECTED_STATUS_{idx}\" != running ]; then "
                "printf '%s\\n' COGNUS_RUNTIME_TOPOLOGY_NOT_RUNNING:{node_type}:{node_id_plain}:$COGNUS_EXPECTED_STATUS_{idx} >&2; "
                "exit 125; "
                "fi; "
            ).format(
                idx=index,
                container=shlex.quote(expected_container_name),
                node_id=shlex.quote(candidate_node_id),
                node_id_plain=candidate_node_id,
                node_type=candidate_type,
            )
        )

    return "".join(guard_steps)


def _resolve_runtime_chaincode_commit_guard_step(
    run_state, host_row, enforce_failures=True
):
    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type not in ("apigateway", "netapi"):
        return ""

    scope = _collect_gateway_channel_chaincode_pairs(run_state, host_row)
    channel_ids = [channel_id for channel_id in scope.get("channels", []) if channel_id]
    scoped_pairs = [
        {
            "channel_id": str(pair.get("channel_id", "") or "").strip().lower(),
            "chaincode_id": str(pair.get("chaincode_id", "") or "").strip().lower(),
        }
        for pair in scope.get("pairs", [])
        if isinstance(pair, dict)
        and str(pair.get("channel_id", "") or "").strip()
        and str(pair.get("chaincode_id", "") or "").strip()
    ]
    organization = scope.get("organization", {})
    if not channel_ids or not scoped_pairs:
        return ""
    primary_channel_id = str(channel_ids[0] or "").strip().lower()
    pairs_by_channel = {}
    for scoped_pair in scoped_pairs:
        channel_pairs = pairs_by_channel.setdefault(
            str(scoped_pair.get("channel_id", "") or "").strip().lower(),
            [],
        )
        chaincode_id = str(scoped_pair.get("chaincode_id", "") or "").strip().lower()
        if chaincode_id and chaincode_id not in channel_pairs:
            channel_pairs.append(chaincode_id)

    peer_container = _resolve_runtime_container_name_for_node_type(
        run_state,
        host_row,
        "peer",
    )
    orderer_container = _resolve_runtime_container_name_for_node_type(
        run_state,
        host_row,
        "orderer",
    )
    peer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "peer")
    orderer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "orderer")

    resolve_peer_step = (
        "COGNUS_RUN_ID={run_id}; "
        "COGNUS_PEER_CONTAINER={peer}; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ] || ! docker inspect \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --filter label=cognus.node_id={peer_node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then "
        "COGNUS_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then "
        "COGNUS_PEER_CONTAINER=$(docker ps --format '{{{{.Names}}}} {{{{.Image}}}}' | awk '/fabric-peer/ {{{{print $1; exit}}}}'); "
        "fi; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then printf '%s\\n' COGNUS_CHAINCODE_LIFECYCLE_INVALID:missing_peer >&2; exit 126; fi; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"mkdir -p /tmp/cognus >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
        "COGNUS_PEER_LOCAL_MSPID=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"echo ${{CORE_PEER_LOCALMSPID:-Org1MSP}}\" 2>/dev/null | head -n 1); "
        "COGNUS_PEER_CORE_LOCAL_MSPID=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"awk -F': ' '/^[[:space:]]*localMspId:/ {{print \\$2; exit}}' /etc/hyperledger/fabric/core.yaml 2>/dev/null | tr -d '[:space:]'\" 2>/dev/null | head -n 1); "
        "COGNUS_PEER_SAMPLE_MSP=0; "
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -f /etc/hyperledger/fabric/msp/.cognus-sample-msp || test -f /var/lib/cognus/msp/.cognus-sample-msp || test -s /etc/hyperledger/fabric/msp/signcerts/peer.pem || test -s /var/lib/cognus/msp/signcerts/peer.pem' >/dev/null 2>&1; then COGNUS_PEER_SAMPLE_MSP=1; fi; "
        "COGNUS_ORG_MSP_ID={org_msp_id}; "
        "if [ -n \"$COGNUS_PEER_CORE_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=$COGNUS_PEER_CORE_LOCAL_MSPID; COGNUS_MSP_FALLBACK_REASON=core_yaml_local_mspid; fi; "
        "if [ \"$COGNUS_PEER_SAMPLE_MSP\" = \"1\" ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; COGNUS_MSP_FALLBACK_REASON=sample_msp_detected; fi; "
        "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ] && [ -n \"$COGNUS_ORG_MSP_ID\" ]; then COGNUS_PEER_LOCAL_MSPID=$COGNUS_ORG_MSP_ID; COGNUS_MSP_FALLBACK_REASON=org_msp_fallback; fi; "
        "if [ \"$COGNUS_PEER_LOCAL_MSPID\" = sampleorg ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; fi; "
        "if [ \"$COGNUS_PEER_SAMPLE_MSP\" != \"1\" ] && [ \"$COGNUS_PEER_LOCAL_MSPID\" = SampleOrg ] && [ -n \"$COGNUS_ORG_MSP_ID\" ] && [ \"$COGNUS_ORG_MSP_ID\" != SampleOrg ]; then COGNUS_PEER_LOCAL_MSPID=$COGNUS_ORG_MSP_ID; COGNUS_MSP_FALLBACK_REASON=org_msp_preferred_over_sampleorg; fi; "
        "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=Org1MSP; fi; "
        "COGNUS_PEER_ADMIN_MSPCONFIGPATH=/etc/hyperledger/fabric/msp; "
        "COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"find /etc/hyperledger/fabric -maxdepth 12 -type d -path '*/users/*/msp' 2>/dev/null | grep -E '/users/[Aa]dmin(@[^/]*)?/msp$' | head -n 1\" 2>/dev/null | head -n 1); "
        "if [ -z \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED\" ]; then COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"find /etc/hyperledger/fabric -maxdepth 12 -type d -path '*/users/*Admin@*/msp' 2>/dev/null | head -n 1\" 2>/dev/null | head -n 1); fi; "
        "if [ -n \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED\" ] && docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc '(test -s \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED/signcerts/cert.pem\" || test -s \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED/signcerts/admin.pem\") && find \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED/keystore\" -type f | head -n 1 >/dev/null 2>&1' >/dev/null 2>&1; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH_DISCOVERED; COGNUS_MSP_FALLBACK_REASON=peer_container_admin_msp_discovered; fi; "
        "if [ -z \"$COGNUS_MSP_FALLBACK_REASON\" ]; then COGNUS_MSP_FALLBACK_REASON=default_peer_local_msp; fi; "
        "COGNUS_ADMIN_MSP_HOST=''; "
        "for COGNUS_ADMIN_ROOT in /var/cognus/crypto \"${{HOME}}/.cognus\" /tmp/cognus \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain\"; do "
        "[ -d \"$COGNUS_ADMIN_ROOT\" ] || continue; "
        "COGNUS_ADMIN_MSP_HOST=$(find \"$COGNUS_ADMIN_ROOT\" -maxdepth 14 -type d -path '*/users/*/msp' 2>/dev/null | grep -E '/users/[Aa]dmin(@[^/]*)?/msp$' | head -n 1); "
        "if [ -z \"$COGNUS_ADMIN_MSP_HOST\" ]; then COGNUS_ADMIN_MSP_HOST=$(find \"$COGNUS_ADMIN_ROOT\" -maxdepth 14 -type d -path '*/users/*Admin@*/msp' 2>/dev/null | head -n 1); fi; "
        "if [ -n \"$COGNUS_ADMIN_MSP_HOST\" ]; then break; fi; "
        "done; "
        "if [ -n \"$COGNUS_ADMIN_MSP_HOST\" ]; then "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'rm -rf /tmp/cognus/admin-msp >/dev/null 2>&1 || true; mkdir -p /tmp/cognus/admin-msp >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
        "docker cp \"$COGNUS_ADMIN_MSP_HOST/.\" \"$COGNUS_PEER_CONTAINER:/tmp/cognus/admin-msp/\" >/dev/null 2>&1 || true; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'if [ ! -s /tmp/cognus/admin-msp/signcerts/cert.pem ] && [ -s /tmp/cognus/admin-msp/signcerts/admin.pem ]; then cp /tmp/cognus/admin-msp/signcerts/admin.pem /tmp/cognus/admin-msp/signcerts/cert.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/admin-msp/keystore/key.pem ]; then k=$(find /tmp/cognus/admin-msp/keystore -type f | head -n 1); if [ -n \"$k\" ]; then cp \"$k\" /tmp/cognus/admin-msp/keystore/key.pem >/dev/null 2>&1 || true; fi; fi; if [ ! -s /tmp/cognus/admin-msp/cacerts/cacert.pem ]; then for c in /tmp/cognus/admin-msp/cacerts/* /tmp/cognus/admin-msp/tlscacerts/* /etc/hyperledger/fabric/msp/cacerts/cacert.pem /etc/hyperledger/fabric/msp/tlscacerts/tlsroot.pem; do [ -s \"$c\" ] || continue; cp \"$c\" /tmp/cognus/admin-msp/cacerts/cacert.pem >/dev/null 2>&1 || true; break; done; fi; if [ ! -s /tmp/cognus/admin-msp/config.yaml ] && [ -s /etc/hyperledger/fabric/msp/config.yaml ]; then cp /etc/hyperledger/fabric/msp/config.yaml /tmp/cognus/admin-msp/config.yaml >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/admin-msp/admincerts/admincert.pem ] && [ -s /tmp/cognus/admin-msp/signcerts/cert.pem ]; then cp /tmp/cognus/admin-msp/signcerts/cert.pem /tmp/cognus/admin-msp/admincerts/admincert.pem >/dev/null 2>&1 || true; fi' >/dev/null 2>&1 || true; "
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc '(test -s /tmp/cognus/admin-msp/signcerts/cert.pem || test -s /tmp/cognus/admin-msp/signcerts/admin.pem) && find /tmp/cognus/admin-msp/keystore -type f | head -n 1 >/dev/null 2>&1' >/dev/null 2>&1; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=/tmp/cognus/admin-msp; COGNUS_MSP_FALLBACK_REASON=host_admin_msp_materialized; fi; "
        "fi; "
        "if [ \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH\" = /etc/hyperledger/fabric/msp ]; then "
        "COGNUS_CFG_MSP_HOST={cfg_msp_host}; "
        "if [ ! -d \"$COGNUS_CFG_MSP_HOST\" ]; then COGNUS_CFG_MSP_HOST=$(find /tmp -maxdepth 6 -type d -path \"/tmp/cognus-run-${{COGNUS_RUN_ID}}-*/cfg-gen/msp\" 2>/dev/null | head -n 1); fi; "
        "if [ -n \"$COGNUS_CFG_MSP_HOST\" ]; then "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'rm -rf /tmp/cognus/admin-msp >/dev/null 2>&1 || true; mkdir -p /tmp/cognus/admin-msp >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
        "docker cp \"$COGNUS_CFG_MSP_HOST/.\" \"$COGNUS_PEER_CONTAINER:/tmp/cognus/admin-msp/\" >/dev/null 2>&1 || true; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'if [ ! -s /tmp/cognus/admin-msp/signcerts/cert.pem ] && [ -s /tmp/cognus/admin-msp/signcerts/peer.pem ]; then cp /tmp/cognus/admin-msp/signcerts/peer.pem /tmp/cognus/admin-msp/signcerts/cert.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/admin-msp/keystore/key.pem ]; then k=$(find /tmp/cognus/admin-msp/keystore -type f | head -n 1); if [ -n \"$k\" ]; then cp \"$k\" /tmp/cognus/admin-msp/keystore/key.pem >/dev/null 2>&1 || true; fi; fi; if [ ! -s /tmp/cognus/admin-msp/admincerts/admincert.pem ] && [ -s /tmp/cognus/admin-msp/signcerts/cert.pem ]; then cp /tmp/cognus/admin-msp/signcerts/cert.pem /tmp/cognus/admin-msp/admincerts/admincert.pem >/dev/null 2>&1 || true; fi' >/dev/null 2>&1 || true; "
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -s /tmp/cognus/admin-msp/signcerts/cert.pem && test -s /tmp/cognus/admin-msp/keystore/key.pem' >/dev/null 2>&1; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=/tmp/cognus/admin-msp; COGNUS_MSP_FALLBACK_REASON=channel_cfg_msp_materialized; fi; "
        "fi; "
        "fi; "
        "if [ \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH\" = /etc/hyperledger/fabric/msp ]; then "
        "COGNUS_CA_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=ca --format '{{{{.Names}}}}' | head -n 1); "
        "if [ -n \"$COGNUS_CA_CONTAINER\" ]; then "
        "COGNUS_CA_ADMIN_STAGE=/tmp/cognus-run-${{COGNUS_RUN_ID}}-ca-admin-msp; "
        "rm -rf \"$COGNUS_CA_ADMIN_STAGE\" >/dev/null 2>&1 || true; mkdir -p \"$COGNUS_CA_ADMIN_STAGE\" >/dev/null 2>&1 || true; "
        "docker exec \"$COGNUS_CA_CONTAINER\" sh -lc 'rm -rf /tmp/cognus/runbook-admin-home /tmp/cognus/runbook-admin-msp >/dev/null 2>&1 || true; mkdir -p /tmp/cognus/runbook-admin-home >/dev/null 2>&1 || true; export FABRIC_CA_CLIENT_HOME=/tmp/cognus/runbook-admin-home; fabric-ca-client enroll -u http://admin:adminpw@127.0.0.1:7054 >/tmp/cognus/runbook-admin-enroll-root.log 2>&1 || true; fabric-ca-client register --id.name runbookadmin --id.secret runbookadminpw --id.type admin >/tmp/cognus/runbook-admin-register.log 2>&1 || true; fabric-ca-client enroll -u http://runbookadmin:runbookadminpw@127.0.0.1:7054 -M /tmp/cognus/runbook-admin-msp >/tmp/cognus/runbook-admin-enroll.log 2>&1 || true' >/dev/null 2>&1 || true; "
        "docker cp \"$COGNUS_CA_CONTAINER:/tmp/cognus/runbook-admin-msp/.\" \"$COGNUS_CA_ADMIN_STAGE/\" >/dev/null 2>&1 || true; "
        "if [ -s \"$COGNUS_CA_ADMIN_STAGE/signcerts/cert.pem\" ] && find \"$COGNUS_CA_ADMIN_STAGE/keystore\" -type f | head -n 1 >/dev/null 2>&1; then "
        "mkdir -p \"$COGNUS_CA_ADMIN_STAGE/admincerts\" \"$COGNUS_CA_ADMIN_STAGE/cacerts\" >/dev/null 2>&1 || true; "
        "if [ ! -s \"$COGNUS_CA_ADMIN_STAGE/admincerts/admincert.pem\" ]; then cp \"$COGNUS_CA_ADMIN_STAGE/signcerts/cert.pem\" \"$COGNUS_CA_ADMIN_STAGE/admincerts/admincert.pem\" >/dev/null 2>&1 || true; fi; "
        "if [ ! -s \"$COGNUS_CA_ADMIN_STAGE/cacerts/cacert.pem\" ]; then c=$(find \"$COGNUS_CA_ADMIN_STAGE/cacerts\" -type f | head -n 1); [ -n \"$c\" ] && cp \"$c\" \"$COGNUS_CA_ADMIN_STAGE/cacerts/cacert.pem\" >/dev/null 2>&1 || true; fi; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'rm -rf /tmp/cognus/admin-msp >/dev/null 2>&1 || true; mkdir -p /tmp/cognus/admin-msp >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
        "docker cp \"$COGNUS_CA_ADMIN_STAGE/.\" \"$COGNUS_PEER_CONTAINER:/tmp/cognus/admin-msp/\" >/dev/null 2>&1 || true; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'mkdir -p /tmp/cognus/admin-msp/admincerts /tmp/cognus/admin-msp/cacerts >/dev/null 2>&1 || true; if [ ! -s /tmp/cognus/admin-msp/admincerts/admincert.pem ] && [ -s /tmp/cognus/admin-msp/signcerts/cert.pem ]; then cp /tmp/cognus/admin-msp/signcerts/cert.pem /tmp/cognus/admin-msp/admincerts/admincert.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/admin-msp/config.yaml ] && [ -s /etc/hyperledger/fabric/msp/config.yaml ]; then cp /etc/hyperledger/fabric/msp/config.yaml /tmp/cognus/admin-msp/config.yaml >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/admin-msp/cacerts/cacert.pem ]; then for c in /tmp/cognus/admin-msp/cacerts/* /etc/hyperledger/fabric/msp/cacerts/cacert.pem; do [ -s \"$c\" ] || continue; cp \"$c\" /tmp/cognus/admin-msp/cacerts/cacert.pem >/dev/null 2>&1 || true; break; done; fi' >/dev/null 2>&1 || true; "
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -s /tmp/cognus/admin-msp/signcerts/cert.pem && find /tmp/cognus/admin-msp/keystore -type f | head -n 1 >/dev/null 2>&1' >/dev/null 2>&1; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=/tmp/cognus/admin-msp; COGNUS_MSP_FALLBACK_REASON=ca_enrolled_admin_msp_materialized; fi; "
        "fi; "
        "fi; "
        "fi; "
        "if [ \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH\" != /etc/hyperledger/fabric/msp ]; then "
        "COGNUS_ADMIN_VALIDATE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode queryinstalled 2>&1 || true); "
        "COGNUS_ADMIN_CHANNEL_VALIDATE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer channel list 2>&1 || true); "
        "if printf '%s\\n%s\\n' \"$COGNUS_ADMIN_VALIDATE_OUT\" \"$COGNUS_ADMIN_CHANNEL_VALIDATE_OUT\" | grep -Eqi 'MSP is not defined|principal deserialization failure|expected MSP ID|no such file|cannot find|certificate signed by unknown authority|creator org unknown'; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=/etc/hyperledger/fabric/msp; COGNUS_MSP_FALLBACK_REASON=host_admin_msp_rejected; fi; "
        "fi; "
        "if [ \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH\" = /etc/hyperledger/fabric/msp ]; then "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'if [ ! -s /etc/hyperledger/fabric/msp/signcerts/cert.pem ]; then s=$(find /etc/hyperledger/fabric/msp/signcerts -type f 2>/dev/null | head -n 1); [ -n \"$s\" ] && cp \"$s\" /etc/hyperledger/fabric/msp/signcerts/cert.pem >/dev/null 2>&1 || true; fi; if [ ! -s /etc/hyperledger/fabric/msp/admincerts/admincert.pem ] && [ -s /etc/hyperledger/fabric/msp/signcerts/cert.pem ]; then cp /etc/hyperledger/fabric/msp/signcerts/cert.pem /etc/hyperledger/fabric/msp/admincerts/admincert.pem >/dev/null 2>&1 || true; fi' >/dev/null 2>&1 || true; "
        "fi; "
        "COGNUS_PEER_LOCAL_MSPID=$(printf '%s' \"$COGNUS_PEER_LOCAL_MSPID\" | tr -d '[:space:]'); "
        "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=${{COGNUS_ORG_MSP_ID:-Org1MSP}}; fi; "
        "if [ \"$COGNUS_PEER_LOCAL_MSPID\" = sampleorg ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; fi; "
        "COGNUS_PEER_CMD_PREFIX=\"CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID\"; "
        "COGNUS_ALT_LOCAL_MSPID=$COGNUS_PEER_LOCAL_MSPID; "
        "if [ \"$COGNUS_PEER_SAMPLE_MSP\" = \"1\" ]; then "
        "COGNUS_ALT_LOCAL_MSPID=$COGNUS_PEER_LOCAL_MSPID; "
        "else "
        "if [ \"$COGNUS_ALT_LOCAL_MSPID\" = SampleOrg ]; then COGNUS_ALT_LOCAL_MSPID=Org1MSP; else COGNUS_ALT_LOCAL_MSPID=SampleOrg; fi; "
        "fi; "
        "COGNUS_PEER_CMD_PREFIX_ALT=\"CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_ALT_LOCAL_MSPID\"; "
        "COGNUS_PRIMARY_CHANNEL_VALIDATE=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel list 2>&1 || true\" 2>/dev/null || true); "
        "if printf '%s\\n' \"$COGNUS_PRIMARY_CHANNEL_VALIDATE\" | grep -Eqi 'creator org unknown|expected MSP ID|principal deserialization failure|MSP is not defined'; then "
        "COGNUS_ALT_CHANNEL_VALIDATE=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel list 2>&1 || true\" 2>/dev/null || true); "
        "if ! printf '%s\\n' \"$COGNUS_ALT_CHANNEL_VALIDATE\" | grep -Eqi 'creator org unknown|expected MSP ID|principal deserialization failure|MSP is not defined'; then "
        "COGNUS_PREV_LOCAL_MSPID=$COGNUS_PEER_LOCAL_MSPID; "
        "COGNUS_PEER_LOCAL_MSPID=$COGNUS_ALT_LOCAL_MSPID; "
        "COGNUS_ALT_LOCAL_MSPID=$COGNUS_PREV_LOCAL_MSPID; "
        "COGNUS_PEER_CMD_PREFIX=\"CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID\"; "
        "COGNUS_PEER_CMD_PREFIX_ALT=\"CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_ALT_LOCAL_MSPID\"; "
        "COGNUS_MSP_FALLBACK_REASON=alt_local_msp_promoted; "
        "fi; "
        "fi; "
    ).format(
        peer=shlex.quote(peer_container),
        run_id=shlex.quote(str(run_state.get("run_id", "") or "").strip()),
        org_msp_id=shlex.quote(str(organization.get("msp_id", "") or "").strip()),
        cfg_msp_host=shlex.quote(
            f"/tmp/cognus-run-{str(run_state.get('run_id', '') or '').strip()}-{primary_channel_id}/cfg-gen/msp"
        ),
        peer_node_id=shlex.quote(str(peer_host_row.get("node_id", "") or "").strip()),
    )

    orderer_tls_expected = _resolve_runtime_node_tls_expected(run_state, host_row, "orderer")

    resolve_orderer_step = (
        "COGNUS_ORDERER_TLS_ENABLED={orderer_tls_enabled}; "
        "COGNUS_ORDERER_CONTAINER={orderer}; "
        "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ] || ! docker inspect \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=orderer --filter label=cognus.node_id={orderer_node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ]; then "
        "COGNUS_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=orderer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ]; then "
        "COGNUS_ORDERER_CONTAINER=$(docker ps --format '{{{{.Names}}}} {{{{.Image}}}}' | awk '/fabric-orderer/ {{{{print $1; exit}}}}'); "
        "fi; "
        "COGNUS_ORDERER_ENDPOINT=$COGNUS_ORDERER_CONTAINER:7050; "
        "COGNUS_ORDERER_ENDPOINT_ALT=''; "
        "COGNUS_ORDERER_LOCAL_MSPID=''; "
        "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
        "COGNUS_ORDERER_LOCAL_MSPID=$(docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc 'printf %s \"${{ORDERER_GENERAL_LOCALMSPID:-}}\"' 2>/dev/null | tr -d '\\r\\n\\t ' || true); "
        "COGNUS_ORDERER_IP=$(docker inspect --format '{{{{range .NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}' \"$COGNUS_ORDERER_CONTAINER\" 2>/dev/null | head -n 1); "
        "if [ -n \"$COGNUS_ORDERER_IP\" ]; then COGNUS_ORDERER_ENDPOINT_ALT=$COGNUS_ORDERER_IP:7050; fi; "
        "COGNUS_ORDERER_MSP_MIRROR=/tmp/cognus/orderer-msp; "
        "COGNUS_ORDERER_MSP_STAGE=/tmp/cognus-run-${{COGNUS_RUN_ID}}-orderer-msp; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'rm -rf /tmp/cognus/orderer-msp >/dev/null 2>&1 || true; mkdir -p /tmp/cognus/orderer-msp >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
        "rm -rf \"$COGNUS_ORDERER_MSP_STAGE\" >/dev/null 2>&1 || true; mkdir -p \"$COGNUS_ORDERER_MSP_STAGE\" >/dev/null 2>&1 || true; "
        "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/msp/.\" \"$COGNUS_ORDERER_MSP_STAGE/\" >/dev/null 2>&1 || true; "
        "docker cp \"$COGNUS_ORDERER_MSP_STAGE/.\" \"$COGNUS_PEER_CONTAINER:$COGNUS_ORDERER_MSP_MIRROR/\" >/dev/null 2>&1 || true; "
        "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'if [ ! -s /tmp/cognus/orderer-msp/signcerts/cert.pem ]; then s=$(find /tmp/cognus/orderer-msp/signcerts -type f 2>/dev/null | head -n 1); [ -n \"$s\" ] && cp \"$s\" /tmp/cognus/orderer-msp/signcerts/cert.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/orderer-msp/keystore/key.pem ]; then k=$(find /tmp/cognus/orderer-msp/keystore -type f 2>/dev/null | head -n 1); [ -n \"$k\" ] && cp \"$k\" /tmp/cognus/orderer-msp/keystore/key.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/orderer-msp/admincerts/admincert.pem ] && [ -s /tmp/cognus/orderer-msp/signcerts/cert.pem ]; then cp /tmp/cognus/orderer-msp/signcerts/cert.pem /tmp/cognus/orderer-msp/admincerts/admincert.pem >/dev/null 2>&1 || true; fi' >/dev/null 2>&1 || true; "
        "fi; "
        "COGNUS_ORDERER_SAMPLE_MSP=0; "
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -f /tmp/cognus/orderer-msp/.cognus-sample-msp || test -s /tmp/cognus/orderer-msp/signcerts/peer.pem' >/dev/null 2>&1; then COGNUS_ORDERER_SAMPLE_MSP=1; fi; "
        "COGNUS_EFFECTIVE_ORDERER_MSPID=$COGNUS_ORDERER_LOCAL_MSPID; "
        "if [ \"$COGNUS_ORDERER_SAMPLE_MSP\" = \"1\" ]; then COGNUS_EFFECTIVE_ORDERER_MSPID=SampleOrg; fi; "
        "if [ -z \"$COGNUS_EFFECTIVE_ORDERER_MSPID\" ] || ( [ \"$COGNUS_ORDERER_SAMPLE_MSP\" != \"1\" ] && ( [ \"$COGNUS_EFFECTIVE_ORDERER_MSPID\" = SampleOrg ] || [ \"$COGNUS_EFFECTIVE_ORDERER_MSPID\" = Org1MSP ] || [ \"$COGNUS_EFFECTIVE_ORDERER_MSPID\" = sampleorg ] ) ); then "
        "if [ -n \"$COGNUS_ORG_MSP_ID\" ]; then COGNUS_EFFECTIVE_ORDERER_MSPID=$COGNUS_ORG_MSP_ID; fi; "
        "fi; "
        "if [ \"$COGNUS_EFFECTIVE_ORDERER_MSPID\" = sampleorg ]; then COGNUS_EFFECTIVE_ORDERER_MSPID=SampleOrg; fi; "
        "if [ -z \"$COGNUS_EFFECTIVE_ORDERER_MSPID\" ]; then COGNUS_EFFECTIVE_ORDERER_MSPID=Org1MSP; fi; "
    ).format(
        orderer_tls_enabled="1" if orderer_tls_expected else "0",
        orderer=shlex.quote(orderer_container),
        run_id=shlex.quote(str(run_state.get("run_id", "") or "").strip()),
        orderer_node_id=shlex.quote(str(orderer_host_row.get("node_id", "") or "").strip()),
    )

    checks = []
    for channel_id in channel_ids:
        normalized_channel_id = str(channel_id or "").strip().lower()
        if not normalized_channel_id:
            continue

        channel_q = shlex.quote(normalized_channel_id)
        configtx_profile_autodetect_cfg_step = _build_configtx_profile_autodetect_step(
            normalized_channel_id,
            "cfg",
        )
        configtx_profile_autodetect_cfg_dir_step = _build_configtx_profile_autodetect_step(
            normalized_channel_id,
            "cfg_dir",
        )
        configtx_block_direct_fallback_step = _build_configtx_block_direct_fallback_step(
            normalized_channel_id
        )
        channel_block_expected_channel_guard_step = (
            _build_channel_block_expected_channel_guard_step(normalized_channel_id)
        )
        channel_failure_clause = (
            "printf '%s\\n' COGNUS_CHANNEL_NOT_JOINED:{channel} >&2; exit 126; "
            if enforce_failures
            else "printf '%s\\n' COGNUS_CHANNEL_NOT_JOINED_DEFERRED:{channel} >&2; "
        ).format(channel=normalized_channel_id)

        auto_repair_probe_step = ""
        if RUNBOOK_CHAINCODE_CHANNEL_AUTO_REPAIR_ENABLED:
            auto_repair_probe_step = (
                "COGNUS_FORCE_CHANNEL_RECOVER=0; "
                "COGNUS_CHANNEL_GETINFO_OK=0; "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel getinfo -c {channel_q} >/dev/null 2>&1\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel getinfo -c {channel_q} >/dev/null 2>&1\" >/dev/null 2>&1; then COGNUS_CHANNEL_GETINFO_OK=1; fi; "
                "COGNUS_PEER_DELIVER_LOG=$(docker logs --tail 220 \"$COGNUS_PEER_CONTAINER\" 2>&1 || true); "
                "if printf '%s\\n' \"$COGNUS_PEER_DELIVER_LOG\" | grep -Eqi \"implicit policy evaluation failed\" && "
                "printf '%s\\n' \"$COGNUS_PEER_DELIVER_LOG\" | grep -Eqi \"Writers\" && "
                "printf '%s\\n' \"$COGNUS_PEER_DELIVER_LOG\" | grep -Eqi \"certificate signed by unknown authority\"; then "
                "COGNUS_FORCE_CHANNEL_RECOVER=1; "
                "printf '%s\\n' COGNUS_CHANNEL_RECOVERY_TRIGGERED:{channel}:writers_unknown_authority >&2; "
                "fi; "
                "if [ \"$COGNUS_CHANNEL_GETINFO_OK\" != \"1\" ] && printf '%s\\n' \"$COGNUS_PEER_DELIVER_LOG\" | grep -Eqi \"could not dial endpoint '127.0.0.1:7050'\"; then "
                "COGNUS_FORCE_CHANNEL_RECOVER=1; "
                "printf '%s\\n' COGNUS_CHANNEL_RECOVERY_TRIGGERED:{channel}:orderer_endpoint_loopback >&2; "
                "fi; "
            ).format(channel=normalized_channel_id, channel_q=channel_q)

        checks.append(
            (
                "COGNUS_TMP_ROOT=/tmp/cognus-run-${{COGNUS_RUN_ID}}-{channel}; "
                "COGNUS_HOST_BLOCK=$COGNUS_TMP_ROOT/channel.block; "
                "COGNUS_PEER_BLOCK=/tmp/cognus/channel.block; "
                "COGNUS_PEER_CA=/tmp/cognus/orderer-ca-{channel}.crt; "
                "mkdir -p \"$COGNUS_TMP_ROOT\" >/dev/null 2>&1 || true; "
                "{auto_repair_probe_step}"
                "if [ \"$COGNUS_FORCE_CHANNEL_RECOVER\" = \"1\" ]; then rm -f \"$COGNUS_HOST_BLOCK\" >/dev/null 2>&1 || true; fi; "
                "if ( ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel list 2>/dev/null | grep -F {channel_q} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel list 2>/dev/null | grep -F {channel_q} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel list 2>/dev/null | grep -F {channel_q} >/dev/null\" >/dev/null 2>&1 ) || [ \"$COGNUS_FORCE_CHANNEL_RECOVER\" = \"1\" ]; then "
                "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/tls/ca.crt\" \"$COGNUS_TMP_ROOT/orderer-ca.crt\" >/dev/null 2>&1 || true; "
                "if [ -s \"$COGNUS_TMP_ROOT/orderer-ca.crt\" ]; then docker cp \"$COGNUS_TMP_ROOT/orderer-ca.crt\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CA\" >/dev/null 2>&1 || true; fi; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA' >/dev/null 2>&1\" >/dev/null 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_BLOCK\" \"$COGNUS_HOST_BLOCK\" >/dev/null 2>&1 || true; "
                "{channel_block_expected_channel_guard_step}"
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ]; then "
                "docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"ls -1 /var/hyperledger/production/orderer/chains/{channel}/blockfile /var/hyperledger/production/orderer/chains/chains/{channel}/blockfile 2>/dev/null | head -n 1\" >\"$COGNUS_TMP_ROOT/orderer-block-path.txt\" 2>/dev/null || true; "
                "COGNUS_ORDERER_BLOCK_PATH=$(cat \"$COGNUS_TMP_ROOT/orderer-block-path.txt\" 2>/dev/null | head -n 1); "
                "if [ -n \"$COGNUS_ORDERER_BLOCK_PATH\" ] && [ \"$COGNUS_FORCE_CHANNEL_RECOVER\" != \"1\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_ORDERER_BLOCK_PATH\" \"$COGNUS_HOST_BLOCK\" >/dev/null 2>&1 || true; fi; "
                "{channel_block_expected_channel_guard_step}"
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ]; then "
                "COGNUS_CONFIGTX_WORK=$COGNUS_TMP_ROOT/configtx.yaml; "
                "COGNUS_CONFIGTX_ORIGINAL=$COGNUS_TMP_ROOT/configtx.original.yaml; "
                "COGNUS_CONFIGTX_FALLBACK=$COGNUS_TMP_ROOT/configtx.fallback.yaml; "
                "COGNUS_CONFIGTX_HOST_SOURCE=$COGNUS_TMP_ROOT/configtx.hostsource.yaml; "
                "rm -f \"$COGNUS_CONFIGTX_WORK\" \"$COGNUS_CONFIGTX_ORIGINAL\" \"$COGNUS_CONFIGTX_FALLBACK\" >/dev/null 2>&1 || true; "
                "rm -f \"$COGNUS_CONFIGTX_HOST_SOURCE\" >/dev/null 2>&1 || true; "
                "for candidate in /var/cognus/configtx/configtx.yaml /var/cognus/channel-artifacts/configtx.yaml \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain/automation/resources/configtx/configtx.yaml\" /etc/hyperledger/fabric/configtx.yaml; do "
                "if [ -s \"$candidate\" ]; then cp \"$candidate\" \"$COGNUS_CONFIGTX_HOST_SOURCE\" >/dev/null 2>&1 || true; test -s \"$COGNUS_CONFIGTX_HOST_SOURCE\" && break; fi; "
                "done; "
                "if [ ! -s \"$COGNUS_CONFIGTX_HOST_SOURCE\" ]; then "
                "for COGNUS_CFG_ROOT in \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain\" \"${{HOME}}\" /workspace /var/cognus; do "
                "[ -d \"$COGNUS_CFG_ROOT\" ] || continue; "
                "COGNUS_CFG_FOUND=$(find \"$COGNUS_CFG_ROOT\" -maxdepth 8 -type f \\( -path '*/configtx/configtx.yaml' -o -path '*/fabric/configtx.yaml' -o -path '*/fabric/config/configtx.yaml' -o -name 'configtx.yaml' \\) 2>/dev/null | head -n 1); "
                "if [ -n \"$COGNUS_CFG_FOUND\" ] && [ -s \"$COGNUS_CFG_FOUND\" ]; then cp \"$COGNUS_CFG_FOUND\" \"$COGNUS_CONFIGTX_HOST_SOURCE\" >/dev/null 2>&1 || true; test -s \"$COGNUS_CONFIGTX_HOST_SOURCE\" && break; fi; "
                "done; "
                "fi; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/configtx.yaml\" \"$COGNUS_CONFIGTX_ORIGINAL\" >/dev/null 2>&1 || true; "
                "printf '%s' '{configtx_fallback_b64}' | base64 -d > \"$COGNUS_CONFIGTX_FALLBACK\" 2>/dev/null || true; "
                "if [ ! -s \"$COGNUS_CONFIGTX_ORIGINAL\" ] && [ -s \"$COGNUS_CONFIGTX_HOST_SOURCE\" ]; then cp \"$COGNUS_CONFIGTX_HOST_SOURCE\" \"$COGNUS_CONFIGTX_ORIGINAL\" >/dev/null 2>&1 || true; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_ORIGINAL\" ] && [ -s \"$COGNUS_CONFIGTX_FALLBACK\" ]; then cp \"$COGNUS_CONFIGTX_FALLBACK\" \"$COGNUS_CONFIGTX_ORIGINAL\" >/dev/null 2>&1 || true; fi; "
                "COGNUS_CONFIGTX_CONSENTER_COUNT=$(grep -Ec '^[[:space:]]*-[[:space:]]*Host:' \"$COGNUS_CONFIGTX_ORIGINAL\" 2>/dev/null || echo 0); "
                "if [ \"$COGNUS_CONFIGTX_CONSENTER_COUNT\" -gt 1 ] && [ -s \"$COGNUS_CONFIGTX_FALLBACK\" ]; then "
                "printf '%s\\n' COGNUS_CONFIGTX_MULTI_CONSENTER_DETECTED:{channel}:using_original >&2; "
                "cp \"$COGNUS_CONFIGTX_FALLBACK\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "printf '%s\\n' COGNUS_CONFIGTX_FALLBACK_APPLIED:{channel}:multi_consenter >&2; "
                "fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_WORK\" ] && [ -s \"$COGNUS_CONFIGTX_ORIGINAL\" ]; then cp \"$COGNUS_CONFIGTX_ORIGINAL\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_WORK\" ] && [ -s \"$COGNUS_CONFIGTX_HOST_SOURCE\" ]; then cp \"$COGNUS_CONFIGTX_HOST_SOURCE\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; printf '%s\\n' COGNUS_CONFIGTX_FALLBACK_APPLIED:{channel}:host_source >&2; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_WORK\" ] && [ -s \"$COGNUS_CONFIGTX_FALLBACK\" ]; then cp \"$COGNUS_CONFIGTX_FALLBACK\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; printf '%s\\n' COGNUS_CONFIGTX_FALLBACK_APPLIED:{channel}:source_missing >&2; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_WORK\" ]; then printf '%s\\n' COGNUS_CONFIGTX_SOURCE_MISSING:{channel} >&2; fi; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/tls/server.crt\" \"$COGNUS_TMP_ROOT/orderer-tls.crt\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g; s#raft[0-9]+\\.example\\.com#$COGNUS_ORDERER_CONTAINER#g; s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\.0\\.0\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg#$COGNUS_EFFECTIVE_ORDERER_MSPID#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#/etc/hyperledger/fabric/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#path/to/ClientTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#path/to/ServerTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: /etc/hyperledger/fabric/msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: /etc/hyperledger/fabric/msp#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "awk 'BEGIN{{in_cons=0;cons_idx=0;keep=1}} /^[[:space:]]*Consenters:[[:space:]]*$/{{in_cons=1;cons_idx=0;keep=1;print;next}} {{ if (in_cons) {{ if ($0 ~ /^[[:space:]]*-[[:space:]]*Host:[[:space:]]*/) {{ cons_idx++; keep=(cons_idx==1); if (keep) print; next }} if ($0 ~ /^[[:space:]]*(Port:|ClientTLSCert:|ServerTLSCert:)[[:space:]]*/) {{ if (keep) print; next }} if ($0 ~ /^[[:space:]]*$/) {{ if (keep) print; next }} in_cons=0 }} print }}' \"$COGNUS_CONFIGTX_WORK\" > \"$COGNUS_CONFIGTX_WORK.dedup\" 2>/dev/null && mv \"$COGNUS_CONFIGTX_WORK.dedup\" \"$COGNUS_CONFIGTX_WORK\" || true; "
                "docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || docker pull hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"cfg=/tmp/cognus/cfg-gen; rm -rf \\\"\\$cfg\\\"; mkdir -p \\\"\\$cfg\\\"; cp /tmp/cognus/configtx.yaml \\\"\\$cfg/configtx.yaml\\\"; if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; fi; {configtx_profile_autodetect_cfg_step} chmod -R a+rX \\\"\\$cfg\\\" >/dev/null 2>&1 || true; find \\\"\\$cfg\\\" -type f -name '*.pem' -exec chmod 644 {{}} \\\\; >/dev/null 2>&1 || true; chmod 666 /tmp/cognus/channel.block /tmp/cognus/channel.tx >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_CONFIGTX_HOST_SOURCE\" ]; then "
                "cp \"$COGNUS_CONFIGTX_HOST_SOURCE\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "printf '%s\\n' COGNUS_CONFIGTX_FALLBACK_APPLIED:{channel}:host_source_retry >&2; "
                "sed -i -E \"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g; s#raft[0-9]+\\.example\\.com#$COGNUS_ORDERER_CONTAINER#g; s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\.0\\.0\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg#$COGNUS_EFFECTIVE_ORDERER_MSPID#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#/etc/hyperledger/fabric/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#path/to/ClientTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#path/to/ServerTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: /etc/hyperledger/fabric/msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: /etc/hyperledger/fabric/msp#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"cfg=/tmp/cognus/cfg-gen; rm -rf \\\"\\$cfg\\\"; mkdir -p \\\"\\$cfg\\\"; cp /tmp/cognus/configtx.yaml \\\"\\$cfg/configtx.yaml\\\"; if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; fi; {configtx_profile_autodetect_cfg_step} chmod -R a+rX \\\"\\$cfg\\\" >/dev/null 2>&1 || true; find \\\"\\$cfg\\\" -type f -name '*.pem' -exec chmod 644 {{}} \\\\; >/dev/null 2>&1 || true; chmod 666 /tmp/cognus/channel.block /tmp/cognus/channel.tx >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "fi; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_CONFIGTX_FALLBACK\" ]; then "
                "cp \"$COGNUS_CONFIGTX_FALLBACK\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "printf '%s\\n' COGNUS_CONFIGTX_FALLBACK_APPLIED:{channel}:generation_retry >&2; "
                "sed -i -E \"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g; s#raft[0-9]+\\.example\\.com#$COGNUS_ORDERER_CONTAINER#g; s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\.0\\.0\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg#$COGNUS_EFFECTIVE_ORDERER_MSPID#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#/etc/hyperledger/fabric/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#path/to/ClientTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#path/to/ServerTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: /etc/hyperledger/fabric/msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: /etc/hyperledger/fabric/msp#g\" \"$COGNUS_CONFIGTX_WORK\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"cfg=/tmp/cognus/cfg-gen; rm -rf \\\"\\$cfg\\\"; mkdir -p \\\"\\$cfg\\\"; cp /tmp/cognus/configtx.yaml \\\"\\$cfg/configtx.yaml\\\"; if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; fi; {configtx_profile_autodetect_cfg_step} chmod -R a+rX \\\"\\$cfg\\\" >/dev/null 2>&1 || true; find \\\"\\$cfg\\\" -type f -name '*.pem' -exec chmod 644 {{}} \\\\; >/dev/null 2>&1 || true; chmod 666 /tmp/cognus/channel.block /tmp/cognus/channel.tx >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "fi; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_CONFIGTX_WORK\" ]; then "
                "{configtx_block_direct_fallback_step}"
                "fi; "
                "{channel_block_expected_channel_guard_step}"
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ ! -s \"$COGNUS_TMP_ROOT/channel.tx\" ]; then printf '%s\\n' COGNUS_CHANNEL_TX_MISSING:{channel} >&2; fi; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_TMP_ROOT/channel.tx\" ]; then "
                "COGNUS_PEER_TX=/tmp/cognus/channel.tx; "
                "COGNUS_CHANNEL_CREATE_DIAG=$COGNUS_TMP_ROOT/channel-create-diag.log; "
                "COGNUS_CHANNEL_FETCH_DIAG=$COGNUS_TMP_ROOT/channel-fetch-diag.log; "
                "rm -f \"$COGNUS_CHANNEL_CREATE_DIAG\" \"$COGNUS_CHANNEL_FETCH_DIAG\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"mkdir -p /tmp/cognus >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_TMP_ROOT/channel.tx\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_TX\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA'\" >\"$COGNUS_CHANNEL_FETCH_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT\" >>\"$COGNUS_CHANNEL_FETCH_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA'\" >>\"$COGNUS_CHANNEL_FETCH_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT\" >>\"$COGNUS_CHANNEL_FETCH_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA'\" >>\"$COGNUS_CHANNEL_FETCH_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_BLOCK' -c {channel_q} -o $COGNUS_ORDERER_ENDPOINT\" >>\"$COGNUS_CHANNEL_FETCH_DIAG\" 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel create -c {channel_q} -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK' -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA'\" >\"$COGNUS_CHANNEL_CREATE_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel create -c {channel_q} -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK' -o $COGNUS_ORDERER_ENDPOINT\" >>\"$COGNUS_CHANNEL_CREATE_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel create -c {channel_q} -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK' -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA'\" >>\"$COGNUS_CHANNEL_CREATE_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel create -c {channel_q} -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK' -o $COGNUS_ORDERER_ENDPOINT\" >>\"$COGNUS_CHANNEL_CREATE_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel create -c {channel_q} -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK' -o $COGNUS_ORDERER_ENDPOINT --tls --cafile '$COGNUS_PEER_CA'\" >>\"$COGNUS_CHANNEL_CREATE_DIAG\" 2>&1 || "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel create -c {channel_q} -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK' -o $COGNUS_ORDERER_ENDPOINT\" >>\"$COGNUS_CHANNEL_CREATE_DIAG\" 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_BLOCK\" \"$COGNUS_HOST_BLOCK\" >/dev/null 2>&1 || true; "
                "{channel_block_expected_channel_guard_step}"
                "if [ -s \"$COGNUS_HOST_BLOCK\" ]; then printf '%s\\n' COGNUS_CHANNEL_BLOCK_RECOVERED_VIA_TX:{channel} >&2; fi; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_CHANNEL_FETCH_DIAG\" ]; then COGNUS_FETCH_LAST=$(tail -n 1 \"$COGNUS_CHANNEL_FETCH_DIAG\" | tr -d '\\r' | cut -c1-220); [ -n \"$COGNUS_FETCH_LAST\" ] && printf '%s\\n' COGNUS_CHANNEL_FETCH_DIAG:$COGNUS_FETCH_LAST >&2; fi; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ] && [ -s \"$COGNUS_CHANNEL_CREATE_DIAG\" ]; then COGNUS_CREATE_LAST=$(tail -n 1 \"$COGNUS_CHANNEL_CREATE_DIAG\" | tr -d '\\r' | cut -c1-220); [ -n \"$COGNUS_CREATE_LAST\" ] && printf '%s\\n' COGNUS_CHANNEL_CREATE_DIAG:$COGNUS_CREATE_LAST >&2; fi; "
                "fi; "
                "fi; "
                "fi; "
                "COGNUS_PARTICIPATION_STATE=$COGNUS_TMP_ROOT/participation-state.json; "
                "COGNUS_PARTICIPATION_DETAIL=$COGNUS_TMP_ROOT/participation-detail.json; "
                "COGNUS_ORDERER_LOG_TAIL=$COGNUS_TMP_ROOT/orderer-log-tail.txt; "
                "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels >\"$COGNUS_PARTICIPATION_STATE\" 2>/dev/null || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels/{channel} >\"$COGNUS_PARTICIPATION_DETAIL\" 2>/dev/null || true; "
                "docker logs --tail 240 \"$COGNUS_ORDERER_CONTAINER\" >\"$COGNUS_ORDERER_LOG_TAIL\" 2>&1 || true; "
                "fi; "
                "if [ ! -s \"$COGNUS_HOST_BLOCK\" ]; then printf '%s\\n' COGNUS_CHANNEL_BLOCK_INVALID:{channel} >&2; fi; "
                "if [ -s \"$COGNUS_HOST_BLOCK\" ]; then "
                "COGNUS_BLOCK_SHA_FILE=$COGNUS_TMP_ROOT/channel.block.sha256; "
                "COGNUS_BLOCK_SIZE_FILE=$COGNUS_TMP_ROOT/channel.block.size; "
                "COGNUS_BLOCK_INSPECT_JSON=$COGNUS_TMP_ROOT/channel.block.inspect.json; "
                "COGNUS_BLOCK_INSPECT_ERR=$COGNUS_TMP_ROOT/channel.block.inspect.err; "
                "sha256sum \"$COGNUS_HOST_BLOCK\" 2>/dev/null | awk '{{print $1}}' >\"$COGNUS_BLOCK_SHA_FILE\" || true; "
                "stat -c %s \"$COGNUS_HOST_BLOCK\" >\"$COGNUS_BLOCK_SIZE_FILE\" 2>/dev/null || wc -c <\"$COGNUS_HOST_BLOCK\" >\"$COGNUS_BLOCK_SIZE_FILE\" 2>/dev/null || true; "
                "COGNUS_BLOCK_SHA=$(cat \"$COGNUS_BLOCK_SHA_FILE\" 2>/dev/null | head -n 1 | tr -d '[:space:]' || true); "
                "COGNUS_BLOCK_SIZE=$(cat \"$COGNUS_BLOCK_SIZE_FILE\" 2>/dev/null | head -n 1 | tr -d '[:space:]' || true); "
                "COGNUS_BLOCK_VALID=0; "
                "docker run --rm -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc 'configtxgen -inspectBlock /tmp/cognus/channel.block >/tmp/cognus/channel.block.inspect.json 2>/tmp/cognus/channel.block.inspect.err' >/dev/null 2>&1 && COGNUS_BLOCK_VALID=1 || true; "
                "if [ -z \"$COGNUS_BLOCK_SHA\" ] || [ ${{#COGNUS_BLOCK_SHA}} -ne 64 ] || [ -z \"$COGNUS_BLOCK_SIZE\" ] || [ \"$COGNUS_BLOCK_SIZE\" -le 0 ] 2>/dev/null; then COGNUS_BLOCK_VALID=0; fi; "
                "if [ \"$COGNUS_BLOCK_VALID\" != \"1\" ]; then printf '%s\n' COGNUS_CHANNEL_BLOCK_INVALID:{channel} >&2; fi; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels >\"$COGNUS_PARTICIPATION_STATE\" 2>/dev/null || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels/{channel} >\"$COGNUS_PARTICIPATION_DETAIL\" 2>/dev/null || true; "
                "if [ \"$COGNUS_BLOCK_VALID\" = \"1\" ] && ( [ \"$COGNUS_FORCE_CHANNEL_RECOVER\" = \"1\" ] || ! grep -E '\"name\"[[:space:]]*:[[:space:]]*\"{channel}\"' \"$COGNUS_PARTICIPATION_DETAIL\" >/dev/null 2>&1 || ! grep -E '\"consensusRelation\"[[:space:]]*:[[:space:]]*\"(consenter|follower)\"' \"$COGNUS_PARTICIPATION_DETAIL\" >/dev/null 2>&1 ); then "
                "docker image inspect alpine:3.20 >/dev/null 2>&1 || docker pull alpine:3.20 >/dev/null 2>&1 || true; "
                "docker stop \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" alpine:3.20 sh -lc 'rm -rf /var/hyperledger/production/orderer/chains/{channel} /var/hyperledger/production/orderer/chains/chains/{channel} /var/hyperledger/production/orderer/pendingops/{channel} /var/hyperledger/production/orderer/pendingops/join/{channel} /var/hyperledger/production/orderer/pendingops/remove/{channel} /var/hyperledger/production/orderer/etcdraft/chains/{channel} /var/hyperledger/production/orderer/etcdraft/wal/{channel} /var/hyperledger/production/orderer/etcdraft/snapshot/{channel} 2>/dev/null || true; mkdir -p /var/hyperledger/production/orderer/chains /var/hyperledger/production/orderer/pendingops /var/hyperledger/production/orderer/etcdraft >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
                "docker start \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; "
                "sleep 8; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels >/dev/null 2>&1 || true; "
                "sleep 2; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS -X DELETE http://127.0.0.1:9443/participation/v1/channels/{channel} >/dev/null 2>&1 || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER -v \"$COGNUS_TMP_ROOT\":/tmp/cognus curlimages/curl:8.6.0 -sS -X POST http://127.0.0.1:9443/participation/v1/channels -F \"config-block=@/tmp/cognus/channel.block\" >\"$COGNUS_TMP_ROOT/participation-post.log\" 2>&1 || true; "
                "sleep 2; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels >\"$COGNUS_PARTICIPATION_STATE\" 2>/dev/null || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels/{channel} >\"$COGNUS_PARTICIPATION_DETAIL\" 2>/dev/null || true; "
                "if ! grep -E '\"name\"[[:space:]]*:[[:space:]]*\"{channel}\"' \"$COGNUS_PARTICIPATION_DETAIL\" >/dev/null 2>&1 || ! grep -E '\"consensusRelation\"[[:space:]]*:[[:space:]]*\"(consenter|follower)\"' \"$COGNUS_PARTICIPATION_DETAIL\" >/dev/null 2>&1; then "
                "sleep 4; "
                "docker stop \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" alpine:3.20 sh -lc 'rm -rf /var/hyperledger/production/orderer/chains/{channel} /var/hyperledger/production/orderer/chains/chains/{channel} /var/hyperledger/production/orderer/pendingops/{channel} /var/hyperledger/production/orderer/pendingops/join/{channel} /var/hyperledger/production/orderer/pendingops/remove/{channel} /var/hyperledger/production/orderer/etcdraft/chains/{channel} /var/hyperledger/production/orderer/etcdraft/wal/{channel} /var/hyperledger/production/orderer/etcdraft/snapshot/{channel} 2>/dev/null || true; mkdir -p /var/hyperledger/production/orderer/chains /var/hyperledger/production/orderer/pendingops /var/hyperledger/production/orderer/etcdraft >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; "
                "docker start \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; "
                "sleep 8; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS -X DELETE http://127.0.0.1:9443/participation/v1/channels/{channel} >/dev/null 2>&1 || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER -v \"$COGNUS_TMP_ROOT\":/tmp/cognus curlimages/curl:8.6.0 -sS -X POST http://127.0.0.1:9443/participation/v1/channels -F \"config-block=@/tmp/cognus/channel.block\" >\"$COGNUS_TMP_ROOT/participation-post-retry.log\" 2>&1 || true; "
                "sleep 4; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels >\"$COGNUS_PARTICIPATION_STATE\" 2>/dev/null || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER curlimages/curl:8.6.0 -sS http://127.0.0.1:9443/participation/v1/channels/{channel} >\"$COGNUS_PARTICIPATION_DETAIL\" 2>/dev/null || true; "
                "fi; "
                "docker logs --tail 240 \"$COGNUS_ORDERER_CONTAINER\" >\"$COGNUS_ORDERER_LOG_TAIL\" 2>&1 || true; "
                "if grep -Eqi 'unexpected EOF|error decoding varint|panic serving' \"$COGNUS_TMP_ROOT/participation-post.log\" \"$COGNUS_TMP_ROOT/participation-post-retry.log\" \"$COGNUS_ORDERER_LOG_TAIL\" 2>/dev/null; then printf '%s\n' COGNUS_ORDERER_LEDGER_CORRUPTION:{channel} >&2; fi; "
                "if ! grep -E '\"name\"[[:space:]]*:[[:space:]]*\"{channel}\"' \"$COGNUS_PARTICIPATION_DETAIL\" >/dev/null 2>&1 || ! grep -E '\"consensusRelation\"[[:space:]]*:[[:space:]]*\"(consenter|follower)\"' \"$COGNUS_PARTICIPATION_DETAIL\" >/dev/null 2>&1; then printf '%s\n' COGNUS_ORDERER_PARTICIPATION_JOIN_FAILED:{channel} >&2; fi; "
                "fi; "
                "docker cp \"$COGNUS_HOST_BLOCK\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_BLOCK\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel join -b '$COGNUS_PEER_BLOCK' >/dev/null 2>&1\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel join -b '$COGNUS_PEER_BLOCK' >/dev/null 2>&1\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel join -b '$COGNUS_PEER_BLOCK' >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "fi; "
                "fi; "
                "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer channel list 2>/dev/null | grep -F {channel_q} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer channel list 2>/dev/null | grep -F {channel_q} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel list 2>/dev/null | grep -F {channel_q} >/dev/null\" >/dev/null 2>&1; then {channel_failure} fi; "
                "fi; "
            ).format(
                channel=normalized_channel_id,
                channel_q=channel_q,
                channel_failure=channel_failure_clause,
                configtx_fallback_b64=RUNBOOK_CONFIGTX_FALLBACK_B64,
                configtx_profile_autodetect_cfg_step=configtx_profile_autodetect_cfg_step,
                configtx_block_direct_fallback_step=configtx_block_direct_fallback_step,
                channel_block_expected_channel_guard_step=channel_block_expected_channel_guard_step,
                auto_repair_probe_step=auto_repair_probe_step,
            )
        )

        for chaincode_id in pairs_by_channel.get(normalized_channel_id, []):
            normalized_chaincode_id = str(chaincode_id or "").strip().lower()
            if not normalized_chaincode_id:
                continue
            chaincode_artifact_metadata = _resolve_chaincode_artifact_metadata(
                run_state,
                normalized_channel_id,
                normalized_chaincode_id,
            )
            chaincode_package_lookup_step = _build_chaincode_package_lookup_step(
                "COGNUS_CC_PACKAGE_HOST",
                chaincode_artifact_metadata.get("search_terms", []),
                chaincode_artifact_metadata.get("candidate_files", []),
            )
            chaincode_package_id_lookup_step = _build_chaincode_package_id_lookup_step(
                "COGNUS_CC_PACKAGE_ID",
                "COGNUS_CC_QUERYINSTALLED",
                chaincode_artifact_metadata.get("search_terms", []),
            )
            chaincode_bootstrap_lookup_step = _build_chaincode_package_lookup_step(
                "COGNUS_CC_BOOTSTRAP_PKG",
                chaincode_artifact_metadata.get("search_terms", []),
                chaincode_artifact_metadata.get("candidate_files", []),
            )
            lifecycle_failure_clause = (
                "printf '%s\\n' COGNUS_CHAINCODE_NOT_COMMITTED:{channel}:{chaincode} >&2; exit 126; "
                if enforce_failures
                else "printf '%s\\n' COGNUS_CHAINCODE_NOT_COMMITTED_DEFERRED:{channel}:{chaincode} >&2; "
            ).format(channel=normalized_channel_id, chaincode=normalized_chaincode_id)
            chaincode_runtime_mode = _resolve_chaincode_runtime_mode(
                chaincode_artifact_metadata
            )
            if chaincode_runtime_mode == "ccaas":
                checks.append(
                    _build_chaincode_ccaas_lifecycle_check(
                        run_state,
                        host_row,
                        normalized_channel_id,
                        normalized_chaincode_id,
                        chaincode_artifact_metadata,
                        lifecycle_failure_clause,
                    )
                )
                continue
            checks.append(
                (
                    "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ] && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CA'\" >/dev/null 2>&1; then "
                    "COGNUS_TLS_TMP=/tmp/cognus-run-${{COGNUS_RUN_ID}}-{channel_plain}; "
                    "mkdir -p \"$COGNUS_TLS_TMP\" >/dev/null 2>&1 || true; "
                    "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/tls/ca.crt\" \"$COGNUS_TLS_TMP/orderer-ca.crt\" >/dev/null 2>&1 || true; "
                    "if [ -s \"$COGNUS_TLS_TMP/orderer-ca.crt\" ]; then docker cp \"$COGNUS_TLS_TMP/orderer-ca.crt\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CA\" >/dev/null 2>&1 || true; fi; "
                    "fi; "
                    "{chaincode_bootstrap_lookup_step}"
                    "if [ -s \"$COGNUS_CC_BOOTSTRAP_PKG\" ]; then "
                    "printf '%s%s\n' 'COGNUS_CC_BOOTSTRAP_PKG_FOUND:' \"$COGNUS_CC_BOOTSTRAP_PKG\" >&2; "
                    "COGNUS_CC_BOOTSTRAP_PEER=/tmp/cognus/${{COGNUS_RUN_ID}}-{chaincode_pattern}-bootstrap.tar.gz; "
                    "docker cp \"$COGNUS_CC_BOOTSTRAP_PKG\" \"$COGNUS_PEER_CONTAINER:$COGNUS_CC_BOOTSTRAP_PEER\" >/dev/null 2>&1 || true; "
                    "docker exec \"$COGNUS_PEER_CONTAINER\" peer lifecycle chaincode install \"$COGNUS_CC_BOOTSTRAP_PEER\" >/dev/null 2>&1 || true; "
                    "COGNUS_CC_BOOTSTRAP_QI=$(docker exec \"$COGNUS_PEER_CONTAINER\" peer lifecycle chaincode queryinstalled 2>/dev/null | grep -i 'Package ID:' | head -n 1 || true); "
                    "if [ -n \"$COGNUS_CC_BOOTSTRAP_QI\" ]; then printf '%s%s\n' 'COGNUS_CC_BOOTSTRAP_QI:' \"$COGNUS_CC_BOOTSTRAP_QI\" >&2; fi; "
                    "fi; "
                    "if [ ! -s \"$COGNUS_CC_BOOTSTRAP_PKG\" ]; then printf '%s%s\n' 'COGNUS_CC_BOOTSTRAP_PKG_MISSING:' {chaincode}; fi; "
                    "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then "
                    "COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode queryinstalled 2>/dev/null || true); "
                    "if ! printf '%s\\n' \"$COGNUS_CC_QUERYINSTALLED\" | grep -qi 'Package ID:'; then COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'peer lifecycle chaincode queryinstalled 2>/dev/null || true' 2>/dev/null || true); fi; "
                    "{chaincode_package_id_lookup_step}"
                    "if [ -z \"$COGNUS_CC_PACKAGE_ID\" ]; then "
                    "{chaincode_package_lookup_step}"
                    "COGNUS_CC_PACKAGE_PEER=/tmp/cognus/${{COGNUS_RUN_ID}}-{chaincode_pattern}.tar.gz; "
                    "if [ -n \"$COGNUS_CC_PACKAGE_HOST\" ]; then "
                    "docker image inspect hyperledger/fabric-ccenv:2.5 >/dev/null 2>&1 || docker pull hyperledger/fabric-ccenv:2.5 >/dev/null 2>&1 || true; "
                    "docker cp \"$COGNUS_CC_PACKAGE_HOST\" \"$COGNUS_PEER_CONTAINER:$COGNUS_CC_PACKAGE_PEER\" >/dev/null 2>&1 || true; "
                    "docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode install \"$COGNUS_CC_PACKAGE_PEER\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" peer lifecycle chaincode install \"$COGNUS_CC_PACKAGE_PEER\" >/dev/null 2>&1 || true; "
                    "COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode queryinstalled 2>/dev/null || true); "
                    "if ! printf '%s\\n' \"$COGNUS_CC_QUERYINSTALLED\" | grep -qi 'Package ID:'; then COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'peer lifecycle chaincode queryinstalled 2>/dev/null || true' 2>/dev/null || true); fi; "
                    "{chaincode_package_id_lookup_step}"
                    "fi; "
                    "fi; "
                    "if [ -z \"$COGNUS_CC_PACKAGE_ID\" ]; then "
                    "{chaincode_bootstrap_lookup_step}"
                    "COGNUS_CC_FALLBACK_PKG=$COGNUS_CC_BOOTSTRAP_PKG; "
                    "if [ -s \"$COGNUS_CC_FALLBACK_PKG\" ]; then "
                    "COGNUS_CC_PACKAGE_PEER=/tmp/cognus/${{COGNUS_RUN_ID}}-{chaincode_pattern}-fallback.tar.gz; "
                    "docker cp \"$COGNUS_CC_FALLBACK_PKG\" \"$COGNUS_PEER_CONTAINER:$COGNUS_CC_PACKAGE_PEER\" >/dev/null 2>&1 || true; "
                    "docker exec \"$COGNUS_PEER_CONTAINER\" peer lifecycle chaincode install \"$COGNUS_CC_PACKAGE_PEER\" >/dev/null 2>&1 || true; "
                    "COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'peer lifecycle chaincode queryinstalled 2>/dev/null || true' 2>/dev/null || true); "
                    "{chaincode_package_id_lookup_step}"
                    "fi; "
                    "fi; "
                    "COGNUS_CC_ORDERER_FLAGS=''; "
                    "COGNUS_CC_ORDERER_HOST_OVERRIDES=''; "
                    "if [ \"${{COGNUS_ORDERER_TLS_ENABLED:-0}}\" = \"1\" ] && docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CA'\" >/dev/null 2>&1; then COGNUS_CC_ORDERER_FLAGS=\"--tls --cafile $COGNUS_PEER_CA\"; COGNUS_CC_ORDERER_HOST_OVERRIDES=\"{orderer_host_override} $COGNUS_ORDERER_CONTAINER localhost\"; fi; "
                    "if [ -n \"$COGNUS_CC_PACKAGE_ID\" ]; then "
                    "for COGNUS_CC_ORDERER_EP in \"$COGNUS_ORDERER_ENDPOINT\" \"$COGNUS_ORDERER_ENDPOINT_ALT\"; do "
                    "[ -n \"$COGNUS_CC_ORDERER_EP\" ] || continue; "
                    "if [ -n \"$COGNUS_CC_ORDERER_HOST_OVERRIDES\" ]; then "
                    "for COGNUS_CC_HOST_OVERRIDE in $COGNUS_CC_ORDERER_HOST_OVERRIDES; do "
                    "for COGNUS_CC_ATTEMPT in 1 2 3; do "
                    "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" $COGNUS_CC_ORDERER_FLAGS --ordererTLSHostnameOverride \"$COGNUS_CC_HOST_OVERRIDE\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); "
                    "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" $COGNUS_CC_ORDERER_FLAGS --ordererTLSHostnameOverride \"$COGNUS_CC_HOST_OVERRIDE\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); "
                    "if printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -qi 'system channel creation pending: server requires restart'; then if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then docker restart \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; sleep 5; fi; fi; "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 3; fi; "
                    "sleep 2; "
                    "done; "
                    "done; "
                    "else "
                    "for COGNUS_CC_ATTEMPT in 1 2 3; do "
                    "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); "
                    "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); "
                    "if printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -qi 'system channel creation pending: server requires restart'; then if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then docker restart \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; sleep 5; fi; fi; "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 2; fi; "
                    "sleep 2; "
                    "done; "
                    "fi; "
                    "done; "
                    "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -Eqi 'x509|tls|certificate'; then "
                    "for COGNUS_CC_ORDERER_EP in \"$COGNUS_ORDERER_ENDPOINT\" \"$COGNUS_ORDERER_ENDPOINT_ALT\"; do "
                    "[ -n \"$COGNUS_CC_ORDERER_EP\" ] || continue; "
                    "for COGNUS_CC_ATTEMPT in 1 2; do "
                    "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); "
                    "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 2; fi; "
                    "sleep 2; "
                    "done; "
                    "done; "
                    "fi; "
                    "if [ \"$COGNUS_PEER_SAMPLE_MSP\" != \"1\" ] && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -Eqi 'creator org unknown'; then "
                    "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -s /tmp/cognus/orderer-msp/signcerts/cert.pem && find /tmp/cognus/orderer-msp/keystore -type f | head -n 1 >/dev/null 2>&1' >/dev/null 2>&1; then "
                    "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ] || ! docker inspect \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1; then COGNUS_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id_plain} --filter label=cognus.node_type=orderer --format '{{{{.Names}}}}' | head -n 1); fi; "
                    "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then COGNUS_ORDERER_MSP_STAGE=/tmp/cognus-run-${{COGNUS_RUN_ID}}-orderer-msp-retry; docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'rm -rf /tmp/cognus/orderer-msp >/dev/null 2>&1 || true; mkdir -p /tmp/cognus/orderer-msp >/dev/null 2>&1 || true' >/dev/null 2>&1 || true; rm -rf \"$COGNUS_ORDERER_MSP_STAGE\" >/dev/null 2>&1 || true; mkdir -p \"$COGNUS_ORDERER_MSP_STAGE\" >/dev/null 2>&1 || true; docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/msp/.\" \"$COGNUS_ORDERER_MSP_STAGE/\" >/dev/null 2>&1 || true; docker cp \"$COGNUS_ORDERER_MSP_STAGE/.\" \"$COGNUS_PEER_CONTAINER:/tmp/cognus/orderer-msp/\" >/dev/null 2>&1 || true; docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'if [ ! -s /tmp/cognus/orderer-msp/signcerts/cert.pem ]; then s=$(find /tmp/cognus/orderer-msp/signcerts -type f 2>/dev/null | head -n 1); [ -n \"$s\" ] && cp \"$s\" /tmp/cognus/orderer-msp/signcerts/cert.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/orderer-msp/keystore/key.pem ]; then k=$(find /tmp/cognus/orderer-msp/keystore -type f 2>/dev/null | head -n 1); [ -n \"$k\" ] && cp \"$k\" /tmp/cognus/orderer-msp/keystore/key.pem >/dev/null 2>&1 || true; fi; if [ ! -s /tmp/cognus/orderer-msp/admincerts/admincert.pem ] && [ -s /tmp/cognus/orderer-msp/signcerts/cert.pem ]; then cp /tmp/cognus/orderer-msp/signcerts/cert.pem /tmp/cognus/orderer-msp/admincerts/admincert.pem >/dev/null 2>&1 || true; fi' >/dev/null 2>&1 || true; fi; "
                    "fi; "
                    "COGNUS_ORDERER_MSP_READY=0; "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -s /tmp/cognus/orderer-msp/signcerts/cert.pem && find /tmp/cognus/orderer-msp/keystore -type f | head -n 1 >/dev/null 2>&1' >/dev/null 2>&1; then COGNUS_ORDERER_MSP_READY=1; fi; "
                    "printf '%s%s\n' 'COGNUS_ORDERER_MSP_READY:' \"$COGNUS_ORDERER_MSP_READY\" >&2; "
                    "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then printf '%s%s\n' 'COGNUS_ORDERER_CONTAINER_USED:' \"$COGNUS_ORDERER_CONTAINER\" >&2; fi; "
                    "if [ -z \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH\" ]; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=/etc/hyperledger/fabric/msp; fi; "
                    "if [ \"$COGNUS_ORDERER_MSP_READY\" = \"1\" ]; then "
                    "COGNUS_PEER_ADMIN_MSPCONFIGPATH=/tmp/cognus/orderer-msp; "
                    "COGNUS_MSP_FALLBACK_REASON=creator_org_unknown_retry_with_orderer_msp; "
                    "else "
                    "COGNUS_MSP_FALLBACK_REASON=creator_org_unknown_retry_with_current_msp; "
                    "fi; "
                    "COGNUS_PEER_LOCAL_MSPID=$(printf '%s' \"$COGNUS_PEER_LOCAL_MSPID\" | tr -d '[:space:]'); "
                    "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=${{COGNUS_ORG_MSP_ID:-Org1MSP}}; fi; "
                    "if [ \"$COGNUS_PEER_LOCAL_MSPID\" = sampleorg ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; fi; "
                    "COGNUS_EXPECTED_MSP=$(printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | sed -nE 's/.*expected MSP ID ([A-Za-z0-9_.-]+).*/\\1/p' | head -n 1 | tr -d '[:space:]'); "
                    "if [ -n \"$COGNUS_EXPECTED_MSP\" ] && [ \"$COGNUS_EXPECTED_MSP\" != \"$COGNUS_PEER_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=$COGNUS_EXPECTED_MSP; COGNUS_MSP_FALLBACK_REASON=creator_org_unknown_retry_with_expected_msp; fi; "
                    "COGNUS_PEER_CMD_PREFIX=\"CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID\"; "
                    "for COGNUS_CC_ORDERER_EP in \"$COGNUS_ORDERER_ENDPOINT\" \"$COGNUS_ORDERER_ENDPOINT_ALT\"; do "
                    "[ -n \"$COGNUS_CC_ORDERER_EP\" ] || continue; "
                    "for COGNUS_CC_HOST_OVERRIDE in $COGNUS_CC_ORDERER_HOST_OVERRIDES localhost; do "
                    "[ -n \"$COGNUS_CC_HOST_OVERRIDE\" ] || continue; "
                    "for COGNUS_CC_ATTEMPT in 1 2; do "
                    "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" $COGNUS_CC_ORDERER_FLAGS --ordererTLSHostnameOverride \"$COGNUS_CC_HOST_OVERRIDE\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); "
                    "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" $COGNUS_CC_ORDERER_FLAGS --ordererTLSHostnameOverride \"$COGNUS_CC_HOST_OVERRIDE\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 3; fi; "
                    "sleep 2; "
                    "done; "
                    "done; "
                    "done; "
                    "fi; "
                    "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -Eqi 'x509|tls|certificate'; then "
                    "for COGNUS_CC_ORDERER_EP in \"$COGNUS_ORDERER_ENDPOINT\" \"$COGNUS_ORDERER_ENDPOINT_ALT\"; do "
                    "[ -n \"$COGNUS_CC_ORDERER_EP\" ] || continue; "
                    "for COGNUS_CC_ATTEMPT in 1 2; do "
                    "COGNUS_CC_APPROVE_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id \"$COGNUS_CC_PACKAGE_ID\" 2>&1 || true); "
                    "COGNUS_CC_COMMIT_OUT=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit -o \"$COGNUS_CC_ORDERER_EP\" -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} 2>&1 || true); "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then break 2; fi; "
                    "sleep 2; "
                    "done; "
                    "done; "
                    "fi; "
                    "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX_ALT peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1 && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null | grep -F {needle} >/dev/null\" >/dev/null 2>&1; then "
                    "printf '%s%s\n' 'COGNUS_LOCALMSPID_USED:' \"$COGNUS_PEER_LOCAL_MSPID\" >&2; "
                    "printf '%s%s\n' 'COGNUS_MSP_PATH_USED:' \"$COGNUS_PEER_ADMIN_MSPCONFIGPATH\" >&2; "
                    "printf '%s%s\n' 'COGNUS_MSP_FALLBACK_REASON:' \"$COGNUS_MSP_FALLBACK_REASON\" >&2; "
                    "COGNUS_MSP_SUMMARY=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH sh -lc 'for f in \"$CORE_PEER_MSPCONFIGPATH/signcerts/cert.pem\" \"$CORE_PEER_MSPCONFIGPATH/signcerts/admin.pem\" \"$CORE_PEER_MSPCONFIGPATH/keystore/key.pem\" \"$CORE_PEER_MSPCONFIGPATH/cacerts/cacert.pem\" \"$CORE_PEER_MSPCONFIGPATH/admincerts/admincert.pem\" \"$CORE_PEER_MSPCONFIGPATH/config.yaml\"; do if [ -s \"$f\" ]; then echo \"ok:$f\"; else echo \"missing:$f\"; fi; done' 2>/dev/null | tr '\n' '|' | cut -c1-380); "
                    "[ -n \"$COGNUS_MSP_SUMMARY\" ] && printf '%s%s\n' 'COGNUS_MSP_SUMMARY:' \"$COGNUS_MSP_SUMMARY\" >&2 || true; "
                    "COGNUS_CC_LAST_ERR=$(printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | tail -n 8 | tr '\n' '|' | cut -c1-380); "
                    "[ -n \"$COGNUS_CC_LAST_ERR\" ] && printf '%s%s\n' 'COGNUS_LIFECYCLE_LAST_ERR:' \"$COGNUS_CC_LAST_ERR\" >&2 || true; "
                    "if printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -Eqi 'FORBIDDEN|policy requires|access denied'; then printf '%s\n' COGNUS_LIFECYCLE_ERROR_CLASS:msp_policy_forbidden >&2; fi; "
                    "if printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -Eqi 'x509|tls|certificate'; then printf '%s\n' COGNUS_LIFECYCLE_ERROR_CLASS:tls_orderer >&2; fi; "
                    "if printf '%s\\n%s\\n' \"$COGNUS_CC_APPROVE_OUT\" \"$COGNUS_CC_COMMIT_OUT\" | grep -Eqi 'system channel|channel creation request not allowed'; then printf '%s\n' COGNUS_LIFECYCLE_ERROR_CLASS:orderer_channel_state >&2; fi; "
                    "{failure} fi; "
                    "else {failure} fi; "
                    "fi; "
                ).format(
                    channel=channel_q,
                    needle=shlex.quote("Name: {}".format(normalized_chaincode_id)),
                    chaincode=shlex.quote(normalized_chaincode_id),
                    channel_plain=normalized_channel_id,
                    chaincode_plain=normalized_chaincode_id,
                    chaincode_pattern=normalized_chaincode_id,
                    chaincode_version=shlex.quote(
                        chaincode_artifact_metadata.get(
                            "version", RUNBOOK_CHAINCODE_AUTODEFINE_VERSION
                        )
                    ),
                    chaincode_version_plain=str(
                        chaincode_artifact_metadata.get(
                            "version", RUNBOOK_CHAINCODE_AUTODEFINE_VERSION
                        )
                    ),
                    chaincode_sequence=shlex.quote(
                        chaincode_artifact_metadata.get(
                            "sequence", RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE
                        )
                    ),
                    run_id_plain=str(run_state.get("run_id", "") or "").strip(),
                    orderer_host_override=shlex.quote(RUNBOOK_ORDERER_TLS_HOST_OVERRIDE),
                    chaincode_bootstrap_lookup_step=chaincode_bootstrap_lookup_step,
                    chaincode_package_lookup_step=chaincode_package_lookup_step,
                    chaincode_package_id_lookup_step=chaincode_package_id_lookup_step,
                    failure=lifecycle_failure_clause,
                )
            )

    runtime_gateway_host_dir = _resolve_apigateway_runtime_host_dir(run_state, host_row)
    cfg_msp_refresh_step = ""
    if runtime_gateway_host_dir:
        cfg_msp_refresh_step = (
            "COGNUS_RUNTIME_GATEWAY_HOST_DIR={gateway_host}; "
            "COGNUS_CFG_MSP_HOST={cfg_msp_host}; "
            "if [ -n \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR\" ] && [ -d \"$COGNUS_CFG_MSP_HOST\" ]; then "
            "mkdir -p \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp\" >/dev/null 2>&1 || true; "
            "rm -rf \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp\"/* >/dev/null 2>&1 || true; "
            "cp -a \"$COGNUS_CFG_MSP_HOST/.\" \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/\" >/dev/null 2>&1 || true; "
            "if [ ! -s \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/signcerts/cert.pem\" ] && [ -s \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/signcerts/peer.pem\" ]; then cp \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/signcerts/peer.pem\" \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/signcerts/cert.pem\" >/dev/null 2>&1 || true; fi; "
            "if [ ! -s \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/keystore/key.pem\" ]; then COGNUS_MSP_KEY=$(find \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/keystore\" -type f 2>/dev/null | head -n 1); if [ -n \"$COGNUS_MSP_KEY\" ]; then cp \"$COGNUS_MSP_KEY\" \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/keystore/key.pem\" >/dev/null 2>&1 || true; fi; fi; "
            "if [ ! -s \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/admincerts/admincert.pem\" ] && [ -s \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/signcerts/cert.pem\" ]; then cp \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/signcerts/cert.pem\" \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/admincerts/admincert.pem\" >/dev/null 2>&1 || true; fi; "
            "if [ ! -s \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/cacerts/cacert.pem\" ]; then COGNUS_MSP_CA=$(find \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/cacerts\" \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/tlscacerts\" -type f 2>/dev/null | head -n 1); if [ -n \"$COGNUS_MSP_CA\" ]; then cp \"$COGNUS_MSP_CA\" \"$COGNUS_RUNTIME_GATEWAY_HOST_DIR/msp/cacerts/cacert.pem\" >/dev/null 2>&1 || true; fi; fi; "
            "fi; "
        ).format(
            gateway_host=shlex.quote(runtime_gateway_host_dir),
            cfg_msp_host=shlex.quote(
                f"/tmp/cognus-run-{str(run_state.get('run_id', '') or '').strip()}-{primary_channel_id}/cfg-gen/msp"
            ),
        )

    if checks:
        return resolve_peer_step + resolve_orderer_step + cfg_msp_refresh_step + "".join(checks)

    runtime_orderer_tlsca_host_q = (
        shlex.quote(f"{runtime_gateway_host_dir}/orderer-tlsca.pem")
        if runtime_gateway_host_dir
        else "''"
    )

    peer_container = _resolve_runtime_container_name_for_node_type(
        run_state,
        host_row,
        "peer",
    )
    orderer_container = _resolve_runtime_container_name_for_node_type(
        run_state,
        host_row,
        "orderer",
    )
    peer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "peer")
    peer_node_id = str(peer_host_row.get("node_id", "") or "").strip()
    orderer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "orderer")
    orderer_node_id = str(orderer_host_row.get("node_id", "") or "").strip()

    peer_container_q = shlex.quote(peer_container)
    orderer_container_q = shlex.quote(orderer_container)
    run_id_q = shlex.quote(str(run_state.get("run_id", "") or "").strip())
    peer_node_id_q = shlex.quote(peer_node_id)
    orderer_node_id_q = shlex.quote(orderer_node_id)

    resolve_peer_step = (
        "COGNUS_PEER_CONTAINER={peer}; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ] || ! docker inspect \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --filter label=cognus.node_id={peer_node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then "
        "COGNUS_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -n \"$COGNUS_PEER_CONTAINER\" ] && ! docker ps --format '{{{{.Names}}}}' | grep -Fx \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1; then "
        "docker start \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1 || true; "
        "fi; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then "
        "COGNUS_PEER_CONTAINER=$(docker ps --format '{{{{.Names}}}} {{{{.Image}}}}' | awk '/fabric-peer/ {{{{print $1; exit}}}}'); "
        "fi; "
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then "
        "printf '%s\\n' COGNUS_CHAINCODE_LIFECYCLE_INVALID:missing_peer >&2; "
        "exit 126; "
        "fi; "
    ).format(
        peer=peer_container_q,
        run_id=run_id_q,
        peer_node_id=peer_node_id_q,
    )

    resolve_orderer_step = (
        "COGNUS_ORDERER_CONTAINER={orderer}; "
        "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ] || ! docker inspect \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=orderer --filter label=cognus.node_id={orderer_node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ]; then "
        "COGNUS_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=orderer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; "
        "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ] && ! docker ps --format '{{{{.Names}}}}' | grep -Fx \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1; then "
        "docker start \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; "
        "fi; "
        "if [ -z \"$COGNUS_ORDERER_CONTAINER\" ]; then "
        "COGNUS_ORDERER_CONTAINER=$(docker ps --format '{{{{.Names}}}} {{{{.Image}}}}' | awk '/fabric-orderer/ {{{{print $1; exit}}}}'); "
        "fi; "
    ).format(
        orderer=orderer_container_q,
        run_id=run_id_q,
        orderer_node_id=orderer_node_id_q,
    )

    checks = []
    for channel_id in channel_ids:
        normalized_channel_id = str(channel_id or "").strip().lower()
        if not normalized_channel_id:
            continue
        for chaincode_id in chaincode_ids:
            normalized_chaincode_id = str(chaincode_id or "").strip().lower()
            if not normalized_chaincode_id:
                continue
            channel_q = shlex.quote(normalized_channel_id)
            chaincode_q = shlex.quote(normalized_chaincode_id)
            channel_membership_check = (
                "peer channel list 2>/dev/null | grep -F {channel} >/dev/null"
            ).format(channel=channel_q)
            lifecycle_check = (
                "peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null "
                "| grep -F {needle} >/dev/null"
            ).format(
                channel=channel_q,
                needle=shlex.quote("Name: {}".format(normalized_chaincode_id)),
            )
            if enforce_failures:
                channel_failure_clause = (
                    "if [ ! -s \"$channel_block\" ]; then printf '%s\\n' COGNUS_CHANNEL_BLOCK_ARTIFACT_MISSING:{channel} >&2; fi; "
                    "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                    "docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"ls -1 /var/hyperledger/production/orderer/chains/{channel_raw}/blockfile /var/hyperledger/production/orderer/chains/chains/{channel_raw}/blockfile 2>/dev/null | head -n 1\" >/tmp/cognus-${{COGNUS_RUN_ID}}-orderer-channel-check.txt 2>/dev/null || true; "
                    "ORDERER_CHANNEL_BLOCK_PATH=$(cat /tmp/cognus-${{COGNUS_RUN_ID}}-orderer-channel-check.txt 2>/dev/null | head -n 1); "
                    "if [ -z \"$ORDERER_CHANNEL_BLOCK_PATH\" ]; then printf '%s\\n' COGNUS_ORDERER_CHANNEL_MISSING:{channel} >&2; fi; "
                    "fi; "
                    "printf '%s\\n' COGNUS_CHANNEL_NOT_JOINED:{channel} >&2; "
                    "exit 126; "
                ).format(channel=normalized_channel_id, channel_raw=normalized_channel_id)
                lifecycle_failure_clause = (
                    "printf '%s\\n' COGNUS_CHAINCODE_NOT_COMMITTED:{channel}:{chaincode} >&2; exit 126; "
                ).format(
                    channel=normalized_channel_id,
                    chaincode=normalized_chaincode_id,
                )
            else:
                channel_failure_clause = (
                    "if [ ! -s \"$channel_block\" ]; then printf '%s\\n' COGNUS_CHANNEL_BLOCK_ARTIFACT_MISSING_DEFERRED:{channel} >&2; fi; "
                    "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                    "docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"ls -1 /var/hyperledger/production/orderer/chains/{channel_raw}/blockfile /var/hyperledger/production/orderer/chains/chains/{channel_raw}/blockfile 2>/dev/null | head -n 1\" >/tmp/cognus-${{COGNUS_RUN_ID}}-orderer-channel-check.txt 2>/dev/null || true; "
                    "ORDERER_CHANNEL_BLOCK_PATH=$(cat /tmp/cognus-${{COGNUS_RUN_ID}}-orderer-channel-check.txt 2>/dev/null | head -n 1); "
                    "if [ -z \"$ORDERER_CHANNEL_BLOCK_PATH\" ]; then printf '%s\\n' COGNUS_ORDERER_CHANNEL_MISSING_DEFERRED:{channel} >&2; fi; "
                    "fi; "
                    "printf '%s\\n' COGNUS_CHANNEL_NOT_JOINED_DEFERRED:{channel} >&2; "
                ).format(channel=normalized_channel_id, channel_raw=normalized_channel_id)
                lifecycle_failure_clause = (
                    "printf '%s\\n' COGNUS_CHAINCODE_NOT_COMMITTED_DEFERRED:{channel}:{chaincode} >&2; "
                ).format(
                    channel=normalized_channel_id,
                    chaincode=normalized_chaincode_id,
                )
            chaincode_artifact_metadata = _resolve_chaincode_artifact_metadata(
                run_state,
                normalized_channel_id,
                normalized_chaincode_id,
            )
            chaincode_package_lookup_step = _build_chaincode_package_lookup_step(
                "COGNUS_CC_PACKAGE_HOST",
                chaincode_artifact_metadata.get("search_terms", []),
                chaincode_artifact_metadata.get("candidate_files", []),
            )
            chaincode_package_id_lookup_step = _build_chaincode_package_id_lookup_step(
                "COGNUS_CC_PACKAGE_ID",
                "COGNUS_CC_QUERYINSTALLED",
                chaincode_artifact_metadata.get("search_terms", []),
            )
            chaincode_define_step = ""
            if RUNBOOK_CHAINCODE_AUTODEFINE_ENABLED:
                chaincode_version_q = shlex.quote(
                    chaincode_artifact_metadata.get(
                        "version", RUNBOOK_CHAINCODE_AUTODEFINE_VERSION
                    )
                )
                chaincode_sequence_q = shlex.quote(
                    chaincode_artifact_metadata.get(
                        "sequence", RUNBOOK_CHAINCODE_AUTODEFINE_SEQUENCE
                    )
                )
                orderer_host_override_q = shlex.quote(RUNBOOK_ORDERER_TLS_HOST_OVERRIDE)
                chaincode_define_step = (
                    "{chaincode_package_lookup_step}"
                    "COGNUS_CC_PACKAGE_PEER=/tmp/cognus-${{COGNUS_RUN_ID}}-{channel_raw}-{chaincode_raw}.tar.gz; "
                    "if [ -n \"$COGNUS_CC_PACKAGE_HOST\" ]; then docker cp \"$COGNUS_CC_PACKAGE_HOST\" \"$COGNUS_PEER_CONTAINER:$COGNUS_CC_PACKAGE_PEER\" >/dev/null 2>&1 || true; fi; "
                    "COGNUS_PEER_LOCAL_MSPID=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"awk -F': ' '/^[[:space:]]*localMspId:/ {{print \\$2; exit}}' /etc/hyperledger/fabric/core.yaml | tr -d '[:space:]'\" 2>/dev/null || true); "
                    "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; fi; "
                    "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_CC_PACKAGE_PEER'\" >/dev/null 2>&1; then "
                    "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"CORE_PEER_MSPCONFIGPATH=/etc/hyperledger/fabric/msp CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode install '$COGNUS_CC_PACKAGE_PEER' >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                    "fi; "
                    "COGNUS_CC_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'peer lifecycle chaincode queryinstalled 2>/dev/null || true' 2>/dev/null || true); "
                    "{chaincode_package_id_lookup_step}"
                    "if [ -n \"$COGNUS_CC_PACKAGE_ID\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                    "COGNUS_ORDERER_ENDPOINT=${{COGNUS_ORDERER_ENDPOINT:-$COGNUS_ORDERER_CONTAINER:7050}}; "
                    "COGNUS_LC_ORDERER_FLAGS=\"-o $COGNUS_ORDERER_ENDPOINT\"; "
                    "if [ -n \"$COGNUS_ORDERER_CAFILE\" ]; then COGNUS_LC_ORDERER_FLAGS=\"$COGNUS_LC_ORDERER_FLAGS --tls --cafile $COGNUS_ORDERER_CAFILE --ordererTLSHostnameOverride {orderer_host_override}\"; fi; "
                    "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"CORE_PEER_MSPCONFIGPATH=/etc/hyperledger/fabric/msp CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode approveformyorg $COGNUS_LC_ORDERER_FLAGS -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} --package-id $COGNUS_CC_PACKAGE_ID >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                    "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"CORE_PEER_MSPCONFIGPATH=/etc/hyperledger/fabric/msp CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode commit $COGNUS_LC_ORDERER_FLAGS -C {channel} -n {chaincode} --version {chaincode_version} --sequence {chaincode_sequence} >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                    "fi; "
                ).format(
                    channel=channel_q,
                    channel_raw=normalized_channel_id,
                    chaincode=chaincode_q,
                    chaincode_raw=normalized_chaincode_id,
                    chaincode_version=chaincode_version_q,
                    chaincode_sequence=chaincode_sequence_q,
                    orderer_host_override=orderer_host_override_q,
                    chaincode_package_lookup_step=chaincode_package_lookup_step,
                    chaincode_package_id_lookup_step=chaincode_package_id_lookup_step,
                )
            channel_fetch_and_join = (
                "COGNUS_TMP_ROOT=/tmp/cognus-${{COGNUS_RUN_ID}}-{channel_raw}; "
                "mkdir -p \"$COGNUS_TMP_ROOT\" >/dev/null 2>&1 || true; "
                "channel_block=$COGNUS_TMP_ROOT/channel.block; "
                "channel_tx=$COGNUS_TMP_ROOT/channel.tx; "
                "COGNUS_PEER_CHANNEL_BLOCK=/tmp/cognus/channel.block; "
                "COGNUS_PEER_CHANNEL_TX=/tmp/cognus/channel.tx; "
                "rm -f \"$channel_block\" \"$channel_tx\" >/dev/null 2>&1 || true; "
                "COGNUS_ORDERER_CAFILE=''; "
                "COGNUS_ORDERER_CA_PEER_TMP=/tmp/cognus-orderer-ca-{channel_raw}.crt; "
                "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "COGNUS_ORDERER_CA_HOST=''; "
                "for candidate in /etc/hyperledger/fabric/tls/ca.crt /etc/hyperledger/fabric/msp/tlscacerts/tlsca.pem /etc/hyperledger/fabric/msp/tlscacerts/tlsroot.pem /var/lib/cognus/msp/tlscacerts/tlsca.pem /var/lib/cognus/msp/tlscacerts/orderer-tls-ca.crt /var/lib/cognus/msp/cacerts/cacert.pem /var/lib/cognus/msp/cacerts/orderer-ca.crt; do "
                "if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_ORDERER_CA_HOST=$candidate; break; fi; "
                "done; "
                "if [ -n \"$COGNUS_ORDERER_CA_HOST\" ]; then "
                "COGNUS_ORDERER_CA_TMP=/tmp/cognus-${{COGNUS_RUN_ID}}-{channel_raw}-orderer-ca.crt; "
                "rm -f \"$COGNUS_ORDERER_CA_TMP\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_ORDERER_CA_HOST\" \"$COGNUS_ORDERER_CA_TMP\" >/dev/null 2>&1 || true; "
                "if [ -s \"$COGNUS_ORDERER_CA_TMP\" ]; then docker cp \"$COGNUS_ORDERER_CA_TMP\" \"$COGNUS_PEER_CONTAINER:$COGNUS_ORDERER_CA_PEER_TMP\" >/dev/null 2>&1 || true; fi; "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_ORDERER_CA_PEER_TMP'\" >/dev/null 2>&1; then COGNUS_ORDERER_CAFILE=$COGNUS_ORDERER_CA_PEER_TMP; fi; "
                "fi; "
                "fi; "
                "if [ -z \"$COGNUS_ORDERER_CAFILE\" ]; then "
                "COGNUS_ORDERER_CA_HOST_RUNTIME={runtime_orderer_tlsca_host}; "
                "if [ -s \"$COGNUS_ORDERER_CA_HOST_RUNTIME\" ]; then "
                "docker cp \"$COGNUS_ORDERER_CA_HOST_RUNTIME\" \"$COGNUS_PEER_CONTAINER:$COGNUS_ORDERER_CA_PEER_TMP\" >/dev/null 2>&1 || true; "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_ORDERER_CA_PEER_TMP'\" >/dev/null 2>&1; then COGNUS_ORDERER_CAFILE=$COGNUS_ORDERER_CA_PEER_TMP; fi; "
                "fi; "
                "fi; "
                "if [ -z \"$COGNUS_ORDERER_CAFILE\" ]; then "
                "for candidate in $COGNUS_ORDERER_CA_PEER_TMP /etc/hyperledger/fabric/tls/ca.crt /etc/hyperledger/fabric/msp/tlscacerts/tlsca.pem /etc/hyperledger/fabric/msp/tlscacerts/tlsroot.pem /var/lib/cognus/msp/tlscacerts/tlsca.pem /var/lib/cognus/msp/tlscacerts/orderer-tls-ca.crt /var/lib/cognus/msp/cacerts/cacert.pem /var/lib/cognus/msp/cacerts/orderer-ca.crt; do "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_ORDERER_CAFILE=$candidate; break; fi; "
                "done; "
                "fi; "
                "COGNUS_PEER_CLIENT_CERT=''; COGNUS_PEER_CLIENT_KEY=''; "
                "for candidate in /etc/hyperledger/fabric/tls/server.crt /etc/hyperledger/fabric/tls/client.crt /etc/hyperledger/fabric/msp/signcerts/cert.pem /etc/hyperledger/fabric/msp/signcerts/peer.pem /var/lib/cognus/msp/signcerts/cert.pem /var/lib/cognus/msp/signcerts/admin.pem; do "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_PEER_CLIENT_CERT=$candidate; break; fi; "
                "done; "
                "for candidate in /etc/hyperledger/fabric/tls/server.key /etc/hyperledger/fabric/tls/client.key /etc/hyperledger/fabric/msp/keystore/key.pem /var/lib/cognus/msp/keystore/key.pem /var/lib/cognus/msp/keystore/keystore/key.pem /var/lib/cognus/msp/keystore/keystore/admin_key.pem; do "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_PEER_CLIENT_KEY=$candidate; break; fi; "
                "done; "
                "COGNUS_TLS_CLIENT_FLAGS=''; "
                "if [ -n \"$COGNUS_PEER_CLIENT_CERT\" ] && [ -n \"$COGNUS_PEER_CLIENT_KEY\" ]; then COGNUS_TLS_CLIENT_FLAGS=\"--clientauth --certfile $COGNUS_PEER_CLIENT_CERT --keyfile $COGNUS_PEER_CLIENT_KEY\"; fi; "
                "COGNUS_ORDERER_ENDPOINT=$COGNUS_ORDERER_CONTAINER:7050; "
                "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "COGNUS_ORDERER_IP=$(docker inspect --format '{{{{range .NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}' \"$COGNUS_ORDERER_CONTAINER\" 2>/dev/null | head -n 1); "
                "if [ -n \"$COGNUS_ORDERER_IP\" ]; then COGNUS_ORDERER_ENDPOINT=$COGNUS_ORDERER_IP:7050; fi; "
                "fi; "
                "COGNUS_FETCH_FLAGS=\"--tls --ordererTLSHostnameOverride {orderer_host_override}\"; "
                "if [ -n \"$COGNUS_ORDERER_CAFILE\" ]; then COGNUS_FETCH_FLAGS=\"$COGNUS_FETCH_FLAGS --cafile $COGNUS_ORDERER_CAFILE\"; fi; "
                "if [ -n \"$COGNUS_TLS_CLIENT_FLAGS\" ]; then COGNUS_FETCH_FLAGS=\"$COGNUS_FETCH_FLAGS $COGNUS_TLS_CLIENT_FLAGS\"; fi; "
                "COGNUS_FETCH_FLAGS_TLS=\"--tls\"; "
                "if [ -n \"$COGNUS_ORDERER_CAFILE\" ]; then COGNUS_FETCH_FLAGS_TLS=\"$COGNUS_FETCH_FLAGS_TLS --cafile $COGNUS_ORDERER_CAFILE\"; fi; "
                "if [ -n \"$COGNUS_TLS_CLIENT_FLAGS\" ]; then COGNUS_FETCH_FLAGS_TLS=\"$COGNUS_FETCH_FLAGS_TLS $COGNUS_TLS_CLIENT_FLAGS\"; fi; "
                "COGNUS_FETCH_FLAGS_PLAIN=''; "
                "if [ ! -s \"$channel_block\" ]; then "
                "for COGNUS_BLOCK_ROOT in /tmp/cognus /var/cognus \"${{HOME}}/.cognus\" \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain\"; do "
                "[ -d \"$COGNUS_BLOCK_ROOT\" ] || continue; "
                "COGNUS_FOUND_BLOCK=$(find \"$COGNUS_BLOCK_ROOT\" -maxdepth 8 -type f \\( -name '*{channel_raw}*.block' -o -name '*{channel_raw}*genesis*.block' -o -name '*{channel_raw}*genesis*.pb' \\) 2>/dev/null | head -n 1); "
                "if [ -n \"$COGNUS_FOUND_BLOCK\" ] && [ -s \"$COGNUS_FOUND_BLOCK\" ]; then cp \"$COGNUS_FOUND_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; fi; "
                "[ -s \"$channel_block\" ] && break; "
                "done; "
                "fi; "
                "if [ ! -s \"$channel_block\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$channel_block' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$channel_block' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS_TLS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$channel_block' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS_PLAIN >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "fi; "
                "if [ ! -s \"$channel_block\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "mkdir -p \"$COGNUS_TMP_ROOT\" >/dev/null 2>&1 || true; "
                "docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || docker pull hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || true; "
                "COGNUS_CONFIGTX_HOST=''; "
                "for candidate in /var/cognus/configtx/configtx.yaml /var/cognus/channel-artifacts/configtx.yaml \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain/automation/resources/configtx/configtx.yaml\" /etc/hyperledger/fabric/configtx.yaml; do "
                "if [ -s \"$candidate\" ]; then COGNUS_CONFIGTX_HOST=$candidate; break; fi; "
                "done; "
                "if [ -z \"$COGNUS_CONFIGTX_HOST\" ]; then "
                "for COGNUS_CFG_ROOT in \"${{HOME}}/UFG-Fabric-Orchestrator-Blockchain\" \"${{HOME}}\" /workspace /var/cognus; do "
                "[ -d \"$COGNUS_CFG_ROOT\" ] || continue; "
                "COGNUS_CFG_FOUND=$(find \"$COGNUS_CFG_ROOT\" -maxdepth 8 -type f \\( -path '*/configtx/configtx.yaml' -o -path '*/fabric/configtx.yaml' -o -path '*/fabric/config/configtx.yaml' -o -name 'configtx.yaml' \\) 2>/dev/null | head -n 1); "
                "if [ -n \"$COGNUS_CFG_FOUND\" ] && [ -s \"$COGNUS_CFG_FOUND\" ]; then COGNUS_CONFIGTX_HOST=$COGNUS_CFG_FOUND; break; fi; "
                "done; "
                "fi; "
                "COGNUS_CONFIGTX_HOST_COPY=$COGNUS_TMP_ROOT/configtx-${{COGNUS_RUN_ID}}-{channel_raw}.yaml; "
                "rm -f \"$COGNUS_CONFIGTX_HOST_COPY\" >/dev/null 2>&1 || true; "
                "if [ -n \"$COGNUS_CONFIGTX_HOST\" ] && [ -s \"$COGNUS_CONFIGTX_HOST\" ]; then cp \"$COGNUS_CONFIGTX_HOST\" \"$COGNUS_CONFIGTX_HOST_COPY\" >/dev/null 2>&1 || true; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_HOST_COPY\" ]; then docker cp \"$COGNUS_PEER_CONTAINER:/etc/hyperledger/fabric/configtx.yaml\" \"$COGNUS_CONFIGTX_HOST_COPY\" >/dev/null 2>&1 || true; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_HOST_COPY\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/configtx.yaml\" \"$COGNUS_CONFIGTX_HOST_COPY\" >/dev/null 2>&1 || true; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_HOST_COPY\" ]; then printf '%s' '{configtx_fallback_b64}' | base64 -d > \"$COGNUS_CONFIGTX_HOST_COPY\" 2>/dev/null || true; fi; "
                "if [ ! -s \"$COGNUS_CONFIGTX_HOST_COPY\" ]; then printf '%s\\n' COGNUS_CONFIGTX_SOURCE_MISSING:{channel} >&2; fi; "
                "if [ -s \"$COGNUS_CONFIGTX_HOST_COPY\" ]; then cp \"$COGNUS_CONFIGTX_HOST_COPY\" \"$COGNUS_TMP_ROOT/configtx.yaml\" >/dev/null 2>&1 || true; fi; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/tls/server.crt\" \"$COGNUS_TMP_ROOT/orderer-tls.crt\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"cfg_dir=/tmp/cognus/configtx; mkdir -p \\\"$cfg_dir\\\"; cfg_src=/tmp/cognus/configtx.yaml; if [ -s \\\"$cfg_src\\\" ]; then cp \\\"$cfg_src\\\" \\\"$cfg_dir/configtx.yaml\\\"; elif [ -s /etc/hyperledger/fabric/configtx.yaml ]; then cp /etc/hyperledger/fabric/configtx.yaml \\\"$cfg_dir/configtx.yaml\\\"; fi; if [ ! -d \\\"$cfg_dir/msp\\\" ]; then if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"$cfg_dir/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"$cfg_dir/msp\\\" >/dev/null 2>&1 || true; fi; fi; if [ -s \\\"$cfg_dir/configtx.yaml\\\" ]; then sed -E -i 's#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: msp#g; s#MSPDir:[[:space:]]*/etc/hyperledger/fabric/msp#MSPDir: msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: msp#g; s#path/to/ClientTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#path/to/ServerTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#/etc/hyperledger/fabric/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#/var/lib/cognus/tls/server.crt#/tmp/cognus/orderer-tls.crt#g' \\\"$cfg_dir/configtx.yaml\\\"; sed -i -E \\\"s#raft[0-9]+\\\\.example\\\\.com#$COGNUS_ORDERER_CONTAINER#g\\\" \\\"$cfg_dir/configtx.yaml\\\"; sed -i -E \\\"s#cognusrb-[A-Za-z0-9-]*-ordere#$COGNUS_ORDERER_CONTAINER#g\\\" \\\"$cfg_dir/configtx.yaml\\\"; sed -i -E \\\"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g; s#127\\\\.0\\\\.0\\\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\\\.0\\\\.0\\\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g\\\" \\\"$cfg_dir/configtx.yaml\\\"; sed -i -E \\\"s#(Host:[[:space:]]*\\\").*(\\\"[[:space:]]*)#\\\\1$COGNUS_ORDERER_CONTAINER\\\\2#g\\\" \\\"$cfg_dir/configtx.yaml\\\"; sed -i -E \\\"s#(-[[:space:]]*)([A-Za-z0-9._-]+):7050#\\\\1$COGNUS_ORDERER_CONTAINER:7050#g\\\" \\\"$cfg_dir/configtx.yaml\\\"; sed -i -E \\\"s#(-[[:space:]]*\\\").*:7050(\\\"[[:space:]]*)#\\\\1$COGNUS_ORDERER_CONTAINER:7050\\\\2#g\\\" \\\"$cfg_dir/configtx.yaml\\\"; {configtx_profile_autodetect_cfg_dir_step} chmod 666 /tmp/cognus/channel.block /tmp/cognus/channel.tx /tmp/cognus/orderer-tls.crt >/dev/null 2>&1 || true; fi\" >/dev/null 2>&1 || true; "
                "if [ ! -s \"$channel_block\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "COGNUS_DIRECT_CFG=$COGNUS_TMP_ROOT/configtx-direct-${{COGNUS_RUN_ID}}-{channel_raw}.yaml; "
                "rm -f \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/configtx.yaml\" \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "if [ ! -s \"$COGNUS_DIRECT_CFG\" ]; then printf '%s' '{configtx_fallback_b64}' | base64 -d > \"$COGNUS_DIRECT_CFG\" 2>/dev/null || true; fi; "
                "if [ ! -s \"$COGNUS_DIRECT_CFG\" ]; then printf '%s\\n' COGNUS_CONFIGTX_DIRECT_MISSING:{channel} >&2; fi; "
                "if [ -s \"$COGNUS_DIRECT_CFG\" ]; then "
                "cp \"$COGNUS_DIRECT_CFG\" \"$COGNUS_TMP_ROOT/configtx-direct.yaml\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/tls/server.crt\" \"$COGNUS_TMP_ROOT/orderer-tls.crt\" >/dev/null 2>&1 || true; "
                "sed -E -i 's#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: msp#g; s#MSPDir:[[:space:]]*/etc/hyperledger/fabric/msp#MSPDir: msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: msp#g; s#path/to/ClientTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#path/to/ServerTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#/etc/hyperledger/fabric/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#/var/lib/cognus/tls/server.crt#/tmp/cognus/orderer-tls.crt#g' \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g\" \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#raft[0-9]+\\\\.example\\\\.com#$COGNUS_ORDERER_CONTAINER#g\" \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#(Host:[[:space:]]*\\\").*(\\\"[[:space:]]*)#\\\\1$COGNUS_ORDERER_CONTAINER\\\\2#g\" \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#(-[[:space:]]*)([A-Za-z0-9._-]+):7050#\\\\1$COGNUS_ORDERER_CONTAINER:7050#g\" \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\.0\\.0\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#(-[[:space:]]*\\\").*:7050(\\\"[[:space:]]*)#\\\\1$COGNUS_ORDERER_CONTAINER:7050\\\\2#g\" \"$COGNUS_DIRECT_CFG\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"set -e; cfg_dir=/tmp/cognus/configtx-direct; mkdir -p \\\"\\$cfg_dir\\\"; cfg_src=/tmp/cognus/configtx-direct.yaml; cp \\\"\\$cfg_src\\\" \\\"\\$cfg_dir/configtx.yaml\\\"; if [ ! -d \\\"\\$cfg_dir/msp\\\" ]; then if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg_dir/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg_dir/msp\\\" >/dev/null 2>&1 || true; fi; fi; {configtx_profile_autodetect_cfg_dir_step} chmod 666 /tmp/cognus/channel.block /tmp/cognus/channel.tx /tmp/cognus/orderer-tls.crt >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "fi; "
                "fi; "
                "if [ -s \"$channel_block\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "COGNUS_BLOCK_INSPECT=$COGNUS_TMP_ROOT/channel-block.inspect.json; "
                "rm -f \"$COGNUS_BLOCK_INSPECT\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"configtxgen -inspectBlock /tmp/cognus/channel.block > /tmp/cognus/channel-block.inspect.json 2>/dev/null || true\" >/dev/null 2>&1 || true; "
                "if grep -F '\"type\": \"solo\"' \"$COGNUS_BLOCK_INSPECT\" >/dev/null 2>&1; then "
                "COGNUS_FORCE_ETCD_CFG=$COGNUS_TMP_ROOT/configtx-force-etcdraft-${{COGNUS_RUN_ID}}-{channel_raw}.yaml; "
                "rm -f \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "printf '%s' '{configtx_fallback_b64}' | base64 -d > \"$COGNUS_FORCE_ETCD_CFG\" 2>/dev/null || true; "
                "sed -E -i 's#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: msp#g; s#MSPDir:[[:space:]]*/etc/hyperledger/fabric/msp#MSPDir: msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: msp#g; s#/etc/hyperledger/fabric/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#/var/lib/cognus/tls/server.crt#/tmp/cognus/orderer-tls.crt#g; s#path/to/ClientTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g; s#path/to/ServerTLSCert[0-9]+#/tmp/cognus/orderer-tls.crt#g' \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g\" \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#raft[0-9]+\\.example\\.com#$COGNUS_ORDERER_CONTAINER#g\" \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#(Host:[[:space:]]*\").*(\"[[:space:]]*)#\\1$COGNUS_ORDERER_CONTAINER\\2#g\" \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#(-[[:space:]]*)([A-Za-z0-9._-]+):7050#\\1$COGNUS_ORDERER_CONTAINER:7050#g\" \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\.0\\.0\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#(-[[:space:]]*\\\").*:7050(\\\"[[:space:]]*)#\\1$COGNUS_ORDERER_CONTAINER:7050\\2#g\" \"$COGNUS_FORCE_ETCD_CFG\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"cfg=/tmp/cognus/configtx-force; rm -rf \\\"\\$cfg\\\"; mkdir -p \\\"\\$cfg\\\"; cp /tmp/cognus/$(basename \"$COGNUS_FORCE_ETCD_CFG\") \\\"\\$cfg/configtx.yaml\\\"; if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; fi; {configtx_profile_autodetect_cfg_step} chmod 666 /tmp/cognus/channel.block /tmp/cognus/channel.tx >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "fi; "
                "fi; "
                "chmod 644 \"$channel_block\" \"$channel_tx\" >/dev/null 2>&1 || true; "
                "docker run --rm -v \"$COGNUS_TMP_ROOT\":/tmp/cognus-fix alpine:3.20 sh -lc \"chmod -R a+rwX /tmp/cognus-fix >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "if [ ! -s \"$channel_tx\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "COGNUS_TX_CFG=$COGNUS_TMP_ROOT/configtx-tx-${{COGNUS_RUN_ID}}-{channel_raw}.yaml; "
                "COGNUS_TX_CFG_LOCAL=$COGNUS_TMP_ROOT/configtx-tx-local-${{COGNUS_RUN_ID}}-{channel_raw}.yaml; "
                "COGNUS_TX_LOG=$COGNUS_TMP_ROOT/configtxgen-tx-${{COGNUS_RUN_ID}}-{channel_raw}.log; "
                "rm -f \"$COGNUS_TX_CFG\" \"$COGNUS_TX_CFG_LOCAL\" \"$COGNUS_TX_LOG\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_ORDERER_CONTAINER:/etc/hyperledger/fabric/configtx.yaml\" \"$COGNUS_TX_CFG\" >/dev/null 2>&1 || true; "
                "if [ ! -s \"$COGNUS_TX_CFG\" ]; then printf '%s' '{configtx_fallback_b64}' | base64 -d > \"$COGNUS_TX_CFG\" 2>/dev/null || true; fi; "
                "if [ -s \"$COGNUS_TX_CFG\" ]; then "
                "cp \"$COGNUS_TX_CFG\" \"$COGNUS_TX_CFG_LOCAL\" >/dev/null 2>&1 || true; "
                "sed -E -i 's#MSPDir:[[:space:]]*/msp/SampleOrg#MSPDir: msp#g; s#MSPDir:[[:space:]]*/etc/hyperledger/fabric/msp#MSPDir: msp#g; s#MSPDir:[[:space:]]*msp#MSPDir: msp#g' \"$COGNUS_TX_CFG_LOCAL\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#SampleOrg-orderer#$COGNUS_ORDERER_CONTAINER#g; s#raft[0-9]+\\.example\\.com#$COGNUS_ORDERER_CONTAINER#g; s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#localhost:7050#$COGNUS_ORDERER_CONTAINER:7050#g; s#0\\.0\\.0\\.0:7050#$COGNUS_ORDERER_CONTAINER:7050#g\" \"$COGNUS_TX_CFG_LOCAL\" >/dev/null 2>&1 || true; "
                "sed -i -E \"s#(-[[:space:]]*)([A-Za-z0-9._-]+):7050#\\\\1$COGNUS_ORDERER_CONTAINER:7050#g; s#(-[[:space:]]*\\\").*:7050(\\\"[[:space:]]*)#\\\\1$COGNUS_ORDERER_CONTAINER:7050\\\\2#g\" \"$COGNUS_TX_CFG_LOCAL\" >/dev/null 2>&1 || true; "
                "docker run --rm --volumes-from \"$COGNUS_ORDERER_CONTAINER\" --volumes-from \"$COGNUS_PEER_CONTAINER\" -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"cfg=/tmp/cognus/cfg-tx; mkdir -p \\\"\\$cfg\\\"; cp /tmp/cognus/$(basename \"$COGNUS_TX_CFG_LOCAL\") \\\"\\$cfg/configtx.yaml\\\"; if [ -d /etc/hyperledger/fabric/msp ]; then cp -a /etc/hyperledger/fabric/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; elif [ -d /var/lib/cognus/msp ]; then cp -a /var/lib/cognus/msp \\\"\\$cfg/msp\\\" >/dev/null 2>&1 || true; fi; {configtx_profile_autodetect_cfg_step}\" >\"$COGNUS_TX_LOG\" 2>&1 || true; "
                "fi; "
                "if [ ! -s \"$channel_tx\" ]; then printf '%s\\n' COGNUS_CHANNEL_TX_MISSING:{channel} >&2; [ -s \"$COGNUS_TX_LOG\" ] && tail -n 1 \"$COGNUS_TX_LOG\" | cut -c1-220 | sed 's/^/COGNUS_CHANNEL_TX_DIAG:/' >&2 || true; fi; "
                "fi; "
                "chmod 644 \"$channel_tx\" \"$channel_block\" >/dev/null 2>&1 || true; "
                "docker run --rm -v \"$COGNUS_TMP_ROOT\":/tmp/cognus-fix alpine:3.20 sh -lc \"chmod -R a+rwX /tmp/cognus-fix >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "if [ ! -s \"$channel_block\" ] && [ -s \"$channel_tx\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"mkdir -p /tmp/cognus >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "docker cp \"$channel_tx\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_TX\" >/dev/null 2>&1 || true; "
                "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CHANNEL_TX'\" >/dev/null 2>&1; then printf '%s\\n' COGNUS_CHANNEL_TX_COPY_MISSING:{channel} >&2; fi; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"if [ -s '$COGNUS_PEER_CHANNEL_TX' ]; then peer channel create -o $COGNUS_ORDERER_ENDPOINT -c {channel} -f '$COGNUS_PEER_CHANNEL_TX' --outputBlock '$COGNUS_PEER_CHANNEL_BLOCK' $COGNUS_FETCH_FLAGS >/dev/null 2>&1; fi\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"if [ -s '$COGNUS_PEER_CHANNEL_TX' ]; then peer channel create -o $COGNUS_ORDERER_ENDPOINT -c {channel} -f '$COGNUS_PEER_CHANNEL_TX' --outputBlock '$COGNUS_PEER_CHANNEL_BLOCK' $COGNUS_FETCH_FLAGS_TLS >/dev/null 2>&1; fi\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; "
                "fi; "
                "if [ -s \"$channel_block\" ]; then "
                "COGNUS_OSN_CA_HOST=''; COGNUS_OSN_CERT_HOST=''; COGNUS_OSN_KEY_HOST=''; "
                "for candidate in /etc/hyperledger/fabric/tls/ca.crt /etc/hyperledger/fabric/msp/tlscacerts/tlsca.pem /var/lib/cognus/msp/tlscacerts/tlsca.pem; do if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_OSN_CA_HOST=$candidate; break; fi; done; "
                "for candidate in /etc/hyperledger/fabric/tls/server.crt /etc/hyperledger/fabric/msp/signcerts/cert.pem /var/lib/cognus/msp/signcerts/cert.pem; do if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_OSN_CERT_HOST=$candidate; break; fi; done; "
                "for candidate in /etc/hyperledger/fabric/tls/server.key /etc/hyperledger/fabric/msp/keystore/key.pem /var/lib/cognus/msp/keystore/key.pem; do if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_OSN_KEY_HOST=$candidate; break; fi; done; "
                "COGNUS_ORDERER_CHANNEL_BLOCK_HOST=$COGNUS_TMP_ROOT/orderer-channel.block; "
                "COGNUS_OSN_CA_HOST_TMP=$COGNUS_TMP_ROOT/osn-ca-${{COGNUS_RUN_ID}}-{channel_raw}.pem; "
                "COGNUS_OSN_CERT_HOST_TMP=$COGNUS_TMP_ROOT/osn-cert-${{COGNUS_RUN_ID}}-{channel_raw}.crt; "
                "COGNUS_OSN_KEY_HOST_TMP=$COGNUS_TMP_ROOT/osn-key-${{COGNUS_RUN_ID}}-{channel_raw}.key; "
                "rm -f \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" \"$COGNUS_OSN_CA_HOST_TMP\" \"$COGNUS_OSN_CERT_HOST_TMP\" \"$COGNUS_OSN_KEY_HOST_TMP\" >/dev/null 2>&1 || true; "
                "cp \"$channel_block\" \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "if [ -n \"$COGNUS_OSN_CA_HOST\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_OSN_CA_HOST\" \"$COGNUS_OSN_CA_HOST_TMP\" >/dev/null 2>&1 || true; fi; "
                "if [ -n \"$COGNUS_OSN_CERT_HOST\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_OSN_CERT_HOST\" \"$COGNUS_OSN_CERT_HOST_TMP\" >/dev/null 2>&1 || true; fi; "
                "if [ -n \"$COGNUS_OSN_KEY_HOST\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_OSN_KEY_HOST\" \"$COGNUS_OSN_KEY_HOST_TMP\" >/dev/null 2>&1 || true; fi; "
                "chmod 644 \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" \"$COGNUS_OSN_CA_HOST_TMP\" \"$COGNUS_OSN_CERT_HOST_TMP\" \"$COGNUS_OSN_KEY_HOST_TMP\" >/dev/null 2>&1 || true; "
                "docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || docker pull hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || true; "
                "if [ -s \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" ]; then COGNUS_OSN_TLS_ARGS=''; if [ -s \"$COGNUS_OSN_CA_HOST_TMP\" ] && [ -s \"$COGNUS_OSN_CERT_HOST_TMP\" ] && [ -s \"$COGNUS_OSN_KEY_HOST_TMP\" ]; then COGNUS_OSN_TLS_ARGS=\"--ca-file $COGNUS_OSN_CA_HOST_TMP --client-cert $COGNUS_OSN_CERT_HOST_TMP --client-key $COGNUS_OSN_KEY_HOST_TMP\"; fi; docker run --rm --network container:$COGNUS_ORDERER_CONTAINER -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"for admin_ep in 127.0.0.1:9443 127.0.0.1:7053; do osnadmin channel join --channelID {channel} --config-block /tmp/cognus/orderer-channel.block -o \\\"$admin_ep\\\" $COGNUS_OSN_TLS_ARGS >/dev/null 2>&1 && exit 0; done; exit 0\" >/dev/null 2>&1 || true; fi; "
                "if [ -s \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" ]; then "
                "COGNUS_ORDERER_CHANNEL_BLOCK_HOST=$COGNUS_TMP_ROOT/orderer-channel.block; "
                "rm -f \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "cp \"$channel_block\" \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "chmod 644 \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "docker image inspect curlimages/curl:8.6.0 >/dev/null 2>&1 || docker pull curlimages/curl:8.6.0 >/dev/null 2>&1 || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER -v \"$COGNUS_TMP_ROOT\":/tmp/cognus curlimages/curl:8.6.0 -sS -X POST http://127.0.0.1:9443/participation/v1/channels -F \"config-block=@/tmp/cognus/orderer-channel.block\" >/dev/null 2>&1 || true; "
                "fi; "
                "docker cp \"$channel_block\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS_TLS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS_PLAIN >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; "
                "fi; "
                "if [ ! -s \"$channel_block\" ] && [ -s \"$channel_tx\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"mkdir -p /tmp/cognus >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; "
                "docker cp \"$channel_tx\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_TX\" >/dev/null 2>&1 || true; "
                "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CHANNEL_TX'\" >/dev/null 2>&1; then printf '%s\\n' COGNUS_CHANNEL_TX_COPY_MISSING:{channel} >&2; fi; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel create -o $COGNUS_ORDERER_ENDPOINT -c {channel} -f '$COGNUS_PEER_CHANNEL_TX' --outputBlock '$COGNUS_PEER_CHANNEL_BLOCK' $COGNUS_FETCH_FLAGS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel create -o $COGNUS_ORDERER_ENDPOINT -c {channel} -f '$COGNUS_PEER_CHANNEL_TX' --outputBlock '$COGNUS_PEER_CHANNEL_BLOCK' $COGNUS_FETCH_FLAGS_TLS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel create -o $COGNUS_ORDERER_ENDPOINT -c {channel} -f '$COGNUS_PEER_CHANNEL_TX' --outputBlock '$COGNUS_PEER_CHANNEL_BLOCK' $COGNUS_FETCH_FLAGS_PLAIN >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; "
                "if [ -s \"$channel_block\" ]; then "
                "COGNUS_OSN_CA_HOST=''; COGNUS_OSN_CERT_HOST=''; COGNUS_OSN_KEY_HOST=''; "
                "for candidate in /etc/hyperledger/fabric/tls/ca.crt /etc/hyperledger/fabric/msp/tlscacerts/tlsca.pem /var/lib/cognus/msp/tlscacerts/tlsca.pem; do if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_OSN_CA_HOST=$candidate; break; fi; done; "
                "for candidate in /etc/hyperledger/fabric/tls/server.crt /etc/hyperledger/fabric/msp/signcerts/cert.pem /var/lib/cognus/msp/signcerts/cert.pem; do if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_OSN_CERT_HOST=$candidate; break; fi; done; "
                "for candidate in /etc/hyperledger/fabric/tls/server.key /etc/hyperledger/fabric/msp/keystore/key.pem /var/lib/cognus/msp/keystore/key.pem; do if docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"test -s '$candidate'\" >/dev/null 2>&1; then COGNUS_OSN_KEY_HOST=$candidate; break; fi; done; "
                "COGNUS_ORDERER_CHANNEL_BLOCK_HOST=$COGNUS_TMP_ROOT/orderer-channel.block; "
                "COGNUS_OSN_CA_HOST_TMP=$COGNUS_TMP_ROOT/osn-ca-${{COGNUS_RUN_ID}}-{channel_raw}.pem; "
                "COGNUS_OSN_CERT_HOST_TMP=$COGNUS_TMP_ROOT/osn-cert-${{COGNUS_RUN_ID}}-{channel_raw}.crt; "
                "COGNUS_OSN_KEY_HOST_TMP=$COGNUS_TMP_ROOT/osn-key-${{COGNUS_RUN_ID}}-{channel_raw}.key; "
                "rm -f \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" \"$COGNUS_OSN_CA_HOST_TMP\" \"$COGNUS_OSN_CERT_HOST_TMP\" \"$COGNUS_OSN_KEY_HOST_TMP\" >/dev/null 2>&1 || true; "
                "cp \"$channel_block\" \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "if [ -n \"$COGNUS_OSN_CA_HOST\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_OSN_CA_HOST\" \"$COGNUS_OSN_CA_HOST_TMP\" >/dev/null 2>&1 || true; fi; "
                "if [ -n \"$COGNUS_OSN_CERT_HOST\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_OSN_CERT_HOST\" \"$COGNUS_OSN_CERT_HOST_TMP\" >/dev/null 2>&1 || true; fi; "
                "if [ -n \"$COGNUS_OSN_KEY_HOST\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$COGNUS_OSN_KEY_HOST\" \"$COGNUS_OSN_KEY_HOST_TMP\" >/dev/null 2>&1 || true; fi; "
                "chmod 644 \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" \"$COGNUS_OSN_CA_HOST_TMP\" \"$COGNUS_OSN_CERT_HOST_TMP\" \"$COGNUS_OSN_KEY_HOST_TMP\" >/dev/null 2>&1 || true; "
                "docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || docker pull hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || true; "
                "if [ -s \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" ]; then COGNUS_OSN_TLS_ARGS=''; if [ -s \"$COGNUS_OSN_CA_HOST_TMP\" ] && [ -s \"$COGNUS_OSN_CERT_HOST_TMP\" ] && [ -s \"$COGNUS_OSN_KEY_HOST_TMP\" ]; then COGNUS_OSN_TLS_ARGS=\"--ca-file $COGNUS_OSN_CA_HOST_TMP --client-cert $COGNUS_OSN_CERT_HOST_TMP --client-key $COGNUS_OSN_KEY_HOST_TMP\"; fi; docker run --rm --network container:$COGNUS_ORDERER_CONTAINER -v \"$COGNUS_TMP_ROOT\":/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc \"for admin_ep in 127.0.0.1:9443 127.0.0.1:7053; do osnadmin channel join --channelID {channel} --config-block /tmp/cognus/orderer-channel.block -o \\\"$admin_ep\\\" $COGNUS_OSN_TLS_ARGS >/dev/null 2>&1 && exit 0; done; exit 0\" >/dev/null 2>&1 || true; fi; "
                "if [ -s \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" ]; then "
                "COGNUS_ORDERER_CHANNEL_BLOCK_HOST=$COGNUS_TMP_ROOT/orderer-channel.block; "
                "rm -f \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "cp \"$channel_block\" \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "chmod 644 \"$COGNUS_ORDERER_CHANNEL_BLOCK_HOST\" >/dev/null 2>&1 || true; "
                "docker image inspect curlimages/curl:8.6.0 >/dev/null 2>&1 || docker pull curlimages/curl:8.6.0 >/dev/null 2>&1 || true; "
                "docker run --rm --network container:$COGNUS_ORDERER_CONTAINER -v \"$COGNUS_TMP_ROOT\":/tmp/cognus curlimages/curl:8.6.0 -sS -X POST http://127.0.0.1:9443/participation/v1/channels -F \"config-block=@/tmp/cognus/orderer-channel.block\" >/dev/null 2>&1 || true; "
                "fi; "
                "fi; "
                "fi; "
                "fi; "
                "if [ -s \"$channel_block\" ]; then docker cp \"$channel_block\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" >/dev/null 2>&1 || true; fi; "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CHANNEL_BLOCK'\" >/dev/null 2>&1; then "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel join -b '$COGNUS_PEER_CHANNEL_BLOCK' >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "fi; "
                "if ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CHANNEL_BLOCK'\" >/dev/null 2>&1 && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker exec \"$COGNUS_ORDERER_CONTAINER\" sh -lc \"ls -1 /var/hyperledger/production/orderer/chains/{channel}/blockfile /var/hyperledger/production/orderer/chains/chains/{channel}/blockfile 2>/dev/null | head -n 1\" >/tmp/cognus-${{COGNUS_RUN_ID}}-orderer-blockpath.txt 2>/dev/null || true; "
                "ORDERER_BLOCK_PATH=$(cat /tmp/cognus-${{COGNUS_RUN_ID}}-orderer-blockpath.txt 2>/dev/null | head -n 1); "
                "if [ -n \"$ORDERER_BLOCK_PATH\" ]; then docker cp \"$COGNUS_ORDERER_CONTAINER:$ORDERER_BLOCK_PATH\" \"$channel_block\" >/dev/null 2>&1 || true; fi; "
                "if [ -s \"$channel_block\" ]; then docker cp \"$channel_block\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" >/dev/null 2>&1 || true; docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel join -b '$COGNUS_PEER_CHANNEL_BLOCK' >/dev/null 2>&1\" >/dev/null 2>&1 || true; docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; fi; "
                "fi; "
                "if [ ! -s \"$channel_block\" ] && [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "COGNUS_FETCH_DIAG=/tmp/cognus-${{COGNUS_RUN_ID}}-{channel_raw}-fetch-diag.log; "
                "COGNUS_CREATE_DIAG=/tmp/cognus-${{COGNUS_RUN_ID}}-{channel_raw}-create-diag.log; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS\" >\"$COGNUS_FETCH_DIAG\" 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; "
                "if [ -s \"$channel_tx\" ]; then docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"mkdir -p /tmp/cognus >/dev/null 2>&1 || true\" >/dev/null 2>&1 || true; docker cp \"$channel_tx\" \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_TX\" >/dev/null 2>&1 || true; if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CHANNEL_TX'\" >/dev/null 2>&1; then docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel create -o $COGNUS_ORDERER_ENDPOINT -c {channel} -f '$COGNUS_PEER_CHANNEL_TX' --outputBlock '$COGNUS_PEER_CHANNEL_BLOCK' $COGNUS_FETCH_FLAGS\" >\"$COGNUS_CREATE_DIAG\" 2>&1 || true; docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; else printf '%s\\n' COGNUS_CHANNEL_TX_COPY_MISSING:{channel} >&2; fi; fi; "
                "if [ -s \"$COGNUS_FETCH_DIAG\" ]; then COGNUS_FETCH_LAST=$(tail -n 1 \"$COGNUS_FETCH_DIAG\" | tr -d '\\r' | cut -c1-220); [ -n \"$COGNUS_FETCH_LAST\" ] && printf '%s\\n' COGNUS_CHANNEL_FETCH_DIAG:$COGNUS_FETCH_LAST >&2; fi; "
                "if [ -s \"$COGNUS_CREATE_DIAG\" ]; then COGNUS_CREATE_LAST=$(tail -n 1 \"$COGNUS_CREATE_DIAG\" | tr -d '\\r' | cut -c1-220); [ -n \"$COGNUS_CREATE_LAST\" ] && printf '%s\\n' COGNUS_CHANNEL_CREATE_DIAG:$COGNUS_CREATE_LAST >&2; fi; "
                "fi; "
                "COGNUS_CHANNEL_READY=0; "
                "for COGNUS_JOIN_ATTEMPT in $(seq 1 4); do "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel getinfo -c {channel} >/dev/null 2>&1\" >/dev/null 2>&1 || docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel list 2>/dev/null | grep -F {channel} >/dev/null\" >/dev/null 2>&1; then COGNUS_CHANNEL_READY=1; break; fi; "
                "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS_TLS >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel fetch 0 '$COGNUS_PEER_CHANNEL_BLOCK' -c {channel} -o $COGNUS_ORDERER_ENDPOINT $COGNUS_FETCH_FLAGS_PLAIN >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "docker cp \"$COGNUS_PEER_CONTAINER:$COGNUS_PEER_CHANNEL_BLOCK\" \"$channel_block\" >/dev/null 2>&1 || true; "
                "fi; "
                "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"test -s '$COGNUS_PEER_CHANNEL_BLOCK'\" >/dev/null 2>&1; then "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"peer channel join -b '$COGNUS_PEER_CHANNEL_BLOCK' >/dev/null 2>&1\" >/dev/null 2>&1 || true; "
                "fi; "
                "sleep 1; "
                "done; "
            ).format(
                channel=channel_q,
                channel_raw=normalized_channel_id,
                orderer_host_override=shlex.quote(RUNBOOK_ORDERER_TLS_HOST_OVERRIDE),
                runtime_orderer_tlsca_host=runtime_orderer_tlsca_host_q,
                configtx_fallback_b64=RUNBOOK_CONFIGTX_FALLBACK_B64,
                configtx_profile_autodetect_cfg_step=configtx_profile_autodetect_cfg_step,
                configtx_profile_autodetect_cfg_dir_step=configtx_profile_autodetect_cfg_dir_step,
                configtx_block_direct_fallback_step=configtx_block_direct_fallback_step,
            )
            checks.append(
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc {channel_membership_check} >/dev/null 2>&1 || "
                "{{ {channel_fetch_and_join} "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc {channel_membership_check} >/dev/null 2>&1 || "
                "{{ "
                "{channel_failure_clause}"
                "}}; }}; "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc {lifecycle_check} || "
                "{{ {channel_fetch_and_join} "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc {lifecycle_check} || "
                "{{ {chaincode_define_step} "
                "docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc {lifecycle_check} || "
                "{{ {lifecycle_failure_clause} }}; }}; }}; ".format(
                    channel_membership_check=shlex.quote(channel_membership_check),
                    channel_fetch_and_join=channel_fetch_and_join,
                    lifecycle_check=shlex.quote(lifecycle_check),
                    chaincode_define_step=chaincode_define_step,
                    channel_failure_clause=channel_failure_clause,
                    lifecycle_failure_clause=lifecycle_failure_clause,
                )
            )

    return "{}{}COGNUS_RUN_ID={}; {}".format(
        resolve_peer_step,
        resolve_orderer_step,
        run_id_q,
        "".join(checks),
    )


def _build_ssh_remote_command(stage_key, checkpoint_key, run_state=None, host_row=None):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    safe_host_row = host_row if isinstance(host_row, dict) else {}

    node_id = str(safe_host_row.get("node_id", "") or "").strip() or "node"
    node_type = str(safe_host_row.get("node_type", "") or "").strip() or "node"
    runtime_image = str(safe_host_row.get("runtime_image", "") or "").strip()
    if not runtime_image:
        runtime_image = _resolve_runtime_image_for_node_type(node_type)
    runtime_image = _normalize_runtime_image_reference(
        node_type,
        runtime_image,
        _resolve_runtime_image_for_node_type(node_type),
    )
    run_id = str(safe_run_state.get("run_id", "") or "").strip()
    change_id = str(safe_run_state.get("change_id", "") or "").strip()
    container_name = _resolve_runbook_container_name(safe_run_state, safe_host_row)

    quoted_network = shlex.quote(RUNBOOK_BASE_NETWORK_NAME)
    quoted_image = shlex.quote(runtime_image)
    quoted_run_id = shlex.quote(run_id)
    quoted_change_id = shlex.quote(change_id)
    quoted_node_id = shlex.quote(node_id)
    quoted_node_type = shlex.quote(node_type)
    quoted_org_id = shlex.quote(str(safe_host_row.get("org_id", "")).strip().lower())
    quoted_container_name = shlex.quote(container_name)

    if stage_key == "prepare" and checkpoint_key == "prepare.preflight":
        return (
            "set -e; "
            "uname -s >/dev/null; "
            "command -v docker >/dev/null; "
            "docker --version >/dev/null; "
            "ARTIFACT_BASE=\"${HOME:-/tmp}/.cognus\"; "
            "mkdir -p \"${ARTIFACT_BASE}/.preflight\"; "
            "touch \"${ARTIFACT_BASE}/.preflight/write-check\"; "
            "echo COGNUS_PREPARE_PREFLIGHT_OK"
        )

    if stage_key == "prepare" and checkpoint_key == "prepare.connectivity":
        return (
            "set -e; "
            "docker info >/dev/null; "
            "echo COGNUS_PREPARE_CONNECTIVITY_OK"
        )

    runtime_image_bootstrap_command = _build_runtime_image_bootstrap_command(
        node_type, runtime_image
    )
    runtime_run_resource_args = _resolve_runtime_docker_run_resource_args(node_type)
    runtime_run_command_override_args = _resolve_runtime_docker_run_command_override_args(
        node_type
    )
    runtime_volume_args = _resolve_runtime_docker_volume_args(safe_run_state, safe_host_row)
    runtime_state_guard_step = _resolve_runtime_state_guard_step(quoted_container_name)
    runtime_environment_args = _resolve_runtime_docker_environment_args(
        safe_run_state,
        safe_host_row,
    )
    runtime_publish_args = _resolve_runtime_docker_publish_args(
        safe_run_state,
        safe_host_row,
    )
    runtime_host_port_preflight_step = ""
    if stage_key == "provision" and checkpoint_key == "provision.runtime":
        runtime_host_port_preflight_step = _resolve_runtime_host_port_preflight_step(
            safe_run_state,
            safe_host_row,
        )
    runtime_chaincode_commit_guard_step = ""
    if RUNBOOK_CHAINCODE_LIFECYCLE_GUARD_ENABLED and (
        (stage_key == "provision" and checkpoint_key == "provision.runtime")
        or (stage_key == "verify" and checkpoint_key == "verify.consistency")
    ):
        lifecycle_guard_enforce_failures = not (
            stage_key == "provision" and checkpoint_key == "provision.runtime"
        )
        if stage_key == "verify" and checkpoint_key == "verify.consistency":
            lifecycle_guard_enforce_failures = RUNBOOK_VERIFY_CONSISTENCY_STRICT
        runtime_chaincode_commit_guard_step = _resolve_runtime_chaincode_commit_guard_step(
            safe_run_state,
            safe_host_row,
            lifecycle_guard_enforce_failures,
        )
        if (
            stage_key == "verify"
            and checkpoint_key == "verify.consistency"
            and _normalize_runtime_component_node_type(safe_host_row.get("node_type", ""))
            in ("apigateway", "netapi")
            and not _is_primary_gateway_verify_host(safe_run_state, safe_host_row)
        ):
            runtime_chaincode_commit_guard_step = ""
    runtime_apigateway_bootstrap_step = _resolve_apigateway_runtime_bootstrap_step(
        safe_run_state,
        safe_host_row,
    )
    runtime_apigateway_hotpatch_step = _resolve_apigateway_runtime_hotpatch_step(
        safe_run_state,
        safe_host_row,
        container_name,
    )
    runtime_provision_stagger_step = _resolve_runtime_provision_stagger_step()
    zero_provision_cleanup_step = ""
    if stage_key == "provision" and checkpoint_key == "provision.hosts":
        zero_provision_cleanup_step = _resolve_zero_provision_cleanup_step(
            safe_run_state,
            safe_host_row,
        )
    runtime_memory_guard_step = _resolve_runtime_memory_guard_step(node_type)
    runtime_topology_guard_step = _resolve_runtime_topology_guard_step(
        safe_run_state,
        safe_host_row,
    )

    if stage_key == "provision" and checkpoint_key == "provision.hosts":
        return (
            "set -e; "
            "{zero_provision_cleanup_step}"
            "docker network inspect {network} >/dev/null 2>&1 || "
            "docker network create {network} >/dev/null; "
            "echo COGNUS_PROVISION_HOSTS_OK"
        ).format(
            zero_provision_cleanup_step=zero_provision_cleanup_step,
            network=quoted_network,
        )

    if stage_key == "provision" and checkpoint_key == "provision.runtime":
        # Ensure host-side MSP path exists. Try to materialize MSP artifacts
        # from the run_state (if provided) into the pipeline materialization
        # location, then copy them into the runtime mount target. Non-fatal.
        # Build a materialization step that writes base64 payloads from
        # run_state['msp_tls_artifacts'] -> runtime host identity path
        msp_materialize_step = ""
        peer_fallback_msp_seed_step = _resolve_runtime_peer_fallback_msp_seed_step(
            safe_host_row
        )
        runtime_host_identity_path = _resolve_runtime_host_identity_path(
            safe_run_state,
            safe_host_row,
        )
        try:
            msp_artifacts = safe_run_state.get("msp_tls_artifacts") if isinstance(safe_run_state.get("msp_tls_artifacts"), dict) else {}
            if (
                isinstance(msp_artifacts, dict)
                and msp_artifacts
                and runtime_host_identity_path
            ):
                parts = []
                for rel_path, b64_payload in msp_artifacts.items():
                    rel_path_norm = str(rel_path or "").lstrip("/")
                    dest = "{}/{}".format(
                        runtime_host_identity_path.rstrip("/"),
                        rel_path_norm,
                    )
                    parent = os.path.dirname(dest)
                    # create parent dir then write decoded payload non-fatally
                    parts.append(f"mkdir -p {shlex.quote(parent)} >/dev/null 2>&1 || true; printf %s {shlex.quote(str(b64_payload or ''))} | base64 -d > {shlex.quote(dest)} 2>/dev/null || true; chmod 600 {shlex.quote(dest)} 2>/dev/null || true;")
                if parts:
                    candidate_materialize_step = "".join(parts)
                    if (
                        RUNBOOK_MSP_MATERIALIZE_INLINE_MAX_BYTES <= 0
                        or len(candidate_materialize_step.encode("utf-8"))
                        <= RUNBOOK_MSP_MATERIALIZE_INLINE_MAX_BYTES
                    ):
                        msp_materialize_step = candidate_materialize_step
        except Exception:
            msp_materialize_step = ""

        msp_prep_step = ""
        if runtime_host_identity_path:
            msp_prep_step = "mkdir -p {} >/dev/null 2>&1 || true; ".format(
                shlex.quote(runtime_host_identity_path)
            )

        return (
            "set -e; "
            "{runtime_image_bootstrap}"
            "{msp_materialize_step}"
            "{peer_fallback_msp_seed_step}"
            "{msp_prep_step}"
            "{runtime_apigateway_bootstrap_step}"
            "{runtime_chaincode_commit_guard_step}"
            "docker inspect {container} >/dev/null 2>&1 || "
            "({runtime_provision_stagger}"
            "{runtime_host_port_preflight}"
            "{runtime_memory_guard}"
            "docker run -d --name {container} --network {network} "
            "{runtime_publish_args}"
            "{runtime_volume_args}"
            "{runtime_environment_args}"
            "--restart unless-stopped "
            "{runtime_run_resource_args}"
            "--label cognus.run_id={run_id} "
            "--label cognus.change_id={change_id} "
            "--label cognus.node_id={node_id} "
            "--label cognus.node_type={node_type} "
            "--label cognus.runtime_image={runtime_image} "
            "{image}{runtime_run_command_override_args} >/dev/null 2>&1 || (docker inspect {container} >/dev/null 2>&1 && docker start {container} >/dev/null 2>&1)); "
            "docker inspect {container} >/dev/null; "
            "COGNUS_RUNTIME_STATUS_NOW=$(docker inspect -f '{{{{.State.Status}}}}' {container} 2>/dev/null || echo unknown); "
            "if [ \"$COGNUS_RUNTIME_STATUS_NOW\" != running ]; then docker start {container} >/dev/null 2>&1 || true; fi; "
            "{runtime_apigateway_hotpatch_step}"
            "{runtime_state_guard}"
            "echo COGNUS_PROVISION_RUNTIME_OK"
        ).format(
            runtime_image_bootstrap=runtime_image_bootstrap_command,
            msp_materialize_step=msp_materialize_step,
            peer_fallback_msp_seed_step=peer_fallback_msp_seed_step,
            msp_prep_step=msp_prep_step,
            runtime_provision_stagger=runtime_provision_stagger_step,
            runtime_host_port_preflight=runtime_host_port_preflight_step,
            runtime_memory_guard=runtime_memory_guard_step,
            runtime_state_guard=runtime_state_guard_step,
            runtime_publish_args=runtime_publish_args,
            runtime_volume_args=runtime_volume_args,
            runtime_environment_args=runtime_environment_args,
            runtime_apigateway_bootstrap_step=runtime_apigateway_bootstrap_step,
            runtime_apigateway_hotpatch_step=runtime_apigateway_hotpatch_step,
            runtime_chaincode_commit_guard_step="",
            runtime_run_resource_args=runtime_run_resource_args,
            runtime_run_command_override_args=runtime_run_command_override_args,
            container=quoted_container_name,
            network=quoted_network,
            run_id=quoted_run_id,
            change_id=quoted_change_id,
            node_id=quoted_node_id,
            node_type=quoted_node_type,
            runtime_image=shlex.quote(runtime_image),
            image=quoted_image,
        )

    if stage_key == "configure" and checkpoint_key == "configure.artifacts":
        return (
            "set -e; "
            "ARTIFACT_BASE=\"${{HOME:-/tmp}}/.cognus\"; "
            "mkdir -p \"${{ARTIFACT_BASE}}/{run_id}/nodes/{container}\"; "
            "printf '%s\\n' {node_id} > \"${{ARTIFACT_BASE}}/{run_id}/nodes/{container}/node_id.txt\"; "
            "printf '%s\\n' {node_type} > \"${{ARTIFACT_BASE}}/{run_id}/nodes/{container}/node_type.txt\"; "
            "echo COGNUS_CONFIGURE_ARTIFACTS_OK"
        ).format(
            run_id=_sanitize_container_token(run_id, "run"),
            container=_sanitize_container_token(container_name, "node"),
            node_id=quoted_node_id,
            node_type=quoted_node_type,
        )

    if stage_key == "configure" and checkpoint_key == "configure.policies":
        return (
            "set -e; "
            "docker inspect {container} >/dev/null; "
            "docker update --restart unless-stopped {container} >/dev/null; "
            "echo COGNUS_CONFIGURE_POLICIES_OK"
        ).format(container=quoted_container_name)

    if stage_key == "verify" and checkpoint_key == "verify.consistency":
        return (
            "set -e; "
            "docker inspect {container} >/dev/null; "
            "{runtime_topology_guard}"
            "{runtime_chaincode_commit_guard}"
            "echo COGNUS_VERIFY_CONSISTENCY_OK"
        ).format(
            container=quoted_container_name,
            runtime_topology_guard=runtime_topology_guard_step,
            runtime_chaincode_commit_guard=runtime_chaincode_commit_guard_step,
        )

    if stage_key == "verify" and checkpoint_key == "verify.evidence":
        return (
            "set -e; "
            "docker inspect {container} --format '{{{{.Name}}}} {{{{.State.Status}}}}' >/dev/null; "
            "echo COGNUS_VERIFY_EVIDENCE_OK"
        ).format(container=quoted_container_name)

    return "echo COGNUS_RUNBOOK_STAGE={} CHECKPOINT={} && uname -s".format(
        stage_key,
        checkpoint_key,
    )


def _build_ssh_preflight_remote_command(change_id):
    return "echo COGNUS_PREFLIGHT_CHANGE_ID={} && uname -s".format(
        str(change_id or "").strip(),
    )


def _resolve_preflight_failure_feedback(stderr_text, identity_file):
    normalized_stderr = str(stderr_text or "").strip().lower()
    has_identity_file = bool(str(identity_file or "").strip())

    if "permission denied" in normalized_stderr:
        if has_identity_file:
            return (
                "Autenticação SSH rejeitada pelo host remoto.",
                "Validar usuário/chave, política do sshd e se a origem do backend está autorizada em authorized_keys.",
            )
        return (
            "Autenticação SSH rejeitada pelo host remoto.",
            "Vincular credencial SSH válida para o host e reexecutar o preflight técnico.",
        )

    if "host key verification failed" in normalized_stderr:
        return (
            "Verificação de host key falhou durante conexão SSH.",
            "Limpar known_hosts do backend para o host alvo e reexecutar o preflight técnico.",
        )

    if "connection timed out" in normalized_stderr or "operation timed out" in normalized_stderr:
        return (
            "Timeout de conexão SSH no host remoto.",
            "Validar rota de rede, firewall e disponibilidade da porta SSH antes de avançar.",
        )

    if "no route to host" in normalized_stderr or "network is unreachable" in normalized_stderr:
        return (
            "Host remoto inacessível pela rede do backend.",
            "Validar conectividade entre o backend oficial e o host remoto (rota, NAT, firewall).",
        )

    return (
        "Conectividade SSH falhou no endpoint informado.",
        "Validar chave SSH, usuário, porta, known_hosts e política do sshd antes de avançar.",
    )


def _classify_fabric_runtime_failure(stderr_text):
    normalized_stderr = str(stderr_text or "")
    if not normalized_stderr:
        return None

    orderer_ledger_corruption_match = re.search(
        r"COGNUS_ORDERER_LEDGER_CORRUPTION:([^\s]+)",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if orderer_ledger_corruption_match:
        channel_name = str(orderer_ledger_corruption_match.group(1) or "").strip().lower()
        return (
            "runbook_fabric_orderer_channel_state_invalid",
            "Orderer apresentou indicio de corrupcao de ledger durante participation join do canal.",
            {
                "channel_id": channel_name,
                "lifecycle_error_class": "orderer_ledger_corruption",
            },
        )

    channel_block_invalid_match = re.search(
        r"COGNUS_CHANNEL_BLOCK_INVALID:([^\s]+)",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if channel_block_invalid_match:
        channel_name = str(channel_block_invalid_match.group(1) or "").strip().lower()
        return (
            "runbook_fabric_orderer_channel_state_invalid",
            "Bloco de canal invalido para participation join do orderer.",
            {
                "channel_id": channel_name,
                "lifecycle_error_class": "channel_block_invalid",
            },
        )

    orderer_participation_join_failed_match = re.search(
        r"COGNUS_ORDERER_PARTICIPATION_JOIN_FAILED:([^\s]+)",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if orderer_participation_join_failed_match:
        channel_name = str(
            orderer_participation_join_failed_match.group(1) or ""
        ).strip().lower()
        return (
            "runbook_fabric_orderer_channel_state_invalid",
            "Orderer nao conseguiu concluir participation join do canal exigido.",
            {
                "channel_id": channel_name,
                "lifecycle_error_class": "orderer_participation_join_failed",
            },
        )

    lifecycle_error_class_match = re.search(
        r"COGNUS_LIFECYCLE_ERROR_CLASS:([^\s]+)",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if lifecycle_error_class_match:
        lifecycle_error_class = str(
            lifecycle_error_class_match.group(1) or "unknown"
        ).strip().lower()
        error_map = {
            "msp_policy_forbidden": (
                "runbook_fabric_msp_policy_forbidden",
                "Policy de MSP bloqueou approve/commit do lifecycle no host alvo.",
            ),
            "tls_orderer": (
                "runbook_fabric_orderer_tls_mismatch",
                "TLS entre peer e orderer inconsistente durante lifecycle do chaincode.",
            ),
            "orderer_channel_state": (
                "runbook_fabric_orderer_channel_state_invalid",
                "Orderer em estado de canal invalido para concluir lifecycle do chaincode.",
            ),
        }
        mapped_code, mapped_message = error_map.get(
            lifecycle_error_class,
            (
                "runbook_fabric_lifecycle_failed",
                "Lifecycle do chaincode falhou por restricao de identidade/politica no host alvo.",
            ),
        )
        return (
            mapped_code,
            mapped_message,
            {"lifecycle_error_class": lifecycle_error_class},
        )

    if re.search(
        r"channel\s+creation\s+request\s+not\s+allowed|got\s+unexpected\s+status:\s*BAD_REQUEST",
        normalized_stderr,
        flags=re.IGNORECASE,
    ):
        return (
            "runbook_fabric_orderer_channel_state_invalid",
            "Orderer rejeitou operacao de canal/lifecycle por estado de canal inconsistente.",
            {
                "lifecycle_error_class": "orderer_channel_state",
            },
        )

    channel_not_joined_match = re.search(
        r"COGNUS_CHANNEL_NOT_JOINED:([^\s]+)",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if channel_not_joined_match:
        channel_name = str(channel_not_joined_match.group(1) or "").strip().lower()
        return (
            "runbook_fabric_channel_not_joined",
            "Peer nao ingressou no canal Fabric esperado no host alvo.",
            {"channel_id": channel_name},
        )

    chaincode_not_committed_match = re.search(
        r"COGNUS_CHAINCODE_NOT_COMMITTED:([^:\s]+):([^\s]+)",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if chaincode_not_committed_match:
        channel_name = str(chaincode_not_committed_match.group(1) or "").strip().lower()
        chaincode_name = str(chaincode_not_committed_match.group(2) or "").strip().lower()
        return (
            "runbook_fabric_chaincode_not_committed",
            "Chaincode nao foi comprometido no canal Fabric esperado.",
            {
                "channel_id": channel_name,
                "chaincode_id": chaincode_name,
            },
        )

    chaincode_not_found_match = re.search(
        r"chaincode\s+([^\s]+)\s+not\s+found",
        normalized_stderr,
        flags=re.IGNORECASE,
    )
    if chaincode_not_found_match:
        chaincode_name = str(chaincode_not_found_match.group(1) or "").strip().lower()
        return (
            "runbook_fabric_chaincode_not_committed",
            "Chaincode nao foi encontrado no canal durante consulta funcional.",
            {
                "chaincode_id": chaincode_name,
                "lifecycle_error_class": "chaincode_not_found",
            },
        )

    writers_policy_unknown_authority = bool(
        re.search(r"implicit\s+policy\s+evaluation\s+failed", normalized_stderr, flags=re.IGNORECASE)
        and re.search(r"Writers", normalized_stderr, flags=re.IGNORECASE)
        and re.search(
            r"certificate\s+signed\s+by\s+unknown\s+authority",
            normalized_stderr,
            flags=re.IGNORECASE,
        )
    )
    if writers_policy_unknown_authority:
        return (
            "runbook_fabric_orderer_identity_drift",
            "Bloco do orderer rejeitado por policy Writers: identidade do orderer nao confiavel para o canal.",
            {
                "lifecycle_error_class": "orderer_signer_not_trusted_by_channel",
                "hint": "Reprovisionar com material criptografico consistente entre genesis/channel MSP e certificados ativos do orderer.",
            },
        )

    if re.search(
        r"could\s+not\s+dial\s+endpoint\s+'127\.0\.0\.1:7050'",
        normalized_stderr,
        flags=re.IGNORECASE,
    ):
        return (
            "runbook_fabric_orderer_endpoint_unreachable",
            "Peer nao consegue conectar no endpoint do orderer para sincronizar blocos.",
            {
                "lifecycle_error_class": "orderer_endpoint_loopback_unreachable",
                "orderer_endpoint": "127.0.0.1:7050",
            },
        )

    return None


def _summarize_preflight_hosts(host_rows):
    summary = {
        "apto": 0,
        "parcial": 0,
        "bloqueado": 0,
        "total": 0,
    }
    for host_row in host_rows:
        if not isinstance(host_row, dict):
            continue
        host_status = str(host_row.get("status", "")).strip().lower()
        if host_status == "apto":
            summary["apto"] += 1
        elif host_status == "parcial":
            summary["parcial"] += 1
        else:
            summary["bloqueado"] += 1
        summary["total"] += 1

    if summary["total"] == 0:
        overall_status = "bloqueado"
    elif summary["bloqueado"] > 0:
        overall_status = "bloqueado"
    elif summary["parcial"] > 0:
        overall_status = "parcial"
    else:
        overall_status = "apto"

    return summary, overall_status


def _run_remote_command(
    host_address,
    ssh_user,
    ssh_port,
    remote_command,
    identity_file="",
    timeout=None,
):
    remote_command_text = str(remote_command or "")
    stream_via_stdin = _should_stream_remote_command_via_stdin(remote_command_text)
    ssh_args = _build_ssh_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command=(
            _build_ssh_stdin_wrapper_command() if stream_via_stdin else remote_command_text
        ),
        identity_file=identity_file,
    )
    run_kwargs = {
        "capture_output": True,
        "text": True,
        "timeout": timeout or (RUNBOOK_SSH_TIMEOUT_SECONDS + 5),
    }
    if stream_via_stdin:
        run_kwargs["input"] = remote_command_text

    try:
        result = subprocess.run(ssh_args, **run_kwargs)
        return {
            "returncode": result.returncode,
            "stdout": str(result.stdout or "").strip(),
            "stderr": str(result.stderr or "").strip(),
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "returncode": 124,
            "stdout": str(exc.stdout or "").strip(),
            "stderr": str(exc.stderr or "").strip(),
            "timed_out": True,
        }
    except FileNotFoundError as exc:
        return {
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
            "timed_out": False,
        }
    except OSError as exc:
        if getattr(exc, "errno", None) == errno.E2BIG and not stream_via_stdin:
            fallback_ssh_args = _build_ssh_command(
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                remote_command=_build_ssh_stdin_wrapper_command(),
                identity_file=identity_file,
            )
            try:
                result = subprocess.run(
                    fallback_ssh_args,
                    capture_output=True,
                    text=True,
                    timeout=timeout or (RUNBOOK_SSH_TIMEOUT_SECONDS + 5),
                    input=remote_command_text,
                )
                return {
                    "returncode": result.returncode,
                    "stdout": str(result.stdout or "").strip(),
                    "stderr": str(result.stderr or "").strip(),
                    "timed_out": False,
                }
            except subprocess.TimeoutExpired as retry_exc:
                return {
                    "returncode": 124,
                    "stdout": str(retry_exc.stdout or "").strip(),
                    "stderr": str(retry_exc.stderr or "").strip(),
                    "timed_out": True,
                }
            except FileNotFoundError as retry_exc:
                return {
                    "returncode": 127,
                    "stdout": "",
                    "stderr": str(retry_exc),
                    "timed_out": False,
                }
            except OSError as retry_exc:
                return {
                    "returncode": 127,
                    "stdout": "",
                    "stderr": str(retry_exc),
                    "timed_out": False,
                }
        return {
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
            "timed_out": False,
        }


def _is_transient_ssh_failure(returncode, stderr_text, timed_out=False):
    if timed_out:
        return True

    try:
        normalized_returncode = int(returncode or 0)
    except (TypeError, ValueError):
        normalized_returncode = 0

    normalized_stderr = str(stderr_text or "").strip().lower()
    if normalized_returncode == 255:
        if not normalized_stderr:
            return True
        transient_markers = (
            "connection reset by peer",
            "connection closed by",
            "broken pipe",
            "kex_exchange_identification",
            "banner exchange",
            "connection timed out",
            "operation timed out",
            "network is unreachable",
            "no route to host",
            "connection refused",
        )
        return any(marker in normalized_stderr for marker in transient_markers)

    return False


def _runtime_image_preseed_required_for_node_type(node_type):
    if not RUNBOOK_RUNTIME_IMAGE_PRESEED_REQUIRED:
        return False
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    if not normalized_node_type:
        return False
    return normalized_node_type in RUNBOOK_RUNTIME_IMAGE_PRESEED_NODE_TYPES


def _runtime_image_preseed_pull_first_for_node_type(node_type):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    if not normalized_node_type:
        return False
    return normalized_node_type in RUNBOOK_RUNTIME_IMAGE_PRESEED_PULL_FIRST_NODE_TYPES


def _runtime_image_preseed_cache_key(host_address, runtime_image):
    return "{}|{}".format(
        str(host_address or "").strip().lower(),
        str(runtime_image or "").strip().lower(),
    )


def _runtime_image_preseed_cache_store(run_state):
    cache_store = run_state.setdefault("runtime_image_preseed_cache", {})
    if not isinstance(cache_store, dict):
        cache_store = {}
        run_state["runtime_image_preseed_cache"] = cache_store
    return cache_store


def _runtime_image_preseed_cache_get(run_state, host_address, runtime_image):
    cache_store = _runtime_image_preseed_cache_store(run_state)
    cache_key = _runtime_image_preseed_cache_key(host_address, runtime_image)
    cache_row = cache_store.get(cache_key, {})
    if not isinstance(cache_row, dict):
        return {}
    return dict(cache_row)


def _upsert_runtime_image_preseed_cache(
    run_state,
    host_address,
    runtime_image,
    status,
    reason,
    details=None,
    bump_attempt=False,
):
    cache_store = _runtime_image_preseed_cache_store(run_state)
    cache_key = _runtime_image_preseed_cache_key(host_address, runtime_image)
    previous_row = cache_store.get(cache_key, {})
    if not isinstance(previous_row, dict):
        previous_row = {}

    attempts = int(previous_row.get("attempts", 0) or 0)
    if bump_attempt:
        attempts += 1

    now_epoch = int(datetime.now(timezone.utc).timestamp())
    cache_row = {
        "cache_key": cache_key,
        "status": str(status or "").strip().lower(),
        "reason": str(reason or "").strip(),
        "attempts": max(0, attempts),
        "last_attempt_epoch": now_epoch,
        "last_attempt_at_utc": _utc_now(),
        "details": details if isinstance(details, dict) else {},
    }
    cache_store[cache_key] = cache_row
    return dict(cache_row)


def _runtime_image_preseed_cache_cooldown_remaining_seconds(cache_row):
    if not isinstance(cache_row, dict):
        return 0
    cooldown_seconds = max(0, RUNBOOK_RUNTIME_IMAGE_PRESEED_RETRY_COOLDOWN_SECONDS)
    if cooldown_seconds == 0:
        return 0
    last_attempt_epoch = int(cache_row.get("last_attempt_epoch", 0) or 0)
    if last_attempt_epoch <= 0:
        return 0
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    elapsed_seconds = max(0, now_epoch - last_attempt_epoch)
    if elapsed_seconds >= cooldown_seconds:
        return 0
    return cooldown_seconds - elapsed_seconds


def _runtime_image_preseed_cache_backoff_reason(cache_row):
    if not isinstance(cache_row, dict):
        return ""
    if str(cache_row.get("status", "")).strip().lower() != "failed":
        return ""

    attempts = int(cache_row.get("attempts", 0) or 0)
    max_attempts = max(1, RUNBOOK_RUNTIME_IMAGE_PRESEED_MAX_ATTEMPTS_PER_RUN)
    if attempts >= max_attempts:
        return "runtime_image_preseed_attempt_limit_reached"

    cooldown_remaining = _runtime_image_preseed_cache_cooldown_remaining_seconds(cache_row)
    if cooldown_remaining > 0:
        return "runtime_image_preseed_retry_cooldown_active"

    return ""


def _probe_remote_runtime_image(
    host_address,
    ssh_user,
    ssh_port,
    runtime_image,
    identity_file="",
):
    normalized_runtime_image = str(runtime_image or "").strip()
    if not normalized_runtime_image:
        return {
            "available": False,
            "returncode": 1,
            "stdout": "",
            "stderr": "runtime_image_empty",
            "timed_out": False,
        }

    probe_command = "docker image inspect {} >/dev/null 2>&1".format(
        shlex.quote(normalized_runtime_image)
    )
    probe_result = _run_remote_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command=probe_command,
        identity_file=identity_file,
        timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 5,
    )
    return {
        **probe_result,
        "available": bool(
            probe_result.get("returncode", 1) == 0
            and not probe_result.get("timed_out", False)
        ),
        "runtime_image": normalized_runtime_image,
    }


def _probe_remote_mem_available_kb(
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    probe_result = _run_remote_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command="awk '/MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0",
        identity_file=identity_file,
        timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 5,
    )
    parsed_mem_available_kb = 0
    try:
        parsed_mem_available_kb = int(str(probe_result.get("stdout", "0") or "0").splitlines()[-1].strip() or "0")
    except (TypeError, ValueError, IndexError):
        parsed_mem_available_kb = 0

    return {
        **probe_result,
        "mem_available_kb": max(0, parsed_mem_available_kb),
    }


def _gateway_probe_checks(required_paths=None, required_checks=None):
    checks = []
    seen = set()

    def _append_check(item):
        if isinstance(item, str):
            candidate = {"path": item, "method": "GET"}
        elif isinstance(item, dict):
            candidate = dict(item)
        else:
            return

        normalized_path = str(candidate.get("path", "") or "").strip()
        if not normalized_path:
            return
        if not normalized_path.startswith("/"):
            normalized_path = "/{}".format(normalized_path)

        normalized_method = str(candidate.get("method", "GET") or "GET").strip().upper()
        if not normalized_method:
            normalized_method = "GET"

        normalized_headers = {}
        raw_headers = candidate.get("headers", {})
        if isinstance(raw_headers, dict):
            for key, value in raw_headers.items():
                header_key = str(key or "").strip()
                if not header_key:
                    continue
                normalized_headers[header_key] = str(value or "").strip()

        body = candidate.get("body")
        body_text = ""
        if isinstance(body, (dict, list)):
            body_text = json.dumps(
                body,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        elif body is not None:
            body_text = str(body)
        if body_text and "Content-Type" not in normalized_headers:
            normalized_headers["Content-Type"] = "application/json"

        key = (
            normalized_method,
            normalized_path,
            tuple(sorted(normalized_headers.items())),
            hashlib.sha256(body_text.encode("utf-8")).hexdigest() if body_text else "",
        )
        if key in seen:
            return
        seen.add(key)
        checks.append(
            {
                "path": normalized_path,
                "method": normalized_method,
                "headers": normalized_headers,
                "body": body_text,
            }
        )

    for path in required_paths if isinstance(required_paths, list) else []:
        _append_check(path)
    for item in required_checks if isinstance(required_checks, list) else []:
        _append_check(item)

    if not checks:
        checks = [
            {"path": "/", "method": "GET", "headers": {}, "body": ""},
            {"path": "/api", "method": "GET", "headers": {}, "body": ""},
        ]
    return checks


def _decode_b64_excerpt(value):
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    try:
        return base64.b64decode(normalized).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _gateway_verify_scope_cache_key(run_state, host_row, host_address):
    scope = _collect_gateway_channel_chaincode_pairs(run_state, host_row)
    payload = {
        "host_address": str(host_address or "").strip().lower(),
        "org_id": str(scope.get("org_id", "") or "").strip().lower(),
        "channels": sorted(
            {
                str(channel_id or "").strip().lower()
                for channel_id in scope.get("channels", [])
                if str(channel_id or "").strip()
            }
        ),
        "pairs": sorted(
            {
                "{}|{}".format(
                    str(row.get("channel_id", "") or "").strip().lower(),
                    str(row.get("chaincode_id", "") or "").strip().lower(),
                )
                for row in scope.get("pairs", [])
                if isinstance(row, dict)
                and str(row.get("channel_id", "") or "").strip()
                and str(row.get("chaincode_id", "") or "").strip()
            }
        ),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _gateway_verify_scope_cache_store(run_state):
    cache_store = run_state.setdefault("gateway_verify_scope_cache", {})
    if not isinstance(cache_store, dict):
        cache_store = {}
        run_state["gateway_verify_scope_cache"] = cache_store
    return cache_store


def _gateway_verify_scope_cache_get(run_state, cache_key):
    cache_store = _gateway_verify_scope_cache_store(run_state)
    cache_row = cache_store.get(cache_key, {})
    if not isinstance(cache_row, dict):
        return {}
    try:
        return json.loads(json.dumps(cache_row))
    except Exception:
        return dict(cache_row)


def _upsert_gateway_verify_scope_cache(
    run_state,
    cache_key,
    diagnostics,
    commit_matrix,
):
    cache_store = _gateway_verify_scope_cache_store(run_state)
    cache_row = {
        "cache_key": str(cache_key or "").strip(),
        "updated_at_utc": _utc_now(),
        "diagnostics": diagnostics if isinstance(diagnostics, dict) else {},
        "commit_matrix": commit_matrix if isinstance(commit_matrix, dict) else {},
    }
    cache_store[cache_key] = cache_row
    try:
        return json.loads(json.dumps(cache_row))
    except Exception:
        return dict(cache_row)


def _resolve_gateway_http_probe_timeout(timeout_seconds=None, http_probe_timeout_seconds=None):
    resolved_timeout = RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS
    try:
        if http_probe_timeout_seconds is not None:
            resolved_timeout = int(http_probe_timeout_seconds or 0)
        elif timeout_seconds is not None:
            resolved_timeout = min(
                int(timeout_seconds or RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS),
                RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS,
            )
    except (TypeError, ValueError):
        resolved_timeout = RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS
    return max(5, resolved_timeout)


def _collect_verify_consistency_diagnostics(
    run_state,
    host_row,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
    timeout=None,
):
    scope = _collect_gateway_channel_chaincode_pairs(run_state, host_row)
    channel_ids = [channel_id for channel_id in scope.get("channels", []) if channel_id]
    if not channel_ids:
        return {
            "available": False,
            "channels": [],
            "details": {"reason": "no_scoped_channels"},
        }

    run_id = str(run_state.get("run_id", "") or "").strip()
    remote_steps = [
        "COGNUS_RUN_ID={run_id}; ".format(run_id=shlex.quote(run_id)),
    ]
    for channel_id in channel_ids:
        channel_q = shlex.quote(channel_id)
        remote_steps.extend(
            [
                "COGNUS_DIAG_ROOT=/tmp/cognus-run-${{COGNUS_RUN_ID}}-{channel}; ".format(
                    channel=channel_id
                ),
                "printf '%s%s\\n' '__COGNUS_CHANNEL__=' {channel_q}; ".format(
                    channel_q=channel_q
                ),
                "printf '%s%s\\n' 'block_sha=' \"$(cat \"$COGNUS_DIAG_ROOT/channel.block.sha256\" 2>/dev/null | head -n 1 | tr -d '[:space:]' || true)\"; ",
                "printf '%s%s\\n' 'block_size=' \"$(cat \"$COGNUS_DIAG_ROOT/channel.block.size\" 2>/dev/null | head -n 1 | tr -d '[:space:]' || true)\"; ",
                "printf '%s%s\\n' 'inspect_json_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/channel.block.inspect.json\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' 'inspect_err_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/channel.block.inspect.err\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' 'participation_state_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/participation-state.json\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' 'participation_detail_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/participation-detail.json\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' 'post_log_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/participation-post.log\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' 'post_retry_log_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/participation-post-retry.log\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' 'orderer_log_tail_b64=' \"$(head -c 4096 \"$COGNUS_DIAG_ROOT/orderer-log-tail.txt\" 2>/dev/null | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' '__COGNUS_CHANNEL_END__=' {channel_q}; ".format(
                    channel_q=channel_q
                ),
            ]
        )

    probe_result = _run_remote_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command="".join(remote_steps),
        identity_file=identity_file,
        timeout=timeout or max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
    )

    channels = []
    current = {}
    for raw_line in str(probe_result.get("stdout", "") or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith("__COGNUS_CHANNEL__="):
            if current:
                channels.append(current)
            current = {"channel_id": line.split("=", 1)[1].strip().lower()}
            continue
        if line.startswith("__COGNUS_CHANNEL_END__="):
            if current:
                channels.append(current)
                current = {}
            continue
        if "=" not in line or not current:
            continue
        key, value = line.split("=", 1)
        key = str(key or "").strip().lower()
        value = str(value or "").strip()
        if key == "block_size":
            try:
                current["block_size"] = int(value or 0)
            except (TypeError, ValueError):
                current["block_size"] = 0
        elif key.endswith("_b64"):
            current[key[:-4]] = _decode_b64_excerpt(value)
        else:
            current[key] = value
    if current:
        channels.append(current)

    return {
        "available": bool(channels),
        "channels": channels,
        "returncode": _coerce_exit_code(probe_result.get("returncode", 1), default=1),
        "stderr": str(probe_result.get("stderr", "") or "")[:512],
    }


def _collect_remote_chaincode_commit_matrix(
    run_state,
    host_row,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
    timeout=None,
):
    scope = _collect_gateway_channel_chaincode_pairs(run_state, host_row)
    pairs = scope.get("pairs", [])
    if not pairs:
        return {
            "available": False,
            "org_id": scope.get("org_id", ""),
            "rows": [],
            "details": {"reason": "no_channel_chaincode_pairs"},
        }

    peer_container = _resolve_runtime_container_name_for_node_type(run_state, host_row, "peer")
    peer_host_row = _resolve_runtime_host_row_for_node_type(run_state, host_row, "peer")
    peer_node_id = str(peer_host_row.get("node_id", "") or "").strip()
    organization = _resolve_topology_organization_for_host(run_state, host_row)
    organization_msp_id = _resolve_topology_organization_msp_id(
        organization,
        peer_host_row,
    )
    run_id = str(run_state.get("run_id", "") or "").strip()
    remote_steps = [
        "COGNUS_PEER_CONTAINER={peer}; ".format(peer=shlex.quote(peer_container)),
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ] || ! docker inspect \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1; then "
        "COGNUS_PEER_CONTAINER=$(docker ps -a --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --filter label=cognus.node_id={node_id} --format '{{{{.Names}}}}' | head -n 1); "
        "fi; ".format(
            run_id=shlex.quote(run_id),
            node_id=shlex.quote(peer_node_id),
        ),
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then "
        "COGNUS_PEER_CONTAINER=$(docker ps -a --filter label=cognus.run_id={run_id} --filter label=cognus.node_type=peer --format '{{{{.Names}}}}' | head -n 1); "
        "fi; ".format(run_id=shlex.quote(run_id)),
        "if [ -n \"$COGNUS_PEER_CONTAINER\" ] && ! docker ps --format '{{.Names}}' | grep -Fx \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1; then docker start \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1 || true; fi; ",
        "if [ -z \"$COGNUS_PEER_CONTAINER\" ]; then printf '%s\\n' '__COGNUS_ERROR__=missing_peer_container'; exit 0; fi; ",
        "COGNUS_ORG_MSP_ID={org_msp_id}; ".format(
            org_msp_id=shlex.quote(str(organization_msp_id or "").strip())
        ),
        "COGNUS_PEER_LOCAL_MSPID=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"printf %s \\\"${CORE_PEER_LOCALMSPID:-}\\\"\" 2>/dev/null | head -n 1); ",
        "COGNUS_PEER_CORE_LOCAL_MSPID=$(docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"awk -F': ' '/^[[:space:]]*localMspId:/ {{print \\$2; exit}}' /etc/hyperledger/fabric/core.yaml 2>/dev/null | tr -d '[:space:]'\" 2>/dev/null | head -n 1); ",
        "COGNUS_PEER_SAMPLE_MSP=0; "
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -f /etc/hyperledger/fabric/msp/.cognus-sample-msp || test -f /var/lib/cognus/msp/.cognus-sample-msp || test -s /etc/hyperledger/fabric/msp/signcerts/peer.pem || test -s /var/lib/cognus/msp/signcerts/peer.pem' >/dev/null 2>&1; then COGNUS_PEER_SAMPLE_MSP=1; fi; ",
        "if [ -n \"$COGNUS_PEER_CORE_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=$COGNUS_PEER_CORE_LOCAL_MSPID; fi; ",
        "if [ \"$COGNUS_PEER_SAMPLE_MSP\" = \"1\" ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; fi; ",
        "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ] && [ -n \"$COGNUS_ORG_MSP_ID\" ]; then COGNUS_PEER_LOCAL_MSPID=$COGNUS_ORG_MSP_ID; fi; ",
        "if [ \"$COGNUS_PEER_LOCAL_MSPID\" = sampleorg ]; then COGNUS_PEER_LOCAL_MSPID=SampleOrg; fi; ",
        "if [ -z \"$COGNUS_PEER_LOCAL_MSPID\" ]; then COGNUS_PEER_LOCAL_MSPID=Org1MSP; fi; ",
        "COGNUS_PEER_ADMIN_MSPCONFIGPATH=/etc/hyperledger/fabric/msp; ",
        "if docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc 'test -s /tmp/cognus/admin-msp/signcerts/cert.pem && find /tmp/cognus/admin-msp/keystore -type f | head -n 1 >/dev/null 2>&1' >/dev/null 2>&1; then COGNUS_PEER_ADMIN_MSPCONFIGPATH=/tmp/cognus/admin-msp; fi; ",
        "COGNUS_ALT_LOCAL_MSPID=$COGNUS_PEER_LOCAL_MSPID; "
        "if [ \"$COGNUS_PEER_SAMPLE_MSP\" != \"1\" ]; then "
        "if [ \"$COGNUS_ALT_LOCAL_MSPID\" = SampleOrg ]; then COGNUS_ALT_LOCAL_MSPID=Org1MSP; else COGNUS_ALT_LOCAL_MSPID=SampleOrg; fi; "
        "fi; ",
        "COGNUS_PRIMARY_CHANNEL_VALIDATE=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer channel list 2>&1 || true); ",
        "if printf '%s\\n' \"$COGNUS_PRIMARY_CHANNEL_VALIDATE\" | grep -Eqi 'creator org unknown|expected MSP ID|principal deserialization failure|MSP is not defined'; then "
        "COGNUS_ALT_CHANNEL_VALIDATE=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_ALT_LOCAL_MSPID peer channel list 2>&1 || true); "
        "if ! printf '%s\\n' \"$COGNUS_ALT_CHANNEL_VALIDATE\" | grep -Eqi 'creator org unknown|expected MSP ID|principal deserialization failure|MSP is not defined'; then "
        "COGNUS_PREV_LOCAL_MSPID=$COGNUS_PEER_LOCAL_MSPID; "
        "COGNUS_PEER_LOCAL_MSPID=$COGNUS_ALT_LOCAL_MSPID; "
        "COGNUS_ALT_LOCAL_MSPID=$COGNUS_PREV_LOCAL_MSPID; "
        "fi; "
        "fi; ",
        "COGNUS_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode queryinstalled 2>/dev/null || true); ",
        "if [ -z \"$COGNUS_QUERYINSTALLED\" ] || printf '%s\\n' \"$COGNUS_QUERYINSTALLED\" | grep -Eqi 'creator org unknown|expected MSP ID|principal deserialization failure|MSP is not defined'; then "
        "COGNUS_QUERYINSTALLED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_ALT_LOCAL_MSPID peer lifecycle chaincode queryinstalled 2>/dev/null || true); "
        "fi; ",
    ]
    for pair in pairs:
        channel_id = str(pair.get("channel_id", "") or "").strip().lower()
        chaincode_id = str(pair.get("chaincode_id", "") or "").strip().lower()
        if not channel_id or not chaincode_id:
            continue
        pair_key = "{}|{}".format(channel_id, chaincode_id)
        remote_steps.extend(
            [
                "printf '%s%s\\n' '__COGNUS_PAIR__=' {pair_q}; ".format(
                    pair_q=shlex.quote(pair_key)
                ),
                "COGNUS_QUERYCOMMITTED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null || true); ".format(
                    channel=shlex.quote(channel_id)
                ),
                "if [ -z \"$COGNUS_QUERYCOMMITTED\" ] || printf '%s\\n' \"$COGNUS_QUERYCOMMITTED\" | grep -Eqi 'creator org unknown|expected MSP ID|principal deserialization failure|MSP is not defined'; then "
                "COGNUS_QUERYCOMMITTED=$(docker exec \"$COGNUS_PEER_CONTAINER\" env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_ALT_LOCAL_MSPID peer lifecycle chaincode querycommitted -C {channel} 2>/dev/null || true); "
                "fi; ".format(channel=shlex.quote(channel_id)),
                "printf '%s%s\\n' 'committed=' \"$(printf '%s\\n' \"$COGNUS_QUERYCOMMITTED\" | grep -F {needle} >/dev/null 2>&1 && echo 1 || echo 0)\"; ".format(
                    needle=shlex.quote("Name: {}".format(chaincode_id))
                ),
                "printf '%s%s\\n' 'package_id=' \"$(printf '%s\\n' \"$COGNUS_QUERYINSTALLED\" | grep -i 'Package ID:' | grep -i {chaincode} | head -n 1 | sed -E 's/.*Package ID:[[:space:]]*([^,[:space:]]+).*/\\1/' | tr -d '\\r[:space:]' || true)\"; ".format(
                    chaincode=shlex.quote(chaincode_id)
                ),
                "printf '%s%s\\n' 'querycommitted_b64=' \"$(printf '%s' \"$COGNUS_QUERYCOMMITTED\" | base64 -w0 2>/dev/null || true)\"; ",
                "printf '%s%s\\n' '__COGNUS_PAIR_END__=' {pair_q}; ".format(
                    pair_q=shlex.quote(pair_key)
                ),
            ]
        )

    probe_result = _run_remote_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command="".join(remote_steps),
        identity_file=identity_file,
        timeout=timeout or max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
    )

    rows = []
    current = {}
    missing_peer = False
    for raw_line in str(probe_result.get("stdout", "") or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line == "__COGNUS_ERROR__=missing_peer_container":
            missing_peer = True
            continue
        if line.startswith("__COGNUS_PAIR__="):
            if current:
                rows.append(current)
            pair_value = line.split("=", 1)[1].strip().lower()
            channel_id, _, chaincode_id = pair_value.partition("|")
            current = {
                "channel_id": channel_id,
                "chaincode_id": chaincode_id,
            }
            continue
        if line.startswith("__COGNUS_PAIR_END__="):
            if current:
                rows.append(current)
                current = {}
            continue
        if "=" not in line or not current:
            continue
        key, value = line.split("=", 1)
        key = str(key or "").strip().lower()
        value = str(value or "").strip()
        if key == "committed":
            current["committed"] = value == "1"
        elif key == "querycommitted_b64":
            current["querycommitted"] = _decode_b64_excerpt(value)
        else:
            current[key] = value
    if current:
        rows.append(current)

    return {
        "available": not missing_peer,
        "org_id": scope.get("org_id", ""),
        "rows": rows,
        "returncode": _coerce_exit_code(probe_result.get("returncode", 1), default=1),
        "stderr": str(probe_result.get("stderr", "") or "")[:512],
        "details": {
            "missing_peer_container": missing_peer,
            "scoped_pairs": pairs,
        },
    }


def _build_gateway_verify_probe_checks(run_state, host_row, committed_rows=None):
    scope = _collect_gateway_channel_chaincode_pairs(run_state, host_row)
    org_id = str(scope.get("org_id", "") or "").strip().lower()
    candidate_pairs = []
    if isinstance(committed_rows, list) and committed_rows:
        for row in committed_rows:
            if not isinstance(row, dict) or not bool(row.get("committed", False)):
                continue
            channel_id = str(row.get("channel_id", "") or "").strip().lower()
            chaincode_id = str(row.get("chaincode_id", "") or "").strip().lower()
            if channel_id and chaincode_id:
                candidate_pairs.append(
                    {
                        "channel_id": channel_id,
                        "chaincode_id": chaincode_id,
                    }
                )
    if not candidate_pairs:
        candidate_pairs = [
            {
                "channel_id": str(row.get("channel_id", "") or "").strip().lower(),
                "chaincode_id": str(row.get("chaincode_id", "") or "").strip().lower(),
            }
            for row in scope.get("pairs", [])
            if isinstance(row, dict)
            and str(row.get("channel_id", "") or "").strip()
            and str(row.get("chaincode_id", "") or "").strip()
        ]

    probe_checks = [{"path": "/health", "method": "GET"}]
    for pair in candidate_pairs:
        headers = {"Content-Type": "application/json"}
        if org_id:
            headers["X-Fabric-Org"] = org_id
        probe_checks.append(
            {
                "path": "/api/{}/{}/query/search".format(
                    pair.get("channel_id", ""),
                    pair.get("chaincode_id", ""),
                ),
                "method": "POST",
                "headers": headers,
                "body": _default_ccapi_search_payload(),
            }
        )

    return {
        "org_id": org_id,
        "pairs": candidate_pairs,
        "checks": probe_checks,
    }


def _assess_gateway_final_gate(
    run_state,
    host_row,
    host_address,
    host_port,
    ssh_user,
    ssh_port,
    identity_file="",
    timeout_seconds=5,
    http_probe_timeout_seconds=None,
    force_refresh=False,
):
    diagnostics = {}
    commit_matrix = {}
    cache_key = ""
    if isinstance(run_state, dict):
        cache_key = _gateway_verify_scope_cache_key(run_state, host_row, host_address)
        if cache_key and not force_refresh:
            cached_scope = _gateway_verify_scope_cache_get(run_state, cache_key)
            diagnostics = (
                cached_scope.get("diagnostics", {})
                if isinstance(cached_scope.get("diagnostics", {}), dict)
                else {}
            )
            commit_matrix = (
                cached_scope.get("commit_matrix", {})
                if isinstance(cached_scope.get("commit_matrix", {}), dict)
                else {}
            )
    if not diagnostics:
        diagnostics = _collect_verify_consistency_diagnostics(
            run_state,
            host_row,
            host_address,
            ssh_user,
            ssh_port,
            identity_file=identity_file,
            timeout=timeout_seconds,
        )
    if not commit_matrix:
        commit_matrix = _collect_remote_chaincode_commit_matrix(
            run_state,
            host_row,
            host_address,
            ssh_user,
            ssh_port,
            identity_file=identity_file,
            timeout=timeout_seconds,
        )
    if cache_key:
        _upsert_gateway_verify_scope_cache(
            run_state,
            cache_key,
            diagnostics,
            commit_matrix,
        )
    probe_spec = _build_gateway_verify_probe_checks(
        run_state,
        host_row,
        committed_rows=commit_matrix.get("rows", []),
    )
    expected_rows = [row for row in commit_matrix.get("rows", []) if isinstance(row, dict)]
    uncommitted_rows = [row for row in expected_rows if not bool(row.get("committed", False))]
    if expected_rows and uncommitted_rows:
        return {
            "ok": False,
            "diagnostics": diagnostics,
            "commit_matrix": commit_matrix,
            "probe_spec": probe_spec,
            "probe": None,
            "failure_code": "runbook_fabric_chaincode_not_committed",
            "failure_message": "Chaincode nao foi comprometido no canal Fabric esperado.",
            "failure_details": {
                "uncommitted_pairs": uncommitted_rows,
                "commit_matrix": commit_matrix,
                "fabric_consistency_diagnostics": diagnostics,
            },
        }

    probe = _probe_chaincode_gateway_api(
        host_address,
        host_port,
        timeout_seconds=_resolve_gateway_http_probe_timeout(
            timeout_seconds=timeout_seconds,
            http_probe_timeout_seconds=http_probe_timeout_seconds,
        ),
        required_checks=probe_spec.get("checks", []),
    )
    loopback_probe = {}
    if not bool(probe.get("available", False)):
        loopback_probe = _probe_chaincode_gateway_api_via_ssh(
            host_address,
            host_port,
            ssh_user,
            ssh_port,
            identity_file=identity_file,
            timeout_seconds=_resolve_gateway_http_probe_timeout(
                timeout_seconds=timeout_seconds,
                http_probe_timeout_seconds=http_probe_timeout_seconds,
            ),
            required_checks=probe_spec.get("checks", []),
        )
        if bool(loopback_probe.get("available", False)):
            loopback_details = (
                dict(loopback_probe.get("details", {}))
                if isinstance(loopback_probe.get("details", {}), dict)
                else {}
            )
            loopback_details["external_probe"] = probe
            loopback_probe = dict(loopback_probe)
            loopback_probe["details"] = loopback_details
            return {
                "ok": True,
                "diagnostics": diagnostics,
                "commit_matrix": commit_matrix,
                "probe_spec": probe_spec,
                "probe": loopback_probe,
                "failure_code": "",
                "failure_message": "",
                "failure_details": {},
            }
        if loopback_probe:
            probe_details = (
                dict(probe.get("details", {}))
                if isinstance(probe.get("details", {}), dict)
                else {}
            )
            probe_details["ssh_loopback_probe"] = loopback_probe
            probe = dict(probe)
            probe["details"] = probe_details
    if bool(probe.get("available", False)):
        return {
            "ok": True,
            "diagnostics": diagnostics,
            "commit_matrix": commit_matrix,
            "probe_spec": probe_spec,
            "probe": probe,
            "failure_code": "",
            "failure_message": "",
            "failure_details": {},
        }

    failed_check = (
        probe.get("details", {})
        if isinstance(probe.get("details", {}), dict)
        else {}
    )
    first_failed_body = str(failed_check.get("first_failed_body", "") or "")
    failure_code = "runbook_apigateway_functional_unavailable"
    failure_message = (
        "Gateway acessivel, mas endpoint funcional de channel/chaincode permanece indisponivel."
    )
    if "chaincode" in first_failed_body.lower() and "not found" in first_failed_body.lower():
        failure_code = "runbook_fabric_chaincode_not_committed"
        failure_message = "Gateway reporta chaincode nao encontrado no canal durante smoke ccapi."
    elif (
        "couchdb" in first_failed_body.lower()
        or "bookmark" in first_failed_body.lower()
        or "executequerywithmetadata not supported" in first_failed_body.lower()
        or "getqueryresultwithpagination call error" in first_failed_body.lower()
    ):
        failure_code = "runbook_fabric_rich_query_unavailable"
        failure_message = "Rich query/paginacao via gateway ccapi nao esta funcional."

    return {
        "ok": False,
        "diagnostics": diagnostics,
        "commit_matrix": commit_matrix,
        "probe_spec": probe_spec,
        "probe": probe,
        "failure_code": failure_code,
        "failure_message": failure_message,
        "failure_details": {
            "gateway_probe": probe,
            "commit_matrix": commit_matrix,
            "fabric_consistency_diagnostics": diagnostics,
        },
    }


def _probe_chaincode_gateway_api(
    host_address,
    host_port,
    timeout_seconds=5,
    required_paths=None,
    required_checks=None,
):
    """
    Lightweight HTTP probe to the chaincode-gateway API on the target host.
    Returns a dict with availability and diagnostics; does NOT raise.
    """
    result = {
        "available": False,
        "host_address": host_address,
        "host_port": host_port,
        "checked_at_utc": _utc_now(),
        "details": {},
    }
    try:
        import urllib.request
        import urllib.error

        if not host_address or not host_port:
            result["details"]["reason"] = "missing_host_or_port"
            return result

        probe_results = []
        for check in _gateway_probe_checks(required_paths=required_paths, required_checks=required_checks):
            method = str(check.get("method", "GET") or "GET").strip().upper()
            path = str(check.get("path", "") or "").strip()
            headers = check.get("headers", {}) if isinstance(check.get("headers"), dict) else {}
            body_text = str(check.get("body", "") or "")
            body_bytes = body_text.encode("utf-8") if body_text else None
            url = f"http://{host_address}:{host_port}{path}"
            try:
                req = urllib.request.Request(
                    url,
                    data=body_bytes,
                    method=method,
                )
                for header_key, header_value in headers.items():
                    req.add_header(str(header_key), str(header_value))
                with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                    status_code = getattr(resp, "status", None) or resp.getcode()
                    response_body = ""
                    try:
                        response_body = (resp.read() or b"").decode(
                            "utf-8", errors="replace"
                        )
                    except Exception:
                        response_body = ""
                    probe_results.append(
                        {
                            "path": path,
                            "method": method,
                            "url": url,
                            "status_code": int(status_code or 0),
                            "ok": bool(status_code and int(status_code) < 500),
                            "body": response_body[:256],
                            "request_body_sha256": hashlib.sha256(body_bytes).hexdigest() if body_bytes else "",
                        }
                    )
            except urllib.error.HTTPError as he:
                error_body = ""
                try:
                    error_body = (he.read() or b"").decode("utf-8", errors="replace")
                except Exception:
                    error_body = ""
                probe_results.append(
                    {
                        "path": path,
                        "method": method,
                        "url": url,
                        "status_code": int(getattr(he, "code", 0) or 0),
                        "ok": bool(getattr(he, "code", 0) and int(he.code) < 500),
                        "body": error_body[:256],
                        "request_body_sha256": hashlib.sha256(body_bytes).hexdigest() if body_bytes else "",
                    }
                )
            except Exception as e:
                probe_results.append(
                    {
                        "path": path,
                        "method": method,
                        "url": url,
                        "status_code": 0,
                        "ok": False,
                        "error": str(e)[:256],
                        "request_body_sha256": hashlib.sha256(body_bytes).hexdigest() if body_bytes else "",
                    }
                )

        result["details"]["checks"] = probe_results
        if probe_results:
            result["details"]["last_url"] = probe_results[-1].get("url", "")
            result["details"]["status_code"] = int(
                probe_results[-1].get("status_code", 0) or 0
            )
        result["available"] = bool(probe_results) and all(
            bool(check.get("ok", False)) for check in probe_results
        )
        if not result["available"]:
            first_failed_check = next(
                (check for check in probe_results if not bool(check.get("ok", False))),
                None,
            )
            if isinstance(first_failed_check, dict):
                result["details"]["first_failed_path"] = first_failed_check.get(
                    "path", ""
                )
                result["details"]["first_failed_status_code"] = int(
                    first_failed_check.get("status_code", 0) or 0
                )
                result["details"]["first_failed_body"] = str(
                    first_failed_check.get("body", "") or ""
                )[:256]
        return result
    except Exception as exc:
        result["details"]["probe_exception"] = str(exc)[:512]
        return result


def _probe_chaincode_gateway_api_via_ssh(
    host_address,
    host_port,
    ssh_user,
    ssh_port,
    identity_file="",
    timeout_seconds=5,
    required_paths=None,
    required_checks=None,
):
    result = {
        "available": False,
        "host_address": host_address,
        "host_port": host_port,
        "checked_at_utc": _utc_now(),
        "details": {"probe_transport": "ssh_loopback"},
    }
    if not host_address or not host_port or not ssh_user:
        result["details"]["reason"] = "missing_gateway_probe_ssh_context"
        return result

    checks = _gateway_probe_checks(
        required_paths=required_paths,
        required_checks=required_checks,
    )
    if not checks:
        result["details"]["reason"] = "missing_required_checks"
        return result

    checks_b64 = base64.b64encode(
        json.dumps(
            checks,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii")
    loopback_candidates = ["127.0.0.1", "localhost"]
    remote_script = (
        "import base64, hashlib, json, subprocess, urllib.error, urllib.request\n"
        "checks = json.loads(base64.b64decode(%r).decode('utf-8'))\n"
        "timeout = %d\n"
        "host_port = %d\n"
        "loopbacks = %s\n"
        "try:\n"
        "    hostname_i = subprocess.check_output(['sh', '-lc', 'hostname -I 2>/dev/null || true'], text=True).strip()\n"
        "except Exception:\n"
        "    hostname_i = ''\n"
        "for candidate in hostname_i.split():\n"
        "    if candidate and candidate not in loopbacks:\n"
        "        loopbacks.insert(0, candidate)\n"
        "def run_probe(loopback):\n"
        "    probe_results = []\n"
        "    for check in checks:\n"
        "        method = str(check.get('method', 'GET') or 'GET').strip().upper()\n"
        "        path = str(check.get('path', '') or '').strip()\n"
        "        headers = check.get('headers', {}) if isinstance(check.get('headers'), dict) else {}\n"
        "        body_text = str(check.get('body', '') or '')\n"
        "        body_bytes = body_text.encode('utf-8') if body_text else None\n"
        "        url = 'http://{}:{}{}'.format(loopback, host_port, path)\n"
        "        try:\n"
        "            req = urllib.request.Request(url, data=body_bytes, method=method)\n"
        "            for header_key, header_value in headers.items():\n"
        "                req.add_header(str(header_key), str(header_value))\n"
        "            with urllib.request.urlopen(req, timeout=timeout) as resp:\n"
        "                status_code = getattr(resp, 'status', None) or resp.getcode()\n"
        "                response_body = ''\n"
        "                try:\n"
        "                    response_body = (resp.read() or b'').decode('utf-8', errors='replace')\n"
        "                except Exception:\n"
        "                    response_body = ''\n"
        "                probe_results.append({'path': path, 'method': method, 'url': url, 'status_code': int(status_code or 0), 'ok': bool(status_code and int(status_code) < 500), 'body': response_body[:256], 'request_body_sha256': hashlib.sha256(body_bytes).hexdigest() if body_bytes else ''})\n"
        "        except urllib.error.HTTPError as he:\n"
        "            error_body = ''\n"
        "            try:\n"
        "                error_body = (he.read() or b'').decode('utf-8', errors='replace')\n"
        "            except Exception:\n"
        "                error_body = ''\n"
        "            probe_results.append({'path': path, 'method': method, 'url': url, 'status_code': int(getattr(he, 'code', 0) or 0), 'ok': bool(getattr(he, 'code', 0) and int(he.code) < 500), 'body': error_body[:256], 'request_body_sha256': hashlib.sha256(body_bytes).hexdigest() if body_bytes else ''})\n"
        "        except Exception as exc:\n"
        "            probe_results.append({'path': path, 'method': method, 'url': url, 'status_code': 0, 'ok': False, 'error': str(exc)[:256], 'request_body_sha256': hashlib.sha256(body_bytes).hexdigest() if body_bytes else ''})\n"
        "    result = {'available': bool(probe_results) and all(bool(check.get('ok', False)) for check in probe_results), 'host_address': loopback, 'host_port': host_port, 'details': {'checks': probe_results}}\n"
        "    if probe_results:\n"
        "        result['details']['last_url'] = probe_results[-1].get('url', '')\n"
        "        result['details']['status_code'] = int(probe_results[-1].get('status_code', 0) or 0)\n"
        "    if not result['available']:\n"
        "        first_failed_check = next((check for check in probe_results if not bool(check.get('ok', False))), None)\n"
        "        if isinstance(first_failed_check, dict):\n"
        "            result['details']['first_failed_path'] = first_failed_check.get('path', '')\n"
        "            result['details']['first_failed_status_code'] = int(first_failed_check.get('status_code', 0) or 0)\n"
        "            result['details']['first_failed_body'] = str(first_failed_check.get('body', '') or '')[:256]\n"
        "    return result\n"
        "best = None\n"
        "for loopback in loopbacks:\n"
        "    current = run_probe(loopback)\n"
        "    if best is None:\n"
        "        best = current\n"
        "    if current.get('available', False):\n"
        "        best = current\n"
        "        break\n"
        "print(json.dumps(best or {'available': False, 'host_address': '', 'host_port': host_port, 'details': {}}))\n"
    ) % (
        checks_b64,
        max(5, int(timeout_seconds or 5)),
        int(host_port or 0),
        json.dumps(loopback_candidates),
    )
    probe_result = _run_remote_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command="python3 -c {}".format(shlex.quote(remote_script)),
        identity_file=identity_file,
        timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, int(timeout_seconds or 5) + 10),
    )
    remote_stdout = str(probe_result.get("stdout", "") or "").strip()
    remote_payload = {}
    if remote_stdout:
        try:
            remote_payload = json.loads(remote_stdout.splitlines()[-1])
        except Exception:
            remote_payload = {}
    if not isinstance(remote_payload, dict):
        remote_payload = {}

    loopback_host = str(remote_payload.get("host_address", "") or "").strip()
    details = (
        dict(remote_payload.get("details", {}))
        if isinstance(remote_payload.get("details", {}), dict)
        else {}
    )
    details["probe_transport"] = "ssh_loopback"
    if loopback_host:
        details["loopback_host"] = loopback_host
    normalized_result = {
        "available": bool(remote_payload.get("available", False)),
        "host_address": host_address,
        "host_port": host_port,
        "checked_at_utc": _utc_now(),
        "details": details,
    }
    ssh_probe_returncode = _coerce_exit_code(
        probe_result.get("returncode", 1),
        default=1,
    )
    if ssh_probe_returncode != 0:
        normalized_result["details"]["ssh_probe_returncode"] = ssh_probe_returncode
        normalized_result["details"]["ssh_probe_stdout"] = remote_stdout[:512]
        normalized_result["details"]["ssh_probe_stderr"] = str(
            probe_result.get("stderr", "") or ""
        )[:512]
    if not remote_payload and remote_stdout:
        normalized_result["details"]["ssh_probe_stdout"] = remote_stdout[:512]
    return normalized_result


def _ensure_remote_docker_ready(
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    check_timeout = max(
        RUNBOOK_SSH_TIMEOUT_SECONDS + 5,
        RUNBOOK_PROVISION_RUNTIME_TIMEOUT_SECONDS,
    )

    check = _run_remote_command(
        host_address,
        ssh_user,
        ssh_port,
        "PATH=/usr/bin:/usr/local/bin:/bin:/usr/sbin docker --version",
        identity_file=identity_file,
        timeout=check_timeout,
    )

    retry_attempts = 0
    max_retry_attempts = RUNBOOK_SSH_TRANSIENT_RETRY_ATTEMPTS
    while retry_attempts < max_retry_attempts and _is_transient_ssh_failure(
        check.get("returncode", 1),
        check.get("stderr", ""),
        check.get("timed_out", False),
    ):
        retry_attempts += 1
        if RUNBOOK_SSH_TRANSIENT_RETRY_DELAY_SECONDS > 0:
            try:
                import time

                time.sleep(RUNBOOK_SSH_TRANSIENT_RETRY_DELAY_SECONDS)
            except Exception:
                pass

        check = _run_remote_command(
            host_address,
            ssh_user,
            ssh_port,
            "PATH=/usr/bin:/usr/local/bin:/bin:/usr/sbin docker --version",
            identity_file=identity_file,
            timeout=check_timeout,
        )

    if check["returncode"] == 0 and not check.get("timed_out"):
        return {
            "attempted": False,
            "status": "ready",
            "stdout": check.get("stdout", ""),
            "stderr": check.get("stderr", ""),
            "returncode": check.get("returncode", 0),
        }
    return {
        "attempted": False,
        "status": "missing",
        "reason": "docker_not_found",
        "stdout": check.get("stdout", ""),
        "stderr": check.get("stderr", ""),
        "timed_out": check.get("timed_out", False),
        "returncode": check.get("returncode", 1),
    }


def _runtime_host_connection_cache_key(
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    cache_material = "|".join(
        [
            str(host_address or "").strip().lower(),
            str(ssh_user or "").strip().lower(),
            str(ssh_port or 22).strip(),
            str(identity_file or "").strip(),
        ]
    )
    return hashlib.sha256(cache_material.encode("utf-8")).hexdigest()[:32]


def _runtime_host_docker_ready_cache_store(run_state):
    cache_store = run_state.setdefault("runtime_host_docker_ready_cache", {})
    if not isinstance(cache_store, dict):
        cache_store = {}
        run_state["runtime_host_docker_ready_cache"] = cache_store
    return cache_store


def _runtime_host_docker_ready_cache_get(run_state, cache_key):
    cache_store = _runtime_host_docker_ready_cache_store(run_state)
    cache_row = cache_store.get(str(cache_key or "").strip(), {})
    if not isinstance(cache_row, dict):
        return {}
    return dict(cache_row)


def _upsert_runtime_host_docker_ready_cache(run_state, cache_key, ready_row):
    cache_store = _runtime_host_docker_ready_cache_store(run_state)
    normalized_cache_key = str(cache_key or "").strip()
    cache_row = dict(ready_row) if isinstance(ready_row, dict) else {}
    cache_row["cache_key"] = normalized_cache_key
    cache_row.pop("cache_hit", None)
    cache_store[normalized_cache_key] = cache_row
    return dict(cache_row)


def _runtime_host_image_warmup_cache_store(run_state):
    cache_store = run_state.setdefault("runtime_host_image_warmup_cache", {})
    if not isinstance(cache_store, dict):
        cache_store = {}
        run_state["runtime_host_image_warmup_cache"] = cache_store
    return cache_store


def _runtime_host_image_warmup_cache_get(run_state, cache_key):
    cache_store = _runtime_host_image_warmup_cache_store(run_state)
    cache_row = cache_store.get(str(cache_key or "").strip(), {})
    if not isinstance(cache_row, dict):
        return {}
    return dict(cache_row)


def _upsert_runtime_host_image_warmup_cache(run_state, cache_key, warmup_row):
    cache_store = _runtime_host_image_warmup_cache_store(run_state)
    normalized_cache_key = str(cache_key or "").strip()
    cache_row = dict(warmup_row) if isinstance(warmup_row, dict) else {}
    cache_row["cache_key"] = normalized_cache_key
    cache_row.pop("cache_hit", None)
    cache_store[normalized_cache_key] = cache_row
    return dict(cache_row)


def _collect_runtime_images_for_host_rows(host_rows):
    collected_by_image = {}
    ordered_images = []

    for host_row in host_rows or []:
        if not isinstance(host_row, dict):
            continue
        node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        if not node_type or node_type == "chaincode":
            continue
        runtime_image = _normalize_runtime_image_reference(
            node_type,
            str(host_row.get("runtime_image", "") or "").strip(),
            _resolve_runtime_image_for_node_type(node_type),
        )
        if not runtime_image:
            continue
        image_key = runtime_image.lower()
        image_row = collected_by_image.get(image_key)
        if image_row is None:
            image_row = {
                "node_type": node_type,
                "runtime_image": runtime_image,
                "node_types": [],
                "node_ids": [],
            }
            collected_by_image[image_key] = image_row
            ordered_images.append(image_row)

        if node_type not in image_row["node_types"]:
            image_row["node_types"].append(node_type)
        node_id = str(host_row.get("node_id", "") or "").strip()
        if node_id and node_id not in image_row["node_ids"]:
            image_row["node_ids"].append(node_id)
        if image_row.get("node_type") not in ("apigateway", "netapi") and node_type in (
            "apigateway",
            "netapi",
        ):
            image_row["node_type"] = node_type

    return [dict(image_row) for image_row in ordered_images]


def _build_runtime_host_connection_groups(host_mapping, credential_by_machine):
    grouped_rows = {}
    safe_credentials = credential_by_machine if isinstance(credential_by_machine, dict) else {}

    for host_row in host_mapping or []:
        normalized_host_row, normalized_error = _normalize_host_ssh_target(host_row)
        if normalized_error:
            continue
        node_type = _normalize_runtime_component_node_type(
            normalized_host_row.get("node_type", "")
        )
        if node_type == "chaincode":
            continue

        machine_id = _resolve_machine_id_from_host_row(host_row if isinstance(host_row, dict) else {})
        identity_file = _ensure_machine_credential_identity_file(
            safe_credentials,
            machine_id,
        )
        connection_key = _runtime_host_connection_cache_key(
            normalized_host_row.get("host_address", ""),
            normalized_host_row.get("ssh_user", ""),
            normalized_host_row.get("ssh_port", 22),
            identity_file=identity_file,
        )
        grouped_rows.setdefault(connection_key, []).append(normalized_host_row)

    return grouped_rows


def _select_runtime_host_rows_for_warmup(host_rows, current_host_row):
    safe_current_host_row = current_host_row if isinstance(current_host_row, dict) else {}
    if not isinstance(host_rows, list):
        host_rows = []

    current_node_type = _normalize_runtime_component_node_type(
        safe_current_host_row.get("node_type", "")
    )
    current_runtime_image = _normalize_runtime_image_reference(
        current_node_type,
        str(safe_current_host_row.get("runtime_image", "") or "").strip(),
        _resolve_runtime_image_for_node_type(current_node_type),
    )
    current_runtime_image_key = str(current_runtime_image or "").strip().lower()
    current_rank = _runtime_host_execution_rank(safe_current_host_row)

    selected_rows = []
    for host_row in host_rows:
        if not isinstance(host_row, dict):
            continue
        candidate_node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        if candidate_node_type == "chaincode":
            continue

        candidate_runtime_image = _normalize_runtime_image_reference(
            candidate_node_type,
            str(host_row.get("runtime_image", "") or "").strip(),
            _resolve_runtime_image_for_node_type(candidate_node_type),
        )
        candidate_runtime_image_key = str(candidate_runtime_image or "").strip().lower()
        candidate_rank = _runtime_host_execution_rank(host_row)

        if current_runtime_image_key and candidate_runtime_image_key == current_runtime_image_key:
            selected_rows.append(host_row)
            continue
        if candidate_rank == current_rank and candidate_node_type == current_node_type:
            selected_rows.append(host_row)

    if selected_rows:
        return selected_rows
    return [safe_current_host_row] if safe_current_host_row else []


def _ensure_remote_docker_ready_cached(
    run_state,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    cache_key = _runtime_host_connection_cache_key(
        host_address,
        ssh_user,
        ssh_port,
        identity_file=identity_file,
    )
    cached_row = _runtime_host_docker_ready_cache_get(run_state, cache_key)
    if cached_row:
        cached_row["cache_hit"] = True
        return cached_row

    ready_row = _ensure_remote_docker_ready(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        identity_file=identity_file,
    )
    cached_ready_row = _upsert_runtime_host_docker_ready_cache(
        run_state,
        cache_key,
        ready_row,
    )
    cached_ready_row["cache_hit"] = False
    return cached_ready_row


def _runtime_image_warmup_prefers_local_seed(node_type, runtime_image):
    normalized_node_type = _normalize_runtime_component_node_type(node_type)
    normalized_runtime_image = str(runtime_image or "").strip().lower()
    if normalized_node_type in ("apigateway", "netapi"):
        return True
    return normalized_runtime_image.startswith("cognus/")


def _ensure_runtime_host_image_warmup(
    run_state,
    host_rows,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    connection_key = _runtime_host_connection_cache_key(
        host_address,
        ssh_user,
        ssh_port,
        identity_file=identity_file,
    )
    runtime_images = _collect_runtime_images_for_host_rows(host_rows)
    requested_images = [
        str(image_row.get("runtime_image", "") or "").strip()
        for image_row in runtime_images
        if str(image_row.get("runtime_image", "") or "").strip()
    ]

    if not RUNBOOK_RUNTIME_HOST_IMAGE_WARMUP_ENABLED:
        return {
            "cache_key": connection_key,
            "host_address": str(host_address or "").strip(),
            "requested_images": requested_images,
            "attempted": False,
            "worker_count": 0,
            "status": "skipped",
            "reason": "runtime_host_image_warmup_disabled",
            "rows": [],
            "cache_hit": False,
        }

    if not runtime_images:
        return {
            "cache_key": connection_key,
            "host_address": str(host_address or "").strip(),
            "requested_images": requested_images,
            "attempted": False,
            "worker_count": 0,
            "status": "skipped",
            "reason": "runtime_host_image_warmup_no_images",
            "rows": [],
            "cache_hit": False,
        }

    cached_warmup = _runtime_host_image_warmup_cache_get(run_state, connection_key)
    if (
        cached_warmup
        and cached_warmup.get("requested_images", []) == requested_images
    ):
        cached_warmup["cache_hit"] = True
        return cached_warmup

    warmup_rows = []
    pull_targets = []
    for image_row in runtime_images:
        probe_before = _probe_remote_runtime_image(
            host_address=host_address,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            runtime_image=image_row.get("runtime_image", ""),
            identity_file=identity_file,
        )
        warmup_row = {
            "runtime_image": image_row.get("runtime_image", ""),
            "node_type": image_row.get("node_type", ""),
            "node_types": list(image_row.get("node_types", []) or []),
            "node_ids": list(image_row.get("node_ids", []) or []),
            "probe_before": probe_before,
            "status": "ready" if probe_before.get("available", False) else "missing",
            "reason": (
                "runtime_image_already_available_remotely"
                if probe_before.get("available", False)
                else "runtime_image_missing_remotely"
            ),
        }
        warmup_rows.append(warmup_row)
        if not probe_before.get("available", False):
            pull_targets.append(warmup_row)

    worker_count = min(
        max(1, RUNBOOK_RUNTIME_HOST_IMAGE_WARMUP_MAX_WORKERS),
        max(1, len(pull_targets)),
    )
    if pull_targets:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_row = {
                executor.submit(
                    _warm_runtime_image_on_host,
                    runtime_image=str(warmup_row.get("runtime_image", "") or "").strip(),
                    node_type=str(warmup_row.get("node_type", "") or "").strip(),
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    identity_file=identity_file,
                ): warmup_row
                for warmup_row in pull_targets
            }
            for future in as_completed(future_to_row):
                warmup_row = future_to_row[future]
                try:
                    pull_result = future.result()
                except Exception as exc:
                    pull_result = {
                        "attempted": True,
                        "status": "failed",
                        "reason": "runtime_host_image_warmup_pull_exception",
                        "stderr": str(exc)[:512],
                    }

                probe_after = _probe_remote_runtime_image(
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    runtime_image=warmup_row.get("runtime_image", ""),
                    identity_file=identity_file,
                )
                warmup_row["pull_result"] = pull_result
                warmup_row["probe_after"] = probe_after
                if probe_after.get("available", False):
                    warmup_row["status"] = "ready"
                    warmup_row["reason"] = str(
                        pull_result.get("reason", "runtime_image_pulled_remotely")
                    ).strip() or "runtime_image_pulled_remotely"
                else:
                    warmup_row["status"] = "failed"
                    warmup_row["reason"] = str(
                        pull_result.get(
                            "reason",
                            "runtime_image_unavailable_after_host_warmup",
                        )
                    ).strip() or "runtime_image_unavailable_after_host_warmup"
    else:
        worker_count = 0

    ready_count = sum(1 for warmup_row in warmup_rows if warmup_row.get("status") == "ready")
    if ready_count == len(warmup_rows):
        warmup_status = "ready"
    elif ready_count > 0:
        warmup_status = "partial"
    else:
        warmup_status = "failed"

    warmup_summary = {
        "cache_key": connection_key,
        "host_address": str(host_address or "").strip(),
        "requested_images": requested_images,
        "attempted": bool(pull_targets),
        "worker_count": worker_count,
        "status": warmup_status,
        "reason": (
            "runtime_host_image_warmup_completed"
            if warmup_status in {"ready", "partial"}
            else "runtime_host_image_warmup_failed"
        ),
        "rows": warmup_rows,
        "updated_at_utc": _utc_now(),
    }
    cached_summary = _upsert_runtime_host_image_warmup_cache(
        run_state,
        connection_key,
        warmup_summary,
    )
    cached_summary["cache_hit"] = False
    return cached_summary


def _seed_chaincode_package_remote_via_ssh(
    chaincode_id,
    host_address,
    ssh_user,
    ssh_port,
    identity_file="",
):
    return {
        "chaincode_id": str(chaincode_id or "").strip().lower(),
        "host_address": str(host_address or "").strip(),
        "ssh_user": str(ssh_user or "").strip(),
        "ssh_port": int(ssh_port or 22),
        "identity_file": str(identity_file or "").strip(),
        "status": "skipped",
        "reason": "remote_seed_not_required",
    }


def _run_host_ssh_preflight_probe(host_row, change_id, machine_credential=None):
    normalized_host_row, normalized_error = _normalize_host_ssh_target(host_row)
    host_ref = str(normalized_host_row.get("host_ref", "") or "").strip()
    host_address = str(normalized_host_row.get("host_address", "") or "").strip()
    ssh_user = str(normalized_host_row.get("ssh_user", "") or "").strip()
    ssh_port = int(normalized_host_row.get("ssh_port", 22) or 22)
    checked_at_utc = _utc_now()
    safe_machine_credential = (
        machine_credential if isinstance(machine_credential, dict) else {}
    )
    identity_file = _materialize_identity_file(safe_machine_credential)
    credential_binding = {
        "machine_id": str(safe_machine_credential.get("machine_id", "") or "").strip(),
        "credential_ref": str(safe_machine_credential.get("credential_ref", "") or "").strip(),
        "credential_fingerprint": str(
            safe_machine_credential.get("credential_fingerprint", "") or ""
        ).strip(),
        "identity_file": str(identity_file or "").strip(),
        "reuse_confirmed": bool(safe_machine_credential.get("reuse_confirmed", False)),
    }

    if normalized_error:
        return {
            "host_id": host_ref or host_address,
            "infra_label": host_ref,
            "host_ref": host_ref,
            "host_address": host_address,
            "ssh_user": ssh_user,
            "ssh_port": ssh_port,
            "status": "bloqueado",
            "checked_at_utc": checked_at_utc,
            "primary_cause": normalized_error.get(
                "message", "Host mapping inválido para preflight técnico."
            ),
            "recommended_action": "Corrigir dados de conexão SSH e reexecutar o preflight técnico.",
            "diagnostics": {
                "code": normalized_error.get("code", "runbook_host_mapping_invalid"),
                "stderr": "",
                "stdout": "",
                "exit_code": 255,
            },
            "credential_binding": credential_binding,
        }

    remote_command = _build_ssh_preflight_remote_command(change_id)
    ssh_command = _build_ssh_command(
        host_address=host_address,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        remote_command=remote_command,
        identity_file=credential_binding.get("identity_file", ""),
    )

    execution_result = subprocess.run(
        ssh_command,
        capture_output=True,
        text=True,
        timeout=RUNBOOK_SSH_TIMEOUT_SECONDS + 3,
    )
    stdout_text = (execution_result.stdout or "").strip()
    stderr_text = (execution_result.stderr or "").strip()
    primary_cause, recommended_action = _resolve_preflight_failure_feedback(
        stderr_text,
        credential_binding.get("identity_file", ""),
    )

    if execution_result.returncode == 0:
        return {
            "host_id": host_ref or host_address,
            "infra_label": host_ref,
            "host_ref": host_ref,
            "host_address": host_address,
            "ssh_user": ssh_user,
            "ssh_port": ssh_port,
            "status": "apto",
            "checked_at_utc": checked_at_utc,
            "primary_cause": "Sem pendências técnicas.",
            "recommended_action": "Sem ação imediata.",
            "diagnostics": {
                "code": "ssh_connectivity_ok",
                "stderr": stderr_text[:512],
                "stdout": stdout_text[:512],
                "exit_code": execution_result.returncode,
            },
            "credential_binding": credential_binding,
        }

    return {
        "host_id": host_ref or host_address,
        "infra_label": host_ref,
        "host_ref": host_ref,
        "host_address": host_address,
        "ssh_user": ssh_user,
        "ssh_port": ssh_port,
        "status": "bloqueado",
        "checked_at_utc": checked_at_utc,
        "primary_cause": primary_cause,
        "recommended_action": recommended_action,
        "diagnostics": {
            "code": "ssh_connectivity_failed",
            "stderr": stderr_text[:512],
            "stdout": stdout_text[:512],
            "exit_code": execution_result.returncode,
        },
        "credential_binding": credential_binding,
    }


def _build_ssh_preflight_report(change_id, host_mapping, machine_credentials=None):
    credential_by_machine = {}
    for credential_row in machine_credentials or []:
        if not isinstance(credential_row, dict):
            continue
        machine_id = str(credential_row.get("machine_id", "") or "").strip()
        if machine_id:
            credential_by_machine[machine_id] = credential_row

    host_rows = []
    for host_row in host_mapping:
        machine_id = _resolve_machine_id_from_host_row(host_row)
        host_rows.append(
            _run_host_ssh_preflight_probe(
                host_row,
                change_id,
                credential_by_machine.get(machine_id),
            )
        )

    summary, overall_status = _summarize_preflight_hosts(host_rows)
    return {
        "change_id": str(change_id or "").strip(),
        "executed_at_utc": _utc_now(),
        "overall_status": overall_status,
        "summary": summary,
        "hosts": host_rows,
    }


def _host_preflight_is_ready(host_row):
    host_status = str(host_row.get("preflight_status", "apto")).strip().lower()
    return host_status in ("apto", "ready", "ok", "pass")


def _runtime_host_execution_rank(host_row):
    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    rank_by_node_type = {
        "ca": 10,
        "couch": 20,
        "couchdb": 20,
        "orderer": 30,
        "peer": 40,
        "chaincode": 50,
        "apigateway": 60,
        "netapi": 70,
    }
    return rank_by_node_type.get(node_type, 90)


def _ordered_runtime_host_mapping_for_provision(host_mapping):
    if not isinstance(host_mapping, list):
        return []

    indexed_rows = []
    for index, host_row in enumerate(host_mapping):
        if not isinstance(host_row, dict):
            continue
        node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        if node_type == "chaincode":
            continue
        indexed_rows.append((index, host_row))

    indexed_rows.sort(
        key=lambda item: (
            _runtime_host_execution_rank(item[1]),
            str(item[1].get("host_ref", "") or "").strip().lower(),
            str(item[1].get("org_id", "") or "").strip().lower(),
            str(item[1].get("node_id", "") or "").strip().lower(),
            item[0],
        )
    )
    return [item[1] for item in indexed_rows]


def _execute_ssh_checkpoint_actions(run_state, stage_key, checkpoint_key):
    host_mapping = run_state.get("host_mapping", [])
    if stage_key == "provision" and checkpoint_key == "provision.runtime":
        host_mapping = _ordered_runtime_host_mapping_for_provision(host_mapping)
    credential_by_machine = {}
    for credential_row in run_state.get("machine_credentials", []) or []:
        if not isinstance(credential_row, dict):
            continue
        machine_id = str(credential_row.get("machine_id", "") or "").strip()
        if machine_id:
            credential_by_machine[machine_id] = credential_row

    runtime_host_connection_groups = {}
    runtime_host_warmup_evidence_attached = set()
    if stage_key == "provision" and checkpoint_key == "provision.runtime":
        runtime_host_connection_groups = _build_runtime_host_connection_groups(
            host_mapping,
            credential_by_machine,
        )

    host_evidences = []
    for host_row in host_mapping:
        normalized_host_row, normalized_error = _normalize_host_ssh_target(host_row)
        if normalized_error:
            return None, normalized_error

        source_host_row = host_row if isinstance(host_row, dict) else {}

        safe_host_row = _redact_sensitive_host_fields(normalized_host_row)
        host_address = str(normalized_host_row.get("host_address", "")).strip()
        ssh_user = str(normalized_host_row.get("ssh_user", "")).strip()
        ssh_port = int(normalized_host_row.get("ssh_port", 22) or 22)
        host_ref = str(normalized_host_row.get("host_ref", "")).strip()
        org_id = str(normalized_host_row.get("org_id", "")).strip()
        node_id = str(normalized_host_row.get("node_id", "")).strip()
        node_type = _normalize_runtime_component_node_type(
            normalized_host_row.get("node_type", "")
        )
        runtime_image = _normalize_runtime_image_reference(
            node_type,
            str(normalized_host_row.get("runtime_image", "") or "").strip(),
            _resolve_runtime_image_for_node_type(node_type),
        )
        if node_type == "chaincode":
            continue
        machine_id = _resolve_machine_id_from_host_row(source_host_row)
        credential_binding = _redact_sensitive_host_fields(
            credential_by_machine.get(machine_id, {})
        )

        if isinstance(credential_binding, dict) and not credential_binding.get("identity_file"):
            identity_file = _ensure_machine_credential_identity_file(
                credential_by_machine,
                machine_id,
            )
            if identity_file:
                credential_binding = dict(credential_binding)
                credential_binding["identity_file"] = identity_file
        runtime_host_connection_key = _runtime_host_connection_cache_key(
            host_address,
            ssh_user,
            ssh_port,
            identity_file=credential_binding.get("identity_file", ""),
        )

        if not _host_preflight_is_ready(normalized_host_row):
            return None, _build_runbook_error_payload(
                "runbook_runtime_check_failed",
                "Runtime check bloqueou continuidade da etapa no host alvo.",
                {
                    "host_ref": host_ref,
                    "host_address": host_address,
                    "org_id": org_id,
                    "node_id": node_id,
                    "preflight_status": normalized_host_row.get("preflight_status", ""),
                },
            )

        remote_docker_ready = None
        runtime_image_preseed = None
        runtime_image_host_warmup = None
        if stage_key == "provision" and checkpoint_key == "provision.runtime":
            remote_docker_ready = _ensure_remote_docker_ready_cached(
                run_state,
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                identity_file=credential_binding.get("identity_file", ""),
            )

            if node_type in ("apigateway", "netapi"):
                _, _, scoped_chaincode_ids, _ = _collect_apigateway_org_scope(
                    run_state,
                    normalized_host_row,
                )
                chaincode_package_seed_results = []
                for scoped_chaincode_id in scoped_chaincode_ids:
                    chaincode_package_seed_results.append(
                        _seed_chaincode_package_remote_via_ssh(
                            chaincode_id=scoped_chaincode_id,
                            host_address=host_address,
                            ssh_user=ssh_user,
                            ssh_port=ssh_port,
                            identity_file=credential_binding.get("identity_file", ""),
                        )
                    )
                if chaincode_package_seed_results:
                    if runtime_image_preseed is None:
                        runtime_image_preseed = {}
                    runtime_image_preseed["chaincode_package_seed"] = (
                        chaincode_package_seed_results
                    )

            if remote_docker_ready.get("status") != "ready":
                host_evidence = {
                    "run_id": run_state.get("run_id", ""),
                    "change_id": run_state.get("change_id", ""),
                    "stage": stage_key,
                    "checkpoint": checkpoint_key,
                    "host_ref": host_ref,
                    "host_address": host_address,
                    "org_id": org_id,
                    "node_id": node_id,
                    "executor_mode": "ssh-orchestrator",
                    "command_hash": "",
                    "fingerprint_sha256": _sha256_payload(
                        {
                            "run_id": run_state.get("run_id", ""),
                            "change_id": run_state.get("change_id", ""),
                            "stage": stage_key,
                            "checkpoint": checkpoint_key,
                            "host_ref": host_ref,
                            "org_id": org_id,
                            "host_address": host_address,
                            "node_id": node_id,
                            "reason": "remote_docker_unavailable",
                        }
                    ),
                    "stdout": str(remote_docker_ready.get("stdout", ""))[:512],
                    "stderr": str(remote_docker_ready.get("stderr", ""))[:512],
                    "exit_code": remote_docker_ready.get("returncode", 1),
                    "timestamp_utc": _utc_now(),
                    "host_mapping": safe_host_row,
                    "credential_binding": credential_binding,
                    "remote_docker_ready": remote_docker_ready,
                }
                host_evidences.append(host_evidence)

                failure_details = {
                    "stage": stage_key,
                    "checkpoint": checkpoint_key,
                    "host_ref": host_ref,
                    "host_address": host_address,
                    "org_id": org_id,
                    "node_id": node_id,
                    "remote_docker_status": remote_docker_ready.get("status", "unknown"),
                    "remote_docker_reason": remote_docker_ready.get("reason", ""),
                    "remote_docker_details": remote_docker_ready,
                }
                return host_evidences, _build_runbook_error_payload(
                    "runbook_remote_docker_unavailable",
                    "Host remoto não apresenta o cliente Docker; provision.runtime requer Docker pré-instalado ou auto-install habilitado.",
                    failure_details,
                )

            runtime_image_host_warmup = _ensure_runtime_host_image_warmup(
                run_state,
                _select_runtime_host_rows_for_warmup(
                    runtime_host_connection_groups.get(
                        runtime_host_connection_key,
                        [normalized_host_row],
                    ),
                    normalized_host_row,
                ),
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                identity_file=credential_binding.get("identity_file", ""),
            )

            if _runtime_image_preseed_required_for_node_type(node_type):
                preseed_cache_entry = _runtime_image_preseed_cache_get(
                    run_state,
                    host_address=host_address,
                    runtime_image=runtime_image,
                )
                remote_image_probe = _probe_remote_runtime_image(
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    runtime_image=runtime_image,
                    identity_file=credential_binding.get("identity_file", ""),
                )
                runtime_image_preseed = {
                    "required": True,
                    "node_type": node_type,
                    "runtime_image": runtime_image,
                    "remote_probe_before_seed": remote_image_probe,
                    "attempted": False,
                    "status": "skipped",
                    "reason": "runtime_image_already_available_remotely",
                    "preseed_cache_before": preseed_cache_entry,
                }

                if remote_image_probe.get("available", False):
                    cache_after = _upsert_runtime_image_preseed_cache(
                        run_state,
                        host_address=host_address,
                        runtime_image=runtime_image,
                        status="available",
                        reason="runtime_image_already_available_remotely",
                        details={
                            "node_type": node_type,
                            "runtime_image": runtime_image,
                        },
                        bump_attempt=False,
                    )
                    runtime_image_preseed["preseed_cache_after"] = cache_after
                else:
                    backoff_reason = _runtime_image_preseed_cache_backoff_reason(
                        preseed_cache_entry
                    )
                    if backoff_reason:
                        runtime_image_preseed["status"] = "blocked"
                        runtime_image_preseed["reason"] = backoff_reason
                        cooldown_remaining = (
                            _runtime_image_preseed_cache_cooldown_remaining_seconds(
                                preseed_cache_entry
                            )
                        )
                        if cooldown_remaining > 0:
                            runtime_image_preseed["cooldown_remaining_seconds"] = (
                                cooldown_remaining
                            )
                        host_evidences.append(
                            {
                                "run_id": run_state.get("run_id", ""),
                                "change_id": run_state.get("change_id", ""),
                                "stage": stage_key,
                                "checkpoint": checkpoint_key,
                                "host_ref": host_ref,
                                "host_address": host_address,
                                "org_id": org_id,
                                "node_id": node_id,
                                "executor_mode": "ssh-orchestrator",
                                "command_hash": "",
                                "fingerprint_sha256": _sha256_payload(
                                    {
                                        "run_id": run_state.get("run_id", ""),
                                        "change_id": run_state.get("change_id", ""),
                                        "stage": stage_key,
                                        "checkpoint": checkpoint_key,
                                        "host_ref": host_ref,
                                        "org_id": org_id,
                                        "host_address": host_address,
                                        "node_id": node_id,
                                        "runtime_image": runtime_image,
                                        "reason": backoff_reason,
                                    }
                                ),
                                "stdout": "",
                                "stderr": "",
                                "exit_code": 1,
                                "timestamp_utc": _utc_now(),
                                "host_mapping": safe_host_row,
                                "credential_binding": credential_binding,
                                "runtime_image_preseed": runtime_image_preseed,
                            }
                        )
                        return host_evidences, _build_runbook_error_payload(
                            "runbook_runtime_image_preseed_failed",
                            "Pre-seed de imagem em backoff para evitar repeticao de tentativa pesada no mesmo run.",
                            {
                                "stage": stage_key,
                                "checkpoint": checkpoint_key,
                                "host_ref": host_ref,
                                "host_address": host_address,
                                "org_id": org_id,
                                "node_id": node_id,
                                "node_type": node_type,
                                "runtime_image": runtime_image,
                                "runtime_image_preseed": runtime_image_preseed,
                            },
                        )

                    cache_running = _upsert_runtime_image_preseed_cache(
                        run_state,
                        host_address=host_address,
                        runtime_image=runtime_image,
                        status="running",
                        reason="runtime_image_preseed_started",
                        details={
                            "node_type": node_type,
                            "runtime_image": runtime_image,
                        },
                        bump_attempt=True,
                    )
                    runtime_image_preseed["attempt"] = cache_running.get("attempts", 1)
                    runtime_image_preseed["preseed_cache_running"] = cache_running

                    min_memory_mb_raw = str(
                        RUNBOOK_RUNTIME_MIN_AVAILABLE_MEMORY_MB_BY_NODE_TYPE.get(
                            node_type, "0"
                        )
                    ).strip()
                    try:
                        min_memory_mb = int(min_memory_mb_raw or 0)
                    except (TypeError, ValueError):
                        min_memory_mb = 0
                    min_memory_kb = max(0, min_memory_mb * 1024)

                    memory_probe = _probe_remote_mem_available_kb(
                        host_address=host_address,
                        ssh_user=ssh_user,
                        ssh_port=ssh_port,
                        identity_file=credential_binding.get("identity_file", ""),
                    )
                    runtime_image_preseed["remote_memory_probe"] = memory_probe

                    memory_probe_succeeded = bool(
                        memory_probe.get("returncode", 1) == 0
                        and not memory_probe.get("timed_out", False)
                    )
                    if (
                        min_memory_kb > 0
                        and memory_probe_succeeded
                        and memory_probe.get("mem_available_kb", 0) < min_memory_kb
                    ):
                        runtime_image_preseed["status"] = "blocked"
                        runtime_image_preseed["reason"] = "runtime_image_preseed_memory_pressure"
                        cache_after = _upsert_runtime_image_preseed_cache(
                            run_state,
                            host_address=host_address,
                            runtime_image=runtime_image,
                            status="failed",
                            reason="runtime_image_preseed_memory_pressure",
                            details={
                                "required_memory_mb": min_memory_mb,
                                "available_memory_kb": memory_probe.get(
                                    "mem_available_kb", 0
                                ),
                            },
                            bump_attempt=False,
                        )
                        runtime_image_preseed["preseed_cache_after"] = cache_after
                        host_evidences.append(
                            {
                                "run_id": run_state.get("run_id", ""),
                                "change_id": run_state.get("change_id", ""),
                                "stage": stage_key,
                                "checkpoint": checkpoint_key,
                                "host_ref": host_ref,
                                "host_address": host_address,
                                "org_id": org_id,
                                "node_id": node_id,
                                "executor_mode": "ssh-orchestrator",
                                "command_hash": "",
                                "fingerprint_sha256": _sha256_payload(
                                    {
                                        "run_id": run_state.get("run_id", ""),
                                        "change_id": run_state.get("change_id", ""),
                                        "stage": stage_key,
                                        "checkpoint": checkpoint_key,
                                        "host_ref": host_ref,
                                        "org_id": org_id,
                                        "host_address": host_address,
                                        "node_id": node_id,
                                        "reason": "runtime_image_preseed_memory_pressure",
                                    }
                                ),
                                "stdout": str(memory_probe.get("stdout", ""))[:512],
                                "stderr": str(memory_probe.get("stderr", ""))[:512],
                                "exit_code": memory_probe.get("returncode", 1),
                                "timestamp_utc": _utc_now(),
                                "host_mapping": safe_host_row,
                                "credential_binding": credential_binding,
                                "runtime_image_preseed": runtime_image_preseed,
                            }
                        )
                        return host_evidences, _build_runbook_error_payload(
                            "runbook_runtime_image_preseed_memory_pressure",
                            "Memoria disponivel insuficiente no host remoto para seed seguro da imagem de runtime.",
                            {
                                "stage": stage_key,
                                "checkpoint": checkpoint_key,
                                "host_ref": host_ref,
                                "host_address": host_address,
                                "org_id": org_id,
                                "node_id": node_id,
                                "node_type": node_type,
                                "runtime_image": runtime_image,
                                "required_memory_mb": min_memory_mb,
                                "available_memory_kb": memory_probe.get("mem_available_kb", 0),
                            },
                        )

                    remote_image_available = False
                    if _runtime_image_preseed_pull_first_for_node_type(node_type):
                        pull_result = _pull_runtime_image_via_ssh(
                            runtime_image=runtime_image,
                            node_type=node_type,
                            host_address=host_address,
                            ssh_user=ssh_user,
                            ssh_port=ssh_port,
                            identity_file=credential_binding.get("identity_file", ""),
                        )
                        runtime_image_preseed["attempted"] = bool(
                            pull_result.get("attempted", False)
                        )
                        runtime_image_preseed["remote_pull_result"] = pull_result
                        remote_image_probe_after_pull = _probe_remote_runtime_image(
                            host_address=host_address,
                            ssh_user=ssh_user,
                            ssh_port=ssh_port,
                            runtime_image=runtime_image,
                            identity_file=credential_binding.get("identity_file", ""),
                        )
                        runtime_image_preseed["remote_probe_after_pull"] = (
                            remote_image_probe_after_pull
                        )
                        remote_image_available = bool(
                            remote_image_probe_after_pull.get("available", False)
                        )
                        if remote_image_available:
                            runtime_image_preseed["status"] = "ready"
                            runtime_image_preseed["reason"] = "runtime_image_pulled_remotely"
                            cache_after = _upsert_runtime_image_preseed_cache(
                                run_state,
                                host_address=host_address,
                                runtime_image=runtime_image,
                                status="available",
                                reason="runtime_image_pulled_remotely",
                                details={"pull_result": pull_result},
                                bump_attempt=False,
                            )
                            runtime_image_preseed["preseed_cache_after"] = cache_after

                    if not remote_image_available:
                        local_image_ready = _ensure_local_runtime_image_available(
                            node_type=node_type,
                            runtime_image=runtime_image,
                        )
                        runtime_image_preseed["local_image_ready"] = local_image_ready
                        if str(local_image_ready.get("status", "")).strip().lower() != "ready":
                            cache_after = _upsert_runtime_image_preseed_cache(
                                run_state,
                                host_address=host_address,
                                runtime_image=runtime_image,
                                status="failed",
                                reason="runtime_image_local_unavailable_for_preseed",
                                details={"local_image_ready": local_image_ready},
                                bump_attempt=False,
                            )
                            runtime_image_preseed["preseed_cache_after"] = cache_after
                            host_evidences.append(
                                {
                                    "run_id": run_state.get("run_id", ""),
                                    "change_id": run_state.get("change_id", ""),
                                    "stage": stage_key,
                                    "checkpoint": checkpoint_key,
                                    "host_ref": host_ref,
                                    "host_address": host_address,
                                    "org_id": org_id,
                                    "node_id": node_id,
                                    "executor_mode": "ssh-orchestrator",
                                    "command_hash": "",
                                    "fingerprint_sha256": _sha256_payload(
                                        {
                                            "run_id": run_state.get("run_id", ""),
                                            "change_id": run_state.get("change_id", ""),
                                            "stage": stage_key,
                                            "checkpoint": checkpoint_key,
                                            "host_ref": host_ref,
                                            "org_id": org_id,
                                            "host_address": host_address,
                                            "node_id": node_id,
                                            "runtime_image": runtime_image,
                                            "reason": "runtime_image_local_unavailable_for_preseed",
                                        }
                                    ),
                                    "stdout": "",
                                    "stderr": str(local_image_ready.get("reason", ""))[:512],
                                    "exit_code": 1,
                                    "timestamp_utc": _utc_now(),
                                    "host_mapping": safe_host_row,
                                    "credential_binding": credential_binding,
                                    "runtime_image_preseed": runtime_image_preseed,
                                }
                            )
                            return host_evidences, _build_runbook_error_payload(
                                "runbook_runtime_image_local_unavailable",
                                "Imagem de runtime indisponivel localmente no orquestrador para seed seguro no host alvo.",
                                {
                                    "stage": stage_key,
                                    "checkpoint": checkpoint_key,
                                    "host_ref": host_ref,
                                    "host_address": host_address,
                                    "org_id": org_id,
                                    "node_id": node_id,
                                    "node_type": node_type,
                                    "runtime_image": runtime_image,
                                    "runtime_image_preseed": runtime_image_preseed,
                                },
                            )

                        seed_result = _seed_runtime_image_via_ssh(
                            runtime_image=runtime_image,
                            host_address=host_address,
                            ssh_user=ssh_user,
                            ssh_port=ssh_port,
                            identity_file=credential_binding.get("identity_file", ""),
                        )
                        runtime_image_preseed["attempted"] = True
                        runtime_image_preseed["seed_result"] = seed_result
                        runtime_image_preseed["status"] = str(
                            seed_result.get("status", "failed")
                        )
                        runtime_image_preseed["reason"] = str(
                            seed_result.get("reason", "")
                        ).strip()

                        remote_image_probe_after_seed = _probe_remote_runtime_image(
                            host_address=host_address,
                            ssh_user=ssh_user,
                            ssh_port=ssh_port,
                            runtime_image=runtime_image,
                            identity_file=credential_binding.get("identity_file", ""),
                        )
                        runtime_image_preseed["remote_probe_after_seed"] = (
                            remote_image_probe_after_seed
                        )

                        if not remote_image_probe_after_seed.get("available", False):
                            cache_after = _upsert_runtime_image_preseed_cache(
                                run_state,
                                host_address=host_address,
                                runtime_image=runtime_image,
                                status="failed",
                                reason="runtime_image_preseed_failed",
                                details={"seed_result": seed_result},
                                bump_attempt=False,
                            )
                            runtime_image_preseed["preseed_cache_after"] = cache_after
                            seed_exit_code = seed_result.get("exit_code", 1)
                            try:
                                seed_exit_code = int(seed_exit_code or 1)
                            except (TypeError, ValueError):
                                seed_exit_code = 1
                            host_evidences.append(
                                {
                                    "run_id": run_state.get("run_id", ""),
                                    "change_id": run_state.get("change_id", ""),
                                    "stage": stage_key,
                                    "checkpoint": checkpoint_key,
                                    "host_ref": host_ref,
                                    "host_address": host_address,
                                    "org_id": org_id,
                                    "node_id": node_id,
                                    "executor_mode": "ssh-orchestrator",
                                    "command_hash": "",
                                    "fingerprint_sha256": _sha256_payload(
                                        {
                                            "run_id": run_state.get("run_id", ""),
                                            "change_id": run_state.get("change_id", ""),
                                            "stage": stage_key,
                                            "checkpoint": checkpoint_key,
                                            "host_ref": host_ref,
                                            "org_id": org_id,
                                            "host_address": host_address,
                                            "node_id": node_id,
                                            "runtime_image": runtime_image,
                                            "reason": "runtime_image_preseed_failed",
                                        }
                                    ),
                                    "stdout": str(seed_result.get("stdout", ""))[:512],
                                    "stderr": str(seed_result.get("stderr", ""))[:512],
                                    "exit_code": seed_exit_code,
                                    "timestamp_utc": _utc_now(),
                                    "host_mapping": safe_host_row,
                                    "credential_binding": credential_binding,
                                    "runtime_image_preseed": runtime_image_preseed,
                                }
                            )
                            return host_evidences, _build_runbook_error_payload(
                                "runbook_runtime_image_preseed_failed",
                                "Seed preventivo da imagem de runtime falhou; host remoto segue sem imagem obrigatoria.",
                                {
                                    "stage": stage_key,
                                    "checkpoint": checkpoint_key,
                                    "host_ref": host_ref,
                                    "host_address": host_address,
                                    "org_id": org_id,
                                    "node_id": node_id,
                                    "node_type": node_type,
                                    "runtime_image": runtime_image,
                                    "runtime_image_preseed": runtime_image_preseed,
                                },
                            )

                        runtime_image_preseed["status"] = "ready"
                        runtime_image_preseed["reason"] = "runtime_image_seeded"
                        cache_after = _upsert_runtime_image_preseed_cache(
                            run_state,
                            host_address=host_address,
                            runtime_image=runtime_image,
                            status="available",
                            reason="runtime_image_seeded",
                            details={"seed_result": seed_result},
                            bump_attempt=False,
                        )
                        runtime_image_preseed["preseed_cache_after"] = cache_after

        remote_chaincode_package_seed = None
        if (
            (
                (stage_key == "provision" and checkpoint_key == "provision.runtime")
                or (stage_key == "verify" and checkpoint_key == "verify.consistency")
            )
            and node_type in ("apigateway", "netapi")
        ):
            remote_chaincode_package_seed = _seed_remote_chaincode_packages_for_host(
                run_state,
                normalized_host_row,
                host_address,
                ssh_user,
                ssh_port,
                identity_file=credential_binding.get("identity_file", ""),
                timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 30),
            )
            remote_chaincode_package_seed_error = (
                _build_remote_chaincode_package_seed_failure_payload(
                    run_state,
                    stage_key,
                    checkpoint_key,
                    host_ref,
                    host_address,
                    org_id,
                    node_id,
                    node_type,
                    remote_chaincode_package_seed,
                )
            )
            if remote_chaincode_package_seed_error:
                host_evidences.append(
                    {
                        "run_id": run_state.get("run_id", ""),
                        "change_id": run_state.get("change_id", ""),
                        "stage": stage_key,
                        "checkpoint": checkpoint_key,
                        "host_ref": host_ref,
                        "host_address": host_address,
                        "org_id": org_id,
                        "node_id": node_id,
                        "executor_mode": "ssh-orchestrator",
                        "stdout": "",
                        "stderr": "",
                        "exit_code": 1,
                        "timestamp_utc": _utc_now(),
                        "host_mapping": safe_host_row,
                        "credential_binding": credential_binding,
                        "remote_chaincode_package_seed": remote_chaincode_package_seed,
                    }
                )
                return host_evidences, remote_chaincode_package_seed_error

        remote_command = _build_ssh_remote_command(
            stage_key,
            checkpoint_key,
            run_state,
            normalized_host_row,
        )
        ssh_command = _build_ssh_command(
            host_address=host_address,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            remote_command=remote_command,
            identity_file=credential_binding.get("identity_file", ""),
        )

        ssh_timeout = _resolve_checkpoint_ssh_timeout(stage_key, checkpoint_key)

        execution_result = _run_remote_command(
            host_address=host_address,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            remote_command=remote_command,
            identity_file=credential_binding.get("identity_file", ""),
            timeout=ssh_timeout,
        )
        ssh_retry_evidence = None
        retry_attempts = 0
        max_retry_attempts = RUNBOOK_SSH_TRANSIENT_RETRY_ATTEMPTS
        while retry_attempts < max_retry_attempts and _is_transient_ssh_failure(
            execution_result.get("returncode", 1),
            execution_result.get("stderr", ""),
            execution_result.get("timed_out", False),
        ):
            retry_attempts += 1
            if RUNBOOK_SSH_TRANSIENT_RETRY_DELAY_SECONDS > 0:
                try:
                    import time

                    time.sleep(RUNBOOK_SSH_TRANSIENT_RETRY_DELAY_SECONDS)
                except Exception:
                    pass

            retry_result = _run_remote_command(
                host_address=host_address,
                ssh_user=ssh_user,
                ssh_port=ssh_port,
                remote_command=remote_command,
                identity_file=credential_binding.get("identity_file", ""),
                timeout=ssh_timeout,
            )
            ssh_retry_evidence = {
                "attempt": retry_attempts,
                "max_attempts": max_retry_attempts,
                "initial_returncode": execution_result.get("returncode", 1),
                "initial_timed_out": bool(execution_result.get("timed_out", False)),
                "initial_stderr": str(execution_result.get("stderr", ""))[:256],
                "retry_returncode": retry_result.get("returncode", 1),
                "retry_timed_out": bool(retry_result.get("timed_out", False)),
                "retry_stderr": str(retry_result.get("stderr", ""))[:256],
            }
            execution_result = retry_result
            if execution_result.get("returncode", 1) == 0 and not execution_result.get(
                "timed_out", False
            ):
                break

        raw_execution_returncode = execution_result.get("returncode", 1)
        try:
            execution_returncode = int(
                1 if raw_execution_returncode is None else raw_execution_returncode
            )
        except (TypeError, ValueError):
            execution_returncode = 1
        execution_timed_out = bool(execution_result.get("timed_out", False))
        stdout_text = str(execution_result.get("stdout", "") or "").strip()
        stderr_text = str(execution_result.get("stderr", "") or "").strip()

        host_evidence = {
            "run_id": run_state.get("run_id", ""),
            "change_id": run_state.get("change_id", ""),
            "stage": stage_key,
            "checkpoint": checkpoint_key,
            "host_ref": host_ref,
            "host_address": host_address,
            "org_id": org_id,
            "node_id": node_id,
            "executor_mode": "ssh-orchestrator",
            "command_hash": _sha256_payload({"command": remote_command}),
            "fingerprint_sha256": _sha256_payload(
                {
                    "run_id": run_state.get("run_id", ""),
                    "change_id": run_state.get("change_id", ""),
                    "stage": stage_key,
                    "checkpoint": checkpoint_key,
                    "host_ref": host_ref,
                    "org_id": org_id,
                    "host_address": host_address,
                    "node_id": node_id,
                    "command": remote_command,
                }
            ),
            "stdout": stdout_text[:512],
            "stderr": stderr_text[:512],
            "exit_code": execution_returncode,
            "timestamp_utc": _utc_now(),
            "host_mapping": safe_host_row,
            "credential_binding": credential_binding,
        }
        if ssh_retry_evidence is not None:
            host_evidence["ssh_retry"] = ssh_retry_evidence
        if remote_docker_ready is not None:
            host_evidence["remote_docker_ready"] = remote_docker_ready
        runtime_host_warmup_evidence_key = (
            runtime_host_connection_key,
            tuple(
                runtime_image_host_warmup.get("requested_images", [])
                if isinstance(runtime_image_host_warmup, dict)
                else []
            ),
        )
        if (
            runtime_image_host_warmup is not None
            and runtime_host_warmup_evidence_key
            not in runtime_host_warmup_evidence_attached
        ):
            host_evidence["runtime_image_host_warmup"] = runtime_image_host_warmup
            runtime_host_warmup_evidence_attached.add(runtime_host_warmup_evidence_key)
        if runtime_image_preseed is not None:
            host_evidence["runtime_image_preseed"] = runtime_image_preseed
        if remote_chaincode_package_seed is not None:
            host_evidence["remote_chaincode_package_seed"] = remote_chaincode_package_seed
        host_evidences.append(host_evidence)

        # After successful SSH execution we try an HTTP probe to the chaincode-gateway
        # on the target host to validate that the org-specific API is reachable.
        try:
            if stage_key == "provision" and checkpoint_key == "provision.runtime":
                try:
                    host_port = _resolve_runtime_exposed_host_port(run_state, normalized_host_row)
                    if host_port and int(host_port) > 0:
                        probe = _probe_chaincode_gateway_api(host_address, host_port)
                        host_evidence["chaincode_gateway_probe"] = probe
                    else:
                        host_evidence["chaincode_gateway_probe"] = {
                            "available": False,
                            "details": {"reason": "no_exposed_port_resolved"},
                        }
                except Exception as probe_exc:
                    host_evidence["chaincode_gateway_probe"] = {
                        "available": False,
                        "details": {"probe_exception": str(probe_exc)[:512]},
                    }
        except Exception:
            # Non-fatal: do not break runbook on probe errors
            pass

        if (
            execution_returncode == 0
            and stage_key == "verify"
            and checkpoint_key == "verify.consistency"
            and node_type in ("apigateway", "netapi")
        ):
            verify_host_port = _resolve_runtime_exposed_host_port(
                run_state,
                normalized_host_row,
            )
            verify_gate = _assess_gateway_final_gate(
                run_state,
                normalized_host_row,
                host_address,
                verify_host_port,
                ssh_user,
                ssh_port,
                identity_file=credential_binding.get("identity_file", ""),
                timeout_seconds=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
            )
            host_evidence["fabric_consistency_diagnostics"] = verify_gate.get(
                "diagnostics", {}
            )
            host_evidence["chaincode_commit_matrix"] = verify_gate.get(
                "commit_matrix", {}
            )
            host_evidence["chaincode_gateway_verify_probe_spec"] = verify_gate.get(
                "probe_spec", {}
            )
            if isinstance(run_state, dict):
                run_state.setdefault("chaincode_commit_matrix", {})[
                    str(node_id or host_ref or "gateway")
                ] = verify_gate.get("commit_matrix", {})
                run_state.setdefault("fabric_consistency_diagnostics", {})[
                    str(node_id or host_ref or "gateway")
                ] = verify_gate.get("diagnostics", {})

            verify_probe = (
                verify_gate.get("probe", {})
                if isinstance(verify_gate.get("probe"), dict)
                else {}
            )
            if verify_probe:
                host_evidence["chaincode_gateway_verify_probe"] = verify_probe

            if not bool(verify_gate.get("ok", False)):
                verify_repair_result = _run_remote_command(
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    remote_command=remote_command,
                    identity_file=credential_binding.get("identity_file", ""),
                    timeout=ssh_timeout,
                )
                verify_repair_returncode = _coerce_exit_code(
                    verify_repair_result.get("returncode", 1),
                    default=1,
                )
                verify_repair_stderr = str(
                    verify_repair_result.get("stderr", "") or ""
                ).strip()
                verify_repair_stdout = str(
                    verify_repair_result.get("stdout", "") or ""
                ).strip()

                verify_gate_after_repair = _assess_gateway_final_gate(
                    run_state,
                    normalized_host_row,
                    host_address,
                    verify_host_port,
                    ssh_user,
                    ssh_port,
                    identity_file=credential_binding.get("identity_file", ""),
                    timeout_seconds=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
                    force_refresh=True,
                )
                verify_probe_after_repair = (
                    verify_gate_after_repair.get("probe", {})
                    if isinstance(verify_gate_after_repair.get("probe"), dict)
                    else {}
                )
                host_evidence["chaincode_gateway_verify_repair"] = {
                    "returncode": verify_repair_returncode,
                    "stdout": verify_repair_stdout[:512],
                    "stderr": verify_repair_stderr[:512],
                    "probe_after_repair": verify_probe_after_repair,
                    "commit_matrix_after_repair": verify_gate_after_repair.get(
                        "commit_matrix", {}
                    ),
                    "fabric_consistency_diagnostics_after_repair": verify_gate_after_repair.get(
                        "diagnostics", {}
                    ),
                }

                if bool(verify_gate_after_repair.get("ok", False)):
                    host_evidence["chaincode_gateway_verify_probe"] = (
                        verify_probe_after_repair
                    )
                    host_evidence["chaincode_commit_matrix"] = verify_gate_after_repair.get(
                        "commit_matrix", {}
                    )
                    host_evidence["fabric_consistency_diagnostics"] = verify_gate_after_repair.get(
                        "diagnostics", {}
                    )
                    if isinstance(run_state, dict):
                        run_state.setdefault("chaincode_commit_matrix", {})[
                            str(node_id or host_ref or "gateway")
                        ] = verify_gate_after_repair.get("commit_matrix", {})
                        run_state.setdefault("fabric_consistency_diagnostics", {})[
                            str(node_id or host_ref or "gateway")
                        ] = verify_gate_after_repair.get("diagnostics", {})
                else:
                    verify_failure_code = str(
                        verify_gate_after_repair.get(
                            "failure_code",
                            "runbook_apigateway_functional_unavailable",
                        )
                    ).strip() or "runbook_apigateway_functional_unavailable"
                    verify_failure_message = str(
                        verify_gate_after_repair.get(
                            "failure_message",
                            "Gateway acessivel, mas endpoint funcional de channel/chaincode permanece indisponivel apos auto-reparo.",
                        )
                    ).strip() or (
                        "Gateway acessivel, mas endpoint funcional de channel/chaincode permanece indisponivel apos auto-reparo."
                    )
                    verify_failure_details = {
                        "stage": stage_key,
                        "checkpoint": checkpoint_key,
                        "host_ref": host_ref,
                        "host_address": host_address,
                        "org_id": org_id,
                        "node_id": node_id,
                        "node_type": node_type,
                        "probe_spec": verify_gate_after_repair.get("probe_spec", {}),
                        "probe_before_repair": verify_probe,
                        "probe_after_repair": verify_probe_after_repair,
                        "repair_returncode": verify_repair_returncode,
                        "repair_stderr": verify_repair_stderr[:512],
                        "repair_stdout": verify_repair_stdout[:512],
                    }
                    verify_failure_details.update(
                        verify_gate_after_repair.get("failure_details", {})
                    )

                    repair_fabric_runtime_failure = _classify_fabric_runtime_failure(
                        verify_repair_stderr
                    )
                    if repair_fabric_runtime_failure:
                        (
                            verify_failure_code,
                            verify_failure_message,
                            verify_fabric_failure_details,
                        ) = repair_fabric_runtime_failure
                        verify_failure_details.update(verify_fabric_failure_details)

                    failed_check = (
                        verify_probe_after_repair.get("details", {})
                        if isinstance(verify_probe_after_repair.get("details", {}), dict)
                        else {}
                    )
                    first_failed_body = str(
                        failed_check.get("first_failed_body", "") or ""
                    )
                    if "chaincode" in first_failed_body.lower() and "not found" in first_failed_body.lower():
                        verify_failure_code = "runbook_fabric_chaincode_not_committed"
                        verify_failure_message = (
                            "Gateway reporta chaincode nao encontrado no canal apos tentativa de auto-reparo."
                        )

                    return host_evidences, _build_runbook_error_payload(
                        verify_failure_code,
                        verify_failure_message,
                        verify_failure_details,
                    )

        if execution_returncode != 0:
            if stage_key == "provision" and checkpoint_key == "provision.runtime":
                runtime_container_name = _resolve_runbook_container_name(
                    run_state, normalized_host_row
                )
                runtime_container_probe = _run_remote_command(
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    remote_command=(
                        "COGNUS_RUNTIME_CONTAINER={container}; "
                        "if [ -z \"$COGNUS_RUNTIME_CONTAINER\" ] || ! docker inspect \"$COGNUS_RUNTIME_CONTAINER\" >/dev/null 2>&1; then "
                        "COGNUS_RUNTIME_CONTAINER=$(docker ps -a --filter label=cognus.run_id={run_id} --filter label=cognus.node_type={node_type} --filter label=cognus.node_id={node_id} --format '{{{{.Names}}}}' | head -n 1); "
                        "fi; "
                        "if [ -z \"$COGNUS_RUNTIME_CONTAINER\" ]; then "
                        "COGNUS_RUNTIME_CONTAINER=$(docker ps -a --filter label=cognus.run_id={run_id} --filter label=cognus.node_type={node_type} --format '{{{{.Names}}}}' | head -n 1); "
                        "fi; "
                        "if [ -n \"$COGNUS_RUNTIME_CONTAINER\" ]; then docker inspect -f '{{{{.State.Status}}}}' \"$COGNUS_RUNTIME_CONTAINER\" 2>/dev/null || true; fi"
                    ).format(
                        container=shlex.quote(runtime_container_name),
                        run_id=shlex.quote(str(run_state.get("run_id", "") or "")),
                        node_type=shlex.quote(node_type),
                        node_id=shlex.quote(str(node_id or "")),
                    ),
                    identity_file=credential_binding.get("identity_file", ""),
                    timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
                )
                runtime_status = str(runtime_container_probe.get("stdout", "") or "").strip()
                runtime_status_after_start = runtime_status

                if not runtime_status:
                    runtime_container_running_fallback_probe = _run_remote_command(
                        host_address=host_address,
                        ssh_user=ssh_user,
                        ssh_port=ssh_port,
                        remote_command=(
                            "COGNUS_RUNTIME_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type={node_type} --filter label=cognus.node_id={node_id} --filter status=running --format '{{{{.Names}}}}' | head -n 1); "
                            "if [ -z \"$COGNUS_RUNTIME_CONTAINER\" ]; then "
                            "COGNUS_RUNTIME_CONTAINER=$(docker ps --filter label=cognus.run_id={run_id} --filter label=cognus.node_type={node_type} --filter status=running --format '{{{{.Names}}}}' | head -n 1); "
                            "fi; "
                            "if [ -n \"$COGNUS_RUNTIME_CONTAINER\" ]; then echo running; fi"
                        ).format(
                            run_id=shlex.quote(str(run_state.get("run_id", "") or "")),
                            node_type=shlex.quote(node_type),
                            node_id=shlex.quote(str(node_id or "")),
                        ),
                        identity_file=credential_binding.get("identity_file", ""),
                        timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
                    )
                    runtime_status = str(
                        runtime_container_running_fallback_probe.get("stdout", "") or ""
                    ).strip()
                    host_evidence["runtime_container_fallback_probe"] = {
                        "returncode": runtime_container_running_fallback_probe.get(
                            "returncode", 0
                        ),
                        "stdout": str(
                            runtime_container_running_fallback_probe.get("stdout", "")
                        )[:256],
                        "stderr": str(
                            runtime_container_running_fallback_probe.get("stderr", "")
                        )[:256],
                    }
                    runtime_status_after_start = runtime_status_after_start or runtime_status

                if runtime_status and runtime_status != "running":
                    runtime_container_start_attempt = _run_remote_command(
                        host_address=host_address,
                        ssh_user=ssh_user,
                        ssh_port=ssh_port,
                        remote_command=(
                            "COGNUS_RUNTIME_CONTAINER={container}; "
                            "if [ -z \"$COGNUS_RUNTIME_CONTAINER\" ] || ! docker inspect \"$COGNUS_RUNTIME_CONTAINER\" >/dev/null 2>&1; then "
                            "COGNUS_RUNTIME_CONTAINER=$(docker ps -a --filter label=cognus.run_id={run_id} --filter label=cognus.node_type={node_type} --filter label=cognus.node_id={node_id} --format '{{{{.Names}}}}' | head -n 1); "
                            "fi; "
                            "if [ -z \"$COGNUS_RUNTIME_CONTAINER\" ]; then "
                            "COGNUS_RUNTIME_CONTAINER=$(docker ps -a --filter label=cognus.run_id={run_id} --filter label=cognus.node_type={node_type} --format '{{{{.Names}}}}' | head -n 1); "
                            "fi; "
                            "if [ -n \"$COGNUS_RUNTIME_CONTAINER\" ]; then docker start \"$COGNUS_RUNTIME_CONTAINER\" >/dev/null 2>&1 || true; fi; "
                            "if [ -n \"$COGNUS_RUNTIME_CONTAINER\" ]; then docker inspect -f '{{{{.State.Status}}}}' \"$COGNUS_RUNTIME_CONTAINER\" 2>/dev/null || true; fi"
                        ).format(
                            container=shlex.quote(runtime_container_name),
                            run_id=shlex.quote(str(run_state.get("run_id", "") or "")),
                            node_type=shlex.quote(node_type),
                            node_id=shlex.quote(str(node_id or "")),
                        ),
                        identity_file=credential_binding.get("identity_file", ""),
                        timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
                    )
                    runtime_status_after_start = str(
                        runtime_container_start_attempt.get("stdout", "") or ""
                    ).strip()
                    host_evidence["runtime_container_start_attempt"] = {
                        "status_before": runtime_status,
                        "status_after": runtime_status_after_start,
                        "probe": {
                            "returncode": runtime_container_start_attempt.get(
                                "returncode", 0
                            ),
                            "stdout": str(
                                runtime_container_start_attempt.get("stdout", "")
                            )[:256],
                            "stderr": str(
                                runtime_container_start_attempt.get("stderr", "")
                            )[:256],
                        },
                    }

                runtime_status = runtime_status_after_start or runtime_status
                if runtime_status == "running":
                    host_evidence["runtime_container_recovered"] = {
                        "status": runtime_status,
                        "original_exit_code": execution_returncode,
                        "node_type": node_type,
                        "probe": {
                            "returncode": runtime_container_probe.get("returncode", 0),
                            "stdout": str(runtime_container_probe.get("stdout", ""))[:256],
                            "stderr": str(runtime_container_probe.get("stderr", ""))[:256],
                        },
                    }
                    if node_type == "chaincode":
                        host_evidence["runtime_chaincode_recovered"] = {
                            "status": runtime_status,
                            "original_exit_code": execution_returncode,
                            "probe": {
                                "returncode": runtime_container_probe.get("returncode", 0),
                                "stdout": str(runtime_container_probe.get("stdout", ""))[:256],
                                "stderr": str(runtime_container_probe.get("stderr", ""))[:256],
                            },
                        }
                    host_evidence["exit_code"] = 0
                    continue

                if node_type in ("apigateway", "netapi"):
                    gateway_probe = host_evidence.get("chaincode_gateway_probe", {})
                    gateway_available = bool(gateway_probe.get("available", False))
                    if gateway_available:
                        host_evidence["runtime_container_recovered"] = {
                            "status": runtime_status,
                            "original_exit_code": execution_returncode,
                            "node_type": node_type,
                            "recovery_mode": "http_probe",
                            "gateway_probe": gateway_probe,
                            "probe": {
                                "returncode": runtime_container_probe.get("returncode", 0),
                                "stdout": str(runtime_container_probe.get("stdout", ""))[:256],
                                "stderr": str(runtime_container_probe.get("stderr", ""))[:256],
                            },
                        }
                        host_evidence["exit_code"] = 0
                        continue

                if node_type == "chaincode":
                    chaincode_placeholder_probe = _run_remote_command(
                        host_address=host_address,
                        ssh_user=ssh_user,
                        ssh_port=ssh_port,
                        remote_command=(
                            "COGNUS_RUNTIME_CONTAINER={container}; "
                            "if [ -z \"$COGNUS_RUNTIME_CONTAINER\" ]; then COGNUS_RUNTIME_CONTAINER={container}; fi; "
                            "if ! docker inspect \"$COGNUS_RUNTIME_CONTAINER\" >/dev/null 2>&1; then "
                            "docker image inspect alpine:3.20 >/dev/null 2>&1 || docker pull alpine:3.20 >/dev/null 2>&1 || true; "
                            "docker run -d --name \"$COGNUS_RUNTIME_CONTAINER\" --network {network} --restart unless-stopped "
                            "--label cognus.run_id={run_id} --label cognus.change_id={change_id} --label cognus.node_id={node_id} --label cognus.node_type=chaincode "
                            "alpine:3.20 tail -f /dev/null >/dev/null 2>&1 || true; "
                            "fi; "
                            "docker inspect -f '{{{{.State.Status}}}}' \"$COGNUS_RUNTIME_CONTAINER\" 2>/dev/null || true"
                        ).format(
                            container=shlex.quote(runtime_container_name),
                            network=shlex.quote(RUNBOOK_BASE_NETWORK_NAME),
                            run_id=shlex.quote(str(run_state.get("run_id", "") or "")),
                            change_id=shlex.quote(str(run_state.get("change_id", "") or "")),
                            node_id=shlex.quote(str(node_id or "")),
                        ),
                        identity_file=credential_binding.get("identity_file", ""),
                        timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS + 20, 45),
                    )
                    chaincode_placeholder_status = str(
                        chaincode_placeholder_probe.get("stdout", "") or ""
                    ).strip()
                    host_evidence["runtime_chaincode_placeholder"] = {
                        "status": chaincode_placeholder_status,
                        "probe": {
                            "returncode": chaincode_placeholder_probe.get("returncode", 0),
                            "stdout": str(
                                chaincode_placeholder_probe.get("stdout", "")
                            )[:256],
                            "stderr": str(
                                chaincode_placeholder_probe.get("stderr", "")
                            )[:256],
                        },
                    }
                    if chaincode_placeholder_status == "running":
                        host_evidence["runtime_container_recovered"] = {
                            "status": chaincode_placeholder_status,
                            "original_exit_code": execution_returncode,
                            "node_type": node_type,
                            "probe": {
                                "returncode": chaincode_placeholder_probe.get(
                                    "returncode", 0
                                ),
                                "stdout": str(
                                    chaincode_placeholder_probe.get("stdout", "")
                                )[:256],
                                "stderr": str(
                                    chaincode_placeholder_probe.get("stderr", "")
                                )[:256],
                            },
                        }
                        host_evidence["exit_code"] = 0
                        continue

            failure_code = "runbook_ssh_execution_failed"
            failure_message = "Execucao SSH no host alvo falhou durante etapa operacional."
            failure_details = {
                "stage": stage_key,
                "checkpoint": checkpoint_key,
                "host_ref": host_ref,
                "host_address": host_address,
                "org_id": org_id,
                "node_id": node_id,
                "node_type": node_type,
                "exit_code": execution_returncode,
                "stderr": stderr_text[:512],
                "stdout": stdout_text[:512],
            }
            if (
                stage_key == "verify"
                and checkpoint_key == "verify.consistency"
                and node_type in ("apigateway", "netapi")
            ):
                verify_diagnostics = {}
                if isinstance(run_state, dict):
                    verify_cache_key = _gateway_verify_scope_cache_key(
                        run_state,
                        normalized_host_row,
                        host_address,
                    )
                    cached_scope = _gateway_verify_scope_cache_get(
                        run_state,
                        verify_cache_key,
                    )
                    verify_diagnostics = (
                        cached_scope.get("diagnostics", {})
                        if isinstance(cached_scope.get("diagnostics", {}), dict)
                        else {}
                    )
                if not verify_diagnostics:
                    verify_diagnostics = _collect_verify_consistency_diagnostics(
                        run_state,
                        normalized_host_row,
                        host_address,
                        ssh_user,
                        ssh_port,
                        identity_file=credential_binding.get("identity_file", ""),
                        timeout=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
                    )
                host_evidence["fabric_consistency_diagnostics"] = verify_diagnostics
                failure_details["fabric_consistency_diagnostics"] = verify_diagnostics
            if execution_timed_out:
                failure_code = "runbook_ssh_timeout"
                failure_message = (
                    "Execucao SSH no host alvo excedeu o timeout configurado durante etapa operacional."
                )
                failure_details["timed_out"] = True
            runtime_memory_low_match = re.search(
                r"COGNUS_RUNTIME_MEMORY_LOW:([^:]+):([0-9]+):([0-9]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if runtime_memory_low_match:
                failure_code = "runbook_runtime_memory_pressure"
                failure_message = (
                    "Host alvo com memoria disponivel insuficiente para provisionar o runtime com seguranca."
                )
                failure_details["node_type"] = str(
                    runtime_memory_low_match.group(1) or node_type
                ).strip()
                failure_details["required_memory_mb"] = int(
                    runtime_memory_low_match.group(2) or 0
                )
                failure_details["available_memory_kb"] = int(
                    runtime_memory_low_match.group(3) or 0
                )

            runtime_status_invalid_match = re.search(
                r"COGNUS_RUNTIME_STATUS_INVALID:([^\s]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if runtime_status_invalid_match:
                failure_code = "runbook_runtime_container_unhealthy"
                failure_message = (
                    "Container de runtime fora de estado running estavel no host alvo."
                )
                failure_details["runtime_status"] = str(
                    runtime_status_invalid_match.group(1) or ""
                ).strip()

            runtime_restart_loop_match = re.search(
                r"COGNUS_RUNTIME_RESTART_LOOP:([0-9]+):([0-9]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if runtime_restart_loop_match:
                failure_code = "runbook_runtime_container_unhealthy"
                failure_message = "Container de runtime em loop de reinicio no host alvo."
                failure_details["restart_count_before"] = int(
                    runtime_restart_loop_match.group(1) or 0
                )
                failure_details["restart_count_after"] = int(
                    runtime_restart_loop_match.group(2) or 0
                )

            runtime_topology_missing_match = re.search(
                r"COGNUS_RUNTIME_TOPOLOGY_MISSING:([^:\s]+):([^\s]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if runtime_topology_missing_match:
                failure_code = "runbook_runtime_topology_incomplete"
                failure_message = (
                    "Runtime incompleto no host alvo; container obrigatorio do provisionamento nao foi localizado."
                )
                failure_details["missing_node_type"] = str(
                    runtime_topology_missing_match.group(1) or ""
                ).strip()
                failure_details["missing_node_id"] = str(
                    runtime_topology_missing_match.group(2) or ""
                ).strip()

            runtime_topology_not_running_match = re.search(
                r"COGNUS_RUNTIME_TOPOLOGY_NOT_RUNNING:([^:\s]+):([^:\s]+):([^\s]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if runtime_topology_not_running_match:
                failure_code = "runbook_runtime_topology_incomplete"
                failure_message = (
                    "Runtime incompleto no host alvo; container obrigatorio nao permaneceu em estado running."
                )
                failure_details["missing_node_type"] = str(
                    runtime_topology_not_running_match.group(1) or ""
                ).strip()
                failure_details["missing_node_id"] = str(
                    runtime_topology_not_running_match.group(2) or ""
                ).strip()
                failure_details["runtime_status"] = str(
                    runtime_topology_not_running_match.group(3) or ""
                ).strip()

            fabric_runtime_failure = _classify_fabric_runtime_failure(stderr_text)
            if fabric_runtime_failure:
                (
                    failure_code,
                    failure_message,
                    fabric_failure_details,
                ) = fabric_runtime_failure
                failure_details.update(fabric_failure_details)

            apigw_identity_invalid_match = re.search(
                r"COGNUS_APIGW_IDENTITIES_INVALID:([^\s]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if apigw_identity_invalid_match:
                failure_code = "runbook_apigateway_identity_invalid"
                failure_message = (
                    "Gateway iniciado sem identities.json valido; provisionamento bloqueado para evitar orgs vazias."
                )
                failure_details["identity_failure_reason"] = str(
                    apigw_identity_invalid_match.group(1) or "unknown"
                ).strip()

            runtime_image_missing_match = re.search(
                r"COGNUS_RUNTIME_IMAGE_MISSING:([^\s]+)",
                stderr_text or "",
                flags=re.IGNORECASE,
            )
            if runtime_image_missing_match:
                failure_code = "runbook_runtime_image_missing"
                failure_message = (
                    "Imagem de runtime obrigatoria do COGNUS indisponivel no host alvo."
                )
                runtime_image = str(runtime_image_missing_match.group(1) or "").strip()
                failure_details["runtime_image"] = runtime_image

                if stage_key == "provision" and checkpoint_key == "provision.runtime":
                    remote_pull_result = _pull_runtime_image_via_ssh(
                        runtime_image=runtime_image,
                        node_type=node_type,
                        host_address=host_address,
                        ssh_user=ssh_user,
                        ssh_port=ssh_port,
                        identity_file=credential_binding.get("identity_file", ""),
                    )
                    remote_probe_after_pull = _probe_remote_runtime_image(
                        host_address=host_address,
                        ssh_user=ssh_user,
                        ssh_port=ssh_port,
                        runtime_image=runtime_image,
                        identity_file=credential_binding.get("identity_file", ""),
                    )
                    host_evidence["runtime_image_remote_pull"] = {
                        "pull_result": remote_pull_result,
                        "remote_probe_after_pull": remote_probe_after_pull,
                    }
                    failure_details["runtime_image_remote_pull"] = {
                        "pull_result": remote_pull_result,
                        "remote_probe_after_pull": remote_probe_after_pull,
                    }

                    if remote_probe_after_pull.get("available", False):
                        retry_result = subprocess.run(
                            ssh_command,
                            capture_output=True,
                            text=True,
                            timeout=ssh_timeout,
                        )
                        retry_stdout_text = str(retry_result.stdout or "").strip()
                        retry_stderr_text = str(retry_result.stderr or "").strip()

                        retry_host_evidence = {
                            "run_id": run_state.get("run_id", ""),
                            "change_id": run_state.get("change_id", ""),
                            "stage": stage_key,
                            "checkpoint": checkpoint_key,
                            "host_ref": host_ref,
                            "host_address": host_address,
                            "org_id": org_id,
                            "node_id": node_id,
                            "executor_mode": "ssh-orchestrator",
                            "command_hash": _sha256_payload({"command": remote_command}),
                            "fingerprint_sha256": _sha256_payload(
                                {
                                    "run_id": run_state.get("run_id", ""),
                                    "change_id": run_state.get("change_id", ""),
                                    "stage": stage_key,
                                    "checkpoint": checkpoint_key,
                                    "host_ref": host_ref,
                                    "org_id": org_id,
                                    "host_address": host_address,
                                    "node_id": node_id,
                                    "command": remote_command,
                                    "retry_reason": "runtime_image_pulled_remotely_after_missing",
                                }
                            ),
                            "stdout": retry_stdout_text[:512],
                            "stderr": retry_stderr_text[:512],
                            "exit_code": retry_result.returncode,
                            "timestamp_utc": _utc_now(),
                            "host_mapping": safe_host_row,
                            "credential_binding": credential_binding,
                            "retry_of_command_hash": host_evidence.get("command_hash", ""),
                            "retry_reason": "runtime_image_pulled_remotely_after_missing",
                            "runtime_image_remote_pull": {
                                "pull_result": remote_pull_result,
                                "remote_probe_after_pull": remote_probe_after_pull,
                            },
                        }
                        host_evidences.append(retry_host_evidence)
                        if retry_result.returncode == 0:
                            continue

                    if _runtime_image_seed_enabled_for_node_type(node_type):
                        local_image_ready = _ensure_local_runtime_image_available(
                            node_type=node_type,
                            runtime_image=runtime_image,
                        )
                        host_evidence["runtime_image_local_ready"] = local_image_ready
                        failure_details["runtime_image_local_ready"] = local_image_ready

                        if str(local_image_ready.get("status", "")).strip().lower() != "ready":
                            seed_result = {
                                "attempted": False,
                                "status": "skipped",
                                "reason": "runtime_image_not_available_locally",
                                "local_image_ready": local_image_ready,
                            }
                        else:
                            seed_result = _seed_runtime_image_via_ssh(
                                runtime_image=runtime_image,
                                host_address=host_address,
                                ssh_user=ssh_user,
                                ssh_port=ssh_port,
                                identity_file=credential_binding.get("identity_file", ""),
                            )
                    else:
                        seed_result = {
                            "attempted": False,
                            "status": "skipped",
                            "reason": "runtime_image_seed_disabled",
                            "node_type": node_type,
                        }
                    host_evidence["runtime_image_seed"] = seed_result
                    failure_details["runtime_image_seed"] = seed_result

                    if seed_result.get("status") == "seeded":
                        retry_result = subprocess.run(
                            ssh_command,
                            capture_output=True,
                            text=True,
                            timeout=ssh_timeout,
                        )
                        retry_stdout_text = str(retry_result.stdout or "").strip()
                        retry_stderr_text = str(retry_result.stderr or "").strip()

                        retry_host_evidence = {
                            "run_id": run_state.get("run_id", ""),
                            "change_id": run_state.get("change_id", ""),
                            "stage": stage_key,
                            "checkpoint": checkpoint_key,
                            "host_ref": host_ref,
                            "host_address": host_address,
                            "org_id": org_id,
                            "node_id": node_id,
                            "executor_mode": "ssh-orchestrator",
                            "command_hash": _sha256_payload({"command": remote_command}),
                            "fingerprint_sha256": _sha256_payload(
                                {
                                    "run_id": run_state.get("run_id", ""),
                                    "change_id": run_state.get("change_id", ""),
                                    "stage": stage_key,
                                    "checkpoint": checkpoint_key,
                                    "host_ref": host_ref,
                                    "org_id": org_id,
                                    "host_address": host_address,
                                    "node_id": node_id,
                                    "command": remote_command,
                                    "retry_reason": "runtime_image_seeded_from_orchestrator",
                                }
                            ),
                            "stdout": retry_stdout_text[:512],
                            "stderr": retry_stderr_text[:512],
                            "exit_code": retry_result.returncode,
                            "timestamp_utc": _utc_now(),
                            "host_mapping": safe_host_row,
                            "credential_binding": credential_binding,
                            "retry_of_command_hash": host_evidence.get("command_hash", ""),
                            "retry_reason": "runtime_image_seeded_from_orchestrator",
                            "runtime_image_seed": seed_result,
                        }
                        host_evidences.append(retry_host_evidence)

                        if retry_result.returncode == 0:
                            continue

                        retry_missing_match = re.search(
                            r"COGNUS_RUNTIME_IMAGE_MISSING:([^\s]+)",
                            retry_stderr_text or "",
                            flags=re.IGNORECASE,
                        )
                        if retry_missing_match:
                            failure_code = "runbook_runtime_image_missing"
                            failure_message = (
                                "Imagem de runtime obrigatoria do COGNUS indisponivel no host alvo."
                            )
                            failure_details["runtime_image"] = (
                                str(retry_missing_match.group(1) or "").strip()
                            )
                        else:
                            retry_status_invalid_match = re.search(
                                r"COGNUS_RUNTIME_STATUS_INVALID:([^\s]+)",
                                retry_stderr_text or "",
                                flags=re.IGNORECASE,
                            )
                            retry_restart_loop_match = re.search(
                                r"COGNUS_RUNTIME_RESTART_LOOP:([0-9]+):([0-9]+)",
                                retry_stderr_text or "",
                                flags=re.IGNORECASE,
                            )
                            if retry_status_invalid_match:
                                failure_code = "runbook_runtime_container_unhealthy"
                                failure_message = (
                                    "Container de runtime fora de estado running estavel no host alvo."
                                )
                                failure_details["runtime_status"] = str(
                                    retry_status_invalid_match.group(1) or ""
                                ).strip()
                            elif retry_restart_loop_match:
                                failure_code = "runbook_runtime_container_unhealthy"
                                failure_message = (
                                    "Container de runtime em loop de reinicio no host alvo."
                                )
                                failure_details["restart_count_before"] = int(
                                    retry_restart_loop_match.group(1) or 0
                                )
                                failure_details["restart_count_after"] = int(
                                    retry_restart_loop_match.group(2) or 0
                                )
                            else:
                                failure_code = "runbook_ssh_execution_failed"
                                failure_message = (
                                    "Execucao SSH no host alvo falhou durante etapa operacional."
                                )

                        retry_fabric_runtime_failure = _classify_fabric_runtime_failure(
                            retry_stderr_text
                        )
                        if retry_fabric_runtime_failure:
                            (
                                failure_code,
                                failure_message,
                                retry_fabric_failure_details,
                            ) = retry_fabric_runtime_failure
                            failure_details.update(retry_fabric_failure_details)

                        failure_details["exit_code"] = retry_result.returncode
                        failure_details["stderr"] = retry_stderr_text[:512]
                        failure_details["stdout"] = retry_stdout_text[:512]

            if (
                stage_key == "verify"
                and checkpoint_key == "verify.consistency"
                and node_type in ("apigateway", "netapi")
                and failure_code
                in {
                    "runbook_fabric_chaincode_not_committed",
                    "runbook_fabric_orderer_endpoint_unreachable",
                    "runbook_fabric_channel_not_joined",
                    "runbook_fabric_orderer_channel_state_invalid",
                }
            ):
                remediation_restart_command = (
                    "COGNUS_RUN_ID={run_id}; "
                    "COGNUS_PEER_CONTAINER=$(docker ps --filter label=cognus.run_id=$COGNUS_RUN_ID --filter label=cognus.node_type=peer --format '{{{{.Names}}}}' | head -n 1); "
                    "COGNUS_ORDERER_CONTAINER=$(docker ps --filter label=cognus.run_id=$COGNUS_RUN_ID --filter label=cognus.node_type=orderer --format '{{{{.Names}}}}' | head -n 1); "
                    "if [ -n \"$COGNUS_ORDERER_CONTAINER\" ]; then docker restart \"$COGNUS_ORDERER_CONTAINER\" >/dev/null 2>&1 || true; fi; "
                    "if [ -n \"$COGNUS_PEER_CONTAINER\" ]; then docker restart \"$COGNUS_PEER_CONTAINER\" >/dev/null 2>&1 || true; fi; "
                    "sleep 6; "
                    "echo COGNUS_FABRIC_RESTART_REMEDIATION_DONE"
                ).format(run_id=shlex.quote(str(run_state.get("run_id", "") or "")))
                remediation_restart_result = _run_remote_command(
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    remote_command=remediation_restart_command,
                    identity_file=credential_binding.get("identity_file", ""),
                    timeout=max(ssh_timeout, RUNBOOK_SSH_TIMEOUT_SECONDS + 30),
                )

                remediation_retry_result = _run_remote_command(
                    host_address=host_address,
                    ssh_user=ssh_user,
                    ssh_port=ssh_port,
                    remote_command=remote_command,
                    identity_file=credential_binding.get("identity_file", ""),
                    timeout=ssh_timeout,
                )
                remediation_retry_stdout = str(
                    remediation_retry_result.get("stdout", "") or ""
                ).strip()
                remediation_retry_stderr = str(
                    remediation_retry_result.get("stderr", "") or ""
                ).strip()
                remediation_retry_returncode = _coerce_exit_code(
                    remediation_retry_result.get("returncode", 1),
                    default=1,
                )
                host_evidence["verify_fabric_remediation_retry"] = {
                    "restart": {
                        "returncode": remediation_restart_result.get("returncode", 1),
                        "stdout": str(remediation_restart_result.get("stdout", ""))[:256],
                        "stderr": str(remediation_restart_result.get("stderr", ""))[:256],
                    },
                    "retry": {
                        "returncode": remediation_retry_returncode,
                        "stdout": remediation_retry_stdout[:512],
                        "stderr": remediation_retry_stderr[:512],
                    },
                }

                if remediation_retry_returncode == 0:
                    remediation_host_port = _resolve_runtime_exposed_host_port(
                        run_state,
                        normalized_host_row,
                    )
                    remediation_gate = _assess_gateway_final_gate(
                        run_state,
                        normalized_host_row,
                        host_address,
                        remediation_host_port,
                        ssh_user,
                        ssh_port,
                        identity_file=credential_binding.get("identity_file", ""),
                        timeout_seconds=max(RUNBOOK_SSH_TIMEOUT_SECONDS, 20),
                        force_refresh=True,
                    )
                    remediation_probe = (
                        remediation_gate.get("probe", {})
                        if isinstance(remediation_gate.get("probe"), dict)
                        else {}
                    )
                    host_evidence["chaincode_commit_matrix"] = remediation_gate.get(
                        "commit_matrix", {}
                    )
                    host_evidence["fabric_consistency_diagnostics"] = remediation_gate.get(
                        "diagnostics", {}
                    )
                    host_evidence["chaincode_gateway_verify_probe_spec"] = remediation_gate.get(
                        "probe_spec", {}
                    )
                    host_evidence["chaincode_gateway_verify_probe"] = remediation_probe

                    if bool(remediation_gate.get("ok", False)):
                        host_evidence["exit_code"] = 0
                        continue

                    failure_details["gateway_probe_after_remediation"] = remediation_probe
                    failure_details["chaincode_commit_matrix_after_remediation"] = remediation_gate.get(
                        "commit_matrix", {}
                    )
                    failure_details["fabric_consistency_diagnostics_after_remediation"] = remediation_gate.get(
                        "diagnostics", {}
                    )
                    failure_details["gateway_probe_spec_after_remediation"] = remediation_gate.get(
                        "probe_spec", {}
                    )
                    if str(remediation_gate.get("failure_code", "")).strip():
                        failure_code = str(remediation_gate.get("failure_code", "")).strip()
                    if str(remediation_gate.get("failure_message", "")).strip():
                        failure_message = str(remediation_gate.get("failure_message", "")).strip()
                    failure_details.update(remediation_gate.get("failure_details", {}))

                retry_fabric_failure = _classify_fabric_runtime_failure(
                    remediation_retry_stderr
                )
                if retry_fabric_failure:
                    (
                        failure_code,
                        failure_message,
                        retry_fabric_failure_details,
                    ) = retry_fabric_failure
                    failure_details.update(retry_fabric_failure_details)
                failure_details["verify_fabric_remediation_retry"] = host_evidence.get(
                    "verify_fabric_remediation_retry", {}
                )

            return host_evidences, _build_runbook_error_payload(
                failure_code,
                failure_message,
                failure_details,
            )

    return host_evidences, None


def _append_stage_host_evidence(run_state, stage_key, checkpoint_key, host_evidences):
    if not isinstance(host_evidences, list) or len(host_evidences) == 0:
        return

    evidence_store = run_state.setdefault("host_stage_evidences", {})
    stage_rows = evidence_store.setdefault(stage_key, [])
    stage_rows.extend(host_evidences)

    host_inventory = run_state.setdefault("host_inventory", [])
    for evidence in host_evidences:
        host_inventory.append(
            {
                "run_id": evidence.get("run_id", ""),
                "change_id": evidence.get("change_id", ""),
                "stage": stage_key,
                "checkpoint": checkpoint_key,
                "host_ref": evidence.get("host_ref", ""),
                "host_address": evidence.get("host_address", ""),
                "org_id": evidence.get("org_id", ""),
                "node_id": evidence.get("node_id", ""),
                "exit_code": evidence.get("exit_code", 0),
                "timestamp_utc": evidence.get("timestamp_utc", ""),
                "fingerprint_sha256": evidence.get("fingerprint_sha256", ""),
            }
        )
        _append_event(
            run_state,
            "info",
            "runbook_stage_host_telemetry",
            "Telemetria oficial de host persistida para etapa/checkpoint.",
            host_ref=evidence.get("host_ref", ""),
            org_id=evidence.get("org_id", ""),
            fingerprint_sha256=evidence.get("fingerprint_sha256", ""),
            component="host:{}".format(evidence.get("host_ref", ""))
            if evidence.get("host_ref", "")
            else "",
            cause="Evidencia oficial de telemetria de host persistida no backend.",
            impact="Inventario tecnico da etapa atualizado para auditoria e troubleshooting.",
            recommended_action="Sem acao imediata.",
            classification="informational",
        )


def _upsert_run_checkpoint(
    store_payload,
    run_state,
    stage_key,
    checkpoint_key,
    checkpoint_status,
    executor,
    input_payload,
    output_payload,
):
    checkpoint_store = store_payload.setdefault("checkpoints", {})
    run_id = run_state.get("run_id", "")
    run_checkpoint_rows = checkpoint_store.setdefault(run_id, [])

    input_hash = _sha256_payload(input_payload)
    output_hash = _sha256_payload(output_payload)

    previous_attempts = [
        row
        for row in run_checkpoint_rows
        if row.get("stage", "") == stage_key
        and row.get("checkpoint", "") == checkpoint_key
    ]
    attempt = len(previous_attempts) + 1
    idempotency_key = "{}|{}|{}|{}|{}".format(
        run_state.get("idempotency_key", ""),
        stage_key,
        checkpoint_key,
        checkpoint_status,
        input_hash,
    )

    for existing_row in previous_attempts:
        if existing_row.get("idempotency_key", "") == idempotency_key:
            return existing_row

    checkpoint_row = {
        "run_id": run_id,
        "change_id": run_state.get("change_id", ""),
        "stage": stage_key,
        "checkpoint": checkpoint_key,
        "status": checkpoint_status,
        "idempotency_key": idempotency_key,
        "attempt": attempt,
        "input_hash": input_hash,
        "output_hash": output_hash,
        "executor": executor,
        "timestamp_utc": _utc_now(),
    }
    run_checkpoint_rows.append(checkpoint_row)

    return checkpoint_row


def _version_artifact(
    store_payload,
    run_state,
    artifact_group,
    artifact_name,
    payload,
    stage_key,
    executor,
):
    artifact_store = store_payload.setdefault("artifacts", {})
    run_id = run_state.get("run_id", "")
    run_artifact_rows = artifact_store.setdefault(run_id, [])

    payload_hash = _sha256_payload(payload)
    artifact_key = "{}:{}".format(artifact_group, artifact_name)
    group_versions = [
        row for row in run_artifact_rows if row.get("artifact_key", "") == artifact_key
    ]
    if group_versions and group_versions[-1].get("payload_hash", "") == payload_hash:
        return group_versions[-1]

    version = len(group_versions) + 1
    artifact_row = {
        "run_id": run_id,
        "change_id": run_state.get("change_id", ""),
        "artifact_group": artifact_group,
        "artifact_name": artifact_name,
        "artifact_key": artifact_key,
        "version": version,
        "payload_hash": payload_hash,
        "payload": payload,
        "stage": stage_key,
        "executor": executor,
        "timestamp_utc": _utc_now(),
    }
    run_artifact_rows.append(artifact_row)

    return artifact_row


def _version_stage_reports(store_payload, run_state, executor):
    for stage in run_state.get("stages", []):
        stage_key = stage.get("key", "")
        stage_payload = {
            "run_id": run_state.get("run_id", ""),
            "change_id": run_state.get("change_id", ""),
            "stage": stage_key,
            "status": stage.get("status", "pending"),
            "checkpoints": stage.get("checkpoints", []),
            "host_evidences": run_state.get("host_stage_evidences", {}).get(stage_key, []),
            "updated_at_utc": run_state.get("updated_at_utc", ""),
        }
        _version_artifact(
            store_payload,
            run_state,
            "stage-reports",
            "{}-report.json".format(stage_key),
            stage_payload,
            stage_key,
            executor,
        )


def _latest_artifact_payloads(store_payload, run_id):
    artifacts_by_key = {}
    run_artifact_rows = store_payload.get("artifacts", {}).get(run_id, [])
    for artifact in run_artifact_rows:
        if not isinstance(artifact, dict):
            continue
        artifact_key = artifact.get("artifact_key", "")
        if not artifact_key:
            continue
        current = artifacts_by_key.get(artifact_key)
        if current is None or artifact.get("version", 0) >= current.get("version", 0):
            artifacts_by_key[artifact_key] = artifact
    return artifacts_by_key


def _validate_verify_artifact_consistency(store_payload, run_state):
    run_id = run_state.get("run_id", "")
    artifacts_by_key = _latest_artifact_payloads(store_payload, run_id)
    persisted_checkpoints = store_payload.get("checkpoints", {}).get(run_id, [])

    required_artifacts = [
        "inventory-final:inventory-final.json",
        "pipeline-report:pipeline-report.json",
        "history:history.jsonl",
        "decision-trace:decision-trace.jsonl",
    ]
    for stage in run_state.get("stages", []):
        required_artifacts.append(
            "stage-reports:{}-report.json".format(stage.get("key", ""))
        )

    missing_artifacts = [
        artifact_key
        for artifact_key in required_artifacts
        if artifact_key not in artifacts_by_key
    ]
    if missing_artifacts:
        return _build_runbook_error_payload(
            "runbook_verify_artifact_inconsistent",
            "Verify reprovado: artefatos obrigatorios ausentes para conclusao oficial.",
            {
                "missing_artifacts": missing_artifacts,
            },
        )

    for stage in run_state.get("stages", []):
        artifact_key = "stage-reports:{}-report.json".format(stage.get("key", ""))
        stage_artifact = artifacts_by_key[artifact_key]
        stage_payload = stage_artifact.get("payload", {})
        if stage_payload.get("status", "") != stage.get("status", ""):
            return _build_runbook_error_payload(
                "runbook_verify_artifact_inconsistent",
                "Verify reprovado: status da etapa diverge do artefato versionado.",
                {
                    "stage": stage.get("key", ""),
                    "expected_status": stage.get("status", ""),
                    "artifact_status": stage_payload.get("status", ""),
                },
            )

        for checkpoint in stage.get("checkpoints", []):
            if checkpoint.get("status", "") != "completed":
                continue

            has_persisted_completed = any(
                persisted.get("stage", "") == stage.get("key", "")
                and persisted.get("checkpoint", "") == checkpoint.get("key", "")
                and persisted.get("status", "") == "completed"
                for persisted in persisted_checkpoints
                if isinstance(persisted, dict)
            )
            if not has_persisted_completed:
                return _build_runbook_error_payload(
                    "runbook_verify_artifact_inconsistent",
                    "Verify reprovado: checkpoint concluido sem evidencia persistida.",
                    {
                        "stage": stage.get("key", ""),
                        "checkpoint": checkpoint.get("key", ""),
                    },
                )

    return None


def _minimum_required_artifact_keys(run_state):
    required_keys = [*MINIMUM_RUN_ARTIFACT_KEYS]
    for stage in run_state.get("stages", []):
        required_keys.append("stage-reports:{}-report.json".format(stage.get("key", "")))
    return sorted(set([artifact_key for artifact_key in required_keys if artifact_key]))


def _resolve_missing_minimum_evidence(store_payload, run_state):
    run_id = run_state.get("run_id", "")
    artifacts_by_key = _latest_artifact_payloads(store_payload, run_id)
    required_keys = _minimum_required_artifact_keys(run_state)
    missing_keys = [artifact_key for artifact_key in required_keys if artifact_key not in artifacts_by_key]
    return {
        "required_keys": required_keys,
        "missing_keys": missing_keys,
        "evidence_minimum_valid": len(missing_keys) == 0,
    }


def _normalize_a2a_artifact_token(value):
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    normalized = normalized.replace("\\", "/").replace(":", "/")
    normalized = re.sub(r"/+", "/", normalized)
    normalized = re.sub(r"\.json$", "", normalized)
    return normalized.strip("/")


def _collect_a2a_available_artifact_tokens(store_payload, run_state):
    run_id = str(run_state.get("run_id", "") or "").strip()
    artifacts_by_key = _latest_artifact_payloads(store_payload, run_id)
    tokens = set()

    for artifact_key in artifacts_by_key.keys():
        normalized = _normalize_a2a_artifact_token(artifact_key)
        if normalized:
            tokens.add(normalized)

    for artifact_row in run_state.get("artifact_rows", []):
        if not isinstance(artifact_row, dict):
            continue
        for candidate in (
            artifact_row.get("key", ""),
            artifact_row.get("artifact_key", ""),
        ):
            normalized = _normalize_a2a_artifact_token(candidate)
            if normalized:
                tokens.add(normalized)

    for artifact_key in run_state.get("a2_2_available_artifacts", []):
        normalized = _normalize_a2a_artifact_token(artifact_key)
        if normalized:
            tokens.add(normalized)

    return sorted(tokens)


def _artifact_token_present(available_tokens, required_key):
    safe_tokens = set(available_tokens if isinstance(available_tokens, list) else [])
    normalized_required_key = _normalize_a2a_artifact_token(required_key)
    if not normalized_required_key:
        return False

    if normalized_required_key == "inventory-final":
        return any(
            token in {"inventory-final", "inventory-final/inventory-final"}
            for token in safe_tokens
        )
    if normalized_required_key == "verify-report":
        return any(
            token in {"verify-report", "stage-reports/verify-report"}
            for token in safe_tokens
        )
    if normalized_required_key == "stage-reports":
        return any(token == "stage-reports" or token.startswith("stage-reports/") for token in safe_tokens)
    if normalized_required_key == "ssh-execution-log":
        return any(
            token == "ssh-execution-log" or token.startswith("ssh-execution-log/")
            for token in safe_tokens
        )

    return normalized_required_key in safe_tokens


def _build_a2a_entry_issue(
    code,
    message,
    severity="error",
    cause="",
    impact="",
    recommended_action="",
    details=None,
):
    issue = {
        "code": str(code or "").strip(),
        "message": str(message or "").strip(),
        "severity": str(severity or "error").strip().lower() or "error",
        "cause": str(cause or "").strip() or str(message or "").strip(),
        "impact": str(impact or "").strip()
        or "Bloqueia habilitacao das acoes operacionais do dashboard A2A.",
        "recommended_action": str(recommended_action or "").strip()
        or "Corrigir o contrato oficial do runbook e consultar novamente o status.",
    }
    if isinstance(details, dict) and details:
        issue["details"] = details
    return issue


def _resolve_a2a_dependency_payload_issue_prefix(dependency_key):
    normalized_key = str(dependency_key or "").strip().lower()
    if normalized_key in {"a2_3", "a2_4"}:
        return normalized_key
    return "a2_dependency"


def _evaluate_a2a_dependency_payload(
    dependency_key,
    handoff_payload,
    readiness_payload,
    run_id,
    change_id,
    manifest_fingerprint,
    source_blueprint_fingerprint,
):
    issue_prefix = _resolve_a2a_dependency_payload_issue_prefix(dependency_key)
    handoff_valid = True
    readiness_valid = True
    handoff_applicable = False
    readiness_applicable = False
    issues = []

    if handoff_payload not in (None, {}):
        handoff_applicable = True
    if handoff_applicable and not isinstance(handoff_payload, dict):
        handoff_valid = False
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_{}_handoff_invalid_type".format(issue_prefix),
                "Handoff {} fornecido em formato inválido para o gate A2A.".format(
                    dependency_key.upper().replace("_", ".")
                ),
                cause="O payload oficial do handoff não é um objeto JSON válido.",
                recommended_action="Republicar o handoff oficial antes de abrir o dashboard A2A.",
            )
        )
    elif handoff_applicable:
        handoff_decision = str(
            handoff_payload.get("handoff_decision", "") or handoff_payload.get("decision", "")
        ).strip().lower()
        if handoff_decision and handoff_decision != "allow":
            handoff_valid = False
            issues.append(
                _build_a2a_entry_issue(
                    "runbook_a2a_{}_handoff_blocked".format(issue_prefix),
                    "Handoff {} não liberou continuidade para o A2A.".format(
                        dependency_key.upper().replace("_", ".")
                    ),
                    cause="O handoff oficial foi publicado com decisão diferente de allow.",
                    recommended_action="Concluir o WP de origem e republicar o handoff com decisão allow.",
                    details={"handoff_decision": handoff_decision},
                )
            )

        dependency_correlation = {
            "run_id": str(handoff_payload.get("run_id", "") or "").strip(),
            "change_id": str(handoff_payload.get("change_id", "") or "").strip(),
            "manifest_fingerprint": str(
                handoff_payload.get("manifest_fingerprint", "") or ""
            ).strip().lower(),
            "source_blueprint_fingerprint": str(
                handoff_payload.get("source_blueprint_fingerprint", "") or ""
            ).strip().lower(),
        }
        mismatched_fields = []
        if (
            dependency_correlation["run_id"]
            and run_id
            and dependency_correlation["run_id"] != run_id
        ):
            mismatched_fields.append("run_id")
        if (
            dependency_correlation["change_id"]
            and change_id
            and dependency_correlation["change_id"] != change_id
        ):
            mismatched_fields.append("change_id")
        if (
            dependency_correlation["manifest_fingerprint"]
            and manifest_fingerprint
            and dependency_correlation["manifest_fingerprint"] != manifest_fingerprint
        ):
            mismatched_fields.append("manifest_fingerprint")
        if (
            dependency_correlation["source_blueprint_fingerprint"]
            and source_blueprint_fingerprint
            and dependency_correlation["source_blueprint_fingerprint"]
            != source_blueprint_fingerprint
        ):
            mismatched_fields.append("source_blueprint_fingerprint")
        if mismatched_fields:
            handoff_valid = False
            issues.append(
                _build_a2a_entry_issue(
                    "runbook_a2a_{}_handoff_correlation_mismatch".format(issue_prefix),
                    "Handoff {} diverge da correlação oficial do runbook.".format(
                        dependency_key.upper().replace("_", ".")
                    ),
                    cause="O handoff do WP anterior não aponta para o mesmo contexto técnico do A2A.",
                    recommended_action="Regerar o handoff com os mesmos run_id, change_id e fingerprints oficiais.",
                    details={"mismatched_fields": sorted(mismatched_fields)},
                )
            )

    if readiness_payload not in (None, {}):
        readiness_applicable = True
    if readiness_applicable and not isinstance(readiness_payload, dict):
        readiness_valid = False
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_{}_readiness_invalid_type".format(issue_prefix),
                "Checklist de prontidão {} fornecido em formato inválido para o gate A2A.".format(
                    dependency_key.upper().replace("_", ".")
                ),
                cause="O checklist oficial não é um objeto JSON válido.",
                recommended_action="Republicar o checklist oficial de prontidão antes de abrir o dashboard A2A.",
            )
        )
    elif readiness_applicable:
        dependency_correlation = {
            "run_id": str(readiness_payload.get("run_id", "") or "").strip(),
            "change_id": str(readiness_payload.get("change_id", "") or "").strip(),
            "manifest_fingerprint": str(
                readiness_payload.get("manifest_fingerprint", "") or ""
            ).strip().lower(),
            "source_blueprint_fingerprint": str(
                readiness_payload.get("source_blueprint_fingerprint", "") or ""
            ).strip().lower(),
        }
        mismatched_fields = []
        if (
            dependency_correlation["run_id"]
            and run_id
            and dependency_correlation["run_id"] != run_id
        ):
            mismatched_fields.append("run_id")
        if (
            dependency_correlation["change_id"]
            and change_id
            and dependency_correlation["change_id"] != change_id
        ):
            mismatched_fields.append("change_id")
        if (
            dependency_correlation["manifest_fingerprint"]
            and manifest_fingerprint
            and dependency_correlation["manifest_fingerprint"] != manifest_fingerprint
        ):
            mismatched_fields.append("manifest_fingerprint")
        if (
            dependency_correlation["source_blueprint_fingerprint"]
            and source_blueprint_fingerprint
            and dependency_correlation["source_blueprint_fingerprint"]
            != source_blueprint_fingerprint
        ):
            mismatched_fields.append("source_blueprint_fingerprint")
        if mismatched_fields:
            readiness_valid = False
            issues.append(
                _build_a2a_entry_issue(
                    "runbook_a2a_{}_readiness_correlation_mismatch".format(issue_prefix),
                    "Checklist de prontidão {} diverge da correlação oficial do runbook.".format(
                        dependency_key.upper().replace("_", ".")
                    ),
                    cause="O checklist do WP anterior não corresponde ao mesmo contexto técnico do A2A.",
                    recommended_action="Republicar o checklist com os mesmos identificadores e fingerprints oficiais.",
                    details={"mismatched_fields": sorted(mismatched_fields)},
                )
            )

        checklist = readiness_payload.get("checklist", readiness_payload)
        if not isinstance(checklist, dict):
            readiness_valid = False
            issues.append(
                _build_a2a_entry_issue(
                    "runbook_a2a_{}_readiness_payload_invalid".format(issue_prefix),
                    "Checklist de prontidão {} sem estrutura válida para o gate A2A.".format(
                        dependency_key.upper().replace("_", ".")
                    ),
                    cause="O payload de checklist não contém um mapa de checks booleanos.",
                    recommended_action="Publicar novamente o checklist oficial com checks booleanos explícitos.",
                )
            )
        else:
            unsatisfied_checks = sorted(
                [
                    str(key).strip()
                    for key, value in checklist.items()
                    if not bool(value)
                ]
            )
            if unsatisfied_checks:
                readiness_valid = False
                issues.append(
                    _build_a2a_entry_issue(
                        "runbook_a2a_{}_readiness_not_satisfied".format(issue_prefix),
                        "Checklist de prontidão {} ainda não está satisfeito para o A2A.".format(
                            dependency_key.upper().replace("_", ".")
                        ),
                        cause="Há checks oficiais do WP anterior ainda não satisfeitos.",
                        recommended_action="Concluir os itens pendentes do checklist antes de habilitar o dashboard A2A.",
                        details={"unsatisfied_checks": unsatisfied_checks},
                    )
                )

    return {
        "handoff_applicable": handoff_applicable,
        "handoff_valid": handoff_valid,
        "readiness_applicable": readiness_applicable,
        "readiness_valid": readiness_valid,
        "issues": issues,
    }


def _normalize_a2a_org_lookup(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _collect_a2a_organization_rows(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    topology_catalog = safe_run_state.get("topology_catalog", {})
    if not isinstance(topology_catalog, dict):
        topology_catalog = {}

    organization_rows = []
    organization_by_lookup = {}

    for organization in topology_catalog.get("organizations", []):
        if not isinstance(organization, dict):
            continue
        org_id = str(
            organization.get("org_id")
            or organization.get("orgId")
            or organization.get("org_key")
            or ""
        ).strip()
        org_name = str(
            organization.get("org_name") or organization.get("orgName") or org_id
        ).strip()
        org_lookup = _normalize_a2a_org_lookup(org_id or org_name)
        if not org_lookup:
            continue
        if org_lookup in organization_by_lookup:
            continue
        organization_row = {
            "organization_id": org_id,
            "organization_name": org_name or org_id,
            "organization_lookup": org_lookup,
        }
        organization_by_lookup[org_lookup] = organization_row
        organization_rows.append(organization_row)

    for host_row in safe_run_state.get("host_inventory", []):
        if not isinstance(host_row, dict):
            continue
        org_id = str(host_row.get("org_id", "") or "").strip()
        org_lookup = _normalize_a2a_org_lookup(org_id)
        if not org_lookup or org_lookup in organization_by_lookup:
            continue
        organization_row = {
            "organization_id": org_id,
            "organization_name": org_id,
            "organization_lookup": org_lookup,
        }
        organization_by_lookup[org_lookup] = organization_row
        organization_rows.append(organization_row)

    organization_rows.sort(key=lambda row: row.get("organization_name", ""))
    return organization_rows


def _resolve_a2a_entry_gate(store_payload, run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    run_id = str(safe_run_state.get("run_id", "") or "").strip()
    change_id = str(safe_run_state.get("change_id", "") or "").strip()
    manifest_fingerprint = str(safe_run_state.get("manifest_fingerprint", "") or "").strip().lower()
    source_blueprint_fingerprint = str(
        safe_run_state.get("source_blueprint_fingerprint", "") or ""
    ).strip().lower()
    backend_state = str(safe_run_state.get("backend_state", "") or "").strip().lower()
    pipeline_status = str(safe_run_state.get("status", "") or "pending").strip().lower() or "pending"
    official_decision = safe_run_state.get("official_decision", {})
    if not isinstance(official_decision, dict):
        official_decision = {}
    decision_value = str(official_decision.get("decision", "") or "").strip().lower()
    evidence_minimum_valid = bool(official_decision.get("evidence_minimum_valid", False))
    handoff_payload = safe_run_state.get("handoff_payload", {})
    if not isinstance(handoff_payload, dict):
        handoff_payload = {}
    handoff_correlation = handoff_payload.get("correlation", {})
    if not isinstance(handoff_correlation, dict):
        handoff_correlation = {}
    a2_3_dependency = _evaluate_a2a_dependency_payload(
        "a2_3",
        safe_run_state.get("a2_3_handoff", {}),
        safe_run_state.get("a2_3_readiness_checklist", {}),
        run_id,
        change_id,
        manifest_fingerprint,
        source_blueprint_fingerprint,
    )
    a2_4_dependency = _evaluate_a2a_dependency_payload(
        "a2_4",
        safe_run_state.get("a2_4_handoff", {}),
        safe_run_state.get("a2_4_readiness_checklist", {}),
        run_id,
        change_id,
        manifest_fingerprint,
        source_blueprint_fingerprint,
    )

    issues = []

    correlation_checks = {
        "run_id_present": bool(run_id),
        "change_id_present": bool(change_id),
        "manifest_fingerprint_present": bool(manifest_fingerprint),
        "source_blueprint_fingerprint_present": bool(source_blueprint_fingerprint),
    }
    correlation_valid = all(correlation_checks.values())

    if not correlation_checks["run_id_present"]:
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_run_id_required",
                "run_id oficial ausente para habilitar o dashboard operacional A2A.",
                cause="Nao existe correlacao oficial de execucao para o painel runtime.",
                recommended_action="Iniciar ou recuperar um run oficial com run_id persistido antes de abrir a jornada A2A.",
            )
        )
    if not correlation_checks["change_id_present"]:
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_change_id_required",
                "change_id oficial ausente para habilitar o dashboard operacional A2A.",
                cause="A execucao nao possui vinculacao oficial com a mudanca operacional.",
                recommended_action="Reexecutar o handoff/start do runbook com change_id valido e auditavel.",
            )
        )
    if not correlation_checks["manifest_fingerprint_present"]:
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_manifest_fingerprint_required",
                "manifest_fingerprint ausente para habilitar o dashboard operacional A2A.",
                cause="Nao ha fingerprint canonico do manifesto runtime para correlacao do painel.",
                recommended_action="Publicar novamente o handoff A2 com manifest_fingerprint oficial antes de abrir o dashboard.",
            )
        )
    if not correlation_checks["source_blueprint_fingerprint_present"]:
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_source_blueprint_fingerprint_required",
                "source_blueprint_fingerprint ausente para habilitar o dashboard operacional A2A.",
                cause="Nao ha correlacao completa entre blueprint de origem e runtime observado.",
                recommended_action="Regenerar o contexto oficial do run com source_blueprint_fingerprint persistido.",
            )
        )

    handoff_mismatches = []
    handoff_change_id = str(handoff_correlation.get("change_id", "") or "").strip()
    if handoff_change_id and change_id and handoff_change_id != change_id:
        handoff_mismatches.append("change_id")
    handoff_manifest = str(handoff_correlation.get("manifest_fingerprint", "") or "").strip().lower()
    if handoff_manifest and manifest_fingerprint and handoff_manifest != manifest_fingerprint:
        handoff_mismatches.append("manifest_fingerprint")
    handoff_source = str(
        handoff_correlation.get("source_blueprint_fingerprint", "") or ""
    ).strip().lower()
    if (
        handoff_source
        and source_blueprint_fingerprint
        and handoff_source != source_blueprint_fingerprint
    ):
        handoff_mismatches.append("source_blueprint_fingerprint")
    if handoff_mismatches:
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_handoff_correlation_mismatch",
                "Correlacao do handoff diverge do estado oficial retornado pelo backend.",
                cause="O contexto oficial da UI nao corresponde ao payload persistido do runbook.",
                recommended_action="Invalidar o contexto atual da tela e recarregar o run oficial antes de habilitar a jornada A2A.",
                details={"mismatched_fields": sorted(handoff_mismatches)},
            )
        )
    handoff_correlation_valid = len(handoff_mismatches) == 0
    dependency_gate_valid = (
        a2_3_dependency.get("handoff_valid", True)
        and a2_3_dependency.get("readiness_valid", True)
        and a2_4_dependency.get("handoff_valid", True)
        and a2_4_dependency.get("readiness_valid", True)
    )
    issues.extend(a2_3_dependency.get("issues", []))
    issues.extend(a2_4_dependency.get("issues", []))

    available_artifacts = _collect_a2a_available_artifact_tokens(store_payload, safe_run_state)
    artifact_checks = {}
    missing_artifacts = []
    for required_artifact in A2A_ENTRY_REQUIRED_ARTIFACT_KEYS:
        available = _artifact_token_present(available_artifacts, required_artifact)
        artifact_checks[required_artifact] = available
        if not available:
            missing_artifacts.append(required_artifact)

    official_artifacts_ready = len(missing_artifacts) == 0
    if missing_artifacts:
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_required_artifacts_missing",
                "Artefatos oficiais minimos do A2A ausentes para habilitar o dashboard operacional.",
                cause="O backend nao apresentou todas as evidencias minimas de runtime exigidas pela entrada do A2A.",
                recommended_action="Concluir a baseline oficial do A2 e regenerar inventory-final, verify-report, stage-reports e ssh-execution-log antes de usar o dashboard.",
                details={
                    "missing_artifacts": missing_artifacts,
                    "available_artifacts": available_artifacts,
                },
            )
        )

    baseline_converged = (
        backend_state == "ready"
        and pipeline_status == "completed"
        and decision_value == "allow"
        and evidence_minimum_valid
        and correlation_valid
        and handoff_correlation_valid
        and dependency_gate_valid
        and official_artifacts_ready
    )

    if (
        correlation_valid
        and handoff_correlation_valid
        and dependency_gate_valid
        and official_artifacts_ready
        and not baseline_converged
    ):
        issues.append(
            _build_a2a_entry_issue(
                "runbook_a2a_baseline_not_converged",
                "Baseline tecnica oficial ainda nao convergiu para habilitar interatividade do A2A.",
                severity="warning" if pipeline_status in {"running", "pending", "paused"} else "error",
                cause="A decisao oficial do backend ou a convergencia final do pipeline ainda nao atingiram estado operacional liberado.",
                impact="O A2A permanece em modo leitura bloqueada e as acoes incrementais nao podem ser liberadas.",
                recommended_action="Aguardar conclusao do pipeline oficial ou tratar a falha reportada no verify/decision-trace antes de prosseguir.",
                details={
                    "backend_state": backend_state,
                    "pipeline_status": pipeline_status,
                    "official_decision": decision_value,
                    "evidence_minimum_valid": evidence_minimum_valid,
                },
            )
        )

    if baseline_converged:
        readiness_status = "implemented"
    elif not correlation_valid or not handoff_correlation_valid or not dependency_gate_valid:
        readiness_status = "blocked"
    elif pipeline_status in {"pending", "running", "paused"}:
        readiness_status = "pending"
    elif decision_value == "block" or pipeline_status == "failed":
        readiness_status = "blocked"
    else:
        readiness_status = "partial"

    interactive_enabled = readiness_status == "implemented"
    organization_rows = _collect_a2a_organization_rows(safe_run_state)
    host_inventory = safe_run_state.get("host_inventory", [])
    if not isinstance(host_inventory, list):
        host_inventory = []
    host_evidence_by_org = {}
    for host_row in host_inventory:
        if not isinstance(host_row, dict):
            continue
        org_lookup = _normalize_a2a_org_lookup(host_row.get("org_id", ""))
        if not org_lookup:
            continue
        host_evidence_by_org[org_lookup] = host_evidence_by_org.get(org_lookup, 0) + 1

    issue_codes = [
        issue.get("code", "")
        for issue in issues
        if isinstance(issue, dict) and str(issue.get("code", "")).strip()
    ]
    organization_readiness = []
    for organization_row in organization_rows:
        org_lookup = organization_row.get("organization_lookup", "")
        observed_host_count = host_evidence_by_org.get(org_lookup, 0)
        observed_in_runtime = observed_host_count > 0

        if not correlation_valid:
            organization_status = "blocked"
        elif decision_value == "block" or pipeline_status == "failed":
            organization_status = "blocked"
        elif baseline_converged and observed_in_runtime:
            organization_status = "implemented"
        elif pipeline_status in {"pending", "running", "paused"}:
            organization_status = "partial" if observed_in_runtime else "pending"
        elif official_artifacts_ready and observed_in_runtime:
            organization_status = "partial"
        else:
            organization_status = "pending"

        organization_readiness.append(
            {
                "organization_id": organization_row.get("organization_id", ""),
                "organization_name": organization_row.get("organization_name", ""),
                "status": organization_status,
                "mode": "interactive" if organization_status == "implemented" else "read_only_blocked",
                "observed_host_count": observed_host_count,
                "observed_in_runtime": observed_in_runtime,
                "issue_codes": list(issue_codes),
            }
        )

    return {
        "contract_version": "a2a-entry-gate.v1",
        "source_of_truth": "official_backend_artifacts",
        "status": readiness_status,
        "mode": "interactive" if interactive_enabled else "read_only_blocked",
        "interactive_enabled": interactive_enabled,
        "correlation_valid": correlation_valid,
        "baseline_converged": baseline_converged,
        "official_artifacts_ready": official_artifacts_ready,
        "correlation": {
            "run_id": run_id,
            "change_id": change_id,
            "manifest_fingerprint": manifest_fingerprint,
            "source_blueprint_fingerprint": source_blueprint_fingerprint,
            "checks": correlation_checks,
        },
        "checks": {
            "backend_state_ready": backend_state == "ready",
            "pipeline_completed": pipeline_status == "completed",
            "official_decision_allow": decision_value == "allow",
            "evidence_minimum_valid": evidence_minimum_valid,
            "handoff_correlation_valid": handoff_correlation_valid,
            "a2_3_handoff_valid": a2_3_dependency.get("handoff_valid", True),
            "a2_3_readiness_valid": a2_3_dependency.get("readiness_valid", True),
            "a2_4_handoff_valid": a2_4_dependency.get("handoff_valid", True),
            "a2_4_readiness_valid": a2_4_dependency.get("readiness_valid", True),
        },
        "required_artifacts": list(A2A_ENTRY_REQUIRED_ARTIFACT_KEYS),
        "available_artifacts": available_artifacts,
        "missing_artifacts": missing_artifacts,
        "artifact_checks": artifact_checks,
        "action_availability": {
            "open_operational_dashboard": correlation_valid
            and handoff_correlation_valid
            and dependency_gate_valid,
            "add_peer": interactive_enabled,
            "add_orderer": interactive_enabled,
            "add_channel": interactive_enabled,
            "add_chaincode": interactive_enabled,
        },
        "organization_readiness": organization_readiness,
        "issues": issues,
        "updated_at_utc": str(safe_run_state.get("updated_at_utc", "") or "").strip(),
    }


def _normalize_runtime_telemetry_component_type(value):
    normalized = _normalize_runtime_component_node_type(value)
    if normalized == "apigateway":
        return "api_gateway"
    if normalized == "netapi":
        return "network_api"
    if normalized == "chaincode":
        return "chaincode_runtime"
    return normalized


def _normalize_runtime_telemetry_scope(component_type, value):
    normalized = str(value or "").strip().lower()
    if normalized in {"required", "planned", "optional"}:
        return normalized
    return A2A_RUNTIME_DEFAULT_SCOPE_BY_COMPONENT_TYPE.get(component_type, "required")


def _normalize_runtime_telemetry_criticality(component_type, value):
    normalized = str(value or "").strip().lower()
    if normalized in {"critical", "supporting"}:
        return normalized
    return A2A_RUNTIME_DEFAULT_CRITICALITY_BY_COMPONENT_TYPE.get(
        component_type, "supporting"
    )


def _resolve_runtime_telemetry_required_state(scope):
    if str(scope or "").strip().lower() == "planned":
        return "planned"
    return "running"


def _parse_runtime_utc_timestamp(value):
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _resolve_runtime_data_freshness(source_updated_at_utc, observed_at_utc):
    updated_dt = _parse_runtime_utc_timestamp(source_updated_at_utc)
    observed_dt = _parse_runtime_utc_timestamp(observed_at_utc)
    if updated_dt is None or observed_dt is None:
        return {
            "status": "unknown",
            "source_updated_at_utc": str(source_updated_at_utc or "").strip(),
            "observed_at_utc": str(observed_at_utc or "").strip(),
            "lag_seconds": None,
        }

    lag_seconds = int(max(0, (updated_dt - observed_dt).total_seconds()))
    return {
        "status": "fresh" if lag_seconds == 0 else "stale",
        "source_updated_at_utc": str(source_updated_at_utc or "").strip(),
        "observed_at_utc": str(observed_at_utc or "").strip(),
        "lag_seconds": lag_seconds,
    }


def _runtime_telemetry_sort_key(row):
    return (
        str(row.get("organization_name", "") or "").lower(),
        str(row.get("organization_id", "") or "").lower(),
        str(row.get("component_type", "") or "").lower(),
        str(row.get("name", "") or "").lower(),
        str(row.get("host_id", "") or "").lower(),
    )


def _collect_runtime_observation_rows(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    observation_rows = []

    host_inventory = safe_run_state.get("host_inventory", [])
    if isinstance(host_inventory, list):
        for host_row in host_inventory:
            if not isinstance(host_row, dict):
                continue
            observation_rows.append(
                {
                    "stage": str(host_row.get("stage", "") or "").strip(),
                    "checkpoint": str(host_row.get("checkpoint", "") or "").strip(),
                    "host_ref": str(host_row.get("host_ref", "") or "").strip(),
                    "org_id": str(host_row.get("org_id", "") or "").strip(),
                    "node_id": str(host_row.get("node_id", "") or "").strip(),
                    "observed_at_utc": str(host_row.get("timestamp_utc", "") or "").strip(),
                    "exit_code": _coerce_exit_code(host_row.get("exit_code", 0), default=0),
                    "container_status": "",
                    "gateway_available": False,
                    "source": "host_inventory",
                }
            )

    host_stage_evidences = safe_run_state.get("host_stage_evidences", {})
    if isinstance(host_stage_evidences, dict):
        for stage_key, stage_rows in host_stage_evidences.items():
            if not isinstance(stage_rows, list):
                continue
            for evidence_row in stage_rows:
                if not isinstance(evidence_row, dict):
                    continue
                runtime_container_recovered = evidence_row.get("runtime_container_recovered", {})
                if not isinstance(runtime_container_recovered, dict):
                    runtime_container_recovered = {}
                runtime_chaincode_placeholder = evidence_row.get(
                    "runtime_chaincode_placeholder", {}
                )
                if not isinstance(runtime_chaincode_placeholder, dict):
                    runtime_chaincode_placeholder = {}
                runtime_container_start_attempt = evidence_row.get(
                    "runtime_container_start_attempt", {}
                )
                if not isinstance(runtime_container_start_attempt, dict):
                    runtime_container_start_attempt = {}
                gateway_probe = evidence_row.get("chaincode_gateway_probe", {})
                if not isinstance(gateway_probe, dict):
                    gateway_probe = {}
                gateway_verify_probe = evidence_row.get("chaincode_gateway_verify_probe", {})
                if not isinstance(gateway_verify_probe, dict):
                    gateway_verify_probe = {}

                container_status = str(
                    runtime_container_recovered.get("status")
                    or runtime_chaincode_placeholder.get("status")
                    or runtime_container_start_attempt.get("status_after")
                    or ""
                ).strip().lower()
                gateway_available = bool(
                    gateway_probe.get("available", False)
                    or gateway_verify_probe.get("available", False)
                )
                if not container_status and gateway_available:
                    container_status = "running"

                observation_rows.append(
                    {
                        "stage": str(
                            evidence_row.get("stage") or stage_key or ""
                        ).strip(),
                        "checkpoint": str(evidence_row.get("checkpoint", "") or "").strip(),
                        "host_ref": str(evidence_row.get("host_ref", "") or "").strip(),
                        "org_id": str(evidence_row.get("org_id", "") or "").strip(),
                        "node_id": str(evidence_row.get("node_id", "") or "").strip(),
                        "observed_at_utc": str(
                            evidence_row.get("timestamp_utc")
                            or evidence_row.get("timestamp")
                            or ""
                        ).strip(),
                        "exit_code": _coerce_exit_code(
                            evidence_row.get("exit_code", 0), default=0
                        ),
                        "container_status": container_status,
                        "gateway_available": gateway_available,
                        "source": "host_stage_evidences",
                    }
                )

    observation_rows.sort(
        key=lambda row: (
            str(row.get("observed_at_utc", "") or ""),
            str(row.get("stage", "") or ""),
            str(row.get("checkpoint", "") or ""),
            str(row.get("node_id", "") or ""),
            str(row.get("host_ref", "") or ""),
        )
    )
    return observation_rows


def _match_runtime_observations(component_row, observation_rows):
    component_node_id = str(component_row.get("node_id", "") or "").strip()
    component_host_id = str(component_row.get("host_id", "") or "").strip()
    component_org_id = str(component_row.get("org_id", "") or "").strip().lower()
    component_type = str(component_row.get("component_type", "") or "").strip().lower()

    matched_rows = []
    for observation_row in observation_rows:
        if not isinstance(observation_row, dict):
            continue
        observation_node_id = str(observation_row.get("node_id", "") or "").strip()
        observation_host_id = str(observation_row.get("host_ref", "") or "").strip()
        observation_org_id = str(observation_row.get("org_id", "") or "").strip().lower()

        if component_node_id and observation_node_id and component_node_id == observation_node_id:
            matched_rows.append(observation_row)
            continue

        if component_type == "chaincode_runtime":
            continue

        if (
            component_host_id
            and observation_host_id
            and component_host_id == observation_host_id
            and component_org_id
            and observation_org_id
            and component_org_id == observation_org_id
        ):
            matched_rows.append(observation_row)

    return matched_rows


def _resolve_runtime_semantic_status(component_row, observation_rows, run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    scope = str(component_row.get("scope", "") or "").strip().lower()
    pipeline_status = str(safe_run_state.get("status", "pending") or "pending").strip().lower()
    official_decision = safe_run_state.get("official_decision", {})
    if not isinstance(official_decision, dict):
        official_decision = {}
    decision_value = str(official_decision.get("decision", "") or "").strip().lower()
    component_type = str(component_row.get("component_type", "") or "").strip().lower()

    if scope == "planned":
        return "planned", {
            "semantic_status": "planned",
            "evidence_source": "topology_catalog",
            "observed_at_utc": "",
            "stage": "",
            "checkpoint": "",
            "exit_code": 0,
            "container_status": "planned",
        }, {
            "status": "planned",
            "reason": "component_declared_as_planned",
        }, []

    if component_type == "chaincode_runtime":
        if pipeline_status == "completed" and decision_value == "allow":
            observed_at_utc = str(safe_run_state.get("updated_at_utc", "") or "").strip()
            return "running", {
                "semantic_status": "running",
                "evidence_source": "official_decision",
                "observed_at_utc": observed_at_utc,
                "stage": "verify",
                "checkpoint": "verify.consistency",
                "exit_code": 0,
                "container_status": "running",
            }, {
                "status": "healthy",
                "reason": "chaincode_runtime_committed_in_official_verify",
            }, []
        if pipeline_status == "failed" or decision_value == "block":
            return "degraded", {
                "semantic_status": "degraded",
                "evidence_source": "official_decision",
                "observed_at_utc": str(safe_run_state.get("updated_at_utc", "") or "").strip(),
                "stage": "verify",
                "checkpoint": "verify.consistency",
                "exit_code": 1,
                "container_status": "unknown",
            }, {
                "status": "degraded",
                "reason": "official_verify_blocked_chaincode_runtime",
            }, [
                _build_a2a_entry_issue(
                    "runbook_runtime_chaincode_not_converged",
                    "Runtime de chaincode ainda nao convergido na evidencia oficial do backend.",
                    severity="warning",
                    cause="A verificacao final do backend nao publicou allow para o runtime de chaincode.",
                    recommended_action="Reexecutar reconcile/verify e validar lifecycle committed antes de liberar a organizacao no A2A.",
                )
            ]
        return "unknown", {
            "semantic_status": "unknown",
            "evidence_source": "topology_catalog",
            "observed_at_utc": "",
            "stage": "",
            "checkpoint": "",
            "exit_code": 0,
            "container_status": "unknown",
        }, {
            "status": "unknown",
            "reason": "chaincode_runtime_not_observed_yet",
        }, []

    matched_rows = _match_runtime_observations(component_row, observation_rows)
    if matched_rows:
        latest_row = matched_rows[-1]
        exit_code = _coerce_exit_code(latest_row.get("exit_code", 0), default=0)
        container_status = str(latest_row.get("container_status", "") or "").strip().lower()
        if container_status in {"exited", "stopped", "dead"}:
            return "stopped", {
                "semantic_status": "stopped",
                "evidence_source": latest_row.get("source", "host_inventory"),
                "observed_at_utc": latest_row.get("observed_at_utc", ""),
                "stage": latest_row.get("stage", ""),
                "checkpoint": latest_row.get("checkpoint", ""),
                "exit_code": exit_code,
                "container_status": container_status,
            }, {
                "status": "degraded",
                "reason": "container_observed_as_stopped",
            }, [
                _build_a2a_entry_issue(
                    "runbook_runtime_component_stopped",
                    "Componente observado como parado na evidencia oficial do host.",
                    severity="warning",
                    cause="O backend encontrou o container sem status running na etapa oficial.",
                    recommended_action="Inspecionar o detalhe operacional do componente e executar repair/retry antes de liberar a operacao incremental.",
                )
            ]

        if exit_code > 0:
            return "degraded", {
                "semantic_status": "degraded",
                "evidence_source": latest_row.get("source", "host_inventory"),
                "observed_at_utc": latest_row.get("observed_at_utc", ""),
                "stage": latest_row.get("stage", ""),
                "checkpoint": latest_row.get("checkpoint", ""),
                "exit_code": exit_code,
                "container_status": container_status or "unknown",
            }, {
                "status": "degraded",
                "reason": "host_evidence_exit_code_non_zero",
            }, [
                _build_a2a_entry_issue(
                    "runbook_runtime_component_degraded",
                    "Componente com evidencias oficiais de falha ou degradacao tecnica.",
                    severity="warning",
                    cause="Ao menos uma execucao oficial do host retornou exit_code diferente de zero para o componente.",
                    recommended_action="Correlacionar stage-report e ssh-execution-log do run antes de qualquer acao incremental.",
                )
            ]

        return "running", {
            "semantic_status": "running",
            "evidence_source": latest_row.get("source", "host_inventory"),
            "observed_at_utc": latest_row.get("observed_at_utc", ""),
            "stage": latest_row.get("stage", ""),
            "checkpoint": latest_row.get("checkpoint", ""),
            "exit_code": exit_code,
            "container_status": container_status or "running",
        }, {
            "status": "healthy",
            "reason": "host_evidence_available_and_successful",
        }, []

    if pipeline_status == "completed" and decision_value == "allow":
        return "missing", {
            "semantic_status": "missing",
            "evidence_source": "official_decision",
            "observed_at_utc": str(safe_run_state.get("updated_at_utc", "") or "").strip(),
            "stage": "verify",
            "checkpoint": "verify.consistency",
            "exit_code": 1,
            "container_status": "missing",
        }, {
            "status": "degraded",
            "reason": "component_expected_but_not_observed",
        }, [
            _build_a2a_entry_issue(
                "runbook_runtime_component_missing",
                "Componente esperado pela topologia oficial sem observacao correspondente no snapshot runtime.",
                severity="warning",
                cause="A baseline oficial marcou allow, mas nao existe telemetria oficial do host para este componente no snapshot atual.",
                recommended_action="Executar refresh do snapshot runtime e validar consistencia entre topology_catalog, inventory-final e host evidences.",
            )
        ]

    if pipeline_status == "failed" or decision_value == "block":
        return "degraded", {
            "semantic_status": "degraded",
            "evidence_source": "official_decision",
            "observed_at_utc": str(safe_run_state.get("updated_at_utc", "") or "").strip(),
            "stage": "verify",
            "checkpoint": "verify.consistency",
            "exit_code": 1,
            "container_status": "unknown",
        }, {
            "status": "degraded",
            "reason": "pipeline_failed_without_runtime_observation",
        }, []

    return "unknown", {
        "semantic_status": "unknown",
        "evidence_source": "topology_catalog",
        "observed_at_utc": "",
        "stage": "",
        "checkpoint": "",
        "exit_code": 0,
        "container_status": "unknown",
    }, {
        "status": "unknown",
        "reason": "runtime_not_observed_yet",
    }, []


def _parse_runtime_environment_rows(run_state, host_row):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return []

    environment_args = _resolve_runtime_docker_environment_args(run_state, host_row)
    if not environment_args:
        return []

    parsed_tokens = shlex.split(environment_args)
    environment_rows = []
    index = 0
    while index < len(parsed_tokens):
        token = parsed_tokens[index]
        if token != "--env" or index + 1 >= len(parsed_tokens):
            index += 1
            continue
        raw_pair = parsed_tokens[index + 1]
        index += 2
        if "=" not in raw_pair:
            continue
        env_key, env_value = raw_pair.split("=", 1)
        normalized_key = str(env_key or "").strip()
        normalized_value = str(env_value or "").strip()
        if not normalized_key:
            continue

        entry = {
            "key": normalized_key,
            "secure_ref": normalized_value if _is_secure_credential_reference(normalized_value) else "",
            "value": "",
            "value_redacted": False,
            "value_digest": hashlib.sha256(normalized_value.encode("utf-8")).hexdigest()
            if normalized_value
            else "",
        }
        if entry["secure_ref"]:
            entry["value_redacted"] = True
        elif RUNTIME_TELEMETRY_SENSITIVE_ENV_KEY_REGEX.search(normalized_key):
            entry["value_redacted"] = True
        else:
            entry["value"] = normalized_value

        environment_rows.append(entry)

    environment_rows.sort(key=lambda row: str(row.get("key", "") or "").lower())
    return environment_rows


def _parse_runtime_mount_rows(run_state, host_row):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return []

    volume_args = _resolve_runtime_docker_volume_args(run_state, host_row)
    if not volume_args:
        return []

    parsed_tokens = shlex.split(volume_args)
    mount_rows = []
    index = 0
    while index < len(parsed_tokens):
        token = parsed_tokens[index]
        if token != "--volume" or index + 1 >= len(parsed_tokens):
            index += 1
            continue
        raw_binding = parsed_tokens[index + 1]
        index += 2
        host_path, separator, container_path = raw_binding.partition(":")
        if not separator:
            continue
        mount_rows.append(
            {
                "source": str(host_path or "").strip(),
                "target": str(container_path or "").strip(),
                "mode": "rw",
            }
        )

    mount_rows.sort(
        key=lambda row: (
            str(row.get("target", "") or "").lower(),
            str(row.get("source", "") or "").lower(),
        )
    )
    return mount_rows


def _resolve_runtime_component_ports(run_state, host_row):
    if not isinstance(run_state, dict) or not isinstance(host_row, dict):
        return []

    component_type = _normalize_runtime_telemetry_component_type(
        host_row.get("node_type", "")
    )
    host_port = _resolve_runtime_exposed_host_port(run_state, host_row)
    container_port = 0
    node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
    if node_type in RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE:
        container_port = _parse_port_value(
            RUNBOOK_RUNTIME_CONTAINER_PORT_BY_NODE_TYPE.get(node_type, "")
        )
    else:
        container_port = A2A_RUNTIME_DEFAULT_CONTAINER_PORTS_BY_COMPONENT_TYPE.get(
            component_type, 0
        )
    host_port_value = _parse_port_value(host_port)
    if container_port <= 0 and host_port_value <= 0:
        return []
    return [
        {
            "protocol": "tcp",
            "container_port": container_port if container_port > 0 else None,
            "host_port": host_port_value if host_port_value > 0 else None,
        }
    ]


def _resolve_runtime_component_artifacts(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    artifact_rows = safe_run_state.get("artifact_rows", [])
    if not isinstance(artifact_rows, list):
        artifact_rows = []

    correlated_rows = []
    for artifact_row in artifact_rows:
        if not isinstance(artifact_row, dict):
            continue
        correlated_rows.append(
            {
                "key": str(artifact_row.get("key", "") or "").strip(),
                "artifact_key": str(artifact_row.get("artifact_key", "") or "").strip(),
                "stage": str(artifact_row.get("stage", "") or "").strip(),
                "available": bool(artifact_row.get("available", False)),
                "payload_hash": str(artifact_row.get("payload_hash", "") or "").strip(),
                "timestamp_utc": str(artifact_row.get("timestamp_utc", "") or "").strip(),
            }
        )

    correlated_rows.sort(
        key=lambda row: (
            str(row.get("key", "") or "").lower(),
            str(row.get("artifact_key", "") or "").lower(),
        )
    )
    return {
        "run_id": str(safe_run_state.get("run_id", "") or "").strip(),
        "change_id": str(safe_run_state.get("change_id", "") or "").strip(),
        "manifest_fingerprint": str(
            safe_run_state.get("manifest_fingerprint", "") or ""
        ).strip(),
        "source_blueprint_fingerprint": str(
            safe_run_state.get("source_blueprint_fingerprint", "") or ""
        ).strip(),
        "artifact_rows": correlated_rows,
    }


def _build_runtime_telemetry_component_row(
    run_state,
    organization_row,
    component_type,
    name,
    node_id="",
    host_row=None,
    host_id="",
    channel_refs=None,
    chaincode_refs=None,
    scope="",
    criticality="",
):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    host_row = host_row if isinstance(host_row, dict) else {}
    normalized_component_type = _normalize_runtime_telemetry_component_type(component_type)
    normalized_scope = _normalize_runtime_telemetry_scope(
        normalized_component_type, scope
    )
    normalized_criticality = _normalize_runtime_telemetry_criticality(
        normalized_component_type, criticality
    )
    resolved_host_id = str(host_id or host_row.get("host_ref", "") or "").strip()
    resolved_node_id = str(node_id or host_row.get("node_id", "") or "").strip()
    component_name = str(name or resolved_node_id or normalized_component_type or "component").strip()
    observed_rows = _collect_runtime_observation_rows(safe_run_state)
    semantic_status, observed_state, health_row, issues = _resolve_runtime_semantic_status(
        {
            "component_type": normalized_component_type,
            "scope": normalized_scope,
            "node_id": resolved_node_id,
            "host_id": resolved_host_id,
            "org_id": organization_row.get("org_id", ""),
        },
        observed_rows,
        safe_run_state,
    )
    observed_at_utc = str(observed_state.get("observed_at_utc", "") or "").strip()
    container_name = ""
    if host_row:
        container_name = _resolve_runbook_container_name(safe_run_state, host_row)
    runtime_image = str(host_row.get("runtime_image", "") or "").strip()
    component_channels = sorted(
        [str(value or "").strip() for value in (channel_refs or []) if str(value or "").strip()]
    )
    component_chaincodes = sorted(
        [str(value or "").strip() for value in (chaincode_refs or []) if str(value or "").strip()]
    )
    component_row = {
        "component_id": "{}:{}".format(normalized_component_type, component_name),
        "component_type": normalized_component_type,
        "name": component_name,
        "container_name": container_name,
        "image": runtime_image,
        "platform": str(host_row.get("platform", "") or "docker/linux").strip()
        or "docker/linux",
        "status": semantic_status,
        "started_at": observed_at_utc,
        "health": health_row,
        "ports": _resolve_runtime_component_ports(safe_run_state, host_row),
        "mounts": _parse_runtime_mount_rows(safe_run_state, host_row),
        "env": _parse_runtime_environment_rows(safe_run_state, host_row),
        "host_id": resolved_host_id,
        "org_id": organization_row.get("org_id", ""),
        "channel_refs": component_channels,
        "chaincode_refs": component_chaincodes,
        "required_state": _resolve_runtime_telemetry_required_state(normalized_scope),
        "observed_state": observed_state,
        "scope": normalized_scope,
        "criticality": normalized_criticality,
        "observed_at": observed_at_utc,
        "issues": issues,
    }
    return component_row


def _build_runtime_telemetry_contract(run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    topology_catalog = safe_run_state.get("topology_catalog", {})
    if not isinstance(topology_catalog, dict):
        topology_catalog = {}
    organizations = topology_catalog.get("organizations", [])
    if not isinstance(organizations, list):
        organizations = []

    alias_registry = _build_topology_org_alias_registry(topology_catalog)
    host_mapping = safe_run_state.get("host_mapping", [])
    if not isinstance(host_mapping, list):
        host_mapping = []

    api_registry = safe_run_state.get("api_registry", [])
    if not isinstance(api_registry, list):
        api_registry = []

    artifact_bundle = _resolve_runtime_component_artifacts(safe_run_state)
    organization_payloads = []

    def _resolve_org_host_row(org_id, node_type):
        seed_row = {"org_id": org_id}
        resolved_host_row = _resolve_runtime_host_row_for_node_type(
            safe_run_state,
            seed_row,
            node_type,
        )
        return resolved_host_row if isinstance(resolved_host_row, dict) else {}

    for organization in organizations:
        if not isinstance(organization, dict):
            continue
        org_id = _resolve_canonical_org_id(
            organization.get("org_id") or organization.get("org_name") or "",
            alias_registry,
        )
        if not org_id:
            continue
        org_name = str(
            organization.get("org_name") or organization.get("orgName") or org_id
        ).strip() or org_id
        organization_row = {
            "org_id": org_id,
            "org_name": org_name,
            "domain": str(organization.get("domain", "") or "").strip(),
        }

        component_rows = []

        peers = organization.get("peers", []) if isinstance(organization.get("peers", []), list) else []
        for peer_index, peer_row in enumerate(peers):
            if not isinstance(peer_row, dict):
                continue
            peer_host_row = _resolve_org_host_row(org_id, "peer")
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "peer",
                    peer_row.get("node_id") or peer_row.get("name") or "peer-{}".format(peer_index + 1),
                    node_id=peer_row.get("node_id", ""),
                    host_row=peer_host_row,
                    host_id=peer_row.get("host_ref") or peer_host_row.get("host_ref", ""),
                    channel_refs=organization.get("channels", []),
                    chaincode_refs=organization.get("chaincodes", []),
                    scope=peer_row.get("desired_state", "required"),
                    criticality=peer_row.get("criticality", "critical"),
                )
            )

        if not peers:
            peer_host_row = _resolve_org_host_row(org_id, "peer")
            if peer_host_row:
                component_rows.append(
                    _build_runtime_telemetry_component_row(
                        safe_run_state,
                        organization_row,
                        "peer",
                        peer_host_row.get("node_id") or "peer0-{}".format(org_id),
                        node_id=peer_host_row.get("node_id", ""),
                        host_row=peer_host_row,
                        host_id=peer_host_row.get("host_ref", ""),
                        channel_refs=organization.get("channels", []),
                        chaincode_refs=organization.get("chaincodes", []),
                    )
                )

        orderers = (
            organization.get("orderers", [])
            if isinstance(organization.get("orderers", []), list)
            else []
        )
        for orderer_index, orderer_row in enumerate(orderers):
            if not isinstance(orderer_row, dict):
                continue
            orderer_host_row = _resolve_org_host_row(org_id, "orderer")
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "orderer",
                    orderer_row.get("node_id")
                    or orderer_row.get("name")
                    or "orderer-{}".format(orderer_index + 1),
                    node_id=orderer_row.get("node_id", ""),
                    host_row=orderer_host_row,
                    host_id=orderer_row.get("host_ref") or orderer_host_row.get("host_ref", ""),
                    channel_refs=organization.get("channels", []),
                    chaincode_refs=organization.get("chaincodes", []),
                    scope=orderer_row.get("desired_state", "required"),
                    criticality=orderer_row.get("criticality", "critical"),
                )
            )

        if not orderers:
            orderer_host_row = _resolve_org_host_row(org_id, "orderer")
            if orderer_host_row:
                component_rows.append(
                    _build_runtime_telemetry_component_row(
                        safe_run_state,
                        organization_row,
                        "orderer",
                        orderer_host_row.get("node_id") or "orderer0-{}".format(org_id),
                        node_id=orderer_host_row.get("node_id", ""),
                        host_row=orderer_host_row,
                        host_id=orderer_host_row.get("host_ref", ""),
                        channel_refs=organization.get("channels", []),
                        chaincode_refs=organization.get("chaincodes", []),
                    )
                )

        cas = organization.get("cas", []) if isinstance(organization.get("cas", []), list) else []
        for ca_index, ca_row in enumerate(cas):
            if not isinstance(ca_row, dict):
                continue
            ca_host_row = _resolve_org_host_row(org_id, "ca")
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "ca",
                    ca_row.get("node_id") or ca_row.get("name") or "ca-{}".format(ca_index + 1),
                    node_id=ca_row.get("node_id", ""),
                    host_row=ca_host_row,
                    host_id=ca_row.get("host_ref") or ca_host_row.get("host_ref", ""),
                    channel_refs=organization.get("channels", []),
                    chaincode_refs=organization.get("chaincodes", []),
                    scope=ca_row.get("desired_state", "required"),
                    criticality=ca_row.get("criticality", "critical"),
                )
            )

        if not cas:
            ca_host_row = _resolve_org_host_row(org_id, "ca")
            if ca_host_row or organization.get("ca"):
                organization_ca = organization.get("ca", {}) if isinstance(organization.get("ca", {}), dict) else {}
                component_rows.append(
                    _build_runtime_telemetry_component_row(
                        safe_run_state,
                        organization_row,
                        "ca",
                        organization_ca.get("name")
                        or organization_ca.get("node_id")
                        or ca_host_row.get("node_id")
                        or "ca-{}".format(org_id),
                        node_id=organization_ca.get("node_id") or ca_host_row.get("node_id", ""),
                        host_row=ca_host_row,
                        host_id=organization_ca.get("host_ref") or ca_host_row.get("host_ref", ""),
                        channel_refs=organization.get("channels", []),
                        chaincode_refs=organization.get("chaincodes", []),
                    )
                )

        couch_host_row = _resolve_org_host_row(org_id, "couch")
        if couch_host_row:
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "couch",
                    couch_host_row.get("node_id") or "couch-{}".format(org_id),
                    node_id=couch_host_row.get("node_id", ""),
                    host_row=couch_host_row,
                    host_id=couch_host_row.get("host_ref", ""),
                    channel_refs=organization.get("channels", []),
                    chaincode_refs=organization.get("chaincodes", []),
                )
            )

        api_gateway_host_row = _resolve_org_host_row(org_id, "apigateway")
        if api_gateway_host_row:
            api_channel_refs = []
            api_chaincode_refs = []
            for api_row in api_registry:
                if not isinstance(api_row, dict):
                    continue
                api_org = _resolve_canonical_org_id(
                    api_row.get("org_id") or api_row.get("org_name") or "",
                    alias_registry,
                )
                if str(api_org or "").strip().lower() != org_id.lower():
                    continue
                api_channel_id = str(api_row.get("channel_id", "") or "").strip()
                api_chaincode_id = str(api_row.get("chaincode_id", "") or "").strip()
                if api_channel_id:
                    api_channel_refs.append(api_channel_id)
                if api_chaincode_id:
                    api_chaincode_refs.append(api_chaincode_id)
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "apigateway",
                    api_gateway_host_row.get("node_id") or "api-gateway-{}".format(org_id),
                    node_id=api_gateway_host_row.get("node_id", ""),
                    host_row=api_gateway_host_row,
                    host_id=api_gateway_host_row.get("host_ref", ""),
                    channel_refs=api_channel_refs or organization.get("channels", []),
                    chaincode_refs=api_chaincode_refs or organization.get("chaincodes", []),
                )
            )

        network_api_host_row = _resolve_org_host_row(org_id, "netapi")
        if network_api_host_row:
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "netapi",
                    network_api_host_row.get("node_id") or "network-api-{}".format(org_id),
                    node_id=network_api_host_row.get("node_id", ""),
                    host_row=network_api_host_row,
                    host_id=network_api_host_row.get("host_ref", ""),
                    channel_refs=organization.get("channels", []),
                    chaincode_refs=organization.get("chaincodes", []),
                )
            )

        chaincode_components = {}
        organization_channels = organization.get("channels", [])
        if not isinstance(organization_channels, list):
            organization_channels = []
        for channel_row in organization_channels:
            if isinstance(channel_row, dict):
                channel_id = str(
                    channel_row.get("channel_id")
                    or channel_row.get("name")
                    or channel_row.get("id")
                    or ""
                ).strip()
                chaincodes = channel_row.get("chaincodes", [])
                if not isinstance(chaincodes, list):
                    chaincodes = []
                for chaincode_id in chaincodes:
                    normalized_chaincode_id = str(chaincode_id or "").strip()
                    if not normalized_chaincode_id:
                        continue
                    chaincode_components[
                        "{}|{}".format(channel_id.lower(), normalized_chaincode_id.lower())
                    ] = {
                        "channel_id": channel_id,
                        "chaincode_id": normalized_chaincode_id,
                    }
                continue

            channel_id = str(channel_row or "").strip()
            if not channel_id:
                continue
            for chaincode_id in organization.get("chaincodes", []):
                normalized_chaincode_id = str(chaincode_id or "").strip()
                if not normalized_chaincode_id:
                    continue
                chaincode_components[
                    "{}|{}".format(channel_id.lower(), normalized_chaincode_id.lower())
                ] = {
                    "channel_id": channel_id,
                    "chaincode_id": normalized_chaincode_id,
                }

        for api_row in api_registry:
            if not isinstance(api_row, dict):
                continue
            api_org = _resolve_canonical_org_id(
                api_row.get("org_id") or api_row.get("org_name") or "",
                alias_registry,
            )
            if str(api_org or "").strip().lower() != org_id.lower():
                continue
            channel_id = str(api_row.get("channel_id", "") or "").strip()
            chaincode_id = str(api_row.get("chaincode_id", "") or "").strip()
            if not chaincode_id:
                continue
            chaincode_components[
                "{}|{}".format(channel_id.lower(), chaincode_id.lower())
            ] = {
                "channel_id": channel_id,
                "chaincode_id": chaincode_id,
            }

        peer_host_row = _resolve_org_host_row(org_id, "peer")
        for chaincode_row in sorted(
            chaincode_components.values(),
            key=lambda row: (
                str(row.get("channel_id", "") or "").lower(),
                str(row.get("chaincode_id", "") or "").lower(),
            ),
        ):
            chaincode_id = str(chaincode_row.get("chaincode_id", "") or "").strip()
            channel_id = str(chaincode_row.get("channel_id", "") or "").strip()
            if not chaincode_id:
                continue
            chaincode_name = (
                "{}@{}".format(channel_id, chaincode_id) if channel_id else chaincode_id
            )
            component_rows.append(
                _build_runtime_telemetry_component_row(
                    safe_run_state,
                    organization_row,
                    "chaincode",
                    chaincode_name,
                    node_id="",
                    host_row=peer_host_row,
                    host_id=peer_host_row.get("host_ref", "") if isinstance(peer_host_row, dict) else "",
                    channel_refs=[channel_id] if channel_id else [],
                    chaincode_refs=[chaincode_id],
                    scope="optional",
                    criticality="supporting",
                )
            )

        component_rows.sort(key=_runtime_telemetry_sort_key)
        observed_candidates = [
            str(row.get("observed_at", "") or "").strip()
            for row in component_rows
            if str(row.get("observed_at", "") or "").strip()
        ]
        observed_at_utc = (
            sorted(observed_candidates)[-1]
            if observed_candidates
            else str(safe_run_state.get("updated_at_utc", "") or "").strip()
        )
        issue_rows = []
        for component_row in component_rows:
            issue_rows.extend(component_row.get("issues", []))
        if not issue_rows and isinstance(safe_run_state.get("a2a_entry_gate", {}), dict):
            for gate_issue in safe_run_state.get("a2a_entry_gate", {}).get("issues", []):
                if isinstance(gate_issue, dict):
                    issue_rows.append(gate_issue)

        required_components = [
            row for row in component_rows if row.get("scope") == "required"
        ]
        required_running = [
            row for row in required_components if row.get("status") == "running"
        ]
        if required_components and len(required_running) == len(required_components):
            organization_health = "healthy"
        elif any(
            row.get("status") in {"degraded", "stopped", "missing"}
            for row in required_components
        ):
            organization_health = "degraded"
        else:
            organization_health = "unknown"

        channel_rows = []
        raw_channels = organization.get("channels", [])
        if not isinstance(raw_channels, list):
            raw_channels = []
        for raw_channel in raw_channels:
            if isinstance(raw_channel, dict):
                channel_id = str(
                    raw_channel.get("channel_id")
                    or raw_channel.get("name")
                    or raw_channel.get("id")
                    or ""
                ).strip()
                chaincodes = raw_channel.get("chaincodes", [])
                if not isinstance(chaincodes, list):
                    chaincodes = []
                member_orgs = raw_channel.get("member_orgs", [])
                if not isinstance(member_orgs, list):
                    member_orgs = []
            else:
                channel_id = str(raw_channel or "").strip()
                chaincodes = organization.get("chaincodes", [])
                if not isinstance(chaincodes, list):
                    chaincodes = []
                member_orgs = [org_name]
            if not channel_id:
                continue
            channel_rows.append(
                {
                    "channel_id": channel_id,
                    "member_orgs": sorted(
                        [str(value or "").strip() for value in member_orgs if str(value or "").strip()]
                    ),
                    "chaincodes": sorted(
                        [str(value or "").strip() for value in chaincodes if str(value or "").strip()]
                    ),
                    "status": "running"
                    if organization_health == "healthy"
                    else ("degraded" if organization_health == "degraded" else "unknown"),
                }
            )
        channel_rows.sort(key=lambda row: str(row.get("channel_id", "") or "").lower())

        chaincode_rows = []
        for component_row in component_rows:
            if component_row.get("component_type") != "chaincode_runtime":
                continue
            chaincode_refs = component_row.get("chaincode_refs", [])
            chaincode_rows.append(
                {
                    "chaincode_id": chaincode_refs[0] if chaincode_refs else component_row.get("name", ""),
                    "channel_refs": list(component_row.get("channel_refs", [])),
                    "status": component_row.get("status", "unknown"),
                }
            )
        chaincode_rows.sort(
            key=lambda row: (
                str(row.get("chaincode_id", "") or "").lower(),
                ",".join(row.get("channel_refs", [])),
            )
        )

        organization_payload = {
            "organization": organization_row,
            "components": component_rows,
            "channels": channel_rows,
            "chaincodes": chaincode_rows,
            "health": organization_health,
            "criticality": "critical"
            if any(row.get("criticality") == "critical" for row in component_rows)
            else "supporting",
            "observed_at": observed_at_utc,
            "data_freshness": _resolve_runtime_data_freshness(
                safe_run_state.get("updated_at_utc", ""),
                observed_at_utc,
            ),
            "issues": issue_rows,
            "artifacts": artifact_bundle,
        }
        organization_payload["read_model_fingerprint"] = _sha256_payload(
            organization_payload
        )
        organization_payloads.append(organization_payload)

    organization_payloads.sort(
        key=lambda row: (
            str(row.get("organization", {}).get("org_name", "") or "").lower(),
            str(row.get("organization", {}).get("org_id", "") or "").lower(),
        )
    )

    summary = {
        "organization_total": len(organization_payloads),
        "component_total": sum(
            len(row.get("components", [])) for row in organization_payloads
        ),
        "healthy_total": len(
            [row for row in organization_payloads if row.get("health") == "healthy"]
        ),
        "degraded_total": len(
            [row for row in organization_payloads if row.get("health") == "degraded"]
        ),
        "unknown_total": len(
            [row for row in organization_payloads if row.get("health") == "unknown"]
        ),
    }

    return {
        "contract_version": A2A_RUNTIME_TELEMETRY_CONTRACT_VERSION,
        "source_of_truth": "official_backend_artifacts",
        "generated_at_utc": str(safe_run_state.get("updated_at_utc", "") or "").strip(),
        "correlation": {
            "run_id": str(safe_run_state.get("run_id", "") or "").strip(),
            "change_id": str(safe_run_state.get("change_id", "") or "").strip(),
            "manifest_fingerprint": str(
                safe_run_state.get("manifest_fingerprint", "") or ""
            ).strip(),
            "source_blueprint_fingerprint": str(
                safe_run_state.get("source_blueprint_fingerprint", "") or ""
            ).strip(),
        },
        "organizations": organization_payloads,
        "summary": summary,
    }


def _normalize_a2a_org_lookup(value):
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    return re.sub(r"[^a-z0-9]+", "", normalized)


def _resolve_a2a_read_model_artifact_catalog(store_payload, run_state):
    available_tokens = _collect_a2a_available_artifact_tokens(store_payload, run_state)
    candidate_tokens_by_artifact = {
        "inventory-final": ["inventory-final", "inventory-final/inventory-final"],
        "verify-report": ["verify-report", "stage-reports/verify-report"],
        "runtime-reconcile-report": [
            "runtime-reconcile-report",
            "stage-reports/runtime-reconcile-report",
            "incremental-reconcile-report",
            "stage-reports/incremental-reconcile-report",
        ],
        "api-smoke-report": ["api-smoke-report", "stage-reports/api-smoke-report"],
    }
    catalog = {}
    for artifact_name, candidate_tokens in candidate_tokens_by_artifact.items():
        normalized_candidates = [
            _normalize_a2a_artifact_token(token) for token in candidate_tokens if token
        ]
        matched_tokens = sorted(
            [
                token
                for token in available_tokens
                if any(
                    token == candidate or token.startswith("{}/".format(candidate))
                    for candidate in normalized_candidates
                    if candidate
                )
            ]
        )
        catalog[artifact_name] = {
            "artifact": artifact_name,
            "available": bool(matched_tokens),
            "matched_tokens": matched_tokens,
            "primary_token": matched_tokens[0] if matched_tokens else (normalized_candidates[0] if normalized_candidates else ""),
        }
    return catalog


def _select_a2a_read_model_artifacts(artifact_catalog, artifact_names):
    safe_catalog = artifact_catalog if isinstance(artifact_catalog, dict) else {}
    selected_rows = []
    seen_names = set()
    for artifact_name in artifact_names or []:
        normalized_name = str(artifact_name or "").strip()
        if not normalized_name or normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)
        if normalized_name in safe_catalog and isinstance(safe_catalog.get(normalized_name), dict):
            selected_rows.append(dict(safe_catalog[normalized_name]))
            continue
        selected_rows.append(
            {
                "artifact": normalized_name,
                "available": False,
                "matched_tokens": [],
                "primary_token": _normalize_a2a_artifact_token(normalized_name),
            }
        )
    return selected_rows


def _resolve_a2a_workspace_health(status_rows):
    normalized_statuses = [
        str(status or "").strip().lower() for status in (status_rows or []) if str(status or "").strip()
    ]
    if not normalized_statuses:
        return "unknown"
    if any(status in {"degraded", "stopped", "missing"} for status in normalized_statuses):
        return "degraded"
    if normalized_statuses and all(status == "running" for status in normalized_statuses):
        return "healthy"
    if any(status == "planned" for status in normalized_statuses) and all(
        status in {"running", "planned"} for status in normalized_statuses
    ):
        return "planned"
    return "unknown"


def _resolve_a2a_workspace_observation_state(item_rows):
    safe_items = item_rows if isinstance(item_rows, list) else []
    if not safe_items:
        return "not_observed"
    observation_states = [
        str(row.get("observation_state", "") or "").strip().lower()
        for row in safe_items
        if isinstance(row, dict)
    ]
    if any(state == "observed" for state in observation_states):
        return "observed"
    if any(state == "planned" for state in observation_states):
        return "planned"
    return "not_observed"


def _deduplicate_a2a_detail_refs(detail_refs):
    deduplicated = []
    seen = set()
    for detail_ref in detail_refs or []:
        if not isinstance(detail_ref, dict):
            continue
        key = json.dumps(detail_ref, sort_keys=True, separators=(",", ":"))
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(detail_ref)
    return deduplicated


def _build_a2a_component_detail_ref(component_row):
    safe_component = component_row if isinstance(component_row, dict) else {}
    return {
        "kind": "component",
        "org_id": str(safe_component.get("org_id", "") or "").strip(),
        "host_id": str(safe_component.get("host_id", "") or "").strip(),
        "component_id": str(safe_component.get("component_id", "") or "").strip(),
    }


def _build_a2a_component_projection(component_row, source_artifacts):
    safe_component = component_row if isinstance(component_row, dict) else {}
    observed_state = safe_component.get("observed_state", {})
    if not isinstance(observed_state, dict):
        observed_state = {}
    evidence_source = str(observed_state.get("evidence_source", "") or "").strip().lower()
    observed_at = str(
        safe_component.get("observed_at", "") or observed_state.get("observed_at_utc", "") or ""
    ).strip()
    observation_state = "not_observed"
    if observed_at or evidence_source not in {"", "topology_catalog"}:
        observation_state = "observed"
    elif str(safe_component.get("status", "") or "").strip().lower() == "planned":
        observation_state = "planned"
    return {
        "component_id": str(safe_component.get("component_id", "") or "").strip(),
        "component_type": str(safe_component.get("component_type", "") or "").strip(),
        "name": str(safe_component.get("name", "") or "").strip(),
        "container_name": str(safe_component.get("container_name", "") or "").strip(),
        "host_id": str(safe_component.get("host_id", "") or "").strip(),
        "status": str(safe_component.get("status", "") or "").strip().lower() or "unknown",
        "health": safe_component.get("health", {}) if isinstance(safe_component.get("health", {}), dict) else {},
        "scope": str(safe_component.get("scope", "") or "").strip().lower(),
        "criticality": str(safe_component.get("criticality", "") or "").strip().lower(),
        "channel_refs": [
            str(value or "").strip() for value in safe_component.get("channel_refs", []) if str(value or "").strip()
        ],
        "chaincode_refs": [
            str(value or "").strip() for value in safe_component.get("chaincode_refs", []) if str(value or "").strip()
        ],
        "observed_at": observed_at,
        "observation_state": observation_state,
        "detail_ref": _build_a2a_component_detail_ref(safe_component),
        "source_artifacts": _clone_json_compatible(source_artifacts, source_artifacts) or [],
    }


def _build_a2a_workspace_block(block_id, title, item_rows, source_artifacts):
    safe_items = item_rows if isinstance(item_rows, list) else []
    status_rows = [
        str(item.get("status", "") or "").strip().lower()
        for item in safe_items
        if isinstance(item, dict)
    ]
    host_ids = sorted(
        {
            str(item.get("host_id", "") or "").strip()
            for item in safe_items
            if isinstance(item, dict) and str(item.get("host_id", "") or "").strip()
        }
    )
    detail_refs = _deduplicate_a2a_detail_refs(
        [item.get("detail_ref", {}) for item in safe_items if isinstance(item, dict)]
    )
    return {
        "block_id": str(block_id or "").strip(),
        "title": str(title or "").strip(),
        "item_total": len(safe_items),
        "status_totals": {
            "running": len([status for status in status_rows if status == "running"]),
            "degraded": len([status for status in status_rows if status == "degraded"]),
            "planned": len([status for status in status_rows if status == "planned"]),
            "stopped": len([status for status in status_rows if status == "stopped"]),
            "missing": len([status for status in status_rows if status == "missing"]),
            "unknown": len([status for status in status_rows if status == "unknown"]),
        },
        "health": _resolve_a2a_workspace_health(status_rows),
        "observation_state": _resolve_a2a_workspace_observation_state(safe_items),
        "host_ids": host_ids,
        "detail_refs": detail_refs,
        "items": safe_items,
        "source_artifacts": _clone_json_compatible(source_artifacts, source_artifacts) or [],
    }


def _build_a2a_business_group_rows(topology_catalog, organization_row, organization_payload, source_artifacts):
    safe_catalog = topology_catalog if isinstance(topology_catalog, dict) else {}
    safe_organization = organization_row if isinstance(organization_row, dict) else {}
    safe_payload = organization_payload if isinstance(organization_payload, dict) else {}
    organization_lookup_values = {
        _normalize_a2a_org_lookup(safe_organization.get("org_id", "")),
        _normalize_a2a_org_lookup(safe_organization.get("org_name", "")),
    }
    organization_lookup_values.discard("")
    organization_channel_ids = {
        str(channel_row.get("channel_id", "") or "").strip().lower()
        for channel_row in safe_payload.get("channels", [])
        if isinstance(channel_row, dict) and str(channel_row.get("channel_id", "") or "").strip()
    }
    business_group_rows = []
    for business_group in safe_catalog.get("business_groups", []):
        if not isinstance(business_group, dict):
            continue
        group_channels = business_group.get("channels", [])
        if not isinstance(group_channels, list):
            group_channels = []
        matched_channel_ids = []
        matched_member_rows = []
        for group_channel in group_channels:
            if isinstance(group_channel, dict):
                channel_id = str(
                    group_channel.get("channel_id")
                    or group_channel.get("name")
                    or group_channel.get("id")
                    or ""
                ).strip()
                member_orgs = group_channel.get("member_orgs", [])
                if not isinstance(member_orgs, list):
                    member_orgs = []
            else:
                channel_id = str(group_channel or "").strip()
                member_orgs = []
            member_lookups = {_normalize_a2a_org_lookup(value) for value in member_orgs if value}
            if channel_id and channel_id.lower() in organization_channel_ids:
                matched_channel_ids.append(channel_id)
                matched_member_rows.extend([str(value or "").strip() for value in member_orgs if str(value or "").strip()])
                continue
            if organization_lookup_values.intersection(member_lookups):
                if channel_id:
                    matched_channel_ids.append(channel_id)
                matched_member_rows.extend([str(value or "").strip() for value in member_orgs if str(value or "").strip()])

        direct_member_orgs = business_group.get("member_orgs", [])
        if not isinstance(direct_member_orgs, list):
            direct_member_orgs = []
        direct_member_lookups = {
            _normalize_a2a_org_lookup(value) for value in direct_member_orgs if value
        }
        if not matched_channel_ids and not organization_lookup_values.intersection(direct_member_lookups):
            continue

        business_group_rows.append(
            {
                "group_id": str(
                    business_group.get("group_id")
                    or business_group.get("id")
                    or business_group.get("name")
                    or "business-group"
                ).strip(),
                "name": str(business_group.get("name", "") or "Business Group").strip() or "Business Group",
                "description": str(business_group.get("description", "") or "").strip(),
                "channel_refs": sorted({value for value in matched_channel_ids if value}),
                "member_orgs": sorted(
                    {
                        value
                        for value in [*matched_member_rows, *[str(value or "").strip() for value in direct_member_orgs]]
                        if value
                    }
                ),
                "status": "running" if matched_channel_ids else "unknown",
                "observation_state": "observed" if matched_channel_ids else "not_observed",
                "detail_ref": {
                    "kind": "business_group",
                    "group_id": str(
                        business_group.get("group_id")
                        or business_group.get("id")
                        or business_group.get("name")
                        or "business-group"
                    ).strip(),
                    "org_id": str(safe_organization.get("org_id", "") or "").strip(),
                },
                "source_artifacts": _clone_json_compatible(source_artifacts, source_artifacts) or [],
            }
        )
    return business_group_rows


def _build_a2a_channel_projections(organization_payload, peer_rows, orderer_rows, chaincode_rows, api_rows, source_artifacts):
    safe_payload = organization_payload if isinstance(organization_payload, dict) else {}
    channel_projections = []
    for channel_row in safe_payload.get("channels", []):
        if not isinstance(channel_row, dict):
            continue
        channel_id = str(channel_row.get("channel_id", "") or "").strip()
        if not channel_id:
            continue
        related_peer_rows = [
            row for row in (peer_rows or []) if channel_id in row.get("channel_refs", [])
        ]
        related_chaincodes = [
            row for row in (chaincode_rows or []) if channel_id in row.get("channel_refs", [])
        ]
        related_apis = [
            row for row in (api_rows or []) if channel_id in row.get("channel_refs", [])
        ]
        health = _resolve_a2a_workspace_health(
            [channel_row.get("status", "")] +
            [row.get("status", "") for row in related_peer_rows] +
            [row.get("status", "") for row in related_chaincodes] +
            [row.get("status", "") for row in related_apis]
        )
        channel_projections.append(
            {
                "channel_id": channel_id,
                "member_orgs": [
                    str(value or "").strip()
                    for value in channel_row.get("member_orgs", [])
                    if str(value or "").strip()
                ],
                "peer_total": len(related_peer_rows),
                "orderer_total": len(orderer_rows or []),
                "chaincode_total": len(related_chaincodes),
                "api_total": len(related_apis),
                "status": str(channel_row.get("status", "") or "").strip().lower() or "unknown",
                "health": health,
                "observation_state": "observed",
                "detail_ref": {
                    "kind": "channel",
                    "channel_id": channel_id,
                    "org_id": str(
                        safe_payload.get("organization", {}).get("org_id", "")
                        if isinstance(safe_payload.get("organization", {}), dict)
                        else ""
                    ).strip(),
                },
                "source_artifacts": _clone_json_compatible(source_artifacts, source_artifacts) or [],
            }
        )
    channel_projections.sort(key=lambda row: str(row.get("channel_id", "") or "").lower())
    return channel_projections


def _build_a2a_member_projections(organization_row, channel_projections, source_artifacts):
    safe_organization = organization_row if isinstance(organization_row, dict) else {}
    local_org_lookup = {
        _normalize_a2a_org_lookup(safe_organization.get("org_id", "")),
        _normalize_a2a_org_lookup(safe_organization.get("org_name", "")),
    }
    local_org_lookup.discard("")
    member_registry = {}
    for channel_row in channel_projections or []:
        if not isinstance(channel_row, dict):
            continue
        channel_id = str(channel_row.get("channel_id", "") or "").strip()
        for member_org in channel_row.get("member_orgs", []):
            normalized_member = str(member_org or "").strip()
            if not normalized_member:
                continue
            registry_row = member_registry.setdefault(
                normalized_member,
                {
                    "member_id": normalized_member,
                    "member_name": normalized_member,
                    "channel_refs": [],
                },
            )
            if channel_id and channel_id not in registry_row["channel_refs"]:
                registry_row["channel_refs"].append(channel_id)
    projections = []
    for member_name, registry_row in sorted(member_registry.items(), key=lambda item: item[0].lower()):
        member_lookup = _normalize_a2a_org_lookup(member_name)
        projections.append(
            {
                "member_id": registry_row["member_id"],
                "member_name": registry_row["member_name"],
                "membership_role": "local_org" if member_lookup in local_org_lookup else "member_org",
                "channel_total": len(registry_row["channel_refs"]),
                "channel_refs": sorted(registry_row["channel_refs"]),
                "health": "healthy" if registry_row["channel_refs"] else "unknown",
                "observation_state": "observed" if registry_row["channel_refs"] else "not_observed",
                "source_artifacts": _clone_json_compatible(source_artifacts, source_artifacts) or [],
            }
        )
    return projections


def _build_a2a_chaincode_projections(organization_payload, chaincode_component_rows, api_rows, source_artifacts):
    safe_payload = organization_payload if isinstance(organization_payload, dict) else {}
    chaincode_registry = {}
    for chaincode_row in safe_payload.get("chaincodes", []):
        if not isinstance(chaincode_row, dict):
            continue
        chaincode_id = str(chaincode_row.get("chaincode_id", "") or "").strip()
        if not chaincode_id:
            continue
        chaincode_registry.setdefault(
            chaincode_id.lower(),
            {
                "chaincode_id": chaincode_id,
                "channel_refs": [],
                "statuses": [],
            },
        )
        registry_row = chaincode_registry[chaincode_id.lower()]
        for channel_id in chaincode_row.get("channel_refs", []):
            normalized_channel_id = str(channel_id or "").strip()
            if normalized_channel_id and normalized_channel_id not in registry_row["channel_refs"]:
                registry_row["channel_refs"].append(normalized_channel_id)
        registry_row["statuses"].append(str(chaincode_row.get("status", "") or "").strip().lower() or "unknown")

    for component_row in chaincode_component_rows or []:
        if not isinstance(component_row, dict):
            continue
        chaincode_id = ""
        chaincode_refs = component_row.get("chaincode_refs", [])
        if isinstance(chaincode_refs, list) and chaincode_refs:
            chaincode_id = str(chaincode_refs[0] or "").strip()
        if not chaincode_id:
            chaincode_id = str(component_row.get("name", "") or "").strip()
        if not chaincode_id:
            continue
        registry_row = chaincode_registry.setdefault(
            chaincode_id.lower(),
            {
                "chaincode_id": chaincode_id,
                "channel_refs": [],
                "statuses": [],
            },
        )
        for channel_id in component_row.get("channel_refs", []):
            normalized_channel_id = str(channel_id or "").strip()
            if normalized_channel_id and normalized_channel_id not in registry_row["channel_refs"]:
                registry_row["channel_refs"].append(normalized_channel_id)
        registry_row["statuses"].append(str(component_row.get("status", "") or "").strip().lower() or "unknown")

    projections = []
    for registry_row in sorted(chaincode_registry.values(), key=lambda row: str(row.get("chaincode_id", "") or "").lower()):
        chaincode_id = str(registry_row.get("chaincode_id", "") or "").strip()
        api_total = len(
            [
                row
                for row in (api_rows or [])
                if chaincode_id in row.get("chaincode_refs", [])
            ]
        )
        projections.append(
            {
                "chaincode_id": chaincode_id,
                "channel_refs": sorted(registry_row.get("channel_refs", [])),
                "api_total": api_total,
                "status": _resolve_a2a_workspace_health(registry_row.get("statuses", [])),
                "observation_state": "observed" if registry_row.get("statuses") else "not_observed",
                "source_artifacts": _clone_json_compatible(source_artifacts, source_artifacts) or [],
            }
        )
    return projections


def _build_organization_read_model_contract(store_payload, run_state):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    runtime_telemetry = safe_run_state.get("runtime_telemetry", {})
    if not isinstance(runtime_telemetry, dict) or not runtime_telemetry.get("organizations"):
        runtime_telemetry = _build_runtime_telemetry_contract(safe_run_state)

    topology_catalog = safe_run_state.get("topology_catalog", {})
    if not isinstance(topology_catalog, dict):
        topology_catalog = {}

    artifact_catalog = _resolve_a2a_read_model_artifact_catalog(store_payload, safe_run_state)
    component_source_artifacts = _select_a2a_read_model_artifacts(
        artifact_catalog,
        ["inventory-final", "verify-report", "runtime-reconcile-report"],
    )
    api_source_artifacts = _select_a2a_read_model_artifacts(
        artifact_catalog,
        ["inventory-final", "verify-report", "api-smoke-report"],
    )
    channel_source_artifacts = _select_a2a_read_model_artifacts(
        artifact_catalog,
        ["inventory-final", "verify-report", "runtime-reconcile-report", "api-smoke-report"],
    )

    organization_rows = []
    for organization_payload in runtime_telemetry.get("organizations", []):
        if not isinstance(organization_payload, dict):
            continue
        organization_row = organization_payload.get("organization", {})
        if not isinstance(organization_row, dict):
            organization_row = {}
        component_rows = organization_payload.get("components", [])
        if not isinstance(component_rows, list):
            component_rows = []

        peer_rows = [
            _build_a2a_component_projection(component_row, component_source_artifacts)
            for component_row in component_rows
            if isinstance(component_row, dict) and str(component_row.get("component_type", "") or "").strip() == "peer"
        ]
        orderer_rows = [
            _build_a2a_component_projection(component_row, component_source_artifacts)
            for component_row in component_rows
            if isinstance(component_row, dict) and str(component_row.get("component_type", "") or "").strip() == "orderer"
        ]
        ca_rows = [
            _build_a2a_component_projection(component_row, component_source_artifacts)
            for component_row in component_rows
            if isinstance(component_row, dict) and str(component_row.get("component_type", "") or "").strip() == "ca"
        ]
        api_rows = [
            _build_a2a_component_projection(component_row, api_source_artifacts)
            for component_row in component_rows
            if isinstance(component_row, dict)
            and str(component_row.get("component_type", "") or "").strip() in {"api_gateway", "network_api"}
        ]
        chaincode_component_rows = [
            _build_a2a_component_projection(component_row, channel_source_artifacts)
            for component_row in component_rows
            if isinstance(component_row, dict) and str(component_row.get("component_type", "") or "").strip() == "chaincode_runtime"
        ]
        business_group_rows = _build_a2a_business_group_rows(
            topology_catalog,
            organization_row,
            organization_payload,
            _select_a2a_read_model_artifacts(artifact_catalog, ["inventory-final", "verify-report"]),
        )
        channel_rows = _build_a2a_channel_projections(
            organization_payload,
            peer_rows,
            orderer_rows,
            chaincode_component_rows,
            api_rows,
            channel_source_artifacts,
        )
        member_rows = _build_a2a_member_projections(
            organization_row,
            channel_rows,
            _select_a2a_read_model_artifacts(artifact_catalog, ["inventory-final", "verify-report"]),
        )
        chaincode_rows = _build_a2a_chaincode_projections(
            organization_payload,
            chaincode_component_rows,
            api_rows,
            channel_source_artifacts,
        )

        blocks = {
            "ca": _build_a2a_workspace_block("ca", "CA", ca_rows, component_source_artifacts),
            "api": _build_a2a_workspace_block("api", "API", api_rows, api_source_artifacts),
            "peers": _build_a2a_workspace_block("peers", "Peers", peer_rows, component_source_artifacts),
            "orderers": _build_a2a_workspace_block("orderers", "Orderers", orderer_rows, component_source_artifacts),
            "business_group": _build_a2a_workspace_block(
                "business_group",
                "Business Group",
                business_group_rows,
                _select_a2a_read_model_artifacts(artifact_catalog, ["inventory-final", "verify-report"]),
            ),
            "channels": _build_a2a_workspace_block(
                "channels",
                "Channels",
                channel_rows,
                channel_source_artifacts,
            ),
        }

        organization_read_model = {
            "organization": {
                "org_id": str(organization_row.get("org_id", "") or "").strip(),
                "org_name": str(
                    organization_row.get("org_name", "") or organization_row.get("org_id", "") or ""
                ).strip(),
                "domain": str(organization_row.get("domain", "") or "").strip(),
            },
            "health": str(organization_payload.get("health", "") or "").strip().lower() or "unknown",
            "observed_at": str(organization_payload.get("observed_at", "") or "").strip(),
            "data_freshness": _clone_json_compatible(
                organization_payload.get("data_freshness", {}),
                organization_payload.get("data_freshness", {}),
            ) or {},
            "workspace": {
                "blocks": blocks,
                "projections": {
                    "channels": channel_rows,
                    "organization_members": member_rows,
                    "peers": peer_rows,
                    "orderers": orderer_rows,
                    "chaincodes": chaincode_rows,
                },
            },
            "artifact_origins": _select_a2a_read_model_artifacts(
                artifact_catalog,
                ["inventory-final", "verify-report", "runtime-reconcile-report", "api-smoke-report"],
            ),
            "issues": _clone_json_compatible(organization_payload.get("issues", []), organization_payload.get("issues", [])) or [],
        }
        organization_read_model["read_model_fingerprint"] = _sha256_payload(organization_read_model)
        organization_rows.append(organization_read_model)

    organization_rows.sort(
        key=lambda row: (
            str(row.get("organization", {}).get("org_name", "") or "").lower(),
            str(row.get("organization", {}).get("org_id", "") or "").lower(),
        )
    )

    contract_payload = {
        "contract_version": A2A_ORGANIZATION_READ_MODEL_CONTRACT_VERSION,
        "source_of_truth": "official_backend_artifacts",
        "generated_at_utc": str(safe_run_state.get("updated_at_utc", "") or "").strip(),
        "correlation": {
            "run_id": str(safe_run_state.get("run_id", "") or "").strip(),
            "change_id": str(safe_run_state.get("change_id", "") or "").strip(),
            "manifest_fingerprint": str(safe_run_state.get("manifest_fingerprint", "") or "").strip(),
            "source_blueprint_fingerprint": str(
                safe_run_state.get("source_blueprint_fingerprint", "") or ""
            ).strip(),
        },
        "artifact_origins": _select_a2a_read_model_artifacts(
            artifact_catalog,
            ["inventory-final", "verify-report", "runtime-reconcile-report", "api-smoke-report"],
        ),
        "organizations": organization_rows,
        "summary": {
            "organization_total": len(organization_rows),
            "healthy_total": len(
                [row for row in organization_rows if row.get("health") == "healthy"]
            ),
            "degraded_total": len(
                [row for row in organization_rows if row.get("health") == "degraded"]
            ),
            "unknown_total": len(
                [row for row in organization_rows if row.get("health") == "unknown"]
            ),
        },
    }
    contract_payload["read_model_fingerprint"] = _sha256_payload(contract_payload)
    return contract_payload


def _clone_json_compatible(value, fallback=None):
    try:
        return json.loads(json.dumps(value))
    except Exception:
        return fallback


def _runtime_inspection_cache_store(run_state):
    cache_store = run_state.setdefault("runtime_inspection_cache", {})
    if not isinstance(cache_store, dict):
        cache_store = {}
        run_state["runtime_inspection_cache"] = cache_store
    return cache_store


def _runtime_inspection_cache_key(org_id, host_id, component_id, inspection_scope):
    payload = {
        "org_id": str(org_id or "").strip().lower(),
        "host_id": str(host_id or "").strip().lower(),
        "component_id": str(component_id or "").strip().lower(),
        "inspection_scope": str(inspection_scope or "").strip().lower(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _runtime_inspection_cache_get(run_state, cache_key):
    cache_store = _runtime_inspection_cache_store(run_state)
    cache_row = cache_store.get(str(cache_key or "").strip(), {})
    if not isinstance(cache_row, dict):
        return {}
    return _clone_json_compatible(cache_row, dict(cache_row)) or {}


def _upsert_runtime_inspection_cache(run_state, cache_key, cache_row):
    cache_store = _runtime_inspection_cache_store(run_state)
    normalized_cache_key = str(cache_key or "").strip()
    normalized_row = cache_row if isinstance(cache_row, dict) else {}
    normalized_row = _clone_json_compatible(normalized_row, dict(normalized_row)) or {}
    normalized_row["cache_key"] = normalized_cache_key
    cache_store[normalized_cache_key] = normalized_row
    return _clone_json_compatible(normalized_row, dict(normalized_row)) or {}


def _runtime_inspection_trail_store(run_state):
    trail_rows = run_state.setdefault("runtime_inspection_trail", [])
    if not isinstance(trail_rows, list):
        trail_rows = []
        run_state["runtime_inspection_trail"] = trail_rows
    return trail_rows


def _append_runtime_inspection_trail(
    run_state,
    org_id,
    host_id,
    component_id,
    inspection_scope,
    cache_hit,
    cache_miss,
    stale,
    refresh_requested_at,
    refreshed_at,
    inspection_source,
    collection_status,
):
    trail_rows = _runtime_inspection_trail_store(run_state)
    trail_rows.append(
        {
            "timestamp_utc": _utc_now(),
            "org_id": str(org_id or "").strip(),
            "host_id": str(host_id or "").strip(),
            "component_id": str(component_id or "").strip(),
            "inspection_scope": str(inspection_scope or "").strip(),
            "cache_hit": bool(cache_hit),
            "cache_miss": bool(cache_miss),
            "stale": bool(stale),
            "refresh_requested_at": str(refresh_requested_at or "").strip(),
            "refreshed_at": str(refreshed_at or "").strip(),
            "inspection_source": str(inspection_source or "").strip(),
            "collection_status": str(collection_status or "").strip(),
        }
    )
    if len(trail_rows) > A2A_RUNTIME_INSPECTION_TRAIL_LIMIT:
        run_state["runtime_inspection_trail"] = trail_rows[-A2A_RUNTIME_INSPECTION_TRAIL_LIMIT :]


def _normalize_runtime_inspection_scope(value):
    normalized = str(value or "all").strip().lower() or "all"
    if normalized == "all":
        return normalized
    if normalized in set(A2A_RUNTIME_INSPECTION_SUPPORTED_SCOPES):
        return normalized
    return ""


def _resolve_runtime_inspection_ttl_seconds(inspection_scope):
    if str(inspection_scope or "").strip().lower() == "docker_logs":
        return A2A_RUNTIME_INSPECTION_LOG_TTL_SECONDS
    return A2A_RUNTIME_INSPECTION_DEFAULT_TTL_SECONDS


def _coerce_runtime_inspection_refresh_flag(value):
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "on", "refresh"}


def _resolve_runtime_inspection_node_type(component_type):
    normalized = str(component_type or "").strip().lower()
    if normalized == "api_gateway":
        return "apigateway"
    if normalized == "network_api":
        return "netapi"
    if normalized == "chaincode_runtime":
        return "chaincode"
    return normalized


def _resolve_runtime_inspection_component_context(run_state, org_id, host_id, component_id):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    runtime_telemetry = safe_run_state.get("runtime_telemetry", {})
    if not isinstance(runtime_telemetry, dict) or not runtime_telemetry.get("organizations"):
        runtime_telemetry = _build_runtime_telemetry_contract(safe_run_state)
        safe_run_state["runtime_telemetry"] = runtime_telemetry

    normalized_org_id = str(org_id or "").strip().lower()
    normalized_host_id = str(host_id or "").strip().lower()
    normalized_component_id = str(component_id or "").strip().lower()
    if not normalized_component_id:
        return {}, _build_runbook_error_payload(
            "runbook_runtime_inspection_component_required",
            "component_id oficial obrigatorio para inspecao runtime.",
        )

    organization_row = {}
    component_row = {}
    for organization_payload in runtime_telemetry.get("organizations", []):
        if not isinstance(organization_payload, dict):
            continue
        current_organization = organization_payload.get("organization", {})
        if not isinstance(current_organization, dict):
            current_organization = {}
        current_org_id = str(current_organization.get("org_id", "") or "").strip().lower()
        if normalized_org_id and current_org_id and current_org_id != normalized_org_id:
            continue
        for current_component in organization_payload.get("components", []):
            if not isinstance(current_component, dict):
                continue
            current_component_id = str(current_component.get("component_id", "") or "").strip().lower()
            current_host_id = str(current_component.get("host_id", "") or "").strip().lower()
            if current_component_id != normalized_component_id:
                continue
            if normalized_host_id and current_host_id and current_host_id != normalized_host_id:
                continue
            organization_row = current_organization
            component_row = current_component
            break
        if component_row:
            break

    if not component_row:
        return {}, _build_runbook_error_payload(
            "runbook_runtime_inspection_component_not_found",
            "Componente oficial nao encontrado no contrato runtime_telemetry.",
            {
                "org_id": str(org_id or "").strip(),
                "host_id": str(host_id or "").strip(),
                "component_id": str(component_id or "").strip(),
            },
        )

    host_mapping = safe_run_state.get("host_mapping", [])
    if not isinstance(host_mapping, list):
        host_mapping = []

    resolved_host_row = {}
    for host_row in host_mapping:
        if not isinstance(host_row, dict):
            continue
        candidate_host_id = str(host_row.get("host_ref", "") or "").strip().lower()
        candidate_org_id = str(host_row.get("org_id", "") or "").strip().lower()
        if normalized_host_id and candidate_host_id != normalized_host_id:
            continue
        if normalized_org_id and candidate_org_id and candidate_org_id != normalized_org_id:
            continue
        resolved_host_row = host_row
        break

    if not resolved_host_row:
        component_node_type = _resolve_runtime_inspection_node_type(
            component_row.get("component_type", "")
        )
        host_seed_row = {
            "org_id": organization_row.get("org_id", ""),
            "host_ref": component_row.get("host_id", ""),
        }
        if component_node_type == "chaincode":
            component_node_type = "peer"
        resolved_host_row = _resolve_runtime_host_row_for_node_type(
            safe_run_state,
            host_seed_row,
            component_node_type,
        )
        if not isinstance(resolved_host_row, dict):
            resolved_host_row = {}

    normalized_host_row, host_error = _normalize_host_ssh_target(resolved_host_row)
    if host_error:
        return {}, _build_runbook_error_payload(
            "runbook_runtime_inspection_host_unresolved",
            "Host oficial do componente nao pode ser resolvido para inspecao runtime.",
            {
                "component_id": str(component_row.get("component_id", "") or "").strip(),
                "host_id": str(component_row.get("host_id", "") or "").strip(),
            },
        )

    credential_by_machine = {}
    for credential_row in safe_run_state.get("machine_credentials", []):
        if not isinstance(credential_row, dict):
            continue
        machine_id = str(credential_row.get("machine_id", "") or "").strip()
        if machine_id:
            credential_by_machine[machine_id] = credential_row
    identity_file = _ensure_machine_credential_identity_file(
        credential_by_machine,
        _resolve_machine_id_from_host_row(normalized_host_row),
    )

    return {
        "organization": organization_row,
        "component": component_row,
        "host_row": normalized_host_row,
        "identity_file": identity_file,
        "node_type": _resolve_runtime_inspection_node_type(
            component_row.get("component_type", "")
        ),
    }, None


def _sanitize_runtime_inspection_text(value, limit=4000):
    sanitized = str(value or "")
    sanitized = RUNTIME_INSPECTION_SENSITIVE_ASSIGNMENT_REGEX.sub(r"\1[REDACTED]", sanitized)
    sanitized = RUNTIME_INSPECTION_SENSITIVE_FLAG_REGEX.sub(r"\1[REDACTED]", sanitized)
    return sanitized[:limit]


def _sanitize_runtime_inspection_command(parts):
    safe_parts = [str(part or "") for part in (parts or [])]
    redacted_parts = []
    previous_sensitive_flag = False
    redacted = False
    for part in safe_parts:
        normalized_part = str(part or "")
        lower_part = normalized_part.lower()
        if previous_sensitive_flag:
            redacted_parts.append("[REDACTED]")
            previous_sensitive_flag = False
            redacted = True
            continue
        if re.search(r"--(?:password|secret|token|passphrase|private-key|credential)$", lower_part):
            redacted_parts.append(normalized_part)
            previous_sensitive_flag = True
            continue
        if re.search(r"--(?:password|secret|token|passphrase|private-key|credential)=", lower_part):
            flag, _, _ = normalized_part.partition("=")
            redacted_parts.append("{}=[REDACTED]".format(flag))
            redacted = True
            continue
        redacted_text = _sanitize_runtime_inspection_text(normalized_part, limit=256)
        if redacted_text != normalized_part:
            redacted = True
        redacted_parts.append(redacted_text)
    return {
        "argv": redacted_parts,
        "redacted": redacted,
        "command_digest": hashlib.sha256("\u0000".join(safe_parts).encode("utf-8")).hexdigest()
        if safe_parts
        else "",
    }


def _sanitize_runtime_inspection_env_rows(env_rows):
    sanitized_rows = []
    for raw_env in env_rows or []:
        normalized_env = str(raw_env or "").strip()
        if not normalized_env or "=" not in normalized_env:
            continue
        env_key, env_value = normalized_env.split("=", 1)
        env_key = str(env_key or "").strip()
        env_value = str(env_value or "").strip()
        if not env_key:
            continue
        entry = {
            "key": env_key,
            "value": "",
            "value_redacted": False,
            "value_digest": hashlib.sha256(env_value.encode("utf-8")).hexdigest() if env_value else "",
            "secure_ref": env_value if _is_secure_credential_reference(env_value) else "",
        }
        if entry["secure_ref"] or RUNTIME_TELEMETRY_SENSITIVE_ENV_KEY_REGEX.search(env_key):
            entry["value_redacted"] = True
        else:
            entry["value"] = env_value
        sanitized_rows.append(entry)
    sanitized_rows.sort(key=lambda row: str(row.get("key", "") or "").lower())
    return sanitized_rows


def _sanitize_runtime_inspection_label_rows(label_rows):
    sanitized = {}
    for label_key in sorted(label_rows.keys() if isinstance(label_rows, dict) else []):
        label_value = str(label_rows.get(label_key, "") or "")
        if RUNTIME_TELEMETRY_SENSITIVE_ENV_KEY_REGEX.search(label_key):
            sanitized[label_key] = {
                "value": "",
                "value_redacted": True,
                "value_digest": hashlib.sha256(label_value.encode("utf-8")).hexdigest()
                if label_value
                else "",
            }
        else:
            sanitized[label_key] = {
                "value": _sanitize_runtime_inspection_text(label_value, limit=256),
                "value_redacted": False,
                "value_digest": hashlib.sha256(label_value.encode("utf-8")).hexdigest()
                if label_value
                else "",
            }
    return sanitized


def _sanitize_runtime_inspection_mount_rows(mount_rows):
    sanitized_rows = []
    for mount_row in mount_rows or []:
        if not isinstance(mount_row, dict):
            continue
        sanitized_rows.append(
            {
                "source": str(mount_row.get("Source", "") or mount_row.get("source", "") or "").strip(),
                "target": str(mount_row.get("Destination", "") or mount_row.get("target", "") or "").strip(),
                "mode": str(mount_row.get("Mode", "") or mount_row.get("mode", "") or "rw").strip() or "rw",
                "read_only": bool(mount_row.get("RW") is False or str(mount_row.get("mode", "")).strip().lower() == "ro"),
                "type": str(mount_row.get("Type", "") or mount_row.get("type", "") or "bind").strip() or "bind",
            }
        )
    sanitized_rows.sort(
        key=lambda row: (
            str(row.get("target", "") or "").lower(),
            str(row.get("source", "") or "").lower(),
        )
    )
    return sanitized_rows


def _sanitize_runtime_inspection_port_rows(port_rows):
    sanitized_rows = []
    if isinstance(port_rows, dict):
        for container_binding, host_bindings in port_rows.items():
            container_port, _, protocol = str(container_binding or "").partition("/")
            normalized_protocol = str(protocol or "tcp").strip() or "tcp"
            container_port_value = _parse_port_value(container_port)
            host_bindings = host_bindings if isinstance(host_bindings, list) else []
            if not host_bindings:
                sanitized_rows.append(
                    {
                        "protocol": normalized_protocol,
                        "container_port": container_port_value if container_port_value > 0 else None,
                        "host_port": None,
                        "host_ip": "",
                    }
                )
                continue
            for host_binding in host_bindings:
                if not isinstance(host_binding, dict):
                    continue
                sanitized_rows.append(
                    {
                        "protocol": normalized_protocol,
                        "container_port": container_port_value if container_port_value > 0 else None,
                        "host_port": _parse_port_value(host_binding.get("HostPort", "")) or None,
                        "host_ip": str(host_binding.get("HostIp", "") or "").strip(),
                    }
                )
    elif isinstance(port_rows, list):
        for port_row in port_rows:
            if not isinstance(port_row, dict):
                continue
            sanitized_rows.append(
                {
                    "protocol": str(port_row.get("protocol", "tcp") or "tcp").strip() or "tcp",
                    "container_port": _parse_port_value(port_row.get("container_port", "")) or None,
                    "host_port": _parse_port_value(port_row.get("host_port", "")) or None,
                    "host_ip": str(port_row.get("host_ip", "") or "").strip(),
                }
            )
    sanitized_rows.sort(
        key=lambda row: (
            str(row.get("host_port") or 0),
            str(row.get("container_port") or 0),
            str(row.get("protocol", "") or "").lower(),
        )
    )
    return sanitized_rows


def _build_runtime_inspection_container_resolution_command(run_state, component_context):
    safe_run_state = run_state if isinstance(run_state, dict) else {}
    safe_context = component_context if isinstance(component_context, dict) else {}
    component_row = safe_context.get("component", {}) if isinstance(safe_context.get("component", {}), dict) else {}
    host_row = safe_context.get("host_row", {}) if isinstance(safe_context.get("host_row", {}), dict) else {}
    node_type = str(safe_context.get("node_type", "") or "").strip().lower()
    node_id = str(host_row.get("node_id", "") or "").strip()
    run_id = str(safe_run_state.get("run_id", "") or "").strip()
    known_container_name = str(component_row.get("container_name", "") or "").strip()
    image_pattern = RUNTIME_INSPECTION_IMAGE_PATTERN_BY_NODE_TYPE.get(node_type, "")
    return (
        "COGNUS_KNOWN_CONTAINER={known_container}; "
        "COGNUS_NODE_ID={node_id}; "
        "COGNUS_NODE_TYPE={node_type}; "
        "COGNUS_RUN_ID={run_id}; "
        "COGNUS_IMAGE_PATTERN={image_pattern}; "
        "COGNUS_TARGET=; "
        "if [ -n \"$COGNUS_KNOWN_CONTAINER\" ] && docker inspect \"$COGNUS_KNOWN_CONTAINER\" >/dev/null 2>&1; then COGNUS_TARGET=\"$COGNUS_KNOWN_CONTAINER\"; fi; "
        "if [ -z \"$COGNUS_TARGET\" ] && [ -n \"$COGNUS_NODE_ID\" ]; then COGNUS_TARGET=$(docker ps -a --filter label=cognus.node_id=\"$COGNUS_NODE_ID\" --format '{{{{.Names}}}}' | head -n 1); fi; "
        "if [ -z \"$COGNUS_TARGET\" ] && [ -n \"$COGNUS_RUN_ID\" ] && [ -n \"$COGNUS_NODE_TYPE\" ]; then COGNUS_TARGET=$(docker ps -a --filter label=cognus.run_id=\"$COGNUS_RUN_ID\" --filter label=cognus.node_type=\"$COGNUS_NODE_TYPE\" --format '{{{{.Names}}}}' | head -n 1); fi; "
        "if [ -z \"$COGNUS_TARGET\" ] && [ -n \"$COGNUS_IMAGE_PATTERN\" ]; then COGNUS_TARGET=$(docker ps -a --format '{{{{.Names}}}} {{{{.Image}}}}' | grep -E \"$COGNUS_IMAGE_PATTERN\" | awk '{{print $1; exit}}'); fi; "
        "if [ -z \"$COGNUS_TARGET\" ]; then exit 1; fi; "
        "printf '%s\\n' \"$COGNUS_TARGET\""
    ).format(
        known_container=shlex.quote(known_container_name),
        node_id=shlex.quote(node_id),
        node_type=shlex.quote(node_type),
        run_id=shlex.quote(run_id),
        image_pattern=shlex.quote(image_pattern),
    )


def _resolve_runtime_inspection_remote_container_name(run_state, component_context):
    safe_context = component_context if isinstance(component_context, dict) else {}
    host_row = safe_context.get("host_row", {}) if isinstance(safe_context.get("host_row", {}), dict) else {}
    command = _build_runtime_inspection_container_resolution_command(run_state, safe_context)
    result = _run_remote_command(
        host_row.get("host_address", ""),
        host_row.get("ssh_user", ""),
        host_row.get("ssh_port", 22),
        command,
        safe_context.get("identity_file", ""),
    )
    container_name = str(result.get("stdout", "") or "").strip().splitlines()
    container_name = container_name[-1].strip() if container_name else ""
    if result.get("returncode", 1) != 0 or not container_name:
        return {
            "ok": False,
            "container_name": "",
            "error": _build_runbook_error_payload(
                "runbook_runtime_inspection_container_not_found",
                "Container oficial nao encontrado no host alvo para a inspecao runtime.",
                {
                    "component_id": str(
                        safe_context.get("component", {}).get("component_id", "")
                        if isinstance(safe_context.get("component", {}), dict)
                        else ""
                    ).strip(),
                    "stderr": _sanitize_runtime_inspection_text(result.get("stderr", ""), limit=512),
                },
            ),
        }
    return {
        "ok": True,
        "container_name": container_name,
    }


def _build_runtime_inspection_inspect_payload(component_context, container_name, inspect_row):
    safe_context = component_context if isinstance(component_context, dict) else {}
    component_row = safe_context.get("component", {}) if isinstance(safe_context.get("component", {}), dict) else {}
    safe_inspect_row = inspect_row if isinstance(inspect_row, dict) else {}
    config_row = safe_inspect_row.get("Config", {}) if isinstance(safe_inspect_row.get("Config", {}), dict) else {}
    state_row = safe_inspect_row.get("State", {}) if isinstance(safe_inspect_row.get("State", {}), dict) else {}
    health_row = state_row.get("Health", {}) if isinstance(state_row.get("Health", {}), dict) else {}
    network_settings = safe_inspect_row.get("NetworkSettings", {}) if isinstance(safe_inspect_row.get("NetworkSettings", {}), dict) else {}
    networks = network_settings.get("Networks", {}) if isinstance(network_settings.get("Networks", {}), dict) else {}

    health_logs = []
    for health_log_row in health_row.get("Log", []) if isinstance(health_row.get("Log", []), list) else []:
        if not isinstance(health_log_row, dict):
            continue
        health_logs.append(
            {
                "start": str(health_log_row.get("Start", "") or "").strip(),
                "end": str(health_log_row.get("End", "") or "").strip(),
                "exit_code": _coerce_exit_code(health_log_row.get("ExitCode", 0), default=0),
                "output": _sanitize_runtime_inspection_text(health_log_row.get("Output", ""), limit=512),
            }
        )

    command_parts = []
    if isinstance(config_row.get("Entrypoint", []), list):
        command_parts.extend([str(value or "") for value in config_row.get("Entrypoint", [])])
    if isinstance(config_row.get("Cmd", []), list):
        command_parts.extend([str(value or "") for value in config_row.get("Cmd", [])])
    if not command_parts and str(safe_inspect_row.get("Path", "") or "").strip():
        command_parts.append(str(safe_inspect_row.get("Path", "") or "").strip())
        if isinstance(safe_inspect_row.get("Args", []), list):
            command_parts.extend([str(value or "") for value in safe_inspect_row.get("Args", [])])

    return {
        "container_name": str(container_name or "").strip(),
        "container_id": str(safe_inspect_row.get("Id", "") or "").strip()[:12],
        "image": str(config_row.get("Image", "") or component_row.get("image", "") or "").strip(),
        "platform": str(component_row.get("platform", "") or "docker/linux").strip() or "docker/linux",
        "created_at": str(safe_inspect_row.get("Created", "") or "").strip(),
        "state": {
            "status": str(state_row.get("Status", "") or "").strip().lower(),
            "running": bool(state_row.get("Running", False)),
            "paused": bool(state_row.get("Paused", False)),
            "restarting": bool(state_row.get("Restarting", False)),
            "oom_killed": bool(state_row.get("OOMKilled", False)),
            "exit_code": _coerce_exit_code(state_row.get("ExitCode", 0), default=0),
            "started_at": str(state_row.get("StartedAt", "") or "").strip(),
            "finished_at": str(state_row.get("FinishedAt", "") or "").strip(),
            "restart_count": _coerce_exit_code(safe_inspect_row.get("RestartCount", 0), default=0),
        },
        "health": {
            "status": str(health_row.get("Status", "") or "").strip().lower(),
            "failing_streak": _coerce_exit_code(health_row.get("FailingStreak", 0), default=0),
            "logs": health_logs,
        },
        "command": _sanitize_runtime_inspection_command(command_parts),
        "env": _sanitize_runtime_inspection_env_rows(config_row.get("Env", [])),
        "mounts": _sanitize_runtime_inspection_mount_rows(safe_inspect_row.get("Mounts", [])),
        "ports": _sanitize_runtime_inspection_port_rows(network_settings.get("Ports", {})),
        "labels": _sanitize_runtime_inspection_label_rows(config_row.get("Labels", {})),
        "network": {
            "network_names": sorted([str(name or "").strip() for name in networks.keys()]),
            "ip_addresses": sorted(
                [str((row or {}).get("IPAddress", "") or "").strip() for row in networks.values() if isinstance(row, dict) and str((row or {}).get("IPAddress", "") or "").strip()]
            ),
        },
    }


def _collect_runtime_inspection_remote_inspect(run_state, component_context):
    safe_context = component_context if isinstance(component_context, dict) else {}
    host_row = safe_context.get("host_row", {}) if isinstance(safe_context.get("host_row", {}), dict) else {}
    container_resolution = _resolve_runtime_inspection_remote_container_name(run_state, safe_context)
    if not container_resolution.get("ok", False):
        return {
            "ok": False,
            "payload": {},
            "inspection_source": "remote_docker_inspect",
            "error": container_resolution.get("error", {}),
        }

    container_name = str(container_resolution.get("container_name", "") or "").strip()
    inspect_result = _run_remote_command(
        host_row.get("host_address", ""),
        host_row.get("ssh_user", ""),
        host_row.get("ssh_port", 22),
        "docker inspect --type container {}".format(shlex.quote(container_name)),
        safe_context.get("identity_file", ""),
    )
    if inspect_result.get("returncode", 1) != 0:
        return {
            "ok": False,
            "payload": {},
            "inspection_source": "remote_docker_inspect",
            "error": _build_runbook_error_payload(
                "runbook_runtime_inspection_remote_failed",
                "Falha na coleta remota de docker inspect para o componente oficial.",
                {
                    "component_id": str(
                        safe_context.get("component", {}).get("component_id", "")
                        if isinstance(safe_context.get("component", {}), dict)
                        else ""
                    ).strip(),
                    "stderr": _sanitize_runtime_inspection_text(inspect_result.get("stderr", ""), limit=512),
                },
            ),
        }
    try:
        parsed_payload = json.loads(str(inspect_result.get("stdout", "") or "[]"))
    except Exception:
        parsed_payload = []
    inspect_row = parsed_payload[0] if isinstance(parsed_payload, list) and parsed_payload else {}
    if not isinstance(inspect_row, dict) or not inspect_row:
        return {
            "ok": False,
            "payload": {},
            "inspection_source": "remote_docker_inspect",
            "error": _build_runbook_error_payload(
                "runbook_runtime_inspection_invalid_payload",
                "docker inspect retornou payload invalido para o componente oficial.",
                {
                    "component_id": str(
                        safe_context.get("component", {}).get("component_id", "")
                        if isinstance(safe_context.get("component", {}), dict)
                        else ""
                    ).strip(),
                },
            ),
        }
    return {
        "ok": True,
        "payload": _build_runtime_inspection_inspect_payload(
            safe_context,
            container_name,
            inspect_row,
        ),
        "inspection_source": "remote_docker_inspect",
        "error": {},
    }


def _collect_runtime_inspection_remote_logs(run_state, component_context):
    safe_context = component_context if isinstance(component_context, dict) else {}
    host_row = safe_context.get("host_row", {}) if isinstance(safe_context.get("host_row", {}), dict) else {}
    container_resolution = _resolve_runtime_inspection_remote_container_name(run_state, safe_context)
    if not container_resolution.get("ok", False):
        return {
            "ok": False,
            "payload": {},
            "inspection_source": "remote_docker_logs",
            "error": container_resolution.get("error", {}),
        }

    container_name = str(container_resolution.get("container_name", "") or "").strip()
    logs_result = _run_remote_command(
        host_row.get("host_address", ""),
        host_row.get("ssh_user", ""),
        host_row.get("ssh_port", 22),
        "docker logs --tail {} {} 2>&1".format(
            A2A_RUNTIME_INSPECTION_LOG_TAIL_LINES,
            shlex.quote(container_name),
        ),
        safe_context.get("identity_file", ""),
    )
    if logs_result.get("returncode", 1) != 0:
        return {
            "ok": False,
            "payload": {},
            "inspection_source": "remote_docker_logs",
            "error": _build_runbook_error_payload(
                "runbook_runtime_logs_remote_failed",
                "Falha na coleta remota de docker logs para o componente oficial.",
                {
                    "component_id": str(
                        safe_context.get("component", {}).get("component_id", "")
                        if isinstance(safe_context.get("component", {}), dict)
                        else ""
                    ).strip(),
                    "stderr": _sanitize_runtime_inspection_text(logs_result.get("stderr", ""), limit=512),
                },
            ),
        }
    sanitized_logs = _sanitize_runtime_inspection_text(logs_result.get("stdout", ""), limit=16000)
    log_lines = sanitized_logs.splitlines()
    return {
        "ok": True,
        "payload": {
            "container_name": container_name,
            "tail_lines": A2A_RUNTIME_INSPECTION_LOG_TAIL_LINES,
            "line_count": len(log_lines),
            "logs": sanitized_logs,
            "log_digest": hashlib.sha256(sanitized_logs.encode("utf-8")).hexdigest() if sanitized_logs else "",
        },
        "inspection_source": "remote_docker_logs",
        "error": {},
    }


def _derive_runtime_inspection_scope_payload(inspection_scope, inspect_payload):
    safe_inspect_payload = inspect_payload if isinstance(inspect_payload, dict) else {}
    if inspection_scope == "environment":
        return {
            "container_name": str(safe_inspect_payload.get("container_name", "") or "").strip(),
            "env": _clone_json_compatible(safe_inspect_payload.get("env", []), []),
        }
    if inspection_scope == "ports":
        return {
            "container_name": str(safe_inspect_payload.get("container_name", "") or "").strip(),
            "ports": _clone_json_compatible(safe_inspect_payload.get("ports", []), []),
        }
    if inspection_scope == "mounts":
        return {
            "container_name": str(safe_inspect_payload.get("container_name", "") or "").strip(),
            "mounts": _clone_json_compatible(safe_inspect_payload.get("mounts", []), []),
        }
    return {}


def _runtime_inspection_payload_present(payload):
    if payload in (None, ""):
        return False
    if isinstance(payload, dict):
        return len(payload) > 0
    if isinstance(payload, list):
        return len(payload) > 0
    return True


def _runtime_inspection_cache_is_stale(cache_row, inspection_scope):
    safe_cache_row = cache_row if isinstance(cache_row, dict) else {}
    refreshed_at = str(safe_cache_row.get("refreshed_at", "") or safe_cache_row.get("last_successful_refreshed_at", "")).strip()
    refreshed_dt = _parse_runtime_utc_timestamp(refreshed_at)
    if refreshed_dt is None:
        return True
    now_dt = _parse_runtime_utc_timestamp(_utc_now())
    if now_dt is None:
        return True
    ttl_seconds = _resolve_runtime_inspection_ttl_seconds(inspection_scope)
    return (now_dt - refreshed_dt).total_seconds() > ttl_seconds


def _build_runtime_inspection_scope_response(
    run_state,
    component_context,
    inspection_scope,
    cache_row,
    cache_hit,
    cache_miss,
    stale,
):
    safe_context = component_context if isinstance(component_context, dict) else {}
    safe_cache_row = cache_row if isinstance(cache_row, dict) else {}
    component_row = safe_context.get("component", {}) if isinstance(safe_context.get("component", {}), dict) else {}
    _append_runtime_inspection_trail(
        run_state,
        component_row.get("org_id", ""),
        component_row.get("host_id", ""),
        component_row.get("component_id", ""),
        inspection_scope,
        cache_hit,
        cache_miss,
        stale,
        safe_cache_row.get("refresh_requested_at", ""),
        safe_cache_row.get("refreshed_at", ""),
        safe_cache_row.get("inspection_source", ""),
        safe_cache_row.get("collection_status", ""),
    )
    return {
        "inspection_scope": inspection_scope,
        "payload": _clone_json_compatible(safe_cache_row.get("payload", {}), safe_cache_row.get("payload", {})),
        "cache": {
            "cache_key": str(safe_cache_row.get("cache_key", "") or "").strip(),
            "ttl_seconds": _resolve_runtime_inspection_ttl_seconds(inspection_scope),
            "cache_hit": bool(cache_hit),
            "cache_miss": bool(cache_miss),
            "stale": bool(stale),
            "refresh_requested_at": str(safe_cache_row.get("refresh_requested_at", "") or "").strip(),
            "refreshed_at": str(safe_cache_row.get("refreshed_at", "") or "").strip(),
            "inspection_source": str(safe_cache_row.get("inspection_source", "") or "").strip(),
            "collection_status": str(safe_cache_row.get("collection_status", "") or "").strip(),
            "payload_hash": str(safe_cache_row.get("payload_hash", "") or "").strip(),
            "last_error": _clone_json_compatible(safe_cache_row.get("last_error", {}), safe_cache_row.get("last_error", {})),
        },
    }


def _resolve_runtime_inspection_scope_entry(
    run_state,
    component_context,
    inspection_scope,
    refresh_requested=False,
    inspect_scope_entry=None,
):
    safe_context = component_context if isinstance(component_context, dict) else {}
    component_row = safe_context.get("component", {}) if isinstance(safe_context.get("component", {}), dict) else {}
    cache_key = _runtime_inspection_cache_key(
        component_row.get("org_id", ""),
        component_row.get("host_id", ""),
        component_row.get("component_id", ""),
        inspection_scope,
    )
    existing_cache_row = _runtime_inspection_cache_get(run_state, cache_key)
    if existing_cache_row and not refresh_requested:
        stale = _runtime_inspection_cache_is_stale(existing_cache_row, inspection_scope)
        return _build_runtime_inspection_scope_response(
            run_state,
            safe_context,
            inspection_scope,
            existing_cache_row,
            cache_hit=True,
            cache_miss=False,
            stale=stale,
        )

    refresh_requested_at = _utc_now() if refresh_requested else str(existing_cache_row.get("refresh_requested_at", "") or "").strip()
    if inspection_scope in {"environment", "ports", "mounts"}:
        effective_inspect_entry = inspect_scope_entry
        if not isinstance(effective_inspect_entry, dict) or not effective_inspect_entry.get("payload"):
            effective_inspect_entry = _resolve_runtime_inspection_scope_entry(
                run_state,
                safe_context,
                "docker_inspect",
                refresh_requested=refresh_requested,
            )
        inspect_payload = effective_inspect_entry.get("payload", {}) if isinstance(effective_inspect_entry, dict) else {}
        if _runtime_inspection_payload_present(inspect_payload):
            derived_payload = _derive_runtime_inspection_scope_payload(inspection_scope, inspect_payload)
            cache_row = {
                "cache_key": cache_key,
                "org_id": str(component_row.get("org_id", "") or "").strip(),
                "host_id": str(component_row.get("host_id", "") or "").strip(),
                "component_id": str(component_row.get("component_id", "") or "").strip(),
                "inspection_scope": inspection_scope,
                "refresh_requested_at": refresh_requested_at,
                "refreshed_at": _utc_now(),
                "last_successful_refreshed_at": _utc_now(),
                "inspection_source": "docker_inspect_projection",
                "collection_status": "ready",
                "payload_hash": _sha256_payload(derived_payload),
                "payload": derived_payload,
                "last_error": {},
            }
            persisted_cache_row = _upsert_runtime_inspection_cache(run_state, cache_key, cache_row)
            return _build_runtime_inspection_scope_response(
                run_state,
                safe_context,
                inspection_scope,
                persisted_cache_row,
                cache_hit=False,
                cache_miss=True,
                stale=False,
            )

        failed_cache_row = dict(existing_cache_row) if isinstance(existing_cache_row, dict) else {}
        failed_cache_row.update(
            {
                "cache_key": cache_key,
                "org_id": str(component_row.get("org_id", "") or "").strip(),
                "host_id": str(component_row.get("host_id", "") or "").strip(),
                "component_id": str(component_row.get("component_id", "") or "").strip(),
                "inspection_scope": inspection_scope,
                "refresh_requested_at": refresh_requested_at,
                "inspection_source": "docker_inspect_projection",
                "collection_status": "failed",
                "last_error": {
                    "code": "runbook_runtime_inspection_dependency_missing",
                    "message": "Nao foi possivel derivar o escopo a partir do docker inspect oficial.",
                    "timestamp_utc": _utc_now(),
                },
            }
        )
        persisted_cache_row = _upsert_runtime_inspection_cache(run_state, cache_key, failed_cache_row)
        return _build_runtime_inspection_scope_response(
            run_state,
            safe_context,
            inspection_scope,
            persisted_cache_row,
            cache_hit=bool(existing_cache_row),
            cache_miss=not bool(existing_cache_row),
            stale=True,
        )

    collector = _collect_runtime_inspection_remote_inspect
    inspection_source = "remote_docker_inspect"
    if inspection_scope == "docker_logs":
        collector = _collect_runtime_inspection_remote_logs
        inspection_source = "remote_docker_logs"

    collection_result = collector(run_state, safe_context)
    if collection_result.get("ok", False) and _runtime_inspection_payload_present(collection_result.get("payload", {})):
        refreshed_at = _utc_now()
        cache_row = {
            "cache_key": cache_key,
            "org_id": str(component_row.get("org_id", "") or "").strip(),
            "host_id": str(component_row.get("host_id", "") or "").strip(),
            "component_id": str(component_row.get("component_id", "") or "").strip(),
            "inspection_scope": inspection_scope,
            "refresh_requested_at": refresh_requested_at,
            "refreshed_at": refreshed_at,
            "last_successful_refreshed_at": refreshed_at,
            "inspection_source": str(collection_result.get("inspection_source", inspection_source) or inspection_source).strip(),
            "collection_status": "ready",
            "payload_hash": _sha256_payload(collection_result.get("payload", {})),
            "payload": collection_result.get("payload", {}),
            "last_error": {},
        }
        persisted_cache_row = _upsert_runtime_inspection_cache(run_state, cache_key, cache_row)
        return _build_runtime_inspection_scope_response(
            run_state,
            safe_context,
            inspection_scope,
            persisted_cache_row,
            cache_hit=False,
            cache_miss=True,
            stale=False,
        )

    preserved_cache_row = dict(existing_cache_row) if isinstance(existing_cache_row, dict) else {}
    preserved_cache_row.update(
        {
            "cache_key": cache_key,
            "org_id": str(component_row.get("org_id", "") or "").strip(),
            "host_id": str(component_row.get("host_id", "") or "").strip(),
            "component_id": str(component_row.get("component_id", "") or "").strip(),
            "inspection_scope": inspection_scope,
            "refresh_requested_at": refresh_requested_at,
            "inspection_source": str(collection_result.get("inspection_source", inspection_source) or inspection_source).strip(),
            "collection_status": "failed",
            "last_error": collection_result.get("error", {}),
        }
    )
    persisted_cache_row = _upsert_runtime_inspection_cache(run_state, cache_key, preserved_cache_row)
    return _build_runtime_inspection_scope_response(
        run_state,
        safe_context,
        inspection_scope,
        persisted_cache_row,
        cache_hit=bool(existing_cache_row),
        cache_miss=not bool(existing_cache_row),
        stale=True,
    )


def _build_runtime_inspection_response(
    run_state,
    org_id,
    host_id,
    component_id,
    inspection_scope,
    refresh_requested=False,
):
    component_context, component_error = _resolve_runtime_inspection_component_context(
        run_state,
        org_id,
        host_id,
        component_id,
    )
    if component_error:
        return {}, component_error

    resolved_scope = _normalize_runtime_inspection_scope(inspection_scope)
    if not resolved_scope:
        return {}, _build_runbook_error_payload(
            "runbook_runtime_inspection_scope_invalid",
            "inspection_scope invalido para a inspecao runtime oficial.",
            {
                "inspection_scope": str(inspection_scope or "").strip(),
                "supported_scopes": ["all", *A2A_RUNTIME_INSPECTION_SUPPORTED_SCOPES],
            },
        )

    scopes = {}
    inspect_scope_entry = _resolve_runtime_inspection_scope_entry(
        run_state,
        component_context,
        "docker_inspect",
        refresh_requested=refresh_requested if resolved_scope in {"all", "docker_inspect", "environment", "ports", "mounts"} else False,
    )
    if resolved_scope in {"all", "docker_inspect"}:
        scopes["docker_inspect"] = inspect_scope_entry
    if resolved_scope in {"all", "docker_logs"}:
        scopes["docker_logs"] = _resolve_runtime_inspection_scope_entry(
            run_state,
            component_context,
            "docker_logs",
            refresh_requested=refresh_requested,
        )
    for derived_scope in ("environment", "ports", "mounts"):
        if resolved_scope in {"all", derived_scope}:
            scopes[derived_scope] = _resolve_runtime_inspection_scope_entry(
                run_state,
                component_context,
                derived_scope,
                refresh_requested=refresh_requested,
                inspect_scope_entry=inspect_scope_entry,
            )

    issues = []
    stale = False
    for scope_entry in scopes.values():
        if not isinstance(scope_entry, dict):
            continue
        cache_row = scope_entry.get("cache", {}) if isinstance(scope_entry.get("cache", {}), dict) else {}
        stale = stale or bool(cache_row.get("stale", False))
        last_error = cache_row.get("last_error", {}) if isinstance(cache_row.get("last_error", {}), dict) else {}
        if last_error:
            issues.append(last_error)

    component_row = component_context.get("component", {}) if isinstance(component_context.get("component", {}), dict) else {}
    organization_row = component_context.get("organization", {}) if isinstance(component_context.get("organization", {}), dict) else {}
    return {
        "contract_version": A2A_RUNTIME_INSPECTION_CONTRACT_VERSION,
        "source_of_truth": "official_runtime_inspection_cache",
        "correlation": {
            "run_id": str(run_state.get("run_id", "") or "").strip(),
            "change_id": str(run_state.get("change_id", "") or "").strip(),
            "manifest_fingerprint": str(run_state.get("manifest_fingerprint", "") or "").strip(),
            "source_blueprint_fingerprint": str(run_state.get("source_blueprint_fingerprint", "") or "").strip(),
        },
        "component": {
            "component_id": str(component_row.get("component_id", "") or "").strip(),
            "component_type": str(component_row.get("component_type", "") or "").strip(),
            "name": str(component_row.get("name", "") or "").strip(),
            "container_name": str(component_row.get("container_name", "") or "").strip(),
            "image": str(component_row.get("image", "") or "").strip(),
            "platform": str(component_row.get("platform", "") or "").strip(),
            "status": str(component_row.get("status", "") or "").strip(),
            "org_id": str(component_row.get("org_id", "") or organization_row.get("org_id", "") or "").strip(),
            "host_id": str(component_row.get("host_id", "") or "").strip(),
            "scope": str(component_row.get("scope", "") or "").strip(),
            "criticality": str(component_row.get("criticality", "") or "").strip(),
        },
        "inspection_scope": resolved_scope,
        "stale": stale,
        "scopes": scopes,
        "issues": issues,
    }, None


def _resolve_runtime_image_policy(run_state):
    host_rows = run_state.get("host_mapping", [])
    if not isinstance(host_rows, list):
        host_rows = []

    placeholder_components = []
    for host_row in host_rows:
        if not isinstance(host_row, dict):
            continue
        runtime_image = str(host_row.get("runtime_image", "")).strip()
        node_type = _normalize_runtime_component_node_type(host_row.get("node_type", ""))
        if not _is_runtime_node_type_enabled(node_type):
            continue
        if _runtime_image_is_placeholder(runtime_image):
            placeholder_components.append(
                {
                    "org_id": str(host_row.get("org_id", "") or "").strip(),
                    "node_id": str(host_row.get("node_id", "") or "").strip(),
                    "node_type": node_type,
                    "host_ref": str(host_row.get("host_ref", "") or "").strip(),
                    "runtime_image": runtime_image,
                }
            )

    return {
        "placeholder_components": placeholder_components,
        "runtime_images_valid": len(placeholder_components) == 0,
    }


def _build_history_rows(run_state):
    run_id = run_state.get("run_id", "")
    change_id = run_state.get("change_id", "")
    history_rows = []

    for event in run_state.get("events", []):
        if not isinstance(event, dict):
            continue
        history_rows.append(
            {
                "timestamp_utc": event.get("timestamp_utc", ""),
                "level": event.get("level", "info"),
                "code": event.get("code", "runbook_event"),
                "message": event.get("message", ""),
                "run_id": event.get("run_id", run_id),
                "change_id": event.get("change_id", change_id),
                "stage": event.get("stage", ""),
                "checkpoint": event.get("checkpoint", ""),
                "host_ref": event.get("host_ref", ""),
                "org_id": event.get("org_id", ""),
                "fingerprint_sha256": event.get("fingerprint_sha256", ""),
                "actor_user": event.get("actor_user", ""),
                "actor_role": event.get("actor_role", ""),
            }
        )

    return history_rows


def _version_run_artifacts(store_payload, run_state, executor):
    _version_stage_reports(store_payload, run_state, executor)

    inventory_payload = {
        "run_id": run_state.get("run_id", ""),
        "change_id": run_state.get("change_id", ""),
        "status": run_state.get("status", "pending"),
        "current_stage": run_state.get("current_stage", ""),
        "current_checkpoint": run_state.get("current_checkpoint", ""),
        "host_inventory": run_state.get("host_inventory", []),
        "stage_statuses": [
            {
                "stage": stage.get("key", ""),
                "status": stage.get("status", "pending"),
            }
            for stage in run_state.get("stages", [])
        ],
        "timestamp_utc": run_state.get("updated_at_utc", ""),
    }
    _version_artifact(
        store_payload,
        run_state,
        "inventory-final",
        "inventory-final.json",
        inventory_payload,
        run_state.get("current_stage", ""),
        executor,
    )

    pipeline_payload = {
        "run_id": run_state.get("run_id", ""),
        "change_id": run_state.get("change_id", ""),
        "provider_key": run_state.get("provider_key", ""),
        "blueprint_fingerprint": run_state.get("blueprint_fingerprint", ""),
        "resolved_schema_version": run_state.get("resolved_schema_version", ""),
        "resource_lock_keys": _resource_keys_for_stage(
            run_state,
            run_state.get("current_stage", ""),
        ),
        "status": run_state.get("status", "pending"),
        "started_at_utc": run_state.get("started_at_utc", ""),
        "finished_at_utc": run_state.get("finished_at_utc", ""),
        "updated_at_utc": run_state.get("updated_at_utc", ""),
    }
    _version_artifact(
        store_payload,
        run_state,
        "pipeline-report",
        "pipeline-report.json",
        pipeline_payload,
        run_state.get("current_stage", ""),
        executor,
    )

    history_payload = {
        "run_id": run_state.get("run_id", ""),
        "change_id": run_state.get("change_id", ""),
        "format": "jsonl",
        "line_count": 0,
        "rows": [],
        "timestamp_utc": run_state.get("updated_at_utc", ""),
    }
    history_rows = _build_history_rows(run_state)
    history_payload["rows"] = history_rows
    history_payload["line_count"] = len(history_rows)
    _version_artifact(
        store_payload,
        run_state,
        "history",
        "history.jsonl",
        history_payload,
        run_state.get("current_stage", ""),
        executor,
    )

    minimum_evidence = _resolve_missing_minimum_evidence(store_payload, run_state)
    runtime_image_policy = _resolve_runtime_image_policy(run_state)
    decision_status = run_state.get("status", "pending")
    is_completed = decision_status == "completed"
    decision_allow = (
        is_completed
        and minimum_evidence["evidence_minimum_valid"]
        and runtime_image_policy["runtime_images_valid"]
    )
    decision_code = "ALLOW_COMPLETED_WITH_MIN_EVIDENCE"
    decision_reasons = []
    if not is_completed:
        decision_code = "BLOCK_RUN_NOT_COMPLETED"
        decision_reasons.append("runbook_not_completed")
    if not minimum_evidence["evidence_minimum_valid"]:
        decision_code = "BLOCK_MINIMUM_EVIDENCE_MISSING"
        decision_reasons.append("minimum_evidence_missing")
    if not runtime_image_policy["runtime_images_valid"]:
        decision_code = "BLOCK_RUNTIME_PLACEHOLDER_IMAGE"
        decision_reasons.append("runtime_placeholder_image")

    decision_payload = {
        "run_id": run_state.get("run_id", ""),
        "change_id": run_state.get("change_id", ""),
        "status": decision_status,
        "decision": "allow" if decision_allow else "block",
        "decision_code": decision_code,
        "decision_reasons": decision_reasons,
        "required_evidence_keys": minimum_evidence["required_keys"],
        "missing_evidence_keys": minimum_evidence["missing_keys"],
        "evidence_minimum_valid": minimum_evidence["evidence_minimum_valid"],
        "runtime_images_valid": runtime_image_policy["runtime_images_valid"],
        "placeholder_runtime_components": runtime_image_policy["placeholder_components"],
        "timestamp_utc": run_state.get("updated_at_utc", ""),
    }
    _version_artifact(
        store_payload,
        run_state,
        "decision-trace",
        "decision-trace.jsonl",
        decision_payload,
        run_state.get("current_stage", ""),
        executor,
    )
    run_state["official_decision"] = decision_payload

    artifacts_by_key = _latest_artifact_payloads(
        store_payload,
        run_state.get("run_id", ""),
    )
    artifact_rows = []
    for artifact_key, artifact_row in artifacts_by_key.items():
        if not isinstance(artifact_row, dict):
            continue
        artifact_group = artifact_row.get("artifact_group", "")
        artifact_name = artifact_row.get("artifact_name", "")
        display_key = artifact_name
        if artifact_group == "stage-reports":
            display_key = "stage-reports/{}".format(artifact_name)
        artifact_rows.append(
            {
                "key": display_key,
                "artifact_key": artifact_key,
                "artifact_group": artifact_group,
                "artifact_name": artifact_name,
                "version": artifact_row.get("version", 0),
                "available": True,
                "validated_at_utc": artifact_row.get("timestamp_utc", ""),
                "fingerprint_sha256": artifact_row.get("payload_hash", ""),
            }
        )
    artifact_rows.sort(key=lambda row: row.get("artifact_key", ""))
    run_state["artifact_rows"] = artifact_rows


def _normalize_event_level(level):
    normalized = str(level or "").strip().lower()
    if normalized in RUNBOOK_EVENT_LEVELS:
        return normalized
    return "info"


def _normalize_event_classification(classification, level):
    normalized = str(classification or "").strip().lower()
    if normalized in {"critical", "transient", "informational"}:
        return normalized
    return RUNBOOK_EVENT_CLASSIFICATION_BY_LEVEL.get(level, "informational")


def _resolve_event_component(
    stage,
    checkpoint,
    host_ref,
    component="",
):
    normalized_component = str(component or "").strip()
    if normalized_component:
        return normalized_component

    normalized_host_ref = str(host_ref or "").strip()
    if normalized_host_ref:
        return "host:{}".format(normalized_host_ref)

    normalized_stage = str(stage or "").strip()
    normalized_checkpoint = str(checkpoint or "").strip()
    if normalized_stage and normalized_checkpoint:
        return "{}::{}".format(normalized_stage, normalized_checkpoint)
    if normalized_stage:
        return "stage:{}".format(normalized_stage)
    if normalized_checkpoint:
        return "checkpoint:{}".format(normalized_checkpoint)
    return "runbook"


def _resolve_event_cause(cause, message, level):
    normalized_cause = str(cause or "").strip()
    if normalized_cause:
        return normalized_cause

    normalized_message = str(message or "").strip()
    if normalized_message:
        return normalized_message

    return RUNBOOK_EVENT_CAUSE_BY_LEVEL.get(level, RUNBOOK_EVENT_CAUSE_BY_LEVEL["info"])


def _resolve_event_impact(impact, level, stage):
    normalized_impact = str(impact or "").strip()
    if normalized_impact:
        return normalized_impact

    normalized_stage = str(stage or "").strip()
    if normalized_stage and level in {"warning", "error"}:
        return "Impacta continuidade da etapa '{}' ate tratamento do evento.".format(
            normalized_stage
        )

    return RUNBOOK_EVENT_IMPACT_BY_LEVEL.get(level, RUNBOOK_EVENT_IMPACT_BY_LEVEL["info"])


def _resolve_event_recommended_action(recommended_action, level):
    normalized_action = str(recommended_action or "").strip()
    if normalized_action:
        return normalized_action
    return RUNBOOK_EVENT_RECOMMENDED_ACTION_BY_LEVEL.get(
        level, RUNBOOK_EVENT_RECOMMENDED_ACTION_BY_LEVEL["info"]
    )


def _build_stage_order_map(run_state):
    stage_rows = run_state.get("stages", [])
    if not isinstance(stage_rows, list):
        return {}

    stage_order_map = {}
    for stage_index, stage in enumerate(stage_rows):
        if not isinstance(stage, dict):
            continue
        stage_key = str(stage.get("key", "")).strip()
        if not stage_key:
            continue
        raw_stage_order = stage.get("order", stage_index + 1)
        try:
            stage_order = int(raw_stage_order)
        except (TypeError, ValueError):
            stage_order = stage_index + 1
        stage_order_map[stage_key] = stage_order

    return stage_order_map


def _event_sort_key(event_row, stage_order_map):
    stage_key = str(event_row.get("stage", "")).strip()
    stage_order = stage_order_map.get(stage_key, 9999)
    timestamp_utc = str(event_row.get("timestamp_utc", "")).strip()
    if not timestamp_utc:
        timestamp_utc = "9999-12-31T23:59:59Z"

    return (
        stage_order,
        stage_key or "zzzz",
        timestamp_utc,
        str(event_row.get("id", "")).strip() or "zzzz",
        str(event_row.get("code", "")).strip() or "zzzz",
    )


def _normalize_event_row(event_row, run_state):
    if not isinstance(event_row, dict):
        return None

    level = _normalize_event_level(event_row.get("level", "info"))
    stage = str(event_row.get("stage", "")).strip()
    checkpoint = str(event_row.get("checkpoint", "")).strip()
    host_ref = str(event_row.get("host_ref", "")).strip()
    message = str(event_row.get("message", "")).strip()
    if not message:
        message = "Evento operacional recebido do backend."

    code = str(event_row.get("code", "")).strip() or "runbook_event"
    timestamp_utc = str(
        event_row.get("timestamp_utc", event_row.get("timestamp", ""))
    ).strip()
    if not timestamp_utc:
        timestamp_utc = _utc_now()

    recommended_action = event_row.get(
        "recommended_action", event_row.get("recommendedAction", "")
    )
    cause = event_row.get("cause", event_row.get("primary_cause", ""))
    impact = event_row.get("impact", "")
    classification = event_row.get("classification", "")
    component = event_row.get("component", "")

    return {
        "id": str(event_row.get("id", "")).strip()
        or "evt-{}".format(uuid.uuid4().hex[:12]),
        "timestamp_utc": timestamp_utc,
        "level": level,
        "code": code,
        "message": message,
        "run_id": str(event_row.get("run_id", run_state.get("run_id", ""))).strip(),
        "change_id": str(
            event_row.get("change_id", run_state.get("change_id", ""))
        ).strip(),
        "stage": stage,
        "checkpoint": checkpoint,
        "host_ref": host_ref,
        "org_id": str(event_row.get("org_id", "")).strip(),
        "fingerprint_sha256": str(event_row.get("fingerprint_sha256", "")).strip(),
        "actor_user": str(event_row.get("actor_user", "")).strip(),
        "actor_role": str(event_row.get("actor_role", "")).strip(),
        "component": _resolve_event_component(
            stage,
            checkpoint,
            host_ref,
            component=component,
        ),
        "cause": _resolve_event_cause(cause, message, level),
        "impact": _resolve_event_impact(impact, level, stage),
        "recommended_action": _resolve_event_recommended_action(
            recommended_action, level
        ),
        "classification": _normalize_event_classification(classification, level),
    }


def _append_event(
    run_state,
    level,
    code,
    message,
    host_ref="",
    org_id="",
    fingerprint_sha256="",
    actor_user="",
    actor_role="",
    component="",
    cause="",
    impact="",
    recommended_action="",
    classification="",
):
    event_rows = run_state.get("events", [])
    stage = run_state.get("current_stage", "")
    checkpoint = run_state.get("current_checkpoint", "")
    normalized_event_row = _normalize_event_row(
        {
            "id": "evt-{}".format(uuid.uuid4().hex[:12]),
            "timestamp_utc": _utc_now(),
            "level": level,
            "code": code,
            "message": message,
            "run_id": run_state.get("run_id", ""),
            "change_id": run_state.get("change_id", ""),
            "stage": stage,
            "checkpoint": checkpoint,
            "host_ref": host_ref,
            "org_id": org_id,
            "fingerprint_sha256": fingerprint_sha256,
            "actor_user": actor_user,
            "actor_role": actor_role,
            "component": component,
            "cause": cause,
            "impact": impact,
            "recommended_action": recommended_action,
            "classification": classification,
        },
        run_state,
    )
    if normalized_event_row:
        event_rows.append(normalized_event_row)
    run_state["events"] = event_rows


def _set_current_checkpoint(run_state, stage_index, checkpoint_index):
    run_state["current_stage_index"] = stage_index
    run_state["current_checkpoint_index"] = checkpoint_index
    stage = _get_stage(run_state, stage_index)
    checkpoint = _get_checkpoint(stage, checkpoint_index)
    run_state["current_stage"] = stage.get("key", "") if stage else ""
    run_state["current_checkpoint"] = checkpoint.get("key", "") if checkpoint else ""


def _resolve_stage_status(stage):
    checkpoint_rows = stage.get("checkpoints", [])
    checkpoint_statuses = [checkpoint.get("status", "pending") for checkpoint in checkpoint_rows]

    if any(status_name == "failed" for status_name in checkpoint_statuses):
        return "failed"
    if any(status_name == "paused" for status_name in checkpoint_statuses):
        return "paused"
    if checkpoint_rows and all(
        status_name == "completed" for status_name in checkpoint_statuses
    ):
        return "completed"
    if any(status_name == "running" for status_name in checkpoint_statuses):
        return "running"
    return "pending"


def _recompute_stage_statuses(run_state):
    stage_rows = run_state.get("stages", [])
    for stage in stage_rows:
        stage["status"] = _resolve_stage_status(stage)


def _get_stage(run_state, stage_index):
    stage_rows = run_state.get("stages", [])
    if stage_index < 0 or stage_index >= len(stage_rows):
        return None
    return stage_rows[stage_index]


def _get_checkpoint(stage, checkpoint_index):
    checkpoint_rows = stage.get("checkpoints", [])
    if checkpoint_index < 0 or checkpoint_index >= len(checkpoint_rows):
        return None
    return checkpoint_rows[checkpoint_index]


def _find_next_pending_checkpoint(run_state, start_stage_index, start_checkpoint_index):
    stage_rows = run_state.get("stages", [])
    for stage_index in range(start_stage_index, len(stage_rows)):
        checkpoint_rows = stage_rows[stage_index].get("checkpoints", [])
        first_checkpoint_index = 0
        if stage_index == start_stage_index:
            first_checkpoint_index = start_checkpoint_index

        for checkpoint_index in range(first_checkpoint_index, len(checkpoint_rows)):
            checkpoint = checkpoint_rows[checkpoint_index]
            if checkpoint.get("status") == "pending":
                return stage_index, checkpoint_index

    return None, None


def _activate_checkpoint(run_state, stage_index, checkpoint_index):
    stage = _get_stage(run_state, stage_index)
    checkpoint = _get_checkpoint(stage, checkpoint_index)
    if not stage or not checkpoint:
        return False

    checkpoint["status"] = "running"
    checkpoint["started_at_utc"] = _utc_now()
    checkpoint["completed_at_utc"] = ""
    stage["status"] = "running"
    _set_current_checkpoint(run_state, stage_index, checkpoint_index)
    run_state["updated_at_utc"] = _utc_now()

    return True


def _build_runbook_error_payload(code, message, details=None):
    payload = {
        "code": code,
        "message": message,
        "contract_version": RUNBOOK_CONTRACT_VERSION,
        "source_mode": "official",
    }
    if isinstance(details, dict) and details:
        payload["details"] = details
    return payload


def _error_response(code, message, http_status=status.HTTP_400_BAD_REQUEST, details=None):
    return Response(
        {
            "status": "fail",
            "msg": code,
            "data": _build_runbook_error_payload(code, message, details),
        },
        status=http_status,
    )


def _runbook_idempotency_key(
    change_id,
    blueprint_fingerprint,
    resolved_schema_version,
    manifest_fingerprint="",
    source_blueprint_fingerprint="",
    handoff_fingerprint="",
):
    return "{}|{}|{}|{}|{}|{}".format(
        str(change_id or "").strip(),
        str(blueprint_fingerprint or "").strip().lower(),
        str(resolved_schema_version or "").strip(),
        str(manifest_fingerprint or "").strip().lower(),
        str(source_blueprint_fingerprint or "").strip().lower(),
        str(handoff_fingerprint or "").strip().lower(),
    )


def _find_run_by_idempotency_key(store_payload, idempotency_key):
    runs = (store_payload or {}).get("runs", {})
    if not isinstance(runs, dict):
        return None

    for run_state in runs.values():
        if not isinstance(run_state, dict):
            continue
        if run_state.get("idempotency_key", "") == idempotency_key:
            return run_state

    return None


def _sanitize_official_events(run_state):
    event_rows = run_state.get("events", [])
    if not isinstance(event_rows, list):
        run_state["events"] = []
        return

    sanitized_rows = []
    removed_local_events = 0
    for event in event_rows:
        if not isinstance(event, dict):
            continue
        event_code = str(event.get("code", "")).lower()
        if event_code.endswith("_local") or "_local" in event_code:
            removed_local_events += 1
            continue
        normalized_event = _normalize_event_row(event, run_state)
        if normalized_event:
            sanitized_rows.append(normalized_event)

    if removed_local_events > 0:
        sanitized_event = _normalize_event_row(
            {
                "id": "evt-{}".format(uuid.uuid4().hex[:12]),
                "timestamp_utc": _utc_now(),
                "level": "warning",
                "code": "runbook_official_timeline_sanitized",
                "message": "Eventos simulados locais removidos da timeline oficial.",
                "run_id": run_state.get("run_id", ""),
                "change_id": run_state.get("change_id", ""),
                "stage": run_state.get("current_stage", ""),
                "checkpoint": run_state.get("current_checkpoint", ""),
                "host_ref": "",
                "org_id": "",
                "fingerprint_sha256": "",
            },
            run_state,
        )
        if sanitized_event:
            sanitized_rows.append(sanitized_event)

    stage_order_map = _build_stage_order_map(run_state)
    sanitized_rows.sort(key=lambda row: _event_sort_key(row, stage_order_map))

    run_state["events"] = sanitized_rows


def _build_run_snapshot(run_state):
    stage_rows = run_state.get("stages", [])
    snapshot_stage_rows = []
    for stage in stage_rows:
        checkpoint_rows = stage.get("checkpoints", [])
        checkpoint_total = len(checkpoint_rows)
        checkpoint_completed = len(
            [
                checkpoint
                for checkpoint in checkpoint_rows
                if checkpoint.get("status") == "completed"
            ]
        )
        snapshot_stage_rows.append(
            {
                "stage_key": stage.get("key", ""),
                "stage_label": stage.get("label", ""),
                "status": stage.get("status", "pending"),
                "checkpoints_total": checkpoint_total,
                "checkpoints_completed": checkpoint_completed,
            }
        )

    return {
        "contract_version": RUNBOOK_CONTRACT_VERSION,
        "source_mode": "official",
        "run_id": run_state.get("run_id", ""),
        "change_id": run_state.get("change_id", ""),
        "manifest_fingerprint": run_state.get("manifest_fingerprint", ""),
        "source_blueprint_fingerprint": run_state.get(
            "source_blueprint_fingerprint", ""
        ),
        "handoff_fingerprint": run_state.get("handoff_fingerprint", ""),
        "a2a_entry_gate": run_state.get("a2a_entry_gate", {}),
        "runtime_telemetry": run_state.get("runtime_telemetry", {}),
        "organization_read_model": run_state.get("organization_read_model", {}),
        "runtime_inspection_cache": run_state.get("runtime_inspection_cache", {}),
        "runtime_inspection_trail": run_state.get("runtime_inspection_trail", []),
        "pipeline_status": run_state.get("status", "pending"),
        "current_stage": run_state.get("current_stage", ""),
        "current_checkpoint": run_state.get("current_checkpoint", ""),
        "official_decision": run_state.get("official_decision", {}),
        "stage_statuses": snapshot_stage_rows,
        "event_total": len(run_state.get("events", [])),
        "last_updated_at_utc": run_state.get("updated_at_utc", ""),
    }


def _attach_run_snapshot(run_state, store_payload=None):
    _sanitize_official_events(run_state)
    run_state["contract_version"] = RUNBOOK_CONTRACT_VERSION
    run_state["source_mode"] = "official"
    run_state["backend_state"] = "ready"
    effective_store_payload = store_payload if isinstance(store_payload, dict) else _load_store()
    run_state["a2a_entry_gate"] = _resolve_a2a_entry_gate(effective_store_payload, run_state)
    run_state["runtime_telemetry"] = _build_runtime_telemetry_contract(run_state)
    run_state["organization_read_model"] = _build_organization_read_model_contract(
        effective_store_payload, run_state
    )
    run_state["snapshot"] = _build_run_snapshot(run_state)


def _build_started_run(validated, actor_context=None):
    run_id = validated.get("run_id") or "run-{}".format(uuid.uuid4().hex[:12])
    stage_rows = _build_runbook_stages()
    now_utc = _utc_now()
    safe_actor_context = actor_context if isinstance(actor_context, dict) else {}

    run_state = {
        "run_id": run_id,
        "idempotency_key": _runbook_idempotency_key(
            validated.get("change_id", ""),
            validated.get("blueprint_fingerprint", ""),
            validated.get("resolved_schema_version", ""),
            validated.get("manifest_fingerprint", ""),
            validated.get("source_blueprint_fingerprint", ""),
            validated.get("handoff_fingerprint", ""),
        ),
        "change_id": validated.get("change_id", ""),
        "provider_key": validated.get("provider_key", ""),
        "environment_profile": validated.get("environment_profile", ""),
        "blueprint_version": validated.get("blueprint_version", ""),
        "blueprint_fingerprint": validated.get("blueprint_fingerprint", ""),
        "manifest_fingerprint": validated.get("manifest_fingerprint", ""),
        "source_blueprint_fingerprint": validated.get(
            "source_blueprint_fingerprint", ""
        ),
        "resolved_schema_version": validated.get("resolved_schema_version", ""),
        "pipeline_preconditions_ready": bool(
            validated.get("pipeline_preconditions_ready", True)
        ),
        "blueprint_validated": bool(validated.get("blueprint_validated", True)),
        "preflight_approved": bool(validated.get("preflight_approved", True)),
        "a2_2_minimum_artifacts": validated.get("a2_2_minimum_artifacts", []),
        "a2_2_available_artifacts": validated.get("a2_2_available_artifacts", []),
        "enabled_runtime_node_types": list(RUNBOOK_ENABLED_NODE_TYPES),
        "host_mapping": _enrich_runtime_host_mapping(
            _filter_enabled_runtime_host_mapping(
                _sanitize_host_mapping_collection(validated.get("host_mapping", []))
            )
        ),
        "machine_credentials": _sanitize_machine_credentials_collection(
            validated.get("machine_credentials", [])
        ),
        "api_registry": validated.get("api_registry", []),
        "incremental_expansions": validated.get("incremental_expansions", []),
        "topology_catalog": validated.get("topology_catalog", {}),
        "handoff_contract_version": validated.get("handoff_contract_version", ""),
        "handoff_fingerprint": validated.get("handoff_fingerprint", ""),
        "handoff_payload": validated.get("handoff_payload", {}),
        "a2_3_handoff": validated.get("a2_3_handoff", {}),
        "a2_3_readiness_checklist": validated.get("a2_3_readiness_checklist", {}),
        "a2_4_handoff": validated.get("a2_4_handoff", {}),
        "a2_4_readiness_checklist": validated.get("a2_4_readiness_checklist", {}),
        "handoff_trace": validated.get("handoff_trace", []),
        "runbook_resume_context": validated.get("runbook_resume_context", {}),
        "actor_scope": {
            "username": str(safe_actor_context.get("username", "") or "").strip(),
            "email": str(safe_actor_context.get("email", "") or "").strip().lower(),
            "role": str(safe_actor_context.get("role", "user") or "user").strip().lower(),
            "organization_id": str(safe_actor_context.get("organization_id", "") or "").strip(),
            "organization_name": str(
                safe_actor_context.get("organization_name", "") or ""
            ).strip().lower(),
        },
        "status": "running",
        "created_at_utc": now_utc,
        "started_at_utc": now_utc,
        "finished_at_utc": "",
        "updated_at_utc": now_utc,
        "current_stage_index": 0,
        "current_checkpoint_index": 0,
        "current_stage": "",
        "current_checkpoint": "",
        "stages": stage_rows,
        "events": [],
        "host_stage_evidences": {},
        "host_inventory": [],
        "runtime_image_preseed_cache": {},
        "runtime_host_docker_ready_cache": {},
        "runtime_host_image_warmup_cache": {},
        "gateway_verify_scope_cache": {},
        "artifact_rows": [],
        "last_failure": None,
        "store_mode": "official",
        "store_version": RUNBOOK_STORE_SCHEMA_VERSION,
    }

    _activate_checkpoint(run_state, 0, 0)
    _append_event(
        run_state,
        "info",
        "runbook_started",
        "Execucao de runbook iniciada com backend oficial.",
    )
    _attach_run_snapshot(run_state)

    return run_state


def _ensure_run_state_precondition(run_state, expected_statuses, action_name):
    current_status = run_state.get("status", "")
    if current_status not in expected_statuses:
        return _build_runbook_error_payload(
            "runbook_invalid_transition",
            "Comando '{}' invalido para status '{}'.".format(
                action_name, current_status
            ),
            {
                "action": action_name,
                "current_status": current_status,
                "allowed_statuses": expected_statuses,
            },
        )
    return None


def _run_operation_pause(store_payload, run_state, executor):
    validation_error = _ensure_run_state_precondition(run_state, ["running"], "pause")
    if validation_error:
        return validation_error

    stage = _get_stage(run_state, run_state.get("current_stage_index", -1))
    checkpoint = _get_checkpoint(stage, run_state.get("current_checkpoint_index", -1))
    if not stage or not checkpoint or checkpoint.get("status") != "running":
        return _build_runbook_error_payload(
            "runbook_pause_checkpoint_not_running",
            "Nao existe checkpoint em execucao para pausar.",
        )

    checkpoint["status"] = "paused"
    stage["status"] = "paused"
    run_state["status"] = "paused"
    run_state["updated_at_utc"] = _utc_now()
    _upsert_run_checkpoint(
        store_payload,
        run_state,
        stage.get("key", ""),
        checkpoint.get("key", ""),
        "paused",
        executor,
        {"action": "pause", "run_id": run_state.get("run_id", "")},
        {
            "status": run_state.get("status", ""),
            "stage": run_state.get("current_stage", ""),
            "checkpoint": run_state.get("current_checkpoint", ""),
        },
    )
    _version_run_artifacts(store_payload, run_state, executor)
    _append_event(run_state, "warning", "runbook_paused", "Execucao pausada por comando.")
    _attach_run_snapshot(run_state, store_payload)
    return None


def _run_operation_resume(store_payload, run_state, executor):
    validation_error = _ensure_run_state_precondition(run_state, ["paused"], "resume")
    if validation_error:
        return validation_error

    stage = _get_stage(run_state, run_state.get("current_stage_index", -1))
    checkpoint = _get_checkpoint(stage, run_state.get("current_checkpoint_index", -1))
    if not stage or not checkpoint or checkpoint.get("status") != "paused":
        return _build_runbook_error_payload(
            "runbook_resume_checkpoint_not_paused",
            "Nao existe checkpoint pausado para retomar.",
        )

    checkpoint["status"] = "running"
    stage["status"] = "running"
    run_state["status"] = "running"
    run_state["updated_at_utc"] = _utc_now()
    _upsert_run_checkpoint(
        store_payload,
        run_state,
        stage.get("key", ""),
        checkpoint.get("key", ""),
        "running",
        executor,
        {"action": "resume", "run_id": run_state.get("run_id", "")},
        {
            "status": run_state.get("status", ""),
            "stage": run_state.get("current_stage", ""),
            "checkpoint": run_state.get("current_checkpoint", ""),
        },
    )
    _version_run_artifacts(store_payload, run_state, executor)
    _append_event(run_state, "info", "runbook_resumed", "Execucao retomada.")
    _attach_run_snapshot(run_state, store_payload)
    return None


def _run_operation_advance(store_payload, run_state, executor):
    validation_error = _ensure_run_state_precondition(run_state, ["running"], "advance")
    if validation_error:
        return validation_error

    stage_index = run_state.get("current_stage_index", -1)
    checkpoint_index = run_state.get("current_checkpoint_index", -1)
    stage = _get_stage(run_state, stage_index)
    checkpoint = _get_checkpoint(stage, checkpoint_index)
    if not stage or not checkpoint or checkpoint.get("status") != "running":
        return _build_runbook_error_payload(
            "runbook_advance_checkpoint_not_running",
            "Nao existe checkpoint em execucao para avancar.",
        )

    if not bool(run_state.get("preflight_approved", True)):
        return _build_runbook_error_payload(
            "runbook_preflight_not_approved",
            "Preflight tecnico nao aprovado para avancar no runbook.",
            {
                "run_id": run_state.get("run_id", ""),
                "stage": stage.get("key", ""),
                "checkpoint": checkpoint.get("key", ""),
            },
        )

    stage_key = stage.get("key", "")
    checkpoint_key = checkpoint.get("key", "")
    lock_error = _acquire_stage_resource_locks(store_payload, run_state, stage_key)
    if lock_error:
        return lock_error

    host_evidences = []
    execution_error = None
    try:
        host_evidences, execution_error = _execute_ssh_checkpoint_actions(
            run_state,
            stage_key,
            checkpoint_key,
        )
    except subprocess.TimeoutExpired as timeout_exc:
        timeout_seconds = _resolve_checkpoint_ssh_timeout(stage_key, checkpoint_key)
        try:
            timeout_seconds = int(getattr(timeout_exc, "timeout", timeout_seconds) or timeout_seconds)
        except (TypeError, ValueError):
            timeout_seconds = _resolve_checkpoint_ssh_timeout(stage_key, checkpoint_key)
        execution_error = _build_runbook_error_payload(
            "runbook_ssh_timeout",
            "Execucao SSH excedeu timeout configurado para checkpoint.",
            {
                "run_id": run_state.get("run_id", ""),
                "stage": stage_key,
                "checkpoint": checkpoint_key,
                "timeout_seconds": timeout_seconds,
            },
        )
    except Exception as exc:
        execution_error = _build_runbook_error_payload(
            "runbook_ssh_executor_unavailable",
            "Falha inesperada no executor SSH do orchestrator.",
            {
                "run_id": run_state.get("run_id", ""),
                "stage": stage_key,
                "checkpoint": checkpoint_key,
                "reason": str(exc),
            },
        )
    finally:
        _release_stage_resource_locks(store_payload, run_state, stage_key)

    _append_stage_host_evidence(run_state, stage_key, checkpoint_key, host_evidences)
    if execution_error:
        host_evidence_count = len(host_evidences or [])
        failure_code = execution_error.get("code", "runbook_ssh_execution_failed")
        failure_message = execution_error.get(
            "message", "Execucao SSH falhou na etapa operacional."
        )
        if failure_code == "runbook_ssh_executor_unavailable":
            execution_details = execution_error.get("details", {})
            if not isinstance(execution_details, dict):
                execution_details = {}
            raw_reason = str(execution_details.get("reason", "") or "").strip()
            if raw_reason:
                if len(raw_reason) > 256:
                    raw_reason = "{}...".format(raw_reason[:253])
                failure_message = "{} [{}]".format(failure_message, raw_reason)

        checkpoint["status"] = "failed"
        stage["status"] = "failed"
        run_state["status"] = "failed"
        run_state["updated_at_utc"] = _utc_now()
        run_state["last_failure"] = {
            "code": failure_code,
            "message": failure_message,
            "timestamp_utc": _utc_now(),
            "stage": stage_key,
            "checkpoint": checkpoint_key,
        }
        _append_event(
            run_state,
            "error",
            run_state["last_failure"]["code"],
            run_state["last_failure"]["message"],
        )
        _upsert_run_checkpoint(
            store_payload,
            run_state,
            stage_key,
            checkpoint_key,
            "failed",
            executor,
            {
                "action": "advance",
                "run_id": run_state.get("run_id", ""),
                "stage": stage_key,
                "checkpoint": checkpoint_key,
            },
            {
                "status": "failed",
                "failure": run_state["last_failure"],
                "host_evidence_count": host_evidence_count,
            },
        )
        _version_run_artifacts(store_payload, run_state, executor)
        _attach_run_snapshot(run_state, store_payload)
        return execution_error

    checkpoint["status"] = "completed"
    checkpoint["completed_at_utc"] = _utc_now()
    _upsert_run_checkpoint(
        store_payload,
        run_state,
        stage.get("key", ""),
        checkpoint.get("key", ""),
        "completed",
        executor,
        {
            "action": "advance",
            "run_id": run_state.get("run_id", ""),
            "stage": stage.get("key", ""),
            "checkpoint": checkpoint.get("key", ""),
        },
        {
            "status": checkpoint.get("status", ""),
            "completed_at_utc": checkpoint.get("completed_at_utc", ""),
        },
    )

    next_stage_index, next_checkpoint_index = _find_next_pending_checkpoint(
        run_state, stage_index, checkpoint_index + 1
    )
    if next_stage_index is None:
        run_state["status"] = "completed"
        run_state["finished_at_utc"] = _utc_now()
        run_state["updated_at_utc"] = _utc_now()
        _release_stage_resource_locks(store_payload, run_state)
        _recompute_stage_statuses(run_state)
        _version_run_artifacts(store_payload, run_state, executor)
        verify_error = _validate_verify_artifact_consistency(store_payload, run_state)
        if verify_error:
            run_state["status"] = "failed"
            run_state["last_failure"] = {
                "code": verify_error.get("code", "runbook_verify_artifact_inconsistent"),
                "message": verify_error.get(
                    "message",
                    "Verify reprovado por inconsistencias entre estado e artefatos.",
                ),
                "timestamp_utc": _utc_now(),
                "stage": stage.get("key", ""),
                "checkpoint": checkpoint.get("key", ""),
            }
            _append_event(
                run_state,
                "error",
                run_state["last_failure"]["code"],
                run_state["last_failure"]["message"],
            )
            _version_run_artifacts(store_payload, run_state, executor)
            _attach_run_snapshot(run_state, store_payload)
            return verify_error
        _append_event(
            run_state,
            "info",
            "runbook_completed",
            "Execucao concluida com sucesso no backend.",
        )
        _attach_run_snapshot(run_state, store_payload)
        return None

    _activate_checkpoint(run_state, next_stage_index, next_checkpoint_index)
    next_stage = _get_stage(run_state, next_stage_index)
    next_checkpoint = _get_checkpoint(next_stage, next_checkpoint_index)
    _upsert_run_checkpoint(
        store_payload,
        run_state,
        next_stage.get("key", "") if next_stage else "",
        next_checkpoint.get("key", "") if next_checkpoint else "",
        "running",
        executor,
        {
            "action": "advance",
            "run_id": run_state.get("run_id", ""),
            "next_stage": run_state.get("current_stage", ""),
            "next_checkpoint": run_state.get("current_checkpoint", ""),
        },
        {
            "status": "running",
            "started_at_utc": next_checkpoint.get("started_at_utc", "")
            if next_checkpoint
            else "",
        },
    )
    _recompute_stage_statuses(run_state)
    _version_run_artifacts(store_payload, run_state, executor)
    _append_event(run_state, "info", "runbook_advanced", "Checkpoint avancado com sucesso.")
    _attach_run_snapshot(run_state, store_payload)
    return None


def _run_operation_fail(store_payload, run_state, failure_code, failure_message, executor):
    validation_error = _ensure_run_state_precondition(run_state, ["running", "paused"], "fail")
    if validation_error:
        return validation_error

    stage = _get_stage(run_state, run_state.get("current_stage_index", -1))
    checkpoint = _get_checkpoint(stage, run_state.get("current_checkpoint_index", -1))
    if not stage or not checkpoint or checkpoint.get("status") not in ["running", "paused"]:
        return _build_runbook_error_payload(
            "runbook_fail_checkpoint_not_active",
            "Nao existe checkpoint ativo para marcar falha.",
        )

    checkpoint["status"] = "failed"
    stage["status"] = "failed"
    run_state["status"] = "failed"
    run_state["updated_at_utc"] = _utc_now()
    _release_stage_resource_locks(store_payload, run_state)
    run_state["last_failure"] = {
        "code": failure_code or "runbook_failure",
        "message": failure_message or "Falha operacional registrada no backend.",
        "timestamp_utc": _utc_now(),
        "stage": stage.get("key", ""),
        "checkpoint": checkpoint.get("key", ""),
    }
    _append_event(
        run_state,
        "error",
        run_state["last_failure"]["code"],
        run_state["last_failure"]["message"],
    )
    _upsert_run_checkpoint(
        store_payload,
        run_state,
        stage.get("key", ""),
        checkpoint.get("key", ""),
        "failed",
        executor,
        {
            "action": "fail",
            "run_id": run_state.get("run_id", ""),
            "failure_code": run_state["last_failure"]["code"],
        },
        run_state["last_failure"],
    )
    _version_run_artifacts(store_payload, run_state, executor)
    _attach_run_snapshot(run_state, store_payload)
    return None


def _run_operation_retry(store_payload, run_state, executor):
    validation_error = _ensure_run_state_precondition(
        run_state, ["failed", "paused"], "retry"
    )
    if validation_error:
        return validation_error

    stage_rows = run_state.get("stages", [])
    for stage in stage_rows:
        for checkpoint in stage.get("checkpoints", []):
            if checkpoint.get("status") in ["failed", "paused", "running"]:
                checkpoint["status"] = "pending"
                checkpoint["started_at_utc"] = ""
                checkpoint["completed_at_utc"] = ""

    next_stage_index, next_checkpoint_index = _find_next_pending_checkpoint(run_state, 0, 0)
    if next_stage_index is None:
        return _build_runbook_error_payload(
            "runbook_retry_no_pending_checkpoint",
            "Nao existe checkpoint pendente para reexecucao.",
        )

    _activate_checkpoint(run_state, next_stage_index, next_checkpoint_index)
    run_state["status"] = "running"
    _release_stage_resource_locks(store_payload, run_state)
    run_state["runtime_image_preseed_cache"] = {}
    run_state["runtime_host_docker_ready_cache"] = {}
    run_state["runtime_host_image_warmup_cache"] = {}
    run_state["gateway_verify_scope_cache"] = {}
    run_state["last_failure"] = None
    _recompute_stage_statuses(run_state)
    current_stage = _get_stage(run_state, run_state.get("current_stage_index", -1))
    current_checkpoint = _get_checkpoint(
        current_stage, run_state.get("current_checkpoint_index", -1)
    )
    _upsert_run_checkpoint(
        store_payload,
        run_state,
        current_stage.get("key", "") if current_stage else "",
        current_checkpoint.get("key", "") if current_checkpoint else "",
        "running",
        executor,
        {
            "action": "retry",
            "run_id": run_state.get("run_id", ""),
        },
        {
            "status": run_state.get("status", ""),
            "stage": run_state.get("current_stage", ""),
            "checkpoint": run_state.get("current_checkpoint", ""),
        },
    )
    _version_run_artifacts(store_payload, run_state, executor)
    _append_event(
        run_state,
        "warning",
        "runbook_retry",
        "Reexecucao segura iniciada a partir do ultimo checkpoint valido.",
    )
    _attach_run_snapshot(run_state, store_payload)
    return None


class RunbookViewSet(viewsets.ViewSet):
    permission_classes = [
        IsAuthenticated,
    ]

    @swagger_auto_schema(
        methods=["post"],
        request_body=RunbookPreflightRequestSerializer,
        responses=with_common_response({status.HTTP_200_OK: RunbookPreflightEnvelopeSerializer}),
    )
    @action(detail=False, methods=["post"], url_path="preflight")
    def preflight(self, request):
        serializer = RunbookPreflightRequestSerializer(data=request.data)

        try:
            if serializer.is_valid(raise_exception=True):
                validated = serializer.validated_data
                actor_context = _resolve_actor_context(request)
                store_payload = _load_store()

                if not _is_actor_authorized("preflight", actor_context.get("role", "user")):
                    _append_access_audit(
                        store_payload,
                        "preflight",
                        actor_context,
                        False,
                        "runbook_access_forbidden",
                        "Papel sem permissao para executar preflight técnico oficial.",
                        run_id="",
                        change_id=validated.get("change_id", ""),
                        provider_key=validated.get("provider_key", ""),
                    )
                    _save_store(store_payload)
                    return _error_response(
                        "runbook_access_forbidden",
                        "Autorizacao insuficiente para executar preflight técnico oficial.",
                        http_status=status.HTTP_403_FORBIDDEN,
                        details={
                            "required_roles": sorted(
                                list(RUNBOOK_ALLOWED_ROLES_BY_ACTION.get("preflight", set()))
                            ),
                            "actor_role": actor_context.get("role", "user"),
                        },
                    )

                provider_key = str(validated.get("provider_key", "") or "").strip()
                if provider_key and provider_key != RUNBOOK_PROVIDER_KEY:
                    return _error_response(
                        "runbook_provider_not_supported",
                        "Provider invalido para preflight técnico. Use external-linux.",
                    )

                host_mapping = validated.get("host_mapping", [])
                host_mapping_error = _validate_host_mapping_security(host_mapping)
                if host_mapping_error:
                    return _error_response(
                        host_mapping_error.get("code", "runbook_host_mapping_invalid"),
                        host_mapping_error.get(
                            "message",
                            "Host mapping invalido para preflight técnico oficial.",
                        ),
                        details=host_mapping_error.get("details", {}),
                    )

                machine_credentials = validated.get("machine_credentials", [])
                machine_credentials_error = _validate_machine_credentials_security(
                    host_mapping,
                    machine_credentials,
                )
                if machine_credentials_error:
                    return _error_response(
                        machine_credentials_error.get(
                            "code", "runbook_machine_credentials_invalid"
                        ),
                        machine_credentials_error.get(
                            "message",
                            "machine_credentials invalido para preflight tecnico oficial.",
                        ),
                        details=machine_credentials_error.get("details", {}),
                    )

                preflight_report = _build_ssh_preflight_report(
                    validated.get("change_id", ""),
                    host_mapping,
                    machine_credentials,
                )

                response = RunbookPreflightEnvelopeSerializer(
                    data={"preflight": preflight_report}
                )
                if response.is_valid(raise_exception=True):
                    _append_access_audit(
                        store_payload,
                        "preflight",
                        actor_context,
                        True,
                        "runbook_access_allowed",
                        "Preflight técnico oficial executado com probe SSH dinâmico.",
                        run_id="",
                        change_id=validated.get("change_id", ""),
                        provider_key=RUNBOOK_PROVIDER_KEY,
                    )
                    _save_store(store_payload)
                    return Response(ok(response.validated_data), status=status.HTTP_200_OK)

        except Exception as exc:
            LOG.exception("Runbook preflight endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["post"],
        request_body=RunbookStartRequestSerializer,
        responses=with_common_response({status.HTTP_201_CREATED: RunbookStateEnvelopeSerializer}),
    )
    @action(detail=False, methods=["post"], url_path="start")
    def start(self, request):
        serializer = RunbookStartRequestSerializer(data=request.data)

        try:
            if serializer.is_valid(raise_exception=True):
                validated = serializer.validated_data
                actor_context = _resolve_actor_context(request)
                store_payload = _load_store()

                if not _is_actor_authorized("start", actor_context.get("role", "user")):
                    _append_access_audit(
                        store_payload,
                        "start",
                        actor_context,
                        False,
                        "runbook_access_forbidden",
                        "Papel sem permissao para iniciar runbook oficial.",
                        run_id=validated.get("run_id", ""),
                        change_id=validated.get("change_id", ""),
                        provider_key=validated.get("provider_key", ""),
                    )
                    _save_store(store_payload)
                    return _error_response(
                        "runbook_access_forbidden",
                        "Autorizacao insuficiente para start oficial do runbook.",
                        http_status=status.HTTP_403_FORBIDDEN,
                        details={
                            "required_roles": sorted(
                                list(RUNBOOK_ALLOWED_ROLES_BY_ACTION.get("start", set()))
                            ),
                            "actor_role": actor_context.get("role", "user"),
                        },
                    )

                if validated.get("provider_key") != RUNBOOK_PROVIDER_KEY:
                    _append_access_audit(
                        store_payload,
                        "start",
                        actor_context,
                        False,
                        "runbook_scope_invalid",
                        "Escopo operacional invalido para start.",
                        run_id=validated.get("run_id", ""),
                        change_id=validated.get("change_id", ""),
                        provider_key=validated.get("provider_key", ""),
                    )
                    _save_store(store_payload)
                    return _error_response(
                        "runbook_provider_not_supported",
                        "Provider invalido para E1. Use external-linux.",
                    )
                if not _is_valid_change_context(validated.get("change_id", "")):
                    _append_access_audit(
                        store_payload,
                        "start",
                        actor_context,
                        False,
                        "runbook_change_context_required",
                        "change_id ausente em start de comando critico.",
                        run_id=validated.get("run_id", ""),
                        change_id=validated.get("change_id", ""),
                        provider_key=validated.get("provider_key", ""),
                    )
                    _save_store(store_payload)
                    return _error_response(
                        "runbook_change_context_required",
                        "change_id obrigatorio para iniciar runbook operacional.",
                    )
                if not validated.get("blueprint_validated", True):
                    return _error_response(
                        "runbook_blueprint_not_validated",
                        "Blueprint nao validado para execucao.",
                    )
                if not validated.get("pipeline_preconditions_ready", True):
                    return _error_response(
                        "runbook_pipeline_preconditions_missing",
                        "Pre-condicoes de pipeline ainda nao atendidas.",
                    )
                if not validated.get("preflight_approved", True):
                    return _error_response(
                        "runbook_preflight_not_approved",
                        "Preflight tecnico nao aprovado para execucao operacional.",
                    )

                request_payload = request.data
                request_host_mapping = []
                request_machine_credentials = []
                if isinstance(request_payload, dict):
                    request_host_mapping = request_payload.get("host_mapping", [])
                    request_machine_credentials = request_payload.get(
                        "machine_credentials", []
                    )
                host_mapping_error = _validate_host_mapping_security(
                    request_host_mapping
                )
                if host_mapping_error:
                    return _error_response(
                        host_mapping_error.get("code", "runbook_host_mapping_invalid"),
                        host_mapping_error.get(
                            "message",
                            "Host mapping invalido para execucao operacional.",
                        ),
                        details=host_mapping_error.get("details", {}),
                    )
                normalized_request_host_mapping = _sanitize_host_mapping_collection(
                    request_host_mapping
                )
                normalized_request_host_mapping = _filter_enabled_runtime_host_mapping(
                    normalized_request_host_mapping
                )
                normalized_request_host_mapping = _autoexpand_runtime_host_mapping(
                    normalized_request_host_mapping,
                    validated.get("topology_catalog", {}),
                )
                if len(normalized_request_host_mapping) == 0:
                    return _error_response(
                        "runbook_host_mapping_no_enabled_components",
                        "Host mapping sem componentes habilitados para provisionamento.",
                        details={
                            "enabled_node_types": list(RUNBOOK_ENABLED_NODE_TYPES),
                            "requested_host_rows": len(request_host_mapping),
                        },
                    )

                machine_credentials_error = _validate_machine_credentials_security(
                    normalized_request_host_mapping,
                    request_machine_credentials,
                )
                if machine_credentials_error:
                    return _error_response(
                        machine_credentials_error.get(
                            "code", "runbook_machine_credentials_invalid"
                        ),
                        machine_credentials_error.get(
                            "message",
                            "machine_credentials invalido para execucao operacional.",
                        ),
                        details=machine_credentials_error.get("details", {}),
                    )

                topology_runtime_mapping_error = (
                    _validate_topology_runtime_host_mapping_coverage(
                        validated.get("topology_catalog", {}),
                        normalized_request_host_mapping,
                    )
                )
                if topology_runtime_mapping_error:
                    return _error_response(
                        topology_runtime_mapping_error.get(
                            "code", "runbook_topology_runtime_mapping_incomplete"
                        ),
                        topology_runtime_mapping_error.get(
                            "message",
                            "Host mapping incompleto para topologia runtime declarada.",
                        ),
                        details=topology_runtime_mapping_error.get("details", {}),
                    )

                enriched_request_host_mapping = _enrich_runtime_host_mapping(
                    normalized_request_host_mapping
                )
                runtime_image_catalog_error = _validate_runtime_image_catalog_alignment(
                    enriched_request_host_mapping
                )
                if runtime_image_catalog_error:
                    return _error_response(
                        runtime_image_catalog_error.get(
                            "code", "runbook_runtime_image_catalog_violation"
                        ),
                        runtime_image_catalog_error.get(
                            "message",
                            "Catalogo de imagens invalido para runtime oficial.",
                        ),
                        details=runtime_image_catalog_error.get("details", {}),
                    )

                validated["host_mapping"] = enriched_request_host_mapping
                run_state = _build_started_run(validated, actor_context)

                run_id = run_state["run_id"]
                existing_run_by_id = store_payload.get("runs", {}).get(run_id)
                if existing_run_by_id:
                    if existing_run_by_id.get("idempotency_key", "") == run_state.get(
                        "idempotency_key", ""
                    ):
                        _attach_run_snapshot(existing_run_by_id, store_payload)
                        response = RunbookStateEnvelopeSerializer(
                            data={
                                "run": existing_run_by_id,
                                "snapshot": existing_run_by_id.get("snapshot", {}),
                            }
                        )
                        if response.is_valid(raise_exception=True):
                            return Response(
                                ok(response.validated_data), status=status.HTTP_200_OK
                            )

                    return _error_response(
                        "runbook_run_id_conflict",
                        "run_id ja existe com contexto de execucao diferente.",
                        http_status=status.HTTP_409_CONFLICT,
                    )

                existing_run_by_key = _find_run_by_idempotency_key(
                    store_payload, run_state.get("idempotency_key", "")
                )
                if existing_run_by_key:
                    _attach_run_snapshot(existing_run_by_key, store_payload)
                    response = RunbookStateEnvelopeSerializer(
                        data={
                            "run": existing_run_by_key,
                            "snapshot": existing_run_by_key.get("snapshot", {}),
                        }
                    )
                    if response.is_valid(raise_exception=True):
                        return Response(ok(response.validated_data), status=status.HTTP_200_OK)

                chaincode_binding_issues = _collect_runbook_chaincode_binding_issues(
                    run_state
                )
                if chaincode_binding_issues:
                    return _error_response(
                        "runbook_chaincode_binding_invalid",
                        "Cada chaincode operacional deve declarar artifact_ref ou source_ref explicito e acessivel pelo orchestrator.",
                        details={
                            "issues": chaincode_binding_issues,
                            "run_id": run_state.get("run_id", ""),
                            "change_id": run_state.get("change_id", ""),
                        },
                    )

                store_payload["runs"][run_state["run_id"]] = run_state
                _upsert_run_checkpoint(
                    store_payload,
                    run_state,
                    run_state.get("current_stage", ""),
                    run_state.get("current_checkpoint", ""),
                    "running",
                    actor_context.get("username", "system"),
                    {
                        "action": "start",
                        "run_id": run_state.get("run_id", ""),
                        "change_id": run_state.get("change_id", ""),
                    },
                    {
                        "status": run_state.get("status", ""),
                        "stage": run_state.get("current_stage", ""),
                        "checkpoint": run_state.get("current_checkpoint", ""),
                    },
                )
                _version_run_artifacts(
                    store_payload,
                    run_state,
                    actor_context.get("username", "system"),
                )
                _attach_run_snapshot(run_state, store_payload)
                _append_event(
                    run_state,
                    "info",
                    "runbook_access_granted",
                    "Acesso autorizado para start oficial do runbook.",
                    actor_user=actor_context.get("username", "system"),
                    actor_role=actor_context.get("role", "user"),
                )
                _append_access_audit(
                    store_payload,
                    "start",
                    actor_context,
                    True,
                    "runbook_access_allowed",
                    "Start autorizado para contexto operacional valido.",
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)

                response = RunbookStateEnvelopeSerializer(
                    data={"run": run_state, "snapshot": run_state.get("snapshot", {})}
                )
                if response.is_valid(raise_exception=True):
                    return Response(ok(response.validated_data), status=status.HTTP_201_CREATED)

        except Exception as exc:
            LOG.exception("Runbook start endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["post"],
        request_body=RunbookOperateRequestSerializer,
        responses=with_common_response({status.HTTP_200_OK: RunbookStateEnvelopeSerializer}),
    )
    @action(detail=False, methods=["post"], url_path="operate")
    def operate(self, request):
        serializer = RunbookOperateRequestSerializer(data=request.data)

        try:
            if serializer.is_valid(raise_exception=True):
                validated = serializer.validated_data
                run_id = validated.get("run_id")
                operation = validated.get("action")
                actor_context = _resolve_actor_context(request)
                executor = actor_context.get("username", "system")

                store_payload = _load_store()
                run_state = store_payload.get("runs", {}).get(run_id)
                if not run_state:
                    return _error_response(
                        "runbook_not_found",
                        "Runbook nao encontrado para o run_id informado.",
                        http_status=status.HTTP_404_NOT_FOUND,
                    )

                if not _is_actor_authorized("operate", actor_context.get("role", "user")):
                    _append_access_audit(
                        store_payload,
                        "operate:{}".format(operation),
                        actor_context,
                        False,
                        "runbook_access_forbidden",
                        "Papel sem permissao para operar runbook oficial.",
                        run_id=run_state.get("run_id", ""),
                        change_id=run_state.get("change_id", ""),
                        provider_key=run_state.get("provider_key", ""),
                    )
                    _append_event(
                        run_state,
                        "error",
                        "runbook_access_forbidden",
                        "Acesso negado para comando operacional no runbook.",
                        actor_user=actor_context.get("username", "system"),
                        actor_role=actor_context.get("role", "user"),
                    )
                    store_payload["runs"][run_id] = run_state
                    _save_store(store_payload)
                    return _error_response(
                        "runbook_access_forbidden",
                        "Autorizacao insuficiente para operar o runbook oficial.",
                        http_status=status.HTTP_403_FORBIDDEN,
                        details={
                            "required_roles": sorted(
                                list(RUNBOOK_ALLOWED_ROLES_BY_ACTION.get("operate", set()))
                            ),
                            "actor_role": actor_context.get("role", "user"),
                            "action": operation,
                            "run_id": run_state.get("run_id", ""),
                        },
                    )

                if not _run_contains_actor_scope(run_state, actor_context):
                    _append_access_audit(
                        store_payload,
                        "operate:{}".format(operation),
                        actor_context,
                        False,
                        "runbook_scope_forbidden",
                        "Escopo organizacional sem permissao no runbook.",
                        run_id=run_state.get("run_id", ""),
                        change_id=run_state.get("change_id", ""),
                        provider_key=run_state.get("provider_key", ""),
                    )
                    _append_event(
                        run_state,
                        "error",
                        "runbook_scope_forbidden",
                        "Acesso negado por escopo organizacional no runbook.",
                        actor_user=actor_context.get("username", "system"),
                        actor_role=actor_context.get("role", "user"),
                    )
                    store_payload["runs"][run_id] = run_state
                    _save_store(store_payload)
                    return _error_response(
                        "runbook_scope_forbidden",
                        "Escopo organizacional invalido para operar este runbook.",
                        http_status=status.HTTP_403_FORBIDDEN,
                        details={
                            "actor_organization": actor_context.get("organization_name", ""),
                            "action": operation,
                            "run_id": run_state.get("run_id", ""),
                        },
                    )

                context_error = _validate_run_context_for_critical_commands(run_state)
                if context_error:
                    _append_access_audit(
                        store_payload,
                        "operate:{}".format(operation),
                        actor_context,
                        False,
                        context_error.get("code", "runbook_change_context_required"),
                        context_error.get("message", "Contexto operacional invalido."),
                        run_id=run_state.get("run_id", ""),
                        change_id=run_state.get("change_id", ""),
                        provider_key=run_state.get("provider_key", ""),
                    )
                    _append_event(
                        run_state,
                        "error",
                        context_error.get("code", "runbook_change_context_required"),
                        context_error.get("message", "Contexto operacional invalido."),
                        actor_user=actor_context.get("username", "system"),
                        actor_role=actor_context.get("role", "user"),
                    )
                    store_payload["runs"][run_id] = run_state
                    _save_store(store_payload)
                    return _error_response(
                        context_error.get("code", "runbook_change_context_required"),
                        context_error.get("message", "Contexto operacional invalido."),
                        http_status=status.HTTP_409_CONFLICT,
                        details={
                            "run_id": run_state.get("run_id", ""),
                            "action": operation,
                            **context_error.get("details", {}),
                        },
                    )

                run_state["host_mapping"] = _enrich_runtime_host_mapping(
                    _filter_enabled_runtime_host_mapping(
                        _sanitize_host_mapping_collection(
                            run_state.get("host_mapping", [])
                        )
                    )
                )
                run_state["host_mapping"] = _autoexpand_runtime_host_mapping(
                    run_state.get("host_mapping", []),
                    run_state.get("topology_catalog", {}),
                )
                run_state["host_mapping"] = _enrich_runtime_host_mapping(
                    run_state.get("host_mapping", [])
                )
                runtime_image_catalog_error = _validate_runtime_image_catalog_alignment(
                    run_state.get("host_mapping", [])
                )
                if runtime_image_catalog_error:
                    _append_event(
                        run_state,
                        "error",
                        runtime_image_catalog_error.get(
                            "code", "runbook_runtime_image_catalog_violation"
                        ),
                        runtime_image_catalog_error.get(
                            "message",
                            "Catalogo de imagens invalido para runtime oficial.",
                        ),
                        actor_user=actor_context.get("username", "system"),
                        actor_role=actor_context.get("role", "user"),
                    )
                    store_payload["runs"][run_id] = run_state
                    _save_store(store_payload)
                    return _error_response(
                        runtime_image_catalog_error.get(
                            "code", "runbook_runtime_image_catalog_violation"
                        ),
                        runtime_image_catalog_error.get(
                            "message",
                            "Catalogo de imagens invalido para runtime oficial.",
                        ),
                        details={
                            "run_id": run_state.get("run_id", ""),
                            "action": operation,
                            **runtime_image_catalog_error.get("details", {}),
                        },
                    )

                _append_access_audit(
                    store_payload,
                    "operate:{}".format(operation),
                    actor_context,
                    True,
                    "runbook_access_allowed",
                    "Operacao autorizada para contexto oficial.",
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _append_event(
                    run_state,
                    "info",
                    "runbook_access_granted",
                    "Acesso autorizado para operacao do runbook.",
                    actor_user=actor_context.get("username", "system"),
                    actor_role=actor_context.get("role", "user"),
                )

                operation_error = None
                if operation == "pause":
                    operation_error = _run_operation_pause(
                        store_payload, run_state, executor
                    )
                elif operation == "resume":
                    operation_error = _run_operation_resume(
                        store_payload, run_state, executor
                    )
                elif operation == "advance":
                    operation_error = _run_operation_advance(
                        store_payload, run_state, executor
                    )
                elif operation == "fail":
                    operation_error = _run_operation_fail(
                        store_payload,
                        run_state,
                        validated.get("failure_code", ""),
                        validated.get("failure_message", ""),
                        executor,
                    )
                elif operation == "retry":
                    operation_error = _run_operation_retry(
                        store_payload, run_state, executor
                    )

                if operation_error:
                    store_payload["runs"][run_id] = run_state
                    _save_store(store_payload)
                    error_details = operation_error.get("details", {})
                    if not isinstance(error_details, dict):
                        error_details = {}
                    error_details["run_id"] = run_state.get("run_id", "")
                    error_details["action"] = operation
                    return _error_response(
                        operation_error.get("code", "runbook_invalid_transition"),
                        operation_error.get("message", "Transicao invalida para o estado atual."),
                        http_status=status.HTTP_409_CONFLICT,
                        details=error_details,
                    )

                store_payload["runs"][run_id] = run_state
                _save_store(store_payload)

                response = RunbookStateEnvelopeSerializer(
                    data={"run": run_state, "snapshot": run_state.get("snapshot", {})}
                )
                if response.is_valid(raise_exception=True):
                    return Response(ok(response.validated_data), status=status.HTTP_200_OK)

        except Exception as exc:
            LOG.exception("Runbook operate endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["get"],
        responses=with_common_response({status.HTTP_200_OK: RunbookStateEnvelopeSerializer}),
    )
    @action(detail=True, methods=["get"], url_path="status")
    def status(self, request, pk=None):
        try:
            run_id = str(pk or "")
            store_payload = _load_store()
            run_state = store_payload.get("runs", {}).get(run_id)
            actor_context = _resolve_actor_context(request)
            if not run_state:
                return _error_response(
                    "runbook_not_found",
                    "Runbook nao encontrado para o run_id informado.",
                    http_status=status.HTTP_404_NOT_FOUND,
                )

            if not _is_actor_authorized("status", actor_context.get("role", "user")):
                _append_access_audit(
                    store_payload,
                    "status",
                    actor_context,
                    False,
                    "runbook_access_forbidden",
                    "Papel sem permissao para consultar status de runbook.",
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)
                return _error_response(
                    "runbook_access_forbidden",
                    "Autorizacao insuficiente para consultar status oficial do runbook.",
                    http_status=status.HTTP_403_FORBIDDEN,
                    details={
                        "required_roles": sorted(
                            list(RUNBOOK_ALLOWED_ROLES_BY_ACTION.get("status", set()))
                        ),
                        "actor_role": actor_context.get("role", "user"),
                        "run_id": run_state.get("run_id", ""),
                    },
                )

            if not _run_contains_actor_scope(run_state, actor_context):
                _append_access_audit(
                    store_payload,
                    "status",
                    actor_context,
                    False,
                    "runbook_scope_forbidden",
                    "Escopo organizacional sem permissao para status.",
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)
                return _error_response(
                    "runbook_scope_forbidden",
                    "Escopo organizacional invalido para consultar este runbook.",
                    http_status=status.HTTP_403_FORBIDDEN,
                    details={
                        "actor_organization": actor_context.get("organization_name", ""),
                        "run_id": run_state.get("run_id", ""),
                    },
                )

            context_error = _validate_run_context_for_critical_commands(run_state)
            if context_error:
                _append_access_audit(
                    store_payload,
                    "status",
                    actor_context,
                    False,
                    context_error.get("code", "runbook_change_context_required"),
                    context_error.get("message", "Contexto operacional invalido."),
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)
                return _error_response(
                    context_error.get("code", "runbook_change_context_required"),
                    context_error.get("message", "Contexto operacional invalido."),
                    http_status=status.HTTP_409_CONFLICT,
                    details={
                        "run_id": run_state.get("run_id", ""),
                        **context_error.get("details", {}),
                    },
                )

            _append_access_audit(
                store_payload,
                "status",
                actor_context,
                True,
                "runbook_access_allowed",
                "Consulta de status autorizada para contexto oficial.",
                run_id=run_state.get("run_id", ""),
                change_id=run_state.get("change_id", ""),
                provider_key=run_state.get("provider_key", ""),
            )
            _append_event(
                run_state,
                "info",
                "runbook_access_granted",
                "Consulta de status autorizada no runbook.",
                actor_user=actor_context.get("username", "system"),
                actor_role=actor_context.get("role", "user"),
            )

            if run_state.get("status", "") == "completed":
                verify_error = _validate_verify_artifact_consistency(store_payload, run_state)
                if verify_error:
                    run_state["status"] = "failed"
                    run_state["updated_at_utc"] = _utc_now()
                    run_state["last_failure"] = {
                        "code": verify_error.get("code", "runbook_verify_artifact_inconsistent"),
                        "message": verify_error.get(
                            "message",
                            "Auditoria invalida por ausencia de evidencia minima obrigatoria.",
                        ),
                        "timestamp_utc": _utc_now(),
                        "stage": run_state.get("current_stage", ""),
                        "checkpoint": run_state.get("current_checkpoint", ""),
                    }
                    _append_event(
                        run_state,
                        "error",
                        run_state["last_failure"]["code"],
                        run_state["last_failure"]["message"],
                    )

            _attach_run_snapshot(run_state, store_payload)
            store_payload["runs"][run_id] = run_state
            _save_store(store_payload)

            response = RunbookStateEnvelopeSerializer(
                data={"run": run_state, "snapshot": run_state.get("snapshot", {})}
            )
            if response.is_valid(raise_exception=True):
                return Response(ok(response.validated_data), status=status.HTTP_200_OK)

        except Exception as exc:
            LOG.exception("Runbook status endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["get"],
        responses=with_common_response(
            {drf_status.HTTP_200_OK: RunbookCatalogEnvelopeSerializer}
        ),
    )
    @action(detail=False, methods=["get"], url_path="catalog")
    def catalog(self, request):
        try:
            store_payload = _load_store()
            actor_context = _resolve_actor_context(request)

            if not _is_actor_authorized("catalog", actor_context.get("role", "user")):
                return _error_response(
                    "runbook_access_forbidden",
                    "Autorizacao insuficiente para consultar o catalogo oficial do runbook.",
                    http_status=status.HTTP_403_FORBIDDEN,
                    details={
                        "required_roles": sorted(
                            list(RUNBOOK_ALLOWED_ROLES_BY_ACTION.get("catalog", set()))
                        ),
                        "actor_role": actor_context.get("role", "user"),
                    },
                )

            runs = store_payload.get("runs", {})
            if not isinstance(runs, dict):
                runs = {}

            catalog_rows = []
            for run_state in runs.values():
                if not isinstance(run_state, dict):
                    continue
                if str(run_state.get("provider_key", "") or "").strip() != RUNBOOK_PROVIDER_KEY:
                    continue
                if not _run_contains_actor_scope(run_state, actor_context):
                    continue
                catalog_entry = _build_runbook_catalog_entry(run_state)
                if catalog_entry:
                    catalog_rows.append(catalog_entry)

            catalog_rows.sort(
                key=lambda row: str(
                    row.get("finishedAt", "") or row.get("capturedAt", "") or ""
                ).strip(),
                reverse=True,
            )

            response = RunbookCatalogEnvelopeSerializer(data={"runs": catalog_rows})
            if response.is_valid(raise_exception=True):
                return Response(ok(response.validated_data), status=status.HTTP_200_OK)
        except Exception as exc:
            LOG.exception("Runbook catalog endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["get"],
        responses=with_common_response(
            {drf_status.HTTP_200_OK: RunbookRuntimeInspectionEnvelopeSerializer}
        ),
    )
    @action(detail=True, methods=["get"], url_path="runtime-inspection")
    def runtime_inspection(self, request, pk=None):
        try:
            run_id = str(pk or "")
            store_payload = _load_store()
            run_state = store_payload.get("runs", {}).get(run_id)
            actor_context = _resolve_actor_context(request)
            if not run_state:
                return _error_response(
                    "runbook_not_found",
                    "Runbook nao encontrado para o run_id informado.",
                    http_status=status.HTTP_404_NOT_FOUND,
                )

            if not _is_actor_authorized("runtime-inspection", actor_context.get("role", "user")):
                _append_access_audit(
                    store_payload,
                    "runtime-inspection",
                    actor_context,
                    False,
                    "runbook_access_forbidden",
                    "Papel sem permissao para consultar inspecao runtime oficial.",
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)
                return _error_response(
                    "runbook_access_forbidden",
                    "Autorizacao insuficiente para consultar inspecao runtime oficial.",
                    http_status=status.HTTP_403_FORBIDDEN,
                    details={
                        "required_roles": sorted(
                            list(RUNBOOK_ALLOWED_ROLES_BY_ACTION.get("runtime-inspection", set()))
                        ),
                        "actor_role": actor_context.get("role", "user"),
                        "run_id": run_state.get("run_id", ""),
                    },
                )

            if not _run_contains_actor_scope(run_state, actor_context):
                _append_access_audit(
                    store_payload,
                    "runtime-inspection",
                    actor_context,
                    False,
                    "runbook_scope_forbidden",
                    "Escopo organizacional sem permissao para inspecao runtime.",
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)
                return _error_response(
                    "runbook_scope_forbidden",
                    "Escopo organizacional invalido para consultar esta inspecao runtime.",
                    http_status=status.HTTP_403_FORBIDDEN,
                    details={
                        "actor_organization": actor_context.get("organization_name", ""),
                        "run_id": run_state.get("run_id", ""),
                    },
                )

            context_error = _validate_run_context_for_critical_commands(run_state)
            if context_error:
                _append_access_audit(
                    store_payload,
                    "runtime-inspection",
                    actor_context,
                    False,
                    context_error.get("code", "runbook_change_context_required"),
                    context_error.get("message", "Contexto operacional invalido."),
                    run_id=run_state.get("run_id", ""),
                    change_id=run_state.get("change_id", ""),
                    provider_key=run_state.get("provider_key", ""),
                )
                _save_store(store_payload)
                return _error_response(
                    context_error.get("code", "runbook_change_context_required"),
                    context_error.get("message", "Contexto operacional invalido."),
                    http_status=status.HTTP_409_CONFLICT,
                    details={
                        "run_id": run_state.get("run_id", ""),
                        **context_error.get("details", {}),
                    },
                )

            org_id = str(request.query_params.get("org_id", "") or "").strip()
            host_id = str(request.query_params.get("host_id", "") or "").strip()
            component_id = str(request.query_params.get("component_id", "") or "").strip()
            inspection_scope = str(request.query_params.get("inspection_scope", "all") or "all").strip()
            refresh_requested = _coerce_runtime_inspection_refresh_flag(
                request.query_params.get("refresh", "")
            )

            _append_access_audit(
                store_payload,
                "runtime-inspection",
                actor_context,
                True,
                "runbook_access_allowed",
                "Consulta de inspecao runtime autorizada para contexto oficial.",
                run_id=run_state.get("run_id", ""),
                change_id=run_state.get("change_id", ""),
                provider_key=run_state.get("provider_key", ""),
            )
            _append_event(
                run_state,
                "info",
                "runbook_runtime_inspection_requested",
                "Consulta de inspecao runtime autorizada no runbook.",
                actor_user=actor_context.get("username", "system"),
                actor_role=actor_context.get("role", "user"),
            )

            inspection_payload, inspection_error = _build_runtime_inspection_response(
                run_state,
                org_id,
                host_id,
                component_id,
                inspection_scope,
                refresh_requested=refresh_requested,
            )
            if inspection_error:
                store_payload["runs"][run_id] = run_state
                _save_store(store_payload)
                return _error_response(
                    inspection_error.get("code", "runbook_runtime_inspection_failed"),
                    inspection_error.get("message", "Falha ao consultar inspecao runtime oficial."),
                    http_status=status.HTTP_400_BAD_REQUEST,
                    details={
                        "run_id": run_state.get("run_id", ""),
                        **inspection_error.get("details", {}),
                    },
                )

            _attach_run_snapshot(run_state, store_payload)
            store_payload["runs"][run_id] = run_state
            _save_store(store_payload)

            response = RunbookRuntimeInspectionEnvelopeSerializer(
                data={"inspection": inspection_payload}
            )
            if response.is_valid(raise_exception=True):
                return Response(ok(response.validated_data), status=status.HTTP_200_OK)

        except Exception as exc:
            LOG.exception("Runbook runtime inspection endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)
