#!/usr/bin/env python3
"""
Fabric Auto Operator

Monitors running Fabric peer containers provisioned by Cello and automatically
packages, installs, approves, and commits available chaincode artifacts. It also
synchronizes the local chaincode-gateway configuration so REST APIs are ready
without manual intervention.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import docker
import yaml
from docker.models.containers import Container
from packaging import version as packaging_version


LOG_LEVEL = os.getenv("FABRIC_OPERATOR_LOG_LEVEL", "INFO").upper()

# Use actual local paths for development
POLL_INTERVAL = int(os.getenv("FABRIC_OPERATOR_POLL_INTERVAL", "30"))
HOST_DATA_DIR = Path(os.getenv("HOST_DATA_DIR", "chaincode-gateway/data")).resolve()
GATEWAY_DATA_ROOT = Path(os.getenv("GATEWAY_DATA_ROOT", "chaincode-gateway/data")).resolve()
IDENTITIES_PATH = Path(os.getenv("HOST_IDENTITIES_PATH", HOST_DATA_DIR / "identities.json")).resolve()
TLS_TRUST_BUNDLE = HOST_DATA_DIR / "fabric-tlscas.pem"
HOST_WALLET_DIR = Path(os.getenv("HOST_WALLET_DIR", "chaincode-gateway/.gateway-wallet")).resolve()

FALLBACK_SEARCH_SOURCE = (
    Path(__file__).resolve().parent / "resources" / "cc_tools_search_fallback.go"
)
SEARCH_REWRITE_TARGETS = (
    "cc-tools/assets/search.go",
    "src/cc-tools/assets/search.go",
    "src/vendor/github.com/hyperledger-labs/cc-tools/assets/search.go",
    "vendor/github.com/hyperledger-labs/cc-tools/assets/search.go",
)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
HOST_CHAINCODE_ARCHIVE_CANDIDATES = (
    PROJECT_ROOT / "chaincodes",
    PROJECT_ROOT / "cello-storage" / "chaincode",
    Path("/workspace/chaincodes"),
    Path("/workspace/cello-storage/chaincode"),
)

PEER_CHAINCODE_DIR = "/opt/cello/chaincode"
FABRIC_CA_ROOT = "/opt/cello"
FABRIC_TLS_BUNDLE_ORDERER = (
    "crypto-config/ordererOrganizations/cello.local/tlsca/tlsca.cello.local-cert.pem"
)

PEER_ENV_TEMPLATE = {
    "CORE_PEER_TLS_ENABLED": "true",
    "CORE_PEER_TLS_ROOTCERT_FILE": "/etc/hyperledger/fabric/tls/ca.crt",
    "GODEBUG": "netdns=go",
    "FABRIC_LOGGING_SPEC": "error",
    "PATH": "/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "FABRIC_CFG_PATH": "/etc/hyperledger/fabric",
}

PEER_CONTAINER_REGEX = re.compile(r"^(peer)(\d+)\.(.+)$")
ORDERER_CONTAINER_REGEX = re.compile(r"^(orderer)(\d+)\.(.+)$")


@dataclass
class ChaincodePackage:
    name: str
    version: str
    label: str
    tar_path: str


class FabricAutoOperator:
    # ...existing code...
    def __init__(self) -> None:
        logging.basicConfig(
            level=LOG_LEVEL,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        self.logger = logging.getLogger(__name__)
        self.client = docker.from_env()
        self._ensure_directories()
        self._patched_archives: Dict[str, float] = {}
        self.logger.info(
            "Fabric Auto Operator initialized. Monitoring peers every %s seconds.",
            POLL_INTERVAL,
        )

    def _ensure_directories(self) -> None:
        HOST_DATA_DIR.mkdir(parents=True, exist_ok=True)
        HOST_WALLET_DIR.parent.mkdir(parents=True, exist_ok=True)

    def run(self) -> None:
        while True:
            try:
                self.reconcile()
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Unexpected error during reconciliation")
            time.sleep(POLL_INTERVAL)

    def reconcile(self) -> None:
        self._patch_host_chaincode_archives()
        peers = self._discover_peer_containers()
        if not peers:
            self.logger.debug("No running peer containers found.")
        for container in peers:
            try:
                self._reconcile_peer(container)
            except TemporarySkip as exc:
                self.logger.info("%s; will retry later.", exc)
            except Exception as exc:  # pylint: disable=broad-except
                err_msg = str(exc)
                # If MSP ID or config is missing, log as warning and skip error
                if (
                    "Pulando reconciliação para o peer" in err_msg
                    or "Arquivo" in err_msg and "não encontrado" in err_msg
                    or "Peer organization Name missing" in err_msg
                ):
                    self.logger.warning(
                        "Peer %s skipped due to missing MSP ID or config: %s", container.name, err_msg
                    )
                    continue
                # Detecta erros críticos de identidade/certificado
                if any(
                    s in err_msg.lower() for s in [
                        "creator org unknown",
                        "creator is malformed",
                        "access denied",
                        "error validating proposal",
                        "failed to endorse proposal"
                    ]
                ):
                    self.logger.error("Erro crítico de identidade/certificado detectado. Acionando automação de limpeza/regeneração de artefatos para o peer %s.", container.name)
                    self._automate_cleanup_and_regeneration(container)
                self.logger.exception(
                    "Failed to reconcile peer %s", container.name
                )

    def _automate_cleanup_and_regeneration(self, container):
        # Exemplo: remove artefatos antigos e reinicia o processo
        # Implemente a lógica real de limpeza conforme necessário
        peer_name = container.name
        self.logger.info(f"[AUTO] Limpando artefatos e regenerando para o peer {peer_name}")
        # Exemplo: remover diretórios de artefatos (ajuste paths conforme seu ambiente)
        try:
            # Remover artefatos do peer
            container.exec_run(["rm", "-rf", "/var/hyperledger/production"], user="root")
            # Opcional: reiniciar o container
            container.restart()
            self.logger.info(f"[AUTO] Artefatos limpos e container {peer_name} reiniciado.")
        except Exception as e:
            self.logger.error(f"[AUTO] Falha ao limpar/regenerar artefatos para {peer_name}: {e}")

    def _discover_peer_containers(self) -> List[Container]:
        containers = self.client.containers.list()
        peers: List[Container] = []
        for container in containers:
            name = container.name
            if PEER_CONTAINER_REGEX.match(name):
                peers.append(container)
        return peers

    @staticmethod
    def _get_env_var(container: Container, key: str) -> Optional[str]:
        env_items = container.attrs.get("Config", {}).get("Env", []) or []
        prefix = f"{key}="
        for entry in env_items:
            if entry.startswith(prefix):
                return entry[len(prefix) :].strip()
        return None

    def _decode_tls_bundle(self, packed: Optional[str]) -> Optional[str]:
        if not packed:
            return None
        try:
            raw = base64.b64decode(packed)
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                for candidate in (
                    "tls/ca.crt",
                    "ca.crt",
                    "tls/ca.pem",
                    "ca.pem",
                ):
                    try:
                        payload = zf.read(candidate)
                        return payload.decode("utf-8")
                    except KeyError:
                        continue
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.warning(
                "Failed to decode TLS bundle from env: %s", exc
            )
        return None

    def _discover_orderer_endpoint(
        self, peer_index: int
    ) -> Tuple[str, Optional[str]]:
        orderer_containers = [
            container
            for container in self.client.containers.list()
            if ORDERER_CONTAINER_REGEX.match(container.name)
        ]
        candidate: Optional[Container] = None
        for container in orderer_containers:
            match = ORDERER_CONTAINER_REGEX.match(container.name)
            if match and int(match.group(2)) == peer_index:
                candidate = container
                break
        if candidate is None and orderer_containers:
            candidate = orderer_containers[0]
        if candidate is None:
            return "orderer1.cello.local", None
        tls_env = self._get_env_var(candidate, "HLF_NODE_TLS")
        tls_pem = self._decode_tls_bundle(tls_env)
        if tls_pem is None:
            self.logger.warning(
                "Could not decode TLS bundle for orderer container %s from environment; gateway will reuse peer TLS bundle.",
                candidate.name,
            )
        return candidate.name, tls_pem

    def _reconcile_peer(self, container: Container) -> None:
        peer_host = container.name
        match = PEER_CONTAINER_REGEX.match(peer_host)
        if not match:
            raise ValueError(f"Container name {peer_host} does not match peer pattern.")

        peer_index = int(match.group(2))
        peer_domain = match.group(3)
        alias = peer_domain.split(".")[0]
        peer_port = 7051
        orderer_host, orderer_tls_pem = self._discover_orderer_endpoint(peer_index)
        orderer_port = 7050

        self.logger.info(
            "Reconciling peer %s (alias=%s, orderer=%s)",
            peer_host,
            alias,
            orderer_host,
        )

        try:
            msp_id = self._probe_msp_id(container, peer_domain)
        except ExecCommandError as e:
            self.logger.warning(f"Peer {peer_domain} ignorado: {e}")
            return
        if not msp_id:
            self.logger.warning(f"Pulando reconciliação para peer {peer_domain} por falta de MSP ID ou arquivo de configuração.")
            return
        self._validate_state_database(container)
        env = self._build_peer_env(peer_domain, peer_host, peer_port, msp_id)

        channels = self._list_channels(container, env)
        if not channels:
            raise TemporarySkip(
                f"Peer {peer_host} has not joined any channel yet."
            )

        packages = self._discover_chaincode_packages(container)
        if not packages:
            raise TemporarySkip(
                f"No chaincode packages found inside {PEER_CHAINCODE_DIR} for peer {peer_host}"
            )

        packages = self._select_latest_packages(packages)
        package_ids = self._ensure_chaincodes_installed(container, env, packages)
        self._ensure_chaincodes_committed(
            container,
            env,
            channels,
            packages,
            package_ids,
            peer_host,
            peer_port,
            orderer_host,
            orderer_port,
            peer_domain,
        )
        self._sync_gateway(
            container=container,
            alias=alias,
            msp_id=msp_id,
            channels=channels,
            peer_domain=peer_domain,
            peer_host=peer_host,
            peer_port=peer_port,
            orderer_host=orderer_host,
            orderer_port=orderer_port,
            orderer_tls_pem=orderer_tls_pem,
        )
        self.logger.info(
            "Peer %s processed successfully. Channels: %s",
            peer_host,
            ", ".join(sorted(channels)),
        )

    # --------------------------------------------------------------------- #
    # Peer metadata helpers
    # --------------------------------------------------------------------- #
    def _probe_msp_id(self, container: Container, peer_domain: str) -> Optional[str]:
        env_items = container.attrs.get("Config", {}).get("Env", []) or []
        for entry in env_items:
            if entry.startswith("CORE_PEER_LOCALMSPID="):
                value = entry.split("=", 1)[1].strip()
                if value:
                    self.logger.debug(
                        "Detected MSP ID '%s' for peer %s via container environment.",
                        value,
                        peer_domain,
                    )
                    return value

        crypto_path = f"{FABRIC_CA_ROOT}/{peer_domain}/crypto-config.yaml"
        try:
            raw_yaml = self._exec_simple(container, ["cat", crypto_path])
        except Exception:
            inferred = self._derive_msp_id_from_domain(peer_domain)
            if inferred:
                self.logger.warning(
                    "Arquivo %s não encontrado no container. Inferindo MSP ID '%s' a partir do domínio do peer %s.",
                    crypto_path,
                    inferred,
                    peer_domain,
                )
                return inferred
            self.logger.warning(
                "Arquivo %s não encontrado no container e não foi possível inferir MSP ID para o peer %s.",
                crypto_path,
                peer_domain,
            )
            return None
        document = yaml.safe_load(raw_yaml)
        peer_orgs = document.get("PeerOrgs") or []
        if not peer_orgs:
            self.logger.warning(f"No PeerOrgs found in {crypto_path}. Pulando peer {peer_domain}.")
            return None
        name = peer_orgs[0].get("Name")
        if not name:
            self.logger.warning(f"Peer organization Name missing in {crypto_path}. Pulando peer {peer_domain}.")
            return None
        msp_id = f"{name}MSP"
        return msp_id

    @staticmethod
    def _derive_msp_id_from_domain(peer_domain: str) -> Optional[str]:
        if not peer_domain:
            return None
        parts = peer_domain.split(".")
        if len(parts) < 2:
            return None
        org_segment = parts[1].strip()
        if not org_segment:
            return None
        base = org_segment[0].upper() + org_segment[1:]
        return f"{base}MSP"

    def _build_peer_env(
        self,
        peer_domain: str,
        peer_host: str,
        peer_port: int,
        msp_id: str,
    ) -> Dict[str, str]:
        env = dict(PEER_ENV_TEMPLATE)
        env["CORE_PEER_LOCALMSPID"] = msp_id
        env["CORE_PEER_ADDRESS"] = f"{peer_host}:{peer_port}"
        env[
            "CORE_PEER_MSPCONFIGPATH"
        ] = (
            f"{FABRIC_CA_ROOT}/{peer_domain}/crypto-config/peerOrganizations/"
            f"{peer_domain}/users/Admin@{peer_domain}/msp"
        )
        return env

    def _validate_state_database(self, container: Container) -> None:
        env_items = container.attrs.get("Config", {}).get("Env", []) or []
        db_value = None
        for entry in env_items:
            if entry.startswith("CORE_LEDGER_STATE_STATEDATABASE="):
                db_value = entry.split("=", 1)[1].strip()
                break
        if not db_value:
            raise RuntimeError(
                f"Não foi possível detectar CORE_LEDGER_STATE_STATEDATABASE em {container.name}; configure CouchDB antes de continuar."
            )
        if db_value.lower() != "couchdb":
            raise RuntimeError(
                f"Peer {container.name} está configurado com state database '{db_value}'. É obrigatório usar CouchDB para provisionar chaincodes."
            )

    def _list_channels(
        self, container: Container, env: Dict[str, str]
    ) -> List[str]:
        try:
            output = self._exec_peer(
                container,
                "peer channel list",
                env,
                "listing joined channels",
            )
        except ExecCommandError as exc:
            if "query to peer failed" in exc.stderr.lower():
                raise TemporarySkip(
                    "peer channel list not ready yet (peer not joined a channel)"
                ) from exc
            raise

        channels: List[str] = []
        for line in output.splitlines():
            if line.strip() and not line.lower().startswith("channels"):
                channels.append(line.strip())
        return channels

    # --------------------------------------------------------------------- #
    # Chaincode package discovery
    # --------------------------------------------------------------------- #
    def _discover_chaincode_packages(
        self, container: Container
    ) -> List[ChaincodePackage]:
        try:
            listing = self._exec_simple(
                container, ["ls", "-1", PEER_CHAINCODE_DIR]
            )
        except ExecCommandError as exc:
            if exc.exit_code == 2:
                return []
            raise

        packages: List[ChaincodePackage] = []
        for entry in listing.splitlines():
            entry = entry.strip()
            if not entry:
                continue

            tar_path: Optional[str] = None
            label: Optional[str] = None

            if entry.endswith(".tar.gz"):
                label = entry[:-7]
                tar_path = f"{PEER_CHAINCODE_DIR}/{entry}"
            elif ":" in entry:
                label = entry.split(":", 1)[0]
                tar_path = f"{PEER_CHAINCODE_DIR}/{entry}/{label}.tar.gz"
            else:
                continue

            if "_" not in label:
                continue

            name, version_str = label.rsplit("_", 1)
            tar_exists = self._exec_simple(
                container,
                ["sh", "-c", f"test -f {tar_path} && echo ok || echo missing"],
            ).strip()
            if tar_exists != "ok":
                self.logger.warning(
                    "Skipping chaincode label %s: tarball %s missing",
                    label,
                    tar_path,
                )
                continue

            self._ensure_search_fallback(tar_path)

            packages.append(
                ChaincodePackage(
                    name=name,
                    version=version_str,
                    label=label,
                    tar_path=tar_path,
                )
            )

        return packages

    def _ensure_search_fallback(self, tar_path: str) -> bool:
        if not FALLBACK_SEARCH_SOURCE.exists():
            return False

        fallback_contents = FALLBACK_SEARCH_SOURCE.read_text()

        def patch_search_sources(base_dir: Path) -> bool:
            patched = False
            for relative in SEARCH_REWRITE_TARGETS:
                candidate = base_dir / relative
                if not candidate.exists():
                    continue
                try:
                    if candidate.read_text() == fallback_contents:
                        continue
                except OSError:
                    continue
                candidate.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_text(fallback_contents)
                patched = True
            return patched

        patched_any = False

        try:
            with tarfile.open(tar_path, "r:gz") as source_tar:
                with tempfile.TemporaryDirectory(prefix="cc_patch_") as tmpdir:
                    extract_dir = Path(tmpdir)
                    source_tar.extractall(extract_dir)

                    if patch_search_sources(extract_dir):
                        patched_any = True

                    nested_archives: List[Tuple[Path, Path]] = []
                    for nested_tar in extract_dir.rglob("code.tar.gz"):
                        nested_dir = nested_tar.parent / f".patched_{nested_tar.stem}"
                        try:
                            with tarfile.open(nested_tar, "r:gz") as inner_tar:
                                nested_dir.mkdir(exist_ok=True)
                                inner_tar.extractall(nested_dir)
                        except tarfile.TarError:
                            shutil.rmtree(nested_dir, ignore_errors=True)
                            continue
                        if patch_search_sources(nested_dir):
                            nested_archives.append((nested_tar, nested_dir))
                            patched_any = True
                        else:
                            shutil.rmtree(nested_dir, ignore_errors=True)

                    for nested_tar, nested_dir in nested_archives:
                        patched_tmp = nested_dir.parent / f"{nested_dir.name}.tar.gz"
                        with tarfile.open(patched_tmp, "w:gz") as rebuilt:
                            for child in sorted(nested_dir.iterdir()):
                                rebuilt.add(child, arcname=child.name)
                        shutil.move(patched_tmp, nested_tar)
                        shutil.rmtree(nested_dir, ignore_errors=True)

                    if not patched_any:
                        return False

                    patched_tar_path = extract_dir / "patched.tar.gz"
                    with tarfile.open(patched_tar_path, "w:gz") as patched_tar:
                        for child in sorted(extract_dir.iterdir()):
                            if child.name in {"patched.tar.gz"} or child.name.startswith(".patched_"):
                                continue
                            patched_tar.add(child, arcname=child.name)

                    shutil.move(patched_tar_path, tar_path)
                    return True
        except (tarfile.TarError, OSError) as err:
            self.logger.warning(
                "Unable to patch search fallback in %s: %s", tar_path, err
            )
        return False

    def _patch_host_chaincode_archives(self) -> None:
        candidates: List[Path] = []
        seen: set[str] = set()
        for root in HOST_CHAINCODE_ARCHIVE_CANDIDATES:
            try:
                resolved = root.resolve()
            except OSError:
                continue
            if not resolved.exists():
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(resolved)

        for root in candidates:
            for tar_path in root.rglob("*.tar.gz"):
                try:
                    mtime = tar_path.stat().st_mtime
                except OSError:
                    continue
                cache_key = str(tar_path)
                cached_mtime = self._patched_archives.get(cache_key, 0.0)
                if cached_mtime and cached_mtime >= mtime:
                    continue
                if self._ensure_search_fallback(str(tar_path)):
                    try:
                        self._patched_archives[cache_key] = tar_path.stat().st_mtime
                    except OSError:
                        self._patched_archives[cache_key] = mtime
                else:
                    self._patched_archives[cache_key] = mtime

    def _select_latest_packages(
        self, packages: List[ChaincodePackage]
    ) -> List[ChaincodePackage]:
        selected: List[ChaincodePackage] = []
        grouped: Dict[str, List[ChaincodePackage]] = {}
        for package in packages:
            grouped.setdefault(package.name, []).append(package)

        for _, items in grouped.items():
            if len(items) == 1:
                selected.append(items[0])
                continue
            sorted_items = sorted(
                items, key=lambda p: packaging_version.parse(p.version)
            )
            selected.append(sorted_items[-1])
        return selected

    # --------------------------------------------------------------------- #
    # Chaincode lifecycle: install & commit
    # --------------------------------------------------------------------- #
    def _ensure_chaincodes_installed(
        self,
        container: Container,
        env: Dict[str, str],
        packages: Sequence[ChaincodePackage],
    ) -> Dict[str, str]:
        installed_map = self._query_installed(container, env)
        for package in packages:
            if package.label not in installed_map:
                self.logger.info(
                    "Installing chaincode %s on %s", package.label, container.name
                )
                self._exec_peer(
                    container,
                    f"peer lifecycle chaincode install {package.tar_path}",
                    env,
                    f"installing {package.label}",
                )
                installed_map = self._query_installed(container, env)
        return installed_map

    def _query_installed(
        self, container: Container, env: Dict[str, str]
    ) -> Dict[str, str]:
        output = self._exec_peer(
            container,
            "peer lifecycle chaincode queryinstalled",
            env,
            "query installed chaincodes",
        )
        mapping: Dict[str, str] = {}
        for line in output.splitlines():
            match = re.search(r"Package ID:\s*(.*?),\s*Label:\s*(.+)", line)
            if match:
                package_id = match.group(1).strip()
                label = match.group(2).strip()
                mapping[label] = package_id
        return mapping

    def _ensure_chaincodes_committed(
        self,
        container: Container,
        env: Dict[str, str],
        channels: Sequence[str],
        packages: Sequence[ChaincodePackage],
        package_ids: Dict[str, str],
        peer_host: str,
        peer_port: int,
        orderer_host: str,
        orderer_port: int,
        peer_domain: str,
    ) -> None:
        committed_cache: Dict[str, Dict[str, Dict[str, str]]] = {}
        orderer_tls_path = (
            f"{FABRIC_CA_ROOT}/{peer_domain}/{FABRIC_TLS_BUNDLE_ORDERER}"
        )

        for channel in channels:
            committed_cache[channel] = self._query_committed(container, env, channel)

        for package in packages:
            package_id = package_ids.get(package.label)
            if not package_id:
                self.logger.error(
                    "Package ID not found for label %s; skipping",
                    package.label,
                )
                continue

            for channel in channels:
                channel_committed = committed_cache.get(channel, {})
                committed_info = channel_committed.get(package.name)
                if committed_info and committed_info.get("version") == package.version:
                    self.logger.debug(
                        "Chaincode %s already committed on channel %s",
                        package.label,
                        channel,
                    )
                    continue

                if committed_info:
                    next_sequence = int(committed_info["sequence"]) + 1
                else:
                    next_sequence = 1

                self.logger.info(
                    "Approving chaincode %s on channel %s (sequence %s)",
                    package.label,
                    channel,
                    next_sequence,
                )
                approve_cmd = " ".join(
                    [
                        "peer lifecycle chaincode approveformyorg",
                        f"-o {orderer_host}:{orderer_port}",
                        f"--ordererTLSHostnameOverride {orderer_host}",
                        "--tls",
                        f"--cafile {orderer_tls_path}",
                        f"-C {channel}",
                        f"-n {package.name}",
                        f"-v {package.version}",
                        f"--sequence {next_sequence}",
                        f"--package-id {package_id}",
                    ]
                )
                self._exec_peer(
                    container,
                    approve_cmd,
                    env,
                    f"approving {package.label} on {channel}",
                )

                self.logger.info(
                    "Committing chaincode %s on channel %s (sequence %s)",
                    package.label,
                    channel,
                    next_sequence,
                )
                commit_cmd = " ".join(
                    [
                        "peer lifecycle chaincode commit",
                        f"-o {orderer_host}:{orderer_port}",
                        f"--ordererTLSHostnameOverride {orderer_host}",
                        "--tls",
                        f"--cafile {orderer_tls_path}",
                        f"-C {channel}",
                        f"-n {package.name}",
                        f"-v {package.version}",
                        f"--sequence {next_sequence}",
                        f"--peerAddresses {peer_host}:{peer_port}",
                        "--tlsRootCertFiles /etc/hyperledger/fabric/tls/ca.crt",
                    ]
                )
                self._exec_peer(
                    container,
                    commit_cmd,
                    env,
                    f"committing {package.label} on {channel}",
                )

                committed_cache[channel] = self._query_committed(
                    container, env, channel
                )

    def _query_committed(
        self, container: Container, env: Dict[str, str], channel: str
    ) -> Dict[str, Dict[str, str]]:
        output = self._exec_peer(
            container,
            f"peer lifecycle chaincode querycommitted -C {channel}",
            env,
            f"query committed chaincodes on {channel}",
        )
        committed: Dict[str, Dict[str, str]] = {}
        for line in output.splitlines():
            match = re.search(
                r"Name:\s*(\S+),\s*Version:\s*(\S+),\s*Sequence:\s*(\d+)", line
            )
            if match:
                committed[match.group(1)] = {
                    "version": match.group(2),
                    "sequence": match.group(3),
                }
        return committed

    # --------------------------------------------------------------------- #
    # Gateway synchronization
    # --------------------------------------------------------------------- #
    def _sync_gateway(
        self,
        *,
        container: Container,
        alias: str,
        msp_id: str,
        channels: Iterable[str],
        peer_domain: str,
        peer_host: str,
        peer_port: int,
        orderer_host: str,
        orderer_port: int,
        orderer_tls_pem: Optional[str],
    ) -> None:
        alias_dir = HOST_DATA_DIR / alias
        msp_dir = alias_dir / "msp"
        signcerts_dir = msp_dir / "signcerts"
        keystore_dir = msp_dir / "keystore"
        tls_dir = alias_dir / "tls"

        _clear_directory(signcerts_dir)
        _clear_directory(keystore_dir)
        _clear_directory(tls_dir)

        admin_signcert_dir = (
            f"{FABRIC_CA_ROOT}/{peer_domain}/crypto-config/peerOrganizations/"
            f"{peer_domain}/users/Admin@{peer_domain}/msp/signcerts"
        )
        admin_keystore_dir = (
            f"{FABRIC_CA_ROOT}/{peer_domain}/crypto-config/peerOrganizations/"
            f"{peer_domain}/users/Admin@{peer_domain}/msp/keystore"
        )

        cert_filename, cert_changed = self._copy_single_file(
            container, admin_signcert_dir, signcerts_dir
        )
        key_filename, key_changed = self._copy_single_file(
            container, admin_keystore_dir, keystore_dir, file_mode=0o600
        )

        peer_tls_path = (
            f"{FABRIC_CA_ROOT}/{peer_domain}/crypto-config/peerOrganizations/"
            f"{peer_domain}/tlsca/tlsca.{peer_domain}-cert.pem"
        )
        peer_tls_content = self._read_container_file(container, peer_tls_path)
        peer_tls_local = tls_dir / f"{peer_host}-tlsca.pem"
        peer_tls_changed = _write_file(peer_tls_local, peer_tls_content, mode=0o644)

        orderer_tls_content = orderer_tls_pem
        if not orderer_tls_content:
            orderer_tls_path = (
                f"{FABRIC_CA_ROOT}/{peer_domain}/crypto-config/"
                "ordererOrganizations/cello.local/tlsca/tlsca.cello.local-cert.pem"
            )
            try:
                orderer_tls_content = self._read_container_file(
                    container, orderer_tls_path
                )
            except ExecCommandError as exc:
                self.logger.debug(
                    "Unable to read orderer TLS bundle from %s: %s",
                    orderer_tls_path,
                    exc,
                )
        orderer_tls_local = tls_dir / f"{orderer_host}-tlsca.pem"
        orderer_tls_changed = False
        if orderer_tls_content:
            orderer_tls_changed = _write_file(
                orderer_tls_local, orderer_tls_content, mode=0o644
            )

        _append_unique_certificate(TLS_TRUST_BUNDLE, peer_tls_content)
        if orderer_tls_content:
            _append_unique_certificate(TLS_TRUST_BUNDLE, orderer_tls_content)

        connection_profile_host = HOST_DATA_DIR / f"connection-{alias}.json"
        connection_profile_container = (
            GATEWAY_DATA_ROOT / f"connection-{alias}.json"
        )

        peer_tls_container_path = str(
            GATEWAY_DATA_ROOT / alias / "tls" / f"{peer_host}-tlsca.pem"
        )
        orderer_tls_container_path = str(
            GATEWAY_DATA_ROOT / alias / "tls" / f"{orderer_host}-tlsca.pem"
        )
        if not orderer_tls_content:
            orderer_tls_container_path = peer_tls_container_path

        connection_profile = {
            "name": f"{alias}-gateway",
            "version": "1.0.0",
            "client": {"organization": alias},
            "organizations": {
                alias: {
                    "mspid": msp_id,
                    "peers": [peer_host],
                }
            },
            "peers": {
                peer_host: {
                    "url": f"grpcs://{peer_host}:{peer_port}",
                    "tlsCACerts": {"path": peer_tls_container_path},
                    "grpcOptions": {
                        "ssl-target-name-override": peer_host,
                        "hostnameOverride": peer_host,
                    },
                }
            },
            "orderers": {
                orderer_host: {
                    "url": f"grpcs://{orderer_host}:{orderer_port}",
                    "tlsCACerts": {"path": orderer_tls_container_path},
                    "grpcOptions": {
                        "ssl-target-name-override": orderer_host,
                        "hostnameOverride": orderer_host,
                    },
                }
            },
        }
        profile_changed = _write_json(connection_profile_host, connection_profile)

        cert_container_path = str(
            GATEWAY_DATA_ROOT
            / alias
            / "msp"
            / "signcerts"
            / cert_filename
        )
        key_container_path = str(
            GATEWAY_DATA_ROOT / alias / "msp" / "keystore" / key_filename
        )

        channels_lower = sorted({channel.lower() for channel in channels})

        identities = {}
        if IDENTITIES_PATH.exists():
            try:
                identities = json.loads(IDENTITIES_PATH.read_text())
            except json.JSONDecodeError:
                self.logger.warning(
                    "identities.json is invalid JSON; recreating"
                )
                identities = {}

        # Sempre atualiza a entrada da organização provisionada
        identities[alias] = {
            "mspId": msp_id,
            "ccpPath": str(connection_profile_container),
            "certPath": cert_container_path,
            "keyPath": key_container_path,
            "channels": channels_lower,
            "discoveryAsLocalhost": False,
        }
        # Remove fallback para default
        if "default" in identities:
            del identities["default"]
        identities_changed = _write_json(IDENTITIES_PATH, identities)

        if any([cert_changed, key_changed, profile_changed, identities_changed]):
            self._reset_wallet_directory()
            self.logger.info(
                "Gateway wallet reset for alias %s due to configuration updates",
                alias,
            )

    def _copy_single_file(
        self,
        container: Container,
        source_dir: str,
        destination_dir: Path,
        file_mode: int = 0o644,
    ) -> Tuple[str, bool]:
        listing = self._exec_simple(container, ["ls", "-1", source_dir])
        files = [line.strip() for line in listing.splitlines() if line.strip()]
        if not files:
            raise RuntimeError(f"No files found in {source_dir}")
        filename = files[0]
        content = self._read_container_file(container, f"{source_dir}/{filename}")
        destination_path = destination_dir / filename
        changed = _write_file(destination_path, content, mode=file_mode)
        return filename, changed

    def _reset_wallet_directory(self) -> None:
        if HOST_WALLET_DIR.exists():
            shutil.rmtree(HOST_WALLET_DIR)
        HOST_WALLET_DIR.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------- #
    # Docker helpers
    # --------------------------------------------------------------------- #
    def _exec_simple(self, container: Container, args: List[str]) -> str:
        result = container.exec_run(args, user="root", demux=True)
        stdout, stderr = result.output
        stdout_text = (stdout or b"").decode("utf-8", errors="replace")
        stderr_text = (stderr or b"").decode("utf-8", errors="replace")
        if result.exit_code != 0:
            raise ExecCommandError(
                command=" ".join(args),
                exit_code=result.exit_code,
                stdout=stdout_text,
                stderr=stderr_text,
            )
        return stdout_text

    def _exec_peer(
        self,
        container: Container,
        script: str,
        env: Dict[str, str],
        description: str,
    ) -> str:
        command = [
            "bash",
            "-lc",
            f"set -eo pipefail; export PATH=\"$PATH:/go/bin\"; {script}",
        ]
        result = container.exec_run(
            command,
            environment=env,
            user="root",
            demux=True,
        )
        stdout, stderr = result.output
        stdout_text = (stdout or b"").decode("utf-8", errors="replace")
        stderr_text = (stderr or b"").decode("utf-8", errors="replace")
        if result.exit_code != 0:
            raise ExecCommandError(
                command=script,
                exit_code=result.exit_code,
                stdout=stdout_text,
                stderr=stderr_text,
            )
        if stderr_text.strip():
            self.logger.debug(
                "Command '%s' produced stderr output: %s",
                description,
                stderr_text.strip(),
            )
        return stdout_text

    def _read_container_file(
        self, container: Container, file_path: str
    ) -> str:
        return self._exec_simple(container, ["cat", file_path])


class ExecCommandError(RuntimeError):
    def __init__(
        self,
        command: str,
        exit_code: int,
        stdout: str,
        stderr: str,
    ) -> None:
        super().__init__(
            f"Command '{command}' failed with exit code {exit_code}: {stderr or stdout}"
        )
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr


class TemporarySkip(RuntimeError):
    """Signal that reconciliation should be retried later."""


def _clear_directory(path: Path) -> None:
    if path.exists():
        # Dynamically fix permissions before rmtree to avoid PermissionError from container-created files
        try:
            import subprocess
            subprocess.run([
                'sudo', 'chown', '-R', f'{os.getuid()}:{os.getgid()}', str(path)
            ], check=False)
            subprocess.run([
                'sudo', 'chmod', '-R', 'u+rwX', str(path)
            ], check=False)
        except Exception as e:
            pass
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _write_file(path: Path, content: str, mode: int = 0o644) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text() == content:
        os.chmod(path, mode)
        return False
    path.write_text(content)
    os.chmod(path, mode)
    return True


def _write_json(path: Path, data: Dict) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text() == payload:
        return False
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(payload)
    os.chmod(tmp_path, 0o644)
    tmp_path.replace(path)
    return True


def _append_unique_certificate(bundle: Path, certificate_pem: str) -> bool:
    normalized = certificate_pem.strip()
    if not normalized:
        raise RuntimeError("TLS certificate content is empty.")
    if bundle.exists():
        existing = bundle.read_text()
        if normalized in existing:
            return False
        if not existing.endswith("\n"):
            existing += "\n"
        bundle.write_text(existing + normalized + "\n")
    else:
        bundle.write_text(normalized + "\n")
    os.chmod(bundle, 0o644)
    return True


def main() -> None:
    operator = FabricAutoOperator()
    operator.run()


if __name__ == "__main__":
    main()
