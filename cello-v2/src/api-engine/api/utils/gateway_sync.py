"""
Utilities to keep chaincode-gateway artifacts in sync with the organizations
managed by the API.

This replicates (and extends) the bootstrap logic that used to live only in
start_env.sh, allowing the API to refresh connection profiles, identities and
trust bundles whenever new orgs/channels are created at runtime.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from api.config import CELLO_HOME

LOG = logging.getLogger(__name__)

CELLO_HOME_PATH = Path(CELLO_HOME)

DEFAULT_GATEWAY_DIR = Path(
    os.getenv("GATEWAY_DATA_DIR", "/workspace/chaincode-gateway/data")
).resolve()
HOST_GATEWAY_DIR_ENV = os.getenv("HOST_GATEWAY_DATA_DIR")

DEFAULT_AUTOMATION_DIR = Path(
    os.getenv("AUTOMATION_RESOURCES_DIR", "/workspace/automation/resources")
).resolve()
HOST_AUTOMATION_DIR_ENV = os.getenv("HOST_AUTOMATION_RESOURCES_DIR")

GATEWAY_DATA_DIRS: List[Path] = list(
    dict.fromkeys(
        [
            DEFAULT_GATEWAY_DIR,
            *(
                (Path(HOST_GATEWAY_DIR_ENV).resolve(),)
                if HOST_GATEWAY_DIR_ENV
                else ()
            ),
        ]
    )
)

AUTOMATION_DIRS: List[Path] = list(
    dict.fromkeys(
        [
            DEFAULT_AUTOMATION_DIR,
            *(
                (Path(HOST_AUTOMATION_DIR_ENV).resolve(),)
                if HOST_AUTOMATION_DIR_ENV
                else ()
            ),
        ]
    )
)


def _prepare_data_dir(base: Path, expected_aliases: Iterable[str]) -> None:
    """
    Ensure the target gateway data directory exists and drop stale aliases.
    """
    expected: Set[str] = {
        alias for alias in expected_aliases if alias and alias.strip()
    }
    base.mkdir(parents=True, exist_ok=True)

    for entry in list(base.iterdir()):
        name = entry.name
        if entry.is_dir():
            if name not in expected:
                shutil.rmtree(entry, ignore_errors=True)
            continue
        if entry.suffix == ".json" and name.startswith("connection-"):
            alias = name[len("connection-") : -len(".json")]
            if alias not in expected:
                entry.unlink(missing_ok=True)


def _identities_path(base: Path) -> Path:
    return base / "identities.json"


def _trust_bundle_path(base: Path) -> Path:
    return base / "fabric-tlscas.pem"


def _derive_alias(org_domain: str) -> str:
    """Return a short alias used by gateway files."""
    if not org_domain:
        return org_domain
    return org_domain.split(".", 1)[0]


def _to_msp_id(org_domain: str) -> str:
    alias = _derive_alias(org_domain)
    if not alias:
        return "OrgMSP"
    return f"{alias[:1].upper()}{alias[1:]}MSP"


def _collect_peer_hosts(peers_dir: Path) -> List[str]:
    hosts: List[str] = []
    if not peers_dir.exists():
        return hosts
    for entry in sorted(peers_dir.iterdir()):
        if entry.is_dir():
            hosts.append(entry.name)
    return hosts


def _first_file(directory: Path) -> Optional[Path]:
    if not directory.exists():
        return None
    for entry in directory.iterdir():
        if entry.is_file():
            return entry
    return None


def _resolve_orderer_for_org(
    crypto_root: Path, org_domain: str
) -> (Optional[str], Optional[Path]):
    if "." not in org_domain:
        return None, None
    orderer_domain = org_domain.split(".", 1)[1]
    orderer_root = (
        crypto_root / "ordererOrganizations" / orderer_domain / "orderers"
    )
    if not orderer_root.exists():
        # Try to reuse the first orderer available anywhere under crypto_root
        fallback = list(
            (crypto_root / "ordererOrganizations").glob("*/orderers/*")
        )
        if not fallback:
            return None, None
        chosen = fallback[0]
        host = chosen.name
        tls_path = (
            chosen.parent.parent
            / "tlsca"
            / f"tlsca.{chosen.parent.parent.name}-cert.pem"
        )
        return host, tls_path if tls_path.exists() else None

    chosen = next((p for p in sorted(orderer_root.iterdir()) if p.is_dir()), None)
    if not chosen:
        return None, None
    host = chosen.name
    tls_path = (
        chosen.parent.parent
        / "tlsca"
        / f"tlsca.{orderer_domain}-cert.pem"
    )
    if not tls_path.exists():
        tls_path = None
    return host, tls_path


def _discover_peer_orgs() -> List[Dict[str, object]]:
    orgs: List[Dict[str, object]] = []
    if not CELLO_HOME_PATH.exists():
        return orgs
    for org_root in sorted(CELLO_HOME_PATH.iterdir()):
        crypto_root = org_root / "crypto-config"
        peer_orgs_root = crypto_root / "peerOrganizations"
        if not peer_orgs_root.exists():
            continue
        for org_dir in sorted(peer_orgs_root.iterdir()):
            if not org_dir.is_dir():
                continue
            org_domain = org_dir.name
            admin_base = (
                org_dir / "users" / f"Admin@{org_domain}" / "msp"
            )
            cert_path = (
                admin_base / "signcerts" / f"Admin@{org_domain}-cert.pem"
            )
            key_dir = admin_base / "keystore"
            key_path = _first_file(key_dir)
            if not cert_path.exists() or key_path is None:
                LOG.warning(
                    "[GATEWAY_SYNC] Artefatos MSP ausentes para %s (cert=%s, key=%s)",
                    org_domain,
                    cert_path,
                    key_dir,
                )
                continue
            tls_ca_path = (
                org_dir / "tlsca" / f"tlsca.{org_domain}-cert.pem"
            )
            peers_dir = org_dir / "peers"
            peer_hosts = _collect_peer_hosts(peers_dir)
            if not peer_hosts:
                LOG.warning(
                    "[GATEWAY_SYNC] Nenhum peer encontrado para %s em %s",
                    org_domain,
                    peers_dir,
                )
            orderer_host, orderer_tls = _resolve_orderer_for_org(
                crypto_root, org_domain
            )
            orgs.append(
                {
                    "org_domain": org_domain,
                    "alias": _derive_alias(org_domain),
                    "msp_id": _to_msp_id(org_domain),
                    "cert_path": cert_path,
                    "key_path": key_path,
                    "tls_ca_path": tls_ca_path if tls_ca_path.exists() else None,
                    "peer_hosts": peer_hosts,
                    "orderer_host": orderer_host,
                    "orderer_tls": orderer_tls,
                }
            )
    return orgs


def _discover_channels() -> List[str]:
    channels: Set[str] = set()
    if not CELLO_HOME_PATH.exists():
        return []
    for block_file in CELLO_HOME_PATH.rglob("*.block"):
        channels.add(block_file.stem)
    return sorted(channels)


def _write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2))
    os.chmod(tmp_path, 0o644)
    tmp_path.replace(path)


def sync_gateway_artifacts() -> None:
    """
    Regenerate connection profiles, identities.json e bundles TLS em todos os
    diretórios compartilhados com o gateway (container e host).
    """
    try:
        orgs = _discover_peer_orgs()
        channels = _discover_channels()

        desired_aliases = {org["alias"] for org in orgs}
        for auto_dir in AUTOMATION_DIRS:
            auto_dir.mkdir(parents=True, exist_ok=True)
        for data_dir in GATEWAY_DATA_DIRS:
            _prepare_data_dir(data_dir, desired_aliases)

        identities_payload: Dict[str, Dict] = {}
        automation_written: Dict[Path, Set[Path]] = {
            auto_dir: set() for auto_dir in AUTOMATION_DIRS
        }
        trust_certificates: Set[str] = set()

        for org in orgs:
            alias = org["alias"]
            orderer_host = org["orderer_host"] or "orderer"

            peers_section = {
                host: {
                    "url": f"grpcs://{host}:7051",
                    "tlsCACerts": {
                        "path": f"/workspace/data/{alias}/tls/{alias}-tlsca.pem"
                    },
                    "grpcOptions": {
                        "ssl-target-name-override": host,
                        "hostnameOverride": host,
                    },
                }
                for host in org["peer_hosts"]
            }
            channels_section = {
                channel: {
                    "orderers": [orderer_host],
                    "peers": {
                        host: {
                            "endorsingPeer": True,
                            "chaincodeQuery": True,
                            "ledgerQuery": True,
                            "eventSource": True,
                        }
                        for host in peers_section
                    },
                }
                for channel in channels
            }

            for data_dir in GATEWAY_DATA_DIRS:
                alias_dir = data_dir / alias
                shutil.rmtree(alias_dir, ignore_errors=True)
                (alias_dir / "msp" / "signcerts").mkdir(parents=True, exist_ok=True)
                (alias_dir / "msp" / "keystore").mkdir(parents=True, exist_ok=True)
                (alias_dir / "tls").mkdir(parents=True, exist_ok=True)

                admin_cert_dest = alias_dir / "msp" / "signcerts" / org["cert_path"].name
                shutil.copy(org["cert_path"], admin_cert_dest)
                os.chmod(admin_cert_dest, 0o644)

                key_dest = alias_dir / "msp" / "keystore" / org["key_path"].name
                shutil.copy(org["key_path"], key_dest)
                os.chmod(key_dest, 0o600)

                peer_tls_dest = None
                if org["tls_ca_path"]:
                    peer_tls_dest = alias_dir / "tls" / f"{alias}-tlsca.pem"
                    shutil.copy(org["tls_ca_path"], peer_tls_dest)
                    os.chmod(peer_tls_dest, 0o644)
                    cert_text = Path(org["tls_ca_path"]).read_text().strip()
                    if cert_text:
                        trust_certificates.add(cert_text)

                orderer_tls_dest = None
                if org["orderer_tls"] and Path(org["orderer_tls"]).exists():
                    cert_text = Path(org["orderer_tls"]).read_text().strip()
                    if cert_text:
                        trust_certificates.add(cert_text)
                    orderer_tls_dest = alias_dir / "tls" / f"{orderer_host}-tlsca.pem"
                    shutil.copy(org["orderer_tls"], orderer_tls_dest)
                    os.chmod(orderer_tls_dest, 0o644)
                elif peer_tls_dest is not None:
                    orderer_tls_dest = peer_tls_dest

                orderer_tls_path = (
                    f"/workspace/data/{alias}/tls/{orderer_host}-tlsca.pem"
                    if orderer_tls_dest is not None
                    else ""
                )

                connection_payload = {
                    "name": f"{alias}-network",
                    "version": "1.0.0",
                    "client": {
                        "organization": org["msp_id"],
                    },
                    "organizations": {
                        org["msp_id"]: {
                            "mspid": org["msp_id"],
                            "peers": list(peers_section.keys()),
                            "certificateAuthorities": [],
                        }
                    },
                    "peers": peers_section,
                    "orderers": {
                        orderer_host: {
                            "url": f"grpcs://{orderer_host}:7050",
                            "tlsCACerts": {"path": orderer_tls_path},
                            "grpcOptions": {
                                "ssl-target-name-override": orderer_host,
                                "hostnameOverride": orderer_host,
                            },
                        }
                    },
                    "channels": channels_section,
                }
                connection_path = data_dir / f"connection-{alias}.json"
                _write_json(connection_path, connection_payload)

            cert_filename = org["cert_path"].name
            key_filename = org["key_path"].name
            identity_entry = {
                "mspId": org["msp_id"],
                "ccpPath": f"/workspace/data/connection-{alias}.json",
                "certPath": f"/workspace/data/{alias}/msp/signcerts/{cert_filename}",
                "keyPath": f"/workspace/data/{alias}/msp/keystore/{key_filename}",
                "channels": [channel.lower() for channel in channels] or [],
                "discoveryAsLocalhost": False,
            }
            identities_payload[alias] = identity_entry

            for auto_dir in AUTOMATION_DIRS:
                automation_path = auto_dir / f"identities.{alias}.json"
                _write_json(automation_path, {alias: identity_entry})
                automation_written[auto_dir].add(automation_path)

        for auto_dir, written in automation_written.items():
            for file in auto_dir.glob("identities.*.json"):
                if file not in written:
                    file.unlink(missing_ok=True)

        combined = dict(identities_payload)
        if combined:
            first_key = sorted(combined.keys())[0]
            combined["default"] = combined[first_key]

        for data_dir in GATEWAY_DATA_DIRS:
            identities_path = _identities_path(data_dir)
            if combined:
                _write_json(identities_path, combined)
                os.utime(identities_path, None)
            else:
                _write_json(identities_path, {})

            trust_path = _trust_bundle_path(data_dir)
            if trust_certificates:
                trust_path.parent.mkdir(parents=True, exist_ok=True)
                trust_path.write_text(
                    "\n".join(sorted({cert for cert in trust_certificates if cert})) + "\n"
                )
                os.chmod(trust_path, 0o644)
            elif trust_path.exists():
                trust_path.unlink()

        LOG.info(
            "[GATEWAY_SYNC] Atualização concluída: %d organizações, %d canais.",
            len(identities_payload),
            len(channels),
        )
    except Exception:
        LOG.exception("[GATEWAY_SYNC] Falha ao sincronizar artefatos do gateway.")
