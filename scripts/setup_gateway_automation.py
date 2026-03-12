#!/usr/bin/env python3
"""
Automate Fabric chaincode lifecycle and gateway configuration for Cello-generated orgs.

The script orchestrates the following steps:
  1. Package the requested chaincode inside the target peer container.
  2. Install/approve/commit the chaincode for the organization.
  3. Sync gateway artifacts (connection profile, identities.json, certs, wallet).
  4. Rebuild and restart the chaincode gateway container.

Example usage:
  python3 scripts/setup_gateway_automation.py \
    --org-root cello-storage/infufg3.cello.local \
    --alias infufg \
    --channel stcs-channel6 \
    --chaincode-name fakenews-cc \
    --chaincode-version 1.0 \
    --sequence 3
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = REPO_ROOT / "chaincode-gateway" / "data"
DEFAULT_IDENTITY_CONFIG = DEFAULT_DATA_DIR / "identities.json"
DEFAULT_WALLET_DIR = REPO_ROOT / "chaincode-gateway" / ".gateway-wallet"
DEFAULT_COMPOSE_FILE = REPO_ROOT / "chaincode-gateway" / "docker-compose.yaml"
WORKSPACE_DATA_ROOT = Path("/workspace/data")
TLS_TRUST_BUNDLE = DEFAULT_DATA_DIR / "fabric-tlscas.pem"


class SetupError(Exception):
    """Raised when the automation encounters a fatal issue."""


def info(message: str) -> None:
    print(f"[INFO] {message}")


def warn(message: str) -> None:
    print(f"[WARN] {message}")


def debug(message: str) -> None:
    if os.environ.get("AUTOMATION_DEBUG"):
        print(f"[DEBUG] {message}")


def run_command(
    command: Iterable[str],
    *,
    capture_output: bool = False,
    text: bool = True,
    check: bool = True,
    cwd: Optional[Path] = None,
) -> subprocess.CompletedProcess:
    command_list = list(command)
    debug(f"Running command: {' '.join(shlex.quote(c) for c in command_list)}")
    return subprocess.run(
        command_list,
        cwd=cwd,
        capture_output=capture_output,
        text=text,
        check=check,
    )


def docker_exec(
    container: str,
    script: str,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    """Execute a bash script inside the container while preserving go/bin in PATH."""
    wrapped = (
        "set -euo pipefail; "
        "export PATH=\"$PATH:/go/bin\"; "
        f"{script}"
    )
    cmd = ["docker", "exec", container, "bash", "-lc", wrapped]
    return run_command(cmd, capture_output=capture_output)


def read_container_file(container: str, file_path: str) -> str:
    result = run_command(
        ["docker", "exec", container, "cat", file_path],
        capture_output=True,
    )
    return result.stdout


def list_container_dir(container: str, directory: str) -> Iterable[str]:
    result = run_command(
        ["docker", "exec", container, "ls", "-1", directory],
        capture_output=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def find_chaincode_dir(chaincode_root: Path, name: str, version: str) -> Path:
    prefix = f"{name}_{version}:"
    candidates = [
        entry
        for entry in chaincode_root.iterdir()
        if entry.is_dir() and entry.name.startswith(prefix)
    ]
    if not candidates:
        raise SetupError(
            f"Unable to locate chaincode directory with prefix '{prefix}' in {chaincode_root}"
        )
    if len(candidates) > 1:
        warn(
            f"Multiple chaincode directories found for prefix '{prefix}'. "
            f"Using the first one: {candidates[0].name}"
        )
    return candidates[0]


def package_chaincode(
    container: str,
    chaincode_dir_name: str,
    tarball_name: str,
) -> None:
    src = shlex.quote(f"/opt/cello/chaincode/{chaincode_dir_name}")
    dest = shlex.quote(f"/opt/cello/chaincode/{tarball_name}")
    script = (
        f"tar -czf {dest} --exclude='*.tar.gz' -C {src} ."
    )
    info(f"Packaging chaincode into {tarball_name} inside container {container}")
    docker_exec(container, script)


def run_peer_lifecycle(
    container: str,
    env_vars: Dict[str, str],
    command: str,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    exports = " ".join(f"export {key}={shlex.quote(value)};" for key, value in env_vars.items())
    script = f"{exports} {command}"
    return docker_exec(container, script, capture_output=capture_output)


def extract_package_id(query_output: str, label: str) -> str:
    pattern = re.compile(r"Package ID: (.*?), Label: (.+)")
    for line in query_output.splitlines():
        match = pattern.search(line)
        if match and match.group(2) == label:
            return match.group(1)
    raise SetupError(f"Package ID for label '{label}' not found in peer output.")


def derive_org_metadata(org_root: Path) -> Dict[str, str]:
    crypto_config = org_root / "crypto-config.yaml"
    if not crypto_config.exists():
        raise SetupError(f"{crypto_config} not found. Ensure --org-root points to the Cello output.")
    document = yaml.safe_load(crypto_config.read_text())
    peer_orgs = document.get("PeerOrgs") or []
    if not peer_orgs:
        raise SetupError(f"No peer organizations defined in {crypto_config}")
    peer_org = peer_orgs[0]
    name = peer_org.get("Name")
    domain = peer_org.get("Domain")
    if not name or not domain:
        raise SetupError("Missing Name or Domain in peer organization definition.")
    orderer_orgs = document.get("OrdererOrgs") or []
    orderer_spec = orderer_orgs[0]["Specs"][0] if orderer_orgs and orderer_orgs[0].get("Specs") else None
    orderer_host = f"{orderer_spec['Hostname']}.{orderer_orgs[0]['Domain']}" if orderer_spec else "orderer.cello.local"
    return {
        "peer_name": name,
        "peer_domain": domain,
        "msp_id": f"{name}MSP",
        "orderer_host": orderer_host,
    }


def resolve_peer_and_orderer_dirs(org_root: Path, peer_domain: str) -> Dict[str, Path]:
    base_crypto = org_root / "crypto-config"
    peer_org_path = base_crypto / "peerOrganizations" / peer_domain
    if not peer_org_path.exists():
        raise SetupError(f"Peer org path {peer_org_path} does not exist.")
    peers_dir = peer_org_path / "peers"
    peer_entries = [entry for entry in peers_dir.iterdir() if entry.is_dir()]
    if not peer_entries:
        raise SetupError(f"No peers found in {peers_dir}")
    orderer_org_path = base_crypto / "ordererOrganizations" / "cello.local"
    orderers_dir = orderer_org_path / "orderers"
    orderer_entries = [entry for entry in orderers_dir.iterdir() if entry.is_dir()]
    if not orderer_entries:
        raise SetupError(f"No orderers found in {orderers_dir}")
    return {
        "peer_dir": peer_org_path,
        "peer_host": peer_entries[0].name,
        "orderer_dir": orderer_org_path,
        "orderer_host": orderer_entries[0].name,
    }


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def clear_directory(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return
    for entry in path.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def write_file(path: Path, content: str, mode: int = 0o644) -> None:
    ensure_directory(path.parent)
    path.write_text(content)
    os.chmod(path, mode)


def append_unique_certificate(bundle: Path, certificate_pem: str) -> None:
    normalized = certificate_pem.strip()
    if not normalized:
        raise SetupError("TLS certificate content is empty.")
    if bundle.exists():
        existing = bundle.read_text()
        if normalized in existing:
            info(f"TLS certificate already present in {bundle.name}, skipping append.")
            return
        if not existing.endswith("\n"):
            existing += "\n"
        bundle.write_text(existing + normalized + "\n")
    else:
        bundle.write_text(normalized + "\n")
    os.chmod(bundle, 0o644)


def to_workspace_path(data_dir: Path, target: Path) -> Path:
    data_dir_resolved = data_dir.resolve()
    target_resolved = target.resolve(strict=False)
    try:
        relative = target_resolved.relative_to(data_dir_resolved)
    except ValueError as exc:
        raise SetupError(
            f"{target_resolved} is outside of the gateway data directory {data_dir_resolved}. "
            "Place artifacts inside the data directory or pass --data-dir accordingly."
        ) from exc
    return WORKSPACE_DATA_ROOT / relative


def update_connection_profile(
    path: Path,
    alias: str,
    msp_id: str,
    peer_host: str,
    peer_port: int,
    peer_tls_path: str,
    orderer_host: str,
    orderer_port: int,
    orderer_tls_path: str,
) -> None:
    info(f"Updating connection profile at {path}")
    profile = {
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
                "tlsCACerts": {"path": peer_tls_path},
                "grpcOptions": {
                    "ssl-target-name-override": peer_host,
                    "hostnameOverride": peer_host,
                },
            }
        },
        "orderers": {
            orderer_host: {
                "url": f"grpcs://{orderer_host}:{orderer_port}",
                "tlsCACerts": {"path": orderer_tls_path},
                "grpcOptions": {
                    "ssl-target-name-override": orderer_host,
                    "hostnameOverride": orderer_host,
                },
            }
        },
    }
    write_file(path, json.dumps(profile, indent=2) + "\n")


def update_identities(
    path: Path,
    alias: str,
    msp_id: str,
    ccp_path: Path,
    cert_path: Path,
    key_path: Path,
    channels: Iterable[str],
) -> None:
    info(f"Updating identities configuration at {path}")
    data = {}
    if path.exists():
        data = json.loads(path.read_text())
    data[alias] = {
        "mspId": msp_id,
        "ccpPath": str(ccp_path),
        "certPath": str(cert_path),
        "keyPath": str(key_path),
        "channels": list(channels),
        "discoveryAsLocalhost": False,
    }
    write_file(path, json.dumps(data, indent=2) + "\n")


def clear_wallet(wallet_dir: Path) -> None:
    if wallet_dir.exists():
        info(f"Removing wallet directory {wallet_dir}")
        shutil.rmtree(wallet_dir)
    wallet_dir.mkdir(parents=True, exist_ok=True)


def rebuild_gateway(compose_file: Path) -> None:
    info("Rebuilding chaincode-gateway container")
    run_command(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "up",
            "-d",
            "--build",
            "--force-recreate",
        ]
    )
    run_command(["docker", "logs", "chaincode-gateway", "--tail", "20"])


def configure_gateway_artifacts(
    *,
    container: str,
    alias: str,
    msp_id: str,
    channels: Iterable[str],
    peer_domain: str,
    peer_host: str,
    peer_port: int,
    orderer_host: str,
    orderer_port: int,
    data_dir: Path,
    connection_profile: Path,
    identities_path: Path,
) -> None:
    alias_dir = data_dir / alias
    msp_dir = alias_dir / "msp"
    signcerts_dir = msp_dir / "signcerts"
    keystore_dir = msp_dir / "keystore"
    tls_dir = alias_dir / "tls"

    clear_directory(signcerts_dir)
    clear_directory(keystore_dir)
    clear_directory(tls_dir)

    cert_dir_container = (
        f"/opt/cello/{peer_domain}/crypto-config/peerOrganizations/{peer_domain}/users"
        f"/Admin@{peer_domain}/msp/signcerts"
    )
    cert_files = sorted(list_container_dir(container, cert_dir_container))
    if not cert_files:
        raise SetupError(f"No certs found in {cert_dir_container}")
    cert_filename = cert_files[0]
    cert_content = read_container_file(container, f"{cert_dir_container}/{cert_filename}")
    local_cert = signcerts_dir / cert_filename
    write_file(local_cert, cert_content, mode=0o644)

    keystore_dir_container = (
        f"/opt/cello/{peer_domain}/crypto-config/peerOrganizations/{peer_domain}/users"
        f"/Admin@{peer_domain}/msp/keystore"
    )
    key_files = sorted(list_container_dir(container, keystore_dir_container))
    if not key_files:
        raise SetupError(f"No key files found in {keystore_dir_container}")
    key_filename = key_files[0]
    key_content = read_container_file(container, f"{keystore_dir_container}/{key_filename}")
    local_key = keystore_dir / key_filename
    write_file(local_key, key_content, mode=0o600)

    peer_tls_container_path = (
        f"/opt/cello/{peer_domain}/crypto-config/peerOrganizations/{peer_domain}/tlsca/"
        f"tlsca.{peer_domain}-cert.pem"
    )
    peer_tls_content = read_container_file(container, peer_tls_container_path)
    peer_tls_local = tls_dir / f"{peer_host}-tlsca.pem"
    write_file(peer_tls_local, peer_tls_content, mode=0o644)
    append_unique_certificate(TLS_TRUST_BUNDLE, peer_tls_content)

    orderer_tls_container_path = (
        f"/opt/cello/{peer_domain}/crypto-config/ordererOrganizations/cello.local/tlsca/"
        "tlsca.cello.local-cert.pem"
    )
    orderer_tls_content = read_container_file(container, orderer_tls_container_path)
    orderer_tls_local = tls_dir / f"{orderer_host}-tlsca.pem"
    write_file(orderer_tls_local, orderer_tls_content, mode=0o644)
    append_unique_certificate(TLS_TRUST_BUNDLE, orderer_tls_content)

    peer_tls_workspace_path = to_workspace_path(data_dir, peer_tls_local)
    orderer_tls_workspace_path = to_workspace_path(data_dir, orderer_tls_local)

    update_connection_profile(
        connection_profile,
        alias,
        msp_id,
        peer_host,
        peer_port,
        str(peer_tls_workspace_path),
        orderer_host,
        orderer_port,
        str(orderer_tls_workspace_path),
    )

    connection_profile_workspace_path = to_workspace_path(data_dir, connection_profile)
    local_cert_workspace_path = to_workspace_path(data_dir, local_cert)
    local_key_workspace_path = to_workspace_path(data_dir, local_key)

    update_identities(
        identities_path,
        alias,
        msp_id,
        connection_profile_workspace_path,
        local_cert_workspace_path,
        local_key_workspace_path,
        channels,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Automate Fabric gateway setup for a Cello organization.")
    parser.add_argument("--org-root", required=True, type=Path, help="Path to the Cello output directory (e.g. cello-storage/infufg2.cello.local)")
    parser.add_argument("--alias", required=True, help="Alias used inside identities.json and local data directory (e.g. infufg)")
    parser.add_argument("--channel", required=True, action="append", help="Channel to register. Repeat for multiple channels.")
    parser.add_argument("--chaincode-name", required=True, help="Chaincode name/label (e.g. fakenews-cc)")
    parser.add_argument("--chaincode-version", required=True, help="Chaincode version label (e.g. 1.0)")
    parser.add_argument("--sequence", required=True, type=int, help="Chaincode lifecycle sequence number")
    parser.add_argument("--peer-container", help="Docker container name for the peer. Defaults to the first peer discovered.")
    parser.add_argument("--peer-port", type=int, default=7051)
    parser.add_argument("--orderer-host", help="Orderer hostname (defaults to the first orderer discovered).")
    parser.add_argument("--orderer-port", type=int, default=7050)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="chaincode-gateway data directory.")
    parser.add_argument("--connection-profile", type=Path, help="Optional explicit path to the connection profile JSON.")
    parser.add_argument("--identities", type=Path, default=DEFAULT_IDENTITY_CONFIG, help="Path to identities.json.")
    parser.add_argument("--wallet-dir", type=Path, default=DEFAULT_WALLET_DIR, help="Path to the gateway wallet directory.")
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE, help="Docker compose file for the gateway.")
    parser.add_argument("--skip-lifecycle", action="store_true", help="Skip peer lifecycle (install/approve/commit)")
    parser.add_argument("--skip-compose", action="store_true", help="Skip docker compose rebuild.")
    args = parser.parse_args()

    org_root = args.org_root.resolve()
    if not org_root.exists():
        raise SetupError(f"Organization root {org_root} does not exist.")

    metadata = derive_org_metadata(org_root)
    directories = resolve_peer_and_orderer_dirs(org_root, metadata["peer_domain"])

    peer_host = directories["peer_host"]
    orderer_host = args.orderer_host or directories["orderer_host"]
    peer_container = args.peer_container or peer_host

    channels = args.channel
    chaincode_root = org_root.parent / "chaincode"
    chaincode_dir = find_chaincode_dir(chaincode_root, args.chaincode_name, args.chaincode_version)
    tarball_name = f"{args.chaincode_name}_{args.chaincode_version}.tar.gz"
    label = f"{args.chaincode_name}_{args.chaincode_version}"

    env_vars = {
        "CORE_PEER_TLS_ENABLED": "true",
        "CORE_PEER_LOCALMSPID": metadata["msp_id"],
        "CORE_PEER_MSPCONFIGPATH": (
            f"/opt/cello/{metadata['peer_domain']}/crypto-config/peerOrganizations/{metadata['peer_domain']}/users/"
            f"Admin@{metadata['peer_domain']}/msp"
        ),
        "CORE_PEER_ADDRESS": f"{peer_host}:{args.peer_port}",
        "CORE_PEER_TLS_ROOTCERT_FILE": "/etc/hyperledger/fabric/tls/ca.crt",
    }

    if not args.skip_lifecycle:
        package_chaincode(peer_container, chaincode_dir.name, tarball_name)

        info("Installing chaincode on peer")
        run_peer_lifecycle(
            peer_container,
            env_vars,
            f"peer lifecycle chaincode install /opt/cello/chaincode/{tarball_name}",
        )

        info("Querying installed chaincodes to fetch Package ID")
        query_output = run_peer_lifecycle(
            peer_container,
            env_vars,
            "peer lifecycle chaincode queryinstalled",
            capture_output=True,
        ).stdout
        package_id = extract_package_id(query_output, label)
        info(f"Detected package ID: {package_id}")

        approve_cmd = " ".join(
            [
                "peer lifecycle chaincode approveformyorg",
                f"-o {orderer_host}:{args.orderer_port}",
                f"--ordererTLSHostnameOverride {orderer_host}",
                "--tls",
                f"--cafile /opt/cello/{metadata['peer_domain']}/crypto-config/ordererOrganizations/cello.local/tlsca/tlsca.cello.local-cert.pem",
                f"-C {channels[0]}",
                f"-n {args.chaincode_name}",
                f"-v {args.chaincode_version}",
                f"--sequence {args.sequence}",
                f"--package-id {package_id}",
            ]
        )
        info("Approving chaincode for organization")
        run_peer_lifecycle(peer_container, env_vars, approve_cmd)

        commit_cmd = " ".join(
            [
                "peer lifecycle chaincode commit",
                f"-o {orderer_host}:{args.orderer_port}",
                f"--ordererTLSHostnameOverride {orderer_host}",
                "--tls",
                f"--cafile /opt/cello/{metadata['peer_domain']}/crypto-config/ordererOrganizations/cello.local/tlsca/tlsca.cello.local-cert.pem",
                f"-C {channels[0]}",
                f"-n {args.chaincode_name}",
                f"-v {args.chaincode_version}",
                f"--sequence {args.sequence}",
                f"--peerAddresses {peer_host}:{args.peer_port}",
                "--tlsRootCertFiles /etc/hyperledger/fabric/tls/ca.crt",
            ]
        )
        info("Committing chaincode to channel")
        run_peer_lifecycle(peer_container, env_vars, commit_cmd)

        info("Querying committed chaincodes for confirmation")
        run_peer_lifecycle(
            peer_container,
            env_vars,
            f"peer lifecycle chaincode querycommitted -C {channels[0]}",
        )

    data_dir = args.data_dir.resolve()
    connection_profile = (
        args.connection_profile.resolve()
        if args.connection_profile
        else data_dir / f"connection-{args.alias}.json"
    )
    identities_path = args.identities.resolve()

    configure_gateway_artifacts(
        container=peer_container,
        alias=args.alias,
        msp_id=metadata["msp_id"],
        channels=channels,
        peer_domain=metadata["peer_domain"],
        peer_host=peer_host,
        peer_port=args.peer_port,
        orderer_host=orderer_host,
        orderer_port=args.orderer_port,
        data_dir=data_dir,
        connection_profile=connection_profile,
        identities_path=identities_path,
    )

    clear_wallet(args.wallet_dir.resolve())

    if not args.skip_compose:
        rebuild_gateway(args.compose_file.resolve())

    info("Automation completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except SetupError as exc:
        warn(str(exc))
        sys.exit(1)
    except subprocess.CalledProcessError as exc:
        warn(f"Command failed with exit code {exc.returncode}: {' '.join(exc.cmd)}")
        if exc.stdout:
            print(exc.stdout)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        sys.exit(exc.returncode)
