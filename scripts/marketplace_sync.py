#!/usr/bin/env python3
"""
Descobre automaticamente canais/chaincodes disponíveis via ccapi e gera os
artefatos do marketplace (manifesto + templates dinâmicos) a partir das
respostas `getSchema`, `getTx`, `getEvents` e `getHeader`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import error, request


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASE_URL = os.environ.get("MARKETPLACE_BASE_URL", "http://localhost:8085/api")
IDENTITY_CONFIG = ROOT / "chaincode-gateway" / "data" / "identities.json"
CHAINCODE_STORAGE = ROOT / "cello-v2" / "cello-storage" / "chaincode"
MARKETPLACE_DIR = ROOT / "marketplace"


class MarketplaceSyncError(RuntimeError):
    """Erro fatal ao sincronizar marketplace."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza manifestos/templates do marketplace consultando o ccapi.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Canal específico a ser sincronizado (pode ser informado múltiplas vezes).",
    )
    parser.add_argument(
        "--chaincode",
        action="append",
        default=[],
        help="Chaincode específico a ser sincronizado (pode ser informado múltiplas vezes).",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="URL base do gateway ccapi.",
    )
    parser.add_argument(
        "--token",
        help="Token JWT para chamadas autenticadas. Se omitido, tenta sem autenticação.",
    )
    parser.add_argument(
        "--org",
        help="Organização utilizada nas chamadas (?org=<org>). Se omitida, gateway decide automaticamente.",
    )
    parser.add_argument(
        "--identity-config",
        default=str(IDENTITY_CONFIG),
        help="Arquivo identities.json para descoberta de canais.",
    )
    parser.add_argument(
        "--chaincode-dir",
        default=str(CHAINCODE_STORAGE),
        help="Diretório onde o Cello armazena pacotes de chaincode (para inferir nomes).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(MARKETPLACE_DIR),
        help="Diretório destino dos arquivos gerados.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Timeout (segundos) das chamadas HTTP ao gateway.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mostra logs detalhados durante a execução.",
    )
    return parser.parse_args()


def log(message: str, *, verbose: bool = True) -> None:
    if verbose:
        print(message)


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        raise MarketplaceSyncError(f"Arquivo JSON inválido: {path}") from exc


def discover_channels(
    explicit_channels: Sequence[str],
    identity_file: Path,
) -> List[str]:
    if explicit_channels:
        return sorted({c for channel in explicit_channels for c in channel.split(",") if c})

    data = read_json_file(identity_file)
    channels: set[str] = set()
    if isinstance(data, dict):
        for entry in data.values():
            if isinstance(entry, dict):
                for channel in entry.get("channels", []) or []:
                    if isinstance(channel, str) and channel.strip():
                        channels.add(channel.strip())

    if not channels:
        raise MarketplaceSyncError(
            "Nenhum canal encontrado. Informe --channel ou preencha chaincode-gateway/data/identities.json."
        )
    return sorted(channels)


def discover_chaincodes(
    explicit_chaincodes: Sequence[str],
    chaincode_dir: Path,
) -> List[str]:
    if explicit_chaincodes:
        return sorted({cc for item in explicit_chaincodes for cc in item.split(",") if cc})

    discovered: set[str] = set()
    if chaincode_dir.exists():
        for entry in chaincode_dir.iterdir():
            name = entry.name.split("_", 1)[0].strip()
            if name:
                discovered.add(name)

    if not discovered:
        raise MarketplaceSyncError(
            "Nenhum chaincode encontrado. Informe --chaincode ou garanta acesso ao diretório de packages."
        )
    return sorted(discovered)


def http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int,
) -> Any:
    data: Optional[bytes] = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = request.Request(url=url, method=method.upper(), data=data)
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "ignore")
        raise MarketplaceSyncError(
            f"{method.upper()} {url} -> HTTP {exc.code}: {detail}"
        ) from exc
    except error.URLError as exc:
        raise MarketplaceSyncError(f"{method.upper()} {url} falhou: {exc.reason}") from exc

    if not payload:
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError:
        return payload.decode("utf-8")


def gateway_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"JWT {token}"
    return headers


def fetch_schema(
    base_url: str,
    channel: str,
    chaincode: str,
    *,
    headers: Dict[str, str],
    org: Optional[str],
    timeout: int,
) -> List[Dict[str, Any]]:
    suffix = f"?org={org}" if org else ""
    url = f"{base_url}/{channel}/{chaincode}/query/getSchema{suffix}"
    response = http_request("POST", url, headers=headers, body={}, timeout=timeout)

    if isinstance(response, dict) and "result" in response:
        result = response["result"]
    else:
        result = response

    if not isinstance(result, list):
        raise MarketplaceSyncError(f"Resposta inválida de getSchema ({channel}/{chaincode})")
    return result


def fetch_header(
    base_url: str,
    channel: str,
    chaincode: str,
    *,
    headers: Dict[str, str],
    org: Optional[str],
    timeout: int,
) -> Optional[Dict[str, Any]]:
    suffix = f"?org={org}" if org else ""
    url = f"{base_url}/{channel}/{chaincode}/query/getHeader/{suffix}"
    try:
        response = http_request("POST", url, headers=headers, body={}, timeout=timeout)
        if isinstance(response, dict):
            return response
    except MarketplaceSyncError:
        return None
    return None


def fetch_event_list(
    base_url: str,
    channel: str,
    chaincode: str,
    *,
    headers: Dict[str, str],
    org: Optional[str],
    timeout: int,
) -> List[Dict[str, Any]]:
    suffix = f"?org={org}" if org else ""
    url = f"{base_url}/{channel}/{chaincode}/query/getEvents{suffix}"
    response = http_request("GET", url, headers=headers, timeout=timeout)
    if isinstance(response, dict) and "result" in response:
        result = response["result"]
    else:
        result = response
    if isinstance(result, list):
        return result
    return []


def fetch_transaction_list(
    base_url: str,
    channel: str,
    chaincode: str,
    *,
    headers: Dict[str, str],
    org: Optional[str],
    timeout: int,
) -> List[Dict[str, Any]]:
    suffix = f"?org={org}" if org else ""
    url = f"{base_url}/{channel}/{chaincode}/query/getTx{suffix}"
    response = http_request("GET", url, headers=headers, timeout=timeout)
    if isinstance(response, dict) and "result" in response:
        result = response["result"]
    else:
        result = response
    if isinstance(result, list):
        return result
    raise MarketplaceSyncError(f"Lista de transações inválida para {channel}/{chaincode}")


def fetch_transaction_definition(
    base_url: str,
    channel: str,
    chaincode: str,
    *,
    tag: str,
    headers: Dict[str, str],
    org: Optional[str],
    timeout: int,
) -> Dict[str, Any]:
    suffix = f"?org={org}" if org else ""
    url = f"{base_url}/{channel}/{chaincode}/query/getTx{suffix}"
    response = http_request(
        "POST",
        url,
        headers=headers,
        body={"txName": tag},
        timeout=timeout,
    )
    if not isinstance(response, dict):
        raise MarketplaceSyncError(f"Definição inválida para transação {tag} ({channel}/{chaincode})")
    return response


def compute_schema_hash(
    assets: List[Dict[str, Any]],
    transactions: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
) -> str:
    canonical = json.dumps(
        {"assets": assets, "transactions": transactions, "events": events},
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def build_templates(
    channel: str,
    chaincode: str,
    schema_version: str,
    schema_hash: str,
    generated_at: str,
    assets: List[Dict[str, Any]],
    transactions: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    def infer_transformation(data_type: Optional[str]) -> str:
        if not data_type:
            return "identity"
        if data_type.startswith("[]"):
            return f"array<{infer_transformation(data_type[2:])}>"
        if data_type == "datetime":
            return "datetime::rfc3339"
        if data_type in {"number", "integer"}:
            return "numeric::identity"
        if "sha256" in data_type:
            return "digest::sha256"
        if data_type.startswith("->"):
            return f"reference::{data_type[2:]}"
        if data_type.startswith("@"):
            return f"cc-tools::{data_type[1:]}"
        return "identity"

    asset_templates: List[Dict[str, Any]] = []
    for asset in assets:
        props = asset.get("props") or []
        fields = []
        relationships = []
        for prop in props:
            data_type = prop.get("dataType")
            fields.append(
                {
                    "tag": prop.get("tag"),
                    "label": prop.get("label"),
                    "description": prop.get("description"),
                    "dataType": data_type,
                    "required": bool(prop.get("required") or prop.get("isKey")),
                    "isKey": prop.get("isKey", False),
                    "readOnly": prop.get("readOnly", False),
                    "defaultValue": prop.get("defaultValue"),
                    "writers": prop.get("writers"),
                    "transformation": infer_transformation(data_type),
                }
            )
            if isinstance(data_type, str):
                base_type = data_type.lstrip("[]")
                if base_type.startswith("->"):
                    relationships.append(
                        {
                            "property": prop.get("tag"),
                            "targetAsset": base_type[2:],
                            "cardinality": "many" if data_type.startswith("[]") else "one",
                        }
                    )
        asset_templates.append(
            {
                "tag": asset.get("tag"),
                "label": asset.get("label"),
                "description": asset.get("description"),
                "dynamic": asset.get("dynamic"),
                "readers": asset.get("readers"),
                "fields": fields,
                "relationships": relationships,
            }
        )

    tx_templates = []
    for tx in transactions:
        tx_templates.append(
            {
                "tag": tx.get("tag"),
                "label": tx.get("label"),
                "description": tx.get("description"),
                "method": tx.get("method"),
                "metaTx": tx.get("metaTx"),
                "readOnly": tx.get("readOnly"),
                "callers": tx.get("callers"),
                "args": tx.get("args"),
            }
        )

    event_templates = []
    for event in events:
        event_templates.append(
            {
                "tag": event.get("tag"),
                "label": event.get("label"),
                "description": event.get("description"),
                "baseLog": event.get("baseLog"),
                "type": event.get("type"),
                "receivers": event.get("receivers"),
                "transaction": event.get("transaction"),
                "channel": event.get("channel"),
                "chaincode": event.get("chaincode"),
                "readOnly": event.get("readOnly"),
            }
        )

    return {
        "generatedAt": generated_at,
        "schemaVersion": schema_version,
        "schemaHash": schema_hash,
        "channel": channel,
        "chaincode": chaincode,
        "assets": asset_templates,
        "transactions": tx_templates,
        "events": event_templates,
    }


def write_if_changed(path: Path, content: str) -> bool:
    if path.exists():
        current = path.read_text(encoding="utf-8")
        if current == content:
            return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def sync_target(
    base_url: str,
    channel: str,
    chaincode: str,
    *,
    headers: Dict[str, str],
    org: Optional[str],
    timeout: int,
    output_dir: Path,
    verbose: bool,
) -> None:
    schema = fetch_schema(
        base_url, channel, chaincode, headers=headers, org=org, timeout=timeout
    )
    tx_list = fetch_transaction_list(
        base_url, channel, chaincode, headers=headers, org=org, timeout=timeout
    )
    tx_definitions = []
    for entry in tx_list:
        tag = entry.get("tag")
        if not tag:
            continue
        try:
            tx_definitions.append(
                fetch_transaction_definition(
                    base_url,
                    channel,
                    chaincode,
                    tag=tag,
                    headers=headers,
                    org=org,
                    timeout=timeout,
                )
            )
        except MarketplaceSyncError as exc:
            log(f"Aviso: falha ao buscar definição de {tag} ({exc})", verbose=verbose)
    events = fetch_event_list(
        base_url, channel, chaincode, headers=headers, org=org, timeout=timeout
    )
    header = fetch_header(
        base_url, channel, chaincode, headers=headers, org=org, timeout=timeout
    )

    generated_at = header.get("timestamp") if isinstance(header, dict) else None
    if not generated_at:
        from datetime import datetime

        generated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    schema_hash = compute_schema_hash(schema, tx_definitions, events)
    schema_version = schema_hash[:12]

    manifest = {
        "generatedAt": generated_at,
        "schemaVersion": schema_version,
        "schemaHash": schema_hash,
        "channel": channel,
        "chaincode": chaincode,
        "baseUrl": base_url,
        "assets": schema,
        "transactions": tx_definitions,
        "events": events,
        "header": header,
    }

    templates = build_templates(
        channel,
        chaincode,
        schema_version,
        schema_hash,
        generated_at,
        schema,
        tx_definitions,
        events,
    )

    manifest_path = output_dir / f"{channel}__{chaincode}.json"
    templates_path = output_dir / f"{channel}__{chaincode}_templates.json"

    manifest_changed = write_if_changed(
        manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    )
    templates_changed = write_if_changed(
        templates_path, json.dumps(templates, ensure_ascii=False, indent=2) + "\n"
    )

    status_parts = []
    status_parts.append("manifest atualizado" if manifest_changed else "manifest sem alterações")
    status_parts.append("templates atualizados" if templates_changed else "templates sem alterações")
    log(f"[{channel}/{chaincode}] " + " / ".join(status_parts), verbose=verbose)


def main() -> None:
    args = parse_args()

    channels = discover_channels(args.channel, Path(args.identity_config))
    chaincodes = discover_chaincodes(args.chaincode, Path(args.chaincode_dir))
    headers = gateway_headers(args.token)
    output_dir = Path(args.output_dir)
    ensure_output_dir(output_dir)

    targets: List[Tuple[str, str]] = []
    # Distribui chaincodes por canal tentando detectar combinações válidas.
    if args.channel and not args.chaincode:
        # Quando usuário informa canal explicitamente, iteramos todos os chaincodes descobertos.
        for channel in channels:
            for chaincode in chaincodes:
                targets.append((channel, chaincode))
    elif args.chaincode and not args.channel:
        for chaincode in chaincodes:
            for channel in channels:
                targets.append((channel, chaincode))
    else:
        # Caso padrão: usar todos os canais e chaincodes descobertos.
        for channel in channels:
            for chaincode in chaincodes:
                targets.append((channel, chaincode))

    errors: Dict[Tuple[str, str], str] = {}
    skipped: Dict[Tuple[str, str], str] = {}
    processed = 0
    for channel, chaincode in targets:
        try:
            sync_target(
                args.base_url,
                channel,
                chaincode,
                headers=headers,
                org=args.org,
                timeout=args.timeout,
                output_dir=output_dir,
                verbose=args.verbose,
            )
            processed += 1
        except MarketplaceSyncError as exc:
            message = str(exc)
            lower = message.lower()
            if (
                "http 404" in lower
                or "does not exist" in lower
                or "access denied" in lower
                or "permission denied" in lower
                or "discoveryservice" in lower
            ):
                skipped[(channel, chaincode)] = message
                log(f"[{channel}/{chaincode}] ignorado: {message}", verbose=True)
            else:
                errors[(channel, chaincode)] = message
                log(f"[{channel}/{chaincode}] falhou: {message}", verbose=True)

    if errors:
        msg_lines = [
            f"- {channel}/{chaincode}: {reason}" for (channel, chaincode), reason in errors.items()
        ]
        raise MarketplaceSyncError(
            f"{len(errors)} combinação(ões) falharam:\n" + "\n".join(msg_lines)
        )
    if processed == 0:
        if skipped:
            msg_lines = [
                f"- {channel}/{chaincode}: {reason}" for (channel, chaincode), reason in skipped.items()
            ]
            raise MarketplaceSyncError(
                "Nenhuma combinação válida encontrada. Verifique se ao menos um canal/chaincode está publicado.\n"
                + "\n".join(msg_lines)
            )
        raise MarketplaceSyncError("Nenhum canal/chaincode processado; verifique a configuração.")


if __name__ == "__main__":
    try:
        main()
    except MarketplaceSyncError as exc:
        print(f"marketplace_sync: {exc}", file=sys.stderr)
        sys.exit(1)
