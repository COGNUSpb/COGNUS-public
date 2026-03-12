#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from automation.org_runtime_manifest import (
    OrgRuntimeManifestStateStore,
    load_org_runtime_manifest,
    validate_org_runtime_manifest_file,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lint do OrgRuntimeManifest (WP A2.1)."
    )
    parser.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help="Caminho do OrgRuntimeManifest (.json/.yaml/.yml).",
    )
    parser.add_argument(
        "--output",
        choices=("text", "json"),
        default="text",
        help="Formato da saida.",
    )
    parser.add_argument(
        "--normalized-out",
        type=Path,
        default=None,
        help="Arquivo para gravar o manifesto normalizado em JSON.",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Arquivo para gravar o relatorio completo em JSON.",
    )
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Permite migracao de schema do manifesto quando houver rota suportada.",
    )
    parser.add_argument(
        "--persist-root",
        type=Path,
        default=None,
        help=(
            "Diretorio raiz para persistir historico imutavel do manifesto "
            "(org-runtime-manifests)."
        ),
    )
    parser.add_argument(
        "--persist-actor",
        default="org-runtime-manifest-lint",
        help="Identificador do ator para trilha de persistencia.",
    )
    return parser


def _print_text_report(report: dict) -> None:
    errors = len(report.get("errors", []))
    warnings = len(report.get("warnings", []))
    hints = len(report.get("hints", []))
    print(f"valid={report.get('valid')} errors={errors} warnings={warnings} hints={hints}")
    print(
        "manifest="
        f"{report.get('manifest_name', '')}@{report.get('manifest_version', '')} "
        f"runtime={report.get('manifest_runtime_version', '')} "
        f"resolved={report.get('resolved_manifest_version', '')}"
    )
    print(
        "context="
        f"change_id={report.get('change_id', '')} "
        f"run_id={report.get('run_id', '')} "
        f"org_id={report.get('org_id', '')}"
    )
    print(f"manifest_fingerprint={report.get('manifest_fingerprint', '')}")
    print(f"fingerprint_sha256={report.get('fingerprint_sha256', '')}")

    for issue in report.get("issues", []):
        level = issue.get("level", "unknown").upper()
        code = issue.get("code", "")
        path = issue.get("path", "")
        message = issue.get("message", "")
        print(f"[{level}] {code} {path}: {message}")


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    report = validate_org_runtime_manifest_file(
        args.manifest,
        allow_schema_migration=args.migrate,
    ).to_dict()

    if args.output == "json":
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        _print_text_report(report)

    if args.normalized_out is not None:
        args.normalized_out.parent.mkdir(parents=True, exist_ok=True)
        args.normalized_out.write_text(
            json.dumps(
                {
                    "manifest_name": report.get("manifest_name", ""),
                    "manifest_version": report.get("manifest_version", ""),
                    "resolved_manifest_version": report.get("resolved_manifest_version", ""),
                    "manifest_runtime_version": report.get("manifest_runtime_version", ""),
                    "migration_applied": report.get("migration_applied", False),
                    "migrated_from_manifest_version": report.get(
                        "migrated_from_manifest_version",
                        "",
                    ),
                    "generated_at": report.get("generated_at", ""),
                    "change_id": report.get("change_id", ""),
                    "run_id": report.get("run_id", ""),
                    "org_id": report.get("org_id", ""),
                    "org_label": report.get("org_label", ""),
                    "domain": report.get("domain", ""),
                    "environment_profile_ref": report.get("environment_profile_ref", ""),
                    "source_blueprint_fingerprint": report.get("source_blueprint_fingerprint", ""),
                    "source_blueprint_version": report.get("source_blueprint_version", ""),
                    "orchestrator_context": report.get("orchestrator_context", ""),
                    "source_blueprint_scope": report.get("normalized_source_blueprint_scope", {}),
                    "org_identity": report.get("normalized_org_identity", {}),
                    "hosts": report.get("normalized_hosts", []),
                    "components": report.get("normalized_components", []),
                    "chaincode_runtimes": report.get("normalized_chaincode_runtimes", []),
                    "manifest_fingerprint": report.get("manifest_fingerprint", ""),
                    "fingerprint_sha256": report.get("fingerprint_sha256", ""),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    if args.report_out is not None:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(
            json.dumps(report, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    if args.persist_root is not None:
        store = OrgRuntimeManifestStateStore(args.persist_root)
        manifest_payload = load_org_runtime_manifest(args.manifest)
        persistence = store.persist_manifest(
            manifest_payload,
            allow_schema_migration=args.migrate,
            actor=args.persist_actor,
        )
        persistence_payload = persistence.to_dict()
        if args.output == "json":
            print(
                json.dumps(
                    {"persistence": persistence_payload},
                    indent=2,
                    ensure_ascii=False,
                )
            )
        else:
            print(
                "persisted="
                f"manifest_fingerprint={persistence_payload.get('manifest_fingerprint', '')} "
                f"manifest_path={persistence_payload.get('manifest_path', '')} "
                f"history_path={persistence_payload.get('history_path', '')}"
            )

    return 0 if report.get("valid") else 1


if __name__ == "__main__":
    sys.exit(main())
