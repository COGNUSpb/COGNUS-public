#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from automation.blueprint_schema import validate_blueprint_file


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lint do blueprint versionado (WP A1.1: orgs[] e channels[])."
    )
    parser.add_argument(
        "--blueprint",
        required=True,
        type=Path,
        help="Caminho do blueprint (.json/.yaml/.yml)",
    )
    parser.add_argument(
        "--output",
        choices=("text", "json"),
        default="text",
        help="Formato de saída do relatório.",
    )
    parser.add_argument(
        "--normalized-out",
        type=Path,
        default=None,
        help="Arquivo para gravar blueprint normalizado (orgs + channels) em JSON.",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Arquivo para gravar o relatório completo do lint em JSON.",
    )
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Permite migração de schema quando existir rotina suportada.",
    )
    return parser


def _print_text_report(report: dict) -> None:
    issues = report["issues"]
    errors = sum(1 for issue in issues if issue["level"] == "error")
    warnings = sum(1 for issue in issues if issue["level"] == "warning")
    hints = sum(1 for issue in issues if issue["level"] == "hint")
    schema_name = report.get("schema_name", "")
    schema_version = report.get("schema_version", "")
    blueprint_version = report.get("blueprint_version", "")
    fingerprint = report.get("fingerprint_sha256", "")
    current_schema = report.get("current_schema_version", "")
    resolved_schema = report.get("resolved_schema_version", "")
    migration_applied = report.get("migration_applied", False)
    migrated_from = report.get("migrated_from_schema_version", "")
    print(f"valid={report['valid']} errors={errors} warnings={warnings} hints={hints}")
    if schema_name or schema_version or blueprint_version:
        print(
            f"schema={schema_name}@{schema_version} blueprint_version={blueprint_version}"
        )
    if current_schema:
        print(
            f"schema_runtime={current_schema} resolved_schema_version={resolved_schema or schema_version}"
        )
    if migration_applied:
        print(f"schema_migration={migrated_from}->{resolved_schema}")
    if fingerprint:
        print(f"fingerprint_sha256={fingerprint}")
    for issue in issues:
        print(
            f"[{issue['level'].upper()}] {issue['code']} {issue['path']}: {issue['message']}"
        )


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    report = validate_blueprint_file(
        args.blueprint,
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
                    "orgs": report["normalized_orgs"],
                    "channels": report.get("normalized_channels", []),
                    "nodes": report.get("normalized_nodes", []),
                    "policies": report.get("normalized_policies", []),
                    "environment_profile": report.get("normalized_environment_profile", {}),
                    "identity_baseline": report.get("normalized_identity_baseline", {}),
                    "fingerprint_sha256": report.get("fingerprint_sha256", ""),
                    "current_schema_version": report.get("current_schema_version", ""),
                    "resolved_schema_version": report.get("resolved_schema_version", ""),
                    "migration_applied": report.get("migration_applied", False),
                    "migrated_from_schema_version": report.get(
                        "migrated_from_schema_version", ""
                    ),
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

    return 0 if report["valid"] else 1


if __name__ == "__main__":
    sys.exit(main())
