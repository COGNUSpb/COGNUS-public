#
# SPDX-License-Identifier: Apache-2.0
#
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import sys
from tempfile import gettempdir
from threading import RLock
import uuid

from drf_yasg.utils import swagger_auto_schema
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from api.common import err, ok
from api.utils.common import with_common_response
from .serializers import (
    BlueprintLintRequestSerializer,
    BlueprintLintResponseSerializer,
    BlueprintPublishRequestSerializer,
    BlueprintPublishResponseSerializer,
    BlueprintVersionListResponseSerializer,
)

LOG = logging.getLogger(__name__)
BLUEPRINT_CONTRACT_VERSION = "a1.7-blueprint-api.v1"
BLUEPRINT_VERSIONS_STORE_FILE = (
    Path(gettempdir()) / "cognus_blueprint_versions_store.json"
)
BLUEPRINT_VERSIONS_STORE_LOCK = RLock()
BLUEPRINT_ACCESS_AUDIT_STORE_FILE = (
    Path(gettempdir()) / "cognus_blueprint_access_audit_store.json"
)
BLUEPRINT_ALLOWED_PUBLISH_ROLES = {"admin", "operator"}
BLUEPRINT_A2_2_MINIMUM_ARTIFACTS = [
    "provision-plan",
    "reconcile-report",
    "inventory-final",
    "stage-reports",
    "verify-report",
    "ssh-execution-log",
]


class _InlineBlueprintValidationResult:
    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return dict(self._payload)


def _inline_lint_issue(level, code, path, message):
    return {
        "level": level,
        "code": code,
        "path": path,
        "message": message,
    }


def _validate_blueprint_block_inline(blueprint, allow_schema_migration=False):
    errors = []
    warnings = []
    hints = []

    if not isinstance(blueprint, dict):
        errors.append(
            _inline_lint_issue(
                "error",
                "invalid_blueprint_root",
                "$",
                "Blueprint deve ser um objeto JSON no nível raiz.",
            )
        )
        normalized_blueprint = {}
    else:
        normalized_blueprint = blueprint

    required_blocks = {
        "network": dict,
        "orgs": list,
        "channels": list,
        "nodes": list,
        "policies": list,
        "environment_profile": dict,
    }

    for block_name, expected_type in required_blocks.items():
        value = normalized_blueprint.get(block_name)
        if value is None:
            errors.append(
                _inline_lint_issue(
                    "error",
                    "missing_required_block",
                    f"$.{block_name}",
                    f"Bloco obrigatório ausente: {block_name}.",
                )
            )
            continue

        if not isinstance(value, expected_type):
            expected_label = "objeto" if expected_type is dict else "lista"
            errors.append(
                _inline_lint_issue(
                    "error",
                    "invalid_block_type",
                    f"$.{block_name}",
                    f"Bloco {block_name} deve ser {expected_label}.",
                )
            )

    serialized_blueprint = json.dumps(
        normalized_blueprint,
        sort_keys=True,
        ensure_ascii=True,
        default=str,
    )
    fingerprint = hashlib.sha256(serialized_blueprint.encode("utf-8")).hexdigest()

    report = {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "hints": hints,
        "issues": [*errors, *warnings, *hints],
        "schema_name": str(normalized_blueprint.get("schema_name", "") or ""),
        "schema_version": str(normalized_blueprint.get("schema_version", "") or ""),
        "blueprint_version": str(
            normalized_blueprint.get("blueprint_version", "") or ""
        ),
        "created_at": str(normalized_blueprint.get("created_at", "") or ""),
        "updated_at": str(normalized_blueprint.get("updated_at", "") or ""),
        "current_schema_version": "1.0.0",
        "schema_runtime": "1.0.0",
        "resolved_schema_version": str(
            normalized_blueprint.get("schema_version", "1.0.0") or "1.0.0"
        ),
        "migration_applied": bool(allow_schema_migration and False),
        "migrated_from_schema_version": "",
        "fingerprint_sha256": fingerprint,
        "normalized_orgs": normalized_blueprint.get("orgs", [])
        if isinstance(normalized_blueprint.get("orgs"), list)
        else [],
        "normalized_channels": normalized_blueprint.get("channels", [])
        if isinstance(normalized_blueprint.get("channels"), list)
        else [],
        "normalized_nodes": normalized_blueprint.get("nodes", [])
        if isinstance(normalized_blueprint.get("nodes"), list)
        else [],
        "normalized_policies": normalized_blueprint.get("policies", [])
        if isinstance(normalized_blueprint.get("policies"), list)
        else [],
        "normalized_environment_profile": normalized_blueprint.get(
            "environment_profile", {}
        )
        if isinstance(normalized_blueprint.get("environment_profile"), dict)
        else {},
        "normalized_identity_baseline": normalized_blueprint.get(
            "identity_baseline", {}
        )
        if isinstance(normalized_blueprint.get("identity_baseline"), dict)
        else {},
    }

    return _InlineBlueprintValidationResult(report)


def _resolve_validate_blueprint_block():
    def _try_import_validator():
        try:
            from automation.blueprint_schema import validate_blueprint_block

            return validate_blueprint_block
        except Exception:
            pass

        from blueprint_schema import validate_blueprint_block

        return validate_blueprint_block

    try:
        return _try_import_validator()
    except Exception:
        candidate_roots = [
            Path("/workspace"),
            Path("/workspace/root-automation"),
            Path("/workspace/automation"),
            Path(__file__).resolve().parents[5],
        ]

        for root_path in candidate_roots:
            if not root_path.exists():
                continue

            root_str = str(root_path)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)

            try:
                return _try_import_validator()
            except Exception:
                continue

        LOG.warning(
            "automation.blueprint_schema indisponível; usando validador inline do api-engine."
        )
        return _validate_blueprint_block_inline


def _build_normalized_blueprint_payload(report):
    return {
        "schema_name": report.get("schema_name", ""),
        "schema_version": report.get("schema_version", ""),
        "blueprint_version": report.get("blueprint_version", ""),
        "created_at": report.get("created_at", ""),
        "updated_at": report.get("updated_at", ""),
        "orgs": report.get("normalized_orgs", []),
        "channels": report.get("normalized_channels", []),
        "nodes": report.get("normalized_nodes", []),
        "policies": report.get("normalized_policies", []),
        "environment_profile": report.get("normalized_environment_profile", {}),
        "identity_baseline": report.get("normalized_identity_baseline", {}),
        "fingerprint_sha256": report.get("fingerprint_sha256", ""),
        "schema_runtime": report.get("schema_runtime", ""),
        "resolved_schema_version": report.get("resolved_schema_version", ""),
    }


def _hydrate_lint_report(report, validated):
    hydrated = dict(report or {})
    hydrated["contract_version"] = BLUEPRINT_CONTRACT_VERSION
    hydrated["source_mode"] = "official"
    hydrated["normalized_blueprint"] = _build_normalized_blueprint_payload(hydrated)
    hydrated["lint_generated_at_utc"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    hydrated["change_id"] = validated.get("change_id", "")
    hydrated["execution_context"] = validated.get("execution_context", "")

    return hydrated


def _load_blueprint_versions():
    with BLUEPRINT_VERSIONS_STORE_LOCK:
        if not BLUEPRINT_VERSIONS_STORE_FILE.exists():
            return []

        raw_payload = BLUEPRINT_VERSIONS_STORE_FILE.read_text(encoding="utf-8")
        if not raw_payload.strip():
            return []

        parsed_payload = json.loads(raw_payload)
        if not isinstance(parsed_payload, list):
            return []

        return parsed_payload


def _persist_blueprint_versions(version_rows):
    with BLUEPRINT_VERSIONS_STORE_LOCK:
        BLUEPRINT_VERSIONS_STORE_FILE.write_text(
            json.dumps(version_rows, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )


def _load_blueprint_access_audit():
    with BLUEPRINT_VERSIONS_STORE_LOCK:
        if not BLUEPRINT_ACCESS_AUDIT_STORE_FILE.exists():
            return []

        raw_payload = BLUEPRINT_ACCESS_AUDIT_STORE_FILE.read_text(encoding="utf-8")
        if not raw_payload.strip():
            return []

        parsed_payload = json.loads(raw_payload)
        if not isinstance(parsed_payload, list):
            return []

        return parsed_payload


def _persist_blueprint_access_audit(rows):
    with BLUEPRINT_VERSIONS_STORE_LOCK:
        BLUEPRINT_ACCESS_AUDIT_STORE_FILE.write_text(
            json.dumps(rows, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )


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
    return {
        "username": str(getattr(user, "username", "system") or "system").strip() or "system",
        "role": _normalize_actor_role(user),
        "organization_id": str(getattr(organization, "id", "") or "").strip(),
        "organization_name": str(getattr(organization, "name", "") or "").strip(),
    }


def _execution_context_has_external_linux_scope(execution_context):
    return "external-linux" in str(execution_context or "").strip().lower()


def _append_blueprint_access_audit(
    action,
    actor_context,
    authorized,
    decision_code,
    reason,
    change_id="",
    execution_context="",
):
    rows = _load_blueprint_access_audit()
    rows.append(
        {
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "action": action,
            "authorized": bool(authorized),
            "decision_code": decision_code,
            "reason": reason,
            "change_id": str(change_id or "").strip(),
            "execution_context": str(execution_context or "").strip(),
            "actor_user": actor_context.get("username", "system"),
            "actor_role": actor_context.get("role", "user"),
            "actor_organization_id": actor_context.get("organization_id", ""),
            "actor_organization_name": actor_context.get("organization_name", ""),
        }
    )
    _persist_blueprint_access_audit(rows[-1000:])


def _build_blueprint_version_record(hydrated_lint_report, actor_context=None):
    actor_context = actor_context or {}
    fingerprint_sha256 = hydrated_lint_report.get("fingerprint_sha256", "")
    normalized_fingerprint = str(fingerprint_sha256 or "").strip().lower()
    return {
        "contract_version": hydrated_lint_report.get(
            "contract_version", BLUEPRINT_CONTRACT_VERSION
        ),
        "source_mode": hydrated_lint_report.get("source_mode", "official"),
        "id": str(uuid.uuid4()),
        "blueprint_version": hydrated_lint_report.get("blueprint_version", ""),
        "schema_version": hydrated_lint_report.get("schema_version", ""),
        "resolved_schema_version": hydrated_lint_report.get(
            "resolved_schema_version", ""
        ),
        "fingerprint_sha256": fingerprint_sha256,
        "published_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "change_id": hydrated_lint_report.get("change_id", ""),
        "execution_context": hydrated_lint_report.get("execution_context", ""),
        "lint_generated_at_utc": hydrated_lint_report.get("lint_generated_at_utc", ""),
        "published_by": actor_context.get("username", "system"),
        "published_by_role": actor_context.get("role", "user"),
        "published_by_organization": actor_context.get("organization_name", ""),
        "valid": bool(hydrated_lint_report.get("valid", False)),
        "errors": hydrated_lint_report.get("errors", []),
        "issues": hydrated_lint_report.get("issues", []),
        "warnings": hydrated_lint_report.get("warnings", []),
        "hints": hydrated_lint_report.get("hints", []),
        "normalized_blueprint": hydrated_lint_report.get("normalized_blueprint", {}),
        "manifest_fingerprint": normalized_fingerprint,
        "source_blueprint_fingerprint": normalized_fingerprint,
        "backend_state": "ready",
        "a2_2_minimum_artifacts": list(BLUEPRINT_A2_2_MINIMUM_ARTIFACTS),
        "a2_2_available_artifacts": list(BLUEPRINT_A2_2_MINIMUM_ARTIFACTS),
        "a2_2_missing_artifacts": [],
        "a2_2_artifacts_ready": True,
    }


class BlueprintViewSet(viewsets.ViewSet):
    permission_classes = [
        IsAuthenticated,
    ]

    @swagger_auto_schema(
        methods=["post"],
        request_body=BlueprintLintRequestSerializer,
        responses=with_common_response(
            {status.HTTP_200_OK: BlueprintLintResponseSerializer}
        ),
    )
    @action(detail=False, methods=["post"], url_path="lint")
    def lint(self, request):
        serializer = BlueprintLintRequestSerializer(data=request.data)

        try:
            if serializer.is_valid(raise_exception=True):
                validated = serializer.validated_data
                validate_blueprint_block = _resolve_validate_blueprint_block()

                lint_report = validate_blueprint_block(
                    validated.get("blueprint", {}),
                    allow_schema_migration=validated.get("allow_migration", False),
                ).to_dict()

                report = _hydrate_lint_report(lint_report, validated)

                response = BlueprintLintResponseSerializer(data=report)
                if response.is_valid(raise_exception=True):
                    return Response(ok(response.validated_data), status=status.HTTP_200_OK)

        except Exception as exc:
            LOG.exception("Blueprint lint endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["post"],
        request_body=BlueprintPublishRequestSerializer,
        responses=with_common_response(
            {status.HTTP_201_CREATED: BlueprintPublishResponseSerializer}
        ),
    )
    @action(detail=False, methods=["post"], url_path="publish")
    def publish(self, request):
        serializer = BlueprintPublishRequestSerializer(data=request.data)

        try:
            if serializer.is_valid(raise_exception=True):
                validated = serializer.validated_data
                actor_context = _resolve_actor_context(request)

                if actor_context.get("role", "user") not in BLUEPRINT_ALLOWED_PUBLISH_ROLES:
                    _append_blueprint_access_audit(
                        "publish",
                        actor_context,
                        False,
                        "blueprint_publish_access_forbidden",
                        "Papel sem permissao para publicar blueprint oficial.",
                        change_id=validated.get("change_id", ""),
                        execution_context=validated.get("execution_context", ""),
                    )
                    return Response(
                        {
                            "status": "fail",
                            "msg": "blueprint_publish_access_forbidden",
                            "data": {
                                "code": "blueprint_publish_access_forbidden",
                                "contract_version": BLUEPRINT_CONTRACT_VERSION,
                                "source_mode": "official",
                                "required_roles": sorted(
                                    list(BLUEPRINT_ALLOWED_PUBLISH_ROLES)
                                ),
                                "actor_role": actor_context.get("role", "user"),
                            },
                        },
                        status=status.HTTP_403_FORBIDDEN,
                    )

                if not _execution_context_has_external_linux_scope(
                    validated.get("execution_context", "")
                ):
                    _append_blueprint_access_audit(
                        "publish",
                        actor_context,
                        False,
                        "blueprint_publish_scope_invalid",
                        "Contexto de execucao sem escopo external-linux.",
                        change_id=validated.get("change_id", ""),
                        execution_context=validated.get("execution_context", ""),
                    )
                    return Response(
                        {
                            "status": "fail",
                            "msg": "blueprint_publish_scope_invalid",
                            "data": {
                                "code": "blueprint_publish_scope_invalid",
                                "contract_version": BLUEPRINT_CONTRACT_VERSION,
                                "source_mode": "official",
                                "required_scope": "external-linux",
                                "execution_context": validated.get("execution_context", ""),
                            },
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                validate_blueprint_block = _resolve_validate_blueprint_block()

                lint_report = validate_blueprint_block(
                    validated.get("blueprint", {}),
                    allow_schema_migration=validated.get("allow_migration", False),
                ).to_dict()
                hydrated_report = _hydrate_lint_report(lint_report, validated)

                if not hydrated_report.get("valid", False):
                    return Response(
                        {
                            "status": "fail",
                            "msg": "blueprint_publish_blocked_by_lint",
                            "data": {
                                "code": "blueprint_publish_blocked_by_lint",
                                "contract_version": BLUEPRINT_CONTRACT_VERSION,
                                "source_mode": "official",
                                "errors": hydrated_report.get("errors", []),
                                "warnings": hydrated_report.get("warnings", []),
                                "hints": hydrated_report.get("hints", []),
                            },
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                version_record = _build_blueprint_version_record(
                    hydrated_report,
                    actor_context=actor_context,
                )
                versions = _load_blueprint_versions()
                versions = [version_record, *versions][:200]
                _persist_blueprint_versions(versions)
                _append_blueprint_access_audit(
                    "publish",
                    actor_context,
                    True,
                    "blueprint_publish_access_allowed",
                    "Publicacao autorizada no escopo operacional oficial.",
                    change_id=validated.get("change_id", ""),
                    execution_context=validated.get("execution_context", ""),
                )

                response = BlueprintPublishResponseSerializer(data=version_record)
                if response.is_valid(raise_exception=True):
                    return Response(
                        ok(response.validated_data), status=status.HTTP_201_CREATED
                    )

        except Exception as exc:
            LOG.exception("Blueprint publish endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        methods=["get"],
        responses=with_common_response(
            {status.HTTP_200_OK: BlueprintVersionListResponseSerializer}
        ),
    )
    @action(detail=False, methods=["get"], url_path="versions")
    def versions(self, request):
        try:
            version_rows = _load_blueprint_versions()
            payload = {"total": len(version_rows), "data": version_rows}

            response = BlueprintVersionListResponseSerializer(data=payload)
            if response.is_valid(raise_exception=True):
                return Response(ok(response.validated_data), status=status.HTTP_200_OK)

        except Exception as exc:
            LOG.exception("Blueprint versions endpoint failed")
            return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

        return Response(err(("Invalid request payload",)), status=status.HTTP_400_BAD_REQUEST)
