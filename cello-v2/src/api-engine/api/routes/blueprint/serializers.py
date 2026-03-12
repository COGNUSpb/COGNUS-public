#
# SPDX-License-Identifier: Apache-2.0
#
from rest_framework import serializers


class BlueprintLintIssueSerializer(serializers.Serializer):
    level = serializers.CharField(help_text="Nível da issue")
    code = serializers.CharField(help_text="Código estável da regra")
    path = serializers.CharField(help_text="Caminho do campo no blueprint")
    message = serializers.CharField(help_text="Mensagem de diagnóstico")


class BlueprintLintRequestSerializer(serializers.Serializer):
    blueprint = serializers.JSONField(help_text="Blueprint serializado em objeto JSON")
    allow_migration = serializers.BooleanField(
        required=False,
        default=False,
        help_text="Permite migração de schema quando houver rotina suportada",
    )
    change_id = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=128,
        help_text="Identificador da mudança (opcional)",
    )
    execution_context = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=2048,
        help_text="Contexto de execução vinculado ao lint (opcional)",
    )


class BlueprintLintResponseSerializer(serializers.Serializer):
    contract_version = serializers.CharField(required=False, allow_blank=True)
    source_mode = serializers.CharField(required=False, allow_blank=True)
    valid = serializers.BooleanField()
    errors = BlueprintLintIssueSerializer(many=True)
    warnings = BlueprintLintIssueSerializer(many=True)
    hints = BlueprintLintIssueSerializer(many=True)
    issues = BlueprintLintIssueSerializer(many=True)

    schema_name = serializers.CharField(required=False, allow_blank=True)
    schema_version = serializers.CharField(required=False, allow_blank=True)
    blueprint_version = serializers.CharField(required=False, allow_blank=True)
    created_at = serializers.CharField(required=False, allow_blank=True)
    updated_at = serializers.CharField(required=False, allow_blank=True)

    current_schema_version = serializers.CharField(required=False, allow_blank=True)
    schema_runtime = serializers.CharField(required=False, allow_blank=True)
    resolved_schema_version = serializers.CharField(required=False, allow_blank=True)
    migration_applied = serializers.BooleanField(required=False)
    migrated_from_schema_version = serializers.CharField(required=False, allow_blank=True)

    fingerprint_sha256 = serializers.CharField(required=False, allow_blank=True)

    normalized_orgs = serializers.ListField(child=serializers.JSONField(), required=False)
    normalized_channels = serializers.ListField(child=serializers.JSONField(), required=False)
    normalized_nodes = serializers.ListField(child=serializers.JSONField(), required=False)
    normalized_policies = serializers.ListField(child=serializers.JSONField(), required=False)
    normalized_environment_profile = serializers.JSONField(required=False)
    normalized_identity_baseline = serializers.JSONField(required=False)

    normalized_blueprint = serializers.JSONField(required=False)
    lint_generated_at_utc = serializers.CharField(required=False, allow_blank=True)
    change_id = serializers.CharField(required=False, allow_blank=True)
    execution_context = serializers.CharField(required=False, allow_blank=True)


class BlueprintPublishRequestSerializer(serializers.Serializer):
    blueprint = serializers.JSONField(help_text="Blueprint serializado em objeto JSON")
    change_id = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=128,
        help_text="Identificador da mudança",
    )
    execution_context = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=2048,
        help_text="Contexto operacional vinculado à publicação",
    )
    allow_migration = serializers.BooleanField(
        required=False,
        default=False,
        help_text="Permite migração de schema quando houver rotina suportada",
    )


class BlueprintVersionRecordSerializer(serializers.Serializer):
    contract_version = serializers.CharField(required=False, allow_blank=True)
    source_mode = serializers.CharField(required=False, allow_blank=True)
    id = serializers.CharField()
    blueprint_version = serializers.CharField(allow_blank=True)
    schema_version = serializers.CharField(allow_blank=True)
    resolved_schema_version = serializers.CharField(allow_blank=True)
    fingerprint_sha256 = serializers.CharField(allow_blank=True)
    published_at_utc = serializers.CharField(allow_blank=True)
    change_id = serializers.CharField(allow_blank=True)
    execution_context = serializers.CharField(allow_blank=True)
    lint_generated_at_utc = serializers.CharField(allow_blank=True)
    published_by = serializers.CharField(required=False, allow_blank=True)
    published_by_role = serializers.CharField(required=False, allow_blank=True)
    published_by_organization = serializers.CharField(required=False, allow_blank=True)
    valid = serializers.BooleanField()
    errors = BlueprintLintIssueSerializer(many=True)
    issues = BlueprintLintIssueSerializer(many=True)
    warnings = BlueprintLintIssueSerializer(many=True)
    hints = BlueprintLintIssueSerializer(many=True)
    normalized_blueprint = serializers.JSONField()
    manifest_fingerprint = serializers.CharField(required=False, allow_blank=True)
    source_blueprint_fingerprint = serializers.CharField(required=False, allow_blank=True)
    backend_state = serializers.CharField(required=False, allow_blank=True)
    a2_2_minimum_artifacts = serializers.ListField(
        child=serializers.CharField(), required=False
    )
    a2_2_available_artifacts = serializers.ListField(
        child=serializers.CharField(), required=False
    )
    a2_2_missing_artifacts = serializers.ListField(
        child=serializers.CharField(), required=False
    )
    a2_2_artifacts_ready = serializers.BooleanField(required=False)


class BlueprintPublishResponseSerializer(BlueprintVersionRecordSerializer):
    pass


class BlueprintVersionListResponseSerializer(serializers.Serializer):
    total = serializers.IntegerField()
    data = BlueprintVersionRecordSerializer(many=True)
