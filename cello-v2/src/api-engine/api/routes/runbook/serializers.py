#
# SPDX-License-Identifier: Apache-2.0
#
from rest_framework import serializers


class RunbookHostMappingEntrySerializer(serializers.Serializer):
    node_id = serializers.CharField(required=False, allow_blank=True, max_length=128)
    node_type = serializers.CharField(required=False, allow_blank=True, max_length=64)
    org_id = serializers.CharField(required=False, allow_blank=True, max_length=128)
    host_ref = serializers.CharField(required=False, allow_blank=True, max_length=128)
    host_address = serializers.CharField(required=True, allow_blank=False, max_length=256)
    ssh_user = serializers.CharField(required=True, allow_blank=False, max_length=128)
    ssh_port = serializers.IntegerField(required=False, min_value=1, max_value=65535, default=22)
    preflight_status = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=32,
        default="apto",
        help_text="Status tecnico do host no preflight (apto/parcial/bloqueado)",
    )
    runtime_image = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=512,
        help_text="Imagem runtime canônica por componente para provisionamento oficial",
    )


class RunbookApiRegistryEntrySerializer(serializers.Serializer):
    api_id = serializers.CharField(required=False, allow_blank=True, max_length=128)
    org_name = serializers.CharField(required=False, allow_blank=True, max_length=128)
    channel_id = serializers.CharField(required=False, allow_blank=True, max_length=128)
    chaincode_id = serializers.CharField(required=False, allow_blank=True, max_length=128)
    route_path = serializers.CharField(required=False, allow_blank=True, max_length=512)


class RunbookMachineCredentialEntrySerializer(serializers.Serializer):
    machine_id = serializers.CharField(required=True, allow_blank=False, max_length=128)
    credential_ref = serializers.CharField(required=False, allow_blank=True, max_length=512)
    credential_payload = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=65536,
        help_text="Conteudo da chave privada em base64 para referencias local-file.",
    )
    credential_fingerprint = serializers.CharField(
        required=False, allow_blank=True, max_length=256
    )
    reuse_confirmed = serializers.BooleanField(required=False, default=False)


class RunbookStartRequestSerializer(serializers.Serializer):
    change_id = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=128,
        help_text="Identificador da mudança",
    )
    provider_key = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=64,
        help_text="Chave do provider",
    )
    environment_profile = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=128,
        help_text="Perfil de ambiente",
    )
    blueprint_version = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=128,
        help_text="Versao do blueprint",
    )
    blueprint_fingerprint = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=256,
        help_text="Fingerprint do blueprint",
    )
    manifest_fingerprint = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=256,
        help_text="Fingerprint canônico do manifesto runtime correlacionado ao handoff A2",
    )
    source_blueprint_fingerprint = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=256,
        help_text="Fingerprint do blueprint de origem para correlação A2",
    )
    resolved_schema_version = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=128,
        help_text="Versao de schema resolvida",
    )
    run_id = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=128,
        help_text="Identificador opcional da execucao",
    )
    pipeline_preconditions_ready = serializers.BooleanField(
        required=False,
        default=True,
        help_text="Indica se pre-condicoes minimas do pipeline foram satisfeitas",
    )
    blueprint_validated = serializers.BooleanField(
        required=False,
        default=True,
        help_text="Indica se o blueprint esta validado para execucao",
    )
    preflight_approved = serializers.BooleanField(
        required=False,
        default=True,
        help_text="Indica se preflight tecnico de infraestrutura foi aprovado",
    )
    a2_2_minimum_artifacts = serializers.ListField(
        child=serializers.CharField(max_length=256),
        required=False,
        default=list,
        help_text="Artefatos mínimos A2.2 exigidos no gate de entrada do runbook",
    )
    a2_3_handoff = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Handoff oficial opcional A2.3 -> A2.4/A2A para correlacao e gate de entrada",
    )
    a2_3_readiness_checklist = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Checklist oficial opcional de prontidao A2.3 consumido pelo gate A2A",
    )
    a2_4_handoff = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Handoff oficial opcional A2.4 -> A2.5/A2A para correlacao e gate de entrada",
    )
    a2_4_readiness_checklist = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Checklist oficial opcional de prontidao A2.4 consumido pelo gate A2A",
    )
    a2_2_available_artifacts = serializers.ListField(
        child=serializers.CharField(max_length=256),
        required=False,
        default=list,
        help_text="Artefatos A2.2 disponíveis no momento do handoff",
    )
    host_mapping = RunbookHostMappingEntrySerializer(
        many=True,
        required=False,
        default=list,
        help_text="Mapeamento host/org/no para execucao real via SSH",
    )
    machine_credentials = RunbookMachineCredentialEntrySerializer(
        many=True,
        required=False,
        default=list,
        help_text="Mapeamento determinístico de credenciais SSH por machine_id",
    )
    api_registry = RunbookApiRegistryEntrySerializer(
        many=True,
        required=False,
        default=list,
        help_text="Registro de APIs por org/canal/chaincode correlacionado ao run",
    )
    incremental_expansions = serializers.ListField(
        child=serializers.JSONField(),
        required=False,
        default=list,
        help_text="Incrementos de onboarding correlacionados por change_id/run_id",
    )
    topology_catalog = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Catálogo canônico de topologia (org/canal/chaincode) do handoff frontend->backend",
    )
    handoff_contract_version = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=128,
        help_text="Versão do contrato canônico de handoff frontend->backend",
    )
    handoff_fingerprint = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=256,
        help_text="Fingerprint canônico do payload de handoff",
    )
    handoff_payload = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Payload canônico do handoff frontend->backend A2",
    )
    handoff_trace = serializers.ListField(
        child=serializers.JSONField(),
        required=False,
        default=list,
        help_text="Trilha de execução para reabertura do runbook sem perda de contexto",
    )
    runbook_resume_context = serializers.JSONField(
        required=False,
        default=dict,
        help_text="Contexto resumido de retomada para reabertura determinística do runbook",
    )


class RunbookPreflightRequestSerializer(serializers.Serializer):
    change_id = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=128,
        help_text="Identificador da mudança para o gate técnico",
    )
    provider_key = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=64,
        default="external-linux",
        help_text="Chave do provider (deve ser external-linux)",
    )
    host_mapping = RunbookHostMappingEntrySerializer(
        many=True,
        required=True,
        allow_empty=False,
        help_text="Mapeamento host para validação dinâmica de conectividade SSH",
    )
    machine_credentials = RunbookMachineCredentialEntrySerializer(
        many=True,
        required=False,
        default=list,
        help_text="Mapeamento determinístico de credenciais SSH por machine_id para preflight",
    )


class RunbookOperateRequestSerializer(serializers.Serializer):
    run_id = serializers.CharField(
        required=True,
        allow_blank=False,
        max_length=128,
        help_text="Identificador da execucao",
    )
    action = serializers.ChoiceField(
        choices=["pause", "resume", "advance", "fail", "retry"],
        help_text="Comando operacional sobre o runbook",
    )
    failure_code = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=128,
        help_text="Codigo tecnico de falha (apenas para action=fail)",
    )
    failure_message = serializers.CharField(
        required=False,
        allow_blank=True,
        max_length=1024,
        help_text="Mensagem de falha (apenas para action=fail)",
    )


class RunbookStateEnvelopeSerializer(serializers.Serializer):
    run = serializers.JSONField(help_text="Estado oficial do runbook")
    snapshot = serializers.JSONField(
        required=False,
        help_text="Snapshot oficial de execucao por etapa/checkpoint",
    )


class RunbookRuntimeInspectionEnvelopeSerializer(serializers.Serializer):
    inspection = serializers.JSONField(
        help_text="Resposta oficial do cache de inspecao runtime por componente/escopo"
    )


class RunbookPreflightEnvelopeSerializer(serializers.Serializer):
    preflight = serializers.JSONField(help_text="Relatório oficial de preflight técnico SSH")


class RunbookCatalogEnvelopeSerializer(serializers.Serializer):
    runs = serializers.ListField(
        child=serializers.JSONField(),
        help_text="Catalogo oficial de runs recuperaveis para reidratacao do historico local",
    )
