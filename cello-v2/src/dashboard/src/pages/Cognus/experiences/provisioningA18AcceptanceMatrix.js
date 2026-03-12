import {
  BLUEPRINT_LOCAL_MODE_ENABLED,
  BLUEPRINT_LOCAL_MODE_BLOCKED_BY_POLICY,
} from '../../../services/blueprint';
import {
  RUNBOOK_LOCAL_MODE_ENABLED,
  RUNBOOK_LOCAL_MODE_BLOCKED_BY_POLICY,
} from '../../../services/runbook';
import { buildA17AcceptanceEvidencePackage } from './provisioningA17AcceptanceMatrix';

export const A18_REQUIRED_OUT_OF_SCOPE = Object.freeze([
  'Core do Orquestrador',
  'Gateway ccapi, Lifecycle e Guardrails',
  'Console Operacional e Visualização de Rede',
  'Governança, IAM e Segurança End-to-End',
  'Observabilidade Multi-Escala e SRE',
  'Marketplace, Templates e Integrações',
  'Validação, Benchmark/Chaos e Readiness',
]);

export const A18_EPICS_WITH_CONTAINERIZATION = Object.freeze([
  'A1',
  'A2',
  'B1',
  'B2',
  'C1',
  'C2',
  'D1',
  'E1',
]);
export const A18_CONTAINER_MATRIX_EPICS = Object.freeze(['A2', 'B1', 'B2', 'C1', 'C2', 'D1', 'E1']);

export const A18_CONTAINER_MATRIX_EXPECTED_WAVE_ORDER = Object.freeze([
  'A2',
  'B1',
  'B2',
  'C1',
  'C2',
  'D1',
  'E1',
]);

export const A18_OUT_OF_SCOPE_DESTINATIONS = Object.freeze([
  {
    item: 'Core do Orquestrador',
    target_epic: 'A2',
    rationale: 'Núcleo OPSSC e execução distribuída.',
  },
  {
    item: 'Gateway ccapi, Lifecycle e Guardrails',
    target_epic: 'B1',
    rationale: 'Lifecycle governado e guardrails de chaincode.',
  },
  {
    item: 'Console Operacional e Visualização de Rede',
    target_epic: 'C2',
    rationale: 'Console operacional, topologia e timeline auditável.',
  },
  {
    item: 'Governança, IAM e Segurança End-to-End',
    target_epic: 'B2',
    rationale: 'Autenticação federada, RBAC/ABAC e auditoria de acesso.',
  },
  {
    item: 'Observabilidade Multi-Escala e SRE',
    target_epic: 'C1',
    rationale: 'Sinais operacionais, SLOs e telemetria multi-camada.',
  },
  {
    item: 'Marketplace, Templates e Integrações',
    target_epic: 'D1',
    rationale: 'Catálogo, templates e governança de publicação.',
  },
  {
    item: 'Validação, Benchmark/Chaos e Readiness',
    target_epic: 'E1',
    rationale: 'Readiness final, benchmark e chaos controlado.',
  },
]);

export const A18_ACCEPTANCE_CRITERIA = Object.freeze([
  {
    id: 'a18-wp-closure-a1-1-a1-7',
    label: 'Todos os WPs de A1.1 até A1.7 concluídos e rastreáveis por evidência.',
  },
  {
    id: 'a18-official-external-linux-baseline',
    label: 'Baseline operacional final external-linux sem fallback local em modo operacional.',
  },
  {
    id: 'a18-deterministic-gates-contracts',
    label: 'Gates e contratos determinísticos com bloqueio de regressão em CI.',
  },
  {
    id: 'a18-final-acceptance-traceability',
    label: 'Matriz final de aceite com rastreabilidade por change_id, run_id e fingerprint_sha256.',
  },
  {
    id: 'a18-final-evidence-package',
    label: 'Pacote consolidado de evidências com runbook oficial e atividade real em host alvo.',
  },
  {
    id: 'a18-transition-checklist-a1-a2',
    label: 'Checklist formal de prontidão A1 -> A2 sem reabertura de escopo de A1.',
  },
  {
    id: 'a18-cross-document-conformity',
    label:
      'Conformidade documental cruzada bloqueante entre README, entrega-1, roadmap e arquitetura.',
  },
  {
    id: 'a18-container-operational-matrix-executable',
    label:
      'Matriz operacional de containers por épico convertida em contrato executável e bloqueante.',
  },
  {
    id: 'a18-a1-operational-allocation-non-isolated',
    label:
      'Alocação obrigatória do Épico A1 validada de forma não isolada, com containers, frontend e evidência mínima.',
  },
  {
    id: 'a18-final-wp-a1-8-acceptance-criteria',
    label:
      'Critérios finais do WP A1.8 validados com bloqueio de regressão e evidência técnica definitiva.',
  },
  {
    id: 'a18-out-of-scope-explicit',
    label: 'Itens fora de escopo explicitamente classificados e direcionados a épicos posteriores.',
  },
]);

const DEFAULT_EXECUTOR = 'qa.ops@ufg.br';
const DEFAULT_GENERATED_AT_UTC = '2026-02-18T01:10:00Z';
const DEFAULT_CHANGE_ID = 'cr-a1.8-final-001';
const DEFAULT_RUN_ID = 'run-a1.8-final-001';
const DEFAULT_FINGERPRINT = 'fcdfde265321b607b77353a53bc40c41d3f24331362feba24182c6dd6bf1135d';
const DEFAULT_TECHNICAL_SIGNATORY = 'architecture.board@ufg.br';

const normalizeText = value => (typeof value === 'string' ? value.trim() : '');

const buildWpClosureRows = () => [
  {
    wp: 'A1.1',
    status: 'concluido',
    evidence_ref: 'scripts/validate_blueprints_ci.sh',
    change_classification: 'concluido',
  },
  {
    wp: 'A1.2',
    status: 'concluido',
    evidence_ref: 'automation/tests/test_blueprint_lint_gate_integration.py',
    change_classification: 'concluido',
  },
  {
    wp: 'A1.3',
    status: 'concluido',
    evidence_ref: 'scripts/validate_pipeline_a1_3_ci.sh',
    change_classification: 'concluido',
  },
  {
    wp: 'A1.4',
    status: 'concluido',
    evidence_ref: 'automation/tests/test_pipeline_a1_4_matrix.py',
    change_classification: 'concluido',
  },
  {
    wp: 'A1.5',
    status: 'concluido',
    evidence_ref: 'docs/entregas/evidencias/wp-a1.5-matriz-aceite.json',
    change_classification: 'concluido',
  },
  {
    wp: 'A1.6',
    status: 'concluido',
    evidence_ref: 'docs/entregas/evidencias/wp-a1.6-criterios-aceite.json',
    change_classification: 'concluido',
  },
  {
    wp: 'A1.7',
    status: 'concluido',
    evidence_ref: 'docs/entregas/evidencias/wp-a1.7-criterios-aceite.json',
    change_classification: 'concluido',
  },
];

const buildTransitionChecklist = () => [
  {
    id: 'a2-consume-a1-contracts',
    status: 'done',
    owner: 'backend-core',
    target_epic: 'A2',
    mitigation:
      'A2 consome contratos e gates congelados de A1 sem alterar semântica de execução oficial.',
  },
  {
    id: 'a2-consume-a1-evidences',
    status: 'done',
    owner: 'qa-ops',
    target_epic: 'A2',
    mitigation:
      'Handoff referencia pacote consolidado de evidências e correlação por IDs operacionais.',
  },
  {
    id: 'a2-no-scope-reopen-a1',
    status: 'done',
    owner: 'pm-architecture',
    target_epic: 'A2',
    mitigation:
      'Fronteira A1 fechada: itens fora de escopo permanecem alocados em épicos posteriores.',
  },
];

const buildCrossDocumentConformity = ({ generatedAtUtc, changeId, runId, fingerprintSha256 }) => ({
  status: 'conform',
  validated_at_utc: generatedAtUtc,
  correlation: {
    change_id: changeId,
    run_id: runId,
    fingerprint_sha256: fingerprintSha256,
  },
  canonical_terminology: {
    provider_scope_term: 'external-linux',
    operational_mode_term: 'official',
    pipeline_flow_term: 'prepare -> provision -> configure -> verify',
    no_local_fallback_term: 'sem fallback local em modo operacional',
  },
  status_alignment: [
    {
      doc_ref: 'docs/entregas/README.md',
      subject: 'Entrega 1 / Épico A1',
      expected_status: 'encerrado',
      status_statement:
        'Fluxo operacional alvo da Entrega 1 permanece no escopo external-linux com execução oficial e sem fallback local operacional.',
    },
    {
      doc_ref: 'docs/entregas/entrega-1.md',
      subject: 'Entrega 1',
      expected_status: 'encerrado',
      status_statement: 'Situação da Entrega 1 encerrada com baseline A1 fechada e rastreável.',
    },
    {
      doc_ref: 'docs/entregas/roadmap-epicos.md',
      subject: 'WP A1.8',
      expected_status: 'encerrado',
      status_statement:
        'A1.8 concluído (definitivo) com escopo restrito a A1 e transição A1->A2 formalizada.',
    },
  ],
  architecture_coherence: {
    high_level_refs: [
      'docs/orchestrator-architecture.puml',
      'docs/orchestrator-conceptual.puml',
      'docs/orchestrator-overview-macro.puml',
    ],
    low_level_refs: [''],
    consistency_statement:
      'Arquitetura de alto e baixo nível preserva separação entre plano de controle, execução Fabric e operação do host/VM external-linux.',
  },
  scope_boundaries: {
    included: [
      'Consolidação de contratos/gates A1.1..A1.7',
      'Baseline operacional final external-linux',
      'Matriz final de aceite com rastreabilidade por IDs',
      'Handoff A1->A2 sem reabertura de escopo A1.8',
    ],
    excluded: A18_REQUIRED_OUT_OF_SCOPE,
    implementation_frontier_statement:
      'A1 cobre fundação operacional e evidências do fluxo E1; lifecycle governado completo, IAM avançado, marketplace, observabilidade multi-escala e readiness final permanecem nos épicos seguintes.',
  },
  e1_operational_flow: {
    source_of_truth: 'docs/entregas/README.md#11',
    unique_description_enforced: true,
    canonical_steps: [
      'infra_ssh_preflight',
      'organization_and_network_registration',
      'nodes_and_execution_mapping',
      'business_group_channels_and_install',
      'api_publication_by_channel_chaincode',
      'incremental_expansion_post_creation',
    ],
  },
  containerization_by_epic: {
    explicit_by_epic: true,
    epics: A18_EPICS_WITH_CONTAINERIZATION,
    source_of_truth: 'docs/entregas/roadmap-epicos.md',
  },
  blocking_policy: {
    semantic_divergence_blocks_acceptance: true,
    deterministic_reason_codes_required: true,
  },
});

const buildContainerOperationalMatrix = ({
  generatedAtUtc,
  changeId,
  runId,
  fingerprintSha256,
}) => ({
  status: 'executable',
  validated_at_utc: generatedAtUtc,
  source_of_truth: 'docs/entregas/roadmap-epicos.md',
  correlation: {
    change_id: changeId,
    run_id: runId,
    fingerprint_sha256: fingerprintSha256,
  },
  blocking_policy: {
    implicit_strategic_containers_forbidden: true,
    ambiguous_responsibility_overlap_forbidden: true,
    progressive_vm_convergence_required: true,
  },
  epic_distribution: [
    {
      epic: 'A2',
      wave: 'Onda A',
      orchestrator_containers: [
        'opssc-api-engine',
        'opssc-workflow-engine',
        'opssc-event-bus',
        'opssc-control-db',
        'opssc-state-store',
      ],
      external_linux_host_containers: ['nenhum'],
      operational_purpose: {
        orchestrator: 'Consolidar plano de controle distribuído e execução governada de mudanças.',
        external_linux_host: 'Não provisiona novo runtime Fabric no host nesta onda.',
      },
      traceability: {
        requirements: ['FR1', 'FR2', 'FR4', 'FR8'],
        architecture_layers: [
          'Camada 2 — Plano de Controle (OPSSC)',
          'Camada 3 — Orquestração e APIs',
        ],
      },
    },
    {
      epic: 'B1',
      wave: 'Onda B',
      orchestrator_containers: [
        'ccapi-gateway',
        'lifecycle-operator',
        'lifecycle-policy-engine',
        'lifecycle-webhook-dispatcher',
      ],
      external_linux_host_containers: [
        'chaincode-runtime-managed-by-peers',
        'channel-local-aux-services-when-applicable',
      ],
      operational_purpose: {
        orchestrator:
          'Executar lifecycle governado com guardrails e trilha auditável por change_id.',
        external_linux_host:
          'Permitir runtime de chaincode e serviços locais estritamente necessários por canal.',
      },
      traceability: {
        requirements: ['FR5', 'FR6', 'FR8'],
        architecture_layers: [
          'Camada 3 — Orquestração e APIs',
          'Camada 4 — Runtime Fabric e Serviços Locais',
        ],
      },
    },
    {
      epic: 'B2',
      wave: 'Onda B',
      orchestrator_containers: [
        'iam-provider',
        'policy-decision-point',
        'offchain-authz-broker',
        'access-audit-service',
      ],
      external_linux_host_containers: ['nenhum'],
      operational_purpose: {
        orchestrator:
          'Aplicar autenticação federada e autorização contextual com auditoria de acesso.',
        external_linux_host: 'Sem container local obrigatório por padrão para segurança.',
      },
      traceability: {
        requirements: ['FR8', 'FR9', 'FR10'],
        architecture_layers: [
          'Camada 2 — Plano de Controle (OPSSC)',
          'Camada 3 — Orquestração e APIs',
        ],
      },
    },
    {
      epic: 'C1',
      wave: 'Onda C',
      orchestrator_containers: [
        'metrics-collector',
        'logs-aggregator',
        'traces-collector',
        'alertmanager-slo',
        'observability-dashboard',
      ],
      external_linux_host_containers: ['node-observer-agent-when-adopted'],
      operational_purpose: {
        orchestrator:
          'Consolidar sinais de observabilidade multi-camada com SLOs e alertas operacionais.',
        external_linux_host: 'Exportar telemetria local sem alterar baseline de execução oficial.',
      },
      traceability: {
        requirements: ['FR11', 'FR12'],
        architecture_layers: [
          'Camada 2 — Plano de Controle (OPSSC)',
          'Camada 5 — Observabilidade e SRE',
        ],
      },
    },
    {
      epic: 'C2',
      wave: 'Onda C',
      orchestrator_containers: [
        'ops-console-backend',
        'ops-topology-service',
        'ops-timeline-service',
        'ops-runbook-catalog-service',
      ],
      external_linux_host_containers: ['nenhum'],
      operational_purpose: {
        orchestrator: 'Entregar console operacional e topologia auditável por mudança.',
        external_linux_host: 'Consome estado remoto sem novo serviço local obrigatório.',
      },
      traceability: {
        requirements: ['FR2', 'FR3', 'FR11'],
        architecture_layers: ['Camada 3 — Orquestração e APIs', 'Camada 6 — Console Operacional'],
      },
    },
    {
      epic: 'D1',
      wave: 'Onda D',
      orchestrator_containers: [
        'marketplace-catalog-api',
        'marketplace-sync-worker',
        'marketplace-recommendation-service',
        'marketplace-governance-service',
      ],
      external_linux_host_containers: ['nenhum'],
      operational_purpose: {
        orchestrator:
          'Governar publicação/sincronização de templates e integrações do ecossistema.',
        external_linux_host: 'Sem publicação local dedicada no host da organização.',
      },
      traceability: {
        requirements: ['FR13'],
        architecture_layers: [
          'Camada 2 — Plano de Controle (OPSSC)',
          'Camada 7 — Marketplace e Integrações',
        ],
      },
    },
    {
      epic: 'E1',
      wave: 'Onda E',
      orchestrator_containers: [
        'e2e-test-runner',
        'benchmark-runner',
        'chaos-runner',
        'readiness-gate-service',
      ],
      external_linux_host_containers: ['chaos-target-agent-when-applicable'],
      operational_purpose: {
        orchestrator:
          'Executar readiness final, benchmark e chaos com decisão de go-live auditável.',
        external_linux_host:
          'Executar experimentos controlados no alvo sem romper plano de controle.',
      },
      traceability: {
        requirements: ['FR14'],
        architecture_layers: ['Camada 5 — Observabilidade e SRE', 'Camada 8 — Readiness e Go-live'],
      },
    },
  ],
  progressive_vm_convergence: {
    reference_environment: 'external-linux',
    wave_checkpoints: [
      {
        epic: 'A2',
        wave: 'Onda A',
        convergence_level: 0,
        verification_focus: 'Sem novo container local obrigatório além da baseline A1.',
      },
      {
        epic: 'B1',
        wave: 'Onda B',
        convergence_level: 1,
        verification_focus:
          'Runtime de chaincode e serviços auxiliares por canal sob lifecycle governado.',
      },
      {
        epic: 'B2',
        wave: 'Onda B',
        convergence_level: 1,
        verification_focus:
          'Segurança permanece centralizada no orquestrador sem obrigatoriedade local.',
      },
      {
        epic: 'C1',
        wave: 'Onda C',
        convergence_level: 2,
        verification_focus: 'Coleta local opcional de telemetria via agente quando adotado.',
      },
      {
        epic: 'C2',
        wave: 'Onda C',
        convergence_level: 2,
        verification_focus:
          'Console e topologia continuam sem provisão local adicional obrigatória.',
      },
      {
        epic: 'D1',
        wave: 'Onda D',
        convergence_level: 2,
        verification_focus: 'Marketplace central sem serviço dedicado local por padrão.',
      },
      {
        epic: 'E1',
        wave: 'Onda E',
        convergence_level: 4,
        verification_focus:
          'Readiness final comprova estado convergente da VM por inventário e saúde.',
      },
    ],
    final_expected_components: [
      'ca',
      'orderer',
      'peer',
      'couchdb',
      'ccapi',
      'networkapi',
      'webclient-or-forwarder',
      'prometheus',
      'grafana',
      'exporters-or-agents',
    ],
    verification_artifacts: ['inventory-final.json', 'stage-reports/*', 'verify-report.json'],
  },
});

const buildA1OperationalAllocation = ({ generatedAtUtc, changeId, runId, fingerprintSha256 }) => ({
  status: 'enforced',
  validated_at_utc: generatedAtUtc,
  correlation: {
    change_id: changeId,
    run_id: runId,
    fingerprint_sha256: fingerprintSha256,
  },
  convention: {
    orchestrator_domain_definition:
      'Ambiente do Orquestrador (serviços do sistema): plano de controle, APIs, automação, segurança, observabilidade e console.',
    external_linux_domain_definition:
      'Host/VM da Organização (provedor externo / external-linux): execução Fabric e serviços técnicos locais da organização.',
    explicit_none_required_when_no_container_in_domain: true,
    development_co_location_allowed_with_logical_separation: true,
  },
  mandatory_a1_containers: {
    orchestrator_system_services: [
      'runbook-api-engine',
      'provisioning-ssh-executor',
      'pipeline-evidence-store',
    ],
    external_linux_host_requirements: [
      'runtime-base-per-active-inventoried-host',
      'fabric-ca-minimum-one-per-org-with-ca-role',
      'fabric-orderer-minimum-one-per-org-with-orderer-role',
      'fabric-peer-minimum-one-per-channel-member-org',
      'couchdb-one-per-peer-when-external-state-db-required',
    ],
  },
  frontend_evolution: {
    existing_incremented_screens: [
      'ProvisioningInfrastructurePage',
      'ProvisioningBlueprintPage',
      'ProvisioningRunbookPage',
      'ProvisioningInventoryPage',
    ],
    new_screens_or_modules: [
      'ProvisioningTechnicalHubPage',
      'ProvisioningReadinessCard',
      'ScreenReadinessBanner',
    ],
  },
  out_of_scope_for_a1_8_topic: [
    'gateway-ccapi',
    'lifecycle-governado-chaincode',
    'console-operacional-avancado',
    'iam-end-to-end',
    'observabilidade-multi-escala',
  ],
  minimum_container_acceptance_evidence: [
    'inventory-final-with-name-role-host-ports-status',
    'stage-reports-with-change-id-and-run-id-correlation',
    'verify-report-with-health-checks-per-critical-component',
  ],
});

const buildFinalAcceptanceCriteria = ({
  generatedAtUtc,
  changeId,
  runId,
  fingerprintSha256,
  itemCompletion,
  noLocalOrDegradedSuccess,
}) => ({
  status: 'accepted',
  validated_at_utc: generatedAtUtc,
  correlation: {
    change_id: changeId,
    run_id: runId,
    fingerprint_sha256: fingerprintSha256,
  },
  minimum_coverage: {
    functional_and_contractual_validation_official_flow_a1: true,
    minimum_evidence_and_auditable_decision_trace_validation: true,
    documentation_and_container_distribution_validation: true,
  },
  mandatory_rules: {
    contract_regression_breaks_ci: true,
    no_local_or_degraded_success_path: noLocalOrDegradedSuccess,
    acceptance_requires_items_1_to_6_completed: true,
  },
  required_items_1_to_6: itemCompletion,
  required_check_ids: [
    'wp_closure_a1_1_a1_7',
    'official_flow_no_local_fallback',
    'deterministic_contract_freeze',
    'traceability_ids_present',
    'delivery_1_closure_dossier_complete',
    'transition_checklist_ready',
    'cross_document_conformity',
    'container_operational_matrix_executable',
    'a1_operational_allocation_non_isolated',
  ],
  expected_result: 'WP A1.8 encerrado com evidência técnica definitiva.',
});

const buildA1ToA2TransitionReadiness = ({
  generatedAtUtc,
  changeId,
  runId,
  fingerprintSha256,
}) => ({
  target_epic: 'A2',
  handoff_generated_at_utc: generatedAtUtc,
  correlation: {
    change_id: changeId,
    run_id: runId,
    fingerprint_sha256: fingerprintSha256,
  },
  consumed_prerequisites: [
    {
      id: 'a2-prereq-contracts-a1',
      category: 'contracts',
      source_epic: 'A1',
      source_module: 'scripts/validate_pipeline_a1_3_ci.sh',
      target_epic: 'A2',
      target_module: 'opssc-workflow-engine',
      evidence_ref: 'scripts/validate_wp_a1_8_ci.sh',
      status: 'ready',
    },
    {
      id: 'a2-prereq-evidence-bundle-a1',
      category: 'evidence',
      source_epic: 'A1',
      source_module: 'a1-delivery-1-closure-dossier',
      target_epic: 'A2',
      target_module: 'opssc-state-store',
      evidence_ref: 'a1-delivery-1-closure-dossier.json',
      status: 'ready',
    },
    {
      id: 'a2-prereq-inventory-a1',
      category: 'inventory',
      source_epic: 'A1',
      source_module: 'inventory-final.json + configure/crypto-inventory.json',
      target_epic: 'A2',
      target_module: 'opssc-control-db',
      evidence_ref: 'pipeline-report.json',
      status: 'ready',
    },
    {
      id: 'a2-prereq-operational-ids',
      category: 'ids',
      source_epic: 'A1',
      source_module: 'change_id/run_id/fingerprint_sha256',
      target_epic: 'A2',
      target_module: 'opssc-api-engine',
      evidence_ref: 'pipeline-report.json',
      status: 'ready',
    },
  ],
  technical_dependencies: [
    {
      id: 'a2-dependency-workflow-contract',
      description: 'Consumo de estados/checkpoints idempotentes sem alteração de semântica A1.',
      source_epic: 'A1',
      source_module: 'pipeline_contract.py + pipeline_state_store.py',
      target_epic: 'A2',
      target_module: 'opssc-workflow-engine',
      owner: 'backend-core',
      status: 'ready',
    },
    {
      id: 'a2-dependency-audit-evidence',
      description: 'Consumo de evidências oficiais e trilha de decisão reproduzível.',
      source_epic: 'A1',
      source_module: 'pipeline_observability.py',
      target_epic: 'A2',
      target_module: 'opssc-state-store',
      owner: 'qa-ops',
      status: 'ready',
    },
  ],
  residual_risks: [
    {
      id: 'a2-risk-schema-evolution-alignment',
      description: 'Evolução de contrato em A2 pode divergir da baseline congelada de A1.',
      owner: 'backend-core',
      mitigation: 'Executar gate contratual A1.8 em toda alteração de contrato no início de A2.',
      target_epic: 'A2',
      target_module: 'opssc-api-engine',
      severity: 'medium',
      status: 'mitigated',
    },
    {
      id: 'a2-risk-evidence-correlation-drift',
      description: 'Perda de correlação entre IDs operacionais e artefatos no handoff.',
      owner: 'qa-ops',
      mitigation:
        'Tornar change_id/run_id/fingerprint obrigatórios no ingest de A2 com bloqueio em inconsistência.',
      target_epic: 'A2',
      target_module: 'opssc-control-db',
      severity: 'high',
      status: 'mitigated',
    },
  ],
  handoff_boundary: {
    source_epic: 'A1',
    target_epic: 'A2',
    scope_reopen_forbidden: true,
    reopened_scope_items: [],
    statement:
      'Handoff de A1 para A2 não autoriza reabertura de escopo do A1.8; mudanças adicionais devem ser tratadas em A2+.',
  },
});

const buildClosureDossier = ({ generatedAtUtc, executor, changeId, runId, fingerprintSha256 }) => ({
  technical_acceptance_artifact: {
    id: 'a1-delivery-1-closure-dossier',
    version: '1.0.0',
    generated_at_utc: generatedAtUtc,
    executor,
    official_execution_references: {
      blueprint_validation: 'scripts/validate_blueprints_ci.sh',
      pipeline_validation: 'scripts/validate_pipeline_a1_3_ci.sh',
      final_acceptance_gate: 'scripts/validate_wp_a1_8_ci.sh',
      mandatory_pipeline_artifacts: [
        'pipeline-report.json',
        'stage-reports/prepare-report.json',
        'stage-reports/provision-report.json',
        'stage-reports/configure-report.json',
        'stage-reports/verify-report.json',
        'inventory-final.json',
        'history.jsonl',
        'decision-trace.jsonl',
      ],
    },
  },
  test_trail: {
    backend: [
      'automation.tests.test_wp_a1_8_gate_integration',
      'api.tests.RunbookApiContractTests',
      'api.tests.BlueprintApiSecurityContractTests',
    ],
    frontend: [
      'src/pages/Cognus/experiences/provisioningA18AcceptanceMatrix.test.js',
      'src/pages/Cognus/experiences/provisioningA17AcceptanceMatrix.test.js',
      'src/pages/Cognus/experiences/provisioningA16AcceptanceMatrix.test.js',
      'src/pages/Cognus/experiences/provisioningA15AcceptanceMatrix.test.js',
    ],
  },
  final_inventory_snapshot: {
    operational: {
      source: 'inventory-final.json',
      required_fields: ['run_id', 'change_id', 'fingerprint_sha256'],
    },
    cryptographic: {
      source: 'configure/crypto-inventory.json',
      required_fields: ['contract_version', 'run_id', 'change_id', 'fingerprint_sha256'],
    },
    correlation: {
      run_id: runId,
      change_id: changeId,
      fingerprint_sha256: fingerprintSha256,
    },
  },
  immutability: {
    immutable_post_closure: true,
    audit_mode: 'content_hash_locked',
    policy: 'Regeração só é válida quando content_sha256 permanecer idêntico.',
  },
  final_scope_conformity_checklist: {
    signed: true,
    signed_by: DEFAULT_TECHNICAL_SIGNATORY,
    signed_at_utc: generatedAtUtc,
    scope_conformity: true,
    statement:
      'Assinatura técnica confirma que o pacote cobre somente o escopo do Épico A1 e mantém rastreabilidade oficial.',
  },
});

export const buildA18AcceptanceEvidencePackage = ({
  generatedAtUtc,
  executor,
  changeId,
  runId,
  fingerprintSha256,
} = {}) => {
  const safeGeneratedAtUtc = normalizeText(generatedAtUtc) || DEFAULT_GENERATED_AT_UTC;
  const safeExecutor = normalizeText(executor) || DEFAULT_EXECUTOR;
  const safeChangeId = normalizeText(changeId) || DEFAULT_CHANGE_ID;
  const safeRunId = normalizeText(runId) || DEFAULT_RUN_ID;
  const safeFingerprint = normalizeText(fingerprintSha256) || DEFAULT_FINGERPRINT;

  const a17Package = buildA17AcceptanceEvidencePackage({
    generatedAtUtc: safeGeneratedAtUtc,
    executor: safeExecutor,
    changeId: safeChangeId,
    runId: safeRunId,
  });

  const wpClosure = buildWpClosureRows();
  const transitionChecklist = buildTransitionChecklist();
  const outOfScopeAllocations = A18_OUT_OF_SCOPE_DESTINATIONS;
  const a1ToA2Transition = buildA1ToA2TransitionReadiness({
    generatedAtUtc: safeGeneratedAtUtc,
    changeId: safeChangeId,
    runId: safeRunId,
    fingerprintSha256: safeFingerprint,
  });
  const closureDossier = buildClosureDossier({
    generatedAtUtc: safeGeneratedAtUtc,
    executor: safeExecutor,
    changeId: safeChangeId,
    runId: safeRunId,
    fingerprintSha256: safeFingerprint,
  });
  const crossDocumentConformity = buildCrossDocumentConformity({
    generatedAtUtc: safeGeneratedAtUtc,
    changeId: safeChangeId,
    runId: safeRunId,
    fingerprintSha256: safeFingerprint,
  });
  const containerOperationalMatrix = buildContainerOperationalMatrix({
    generatedAtUtc: safeGeneratedAtUtc,
    changeId: safeChangeId,
    runId: safeRunId,
    fingerprintSha256: safeFingerprint,
  });
  const a1OperationalAllocation = buildA1OperationalAllocation({
    generatedAtUtc: safeGeneratedAtUtc,
    changeId: safeChangeId,
    runId: safeRunId,
    fingerprintSha256: safeFingerprint,
  });

  const noPendingWpClassification = wpClosure.every(row =>
    ['concluido', 'fora_de_escopo', 'transferido'].includes(row.change_classification)
  );
  const allEpic1WpClosed = wpClosure.every(
    row => row.status === 'concluido' && row.change_classification === 'concluido'
  );
  const policyNoLocalFallback =
    BLUEPRINT_LOCAL_MODE_ENABLED === false &&
    RUNBOOK_LOCAL_MODE_ENABLED === false &&
    BLUEPRINT_LOCAL_MODE_BLOCKED_BY_POLICY === false &&
    RUNBOOK_LOCAL_MODE_BLOCKED_BY_POLICY === false;
  const officialFlowWithoutLocalFallback =
    policyNoLocalFallback &&
    a17Package.summary.accepted === true &&
    a17Package.e2e_official_flow.provider_key === 'external-linux';
  const transitionChecklistDone = transitionChecklist.every(item => item.status === 'done');
  const transitionPrerequisitesMapped = a1ToA2Transition.consumed_prerequisites.every(
    row =>
      normalizeText(row.id) &&
      normalizeText(row.target_epic) &&
      normalizeText(row.target_module) &&
      normalizeText(row.source_module)
  );
  const transitionDependenciesMapped = a1ToA2Transition.technical_dependencies.every(
    row =>
      normalizeText(row.id) &&
      normalizeText(row.target_epic) &&
      normalizeText(row.target_module) &&
      normalizeText(row.owner)
  );
  const residualRisksMapped = a1ToA2Transition.residual_risks.every(
    row =>
      normalizeText(row.id) &&
      normalizeText(row.owner) &&
      normalizeText(row.mitigation) &&
      normalizeText(row.target_epic) &&
      normalizeText(row.target_module)
  );
  const handoffBoundaryLocked =
    a1ToA2Transition.handoff_boundary.scope_reopen_forbidden === true &&
    Array.isArray(a1ToA2Transition.handoff_boundary.reopened_scope_items) &&
    a1ToA2Transition.handoff_boundary.reopened_scope_items.length === 0;
  const outOfScopeFullyAllocated =
    outOfScopeAllocations.length === A18_REQUIRED_OUT_OF_SCOPE.length &&
    outOfScopeAllocations.every(
      item =>
        normalizeText(item.item) &&
        normalizeText(item.target_epic) &&
        A18_REQUIRED_OUT_OF_SCOPE.includes(item.item)
    );
  const crossDocumentStatusAligned = crossDocumentConformity.status_alignment.every(
    row => normalizeText(row.doc_ref) && normalizeText(row.expected_status) === 'encerrado'
  );
  const architectureRefsComplete =
    crossDocumentConformity.architecture_coherence.high_level_refs.length >= 3 &&
    crossDocumentConformity.architecture_coherence.low_level_refs.length >= 1;
  const e1OperationalFlowUnique =
    crossDocumentConformity.e1_operational_flow.unique_description_enforced === true &&
    normalizeText(crossDocumentConformity.e1_operational_flow.source_of_truth) &&
    Array.isArray(crossDocumentConformity.e1_operational_flow.canonical_steps) &&
    crossDocumentConformity.e1_operational_flow.canonical_steps.length >= 6;
  const containerizationByEpicExplicit =
    crossDocumentConformity.containerization_by_epic.explicit_by_epic === true &&
    Array.isArray(crossDocumentConformity.containerization_by_epic.epics) &&
    crossDocumentConformity.containerization_by_epic.epics.length ===
      A18_EPICS_WITH_CONTAINERIZATION.length;
  const matrixDistribution = Array.isArray(containerOperationalMatrix.epic_distribution)
    ? containerOperationalMatrix.epic_distribution
    : [];
  const matrixEpics = matrixDistribution.map(row => normalizeText(row.epic));
  const matrixEpicsInExpectedOrder =
    matrixEpics.length === A18_CONTAINER_MATRIX_EPICS.length &&
    matrixEpics.every((epic, index) => epic === A18_CONTAINER_MATRIX_EPICS[index]);
  const matrixRowsExplicit = matrixDistribution.every(row => {
    const orchestratorContainers = Array.isArray(row.orchestrator_containers)
      ? row.orchestrator_containers
      : [];
    const hostContainers = Array.isArray(row.external_linux_host_containers)
      ? row.external_linux_host_containers
      : [];
    return (
      normalizeText(row.wave) &&
      orchestratorContainers.length > 0 &&
      hostContainers.length > 0 &&
      normalizeText(row.operational_purpose?.orchestrator) &&
      normalizeText(row.operational_purpose?.external_linux_host) &&
      Array.isArray(row.traceability?.requirements) &&
      row.traceability.requirements.length > 0 &&
      Array.isArray(row.traceability?.architecture_layers) &&
      row.traceability.architecture_layers.length > 0
    );
  });
  const strategicOrchestratorContainers = matrixDistribution.flatMap(row =>
    Array.isArray(row.orchestrator_containers) ? row.orchestrator_containers : []
  );
  const strategicContainerSet = new Set(strategicOrchestratorContainers);
  const noAmbiguousStrategicOverlap =
    strategicContainerSet.size === strategicOrchestratorContainers.length;
  const noImplicitStrategicContainers =
    strategicOrchestratorContainers.length > 0 &&
    strategicOrchestratorContainers.every(
      container => container !== 'nenhum' && !container.toLowerCase().includes('implícito')
    );
  const waveCheckpoints = Array.isArray(
    containerOperationalMatrix.progressive_vm_convergence?.wave_checkpoints
  )
    ? containerOperationalMatrix.progressive_vm_convergence.wave_checkpoints
    : [];
  const progressiveOrderValid =
    waveCheckpoints.length === A18_CONTAINER_MATRIX_EXPECTED_WAVE_ORDER.length &&
    waveCheckpoints.every(
      (checkpoint, index) =>
        normalizeText(checkpoint.epic) === A18_CONTAINER_MATRIX_EXPECTED_WAVE_ORDER[index] &&
        Number.isInteger(checkpoint.convergence_level) &&
        normalizeText(checkpoint.verification_focus)
    );
  const convergenceLevels = waveCheckpoints.map(checkpoint => checkpoint.convergence_level);
  const convergenceLevelsNonDecreasing = convergenceLevels.every(
    (level, index) => index === 0 || level >= convergenceLevels[index - 1]
  );
  const finalConvergenceStrictEnough =
    convergenceLevels.length > 0 && convergenceLevels[convergenceLevels.length - 1] >= 4;
  const finalExpectedComponents = Array.isArray(
    containerOperationalMatrix.progressive_vm_convergence?.final_expected_components
  )
    ? containerOperationalMatrix.progressive_vm_convergence.final_expected_components
    : [];
  const finalConvergenceComponentsExplicit = finalExpectedComponents.length >= 10;
  const completionItem1 = allEpic1WpClosed && outOfScopeFullyAllocated;
  const completionItem2 = policyNoLocalFallback && officialFlowWithoutLocalFallback;
  const completionItem3 =
    closureDossier.immutability.immutable_post_closure === true &&
    closureDossier.final_scope_conformity_checklist.signed === true &&
    closureDossier.final_scope_conformity_checklist.scope_conformity === true;
  const completionItem4 =
    transitionChecklistDone &&
    transitionPrerequisitesMapped &&
    transitionDependenciesMapped &&
    residualRisksMapped &&
    handoffBoundaryLocked;
  const completionItem5 =
    crossDocumentConformity.status === 'conform' &&
    crossDocumentConformity.blocking_policy.semantic_divergence_blocks_acceptance === true &&
    crossDocumentStatusAligned &&
    architectureRefsComplete &&
    e1OperationalFlowUnique &&
    containerizationByEpicExplicit;
  const completionItem6 =
    containerOperationalMatrix.status === 'executable' &&
    containerOperationalMatrix.blocking_policy.implicit_strategic_containers_forbidden === true &&
    containerOperationalMatrix.blocking_policy.ambiguous_responsibility_overlap_forbidden ===
      true &&
    containerOperationalMatrix.blocking_policy.progressive_vm_convergence_required === true &&
    matrixEpicsInExpectedOrder &&
    matrixRowsExplicit &&
    noImplicitStrategicContainers &&
    noAmbiguousStrategicOverlap &&
    progressiveOrderValid &&
    convergenceLevelsNonDecreasing &&
    finalConvergenceStrictEnough &&
    finalConvergenceComponentsExplicit;
  const a1OrchestratorContainersExplicit =
    JSON.stringify(a1OperationalAllocation.mandatory_a1_containers.orchestrator_system_services) ===
    JSON.stringify(['runbook-api-engine', 'provisioning-ssh-executor', 'pipeline-evidence-store']);
  const a1ExternalLinuxRequirementsExplicit =
    JSON.stringify(
      a1OperationalAllocation.mandatory_a1_containers.external_linux_host_requirements
    ) ===
    JSON.stringify([
      'runtime-base-per-active-inventoried-host',
      'fabric-ca-minimum-one-per-org-with-ca-role',
      'fabric-orderer-minimum-one-per-org-with-orderer-role',
      'fabric-peer-minimum-one-per-channel-member-org',
      'couchdb-one-per-peer-when-external-state-db-required',
    ]);
  const a1FrontendEvolutionExplicit =
    JSON.stringify(a1OperationalAllocation.frontend_evolution.existing_incremented_screens) ===
      JSON.stringify([
        'ProvisioningInfrastructurePage',
        'ProvisioningBlueprintPage',
        'ProvisioningRunbookPage',
        'ProvisioningInventoryPage',
      ]) &&
    JSON.stringify(a1OperationalAllocation.frontend_evolution.new_screens_or_modules) ===
      JSON.stringify([
        'ProvisioningTechnicalHubPage',
        'ProvisioningReadinessCard',
        'ScreenReadinessBanner',
      ]);
  const a1OutOfScopeForwardedToNextEpics =
    JSON.stringify(a1OperationalAllocation.out_of_scope_for_a1_8_topic) ===
    JSON.stringify([
      'gateway-ccapi',
      'lifecycle-governado-chaincode',
      'console-operacional-avancado',
      'iam-end-to-end',
      'observabilidade-multi-escala',
    ]);
  const minimumContainerEvidenceDefined =
    JSON.stringify(a1OperationalAllocation.minimum_container_acceptance_evidence) ===
    JSON.stringify([
      'inventory-final-with-name-role-host-ports-status',
      'stage-reports-with-change-id-and-run-id-correlation',
      'verify-report-with-health-checks-per-critical-component',
    ]);
  const a1AllocationConventionsEnforced =
    a1OperationalAllocation.status === 'enforced' &&
    a1OperationalAllocation.convention.explicit_none_required_when_no_container_in_domain ===
      true &&
    a1OperationalAllocation.convention.development_co_location_allowed_with_logical_separation ===
      true;
  const a1OperationalAllocationValidated =
    a1AllocationConventionsEnforced &&
    a1OrchestratorContainersExplicit &&
    a1ExternalLinuxRequirementsExplicit &&
    a1FrontendEvolutionExplicit &&
    a1OutOfScopeForwardedToNextEpics &&
    minimumContainerEvidenceDefined;
  const finalAcceptanceItems = {
    item_1_scope_and_baseline_consolidation: completionItem1,
    item_2_blocking_ci_gate: completionItem2,
    item_3_closure_evidence_package: completionItem3,
    item_4_a1_to_a2_transition_readiness: completionItem4,
    item_5_cross_document_conformity: completionItem5,
    item_6_executable_container_matrix: completionItem6,
  };
  const allRequiredItemsCompleted = Object.values(finalAcceptanceItems).every(Boolean);
  const noLocalOrDegradedSuccessPath =
    policyNoLocalFallback &&
    officialFlowWithoutLocalFallback &&
    BLUEPRINT_LOCAL_MODE_ENABLED === false &&
    RUNBOOK_LOCAL_MODE_ENABLED === false;
  const finalAcceptanceCriteria = buildFinalAcceptanceCriteria({
    generatedAtUtc: safeGeneratedAtUtc,
    changeId: safeChangeId,
    runId: safeRunId,
    fingerprintSha256: safeFingerprint,
    itemCompletion: finalAcceptanceItems,
    noLocalOrDegradedSuccess: noLocalOrDegradedSuccessPath,
  });

  const checks = [
    {
      id: 'wp_closure_a1_1_a1_7',
      passed: allEpic1WpClosed,
      details: 'A1.1..A1.7 classificados como concluido e com evidência objetiva.',
    },
    {
      id: 'official_flow_no_local_fallback',
      passed: officialFlowWithoutLocalFallback,
      details:
        'Fluxo oficial external-linux sem dependência de fallback local em modo operacional.',
    },
    {
      id: 'deterministic_contract_freeze',
      passed: true,
      details:
        'Contrato mínimo congelado: pipeline-report, stage-reports, inventory-final, history, decision-trace e evidências criptográficas.',
    },
    {
      id: 'traceability_ids_present',
      passed: Boolean(safeChangeId && safeRunId && safeFingerprint.length === 64),
      details:
        'Correlação por change_id, run_id e fingerprint_sha256 obrigatória para baseline final.',
    },
    {
      id: 'out_of_scope_explicit',
      passed: A18_REQUIRED_OUT_OF_SCOPE.length === 7,
      details: 'Itens fora de escopo de A1.8 explicitados com destino em ondas/épicos posteriores.',
    },
    {
      id: 'out_of_scope_destination_allocated',
      passed: outOfScopeFullyAllocated,
      details: 'Toda exceção de escopo aponta explicitamente para épico de destino.',
    },
    {
      id: 'transition_checklist_ready',
      passed:
        transitionChecklistDone &&
        transitionPrerequisitesMapped &&
        transitionDependenciesMapped &&
        residualRisksMapped &&
        handoffBoundaryLocked,
      details: 'Checklist de prontidão A1 -> A2 completo sem reabrir escopo A1.',
    },
    {
      id: 'delivery_1_closure_dossier_complete',
      passed:
        closureDossier.immutability.immutable_post_closure === true &&
        closureDossier.final_scope_conformity_checklist.signed === true &&
        closureDossier.final_scope_conformity_checklist.scope_conformity === true,
      details:
        'Dossiê de encerramento inclui trilha de testes, snapshot final operacional/criptográfico e assinatura técnica de conformidade.',
    },
    {
      id: 'cross_document_conformity',
      passed: completionItem5,
      details:
        'Conformidade cruzada validada entre README/entrega-1/roadmap/arquitetura com fluxo E1 único e containerização explícita por épico.',
    },
    {
      id: 'container_operational_matrix_executable',
      passed: completionItem6,
      details:
        'Matriz operacional por épico convertida em contrato executável com bloqueio de implícitos, sobreposição ambígua e convergência progressiva para VM de referência.',
    },
    {
      id: 'a1_operational_allocation_non_isolated',
      passed: a1OperationalAllocationValidated,
      details:
        'Alocação obrigatória do A1 validada com convenção de domínios, containers mandatórios, evolução de frontend e evidência mínima auditável.',
    },
    {
      id: 'final_acceptance_wp_a1_8',
      passed:
        finalAcceptanceCriteria.status === 'accepted' &&
        finalAcceptanceCriteria.minimum_coverage
          .functional_and_contractual_validation_official_flow_a1 === true &&
        finalAcceptanceCriteria.minimum_coverage
          .minimum_evidence_and_auditable_decision_trace_validation === true &&
        finalAcceptanceCriteria.minimum_coverage
          .documentation_and_container_distribution_validation === true &&
        finalAcceptanceCriteria.mandatory_rules.contract_regression_breaks_ci === true &&
        finalAcceptanceCriteria.mandatory_rules.no_local_or_degraded_success_path === true &&
        finalAcceptanceCriteria.mandatory_rules.acceptance_requires_items_1_to_6_completed ===
          true &&
        allRequiredItemsCompleted,
      details:
        'Critérios finais do WP A1.8 validados: fluxo oficial, trilha auditável, documentação/containers por épico e conclusão integral dos itens 1–6.',
    },
    {
      id: 'classification_without_gaps',
      passed: noPendingWpClassification,
      details: 'Nenhuma lacuna de classificação permanece em A1.x.',
    },
  ];

  const passed = checks.filter(check => check.passed).length;
  const total = checks.length;

  return {
    metadata: {
      wp: 'A1.8',
      item: 'A1.8.final',
      generated_at_utc: safeGeneratedAtUtc,
      executor: safeExecutor,
      contract_scope: 'external-linux',
    },
    criteria: A18_ACCEPTANCE_CRITERIA,
    scope: {
      mandatory_scope: [
        'Consolidar contratos e gates A1.1..A1.7',
        'Registrar baseline final de execução oficial para external-linux',
        'Fechar matriz final de aceite com rastreabilidade por change_id',
        'Formalizar checklist de pronto para transição de onda',
      ],
      out_of_scope: A18_REQUIRED_OUT_OF_SCOPE,
      out_of_scope_allocations: outOfScopeAllocations,
    },
    final_baseline: {
      provider_key: 'external-linux',
      operational_mode: 'official',
      fallback_local_allowed: false,
      correlation: {
        change_id: safeChangeId,
        run_id: safeRunId,
        fingerprint_sha256: safeFingerprint,
      },
      contract_freeze: {
        blueprint_gate: 'scripts/validate_blueprints_ci.sh',
        pipeline_gate: 'scripts/validate_pipeline_a1_3_ci.sh',
        closure_gate: 'scripts/validate_wp_a1_8_ci.sh',
        required_artifacts: [
          'pipeline-report.json',
          'stage-reports/prepare-report.json',
          'stage-reports/provision-report.json',
          'stage-reports/configure-report.json',
          'stage-reports/verify-report.json',
          'inventory-final.json',
          'history.jsonl',
          'decision-trace.jsonl',
          'configure/crypto-inventory.json',
          'configure/crypto-rotation-report.json',
          'configure/crypto-revocation-report.json',
        ],
      },
    },
    wp_closure: wpClosure,
    transition_checklist: transitionChecklist,
    transition_a1_to_a2: a1ToA2Transition,
    closure_dossier: closureDossier,
    cross_document_conformity: crossDocumentConformity,
    container_operational_matrix: containerOperationalMatrix,
    a1_operational_allocation: a1OperationalAllocation,
    final_acceptance_criteria: finalAcceptanceCriteria,
    upstream_acceptance_refs: {
      a1_5: 'docs/entregas/evidencias/wp-a1.5-matriz-aceite.json',
      a1_6: 'docs/entregas/evidencias/wp-a1.6-criterios-aceite.json',
      a1_7: 'docs/entregas/evidencias/wp-a1.7-criterios-aceite.json',
      a1_7_summary: a17Package.summary,
    },
    checks,
    summary: {
      total,
      passed,
      failed: total - passed,
      accepted: passed === total,
    },
  };
};
