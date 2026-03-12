import {
  IS_PROVISIONING_OPERATIONAL_ENV,
  PROVISIONING_DEGRADED_MODE_ALLOWED,
  PROVISIONING_ENV_PROFILE,
} from '../../../services/provisioningEnvironmentPolicy';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const READINESS_TEXT_EN_US = Object.freeze({
  implementado: 'implemented',
  parcial: 'partial',
  pendente: 'pending',
  'Endpoint do orquestrador': 'Orchestrator endpoint',
  'Hibrido (orquestrador + degradacao controlada)':
    'Hybrid (orchestrator + controlled degradation)',
  'Simulado (sem endpoint do orquestrador)': 'Simulated (without orchestrator endpoint)',
  Indisponivel: 'Unavailable',
  'Escopo tecnico obrigatorio': 'Mandatory technical scope',
  'Fluxo E1 restrito a external provider + VM Linux. Integracoes fora desse recorte permanecem bloqueadas.':
    'E1 flow restricted to external provider + Linux VM. Integrations outside this scope remain blocked.',
  oficial: 'official',
  degradado: 'degraded',
  'Tela guiada de infraestrutura SSH concluida no escopo A1, integrada ao backend de runbook do orquestrador (start/operate/status).':
    'Guided SSH infrastructure screen completed in scope A1, integrated with the orchestrator runbook backend (start/operate/status).',
  'Validar formulario de infraestrutura': 'Validate infrastructure form',
  'Validacao operacional ocorre no frontend com gate tecnico integrado ao fluxo de execucao do orquestrador.':
    'Operational validation runs in the frontend with a technical gate integrated into the orchestrator execution flow.',
  'Exportar rascunho tecnico': 'Export technical draft',
  'Exportacao tecnica permanece disponivel como artefato operacional integrado ao fluxo do orquestrador.':
    'Technical export remains available as an operational artifact integrated with the orchestrator flow.',
  'Iniciar provisao SSH (runbook)': 'Start SSH provisioning (runbook)',
  'Execucao ocorre via pipeline prepare -> provision -> reconcile -> verify no endpoint de runbook do orquestrador.':
    'Execution runs through the prepare -> provision -> reconcile -> verify pipeline in the orchestrator runbook endpoint.',
  'Formulario operacional de infraestrutura': 'Operational infrastructure form',
  'Fonte local para captura de hosts/credenciais com handoff para execucao do orquestrador.':
    'Local source for capturing hosts/credentials with handoff to orchestrator execution.',
  'Pipeline de provisao (runbook)': 'Provisioning pipeline (runbook)',
  'Execucao consolidada via endpoints de runbook com correlacao por change_id/run_id.':
    'Consolidated execution through runbook endpoints with correlation by change_id/run_id.',
  'Jornada tecnica avancada passo a passo com seis etapas (blueprint, runbook, inventario, lifecycle, guardrails e rollback).':
    'Advanced step-by-step technical journey with six stages (blueprint, runbook, inventory, lifecycle, guardrails, and rollback).',
  'Etapa 1: Blueprint e versionamento': 'Step 1: Blueprint and versioning',
  'Navegacao direta para tela tecnica integrada com lint e versionamento do orquestrador.':
    'Direct navigation to the technical screen integrated with orchestrator lint and versioning.',
  'Etapa 2: Provisao assistida': 'Step 2: Assisted provisioning',
  'Navegacao direta para execucao assistida com comandos do orquestrador no backend.':
    'Direct navigation to assisted execution with orchestrator commands in the backend.',
  'Etapa 3: Inventario inicial': 'Step 3: Initial inventory',
  'Navegacao direta para inventario com fontes do orquestrador e fallback explicitamente sinalizado.':
    'Direct navigation to inventory with orchestrator sources and explicitly signaled fallback.',
  'Etapa 4: Lifecycle de chaincode (Gateway API)':
    'Step 4: Chaincode lifecycle (Gateway API)',
  'Tela de lifecycle disponivel com integracao parcial; fluxo governado completo segue em evolucao.':
    'Lifecycle screen available with partial integration; the complete governed flow remains under development.',
  'Etapa 5: Guardrails de disponibilidade': 'Step 5: Availability guardrails',
  'Tela acessivel para definicao de guardrails, com operacoes backend ainda pendentes nesta entrega.':
    'Screen accessible for guardrail definition, with backend operations still pending in this delivery.',
  'Etapa 6: Versoes e rollback': 'Step 6: Versions and rollback',
  'Consulta de versoes esta parcialmente integrada e rollback governado completo permanece em evolucao.':
    'Version lookup is partially integrated and full governed rollback remains under development.',
  'Trilha tecnica A1': 'A1 technical trail',
  'Entrada unica para as tres etapas de provisao tecnica base da Entrega 1.':
    'Single entry point for the three base technical provisioning stages of Delivery 1.',
  'Trilha tecnica avancada acoplada': 'Coupled advanced technical trail',
  'A jornada avancada referencia lifecycle/guardrails/rollback para continuidade operacional em um fluxo unico.':
    'The advanced journey references lifecycle/guardrails/rollback for operational continuity in a single flow.',
  'Lint, publicacao e historico de versoes usam endpoints do orquestrador; importacao e rascunho permanecem locais.':
    'Lint, publication, and version history use orchestrator endpoints; import and draft remain local.',
  'Importar JSON/YAML': 'Import JSON/YAML',
  'Importacao ocorre em memoria local porque nao existe endpoint de upload de blueprint no orquestrador para a E1.':
    'Import occurs in local memory because there is no blueprint upload endpoint in the orchestrator for E1.',
  'Executar lint pre-plano': 'Run pre-plan lint',
  'Validacao executada pelo endpoint de lint A1.2 no orquestrador, com fallback bloqueante se contrato falhar.':
    'Validation executed by the A1.2 lint endpoint in the orchestrator, with blocking fallback if the contract fails.',
  'Publicar versao': 'Publish version',
  'Publicacao validada e persistida no endpoint de versionamento de blueprint do orquestrador.':
    'Publication validated and persisted in the orchestrator blueprint versioning endpoint.',
  'Novo rascunho': 'New draft',
  'Criacao de rascunho ocorre somente no frontend para preparacao de documento antes do lint.':
    'Draft creation happens only in the frontend for document preparation before lint.',
  'Lint de blueprint': 'Blueprint lint',
  'Fonte do orquestrador para diagnostico tecnico A1.2.':
    'Orchestrator source for A1.2 technical diagnostics.',
  'Historico de versoes publicadas': 'Published version history',
  'Fonte do historico publicado, consumida diretamente do backend do orquestrador.':
    'Source of the published history, consumed directly from the orchestrator backend.',
  'Execucao do runbook base implementada com comandos start/pause/resume/advance/fail/retry e sincronizacao por run_id.':
    'Base runbook execution implemented with start/pause/resume/advance/fail/retry commands and synchronization by run_id.',
  'Iniciar runbook': 'Start runbook',
  'Inicio executa o pipeline prepare -> provision -> reconcile -> verify no backend.':
    'Start executes the prepare -> provision -> reconcile -> verify pipeline in the backend.',
  Pausar: 'Pause',
  'Pausa por checkpoint via endpoint de operacao do runbook.':
    'Pause by checkpoint through the runbook operation endpoint.',
  Retomar: 'Resume',
  'Retomada do ultimo checkpoint valido via endpoint de operacao do runbook.':
    'Resume from the latest valid checkpoint through the runbook operation endpoint.',
  'Avancar checkpoint': 'Advance checkpoint',
  'Avanco de checkpoint com persistencia de estado no backend.':
    'Checkpoint advance with persisted state in the backend.',
  'Simular falha': 'Simulate failure',
  'Registro de falha tecnica para troubleshooting e retomada segura.':
    'Technical failure registration for troubleshooting and safe resume.',
  'Reexecucao segura': 'Safe re-execution',
  'Retry controlado por checkpoint valido no backend.':
    'Retry controlled by a valid checkpoint in the backend.',
  'Estado do pipeline': 'Pipeline state',
  'Fonte por run_id para status consolidado e timeline operacional.':
    'Source by run_id for consolidated status and operational timeline.',
  'Inventario combina endpoints legados (organizacoes/nos) com trilha de evidencias por runbook para auditoria operacional.':
    'Inventory combines legacy endpoints (organizations/nodes) with runbook evidence trail for operational auditing.',
  'Sincronizar manualmente': 'Synchronize manually',
  'Sincronizacao usa organizacoes/nos do orquestrador quando disponiveis e fallback local para fontes sem endpoint.':
    'Synchronization uses orchestrator organizations/nodes when available and local fallback for sources without endpoint.',
  'Exportar snapshot': 'Export snapshot',
  'Exportacao ocorre sobre snapshot consolidado da tela, com marcacao explicita das fontes do orquestrador e mockadas.':
    'Export occurs over the screen consolidated snapshot, with explicit marking of orchestrator and mocked sources.',
  'Fechar execucao E1': 'Close E1 execution',
  'Fechamento usa gate de evidencias do runbook; bloqueia quando artefatos minimos/auditoria estao incompletos.':
    'Closure uses the runbook evidence gate; blocks when minimum artifacts/auditing are incomplete.',
  'Exportar evidencias': 'Export evidence',
  'Exportacao consolida painel derivado do estado do runbook (status/artifacts) com bloqueio por gate tecnico.':
    'Export consolidates a panel derived from runbook state (status/artifacts) with blocking by technical gate.',
  Organizacoes: 'Organizations',
  'Consulta endpoint do orquestrador quando disponivel; usa fallback local somente em indisponibilidade.':
    'Queries the orchestrator endpoint when available; uses local fallback only on unavailability.',
  Nos: 'Nodes',
  'Snapshot agregado de inventario': 'Aggregated inventory snapshot',
  'Agregacao local continua para exibicao de hosts/canais/baseline, com evidencias finais vinculadas ao runbook.':
    'Local aggregation continues for displaying hosts/channels/baseline, with final evidence linked to the runbook.',
  'Evidencias por run_id': 'Evidence by run_id',
  'Fonte oficial para reconcile-report, inventory-final, verify-report, ssh-execution-log e stage-reports usados no gate auditavel A2.':
    'Official source for reconcile-report, inventory-final, verify-report, ssh-execution-log, and stage-reports used in the auditable A2 gate.',
  'Visão consolidada da topologia runtime por organização com status baseado exclusivamente em inventário/evidência oficial do runbook.':
    'Consolidated runtime topology view by organization with status based exclusively on official inventory/runbook evidence.',
  'Carregar topologia oficial por run_id': 'Load official topology by run_id',
  'A tela consulta apenas o endpoint oficial de status do runbook para compor convergência por componente.':
    'The screen queries only the official runbook status endpoint to build per-component convergence.',
  'Filtrar por host/tipo/status/criticidade': 'Filter by host/type/status/criticality',
  'Filtros operam sobre dataset oficial já carregado do backend, sem mutação de estado técnico.':
    'Filters operate on the official dataset already loaded from the backend, without mutating technical state.',
  'Abrir inventário inicial correlacionado': 'Open correlated initial inventory',
  'Navegação para conferência detalhada de evidências e artifacts oficiais correlacionados por run_id.':
    'Navigation for detailed verification of official evidence and artifacts correlated by run_id.',
  'Reabrir runbook oficial correlacionado': 'Reopen correlated official runbook',
  'Navegação para continuidade operacional no pipeline oficial prepare -> provision -> reconcile -> verify.':
    'Navigation for operational continuity in the official prepare -> provision -> reconcile -> verify pipeline.',
  'Status oficial do runbook': 'Official runbook status',
  'Fonte canônica para contexto de execução, stages, artifact_rows, official_decision e correlação change_id/run_id.':
    'Canonical source for execution context, stages, artifact_rows, official_decision, and change_id/run_id correlation.',
  'Catálogo de topologia por organização': 'Topology catalog by organization',
  'Estrutura topology_catalog/api_registry oficial usada para compor peers, orderers, CA, couch, API gateway, netapi e runtimes de chaincode.':
    'Official topology_catalog/api_registry structure used to compose peers, orderers, CA, couch, API gateway, netapi, and chaincode runtimes.',
  'Evidências de host/stage por componente': 'Host/stage evidence by component',
  'Status por componente deriva de host_inventory/host_stage_evidences e artifacts oficiais, sem fallback simulado.':
    'Per-component status derives from host_inventory/host_stage_evidences and official artifacts, without simulated fallback.',
  'Workspace oficial do canal com projeção por organização membro e drill-down técnico por componente correlacionado ao channel_id.':
    'Official channel workspace with projection by member organization and technical drill-down by component correlated to channel_id.',
  'Carregar workspace oficial do canal': 'Load official channel workspace',
  'A tela depende exclusivamente do status oficial do runbook para projetar membros, chaincodes e componentes do canal.':
    'The screen depends exclusively on the official runbook status to project channel members, chaincodes, and components.',
  'Inspecionar componente do canal': 'Inspect channel component',
  'Cada clique de componente executa consulta oficial de runtime-inspection, preservando freshness/cache/source e sanitização.':
    'Each component click executes an official runtime-inspection query, preserving freshness/cache/source and sanitization.',
  'Reabrir topologia runtime correlacionada': 'Reopen correlated runtime topology',
  'Permite retomar a visão macro oficial da organização/canal sem depender de snapshots locais no frontend.':
    'Allows resuming the official macro view of the organization/channel without depending on local frontend snapshots.',
  'Read model oficial da organização': 'Official organization read model',
  'Usado para projetar membros do canal, chaincodes e saúde operacional observada por organização.':
    'Used to project channel members, chaincodes, and observed operational health by organization.',
  'Runtime inspection oficial': 'Official runtime inspection',
  'Consulta técnica oficial por componente com logs sanitizados, freshness, cache_hit/miss e inspection_source.':
    'Official technical query per component with sanitized logs, freshness, cache_hit/miss, and inspection_source.',
  'Readiness de backend nao mapeado para esta tela.': 'Backend readiness not mapped for this screen.',
  'Ambiente de desenvolvimento: fallback degradado habilitado e explicitamente sinalizado.':
    'Development environment: degraded fallback enabled and explicitly signaled.',
  'Ambiente operacional: fallback degradado proibido para semântica inequívoca de execução.':
    'Operational environment: degraded fallback forbidden for unambiguous execution semantics.',
  'Fluxo em modo do orquestrador sem dependência de fallback degradado ativo.':
    'Flow in orchestrator mode without dependence on active degraded fallback.',
  habilitado: 'enabled',
  bloqueado: 'blocked',
});

const localizeReadinessText = (value, localeCandidate = resolveCognusLocale()) => {
  const normalizedValue = String(value || '').trim();
  if (!normalizedValue) {
    return normalizedValue;
  }
  return pickCognusText(
    normalizedValue,
    READINESS_TEXT_EN_US[normalizedValue] || normalizedValue,
    localeCandidate
  );
};

export const READINESS_STATUS = Object.freeze({
  implemented: 'implementado',
  partial: 'parcial',
  pending: 'pendente',
});

export const READINESS_STATUS_TONE_MAP = Object.freeze({
  implementado: 'success',
  parcial: 'processing',
  pendente: 'warning',
});

export const READINESS_EXECUTION_MODE = Object.freeze({
  official: 'endpoint_oficial',
  hybrid: 'hibrido',
  mock: 'mockado',
  unavailable: 'indisponivel',
});

export const READINESS_EXECUTION_MODE_LABEL = Object.freeze({
  endpoint_oficial: 'Endpoint do orquestrador',
  hibrido: 'Hibrido (orquestrador + degradacao controlada)',
  mockado: 'Simulado (sem endpoint do orquestrador)',
  indisponivel: 'Indisponivel',
});

export const READINESS_EXECUTION_MODE_TONE_MAP = Object.freeze({
  endpoint_oficial: 'success',
  hibrido: 'processing',
  mockado: 'warning',
  indisponivel: 'error',
});

export const PROVISIONING_SCOPE_ALERT = Object.freeze({
  title: localizeReadinessText('Escopo tecnico obrigatorio'),
  message: localizeReadinessText(
    'Fluxo E1 restrito a external provider + VM Linux. Integracoes fora desse recorte permanecem bloqueadas.'
  ),
});

const STATUS = READINESS_STATUS;
const MODE = READINESS_EXECUTION_MODE;

export const PROVISIONING_EXECUTION_STATE = Object.freeze({
  official: 'oficial',
  degraded: 'degradado',
});

const OFFICIAL_EXECUTION_MODE_SET = new Set([MODE.official, MODE.unavailable]);

const PROVISIONING_READINESS_CATALOG = Object.freeze({
  'e1-infra-ssh': Object.freeze({
    screenStatus: STATUS.implemented,
    summary:
      'Tela guiada de infraestrutura SSH concluida no escopo A1, integrada ao backend de runbook do orquestrador (start/operate/status).',
    actions: Object.freeze({
      validate_infra_form: Object.freeze({
        label: 'Validar formulario de infraestrutura',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.hybrid,
        reason:
          'Validacao operacional ocorre no frontend com gate tecnico integrado ao fluxo de execucao do orquestrador.',
      }),
      export_infra_seed: Object.freeze({
        label: 'Exportar rascunho tecnico',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.hybrid,
        reason:
          'Exportacao tecnica permanece disponivel como artefato operacional integrado ao fluxo do orquestrador.',
      }),
      start_ssh_provisioning: Object.freeze({
        label: 'Iniciar provisao SSH (runbook)',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/start',
        reason:
          'Execucao ocorre via pipeline prepare -> provision -> reconcile -> verify no endpoint de runbook do orquestrador.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'infra-form-model',
        label: 'Formulario operacional de infraestrutura',
        executionMode: MODE.hybrid,
        endpoint: '',
        note:
          'Fonte local para captura de hosts/credenciais com handoff para execucao do orquestrador.',
      }),
      Object.freeze({
        key: 'infra-runbook-official-endpoints',
        label: 'Pipeline de provisao (runbook)',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{start|operate|{run_id}/status}',
        note: 'Execucao consolidada via endpoints de runbook com correlacao por change_id/run_id.',
      }),
    ]),
  }),
  'e1-tecnico': Object.freeze({
    screenStatus: STATUS.partial,
    summary:
      'Jornada tecnica avancada passo a passo com seis etapas (blueprint, runbook, inventario, lifecycle, guardrails e rollback).',
    actions: Object.freeze({
      open_blueprint_screen: Object.freeze({
        label: 'Etapa 1: Blueprint e versionamento',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/provisioning/blueprints',
        reason:
          'Navegacao direta para tela tecnica integrada com lint e versionamento do orquestrador.',
      }),
      open_runbook_screen: Object.freeze({
        label: 'Etapa 2: Provisao assistida',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/provisioning/runbook',
        reason: 'Navegacao direta para execucao assistida com comandos do orquestrador no backend.',
      }),
      open_inventory_screen: Object.freeze({
        label: 'Etapa 3: Inventario inicial',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.hybrid,
        endpoint: '/provisioning/inventory',
        reason:
          'Navegacao direta para inventario com fontes do orquestrador e fallback explicitamente sinalizado.',
      }),
      open_lifecycle_screen: Object.freeze({
        label: 'Etapa 4: Lifecycle de chaincode (Gateway API)',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.hybrid,
        endpoint: '/chaincode-ops/lifecycle',
        reason:
          'Tela de lifecycle disponivel com integracao parcial; fluxo governado completo segue em evolucao.',
      }),
      open_guardrails_screen: Object.freeze({
        label: 'Etapa 5: Guardrails de disponibilidade',
        status: STATUS.pending,
        available: true,
        executionMode: MODE.mock,
        endpoint: '/chaincode-ops/guardrails',
        reason:
          'Tela acessivel para definicao de guardrails, com operacoes backend ainda pendentes nesta entrega.',
      }),
      open_rollback_screen: Object.freeze({
        label: 'Etapa 6: Versoes e rollback',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.hybrid,
        endpoint: '/chaincode-ops/rollback',
        reason:
          'Consulta de versoes esta parcialmente integrada e rollback governado completo permanece em evolucao.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'technical-hub-contract-a1',
        label: 'Trilha tecnica A1',
        executionMode: MODE.official,
        endpoint: '/provisioning/{blueprints|runbook|inventory}',
        note: 'Entrada unica para as tres etapas de provisao tecnica base da Entrega 1.',
      }),
      Object.freeze({
        key: 'technical-hub-contract-e3',
        label: 'Trilha tecnica avancada acoplada',
        executionMode: MODE.hybrid,
        endpoint: '/chaincode-ops/{lifecycle|guardrails|rollback}',
        note:
          'A jornada avancada referencia lifecycle/guardrails/rollback para continuidade operacional em um fluxo unico.',
      }),
    ]),
  }),
  'e1-blueprint': Object.freeze({
    screenStatus: STATUS.partial,
    summary:
      'Lint, publicacao e historico de versoes usam endpoints do orquestrador; importacao e rascunho permanecem locais.',
    actions: Object.freeze({
      import_blueprint: Object.freeze({
        label: 'Importar JSON/YAML',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.mock,
        reason:
          'Importacao ocorre em memoria local porque nao existe endpoint de upload de blueprint no orquestrador para a E1.',
      }),
      run_lint: Object.freeze({
        label: 'Executar lint pre-plano',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/blueprints/lint',
        reason:
          'Validacao executada pelo endpoint de lint A1.2 no orquestrador, com fallback bloqueante se contrato falhar.',
      }),
      publish_version: Object.freeze({
        label: 'Publicar versao',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/blueprints/publish',
        reason:
          'Publicacao validada e persistida no endpoint de versionamento de blueprint do orquestrador.',
      }),
      create_draft: Object.freeze({
        label: 'Novo rascunho',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.mock,
        reason:
          'Criacao de rascunho ocorre somente no frontend para preparacao de documento antes do lint.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'blueprint-lint-backend',
        label: 'Lint de blueprint',
        executionMode: MODE.official,
        endpoint: '/api/v1/blueprints/lint',
        note: 'Fonte do orquestrador para diagnostico tecnico A1.2.',
      }),
      Object.freeze({
        key: 'blueprint-version-history',
        label: 'Historico de versoes publicadas',
        executionMode: MODE.official,
        endpoint: '/api/v1/blueprints/versions',
        note: 'Fonte do historico publicado, consumida diretamente do backend do orquestrador.',
      }),
    ]),
  }),
  'e1-provisionamento': Object.freeze({
    screenStatus: STATUS.partial,
    summary:
      'Execucao do runbook base implementada com comandos start/pause/resume/advance/fail/retry e sincronizacao por run_id.',
    actions: Object.freeze({
      start_runbook: Object.freeze({
        label: 'Iniciar runbook',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/start',
        reason: 'Inicio executa o pipeline prepare -> provision -> reconcile -> verify no backend.',
      }),
      pause_runbook: Object.freeze({
        label: 'Pausar',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/operate',
        reason: 'Pausa por checkpoint via endpoint de operacao do runbook.',
      }),
      resume_runbook: Object.freeze({
        label: 'Retomar',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/operate',
        reason: 'Retomada do ultimo checkpoint valido via endpoint de operacao do runbook.',
      }),
      advance_checkpoint: Object.freeze({
        label: 'Avancar checkpoint',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/operate',
        reason: 'Avanco de checkpoint com persistencia de estado no backend.',
      }),
      simulate_failure: Object.freeze({
        label: 'Simular falha',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/operate',
        reason: 'Registro de falha tecnica para troubleshooting e retomada segura.',
      }),
      retry_checkpoint: Object.freeze({
        label: 'Reexecucao segura',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/operate',
        reason: 'Retry controlado por checkpoint valido no backend.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'runbook-official-pipeline',
        label: 'Estado do pipeline',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note: 'Fonte por run_id para status consolidado e timeline operacional.',
      }),
    ]),
  }),
  'e1-inventario': Object.freeze({
    screenStatus: STATUS.partial,
    summary:
      'Inventario combina endpoints legados (organizacoes/nos) com trilha de evidencias por runbook para auditoria operacional.',
    actions: Object.freeze({
      manual_sync: Object.freeze({
        label: 'Sincronizar manualmente',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.hybrid,
        reason:
          'Sincronizacao usa organizacoes/nos do orquestrador quando disponiveis e fallback local para fontes sem endpoint.',
      }),
      export_snapshot: Object.freeze({
        label: 'Exportar snapshot',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.hybrid,
        reason:
          'Exportacao ocorre sobre snapshot consolidado da tela, com marcacao explicita das fontes do orquestrador e mockadas.',
      }),
      close_execution: Object.freeze({
        label: 'Fechar execucao E1',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.hybrid,
        reason:
          'Fechamento usa gate de evidencias do runbook; bloqueia quando artefatos minimos/auditoria estao incompletos.',
      }),
      export_evidence: Object.freeze({
        label: 'Exportar evidencias',
        status: STATUS.partial,
        available: true,
        executionMode: MODE.hybrid,
        reason:
          'Exportacao consolida painel derivado do estado do runbook (status/artifacts) com bloqueio por gate tecnico.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'inventory-organizations',
        label: 'Organizacoes',
        executionMode: MODE.hybrid,
        endpoint: '/api/v1/organizations',
        note:
          'Consulta endpoint do orquestrador quando disponivel; usa fallback local somente em indisponibilidade.',
      }),
      Object.freeze({
        key: 'inventory-nodes',
        label: 'Nos',
        executionMode: MODE.hybrid,
        endpoint: '/api/v1/nodes',
        note:
          'Consulta endpoint do orquestrador quando disponivel; usa fallback local somente em indisponibilidade.',
      }),
      Object.freeze({
        key: 'inventory-aggregated-snapshot',
        label: 'Snapshot agregado de inventario',
        executionMode: MODE.hybrid,
        endpoint: '',
        note:
          'Agregacao local continua para exibicao de hosts/canais/baseline, com evidencias finais vinculadas ao runbook.',
      }),
      Object.freeze({
        key: 'inventory-runbook-official-evidence',
        label: 'Evidencias por run_id',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note:
          'Fonte oficial para reconcile-report, inventory-final, verify-report, ssh-execution-log e stage-reports usados no gate auditavel A2.',
      }),
    ]),
  }),
  'e1-topologia-runtime-org': Object.freeze({
    screenStatus: STATUS.implemented,
    summary:
      'Visão consolidada da topologia runtime por organização com status baseado exclusivamente em inventário/evidência oficial do runbook.',
    actions: Object.freeze({
      load_official_topology: Object.freeze({
        label: 'Carregar topologia oficial por run_id',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        reason:
          'A tela consulta apenas o endpoint oficial de status do runbook para compor convergência por componente.',
      }),
      filter_runtime_components: Object.freeze({
        label: 'Filtrar por host/tipo/status/criticidade',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        reason:
          'Filtros operam sobre dataset oficial já carregado do backend, sem mutação de estado técnico.',
      }),
      open_inventory_screen: Object.freeze({
        label: 'Abrir inventário inicial correlacionado',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/provisioning/inventory',
        reason:
          'Navegação para conferência detalhada de evidências e artifacts oficiais correlacionados por run_id.',
      }),
      open_runbook_screen: Object.freeze({
        label: 'Reabrir runbook oficial correlacionado',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/provisioning/runbook',
        reason:
          'Navegação para continuidade operacional no pipeline oficial prepare -> provision -> reconcile -> verify.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'runtime-topology-runbook-status',
        label: 'Status oficial do runbook',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note:
          'Fonte canônica para contexto de execução, stages, artifact_rows, official_decision e correlação change_id/run_id.',
      }),
      Object.freeze({
        key: 'runtime-topology-topology-catalog',
        label: 'Catálogo de topologia por organização',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note:
          'Estrutura topology_catalog/api_registry oficial usada para compor peers, orderers, CA, couch, API gateway, netapi e runtimes de chaincode.',
      }),
      Object.freeze({
        key: 'runtime-topology-host-evidences',
        label: 'Evidências de host/stage por componente',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note:
          'Status por componente deriva de host_inventory/host_stage_evidences e artifacts oficiais, sem fallback simulado.',
      }),
    ]),
  }),
  'e1-workspace-canal': Object.freeze({
    screenStatus: STATUS.implemented,
    summary:
      'Workspace oficial do canal com projeção por organização membro e drill-down técnico por componente correlacionado ao channel_id.',
    actions: Object.freeze({
      load_official_channel_workspace: Object.freeze({
        label: 'Carregar workspace oficial do canal',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        reason:
          'A tela depende exclusivamente do status oficial do runbook para projetar membros, chaincodes e componentes do canal.',
      }),
      inspect_channel_component: Object.freeze({
        label: 'Inspecionar componente do canal',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/runtime-inspection',
        reason:
          'Cada clique de componente executa consulta oficial de runtime-inspection, preservando freshness/cache/source e sanitização.',
      }),
      reopen_runtime_topology: Object.freeze({
        label: 'Reabrir topologia runtime correlacionada',
        status: STATUS.implemented,
        available: true,
        executionMode: MODE.official,
        endpoint: '/provisioning/runtime-topology',
        reason:
          'Permite retomar a visão macro oficial da organização/canal sem depender de snapshots locais no frontend.',
      }),
    }),
    dataSources: Object.freeze([
      Object.freeze({
        key: 'channel-workspace-runbook-status',
        label: 'Status oficial do runbook',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note:
          'Fonte canônica para run_id/change_id, read model oficial da organização e topologia runtime por componente.',
      }),
      Object.freeze({
        key: 'channel-workspace-organization-read-model',
        label: 'Read model oficial da organização',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/status',
        note:
          'Usado para projetar membros do canal, chaincodes e saúde operacional observada por organização.',
      }),
      Object.freeze({
        key: 'channel-workspace-runtime-inspection',
        label: 'Runtime inspection oficial',
        executionMode: MODE.official,
        endpoint: '/api/v1/runbooks/{run_id}/runtime-inspection',
        note:
          'Consulta técnica oficial por componente com logs sanitizados, freshness, cache_hit/miss e inspection_source.',
      }),
    ]),
  }),
});

const cloneActionReadiness = action => ({
  ...action,
  label: localizeReadinessText(action.label),
  reason: localizeReadinessText(action.reason),
  modeLabel:
    localizeReadinessText(
      READINESS_EXECUTION_MODE_LABEL[action.executionMode] ||
        READINESS_EXECUTION_MODE_LABEL[MODE.unavailable]
    ),
  statusLabel: localizeReadinessText(action.status),
});

const cloneDataSourceReadiness = source => ({
  ...source,
  label: localizeReadinessText(source.label),
  note: localizeReadinessText(source.note),
  modeLabel:
    localizeReadinessText(
      READINESS_EXECUTION_MODE_LABEL[source.executionMode] ||
        READINESS_EXECUTION_MODE_LABEL[MODE.unavailable]
    ),
});

const hasDegradedExecutionPath = screenReadiness => {
  const actionHasDegradedMode = Object.values(screenReadiness.actions || {}).some(
    action => !OFFICIAL_EXECUTION_MODE_SET.has(action.executionMode)
  );
  if (actionHasDegradedMode) {
    return true;
  }

  return (screenReadiness.dataSources || []).some(
    source => !OFFICIAL_EXECUTION_MODE_SET.has(source.executionMode)
  );
};

const buildFallbackScreenReadiness = screenKey => ({
  key: screenKey,
  screenStatus: STATUS.pending,
  summary: localizeReadinessText('Readiness de backend nao mapeado para esta tela.'),
  actions: {},
  dataSources: [],
});

export const getProvisioningScreenReadiness = screenKey => {
  const catalog =
    PROVISIONING_READINESS_CATALOG[screenKey] || buildFallbackScreenReadiness(screenKey);
  const clonedReadiness = {
    key: screenKey,
    screenStatus: catalog.screenStatus,
    summary: localizeReadinessText(catalog.summary),
    actions: Object.entries(catalog.actions || {}).reduce((accumulator, [key, action]) => {
      accumulator[key] = cloneActionReadiness(action);
      return accumulator;
    }, {}),
    dataSources: (catalog.dataSources || []).map(cloneDataSourceReadiness),
  };

  const degradedPathDetected = hasDegradedExecutionPath(clonedReadiness);
  const executionState =
    degradedPathDetected && PROVISIONING_DEGRADED_MODE_ALLOWED
      ? PROVISIONING_EXECUTION_STATE.degraded
      : PROVISIONING_EXECUTION_STATE.official;

  let executionStateReason =
    localizeReadinessText(
      'Ambiente de desenvolvimento: fallback degradado habilitado e explicitamente sinalizado.'
    );
  if (executionState === PROVISIONING_EXECUTION_STATE.official) {
    executionStateReason = IS_PROVISIONING_OPERATIONAL_ENV
      ? localizeReadinessText(
          'Ambiente operacional: fallback degradado proibido para semântica inequívoca de execução.'
        )
      : localizeReadinessText(
          'Fluxo em modo do orquestrador sem dependência de fallback degradado ativo.'
        );
  }

  return {
    ...clonedReadiness,
    executionState,
    envProfile: PROVISIONING_ENV_PROFILE,
    operationalEnv: IS_PROVISIONING_OPERATIONAL_ENV,
    degradedPathDetected,
    executionStateReason,
  };
};

export const getProvisioningActionReadiness = (screenKey, actionKey) => {
  const screenReadiness = getProvisioningScreenReadiness(screenKey);
  const action = screenReadiness.actions[actionKey];
  if (action) {
    return action;
  }

  return {
    label: actionKey,
    status: STATUS.pending,
    statusLabel: localizeReadinessText(STATUS.pending),
    available: false,
    executionMode: MODE.unavailable,
    modeLabel: localizeReadinessText(READINESS_EXECUTION_MODE_LABEL[MODE.unavailable]),
    reason: pickCognusText(
      `Acao '${actionKey}' sem mapeamento de readiness.`,
      `Action '${actionKey}' without readiness mapping.`
    ),
  };
};

export const buildProvisioningReadinessActionRows = (screenKey, preferredOrder = []) => {
  const screenReadiness = getProvisioningScreenReadiness(screenKey);
  const actionEntries = Object.entries(screenReadiness.actions);
  const order = Array.isArray(preferredOrder) ? preferredOrder : [];
  const explicitOrderIndexByKey = order.reduce((accumulator, key, index) => {
    accumulator[key] = index;
    return accumulator;
  }, {});

  return actionEntries
    .sort(([leftKey], [rightKey]) => {
      const leftIndex = explicitOrderIndexByKey[leftKey];
      const rightIndex = explicitOrderIndexByKey[rightKey];

      if (Number.isInteger(leftIndex) && Number.isInteger(rightIndex)) {
        return leftIndex - rightIndex;
      }
      if (Number.isInteger(leftIndex)) {
        return -1;
      }
      if (Number.isInteger(rightIndex)) {
        return 1;
      }
      return leftKey.localeCompare(rightKey);
    })
    .map(([key, action]) => ({
      key,
      actionKey: key,
      actionLabel: action.label,
      status: action.status,
      statusLabel: action.statusLabel,
      available: action.available,
      availabilityLabel: localizeReadinessText(action.available ? 'habilitado' : 'bloqueado'),
      availabilityTone: action.available ? 'success' : 'error',
      executionMode: action.executionMode,
      modeLabel: action.modeLabel,
      modeTone:
        READINESS_EXECUTION_MODE_TONE_MAP[action.executionMode] ||
        READINESS_EXECUTION_MODE_TONE_MAP[MODE.unavailable],
      endpoint: action.endpoint || '',
      reason: action.reason || '',
    }));
};

export const buildProvisioningReadinessSourceRows = screenKey => {
  const screenReadiness = getProvisioningScreenReadiness(screenKey);
  return screenReadiness.dataSources.map(source => ({
    key: source.key,
    label: source.label,
    executionMode: source.executionMode,
    modeLabel: source.modeLabel,
    modeTone:
      READINESS_EXECUTION_MODE_TONE_MAP[source.executionMode] ||
      READINESS_EXECUTION_MODE_TONE_MAP[MODE.unavailable],
    endpoint: source.endpoint || '',
    note: source.note || '',
  }));
};

export const hasProvisioningReadinessMockSources = screenKey =>
  buildProvisioningReadinessSourceRows(screenKey).some(
    source => source.executionMode === MODE.mock
  );
