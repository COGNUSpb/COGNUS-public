import {
  PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
  PROVISIONING_DEFAULT_ROUTE_PATH,
  PROVISIONING_INFRA_SCREEN_KEY,
  PROVISIONING_MODULE_SCREEN_KEYS,
  PROVISIONING_MODULE_SCREEN_PATH_BY_KEY,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
  PROVISIONING_SCREEN_PATH_BY_KEY,
  PROVISIONING_TECHNICAL_HUB_SCREEN_KEY,
} from './provisioningContract';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const locale = resolveCognusLocale();
const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

const LEGACY_CATALOG_EN_US_BY_PT_BR = Object.freeze({
  Sim: 'Yes',
  Parcial: 'Partial',
  Pendente: 'Pending',
  Rascunho: 'Draft',
  Ativo: 'Active',
  Planejado: 'Planned',
  'Em validação': 'Under validation',
  'Em upgrade': 'Upgrading',
  'Não avaliado': 'Not evaluated',
  'Sem validação': 'No validation',
  'Rollback candidato': 'Rollback candidate',
  Mockado: 'Mocked',
  Monitorado: 'Monitored',
  Simulado: 'Simulated',
  'Sem backend': 'No backend',
  'Sem dados': 'No data',
  'Sem correlação': 'No correlation',
  'Em revisão': 'Under review',
  'Pendente ancoragem': 'Anchoring pending',
  'CR abertas': 'Open CRs',
  'Tipos de mudança': 'Change types',
  'Tempo médio de abertura': 'Average opening time',
  'Aprovações pendentes': 'Pending approvals',
  'Quórum configurável': 'Configurable quorum',
  'SOD validada': 'Validated SoD',
  'Estados rastreados': 'Tracked states',
  'Retries automáticos': 'Automatic retries',
  'Checkpoint válido': 'Valid checkpoint',
  'Chaincodes listados': 'Listed chaincodes',
  'Canais monitorados': 'Monitored channels',
  'Operações governadas': 'Governed operations',
  'Guardrails ativos': 'Active guardrails',
  'Bloqueios automáticos': 'Automatic blocks',
  'Mudanças liberadas': 'Released changes',
  'Versões catalogadas': 'Cataloged versions',
  'Rollback executado': 'Executed rollback',
  'Cobertura por canal': 'Coverage by channel',
  'Jornadas cobertas': 'Covered journeys',
  'Módulos conectados': 'Connected modules',
  'Atualização em tempo real': 'Real-time updates',
  'Componentes mapeados': 'Mapped components',
  'Domínios de canal': 'Channel domains',
  'Impacto correlacionado': 'Correlated impact',
  'Runbooks publicados': 'Published runbooks',
  'Categorias BSO': 'BSO categories',
  'Papéis ativos': 'Active roles',
  'Políticas publicadas': 'Published policies',
  'Cobertura SOD': 'SoD coverage',
  'Autorizações emitidas': 'Issued authorizations',
  'Revogações automáticas': 'Automatic revocations',
  'Falhas de autorização': 'Authorization failures',
  'Evidências ancoradas': 'Anchored evidence',
  'Verificações de hash': 'Hash verifications',
  'Integridade validada': 'Validated integrity',
  'Dashboards ativos': 'Active dashboards',
  'Labels padronizadas': 'Standardized labels',
  'Cobertura de serviços': 'Service coverage',
  'Incidentes ativos': 'Active incidents',
  'Alertas acionáveis': 'Actionable alerts',
  'Mudanças correlacionadas': 'Correlated changes',
  'Serviços com impacto': 'Services with impact',
  'Sinais de saúde vinculados': 'Linked health signals',
  'Artefatos publicados': 'Published artifacts',
  'Categorias disponíveis': 'Available categories',
  'Versões homologadas': 'Approved versions',
  'Consultas de descoberta': 'Discovery queries',
  'Matriz de compatibilidade': 'Compatibility matrix',
  'Incompatibilidades detectadas': 'Detected incompatibilities',
  'CR com template': 'CRs with template',
  'Reuso de artefato': 'Artifact reuse',
  'Falhas por incompatibilidade': 'Compatibility failures',
  'Telas prontas para gate': 'Screens ready for the gate',
  'Fluxos críticos revisados': 'Reviewed critical flows',
  'Pendências de backend': 'Backend pending items',
  Alta: 'High',
  'Perfis avaliados': 'Evaluated profiles',
  'Cenários críticos': 'Critical scenarios',
  'Tempo médio de tarefa': 'Average task time',
  'Guias finais publicados': 'Published final guides',
  'Perfis cobertos': 'Covered profiles',
  'Prontidão documental': 'Documentation readiness',
  'Selecionar tipo de mudança e impacto': 'Select the change type and impact',
  'Definir janela e recursos críticos': 'Define the maintenance window and critical resources',
  'Anexar evidências e artefatos obrigatórios': 'Attach required evidence and artifacts',
  'Submeter para aprovação multi-org': 'Submit for multi-org approval',
  'Criar CR': 'Create CR',
  'Salvar como rascunho': 'Save as draft',
  'Anexar artefatos da mudança': 'Attach change artifacts',
  'Enviar para aprovação': 'Send for approval',
  'Atualização de chaincode': 'Chaincode update',
  'Entrada de organização': 'Organization onboarding',
  'Ajuste de política': 'Policy adjustment',
  'escopo técnico': 'technical scope',
  'janela operacional': 'operational window',
  'risco classificado': 'classified risk',
  'Definir quórum por organização e canal': 'Define quorum by organization and channel',
  'Coletar decisões de aprovadores elegíveis': 'Collect decisions from eligible approvers',
  'Bloquear conflito de segregação de deveres': 'Block segregation-of-duties conflicts',
  'Promover CR para estado aprovado': 'Promote the CR to approved status',
  'Registrar decisão de aprovação': 'Register approval decision',
  'Rejeitar mudança com justificativa': 'Reject change with justification',
  'Solicitar revisão técnica': 'Request technical review',
  'Recalcular quórum': 'Recalculate quorum',
  'Aprovador primário': 'Primary approver',
  'Aprovador secundário': 'Secondary approver',
  Auditoria: 'Audit',
  'decisão assinada': 'signed decision',
  justificativa: 'justification',
  'timestamp de aprovação': 'approval timestamp',
  'quórum consolidado': 'consolidated quorum',
  'Evolução rascunho -> pending_approval -> approved': 'Draft progression -> pending_approval -> approved',
  'Execução running com checkpoints intermediários': 'Running execution with intermediate checkpoints',
  'Tratamento de bloqueio e rollback': 'Block and rollback handling',
  'Fechamento com evidências por etapa': 'Closing with evidence per stage',
  'Visualizar eventos do workflow': 'View workflow events',
  'Retomar do último checkpoint': 'Resume from the latest checkpoint',
  'Disparar rollback assistido': 'Trigger assisted rollback',
  'Exportar trilha de execução': 'Export execution trail',
  'timeline por change-id': 'timeline by change-id',
  'executor por etapa': 'executor by stage',
  'motivo de falha': 'failure reason',
  'hash do pacote de evidência': 'evidence package hash',
  'Selecionar channel e chaincode alvo': 'Select the target channel and chaincode',
  'Executar etapas de lifecycle governado': 'Execute governed lifecycle stages',
  'Anexar evidências por organização': 'Attach evidence by organization',
  'Consolidar status final por change-id': 'Consolidate the final status by change-id',
  'Consultar chaincodes existentes': 'Query existing chaincodes',
  'Iniciar lifecycle governado': 'Start governed lifecycle',
  'Acompanhar etapa por organização': 'Track the stage by organization',
  'Emitir relatório de execução': 'Generate execution report',
  'aprovação por organização': 'approval by organization',
  'hash do pacote': 'package hash',
  'resultado de commit': 'commit result',
  'Executar pré-checagem de saúde da rede': 'Run network health pre-check',
  'Avaliar risco por política operacional': 'Evaluate risk by operational policy',
  'Emitir decisão bloqueado/liberado': 'Issue a blocked/released decision',
  'Persistir decisão como evidência auditável': 'Persist the decision as auditable evidence',
  'Rodar pré-checagem': 'Run pre-check',
  'Aprovar exceção de guardrail': 'Approve guardrail exception',
  'Bloquear CR automaticamente': 'Automatically block the CR',
  'Consultar histórico de bloqueios': 'Query block history',
  'SLO de latência': 'Latency SLO',
  'Saúde de peers': 'Peer health',
  'Integridade de ordem': 'Ordering integrity',
  'resultado dos sinais de saúde': 'health signal result',
  'motivo do bloqueio': 'block reason',
  'política aplicada': 'applied policy',
  'timestamp da decisão': 'decision timestamp',
  'Comparar versão atual vs. candidata': 'Compare the current vs. candidate version',
  'Selecionar ponto de rollback seguro': 'Select a safe rollback point',
  'Executar compensação por etapa': 'Execute stage-by-stage compensation',
  'Registrar resultado e impacto': 'Register result and impact',
  'Consultar histórico de versão': 'Query version history',
  'Abrir plano de rollback': 'Open rollback plan',
  'Executar rollback governado': 'Execute governed rollback',
  'Anexar evidências pós-rollback': 'Attach post-rollback evidence',
  'histórico de versão': 'version history',
  'motivo do rollback': 'rollback reason',
  'resultado da compensação': 'compensation result',
  'change-id correlacionado': 'correlated change-id',
  'Abertura de mudança com contexto operacional': 'Open a change with operational context',
  'Acompanhamento unificado de status e evidências': 'Unified monitoring of status and evidence',
  'Finalização com exportação de auditoria': 'Complete with audit export',
  'Notificação de riscos e bloqueios ativos': 'Notify risks and active blocks',
  'Iniciar jornada operacional': 'Start the operational journey',
  'Acompanhar CR em tempo real': 'Track the CR in real time',
  'Visualizar decisão de guardrail': 'View guardrail decision',
  'Exportar trilha de operação': 'Export operation trail',
  'Fluxo de mudança crítica': 'Critical change flow',
  'Painel de auditoria': 'Audit dashboard',
  'Feed de alertas': 'Alert feed',
  'contexto org/canal': 'org/channel context',
  'timeline de execução': 'execution timeline',
  'alertas por severidade': 'alerts by severity',
  'relatório auditável': 'auditable report',
  'Carregar topologia por organização': 'Load topology by organization',
  'Filtrar por canal e componente': 'Filter by channel and component',
  'Destacar elementos impactados por mudança': 'Highlight change-impacted elements',
  'Navegar para evidências relacionadas': 'Navigate to related evidence',
  'Listar nós e canais existentes': 'List existing nodes and channels',
  'Visualizar grafo consolidado': 'View consolidated graph',
  'Aplicar filtro de impacto': 'Apply impact filter',
  'Exportar mapa de topologia': 'Export topology map',
  'snapshot de topologia': 'topology snapshot',
  'componentes impactados': 'impacted components',
  'estado por organização': 'state by organization',
  'change-id associado': 'associated change-id',
  'Selecionar runbook por categoria': 'Select runbook by category',
  'Validar pré-condições de execução': 'Validate execution prerequisites',
  'Executar checklist com evidências': 'Execute checklist with evidence',
  'Publicar resultado no histórico operacional': 'Publish result to operational history',
  'Criar runbook operacional': 'Create operational runbook',
  'Executar checklist guiado': 'Execute guided checklist',
  'Registrar resultado da execução': 'Register execution result',
  'Versionar runbook': 'Version runbook',
  'checklist executado': 'executed checklist',
  'resultado por etapa': 'result by stage',
  'anexos técnicos': 'technical attachments',
  'assinatura de execução': 'execution signature',
  'Mapear papéis por perfil de operação': 'Map roles by operation profile',
  'Definir política por recurso e ação': 'Define policy by resource and action',
  'Validar segregação de deveres': 'Validate segregation of duties',
  'Aplicar política no endpoint BAF/PEP': 'Apply policy at the BAF/PEP endpoint',
  'Cadastrar usuário e papel legado': 'Register legacy user and role',
  'Criar política contextual': 'Create contextual policy',
  'Simular permissão efetiva': 'Simulate effective permission',
  'Publicar política ativa': 'Publish active policy',
  'matriz de papéis': 'role matrix',
  'policy-id versionado': 'versioned policy-id',
  'resultado de SOD': 'SoD result',
  'log de publicação': 'publication log',
  'Emitir autorização contextual com prazo': 'Issue contextual authorization with expiration',
  'Monitorar expiração e revogação': 'Monitor expiration and revocation',
  'Bloquear acesso fora de política': 'Block access outside policy',
  'Consolidar trilha por access-id': 'Consolidate trail by access-id',
  'Emitir autorização temporária': 'Issue temporary authorization',
  'Revogar autorização ativa': 'Revoke active authorization',
  'Consultar auditoria por access-id': 'Query audit by access-id',
  'Exportar relatório de acesso': 'Export access report',
  'motivo da emissão': 'issuance reason',
  'timestamp de expiração': 'expiration timestamp',
  'evento de revogação': 'revocation event',
  'Selecionar evidência por change-id': 'Select evidence by change-id',
  'Comparar hash e metadados': 'Compare hash and metadata',
  'Validar cadeia de decisões associadas': 'Validate the associated decision chain',
  'Emitir parecer de integridade': 'Issue integrity opinion',
  'Registrar evidência ancorada': 'Register anchored evidence',
  'Verificar integridade por hash': 'Verify integrity by hash',
  'Relacionar evidência a aprovação': 'Link evidence to approval',
  'Exportar laudo de integridade': 'Export integrity report',
  'hash SHA-256': 'SHA-256 hash',
  'metadados de origem': 'source metadata',
  'assinatura de aprovação': 'approval signature',
  'resultado da verificação': 'verification result',
  'Definir labels obrigatórias de telemetria': 'Define required telemetry labels',
  'Consolidar coleta de métricas, logs e traces': 'Consolidate metrics, logs, and traces collection',
  'Filtrar visualização por org/canal/change-id': 'Filter the view by org/channel/change-id',
  'Publicar baseline de SLO por operação crítica': 'Publish SLO baseline by critical operation',
  'Abrir dashboard por camada': 'Open dashboard by layer',
  'Aplicar filtros operacionais': 'Apply operational filters',
  'Correlacionar métricas e logs': 'Correlate metrics and logs',
  'Exportar snapshot de observabilidade': 'Export observability snapshot',
  orquestração: 'orchestration',
  'Saúde de peer': 'Peer health',
  'labels padronizadas': 'standardized labels',
  'série temporal por operação': 'time series by operation',
  'erro por serviço': 'error by service',
  'correlação por change-id': 'correlation by change-id',
  'Detectar incidente e classificar severidade': 'Detect the incident and classify severity',
  'Executar runbook de contenção': 'Execute containment runbook',
  'Registrar ações e timeline de resposta': 'Register actions and response timeline',
  'Fechar incidente com pós-mortem estruturado': 'Close incident with structured post-mortem',
  'Registrar incidente': 'Register incident',
  'Acionar runbook SRE': 'Trigger SRE runbook',
  'Atualizar status de mitigação': 'Update mitigation status',
  'Publicar pós-mortem': 'Publish post-mortem',
  'id do incidente': 'incident id',
  'severidade e impacto': 'severity and impact',
  'timeline de mitigação': 'mitigation timeline',
  'laudo pós-mortem': 'post-mortem report',
  'Selecionar change-id alvo': 'Select target change-id',
  'Comparar baseline antes/depois': 'Compare before/after baseline',
  'Evidenciar impacto por serviço e canal': 'Show impact by service and channel',
  'Anexar correlação ao relatório de mudança': 'Attach correlation to the change report',
  'Buscar change-id': 'Search change-id',
  'Carregar série temporal comparativa': 'Load comparative time series',
  'Gerar relatório de impacto': 'Generate impact report',
  'Vincular efeito ao guardrail': 'Link effect to guardrail',
  'sinal antes/depois': 'signal before/after',
  'impacto por domínio': 'impact by domain',
  'decisão operacional': 'operational decision',
  'Cadastrar metadados e proveniência do artefato': 'Register artifact metadata and provenance',
  'Validar qualidade e compatibilidade mínima': 'Validate quality and minimum compatibility',
  'Publicar versão com política de depreciação': 'Publish version with deprecation policy',
  'Disponibilizar para consumo no console': 'Make it available for console consumption',
  'Publicar novo template': 'Publish new template',
  'Submeter aprovação de publicação': 'Submit publication approval',
  'Deprecar versão anterior': 'Deprecate previous version',
  'Sincronizar catálogo': 'Synchronize catalog',
  'versão semântica': 'semantic version',
  'aprovação de publicação': 'publication approval',
  'Definir perfil técnico do ambiente alvo': 'Define the target environment technical profile',
  'Aplicar filtros de versão e canal': 'Apply version and channel filters',
  'Avaliar restrições de compatibilidade': 'Evaluate compatibility restrictions',
  'Selecionar artefato recomendado': 'Select recommended artifact',
  'Buscar por perfil de rede': 'Search by network profile',
  'Comparar compatibilidade de versões': 'Compare version compatibility',
  'Validar pré-requisitos técnicos': 'Validate technical prerequisites',
  'Salvar shortlist de templates': 'Save template shortlist',
  'perfil consultado': 'queried profile',
  'resultado de compatibilidade': 'compatibility result',
  'restrições detectadas': 'detected restrictions',
  'decisão de seleção': 'selection decision',
  'Selecionar template homologado no catálogo': 'Select approved template in the catalog',
  'Anexar template ao wizard de mudança': 'Attach template to the change wizard',
  'Executar validações de pré-uso': 'Run pre-use validations',
  'Persistir vínculo artifact -> change-id': 'Persist artifact -> change-id link',
  'Anexar template ao CR': 'Attach template to the CR',
  'Validar compatibilidade antes da submissão': 'Validate compatibility before submission',
  'Rastrear uso do template no histórico': 'Track template usage in history',
  'Emitir relatório de reuso': 'Generate reuse report',
  rascunho: 'draft',
  'versão aplicada': 'applied version',
  'resultado de validação': 'validation result',
  'histórico de uso': 'usage history',
  'Padronizar componentes e feedback operacional': 'Standardize components and operational feedback',
  'Reforçar semântica visual de risco e estado': 'Strengthen visual semantics for risk and state',
  'Validar consistência entre jornadas': 'Validate consistency across journeys',
  'Preparar checklist de readiness UX': 'Prepare UX readiness checklist',
  'Executar checklist de UX': 'Run UX checklist',
  'Registrar achados de usabilidade': 'Register usability findings',
  'Priorizar ajustes antes do gate': 'Prioritize adjustments before the gate',
  'Gerar parecer de prontidão visual': 'Generate visual readiness opinion',
  'achados priorizados': 'prioritized findings',
  'tempo por tarefa': 'time per task',
  'parecer de prontidão': 'readiness opinion',
  'Planejar roteiro de teste por perfil': 'Plan the test script by profile',
  'Executar tarefas críticas no console': 'Execute critical tasks in the console',
  'Mensurar tempo e taxa de sucesso': 'Measure time and success rate',
  'Consolidar recomendações de melhoria': 'Consolidate improvement recommendations',
  'Iniciar sessão de teste guiado': 'Start guided test session',
  'Registrar dificuldade por tarefa': 'Register difficulty per task',
  'Comparar resultados por perfil': 'Compare results by profile',
  'Exportar relatório de usabilidade': 'Export usability report',
  'feedback qualitativo': 'qualitative feedback',
  'recomendação aprovada': 'approved recommendation',
  'Consolidar procedimentos por perfil': 'Consolidate procedures by profile',
  'Vincular guias aos fluxos críticos': 'Link guides to critical flows',
  'Anexar evidências e troubleshooting': 'Attach evidence and troubleshooting',
  'Publicar pacote final de go-live': 'Publish final go-live package',
  'Gerar guia do operador': 'Generate operator guide',
  'Gerar guia do auditor': 'Generate auditor guide',
  'Gerar guia do administrador': 'Generate administrator guide',
  'Exportar pacote de documentação final': 'Export final documentation package',
  'Operação diária': 'Daily operations',
  'Auditoria técnica': 'Technical auditing',
  'Governança e IAM': 'Governance and IAM',
  'versão do guia': 'guide version',
  'escopo por perfil': 'scope by profile',
  'checklist de atualização': 'update checklist',
  'aprovação final': 'final approval',
});

const localizeLegacyCatalogValue = value => {
  if (locale !== 'en-US') {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map(localizeLegacyCatalogValue);
  }

  if (value && typeof value === 'object') {
    return Object.entries(value).reduce((accumulator, [key, currentValue]) => {
      accumulator[key] = localizeLegacyCatalogValue(currentValue);
      return accumulator;
    }, {});
  }

  if (typeof value !== 'string') {
    return value;
  }

  return LEGACY_CATALOG_EN_US_BY_PT_BR[value] || value;
};

export const backendStatusMeta = {
  ready: {
    label: t('Backend implementado', 'Backend implemented'),
    tone: 'success',
    warning: false,
    description: t(
      'Esta tela já possui integração backend disponível.',
      'This screen already has backend integration available.'
    ),
  },
  partial: {
    label: t('Backend parcial', 'Partial backend'),
    tone: 'processing',
    warning: true,
    description: t(
      'A tela possui integração parcial e ainda depende de endpoints do orquestrador.',
      'This screen has partial integration and still depends on orchestrator endpoints.'
    ),
  },
  pending: {
    label: t('Backend pendente', 'Backend pending'),
    tone: 'warning',
    warning: true,
    description: t(
      'A integração backend desta tela ainda não foi implementada.',
      'Backend integration for this screen has not been implemented yet.'
    ),
  },
};

const normalizeCatalogMetadata = screen =>
  localizeLegacyCatalogValue({
    ...screen,
    objective: screen.objective || screen.summary,
    checkpoints: Array.isArray(screen.checkpoints) ? screen.checkpoints : [],
    actions: Array.isArray(screen.actions) ? screen.actions : [],
    evidences: Array.isArray(screen.evidences) ? screen.evidences : [],
  });

export const deliveries = [
  {
    key: 'entrega1',
    label: t('Entrega 1', 'Delivery 1'),
    objective: t(
      'Infraestrutura base, blueprint e provisão multi-org.',
      'Base infrastructure, blueprint, and multi-org provisioning.'
    ),
    path: PROVISIONING_DEFAULT_ROUTE_PATH,
    screens: PROVISIONING_MODULE_SCREEN_KEYS,
  },
  {
    key: 'entrega2',
    label: t('Entrega 2', 'Delivery 2'),
    objective: t(
      'Núcleo OPSSC com Solicitação de Mudança (CR) e execução distribuída.',
      'OPSSC core with Change Request (CR) and distributed execution.'
    ),
    path: '/changes/change-request',
    screens: ['e2-cr', 'e2-aprovacoes', 'e2-timeline'],
  },
  {
    key: 'entrega3',
    label: t('Entrega 3', 'Delivery 3'),
    objective: t(
      'Lifecycle de chaincode governado e guardrails preventivos.',
      'Governed chaincode lifecycle and preventive guardrails.'
    ),
    path: '/chaincode-ops/lifecycle',
    screens: ['e3-lifecycle', 'e3-guardrails', 'e3-rollback'],
  },
  {
    key: 'entrega4',
    label: t('Entrega 4', 'Delivery 4'),
    objective: t(
      'Console operacional e visualização de topologia.',
      'Operational console and topology visualization.'
    ),
    path: '/operations/console',
    screens: ['e4-console', 'e4-topologia', 'e4-runbooks'],
  },
  {
    key: 'entrega5',
    label: t('Entrega 5', 'Delivery 5'),
    objective: t(
      'Governança, IAM e segurança end-to-end.',
      'Governance, IAM, and end-to-end security.'
    ),
    path: '/governance/policies',
    screens: ['e5-politicas', 'e5-auditoria', 'e5-evidencias'],
  },
  {
    key: 'entrega6',
    label: t('Entrega 6', 'Delivery 6'),
    objective: t(
      'Observabilidade multi-escala e operação SRE.',
      'Multi-scale observability and SRE operations.'
    ),
    path: '/observability/dashboards',
    screens: ['e6-observabilidade', 'e6-incidentes', 'e6-correlacao'],
  },
  {
    key: 'entrega7',
    label: t('Entrega 7', 'Delivery 7'),
    objective: t(
      'Catálogo de modelos e integrações governadas.',
      'Catalog of templates and governed integrations.'
    ),
    path: '/catalog/templates',
    screens: ['e7-marketplace', 'e7-compatibilidade', 'e7-template-cr'],
  },
  {
    key: 'entrega8',
    label: t('Entrega 8', 'Delivery 8'),
    objective: t(
      'Prontidão final, UX operacional e documentação.',
      'Final readiness, operational UX, and documentation.'
    ),
    path: '/readiness/ux',
    screens: ['e8-readiness', 'e8-usabilidade', 'e8-documentacao'],
  },
];

const rawScreens = [
  {
    key: PROVISIONING_INFRA_SCREEN_KEY,
    path: PROVISIONING_MODULE_SCREEN_PATH_BY_KEY[PROVISIONING_INFRA_SCREEN_KEY],
    deliveryKey: 'entrega1',
    title: t('Provisionamento de Infraestrutura via SSH', 'Infrastructure Provisioning via SSH'),
    summary: t(
      'Cadastro operacional de VMs Linux por acesso SSH para preparar infraestrutura base sem edição manual de YAML/JSON.',
      'Operational registration of Linux VMs through SSH access to prepare the base infrastructure without manual YAML/JSON editing.'
    ),
    objective: t(
      'Guiar o operador na criação de infraestrutura external-linux via SSH com sequência operacional obrigatória: Organizations -> Nodes -> Business Groups -> Channels -> Install Chaincodes (cc-tools).',
      'Guide the operator through creating external-linux infrastructure via SSH with the required operational sequence: Organizations -> Nodes -> Business Groups -> Channels -> Install Chaincodes (cc-tools).'
    ),
    backendStatus: 'ready',
    backendNote: t(
      'Fluxo guiado, publicação e execução via runbook concluídos no escopo A1, com correlação por change_id/run_id.',
      'Guided flow, publishing, and runbook execution are completed in the A1 scope, with change_id/run_id correlation.'
    ),
    metrics: [
      { label: t('Provedor permitido', 'Allowed provider'), value: 'external-linux' },
      { label: t('Porta SSH padrão', 'Default SSH port'), value: '22' },
      { label: t('Porta Docker padrão', 'Default Docker port'), value: '2376' },
    ],
    checkpoints: [
      t(
        'Cadastrar acesso SSH, hosts de VM Linux e executar preflight apto',
        'Register SSH access, Linux VM hosts, and execute a ready preflight'
      ),
      t(
        'Definir Organizations com domínio, Network API e política de CA (interna/externa)',
        'Define Organizations with domain, Network API, and CA policy (internal/external)'
      ),
      t(
        'Configurar Nodes, Business Groups e Channels em ordem dependente',
        'Configure Nodes, Business Groups, and Channels in dependency order'
      ),
      t(
        'Planejar install de chaincode cc-tools por channel (sem build/versionamento nesta fase)',
        'Plan cc-tools chaincode install per channel (without build/versioning at this stage)'
      ),
    ],
    actions: [
      { label: t('Cadastrar infraestrutura SSH', 'Register SSH infrastructure'), backendReady: true },
      { label: t('Gerar rascunho técnico para blueprint', 'Generate technical draft for blueprint'), backendReady: true },
      { label: t('Validar pré-condições operacionais', 'Validate operational prerequisites'), backendReady: true },
      { label: t('Iniciar provisão SSH', 'Start SSH provisioning'), backendReady: true },
    ],
    mockRows: [
      {
        key: '1',
        item: 'machine0.infra-web3-ufg-dev',
        scope: '200.137.197.215:21525',
        state: t('Cadastro integrado', 'Integrated registration'),
        backendReady: true,
      },
      {
        key: '2',
        item: 'ORG-INF-UFG',
        scope: 'api.inf.ufg.br:31522',
        state: t('Estrutura pronta', 'Structure ready'),
        backendReady: true,
      },
      {
        key: '3',
        item: 'fakenews-channel-dev / fakenews-cc-dev',
        scope: '/api/fakenews-channel-dev/fakenews-cc-dev',
        state: t('Roteamento ativo', 'Routing active'),
        backendReady: true,
      },
    ],
    evidences: [
      t('fingerprint do plano de infraestrutura', 'infrastructure plan fingerprint'),
      t('inventário de hosts SSH', 'SSH hosts inventory'),
      t('vínculo com change_id', 'change_id link'),
      t('snapshot UTC da configuração', 'UTC snapshot of the configuration'),
    ],
  },
  {
    key: PROVISIONING_TECHNICAL_HUB_SCREEN_KEY,
    path: PROVISIONING_MODULE_SCREEN_PATH_BY_KEY[PROVISIONING_TECHNICAL_HUB_SCREEN_KEY],
    deliveryKey: 'entrega1',
    title: t('Provisão técnica avançada', 'Advanced technical provisioning'),
    summary: t(
      'Jornada técnica única em etapas para evoluir de blueprint até lifecycle e rollback com navegação guiada.',
      'Single technical journey in steps to evolve from blueprint to lifecycle and rollback with guided navigation.'
    ),
    objective: t(
      'Conduzir o operador em seis etapas sequenciais: blueprint/versionamento, provisão assistida, inventário inicial, lifecycle de chaincode, guardrails de disponibilidade e versões/rollback.',
      'Guide the operator through six sequential steps: blueprint/versioning, assisted provisioning, initial inventory, chaincode lifecycle, availability guardrails, and versions/rollback.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'As etapas técnicas já existentes permanecem ativas e agora são orquestradas em um único fluxo guiado com readiness explícito por ação.',
      'The existing technical steps remain active and are now orchestrated in a single guided flow with explicit readiness per action.'
    ),
    metrics: [
      { label: t('Etapas técnicas guiadas', 'Guided technical steps'), value: '06' },
      { label: t('Pipeline alvo', 'Target pipeline'), value: 'prepare -> provision -> configure -> verify' },
      { label: t('Escopo obrigatório', 'Mandatory scope'), value: 'external-linux + VM Linux' },
    ],
    checkpoints: [
      t('Blueprint e versionamento', 'Blueprint and versioning'),
      t('Provisão assistida (runbook)', 'Assisted provisioning (runbook)'),
      t('Inventário inicial e evidências', 'Initial inventory and evidence'),
      t('Lifecycle de chaincode via Gateway API', 'Chaincode lifecycle via Gateway API'),
      t('Guardrails de disponibilidade', 'Availability guardrails'),
      t('Versões e rollback governado', 'Versions and governed rollback'),
    ],
    actions: [
      { label: t('Abrir etapa Blueprint e versionamento', 'Open Blueprint and versioning step'), backendReady: true },
      { label: t('Abrir etapa Provisão assistida', 'Open assisted provisioning step'), backendReady: true },
      { label: t('Abrir etapa Inventário inicial', 'Open initial inventory step'), backendReady: true },
      { label: t('Abrir etapa Lifecycle de chaincode', 'Open chaincode lifecycle step'), backendReady: true },
      { label: t('Abrir etapa Guardrails de disponibilidade', 'Open availability guardrails step'), backendReady: true },
      { label: t('Abrir etapa Versões e rollback', 'Open versions and rollback step'), backendReady: true },
      { label: t('Revisar readiness por ação', 'Review readiness by action'), backendReady: true },
    ],
    mockRows: [
      {
        key: '1',
        item: t('Etapas 1-3 (A1)', 'Steps 1-3 (A1)'),
        scope: t('blueprint/runbook/inventário', 'blueprint/runbook/inventory'),
        state: t('Disponível', 'Available'),
        backendReady: true,
      },
      {
        key: '2',
        item: t('Etapas 4-6 (E3 acoplada)', 'Steps 4-6 (coupled E3)'),
        scope: 'lifecycle/guardrails/rollback',
        state: t('Disponível', 'Available'),
        backendReady: true,
      },
      {
        key: '3',
        item: t('Fluxo técnico único', 'Single technical flow'),
        scope: t('navegação passo a passo', 'step-by-step navigation'),
        state: t('Disponível', 'Available'),
        backendReady: true,
      },
    ],
    evidences: [
      t('status de lint e publicação de blueprint', 'lint status and blueprint publication'),
      t('status consolidado por etapa de runbook', 'consolidated status per runbook stage'),
      t('snapshot de inventário pós-provisão', 'post-provision inventory snapshot'),
      t('status de lifecycle/guardrails/rollback', 'lifecycle/guardrails/rollback status'),
      t('correlação change_id/run_id', 'change_id/run_id correlation'),
    ],
  },
  {
    key: 'e1-blueprint',
    path: PROVISIONING_SCREEN_PATH_BY_KEY['e1-blueprint'],
    deliveryKey: 'entrega1',
    title: t('Cadastro e Versionamento de Blueprint', 'Blueprint Registration and Versioning'),
    summary: t(
      'Modela topologia, perfis de ambiente e segurança para execução idempotente de provisionamento.',
      'Models topology, environment profiles, and security for idempotent provisioning execution.'
    ),
    objective: t(
      'Cadastrar e versionar blueprint de ambiente/topologia com contrato técnico rastreável para a execução de runbook.',
      'Register and version an environment/topology blueprint with a traceable technical contract for runbook execution.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Lint, publicação e histórico de versões usam endpoints do orquestrador; upload dedicado de arquivos permanece no fluxo local.',
      'Lint, publishing, and version history use orchestrator endpoints; dedicated file upload remains in the local flow.'
    ),
    metrics: [
      { label: t('Blocos cobertos no editor', 'Blocks covered in the editor'), value: '06/06' },
      { label: t('Schema runtime', 'Runtime schema'), value: 'v1.0.0' },
      { label: t('Gate de lint', 'Lint gate'), value: t('Bloqueante', 'Blocking') },
    ],
    checkpoints: [
      t('Cadastrar/importar blueprint em JSON/YAML', 'Register/import blueprint in JSON/YAML'),
      t(
        'Executar lint pré-plano com restrição external provider + VM Linux',
        'Run pre-plan lint with external provider + Linux VM restriction'
      ),
      t(
        'Exibir schema resolvida, versão e fingerprint canônico',
        'Display resolved schema, version, and canonical fingerprint'
      ),
      t(
        'Publicar versão vinculada a change_id e contexto de execução',
        'Publish version linked to change_id and execution context'
      ),
    ],
    actions: [
      { label: t('Criar/editar blueprint', 'Create/edit blueprint'), backendReady: false },
      { label: t('Importar JSON/YAML', 'Import JSON/YAML'), backendReady: false },
      { label: t('Executar lint pré-plano', 'Run pre-plan lint'), backendReady: true },
      { label: t('Comparar versões publicadas', 'Compare published versions'), backendReady: true },
    ],
    mockRows: [
      {
        key: '1',
        item: 'blueprint-v1.0.0',
        scope: 'change_id: cr-2026-02-16-001',
        state: t('Publicado', 'Published'),
        backendReady: false,
      },
      {
        key: '2',
        item: 'blueprint-v1.1.0',
        scope: 'change_id: cr-2026-02-16-002',
        state: t('Em validação', 'Under validation'),
        backendReady: false,
      },
      {
        key: '3',
        item: 'blueprint-v1.2.0',
        scope: 'change_id: cr-2026-02-16-003',
        state: t('Rascunho', 'Draft'),
        backendReady: false,
      },
    ],
    evidences: [
      t('fingerprint canônico do blueprint', 'canonical blueprint fingerprint'),
      t('resultado de lint por versão', 'lint result by version'),
      t('timestamp de publicação', 'publication timestamp'),
      t('vínculo explícito com change_id/contexto', 'explicit link to change_id/context'),
    ],
  },
  {
    key: 'e1-provisionamento',
    path: PROVISIONING_SCREEN_PATH_BY_KEY['e1-provisionamento'],
    deliveryKey: 'entrega1',
    title: t('Execução Assistida de Provisão', 'Assisted Provisioning Execution'),
    summary: t(
      'Controla o runbook `prepare -> provision -> configure -> verify` com checkpoints e retomada segura.',
      'Controls the `prepare -> provision -> configure -> verify` runbook with checkpoints and safe resume.'
    ),
    objective: t(
      'Executar o runbook base em provedor externo (VM Linux) com estado por etapa, checkpoints e retomada segura.',
      'Execute the base runbook on an external provider (Linux VM) with per-stage state, checkpoints, and safe resume.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Execução do runbook base disponível em endpoints dedicados do orquestrador, com controle por etapa/checkpoint e correlação por run_id.',
      'Base runbook execution is available through dedicated orchestrator endpoints, with stage/checkpoint control and run_id correlation.'
    ),
    metrics: [
      { label: t('Execuções de runbook', 'Runbook executions'), value: '00' },
      { label: t('Etapas monitoradas', 'Monitored stages'), value: '04' },
      { label: t('Retomadas válidas', 'Valid resumes'), value: '0%' },
    ],
    checkpoints: [
      t('Pré-checagens de infraestrutura e credenciais', 'Infrastructure and credential pre-checks'),
      t('Provisionamento por perfil de ambiente', 'Provisioning by environment profile'),
      t('Configuração de artefatos técnicos', 'Technical artifact configuration'),
      t('Verificação final e publicação de evidência', 'Final verification and evidence publication'),
    ],
    actions: [
      { label: t('Iniciar runbook', 'Start runbook'), backendReady: true },
      { label: t('Pausar em checkpoint', 'Pause at checkpoint'), backendReady: true },
      { label: t('Retomar execução', 'Resume execution'), backendReady: true },
      { label: t('Exportar relatório de runbook', 'Export runbook report'), backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'prepare',
        scope: 'consortium-dev',
        state: t('Pendente', 'Pending'),
        backendReady: false,
      },
      {
        key: '2',
        item: 'provision',
        scope: 'consortium-dev',
        state: t('Pendente', 'Pending'),
        backendReady: false,
      },
      {
        key: '3',
        item: 'configure',
        scope: 'consortium-dev',
        state: t('Pendente', 'Pending'),
        backendReady: false,
      },
    ],
    evidences: [
      t('log estruturado por etapa', 'structured log per stage'),
      t('executor técnico', 'technical executor'),
      t('hash de artefatos', 'artifact hash'),
      t('resultado do verify', 'verify result'),
    ],
  },
  {
    key: 'e1-inventario',
    path: PROVISIONING_SCREEN_PATH_BY_KEY['e1-inventario'],
    deliveryKey: 'entrega1',
    title: t('Inventário Inicial de Componentes', 'Initial Component Inventory'),
    summary: t(
      'Consolida visão mínima de organizações, canais, nós e certificados para baseline operacional.',
      'Consolidates a minimal view of organizations, channels, nodes, and certificates for the operational baseline.'
    ),
    objective: t(
      'Consolidar inventário operacional inicial por organização/canal/host/nó com evidências mínimas de baseline.',
      'Consolidate the initial operational inventory by organization/channel/host/node with minimum baseline evidence.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Há endpoints legados para listagem de organizações e nós, mas falta agregação do inventário COGNUS no orquestrador.',
      'There are legacy endpoints for listing organizations and nodes, but COGNUS inventory aggregation is still missing in the orchestrator.'
    ),
    metrics: [
      { label: t('Organizações detectadas', 'Detected organizations'), value: '05' },
      { label: t('Nós catalogados', 'Cataloged nodes'), value: '18' },
      { label: t('Canais mapeados', 'Mapped channels'), value: '04' },
    ],
    checkpoints: [
      t('Consolidar organizações e MSPs ativas', 'Consolidate active organizations and MSPs'),
      t('Mapear nós por papel e disponibilidade', 'Map nodes by role and availability'),
      t('Exibir canais por domínio de governança', 'Display channels by governance domain'),
      t('Registrar baseline criptográfico', 'Register cryptographic baseline'),
    ],
    actions: [
      { label: t('Sincronizar inventário', 'Synchronize inventory'), backendReady: false },
      { label: t('Listar organizações existentes', 'List existing organizations'), backendReady: true },
      { label: t('Listar nós existentes', 'List existing nodes'), backendReady: true },
      { label: t('Exportar baseline de inventário', 'Export inventory baseline'), backendReady: false },
    ],
    mockRows: [
      { key: '1', item: 'orgAlpha', scope: t('MSP ativo', 'Active MSP'), state: t('Ativo', 'Active'), backendReady: true },
      { key: '2', item: 'peer0.orgAlpha', scope: t('Nó peer', 'Peer node'), state: t('Ativo', 'Active'), backendReady: true },
      {
        key: '3',
        item: 'channel-main',
        scope: t('Canal consórcio', 'Consortium channel'),
        state: t('Em validação', 'Under validation'),
        backendReady: false,
      },
    ],
    evidences: [
      t('inventário de certificados', 'certificate inventory'),
      t('estado de nós', 'node state'),
      t('assinatura do snapshot', 'snapshot signature'),
      t('histórico de sincronização', 'synchronization history'),
    ],
  },
  {
    key: PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
    path: PROVISIONING_SCREEN_PATH_BY_KEY[PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY],
    deliveryKey: 'entrega1',
    title: t('Topologia Runtime da Organização', 'Organization Runtime Topology'),
    summary: t(
      'Tela dedicada para convergência oficial da topologia A2 por organização, com status por componente e correlação por run_id.',
      'Screen dedicated to official convergence of the A2 topology by organization, with per-component status and run_id correlation.'
    ),
    objective: t(
      'Consolidar peers, orderers, CA, couch, apiGateway, netapi e runtimes de chaincode em uma visão auditável baseada no inventário/evidência oficial da execução.',
      'Consolidate peers, orderers, CA, couch, apiGateway, netapi, and chaincode runtimes into an auditable view based on the official execution inventory/evidence.'
    ),
    backendStatus: 'implemented',
    backendNote: t(
      'Consome exclusivamente o endpoint oficial de status do runbook para exibir estado e evidências de convergência da topologia.',
      'Consumes the official runbook status endpoint exclusively to display topology convergence state and evidence.'
    ),
    metrics: [
      { label: t('Fonte de status', 'Status source'), value: '/api/v1/runbooks/{run_id}/status' },
      {
        label: t('Escopo de componentes', 'Component scope'),
        value: 'peer/orderer/ca/couch/apiGateway/netapi/chaincode',
      },
      { label: t('Classificação de escopo', 'Scope classification'), value: 'required/planned/optional' },
    ],
    checkpoints: [
      t('Carregar execução oficial por run_id', 'Load official execution by run_id'),
      t('Consolidar topologia por organização e host', 'Consolidate topology by organization and host'),
      t('Aplicar filtros por host, tipo, status e criticidade', 'Apply filters by host, type, status, and criticality'),
      t('Conferir convergência dos componentes required com evidência oficial', 'Check convergence of required components with official evidence'),
    ],
    actions: [
      { label: t('Carregar topologia oficial por run_id', 'Load official topology by run_id'), backendReady: true },
      { label: t('Filtrar componentes runtime', 'Filter runtime components'), backendReady: true },
      { label: t('Abrir inventário oficial correlacionado', 'Open correlated official inventory'), backendReady: true },
      { label: t('Reabrir runbook oficial correlacionado', 'Reopen correlated official runbook'), backendReady: true },
    ],
    mockRows: [
      {
        key: '1',
        item: 'org1 | peer0-org1',
        scope: 'required / critical',
        state: t('ready (evidência oficial)', 'ready (official evidence)'),
        backendReady: true,
      },
      {
        key: '2',
        item: 'org1 | netapi',
        scope: 'planned / supporting',
        state: 'planned',
        backendReady: true,
      },
      {
        key: '3',
        item: 'org1 | cc-tools runtime',
        scope: 'optional / supporting',
        state: 'ready (official_decision)',
        backendReady: true,
      },
    ],
    evidences: [
      t('correlação run_id/change_id oficial', 'official run_id/change_id correlation'),
      t('status por componente com fonte de evidência', 'status per component with evidence source'),
      t('artefatos verify/inventory final', 'final verify/inventory artifacts'),
      t('decisão oficial allow/block', 'official allow/block decision'),
    ],
  },
  {
    key: PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
    path: PROVISIONING_SCREEN_PATH_BY_KEY[PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY],
    deliveryKey: 'entrega1',
    title: t('Workspace Operacional do Canal', 'Operational Channel Workspace'),
    summary: t(
      'Visão oficial do canal com participantes, componentes correlacionados e drill-down técnico por run_id/channel_id.',
      'Official channel view with participants, correlated components, and technical drill-down by run_id/channel_id.'
    ),
    objective: t(
      'Permitir ao operador auditar um canal provisionado a partir do read model e da topologia runtime oficiais, com inspeção técnica por componente sem sair da jornada operacional.',
      'Allow the operator to audit a provisioned channel from the official read model and runtime topology, with technical inspection per component without leaving the operational journey.'
    ),
    backendStatus: 'implemented',
    backendNote: t(
      'Consome status oficial do runbook para projetar organizações membros, chaincodes e componentes correlacionados ao channel_id.',
      'Consumes official runbook status to project member organizations, chaincodes, and components correlated to channel_id.'
    ),
    metrics: [
      { label: t('Fonte de status', 'Status source'), value: '/api/v1/runbooks/{run_id}/status' },
      { label: t('Correlacao principal', 'Primary correlation'), value: 'run_id + channel_id + org_id?' },
      { label: t('Drill-down tecnico', 'Technical drill-down'), value: t('runtime-inspection oficial', 'official runtime inspection') },
    ],
    checkpoints: [
      t('Carregar estado oficial por run_id', 'Load official state by run_id'),
      t(
        'Resolver membros e chaincodes do canal por read model/topologia runtime',
        'Resolve channel members and chaincodes through the read model/runtime topology'
      ),
      t('Abrir drill-down técnico por componente', 'Open technical drill-down by component'),
      t('Reabrir topologia runtime correlacionada quando necessário', 'Reopen correlated runtime topology when necessary'),
    ],
    actions: [
      { label: t('Carregar workspace oficial do canal', 'Load official channel workspace'), backendReady: true },
      { label: t('Inspecionar componente do canal', 'Inspect channel component'), backendReady: true },
      { label: t('Reabrir topologia runtime correlacionada', 'Reopen correlated runtime topology'), backendReady: true },
    ],
    mockRows: [
      {
        key: '1',
        item: 'channel-core',
        scope: 'run_id + channel_id',
        state: t('Workspace oficial pronto', 'Official workspace ready'),
        backendReady: true,
      },
      {
        key: '2',
        item: 'org1 / peer0 / gateway',
        scope: 'component drill-down',
        state: t('Inspeção oficial pronta', 'Official inspection ready'),
        backendReady: true,
      },
      {
        key: '3',
        item: t('logs e env sanitizados', 'sanitized logs and env'),
        scope: t('runtime-inspection oficial', 'official runtime inspection'),
        state: t('Protegido', 'Protected'),
        backendReady: true,
      },
    ],
    evidences: [
      t('correlação run_id/change_id do canal', 'channel run_id/change_id correlation'),
      t('membros e chaincodes do read model oficial', 'members and chaincodes from the official read model'),
      t('componentes filtrados por channel_id', 'components filtered by channel_id'),
      t('payload de inspeção com freshness/cache/source', 'inspection payload with freshness/cache/source'),
    ],
  },
  {
    key: 'e2-cr',
    path: '/changes/change-request',
    deliveryKey: 'entrega2',
    title: t(
      'Criação e Acompanhamento de Solicitação de Mudança (CR)',
      'Change Request (CR) creation and tracking'
    ),
    summary: t(
      'Abre CR com escopo técnico, janela de execução e vínculos obrigatórios para fluxo governado.',
      'Opens a CR with technical scope, execution window, and mandatory links for the governed flow.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'O modelo canônico de Solicitação de Mudança (CR) e APIs de criação ainda não foram implementados no núcleo OPSSC.',
      'The canonical Change Request (CR) model and creation APIs have not been implemented in the OPSSC core yet.'
    ),
    metrics: [
      { label: 'CR abertas', value: '00' },
      { label: 'Tipos de mudança', value: '06' },
      { label: 'Tempo médio de abertura', value: '--' },
    ],
    checkpoints: [
      'Selecionar tipo de mudança e impacto',
      'Definir janela e recursos críticos',
      'Anexar evidências e artefatos obrigatórios',
      'Submeter para aprovação multi-org',
    ],
    actions: [
      { label: 'Criar CR', backendReady: false },
      { label: 'Salvar como rascunho', backendReady: false },
      { label: 'Anexar artefatos da mudança', backendReady: false },
      { label: 'Enviar para aprovação', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'CR-2026-0012',
        scope: 'Atualização de chaincode',
        state: 'Rascunho',
        backendReady: false,
      },
      {
        key: '2',
        item: 'CR-2026-0013',
        scope: 'Entrada de organização',
        state: 'Rascunho',
        backendReady: false,
      },
      {
        key: '3',
        item: 'CR-2026-0014',
        scope: 'Ajuste de política',
        state: 'Rascunho',
        backendReady: false,
      },
    ],
    evidences: ['change-id', 'escopo técnico', 'janela operacional', 'risco classificado'],
  },
  {
    key: 'e2-aprovacoes',
    path: '/changes/approvals',
    deliveryKey: 'entrega2',
    title: t('Painel de Aprovações Multi-Org', 'Multi-org approvals dashboard'),
    summary: t(
      'Apresenta quórum por organização, pendências e separação de deveres antes da execução.',
      'Shows quorum by organization, pending items, and separation of duties before execution.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A engine de política/quórum e os endpoints de aprovação ainda não foram implementados no backend.',
      'The policy/quorum engine and approval endpoints have not been implemented in the backend yet.'
    ),
    metrics: [
      { label: 'Aprovações pendentes', value: '00' },
      { label: 'Quórum configurável', value: 'Sim' },
      { label: 'SOD validada', value: '0%' },
    ],
    checkpoints: [
      'Definir quórum por organização e canal',
      'Coletar decisões de aprovadores elegíveis',
      'Bloquear conflito de segregação de deveres',
      'Promover CR para estado aprovado',
    ],
    actions: [
      { label: 'Registrar decisão de aprovação', backendReady: false },
      { label: 'Rejeitar mudança com justificativa', backendReady: false },
      { label: 'Solicitar revisão técnica', backendReady: false },
      { label: 'Recalcular quórum', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'orgAlpha',
        scope: 'Aprovador primário',
        state: 'Pendente',
        backendReady: false,
      },
      {
        key: '2',
        item: 'orgBeta',
        scope: 'Aprovador secundário',
        state: 'Pendente',
        backendReady: false,
      },
      { key: '3', item: 'orgGamma', scope: 'Auditoria', state: 'Pendente', backendReady: false },
    ],
    evidences: [
      'decisão assinada',
      'justificativa',
      'timestamp de aprovação',
      'quórum consolidado',
    ],
  },
  {
    key: 'e2-timeline',
    path: '/changes/timeline',
    deliveryKey: 'entrega2',
    title: t('Linha do tempo de Execução e Checkpoints', 'Execution timeline and checkpoints'),
    summary: t(
      'Exibe progressão de estados do workflow com retry, retomada e falha classificada.',
      'Displays workflow state progression with retry, resume, and classified failure.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A máquina de estados do workflow ainda não está disponível, portanto a timeline opera em modo simulado.',
      'The workflow state machine is not available yet, so the timeline operates in simulated mode.'
    ),
    metrics: [
      { label: 'Estados rastreados', value: '08' },
      { label: 'Retries automáticos', value: '0' },
      { label: 'Checkpoint válido', value: '--' },
    ],
    checkpoints: [
      'Evolução rascunho -> pending_approval -> approved',
      'Execução running com checkpoints intermediários',
      'Tratamento de bloqueio e rollback',
      'Fechamento com evidências por etapa',
    ],
    actions: [
      { label: 'Visualizar eventos do workflow', backendReady: false },
      { label: 'Retomar do último checkpoint', backendReady: false },
      { label: 'Disparar rollback assistido', backendReady: false },
      { label: 'Exportar trilha de execução', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'pending_approval',
        scope: 'CR-2026-0012',
        state: 'Simulado',
        backendReady: false,
      },
      { key: '2', item: 'running', scope: 'CR-2026-0012', state: 'Simulado', backendReady: false },
      {
        key: '3',
        item: 'completed',
        scope: 'CR-2026-0012',
        state: 'Simulado',
        backendReady: false,
      },
    ],
    evidences: [
      'timeline por change-id',
      'executor por etapa',
      'motivo de falha',
      'hash do pacote de evidência',
    ],
  },
  {
    key: 'e3-lifecycle',
    path: '/chaincode-ops/lifecycle',
    deliveryKey: 'entrega3',
    title: t('Painel de Lifecycle de Chaincode', 'Chaincode lifecycle dashboard'),
    summary: t(
      'Orquestra package/install/approve/commit/upgrade com visão por organização e canal.',
      'Orchestrates package/install/approve/commit/upgrade with a view by organization and channel.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Existem operações legadas de chaincode, porém ainda sem integração nativa ao fluxo de Solicitação de Mudança (CR).',
      'There are legacy chaincode operations, but still without native integration with the Change Request (CR) flow.'
    ),
    metrics: [
      { label: 'Chaincodes listados', value: '11' },
      { label: 'Canais monitorados', value: '04' },
      { label: 'Operações governadas', value: 'Parcial' },
    ],
    checkpoints: [
      'Selecionar channel e chaincode alvo',
      'Executar etapas de lifecycle governado',
      'Anexar evidências por organização',
      'Consolidar status final por change-id',
    ],
    actions: [
      { label: 'Consultar chaincodes existentes', backendReady: true },
      { label: 'Iniciar lifecycle governado', backendReady: false },
      { label: 'Acompanhar etapa por organização', backendReady: false },
      { label: 'Emitir relatório de execução', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'asset-transfer',
        scope: 'channel-main',
        state: 'Ativo',
        backendReady: true,
      },
      {
        key: '2',
        item: 'kyc-contract',
        scope: 'channel-finance',
        state: 'Em upgrade',
        backendReady: false,
      },
      {
        key: '3',
        item: 'audit-ledger',
        scope: 'channel-audit',
        state: 'Planejado',
        backendReady: false,
      },
    ],
    evidences: [
      'sequence/version',
      'aprovação por organização',
      'hash do pacote',
      'resultado de commit',
    ],
  },
  {
    key: 'e3-guardrails',
    path: '/chaincode-ops/guardrails',
    deliveryKey: 'entrega3',
    title: t('Guardrails e Bloqueios de Mudança', 'Guardrails and change blocks'),
    summary: t(
      'Exibe pré-checagens operacionais e decisão automática de bloquear/liberar mudança crítica.',
      'Shows operational pre-checks and the automatic decision to block/release a critical change.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'O módulo de guardrails consumindo sinais de saúde ainda não foi implementado no backend.',
      'The guardrails module consuming health signals has not been implemented in the backend yet.'
    ),
    metrics: [
      { label: 'Guardrails ativos', value: '00' },
      { label: 'Bloqueios automáticos', value: '00' },
      { label: 'Mudanças liberadas', value: '00' },
    ],
    checkpoints: [
      'Executar pré-checagem de saúde da rede',
      'Avaliar risco por política operacional',
      'Emitir decisão bloqueado/liberado',
      'Persistir decisão como evidência auditável',
    ],
    actions: [
      { label: 'Rodar pré-checagem', backendReady: false },
      { label: 'Aprovar exceção de guardrail', backendReady: false },
      { label: 'Bloquear CR automaticamente', backendReady: false },
      { label: 'Consultar histórico de bloqueios', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'SLO de latência',
        scope: 'channel-main',
        state: 'Não avaliado',
        backendReady: false,
      },
      {
        key: '2',
        item: 'Saúde de peers',
        scope: 'orgAlpha',
        state: 'Não avaliado',
        backendReady: false,
      },
      {
        key: '3',
        item: 'Integridade de ordem',
        scope: 'cluster-orderer',
        state: 'Não avaliado',
        backendReady: false,
      },
    ],
    evidences: [
      'resultado dos sinais de saúde',
      'motivo do bloqueio',
      'política aplicada',
      'timestamp da decisão',
    ],
  },
  {
    key: 'e3-rollback',
    path: '/chaincode-ops/rollback',
    deliveryKey: 'entrega3',
    title: t('Histórico de Versões e Rollback', 'Version history and rollback'),
    summary: t(
      'Consolida versões implantadas por canal e permite rollback assistido com justificativa técnica.',
      'Consolidates deployed versions by channel and allows assisted rollback with technical justification.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Há consulta parcial de versões no backend legado, mas o rollback governado por workflow ainda não está pronto.',
      'There is partial version lookup in the legacy backend, but workflow-governed rollback is not ready yet.'
    ),
    metrics: [
      { label: 'Versões catalogadas', value: '19' },
      { label: 'Rollback executado', value: '00' },
      { label: 'Cobertura por canal', value: 'Parcial' },
    ],
    checkpoints: [
      'Comparar versão atual vs. candidata',
      'Selecionar ponto de rollback seguro',
      'Executar compensação por etapa',
      'Registrar resultado e impacto',
    ],
    actions: [
      { label: 'Consultar histórico de versão', backendReady: true },
      { label: 'Abrir plano de rollback', backendReady: false },
      { label: 'Executar rollback governado', backendReady: false },
      { label: 'Anexar evidências pós-rollback', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'asset-transfer@2.1.0',
        scope: 'channel-main',
        state: 'Ativo',
        backendReady: true,
      },
      {
        key: '2',
        item: 'asset-transfer@2.0.3',
        scope: 'channel-main',
        state: 'Rollback candidato',
        backendReady: false,
      },
      {
        key: '3',
        item: 'kyc-contract@1.4.2',
        scope: 'channel-finance',
        state: 'Ativo',
        backendReady: true,
      },
    ],
    evidences: [
      'histórico de versão',
      'motivo do rollback',
      'resultado da compensação',
      'change-id correlacionado',
    ],
  },
  {
    key: 'e4-console',
    path: '/operations/console',
    deliveryKey: 'entrega4',
    title: t('Console Operacional Completo', 'Full operational console'),
    summary: t(
      'Orquestra jornada do operador para criar, revisar, executar e auditar mudanças em uma única experiência.',
      'Orchestrates the operator journey to create, review, execute, and audit changes in a single experience.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'As APIs agregadas de estado operacional para o console ainda não foram implementadas no backend.',
      'The aggregated operational state APIs for the console have not been implemented in the backend yet.'
    ),
    metrics: [
      { label: 'Jornadas cobertas', value: '03' },
      { label: 'Módulos conectados', value: '08' },
      { label: 'Atualização em tempo real', value: 'Pendente' },
    ],
    checkpoints: [
      'Abertura de mudança com contexto operacional',
      'Acompanhamento unificado de status e evidências',
      'Finalização com exportação de auditoria',
      'Notificação de riscos e bloqueios ativos',
    ],
    actions: [
      { label: 'Iniciar jornada operacional', backendReady: false },
      { label: 'Acompanhar CR em tempo real', backendReady: false },
      { label: 'Visualizar decisão de guardrail', backendReady: false },
      { label: 'Exportar trilha de operação', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'Fluxo de mudança crítica',
        scope: 'operator',
        state: 'Mockado',
        backendReady: false,
      },
      {
        key: '2',
        item: 'Painel de auditoria',
        scope: 'auditor',
        state: 'Mockado',
        backendReady: false,
      },
      {
        key: '3',
        item: 'Feed de alertas',
        scope: 'operator',
        state: 'Mockado',
        backendReady: false,
      },
    ],
    evidences: [
      'contexto org/canal',
      'timeline de execução',
      'alertas por severidade',
      'relatório auditável',
    ],
  },
  {
    key: 'e4-topologia',
    path: '/operations/topology',
    deliveryKey: 'entrega4',
    title: t('Visualização de Topologia Multi-Org/Canal', 'Multi-org/channel topology view'),
    summary: t(
      'Mostra componentes de rede com drill-down por organização e impacto da mudança nos elementos afetados.',
      'Shows network components with drill-down by organization and change impact on affected elements.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Há dados legados de rede e nós, mas falta o endpoint consolidado de topologia com impacto por change-id.',
      'There is legacy network and node data, but the consolidated topology endpoint with change-id impact is still missing.'
    ),
    metrics: [
      { label: 'Componentes mapeados', value: '42' },
      { label: 'Domínios de canal', value: '04' },
      { label: 'Impacto correlacionado', value: 'Parcial' },
    ],
    checkpoints: [
      'Carregar topologia por organização',
      'Filtrar por canal e componente',
      'Destacar elementos impactados por mudança',
      'Navegar para evidências relacionadas',
    ],
    actions: [
      { label: 'Listar nós e canais existentes', backendReady: true },
      { label: 'Visualizar grafo consolidado', backendReady: false },
      { label: 'Aplicar filtro de impacto', backendReady: false },
      { label: 'Exportar mapa de topologia', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'orgAlpha -> peer0',
        scope: 'channel-main',
        state: 'Ativo',
        backendReady: true,
      },
      {
        key: '2',
        item: 'orgBeta -> peer1',
        scope: 'channel-main',
        state: 'Ativo',
        backendReady: true,
      },
      {
        key: '3',
        item: 'cluster-orderer',
        scope: 'global',
        state: 'Monitorado',
        backendReady: false,
      },
    ],
    evidences: [
      'snapshot de topologia',
      'componentes impactados',
      'estado por organização',
      'change-id associado',
    ],
  },
  {
    key: 'e4-runbooks',
    path: '/operations/runbooks',
    deliveryKey: 'entrega4',
    title: t('Catálogo Operacional de Runbooks', 'Operational runbook catalog'),
    summary: t(
      'Centraliza runbooks por categoria BSO com execução assistida, checklist e trilha de evidências.',
      'Centralizes runbooks by BSO category with assisted execution, checklists, and evidence trail.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'O catálogo operacional e o executor de runbooks versionados ainda não existem no backend COGNUS.',
      'The operational catalog and versioned runbook executor do not exist in the COGNUS backend yet.'
    ),
    metrics: [
      { label: 'Runbooks publicados', value: '00' },
      { label: 'Categorias BSO', value: '05' },
      { label: 'Execuções assistidas', value: '00' },
    ],
    checkpoints: [
      'Selecionar runbook por categoria',
      'Validar pré-condições de execução',
      'Executar checklist com evidências',
      'Publicar resultado no histórico operacional',
    ],
    actions: [
      { label: 'Criar runbook operacional', backendReady: false },
      { label: 'Executar checklist guiado', backendReady: false },
      { label: 'Registrar resultado da execução', backendReady: false },
      { label: 'Versionar runbook', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'RBK-ONBOARD-ORG',
        scope: 'Onboarding',
        state: 'Planejado',
        backendReady: false,
      },
      {
        key: '2',
        item: 'RBK-CC-UPGRADE',
        scope: 'Lifecycle',
        state: 'Planejado',
        backendReady: false,
      },
      {
        key: '3',
        item: 'RBK-INCIDENT-RECOVERY',
        scope: 'SRE',
        state: 'Planejado',
        backendReady: false,
      },
    ],
    evidences: [
      'checklist executado',
      'resultado por etapa',
      'anexos técnicos',
      'assinatura de execução',
    ],
  },
  {
    key: 'e5-politicas',
    path: '/governance/policies',
    deliveryKey: 'entrega5',
    title: t('Gestão de Papéis e Políticas', 'Role and policy management'),
    summary: t(
      'Gerencia RBAC/ABAC por organização, canal e operação com política de menor privilégio.',
      'Manages RBAC/ABAC by organization, channel, and operation with least-privilege policies.'
    ),
    backendStatus: 'partial',
    backendNote: t(
      'Existe gestão legada de usuários, mas a engine de políticas dinâmicas por contexto ainda está pendente.',
      'There is legacy user management, but the dynamic policy engine by context is still pending.'
    ),
    metrics: [
      { label: 'Papéis ativos', value: '07' },
      { label: 'Políticas publicadas', value: '00' },
      { label: 'Cobertura SOD', value: 'Parcial' },
    ],
    checkpoints: [
      'Mapear papéis por perfil de operação',
      'Definir política por recurso e ação',
      'Validar segregação de deveres',
      'Aplicar política no endpoint BAF/PEP',
    ],
    actions: [
      { label: 'Cadastrar usuário e papel legado', backendReady: true },
      { label: 'Criar política contextual', backendReady: false },
      { label: 'Simular permissão efetiva', backendReady: false },
      { label: 'Publicar política ativa', backendReady: false },
    ],
    mockRows: [
      { key: '1', item: 'admin', scope: 'orgAlpha', state: 'Ativo', backendReady: true },
      { key: '2', item: 'operator', scope: 'orgBeta', state: 'Ativo', backendReady: true },
      {
        key: '3',
        item: 'policy.CR.approve',
        scope: 'multi-org',
        state: 'Pendente',
        backendReady: false,
      },
    ],
    evidences: [
      'matriz de papéis',
      'policy-id versionado',
      'resultado de SOD',
      'log de publicação',
    ],
  },
  {
    key: 'e5-auditoria',
    path: '/governance/access-audit',
    deliveryKey: 'entrega5',
    title: t('Auditoria de Acessos e Autorizações Off-chain', 'Off-chain access and authorization auditing'),
    summary: t(
      'Rastreia emissões e revogações de autorização temporal por `access-id` com trilha auditável.',
      'Tracks temporary authorization issuance and revocation by `access-id` with an auditable trail.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'O serviço de autorização off-chain temporal (lease/token) ainda não está implementado.',
      'The temporary off-chain authorization service (lease/token) has not been implemented yet.'
    ),
    metrics: [
      { label: 'Autorizações emitidas', value: '00' },
      { label: 'Revogações automáticas', value: '00' },
      { label: 'Falhas de autorização', value: '00' },
    ],
    checkpoints: [
      'Emitir autorização contextual com prazo',
      'Monitorar expiração e revogação',
      'Bloquear acesso fora de política',
      'Consolidar trilha por access-id',
    ],
    actions: [
      { label: 'Emitir autorização temporária', backendReady: false },
      { label: 'Revogar autorização ativa', backendReady: false },
      { label: 'Consultar auditoria por access-id', backendReady: false },
      { label: 'Exportar relatório de acesso', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'ACCESS-2026-0101',
        scope: 'orgAlpha/channel-main',
        state: 'Simulado',
        backendReady: false,
      },
      {
        key: '2',
        item: 'ACCESS-2026-0102',
        scope: 'orgBeta/channel-fin',
        state: 'Simulado',
        backendReady: false,
      },
      {
        key: '3',
        item: 'ACCESS-2026-0103',
        scope: 'orgGamma/channel-audit',
        state: 'Simulado',
        backendReady: false,
      },
    ],
    evidences: ['access-id', 'motivo da emissão', 'timestamp de expiração', 'evento de revogação'],
  },
  {
    key: 'e5-evidencias',
    path: '/governance/anchored-evidence',
    deliveryKey: 'entrega5',
    title: t('Inspeção de Evidências Ancoradas', 'Anchored evidence inspection'),
    summary: t(
      'Permite validar integridade de artefatos com hash, metadados e vínculo entre decisão e execução.',
      'Allows validating artifact integrity with hash, metadata, and linkage between decision and execution.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A trilha de anchoring de evidências ainda não está implementada na camada de backend.',
      'The evidence anchoring trail has not been implemented in the backend layer yet.'
    ),
    metrics: [
      { label: 'Evidências ancoradas', value: '00' },
      { label: 'Verificações de hash', value: '00' },
      { label: 'Integridade validada', value: '0%' },
    ],
    checkpoints: [
      'Selecionar evidência por change-id',
      'Comparar hash e metadados',
      'Validar cadeia de decisões associadas',
      'Emitir parecer de integridade',
    ],
    actions: [
      { label: 'Registrar evidência ancorada', backendReady: false },
      { label: 'Verificar integridade por hash', backendReady: false },
      { label: 'Relacionar evidência a aprovação', backendReady: false },
      { label: 'Exportar laudo de integridade', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'EV-2026-0090',
        scope: 'CR-2026-0012',
        state: 'Pendente ancoragem',
        backendReady: false,
      },
      {
        key: '2',
        item: 'EV-2026-0091',
        scope: 'CR-2026-0013',
        state: 'Pendente ancoragem',
        backendReady: false,
      },
      {
        key: '3',
        item: 'EV-2026-0092',
        scope: 'CR-2026-0014',
        state: 'Pendente ancoragem',
        backendReady: false,
      },
    ],
    evidences: [
      'hash SHA-256',
      'metadados de origem',
      'assinatura de aprovação',
      'resultado da verificação',
    ],
  },
  {
    key: 'e6-observabilidade',
    path: '/observability/dashboards',
    deliveryKey: 'entrega6',
    title: t('Dashboards de Observabilidade por Camada', 'Layered observability dashboards'),
    summary: t(
      'Consolida métricas, logs e traces por camada (infra, orquestração e Fabric) com filtros operacionais.',
      'Consolidates metrics, logs, and traces by layer (infra, orchestration, and Fabric) with operational filters.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A padronização de telemetria e os endpoints agregados de observabilidade ainda não foram implementados.',
      'Telemetry standardization and aggregated observability endpoints have not been implemented yet.'
    ),
    metrics: [
      { label: 'Dashboards ativos', value: '00' },
      { label: 'Labels padronizadas', value: '00' },
      { label: 'Cobertura de serviços', value: '0%' },
    ],
    checkpoints: [
      'Definir labels obrigatórias de telemetria',
      'Consolidar coleta de métricas, logs e traces',
      'Filtrar visualização por org/canal/change-id',
      'Publicar baseline de SLO por operação crítica',
    ],
    actions: [
      { label: 'Abrir dashboard por camada', backendReady: false },
      { label: 'Aplicar filtros operacionais', backendReady: false },
      { label: 'Correlacionar métricas e logs', backendReady: false },
      { label: 'Exportar snapshot de observabilidade', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'Infra availability',
        scope: 'global',
        state: 'Sem dados',
        backendReady: false,
      },
      {
        key: '2',
        item: 'Workflow latency',
        scope: 'orquestração',
        state: 'Sem dados',
        backendReady: false,
      },
      { key: '3', item: 'Saúde de peer', scope: 'Fabric', state: 'Sem dados', backendReady: false },
    ],
    evidences: [
      'labels padronizadas',
      'série temporal por operação',
      'erro por serviço',
      'correlação por change-id',
    ],
  },
  {
    key: 'e6-incidentes',
    path: '/observability/incidents',
    deliveryKey: 'entrega6',
    title: t('Incidentes Ativos, Alertas e Pós-Mortem', 'Active incidents, alerts, and post-mortem'),
    summary: t(
      'Gerencia incidentes com severidade, ação sugerida e trilha de resolução orientada por runbook SRE.',
      'Manages incidents with severity, suggested action, and a resolution trail guided by an SRE runbook.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A integração de alertas acionáveis e histórico de incidentes ainda não está disponível no backend.',
      'Actionable alert integration and incident history are not available in the backend yet.'
    ),
    metrics: [
      { label: 'Incidentes ativos', value: '00' },
      { label: 'Alertas acionáveis', value: '00' },
      { label: 'MTTR monitorado', value: '--' },
    ],
    checkpoints: [
      'Detectar incidente e classificar severidade',
      'Executar runbook de contenção',
      'Registrar ações e timeline de resposta',
      'Fechar incidente com pós-mortem estruturado',
    ],
    actions: [
      { label: 'Registrar incidente', backendReady: false },
      { label: 'Acionar runbook SRE', backendReady: false },
      { label: 'Atualizar status de mitigação', backendReady: false },
      { label: 'Publicar pós-mortem', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'INC-2026-0001',
        scope: 'channel-main',
        state: 'Sem backend',
        backendReady: false,
      },
      {
        key: '2',
        item: 'INC-2026-0002',
        scope: 'gateway-api',
        state: 'Sem backend',
        backendReady: false,
      },
      {
        key: '3',
        item: 'INC-2026-0003',
        scope: 'ops-agent',
        state: 'Sem backend',
        backendReady: false,
      },
    ],
    evidences: [
      'id do incidente',
      'severidade e impacto',
      'timeline de mitigação',
      'laudo pós-mortem',
    ],
  },
  {
    key: 'e6-correlacao',
    path: '/observability/correlation',
    deliveryKey: 'entrega6',
    title: t(
      'Correlação Visual change-id -> Efeito Operacional',
      'Visual correlation change-id -> operational effect'
    ),
    summary: t(
      'Conecta mudança executada aos efeitos observados em disponibilidade, latência e erro por serviço.',
      'Connects an executed change to the observed effects on availability, latency, and errors by service.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A correlação ponta a ponta por `change-id` e `access-id` ainda depende da instrumentação backend.',
      'End-to-end correlation by `change-id` and `access-id` still depends on backend instrumentation.'
    ),
    metrics: [
      { label: 'Mudanças correlacionadas', value: '00' },
      { label: 'Serviços com impacto', value: '00' },
      { label: 'Sinais de saúde vinculados', value: '0%' },
    ],
    checkpoints: [
      'Selecionar change-id alvo',
      'Comparar baseline antes/depois',
      'Evidenciar impacto por serviço e canal',
      'Anexar correlação ao relatório de mudança',
    ],
    actions: [
      { label: 'Buscar change-id', backendReady: false },
      { label: 'Carregar série temporal comparativa', backendReady: false },
      { label: 'Gerar relatório de impacto', backendReady: false },
      { label: 'Vincular efeito ao guardrail', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'CR-2026-0012',
        scope: 'gateway',
        state: 'Sem correlação',
        backendReady: false,
      },
      {
        key: '2',
        item: 'CR-2026-0013',
        scope: 'workflow-engine',
        state: 'Sem correlação',
        backendReady: false,
      },
      {
        key: '3',
        item: 'CR-2026-0014',
        scope: 'peer-network',
        state: 'Sem correlação',
        backendReady: false,
      },
    ],
    evidences: [
      'change-id correlacionado',
      'sinal antes/depois',
      'impacto por domínio',
      'decisão operacional',
    ],
  },
  {
    key: 'e7-marketplace',
    path: '/catalog/templates',
    deliveryKey: 'entrega7',
    title: t('Catálogo de modelos e artefatos', 'Catalog of templates and artifacts'),
    summary: t(
      'Disponibiliza catálogo versionado para templates, chaincodes e manifests com governança de publicação.',
      'Provides a versioned catalog for templates, chaincodes, and manifests with publication governance.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'O modelo de catálogo, pipeline de publicação e APIs de marketplace ainda não foram implementados.',
      'The catalog model, publication pipeline, and marketplace APIs have not been implemented yet.'
    ),
    metrics: [
      { label: 'Artefatos publicados', value: '00' },
      { label: 'Categorias disponíveis', value: '00' },
      { label: 'Versões homologadas', value: '00' },
    ],
    checkpoints: [
      'Cadastrar metadados e proveniência do artefato',
      'Validar qualidade e compatibilidade mínima',
      'Publicar versão com política de depreciação',
      'Disponibilizar para consumo no console',
    ],
    actions: [
      { label: 'Publicar novo template', backendReady: false },
      { label: 'Submeter aprovação de publicação', backendReady: false },
      { label: 'Deprecar versão anterior', backendReady: false },
      { label: 'Sincronizar catálogo', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'tpl-chaincode-upgrade',
        scope: 'Lifecycle',
        state: 'Planejado',
        backendReady: false,
      },
      {
        key: '2',
        item: 'tpl-onboard-org',
        scope: 'Provisionamento',
        state: 'Planejado',
        backendReady: false,
      },
      {
        key: '3',
        item: 'tpl-incident-runbook',
        scope: 'SRE',
        state: 'Planejado',
        backendReady: false,
      },
    ],
    evidences: ['artifact-id', 'publisher', 'versão semântica', 'aprovação de publicação'],
  },
  {
    key: 'e7-compatibilidade',
    path: '/catalog/compatibility',
    deliveryKey: 'entrega7',
    title: t('Descoberta por Compatibilidade', 'Compatibility discovery'),
    summary: t(
      'Permite filtrar templates por ambiente, canal, perfil de rede e requisitos técnicos.',
      'Allows filtering templates by environment, channel, network profile, and technical requirements.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'As APIs de descoberta e recomendação por compatibilidade ainda não estão disponíveis no backend.',
      'Compatibility discovery and recommendation APIs are not available in the backend yet.'
    ),
    metrics: [
      { label: 'Consultas de descoberta', value: '00' },
      { label: 'Matriz de compatibilidade', value: '0%' },
      { label: 'Incompatibilidades detectadas', value: '00' },
    ],
    checkpoints: [
      'Definir perfil técnico do ambiente alvo',
      'Aplicar filtros de versão e canal',
      'Avaliar restrições de compatibilidade',
      'Selecionar artefato recomendado',
    ],
    actions: [
      { label: 'Buscar por perfil de rede', backendReady: false },
      { label: 'Comparar compatibilidade de versões', backendReady: false },
      { label: 'Validar pré-requisitos técnicos', backendReady: false },
      { label: 'Salvar shortlist de templates', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'tpl-chaincode-upgrade',
        scope: 'dev/k8s',
        state: t('Sem validação', 'No validation'),
        backendReady: false,
      },
      {
        key: '2',
        item: 'tpl-chaincode-upgrade',
        scope: 'prod/docker',
        state: t('Sem validação', 'No validation'),
        backendReady: false,
      },
      {
        key: '3',
        item: 'tpl-onboard-org',
        scope: 'hml/k8s',
        state: t('Sem validação', 'No validation'),
        backendReady: false,
      },
    ],
    evidences: [
      'perfil consultado',
      'resultado de compatibilidade',
      'restrições detectadas',
      'decisão de seleção',
    ],
  },
  {
    key: 'e7-template-cr',
    path: '/catalog/template-link',
    deliveryKey: 'entrega7',
    title: t(
      'Vínculo Modelo -> Solicitação de Mudança (CR)',
      'Template -> Change Request (CR) link'
    ),
    summary: t(
      'Conecta template selecionado ao wizard de CR e rastreia seu uso no histórico operacional.',
      'Connects the selected template to the CR wizard and tracks its use in the operational history.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'Ainda não existe integração backend entre catálogo versionado e criação de Solicitação de Mudança (CR) no orquestrador.',
      'There is still no backend integration between the versioned catalog and Change Request (CR) creation in the orchestrator.'
    ),
    metrics: [
      { label: 'CR com template', value: '00' },
      { label: 'Reuso de artefato', value: '0%' },
      { label: 'Falhas por incompatibilidade', value: '00' },
    ],
    checkpoints: [
      'Selecionar template homologado no catálogo',
      'Anexar template ao wizard de mudança',
      'Executar validações de pré-uso',
      'Persistir vínculo artifact -> change-id',
    ],
    actions: [
      { label: 'Anexar template ao CR', backendReady: false },
      { label: 'Validar compatibilidade antes da submissão', backendReady: false },
      { label: 'Rastrear uso do template no histórico', backendReady: false },
      { label: 'Emitir relatório de reuso', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'CR-2026-0101 <- tpl-chaincode-upgrade',
        scope: 'rascunho',
        state: 'Sem backend',
        backendReady: false,
      },
      {
        key: '2',
        item: 'CR-2026-0102 <- tpl-onboard-org',
        scope: 'rascunho',
        state: 'Sem backend',
        backendReady: false,
      },
      {
        key: '3',
        item: 'CR-2026-0103 <- tpl-incident-runbook',
        scope: 'rascunho',
        state: 'Sem backend',
        backendReady: false,
      },
    ],
    evidences: [
      'artifact-id + change-id',
      'versão aplicada',
      'resultado de validação',
      'histórico de uso',
    ],
  },
  {
    key: 'e8-readiness',
    path: '/readiness/ux',
    deliveryKey: 'entrega8',
    title: t('Refinamento de UX para prontidão', 'UX refinement for readiness'),
    summary: t(
      'Consolida consistência visual, clareza de risco e fluidez das jornadas críticas para gate final.',
      'Consolidates visual consistency, risk clarity, and flow of critical journeys for the final gate.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A etapa depende da conclusão das integrações backend das entregas anteriores para validação final.',
      'This stage depends on the completion of backend integrations from previous deliveries for final validation.'
    ),
    metrics: [
      { label: 'Telas prontas para gate', value: '24' },
      { label: 'Fluxos críticos revisados', value: '08' },
      { label: 'Pendências de backend', value: 'Alta' },
    ],
    checkpoints: [
      'Padronizar componentes e feedback operacional',
      'Reforçar semântica visual de risco e estado',
      'Validar consistência entre jornadas',
      'Preparar checklist de readiness UX',
    ],
    actions: [
      { label: 'Executar checklist de UX', backendReady: false },
      { label: 'Registrar achados de usabilidade', backendReady: false },
      { label: 'Priorizar ajustes antes do gate', backendReady: false },
      { label: 'Gerar parecer de prontidão visual', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'Fluxo CR -> aprovação',
        scope: 'operator',
        state: 'Em revisão',
        backendReady: false,
      },
      {
        key: '2',
        item: 'Fluxo auditoria',
        scope: 'auditor',
        state: 'Em revisão',
        backendReady: false,
      },
      {
        key: '3',
        item: 'Fluxo incidentes',
        scope: 'SRE',
        state: 'Em revisão',
        backendReady: false,
      },
    ],
    evidences: ['checklist UX', 'achados priorizados', 'tempo por tarefa', 'parecer de prontidão'],
  },
  {
    key: 'e8-usabilidade',
    path: '/readiness/usability',
    deliveryKey: 'entrega8',
    title: t('Validação de Usabilidade Operacional', 'Operational usability validation'),
    summary: t(
      'Executa testes guiados por perfil (`operator`, `auditor`, `admin`) para confirmar legibilidade e eficiência.',
      'Runs guided tests by profile (`operator`, `auditor`, `admin`) to confirm readability and efficiency.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A validação final requer integração backend efetiva para medição real de jornada e latência.',
      'Final validation requires effective backend integration for real measurement of journey and latency.'
    ),
    metrics: [
      { label: 'Perfis avaliados', value: '03' },
      { label: 'Cenários críticos', value: '12' },
      { label: 'Tempo médio de tarefa', value: '--' },
    ],
    checkpoints: [
      'Planejar roteiro de teste por perfil',
      'Executar tarefas críticas no console',
      'Mensurar tempo e taxa de sucesso',
      'Consolidar recomendações de melhoria',
    ],
    actions: [
      { label: 'Iniciar sessão de teste guiado', backendReady: false },
      { label: 'Registrar dificuldade por tarefa', backendReady: false },
      { label: 'Comparar resultados por perfil', backendReady: false },
      { label: 'Exportar relatório de usabilidade', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'Jornada do operador',
        scope: 'CR + execução',
        state: 'Planejado',
        backendReady: false,
      },
      {
        key: '2',
        item: 'Jornada do auditor',
        scope: 'evidência + access-id',
        state: 'Planejado',
        backendReady: false,
      },
      {
        key: '3',
        item: 'Jornada de administrador',
        scope: 'política + papéis',
        state: 'Planejado',
        backendReady: false,
      },
    ],
    evidences: [
      'tempo por tarefa',
      'taxa de erro',
      'feedback qualitativo',
      'recomendação aprovada',
    ],
  },
  {
    key: 'e8-documentacao',
    path: '/readiness/docs',
    deliveryKey: 'entrega8',
    title: t('Documentação Final por Perfil', 'Final documentation by profile'),
    summary: t(
      'Publica guias finais de uso do console por perfil com trilhas operacionais e auditoria.',
      'Publishes final console usage guides by profile with operational and auditing trails.'
    ),
    backendStatus: 'pending',
    backendNote: t(
      'A geração automática de documentação vinculada ao estado real do sistema ainda não está implementada.',
      'Automatic documentation generation linked to the real system state has not been implemented yet.'
    ),
    metrics: [
      { label: 'Guias finais publicados', value: '00' },
      { label: 'Perfis cobertos', value: '03' },
      { label: 'Prontidão documental', value: 'Pendente' },
    ],
    checkpoints: [
      'Consolidar procedimentos por perfil',
      'Vincular guias aos fluxos críticos',
      'Anexar evidências e troubleshooting',
      'Publicar pacote final de go-live',
    ],
    actions: [
      { label: 'Gerar guia do operador', backendReady: false },
      { label: 'Gerar guia do auditor', backendReady: false },
      { label: 'Gerar guia do administrador', backendReady: false },
      { label: 'Exportar pacote de documentação final', backendReady: false },
    ],
    mockRows: [
      {
        key: '1',
        item: 'Guide-Operator-v1',
        scope: 'Operação diária',
        state: 'Rascunho',
        backendReady: false,
      },
      {
        key: '2',
        item: 'Guide-Auditor-v1',
        scope: 'Auditoria técnica',
        state: 'Rascunho',
        backendReady: false,
      },
      {
        key: '3',
        item: 'Guide-Admin-v1',
        scope: 'Governança e IAM',
        state: 'Rascunho',
        backendReady: false,
      },
    ],
    evidences: [
      'versão do guia',
      'escopo por perfil',
      'checklist de atualização',
      'aprovação final',
    ],
  },
];

export const screens = rawScreens.map(normalizeCatalogMetadata);

export const screenByKey = screens.reduce((accumulator, screen) => {
  accumulator[screen.key] = screen;
  return accumulator;
}, {});

export const screenByPath = screens.reduce((accumulator, screen) => {
  accumulator[screen.path] = screen;
  return accumulator;
}, {});

export const getDeliveryByKey = key => deliveries.find(delivery => delivery.key === key);
