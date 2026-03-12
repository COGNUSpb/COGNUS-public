import { sanitizeSensitiveText } from '../../../utils/provisioningSecurityRedaction';
import {
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from '../cognusI18n';

export const RUNBOOK_PROVIDER_KEY = 'external-linux';

export const RUNBOOK_BACKEND_STATES = Object.freeze({
  ready: 'ready',
  pending: 'pending',
  invalid: 'invalid',
});

export const RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS = Object.freeze([
  'provision-plan',
  'reconcile-report',
  'inventory-final',
  'stage-reports',
  'verify-report',
  'ssh-execution-log',
]);

const RUNBOOK_STATUS = Object.freeze({
  idle: 'idle',
  running: 'running',
  paused: 'paused',
  failed: 'failed',
  completed: 'completed',
});

const CHECKPOINT_STATUS = Object.freeze({
  pending: 'pending',
  running: 'running',
  paused: 'paused',
  failed: 'failed',
  completed: 'completed',
});

const localizeRunbookUtilsText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const formatRunbookUtilsText = (ptBR, enUS, values, localeCandidate) =>
  formatCognusTemplate(ptBR, enUS, values, localeCandidate || resolveCognusLocale());

const RUNBOOK_UTILS_EN_US_BY_PT_BR = Object.freeze({
  'Validar gate A1.2 do blueprint': 'Validate blueprint A1.2 gate',
  'Resolver hosts alvo no escopo external-linux': 'Resolve target hosts in the external-linux scope',
  'Confirmar pré-condições mínimas do pipeline': 'Confirm minimum pipeline prerequisites',
  'Aplicar lock de recurso crítico/idempotência': 'Apply critical resource/idempotency lock',
  'Materializar runtime em hosts Linux': 'Materialize runtime on Linux hosts',
  'Persistir checkpoint de provision': 'Persist provision checkpoint',
  'Reconciliar baseline MSP/TLS/CA': 'Reconcile MSP/TLS/CA baseline',
  'Reconciliar manifests e connection profiles': 'Reconcile manifests and connection profiles',
  'Persistir checkpoint de reconcile': 'Persist reconcile checkpoint',
  'Executar health checks finais': 'Execute final health checks',
  'Validar consistência do inventário final': 'Validate final inventory consistency',
  'Consolidar evidências e decisão final': 'Consolidate evidence and final decision',
  'Falha sem classificação técnica no catálogo local do runbook.': 'Failure without technical classification in the local runbook catalog.',
  'Coletar evidências do pipeline e revisar o estado da execução antes de nova tentativa.':
    'Collect pipeline evidence and review the execution state before a new attempt.',
  'Provedor informado está fora do escopo external-linux da Entrega 1.':
    'Provided provider is outside Delivery 1 external-linux scope.',
  'Selecionar exclusivamente o provedor external-linux (VM Linux).':
    'Select only the external-linux provider (Linux VM).',
  'Backend do orquestrador ainda está pendente para comandos operacionais.':
    'The orchestrator backend is still pending for operational commands.',
  'Aguardar backend ready e repetir o comando.': 'Wait for backend ready and retry the command.',
  'Backend do orquestrador foi sinalizado como inválido.':
    'The orchestrator backend was flagged as invalid.',
  'Corrigir a inconsistência no backend e validar o pipeline antes de continuar.':
    'Fix the backend inconsistency and validate the pipeline before continuing.',
  'Não há versão de blueprint definida para rastreabilidade da execução.':
    'There is no blueprint version defined for execution traceability.',
  'Selecionar versão publicada e validada do blueprint.':
    'Select a published and validated blueprint version.',
  'Execução sem change_id impede correlação auditável da mudança.':
    'Execution without change_id prevents auditable change correlation.',
  'Definir change_id antes do início da execução.': 'Define change_id before starting execution.',
  'Execução sem run_id oficial impede reabertura e rastreabilidade do fluxo A2.5.':
    'Execution without an official run_id prevents reopening and traceability for flow A2.5.',
  'Carregar contexto oficial do backend contendo run_id antes de iniciar.':
    'Load the official backend context containing run_id before starting.',
  'manifest_fingerprint ausente no contexto oficial da execução A2.5.':
    'manifest_fingerprint is missing from the official A2.5 execution context.',
  'Sincronizar contexto oficial do backend e garantir manifesto validado antes de executar.':
    'Synchronize the official backend context and ensure a validated manifest before executing.',
  'source_blueprint_fingerprint ausente no contexto oficial da execução A2.5.':
    'source_blueprint_fingerprint is missing from the official A2.5 execution context.',
  'Sincronizar contexto oficial do backend e garantir correlação com blueprint fonte.':
    'Synchronize the official backend context and ensure correlation with the source blueprint.',
  'Artefatos mínimos do WP A2.2 não foram confirmados no contexto oficial.':
    'Minimum WP A2.2 artifacts were not confirmed in the official context.',
  'Validar provision-plan/reconcile-report/inventory/stage-reports/verify/ssh-log no backend antes de iniciar.':
    'Validate provision-plan/reconcile-report/inventory/stage-reports/verify/ssh-log in the backend before starting.',
  'Blueprint ainda não passou no gate de validações pré-plano (A1.2).':
    'Blueprint has not yet passed the pre-plan validation gate (A1.2).',
  'Executar lint A1.2 e corrigir inconformidades antes de iniciar.':
    'Run A1.2 lint and fix non-conformities before starting.',
  'Pré-condições mínimas do pipeline A1.3 não foram atendidas.':
    'Minimum A1.3 pipeline prerequisites were not met.',
  'Confirmar evidências mínimas e baseline criptográfico antes de iniciar o runbook.':
    'Confirm minimum evidence and cryptographic baseline before starting the runbook.',
  'Preflight técnico da infraestrutura SSH ainda não está aprovado.':
    'The technical preflight for SSH infrastructure is not yet approved.',
  'Executar preflight no onboarding de Infra SSH e manter todos os hosts como apto antes de iniciar.':
    'Run the preflight in SSH Infra onboarding and keep all hosts ready before starting.',
  'Conectividade SSH para host alvo falhou na etapa prepare.':
    'SSH connectivity to the target host failed during the prepare stage.',
  'Validar acesso SSH (usuário/chave/porta/rede) e retomar do checkpoint.':
    'Validate SSH access (user/key/port/network) and resume from the checkpoint.',
  'Falha de provisionamento no host Linux durante materialização do runtime.':
    'Provisioning failure on the Linux host during runtime materialization.',
  'Analisar relatório de provision, corrigir host alvo e reexecutar do checkpoint válido.':
    'Analyze the provision report, fix the target host, and rerun from the valid checkpoint.',
  'Falha na aplicação de configuração base durante etapa configure.':
    'Failure while applying the base configuration during the configure stage.',
  'Revisar baseline MSP/TLS/CA e artefatos de configuração antes de retomar.':
    'Review the MSP/TLS/CA baseline and configuration artifacts before resuming.',
  'Falha na reconciliação de configuração base durante etapa reconcile.':
    'Failure while reconciling the base configuration during the reconcile stage.',
  'Revisar baseline MSP/TLS/CA, manifests e artifacts de reconciliação antes de retomar.':
    'Review the MSP/TLS/CA baseline, manifests, and reconciliation artifacts before resuming.',
  'Inventário final diverge da topologia declarada no blueprint.':
    'Final inventory diverges from the topology declared in the blueprint.',
  'Comparar artifacts do verify com blueprint e ajustar inconsistências antes da retomada.':
    'Compare verify artifacts with the blueprint and fix inconsistencies before resuming.',
  'Comando solicitado exige execução em estado running.':
    'The requested command requires the execution to be in running state.',
  'Iniciar ou retomar o runbook antes de avançar checkpoints.':
    'Start or resume the runbook before advancing checkpoints.',
  'Comando de retomada exige execução em estado paused.':
    'The resume command requires the execution to be in paused state.',
  'Pausar a execução ativa ou iniciar nova execução.':
    'Pause the active execution or start a new one.',
  'Falha só pode ser registrada quando o runbook está running/paused.':
    'A failure can only be registered when the runbook is running/paused.',
  'Iniciar ou retomar o runbook antes de registrar falha.':
    'Start or resume the runbook before registering a failure.',
  'Reexecução segura só é permitida em estado failed/paused.':
    'Safe re-execution is only allowed in failed/paused state.',
  'Pausar ou marcar falha para liberar retry por checkpoint.':
    'Pause or mark as failed to allow checkpoint retry.',
  'Etapa ativa não foi resolvida no estado corrente do pipeline.':
    'The active stage was not resolved in the current pipeline state.',
  'Reavaliar consistência do estado do runbook e reiniciar execução se necessário.':
    'Reevaluate runbook state consistency and restart execution if necessary.',
  'Etapa ativa sem checkpoint elegível para avanço.':
    'Active stage has no eligible checkpoint for advancement.',
  'Executar troubleshooting da etapa e normalizar checkpoint antes de avançar.':
    'Troubleshoot the stage and normalize the checkpoint before advancing.',
  'Comando bloqueado porque backend do orquestrador está em pending.':
    'Command blocked because the orchestrator backend is pending.',
  'Aguardar backend ready para liberar comandos de execução.':
    'Wait for backend ready to release execution commands.',
  'Comando bloqueado porque backend do orquestrador está em invalid.':
    'Command blocked because the orchestrator backend is invalid.',
  'Corrigir o estado inválido do backend antes de qualquer novo comando operacional.':
    'Fix the invalid backend state before any new operational command.',
  'Conflito de lock oficial detectado para recurso crítico no escopo da execução.':
    'Official lock conflict detected for a critical resource in the execution scope.',
  'Aguardar liberação do lock oficial, sincronizar status e retomar apenas após desbloqueio.':
    'Wait for the official lock release, synchronize status, and resume only after unlock.',
  'Lock oficial ativo no escopo atual impede execução concorrente.':
    'An active official lock in the current scope prevents concurrent execution.',
  'Não executar comandos concorrentes enquanto lock oficial estiver ativo no mesmo escopo.':
    'Do not execute concurrent commands while an official lock is active in the same scope.',
  'Execução ativa sem run_id definido compromete rastreabilidade.':
    'Active execution without a defined run_id compromises traceability.',
  'Reiniciar execução para regenerar run_id determinístico.':
    'Restart execution to regenerate a deterministic run_id.',
  'Execução ativa sem timestamp de início em UTC.':
    'Active execution without a UTC start timestamp.',
  'Normalizar estado com timestamp inicial antes de continuar.':
    'Normalize the state with an initial timestamp before continuing.',
  'Runbook concluído sem timestamp final em UTC.':
    'Runbook completed without a UTC finish timestamp.',
  'Registrar timestamp de conclusão para manter trilha auditável.':
    'Register the completion timestamp to keep an auditable trail.',
  'Runbook em running não pode manter timestamp final preenchido.':
    'A running runbook cannot keep a final timestamp filled in.',
  'Corrigir estado da execução e limpar finalização inválida.':
    'Fix execution state and clear invalid completion.',
  'Etapa ativa não possui checkpoint atual para execução/retomada.':
    'Active stage has no current checkpoint for execution/resume.',
  'Reinicializar etapa ou reexecutar do último checkpoint válido.':
    'Reinitialize the stage or rerun from the latest valid checkpoint.',
  'Pré-condição inválida.': 'Invalid precondition.',
});

const localizeRunbookUtilsValue = value => {
  if (Array.isArray(value)) {
    return value.map(localizeRunbookUtilsValue);
  }
  if (value && typeof value === 'object') {
    return Object.entries(value).reduce((accumulator, [key, currentValue]) => {
      accumulator[key] = localizeRunbookUtilsValue(currentValue);
      return accumulator;
    }, {});
  }
  if (typeof value !== 'string') {
    return value;
  }
  return localizeRunbookUtilsText(value, RUNBOOK_UTILS_EN_US_BY_PT_BR[value] || value);
};

const RUNBOOK_STAGE_TEMPLATES = Object.freeze(localizeRunbookUtilsValue([
  {
    key: 'prepare',
    label: 'prepare',
    completionCriteria: ['preconditions_validated', 'execution_plan_materialized'],
    evidenceArtifacts: ['stage-reports/prepare-report.json', 'execution-plan.json'],
    checkpoints: [
      { key: 'validate_blueprint_gate', label: 'Validar gate A1.2 do blueprint' },
      { key: 'resolve_target_hosts', label: 'Resolver hosts alvo no escopo external-linux' },
      { key: 'check_pipeline_prerequisites', label: 'Confirmar pré-condições mínimas do pipeline' },
    ],
  },
  {
    key: 'provision',
    label: 'provision',
    completionCriteria: ['runtime_dependencies_ready', 'host_node_mapping_applied'],
    evidenceArtifacts: ['stage-reports/provision-report.json', 'runtime-inventory.json'],
    checkpoints: [
      { key: 'acquire_resource_locks', label: 'Aplicar lock de recurso crítico/idempotência' },
      { key: 'materialize_runtime_targets', label: 'Materializar runtime em hosts Linux' },
      { key: 'persist_provision_checkpoint', label: 'Persistir checkpoint de provision' },
    ],
  },
  {
    key: 'reconcile',
    label: 'reconcile',
    completionCriteria: ['reconcile_configuration_applied', 'critical_components_validated'],
    evidenceArtifacts: [
      'stage-reports/reconcile-report.json',
      'connection-profiles.json',
      'network-manifests.json',
    ],
    checkpoints: [
      { key: 'reconcile_msp_tls_baseline', label: 'Reconciliar baseline MSP/TLS/CA' },
      {
        key: 'reconcile_network_artifacts',
        label: 'Reconciliar manifests e connection profiles',
      },
      { key: 'persist_reconcile_checkpoint', label: 'Persistir checkpoint de reconcile' },
    ],
  },
  {
    key: 'verify',
    label: 'verify',
    completionCriteria: ['topology_health_checks_passed', 'inventory_consistency_validated'],
    evidenceArtifacts: [
      'stage-reports/verify-report.json',
      'pipeline-report.json',
      'inventory-final.json',
    ],
    checkpoints: [
      { key: 'run_health_checks', label: 'Executar health checks finais' },
      { key: 'validate_inventory_consistency', label: 'Validar consistência do inventário final' },
      { key: 'consolidate_pipeline_evidence', label: 'Consolidar evidências e decisão final' },
    ],
  },
]));

const DEFAULT_RUNBOOK_DIAGNOSTIC = Object.freeze(localizeRunbookUtilsValue({
  classification: 'critical',
  cause: 'Falha sem classificação técnica no catálogo local do runbook.',
  recommendedAction:
    'Coletar evidências do pipeline e revisar o estado da execução antes de nova tentativa.',
}));

const RUNBOOK_DIAGNOSTIC_CATALOG = Object.freeze(localizeRunbookUtilsValue({
  runbook_provider_not_supported: {
    classification: 'critical',
    cause: 'Provedor informado está fora do escopo external-linux da Entrega 1.',
    recommendedAction: 'Selecionar exclusivamente o provedor external-linux (VM Linux).',
  },
  runbook_backend_pending: {
    classification: 'transient',
    cause: 'Backend do orquestrador ainda está pendente para comandos operacionais.',
    recommendedAction: 'Aguardar backend ready e repetir o comando.',
  },
  runbook_backend_invalid: {
    classification: 'critical',
    cause: 'Backend do orquestrador foi sinalizado como inválido.',
    recommendedAction:
      'Corrigir a inconsistência no backend e validar o pipeline antes de continuar.',
  },
  runbook_blueprint_version_required: {
    classification: 'critical',
    cause: 'Não há versão de blueprint definida para rastreabilidade da execução.',
    recommendedAction: 'Selecionar versão publicada e validada do blueprint.',
  },
  runbook_change_id_required: {
    classification: 'critical',
    cause: 'Execução sem change_id impede correlação auditável da mudança.',
    recommendedAction: 'Definir change_id antes do início da execução.',
  },
  runbook_run_id_required: {
    classification: 'critical',
    cause: 'Execução sem run_id oficial impede reabertura e rastreabilidade do fluxo A2.5.',
    recommendedAction: 'Carregar contexto oficial do backend contendo run_id antes de iniciar.',
  },
  runbook_manifest_fingerprint_required: {
    classification: 'critical',
    cause: 'manifest_fingerprint ausente no contexto oficial da execução A2.5.',
    recommendedAction:
      'Sincronizar contexto oficial do backend e garantir manifesto validado antes de executar.',
  },
  runbook_source_blueprint_fingerprint_required: {
    classification: 'critical',
    cause: 'source_blueprint_fingerprint ausente no contexto oficial da execução A2.5.',
    recommendedAction:
      'Sincronizar contexto oficial do backend e garantir correlação com blueprint fonte.',
  },
  runbook_a2_2_artifacts_missing: {
    classification: 'critical',
    cause: 'Artefatos mínimos do WP A2.2 não foram confirmados no contexto oficial.',
    recommendedAction:
      'Validar provision-plan/reconcile-report/inventory/stage-reports/verify/ssh-log no backend antes de iniciar.',
  },
  runbook_blueprint_not_validated: {
    classification: 'critical',
    cause: 'Blueprint ainda não passou no gate de validações pré-plano (A1.2).',
    recommendedAction: 'Executar lint A1.2 e corrigir inconformidades antes de iniciar.',
  },
  runbook_pipeline_preconditions_missing: {
    classification: 'critical',
    cause: 'Pré-condições mínimas do pipeline A1.3 não foram atendidas.',
    recommendedAction:
      'Confirmar evidências mínimas e baseline criptográfico antes de iniciar o runbook.',
  },
  runbook_preflight_not_approved: {
    classification: 'critical',
    cause: 'Preflight técnico da infraestrutura SSH ainda não está aprovado.',
    recommendedAction:
      'Executar preflight no onboarding de Infra SSH e manter todos os hosts como apto antes de iniciar.',
  },
  prepare_ssh_access_failed: {
    classification: 'critical',
    cause: 'Conectividade SSH para host alvo falhou na etapa prepare.',
    recommendedAction: 'Validar acesso SSH (usuário/chave/porta/rede) e retomar do checkpoint.',
  },
  runbook_ssh_execution_failed: {
    classification: 'critical',
    cause:
      'Execução remota via SSH falhou no host alvo (ex.: chave do host não confiável, chave privada inválida ou acesso negado).',
    recommendedAction:
      'Validar known_hosts/chave do host, credenciais SSH (usuário/chave) e conectividade; após correção, executar retry a partir do checkpoint.',
  },
  runbook_runtime_image_missing: {
    classification: 'critical',
    cause:
      'Imagem runtime obrigatória do COGNUS não está disponível no host alvo para o componente solicitado.',
    recommendedAction:
      'Publicar/disponibilizar a imagem COGNUS exigida no host (ou registry autorizado) e executar retry do checkpoint provision.runtime.',
  },
  provision_host_failure: {
    classification: 'critical',
    cause: 'Falha de provisionamento no host Linux durante materialização do runtime.',
    recommendedAction:
      'Analisar relatório de provision, corrigir host alvo e reexecutar do checkpoint válido.',
  },
  configure_host_failure: {
    classification: 'critical',
    cause: 'Falha na aplicação de configuração base durante etapa configure.',
    recommendedAction: 'Revisar baseline MSP/TLS/CA e artefatos de configuração antes de retomar.',
  },
  reconcile_host_failure: {
    classification: 'critical',
    cause: 'Falha na reconciliação de configuração base durante etapa reconcile.',
    recommendedAction:
      'Revisar baseline MSP/TLS/CA, manifests e artifacts de reconciliação antes de retomar.',
  },
  verify_inventory_node_mismatch: {
    classification: 'critical',
    cause: 'Inventário final diverge da topologia declarada no blueprint.',
    recommendedAction:
      'Comparar artifacts do verify com blueprint e ajustar inconsistências antes da retomada.',
  },
  runbook_not_running: {
    classification: 'transient',
    cause: 'Comando solicitado exige execução em estado running.',
    recommendedAction: 'Iniciar ou retomar o runbook antes de avançar checkpoints.',
  },
  runbook_not_paused: {
    classification: 'transient',
    cause: 'Comando de retomada exige execução em estado paused.',
    recommendedAction: 'Pausar a execução ativa ou iniciar nova execução.',
  },
  runbook_not_active: {
    classification: 'transient',
    cause: 'Falha só pode ser registrada quando o runbook está running/paused.',
    recommendedAction: 'Iniciar ou retomar o runbook antes de registrar falha.',
  },
  runbook_retry_not_allowed: {
    classification: 'transient',
    cause: 'Reexecução segura só é permitida em estado failed/paused.',
    recommendedAction: 'Pausar ou marcar falha para liberar retry por checkpoint.',
  },
  runbook_stage_not_found: {
    classification: 'critical',
    cause: 'Etapa ativa não foi resolvida no estado corrente do pipeline.',
    recommendedAction:
      'Reavaliar consistência do estado do runbook e reiniciar execução se necessário.',
  },
  runbook_stage_without_pending_checkpoint: {
    classification: 'critical',
    cause: 'Etapa ativa sem checkpoint elegível para avanço.',
    recommendedAction:
      'Executar troubleshooting da etapa e normalizar checkpoint antes de avançar.',
  },
  runbook_command_blocked_backend_pending: {
    classification: 'transient',
    cause: 'Comando bloqueado porque backend do orquestrador está em pending.',
    recommendedAction: 'Aguardar backend ready para liberar comandos de execução.',
  },
  runbook_command_blocked_backend_invalid: {
    classification: 'critical',
    cause: 'Comando bloqueado porque backend do orquestrador está em invalid.',
    recommendedAction:
      'Corrigir o estado inválido do backend antes de qualquer novo comando operacional.',
  },
  runbook_resource_lock_conflict: {
    classification: 'transient',
    cause: 'Conflito de lock oficial detectado para recurso crítico no escopo da execução.',
    recommendedAction:
      'Aguardar liberação do lock oficial, sincronizar status e retomar apenas após desbloqueio.',
  },
  runbook_scope_lock_active: {
    classification: 'transient',
    cause: 'Lock oficial ativo no escopo atual impede execução concorrente.',
    recommendedAction:
      'Não executar comandos concorrentes enquanto lock oficial estiver ativo no mesmo escopo.',
  },
  runbook_coherence_missing_run_id: {
    classification: 'critical',
    cause: 'Execução ativa sem run_id definido compromete rastreabilidade.',
    recommendedAction: 'Reiniciar execução para regenerar run_id determinístico.',
  },
  runbook_coherence_missing_started_at: {
    classification: 'critical',
    cause: 'Execução ativa sem timestamp de início em UTC.',
    recommendedAction: 'Normalizar estado com timestamp inicial antes de continuar.',
  },
  runbook_coherence_completed_without_finished_at: {
    classification: 'critical',
    cause: 'Runbook concluído sem timestamp final em UTC.',
    recommendedAction: 'Registrar timestamp de conclusão para manter trilha auditável.',
  },
  runbook_coherence_running_with_finished_at: {
    classification: 'critical',
    cause: 'Runbook em running não pode manter timestamp final preenchido.',
    recommendedAction: 'Corrigir estado da execução e limpar finalização inválida.',
  },
  runbook_coherence_active_without_checkpoint: {
    classification: 'critical',
    cause: 'Etapa ativa não possui checkpoint atual para execução/retomada.',
    recommendedAction: 'Reinicializar etapa ou reexecutar do último checkpoint válido.',
  },
}));

const isPlainObject = value => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const normalizeText = value => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim();
};

const normalizeLower = value => normalizeText(value).toLowerCase();

const normalizeArtifactToken = value =>
  normalizeLower(value)
    .replace(/\\/g, '/')
    .replace(/\.json$/g, '');

const normalizeArtifactCatalog = artifacts =>
  (Array.isArray(artifacts) ? artifacts : [])
    .map(item => {
      if (typeof item === 'string') {
        return item;
      }
      if (isPlainObject(item)) {
        return item.key || item.path || item.artifact || item.name || '';
      }
      return '';
    })
    .map(normalizeArtifactToken)
    .filter(Boolean)
    .filter((token, index, allTokens) => allTokens.indexOf(token) === index);

const resolveMissingRequiredArtifacts = (requiredArtifacts, availableArtifacts) =>
  requiredArtifacts.filter(
    requiredToken =>
      !availableArtifacts.some(availableToken => availableToken.includes(requiredToken))
  );

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const normalizeClassification = value => {
  const normalized = normalizeLower(value);
  if (normalized === 'transient' || normalized === 'critical') {
    return normalized;
  }
  return '';
};

const levelByClassification = classification => {
  const normalized = normalizeClassification(classification);
  if (normalized === 'critical') {
    return 'error';
  }
  if (normalized === 'transient') {
    return 'warning';
  }
  return 'info';
};

const resolveDiagnosticImpactByClassification = classification => {
  const normalized = normalizeClassification(classification);
  if (normalized === 'critical') {
    return localizeRunbookUtilsText(
      'Bloqueia continuidade da execução até correção da causa técnica.',
      'It blocks execution continuity until the technical cause is corrected.'
    );
  }
  if (normalized === 'transient') {
    return localizeRunbookUtilsText(
      'Pode impactar continuidade da etapa atual caso não seja tratado.',
      'It may impact continuity of the current stage if not handled.'
    );
  }
  return localizeRunbookUtilsText(
    'Sem impacto bloqueante para continuidade operacional.',
    'No blocking impact on operational continuity.'
  );
};

const resolveDiagnosticComponent = ({ component = '', stage = '', checkpoint = '' } = {}) => {
  const normalizedComponent = normalizeText(component);
  if (normalizedComponent) {
    return normalizedComponent;
  }
  const normalizedStage = normalizeText(stage);
  const normalizedCheckpoint = normalizeText(checkpoint);
  if (normalizedStage && normalizedCheckpoint) {
    return `${normalizedStage}::${normalizedCheckpoint}`;
  }
  if (normalizedStage) {
    return `stage:${normalizedStage}`;
  }
  if (normalizedCheckpoint) {
    return `checkpoint:${normalizedCheckpoint}`;
  }
  return 'runbook';
};

const resolveDiagnosticFromCatalog = code => {
  const normalizedCode = normalizeLower(code);
  if (normalizedCode && RUNBOOK_DIAGNOSTIC_CATALOG[normalizedCode]) {
    return RUNBOOK_DIAGNOSTIC_CATALOG[normalizedCode];
  }
  return DEFAULT_RUNBOOK_DIAGNOSTIC;
};

const sortIsoTimestamps = values =>
  values
    .map(normalizeText)
    .filter(Boolean)
    .sort((first, second) => first.localeCompare(second));

const latestIsoTimestamp = values => {
  const sorted = sortIsoTimestamps(values);
  if (sorted.length === 0) {
    return '';
  }
  return sorted[sorted.length - 1];
};

const seededHash = (input, seed) => {
  const modulus = 4294967291;
  let hash = Number(seed) % modulus;

  for (let index = 0; index < input.length; index += 1) {
    const charCode = input.charCodeAt(index);
    hash = (hash * 1664525 + charCode + 1013904223) % modulus;
  }

  return Math.floor(hash)
    .toString(16)
    .slice(-8)
    .padStart(8, '0');
};

const normalizeBackendState = backendState => {
  const normalized = normalizeLower(backendState);
  if (normalized === RUNBOOK_BACKEND_STATES.ready) {
    return RUNBOOK_BACKEND_STATES.ready;
  }
  if (normalized === RUNBOOK_BACKEND_STATES.invalid) {
    return RUNBOOK_BACKEND_STATES.invalid;
  }
  return RUNBOOK_BACKEND_STATES.pending;
};

const normalizeDecision = decision => {
  const normalized = normalizeLower(decision);
  if (normalized === 'allow') {
    return 'allow';
  }
  return 'block';
};

const RUNBOOK_EXECUTION_MODES = Object.freeze({
  fresh: 'fresh',
  reused: 'reused',
  reexecuted: 'reexecuted',
});

const RUNBOOK_RESUME_STRATEGIES = Object.freeze({
  freshRun: 'fresh_run',
  sameRunCheckpointReuse: 'same_run_checkpoint_reuse',
  newRunDiff: 'new_run_diff',
  newRunFull: 'new_run_full',
});

const buildDefaultExecutionSemantics = () => ({
  mode: RUNBOOK_EXECUTION_MODES.fresh,
  reused: false,
  reexecuted: false,
  resumeStrategy: RUNBOOK_RESUME_STRATEGIES.freshRun,
  operationType: 'start',
  diffOnly: false,
  sourceRunId: '',
  targetRunId: '',
  sourceChangeId: '',
  targetChangeId: '',
  reusedCheckpointKey: '',
  reusedCheckpointOrder: 0,
});

const buildDefaultScopeLock = () => ({
  active: false,
  scopeKey: '',
  stage: '',
  checkpoint: '',
  acquiredAt: '',
  expiresAt: '',
  ownerRunId: '',
  ownerChangeId: '',
  operationType: '',
  reasonCode: '',
  reasonMessage: '',
});

const pickDefinedValue = (...values) => values.find(value => value !== undefined && value !== null);

const normalizeBoolean = value => {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value === 1;
  }
  const normalized = normalizeLower(value);
  if (!normalized) {
    return false;
  }
  return ['1', 'true', 'yes', 'y', 'sim', 's'].includes(normalized);
};

const normalizePositiveInteger = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizeExecutionMode = value => {
  const normalized = normalizeLower(value);
  if (normalized === RUNBOOK_EXECUTION_MODES.fresh) {
    return RUNBOOK_EXECUTION_MODES.fresh;
  }
  if (normalized === RUNBOOK_EXECUTION_MODES.reused) {
    return RUNBOOK_EXECUTION_MODES.reused;
  }
  if (normalized === RUNBOOK_EXECUTION_MODES.reexecuted) {
    return RUNBOOK_EXECUTION_MODES.reexecuted;
  }
  return RUNBOOK_EXECUTION_MODES.fresh;
};

const normalizeResumeStrategy = value => {
  const normalized = normalizeLower(value);
  if (normalized === RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse) {
    return RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse;
  }
  if (normalized === RUNBOOK_RESUME_STRATEGIES.newRunDiff) {
    return RUNBOOK_RESUME_STRATEGIES.newRunDiff;
  }
  if (normalized === RUNBOOK_RESUME_STRATEGIES.newRunFull) {
    return RUNBOOK_RESUME_STRATEGIES.newRunFull;
  }
  return RUNBOOK_RESUME_STRATEGIES.freshRun;
};

const normalizeExecutionSemantics = (value, fallback = {}) => {
  const safeSemantics = isPlainObject(value) ? value : {};
  const safeFallback = isPlainObject(fallback) ? fallback : {};
  let mode = normalizeExecutionMode(
    pickDefinedValue(
      safeSemantics.mode,
      safeSemantics.execution_mode,
      safeSemantics.executionMode,
      safeFallback.mode
    )
  );
  let reused = normalizeBoolean(
    pickDefinedValue(
      safeSemantics.reused,
      safeSemantics.is_reused,
      safeSemantics.isReused,
      safeFallback.reused
    )
  );
  let reexecuted = normalizeBoolean(
    pickDefinedValue(
      safeSemantics.reexecuted,
      safeSemantics.is_reexecuted,
      safeSemantics.isReexecuted,
      safeFallback.reexecuted
    )
  );
  const resumeStrategy = normalizeResumeStrategy(
    pickDefinedValue(
      safeSemantics.resumeStrategy,
      safeSemantics.resume_strategy,
      safeFallback.resumeStrategy,
      safeFallback.resume_strategy
    )
  );
  let diffOnly = normalizeBoolean(
    pickDefinedValue(
      safeSemantics.diffOnly,
      safeSemantics.diff_only,
      safeSemantics.apply_diff_only,
      safeFallback.diffOnly,
      safeFallback.diff_only
    )
  );

  if (resumeStrategy === RUNBOOK_RESUME_STRATEGIES.newRunDiff) {
    diffOnly = true;
  }
  if (mode === RUNBOOK_EXECUTION_MODES.reexecuted) {
    reexecuted = true;
    reused = true;
  }
  if (reexecuted) {
    mode = RUNBOOK_EXECUTION_MODES.reexecuted;
    reused = true;
  } else if (reused && mode === RUNBOOK_EXECUTION_MODES.fresh) {
    mode = RUNBOOK_EXECUTION_MODES.reused;
  }

  return {
    ...buildDefaultExecutionSemantics(),
    ...safeFallback,
    ...safeSemantics,
    mode,
    reused,
    reexecuted,
    resumeStrategy,
    operationType: normalizeText(
      pickDefinedValue(
        safeSemantics.operationType,
        safeSemantics.operation_type,
        safeFallback.operationType,
        safeFallback.operation_type,
        'start'
      )
    ),
    diffOnly,
    sourceRunId: normalizeText(
      pickDefinedValue(
        safeSemantics.sourceRunId,
        safeSemantics.source_run_id,
        safeFallback.sourceRunId,
        safeFallback.source_run_id
      )
    ),
    targetRunId: normalizeText(
      pickDefinedValue(
        safeSemantics.targetRunId,
        safeSemantics.target_run_id,
        safeFallback.targetRunId,
        safeFallback.target_run_id
      )
    ),
    sourceChangeId: normalizeText(
      pickDefinedValue(
        safeSemantics.sourceChangeId,
        safeSemantics.source_change_id,
        safeFallback.sourceChangeId,
        safeFallback.source_change_id
      )
    ),
    targetChangeId: normalizeText(
      pickDefinedValue(
        safeSemantics.targetChangeId,
        safeSemantics.target_change_id,
        safeFallback.targetChangeId,
        safeFallback.target_change_id
      )
    ),
    reusedCheckpointKey: normalizeText(
      pickDefinedValue(
        safeSemantics.reusedCheckpointKey,
        safeSemantics.reused_checkpoint_key,
        safeSemantics.resumeCheckpointKey,
        safeSemantics.resume_checkpoint_key,
        safeFallback.reusedCheckpointKey,
        safeFallback.reused_checkpoint_key
      )
    ),
    reusedCheckpointOrder: normalizePositiveInteger(
      pickDefinedValue(
        safeSemantics.reusedCheckpointOrder,
        safeSemantics.reused_checkpoint_order,
        safeSemantics.resumeCheckpointOrder,
        safeSemantics.resume_checkpoint_order,
        safeFallback.reusedCheckpointOrder,
        safeFallback.reused_checkpoint_order
      )
    ),
  };
};

const normalizeScopeLock = value => {
  const safeLock = isPlainObject(value) ? value : {};
  const explicitActive = pickDefinedValue(
    safeLock.active,
    safeLock.isActive,
    safeLock.is_active,
    safeLock.locked,
    safeLock.is_locked
  );
  const statusValue = normalizeLower(
    pickDefinedValue(safeLock.status, safeLock.lock_status, safeLock.lockStatus)
  );
  const activeByStatus =
    statusValue === 'active' || statusValue === 'locked' || statusValue === 'blocking';

  return {
    ...buildDefaultScopeLock(),
    ...safeLock,
    active:
      explicitActive !== undefined && explicitActive !== null
        ? normalizeBoolean(explicitActive)
        : activeByStatus,
    scopeKey: normalizeText(
      pickDefinedValue(
        safeLock.scopeKey,
        safeLock.scope_key,
        safeLock.resourceKey,
        safeLock.resource_key,
        safeLock.key,
        safeLock.scope
      )
    ),
    stage: normalizeText(safeLock.stage),
    checkpoint: normalizeText(safeLock.checkpoint),
    acquiredAt: normalizeText(
      pickDefinedValue(
        safeLock.acquiredAt,
        safeLock.acquired_at_utc,
        safeLock.acquiredAtUtc,
        safeLock.acquired_at
      )
    ),
    expiresAt: normalizeText(
      pickDefinedValue(
        safeLock.expiresAt,
        safeLock.expires_at_utc,
        safeLock.expiresAtUtc,
        safeLock.expires_at
      )
    ),
    ownerRunId: normalizeText(
      pickDefinedValue(safeLock.ownerRunId, safeLock.owner_run_id, safeLock.runId, safeLock.run_id)
    ),
    ownerChangeId: normalizeText(
      pickDefinedValue(
        safeLock.ownerChangeId,
        safeLock.owner_change_id,
        safeLock.changeId,
        safeLock.change_id
      )
    ),
    operationType: normalizeText(
      pickDefinedValue(safeLock.operationType, safeLock.operation_type, safeLock.action)
    ),
    reasonCode: normalizeLower(
      pickDefinedValue(safeLock.reasonCode, safeLock.reason_code, safeLock.code)
    ),
    reasonMessage: normalizeText(
      pickDefinedValue(safeLock.reasonMessage, safeLock.reason_message, safeLock.message)
    ),
  };
};

const normalizeOfficialDecision = (
  officialDecision,
  { pipelineStatus = RUNBOOK_STATUS.idle } = {}
) => {
  const safeDecision = isPlainObject(officialDecision) ? officialDecision : {};
  const isCompleted = normalizeLower(pipelineStatus) === RUNBOOK_STATUS.completed;
  const normalizedDecision = normalizeDecision(
    safeDecision.decision || (isCompleted ? 'allow' : 'block')
  );
  let normalizedReasons = [];
  if (Array.isArray(safeDecision.decisionReasons)) {
    normalizedReasons = safeDecision.decisionReasons;
  } else if (Array.isArray(safeDecision.decision_reasons)) {
    normalizedReasons = safeDecision.decision_reasons;
  }
  let requiredEvidenceKeys = [];
  if (Array.isArray(safeDecision.requiredEvidenceKeys)) {
    requiredEvidenceKeys = [...safeDecision.requiredEvidenceKeys];
  } else if (Array.isArray(safeDecision.required_evidence_keys)) {
    requiredEvidenceKeys = [...safeDecision.required_evidence_keys];
  }
  let missingEvidenceKeys = [];
  if (Array.isArray(safeDecision.missingEvidenceKeys)) {
    missingEvidenceKeys = [...safeDecision.missingEvidenceKeys];
  } else if (Array.isArray(safeDecision.missing_evidence_keys)) {
    missingEvidenceKeys = [...safeDecision.missing_evidence_keys];
  }

  return {
    decision: normalizedDecision,
    decisionCode:
      normalizeText(safeDecision.decisionCode) ||
      normalizeText(safeDecision.decision_code) ||
      (normalizedDecision === 'allow'
        ? 'ALLOW_COMPLETED_WITH_MIN_EVIDENCE'
        : 'BLOCK_RUN_NOT_COMPLETED'),
    decisionReasons: normalizedReasons
      .map(reason => normalizeText(reason))
      .filter(Boolean)
      .filter((reason, index, allReasons) => allReasons.indexOf(reason) === index),
    status: normalizeLower(safeDecision.status) || normalizeLower(pipelineStatus) || 'pending',
    requiredEvidenceKeys,
    missingEvidenceKeys,
    evidenceMinimumValid: Boolean(
      safeDecision.evidenceMinimumValid || safeDecision.evidence_minimum_valid
    ),
    timestamp: normalizeText(safeDecision.timestamp) || normalizeText(safeDecision.timestamp_utc),
  };
};

const cloneCheckpoints = (checkpoints = []) =>
  checkpoints.map((checkpoint, index) => ({
    key: checkpoint.key,
    label: checkpoint.label,
    order: index + 1,
    status: CHECKPOINT_STATUS.pending,
    startedAt: '',
    finishedAt: '',
    failure: null,
  }));

const cloneStageTemplates = () =>
  RUNBOOK_STAGE_TEMPLATES.map((stage, index) => ({
    key: stage.key,
    label: stage.label,
    order: index + 1,
    status: CHECKPOINT_STATUS.pending,
    startedAt: '',
    finishedAt: '',
    completionCriteria: [...stage.completionCriteria],
    evidenceArtifacts: [...stage.evidenceArtifacts],
    checkpoints: cloneCheckpoints(stage.checkpoints),
    failure: null,
  }));

export function createRunbookExecutionState(overrides = {}) {
  const defaultOfficialDecision = normalizeOfficialDecision(
    {
      decision: 'block',
      decisionCode: 'BLOCK_RUN_NOT_COMPLETED',
      status: RUNBOOK_STATUS.idle,
      decisionReasons: ['runbook_not_completed'],
    },
    { pipelineStatus: RUNBOOK_STATUS.idle }
  );
  const defaultExecutionSemantics = buildDefaultExecutionSemantics();
  const safeOverrides = isPlainObject(overrides) ? overrides : {};
  const normalizedExecutionSemantics = normalizeExecutionSemantics(
    safeOverrides.executionSemantics,
    defaultExecutionSemantics
  );
  const normalizedScopeLock = normalizeScopeLock(safeOverrides.scopeLock);

  return {
    runId: '',
    changeId: '',
    manifestFingerprint: '',
    sourceBlueprintFingerprint: '',
    providerKey: RUNBOOK_PROVIDER_KEY,
    environmentProfile: 'dev-external-linux',
    blueprintVersion: '',
    blueprintFingerprint: '',
    resolvedSchemaVersion: '1.0.0',
    requiredArtifacts: [...RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS],
    availableArtifacts: [],
    backendState: RUNBOOK_BACKEND_STATES.pending,
    status: RUNBOOK_STATUS.idle,
    startedAt: '',
    finishedAt: '',
    currentStageIndex: 0,
    stages: cloneStageTemplates(),
    events: [],
    officialDecision: defaultOfficialDecision,
    ...safeOverrides,
    runbookResumeContext: isPlainObject(safeOverrides.runbookResumeContext)
      ? { ...safeOverrides.runbookResumeContext }
      : {},
    executionSemantics: normalizedExecutionSemantics,
    scopeLock: normalizedScopeLock,
  };
}

const cloneExecutionState = state => {
  if (!isPlainObject(state)) {
    return createRunbookExecutionState();
  }

  return {
    ...createRunbookExecutionState(),
    ...state,
    requiredArtifacts: Array.isArray(state.requiredArtifacts)
      ? [...state.requiredArtifacts]
      : [...RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS],
    availableArtifacts: Array.isArray(state.availableArtifacts)
      ? [...state.availableArtifacts]
      : [],
    stages: Array.isArray(state.stages)
      ? state.stages.map(stage => ({
          ...stage,
          checkpoints: Array.isArray(stage.checkpoints)
            ? stage.checkpoints.map(checkpoint => ({ ...checkpoint }))
            : [],
          completionCriteria: Array.isArray(stage.completionCriteria)
            ? [...stage.completionCriteria]
            : [],
          evidenceArtifacts: Array.isArray(stage.evidenceArtifacts)
            ? [...stage.evidenceArtifacts]
            : [],
        }))
      : cloneStageTemplates(),
    events: Array.isArray(state.events) ? state.events.map(event => ({ ...event })) : [],
    runbookResumeContext: isPlainObject(state.runbookResumeContext)
      ? { ...state.runbookResumeContext }
      : {},
    executionSemantics: normalizeExecutionSemantics(
      state.executionSemantics,
      createRunbookExecutionState().executionSemantics
    ),
    scopeLock: normalizeScopeLock(state.scopeLock),
    officialDecision: normalizeOfficialDecision(state.officialDecision, {
      pipelineStatus: state.status,
    }),
  };
};

const getCurrentStage = state => {
  if (!isPlainObject(state) || !Array.isArray(state.stages) || state.stages.length === 0) {
    return { stage: null, stageIndex: -1 };
  }

  const stageIndex = Number.isInteger(state.currentStageIndex) ? state.currentStageIndex : 0;

  if (stageIndex >= 0 && stageIndex < state.stages.length) {
    return {
      stage: state.stages[stageIndex],
      stageIndex,
    };
  }

  return { stage: null, stageIndex: -1 };
};

const getCheckpointIndexByStatus = (stage, allowedStatuses) => {
  if (!isPlainObject(stage) || !Array.isArray(stage.checkpoints)) {
    return -1;
  }

  return stage.checkpoints.findIndex(checkpoint => allowedStatuses.includes(checkpoint.status));
};

const appendEvent = (
  state,
  {
    level = '',
    code = 'runbook_event',
    message = '',
    stage = '',
    checkpoint = '',
    changeId = '',
    runId = '',
    component = '',
    cause = '',
    impact = '',
    recommendedAction = '',
    classification = '',
    path = '',
    backendState = '',
    timestamp = toIsoUtc(),
  } = {}
) => {
  const nextState = cloneExecutionState(state);
  const eventIndex = nextState.events.length + 1;
  const normalizedClassification = normalizeClassification(classification);
  const normalizedLevel = normalizeLower(level);
  const normalizedBackendState = backendState
    ? normalizeBackendState(backendState)
    : normalizeBackendState(nextState.backendState);

  nextState.events.push({
    id: `evt-${eventIndex.toString().padStart(4, '0')}`,
    timestamp,
    level: normalizedLevel || levelByClassification(normalizedClassification),
    code: normalizeLower(code) || 'runbook_event',
    stage,
    checkpoint,
    message: sanitizeSensitiveText(message),
    runId: normalizeText(runId) || normalizeText(nextState.runId),
    changeId: normalizeText(changeId) || normalizeText(nextState.changeId),
    component: sanitizeSensitiveText(normalizeText(component)),
    cause: sanitizeSensitiveText(normalizeText(cause)),
    impact: sanitizeSensitiveText(normalizeText(impact)),
    recommendedAction: sanitizeSensitiveText(normalizeText(recommendedAction)),
    classification: normalizedClassification,
    path: sanitizeSensitiveText(normalizeText(path)),
    backendState: normalizedBackendState,
  });

  return nextState;
};

const activateStage = (stage, timestamp) => {
  const stageRef = stage;
  if (!isPlainObject(stageRef)) {
    return;
  }

  stageRef.status = CHECKPOINT_STATUS.running;
  stageRef.startedAt = stageRef.startedAt || timestamp;
  stageRef.failure = null;
};

const activateCheckpoint = (checkpoint, timestamp) => {
  const checkpointRef = checkpoint;
  if (!isPlainObject(checkpointRef)) {
    return;
  }

  checkpointRef.status = CHECKPOINT_STATUS.running;
  checkpointRef.startedAt = checkpointRef.startedAt || timestamp;
  checkpointRef.failure = null;
};

const resetCheckpointForRetry = checkpoint => {
  const checkpointRef = checkpoint;
  if (!isPlainObject(checkpointRef) || checkpointRef.status === CHECKPOINT_STATUS.completed) {
    return;
  }

  checkpointRef.status = CHECKPOINT_STATUS.pending;
  checkpointRef.startedAt = '';
  checkpointRef.finishedAt = '';
  checkpointRef.failure = null;
};

const forceResetCheckpointToPending = checkpoint => {
  const checkpointRef = checkpoint;
  if (!isPlainObject(checkpointRef)) {
    return;
  }

  checkpointRef.status = CHECKPOINT_STATUS.pending;
  checkpointRef.startedAt = '';
  checkpointRef.finishedAt = '';
  checkpointRef.failure = null;
};

export const isBackendCommandReady = backendState =>
  normalizeBackendState(backendState) === RUNBOOK_BACKEND_STATES.ready;

export const resolveRunbookFailureDiagnostic = ({
  code = 'runbook_stage_failure',
  message = '',
  component = '',
  cause = '',
  impact = '',
  recommendedAction = '',
  classification = '',
} = {}) => {
  const normalizedCode = normalizeLower(code) || 'runbook_stage_failure';
  const diagnostic = resolveDiagnosticFromCatalog(normalizedCode);
  const normalizedClassification =
    normalizeClassification(classification) ||
    normalizeClassification(diagnostic.classification) ||
    'critical';

  return {
    code: normalizedCode,
    message:
      sanitizeSensitiveText(normalizeText(message)) ||
      sanitizeSensitiveText(normalizeText(diagnostic.message)) ||
      `Evento técnico identificado: ${normalizedCode}.`,
    component: sanitizeSensitiveText(normalizeText(component)),
    cause:
      sanitizeSensitiveText(normalizeText(cause)) ||
      sanitizeSensitiveText(normalizeText(diagnostic.cause)) ||
      DEFAULT_RUNBOOK_DIAGNOSTIC.cause,
    impact:
      sanitizeSensitiveText(normalizeText(impact)) ||
      resolveDiagnosticImpactByClassification(normalizedClassification),
    recommendedAction:
      sanitizeSensitiveText(normalizeText(recommendedAction)) ||
      sanitizeSensitiveText(normalizeText(diagnostic.recommendedAction)) ||
      DEFAULT_RUNBOOK_DIAGNOSTIC.recommendedAction,
    classification: normalizedClassification,
  };
};

export const recordRunbookBlockingEvent = (
  currentState,
  {
    code = 'runbook_event',
    message = '',
    stage = '',
    checkpoint = '',
    path = '',
    backendState = '',
    changeId = '',
    runId = '',
  } = {}
) => {
  const nextState = cloneExecutionState(currentState);
  const timestamp = toIsoUtc();
  const diagnosticComponent = resolveDiagnosticComponent({ stage, checkpoint });
  const diagnostic = resolveRunbookFailureDiagnostic({
    code,
    message,
    component: diagnosticComponent,
  });
  const normalizedBackendState = backendState
    ? normalizeBackendState(backendState)
    : normalizeBackendState(nextState.backendState);

  nextState.backendState = normalizedBackendState;
  const normalizedCode = normalizeLower(diagnostic.code);
  if (
    normalizedCode === 'runbook_resource_lock_conflict' ||
    normalizedCode === 'runbook_scope_lock_active'
  ) {
    nextState.scopeLock = normalizeScopeLock({
      ...(isPlainObject(nextState.scopeLock) ? nextState.scopeLock : {}),
      active: true,
      scopeKey: isPlainObject(nextState.scopeLock) ? nextState.scopeLock.scopeKey : '',
      stage,
      checkpoint,
      ownerRunId: normalizeText(runId) || normalizeText(nextState.runId),
      ownerChangeId: normalizeText(changeId) || normalizeText(nextState.changeId),
      reasonCode: normalizedCode,
      reasonMessage: diagnostic.message,
    });
  }

  return appendEvent(nextState, {
    code: diagnostic.code,
    message: diagnostic.message,
    stage,
    checkpoint,
    path,
    changeId,
    runId,
    component: diagnostic.component || diagnosticComponent,
    cause: diagnostic.cause,
    impact: diagnostic.impact,
    recommendedAction: diagnostic.recommendedAction,
    classification: diagnostic.classification,
    backendState: normalizedBackendState,
    level: levelByClassification(diagnostic.classification),
    timestamp,
  });
};

export const recordRunbookPreconditionEvents = (currentState, reasons = [], context = {}) =>
  reasons.reduce(
    (state, reason) =>
      recordRunbookBlockingEvent(state, {
        code: reason && reason.code ? reason.code : 'runbook_event',
        message:
          reason && reason.message
            ? reason.message
            : localizeRunbookUtilsText('Pré-condição inválida.', 'Invalid precondition.'),
        path: reason && reason.path ? reason.path : '',
        ...context,
      }),
    cloneExecutionState(currentState)
  );

export const deterministicRunId = ({ changeId, blueprintFingerprint, resolvedSchemaVersion }) => {
  const basis = `${normalizeText(changeId)}|${normalizeLower(blueprintFingerprint)}|${normalizeText(
    resolvedSchemaVersion
  )}`;
  const digest = [
    seededHash(basis, 0x811c9dc5),
    seededHash(basis, 0x27d4eb2f),
    seededHash(basis, 0x9e3779b1),
  ].join('');
  return `run-${digest.slice(0, 24)}`;
};

export const evaluateRunbookStartPreconditions = ({
  providerKey,
  backendState,
  blueprintVersion,
  blueprintValidated,
  pipelinePreconditionsReady,
  preflightApproved = true,
  changeId,
  runId = '',
  manifestFingerprint = '',
  sourceBlueprintFingerprint = '',
  requiredArtifacts = RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS,
  availableArtifacts = [],
  requireOfficialExecutionContext = false,
} = {}) => {
  const reasons = [];
  const normalizedProvider = normalizeLower(providerKey);
  const normalizedBackendState = normalizeBackendState(backendState);
  const normalizedBlueprintVersion = normalizeText(blueprintVersion);
  const normalizedChangeId = normalizeText(changeId);
  const normalizedRunId = normalizeText(runId);
  const normalizedManifestFingerprint = normalizeLower(manifestFingerprint);
  const normalizedSourceBlueprintFingerprint = normalizeLower(sourceBlueprintFingerprint);
  const normalizedRequiredArtifacts = normalizeArtifactCatalog(requiredArtifacts);
  const effectiveRequiredArtifacts =
    normalizedRequiredArtifacts.length > 0
      ? normalizedRequiredArtifacts
      : normalizeArtifactCatalog(RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS);
  const normalizedAvailableArtifacts = normalizeArtifactCatalog(availableArtifacts);

  if (normalizedProvider !== RUNBOOK_PROVIDER_KEY) {
    reasons.push({
      code: 'runbook_provider_not_supported',
      path: 'provider',
      message: localizeRunbookUtilsText(
        'Execução permitida somente para provedor external-linux (VM Linux).',
        'Execution is allowed only for the external-linux provider (Linux VM).'
      ),
    });
  }

  if (normalizedBackendState === RUNBOOK_BACKEND_STATES.pending) {
    reasons.push({
      code: 'runbook_backend_pending',
      path: 'backend_state',
      message: localizeRunbookUtilsText(
        "Comandos bloqueados: backend do pipeline está em estado 'pendente'.",
        "Commands are blocked: pipeline backend is in 'pending' state."
      ),
    });
  }

  if (normalizedBackendState === RUNBOOK_BACKEND_STATES.invalid) {
    reasons.push({
      code: 'runbook_backend_invalid',
      path: 'backend_state',
      message: localizeRunbookUtilsText(
        "Comandos bloqueados: backend do pipeline está em estado 'inválido'.",
        "Commands are blocked: pipeline backend is in 'invalid' state."
      ),
    });
  }

  if (!normalizedBlueprintVersion) {
    reasons.push({
      code: 'runbook_blueprint_version_required',
      path: 'blueprint_version',
      message: localizeRunbookUtilsText(
        'Selecione uma versão de blueprint antes de iniciar o runbook.',
        'Select a blueprint version before starting the runbook.'
      ),
    });
  }

  if (!normalizedChangeId) {
    reasons.push({
      code: 'runbook_change_id_required',
      path: 'change_id',
      message: localizeRunbookUtilsText(
        'change_id é obrigatório para rastrear a execução do runbook.',
        'change_id is required to track runbook execution.'
      ),
    });
  }

  if (requireOfficialExecutionContext) {
    if (!normalizedRunId) {
      reasons.push({
        code: 'runbook_run_id_required',
        path: 'run_id',
        message: localizeRunbookUtilsText(
          'run_id oficial é obrigatório para habilitar ações críticas da jornada A2.5.',
          'Official run_id is required to enable critical actions in journey A2.5.'
        ),
      });
    }

    if (!normalizedManifestFingerprint) {
      reasons.push({
        code: 'runbook_manifest_fingerprint_required',
        path: 'manifest_fingerprint',
        message: localizeRunbookUtilsText(
          'manifest_fingerprint oficial é obrigatório para habilitar ações críticas da jornada A2.5.',
          'Official manifest_fingerprint is required to enable critical actions in journey A2.5.'
        ),
      });
    }

    if (!normalizedSourceBlueprintFingerprint) {
      reasons.push({
        code: 'runbook_source_blueprint_fingerprint_required',
        path: 'source_blueprint_fingerprint',
        message: localizeRunbookUtilsText(
          'source_blueprint_fingerprint oficial é obrigatório para habilitar ações críticas da jornada A2.5.',
          'Official source_blueprint_fingerprint is required to enable critical actions in journey A2.5.'
        ),
      });
    }

    const missingRequiredArtifacts = resolveMissingRequiredArtifacts(
      effectiveRequiredArtifacts,
      normalizedAvailableArtifacts
    );
    if (missingRequiredArtifacts.length > 0) {
      reasons.push({
        code: 'runbook_a2_2_artifacts_missing',
        path: 'a2_2_artifacts',
        message: formatRunbookUtilsText(
          'Início bloqueado: artefatos mínimos do A2.2 ausentes/inconsistentes ({artifacts}).',
          'Start blocked: minimum A2.2 artifacts are missing/inconsistent ({artifacts}).',
          {
            artifacts: missingRequiredArtifacts.join(', '),
          }
        ),
        missingArtifacts: missingRequiredArtifacts,
        requiredArtifacts: effectiveRequiredArtifacts,
        availableArtifacts: normalizedAvailableArtifacts,
      });
    }
  }

  if (!blueprintValidated) {
    reasons.push({
      code: 'runbook_blueprint_not_validated',
      path: 'blueprint_validation',
      message: localizeRunbookUtilsText(
        'Início bloqueado: blueprint precisa estar validado no gate A1.2.',
        'Start blocked: blueprint must be validated at gate A1.2.'
      ),
    });
  }

  if (!pipelinePreconditionsReady) {
    reasons.push({
      code: 'runbook_pipeline_preconditions_missing',
      path: 'pipeline_preconditions',
      message: localizeRunbookUtilsText(
        'Início bloqueado: pré-condições mínimas do pipeline A1.3 não foram atendidas.',
        'Start blocked: minimum A1.3 pipeline prerequisites were not met.'
      ),
    });
  }

  if (!preflightApproved) {
    reasons.push({
      code: 'runbook_preflight_not_approved',
      path: 'preflight_approval',
      message: localizeRunbookUtilsText(
        'Início bloqueado: preflight técnico deve estar aprovado (todos os hosts aptos).',
        'Start blocked: technical preflight must be approved (all hosts ready).'
      ),
    });
  }

  return {
    allowStart: reasons.length === 0,
    reasons,
  };
};

export const startRunbookExecution = (
  currentState,
  {
    changeId,
    runId = '',
    manifestFingerprint = '',
    sourceBlueprintFingerprint = '',
    providerKey = RUNBOOK_PROVIDER_KEY,
    environmentProfile = 'dev-external-linux',
    blueprintVersion,
    blueprintFingerprint,
    resolvedSchemaVersion = '1.0.0',
    backendState = RUNBOOK_BACKEND_STATES.pending,
    blueprintValidated = false,
    pipelinePreconditionsReady = false,
    preflightApproved = true,
    requiredArtifacts = RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS,
    availableArtifacts = [],
    requireOfficialExecutionContext = false,
  } = {}
) => {
  const normalizedRequiredArtifacts = normalizeArtifactCatalog(requiredArtifacts);
  const normalizedAvailableArtifacts = normalizeArtifactCatalog(availableArtifacts);
  const preconditions = evaluateRunbookStartPreconditions({
    providerKey,
    backendState,
    blueprintVersion,
    blueprintValidated,
    pipelinePreconditionsReady,
    preflightApproved,
    changeId,
    runId,
    manifestFingerprint,
    sourceBlueprintFingerprint,
    requiredArtifacts: normalizedRequiredArtifacts,
    availableArtifacts: normalizedAvailableArtifacts,
    requireOfficialExecutionContext,
  });

  if (!preconditions.allowStart) {
    return {
      started: false,
      preconditions,
      nextState: cloneExecutionState(currentState),
    };
  }

  const timestamp = toIsoUtc();
  const seededState = createRunbookExecutionState({
    changeId: normalizeText(changeId),
    runId: normalizeText(runId),
    manifestFingerprint: normalizeLower(manifestFingerprint),
    sourceBlueprintFingerprint: normalizeLower(sourceBlueprintFingerprint),
    providerKey: RUNBOOK_PROVIDER_KEY,
    environmentProfile: normalizeText(environmentProfile),
    blueprintVersion: normalizeText(blueprintVersion),
    blueprintFingerprint: normalizeLower(blueprintFingerprint),
    resolvedSchemaVersion: normalizeText(resolvedSchemaVersion),
    requiredArtifacts:
      normalizedRequiredArtifacts.length > 0
        ? normalizedRequiredArtifacts
        : [...RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS],
    availableArtifacts: normalizedAvailableArtifacts,
    backendState: normalizeBackendState(backendState),
    status: RUNBOOK_STATUS.running,
    startedAt: timestamp,
    finishedAt: '',
    currentStageIndex: 0,
  });

  if (!seededState.runId) {
    seededState.runId = deterministicRunId({
      changeId: seededState.changeId,
      blueprintFingerprint: seededState.blueprintFingerprint,
      resolvedSchemaVersion: seededState.resolvedSchemaVersion,
    });
  }
  seededState.executionSemantics = normalizeExecutionSemantics({
    mode: RUNBOOK_EXECUTION_MODES.fresh,
    reused: false,
    reexecuted: false,
    resumeStrategy: RUNBOOK_RESUME_STRATEGIES.freshRun,
    operationType: 'start',
    diffOnly: false,
    sourceRunId: '',
    targetRunId: seededState.runId,
    sourceChangeId: '',
    targetChangeId: seededState.changeId,
    reusedCheckpointKey: '',
    reusedCheckpointOrder: 0,
  });
  seededState.scopeLock = normalizeScopeLock({ active: false });

  const firstStage = seededState.stages[0];
  activateStage(firstStage, timestamp);
  activateCheckpoint(firstStage.checkpoints[0], timestamp);

  let nextState = appendEvent(seededState, {
    code: 'runbook_started',
    stage: firstStage.key,
    checkpoint: firstStage.checkpoints[0].key,
    message: formatRunbookUtilsText(
      'Runbook {runId} iniciado para change_id {changeId}.',
      'Runbook {runId} started for change_id {changeId}.',
      {
        runId: seededState.runId,
        changeId: seededState.changeId,
      }
    ),
    timestamp,
  });

  nextState = appendEvent(nextState, {
    code: 'stage_started',
    stage: firstStage.key,
    message: formatRunbookUtilsText(
      'Etapa {stageLabel} iniciada.',
      'Stage {stageLabel} started.',
      { stageLabel: firstStage.label }
    ),
    timestamp,
  });

  nextState = appendEvent(nextState, {
    code: 'checkpoint_started',
    stage: firstStage.key,
    checkpoint: firstStage.checkpoints[0].key,
    message: formatRunbookUtilsText(
      "Checkpoint '{checkpointLabel}' em execução.",
      "Checkpoint '{checkpointLabel}' is running.",
      {
        checkpointLabel: firstStage.checkpoints[0].label,
      }
    ),
    timestamp,
  });

  return {
    started: true,
    preconditions,
    nextState,
  };
};

export const advanceRunbookExecution = currentState => {
  const nextState = cloneExecutionState(currentState);

  if (nextState.status !== RUNBOOK_STATUS.running) {
    return {
      advanced: false,
      nextState,
      error: {
        code: 'runbook_not_running',
        message: localizeRunbookUtilsText(
          'A execução só avança quando o runbook está em estado running.',
          'Execution only advances when the runbook is in running state.'
        ),
      },
    };
  }

  const { stage, stageIndex } = getCurrentStage(nextState);
  if (!stage) {
    return {
      advanced: false,
      nextState,
      error: {
        code: 'runbook_stage_not_found',
        message: localizeRunbookUtilsText(
          'Etapa ativa não encontrada para avanço do runbook.',
          'Active stage not found to advance the runbook.'
        ),
      },
    };
  }

  const timestamp = toIsoUtc();
  let checkpointIndex = getCheckpointIndexByStatus(stage, [CHECKPOINT_STATUS.running]);
  if (checkpointIndex < 0) {
    checkpointIndex = getCheckpointIndexByStatus(stage, [CHECKPOINT_STATUS.pending]);
  }

  if (checkpointIndex < 0) {
    return {
      advanced: false,
      nextState,
      error: {
        code: 'runbook_stage_without_pending_checkpoint',
        message: localizeRunbookUtilsText(
          'Nenhum checkpoint pendente na etapa ativa.',
          'There is no pending checkpoint in the active stage.'
        ),
      },
    };
  }

  const checkpoint = stage.checkpoints[checkpointIndex];
  activateStage(stage, timestamp);
  activateCheckpoint(checkpoint, timestamp);
  checkpoint.status = CHECKPOINT_STATUS.completed;
  checkpoint.finishedAt = timestamp;

  let updatedState = appendEvent(nextState, {
    code: 'checkpoint_completed',
    stage: stage.key,
    checkpoint: checkpoint.key,
    message: formatRunbookUtilsText(
      "Checkpoint '{checkpointLabel}' concluído.",
      "Checkpoint '{checkpointLabel}' completed.",
      { checkpointLabel: checkpoint.label }
    ),
    timestamp,
  });

  const updatedStage = updatedState.stages[stageIndex];
  const nextCheckpoint = updatedStage.checkpoints[checkpointIndex + 1];

  if (nextCheckpoint) {
    activateCheckpoint(nextCheckpoint, timestamp);
    updatedState = appendEvent(updatedState, {
      code: 'checkpoint_started',
      stage: updatedStage.key,
      checkpoint: nextCheckpoint.key,
      message: formatRunbookUtilsText(
        "Checkpoint '{checkpointLabel}' em execução.",
        "Checkpoint '{checkpointLabel}' is running.",
        { checkpointLabel: nextCheckpoint.label }
      ),
      timestamp,
    });
    updatedState.scopeLock = normalizeScopeLock({ active: false });

    return {
      advanced: true,
      nextState: updatedState,
      error: null,
    };
  }

  updatedStage.status = CHECKPOINT_STATUS.completed;
  updatedStage.finishedAt = timestamp;
  updatedState = appendEvent(updatedState, {
    code: 'stage_completed',
    stage: updatedStage.key,
    message: formatRunbookUtilsText(
      'Etapa {stageLabel} concluída.',
      'Stage {stageLabel} completed.',
      { stageLabel: updatedStage.label }
    ),
    timestamp,
  });

  const nextStageIndex = stageIndex + 1;
  if (nextStageIndex < updatedState.stages.length) {
    const nextStage = updatedState.stages[nextStageIndex];
    updatedState.currentStageIndex = nextStageIndex;
    activateStage(nextStage, timestamp);
    activateCheckpoint(nextStage.checkpoints[0], timestamp);

    updatedState = appendEvent(updatedState, {
      code: 'stage_started',
      stage: nextStage.key,
      checkpoint: nextStage.checkpoints[0].key,
      message: formatRunbookUtilsText(
        'Etapa {stageLabel} iniciada.',
        'Stage {stageLabel} started.',
        { stageLabel: nextStage.label }
      ),
      timestamp,
    });

    updatedState = appendEvent(updatedState, {
      code: 'checkpoint_started',
      stage: nextStage.key,
      checkpoint: nextStage.checkpoints[0].key,
      message: formatRunbookUtilsText(
        "Checkpoint '{checkpointLabel}' em execução.",
        "Checkpoint '{checkpointLabel}' is running.",
        {
          checkpointLabel: nextStage.checkpoints[0].label,
        }
      ),
      timestamp,
    });
    updatedState.scopeLock = normalizeScopeLock({ active: false });

    return {
      advanced: true,
      nextState: updatedState,
      error: null,
    };
  }

  updatedState.status = RUNBOOK_STATUS.completed;
  updatedState.finishedAt = timestamp;
  updatedState = appendEvent(updatedState, {
    code: 'runbook_completed',
    message: formatRunbookUtilsText(
      'Runbook {runId} concluído com sucesso.',
      'Runbook {runId} completed successfully.',
      { runId: updatedState.runId }
    ),
    timestamp,
  });
  updatedState.scopeLock = normalizeScopeLock({ active: false });

  return {
    advanced: true,
    nextState: updatedState,
    error: null,
  };
};

export const pauseRunbookExecution = currentState => {
  const nextState = cloneExecutionState(currentState);
  if (nextState.status !== RUNBOOK_STATUS.running) {
    return {
      paused: false,
      nextState,
      error: {
        code: 'runbook_not_running',
        message: localizeRunbookUtilsText(
          'Somente execuções em running podem ser pausadas.',
          'Only running executions can be paused.'
        ),
      },
    };
  }

  const { stage } = getCurrentStage(nextState);
  const timestamp = toIsoUtc();

  nextState.status = RUNBOOK_STATUS.paused;
  if (stage && stage.status === CHECKPOINT_STATUS.running) {
    stage.status = CHECKPOINT_STATUS.paused;
    const checkpointIndex = getCheckpointIndexByStatus(stage, [CHECKPOINT_STATUS.running]);
    if (checkpointIndex >= 0) {
      stage.checkpoints[checkpointIndex].status = CHECKPOINT_STATUS.paused;
    }
  }
  nextState.scopeLock = normalizeScopeLock({ active: false });

  return {
    paused: true,
    nextState: appendEvent(nextState, {
      code: 'runbook_paused',
      stage: stage ? stage.key : '',
      message: localizeRunbookUtilsText(
        'Execução pausada no checkpoint ativo.',
        'Execution paused at the active checkpoint.'
      ),
      timestamp,
    }),
    error: null,
  };
};

export const resumeRunbookExecution = currentState => {
  const nextState = cloneExecutionState(currentState);
  if (nextState.status !== RUNBOOK_STATUS.paused) {
    return {
      resumed: false,
      nextState,
      error: {
        code: 'runbook_not_paused',
        message: localizeRunbookUtilsText(
          'Somente execuções em paused podem ser retomadas.',
          'Only paused executions can be resumed.'
        ),
      },
    };
  }

  const { stage } = getCurrentStage(nextState);
  const timestamp = toIsoUtc();

  nextState.status = RUNBOOK_STATUS.running;
  if (stage && stage.status === CHECKPOINT_STATUS.paused) {
    stage.status = CHECKPOINT_STATUS.running;
    let checkpointIndex = getCheckpointIndexByStatus(stage, [CHECKPOINT_STATUS.paused]);
    if (checkpointIndex < 0) {
      checkpointIndex = getCheckpointIndexByStatus(stage, [CHECKPOINT_STATUS.pending]);
    }
    if (checkpointIndex >= 0) {
      activateCheckpoint(stage.checkpoints[checkpointIndex], timestamp);
    }
  }
  const resumedCheckpoint =
    stage &&
    stage.checkpoints.find(checkpoint =>
      [CHECKPOINT_STATUS.running, CHECKPOINT_STATUS.paused, CHECKPOINT_STATUS.pending].includes(
        checkpoint.status
      )
    );
  nextState.executionSemantics = normalizeExecutionSemantics({
    mode: RUNBOOK_EXECUTION_MODES.reused,
    reused: true,
    reexecuted: false,
    resumeStrategy: RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse,
    operationType: 'resume',
    diffOnly: false,
    sourceRunId: nextState.runId,
    targetRunId: nextState.runId,
    sourceChangeId: nextState.changeId,
    targetChangeId: nextState.changeId,
    reusedCheckpointKey: resumedCheckpoint ? resumedCheckpoint.key : '',
    reusedCheckpointOrder: resumedCheckpoint ? resumedCheckpoint.order : 0,
  });
  nextState.scopeLock = normalizeScopeLock({ active: false });

  return {
    resumed: true,
    nextState: appendEvent(nextState, {
      code: 'runbook_resumed',
      stage: stage ? stage.key : '',
      message: localizeRunbookUtilsText(
        'Execução retomada a partir do último checkpoint válido.',
        'Execution resumed from the latest valid checkpoint.'
      ),
      timestamp,
    }),
    error: null,
  };
};

export const failRunbookExecution = (
  currentState,
  {
    code = 'runbook_stage_failure',
    message = localizeRunbookUtilsText(
      'Falha técnica reportada na etapa ativa.',
      'Technical failure reported in the active stage.'
    ),
    cause = '',
    recommendedAction = '',
    classification = '',
  } = {}
) => {
  const nextState = cloneExecutionState(currentState);
  if (![RUNBOOK_STATUS.running, RUNBOOK_STATUS.paused].includes(nextState.status)) {
    return {
      failed: false,
      nextState,
      error: {
        code: 'runbook_not_active',
        message: localizeRunbookUtilsText(
          'Somente execuções em running/paused podem falhar.',
          'Only running/paused executions can fail.'
        ),
      },
    };
  }

  const { stage } = getCurrentStage(nextState);
  const timestamp = toIsoUtc();
  const activeCheckpointIndex = stage
    ? getCheckpointIndexByStatus(stage, [CHECKPOINT_STATUS.running, CHECKPOINT_STATUS.paused])
    : -1;
  const activeCheckpoint =
    stage && activeCheckpointIndex >= 0 ? stage.checkpoints[activeCheckpointIndex] : null;
  const diagnosticComponent = resolveDiagnosticComponent({
    stage: stage ? stage.key : '',
    checkpoint: activeCheckpoint ? activeCheckpoint.key : '',
  });
  const diagnostic = resolveRunbookFailureDiagnostic({
    code,
    message,
    component: diagnosticComponent,
    cause,
    impact: '',
    recommendedAction,
    classification,
  });

  nextState.status = RUNBOOK_STATUS.failed;
  nextState.finishedAt = timestamp;
  nextState.scopeLock = normalizeScopeLock({ active: false });

  let failedCheckpoint = null;
  if (stage) {
    stage.status = CHECKPOINT_STATUS.failed;
    stage.failure = {
      code: diagnostic.code,
      message: diagnostic.message,
      component: diagnostic.component || diagnosticComponent,
      cause: diagnostic.cause,
      impact: diagnostic.impact,
      recommendedAction: diagnostic.recommendedAction,
      classification: diagnostic.classification,
      timestamp,
    };

    if (activeCheckpointIndex >= 0) {
      const checkpoint = stage.checkpoints[activeCheckpointIndex];
      checkpoint.status = CHECKPOINT_STATUS.failed;
      checkpoint.finishedAt = timestamp;
      checkpoint.failure = {
        code: diagnostic.code,
        message: diagnostic.message,
        component: diagnostic.component || diagnosticComponent,
        cause: diagnostic.cause,
        impact: diagnostic.impact,
        recommendedAction: diagnostic.recommendedAction,
        classification: diagnostic.classification,
        timestamp,
      };
      failedCheckpoint = checkpoint;
    }
  }

  return {
    failed: true,
    nextState: appendEvent(nextState, {
      level: levelByClassification(diagnostic.classification),
      code: diagnostic.code,
      stage: stage ? stage.key : '',
      checkpoint: failedCheckpoint ? failedCheckpoint.key : '',
      message: diagnostic.message,
      component: diagnostic.component || diagnosticComponent,
      cause: diagnostic.cause,
      impact: diagnostic.impact,
      recommendedAction: diagnostic.recommendedAction,
      classification: diagnostic.classification,
      timestamp,
    }),
    error: null,
  };
};

export const retryRunbookExecutionFromCheckpoint = currentState => {
  const nextState = cloneExecutionState(currentState);
  if (![RUNBOOK_STATUS.failed, RUNBOOK_STATUS.paused].includes(nextState.status)) {
    return {
      retried: false,
      nextState,
      error: {
        code: 'runbook_retry_not_allowed',
        message: localizeRunbookUtilsText(
          'Reexecução segura só é permitida para estados failed/paused.',
          'Safe re-execution is only allowed for failed/paused states.'
        ),
      },
    };
  }

  const { stage, stageIndex } = getCurrentStage(nextState);
  if (!stage) {
    return {
      retried: false,
      nextState,
      error: {
        code: 'runbook_stage_not_found',
        message: localizeRunbookUtilsText(
          'Não foi possível determinar etapa para reexecução.',
          'Could not determine the stage for re-execution.'
        ),
      },
    };
  }

  const timestamp = toIsoUtc();
  const completedIndexes = stage.checkpoints
    .map((checkpoint, index) => (checkpoint.status === CHECKPOINT_STATUS.completed ? index : -1))
    .filter(index => index >= 0);
  const lastCompletedIndex =
    completedIndexes.length > 0 ? completedIndexes[completedIndexes.length - 1] : -1;
  const resumeCheckpointIndex = lastCompletedIndex + 1;

  stage.checkpoints.forEach((checkpoint, index) => {
    if (index <= lastCompletedIndex) {
      return;
    }
    resetCheckpointForRetry(checkpoint);
  });

  for (
    let pendingStageIndex = stageIndex + 1;
    pendingStageIndex < nextState.stages.length;
    pendingStageIndex += 1
  ) {
    const pendingStage = nextState.stages[pendingStageIndex];
    pendingStage.status = CHECKPOINT_STATUS.pending;
    pendingStage.startedAt = '';
    pendingStage.finishedAt = '';
    pendingStage.failure = null;
    pendingStage.checkpoints.forEach(checkpoint => {
      forceResetCheckpointToPending(checkpoint);
    });
  }

  stage.status = CHECKPOINT_STATUS.running;
  stage.finishedAt = '';
  stage.failure = null;
  const resumeCheckpoint =
    resumeCheckpointIndex < stage.checkpoints.length
      ? stage.checkpoints[resumeCheckpointIndex]
      : null;
  if (resumeCheckpointIndex < stage.checkpoints.length) {
    activateCheckpoint(stage.checkpoints[resumeCheckpointIndex], timestamp);
  }

  nextState.status = RUNBOOK_STATUS.running;
  nextState.finishedAt = '';
  nextState.currentStageIndex = stageIndex;
  nextState.executionSemantics = normalizeExecutionSemantics({
    mode: RUNBOOK_EXECUTION_MODES.reexecuted,
    reused: true,
    reexecuted: true,
    resumeStrategy: RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse,
    operationType: 'retry',
    diffOnly: false,
    sourceRunId: nextState.runId,
    targetRunId: nextState.runId,
    sourceChangeId: nextState.changeId,
    targetChangeId: nextState.changeId,
    reusedCheckpointKey: resumeCheckpoint ? resumeCheckpoint.key : '',
    reusedCheckpointOrder: resumeCheckpoint ? resumeCheckpoint.order : 0,
  });
  nextState.scopeLock = normalizeScopeLock({ active: false });

  let updatedState = appendEvent(nextState, {
    code: 'runbook_retry_from_checkpoint',
    stage: stage.key,
    message: formatRunbookUtilsText(
      'Reexecução segura iniciada a partir do checkpoint {checkpoint}.',
      'Safe re-execution started from checkpoint {checkpoint}.',
      {
        checkpoint: Math.max(resumeCheckpointIndex + 1, 1),
      }
    ),
    timestamp,
  });

  if (resumeCheckpointIndex < stage.checkpoints.length) {
    updatedState = appendEvent(updatedState, {
      code: 'checkpoint_started',
      stage: stage.key,
      checkpoint: stage.checkpoints[resumeCheckpointIndex].key,
      message: formatRunbookUtilsText(
        "Checkpoint '{checkpointLabel}' em execução após retry.",
        "Checkpoint '{checkpointLabel}' is running after retry.",
        {
          checkpointLabel: stage.checkpoints[resumeCheckpointIndex].label,
        }
      ),
      timestamp,
    });
  }

  return {
    retried: true,
    nextState: updatedState,
    error: null,
  };
};

export const calculateRunbookProgress = executionState => {
  const state = cloneExecutionState(executionState);
  const totalCheckpoints = state.stages.reduce(
    (accumulator, stage) => accumulator + stage.checkpoints.length,
    0
  );
  if (totalCheckpoints <= 0) {
    return 0;
  }

  const completedCheckpoints = state.stages.reduce(
    (accumulator, stage) =>
      accumulator +
      stage.checkpoints.filter(checkpoint => checkpoint.status === CHECKPOINT_STATUS.completed)
        .length,
    0
  );

  if (state.status === RUNBOOK_STATUS.completed) {
    return 100;
  }

  return Math.round((completedCheckpoints / totalCheckpoints) * 100);
};

export const buildRunbookStageRows = executionState => {
  const state = cloneExecutionState(executionState);

  return state.stages.map((stage, index) => {
    const completedCount = stage.checkpoints.filter(
      checkpoint => checkpoint.status === CHECKPOINT_STATUS.completed
    ).length;

    const currentCheckpoint =
      stage.checkpoints.find(checkpoint =>
        [CHECKPOINT_STATUS.running, CHECKPOINT_STATUS.paused, CHECKPOINT_STATUS.failed].includes(
          checkpoint.status
        )
      ) || stage.checkpoints.find(checkpoint => checkpoint.status === CHECKPOINT_STATUS.pending);

    return {
      key: stage.key,
      order: stage.order,
      stageKey: stage.key,
      stageLabel: stage.label,
      stageStatus: stage.status,
      stageFailureCode: stage.failure && stage.failure.code ? stage.failure.code : '',
      checkpointsCompleted: completedCount,
      checkpointsTotal: stage.checkpoints.length,
      currentCheckpointLabel: currentCheckpoint ? currentCheckpoint.label : '-',
      currentCheckpointStatus: currentCheckpoint ? currentCheckpoint.status : '-',
      evidenceArtifacts: stage.evidenceArtifacts,
      isCurrentStage: index === state.currentStageIndex,
    };
  });
};

const buildCoherenceIssue = code => {
  const diagnostic = resolveRunbookFailureDiagnostic({ code });
  return {
    code: diagnostic.code,
    message: diagnostic.message,
    cause: diagnostic.cause,
    recommendedAction: diagnostic.recommendedAction,
    classification: diagnostic.classification,
  };
};

const resolveLatestExecutionTimestamp = state => {
  const timestamps = [state.startedAt, state.finishedAt];

  state.events.forEach(event => {
    timestamps.push(event.timestamp);
  });

  state.stages.forEach(stage => {
    timestamps.push(stage.startedAt);
    timestamps.push(stage.finishedAt);

    if (stage.failure && stage.failure.timestamp) {
      timestamps.push(stage.failure.timestamp);
    }

    stage.checkpoints.forEach(checkpoint => {
      timestamps.push(checkpoint.startedAt);
      timestamps.push(checkpoint.finishedAt);

      if (checkpoint.failure && checkpoint.failure.timestamp) {
        timestamps.push(checkpoint.failure.timestamp);
      }
    });
  });

  return latestIsoTimestamp(timestamps);
};

const resolveLatestCheckpointTimestamp = checkpoints =>
  latestIsoTimestamp(checkpoints.map(checkpoint => checkpoint.finishedAt));

export const buildRunbookProducedEvidenceRows = executionState => {
  const state = cloneExecutionState(executionState);

  return state.stages
    .map(stage => {
      const checkpointsCompleted = stage.checkpoints.filter(
        checkpoint => checkpoint.status === CHECKPOINT_STATUS.completed
      ).length;

      if (checkpointsCompleted <= 0) {
        return null;
      }

      return {
        key: `${stage.key}-evidence`,
        stageKey: stage.key,
        stageLabel: stage.label,
        stageOrder: stage.order,
        checkpointsCompleted,
        checkpointsTotal: stage.checkpoints.length,
        checkpointProgress: `${checkpointsCompleted}/${stage.checkpoints.length}`,
        evidenceStatus:
          checkpointsCompleted === stage.checkpoints.length ? 'consolidated' : 'partial',
        evidenceArtifacts: [...stage.evidenceArtifacts],
        producedAt:
          normalizeText(stage.finishedAt) || resolveLatestCheckpointTimestamp(stage.checkpoints),
      };
    })
    .filter(Boolean);
};

export const buildRunbookResumeContext = executionState => {
  const state = cloneExecutionState(executionState);
  const { stage, stageIndex } = getCurrentStage(state);

  if (!stage) {
    return null;
  }

  const completedCheckpointIndexes = stage.checkpoints
    .map((checkpoint, checkpointIndex) =>
      checkpoint.status === CHECKPOINT_STATUS.completed ? checkpointIndex : -1
    )
    .filter(checkpointIndex => checkpointIndex >= 0);
  const lastCompletedCheckpointIndex =
    completedCheckpointIndexes.length > 0
      ? completedCheckpointIndexes[completedCheckpointIndexes.length - 1]
      : -1;
  const resumeCheckpointIndex = lastCompletedCheckpointIndex + 1;
  const lastCompletedCheckpoint =
    lastCompletedCheckpointIndex >= 0 ? stage.checkpoints[lastCompletedCheckpointIndex] : null;
  const resumeCheckpoint =
    resumeCheckpointIndex < stage.checkpoints.length
      ? stage.checkpoints[resumeCheckpointIndex]
      : null;
  const producedEvidenceRows = buildRunbookProducedEvidenceRows(state);
  const executionSemantics = normalizeExecutionSemantics(state.executionSemantics, {
    targetRunId: state.runId,
    targetChangeId: state.changeId,
  });

  return {
    runId: state.runId,
    changeId: state.changeId,
    stageKey: stage.key,
    stageLabel: stage.label,
    stageOrder: stage.order,
    stageIndex,
    lastCompletedCheckpointKey: lastCompletedCheckpoint ? lastCompletedCheckpoint.key : '',
    lastCompletedCheckpointLabel: lastCompletedCheckpoint ? lastCompletedCheckpoint.label : '',
    lastCompletedCheckpointOrder: lastCompletedCheckpoint ? lastCompletedCheckpoint.order : 0,
    resumeCheckpointKey: resumeCheckpoint ? resumeCheckpoint.key : '',
    resumeCheckpointLabel: resumeCheckpoint ? resumeCheckpoint.label : '',
    resumeCheckpointOrder: resumeCheckpoint ? resumeCheckpoint.order : 0,
    executionMode: executionSemantics.mode,
    resumeStrategy: executionSemantics.resumeStrategy,
    reused: executionSemantics.reused,
    reexecuted: executionSemantics.reexecuted,
    diffOnly: executionSemantics.diffOnly,
    sourceRunId: executionSemantics.sourceRunId,
    targetRunId: executionSemantics.targetRunId || state.runId,
    evidenceCount: producedEvidenceRows.length,
    evidenceRows: producedEvidenceRows,
  };
};

export const buildRunbookOfficialSnapshot = (executionState, { backendState = '' } = {}) => {
  const state = cloneExecutionState(executionState);
  const officialBackendState = backendState
    ? normalizeBackendState(backendState)
    : normalizeBackendState(state.backendState);
  const executionSemantics = normalizeExecutionSemantics(state.executionSemantics, {
    targetRunId: state.runId,
    targetChangeId: state.changeId,
  });
  const scopeLock = normalizeScopeLock(state.scopeLock);
  const stageStatuses = state.stages.map(stage => ({
    stageKey: stage.key,
    stageLabel: stage.label,
    status: stage.status,
    checkpointsCompleted: stage.checkpoints.filter(
      checkpoint => checkpoint.status === CHECKPOINT_STATUS.completed
    ).length,
    checkpointsTotal: stage.checkpoints.length,
  }));
  const issues = [];

  if (
    [
      RUNBOOK_STATUS.running,
      RUNBOOK_STATUS.paused,
      RUNBOOK_STATUS.failed,
      RUNBOOK_STATUS.completed,
    ].includes(state.status) &&
    !normalizeText(state.runId)
  ) {
    issues.push(buildCoherenceIssue('runbook_coherence_missing_run_id'));
  }

  if (
    [
      RUNBOOK_STATUS.running,
      RUNBOOK_STATUS.paused,
      RUNBOOK_STATUS.failed,
      RUNBOOK_STATUS.completed,
    ].includes(state.status) &&
    !normalizeText(state.startedAt)
  ) {
    issues.push(buildCoherenceIssue('runbook_coherence_missing_started_at'));
  }

  if (state.status === RUNBOOK_STATUS.completed && !normalizeText(state.finishedAt)) {
    issues.push(buildCoherenceIssue('runbook_coherence_completed_without_finished_at'));
  }

  if (state.status === RUNBOOK_STATUS.running && normalizeText(state.finishedAt)) {
    issues.push(buildCoherenceIssue('runbook_coherence_running_with_finished_at'));
  }

  if ([RUNBOOK_STATUS.running, RUNBOOK_STATUS.paused].includes(state.status)) {
    const { stage } = getCurrentStage(state);
    const hasActiveCheckpoint = Boolean(
      stage &&
        stage.checkpoints.some(checkpoint =>
          [CHECKPOINT_STATUS.running, CHECKPOINT_STATUS.paused, CHECKPOINT_STATUS.pending].includes(
            checkpoint.status
          )
        )
    );

    if (!hasActiveCheckpoint) {
      issues.push(buildCoherenceIssue('runbook_coherence_active_without_checkpoint'));
    }
  }

  if (
    [RUNBOOK_STATUS.running, RUNBOOK_STATUS.paused].includes(state.status) &&
    officialBackendState !== RUNBOOK_BACKEND_STATES.ready
  ) {
    issues.push(
      buildCoherenceIssue(
        officialBackendState === RUNBOOK_BACKEND_STATES.pending
          ? 'runbook_command_blocked_backend_pending'
          : 'runbook_command_blocked_backend_invalid'
      )
    );
  }

  if (
    scopeLock.active &&
    [RUNBOOK_STATUS.running, RUNBOOK_STATUS.paused, RUNBOOK_STATUS.failed].includes(state.status)
  ) {
    issues.push(buildCoherenceIssue('runbook_scope_lock_active'));
  }

  const hasCriticalIssue = issues.some(issue => issue.classification === 'critical');
  let coherenceStatus = 'consistent';
  if (hasCriticalIssue) {
    coherenceStatus = 'inconsistent';
  } else if (issues.length > 0) {
    coherenceStatus = 'attention';
  }

  const officialDecision = normalizeOfficialDecision(state.officialDecision, {
    pipelineStatus: state.status,
  });

  return {
    runId: normalizeText(state.runId),
    changeId: normalizeText(state.changeId),
    pipelineStatus: state.status,
    backendState: officialBackendState,
    executionSemantics,
    scopeLock,
    stageStatuses,
    officialDecision,
    lastUpdatedAt: resolveLatestExecutionTimestamp(state),
    coherence: {
      isConsistent: issues.length === 0,
      status: coherenceStatus,
      issues,
    },
  };
};

export const getLatestRunbookFailure = executionState => {
  const state = cloneExecutionState(executionState);
  const latestDiagnosticEvent = [...state.events].reverse().find(event => {
    const eventClassification = normalizeClassification(event.classification);
    return (
      event.level === 'error' ||
      event.level === 'warning' ||
      eventClassification === 'critical' ||
      eventClassification === 'transient'
    );
  });

  if (latestDiagnosticEvent) {
    const diagnostic = resolveRunbookFailureDiagnostic({
      code: latestDiagnosticEvent.code,
      message: latestDiagnosticEvent.message,
      component: latestDiagnosticEvent.component,
      cause: latestDiagnosticEvent.cause,
      impact: latestDiagnosticEvent.impact,
      recommendedAction: latestDiagnosticEvent.recommendedAction,
      classification: latestDiagnosticEvent.classification,
    });

    return {
      ...diagnostic,
      timestamp: latestDiagnosticEvent.timestamp,
      stage: latestDiagnosticEvent.stage || '',
      checkpoint: latestDiagnosticEvent.checkpoint || '',
      component:
        latestDiagnosticEvent.component ||
        resolveDiagnosticComponent({
          stage: latestDiagnosticEvent.stage || '',
          checkpoint: latestDiagnosticEvent.checkpoint || '',
        }),
      changeId: latestDiagnosticEvent.changeId || state.changeId,
      runId: latestDiagnosticEvent.runId || state.runId,
      backendState: latestDiagnosticEvent.backendState || state.backendState,
      path: latestDiagnosticEvent.path || '',
    };
  }

  const latestStageFailure = [...state.stages]
    .reverse()
    .find(stage => stage.failure && stage.failure.code);

  if (!latestStageFailure) {
    return null;
  }

  const diagnostic = resolveRunbookFailureDiagnostic({
    code: latestStageFailure.failure.code,
    message: latestStageFailure.failure.message,
    component: latestStageFailure.failure.component,
    cause: latestStageFailure.failure.cause,
    impact: latestStageFailure.failure.impact,
    recommendedAction: latestStageFailure.failure.recommendedAction,
    classification: latestStageFailure.failure.classification,
  });

  return {
    ...diagnostic,
    timestamp: latestStageFailure.failure.timestamp || '',
    stage: latestStageFailure.key,
    checkpoint: '',
    component:
      latestStageFailure.failure.component ||
      resolveDiagnosticComponent({ stage: latestStageFailure.key, checkpoint: '' }),
    changeId: state.changeId,
    runId: state.runId,
    backendState: state.backendState,
    path: '',
  };
};

export const formatUtcTimestamp = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return '-';
  }
  return normalized.replace('T', ' ').replace('Z', ' UTC');
};

export const runbookStatusLabelMap = Object.freeze({
  idle: 'idle',
  running: 'running',
  paused: 'paused',
  failed: 'failed',
  completed: 'completed',
});

export const runbookStatusToneMap = Object.freeze({
  idle: 'default',
  running: 'processing',
  paused: 'warning',
  failed: 'error',
  completed: 'success',
});

export const checkpointStatusToneMap = Object.freeze({
  pending: 'default',
  running: 'processing',
  paused: 'warning',
  failed: 'error',
  completed: 'success',
});

export const RUNBOOK_EXECUTION_STATUS = RUNBOOK_STATUS;
export const RUNBOOK_CHECKPOINT_STATUS = CHECKPOINT_STATUS;
