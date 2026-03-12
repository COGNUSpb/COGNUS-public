import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Collapse,
  Input,
  Progress,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import { history } from 'umi';
import {
  ArrowLeftOutlined,
  ArrowRightOutlined,
  CheckCircleOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  StepForwardOutlined,
} from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import styles from '../components/NeoOpsLayout.less';
import runbookStyles from './ProvisioningRunbookPage.less';
import { screenByKey } from '../data/screens';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import {
  getProvisioningActionReadiness,
  READINESS_STATUS,
  READINESS_STATUS_TONE_MAP,
} from './provisioningBackendReadiness';
import { PROVISIONING_INFRA_ROUTE_PATH } from '../data/provisioningContract';
import {
  RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS,
  RUNBOOK_BACKEND_STATES,
  RUNBOOK_CHECKPOINT_STATUS,
  RUNBOOK_EXECUTION_STATUS,
  RUNBOOK_PROVIDER_KEY,
  buildRunbookOfficialSnapshot,
  buildRunbookProducedEvidenceRows,
  buildRunbookResumeContext,
  buildRunbookStageRows,
  calculateRunbookProgress,
  checkpointStatusToneMap,
  createRunbookExecutionState,
  deterministicRunId,
  evaluateRunbookStartPreconditions,
  formatUtcTimestamp,
  getLatestRunbookFailure,
  isBackendCommandReady,
  recordRunbookBlockingEvent,
  recordRunbookPreconditionEvents,
  runbookStatusToneMap,
} from './provisioningRunbookUtils';
import {
  buildRunbookBlueprintCatalogEntry,
  clearOnboardingRunbookHandoff,
  consumeOnboardingRunbookHandoff,
} from './provisioningOnboardingHandoffUtils';
import { RUNBOOK_LOCAL_MODE_ENABLED, operateRunbook, startRunbook } from '@/services/runbook';
import { listBlueprintVersions } from '@/services/blueprint';
import { IS_PROVISIONING_OPERATIONAL_ENV } from '@/services/provisioningEnvironmentPolicy';
import { sanitizeSensitiveText } from '@/utils/provisioningSecurityRedaction';
import {
  OperationalWindowDialog,
  OperationalWindowManagerProvider,
} from '../components/OperationalWindowManager';
import {
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from '../cognusI18n';

const screen = screenByKey['e1-provisionamento'];

const providerOptions = [
  {
    value: RUNBOOK_PROVIDER_KEY,
    label: 'external-linux (VM Linux)',
  },
];

const environmentProfileOptions = [
  { value: 'dev-external-linux', label: 'dev-external-linux' },
  { value: 'hml-external-linux', label: 'hml-external-linux' },
  { value: 'prod-external-linux', label: 'prod-external-linux' },
];

const fallbackBlueprintVersionCatalog = [
  {
    value: '1.2.0',
    label: 'v1.2.0',
    lintValid: true,
    resolvedSchemaVersion: '1.0.0',
    fingerprint: '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
  },
  {
    value: '1.1.0',
    label: 'v1.1.0',
    lintValid: true,
    resolvedSchemaVersion: '1.0.0',
    fingerprint: '4c66f210f9d3bb14cf259503d2a8d02e6ba5ef1ef6a8e0a52691682a3e8d9c42',
  },
  {
    value: '1.0.0-legacy',
    label: 'v1.0.0-legacy',
    lintValid: false,
    resolvedSchemaVersion: '1.0.0',
    fingerprint: 'd2ae53f1b98f2ac0f264a6303b6383dbac152d5ff91f51d1da6175989d038181',
  },
];

const backendStateOptions = [
  { value: RUNBOOK_BACKEND_STATES.ready, label: 'ready' },
  { value: RUNBOOK_BACKEND_STATES.pending, label: 'pending' },
  { value: RUNBOOK_BACKEND_STATES.invalid, label: 'invalid' },
];
const OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH = 'official_backend_publish';

const eventLevelColor = {
  info: 'blue',
  warning: 'orange',
  error: 'red',
};
const eventLevelLabel = {
  info: 'info',
  warning: 'warning',
  error: 'error',
};

const statusTagColor = {
  default: 'default',
  processing: 'blue',
  warning: 'orange',
  error: 'red',
  success: 'green',
};

const diagnosticClassificationToneMap = {
  informational: 'processing',
  transient: 'warning',
  critical: 'error',
};

const TIMELINE_UNKNOWN_STAGE_ORDER = 9999;
const TIMELINE_SORT_FALLBACK_TIMESTAMP = '9999-12-31T23:59:59Z';

const snapshotCoherenceToneMap = {
  consistent: 'success',
  attention: 'warning',
  inconsistent: 'error',
};

const decisionToneMap = {
  allow: 'success',
  block: 'error',
};

const evidenceStatusToneMap = {
  partial: 'warning',
  consolidated: 'success',
};

const executionModeToneMap = {
  fresh: 'default',
  reused: 'warning',
  reexecuted: 'processing',
};

const executionModeLabelMap = {
  fresh: { ptBR: 'nova execução', enUS: 'fresh' },
  reused: { ptBR: 'reaproveitada', enUS: 'reused' },
  reexecuted: { ptBR: 'reexecutada', enUS: 'reexecuted' },
};

const resumeStrategyLabelMap = {
  fresh_run: { ptBR: 'novo run completo', enUS: 'new full run' },
  same_run_checkpoint_reuse: {
    ptBR: 'mesmo run com reaproveitamento de checkpoint',
    enUS: 'same run with checkpoint reuse',
  },
  new_run_diff: {
    ptBR: 'novo run com aplicação apenas de diffs',
    enUS: 'new run with diff-only application',
  },
  new_run_full: {
    ptBR: 'novo run sem reaproveitamento de checkpoint',
    enUS: 'new run without checkpoint reuse',
  },
};

const A2_5_GATE_REASON_CODE_SET = new Set([
  'runbook_backend_pending',
  'runbook_backend_invalid',
  'runbook_change_id_required',
  'runbook_run_id_required',
  'runbook_manifest_fingerprint_required',
  'runbook_source_blueprint_fingerprint_required',
  'runbook_a2_2_artifacts_missing',
]);

const normalizeText = value => String(value || '').trim();

const resolveLocalizedRunbookLabel = (labelCandidate, localeCandidate) => {
  if (
    labelCandidate &&
    typeof labelCandidate === 'object' &&
    typeof labelCandidate.ptBR === 'string' &&
    typeof labelCandidate.enUS === 'string'
  ) {
    return pickCognusText(labelCandidate.ptBR, labelCandidate.enUS, localeCandidate);
  }

  return normalizeText(labelCandidate);
};

const localizeRunbookOperationalText = (text, localeCandidate) => {
  const normalizedText = normalizeText(text);
  if (!normalizedText) {
    return '';
  }

  const translations = {
    'Evento operacional recebido do backend.': 'Operational event received from the backend.',
    'Falha técnica detectada no backend oficial.': 'Technical failure detected in the official backend.',
    'Condição transitória detectada no backend oficial.': 'Transient condition detected in the official backend.',
    'Evento operacional informativo recebido do backend oficial.':
      'Informational operational event received from the official backend.',
    'Bloqueia continuidade da execução até correção.':
      'Blocks execution continuity until the issue is fixed.',
    'Pode impactar continuidade da etapa atual.':
      'May impact continuity of the current stage.',
    'Sem impacto bloqueante para continuidade operacional.':
      'No blocking impact for operational continuity.',
    'Corrigir a causa técnica, validar evidências e executar retry.':
      'Fix the technical cause, validate the evidence, and run a retry.',
    'Analisar evidências da etapa e confirmar estabilidade antes de avançar.':
      'Review the stage evidence and confirm stability before advancing.',
    'Sem ação imediata.': 'No immediate action.',
    'Decisão oficial bloqueada até reconciliação técnica.':
      'Official decision blocked until technical reconciliation.',
    'Decisão oficial liberada para continuidade operacional.':
      'Official decision cleared for operational continuity.',
    'sem lock ativo no escopo operacional': 'no active lock in the operational scope',
  };

  return pickCognusText(normalizedText, translations[normalizedText] || normalizedText, localeCandidate);
};

const normalizeTimelineLevel = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === 'warning' || normalized === 'error' || normalized === 'info') {
    return normalized;
  }
  return 'info';
};

const normalizeTimelineClassification = (value, level) => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === 'critical' || normalized === 'transient' || normalized === 'informational') {
    return normalized;
  }
  if (level === 'error') {
    return 'critical';
  }
  if (level === 'warning') {
    return 'transient';
  }
  return 'informational';
};

const resolveTimelineComponent = ({
  component = '',
  hostRef = '',
  stage = '',
  checkpoint = '',
}) => {
  const normalizedComponent = normalizeText(component);
  if (normalizedComponent) {
    return normalizedComponent;
  }
  const normalizedHostRef = normalizeText(hostRef);
  if (normalizedHostRef) {
    return `host:${normalizedHostRef}`;
  }
  if (stage && checkpoint) {
    return `${stage}::${checkpoint}`;
  }
  if (stage) {
    return `stage:${stage}`;
  }
  if (checkpoint) {
    return `checkpoint:${checkpoint}`;
  }
  return 'runbook';
};

const resolveTimelineCause = ({ cause = '', eventMessage = '', level = 'info', localeCandidate }) => {
  const normalizedCause = normalizeText(cause);
  if (normalizedCause) {
    return localizeRunbookOperationalText(normalizedCause, localeCandidate);
  }
  const normalizedMessage = normalizeText(eventMessage);
  if (normalizedMessage) {
    return localizeRunbookOperationalText(normalizedMessage, localeCandidate);
  }
  if (level === 'error') {
    return localizeRunbookOperationalText(
      'Falha técnica detectada no backend oficial.',
      localeCandidate
    );
  }
  if (level === 'warning') {
    return localizeRunbookOperationalText(
      'Condição transitória detectada no backend oficial.',
      localeCandidate
    );
  }
  return localizeRunbookOperationalText(
    'Evento operacional informativo recebido do backend oficial.',
    localeCandidate
  );
};

const resolveTimelineImpact = ({ impact = '', level = 'info', stage = '', localeCandidate }) => {
  const normalizedImpact = normalizeText(impact);
  if (normalizedImpact) {
    return localizeRunbookOperationalText(normalizedImpact, localeCandidate);
  }
  if (stage && (level === 'error' || level === 'warning')) {
    return formatCognusTemplate(
      "Impacta continuidade da etapa '{stage}' até tratamento do evento.",
      "Impacts continuity of stage '{stage}' until the event is handled.",
      { stage },
      localeCandidate
    );
  }
  if (level === 'error') {
    return localizeRunbookOperationalText(
      'Bloqueia continuidade da execução até correção.',
      localeCandidate
    );
  }
  if (level === 'warning') {
    return localizeRunbookOperationalText(
      'Pode impactar continuidade da etapa atual.',
      localeCandidate
    );
  }
  return localizeRunbookOperationalText(
    'Sem impacto bloqueante para continuidade operacional.',
    localeCandidate
  );
};

const resolveTimelineRecommendedAction = ({
  recommendedAction = '',
  level = 'info',
  localeCandidate,
}) => {
  const normalizedAction = normalizeText(recommendedAction);
  if (normalizedAction) {
    return localizeRunbookOperationalText(normalizedAction, localeCandidate);
  }
  if (level === 'error') {
    return localizeRunbookOperationalText(
      'Corrigir a causa técnica, validar evidências e executar retry.',
      localeCandidate
    );
  }
  if (level === 'warning') {
    return localizeRunbookOperationalText(
      'Analisar evidências da etapa e confirmar estabilidade antes de avançar.',
      localeCandidate
    );
  }
  return localizeRunbookOperationalText('Sem ação imediata.', localeCandidate);
};

const buildTimelineStageOrderByKey = stages =>
  (Array.isArray(stages) ? stages : []).reduce((accumulator, stage, stageIndex) => {
    const stageKey = normalizeText(stage && stage.key);
    if (!stageKey) {
      return accumulator;
    }
    accumulator[stageKey] = Number(stage && stage.order) || stageIndex + 1;
    return accumulator;
  }, {});

const resolveTimelineStageOrder = (stage, stageOrderByKey, explicitStageOrder) => {
  if (Number.isFinite(explicitStageOrder) && explicitStageOrder > 0) {
    return explicitStageOrder;
  }
  const stageKey = normalizeText(stage);
  if (!stageKey) {
    return TIMELINE_UNKNOWN_STAGE_ORDER;
  }
  return Number(stageOrderByKey && stageOrderByKey[stageKey]) || TIMELINE_UNKNOWN_STAGE_ORDER;
};

const normalizeTimelineSortTimestamp = value =>
  normalizeText(value) || TIMELINE_SORT_FALLBACK_TIMESTAMP;

const compareTimelineRows = (left, right) => {
  if (left.stageOrder !== right.stageOrder) {
    return left.stageOrder - right.stageOrder;
  }

  const leftStage = normalizeText(left.stage);
  const rightStage = normalizeText(right.stage);
  if (leftStage !== rightStage) {
    return leftStage.localeCompare(rightStage);
  }

  const leftTimestamp = normalizeTimelineSortTimestamp(left.timestamp);
  const rightTimestamp = normalizeTimelineSortTimestamp(right.timestamp);
  if (leftTimestamp !== rightTimestamp) {
    return leftTimestamp.localeCompare(rightTimestamp);
  }

  const leftId = normalizeText(left.id);
  const rightId = normalizeText(right.id);
  if (leftId !== rightId) {
    return leftId.localeCompare(rightId);
  }

  const leftCode = normalizeText(left.code);
  const rightCode = normalizeText(right.code);
  if (leftCode !== rightCode) {
    return leftCode.localeCompare(rightCode);
  }

  return (Number(left.originalIndex) || 0) - (Number(right.originalIndex) || 0);
};

const buildTimelineRows = ({
  events = [],
  stageOrderByKey = {},
  defaultRunId = '',
  defaultChangeId = '',
  localeCandidate,
}) =>
  (Array.isArray(events) ? events : [])
    .map((event, eventIndex) => {
      const level = normalizeTimelineLevel(event && event.level);
      const stage = normalizeText(event && event.stage);
      const checkpoint = normalizeText(event && event.checkpoint);
      const hostRef = normalizeText((event && event.hostRef) || (event && event.host_ref));
      const eventMessage =
        sanitizeSensitiveText(normalizeText(event && event.message)) ||
        localizeRunbookOperationalText('Evento operacional recebido do backend.', localeCandidate);
      const stageOrder = resolveTimelineStageOrder(
        stage,
        stageOrderByKey,
        Number(event && event.stageOrder)
      );

      return {
        key:
          normalizeText(event && event.id) || `evt-row-${String(eventIndex + 1).padStart(4, '0')}`,
        id: normalizeText(event && event.id),
        timestamp: normalizeText((event && event.timestamp) || (event && event.timestamp_utc)),
        level,
        code: normalizeText(event && event.code) || 'runbook_event',
        stage,
        stageOrder,
        checkpoint,
        message: eventMessage,
        runId:
          normalizeText(event && event.runId) ||
          normalizeText(event && event.run_id) ||
          defaultRunId,
        changeId:
          normalizeText(event && event.changeId) ||
          normalizeText(event && event.change_id) ||
          defaultChangeId,
        component: resolveTimelineComponent({
          component: sanitizeSensitiveText(normalizeText(event && event.component)),
          hostRef,
          stage,
          checkpoint,
        }),
        cause: resolveTimelineCause({
          cause: sanitizeSensitiveText(
            normalizeText(event && (event.cause || event.primary_cause))
          ),
          eventMessage,
          level,
          localeCandidate,
        }),
        impact: resolveTimelineImpact({
          impact: sanitizeSensitiveText(normalizeText(event && event.impact)),
          level,
          stage,
          localeCandidate,
        }),
        recommendedAction: resolveTimelineRecommendedAction({
          recommendedAction: sanitizeSensitiveText(
            normalizeText(event && event.recommendedAction) ||
              normalizeText(event && event.recommended_action)
          ),
          level,
          localeCandidate,
        }),
        classification: normalizeTimelineClassification(
          normalizeText(event && event.classification),
          level
        ),
        path: sanitizeSensitiveText(normalizeText(event && event.path)),
        backendState: normalizeText(event && event.backendState),
        originalIndex: eventIndex,
      };
    })
    .sort(compareTimelineRows)
    .map(({ originalIndex, ...row }) => row);

const normalizeBackendState = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (
    normalized === RUNBOOK_BACKEND_STATES.ready ||
    normalized === RUNBOOK_BACKEND_STATES.pending ||
    normalized === RUNBOOK_BACKEND_STATES.invalid
  ) {
    return normalized;
  }
  return '';
};

const resolveBackendStateByContextSource = ({ backendState, contextSource } = {}) => {
  const normalizedBackendState = normalizeBackendState(backendState);
  if (normalizedBackendState) {
    return normalizedBackendState;
  }
  return normalizeText(contextSource).toLowerCase() === OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH
    ? RUNBOOK_BACKEND_STATES.ready
    : RUNBOOK_BACKEND_STATES.pending;
};

const resolveSourceBlueprintFingerprint = (...candidates) =>
  candidates.map(candidate => normalizeText(candidate)).find(Boolean) || '';

const resolveManifestFingerprint = ({ manifestFingerprint, sourceBlueprintFingerprint } = {}) =>
  resolveSourceBlueprintFingerprint(manifestFingerprint, sourceBlueprintFingerprint);

const normalizeDecision = value =>
  normalizeText(value).toLowerCase() === 'allow' ? 'allow' : 'block';

const normalizeBoolean = value => {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value === 1;
  }
  const normalized = normalizeText(value).toLowerCase();
  if (!normalized) {
    return false;
  }
  return ['1', 'true', 'yes', 'y', 'sim', 's'].includes(normalized);
};

const normalizeExecutionMode = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === 'reused' || normalized === 'reexecuted') {
    return normalized;
  }
  return 'fresh';
};

const normalizeResumeStrategy = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (
    normalized === 'same_run_checkpoint_reuse' ||
    normalized === 'new_run_diff' ||
    normalized === 'new_run_full'
  ) {
    return normalized;
  }
  return 'fresh_run';
};

const isObjectRecord = value => Boolean(value) && typeof value === 'object';

const normalizeExecutionSemantics = (value, fallback = {}) => {
  const safeSemantics = isObjectRecord(value) ? value : {};
  const safeFallback = isObjectRecord(fallback) ? fallback : {};
  let mode = normalizeExecutionMode(
    safeSemantics.mode ||
      safeSemantics.execution_mode ||
      safeSemantics.executionMode ||
      safeFallback.mode
  );
  let reused = normalizeBoolean(
    safeSemantics.reused || safeSemantics.is_reused || safeSemantics.isReused || safeFallback.reused
  );
  let reexecuted = normalizeBoolean(
    safeSemantics.reexecuted ||
      safeSemantics.is_reexecuted ||
      safeSemantics.isReexecuted ||
      safeFallback.reexecuted
  );
  const resumeStrategy = normalizeResumeStrategy(
    safeSemantics.resumeStrategy ||
      safeSemantics.resume_strategy ||
      safeFallback.resumeStrategy ||
      safeFallback.resume_strategy
  );
  let diffOnly = normalizeBoolean(
    safeSemantics.diffOnly ||
      safeSemantics.diff_only ||
      safeSemantics.apply_diff_only ||
      safeFallback.diffOnly ||
      safeFallback.diff_only
  );

  if (resumeStrategy === 'new_run_diff') {
    diffOnly = true;
  }
  if (mode === 'reexecuted') {
    reexecuted = true;
    reused = true;
  }
  if (reexecuted) {
    mode = 'reexecuted';
    reused = true;
  } else if (reused && mode === 'fresh') {
    mode = 'reused';
  }

  return {
    mode,
    reused,
    reexecuted,
    resumeStrategy,
    operationType: normalizeText(
      safeSemantics.operationType ||
        safeSemantics.operation_type ||
        safeFallback.operationType ||
        safeFallback.operation_type ||
        'start'
    ),
    diffOnly,
    sourceRunId: normalizeText(
      safeSemantics.sourceRunId ||
        safeSemantics.source_run_id ||
        safeFallback.sourceRunId ||
        safeFallback.source_run_id
    ),
    targetRunId: normalizeText(
      safeSemantics.targetRunId ||
        safeSemantics.target_run_id ||
        safeFallback.targetRunId ||
        safeFallback.target_run_id
    ),
    sourceChangeId: normalizeText(
      safeSemantics.sourceChangeId ||
        safeSemantics.source_change_id ||
        safeFallback.sourceChangeId ||
        safeFallback.source_change_id
    ),
    targetChangeId: normalizeText(
      safeSemantics.targetChangeId ||
        safeSemantics.target_change_id ||
        safeFallback.targetChangeId ||
        safeFallback.target_change_id
    ),
    reusedCheckpointKey: normalizeText(
      safeSemantics.reusedCheckpointKey ||
        safeSemantics.reused_checkpoint_key ||
        safeSemantics.resumeCheckpointKey ||
        safeSemantics.resume_checkpoint_key ||
        safeFallback.reusedCheckpointKey ||
        safeFallback.reused_checkpoint_key
    ),
    reusedCheckpointOrder: Number.isFinite(
      Number(
        safeSemantics.reusedCheckpointOrder ||
          safeSemantics.reused_checkpoint_order ||
          safeSemantics.resumeCheckpointOrder ||
          safeSemantics.resume_checkpoint_order ||
          safeFallback.reusedCheckpointOrder ||
          safeFallback.reused_checkpoint_order
      )
    )
      ? Math.max(
          0,
          Math.floor(
            Number(
              safeSemantics.reusedCheckpointOrder ||
                safeSemantics.reused_checkpoint_order ||
                safeSemantics.resumeCheckpointOrder ||
                safeSemantics.resume_checkpoint_order ||
                safeFallback.reusedCheckpointOrder ||
                safeFallback.reused_checkpoint_order
            )
          )
        )
      : 0,
  };
};

const normalizeScopeLock = value => {
  const safeLock = isObjectRecord(value) ? value : {};
  const normalizedStatus = normalizeText(
    safeLock.status || safeLock.lock_status || safeLock.lockStatus
  ).toLowerCase();
  let explicitActive;
  if (safeLock.active !== undefined) {
    explicitActive = safeLock.active;
  } else if (safeLock.isActive !== undefined) {
    explicitActive = safeLock.isActive;
  } else if (safeLock.is_active !== undefined) {
    explicitActive = safeLock.is_active;
  } else if (safeLock.locked !== undefined) {
    explicitActive = safeLock.locked;
  } else {
    explicitActive = safeLock.is_locked;
  }
  const activeByStatus =
    normalizedStatus === 'active' ||
    normalizedStatus === 'locked' ||
    normalizedStatus === 'blocking';
  return {
    active:
      explicitActive !== undefined && explicitActive !== null
        ? normalizeBoolean(explicitActive)
        : activeByStatus,
    scopeKey: normalizeText(
      safeLock.scopeKey ||
        safeLock.scope_key ||
        safeLock.resourceKey ||
        safeLock.resource_key ||
        safeLock.scope ||
        safeLock.key
    ),
    stage: normalizeText(safeLock.stage),
    checkpoint: normalizeText(safeLock.checkpoint),
    acquiredAt: normalizeText(
      safeLock.acquiredAt ||
        safeLock.acquired_at_utc ||
        safeLock.acquiredAtUtc ||
        safeLock.acquired_at
    ),
    expiresAt: normalizeText(
      safeLock.expiresAt || safeLock.expires_at_utc || safeLock.expiresAtUtc || safeLock.expires_at
    ),
    ownerRunId: normalizeText(
      safeLock.ownerRunId || safeLock.owner_run_id || safeLock.runId || safeLock.run_id
    ),
    ownerChangeId: normalizeText(
      safeLock.ownerChangeId || safeLock.owner_change_id || safeLock.changeId || safeLock.change_id
    ),
    operationType: normalizeText(
      safeLock.operationType || safeLock.operation_type || safeLock.action
    ),
    reasonCode: normalizeText(
      safeLock.reasonCode || safeLock.reason_code || safeLock.code
    ).toLowerCase(),
    reasonMessage: normalizeText(
      safeLock.reasonMessage || safeLock.reason_message || safeLock.message
    ),
  };
};

const normalizeArtifactToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/\\/g, '/')
    .replace(/\.json$/g, '');

const normalizeArtifactCatalog = artifacts =>
  (Array.isArray(artifacts) ? artifacts : [])
    .map(item => {
      if (typeof item === 'string') {
        return item;
      }
      if (item && typeof item === 'object') {
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

const resolveOfficialArtifactsContext = ({
  requiredArtifacts = [],
  availableArtifacts = [],
  artifactsReady = null,
  contextSource = '',
} = {}) => {
  const normalizedRequiredArtifacts = normalizeArtifactCatalog(requiredArtifacts);
  const effectiveRequiredArtifacts =
    normalizedRequiredArtifacts.length > 0
      ? normalizedRequiredArtifacts
      : normalizeArtifactCatalog(RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS);
  const normalizedAvailableArtifacts = normalizeArtifactCatalog(availableArtifacts);
  const explicitArtifactsReady = typeof artifactsReady === 'boolean' ? artifactsReady : null;
  const defaultArtifactsReady =
    explicitArtifactsReady === null &&
    normalizeText(contextSource).toLowerCase() === OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH &&
    normalizedRequiredArtifacts.length === 0 &&
    normalizedAvailableArtifacts.length === 0;
  const effectiveArtifactsReady =
    explicitArtifactsReady !== null ? explicitArtifactsReady : defaultArtifactsReady;

  let effectiveAvailableArtifacts = normalizedAvailableArtifacts;
  if (effectiveAvailableArtifacts.length === 0 && effectiveArtifactsReady) {
    effectiveAvailableArtifacts = [...effectiveRequiredArtifacts];
  }
  const missingArtifacts = effectiveArtifactsReady
    ? []
    : resolveMissingRequiredArtifacts(effectiveRequiredArtifacts, effectiveAvailableArtifacts);
  const artifactsReadyResolved =
    explicitArtifactsReady !== null
      ? Boolean(explicitArtifactsReady)
      : defaultArtifactsReady ||
        (missingArtifacts.length === 0 && effectiveAvailableArtifacts.length > 0);

  return {
    requiredArtifacts: effectiveRequiredArtifacts,
    availableArtifacts: effectiveAvailableArtifacts,
    missingArtifacts,
    artifactsReady: artifactsReadyResolved,
  };
};

const buildRunbookAssistantSteps = localeCandidate => [
  {
    key: 'scope',
    title: pickCognusText('Escopo e backend', 'Scope and backend', localeCandidate),
  },
  {
    key: 'preconditions',
    title: pickCognusText('Pré-condições de início', 'Startup preconditions', localeCandidate),
  },
  {
    key: 'operations',
    title: pickCognusText(
      'Operação, observabilidade e retomada',
      'Operation, observability, and resume',
      localeCandidate
    ),
  },
];

const RUNBOOK_MONITORING_STEP_KEYS = new Set(['operations']);
const { Panel: CollapsePanel } = Collapse;
const RUNBOOK_AUDIT_HISTORY_STORAGE_KEY = 'cognus.provisioning.runbook.audit.history.v2';
const RUNBOOK_AUDIT_HISTORY_MAX_ENTRIES = 15;
const RUNBOOK_AUDIT_SELECTED_STORAGE_KEY = 'cognus.provisioning.runbook.audit.selected.v1';
const INFRA_ONBOARDING_STORAGE_KEY = 'cognus.provisioning.infra.onboarding.v2';
const INFRA_ONBOARDING_STORAGE_KEY_LEGACY = 'cognus.provisioning.infra.onboarding.v1';

const getBrowserLocalStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.localStorage || null;
  } catch (error) {
    return null;
  }
};

const getBrowserSessionStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.sessionStorage || null;
  } catch (error) {
    return null;
  }
};

const tryParseJsonObject = value => {
  if (typeof value !== 'string') {
    return null;
  }

  const normalizedValue = value.trim();
  if (!normalizedValue.startsWith('{') || !normalizedValue.endsWith('}')) {
    return null;
  }

  try {
    const parsed = JSON.parse(normalizedValue);
    return isObjectRecord(parsed) ? parsed : null;
  } catch (parseError) {
    return null;
  }
};

const isRunbookFailEnvelope = payload =>
  isObjectRecord(payload) &&
  (String(payload.status || '').toLowerCase() === 'fail' ||
    (String(payload.msg || '').trim() &&
      isObjectRecord(payload.data) &&
      String(payload.data.message || '').trim()));

const extractRunbookFailureEnvelope = error => {
  const queue = [error];
  const visited = new Set();

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      // ignore empty nodes
    } else if (typeof current === 'string') {
      const parsedCurrent = tryParseJsonObject(current);
      if (parsedCurrent && !visited.has(parsedCurrent)) {
        queue.push(parsedCurrent);
      }
    } else if (isObjectRecord(current) && !visited.has(current)) {
      visited.add(current);

      if (isRunbookFailEnvelope(current)) {
        return current;
      }

      Object.values(current).forEach(value => {
        if (isObjectRecord(value) || typeof value === 'string') {
          queue.push(value);
        }
      });
    }
  }

  return null;
};

const resolveRequestErrorMessage = error => {
  if (!error) {
    return pickCognusText('falha de conectividade.', 'connectivity failure.');
  }

  const runbookFailureEnvelope = extractRunbookFailureEnvelope(error);
  if (runbookFailureEnvelope) {
    const envelopeMessage =
      String((runbookFailureEnvelope.data && runbookFailureEnvelope.data.message) || '').trim() ||
      String(runbookFailureEnvelope.message || '').trim() ||
      String(runbookFailureEnvelope.msg || '').trim();

    if (envelopeMessage) {
      return envelopeMessage;
    }
  }

  const backendError = error.data ? error.data.detail || error.data.msg || error.data.message : '';
  if (Array.isArray(backendError)) {
    return backendError.join(' ');
  }
  if (typeof backendError === 'string' && backendError.trim()) {
    return backendError;
  }
  if (backendError && typeof backendError === 'object') {
    return JSON.stringify(backendError);
  }

  return typeof error.message === 'string' && error.message.trim()
    ? error.message
    : pickCognusText('falha de conectividade.', 'connectivity failure.');
};

const shouldMarkBackendInvalid = error => {
  if (extractRunbookFailureEnvelope(error)) {
    return false;
  }

  const statusCode = error && error.response ? error.response.status : null;
  if (!statusCode) {
    return true;
  }
  return statusCode >= 500;
};

const resolveBackendFailureCode = error => {
  const runbookFailureEnvelope = extractRunbookFailureEnvelope(error);
  if (runbookFailureEnvelope) {
    const envelopeCode =
      String((runbookFailureEnvelope.data && runbookFailureEnvelope.data.code) || '').trim() ||
      String(runbookFailureEnvelope.msg || '').trim() ||
      String(runbookFailureEnvelope.code || '').trim();

    if (envelopeCode) {
      return envelopeCode;
    }
  }

  const codeFromData =
    error &&
    error.data &&
    (String(error.data.code || '').trim() || String(error.data.msg || '').trim());

  if (codeFromData) {
    return codeFromData;
  }

  return '';
};

const resolveBackendFailureDetails = error => {
  const runbookFailureEnvelope = extractRunbookFailureEnvelope(error);
  if (
    runbookFailureEnvelope &&
    runbookFailureEnvelope.data &&
    typeof runbookFailureEnvelope.data.details === 'object'
  ) {
    return runbookFailureEnvelope.data.details;
  }
  if (error && error.data && typeof error.data.details === 'object') {
    return error.data.details;
  }
  return {};
};

const mapBackendBlueprintVersion = record => {
  const safeRecord = record && typeof record === 'object' ? record : {};
  const normalizedVersion = String(safeRecord.blueprint_version || '').trim();
  if (!normalizedVersion) {
    return null;
  }

  return {
    value: normalizedVersion,
    label: `v${normalizedVersion}`,
    lintValid: Boolean(safeRecord.valid),
    resolvedSchemaVersion: String(safeRecord.resolved_schema_version || '1.0.0'),
    fingerprint: String(safeRecord.fingerprint_sha256 || ''),
    changeId: String(safeRecord.change_id || ''),
  };
};

const renderStatusTag = (status, toneMap) => {
  const normalizedStatus = String(status || '')
    .trim()
    .toLowerCase();
  const tone = toneMap[normalizedStatus] || 'default';
  return <Tag color={statusTagColor[tone] || 'default'}>{normalizedStatus || '-'}</Tag>;
};

const toSafePositiveInt = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizeReadableToken = value =>
  String(value || '')
    .trim()
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ');

const normalizeTopologyHosts = hosts =>
  (Array.isArray(hosts) ? hosts : [])
    .map(host => ({
      hostRef: String(
        (host && (host.host_ref || host.hostRef || host.infra_label || host.infraLabel)) || ''
      ).trim(),
      hostAddress: String((host && (host.host_address || host.hostAddress)) || '').trim(),
      sshUser: String((host && (host.ssh_user || host.sshUser)) || '').trim(),
      sshPort: toSafePositiveInt(host && (host.ssh_port || host.sshPort)) || 22,
      dockerPort: toSafePositiveInt(host && (host.docker_port || host.dockerPort)) || 2376,
    }))
    .filter(host => host.hostRef || host.hostAddress);

const normalizeTopologyOrganizations = organizations =>
  (Array.isArray(organizations) ? organizations : [])
    .map(organization => ({
      orgId: String(
        (organization && (organization.org_id || organization.orgId || organization.org_key)) || ''
      ).trim(),
      orgName: String(
        (organization && (organization.org_name || organization.orgName)) || ''
      ).trim(),
      domain: String((organization && organization.domain) || '').trim(),
      peerHostRef: String(
        (organization && (organization.peer_host_ref || organization.peerHostRef)) || ''
      ).trim(),
      ordererHostRef: String(
        (organization && (organization.orderer_host_ref || organization.ordererHostRef)) || ''
      ).trim(),
      ca: {
        mode: String((organization && organization.ca && organization.ca.mode) || '').trim(),
        name: String((organization && organization.ca && organization.ca.name) || '').trim(),
        host: String((organization && organization.ca && organization.ca.host) || '').trim(),
        port: toSafePositiveInt(organization && organization.ca && organization.ca.port),
        user: String((organization && organization.ca && organization.ca.user) || '').trim(),
        hostRef: String((organization && organization.ca && organization.ca.host_ref) || '').trim(),
      },
      networkApi: {
        host: String(
          (organization && organization.network_api && organization.network_api.host) ||
            (organization && organization.networkApi && organization.networkApi.host) ||
            ''
        ).trim(),
        port: toSafePositiveInt(
          (organization && organization.network_api && organization.network_api.port) ||
            (organization && organization.networkApi && organization.networkApi.port)
        ),
        exposureIp: String(
          (organization && organization.network_api && organization.network_api.exposure_ip) ||
            (organization && organization.networkApi && organization.networkApi.exposureIp) ||
            ''
        ).trim(),
      },
      peers: (Array.isArray(organization && organization.peers) ? organization.peers : [])
        .map(node => ({
          nodeId: String((node && (node.node_id || node.nodeId)) || '').trim(),
          hostRef: String((node && (node.host_ref || node.hostRef)) || '').trim(),
          hostAddress: String((node && (node.host_address || node.hostAddress)) || '').trim(),
        }))
        .filter(node => node.nodeId || node.hostRef || node.hostAddress),
      orderers: (Array.isArray(organization && organization.orderers) ? organization.orderers : [])
        .map(node => ({
          nodeId: String((node && (node.node_id || node.nodeId)) || '').trim(),
          hostRef: String((node && (node.host_ref || node.hostRef)) || '').trim(),
          hostAddress: String((node && (node.host_address || node.hostAddress)) || '').trim(),
        }))
        .filter(node => node.nodeId || node.hostRef || node.hostAddress),
      channels: (Array.isArray(organization && organization.channels) ? organization.channels : [])
        .map(channel => String(channel || '').trim())
        .filter(Boolean),
      chaincodes: (Array.isArray(organization && organization.chaincodes)
        ? organization.chaincodes
        : []
      )
        .map(chaincode => String(chaincode || '').trim())
        .filter(Boolean),
      apis: (Array.isArray(organization && organization.apis) ? organization.apis : []).map(
        api => ({
          apiId: String((api && (api.api_id || api.apiId)) || '').trim(),
          channelId: String((api && (api.channel_id || api.channelId)) || '').trim(),
          chaincodeId: String((api && (api.chaincode_id || api.chaincodeId)) || '').trim(),
          routePath: String((api && (api.route_path || api.routePath)) || '').trim(),
          exposureHost: String(
            (api && api.exposure && (api.exposure.host || api.exposure.exposure_host)) || ''
          ).trim(),
          exposurePort: toSafePositiveInt(api && api.exposure && api.exposure.port),
        })
      ),
    }))
    .filter(organization => organization.orgId || organization.orgName);

const normalizeTopologyBusinessGroups = businessGroups =>
  (Array.isArray(businessGroups) ? businessGroups : [])
    .map(group => ({
      groupId: String(
        (group && (group.group_id || group.groupId || group.network_id)) || ''
      ).trim(),
      name: String((group && group.name) || '').trim(),
      networkId: String((group && (group.network_id || group.networkId)) || '').trim(),
      channels: (Array.isArray(group && group.channels) ? group.channels : [])
        .map(channel => ({
          channelId: String((channel && (channel.channel_id || channel.channelId)) || '').trim(),
          memberOrgs: (Array.isArray(channel && (channel.member_orgs || channel.memberOrgs))
            ? channel.member_orgs || channel.memberOrgs
            : []
          )
            .map(orgName => String(orgName || '').trim())
            .filter(Boolean),
          chaincodes: (Array.isArray(channel && channel.chaincodes) ? channel.chaincodes : [])
            .map(chaincode => String(chaincode || '').trim())
            .filter(Boolean),
        }))
        .filter(channel => channel.channelId),
    }))
    .filter(group => group.groupId || group.name || group.networkId);

const normalizeTopologyCatalog = topology => {
  const safeTopology = topology && typeof topology === 'object' ? topology : {};
  const hosts = normalizeTopologyHosts(safeTopology.hosts);
  const organizations = normalizeTopologyOrganizations(safeTopology.organizations);
  const businessGroups = normalizeTopologyBusinessGroups(safeTopology.business_groups);

  return {
    hosts,
    organizations,
    businessGroups,
  };
};

const normalizeContextHostMapping = hostMapping =>
  (Array.isArray(hostMapping) ? hostMapping : [])
    .map(entry => ({
      org_id: String((entry && (entry.org_id || entry.orgId)) || '').trim(),
      host_ref: String(
        (entry && (entry.host_ref || entry.hostRef || entry.node_id || entry.nodeId)) || ''
      ).trim(),
      host_address: String((entry && (entry.host_address || entry.hostAddress)) || '').trim(),
      ssh_user: String((entry && (entry.ssh_user || entry.sshUser)) || '').trim(),
      ssh_port: toSafePositiveInt(entry && (entry.ssh_port || entry.sshPort)) || 22,
      docker_port: toSafePositiveInt(entry && (entry.docker_port || entry.dockerPort)) || 2376,
      node_type: String((entry && (entry.node_type || entry.nodeType)) || '')
        .trim()
        .toLowerCase(),
    }))
    .filter(entry => entry.host_ref || entry.host_address);

const normalizeContextMachineCredentials = machineCredentials =>
  (Array.isArray(machineCredentials) ? machineCredentials : [])
    .map(entry => ({
      machine_id: String((entry && entry.machine_id) || '').trim(),
      credential_ref: String((entry && entry.credential_ref) || '').trim(),
      credential_payload: String((entry && entry.credential_payload) || '').trim(),
      credential_fingerprint: String((entry && entry.credential_fingerprint) || '').trim(),
      reuse_confirmed: Boolean(entry && entry.reuse_confirmed),
    }))
    .filter(entry => entry.machine_id && (entry.credential_ref || entry.credential_fingerprint));

const buildProvisioningOrganizationsSummary = ({ handoff }) => {
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const hostMapping = Array.isArray(safeHandoff.host_mapping) ? safeHandoff.host_mapping : [];
  const apiRegistry = Array.isArray(safeHandoff.api_registry) ? safeHandoff.api_registry : [];
  const orgRegistry = new Map();

  const ensureOrg = rawOrgId => {
    const normalizedOrgId = String(rawOrgId || '').trim();
    if (!normalizedOrgId) {
      return null;
    }
    const orgKey = normalizedOrgId.toLowerCase();
    if (!orgRegistry.has(orgKey)) {
      orgRegistry.set(orgKey, {
        orgId: normalizedOrgId,
        orgName: normalizeReadableToken(normalizedOrgId) || normalizedOrgId,
        nodeCount: 0,
        peerCount: 0,
        ordererCount: 0,
        caCount: 0,
        apiCount: 0,
        channels: new Set(),
        chaincodes: new Set(),
      });
    }
    return orgRegistry.get(orgKey);
  };

  hostMapping.forEach(entry => {
    const orgSummary = ensureOrg(entry && entry.org_id);
    if (!orgSummary) {
      return;
    }
    orgSummary.nodeCount += 1;
    const nodeType = String(entry && entry.node_type ? entry.node_type : '')
      .trim()
      .toLowerCase();
    if (nodeType === 'peer') {
      orgSummary.peerCount += 1;
    } else if (nodeType === 'orderer') {
      orgSummary.ordererCount += 1;
    } else if (nodeType === 'ca') {
      orgSummary.caCount += 1;
    }
  });

  apiRegistry.forEach(entry => {
    const orgSummary = ensureOrg((entry && entry.org_name) || (entry && entry.org_id));
    if (!orgSummary) {
      return;
    }
    orgSummary.apiCount += 1;
    const channelId = String(entry && entry.channel_id ? entry.channel_id : '').trim();
    if (channelId) {
      orgSummary.channels.add(channelId);
    }
    const chaincodeId = String(entry && entry.chaincode_id ? entry.chaincode_id : '').trim();
    if (chaincodeId) {
      orgSummary.chaincodes.add(chaincodeId);
    }
  });

  return Array.from(orgRegistry.values())
    .map(orgSummary => ({
      orgId: orgSummary.orgId,
      orgName: orgSummary.orgName,
      nodeCount: toSafePositiveInt(orgSummary.nodeCount),
      peerCount: toSafePositiveInt(orgSummary.peerCount),
      ordererCount: toSafePositiveInt(orgSummary.ordererCount),
      caCount: toSafePositiveInt(orgSummary.caCount),
      apiCount: toSafePositiveInt(orgSummary.apiCount),
      channels: Array.from(orgSummary.channels).sort(),
      chaincodes: Array.from(orgSummary.chaincodes).sort(),
    }))
    .sort((left, right) => left.orgName.localeCompare(right.orgName));
};

const buildProvisioningContextSummary = ({ snapshot, handoff }) => {
  const safeSnapshot = snapshot && typeof snapshot === 'object' ? snapshot : {};
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const hostMapping = Array.isArray(safeHandoff.host_mapping) ? safeHandoff.host_mapping : [];
  const organizations = buildProvisioningOrganizationsSummary({ handoff: safeHandoff });
  const topology = normalizeTopologyCatalog(safeHandoff.topology_catalog);
  const uniqueHosts = new Set(
    hostMapping
      .map(entry => String(entry && entry.host_ref ? entry.host_ref : '').trim())
      .filter(Boolean)
  );
  const uniqueOrganizations = new Set(
    organizations.map(entry => String(entry.orgId || '').trim()).filter(Boolean)
  );

  return {
    providerKey: String(safeSnapshot.providerKey || RUNBOOK_PROVIDER_KEY).trim(),
    environmentProfile: String(
      safeSnapshot.environmentProfile || safeHandoff.environment_profile || ''
    ).trim(),
    blueprintVersion: String(
      safeSnapshot.blueprintVersion || safeHandoff.blueprint_version || ''
    ).trim(),
    hostCount: toSafePositiveInt(uniqueHosts.size),
    organizationCount: toSafePositiveInt(uniqueOrganizations.size),
    nodeCount: toSafePositiveInt(hostMapping.length),
    apiCount: toSafePositiveInt(
      Array.isArray(safeHandoff.api_registry) ? safeHandoff.api_registry.length : 0
    ),
    incrementalCount: toSafePositiveInt(
      Array.isArray(safeHandoff.incremental_expansions)
        ? safeHandoff.incremental_expansions.length
        : 0
    ),
    organizations,
    topology,
    host_mapping: normalizeContextHostMapping(hostMapping),
    machine_credentials: normalizeContextMachineCredentials(safeHandoff.machine_credentials),
    handoff_fingerprint: String(safeHandoff.handoff_fingerprint || '').trim(),
  };
};

const mergeProvisioningContextSummary = (existingContext, incomingContext) => {
  const safeExistingContext =
    existingContext && typeof existingContext === 'object' ? existingContext : {};
  const safeIncomingContext =
    incomingContext && typeof incomingContext === 'object' ? incomingContext : {};

  const existingOrganizations = Array.isArray(safeExistingContext.organizations)
    ? safeExistingContext.organizations
    : [];
  const incomingOrganizations = Array.isArray(safeIncomingContext.organizations)
    ? safeIncomingContext.organizations
    : [];
  const existingTopology = normalizeTopologyCatalog(safeExistingContext.topology);
  const incomingTopology = normalizeTopologyCatalog(safeIncomingContext.topology);
  const mergedTopology =
    incomingTopology.organizations.length > 0 ||
    incomingTopology.hosts.length > 0 ||
    incomingTopology.businessGroups.length > 0
      ? incomingTopology
      : existingTopology;

  const mergedOrganizations =
    incomingOrganizations.length >= existingOrganizations.length
      ? incomingOrganizations
      : existingOrganizations;

  return {
    providerKey: String(
      safeIncomingContext.providerKey || safeExistingContext.providerKey || ''
    ).trim(),
    environmentProfile: String(
      safeIncomingContext.environmentProfile || safeExistingContext.environmentProfile || ''
    ).trim(),
    blueprintVersion: String(
      safeIncomingContext.blueprintVersion || safeExistingContext.blueprintVersion || ''
    ).trim(),
    hostCount: Math.max(
      toSafePositiveInt(safeExistingContext.hostCount),
      toSafePositiveInt(safeIncomingContext.hostCount)
    ),
    organizationCount: Math.max(
      toSafePositiveInt(safeExistingContext.organizationCount),
      toSafePositiveInt(safeIncomingContext.organizationCount)
    ),
    nodeCount: Math.max(
      toSafePositiveInt(safeExistingContext.nodeCount),
      toSafePositiveInt(safeIncomingContext.nodeCount)
    ),
    apiCount: Math.max(
      toSafePositiveInt(safeExistingContext.apiCount),
      toSafePositiveInt(safeIncomingContext.apiCount)
    ),
    incrementalCount: Math.max(
      toSafePositiveInt(safeExistingContext.incrementalCount),
      toSafePositiveInt(safeIncomingContext.incrementalCount)
    ),
    organizations: mergedOrganizations,
    topology: mergedTopology,
    host_mapping:
      Array.isArray(safeIncomingContext.host_mapping) && safeIncomingContext.host_mapping.length > 0
        ? normalizeContextHostMapping(safeIncomingContext.host_mapping)
        : normalizeContextHostMapping(safeExistingContext.host_mapping),
    machine_credentials:
      Array.isArray(safeIncomingContext.machine_credentials) &&
      safeIncomingContext.machine_credentials.length > 0
        ? normalizeContextMachineCredentials(safeIncomingContext.machine_credentials)
        : normalizeContextMachineCredentials(safeExistingContext.machine_credentials),
    handoff_fingerprint: String(
      safeIncomingContext.handoff_fingerprint ||
        safeExistingContext.handoff_fingerprint ||
        ''
    ).trim(),
  };
};

const resolveProvisioningHistoryLevel = snapshot => {
  const status = String(snapshot && snapshot.status ? snapshot.status : '')
    .trim()
    .toLowerCase();
  const hasOfficialDecision = Boolean(
    snapshot &&
      snapshot.officialDecision &&
      typeof snapshot.officialDecision === 'object' &&
      String(snapshot.officialDecision.decision || '').trim()
  );
  const decision = hasOfficialDecision
    ? normalizeDecision(snapshot && snapshot.officialDecision && snapshot.officialDecision.decision)
    : '';
  if (status === RUNBOOK_EXECUTION_STATUS.completed && decision === 'block') {
    return { label: pickCognusText('bloqueado', 'blocked'), color: 'red' };
  }
  if (status === RUNBOOK_EXECUTION_STATUS.completed) {
    return { label: pickCognusText('consolidado', 'consolidated'), color: 'green' };
  }
  if (status === RUNBOOK_EXECUTION_STATUS.failed) {
    return { label: pickCognusText('falha', 'failed'), color: 'red' };
  }
  return { label: pickCognusText('parcial', 'partial'), color: 'orange' };
};

const cloneRunbookExecutionSnapshot = executionState => ({
  ...executionState,
  stages: Array.isArray(executionState && executionState.stages)
    ? executionState.stages.map(stage => ({
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
    : [],
  events: Array.isArray(executionState && executionState.events)
    ? executionState.events.map(event => ({ ...event }))
    : [],
  runbookResumeContext:
    executionState &&
    executionState.runbookResumeContext &&
    typeof executionState.runbookResumeContext === 'object'
      ? { ...executionState.runbookResumeContext }
      : {},
  executionSemantics: normalizeExecutionSemantics(
    executionState && executionState.executionSemantics,
    {
      targetRunId: normalizeText(executionState && executionState.runId),
      targetChangeId: normalizeText(executionState && executionState.changeId),
    }
  ),
  scopeLock: normalizeScopeLock(executionState && executionState.scopeLock),
  officialDecision:
    executionState &&
    executionState.officialDecision &&
    typeof executionState.officialDecision === 'object'
      ? {
          ...executionState.officialDecision,
          decisionReasons: Array.isArray(executionState.officialDecision.decisionReasons)
            ? [...executionState.officialDecision.decisionReasons]
            : [],
          requiredEvidenceKeys: Array.isArray(executionState.officialDecision.requiredEvidenceKeys)
            ? [...executionState.officialDecision.requiredEvidenceKeys]
            : [],
          missingEvidenceKeys: Array.isArray(executionState.officialDecision.missingEvidenceKeys)
            ? [...executionState.officialDecision.missingEvidenceKeys]
            : [],
        }
      : {},
});

const ProvisioningRunbookPage = () => {
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale),
    [locale]
  );
  const runbookAssistantSteps = useMemo(
    () => buildRunbookAssistantSteps(locale),
    [locale]
  );
  const runbookMonitoringStepStartIndex = useMemo(
    () => runbookAssistantSteps.findIndex(step => step.key === 'operations'),
    [runbookAssistantSteps]
  );
  const [changeId, setChangeId] = useState('cr-2026-02-16-003');
  const [providerKey] = useState(RUNBOOK_PROVIDER_KEY);
  const [environmentProfile, setEnvironmentProfile] = useState('dev-external-linux');
  const [blueprintCatalog, setBlueprintCatalog] = useState(fallbackBlueprintVersionCatalog);
  const [selectedBlueprintVersion, setSelectedBlueprintVersion] = useState(
    fallbackBlueprintVersionCatalog[0].value
  );
  const [pipelineEvidenceReady, setPipelineEvidenceReady] = useState(RUNBOOK_LOCAL_MODE_ENABLED);
  const [cryptoBaselineReady, setCryptoBaselineReady] = useState(RUNBOOK_LOCAL_MODE_ENABLED);
  const [preflightApproved, setPreflightApproved] = useState(RUNBOOK_LOCAL_MODE_ENABLED);
  const [plannedRunId, setPlannedRunId] = useState('');
  const [onboardingHandoff, setOnboardingHandoff] = useState(null);
  const [backendState, setBackendState] = useState(RUNBOOK_BACKEND_STATES.pending);
  const [executionState, setExecutionState] = useState(() =>
    createRunbookExecutionState({
      backendState: RUNBOOK_BACKEND_STATES.pending,
    })
  );
  const [isAssistantDialogOpen, setIsAssistantDialogOpen] = useState(false);
  const [assistantStepIndex, setAssistantStepIndex] = useState(0);
  const [scopeStepApproved, setScopeStepApproved] = useState(false);
  const [preconditionsStepApproved, setPreconditionsStepApproved] = useState(false);
  const [isAdvancingCheckpoint, setIsAdvancingCheckpoint] = useState(false);
  const [provisioningHistory, setProvisioningHistory] = useState([]);
  const [showEvidencePanel, setShowEvidencePanel] = useState(true);
  const [showTimelinePanel, setShowTimelinePanel] = useState(true);
  const [expandedLogPanel, setExpandedLogPanel] = useState('');
  const historyHydratedRef = useRef(false);
  const startActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-provisionamento', 'start_runbook'),
    []
  );
  const pauseActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-provisionamento', 'pause_runbook'),
    []
  );
  const resumeActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-provisionamento', 'resume_runbook'),
    []
  );
  const advanceActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-provisionamento', 'advance_checkpoint'),
    []
  );
  const retryActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-provisionamento', 'retry_checkpoint'),
    []
  );

  const buildReadinessTooltip = readiness => {
    if (!readiness.available) {
      return t('Acao bloqueada: {reason}', 'Action blocked: {reason}', {
        reason: readiness.reason,
      });
    }
    return t('Modo: {mode}. {reason}', 'Mode: {mode}. {reason}', {
      mode: readiness.modeLabel,
      reason: readiness.reason,
    });
  };

  const loadBlueprintCatalog = useCallback(async ({ silent = false } = {}) => {
    try {
      const response = await listBlueprintVersions();
      const rows =
        response &&
        response.status === 'successful' &&
        response.data &&
        Array.isArray(response.data.data)
          ? response.data.data
          : null;

      if (!rows) {
        throw new Error('Resposta de versoes de blueprint fora do contrato esperado.');
      }

      const mappedRows = rows
        .map(mapBackendBlueprintVersion)
        .filter(Boolean)
        .filter(
          (currentRow, currentIndex, allRows) =>
            allRows.findIndex(row => row.value === currentRow.value) === currentIndex
        );

      if (mappedRows.length > 0) {
        setBlueprintCatalog(mappedRows);
        setSelectedBlueprintVersion(currentValue => {
          const hasCurrentValue = mappedRows.some(row => row.value === currentValue);
          return hasCurrentValue ? currentValue : mappedRows[0].value;
        });
        return;
      }

      if (IS_PROVISIONING_OPERATIONAL_ENV) {
        throw new Error(
          'Nenhuma versão oficial de blueprint disponível. Fallback local bloqueado em hml/prod.'
        );
      }

      setBlueprintCatalog(fallbackBlueprintVersionCatalog);
      setSelectedBlueprintVersion(currentValue => {
        const hasCurrentValue = fallbackBlueprintVersionCatalog.some(
          row => row.value === currentValue
        );
        return hasCurrentValue ? currentValue : fallbackBlueprintVersionCatalog[0].value;
      });
    } catch (error) {
      if (!silent) {
        message.warning(
          t(
            'Nao foi possivel sincronizar versoes publicadas de blueprint: {message}',
            'Could not synchronize published blueprint versions: {message}',
            { message: resolveRequestErrorMessage(error) }
          )
        );
      }

      if (IS_PROVISIONING_OPERATIONAL_ENV) {
        setBlueprintCatalog([]);
        setSelectedBlueprintVersion('');
        return;
      }

      setBlueprintCatalog(fallbackBlueprintVersionCatalog);
      setSelectedBlueprintVersion(currentValue => {
        const hasCurrentValue = fallbackBlueprintVersionCatalog.some(
          row => row.value === currentValue
        );
        return hasCurrentValue ? currentValue : fallbackBlueprintVersionCatalog[0].value;
      });
    }
  }, [t]);

  useEffect(() => {
    loadBlueprintCatalog({ silent: true });
  }, [loadBlueprintCatalog]);

  useEffect(() => {
    const handoff = consumeOnboardingRunbookHandoff();
    if (!handoff) {
      return;
    }

    const handoffContextSource = normalizeText(handoff.context_source || handoff.source);
    const handoffBackendState = resolveBackendStateByContextSource({
      backendState: handoff.official_backend_state,
      contextSource: handoffContextSource,
    });
    const handoffSourceBlueprintFingerprint = resolveSourceBlueprintFingerprint(
      handoff.source_blueprint_fingerprint,
      handoff.blueprint_fingerprint
    );
    const handoffManifestFingerprint = resolveManifestFingerprint({
      manifestFingerprint: handoff.manifest_fingerprint,
      sourceBlueprintFingerprint: handoffSourceBlueprintFingerprint,
    });
    const handoffArtifactsContext = resolveOfficialArtifactsContext({
      requiredArtifacts: handoff.a2_2_minimum_artifacts,
      availableArtifacts: handoff.a2_2_available_artifacts,
      artifactsReady:
        typeof handoff.a2_2_artifacts_ready === 'boolean' ? handoff.a2_2_artifacts_ready : null,
      contextSource: handoffContextSource,
    });

    setOnboardingHandoff(handoff);

    if (handoff.change_id) {
      setChangeId(String(handoff.change_id));
    }
    if (handoff.environment_profile) {
      setEnvironmentProfile(String(handoff.environment_profile));
    }

    const handoffArtifactsReady = handoffArtifactsContext.artifactsReady;
    setPipelineEvidenceReady(Boolean(handoffArtifactsReady));
    setCryptoBaselineReady(Boolean(handoffArtifactsReady));
    setPreflightApproved(Boolean(handoff.preflight_approved));
    setPlannedRunId(String(handoff.run_id || ''));

    const handoffCatalogEntry = buildRunbookBlueprintCatalogEntry(handoff);
    if (handoffCatalogEntry) {
      setBlueprintCatalog(currentRows => {
        const filteredRows = currentRows.filter(row => row.value !== handoffCatalogEntry.value);
        return [handoffCatalogEntry, ...filteredRows];
      });
      setSelectedBlueprintVersion(handoffCatalogEntry.value);
    }

    setExecutionState(currentState => ({
      ...createRunbookExecutionState({
        backendState: handoffBackendState,
      }),
      changeId: String(handoff.change_id || currentState.changeId || ''),
      providerKey: RUNBOOK_PROVIDER_KEY,
      environmentProfile: String(
        handoff.environment_profile || currentState.environmentProfile || ''
      ),
      blueprintVersion: String(handoff.blueprint_version || currentState.blueprintVersion || ''),
      blueprintFingerprint: String(handoff.blueprint_fingerprint || ''),
      manifestFingerprint: handoffManifestFingerprint,
      sourceBlueprintFingerprint: handoffSourceBlueprintFingerprint,
      resolvedSchemaVersion: String(handoff.resolved_schema_version || '1.0.0'),
      requiredArtifacts: handoffArtifactsContext.requiredArtifacts,
      availableArtifacts: handoffArtifactsContext.availableArtifacts,
      status: RUNBOOK_EXECUTION_STATUS.idle,
      runId: '',
      startedAt: '',
      finishedAt: '',
      events: [],
    }));
    setBackendState(handoffBackendState);
    setScopeStepApproved(false);
    setPreconditionsStepApproved(false);
    setAssistantStepIndex(0);
    setShowEvidencePanel(true);
    setShowTimelinePanel(true);
    setExpandedLogPanel('');
  }, []);

  const selectedBlueprint = useMemo(
    () => blueprintCatalog.find(blueprint => blueprint.value === selectedBlueprintVersion) || null,
    [blueprintCatalog, selectedBlueprintVersion]
  );

  const officialEntryGateContext = useMemo(() => {
    const safeHandoff = isObjectRecord(onboardingHandoff) ? onboardingHandoff : {};
    const contextSource = normalizeText(safeHandoff.context_source || safeHandoff.source);
    const sourceBlueprintFingerprint = resolveSourceBlueprintFingerprint(
      safeHandoff.source_blueprint_fingerprint,
      safeHandoff.blueprint_fingerprint
    );
    const manifestFingerprint = resolveManifestFingerprint({
      manifestFingerprint: safeHandoff.manifest_fingerprint,
      sourceBlueprintFingerprint,
    });
    const artifactsContext = resolveOfficialArtifactsContext({
      requiredArtifacts: safeHandoff.a2_2_minimum_artifacts,
      availableArtifacts: safeHandoff.a2_2_available_artifacts,
      artifactsReady:
        typeof safeHandoff.a2_2_artifacts_ready === 'boolean'
          ? safeHandoff.a2_2_artifacts_ready
          : null,
      contextSource,
    });

    return {
      changeId: normalizeText(safeHandoff.change_id),
      runId: normalizeText(safeHandoff.run_id),
      manifestFingerprint,
      sourceBlueprintFingerprint,
      handoffFingerprint: normalizeText(safeHandoff.handoff_fingerprint),
      handoffContractVersion: normalizeText(safeHandoff.handoff_contract_version),
      requiredArtifacts: artifactsContext.requiredArtifacts,
      availableArtifacts: artifactsContext.availableArtifacts,
      missingArtifacts: artifactsContext.missingArtifacts,
      artifactsReady: artifactsContext.artifactsReady,
      contextSource,
      backendState: resolveBackendStateByContextSource({
        backendState: safeHandoff.official_backend_state,
        contextSource,
      }),
    };
  }, [onboardingHandoff]);

  const officialContextValues = useMemo(
    () => [
      officialEntryGateContext.changeId,
      officialEntryGateContext.runId,
      officialEntryGateContext.manifestFingerprint,
      officialEntryGateContext.sourceBlueprintFingerprint,
    ],
    [officialEntryGateContext]
  );

  const officialContextFieldsFilled = useMemo(() => officialContextValues.filter(Boolean).length, [
    officialContextValues,
  ]);

  const officialContextComplete = officialContextFieldsFilled === officialContextValues.length;
  const pipelinePreconditionsReady = officialEntryGateContext.artifactsReady;
  const blueprintValidated = Boolean(selectedBlueprint && selectedBlueprint.lintValid);
  const backendCommandReady = isBackendCommandReady(backendState);

  useEffect(() => {
    if (executionState.runId || officialEntryGateContext.runId) {
      return;
    }

    const derivedRunId = deterministicRunId({
      changeId,
      blueprintFingerprint: selectedBlueprint ? selectedBlueprint.fingerprint : '',
      resolvedSchemaVersion: selectedBlueprint ? selectedBlueprint.resolvedSchemaVersion : '1.0.0',
    });
    setPlannedRunId(derivedRunId);
  }, [changeId, executionState.runId, officialEntryGateContext.runId, selectedBlueprint]);

  const startPreconditions = useMemo(
    () =>
      evaluateRunbookStartPreconditions({
        providerKey,
        backendState,
        blueprintVersion: selectedBlueprintVersion,
        blueprintValidated,
        pipelinePreconditionsReady,
        preflightApproved,
        changeId: officialEntryGateContext.changeId || changeId,
        runId: officialEntryGateContext.runId,
        manifestFingerprint: officialEntryGateContext.manifestFingerprint,
        sourceBlueprintFingerprint: officialEntryGateContext.sourceBlueprintFingerprint,
        requiredArtifacts: officialEntryGateContext.requiredArtifacts,
        availableArtifacts: officialEntryGateContext.availableArtifacts,
        requireOfficialExecutionContext: true,
      }),
    [
      providerKey,
      backendState,
      selectedBlueprintVersion,
      blueprintValidated,
      pipelinePreconditionsReady,
      preflightApproved,
      changeId,
      officialEntryGateContext,
    ]
  );

  const a25GateReadinessStatus = useMemo(() => {
    if (officialContextComplete && officialEntryGateContext.artifactsReady && backendCommandReady) {
      return READINESS_STATUS.implemented;
    }
    if (officialContextFieldsFilled > 0 || officialEntryGateContext.availableArtifacts.length > 0) {
      return READINESS_STATUS.partial;
    }
    return READINESS_STATUS.pending;
  }, [
    officialContextComplete,
    officialEntryGateContext.artifactsReady,
    officialEntryGateContext.availableArtifacts.length,
    officialContextFieldsFilled,
    backendCommandReady,
  ]);

  const a25GateReasons = useMemo(
    () =>
      startPreconditions.reasons
        .filter(reason => A2_5_GATE_REASON_CODE_SET.has(String(reason.code || '').toLowerCase()))
        .map(reason => reason.message),
    [startPreconditions]
  );

  const executionStatus = executionState.status;
  const stageRows = useMemo(() => buildRunbookStageRows(executionState), [executionState]);
  const runbookSnapshot = useMemo(
    () => buildRunbookOfficialSnapshot(executionState, { backendState }),
    [executionState, backendState]
  );
  const executionSemantics = useMemo(
    () =>
      normalizeExecutionSemantics(runbookSnapshot.executionSemantics, {
        targetRunId: executionState.runId || plannedRunId || officialEntryGateContext.runId,
        targetChangeId: executionState.changeId || changeId,
      }),
    [
      changeId,
      executionState.changeId,
      executionState.runId,
      officialEntryGateContext.runId,
      plannedRunId,
      runbookSnapshot.executionSemantics,
    ]
  );
  const executionMode = normalizeExecutionMode(executionSemantics.mode);
  const executionModeLabel = resolveLocalizedRunbookLabel(
    executionModeLabelMap[executionMode] || executionModeLabelMap.fresh,
    locale
  );
  const resumeStrategyLabel = resolveLocalizedRunbookLabel(
    resumeStrategyLabelMap[executionSemantics.resumeStrategy] || resumeStrategyLabelMap.fresh_run,
    locale
  );
  const scopeLock = useMemo(
    () => normalizeScopeLock(runbookSnapshot.scopeLock || executionState.scopeLock),
    [executionState.scopeLock, runbookSnapshot.scopeLock]
  );
  const scopeLockActive = Boolean(scopeLock.active);
  const scopeLockSummary = scopeLockActive
    ? t('lock ativo em {scope} ({reason})', 'active lock in {scope} ({reason})', {
        scope: scopeLock.scopeKey || t('escopo operacional', 'operational scope'),
        reason: scopeLock.reasonCode || 'runbook_resource_lock_conflict',
      })
    : t('sem lock ativo no escopo operacional', 'no active lock in the operational scope');
  const officialDecision = runbookSnapshot.officialDecision || {};
  const officialDecisionValue = normalizeDecision(officialDecision.decision);
  const officialDecisionCode = String(officialDecision.decisionCode || '').trim();
  const officialDecisionReasons = Array.isArray(officialDecision.decisionReasons)
    ? officialDecision.decisionReasons
    : [];
  let officialDecisionSummary = t(
    'Decisão oficial bloqueada até reconciliação técnica.',
    'Official decision blocked until technical reconciliation.'
  );
  if (officialDecisionReasons.length > 0) {
    officialDecisionSummary = officialDecisionReasons.join(', ');
  } else if (officialDecisionValue === 'allow') {
    officialDecisionSummary = t(
      'Decisão oficial liberada para continuidade operacional.',
      'Official decision cleared for operational continuity.'
    );
  }
  const producedEvidenceRows = useMemo(() => buildRunbookProducedEvidenceRows(executionState), [
    executionState,
  ]);
  const resumeContext = useMemo(() => buildRunbookResumeContext(executionState), [executionState]);
  const latestFailure = useMemo(() => getLatestRunbookFailure(executionState), [executionState]);
  const executionProgress = useMemo(() => calculateRunbookProgress(executionState), [
    executionState,
  ]);

  const canStart = [RUNBOOK_EXECUTION_STATUS.idle, RUNBOOK_EXECUTION_STATUS.completed].includes(
    executionStatus
  );
  const canPause = executionStatus === RUNBOOK_EXECUTION_STATUS.running;
  const canResume = executionStatus === RUNBOOK_EXECUTION_STATUS.paused;
  const canAdvance = executionStatus === RUNBOOK_EXECUTION_STATUS.running;
  const canRetry = [RUNBOOK_EXECUTION_STATUS.failed, RUNBOOK_EXECUTION_STATUS.paused].includes(
    executionStatus
  );
  const isAuditOnlyMode = executionStatus === RUNBOOK_EXECUTION_STATUS.completed;
  const assistantMinStepIndex = isAuditOnlyMode ? runbookMonitoringStepStartIndex : 0;

  const clearProvisioningInfraDraft = useCallback(() => {
    const storage = getBrowserLocalStorage();
    if (!storage) {
      return;
    }

    storage.removeItem(INFRA_ONBOARDING_STORAGE_KEY);
    storage.removeItem(INFRA_ONBOARDING_STORAGE_KEY_LEGACY);
  }, []);

  const appendProvisioningHistory = useCallback((snapshot, contextSummary) => {
    if (!snapshot || !snapshot.runId) {
      return;
    }

    const normalizedFinishedAt = String(snapshot.finishedAt || '').trim();
    const normalizedRunId = String(snapshot.runId || '').trim();
    const historyEntryKey = `${normalizedRunId}::${normalizedFinishedAt || 'open'}`;
    const historyEntry = {
      key: historyEntryKey,
      runId: normalizedRunId,
      changeId: String(snapshot.changeId || '').trim(),
      status: String(snapshot.status || '').trim(),
      startedAt: String(snapshot.startedAt || '').trim(),
      finishedAt: normalizedFinishedAt,
      capturedAt: new Date().toISOString(),
      context: {
        ...buildProvisioningContextSummary({ snapshot }),
        ...(contextSummary && typeof contextSummary === 'object' ? contextSummary : {}),
      },
      executionState: cloneRunbookExecutionSnapshot(snapshot),
    };

    setProvisioningHistory(currentEntries => {
      const existingEntry = currentEntries.find(entry => entry.key === historyEntryKey);
      if (existingEntry && existingEntry.context) {
        historyEntry.context = mergeProvisioningContextSummary(
          existingEntry.context,
          historyEntry.context
        );
      }
      const filteredEntries = currentEntries.filter(entry => entry.key !== historyEntryKey);
      return [historyEntry, ...filteredEntries];
    });
  }, []);

  useEffect(() => {
    if (historyHydratedRef.current) {
      return;
    }
    historyHydratedRef.current = true;

    const storage = getBrowserLocalStorage();
    if (!storage) {
      return;
    }

    const rawHistoryPayload = storage.getItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
    if (!rawHistoryPayload) {
      return;
    }

    try {
      const parsedHistory = JSON.parse(rawHistoryPayload);
      const normalizedHistory = (Array.isArray(parsedHistory) ? parsedHistory : [])
        .filter(entry => entry && typeof entry === 'object')
        .filter(entry => entry.executionState && typeof entry.executionState === 'object')
        .map(entry => ({
          ...entry,
          key: String(entry.key || ''),
          runId: String(entry.runId || ''),
          changeId: String(entry.changeId || ''),
          status: String(entry.status || ''),
          startedAt: String(entry.startedAt || ''),
          finishedAt: String(entry.finishedAt || ''),
          capturedAt: String(entry.capturedAt || ''),
          context: {
            ...buildProvisioningContextSummary({ snapshot: entry.executionState }),
            ...(entry.context && typeof entry.context === 'object' ? entry.context : {}),
          },
          executionState: cloneRunbookExecutionSnapshot(entry.executionState),
        }))
        .filter(entry => entry.key && entry.runId);

      if (normalizedHistory.length === 0) {
        return;
      }

      setProvisioningHistory(normalizedHistory);
    } catch (error) {
      storage.removeItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
    }
  }, []);

  useEffect(() => {
    const storage = getBrowserLocalStorage();
    if (!storage) {
      return;
    }

    const boundedHistory = provisioningHistory.slice(0, RUNBOOK_AUDIT_HISTORY_MAX_ENTRIES);
    storage.setItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY, JSON.stringify(boundedHistory));
  }, [provisioningHistory]);

  useEffect(() => {
    if (
      !startPreconditions.allowStart &&
      preconditionsStepApproved &&
      executionStatus === RUNBOOK_EXECUTION_STATUS.idle
    ) {
      setPreconditionsStepApproved(false);
    }
  }, [executionStatus, preconditionsStepApproved, startPreconditions.allowStart]);

  useEffect(() => {
    if (assistantStepIndex >= assistantMinStepIndex) {
      return;
    }

    setAssistantStepIndex(assistantMinStepIndex);
  }, [assistantMinStepIndex, assistantStepIndex]);

  useEffect(() => {
    if (executionStatus !== RUNBOOK_EXECUTION_STATUS.completed || !executionState.runId) {
      return;
    }

    const historyContext = buildProvisioningContextSummary({
      snapshot: executionState,
      handoff: onboardingHandoff,
    });
    appendProvisioningHistory(executionState, historyContext);
    clearOnboardingRunbookHandoff();
    setOnboardingHandoff(null);
    clearProvisioningInfraDraft();
  }, [
    appendProvisioningHistory,
    clearProvisioningInfraDraft,
    executionState,
    executionStatus,
    onboardingHandoff,
  ]);

  const activeStage = useMemo(
    () =>
      executionState.stages.find(
        (stage, stageIndex) => stageIndex === executionState.currentStageIndex
      ) || null,
    [executionState]
  );
  const activeCheckpoint = useMemo(
    () =>
      (activeStage &&
        (activeStage.checkpoints.find(checkpoint =>
          [
            RUNBOOK_CHECKPOINT_STATUS.running,
            RUNBOOK_CHECKPOINT_STATUS.paused,
            RUNBOOK_CHECKPOINT_STATUS.failed,
          ].includes(checkpoint.status)
        ) ||
          activeStage.checkpoints.find(
            checkpoint => checkpoint.status === RUNBOOK_CHECKPOINT_STATUS.pending
          ))) ||
      null,
    [activeStage]
  );
  const timelineStageOrderByKey = useMemo(
    () => buildTimelineStageOrderByKey(executionState.stages),
    [executionState.stages]
  );
  const eventRows = useMemo(
    () =>
      buildTimelineRows({
        events: executionState.events,
        stageOrderByKey: timelineStageOrderByKey,
        defaultRunId: executionState.runId,
        defaultChangeId: executionState.changeId,
        localeCandidate: locale,
      }),
    [executionState.changeId, executionState.events, executionState.runId, locale, timelineStageOrderByKey]
  );

  const registerBlockingEvent = ({ code, message: eventMessage, path = '' }) => {
    setExecutionState(currentState =>
      recordRunbookBlockingEvent(currentState, {
        code,
        message: eventMessage,
        path,
        stage: activeStage ? activeStage.key : '',
        checkpoint: activeCheckpoint ? activeCheckpoint.key : '',
        backendState,
        changeId: changeId.trim() || currentState.changeId,
        runId: currentState.runId,
      })
    );
  };

  const registerBackendBlockedCommand = commandLabel => {
    const commandBlockedCode =
      backendState === RUNBOOK_BACKEND_STATES.invalid
        ? 'runbook_command_blocked_backend_invalid'
        : 'runbook_command_blocked_backend_pending';

    registerBlockingEvent({
      code: commandBlockedCode,
      message: `Comando '${commandLabel}' bloqueado: backend em estado ${backendState}.`,
      path: 'backend_state',
    });
  };

  const applyBackendExecutionState = nextExecutionState => {
    const backendStateFromResponse =
      normalizeBackendState(nextExecutionState && nextExecutionState.backendState) ||
      RUNBOOK_BACKEND_STATES.ready;
    setBackendState(backendStateFromResponse);
    setExecutionState({
      ...nextExecutionState,
      backendState: backendStateFromResponse,
    });
  };

  const markBackendFailure = (commandLabel, error) => {
    const backendMessage = sanitizeSensitiveText(resolveRequestErrorMessage(error));
    const markAsInvalid = shouldMarkBackendInvalid(error);
    const backendFailureCode = resolveBackendFailureCode(error);
    const backendFailureDetails = resolveBackendFailureDetails(error);
    const inferredRuntimeImageMissingCode =
      !backendFailureCode &&
      /cognus_runtime_image_missing|runtime_image_missing|imagem de runtime obrigatoria do cognus/i.test(
        backendMessage
      )
        ? 'runbook_runtime_image_missing'
        : '';
    const inferredSshFailureCode =
      !backendFailureCode &&
      !inferredRuntimeImageMissingCode &&
      /execucao ssh no host alvo falhou|docker:\s*error response from daemon|runtime image|runtime_image|cognus_runtime_image_missing/i.test(
        backendMessage
      )
        ? 'runbook_ssh_execution_failed'
        : '';
    const nextBackendState = markAsInvalid
      ? RUNBOOK_BACKEND_STATES.invalid
      : RUNBOOK_BACKEND_STATES.ready;
    const failureCode = markAsInvalid
      ? 'runbook_command_blocked_backend_invalid'
      : backendFailureCode ||
        inferredRuntimeImageMissingCode ||
        inferredSshFailureCode ||
        'runbook_command_blocked_backend_pending';
    const detailsPairs = Object.entries(backendFailureDetails || {})
      .map(([key, value]) => `${key}=${sanitizeSensitiveText(String(value || '').trim())}`)
      .filter(entry => !entry.endsWith('='));
    const detailsSuffix =
      detailsPairs.length > 0 ? ` | detalhes: ${detailsPairs.slice(0, 3).join(', ')}` : '';

    setBackendState(nextBackendState);
    setExecutionState(currentState =>
      recordRunbookBlockingEvent(currentState, {
        code: failureCode,
        message: `Falha no comando '${commandLabel}' no backend do orquestrador: ${backendMessage}${detailsSuffix}`,
        path: 'backend_state',
        stage: activeStage ? activeStage.key : '',
        checkpoint: activeCheckpoint ? activeCheckpoint.key : '',
        backendState: nextBackendState,
        changeId: changeId.trim() || currentState.changeId,
        runId: currentState.runId,
      })
    );

    return backendMessage;
  };

  const executeOperateCommand = async ({
    action,
    commandLabel,
    successMessage,
    onSuccess,
    failurePayload = {},
  }) => {
    if (!backendCommandReady) {
      registerBackendBlockedCommand(commandLabel);
      message.error(
        t(
          'Comando bloqueado: backend em estado pendente ou invalido.',
          'Command blocked: backend in pending or invalid state.'
        )
      );
      return;
    }

    if (!executionState.runId) {
      message.error(
        t(
          'run_id ausente. Inicie uma execucao antes de enviar comandos operacionais.',
          'Missing run_id. Start an execution before sending operational commands.'
        )
      );
      return;
    }

    try {
      const backendResponse = await operateRunbook({
        run_id: executionState.runId,
        action,
        ...failurePayload,
      });
      applyBackendExecutionState(backendResponse.executionState);
      if (typeof onSuccess === 'function') {
        onSuccess(backendResponse.executionState);
      } else if (successMessage) {
        message.success(successMessage);
      }
    } catch (error) {
      const backendMessage = markBackendFailure(commandLabel, error);
      message.error(
        t(
          "Falha no comando '{commandLabel}': {backendMessage}",
          "Command '{commandLabel}' failed: {backendMessage}",
          { commandLabel, backendMessage }
        )
      );
    }
  };

  const handleStart = async () => {
    if (!startActionReadiness.available) {
      message.error(startActionReadiness.reason);
      return;
    }

    if (!backendCommandReady) {
      registerBackendBlockedCommand('start');
      message.error(
        t(
          'Comando bloqueado: backend em estado pendente ou invalido.',
          'Command blocked: backend in pending or invalid state.'
        )
      );
      return;
    }

    if (!startPreconditions.allowStart) {
      const firstReason = startPreconditions.reasons[0];
      setExecutionState(currentState =>
        recordRunbookPreconditionEvents(currentState, startPreconditions.reasons, {
          backendState,
          stage: activeStage ? activeStage.key : '',
          checkpoint: activeCheckpoint ? activeCheckpoint.key : '',
          changeId: changeId.trim() || currentState.changeId,
          runId: currentState.runId,
        })
      );
      message.error(
        firstReason
          ? firstReason.message
          : 'Não foi possível iniciar o runbook por pré-condições inválidas.'
      );
      return;
    }

    try {
      const latestHandoff = consumeOnboardingRunbookHandoff();
      const effectiveHandoff = latestHandoff || onboardingHandoff;

      const expectedChangeId = String(
        (effectiveHandoff && effectiveHandoff.change_id) || changeId || ''
      ).trim();
      const expectedRunId = String(
        (effectiveHandoff && effectiveHandoff.run_id) || plannedRunId || ''
      ).trim();
      const expectedBlueprintFingerprint = String(
        (effectiveHandoff && effectiveHandoff.blueprint_fingerprint) ||
          (selectedBlueprint && selectedBlueprint.fingerprint) ||
          ''
      ).trim();
      const expectedSourceBlueprintFingerprint = resolveSourceBlueprintFingerprint(
        effectiveHandoff && effectiveHandoff.source_blueprint_fingerprint,
        effectiveHandoff && effectiveHandoff.blueprint_fingerprint,
        expectedBlueprintFingerprint
      );
      const expectedManifestFingerprint = resolveManifestFingerprint({
        manifestFingerprint: effectiveHandoff && effectiveHandoff.manifest_fingerprint,
        sourceBlueprintFingerprint: expectedSourceBlueprintFingerprint,
      });
      const expectedHandoffContractVersion = String(
        (effectiveHandoff && effectiveHandoff.handoff_contract_version) || ''
      ).trim();
      const expectedHandoffFingerprint = String(
        (effectiveHandoff && effectiveHandoff.handoff_fingerprint) || ''
      ).trim();
      const expectedHandoffPayload =
        effectiveHandoff && typeof effectiveHandoff.handoff_payload === 'object'
          ? effectiveHandoff.handoff_payload
          : null;
      const expectedHandoffTrace = Array.isArray(effectiveHandoff && effectiveHandoff.handoff_trace)
        ? effectiveHandoff.handoff_trace
        : [];
      const expectedRunbookResumeContext =
        effectiveHandoff && typeof effectiveHandoff.runbook_resume_context === 'object'
          ? effectiveHandoff.runbook_resume_context
          : null;
      const expectedTopologyCatalog =
        effectiveHandoff && typeof effectiveHandoff.topology_catalog === 'object'
          ? effectiveHandoff.topology_catalog
          : null;
      const expectedHostMapping = Array.isArray(effectiveHandoff && effectiveHandoff.host_mapping)
        ? effectiveHandoff.host_mapping
        : [];
      const expectedMachineCredentials = Array.isArray(
        effectiveHandoff && effectiveHandoff.machine_credentials
      )
        ? effectiveHandoff.machine_credentials
        : [];
      const expectedApiRegistry = Array.isArray(effectiveHandoff && effectiveHandoff.api_registry)
        ? effectiveHandoff.api_registry
        : [];
      const expectedIncrementalExpansions = Array.isArray(
        effectiveHandoff && effectiveHandoff.incremental_expansions
      )
        ? effectiveHandoff.incremental_expansions
        : [];
      const expectedArtifactsContext = resolveOfficialArtifactsContext({
        requiredArtifacts: effectiveHandoff && effectiveHandoff.a2_2_minimum_artifacts,
        availableArtifacts: effectiveHandoff && effectiveHandoff.a2_2_available_artifacts,
        artifactsReady:
          effectiveHandoff && typeof effectiveHandoff.a2_2_artifacts_ready === 'boolean'
            ? effectiveHandoff.a2_2_artifacts_ready
            : null,
        contextSource: normalizeText(
          (effectiveHandoff && (effectiveHandoff.context_source || effectiveHandoff.source)) || ''
        ),
      });
      const expectedRequiredArtifacts = expectedArtifactsContext.requiredArtifacts;
      const expectedAvailableArtifacts = expectedArtifactsContext.availableArtifacts;
      let expectedPipelineReady = expectedArtifactsContext.artifactsReady;
      if (
        !expectedPipelineReady &&
        effectiveHandoff &&
        typeof effectiveHandoff.pipeline_preconditions_ready === 'boolean'
      ) {
        expectedPipelineReady = Boolean(effectiveHandoff.pipeline_preconditions_ready);
      }
      const expectedBlueprintValidated =
        effectiveHandoff && typeof effectiveHandoff.blueprint_validated === 'boolean'
          ? Boolean(effectiveHandoff.blueprint_validated)
          : blueprintValidated;
      const expectedPreflightApproved =
        effectiveHandoff && typeof effectiveHandoff.preflight_approved === 'boolean'
          ? Boolean(effectiveHandoff.preflight_approved)
          : preflightApproved;

      if (latestHandoff) {
        setOnboardingHandoff(latestHandoff);
        setChangeId(expectedChangeId);
        setPlannedRunId(expectedRunId);
        setPipelineEvidenceReady(expectedPipelineReady);
        setCryptoBaselineReady(expectedPipelineReady);
        setPreflightApproved(expectedPreflightApproved);
      }

      const backendResponse = await startRunbook({
        change_id: expectedChangeId,
        provider_key: providerKey,
        environment_profile: environmentProfile,
        blueprint_version: selectedBlueprintVersion,
        blueprint_fingerprint: expectedBlueprintFingerprint,
        resolved_schema_version: selectedBlueprint
          ? selectedBlueprint.resolvedSchemaVersion
          : '1.0.0',
        run_id: expectedRunId,
        manifest_fingerprint: expectedManifestFingerprint,
        source_blueprint_fingerprint: expectedSourceBlueprintFingerprint,
        a2_2_minimum_artifacts: expectedRequiredArtifacts,
        a2_2_available_artifacts: expectedAvailableArtifacts,
        pipeline_preconditions_ready: expectedPipelineReady,
        blueprint_validated: expectedBlueprintValidated,
        preflight_approved: expectedPreflightApproved,
        host_mapping: expectedHostMapping,
        machine_credentials: expectedMachineCredentials,
        api_registry: expectedApiRegistry,
        incremental_expansions: expectedIncrementalExpansions,
        topology_catalog: expectedTopologyCatalog,
        handoff_contract_version: expectedHandoffContractVersion,
        handoff_fingerprint: expectedHandoffFingerprint,
        handoff_payload: expectedHandoffPayload,
        handoff_trace: expectedHandoffTrace,
        runbook_resume_context: expectedRunbookResumeContext,
      });

      const backendRun = backendResponse && backendResponse.run ? backendResponse.run : {};
      const persistedRunId = String(backendRun.run_id || '').trim();
      const persistedChangeId = String(backendRun.change_id || '').trim();
      const persistedFingerprint = String(backendRun.blueprint_fingerprint || '').trim();
      const persistedManifestFingerprint = String(backendRun.manifest_fingerprint || '').trim();
      const persistedSourceBlueprintFingerprint = String(
        backendRun.source_blueprint_fingerprint || ''
      ).trim();
      const persistedHandoffFingerprint = String(backendRun.handoff_fingerprint || '').trim();

      const coherenceMismatches = [];
      const evaluateMismatch = ({ fieldKey, expectedValue, persistedValue }) => {
        if (!expectedValue) {
          return;
        }
        if (!persistedValue) {
          coherenceMismatches.push(`${fieldKey}_missing`);
          return;
        }
        if (persistedValue !== expectedValue) {
          coherenceMismatches.push(fieldKey);
        }
      };

      evaluateMismatch({
        fieldKey: 'run_id',
        expectedValue: expectedRunId,
        persistedValue: persistedRunId,
      });
      evaluateMismatch({
        fieldKey: 'change_id',
        expectedValue: expectedChangeId,
        persistedValue: persistedChangeId,
      });
      evaluateMismatch({
        fieldKey: 'blueprint_fingerprint',
        expectedValue: expectedBlueprintFingerprint,
        persistedValue: persistedFingerprint,
      });
      evaluateMismatch({
        fieldKey: 'manifest_fingerprint',
        expectedValue: expectedManifestFingerprint,
        persistedValue: persistedManifestFingerprint,
      });
      evaluateMismatch({
        fieldKey: 'source_blueprint_fingerprint',
        expectedValue: expectedSourceBlueprintFingerprint,
        persistedValue: persistedSourceBlueprintFingerprint,
      });
      evaluateMismatch({
        fieldKey: 'handoff_fingerprint',
        expectedValue: expectedHandoffFingerprint,
        persistedValue: persistedHandoffFingerprint,
      });

      if (coherenceMismatches.length > 0) {
        const mismatchFields = coherenceMismatches.join(', ');
        setBackendState(RUNBOOK_BACKEND_STATES.invalid);
        setExecutionState(currentState =>
          recordRunbookBlockingEvent(currentState, {
            code: 'runbook_start_identity_mismatch',
            message: `Inconsistencia entre UI e backend do orquestrador apos start (${mismatchFields}). Execucao bloqueada ate reconciliacao.`,
            path: 'run_identity',
            stage: activeStage ? activeStage.key : '',
            checkpoint: activeCheckpoint ? activeCheckpoint.key : '',
            backendState: RUNBOOK_BACKEND_STATES.invalid,
            changeId: expectedChangeId || currentState.changeId,
            runId: persistedRunId || expectedRunId || currentState.runId,
          })
        );
        message.error(
          `Start bloqueado: incoerencia de identidade no backend do orquestrador (${mismatchFields}).`
        );
        return;
      }

      applyBackendExecutionState(backendResponse.executionState);
      message.success(
        t(
          'Runbook {runId} iniciado.',
          'Runbook {runId} started.',
          { runId: backendResponse.executionState.runId }
        )
      );
    } catch (error) {
      const backendMessage = markBackendFailure('start', error);
      message.error(
        t(
          'Falha ao iniciar runbook: {backendMessage}',
          'Failed to start runbook: {backendMessage}',
          { backendMessage }
        )
      );
    }
  };

  const handlePause = async () => {
    if (!pauseActionReadiness.available) {
      message.error(pauseActionReadiness.reason);
      return;
    }

    await executeOperateCommand({
      action: 'pause',
      commandLabel: 'pause',
      successMessage: 'Execucao pausada.',
    });
  };

  const handleResume = async () => {
    if (!resumeActionReadiness.available) {
      message.error(resumeActionReadiness.reason);
      return;
    }

    await executeOperateCommand({
      action: 'resume',
      commandLabel: 'resume',
      successMessage: 'Execucao retomada do ultimo checkpoint valido.',
    });
  };

  const handleAdvance = async () => {
    if (isAdvancingCheckpoint) {
      return;
    }

    if (!advanceActionReadiness.available) {
      message.error(advanceActionReadiness.reason);
      return;
    }

    setIsAdvancingCheckpoint(true);
    try {
      await executeOperateCommand({
        action: 'advance',
        commandLabel: 'advance',
        onSuccess: nextExecutionState => {
          if (nextExecutionState.status === RUNBOOK_EXECUTION_STATUS.completed) {
            const completionDecision = normalizeDecision(
              nextExecutionState &&
                nextExecutionState.officialDecision &&
                nextExecutionState.officialDecision.decision
            );
            if (completionDecision === 'allow') {
              message.success(
                t(
                  'Runbook concluído com decisão técnica allow.',
                  'Runbook completed with official technical decision allow.'
                )
              );
              setIsAssistantDialogOpen(false);
            } else {
              message.error(
                'Runbook concluído com decisão técnica block. Continuidade operacional permanece bloqueada.'
              );
              setIsAssistantDialogOpen(true);
            }
            setAssistantStepIndex(runbookMonitoringStepStartIndex);
            return;
          }
          message.success(t('Checkpoint avançado.', 'Checkpoint advanced.'));
        },
      });
    } finally {
      setIsAdvancingCheckpoint(false);
    }
  };

  const handleRetry = async () => {
    if (!retryActionReadiness.available) {
      message.error(retryActionReadiness.reason);
      return;
    }

    await executeOperateCommand({
      action: 'retry',
      commandLabel: 'retry',
      successMessage: 'Reexecucao segura iniciada a partir do ultimo checkpoint valido.',
    });
  };

  const stageColumns = [
    {
      title: t('Etapa', 'Stage'),
      dataIndex: 'stageLabel',
      key: 'stageLabel',
      width: 120,
      render: (value, row) => (
        <Space>
          <Typography.Text strong>{`${row.order}. ${value}`}</Typography.Text>
          {row.isCurrentStage && <Tag color="blue">{t('ativa', 'active')}</Tag>}
        </Space>
      ),
    },
    {
      title: t('Status', 'Status'),
      dataIndex: 'stageStatus',
      key: 'stageStatus',
      width: 120,
      render: status => renderStatusTag(status, checkpointStatusToneMap),
    },
    {
      title: t('Checkpoints', 'Checkpoints'),
      key: 'checkpoints',
      width: 150,
      render: (_, row) => (
        <Typography.Text>{`${row.checkpointsCompleted}/${row.checkpointsTotal}`}</Typography.Text>
      ),
    },
    {
      title: t('Checkpoint atual', 'Current checkpoint'),
      key: 'currentCheckpoint',
      render: (_, row) => (
        <Space>
          <Typography.Text>{row.currentCheckpointLabel}</Typography.Text>
          {renderStatusTag(row.currentCheckpointStatus, checkpointStatusToneMap)}
        </Space>
      ),
    },
    {
      title: t('Evidências', 'Evidence'),
      key: 'evidenceArtifacts',
      render: (_, row) => <Typography.Text>{row.evidenceArtifacts.join(', ')}</Typography.Text>,
    },
  ];

  const eventColumns = [
    {
      title: t('UTC', 'UTC'),
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 164,
      render: value => {
        const timestampLabel = formatUtcTimestamp(value);
        return (
          <Typography.Text className={runbookStyles.compactCell} title={timestampLabel}>
            {timestampLabel}
          </Typography.Text>
        );
      },
    },
    {
      title: t('Nível', 'Level'),
      dataIndex: 'level',
      key: 'level',
      width: 90,
      render: value => {
        const normalizedLevel = normalizeTimelineLevel(value);
        return (
          <Tag color={eventLevelColor[normalizedLevel] || 'blue'}>
            {eventLevelLabel[normalizedLevel] || normalizedLevel}
          </Tag>
        );
      },
    },
    {
      title: t('Classificação', 'Classification'),
      dataIndex: 'classification',
      key: 'classification',
      width: 130,
      render: value =>
        renderStatusTag(value || '-', {
          ...diagnosticClassificationToneMap,
          '-': 'default',
        }),
    },
    {
      title: t('Código', 'Code'),
      dataIndex: 'code',
      key: 'code',
      width: 208,
      render: value => (
        <Typography.Text className={runbookStyles.compactCell} code title={value}>
          {value}
        </Typography.Text>
      ),
    },
    {
      title: t('run_id', 'run_id'),
      dataIndex: 'runId',
      key: 'runId',
      width: 186,
      render: value =>
        value ? (
          <Typography.Text className={runbookStyles.compactCell} code title={value}>
            {value}
          </Typography.Text>
        ) : (
          '-'
        ),
    },
    {
      title: t('change_id', 'change_id'),
      dataIndex: 'changeId',
      key: 'changeId',
      width: 166,
      render: value =>
        value ? (
          <Typography.Text className={runbookStyles.compactCell} code title={value}>
            {value}
          </Typography.Text>
        ) : (
          '-'
        ),
    },
    {
      title: t('Etapa', 'Stage'),
      dataIndex: 'stage',
      key: 'stage',
      width: 110,
      render: (value, row) => {
        if (!value) {
          return '-';
        }
        const stageLabel =
          row.stageOrder && row.stageOrder < TIMELINE_UNKNOWN_STAGE_ORDER
            ? `${row.stageOrder}. ${value}`
            : value;
        return (
          <Typography.Text className={runbookStyles.compactCell} title={stageLabel}>
            {stageLabel}
          </Typography.Text>
        );
      },
    },
    {
      title: t('Componente', 'Component'),
      dataIndex: 'component',
      key: 'component',
      width: 190,
      render: value => (
        <Typography.Text
          className={runbookStyles.compactCell}
          title={sanitizeSensitiveText(value) || '-'}
        >
          {sanitizeSensitiveText(value) || '-'}
        </Typography.Text>
      ),
    },
    {
      title: t('Diagnóstico técnico', 'Technical diagnostic'),
      key: 'diagnostic',
      width: 420,
      render: (_, row) => {
        const diagnosticLabel = t(
          'causa: {cause} | impacto: {impact} | ação recomendada: {action}',
          'cause: {cause} | impact: {impact} | recommended action: {action}',
          {
            cause: sanitizeSensitiveText(row.cause) || '-',
            impact: sanitizeSensitiveText(row.impact) || '-',
            action: sanitizeSensitiveText(row.recommendedAction) || '-',
          }
        );
        return (
          <Typography.Text className={runbookStyles.compactCell} title={diagnosticLabel}>
            {diagnosticLabel}
          </Typography.Text>
        );
      },
    },
    {
      title: t('Mensagem', 'Message'),
      dataIndex: 'message',
      key: 'message',
      width: 300,
      render: value => (
        <Typography.Text
          className={runbookStyles.compactCell}
          title={sanitizeSensitiveText(value) || '-'}
        >
          {sanitizeSensitiveText(value) || '-'}
        </Typography.Text>
      ),
    },
  ];

  const evidenceColumns = [
    {
      title: t('Etapa', 'Stage'),
      dataIndex: 'stageLabel',
      key: 'stageLabel',
      width: 140,
      render: (value, row) => (
        <Typography.Text
          className={runbookStyles.compactCell}
          strong
        >{`${row.stageOrder}. ${value}`}</Typography.Text>
      ),
    },
    {
      title: t('Checkpoints', 'Checkpoints'),
      dataIndex: 'checkpointProgress',
      key: 'checkpointProgress',
      width: 120,
    },
    {
      title: t('Status da evidência', 'Evidence status'),
      dataIndex: 'evidenceStatus',
      key: 'evidenceStatus',
      width: 150,
      render: value => renderStatusTag(value, evidenceStatusToneMap),
    },
    {
      title: t('Produzido em UTC', 'Produced at UTC'),
      dataIndex: 'producedAt',
      key: 'producedAt',
      width: 170,
      render: value => {
        const timestampLabel = formatUtcTimestamp(value);
        return (
          <Typography.Text className={runbookStyles.compactCell} title={timestampLabel}>
            {timestampLabel}
          </Typography.Text>
        );
      },
    },
    {
      title: t('Artefatos', 'Artifacts'),
      dataIndex: 'evidenceArtifacts',
      key: 'evidenceArtifacts',
      width: 260,
      render: value => {
        const artifactsLabel = Array.isArray(value) ? value.join(', ') : '-';
        return (
          <Typography.Text className={runbookStyles.compactCell} title={artifactsLabel}>
            {artifactsLabel}
          </Typography.Text>
        );
      },
    },
  ];

  const assistantStep = runbookAssistantSteps[assistantStepIndex];
  const isMonitoringStep = assistantStep
    ? RUNBOOK_MONITORING_STEP_KEYS.has(assistantStep.key)
    : false;
  const isFirstAssistantStep = assistantStepIndex === assistantMinStepIndex;
  const isLastAssistantStep = assistantStepIndex === runbookAssistantSteps.length - 1;
  const onboardingProvisioningSummary = useMemo(
    () => buildProvisioningContextSummary({ snapshot: executionState, handoff: onboardingHandoff }),
    [executionState, onboardingHandoff]
  );
  const hasPendingOnboardingProvisioning = Boolean(
    onboardingHandoff &&
      String(onboardingHandoff.run_id || onboardingHandoff.change_id || '').trim() &&
      executionStatus === RUNBOOK_EXECUTION_STATUS.idle
  );
  const plannedOrActiveRunId = String(
    (onboardingHandoff && onboardingHandoff.run_id) || plannedRunId || ''
  ).trim();
  const officialRunIdLabel = officialEntryGateContext.runId || plannedRunId || '-';
  const lockManualPreconditions = true;
  const a25GateSummary =
    a25GateReadinessStatus === READINESS_STATUS.implemented
      ? t(
          'Gate A2.5 aprovado: contexto oficial completo e artefatos mínimos A2.2 confirmados.',
          'A2.5 gate approved: complete official context and minimum A2.2 artifacts confirmed.'
        )
      : t(
          'Gate A2.5 bloqueante: ações críticas permanecem desabilitadas até correlação oficial completa.',
          'Blocking A2.5 gate: critical actions remain disabled until official correlation is complete.'
        );
  const controlsUnlocked =
    executionStatus !== RUNBOOK_EXECUTION_STATUS.idle ||
    (scopeStepApproved && preconditionsStepApproved && startPreconditions.allowStart);
  const activeStageStatus = String(activeStage && activeStage.status ? activeStage.status : '')
    .trim()
    .toLowerCase();
  const blockedByBackendState = !backendCommandReady;
  const blockedByCoherence = runbookSnapshot.coherence.status === 'inconsistent';
  const blockedByScopeLock = scopeLockActive;
  const blockedByFailedStage =
    activeStageStatus === RUNBOOK_CHECKPOINT_STATUS.failed ||
    executionStatus === RUNBOOK_EXECUTION_STATUS.failed;
  const officialStageBlocked =
    blockedByBackendState || blockedByCoherence || blockedByFailedStage || blockedByScopeLock;
  const officialStageBlockReasons = [];
  if (blockedByBackendState) {
    officialStageBlockReasons.push(
      t('backend do orquestrador em estado {state}', 'orchestrator backend in state {state}', {
        state: backendState || RUNBOOK_BACKEND_STATES.pending,
      })
    );
  }
  if (blockedByCoherence) {
    officialStageBlockReasons.push(
      t(
        'snapshot oficial inconsistente (coherence=inconsistent)',
        'inconsistent official snapshot (coherence=inconsistent)'
      )
    );
  }
  if (blockedByFailedStage) {
    officialStageBlockReasons.push(
      t('etapa oficial em failed, aguardando retry', 'official stage in failed state, waiting for retry')
    );
  }
  if (blockedByScopeLock) {
    const scopedLock = scopeLock.scopeKey || t('escopo operacional', 'operational scope');
    officialStageBlockReasons.push(
      t('lock oficial ativo em {scope} ({reason})', 'active official lock in {scope} ({reason})', {
        scope: scopedLock,
        reason: scopeLock.reasonCode || 'runbook_resource_lock_conflict',
      })
    );
  }
  const officialStageBlockReason =
    officialStageBlockReasons.length > 0
      ? officialStageBlockReasons.join('; ')
      : t('Etapa oficial bloqueada por validação técnica.', 'Official stage blocked by technical validation.');
  const disableStartCommand =
    !controlsUnlocked ||
    !canStart ||
    !startActionReadiness.available ||
    isAdvancingCheckpoint ||
    blockedByBackendState ||
    blockedByScopeLock;
  const disablePauseCommand =
    !controlsUnlocked ||
    !canPause ||
    !pauseActionReadiness.available ||
    isAdvancingCheckpoint ||
    officialStageBlocked;
  const disableResumeCommand =
    !controlsUnlocked ||
    !canResume ||
    !resumeActionReadiness.available ||
    isAdvancingCheckpoint ||
    officialStageBlocked;
  const disableAdvanceCommand =
    !controlsUnlocked ||
    !canAdvance ||
    !advanceActionReadiness.available ||
    isAdvancingCheckpoint ||
    officialStageBlocked;
  const disableRetryCommand =
    !controlsUnlocked ||
    !canRetry ||
    !retryActionReadiness.available ||
    isAdvancingCheckpoint ||
    blockedByBackendState ||
    blockedByCoherence ||
    blockedByScopeLock;
  let controlsLockReason = t(
    'As pré-condições de início precisam permanecer válidas para liberar os controles.',
    'Startup preconditions must remain valid to unlock the controls.'
  );
  if (!scopeStepApproved) {
    controlsLockReason = t(
      'Aprove o Passo 1 (Escopo e backend) para liberar os controles operacionais.',
      'Approve Step 1 (Scope and backend) to unlock the operational controls.'
    );
  } else if (!preconditionsStepApproved) {
    controlsLockReason = t(
      'Aprove o Passo 2 (Pré-condições de início) para liberar os controles operacionais.',
      'Approve Step 2 (Startup preconditions) to unlock the operational controls.'
    );
  }
  const buildControlTooltip = (
    readiness,
    { respectStageBlock = true, allowFailedStage = false } = {}
  ) => {
    if (!controlsUnlocked) {
      return controlsLockReason;
    }
    if (!readiness.available) {
      return buildReadinessTooltip(readiness);
    }
    if (blockedByBackendState) {
      return `Comando bloqueado: ${officialStageBlockReason}.`;
    }
    if (respectStageBlock && blockedByScopeLock) {
      return `Comando bloqueado: ${officialStageBlockReason}.`;
    }
    if (respectStageBlock && blockedByCoherence) {
      return `Comando bloqueado: ${officialStageBlockReason}.`;
    }
    if (respectStageBlock && blockedByFailedStage && !allowFailedStage) {
      return `Comando bloqueado: ${officialStageBlockReason}.`;
    }
    return buildReadinessTooltip(readiness);
  };

  const applyOnboardingProvisioningContext = useCallback(
    ({ openDialog = false } = {}) => {
      if (!onboardingHandoff) {
        return false;
      }

      const handoffChangeId = String(onboardingHandoff.change_id || '').trim();
      const handoffEnvironmentProfile = String(onboardingHandoff.environment_profile || '').trim();
      const handoffRunId = String(onboardingHandoff.run_id || '').trim();
      const handoffBlueprintVersion = String(onboardingHandoff.blueprint_version || '').trim();
      const handoffBlueprintFingerprint = String(
        onboardingHandoff.blueprint_fingerprint || ''
      ).trim();
      const handoffContextSource = normalizeText(
        onboardingHandoff.context_source || onboardingHandoff.source
      );
      const handoffSourceBlueprintFingerprint = resolveSourceBlueprintFingerprint(
        onboardingHandoff.source_blueprint_fingerprint,
        onboardingHandoff.blueprint_fingerprint
      );
      const handoffManifestFingerprint = resolveManifestFingerprint({
        manifestFingerprint: onboardingHandoff.manifest_fingerprint,
        sourceBlueprintFingerprint: handoffSourceBlueprintFingerprint,
      });
      const handoffArtifactsContext = resolveOfficialArtifactsContext({
        requiredArtifacts: onboardingHandoff.a2_2_minimum_artifacts,
        availableArtifacts: onboardingHandoff.a2_2_available_artifacts,
        artifactsReady:
          typeof onboardingHandoff.a2_2_artifacts_ready === 'boolean'
            ? onboardingHandoff.a2_2_artifacts_ready
            : null,
        contextSource: handoffContextSource,
      });
      const handoffResolvedSchemaVersion = String(
        onboardingHandoff.resolved_schema_version || '1.0.0'
      ).trim();
      const handoffPipelineReady = handoffArtifactsContext.artifactsReady;
      const handoffPreflightApproved = Boolean(onboardingHandoff.preflight_approved);
      const handoffBackendState = resolveBackendStateByContextSource({
        backendState: onboardingHandoff.official_backend_state,
        contextSource: handoffContextSource,
      });
      const handoffCatalogEntry = buildRunbookBlueprintCatalogEntry(onboardingHandoff);

      if (handoffCatalogEntry) {
        setBlueprintCatalog(currentRows => {
          const filteredRows = currentRows.filter(row => row.value !== handoffCatalogEntry.value);
          return [handoffCatalogEntry, ...filteredRows];
        });
        setSelectedBlueprintVersion(handoffCatalogEntry.value);
      }

      if (handoffChangeId) {
        setChangeId(handoffChangeId);
      }
      if (handoffEnvironmentProfile) {
        setEnvironmentProfile(handoffEnvironmentProfile);
      }

      setPlannedRunId(handoffRunId);
      setPipelineEvidenceReady(handoffPipelineReady);
      setCryptoBaselineReady(handoffPipelineReady);
      setPreflightApproved(handoffPreflightApproved);
      setBackendState(handoffBackendState);
      setExecutionState(
        createRunbookExecutionState({
          backendState: handoffBackendState,
          changeId: handoffChangeId,
          providerKey: RUNBOOK_PROVIDER_KEY,
          environmentProfile: handoffEnvironmentProfile || 'dev-external-linux',
          blueprintVersion: handoffBlueprintVersion,
          blueprintFingerprint: handoffBlueprintFingerprint,
          manifestFingerprint: handoffManifestFingerprint,
          sourceBlueprintFingerprint: handoffSourceBlueprintFingerprint,
          resolvedSchemaVersion: handoffResolvedSchemaVersion || '1.0.0',
          requiredArtifacts: handoffArtifactsContext.requiredArtifacts,
          availableArtifacts: handoffArtifactsContext.availableArtifacts,
          status: RUNBOOK_EXECUTION_STATUS.idle,
        })
      );
      setScopeStepApproved(false);
      setPreconditionsStepApproved(false);
      setAssistantStepIndex(0);
      setShowEvidencePanel(true);
      setShowTimelinePanel(true);
      setExpandedLogPanel('');

      if (openDialog) {
        setIsAssistantDialogOpen(true);
      }

      return true;
    },
    [onboardingHandoff]
  );
  let cockpitCardTitle = t(
    'Missão assistida passo a passo',
    'Step-by-step assisted mission'
  );
  let cockpitCardDescription = t(
    'Abra o dialog para navegar em etapas e concluir a execução assistida de provisão.',
    'Open the dialog to move through the stages and complete the assisted provisioning execution.'
  );
  let cockpitPrimaryButtonLabel = t('Abrir missão assistida', 'Open assisted mission');

  if (hasPendingOnboardingProvisioning) {
    cockpitCardTitle = t(
      'Provisionamento pendente pronto para continuar',
      'Pending provisioning ready to continue'
    );
    cockpitCardDescription = t(
      'Contexto do cockpit SSH recebido. Continue o provisionamento para executar start/pause/resume/advance com rastreabilidade por run_id e change_id.',
      'SSH cockpit context received. Continue the provisioning to run start/pause/resume/advance with run_id and change_id traceability.'
    );
    cockpitPrimaryButtonLabel = t('Continuar provisionamento', 'Continue provisioning');
  } else if (isAuditOnlyMode) {
    cockpitCardTitle = t('Missão assistida (auditoria)', 'Assisted mission (audit)');
    cockpitCardDescription = t(
      'Provisionamento concluído. Abra o dialog para auditar execução, evidências e timeline do provisionamento atual.',
      'Provisioning completed. Open the dialog to audit the execution, evidence, and timeline of the current provisioning.'
    );
    cockpitPrimaryButtonLabel = t('Abrir auditoria da missão', 'Open mission audit');
  }

  const openAssistantDialog = () => {
    setAssistantStepIndex(assistantMinStepIndex);
    setIsAssistantDialogOpen(true);
  };

  const handleContinueProvisioning = () => {
    const applied = applyOnboardingProvisioningContext({ openDialog: true });
    if (!applied) {
      message.warning(
        t(
          'Nenhum provisionamento pendente encontrado para continuar.',
          'No pending provisioning found to continue.'
        )
      );
    }
  };

  const moveAssistantStep = delta => {
    setAssistantStepIndex(current =>
      Math.max(assistantMinStepIndex, Math.min(runbookAssistantSteps.length - 1, current + delta))
    );
  };

  const handleApproveScopeStep = () => {
    setScopeStepApproved(true);
    message.success(t('Passo 1 aprovado.', 'Step 1 approved.'));
  };

  const handleApprovePreconditionsStep = () => {
    if (!scopeStepApproved) {
      message.warning(
        t(
          'Aprove primeiro o Passo 1 (Escopo e backend).',
          'Approve Step 1 first (Scope and backend).'
        )
      );
      return;
    }

    if (!startPreconditions.allowStart) {
      message.error(
        t(
          'Não foi possível aprovar o Passo 2: ainda existem pré-condições pendentes.',
          'Could not approve Step 2: there are still pending preconditions.'
        )
      );
      return;
    }

    setPreconditionsStepApproved(true);
    message.success(
      t(
        'Passo 2 aprovado. Controles operacionais liberados.',
        'Step 2 approved. Operational controls enabled.'
      )
    );
  };

  const handleStartNewProvisioning = () => {
    setExecutionState(
      createRunbookExecutionState({
        backendState: RUNBOOK_BACKEND_STATES.pending,
        changeId: changeId.trim(),
        blueprintVersion: selectedBlueprintVersion,
        blueprintFingerprint: selectedBlueprint ? selectedBlueprint.fingerprint : '',
        resolvedSchemaVersion: selectedBlueprint
          ? selectedBlueprint.resolvedSchemaVersion
          : '1.0.0',
      })
    );
    setBackendState(RUNBOOK_BACKEND_STATES.pending);
    setScopeStepApproved(false);
    setPreconditionsStepApproved(false);
    setAssistantStepIndex(0);
    setIsAssistantDialogOpen(false);
    setShowEvidencePanel(true);
    setShowTimelinePanel(true);
    setExpandedLogPanel('');
    setPlannedRunId('');
    clearOnboardingRunbookHandoff();
    setOnboardingHandoff(null);
    clearProvisioningInfraDraft();
    message.success(
      t(
        'Novo provisionamento preparado. Retornando ao Provisionamento de Infraestrutura via SSH.',
        'New provisioning prepared. Returning to Infrastructure Provisioning via SSH.'
      )
    );
    history.push(PROVISIONING_INFRA_ROUTE_PATH);
  };

  const handleOpenHistoryAudit = useCallback(historyEntry => {
    if (!historyEntry || !historyEntry.executionState) {
      return;
    }

    const historyState = cloneRunbookExecutionSnapshot(historyEntry.executionState);
    setExecutionState(historyState);
    setBackendState(String(historyState.backendState || RUNBOOK_BACKEND_STATES.ready));
    setAssistantStepIndex(runbookMonitoringStepStartIndex);
    setIsAssistantDialogOpen(true);
  }, [runbookMonitoringStepStartIndex]);

  useEffect(() => {
    if (provisioningHistory.length === 0) {
      return;
    }

    const storage = getBrowserSessionStorage();
    if (!storage) {
      return;
    }

    const rawSelectedAuditPayload = storage.getItem(RUNBOOK_AUDIT_SELECTED_STORAGE_KEY);
    if (!rawSelectedAuditPayload) {
      return;
    }

    let parsedSelectedAudit = null;
    try {
      parsedSelectedAudit = JSON.parse(rawSelectedAuditPayload);
    } catch (error) {
      storage.removeItem(RUNBOOK_AUDIT_SELECTED_STORAGE_KEY);
      return;
    }

    const selectedKey = String(parsedSelectedAudit && parsedSelectedAudit.key).trim();
    const selectedRunId = String(parsedSelectedAudit && parsedSelectedAudit.runId).trim();
    const selectedCapturedAt = String(parsedSelectedAudit && parsedSelectedAudit.capturedAt).trim();

    const selectedEntry = provisioningHistory.find(entry => {
      if (selectedKey && entry.key === selectedKey) {
        return true;
      }
      if (selectedRunId && entry.runId === selectedRunId) {
        if (!selectedCapturedAt) {
          return true;
        }
        return String(entry.capturedAt || '').trim() === selectedCapturedAt;
      }
      return false;
    });

    storage.removeItem(RUNBOOK_AUDIT_SELECTED_STORAGE_KEY);
    if (selectedEntry) {
      handleOpenHistoryAudit(selectedEntry);
    }
  }, [handleOpenHistoryAudit, provisioningHistory]);

  const toggleLogPanel = panel => {
    if (!controlsUnlocked) {
      return;
    }

    if (panel === 'evidence') {
      setShowEvidencePanel(current => {
        const nextValue = !current;
        if (!nextValue && expandedLogPanel === 'evidence') {
          setExpandedLogPanel('');
        }
        return nextValue;
      });
      return;
    }

    setShowTimelinePanel(current => {
      const nextValue = !current;
      if (!nextValue && expandedLogPanel === 'timeline') {
        setExpandedLogPanel('');
      }
      return nextValue;
    });
  };

  const toggleExpandedLogPanel = panel => {
    if (!controlsUnlocked) {
      return;
    }
    setExpandedLogPanel(current => (current === panel ? '' : panel));
  };

  const visibleLogPanelsCount = Number(showEvidencePanel) + Number(showTimelinePanel);
  const shouldRenderLogDock = controlsUnlocked && visibleLogPanelsCount > 0;
  let logDockGridTemplateColumns = '1fr';
  if (showEvidencePanel && showTimelinePanel) {
    if (expandedLogPanel === 'evidence') {
      logDockGridTemplateColumns = '1.6fr 1fr';
    } else if (expandedLogPanel === 'timeline') {
      logDockGridTemplateColumns = '1fr 1.6fr';
    } else {
      logDockGridTemplateColumns = 'repeat(2, minmax(0, 1fr))';
    }
  }

  const renderMonitoringPanels = () => (
    <div style={{ display: 'grid', gap: 12 }}>
      <div className={styles.neoGrid2}>
        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Estado da execução', 'Execution state')}
          </Typography.Title>
          <div style={{ display: 'grid', gap: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>run_id</Typography.Text>
              <Typography.Text code>{executionState.runId || plannedRunId || '-'}</Typography.Text>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('status', 'status')}
              </Typography.Text>
              {renderStatusTag(executionStatus, runbookStatusToneMap)}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>change_id</Typography.Text>
              <Typography.Text code>{executionState.changeId || '-'}</Typography.Text>
            </div>
            <div>
              <Typography.Text className={styles.neoLabel}>
                {t('progresso do runbook', 'runbook progress')}
              </Typography.Text>
              <Progress
                percent={executionProgress}
                status={
                  executionStatus === RUNBOOK_EXECUTION_STATUS.failed ? 'exception' : 'active'
                }
              />
            </div>
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Sinais operacionais', 'Operational signals')}
          </Typography.Title>
          <div style={{ display: 'grid', gap: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('estado do backend', 'backend state')}
              </Typography.Text>
              {renderStatusTag(backendState, {
                ready: 'success',
                pending: 'warning',
                invalid: 'error',
              })}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('status do pipeline', 'pipeline status')}
              </Typography.Text>
              {renderStatusTag(runbookSnapshot.pipelineStatus, runbookStatusToneMap)}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('decisão técnica oficial', 'official technical decision')}
              </Typography.Text>
              {renderStatusTag(officialDecisionValue, decisionToneMap)}
            </div>
            <Typography.Text className={styles.neoLabel}>
              {t('código da decisão: {code}', 'decision code: {code}', {
                code: officialDecisionCode || '-',
              })}
            </Typography.Text>
            <Typography.Text className={styles.neoLabel}>
              {t('motivos da decisão: {summary}', 'decision reasons: {summary}', {
                summary: sanitizeSensitiveText(officialDecisionSummary),
              })}
            </Typography.Text>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('coerência UI x pipeline', 'UI x pipeline coherence')}
              </Typography.Text>
              {renderStatusTag(runbookSnapshot.coherence.status, snapshotCoherenceToneMap)}
            </div>
            <Typography.Text className={styles.neoLabel}>
              {t('última atualização UTC: {value}', 'last update UTC: {value}', {
                value: formatUtcTimestamp(runbookSnapshot.lastUpdatedAt),
              })}
            </Typography.Text>
          </div>
        </div>
      </div>

      <div className={styles.neoCard}>
        <Typography.Title level={4} className={styles.neoCardTitle}>
          {t('Etapas e checkpoints do pipeline', 'Pipeline stages and checkpoints')}
        </Typography.Title>
        <Table
          rowKey="stageKey"
          columns={stageColumns}
          dataSource={stageRows}
          pagination={false}
          size="small"
          scroll={{ x: 980, y: 260 }}
        />
      </div>

      <div className={styles.neoGrid2}>
        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Diagnóstico técnico da execução', 'Technical execution diagnostics')}
          </Typography.Title>
          {!latestFailure ? (
            <Alert
              showIcon
              type="info"
              message={t('Nenhuma falha registrada', 'No recorded failures')}
              description={t(
                'A timeline registrará bloqueios/falhas transitórias e críticas quando ocorrerem.',
                'The timeline will register transient and critical blocks/failures when they occur.'
              )}
            />
          ) : (
            <div style={{ display: 'grid', gap: 8 }}>
              <Typography.Text className={styles.neoLabel}>
                {t('código', 'code')}: <Typography.Text code>{latestFailure.code}</Typography.Text>
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('mensagem', 'message')}: {sanitizeSensitiveText(latestFailure.message)}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('causa', 'cause')}: {sanitizeSensitiveText(latestFailure.cause)}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('componente', 'component')}: {sanitizeSensitiveText(latestFailure.component) || '-'}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('impacto', 'impact')}: {sanitizeSensitiveText(latestFailure.impact) || '-'}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('ação recomendada', 'recommended action')}:{' '}
                {sanitizeSensitiveText(latestFailure.recommendedAction)}
              </Typography.Text>
            </div>
          )}
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Contexto de retomada', 'Resume context')}
          </Typography.Title>
          {!resumeContext ? (
            <Alert
              showIcon
              type="info"
              message={t('Sem checkpoint de retomada', 'No resume checkpoint')}
              description={t(
                'Inicie o runbook para habilitar cálculo de retomada por checkpoint válido.',
                'Start the runbook to enable resume calculation by valid checkpoint.'
              )}
            />
          ) : (
            <div style={{ display: 'grid', gap: 8 }}>
              <Typography.Text className={styles.neoLabel}>
                {t('modo de execução oficial: {mode}', 'official execution mode: {mode}', {
                  mode: executionModeLabel,
                })}
              </Typography.Text>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>
                  {t('semântica idempotente', 'idempotent semantics')}
                </Typography.Text>
                {renderStatusTag(executionMode, executionModeToneMap)}
              </div>
              <Typography.Text className={styles.neoLabel}>
                {t('estratégia de retomada: {strategy}', 'resume strategy: {strategy}', {
                  strategy: resumeStrategyLabel,
                })}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('lock de escopo: {summary}', 'scope lock: {summary}', {
                  summary: scopeLockSummary,
                })}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('etapa corrente: {stage}', 'current stage: {stage}', {
                  stage: `${resumeContext.stageOrder}. ${resumeContext.stageLabel}`,
                })}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('checkpoint de retomada: {value}', 'resume checkpoint: {value}', {
                  value: resumeContext.resumeCheckpointLabel
                    ? `${resumeContext.resumeCheckpointOrder}. ${resumeContext.resumeCheckpointLabel}`
                    : t(
                        'etapa corrente sem checkpoint pendente',
                        'current stage without a pending checkpoint'
                      ),
                })}
              </Typography.Text>
              <Typography.Text className={styles.neoLabel}>
                {t('evidências já produzidas: {count}', 'evidence already produced: {count}', {
                  count: resumeContext.evidenceCount,
                })}
              </Typography.Text>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const renderAssistantStepContent = () => {
    const stepKey = assistantStep ? assistantStep.key : runbookAssistantSteps[0].key;

    if (RUNBOOK_MONITORING_STEP_KEYS.has(stepKey)) {
      return renderMonitoringPanels();
    }

    switch (stepKey) {
      case 'scope':
        return (
          <div style={{ display: 'grid', gap: 12 }}>
            <div className={styles.neoCard}>
              <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
                <Typography.Title level={4} className={styles.neoCardTitle}>
                  {t('Escopo e backend', 'Scope and backend')}
                </Typography.Title>
                {scopeStepApproved ? (
                  <Tag color="green">{t('Passo 1 aprovado', 'Step 1 approved')}</Tag>
                ) : (
                  <Button type="primary" onClick={handleApproveScopeStep}>
                    {t('Aprovar passo 1', 'Approve step 1')}
                  </Button>
                )}
              </Space>
              <Typography.Paragraph className={styles.neoLabel}>
                {t(
                  'A execução da Entrega 1 é restrita a provider `external-linux` (sem conectores proprietários).',
                  'Delivery 1 execution is restricted to the `external-linux` provider (without proprietary connectors).'
                )}
              </Typography.Paragraph>
              <div className={styles.formGrid2}>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('provedor', 'provider')}
                  </Typography.Text>
                  <Select value={providerKey} options={providerOptions} disabled />
                </div>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('estado backend do runbook', 'runbook backend state')}
                  </Typography.Text>
                  <Select value={backendState} options={backendStateOptions} disabled />
                </div>
              </div>
              {screen.backendNote && (
                <Alert
                  showIcon
                  type="info"
                  style={{ marginTop: 12 }}
                  message={t('Observação de integração', 'Integration note')}
                  description={t(
                    screen.backendNote,
                    'The base runbook execution is available through dedicated orchestrator endpoints, with stage/checkpoint control and run_id correlation.'
                  )}
                />
              )}
              {scopeStepApproved && (
                <Alert
                  showIcon
                  type="success"
                  style={{ marginTop: 12 }}
                  message={t('Passo 1 aprovado', 'Step 1 approved')}
                  description={t(
                    'Escopo e backend confirmados para sequência do fluxo assistido.',
                    'Scope and backend confirmed for the assisted flow sequence.'
                  )}
                />
              )}
            </div>
          </div>
        );

      case 'preconditions':
        return (
          <div className={styles.neoCard}>
            <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Pré-condições de início', 'Startup preconditions')}
              </Typography.Title>
              {preconditionsStepApproved ? (
                <Tag color="green">{t('Passo 2 aprovado', 'Step 2 approved')}</Tag>
              ) : (
                <Button
                  type="primary"
                  onClick={handleApprovePreconditionsStep}
                  disabled={!scopeStepApproved || !startPreconditions.allowStart}
                >
                  {t('Aprovar passo 2', 'Approve step 2')}
                </Button>
              )}
            </Space>
            <div style={{ display: 'grid', gap: 10 }}>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>change_id</Typography.Text>
                <Input
                  value={changeId}
                  onChange={event => setChangeId(event.target.value)}
                  placeholder="cr-2026-02-16-003"
                  disabled={lockManualPreconditions}
                />
              </div>
              <div className={styles.formGrid2}>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('run_id oficial', 'official run_id')}
                  </Typography.Text>
                  <Input
                    value={officialRunIdLabel}
                    placeholder={t('run_id oficial', 'official run_id')}
                    disabled
                  />
                </div>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    manifest_fingerprint
                  </Typography.Text>
                  <Input
                    value={officialEntryGateContext.manifestFingerprint || ''}
                    placeholder="manifest_fingerprint oficial"
                    disabled
                  />
                </div>
              </div>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>
                  source_blueprint_fingerprint
                </Typography.Text>
                <Input
                  value={officialEntryGateContext.sourceBlueprintFingerprint || ''}
                  placeholder="source_blueprint_fingerprint oficial"
                  disabled
                />
              </div>
              <div className={styles.formGrid2}>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('versão blueprint', 'blueprint version')}
                  </Typography.Text>
                  <Select
                    value={selectedBlueprintVersion}
                    options={blueprintCatalog.map(blueprint => ({
                      value: blueprint.value,
                      label: `${blueprint.label} (${
                        blueprint.lintValid ? 'validado' : 'bloqueado'
                      })`,
                    }))}
                    onChange={value => setSelectedBlueprintVersion(value)}
                    disabled={lockManualPreconditions}
                  />
                </div>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    perfil de ambiente
                  </Typography.Text>
                  <Select
                    value={environmentProfile}
                    options={environmentProfileOptions}
                    onChange={value => setEnvironmentProfile(value)}
                    disabled={lockManualPreconditions}
                  />
                </div>
              </div>
              <div style={{ display: 'grid', gap: 8 }}>
                <Space>
                  <Switch
                    checked={pipelineEvidenceReady}
                    onChange={value => setPipelineEvidenceReady(value)}
                    disabled={lockManualPreconditions}
                  />
                  <Typography.Text className={styles.neoLabel}>
                    {t(
                      'Evidências mínimas do A2.2 confirmadas no backend oficial',
                      'Minimum A2.2 evidence confirmed in the official backend'
                    )}
                  </Typography.Text>
                </Space>
                <Space>
                  <Switch
                    checked={cryptoBaselineReady}
                    onChange={value => setCryptoBaselineReady(value)}
                    disabled={lockManualPreconditions}
                  />
                  <Typography.Text className={styles.neoLabel}>
                    {t(
                      'Baseline criptográfico oficial apto para continuidade operacional',
                      'Official cryptographic baseline ready for operational continuity'
                    )}
                  </Typography.Text>
                </Space>
                <Space>
                  <Switch
                    checked={preflightApproved}
                    onChange={value => setPreflightApproved(value)}
                    disabled={lockManualPreconditions}
                  />
                  <Typography.Text className={styles.neoLabel}>
                    {t(
                      'Preflight SSH aprovado (todos os hosts aptos)',
                      'SSH preflight approved (all hosts ready)'
                    )}
                  </Typography.Text>
                </Space>
              </div>
              <Alert
                showIcon
                type={
                  a25GateReadinessStatus === READINESS_STATUS.implemented ? 'success' : 'warning'
                }
                message={t('Gate A2.5 por contexto oficial', 'A2.5 gate by official context')}
                description={a25GateSummary}
              />
              {a25GateReasons.length > 0 && (
                <Alert
                  showIcon
                  type="error"
                  message={t('Bloqueios ativos no gate A2.5', 'Active blocks in the A2.5 gate')}
                  description={a25GateReasons.join(' ')}
                />
              )}
              {!startPreconditions.allowStart && (
                <Alert
                  showIcon
                  type="warning"
                  message={t('Pré-condições pendentes para iniciar', 'Pending preconditions to start')}
                  description={startPreconditions.reasons.map(reason => reason.message).join(' ')}
                />
              )}
              {preconditionsStepApproved && startPreconditions.allowStart && (
                <Alert
                  showIcon
                  type="success"
                  message={t('Passo 2 aprovado', 'Step 2 approved')}
                  description={t(
                    'Pré-condições válidas e controles operacionais liberados.',
                    'Valid preconditions and operational controls enabled.'
                  )}
                />
              )}
            </div>
          </div>
        );
      default:
        return renderMonitoringPanels();
    }
  };

  return (
    <OperationalWindowManagerProvider>
      <NeoOpsLayout
      screenKey="e1-provisionamento"
      sectionLabel={PROVISIONING_SECTION_LABEL}
      title={t('Execução Assistida de Provisão', 'Assisted Provisioning Execution')}
      subtitle={t(
        'Conduzir o operador na execução guiada do runbook oficial, com checkpoints, observabilidade e retomada segura.',
        'Guide the operator through the official guided runbook execution, with checkpoints, observability, and safe resume.'
      )}
      navItems={provisioningNavItems}
      activeNavKey={resolveProvisioningActiveNavKey('e1-provisionamento')}
      breadcrumbs={getProvisioningBreadcrumbs('e1-provisionamento')}
    >
      <div className={runbookStyles.runbookPage}>
        <div className={styles.content}>
          <div className={styles.neoCard}>
            <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
              <div>
                <Typography.Text className={styles.neoLabel}>
                  {t('Cockpit guiado', 'Guided cockpit')}
                </Typography.Text>
                <Typography.Title level={4} className={styles.neoCardTitle}>
                  {cockpitCardTitle}
                </Typography.Title>
                <Typography.Paragraph className={styles.neoLabel} style={{ marginBottom: 0 }}>
                  {cockpitCardDescription}
                </Typography.Paragraph>
                {hasPendingOnboardingProvisioning && (
                  <div className={styles.chipRow} style={{ marginTop: 8 }}>
                    <span className={styles.chip}>{`run_id planejado: ${plannedOrActiveRunId ||
                      '-'}`}</span>
                    <span className={styles.chip}>{`change_id: ${changeId || '-'}`}</span>
                    <span
                      className={styles.chip}
                    >
                      {t('ambiente: {value}', 'environment: {value}', {
                        value: onboardingProvisioningSummary.environmentProfile || '-',
                      })}
                    </span>
                    <span
                      className={styles.chip}
                    >{`hosts: ${onboardingProvisioningSummary.hostCount}`}</span>
                    <span
                      className={styles.chip}
                    >
                      {t('organizações: {value}', 'organizations: {value}', {
                        value: onboardingProvisioningSummary.organizationCount,
                      })}
                    </span>
                    <span
                      className={styles.chip}
                    >{`nodes: ${onboardingProvisioningSummary.nodeCount}`}</span>
                    <span
                      className={styles.chip}
                    >{`APIs: ${onboardingProvisioningSummary.apiCount}`}</span>
                  </div>
                )}
              </div>
              <Space wrap>
                <Button
                  type="primary"
                  icon={<ArrowRightOutlined />}
                  onClick={
                    hasPendingOnboardingProvisioning
                      ? handleContinueProvisioning
                      : openAssistantDialog
                  }
                >
                  {cockpitPrimaryButtonLabel}
                </Button>
                {isAuditOnlyMode && (
                  <Button onClick={handleStartNewProvisioning}>
                    {t('Novo provisionamento', 'New provisioning')}
                  </Button>
                )}
              </Space>
            </Space>
          </div>

          <div className={styles.neoCard}>
            <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
              <div>
                <Typography.Text className={styles.neoLabel}>
                  {t('Gate de entrada A2.5', 'A2.5 entry gate')}
                </Typography.Text>
                <Typography.Title level={4} className={styles.neoCardTitle}>
                  {t(
                    'Contexto oficial obrigatório para ações críticas',
                    'Official context required for critical actions'
                  )}
                </Typography.Title>
                <Typography.Paragraph className={styles.neoLabel} style={{ marginBottom: 0 }}>
                  {a25GateSummary}
                </Typography.Paragraph>
              </div>
              {renderStatusTag(a25GateReadinessStatus, READINESS_STATUS_TONE_MAP)}
            </Space>

            <div className={styles.chipRow} style={{ marginTop: 8 }}>
              <span className={styles.chip}>{`change_id: ${officialEntryGateContext.changeId ||
                '-'}`}</span>
              <span className={styles.chip}>{`run_id: ${officialRunIdLabel}`}</span>
              <span
                className={styles.chip}
              >{`manifest_fingerprint: ${officialEntryGateContext.manifestFingerprint ||
                '-'}`}</span>
              <span
                className={styles.chip}
              >{`source_blueprint_fingerprint: ${officialEntryGateContext.sourceBlueprintFingerprint ||
                '-'}`}</span>
              <span
                className={styles.chip}
              >{`artefatos A2.2: ${officialEntryGateContext.availableArtifacts.length}/${officialEntryGateContext.requiredArtifacts.length}`}</span>
              <span
                className={styles.chip}
              >{`handoff_fingerprint: ${officialEntryGateContext.handoffFingerprint || '-'}`}</span>
              <span
                className={styles.chip}
              >{`handoff_contract: ${officialEntryGateContext.handoffContractVersion ||
                '-'}`}</span>
              <span className={styles.chip}>{`backend: ${backendState}`}</span>
              <span
                className={styles.chip}
              >{`decisão oficial: ${officialDecisionValue} (${officialDecisionCode || '-'})`}</span>
            </div>

            {a25GateReasons.length > 0 && (
              <Alert
                showIcon
                type="warning"
                style={{ marginTop: 12 }}
                message={t('Gate A2.5 bloqueante ativo', 'Blocking A2.5 gate active')}
                description={a25GateReasons.join(' ')}
              />
            )}

            {IS_PROVISIONING_OPERATIONAL_ENV && (
              <Alert
                showIcon
                type="info"
                style={{ marginTop: 12 }}
                message={t('Ambiente operacional (hml/prod)', 'Operational environment (hml/prod)')}
                description={t(
                  'Fallback local/degradado permanece desabilitado por política para preservar semântica oficial de execução.',
                  'Local/degraded fallback remains disabled by policy to preserve the official execution semantics.'
                )}
              />
            )}
          </div>

          <OperationalWindowDialog
            windowId="provisioning-runbook-assistant"
            title={t(
              'Execução Assistida de Provisão · passo a passo',
              'Assisted Provisioning Execution · step by step'
            )}
            eyebrow={t('Workspace de runbook', 'Runbook workspace')}
            open={isAssistantDialogOpen}
            onClose={() => {
              if (isAdvancingCheckpoint) {
                return;
              }
              setIsAssistantDialogOpen(false);
            }}
            preferredWidth="96vw"
            preferredHeight="82vh"
          >
            {isAdvancingCheckpoint && (
              <div className={runbookStyles.checkpointLoadingOverlay}>
                <Spin size="large" />
                <div>
                  <Typography.Text strong>
                    {t('Executando avanço de checkpoint...', 'Advancing checkpoint...')}
                  </Typography.Text>
                  <br />
                  <Typography.Text type="secondary">
                    {t(
                      'Aguarde a confirmação do backend do orquestrador.',
                      'Wait for confirmation from the orchestrator backend.'
                    )}
                  </Typography.Text>
                </div>
              </div>
            )}
            <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
              <Space wrap size={6}>
                {runbookAssistantSteps.map((step, stepIndex) => {
                  const stepLockedByAuditMode =
                    isAuditOnlyMode && stepIndex < runbookMonitoringStepStartIndex;
                  return (
                    <Tag
                      key={step.key}
                      color={stepIndex === assistantStepIndex ? 'blue' : 'default'}
                      style={{
                        cursor: stepLockedByAuditMode ? 'not-allowed' : 'pointer',
                        opacity: stepLockedByAuditMode ? 0.45 : 1,
                      }}
                      onClick={() => {
                        if (stepLockedByAuditMode) {
                          return;
                        }
                        setAssistantStepIndex(stepIndex);
                      }}
                    >
                      {t('Passo {step}: {title}', 'Step {step}: {title}', {
                        step: stepIndex + 1,
                        title: step.title,
                      })}
                    </Tag>
                  );
                })}
              </Space>
              <Tag color="blue">
                {t('Passo {current}/{total}', 'Step {current}/{total}', {
                  current: assistantStepIndex + 1,
                  total: runbookAssistantSteps.length,
                })}
              </Tag>
            </Space>

            {isAuditOnlyMode && (
              <Alert
                showIcon
                type="info"
                style={{ marginTop: 8 }}
                message={t('Modo auditoria ativo', 'Audit mode active')}
                description={t(
                  'Após conclusão técnica, o passo a passo permanece disponível para auditoria dos sinais operacionais, checkpoints, evidências e decisão oficial allow|block.',
                  'After the technical completion, the step-by-step flow remains available for auditing the operational signals, checkpoints, evidence, and official allow|block decision.'
                )}
              />
            )}

            <div style={{ marginTop: 12 }}>
              {isMonitoringStep ? renderMonitoringPanels() : renderAssistantStepContent()}
            </div>

            <div
              className={`${styles.neoCard} ${runbookStyles.modalControlCard}`}
              style={{ marginTop: 12 }}
            >
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Controles de execução e observabilidade', 'Execution and observability controls')}
              </Typography.Title>
              {!controlsUnlocked ? (
                <Alert
                  showIcon
                  type="warning"
                  style={{ marginBottom: 8 }}
                  message={t(
                    'Controles bloqueados até aprovação de Passo 1 e Passo 2',
                    'Controls blocked until Step 1 and Step 2 approval'
                  )}
                  description={`${controlsLockReason} Status: Passo 1 ${
                    scopeStepApproved ? t('aprovado', 'approved') : t('pendente', 'pending')
                  } | Passo 2 ${
                    preconditionsStepApproved ? t('aprovado', 'approved') : t('pendente', 'pending')
                  }.`}
                />
              ) : (
                <>
                  <Space wrap size={6} style={{ marginBottom: 8 }}>
                    <Button
                      size="small"
                      type={showEvidencePanel ? 'primary' : 'default'}
                      disabled={!controlsUnlocked || isAdvancingCheckpoint}
                      onClick={() => toggleLogPanel('evidence')}
                    >
                      {t('Evidências produzidas', 'Produced evidence')}
                    </Button>
                    <Button
                      size="small"
                      type={showTimelinePanel ? 'primary' : 'default'}
                      disabled={!controlsUnlocked || isAdvancingCheckpoint}
                      onClick={() => toggleLogPanel('timeline')}
                    >
                      {t('Timeline operacional', 'Operational timeline')}
                    </Button>
                  </Space>
                  {officialStageBlocked && (
                    <Alert
                      showIcon
                      type={blockedByFailedStage && !blockedByCoherence ? 'warning' : 'error'}
                      style={{ marginBottom: 8 }}
                      message={t('Etapa oficial bloqueada', 'Official stage blocked')}
                      description={`${officialStageBlockReason}. ${
                        blockedByFailedStage && !blockedByCoherence
                          ? t(
                              'Somente retry permanece habilitado para retomada idempotente.',
                              'Only retry remains enabled for idempotent resume.'
                            )
                          : t(
                              'Nenhum comando operacional pode seguir até normalização oficial.',
                              'No operational command may proceed until official normalization.'
                            )
                      }`}
                    />
                  )}
                  <div className={runbookStyles.modalCommandGrid}>
                    <Tooltip
                      title={buildControlTooltip(startActionReadiness, {
                        respectStageBlock: false,
                      })}
                    >
                      <span>
                        <Button
                          type="primary"
                          icon={<PlayCircleOutlined />}
                          onClick={handleStart}
                          disabled={disableStartCommand}
                          className={runbookStyles.commandDockButton}
                        >
                          {t('Iniciar runbook', 'Start runbook')}
                        </Button>
                      </span>
                    </Tooltip>
                    <Tooltip title={buildControlTooltip(pauseActionReadiness)}>
                      <span>
                        <Button
                          icon={<PauseCircleOutlined />}
                          onClick={handlePause}
                          disabled={disablePauseCommand}
                          className={runbookStyles.commandDockButton}
                        >
                          {t('Pausar', 'Pause')}
                        </Button>
                      </span>
                    </Tooltip>
                    <Tooltip title={buildControlTooltip(resumeActionReadiness)}>
                      <span>
                        <Button
                          icon={<ReloadOutlined />}
                          onClick={handleResume}
                          disabled={disableResumeCommand}
                          className={runbookStyles.commandDockButton}
                        >
                          {t('Retomar', 'Resume')}
                        </Button>
                      </span>
                    </Tooltip>
                    <Tooltip title={buildControlTooltip(advanceActionReadiness)}>
                      <span>
                        <Button
                          icon={<StepForwardOutlined />}
                          onClick={handleAdvance}
                          loading={isAdvancingCheckpoint}
                          disabled={disableAdvanceCommand}
                          className={runbookStyles.commandDockButton}
                        >
                          {t('Avancar checkpoint', 'Advance checkpoint')}
                        </Button>
                      </span>
                    </Tooltip>
                    <Tooltip
                      title={buildControlTooltip(retryActionReadiness, {
                        allowFailedStage: true,
                      })}
                    >
                      <span>
                        <Button
                          icon={<CheckCircleOutlined />}
                          onClick={handleRetry}
                          disabled={disableRetryCommand}
                          className={runbookStyles.commandDockButton}
                        >
                          {t('Reexecucao segura', 'Safe re-execution')}
                        </Button>
                      </span>
                    </Tooltip>
                  </div>
                </>
              )}
            </div>

            {shouldRenderLogDock && (
              <div
                className={runbookStyles.modalLogDock}
                style={{
                  gridTemplateColumns: logDockGridTemplateColumns,
                  minHeight: expandedLogPanel ? 300 : 220,
                }}
              >
                {showEvidencePanel && (
                  <div className={runbookStyles.logPane}>
                    <div className={runbookStyles.logPaneHeader}>
                      <Typography.Title level={5} className={runbookStyles.logPaneTitle}>
                        {t(
                          'Evidências produzidas para retomada segura',
                          'Produced evidence for safe resume'
                        )}
                      </Typography.Title>
                      <Button
                        size="small"
                        onClick={() => toggleExpandedLogPanel('evidence')}
                        className={runbookStyles.logPaneAction}
                        disabled={isAdvancingCheckpoint}
                      >
                        {expandedLogPanel === 'evidence'
                          ? t('Reduzir', 'Collapse')
                          : t('Expandir', 'Expand')}
                      </Button>
                    </div>
                    <div className={runbookStyles.logPaneBody}>
                      {producedEvidenceRows.length === 0 ? (
                        <Alert
                          showIcon
                          type="warning"
                          message={t(
                            'Nenhuma evidência produzida até o momento',
                            'No evidence produced yet'
                          )}
                          description={t(
                            'Evidências serão listadas conforme checkpoints forem concluídos.',
                            'Evidence will be listed as checkpoints are completed.'
                          )}
                        />
                      ) : (
                        <Table
                          rowKey="key"
                          className={runbookStyles.compactLogTable}
                          columns={evidenceColumns}
                          dataSource={producedEvidenceRows}
                          pagination={false}
                          size="small"
                          tableLayout="fixed"
                          scroll={{ x: 980, y: expandedLogPanel === 'evidence' ? 280 : 200 }}
                        />
                      )}
                    </div>
                  </div>
                )}

                {showTimelinePanel && (
                  <div className={runbookStyles.logPane}>
                    <div className={runbookStyles.logPaneHeader}>
                      <Typography.Title level={5} className={runbookStyles.logPaneTitle}>
                        {t('Timeline operacional', 'Operational timeline')}
                      </Typography.Title>
                      <Button
                        size="small"
                        onClick={() => toggleExpandedLogPanel('timeline')}
                        className={runbookStyles.logPaneAction}
                        disabled={isAdvancingCheckpoint}
                      >
                        {expandedLogPanel === 'timeline'
                          ? t('Reduzir', 'Collapse')
                          : t('Expandir', 'Expand')}
                      </Button>
                    </div>
                    <div className={runbookStyles.logPaneBody}>
                      {eventRows.length === 0 ? (
                        <Alert
                          showIcon
                          type="info"
                          message={t('Nenhum evento registrado', 'No recorded events')}
                          description={t(
                            'Inicie o runbook para registrar eventos por etapa/checkpoint.',
                            'Start the runbook to register events by stage/checkpoint.'
                          )}
                        />
                      ) : (
                        <Table
                          rowKey="key"
                          className={runbookStyles.compactLogTable}
                          columns={eventColumns}
                          dataSource={eventRows}
                          pagination={false}
                          size="small"
                          tableLayout="fixed"
                          scroll={{ x: 1900, y: expandedLogPanel === 'timeline' ? 280 : 200 }}
                        />
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}

            <div className={styles.windowWorkspaceFooter}>
              <div className={runbookStyles.assistantFooterNav}>
                <Space wrap>
                  <Button
                    icon={<ArrowLeftOutlined />}
                    disabled={isFirstAssistantStep || isAdvancingCheckpoint}
                    onClick={() => moveAssistantStep(-1)}
                  >
                    {t('Anterior', 'Previous')}
                  </Button>
                  <Button
                    icon={<ArrowRightOutlined />}
                    disabled={isLastAssistantStep || isAdvancingCheckpoint}
                    onClick={() => moveAssistantStep(1)}
                  >
                    {t('Avançar', 'Next')}
                  </Button>
                </Space>

                <div className={styles.windowWorkspaceFooterActions}>
                  <Button
                    type="primary"
                    disabled={isAdvancingCheckpoint}
                    onClick={() => setIsAssistantDialogOpen(false)}
                  >
                    {isAuditOnlyMode
                      ? t('Encerrar auditoria', 'Close audit')
                      : t('Encerrar passo a passo', 'Close step by step')}
                  </Button>
                </div>
              </div>
            </div>
          </OperationalWindowDialog>

          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Histórico de provisionamentos (auditoria)', 'Provisioning history (audit)')}
            </Typography.Title>
            {provisioningHistory.length === 0 ? (
              <Alert
                showIcon
                type="info"
                message={t('Nenhum provisionamento concluído no histórico', 'No completed provisioning in history')}
                description={t(
                  'Ao concluir um runbook, o snapshot de execução é salvo aqui para auditoria expansível.',
                  'When a runbook completes, the execution snapshot is saved here for expandable auditing.'
                )}
              />
            ) : (
              <Collapse ghost expandIconPosition="right">
                {provisioningHistory.map(historyEntry => {
                  const historyState = historyEntry.executionState;
                  const historyContext =
                    historyEntry.context && typeof historyEntry.context === 'object'
                      ? historyEntry.context
                      : buildProvisioningContextSummary({ snapshot: historyState });
                  const historyLevel = resolveProvisioningHistoryLevel(historyState);
                  const historyOrganizations = Array.isArray(historyContext.organizations)
                    ? historyContext.organizations
                    : [];
                  const historyTopologyKnown =
                    historyOrganizations.length > 0 ||
                    toSafePositiveInt(historyContext.hostCount) > 0 ||
                    toSafePositiveInt(historyContext.organizationCount) > 0 ||
                    toSafePositiveInt(historyContext.nodeCount) > 0;
                  const historyTimelineStageOrderByKey = buildTimelineStageOrderByKey(
                    historyState.stages
                  );
                  const historyEventRows = buildTimelineRows({
                    events: historyState.events,
                    stageOrderByKey: historyTimelineStageOrderByKey,
                    defaultRunId: historyState.runId,
                    defaultChangeId: historyState.changeId,
                    localeCandidate: locale,
                  });
                  const historyEvidenceRows = buildRunbookProducedEvidenceRows(historyState);
                  const historyProgress = calculateRunbookProgress(historyState);
                  const historySnapshot = buildRunbookOfficialSnapshot(historyState, {
                    backendState: historyState.backendState || backendState,
                  });
                  return (
                    <CollapsePanel
                      key={historyEntry.key}
                      header={
                        <Space wrap>
                          <Typography.Text strong>{historyEntry.runId || '-'}</Typography.Text>
                          <Tag color={historyLevel.color}>{historyLevel.label}</Tag>
                          <Typography.Text type="secondary">
                            {t('status {status} | fim UTC {finishedAt}', 'status {status} | end UTC {finishedAt}', {
                              status: historyEntry.status || '-',
                              finishedAt: formatUtcTimestamp(historyEntry.finishedAt),
                            })}
                          </Typography.Text>
                        </Space>
                      }
                    >
                      <div style={{ display: 'grid', gap: 10 }}>
                        <div className={styles.chipRow}>
                          <span className={styles.chip}>{`change_id: ${historyEntry.changeId ||
                            '-'}`}</span>
                          <span className={styles.chip}>
                            {t('ambiente: {value}', 'environment: {value}', {
                              value: historyContext.environmentProfile || '-',
                            })}
                          </span>
                          <span className={styles.chip}>
                            {`provider: ${historyContext.providerKey || '-'}`}
                          </span>
                          <span className={styles.chip}>
                            {`hosts: ${
                              historyTopologyKnown
                                ? toSafePositiveInt(historyContext.hostCount)
                                : 'n/d'
                            }`}
                          </span>
                          <span className={styles.chip}>
                            {t('organizações: {value}', 'organizations: {value}', {
                              value: historyTopologyKnown
                                ? toSafePositiveInt(historyContext.organizationCount)
                                : t('n/d', 'n/a'),
                            })}
                          </span>
                          <span className={styles.chip}>
                            {`nodes: ${
                              historyTopologyKnown
                                ? toSafePositiveInt(historyContext.nodeCount)
                                : 'n/d'
                            }`}
                          </span>
                          <span className={styles.chip}>
                            {`APIs: ${toSafePositiveInt(historyContext.apiCount)}`}
                          </span>
                          <span className={styles.chip}>
                            {t('incrementais: {value}', 'incremental: {value}', {
                              value: toSafePositiveInt(historyContext.incrementalCount),
                            })}
                          </span>
                          <span className={styles.chip}>
                            {`pipeline: ${historySnapshot.pipelineStatus || '-'}`}
                          </span>
                          <span className={styles.chip}>
                            {t('coerência: {value}', 'coherence: {value}', {
                              value: historySnapshot.coherence.status || '-',
                            })}
                          </span>
                        </div>
                        <Space wrap>
                          <Button
                            type="primary"
                            size="small"
                            onClick={() => handleOpenHistoryAudit(historyEntry)}
                          >
                            {t('Abrir auditoria deste provisionamento', 'Open audit for this provisioning')}
                          </Button>
                        </Space>
                        <Progress
                          percent={historyProgress}
                          status={
                            historyState.status === RUNBOOK_EXECUTION_STATUS.failed
                              ? 'exception'
                              : 'success'
                          }
                        />
                        <Collapse expandIconPosition="right">
                          <CollapsePanel
                            key={`${historyEntry.key}-evidence`}
                            header={t(
                              'Evidências produzidas ({count})',
                              'Produced evidence ({count})',
                              { count: historyEvidenceRows.length }
                            )}
                          >
                            {historyEvidenceRows.length === 0 ? (
                              <Alert
                                showIcon
                                type="warning"
                                message={t('Sem evidências registradas', 'No recorded evidence')}
                                description={t(
                                  'Esta execução não registrou evidências produzidas.',
                                  'This execution did not register produced evidence.'
                                )}
                              />
                            ) : (
                              <Table
                                rowKey="key"
                                className={runbookStyles.compactLogTable}
                                columns={evidenceColumns}
                                dataSource={historyEvidenceRows}
                                pagination={false}
                                size="small"
                                tableLayout="fixed"
                                scroll={{ x: 980, y: 220 }}
                              />
                            )}
                          </CollapsePanel>
                          <CollapsePanel
                            key={`${historyEntry.key}-timeline`}
                            header={t(
                              'Timeline operacional ({count})',
                              'Operational timeline ({count})',
                              { count: historyEventRows.length }
                            )}
                          >
                            {historyEventRows.length === 0 ? (
                              <Alert
                                showIcon
                                type="info"
                                message={t('Sem eventos registrados', 'No recorded events')}
                                description={t(
                                  'Esta execução não registrou eventos operacionais na timeline.',
                                  'This execution did not register operational events in the timeline.'
                                )}
                              />
                            ) : (
                              <Table
                                rowKey="key"
                                className={runbookStyles.compactLogTable}
                                columns={eventColumns}
                                dataSource={historyEventRows}
                                pagination={false}
                                size="small"
                                tableLayout="fixed"
                                scroll={{ x: 1900, y: 220 }}
                              />
                            )}
                          </CollapsePanel>
                        </Collapse>
                      </div>
                    </CollapsePanel>
                  );
                })}
              </Collapse>
            )}
          </div>
        </div>
      </div>
    </NeoOpsLayout>
    </OperationalWindowManagerProvider>
  );
};

export default ProvisioningRunbookPage;
