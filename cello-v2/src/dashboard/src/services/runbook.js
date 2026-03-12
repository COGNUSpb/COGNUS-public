import request from '../utils/request';
import {
  isProvisioningLocalModeBlockedByPolicy,
  isProvisioningLocalModePermitted,
  recordProvisioningFallbackUsage,
} from './provisioningEnvironmentPolicy';
import { pickCognusText, resolveCognusLocale } from '../pages/Cognus/cognusI18n';
import { sanitizeSensitiveText } from '../utils/provisioningSecurityRedaction';

const RUNBOOK_LOCAL_MODE_FLAG = String(process.env.COGNUS_RUNBOOK_LOCAL_MODE || '')
  .trim()
  .toLowerCase();

export const RUNBOOK_LOCAL_MODE_FLAG_ENABLED =
  RUNBOOK_LOCAL_MODE_FLAG === '1' || RUNBOOK_LOCAL_MODE_FLAG === 'true';
export const RUNBOOK_LOCAL_MODE_ENABLED = isProvisioningLocalModePermitted(
  RUNBOOK_LOCAL_MODE_FLAG_ENABLED
);
export const RUNBOOK_LOCAL_MODE_BLOCKED_BY_POLICY = isProvisioningLocalModeBlockedByPolicy(
  RUNBOOK_LOCAL_MODE_FLAG_ENABLED
);
const RUNBOOK_LOCAL_STORAGE_KEY = 'cognus.runbooks.local.state.v1';
const RUNBOOK_OFFICIAL_STATUS_CACHE_STORAGE_KEY =
  'cognus.runbooks.official.status.cache.v1';
const RUNBOOK_OFFICIAL_STATUS_CACHE_MAX_ENTRIES = 25;
const RUNBOOK_AUDIT_HISTORY_STORAGE_KEY = 'cognus.provisioning.runbook.audit.history.v2';

const DEFAULT_STAGE_METADATA = Object.freeze({
  completionCriteria: [],
  evidenceArtifacts: [],
});

const STAGE_METADATA_BY_KEY = Object.freeze({
  prepare: Object.freeze({
    completionCriteria: ['preflight_ready', 'connectivity_ready'],
    evidenceArtifacts: ['stage-reports/prepare-report.json', 'execution-plan.json'],
  }),
  provision: Object.freeze({
    completionCriteria: ['hosts_ready', 'runtime_ready'],
    evidenceArtifacts: ['stage-reports/provision-report.json', 'runtime-inventory.json'],
  }),
  reconcile: Object.freeze({
    completionCriteria: ['baseline_ready', 'artifacts_ready'],
    evidenceArtifacts: ['stage-reports/reconcile-report.json', 'connection-profiles.json'],
  }),
  configure: Object.freeze({
    completionCriteria: ['baseline_ready', 'artifacts_ready'],
    evidenceArtifacts: ['stage-reports/configure-report.json', 'connection-profiles.json'],
  }),
  verify: Object.freeze({
    completionCriteria: ['health_checks_passed', 'evidence_ready'],
    evidenceArtifacts: ['stage-reports/verify-report.json', 'pipeline-report.json'],
  }),
});

const FORBIDDEN_PLAINTEXT_SECRET_KEYS = new Set([
  'private_key',
  'private_key_pem',
  'ssh_private_key',
  'ssh_key',
  'password',
  'passphrase',
]);

const SENSITIVE_PAYLOAD_KEYS = new Set(['credential_payload']);

const SENSITIVE_REFERENCE_KEYS = new Set([
  'private_key_ref',
  'vault_ref',
  'secret_ref',
  'credential_ref',
  'token_ref',
]);

const normalizeText = value => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim();
};

const localizeRunbookText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const normalizeStatus = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (!normalized) {
    return 'pending';
  }
  return normalized;
};

const normalizeBackendState = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === 'ready' || normalized === 'pending' || normalized === 'invalid') {
    return normalized;
  }
  return 'ready';
};

const resolveRunbookBackendCode = error =>
  normalizeText(
    error &&
      ((error.data &&
        (error.data.code ||
          error.data.msg ||
          error.data.message ||
          (error.data.details && error.data.details.code))) ||
        (error.response &&
          error.response.data &&
          (error.response.data.code ||
            error.response.data.msg ||
            error.response.data.message ||
            (error.response.data.data &&
              (error.response.data.data.code || error.response.data.data.message)))))
  ).toLowerCase();

const resolveRunbookBackendMessage = error =>
  normalizeText(
    error &&
      ((error.data &&
        (error.data.detail ||
          error.data.message ||
          error.data.msg ||
          (error.data.details && error.data.details.message))) ||
        error.message ||
        (error.response &&
          error.response.data &&
          (error.response.data.message ||
            error.response.data.msg ||
            (error.response.data.data && error.response.data.data.message))))
  );

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

const cloneSerializableObject = value => {
  if (!value || typeof value !== 'object') {
    return null;
  }

  try {
    return JSON.parse(JSON.stringify(value));
  } catch (error) {
    return null;
  }
};

const readOfficialStatusCacheRegistry = () => {
  const storage = getBrowserLocalStorage();
  if (!storage) {
    return {};
  }

  const rawPayload = storage.getItem(RUNBOOK_OFFICIAL_STATUS_CACHE_STORAGE_KEY);
  if (!rawPayload) {
    return {};
  }

  try {
    const parsedPayload = JSON.parse(rawPayload);
    return parsedPayload && typeof parsedPayload === 'object' && !Array.isArray(parsedPayload)
      ? parsedPayload
      : {};
  } catch (error) {
    storage.removeItem(RUNBOOK_OFFICIAL_STATUS_CACHE_STORAGE_KEY);
    return {};
  }
};

const persistOfficialStatusCacheRegistry = registry => {
  const storage = getBrowserLocalStorage();
  if (!storage) {
    return;
  }

  try {
    storage.setItem(
      RUNBOOK_OFFICIAL_STATUS_CACHE_STORAGE_KEY,
      JSON.stringify(registry && typeof registry === 'object' ? registry : {})
    );
  } catch (error) {
    // Ignore storage quota or privacy-mode failures: cache is opportunistic only.
  }
};

const readRunbookAuditHistoryEntries = () => {
  const storage = getBrowserLocalStorage();
  if (!storage) {
    return [];
  }

  const rawPayload = storage.getItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
  if (!rawPayload) {
    return [];
  }

  try {
    const parsedPayload = JSON.parse(rawPayload);
    return Array.isArray(parsedPayload) ? parsedPayload : [];
  } catch (error) {
    return [];
  }
};

const sortAuditHistoryEntries = entries =>
  (Array.isArray(entries) ? entries : []).slice().sort(
    (left, right) =>
      Date.parse(
        normalizeText(
          right &&
            (right.capturedAt || right.finishedAt || right.startedAt || right.updatedAt)
        )
      ) -
      Date.parse(
        normalizeText(
          left && (left.capturedAt || left.finishedAt || left.startedAt || left.updatedAt)
        )
      )
  );

const buildHistoricalFallbackRun = historyEntry => {
  const safeEntry = historyEntry && typeof historyEntry === 'object' ? historyEntry : null;
  const executionState =
    safeEntry && safeEntry.executionState && typeof safeEntry.executionState === 'object'
      ? safeEntry.executionState
      : null;
  if (!safeEntry || !executionState) {
    return null;
  }

  const safeContext = safeEntry.context && typeof safeEntry.context === 'object' ? safeEntry.context : {};
  const savedAtUtc = normalizeText(safeEntry.capturedAt || safeEntry.finishedAt || safeEntry.startedAt);
  const runId = normalizeText(safeEntry.runId || executionState.runId);
  if (!runId) {
    return null;
  }

  const stages = Array.isArray(executionState.stages)
    ? executionState.stages.map(stage => ({
        key: normalizeText(stage && stage.key),
        label: normalizeText(stage && stage.label),
        status: normalizeStatus(stage && stage.status),
        checkpoints: Array.isArray(stage && stage.checkpoints)
          ? stage.checkpoints.map(checkpoint => ({
              key: normalizeText(checkpoint && checkpoint.key),
              label: normalizeText(checkpoint && checkpoint.label),
              status: normalizeStatus(checkpoint && checkpoint.status),
              started_at_utc: normalizeText(
                checkpoint && (checkpoint.started_at_utc || checkpoint.startedAt)
              ),
              completed_at_utc: normalizeText(
                checkpoint && (checkpoint.completed_at_utc || checkpoint.completedAt)
              ),
            }))
          : [],
      }))
    : [];
  const events = Array.isArray(executionState.events)
    ? executionState.events.map(event => ({
        id: normalizeText(event && event.id),
        timestamp_utc: normalizeText(event && (event.timestamp_utc || event.timestamp)),
        level: normalizeText(event && event.level),
        code: normalizeText(event && event.code),
        message: normalizeText(event && event.message),
        run_id: normalizeText(event && (event.run_id || event.runId)) || runId,
        change_id:
          normalizeText(event && (event.change_id || event.changeId)) ||
          normalizeText(safeEntry.changeId || executionState.changeId),
        stage: normalizeText(event && event.stage),
        checkpoint: normalizeText(event && event.checkpoint),
        component: normalizeText(event && event.component),
        cause: normalizeText(event && event.cause),
        impact: normalizeText(event && event.impact),
        recommended_action: normalizeText(
          event && (event.recommended_action || event.recommendedAction)
        ),
      }))
    : [];

  return {
    run_id: runId,
    change_id: normalizeText(safeEntry.changeId || executionState.changeId),
    status: normalizeStatus(safeEntry.status || executionState.status),
    backend_state: normalizeBackendState(executionState.backendState),
    started_at_utc: normalizeText(safeEntry.startedAt || executionState.startedAt),
    finished_at_utc: normalizeText(safeEntry.finishedAt || executionState.finishedAt),
    updated_at_utc: savedAtUtc,
    manifest_fingerprint: normalizeText(executionState.manifestFingerprint),
    source_blueprint_fingerprint: normalizeText(executionState.sourceBlueprintFingerprint),
    provider_key: normalizeText(executionState.providerKey || safeContext.providerKey),
    environment_profile: normalizeText(
      executionState.environmentProfile || safeContext.environmentProfile
    ),
    stages,
    events,
    topology_catalog:
      safeContext.topology && typeof safeContext.topology === 'object' ? safeContext.topology : {},
    official_decision:
      executionState.officialDecision && typeof executionState.officialDecision === 'object'
        ? executionState.officialDecision
        : {},
    snapshot: {
      backend_state: normalizeBackendState(executionState.backendState),
      last_updated_at_utc: savedAtUtc,
      official_decision:
        executionState.officialDecision && typeof executionState.officialDecision === 'object'
          ? executionState.officialDecision
          : {},
    },
  };
};

const readHistoricalFallbackRun = runId => {
  const normalizedRunId = normalizeText(runId);
  if (!normalizedRunId) {
    return null;
  }

  const matchingEntry = sortAuditHistoryEntries(readRunbookAuditHistoryEntries()).find(entry => {
    return normalizeText(entry && entry.runId) === normalizedRunId;
  });

  return buildHistoricalFallbackRun(matchingEntry);
};

const writeCachedOfficialRun = run => {
  const safeRun = cloneSerializableObject(run);
  const runId = normalizeText(safeRun && safeRun.run_id);
  if (!safeRun || !runId) {
    return;
  }

  const currentRegistry = readOfficialStatusCacheRegistry();
  const previousEntry =
    currentRegistry[runId] && typeof currentRegistry[runId] === 'object'
      ? currentRegistry[runId]
      : {};
  const nextRegistry = {
    ...currentRegistry,
    [runId]: {
      ...previousEntry,
      saved_at_utc: new Date().toISOString(),
      run: safeRun,
      unavailable: null,
    },
  };
  const boundedEntries = Object.entries(nextRegistry)
    .filter(([, entry]) => entry && typeof entry === 'object' && entry.run)
    .sort(
      (left, right) =>
        Date.parse((right[1] && right[1].saved_at_utc) || '') -
        Date.parse((left[1] && left[1].saved_at_utc) || '')
    )
    .slice(0, RUNBOOK_OFFICIAL_STATUS_CACHE_MAX_ENTRIES);

  persistOfficialStatusCacheRegistry(
    boundedEntries.reduce((accumulator, [entryRunId, entryValue]) => {
      accumulator[entryRunId] = entryValue;
      return accumulator;
    }, {})
  );
};

const readCachedOfficialRun = runId => {
  const normalizedRunId = normalizeText(runId);
  if (!normalizedRunId) {
    return null;
  }

  const registry = readOfficialStatusCacheRegistry();
  const cachedEntry = registry[normalizedRunId];
  if (!cachedEntry || typeof cachedEntry !== 'object') {
    return null;
  }

  const clonedRun = cloneSerializableObject(cachedEntry.run);
  if (!clonedRun) {
    return null;
  }

  return {
    run: clonedRun,
    savedAtUtc: normalizeText(cachedEntry.saved_at_utc),
    unavailable:
      cachedEntry.unavailable && typeof cachedEntry.unavailable === 'object'
        ? {
            reasonCode: normalizeText(cachedEntry.unavailable.reason_code),
            reasonMessage: normalizeText(cachedEntry.unavailable.reason_message),
            markedAtUtc: normalizeText(cachedEntry.unavailable.marked_at_utc),
          }
        : null,
  };
};

const markCachedOfficialRunUnavailable = (runId, error) => {
  const normalizedRunId = normalizeText(runId);
  if (!normalizedRunId) {
    return;
  }

  const registry = readOfficialStatusCacheRegistry();
  const currentEntry =
    registry[normalizedRunId] && typeof registry[normalizedRunId] === 'object'
      ? registry[normalizedRunId]
      : {};

  registry[normalizedRunId] = {
    ...currentEntry,
    unavailable: {
      reason_code: resolveRunbookBackendCode(error),
      reason_message: resolveRunbookBackendMessage(error),
      marked_at_utc: new Date().toISOString(),
    },
  };

  persistOfficialStatusCacheRegistry(registry);
};

const shouldBypassOfficialRunRequest = cachedEntry =>
  Boolean(
    cachedEntry &&
      cachedEntry.run &&
      cachedEntry.unavailable &&
      cachedEntry.unavailable.reasonCode === 'runbook_not_found'
  );

const attachCachedStatusFallbackMetadata = (run, fallbackMetadata) => {
  const safeRun = cloneSerializableObject(run);
  if (!safeRun) {
    return null;
  }

  const fallback = {
    active: true,
    source: normalizeText(fallbackMetadata && fallbackMetadata.source) || 'official_status_cache',
    savedAtUtc: normalizeText(fallbackMetadata && fallbackMetadata.savedAtUtc),
    reasonCode: normalizeText(fallbackMetadata && fallbackMetadata.reasonCode),
    reasonMessage: normalizeText(fallbackMetadata && fallbackMetadata.reasonMessage),
  };

  safeRun.cached_status_fallback = fallback;
  if (safeRun.snapshot && typeof safeRun.snapshot === 'object' && !Array.isArray(safeRun.snapshot)) {
    safeRun.snapshot = {
      ...safeRun.snapshot,
      cached_status_fallback: fallback,
    };
  }

  return safeRun;
};

const RUNBOOK_RUNTIME_INSPECTION_SUPPORTED_SCOPES = Object.freeze([
  'docker_inspect',
  'docker_logs',
  'environment',
  'ports',
  'mounts',
]);

const normalizeRuntimeInspectionScope = value => {
  const normalized = normalizeText(value).toLowerCase() || 'all';
  if (normalized === 'all') {
    return normalized;
  }
  return RUNBOOK_RUNTIME_INSPECTION_SUPPORTED_SCOPES.includes(normalized) ? normalized : 'all';
};

const readRuntimeInspectionCacheStore = run => {
  const safeRun = run && typeof run === 'object' ? run : null;
  const snapshot =
    safeRun && safeRun.snapshot && typeof safeRun.snapshot === 'object' && !Array.isArray(safeRun.snapshot)
      ? safeRun.snapshot
      : null;
  const snapshotCache =
    snapshot &&
    snapshot.runtime_inspection_cache &&
    typeof snapshot.runtime_inspection_cache === 'object' &&
    !Array.isArray(snapshot.runtime_inspection_cache)
      ? snapshot.runtime_inspection_cache
      : null;
  if (snapshotCache) {
    return snapshotCache;
  }

  return safeRun &&
    safeRun.runtime_inspection_cache &&
    typeof safeRun.runtime_inspection_cache === 'object' &&
    !Array.isArray(safeRun.runtime_inspection_cache)
    ? safeRun.runtime_inspection_cache
    : {};
};

const resolveRuntimeInspectionComponentType = componentId => {
  const normalizedComponentId = normalizeText(componentId);
  if (!normalizedComponentId || !normalizedComponentId.includes(':')) {
    return '';
  }
  return normalizedComponentId.split(':')[0].trim().toLowerCase();
};

const resolveRuntimeInspectionComponentName = componentId => {
  const normalizedComponentId = normalizeText(componentId);
  if (!normalizedComponentId || !normalizedComponentId.includes(':')) {
    return normalizedComponentId;
  }
  return normalizedComponentId.split(':').slice(1).join(':').trim();
};

const normalizeRuntimeInspectionLookupToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');

const isRuntimeInspectionComponentMatch = (requestedComponentId, cacheRow) => {
  const requestedIdToken = normalizeRuntimeInspectionLookupToken(requestedComponentId);
  const requestedNameToken = normalizeRuntimeInspectionLookupToken(
    resolveRuntimeInspectionComponentName(requestedComponentId)
  );
  const cacheComponentId = normalizeText(cacheRow && cacheRow.component_id);
  const cacheIdToken = normalizeRuntimeInspectionLookupToken(cacheComponentId);
  const cacheNameToken = normalizeRuntimeInspectionLookupToken(
    resolveRuntimeInspectionComponentName(cacheComponentId)
  );

  if (!requestedIdToken) {
    return false;
  }

  return (
    requestedIdToken === cacheIdToken ||
    requestedIdToken === cacheNameToken ||
    (requestedNameToken && requestedNameToken === cacheIdToken) ||
    (requestedNameToken && requestedNameToken === cacheNameToken)
  );
};

const buildCachedRuntimeInspectionScopeEntry = cacheRow => {
  const safeCacheRow = cacheRow && typeof cacheRow === 'object' ? cacheRow : {};
  return {
    inspection_scope: normalizeText(safeCacheRow.inspection_scope),
    payload: cloneSerializableObject(safeCacheRow.payload) || {},
    cache: {
      cache_key: normalizeText(safeCacheRow.cache_key),
      ttl_seconds: 0,
      cache_hit: true,
      cache_miss: false,
      stale: true,
      refresh_requested_at: normalizeText(safeCacheRow.refresh_requested_at),
      refreshed_at:
        normalizeText(safeCacheRow.refreshed_at) ||
        normalizeText(safeCacheRow.last_successful_refreshed_at),
      inspection_source:
        normalizeText(safeCacheRow.inspection_source) || 'official_runtime_inspection_cache',
      collection_status: normalizeText(safeCacheRow.collection_status) || 'ready',
      payload_hash: normalizeText(safeCacheRow.payload_hash),
      last_error:
        cloneSerializableObject(safeCacheRow.last_error) ||
        (safeCacheRow.last_error && typeof safeCacheRow.last_error === 'object'
          ? safeCacheRow.last_error
          : {}),
    },
  };
};

const buildCachedRuntimeInspectionFallback = (
  cachedRunEntry,
  params = {},
  fallbackMetadata = {}
) => {
  const safeEntry = cachedRunEntry && typeof cachedRunEntry === 'object' ? cachedRunEntry : null;
  const cachedRun = safeEntry && safeEntry.run && typeof safeEntry.run === 'object' ? safeEntry.run : null;
  if (!cachedRun) {
    return null;
  }

  const cacheStore = readRuntimeInspectionCacheStore(cachedRun);
  const cacheRows = Object.values(cacheStore).filter(row => row && typeof row === 'object');
  if (cacheRows.length === 0) {
    return null;
  }

  const requestedComponentId = normalizeText(params.componentId || params.component_id).toLowerCase();
  if (!requestedComponentId) {
    return null;
  }

  const requestedOrgId = normalizeText(params.orgId || params.org_id).toLowerCase();
  const requestedHostId = normalizeText(params.hostId || params.host_id).toLowerCase();
  const requestedScope = normalizeRuntimeInspectionScope(
    params.inspectionScope || params.inspection_scope || 'all'
  );

  const matchingRows = cacheRows.filter(row => {
    if (!isRuntimeInspectionComponentMatch(requestedComponentId, row)) {
      return false;
    }

    const rowOrgId = normalizeText(row.org_id).toLowerCase();
    if (requestedOrgId && rowOrgId && rowOrgId !== requestedOrgId) {
      return false;
    }

    const rowHostId = normalizeText(row.host_id || row.host_ref || row.hostRef).toLowerCase();
    if (requestedHostId && rowHostId && rowHostId !== requestedHostId) {
      return false;
    }

    return true;
  });

  if (matchingRows.length === 0) {
    return null;
  }

  const scopeRows =
    requestedScope === 'all'
      ? matchingRows.filter(row =>
          RUNBOOK_RUNTIME_INSPECTION_SUPPORTED_SCOPES.includes(
            normalizeText(row.inspection_scope).toLowerCase()
          )
        )
      : matchingRows.filter(
          row => normalizeText(row.inspection_scope).toLowerCase() === requestedScope
        );

  if (scopeRows.length === 0) {
    return null;
  }

  const scopes = scopeRows.reduce((accumulator, row) => {
    const scopeKey = normalizeText(row.inspection_scope).toLowerCase();
    if (!scopeKey || accumulator[scopeKey]) {
      return accumulator;
    }

    accumulator[scopeKey] = buildCachedRuntimeInspectionScopeEntry(row);
    return accumulator;
  }, {});

  const dockerInspectScope = scopes.docker_inspect || null;
  const dockerLogsScope = scopes.docker_logs || null;
  const dockerInspectPayload =
    dockerInspectScope && dockerInspectScope.payload && typeof dockerInspectScope.payload === 'object'
      ? dockerInspectScope.payload
      : {};
  const dockerInspectState =
    dockerInspectPayload.state && typeof dockerInspectPayload.state === 'object'
      ? dockerInspectPayload.state
      : {};
  const dockerLogsPayload =
    dockerLogsScope && dockerLogsScope.payload && typeof dockerLogsScope.payload === 'object'
      ? dockerLogsScope.payload
      : {};
  const componentId = normalizeText(params.componentId || params.component_id);
  const issues = Object.values(scopes).reduce((accumulator, scopeEntry) => {
    const lastError =
      scopeEntry && scopeEntry.cache && scopeEntry.cache.last_error && typeof scopeEntry.cache.last_error === 'object'
        ? cloneSerializableObject(scopeEntry.cache.last_error)
        : null;
    if (lastError && Object.keys(lastError).length > 0) {
      accumulator.push(lastError);
    }
    return accumulator;
  }, []);

  return {
    contract_version: 'a2a-runtime-inspection-cache.v1',
    source_of_truth: 'official_runtime_inspection_cache_fallback',
    correlation: {
      run_id: normalizeText(cachedRun.run_id),
      change_id: normalizeText(cachedRun.change_id),
      manifest_fingerprint: normalizeText(cachedRun.manifest_fingerprint),
      source_blueprint_fingerprint: normalizeText(cachedRun.source_blueprint_fingerprint),
    },
    component: {
      component_id: componentId,
      component_type: resolveRuntimeInspectionComponentType(componentId),
      name: resolveRuntimeInspectionComponentName(componentId),
      container_name:
        normalizeText(dockerInspectPayload.container_name) ||
        normalizeText(dockerLogsPayload.container_name),
      image: normalizeText(dockerInspectPayload.image),
      platform: normalizeText(dockerInspectPayload.platform) || 'docker/linux',
      status: normalizeText(dockerInspectState.status),
      org_id: normalizeText(params.orgId || params.org_id),
      host_id:
        normalizeText(dockerInspectScope && dockerInspectScope.cache && dockerInspectScope.cache.host_id) ||
        normalizeText(scopeRows[0] && scopeRows[0].host_id) ||
        normalizeText(params.hostId || params.host_id),
      scope: '',
      criticality: '',
    },
    inspection_scope: requestedScope,
    stale: true,
    scopes,
    issues,
    cached_status_fallback: {
      active: true,
      source: normalizeText(fallbackMetadata.source) || 'official_status_cache',
      savedAtUtc: normalizeText(fallbackMetadata.savedAtUtc || (safeEntry && safeEntry.savedAtUtc)),
      reasonCode: normalizeText(fallbackMetadata.reasonCode),
      reasonMessage: normalizeText(fallbackMetadata.reasonMessage),
    },
  };
};

const buildRuntimeInspectionCacheRowFromScope = (inspection, scopeKey, scopeEntry) => {
  const safeInspection = inspection && typeof inspection === 'object' ? inspection : {};
  const safeScopeEntry = scopeEntry && typeof scopeEntry === 'object' ? scopeEntry : {};
  const safeCache =
    safeScopeEntry.cache && typeof safeScopeEntry.cache === 'object' ? safeScopeEntry.cache : {};
  const payload = cloneSerializableObject(safeScopeEntry.payload) || {};
  const component =
    safeInspection.component && typeof safeInspection.component === 'object'
      ? safeInspection.component
      : {};
  const cacheKey =
    normalizeText(safeCache.cache_key) ||
    `${normalizeText(safeInspection.correlation && safeInspection.correlation.run_id)}:${normalizeText(
      component.component_id
    )}:${scopeKey}`;

  return {
    cache_key: cacheKey,
    org_id: normalizeText(component.org_id),
    host_id: normalizeText(component.host_id),
    component_id: normalizeText(component.component_id),
    inspection_scope: scopeKey,
    refresh_requested_at: normalizeText(safeCache.refresh_requested_at),
    refreshed_at: normalizeText(safeCache.refreshed_at),
    last_successful_refreshed_at: normalizeText(safeCache.refreshed_at),
    inspection_source: normalizeText(safeCache.inspection_source),
    collection_status: normalizeText(safeCache.collection_status) || 'ready',
    payload_hash: normalizeText(safeCache.payload_hash),
    payload,
    last_error:
      cloneSerializableObject(safeCache.last_error) ||
      (safeCache.last_error && typeof safeCache.last_error === 'object' ? safeCache.last_error : {}),
  };
};

const persistCachedRuntimeInspection = (runId, inspection) => {
  const normalizedRunId = normalizeText(runId);
  const safeInspection = inspection && typeof inspection === 'object' ? inspection : null;
  if (!normalizedRunId || !safeInspection || !safeInspection.scopes) {
    return;
  }

  const cachedRunEntry = readCachedOfficialRun(normalizedRunId);
  if (!cachedRunEntry || !cachedRunEntry.run) {
    return;
  }

  const nextRun = cloneSerializableObject(cachedRunEntry.run);
  if (!nextRun) {
    return;
  }

  const nextCacheStore = {
    ...readRuntimeInspectionCacheStore(nextRun),
  };

  Object.entries(safeInspection.scopes).forEach(([scopeKey, scopeEntry]) => {
    const normalizedScopeKey = normalizeText(scopeKey).toLowerCase();
    if (!RUNBOOK_RUNTIME_INSPECTION_SUPPORTED_SCOPES.includes(normalizedScopeKey)) {
      return;
    }

    const cacheRow = buildRuntimeInspectionCacheRowFromScope(
      safeInspection,
      normalizedScopeKey,
      scopeEntry
    );
    nextCacheStore[cacheRow.cache_key] = cacheRow;
  });

  nextRun.runtime_inspection_cache = nextCacheStore;
  nextRun.snapshot = {
    ...(nextRun.snapshot && typeof nextRun.snapshot === 'object' && !Array.isArray(nextRun.snapshot)
      ? nextRun.snapshot
      : {}),
    runtime_inspection_cache: nextCacheStore,
  };

  writeCachedOfficialRun(nextRun);
};

const RUNTIME_INSPECTION_PREWARM_REQUIRED_SCOPES = Object.freeze([
  'docker_inspect',
  'docker_logs',
]);
const RUNTIME_INSPECTION_PREWARM_IN_FLIGHT = new Map();

const buildRuntimeInspectionPrewarmKey = (runId, componentId) =>
  `${normalizeText(runId)}::${normalizeText(componentId)}`;

const buildRuntimeInspectionCachedScopeSet = (run, componentId) => {
  const cacheRows = Object.values(readRuntimeInspectionCacheStore(run)).filter(
    row => row && typeof row === 'object'
  );

  return cacheRows.reduce((accumulator, row) => {
    if (!isRuntimeInspectionComponentMatch(componentId, row)) {
      return accumulator;
    }

    const scopeKey = normalizeText(row.inspection_scope).toLowerCase();
    if (scopeKey) {
      accumulator.add(scopeKey);
    }
    return accumulator;
  }, new Set());
};

const hasCachedRuntimeInspectionForComponent = (run, componentId) => {
  const cachedScopes = buildRuntimeInspectionCachedScopeSet(run, componentId);
  return RUNTIME_INSPECTION_PREWARM_REQUIRED_SCOPES.every(scopeKey => cachedScopes.has(scopeKey));
};

const resolveRuntimeInspectionPrewarmPriority = row => {
  const scopeWeight = normalizeText(row && row.scope).toLowerCase() === 'required' ? 100 : 0;
  const criticalityWeight =
    normalizeText(row && row.criticality).toLowerCase() === 'critical' ? 50 : 0;
  const status = normalizeText(row && row.status).toLowerCase();
  let statusWeight = 0;
  if (status === 'running') {
    statusWeight = 40;
  } else if (status === 'degraded') {
    statusWeight = 30;
  } else if (status === 'stopped') {
    statusWeight = 20;
  } else if (status === 'unknown') {
    statusWeight = 10;
  }

  return scopeWeight + criticalityWeight + statusWeight;
};

const sortRuntimeInspectionPrewarmRows = rows =>
  (Array.isArray(rows) ? rows : []).slice().sort((left, right) => {
    const leftPriority = resolveRuntimeInspectionPrewarmPriority(left);
    const rightPriority = resolveRuntimeInspectionPrewarmPriority(right);
    if (leftPriority !== rightPriority) {
      return rightPriority - leftPriority;
    }

    return normalizeText(left && left.componentId).localeCompare(
      normalizeText(right && right.componentId)
    );
  });

const prewarmRuntimeInspectionForRow = (runId, row) => {
  const normalizedRunId = normalizeText(runId);
  const componentId = normalizeText(row && row.componentId);
  const prewarmKey = buildRuntimeInspectionPrewarmKey(normalizedRunId, componentId);
  if (RUNTIME_INSPECTION_PREWARM_IN_FLIGHT.has(prewarmKey)) {
    return RUNTIME_INSPECTION_PREWARM_IN_FLIGHT.get(prewarmKey);
  }

  // eslint-disable-next-line no-use-before-define
  const prewarmPromise = getRunbookRuntimeInspection(normalizedRunId, {
    orgId: normalizeText(row && row.organizationId),
    hostId: normalizeText(row && row.hostRef),
    componentId,
    inspectionScope: 'all',
  })
    .then(inspection => Boolean(inspection && inspection.scopes))
    .catch(() => false)
    .finally(() => {
      RUNTIME_INSPECTION_PREWARM_IN_FLIGHT.delete(prewarmKey);
    });

  RUNTIME_INSPECTION_PREWARM_IN_FLIGHT.set(prewarmKey, prewarmPromise);
  return prewarmPromise;
};

export async function prewarmRunbookRuntimeInspectionCache(
  runId,
  topologyRows = [],
  options = {}
) {
  const normalizedRunId = normalizeText(runId);
  if (!normalizedRunId) {
    return {
      requested: 0,
      skipped: 0,
      warmed: 0,
      failed: 0,
    };
  }

  const maxComponents = Math.max(1, Number(options.maxComponents) || 12);
  const deduplicatedRows = sortRuntimeInspectionPrewarmRows(topologyRows).reduce(
    (accumulator, row) => {
      const componentId = normalizeText(row && row.componentId);
      const status = normalizeText(row && row.status).toLowerCase();
      if (!componentId || status === 'planned') {
        return accumulator;
      }

      if (accumulator.seen.has(componentId)) {
        return accumulator;
      }

      accumulator.seen.add(componentId);
      accumulator.rows.push(row);
      return accumulator;
    },
    {
      seen: new Set(),
      rows: [],
    }
  ).rows;

  const cachedRunEntry = readCachedOfficialRun(normalizedRunId);
  const cachedRun = cachedRunEntry && cachedRunEntry.run ? cachedRunEntry.run : null;
  const rowsToWarm = deduplicatedRows
    .filter(row => !hasCachedRuntimeInspectionForComponent(cachedRun, row.componentId))
    .slice(0, maxComponents);

  if (rowsToWarm.length === 0) {
    return {
      requested: 0,
      skipped: deduplicatedRows.length,
      warmed: 0,
      failed: 0,
    };
  }

  const prewarmResult = await rowsToWarm.reduce(
    (resultPromise, row) =>
      resultPromise.then(async currentResult => {
        // Sequential prewarm preserves backend stability while guaranteeing cache persistence.
        const success = await prewarmRuntimeInspectionForRow(normalizedRunId, row);
        return {
          warmed: currentResult.warmed + (success ? 1 : 0),
          failed: currentResult.failed + (success ? 0 : 1),
        };
      }),
    Promise.resolve({
      warmed: 0,
      failed: 0,
    })
  );

  return {
    requested: rowsToWarm.length,
    skipped: Math.max(deduplicatedRows.length - rowsToWarm.length, 0),
    warmed: prewarmResult.warmed,
    failed: prewarmResult.failed,
  };
}

const sanitizeHostMappingEntryForTransport = (
  hostEntry,
  { redactSensitiveReferences = false, allowSensitivePayload = false } = {}
) => {
  if (!hostEntry || typeof hostEntry !== 'object') {
    return {};
  }

  return Object.entries(hostEntry).reduce((accumulator, [key, value]) => {
    if (FORBIDDEN_PLAINTEXT_SECRET_KEYS.has(key)) {
      return accumulator;
    }

    if (SENSITIVE_PAYLOAD_KEYS.has(key) && !allowSensitivePayload) {
      return accumulator;
    }

    if (SENSITIVE_REFERENCE_KEYS.has(key)) {
      accumulator[key] = redactSensitiveReferences ? '[PROTECTED_REF]' : value;
      return accumulator;
    }

    accumulator[key] = value;
    return accumulator;
  }, {});
};

const sanitizeRunbookStartPayload = params => {
  const safePayload = params && typeof params === 'object' ? { ...params } : {};
  const transportSanitizationOptions = {
    redactSensitiveReferences: RUNBOOK_LOCAL_MODE_ENABLED,
    allowSensitivePayload: !RUNBOOK_LOCAL_MODE_ENABLED,
  };
  const hostMapping = Array.isArray(safePayload.host_mapping) ? safePayload.host_mapping : [];
  safePayload.host_mapping = hostMapping.map(hostEntry =>
    sanitizeHostMappingEntryForTransport(hostEntry, transportSanitizationOptions)
  );
  const machineCredentials = Array.isArray(safePayload.machine_credentials)
    ? safePayload.machine_credentials
    : [];
  safePayload.machine_credentials = machineCredentials.map(credentialRow =>
    sanitizeHostMappingEntryForTransport(credentialRow, transportSanitizationOptions)
  );
  return safePayload;
};

const normalizeDecision = value =>
  normalizeText(value).toLowerCase() === 'allow' ? 'allow' : 'block';

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

const pickDefinedValue = (...values) => values.find(value => value !== undefined && value !== null);

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

const normalizePositiveInteger = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizeExecutionMode = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === RUNBOOK_EXECUTION_MODES.fresh) {
    return RUNBOOK_EXECUTION_MODES.fresh;
  }
  if (normalized === RUNBOOK_EXECUTION_MODES.reused) {
    return RUNBOOK_EXECUTION_MODES.reused;
  }
  if (normalized === RUNBOOK_EXECUTION_MODES.reexecuted) {
    return RUNBOOK_EXECUTION_MODES.reexecuted;
  }
  return '';
};

const normalizeResumeStrategy = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === RUNBOOK_RESUME_STRATEGIES.freshRun) {
    return RUNBOOK_RESUME_STRATEGIES.freshRun;
  }
  if (normalized === RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse) {
    return RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse;
  }
  if (normalized === RUNBOOK_RESUME_STRATEGIES.newRunDiff) {
    return RUNBOOK_RESUME_STRATEGIES.newRunDiff;
  }
  if (normalized === RUNBOOK_RESUME_STRATEGIES.newRunFull) {
    return RUNBOOK_RESUME_STRATEGIES.newRunFull;
  }
  return '';
};

const extractRunbookResumeContext = run => {
  const safeRun = run && typeof run === 'object' ? run : {};
  const candidates = [
    safeRun.runbook_resume_context,
    safeRun.runbookResumeContext,
    safeRun.snapshot && safeRun.snapshot.runbook_resume_context,
    safeRun.snapshot && safeRun.snapshot.runbookResumeContext,
  ];
  return (
    candidates.find(
      candidate => candidate && typeof candidate === 'object' && !Array.isArray(candidate)
    ) || {}
  );
};

const resolveExecutionSemantics = ({ run, mappedEvents, runbookResumeContext }) => {
  const safeRun = run && typeof run === 'object' ? run : {};
  const safeResumeContext =
    runbookResumeContext && typeof runbookResumeContext === 'object' ? runbookResumeContext : {};
  const semanticsCandidates = [
    safeRun.execution_semantics,
    safeRun.executionSemantics,
    safeRun.snapshot && safeRun.snapshot.execution_semantics,
    safeRun.snapshot && safeRun.snapshot.executionSemantics,
  ];
  const safeSemantics =
    semanticsCandidates.find(
      candidate => candidate && typeof candidate === 'object' && !Array.isArray(candidate)
    ) || {};

  const eventCodeSet = new Set(
    (Array.isArray(mappedEvents) ? mappedEvents : [])
      .map(event => normalizeText(event && event.code).toLowerCase())
      .filter(Boolean)
  );
  const hasRetryEvent =
    eventCodeSet.has('runbook_retry') ||
    eventCodeSet.has('runbook_retry_from_checkpoint') ||
    eventCodeSet.has('runbook_retry_local');
  const hasResumeEvent =
    eventCodeSet.has('runbook_resumed') || eventCodeSet.has('runbook_resumed_local');

  const targetRunId = normalizeText(safeRun.run_id);
  const targetChangeId = normalizeText(safeRun.change_id);
  const sourceRunId = normalizeText(
    pickDefinedValue(
      safeSemantics.source_run_id,
      safeSemantics.sourceRunId,
      safeResumeContext.source_run_id,
      safeResumeContext.sourceRunId,
      safeResumeContext.previous_run_id,
      safeResumeContext.previousRunId,
      safeResumeContext.run_id
    )
  );
  const sourceChangeId = normalizeText(
    pickDefinedValue(
      safeSemantics.source_change_id,
      safeSemantics.sourceChangeId,
      safeResumeContext.source_change_id,
      safeResumeContext.sourceChangeId,
      safeResumeContext.change_id
    )
  );
  const reusedCheckpointKey = normalizeText(
    pickDefinedValue(
      safeSemantics.reused_checkpoint_key,
      safeSemantics.reusedCheckpointKey,
      safeSemantics.resume_checkpoint_key,
      safeSemantics.resumeCheckpointKey,
      safeResumeContext.reused_checkpoint_key,
      safeResumeContext.reusedCheckpointKey,
      safeResumeContext.resume_checkpoint_key,
      safeResumeContext.resumeCheckpointKey
    )
  );
  const reusedCheckpointOrder = normalizePositiveInteger(
    pickDefinedValue(
      safeSemantics.reused_checkpoint_order,
      safeSemantics.reusedCheckpointOrder,
      safeSemantics.resume_checkpoint_order,
      safeSemantics.resumeCheckpointOrder,
      safeResumeContext.reused_checkpoint_order,
      safeResumeContext.reusedCheckpointOrder,
      safeResumeContext.resume_checkpoint_order,
      safeResumeContext.resumeCheckpointOrder
    )
  );
  let operationType = normalizeText(
    pickDefinedValue(
      safeSemantics.operation_type,
      safeSemantics.operationType,
      safeSemantics.action,
      safeResumeContext.operation_type,
      safeResumeContext.operationType,
      safeResumeContext.action
    )
  );
  let mode = normalizeExecutionMode(
    pickDefinedValue(safeSemantics.mode, safeSemantics.execution_mode, safeSemantics.executionMode)
  );
  let resumeStrategy = normalizeResumeStrategy(
    pickDefinedValue(
      safeSemantics.resume_strategy,
      safeSemantics.resumeStrategy,
      safeSemantics.strategy,
      safeResumeContext.resume_strategy,
      safeResumeContext.resumeStrategy,
      safeResumeContext.strategy
    )
  );
  let reused = normalizeBoolean(
    pickDefinedValue(safeSemantics.reused, safeSemantics.is_reused, safeSemantics.isReused)
  );
  let reexecuted = normalizeBoolean(
    pickDefinedValue(
      safeSemantics.reexecuted,
      safeSemantics.is_reexecuted,
      safeSemantics.isReexecuted
    )
  );
  let diffOnly = normalizeBoolean(
    pickDefinedValue(
      safeSemantics.diff_only,
      safeSemantics.diffOnly,
      safeSemantics.apply_diff_only,
      safeSemantics.applyDiffOnly,
      safeResumeContext.diff_only,
      safeResumeContext.diffOnly,
      safeResumeContext.apply_diff_only,
      safeResumeContext.applyDiffOnly
    )
  );

  const sameRunResume = Boolean(sourceRunId) && sourceRunId === targetRunId;
  const newRunResume =
    Boolean(sourceRunId) &&
    sourceRunId !== targetRunId &&
    Boolean(sourceChangeId) &&
    sourceChangeId === targetChangeId;

  if (!operationType) {
    if (hasRetryEvent) {
      operationType = 'retry';
    } else if (hasResumeEvent) {
      operationType = 'resume';
    } else if (targetRunId) {
      operationType = 'start';
    }
  }

  if (!reused && (hasRetryEvent || hasResumeEvent || sameRunResume || newRunResume)) {
    reused = true;
  }
  if (!reexecuted && hasRetryEvent) {
    reexecuted = true;
  }

  if (!resumeStrategy) {
    if (reexecuted || hasRetryEvent || sameRunResume) {
      resumeStrategy = RUNBOOK_RESUME_STRATEGIES.sameRunCheckpointReuse;
    } else if (newRunResume) {
      resumeStrategy = diffOnly
        ? RUNBOOK_RESUME_STRATEGIES.newRunDiff
        : RUNBOOK_RESUME_STRATEGIES.newRunFull;
    } else {
      resumeStrategy = RUNBOOK_RESUME_STRATEGIES.freshRun;
    }
  }

  if (resumeStrategy === RUNBOOK_RESUME_STRATEGIES.newRunDiff) {
    diffOnly = true;
  }

  if (!mode) {
    if (reexecuted) {
      mode = RUNBOOK_EXECUTION_MODES.reexecuted;
    } else if (reused) {
      mode = RUNBOOK_EXECUTION_MODES.reused;
    } else {
      mode = RUNBOOK_EXECUTION_MODES.fresh;
    }
  }

  if (mode === RUNBOOK_EXECUTION_MODES.reexecuted) {
    reexecuted = true;
    reused = true;
  }
  if (reexecuted) {
    reused = true;
  }

  return {
    mode,
    reused,
    reexecuted,
    resumeStrategy,
    operationType,
    diffOnly,
    sourceRunId,
    targetRunId,
    sourceChangeId,
    targetChangeId,
    reusedCheckpointKey,
    reusedCheckpointOrder,
  };
};

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

const normalizeScopeLock = (lockRow, { fallbackRunId = '', fallbackChangeId = '' } = {}) => {
  const safeLock = lockRow && typeof lockRow === 'object' ? lockRow : {};
  const normalizedStatus = normalizeText(
    pickDefinedValue(safeLock.status, safeLock.lock_status, safeLock.lockStatus)
  ).toLowerCase();
  const explicitActive = pickDefinedValue(
    safeLock.active,
    safeLock.is_active,
    safeLock.isActive,
    safeLock.locked,
    safeLock.is_locked,
    safeLock.isLocked
  );
  const isStatusActive =
    normalizedStatus === 'active' ||
    normalizedStatus === 'locked' ||
    normalizedStatus === 'blocking';
  const scopeKey = normalizeText(
    pickDefinedValue(
      safeLock.scope_key,
      safeLock.scopeKey,
      safeLock.resource_key,
      safeLock.resourceKey,
      safeLock.scope,
      safeLock.key
    )
  );

  return {
    active:
      explicitActive !== undefined && explicitActive !== null
        ? normalizeBoolean(explicitActive)
        : isStatusActive,
    scopeKey,
    stage: normalizeText(safeLock.stage),
    checkpoint: normalizeText(safeLock.checkpoint),
    acquiredAt: normalizeText(
      pickDefinedValue(
        safeLock.acquired_at_utc,
        safeLock.acquiredAtUtc,
        safeLock.acquired_at,
        safeLock.acquiredAt
      )
    ),
    expiresAt: normalizeText(
      pickDefinedValue(
        safeLock.expires_at_utc,
        safeLock.expiresAtUtc,
        safeLock.expires_at,
        safeLock.expiresAt
      )
    ),
    ownerRunId: normalizeText(
      pickDefinedValue(
        safeLock.owner_run_id,
        safeLock.ownerRunId,
        safeLock.run_id,
        safeLock.runId,
        fallbackRunId
      )
    ),
    ownerChangeId: normalizeText(
      pickDefinedValue(
        safeLock.owner_change_id,
        safeLock.ownerChangeId,
        safeLock.change_id,
        safeLock.changeId,
        fallbackChangeId
      )
    ),
    operationType: normalizeText(
      pickDefinedValue(safeLock.operation_type, safeLock.operationType, safeLock.action)
    ),
    reasonCode: normalizeText(
      pickDefinedValue(safeLock.reason_code, safeLock.reasonCode, safeLock.code)
    ),
    reasonMessage: normalizeText(
      pickDefinedValue(safeLock.reason_message, safeLock.reasonMessage, safeLock.message)
    ),
  };
};

const resolveScopeLockFromRun = ({ run, lastFailure = null } = {}) => {
  const safeRun = run && typeof run === 'object' ? run : {};
  const normalizedRunId = normalizeText(safeRun.run_id);
  const normalizedChangeId = normalizeText(safeRun.change_id);
  const lockCandidates = [
    safeRun.scope_lock,
    safeRun.scopeLock,
    safeRun.execution_lock,
    safeRun.executionLock,
    safeRun.active_lock,
    safeRun.activeLock,
    safeRun.resource_lock,
    safeRun.resourceLock,
    safeRun.snapshot && safeRun.snapshot.scope_lock,
    safeRun.snapshot && safeRun.snapshot.scopeLock,
    safeRun.snapshot && safeRun.snapshot.execution_lock,
    safeRun.snapshot && safeRun.snapshot.executionLock,
  ];
  const selectedLockCandidate = lockCandidates.find(
    candidate => candidate && typeof candidate === 'object' && !Array.isArray(candidate)
  );

  if (selectedLockCandidate) {
    const normalizedLock = normalizeScopeLock(selectedLockCandidate, {
      fallbackRunId: normalizedRunId,
      fallbackChangeId: normalizedChangeId,
    });
    if (normalizedLock.active) {
      return normalizedLock;
    }
  }

  let activeLocks = [];
  if (Array.isArray(safeRun.active_resource_locks)) {
    activeLocks = safeRun.active_resource_locks;
  } else if (Array.isArray(safeRun.activeResourceLocks)) {
    activeLocks = safeRun.activeResourceLocks;
  }
  if (activeLocks.length > 0) {
    const firstLock = normalizeScopeLock(activeLocks[0], {
      fallbackRunId: normalizedRunId,
      fallbackChangeId: normalizedChangeId,
    });
    return {
      ...firstLock,
      active: true,
    };
  }

  const normalizedFailureCode = normalizeText(lastFailure && lastFailure.code).toLowerCase();
  if (normalizedFailureCode === 'runbook_resource_lock_conflict') {
    return {
      ...buildDefaultScopeLock(),
      active: true,
      stage: normalizeText(lastFailure && lastFailure.stage),
      checkpoint: normalizeText(lastFailure && lastFailure.checkpoint),
      ownerRunId: normalizedRunId,
      ownerChangeId: normalizedChangeId,
      reasonCode: normalizedFailureCode,
      reasonMessage:
        normalizeText(lastFailure && lastFailure.message) ||
        localizeRunbookText(
          'Conflito de lock oficial para recurso crítico.',
          'Official lock conflict for a critical resource.'
        ),
    };
  }

  return buildDefaultScopeLock();
};

const normalizeDecisionArray = values =>
  (Array.isArray(values) ? values : [])
    .map(value => normalizeText(value))
    .filter(Boolean)
    .filter((value, index, allValues) => allValues.indexOf(value) === index);

const normalizeDecisionPayload = (payload, { status = 'pending' } = {}) => {
  const safePayload = payload && typeof payload === 'object' ? payload : {};
  const normalizedStatus = normalizeStatus(safePayload.status || status);
  const isCompleted = normalizedStatus === 'completed';
  const normalizedDecision = normalizeDecision(
    safePayload.decision || (isCompleted ? 'allow' : 'block')
  );
  const requiredEvidenceKeys = normalizeDecisionArray(
    safePayload.required_evidence_keys || safePayload.requiredEvidenceKeys
  );
  const missingEvidenceKeys = normalizeDecisionArray(
    safePayload.missing_evidence_keys || safePayload.missingEvidenceKeys
  );
  let evidenceMinimumValid = false;
  if (typeof safePayload.evidence_minimum_valid === 'boolean') {
    evidenceMinimumValid = safePayload.evidence_minimum_valid;
  } else if (typeof safePayload.evidenceMinimumValid === 'boolean') {
    evidenceMinimumValid = safePayload.evidenceMinimumValid;
  } else if (normalizedDecision === 'allow') {
    evidenceMinimumValid = true;
  }

  return {
    decision: normalizedDecision,
    decisionCode:
      normalizeText(safePayload.decision_code) ||
      normalizeText(safePayload.decisionCode) ||
      (normalizedDecision === 'allow'
        ? 'ALLOW_COMPLETED_WITH_MIN_EVIDENCE'
        : 'BLOCK_RUN_NOT_COMPLETED'),
    decisionReasons: normalizeDecisionArray(
      safePayload.decision_reasons || safePayload.decisionReasons
    ),
    status: normalizedStatus,
    requiredEvidenceKeys,
    missingEvidenceKeys,
    evidenceMinimumValid,
    timestamp:
      normalizeText(safePayload.timestamp_utc) || normalizeText(safePayload.timestamp) || '',
  };
};

const resolveDecisionPayloadFromArtifactRows = run => {
  const safeRun = run && typeof run === 'object' ? run : {};
  const artifactRows = Array.isArray(safeRun.artifact_rows) ? safeRun.artifact_rows : [];
  const decisionRows = artifactRows
    .filter(artifactRow => artifactRow && typeof artifactRow === 'object')
    .filter(artifactRow => {
      const artifactKey = normalizeText(artifactRow.artifact_key);
      if (artifactKey === 'decision-trace:decision-trace.jsonl') {
        return true;
      }
      const artifactGroup = normalizeText(artifactRow.artifact_group);
      const artifactName = normalizeText(artifactRow.artifact_name);
      return artifactGroup === 'decision-trace' && artifactName === 'decision-trace.jsonl';
    })
    .sort((left, right) => {
      const leftVersion = Number(left.version) || 0;
      const rightVersion = Number(right.version) || 0;
      if (leftVersion !== rightVersion) {
        return rightVersion - leftVersion;
      }
      const leftTimestamp = normalizeText(left.timestamp_utc);
      const rightTimestamp = normalizeText(right.timestamp_utc);
      return rightTimestamp.localeCompare(leftTimestamp);
    });

  if (decisionRows.length === 0) {
    return null;
  }

  const latestRow = decisionRows[0];
  return latestRow && typeof latestRow.payload === 'object' ? latestRow.payload : null;
};

const resolveRunOfficialDecision = run => {
  const safeRun = run && typeof run === 'object' ? run : {};
  const decisionPayloadCandidates = [
    safeRun.official_decision,
    safeRun.snapshot && safeRun.snapshot.official_decision,
    resolveDecisionPayloadFromArtifactRows(safeRun),
  ];

  const payload =
    decisionPayloadCandidates.find(candidate => candidate && typeof candidate === 'object') || {};

  return normalizeDecisionPayload(payload, {
    status: safeRun.status || (safeRun.snapshot && safeRun.snapshot.pipeline_status) || 'pending',
  });
};

const normalizeEventLevel = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === 'error' || normalized === 'warning' || normalized === 'info') {
    return normalized;
  }
  return 'info';
};

const classifyByLevel = level => {
  const normalized = normalizeEventLevel(level);
  if (normalized === 'error') {
    return 'critical';
  }
  if (normalized === 'warning') {
    return 'transient';
  }
  return 'informational';
};

const normalizeEventClassification = (value, level) => {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === 'critical' || normalized === 'transient' || normalized === 'informational') {
    return normalized;
  }
  return classifyByLevel(level);
};

const resolveEventCauseByLevel = (level, localeCandidate) => {
  if (level === 'warning') {
    return localizeRunbookText(
      'Condicao transitoria detectada no backend oficial.',
      'Transient condition detected in the official backend.',
      localeCandidate
    );
  }
  if (level === 'error') {
    return localizeRunbookText(
      'Falha tecnica detectada no backend oficial.',
      'Technical failure detected in the official backend.',
      localeCandidate
    );
  }
  return localizeRunbookText(
    'Evento operacional informativo recebido do backend oficial.',
    'Informational operational event received from the official backend.',
    localeCandidate
  );
};

const resolveEventImpactByLevel = (level, localeCandidate) => {
  if (level === 'warning') {
    return localizeRunbookText(
      'Pode afetar continuidade da etapa corrente se nao tratado.',
      'It may affect continuity of the current stage if not handled.',
      localeCandidate
    );
  }
  if (level === 'error') {
    return localizeRunbookText(
      'Bloqueia continuidade da execucao ate correcao da causa.',
      'It blocks execution continuity until the cause is corrected.',
      localeCandidate
    );
  }
  return localizeRunbookText(
    'Sem impacto bloqueante para continuidade operacional.',
    'No blocking impact on operational continuity.',
    localeCandidate
  );
};

const resolveEventRecommendedActionByLevel = (level, localeCandidate) => {
  if (level === 'warning') {
    return localizeRunbookText(
      'Analisar a evidencia oficial e confirmar estabilidade antes de avancar.',
      'Analyze the official evidence and confirm stability before advancing.',
      localeCandidate
    );
  }
  if (level === 'error') {
    return localizeRunbookText(
      'Corrigir causa tecnica, validar artefatos e executar retry da etapa/checkpoint.',
      'Correct the technical cause, validate artifacts, and retry the stage/checkpoint.',
      localeCandidate
    );
  }
  return localizeRunbookText('Sem acao imediata.', 'No immediate action.', localeCandidate);
};

const resolveEventComponent = ({ component = '', hostRef = '', stage = '', checkpoint = '' }) => {
  const normalizedComponent = normalizeText(component);
  if (normalizedComponent) {
    return normalizedComponent;
  }

  const normalizedHostRef = normalizeText(hostRef);
  if (normalizedHostRef) {
    return `host:${normalizedHostRef}`;
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

const resolveEventCause = ({ cause = '', message = '', level = 'info' }) =>
  normalizeText(cause) ||
  normalizeText(message) ||
  resolveEventCauseByLevel(level) ||
  resolveEventCauseByLevel('info');

const resolveEventImpact = ({ impact = '', level = 'info', stage = '' }) => {
  const normalizedImpact = normalizeText(impact);
  if (normalizedImpact) {
    return normalizedImpact;
  }

  const normalizedStage = normalizeText(stage);
  if (normalizedStage && (level === 'warning' || level === 'error')) {
    return localizeRunbookText(
      `Impacta continuidade da etapa '${normalizedStage}' ate tratamento do evento.`,
      `It impacts continuity of the '${normalizedStage}' stage until the event is handled.`
    );
  }

  return resolveEventImpactByLevel(level) || resolveEventImpactByLevel('info');
};

const resolveEventRecommendedAction = ({ recommendedAction = '', level = 'info' }) =>
  normalizeText(recommendedAction) ||
  resolveEventRecommendedActionByLevel(level) ||
  resolveEventRecommendedActionByLevel('info');

const buildStageOrderByKey = stages =>
  (Array.isArray(stages) ? stages : []).reduce((accumulator, stage, stageIndex) => {
    const stageKey = normalizeText(stage && stage.key);
    if (!stageKey) {
      return accumulator;
    }
    accumulator[stageKey] = Number(stage && stage.order) || stageIndex + 1;
    return accumulator;
  }, {});

const resolveEventStageOrder = (stage, stageOrderByKey) => {
  const stageKey = normalizeText(stage);
  if (!stageKey) {
    return 9999;
  }
  return Number(stageOrderByKey && stageOrderByKey[stageKey]) || 9999;
};

const normalizeTimestampForTimelineSort = value => normalizeText(value) || '9999-12-31T23:59:59Z';

const compareMappedRunEvents = (left, right) => {
  if (left.stageOrder !== right.stageOrder) {
    return left.stageOrder - right.stageOrder;
  }

  const leftStage = normalizeText(left.stage);
  const rightStage = normalizeText(right.stage);
  if (leftStage !== rightStage) {
    return leftStage.localeCompare(rightStage);
  }

  const leftTimestamp = normalizeTimestampForTimelineSort(left.timestamp);
  const rightTimestamp = normalizeTimestampForTimelineSort(right.timestamp);
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

const resolveStageMetadata = stageKey => STAGE_METADATA_BY_KEY[stageKey] || DEFAULT_STAGE_METADATA;

const deriveTimestampRange = checkpoints => {
  const timestamps = checkpoints
    .map(checkpoint =>
      [normalizeText(checkpoint.startedAt), normalizeText(checkpoint.finishedAt)].filter(Boolean)
    )
    .flat()
    .sort((left, right) => left.localeCompare(right));

  return {
    startedAt: timestamps[0] || '',
    finishedAt: timestamps.length > 0 ? timestamps[timestamps.length - 1] : '',
  };
};

export const mapBackendRunToExecutionState = backendRun => {
  const safeRun = backendRun && typeof backendRun === 'object' ? backendRun : {};
  const runBackendState = normalizeBackendState(
    safeRun.backend_state || (safeRun.snapshot && safeRun.snapshot.backend_state) || 'ready'
  );
  const stagesFromBackend = Array.isArray(safeRun.stages) ? safeRun.stages : [];
  const lastFailure =
    safeRun.last_failure && typeof safeRun.last_failure === 'object' ? safeRun.last_failure : null;

  const mappedStages = stagesFromBackend.map((stage, stageIndex) => {
    const stageKey = normalizeText(stage.key);
    const metadata = resolveStageMetadata(stageKey);
    const checkpointsFromBackend = Array.isArray(stage.checkpoints) ? stage.checkpoints : [];

    const mappedCheckpoints = checkpointsFromBackend.map(checkpoint => {
      const checkpointKey = normalizeText(checkpoint.key);
      const checkpointFailure =
        lastFailure && normalizeText(lastFailure.checkpoint) === checkpointKey
          ? {
              code: normalizeText(lastFailure.code) || 'runbook_failure',
              message:
                sanitizeSensitiveText(normalizeText(lastFailure.message)) ||
                'Falha reportada pelo backend.',
              cause: '',
              recommendedAction: '',
              classification: 'critical',
              timestamp: normalizeText(lastFailure.timestamp_utc),
            }
          : null;

      return {
        key: checkpointKey,
        label: normalizeText(checkpoint.label) || checkpointKey || '-',
        order: Number.isInteger(checkpoint.order) ? checkpoint.order : 1,
        status: normalizeStatus(checkpoint.status),
        startedAt: normalizeText(checkpoint.started_at_utc),
        finishedAt: normalizeText(checkpoint.completed_at_utc),
        failure: checkpointFailure,
      };
    });

    const stageTimestampRange = deriveTimestampRange(mappedCheckpoints);
    const stageFailure =
      (stage.failure &&
        typeof stage.failure === 'object' && {
          code: normalizeText(stage.failure.code) || 'runbook_failure',
          message:
            sanitizeSensitiveText(normalizeText(stage.failure.message)) ||
            'Falha reportada pelo backend.',
          cause: sanitizeSensitiveText(normalizeText(stage.failure.cause)),
          recommendedAction: sanitizeSensitiveText(normalizeText(stage.failure.recommendedAction)),
          classification: normalizeText(stage.failure.classification) || 'critical',
          timestamp: normalizeText(stage.failure.timestamp),
        }) ||
      (lastFailure && normalizeText(lastFailure.stage) === stageKey
        ? {
            code: normalizeText(lastFailure.code) || 'runbook_failure',
            message:
              sanitizeSensitiveText(normalizeText(lastFailure.message)) ||
              'Falha reportada pelo backend.',
            cause: '',
            recommendedAction: '',
            classification: 'critical',
            timestamp: normalizeText(lastFailure.timestamp_utc),
          }
        : null);

    return {
      key: stageKey || `stage-${stageIndex + 1}`,
      label: normalizeText(stage.label) || stageKey || `stage-${stageIndex + 1}`,
      order: Number.isInteger(stage.order) ? stage.order : stageIndex + 1,
      status: normalizeStatus(stage.status),
      startedAt: stageTimestampRange.startedAt,
      finishedAt:
        normalizeStatus(stage.status) === 'completed' ? stageTimestampRange.finishedAt : '',
      completionCriteria: [...metadata.completionCriteria],
      evidenceArtifacts: [...metadata.evidenceArtifacts],
      checkpoints: mappedCheckpoints,
      failure: stageFailure,
    };
  });

  const stageOrderByKey = buildStageOrderByKey(mappedStages);
  const mappedEvents = (Array.isArray(safeRun.events) ? safeRun.events : [])
    .map((event, index) => {
      const eventLevel = normalizeEventLevel(event && event.level);
      const stage = normalizeText(event && event.stage);
      const checkpoint = normalizeText(event && event.checkpoint);
      const hostRef = normalizeText((event && event.host_ref) || (event && event.hostRef));
      const message =
        sanitizeSensitiveText(normalizeText(event && event.message)) ||
        localizeRunbookText(
          'Evento operacional recebido do backend.',
          'Operational event received from the backend.'
        );
      const classification = normalizeEventClassification(
        event && event.classification,
        eventLevel
      );

      return {
        id: normalizeText(event && event.id) || `evt-${String(index + 1).padStart(4, '0')}`,
        timestamp: normalizeText((event && event.timestamp_utc) || (event && event.timestamp)),
        level: eventLevel,
        code: normalizeText(event && event.code) || 'runbook_event',
        stage,
        stageOrder: resolveEventStageOrder(stage, stageOrderByKey),
        checkpoint,
        message,
        runId: normalizeText(event && event.run_id) || normalizeText(safeRun.run_id),
        changeId: normalizeText(event && event.change_id) || normalizeText(safeRun.change_id),
        component: resolveEventComponent({
          component: sanitizeSensitiveText((event && event.component) || ''),
          hostRef,
          stage,
          checkpoint,
        }),
        cause: resolveEventCause({
          cause: sanitizeSensitiveText((event && (event.cause || event.primary_cause)) || ''),
          message,
          level: eventLevel,
        }),
        impact: resolveEventImpact({
          impact: sanitizeSensitiveText((event && event.impact) || ''),
          level: eventLevel,
          stage,
        }),
        recommendedAction: resolveEventRecommendedAction({
          recommendedAction: sanitizeSensitiveText(
            (event && (event.recommended_action || event.recommendedAction)) || ''
          ),
          level: eventLevel,
        }),
        classification,
        path: sanitizeSensitiveText(normalizeText(event && event.path)),
        backendState: runBackendState,
        originalIndex: index,
      };
    })
    .sort(compareMappedRunEvents)
    .map(({ originalIndex, ...event }) => event);
  const officialDecision = resolveRunOfficialDecision(safeRun);
  const runbookResumeContext = extractRunbookResumeContext(safeRun);
  const executionSemantics = resolveExecutionSemantics({
    run: safeRun,
    mappedEvents,
    runbookResumeContext,
  });
  const scopeLock = resolveScopeLockFromRun({
    run: safeRun,
    lastFailure,
  });

  return {
    runId: normalizeText(safeRun.run_id),
    changeId: normalizeText(safeRun.change_id),
    providerKey: normalizeText(safeRun.provider_key),
    environmentProfile: normalizeText(safeRun.environment_profile),
    blueprintVersion: normalizeText(safeRun.blueprint_version),
    blueprintFingerprint: normalizeText(safeRun.blueprint_fingerprint),
    manifestFingerprint: normalizeText(safeRun.manifest_fingerprint),
    sourceBlueprintFingerprint: normalizeText(safeRun.source_blueprint_fingerprint),
    requiredArtifacts: Array.isArray(safeRun.a2_2_minimum_artifacts)
      ? safeRun.a2_2_minimum_artifacts
      : [],
    availableArtifacts: Array.isArray(safeRun.a2_2_available_artifacts)
      ? safeRun.a2_2_available_artifacts
      : [],
    resolvedSchemaVersion: normalizeText(safeRun.resolved_schema_version),
    backendState: runBackendState,
    status: normalizeStatus(safeRun.status) || 'idle',
    startedAt: normalizeText(safeRun.started_at_utc),
    finishedAt: normalizeText(safeRun.finished_at_utc),
    currentStageIndex: Number.isInteger(safeRun.current_stage_index)
      ? safeRun.current_stage_index
      : 0,
    stages: mappedStages,
    events: mappedEvents,
    runbookResumeContext,
    executionSemantics,
    scopeLock,
    officialDecision,
  };
};

const validateOfficialRunContract = run => {
  const safeRun = run && typeof run === 'object' ? run : null;
  if (!safeRun) {
    throw new Error(
      localizeRunbookText(
        'Contrato de runbook inválido: payload run ausente.',
        'Invalid runbook contract: missing run payload.'
      )
    );
  }

  const runId = normalizeText(safeRun.run_id);
  const status = normalizeText(safeRun.status);
  const stages = Array.isArray(safeRun.stages) ? safeRun.stages : null;
  const events = Array.isArray(safeRun.events) ? safeRun.events : null;

  if (!runId || !status || !stages || !events) {
    throw new Error(
      localizeRunbookText(
        'Contrato de runbook inválido: campos obrigatórios ausentes (run_id/status/stages/events).',
        'Invalid runbook contract: required fields are missing (run_id/status/stages/events).'
      )
    );
  }

  const containsLocalEventCode = events.some(event => {
    const code = normalizeText(event && event.code).toLowerCase();
    return code.includes('_local');
  });

  if (containsLocalEventCode) {
    throw new Error(
      localizeRunbookText(
        'Contrato de runbook inválido: timeline oficial contém eventos simulados *_local.',
        'Invalid runbook contract: official timeline contains simulated *_local events.'
      )
    );
  }

  const hasInvalidStage = stages.some(stage => {
    if (!stage || typeof stage !== 'object') {
      return true;
    }
    return !Array.isArray(stage.checkpoints);
  });

  if (hasInvalidStage) {
    throw new Error(
      localizeRunbookText(
        'Contrato de runbook inválido: stage sem checkpoints.',
        'Invalid runbook contract: stage without checkpoints.'
      )
    );
  }
};

const isRunbookEnvelope = payload =>
  Boolean(payload) &&
  typeof payload === 'object' &&
  (payload.status === 'successful' || payload.status === 'fail');

const extractRunbookFailPayloadParts = payload => {
  if (!payload || typeof payload !== 'object') {
    return {
      code: '',
      msg: '',
      message: '',
      details: {},
    };
  }

  const dataPayload =
    payload.data && typeof payload.data === 'object' && !Array.isArray(payload.data)
      ? payload.data
      : {};

  const code =
    normalizeText(dataPayload.code) ||
    normalizeText(payload.code) ||
    normalizeText(payload.msg) ||
    normalizeText(dataPayload.msg) ||
    normalizeText(dataPayload.error_code) ||
    normalizeText(payload.error_code);

  const message =
    sanitizeSensitiveText(normalizeText(dataPayload.message)) ||
    sanitizeSensitiveText(normalizeText(payload.message)) ||
    sanitizeSensitiveText(normalizeText(dataPayload.detail)) ||
    sanitizeSensitiveText(normalizeText(payload.detail)) ||
    sanitizeSensitiveText(normalizeText(dataPayload.error)) ||
    sanitizeSensitiveText(normalizeText(payload.error));

  const details =
    (dataPayload.details && typeof dataPayload.details === 'object' && dataPayload.details) ||
    (payload.details && typeof payload.details === 'object' && payload.details) ||
    {};

  const msg =
    normalizeText(payload.msg) ||
    normalizeText(dataPayload.msg) ||
    normalizeText(payload.code) ||
    normalizeText(dataPayload.code);

  return {
    code,
    msg,
    message,
    details,
  };
};

const isRunbookFailLikePayload = payload => {
  if (!payload || typeof payload !== 'object') {
    return false;
  }

  const { code, message } = extractRunbookFailPayloadParts(payload);

  return Boolean(message && code);
};

const resolveRunbookEnvelope = response => {
  const responsePayload = response && typeof response === 'object' ? response : {};
  const candidates = [
    responsePayload,
    responsePayload.data,
    responsePayload.data && responsePayload.data.data,
    responsePayload.error,
    responsePayload.error && responsePayload.error.data,
    responsePayload.response,
    responsePayload.response && responsePayload.response.data,
    responsePayload.body,
    responsePayload.body && responsePayload.body.data,
  ];

  return (
    candidates.find(
      candidate => isRunbookEnvelope(candidate) || isRunbookFailLikePayload(candidate)
    ) || null
  );
};

const buildRunbookBackendError = (
  response,
  fallbackMessage = localizeRunbookText(
    'Falha operacional no backend de runbook.',
    'Operational failure in the runbook backend.'
  )
) => {
  const responsePayload = response && typeof response === 'object' ? response : {};
  const envelopePayload = resolveRunbookEnvelope(responsePayload);
  const effectivePayload = envelopePayload || responsePayload;
  const effectiveDataPayload =
    effectivePayload.data &&
    typeof effectivePayload.data === 'object' &&
    !Array.isArray(effectivePayload.data)
      ? effectivePayload.data
      : {};
  const failParts = extractRunbookFailPayloadParts(effectivePayload);
  const backendMessage = failParts.message || sanitizeSensitiveText(normalizeText(failParts.msg));
  const message = backendMessage || sanitizeSensitiveText(fallbackMessage);
  const statusCode =
    Number(
      effectivePayload.statusCode ||
        effectivePayload.status_code ||
        effectivePayload.http_status ||
        (responsePayload.response && responsePayload.response.status) ||
        responsePayload.status
    ) || 400;

  const error = new Error(message);
  error.data = {
    ...(effectiveDataPayload || {}),
    code: failParts.code,
    msg: sanitizeSensitiveText(failParts.msg),
    details: failParts.details || {},
    detail: backendMessage || sanitizeSensitiveText(fallbackMessage),
    message,
  };
  error.response = {
    status: statusCode,
  };

  return error;
};

const unwrapRunbookResponse = response => {
  const responsePayload = response && typeof response === 'object' ? response : {};
  const envelope = resolveRunbookEnvelope(responsePayload) || responsePayload;

  if (envelope && envelope.status === 'successful' && envelope.data && envelope.data.run) {
    validateOfficialRunContract(envelope.data.run);
    return envelope.data.run;
  }

  if (envelope && envelope.status === 'fail') {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de runbook.',
        'Operational failure reported by the runbook backend.'
      )
    );
  }

  if (isRunbookFailLikePayload(envelope)) {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de runbook.',
        'Operational failure reported by the runbook backend.'
      )
    );
  }

  throw buildRunbookBackendError(
    response,
    localizeRunbookText(
      'Resposta do backend de runbook fora do contrato esperado.',
      'Runbook backend response is outside the expected contract.'
    )
  );
};

const unwrapRunbookPreflightResponse = response => {
  const responsePayload = response && typeof response === 'object' ? response : {};
  const envelope = resolveRunbookEnvelope(responsePayload) || responsePayload;

  if (envelope && envelope.status === 'successful' && envelope.data && envelope.data.preflight) {
    return envelope.data.preflight;
  }

  if (envelope && envelope.status === 'fail') {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de preflight técnico.',
        'Operational failure reported by the technical preflight backend.'
      )
    );
  }

  if (isRunbookFailLikePayload(envelope)) {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de preflight técnico.',
        'Operational failure reported by the technical preflight backend.'
      )
    );
  }

  throw buildRunbookBackendError(
    response,
    localizeRunbookText(
      'Resposta do backend de preflight técnico fora do contrato esperado.',
      'Technical preflight backend response is outside the expected contract.'
    )
  );
};

const unwrapRunbookInspectionResponse = response => {
  const responsePayload = response && typeof response === 'object' ? response : {};
  const envelope = resolveRunbookEnvelope(responsePayload) || responsePayload;

  if (
    envelope &&
    envelope.status === 'successful' &&
    envelope.data &&
    envelope.data.inspection
  ) {
    return envelope.data.inspection;
  }

  if (envelope && envelope.status === 'fail') {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de inspecao runtime.',
        'Operational failure reported by the runtime inspection backend.'
      )
    );
  }

  if (isRunbookFailLikePayload(envelope)) {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de inspecao runtime.',
        'Operational failure reported by the runtime inspection backend.'
      )
    );
  }

  throw buildRunbookBackendError(
    response,
    localizeRunbookText(
      'Resposta do backend de inspecao runtime fora do contrato esperado.',
      'Runtime inspection backend response is outside the expected contract.'
    )
  );
};

const unwrapRunbookCatalogResponse = response => {
  const responsePayload = response && typeof response === 'object' ? response : {};
  const envelope = resolveRunbookEnvelope(responsePayload) || responsePayload;

  if (envelope && envelope.status === 'successful' && envelope.data && Array.isArray(envelope.data.runs)) {
    return envelope.data.runs;
  }

  if (envelope && envelope.status === 'fail') {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de catalogo do runbook.',
        'Operational failure reported by the runbook catalog backend.'
      )
    );
  }

  if (isRunbookFailLikePayload(envelope)) {
    throw buildRunbookBackendError(
      envelope,
      localizeRunbookText(
        'Falha operacional reportada pelo backend de catalogo do runbook.',
        'Operational failure reported by the runbook catalog backend.'
      )
    );
  }

  throw buildRunbookBackendError(
    response,
    localizeRunbookText(
      'Resposta do backend de catalogo do runbook fora do contrato esperado.',
      'Runbook catalog backend response is outside the expected contract.'
    )
  );
};

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const getStorage = () => {
  if (typeof window === 'undefined' || !window.localStorage) {
    return null;
  }
  return window.localStorage;
};

const readLocalRuns = () => {
  const storage = getStorage();
  if (!storage) {
    return {};
  }

  try {
    const rawPayload = storage.getItem(RUNBOOK_LOCAL_STORAGE_KEY);
    if (!rawPayload) {
      return {};
    }
    const parsed = JSON.parse(rawPayload);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (error) {
    return {};
  }
};

const writeLocalRuns = runs => {
  const storage = getStorage();
  if (!storage) {
    return;
  }
  storage.setItem(
    RUNBOOK_LOCAL_STORAGE_KEY,
    JSON.stringify(runs && typeof runs === 'object' ? runs : {})
  );
};

const normalizeCheckpointForStart = checkpoint => ({
  ...checkpoint,
  status: 'pending',
  started_at_utc: '',
  completed_at_utc: '',
});

const createLocalRunFromStartParams = params => {
  const now = toIsoUtc();
  const runId = normalizeText(params && params.run_id) || `run-local-${Date.now()}`;
  const stages = Object.keys(STAGE_METADATA_BY_KEY).map((stageKey, stageIndex) => {
    const stageBaseCheckpoints = [
      {
        key: stageKey === 'prepare' ? 'validate_blueprint_gate' : `${stageKey}_checkpoint_1`,
        label:
          stageKey === 'prepare'
            ? localizeRunbookText(
                'Validar gate A1.2 do blueprint',
                'Validate blueprint A1.2 gate'
              )
            : `Checkpoint 1 ${stageKey}`,
        order: 1,
      },
      {
        key: stageKey === 'prepare' ? 'resolve_target_hosts' : `${stageKey}_checkpoint_2`,
        label:
          stageKey === 'prepare'
            ? localizeRunbookText(
                'Resolver hosts alvo no escopo external-linux',
                'Resolve target hosts in the external-linux scope'
              )
            : `Checkpoint 2 ${stageKey}`,
        order: 2,
      },
      {
        key: stageKey === 'prepare' ? 'check_pipeline_prerequisites' : `${stageKey}_checkpoint_3`,
        label:
          stageKey === 'prepare'
            ? localizeRunbookText(
                'Confirmar pré-condições mínimas do pipeline',
                'Confirm minimum pipeline prerequisites'
              )
            : `Checkpoint 3 ${stageKey}`,
        order: 3,
      },
    ];

    const checkpoints = stageBaseCheckpoints.map(normalizeCheckpointForStart);
    if (stageIndex === 0) {
      checkpoints[0] = {
        ...checkpoints[0],
        status: 'running',
        started_at_utc: now,
      };
    }

    return {
      key: stageKey,
      label: stageKey,
      order: stageIndex + 1,
      status: stageIndex === 0 ? 'running' : 'pending',
      checkpoints,
    };
  });

  return {
    run_id: runId,
    change_id: normalizeText(params && params.change_id),
    provider_key: normalizeText(params && params.provider_key),
    environment_profile: normalizeText(params && params.environment_profile),
    blueprint_version: normalizeText(params && params.blueprint_version),
    blueprint_fingerprint: normalizeText(params && params.blueprint_fingerprint),
    manifest_fingerprint: normalizeText(params && params.manifest_fingerprint),
    source_blueprint_fingerprint: normalizeText(params && params.source_blueprint_fingerprint),
    a2_2_minimum_artifacts: Array.isArray(params && params.a2_2_minimum_artifacts)
      ? params.a2_2_minimum_artifacts
      : [],
    a2_2_available_artifacts: Array.isArray(params && params.a2_2_available_artifacts)
      ? params.a2_2_available_artifacts
      : [],
    topology_catalog:
      params && typeof params.topology_catalog === 'object' ? params.topology_catalog : {},
    handoff_contract_version: normalizeText(params && params.handoff_contract_version),
    handoff_fingerprint: normalizeText(params && params.handoff_fingerprint),
    handoff_payload:
      params && typeof params.handoff_payload === 'object' ? params.handoff_payload : {},
    handoff_trace: Array.isArray(params && params.handoff_trace) ? params.handoff_trace : [],
    runbook_resume_context:
      params && typeof params.runbook_resume_context === 'object'
        ? params.runbook_resume_context
        : {},
    resolved_schema_version: normalizeText(params && params.resolved_schema_version) || '1.0.0',
    backend_state: 'ready',
    status: 'running',
    started_at_utc: now,
    finished_at_utc: '',
    current_stage_index: 0,
    stages,
    events: [
      {
        id: `evt-${Date.now()}-start`,
        timestamp_utc: now,
        level: 'info',
        code: 'runbook_started_local',
        stage: 'prepare',
        checkpoint: stages[0].checkpoints[0].key,
        message: localizeRunbookText(
          'Runbook iniciado em modo local.',
          'Runbook started in local mode.'
        ),
        run_id: runId,
        change_id: normalizeText(params && params.change_id),
      },
    ],
    last_failure: null,
    official_decision: normalizeDecisionPayload(
      {
        decision: 'block',
        decision_code: 'BLOCK_RUN_NOT_COMPLETED',
        decision_reasons: ['runbook_not_completed'],
        status: 'running',
        required_evidence_keys: [],
        missing_evidence_keys: ['history:history.jsonl'],
        evidence_minimum_valid: false,
        timestamp_utc: now,
      },
      { status: 'running' }
    ),
  };
};

const withLocalOfficialDecision = run => {
  const safeRun = run && typeof run === 'object' ? run : {};
  const runStatus = normalizeStatus(safeRun.status);
  const isCompleted = runStatus === 'completed';
  const timestamp = toIsoUtc();

  return {
    ...safeRun,
    official_decision: normalizeDecisionPayload(
      {
        decision: isCompleted ? 'allow' : 'block',
        decision_code: isCompleted
          ? 'ALLOW_COMPLETED_WITH_MIN_EVIDENCE'
          : 'BLOCK_RUN_NOT_COMPLETED',
        decision_reasons: isCompleted ? [] : ['runbook_not_completed'],
        status: runStatus,
        required_evidence_keys: [],
        missing_evidence_keys: isCompleted ? [] : ['history:history.jsonl'],
        evidence_minimum_valid: isCompleted,
        timestamp_utc: timestamp,
      },
      { status: runStatus }
    ),
  };
};

const appendLocalEvent = (run, event) =>
  withLocalOfficialDecision({
    ...run,
    events: [
      ...(Array.isArray(run.events) ? run.events : []),
      {
        id: `evt-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
        timestamp_utc: toIsoUtc(),
        level: 'info',
        run_id: run.run_id,
        change_id: run.change_id,
        ...event,
      },
    ],
  });

const findCurrentRunningCheckpoint = run => {
  const stages = Array.isArray(run.stages) ? run.stages : [];
  for (let stageIndex = 0; stageIndex < stages.length; stageIndex += 1) {
    const stage = stages[stageIndex];
    const checkpoints = Array.isArray(stage.checkpoints) ? stage.checkpoints : [];
    const checkpointIndex = checkpoints.findIndex(
      checkpoint => normalizeStatus(checkpoint.status) === 'running'
    );
    if (checkpointIndex >= 0) {
      return { stageIndex, checkpointIndex };
    }
  }
  return null;
};

const findNextPendingCheckpointIndex = stage => {
  const checkpoints = Array.isArray(stage.checkpoints) ? stage.checkpoints : [];
  return checkpoints.findIndex(checkpoint => normalizeStatus(checkpoint.status) === 'pending');
};

const performLocalAdvance = run => {
  const now = toIsoUtc();
  const stages = Array.isArray(run.stages) ? [...run.stages] : [];
  const current = findCurrentRunningCheckpoint(run);

  if (!current) {
    return appendLocalEvent(run, {
      level: 'warning',
      code: 'runbook_advance_no_running_checkpoint',
      message: localizeRunbookText(
        'Não há checkpoint em execução para avançar.',
        'There is no running checkpoint to advance.'
      ),
    });
  }

  const stage = { ...stages[current.stageIndex] };
  const checkpoints = Array.isArray(stage.checkpoints) ? [...stage.checkpoints] : [];
  const activeCheckpoint = { ...checkpoints[current.checkpointIndex] };
  activeCheckpoint.status = 'completed';
  if (!activeCheckpoint.started_at_utc) {
    activeCheckpoint.started_at_utc = now;
  }
  activeCheckpoint.completed_at_utc = now;
  checkpoints[current.checkpointIndex] = activeCheckpoint;

  const nextPendingIndex = findNextPendingCheckpointIndex({ ...stage, checkpoints });
  if (nextPendingIndex >= 0) {
    const nextCheckpoint = { ...checkpoints[nextPendingIndex] };
    nextCheckpoint.status = 'running';
    nextCheckpoint.started_at_utc = now;
    checkpoints[nextPendingIndex] = nextCheckpoint;
    stage.checkpoints = checkpoints;
    stage.status = 'running';
    stages[current.stageIndex] = stage;

    return appendLocalEvent(
      {
        ...run,
        stages,
        current_stage_index: current.stageIndex,
      },
      {
        code: 'runbook_checkpoint_advanced_local',
        stage: stage.key,
        checkpoint: nextCheckpoint.key,
        message: localizeRunbookText(
          'Checkpoint avançado em modo local.',
          'Checkpoint advanced in local mode.'
        ),
      }
    );
  }

  stage.checkpoints = checkpoints;
  stage.status = 'completed';
  stages[current.stageIndex] = stage;

  const nextStageIndex = current.stageIndex + 1;
  if (nextStageIndex < stages.length) {
    const nextStage = { ...stages[nextStageIndex] };
    const nextStageCheckpoints = Array.isArray(nextStage.checkpoints)
      ? [...nextStage.checkpoints]
      : [];

    if (nextStageCheckpoints.length > 0) {
      nextStageCheckpoints[0] = {
        ...nextStageCheckpoints[0],
        status: 'running',
        started_at_utc: now,
      };
    }

    nextStage.checkpoints = nextStageCheckpoints;
    nextStage.status = 'running';
    stages[nextStageIndex] = nextStage;

    return appendLocalEvent(
      {
        ...run,
        stages,
        current_stage_index: nextStageIndex,
      },
      {
        code: 'runbook_stage_advanced_local',
        stage: nextStage.key,
        checkpoint: nextStageCheckpoints[0] ? nextStageCheckpoints[0].key : '',
        message: localizeRunbookText(
          'Etapa avançada em modo local.',
          'Stage advanced in local mode.'
        ),
      }
    );
  }

  return appendLocalEvent(
    {
      ...run,
      stages,
      status: 'completed',
      finished_at_utc: now,
      current_stage_index: stages.length - 1,
    },
    {
      code: 'runbook_completed_local',
      stage: stage.key,
      message: localizeRunbookText(
        'Runbook concluído em modo local.',
        'Runbook completed in local mode.'
      ),
    }
  );
};

const performLocalFailure = (run, params) => {
  const now = toIsoUtc();
  const current = findCurrentRunningCheckpoint(run);
  const stages = Array.isArray(run.stages) ? [...run.stages] : [];

  if (current) {
    const stage = { ...stages[current.stageIndex] };
    const checkpoints = Array.isArray(stage.checkpoints) ? [...stage.checkpoints] : [];
    checkpoints[current.checkpointIndex] = {
      ...checkpoints[current.checkpointIndex],
      status: 'failed',
      completed_at_utc: now,
    };
    stage.checkpoints = checkpoints;
    stage.status = 'failed';
    stages[current.stageIndex] = stage;
  }

  const failedRun = {
    ...run,
    stages,
    status: 'failed',
    last_failure: {
      code: normalizeText(params && params.failure_code) || 'runbook_stage_failure',
      message:
        sanitizeSensitiveText(normalizeText(params && params.failure_message)) ||
        localizeRunbookText(
          'Falha simulada em modo local.',
          'Simulated failure in local mode.'
        ),
      stage: current
        ? normalizeText(stages[current.stageIndex] && stages[current.stageIndex].key)
        : '',
      checkpoint:
        current &&
        stages[current.stageIndex] &&
        Array.isArray(stages[current.stageIndex].checkpoints)
          ? normalizeText(stages[current.stageIndex].checkpoints[current.checkpointIndex].key)
          : '',
      timestamp_utc: now,
    },
  };

  return appendLocalEvent(failedRun, {
    level: 'error',
    code: 'runbook_failed_local',
    message:
      sanitizeSensitiveText(normalizeText(params && params.failure_message)) ||
      localizeRunbookText(
        'Falha simulada em modo local.',
        'Simulated failure in local mode.'
      ),
  });
};

const performLocalRetry = run => {
  const now = toIsoUtc();
  const stages = Array.isArray(run.stages) ? [...run.stages] : [];

  for (let stageIndex = 0; stageIndex < stages.length; stageIndex += 1) {
    const stage = { ...stages[stageIndex] };
    const checkpoints = Array.isArray(stage.checkpoints) ? [...stage.checkpoints] : [];
    const failedIndex = checkpoints.findIndex(
      checkpoint => normalizeStatus(checkpoint.status) === 'failed'
    );

    if (failedIndex >= 0) {
      checkpoints[failedIndex] = {
        ...checkpoints[failedIndex],
        status: 'running',
        started_at_utc: now,
        completed_at_utc: '',
      };
      stage.checkpoints = checkpoints;
      stage.status = 'running';
      stages[stageIndex] = stage;

      return appendLocalEvent(
        {
          ...run,
          stages,
          status: 'running',
          current_stage_index: stageIndex,
          last_failure: null,
          finished_at_utc: '',
        },
        {
          code: 'runbook_retry_local',
          stage: stage.key,
          checkpoint: checkpoints[failedIndex].key,
          message: localizeRunbookText(
            'Retomada local a partir do checkpoint com falha.',
            'Local resume started from the failed checkpoint.'
          ),
        }
      );
    }
  }

  return appendLocalEvent(
    {
      ...run,
      status: 'running',
      last_failure: null,
      finished_at_utc: '',
    },
    {
      code: 'runbook_retry_local_no_failed_checkpoint',
      message: localizeRunbookText(
        'Retomada local executada sem checkpoint marcado como failed.',
        'Local resume executed with no checkpoint marked as failed.'
      ),
    }
  );
};

const localOperateRun = (run, params) => {
  const action = normalizeText(params && params.action).toLowerCase();

  if (action === 'pause') {
    return appendLocalEvent(
      {
        ...run,
        status: 'paused',
      },
      {
        code: 'runbook_paused_local',
        message: localizeRunbookText(
          'Execução pausada em modo local.',
          'Execution paused in local mode.'
        ),
      }
    );
  }

  if (action === 'resume') {
    return appendLocalEvent(
      {
        ...run,
        status: 'running',
      },
      {
        code: 'runbook_resumed_local',
        message: localizeRunbookText(
          'Execução retomada em modo local.',
          'Execution resumed in local mode.'
        ),
      }
    );
  }

  if (action === 'advance') {
    return performLocalAdvance({
      ...run,
      status: run.status === 'paused' ? 'running' : run.status,
    });
  }

  if (action === 'fail') {
    return performLocalFailure(run, params);
  }

  if (action === 'retry') {
    return performLocalRetry(run);
  }

  return appendLocalEvent(run, {
    level: 'warning',
    code: 'runbook_unknown_action_local',
    message: localizeRunbookText(
      `Ação '${action || '-'}' não reconhecida no modo local.`,
      `Action '${action || '-'}' is not recognized in local mode.`
    ),
  });
};

export async function startRunbook(params) {
  const safeParams = sanitizeRunbookStartPayload(params);

  if (RUNBOOK_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'runbook',
      action: 'start',
      reasonCode: 'runbook_local_start_dev_only',
      details: localizeRunbookText(
        'Start local de runbook permitido apenas em modo degradado de desenvolvimento.',
        'Local runbook start is allowed only in degraded development mode.'
      ),
    });
    const run = createLocalRunFromStartParams(safeParams);
    const runs = readLocalRuns();
    runs[run.run_id] = run;
    writeLocalRuns(runs);

    return {
      run,
      executionState: mapBackendRunToExecutionState(run),
    };
  }

  try {
    const response = await request('/api/v1/runbooks/start', {
      method: 'POST',
      skipErrorHandler: true,
      data: safeParams,
    });
    const run = unwrapRunbookResponse(response);
    return {
      run,
      executionState: mapBackendRunToExecutionState(run),
    };
  } catch (error) {
    throw buildRunbookBackendError(
      error,
      localizeRunbookText(
        'Falha operacional no start do runbook oficial.',
        'Operational failure while starting the official runbook.'
      )
    );
  }
}

export async function operateRunbook(params) {
  if (RUNBOOK_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'runbook',
      action: 'operate',
      reasonCode: 'runbook_local_operate_dev_only',
      details: localizeRunbookText(
        'Operação local de runbook permitida apenas em modo degradado de desenvolvimento.',
        'Local runbook operation is allowed only in degraded development mode.'
      ),
    });
    const runId = normalizeText(params && params.run_id);
    if (!runId) {
      throw new Error(
        localizeRunbookText(
          'run_id obrigatorio para operar runbook em modo local.',
          'run_id is required to operate the runbook in local mode.'
        )
      );
    }

    const runs = readLocalRuns();
    const currentRun = runs[runId];
    if (!currentRun) {
      throw new Error(
        localizeRunbookText(
          `Execução '${runId}' não encontrada no estado local do runbook.`,
          `Execution '${runId}' was not found in the local runbook state.`
        )
      );
    }

    const nextRun = localOperateRun(currentRun, params);
    runs[runId] = nextRun;
    writeLocalRuns(runs);

    return {
      run: nextRun,
      executionState: mapBackendRunToExecutionState(nextRun),
    };
  }

  try {
    const response = await request('/api/v1/runbooks/operate', {
      method: 'POST',
      skipErrorHandler: true,
      data: params,
    });
    const run = unwrapRunbookResponse(response);
    return {
      run,
      executionState: mapBackendRunToExecutionState(run),
    };
  } catch (error) {
    throw buildRunbookBackendError(
      error,
      localizeRunbookText(
        'Falha operacional no comando de runbook.',
        'Operational failure while issuing the runbook command.'
      )
    );
  }
}

export async function runbookTechnicalPreflight(params) {
  if (RUNBOOK_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'runbook',
      action: 'preflight',
      reasonCode: 'runbook_local_preflight_dev_only',
      details: localizeRunbookText(
        'Preflight local permitido apenas em modo degradado de desenvolvimento.',
        'Local preflight is allowed only in degraded development mode.'
      ),
    });
    const safeParams = params && typeof params === 'object' ? params : {};
    const hostMapping = Array.isArray(safeParams.host_mapping) ? safeParams.host_mapping : [];
    return {
      change_id: normalizeText(safeParams.change_id),
      executed_at_utc: toIsoUtc(),
      overall_status: hostMapping.length > 0 ? 'apto' : 'bloqueado',
      summary: {
        apto: hostMapping.length,
        parcial: 0,
        bloqueado: hostMapping.length > 0 ? 0 : 1,
        total: hostMapping.length,
      },
      hosts: hostMapping.map(host => ({
        host_id: normalizeText(host.host_ref) || normalizeText(host.host_address),
        infra_label: normalizeText(host.host_ref),
        host_ref: normalizeText(host.host_ref),
        host_address: normalizeText(host.host_address),
        ssh_user: normalizeText(host.ssh_user),
        ssh_port: Number(host.ssh_port) || 22,
        status: 'apto',
        checked_at_utc: toIsoUtc(),
        primary_cause: localizeRunbookText(
          'Sem pendências técnicas (modo local).',
          'No technical issues (local mode).'
        ),
        recommended_action: localizeRunbookText(
          'Sem ação imediata.',
          'No immediate action.'
        ),
        diagnostics: {
          code: 'ssh_connectivity_ok_local_mode',
          stderr: '',
          stdout: 'local-mode',
          exit_code: 0,
        },
      })),
    };
  }

  try {
    const response = await request('/api/v1/runbooks/preflight', {
      method: 'POST',
      skipErrorHandler: true,
      data: params,
    });
    return unwrapRunbookPreflightResponse(response);
  } catch (error) {
    throw buildRunbookBackendError(
      error,
      localizeRunbookText(
        'Falha operacional no preflight técnico do runbook oficial.',
        'Operational failure during the official runbook technical preflight.'
      )
    );
  }
}

export async function getRunbookStatus(runId) {
  const normalizedRunId = normalizeText(runId);
  if (!normalizedRunId) {
    throw new Error(
      localizeRunbookText(
        'run_id obrigatorio para consultar status de runbook.',
        'run_id is required to query runbook status.'
      )
    );
  }

  const cachedOfficialRun = readCachedOfficialRun(normalizedRunId);
  if (shouldBypassOfficialRunRequest(cachedOfficialRun)) {
    const fallbackRun = attachCachedStatusFallbackMetadata(cachedOfficialRun.run, {
      source: 'official_status_cache',
      savedAtUtc: cachedOfficialRun.savedAtUtc,
      reasonCode: cachedOfficialRun.unavailable.reasonCode,
      reasonMessage:
        cachedOfficialRun.unavailable.reasonMessage ||
        localizeRunbookText(
          'Runbook oficial nao mantido mais pelo backend; usando snapshot auditavel local.',
          'Official runbook is no longer maintained by the backend; using local auditable snapshot.'
        ),
    });

    if (fallbackRun) {
      validateOfficialRunContract(fallbackRun);
      return {
        run: fallbackRun,
        executionState: mapBackendRunToExecutionState(fallbackRun),
      };
    }
  }

  if (RUNBOOK_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'runbook',
      action: 'status',
      reasonCode: 'runbook_local_status_dev_only',
      details: localizeRunbookText(
        'Consulta local de status permitida apenas em modo degradado de desenvolvimento.',
        'Local status query is allowed only in degraded development mode.'
      ),
    });
    const runs = readLocalRuns();
    const run = runs[normalizedRunId];
    if (!run) {
      throw new Error(
        localizeRunbookText(
          `Execução '${normalizedRunId}' não encontrada no estado local do runbook.`,
          `Execution '${normalizedRunId}' was not found in the local runbook state.`
        )
      );
    }

    return {
      run,
      executionState: mapBackendRunToExecutionState(run),
    };
  }

  try {
    const response = await request(`/api/v1/runbooks/${normalizedRunId}/status`, {
      skipErrorHandler: true,
    });
    const run = unwrapRunbookResponse(response);
    writeCachedOfficialRun(run);
    return {
      run,
      executionState: mapBackendRunToExecutionState(run),
    };
  } catch (error) {
    const backendCode = resolveRunbookBackendCode(error);
    if (backendCode === 'runbook_not_found') {
      markCachedOfficialRunUnavailable(normalizedRunId, error);
    }

    const fallbackCachedRun = readCachedOfficialRun(normalizedRunId);
    if (fallbackCachedRun && fallbackCachedRun.run) {
      const fallbackRun = attachCachedStatusFallbackMetadata(fallbackCachedRun.run, {
        source: 'official_status_cache',
        savedAtUtc: fallbackCachedRun.savedAtUtc,
        reasonCode: backendCode,
        reasonMessage: resolveRunbookBackendMessage(error),
      });

      if (fallbackRun) {
        validateOfficialRunContract(fallbackRun);
        return {
          run: fallbackRun,
          executionState: mapBackendRunToExecutionState(fallbackRun),
        };
      }
    }

    const historicalFallbackRun = readHistoricalFallbackRun(normalizedRunId);
    if (historicalFallbackRun) {
      const fallbackRun = attachCachedStatusFallbackMetadata(historicalFallbackRun, {
        source: 'audit_history_fallback',
        savedAtUtc: normalizeText(historicalFallbackRun.updated_at_utc),
        reasonCode: normalizeText(error && error.data && error.data.code),
        reasonMessage:
          normalizeText(error && error.data && (error.data.detail || error.data.message)) ||
          normalizeText(error && error.message),
      });

      if (fallbackRun) {
        validateOfficialRunContract(fallbackRun);
        return {
          run: fallbackRun,
          executionState: mapBackendRunToExecutionState(fallbackRun),
        };
      }
    }

    throw buildRunbookBackendError(
      error,
      localizeRunbookText(
        'Falha operacional na consulta de status do runbook.',
        'Operational failure while querying runbook status.'
      )
    );
  }
}

export async function getRunbookCatalog() {
  if (RUNBOOK_LOCAL_MODE_ENABLED) {
    const runs = readLocalRuns();
    return Object.values(runs || {}).map(run => ({
      key: normalizeText(run && run.run_id),
      runId: normalizeText(run && run.run_id),
      changeId: normalizeText(run && run.change_id),
      status: normalizeText(run && run.status).toLowerCase(),
      finishedAt: normalizeText(run && run.completed_at_utc),
      capturedAt: normalizeText(run && (run.updated_at_utc || run.started_at_utc)),
      context: {
        providerKey: normalizeText(run && run.provider_key),
        environmentProfile: normalizeText(run && run.environment_profile),
        hostCount: Array.isArray(run && run.host_mapping) ? run.host_mapping.length : 0,
        organizationCount:
          run && run.topology_catalog && Array.isArray(run.topology_catalog.organizations)
            ? run.topology_catalog.organizations.length
            : 0,
        nodeCount: Array.isArray(run && run.host_mapping) ? run.host_mapping.length : 0,
        apiCount: Array.isArray(run && run.api_registry) ? run.api_registry.length : 0,
        incrementalCount:
          Array.isArray(run && run.incremental_expansions) ? run.incremental_expansions.length : 0,
        organizations: [],
        topology: (run && run.topology_catalog) || {},
        host_mapping: Array.isArray(run && run.host_mapping) ? run.host_mapping : [],
        machine_credentials:
          Array.isArray(run && run.machine_credentials) ? run.machine_credentials : [],
        handoff_fingerprint: normalizeText(run && run.handoff_fingerprint),
      },
    }));
  }

  const response = await request('/api/v1/runbooks/catalog', {
    skipErrorHandler: true,
  });
  return unwrapRunbookCatalogResponse(response);
}

export async function getRunbookRuntimeInspection(runId, params = {}) {
  const normalizedRunId = normalizeText(runId);
  if (!normalizedRunId) {
    throw new Error(
      localizeRunbookText(
        'run_id obrigatorio para consultar inspecao runtime.',
        'run_id is required to query runtime inspection.'
      )
    );
  }

  const cachedOfficialRun = readCachedOfficialRun(normalizedRunId);

  const queryParams = new URLSearchParams();
  const orgId = normalizeText(params.orgId || params.org_id);
  const hostId = normalizeText(params.hostId || params.host_id);
  const componentId = normalizeText(params.componentId || params.component_id);
  const inspectionScope = normalizeText(params.inspectionScope || params.inspection_scope || 'all');

  if (!componentId) {
    throw new Error(
      localizeRunbookText(
        'component_id obrigatorio para consultar inspecao runtime.',
        'component_id is required to query runtime inspection.'
      )
    );
  }

  if (shouldBypassOfficialRunRequest(cachedOfficialRun)) {
    const fallbackInspection = buildCachedRuntimeInspectionFallback(cachedOfficialRun, params, {
      source: 'official_status_cache',
      savedAtUtc: cachedOfficialRun.savedAtUtc,
      reasonCode: cachedOfficialRun.unavailable.reasonCode,
      reasonMessage:
        cachedOfficialRun.unavailable.reasonMessage ||
        localizeRunbookText(
          'Runbook oficial nao mantido mais pelo backend; usando cache auditavel local.',
          'Official runbook is no longer maintained by the backend; using local auditable cache.'
        ),
    });
    if (fallbackInspection) {
      return fallbackInspection;
    }
  }

  if (orgId) {
    queryParams.set('org_id', orgId);
  }
  if (hostId) {
    queryParams.set('host_id', hostId);
  }
  queryParams.set('component_id', componentId);
  queryParams.set('inspection_scope', inspectionScope || 'all');
  if (normalizeBoolean(params.refresh)) {
    queryParams.set('refresh', 'true');
  }

  try {
    const response = await request(
      `/api/v1/runbooks/${normalizedRunId}/runtime-inspection?${queryParams.toString()}`,
      {
        skipErrorHandler: true,
      }
    );
    const inspection = unwrapRunbookInspectionResponse(response);
    persistCachedRuntimeInspection(normalizedRunId, inspection);
    return inspection;
  } catch (error) {
    const backendCode = resolveRunbookBackendCode(error);
    if (backendCode === 'runbook_not_found') {
      markCachedOfficialRunUnavailable(normalizedRunId, error);
      const fallbackCachedRun = readCachedOfficialRun(normalizedRunId);
      const fallbackInspection = buildCachedRuntimeInspectionFallback(fallbackCachedRun, params, {
        source: 'official_status_cache',
        savedAtUtc: normalizeText(fallbackCachedRun && fallbackCachedRun.savedAtUtc),
        reasonCode: backendCode,
        reasonMessage: resolveRunbookBackendMessage(error),
      });
      if (fallbackInspection) {
        return fallbackInspection;
      }
    }

    throw buildRunbookBackendError(
      error,
      localizeRunbookText(
        'Falha operacional na consulta de inspecao runtime oficial.',
        'Operational failure while querying official runtime inspection.'
      )
    );
  }
}
