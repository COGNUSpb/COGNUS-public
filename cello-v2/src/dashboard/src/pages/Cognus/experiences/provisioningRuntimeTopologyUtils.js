import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const isPlainObject = value => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const normalizeText = value => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim();
};

const normalizeLower = value => normalizeText(value).toLowerCase();

const firstNonEmptyText = (...values) =>
  values.map(item => normalizeText(item)).find(Boolean) || '';

const safeArray = value => (Array.isArray(value) ? value : []);

const toSafeInteger = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizeOrgLookup = value =>
  normalizeLower(value)
    .replace(/[^a-z0-9]+/g, '')
    .trim();

const normalizeArtifactToken = value =>
  normalizeLower(value)
    .replace(/\\/g, '/')
    .replace(/:/g, '/')
    .replace(/\/{2,}/g, '/')
    .replace(/\.json$/g, '');

const normalizeA2AReadinessStatus = value => {
  const normalized = normalizeLower(value);
  if (
    normalized === 'implemented' ||
    normalized === 'partial' ||
    normalized === 'pending' ||
    normalized === 'blocked'
  ) {
    return normalized;
  }
  return 'pending';
};

const normalizeA2AGateMode = value => {
  const normalized = normalizeLower(value);
  if (normalized === 'interactive' || normalized === 'read_only_blocked') {
    return normalized;
  }
  return 'read_only_blocked';
};

const normalizeComponentType = value => {
  const normalized = normalizeLower(value).replace(/[^a-z0-9]+/g, '');
  if (normalized === 'peer') {
    return 'peer';
  }
  if (normalized === 'orderer') {
    return 'orderer';
  }
  if (normalized === 'ca') {
    return 'ca';
  }
  if (normalized === 'couch' || normalized === 'couchdb') {
    return 'couch';
  }
  if (normalized === 'apigateway' || normalized === 'apigw') {
    return 'apiGateway';
  }
  if (
    normalized === 'netapi' ||
    normalized === 'networkapi' ||
    normalized === 'networkgateway' ||
    normalized === 'networkservice'
  ) {
    return 'netapi';
  }
  if (normalized === 'chaincoderuntime' || normalized === 'runtimechaincode') {
    return 'chaincode_runtime';
  }
  return '';
};

const normalizeScope = value => {
  const normalized = normalizeLower(value);
  if (normalized === 'required') {
    return 'required';
  }
  if (normalized === 'planned') {
    return 'planned';
  }
  if (normalized === 'optional') {
    return 'optional';
  }
  return '';
};

const normalizeCriticality = value => {
  const normalized = normalizeLower(value);
  if (normalized === 'critical') {
    return 'critical';
  }
  if (normalized === 'supporting') {
    return 'supporting';
  }
  return '';
};

const normalizePipelineStatus = value => {
  const normalized = normalizeLower(value);
  if (
    normalized === 'idle' ||
    normalized === 'running' ||
    normalized === 'paused' ||
    normalized === 'failed' ||
    normalized === 'completed' ||
    normalized === 'pending'
  ) {
    return normalized;
  }
  return 'pending';
};

const normalizeDecision = value => (normalizeLower(value) === 'allow' ? 'allow' : 'block');

const toSortableTimestamp = value => normalizeText(value) || '0000-00-00T00:00:00Z';
const localizeRuntimeTopologyText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

export const ORG_RUNTIME_COMPONENT_TYPES = Object.freeze({
  peer: 'peer',
  orderer: 'orderer',
  ca: 'ca',
  couch: 'couch',
  apiGateway: 'apiGateway',
  netapi: 'netapi',
  chaincodeRuntime: 'chaincode_runtime',
});

export const ORG_RUNTIME_COMPONENT_TYPE_LABEL = Object.freeze({
  peer: 'peer',
  orderer: 'orderer',
  ca: 'ca',
  couch: 'couch',
  apiGateway: 'apiGateway',
  netapi: 'netapi',
  chaincode_runtime: 'chaincode-runtime',
});

export const ORG_RUNTIME_SCOPE = Object.freeze({
  required: 'required',
  planned: 'planned',
  optional: 'optional',
});

export const ORG_RUNTIME_SCOPE_LABEL = Object.freeze({
  required: 'required',
  planned: 'planned',
  optional: 'optional',
});

export const ORG_RUNTIME_SCOPE_TONE_MAP = Object.freeze({
  required: 'error',
  planned: 'warning',
  optional: 'success',
});

export const ORG_RUNTIME_CRITICALITY = Object.freeze({
  critical: 'critical',
  supporting: 'supporting',
});

export const ORG_RUNTIME_CRITICALITY_LABEL = Object.freeze({
  critical: 'critical',
  supporting: 'supporting',
});

export const ORG_RUNTIME_CRITICALITY_TONE_MAP = Object.freeze({
  critical: 'error',
  supporting: 'processing',
});

export const ORG_RUNTIME_STATUS = Object.freeze({
  running: 'running',
  degraded: 'degraded',
  planned: 'planned',
  stopped: 'stopped',
  missing: 'missing',
  unknown: 'unknown',
});

export const ORG_RUNTIME_STATUS_LABEL = Object.freeze({
  running: 'running',
  degraded: 'degraded',
  planned: 'planned',
  stopped: 'stopped',
  missing: 'missing',
  unknown: 'unknown',
});

export const ORG_RUNTIME_STATUS_TONE_MAP = Object.freeze({
  running: 'success',
  degraded: 'warning',
  planned: 'warning',
  stopped: 'error',
  missing: 'error',
  unknown: 'default',
});

export const ORG_RUNTIME_STATUS_SOURCE_LABEL = Object.freeze({
  runtime_telemetry: localizeRuntimeTopologyText(
    'runtime_telemetry oficial',
    'official runtime_telemetry'
  ),
  host_inventory: localizeRuntimeTopologyText('host_inventory oficial', 'official host_inventory'),
  artifact_rows: localizeRuntimeTopologyText('artifact_rows oficial', 'official artifact_rows'),
  official_decision: 'official_decision',
  run_status: localizeRuntimeTopologyText('status oficial do run', 'official run status'),
  topology_catalog: localizeRuntimeTopologyText(
    'topology_catalog oficial',
    'official topology_catalog'
  ),
});

const COMPONENT_TYPE_SORT_ORDER = Object.freeze({
  peer: 1,
  orderer: 2,
  ca: 3,
  couch: 4,
  apiGateway: 5,
  netapi: 6,
  chaincode_runtime: 7,
});

const STATUS_SORT_ORDER = Object.freeze({
  missing: 1,
  stopped: 2,
  degraded: 3,
  unknown: 4,
  planned: 5,
  running: 6,
});

const DEFAULT_SCOPE_BY_COMPONENT_TYPE = Object.freeze({
  peer: ORG_RUNTIME_SCOPE.required,
  orderer: ORG_RUNTIME_SCOPE.required,
  ca: ORG_RUNTIME_SCOPE.required,
  couch: ORG_RUNTIME_SCOPE.required,
  apiGateway: ORG_RUNTIME_SCOPE.required,
  netapi: ORG_RUNTIME_SCOPE.required,
  chaincode_runtime: ORG_RUNTIME_SCOPE.optional,
});

const DEFAULT_CRITICALITY_BY_COMPONENT_TYPE = Object.freeze({
  peer: ORG_RUNTIME_CRITICALITY.critical,
  orderer: ORG_RUNTIME_CRITICALITY.critical,
  ca: ORG_RUNTIME_CRITICALITY.critical,
  couch: ORG_RUNTIME_CRITICALITY.supporting,
  apiGateway: ORG_RUNTIME_CRITICALITY.supporting,
  netapi: ORG_RUNTIME_CRITICALITY.supporting,
  chaincode_runtime: ORG_RUNTIME_CRITICALITY.supporting,
});

const buildStageStatusByKey = stages =>
  safeArray(stages).reduce((accumulator, stage) => {
    const stageKey = normalizeText(stage && stage.key);
    if (!stageKey) {
      return accumulator;
    }

    accumulator[stageKey] = normalizePipelineStatus(stage && stage.status);
    return accumulator;
  }, {});

const collectArtifactKeySet = artifactRows => {
  const tokens = new Set();
  safeArray(artifactRows).forEach(row => {
    const keyToken = normalizeArtifactToken(row && row.key);
    const artifactKeyToken = normalizeArtifactToken(row && row.artifact_key);
    if (keyToken) {
      tokens.add(keyToken);
    }
    if (artifactKeyToken) {
      tokens.add(artifactKeyToken);
    }
  });
  return tokens;
};

const collectA2AOrganizationRows = run => {
  const safeRun = isPlainObject(run) ? run : {};
  const topologyCatalog = isPlainObject(safeRun.topology_catalog) ? safeRun.topology_catalog : {};
  const organizations = safeArray(topologyCatalog.organizations);
  const dedup = new Set();
  const rows = [];

  organizations.forEach((organization, index) => {
    const organizationId = firstNonEmptyText(
      organization && (organization.org_id || organization.orgId || organization.org_key),
      `org-${index + 1}`
    );
    const organizationName = firstNonEmptyText(
      organization && (organization.org_name || organization.orgName),
      organizationId
    );
    const organizationLookup = normalizeOrgLookup(organizationId || organizationName);
    if (!organizationLookup || dedup.has(organizationLookup)) {
      return;
    }
    dedup.add(organizationLookup);
    rows.push({
      organizationId,
      organizationName,
      organizationLookup,
    });
  });

  safeArray(safeRun.host_inventory).forEach(row => {
    const organizationId = normalizeText(row && row.org_id);
    const organizationLookup = normalizeOrgLookup(organizationId);
    if (!organizationLookup || dedup.has(organizationLookup)) {
      return;
    }
    dedup.add(organizationLookup);
    rows.push({
      organizationId,
      organizationName: organizationId,
      organizationLookup,
    });
  });

  return rows.sort((left, right) =>
    normalizeText(left.organizationName).localeCompare(normalizeText(right.organizationName))
  );
};

const buildFallbackA2AEntryGate = (run, evidenceContext) => {
  const safeRun = isPlainObject(run) ? run : {};
  const safeEvidenceContext = isPlainObject(evidenceContext) ? evidenceContext : {};
  const requiredArtifacts = [
    'inventory-final',
    'verify-report',
    'stage-reports',
    'ssh-execution-log',
  ];
  const artifactChecks = {
    'inventory-final': Boolean(safeEvidenceContext.inventoryArtifactAvailable),
    'verify-report': Boolean(safeEvidenceContext.verifyArtifactAvailable),
    'stage-reports': Boolean(safeEvidenceContext.stageReportsArtifactAvailable),
    'ssh-execution-log': Boolean(safeEvidenceContext.sshExecutionLogArtifactAvailable),
  };
  const missingArtifacts = requiredArtifacts.filter(key => !artifactChecks[key]);
  const correlationChecks = {
    run_id_present: Boolean(safeEvidenceContext.runId),
    change_id_present: Boolean(safeEvidenceContext.changeId),
    manifest_fingerprint_present: Boolean(safeEvidenceContext.manifestFingerprint),
    source_blueprint_fingerprint_present: Boolean(safeEvidenceContext.sourceBlueprintFingerprint),
  };
  const correlationValid = Object.values(correlationChecks).every(Boolean);
  const officialArtifactsReady = missingArtifacts.length === 0;
  const baselineConverged =
    normalizeLower(safeEvidenceContext.backendState) === 'ready' &&
    normalizeLower(safeEvidenceContext.pipelineStatus) === 'completed' &&
    normalizeLower(safeEvidenceContext.decision) === 'allow' &&
    Boolean(safeEvidenceContext.evidenceMinimumValid) &&
    correlationValid &&
    officialArtifactsReady;

  let status = 'partial';
  if (baselineConverged) {
    status = 'implemented';
  } else if (!correlationValid) {
    status = 'blocked';
  } else if (safeEvidenceContext.pipelineStatus === 'pending' || safeEvidenceContext.pipelineStatus === 'running' || safeEvidenceContext.pipelineStatus === 'paused') {
    status = 'pending';
  } else if (safeEvidenceContext.pipelineStatus === 'failed' || safeEvidenceContext.decision === 'block') {
    status = 'blocked';
  }

  const issueCodes = [];
  if (!correlationValid) {
    issueCodes.push('runbook_a2a_correlation_invalid');
  }
  if (!officialArtifactsReady) {
    issueCodes.push('runbook_a2a_required_artifacts_missing');
  }
  if (!baselineConverged && correlationValid && officialArtifactsReady) {
    issueCodes.push('runbook_a2a_baseline_not_converged');
  }

  const evidenceByOrg = safeArray(safeRun.host_inventory).reduce((accumulator, row) => {
    const organizationLookup = normalizeOrgLookup(row && row.org_id);
    if (!organizationLookup) {
      return accumulator;
    }
    accumulator[organizationLookup] = (accumulator[organizationLookup] || 0) + 1;
    return accumulator;
  }, {});

  const organizationReadiness = collectA2AOrganizationRows(safeRun).map(organization => {
    const observedHostCount = evidenceByOrg[organization.organizationLookup] || 0;
    const observedInRuntime = observedHostCount > 0;
    let organizationStatus = 'pending';

    if (!correlationValid) {
      organizationStatus = 'blocked';
    } else if (safeEvidenceContext.pipelineStatus === 'failed' || safeEvidenceContext.decision === 'block') {
      organizationStatus = 'blocked';
    } else if (baselineConverged && observedInRuntime) {
      organizationStatus = 'implemented';
    } else if (
      safeEvidenceContext.pipelineStatus === 'pending' ||
      safeEvidenceContext.pipelineStatus === 'running' ||
      safeEvidenceContext.pipelineStatus === 'paused'
    ) {
      organizationStatus = observedInRuntime ? 'partial' : 'pending';
    } else if (officialArtifactsReady && observedInRuntime) {
      organizationStatus = 'partial';
    }

    return {
      organization_id: organization.organizationId,
      organization_name: organization.organizationName,
      status: organizationStatus,
      mode: organizationStatus === 'implemented' ? 'interactive' : 'read_only_blocked',
      observed_host_count: observedHostCount,
      observed_in_runtime: observedInRuntime,
      issue_codes: issueCodes,
    };
  });

  return {
    contractVersion: 'a2a-entry-gate.v1-fallback',
    sourceOfTruth: 'official_runbook_payload_fallback',
    status,
    mode: status === 'implemented' ? 'interactive' : 'read_only_blocked',
    interactiveEnabled: status === 'implemented',
    correlationValid,
    baselineConverged,
    officialArtifactsReady,
    requiredArtifacts,
    missingArtifacts,
    artifactChecks,
    actionAvailability: {
      openOperationalDashboard: correlationValid,
      addPeer: status === 'implemented',
      addOrderer: status === 'implemented',
      addChannel: status === 'implemented',
      addChaincode: status === 'implemented',
    },
    organizationReadiness,
    issues: issueCodes.map(code => ({ code })),
    updatedAtUtc: safeEvidenceContext.updatedAtUtc || '',
  };
};

const normalizeA2AEntryGate = (gate, run, evidenceContext) => {
  const fallbackGate = buildFallbackA2AEntryGate(run, evidenceContext);
  let cachedStatusFallback = null;
  if (isPlainObject(run) && isPlainObject(run.cached_status_fallback)) {
    cachedStatusFallback = run.cached_status_fallback;
  } else if (
    isPlainObject(run) &&
    isPlainObject(run.snapshot) &&
    isPlainObject(run.snapshot.cached_status_fallback)
  ) {
    cachedStatusFallback = run.snapshot.cached_status_fallback;
  }
  if (!isPlainObject(gate)) {
    if (!cachedStatusFallback) {
      return fallbackGate;
    }

    return {
      ...fallbackGate,
      status: 'partial',
      mode: 'read_only_blocked',
      interactiveEnabled: false,
      actionAvailability: {
        ...fallbackGate.actionAvailability,
        addPeer: false,
        addOrderer: false,
        addChannel: false,
        addChaincode: false,
      },
      issues: [
        ...fallbackGate.issues,
        {
          code: 'runbook_a2a_status_cache_fallback',
          message: localizeRuntimeTopologyText(
            'Run oficial indisponivel no backend; exibindo ultimo snapshot oficial cacheado apenas para auditoria.',
            'Official run unavailable in the backend; showing the latest official snapshot cached only for auditing.'
          ),
          severity: 'warning',
        },
      ],
    };
  }

  const actionAvailability = isPlainObject(gate.action_availability)
    ? gate.action_availability
    : {};

  const normalizedGate = {
    contractVersion: normalizeText(gate.contract_version) || fallbackGate.contractVersion,
    sourceOfTruth: normalizeText(gate.source_of_truth) || fallbackGate.sourceOfTruth,
    status: normalizeA2AReadinessStatus(gate.status || fallbackGate.status),
    mode: normalizeA2AGateMode(gate.mode || fallbackGate.mode),
    interactiveEnabled:
      typeof gate.interactive_enabled === 'boolean'
        ? gate.interactive_enabled
        : fallbackGate.interactiveEnabled,
    correlationValid:
      typeof gate.correlation_valid === 'boolean'
        ? gate.correlation_valid
        : fallbackGate.correlationValid,
    baselineConverged:
      typeof gate.baseline_converged === 'boolean'
        ? gate.baseline_converged
        : fallbackGate.baselineConverged,
    officialArtifactsReady:
      typeof gate.official_artifacts_ready === 'boolean'
        ? gate.official_artifacts_ready
        : fallbackGate.officialArtifactsReady,
    requiredArtifacts: safeArray(gate.required_artifacts)
      .map(value => normalizeText(value))
      .filter(Boolean),
    missingArtifacts: safeArray(gate.missing_artifacts)
      .map(value => normalizeText(value))
      .filter(Boolean),
    artifactChecks: isPlainObject(gate.artifact_checks)
      ? gate.artifact_checks
      : fallbackGate.artifactChecks,
    actionAvailability: {
      openOperationalDashboard:
        typeof actionAvailability.open_operational_dashboard === 'boolean'
          ? actionAvailability.open_operational_dashboard
          : fallbackGate.actionAvailability.openOperationalDashboard,
      addPeer:
        typeof actionAvailability.add_peer === 'boolean'
          ? actionAvailability.add_peer
          : fallbackGate.actionAvailability.addPeer,
      addOrderer:
        typeof actionAvailability.add_orderer === 'boolean'
          ? actionAvailability.add_orderer
          : fallbackGate.actionAvailability.addOrderer,
      addChannel:
        typeof actionAvailability.add_channel === 'boolean'
          ? actionAvailability.add_channel
          : fallbackGate.actionAvailability.addChannel,
      addChaincode:
        typeof actionAvailability.add_chaincode === 'boolean'
          ? actionAvailability.add_chaincode
          : fallbackGate.actionAvailability.addChaincode,
    },
    organizationReadiness: safeArray(gate.organization_readiness).map(row => ({
      organizationId: normalizeText(row && row.organization_id),
      organizationName: firstNonEmptyText(
        row && row.organization_name,
        row && row.organization_id
      ),
      status: normalizeA2AReadinessStatus(row && row.status),
      mode: normalizeA2AGateMode(row && row.mode),
      observedHostCount: toSafeInteger(row && row.observed_host_count),
      observedInRuntime: Boolean(row && row.observed_in_runtime),
      issueCodes: safeArray(row && row.issue_codes)
        .map(value => normalizeText(value))
        .filter(Boolean),
    })),
    issues: safeArray(gate.issues).map(issue => ({
      code: normalizeText(issue && issue.code),
      message: normalizeText(issue && issue.message),
      severity: normalizeLower(issue && issue.severity) || 'error',
      cause: normalizeText(issue && issue.cause),
      impact: normalizeText(issue && issue.impact),
      recommendedAction: normalizeText(issue && issue.recommended_action),
    })),
    updatedAtUtc: normalizeText(gate.updated_at_utc) || fallbackGate.updatedAtUtc,
  };

  if (!cachedStatusFallback) {
    return normalizedGate;
  }

  return {
    ...normalizedGate,
    status: normalizedGate.status === 'blocked' ? 'blocked' : 'partial',
    mode: 'read_only_blocked',
    interactiveEnabled: false,
    actionAvailability: {
      ...normalizedGate.actionAvailability,
      addPeer: false,
      addOrderer: false,
      addChannel: false,
      addChaincode: false,
    },
    issues: [
      ...normalizedGate.issues,
      {
        code: 'runbook_a2a_status_cache_fallback',
        message: localizeRuntimeTopologyText(
          'Run oficial indisponivel no backend; exibindo ultimo snapshot oficial cacheado apenas para auditoria.',
          'Official run unavailable in the backend; showing the latest official snapshot cached only for auditing.'
        ),
        severity: 'warning',
      },
    ],
  };
};

const hasArtifact = (artifactKeySet, artifactPath) =>
  artifactKeySet.has(normalizeArtifactToken(artifactPath));

const flattenHostEvidenceRows = run => {
  const safeRun = isPlainObject(run) ? run : {};
  const hostInventoryRows = safeArray(safeRun.host_inventory);
  const hostStageEvidences = isPlainObject(safeRun.host_stage_evidences)
    ? safeRun.host_stage_evidences
    : {};
  const flattenedRows = [];

  hostInventoryRows.forEach(row => {
    if (!isPlainObject(row)) {
      return;
    }
    flattenedRows.push({
      stage: normalizeText(row.stage),
      checkpoint: normalizeText(row.checkpoint),
      hostRef: normalizeText(row.host_ref),
      organizationId: normalizeText(row.org_id),
      nodeId: normalizeText(row.node_id),
      timestampUtc: firstNonEmptyText(row.timestamp_utc, row.timestamp),
      exitCode: toSafeInteger(row.exit_code),
    });
  });

  Object.entries(hostStageEvidences).forEach(([stageKey, rows]) => {
    safeArray(rows).forEach(row => {
      if (!isPlainObject(row)) {
        return;
      }
      flattenedRows.push({
        stage: normalizeText(row.stage) || normalizeText(stageKey),
        checkpoint: normalizeText(row.checkpoint),
        hostRef: normalizeText(firstNonEmptyText(row.host_ref, row.hostRef)),
        organizationId: normalizeText(firstNonEmptyText(row.org_id, row.orgId)),
        nodeId: normalizeText(firstNonEmptyText(row.node_id, row.nodeId)),
        timestampUtc: firstNonEmptyText(row.timestamp_utc, row.timestamp),
        exitCode: toSafeInteger(row.exit_code),
      });
    });
  });

  return flattenedRows.sort((left, right) =>
    toSortableTimestamp(left.timestampUtc).localeCompare(toSortableTimestamp(right.timestampUtc))
  );
};

const buildEvidenceContext = officialRun => {
  const safeRun = isPlainObject(officialRun) ? officialRun : {};
  const stageStatusByKey = buildStageStatusByKey(safeRun.stages);
  const artifactKeySet = collectArtifactKeySet(safeRun.artifact_rows);
  const pipelineStatus = normalizePipelineStatus(safeRun.status);
  let decisionPayload = {};
  if (isPlainObject(safeRun.official_decision)) {
    decisionPayload = safeRun.official_decision;
  } else if (isPlainObject(safeRun.snapshot && safeRun.snapshot.official_decision)) {
    decisionPayload = safeRun.snapshot.official_decision;
  }
  const verifyArtifactAvailable = hasArtifact(artifactKeySet, 'stage-reports/verify-report.json');
  const inventoryArtifactAvailable = hasArtifact(artifactKeySet, 'inventory-final.json');
  const stageReportsArtifactAvailable = hasArtifact(artifactKeySet, 'stage-reports');
  const sshExecutionLogArtifactAvailable = hasArtifact(artifactKeySet, 'ssh-execution-log');

  const evidenceContext = {
    hasOfficialRun: Boolean(normalizeText(safeRun.run_id)),
    runId: normalizeText(safeRun.run_id),
    changeId: normalizeText(safeRun.change_id),
    manifestFingerprint: normalizeText(safeRun.manifest_fingerprint),
    sourceBlueprintFingerprint: normalizeText(safeRun.source_blueprint_fingerprint),
    handoffFingerprint: normalizeText(safeRun.handoff_fingerprint),
    backendState: normalizeLower(
      safeRun.backend_state || (safeRun.snapshot && safeRun.snapshot.backend_state)
    ),
    pipelineStatus,
    decision: normalizeDecision(decisionPayload.decision),
    decisionCode: normalizeText(decisionPayload.decision_code || decisionPayload.decisionCode),
    evidenceMinimumValid: Boolean(
      decisionPayload.evidence_minimum_valid || decisionPayload.evidenceMinimumValid
    ),
    verifyArtifactAvailable,
    inventoryArtifactAvailable,
    stageReportsArtifactAvailable,
    sshExecutionLogArtifactAvailable,
    requiredEvidenceKeys: safeArray(
      decisionPayload.required_evidence_keys || decisionPayload.requiredEvidenceKeys
    )
      .map(value => normalizeText(value))
      .filter(Boolean),
    missingEvidenceKeys: safeArray(
      decisionPayload.missing_evidence_keys || decisionPayload.missingEvidenceKeys
    )
      .map(value => normalizeText(value))
      .filter(Boolean),
    stageStatusByKey,
    hostEvidenceRows: flattenHostEvidenceRows(safeRun),
    updatedAtUtc: firstNonEmptyText(
      safeRun.updated_at_utc,
      safeRun.snapshot && safeRun.snapshot.last_updated_at_utc
    ),
  };

  let a2aEntryGatePayload = null;
  if (isPlainObject(safeRun.a2a_entry_gate)) {
    a2aEntryGatePayload = safeRun.a2a_entry_gate;
  } else if (isPlainObject(safeRun.snapshot && safeRun.snapshot.a2a_entry_gate)) {
    a2aEntryGatePayload = safeRun.snapshot.a2a_entry_gate;
  }

  return {
    ...evidenceContext,
    a2aEntryGate: normalizeA2AEntryGate(a2aEntryGatePayload, safeRun, evidenceContext),
    // eslint-disable-next-line no-use-before-define
    runtimeTelemetry: normalizeRuntimeTelemetryContract(safeRun),
  };
};

const normalizeRuntimeSemanticStatus = value => {
  const normalized = normalizeLower(value);
  if (
    normalized === 'running' ||
    normalized === 'degraded' ||
    normalized === 'planned' ||
    normalized === 'stopped' ||
    normalized === 'missing' ||
    normalized === 'unknown'
  ) {
    return normalized;
  }
  return 'unknown';
};

const normalizeWorkspaceHealth = value => {
  const normalized = normalizeLower(value);
  if (
    normalized === 'healthy' ||
    normalized === 'degraded' ||
    normalized === 'planned' ||
    normalized === 'unknown'
  ) {
    return normalized;
  }
  return 'unknown';
};

const normalizeWorkspaceObservationState = value => {
  const normalized = normalizeLower(value);
  if (normalized === 'observed' || normalized === 'planned' || normalized === 'not_observed') {
    return normalized;
  }
  return 'not_observed';
};

const normalizeReadModelArtifactOrigins = value =>
  safeArray(value)
    .filter(isPlainObject)
    .map(row => ({
      artifact: normalizeText(row.artifact),
      available: Boolean(row.available),
      matchedTokens: safeArray(row.matched_tokens)
        .map(item => normalizeText(item))
        .filter(Boolean),
      primaryToken: normalizeText(row.primary_token),
    }))
    .filter(row => row.artifact);

const normalizeReadModelDetailRef = value => {
  if (!isPlainObject(value)) {
    return {};
  }

  return {
    kind: normalizeText(value.kind),
    orgId: normalizeText(value.org_id),
    hostId: normalizeText(value.host_id),
    componentId: normalizeText(value.component_id),
    channelId: normalizeText(value.channel_id),
    groupId: normalizeText(value.group_id),
  };
};

const normalizeReadModelItem = value => {
  if (!isPlainObject(value)) {
    return null;
  }

  let normalizedStatus = normalizeRuntimeSemanticStatus(value.status);
  if (normalizedStatus === 'unknown') {
    const healthAsStatus = normalizeWorkspaceHealth(value.status);
    if (healthAsStatus === 'healthy') {
      normalizedStatus = 'running';
    } else if (healthAsStatus === 'degraded') {
      normalizedStatus = 'degraded';
    } else if (healthAsStatus === 'planned') {
      normalizedStatus = 'planned';
    }
  }

  return {
    id: firstNonEmptyText(
      value.component_id,
      value.channel_id,
      value.group_id,
      value.member_id,
      value.chaincode_id,
      value.name
    ),
    name: firstNonEmptyText(
      value.name,
      value.channel_id,
      value.member_name,
      value.chaincode_id,
      value.component_id,
      value.group_id
    ),
    componentId: normalizeText(value.component_id),
    componentType: normalizeComponentType(value.component_type),
    containerName: normalizeText(value.container_name),
    hostId: normalizeText(value.host_id),
    status: normalizedStatus,
    health: normalizeWorkspaceHealth(value.health),
    scope: normalizeScope(value.scope),
    criticality: normalizeCriticality(value.criticality),
    observationState: normalizeWorkspaceObservationState(value.observation_state),
    observedAt: normalizeText(value.observed_at),
    channelRefs: safeArray(value.channel_refs)
      .map(item => normalizeText(item))
      .filter(Boolean),
    chaincodeRefs: safeArray(value.chaincode_refs)
      .map(item => normalizeText(item))
      .filter(Boolean),
    memberOrgs: safeArray(value.member_orgs)
      .map(item => normalizeText(item))
      .filter(Boolean),
    membershipRole: normalizeText(value.membership_role),
    memberName: firstNonEmptyText(value.member_name, value.member_id),
    peerTotal: toSafeInteger(value.peer_total),
    ordererTotal: toSafeInteger(value.orderer_total),
    chaincodeTotal: toSafeInteger(value.chaincode_total),
    apiTotal: toSafeInteger(value.api_total),
    channelTotal: toSafeInteger(value.channel_total),
    detailRef: normalizeReadModelDetailRef(value.detail_ref),
    sourceArtifacts: normalizeReadModelArtifactOrigins(value.source_artifacts),
  };
};

const normalizeWorkspaceBlock = (value, fallbackBlockId, fallbackTitle) => {
  if (!isPlainObject(value)) {
    return {
      blockId: fallbackBlockId,
      title: fallbackTitle,
      itemTotal: 0,
      statusTotals: {
        running: 0,
        degraded: 0,
        planned: 0,
        stopped: 0,
        missing: 0,
        unknown: 0,
      },
      health: 'unknown',
      observationState: 'not_observed',
      hostIds: [],
      detailRefs: [],
      items: [],
      sourceArtifacts: [],
    };
  }

  const statusTotals = {
    running: 0,
    degraded: 0,
    planned: 0,
    stopped: 0,
    missing: 0,
    unknown: 0,
    ...(isPlainObject(value.status_totals) ? value.status_totals : {}),
  };

  return {
    blockId: firstNonEmptyText(value.block_id, fallbackBlockId),
    title: firstNonEmptyText(value.title, fallbackTitle),
    itemTotal: toSafeInteger(value.item_total),
    statusTotals,
    health: normalizeWorkspaceHealth(value.health),
    observationState: normalizeWorkspaceObservationState(value.observation_state),
    hostIds: safeArray(value.host_ids)
      .map(item => normalizeText(item))
      .filter(Boolean),
    detailRefs: safeArray(value.detail_refs).map(normalizeReadModelDetailRef).filter(ref => ref.kind),
    items: safeArray(value.items).map(normalizeReadModelItem).filter(Boolean),
    sourceArtifacts: normalizeReadModelArtifactOrigins(value.source_artifacts),
  };
};

function normalizeOrganizationReadModelContract(officialRun) {
  const safeRun = isPlainObject(officialRun) ? officialRun : {};
  let contractPayload = {};
  if (isPlainObject(safeRun.organization_read_model)) {
    contractPayload = safeRun.organization_read_model;
  } else if (isPlainObject(safeRun.snapshot && safeRun.snapshot.organization_read_model)) {
    contractPayload = safeRun.snapshot.organization_read_model;
  }

  const organizations = safeArray(contractPayload.organizations)
    .filter(isPlainObject)
    .map(row => {
      const organization = isPlainObject(row.organization) ? row.organization : {};
      const workspace = isPlainObject(row.workspace) ? row.workspace : {};
      const blocks = isPlainObject(workspace.blocks) ? workspace.blocks : {};
      const projections = isPlainObject(workspace.projections) ? workspace.projections : {};

      return {
        organization: {
          orgId: normalizeText(organization.org_id),
          orgName: firstNonEmptyText(organization.org_name, organization.org_id),
          domain: normalizeText(organization.domain),
        },
        health: normalizeWorkspaceHealth(row.health),
        observedAt: normalizeText(row.observed_at),
        dataFreshness: isPlainObject(row.data_freshness) ? row.data_freshness : {},
        readModelFingerprint: normalizeText(row.read_model_fingerprint),
        artifactOrigins: normalizeReadModelArtifactOrigins(row.artifact_origins),
        issues: safeArray(row.issues),
        workspace: {
          blocks: {
            ca: normalizeWorkspaceBlock(blocks.ca, 'ca', 'CA'),
            api: normalizeWorkspaceBlock(blocks.api, 'api', 'API'),
            peers: normalizeWorkspaceBlock(blocks.peers, 'peers', 'Peers'),
            orderers: normalizeWorkspaceBlock(blocks.orderers, 'orderers', 'Orderers'),
            businessGroup: normalizeWorkspaceBlock(
              blocks.business_group,
              'business_group',
              'Business Group'
            ),
            channels: normalizeWorkspaceBlock(blocks.channels, 'channels', 'Channels'),
          },
          projections: {
            channels: safeArray(projections.channels).map(normalizeReadModelItem).filter(Boolean),
            organizationMembers: safeArray(projections.organization_members)
              .map(normalizeReadModelItem)
              .filter(Boolean),
            peers: safeArray(projections.peers).map(normalizeReadModelItem).filter(Boolean),
            orderers: safeArray(projections.orderers).map(normalizeReadModelItem).filter(Boolean),
            chaincodes: safeArray(projections.chaincodes)
              .map(normalizeReadModelItem)
              .filter(Boolean),
          },
        },
      };
    })
    .filter(row => row.organization.orgId || row.organization.orgName);

  return {
    contractVersion: normalizeText(contractPayload.contract_version),
    sourceOfTruth: normalizeText(contractPayload.source_of_truth),
    generatedAtUtc: normalizeText(contractPayload.generated_at_utc),
    correlation: isPlainObject(contractPayload.correlation) ? contractPayload.correlation : {},
    artifactOrigins: normalizeReadModelArtifactOrigins(contractPayload.artifact_origins),
    readModelFingerprint: normalizeText(contractPayload.read_model_fingerprint),
    summary: isPlainObject(contractPayload.summary) ? contractPayload.summary : {},
    organizations,
  };
}

export const buildOrganizationWorkspaceReadModels = officialRun =>
  normalizeOrganizationReadModelContract(officialRun).organizations;

function normalizeRuntimeTelemetryContract(officialRun) {
  const safeRun = isPlainObject(officialRun) ? officialRun : {};
  let contractPayload = {};
  if (isPlainObject(safeRun.runtime_telemetry)) {
    contractPayload = safeRun.runtime_telemetry;
  } else if (isPlainObject(safeRun.snapshot && safeRun.snapshot.runtime_telemetry)) {
    contractPayload = safeRun.snapshot.runtime_telemetry;
  }

  const organizations = safeArray(contractPayload.organizations)
    .filter(isPlainObject)
    .map(organizationRow => {
      const organization = isPlainObject(organizationRow.organization)
        ? organizationRow.organization
        : {};
      return {
        organization: {
          orgId: normalizeText(organization.org_id),
          orgName: firstNonEmptyText(organization.org_name, organization.org_id),
          domain: normalizeText(organization.domain),
        },
        components: safeArray(organizationRow.components)
          .filter(isPlainObject)
          .map(component => ({
            componentId: normalizeText(component.component_id),
            componentType: normalizeComponentType(component.component_type),
            name: firstNonEmptyText(component.name, component.component_id),
            containerName: normalizeText(component.container_name),
            image: normalizeText(component.image),
            platform: normalizeText(component.platform),
            status: normalizeRuntimeSemanticStatus(component.status),
            startedAt: normalizeText(component.started_at),
            health: isPlainObject(component.health) ? component.health : {},
            ports: safeArray(component.ports),
            mounts: safeArray(component.mounts),
            env: safeArray(component.env),
            hostId: normalizeText(component.host_id),
            orgId: normalizeText(component.org_id),
            channelRefs: safeArray(component.channel_refs)
              .map(value => normalizeText(value))
              .filter(Boolean),
            chaincodeRefs: safeArray(component.chaincode_refs)
              .map(value => normalizeText(value))
              .filter(Boolean),
            requiredState: normalizeText(component.required_state),
            observedState: isPlainObject(component.observed_state) ? component.observed_state : {},
            scope: normalizeScope(component.scope),
            criticality: normalizeCriticality(component.criticality),
            observedAt: firstNonEmptyText(component.observed_at, component.started_at),
            issues: safeArray(component.issues),
          }))
          .filter(component => component.componentType && component.name),
        channels: safeArray(organizationRow.channels),
        chaincodes: safeArray(organizationRow.chaincodes),
        health: normalizeLower(organizationRow.health),
        criticality: normalizeCriticality(organizationRow.criticality),
        observedAt: normalizeText(organizationRow.observed_at),
        dataFreshness: isPlainObject(organizationRow.data_freshness)
          ? organizationRow.data_freshness
          : {},
        issues: safeArray(organizationRow.issues),
        artifacts: isPlainObject(organizationRow.artifacts) ? organizationRow.artifacts : {},
        readModelFingerprint: normalizeText(organizationRow.read_model_fingerprint),
      };
    })
    .filter(row => row.organization.orgId || row.organization.orgName);

  return {
    contractVersion: normalizeText(contractPayload.contract_version),
    sourceOfTruth: normalizeText(contractPayload.source_of_truth),
    generatedAtUtc: normalizeText(contractPayload.generated_at_utc),
    correlation: isPlainObject(contractPayload.correlation) ? contractPayload.correlation : {},
    summary: isPlainObject(contractPayload.summary) ? contractPayload.summary : {},
    organizations,
  };
}

function buildRowsFromRuntimeTelemetry(runtimeTelemetry) {
  const safeTelemetry = isPlainObject(runtimeTelemetry) ? runtimeTelemetry : {};
  const rows = [];

  safeArray(safeTelemetry.organizations).forEach(organizationRow => {
    const organization = isPlainObject(organizationRow.organization)
      ? organizationRow.organization
      : {};
    safeArray(organizationRow.components).forEach(component => {
      const componentType = normalizeComponentType(component.componentType);
      if (!componentType) {
        return;
      }

      rows.push({
        key: normalizeText(component.componentId) || `${organization.orgId}|${componentType}|${component.name}`,
        componentId: normalizeText(component.componentId),
        organizationId: normalizeText(component.orgId) || normalizeText(organization.orgId),
        organizationName: firstNonEmptyText(component.orgName, organization.orgName, organization.orgId),
        componentType,
        componentTypeLabel: ORG_RUNTIME_COMPONENT_TYPE_LABEL[componentType] || componentType,
        componentName: normalizeText(component.name),
        nodeId: normalizeText(component.componentId),
        hostRef: normalizeText(component.hostId),
        // eslint-disable-next-line no-use-before-define
        scope: normalizeScope(component.scope) || resolveScopeByComponentType(componentType),
        // eslint-disable-next-line no-use-before-define
        criticality: normalizeCriticality(component.criticality) || resolveCriticalityByComponentType(componentType),
        channelId: safeArray(component.channelRefs)[0] || '',
        chaincodeId: safeArray(component.chaincodeRefs)[0] || '',
        apiId: '',
        routePath: '',
        details: 'runtime_telemetry',
        status: normalizeRuntimeSemanticStatus(component.status),
        statusSource: 'runtime_telemetry',
        evidenceTimestampUtc: firstNonEmptyText(component.observedAt, organizationRow.observedAt),
        evidenceStage: normalizeText(component.observedState && component.observedState.stage),
        containerName: normalizeText(component.containerName),
        image: normalizeText(component.image),
        platform: normalizeText(component.platform),
        ports: safeArray(component.ports),
        mounts: safeArray(component.mounts),
        env: safeArray(component.env),
        requiredState: normalizeText(component.requiredState),
        observedState: isPlainObject(component.observedState) ? component.observedState : {},
        health: isPlainObject(component.health) ? component.health : {},
        issues: safeArray(component.issues),
      });
    });
  });

  // eslint-disable-next-line no-use-before-define
  return rows.sort(sortTopologyRows);
}

function resolveScopeByComponentType(componentType) {
  const normalizedType = normalizeComponentType(componentType);
  return DEFAULT_SCOPE_BY_COMPONENT_TYPE[normalizedType] || ORG_RUNTIME_SCOPE.required;
}

function resolveCriticalityByComponentType(componentType) {
  const normalizedType = normalizeComponentType(componentType);
  return (
    DEFAULT_CRITICALITY_BY_COMPONENT_TYPE[normalizedType] || ORG_RUNTIME_CRITICALITY.supporting
  );
}

const findMatchingEvidenceRows = (componentRow, hostEvidenceRows) =>
  hostEvidenceRows.filter(evidenceRow => {
    const componentNodeId = normalizeText(componentRow.nodeId);
    const componentHostRef = normalizeText(componentRow.hostRef);
    const componentOrgId = normalizeText(componentRow.organizationId);
    const evidenceNodeId = normalizeText(evidenceRow.nodeId);
    const evidenceHostRef = normalizeText(evidenceRow.hostRef);
    const evidenceOrgId = normalizeText(evidenceRow.organizationId);

    if (componentNodeId && evidenceNodeId && componentNodeId === evidenceNodeId) {
      return true;
    }

    if (componentHostRef && evidenceHostRef && componentHostRef === evidenceHostRef) {
      if (!componentOrgId || !evidenceOrgId) {
        return true;
      }
      return componentOrgId === evidenceOrgId;
    }

    if (componentOrgId && evidenceOrgId && componentOrgId === evidenceOrgId) {
      return !componentHostRef;
    }

    return false;
  });

const resolveRuntimeStatusByEvidence = (componentRow, evidenceContext) => {
  const scopedAsPlanned = componentRow.scope === ORG_RUNTIME_SCOPE.planned;
  if (scopedAsPlanned) {
    return {
      status: ORG_RUNTIME_STATUS.planned,
      statusSource: 'topology_catalog',
      evidenceTimestampUtc: '',
      evidenceStage: '',
    };
  }

  const matchingEvidenceRows = findMatchingEvidenceRows(
    componentRow,
    evidenceContext.hostEvidenceRows
  ).sort((left, right) =>
    toSortableTimestamp(left.timestampUtc).localeCompare(toSortableTimestamp(right.timestampUtc))
  );

  if (matchingEvidenceRows.length > 0) {
    const latestEvidence = matchingEvidenceRows[matchingEvidenceRows.length - 1];
    const hasFailedEvidence = matchingEvidenceRows.some(row => toSafeInteger(row.exitCode) > 0);

    if (hasFailedEvidence) {
      return {
        status: ORG_RUNTIME_STATUS.degraded,
        statusSource: 'host_inventory',
        evidenceTimestampUtc: latestEvidence.timestampUtc,
        evidenceStage: latestEvidence.stage,
      };
    }

    if (
      evidenceContext.stageStatusByKey.verify === 'completed' ||
      (evidenceContext.pipelineStatus === 'completed' && evidenceContext.decision === 'allow')
    ) {
      return {
        status: ORG_RUNTIME_STATUS.running,
        statusSource: 'host_inventory',
        evidenceTimestampUtc: latestEvidence.timestampUtc,
        evidenceStage: latestEvidence.stage,
      };
    }

    return {
      status: ORG_RUNTIME_STATUS.running,
      statusSource: 'host_inventory',
      evidenceTimestampUtc: latestEvidence.timestampUtc,
      evidenceStage: latestEvidence.stage,
    };
  }

  if (evidenceContext.pipelineStatus === 'failed') {
    return {
      status: ORG_RUNTIME_STATUS.degraded,
      statusSource: 'run_status',
      evidenceTimestampUtc: evidenceContext.updatedAtUtc,
      evidenceStage: '',
    };
  }

  if (evidenceContext.pipelineStatus === 'completed') {
    if (evidenceContext.decision === 'allow') {
      if (evidenceContext.verifyArtifactAvailable || evidenceContext.inventoryArtifactAvailable) {
        return {
          status: ORG_RUNTIME_STATUS.missing,
          statusSource: 'official_decision',
          evidenceTimestampUtc: evidenceContext.updatedAtUtc,
          evidenceStage: 'verify',
        };
      }
      return {
        status: ORG_RUNTIME_STATUS.unknown,
        statusSource: 'artifact_rows',
        evidenceTimestampUtc: evidenceContext.updatedAtUtc,
        evidenceStage: 'verify',
      };
    }

    if (evidenceContext.decision === 'block') {
      return {
        status: ORG_RUNTIME_STATUS.degraded,
        statusSource: 'official_decision',
        evidenceTimestampUtc: evidenceContext.updatedAtUtc,
        evidenceStage: 'verify',
      };
    }
  }

  return {
    status: ORG_RUNTIME_STATUS.unknown,
    statusSource: 'run_status',
    evidenceTimestampUtc: evidenceContext.updatedAtUtc,
    evidenceStage: '',
  };
};

function sortTopologyRows(left, right) {
  const leftOrg = normalizeText(left.organizationName);
  const rightOrg = normalizeText(right.organizationName);
  if (leftOrg !== rightOrg) {
    return leftOrg.localeCompare(rightOrg);
  }

  const leftTypeOrder = COMPONENT_TYPE_SORT_ORDER[left.componentType] || 999;
  const rightTypeOrder = COMPONENT_TYPE_SORT_ORDER[right.componentType] || 999;
  if (leftTypeOrder !== rightTypeOrder) {
    return leftTypeOrder - rightTypeOrder;
  }

  const leftScope = normalizeText(left.scope);
  const rightScope = normalizeText(right.scope);
  if (leftScope !== rightScope) {
    return leftScope.localeCompare(rightScope);
  }

  const leftName = normalizeText(left.componentName);
  const rightName = normalizeText(right.componentName);
  if (leftName !== rightName) {
    return leftName.localeCompare(rightName);
  }

  return normalizeText(left.hostRef).localeCompare(normalizeText(right.hostRef));
}

const normalizeOrganizationApiRegistryRows = run =>
  safeArray(run && run.api_registry)
    .map((row, index) => ({
      key: `api-registry-${String(index + 1).padStart(3, '0')}`,
      orgName: firstNonEmptyText(row && row.org_name, row && row.org_id),
      orgLookup: normalizeOrgLookup(firstNonEmptyText(row && row.org_name, row && row.org_id)),
      channelId: normalizeText(row && row.channel_id),
      chaincodeId: normalizeText(row && row.chaincode_id),
      apiId: normalizeText(row && row.api_id),
      routePath: normalizeText(row && row.route_path),
      desiredState: normalizeScope(row && (row.desired_state || row.desiredState)),
      criticality: normalizeCriticality(row && (row.criticality || row.criticidade)),
    }))
    .filter(row => row.orgLookup && (row.apiId || row.routePath || row.chaincodeId));

export const buildOrganizationRuntimeTopologyRows = officialRun => {
  const safeRun = isPlainObject(officialRun) ? officialRun : {};
  const runtimeTelemetry = normalizeRuntimeTelemetryContract(safeRun);
  if (runtimeTelemetry.organizations.length > 0) {
    return buildRowsFromRuntimeTelemetry(runtimeTelemetry);
  }

  const topologyCatalog = isPlainObject(safeRun.topology_catalog) ? safeRun.topology_catalog : {};
  const organizations = safeArray(topologyCatalog.organizations);
  const evidenceContext = buildEvidenceContext(safeRun);
  const apiRegistryRows = normalizeOrganizationApiRegistryRows(safeRun);
  const rows = [];
  const dedupKeys = new Set();

  const appendComponent = ({
    organizationId = '',
    organizationName = '',
    componentType = '',
    componentName = '',
    nodeId = '',
    hostRef = '',
    scope = '',
    criticality = '',
    metadata = {},
  }) => {
    const normalizedComponentType = normalizeComponentType(componentType);
    if (!normalizedComponentType) {
      return;
    }

    const normalizedOrganizationId = normalizeText(organizationId);
    const normalizedOrganizationName = normalizeText(organizationName) || normalizedOrganizationId;
    const normalizedHostRef = normalizeText(hostRef);
    const normalizedComponentName =
      normalizeText(componentName) || `${normalizedComponentType}-${rows.length + 1}`;
    const normalizedScope =
      normalizeScope(scope) || resolveScopeByComponentType(normalizedComponentType);
    const normalizedCriticality =
      normalizeCriticality(criticality) ||
      resolveCriticalityByComponentType(normalizedComponentType);

    const key = [
      normalizeOrgLookup(normalizedOrganizationId || normalizedOrganizationName),
      normalizedComponentType,
      normalizeLower(normalizedComponentName),
      normalizeLower(normalizedHostRef),
    ].join('|');

    if (dedupKeys.has(key)) {
      return;
    }
    dedupKeys.add(key);

    const row = {
      key,
      componentId: normalizeText(nodeId),
      organizationId: normalizedOrganizationId,
      organizationName: normalizedOrganizationName,
      componentType: normalizedComponentType,
      componentTypeLabel:
        ORG_RUNTIME_COMPONENT_TYPE_LABEL[normalizedComponentType] || normalizedComponentType,
      componentName: normalizedComponentName,
      nodeId: normalizeText(nodeId),
      hostRef: normalizedHostRef,
      scope: normalizedScope,
      criticality: normalizedCriticality,
      channelId: normalizeText(metadata.channelId),
      chaincodeId: normalizeText(metadata.chaincodeId),
      apiId: normalizeText(metadata.apiId),
      routePath: normalizeText(metadata.routePath),
      details: normalizeText(metadata.details),
      status: ORG_RUNTIME_STATUS.unknown,
      statusSource: 'run_status',
      evidenceTimestampUtc: '',
      evidenceStage: '',
    };

    const statusByEvidence = resolveRuntimeStatusByEvidence(row, evidenceContext);
    row.status = statusByEvidence.status;
    row.statusSource = statusByEvidence.statusSource;
    row.evidenceTimestampUtc = statusByEvidence.evidenceTimestampUtc;
    row.evidenceStage = statusByEvidence.evidenceStage;

    rows.push(row);
  };

  organizations.forEach((organization, organizationIndex) => {
    const orgId = firstNonEmptyText(
      organization && (organization.org_id || organization.orgId || organization.org_key),
      `org-${organizationIndex + 1}`
    );
    const orgName = firstNonEmptyText(
      organization && (organization.org_name || organization.orgName),
      orgId
    );
    const orgLookup = normalizeOrgLookup(orgName || orgId);
    const serviceHostMapping = isPlainObject(organization && organization.service_host_mapping)
      ? organization.service_host_mapping
      : {};
    const serviceParameters = isPlainObject(organization && organization.service_parameters)
      ? organization.service_parameters
      : {};
    const organizationApiRows = apiRegistryRows.filter(
      row => row.orgLookup === orgLookup || row.orgLookup === normalizeOrgLookup(orgId)
    );

    safeArray(organization && organization.peers).forEach((peer, index) => {
      appendComponent({
        organizationId: orgId,
        organizationName: orgName,
        componentType: firstNonEmptyText(peer && peer.node_type, 'peer'),
        componentName: firstNonEmptyText(
          peer && (peer.node_id || peer.nodeId),
          `peer-${index + 1}`
        ),
        nodeId: firstNonEmptyText(peer && (peer.node_id || peer.nodeId)),
        hostRef: firstNonEmptyText(
          peer && (peer.host_ref || peer.hostRef),
          organization && organization.peer_host_ref,
          serviceHostMapping.peer
        ),
        scope: firstNonEmptyText(peer && (peer.desired_state || peer.desiredState)),
        criticality: firstNonEmptyText(peer && (peer.criticality || peer.criticidade)),
      });
    });

    safeArray(organization && organization.orderers).forEach((orderer, index) => {
      appendComponent({
        organizationId: orgId,
        organizationName: orgName,
        componentType: firstNonEmptyText(orderer && orderer.node_type, 'orderer'),
        componentName: firstNonEmptyText(
          orderer && (orderer.node_id || orderer.nodeId),
          `orderer-${index + 1}`
        ),
        nodeId: firstNonEmptyText(orderer && (orderer.node_id || orderer.nodeId)),
        hostRef: firstNonEmptyText(
          orderer && (orderer.host_ref || orderer.hostRef),
          organization && organization.orderer_host_ref,
          serviceHostMapping.orderer
        ),
        scope: firstNonEmptyText(orderer && (orderer.desired_state || orderer.desiredState)),
        criticality: firstNonEmptyText(orderer && (orderer.criticality || orderer.criticidade)),
      });
    });

    const caRows = safeArray(organization && organization.cas);
    if (caRows.length > 0) {
      caRows.forEach((ca, index) => {
        appendComponent({
          organizationId: orgId,
          organizationName: orgName,
          componentType: firstNonEmptyText(ca && ca.node_type, 'ca'),
          componentName: firstNonEmptyText(ca && (ca.node_id || ca.nodeId), `ca-${index + 1}`),
          nodeId: firstNonEmptyText(ca && (ca.node_id || ca.nodeId)),
          hostRef: firstNonEmptyText(
            ca && (ca.host_ref || ca.hostRef),
            serviceHostMapping.ca,
            organization && organization.peer_host_ref
          ),
          scope: firstNonEmptyText(ca && (ca.desired_state || ca.desiredState)),
          criticality: firstNonEmptyText(ca && (ca.criticality || ca.criticidade)),
        });
      });
    } else {
      appendComponent({
        organizationId: orgId,
        organizationName: orgName,
        componentType: 'ca',
        componentName: firstNonEmptyText(
          organization && organization.ca && (organization.ca.name || organization.ca.node_id),
          `ca-${orgId}`
        ),
        hostRef: firstNonEmptyText(
          organization && organization.ca && (organization.ca.host_ref || organization.ca.hostRef),
          serviceHostMapping.ca,
          organization && organization.peer_host_ref
        ),
        scope: firstNonEmptyText(
          organization &&
            organization.ca &&
            (organization.ca.desired_state || organization.ca.desiredState),
          serviceParameters && serviceParameters.ca && serviceParameters.ca.desired_state
        ),
        criticality: firstNonEmptyText(
          organization &&
            organization.ca &&
            (organization.ca.criticality || organization.ca.criticidade)
        ),
      });
    }

    appendComponent({
      organizationId: orgId,
      organizationName: orgName,
      componentType: 'couch',
      componentName: firstNonEmptyText(
        serviceParameters && serviceParameters.couch && serviceParameters.couch.database,
        `couch-${orgId}`
      ),
      hostRef: firstNonEmptyText(
        serviceHostMapping.couch,
        serviceParameters && serviceParameters.couch && serviceParameters.couch.host_ref,
        organization && organization.peer_host_ref
      ),
      scope: firstNonEmptyText(
        serviceParameters && serviceParameters.couch && serviceParameters.couch.desired_state
      ),
      criticality: firstNonEmptyText(
        serviceParameters && serviceParameters.couch && serviceParameters.couch.criticality
      ),
    });

    appendComponent({
      organizationId: orgId,
      organizationName: orgName,
      componentType: 'apiGateway',
      componentName: firstNonEmptyText(
        serviceParameters &&
          serviceParameters.apiGateway &&
          serviceParameters.apiGateway.route_prefix,
        `api-gateway-${orgId}`
      ),
      hostRef: firstNonEmptyText(
        serviceHostMapping.apiGateway,
        serviceParameters && serviceParameters.apiGateway && serviceParameters.apiGateway.host_ref,
        organization && organization.peer_host_ref
      ),
      scope: firstNonEmptyText(
        serviceParameters &&
          serviceParameters.apiGateway &&
          serviceParameters.apiGateway.desired_state
      ),
      criticality: firstNonEmptyText(
        serviceParameters &&
          serviceParameters.apiGateway &&
          serviceParameters.apiGateway.criticality
      ),
    });

    appendComponent({
      organizationId: orgId,
      organizationName: orgName,
      componentType: 'netapi',
      componentName: firstNonEmptyText(
        serviceParameters && serviceParameters.netapi && serviceParameters.netapi.route_prefix,
        `netapi-${orgId}`
      ),
      hostRef: firstNonEmptyText(
        serviceHostMapping.netapi,
        serviceParameters && serviceParameters.netapi && serviceParameters.netapi.host_ref,
        organization && organization.peer_host_ref
      ),
      scope: firstNonEmptyText(
        serviceParameters && serviceParameters.netapi && serviceParameters.netapi.desired_state
      ),
      criticality: firstNonEmptyText(
        serviceParameters && serviceParameters.netapi && serviceParameters.netapi.criticality
      ),
    });

    organizationApiRows.forEach(apiRow => {
      appendComponent({
        organizationId: orgId,
        organizationName: orgName,
        componentType: 'apiGateway',
        componentName: firstNonEmptyText(
          apiRow.apiId,
          apiRow.routePath,
          `${apiRow.chaincodeId || 'api'}-route`
        ),
        hostRef: firstNonEmptyText(serviceHostMapping.apiGateway, serviceHostMapping.netapi),
        scope: apiRow.desiredState || ORG_RUNTIME_SCOPE.optional,
        criticality: apiRow.criticality || ORG_RUNTIME_CRITICALITY.supporting,
        metadata: {
          channelId: apiRow.channelId,
          chaincodeId: apiRow.chaincodeId,
          apiId: apiRow.apiId,
          routePath: apiRow.routePath,
          details: 'api_registry',
        },
      });
    });

    const chaincodeByKey = new Map();
    safeArray(organization && organization.chaincodes).forEach(chaincodeId => {
      const normalizedChaincodeId = normalizeText(chaincodeId);
      if (!normalizedChaincodeId) {
        return;
      }
      chaincodeByKey.set(`${normalizedChaincodeId}|`, {
        chaincodeId: normalizedChaincodeId,
        channelId: '',
      });
    });
    organizationApiRows.forEach(apiRow => {
      if (!apiRow.chaincodeId) {
        return;
      }
      chaincodeByKey.set(`${apiRow.chaincodeId}|${apiRow.channelId}`, {
        chaincodeId: apiRow.chaincodeId,
        channelId: apiRow.channelId,
      });
    });

    Array.from(chaincodeByKey.values()).forEach(chaincode => {
      appendComponent({
        organizationId: orgId,
        organizationName: orgName,
        componentType: ORG_RUNTIME_COMPONENT_TYPES.chaincodeRuntime,
        componentName: chaincode.chaincodeId,
        hostRef: firstNonEmptyText(serviceHostMapping.peer, serviceHostMapping.apiGateway),
        scope: ORG_RUNTIME_SCOPE.optional,
        criticality: ORG_RUNTIME_CRITICALITY.supporting,
        metadata: {
          channelId: chaincode.channelId,
          chaincodeId: chaincode.chaincodeId,
          details: 'chaincode_runtime',
        },
      });
    });
  });

  return rows.sort(sortTopologyRows);
};

export const buildOrganizationRuntimeTopologyFilterOptions = rows => {
  const safeRows = safeArray(rows);
  const hosts = Array.from(
    new Set(safeRows.map(row => normalizeText(row && row.hostRef)).filter(Boolean))
  ).sort((left, right) => left.localeCompare(right));

  const componentTypes = Array.from(
    new Set(safeRows.map(row => normalizeText(row && row.componentType)).filter(Boolean))
  ).sort((left, right) => {
    const leftOrder = COMPONENT_TYPE_SORT_ORDER[left] || 999;
    const rightOrder = COMPONENT_TYPE_SORT_ORDER[right] || 999;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    return left.localeCompare(right);
  });

  const statuses = Array.from(
    new Set(safeRows.map(row => normalizeText(row && row.status)).filter(Boolean))
  ).sort((left, right) => {
    const leftOrder = STATUS_SORT_ORDER[left] || 999;
    const rightOrder = STATUS_SORT_ORDER[right] || 999;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    return left.localeCompare(right);
  });

  const criticalities = Array.from(
    new Set(safeRows.map(row => normalizeText(row && row.criticality)).filter(Boolean))
  ).sort((left, right) => left.localeCompare(right));

  return {
    hosts,
    componentTypes,
    statuses,
    criticalities,
  };
};

export const filterOrganizationRuntimeTopologyRows = (rows, filters = {}) => {
  const safeRows = safeArray(rows);
  const hostFilter = normalizeText(filters.host);
  const componentTypeFilter = normalizeText(filters.componentType);
  const statusFilter = normalizeText(filters.status);
  const criticalityFilter = normalizeText(filters.criticality);

  return safeRows.filter(row => {
    const hostMatches =
      hostFilter === 'all' || !hostFilter || normalizeText(row.hostRef) === hostFilter;
    if (!hostMatches) {
      return false;
    }

    const componentTypeMatches =
      componentTypeFilter === 'all' ||
      !componentTypeFilter ||
      normalizeText(row.componentType) === componentTypeFilter;
    if (!componentTypeMatches) {
      return false;
    }

    const statusMatches =
      statusFilter === 'all' || !statusFilter || normalizeText(row.status) === statusFilter;
    if (!statusMatches) {
      return false;
    }

    const criticalityMatches =
      criticalityFilter === 'all' ||
      !criticalityFilter ||
      normalizeText(row.criticality) === criticalityFilter;
    if (!criticalityMatches) {
      return false;
    }

    return true;
  });
};

const initializeCounterMap = keys =>
  keys.reduce((accumulator, key) => {
    accumulator[key] = 0;
    return accumulator;
  }, {});

export const buildOrganizationRuntimeTopologySummary = rows => {
  const safeRows = safeArray(rows);
  const statusTotals = initializeCounterMap(Object.values(ORG_RUNTIME_STATUS));
  const scopeTotals = initializeCounterMap(Object.values(ORG_RUNTIME_SCOPE));
  const criticalityTotals = initializeCounterMap(Object.values(ORG_RUNTIME_CRITICALITY));
  const typeTotals = initializeCounterMap(Object.values(ORG_RUNTIME_COMPONENT_TYPE_LABEL));

  safeRows.forEach(row => {
    const status = normalizeText(row && row.status);
    const scope = normalizeText(row && row.scope);
    const criticality = normalizeText(row && row.criticality);
    const componentType = normalizeText(row && row.componentType);

    if (Object.prototype.hasOwnProperty.call(statusTotals, status)) {
      statusTotals[status] += 1;
    }
    if (Object.prototype.hasOwnProperty.call(scopeTotals, scope)) {
      scopeTotals[scope] += 1;
    }
    if (Object.prototype.hasOwnProperty.call(criticalityTotals, criticality)) {
      criticalityTotals[criticality] += 1;
    }
    if (Object.prototype.hasOwnProperty.call(typeTotals, componentType)) {
      typeTotals[componentType] += 1;
    }
  });

  const requiredTotal = scopeTotals.required || 0;
  const requiredConverged = safeRows.filter(
    row => row.scope === ORG_RUNTIME_SCOPE.required && row.status === ORG_RUNTIME_STATUS.running
  ).length;
  const requiredConvergencePercent =
    requiredTotal > 0 ? Math.round((requiredConverged / requiredTotal) * 100) : 0;

  return {
    totalComponents: safeRows.length,
    requiredConverged,
    requiredTotal,
    requiredConvergencePercent,
    statusTotals,
    scopeTotals,
    criticalityTotals,
    typeTotals,
  };
};

export const buildOfficialRunEvidenceMeta = officialRun => buildEvidenceContext(officialRun);

export const formatRuntimeTopologyUtcTimestamp = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return '-';
  }
  return normalized.replace('T', ' ').replace('Z', ' UTC');
};
