import {
  sanitizeSensitiveText,
  sanitizeStructuredData,
} from '../../../utils/provisioningSecurityRedaction';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

export const EVIDENCE_STATUS = Object.freeze({
  ready: 'ready',
  partial: 'partial',
  missing: 'missing',
});

export const EVIDENCE_STATUS_TONE_MAP = Object.freeze({
  ready: 'success',
  partial: 'warning',
  missing: 'error',
});

const DEFAULT_EXECUTOR = 'ops.cognus@ufg.br';

const REQUIRED_EVIDENCE_KEYS = Object.freeze([
  'reconcile_report',
  'inventory_final',
  'verify_report',
  'ssh_execution_log',
  'stage_reports',
]);

const BLUEPRINT_REFERENCE = Object.freeze({
  artifact: 'blueprints/blueprint-v1.2.0.json',
  blueprintVersion: '1.2.0',
  schemaVersion: '1.0.0',
  fingerprint: '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
  timestampUtc: '2026-02-16T11:45:00Z',
});

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
    .replace(/:/g, '/')
    .replace(/\/+/g, '/')
    .replace(/\.json$/g, '');

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');
const localizeEvidenceText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const normalizeEvidenceStatus = status => {
  const normalized = normalizeLower(status);
  if (normalized === EVIDENCE_STATUS.ready) {
    return EVIDENCE_STATUS.ready;
  }
  if (normalized === EVIDENCE_STATUS.partial) {
    return EVIDENCE_STATUS.partial;
  }
  return EVIDENCE_STATUS.missing;
};

const seededHash = (input, seed) => {
  const modulus = 4294967291;
  let hash = Number(seed) % modulus;

  for (let index = 0; index < input.length; index += 1) {
    hash = (hash * 1664525 + input.charCodeAt(index) + 1013904223) % modulus;
  }

  return Math.floor(hash)
    .toString(16)
    .slice(-8)
    .padStart(8, '0');
};

const computeDeterministicFingerprint = value => {
  const payload = JSON.stringify(value || {});
  return [seededHash(payload, 0x811c9dc5), seededHash(payload, 0x27d4eb2f)].join('');
};

const findArtifactByKey = (snapshot, artifactKey) => {
  const artifacts = Array.isArray(snapshot && snapshot.artifactRows) ? snapshot.artifactRows : [];
  return artifacts.find(artifact => normalizeText(artifact.key) === artifactKey) || null;
};

const findOfficialArtifactByKey = (officialRun, artifactKey) => {
  const artifacts = Array.isArray(officialRun && officialRun.artifact_rows)
    ? officialRun.artifact_rows
    : [];
  const targetToken = normalizeArtifactToken(artifactKey);
  return (
    artifacts.find(artifact => {
      const keyToken = normalizeArtifactToken(artifact && artifact.key);
      const artifactKeyToken = normalizeArtifactToken(artifact && artifact.artifact_key);
      return keyToken === targetToken || artifactKeyToken === targetToken;
    }) || null
  );
};

const normalizeSnapshot = snapshot => {
  if (!isPlainObject(snapshot)) {
    return {
      syncPhase: 'unsynced',
      syncedAt: '',
      records: {
        organizations: [],
        channels: [],
        hosts: [],
        nodes: [],
        cryptoBaseline: [],
      },
      artifactRows: [],
      consistency: {
        isConsistent: false,
        missingArtifacts: [],
      },
    };
  }

  return {
    ...snapshot,
    syncPhase: normalizeText(snapshot.syncPhase) || 'unsynced',
    syncedAt: normalizeText(snapshot.syncedAt),
    artifactRows: Array.isArray(snapshot.artifactRows)
      ? snapshot.artifactRows.map(artifact => ({ ...artifact }))
      : [],
    records: {
      organizations: Array.isArray(snapshot.records && snapshot.records.organizations)
        ? snapshot.records.organizations.map(record => ({ ...record }))
        : [],
      channels: Array.isArray(snapshot.records && snapshot.records.channels)
        ? snapshot.records.channels.map(record => ({ ...record }))
        : [],
      hosts: Array.isArray(snapshot.records && snapshot.records.hosts)
        ? snapshot.records.hosts.map(record => ({ ...record }))
        : [],
      nodes: Array.isArray(snapshot.records && snapshot.records.nodes)
        ? snapshot.records.nodes.map(record => ({ ...record }))
        : [],
      cryptoBaseline: Array.isArray(snapshot.records && snapshot.records.cryptoBaseline)
        ? snapshot.records.cryptoBaseline.map(record => ({ ...record }))
        : [],
    },
    consistency: isPlainObject(snapshot.consistency)
      ? {
          ...snapshot.consistency,
          missingArtifacts: Array.isArray(snapshot.consistency.missingArtifacts)
            ? [...snapshot.consistency.missingArtifacts]
            : [],
        }
      : {
          isConsistent: false,
          missingArtifacts: [],
        },
  };
};

const deriveRunbookStatus = snapshot => {
  if (snapshot.syncPhase === 'full') {
    return EVIDENCE_STATUS.ready;
  }
  if (snapshot.syncPhase === 'partial') {
    return EVIDENCE_STATUS.partial;
  }
  return EVIDENCE_STATUS.missing;
};

const deriveInventoryStatus = artifact => {
  if (!artifact) {
    return EVIDENCE_STATUS.missing;
  }
  return artifact.available ? EVIDENCE_STATUS.ready : EVIDENCE_STATUS.missing;
};

const buildEvidenceRow = ({
  key,
  source,
  artifact,
  fingerprint,
  status,
  timestampUtc,
  executor,
  changeId,
  runId,
  required = true,
  details = '',
}) => ({
  key: sanitizeSensitiveText(key),
  source: sanitizeSensitiveText(source),
  artifact: sanitizeSensitiveText(artifact),
  fingerprint: normalizeText(fingerprint),
  status: normalizeEvidenceStatus(status),
  timestampUtc: normalizeText(timestampUtc),
  executor: sanitizeSensitiveText(normalizeText(executor)),
  changeId: normalizeText(changeId),
  runId: normalizeText(runId),
  required: Boolean(required),
  details: sanitizeSensitiveText(normalizeText(details)),
});

export const truncateEvidenceFingerprint = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return '-';
  }
  if (normalized.length <= 18) {
    return normalized;
  }
  return `${normalized.slice(0, 10)}...${normalized.slice(-6)}`;
};

export const formatEvidenceUtcTimestamp = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return '-';
  }
  return normalized.replace('T', ' ').replace('Z', ' UTC');
};

export const buildExecutionEvidenceRows = ({
  changeId = '',
  runId = '',
  executor = DEFAULT_EXECUTOR,
  snapshot,
  officialRun,
} = {}) => {
  const normalizedChangeId = normalizeText(changeId);
  const normalizedRunId = normalizeText(runId);
  const normalizedExecutor = normalizeText(executor) || DEFAULT_EXECUTOR;
  const normalizedSnapshot = normalizeSnapshot(snapshot);
  const syncedAt = normalizedSnapshot.syncedAt;
  const safeOfficialRun = isPlainObject(officialRun) ? officialRun : null;
  const officialStages = Array.isArray(safeOfficialRun && safeOfficialRun.stages)
    ? safeOfficialRun.stages
    : [];

  const verifyArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'stage-reports/verify-report.json')
    : findArtifactByKey(normalizedSnapshot, 'stage-reports/verify-report.json');
  const reconcileArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'stage-reports/reconcile-report.json') ||
      findOfficialArtifactByKey(safeOfficialRun, 'reconcile-report.json')
    : null;
  const sshExecutionLogArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'ssh-execution-log.json')
    : null;
  const inventoryFinalArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'inventory-final.json')
    : findArtifactByKey(normalizedSnapshot, 'inventory-final.json');
  const pipelineReportArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'pipeline-report.json')
    : null;
  const historyArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'history.jsonl')
    : null;
  const decisionTraceArtifact = safeOfficialRun
    ? findOfficialArtifactByKey(safeOfficialRun, 'decision-trace.jsonl')
    : null;

  const requiredStageArtifacts = officialStages
    .map(stage => `stage-reports/${normalizeText(stage && stage.key)}-report.json`)
    .filter(artifactKey => artifactKey !== 'stage-reports/-report.json');
  const presentStageArtifacts = requiredStageArtifacts.filter(artifactKey =>
    Boolean(findOfficialArtifactByKey(safeOfficialRun, artifactKey))
  );
  const missingStageArtifacts = requiredStageArtifacts.filter(
    artifactKey => !presentStageArtifacts.includes(artifactKey)
  );

  let runbookFingerprint = '';
  if (safeOfficialRun) {
    runbookFingerprint = computeDeterministicFingerprint({
      stageArtifacts: presentStageArtifacts,
      stageArtifactFingerprints: presentStageArtifacts.map(artifactKey => {
        const artifactRow = findOfficialArtifactByKey(safeOfficialRun, artifactKey);
        return normalizeText(artifactRow && artifactRow.fingerprint_sha256);
      }),
      runId: normalizedRunId,
    });
  } else if (normalizedSnapshot.syncPhase !== 'unsynced') {
    runbookFingerprint = computeDeterministicFingerprint({
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      artifact: 'stage-reports/verify-report.json',
      syncPhase: normalizedSnapshot.syncPhase,
    });
  }

  let inventorySnapshotFingerprint = '';
  if (safeOfficialRun) {
    inventorySnapshotFingerprint = normalizeText(
      inventoryFinalArtifact && inventoryFinalArtifact.fingerprint_sha256
    );
  } else if (normalizedSnapshot.syncPhase !== 'unsynced') {
    inventorySnapshotFingerprint = computeDeterministicFingerprint({
      records: normalizedSnapshot.records,
      artifact: 'inventory-final.json',
      syncPhase: normalizedSnapshot.syncPhase,
    });
  }

  const pipelineReportFingerprint = normalizeText(
    pipelineReportArtifact && pipelineReportArtifact.fingerprint_sha256
  );
  const historyFingerprint = normalizeText(historyArtifact && historyArtifact.fingerprint_sha256);
  const decisionFingerprint = normalizeText(
    decisionTraceArtifact && decisionTraceArtifact.fingerprint_sha256
  );

  const blueprintFingerprint =
    normalizeText(safeOfficialRun && safeOfficialRun.blueprint_fingerprint) ||
    BLUEPRINT_REFERENCE.fingerprint;
  const officialUpdatedAt = normalizeText(safeOfficialRun && safeOfficialRun.updated_at_utc);
  const runbookTimestamp =
    normalizeText(verifyArtifact && verifyArtifact.validated_at_utc) || officialUpdatedAt;

  let stageReportsStatus = deriveRunbookStatus(normalizedSnapshot);
  if (safeOfficialRun) {
    if (missingStageArtifacts.length === 0) {
      stageReportsStatus = EVIDENCE_STATUS.ready;
    } else if (presentStageArtifacts.length > 0) {
      stageReportsStatus = EVIDENCE_STATUS.partial;
    }
  }

  const stageReportsReady = safeOfficialRun
    ? missingStageArtifacts.length === 0 && presentStageArtifacts.length > 0
    : normalizedSnapshot.syncPhase === 'full';

  return [
    buildEvidenceRow({
      key: 'blueprint_fingerprint',
      source: 'blueprint',
      artifact: BLUEPRINT_REFERENCE.artifact,
      fingerprint: blueprintFingerprint,
      status: blueprintFingerprint ? EVIDENCE_STATUS.ready : EVIDENCE_STATUS.missing,
      timestampUtc: officialUpdatedAt || BLUEPRINT_REFERENCE.timestampUtc,
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      required: false,
      details: `blueprint_version=${BLUEPRINT_REFERENCE.blueprintVersion}; schema=${BLUEPRINT_REFERENCE.schemaVersion}`,
    }),
    buildEvidenceRow({
      key: 'stage_reports',
      source: 'runbook',
      artifact: 'stage-reports/*',
      fingerprint: runbookFingerprint,
      status: stageReportsReady ? EVIDENCE_STATUS.ready : stageReportsStatus,
      timestampUtc:
        runbookTimestamp || (verifyArtifact && verifyArtifact.available ? syncedAt : ''),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      details:
        missingStageArtifacts.length > 0
          ? `missing_stage_reports=${missingStageArtifacts.join(',')}`
          : `sync_phase=${normalizedSnapshot.syncPhase}`,
    }),
    buildEvidenceRow({
      key: 'reconcile_report',
      source: 'runbook',
      artifact: 'reconcile-report.json',
      fingerprint: normalizeText(reconcileArtifact && reconcileArtifact.fingerprint_sha256),
      status:
        reconcileArtifact && reconcileArtifact.available
          ? EVIDENCE_STATUS.ready
          : EVIDENCE_STATUS.missing,
      timestampUtc: normalizeText(reconcileArtifact && reconcileArtifact.validated_at_utc),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      details: localizeEvidenceText(
        'relatório oficial de reconciliação da execução A2',
        'official reconciliation report for A2 execution'
      ),
    }),
    buildEvidenceRow({
      key: 'pipeline_report',
      source: 'runbook',
      artifact: 'pipeline-report.json',
      fingerprint: pipelineReportFingerprint,
      status:
        pipelineReportArtifact && pipelineReportArtifact.available
          ? EVIDENCE_STATUS.ready
          : EVIDENCE_STATUS.missing,
      timestampUtc: normalizeText(
        pipelineReportArtifact && pipelineReportArtifact.validated_at_utc
      ),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      required: false,
      details: 'artifact_group=pipeline-report',
    }),
    buildEvidenceRow({
      key: 'inventory_final',
      source: localizeEvidenceText('inventario', 'inventory'),
      artifact: 'inventory-final.json',
      fingerprint: inventorySnapshotFingerprint,
      status: deriveInventoryStatus(inventoryFinalArtifact),
      timestampUtc:
        normalizeText(inventoryFinalArtifact && inventoryFinalArtifact.validated_at_utc) ||
        (inventoryFinalArtifact && inventoryFinalArtifact.available ? syncedAt : ''),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      details: localizeEvidenceText(
        'snapshot consolidado de inventário pós-provisão',
        'post-provisioning consolidated inventory snapshot'
      ),
    }),
    buildEvidenceRow({
      key: 'verify_report',
      source: 'runbook',
      artifact: 'verify-report.json',
      fingerprint: normalizeText(verifyArtifact && verifyArtifact.fingerprint_sha256),
      status:
        verifyArtifact && verifyArtifact.available
          ? EVIDENCE_STATUS.ready
          : EVIDENCE_STATUS.missing,
      timestampUtc: normalizeText(verifyArtifact && verifyArtifact.validated_at_utc),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      details: localizeEvidenceText(
        'relatório oficial de verificação técnica da execução A2',
        'official technical verification report for A2 execution'
      ),
    }),
    buildEvidenceRow({
      key: 'ssh_execution_log',
      source: 'runbook',
      artifact: 'ssh-execution-log.json',
      fingerprint: normalizeText(
        sshExecutionLogArtifact && sshExecutionLogArtifact.fingerprint_sha256
      ),
      status:
        sshExecutionLogArtifact && sshExecutionLogArtifact.available
          ? EVIDENCE_STATUS.ready
          : EVIDENCE_STATUS.missing,
      timestampUtc: normalizeText(
        sshExecutionLogArtifact && sshExecutionLogArtifact.validated_at_utc
      ),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      details: localizeEvidenceText(
        'log oficial correlacionado das execuções SSH do pipeline A2',
        'official correlated log of A2 pipeline SSH executions'
      ),
    }),
    buildEvidenceRow({
      key: 'history_jsonl',
      source: localizeEvidenceText('observabilidade', 'observability'),
      artifact: 'history.jsonl',
      fingerprint: historyFingerprint,
      status:
        historyArtifact && historyArtifact.available
          ? EVIDENCE_STATUS.ready
          : EVIDENCE_STATUS.missing,
      timestampUtc: normalizeText(historyArtifact && historyArtifact.validated_at_utc),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      required: false,
      details: localizeEvidenceText(
        'linha do tempo oficial da execução',
        'official execution timeline'
      ),
    }),
    buildEvidenceRow({
      key: 'decision_trace_jsonl',
      source: localizeEvidenceText('observabilidade', 'observability'),
      artifact: 'decision-trace.jsonl',
      fingerprint: decisionFingerprint,
      status:
        decisionTraceArtifact && decisionTraceArtifact.available
          ? EVIDENCE_STATUS.ready
          : EVIDENCE_STATUS.missing,
      timestampUtc: normalizeText(decisionTraceArtifact && decisionTraceArtifact.validated_at_utc),
      executor: normalizedExecutor,
      changeId: normalizedChangeId,
      runId: normalizedRunId,
      required: false,
      details: localizeEvidenceText(
        'decisão técnica reproduzível de bloqueio/liberação',
        'reproducible technical decision for block/release'
      ),
    }),
  ];
};

const isEvidenceRowMissing = row =>
  normalizeEvidenceStatus(row.status) !== EVIDENCE_STATUS.ready ||
  !normalizeText(row.fingerprint) ||
  !normalizeText(row.timestampUtc) ||
  !normalizeText(row.executor) ||
  !normalizeText(row.changeId) ||
  !normalizeText(row.runId);

export const evaluateExecutionEvidenceGate = ({
  evidenceRows = [],
  changeId = '',
  runId = '',
} = {}) => {
  const normalizedChangeId = normalizeText(changeId);
  const normalizedRunId = normalizeText(runId);
  const rows = Array.isArray(evidenceRows) ? evidenceRows : [];

  const correlationIssues = [];
  if (!normalizedChangeId) {
    correlationIssues.push(
      localizeEvidenceText(
        'change_id não informado para o contexto de evidências.',
        'change_id not informed for the evidence context.'
      )
    );
  }
  if (!normalizedRunId) {
    correlationIssues.push(
      localizeEvidenceText(
        'run_id não informado para o contexto de evidências.',
        'run_id not informed for the evidence context.'
      )
    );
  }

  const missingRequired = REQUIRED_EVIDENCE_KEYS.map(requiredKey => {
    const row = rows.find(currentRow => currentRow.key === requiredKey);
    if (!row) {
      return {
        key: requiredKey,
        artifact: requiredKey,
      };
    }
    if (isEvidenceRowMissing(row)) {
      return {
        key: row.key,
        artifact: row.artifact,
      };
    }
    return null;
  }).filter(Boolean);
  const missingRequiredKeys = missingRequired.map(entry => entry.key);
  const missingRequiredDescriptions = missingRequired
    .map(entry => `${entry.key} (${entry.artifact})`)
    .map(value => sanitizeSensitiveText(value));

  const gateIssues = [
    ...correlationIssues,
    ...missingRequiredDescriptions.map(
      description =>
        localizeEvidenceText(
          `Evidência obrigatória ausente/incompleta: ${description}.`,
          `Required evidence missing/incomplete: ${description}.`
        )
    ),
  ].map(issue => sanitizeSensitiveText(issue));

  return {
    allowCloseExecution: gateIssues.length === 0,
    allowExport: gateIssues.length === 0,
    correlationIssues,
    missingRequiredKeys,
    missingRequiredDescriptions,
    gateIssues,
  };
};

const deepFreeze = value => {
  if (!isPlainObject(value) && !Array.isArray(value)) {
    return value;
  }

  const reference = value;
  Object.getOwnPropertyNames(reference).forEach(property => {
    const propertyValue = reference[property];
    if (
      propertyValue &&
      (isPlainObject(propertyValue) || Array.isArray(propertyValue)) &&
      !Object.isFrozen(propertyValue)
    ) {
      deepFreeze(propertyValue);
    }
  });

  return Object.freeze(reference);
};

export const createImmutableEvidenceBundle = ({
  evidenceRows = [],
  changeId = '',
  runId = '',
  executor = DEFAULT_EXECUTOR,
  lockedAtUtc = toIsoUtc(),
} = {}) => {
  const normalizedRows = (Array.isArray(evidenceRows) ? evidenceRows : []).map(row => ({
    ...row,
    key: sanitizeSensitiveText(row.key),
    source: sanitizeSensitiveText(row.source),
    artifact: sanitizeSensitiveText(row.artifact),
    status: normalizeEvidenceStatus(row.status),
    fingerprint: normalizeText(row.fingerprint),
    timestampUtc: normalizeText(row.timestampUtc),
    executor: sanitizeSensitiveText(normalizeText(row.executor)),
    changeId: normalizeText(row.changeId),
    runId: normalizeText(row.runId),
    details: sanitizeSensitiveText(normalizeText(row.details)),
  }));
  const gate = evaluateExecutionEvidenceGate({
    evidenceRows: normalizedRows,
    changeId,
    runId,
  });

  const bundle = {
    bundleId: `evd-${computeDeterministicFingerprint({
      changeId: normalizeText(changeId),
      runId: normalizeText(runId),
      lockedAtUtc: normalizeText(lockedAtUtc),
      rows: normalizedRows,
    }).slice(0, 12)}`,
    changeId: normalizeText(changeId),
    runId: normalizeText(runId),
    executor: normalizeText(executor) || DEFAULT_EXECUTOR,
    lockedAtUtc: normalizeText(lockedAtUtc),
    immutable: true,
    gate,
    rows: normalizedRows,
  };

  return deepFreeze(bundle);
};

export const buildEvidenceExportPayload = ({
  evidenceBundle,
  evidenceRows = [],
  gate,
  exportedAtUtc = toIsoUtc(),
} = {}) => {
  const isBundle = isPlainObject(evidenceBundle);
  let rows = [];
  if (isBundle) {
    rows = evidenceBundle.rows.map(row => ({ ...row }));
  } else {
    rows = (Array.isArray(evidenceRows) ? evidenceRows : []).map(row => ({ ...row }));
  }

  const firstRow = rows[0] || null;

  let effectiveGate = gate;
  if (!effectiveGate) {
    if (isBundle) {
      effectiveGate = evidenceBundle.gate;
    } else {
      effectiveGate = evaluateExecutionEvidenceGate({
        evidenceRows: rows,
        changeId: firstRow ? firstRow.changeId : '',
        runId: firstRow ? firstRow.runId : '',
      });
    }
  }

  let changeId = '';
  let runId = '';
  let executor = DEFAULT_EXECUTOR;
  if (isBundle) {
    changeId = evidenceBundle.changeId;
    runId = evidenceBundle.runId;
    executor = evidenceBundle.executor;
  } else if (firstRow) {
    changeId = normalizeText(firstRow.changeId);
    runId = normalizeText(firstRow.runId);
    executor = normalizeText(firstRow.executor) || DEFAULT_EXECUTOR;
  }

  const payload = {
    metadata: {
      change_id: changeId,
      run_id: runId,
      executor,
      bundle_id: isBundle ? evidenceBundle.bundleId : '',
      immutable: Boolean(isBundle && evidenceBundle.immutable),
      locked_at_utc: isBundle ? evidenceBundle.lockedAtUtc : '',
      exported_at_utc: normalizeText(exportedAtUtc),
    },
    gate: {
      allow_close_execution: effectiveGate.allowCloseExecution,
      allow_export: effectiveGate.allowExport,
      missing_required_keys: [...effectiveGate.missingRequiredKeys],
      issues: [...effectiveGate.gateIssues],
    },
    evidences: rows.map(row => ({
      key: row.key,
      source: row.source,
      artifact: row.artifact,
      fingerprint: row.fingerprint,
      status: row.status,
      timestamp_utc: row.timestampUtc,
      executor: row.executor,
      change_id: row.changeId,
      run_id: row.runId,
      required: Boolean(row.required),
      details: row.details,
    })),
  };

  const sanitizedPayload = sanitizeStructuredData(payload);
  return {
    ...sanitizedPayload,
    canonical_fingerprint: computeDeterministicFingerprint(sanitizedPayload),
  };
};

export const downloadEvidencePayloadFile = (fileName, payload) => {
  if (!payload || typeof window === 'undefined' || typeof document === 'undefined') {
    return;
  }

  const fileContent = JSON.stringify(payload, null, 2);
  const blob = new Blob([fileContent], { type: 'application/json;charset=utf-8' });
  const downloadUrl = window.URL.createObjectURL(blob);
  const anchor = document.createElement('a');

  anchor.href = downloadUrl;
  anchor.download = fileName;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  window.URL.revokeObjectURL(downloadUrl);
};
