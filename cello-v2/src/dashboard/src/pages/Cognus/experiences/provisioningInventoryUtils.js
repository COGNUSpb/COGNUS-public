import {
  sanitizeSensitiveText,
  sanitizeStructuredData,
} from '../../../utils/provisioningSecurityRedaction';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

export const INVENTORY_PROVIDER_SCOPE = 'external-linux';

export const INVENTORY_SOURCE_STATUS = Object.freeze({
  ready: 'ready',
  partial: 'partial',
  pending: 'pending',
});

export const INVENTORY_COVERAGE_TONE_MAP = Object.freeze({
  ready: 'success',
  partial: 'warning',
  pending: 'error',
});

const COVERAGE_PERCENT_BY_STATUS = Object.freeze({
  ready: 100,
  partial: 65,
  pending: 0,
});

function localizeInventoryUtilsText(ptBR, enUS, localeCandidate) {
  return pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());
}

const DEFAULT_FILTER_VALUE = 'all';

const SOURCE_CATALOG = Object.freeze([
  {
    key: 'legacy_inventory_api',
    label: localizeInventoryUtilsText(
      'API legada (organizações e nós)',
      'Legacy API (organizations and nodes)'
    ),
    domains: ['orgs', 'nodes'],
  },
  {
    key: 'pipeline_inventory_artifacts',
    label: localizeInventoryUtilsText(
      'Artefatos técnicos do pipeline',
      'Pipeline technical artifacts'
    ),
    domains: ['hosts', 'channels', 'inventory-final.json'],
  },
  {
    key: 'crypto_inventory_artifacts',
    label: localizeInventoryUtilsText(
      'Inventário criptográfico inicial',
      'Initial cryptographic inventory'
    ),
    domains: ['msp', 'tls', 'ca', 'inventory-crypto.json'],
  },
]);

const BASE_ORGANIZATIONS = Object.freeze([
  {
    orgId: 'infufg',
    displayName: 'INF UFG',
    mspId: 'InfufgMSP',
    environment: 'dev-external-linux',
    status: 'active',
  },
  {
    orgId: 'anatel',
    displayName: 'ANATEL',
    mspId: 'AnatelMSP',
    environment: 'dev-external-linux',
    status: 'active',
  },
  {
    orgId: 'anp',
    displayName: 'ANP',
    mspId: 'AnpMSP',
    environment: 'hml-external-linux',
    status: 'active',
  },
]);

const BASE_CHANNELS = Object.freeze([
  {
    channelId: 'ops-channel-dev',
    environment: 'dev-external-linux',
    governanceDomain: 'operations',
    members: ['infufg', 'anatel'],
    status: 'active',
  },
  {
    channelId: 'governance-channel-hml',
    environment: 'hml-external-linux',
    governanceDomain: 'governance',
    members: ['infufg', 'anp'],
    status: 'active',
  },
]);

const BASE_HOSTS = Object.freeze([
  {
    hostId: 'vm-dev-01',
    address: '10.20.0.11',
    orgId: 'infufg',
    environment: 'dev-external-linux',
    providerKey: 'external-linux',
    osFamily: 'linux',
    runtimeStatus: 'ready',
  },
  {
    hostId: 'vm-dev-02',
    address: '10.20.0.12',
    orgId: 'anatel',
    environment: 'dev-external-linux',
    providerKey: 'external-linux',
    osFamily: 'linux',
    runtimeStatus: 'ready',
  },
  {
    hostId: 'vm-hml-01',
    address: '10.20.1.21',
    orgId: 'anp',
    environment: 'hml-external-linux',
    providerKey: 'external-linux',
    osFamily: 'linux',
    runtimeStatus: 'ready',
  },
  {
    hostId: 'vm-hml-outscope-01',
    address: '10.80.0.31',
    orgId: 'anp',
    environment: 'hml-external-linux',
    providerKey: 'aws',
    osFamily: 'linux',
    runtimeStatus: 'ready',
  },
]);

const BASE_NODES = Object.freeze([
  {
    nodeId: 'peer0-infufg',
    nodeType: 'peer',
    orgId: 'infufg',
    hostId: 'vm-dev-01',
    environment: 'dev-external-linux',
    providerKey: 'external-linux',
    status: 'up',
  },
  {
    nodeId: 'peer0-anatel',
    nodeType: 'peer',
    orgId: 'anatel',
    hostId: 'vm-dev-02',
    environment: 'dev-external-linux',
    providerKey: 'external-linux',
    status: 'up',
  },
  {
    nodeId: 'orderer0-infufg',
    nodeType: 'orderer',
    orgId: 'infufg',
    hostId: 'vm-hml-01',
    environment: 'hml-external-linux',
    providerKey: 'external-linux',
    status: 'up',
  },
  {
    nodeId: 'peer0-anp',
    nodeType: 'peer',
    orgId: 'anp',
    hostId: 'vm-hml-01',
    environment: 'hml-external-linux',
    providerKey: 'external-linux',
    status: 'up',
  },
  {
    nodeId: 'peer-outscope-cloud',
    nodeType: 'peer',
    orgId: 'anp',
    hostId: 'vm-hml-outscope-01',
    environment: 'hml-external-linux',
    providerKey: 'azure',
    status: 'up',
  },
]);

const FULL_CRYPTO_BASELINE = Object.freeze([
  {
    artifactId: 'msp-root-infufg',
    orgId: 'infufg',
    environment: 'dev-external-linux',
    artifactType: 'msp-root-cert',
    fingerprint: '1f3ce0d44f49a7ed2e2cc7de9d7f8a6a1fc4ef9d95a59fbd2a4fefb6c88e4d5f',
    expiresAtUtc: '2027-08-10T18:00:00Z',
    status: 'active',
  },
  {
    artifactId: 'tls-ca-infufg',
    orgId: 'infufg',
    environment: 'dev-external-linux',
    artifactType: 'tls-ca-cert',
    fingerprint: 'c0e1b3b6b7650f44f09df64efa3f169ad6f80c43a2e748f02e82bd4f9322b1f7',
    expiresAtUtc: '2027-08-10T18:00:00Z',
    status: 'active',
  },
  {
    artifactId: 'msp-root-anatel',
    orgId: 'anatel',
    environment: 'dev-external-linux',
    artifactType: 'msp-root-cert',
    fingerprint: '7d80d4b16ab35d0b6af8a4572fdf36bc0e13b19f45f6bb2ed09b4111bce713ea',
    expiresAtUtc: '2027-09-01T10:30:00Z',
    status: 'active',
  },
  {
    artifactId: 'msp-root-anp',
    orgId: 'anp',
    environment: 'hml-external-linux',
    artifactType: 'msp-root-cert',
    fingerprint: '9db60e9f4955728c11fb89d3e8b9d9b5cf4127e6be7d8210d9870c5a9f5ff8b2',
    expiresAtUtc: '2027-11-12T09:00:00Z',
    status: 'active',
  },
]);

const REQUIRED_PIPELINE_ARTIFACTS = Object.freeze([
  {
    key: 'reconcile-report.json',
    required: true,
    origin: 'reconcile',
    description: localizeInventoryUtilsText(
      'Relatório técnico de reconciliação do pipeline.',
      'Technical pipeline reconciliation report.'
    ),
  },
  {
    key: 'inventory-final.json',
    required: true,
    origin: 'verify',
    description: localizeInventoryUtilsText(
      'Snapshot consolidado de inventário pós-provisão.',
      'Consolidated post-provision inventory snapshot.'
    ),
  },
  {
    key: 'verify-report.json',
    required: true,
    origin: 'verify',
    description: localizeInventoryUtilsText(
      'Relatório técnico final de verificação do pipeline.',
      'Final technical pipeline verification report.'
    ),
  },
  {
    key: 'ssh-execution-log.json',
    required: true,
    origin: 'provision',
    description: localizeInventoryUtilsText(
      'Log correlacionado das execuções SSH da jornada A2.',
      'Correlated log of the A2 SSH executions.'
    ),
  },
  {
    key: 'stage-reports',
    required: true,
    origin: 'pipeline',
    description: localizeInventoryUtilsText(
      'Bundle consolidado dos relatórios por etapa (prepare/provision/reconcile/verify).',
      'Consolidated bundle of reports by stage (prepare/provision/reconcile/verify).'
    ),
  },
  {
    key: 'inventory-crypto.json',
    required: false,
    origin: 'configure',
    description: localizeInventoryUtilsText(
      'Inventário criptográfico (MSP/TLS/CA) da baseline inicial.',
      'Cryptographic inventory (MSP/TLS/CA) for the initial baseline.'
    ),
  },
]);

export const INVENTORY_A2_OFFICIAL_REQUIRED_ARTIFACT_KEYS = Object.freeze([
  'reconcile-report',
  'inventory-final',
  'verify-report',
  'ssh-execution-log',
  'stage-reports',
]);

const INVENTORY_A2_OFFICIAL_REQUIRED_ARTIFACT_SPECS = Object.freeze([
  {
    key: 'reconcile-report',
    origin: 'reconcile',
    description: localizeInventoryUtilsText(
      'Relatório oficial de reconciliação da execução.',
      'Official execution reconciliation report.'
    ),
    expectedTokens: ['reconcile-report', 'stage-reports/reconcile-report'],
  },
  {
    key: 'inventory-final',
    origin: 'verify',
    description: localizeInventoryUtilsText(
      'Snapshot oficial final de inventário pós-run.',
      'Official final post-run inventory snapshot.'
    ),
    expectedTokens: ['inventory-final', 'inventory-final/inventory-final'],
  },
  {
    key: 'verify-report',
    origin: 'verify',
    description: localizeInventoryUtilsText(
      'Relatório oficial de verificação da topologia.',
      'Official topology verification report.'
    ),
    expectedTokens: ['verify-report', 'stage-reports/verify-report'],
  },
  {
    key: 'ssh-execution-log',
    origin: 'prepare',
    description: localizeInventoryUtilsText(
      'Log oficial da execução SSH por host/checkpoint.',
      'Official SSH execution log by host/checkpoint.'
    ),
    expectedTokens: ['ssh-execution-log'],
  },
  {
    key: 'stage-reports',
    origin: 'pipeline',
    description: localizeInventoryUtilsText(
      'Bundle oficial de relatórios por estágio.',
      'Official bundle of reports by stage.'
    ),
    expectedTokens: ['stage-reports'],
  },
]);

const DEFAULT_A2_STAGE_KEYS = Object.freeze(['prepare', 'provision', 'reconcile', 'verify']);

const isPlainObject = value => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const normalizeText = value => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim();
};

const normalizeLower = value => normalizeText(value).toLowerCase();

const safeArray = value => (Array.isArray(value) ? value : []);

const normalizeArtifactToken = value =>
  normalizeLower(value)
    .replace(/\\/g, '/')
    .replace(/:/g, '/')
    .replace(/\.jsonl?$/g, '')
    .replace(/\/{2,}/g, '/');

const toUniqueTokens = values =>
  [
    ...new Set((Array.isArray(values) ? values : []).map(normalizeArtifactToken).filter(Boolean)),
  ].sort();

const normalizeArtifactCatalog = artifacts =>
  toUniqueTokens(
    safeArray(artifacts).map(item => {
      if (typeof item === 'string') {
        return item;
      }
      if (isPlainObject(item)) {
        return item.key || item.path || item.artifact || item.name || '';
      }
      return '';
    })
  );

const artifactTokensMatch = (leftToken, rightToken) => {
  const normalizedLeft = normalizeArtifactToken(leftToken);
  const normalizedRight = normalizeArtifactToken(rightToken);
  if (!normalizedLeft || !normalizedRight) {
    return false;
  }
  return (
    normalizedLeft === normalizedRight ||
    normalizedLeft.includes(normalizedRight) ||
    normalizedRight.includes(normalizedLeft)
  );
};

const catalogIncludesArtifactToken = (catalogTokens, expectedToken) =>
  (Array.isArray(catalogTokens) ? catalogTokens : []).some(catalogToken =>
    artifactTokensMatch(catalogToken, expectedToken)
  );

const latestUtcTimestamp = values =>
  [...new Set((Array.isArray(values) ? values : []).map(normalizeText).filter(Boolean))]
    .sort((left, right) => left.localeCompare(right))
    .slice(-1)[0] || '';

const buildOfficialArtifactRows = officialRun =>
  safeArray(officialRun && officialRun.artifact_rows).map((row, index) => {
    const artifactGroup = normalizeText(row && row.artifact_group);
    const artifactName = normalizeText(row && row.artifact_name);
    const artifactKey = normalizeText(row && row.artifact_key);
    const displayKey = normalizeText(row && row.key);
    const available = typeof (row && row.available) === 'boolean' ? Boolean(row.available) : true;
    const fingerprintSha256 = normalizeText(
      row && (row.fingerprint_sha256 || row.fingerprint || row.payload_hash)
    );
    const validatedAtUtc = normalizeText(
      row && (row.validated_at_utc || row.timestamp_utc || row.timestamp)
    );

    const tokens = toUniqueTokens([
      displayKey,
      artifactKey,
      artifactGroup,
      artifactName,
      artifactGroup && artifactName ? `${artifactGroup}/${artifactName}` : '',
      artifactGroup && artifactName ? `${artifactGroup}:${artifactName}` : '',
    ]);

    return {
      key: sanitizeSensitiveText(displayKey || artifactKey || `artifact-row-${index + 1}`),
      artifactKey: sanitizeSensitiveText(artifactKey),
      artifactGroup: sanitizeSensitiveText(artifactGroup),
      artifactName: sanitizeSensitiveText(artifactName),
      available,
      fingerprintSha256,
      validatedAtUtc,
      tokens,
    };
  });

const resolveEvidenceSourceLabel = ({ fromCatalog = false, fromArtifactRows = false }) => {
  if (fromCatalog && fromArtifactRows) {
    return localizeInventoryUtilsText(
      'a2_2_available_artifacts + artifact_rows',
      'a2_2_available_artifacts + artifact_rows'
    );
  }
  if (fromArtifactRows) {
    return localizeInventoryUtilsText('artifact_rows', 'artifact_rows');
  }
  if (fromCatalog) {
    return localizeInventoryUtilsText(
      'a2_2_available_artifacts',
      'a2_2_available_artifacts'
    );
  }
  return localizeInventoryUtilsText('-', '-');
};

const resolveStageReportExpectedTokens = officialRun => {
  const stageKeys = toUniqueTokens(
    safeArray(officialRun && officialRun.stages).map(stage => normalizeText(stage && stage.key))
  );
  const effectiveStageKeys = stageKeys.length > 0 ? stageKeys : [...DEFAULT_A2_STAGE_KEYS];
  return effectiveStageKeys.map(stageKey => `stage-reports/${stageKey}-report`);
};

const resolveArtifactCoverageStatus = ({ available = false, consistent = false } = {}) => {
  if (consistent) {
    return INVENTORY_SOURCE_STATUS.ready;
  }
  if (available) {
    return INVENTORY_SOURCE_STATUS.partial;
  }
  return INVENTORY_SOURCE_STATUS.pending;
};

const resolveFingerprintByToken = (rows, expectedToken) => {
  const matchedRow = (Array.isArray(rows) ? rows : []).find(
    row =>
      row &&
      row.available &&
      row.fingerprintSha256 &&
      catalogIncludesArtifactToken(row.tokens, expectedToken)
  );
  return matchedRow ? matchedRow.fingerprintSha256 : '';
};

const resolveValidatedAtByToken = (rows, expectedToken) => {
  const matchedRow = (Array.isArray(rows) ? rows : []).find(
    row =>
      row &&
      row.available &&
      row.validatedAtUtc &&
      catalogIncludesArtifactToken(row.tokens, expectedToken)
  );
  return matchedRow ? matchedRow.validatedAtUtc : '';
};

const normalizeStatus = status => {
  const normalized = normalizeLower(status);
  if (normalized === INVENTORY_SOURCE_STATUS.ready) {
    return INVENTORY_SOURCE_STATUS.ready;
  }
  if (normalized === INVENTORY_SOURCE_STATUS.partial) {
    return INVENTORY_SOURCE_STATUS.partial;
  }
  return INVENTORY_SOURCE_STATUS.pending;
};

const normalizeFilterValue = value => {
  const normalized = normalizeText(value);
  return normalized || DEFAULT_FILTER_VALUE;
};

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const toCoveragePercent = status => {
  const normalized = normalizeStatus(status);
  return COVERAGE_PERCENT_BY_STATUS[normalized];
};

const mapUniqueOptions = values => {
  const normalizedValues = values.map(normalizeText).filter(Boolean);
  return [...new Set(normalizedValues)].sort((left, right) => left.localeCompare(right));
};

const deriveSyncPhase = syncCount => {
  const normalizedCount = Number.isInteger(syncCount) ? syncCount : 0;
  if (normalizedCount <= 0) {
    return 'unsynced';
  }
  if (normalizedCount % 2 === 1) {
    return 'partial';
  }
  return 'full';
};

const buildSourceRows = (syncPhase, syncedAt) => {
  if (syncPhase === 'unsynced') {
    return SOURCE_CATALOG.map(source => ({
      ...source,
      status: INVENTORY_SOURCE_STATUS.pending,
      coveragePercent: toCoveragePercent(INVENTORY_SOURCE_STATUS.pending),
      lastSyncedAt: '',
    }));
  }

  if (syncPhase === 'partial') {
    return [
      {
        ...SOURCE_CATALOG[0],
        status: INVENTORY_SOURCE_STATUS.ready,
        coveragePercent: toCoveragePercent(INVENTORY_SOURCE_STATUS.ready),
        lastSyncedAt: syncedAt,
      },
      {
        ...SOURCE_CATALOG[1],
        status: INVENTORY_SOURCE_STATUS.partial,
        coveragePercent: toCoveragePercent(INVENTORY_SOURCE_STATUS.partial),
        lastSyncedAt: syncedAt,
      },
      {
        ...SOURCE_CATALOG[2],
        status: INVENTORY_SOURCE_STATUS.partial,
        coveragePercent: 50,
        lastSyncedAt: syncedAt,
      },
    ];
  }

  return SOURCE_CATALOG.map(source => ({
    ...source,
    status: INVENTORY_SOURCE_STATUS.ready,
    coveragePercent: toCoveragePercent(INVENTORY_SOURCE_STATUS.ready),
    lastSyncedAt: syncedAt,
  }));
};

const buildArtifactRows = (syncPhase, syncedAt) =>
  REQUIRED_PIPELINE_ARTIFACTS.map(artifact => {
    let available = false;
    if (syncPhase === 'partial') {
      available = artifact.key === 'inventory-final.json' || artifact.key === 'stage-reports';
    }
    if (syncPhase === 'full') {
      available = true;
    }

    return {
      ...artifact,
      available,
      validatedAt: available ? syncedAt : '',
    };
  });

const buildCryptoBaseline = syncPhase => {
  if (syncPhase === 'unsynced') {
    return [];
  }
  if (syncPhase === 'partial') {
    return FULL_CRYPTO_BASELINE.slice(0, 2);
  }
  return [...FULL_CRYPTO_BASELINE];
};

const applyProviderScopeToRecords = records => {
  const filteredHosts = records.hosts.filter(
    host => normalizeLower(host.providerKey) === INVENTORY_PROVIDER_SCOPE
  );
  const filteredNodes = records.nodes.filter(
    node => normalizeLower(node.providerKey) === INVENTORY_PROVIDER_SCOPE
  );

  return {
    organizations: [...records.organizations],
    channels: [...records.channels],
    hosts: filteredHosts,
    nodes: filteredNodes,
    cryptoBaseline: [...records.cryptoBaseline],
    outOfScopeCounters: {
      hosts: records.hosts.length - filteredHosts.length,
      nodes: records.nodes.length - filteredNodes.length,
    },
  };
};

const resolveCoverageStatus = coveragePercent => {
  if (coveragePercent >= 100) {
    return INVENTORY_SOURCE_STATUS.ready;
  }
  if (coveragePercent > 0) {
    return INVENTORY_SOURCE_STATUS.partial;
  }
  return INVENTORY_SOURCE_STATUS.pending;
};

const evaluateInventoryConsistency = ({ artifactRows, cryptoBaseline }) => {
  const missingArtifacts = artifactRows
    .filter(artifact => artifact.required && !artifact.available)
    .map(artifact => artifact.key);
  const hasCryptoBaseline = cryptoBaseline.length > 0;
  const isConsistent = missingArtifacts.length === 0;

  const issues = [];
  if (missingArtifacts.length > 0) {
    issues.push(
      localizeInventoryUtilsText(
        `Artefatos obrigatórios ausentes: ${missingArtifacts.join(', ')}.`,
        `Required artifacts missing: ${missingArtifacts.join(', ')}.`
      )
    );
  }
  if (!hasCryptoBaseline) {
    issues.push(
      localizeInventoryUtilsText(
        'Baseline criptográfico inicial indisponível no inventário atual.',
        'Initial cryptographic baseline unavailable in the current inventory.'
      )
    );
  }

  return {
    isConsistent,
    level: isConsistent ? 'success' : 'warning',
    missingArtifacts,
    issues,
  };
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

const toCanonicalFingerprint = value => {
  const payload = JSON.stringify(value || {});
  return [seededHash(payload, 0x811c9dc5), seededHash(payload, 0x27d4eb2f)].join('');
};

const cloneSnapshot = snapshot => {
  if (!isPlainObject(snapshot)) {
    return {
      providerScope: INVENTORY_PROVIDER_SCOPE,
      syncCount: 0,
      syncPhase: 'unsynced',
      syncedAt: '',
      changeId: '',
      runId: '',
      sourceRows: [],
      coveragePercent: 0,
      coverageStatus: INVENTORY_SOURCE_STATUS.pending,
      artifactRows: [],
      consistency: {
        isConsistent: false,
        level: 'warning',
        missingArtifacts: [],
        issues: [],
      },
      scopeEnforcement: {
        excludedHosts: 0,
        excludedNodes: 0,
        excludedTotal: 0,
      },
      records: {
        organizations: [],
        channels: [],
        hosts: [],
        nodes: [],
        cryptoBaseline: [],
      },
    };
  }

  return {
    ...snapshot,
    sourceRows: Array.isArray(snapshot.sourceRows)
      ? snapshot.sourceRows.map(source => ({ ...source }))
      : [],
    artifactRows: Array.isArray(snapshot.artifactRows)
      ? snapshot.artifactRows.map(artifact => ({ ...artifact }))
      : [],
    records: {
      organizations: Array.isArray(snapshot.records && snapshot.records.organizations)
        ? snapshot.records.organizations.map(record => ({ ...record }))
        : [],
      channels: Array.isArray(snapshot.records && snapshot.records.channels)
        ? snapshot.records.channels.map(record => ({ ...record, members: [...record.members] }))
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
  };
};

export function createInventorySnapshotState({
  syncCount = 0,
  syncedAt = '',
  changeId = '',
  runId = '',
} = {}) {
  const syncPhase = deriveSyncPhase(syncCount);
  const sourceRows = buildSourceRows(syncPhase, normalizeText(syncedAt));
  const artifactRows = buildArtifactRows(syncPhase, normalizeText(syncedAt));
  const scopedRecords = applyProviderScopeToRecords({
    organizations: [...BASE_ORGANIZATIONS],
    channels: [...BASE_CHANNELS],
    hosts: [...BASE_HOSTS],
    nodes: [...BASE_NODES],
    cryptoBaseline: buildCryptoBaseline(syncPhase),
  });

  const coveragePercent =
    sourceRows.length > 0
      ? Math.round(
          sourceRows.reduce((accumulator, source) => accumulator + source.coveragePercent, 0) /
            sourceRows.length
        )
      : 0;
  const consistency = evaluateInventoryConsistency({
    artifactRows,
    cryptoBaseline: scopedRecords.cryptoBaseline,
  });

  return {
    providerScope: INVENTORY_PROVIDER_SCOPE,
    syncCount: Number.isInteger(syncCount) ? syncCount : 0,
    syncPhase,
    syncedAt: normalizeText(syncedAt),
    changeId: normalizeText(changeId),
    runId: normalizeText(runId),
    sourceRows,
    coveragePercent,
    coverageStatus: resolveCoverageStatus(coveragePercent),
    artifactRows,
    consistency,
    scopeEnforcement: {
      excludedHosts: scopedRecords.outOfScopeCounters.hosts,
      excludedNodes: scopedRecords.outOfScopeCounters.nodes,
      excludedTotal:
        scopedRecords.outOfScopeCounters.hosts + scopedRecords.outOfScopeCounters.nodes,
    },
    records: {
      organizations: scopedRecords.organizations,
      channels: scopedRecords.channels,
      hosts: scopedRecords.hosts,
      nodes: scopedRecords.nodes,
      cryptoBaseline: scopedRecords.cryptoBaseline,
    },
  };
}

export const synchronizeInventorySnapshot = (
  currentSnapshot,
  { timestamp = toIsoUtc(), changeId = '', runId = '' } = {}
) => {
  const previousSnapshot = cloneSnapshot(currentSnapshot);
  const nextSyncCount = (previousSnapshot.syncCount || 0) + 1;

  return createInventorySnapshotState({
    syncCount: nextSyncCount,
    syncedAt: normalizeText(timestamp),
    changeId: normalizeText(changeId) || previousSnapshot.changeId,
    runId: normalizeText(runId) || previousSnapshot.runId,
  });
};

export const buildInventoryFilterOptions = snapshot => {
  const state = cloneSnapshot(snapshot);
  const organizations = mapUniqueOptions(state.records.organizations.map(record => record.orgId));
  const channels = mapUniqueOptions(state.records.channels.map(record => record.channelId));
  const environments = mapUniqueOptions(
    [
      ...state.records.organizations.map(record => record.environment),
      ...state.records.channels.map(record => record.environment),
      ...state.records.hosts.map(record => record.environment),
      ...state.records.nodes.map(record => record.environment),
      ...state.records.cryptoBaseline.map(record => record.environment),
    ].filter(Boolean)
  );

  return {
    organizations,
    channels,
    environments,
  };
};

const resolveSelectedChannels = (channels, selectedChannel, selectedEnvironment) =>
  channels.filter(channel => {
    if (
      selectedEnvironment !== DEFAULT_FILTER_VALUE &&
      channel.environment !== selectedEnvironment
    ) {
      return false;
    }

    if (selectedChannel !== DEFAULT_FILTER_VALUE && channel.channelId !== selectedChannel) {
      return false;
    }

    return true;
  });

export const filterInventorySnapshot = (
  snapshot,
  {
    organization = DEFAULT_FILTER_VALUE,
    channel = DEFAULT_FILTER_VALUE,
    environment = DEFAULT_FILTER_VALUE,
  } = {}
) => {
  const state = cloneSnapshot(snapshot);
  const selectedOrganization = normalizeFilterValue(organization);
  const selectedChannel = normalizeFilterValue(channel);
  const selectedEnvironment = normalizeFilterValue(environment);

  const filteredChannels = resolveSelectedChannels(
    state.records.channels,
    selectedChannel,
    selectedEnvironment
  );
  const channelMemberOrgIds = new Set(
    filteredChannels.flatMap(filteredChannel => filteredChannel.members || [])
  );

  const filteredOrganizations = state.records.organizations.filter(record => {
    if (
      selectedEnvironment !== DEFAULT_FILTER_VALUE &&
      record.environment !== selectedEnvironment
    ) {
      return false;
    }
    if (selectedOrganization !== DEFAULT_FILTER_VALUE && record.orgId !== selectedOrganization) {
      return false;
    }
    if (selectedChannel !== DEFAULT_FILTER_VALUE && !channelMemberOrgIds.has(record.orgId)) {
      return false;
    }
    return true;
  });

  const allowedOrgIds = new Set(filteredOrganizations.map(record => record.orgId));
  const effectiveOrgIds = allowedOrgIds.size > 0 ? allowedOrgIds : channelMemberOrgIds;

  const filteredHosts = state.records.hosts.filter(record => {
    if (
      selectedEnvironment !== DEFAULT_FILTER_VALUE &&
      record.environment !== selectedEnvironment
    ) {
      return false;
    }
    if (selectedOrganization !== DEFAULT_FILTER_VALUE && record.orgId !== selectedOrganization) {
      return false;
    }
    if (selectedChannel !== DEFAULT_FILTER_VALUE && !effectiveOrgIds.has(record.orgId)) {
      return false;
    }
    return true;
  });

  const filteredNodes = state.records.nodes.filter(record => {
    if (
      selectedEnvironment !== DEFAULT_FILTER_VALUE &&
      record.environment !== selectedEnvironment
    ) {
      return false;
    }
    if (selectedOrganization !== DEFAULT_FILTER_VALUE && record.orgId !== selectedOrganization) {
      return false;
    }
    if (selectedChannel !== DEFAULT_FILTER_VALUE && !effectiveOrgIds.has(record.orgId)) {
      return false;
    }
    return true;
  });

  const filteredCryptoBaseline = state.records.cryptoBaseline.filter(record => {
    if (
      selectedEnvironment !== DEFAULT_FILTER_VALUE &&
      record.environment !== selectedEnvironment
    ) {
      return false;
    }
    if (selectedOrganization !== DEFAULT_FILTER_VALUE && record.orgId !== selectedOrganization) {
      return false;
    }
    if (selectedChannel !== DEFAULT_FILTER_VALUE && !effectiveOrgIds.has(record.orgId)) {
      return false;
    }
    return true;
  });

  return {
    organizations: filteredOrganizations,
    channels: filteredChannels,
    hosts: filteredHosts,
    nodes: filteredNodes,
    cryptoBaseline: filteredCryptoBaseline,
  };
};

export const evaluateOfficialInventoryEvidenceGate = ({
  officialRun,
  changeId = '',
  runId = '',
} = {}) => {
  const safeRun = isPlainObject(officialRun) ? officialRun : {};
  const hasOfficialRunPayload = isPlainObject(officialRun);
  const expectedChangeId = normalizeText(changeId);
  const expectedRunId = normalizeText(runId);
  const officialChangeId = normalizeText(safeRun.change_id);
  const officialRunId = normalizeText(safeRun.run_id);
  const manifestFingerprint = normalizeLower(safeRun.manifest_fingerprint);
  const sourceBlueprintFingerprint = normalizeLower(safeRun.source_blueprint_fingerprint);
  const blueprintFingerprint = normalizeLower(safeRun.blueprint_fingerprint);
  const handoffFingerprint = normalizeLower(safeRun.handoff_fingerprint);
  const backendState = normalizeText(safeRun.backend_state);
  const pipelineStatus = normalizeText(safeRun.status);
  let officialDecision = {};
  if (isPlainObject(safeRun.official_decision)) {
    officialDecision = safeRun.official_decision;
  } else if (isPlainObject(safeRun.snapshot && safeRun.snapshot.official_decision)) {
    officialDecision = safeRun.snapshot.official_decision;
  }

  const requiredArtifactsFromBackend = normalizeArtifactCatalog([
    ...safeArray(safeRun.a2_2_minimum_artifacts),
    ...safeArray(safeRun.required_artifacts),
  ]);
  const effectiveRequiredArtifacts =
    requiredArtifactsFromBackend.length > 0
      ? requiredArtifactsFromBackend
      : [...INVENTORY_A2_OFFICIAL_REQUIRED_ARTIFACT_KEYS];
  const availableArtifactsFromBackend = normalizeArtifactCatalog([
    ...safeArray(safeRun.a2_2_available_artifacts),
    ...safeArray(safeRun.available_artifacts),
  ]);

  const officialArtifactRows = buildOfficialArtifactRows(safeRun);
  const availableOfficialArtifactRows = officialArtifactRows.filter(row => row.available);
  const expectedStageReportTokens = resolveStageReportExpectedTokens(safeRun);
  const stageReportRows = availableOfficialArtifactRows.filter(row =>
    row.tokens.some(token => token.startsWith('stage-reports/') && token.endsWith('-report'))
  );

  const artifactRows = INVENTORY_A2_OFFICIAL_REQUIRED_ARTIFACT_SPECS.map(spec => {
    const required = true;
    const expectedTokens = toUniqueTokens(spec.expectedTokens);
    const expectedByBackend = effectiveRequiredArtifacts.some(requiredToken =>
      expectedTokens.some(expectedToken => artifactTokensMatch(requiredToken, expectedToken))
    );

    if (spec.key === 'stage-reports') {
      const availableFromCatalog = catalogIncludesArtifactToken(
        availableArtifactsFromBackend,
        'stage-reports'
      );
      const availableFromRows = stageReportRows.length > 0;
      const available = availableFromCatalog || availableFromRows;
      const missingStageReports = expectedStageReportTokens.filter(
        stageToken =>
          !stageReportRows.some(row => catalogIncludesArtifactToken(row.tokens, stageToken))
      );
      const missingStageReportFingerprints = expectedStageReportTokens.filter(
        stageToken => !resolveFingerprintByToken(stageReportRows, stageToken)
      );
      const missingStageReportValidation = expectedStageReportTokens.filter(
        stageToken => !resolveValidatedAtByToken(stageReportRows, stageToken)
      );

      const inconsistencyReasons = [];
      if (available && !availableFromRows) {
        inconsistencyReasons.push(
          localizeInventoryUtilsText(
            'Disponível apenas no catálogo a2_2_available_artifacts, sem linhas oficiais em artifact_rows.',
            'Available only in the a2_2_available_artifacts catalog, without official lines in artifact_rows.'
          )
        );
      }
      if (missingStageReports.length > 0) {
        inconsistencyReasons.push(
          localizeInventoryUtilsText(
            `stage_reports ausentes: ${missingStageReports.join(', ')}.`,
            `Missing stage_reports: ${missingStageReports.join(', ')}.`
          )
        );
      }
      if (missingStageReportFingerprints.length > 0) {
        inconsistencyReasons.push(
          localizeInventoryUtilsText(
            `stage_reports sem fingerprint: ${missingStageReportFingerprints.join(', ')}.`,
            `stage_reports without fingerprint: ${missingStageReportFingerprints.join(', ')}.`
          )
        );
      }
      if (missingStageReportValidation.length > 0) {
        inconsistencyReasons.push(
          localizeInventoryUtilsText(
            `stage_reports sem timestamp de validação: ${missingStageReportValidation.join(', ')}.`,
            `stage_reports without validation timestamp: ${missingStageReportValidation.join(
              ', '
            )}.`
          )
        );
      }

      const fingerprints = expectedStageReportTokens
        .map(stageToken => resolveFingerprintByToken(stageReportRows, stageToken))
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right));
      const fingerprintSha256 = fingerprints.length > 0 ? toCanonicalFingerprint(fingerprints) : '';
      const validatedAtUtc = latestUtcTimestamp(
        expectedStageReportTokens.map(stageToken =>
          resolveValidatedAtByToken(stageReportRows, stageToken)
        )
      );
      const consistent = available && inconsistencyReasons.length === 0;
      const status = resolveArtifactCoverageStatus({ available, consistent });

      return {
        key: spec.key,
        origin: spec.origin,
        required,
        expectedByBackend,
        available,
        consistent,
        status,
        evidenceSource: resolveEvidenceSourceLabel({
          fromCatalog: availableFromCatalog,
          fromArtifactRows: availableFromRows,
        }),
        fingerprintSha256,
        validatedAtUtc,
        matchedArtifactKeys: [...new Set(stageReportRows.map(row => row.key || row.artifactKey))]
          .filter(Boolean)
          .sort((left, right) => left.localeCompare(right)),
        inconsistencyReasons,
        missingStageReports,
        description: spec.description,
      };
    }

    const matchedRows = availableOfficialArtifactRows.filter(row =>
      expectedTokens.some(expectedToken => catalogIncludesArtifactToken(row.tokens, expectedToken))
    );
    const availableFromCatalog = expectedTokens.some(expectedToken =>
      catalogIncludesArtifactToken(availableArtifactsFromBackend, expectedToken)
    );
    const availableFromRows = matchedRows.length > 0;
    const available = availableFromCatalog || availableFromRows;
    const fingerprintSha256 = matchedRows.map(row => row.fingerprintSha256).find(Boolean) || '';
    const validatedAtUtc = latestUtcTimestamp(matchedRows.map(row => row.validatedAtUtc));

    const inconsistencyReasons = [];
    if (available && !availableFromRows) {
      inconsistencyReasons.push(
        localizeInventoryUtilsText(
          'Disponível apenas no catálogo a2_2_available_artifacts, sem linhas oficiais em artifact_rows.',
          'Available only in the a2_2_available_artifacts catalog, without official lines in artifact_rows.'
        )
      );
    }
    if (availableFromRows && !fingerprintSha256) {
      inconsistencyReasons.push(
        localizeInventoryUtilsText(
          'Artefato oficial sem fingerprint_sha256.',
          'Official artifact without fingerprint_sha256.'
        )
      );
    }
    if (availableFromRows && !validatedAtUtc) {
      inconsistencyReasons.push(
        localizeInventoryUtilsText(
          'Artefato oficial sem validated_at_utc.',
          'Official artifact without validated_at_utc.'
        )
      );
    }

    const consistent = available && inconsistencyReasons.length === 0;
    const status = resolveArtifactCoverageStatus({ available, consistent });

    return {
      key: spec.key,
      origin: spec.origin,
      required,
      expectedByBackend,
      available,
      consistent,
      status,
      evidenceSource: resolveEvidenceSourceLabel({
        fromCatalog: availableFromCatalog,
        fromArtifactRows: availableFromRows,
      }),
      fingerprintSha256,
      validatedAtUtc,
      matchedArtifactKeys: [...new Set(matchedRows.map(row => row.key || row.artifactKey))]
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right)),
      inconsistencyReasons,
      missingStageReports: [],
      description: spec.description,
    };
  });

  const missingRequiredArtifacts = artifactRows
    .filter(row => row.required && !row.available)
    .map(row => sanitizeSensitiveText(row.key));
  const inconsistentArtifacts = artifactRows
    .filter(row => row.required && row.available && !row.consistent)
    .map(row => sanitizeSensitiveText(row.key));

  const correlationIssues = [];
  if (!hasOfficialRunPayload) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        'Runbook oficial indisponível para validação dos artefatos auditáveis A2.',
        'Official runbook unavailable for validation of auditable A2 artifacts.'
      )
    );
  }
  if (!officialRunId) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        'run_id oficial ausente no retorno do backend.',
        'Official run_id missing from the backend response.'
      )
    );
  }
  if (!officialChangeId) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        'change_id oficial ausente no retorno do backend.',
        'Official change_id missing from the backend response.'
      )
    );
  }
  if (expectedRunId && officialRunId && expectedRunId !== officialRunId) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        `run_id divergente: UI=${expectedRunId} | backend=${officialRunId}.`,
        `Divergent run_id: UI=${expectedRunId} | backend=${officialRunId}.`
      )
    );
  }
  if (expectedChangeId && officialChangeId && expectedChangeId !== officialChangeId) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        `change_id divergente: UI=${expectedChangeId} | backend=${officialChangeId}.`,
        `Divergent change_id: UI=${expectedChangeId} | backend=${officialChangeId}.`
      )
    );
  }
  if (!manifestFingerprint) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        'manifest_fingerprint ausente no contexto oficial para exportação auditável.',
        'manifest_fingerprint missing from the official context for auditable export.'
      )
    );
  }
  if (!sourceBlueprintFingerprint) {
    correlationIssues.push(
      localizeInventoryUtilsText(
        'source_blueprint_fingerprint ausente no contexto oficial para exportação auditável.',
        'source_blueprint_fingerprint missing from the official context for auditable export.'
      )
    );
  }

  const gateIssues = [
    ...correlationIssues,
    ...missingRequiredArtifacts.map(
      artifactKey =>
        localizeInventoryUtilsText(
          `Evidência oficial obrigatória ausente: ${artifactKey}.`,
          `Required official evidence missing: ${artifactKey}.`
        )
    ),
    ...inconsistentArtifacts.map(artifactKey =>
      localizeInventoryUtilsText(
        `Evidência oficial inconsistente: ${artifactKey}.`,
        `Inconsistent official evidence: ${artifactKey}.`
      )
    ),
  ].map(issue => sanitizeSensitiveText(issue));

  const readyCount = artifactRows.filter(row => row.status === INVENTORY_SOURCE_STATUS.ready)
    .length;
  const partialCount = artifactRows.filter(row => row.status === INVENTORY_SOURCE_STATUS.partial)
    .length;
  const pendingCount = artifactRows.filter(row => row.status === INVENTORY_SOURCE_STATUS.pending)
    .length;
  const coveragePercent =
    artifactRows.length > 0
      ? Math.round(((readyCount + partialCount * 0.5) / artifactRows.length) * 100)
      : 0;
  let coverageStatus = INVENTORY_SOURCE_STATUS.pending;
  if (readyCount === artifactRows.length && artifactRows.length > 0) {
    coverageStatus = INVENTORY_SOURCE_STATUS.ready;
  } else if (readyCount > 0 || partialCount > 0) {
    coverageStatus = INVENTORY_SOURCE_STATUS.partial;
  }

  const sanitizedArtifactRows = artifactRows.map(row => ({
    ...row,
    key: sanitizeSensitiveText(row.key),
    evidenceSource: sanitizeSensitiveText(row.evidenceSource),
    matchedArtifactKeys: safeArray(row.matchedArtifactKeys).map(value =>
      sanitizeSensitiveText(value)
    ),
    inconsistencyReasons: safeArray(row.inconsistencyReasons).map(value =>
      sanitizeSensitiveText(value)
    ),
    missingStageReports: safeArray(row.missingStageReports).map(value =>
      sanitizeSensitiveText(value)
    ),
    description: sanitizeSensitiveText(row.description),
  }));
  const sanitizedCorrelationIssues = correlationIssues.map(issue => sanitizeSensitiveText(issue));

  return {
    hasOfficialRun: Boolean(hasOfficialRunPayload && (officialRunId || artifactRows.length > 0)),
    allowExport: gateIssues.length === 0,
    allowCloseExecution: gateIssues.length === 0,
    coverageStatus,
    coveragePercent,
    readyCount,
    partialCount,
    pendingCount,
    requiredArtifacts: [...INVENTORY_A2_OFFICIAL_REQUIRED_ARTIFACT_KEYS],
    requiredArtifactsFromBackend: [...effectiveRequiredArtifacts],
    availableArtifactsFromBackend: [...availableArtifactsFromBackend],
    missingRequiredArtifacts,
    inconsistentArtifacts,
    correlationIssues: sanitizedCorrelationIssues,
    gateIssues,
    artifactRows: sanitizedArtifactRows,
    correlation: {
      expectedChangeId,
      expectedRunId,
      officialChangeId,
      officialRunId,
      manifestFingerprint,
      sourceBlueprintFingerprint,
      blueprintFingerprint,
      handoffFingerprint,
      backendState,
      pipelineStatus,
      officialDecision: normalizeLower(officialDecision.decision),
      officialDecisionCode: sanitizeSensitiveText(
        normalizeText(officialDecision.decision_code || officialDecision.decisionCode)
      ),
      officialDecisionTimestamp: normalizeText(
        officialDecision.timestamp_utc || officialDecision.timestamp
      ),
      isComplete:
        Boolean(officialRunId) &&
        Boolean(officialChangeId) &&
        Boolean(manifestFingerprint) &&
        Boolean(sourceBlueprintFingerprint),
    },
  };
};

export const buildInventoryExportPayload = (
  snapshot,
  filteredInventory,
  {
    filters = {},
    changeId = '',
    runId = '',
    exportedAt = toIsoUtc(),
    officialRun = null,
    officialArtifactGate = null,
  } = {}
) => {
  const state = cloneSnapshot(snapshot);
  const records = filteredInventory || filterInventorySnapshot(state, filters);
  const effectiveChangeId = normalizeText(changeId) || state.changeId;
  const effectiveRunId = normalizeText(runId) || state.runId;
  const normalizedFilters = {
    organization: normalizeFilterValue(filters.organization),
    channel: normalizeFilterValue(filters.channel),
    environment: normalizeFilterValue(filters.environment),
  };
  const effectiveOfficialGate =
    isPlainObject(officialArtifactGate) && Array.isArray(officialArtifactGate.artifactRows)
      ? officialArtifactGate
      : evaluateOfficialInventoryEvidenceGate({
          officialRun,
          changeId: effectiveChangeId,
          runId: effectiveRunId,
        });
  const officialCorrelation = isPlainObject(effectiveOfficialGate.correlation)
    ? effectiveOfficialGate.correlation
    : {};
  const officialRows = Array.isArray(effectiveOfficialGate.artifactRows)
    ? effectiveOfficialGate.artifactRows.map(row => ({
        key: normalizeText(row.key),
        origin: normalizeText(row.origin),
        required: Boolean(row.required),
        expected_by_backend: Boolean(row.expectedByBackend),
        available: Boolean(row.available),
        consistent: Boolean(row.consistent),
        status: normalizeText(row.status),
        source: normalizeText(row.evidenceSource),
        fingerprint_sha256: normalizeText(row.fingerprintSha256),
        validated_at_utc: normalizeText(row.validatedAtUtc),
        matched_artifact_keys: safeArray(row.matchedArtifactKeys).map(value =>
          normalizeText(value)
        ),
        inconsistency_reasons: safeArray(row.inconsistencyReasons).map(value =>
          normalizeText(value)
        ),
        missing_stage_reports: safeArray(row.missingStageReports).map(value =>
          normalizeText(value)
        ),
        description: normalizeText(row.description),
      }))
    : [];

  const payload = {
    metadata: {
      provider_scope: INVENTORY_PROVIDER_SCOPE,
      change_id: effectiveChangeId,
      run_id: effectiveRunId,
      synced_at: state.syncedAt,
      exported_at: normalizeText(exportedAt),
      sync_phase: state.syncPhase,
      sync_count: state.syncCount,
      official_gate_allow_export: Boolean(effectiveOfficialGate.allowExport),
      official_gate_allow_close_execution: Boolean(effectiveOfficialGate.allowCloseExecution),
      official_correlation_complete: Boolean(officialCorrelation.isComplete),
    },
    filters: normalizedFilters,
    coverage: {
      status: state.coverageStatus,
      percent: state.coveragePercent,
      sources: state.sourceRows.map(source => ({
        source_key: source.key,
        status: source.status,
        coverage_percent: source.coveragePercent,
      })),
    },
    consistency: {
      is_consistent: state.consistency.isConsistent,
      missing_artifacts: [...state.consistency.missingArtifacts],
      issues: [...state.consistency.issues],
      artifacts: state.artifactRows.map(artifact => ({
        key: artifact.key,
        required: artifact.required,
        available: artifact.available,
        origin: artifact.origin,
      })),
    },
    official_correlation: {
      expected_change_id: normalizeText(officialCorrelation.expectedChangeId),
      expected_run_id: normalizeText(officialCorrelation.expectedRunId),
      official_change_id: normalizeText(officialCorrelation.officialChangeId),
      official_run_id: normalizeText(officialCorrelation.officialRunId),
      manifest_fingerprint: normalizeText(officialCorrelation.manifestFingerprint),
      source_blueprint_fingerprint: normalizeText(officialCorrelation.sourceBlueprintFingerprint),
      blueprint_fingerprint: normalizeText(officialCorrelation.blueprintFingerprint),
      handoff_fingerprint: normalizeText(officialCorrelation.handoffFingerprint),
      backend_state: normalizeText(officialCorrelation.backendState),
      pipeline_status: normalizeText(officialCorrelation.pipelineStatus),
      official_decision: normalizeText(officialCorrelation.officialDecision),
      official_decision_code: normalizeText(officialCorrelation.officialDecisionCode),
      official_decision_timestamp: normalizeText(officialCorrelation.officialDecisionTimestamp),
      complete: Boolean(officialCorrelation.isComplete),
      issues: safeArray(effectiveOfficialGate.correlationIssues).map(issue => normalizeText(issue)),
    },
    official_artifacts: {
      coverage_status: normalizeText(effectiveOfficialGate.coverageStatus),
      coverage_percent: Number(effectiveOfficialGate.coveragePercent) || 0,
      ready_count: Number(effectiveOfficialGate.readyCount) || 0,
      partial_count: Number(effectiveOfficialGate.partialCount) || 0,
      pending_count: Number(effectiveOfficialGate.pendingCount) || 0,
      required_artifacts: safeArray(effectiveOfficialGate.requiredArtifacts).map(item =>
        normalizeText(item)
      ),
      required_artifacts_from_backend: safeArray(
        effectiveOfficialGate.requiredArtifactsFromBackend
      ).map(item => normalizeText(item)),
      available_artifacts_from_backend: safeArray(
        effectiveOfficialGate.availableArtifactsFromBackend
      ).map(item => normalizeText(item)),
      missing_required_artifacts: safeArray(
        effectiveOfficialGate.missingRequiredArtifacts
      ).map(item => normalizeText(item)),
      inconsistent_artifacts: safeArray(effectiveOfficialGate.inconsistentArtifacts).map(item =>
        normalizeText(item)
      ),
      issues: safeArray(effectiveOfficialGate.gateIssues).map(issue => normalizeText(issue)),
      rows: officialRows,
    },
    summary: {
      organizations: records.organizations.length,
      channels: records.channels.length,
      hosts: records.hosts.length,
      nodes: records.nodes.length,
      crypto_baseline: records.cryptoBaseline.length,
      scope_excluded_records: state.scopeEnforcement.excludedTotal,
    },
    records: {
      organizations: records.organizations.map(record => ({ ...record })),
      channels: records.channels.map(record => ({
        ...record,
        members: [...record.members],
      })),
      hosts: records.hosts.map(record => ({ ...record })),
      nodes: records.nodes.map(record => ({ ...record })),
      crypto_baseline: records.cryptoBaseline.map(record => ({ ...record })),
    },
  };

  const sanitizedPayload = sanitizeStructuredData(payload);
  return {
    ...sanitizedPayload,
    canonical_fingerprint: toCanonicalFingerprint(sanitizedPayload),
  };
};

export const downloadInventorySnapshotFile = (fileName, payload) => {
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

export const formatInventoryUtcTimestamp = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return '-';
  }
  return normalized.replace('T', ' ').replace('Z', ' UTC');
};
