import {
  INVENTORY_PROVIDER_SCOPE,
  INVENTORY_SOURCE_STATUS,
  buildInventoryExportPayload,
  buildInventoryFilterOptions,
  createInventorySnapshotState,
  evaluateOfficialInventoryEvidenceGate,
  filterInventorySnapshot,
  formatInventoryUtcTimestamp,
  synchronizeInventorySnapshot,
} from './provisioningInventoryUtils';

describe('provisioningInventoryUtils', () => {
  const buildOfficialRun = (overrides = {}) => ({
    run_id: 'run-inventory-04',
    change_id: 'cr-2026-02-16-033',
    manifest_fingerprint: 'ab'.repeat(32),
    source_blueprint_fingerprint: 'cd'.repeat(32),
    blueprint_fingerprint: 'ef'.repeat(32),
    backend_state: 'ready',
    status: 'completed',
    stages: [{ key: 'prepare' }, { key: 'provision' }, { key: 'reconcile' }, { key: 'verify' }],
    a2_2_minimum_artifacts: [
      'reconcile-report',
      'inventory-final',
      'verify-report',
      'ssh-execution-log',
      'stage-reports',
    ],
    a2_2_available_artifacts: [
      'reconcile-report',
      'inventory-final',
      'verify-report',
      'ssh-execution-log',
      'stage-reports',
    ],
    artifact_rows: [
      {
        key: 'stage-reports/prepare-report.json',
        artifact_key: 'stage-reports:prepare-report.json',
        available: true,
        validated_at_utc: '2026-02-16T14:05:00Z',
        fingerprint_sha256: 'a'.repeat(64),
      },
      {
        key: 'stage-reports/provision-report.json',
        artifact_key: 'stage-reports:provision-report.json',
        available: true,
        validated_at_utc: '2026-02-16T14:06:00Z',
        fingerprint_sha256: 'b'.repeat(64),
      },
      {
        key: 'stage-reports/reconcile-report.json',
        artifact_key: 'stage-reports:reconcile-report.json',
        available: true,
        validated_at_utc: '2026-02-16T14:07:00Z',
        fingerprint_sha256: 'c'.repeat(64),
      },
      {
        key: 'stage-reports/verify-report.json',
        artifact_key: 'stage-reports:verify-report.json',
        available: true,
        validated_at_utc: '2026-02-16T14:08:00Z',
        fingerprint_sha256: 'd'.repeat(64),
      },
      {
        key: 'inventory-final.json',
        artifact_key: 'inventory-final:inventory-final.json',
        available: true,
        validated_at_utc: '2026-02-16T14:09:00Z',
        fingerprint_sha256: 'e'.repeat(64),
      },
      {
        key: 'ssh-execution-log.json',
        artifact_key: 'ssh-execution-log:ssh-execution-log.json',
        available: true,
        validated_at_utc: '2026-02-16T14:10:00Z',
        fingerprint_sha256: 'f'.repeat(64),
      },
    ],
    official_decision: {
      decision: 'allow',
      decision_code: 'ALLOW_COMPLETED_WITH_MIN_EVIDENCE',
      timestamp_utc: '2026-02-16T14:11:00Z',
    },
    ...overrides,
  });

  it('creates unsynced snapshot with pending coverage and scope filtering applied', () => {
    const snapshot = createInventorySnapshotState({
      changeId: 'cr-2026-02-16-030',
      runId: 'run-inventory-01',
    });

    expect(snapshot.syncPhase).toBe('unsynced');
    expect(snapshot.coverageStatus).toBe(INVENTORY_SOURCE_STATUS.pending);
    expect(snapshot.syncedAt).toBe('');
    expect(snapshot.scopeEnforcement.excludedTotal).toBeGreaterThan(0);
    snapshot.records.hosts.forEach(host => {
      expect(host.providerKey).toBe(INVENTORY_PROVIDER_SCOPE);
    });
    snapshot.records.nodes.forEach(node => {
      expect(node.providerKey).toBe(INVENTORY_PROVIDER_SCOPE);
    });
  });

  it('first manual synchronization yields partial coverage with missing crypto artifact', () => {
    const initial = createInventorySnapshotState();
    const synced = synchronizeInventorySnapshot(initial, {
      timestamp: '2026-02-16T12:00:00Z',
      changeId: 'cr-2026-02-16-031',
      runId: 'run-inventory-02',
    });

    expect(synced.syncCount).toBe(1);
    expect(synced.syncPhase).toBe('partial');
    expect(synced.coverageStatus).toBe(INVENTORY_SOURCE_STATUS.partial);
    expect(synced.consistency.isConsistent).toBe(false);
    expect(synced.consistency.missingArtifacts).toContain('reconcile-report.json');
    expect(synced.consistency.missingArtifacts).toContain('verify-report.json');
    expect(synced.consistency.missingArtifacts).toContain('ssh-execution-log.json');
    expect(synced.changeId).toBe('cr-2026-02-16-031');
    expect(synced.runId).toBe('run-inventory-02');
  });

  it('second manual synchronization yields full coverage and consistent snapshot', () => {
    const firstSync = synchronizeInventorySnapshot(createInventorySnapshotState(), {
      timestamp: '2026-02-16T12:10:00Z',
      changeId: 'cr-2026-02-16-032',
      runId: 'run-inventory-03',
    });
    const secondSync = synchronizeInventorySnapshot(firstSync, {
      timestamp: '2026-02-16T12:20:00Z',
    });

    expect(secondSync.syncCount).toBe(2);
    expect(secondSync.syncPhase).toBe('full');
    expect(secondSync.coverageStatus).toBe(INVENTORY_SOURCE_STATUS.ready);
    expect(secondSync.consistency.isConsistent).toBe(true);
    expect(secondSync.consistency.missingArtifacts).toHaveLength(0);
    expect(secondSync.records.cryptoBaseline.length).toBeGreaterThan(2);
  });

  it('builds filter options for organization/channel/environment', () => {
    const fullSnapshot = synchronizeInventorySnapshot(
      synchronizeInventorySnapshot(createInventorySnapshotState())
    );
    const options = buildInventoryFilterOptions(fullSnapshot);

    expect(options.organizations).toContain('infufg');
    expect(options.channels).toContain('ops-channel-dev');
    expect(options.environments).toContain('dev-external-linux');
  });

  it('filters records by organization, channel and environment', () => {
    const fullSnapshot = synchronizeInventorySnapshot(
      synchronizeInventorySnapshot(createInventorySnapshotState())
    );
    const filtered = filterInventorySnapshot(fullSnapshot, {
      organization: 'anatel',
      channel: 'ops-channel-dev',
      environment: 'dev-external-linux',
    });

    expect(filtered.organizations).toHaveLength(1);
    expect(filtered.organizations[0].orgId).toBe('anatel');
    expect(filtered.channels).toHaveLength(1);
    expect(filtered.channels[0].channelId).toBe('ops-channel-dev');
    filtered.hosts.forEach(host => {
      expect(host.orgId).toBe('anatel');
      expect(host.environment).toBe('dev-external-linux');
      expect(host.providerKey).toBe(INVENTORY_PROVIDER_SCOPE);
    });
    filtered.nodes.forEach(node => {
      expect(node.orgId).toBe('anatel');
      expect(node.environment).toBe('dev-external-linux');
      expect(node.providerKey).toBe(INVENTORY_PROVIDER_SCOPE);
    });
  });

  it('evaluates official A2 artifact gate and releases export/close when all evidences are consistent', () => {
    const gate = evaluateOfficialInventoryEvidenceGate({
      officialRun: buildOfficialRun(),
      changeId: 'cr-2026-02-16-033',
      runId: 'run-inventory-04',
    });

    expect(gate.allowExport).toBe(true);
    expect(gate.allowCloseExecution).toBe(true);
    expect(gate.coverageStatus).toBe(INVENTORY_SOURCE_STATUS.ready);
    expect(gate.missingRequiredArtifacts).toHaveLength(0);
    expect(gate.inconsistentArtifacts).toHaveLength(0);
    expect(gate.artifactRows.find(row => row.key === 'stage-reports').status).toBe(
      INVENTORY_SOURCE_STATUS.ready
    );
  });

  it('blocks official gate when correlation diverges or mandatory artifact evidence is inconsistent', () => {
    const gate = evaluateOfficialInventoryEvidenceGate({
      officialRun: buildOfficialRun({
        run_id: 'run-inventory-official-mismatch',
        manifest_fingerprint: '',
        source_blueprint_fingerprint: '',
        artifact_rows: [
          {
            key: 'stage-reports/reconcile-report.json',
            artifact_key: 'stage-reports:reconcile-report.json',
            available: true,
            validated_at_utc: '2026-02-16T14:07:00Z',
            fingerprint_sha256: 'c'.repeat(64),
          },
          {
            key: 'inventory-final.json',
            artifact_key: 'inventory-final:inventory-final.json',
            available: true,
            validated_at_utc: '2026-02-16T14:09:00Z',
            fingerprint_sha256: '',
          },
          {
            key: 'stage-reports/verify-report.json',
            artifact_key: 'stage-reports:verify-report.json',
            available: true,
            validated_at_utc: '',
            fingerprint_sha256: 'd'.repeat(64),
          },
        ],
      }),
      changeId: 'cr-2026-02-16-033',
      runId: 'run-inventory-04',
    });

    expect(gate.allowExport).toBe(false);
    expect(gate.allowCloseExecution).toBe(false);
    expect(gate.coverageStatus).toBe(INVENTORY_SOURCE_STATUS.partial);
    expect(gate.correlationIssues.some(issue => issue.includes('run_id divergente'))).toBe(true);
    expect(
      gate.correlationIssues.some(issue => issue.includes('manifest_fingerprint ausente'))
    ).toBe(true);
    expect(
      gate.correlationIssues.some(issue => issue.includes('source_blueprint_fingerprint ausente'))
    ).toBe(true);
    expect(gate.inconsistentArtifacts).toContain('inventory-final');
    expect(gate.inconsistentArtifacts).toContain('verify-report');
  });

  it('sanitizes sensitive diagnostics and decision metadata in official artifact gate output', () => {
    const gate = evaluateOfficialInventoryEvidenceGate({
      officialRun: buildOfficialRun({
        official_decision: {
          decision: 'block',
          decision_code: 'token=abc123',
          timestamp_utc: '2026-02-16T14:11:00Z',
        },
      }),
      changeId: 'cr-2026-02-16-033',
      runId: 'run-inventory-04',
    });

    expect(gate.correlation.officialDecisionCode).toContain('[REDACTED_REF:token:');
    expect(gate.correlation.officialDecisionCode).not.toContain('abc123');
  });

  it('builds export payload with correlation, consistency and canonical fingerprint', () => {
    const fullSnapshot = synchronizeInventorySnapshot(
      synchronizeInventorySnapshot(createInventorySnapshotState(), {
        changeId: 'cr-2026-02-16-033',
        runId: 'run-inventory-04',
      })
    );
    const filtered = filterInventorySnapshot(fullSnapshot, {
      organization: 'all',
      channel: 'all',
      environment: 'all',
    });
    const payload = buildInventoryExportPayload(fullSnapshot, filtered, {
      filters: {
        organization: 'all',
        channel: 'all',
        environment: 'all',
      },
      exportedAt: '2026-02-16T14:00:00Z',
      officialRun: buildOfficialRun(),
    });

    expect(payload.metadata.provider_scope).toBe(INVENTORY_PROVIDER_SCOPE);
    expect(payload.metadata.change_id).toBe('cr-2026-02-16-033');
    expect(payload.metadata.run_id).toBe('run-inventory-04');
    expect(payload.metadata.official_gate_allow_export).toBe(true);
    expect(payload.metadata.official_correlation_complete).toBe(true);
    expect(payload.consistency.is_consistent).toBe(true);
    expect(payload.official_correlation.manifest_fingerprint).toBe('ab'.repeat(32));
    expect(payload.official_correlation.source_blueprint_fingerprint).toBe('cd'.repeat(32));
    expect(payload.official_artifacts.coverage_status).toBe(INVENTORY_SOURCE_STATUS.ready);
    expect(payload.official_artifacts.missing_required_artifacts).toHaveLength(0);
    expect(payload.official_artifacts.inconsistent_artifacts).toHaveLength(0);
    expect(payload.summary.nodes).toBe(filtered.nodes.length);
    expect(payload.canonical_fingerprint).toMatch(/^[0-9a-f]{16}$/);
  });

  it('formats UTC timestamps for screen rendering', () => {
    expect(formatInventoryUtcTimestamp('2026-02-16T15:30:00Z')).toBe('2026-02-16 15:30:00 UTC');
    expect(formatInventoryUtcTimestamp('')).toBe('-');
  });
});
