import {
  EVIDENCE_STATUS,
  buildEvidenceExportPayload,
  buildExecutionEvidenceRows,
  createImmutableEvidenceBundle,
  evaluateExecutionEvidenceGate,
  formatEvidenceUtcTimestamp,
  truncateEvidenceFingerprint,
} from './provisioningEvidenceUtils';
import {
  createInventorySnapshotState,
  synchronizeInventorySnapshot,
} from './provisioningInventoryUtils';

describe('provisioningEvidenceUtils', () => {
  const buildOfficialRun = () => ({
    run_id: 'run-2026-e1-000',
    change_id: 'cr-2026-02-16-000',
    blueprint_fingerprint: '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
    updated_at_utc: '2026-02-16T16:00:00Z',
    stages: [{ key: 'prepare' }, { key: 'provision' }, { key: 'reconcile' }, { key: 'verify' }],
    artifact_rows: [
      {
        key: 'stage-reports/prepare-report.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: 'a'.repeat(64),
      },
      {
        key: 'stage-reports/provision-report.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: 'b'.repeat(64),
      },
      {
        key: 'stage-reports/reconcile-report.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: 'c'.repeat(64),
      },
      {
        key: 'stage-reports/verify-report.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: 'd'.repeat(64),
      },
      {
        key: 'pipeline-report.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: 'e'.repeat(64),
      },
      {
        key: 'inventory-final.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: 'f'.repeat(64),
      },
      {
        key: 'reconcile-report.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: '9'.repeat(64),
      },
      {
        key: 'ssh-execution-log.json',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: '8'.repeat(64),
      },
      {
        key: 'history.jsonl',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: '1'.repeat(64),
      },
      {
        key: 'decision-trace.jsonl',
        available: true,
        validated_at_utc: '2026-02-16T16:00:00Z',
        fingerprint_sha256: '2'.repeat(64),
      },
    ],
  });

  it('builds correlated evidence rows for blueprint, runbook and inventory', () => {
    const rows = buildExecutionEvidenceRows({
      changeId: 'cr-2026-02-16-050',
      runId: 'run-2026-e1-050',
      executor: 'ops.cognus@ufg.br',
      snapshot: createInventorySnapshotState(),
    });

    expect(rows).toHaveLength(9);
    expect(rows[0].key).toBe('blueprint_fingerprint');
    expect(rows[0].status).toBe(EVIDENCE_STATUS.ready);
    expect(rows[0].changeId).toBe('cr-2026-02-16-050');
    expect(rows[0].runId).toBe('run-2026-e1-050');
    expect(rows[1].key).toBe('stage_reports');
    expect(rows[1].status).toBe(EVIDENCE_STATUS.missing);
  });

  it('blocks evidence gate when mandatory evidence is missing or incomplete', () => {
    const rows = buildExecutionEvidenceRows({
      changeId: 'cr-2026-02-16-051',
      runId: 'run-2026-e1-051',
      snapshot: createInventorySnapshotState(),
    });
    const gate = evaluateExecutionEvidenceGate({
      evidenceRows: rows,
      changeId: 'cr-2026-02-16-051',
      runId: 'run-2026-e1-051',
    });

    expect(gate.allowCloseExecution).toBe(false);
    expect(gate.allowExport).toBe(false);
    expect(gate.missingRequiredKeys).toContain('stage_reports');
    expect(gate.missingRequiredKeys).toContain('reconcile_report');
    expect(gate.missingRequiredKeys).toContain('inventory_final');
    expect(gate.missingRequiredKeys).toContain('verify_report');
    expect(gate.missingRequiredKeys).toContain('ssh_execution_log');
  });

  it('allows close/export when official run provides all mandatory evidence', () => {
    const fullSnapshot = synchronizeInventorySnapshot(
      synchronizeInventorySnapshot(createInventorySnapshotState(), {
        changeId: 'cr-2026-02-16-052',
        runId: 'run-2026-e1-052',
      })
    );
    const rows = buildExecutionEvidenceRows({
      changeId: 'cr-2026-02-16-052',
      runId: 'run-2026-e1-052',
      snapshot: fullSnapshot,
      officialRun: {
        ...buildOfficialRun(),
        run_id: 'run-2026-e1-052',
        change_id: 'cr-2026-02-16-052',
      },
    });
    const gate = evaluateExecutionEvidenceGate({
      evidenceRows: rows,
      changeId: 'cr-2026-02-16-052',
      runId: 'run-2026-e1-052',
    });

    expect(gate.allowCloseExecution).toBe(true);
    expect(gate.allowExport).toBe(true);
    expect(gate.missingRequiredKeys).toHaveLength(0);
  });

  it('creates immutable evidence bundle for post-execution view', () => {
    const fullSnapshot = synchronizeInventorySnapshot(
      synchronizeInventorySnapshot(createInventorySnapshotState(), {
        changeId: 'cr-2026-02-16-053',
        runId: 'run-2026-e1-053',
      })
    );
    const rows = buildExecutionEvidenceRows({
      changeId: 'cr-2026-02-16-053',
      runId: 'run-2026-e1-053',
      snapshot: fullSnapshot,
      officialRun: {
        ...buildOfficialRun(),
        run_id: 'run-2026-e1-053',
        change_id: 'cr-2026-02-16-053',
      },
    });
    const bundle = createImmutableEvidenceBundle({
      evidenceRows: rows,
      changeId: 'cr-2026-02-16-053',
      runId: 'run-2026-e1-053',
      executor: 'ops.cognus@ufg.br',
    });

    expect(bundle.immutable).toBe(true);
    expect(bundle.gate.allowCloseExecution).toBe(true);
    expect(Object.isFrozen(bundle)).toBe(true);
    expect(Object.isFrozen(bundle.rows)).toBe(true);
  });

  it('builds evidence export payload with canonical fingerprint and gate metadata', () => {
    const fullSnapshot = synchronizeInventorySnapshot(
      synchronizeInventorySnapshot(createInventorySnapshotState(), {
        changeId: 'cr-2026-02-16-054',
        runId: 'run-2026-e1-054',
      })
    );
    const rows = buildExecutionEvidenceRows({
      changeId: 'cr-2026-02-16-054',
      runId: 'run-2026-e1-054',
      snapshot: fullSnapshot,
      officialRun: {
        ...buildOfficialRun(),
        run_id: 'run-2026-e1-054',
        change_id: 'cr-2026-02-16-054',
      },
    });
    const bundle = createImmutableEvidenceBundle({
      evidenceRows: rows,
      changeId: 'cr-2026-02-16-054',
      runId: 'run-2026-e1-054',
      executor: 'ops.cognus@ufg.br',
      lockedAtUtc: '2026-02-16T16:00:00Z',
    });
    const payload = buildEvidenceExportPayload({
      evidenceBundle: bundle,
      exportedAtUtc: '2026-02-16T16:10:00Z',
    });

    expect(payload.metadata.change_id).toBe('cr-2026-02-16-054');
    expect(payload.metadata.run_id).toBe('run-2026-e1-054');
    expect(payload.metadata.immutable).toBe(true);
    expect(payload.gate.allow_close_execution).toBe(true);
    expect(payload.evidences).toHaveLength(9);
    expect(payload.canonical_fingerprint).toMatch(/^[0-9a-f]{16}$/);
  });

  it('sanitizes sensitive fields in evidence export payload', () => {
    const payload = buildEvidenceExportPayload({
      evidenceRows: [
        {
          key: 'ssh_execution_log',
          source: 'runbook',
          artifact: 'local-file:web3-livre.pem',
          fingerprint: 'ab'.repeat(32),
          status: EVIDENCE_STATUS.ready,
          timestampUtc: '2026-02-16T16:00:00Z',
          executor: 'ops.cognus@ufg.br',
          changeId: 'cr-2026-02-16-055',
          runId: 'run-2026-e1-055',
          required: true,
          details: 'password=admin123 token=abc123',
        },
      ],
      exportedAtUtc: '2026-02-16T16:10:00Z',
    });

    expect(payload.evidences[0].artifact).toContain('local-file:[REDACTED_REF:');
    expect(payload.evidences[0].details).toContain('[REDACTED_REF:password:');
    expect(payload.evidences[0].details).toContain('[REDACTED_REF:token:');
    expect(payload.evidences[0].details).not.toContain('admin123');
    expect(payload.evidences[0].details).not.toContain('abc123');
  });

  it('formats and truncates evidence values for UI rendering', () => {
    expect(formatEvidenceUtcTimestamp('2026-02-16T16:20:00Z')).toBe('2026-02-16 16:20:00 UTC');
    expect(formatEvidenceUtcTimestamp('')).toBe('-');
    expect(
      truncateEvidenceFingerprint(
        '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de'
      )
    ).toBe('97db1fa62b...8cd0de');
  });
});
