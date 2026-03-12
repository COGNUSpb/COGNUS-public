import {
  RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS,
  RUNBOOK_BACKEND_STATES,
  RUNBOOK_CHECKPOINT_STATUS,
  RUNBOOK_EXECUTION_STATUS,
  RUNBOOK_PROVIDER_KEY,
  advanceRunbookExecution,
  buildRunbookOfficialSnapshot,
  buildRunbookProducedEvidenceRows,
  buildRunbookResumeContext,
  buildRunbookStageRows,
  calculateRunbookProgress,
  createRunbookExecutionState,
  deterministicRunId,
  evaluateRunbookStartPreconditions,
  failRunbookExecution,
  getLatestRunbookFailure,
  isBackendCommandReady,
  pauseRunbookExecution,
  recordRunbookBlockingEvent,
  recordRunbookPreconditionEvents,
  resumeRunbookExecution,
  retryRunbookExecutionFromCheckpoint,
  resolveRunbookFailureDiagnostic,
  startRunbookExecution,
} from './provisioningRunbookUtils';

const VALID_START_PARAMS = {
  changeId: 'cr-2026-02-16-010',
  runId: 'run-2026-02-16-a2-010',
  manifestFingerprint: '40ff5d1f8d9dcef0ea0909f4df33768eca98f6c901d37bf80f32472ca03bb6cb',
  sourceBlueprintFingerprint: 'c4b5f166a654f83503de150c89fdd2f4475fd843f7d16d22fe4dc0d5e0c770ce',
  providerKey: RUNBOOK_PROVIDER_KEY,
  backendState: RUNBOOK_BACKEND_STATES.ready,
  blueprintVersion: '1.2.0',
  blueprintFingerprint: 'c4b5f166a654f83503de150c89fdd2f4475fd843f7d16d22fe4dc0d5e0c770ce',
  resolvedSchemaVersion: '1.0.0',
  blueprintValidated: true,
  pipelinePreconditionsReady: true,
  preflightApproved: true,
  requiredArtifacts: [...RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS],
  availableArtifacts: [
    'provision-plan.json',
    'reconcile-report.json',
    'inventory-final.json',
    'stage-reports/prepare-report.json',
    'verify-report.json',
    'ssh-execution-log.json',
  ],
  requireOfficialExecutionContext: true,
  environmentProfile: 'dev-external-linux',
};

describe('provisioningRunbookUtils', () => {
  it('blocks start when provider/backend/blueprint/pipeline preconditions are invalid', () => {
    const result = evaluateRunbookStartPreconditions({
      providerKey: 'aws',
      backendState: RUNBOOK_BACKEND_STATES.pending,
      blueprintVersion: '',
      blueprintValidated: false,
      pipelinePreconditionsReady: false,
      preflightApproved: false,
      changeId: '',
    });

    const reasonCodes = result.reasons.map(reason => reason.code);

    expect(result.allowStart).toBe(false);
    expect(reasonCodes).toContain('runbook_provider_not_supported');
    expect(reasonCodes).toContain('runbook_backend_pending');
    expect(reasonCodes).toContain('runbook_blueprint_version_required');
    expect(reasonCodes).toContain('runbook_blueprint_not_validated');
    expect(reasonCodes).toContain('runbook_pipeline_preconditions_missing');
    expect(reasonCodes).toContain('runbook_preflight_not_approved');
    expect(reasonCodes).toContain('runbook_change_id_required');
  });

  it('enforces official A2.5 context and A2.2 artifact gate when strict mode is enabled', () => {
    const result = evaluateRunbookStartPreconditions({
      providerKey: RUNBOOK_PROVIDER_KEY,
      backendState: RUNBOOK_BACKEND_STATES.ready,
      blueprintVersion: '1.2.0',
      blueprintValidated: true,
      pipelinePreconditionsReady: true,
      preflightApproved: true,
      changeId: 'cr-2026-02-16-ctx-001',
      runId: '',
      manifestFingerprint: '',
      sourceBlueprintFingerprint: '',
      requiredArtifacts: [...RUNBOOK_A2_2_MINIMUM_ARTIFACT_KEYS],
      availableArtifacts: ['inventory-final.json'],
      requireOfficialExecutionContext: true,
    });

    const reasonCodes = result.reasons.map(reason => reason.code);

    expect(result.allowStart).toBe(false);
    expect(reasonCodes).toContain('runbook_run_id_required');
    expect(reasonCodes).toContain('runbook_manifest_fingerprint_required');
    expect(reasonCodes).toContain('runbook_source_blueprint_fingerprint_required');
    expect(reasonCodes).toContain('runbook_a2_2_artifacts_missing');
  });

  it('produces deterministic run_id for same execution inputs', () => {
    const first = deterministicRunId({
      changeId: 'cr-2026-02-16-011',
      blueprintFingerprint: '8f2f0c3edca9f79c7f9f8f0013c6465f5f9588fc3412d47a3d8dea49c4e8a123',
      resolvedSchemaVersion: '1.0.0',
    });
    const second = deterministicRunId({
      changeId: 'cr-2026-02-16-011',
      blueprintFingerprint: '8f2f0c3edca9f79c7f9f8f0013c6465f5f9588fc3412d47a3d8dea49c4e8a123',
      resolvedSchemaVersion: '1.0.0',
    });

    expect(first).toBe(second);
    expect(first.startsWith('run-')).toBe(true);
    expect(first).toHaveLength(28);
  });

  it('starts runbook in running state with first stage/checkpoint active', () => {
    const initial = createRunbookExecutionState();
    const started = startRunbookExecution(initial, VALID_START_PARAMS);

    expect(started.started).toBe(true);
    expect(started.nextState.status).toBe(RUNBOOK_EXECUTION_STATUS.running);
    expect(started.nextState.runId.startsWith('run-')).toBe(true);
    expect(started.nextState.stages[0].status).toBe(RUNBOOK_CHECKPOINT_STATUS.running);
    expect(started.nextState.stages[0].checkpoints[0].status).toBe(
      RUNBOOK_CHECKPOINT_STATUS.running
    );
  });

  it('supports pause and resume while preserving active checkpoint', () => {
    const started = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS);
    const paused = pauseRunbookExecution(started.nextState);
    const resumed = resumeRunbookExecution(paused.nextState);

    expect(paused.paused).toBe(true);
    expect(paused.nextState.status).toBe(RUNBOOK_EXECUTION_STATUS.paused);
    expect(paused.nextState.stages[0].checkpoints[0].status).toBe(RUNBOOK_CHECKPOINT_STATUS.paused);

    expect(resumed.resumed).toBe(true);
    expect(resumed.nextState.status).toBe(RUNBOOK_EXECUTION_STATUS.running);
    expect(resumed.nextState.stages[0].checkpoints[0].status).toBe(
      RUNBOOK_CHECKPOINT_STATUS.running
    );
  });

  it('advances checkpoints deterministically until completed with progress 100%', () => {
    let state = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS).nextState;
    const totalCheckpoints = state.stages.reduce(
      (accumulator, stage) => accumulator + stage.checkpoints.length,
      0
    );

    for (let index = 0; index < totalCheckpoints; index += 1) {
      const advanced = advanceRunbookExecution(state);
      expect(advanced.advanced).toBe(true);
      state = advanced.nextState;
    }

    expect(state.status).toBe(RUNBOOK_EXECUTION_STATUS.completed);
    expect(calculateRunbookProgress(state)).toBe(100);
  });

  it('fails current checkpoint and retries safely from last completed checkpoint', () => {
    let state = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS).nextState;

    state = advanceRunbookExecution(state).nextState;
    const failed = failRunbookExecution(state, {
      code: 'prepare_ssh_access_failed',
      message: 'Host sem acesso SSH para a etapa prepare.',
    });
    const retried = retryRunbookExecutionFromCheckpoint(failed.nextState);

    expect(failed.failed).toBe(true);
    expect(failed.nextState.status).toBe(RUNBOOK_EXECUTION_STATUS.failed);
    expect(retried.retried).toBe(true);
    expect(retried.nextState.status).toBe(RUNBOOK_EXECUTION_STATUS.running);
    expect(retried.nextState.stages[0].checkpoints[0].status).toBe(
      RUNBOOK_CHECKPOINT_STATUS.completed
    );
    expect(retried.nextState.stages[0].checkpoints[1].status).toBe(
      RUNBOOK_CHECKPOINT_STATUS.running
    );
  });

  it('builds ordered stage rows for execution monitoring table', () => {
    const started = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS);
    const rows = buildRunbookStageRows(started.nextState);

    expect(rows).toHaveLength(4);
    expect(rows[0].stageKey).toBe('prepare');
    expect(rows[1].stageKey).toBe('provision');
    expect(rows[2].stageKey).toBe('reconcile');
    expect(rows[3].stageKey).toBe('verify');
    expect(rows[0].isCurrentStage).toBe(true);
  });

  it('treats pending/invalid backend as command-blocking states', () => {
    expect(isBackendCommandReady(RUNBOOK_BACKEND_STATES.ready)).toBe(true);
    expect(isBackendCommandReady(RUNBOOK_BACKEND_STATES.pending)).toBe(false);
    expect(isBackendCommandReady(RUNBOOK_BACKEND_STATES.invalid)).toBe(false);
  });

  it('resolves failure diagnostics with classification, cause, impact and recommended action', () => {
    const diagnostic = resolveRunbookFailureDiagnostic({
      code: 'runbook_command_blocked_backend_pending',
    });
    const sshDiagnostic = resolveRunbookFailureDiagnostic({
      code: 'runbook_ssh_execution_failed',
    });

    expect(diagnostic.code).toBe('runbook_command_blocked_backend_pending');
    expect(diagnostic.classification).toBe('transient');
    expect(diagnostic.cause).toContain('pending');
    expect(diagnostic.impact).toContain('impactar');
    expect(diagnostic.recommendedAction).toContain('ready');

    expect(sshDiagnostic.code).toBe('runbook_ssh_execution_failed');
    expect(sshDiagnostic.classification).toBe('critical');
    expect(sshDiagnostic.cause).toContain('SSH');
    expect(sshDiagnostic.impact).toContain('Bloqueia');
    expect(sshDiagnostic.recommendedAction).toContain('known_hosts');
  });

  it('stores diagnostic metadata on failed stage/checkpoint and timeline event', () => {
    let state = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS).nextState;

    state = advanceRunbookExecution(state).nextState;
    const failed = failRunbookExecution(state, {
      code: 'prepare_ssh_access_failed',
    });
    const lastEvent = failed.nextState.events[failed.nextState.events.length - 1];

    expect(failed.nextState.stages[0].failure.classification).toBe('critical');
    expect(failed.nextState.stages[0].failure.component).toContain('prepare');
    expect(failed.nextState.stages[0].failure.cause).toContain('SSH');
    expect(failed.nextState.stages[0].failure.impact).toContain('Bloqueia');
    expect(failed.nextState.stages[0].checkpoints[1].failure.recommendedAction).toContain('SSH');
    expect(lastEvent.code).toBe('prepare_ssh_access_failed');
    expect(lastEvent.classification).toBe('critical');
    expect(lastEvent.component).toContain('prepare');
    expect(lastEvent.cause).toContain('SSH');
    expect(lastEvent.impact).toContain('Bloqueia');
  });

  it('records blocked commands with correlation ids in timeline', () => {
    const blocked = recordRunbookBlockingEvent(
      createRunbookExecutionState({
        runId: 'run-000001',
        changeId: 'cr-2026-02-16-999',
        backendState: RUNBOOK_BACKEND_STATES.ready,
      }),
      {
        code: 'runbook_command_blocked_backend_pending',
        message: 'Backend pendente para comandos de execução.',
        stage: 'prepare',
        checkpoint: 'validate_blueprint_gate',
        backendState: RUNBOOK_BACKEND_STATES.pending,
      }
    );

    expect(blocked.events).toHaveLength(1);
    expect(blocked.events[0].level).toBe('warning');
    expect(blocked.events[0].classification).toBe('transient');
    expect(blocked.events[0].component).toBe('prepare::validate_blueprint_gate');
    expect(blocked.events[0].impact).toContain('impactar');
    expect(blocked.events[0].runId).toBe('run-000001');
    expect(blocked.events[0].changeId).toBe('cr-2026-02-16-999');
    expect(blocked.events[0].backendState).toBe(RUNBOOK_BACKEND_STATES.pending);
  });

  it('persists all failed preconditions as blocking events', () => {
    const withPreconditions = recordRunbookPreconditionEvents(
      createRunbookExecutionState(),
      [
        {
          code: 'runbook_blueprint_not_validated',
          path: 'blueprint_validation',
          message: 'Blueprint sem validação A1.2.',
        },
        {
          code: 'runbook_pipeline_preconditions_missing',
          path: 'pipeline_preconditions',
          message: 'Pré-condições mínimas do pipeline ausentes.',
        },
      ],
      {
        changeId: 'cr-2026-02-16-123',
      }
    );

    expect(withPreconditions.events).toHaveLength(2);
    expect(withPreconditions.events[0].path).toBe('blueprint_validation');
    expect(withPreconditions.events[1].path).toBe('pipeline_preconditions');
    expect(withPreconditions.events[0].classification).toBe('critical');
    expect(withPreconditions.events[1].changeId).toBe('cr-2026-02-16-123');
  });

  it('builds produced evidence rows and resume context from latest valid checkpoint', () => {
    let state = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS).nextState;

    state = advanceRunbookExecution(state).nextState;
    const evidenceRows = buildRunbookProducedEvidenceRows(state);
    const failed = failRunbookExecution(state, {
      code: 'prepare_ssh_access_failed',
    });
    const resumeContext = buildRunbookResumeContext(failed.nextState);

    expect(evidenceRows).toHaveLength(1);
    expect(evidenceRows[0].stageKey).toBe('prepare');
    expect(evidenceRows[0].checkpointProgress).toBe('1/3');
    expect(resumeContext.lastCompletedCheckpointOrder).toBe(1);
    expect(resumeContext.resumeCheckpointOrder).toBe(2);
    expect(resumeContext.evidenceCount).toBeGreaterThan(0);
  });

  it('flags coherence issues when backend official status diverges from active execution', () => {
    const started = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS);
    const snapshot = buildRunbookOfficialSnapshot(started.nextState, {
      backendState: RUNBOOK_BACKEND_STATES.pending,
    });
    const issueCodes = snapshot.coherence.issues.map(issue => issue.code);

    expect(snapshot.pipelineStatus).toBe(RUNBOOK_EXECUTION_STATUS.running);
    expect(snapshot.coherence.isConsistent).toBe(false);
    expect(snapshot.coherence.status).toBe('attention');
    expect(issueCodes).toContain('runbook_command_blocked_backend_pending');
  });

  it('propagates official allow/block decision into snapshot payload', () => {
    const started = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS);
    const blockedSnapshot = buildRunbookOfficialSnapshot(
      {
        ...started.nextState,
        officialDecision: {
          decision: 'block',
          decision_code: 'BLOCK_MINIMUM_EVIDENCE_MISSING',
          decision_reasons: ['minimum_evidence_missing'],
          status: 'failed',
          required_evidence_keys: ['history:history.jsonl'],
          missing_evidence_keys: ['history:history.jsonl'],
          evidence_minimum_valid: false,
          timestamp_utc: '2026-02-20T10:00:00Z',
        },
      },
      {
        backendState: RUNBOOK_BACKEND_STATES.ready,
      }
    );
    const allowedSnapshot = buildRunbookOfficialSnapshot(
      {
        ...started.nextState,
        status: RUNBOOK_EXECUTION_STATUS.completed,
        finishedAt: '2026-02-20T10:10:00Z',
        officialDecision: {
          decision: 'allow',
          decision_code: 'ALLOW_COMPLETED_WITH_MIN_EVIDENCE',
          decision_reasons: [],
          status: 'completed',
          required_evidence_keys: ['history:history.jsonl'],
          missing_evidence_keys: [],
          evidence_minimum_valid: true,
          timestamp_utc: '2026-02-20T10:10:00Z',
        },
      },
      {
        backendState: RUNBOOK_BACKEND_STATES.ready,
      }
    );

    expect(blockedSnapshot.officialDecision.decision).toBe('block');
    expect(blockedSnapshot.officialDecision.decisionCode).toBe('BLOCK_MINIMUM_EVIDENCE_MISSING');
    expect(blockedSnapshot.officialDecision.decisionReasons).toContain('minimum_evidence_missing');
    expect(allowedSnapshot.officialDecision.decision).toBe('allow');
    expect(allowedSnapshot.officialDecision.decisionCode).toBe('ALLOW_COMPLETED_WITH_MIN_EVIDENCE');
    expect(allowedSnapshot.officialDecision.evidenceMinimumValid).toBe(true);
  });

  it('returns latest diagnostic failure for troubleshooting panel', () => {
    let state = startRunbookExecution(createRunbookExecutionState(), VALID_START_PARAMS).nextState;

    state = recordRunbookBlockingEvent(state, {
      code: 'runbook_command_blocked_backend_pending',
      message: 'Backend pendente para execução.',
      backendState: RUNBOOK_BACKEND_STATES.pending,
    });

    state = failRunbookExecution(state, {
      code: 'prepare_ssh_access_failed',
    }).nextState;

    const latestFailure = getLatestRunbookFailure(state);

    expect(latestFailure).not.toBeNull();
    expect(latestFailure.code).toBe('prepare_ssh_access_failed');
    expect(latestFailure.classification).toBe('critical');
    expect(latestFailure.component).toContain('prepare');
    expect(latestFailure.impact).toContain('Bloqueia');
    expect(latestFailure.runId).toBe(state.runId);
  });
});
