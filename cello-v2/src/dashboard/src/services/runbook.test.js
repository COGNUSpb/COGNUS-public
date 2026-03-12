describe('services/runbook', () => {
  const originalLocalMode = process.env.COGNUS_RUNBOOK_LOCAL_MODE;
  const originalEnvProfile = process.env.COGNUS_ENV_PROFILE;

  const createRunResponse = run => ({
    status: 'successful',
    data: {
      run,
      snapshot: {},
    },
  });

  const createInspectionResponse = inspection => ({
    status: 'successful',
    data: {
      inspection,
    },
  });

  const buildValidRun = (overrides = {}) => ({
    run_id: 'run-2026-02-17-001',
    status: 'running',
    backend_state: 'ready',
    stages: [
      {
        key: 'prepare',
        label: 'prepare',
        status: 'running',
        checkpoints: [
          {
            key: 'prepare.preflight',
            label: 'preflight',
            status: 'running',
            started_at_utc: '2026-02-17T12:00:00Z',
            completed_at_utc: '',
          },
        ],
      },
    ],
    events: [
      {
        id: 'evt-001',
        timestamp_utc: '2026-02-17T12:00:00Z',
        level: 'info',
        code: 'runbook_started',
        message: 'Runbook iniciado.',
        run_id: 'run-2026-02-17-001',
        change_id: 'cr-2026-02-17-001',
      },
    ],
    ...overrides,
  });

  const loadService = async (localModeValue, requestResponseBuilder, envProfileValue) => {
    jest.resetModules();

    if (localModeValue === undefined) {
      delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;
    } else {
      process.env.COGNUS_RUNBOOK_LOCAL_MODE = localModeValue;
    }

    if (envProfileValue === undefined) {
      delete process.env.COGNUS_ENV_PROFILE;
    } else {
      process.env.COGNUS_ENV_PROFILE = envProfileValue;
    }

    const requestMock = jest
      .fn()
      .mockImplementation(() =>
        Promise.resolve(
          createRunResponse(requestResponseBuilder ? requestResponseBuilder() : buildValidRun())
        )
      );

    jest.doMock('../utils/request', () => requestMock);

    const service = await import('./runbook');
    return { service, requestMock };
  };

  afterEach(() => {
    jest.resetModules();
    jest.clearAllMocks();
    if (typeof window !== 'undefined' && window.localStorage) {
      window.localStorage.clear();
    }

    if (originalLocalMode === undefined) {
      delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;
    } else {
      process.env.COGNUS_RUNBOOK_LOCAL_MODE = originalLocalMode;
    }

    if (originalEnvProfile === undefined) {
      delete process.env.COGNUS_ENV_PROFILE;
    } else {
      process.env.COGNUS_ENV_PROFILE = originalEnvProfile;
    }
  });

  it('uses official runbook endpoints by default', async () => {
    const { service, requestMock } = await loadService(undefined);

    expect(service.RUNBOOK_LOCAL_MODE_ENABLED).toBe(false);

    await service.startRunbook({
      change_id: 'cr-2026-02-17-001',
      provider_key: 'external-linux',
      environment_profile: 'dev-external-linux',
      blueprint_version: '1.0.0',
    });

    expect(requestMock).toHaveBeenCalledWith(
      '/api/v1/runbooks/start',
      expect.objectContaining({ method: 'POST', skipErrorHandler: true })
    );
  });

  it('calls the official runtime inspection endpoint with canonical query parameters', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockResolvedValue(
      createInspectionResponse({
        contract_version: 'a2a-runtime-inspection-cache.v1',
        component: { component_id: 'org1-peer0' },
        scopes: {},
      })
    );

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    const inspection = await service.getRunbookRuntimeInspection('run-2026-02-17-001', {
      orgId: 'org1',
      hostId: 'host-a',
      componentId: 'org1-peer0',
      inspectionScope: 'all',
      refresh: true,
    });

    expect(inspection.component.component_id).toBe('org1-peer0');
    expect(requestMock).toHaveBeenCalledWith(
      '/api/v1/runbooks/run-2026-02-17-001/runtime-inspection?org_id=org1&host_id=host-a&component_id=org1-peer0&inspection_scope=all&refresh=true',
      expect.objectContaining({ skipErrorHandler: true })
    );
  });

  it('persists successful runtime inspection responses into the cached official run snapshot', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockImplementation(url => {
      if (url === '/api/v1/runbooks/run-2026-02-17-001/status') {
        return Promise.resolve(createRunResponse(buildValidRun({ status: 'running' })));
      }

      if (url.includes('/runtime-inspection')) {
        return Promise.resolve(
          createInspectionResponse({
            contract_version: 'a2a-runtime-inspection-cache.v1',
            correlation: {
              run_id: 'run-2026-02-17-001',
              change_id: 'cr-2026-02-17-001',
            },
            component: {
              component_id: 'peer:peer0-org1',
              org_id: 'org1',
              host_id: 'machine1',
            },
            inspection_scope: 'all',
            stale: false,
            scopes: {
              docker_inspect: {
                payload: {
                  container_name: 'cognusrb-run-peer0-org1-abcd1234',
                  image: 'hyperledger/fabric-peer:2.5',
                  state: { status: 'running' },
                },
                cache: {
                  cache_key: 'inspect-cache-key',
                  refreshed_at: '2026-02-17T12:10:00Z',
                  inspection_source: 'remote_docker_inspect',
                  collection_status: 'ready',
                  payload_hash: 'hash-inspect',
                },
              },
              docker_logs: {
                payload: {
                  container_name: 'cognusrb-run-peer0-org1-abcd1234',
                  logs: 'real peer log line',
                },
                cache: {
                  cache_key: 'logs-cache-key',
                  refreshed_at: '2026-02-17T12:10:02Z',
                  inspection_source: 'remote_docker_logs',
                  collection_status: 'ready',
                  payload_hash: 'hash-logs',
                },
              },
            },
          })
        );
      }

      return Promise.reject(new Error(`Unexpected request: ${url}`));
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await service.getRunbookStatus('run-2026-02-17-001');
    await service.getRunbookRuntimeInspection('run-2026-02-17-001', {
      orgId: 'org1',
      hostId: 'machine1',
      componentId: 'peer:peer0-org1',
      inspectionScope: 'all',
    });

    const cachedRegistry = JSON.parse(
      window.localStorage.getItem('cognus.runbooks.official.status.cache.v1') || '{}'
    );
    const cachedRun = cachedRegistry['run-2026-02-17-001'].run;

    expect(cachedRun.snapshot.runtime_inspection_cache['inspect-cache-key']).toEqual(
      expect.objectContaining({
        component_id: 'peer:peer0-org1',
        inspection_scope: 'docker_inspect',
      })
    );
    expect(cachedRun.snapshot.runtime_inspection_cache['logs-cache-key']).toEqual(
      expect.objectContaining({
        component_id: 'peer:peer0-org1',
        inspection_scope: 'docker_logs',
      })
    );
  });

  it('reuses the last cached official status snapshot when the backend loses the run state', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest
      .fn()
      .mockResolvedValueOnce(createRunResponse(buildValidRun({ status: 'completed' })))
      .mockRejectedValueOnce({
        status: 'fail',
        msg: 'runbook_execution_not_found',
        data: {
          code: 'runbook_execution_not_found',
          message: 'Execucao oficial nao encontrada no backend.',
        },
      });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    const firstStatus = await service.getRunbookStatus('run-2026-02-17-001');
    expect(firstStatus.run.run_id).toBe('run-2026-02-17-001');
    expect(firstStatus.run.cached_status_fallback).toBeUndefined();

    const fallbackStatus = await service.getRunbookStatus('run-2026-02-17-001');
    expect(fallbackStatus.run.run_id).toBe('run-2026-02-17-001');
    expect(fallbackStatus.run.cached_status_fallback).toEqual(
      expect.objectContaining({
        active: true,
        source: 'official_status_cache',
        reasonCode: 'runbook_execution_not_found',
      })
    );
    expect(fallbackStatus.executionState.runId).toBe('run-2026-02-17-001');
  });

  it('reuses cached runtime inspection payload when the backend no longer keeps the run state', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const cachedRun = buildValidRun({
      status: 'completed',
      change_id: 'cr-2026-02-17-001',
      snapshot: {
        runtime_inspection_cache: {
          inspect: {
            cache_key: 'inspect',
            org_id: 'org1',
            host_id: 'machine1',
            component_id: 'peer:peer0-org1',
            inspection_scope: 'docker_inspect',
            refreshed_at: '2026-02-17T12:10:00Z',
            inspection_source: 'remote_docker_inspect',
            collection_status: 'ready',
            payload_hash: 'hash-inspect',
            payload: {
              container_name: 'cognusrb-run-peer0-org1-abcd1234',
              image: 'hyperledger/fabric-peer:2.5',
              platform: 'docker/linux',
              state: {
                status: 'running',
                running: true,
                restart_count: 0,
                started_at: '2026-02-17T12:00:00Z',
              },
              env: ['CORE_PEER_ID=peer0-org1'],
              ports: [{ container_port: 7051, host_port: 17051, protocol: 'tcp' }],
              mounts: [{ destination: '/var/hyperledger/production' }],
              labels: {
                'cognus.run_id': 'run-2026-02-17-001',
              },
            },
            last_error: {},
          },
          logs: {
            cache_key: 'logs',
            org_id: 'org1',
            host_id: 'machine1',
            component_id: 'peer:peer0-org1',
            inspection_scope: 'docker_logs',
            refreshed_at: '2026-02-17T12:10:02Z',
            inspection_source: 'remote_docker_logs',
            collection_status: 'ready',
            payload_hash: 'hash-logs',
            payload: {
              container_name: 'cognusrb-run-peer0-org1-abcd1234',
              tail_lines: 50,
              line_count: 2,
              logs: '2026-02-17T12:09:58Z info chaincode ready\n2026-02-17T12:09:59Z info joined channel',
            },
            last_error: {},
          },
        },
      },
    });

    const requestMock = jest.fn().mockImplementation(url => {
      if (url === '/api/v1/runbooks/run-2026-02-17-001/status') {
        return Promise.resolve(createRunResponse(cachedRun));
      }

      if (url.includes('/runtime-inspection')) {
        return Promise.reject({
          status: 'fail',
          data: {
            code: 'runbook_not_found',
            message: 'Runbook nao encontrado para o run_id informado.',
          },
        });
      }

      return Promise.reject(new Error(`Unexpected request: ${url}`));
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await service.getRunbookStatus('run-2026-02-17-001');

    const inspection = await service.getRunbookRuntimeInspection('run-2026-02-17-001', {
      orgId: 'org1',
      hostId: 'machine1',
      componentId: 'peer:peer0-org1',
      inspectionScope: 'all',
    });

    expect(inspection.cached_status_fallback).toEqual(
      expect.objectContaining({
        active: true,
        source: 'official_status_cache',
        reasonCode: 'runbook_not_found',
      })
    );
    expect(inspection.component.container_name).toBe('cognusrb-run-peer0-org1-abcd1234');
    expect(inspection.scopes.docker_logs.payload.logs).toContain('joined channel');
    expect(inspection.scopes.docker_inspect.payload.state.status).toBe('running');
    expect(inspection.stale).toBe(true);
  });

  it('reuses cached runtime inspection payload even when the requested host id differs from the cached host alias', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const cachedRun = buildValidRun({
      status: 'completed',
      change_id: 'cr-2026-02-17-001',
      snapshot: {
        runtime_inspection_cache: {
          inspect: {
            cache_key: 'inspect',
            org_id: 'org1',
            host_id: 'machine1',
            component_id: 'peer:peer0-org1',
            inspection_scope: 'docker_inspect',
            refreshed_at: '2026-02-17T12:10:00Z',
            inspection_source: 'remote_docker_inspect',
            collection_status: 'ready',
            payload_hash: 'hash-inspect',
            payload: {
              container_name: 'cognusrb-run-peer0-org1-abcd1234',
              image: 'hyperledger/fabric-peer:2.5',
              platform: 'docker/linux',
              state: {
                status: 'running',
                running: true,
                restart_count: 0,
                started_at: '2026-02-17T12:00:00Z',
              },
            },
            last_error: {},
          },
          logs: {
            cache_key: 'logs',
            org_id: 'org1',
            host_id: 'machine1',
            component_id: 'peer:peer0-org1',
            inspection_scope: 'docker_logs',
            refreshed_at: '2026-02-17T12:10:02Z',
            inspection_source: 'remote_docker_logs',
            collection_status: 'ready',
            payload_hash: 'hash-logs',
            payload: {
              container_name: 'cognusrb-run-peer0-org1-abcd1234',
              logs: '2026-02-17T12:09:59Z info joined channel',
            },
            last_error: {},
          },
        },
      },
    });

    const requestMock = jest.fn().mockImplementation(url => {
      if (url === '/api/v1/runbooks/run-2026-02-17-001/status') {
        return Promise.resolve(createRunResponse(cachedRun));
      }

      if (url.includes('/runtime-inspection')) {
        return Promise.reject({
          status: 'fail',
          data: {
            code: 'runbook_not_found',
            message: 'Runbook nao encontrado para o run_id informado.',
          },
        });
      }

      return Promise.reject(new Error(`Unexpected request: ${url}`));
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await service.getRunbookStatus('run-2026-02-17-001');

    const inspection = await service.getRunbookRuntimeInspection('run-2026-02-17-001', {
      orgId: 'org1',
      hostId: '192.168.1.21',
      componentId: 'peer:peer0-org1',
      inspectionScope: 'all',
    });

    expect(inspection.component.container_name).toBe('cognusrb-run-peer0-org1-abcd1234');
    expect(inspection.component.host_id).toBe('machine1');
    expect(inspection.scopes.docker_logs.payload.logs).toContain('joined channel');
  });

  it('prewarms runtime inspection cache for official topology rows as soon as the run snapshot is available', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockImplementation(url => {
      if (url === '/api/v1/runbooks/run-2026-02-17-001/status') {
        return Promise.resolve(createRunResponse(buildValidRun({ status: 'running' })));
      }

      if (url.includes('/runtime-inspection')) {
        return Promise.resolve(
          createInspectionResponse({
            contract_version: 'a2a-runtime-inspection-cache.v1',
            correlation: {
              run_id: 'run-2026-02-17-001',
              change_id: 'cr-2026-02-17-001',
            },
            component: {
              component_id: 'peer:peer0-org1',
              org_id: 'org1',
              host_id: 'machine1',
            },
            inspection_scope: 'all',
            stale: false,
            scopes: {
              docker_inspect: {
                payload: {
                  container_name: 'cognusrb-run-peer0-org1-abcd1234',
                  image: 'hyperledger/fabric-peer:2.5',
                  state: { status: 'running' },
                },
                cache: {
                  cache_key: 'inspect-cache-key',
                  refreshed_at: '2026-02-17T12:10:00Z',
                  inspection_source: 'remote_docker_inspect',
                  collection_status: 'ready',
                  payload_hash: 'hash-inspect',
                },
              },
              docker_logs: {
                payload: {
                  container_name: 'cognusrb-run-peer0-org1-abcd1234',
                  logs: 'real peer log line',
                },
                cache: {
                  cache_key: 'logs-cache-key',
                  refreshed_at: '2026-02-17T12:10:02Z',
                  inspection_source: 'remote_docker_logs',
                  collection_status: 'ready',
                  payload_hash: 'hash-logs',
                },
              },
            },
          })
        );
      }

      return Promise.reject(new Error(`Unexpected request: ${url}`));
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await service.getRunbookStatus('run-2026-02-17-001');
    const summary = await service.prewarmRunbookRuntimeInspectionCache(
      'run-2026-02-17-001',
      [
        {
          componentId: 'peer:peer0-org1',
          organizationId: 'org1',
          hostRef: 'machine1',
          status: 'running',
          scope: 'required',
          criticality: 'critical',
        },
        {
          componentId: 'netapi:org1',
          organizationId: 'org1',
          hostRef: 'machine1',
          status: 'planned',
          scope: 'required',
          criticality: 'supporting',
        },
      ]
    );

    expect(summary).toEqual(
      expect.objectContaining({
        requested: 1,
        warmed: 1,
        failed: 0,
      })
    );

    const cachedRegistry = JSON.parse(
      window.localStorage.getItem('cognus.runbooks.official.status.cache.v1') || '{}'
    );
    const cachedRun = cachedRegistry['run-2026-02-17-001'].run;

    expect(cachedRun.snapshot.runtime_inspection_cache['inspect-cache-key']).toEqual(
      expect.objectContaining({
        component_id: 'peer:peer0-org1',
        inspection_scope: 'docker_inspect',
      })
    );
    expect(requestMock).toHaveBeenCalledWith(
      '/api/v1/runbooks/run-2026-02-17-001/runtime-inspection?org_id=org1&host_id=machine1&component_id=peer%3Apeer0-org1&inspection_scope=all',
      expect.objectContaining({ skipErrorHandler: true })
    );
  });

  it('rebuilds an audit-only run snapshot from persisted history when backend and cache no longer have the run', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    window.localStorage.setItem(
      'cognus.provisioning.runbook.audit.history.v2',
      JSON.stringify([
        {
          key: 'run-2026-02-17-009::2026-02-17T13:00:00Z',
          runId: 'run-2026-02-17-009',
          changeId: 'cr-2026-02-17-009',
          status: 'completed',
          startedAt: '2026-02-17T12:00:00Z',
          finishedAt: '2026-02-17T13:00:00Z',
          capturedAt: '2026-02-17T13:05:00Z',
          context: {
            providerKey: 'external-linux',
            environmentProfile: 'dev-external-linux',
            topology: {
              organizations: [
                {
                  orgId: 'org1',
                  orgName: 'Org One',
                  peers: [{ nodeId: 'peer0-org1', hostRef: 'host-a' }],
                  orderers: [{ nodeId: 'orderer0-org1', hostRef: 'host-a' }],
                  channels: ['channel1'],
                  chaincodes: ['asset_transfer'],
                },
              ],
            },
          },
          executionState: {
            runId: 'run-2026-02-17-009',
            changeId: 'cr-2026-02-17-009',
            status: 'completed',
            backendState: 'ready',
            startedAt: '2026-02-17T12:00:00Z',
            finishedAt: '2026-02-17T13:00:00Z',
            manifestFingerprint: 'aa'.repeat(32),
            sourceBlueprintFingerprint: 'bb'.repeat(32),
            providerKey: 'external-linux',
            environmentProfile: 'dev-external-linux',
            stages: [
              {
                key: 'verify',
                label: 'verify',
                status: 'completed',
                checkpoints: [
                  {
                    key: 'verify.consolidate',
                    label: 'consolidate',
                    status: 'completed',
                  },
                ],
              },
            ],
            events: [
              {
                id: 'evt-009',
                timestamp: '2026-02-17T13:00:00Z',
                level: 'info',
                code: 'runbook_completed',
                message: 'Runbook concluido.',
              },
            ],
            officialDecision: {
              decision: 'allow',
              decisionReasons: ['baseline convergida'],
            },
          },
        },
      ])
    );

    const requestMock = jest.fn().mockRejectedValue({
      status: 'fail',
      msg: 'runbook_execution_not_found',
      data: {
        code: 'runbook_execution_not_found',
        message: 'Execucao oficial nao encontrada no backend.',
      },
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    const fallbackStatus = await service.getRunbookStatus('run-2026-02-17-009');

    expect(fallbackStatus.run.run_id).toBe('run-2026-02-17-009');
    expect(fallbackStatus.run.topology_catalog.organizations[0].orgId).toBe('org1');
    expect(fallbackStatus.run.cached_status_fallback).toEqual(
      expect.objectContaining({
        active: true,
        source: 'audit_history_fallback',
        reasonCode: 'runbook_execution_not_found',
      })
    );
    expect(fallbackStatus.executionState.runId).toBe('run-2026-02-17-009');
  });

  it('sanitizes sensitive host credentials before official start request', async () => {
    const { service, requestMock } = await loadService(undefined);

    await service.startRunbook({
      change_id: 'cr-2026-02-17-011',
      provider_key: 'external-linux',
      environment_profile: 'dev-external-linux',
      blueprint_version: '1.0.0',
      host_mapping: [
        {
          host_address: '10.0.0.20',
          ssh_user: 'ubuntu',
          ssh_private_key: '-----BEGIN PRIVATE KEY-----',
          private_key_ref: 'vault://ops/ssh-key',
        },
      ],
      machine_credentials: [
        {
          machine_id: 'machine-a',
          credential_ref: 'vault://ops/ssh-key',
          credential_fingerprint: 'fp-machine-a',
          reuse_confirmed: false,
          private_key: '-----BEGIN PRIVATE KEY-----',
        },
      ],
    });

    const requestPayload = requestMock.mock.calls[0][1].data;
    expect(requestPayload.host_mapping[0].ssh_private_key).toBeUndefined();
    expect(requestPayload.host_mapping[0].private_key_ref).toBe('vault://ops/ssh-key');
    expect(requestPayload.machine_credentials[0].private_key).toBeUndefined();
    expect(requestPayload.machine_credentials[0].credential_ref).toBe('vault://ops/ssh-key');
  });

  it('enables local mode only with explicit env flag', async () => {
    const { service, requestMock } = await loadService('true', undefined, 'dev');

    expect(service.RUNBOOK_LOCAL_MODE_ENABLED).toBe(true);

    const result = await service.startRunbook({
      change_id: 'cr-2026-02-17-002',
      provider_key: 'external-linux',
      environment_profile: 'dev-external-linux',
      blueprint_version: '1.0.0',
    });

    expect(result.run.run_id.startsWith('run-local-')).toBe(true);
    expect(requestMock).not.toHaveBeenCalled();
  });

  it('blocks local runbook mode in operational environment even with explicit flag', async () => {
    const { service, requestMock } = await loadService('true', undefined, 'prod');

    expect(service.RUNBOOK_LOCAL_MODE_FLAG_ENABLED).toBe(true);
    expect(service.RUNBOOK_LOCAL_MODE_BLOCKED_BY_POLICY).toBe(true);
    expect(service.RUNBOOK_LOCAL_MODE_ENABLED).toBe(false);

    await service.startRunbook({
      change_id: 'cr-2026-02-17-004',
      provider_key: 'external-linux',
      environment_profile: 'hml-external-linux',
      blueprint_version: '1.0.0',
    });

    expect(requestMock).toHaveBeenCalledWith(
      '/api/v1/runbooks/start',
      expect.objectContaining({ method: 'POST', skipErrorHandler: true })
    );
  });

  it('rejects official runbook payloads containing *_local timeline events', async () => {
    const { service } = await loadService(undefined, () =>
      buildValidRun({
        events: [
          {
            id: 'evt-local',
            timestamp_utc: '2026-02-17T12:00:00Z',
            level: 'info',
            code: 'runbook_started_local',
            message: 'Runbook local.',
            run_id: 'run-2026-02-17-001',
            change_id: 'cr-2026-02-17-001',
          },
        ],
      })
    );

    await expect(
      service.startRunbook({
        change_id: 'cr-2026-02-17-003',
        provider_key: 'external-linux',
        environment_profile: 'dev-external-linux',
        blueprint_version: '1.0.0',
      })
    ).rejects.toThrow('timeline oficial contém eventos simulados *_local');
  });

  it('surfaces backend operational failure envelope for runbook operate', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockResolvedValue({
      status: 'fail',
      msg: 'runbook_ssh_execution_failed',
      data: {
        code: 'runbook_ssh_execution_failed',
        message: 'Execucao SSH no host alvo falhou durante etapa operacional.',
      },
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await expect(
      service.operateRunbook({
        run_id: 'run-2026-02-17-001',
        action: 'advance',
      })
    ).rejects.toThrow('Execucao SSH no host alvo falhou durante etapa operacional.');
  });

  it('surfaces backend operational failure when envelope is nested under response.data', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockResolvedValue({
      status: 200,
      data: {
        status: 'fail',
        msg: 'runbook_ssh_execution_failed',
        data: {
          code: 'runbook_ssh_execution_failed',
          message: 'Execucao SSH no host alvo falhou durante etapa operacional.',
        },
      },
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await expect(
      service.operateRunbook({
        run_id: 'run-2026-02-17-001',
        action: 'advance',
      })
    ).rejects.toThrow('Execucao SSH no host alvo falhou durante etapa operacional.');
  });

  it('surfaces backend operational failure when envelope is nested under response.data.data', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockResolvedValue({
      status: 200,
      data: {
        data: {
          status: 'fail',
          msg: 'runbook_ssh_execution_failed',
          data: {
            code: 'runbook_ssh_execution_failed',
            message: 'Execucao SSH no host alvo falhou durante etapa operacional.',
          },
        },
      },
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await expect(
      service.operateRunbook({
        run_id: 'run-2026-02-17-001',
        action: 'advance',
      })
    ).rejects.toThrow('Execucao SSH no host alvo falhou durante etapa operacional.');
  });

  it('preserves backend failure code/details from fail-like payload without envelope wrapper', async () => {
    jest.resetModules();
    delete process.env.COGNUS_RUNBOOK_LOCAL_MODE;

    const requestMock = jest.fn().mockRejectedValue({
      data: {
        code: 'runbook_runtime_image_missing',
        message: 'Imagem de runtime obrigatoria do COGNUS indisponivel no host alvo.',
        details: {
          runtime_image: 'cognus/chaincode-gateway:latest',
          stage: 'provision',
          checkpoint: 'provision.runtime',
        },
      },
      response: {
        status: 409,
      },
    });

    jest.doMock('../utils/request', () => requestMock);
    const service = await import('./runbook');

    await expect(
      service.operateRunbook({
        run_id: 'run-2026-02-17-001',
        action: 'advance',
      })
    ).rejects.toMatchObject({
      data: expect.objectContaining({
        code: 'runbook_runtime_image_missing',
        details: expect.objectContaining({
          runtime_image: 'cognus/chaincode-gateway:latest',
          stage: 'provision',
          checkpoint: 'provision.runtime',
        }),
      }),
      response: expect.objectContaining({
        status: 409,
      }),
    });
  });
});
