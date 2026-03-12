describe('services/blueprint', () => {
  const originalLocalMode = process.env.COGNUS_BLUEPRINT_LOCAL_MODE;
  const originalEnvProfile = process.env.COGNUS_ENV_PROFILE;

  const loadService = async (localModeValue, envProfileValue) => {
    jest.resetModules();

    if (localModeValue === undefined) {
      delete process.env.COGNUS_BLUEPRINT_LOCAL_MODE;
    } else {
      process.env.COGNUS_BLUEPRINT_LOCAL_MODE = localModeValue;
    }

    if (envProfileValue === undefined) {
      delete process.env.COGNUS_ENV_PROFILE;
    } else {
      process.env.COGNUS_ENV_PROFILE = envProfileValue;
    }

    const requestMock = jest.fn().mockResolvedValue({
      status: 'successful',
      data: { data: [] },
    });

    jest.doMock('../utils/request', () => requestMock);

    const service = await import('./blueprint');
    return { service, requestMock };
  };

  afterEach(() => {
    jest.resetModules();
    jest.clearAllMocks();

    if (originalLocalMode === undefined) {
      delete process.env.COGNUS_BLUEPRINT_LOCAL_MODE;
    } else {
      process.env.COGNUS_BLUEPRINT_LOCAL_MODE = originalLocalMode;
    }

    if (originalEnvProfile === undefined) {
      delete process.env.COGNUS_ENV_PROFILE;
    } else {
      process.env.COGNUS_ENV_PROFILE = originalEnvProfile;
    }
  });

  it('uses official endpoint mode by default', async () => {
    const { service, requestMock } = await loadService(undefined);

    expect(service.BLUEPRINT_LOCAL_MODE_ENABLED).toBe(false);

    await service.lintBlueprint({ blueprint: { schema_version: '1.0.0' } });

    expect(requestMock).toHaveBeenCalledWith(
      '/api/v1/blueprints/lint',
      expect.objectContaining({ method: 'POST', skipErrorHandler: true })
    );
  });

  it('sanitizes sensitive keys before official publish request', async () => {
    const { service, requestMock } = await loadService(undefined);

    await service.publishBlueprintVersion({
      blueprint: {
        nodes: [
          {
            host_address: '10.0.0.30',
            ssh_private_key: '-----BEGIN PRIVATE KEY-----',
            private_key_ref: 'vault://ops/key-ref',
          },
        ],
      },
      change_id: 'cr-2026-02-17-012',
      execution_context: 'external-linux publish',
    });

    const payload = requestMock.mock.calls[0][1].data;
    expect(payload.blueprint.nodes[0].ssh_private_key).toBeUndefined();
    expect(payload.blueprint.nodes[0].private_key_ref).toBe('[PROTECTED_REF]');
  });

  it('uses local mode only when explicit env flag is enabled', async () => {
    const { service, requestMock } = await loadService('true', 'dev');

    expect(service.BLUEPRINT_LOCAL_MODE_ENABLED).toBe(true);

    const response = await service.listBlueprintVersions();

    expect(response.status).toBe('successful');
    expect(requestMock).not.toHaveBeenCalled();
  });

  it('blocks local mode in operational environment even with explicit flag', async () => {
    const { service, requestMock } = await loadService('true', 'hml');

    expect(service.BLUEPRINT_LOCAL_MODE_FLAG_ENABLED).toBe(true);
    expect(service.BLUEPRINT_LOCAL_MODE_BLOCKED_BY_POLICY).toBe(true);
    expect(service.BLUEPRINT_LOCAL_MODE_ENABLED).toBe(false);

    await service.listBlueprintVersions();

    expect(requestMock).toHaveBeenCalledWith(
      '/api/v1/blueprints/versions',
      expect.objectContaining({ skipErrorHandler: true })
    );
  });
});
