import {
  READINESS_EXECUTION_MODE,
  READINESS_STATUS,
  buildProvisioningReadinessActionRows,
  buildProvisioningReadinessSourceRows,
  getProvisioningActionReadiness,
  getProvisioningScreenReadiness,
  hasProvisioningReadinessMockSources,
} from './provisioningBackendReadiness';

describe('provisioningBackendReadiness', () => {
  const originalEnvProfile = process.env.COGNUS_ENV_PROFILE;

  const loadReadiness = async envProfileValue => {
    jest.resetModules();

    if (envProfileValue === undefined) {
      delete process.env.COGNUS_ENV_PROFILE;
    } else {
      process.env.COGNUS_ENV_PROFILE = envProfileValue;
    }

    return import('./provisioningBackendReadiness');
  };

  afterEach(() => {
    jest.resetModules();
    jest.clearAllMocks();

    if (originalEnvProfile === undefined) {
      delete process.env.COGNUS_ENV_PROFILE;
    } else {
      process.env.COGNUS_ENV_PROFILE = originalEnvProfile;
    }
  });

  it('returns expected screen-level readiness for E1 pages', () => {
    expect(getProvisioningScreenReadiness('e1-infra-ssh').screenStatus).toBe(
      READINESS_STATUS.implemented
    );
    expect(getProvisioningScreenReadiness('e1-tecnico').screenStatus).toBe(
      READINESS_STATUS.partial
    );
    expect(getProvisioningScreenReadiness('e1-blueprint').screenStatus).toBe(
      READINESS_STATUS.partial
    );
    expect(getProvisioningScreenReadiness('e1-provisionamento').screenStatus).toBe(
      READINESS_STATUS.partial
    );
    expect(getProvisioningScreenReadiness('e1-inventario').screenStatus).toBe(
      READINESS_STATUS.partial
    );
    expect(getProvisioningScreenReadiness('e1-topologia-runtime-org').screenStatus).toBe(
      READINESS_STATUS.implemented
    );
  });

  it('marks blueprint publication as available via official endpoint', () => {
    const publishReadiness = getProvisioningActionReadiness('e1-blueprint', 'publish_version');

    expect(publishReadiness.available).toBe(true);
    expect(publishReadiness.status).toBe(READINESS_STATUS.implemented);
    expect(publishReadiness.executionMode).toBe(READINESS_EXECUTION_MODE.official);
  });

  it('marks runbook commands as available via official backend execution', () => {
    const startReadiness = getProvisioningActionReadiness('e1-provisionamento', 'start_runbook');

    expect(startReadiness.available).toBe(true);
    expect(startReadiness.executionMode).toBe(READINESS_EXECUTION_MODE.official);
  });

  it('keeps infra SSH execution available via official runbook backend endpoint', () => {
    const startInfraReadiness = getProvisioningActionReadiness(
      'e1-infra-ssh',
      'start_ssh_provisioning'
    );

    expect(startInfraReadiness.available).toBe(true);
    expect(startInfraReadiness.status).toBe(READINESS_STATUS.implemented);
    expect(startInfraReadiness.executionMode).toBe(READINESS_EXECUTION_MODE.official);
    expect(startInfraReadiness.endpoint).toBe('/api/v1/runbooks/start');
  });

  it('keeps inventory manual sync available in hybrid mode', () => {
    const syncReadiness = getProvisioningActionReadiness('e1-inventario', 'manual_sync');

    expect(syncReadiness.available).toBe(true);
    expect(syncReadiness.status).toBe(READINESS_STATUS.partial);
    expect(syncReadiness.executionMode).toBe(READINESS_EXECUTION_MODE.hybrid);
  });

  it('keeps runtime topology load action available via official runbook status endpoint', () => {
    const topologyReadiness = getProvisioningActionReadiness(
      'e1-topologia-runtime-org',
      'load_official_topology'
    );

    expect(topologyReadiness.available).toBe(true);
    expect(topologyReadiness.status).toBe(READINESS_STATUS.implemented);
    expect(topologyReadiness.executionMode).toBe(READINESS_EXECUTION_MODE.official);
    expect(topologyReadiness.endpoint).toBe('/api/v1/runbooks/{run_id}/status');
  });

  it('builds action rows honoring preferred action order', () => {
    const rows = buildProvisioningReadinessActionRows('e1-inventario', [
      'export_evidence',
      'manual_sync',
    ]);

    expect(rows[0].actionKey).toBe('export_evidence');
    expect(rows[1].actionKey).toBe('manual_sync');
  });

  it('keeps inventory evidence sources aligned to hybrid/official modes', () => {
    const sourceRows = buildProvisioningReadinessSourceRows('e1-inventario');

    expect(sourceRows.some(source => source.key === 'inventory-runbook-official-evidence')).toBe(
      true
    );
    expect(sourceRows.some(source => source.executionMode === READINESS_EXECUTION_MODE.mock)).toBe(
      false
    );
    expect(hasProvisioningReadinessMockSources('e1-inventario')).toBe(false);
  });

  it('marks inventory as degraded in development profile', async () => {
    const readinessModule = await loadReadiness('dev');
    const inventoryReadiness = readinessModule.getProvisioningScreenReadiness('e1-inventario');

    expect(inventoryReadiness.operationalEnv).toBe(false);
    expect(inventoryReadiness.executionState).toBe(
      readinessModule.PROVISIONING_EXECUTION_STATE.degraded
    );
  });

  it('marks inventory as official in operational profile', async () => {
    const readinessModule = await loadReadiness('prod');
    const inventoryReadiness = readinessModule.getProvisioningScreenReadiness('e1-inventario');

    expect(inventoryReadiness.operationalEnv).toBe(true);
    expect(inventoryReadiness.executionState).toBe(
      readinessModule.PROVISIONING_EXECUTION_STATE.official
    );
    expect(inventoryReadiness.executionStateReason).toContain('fallback degradado proibido');
  });
});
