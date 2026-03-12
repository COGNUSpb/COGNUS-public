import {
  buildApiExposureTarget,
  buildApiManagementIssues,
  buildApiRegistryEntries,
  buildApiRoutePath,
} from './provisioningApiManagementUtils';

describe('provisioningApiManagementUtils', () => {
  it('builds stable organization base route', () => {
    const route = buildApiRoutePath({
      channelId: 'fakenews-channel-dev',
      chaincodeId: 'fakenews-cc-dev',
    });

    expect(route).toBe('/api/fakenews-channel-dev/fakenews-cc-dev');
  });

  it('resolves exposure host/port preferring organization VM host reference and apiGatewayPort', () => {
    const exposure = buildApiExposureTarget({
      organizationName: 'INF-UFG',
      machines: [
        {
          id: 'machine-1',
          infraLabel: 'machine1',
          hostAddress: '200.137.197.215',
        },
      ],
      organizations: [
        {
          name: 'INF-UFG',
          peerHostRef: 'machine1',
          networkApiHost: 'api.inf.ufg.br',
          apiGatewayPort: 8443,
          networkApiPort: 31522,
        },
      ],
    });

    expect(exposure.host).toBe('200.137.197.215');
    expect(exposure.port).toBe(8443);
  });

  it('falls back to networkApiPort when apiGatewayPort is absent', () => {
    const exposure = buildApiExposureTarget({
      organizationName: 'INF-UFG',
      machines: [
        {
          id: 'machine-1',
          infraLabel: 'machine1',
          hostAddress: '200.137.197.215',
        },
      ],
      organizations: [
        {
          name: 'INF-UFG',
          peerHostRef: 'machine1',
          networkApiHost: 'api.inf.ufg.br',
          networkApiPort: 31522,
        },
      ],
    });

    expect(exposure.port).toBe(31522);
  });

  it('accepts registration independently of specific channel+chaincode activity in organization scope', () => {
    const issues = buildApiManagementIssues({
      changeId: 'cr-2026-02-16-006',
      organizations: [{ name: 'INF-UFG' }],
      apiRegistrations: [
        {
          id: 'api-1',
          organizationName: 'INF-UFG',
          channel: 'fakenews-channel-dev',
          chaincodeId: 'fakenews-cc-dev',
          routePath: '/api/',
          exposureHost: 'api.inf.ufg.br',
          exposurePort: 31522,
        },
      ],
    });

    expect(issues).toHaveLength(0);
  });

  it('accepts valid api registration linked to active channel and chaincode', () => {
    const issues = buildApiManagementIssues({
      changeId: 'cr-2026-02-16-006',
      organizations: [{ name: 'INF-UFG' }],
      apiRegistrations: [
        {
          id: 'api-1',
          organizationName: 'INF-UFG',
          channel: 'fakenews-channel-dev',
          chaincodeId: 'fakenews-cc-dev',
          routePath: '/api/',
          exposureHost: 'api.inf.ufg.br',
          exposurePort: 31522,
        },
      ],
    });

    expect(issues).toHaveLength(0);
  });

  it('builds auditable deterministic registry entries', () => {
    const rows = buildApiRegistryEntries({
      changeId: 'cr-2026-02-16-006',
      executionContext: 'A1.6.5 API onboarding',
      generatedAtUtc: '2026-02-16T13:30:00Z',
      apiRegistrations: [
        {
          id: 'api-2',
          organizationName: 'INF-UFG',
          channel: 'fakenews-channel-dev',
          chaincodeId: 'fakenews-cc-dev',
          routePath: '/api/',
          exposureHost: 'api.inf.ufg.br',
          exposurePort: 31522,
        },
      ],
    });

    expect(rows).toHaveLength(1);
    expect(rows[0].change_id).toBe('cr-2026-02-16-006');
    expect(rows[0].execution_context).toBe('A1.6.5 API onboarding');
    expect(rows[0].exposure.host).toBe('api.inf.ufg.br');
    expect(rows[0].route_path).toBe('/api/');
  });
});
