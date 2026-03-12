import resolveOfficialRuntimeInspectionRow from './runtimeInspectionDrilldownUtils';

describe('resolveOfficialRuntimeInspectionRow', () => {
  const rows = [
    {
      componentId: 'org1-peer0',
      componentName: 'peer0.org1.example.com',
      componentType: 'peer',
      nodeId: 'peer0.org1.example.com',
      organizationId: 'org1',
      hostRef: 'host-a',
      status: 'running',
      channelId: 'channel-alpha',
      chaincodeId: '',
      routePath: '',
    },
    {
      componentId: 'org1-netapi',
      componentName: 'netapi-org1',
      componentType: 'netapi',
      nodeId: 'org1-netapi',
      organizationId: 'org1',
      hostRef: 'host-a',
      status: 'running',
      channelId: '',
      chaincodeId: '',
      routePath: '/org1/netapi',
    },
    {
      componentId: 'org1-apigateway',
      componentName: 'gateway-fakenews',
      componentType: 'apiGateway',
      nodeId: 'org1-apigateway',
      organizationId: 'org1',
      hostRef: 'host-a',
      status: 'planned',
      channelId: 'channel-alpha',
      chaincodeId: 'fakenews',
      routePath: '/fakenews',
    },
    {
      componentId: 'org2-orderer0',
      componentName: 'orderer0.org2.example.com',
      componentType: 'orderer',
      nodeId: 'orderer0.org2.example.com',
      organizationId: 'org2',
      hostRef: 'host-b',
      status: 'degraded',
      channelId: 'channel-alpha',
      chaincodeId: '',
      routePath: '',
    },
  ];

  it('resolves an exact node inside the organization', () => {
    expect(
      resolveOfficialRuntimeInspectionRow(rows, {
        organizationId: 'org1',
        componentType: 'peer',
        nodeId: 'peer0.org1.example.com',
      })
    ).toEqual(rows[0]);
  });

  it('prefers netapi for the organization API drill-down alias', () => {
    expect(
      resolveOfficialRuntimeInspectionRow(rows, {
        organizationId: 'org1',
        componentType: 'api',
      })
    ).toEqual(rows[1]);
  });

  it('filters by channel and route metadata when present', () => {
    expect(
      resolveOfficialRuntimeInspectionRow(rows, {
        organizationId: 'org1',
        componentType: 'apiGateway',
        channelId: 'channel-alpha',
        routePath: '/fakenews',
      })
    ).toEqual(rows[2]);
  });

  it('returns null when no official row matches the selector', () => {
    expect(
      resolveOfficialRuntimeInspectionRow(rows, {
        organizationId: 'org1',
        componentType: 'orderer',
      })
    ).toBeNull();
  });
});
