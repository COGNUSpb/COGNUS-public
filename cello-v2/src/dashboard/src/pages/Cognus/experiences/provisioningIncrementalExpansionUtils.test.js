import {
  applyIncrementalExpansionToModel,
  buildIncrementalExpansionIssues,
  buildIncrementalExpansionPlan,
  buildIncrementalNodeNamingPreview,
  createIncrementalApiDraft,
  createIncrementalChannelDraft,
  createIncrementalInstallDraft,
  createIncrementalNodeExpansionDraft,
} from './provisioningIncrementalExpansionUtils';

describe('provisioningIncrementalExpansionUtils', () => {
  const baseOrganizations = [
    {
      id: 'org-1',
      name: 'INF-UFG',
      domain: 'inf.ufg.br',
      peers: 1,
      orderers: 1,
      peerPortBase: 7051,
      ordererPortBase: 7050,
      peerHostRef: 'machine-a',
      ordererHostRef: 'machine-a',
    },
  ];

  const baseMachines = [
    {
      id: 'machine-1',
      infraLabel: 'machine-a',
      hostAddress: '10.10.10.11',
      sshCredentialRef: 'vault://infra/keys/machine-a',
    },
  ];

  const basePreflightReport = {
    hosts: [
      {
        id: 'machine-1',
        infraLabel: 'machine-a',
        status: 'apto',
        runtimeSnapshot: {
          cpuCores: 2,
          memoryMb: 4096,
          diskGb: 80,
        },
      },
    ],
  };

  const baseChannels = [
    {
      id: 'channel-1',
      name: 'fakenews-channel-dev',
      memberOrgs: 'INF-UFG',
    },
  ];

  const baseInstalls = [
    {
      id: 'install-1',
      channel: 'fakenews-channel-dev',
      packagePattern: 'cc-tools',
      endpointPath: '/api/fakenews-channel-dev/cc-tools/query/readAsset',
    },
  ];

  const baseApis = [
    {
      id: 'api-1',
      organizationName: 'INF-UFG',
      channel: 'fakenews-channel-dev',
      chaincodeId: 'cc-tools',
      routePath: '/api/fakenews-channel-dev/cc-tools',
    },
  ];

  it('creates default drafts for incremental operations', () => {
    expect(createIncrementalNodeExpansionDraft(1).id).toBe('increment-node-1');
    expect(createIncrementalNodeExpansionDraft(1).operationType).toBe('add_peer');
    expect(createIncrementalNodeExpansionDraft(1).requestedCount).toBe(1);
    expect(createIncrementalChannelDraft(1).id).toBe('increment-channel-1');
    expect(createIncrementalInstallDraft(1).packagePattern).toBe('cc-tools');
    expect(createIncrementalApiDraft(1).routePath).toBe('');
  });

  it('builds deterministic naming preview for add_peer/add_orderer operations', () => {
    const preview = buildIncrementalNodeNamingPreview({
      changeId: 'cr-2026-02-22-801',
      runId: 'run-2026-02-22-801',
      organizations: baseOrganizations,
      machines: baseMachines,
      preflightReport: basePreflightReport,
      nodeExpansions: [
        {
          id: 'inc-node-peer',
          organizationName: 'INF-UFG',
          operationType: 'add_peer',
          targetHostRef: 'machine-a',
          requestedCount: 2,
        },
        {
          id: 'inc-node-orderer',
          organizationName: 'INF-UFG',
          operationType: 'add_orderer',
          targetHostRef: 'machine-a',
          requestedCount: 1,
        },
      ],
    });

    expect(preview.issues).toHaveLength(0);
    expect(
      preview.operations.some(operation => operation.component_name === 'peer1.inf-ufg.inf.ufg.br')
    ).toBe(true);
    expect(
      preview.operations.some(operation => operation.component_name === 'peer2.inf-ufg.inf.ufg.br')
    ).toBe(true);
    expect(
      preview.operations.some(
        operation => operation.component_name === 'orderer1.inf-ufg.inf.ufg.br'
      )
    ).toBe(true);
    expect(preview.hostCapacity[0].host_ref).toBe('machine-a');
    expect(preview.hostCapacity[0].planned_operations).toBe(3);
  });

  it('blocks incremental plan when preflight/lint gates are not approved', () => {
    const issues = buildIncrementalExpansionIssues({
      changeId: 'cr-2026-02-16-700',
      preflightApproved: false,
      lintApproved: false,
      organizations: baseOrganizations,
      channels: baseChannels,
      chaincodeInstalls: baseInstalls,
      apiRegistrations: baseApis,
      nodeExpansions: [],
      channelAdditions: [],
      installAdditions: [],
      apiAdditions: [],
    });

    expect(issues).toEqual(
      expect.arrayContaining([
        'Expansão incremental bloqueada: preflight deve permanecer apto.',
        'Expansão incremental bloqueada: lint A1.2 deve estar aprovado para o modelo atual.',
      ])
    );
  });

  it('validates capacity and port conflicts before publishing incremental node plan', () => {
    const issues = buildIncrementalExpansionIssues({
      changeId: 'cr-2026-02-22-802',
      preflightApproved: true,
      lintApproved: true,
      organizations: baseOrganizations,
      channels: baseChannels,
      chaincodeInstalls: baseInstalls,
      apiRegistrations: baseApis,
      machines: baseMachines,
      preflightReport: basePreflightReport,
      nodeExpansions: [
        {
          id: 'inc-node-overload',
          organizationName: 'INF-UFG',
          operationType: 'add_peer',
          targetHostRef: 'machine-a',
          requestedCount: 10,
        },
      ],
      channelAdditions: [],
      installAdditions: [],
      apiAdditions: [],
    });

    expect(issues.some(issue => issue.includes('Capacidade insuficiente no host machine-a'))).toBe(
      true
    );
  });

  it('validates idempotency for duplicated channel/install/api incremental operations', () => {
    const issues = buildIncrementalExpansionIssues({
      changeId: 'cr-2026-02-16-701',
      preflightApproved: true,
      lintApproved: true,
      organizations: baseOrganizations,
      channels: baseChannels,
      chaincodeInstalls: baseInstalls,
      apiRegistrations: baseApis,
      nodeExpansions: [],
      channelAdditions: [
        { id: 'inc-channel', name: 'fakenews-channel-dev', memberOrgs: 'INF-UFG' },
      ],
      installAdditions: [
        { id: 'inc-install', channel: 'fakenews-channel-dev', packagePattern: 'cc-tools' },
      ],
      apiAdditions: [
        {
          id: 'inc-api',
          organizationName: 'INF-UFG',
          channel: 'fakenews-channel-dev',
          chaincodeId: 'cc-tools',
          routePath: '/api/fakenews-channel-dev/cc-tools',
        },
      ],
    });

    expect(issues).toEqual(
      expect.arrayContaining([
        'Channel incremental duplicado ou já existente (fakenews-channel-dev).',
        'Install incremental duplicado para fakenews-channel-dev (cc-tools).',
        'API incremental duplicada para INF-UFG/fakenews-channel-dev//api/fakenews-channel-dev/cc-tools.',
      ])
    );
  });

  it('builds deterministic incremental expansion plan with add_peer/add_orderer trail', () => {
    const firstPlan = buildIncrementalExpansionPlan({
      changeId: 'cr-2026-02-16-702',
      runId: 'run-2026-02-16-702',
      executionContext: 'A1.6.6 incremental expansion',
      generatedAtUtc: '2026-02-16T20:10:00Z',
      organizations: baseOrganizations,
      machines: baseMachines,
      preflightReport: basePreflightReport,
      nodeExpansions: [
        {
          id: 'inc-node-peer',
          organizationName: 'INF-UFG',
          operationType: 'add_peer',
          targetHostRef: 'machine-a',
          requestedCount: 1,
        },
        {
          id: 'inc-node-orderer',
          organizationName: 'INF-UFG',
          operationType: 'add_orderer',
          targetHostRef: 'machine-a',
          requestedCount: 1,
        },
      ],
      channelAdditions: [{ id: 'inc-channel', name: 'new-channel-dev', memberOrgs: 'INF-UFG' }],
      installAdditions: [
        {
          id: 'inc-install',
          channel: 'new-channel-dev',
          packagePattern: 'cc-tools',
          endpointPath: '/api/new-channel-dev/cc-tools/query/readAsset',
        },
      ],
      apiAdditions: [
        {
          id: 'inc-api',
          organizationName: 'INF-UFG',
          channel: 'new-channel-dev',
          chaincodeId: 'cc-tools',
          routePath: '',
        },
      ],
    });

    const secondPlan = buildIncrementalExpansionPlan({
      changeId: 'cr-2026-02-16-702',
      runId: 'run-2026-02-16-702',
      executionContext: 'A1.6.6 incremental expansion',
      generatedAtUtc: '2026-02-16T20:10:00Z',
      organizations: baseOrganizations,
      machines: baseMachines,
      preflightReport: basePreflightReport,
      nodeExpansions: [
        {
          id: 'inc-node-peer',
          organizationName: 'INF-UFG',
          operationType: 'add_peer',
          targetHostRef: 'machine-a',
          requestedCount: 1,
        },
        {
          id: 'inc-node-orderer',
          organizationName: 'INF-UFG',
          operationType: 'add_orderer',
          targetHostRef: 'machine-a',
          requestedCount: 1,
        },
      ],
      channelAdditions: [{ id: 'inc-channel', name: 'new-channel-dev', memberOrgs: 'INF-UFG' }],
      installAdditions: [
        {
          id: 'inc-install',
          channel: 'new-channel-dev',
          packagePattern: 'cc-tools',
          endpointPath: '/api/new-channel-dev/cc-tools/query/readAsset',
        },
      ],
      apiAdditions: [
        {
          id: 'inc-api',
          organizationName: 'INF-UFG',
          channel: 'new-channel-dev',
          chaincodeId: 'cc-tools',
          routePath: '',
        },
      ],
    });

    const operationTypes = firstPlan.map(operation => operation.operation_type);

    expect(firstPlan).toEqual(secondPlan);
    expect(operationTypes).toEqual(expect.arrayContaining(['add_peer', 'add_orderer']));
    expect(firstPlan.every(operation => operation.change_id === 'cr-2026-02-16-702')).toBe(true);
    expect(firstPlan.every(operation => operation.run_id === 'run-2026-02-16-702')).toBe(true);
  });

  it('applies incremental additions without duplicating existing inventory items', () => {
    const nextState = applyIncrementalExpansionToModel({
      changeId: 'cr-2026-02-16-703',
      runId: 'run-2026-02-16-703',
      organizations: baseOrganizations,
      channels: baseChannels,
      chaincodeInstalls: baseInstalls,
      apiRegistrations: baseApis,
      machines: baseMachines,
      preflightReport: basePreflightReport,
      nodeExpansions: [
        {
          id: 'inc-node-peer',
          organizationName: 'INF-UFG',
          operationType: 'add_peer',
          targetHostRef: 'machine-a',
          requestedCount: 2,
        },
        {
          id: 'inc-node-orderer',
          organizationName: 'INF-UFG',
          operationType: 'add_orderer',
          targetHostRef: 'machine-a',
          requestedCount: 1,
        },
      ],
      channelAdditions: [
        { id: 'inc-channel', name: 'new-channel-dev', memberOrgs: 'INF-UFG' },
        { id: 'inc-channel-dup', name: 'new-channel-dev', memberOrgs: 'INF-UFG' },
      ],
      installAdditions: [
        {
          id: 'inc-install',
          channel: 'new-channel-dev',
          packagePattern: 'cc-tools',
          endpointPath: '/api/new-channel-dev/cc-tools/query/readAsset',
        },
        {
          id: 'inc-install-dup',
          channel: 'new-channel-dev',
          packagePattern: 'cc-tools',
          endpointPath: '/api/new-channel-dev/cc-tools/query/readAsset',
        },
      ],
      apiAdditions: [
        {
          id: 'inc-api',
          organizationName: 'INF-UFG',
          channel: 'new-channel-dev',
          chaincodeId: 'cc-tools',
          routePath: '',
          exposureHost: 'api.inf.ufg.br',
          exposurePort: 31522,
        },
        {
          id: 'inc-api-dup',
          organizationName: 'INF-UFG',
          channel: 'new-channel-dev',
          chaincodeId: 'cc-tools',
          routePath: '/api/new-channel-dev/cc-tools',
          exposureHost: 'api.inf.ufg.br',
          exposurePort: 31522,
        },
      ],
    });

    expect(nextState.organizations[0].peers).toBe(3);
    expect(nextState.organizations[0].orderers).toBe(2);
    expect(nextState.organizations[0].incremental_peer_nodes).toHaveLength(2);
    expect(nextState.organizations[0].incremental_orderer_nodes).toHaveLength(1);
    expect(nextState.channels.filter(channel => channel.name === 'new-channel-dev')).toHaveLength(
      1
    );
    expect(
      nextState.chaincodeInstalls.filter(install => install.channel === 'new-channel-dev')
    ).toHaveLength(1);
    expect(
      nextState.apiRegistrations.filter(api => api.channel === 'new-channel-dev')
    ).toHaveLength(1);
  });
});
