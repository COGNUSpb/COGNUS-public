import {
  buildCanonicalOnboardingRunbookHandoffPayload,
  buildOnboardingRunbookHandoff,
  buildResolvedHostMapping,
  buildRunbookBlueprintCatalogEntry,
  clearOnboardingRunbookHandoff,
  consumeOnboardingRunbookHandoff,
  isPreflightApproved,
  listOnboardingRunbookHandoffTrail,
  persistOnboardingRunbookHandoff,
  resolveOnboardingRunbookHandoffFromTrail,
} from './provisioningOnboardingHandoffUtils';

describe('provisioningOnboardingHandoffUtils', () => {
  afterEach(() => {
    window.sessionStorage.clear();
    window.localStorage.clear();
  });

  it('detects approved preflight only when all hosts are apto', () => {
    expect(
      isPreflightApproved({
        overallStatus: 'apto',
        hosts: [{ status: 'apto' }, { status: 'apto' }],
      })
    ).toBe(true);

    expect(
      isPreflightApproved({
        overallStatus: 'parcial',
        hosts: [{ status: 'apto' }],
      })
    ).toBe(false);

    expect(
      isPreflightApproved({
        overallStatus: 'apto',
        hosts: [{ status: 'bloqueado' }],
      })
    ).toBe(false);
  });

  it('builds deterministic resolved host mapping from blueprint nodes and machine registry', () => {
    const mapping = buildResolvedHostMapping({
      blueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
          {
            node_id: 'orderer0-org1',
            node_type: 'orderer',
            org_id: 'org1',
            host_ref: 'machine-b',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-b',
          hostAddress: '10.0.0.11',
          sshUser: 'ubuntu',
          sshPort: 22,
          dockerPort: 2376,
        },
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      preflightReport: {
        overallStatus: 'apto',
        hosts: [
          {
            infraLabel: 'machine-a',
            status: 'apto',
          },
          {
            infraLabel: 'machine-b',
            status: 'bloqueado',
          },
        ],
      },
    });

    expect(mapping).toHaveLength(2);
    expect(mapping[0].node_id).toBe('orderer0-org1');
    expect(mapping[0].host_address).toBe('10.0.0.11');
    expect(mapping[0].preflight_status).toBe('bloqueado');
    expect(mapping[1].node_id).toBe('peer0-org1');
    expect(mapping[1].host_address).toBe('10.0.0.10');
    expect(mapping[1].preflight_status).toBe('apto');
  });

  it('canonicalizes host mapping org_id using topology organization aliases', () => {
    const handoff = buildOnboardingRunbookHandoff({
      changeId: 'cr-2026-02-23-alias-001',
      environmentProfile: 'dev-external-linux',
      runId: 'run-alias-001',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-inf2',
            node_type: 'peer',
            org_id: 'inf2',
            host_ref: 'machine1',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine1',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      organizations: [
        {
          name: 'INF2',
          label: 'org-inf2',
          peerHostRef: 'machine1',
        },
      ],
      channels: [],
      chaincodeInstalls: [],
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '22'.repeat(32),
        resolved_schema_version: '1.0.0',
      },
      preflightReport: {
        overallStatus: 'apto',
        hosts: [{ infraLabel: 'machine1', status: 'apto' }],
      },
      officialPublish: true,
    });

    expect(handoff.host_mapping.length).toBeGreaterThanOrEqual(1);
    const peerRow = handoff.host_mapping.find(row => row.node_type === 'peer');
    expect(peerRow).toBeTruthy();
    expect(peerRow.org_id).toBe('org-inf2');
    const canonicalPeerRow = handoff.handoff_payload.host_mapping.find(
      row => row.node_type === 'peer'
    );
    expect(canonicalPeerRow).toBeTruthy();
    expect(canonicalPeerRow.org_id).toBe('org-inf2');
  });

  it('associates channel chaincodes to single organization when member_orgs is empty', () => {
    const handoff = buildOnboardingRunbookHandoff({
      changeId: 'cr-2026-02-23-single-org-cc-001',
      environmentProfile: 'dev-external-linux',
      runId: 'run-single-org-cc-001',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      organizations: [{ name: 'Org 1', label: 'org1', peerHostRef: 'machine-a' }],
      channels: [{ name: 'library', memberOrgs: '' }],
      chaincodeInstalls: [{ chaincodeName: 'reserve-book', channel: 'library' }],
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '33'.repeat(32),
        resolved_schema_version: '1.0.0',
      },
      preflightReport: {
        overallStatus: 'apto',
        hosts: [{ infraLabel: 'machine-a', status: 'apto' }],
      },
      officialPublish: true,
    });

    expect(handoff.topology_catalog.organizations).toHaveLength(1);
    expect(handoff.topology_catalog.organizations[0].channels).toContain('library');
    expect(handoff.topology_catalog.organizations[0].chaincodes).toContain('reserve-book');
    expect(handoff.host_mapping.some(row => row.node_type === 'chaincode')).toBe(true);
  });

  it('expands runtime chaincode rows per channel+chaincode and keeps one apigateway per org', () => {
    const handoff = buildOnboardingRunbookHandoff({
      changeId: 'cr-2026-02-23-multi-runtime-001',
      environmentProfile: 'dev-external-linux',
      runId: 'run-multi-runtime-001',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      organizations: [
        {
          name: 'Org 1',
          label: 'org1',
          peerHostRef: 'machine-a',
          apiGatewayHostRef: 'machine-a',
          netApiHostRef: 'machine-a',
        },
      ],
      channels: [
        { name: 'channel-a', memberOrgs: 'org1' },
        { name: 'channel-b', memberOrgs: 'org1' },
      ],
      chaincodeInstalls: [
        { chaincodeName: 'cc-a', channel: 'channel-a' },
        { chaincodeName: 'cc-b', channel: 'channel-b' },
      ],
      apiRegistry: [
        {
          api_id: 'api-cc-a',
          org_id: 'org1',
          org_name: 'Org 1',
          channel_id: 'channel-a',
          chaincode_id: 'cc-a',
          route_path: '/api/channel-a/cc-a/*/*',
          exposure: { host: '10.0.0.10', port: 31509 },
          status: 'active',
        },
        {
          api_id: 'api-cc-b',
          org_id: 'org1',
          org_name: 'Org 1',
          channel_id: 'channel-b',
          chaincode_id: 'cc-b',
          route_path: '/api/channel-b/cc-b/*/*',
          exposure: { host: '10.0.0.10', port: 31510 },
          status: 'active',
        },
      ],
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '44'.repeat(32),
        resolved_schema_version: '1.0.0',
      },
      preflightReport: {
        overallStatus: 'apto',
        hosts: [{ infraLabel: 'machine-a', status: 'apto' }],
      },
      officialPublish: true,
    });

    const apigatewayRows = handoff.host_mapping.filter(row => row.node_type === 'apigateway');
    const chaincodeRows = handoff.host_mapping.filter(row => row.node_type === 'chaincode');

    expect(apigatewayRows).toHaveLength(1);
    expect(new Set(apigatewayRows.map(row => row.node_id)).size).toBe(1);
    expect(chaincodeRows).toHaveLength(2);
    expect(new Set(chaincodeRows.map(row => row.node_id)).size).toBe(2);
    expect(
      chaincodeRows.some(row => row.node_id.includes('channel-a') && row.node_id.includes('cc-a'))
    ).toBe(true);
    expect(
      chaincodeRows.some(row => row.node_id.includes('channel-b') && row.node_id.includes('cc-b'))
    ).toBe(true);
  });

  it('builds onboarding handoff with coherent IDs and persisted host mapping', () => {
    const handoff = buildOnboardingRunbookHandoff({
      changeId: 'cr-2026-02-16-ssh-001',
      environmentProfile: 'dev-external-linux',
      runId: 'run-abcdef1234567890abcdef12',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      machineCredentials: [
        {
          machine_id: 'machine-a',
          credential_ref: 'vault://infra/keys/machine-a',
          credential_fingerprint: 'fp-machine-a',
          reuse_confirmed: false,
        },
      ],
      organizations: [
        {
          name: 'Org 1',
          domain: 'org1.example.com',
          label: 'org1',
          networkApiHost: 'api.org1.example.com',
          networkApiPort: 31509,
          netApiHostRef: 'machine-a',
          netApiRoutePrefix: '/network',
          netApiAccessRef: 'vault://netapi/org1/access',
          apiGatewayHostRef: 'machine-a',
          apiGatewayPort: 8443,
          apiGatewayRoutePrefix: '/api',
          apiGatewayAuthRef: 'vault://apigateway/org1/auth',
          couchHostRef: 'machine-a',
          couchPort: 5984,
          couchDatabase: 'org1_ledger',
          couchAdminUser: 'couchdb',
          couchAdminPasswordRef: 'vault://couch/org1/admin',
          caMode: 'internal',
          caName: 'ca-org1',
          caHost: 'ca.org1.example.com',
          caHostRef: 'machine-a',
          caPort: 7054,
          caUser: 'ca-admin',
          caPasswordRef: 'vault://ca/org1/admin',
          peerPortBase: 7051,
          ordererPortBase: 7050,
          peerHostRef: 'machine-a',
          ordererHostRef: 'machine-a',
        },
      ],
      channels: [
        {
          name: 'org1-channel',
          memberOrgs: 'Org 1',
        },
      ],
      chaincodeInstalls: [
        {
          chaincodeName: 'cc-tools',
          channel: 'org1-channel',
        },
      ],
      businessGroupName: 'BG Teste',
      businessGroupDescription: 'BG de teste',
      businessGroupNetworkId: 'bg-teste',
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
        resolved_schema_version: '1.0.0',
        manifest_fingerprint: 'a14fd6be4f7df2ca13dc03f2f2f6fe615f0f093aa5f87fd5fa2b5b6ccba11a88',
        source_blueprint_fingerprint:
          '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
        backend_state: 'ready',
        a2_2_minimum_artifacts: [
          'provision-plan.json',
          'reconcile-report.json',
          'inventory-final.json',
          'stage-reports.json',
          'verify-report.json',
          'ssh-execution-log.json',
        ],
        a2_2_available_artifacts: [
          'provision-plan.json',
          'reconcile-report.json',
          'inventory-final.json',
          'stage-reports/prepare-report.json',
          'verify-report.json',
          'ssh-execution-log.json',
        ],
      },
      preflightReport: {
        overallStatus: 'apto',
        hosts: [{ status: 'apto' }],
      },
      modelingAuditTrail: [
        {
          code: 'guided_lint_passed',
          timestamp_utc: '2026-02-16T13:00:00Z',
        },
      ],
      apiRegistry: [
        {
          api_id: 'api-1',
          org_name: 'Org 1',
          channel_id: 'org1-channel',
          chaincode_id: 'cc-tools',
          route_path: '/api/org1-channel/cc-tools/*/*',
          exposure: {
            host: '10.0.0.10',
            port: 31509,
          },
          status: 'active',
        },
      ],
      executionContext: 'Onboarding SSH -> blueprint -> runbook (A1.6.4)',
      officialPublish: true,
      incrementalExpansions: [
        {
          expansion_id: 'exp-1',
          change_id: 'cr-2026-02-16-ssh-001',
          run_id: 'run-abcdef1234567890abcdef12',
          operations_total: 1,
        },
      ],
    });

    expect(handoff.change_id).toBe('cr-2026-02-16-ssh-001');
    expect(handoff.run_id).toBe('run-abcdef1234567890abcdef12');
    expect(handoff.context_source).toBe('official_backend_publish');
    expect(handoff.blueprint_version).toBe('1.0.0');
    expect(handoff.manifest_fingerprint).toBe(
      'a14fd6be4f7df2ca13dc03f2f2f6fe615f0f093aa5f87fd5fa2b5b6ccba11a88'
    );
    expect(handoff.source_blueprint_fingerprint).toBe(
      '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de'
    );
    expect(handoff.handoff_contract_version).toBe('a2.frontend-handoff.v1');
    expect(handoff.handoff_fingerprint).toMatch(/^[0-9a-f]{64}$/);
    expect(handoff.handoff_payload.correlation.change_id).toBe('cr-2026-02-16-ssh-001');
    expect(handoff.handoff_payload.correlation.run_id).toBe('run-abcdef1234567890abcdef12');
    expect(handoff.runbook_resume_context.run_id).toBe('run-abcdef1234567890abcdef12');
    expect(handoff.handoff_trace[0].event).toBe('handoff_created');
    expect(handoff.official_backend_state).toBe('ready');
    expect(handoff.a2_2_artifacts_ready).toBe(true);
    expect(handoff.a2_2_missing_artifacts).toHaveLength(0);
    expect(handoff.blueprint_validated).toBe(true);
    expect(handoff.preflight_approved).toBe(true);
    expect(handoff.host_mapping.length).toBeGreaterThanOrEqual(5);
    const hostNodeTypes = handoff.host_mapping.map(row => row.node_type);
    expect(hostNodeTypes).toContain('peer');
    expect(hostNodeTypes).toContain('couch');
    expect(hostNodeTypes).toContain('apigateway');
    expect(hostNodeTypes).toContain('netapi');
    expect(hostNodeTypes).toContain('chaincode');
    expect(handoff.host_mapping.every(row => row.host_address === '10.0.0.10')).toBe(true);
    expect(handoff.host_mapping.every(row => row.preflight_status === 'apto')).toBe(true);
    expect(
      handoff.host_mapping.every(row => !Object.prototype.hasOwnProperty.call(row, 'metadata'))
    ).toBe(true);
    expect(handoff.machine_credentials).toHaveLength(1);
    expect(handoff.machine_credentials[0].machine_id).toBe('machine-a');
    expect(handoff.handoff_payload.machine_credentials).toHaveLength(1);
    expect(handoff.incremental_expansions).toHaveLength(1);
    expect(handoff.topology_catalog.organizations).toHaveLength(1);
    expect(handoff.topology_catalog.organizations[0].domain).toBe('org1.example.com');
    expect(handoff.topology_catalog.organizations[0].service_host_mapping.peer).toBe('machine-a');
    expect(handoff.topology_catalog.organizations[0].service_host_mapping.netapi).toBe('machine-a');
    expect(handoff.topology_catalog.organizations[0].service_parameters.couch.port).toBe(5984);
    expect(handoff.topology_catalog.organizations[0].service_parameters.apiGateway.port).toBe(8443);
    expect(handoff.topology_catalog.organizations[0].service_parameters.netapi.access_ref).toBe(
      'vault://netapi/org1/access'
    );
    expect(handoff.topology_catalog.organizations[0].peers).toHaveLength(1);
    expect(handoff.topology_catalog.business_groups[0].channels[0].channel_id).toBe('org1-channel');
  });

  it('builds deterministic canonical handoff for the same input state', () => {
    const basePayload = {
      changeId: 'cr-2026-02-20-001',
      environmentProfile: 'dev-external-linux',
      runId: 'run-a2-canonical-001',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
          {
            node_id: 'orderer0-org1',
            node_type: 'orderer',
            org_id: 'org1',
            host_ref: 'machine-b',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-b',
          hostAddress: '10.0.0.11',
          sshUser: 'ubuntu',
          sshPort: 22,
          dockerPort: 2376,
        },
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      machineCredentials: [
        {
          machine_id: 'machine-a',
          credential_ref: 'vault://infra/keys/machine-a',
          credential_fingerprint: 'fp-machine-a',
          reuse_confirmed: false,
        },
        {
          machine_id: 'machine-b',
          credential_ref: 'vault://infra/keys/machine-b',
          credential_fingerprint: 'fp-machine-b',
          reuse_confirmed: false,
        },
      ],
      organizations: [
        {
          name: 'Org 1',
          label: 'org1',
          domain: 'org1.example.com',
          peers: 1,
          orderers: 1,
          peerHostRef: 'machine-a',
          ordererHostRef: 'machine-b',
          caHostRef: 'machine-a',
          caMode: 'internal',
          caName: 'ca-org1',
          caHost: 'ca.org1.example.com',
          caPort: 7054,
          caUser: 'ca-admin',
          caPasswordRef: 'vault://ca/org1/admin',
          peerPortBase: 7051,
          ordererPortBase: 7050,
          couchHostRef: 'machine-a',
          couchPort: 5984,
          couchDatabase: 'org1_ledger',
          couchAdminUser: 'couchdb',
          couchAdminPasswordRef: 'vault://couch/org1/admin',
          apiGatewayHostRef: 'machine-a',
          apiGatewayPort: 8443,
          apiGatewayRoutePrefix: '/api',
          apiGatewayAuthRef: 'vault://apigateway/org1/auth',
          networkApiHost: 'netapi.org1.example.com',
          netApiHostRef: 'machine-a',
          networkApiPort: 31509,
          netApiRoutePrefix: '/network',
          netApiAccessRef: 'vault://netapi/org1/access',
        },
      ],
      channels: [
        {
          name: 'channel-b',
          memberOrgs: 'Org 1',
        },
        {
          name: 'channel-a',
          memberOrgs: 'Org 1',
        },
      ],
      chaincodeInstalls: [
        {
          chaincodeName: 'cc-b',
          channel: 'channel-b',
        },
        {
          chaincodeName: 'cc-a',
          channel: 'channel-a',
        },
      ],
      businessGroupName: 'BG Teste',
      businessGroupDescription: 'BG de teste',
      businessGroupNetworkId: 'bg-teste',
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
        resolved_schema_version: '1.0.0',
        manifest_fingerprint: 'a14fd6be4f7df2ca13dc03f2f2f6fe615f0f093aa5f87fd5fa2b5b6ccba11a88',
        source_blueprint_fingerprint:
          '97db1fa62b6fa78b4f1bc4d1d02f72f7f2fcfcde4f4ea53d962e7a448a8cd0de',
        backend_state: 'ready',
        a2_2_minimum_artifacts: ['verify-report.json'],
        a2_2_available_artifacts: ['verify-report.json'],
      },
      preflightReport: {
        overallStatus: 'apto',
        hosts: [
          { infraLabel: 'machine-a', status: 'apto' },
          { infraLabel: 'machine-b', status: 'apto' },
        ],
      },
      modelingAuditTrail: [
        { code: 'z-last', timestamp_utc: '2026-02-20T13:02:00Z' },
        { code: 'a-first', timestamp_utc: '2026-02-20T13:01:00Z' },
      ],
      apiRegistry: [
        {
          api_id: 'api-z',
          org_name: 'Org 1',
          channel_id: 'channel-b',
          chaincode_id: 'cc-b',
          route_path: '/b',
          exposure: { host: '10.0.0.10', port: 31509 },
        },
        {
          api_id: 'api-a',
          org_name: 'Org 1',
          channel_id: 'channel-a',
          chaincode_id: 'cc-a',
          route_path: '/a',
          exposure: { host: '10.0.0.10', port: 31509 },
        },
      ],
      executionContext: 'Onboarding SSH -> blueprint -> runbook (A2)',
      officialPublish: true,
      incrementalExpansions: [
        { expansion_id: 'exp-z', change_id: 'cr-2026-02-20-001', run_id: 'run-a2-canonical-001' },
        { expansion_id: 'exp-a', change_id: 'cr-2026-02-20-001', run_id: 'run-a2-canonical-001' },
      ],
    };
    const reorderedPayload = {
      ...basePayload,
      machines: [...basePayload.machines].reverse(),
      channels: [...basePayload.channels].reverse(),
      chaincodeInstalls: [...basePayload.chaincodeInstalls].reverse(),
      apiRegistry: [...basePayload.apiRegistry].reverse(),
      incrementalExpansions: [...basePayload.incrementalExpansions].reverse(),
      modelingAuditTrail: [...basePayload.modelingAuditTrail].reverse(),
    };

    const handoffA = buildOnboardingRunbookHandoff(basePayload);
    const handoffB = buildOnboardingRunbookHandoff(reorderedPayload);

    expect(handoffA.handoff_fingerprint).toBe(handoffB.handoff_fingerprint);
    expect(handoffA.handoff_payload).toEqual(handoffB.handoff_payload);
    expect(buildCanonicalOnboardingRunbookHandoffPayload(handoffA)).toEqual(
      buildCanonicalOnboardingRunbookHandoffPayload(handoffB)
    );
  });

  it('preserves explicit chaincode artifact bindings in topology catalog and canonical handoff', () => {
    const handoff = buildOnboardingRunbookHandoff({
      changeId: 'cr-2026-03-08-artifact-001',
      environmentProfile: 'dev-external-linux',
      runId: 'run-artifact-001',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      organizations: [{ name: 'Org 1', label: 'org1', peerHostRef: 'machine-a' }],
      channels: [{ name: 'channel-a', memberOrgs: 'Org 1' }],
      chaincodeInstalls: [
        {
          id: 'install-1',
          chaincodeName: 'cc-a',
          channel: 'channel-a',
          endpointPath: '/api/channel-a/cc-a/query/readAsset',
          packagePattern: 'cc-tools',
          packageFileName: 'cc-a_1.0.tar.gz',
          artifactRef: 'local-file:/opt/cello/chaincode/cc-a_1.0:abc123/cc-a_1.0.tar.gz',
        },
      ],
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '44'.repeat(32),
        resolved_schema_version: '1.0.0',
      },
      preflightReport: {
        overallStatus: 'apto',
        hosts: [{ infraLabel: 'machine-a', status: 'apto' }],
      },
      officialPublish: true,
    });

    expect(handoff.topology_catalog.chaincodes).toEqual([
      expect.objectContaining({
        channel_id: 'channel-a',
        chaincode_id: 'cc-a',
        package_pattern: 'cc-tools',
        package_file_name: 'cc-a_1.0.tar.gz',
        artifact_ref: 'local-file:/opt/cello/chaincode/cc-a_1.0:abc123/cc-a_1.0.tar.gz',
      }),
    ]);
    expect(handoff.handoff_payload.network.chaincodes_install).toEqual([
      expect.objectContaining({
        channel_id: 'channel-a',
        chaincode_id: 'cc-a',
        package_pattern: 'cc-tools',
        package_file_name: 'cc-a_1.0.tar.gz',
        artifact_ref: 'local-file:/opt/cello/chaincode/cc-a_1.0:abc123/cc-a_1.0.tar.gz',
      }),
    ]);
  });

  it('defaults official publish handoff to ready backend/artifacts when backend omits A2 gate fields', () => {
    const handoff = buildOnboardingRunbookHandoff({
      changeId: 'cr-2026-02-20-002',
      environmentProfile: 'dev-external-linux',
      runId: 'run-a2-fallback-002',
      guidedBlueprint: {
        nodes: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            org_id: 'org1',
            host_ref: 'machine-a',
          },
        ],
      },
      machines: [
        {
          infraLabel: 'machine-a',
          hostAddress: '10.0.0.10',
          sshUser: 'web3',
          sshPort: 22,
          dockerPort: 2376,
        },
      ],
      organizations: [{ name: 'Org 1', label: 'org1', peerHostRef: 'machine-a' }],
      channels: [{ name: 'channel-a', memberOrgs: 'Org 1' }],
      chaincodeInstalls: [{ chaincodeName: 'cc-a', channel: 'channel-a' }],
      publishedBlueprintRecord: {
        blueprint_version: '1.0.0',
        fingerprint_sha256: '11'.repeat(32),
        resolved_schema_version: '1.0.0',
      },
      preflightReport: { overallStatus: 'apto', hosts: [{ status: 'apto' }] },
      officialPublish: true,
    });

    expect(handoff.context_source).toBe('official_backend_publish');
    expect(handoff.official_backend_state).toBe('ready');
    expect(handoff.manifest_fingerprint).toBe('11'.repeat(32));
    expect(handoff.source_blueprint_fingerprint).toBe('11'.repeat(32));
    expect(handoff.a2_2_artifacts_ready).toBe(true);
    expect(handoff.a2_2_missing_artifacts).toHaveLength(0);
    expect(handoff.a2_2_available_artifacts).toEqual(handoff.a2_2_minimum_artifacts);
  });

  it('persists and reads onboarding handoff from session storage', () => {
    const persisted = persistOnboardingRunbookHandoff({
      change_id: 'cr-001',
      run_id: 'run-001',
      manifest_fingerprint: 'ab'.repeat(32),
      source_blueprint_fingerprint: 'cd'.repeat(32),
      blueprint_version: '1.0.0',
    });

    expect(persisted).toBe(true);
    const consumed = consumeOnboardingRunbookHandoff();
    expect(consumed.change_id).toBe('cr-001');
    expect(consumeOnboardingRunbookHandoff().run_id).toBe('run-001');
    expect(listOnboardingRunbookHandoffTrail()).toHaveLength(1);
  });

  it('falls back to compact trail payload when localStorage quota is exceeded', () => {
    const trailStorageKey = 'cognus.provisioning.onboarding.runbook.handoff.trail.v1';
    const originalSetItem = Storage.prototype.setItem;
    let failedOnce = false;
    const setItemSpy = jest
      .spyOn(Storage.prototype, 'setItem')
      .mockImplementation(function mockedSetItem(key, value) {
        if (key === trailStorageKey && !failedOnce) {
          failedOnce = true;
          throw new DOMException('The quota has been exceeded.', 'QuotaExceededError');
        }
        return originalSetItem.call(this, key, value);
      });

    const persisted = persistOnboardingRunbookHandoff({
      change_id: 'cr-001-quota',
      run_id: 'run-001-quota',
      manifest_fingerprint: 'ab'.repeat(32),
      source_blueprint_fingerprint: 'cd'.repeat(32),
      blueprint_version: '1.0.0',
      host_mapping: Array.from({ length: 20 }).map((_, index) => ({
        node_id: `peer-${index}`,
        host_address: `10.0.0.${index + 1}`,
      })),
    });

    setItemSpy.mockRestore();

    expect(persisted).toBe(true);
    expect(listOnboardingRunbookHandoffTrail()).toHaveLength(1);
    expect(consumeOnboardingRunbookHandoff().run_id).toBe('run-001-quota');
  });

  it('restores handoff from trail when session payload is unavailable', () => {
    persistOnboardingRunbookHandoff({
      change_id: 'cr-restore-001',
      run_id: 'run-restore-001',
      manifest_fingerprint: 'ab'.repeat(32),
      source_blueprint_fingerprint: 'cd'.repeat(32),
      blueprint_version: '1.0.0',
    });
    clearOnboardingRunbookHandoff();

    const fallbackHandoff = consumeOnboardingRunbookHandoff();
    expect(fallbackHandoff).not.toBeNull();
    expect(fallbackHandoff.run_id).toBe('run-restore-001');

    const byCorrelation = resolveOnboardingRunbookHandoffFromTrail({
      runId: 'run-restore-001',
      changeId: 'cr-restore-001',
    });
    expect(byCorrelation).not.toBeNull();
    expect(byCorrelation.handoff_fingerprint).toMatch(/^[0-9a-f]{64}$/);
  });

  it('normalizes legacy A2 handoff without context_source to official_backend_publish', () => {
    persistOnboardingRunbookHandoff({
      source: 'infra-ssh-onboarding-a1.6',
      handoff_contract_version: 'a2.frontend-handoff.v1',
      change_id: 'cr-legacy-ctx-001',
      run_id: 'run-legacy-ctx-001',
      source_blueprint_fingerprint: 'ee'.repeat(32),
      manifest_fingerprint: '',
      blueprint_fingerprint: 'ee'.repeat(32),
    });

    const consumed = consumeOnboardingRunbookHandoff();
    expect(consumed.context_source).toBe('official_backend_publish');
    expect(consumed.manifest_fingerprint).toBe('ee'.repeat(32));
    expect(consumed.official_backend_state).toBe('ready');
    expect(consumed.a2_2_artifacts_ready).toBe(true);
  });

  it('restores host_mapping from canonical handoff_payload when top-level mapping is absent', () => {
    persistOnboardingRunbookHandoff({
      source: 'infra-ssh-onboarding-a1.6',
      handoff_contract_version: 'a2.frontend-handoff.v1',
      change_id: 'cr-hostmap-fallback-001',
      run_id: 'run-hostmap-fallback-001',
      host_mapping: [],
      topology_catalog: {
        organizations: [
          {
            org_id: 'org-inf2',
            org_name: 'INF2',
          },
        ],
      },
      handoff_payload: {
        host_mapping: [
          {
            node_id: 'peer0-inf2',
            node_type: 'peer',
            org_id: 'inf2',
            host_ref: 'machine1',
            host_address: '10.0.0.10',
            ssh_user: 'web3',
            ssh_port: 22,
            preflight_status: 'apto',
          },
        ],
      },
    });

    const consumed = consumeOnboardingRunbookHandoff();
    expect(consumed.host_mapping).toHaveLength(1);
    expect(consumed.host_mapping[0].org_id).toBe('org-inf2');
  });

  it('builds runbook catalog entry from handoff payload', () => {
    const entry = buildRunbookBlueprintCatalogEntry({
      blueprint_version: '1.4.0',
      blueprint_validated: true,
      resolved_schema_version: '1.0.0',
      blueprint_fingerprint: '4c66f210f9d3bb14cf259503d2a8d02e6ba5ef1ef6a8e0a52691682a3e8d9c42',
      change_id: 'cr-2026-02-16-004',
    });

    expect(entry.value).toBe('1.4.0');
    expect(entry.lintValid).toBe(true);
    expect(entry.changeId).toBe('cr-2026-02-16-004');
  });
});
