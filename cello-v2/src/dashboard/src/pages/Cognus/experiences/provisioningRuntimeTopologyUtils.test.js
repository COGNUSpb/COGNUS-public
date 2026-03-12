import {
  ORG_RUNTIME_SCOPE,
  ORG_RUNTIME_STATUS,
  buildOfficialRunEvidenceMeta,
  buildOrganizationRuntimeTopologyFilterOptions,
  buildOrganizationRuntimeTopologyRows,
  buildOrganizationRuntimeTopologySummary,
  buildOrganizationWorkspaceReadModels,
  filterOrganizationRuntimeTopologyRows,
} from './provisioningRuntimeTopologyUtils';

const buildSampleOfficialRun = () => ({
  run_id: 'run-a2-topology-001',
  change_id: 'cr-a2-topology-001',
  status: 'completed',
  backend_state: 'ready',
  manifest_fingerprint: 'aa11bb22cc33',
  source_blueprint_fingerprint: 'dd44ee55ff66',
  handoff_fingerprint: '1122334455667788',
  updated_at_utc: '2026-02-22T16:35:00Z',
  a2a_entry_gate: {
    contract_version: 'a2a-entry-gate.v1',
    source_of_truth: 'official_backend_artifacts',
    status: 'implemented',
    mode: 'interactive',
    interactive_enabled: true,
    correlation_valid: true,
    baseline_converged: true,
    official_artifacts_ready: true,
    required_artifacts: ['inventory-final', 'verify-report', 'stage-reports', 'ssh-execution-log'],
    missing_artifacts: [],
    artifact_checks: {
      'inventory-final': true,
      'verify-report': true,
      'stage-reports': true,
      'ssh-execution-log': true,
    },
    action_availability: {
      open_operational_dashboard: true,
      add_peer: true,
      add_orderer: true,
      add_channel: true,
      add_chaincode: true,
    },
    organization_readiness: [
      {
        organization_id: 'org1',
        organization_name: 'Org One',
        status: 'implemented',
        mode: 'interactive',
        observed_host_count: 3,
        observed_in_runtime: true,
        issue_codes: [],
      },
    ],
    issues: [],
    updated_at_utc: '2026-02-22T16:35:00Z',
  },
  official_decision: {
    decision: 'allow',
    decision_code: 'ALLOW_COMPLETED_WITH_MIN_EVIDENCE',
    evidence_minimum_valid: true,
    required_evidence_keys: [
      'stage-reports:verify-report.json',
      'inventory-final:inventory-final.json',
    ],
    missing_evidence_keys: [],
  },
  stages: [
    { key: 'prepare', status: 'completed' },
    { key: 'provision', status: 'completed' },
    { key: 'reconcile', status: 'completed' },
    { key: 'verify', status: 'completed' },
  ],
  artifact_rows: [
    {
      key: 'stage-reports/verify-report.json',
      artifact_key: 'stage-reports:verify-report.json',
      available: true,
    },
    {
      key: 'inventory-final.json',
      artifact_key: 'inventory-final:inventory-final.json',
      available: true,
    },
  ],
  host_inventory: [
    {
      stage: 'verify',
      checkpoint: 'consistency',
      host_ref: 'host-a',
      org_id: 'org1',
      node_id: 'peer0-org1',
      exit_code: 0,
      timestamp_utc: '2026-02-22T16:30:00Z',
    },
    {
      stage: 'verify',
      checkpoint: 'consistency',
      host_ref: 'host-a',
      org_id: 'org1',
      node_id: 'orderer0-org1',
      exit_code: 0,
      timestamp_utc: '2026-02-22T16:31:00Z',
    },
    {
      stage: 'verify',
      checkpoint: 'consistency',
      host_ref: 'host-a',
      org_id: 'org1',
      node_id: 'ca-org1',
      exit_code: 0,
      timestamp_utc: '2026-02-22T16:32:00Z',
    },
  ],
  api_registry: [
    {
      org_name: 'Org One',
      channel_id: 'ops-channel',
      chaincode_id: 'cc-tools',
      api_id: 'api-org1',
      route_path: '/api/ops-channel/cc-tools',
    },
  ],
  topology_catalog: {
    organizations: [
      {
        org_id: 'org1',
        org_name: 'Org One',
        peer_host_ref: 'host-a',
        orderer_host_ref: 'host-a',
        service_host_mapping: {
          peer: 'host-a',
          orderer: 'host-a',
          ca: 'host-a',
          couch: 'host-a',
          apiGateway: 'host-a',
          netapi: 'host-a',
        },
        service_parameters: {
          couch: {
            database: 'org1_ledger',
          },
          apiGateway: {
            route_prefix: '/api',
          },
          netapi: {
            route_prefix: '/network',
            desired_state: 'planned',
          },
        },
        peers: [
          {
            node_id: 'peer0-org1',
            node_type: 'peer',
            host_ref: 'host-a',
            desired_state: 'required',
          },
        ],
        orderers: [
          {
            node_id: 'orderer0-org1',
            node_type: 'orderer',
            host_ref: 'host-a',
            desired_state: 'required',
          },
        ],
        cas: [
          {
            node_id: 'ca-org1',
            node_type: 'ca',
            host_ref: 'host-a',
            desired_state: 'required',
          },
        ],
        chaincodes: ['cc-tools'],
      },
    ],
  },
});

const buildOfficialRuntimeTelemetryRun = () => ({
  ...buildSampleOfficialRun(),
  runtime_telemetry: {
    contract_version: 'a2a-runtime-telemetry.v1',
    source_of_truth: 'official_backend_artifacts',
    generated_at_utc: '2026-02-22T16:35:00Z',
    correlation: {
      run_id: 'run-a2-topology-001',
      change_id: 'cr-a2-topology-001',
      manifest_fingerprint: 'aa11bb22cc33',
      source_blueprint_fingerprint: 'dd44ee55ff66',
    },
    organizations: [
      {
        organization: {
          org_id: 'org1',
          org_name: 'Org One',
          domain: 'org1.example.com',
        },
        components: [
          {
            component_id: 'peer:peer0-org1',
            component_type: 'peer',
            name: 'peer0-org1',
            container_name: 'cognus-peer0-org1',
            image: 'hyperledger/fabric-peer:2.5',
            platform: 'docker/linux',
            status: 'running',
            started_at: '2026-02-22T16:30:00Z',
            health: { status: 'healthy', reason: 'host_evidence_available_and_successful' },
            ports: [{ protocol: 'tcp', container_port: 7051, host_port: 7051 }],
            mounts: [{ source: '/tmp/cognus/msp/org1', target: '/etc/hyperledger/fabric/msp', mode: 'rw' }],
            env: [
              {
                key: 'CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD',
                value: '',
                value_redacted: true,
                value_digest: 'digest-001',
                secure_ref: '',
              },
            ],
            host_id: 'host-a',
            org_id: 'org1',
            channel_refs: ['ops-channel'],
            chaincode_refs: ['cc-tools'],
            required_state: 'running',
            observed_state: {
              semantic_status: 'running',
              evidence_source: 'host_stage_evidences',
              observed_at_utc: '2026-02-22T16:30:00Z',
              stage: 'verify',
              checkpoint: 'verify.consistency',
              exit_code: 0,
              container_status: 'running',
            },
            scope: 'required',
            criticality: 'critical',
            observed_at: '2026-02-22T16:30:00Z',
            issues: [],
          },
          {
            component_id: 'network_api:netapi-org1',
            component_type: 'network_api',
            name: 'netapi-org1',
            container_name: 'cognus-netapi-org1',
            image: 'cognus/netapi:latest',
            platform: 'docker/linux',
            status: 'planned',
            started_at: '',
            health: { status: 'planned', reason: 'component_declared_as_planned' },
            ports: [{ protocol: 'tcp', container_port: 3000, host_port: 3000 }],
            mounts: [],
            env: [],
            host_id: 'host-a',
            org_id: 'org1',
            channel_refs: ['ops-channel'],
            chaincode_refs: ['cc-tools'],
            required_state: 'planned',
            observed_state: {
              semantic_status: 'planned',
              evidence_source: 'topology_catalog',
            },
            scope: 'planned',
            criticality: 'supporting',
            observed_at: '',
            issues: [],
          },
        ],
        channels: [{ channel_id: 'ops-channel', member_orgs: ['Org One'], chaincodes: ['cc-tools'], status: 'running' }],
        chaincodes: [{ chaincode_id: 'cc-tools', channel_refs: ['ops-channel'], status: 'running' }],
        health: 'healthy',
        criticality: 'critical',
        observed_at: '2026-02-22T16:30:00Z',
        data_freshness: {
          status: 'stale',
          source_updated_at_utc: '2026-02-22T16:35:00Z',
          observed_at_utc: '2026-02-22T16:30:00Z',
          lag_seconds: 300,
        },
        issues: [],
        artifacts: { run_id: 'run-a2-topology-001', change_id: 'cr-a2-topology-001', artifact_rows: [] },
        read_model_fingerprint: 'read-model-001',
      },
    ],
    summary: {
      organization_total: 1,
      component_total: 2,
      healthy_total: 1,
      degraded_total: 0,
      unknown_total: 0,
    },
  },
});

const buildOfficialOrganizationReadModelRun = () => ({
  ...buildOfficialRuntimeTelemetryRun(),
  organization_read_model: {
    contract_version: 'a2a-organization-read-model.v1',
    source_of_truth: 'official_backend_artifacts',
    generated_at_utc: '2026-02-22T16:35:00Z',
    correlation: {
      run_id: 'run-a2-topology-001',
      change_id: 'cr-a2-topology-001',
      manifest_fingerprint: 'aa11bb22cc33',
      source_blueprint_fingerprint: 'dd44ee55ff66',
    },
    artifact_origins: [
      { artifact: 'inventory-final', available: true, matched_tokens: ['inventory-final'] },
      { artifact: 'verify-report', available: true, matched_tokens: ['stage-reports/verify-report'] },
      { artifact: 'runtime-reconcile-report', available: true, matched_tokens: ['runtime-reconcile-report'] },
      { artifact: 'api-smoke-report', available: true, matched_tokens: ['api-smoke-report'] },
    ],
    organizations: [
      {
        organization: {
          org_id: 'org1',
          org_name: 'Org One',
          domain: 'org1.example.com',
        },
        health: 'healthy',
        observed_at: '2026-02-22T16:30:00Z',
        data_freshness: { status: 'stale', lag_seconds: 300 },
        workspace: {
          blocks: {
            ca: {
              block_id: 'ca',
              title: 'CA',
              item_total: 1,
              status_totals: { running: 1, degraded: 0, planned: 0, stopped: 0, missing: 0, unknown: 0 },
              health: 'healthy',
              observation_state: 'observed',
              host_ids: ['host-a'],
              detail_refs: [{ kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'ca:ca-org1' }],
              items: [
                {
                  component_id: 'ca:ca-org1',
                  component_type: 'ca',
                  name: 'ca-org1',
                  host_id: 'host-a',
                  status: 'running',
                  observation_state: 'observed',
                  detail_ref: { kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'ca:ca-org1' },
                  source_artifacts: [{ artifact: 'inventory-final', available: true }],
                },
              ],
              source_artifacts: [{ artifact: 'inventory-final', available: true }],
            },
            api: {
              block_id: 'api',
              title: 'API',
              item_total: 2,
              status_totals: { running: 1, degraded: 0, planned: 1, stopped: 0, missing: 0, unknown: 0 },
              health: 'planned',
              observation_state: 'observed',
              host_ids: ['host-a'],
              detail_refs: [{ kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'network_api:netapi-org1' }],
              items: [
                {
                  component_id: 'network_api:netapi-org1',
                  component_type: 'network_api',
                  name: 'netapi-org1',
                  host_id: 'host-a',
                  status: 'planned',
                  observation_state: 'planned',
                  detail_ref: { kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'network_api:netapi-org1' },
                  source_artifacts: [{ artifact: 'api-smoke-report', available: true }],
                },
              ],
              source_artifacts: [{ artifact: 'api-smoke-report', available: true }],
            },
            peers: {
              block_id: 'peers',
              title: 'Peers',
              item_total: 1,
              status_totals: { running: 1, degraded: 0, planned: 0, stopped: 0, missing: 0, unknown: 0 },
              health: 'healthy',
              observation_state: 'observed',
              host_ids: ['host-a'],
              detail_refs: [{ kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'peer:peer0-org1' }],
              items: [
                {
                  component_id: 'peer:peer0-org1',
                  component_type: 'peer',
                  name: 'peer0-org1',
                  host_id: 'host-a',
                  status: 'running',
                  channel_refs: ['ops-channel'],
                  observation_state: 'observed',
                  detail_ref: { kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'peer:peer0-org1' },
                  source_artifacts: [{ artifact: 'verify-report', available: true }],
                },
              ],
              source_artifacts: [{ artifact: 'verify-report', available: true }],
            },
            orderers: {
              block_id: 'orderers',
              title: 'Orderers',
              item_total: 1,
              status_totals: { running: 1, degraded: 0, planned: 0, stopped: 0, missing: 0, unknown: 0 },
              health: 'healthy',
              observation_state: 'observed',
              host_ids: ['host-a'],
              detail_refs: [],
              items: [
                {
                  component_id: 'orderer:orderer0-org1',
                  component_type: 'orderer',
                  name: 'orderer0-org1',
                  host_id: 'host-a',
                  status: 'running',
                  observation_state: 'observed',
                  source_artifacts: [{ artifact: 'runtime-reconcile-report', available: true }],
                },
              ],
              source_artifacts: [{ artifact: 'runtime-reconcile-report', available: true }],
            },
            business_group: {
              block_id: 'business_group',
              title: 'Business Group',
              item_total: 1,
              status_totals: { running: 1, degraded: 0, planned: 0, stopped: 0, missing: 0, unknown: 0 },
              health: 'healthy',
              observation_state: 'observed',
              host_ids: [],
              detail_refs: [{ kind: 'business_group', group_id: 'bg-ops', org_id: 'org1' }],
              items: [
                {
                  group_id: 'bg-ops',
                  name: 'BG Ops',
                  status: 'running',
                  observation_state: 'observed',
                  member_orgs: ['Org One', 'Org Two'],
                  detail_ref: { kind: 'business_group', group_id: 'bg-ops', org_id: 'org1' },
                  source_artifacts: [{ artifact: 'inventory-final', available: true }],
                },
              ],
              source_artifacts: [{ artifact: 'inventory-final', available: true }],
            },
            channels: {
              block_id: 'channels',
              title: 'Channels',
              item_total: 1,
              status_totals: { running: 1, degraded: 0, planned: 0, stopped: 0, missing: 0, unknown: 0 },
              health: 'healthy',
              observation_state: 'observed',
              host_ids: [],
              detail_refs: [{ kind: 'channel', channel_id: 'ops-channel', org_id: 'org1' }],
              items: [
                {
                  channel_id: 'ops-channel',
                  name: 'ops-channel',
                  status: 'running',
                  health: 'healthy',
                  observation_state: 'observed',
                  member_orgs: ['Org One', 'Org Two'],
                  peer_total: 1,
                  orderer_total: 1,
                  chaincode_total: 1,
                  detail_ref: { kind: 'channel', channel_id: 'ops-channel', org_id: 'org1' },
                  source_artifacts: [{ artifact: 'inventory-final', available: true }],
                },
              ],
              source_artifacts: [{ artifact: 'inventory-final', available: true }],
            },
          },
          projections: {
            channels: [
              {
                channel_id: 'ops-channel',
                member_orgs: ['Org One', 'Org Two'],
                peer_total: 1,
                orderer_total: 1,
                chaincode_total: 1,
                api_total: 1,
                status: 'running',
                health: 'healthy',
                observation_state: 'observed',
                detail_ref: { kind: 'channel', channel_id: 'ops-channel', org_id: 'org1' },
                source_artifacts: [{ artifact: 'inventory-final', available: true }],
              },
            ],
            organization_members: [
              {
                member_id: 'Org One',
                member_name: 'Org One',
                membership_role: 'local_org',
                channel_total: 1,
                channel_refs: ['ops-channel'],
                observation_state: 'observed',
                source_artifacts: [{ artifact: 'inventory-final', available: true }],
              },
            ],
            peers: [
              {
                component_id: 'peer:peer0-org1',
                component_type: 'peer',
                name: 'peer0-org1',
                host_id: 'host-a',
                status: 'running',
                channel_refs: ['ops-channel'],
                observation_state: 'observed',
                detail_ref: { kind: 'component', org_id: 'org1', host_id: 'host-a', component_id: 'peer:peer0-org1' },
                source_artifacts: [{ artifact: 'verify-report', available: true }],
              },
            ],
            orderers: [
              {
                component_id: 'orderer:orderer0-org1',
                component_type: 'orderer',
                name: 'orderer0-org1',
                host_id: 'host-a',
                status: 'running',
                observation_state: 'observed',
                source_artifacts: [{ artifact: 'runtime-reconcile-report', available: true }],
              },
            ],
            chaincodes: [
              {
                chaincode_id: 'cc-tools',
                channel_refs: ['ops-channel'],
                api_total: 1,
                status: 'healthy',
                observation_state: 'observed',
                source_artifacts: [{ artifact: 'api-smoke-report', available: true }],
              },
            ],
          },
        },
        artifact_origins: [
          { artifact: 'inventory-final', available: true, matched_tokens: ['inventory-final'] },
          { artifact: 'verify-report', available: true, matched_tokens: ['stage-reports/verify-report'] },
          { artifact: 'runtime-reconcile-report', available: true, matched_tokens: ['runtime-reconcile-report'] },
          { artifact: 'api-smoke-report', available: true, matched_tokens: ['api-smoke-report'] },
        ],
        issues: [],
        read_model_fingerprint: 'org-read-model-001',
      },
    ],
    summary: { organization_total: 1, healthy_total: 1, degraded_total: 0, unknown_total: 0 },
    read_model_fingerprint: 'read-model-root-001',
  },
});

describe('provisioningRuntimeTopologyUtils', () => {
  it('builds deterministic runtime rows by organization with all mandatory component types', () => {
    const rows = buildOrganizationRuntimeTopologyRows(buildSampleOfficialRun());
    const componentTypes = new Set(rows.map(row => row.componentType));

    expect(componentTypes.has('peer')).toBe(true);
    expect(componentTypes.has('orderer')).toBe(true);
    expect(componentTypes.has('ca')).toBe(true);
    expect(componentTypes.has('couch')).toBe(true);
    expect(componentTypes.has('apiGateway')).toBe(true);
    expect(componentTypes.has('netapi')).toBe(true);
    expect(componentTypes.has('chaincode_runtime')).toBe(true);

    const peerRow = rows.find(row => row.componentName === 'peer0-org1');
    expect(peerRow).toBeTruthy();
    expect(peerRow.status).toBe(ORG_RUNTIME_STATUS.running);
    expect(peerRow.statusSource).toBe('host_inventory');

    const netapiRow = rows.find(row => row.componentType === 'netapi');
    expect(netapiRow).toBeTruthy();
    expect(netapiRow.scope).toBe(ORG_RUNTIME_SCOPE.planned);
    expect(netapiRow.status).toBe(ORG_RUNTIME_STATUS.planned);
    expect(netapiRow.statusSource).toBe('topology_catalog');
  });

  it('consumes the official backend runtime telemetry contract when present', () => {
    const rows = buildOrganizationRuntimeTopologyRows(buildOfficialRuntimeTelemetryRun());
    const peerRow = rows.find(row => row.componentName === 'peer0-org1');
    const netapiRow = rows.find(row => row.componentName === 'netapi-org1');

    expect(rows).toHaveLength(2);
    expect(peerRow).toBeTruthy();
    expect(peerRow.status).toBe(ORG_RUNTIME_STATUS.running);
    expect(peerRow.statusSource).toBe('runtime_telemetry');
    expect(peerRow.componentId).toBe('peer:peer0-org1');
    expect(peerRow.containerName).toBe('cognus-peer0-org1');
    expect(peerRow.env[0].value_redacted).toBe(true);
    expect(peerRow.requiredState).toBe('running');
    expect(peerRow.observedState.semantic_status).toBe('running');
    expect(netapiRow).toBeTruthy();
    expect(netapiRow.status).toBe(ORG_RUNTIME_STATUS.planned);
  });

  it('normalizes the official organization read model with workspace blocks and projections', () => {
    const workspaces = buildOrganizationWorkspaceReadModels(buildOfficialOrganizationReadModelRun());
    const workspace = workspaces[0];

    expect(workspaces).toHaveLength(1);
    expect(workspace.organization.orgId).toBe('org1');
    expect(workspace.readModelFingerprint).toBe('org-read-model-001');
    expect(workspace.workspace.blocks.businessGroup.items[0].name).toBe('BG Ops');
    expect(workspace.workspace.blocks.channels.health).toBe('healthy');
    expect(workspace.workspace.blocks.peers.items[0].detailRef.componentId).toBe('peer:peer0-org1');
    expect(workspace.workspace.projections.channels[0].peerTotal).toBe(1);
    expect(workspace.workspace.projections.organizationMembers[0].membershipRole).toBe('local_org');
    expect(workspace.workspace.projections.chaincodes[0].apiTotal).toBe(1);
    expect(workspace.artifactOrigins.find(row => row.artifact === 'api-smoke-report').available).toBe(true);
  });

  it('builds host/type/status/criticality filters and applies deterministic filtering', () => {
    const rows = buildOrganizationRuntimeTopologyRows(buildSampleOfficialRun());
    const options = buildOrganizationRuntimeTopologyFilterOptions(rows);

    expect(options.hosts).toEqual(['host-a']);
    expect(options.componentTypes).toContain('peer');
    expect(options.statuses).toContain('running');
    expect(options.criticalities).toContain('critical');

    const plannedRows = filterOrganizationRuntimeTopologyRows(rows, {
      host: 'all',
      componentType: 'all',
      status: 'planned',
      criticality: 'all',
    });
    expect(plannedRows).toHaveLength(1);
    expect(plannedRows[0].componentType).toBe('netapi');
  });

  it('summarizes convergence and scope totals deterministically', () => {
    const rows = buildOrganizationRuntimeTopologyRows(buildSampleOfficialRun());
    const summary = buildOrganizationRuntimeTopologySummary(rows);

    expect(summary.totalComponents).toBe(rows.length);
    expect(summary.scopeTotals.required).toBeGreaterThan(0);
    expect(summary.scopeTotals.planned).toBe(1);
    expect(summary.requiredConverged).toBe(summary.requiredTotal);
    expect(summary.requiredConvergencePercent).toBe(100);
  });

  it('exposes official evidence metadata from runbook payload', () => {
    const meta = buildOfficialRunEvidenceMeta(buildSampleOfficialRun());

    expect(meta.hasOfficialRun).toBe(true);
    expect(meta.runId).toBe('run-a2-topology-001');
    expect(meta.changeId).toBe('cr-a2-topology-001');
    expect(meta.verifyArtifactAvailable).toBe(true);
    expect(meta.inventoryArtifactAvailable).toBe(true);
    expect(meta.decision).toBe('allow');
    expect(meta.stageStatusByKey.verify).toBe('completed');
    expect(meta.a2aEntryGate.status).toBe('implemented');
    expect(meta.a2aEntryGate.mode).toBe('interactive');
    expect(meta.a2aEntryGate.organizationReadiness[0].organizationName).toBe('Org One');
    expect(meta.a2aEntryGate.actionAvailability.addPeer).toBe(true);
  });

  it('builds deterministic A2A gate fallback when backend contract is absent', () => {
    const runWithoutGate = buildSampleOfficialRun();
    delete runWithoutGate.a2a_entry_gate;

    const meta = buildOfficialRunEvidenceMeta(runWithoutGate);

    expect(meta.a2aEntryGate.contractVersion).toBe('a2a-entry-gate.v1-fallback');
    expect(meta.a2aEntryGate.status).toBe('partial');
    expect(meta.a2aEntryGate.mode).toBe('read_only_blocked');
    expect(meta.a2aEntryGate.missingArtifacts).toContain('ssh-execution-log');
    expect(meta.a2aEntryGate.organizationReadiness[0].status).toBe('pending');
  });

  it('forces cached official snapshots into read-only gate mode for audit-only access', () => {
    const cachedRun = {
      ...buildSampleOfficialRun(),
      cached_status_fallback: {
        active: true,
        source: 'official_status_cache',
        savedAtUtc: '2026-03-09T12:00:00Z',
      },
    };

    const meta = buildOfficialRunEvidenceMeta(cachedRun);

    expect(meta.a2aEntryGate.mode).toBe('read_only_blocked');
    expect(meta.a2aEntryGate.interactiveEnabled).toBe(false);
    expect(meta.a2aEntryGate.actionAvailability.addPeer).toBe(false);
    expect(meta.a2aEntryGate.actionAvailability.addOrderer).toBe(false);
    expect(meta.a2aEntryGate.actionAvailability.addChannel).toBe(false);
    expect(meta.a2aEntryGate.actionAvailability.addChaincode).toBe(false);
    expect(meta.a2aEntryGate.actionAvailability.openOperationalDashboard).toBe(true);
    expect(meta.a2aEntryGate.issues.map(issue => issue.code)).toContain(
      'runbook_a2a_status_cache_fallback'
    );
  });

  it('marks required components as error when official run is failed without matching evidence', () => {
    const failedRun = {
      ...buildSampleOfficialRun(),
      status: 'failed',
      stages: [
        { key: 'prepare', status: 'completed' },
        { key: 'provision', status: 'failed' },
        { key: 'reconcile', status: 'pending' },
        { key: 'verify', status: 'pending' },
      ],
      host_inventory: [],
      host_stage_evidences: {},
    };

    const rows = buildOrganizationRuntimeTopologyRows(failedRun);
    const peerRow = rows.find(row => row.componentName === 'peer0-org1');

    expect(peerRow).toBeTruthy();
    expect(peerRow.scope).toBe(ORG_RUNTIME_SCOPE.required);
    expect(peerRow.status).toBe(ORG_RUNTIME_STATUS.degraded);
    expect(peerRow.statusSource).toBe('run_status');
  });
});
