import {
  buildOverviewOperationalCard,
  buildOverviewOperationalFilterOptions,
  filterOverviewOperationalCards,
} from './overviewOperationalCards';

const buildOrganization = overrides => ({
  key: 'org-one',
  orgId: 'org1',
  orgName: 'Org One',
  domain: 'org1.example.com',
  latestRunId: 'run-001',
  latestChangeId: 'cr-001',
  chaincodes: ['asset-cc'],
  channels: ['ops-channel'],
  hostTargets: [{ hostRef: 'host-a', hostAddress: '10.0.0.10' }],
  runHistory: [
    {
      key: 'audit-001',
      runId: 'run-001',
      changeId: 'cr-001',
      status: 'completed',
      finishedAt: '2025-03-01T10:00:00Z',
      capturedAt: '2025-03-01T10:00:00Z',
    },
  ],
  ...overrides,
});

const buildOfficialRun = ({
  freshnessStatus = 'fresh',
  correlationValid = true,
  orgId = 'org1',
  orgName = 'Org One',
  domain = 'org1.example.com',
} = {}) => ({
  run_id: 'run-001',
  change_id: 'cr-001',
  decision: 'allow',
  a2a_entry_gate: {
    contract_version: 'a2a-entry-gate.v1',
    status: 'implemented',
    mode: 'interactive',
    interactive_enabled: true,
    correlation_valid: correlationValid,
    baseline_converged: true,
    official_artifacts_ready: true,
    missing_artifacts: [],
    action_availability: {
      open_operational_dashboard: true,
      add_peer: true,
      add_orderer: true,
      add_channel: true,
      add_chaincode: true,
    },
    organization_readiness: [
      {
        organization_id: orgId,
        organization_name: orgName,
        status: 'implemented',
        mode: 'interactive',
        observed_host_count: 1,
        observed_in_runtime: true,
        issue_codes: [],
      },
    ],
    issues: [],
  },
  runtime_telemetry: {
    contract_version: 'runtime-telemetry.v1',
    source_of_truth: 'tests',
    generated_at_utc: '2025-03-01T10:00:00Z',
    organizations: [
      {
        organization: {
          org_id: orgId,
          org_name: orgName,
          domain,
        },
        components: [
          {
            component_id: `peer:peer0-${orgId}`,
            component_type: 'peer',
            name: `peer0-${orgId}`,
            container_name: `cognus-peer0-${orgId}`,
            status: 'running',
            host_id: 'host-a',
            org_id: orgId,
            scope: 'required',
            criticality: 'critical',
            observed_at: '2025-03-01T10:00:00Z',
            issues: [],
          },
          {
            component_id: `orderer:orderer0-${orgId}`,
            component_type: 'orderer',
            name: `orderer0-${orgId}`,
            container_name: `cognus-orderer0-${orgId}`,
            status: 'running',
            host_id: 'host-a',
            org_id: orgId,
            scope: 'required',
            criticality: 'critical',
            observed_at: '2025-03-01T10:00:00Z',
            issues: [],
          },
          {
            component_id: `ca:ca-${orgId}`,
            component_type: 'ca',
            name: `ca-${orgId}`,
            container_name: `cognus-ca-${orgId}`,
            status: 'running',
            host_id: 'host-a',
            org_id: orgId,
            scope: 'required',
            criticality: 'critical',
            observed_at: '2025-03-01T10:00:00Z',
            issues: [],
          },
          {
            component_id: `netapi:${orgId}`,
            component_type: 'netapi',
            name: `netapi-${orgId}`,
            container_name: `cognus-netapi-${orgId}`,
            status: 'planned',
            host_id: 'host-a',
            org_id: orgId,
            scope: 'planned',
            criticality: 'supporting',
            observed_at: '2025-03-01T10:00:00Z',
            issues: [],
          },
        ],
        channels: [{ channel_id: 'ops-channel' }],
        chaincodes: [{ chaincode_id: 'asset-cc' }],
        health: 'healthy',
        criticality: 'critical',
        observed_at: '2025-03-01T10:00:00Z',
        data_freshness: {
          status: freshnessStatus,
          observed_at_utc: '2025-03-01T10:00:00Z',
          source_updated_at_utc: '2025-03-01T10:00:00Z',
          lag_seconds: freshnessStatus === 'stale' ? 120 : 0,
        },
        issues: [],
      },
    ],
  },
  organization_read_model: {
    contract_version: 'organization-read-model.v1',
    source_of_truth: 'tests',
    generated_at_utc: '2025-03-01T10:00:00Z',
    organizations: [
      {
        organization: {
          org_id: orgId,
          org_name: orgName,
          domain,
        },
        health: freshnessStatus === 'stale' ? 'degraded' : 'healthy',
        observed_at: '2025-03-01T10:00:00Z',
        data_freshness: {
          status: freshnessStatus,
          observed_at_utc: '2025-03-01T10:00:00Z',
          source_updated_at_utc: '2025-03-01T10:00:00Z',
          lag_seconds: freshnessStatus === 'stale' ? 120 : 0,
        },
        read_model_fingerprint: 'org-read-model-001',
        artifact_origins: [],
        issues: [],
        workspace: {
          blocks: {
            ca: { item_total: 1, health: 'healthy', items: [], host_ids: ['host-a'] },
            api: { item_total: 1, health: 'healthy', items: [], host_ids: ['host-a'] },
            peers: { item_total: 1, health: 'healthy', items: [], host_ids: ['host-a'] },
            orderers: { item_total: 1, health: 'healthy', items: [], host_ids: ['host-a'] },
            business_group: { item_total: 1, health: 'healthy', items: [] },
            channels: { item_total: 1, health: 'healthy', items: [] },
          },
          projections: {
            channels: [{ channel_id: 'ops-channel', status: 'healthy' }],
            organization_members: [],
            peers: [{ component_id: `peer:peer0-${orgId}`, status: 'running' }],
            orderers: [{ component_id: `orderer:orderer0-${orgId}`, status: 'running' }],
            chaincodes: [{ chaincode_id: 'asset-cc', status: 'running', api_total: 1 }],
          },
        },
      },
    ],
  },
});

describe('overviewOperationalCards', () => {
  it('builds an implemented card only from official fresh evidence', () => {
    const card = buildOverviewOperationalCard({
      organization: buildOrganization(),
      officialState: {
        loading: false,
        error: '',
        run: buildOfficialRun(),
      },
    });

    expect(card.operationalStatus).toBe('implemented');
    expect(card.baselineStatus).toBe('converged');
    expect(card.freshnessStatus).toBe('fresh');
    expect(card.channelsCount).toBe(1);
    expect(card.activeChaincodesCount).toBe(1);
    expect(card.pendingAlertsCount).toBe(0);
  });

  it('downgrades the card to partial when official telemetry is stale', () => {
    const card = buildOverviewOperationalCard({
      organization: buildOrganization(),
      officialState: {
        loading: false,
        error: '',
        run: buildOfficialRun({ freshnessStatus: 'stale' }),
      },
    });

    expect(card.operationalStatus).toBe('partial');
    expect(card.freshnessStatus).toBe('stale');
    expect(card.statusReason).toContain('telemetria oficial');
    expect(card.pendingAlertsCount).toBeGreaterThan(0);
  });

  it('blocks the card when the official correlation is invalid', () => {
    const card = buildOverviewOperationalCard({
      organization: buildOrganization(),
      officialState: {
        loading: false,
        error: '',
        run: buildOfficialRun({ correlationValid: false }),
      },
    });

    expect(card.operationalStatus).toBe('blocked');
    expect(card.criticality).toBe('critical');
  });

  it('builds deterministic filter options and applies them', () => {
    const cards = [
      buildOverviewOperationalCard({
        organization: buildOrganization(),
        officialState: { loading: false, error: '', run: buildOfficialRun() },
      }),
      buildOverviewOperationalCard({
        organization: buildOrganization({ key: 'org-two', orgId: 'org2', orgName: 'Org Two' }),
        officialState: {
          loading: false,
          error: '',
          run: buildOfficialRun({
            freshnessStatus: 'stale',
            orgId: 'org2',
            orgName: 'Org Two',
            domain: 'org2.example.com',
          }),
        },
      }),
    ];

    const options = buildOverviewOperationalFilterOptions(cards);
    const filtered = filterOverviewOperationalCards(cards, {
      organization: 'Org Two',
      status: 'partial',
      criticality: 'warning',
      host: 'host-a',
      freshness: 'stale',
    });

    expect(options.organizations).toHaveLength(2);
    expect(options.statuses).toEqual(['implemented', 'partial']);
    expect(options.criticalities).toEqual(['stable', 'warning']);
    expect(options.hosts).toContain('host-a');
    expect(filtered).toHaveLength(1);
    expect(filtered[0].organizationName).toBe('Org Two');
  });
});