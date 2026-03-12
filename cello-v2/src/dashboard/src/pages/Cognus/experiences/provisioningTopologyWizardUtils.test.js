import {
  buildBlueprintFromGuidedTopology,
  importGuidedTopologyFromBlueprint,
} from './provisioningTopologyWizardUtils';

const baseModel = {
  changeId: 'cr-2026-02-16-200',
  environmentProfile: 'dev-external-linux',
  machines: [
    {
      id: 'machine-1',
      infraLabel: 'vm-dev-01',
      hostAddress: '10.10.10.11',
      sshUser: 'web3',
      sshPort: 22,
      dockerPort: 2376,
    },
  ],
  organizations: [
    {
      id: 'org-1',
      name: 'INF UFG',
      domain: 'inf.ufg.br',
      label: 'org-inf-ufg',
      networkApiHost: 'api.inf.ufg.br',
      networkApiPort: 31522,
      caMode: 'internal',
      caName: 'ca-inf-ufg',
      caHost: 'ca.inf.ufg.br',
      caHostRef: 'vm-dev-01',
      caPort: 7054,
      caUser: 'ca-admin',
      caPasswordRef: 'vault://ca/inf-ufg',
      peers: 1,
      orderers: 1,
      peerHostRef: 'vm-dev-01',
      ordererHostRef: 'vm-dev-01',
    },
  ],
  businessGroup: {
    name: 'WEB3 Business Group',
    description: 'Grupo de negócio para onboarding guiado',
    networkId: 'web3-business-group',
  },
  channels: [
    {
      id: 'channel-1',
      name: 'ops-channel-dev',
      memberOrgs: 'inf-ufg',
    },
  ],
  chaincodeInstalls: [
    {
      id: 'install-1',
      channel: 'ops-channel-dev',
      chaincodeName: 'ops-cc-dev',
      endpointPath: '/api/ops-channel-dev/ops-cc-dev/query/getTx',
      packageFileName: 'ops-cc_1.0.tar.gz',
      artifactRef: 'local-file:/opt/cello/chaincode/ops-cc_1.0:abc/ops-cc_1.0.tar.gz',
    },
  ],
  modelingAuditTrail: [{ code: 'model_change', timestamp_utc: '2026-02-16T19:00:00Z' }],
};

describe('provisioningTopologyWizardUtils', () => {
  it('builds blueprint compliant structure from guided topology model', () => {
    const blueprint = buildBlueprintFromGuidedTopology(baseModel);

    expect(blueprint.schema_name).toBe('cognus-blueprint');
    expect(blueprint.network.provisioning_scope.provider_key).toBe('external-linux');
    expect(Array.isArray(blueprint.orgs)).toBe(true);
    expect(Array.isArray(blueprint.channels)).toBe(true);
    expect(Array.isArray(blueprint.nodes)).toBe(true);
    expect(Array.isArray(blueprint.policies)).toBe(true);
    expect(blueprint.environment_profile.profile_id).toBe('dev-external-linux');
    expect(blueprint.identity_baseline.baseline_version).toBe('1.0.0');
  });

  it('creates install plan in network block with explicit artifact binding', () => {
    const blueprint = buildBlueprintFromGuidedTopology(baseModel);

    expect(Array.isArray(blueprint.network.chaincodes_install)).toBe(true);
    expect(blueprint.network.chaincodes_install[0].chaincode_id).toBe('ops-cc-dev');
    expect(blueprint.network.chaincodes_install[0].artifact_ref).toBe(
      'local-file:/opt/cello/chaincode/ops-cc_1.0:abc/ops-cc_1.0.tar.gz'
    );
    expect(blueprint.network.chaincodes_install[0].install_only).toBe(true);
  });

  it('imports blueprint back into guided model defaults', () => {
    const blueprint = buildBlueprintFromGuidedTopology(baseModel);
    const imported = importGuidedTopologyFromBlueprint(blueprint);

    expect(imported.environmentProfile).toBe('dev-external-linux');
    expect(imported.machines.length).toBeGreaterThan(0);
    expect(imported.organizations.length).toBe(1);
    expect(imported.organizations[0].caHostRef).toBe('vm-dev-01');
    expect(imported.channels.length).toBe(1);
    expect(imported.chaincodeInstalls[0].chaincodeName).toBe('ops-cc-dev');
    expect(imported.chaincodeInstalls[0].artifactRef).toBe(
      'local-file:/opt/cello/chaincode/ops-cc_1.0:abc/ops-cc_1.0.tar.gz'
    );
    expect(imported.businessGroupNetworkId).toBe('web3-business-group');
  });

  it('throws for invalid blueprint import payload', () => {
    expect(() => importGuidedTopologyFromBlueprint(null)).toThrow(
      'Blueprint inválido para importação guiada.'
    );
  });
});
