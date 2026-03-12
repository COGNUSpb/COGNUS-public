import { BLUEPRINT_RUNTIME_SCHEMA_VERSION } from './provisioningBlueprintUtils';
import { PROVISIONING_INFRA_PROVIDER_KEY } from './provisioningInfrastructureUtils';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const DEFAULT_NETWORK_API_PORT = 31522;
const DEFAULT_CA_PORT = 7054;
const DEFAULT_DOCKER_PORT = 2376;
const DEFAULT_SSH_PORT = 22;

const isPlainObject = value => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const sanitizeText = value => String(value || '').trim();

const toSlug = value =>
  sanitizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

const normalizePort = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    return fallback;
  }
  return parsed;
};

const profileToStage = profile => {
  const normalized = sanitizeText(profile).toLowerCase();
  if (normalized.startsWith('prod')) {
    return 'prod';
  }
  if (normalized.startsWith('hml')) {
    return 'hml';
  }
  return 'dev';
};

const stageToResourceProfile = stage => {
  if (stage === 'prod') {
    return {
      peer: { cpu: 2, memory_mb: 8192, disk_gb: 120 },
      orderer: { cpu: 2, memory_mb: 6144, disk_gb: 100 },
      ca: { cpu: 1, memory_mb: 4096, disk_gb: 80 },
    };
  }

  if (stage === 'hml') {
    return {
      peer: { cpu: 2, memory_mb: 6144, disk_gb: 100 },
      orderer: { cpu: 2, memory_mb: 4096, disk_gb: 90 },
      ca: { cpu: 1, memory_mb: 3072, disk_gb: 70 },
    };
  }

  return {
    peer: { cpu: 1, memory_mb: 4096, disk_gb: 80 },
    orderer: { cpu: 1, memory_mb: 3072, disk_gb: 70 },
    ca: { cpu: 1, memory_mb: 2048, disk_gb: 50 },
  };
};

const pickMachineLabel = (machineLabel, machines, fallbackIndex) => {
  const requested = sanitizeText(machineLabel);
  if (requested && machines.some(machine => sanitizeText(machine.infraLabel) === requested)) {
    return requested;
  }

  const fallbackMachine = machines[fallbackIndex] || machines[0] || null;
  return fallbackMachine ? sanitizeText(fallbackMachine.infraLabel) : '';
};

const buildOrgIdentity = ({ orgId }) => ({
  ca_profile: `${orgId}-ca-sign`,
  tls_ca_profile: `${orgId}-ca-tls`,
  msp_path: `/var/lib/cognus/msp/${orgId}`,
  association_policies: {
    member_roles: ['admin', 'operator'],
  },
  node_identity_policy: {
    subject_pattern: `CN={{node_id}}.${orgId},OU=nodes,O=${orgId}`,
  },
  admin_identity_policy: {
    subject_pattern: `CN=admin.${orgId},OU=admins,O=${orgId}`,
  },
});

const buildEnvironmentProfile = ({ environmentProfile, stage }) => ({
  profile_id: sanitizeText(environmentProfile) || `${stage}-external-linux`,
  stage,
  provider: 'external',
  compute_target: 'vm_linux',
  os_family: 'linux',
  deployment_stack: 'docker',
  stack_compatibility: ['docker', 'hybrid'],
  infra_constraints: {
    min_cpu_per_node: 1,
    min_memory_mb_per_node: 2048,
    min_disk_gb_per_node: 40,
  },
  security_baseline: 'cognus-baseline-v1',
  observability_level: stage === 'prod' ? 'enhanced' : 'standard',
  cost_class:
    stage === 'prod'
      ? 'high'
      : (() => {
          if (stage === 'hml') {
            return 'medium';
          }
          return 'low';
        })(),
});

export const buildBlueprintFromGuidedTopology = ({
  changeId,
  environmentProfile,
  machines,
  organizations,
  businessGroup,
  channels,
  chaincodeInstalls,
}) => {
  const now = toIsoUtc();
  const stage = profileToStage(environmentProfile);
  const resourceProfile = stageToResourceProfile(stage);

  const normalizedMachines = (machines || []).map((machine, index) => ({
    id: sanitizeText(machine.id) || `machine-${index + 1}`,
    infraLabel: sanitizeText(machine.infraLabel) || `machine-${index + 1}`,
    hostAddress: sanitizeText(machine.hostAddress),
    sshUser: sanitizeText(machine.sshUser),
    sshPort: normalizePort(machine.sshPort, DEFAULT_SSH_PORT),
    dockerPort: normalizePort(machine.dockerPort, DEFAULT_DOCKER_PORT),
  }));

  const normalizedOrganizations = (organizations || []).map((organization, index) => {
    const orgName = sanitizeText(organization.name) || `ORG-${index + 1}`;
    const orgId = toSlug(orgName) || `org-${index + 1}`;

    return {
      id: sanitizeText(organization.id) || `org-${index + 1}`,
      orgId,
      displayName: orgName,
      domain: sanitizeText(organization.domain) || `${orgId}.cognus.local`,
      label: sanitizeText(organization.label) || `org-${orgId}`,
      networkApiHost: sanitizeText(organization.networkApiHost) || `api.${orgId}.cognus.local`,
      networkApiPort: normalizePort(organization.networkApiPort, DEFAULT_NETWORK_API_PORT),
      caMode: sanitizeText(organization.caMode) || 'internal',
      caName: sanitizeText(organization.caName) || `${orgId}-ca`,
      caHost: sanitizeText(organization.caHost) || `ca.${orgId}.cognus.local`,
      caHostRef: pickMachineLabel(
        organization.caHostRef || organization.peerHostRef || organization.ordererHostRef,
        normalizedMachines,
        index
      ),
      caPort: normalizePort(organization.caPort, DEFAULT_CA_PORT),
      caUser: sanitizeText(organization.caUser) || 'ca-admin',
      caPasswordRef: sanitizeText(organization.caPasswordRef) || `vault://ca/${orgId}`,
      peers: Math.max(0, Number(organization.peers || 0)),
      orderers: Math.max(0, Number(organization.orderers || 0)),
      peerHostRef: pickMachineLabel(organization.peerHostRef, normalizedMachines, index),
      ordererHostRef: pickMachineLabel(organization.ordererHostRef, normalizedMachines, index),
    };
  });

  const nodes = [];
  normalizedOrganizations.forEach((organization, index) => {
    const peerBasePort = 7051 + index * 200;
    const ordererBasePort = 7050 + index * 200;

    for (let peerIndex = 0; peerIndex < organization.peers; peerIndex += 1) {
      const peerNodeId = `peer${peerIndex}-${organization.orgId}`;
      nodes.push({
        node_id: peerNodeId,
        org_id: organization.orgId,
        node_type: 'peer',
        host_ref: organization.peerHostRef,
        provider: 'external',
        os_family: 'linux',
        stack: 'docker',
        ports: [peerBasePort + peerIndex * 10, peerBasePort + peerIndex * 10 + 1],
        storage_profile: stage === 'prod' ? 'persistent-ssd' : 'persistent-standard',
        resources: { ...resourceProfile.peer },
      });
    }

    for (let ordererIndex = 0; ordererIndex < organization.orderers; ordererIndex += 1) {
      const ordererNodeId = `orderer${ordererIndex}-${organization.orgId}`;
      nodes.push({
        node_id: ordererNodeId,
        org_id: organization.orgId,
        node_type: 'orderer',
        host_ref: organization.ordererHostRef,
        provider: 'external',
        os_family: 'linux',
        stack: 'docker',
        ports: [ordererBasePort + ordererIndex * 10],
        storage_profile: stage === 'prod' ? 'persistent-ssd' : 'persistent-standard',
        resources: { ...resourceProfile.orderer },
      });
    }

    nodes.push({
      node_id: `ca-${organization.orgId}`,
      org_id: organization.orgId,
      node_type: 'ca',
      host_ref: organization.caHostRef || organization.peerHostRef || organization.ordererHostRef,
      provider: 'external',
      os_family: 'linux',
      stack: 'docker',
      ports: [organization.caPort],
      storage_profile: 'persistent-standard',
      resources: { ...resourceProfile.ca },
    });
  });

  const orgs = normalizedOrganizations.map(organization => ({
    org_id: organization.orgId,
    display_name: organization.displayName,
    msp_id: `${organization.orgId.charAt(0).toUpperCase()}${organization.orgId.slice(1)}MSP`,
    domain: organization.domain,
    roles: [
      ...(organization.peers > 0 ? ['peer'] : []),
      ...(organization.orderers > 0 ? ['orderer'] : []),
      'ca',
    ],
    identity: buildOrgIdentity({ orgId: organization.orgId }),
  }));

  const peerNodeByOrg = nodes
    .filter(node => node.node_type === 'peer')
    .reduce((accumulator, node) => {
      if (!accumulator[node.org_id]) {
        accumulator[node.org_id] = node.node_id;
      }
      return accumulator;
    }, {});

  const normalizedChannels = (channels || []).map((channel, index) => {
    const channelId = toSlug(channel.name) || `channel-${index + 1}`;
    const explicitMembers = sanitizeText(channel.memberOrgs)
      .split(',')
      .map(member => toSlug(member))
      .filter(Boolean);
    const members =
      explicitMembers.length > 0
        ? Array.from(new Set(explicitMembers))
        : normalizedOrganizations.map(organization => organization.orgId).filter(Boolean);

    const anchorPeers = members
      .map(member => peerNodeByOrg[member])
      .filter(Boolean)
      .map(nodeId => ({ node_id: nodeId }));

    return {
      channel_id: channelId,
      type: 'business',
      members,
      anchor_peers: anchorPeers,
      capabilities: {
        application: 'V2_5',
      },
      policy_base: {
        readers: 'ANY Readers',
        writers: 'MAJORITY Writers',
        admins: 'MAJORITY Admins',
      },
    };
  });

  const normalizedInstalls = (chaincodeInstalls || []).map((install, index) => ({
    install_id: sanitizeText(install.id) || `install-${index + 1}`,
    channel_id: toSlug(install.channel),
    chaincode_id:
      sanitizeText(install.chaincodeName) ||
      sanitizeText(install.chaincodeId) ||
      `chaincode-${index + 1}`,
    install_only: true,
    api_endpoint: sanitizeText(install.endpointPath),
    package_pattern: sanitizeText(install.packagePattern),
    package_file_name: sanitizeText(install.packageFileName),
    artifact_ref: sanitizeText(install.artifactRef || install.artifact_ref),
  }));

  const policies = [
    {
      policy_id: 'policy-channel-participation',
      scope: 'channel',
      policy_version: '1.0.0',
      schema_ref: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
      target: {
        channel_ids: normalizedChannels.map(channel => channel.channel_id),
      },
      rules: {
        allow: ['channel.join', 'lifecycle.install', 'lifecycle.approve', 'lifecycle.commit'],
        deny: ['lifecycle.force-commit'],
      },
      approvals: {
        quorum: 1,
      },
      constraints: {
        provider_key: PROVISIONING_INFRA_PROVIDER_KEY,
      },
    },
  ];

  const businessGroupName = sanitizeText(businessGroup?.name);
  const networkId = toSlug(businessGroup?.networkId || businessGroupName || 'cognus-network');

  return {
    schema_name: 'cognus-blueprint',
    schema_version: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
    blueprint_version: '1.0.0',
    created_at: now,
    updated_at: now,
    network: {
      network_id: networkId,
      display_name: businessGroupName || 'COGNUS Network',
      domain: `${networkId}.cognus.local`,
      business_group: {
        name: businessGroupName || 'COGNUS Business Group',
        description: sanitizeText(businessGroup?.description),
      },
      provisioning_scope: {
        provider_key: PROVISIONING_INFRA_PROVIDER_KEY,
        change_id: sanitizeText(changeId),
      },
      chaincodes_install: normalizedInstalls,
    },
    orgs,
    channels: normalizedChannels,
    nodes,
    policies,
    environment_profile: buildEnvironmentProfile({ environmentProfile, stage }),
    identity_baseline: {
      baseline_version: '1.0.0',
      schema_ref: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
      org_crypto_profiles: normalizedOrganizations.map(organization => ({
        org_id: organization.orgId,
        signing_ca: {
          algorithm: 'ecdsa',
          key_size: 256,
          validity_days: 365,
          rotation_days: 90,
          storage_ref: `vault://ca/signing/${organization.orgId}`,
          credential_ref: organization.caPasswordRef,
        },
        tls_ca: {
          algorithm: 'ecdsa',
          key_size: 256,
          validity_days: 365,
          rotation_days: 90,
          storage_ref: `vault://ca/tls/${organization.orgId}`,
          credential_ref: organization.caPasswordRef,
        },
      })),
    },
  };
};

const buildMachineFromHostRef = (hostRef, index) => ({
  id: `machine-${index + 1}`,
  infraLabel: sanitizeText(hostRef) || `machine-${index + 1}`,
  hostAddress: sanitizeText(hostRef),
  sshUser: '',
  sshPort: DEFAULT_SSH_PORT,
  authMethod: 'key',
  dockerPort: null,
});

export const importGuidedTopologyFromBlueprint = blueprint => {
  if (!isPlainObject(blueprint)) {
    throw new Error(
      pickCognusText(
        'Blueprint inválido para importação guiada.',
        'Invalid blueprint for guided import.',
        resolveCognusLocale()
      )
    );
  }

  const orgs = Array.isArray(blueprint.orgs) ? blueprint.orgs : [];
  const nodes = Array.isArray(blueprint.nodes) ? blueprint.nodes : [];
  const channels = Array.isArray(blueprint.channels) ? blueprint.channels : [];
  const network = isPlainObject(blueprint.network) ? blueprint.network : {};

  const hostRefs = Array.from(
    new Set(nodes.map(node => sanitizeText(node.host_ref)).filter(Boolean))
  );

  const machines =
    hostRefs.length > 0 ? hostRefs.map(buildMachineFromHostRef) : [buildMachineFromHostRef('', 0)];

  const organizations = orgs.map((org, index) => {
    const orgId = sanitizeText(org.org_id) || `org-${index + 1}`;
    const orgNodes = nodes.filter(node => sanitizeText(node.org_id) === orgId);
    const peerNodes = orgNodes.filter(node => sanitizeText(node.node_type) === 'peer');
    const ordererNodes = orgNodes.filter(node => sanitizeText(node.node_type) === 'orderer');
    const caNode = orgNodes.find(node => sanitizeText(node.node_type) === 'ca');

    return {
      id: `org-${index + 1}`,
      name: sanitizeText(org.display_name) || orgId.toUpperCase(),
      domain: sanitizeText(org.domain),
      label: toSlug(orgId) ? `org-${toSlug(orgId)}` : `org-${index + 1}`,
      networkApiHost: `api.${sanitizeText(org.domain) || orgId}`,
      networkApiPort: null,
      caMode: 'internal',
      caName: `ca-${orgId}`,
      caHost: sanitizeText(caNode?.host_ref) || `ca.${orgId}`,
      caHostRef: sanitizeText(caNode?.host_ref),
      caPort: normalizePort(caNode?.ports?.[0], DEFAULT_CA_PORT),
      caUser: '',
      caPasswordRef: '',
      peers: peerNodes.length,
      orderers: ordererNodes.length,
      peerHostRef: sanitizeText(peerNodes[0]?.host_ref),
      ordererHostRef: sanitizeText(ordererNodes[0]?.host_ref),
    };
  });

  const importedChannels = channels.map((channel, index) => ({
    id: `channel-${index + 1}`,
    name: sanitizeText(channel.channel_id),
    memberOrgs: (Array.isArray(channel.members) ? channel.members : []).join(','),
  }));

  const chaincodeInstalls = Array.isArray(network.chaincodes_install)
    ? network.chaincodes_install.map((install, index) => ({
        id: sanitizeText(install.install_id) || `install-${index + 1}`,
        channel: sanitizeText(install.channel_id),
        chaincodeName: sanitizeText(install.chaincode_id),
        endpointPath: sanitizeText(install.api_endpoint),
        packagePattern: sanitizeText(install.package_pattern),
        packageFileName: sanitizeText(install.package_file_name),
        artifactRef: sanitizeText(install.artifact_ref),
        uploadStatus: sanitizeText(install.artifact_ref) ? 'uploaded' : 'idle',
      }))
    : [];

  return {
    changeId: sanitizeText(network?.provisioning_scope?.change_id),
    environmentProfile:
      sanitizeText(blueprint?.environment_profile?.profile_id) || 'dev-external-linux',
    machines,
    organizations,
    channels: importedChannels,
    chaincodeInstalls,
    businessGroupName:
      sanitizeText(network?.business_group?.name) || sanitizeText(network?.display_name),
    businessGroupDescription: sanitizeText(network?.business_group?.description),
    businessGroupNetworkId: sanitizeText(network?.network_id),
  };
};
