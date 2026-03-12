import { DEFAULT_DOCKER_PORT } from './provisioningInfrastructureUtils';

const ONBOARDING_RUNBOOK_HANDOFF_STORAGE_KEY = 'cognus.provisioning.onboarding.runbook.handoff.v1';
const ONBOARDING_RUNBOOK_HANDOFF_TRAIL_STORAGE_KEY =
  'cognus.provisioning.onboarding.runbook.handoff.trail.v1';
const ONBOARDING_RUNBOOK_HANDOFF_TRAIL_MAX_ENTRIES = 30;
const ONBOARDING_RUNBOOK_HANDOFF_TRAIL_COMPACT_MAX_ENTRIES = 10;
const ONBOARDING_RUNBOOK_HANDOFF_TRAIL_MINIMAL_MAX_ENTRIES = 5;
const ONBOARDING_RUNBOOK_HANDOFF_CONTRACT_VERSION = 'a2.frontend-handoff.v1';
const OFFICIAL_CONTEXT_SOURCE_GUIDED_UI_DRAFT = 'guided_ui_draft';
const OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH = 'official_backend_publish';
const OFFICIAL_BACKEND_STATE_READY = 'ready';
const OFFICIAL_BACKEND_STATE_PENDING = 'pending';
const OFFICIAL_BACKEND_STATE_INVALID = 'invalid';

export const ONBOARDING_A2_2_MINIMUM_ARTIFACT_KEYS = Object.freeze([
  'provision-plan',
  'reconcile-report',
  'inventory-final',
  'stage-reports',
  'verify-report',
  'ssh-execution-log',
]);
const DEFAULT_RUNTIME_IMAGE_BY_NODE_TYPE = Object.freeze({
  peer: 'hyperledger/fabric-peer:2.5',
  orderer: 'hyperledger/fabric-orderer:2.5',
  ca: 'hyperledger/fabric-ca:1.5',
  couch: 'couchdb:3.2.2',
  apigateway: 'cognus/chaincode-gateway:latest',
  netapi: 'cognus/chaincode-gateway:latest',
  chaincode: 'hyperledger/fabric-ccenv:2.5',
});

const normalizeText = value => String(value || '').trim();
const normalizeLower = value => normalizeText(value).toLowerCase();
const normalizeRuntimeNodeType = value => {
  const normalized = normalizeLower(value);
  if (
    normalized === 'api_gateway' ||
    normalized === 'api-gateway' ||
    normalized === 'api gateway'
  ) {
    return 'apigateway';
  }
  if (
    normalized === 'network_api' ||
    normalized === 'network-api' ||
    normalized === 'network api'
  ) {
    return 'netapi';
  }
  return normalized;
};
const resolveDefaultRuntimeImageForNodeType = nodeType =>
  DEFAULT_RUNTIME_IMAGE_BY_NODE_TYPE[normalizeRuntimeNodeType(nodeType)] || '';

const normalizeToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');

const normalizeSlug = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

const normalizePositiveInt = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizePort = value => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    return null;
  }
  return parsed;
};

const buildSortKey = (...parts) =>
  parts
    .map(part => normalizeText(part))
    .join('|')
    .toLowerCase();

const sortByKey = (rows, keyResolver) =>
  [...rows].sort((left, right) => keyResolver(left).localeCompare(keyResolver(right)));

const canonicalJson = value => {
  if (Array.isArray(value)) {
    return value.map(item => canonicalJson(item));
  }
  if (value && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce((accumulator, key) => {
        accumulator[key] = canonicalJson(value[key]);
        return accumulator;
      }, {});
  }
  return value;
};

const stableStringify = value => JSON.stringify(canonicalJson(value));

const seededHashHex = (input, seed) => {
  const text = String(input || '');
  let hash = Number(seed) || 0;
  hash %= 4294967296;
  if (hash < 0) {
    hash += 4294967296;
  }
  for (let index = 0; index < text.length; index += 1) {
    hash = (Math.imul(hash, 1664525) + text.charCodeAt(index) + 1013904223) % 4294967296;
    if (hash < 0) {
      hash += 4294967296;
    }
  }
  return hash.toString(16).padStart(8, '0');
};

const deterministicFingerprint = payload => {
  const canonicalPayload = stableStringify(payload);
  return [
    seededHashHex(canonicalPayload, 0x811c9dc5),
    seededHashHex(canonicalPayload, 0x27d4eb2f),
    seededHashHex(canonicalPayload, 0x9e3779b1),
    seededHashHex(canonicalPayload, 0xc2b2ae35),
    seededHashHex(canonicalPayload, 0x165667b1),
    seededHashHex(canonicalPayload, 0x85ebca6b),
    seededHashHex(canonicalPayload, 0x27d4eb2d),
    seededHashHex(canonicalPayload, 0x94d049bb),
  ].join('');
};

const normalizeRoutePrefix = (value, fallback) => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return normalizeText(fallback);
  }
  const prefixed = normalized.startsWith('/') ? normalized : `/${normalized}`;
  return prefixed.replace(/\/{2,}/g, '/');
};

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const resolveContextSource = handoff => {
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const explicitContextSource = normalizeText(safeHandoff.context_source);
  if (explicitContextSource) {
    return explicitContextSource;
  }
  const hasRunCorrelation =
    Boolean(normalizeText(safeHandoff.run_id)) &&
    Boolean(
      normalizeText(safeHandoff.manifest_fingerprint) ||
        normalizeText(safeHandoff.source_blueprint_fingerprint)
    );
  const hasA2Contract = normalizeText(safeHandoff.handoff_contract_version).startsWith('a2.');
  if (hasRunCorrelation && hasA2Contract) {
    return OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH;
  }
  const normalizedSource = normalizeText(safeHandoff.source).toLowerCase();
  if (normalizedSource.includes('a2.5') || normalizedSource.includes('official_backend_publish')) {
    return OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH;
  }
  return OFFICIAL_CONTEXT_SOURCE_GUIDED_UI_DRAFT;
};

const resolveBackendStateFallbackByContextSource = contextSource =>
  normalizeText(contextSource).toLowerCase() === OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH
    ? OFFICIAL_BACKEND_STATE_READY
    : OFFICIAL_BACKEND_STATE_PENDING;

const normalizeBackendState = (value, fallback = OFFICIAL_BACKEND_STATE_PENDING) => {
  const normalized = normalizeText(value).toLowerCase();
  if (
    normalized === OFFICIAL_BACKEND_STATE_READY ||
    normalized === OFFICIAL_BACKEND_STATE_PENDING ||
    normalized === OFFICIAL_BACKEND_STATE_INVALID
  ) {
    return normalized;
  }
  const normalizedFallback = normalizeText(fallback).toLowerCase();
  if (
    normalizedFallback === OFFICIAL_BACKEND_STATE_READY ||
    normalizedFallback === OFFICIAL_BACKEND_STATE_PENDING ||
    normalizedFallback === OFFICIAL_BACKEND_STATE_INVALID
  ) {
    return normalizedFallback;
  }
  return OFFICIAL_BACKEND_STATE_PENDING;
};

const normalizeArtifactToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/\\/g, '/')
    .replace(/\.json$/g, '');

const normalizeArtifactCatalog = artifacts =>
  (Array.isArray(artifacts) ? artifacts : [])
    .map(item => {
      if (typeof item === 'string') {
        return item;
      }
      if (item && typeof item === 'object') {
        return item.key || item.path || item.artifact || item.name || '';
      }
      return '';
    })
    .map(normalizeArtifactToken)
    .filter(Boolean)
    .filter((token, index, allTokens) => allTokens.indexOf(token) === index);

const resolveMissingRequiredArtifacts = (requiredArtifacts, availableArtifacts) =>
  requiredArtifacts.filter(
    requiredToken =>
      !availableArtifacts.some(availableToken => availableToken.includes(requiredToken))
  );

const shouldDefaultArtifactsReadyForOfficialContext = ({
  contextSource = '',
  explicitReady = null,
  requiredArtifacts = [],
  availableArtifacts = [],
  missingArtifacts = [],
} = {}) => {
  if (typeof explicitReady === 'boolean') {
    return false;
  }
  if (normalizeText(contextSource).toLowerCase() !== OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH) {
    return false;
  }
  const normalizedRequiredArtifacts = normalizeArtifactCatalog(requiredArtifacts);
  const normalizedAvailableArtifacts = normalizeArtifactCatalog(availableArtifacts);
  const normalizedMissingArtifacts = normalizeArtifactCatalog(missingArtifacts);
  return (
    normalizedRequiredArtifacts.length === 0 &&
    normalizedAvailableArtifacts.length === 0 &&
    normalizedMissingArtifacts.length === 0
  );
};

const firstNonEmptyText = (...values) =>
  values.map(value => normalizeText(value)).find(Boolean) || '';

const firstArrayValue = (...values) => values.find(Array.isArray) || [];

const resolveA2ArtifactsState = ({
  requiredArtifacts = [],
  availableArtifacts = [],
  missingArtifacts = [],
  explicitReady = null,
  defaultToReady = false,
} = {}) => {
  const normalizedRequiredArtifacts = normalizeArtifactCatalog(requiredArtifacts);
  const effectiveRequiredArtifacts =
    normalizedRequiredArtifacts.length > 0
      ? normalizedRequiredArtifacts
      : normalizeArtifactCatalog(ONBOARDING_A2_2_MINIMUM_ARTIFACT_KEYS);
  const normalizedAvailableArtifacts = normalizeArtifactCatalog(availableArtifacts);
  const normalizedMissingArtifacts = normalizeArtifactCatalog(missingArtifacts);
  let effectiveExplicitReady = null;
  if (typeof explicitReady === 'boolean') {
    effectiveExplicitReady = Boolean(explicitReady);
  } else if (defaultToReady) {
    effectiveExplicitReady = true;
  }

  let effectiveAvailableArtifacts = normalizedAvailableArtifacts;
  if (effectiveAvailableArtifacts.length === 0 && effectiveExplicitReady) {
    effectiveAvailableArtifacts = [...effectiveRequiredArtifacts];
  }

  let effectiveMissingArtifacts = normalizedMissingArtifacts;
  if (effectiveExplicitReady) {
    effectiveMissingArtifacts = [];
  } else if (effectiveMissingArtifacts.length === 0) {
    effectiveMissingArtifacts = resolveMissingRequiredArtifacts(
      effectiveRequiredArtifacts,
      effectiveAvailableArtifacts
    );
  }

  const artifactsReady =
    effectiveExplicitReady !== null
      ? Boolean(effectiveExplicitReady)
      : effectiveMissingArtifacts.length === 0 && effectiveAvailableArtifacts.length > 0;

  return {
    requiredArtifacts: effectiveRequiredArtifacts,
    availableArtifacts: effectiveAvailableArtifacts,
    missingArtifacts: effectiveMissingArtifacts,
    artifactsReady,
  };
};

const normalizeHostMappingEntries = hostMapping =>
  sortByKey(Array.isArray(hostMapping) ? hostMapping : [], entry =>
    buildSortKey(entry && entry.org_id, entry && entry.node_type, entry && entry.node_id)
  );

const normalizeMachineCredentialEntries = machineCredentials =>
  sortByKey(
    (Array.isArray(machineCredentials) ? machineCredentials : []).map(entry => ({
      machine_id: normalizeText(entry && entry.machine_id),
      credential_ref: normalizeText(entry && entry.credential_ref),
      credential_payload: normalizeText(entry && entry.credential_payload),
      credential_fingerprint: normalizeText(entry && entry.credential_fingerprint),
      reuse_confirmed: Boolean(entry && entry.reuse_confirmed),
    })),
    entry => buildSortKey(entry.machine_id, entry.credential_ref, entry.credential_fingerprint)
  );

const normalizeChaincodeInstallEntries = chaincodeInstalls =>
  sortByKey(
    (Array.isArray(chaincodeInstalls) ? chaincodeInstalls : [])
      .map((install, index) => {
        const channelId = normalizeText(install && (install.channel_id || install.channel));
        const chaincodeId =
          normalizeText(
            install &&
              (install.chaincode_id ||
                install.chaincodeId ||
                install.chaincode_name ||
                install.chaincodeName ||
                install.name)
          ) || `chaincode-${index + 1}`;
        const fallbackInstallId =
          normalizeSlug(
            [channelId, chaincodeId]
              .map(value => normalizeText(value))
              .filter(Boolean)
              .join('-')
          ) || `install-${index + 1}`;
        return {
          install_id:
            normalizeText(install && (install.install_id || install.id)) || fallbackInstallId,
          channel_id: channelId,
          chaincode_id: chaincodeId,
          install_only:
            typeof (install && install.install_only) === 'boolean'
              ? Boolean(install.install_only)
              : true,
          api_endpoint: normalizeText(install && (install.api_endpoint || install.endpointPath)),
          package_pattern: normalizeText(
            install && (install.package_pattern || install.packagePattern)
          ),
          package_file_name: normalizeText(
            install && (install.package_file_name || install.packageFileName)
          ),
          artifact_ref: normalizeText(install && (install.artifact_ref || install.artifactRef)),
          source_ref: normalizeText(install && (install.source_ref || install.sourceRef)),
          package_id: normalizeText(install && (install.package_id || install.packageId)),
          package_label: normalizeText(install && (install.package_label || install.packageLabel)),
          package_language: normalizeText(
            install && (install.package_language || install.packageLanguage)
          ),
        };
      })
      .filter(entry => entry.channel_id && entry.chaincode_id),
    entry =>
      buildSortKey(
        entry && entry.channel_id,
        entry && entry.chaincode_id,
        entry && entry.install_id,
        entry && entry.package_file_name,
        entry && entry.artifact_ref
      )
  );

const extractTopologyCatalogFromPayload = payload => {
  const safePayload = payload && typeof payload === 'object' ? payload : {};
  if (safePayload.topology_catalog && typeof safePayload.topology_catalog === 'object') {
    return safePayload.topology_catalog;
  }
  const handoffPayload =
    safePayload.handoff_payload && typeof safePayload.handoff_payload === 'object'
      ? safePayload.handoff_payload
      : {};
  if (handoffPayload.topology_catalog && typeof handoffPayload.topology_catalog === 'object') {
    return handoffPayload.topology_catalog;
  }
  return {};
};

const extractHostMappingFromPayload = payload => {
  const safePayload = payload && typeof payload === 'object' ? payload : {};
  if (Array.isArray(safePayload.host_mapping) && safePayload.host_mapping.length > 0) {
    return safePayload.host_mapping;
  }
  const handoffPayload =
    safePayload.handoff_payload && typeof safePayload.handoff_payload === 'object'
      ? safePayload.handoff_payload
      : {};
  if (Array.isArray(handoffPayload.host_mapping) && handoffPayload.host_mapping.length > 0) {
    return handoffPayload.host_mapping;
  }
  return [];
};

const extractMachineCredentialsFromPayload = payload => {
  const safePayload = payload && typeof payload === 'object' ? payload : {};
  if (
    Array.isArray(safePayload.machine_credentials) &&
    safePayload.machine_credentials.length > 0
  ) {
    return safePayload.machine_credentials;
  }
  const handoffPayload =
    safePayload.handoff_payload && typeof safePayload.handoff_payload === 'object'
      ? safePayload.handoff_payload
      : {};
  if (
    Array.isArray(handoffPayload.machine_credentials) &&
    handoffPayload.machine_credentials.length > 0
  ) {
    return handoffPayload.machine_credentials;
  }
  return [];
};

const buildTopologyOrganizationAliasRegistry = topologyCatalog => {
  const safeTopology =
    topologyCatalog && typeof topologyCatalog === 'object' ? topologyCatalog : {};
  const organizations = Array.isArray(safeTopology.organizations) ? safeTopology.organizations : [];
  const registry = {};

  const registerAlias = (aliasValue, canonicalOrgId) => {
    const alias = normalizeText(aliasValue);
    const canonical = normalizeText(canonicalOrgId);
    if (!alias || !canonical) {
      return;
    }
    registry[alias.toLowerCase()] = canonical;
    const slugAlias = normalizeSlug(alias);
    if (slugAlias) {
      registry[slugAlias] = canonical;
    }
    const tokenAlias = normalizeToken(alias);
    if (tokenAlias) {
      registry[tokenAlias] = canonical;
    }
  };

  organizations.forEach(organization => {
    const canonicalOrgId =
      normalizeText(organization && organization.org_id) ||
      normalizeText(organization && organization.org_name) ||
      normalizeText(organization && organization.org_key);
    if (!canonicalOrgId) {
      return;
    }
    registerAlias(canonicalOrgId, canonicalOrgId);
    registerAlias(organization && organization.org_name, canonicalOrgId);
    registerAlias(organization && organization.org_key, canonicalOrgId);
    registerAlias(organization && organization.domain, canonicalOrgId);
  });

  return registry;
};

const resolveCanonicalOrgIdFromTopology = (orgId, topologyAliasRegistry) => {
  const normalizedOrgId = normalizeText(orgId);
  if (!normalizedOrgId) {
    return '';
  }
  const lowerOrgId = normalizedOrgId.toLowerCase();
  if (topologyAliasRegistry[lowerOrgId]) {
    return topologyAliasRegistry[lowerOrgId];
  }
  const slugOrgId = normalizeSlug(normalizedOrgId);
  if (slugOrgId && topologyAliasRegistry[slugOrgId]) {
    return topologyAliasRegistry[slugOrgId];
  }
  const tokenOrgId = normalizeToken(normalizedOrgId);
  if (tokenOrgId && topologyAliasRegistry[tokenOrgId]) {
    return topologyAliasRegistry[tokenOrgId];
  }
  return normalizedOrgId;
};

const canonicalizeHostMappingOrganizationIds = (hostMapping, topologyCatalog) => {
  const topologyAliasRegistry = buildTopologyOrganizationAliasRegistry(topologyCatalog);
  const rows = Array.isArray(hostMapping) ? hostMapping : [];
  if (Object.keys(topologyAliasRegistry).length === 0) {
    return normalizeHostMappingEntries(rows);
  }
  return normalizeHostMappingEntries(
    rows.map(row => {
      const safeRow = row && typeof row === 'object' ? row : {};
      return {
        ...safeRow,
        org_id: resolveCanonicalOrgIdFromTopology(safeRow.org_id, topologyAliasRegistry),
      };
    })
  );
};

const normalizeApiRegistryEntries = apiRegistry =>
  sortByKey(Array.isArray(apiRegistry) ? apiRegistry : [], entry =>
    buildSortKey(
      entry && entry.org_name,
      entry && entry.org_id,
      entry && entry.channel_id,
      entry && entry.chaincode_id,
      entry && entry.api_id,
      entry && entry.route_path
    )
  );

const normalizeIncrementalExpansionEntries = incrementalExpansions =>
  sortByKey(Array.isArray(incrementalExpansions) ? incrementalExpansions : [], entry =>
    buildSortKey(
      entry && entry.expansion_id,
      entry && entry.change_id,
      entry && entry.run_id,
      entry && entry.operation_type
    )
  );

const normalizeOnboardingAuditEntries = events =>
  sortByKey(
    (Array.isArray(events) ? events : []).map(event => ({
      code: normalizeText(event && event.code),
      timestamp_utc: normalizeText(event && event.timestamp_utc),
    })),
    event => buildSortKey(event.timestamp_utc, event.code)
  );

const buildMachineRegistry = machines => {
  const registry = {};

  (Array.isArray(machines) ? machines : []).forEach(machine => {
    const infraLabel = normalizeText(machine && machine.infraLabel);
    if (!infraLabel) {
      return;
    }
    registry[infraLabel] = {
      infra_label: infraLabel,
      host_address: normalizeText(machine.hostAddress),
      ssh_user: normalizeText(machine.sshUser),
      ssh_port: normalizePort(machine.sshPort) || 22,
      docker_port: normalizePort(machine.dockerPort) || DEFAULT_DOCKER_PORT,
    };
  });

  return registry;
};

const buildOrganizationTopologyCatalog = ({
  organizations,
  channels,
  chaincodeInstalls,
  businessGroupName,
  businessGroupDescription,
  businessGroupNetworkId,
  hostMapping,
  apiRegistry,
  machineRegistry,
}) => {
  const hostRegistry = { ...(machineRegistry || {}) };
  const registry = new Map();
  const aliasMap = new Map();
  const channelById = new Map();
  const chaincodesByChannel = new Map();
  const chaincodeMetadataByPair = new Map();
  const normalizedChaincodeInstalls = normalizeChaincodeInstallEntries(chaincodeInstalls);

  const registerAlias = (value, organizationKey) => {
    const aliasToken = normalizeToken(value);
    if (!aliasToken) {
      return;
    }
    aliasMap.set(aliasToken, organizationKey);
  };

  const resolveOrganizationKeyByAlias = value => {
    const aliasToken = normalizeToken(value);
    if (!aliasToken) {
      return '';
    }
    return aliasMap.get(aliasToken) || '';
  };

  const ensureOrganization = input => {
    const inputName = normalizeText(input && input.orgName);
    const inputId = normalizeText(input && input.orgId);
    const fallbackToken = normalizeSlug(inputName || inputId) || `org-${registry.size + 1}`;
    const organizationKey = resolveOrganizationKeyByAlias(inputName || inputId) || fallbackToken;
    if (!registry.has(organizationKey)) {
      registry.set(organizationKey, {
        org_key: organizationKey,
        org_id: inputId || organizationKey,
        org_name: inputName || inputId || organizationKey,
        domain: '',
        peer_host_ref: '',
        orderer_host_ref: '',
        ca: {
          mode: '',
          name: '',
          host: '',
          port: null,
          user: '',
          host_ref: '',
        },
        network_api: {
          host: '',
          port: null,
          exposure_ip: '',
          status_hint: '',
        },
        service_host_mapping: {
          peer: '',
          orderer: '',
          ca: '',
          couch: '',
          apiGateway: '',
          netapi: '',
        },
        service_parameters: {
          peer: {
            count: 0,
            port_base: null,
          },
          orderer: {
            count: 0,
            port_base: null,
          },
          ca: {
            mode: '',
            name: '',
            host: '',
            host_ref: '',
            port: null,
            user: '',
            password_ref: '',
          },
          couch: {
            host_ref: '',
            port: null,
            database: '',
            admin_user: '',
            admin_password_ref: '',
          },
          apiGateway: {
            host_ref: '',
            port: null,
            route_prefix: '',
            auth_ref: '',
          },
          netapi: {
            host: '',
            host_ref: '',
            port: null,
            route_prefix: '',
            access_ref: '',
          },
        },
        peers: [],
        orderers: [],
        cas: [],
        channels: new Set(),
        chaincodes: new Set(),
        apis: [],
      });
    }

    const organization = registry.get(organizationKey);
    if (inputName && !organization.org_name) {
      organization.org_name = inputName;
    }
    if (inputId && !organization.org_id) {
      organization.org_id = inputId;
    }

    registerAlias(organization.org_name, organizationKey);
    registerAlias(organization.org_id, organizationKey);
    registerAlias(normalizeSlug(organization.org_name), organizationKey);
    registerAlias(normalizeSlug(organization.org_id), organizationKey);
    return organization;
  };

  (Array.isArray(channels) ? channels : []).forEach(channel => {
    const channelId = normalizeText(channel && channel.name);
    if (!channelId) {
      return;
    }
    const memberOrgs = normalizeText(channel && channel.memberOrgs)
      .split(',')
      .map(value => normalizeText(value))
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right));
    channelById.set(channelId, {
      channel_id: channelId,
      member_orgs: memberOrgs,
      chaincodes: [],
    });
  });

  normalizedChaincodeInstalls.forEach(chaincode => {
    const chaincodeId = normalizeText(chaincode && chaincode.chaincode_id);
    const channelId = normalizeText(chaincode && chaincode.channel_id);
    if (!chaincodeId || !channelId) {
      return;
    }
    if (!chaincodesByChannel.has(channelId)) {
      chaincodesByChannel.set(channelId, new Set());
    }
    chaincodesByChannel.get(channelId).add(chaincodeId);
    chaincodeMetadataByPair.set(buildSortKey(channelId, chaincodeId), {
      ...chaincode,
      channel_id: channelId,
      chaincode_id: chaincodeId,
    });
  });

  (Array.isArray(organizations) ? organizations : []).forEach(organization => {
    const orgName = normalizeText(organization && organization.name);
    const orgId = normalizeText(organization && organization.label);
    const orgEntry = ensureOrganization({ orgName, orgId });
    orgEntry.domain = normalizeText(organization && organization.domain);
    orgEntry.peer_host_ref = normalizeText(organization && organization.peerHostRef);
    orgEntry.orderer_host_ref = normalizeText(organization && organization.ordererHostRef);
    const caHostRef =
      normalizeText(organization && organization.caHostRef) ||
      orgEntry.peer_host_ref ||
      orgEntry.orderer_host_ref;
    const couchHostRef =
      normalizeText(organization && organization.couchHostRef) || orgEntry.peer_host_ref;
    const apiGatewayHostRef =
      normalizeText(organization && organization.apiGatewayHostRef) || orgEntry.peer_host_ref;
    const netApiHostRef =
      normalizeText(organization && organization.netApiHostRef) || apiGatewayHostRef;
    orgEntry.service_host_mapping = {
      peer: orgEntry.peer_host_ref,
      orderer: orgEntry.orderer_host_ref,
      ca: caHostRef,
      couch: couchHostRef,
      apiGateway: apiGatewayHostRef,
      netapi: netApiHostRef,
    };
    orgEntry.service_parameters = {
      peer: {
        count: normalizePositiveInt(organization && organization.peers),
        port_base: normalizePort(organization && organization.peerPortBase),
      },
      orderer: {
        count: normalizePositiveInt(organization && organization.orderers),
        port_base: normalizePort(organization && organization.ordererPortBase),
      },
      ca: {
        mode: normalizeText(organization && organization.caMode),
        name: normalizeText(organization && organization.caName),
        host: normalizeText(organization && organization.caHost),
        host_ref: caHostRef,
        port: normalizePort(organization && organization.caPort),
        user: normalizeText(organization && organization.caUser),
        password_ref: normalizeText(organization && organization.caPasswordRef),
      },
      couch: {
        host_ref: couchHostRef,
        port: normalizePort(organization && organization.couchPort),
        database: normalizeText(organization && organization.couchDatabase),
        admin_user: normalizeText(organization && organization.couchAdminUser),
        admin_password_ref: normalizeText(organization && organization.couchAdminPasswordRef),
      },
      apiGateway: {
        host_ref: apiGatewayHostRef,
        port: normalizePort(organization && organization.apiGatewayPort),
        route_prefix: normalizeRoutePrefix(
          organization && organization.apiGatewayRoutePrefix,
          '/api'
        ),
        auth_ref: normalizeText(organization && organization.apiGatewayAuthRef),
      },
      netapi: {
        host: normalizeText(organization && organization.networkApiHost),
        host_ref: netApiHostRef,
        port: normalizePort(organization && organization.networkApiPort),
        route_prefix: normalizeRoutePrefix(
          organization && organization.netApiRoutePrefix,
          '/network'
        ),
        access_ref: normalizeText(organization && organization.netApiAccessRef),
      },
    };
    orgEntry.ca = {
      mode: normalizeText(organization && organization.caMode),
      name: normalizeText(organization && organization.caName),
      host: normalizeText(organization && organization.caHost),
      port: normalizePort(organization && organization.caPort),
      user: normalizeText(organization && organization.caUser),
      host_ref: caHostRef,
    };
    orgEntry.network_api = {
      host: normalizeText(organization && organization.networkApiHost),
      port: normalizePort(organization && organization.networkApiPort),
      exposure_ip: '',
      status_hint: '',
    };
  });

  (Array.isArray(hostMapping) ? hostMapping : []).forEach(node => {
    const orgEntry = ensureOrganization({
      orgName: normalizeText(node && node.org_name),
      orgId: normalizeText(node && node.org_id),
    });
    const hostRef = normalizeText(node && node.host_ref);
    const hostAddress = normalizeText(node && node.host_address);
    const nodeId = normalizeText(node && node.node_id);
    const nodeType = normalizeText(node && node.node_type).toLowerCase();

    if (hostRef && !hostRegistry[hostRef]) {
      hostRegistry[hostRef] = {
        infra_label: hostRef,
        host_address: hostAddress,
        ssh_user: normalizeText(node && node.ssh_user),
        ssh_port: normalizePort(node && node.ssh_port) || 22,
        docker_port: normalizePort(node && node.docker_port) || DEFAULT_DOCKER_PORT,
      };
    }

    const nodeEntry = {
      node_id: nodeId || `${nodeType || 'node'}-${orgEntry.org_id || orgEntry.org_name}`,
      node_type: nodeType || 'node',
      host_ref: hostRef,
      host_address:
        hostAddress || (hostRegistry[hostRef] && hostRegistry[hostRef].host_address) || '',
      status_hint: normalizeText(node && node.preflight_status) || 'unknown',
    };

    if (nodeType === 'peer') {
      orgEntry.peers.push(nodeEntry);
    } else if (nodeType === 'orderer') {
      orgEntry.orderers.push(nodeEntry);
    } else if (nodeType === 'ca') {
      orgEntry.cas.push(nodeEntry);
      if (!orgEntry.ca.host_ref) {
        orgEntry.ca.host_ref = hostRef;
      }
      if (!orgEntry.ca.host) {
        orgEntry.ca.host = nodeEntry.host_address;
      }
    }
  });

  (Array.isArray(apiRegistry) ? apiRegistry : []).forEach(api => {
    const orgEntry = ensureOrganization({
      orgName: normalizeText(api && api.org_name),
      orgId: normalizeText(api && api.org_id),
    });
    const channelId = normalizeText(api && api.channel_id);
    const chaincodeId = normalizeText(api && api.chaincode_id);
    if (channelId) {
      orgEntry.channels.add(channelId);
    }
    if (chaincodeId) {
      orgEntry.chaincodes.add(chaincodeId);
    }
    const exposureHost = normalizeText(api?.exposure?.host);
    const exposurePort = normalizePort(api?.exposure?.port);
    orgEntry.apis.push({
      api_id: normalizeText(api && api.api_id),
      channel_id: channelId,
      chaincode_id: chaincodeId,
      route_path: normalizeText(api && api.route_path),
      exposure: {
        host: exposureHost,
        port: exposurePort,
      },
      status_hint: normalizeText(api && api.status) || 'active',
    });
    if (exposureHost && !orgEntry.network_api.exposure_ip) {
      orgEntry.network_api.exposure_ip = exposureHost;
    }
    if (exposurePort && !orgEntry.network_api.port) {
      orgEntry.network_api.port = exposurePort;
    }
  });

  channelById.forEach(channelEntry => {
    const registeredChaincodes = chaincodesByChannel.get(channelEntry.channel_id);
    const channelEntryWithChaincodes = {
      ...channelEntry,
      chaincodes: registeredChaincodes ? Array.from(registeredChaincodes).sort() : [],
    };
    channelById.set(channelEntry.channel_id, channelEntryWithChaincodes);

    const resolvedOrgKeys = channelEntry.member_orgs
      .map(memberOrg => resolveOrganizationKeyByAlias(memberOrg))
      .filter(Boolean);
    let targetOrgKeys = resolvedOrgKeys;
    if (targetOrgKeys.length === 0 && registry.size === 1) {
      targetOrgKeys = [Array.from(registry.keys())[0]];
    }

    targetOrgKeys.forEach(orgKey => {
      const orgEntry = registry.get(orgKey);
      if (!orgEntry) {
        return;
      }
      orgEntry.channels.add(channelEntryWithChaincodes.channel_id);
      channelEntryWithChaincodes.chaincodes.forEach(chaincodeId => {
        orgEntry.chaincodes.add(chaincodeId);
      });
    });
  });

  const organizationsCatalog = Array.from(registry.values())
    .map(orgEntry => ({
      org_key: orgEntry.org_key,
      org_id: orgEntry.org_id,
      org_name: orgEntry.org_name,
      domain: orgEntry.domain,
      peer_host_ref: orgEntry.peer_host_ref,
      orderer_host_ref: orgEntry.orderer_host_ref,
      ca: {
        ...orgEntry.ca,
        host_ref: orgEntry.ca.host_ref || orgEntry.peer_host_ref || orgEntry.orderer_host_ref || '',
      },
      network_api: orgEntry.network_api,
      service_host_mapping: orgEntry.service_host_mapping,
      service_parameters: orgEntry.service_parameters,
      peers: sortByKey(orgEntry.peers, node =>
        buildSortKey(node && node.node_type, node && node.node_id, node && node.host_ref)
      ),
      orderers: sortByKey(orgEntry.orderers, node =>
        buildSortKey(node && node.node_type, node && node.node_id, node && node.host_ref)
      ),
      cas: sortByKey(orgEntry.cas, node =>
        buildSortKey(node && node.node_type, node && node.node_id, node && node.host_ref)
      ),
      channels: Array.from(orgEntry.channels).sort(),
      chaincodes: Array.from(orgEntry.chaincodes).sort(),
      apis: sortByKey(orgEntry.apis, api =>
        buildSortKey(
          api && api.org_name,
          api && api.channel_id,
          api && api.chaincode_id,
          api && api.api_id,
          api && api.route_path
        )
      ),
    }))
    .sort((left, right) => left.org_name.localeCompare(right.org_name));

  const normalizedChannels = sortByKey(Array.from(channelById.values()), channelEntry =>
    buildSortKey(channelEntry && channelEntry.channel_id)
  );
  const normalizedChaincodeRows = [];
  chaincodeMetadataByPair.forEach(chaincodeRow => {
    normalizedChaincodeRows.push(chaincodeRow);
  });
  Array.from(chaincodesByChannel.entries()).forEach(([channelId, chaincodeSet]) => {
    Array.from(chaincodeSet).forEach(chaincodeId => {
      const chaincodeKey = buildSortKey(channelId, chaincodeId);
      if (chaincodeMetadataByPair.has(chaincodeKey)) {
        return;
      }
      normalizedChaincodeRows.push({
        channel_id: channelId,
        chaincode_id: chaincodeId,
      });
    });
  });
  const normalizedChaincodes = sortByKey(normalizedChaincodeRows, chaincodeRow =>
    buildSortKey(
      chaincodeRow && chaincodeRow.channel_id,
      chaincodeRow && chaincodeRow.chaincode_id,
      chaincodeRow && chaincodeRow.install_id
    )
  );
  const normalizedTopologyApis = normalizeApiRegistryEntries(apiRegistry);
  const normalizedHosts = sortByKey(Object.values(hostRegistry), host =>
    buildSortKey(host && host.infra_label, host && host.host_address)
  );

  const businessGroup = {
    group_id: normalizeSlug(businessGroupNetworkId || businessGroupName) || 'business-group',
    name: normalizeText(businessGroupName) || 'Business Group',
    description: normalizeText(businessGroupDescription),
    network_id: normalizeText(businessGroupNetworkId),
    channels: normalizedChannels.map(channelEntry => ({
      channel_id: channelEntry.channel_id,
      member_orgs: channelEntry.member_orgs,
      chaincodes: channelEntry.chaincodes,
    })),
  };

  return {
    hosts: normalizedHosts,
    organizations: organizationsCatalog,
    business_groups: [businessGroup],
    channels: normalizedChannels,
    chaincodes: normalizedChaincodes,
    apis: normalizedTopologyApis,
    counts: {
      hosts: normalizedHosts.length,
      organizations: organizationsCatalog.length,
      peers: organizationsCatalog.reduce(
        (sum, org) => sum + normalizePositiveInt(org.peers.length),
        0
      ),
      orderers: organizationsCatalog.reduce(
        (sum, org) => sum + normalizePositiveInt(org.orderers.length),
        0
      ),
      apis: organizationsCatalog.reduce(
        (sum, org) => sum + normalizePositiveInt(org.apis.length),
        0
      ),
    },
  };
};

export const isPreflightApproved = preflightReport => {
  if (!preflightReport || typeof preflightReport !== 'object') {
    return false;
  }

  const overallStatus = normalizeText(preflightReport.overallStatus).toLowerCase();
  const hosts = Array.isArray(preflightReport.hosts) ? preflightReport.hosts : [];
  if (overallStatus !== 'apto' || hosts.length === 0) {
    return false;
  }

  return hosts.every(host => normalizeText(host.status).toLowerCase() === 'apto');
};

export const buildResolvedHostMapping = ({ blueprint, machines, preflightReport }) => {
  const safeBlueprint = blueprint && typeof blueprint === 'object' ? blueprint : {};
  const fallbackPreflightStatus =
    normalizeText(preflightReport && preflightReport.overallStatus).toLowerCase() || 'bloqueado';
  const machineRegistry = buildMachineRegistry(machines);
  const preflightHosts =
    preflightReport && Array.isArray(preflightReport.hosts) ? preflightReport.hosts : [];
  const preflightRegistry = preflightHosts.reduce((registry, host) => {
    const infraLabel = normalizeText(host.infraLabel);
    const hostRef = normalizeText(host.host_ref);
    const status = normalizeText(host.status).toLowerCase();
    const normalizedStatus = status || 'bloqueado';
    let nextRegistry = registry;
    if (infraLabel) {
      nextRegistry = {
        ...nextRegistry,
        [infraLabel]: normalizedStatus,
      };
    }
    if (hostRef) {
      nextRegistry = {
        ...nextRegistry,
        [hostRef]: normalizedStatus,
      };
    }
    return nextRegistry;
  }, {});
  const nodes = Array.isArray(safeBlueprint.nodes) ? safeBlueprint.nodes : [];

  return nodes
    .map(node => {
      const hostRef = normalizeText(node.host_ref);
      const hostMachine = machineRegistry[hostRef] || null;
      return {
        node_id: normalizeText(node.node_id),
        node_type: normalizeText(node.node_type),
        org_id: normalizeText(node.org_id),
        host_ref: hostRef,
        host_address: hostMachine ? hostMachine.host_address : '',
        ssh_user: hostMachine ? hostMachine.ssh_user : '',
        ssh_port: hostMachine ? hostMachine.ssh_port : 22,
        docker_port: hostMachine ? hostMachine.docker_port : DEFAULT_DOCKER_PORT,
        preflight_status: preflightRegistry[hostRef] || fallbackPreflightStatus,
      };
    })
    .filter(row => row.node_id && row.node_type && row.org_id)
    .sort((left, right) => {
      const leftKey = `${left.org_id}|${left.node_type}|${left.node_id}`;
      const rightKey = `${right.org_id}|${right.node_type}|${right.node_id}`;
      return leftKey.localeCompare(rightKey);
    });
};

const buildPreflightStatusRegistry = preflightReport => {
  const fallbackPreflightStatus =
    normalizeText(preflightReport && preflightReport.overallStatus).toLowerCase() || 'bloqueado';
  const hosts =
    preflightReport && Array.isArray(preflightReport.hosts) ? preflightReport.hosts : [];
  const registry = hosts.reduce((accumulator, host) => {
    const status = normalizeText(host && host.status).toLowerCase() || 'bloqueado';
    const infraLabel = normalizeText(host && host.infraLabel);
    const hostRef = normalizeText(host && host.host_ref);
    const hostAddress = normalizeText(host && host.host_address).toLowerCase();
    const nextRegistry = { ...accumulator };
    if (infraLabel) {
      nextRegistry[infraLabel] = status;
    }
    if (hostRef) {
      nextRegistry[hostRef] = status;
    }
    if (hostAddress) {
      nextRegistry[hostAddress] = status;
    }
    return nextRegistry;
  }, {});
  return {
    fallbackPreflightStatus,
    registry,
  };
};

const resolveMachineByHostRef = (machineRegistry, hostRef) => {
  const normalizedHostRef = normalizeText(hostRef);
  if (!normalizedHostRef) {
    return null;
  }
  if (machineRegistry[normalizedHostRef]) {
    return machineRegistry[normalizedHostRef];
  }
  const lowerHostRef = normalizedHostRef.toLowerCase();
  if (machineRegistry[lowerHostRef]) {
    return machineRegistry[lowerHostRef];
  }
  return null;
};

const resolvePreflightStatusForHost = ({
  hostRef = '',
  hostAddress = '',
  preflightStatusRegistry = {},
  fallbackPreflightStatus = 'bloqueado',
} = {}) => {
  const normalizedHostRef = normalizeText(hostRef);
  const normalizedHostAddress = normalizeText(hostAddress).toLowerCase();
  if (normalizedHostRef && preflightStatusRegistry[normalizedHostRef]) {
    return preflightStatusRegistry[normalizedHostRef];
  }
  if (normalizedHostAddress && preflightStatusRegistry[normalizedHostAddress]) {
    return preflightStatusRegistry[normalizedHostAddress];
  }
  return fallbackPreflightStatus;
};

const toRuntimeNodeId = (prefix, orgId, suffix = '') => {
  const normalizedPrefix = normalizeSlug(prefix) || 'runtime';
  const normalizedOrgId = normalizeSlug(orgId) || 'org';
  const normalizedSuffix = normalizeSlug(suffix);
  if (normalizedSuffix) {
    return `${normalizedPrefix}-${normalizedOrgId}-${normalizedSuffix}`;
  }
  return `${normalizedPrefix}-${normalizedOrgId}`;
};

const mergeHostMappingRows = (...collections) => {
  const registry = new Map();
  collections.forEach(collection => {
    (Array.isArray(collection) ? collection : []).forEach(row => {
      const safeRow = row && typeof row === 'object' ? row : {};
      const orgId = normalizeText(safeRow.org_id);
      const nodeType = normalizeRuntimeNodeType(safeRow.node_type);
      const nodeId = normalizeText(safeRow.node_id);
      const hostRef = normalizeText(safeRow.host_ref);
      const runtimeImage =
        normalizeText(safeRow.runtime_image) || resolveDefaultRuntimeImageForNodeType(nodeType);
      if (!orgId || !nodeType || !nodeId || !hostRef) {
        return;
      }
      const rowKey = buildSortKey(orgId, nodeType, nodeId);
      if (!registry.has(rowKey)) {
        registry.set(rowKey, {
          ...safeRow,
          org_id: orgId,
          node_type: nodeType,
          node_id: nodeId,
          host_ref: hostRef,
          runtime_image: runtimeImage,
        });
      }
    });
  });
  return sortByKey(Array.from(registry.values()), row =>
    buildSortKey(row && row.org_id, row && row.node_type, row && row.node_id)
  );
};

const collectTopologyChaincodesByChannel = topologyCatalog => {
  const safeTopology =
    topologyCatalog && typeof topologyCatalog === 'object' ? topologyCatalog : {};
  const chaincodesByChannel = new Map();

  const registerChannelChaincode = (channelId, chaincodeId) => {
    const normalizedChannelId = normalizeText(channelId);
    const normalizedChannelKey = normalizedChannelId.toLowerCase();
    const normalizedChaincodeId = normalizeText(chaincodeId);
    if (!normalizedChannelKey || !normalizedChaincodeId) {
      return;
    }
    if (!chaincodesByChannel.has(normalizedChannelKey)) {
      chaincodesByChannel.set(normalizedChannelKey, new Set());
    }
    chaincodesByChannel.get(normalizedChannelKey).add(normalizedChaincodeId);
  };

  const ingestChannels = channels => {
    (Array.isArray(channels) ? channels : []).forEach(channel => {
      if (!channel || typeof channel !== 'object') {
        return;
      }
      const channelId = normalizeText(channel.channel_id || channel.name || channel.id);
      if (!channelId) {
        return;
      }
      (Array.isArray(channel.chaincodes) ? channel.chaincodes : []).forEach(chaincodeId => {
        registerChannelChaincode(channelId, chaincodeId);
      });
    });
  };

  ingestChannels(safeTopology.channels);
  (Array.isArray(safeTopology.business_groups) ? safeTopology.business_groups : []).forEach(
    businessGroup => {
      if (!businessGroup || typeof businessGroup !== 'object') {
        return;
      }
      ingestChannels(businessGroup.channels);
    }
  );
  (Array.isArray(safeTopology.chaincodes) ? safeTopology.chaincodes : []).forEach(chaincodeRow => {
    if (!chaincodeRow || typeof chaincodeRow !== 'object') {
      return;
    }
    registerChannelChaincode(chaincodeRow.channel_id, chaincodeRow.chaincode_id);
  });

  return chaincodesByChannel;
};

const collectOrganizationApiComponents = organization => {
  const apiRegistry = new Map();
  (Array.isArray(organization && organization.apis) ? organization.apis : []).forEach(api => {
    if (!api || typeof api !== 'object') {
      return;
    }
    const apiId = normalizeText(api.api_id);
    const channelId = normalizeText(api.channel_id);
    const chaincodeId = normalizeText(api.chaincode_id);
    const routePath = normalizeText(api.route_path);
    const apiKey = buildSortKey(apiId, channelId, chaincodeId, routePath);
    if (!apiRegistry.has(apiKey)) {
      apiRegistry.set(apiKey, {
        apiId,
        channelId,
        chaincodeId,
      });
    }
  });

  return sortByKey(Array.from(apiRegistry.values()), apiComponent =>
    buildSortKey(
      apiComponent && apiComponent.apiId,
      apiComponent && apiComponent.channelId,
      apiComponent && apiComponent.chaincodeId
    )
  );
};

const collectOrganizationChaincodeComponents = ({ organization, chaincodesByChannel }) => {
  const componentRegistry = new Map();
  const scopedChaincodeIds = new Set();
  const organizationChannels = new Set();

  const registerChaincodeComponent = (channelId, chaincodeId) => {
    const normalizedChannelId = normalizeText(channelId);
    const normalizedChaincodeId = normalizeText(chaincodeId);
    if (!normalizedChaincodeId) {
      return;
    }
    const componentKey = buildSortKey(normalizedChannelId, normalizedChaincodeId);
    if (!componentRegistry.has(componentKey)) {
      componentRegistry.set(componentKey, {
        channelId: normalizedChannelId,
        chaincodeId: normalizedChaincodeId,
      });
    }
    if (normalizedChannelId) {
      scopedChaincodeIds.add(normalizedChaincodeId.toLowerCase());
    }
  };

  (Array.isArray(organization && organization.channels) ? organization.channels : []).forEach(
    channelEntry => {
      if (channelEntry && typeof channelEntry === 'object') {
        const channelId = normalizeText(channelEntry.channel_id || channelEntry.name);
        if (!channelId) {
          return;
        }
        organizationChannels.add(channelId);
        (Array.isArray(channelEntry.chaincodes) ? channelEntry.chaincodes : []).forEach(
          chaincodeId => {
            registerChaincodeComponent(channelId, chaincodeId);
          }
        );
        return;
      }
      const channelId = normalizeText(channelEntry);
      if (channelId) {
        organizationChannels.add(channelId);
      }
    }
  );

  collectOrganizationApiComponents(organization).forEach(apiComponent => {
    const channelId = normalizeText(apiComponent && apiComponent.channelId);
    if (channelId) {
      organizationChannels.add(channelId);
    }
    registerChaincodeComponent(channelId, apiComponent && apiComponent.chaincodeId);
  });

  organizationChannels.forEach(channelId => {
    const channelChaincodes = chaincodesByChannel.get(normalizeText(channelId).toLowerCase());
    (channelChaincodes ? Array.from(channelChaincodes) : []).forEach(chaincodeId => {
      registerChaincodeComponent(channelId, chaincodeId);
    });
  });

  (Array.isArray(organization && organization.chaincodes) ? organization.chaincodes : []).forEach(
    chaincodeId => {
      const normalizedChaincodeId = normalizeText(chaincodeId);
      if (!normalizedChaincodeId) {
        return;
      }
      if (scopedChaincodeIds.has(normalizedChaincodeId.toLowerCase())) {
        return;
      }
      registerChaincodeComponent('', normalizedChaincodeId);
    }
  );

  return sortByKey(Array.from(componentRegistry.values()), chaincodeComponent =>
    buildSortKey(
      chaincodeComponent && chaincodeComponent.channelId,
      chaincodeComponent && chaincodeComponent.chaincodeId
    )
  );
};

const buildSupplementalRuntimeHostMapping = ({
  topologyCatalog,
  machineRegistry,
  preflightReport,
} = {}) => {
  const safeTopology =
    topologyCatalog && typeof topologyCatalog === 'object' ? topologyCatalog : {};
  const organizations = Array.isArray(safeTopology.organizations) ? safeTopology.organizations : [];
  const chaincodesByChannel = collectTopologyChaincodesByChannel(safeTopology);
  const {
    fallbackPreflightStatus,
    registry: preflightStatusRegistry,
  } = buildPreflightStatusRegistry(preflightReport);
  const supplementalRows = [];

  const appendRuntimeRow = ({ orgId, nodeType, nodeId, hostRef }) => {
    const normalizedOrgId = normalizeText(orgId);
    const normalizedNodeType = normalizeRuntimeNodeType(nodeType);
    const normalizedNodeId = normalizeText(nodeId);
    const normalizedHostRef = normalizeText(hostRef);
    if (!normalizedOrgId || !normalizedNodeType || !normalizedNodeId || !normalizedHostRef) {
      return;
    }
    const machine = resolveMachineByHostRef(machineRegistry, normalizedHostRef);
    if (!machine) {
      return;
    }
    const hostAddress = normalizeText(machine.host_address);
    const sshUser = normalizeText(machine.ssh_user);
    if (!hostAddress || !sshUser) {
      return;
    }
    supplementalRows.push({
      node_id: normalizedNodeId,
      node_type: normalizedNodeType,
      org_id: normalizedOrgId,
      host_ref: normalizedHostRef,
      host_address: hostAddress,
      ssh_user: sshUser,
      ssh_port: normalizePort(machine.ssh_port) || 22,
      docker_port: normalizePort(machine.docker_port) || DEFAULT_DOCKER_PORT,
      runtime_image: resolveDefaultRuntimeImageForNodeType(normalizedNodeType),
      preflight_status: resolvePreflightStatusForHost({
        hostRef: normalizedHostRef,
        hostAddress,
        preflightStatusRegistry,
        fallbackPreflightStatus,
      }),
    });
  };

  organizations.forEach(organization => {
    const orgId =
      normalizeText(organization && organization.org_id) ||
      normalizeText(organization && organization.org_name);
    if (!orgId) {
      return;
    }
    const serviceHostMapping =
      organization && typeof organization.service_host_mapping === 'object'
        ? organization.service_host_mapping
        : {};
    const chaincodeComponents = collectOrganizationChaincodeComponents({
      organization,
      chaincodesByChannel,
    });

    appendRuntimeRow({
      orgId,
      nodeType: 'couch',
      nodeId: toRuntimeNodeId('couch', orgId),
      hostRef: serviceHostMapping.couch,
    });
    appendRuntimeRow({
      orgId,
      nodeType: 'apigateway',
      nodeId: toRuntimeNodeId('apigateway', orgId),
      hostRef: serviceHostMapping.apiGateway || serviceHostMapping.api_gateway,
    });
    appendRuntimeRow({
      orgId,
      nodeType: 'netapi',
      nodeId: toRuntimeNodeId('netapi', orgId),
      hostRef:
        serviceHostMapping.netapi || serviceHostMapping.netApi || serviceHostMapping.networkApi,
    });

    const chaincodeHostRef =
      normalizeText(serviceHostMapping.peer) ||
      normalizeText(organization && organization.peer_host_ref) ||
      normalizeText(
        serviceHostMapping.netapi || serviceHostMapping.netApi || serviceHostMapping.networkApi
      );
    chaincodeComponents.forEach(chaincodeComponent => {
      const normalizedChaincodeId = normalizeText(
        chaincodeComponent && chaincodeComponent.chaincodeId
      );
      if (!normalizedChaincodeId) {
        return;
      }
      const normalizedChannelId = normalizeText(chaincodeComponent && chaincodeComponent.channelId);
      const chaincodeSuffix = normalizedChannelId
        ? `${normalizedChannelId}-${normalizedChaincodeId}`
        : normalizedChaincodeId;
      appendRuntimeRow({
        orgId,
        nodeType: 'chaincode',
        nodeId: toRuntimeNodeId('chaincode', orgId, chaincodeSuffix),
        hostRef: chaincodeHostRef,
      });
    });
  });

  return mergeHostMappingRows(supplementalRows);
};

export const buildRunbookBlueprintCatalogEntry = handoff => {
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const blueprintVersion = normalizeText(safeHandoff.blueprint_version);
  if (!blueprintVersion) {
    return null;
  }

  return {
    value: blueprintVersion,
    label: `v${blueprintVersion}`,
    lintValid: Boolean(safeHandoff.blueprint_validated),
    resolvedSchemaVersion: normalizeText(safeHandoff.resolved_schema_version) || '1.0.0',
    fingerprint: normalizeText(safeHandoff.blueprint_fingerprint),
    changeId: normalizeText(safeHandoff.change_id),
  };
};

export const buildCanonicalOnboardingRunbookHandoffPayload = handoff => {
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const topologyCatalog = extractTopologyCatalogFromPayload(safeHandoff);
  const safeNetwork =
    safeHandoff.network && typeof safeHandoff.network === 'object' ? safeHandoff.network : {};
  let chaincodeInstallEntries = topologyCatalog.chaincodes;
  if (Array.isArray(safeNetwork.chaincodes_install) && safeNetwork.chaincodes_install.length > 0) {
    chaincodeInstallEntries = safeNetwork.chaincodes_install;
  } else if (Array.isArray(safeNetwork.chaincodes) && safeNetwork.chaincodes.length > 0) {
    chaincodeInstallEntries = safeNetwork.chaincodes;
  }
  const normalizedChaincodeMetadata = normalizeChaincodeInstallEntries(chaincodeInstallEntries);
  const contextSource = resolveContextSource(safeHandoff);
  const normalizedSourceBlueprintFingerprint = normalizeLower(
    firstNonEmptyText(safeHandoff.source_blueprint_fingerprint, safeHandoff.blueprint_fingerprint)
  );
  const normalizedManifestFingerprint = normalizeLower(
    firstNonEmptyText(safeHandoff.manifest_fingerprint, normalizedSourceBlueprintFingerprint)
  );
  const explicitArtifactsReady =
    typeof safeHandoff.a2_2_artifacts_ready === 'boolean' ? safeHandoff.a2_2_artifacts_ready : null;
  const artifactsState = resolveA2ArtifactsState({
    requiredArtifacts: safeHandoff.a2_2_minimum_artifacts,
    availableArtifacts: safeHandoff.a2_2_available_artifacts,
    missingArtifacts: safeHandoff.a2_2_missing_artifacts,
    explicitReady: explicitArtifactsReady,
    defaultToReady: shouldDefaultArtifactsReadyForOfficialContext({
      contextSource,
      explicitReady: explicitArtifactsReady,
      requiredArtifacts: safeHandoff.a2_2_minimum_artifacts,
      availableArtifacts: safeHandoff.a2_2_available_artifacts,
      missingArtifacts: safeHandoff.a2_2_missing_artifacts,
    }),
  });
  const backendStateFallback = resolveBackendStateFallbackByContextSource(contextSource);

  return {
    handoff_contract_version:
      normalizeText(safeHandoff.handoff_contract_version) ||
      ONBOARDING_RUNBOOK_HANDOFF_CONTRACT_VERSION,
    source: normalizeText(safeHandoff.source),
    context_source: contextSource,
    execution_context: normalizeText(safeHandoff.execution_context),
    correlation: {
      change_id: normalizeText(safeHandoff.change_id),
      run_id: normalizeText(safeHandoff.run_id),
      blueprint_version: normalizeText(safeHandoff.blueprint_version),
      blueprint_fingerprint: normalizeLower(safeHandoff.blueprint_fingerprint),
      manifest_fingerprint: normalizedManifestFingerprint,
      source_blueprint_fingerprint: normalizedSourceBlueprintFingerprint,
      resolved_schema_version: normalizeText(safeHandoff.resolved_schema_version) || '1.0.0',
      environment_profile: normalizeText(safeHandoff.environment_profile),
      official_backend_state: normalizeBackendState(
        safeHandoff.official_backend_state,
        backendStateFallback
      ),
    },
    stage_contract: {
      pipeline_preconditions_ready: Boolean(safeHandoff.pipeline_preconditions_ready),
      blueprint_validated: Boolean(safeHandoff.blueprint_validated),
      preflight_approved: Boolean(safeHandoff.preflight_approved),
    },
    artifacts: {
      required: artifactsState.requiredArtifacts,
      available: artifactsState.availableArtifacts,
      missing: artifactsState.missingArtifacts,
      ready: artifactsState.artifactsReady,
    },
    host_mapping: canonicalizeHostMappingOrganizationIds(
      extractHostMappingFromPayload(safeHandoff),
      topologyCatalog
    ),
    machine_credentials: normalizeMachineCredentialEntries(
      extractMachineCredentialsFromPayload(safeHandoff)
    ),
    network: {
      chaincodes: normalizedChaincodeMetadata,
      chaincodes_install: normalizedChaincodeMetadata,
    },
    topology_catalog: topologyCatalog,
    api_registry: normalizeApiRegistryEntries(safeHandoff.api_registry),
    incremental_expansions: normalizeIncrementalExpansionEntries(
      safeHandoff.incremental_expansions
    ),
    onboarding_audit: normalizeOnboardingAuditEntries(safeHandoff.onboarding_audit),
  };
};

const buildRunbookResumeContext = handoff => {
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const contextSource = resolveContextSource(safeHandoff);
  const topologyCatalog =
    safeHandoff.topology_catalog && typeof safeHandoff.topology_catalog === 'object'
      ? safeHandoff.topology_catalog
      : {};
  const topologyCounts =
    topologyCatalog.counts && typeof topologyCatalog.counts === 'object'
      ? topologyCatalog.counts
      : {};
  const runId = normalizeText(safeHandoff.run_id);
  const changeId = normalizeText(safeHandoff.change_id);
  const sourceBlueprintFingerprint = normalizeLower(
    firstNonEmptyText(safeHandoff.source_blueprint_fingerprint, safeHandoff.blueprint_fingerprint)
  );
  const manifestFingerprint = normalizeLower(
    firstNonEmptyText(safeHandoff.manifest_fingerprint, sourceBlueprintFingerprint)
  );
  const backendStateFallback = resolveBackendStateFallbackByContextSource(contextSource);

  return {
    resume_key: buildSortKey(changeId, runId, manifestFingerprint, sourceBlueprintFingerprint),
    change_id: changeId,
    run_id: runId,
    manifest_fingerprint: manifestFingerprint,
    source_blueprint_fingerprint: sourceBlueprintFingerprint,
    environment_profile: normalizeText(safeHandoff.environment_profile),
    backend_state: normalizeBackendState(safeHandoff.official_backend_state, backendStateFallback),
    topology_counts: {
      hosts: normalizePositiveInt(topologyCounts.hosts),
      organizations: normalizePositiveInt(topologyCounts.organizations),
      peers: normalizePositiveInt(topologyCounts.peers),
      orderers: normalizePositiveInt(topologyCounts.orderers),
      apis: normalizePositiveInt(topologyCounts.apis),
    },
    storage_keys: {
      session: ONBOARDING_RUNBOOK_HANDOFF_STORAGE_KEY,
      trail: ONBOARDING_RUNBOOK_HANDOFF_TRAIL_STORAGE_KEY,
    },
  };
};

const buildHandoffTraceRows = ({ handoff, onboardingAudit, createdAtUtc }) => {
  const safeHandoff = handoff && typeof handoff === 'object' ? handoff : {};
  const baseTraceRow = {
    event: 'handoff_created',
    timestamp_utc: normalizeText(createdAtUtc) || toIsoUtc(),
    change_id: normalizeText(safeHandoff.change_id),
    run_id: normalizeText(safeHandoff.run_id),
    manifest_fingerprint: normalizeLower(safeHandoff.manifest_fingerprint),
    source_blueprint_fingerprint: normalizeLower(safeHandoff.source_blueprint_fingerprint),
    handoff_fingerprint: normalizeLower(safeHandoff.handoff_fingerprint),
  };
  const normalizedAuditTrail = normalizeOnboardingAuditEntries(onboardingAudit);
  const traceRows = [baseTraceRow];
  normalizedAuditTrail.forEach(auditEntry => {
    traceRows.push({
      event: normalizeText(auditEntry.code) || 'onboarding_event',
      timestamp_utc: normalizeText(auditEntry.timestamp_utc),
      change_id: baseTraceRow.change_id,
      run_id: baseTraceRow.run_id,
      manifest_fingerprint: baseTraceRow.manifest_fingerprint,
      source_blueprint_fingerprint: baseTraceRow.source_blueprint_fingerprint,
      handoff_fingerprint: baseTraceRow.handoff_fingerprint,
    });
  });
  return traceRows;
};

export const buildOnboardingRunbookHandoff = ({
  changeId,
  environmentProfile,
  runId,
  guidedBlueprint,
  machines,
  machineCredentials,
  organizations,
  channels,
  chaincodeInstalls,
  businessGroupName,
  businessGroupDescription,
  businessGroupNetworkId,
  publishedBlueprintRecord,
  preflightReport,
  modelingAuditTrail,
  executionContext,
  apiRegistry,
  incrementalExpansions,
  officialPublish = false,
}) => {
  const safeRecord =
    publishedBlueprintRecord && typeof publishedBlueprintRecord === 'object'
      ? publishedBlueprintRecord
      : {};
  const blueprintVersion = normalizeText(safeRecord.blueprint_version);
  const blueprintFingerprint = normalizeText(safeRecord.fingerprint_sha256);
  const sourceBlueprintFingerprint = firstNonEmptyText(
    safeRecord.source_blueprint_fingerprint,
    safeRecord.sourceBlueprintFingerprint,
    blueprintFingerprint
  );
  const manifestFingerprint = firstNonEmptyText(
    safeRecord.manifest_fingerprint,
    safeRecord.manifestFingerprint,
    safeRecord.org_runtime_manifest_fingerprint,
    safeRecord.orgRuntimeManifestFingerprint,
    safeRecord.runtime_manifest_fingerprint,
    safeRecord.runtimeManifestFingerprint,
    sourceBlueprintFingerprint
  );
  const requiredArtifactsFromPublish = firstArrayValue(
    safeRecord.a2_2_minimum_artifacts,
    safeRecord.a2_2_required_artifacts,
    safeRecord.minimum_required_artifacts,
    safeRecord.required_artifacts,
    safeRecord.entry_gate_required_artifacts
  );
  const availableArtifactsFromPublish = firstArrayValue(
    safeRecord.a2_2_available_artifacts,
    safeRecord.available_artifacts,
    safeRecord.generated_artifacts,
    safeRecord.evidence_artifacts
  );
  const missingArtifactsFromPublish = firstArrayValue(
    safeRecord.a2_2_missing_artifacts,
    safeRecord.missing_artifacts,
    safeRecord.entry_gate_missing_artifacts
  );
  const explicitArtifactsReady =
    typeof safeRecord.a2_2_artifacts_ready === 'boolean' ? safeRecord.a2_2_artifacts_ready : null;
  const contextSource = officialPublish
    ? OFFICIAL_CONTEXT_SOURCE_BACKEND_PUBLISH
    : OFFICIAL_CONTEXT_SOURCE_GUIDED_UI_DRAFT;
  const artifactsState = resolveA2ArtifactsState({
    requiredArtifacts: requiredArtifactsFromPublish,
    availableArtifacts: availableArtifactsFromPublish,
    missingArtifacts: missingArtifactsFromPublish,
    explicitReady: explicitArtifactsReady,
    defaultToReady: shouldDefaultArtifactsReadyForOfficialContext({
      contextSource,
      explicitReady: explicitArtifactsReady,
      requiredArtifacts: requiredArtifactsFromPublish,
      availableArtifacts: availableArtifactsFromPublish,
      missingArtifacts: missingArtifactsFromPublish,
    }),
  });
  const officialBackendState = normalizeBackendState(
    safeRecord.backend_state || safeRecord.backendState || safeRecord.entry_gate_backend_state,
    officialPublish ? OFFICIAL_BACKEND_STATE_READY : OFFICIAL_BACKEND_STATE_PENDING
  );
  const resolvedSchemaVersion = normalizeText(safeRecord.resolved_schema_version) || '1.0.0';
  const machineRegistry = buildMachineRegistry(machines);
  const baseResolvedHostMapping = normalizeHostMappingEntries(
    buildResolvedHostMapping({
      blueprint: guidedBlueprint,
      machines,
      preflightReport,
    })
  );
  const resolvedMachineCredentials = normalizeMachineCredentialEntries(machineCredentials);
  const normalizedApiRegistry = normalizeApiRegistryEntries(apiRegistry);
  const normalizedIncrementalExpansions = normalizeIncrementalExpansionEntries(
    incrementalExpansions
  );
  const normalizedOnboardingAudit = normalizeOnboardingAuditEntries(modelingAuditTrail);
  const topologyCatalog = buildOrganizationTopologyCatalog({
    organizations,
    channels,
    chaincodeInstalls,
    businessGroupName,
    businessGroupDescription,
    businessGroupNetworkId,
    hostMapping: baseResolvedHostMapping,
    apiRegistry: normalizedApiRegistry,
    machineRegistry,
  });
  const canonicalBaseResolvedHostMapping = canonicalizeHostMappingOrganizationIds(
    baseResolvedHostMapping,
    topologyCatalog
  );
  const supplementalHostMapping = buildSupplementalRuntimeHostMapping({
    topologyCatalog,
    machineRegistry,
    preflightReport,
  });
  const canonicalSupplementalHostMapping = canonicalizeHostMappingOrganizationIds(
    supplementalHostMapping,
    topologyCatalog
  );
  const resolvedHostMapping = mergeHostMappingRows(
    canonicalBaseResolvedHostMapping,
    canonicalSupplementalHostMapping
  );
  const normalizedChaincodeMetadata = normalizeChaincodeInstallEntries(
    Array.isArray(topologyCatalog.chaincodes) ? topologyCatalog.chaincodes : []
  );
  const createdAtUtc = toIsoUtc();
  const baseHandoff = {
    source: officialPublish ? 'infra-ssh-onboarding-a2.5' : 'infra-ssh-onboarding-a1.6',
    context_source: contextSource,
    created_at_utc: createdAtUtc,
    change_id: normalizeText(changeId),
    run_id: normalizeText(runId),
    manifest_fingerprint: normalizeLower(manifestFingerprint),
    source_blueprint_fingerprint: normalizeLower(sourceBlueprintFingerprint),
    environment_profile: normalizeText(environmentProfile),
    blueprint_version: blueprintVersion,
    blueprint_fingerprint: normalizeLower(blueprintFingerprint),
    resolved_schema_version: resolvedSchemaVersion,
    official_backend_state: officialBackendState,
    a2_2_minimum_artifacts: artifactsState.requiredArtifacts,
    a2_2_available_artifacts: artifactsState.availableArtifacts,
    a2_2_missing_artifacts: artifactsState.missingArtifacts,
    a2_2_artifacts_ready: artifactsState.artifactsReady,
    blueprint_validated: true,
    pipeline_preconditions_ready: true,
    execution_context: normalizeText(executionContext),
    preflight_approved: isPreflightApproved(preflightReport),
    preflight: preflightReport || null,
    network: {
      chaincodes: normalizedChaincodeMetadata,
      chaincodes_install: normalizedChaincodeMetadata,
    },
    host_mapping: resolvedHostMapping,
    machine_credentials: resolvedMachineCredentials,
    topology_catalog: topologyCatalog,
    api_registry: normalizedApiRegistry,
    incremental_expansions: normalizedIncrementalExpansions,
    onboarding_audit: normalizedOnboardingAudit,
    handoff_contract_version: ONBOARDING_RUNBOOK_HANDOFF_CONTRACT_VERSION,
  };
  const canonicalPayload = buildCanonicalOnboardingRunbookHandoffPayload(baseHandoff);
  const handoffFingerprint = deterministicFingerprint(canonicalPayload);
  const handoffWithFingerprint = {
    ...baseHandoff,
    handoff_fingerprint: handoffFingerprint,
  };
  const handoffTrace = buildHandoffTraceRows({
    handoff: handoffWithFingerprint,
    onboardingAudit: normalizedOnboardingAudit,
    createdAtUtc,
  });
  const runbookResumeContext = buildRunbookResumeContext(handoffWithFingerprint);

  return {
    ...handoffWithFingerprint,
    handoff_payload: canonicalPayload,
    handoff_trace: handoffTrace,
    runbook_resume_context: runbookResumeContext,
  };
};

const getSessionStorage = () => {
  if (typeof window === 'undefined' || !window.sessionStorage) {
    return null;
  }
  return window.sessionStorage;
};

const getLocalStorage = () => {
  if (typeof window === 'undefined' || !window.localStorage) {
    return null;
  }
  return window.localStorage;
};

const parseHandoffPayload = payload => {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  return payload;
};

const enrichHandoffContract = payload => {
  const parsedPayload = parseHandoffPayload(payload);
  if (!parsedPayload) {
    return null;
  }

  const basePayload = {
    ...parsedPayload,
    handoff_contract_version:
      normalizeText(parsedPayload.handoff_contract_version) ||
      ONBOARDING_RUNBOOK_HANDOFF_CONTRACT_VERSION,
  };
  const normalizedContextSource = resolveContextSource(basePayload);
  const normalizedSourceBlueprintFingerprint = normalizeLower(
    firstNonEmptyText(
      basePayload.source_blueprint_fingerprint,
      basePayload.sourceBlueprintFingerprint,
      basePayload.blueprint_fingerprint,
      basePayload.blueprintFingerprint
    )
  );
  const normalizedManifestFingerprint = normalizeLower(
    firstNonEmptyText(
      basePayload.manifest_fingerprint,
      basePayload.manifestFingerprint,
      basePayload.org_runtime_manifest_fingerprint,
      basePayload.orgRuntimeManifestFingerprint,
      basePayload.runtime_manifest_fingerprint,
      basePayload.runtimeManifestFingerprint,
      normalizedSourceBlueprintFingerprint
    )
  );
  const requiredArtifactsFromPayload = firstArrayValue(
    basePayload.a2_2_minimum_artifacts,
    basePayload.a2_2_required_artifacts,
    basePayload.minimum_required_artifacts,
    basePayload.required_artifacts,
    basePayload.entry_gate_required_artifacts
  );
  const availableArtifactsFromPayload = firstArrayValue(
    basePayload.a2_2_available_artifacts,
    basePayload.available_artifacts,
    basePayload.generated_artifacts,
    basePayload.evidence_artifacts
  );
  const missingArtifactsFromPayload = firstArrayValue(
    basePayload.a2_2_missing_artifacts,
    basePayload.missing_artifacts,
    basePayload.entry_gate_missing_artifacts
  );
  const explicitArtifactsReady =
    typeof basePayload.a2_2_artifacts_ready === 'boolean' ? basePayload.a2_2_artifacts_ready : null;
  const artifactsState = resolveA2ArtifactsState({
    requiredArtifacts: requiredArtifactsFromPayload,
    availableArtifacts: availableArtifactsFromPayload,
    missingArtifacts: missingArtifactsFromPayload,
    explicitReady: explicitArtifactsReady,
    defaultToReady: shouldDefaultArtifactsReadyForOfficialContext({
      contextSource: normalizedContextSource,
      explicitReady: explicitArtifactsReady,
      requiredArtifacts: requiredArtifactsFromPayload,
      availableArtifacts: availableArtifactsFromPayload,
      missingArtifacts: missingArtifactsFromPayload,
    }),
  });
  const normalizedTopologyCatalog = extractTopologyCatalogFromPayload(basePayload);
  const normalizedHostMapping = canonicalizeHostMappingOrganizationIds(
    extractHostMappingFromPayload(basePayload),
    normalizedTopologyCatalog
  );
  const normalizedMachineCredentials = normalizeMachineCredentialEntries(
    extractMachineCredentialsFromPayload(basePayload)
  );
  const backendStateFallback = resolveBackendStateFallbackByContextSource(normalizedContextSource);
  const normalizedBasePayload = {
    ...basePayload,
    context_source: normalizedContextSource,
    source_blueprint_fingerprint: normalizedSourceBlueprintFingerprint,
    manifest_fingerprint: normalizedManifestFingerprint,
    official_backend_state: normalizeBackendState(
      basePayload.official_backend_state,
      backendStateFallback
    ),
    a2_2_minimum_artifacts: artifactsState.requiredArtifacts,
    a2_2_available_artifacts: artifactsState.availableArtifacts,
    a2_2_missing_artifacts: artifactsState.missingArtifacts,
    a2_2_artifacts_ready: artifactsState.artifactsReady,
    topology_catalog: normalizedTopologyCatalog,
    host_mapping: normalizedHostMapping,
    machine_credentials: normalizedMachineCredentials,
  };
  const canonicalPayload =
    normalizedBasePayload.handoff_payload &&
    typeof normalizedBasePayload.handoff_payload === 'object'
      ? normalizedBasePayload.handoff_payload
      : buildCanonicalOnboardingRunbookHandoffPayload(normalizedBasePayload);
  const handoffFingerprint =
    normalizeLower(normalizedBasePayload.handoff_fingerprint) ||
    deterministicFingerprint(canonicalPayload);
  const handoffWithFingerprint = {
    ...normalizedBasePayload,
    handoff_fingerprint: handoffFingerprint,
    handoff_payload: canonicalPayload,
  };
  const handoffTrace =
    Array.isArray(handoffWithFingerprint.handoff_trace) &&
    handoffWithFingerprint.handoff_trace.length > 0
      ? handoffWithFingerprint.handoff_trace
      : buildHandoffTraceRows({
          handoff: handoffWithFingerprint,
          onboardingAudit: handoffWithFingerprint.onboarding_audit,
          createdAtUtc: handoffWithFingerprint.created_at_utc,
        });

  return {
    ...handoffWithFingerprint,
    handoff_trace: handoffTrace,
    runbook_resume_context:
      handoffWithFingerprint.runbook_resume_context &&
      typeof handoffWithFingerprint.runbook_resume_context === 'object'
        ? handoffWithFingerprint.runbook_resume_context
        : buildRunbookResumeContext(handoffWithFingerprint),
  };
};

const readHandoffTrail = () => {
  const storage = getLocalStorage();
  if (!storage) {
    return [];
  }

  const rawPayload = storage.getItem(ONBOARDING_RUNBOOK_HANDOFF_TRAIL_STORAGE_KEY);
  if (!rawPayload) {
    return [];
  }

  try {
    const parsedRows = JSON.parse(rawPayload);
    const trailRows = (Array.isArray(parsedRows) ? parsedRows : [])
      .map(entry => enrichHandoffContract(entry))
      .filter(Boolean);
    return sortByKey(trailRows, row =>
      buildSortKey(
        row && row.created_at_utc,
        row && row.change_id,
        row && row.run_id,
        row && row.handoff_fingerprint
      )
    ).reverse();
  } catch (error) {
    return [];
  }
};

const writeHandoffTrail = entries => {
  const storage = getLocalStorage();
  if (!storage) {
    return false;
  }

  const normalizedEntries = Array.isArray(entries) ? entries : [];
  const buildMinimalTrailEntry = entry => ({
    handoff_contract_version: normalizeText(entry && entry.handoff_contract_version),
    context_source: normalizeText(entry && entry.context_source),
    official_backend_state: normalizeText(entry && entry.official_backend_state),
    change_id: normalizeText(entry && entry.change_id),
    run_id: normalizeText(entry && entry.run_id),
    created_at_utc: normalizeText(entry && entry.created_at_utc),
    handoff_fingerprint: normalizeLower(entry && entry.handoff_fingerprint),
    manifest_fingerprint: normalizeLower(entry && entry.manifest_fingerprint),
    source_blueprint_fingerprint: normalizeLower(entry && entry.source_blueprint_fingerprint),
    blueprint_version: normalizeText(entry && entry.blueprint_version),
  });

  const writeCandidates = [
    normalizedEntries,
    normalizedEntries
      .slice(0, ONBOARDING_RUNBOOK_HANDOFF_TRAIL_COMPACT_MAX_ENTRIES)
      .map(buildMinimalTrailEntry),
    normalizedEntries
      .slice(0, ONBOARDING_RUNBOOK_HANDOFF_TRAIL_MINIMAL_MAX_ENTRIES)
      .map(buildMinimalTrailEntry),
  ];
  let writeFailures = 0;

  for (let index = 0; index < writeCandidates.length; index += 1) {
    try {
      storage.setItem(
        ONBOARDING_RUNBOOK_HANDOFF_TRAIL_STORAGE_KEY,
        JSON.stringify(writeCandidates[index])
      );
      return true;
    } catch (error) {
      if (error) {
        writeFailures += 1;
      }
    }
  }

  if (writeFailures > 0) {
    return false;
  }

  return false;
};

const buildTrailEntryKey = handoff =>
  buildSortKey(
    handoff && handoff.change_id,
    handoff && handoff.run_id,
    handoff && handoff.manifest_fingerprint,
    handoff && handoff.source_blueprint_fingerprint,
    handoff && handoff.handoff_fingerprint
  );

export const persistOnboardingRunbookHandoff = handoff => {
  const normalizedHandoff = enrichHandoffContract(handoff);
  if (!normalizedHandoff) {
    return false;
  }

  let persisted = false;
  const sessionStorage = getSessionStorage();
  if (sessionStorage) {
    try {
      sessionStorage.setItem(
        ONBOARDING_RUNBOOK_HANDOFF_STORAGE_KEY,
        JSON.stringify(normalizedHandoff)
      );
      persisted = true;
    } catch (error) {
      persisted = false;
    }
  }

  const currentTrail = readHandoffTrail();
  const nextTrail = [
    normalizedHandoff,
    ...currentTrail.filter(
      entry => buildTrailEntryKey(entry) !== buildTrailEntryKey(normalizedHandoff)
    ),
  ].slice(0, ONBOARDING_RUNBOOK_HANDOFF_TRAIL_MAX_ENTRIES);
  const trailPersisted = writeHandoffTrail(nextTrail);

  return persisted || trailPersisted;
};

export const listOnboardingRunbookHandoffTrail = () => readHandoffTrail();

export const resolveOnboardingRunbookHandoffFromTrail = ({
  runId = '',
  changeId = '',
  handoffFingerprint = '',
} = {}) => {
  const normalizedRunId = normalizeText(runId);
  const normalizedChangeId = normalizeText(changeId);
  const normalizedFingerprint = normalizeLower(handoffFingerprint);
  const trail = readHandoffTrail();

  if (!normalizedRunId && !normalizedChangeId && !normalizedFingerprint) {
    return trail[0] || null;
  }

  return (
    trail.find(entry => {
      if (
        normalizedFingerprint &&
        normalizeLower(entry && entry.handoff_fingerprint) !== normalizedFingerprint
      ) {
        return false;
      }
      if (normalizedRunId && normalizeText(entry && entry.run_id) !== normalizedRunId) {
        return false;
      }
      if (normalizedChangeId && normalizeText(entry && entry.change_id) !== normalizedChangeId) {
        return false;
      }
      return true;
    }) || null
  );
};

export const consumeOnboardingRunbookHandoff = ({ fallbackToTrail = true } = {}) => {
  const storage = getSessionStorage();
  if (!storage) {
    return fallbackToTrail ? resolveOnboardingRunbookHandoffFromTrail() : null;
  }

  const rawPayload = storage.getItem(ONBOARDING_RUNBOOK_HANDOFF_STORAGE_KEY);
  if (!rawPayload) {
    return fallbackToTrail ? resolveOnboardingRunbookHandoffFromTrail() : null;
  }

  try {
    const parsedPayload = JSON.parse(rawPayload);
    const enrichedPayload = enrichHandoffContract(parsedPayload);
    if (!enrichedPayload) {
      return fallbackToTrail ? resolveOnboardingRunbookHandoffFromTrail() : null;
    }
    return enrichedPayload;
  } catch (error) {
    return fallbackToTrail ? resolveOnboardingRunbookHandoffFromTrail() : null;
  }
};

export const clearOnboardingRunbookHandoff = () => {
  const storage = getSessionStorage();
  if (!storage) {
    return false;
  }

  storage.removeItem(ONBOARDING_RUNBOOK_HANDOFF_STORAGE_KEY);
  return true;
};
