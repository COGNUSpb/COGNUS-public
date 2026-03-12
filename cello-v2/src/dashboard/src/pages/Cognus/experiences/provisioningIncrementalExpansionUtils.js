import { buildApiRoutePath } from './provisioningApiManagementUtils';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const normalizeText = value => String(value || '').trim();
const localizeIncrementalText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const normalizeSegment = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

const normalizeDnsLabel = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

const normalizeDomain = value => {
  const sanitized = normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9.-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/\.+/g, '.')
    .replace(/^\.+|\.+$/g, '');

  if (!sanitized) {
    return '';
  }

  return sanitized
    .split('.')
    .map(token => normalizeDnsLabel(token))
    .filter(Boolean)
    .join('.');
};

const toSafePositiveInt = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const parseMembersCsv = value =>
  normalizeText(value)
    .split(',')
    .map(token => normalizeText(token))
    .filter(Boolean);

const seededHash = (input, seed) => {
  const modulus = 4294967291;
  let hash = Number(seed) % modulus;

  for (let index = 0; index < input.length; index += 1) {
    hash = (hash * 1664525 + input.charCodeAt(index) + 1013904223) % modulus;
  }

  return Math.floor(hash)
    .toString(16)
    .slice(-8)
    .padStart(8, '0');
};

const deterministicId = (prefix, payload) => {
  const source = JSON.stringify(payload || {});
  return `${prefix}-${[seededHash(source, 0x811c9dc5), seededHash(source, 0x27d4eb2f)].join('')}`;
};

const DEFAULT_PEER_PORT_BASE = 7051;
const DEFAULT_ORDERER_PORT_BASE = 7050;
const MAX_ALLOWED_PORT = 65535;
const NODE_PORT_STRIDE = 100;

export const INCREMENTAL_NODE_OPERATION_TYPE = Object.freeze({
  addPeer: 'add_peer',
  addOrderer: 'add_orderer',
});

export const INCREMENTAL_NODE_OPERATION_OPTIONS = Object.freeze([
  {
    value: INCREMENTAL_NODE_OPERATION_TYPE.addPeer,
    label: 'add_peer',
  },
  {
    value: INCREMENTAL_NODE_OPERATION_TYPE.addOrderer,
    label: 'add_orderer',
  },
]);

const INCREMENTAL_NODE_OPERATION_SET = new Set(
  INCREMENTAL_NODE_OPERATION_OPTIONS.map(option => option.value)
);

const NODE_OPERATION_CONFIG = Object.freeze({
  [INCREMENTAL_NODE_OPERATION_TYPE.addPeer]: {
    componentPrefix: 'peer',
    operationIdPrefix: 'inc-peer',
    countField: 'peers',
    hostField: 'peerHostRef',
    trackedNodesField: 'incremental_peer_nodes',
    defaultPortBase: DEFAULT_PEER_PORT_BASE,
    capacityUnits: 2,
  },
  [INCREMENTAL_NODE_OPERATION_TYPE.addOrderer]: {
    componentPrefix: 'orderer',
    operationIdPrefix: 'inc-orderer',
    countField: 'orderers',
    hostField: 'ordererHostRef',
    trackedNodesField: 'incremental_orderer_nodes',
    defaultPortBase: DEFAULT_ORDERER_PORT_BASE,
    capacityUnits: 1,
  },
});

const toHostKey = value => normalizeText(value).toLowerCase();
const toOrganizationKey = value => normalizeText(value).toLowerCase();

const dedupeIssues = issues =>
  Array.from(
    new Set(
      (Array.isArray(issues) ? issues : []).map(issue => normalizeText(issue)).filter(Boolean)
    )
  );

const normalizeOperationType = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (INCREMENTAL_NODE_OPERATION_SET.has(normalized)) {
    return normalized;
  }

  if (
    normalized === 'peer' ||
    normalized === 'addpeer' ||
    normalized === 'add-peer' ||
    normalized === 'peer_add'
  ) {
    return INCREMENTAL_NODE_OPERATION_TYPE.addPeer;
  }

  if (
    normalized === 'orderer' ||
    normalized === 'addorderer' ||
    normalized === 'add-orderer' ||
    normalized === 'orderer_add'
  ) {
    return INCREMENTAL_NODE_OPERATION_TYPE.addOrderer;
  }

  return '';
};

const resolveOrganizationByName = (organizationMap, organizationName) =>
  organizationMap.get(toOrganizationKey(organizationName)) || null;

const resolveOrganizationPortBase = (organization, operationType) => {
  const operationConfig = NODE_OPERATION_CONFIG[operationType];
  if (!operationConfig) {
    return null;
  }

  const operationPort =
    operationType === INCREMENTAL_NODE_OPERATION_TYPE.addPeer
      ? Number(organization && organization.peerPortBase)
      : Number(organization && organization.ordererPortBase);

  if (Number.isInteger(operationPort) && operationPort > 0 && operationPort <= MAX_ALLOWED_PORT) {
    return operationPort;
  }

  return operationConfig.defaultPortBase;
};

const resolveOrganizationDefaultHostRef = (organization, operationType) => {
  const operationConfig = NODE_OPERATION_CONFIG[operationType];
  if (!operationConfig) {
    return '';
  }
  return normalizeText(organization && organization[operationConfig.hostField]);
};

const resolveTrackedNodeRows = (organization, operationType) => {
  const operationConfig = NODE_OPERATION_CONFIG[operationType];
  if (!operationConfig) {
    return [];
  }

  return Array.isArray(organization && organization[operationConfig.trackedNodesField])
    ? organization[operationConfig.trackedNodesField]
    : [];
};

const buildNodeName = ({ operationType, componentIndex, organizationName, organizationDomain }) => {
  const operationConfig = NODE_OPERATION_CONFIG[operationType];
  if (!operationConfig) {
    return '';
  }

  const organizationToken = normalizeDnsLabel(organizationName) || 'org';
  const domainToken = normalizeDomain(organizationDomain) || `${organizationToken}.local`;

  return `${operationConfig.componentPrefix}${componentIndex}.${organizationToken}.${domainToken}`;
};

const registerHostPort = (registry, hostRef, port, descriptor) => {
  const hostKey = toHostKey(hostRef);
  const parsedPort = Number(port);
  if (
    !hostKey ||
    !Number.isInteger(parsedPort) ||
    parsedPort <= 0 ||
    parsedPort > MAX_ALLOWED_PORT
  ) {
    return;
  }

  if (!registry.has(hostKey)) {
    registry.set(hostKey, new Map());
  }

  const hostPortMap = registry.get(hostKey);
  if (!hostPortMap.has(parsedPort)) {
    hostPortMap.set(parsedPort, normalizeText(descriptor));
  }
};

const resolveDeterministicNodePort = (basePort, componentIndex) => {
  const parsedBasePort = Number(basePort);
  const parsedComponentIndex = toSafePositiveInt(componentIndex);

  if (
    !Number.isInteger(parsedBasePort) ||
    parsedBasePort <= 0 ||
    parsedBasePort > MAX_ALLOWED_PORT
  ) {
    return null;
  }

  const candidatePort = parsedBasePort + parsedComponentIndex * NODE_PORT_STRIDE;
  if (!Number.isInteger(candidatePort) || candidatePort <= 0 || candidatePort > MAX_ALLOWED_PORT) {
    return null;
  }

  return candidatePort;
};

const cloneHostPortRegistry = registry => {
  const nextRegistry = new Map();
  (registry instanceof Map ? registry : new Map()).forEach((portMap, hostKey) => {
    nextRegistry.set(hostKey, new Map(portMap));
  });
  return nextRegistry;
};

const buildBaselineNodeState = organizations => {
  const organizationMap = new Map();
  const nextIndexMap = new Map();
  const existingNames = new Set();
  const hostPorts = new Map();

  (Array.isArray(organizations) ? organizations : []).forEach(organization => {
    const organizationName = normalizeText(organization && organization.name);
    if (!organizationName) {
      return;
    }

    const organizationKey = toOrganizationKey(organizationName);
    organizationMap.set(organizationKey, organization);

    [INCREMENTAL_NODE_OPERATION_TYPE.addPeer, INCREMENTAL_NODE_OPERATION_TYPE.addOrderer].forEach(
      operationType => {
        const operationConfig = NODE_OPERATION_CONFIG[operationType];
        const baselineCount = toSafePositiveInt(
          organization && organization[operationConfig.countField]
        );
        const portBase = resolveOrganizationPortBase(organization, operationType);
        const defaultHostRef = resolveOrganizationDefaultHostRef(organization, operationType);

        nextIndexMap.set(`${organizationKey}|${operationType}`, baselineCount);

        for (let index = 0; index < baselineCount; index += 1) {
          const componentName = buildNodeName({
            operationType,
            componentIndex: index,
            organizationName,
            organizationDomain: organization && organization.domain,
          });

          if (componentName) {
            existingNames.add(componentName.toLowerCase());
          }

          registerHostPort(
            hostPorts,
            defaultHostRef,
            resolveDeterministicNodePort(portBase, index),
            componentName
          );
        }

        resolveTrackedNodeRows(organization, operationType).forEach(nodeRow => {
          const trackedName = normalizeText(nodeRow && nodeRow.component_name);
          const trackedIndex = toSafePositiveInt(nodeRow && nodeRow.component_index);
          const trackedHostRef = normalizeText(nodeRow && nodeRow.host_ref);
          const trackedPort = Number(nodeRow && nodeRow.port);

          if (trackedName) {
            existingNames.add(trackedName.toLowerCase());
          }

          registerHostPort(hostPorts, trackedHostRef, trackedPort, trackedName || operationType);

          const nextIndexKey = `${organizationKey}|${operationType}`;
          const currentNextIndex = toSafePositiveInt(nextIndexMap.get(nextIndexKey));
          if (trackedIndex + 1 > currentNextIndex) {
            nextIndexMap.set(nextIndexKey, trackedIndex + 1);
          }
        });
      }
    );
  });

  return {
    organizationMap,
    nextIndexMap,
    existingNames,
    hostPorts,
  };
};

const normalizeNodeIntentRows = nodeExpansions =>
  (Array.isArray(nodeExpansions) ? nodeExpansions : []).flatMap((draft, index) => {
    const draftId = normalizeText(draft && draft.id) || `increment-node-${index + 1}`;
    const organizationName = normalizeText(draft && draft.organizationName);
    const targetHostRef = normalizeText(draft && draft.targetHostRef);
    const legacyAddPeers = toSafePositiveInt(draft && draft.addPeers);
    const legacyAddOrderers = toSafePositiveInt(draft && draft.addOrderers);

    if (legacyAddPeers > 0 || legacyAddOrderers > 0) {
      const legacyIntents = [];

      if (legacyAddPeers > 0) {
        legacyIntents.push({
          draftId,
          organizationName,
          operationType: INCREMENTAL_NODE_OPERATION_TYPE.addPeer,
          requestedCount: legacyAddPeers,
          targetHostRef,
          sourceMode: 'legacy_scale',
        });
      }

      if (legacyAddOrderers > 0) {
        legacyIntents.push({
          draftId,
          organizationName,
          operationType: INCREMENTAL_NODE_OPERATION_TYPE.addOrderer,
          requestedCount: legacyAddOrderers,
          targetHostRef,
          sourceMode: 'legacy_scale',
        });
      }

      return legacyIntents;
    }

    return [
      {
        draftId,
        organizationName,
        operationType: normalizeOperationType(draft && draft.operationType),
        requestedCount: toSafePositiveInt(draft && draft.requestedCount),
        targetHostRef,
        sourceMode: 'assistant',
      },
    ];
  });

const resolveMachineMap = machines => {
  const machineMap = new Map();

  (Array.isArray(machines) ? machines : []).forEach(machine => {
    const hostTokens = [
      normalizeText(machine && machine.infraLabel),
      normalizeText(machine && machine.id),
    ]
      .filter(Boolean)
      .map(hostToken => ({
        hostToken,
        hostKey: toHostKey(hostToken),
      }))
      .filter(entry => Boolean(entry.hostKey));

    hostTokens.forEach(entry => {
      machineMap.set(entry.hostKey, {
        hostRef: entry.hostToken,
        hostKey: entry.hostKey,
        machine,
      });
    });
  });

  return machineMap;
};

const resolvePreflightHostMap = preflightReport => {
  const preflightHostMap = new Map();

  (Array.isArray(preflightReport && preflightReport.hosts) ? preflightReport.hosts : []).forEach(
    host => {
      [
        normalizeText(host && host.infraLabel),
        normalizeText(host && host.id),
        normalizeText(host && host.hostAddress),
      ]
        .filter(Boolean)
        .forEach(hostToken => {
          preflightHostMap.set(toHostKey(hostToken), host);
        });
    }
  );

  return preflightHostMap;
};

const estimateHostCapacityUnits = runtimeSnapshot => {
  const cpuCores = Number(runtimeSnapshot && runtimeSnapshot.cpuCores);
  const memoryMb = Number(runtimeSnapshot && runtimeSnapshot.memoryMb);
  const diskGb = Number(runtimeSnapshot && runtimeSnapshot.diskGb);

  if (!Number.isFinite(cpuCores) || !Number.isFinite(memoryMb) || !Number.isFinite(diskGb)) {
    return 0;
  }

  const cpuUnits = Math.max(0, Math.floor(cpuCores * 4));
  const memoryUnits = Math.max(0, Math.floor(memoryMb / 512));
  const diskUnits = Math.max(0, Math.floor(diskGb / 8));

  return Math.max(0, Math.min(cpuUnits, memoryUnits, diskUnits));
};

export const buildIncrementalNodeNamingPreview = ({
  changeId,
  runId,
  generatedAtUtc,
  organizations,
  machines,
  preflightReport,
  nodeExpansions,
}) => {
  const safeChangeId = normalizeText(changeId);
  const safeRunId = normalizeText(runId);
  const safeTimestamp = normalizeText(generatedAtUtc) || new Date().toISOString();
  const intents = normalizeNodeIntentRows(nodeExpansions);

  const baseline = buildBaselineNodeState(organizations);
  const machineMap = resolveMachineMap(machines);
  const hasMachineCatalog = machineMap.size > 0;
  const preflightHostMap = resolvePreflightHostMap(preflightReport);
  const hasPreflightContext = preflightHostMap.size > 0;

  const nextIndexMap = new Map(baseline.nextIndexMap);
  const nextNames = new Set(baseline.existingNames);
  const nextHostPorts = cloneHostPortRegistry(baseline.hostPorts);
  const issues = [];
  const operations = [];
  const plannedUnitsByHost = {};
  const plannedOperationsByHost = {};

  intents.forEach(intent => {
    const operationType = normalizeOperationType(intent.operationType);
    const operationConfig = NODE_OPERATION_CONFIG[operationType];
    const organizationName = normalizeText(intent.organizationName);
    const draftLabel = normalizeText(intent.draftId) || 'increment-node';

    if (!organizationName) {
      issues.push(
        localizeIncrementalText(
          `Organization obrigatória para expansão de nodes (${draftLabel}).`,
          `Organization is required for node expansion (${draftLabel}).`
        )
      );
      return;
    }

    const fallbackOrganization =
      baseline.organizationMap.size === 0
        ? {
            name: organizationName,
            domain: '',
            peerPortBase: DEFAULT_PEER_PORT_BASE,
            ordererPortBase: DEFAULT_ORDERER_PORT_BASE,
            peerHostRef: '',
            ordererHostRef: '',
            peers: 0,
            orderers: 0,
          }
        : null;

    const organization =
      resolveOrganizationByName(baseline.organizationMap, organizationName) || fallbackOrganization;
    if (!organization) {
      issues.push(
        localizeIncrementalText(
          `Expansão ${draftLabel} referencia organization inexistente (${organizationName}).`,
          `Expansion ${draftLabel} references a non-existent organization (${organizationName}).`
        )
      );
      return;
    }

    if (!operationConfig) {
      issues.push(
        localizeIncrementalText(
          `operation_type inválido para ${draftLabel}; use add_peer ou add_orderer.`,
          `Invalid operation_type for ${draftLabel}; use add_peer or add_orderer.`
        )
      );
      return;
    }

    if (!Number.isInteger(intent.requestedCount) || intent.requestedCount <= 0) {
      issues.push(
        localizeIncrementalText(
          `requestedCount inválido para ${draftLabel}; informe inteiro maior que zero.`,
          `Invalid requestedCount for ${draftLabel}; enter an integer greater than zero.`
        )
      );
      return;
    }

    const resolvedHostRef =
      normalizeText(intent.targetHostRef) ||
      resolveOrganizationDefaultHostRef(organization, operationType);

    if (!resolvedHostRef) {
      issues.push(
        localizeIncrementalText(
          `${operationType} em ${draftLabel} sem host alvo. Defina targetHostRef ou host default da organização.`,
          `${operationType} in ${draftLabel} has no target host. Define targetHostRef or the organization's default host.`
        )
      );
    }

    const resolvedHostKey = toHostKey(resolvedHostRef);
    const machineBinding = machineMap.get(resolvedHostKey);
    const machine = machineBinding && machineBinding.machine;

    if (hasMachineCatalog && resolvedHostRef && !machine) {
      issues.push(
        localizeIncrementalText(
          `Host alvo inválido para ${draftLabel}: ${resolvedHostRef} não existe no cadastro da etapa Infra.`,
          `Invalid target host for ${draftLabel}: ${resolvedHostRef} does not exist in the Infra step catalog.`
        )
      );
    }

    if (machine && !normalizeText(machine.sshCredentialRef)) {
      issues.push(
        localizeIncrementalText(
          `Host ${resolvedHostRef} sem sshCredentialRef para ${draftLabel}; complete credencial antes do incremento.`,
          `Host ${resolvedHostRef} has no sshCredentialRef for ${draftLabel}; complete the credential before the increment.`
        )
      );
    }

    const organizationKey = toOrganizationKey(organizationName);
    const nextIndexKey = `${organizationKey}|${operationType}`;
    const basePort = resolveOrganizationPortBase(organization, operationType);

    for (let sequence = 0; sequence < intent.requestedCount; sequence += 1) {
      const componentIndex = toSafePositiveInt(nextIndexMap.get(nextIndexKey));
      nextIndexMap.set(nextIndexKey, componentIndex + 1);

      const componentName = buildNodeName({
        operationType,
        componentIndex,
        organizationName,
        organizationDomain: organization && organization.domain,
      });

      const componentNameKey = componentName.toLowerCase();
      if (componentName && nextNames.has(componentNameKey)) {
        issues.push(
          localizeIncrementalText(
            `Colisão de naming incremental em ${draftLabel}: ${componentName} já existe na baseline ou no plano atual.`,
            `Incremental naming collision in ${draftLabel}: ${componentName} already exists in the baseline or current plan.`
          )
        );
      } else if (componentName) {
        nextNames.add(componentNameKey);
      }

      const targetPort = resolveDeterministicNodePort(basePort, componentIndex);
      const isValidTargetPort =
        Number.isInteger(targetPort) && targetPort > 0 && targetPort <= MAX_ALLOWED_PORT;

      if (!isValidTargetPort) {
        issues.push(
          localizeIncrementalText(
            `Porta incremental inválida para ${componentName || draftLabel}: ${targetPort}.`,
            `Invalid incremental port for ${componentName || draftLabel}: ${targetPort}.`
          )
        );
      }

      if (resolvedHostKey && isValidTargetPort) {
        if (!nextHostPorts.has(resolvedHostKey)) {
          nextHostPorts.set(resolvedHostKey, new Map());
        }

        const hostPortMap = nextHostPorts.get(resolvedHostKey);
        const existingPortOwner = normalizeText(hostPortMap.get(targetPort));

        if (existingPortOwner) {
          issues.push(
            localizeIncrementalText(
              `Conflito de porta no host ${resolvedHostRef}: ${targetPort} já reservado por ${existingPortOwner}.`,
              `Port conflict on host ${resolvedHostRef}: ${targetPort} is already reserved by ${existingPortOwner}.`
            )
          );
        } else {
          hostPortMap.set(targetPort, componentName || operationType);
        }

        plannedUnitsByHost[resolvedHostKey] =
          (plannedUnitsByHost[resolvedHostKey] || 0) + operationConfig.capacityUnits;
        plannedOperationsByHost[resolvedHostKey] =
          (plannedOperationsByHost[resolvedHostKey] || 0) + 1;
      }

      const operationPayload = {
        change_id: safeChangeId,
        run_id: safeRunId,
        operation_type: operationType,
        organization_name: organizationName,
        target_host_ref: resolvedHostRef,
        component_name: componentName,
        component_index: componentIndex,
        target_port: targetPort,
      };

      operations.push({
        source_draft_id: draftLabel,
        source_mode: intent.sourceMode,
        operation_type: operationType,
        operation_id: deterministicId(operationConfig.operationIdPrefix, operationPayload),
        change_id: safeChangeId,
        run_id: safeRunId,
        generated_at_utc: safeTimestamp,
        organization_name: organizationName,
        target_host_ref: resolvedHostRef,
        component_name: componentName,
        component_index: componentIndex,
        target_port: targetPort,
      });
    }
  });

  const hostCapacity = Object.keys(plannedOperationsByHost)
    .map(hostKey => {
      const machineBinding = machineMap.get(hostKey);
      const preflightHost = preflightHostMap.get(hostKey);
      const hostRef =
        normalizeText(machineBinding && machineBinding.hostRef) ||
        normalizeText(preflightHost && preflightHost.infraLabel) ||
        hostKey;

      const plannedUnits = toSafePositiveInt(plannedUnitsByHost[hostKey]);
      const plannedOperations = toSafePositiveInt(plannedOperationsByHost[hostKey]);
      const availableUnits = estimateHostCapacityUnits(
        preflightHost && preflightHost.runtimeSnapshot
      );
      const preflightStatus = normalizeText(preflightHost && preflightHost.status).toLowerCase();

      if (hasPreflightContext) {
        if (!preflightHost) {
          issues.push(
            localizeIncrementalText(
              `Host ${hostRef} sem evidência de preflight para validar capacidade incremental.`,
              `Host ${hostRef} has no preflight evidence to validate incremental capacity.`
            )
          );
        } else if (preflightStatus !== 'apto') {
          issues.push(
            localizeIncrementalText(
              `Host ${hostRef} não está apto no preflight atual (${preflightStatus ||
                'desconhecido'}).`,
              `Host ${hostRef} is not ready in the current preflight (${preflightStatus ||
                'unknown'}).`
            )
          );
        } else if (availableUnits <= 0) {
          issues.push(
            localizeIncrementalText(
              `Capacidade indisponível no host ${hostRef}: runtimeSnapshot sem métricas válidas de CPU/memória/disco.`,
              `Capacity unavailable on host ${hostRef}: runtimeSnapshot has no valid CPU/memory/disk metrics.`
            )
          );
        } else if (plannedUnits > availableUnits) {
          issues.push(
            localizeIncrementalText(
              `Capacidade insuficiente no host ${hostRef}: planejado ${plannedUnits} unidade(s), limite ${availableUnits}.`,
              `Insufficient capacity on host ${hostRef}: planned ${plannedUnits} unit(s), limit ${availableUnits}.`
            )
          );
        }
      }

      let status = 'unknown';
      if (preflightStatus === 'apto') {
        status = 'apto';
      } else if (preflightStatus) {
        status = preflightStatus;
      }

      return {
        host_ref: hostRef,
        host_key: hostKey,
        status,
        planned_operations: plannedOperations,
        planned_units: plannedUnits,
        available_units: availableUnits,
      };
    })
    .sort((left, right) =>
      normalizeText(left.host_ref).localeCompare(normalizeText(right.host_ref))
    );

  const sortedOperations = [...operations].sort((left, right) =>
    normalizeText(left.operation_id).localeCompare(normalizeText(right.operation_id))
  );

  return {
    operations: sortedOperations,
    hostCapacity,
    issues: dedupeIssues(issues),
  };
};

export const createIncrementalNodeExpansionDraft = (index = 1, overrides = {}) => ({
  id: `increment-node-${index}`,
  organizationName: '',
  operationType: INCREMENTAL_NODE_OPERATION_TYPE.addPeer,
  targetHostRef: '',
  requestedCount: 1,
  addPeers: 0,
  addOrderers: 0,
  ...overrides,
});

export const createIncrementalChannelDraft = (index = 1) => ({
  id: `increment-channel-${index}`,
  name: '',
  memberOrgs: '',
});

export const createIncrementalInstallDraft = (index = 1) => ({
  id: `increment-install-${index}`,
  channel: '',
  endpointPath: '',
  packagePattern: 'cc-tools',
});

export const createIncrementalApiDraft = (index = 1) => ({
  id: `increment-api-${index}`,
  organizationName: '',
  channel: '',
  chaincodeId: 'cc-tools',
  routePath: '',
  exposureHost: '',
  exposurePort: null,
  status: 'active',
});

const toOrganizationNameSet = organizations =>
  new Set(
    (Array.isArray(organizations) ? organizations : [])
      .map(organization => normalizeText(organization.name))
      .filter(Boolean)
  );

const toChannelNameSet = channels =>
  new Set(
    (Array.isArray(channels) ? channels : [])
      .map(channel => normalizeText(channel.name))
      .filter(Boolean)
  );

const toInstallKey = install =>
  `${normalizeText(install.channel)}::${normalizeText(
    install.packagePattern || install.chaincodeId || ''
  )}`;

const toApiKey = api =>
  [
    normalizeText(api.organizationName),
    normalizeText(api.channel),
    normalizeText(api.chaincodeId),
    normalizeText(api.routePath),
  ].join('|');

export const buildIncrementalExpansionIssues = ({
  changeId,
  preflightApproved,
  lintApproved,
  organizations,
  channels,
  chaincodeInstalls,
  apiRegistrations,
  machines,
  preflightReport,
  nodeExpansions,
  channelAdditions,
  installAdditions,
  apiAdditions,
}) => {
  const issues = [];

  if (!normalizeText(changeId)) {
    issues.push(
      localizeIncrementalText(
        'change_id obrigatório para aplicar expansão incremental.',
        'change_id is required to apply incremental expansion.'
      )
    );
  }
  if (!preflightApproved) {
    issues.push(
      localizeIncrementalText(
        'Expansão incremental bloqueada: preflight deve permanecer apto.',
        'Incremental expansion blocked: preflight must remain ready.'
      )
    );
  }
  if (!lintApproved) {
    issues.push(
      localizeIncrementalText(
        'Expansão incremental bloqueada: lint A1.2 deve estar aprovado para o modelo atual.',
        'Incremental expansion blocked: lint A1.2 must be approved for the current model.'
      )
    );
  }

  const nodePreview = buildIncrementalNodeNamingPreview({
    changeId,
    organizations,
    machines,
    preflightReport,
    nodeExpansions,
  });

  issues.push(...nodePreview.issues);

  const organizationNameSet = toOrganizationNameSet(organizations);
  const existingChannelSet = toChannelNameSet(channels);
  const nextChannelSet = new Set(existingChannelSet);

  const existingInstallKeys = new Set(
    (Array.isArray(chaincodeInstalls) ? chaincodeInstalls : [])
      .map(toInstallKey)
      .filter(key => key !== '::')
  );
  const existingApiKeys = new Set(
    (Array.isArray(apiRegistrations) ? apiRegistrations : [])
      .map(toApiKey)
      .filter(key => key !== '|||')
  );

  (Array.isArray(channelAdditions) ? channelAdditions : []).forEach(channel => {
    const channelName = normalizeText(channel.name);
    const memberOrgs = parseMembersCsv(channel.memberOrgs);

    if (!channelName) {
      issues.push(
        localizeIncrementalText(
          `Nome do channel obrigatório (${channel.id}).`,
          `Channel name is required (${channel.id}).`
        )
      );
      return;
    }
    if (nextChannelSet.has(channelName)) {
      issues.push(
        localizeIncrementalText(
          `Channel incremental duplicado ou já existente (${channelName}).`,
          `Incremental channel duplicated or already exists (${channelName}).`
        )
      );
      return;
    }
    if (memberOrgs.length === 0) {
      issues.push(
        localizeIncrementalText(
          `Informe organizations membro para o channel incremental ${channelName}.`,
          `Provide member organizations for incremental channel ${channelName}.`
        )
      );
    }
    memberOrgs.forEach(memberOrg => {
      if (!organizationNameSet.has(memberOrg)) {
        issues.push(
          localizeIncrementalText(
            `Channel incremental ${channelName} referencia org inexistente (${memberOrg}).`,
            `Incremental channel ${channelName} references a non-existent organization (${memberOrg}).`
          )
        );
      }
    });

    nextChannelSet.add(channelName);
  });

  const nextInstallKeys = new Set(existingInstallKeys);
  (Array.isArray(installAdditions) ? installAdditions : []).forEach(install => {
    const targetChannel = normalizeText(install.channel);
    const packagePattern = normalizeText(install.packagePattern || 'cc-tools').toLowerCase();

    if (!targetChannel) {
      issues.push(
        localizeIncrementalText(
          `Channel alvo obrigatório para incremento de install (${install.id}).`,
          `Target channel is required for install increment (${install.id}).`
        )
      );
      return;
    }
    if (!nextChannelSet.has(targetChannel)) {
      issues.push(
        localizeIncrementalText(
          `Install incremental ${install.id} referencia channel inexistente (${targetChannel}).`,
          `Incremental install ${install.id} references a non-existent channel (${targetChannel}).`
        )
      );
    }
    if (packagePattern !== 'cc-tools') {
      issues.push(
        localizeIncrementalText(
          `Somente pacote cc-tools é permitido no incremento (${install.id}).`,
          `Only the cc-tools package is allowed in the increment (${install.id}).`
        )
      );
    }

    const installKey = `${targetChannel}::cc-tools`;
    if (nextInstallKeys.has(installKey)) {
      issues.push(
        localizeIncrementalText(
          `Install incremental duplicado para ${targetChannel} (cc-tools).`,
          `Incremental install duplicated for ${targetChannel} (cc-tools).`
        )
      );
    }
    nextInstallKeys.add(installKey);

    const endpointPath = normalizeText(install.endpointPath);
    if (endpointPath && !endpointPath.startsWith('/api/')) {
      issues.push(
        localizeIncrementalText(
          `Endpoint incremental do ${install.id} deve iniciar com /api/.`,
          `Incremental endpoint for ${install.id} must start with /api/.`
        )
      );
    }
  });

  const activeChaincodePairs = new Set(
    Array.from(nextInstallKeys)
      .map(key => {
        const [channelName, chaincodeName] = key.split('::');
        if (!channelName || !chaincodeName) {
          return '';
        }
        return `${channelName}::${chaincodeName}`;
      })
      .filter(Boolean)
  );

  const nextApiKeys = new Set(existingApiKeys);
  (Array.isArray(apiAdditions) ? apiAdditions : []).forEach(api => {
    const apiId = normalizeText(api.id) || 'increment-api';
    const organizationName = normalizeText(api.organizationName);
    const channelName = normalizeText(api.channel);
    const chaincodeId = normalizeText(api.chaincodeId || 'cc-tools');
    const operation = normalizeSegment(api.operation);
    const functionName = normalizeSegment(api.functionName);
    const routePath =
      normalizeText(api.routePath) ||
      buildApiRoutePath({
        channelId: channelName,
        chaincodeId,
        operation,
        fn: functionName,
      });

    if (!organizationName) {
      issues.push(
        localizeIncrementalText(
          `Organization obrigatória para API incremental (${apiId}).`,
          `Organization is required for incremental API (${apiId}).`
        )
      );
    } else if (!organizationNameSet.has(organizationName)) {
      issues.push(
        localizeIncrementalText(
          `API incremental ${apiId} referencia organization inexistente (${organizationName}).`,
          `Incremental API ${apiId} references a non-existent organization (${organizationName}).`
        )
      );
    }

    if (!channelName) {
      issues.push(
        localizeIncrementalText(
          `Channel obrigatório para API incremental (${apiId}).`,
          `Channel is required for incremental API (${apiId}).`
        )
      );
    } else if (!nextChannelSet.has(channelName)) {
      issues.push(
        localizeIncrementalText(
          `API incremental ${apiId} referencia channel inexistente (${channelName}).`,
          `Incremental API ${apiId} references a non-existent channel (${channelName}).`
        )
      );
    }

    if (!activeChaincodePairs.has(`${channelName}::${chaincodeId}`)) {
      issues.push(
        localizeIncrementalText(
          `API incremental ${apiId} exige channel+chaincode ativo (${channelName}/${chaincodeId}).`,
          `Incremental API ${apiId} requires an active channel+chaincode pair (${channelName}/${chaincodeId}).`
        )
      );
    }

    if (!routePath.startsWith('/api/')) {
      issues.push(
        localizeIncrementalText(
          `Rota da API incremental ${apiId} deve iniciar com /api/.`,
          `Incremental API route ${apiId} must start with /api/.`
        )
      );
    }

    const apiKey = `${organizationName}|${channelName}|${chaincodeId}|${routePath}`;
    if (nextApiKeys.has(apiKey)) {
      issues.push(
        localizeIncrementalText(
          `API incremental duplicada para ${organizationName}/${channelName}/${routePath}.`,
          `Incremental API duplicated for ${organizationName}/${channelName}/${routePath}.`
        )
      );
    }
    nextApiKeys.add(apiKey);
  });

  return dedupeIssues(issues);
};

const normalizeNodeOperationPayload = operation => {
  const parsedPort = Number(operation && operation.target_port);

  return {
    sourceDraftId: normalizeText(operation && operation.source_draft_id),
    organizationName: normalizeText(operation && operation.organization_name),
    operationType: normalizeText(operation && operation.operation_type),
    componentName: normalizeText(operation && operation.component_name),
    componentIndex: toSafePositiveInt(operation && operation.component_index),
    hostRef: normalizeText(operation && operation.target_host_ref),
    port:
      Number.isInteger(parsedPort) && parsedPort > 0 && parsedPort <= MAX_ALLOWED_PORT
        ? parsedPort
        : null,
  };
};

const normalizeChannelAddition = channel => ({
  id: normalizeText(channel.id),
  name: normalizeText(channel.name),
  memberOrgs: parseMembersCsv(channel.memberOrgs),
});

const normalizeInstallAddition = install => ({
  id: normalizeText(install.id),
  channel: normalizeText(install.channel),
  packagePattern: 'cc-tools',
  endpointPath: normalizeText(install.endpointPath),
});

const normalizeApiAddition = api => {
  const channelName = normalizeText(api.channel);
  const chaincodeId = normalizeText(api.chaincodeId || 'cc-tools');
  const parsedExposurePort = Number(api.exposurePort);

  return {
    id: normalizeText(api.id),
    organizationName: normalizeText(api.organizationName),
    channel: channelName,
    chaincodeId,
    routePath: buildApiRoutePath({
      channelId: channelName,
      chaincodeId,
    }),
    exposureHost: normalizeText(api.exposureHost),
    exposurePort:
      Number.isInteger(parsedExposurePort) && parsedExposurePort > 0 && parsedExposurePort <= 65535
        ? parsedExposurePort
        : null,
    status: normalizeText(api.status || 'active') || 'active',
  };
};

export const buildIncrementalExpansionPlan = ({
  changeId,
  runId,
  executionContext,
  generatedAtUtc,
  organizations,
  machines,
  preflightReport,
  nodeExpansions,
  channelAdditions,
  installAdditions,
  apiAdditions,
}) => {
  const safeTimestamp = normalizeText(generatedAtUtc) || new Date().toISOString();
  const safeChangeId = normalizeText(changeId);
  const safeRunId = normalizeText(runId);
  const safeExecutionContext = normalizeText(executionContext);

  const nodePreview = buildIncrementalNodeNamingPreview({
    changeId: safeChangeId,
    runId: safeRunId,
    generatedAtUtc: safeTimestamp,
    organizations,
    machines,
    preflightReport,
    nodeExpansions,
  });

  const nodeOperations = nodePreview.operations.map(operation => ({
    operation_type: normalizeText(operation.operation_type),
    operation_id: normalizeText(operation.operation_id),
    payload: normalizeNodeOperationPayload(operation),
  }));

  const channelOperations = (Array.isArray(channelAdditions) ? channelAdditions : [])
    .map(normalizeChannelAddition)
    .filter(channel => channel.name)
    .map(channel => ({
      operation_type: 'channel_add',
      operation_id: deterministicId('inc-channel', {
        change_id: safeChangeId,
        run_id: safeRunId,
        payload: channel,
      }),
      payload: channel,
    }));

  const installOperations = (Array.isArray(installAdditions) ? installAdditions : [])
    .map(normalizeInstallAddition)
    .filter(install => install.channel)
    .map(install => ({
      operation_type: 'chaincode_install_add',
      operation_id: deterministicId('inc-install', {
        change_id: safeChangeId,
        run_id: safeRunId,
        payload: install,
      }),
      payload: install,
    }));

  const apiOperations = (Array.isArray(apiAdditions) ? apiAdditions : [])
    .map(normalizeApiAddition)
    .filter(api => api.organizationName && api.channel && api.chaincodeId)
    .map(api => ({
      operation_type: 'api_add',
      operation_id: deterministicId('inc-api', {
        change_id: safeChangeId,
        run_id: safeRunId,
        payload: api,
      }),
      payload: api,
    }));

  return [...nodeOperations, ...channelOperations, ...installOperations, ...apiOperations]
    .map(operation => ({
      ...operation,
      change_id: safeChangeId,
      run_id: safeRunId,
      execution_context: safeExecutionContext,
      generated_at_utc: safeTimestamp,
    }))
    .sort((left, right) => left.operation_id.localeCompare(right.operation_id));
};

const sortTrackedNodes = nodes =>
  [...(Array.isArray(nodes) ? nodes : [])].sort((left, right) => {
    const leftIndex = toSafePositiveInt(left && left.component_index);
    const rightIndex = toSafePositiveInt(right && right.component_index);
    if (leftIndex !== rightIndex) {
      return leftIndex - rightIndex;
    }

    return normalizeText(left && left.component_name).localeCompare(
      normalizeText(right && right.component_name)
    );
  });

export const applyIncrementalExpansionToModel = ({
  changeId,
  runId,
  organizations,
  channels,
  chaincodeInstalls,
  apiRegistrations,
  machines,
  preflightReport,
  nodeExpansions,
  channelAdditions,
  installAdditions,
  apiAdditions,
}) => {
  const nodePreview = buildIncrementalNodeNamingPreview({
    changeId,
    runId,
    organizations,
    machines,
    preflightReport,
    nodeExpansions,
  });

  const normalizedNodeOperations = nodePreview.operations.map(normalizeNodeOperationPayload);

  const nodeOperationsByOrganization = normalizedNodeOperations.reduce((accumulator, operation) => {
    const organizationName = normalizeText(operation.organizationName);
    if (!organizationName) {
      return accumulator;
    }

    if (!accumulator[organizationName]) {
      accumulator[organizationName] = [];
    }

    accumulator[organizationName].push(operation);
    return accumulator;
  }, {});

  const nextOrganizations = (Array.isArray(organizations) ? organizations : []).map(
    organization => {
      const organizationName = normalizeText(organization && organization.name);
      const scopedOperations = nodeOperationsByOrganization[organizationName] || [];

      if (scopedOperations.length === 0) {
        return {
          ...organization,
        };
      }

      const peerOperations = scopedOperations.filter(
        operation => operation.operationType === INCREMENTAL_NODE_OPERATION_TYPE.addPeer
      );
      const ordererOperations = scopedOperations.filter(
        operation => operation.operationType === INCREMENTAL_NODE_OPERATION_TYPE.addOrderer
      );

      const trackedPeerNodes = sortTrackedNodes([
        ...resolveTrackedNodeRows(organization, INCREMENTAL_NODE_OPERATION_TYPE.addPeer).map(
          node => ({
            ...node,
          })
        ),
        ...peerOperations.map(operation => ({
          component_name: operation.componentName,
          component_index: operation.componentIndex,
          host_ref: operation.hostRef,
          port: operation.port,
          operation_type: operation.operationType,
          source_draft_id: operation.sourceDraftId,
        })),
      ]);

      const trackedOrdererNodes = sortTrackedNodes([
        ...resolveTrackedNodeRows(organization, INCREMENTAL_NODE_OPERATION_TYPE.addOrderer).map(
          node => ({
            ...node,
          })
        ),
        ...ordererOperations.map(operation => ({
          component_name: operation.componentName,
          component_index: operation.componentIndex,
          host_ref: operation.hostRef,
          port: operation.port,
          operation_type: operation.operationType,
          source_draft_id: operation.sourceDraftId,
        })),
      ]);

      return {
        ...organization,
        peers: Number(organization.peers || 0) + peerOperations.length,
        orderers: Number(organization.orderers || 0) + ordererOperations.length,
        incremental_peer_nodes: trackedPeerNodes,
        incremental_orderer_nodes: trackedOrdererNodes,
      };
    }
  );

  const existingChannelNames = new Set(
    (Array.isArray(channels) ? channels : [])
      .map(channel => normalizeText(channel.name))
      .filter(Boolean)
  );

  const nextChannels = [
    ...(Array.isArray(channels) ? channels : []).map(channel => ({ ...channel })),
    ...(Array.isArray(channelAdditions) ? channelAdditions : [])
      .map(normalizeChannelAddition)
      .filter(channel => {
        if (!channel.name || existingChannelNames.has(channel.name)) {
          return false;
        }
        existingChannelNames.add(channel.name);
        return true;
      })
      .map((channel, index) => ({
        id: channel.id || `channel-increment-${index + 1}`,
        name: channel.name,
        memberOrgs: channel.memberOrgs.join(','),
      })),
  ];

  const existingInstallKeys = new Set(
    (Array.isArray(chaincodeInstalls) ? chaincodeInstalls : [])
      .map(toInstallKey)
      .filter(key => key !== '::')
  );

  const nextInstalls = [
    ...(Array.isArray(chaincodeInstalls) ? chaincodeInstalls : []).map(install => ({ ...install })),
    ...(Array.isArray(installAdditions) ? installAdditions : [])
      .map(normalizeInstallAddition)
      .filter(install => {
        const installKey = `${install.channel}::cc-tools`;
        if (!install.channel || existingInstallKeys.has(installKey)) {
          return false;
        }
        existingInstallKeys.add(installKey);
        return true;
      })
      .map((install, index) => ({
        id: install.id || `install-increment-${index + 1}`,
        channel: install.channel,
        endpointPath: install.endpointPath,
        packagePattern: 'cc-tools',
      })),
  ];

  const existingApiKeys = new Set(
    (Array.isArray(apiRegistrations) ? apiRegistrations : [])
      .map(toApiKey)
      .filter(key => key !== '|||')
  );

  const nextApis = [
    ...(Array.isArray(apiRegistrations) ? apiRegistrations : []).map(api => ({ ...api })),
    ...(Array.isArray(apiAdditions) ? apiAdditions : [])
      .map(normalizeApiAddition)
      .filter(api => {
        const apiKey = toApiKey(api);
        if (
          !api.organizationName ||
          !api.channel ||
          !api.chaincodeId ||
          existingApiKeys.has(apiKey)
        ) {
          return false;
        }
        existingApiKeys.add(apiKey);
        return true;
      })
      .map((api, index) => ({
        id: api.id || `api-increment-${index + 1}`,
        organizationName: api.organizationName,
        channel: api.channel,
        chaincodeId: api.chaincodeId,
        routePath: api.routePath,
        exposureHost: api.exposureHost,
        exposurePort: api.exposurePort,
        status: api.status,
      })),
  ];

  return {
    organizations: nextOrganizations,
    channels: nextChannels,
    chaincodeInstalls: nextInstalls,
    apiRegistrations: nextApis,
  };
};
