import { formatCognusTemplate, resolveCognusLocale } from '../cognusI18n';

const normalizeText = value => String(value || '').trim();

const localizeApiManagementText = (ptBR, enUS, values, localeCandidate) =>
  formatCognusTemplate(ptBR, enUS, values, localeCandidate || resolveCognusLocale());

const normalizeSegment = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

const isValidPort = value => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 && parsed <= 65535;
};

export const buildApiRoutePath = ({ channelId, chaincodeId }) => {
  const normalizedChannel = normalizeSegment(channelId) || '*';
  const normalizedChaincode = normalizeSegment(chaincodeId) || '*';
  return `/api/${normalizedChannel}/${normalizedChaincode}`;
};

export const buildApiExposureTarget = ({ organizationName, organizations, machines }) => {
  const normalizedOrgName = normalizeText(organizationName);
  const organization = (Array.isArray(organizations) ? organizations : []).find(
    current => normalizeText(current.name) === normalizedOrgName
  );

  const machineRegistry = (Array.isArray(machines) ? machines : []).reduce(
    (accumulator, machine) => {
      const machineLabel = normalizeText(machine.infraLabel || machine.id);
      if (!machineLabel) {
        return accumulator;
      }

      accumulator[machineLabel] = {
        hostAddress: normalizeText(machine.hostAddress),
      };
      return accumulator;
    },
    {}
  );

  const peerHostRef = normalizeText(organization && organization.peerHostRef);
  const ordererHostRef = normalizeText(organization && organization.ordererHostRef);

  const resolvedHost =
    normalizeText(machineRegistry[peerHostRef] && machineRegistry[peerHostRef].hostAddress) ||
    normalizeText(machineRegistry[ordererHostRef] && machineRegistry[ordererHostRef].hostAddress) ||
    normalizeText(organization && organization.networkApiHost);

  return {
    host: resolvedHost,
    port: (() => {
      if (organization && isValidPort(organization.apiGatewayPort)) {
        return Number(organization.apiGatewayPort);
      }
      if (organization && isValidPort(organization.networkApiPort)) {
        return Number(organization.networkApiPort);
      }
      return null;
    })(),
  };
};

export const buildApiManagementIssues = ({ apiRegistrations, organizations, changeId }) => {
  const issues = [];
  const organizationNames = new Set(
    (Array.isArray(organizations) ? organizations : [])
      .map(organization => normalizeText(organization.name))
      .filter(Boolean)
  );

  (Array.isArray(apiRegistrations) ? apiRegistrations : []).forEach(api => {
    const registrationId =
      normalizeText(api.id) || localizeApiManagementText('api-sem-id', 'api-no-id');
    const orgName = normalizeText(api.organizationName);
    const routePath = normalizeText(api.routePath);
    const expectedRoutePrefix = buildApiRoutePath({
      channelId: api.channel,
      chaincodeId: api.chaincodeId,
    });
    const exposureHost = normalizeText(api.exposureHost);

    if (!orgName) {
      issues.push(
        localizeApiManagementText(
          'Organization obrigatória para API ({registrationId}).',
          'Organization is required for API ({registrationId}).',
          { registrationId }
        )
      );
    } else if (!organizationNames.has(orgName)) {
      issues.push(
        localizeApiManagementText(
          'API {registrationId} referencia organization inexistente ({orgName}).',
          'API {registrationId} references a non-existing organization ({orgName}).',
          { registrationId, orgName }
        )
      );
    }

    if (!exposureHost) {
      issues.push(
        localizeApiManagementText(
          'Exposure host obrigatório para API {registrationId}.',
          'Exposure host is required for API {registrationId}.',
          { registrationId }
        )
      );
    }

    if (!isValidPort(api.exposurePort)) {
      issues.push(
        localizeApiManagementText(
          'Exposure port inválida para API {registrationId}.',
          'Exposure port is invalid for API {registrationId}.',
          { registrationId }
        )
      );
    }

    if (!routePath.startsWith('/api/')) {
      issues.push(
        localizeApiManagementText(
          'Rota da API {registrationId} deve seguir o padrão /api/{channel}/{chaincode}.',
          'API route {registrationId} must follow the /api/{channel}/{chaincode} pattern.',
          { registrationId }
        )
      );
    }

    if (routePath && routePath !== '/api/' && !routePath.startsWith(`${expectedRoutePrefix}/`)) {
      issues.push(
        localizeApiManagementText(
          'Rota da API {registrationId} fora do padrão esperado ({expectedRoutePrefix}/{operation}/{function}).',
          'API route {registrationId} is outside the expected pattern ({expectedRoutePrefix}/{operation}/{function}).',
          { registrationId, expectedRoutePrefix }
        )
      );
    }

    if (!normalizeText(changeId)) {
      issues.push(
        localizeApiManagementText(
          'change_id obrigatório para rastreabilidade de APIs ({registrationId}).',
          'change_id is required for API traceability ({registrationId}).',
          { registrationId }
        )
      );
    }
  });

  return issues;
};

export const buildApiRegistryEntries = ({
  apiRegistrations,
  changeId,
  executionContext,
  generatedAtUtc,
}) => {
  const safeTimestamp = normalizeText(generatedAtUtc) || new Date().toISOString();

  return (Array.isArray(apiRegistrations) ? apiRegistrations : [])
    .map(api => {
      const normalizedChannel = normalizeText(api.channel) || '*';
      const normalizedChaincodeId = normalizeText(api.chaincodeId) || '*';
      const routePath =
        normalizeText(api.routePath) ||
        `${buildApiRoutePath({
          channelId: normalizedChannel,
          chaincodeId: normalizedChaincodeId,
        })}/*/*`;

      return {
        api_id: normalizeText(api.id),
        org_name: normalizeText(api.organizationName),
        channel_id: normalizedChannel,
        chaincode_id: normalizedChaincodeId,
        operation: '*',
        function: '*',
        route_path: routePath,
        exposure: {
          host: normalizeText(api.exposureHost),
          port: Number(api.exposurePort),
        },
        status: normalizeText(api.status || 'active') || 'active',
        change_id: normalizeText(changeId),
        execution_context: normalizeText(executionContext),
        updated_at_utc: safeTimestamp,
      };
    })
    .filter(entry => entry.api_id && entry.org_name)
    .sort((left, right) => {
      const leftKey = `${left.org_name}|${left.channel_id}|${left.chaincode_id}|${left.route_path}`;
      const rightKey = `${right.org_name}|${right.channel_id}|${right.chaincode_id}|${right.route_path}`;
      return leftKey.localeCompare(rightKey);
    });
};
