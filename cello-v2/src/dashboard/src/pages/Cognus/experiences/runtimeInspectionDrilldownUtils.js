const normalizeText = value => String(value || '').trim();

const normalizeToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');

const COMPONENT_TYPE_ALIASES = {
  api: ['netapi', 'apiGateway'],
  apigateway: ['apiGateway'],
  ca: ['ca'],
  chaincode: ['chaincodeRuntime'],
  chaincoderuntime: ['chaincodeRuntime'],
  couch: ['couch'],
  netapi: ['netapi'],
  orderer: ['orderer'],
  peer: ['peer'],
};

const STATUS_PRIORITY = {
  running: 0,
  degraded: 1,
  unknown: 2,
  missing: 3,
  planned: 4,
  stopped: 5,
};

const resolveRequestedComponentTypes = componentType => {
  const normalizedType = normalizeToken(componentType);
  if (!normalizedType) {
    return [];
  }
  return COMPONENT_TYPE_ALIASES[normalizedType] || [componentType];
};

const resolveStatusPriority = status => {
  const normalizedStatus = normalizeToken(status);
  return Object.prototype.hasOwnProperty.call(STATUS_PRIORITY, normalizedStatus)
    ? STATUS_PRIORITY[normalizedStatus]
    : 999;
};

const buildInspectionCandidateScore = (row, selector, requestedTypes) => {
  const safeRow = row || {};
  let score = 0;

  const componentId = normalizeText(selector.componentId);
  const nodeId = normalizeText(selector.nodeId);
  const componentName = normalizeText(selector.componentName);
  const hostRef = normalizeText(selector.hostRef);
  const channelId = normalizeText(selector.channelId);
  const chaincodeId = normalizeText(selector.chaincodeId);
  const routePath = normalizeText(selector.routePath);

  if (componentId) {
    if (normalizeText(safeRow.componentId) !== componentId) {
      return -1;
    }
    score += 1000;
  }

  if (nodeId) {
    const normalizedNodeId = normalizeText(safeRow.nodeId);
    const normalizedComponentName = normalizeText(safeRow.componentName);
    if (normalizedNodeId !== nodeId && normalizedComponentName !== nodeId) {
      return -1;
    }
    score += normalizedNodeId === nodeId ? 900 : 700;
  }

  if (componentName) {
    if (normalizeText(safeRow.componentName) !== componentName) {
      return -1;
    }
    score += 650;
  }

  if (hostRef) {
    if (normalizeText(safeRow.hostRef) !== hostRef) {
      return -1;
    }
    score += 240;
  }

  if (channelId) {
    if (normalizeText(safeRow.channelId) !== channelId) {
      return -1;
    }
    score += 180;
  }

  if (chaincodeId) {
    if (normalizeText(safeRow.chaincodeId) !== chaincodeId) {
      return -1;
    }
    score += 160;
  }

  if (routePath) {
    if (normalizeText(safeRow.routePath) !== routePath) {
      return -1;
    }
    score += 120;
  }

  const typeIndex = requestedTypes.findIndex(type => type === safeRow.componentType);
  if (requestedTypes.length > 0) {
    if (typeIndex < 0) {
      return -1;
    }
    score += Math.max(80 - typeIndex * 10, 10);
  }

  score += safeRow.componentId ? 12 : 0;
  score += safeRow.hostRef ? 6 : 0;
  score -= resolveStatusPriority(safeRow.status);

  return score;
};

const resolveOfficialRuntimeInspectionRow = (rows, selector = {}) => {
  const safeRows = Array.isArray(rows) ? rows : [];
  const organizationId = normalizeText(selector.organizationId);
  const requestedTypes = resolveRequestedComponentTypes(selector.componentType);

  const filteredRows = safeRows.filter(row => {
    const safeRow = row || {};
    if (organizationId && normalizeText(safeRow.organizationId) !== organizationId) {
      return false;
    }
    return true;
  });

  const scoredRows = filteredRows
    .map(row => ({
      row,
      score: buildInspectionCandidateScore(row, selector, requestedTypes),
    }))
    .filter(candidate => candidate.score >= 0)
    .sort((left, right) => {
      if (left.score !== right.score) {
        return right.score - left.score;
      }
      const leftStatus = resolveStatusPriority(left.row && left.row.status);
      const rightStatus = resolveStatusPriority(right.row && right.row.status);
      if (leftStatus !== rightStatus) {
        return leftStatus - rightStatus;
      }
      return normalizeText(left.row && left.row.componentName).localeCompare(
        normalizeText(right.row && right.row.componentName)
      );
    });

  return scoredRows.length > 0 ? scoredRows[0].row : null;
};

export default resolveOfficialRuntimeInspectionRow;
