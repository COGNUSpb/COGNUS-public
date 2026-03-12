import {
  buildOfficialRunEvidenceMeta,
  buildOrganizationRuntimeTopologyRows,
  buildOrganizationRuntimeTopologySummary,
  buildOrganizationWorkspaceReadModels,
} from './experiences/provisioningRuntimeTopologyUtils';
import { pickCognusText } from './cognusI18n';

const EMPTY_CARD_FILTERS = {
  organization: '',
  status: 'all',
  criticality: 'all',
  host: 'all',
  freshness: 'all',
};

const normalizeText = value => String(value || '').trim();

const normalizeToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');

const safeArray = value => (Array.isArray(value) ? value : []);

const uniq = values => Array.from(new Set(values.filter(Boolean)));

const resolveOrganizationWorkspace = (organization, workspaces) => {
  const organizationId = normalizeText(organization && organization.orgId);
  const organizationName = normalizeText(organization && organization.orgName);

  return (
    safeArray(workspaces).find(workspace => {
      const workspaceOrganization = workspace && workspace.organization;
      return (
        normalizeText(workspaceOrganization && workspaceOrganization.orgId) === organizationId ||
        normalizeText(workspaceOrganization && workspaceOrganization.orgName) === organizationName
      );
    }) || null
  );
};

const resolveOrganizationGateRow = (organization, gate) => {
  const organizationId = normalizeText(organization && organization.orgId);
  const organizationName = normalizeText(organization && organization.orgName);

  return (
    safeArray(gate && gate.organizationReadiness).find(row => {
      return (
        normalizeText(row && row.organizationId) === organizationId ||
        normalizeText(row && row.organizationName) === organizationName
      );
    }) || null
  );
};

const resolveHostLabels = ({ organization, topologyRows, workspace }) => {
  const rowHosts = safeArray(topologyRows)
    .flatMap(row => [normalizeText(row && row.hostRef), normalizeText(row && row.hostAddress)])
    .filter(Boolean);
  const topologyHosts = safeArray(organization && organization.hostTargets).flatMap(target => {
    const hostRef = normalizeText(target && target.hostRef);
    const hostAddress = normalizeText(target && target.hostAddress);
    if (hostRef && hostAddress) {
      return [`${hostRef} (${hostAddress})`, hostRef, hostAddress];
    }
    return [hostRef, hostAddress];
  });
  const workspaceHosts = workspace && workspace.workspace && workspace.workspace.blocks
    ? Object.values(workspace.workspace.blocks)
        .flatMap(block => safeArray(block && block.hostIds))
        .map(value => normalizeText(value))
    : [];

  return uniq([...rowHosts, ...topologyHosts, ...workspaceHosts]).sort((left, right) =>
    left.localeCompare(right)
  );
};

const resolveFreshnessStatus = workspace => {
  const status = normalizeText(workspace && workspace.dataFreshness && workspace.dataFreshness.status).toLowerCase();
  if (status === 'fresh' || status === 'stale' || status === 'unknown') {
    return status;
  }
  return 'unknown';
};

const resolveFreshnessLabel = workspace => {
  const freshnessStatus = resolveFreshnessStatus(workspace);
  const lagSeconds = Number(workspace && workspace.dataFreshness && workspace.dataFreshness.lag_seconds);

  if (freshnessStatus === 'fresh') {
    return pickCognusText(
      'telemetria oficial atualizada',
      'official telemetry updated'
    );
  }
  if (freshnessStatus === 'stale') {
    if (Number.isFinite(lagSeconds) && lagSeconds > 0) {
      return pickCognusText(
        `telemetria expirada (${lagSeconds}s)`,
        `telemetry expired (${lagSeconds}s)`
      );
    }
    return pickCognusText('telemetria expirada', 'telemetry expired');
  }
  return pickCognusText('telemetria oficial sem recencia', 'official telemetry without recency');
};

const resolveBaselineStatus = ({ gate, topologySummary }) => {
  if (!gate) {
    return 'unknown';
  }
  if (gate.baselineConverged && topologySummary.requiredConverged >= topologySummary.requiredTotal) {
    return 'converged';
  }
  if (topologySummary.requiredConverged > 0 || gate.baselineConverged) {
    return 'partial';
  }
  return 'blocked';
};

const resolveBaselineLabel = ({ gate, topologySummary }) => {
  const status = resolveBaselineStatus({ gate, topologySummary });
  if (status === 'converged') {
    return pickCognusText(
      `baseline convergente (${topologySummary.requiredConverged}/${topologySummary.requiredTotal})`,
      `converged baseline (${topologySummary.requiredConverged}/${topologySummary.requiredTotal})`
    );
  }
  if (status === 'partial') {
    return pickCognusText(
      `baseline parcial (${topologySummary.requiredConverged}/${topologySummary.requiredTotal})`,
      `partial baseline (${topologySummary.requiredConverged}/${topologySummary.requiredTotal})`
    );
  }
  if (topologySummary.requiredTotal > 0) {
    return pickCognusText(
      `baseline bloqueada (${topologySummary.requiredConverged}/${topologySummary.requiredTotal})`,
      `blocked baseline (${topologySummary.requiredConverged}/${topologySummary.requiredTotal})`
    );
  }
  return pickCognusText('baseline oficial indisponivel', 'official baseline unavailable');
};

const resolveActiveChaincodesCount = ({ workspace, organization }) => {
  const projectedChaincodes = safeArray(
    workspace && workspace.workspace && workspace.workspace.projections
      ? workspace.workspace.projections.chaincodes
      : []
  );

  if (projectedChaincodes.length > 0) {
    return projectedChaincodes.filter(item => {
      const status = normalizeText(item && item.status).toLowerCase();
      const health = normalizeText(item && item.health).toLowerCase();
      return status === 'running' || health === 'healthy' || status === 'degraded';
    }).length;
  }

  return safeArray(organization && organization.chaincodes).length;
};

const resolveChannelsCount = ({ workspace, organization }) => {
  const projectedChannels = safeArray(
    workspace && workspace.workspace && workspace.workspace.projections
      ? workspace.workspace.projections.channels
      : []
  );
  if (projectedChannels.length > 0) {
    return projectedChannels.length;
  }
  return safeArray(organization && organization.channels).length;
};

const resolveCriticalComponents = topologyRows => {
  const degradedStatuses = new Set(['degraded', 'stopped', 'missing', 'unknown']);
  const criticalRows = safeArray(topologyRows).filter(
    row => normalizeText(row && row.criticality).toLowerCase() === 'critical'
  );
  const degradedCriticalRows = criticalRows.filter(row =>
    degradedStatuses.has(normalizeText(row && row.status).toLowerCase())
  );

  return {
    criticalTotal: criticalRows.length,
    degradedCriticalCount: degradedCriticalRows.length,
  };
};

const resolvePendingAlertsCount = ({ gate, workspace, missingArtifactsCount, freshnessStatus, degradedCriticalCount }) => {
  const gateIssues = safeArray(gate && gate.issues).length;
  const workspaceIssues = safeArray(workspace && workspace.issues).length;
  const freshnessPenalty = freshnessStatus === 'fresh' ? 0 : 1;
  return gateIssues + workspaceIssues + missingArtifactsCount + freshnessPenalty + degradedCriticalCount;
};

const resolveOperationalStatus = ({ officialState, evidenceMeta, gate, gateRow, workspace, topologySummary, degradedCriticalCount, freshnessStatus }) => {
  if (officialState && officialState.loading) {
    return 'pending';
  }
  if (officialState && officialState.error) {
    return 'blocked';
  }
  if (!evidenceMeta || !evidenceMeta.hasOfficialRun) {
    return 'blocked';
  }

  const gateStatus = normalizeText((gateRow && gateRow.status) || (gate && gate.status)).toLowerCase();
  if (gateStatus === 'blocked') {
    return 'blocked';
  }
  if (gateStatus === 'pending') {
    return 'pending';
  }
  if (!gate || gate.correlationValid === false) {
    return 'blocked';
  }

  const workspaceHealth = normalizeText(workspace && workspace.health).toLowerCase();
  const hasMissingArtifacts = safeArray(gate && gate.missingArtifacts).length > 0;
  const hasConvergenceGap = topologySummary.requiredTotal > topologySummary.requiredConverged;

  if (
    gateStatus === 'partial' ||
    freshnessStatus !== 'fresh' ||
    workspaceHealth === 'degraded' ||
    hasMissingArtifacts ||
    hasConvergenceGap ||
    degradedCriticalCount > 0
  ) {
    return 'partial';
  }

  if (gateStatus === 'implemented') {
    return 'implemented';
  }

  return 'pending';
};

const resolveOperationalTone = operationalStatus => {
  if (operationalStatus === 'implemented') {
    return {
      color: 'green',
      label: pickCognusText('operacional', 'operational'),
      alertType: 'success',
    };
  }
  if (operationalStatus === 'partial') {
    return {
      color: 'orange',
      label: pickCognusText('em observacao', 'under observation'),
      alertType: 'warning',
    };
  }
  if (operationalStatus === 'blocked') {
    return {
      color: 'red',
      label: pickCognusText('bloqueado', 'blocked'),
      alertType: 'error',
    };
  }
  return {
    color: 'blue',
    label: pickCognusText('em preparacao', 'in preparation'),
    alertType: 'info',
  };
};

const resolveCriticality = ({ operationalStatus, pendingAlertsCount, degradedCriticalCount }) => {
  if (operationalStatus === 'blocked' || degradedCriticalCount > 0) {
    return 'critical';
  }
  if (operationalStatus === 'partial' || pendingAlertsCount > 0) {
    return 'warning';
  }
  return 'stable';
};

const resolveCriticalityLabel = value => {
  if (value === 'critical') {
    return pickCognusText('critica', 'critical');
  }
  if (value === 'warning') {
    return pickCognusText('observacao', 'warning');
  }
  return pickCognusText('estavel', 'stable');
};

const resolveStatusReason = ({ officialState, gate, gateRow, freshnessStatus, missingArtifactsCount, degradedCriticalCount }) => {
  if (officialState && officialState.loading) {
    return pickCognusText(
      'sincronizando contrato oficial do runbook',
      'synchronizing official runbook contract'
    );
  }
  if (officialState && officialState.error) {
    return officialState.error;
  }
  if (!gate) {
    return pickCognusText('run oficial indisponivel', 'official run unavailable');
  }
  if (gate.correlationValid === false) {
    return pickCognusText(
      'correlacao oficial invalida para a organizacao',
      'invalid official correlation for the organization'
    );
  }
  if (normalizeText((gateRow && gateRow.status) || gate.status).toLowerCase() === 'blocked') {
    return pickCognusText(
      'organizacao bloqueada pelo gate oficial A2A',
      'organization blocked by the official A2A gate'
    );
  }
  if (missingArtifactsCount > 0) {
    return pickCognusText(
      'artefatos oficiais inconsistentes para operacao auditavel',
      'official artifacts inconsistent for auditable operation'
    );
  }
  if (freshnessStatus !== 'fresh') {
    return pickCognusText('telemetria oficial desatualizada', 'official telemetry outdated');
  }
  if (degradedCriticalCount > 0) {
    return pickCognusText(
      'componentes criticos degradados na topologia oficial',
      'critical components degraded in the official topology'
    );
  }
  return pickCognusText(
    'organizacao auditavel com baseline operacional convergente',
    'auditable organization with converged operational baseline'
  );
};

export const buildOverviewOperationalCard = ({ organization, officialState }) => {
  const safeOrganization = organization || {};
  const officialRun = officialState && officialState.run ? officialState.run : null;
  const evidenceMeta = buildOfficialRunEvidenceMeta(officialRun);
  const topologyRows = buildOrganizationRuntimeTopologyRows(officialRun);
  const topologySummary = buildOrganizationRuntimeTopologySummary(topologyRows);
  const workspace = resolveOrganizationWorkspace(
    safeOrganization,
    buildOrganizationWorkspaceReadModels(officialRun)
  );
  const gate = officialState && officialState.gate ? officialState.gate : evidenceMeta.a2aEntryGate || null;
  const gateRow = resolveOrganizationGateRow(safeOrganization, gate);
  const freshnessStatus = resolveFreshnessStatus(workspace);
  const { degradedCriticalCount } = resolveCriticalComponents(topologyRows);
  const missingArtifactsCount = safeArray(gate && gate.missingArtifacts).length;
  const pendingAlertsCount = resolvePendingAlertsCount({
    gate,
    workspace,
    missingArtifactsCount,
    freshnessStatus,
    degradedCriticalCount,
  });
  const operationalStatus = resolveOperationalStatus({
    officialState,
    evidenceMeta,
    gate,
    gateRow,
    workspace,
    topologySummary,
    degradedCriticalCount,
    freshnessStatus,
  });
  const operationalTone = resolveOperationalTone(operationalStatus);
  const criticality = resolveCriticality({
    operationalStatus,
    pendingAlertsCount,
    degradedCriticalCount,
  });

  return {
    key: normalizeText(safeOrganization.key),
    organizationId: normalizeText(safeOrganization.orgId),
    organizationName: normalizeText(safeOrganization.orgName) || normalizeText(safeOrganization.orgId),
    domain: normalizeText(safeOrganization.domain),
    latestRunId: normalizeText(safeOrganization.latestRunId),
    latestChangeId: normalizeText(safeOrganization.latestChangeId),
    operationalStatus,
    operationalTone,
    criticality,
    criticalityLabel: resolveCriticalityLabel(criticality),
    statusReason: resolveStatusReason({
      officialState,
      gate,
      gateRow,
      freshnessStatus,
      missingArtifactsCount,
      degradedCriticalCount,
    }),
    freshnessStatus,
    freshnessLabel: resolveFreshnessLabel(workspace),
    freshnessLagSeconds: Number(
      workspace && workspace.dataFreshness && workspace.dataFreshness.lag_seconds
    ) || 0,
    baselineStatus: resolveBaselineStatus({ gate, topologySummary }),
    baselineLabel: resolveBaselineLabel({ gate, topologySummary }),
    workspaceHealth: normalizeText(workspace && workspace.health).toLowerCase() || 'unknown',
    requiredConverged: topologySummary.requiredConverged,
    requiredTotal: topologySummary.requiredTotal,
    criticalComponentsDegraded: degradedCriticalCount,
    channelsCount: resolveChannelsCount({ workspace, organization: safeOrganization }),
    activeChaincodesCount: resolveActiveChaincodesCount({ workspace, organization: safeOrganization }),
    pendingAlertsCount,
    hostLabels: resolveHostLabels({
      organization: safeOrganization,
      topologyRows,
      workspace,
    }),
    gateMode: normalizeText((gateRow && gateRow.mode) || (gate && gate.mode)),
    gateStatus: normalizeText((gateRow && gateRow.status) || (gate && gate.status)).toLowerCase(),
    officialWorkspaceObservedAt: normalizeText(workspace && workspace.observedAt),
    proposals: safeArray(safeOrganization.runHistory)
      .slice(0, 8)
      .map(entry => ({
        key: normalizeText(entry && entry.key),
        runId: normalizeText(entry && entry.runId),
        changeId: normalizeText(entry && entry.changeId),
        status: normalizeText(entry && entry.status).toLowerCase(),
        finishedAt: normalizeText(entry && entry.finishedAt),
        capturedAt: normalizeText(entry && entry.capturedAt),
      }))
      .filter(entry => entry.key || entry.runId),
  };
};

export const buildOverviewOperationalFilterOptions = cards => {
  const safeCards = safeArray(cards);
  return {
    organizations: safeCards
      .map(card => ({
        label: card.organizationName,
        value: normalizeToken(card.organizationName || card.organizationId),
      }))
      .filter(option => option.value)
      .sort((left, right) => left.label.localeCompare(right.label)),
    statuses: uniq(safeCards.map(card => normalizeText(card.operationalStatus))).sort(),
    criticalities: uniq(safeCards.map(card => normalizeText(card.criticality))).sort(),
    freshness: uniq(safeCards.map(card => normalizeText(card.freshnessStatus))).sort(),
    hosts: uniq(safeCards.flatMap(card => safeArray(card.hostLabels))).sort((left, right) =>
      left.localeCompare(right)
    ),
  };
};

export const filterOverviewOperationalCards = (cards, filters = {}) => {
  const safeCards = safeArray(cards);
  const effectiveFilters = {
    ...EMPTY_CARD_FILTERS,
    ...(filters || {}),
  };
  const organizationFilter = normalizeToken(effectiveFilters.organization);
  const statusFilter = normalizeText(effectiveFilters.status).toLowerCase();
  const criticalityFilter = normalizeText(effectiveFilters.criticality).toLowerCase();
  const hostFilter = normalizeText(effectiveFilters.host);
  const freshnessFilter = normalizeText(effectiveFilters.freshness).toLowerCase();

  return safeCards.filter(card => {
    const matchesOrganization =
      !organizationFilter ||
      normalizeToken(card.organizationName).includes(organizationFilter) ||
      normalizeToken(card.organizationId).includes(organizationFilter);
    if (!matchesOrganization) {
      return false;
    }

    if (statusFilter && statusFilter !== 'all' && normalizeText(card.operationalStatus) !== statusFilter) {
      return false;
    }

    if (
      criticalityFilter &&
      criticalityFilter !== 'all' &&
      normalizeText(card.criticality) !== criticalityFilter
    ) {
      return false;
    }

    if (hostFilter && hostFilter !== 'all' && !safeArray(card.hostLabels).includes(hostFilter)) {
      return false;
    }

    if (
      freshnessFilter &&
      freshnessFilter !== 'all' &&
      normalizeText(card.freshnessStatus) !== freshnessFilter
    ) {
      return false;
    }

    return true;
  });
};

export const OVERVIEW_EMPTY_CARD_FILTERS = EMPTY_CARD_FILTERS;
