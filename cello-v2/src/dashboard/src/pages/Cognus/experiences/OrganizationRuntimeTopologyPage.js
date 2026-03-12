import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Divider,
  Input,
  Select,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
  message,
} from 'antd';
import { history, Link, useLocation } from 'umi';
import { ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { OperationalWindowManagerProvider } from '../components/OperationalWindowManager';
import styles from '../components/NeoOpsLayout.less';
import OfficialRuntimeInspectionDrawer from './OfficialRuntimeInspectionDrawer';
import { screenByKey } from '../data/screens';
import {
  PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
  PROVISIONING_SCREEN_PATH_BY_KEY,
} from '../data/provisioningContract';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import ProvisioningReadinessCard from './ProvisioningReadinessCard';
import {
  getRunbookStatus,
  prewarmRunbookRuntimeInspectionCache,
} from '../../../services/runbook';
import {
  consumeOnboardingRunbookHandoff,
  resolveOnboardingRunbookHandoffFromTrail,
} from './provisioningOnboardingHandoffUtils';
import {
  ORG_RUNTIME_COMPONENT_TYPE_LABEL,
  ORG_RUNTIME_CRITICALITY_LABEL,
  ORG_RUNTIME_CRITICALITY_TONE_MAP,
  ORG_RUNTIME_SCOPE_LABEL,
  ORG_RUNTIME_SCOPE_TONE_MAP,
  ORG_RUNTIME_STATUS_LABEL,
  ORG_RUNTIME_STATUS_SOURCE_LABEL,
  ORG_RUNTIME_STATUS_TONE_MAP,
  buildOfficialRunEvidenceMeta,
  buildOrganizationRuntimeTopologyFilterOptions,
  buildOrganizationRuntimeTopologyRows,
  buildOrganizationWorkspaceReadModels,
  buildOrganizationRuntimeTopologySummary,
  filterOrganizationRuntimeTopologyRows,
  formatRuntimeTopologyUtcTimestamp,
} from './provisioningRuntimeTopologyUtils';
import useOfficialRuntimeInspection from './useOfficialRuntimeInspection';
import {
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from '../cognusI18n';

const locale = resolveCognusLocale();
const t = (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale);

const screen = screenByKey[PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY] || {
  title: t('Topologia runtime da organização', 'Organization runtime topology'),
  summary: t(
    'Visão consolidada oficial da topologia runtime A2.',
    'Official consolidated view of the A2 runtime topology.'
  ),
};

const statusTagColor = {
  default: 'default',
  processing: 'blue',
  warning: 'orange',
  error: 'red',
  success: 'green',
};

const A2A_GATE_STATUS_LABEL = {
  implemented: pickCognusText('operacional', 'operational', locale),
  partial: pickCognusText('em observação', 'under observation', locale),
  pending: pickCognusText('em preparação', 'in preparation', locale),
  blocked: pickCognusText('bloqueado', 'blocked', locale),
};

const A2A_GATE_STATUS_TONE = {
  implemented: 'success',
  partial: 'warning',
  pending: 'default',
  blocked: 'error',
};

const A2A_GATE_MODE_LABEL = {
  interactive: pickCognusText('interativo', 'interactive', locale),
  read_only_blocked: pickCognusText('somente auditoria', 'audit only', locale),
};

const RUNBOOK_AUDIT_HISTORY_STORAGE_KEY = 'cognus.provisioning.runbook.audit.history.v2';

const topologyReadinessActionOrder = [
  'load_official_topology',
  'filter_runtime_components',
  'open_inventory_screen',
  'open_runbook_screen',
];

const normalizeText = value => String(value || '').trim();

const getBrowserLocalStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.localStorage || null;
  } catch (error) {
    return null;
  }
};

const resolveRunIdFromAuditHistory = () => {
  const storage = getBrowserLocalStorage();
  if (!storage) {
    return '';
  }

  const rawHistoryPayload = storage.getItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
  if (!rawHistoryPayload) {
    return '';
  }

  try {
    const parsedHistory = JSON.parse(rawHistoryPayload);
    if (!Array.isArray(parsedHistory) || parsedHistory.length === 0) {
      return '';
    }

    const latestEntry = parsedHistory[0];
    const directRunId = normalizeText(latestEntry && latestEntry.runId);
    if (directRunId) {
      return directRunId;
    }

    const executionStateRunId = normalizeText(
      latestEntry && latestEntry.executionState && latestEntry.executionState.runId
    );
    return executionStateRunId;
  } catch (error) {
    return '';
  }
};

const resolveInitialRunId = queryRunId => {
  const queryValue = normalizeText(queryRunId);
  if (queryValue) {
    return queryValue;
  }

  const onboardingHandoff = consumeOnboardingRunbookHandoff();
  const handoffRunId = normalizeText(onboardingHandoff && onboardingHandoff.run_id);
  if (handoffRunId) {
    return handoffRunId;
  }

  const trailHandoff = resolveOnboardingRunbookHandoffFromTrail();
  const trailRunId = normalizeText(trailHandoff && trailHandoff.run_id);
  if (trailRunId) {
    return trailRunId;
  }

  return resolveRunIdFromAuditHistory();
};

const renderToneTag = (label, tone) => <Tag color={statusTagColor[tone] || 'default'}>{label}</Tag>;

const WORKSPACE_HEALTH_LABEL = {
  healthy: pickCognusText('saudável', 'healthy', locale),
  degraded: pickCognusText('degradado', 'degraded', locale),
  planned: pickCognusText('planejado', 'planned', locale),
  unknown: pickCognusText('desconhecido', 'unknown', locale),
};

const WORKSPACE_HEALTH_TONE = {
  healthy: 'success',
  degraded: 'error',
  planned: 'warning',
  unknown: 'default',
};

const WORKSPACE_OBSERVATION_LABEL = {
  observed: pickCognusText('observado', 'observed', locale),
  planned: pickCognusText('planejado', 'planned', locale),
  not_observed: pickCognusText('não observado', 'not observed', locale),
};

const OrganizationRuntimeTopologyPage = () => {
  const location = useLocation();
  const locationQuery = useMemo(() => new URLSearchParams((location && location.search) || ''), [
    location,
  ]);
  const queryRunId = normalizeText(locationQuery.get('run_id'));
  const initialRunId = useMemo(() => resolveInitialRunId(queryRunId), [queryRunId]);

  const [runIdInput, setRunIdInput] = useState(initialRunId);
  const [selectedRunId, setSelectedRunId] = useState(initialRunId);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [officialRunState, setOfficialRunState] = useState({
    loading: false,
    run: null,
    error: '',
  });
  const [filters, setFilters] = useState({
    host: 'all',
    componentType: 'all',
    status: 'all',
    criticality: 'all',
  });

  useEffect(() => {
    if (!queryRunId) {
      return;
    }
    setRunIdInput(queryRunId);
    setSelectedRunId(queryRunId);
  }, [queryRunId]);

  useEffect(() => {
    const normalizedRunId = normalizeText(selectedRunId);
    if (!normalizedRunId) {
      setOfficialRunState({
        loading: false,
        run: null,
        error: t(
          'run_id oficial obrigatório para consultar a topologia runtime.',
          'Official run_id is required to query the runtime topology.'
        ),
      });
      return undefined;
    }

    let cancelled = false;
    setOfficialRunState(currentState => ({
      ...currentState,
      loading: true,
      error: '',
    }));

    getRunbookStatus(normalizedRunId)
      .then(response => {
        if (cancelled) {
          return;
        }

        const resolvedRun = response && response.run ? response.run : null;
        if (!resolvedRun) {
          setOfficialRunState({
            loading: false,
            run: null,
            error: t(
              'Resposta oficial sem payload `run` no contrato /api/v1/runbooks/{run_id}/status.',
              'Official response without `run` payload in the /api/v1/runbooks/{run_id}/status contract.'
            ),
          });
          return;
        }

        prewarmRunbookRuntimeInspectionCache(
          normalizedRunId,
          buildOrganizationRuntimeTopologyRows(resolvedRun)
        );

        setOfficialRunState({
          loading: false,
          run: resolvedRun,
          error: '',
        });
      })
      .catch(error => {
        if (cancelled) {
          return;
        }

        setOfficialRunState({
          loading: false,
          run: null,
          error:
            (error && error.message) ||
            t(
              'Falha ao consultar o status oficial do runbook para a topologia runtime.',
              'Failed to query the official runbook status for the runtime topology.'
            ),
        });
      });

    return () => {
      cancelled = true;
    };
  }, [selectedRunId, refreshNonce]);

  const officialEvidenceMeta = useMemo(() => buildOfficialRunEvidenceMeta(officialRunState.run), [
    officialRunState.run,
  ]);
  const organizationWorkspaces = useMemo(
    () => buildOrganizationWorkspaceReadModels(officialRunState.run),
    [officialRunState.run]
  );
  const topologyRows = useMemo(() => buildOrganizationRuntimeTopologyRows(officialRunState.run), [
    officialRunState.run,
  ]);
  const filterOptions = useMemo(() => buildOrganizationRuntimeTopologyFilterOptions(topologyRows), [
    topologyRows,
  ]);
  const fullSummary = useMemo(() => buildOrganizationRuntimeTopologySummary(topologyRows), [
    topologyRows,
  ]);
  const filteredRows = useMemo(() => filterOrganizationRuntimeTopologyRows(topologyRows, filters), [
    topologyRows,
    filters,
  ]);
  const filteredSummary = useMemo(() => buildOrganizationRuntimeTopologySummary(filteredRows), [
    filteredRows,
  ]);
  const a2aEntryGate = officialEvidenceMeta.a2aEntryGate || {};
  const a2aOrganizationSummary = useMemo(() => {
    const rows = Array.isArray(a2aEntryGate.organizationReadiness)
      ? a2aEntryGate.organizationReadiness
      : [];

    return rows.reduce(
      (accumulator, row) => {
        const statusKey = row && row.status ? row.status : 'pending';
        accumulator.total += 1;
        accumulator[statusKey] = (accumulator[statusKey] || 0) + 1;
        return accumulator;
      },
      {
        total: 0,
        implemented: 0,
        partial: 0,
        pending: 0,
        blocked: 0,
      }
    );
  }, [a2aEntryGate.organizationReadiness]);

  useEffect(() => {
    setFilters(currentFilters => ({
      host:
        currentFilters.host === 'all' || filterOptions.hosts.includes(currentFilters.host)
          ? currentFilters.host
          : 'all',
      componentType:
        currentFilters.componentType === 'all' ||
        filterOptions.componentTypes.includes(currentFilters.componentType)
          ? currentFilters.componentType
          : 'all',
      status:
        currentFilters.status === 'all' || filterOptions.statuses.includes(currentFilters.status)
          ? currentFilters.status
          : 'all',
      criticality:
        currentFilters.criticality === 'all' ||
        filterOptions.criticalities.includes(currentFilters.criticality)
          ? currentFilters.criticality
          : 'all',
    }));
  }, [filterOptions]);

  const runbookPath = PROVISIONING_SCREEN_PATH_BY_KEY['e1-provisionamento'];
  const inventoryPath = PROVISIONING_SCREEN_PATH_BY_KEY['e1-inventario'];
  const channelWorkspacePath =
    PROVISIONING_SCREEN_PATH_BY_KEY[PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY];
  const runQuerySuffix = selectedRunId ? `?run_id=${encodeURIComponent(selectedRunId)}` : '';
  const {
    inspectionState,
    inspectionScopeEntries,
    openInspection,
    openInspectionByDetailRef,
    refreshInspection,
    closeInspection,
  } = useOfficialRuntimeInspection({
    runId: selectedRunId,
    topologyRows,
  });

  const handleLoadRun = () => {
    const normalizedRunId = normalizeText(runIdInput);
    if (!normalizedRunId) {
      message.error(
        t(
          'Informe o run_id oficial antes de carregar a topologia runtime.',
          'Provide the official run_id before loading the runtime topology.'
        )
      );
      return;
    }

    if (normalizedRunId === selectedRunId) {
      setRefreshNonce(currentValue => currentValue + 1);
      return;
    }

    setSelectedRunId(normalizedRunId);
  };

  const handleRefresh = () => {
    if (!selectedRunId) {
      message.warning(
        t('Não há run_id selecionado para atualizar.', 'There is no selected run_id to refresh.')
      );
      return;
    }

    setRefreshNonce(currentValue => currentValue + 1);
  };

  const handleResetFilters = () => {
    setFilters({
      host: 'all',
      componentType: 'all',
      status: 'all',
      criticality: 'all',
    });
  };

  const handleWorkspaceDetailRef = detailRef => {
    openInspectionByDetailRef(detailRef);
  };

  const handleOpenInspection = row => {
    openInspection(row);
  };

  const handleRefreshInspection = () => {
    if (!inspectionState.row) {
      return;
    }
    refreshInspection();
  };

  const handleCloseInspection = () => {
    closeInspection();
  };

  const handleOpenChannelWorkspace = (channelId, organizationId) => {
    const normalizedChannelId = normalizeText(channelId);
    if (!selectedRunId || !normalizedChannelId) {
      return;
    }

    const query = new URLSearchParams({
      run_id: selectedRunId,
      channel_id: normalizedChannelId,
    });
    if (organizationId) {
      query.set('org_id', organizationId);
    }
    history.push(`${channelWorkspacePath}?${query.toString()}`);
  };

  const columns = [
    {
      title: t('Organização', 'Organization'),
      dataIndex: 'organizationName',
      key: 'organizationName',
      width: 220,
      render: (value, row) => (
        <Space direction="vertical" size={0}>
          <Typography.Text strong>{value || '-'}</Typography.Text>
          <Typography.Text type="secondary" code>
            {row.organizationId || '-'}
          </Typography.Text>
        </Space>
      ),
    },
    {
      title: t('Componente', 'Component'),
      dataIndex: 'componentName',
      key: 'componentName',
      width: 280,
      render: (value, row) => (
        <Space direction="vertical" size={2}>
          <Space size={8}>
            <Typography.Text code>{value || '-'}</Typography.Text>
            {renderToneTag(
              ORG_RUNTIME_COMPONENT_TYPE_LABEL[row.componentType] || row.componentType,
              'processing'
            )}
          </Space>
          {row.nodeId && row.nodeId !== value && (
            <Typography.Text type="secondary" code>
              {row.nodeId}
            </Typography.Text>
          )}
          {row.componentId && row.componentId !== row.nodeId && (
            <Typography.Text type="secondary" code>
              {row.componentId}
            </Typography.Text>
          )}
        </Space>
      ),
    },
    {
      title: t('Host', 'Host'),
      dataIndex: 'hostRef',
      key: 'hostRef',
      width: 170,
      render: value => (value ? <Typography.Text code>{value}</Typography.Text> : '-'),
    },
    {
      title: t('Status oficial', 'Official status'),
      dataIndex: 'status',
      key: 'status',
      width: 230,
      render: (value, row) => (
        <Space direction="vertical" size={2}>
          {renderToneTag(
            ORG_RUNTIME_STATUS_LABEL[value] || value || '-',
            ORG_RUNTIME_STATUS_TONE_MAP[value]
          )}
          <Typography.Text type="secondary">
            {ORG_RUNTIME_STATUS_SOURCE_LABEL[row.statusSource] || row.statusSource || '-'}
          </Typography.Text>
        </Space>
      ),
    },
    {
      title: t('Escopo', 'Scope'),
      dataIndex: 'scope',
      key: 'scope',
      width: 140,
      render: value =>
        renderToneTag(
          ORG_RUNTIME_SCOPE_LABEL[value] || value || '-',
          ORG_RUNTIME_SCOPE_TONE_MAP[value]
        ),
    },
    {
      title: t('Criticidade', 'Criticality'),
      dataIndex: 'criticality',
      key: 'criticality',
      width: 140,
      render: value =>
        renderToneTag(
          ORG_RUNTIME_CRITICALITY_LABEL[value] || value || '-',
          ORG_RUNTIME_CRITICALITY_TONE_MAP[value]
        ),
    },
    {
      title: t('Correlação', 'Correlation'),
      key: 'correlation',
      width: 280,
      render: (_, row) => {
        const details = [
          row.channelId ? `channel=${row.channelId}` : '',
          row.chaincodeId ? `chaincode=${row.chaincodeId}` : '',
          row.apiId ? `api=${row.apiId}` : '',
          row.routePath ? `route=${row.routePath}` : '',
        ].filter(Boolean);
        return (
          <Space direction="vertical" size={4}>
            <Typography.Text type="secondary">
              {details.length > 0 ? details.join(' | ') : '-'}
            </Typography.Text>
            {row.channelId && (
              <Button
                size="small"
                type="link"
                onClick={() => handleOpenChannelWorkspace(row.channelId, row.organizationId)}
                style={{ padding: 0 }}
              >
                {t('Abrir canal', 'Open channel')}
              </Button>
            )}
          </Space>
        );
      },
    },
    {
      title: t('Evidência UTC', 'Evidence UTC'),
      dataIndex: 'evidenceTimestampUtc',
      key: 'evidenceTimestampUtc',
      width: 180,
      render: value => formatRuntimeTopologyUtcTimestamp(value),
    },
    {
      title: t('Inspeção runtime', 'Runtime inspection'),
      key: 'inspection',
      width: 180,
      fixed: 'right',
      render: (_, row) => (
        <Button size="small" onClick={() => handleOpenInspection(row)} disabled={!row.componentId}>
          {t('Drill-down oficial', 'Official drill-down')}
        </Button>
      ),
    },
  ];

  const pipelineDecisionTone = officialEvidenceMeta.decision === 'allow' ? 'success' : 'error';
  const a2aGateTone = A2A_GATE_STATUS_TONE[a2aEntryGate.status] || 'default';
  let a2aGateAlertType = 'warning';
  if (a2aGateTone === 'success') {
    a2aGateAlertType = 'success';
  } else if (a2aGateTone === 'error') {
    a2aGateAlertType = 'error';
  }

  const renderWorkspaceProjectionRows = (rows, renderRow) => {
    const safeRows = Array.isArray(rows) ? rows : [];
    if (safeRows.length === 0) {
      return (
        <Typography.Text type="secondary">
          {t(
            'Sem evidências oficiais correlacionadas.',
            'No correlated official evidence available.'
          )}
        </Typography.Text>
      );
    }
    return (
      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        {safeRows.slice(0, 4).map(renderRow)}
        {safeRows.length > 4 && (
          <Typography.Text type="secondary">
            {t('+{count} registros oficiais', '+{count} official records', {
              count: safeRows.length - 4,
            })}
          </Typography.Text>
        )}
      </Space>
    );
  };

  const resolveWorkspaceProjectionTone = status => {
    if (status === 'running') {
      return {
        label: WORKSPACE_HEALTH_LABEL.healthy,
        tone: 'success',
      };
    }
    if (status === 'degraded') {
      return {
        label: WORKSPACE_HEALTH_LABEL.degraded,
        tone: 'error',
      };
    }
    return {
      label: WORKSPACE_HEALTH_LABEL.unknown,
      tone: 'default',
    };
  };

  const renderWorkspaceBlock = block => {
    const safeBlock = block || {};
    return (
      <div key={safeBlock.blockId} className={styles.selectCard}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
          <div>
            <Typography.Text className={styles.neoLabel}>{safeBlock.title}</Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle} style={{ marginBottom: 0 }}>
              {safeBlock.itemTotal || 0}
            </Typography.Title>
          </div>
          <Space direction="vertical" size={4} style={{ alignItems: 'flex-end' }}>
            {renderToneTag(
              WORKSPACE_HEALTH_LABEL[safeBlock.health] || safeBlock.health || '-',
              WORKSPACE_HEALTH_TONE[safeBlock.health]
            )}
            <Tag>{WORKSPACE_OBSERVATION_LABEL[safeBlock.observationState] || safeBlock.observationState || '-'}</Tag>
          </Space>
        </div>

        <Space wrap style={{ marginTop: 12 }}>
          <Tag>{`running=${safeBlock.statusTotals?.running || 0}`}</Tag>
          <Tag>{`degraded=${safeBlock.statusTotals?.degraded || 0}`}</Tag>
          <Tag>{`unknown=${safeBlock.statusTotals?.unknown || 0}`}</Tag>
        </Space>

        {safeBlock.hostIds && safeBlock.hostIds.length > 0 && (
          <div className={styles.chipRow}>
            {safeBlock.hostIds.map(hostId => (
              <span key={hostId} className={styles.chip}>{`${t('host', 'host')} ${hostId}`}</span>
            ))}
          </div>
        )}

        <Space direction="vertical" size={6} style={{ width: '100%', marginTop: 12 }}>
          {(safeBlock.items || []).slice(0, 3).map(item => (
            <div key={item.id || item.name}>
              <Space wrap>
                <Typography.Text strong>{item.name || '-'}</Typography.Text>
                {item.status &&
                  renderToneTag(
                    ORG_RUNTIME_STATUS_LABEL[item.status] || item.status,
                    ORG_RUNTIME_STATUS_TONE_MAP[item.status]
                  )}
              </Space>
              {item.hostId && (
                <Typography.Text type="secondary">{`${t('host', 'host')} ${item.hostId}`}</Typography.Text>
              )}
            </div>
          ))}
          {(safeBlock.items || []).length > 3 && (
            <Typography.Text type="secondary">
              {t('+{count} itens no bloco', '+{count} items in the block', {
                count: safeBlock.items.length - 3,
              })}
            </Typography.Text>
          )}
        </Space>

        {(safeBlock.detailRefs || []).some(ref => ref.kind === 'component') && (
          <Space wrap style={{ marginTop: 12 }}>
            {(safeBlock.items || [])
              .filter(item => item.detailRef && item.detailRef.kind === 'component' && item.componentId)
              .slice(0, 3)
              .map(item => (
                <Button
                  key={item.componentId}
                  size="small"
                  onClick={() => handleWorkspaceDetailRef(item.detailRef)}
                >
                  {item.name || item.componentId}
                </Button>
              ))}
          </Space>
        )}

        {(safeBlock.sourceArtifacts || []).length > 0 && (
          <div className={styles.chipRow}>
            {safeBlock.sourceArtifacts.map(source => (
              <span key={`${safeBlock.blockId}-${source.artifact}`} className={styles.chip}>
                {`${source.artifact}:${
                  source.available ? t('ok', 'ok') : t('ausente', 'missing')
                }`}
              </span>
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <OperationalWindowManagerProvider>
      <NeoOpsLayout
        screenKey={PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY}
        sectionLabel={PROVISIONING_SECTION_LABEL}
        title={screen.title}
        subtitle={screen.summary}
        navItems={provisioningNavItems}
        activeNavKey={resolveProvisioningActiveNavKey(PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY)}
        breadcrumbs={getProvisioningBreadcrumbs(PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY)}
        toolbar={
          <Space>
            <Link to={`${runbookPath}${runQuerySuffix}`}>
              <Button>{t('Abrir runbook', 'Open runbook')}</Button>
            </Link>
            <Link to={`${inventoryPath}${runQuerySuffix}`}>
              <Button>{t('Abrir inventário', 'Open inventory')}</Button>
            </Link>
            <Button
              icon={<ReloadOutlined />}
              onClick={handleRefresh}
              loading={officialRunState.loading}
              disabled={!selectedRunId}
            >
              {t('Atualizar', 'Refresh')}
            </Button>
          </Space>
        }
      >
      <Alert
        showIcon
        type="info"
        message={t(
          'Fonte oficial obrigatória para status da topologia',
          'Official source required for topology status'
        )}
        description={t(
          'A tela usa exclusivamente evidências oficiais do endpoint /api/v1/runbooks/{run_id}/status. Sem run_id oficial, não há estado de convergência exibido.',
          'This screen uses only official evidence from the /api/v1/runbooks/{run_id}/status endpoint. Without an official run_id, no convergence status is displayed.'
        )}
      />

      {!officialRunState.error && officialEvidenceMeta.hasOfficialRun && (
        <Alert
          showIcon
          type={a2aGateAlertType}
          message={t('Gate de entrada A2A: {status}', 'A2A entry gate: {status}', {
            status: A2A_GATE_STATUS_LABEL[a2aEntryGate.status] || '-',
          })}
          description={
            a2aEntryGate.issues && a2aEntryGate.issues.length > 0
              ? `${A2A_GATE_MODE_LABEL[a2aEntryGate.mode] || A2A_GATE_MODE_LABEL.read_only_blocked} | ${a2aEntryGate.issues
                  .map(issue => issue.message || issue.code)
                  .filter(Boolean)
                  .slice(0, 2)
                  .join(' | ')}`
              : `${A2A_GATE_MODE_LABEL[a2aEntryGate.mode] || A2A_GATE_MODE_LABEL.read_only_blocked} | ${t(
                  'Correlação, artefatos oficiais e baseline auditados pelo backend.',
                  'Correlation, official artifacts, and baseline audited by the backend.'
                )}`
          }
        />
      )}

      <div className={styles.neoCard}>
        <Typography.Title level={4} className={styles.neoCardTitle}>
          {t('Consulta oficial por run_id', 'Official query by run_id')}
        </Typography.Title>
        <Space wrap style={{ width: '100%' }}>
          <Input
            style={{ width: 360 }}
            placeholder={t('run_id oficial', 'official run_id')}
            value={runIdInput}
            onChange={event => setRunIdInput(event.target.value)}
            onPressEnter={handleLoadRun}
          />
          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleLoadRun}
            loading={officialRunState.loading}
          >
            {t('Carregar topologia', 'Load topology')}
          </Button>
          {selectedRunId && (
            <Typography.Text code>
              {t('run_id ativo: {runId}', 'active run_id: {runId}', {
                runId: selectedRunId,
              })}
            </Typography.Text>
          )}
        </Space>
      </div>

      {officialRunState.error && (
        <Alert
          showIcon
          type="error"
          message={t('Consulta oficial indisponível', 'Official query unavailable')}
          description={officialRunState.error}
        />
      )}

      {!officialRunState.error && officialEvidenceMeta.hasOfficialRun && (
        <div className={styles.neoGrid3}>
          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Gate obrigatório do A2A', 'Mandatory A2A gate')}
            </Typography.Text>
            <div style={{ marginTop: 8 }}>
              {renderToneTag(
                A2A_GATE_STATUS_LABEL[a2aEntryGate.status] || a2aEntryGate.status || '-',
                a2aGateTone
              )}
            </div>
            <div style={{ marginTop: 8 }}>
              {renderToneTag(
                A2A_GATE_MODE_LABEL[a2aEntryGate.mode] || a2aEntryGate.mode || '-',
                a2aEntryGate.interactiveEnabled ? 'success' : 'warning'
              )}
            </div>
            <Typography.Text type="secondary">
              {t(
                'orgs operacionais={implemented} | bloqueadas={blocked}',
                'operational orgs={implemented} | blocked={blocked}',
                {
                  implemented: a2aOrganizationSummary.implemented,
                  blocked: a2aOrganizationSummary.blocked,
                }
              )}
            </Typography.Text>
          </div>

          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Status oficial do pipeline', 'Official pipeline status')}
            </Typography.Text>
            <div style={{ marginTop: 8 }}>
              {renderToneTag(officialEvidenceMeta.pipelineStatus || '-', 'processing')}
            </div>
            <div style={{ marginTop: 8 }}>
              {renderToneTag(officialEvidenceMeta.decision || '-', pipelineDecisionTone)}
            </div>
            <Typography.Text type="secondary">
              {officialEvidenceMeta.decisionCode || t('decision_code indisponível', 'decision_code unavailable')}
            </Typography.Text>
          </div>

          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Convergência de componentes obrigatórios', 'Required component convergence')}
            </Typography.Text>
            <Typography.Title level={3} className={styles.neoCardTitle}>
              {`${fullSummary.requiredConverged}/${fullSummary.requiredTotal}`}
            </Typography.Title>
            <Typography.Text>{`${fullSummary.requiredConvergencePercent}%`}</Typography.Text>
          </div>
        </div>
      )}

      {!officialRunState.error && officialEvidenceMeta.hasOfficialRun && (
        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Prontidão A2A por organização', 'A2A readiness by organization')}
          </Typography.Title>
          <Space wrap>
            {(a2aEntryGate.organizationReadiness || []).map(row => (
              <Tag
                key={row.organizationId || row.organizationName}
                color={statusTagColor[A2A_GATE_STATUS_TONE[row.status]] || 'default'}
              >
                {`${row.organizationName || row.organizationId || 'org'}: ${
                  A2A_GATE_STATUS_LABEL[row.status] || row.status || '-'
                }`}
              </Tag>
            ))}
            {(!a2aEntryGate.organizationReadiness ||
              a2aEntryGate.organizationReadiness.length === 0) && (
              <Typography.Text type="secondary">
                {t(
                  'Sem organizações correlacionadas no gate oficial.',
                  'No organizations correlated in the official gate.'
                )}
              </Typography.Text>
            )}
          </Space>
          <div style={{ marginTop: 12 }}>
            <Typography.Text type="secondary">
              {t(
                'Componentes filtrados={total} | falhas={errors} | pendentes={pending}',
                'Filtered components={total} | failures={errors} | pending={pending}',
                {
                  total: filteredSummary.totalComponents,
                  errors: filteredSummary.statusTotals.error,
                  pending: filteredSummary.statusTotals.pending,
                }
              )}
            </Typography.Text>
          </div>
        </div>
      )}

      {!officialRunState.error && officialEvidenceMeta.hasOfficialRun && organizationWorkspaces.length > 0 && (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          {organizationWorkspaces.map(workspace => {
            const workspaceBlocks = [
              workspace.workspace.blocks.ca,
              workspace.workspace.blocks.api,
              workspace.workspace.blocks.peers,
              workspace.workspace.blocks.orderers,
              workspace.workspace.blocks.businessGroup,
              workspace.workspace.blocks.channels,
            ];

            return (
              <div key={workspace.organization.orgId || workspace.organization.orgName} className={styles.neoCard}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
                  <div>
                    <Typography.Title level={4} className={styles.neoCardTitle}>
                      {workspace.organization.orgName ||
                        workspace.organization.orgId ||
                        t('Organização', 'Organization')}
                    </Typography.Title>
                    <Typography.Text type="secondary">
                      {[
                        workspace.organization.orgId,
                        workspace.organization.domain,
                        officialEvidenceMeta.runId && `run_id=${officialEvidenceMeta.runId}`,
                        officialEvidenceMeta.changeId && `change_id=${officialEvidenceMeta.changeId}`,
                      ]
                        .filter(Boolean)
                        .join(' | ')}
                    </Typography.Text>
                  </div>
                  <Space direction="vertical" size={4} style={{ alignItems: 'flex-end' }}>
                    {renderToneTag(
                      WORKSPACE_HEALTH_LABEL[workspace.health] || workspace.health || '-',
                      WORKSPACE_HEALTH_TONE[workspace.health]
                    )}
                    {workspace.readModelFingerprint && (
                      <Typography.Text code>{workspace.readModelFingerprint}</Typography.Text>
                    )}
                  </Space>
                </div>

                <div className={styles.neoGrid3} style={{ marginTop: 16 }}>
                  {workspaceBlocks.map(renderWorkspaceBlock)}
                </div>

                <Divider orientation="left">
                  {t('Projeções agregadas oficiais', 'Official aggregated projections')}
                </Divider>

                <div className={styles.neoGrid3}>
                  <div className={styles.selectCard}>
                    <Typography.Text className={styles.neoLabel}>
                      {t('Canais', 'Channels')}
                    </Typography.Text>
                    {renderWorkspaceProjectionRows(workspace.workspace.projections.channels, row => (
                      <div key={row.id || row.name}>
                        <Space wrap>
                          <Typography.Text strong>{row.name || '-'}</Typography.Text>
                          {renderToneTag(
                            WORKSPACE_HEALTH_LABEL[row.health] || row.health || '-',
                            WORKSPACE_HEALTH_TONE[row.health]
                          )}
                        </Space>
                        <Typography.Text type="secondary">
                          {t(
                            'membros={members} | peers={peers} | orderers={orderers} | chaincodes={chaincodes}',
                            'members={members} | peers={peers} | orderers={orderers} | chaincodes={chaincodes}',
                            {
                              members: row.memberOrgs.length,
                              peers: row.peerTotal,
                              orderers: row.ordererTotal,
                              chaincodes: row.chaincodeTotal,
                            }
                          )}
                        </Typography.Text>
                        {row.name && (
                          <Button
                            size="small"
                            type="link"
                            onClick={() =>
                              handleOpenChannelWorkspace(
                                row.name,
                                workspace.organization.orgId
                              )
                            }
                            style={{ padding: 0 }}
                          >
                            {t('Abrir workspace do canal', 'Open channel workspace')}
                          </Button>
                        )}
                      </div>
                    ))}
                  </div>

                  <div className={styles.selectCard}>
                    <Typography.Text className={styles.neoLabel}>
                      {t('Membros da organização', 'Organization members')}
                    </Typography.Text>
                    {renderWorkspaceProjectionRows(
                      workspace.workspace.projections.organizationMembers,
                      row => (
                        <div key={row.id || row.name}>
                          <Space wrap>
                            <Typography.Text strong>{row.memberName || row.name || '-'}</Typography.Text>
                            <Tag>{row.membershipRole || 'member_org'}</Tag>
                          </Space>
                          <Typography.Text type="secondary">
                            {t('canais={count}', 'channels={count}', {
                              count: row.channelTotal || row.channelRefs.length,
                            })}
                          </Typography.Text>
                        </div>
                      )
                    )}
                  </div>

                  <div className={styles.selectCard}>
                    <Typography.Text className={styles.neoLabel}>
                      {t('Chaincodes', 'Chaincodes')}
                    </Typography.Text>
                    {renderWorkspaceProjectionRows(workspace.workspace.projections.chaincodes, row => {
                      const projectionTone = resolveWorkspaceProjectionTone(row.status);
                      return (
                        <div key={row.id || row.name}>
                          <Space wrap>
                            <Typography.Text strong>{row.name || '-'}</Typography.Text>
                            {renderToneTag(projectionTone.label || row.status || '-', projectionTone.tone)}
                          </Space>
                          <Typography.Text type="secondary">
                            {t('canais={channels} | apis={apis}', 'channels={channels} | apis={apis}', {
                              channels: row.channelRefs.join(', ') || '-',
                              apis: row.apiTotal,
                            })}
                          </Typography.Text>
                        </div>
                      );
                    })}
                  </div>

                  <div className={styles.selectCard}>
                    <Typography.Text className={styles.neoLabel}>
                      {t('Peers', 'Peers')}
                    </Typography.Text>
                    {renderWorkspaceProjectionRows(workspace.workspace.projections.peers, row => (
                      <div key={row.id || row.name}>
                        <Space wrap>
                          <Typography.Text strong>{row.name || '-'}</Typography.Text>
                          {renderToneTag(
                            ORG_RUNTIME_STATUS_LABEL[row.status] || row.status || '-',
                            ORG_RUNTIME_STATUS_TONE_MAP[row.status]
                          )}
                        </Space>
                        <Typography.Text type="secondary">
                          {t('host={host} | canais={channels}', 'host={host} | channels={channels}', {
                            host: row.hostId || '-',
                            channels: row.channelRefs.join(', ') || '-',
                          })}
                        </Typography.Text>
                      </div>
                    ))}
                  </div>

                  <div className={styles.selectCard}>
                    <Typography.Text className={styles.neoLabel}>
                      {t('Orderers', 'Orderers')}
                    </Typography.Text>
                    {renderWorkspaceProjectionRows(workspace.workspace.projections.orderers, row => (
                      <div key={row.id || row.name}>
                        <Space wrap>
                          <Typography.Text strong>{row.name || '-'}</Typography.Text>
                          {renderToneTag(
                            ORG_RUNTIME_STATUS_LABEL[row.status] || row.status || '-',
                            ORG_RUNTIME_STATUS_TONE_MAP[row.status]
                          )}
                        </Space>
                        <Typography.Text type="secondary">
                          {t('host={host}', 'host={host}', { host: row.hostId || '-' })}
                        </Typography.Text>
                      </div>
                    ))}
                  </div>

                  <div className={styles.selectCard}>
                    <Typography.Text className={styles.neoLabel}>
                      {t('Origem do read model', 'Read model source')}
                    </Typography.Text>
                    <div className={styles.chipRow}>
                      {(workspace.artifactOrigins || []).map(source => (
                        <span key={`${workspace.organization.orgId}-${source.artifact}`} className={styles.chip}>
                          {`${source.artifact}:${
                            source.available ? t('ok', 'ok') : t('ausente', 'missing')
                          }`}
                        </span>
                      ))}
                    </div>
                    <Typography.Text type="secondary">
                      {t('observado_em={timestamp}', 'observed_at={timestamp}', {
                        timestamp: formatRuntimeTopologyUtcTimestamp(workspace.observedAt),
                      })}
                    </Typography.Text>
                  </div>
                </div>
              </div>
            );
          })}
        </Space>
      )}

      {!officialRunState.error && officialEvidenceMeta.hasOfficialRun && organizationWorkspaces.length === 0 && (
        <Alert
          showIcon
          type="warning"
          message={t(
            'Read model oficial da organização indisponível',
            'Official organization read model unavailable'
          )}
          description={t(
            'O backend não retornou `organization_read_model` para este run. A topologia técnica continua disponível abaixo, mas o workspace oficial ainda não foi materializado.',
            'The backend did not return `organization_read_model` for this run. The technical topology is still available below, but the official workspace has not been materialized yet.'
          )}
        />
      )}

      {!officialRunState.error &&
        officialEvidenceMeta.hasOfficialRun &&
        (!officialEvidenceMeta.verifyArtifactAvailable ||
          !officialEvidenceMeta.inventoryArtifactAvailable) && (
          <Alert
            showIcon
            type="warning"
            message={t('Evidências oficiais mínimas incompletas', 'Minimum official evidence incomplete')}
            description={`verify-report=${
              officialEvidenceMeta.verifyArtifactAvailable ? t('ok', 'ok') : t('ausente', 'missing')
            } | inventory-final=${
              officialEvidenceMeta.inventoryArtifactAvailable ? t('ok', 'ok') : t('ausente', 'missing')
            }.`}
          />
        )}

      {!officialRunState.error &&
        officialEvidenceMeta.hasOfficialRun &&
        officialEvidenceMeta.decision === 'block' && (
          <Alert
            showIcon
            type="warning"
            message={t('Decisão oficial bloqueia continuidade', 'Official decision blocks continuation')}
            description={
              officialEvidenceMeta.missingEvidenceKeys.length > 0
                ? t('Evidências faltantes: {keys}.', 'Missing evidence: {keys}.', {
                    keys: officialEvidenceMeta.missingEvidenceKeys.join(', '),
                  })
                : t(
                    'Consulte o decision_code e os diagnósticos da execução para liberar continuidade.',
                    'Check the decision_code and execution diagnostics to unblock continuation.'
                  )
            }
          />
        )}

      <div className={styles.neoCard}>
        <Typography.Title level={4} className={styles.neoCardTitle}>
          {t('Filtros operacionais', 'Operational filters')}
        </Typography.Title>
        <div className={styles.selectGrid}>
          <div className={styles.selectCard}>
            <Typography.Text className={styles.neoLabel}>{t('Host', 'Host')}</Typography.Text>
            <Select
              style={{ width: '100%', marginTop: 8 }}
              value={filters.host}
              onChange={value => setFilters(currentFilters => ({ ...currentFilters, host: value }))}
              options={[
                { value: 'all', label: t('todos os hosts', 'all hosts') },
                ...filterOptions.hosts.map(value => ({ value, label: value })),
              ]}
            />
          </div>
          <div className={styles.selectCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Tipo de componente', 'Component type')}
            </Typography.Text>
            <Select
              style={{ width: '100%', marginTop: 8 }}
              value={filters.componentType}
              onChange={value =>
                setFilters(currentFilters => ({ ...currentFilters, componentType: value }))
              }
              options={[
                { value: 'all', label: t('todos os tipos', 'all types') },
                ...filterOptions.componentTypes.map(value => ({
                  value,
                  label: ORG_RUNTIME_COMPONENT_TYPE_LABEL[value] || value,
                })),
              ]}
            />
          </div>
          <div className={styles.selectCard}>
            <Typography.Text className={styles.neoLabel}>{t('Status', 'Status')}</Typography.Text>
            <Select
              style={{ width: '100%', marginTop: 8 }}
              value={filters.status}
              onChange={value =>
                setFilters(currentFilters => ({ ...currentFilters, status: value }))
              }
              options={[
                { value: 'all', label: t('todos os status', 'all statuses') },
                ...filterOptions.statuses.map(value => ({
                  value,
                  label: ORG_RUNTIME_STATUS_LABEL[value] || value,
                })),
              ]}
            />
          </div>
          <div className={styles.selectCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Criticidade', 'Criticality')}
            </Typography.Text>
            <Select
              style={{ width: '100%', marginTop: 8 }}
              value={filters.criticality}
              onChange={value =>
                setFilters(currentFilters => ({ ...currentFilters, criticality: value }))
              }
              options={[
                { value: 'all', label: t('todas as criticidades', 'all criticalities') },
                ...filterOptions.criticalities.map(value => ({
                  value,
                  label: ORG_RUNTIME_CRITICALITY_LABEL[value] || value,
                })),
              ]}
            />
          </div>
        </div>
        <div className={styles.footerActions}>
          <Button onClick={handleResetFilters}>{t('Limpar filtros', 'Clear filters')}</Button>
        </div>
      </div>

      <div className={styles.neoCard}>
        <Typography.Title level={4} className={styles.neoCardTitle}>
          {t(
            'Topologia runtime consolidada por organização',
            'Runtime topology consolidated by organization'
          )}
        </Typography.Title>
        <Spin spinning={officialRunState.loading}>
          <Table
            rowKey="key"
            columns={columns}
            dataSource={filteredRows}
            pagination={{ pageSize: 12, showSizeChanger: true }}
            size="small"
            scroll={{ x: 1880 }}
            locale={{
              emptyText: selectedRunId
                ? t(
                    'Sem componentes para o run_id informado no inventário/evidência oficial.',
                    'No components found for the run_id informed in the official inventory/evidence.'
                  )
                : t(
                    'Informe run_id oficial para visualizar a topologia runtime.',
                    'Provide an official run_id to view the runtime topology.'
                  ),
            }}
          />
        </Spin>
      </div>

      <OfficialRuntimeInspectionDrawer
        inspectionState={inspectionState}
        inspectionScopeEntries={inspectionScopeEntries}
        onClose={handleCloseInspection}
        onRefresh={handleRefreshInspection}
      />

      <ProvisioningReadinessCard
        screenKey={PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY}
        actionOrder={topologyReadinessActionOrder}
      />
      </NeoOpsLayout>
    </OperationalWindowManagerProvider>
  );
};

export default OrganizationRuntimeTopologyPage;
