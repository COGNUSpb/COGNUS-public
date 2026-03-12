import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Button, Empty, List, Space, Spin, Tag, Typography, message } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { history, useLocation } from 'umi';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { OperationalWindowManagerProvider } from '../components/OperationalWindowManager';
import styles from '../components/NeoOpsLayout.less';
import { formatCognusTemplate, resolveCognusLocale } from '../cognusI18n';
import OfficialRuntimeInspectionDrawer from './OfficialRuntimeInspectionDrawer';
import ProvisioningReadinessCard from './ProvisioningReadinessCard';
import {
  buildOfficialRunEvidenceMeta,
  buildOrganizationRuntimeTopologyRows,
  buildOrganizationWorkspaceReadModels,
  ORG_RUNTIME_COMPONENT_TYPE_LABEL,
  ORG_RUNTIME_STATUS_LABEL,
  ORG_RUNTIME_STATUS_TONE_MAP,
} from './provisioningRuntimeTopologyUtils';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import {
  PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
  PROVISIONING_SCREEN_PATH_BY_KEY,
} from '../data/provisioningContract';
import { screenByKey } from '../data/screens';
import {
  getRunbookStatus,
  prewarmRunbookRuntimeInspectionCache,
} from '../../../services/runbook';
import useOfficialRuntimeInspection from './useOfficialRuntimeInspection';

const normalizeText = value => String(value || '').trim();

const statusTagColor = {
  default: 'default',
  processing: 'blue',
  warning: 'orange',
  error: 'red',
  success: 'green',
};

const channelWorkspaceReadinessActionOrder = [
  'load_official_channel_workspace',
  'inspect_channel_component',
  'reopen_runtime_topology',
];

const renderToneTag = (label, tone) => <Tag color={statusTagColor[tone] || 'default'}>{label}</Tag>;

const resolveStatusTone = status => ORG_RUNTIME_STATUS_TONE_MAP[status] || 'default';

const buildChannelOrganizationEntries = ({
  channelId,
  organizationId,
  topologyRows,
  organizationWorkspaces,
}) => {
  const normalizedChannelId = normalizeText(channelId);
  const normalizedOrganizationId = normalizeText(organizationId);
  const safeRows = Array.isArray(topologyRows) ? topologyRows : [];
  const safeWorkspaces = Array.isArray(organizationWorkspaces) ? organizationWorkspaces : [];
  const rowsByOrganization = new Map();

  safeRows.forEach(row => {
    if (!row || normalizeText(row.channelId) !== normalizedChannelId) {
      return;
    }
    if (normalizedOrganizationId && normalizeText(row.organizationId) !== normalizedOrganizationId) {
      return;
    }

    const orgKey = normalizeText(row.organizationId) || normalizeText(row.organizationName);
    if (!rowsByOrganization.has(orgKey)) {
      rowsByOrganization.set(orgKey, []);
    }
    rowsByOrganization.get(orgKey).push(row);
  });

  safeWorkspaces.forEach(workspace => {
    if (!workspace || !workspace.organization) {
      return;
    }

    const orgKey = normalizeText(workspace.organization.orgId) || normalizeText(workspace.organization.orgName);
    if (normalizedOrganizationId && orgKey !== normalizedOrganizationId) {
      return;
    }

    const workspaceChannelRows =
      workspace.workspace && workspace.workspace.projections
        ? workspace.workspace.projections.channels || []
        : [];
    const matchesChannel = workspaceChannelRows.some(
      row => normalizeText(row && (row.name || row.id)) === normalizedChannelId
    );
    if (matchesChannel && !rowsByOrganization.has(orgKey)) {
      rowsByOrganization.set(orgKey, []);
    }
  });

  return Array.from(rowsByOrganization.entries())
    .map(([orgKey, rows]) => {
      const workspace =
        safeWorkspaces.find(
          candidate =>
            candidate &&
            candidate.organization &&
            (normalizeText(candidate.organization.orgId) ||
              normalizeText(candidate.organization.orgName)) === orgKey
        ) || null;
      const channelProjection =
        workspace && workspace.workspace && workspace.workspace.projections
          ? (workspace.workspace.projections.channels || []).find(
              row => normalizeText(row && (row.name || row.id)) === normalizedChannelId
            ) || null
          : null;
      return {
        organizationId: orgKey,
        organizationName:
          (workspace && workspace.organization && workspace.organization.orgName) ||
          (rows[0] && rows[0].organizationName) ||
          orgKey,
        workspace,
        channelProjection,
        rows: rows.sort((left, right) =>
          `${left.componentType}:${left.componentName}`.localeCompare(
            `${right.componentType}:${right.componentName}`
          )
        ),
      };
    })
    .sort((left, right) => left.organizationName.localeCompare(right.organizationName));
};

const ChannelWorkspacePage = () => {
  const location = useLocation();
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale),
    [locale]
  );
  const screen = screenByKey[PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY] || {
    title: t('Workspace operacional do canal', 'Operational channel workspace'),
    summary: t(
      'Visão oficial do canal com drill-down técnico por componente.',
      'Official channel view with technical drill-down by component.'
    ),
  };
  const locationQuery = useMemo(() => new URLSearchParams((location && location.search) || ''), [
    location,
  ]);
  const runId = normalizeText(locationQuery.get('run_id'));
  const channelId = normalizeText(locationQuery.get('channel_id'));
  const organizationId = normalizeText(locationQuery.get('org_id'));
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [officialRunState, setOfficialRunState] = useState({
    loading: false,
    run: null,
    error: '',
  });

  useEffect(() => {
    if (!runId) {
      setOfficialRunState({
        loading: false,
        run: null,
        error: t(
          'run_id oficial obrigatório para abrir o workspace operacional do canal.',
          'Official run_id is required to open the operational channel workspace.'
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

    getRunbookStatus(runId)
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
              'Resposta oficial sem payload run no contrato /api/v1/runbooks/{run_id}/status.',
              'Official response without run payload in the /api/v1/runbooks/{run_id}/status contract.'
            ),
          });
          return;
        }

        prewarmRunbookRuntimeInspectionCache(
          runId,
          buildOrganizationRuntimeTopologyRows(resolvedRun)
        ).catch(() => {});

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
              'Falha ao consultar o status oficial do runbook para o workspace do canal.',
              'Failed to query the official runbook status for the channel workspace.'
            ),
        });
      });

    return () => {
      cancelled = true;
    };
  }, [runId, refreshNonce, t]);

  const officialEvidenceMeta = useMemo(() => buildOfficialRunEvidenceMeta(officialRunState.run), [
    officialRunState.run,
  ]);
  const topologyRows = useMemo(() => buildOrganizationRuntimeTopologyRows(officialRunState.run), [
    officialRunState.run,
  ]);
  const organizationWorkspaces = useMemo(
    () => buildOrganizationWorkspaceReadModels(officialRunState.run),
    [officialRunState.run]
  );
  const channelOrganizations = useMemo(
    () =>
      buildChannelOrganizationEntries({
        channelId,
        organizationId,
        topologyRows,
        organizationWorkspaces,
      }),
    [channelId, organizationId, topologyRows, organizationWorkspaces]
  );
  const channelRows = useMemo(
    () => channelOrganizations.flatMap(entry => entry.rows || []),
    [channelOrganizations]
  );
  const channelChaincodes = useMemo(
    () =>
      Array.from(
        new Set(
          channelRows
            .map(row => normalizeText(row.chaincodeId))
            .concat(
              channelOrganizations.flatMap(entry =>
                entry.channelProjection ? entry.channelProjection.chaincodeRefs || [] : []
              )
            )
            .filter(Boolean)
        )
      ).sort((left, right) => left.localeCompare(right)),
    [channelOrganizations, channelRows]
  );
  const memberOrganizations = useMemo(
    () => channelOrganizations.map(entry => entry.organizationName || entry.organizationId),
    [channelOrganizations]
  );
  const {
    inspectionState,
    inspectionScopeEntries,
    openInspection,
    refreshInspection,
    closeInspection,
  } = useOfficialRuntimeInspection({
    runId,
    topologyRows,
  });

  const handleRefresh = () => {
    if (!runId) {
      message.warning(
        t(
          'run_id oficial obrigatório para atualizar o workspace do canal.',
          'Official run_id is required to refresh the channel workspace.'
        )
      );
      return;
    }
    setRefreshNonce(currentValue => currentValue + 1);
  };

  const handleOpenRuntimeTopology = () => {
    if (!runId) {
      return;
    }
    history.push(
      `${PROVISIONING_SCREEN_PATH_BY_KEY[PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY]}?run_id=${encodeURIComponent(
        runId
      )}`
    );
  };

  return (
    <OperationalWindowManagerProvider>
      <NeoOpsLayout
        screenKey={PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY}
        sectionLabel={PROVISIONING_SECTION_LABEL}
        title={channelId || screen.title}
        subtitle={screen.summary}
        navItems={provisioningNavItems}
        activeNavKey={resolveProvisioningActiveNavKey(PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY)}
        breadcrumbs={getProvisioningBreadcrumbs(PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY).concat(
          channelId ? [channelId] : []
        )}
        toolbar={
          <Space>
            <Button onClick={handleOpenRuntimeTopology}>
              {t('Abrir topologia runtime', 'Open runtime topology')}
            </Button>
            <Button icon={<ReloadOutlined />} onClick={handleRefresh} loading={officialRunState.loading}>
              {t('Atualizar', 'Refresh')}
            </Button>
          </Space>
        }
      >
      <Alert
        showIcon
        type="info"
        message={t(
          'Fonte oficial obrigatória para o workspace do canal',
          'Official source required for the channel workspace'
        )}
        description={t(
          'A tela usa exclusivamente o status oficial do runbook para resolver organizações membros, componentes correlacionados ao channel_id e o drill-down técnico por componente.',
          'This screen uses the official runbook status exclusively to resolve member organizations, components correlated to channel_id, and technical drill-down per component.'
        )}
      />

      {officialRunState.error && (
        <Alert
          showIcon
          type="error"
          message={t('Workspace oficial indisponível', 'Official workspace unavailable')}
          description={officialRunState.error}
        />
      )}

      {!officialRunState.error && (
        <div className={styles.neoGrid3}>
          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Correlação oficial', 'Official correlation')}
            </Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {channelId || '-'}
            </Typography.Title>
            <Typography.Text type="secondary">{`run_id=${runId || '-'} | change_id=${officialEvidenceMeta.changeId || '-'}`}</Typography.Text>
          </div>

          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Organizações membros', 'Member organizations')}
            </Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {memberOrganizations.length}
            </Typography.Title>
            <div className={styles.chipRow}>
              {memberOrganizations.map(name => (
                <span key={name} className={styles.chip}>
                  {name}
                </span>
              ))}
            </div>
          </div>

          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Chaincodes correlacionados', 'Correlated chaincodes')}
            </Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {channelChaincodes.length}
            </Typography.Title>
            <div className={styles.chipRow}>
              {channelChaincodes.map(chaincodeId => (
                <span key={chaincodeId} className={styles.chip}>
                  {chaincodeId}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}

      <div className={styles.neoCard}>
        <Typography.Title level={4} className={styles.neoCardTitle}>
          {t('Organizações e componentes do canal', 'Channel organizations and components')}
        </Typography.Title>
        <Spin spinning={officialRunState.loading}>
          {channelOrganizations.length === 0 ? (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={
                channelId
                  ? t(
                      'Nenhum componente oficial foi correlacionado a este channel_id.',
                      'No official component was correlated to this channel_id.'
                    )
                  : t(
                      'Informe channel_id oficial para abrir o workspace do canal.',
                      'Provide an official channel_id to open the channel workspace.'
                    )
              }
            />
          ) : (
            <Space direction="vertical" size={16} style={{ width: '100%' }}>
              {channelOrganizations.map(entry => (
                <div key={entry.organizationId} className={styles.selectCard}>
                  <Space direction="vertical" size={8} style={{ width: '100%' }}>
                    <div>
                      <Typography.Text className={styles.neoLabel}>
                        {t('Organização', 'Organization')}
                      </Typography.Text>
                      <Typography.Title level={4} className={styles.neoCardTitle}>
                        {entry.organizationName}
                      </Typography.Title>
                      <Typography.Text type="secondary">
                        {`org_id=${entry.organizationId || '-'} | observed_at=${
                          (entry.workspace && entry.workspace.observedAt) || '-'
                        }`}
                      </Typography.Text>
                    </div>

                    {entry.channelProjection && (
                      <div className={styles.chipRow}>
                        <span className={styles.chip}>{`members=${entry.channelProjection.memberOrgs.length}`}</span>
                        <span className={styles.chip}>{`peers=${entry.channelProjection.peerTotal}`}</span>
                        <span className={styles.chip}>{`orderers=${entry.channelProjection.ordererTotal}`}</span>
                        <span className={styles.chip}>{`chaincodes=${entry.channelProjection.chaincodeTotal}`}</span>
                      </div>
                    )}

                    {entry.rows.length === 0 ? (
                      <Typography.Text type="secondary">
                        {t(
                          'Read model oficial presente, mas sem componentes runtime correlacionados ao channel_id.',
                          'Official read model present, but with no runtime components correlated to channel_id.'
                        )}
                      </Typography.Text>
                    ) : (
                      <List
                        dataSource={entry.rows}
                        renderItem={row => (
                          <List.Item
                            actions={[
                              <Button
                                key={`${row.key}-inspect`}
                                type="link"
                                onClick={() => openInspection(row)}
                              >
                                {t('Drill-down oficial', 'Official drill-down')}
                              </Button>,
                            ]}
                          >
                            <List.Item.Meta
                              title={
                                <Space wrap>
                                  <Typography.Text strong>
                                    {row.componentName || row.componentId || '-'}
                                  </Typography.Text>
                                  {renderToneTag(
                                    ORG_RUNTIME_COMPONENT_TYPE_LABEL[row.componentType] ||
                                      row.componentType ||
                                      '-',
                                    'processing'
                                  )}
                                  {renderToneTag(
                                    ORG_RUNTIME_STATUS_LABEL[row.status] || row.status || '-',
                                    resolveStatusTone(row.status)
                                  )}
                                </Space>
                              }
                              description={`host=${row.hostRef || '-'} | component_id=${
                                row.componentId || '-'
                              } | chaincode=${row.chaincodeId || '-'} | route=${
                                row.routePath || '-'
                              }`}
                            />
                          </List.Item>
                        )}
                      />
                    )}
                  </Space>
                </div>
              ))}
            </Space>
          )}
        </Spin>
      </div>

      <OfficialRuntimeInspectionDrawer
        inspectionState={inspectionState}
        inspectionScopeEntries={inspectionScopeEntries}
        onClose={closeInspection}
        onRefresh={refreshInspection}
      />

      <ProvisioningReadinessCard
        screenKey={PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY}
        actionOrder={channelWorkspaceReadinessActionOrder}
      />
      </NeoOpsLayout>
    </OperationalWindowManagerProvider>
  );
};

export default ChannelWorkspacePage;
