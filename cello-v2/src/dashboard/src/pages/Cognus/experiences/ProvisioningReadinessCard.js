import React, { useMemo } from 'react';
import { Alert, Space, Table, Tag, Typography } from 'antd';
import styles from '../components/NeoOpsLayout.less';
import {
  PROVISIONING_EXECUTION_STATE,
  READINESS_STATUS,
  READINESS_STATUS_TONE_MAP,
  buildProvisioningReadinessActionRows,
  buildProvisioningReadinessSourceRows,
  getProvisioningScreenReadiness,
  hasProvisioningReadinessMockSources,
} from './provisioningBackendReadiness';
import { pickCognusText } from '../cognusI18n';

const statusTagColor = {
  default: 'default',
  processing: 'blue',
  warning: 'orange',
  error: 'red',
  success: 'green',
};

const executionStateToneMap = Object.freeze({
  [PROVISIONING_EXECUTION_STATE.official]: 'success',
  [PROVISIONING_EXECUTION_STATE.degraded]: 'warning',
});

const executionStateLabelMap = Object.freeze({
  [PROVISIONING_EXECUTION_STATE.official]: { ptBR: 'orquestrador', enUS: 'orchestrator' },
  [PROVISIONING_EXECUTION_STATE.degraded]: { ptBR: 'degradado', enUS: 'degraded' },
});

const readinessStatusLabelMap = Object.freeze({
  [READINESS_STATUS.implemented]: { ptBR: 'implementado', enUS: 'implemented' },
  [READINESS_STATUS.partial]: { ptBR: 'parcial', enUS: 'partial' },
  [READINESS_STATUS.pending]: { ptBR: 'pendente', enUS: 'pending' },
});

const renderToneTag = (label, tone) => <Tag color={statusTagColor[tone] || 'default'}>{label}</Tag>;

const ProvisioningReadinessCard = ({ screenKey, actionOrder }) => {
  const screenReadiness = useMemo(() => getProvisioningScreenReadiness(screenKey), [screenKey]);
  const actionRows = useMemo(() => buildProvisioningReadinessActionRows(screenKey, actionOrder), [
    screenKey,
    actionOrder,
  ]);
  const sourceRows = useMemo(() => buildProvisioningReadinessSourceRows(screenKey), [screenKey]);
  const hasMockSources = useMemo(() => hasProvisioningReadinessMockSources(screenKey), [screenKey]);
  const blockedActionsCount = actionRows.filter(row => !row.available).length;

  const actionColumns = [
    {
      title: pickCognusText('Acao', 'Action'),
      dataIndex: 'actionLabel',
      key: 'actionLabel',
      width: 230,
    },
    {
      title: pickCognusText('Status backend', 'Backend status'),
      dataIndex: 'statusLabel',
      key: 'statusLabel',
      width: 140,
      render: value => renderToneTag(value, READINESS_STATUS_TONE_MAP[value]),
    },
    {
      title: pickCognusText('Disponibilidade', 'Availability'),
      dataIndex: 'availabilityLabel',
      key: 'availabilityLabel',
      width: 130,
      render: (value, row) => renderToneTag(value, row.availabilityTone),
    },
    {
      title: pickCognusText('Modo de execucao', 'Execution mode'),
      dataIndex: 'modeLabel',
      key: 'modeLabel',
      width: 240,
      render: (value, row) => renderToneTag(value, row.modeTone),
    },
    {
      title: pickCognusText('Endpoint do orquestrador', 'Orchestrator endpoint'),
      dataIndex: 'endpoint',
      key: 'endpoint',
      width: 230,
      render: value => (value ? <Typography.Text code>{value}</Typography.Text> : '-'),
    },
    {
      title: pickCognusText('Justificativa operacional', 'Operational justification'),
      dataIndex: 'reason',
      key: 'reason',
    },
  ];

  const sourceColumns = [
    {
      title: pickCognusText('Fonte de dados', 'Data source'),
      dataIndex: 'label',
      key: 'label',
      width: 210,
    },
    {
      title: pickCognusText('Modo', 'Mode'),
      dataIndex: 'modeLabel',
      key: 'modeLabel',
      width: 250,
      render: (value, row) => renderToneTag(value, row.modeTone),
    },
    {
      title: pickCognusText('Endpoint do orquestrador', 'Orchestrator endpoint'),
      dataIndex: 'endpoint',
      key: 'endpoint',
      width: 230,
      render: value => (value ? <Typography.Text code>{value}</Typography.Text> : '-'),
    },
    {
      title: pickCognusText('Observacao', 'Note'),
      dataIndex: 'note',
      key: 'note',
    },
  ];

  return (
    <div className={styles.neoCard}>
      <Typography.Title level={4} className={styles.neoCardTitle}>
        {pickCognusText('Readiness de backend por ação', 'Backend readiness by action')}
      </Typography.Title>

      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <Typography.Text className={styles.neoLabel}>
            {pickCognusText('estado de execução', 'execution state')}
          </Typography.Text>
          {renderToneTag(
            pickCognusText(
              (executionStateLabelMap[screenReadiness.executionState] || {}).ptBR ||
                screenReadiness.executionState,
              (executionStateLabelMap[screenReadiness.executionState] || {}).enUS ||
                screenReadiness.executionState
            ),
            executionStateToneMap[screenReadiness.executionState]
          )}
        </div>
        <Typography.Text>
          {pickCognusText('perfil de ambiente', 'environment profile')}: {screenReadiness.envProfile}{' '}
          · {screenReadiness.executionStateReason}
        </Typography.Text>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <Typography.Text className={styles.neoLabel}>
            {pickCognusText('status consolidado da tela', 'consolidated screen status')}
          </Typography.Text>
          {renderToneTag(
            pickCognusText(
              (readinessStatusLabelMap[screenReadiness.screenStatus] || {}).ptBR ||
                screenReadiness.screenStatus,
              (readinessStatusLabelMap[screenReadiness.screenStatus] || {}).enUS ||
                screenReadiness.screenStatus
            ),
            READINESS_STATUS_TONE_MAP[screenReadiness.screenStatus]
          )}
        </div>
        <Typography.Text>{screenReadiness.summary}</Typography.Text>
      </Space>

      {screenReadiness.screenStatus !== READINESS_STATUS.implemented && (
        <Alert
          showIcon
          type="warning"
          style={{ marginTop: 12 }}
          message={pickCognusText('Degradacao controlada ativa', 'Controlled degradation active')}
          description={pickCognusText(
            'Esta tela possui acoes bloqueadas e/ou simuladas. A interface explicita o motivo tecnico em cada acao.',
            'This screen has blocked and/or simulated actions. The interface makes the technical reason explicit in each action.'
          )}
        />
      )}

      {hasMockSources && (
        <Alert
          showIcon
          type="info"
          style={{ marginTop: 12 }}
          message={pickCognusText('Dados mockados identificados', 'Mocked data identified')}
          description={pickCognusText(
            'Dados simulados sao exibidos apenas para fontes sem endpoint do orquestrador disponivel nesta entrega.',
            'Simulated data is shown only for sources without an orchestrator endpoint available in this delivery.'
          )}
        />
      )}

      {blockedActionsCount > 0 && (
        <Alert
          showIcon
          type="error"
          style={{ marginTop: 12 }}
          message={pickCognusText(
            `${blockedActionsCount} acao(oes) bloqueada(s) por indisponibilidade de backend`,
            `${blockedActionsCount} action(s) blocked due to backend unavailability`
          )}
          description={pickCognusText(
            'As acoes bloqueadas permanecem desabilitadas na toolbar com justificativa objetiva em tooltip.',
            'Blocked actions remain disabled in the toolbar with an objective justification in the tooltip.'
          )}
        />
      )}

      <Table
        style={{ marginTop: 12 }}
        rowKey="key"
        columns={actionColumns}
        dataSource={actionRows}
        pagination={false}
        size="small"
        scroll={{ x: 1500 }}
      />

      <Table
        style={{ marginTop: 12 }}
        rowKey="key"
        columns={sourceColumns}
        dataSource={sourceRows}
        pagination={false}
        size="small"
        scroll={{ x: 1300 }}
      />
    </div>
  );
};

ProvisioningReadinessCard.defaultProps = {
  actionOrder: [],
};

export default ProvisioningReadinessCard;
