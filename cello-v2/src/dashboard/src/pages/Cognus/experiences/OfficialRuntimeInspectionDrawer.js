import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Descriptions,
  Divider,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd';
import {
  ReloadOutlined,
  VerticalAlignBottomOutlined,
} from '@ant-design/icons';
import {
  ORG_RUNTIME_COMPONENT_TYPE_LABEL,
  ORG_RUNTIME_STATUS_LABEL,
  formatRuntimeTopologyUtcTimestamp,
} from './provisioningRuntimeTopologyUtils';
import {
  sanitizeSensitiveText,
  sanitizeStructuredData,
} from '../../../utils/provisioningSecurityRedaction';
import { OperationalWindowDialog } from '../components/OperationalWindowManager';
import {
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from '../cognusI18n';
import styles from './OfficialRuntimeInspectionDrawer.less';

const normalizeText = value => String(value || '').trim();
const ANSI_ESCAPE_PATTERN = new RegExp(
  `${String.fromCharCode(27)}\\[[0-9;?]*[ -/]*[@-~]`,
  'g'
);
const LOG_LINE_PATTERN =
  /^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+UTC)\s+([0-9a-fA-F]+)\s+([A-Z]+)\s?(.*)$/;
const LOG_MESSAGE_TAG_PATTERN = /(\[[^\]]+\])/g;

const formatInspectionScopeLabel = (scope, localeCandidate) =>
  (
    {
      docker_inspect: 'docker inspect',
      docker_logs: pickCognusText(
        'docker logs snapshot',
        'docker logs snapshot',
        localeCandidate
      ),
      environment: pickCognusText(
        'redacted environment variables',
        'redacted environment variables',
        localeCandidate
      ),
      mounts: 'mounts',
      ports: pickCognusText('ports', 'ports', localeCandidate),
    }[scope]
  ) ||
  scope ||
  '-';

const stringifyInspectionPayload = value => {
  if (value === null || value === undefined) {
    return '-';
  }

  if (typeof value === 'string') {
    return sanitizeSensitiveText(value) || '-';
  }

  try {
    return JSON.stringify(sanitizeStructuredData(value), null, 2);
  } catch (error) {
    return '-';
  }
};

const stripAnsiSequences = value => String(value || '').replace(ANSI_ESCAPE_PATTERN, '');

const renderStructuredConsoleValue = (title, value, language = 'json') => (
  <div className={styles.codePanel}>
    <div className={styles.codePanelHeader}>
      <div>
        <Typography.Text className={styles.codePanelTitle}>{title}</Typography.Text>
      </div>
      <div className={styles.codePanelHeaderMeta}>
        <Tag className={styles.codePanelLanguageTag}>{language}</Tag>
      </div>
    </div>
    <pre className={styles.codePanelSurface}>{value}</pre>
  </div>
);

const renderLogMessage = message => {
  const parts = String(message || '').split(LOG_MESSAGE_TAG_PATTERN);
  let cursor = 0;
  return parts.map((part, index) => {
    if (!part) {
      return null;
    }
    const partKey = `${cursor}-${part.slice(0, 24)}-${index}`;
    cursor += part.length;
    if (/^\[[^\]]+\]$/.test(part)) {
      return (
        <span key={partKey} className={styles.logTagToken}>
          {part}
        </span>
      );
    }
    return (
      <span key={partKey} className={styles.logMessageText}>
        {part}
      </span>
    );
  });
};

const renderLogLine = ({ key, line }) => {
  const normalizedLine = stripAnsiSequences(line);
  if (!normalizedLine) {
    return <div key={key} className={styles.logLineSpacer} />;
  }

  const matchedLine = normalizedLine.match(LOG_LINE_PATTERN);
  if (!matchedLine) {
    return (
      <div key={key} className={styles.logLine}>
        <span className={styles.logMessageText}>{normalizedLine}</span>
      </div>
    );
  }

  const [, timestamp, sequence, level, message] = matchedLine;
  const normalizedLevel = String(level || '').toLowerCase();
  const levelClassName = styles[`logLevel${level}`] || styles.logLevelGeneric;

  return (
    <div key={key} className={styles.logLine}>
      <span className={styles.logTimestamp}>{timestamp}</span>
      <span className={styles.logSequence}>{sequence}</span>
      <span className={`${styles.logLevel} ${levelClassName}`}>{normalizedLevel}</span>
      <span className={styles.logMessage}>{renderLogMessage(message)}</span>
    </div>
  );
};

const hasStructuredEntries = value => {
  if (Array.isArray(value)) {
    return value.length > 0;
  }
  if (value && typeof value === 'object') {
    return Object.keys(value).length > 0;
  }
  return Boolean(value);
};

const resolveScopePayload = (payload, scopeKey) => {
  if (!payload || !payload.scopes || !payload.scopes[scopeKey]) {
    return {};
  }
  return payload.scopes[scopeKey].payload || {};
};

const resolveScopeCache = (payload, scopeKey) => {
  if (!payload || !payload.scopes || !payload.scopes[scopeKey]) {
    return {};
  }
  return payload.scopes[scopeKey].cache || {};
};

const OfficialRuntimeInspectionDrawer = ({
  inspectionState,
  onClose,
  onRefresh,
}) => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale);
  const [autoRefreshEnabled, setAutoRefreshEnabled] = useState(true);
  const [followLatestLogs, setFollowLatestLogs] = useState(true);
  const logsViewportRef = useRef(null);
  const inspectionOpen = Boolean(inspectionState && inspectionState.open);
  const inspectionRow = (inspectionState && inspectionState.row) || null;
  const inspectionLoading = Boolean(inspectionState && inspectionState.loading);
  const payload = inspectionState && inspectionState.payload ? inspectionState.payload : null;
  const dockerInspectPayload = useMemo(
    () => resolveScopePayload(payload, 'docker_inspect'),
    [payload]
  );
  const dockerLogsPayload = useMemo(() => resolveScopePayload(payload, 'docker_logs'), [payload]);
  const environmentPayload = useMemo(() => resolveScopePayload(payload, 'environment'), [payload]);
  const portsPayload = useMemo(() => resolveScopePayload(payload, 'ports'), [payload]);
  const mountsPayload = useMemo(() => resolveScopePayload(payload, 'mounts'), [payload]);
  const dockerInspectCache = useMemo(() => resolveScopeCache(payload, 'docker_inspect'), [payload]);
  const component = payload && payload.component ? payload.component : {};
  const correlation = payload && payload.correlation ? payload.correlation : {};
  const state = dockerInspectPayload && dockerInspectPayload.state ? dockerInspectPayload.state : {};
  const health = dockerInspectPayload && dockerInspectPayload.health ? dockerInspectPayload.health : {};
  const labels = dockerInspectPayload && dockerInspectPayload.labels ? dockerInspectPayload.labels : {};
  const envRows =
    (environmentPayload && environmentPayload.env) ||
    (dockerInspectPayload && dockerInspectPayload.env) ||
    [];
  const ports =
    (portsPayload && portsPayload.ports) || (dockerInspectPayload && dockerInspectPayload.ports) || [];
  const mounts =
    (mountsPayload && mountsPayload.mounts) ||
    (dockerInspectPayload && dockerInspectPayload.mounts) ||
    [];
  const dockerLogsConsoleValue = useMemo(() => {
    const containerName =
      dockerInspectPayload.container_name || component.container_name || '<container>';
    const logOutput =
      stripAnsiSequences(sanitizeSensitiveText(dockerLogsPayload.logs || '-')) || '-';
    return {
      command: `$ docker logs --tail 50 ${containerName}`,
      output: logOutput,
    };
  }, [component.container_name, dockerInspectPayload.container_name, dockerLogsPayload.logs]);
  const dockerLogLines = useMemo(() => {
    const rawOutput = dockerLogsConsoleValue.output || '-';
    let cursor = 0;
    return rawOutput.split(/\r?\n/).map(line => {
      const key = `${cursor}-${line.slice(0, 32)}`;
      cursor += line.length + 1;
      return {
        key,
        line,
      };
    });
  }, [dockerLogsConsoleValue.output]);
  const isLocalSnapshot =
    normalizeText(payload && payload.inspection_scope).toLowerCase() === 'snapshot_local' ||
    normalizeText(dockerInspectCache.collection_status).toLowerCase() === 'snapshot_local';
  const isCachedInspectionFallback = Boolean(
    payload && payload.cached_status_fallback && payload.cached_status_fallback.active
  );
  const inspectionIssues = useMemo(
    () =>
      (Array.isArray(payload && payload.issues) ? payload.issues : []).filter(issue => {
        const issueCode = normalizeText(issue && issue.code).toLowerCase();
        return issueCode && issueCode !== 'snapshot_local_only';
      }),
    [payload]
  );
  let inspectionModeLabel = t('inspecao oficial', 'official inspection');
  let inspectionModeDescription = t(
    'Inspecao oficial com refresh sob demanda e logs sanitizados em tema escuro.',
    'Official inspection with on-demand refresh and sanitized logs in a dark theme.'
  );
  if (isLocalSnapshot) {
    inspectionModeLabel = t('snapshot local sintetico', 'synthetic local snapshot');
    inspectionModeDescription = t(
      'Runbook remoto expirado e sem cache de inspecao. A leitura abaixo usa um snapshot sintetico minimo do componente.',
      'The remote runbook expired without an inspection cache. The view below uses a minimal synthetic snapshot of the component.'
    );
  } else if (isCachedInspectionFallback) {
    inspectionModeLabel = t('cache oficial local', 'local official cache');
    inspectionModeDescription = t(
      'Runbook remoto expirado. A leitura abaixo reaproveita a ultima inspecao runtime oficial preservada localmente.',
      'The remote runbook expired. The view below reuses the latest official runtime inspection preserved locally.'
    );
  }

  let refreshButtonLabel = t('Refresh oficial', 'Refresh official');
  if (isLocalSnapshot || isCachedInspectionFallback) {
    refreshButtonLabel = t('Refresh remoto indisponivel', 'Remote refresh unavailable');
  } else if (autoRefreshEnabled) {
    refreshButtonLabel = t('Refresh oficial ativo', 'Official refresh active');
  }

  let cacheAlertType = 'info';
  let cacheAlertMessage = t('Cache oficial atualizado', 'Official cache updated');
  let cacheAlertDescription = t(
    'Leitura oficial retornada pelo cache de inspecao runtime.',
    'Official reading returned from the runtime inspection cache.'
  );
  if (isLocalSnapshot) {
    cacheAlertType = 'warning';
    cacheAlertMessage = t('Modo snapshot local sintetico', 'Synthetic local snapshot mode');
    cacheAlertDescription = t(
      'O backend oficial nao mantém mais este run_id e nao havia cache de inspecao suficiente. O componente continua visivel apenas com um resumo minimo sintetico.',
      'The official backend no longer keeps this run_id and there was not enough inspection cache. The component remains visible only through a minimal synthetic summary.'
    );
  } else if (isCachedInspectionFallback) {
    cacheAlertType = 'warning';
    cacheAlertMessage = t(
      'Inspecao oficial cacheada localmente',
      'Official inspection cached locally'
    );
    cacheAlertDescription = t(
      'O backend oficial nao mantém mais este run_id, mas os dados abaixo vieram da ultima coleta oficial preservada localmente, incluindo logs e docker inspect do container.',
      'The official backend no longer keeps this run_id, but the data below came from the latest official collection preserved locally, including container logs and docker inspect.'
    );
  } else if (payload?.stale) {
    cacheAlertType = 'warning';
    cacheAlertMessage = t('Cache oficial stale', 'Official cache stale');
    cacheAlertDescription = t(
      'Dados expirados permanecem visiveis para troubleshooting e nao sao tratados como telemetria ao vivo.',
      'Expired data remains visible for troubleshooting and is not treated as live telemetry.'
    );
  }

  useEffect(() => {
    if (isLocalSnapshot || isCachedInspectionFallback) {
      setAutoRefreshEnabled(false);
    }
  }, [isCachedInspectionFallback, isLocalSnapshot]);

  useEffect(() => {
    if (inspectionOpen) {
      setFollowLatestLogs(true);
    }
  }, [inspectionOpen]);

  useEffect(() => {
    if (!inspectionOpen) {
      return undefined;
    }

    if (isLocalSnapshot || isCachedInspectionFallback || !autoRefreshEnabled || !inspectionRow) {
      return undefined;
    }

    const intervalId = window.setInterval(() => {
      if (!inspectionLoading) {
        onRefresh();
      }
    }, 5000);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [
    autoRefreshEnabled,
    inspectionLoading,
    inspectionOpen,
    inspectionRow,
    isCachedInspectionFallback,
    isLocalSnapshot,
    onRefresh,
  ]);

  useEffect(() => {
    if (!followLatestLogs || !logsViewportRef.current) {
      return;
    }
    const viewportNode = logsViewportRef.current;
    viewportNode.scrollTop = viewportNode.scrollHeight;
  }, [dockerLogsConsoleValue.output, followLatestLogs]);

  const handleLogsScroll = event => {
    const viewportNode = event.currentTarget;
    const distanceToBottom =
      viewportNode.scrollHeight - viewportNode.scrollTop - viewportNode.clientHeight;
    setFollowLatestLogs(distanceToBottom <= 24);
  };

  const handleFollowLatestLogs = () => {
    setFollowLatestLogs(true);
    if (logsViewportRef.current) {
      logsViewportRef.current.scrollTop = logsViewportRef.current.scrollHeight;
    }
  };

  return (
    <OperationalWindowDialog
      windowId="official-runtime-inspection"
      eyebrow={t('Inspecao tecnica', 'Technical inspection')}
      title={
        inspectionRow
          ? t(
              'Inspecao runtime oficial: {name}',
              'Official runtime inspection: {name}',
              { name: inspectionRow.componentName || inspectionRow.componentId }
            )
          : t('Inspecao runtime oficial', 'Official runtime inspection')
      }
      onClose={onClose}
      open={inspectionOpen}
      preferredWidth="74vw"
      preferredHeight="74vh"
    >
      <div className={styles.toolbar}>
        <Space wrap>
          <Tag color={isLocalSnapshot || isCachedInspectionFallback ? 'orange' : 'blue'}>
            {inspectionModeLabel}
          </Tag>
          <Typography.Text type="secondary">{inspectionModeDescription}</Typography.Text>
        </Space>
        <Space wrap>
          <Button
            icon={<ReloadOutlined />}
            onClick={onRefresh}
            loading={inspectionLoading}
            disabled={!inspectionRow || isLocalSnapshot || isCachedInspectionFallback}
          >
            {refreshButtonLabel}
          </Button>
        </Space>
      </div>

      {inspectionState && inspectionState.error && !isLocalSnapshot && !isCachedInspectionFallback && (
        <Alert
          showIcon
          type="error"
          message={t(
            'Falha na consulta oficial de inspecao runtime',
            'Failed to fetch the official runtime inspection'
          )}
          description={inspectionState.error}
        />
      )}

      {payload && (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Alert
            showIcon
            type={cacheAlertType}
            message={cacheAlertMessage}
            description={cacheAlertDescription}
          />

          <Descriptions bordered size="small" column={2}>
            <Descriptions.Item label="run_id">
              <Typography.Text code>{correlation.run_id || '-'}</Typography.Text>
            </Descriptions.Item>
            <Descriptions.Item label="change_id">
              <Typography.Text code>{correlation.change_id || '-'}</Typography.Text>
            </Descriptions.Item>
            <Descriptions.Item label={t('Organization', 'Organization')}>
              {component.org_id || '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Host', 'Host')}>{component.host_id || '-'}</Descriptions.Item>
            <Descriptions.Item label="component_id">
              <Typography.Text code>{component.component_id || '-'}</Typography.Text>
            </Descriptions.Item>
            <Descriptions.Item label={t('Type', 'Type')}>
              {ORG_RUNTIME_COMPONENT_TYPE_LABEL[component.component_type] ||
                component.component_type ||
                '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Container', 'Container')}>
              <Typography.Text code>
                {dockerInspectPayload.container_name || component.container_name || '-'}
              </Typography.Text>
            </Descriptions.Item>
            <Descriptions.Item label={t('Image', 'Image')}>
              {dockerInspectPayload.image || component.image || '-'}
            </Descriptions.Item>
            <Descriptions.Item label="Platform">
              {dockerInspectPayload.platform || component.platform || '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Status', 'Status')}>
              {ORG_RUNTIME_STATUS_LABEL[component.status] ||
                state.status ||
                component.status ||
                '-'}
            </Descriptions.Item>
            <Descriptions.Item label="started_at">
              {formatRuntimeTopologyUtcTimestamp(state.started_at)}
            </Descriptions.Item>
            <Descriptions.Item label={t('Requested scope', 'Requested scope')}>
              {formatInspectionScopeLabel(payload.inspection_scope, locale)}
            </Descriptions.Item>
            <Descriptions.Item label="inspection_source">
              {dockerInspectCache.inspection_source || '-'}
            </Descriptions.Item>
          </Descriptions>

          <div className={styles.summaryGrid}>
            <div className={styles.summaryCard}>
              <div className={styles.summaryLabel}>{t('Health', 'Health')}</div>
              <div className={styles.summaryValue}>{health.status || state.status || '-'}</div>
            </div>
            <div className={styles.summaryCard}>
              <div className={styles.summaryLabel}>{t('Em execução', 'Running')}</div>
              <div className={styles.summaryValue}>{String(state.running ?? '-')}</div>
            </div>
            <div className={styles.summaryCard}>
              <div className={styles.summaryLabel}>{t('Contagem de reinícios', 'Restart count')}</div>
              <div className={styles.summaryValue}>{state.restart_count ?? '-'}</div>
            </div>
            <div className={styles.summaryCard}>
              <div className={styles.summaryLabel}>{t('Updated at', 'Updated at')}</div>
              <div className={styles.summaryValue}>
                {formatRuntimeTopologyUtcTimestamp(dockerInspectCache.refreshed_at)}
              </div>
            </div>
          </div>

          {inspectionIssues.length > 0 && (
            <Alert
              showIcon
              type="warning"
              message={t(
                'Falhas preservadas na trilha operacional',
                'Failures preserved in the operational trail'
              )}
              description={inspectionIssues.map(issue => issue.message || issue.code).join(' | ')}
            />
          )}

          <Divider orientation="left">{t('Resumo tecnico', 'Technical summary')}</Divider>
          <Descriptions bordered size="small" column={2}>
            <Descriptions.Item label={t('Status de health', 'Health status')}>
              {health.status || '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Sequência de falhas', 'Failing streak')}>
              {health.failing_streak ?? '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Em execução', 'Running')}>
              {String(state.running ?? '-')}
            </Descriptions.Item>
            <Descriptions.Item label={t('Contagem de reinícios', 'Restart count')}>
              {state.restart_count ?? '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Código de saída', 'Exit code')}>
              {state.exit_code ?? '-'}
            </Descriptions.Item>
            <Descriptions.Item label={t('Finalizado em', 'Finished at')}>
              {formatRuntimeTopologyUtcTimestamp(state.finished_at)}
            </Descriptions.Item>
            <Descriptions.Item label={t('Recência do cache', 'Cache freshness')}>
              <Space wrap>
                <Tag color={payload.stale ? 'orange' : 'green'}>
                  {payload.stale ? t('desatualizado', 'stale') : t('atual', 'fresh')}
                </Tag>
                <Tag color={dockerInspectCache.cache_hit ? 'blue' : 'default'}>
                  {dockerInspectCache.cache_hit
                    ? t('cache_hit', 'cache_hit')
                    : t('cache_miss', 'cache_miss')}
                </Tag>
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="collection_status">
              {dockerInspectCache.collection_status || '-'}
            </Descriptions.Item>
          </Descriptions>

          <Divider orientation="left">{t('Logs sanitizados', 'Sanitized logs')}</Divider>
          <div className={styles.terminalPanel}>
            <div className={styles.terminalPanelHeader}>
              <div className={styles.terminalPanelChrome}>
                <span className={styles.terminalChromeDot} />
                <span className={styles.terminalChromeDot} />
                <span className={styles.terminalChromeDot} />
              </div>
              <div className={styles.terminalPanelContext}>
                <Typography.Text className={styles.terminalPanelTitle}>
                  {t('Snapshot sanitizado do container', 'Sanitized container snapshot')}
                </Typography.Text>
                <Tag className={styles.terminalModeTag}>{t('terminal ubuntu', 'ubuntu terminal')}</Tag>
                <Tag className={styles.terminalModeTagMuted}>{t('logs', 'logs')}</Tag>
              </div>
              <Button
                size="small"
                icon={<VerticalAlignBottomOutlined />}
                onClick={handleFollowLatestLogs}
                className={`${styles.followLogsButton} ${
                  followLatestLogs ? styles.followLogsButtonActive : ''
                }`}
              >
                {followLatestLogs
                  ? t('Seguindo logs recentes', 'Following latest logs')
                  : t('Ir para o fim', 'Jump to bottom')}
              </Button>
            </div>

            <div
              ref={logsViewportRef}
              className={styles.terminalViewport}
              onScroll={handleLogsScroll}
            >
              <div className={styles.terminalCommandRow}>
                <span className={styles.terminalPrompt}>cognus@ubuntu</span>
                <span className={styles.terminalCommand}>{dockerLogsConsoleValue.command}</span>
              </div>
              {dockerLogLines.map(renderLogLine)}
            </div>
          </div>

          {hasStructuredEntries(labels) && (
            <>
              <Divider orientation="left">{t('Labels', 'Labels')}</Divider>
              {renderStructuredConsoleValue(
                t('Labels publicados no runtime', 'Labels published in the runtime'),
                stringifyInspectionPayload(labels)
              )}
            </>
          )}

          {hasStructuredEntries(envRows) && (
            <>
              <Divider orientation="left">
                {t('Variaveis redigidas', 'Redacted variables')}
              </Divider>
              {renderStructuredConsoleValue(
                t('Variaveis de ambiente redigidas', 'Redacted environment variables'),
                stringifyInspectionPayload(envRows)
              )}
            </>
          )}

          {hasStructuredEntries(ports) && (
            <>
              <Divider orientation="left">{t('Portas', 'Ports')}</Divider>
              {renderStructuredConsoleValue(
                t('Portas publicadas e expostas', 'Published and exposed ports'),
                stringifyInspectionPayload(ports)
              )}
            </>
          )}

          {hasStructuredEntries(mounts) && (
            <>
              <Divider orientation="left">{t('Mounts', 'Mounts')}</Divider>
              {renderStructuredConsoleValue(
                t('Mounts e binds observados', 'Observed mounts and binds'),
                stringifyInspectionPayload(mounts)
              )}
            </>
          )}

          {hasStructuredEntries(health.logs) && !isLocalSnapshot && (
            <>
              <Divider orientation="left">{t('Resumo de health', 'Health summary')}</Divider>
              {renderStructuredConsoleValue(
                t('Resumo de healthchecks', 'Healthcheck summary'),
                stringifyInspectionPayload(health.logs)
              )}
            </>
          )}
        </Space>
      )}

      {inspectionState && inspectionState.loading && <Spin spinning />}
    </OperationalWindowDialog>
  );
};

export default OfficialRuntimeInspectionDrawer;
