import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Badge,
  Button,
  Input,
  Progress,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import {
  CheckCircleOutlined,
  DownloadOutlined,
  LockOutlined,
  ReloadOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import styles from '../components/NeoOpsLayout.less';
import { screenByKey } from '../data/screens';
import { listNode } from '@/services/node';
import { listOrganization } from '@/services/organization';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import ProvisioningReadinessCard from './ProvisioningReadinessCard';
import {
  INVENTORY_COVERAGE_TONE_MAP,
  INVENTORY_PROVIDER_SCOPE,
  INVENTORY_SOURCE_STATUS,
  buildInventoryExportPayload,
  buildInventoryFilterOptions,
  createInventorySnapshotState,
  downloadInventorySnapshotFile,
  evaluateOfficialInventoryEvidenceGate,
  filterInventorySnapshot,
  formatInventoryUtcTimestamp,
  synchronizeInventorySnapshot,
} from './provisioningInventoryUtils';
import {
  EVIDENCE_STATUS_TONE_MAP,
  buildEvidenceExportPayload,
  buildExecutionEvidenceRows,
  createImmutableEvidenceBundle,
  downloadEvidencePayloadFile,
  evaluateExecutionEvidenceGate,
  formatEvidenceUtcTimestamp,
  truncateEvidenceFingerprint,
} from './provisioningEvidenceUtils';
import { getRunbookStatus } from '../../../services/runbook';
import {
  getProvisioningActionReadiness,
  READINESS_EXECUTION_MODE,
} from './provisioningBackendReadiness';
import {
  IS_PROVISIONING_OPERATIONAL_ENV,
  PROVISIONING_ENV_PROFILE,
  recordProvisioningFallbackUsage,
} from '../../../services/provisioningEnvironmentPolicy';
import { sanitizeSensitiveText } from '@/utils/provisioningSecurityRedaction';
import {
  formatCognusTemplate,
  resolveCognusLocale,
} from '../cognusI18n';

const providerOptions = [
  {
    value: INVENTORY_PROVIDER_SCOPE,
    label: 'external-linux (VM Linux)',
  },
];

const screen = screenByKey['e1-inventario'];

const inventoryReadinessActionOrder = [
  'manual_sync',
  'export_snapshot',
  'close_execution',
  'export_evidence',
];

const statusTagColor = {
  default: 'default',
  processing: 'blue',
  warning: 'orange',
  error: 'red',
  success: 'green',
};

const runtimeToneMap = {
  up: 'success',
  active: 'success',
  ready: 'success',
  partial: 'warning',
  pending: 'warning',
};

const renderStatusTag = (status, toneMap = {}) => {
  const normalized = String(status || '')
    .trim()
    .toLowerCase();
  const tone = toneMap[normalized] || toneMap.default || 'default';
  return <Tag color={statusTagColor[tone] || 'default'}>{normalized || '-'}</Tag>;
};

const resolveCoverageProgressStatus = status => {
  if (status === INVENTORY_SOURCE_STATUS.ready) {
    return 'success';
  }
  if (status === INVENTORY_SOURCE_STATUS.pending) {
    return 'exception';
  }
  return 'active';
};

const normalizeText = value => String(value || '').trim();
const localizeInventoryText = (ptBR, enUS, localeCandidate, values) =>
  formatCognusTemplate(ptBR, enUS, values, localeCandidate || resolveCognusLocale());

const toSlug = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '');

const resolveCoverageStatus = coveragePercent => {
  if (coveragePercent >= 100) {
    return INVENTORY_SOURCE_STATUS.ready;
  }
  if (coveragePercent > 0) {
    return INVENTORY_SOURCE_STATUS.partial;
  }
  return INVENTORY_SOURCE_STATUS.pending;
};

const extractSuccessfulRows = response => {
  if (!response || response.status !== 'successful') {
    return [];
  }

  const rows = response.data && response.data.data;
  return Array.isArray(rows) ? rows : [];
};

const mapOrganizationsFromApi = rows => {
  const organizationMapByRawId = {};
  const mappedRows = rows.map((row, index) => {
    const rawName = normalizeText(row && row.name);
    const fallbackId = `org-${index + 1}`;
    const orgId = toSlug(rawName) || fallbackId;
    const safeName = rawName || orgId;
    const mappedRecord = {
      orgId,
      displayName: safeName.toUpperCase(),
      mspId: `${safeName.replace(/[^a-zA-Z0-9]/g, '').slice(0, 24) || 'Org'}MSP`,
      environment: 'dev-external-linux',
      status: 'active',
    };

    const rawId = normalizeText(row && row.id);
    if (rawId) {
      organizationMapByRawId[rawId] = mappedRecord.orgId;
    }

    return mappedRecord;
  });

  return {
    rows: mappedRows,
    organizationMapByRawId,
  };
};

const mapNodesFromApi = (rows, organizationMapByRawId) =>
  rows.map((row, index) => {
    const rawName = normalizeText(row && row.name);
    const nodeId = toSlug(rawName) || `node-${index + 1}`;
    const rawType = normalizeText(row && row.type).toLowerCase();
    const nodeType = rawType || 'peer';
    const rawStatus = normalizeText(row && row.status).toLowerCase();
    const status = rawStatus || 'pending';
    const hostLabel = normalizeText(row && row.urls).split(':')[0];
    const hostId = toSlug(hostLabel) || `vm-api-${index + 1}`;
    const rawOrganizationId = normalizeText(row && row.organization);
    const mappedOrgId = organizationMapByRawId[rawOrganizationId];

    return {
      nodeId,
      nodeType,
      orgId: mappedOrgId || 'org-unknown',
      hostId,
      environment: 'dev-external-linux',
      providerKey: INVENTORY_PROVIDER_SCOPE,
      status,
    };
  });

const ProvisioningInventoryPage = () => {
  const locale = resolveCognusLocale();
  const t = React.useCallback(
    (ptBR, enUS, values) => localizeInventoryText(ptBR, enUS, locale, values),
    [locale]
  );
  const [changeId, setChangeId] = useState('cr-2026-02-16-003');
  const [runId, setRunId] = useState('run-e1-inventory-pending');
  const [snapshot, setSnapshot] = useState(() =>
    createInventorySnapshotState({
      changeId: 'cr-2026-02-16-003',
      runId: 'run-e1-inventory-pending',
    })
  );
  const [isSyncing, setIsSyncing] = useState(false);
  const [executor, setExecutor] = useState('ops.cognus@ufg.br');
  const [immutableEvidenceBundle, setImmutableEvidenceBundle] = useState(null);
  const [officialRunEvidence, setOfficialRunEvidence] = useState({
    loading: false,
    run: null,
    error: '',
  });
  const [filters, setFilters] = useState({
    organization: 'all',
    channel: 'all',
    environment: 'all',
  });
  const evidenceViewLocked = Boolean(immutableEvidenceBundle);
  const manualSyncActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-inventario', 'manual_sync'),
    []
  );
  const exportSnapshotActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-inventario', 'export_snapshot'),
    []
  );
  const closeExecutionActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-inventario', 'close_execution'),
    []
  );
  const exportEvidenceActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-inventario', 'export_evidence'),
    []
  );

  const buildReadinessTooltip = readiness => {
    if (!readiness.available) {
      return t('Acao bloqueada: {reason}', 'Action blocked: {reason}', {
        reason: readiness.reason,
      });
    }
    return t('Modo: {mode}. {reason}', 'Mode: {mode}. {reason}', {
      mode: readiness.modeLabel,
      reason: readiness.reason,
    });
  };

  const filterOptions = useMemo(() => buildInventoryFilterOptions(snapshot), [snapshot]);
  const filteredInventory = useMemo(() => filterInventorySnapshot(snapshot, filters), [
    snapshot,
    filters,
  ]);

  const organizationSelectOptions = useMemo(
    () => [
      { value: 'all', label: t('todas as organizações', 'all organizations') },
      ...filterOptions.organizations.map(value => ({
        value,
        label: value,
      })),
    ],
    [filterOptions.organizations, t]
  );

  const channelSelectOptions = useMemo(
    () => [
      { value: 'all', label: t('todos os canais', 'all channels') },
      ...filterOptions.channels.map(value => ({
        value,
        label: value,
      })),
    ],
    [filterOptions.channels, t]
  );

  const environmentSelectOptions = useMemo(
    () => [
      { value: 'all', label: t('todos os ambientes', 'all environments') },
      ...filterOptions.environments.map(value => ({
        value,
        label: value,
      })),
    ],
    [filterOptions.environments, t]
  );

  const filteredSummary = useMemo(
    () => ({
      organizations: filteredInventory.organizations.length,
      channels: filteredInventory.channels.length,
      hosts: filteredInventory.hosts.length,
      nodes: filteredInventory.nodes.length,
      cryptoBaseline: filteredInventory.cryptoBaseline.length,
    }),
    [filteredInventory]
  );

  useEffect(() => {
    const normalizedRunId = runId.trim();
    if (!normalizedRunId) {
      setOfficialRunEvidence({
        loading: false,
        run: null,
        error: t(
          'run_id não informado para consulta oficial de evidências.',
          'run_id not informed for the official evidence query.'
        ),
      });
      return undefined;
    }

    let cancelled = false;
    setOfficialRunEvidence(currentState => ({
      ...currentState,
      loading: true,
      error: '',
    }));

    getRunbookStatus(normalizedRunId)
      .then(response => {
        if (cancelled) {
          return;
        }
        setOfficialRunEvidence({
          loading: false,
          run: response && response.run ? response.run : null,
          error: '',
        });
      })
      .catch(error => {
        if (cancelled) {
          return;
        }
        setOfficialRunEvidence({
          loading: false,
          run: null,
          error:
            (error && error.message) ||
            t(
              'Falha ao consultar status oficial do runbook para composição de evidências.',
              'Failed to query the official runbook status for evidence composition.'
            ),
        });
      });

    return () => {
      cancelled = true;
    };
  }, [runId, t]);

  const evidenceRows = useMemo(() => {
    if (immutableEvidenceBundle) {
      return immutableEvidenceBundle.rows.map(row => ({ ...row }));
    }

    return buildExecutionEvidenceRows({
      changeId: changeId.trim(),
      runId: runId.trim(),
      executor: executor.trim(),
      snapshot,
      officialRun: officialRunEvidence.run,
    });
  }, [immutableEvidenceBundle, changeId, executor, runId, snapshot, officialRunEvidence.run]);

  const evidenceGate = useMemo(() => {
    if (immutableEvidenceBundle) {
      return immutableEvidenceBundle.gate;
    }

    return evaluateExecutionEvidenceGate({
      evidenceRows,
      changeId: changeId.trim(),
      runId: runId.trim(),
    });
  }, [immutableEvidenceBundle, evidenceRows, changeId, runId]);

  const officialArtifactGate = useMemo(
    () =>
      evaluateOfficialInventoryEvidenceGate({
        officialRun: officialRunEvidence.run,
        changeId: changeId.trim(),
        runId: runId.trim(),
      }),
    [officialRunEvidence.run, changeId, runId]
  );

  const closeExecutionAllowed =
    evidenceGate.allowCloseExecution && officialArtifactGate.allowCloseExecution;

  const handleManualSync = async () => {
    if (!manualSyncActionReadiness.available) {
      message.error(manualSyncActionReadiness.reason);
      return;
    }

    if (evidenceViewLocked) {
      message.warning(
        t(
          'Execução encerrada: dados imutáveis. Sincronização bloqueada.',
          'Execution closed: immutable data. Synchronization blocked.'
        )
      );
      return;
    }

    setIsSyncing(true);

    const syncedSnapshot = synchronizeInventorySnapshot(snapshot, {
      changeId: changeId.trim(),
      runId: runId.trim(),
    });

    let nextSnapshot = syncedSnapshot;
    let usedOfficialOrganizations = false;
    let usedOfficialNodes = false;

    try {
      const [organizationResponse, nodeResponse] = await Promise.all([
        listOrganization({ page: 1, per_page: 200 }),
        listNode({ page: 1, per_page: 400 }),
      ]);

      const organizationRows = extractSuccessfulRows(organizationResponse);
      const nodeRows = extractSuccessfulRows(nodeResponse);
      const mappedOrganizations = mapOrganizationsFromApi(organizationRows);
      const mappedNodes = mapNodesFromApi(nodeRows, mappedOrganizations.organizationMapByRawId);
      usedOfficialOrganizations = mappedOrganizations.rows.length > 0;
      usedOfficialNodes = mappedNodes.length > 0;

      const hasOfficialLegacyData = usedOfficialOrganizations || usedOfficialNodes;
      if (IS_PROVISIONING_OPERATIONAL_ENV && !hasOfficialLegacyData) {
        recordProvisioningFallbackUsage({
          domain: 'inventory',
          action: 'manual_sync',
          reasonCode: 'inventory_official_data_unavailable_operational_mode',
          details: t(
            'Sincronização oficial sem dados legados (organizations/nodes). Fallback degradado bloqueado em ambiente operacional.',
            'Official synchronization without legacy data (organizations/nodes). Degraded fallback blocked in the operational environment.'
          ),
        });
        setIsSyncing(false);
        message.error(
          t(
            'Sincronização oficial bloqueada no perfil {profile}: sem dados oficiais legados e fallback degradado proibido.',
            'Official synchronization blocked in profile {profile}: no official legacy data and degraded fallback is forbidden.',
            {
              profile: PROVISIONING_ENV_PROFILE,
            }
          )
        );
        return;
      }

      if (!hasOfficialLegacyData) {
        recordProvisioningFallbackUsage({
          domain: 'inventory',
          action: 'manual_sync',
          reasonCode: 'inventory_official_data_unavailable_degraded_mode',
          details: t(
            'Sincronização em modo degradado aplicada porque endpoints oficiais legados retornaram vazio.',
            'Synchronization in degraded mode applied because official legacy endpoints returned empty data.'
          ),
        });
      }

      const patchedSourceRows = nextSnapshot.sourceRows.map(source => {
        if (source.key !== 'legacy_inventory_api') {
          return source;
        }

        return {
          ...source,
          status: hasOfficialLegacyData
            ? INVENTORY_SOURCE_STATUS.ready
            : INVENTORY_SOURCE_STATUS.partial,
          coveragePercent: hasOfficialLegacyData ? 100 : 65,
          lastSyncedAt: nextSnapshot.syncedAt,
        };
      });

      const coveragePercent =
        patchedSourceRows.length > 0
          ? Math.round(
              patchedSourceRows.reduce(
                (accumulator, source) => accumulator + source.coveragePercent,
                0
              ) / patchedSourceRows.length
            )
          : 0;

      nextSnapshot = {
        ...nextSnapshot,
        sourceRows: patchedSourceRows,
        coveragePercent,
        coverageStatus: resolveCoverageStatus(coveragePercent),
        records: {
          ...nextSnapshot.records,
          organizations:
            mappedOrganizations.rows.length > 0
              ? mappedOrganizations.rows
              : nextSnapshot.records.organizations,
          nodes: mappedNodes.length > 0 ? mappedNodes : nextSnapshot.records.nodes,
        },
      };
    } catch (error) {
      const patchedSourceRows = nextSnapshot.sourceRows.map(source => {
        if (source.key !== 'legacy_inventory_api') {
          return source;
        }

        return {
          ...source,
          status: INVENTORY_SOURCE_STATUS.partial,
          coveragePercent: 65,
          lastSyncedAt: nextSnapshot.syncedAt,
        };
      });

      const coveragePercent =
        patchedSourceRows.length > 0
          ? Math.round(
              patchedSourceRows.reduce(
                (accumulator, source) => accumulator + source.coveragePercent,
                0
              ) / patchedSourceRows.length
            )
          : 0;

      nextSnapshot = {
        ...nextSnapshot,
        sourceRows: patchedSourceRows,
        coveragePercent,
        coverageStatus: resolveCoverageStatus(coveragePercent),
      };

      if (IS_PROVISIONING_OPERATIONAL_ENV) {
        recordProvisioningFallbackUsage({
          domain: 'inventory',
          action: 'manual_sync',
          reasonCode: 'inventory_official_sync_failed_operational_mode',
          details: t(
            'Endpoints oficiais indisponíveis ({message}).',
            'Official endpoints unavailable ({message}).',
            {
              message: error?.message || t('falha não identificada', 'unidentified failure'),
            }
          ),
        });
        setIsSyncing(false);
        message.error(
          t(
            'Sincronização oficial indisponível no perfil {profile}: fallback degradado proibido para evitar sucesso aparente.',
            'Official synchronization unavailable in profile {profile}: degraded fallback forbidden to avoid apparent success.',
            {
              profile: PROVISIONING_ENV_PROFILE,
            }
          )
        );
        return;
      }

      recordProvisioningFallbackUsage({
        domain: 'inventory',
        action: 'manual_sync',
        reasonCode: 'inventory_official_sync_failed_degraded_mode',
        details: t(
          'Endpoints oficiais indisponíveis ({message}).',
          'Official endpoints unavailable ({message}).',
          {
            message: error?.message || t('falha não identificada', 'unidentified failure'),
          }
        ),
      });
    }

    setSnapshot(nextSnapshot);
    setIsSyncing(false);

    const modeLabel =
      manualSyncActionReadiness.executionMode === READINESS_EXECUTION_MODE.hybrid
        ? t('hibrido', 'hybrid')
        : t('degradado', 'degraded');
    const officialSourceSummary = t(
      'Fontes oficiais: organizacoes={orgs}; nos={nodes}.',
      'Official sources: organizations={orgs}; nodes={nodes}.',
      {
        orgs: usedOfficialOrganizations ? t('sim', 'yes') : t('nao', 'no'),
        nodes: usedOfficialNodes ? t('sim', 'yes') : t('no', 'no'),
      }
    );

    if (nextSnapshot.consistency.isConsistent) {
      message.success(
        t(
          'Inventario sincronizado ({phase}, modo {mode}). {summary}',
          'Inventory synchronized ({phase}, mode {mode}). {summary}',
          {
            phase: nextSnapshot.syncPhase,
            mode: modeLabel,
            summary: officialSourceSummary,
          }
        )
      );
      return;
    }

    message.warning(
      t(
        'Inventario sincronizado ({phase}, modo {mode}) com cobertura parcial. {summary}',
        'Inventory synchronized ({phase}, mode {mode}) with partial coverage. {summary}',
        {
          phase: nextSnapshot.syncPhase,
          mode: modeLabel,
          summary: officialSourceSummary,
        }
      )
    );
  };

  const handleExportSnapshot = () => {
    if (!exportSnapshotActionReadiness.available) {
      message.error(exportSnapshotActionReadiness.reason);
      return;
    }

    if (!snapshot.syncedAt) {
      message.error(
        t(
          'Execute uma sincronização manual antes de exportar o snapshot.',
          'Run a manual synchronization before exporting the snapshot.'
        )
      );
      return;
    }

    if (!snapshot.consistency.isConsistent) {
      message.error(
        t(
          'Exportação bloqueada: consistência mínima com artefatos do pipeline ainda não foi atendida.',
          'Export blocked: minimum consistency with pipeline artifacts has not been achieved yet.'
        )
      );
      return;
    }

    if (!officialArtifactGate.allowExport) {
      message.error(
        t(
          'Exportação bloqueada: evidência oficial A2 ausente/inconsistente. {issue}',
          'Export blocked: official A2 evidence missing/inconsistent. {issue}',
          {
            issue: sanitizeSensitiveText(officialArtifactGate.gateIssues[0] || ''),
          }
        )
      );
      return;
    }

    const payload = buildInventoryExportPayload(snapshot, filteredInventory, {
      filters,
      changeId: changeId.trim(),
      runId: runId.trim(),
      officialRun: officialRunEvidence.run,
      officialArtifactGate,
    });
    const fileName = `inventory-snapshot-e1-sync-${String(snapshot.syncCount).padStart(
      2,
      '0'
    )}.json`;

    downloadInventorySnapshotFile(fileName, payload);
    message.success(t('Snapshot exportado ({fileName}).', 'Snapshot exported ({fileName}).', { fileName }));
  };

  const handleCloseExecution = () => {
    if (!closeExecutionActionReadiness.available) {
      message.error(closeExecutionActionReadiness.reason);
      return;
    }

    if (evidenceViewLocked) {
      message.info(
        t(
          'Execução já encerrada com evidências imutáveis.',
          'Execution already closed with immutable evidence.'
        )
      );
      return;
    }

    if (!closeExecutionAllowed) {
      message.error(
        t(
          'Fechamento bloqueado: evidências obrigatórias ausentes/inconsistentes. {issue}',
          'Closure blocked: required evidence missing/inconsistent. {issue}',
          {
            issue: sanitizeSensitiveText(
              evidenceGate.gateIssues[0] || officialArtifactGate.gateIssues[0] || ''
            ),
          }
        )
      );
      return;
    }

    const nextBundle = createImmutableEvidenceBundle({
      evidenceRows,
      changeId: changeId.trim(),
      runId: runId.trim(),
      executor: executor.trim(),
    });
    setImmutableEvidenceBundle(nextBundle);
    message.success(
      t(
        'Execução encerrada. Painel de evidências bloqueado em modo imutável.',
        'Execution closed. Evidence panel locked in immutable mode.'
      )
    );
  };

  const handleExportEvidence = () => {
    if (!exportEvidenceActionReadiness.available) {
      message.error(exportEvidenceActionReadiness.reason);
      return;
    }

    if (!evidenceGate.allowExport) {
      message.error(
        t(
          'Exportação de evidências bloqueada: complete as evidências obrigatórias primeiro.',
          'Evidence export blocked: complete the required evidence first.'
        )
      );
      return;
    }

    const payload = buildEvidenceExportPayload({
      evidenceBundle: immutableEvidenceBundle,
      evidenceRows,
      gate: evidenceGate,
    });
    const fileName = `evidencias-e1-${(changeId || 'change').replace(/\s+/g, '-')}-${String(
      snapshot.syncCount
    ).padStart(2, '0')}.json`;

    downloadEvidencePayloadFile(fileName, payload);
    message.success(t('Evidências exportadas ({fileName}).', 'Evidence exported ({fileName}).', { fileName }));
  };

  const handleResetFilters = () => {
    setFilters({
      organization: 'all',
      channel: 'all',
      environment: 'all',
    });
  };

  const sourceColumns = [
    {
      title: t('Fonte', 'Source'),
      dataIndex: 'label',
      key: 'label',
      width: 280,
    },
    {
      title: t('Status', 'Status'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => renderStatusTag(value, INVENTORY_COVERAGE_TONE_MAP),
    },
    {
      title: t('Cobertura', 'Coverage'),
      dataIndex: 'coveragePercent',
      key: 'coveragePercent',
      width: 120,
      render: value => `${value}%`,
    },
    {
      title: t('Domínios', 'Domains'),
      dataIndex: 'domains',
      key: 'domains',
      render: value => value.join(', '),
    },
    {
      title: t('Última sync UTC', 'Last sync UTC'),
      dataIndex: 'lastSyncedAt',
      key: 'lastSyncedAt',
      width: 170,
      render: value => formatInventoryUtcTimestamp(value),
    },
  ];

  const artifactColumns = [
    {
      title: t('Artefato', 'Artifact'),
      dataIndex: 'key',
      key: 'key',
      width: 180,
      render: value => <Typography.Text code>{sanitizeSensitiveText(value)}</Typography.Text>,
    },
    {
      title: t('Origem', 'Origin'),
      dataIndex: 'origin',
      key: 'origin',
      width: 110,
    },
    {
      title: t('Obrigatório', 'Required'),
      dataIndex: 'required',
      key: 'required',
      width: 120,
      render: value => (value ? t('sim', 'yes') : t('não', 'no')),
    },
    {
      title: t('Disponível', 'Available'),
      dataIndex: 'available',
      key: 'available',
      width: 120,
      render: value =>
        renderStatusTag(value ? 'ready' : 'pending', {
          ready: 'success',
          pending: 'error',
        }),
    },
    {
      title: t('Consistência', 'Consistency'),
      dataIndex: 'status',
      key: 'status',
      width: 120,
      render: value => renderStatusTag(value, INVENTORY_COVERAGE_TONE_MAP),
    },
    {
      title: t('Fonte oficial', 'Official source'),
      dataIndex: 'evidenceSource',
      key: 'evidenceSource',
      width: 210,
      render: value => (
        <Typography.Text code>{sanitizeSensitiveText(value) || '-'}</Typography.Text>
      ),
    },
    {
      title: t('Fingerprint', 'Fingerprint'),
      dataIndex: 'fingerprintSha256',
      key: 'fingerprintSha256',
      width: 180,
      render: value => <Typography.Text code>{truncateEvidenceFingerprint(value)}</Typography.Text>,
    },
    {
      title: t('Validação UTC', 'Validation UTC'),
      dataIndex: 'validatedAtUtc',
      key: 'validatedAtUtc',
      width: 170,
      render: value => formatInventoryUtcTimestamp(value),
    },
    {
      title: t('Diagnóstico', 'Diagnosis'),
      dataIndex: 'inconsistencyReasons',
      key: 'inconsistencyReasons',
      render: value =>
        Array.isArray(value) && value.length > 0
          ? sanitizeSensitiveText(value.join(' '))
          : t('sem inconsistências', 'no inconsistencies'),
    },
  ];

  const evidenceColumns = [
    {
      title: t('Evidência', 'Evidence'),
      dataIndex: 'key',
      key: 'key',
      width: 210,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Fonte', 'Source'),
      dataIndex: 'source',
      key: 'source',
      width: 110,
    },
    {
      title: t('Artefato', 'Artifact'),
      dataIndex: 'artifact',
      key: 'artifact',
      width: 260,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Fingerprint', 'Fingerprint'),
      dataIndex: 'fingerprint',
      key: 'fingerprint',
      width: 180,
      render: value => <Typography.Text code>{truncateEvidenceFingerprint(value)}</Typography.Text>,
    },
    {
      title: t('Status', 'Status'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => renderStatusTag(value, EVIDENCE_STATUS_TONE_MAP),
    },
    {
      title: t('UTC', 'UTC'),
      dataIndex: 'timestampUtc',
      key: 'timestampUtc',
      width: 170,
      render: value => formatEvidenceUtcTimestamp(value),
    },
    {
      title: t('Executor', 'Executor'),
      dataIndex: 'executor',
      key: 'executor',
      width: 170,
      render: value => <Typography.Text code>{value || '-'}</Typography.Text>,
    },
    {
      title: t('change_id', 'change_id'),
      dataIndex: 'changeId',
      key: 'changeId',
      width: 180,
      render: value => <Typography.Text code>{value || '-'}</Typography.Text>,
    },
    {
      title: t('run_id', 'run_id'),
      dataIndex: 'runId',
      key: 'runId',
      width: 180,
      render: value => <Typography.Text code>{value || '-'}</Typography.Text>,
    },
    {
      title: t('Detalhes', 'Details'),
      dataIndex: 'details',
      key: 'details',
      render: value => value || '-',
    },
  ];

  const organizationColumns = [
    {
      title: t('Organização', 'Organization'),
      dataIndex: 'orgId',
      key: 'orgId',
      width: 130,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Nome', 'Name'),
      dataIndex: 'displayName',
      key: 'displayName',
    },
    {
      title: t('MSP', 'MSP'),
      dataIndex: 'mspId',
      key: 'mspId',
      width: 140,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Ambiente', 'Environment'),
      dataIndex: 'environment',
      key: 'environment',
      width: 170,
    },
    {
      title: t('Estado', 'State'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => renderStatusTag(value, runtimeToneMap),
    },
  ];

  const channelColumns = [
    {
      title: t('Canal', 'Channel'),
      dataIndex: 'channelId',
      key: 'channelId',
      width: 170,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Domínio', 'Domain'),
      dataIndex: 'governanceDomain',
      key: 'governanceDomain',
      width: 140,
    },
    {
      title: t('Membros', 'Members'),
      dataIndex: 'members',
      key: 'members',
      render: value => value.join(', '),
    },
    {
      title: t('Ambiente', 'Environment'),
      dataIndex: 'environment',
      key: 'environment',
      width: 170,
    },
    {
      title: t('Estado', 'State'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => renderStatusTag(value, runtimeToneMap),
    },
  ];

  const hostColumns = [
    {
      title: t('Host', 'Host'),
      dataIndex: 'hostId',
      key: 'hostId',
      width: 140,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('IP', 'IP'),
      dataIndex: 'address',
      key: 'address',
      width: 130,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Organização', 'Organization'),
      dataIndex: 'orgId',
      key: 'orgId',
      width: 130,
    },
    {
      title: t('Ambiente', 'Environment'),
      dataIndex: 'environment',
      key: 'environment',
      width: 170,
    },
    {
      title: t('Provedor', 'Provider'),
      dataIndex: 'providerKey',
      key: 'providerKey',
      width: 130,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Estado', 'State'),
      dataIndex: 'runtimeStatus',
      key: 'runtimeStatus',
      width: 110,
      render: value => renderStatusTag(value, runtimeToneMap),
    },
  ];

  const nodeColumns = [
    {
      title: t('Nó', 'Node'),
      dataIndex: 'nodeId',
      key: 'nodeId',
      width: 160,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Tipo', 'Type'),
      dataIndex: 'nodeType',
      key: 'nodeType',
      width: 110,
    },
    {
      title: t('Organização', 'Organization'),
      dataIndex: 'orgId',
      key: 'orgId',
      width: 130,
    },
    {
      title: t('Host', 'Host'),
      dataIndex: 'hostId',
      key: 'hostId',
      width: 140,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Ambiente', 'Environment'),
      dataIndex: 'environment',
      key: 'environment',
      width: 170,
    },
    {
      title: t('Provedor', 'Provider'),
      dataIndex: 'providerKey',
      key: 'providerKey',
      width: 130,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Estado', 'State'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => renderStatusTag(value, runtimeToneMap),
    },
  ];

  const cryptoColumns = [
    {
      title: t('Artefato', 'Artifact'),
      dataIndex: 'artifactId',
      key: 'artifactId',
      width: 180,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Organização', 'Organization'),
      dataIndex: 'orgId',
      key: 'orgId',
      width: 130,
    },
    {
      title: t('Tipo', 'Type'),
      dataIndex: 'artifactType',
      key: 'artifactType',
      width: 150,
    },
    {
      title: t('Fingerprint', 'Fingerprint'),
      dataIndex: 'fingerprint',
      key: 'fingerprint',
      width: 170,
      render: value => <Typography.Text code>{truncateEvidenceFingerprint(value)}</Typography.Text>,
    },
    {
      title: t('Expiração UTC', 'Expiration UTC'),
      dataIndex: 'expiresAtUtc',
      key: 'expiresAtUtc',
      width: 170,
      render: value => formatInventoryUtcTimestamp(value),
    },
    {
      title: t('Estado', 'State'),
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => renderStatusTag(value, runtimeToneMap),
    },
  ];

  return (
    <NeoOpsLayout
      screenKey="e1-inventario"
      sectionLabel={PROVISIONING_SECTION_LABEL}
      title={screen.title}
      subtitle={screen.objective}
      navItems={provisioningNavItems}
      activeNavKey={resolveProvisioningActiveNavKey('e1-inventario')}
      breadcrumbs={getProvisioningBreadcrumbs('e1-inventario')}
      toolbar={
        <Space>
          <Tooltip title={buildReadinessTooltip(manualSyncActionReadiness)}>
            <span>
              <Button
                type="primary"
                icon={<SyncOutlined />}
                onClick={handleManualSync}
                loading={isSyncing}
                disabled={evidenceViewLocked || !manualSyncActionReadiness.available}
              >
                {t('Sincronizar manualmente', 'Synchronize manually')}
              </Button>
            </span>
          </Tooltip>
          <Tooltip title={buildReadinessTooltip(exportSnapshotActionReadiness)}>
            <span>
              <Button
                icon={<DownloadOutlined />}
                onClick={handleExportSnapshot}
                disabled={
                  !snapshot.syncedAt ||
                  !snapshot.consistency.isConsistent ||
                  !officialArtifactGate.allowExport ||
                  !exportSnapshotActionReadiness.available
                }
              >
                {t('Exportar snapshot', 'Export snapshot')}
              </Button>
            </span>
          </Tooltip>
          <Tooltip title={buildReadinessTooltip(closeExecutionActionReadiness)}>
            <span>
              <Button
                icon={<CheckCircleOutlined />}
                onClick={handleCloseExecution}
                disabled={
                  evidenceViewLocked ||
                  !closeExecutionAllowed ||
                  !closeExecutionActionReadiness.available
                }
              >
                {t('Fechar execução E1', 'Close E1 execution')}
              </Button>
            </span>
          </Tooltip>
          <Tooltip title={buildReadinessTooltip(exportEvidenceActionReadiness)}>
            <span>
              <Button
                icon={<LockOutlined />}
                onClick={handleExportEvidence}
                disabled={!evidenceGate.allowExport || !exportEvidenceActionReadiness.available}
              >
                {t('Exportar evidências', 'Export evidence')}
              </Button>
            </span>
          </Tooltip>
        </Space>
      }
    >
      <div className={styles.content}>
        <ProvisioningReadinessCard
          screenKey="e1-inventario"
          actionOrder={inventoryReadinessActionOrder}
        />
        <div className={styles.neoCard}>
          <Typography.Text className={styles.neoLabel}>
            {t('Progresso de inventário', 'Inventory progress')}
          </Typography.Text>
          <div className={styles.steps}>
            <div
              className={`${styles.step} ${
                snapshot.syncPhase === 'unsynced' ? styles.stepActive : ''
              }`}
            >
              {t('1. Definir correlação e escopo', '1. Define correlation and scope')}
            </div>
            <div
              className={`${styles.step} ${
                snapshot.syncPhase === 'partial' ? styles.stepActive : ''
              }`}
            >
              {t('2. Sincronização manual + cobertura parcial', '2. Manual synchronization + partial coverage')}
            </div>
            <div
              className={`${styles.step} ${snapshot.syncPhase === 'full' ? styles.stepActive : ''}`}
            >
              {t('3. Cobertura completa + exportação', '3. Full coverage + export')}
            </div>
          </div>
        </div>

        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Escopo, correlação e sincronização', 'Scope, correlation, and synchronization')}
            </Typography.Title>
            <div className={styles.formGrid2}>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>
                  {t('provider (escopo fixo)', 'provider (fixed scope)')}
                </Typography.Text>
                <Select value={INVENTORY_PROVIDER_SCOPE} options={providerOptions} disabled />
              </div>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>
                  {t('cobertura consolidada', 'consolidated coverage')}
                </Typography.Text>
                <Space>
                  {renderStatusTag(snapshot.coverageStatus, INVENTORY_COVERAGE_TONE_MAP)}
                  <Typography.Text>{`${snapshot.coveragePercent}%`}</Typography.Text>
                </Space>
              </div>
            </div>

            <div style={{ marginTop: 10 }} className={styles.formGrid2}>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>change_id</Typography.Text>
                <Input
                  value={changeId}
                  onChange={event => setChangeId(event.target.value)}
                  placeholder="cr-2026-02-16-003"
                  disabled={evidenceViewLocked}
                />
              </div>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>run_id</Typography.Text>
                <Input
                  value={runId}
                  onChange={event => setRunId(event.target.value)}
                  placeholder="run-..."
                  disabled={evidenceViewLocked}
                />
              </div>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>
                  {t('executor', 'executor')}
                </Typography.Text>
                <Input
                  value={executor}
                  onChange={event => setExecutor(event.target.value)}
                  placeholder="ops.cognus@ufg.br"
                  disabled={evidenceViewLocked}
                />
              </div>
            </div>

            <div style={{ marginTop: 12, display: 'grid', gap: 8 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>{t('última sync UTC', 'last sync UTC')}</Typography.Text>
                <Typography.Text>{formatInventoryUtcTimestamp(snapshot.syncedAt)}</Typography.Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>
                  {t('fase da sincronização', 'synchronization phase')}
                </Typography.Text>
                <Typography.Text code>{snapshot.syncPhase}</Typography.Text>
              </div>
              <Progress
                percent={snapshot.coveragePercent}
                status={resolveCoverageProgressStatus(snapshot.coverageStatus)}
              />
            </div>

            {snapshot.scopeEnforcement.excludedTotal > 0 && (
              <Alert
                showIcon
                type="warning"
                style={{ marginTop: 12 }}
                message={t('Registros fora do escopo removidos', 'Out-of-scope records removed')}
                description={t(
                  'Hosts removidos: {hosts}. Nós removidos: {nodes}. Somente external-linux é exibido na Entrega 1.',
                  'Removed hosts: {hosts}. Removed nodes: {nodes}. Only external-linux is shown in Delivery 1.',
                  {
                    hosts: snapshot.scopeEnforcement.excludedHosts,
                    nodes: snapshot.scopeEnforcement.excludedNodes,
                  }
                )}
              />
            )}
          </div>

          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Filtros operacionais', 'Operational filters')}
            </Typography.Title>
            <div className={styles.formGrid2}>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>{t('organização', 'organization')}</Typography.Text>
                <Select
                  value={filters.organization}
                  options={organizationSelectOptions}
                  disabled={evidenceViewLocked}
                  onChange={value =>
                    setFilters(currentFilters => ({
                      ...currentFilters,
                      organization: value,
                    }))
                  }
                />
              </div>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>{t('canal', 'channel')}</Typography.Text>
                <Select
                  value={filters.channel}
                  options={channelSelectOptions}
                  disabled={evidenceViewLocked}
                  onChange={value =>
                    setFilters(currentFilters => ({
                      ...currentFilters,
                      channel: value,
                    }))
                  }
                />
              </div>
              <div className={styles.formField}>
                <Typography.Text className={styles.formFieldLabel}>{t('ambiente', 'environment')}</Typography.Text>
                <Select
                  value={filters.environment}
                  options={environmentSelectOptions}
                  disabled={evidenceViewLocked}
                  onChange={value =>
                    setFilters(currentFilters => ({
                      ...currentFilters,
                      environment: value,
                    }))
                  }
                />
              </div>
              <div className={styles.footerActions}>
                <Button
                  icon={<ReloadOutlined />}
                  onClick={handleResetFilters}
                  disabled={evidenceViewLocked}
                >
                  {t('Limpar filtros', 'Clear filters')}
                </Button>
              </div>
            </div>

            <div style={{ marginTop: 14 }} className={styles.chipRow}>
              <span className={styles.chip}>{t('organizações: {count}', 'organizations: {count}', { count: filteredSummary.organizations })}</span>
              <span className={styles.chip}>{t('canais: {count}', 'channels: {count}', { count: filteredSummary.channels })}</span>
              <span className={styles.chip}>{t('hosts: {count}', 'hosts: {count}', { count: filteredSummary.hosts })}</span>
              <span className={styles.chip}>{t('nós: {count}', 'nodes: {count}', { count: filteredSummary.nodes })}</span>
              <span className={styles.chip}>
                {t('baseline criptográfico: {count}', 'cryptographic baseline: {count}', {
                  count: filteredSummary.cryptoBaseline,
                })}
              </span>
            </div>
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Cobertura das fontes de dados', 'Data source coverage')}
          </Typography.Title>
          {snapshot.coverageStatus !== INVENTORY_SOURCE_STATUS.ready && (
            <Alert
              showIcon
              type="warning"
              style={{ marginBottom: 10 }}
              message={t('Cobertura parcial detectada', 'Partial coverage detected')}
              description={t(
                'A tabela exibe explicitamente o grau de cobertura por fonte. Sem cobertura integral, o snapshot permanece bloqueado para exportação.',
                'The table explicitly shows the coverage level per source. Without full coverage, the snapshot remains blocked for export.'
              )}
            />
          )}
          <Table
            rowKey="key"
            columns={sourceColumns}
            dataSource={snapshot.sourceRows}
            pagination={false}
            size="small"
            scroll={{ x: 1200 }}
          />
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Artefatos oficiais A2 e consistência auditável', 'Official A2 artifacts and auditable consistency')}
          </Typography.Title>
          <div style={{ marginBottom: 10, display: 'flex', justifyContent: 'space-between' }}>
            <Typography.Text className={styles.neoLabel}>
              {t('status oficial de cobertura', 'official coverage status')}
            </Typography.Text>
            <Space>
              {renderStatusTag(officialArtifactGate.coverageStatus, INVENTORY_COVERAGE_TONE_MAP)}
              <Typography.Text>{`${officialArtifactGate.coveragePercent}%`}</Typography.Text>
            </Space>
          </div>
          {officialArtifactGate.correlationIssues.length > 0 && (
            <Alert
              showIcon
              type="error"
              style={{ marginBottom: 10 }}
              message={t('Correlação oficial incompleta/divergente', 'Official correlation incomplete/divergent')}
              description={sanitizeSensitiveText(officialArtifactGate.correlationIssues.join(' '))}
            />
          )}
          {officialArtifactGate.coverageStatus !== INVENTORY_SOURCE_STATUS.ready && (
            <Alert
              showIcon
              type="warning"
              style={{ marginBottom: 10 }}
              message={t(
                'Cobertura oficial parcial dos artefatos obrigatórios',
                'Partial official coverage of required artifacts'
              )}
              description={t(
                'Ausentes: {missing}. Inconsistentes: {inconsistent}. Exportação/fechamento permanecem bloqueados até convergência oficial.',
                'Missing: {missing}. Inconsistent: {inconsistent}. Export/closure remain blocked until official convergence.',
                {
                  missing: officialArtifactGate.missingRequiredArtifacts.length,
                  inconsistent: officialArtifactGate.inconsistentArtifacts.length,
                }
              )}
            />
          )}
          {officialArtifactGate.gateIssues.length > 0 && (
            <Alert
              showIcon
              type="error"
              style={{ marginBottom: 10 }}
              message={t('Gate auditável A2 bloqueado', 'Auditable A2 gate blocked')}
              description={sanitizeSensitiveText(officialArtifactGate.gateIssues.join(' '))}
            />
          )}
          <Table
            rowKey="key"
            columns={artifactColumns}
            dataSource={officialArtifactGate.artifactRows}
            pagination={false}
            size="small"
            scroll={{ x: 1700 }}
          />
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Painel de evidências técnicas por execução', 'Technical evidence panel by execution')}
          </Typography.Title>
          <div style={{ marginBottom: 10, display: 'grid', gap: 8 }}>
            {officialRunEvidence.loading && (
              <Alert
                showIcon
                type="info"
                message={t('Atualizando trilha oficial de execução', 'Updating official execution trail')}
                description={t(
                  'Consultando `/api/v1/runbooks/{run_id}/status` para refletir evidência oficial no painel.',
                  'Querying `/api/v1/runbooks/{run_id}/status` to reflect official evidence in the panel.'
                )}
              />
            )}
            {!officialRunEvidence.loading && officialRunEvidence.error && (
              <Alert
                showIcon
                type="error"
                message={t('Evidência oficial indisponível', 'Official evidence unavailable')}
                description={officialRunEvidence.error}
              />
            )}
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>{t('status do fechamento', 'closure status')}</Typography.Text>
              {renderStatusTag(
                closeExecutionAllowed ? 'ready' : 'missing',
                EVIDENCE_STATUS_TONE_MAP
              )}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('evidências obrigatórias ausentes', 'missing required evidence')}
              </Typography.Text>
              <Typography.Text code>
                {evidenceGate.missingRequiredKeys.length +
                  officialArtifactGate.missingRequiredArtifacts.length +
                  officialArtifactGate.inconsistentArtifacts.length}
              </Typography.Text>
            </div>
            {evidenceViewLocked && (
              <Alert
                showIcon
                type="info"
                message={t('Visualização pós-execução imutável', 'Immutable post-execution view')}
                description={t(
                  'Execução encerrada em {lockedAt}. Dados de evidência estão bloqueados para edição.',
                  'Execution closed at {lockedAt}. Evidence data is locked for editing.',
                  {
                    lockedAt: formatEvidenceUtcTimestamp(immutableEvidenceBundle.lockedAtUtc),
                  }
                )}
              />
            )}
            {!closeExecutionAllowed && (
              <Alert
                showIcon
                type="error"
                message={t(
                  'Fechamento bloqueado por ausência de evidências obrigatórias',
                  'Closure blocked due to missing required evidence'
                )}
                description={sanitizeSensitiveText(
                  [...evidenceGate.gateIssues, ...officialArtifactGate.gateIssues].join(' ')
                )}
              />
            )}
          </div>
          <Table
            rowKey="key"
            columns={evidenceColumns}
            dataSource={evidenceRows}
            pagination={false}
            size="small"
            scroll={{ x: 1950 }}
          />
        </div>

        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Organizações', 'Organizations')}
            </Typography.Title>
            <Table
              rowKey="orgId"
              columns={organizationColumns}
              dataSource={filteredInventory.organizations}
              pagination={false}
              size="small"
              scroll={{ x: 900 }}
            />
          </div>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Canais', 'Channels')}
            </Typography.Title>
            <Table
              rowKey="channelId"
              columns={channelColumns}
              dataSource={filteredInventory.channels}
              pagination={false}
              size="small"
              scroll={{ x: 900 }}
            />
          </div>
        </div>

        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Hosts', 'Hosts')}
            </Typography.Title>
            <Table
              rowKey="hostId"
              columns={hostColumns}
              dataSource={filteredInventory.hosts}
              pagination={false}
              size="small"
              scroll={{ x: 1000 }}
            />
          </div>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Nós', 'Nodes')}
            </Typography.Title>
            <Table
              rowKey="nodeId"
              columns={nodeColumns}
              dataSource={filteredInventory.nodes}
              pagination={false}
              size="small"
              scroll={{ x: 1100 }}
            />
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Baseline criptográfico inicial', 'Initial cryptographic baseline')}
          </Typography.Title>
          {filteredInventory.cryptoBaseline.length === 0 ? (
            <Alert
              showIcon
              type="warning"
              message={t('Baseline criptográfico indisponível', 'Cryptographic baseline unavailable')}
              description={t(
                'Sincronize novamente até obter `inventory-crypto.json` para liberar consistência e exportação.',
                'Synchronize again until `inventory-crypto.json` is available to unlock consistency and export.'
              )}
            />
          ) : (
            <Table
              rowKey="artifactId"
              columns={cryptoColumns}
              dataSource={filteredInventory.cryptoBaseline}
              pagination={false}
              size="small"
              scroll={{ x: 1050 }}
            />
          )}
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Resumo operacional do recorte atual', 'Operational summary of the current slice')}
          </Typography.Title>
          <div style={{ display: 'grid', gap: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>{t('organizações', 'organizations')}</Typography.Text>
              <Badge count={filteredSummary.organizations} style={{ background: '#1677ff' }} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>{t('canais', 'channels')}</Typography.Text>
              <Badge count={filteredSummary.channels} style={{ background: '#1677ff' }} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>{t('hosts', 'hosts')}</Typography.Text>
              <Badge count={filteredSummary.hosts} style={{ background: '#1677ff' }} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>{t('nós', 'nodes')}</Typography.Text>
              <Badge count={filteredSummary.nodes} style={{ background: '#1677ff' }} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('baseline criptográfico', 'cryptographic baseline')}
              </Typography.Text>
              <Badge count={filteredSummary.cryptoBaseline} style={{ background: '#1677ff' }} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>change_id</Typography.Text>
              <Typography.Text code>{changeId || '-'}</Typography.Text>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Typography.Text className={styles.neoLabel}>run_id</Typography.Text>
              <Typography.Text code>{runId || '-'}</Typography.Text>
            </div>
          </div>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default ProvisioningInventoryPage;
