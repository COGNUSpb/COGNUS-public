import { useCallback, useEffect, useMemo, useState } from 'react';
import { message } from 'antd';
import { getRunbookRuntimeInspection } from '../../../services/runbook';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const normalizeText = value => String(value || '').trim();
const localizeInspectionText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const resolveInspectionBackendCode = error =>
  normalizeText(
    error &&
      ((error.data &&
        (error.data.code ||
          error.data.msg ||
          error.data.message ||
          (error.data.details && error.data.details.code))) ||
        (error.response &&
          error.response.data &&
          (error.response.data.code ||
            error.response.data.msg ||
            error.response.data.message ||
            (error.response.data.data &&
              (error.response.data.data.code || error.response.data.data.message)))))
  ).toLowerCase();

const isRunbookNotFoundInspectionError = error => {
  const backendCode = resolveInspectionBackendCode(error);
  if (backendCode === 'runbook_not_found') {
    return true;
  }

  const detailText = normalizeText(
    error &&
      ((error.data && (error.data.detail || error.data.message || error.data.msg)) ||
        error.message ||
        (error.response &&
          error.response.data &&
          (error.response.data.message ||
            error.response.data.msg ||
            (error.response.data.data && error.response.data.data.message))))
  ).toLowerCase();

  return detailText.includes('runbook nao encontrado');
};

const isMachineCredentialInspectionError = error => {
  const backendCode = resolveInspectionBackendCode(error);
  return (
    backendCode === 'runbook_machine_credentials_required' ||
    backendCode === 'runbook_machine_credentials_missing_binding'
  );
};

const buildLocalInspectionPayload = ({
  row = null,
  runId = '',
  changeId = '',
  fallbackMeta = null,
  localeCandidate = null,
} = {}) => {
  const safeRow = row || {};
  const componentId =
    normalizeText(safeRow.componentId) ||
    normalizeText(safeRow.nodeId) ||
    normalizeText(safeRow.componentName) ||
    'component';
  const fallbackSource = normalizeText(fallbackMeta && fallbackMeta.source) || 'snapshot_local';
  const fallbackReason =
    normalizeText(fallbackMeta && fallbackMeta.reasonMessage) ||
    localizeInspectionText(
      'Detalhe montado a partir do snapshot oficial em cache local.',
      'Detail built from the official snapshot cached locally.',
      localeCandidate
    );
  const cacheBlock = {
    stale: true,
    cache_hit: true,
    collection_status: 'snapshot_local',
    inspection_source: fallbackSource,
    refresh_requested_at: '',
    refreshed_at: normalizeText(fallbackMeta && fallbackMeta.savedAtUtc),
    cache_key: `snapshot-local:${componentId}`,
    payload_hash: '',
  };

  return {
    stale: true,
    inspection_scope: 'snapshot_local',
    component: {
      org_id: normalizeText(safeRow.organizationId),
      host_id: normalizeText(safeRow.hostRef),
      component_id: componentId,
      component_type: normalizeText(safeRow.componentType),
      container_name: normalizeText(safeRow.componentName),
      image: '',
      platform: 'snapshot_local',
      status: normalizeText(safeRow.status) || 'unknown',
    },
    correlation: {
      run_id: normalizeText(runId),
      change_id: normalizeText(changeId),
    },
    issues: [
      {
        code: 'snapshot_local_only',
        message: fallbackReason,
      },
    ],
    scopes: {
      docker_inspect: {
        cache: cacheBlock,
        payload: {
          container_name: normalizeText(safeRow.componentName),
          image: '',
          platform: 'snapshot_local',
          state: {
            status: normalizeText(safeRow.status) || 'unknown',
            running: normalizeText(safeRow.status) === 'running',
            started_at: normalizeText(safeRow.evidenceTimestampUtc),
            finished_at: '',
            restart_count: 0,
            exit_code: '',
          },
          health: {
            status: normalizeText(safeRow.status) || 'unknown',
            failing_streak: 0,
            logs: [fallbackReason],
          },
          labels: {
            organization_id: normalizeText(safeRow.organizationId),
            component_type: normalizeText(safeRow.componentType),
            status_source: normalizeText(safeRow.statusSource),
            host_ref: normalizeText(safeRow.hostRef),
          },
          env: [],
          ports: [],
          mounts: [],
        },
      },
      docker_logs: {
        cache: cacheBlock,
        payload: {
          logs: [
            '# snapshot local',
            localizeInspectionText(
              '# refresh remoto indisponivel para este run_id',
              '# remote refresh unavailable for this run_id',
              localeCandidate
            ),
            `component_id=${componentId}`,
            `component_name=${normalizeText(safeRow.componentName) || '-'}`,
            `status=${normalizeText(safeRow.status) || 'unknown'}`,
            `status_source=${normalizeText(safeRow.statusSource) || 'snapshot_local'}`,
          ].join('\n'),
        },
      },
      environment: {
        cache: cacheBlock,
        payload: {
          env: [
            `ORG_ID=${normalizeText(safeRow.organizationId) || '-'}`,
            `HOST_REF=${normalizeText(safeRow.hostRef) || '-'}`,
            `COMPONENT_TYPE=${normalizeText(safeRow.componentType) || '-'}`,
            `CHANNEL_ID=${normalizeText(safeRow.channelId) || '-'}`,
            `CHAINCODE_ID=${normalizeText(safeRow.chaincodeId) || '-'}`,
          ],
        },
      },
      ports: {
        cache: cacheBlock,
        payload: {
          ports: [],
        },
      },
      mounts: {
        cache: cacheBlock,
        payload: {
          mounts: [],
        },
      },
    },
  };
};

const isLocalSnapshotPayload = payload => {
  if (!payload) {
    return false;
  }

  const inspectionScope = normalizeText(payload.inspection_scope).toLowerCase();
  const dockerInspectCache = payload.scopes && payload.scopes.docker_inspect && payload.scopes.docker_inspect.cache;
  const collectionStatus = normalizeText(dockerInspectCache && dockerInspectCache.collection_status).toLowerCase();

  return inspectionScope === 'snapshot_local' || collectionStatus === 'snapshot_local';
};

const isCachedInspectionFallbackPayload = payload =>
  Boolean(
    payload &&
      payload.cached_status_fallback &&
      payload.cached_status_fallback.active
  );

const useOfficialRuntimeInspection = ({ runId = '', topologyRows = [] } = {}) => {
  const normalizedRunId = normalizeText(runId);
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS) => localizeInspectionText(ptBR, enUS, locale),
    [locale]
  );
  const [inspectionState, setInspectionState] = useState({
    open: false,
    loading: false,
    error: '',
    row: null,
    payload: null,
  });

  useEffect(() => {
    setInspectionState({
      open: false,
      loading: false,
      error: '',
      row: null,
      payload: null,
    });
  }, [normalizedRunId]);

  const loadRuntimeInspection = useCallback((row, refresh = false) => {
    const safeRow = row || inspectionState.row;
    if (!safeRow || !safeRow.componentId) {
      message.error(
        t(
          'component_id oficial obrigatorio para consulta de inspecao runtime.',
          'official component_id is required for runtime inspection query.'
        )
      );
      return;
    }
    if (!normalizedRunId) {
      message.error(
        t(
          'run_id oficial obrigatorio para consulta de inspecao runtime.',
          'official run_id is required for runtime inspection query.'
        )
      );
      return;
    }

    setInspectionState(currentState => ({
      ...currentState,
      open: true,
      loading: true,
      error: '',
      row: safeRow,
    }));

    getRunbookRuntimeInspection(normalizedRunId, {
      orgId: safeRow.organizationId,
      hostId: safeRow.hostRef,
      componentId: safeRow.componentId,
      inspectionScope: 'all',
      refresh,
    })
      .then(payload => {
        setInspectionState(currentState => ({
          ...currentState,
          open: true,
          loading: false,
          error: '',
          row: safeRow,
          payload,
        }));
      })
      .catch(error => {
        if (isRunbookNotFoundInspectionError(error)) {
          setInspectionState(currentState => ({
            ...currentState,
            open: true,
            loading: false,
            error: '',
            row: safeRow,
            payload: buildLocalInspectionPayload({
              row: safeRow,
              runId: normalizedRunId,
              changeId:
                normalizeText(
                  currentState &&
                    currentState.payload &&
                    currentState.payload.correlation &&
                    currentState.payload.correlation.change_id
                ) || normalizeText(safeRow.changeId),
              fallbackMeta: {
                source: 'official_status_cache',
                savedAtUtc: new Date().toISOString(),
                reasonMessage: t(
                  'Runbook oficial expirado para este run_id. Mantendo snapshot local auditavel do componente.',
                  'Official runbook expired for this run_id. Keeping the component local auditable snapshot.'
                ),
              },
              localeCandidate: locale,
            }),
          }));
          return;
        }

        if (isMachineCredentialInspectionError(error)) {
          setInspectionState(currentState => ({
            ...currentState,
            open: true,
            loading: false,
            error: '',
            row: safeRow,
            payload: buildLocalInspectionPayload({
              row: safeRow,
              runId: normalizedRunId,
              changeId:
                normalizeText(
                  currentState &&
                    currentState.payload &&
                    currentState.payload.correlation &&
                    currentState.payload.correlation.change_id
                ) || normalizeText(safeRow.changeId),
              fallbackMeta: {
                source: 'official_status_cache',
                savedAtUtc: new Date().toISOString(),
                reasonMessage: t(
                  'Run oficial sem credenciais SSH persistidas por maquina. Mantendo snapshot auditavel do componente ate novo run com vinculo deterministico.',
                  'Official run without persisted SSH credentials per machine. Keeping the component auditable snapshot until a new run with deterministic binding.'
                ),
              },
              localeCandidate: locale,
            }),
          }));
          message.info(
            t(
              'Inspecao remota indisponivel: este run nao preserva credenciais SSH por maquina. Exibindo snapshot auditavel.',
              'Remote inspection unavailable: this run does not preserve SSH credentials per machine. Showing an auditable snapshot.'
            )
          );
          return;
        }

        setInspectionState(currentState => ({
          ...currentState,
          open: true,
          loading: false,
          error:
            (error && error.message) ||
            t(
              'Falha ao consultar inspecao runtime oficial para o componente selecionado.',
              'Failed to query the official runtime inspection for the selected component.'
            ),
          row: safeRow,
        }));
      });
  }, [inspectionState.row, locale, normalizedRunId, t]);

  const openInspection = useCallback(row => {
    loadRuntimeInspection(row, false);
  }, [loadRuntimeInspection]);

  const openInspectionByComponentId = useCallback(componentId => {
    const normalizedComponentId = normalizeText(componentId);
    if (!normalizedComponentId) {
      return;
    }

    const matchedRow = (Array.isArray(topologyRows) ? topologyRows : []).find(
      row => normalizeText(row && row.componentId) === normalizedComponentId
    );
    if (!matchedRow) {
      message.warning(
        t(
          'Componente oficial nao encontrado na topologia runtime atual.',
          'Official component not found in the current runtime topology.'
        )
      );
      return;
    }
    openInspection(matchedRow);
  }, [openInspection, t, topologyRows]);

  const openInspectionByDetailRef = useCallback(detailRef => {
    const safeDetailRef = detailRef || {};
    openInspectionByComponentId(safeDetailRef.componentId);
  }, [openInspectionByComponentId]);

  const openInspectionFromSnapshot = useCallback(
    (row, { fallbackMeta = null, correlation = {} } = {}) => {
      const safeRow = row || null;
      if (!safeRow) {
        return;
      }

      setInspectionState({
        open: true,
        loading: false,
        error: '',
        row: safeRow,
        payload: buildLocalInspectionPayload({
          row: safeRow,
          runId: correlation.runId || normalizedRunId,
          changeId: correlation.changeId,
          fallbackMeta,
          localeCandidate: locale,
        }),
      });
    },
    [locale, normalizedRunId]
  );

  const refreshInspection = useCallback(() => {
    if (!inspectionState.row) {
      return;
    }

    if (isLocalSnapshotPayload(inspectionState.payload)) {
      message.info(
        t(
          'Refresh remoto indisponivel para este run_id. Mantendo snapshot local sintetico.',
          'Remote refresh unavailable for this run_id. Keeping the local synthesized snapshot.'
        )
      );
      return;
    }

    if (isCachedInspectionFallbackPayload(inspectionState.payload)) {
      message.info(
        t(
          'Refresh remoto indisponivel para este run_id. Exibindo a ultima inspecao oficial cacheada localmente.',
          'Remote refresh unavailable for this run_id. Showing the latest official inspection cached locally.'
        )
      );
      return;
    }

    loadRuntimeInspection(inspectionState.row, true);
  }, [inspectionState.payload, inspectionState.row, loadRuntimeInspection, t]);

  const closeInspection = useCallback(() => {
    setInspectionState(currentState => ({
      ...currentState,
      open: false,
      loading: false,
      error: '',
      row: null,
      payload: null,
    }));
  }, []);

  const inspectionScopeEntries = useMemo(
    () => Object.entries((inspectionState.payload && inspectionState.payload.scopes) || {}),
    [inspectionState.payload]
  );

  return {
    inspectionState,
    inspectionScopeEntries,
    loadRuntimeInspection,
    openInspection,
    openInspectionByComponentId,
    openInspectionByDetailRef,
    openInspectionFromSnapshot,
    refreshInspection,
    closeInspection,
  };
};

export default useOfficialRuntimeInspection;
