import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Badge,
  Button,
  Input,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
  Upload,
  message,
} from 'antd';
import {
  CheckCircleOutlined,
  DownloadOutlined,
  FileSearchOutlined,
  PlusOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import lintBlueprintBackend, {
  listBlueprintVersions,
  publishBlueprintVersion,
} from '@/services/blueprint';
import NeoOpsLayout from '../components/NeoOpsLayout';
import styles from '../components/NeoOpsLayout.less';
import { screenByKey } from '../data/screens';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import ProvisioningReadinessCard from './ProvisioningReadinessCard';
import {
  formatCognusTemplate,
  resolveCognusLocale,
} from '../cognusI18n';
import { getProvisioningActionReadiness } from './provisioningBackendReadiness';
import {
  BLUEPRINT_RUNTIME_SCHEMA_VERSION,
  BLUEPRINT_SCOPE_CONSTRAINTS,
  buildBlockingLintReport,
  buildLintArtifacts,
  buildLintDiagnosticDomains,
  calculateCanonicalFingerprint,
  createBlueprintDraftTemplate,
  inferBlueprintDocumentFormat,
  lintBlueprintDraft,
  normalizeBackendLintReport,
  parseBlueprintDocument,
  serializeBlueprintDocument,
  summarizeBlueprintVersionDiff,
  truncateFingerprint,
} from './provisioningBlueprintUtils';

const { TextArea } = Input;

const screen = screenByKey['e1-blueprint'];

const actionButtonStyle = {
  borderColor: 'rgba(116, 208, 255, 0.65)',
  color: '#d9edff',
  background: 'rgba(11, 24, 48, 0.7)',
};

const contentFormatOptions = [
  { value: 'yaml', label: 'YAML' },
  { value: 'json', label: 'JSON' },
];

const blueprintReadinessActionOrder = [
  'import_blueprint',
  'run_lint',
  'publish_version',
  'create_draft',
];

const parseErrorToReport = error => ({
  valid: false,
  schemaVersion: '',
  resolvedSchemaVersion: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
  blueprintVersion: '',
  createdAt: '',
  updatedAt: '',
  fingerprint: '',
  summary: {
    orgs: 0,
    channels: 0,
    nodes: 0,
    policies: 0,
  },
  errors: [
    {
      level: 'error',
      code: 'invalid_blueprint_document',
      path: '$',
      message: error.message,
    },
  ],
  warnings: [],
  hints: [],
  lintSource: 'parse',
  contractValid: false,
  normalizedBlueprint: {},
  issues: [
    {
      level: 'error',
      code: 'invalid_blueprint_document',
      path: '$',
      message: error.message,
    },
  ],
});

const getIssueColorByLevel = level => {
  if (level === 'error') {
    return 'red';
  }

  if (level === 'warning') {
    return 'orange';
  }

  return 'blue';
};

const formatTimestamp = value => {
  if (!value) {
    return '-';
  }

  return value.replace('T', ' ').replace('Z', ' UTC');
};

const downloadJsonFile = (fileName, payload) => {
  if (!payload || typeof window === 'undefined' || typeof document === 'undefined') {
    return;
  }

  const fileContent = JSON.stringify(payload, null, 2);
  const blob = new Blob([fileContent], { type: 'application/json;charset=utf-8' });
  const downloadUrl = window.URL.createObjectURL(blob);
  const anchor = document.createElement('a');

  anchor.href = downloadUrl;
  anchor.download = fileName;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  window.URL.revokeObjectURL(downloadUrl);
};

const localizeBlueprintPageText = (ptBR, enUS, localeCandidate, values) =>
  formatCognusTemplate(ptBR, enUS, values, localeCandidate || resolveCognusLocale());

const resolveRequestErrorMessage = error => {
  if (!error) {
    return localizeBlueprintPageText(
      'falha de conectividade.',
      'connectivity failure.'
    );
  }

  const backendMessage =
    (error.data && (error.data.detail || error.data.msg || error.data.message)) || '';
  const fallbackMessage = typeof error.message === 'string' ? error.message : '';
  return (
    backendMessage ||
    fallbackMessage ||
    localizeBlueprintPageText('falha de conectividade.', 'connectivity failure.')
  );
};

const mapBackendVersionRecord = record => {
  const safeRecord = record && typeof record === 'object' ? record : {};
  const normalizedBlueprint =
    safeRecord.normalized_blueprint && typeof safeRecord.normalized_blueprint === 'object'
      ? safeRecord.normalized_blueprint
      : {};
  const normalizedIssues = Array.isArray(safeRecord.issues) ? safeRecord.issues : [];
  const normalizedWarnings = Array.isArray(safeRecord.warnings)
    ? safeRecord.warnings
    : normalizedIssues.filter(issue => issue.level === 'warning');
  const normalizedHints = Array.isArray(safeRecord.hints)
    ? safeRecord.hints
    : normalizedIssues.filter(issue => issue.level === 'hint');

  return {
    id: String(safeRecord.id || ''),
    blueprintVersion: String(safeRecord.blueprint_version || '-'),
    schemaVersion: String(safeRecord.schema_version || '-'),
    resolvedSchemaVersion: String(safeRecord.resolved_schema_version || '-'),
    fingerprint: String(safeRecord.fingerprint_sha256 || ''),
    changeId: String(safeRecord.change_id || ''),
    executionContext: String(safeRecord.execution_context || ''),
    publishedAt: String(safeRecord.published_at_utc || ''),
    lintGeneratedAtUtc: String(safeRecord.lint_generated_at_utc || ''),
    issues: normalizedIssues,
    warnings: normalizedWarnings,
    hints: normalizedHints,
    lintSource: 'backend',
    contractValid: Boolean(safeRecord.valid),
    normalizedBlueprint,
    blueprint: normalizedBlueprint,
  };
};

const ProvisioningBlueprintPage = () => {
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS, values) => localizeBlueprintPageText(ptBR, enUS, locale, values),
    [locale]
  );
  const initialBlueprint = useMemo(() => createBlueprintDraftTemplate(), []);

  const [editorFormat, setEditorFormat] = useState('yaml');
  const [editorValue, setEditorValue] = useState(() =>
    serializeBlueprintDocument(initialBlueprint, 'yaml')
  );
  const [changeId, setChangeId] = useState('cr-2026-02-16-001');
  const [executionContext, setExecutionContext] = useState(
    t(
      'Provisionamento inicial do consórcio DEV para pipeline prepare -> provision -> reconcile -> verify.',
      'Initial DEV consortium provisioning for the prepare -> provision -> reconcile -> verify pipeline.'
    )
  );
  const [lintReport, setLintReport] = useState(null);
  const [versionHistory, setVersionHistory] = useState([]);
  const [baseVersionId, setBaseVersionId] = useState(undefined);
  const [targetVersionId, setTargetVersionId] = useState(undefined);
  const [isLinting, setIsLinting] = useState(false);
  const [isPublishing, setIsPublishing] = useState(false);
  const importActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-blueprint', 'import_blueprint'),
    []
  );
  const lintActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-blueprint', 'run_lint'),
    []
  );
  const publishActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-blueprint', 'publish_version'),
    []
  );
  const draftActionReadiness = useMemo(
    () => getProvisioningActionReadiness('e1-blueprint', 'create_draft'),
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

  const loadVersionHistory = useCallback(async ({ silent = false } = {}) => {
    try {
      const response = await listBlueprintVersions();
      const rows =
        response &&
        response.status === 'successful' &&
        response.data &&
        Array.isArray(response.data.data)
          ? response.data.data
          : null;

      if (!rows) {
        throw new Error(
          t(
            'Resposta de historico fora do contrato esperado.',
            'History response outside the expected contract.'
          )
        );
      }

      const mappedRows = rows.map(mapBackendVersionRecord);
      setVersionHistory(mappedRows);

      setBaseVersionId(currentValue =>
        currentValue && mappedRows.some(version => version.id === currentValue)
          ? currentValue
          : undefined
      );
      setTargetVersionId(currentValue => {
        if (currentValue && mappedRows.some(version => version.id === currentValue)) {
          return currentValue;
        }
        return mappedRows.length > 0 ? mappedRows[0].id : undefined;
      });

      if (!silent) {
        message.success(
          t(
            'Historico de versoes sincronizado com backend oficial.',
            'Version history synchronized with the official backend.'
          )
        );
      }
    } catch (error) {
      if (!silent) {
        message.error(
          t(
            'Falha ao sincronizar historico de versoes: {message}',
            'Failed to synchronize version history: {message}',
            {
              message: resolveRequestErrorMessage(error),
            }
          )
        );
      }
    }
  }, [t]);

  useEffect(() => {
    loadVersionHistory({ silent: true });
  }, [loadVersionHistory]);

  const selectedBaseVersion = useMemo(
    () => versionHistory.find(version => version.id === baseVersionId),
    [versionHistory, baseVersionId]
  );

  const selectedTargetVersion = useMemo(
    () => versionHistory.find(version => version.id === targetVersionId),
    [versionHistory, targetVersionId]
  );

  const diffRows = useMemo(() => {
    if (!selectedBaseVersion || !selectedTargetVersion) {
      return [];
    }

    return summarizeBlueprintVersionDiff(
      selectedBaseVersion.blueprint,
      selectedTargetVersion.blueprint
    );
  }, [selectedBaseVersion, selectedTargetVersion]);

  const versionOptions = useMemo(
    () =>
      versionHistory.map(version => ({
        label: `${version.blueprintVersion} (${version.changeId})`,
        value: version.id,
      })),
    [versionHistory]
  );

  const runLint = async ({
    payload,
    format,
    showSuccessMessage = true,
    showErrorMessage = true,
  } = {}) => {
    const activePayload = payload !== undefined ? payload : editorValue;
    const activeFormat = format || editorFormat;

    let parsedBlueprint;
    try {
      parsedBlueprint = parseBlueprintDocument(activePayload, activeFormat);
    } catch (error) {
      const report = parseErrorToReport(error);
      setLintReport(report);
      if (showErrorMessage) {
        message.error(
          t(
            'Falha ao interpretar blueprint. Corrija o documento e tente novamente.',
            'Failed to parse blueprint. Fix the document and try again.'
          )
        );
      }
      return {
        valid: false,
        report,
        blueprint: null,
      };
    }

    const localReportWithoutFingerprint = lintBlueprintDraft(parsedBlueprint, {
      runtimeSchemaVersion: BLUEPRINT_RUNTIME_SCHEMA_VERSION,
      scopeConstraints: BLUEPRINT_SCOPE_CONSTRAINTS,
    });

    const localFingerprint = await calculateCanonicalFingerprint(parsedBlueprint);
    const localReport = {
      ...localReportWithoutFingerprint,
      fingerprint: localFingerprint,
      lintSource: 'local',
      contractValid: true,
      normalizedBlueprint: parsedBlueprint,
    };

    let report = localReport;
    try {
      const backendResponse = await lintBlueprintBackend({
        blueprint: parsedBlueprint,
        allow_migration: false,
        change_id: changeId.trim(),
        execution_context: executionContext.trim(),
      });

      const isSuccessEnvelope =
        backendResponse && backendResponse.status === 'successful' && backendResponse.data;

      if (!isSuccessEnvelope) {
        const backendMessage =
          (backendResponse && backendResponse.msg && String(backendResponse.msg)) ||
          'Resposta sem envelope data/status esperado.';
        report = buildBlockingLintReport(localReport, {
          code: 'lint_backend_invalid_response',
          path: '$backend',
          message: t(
            'Backend de lint respondeu fora do contrato esperado: {message}',
            'Lint backend responded outside the expected contract: {message}',
            {
              message: backendMessage,
            }
          ),
        });
      } else {
        const normalizedBackend = normalizeBackendLintReport(backendResponse.data);

        if (!normalizedBackend.contractValid || !normalizedBackend.report) {
          report = buildBlockingLintReport(localReport, {
            code: 'lint_contract_invalid',
            path: '$contract',
            message: normalizedBackend.contractViolations.join(' '),
          });
        } else {
          report = {
            ...normalizedBackend.report,
            lintSource: 'backend',
            contractValid: true,
          };
        }
      }
    } catch (error) {
      const backendError = resolveRequestErrorMessage(error);
      report = buildBlockingLintReport(localReport, {
        code: 'lint_backend_unavailable',
        path: '$backend',
        message: t(
          'Backend de lint indisponível: {message}',
          'Lint backend unavailable: {message}',
          {
            message: backendError,
          }
        ),
      });
    }

    setLintReport(report);

    const lintPublishReady =
      report.valid && report.contractValid && report.lintSource === 'backend';

    if (showSuccessMessage) {
      if (lintPublishReady) {
        message.success(
          t(
            'Lint executado via backend A1.2 com sucesso. Blueprint apto para publicação.',
            'Lint executed through backend A1.2 successfully. Blueprint ready for publication.'
          )
        );
      } else {
        message.error(
          t(
            'Lint bloqueante: {count} erro(s). Corrija antes de publicar.',
            'Blocking lint: {count} error(s). Fix them before publishing.',
            {
              count: report.errors.length,
            }
          )
        );
      }
    }

    return {
      valid: lintPublishReady,
      report,
      blueprint: parsedBlueprint,
    };
  };

  const applyBlueprintToEditor = (blueprint, format) => {
    const targetFormat = format || editorFormat;
    setEditorFormat(targetFormat);
    setEditorValue(serializeBlueprintDocument(blueprint, targetFormat));
  };

  const handleCreateDraft = () => {
    if (!draftActionReadiness.available) {
      message.error(draftActionReadiness.reason);
      return;
    }

    const draftBlueprint = createBlueprintDraftTemplate();
    applyBlueprintToEditor(draftBlueprint, editorFormat);
    setLintReport(null);
    message.info(
      t('Novo rascunho de blueprint carregado no editor.', 'New blueprint draft loaded in the editor.')
    );
  };

  const handleFormatChange = nextFormat => {
    try {
      const parsedBlueprint = parseBlueprintDocument(editorValue, editorFormat);
      setEditorFormat(nextFormat);
      setEditorValue(serializeBlueprintDocument(parsedBlueprint, nextFormat));
    } catch (error) {
      message.error(
        t(
          'Não foi possível converter o formato porque o blueprint atual está inválido.',
          'Could not convert the format because the current blueprint is invalid.'
        )
      );
    }
  };

  const handleImportFile = file => {
    if (!importActionReadiness.available) {
      message.error(importActionReadiness.reason);
      return false;
    }

    const reader = new FileReader();

    reader.onload = async event => {
      const filePayload = String((event && event.target && event.target.result) || '');
      const inferredFormat = inferBlueprintDocumentFormat(file.name);
      const formatToUse = inferredFormat === 'auto' ? 'yaml' : inferredFormat;

      try {
        const parsedBlueprint = parseBlueprintDocument(filePayload, inferredFormat);
        const serialized = serializeBlueprintDocument(parsedBlueprint, formatToUse);

        setEditorFormat(formatToUse);
        setEditorValue(serialized);

        await runLint({
          payload: serialized,
          format: formatToUse,
          showSuccessMessage: false,
          showErrorMessage: true,
        });

        message.success(
          t("Blueprint '{name}' importado com sucesso.", "Blueprint '{name}' imported successfully.", {
            name: file.name,
          })
        );
      } catch (error) {
        setLintReport(parseErrorToReport(error));
        message.error(
          t(
            "Falha ao importar '{name}'. Verifique a estrutura JSON/YAML.",
            "Failed to import '{name}'. Check the JSON/YAML structure.",
            {
              name: file.name,
            }
          )
        );
      }
    };

    reader.readAsText(file);
    return false;
  };

  const handleRunLint = async () => {
    if (!lintActionReadiness.available) {
      message.error(lintActionReadiness.reason);
      return;
    }

    setIsLinting(true);
    await runLint({});
    setIsLinting(false);
  };

  const handlePublishBlueprint = async () => {
    if (!publishActionReadiness.available) {
      message.error(publishActionReadiness.reason);
      return;
    }

    if (!changeId.trim()) {
      message.error(
        t('Informe o change_id para publicar a versão do blueprint.', 'Provide the change_id to publish the blueprint version.')
      );
      return;
    }

    if (!executionContext.trim()) {
      message.error(
        t(
          'Informe o contexto de execução vinculado ao blueprint.',
          'Provide the execution context linked to the blueprint.'
        )
      );
      return;
    }

    setIsPublishing(true);

    try {
      const lintOutcome = await runLint({
        showSuccessMessage: false,
        showErrorMessage: true,
      });

      if (!lintOutcome.valid || !lintOutcome.blueprint) {
        message.error(
          t(
            'Publicação bloqueada: execute correções de lint antes de publicar.',
            'Publication blocked: run lint fixes before publishing.'
          )
        );
        return;
      }

      const publishResponse = await publishBlueprintVersion({
        blueprint: lintOutcome.blueprint,
        change_id: changeId.trim(),
        execution_context: executionContext.trim(),
        allow_migration: false,
      });
      const record =
        publishResponse && publishResponse.status === 'successful' && publishResponse.data
          ? publishResponse.data
          : null;

      if (!record) {
        throw new Error(
          t(
            'Resposta de publicacao fora do contrato esperado.',
            'Publication response outside the expected contract.'
          )
        );
      }

      const mappedVersion = mapBackendVersionRecord(record);
      setVersionHistory(previousVersions => {
        const mergedRows = [
          mappedVersion,
          ...previousVersions.filter(version => version.id !== mappedVersion.id),
        ];
        return mergedRows;
      });

      setTargetVersionId(mappedVersion.id);
      if (!baseVersionId) {
        setBaseVersionId(targetVersionId || undefined);
      }

      message.success(
        t(
          'Blueprint {version} publicado com vínculo ao {changeId}.',
          'Blueprint {version} published linked to {changeId}.',
          {
            version: mappedVersion.blueprintVersion,
            changeId: mappedVersion.changeId,
          }
        )
      );
    } catch (error) {
      message.error(
        t(
          'Falha ao publicar blueprint: {message}',
          'Failed to publish blueprint: {message}',
          {
            message: resolveRequestErrorMessage(error),
          }
        )
      );
    } finally {
      setIsPublishing(false);
    }
  };

  const loadVersionInEditor = version => {
    const blueprintPayload =
      version.blueprint && typeof version.blueprint === 'object' ? version.blueprint : {};
    applyBlueprintToEditor(blueprintPayload, editorFormat);
    setChangeId(version.changeId);
    setExecutionContext(version.executionContext);
    setLintReport({
      valid: !version.issues.some(issue => issue.level === 'error'),
      schemaVersion: version.schemaVersion,
      resolvedSchemaVersion: version.resolvedSchemaVersion,
      blueprintVersion: version.blueprintVersion,
      createdAt: blueprintPayload.created_at || '',
      updatedAt: blueprintPayload.updated_at || '',
      fingerprint: version.fingerprint,
      summary: {
        orgs: Array.isArray(blueprintPayload.orgs) ? blueprintPayload.orgs.length : 0,
        channels: Array.isArray(blueprintPayload.channels) ? blueprintPayload.channels.length : 0,
        nodes: Array.isArray(blueprintPayload.nodes) ? blueprintPayload.nodes.length : 0,
        policies: Array.isArray(blueprintPayload.policies) ? blueprintPayload.policies.length : 0,
      },
      errors: version.issues.filter(issue => issue.level === 'error'),
      warnings: version.warnings || version.issues.filter(issue => issue.level === 'warning'),
      hints: version.hints || version.issues.filter(issue => issue.level === 'hint'),
      issues: version.issues,
      lintSource: version.lintSource || 'history',
      contractValid: version.contractValid !== false,
      normalizedBlueprint: version.normalizedBlueprint || blueprintPayload,
    });
  };

  const lintStatusTag = (() => {
    if (!lintReport) {
      return <Tag color="default">{t('Sem validação', 'No validation')}</Tag>;
    }

    if (lintReport.valid) {
      return <Tag color="green">{t('Lint aprovado', 'Lint approved')}</Tag>;
    }

    return <Tag color="red">{t('Lint bloqueante', 'Blocking lint')}</Tag>;
  })();

  const lintSourceTag = (() => {
    if (!lintReport || !lintReport.lintSource) {
      return <Tag color="default">{t('Sem fonte', 'No source')}</Tag>;
    }

    if (lintReport.lintSource === 'backend') {
      return <Tag color="green">Backend A1.2</Tag>;
    }

    return <Tag color="orange">{t('Degradado/Bloqueado', 'Degraded/Blocked')}</Tag>;
  })();

  const diagnosticDomains = lintReport ? buildLintDiagnosticDomains(lintReport) : [];

  const handleDownloadLintReport = () => {
    if (!lintReport) {
      message.warning(
        t('Execute o lint antes de baixar artefatos.', 'Run lint before downloading artifacts.')
      );
      return;
    }

    const { reportPayload } = buildLintArtifacts(lintReport);
    const timestamp = new Date().toISOString().replace(/[:]/g, '-');

    downloadJsonFile(`blueprint-lint-report-${timestamp}.json`, reportPayload);
    message.success(t('Relatório de lint exportado.', 'Lint report exported.'));
  };

  const handleDownloadNormalizedBlueprint = () => {
    if (!lintReport) {
      message.warning(
        t('Execute o lint antes de baixar artefatos.', 'Run lint before downloading artifacts.')
      );
      return;
    }

    const { normalizedBlueprintPayload } = buildLintArtifacts(lintReport);
    const hasNormalizedBlueprint =
      normalizedBlueprintPayload && Object.keys(normalizedBlueprintPayload).length > 0;

    if (!hasNormalizedBlueprint) {
      message.error(
        t(
          'Blueprint normalizado indisponível no último relatório de lint.',
          'Normalized blueprint unavailable in the latest lint report.'
        )
      );
      return;
    }

    const timestamp = new Date().toISOString().replace(/[:]/g, '-');
    downloadJsonFile(`blueprint-normalized-${timestamp}.json`, normalizedBlueprintPayload);
    message.success(t('Blueprint normalizado exportado.', 'Normalized blueprint exported.'));
  };

  const issueRows = lintReport
    ? lintReport.issues.map((issue, index) => ({
        key: `${issue.code}-${index}`,
        ...issue,
      }))
    : [];

  const issueColumns = [
    {
      title: t('Nível', 'Level'),
      dataIndex: 'level',
      key: 'level',
      width: 110,
      render: level => <Tag color={getIssueColorByLevel(level)}>{level}</Tag>,
    },
    {
      title: t('Código', 'Code'),
      dataIndex: 'code',
      key: 'code',
      width: 260,
      render: code => <Typography.Text code>{code}</Typography.Text>,
    },
    {
      title: t('Caminho', 'Path'),
      dataIndex: 'path',
      key: 'path',
      width: 240,
      render: path => <Typography.Text>{path}</Typography.Text>,
    },
    {
      title: t('Mensagem', 'Message'),
      dataIndex: 'message',
      key: 'message',
    },
  ];

  const versionColumns = [
    {
      title: t('Blueprint', 'Blueprint'),
      dataIndex: 'blueprintVersion',
      key: 'blueprintVersion',
      width: 140,
      render: value => <Typography.Text strong>{value}</Typography.Text>,
    },
    {
      title: t('Schema (resolvida)', 'Resolved schema'),
      key: 'schema',
      width: 210,
      render: (_, record) => (
        <Typography.Text>{`${record.schemaVersion} -> ${record.resolvedSchemaVersion}`}</Typography.Text>
      ),
    },
    {
      title: t('Fingerprint', 'Fingerprint'),
      dataIndex: 'fingerprint',
      key: 'fingerprint',
      width: 230,
      render: value => <Typography.Text code>{truncateFingerprint(value)}</Typography.Text>,
    },
    {
      title: t('change_id', 'change_id'),
      dataIndex: 'changeId',
      key: 'changeId',
      width: 180,
      render: value => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: t('Publicação', 'Publication'),
      dataIndex: 'publishedAt',
      key: 'publishedAt',
      width: 200,
      render: value => formatTimestamp(value),
    },
    {
      title: t('Ações', 'Actions'),
      key: 'actions',
      width: 280,
      render: (_, record) => (
        <Space>
          <Button size="small" onClick={() => loadVersionInEditor(record)}>
            {t('Carregar', 'Load')}
          </Button>
          <Button size="small" onClick={() => setBaseVersionId(record.id)}>
            {t('Base', 'Base')}
          </Button>
          <Button size="small" onClick={() => setTargetVersionId(record.id)}>
            {t('Alvo', 'Target')}
          </Button>
        </Space>
      ),
    },
  ];

  const diffColumns = [
    {
      title: t('Bloco', 'Block'),
      dataIndex: 'label',
      key: 'label',
      width: 220,
      render: value => <Typography.Text strong>{value}</Typography.Text>,
    },
    {
      title: t('Status', 'Status'),
      dataIndex: 'changed',
      key: 'changed',
      width: 140,
      render: changed =>
        changed ? (
          <Tag color="orange">{t('Alterado', 'Changed')}</Tag>
        ) : (
          <Tag color="green">{t('Igual', 'Same')}</Tag>
        ),
    },
    {
      title: t('Versão base', 'Base version'),
      dataIndex: 'baseSummary',
      key: 'baseSummary',
    },
    {
      title: t('Versão alvo', 'Target version'),
      dataIndex: 'targetSummary',
      key: 'targetSummary',
    },
  ];

  return (
    <NeoOpsLayout
      screenKey="e1-blueprint"
      sectionLabel={PROVISIONING_SECTION_LABEL}
      title={screen.title}
      subtitle={screen.objective}
      navItems={provisioningNavItems}
      activeNavKey={resolveProvisioningActiveNavKey('e1-blueprint')}
      breadcrumbs={getProvisioningBreadcrumbs('e1-blueprint')}
      toolbar={
        <Space>
          <Tooltip title={buildReadinessTooltip(importActionReadiness)}>
            <span>
              <Upload
                accept=".json,.yaml,.yml"
                beforeUpload={handleImportFile}
                showUploadList={false}
                disabled={!importActionReadiness.available}
              >
                <Button
                  style={actionButtonStyle}
                  icon={<UploadOutlined />}
                  disabled={!importActionReadiness.available}
                >
                  {t('Importar JSON/YAML (simulado)', 'Import JSON/YAML (simulated)')}
                </Button>
              </Upload>
            </span>
          </Tooltip>
          <Tooltip title={buildReadinessTooltip(lintActionReadiness)}>
            <span>
              <Button
                style={actionButtonStyle}
                icon={<FileSearchOutlined />}
                loading={isLinting}
                onClick={handleRunLint}
                disabled={!lintActionReadiness.available}
              >
                {t('Executar Lint', 'Run lint')}
              </Button>
            </span>
          </Tooltip>
          <Tooltip title={buildReadinessTooltip(publishActionReadiness)}>
            <span>
              <Button
                type="primary"
                icon={<CheckCircleOutlined />}
                loading={isPublishing}
                onClick={handlePublishBlueprint}
                disabled={!publishActionReadiness.available}
              >
                {t('Publicar Versão', 'Publish version')}
              </Button>
            </span>
          </Tooltip>
          <Tooltip title={buildReadinessTooltip(draftActionReadiness)}>
            <span>
              <Button
                style={actionButtonStyle}
                icon={<PlusOutlined />}
                onClick={handleCreateDraft}
                disabled={!draftActionReadiness.available}
              >
                {t('Novo Rascunho (simulado)', 'New draft (simulated)')}
              </Button>
            </span>
          </Tooltip>
        </Space>
      }
    >
      <div className={styles.content}>
        <ProvisioningReadinessCard
          screenKey="e1-blueprint"
          actionOrder={blueprintReadinessActionOrder}
        />
        <div className={styles.neoCard}>
          <Typography.Text className={styles.neoLabel}>
            {t('Restrição de escopo obrigatória da Entrega 1', 'Mandatory scope restriction for Delivery 1')}
          </Typography.Text>
          <div className={styles.chipRow}>
            <span className={styles.chip}>provider: {BLUEPRINT_SCOPE_CONSTRAINTS.provider}</span>
            <span className={styles.chip}>
              compute_target: {BLUEPRINT_SCOPE_CONSTRAINTS.computeTarget}
            </span>
            <span className={styles.chip}>os_family: {BLUEPRINT_SCOPE_CONSTRAINTS.osFamily}</span>
            <span className={styles.chip}>
              runbook: prepare -&gt; provision -&gt; reconcile -&gt; verify
            </span>
          </div>
          <Alert
            showIcon
            type="info"
            style={{ marginTop: 12 }}
            message={t(
              'Publicação bloqueada para topologias fora de external provider + VM Linux.',
              'Publication blocked for topologies outside external provider + Linux VM.'
            )}
            description={t(
              'A validação pré-plano verifica o escopo antes de permitir versionamento publicável.',
              'The pre-plan validation checks the scope before allowing a publishable version.'
            )}
          />
        </div>

        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Vínculo com contexto de execução', 'Execution context link')}
            </Typography.Title>
            <Typography.Paragraph className={styles.neoLabel}>
              {t(
                'O blueprint publicado precisa registrar explicitamente o change_id e o contexto operacional.',
                'The published blueprint must explicitly register the change_id and the operational context.'
              )}
            </Typography.Paragraph>
            <div style={{ display: 'grid', gap: 10 }}>
              <div>
                <Typography.Text className={styles.formFieldLabel}>change_id</Typography.Text>
                <Input
                  value={changeId}
                  placeholder="cr-2026-02-16-001"
                  onChange={event => setChangeId(event.target.value)}
                />
              </div>
              <div>
                <Typography.Text className={styles.formFieldLabel}>
                  {t('contexto de execução', 'execution context')}
                </Typography.Text>
                <TextArea
                  value={executionContext}
                  rows={4}
                  onChange={event => setExecutionContext(event.target.value)}
                  placeholder={t(
                    'Descreva objetivo, ambiente e evidências esperadas.',
                    'Describe the objective, environment, and expected evidence.'
                  )}
                />
              </div>
            </div>
          </div>

          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Metadados resolvidos', 'Resolved metadata')}
            </Typography.Title>
            <div style={{ display: 'grid', gap: 8 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>{t('Status de lint', 'Lint status')}</Typography.Text>
                {lintStatusTag}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>{t('Fonte do lint', 'Lint source')}</Typography.Text>
                {lintSourceTag}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>
                  {t('Schema runtime', 'Schema runtime')}
                </Typography.Text>
                <Typography.Text code>{BLUEPRINT_RUNTIME_SCHEMA_VERSION}</Typography.Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>{t('Schema resolvida', 'Resolved schema')}</Typography.Text>
                <Typography.Text code>
                  {(lintReport && lintReport.resolvedSchemaVersion) || '-'}
                </Typography.Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>
                  {t('Versão do blueprint', 'Blueprint version')}
                </Typography.Text>
                <Typography.Text code>
                  {(lintReport && lintReport.blueprintVersion) || '-'}
                </Typography.Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>{t('Fingerprint canônico', 'Canonical fingerprint')}</Typography.Text>
                <Typography.Text code>
                  {truncateFingerprint(lintReport && lintReport.fingerprint)}
                </Typography.Text>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Typography.Text className={styles.neoLabel}>{t('Issues', 'Issues')}</Typography.Text>
                <Space>
                  <Badge
                    count={(lintReport && lintReport.errors.length) || 0}
                    style={{ background: '#cf1322' }}
                  />
                  <Badge
                    count={(lintReport && lintReport.warnings.length) || 0}
                    style={{ background: '#d48806' }}
                  />
                  <Badge
                    count={(lintReport && lintReport.hints.length) || 0}
                    style={{ background: '#1677ff' }}
                  />
                </Space>
              </div>
              <div>
                <Typography.Text className={styles.neoLabel}>
                  {t('Diagnóstico por domínio', 'Diagnostics by domain')}
                </Typography.Text>
                <div className={styles.chipRow}>
                  {diagnosticDomains.map(domain => (
                    <span key={domain.key} className={styles.chip}>
                      {domain.label}: {domain.count}
                    </span>
                  ))}
                </div>
              </div>
              {lintReport && lintReport.lintSource !== 'backend' && (
                <Alert
                  showIcon
                  type="error"
                  message={t('Lint degradado: publicação bloqueada', 'Degraded lint: publication blocked')}
                  description={t(
                    'A publicação exige contrato oficial do lint backend (A1.2) válido.',
                    'Publication requires a valid official backend lint contract (A1.2).'
                  )}
                />
              )}
            </div>
          </div>
        </div>

        <div className={styles.neoCard}>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: 12,
            }}
          >
            <Typography.Title level={4} className={styles.neoCardTitle} style={{ marginBottom: 0 }}>
              {t(
                'Editor de blueprint (network/orgs/channels/nodes/policies/environment_profile)',
                'Blueprint editor (network/orgs/channels/nodes/policies/environment_profile)'
              )}
            </Typography.Title>
            <Select
              value={editorFormat}
              options={contentFormatOptions}
              style={{ width: 120 }}
              onChange={handleFormatChange}
            />
          </div>
          <TextArea
            rows={20}
            value={editorValue}
            onChange={event => setEditorValue(event.target.value)}
          />
          <Typography.Paragraph
            className={styles.neoLabel}
            style={{ marginTop: 10, marginBottom: 0 }}
          >
            {t(
              'A publicação só é permitida quando o lint pré-plano estiver sem erros bloqueantes.',
              'Publication is only allowed when the pre-plan lint has no blocking errors.'
            )}
          </Typography.Paragraph>
        </div>

        <div className={styles.neoCard}>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: 10,
            }}
          >
            <Typography.Title level={4} className={styles.neoCardTitle} style={{ marginBottom: 0 }}>
              {t('Resultado do lint pré-plano', 'Pre-plan lint result')}
            </Typography.Title>
            <Space>
              <Button icon={<DownloadOutlined />} onClick={handleDownloadLintReport}>
                {t('Baixar relatório', 'Download report')}
              </Button>
              <Button icon={<DownloadOutlined />} onClick={handleDownloadNormalizedBlueprint}>
                {t('Baixar normalizado', 'Download normalized')}
              </Button>
            </Space>
          </div>
          {issueRows.length === 0 ? (
            <Alert
              showIcon
              type="warning"
              message={t('Nenhum lint executado ainda', 'No lint executed yet')}
              description={t(
                'Execute o lint para validar o blueprint antes de publicar uma nova versão.',
                'Run lint to validate the blueprint before publishing a new version.'
              )}
            />
          ) : (
            <Table
              rowKey="key"
              columns={issueColumns}
              dataSource={issueRows}
              pagination={false}
              size="small"
              scroll={{ x: 1000 }}
            />
          )}
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Histórico de versões publicadas', 'Published version history')}
          </Typography.Title>
          <Table
            rowKey="id"
            columns={versionColumns}
            dataSource={versionHistory}
            pagination={false}
            size="small"
            locale={{
              emptyText: t(
                'Nenhuma versão publicada no backend oficial até o momento para o escopo E1.',
                'No version published in the official backend yet for the E1 scope.'
              ),
            }}
            scroll={{ x: 1260 }}
          />
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Comparação entre versões', 'Version comparison')}
          </Typography.Title>
          <div style={{ display: 'grid', gap: 12 }}>
            <div style={{ display: 'grid', gap: 8, gridTemplateColumns: '1fr 1fr' }}>
              <div>
                <Typography.Text className={styles.formFieldLabel}>{t('Versão base', 'Base version')}</Typography.Text>
                <Select
                  value={baseVersionId}
                  placeholder={t('Selecione versão base', 'Select base version')}
                  options={versionOptions}
                  onChange={value => setBaseVersionId(value)}
                />
              </div>
              <div>
                <Typography.Text className={styles.formFieldLabel}>{t('Versão alvo', 'Target version')}</Typography.Text>
                <Select
                  value={targetVersionId}
                  placeholder={t('Selecione versão alvo', 'Select target version')}
                  options={versionOptions}
                  onChange={value => setTargetVersionId(value)}
                />
              </div>
            </div>

            {diffRows.length === 0 ? (
              <Alert
                type="info"
                showIcon
                message={t(
                  'Selecione versões base e alvo para comparar alterações.',
                  'Select base and target versions to compare changes.'
                )}
              />
            ) : (
              <Table
                rowKey="key"
                columns={diffColumns}
                dataSource={diffRows}
                pagination={false}
                size="small"
              />
            )}
          </div>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default ProvisioningBlueprintPage;
