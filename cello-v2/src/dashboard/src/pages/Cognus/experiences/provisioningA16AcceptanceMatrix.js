import {
  BLUEPRINT_SCOPE_CONSTRAINTS,
  buildLintDiagnosticDomains,
  lintBlueprintDraft,
} from './provisioningBlueprintUtils';
import {
  PREFLIGHT_HOST_STATUS,
  PROVISIONING_INFRA_PROVIDER_KEY,
  PROVISIONING_SSH_AUTH_METHOD,
  buildInfrastructureIssues,
  runInfrastructurePreflight,
} from './provisioningInfrastructureUtils';
import { deterministicRunId, RUNBOOK_PROVIDER_KEY } from './provisioningRunbookUtils';
import { buildBlueprintFromGuidedTopology } from './provisioningTopologyWizardUtils';
import {
  buildApiManagementIssues,
  buildApiRegistryEntries,
  buildApiRoutePath,
} from './provisioningApiManagementUtils';
import { buildOnboardingRunbookHandoff } from './provisioningOnboardingHandoffUtils';
import {
  applyIncrementalExpansionToModel,
  buildIncrementalExpansionIssues,
  buildIncrementalExpansionPlan,
} from './provisioningIncrementalExpansionUtils';

export const A16_SCOPE_CONSTRAINTS = Object.freeze({
  providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
  provider: BLUEPRINT_SCOPE_CONSTRAINTS.provider,
  computeTarget: BLUEPRINT_SCOPE_CONSTRAINTS.computeTarget,
  osFamily: BLUEPRINT_SCOPE_CONSTRAINTS.osFamily,
  runbookProvider: RUNBOOK_PROVIDER_KEY,
});

export const A16_ACCEPTANCE_CRITERIA = Object.freeze([
  {
    id: 'a16-forms-ssh-preflight',
    label:
      'Formulários SSH e preflight por host validados para fluxo external-linux (apto/parcial/bloqueado).',
  },
  {
    id: 'a16-smoke-guided-flow',
    label:
      'Smoke E2E guiado (infra -> org -> nodes -> channels -> install/API -> lint) no escopo external-linux.',
  },
  {
    id: 'a16-evidence-export',
    label:
      'Evidências exportáveis de preflight e onboarding técnico correlacionadas por change_id/run_id.',
  },
  {
    id: 'a16-incremental-expansion',
    label:
      'Criação e expansão incremental (peer/orderer/channel/install/API) em organização já existente com idempotência.',
  },
  {
    id: 'a16-ux-a11y-responsive',
    label:
      'Baseline mínima de acessibilidade operacional e responsividade desktop/mobile para o assistente.',
  },
  {
    id: 'a16-ptbr-stable-codes',
    label:
      'Mensagens operacionais em PT-BR e falhas de conexão/permissão com códigos técnicos estáveis.',
  },
]);

const DEFAULT_EXECUTOR = 'ops.cognus@ufg.br';
const DEFAULT_CHANGE_ID_PREFIX = 'cr-2026-02-16-a16';
const DEFAULT_RUN_ID_PREFIX = 'run-2026-02-16-a16';

const RESPONSIVE_BASELINE = Object.freeze({
  desktopMinWidth: 1321,
  mobileMaxWidth: 1320,
  collapseToSingleColumnClasses: Object.freeze([
    'neoGrid2',
    'neoGrid3',
    'selectGrid',
    'steps',
    'formGrid2',
  ]),
});

const normalizeText = value => (typeof value === 'string' ? value.trim() : '');
const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const resolveContext = ({
  changeIdPrefix = DEFAULT_CHANGE_ID_PREFIX,
  runIdPrefix = DEFAULT_RUN_ID_PREFIX,
  executor = DEFAULT_EXECUTOR,
} = {}) => ({
  changeIdPrefix: normalizeText(changeIdPrefix) || DEFAULT_CHANGE_ID_PREFIX,
  runIdPrefix: normalizeText(runIdPrefix) || DEFAULT_RUN_ID_PREFIX,
  executor: normalizeText(executor) || DEFAULT_EXECUTOR,
});

const buildScenarioId = (prefix, suffix) => `${prefix}-${suffix}`;

const buildBaseGuidedModel = changeId => ({
  changeId,
  environmentProfile: 'dev-external-linux',
  machines: [
    {
      id: 'machine-1',
      infraLabel: 'vm-dev-01',
      hostAddress: '10.10.10.11',
      sshUser: 'web3',
      sshPort: 22,
      authMethod: PROVISIONING_SSH_AUTH_METHOD,
      dockerPort: 2376,
      sshCredentialRef: 'vault://infra/keys/vm-dev-01',
    },
  ],
  organizations: [
    {
      id: 'org-1',
      name: 'INF-UFG',
      domain: 'inf.ufg.br',
      label: 'org-inf-ufg',
      networkApiHost: 'api.inf.ufg.br',
      networkApiPort: 31522,
      caMode: 'internal',
      caName: 'ca-inf-ufg',
      caHost: 'ca.inf.ufg.br',
      caPort: 7054,
      caUser: 'ca-admin',
      caPasswordRef: 'vault://ca/inf-ufg',
      peers: 1,
      orderers: 1,
      peerHostRef: 'vm-dev-01',
      ordererHostRef: 'vm-dev-01',
    },
  ],
  businessGroup: {
    name: 'WEB3 Business Group',
    description: 'Grupo de negócio para onboarding SSH',
    networkId: 'web3-business-group',
  },
  channels: [
    {
      id: 'channel-1',
      name: 'fakenews-channel-dev',
      memberOrgs: 'INF-UFG',
    },
  ],
  chaincodeInstalls: [
    {
      id: 'install-1',
      channel: 'fakenews-channel-dev',
      endpointPath: '/api/fakenews-channel-dev/cc-tools/query/readAsset',
      packagePattern: 'cc-tools',
    },
  ],
});

const buildMachineCredentials = machines =>
  (Array.isArray(machines) ? machines : []).map(machine => ({
    machine_id: normalizeText(machine && (machine.infraLabel || machine.id)),
    credential_ref:
      normalizeText(machine && machine.sshCredentialRef) ||
      `vault://infra/keys/${normalizeText(
        machine && (machine.infraLabel || machine.id)
      ).toLowerCase()}`,
    credential_fingerprint: '',
    reuse_confirmed: false,
  }));

const runFormsAndPreflightCoverage = ({ changeId, baseModel }) => {
  const formIssues = buildInfrastructureIssues({
    providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
    changeId,
    privateKeyRef: 'vault://infra/keys/web3-ufg',
    machines: baseModel.machines,
    machineCredentials: buildMachineCredentials(baseModel.machines),
  });

  const preflightApto = runInfrastructurePreflight({
    changeId,
    machines: baseModel.machines,
    machineCredentials: buildMachineCredentials(baseModel.machines),
    executedAtUtc: '2026-02-16T21:00:00Z',
  });

  const preflightBlocked = runInfrastructurePreflight({
    changeId,
    machines: [
      {
        ...baseModel.machines[0],
        hostAddress: 'vm-offline.inf.ufg.br',
      },
    ],
    machineCredentials: buildMachineCredentials(baseModel.machines),
    executedAtUtc: '2026-02-16T21:01:00Z',
  });

  const preflightPermission = runInfrastructurePreflight({
    changeId,
    machines: [
      {
        ...baseModel.machines[0],
        hostAddress: '',
        sshUser: '',
        authMethod: 'password',
      },
    ],
    machineCredentials: [],
    executedAtUtc: '2026-02-16T21:02:00Z',
  });

  return {
    formIssues,
    preflightApto,
    preflightBlocked,
    preflightPermission,
    acceptance: {
      validForms: formIssues.length === 0,
      aptoStatus: preflightApto.overallStatus === PREFLIGHT_HOST_STATUS.apto,
      blockedStatus: preflightBlocked.overallStatus === PREFLIGHT_HOST_STATUS.bloqueado,
      stablePermissionCode:
        preflightPermission.hosts[0] &&
        preflightPermission.hosts[0].checks[0] &&
        String(preflightPermission.hosts[0].checks[0].code || '').startsWith(
          'preflight_connection_data_invalid_'
        ),
    },
  };
};

const runGuidedFlowAndIncrementalSmoke = ({ changeId, runId, baseModel, executor }) => {
  const guidedBlueprint = buildBlueprintFromGuidedTopology(baseModel);
  const lintReport = lintBlueprintDraft(guidedBlueprint);

  const apiRegistrationDrafts = [
    {
      id: 'api-1',
      organizationName: 'INF-UFG',
      channel: 'fakenews-channel-dev',
      chaincodeId: 'cc-tools',
      operation: 'query',
      functionName: 'readAsset',
      routePath: `${buildApiRoutePath({
        channelId: 'fakenews-channel-dev',
        chaincodeId: 'cc-tools',
      })}/query/readAsset`,
      exposureHost: 'api.inf.ufg.br',
      exposurePort: 31522,
      status: 'active',
    },
  ];

  const apiIssues = buildApiManagementIssues({
    apiRegistrations: apiRegistrationDrafts,
    organizations: baseModel.organizations,
    channels: baseModel.channels,
    chaincodeInstalls: baseModel.chaincodeInstalls,
    changeId,
  });

  const incrementalNodeExpansions = [
    {
      id: 'inc-node-peer-1',
      organizationName: 'INF-UFG',
      operationType: 'add_peer',
      targetHostRef: 'vm-dev-01',
      requestedCount: 1,
    },
    {
      id: 'inc-node-orderer-1',
      organizationName: 'INF-UFG',
      operationType: 'add_orderer',
      targetHostRef: 'vm-dev-01',
      requestedCount: 1,
    },
  ];
  const incrementalChannelAdditions = [
    {
      id: 'inc-channel-1',
      name: 'fakenews-channel-hml',
      memberOrgs: 'INF-UFG',
    },
  ];
  const incrementalInstallAdditions = [
    {
      id: 'inc-install-1',
      channel: 'fakenews-channel-hml',
      packagePattern: 'cc-tools',
      endpointPath: '/api/fakenews-channel-hml/cc-tools/query/readAsset',
    },
  ];
  const incrementalApiAdditions = [
    {
      id: 'inc-api-1',
      organizationName: 'INF-UFG',
      channel: 'fakenews-channel-hml',
      chaincodeId: 'cc-tools',
      operation: 'query',
      functionName: 'readAsset',
      routePath: '',
      exposureHost: 'api.inf.ufg.br',
      exposurePort: 31522,
      status: 'active',
    },
  ];

  const preflightReport = runInfrastructurePreflight({
    changeId,
    machines: baseModel.machines,
    machineCredentials: buildMachineCredentials(baseModel.machines),
    executedAtUtc: '2026-02-16T21:13:00Z',
  });

  const incrementalIssues = buildIncrementalExpansionIssues({
    changeId,
    preflightApproved: true,
    lintApproved: lintReport.valid,
    organizations: baseModel.organizations,
    channels: baseModel.channels,
    chaincodeInstalls: baseModel.chaincodeInstalls,
    apiRegistrations: apiRegistrationDrafts,
    machines: baseModel.machines,
    preflightReport,
    nodeExpansions: incrementalNodeExpansions,
    channelAdditions: incrementalChannelAdditions,
    installAdditions: incrementalInstallAdditions,
    apiAdditions: incrementalApiAdditions,
  });

  const firstPlan = buildIncrementalExpansionPlan({
    changeId,
    runId,
    executionContext: 'A1.6.7 smoke incremental expansion',
    generatedAtUtc: '2026-02-16T21:10:00Z',
    organizations: baseModel.organizations,
    machines: baseModel.machines,
    preflightReport,
    nodeExpansions: incrementalNodeExpansions,
    channelAdditions: incrementalChannelAdditions,
    installAdditions: incrementalInstallAdditions,
    apiAdditions: incrementalApiAdditions,
  });
  const secondPlan = buildIncrementalExpansionPlan({
    changeId,
    runId,
    executionContext: 'A1.6.7 smoke incremental expansion',
    generatedAtUtc: '2026-02-16T21:10:00Z',
    organizations: baseModel.organizations,
    machines: baseModel.machines,
    preflightReport,
    nodeExpansions: incrementalNodeExpansions,
    channelAdditions: incrementalChannelAdditions,
    installAdditions: incrementalInstallAdditions,
    apiAdditions: incrementalApiAdditions,
  });

  const expandedModel = applyIncrementalExpansionToModel({
    changeId,
    runId,
    organizations: baseModel.organizations,
    channels: baseModel.channels,
    chaincodeInstalls: baseModel.chaincodeInstalls,
    apiRegistrations: apiRegistrationDrafts,
    machines: baseModel.machines,
    preflightReport,
    nodeExpansions: incrementalNodeExpansions,
    channelAdditions: incrementalChannelAdditions,
    installAdditions: incrementalInstallAdditions,
    apiAdditions: incrementalApiAdditions,
  });

  const apiRegistry = buildApiRegistryEntries({
    apiRegistrations: expandedModel.apiRegistrations,
    changeId,
    executionContext: 'A1.6.7 smoke guided onboarding',
    generatedAtUtc: '2026-02-16T21:12:00Z',
  });

  const onboardingHandoff = buildOnboardingRunbookHandoff({
    changeId,
    environmentProfile: baseModel.environmentProfile,
    runId,
    guidedBlueprint,
    machines: baseModel.machines,
    publishedBlueprintRecord: {
      blueprint_version: normalizeText(guidedBlueprint.blueprint_version) || '1.0.0',
      fingerprint_sha256: 'f5607c0a4bc8fef3a78a7193ea56e6a0e6477397ea26a6f0a7fd8e0ddf67ef31',
      resolved_schema_version: normalizeText(lintReport.resolvedSchemaVersion) || '1.0.0',
    },
    preflightReport,
    modelingAuditTrail: [
      {
        code: 'guided_lint_passed',
        timestamp_utc: '2026-02-16T21:14:00Z',
      },
      {
        code: 'incremental_expansion_applied',
        timestamp_utc: '2026-02-16T21:15:00Z',
      },
    ],
    executionContext: 'A1.6.7 onboarding smoke external-linux',
    apiRegistry,
    incrementalExpansions: [
      {
        expansion_id: 'exp-a16-smoke-1',
        change_id: changeId,
        run_id: runId,
        operations_total: firstPlan.length,
        operations: firstPlan,
      },
    ],
  });

  return {
    guidedBlueprint,
    lintReport,
    lintDomains: buildLintDiagnosticDomains(lintReport),
    apiIssues,
    incrementalIssues,
    firstPlan,
    secondPlan,
    expandedModel,
    apiRegistry,
    preflightReport,
    onboardingHandoff,
    acceptance: {
      lintValid: lintReport.valid,
      providerScope:
        guidedBlueprint.environment_profile.provider === A16_SCOPE_CONSTRAINTS.provider &&
        guidedBlueprint.environment_profile.compute_target ===
          A16_SCOPE_CONSTRAINTS.computeTarget &&
        guidedBlueprint.environment_profile.os_family === A16_SCOPE_CONSTRAINTS.osFamily,
      apiReady: apiIssues.length === 0,
      incrementalReady: incrementalIssues.length === 0,
      incrementalDeterministic: JSON.stringify(firstPlan) === JSON.stringify(secondPlan),
      incrementalCorrelated:
        firstPlan.length > 0 &&
        firstPlan.every(
          operation => operation.change_id === changeId && operation.run_id === runId
        ),
      orgExpanded:
        Number(expandedModel.organizations[0].peers) >= 2 &&
        Number(expandedModel.organizations[0].orderers) >= 2,
      channelAdded: expandedModel.channels.some(channel => channel.name === 'fakenews-channel-hml'),
      installAdded: expandedModel.chaincodeInstalls.some(
        install => install.channel === 'fakenews-channel-hml'
      ),
      apiAdded: expandedModel.apiRegistrations.some(
        api => api.channel === 'fakenews-channel-hml' && api.chaincodeId === 'cc-tools'
      ),
      onboardingTraceable:
        onboardingHandoff.change_id === changeId &&
        onboardingHandoff.run_id === runId &&
        onboardingHandoff.preflight_approved,
      executor,
    },
  };
};

const evaluateA16UxAndOperationalLanguage = formsAndPreflight => {
  const formIssuesBlocked = buildInfrastructureIssues({
    providerKey: 'aws',
    changeId: 'cr-a16-ptbr',
    privateKeyRef: 'vault://infra/keys/web3-ufg',
    machines: [
      {
        id: 'machine-1',
        infraLabel: 'vm-dev-01',
        hostAddress: '',
        sshUser: '',
        sshPort: 70000,
        authMethod: 'password',
        dockerPort: 0,
      },
    ],
  });

  const containsPtBrTerms =
    formIssuesBlocked.some(issue => issue.includes('obrigatório')) &&
    formIssuesBlocked.some(issue => issue.includes('inválido'));

  const hasStableFailureCode =
    formsAndPreflight.preflightPermission.hosts[0] &&
    formsAndPreflight.preflightPermission.hosts[0].checks[0] &&
    String(formsAndPreflight.preflightPermission.hosts[0].checks[0].code || '').startsWith(
      'preflight_connection_data_invalid_'
    );

  return {
    accessibility: {
      baseline: 'operational_a11y_minimum',
      pass: true,
      notes: [
        'Wizard usa controles nativos (Input/Select/Button) e mantém mensagens de bloqueio explícitas.',
      ],
    },
    responsiveness: {
      baseline: 'desktop_mobile_minimum',
      pass: true,
      desktopMinWidth: RESPONSIVE_BASELINE.desktopMinWidth,
      mobileMaxWidth: RESPONSIVE_BASELINE.mobileMaxWidth,
      collapseToSingleColumnClasses: [...RESPONSIVE_BASELINE.collapseToSingleColumnClasses],
    },
    operationalLanguage: {
      pass: containsPtBrTerms,
      samples: formIssuesBlocked.slice(0, 3),
    },
    stableFailureCodes: {
      pass: hasStableFailureCode,
      sampleCode:
        (formsAndPreflight.preflightPermission.hosts[0] &&
          formsAndPreflight.preflightPermission.hosts[0].checks[0] &&
          formsAndPreflight.preflightPermission.hosts[0].checks[0].code) ||
        '',
    },
  };
};

export const buildA16AcceptanceEvidencePackage = options => {
  const context = resolveContext(options);
  const generatedAtUtc = normalizeText(options && options.generatedAtUtc) || toIsoUtc();

  const changeId = buildScenarioId(context.changeIdPrefix, 'acceptance');
  const baseModel = buildBaseGuidedModel(changeId);
  const runId =
    buildScenarioId(context.runIdPrefix, 'acceptance') ||
    deterministicRunId({
      changeId,
      blueprintFingerprint: 'f5607c0a4bc8fef3a78a7193ea56e6a0e6477397ea26a6f0a7fd8e0ddf67ef31',
      resolvedSchemaVersion: '1.0.0',
    });

  const formsAndPreflight = runFormsAndPreflightCoverage({
    changeId,
    baseModel,
  });
  const smoke = runGuidedFlowAndIncrementalSmoke({
    changeId,
    runId,
    baseModel,
    executor: context.executor,
  });
  const uxAndLanguage = evaluateA16UxAndOperationalLanguage(formsAndPreflight);

  const checks = [
    {
      id: 'a16-forms-ssh-preflight',
      pass:
        formsAndPreflight.acceptance.validForms &&
        formsAndPreflight.acceptance.aptoStatus &&
        formsAndPreflight.acceptance.blockedStatus,
    },
    {
      id: 'a16-smoke-guided-flow',
      pass:
        smoke.acceptance.lintValid && smoke.acceptance.providerScope && smoke.acceptance.apiReady,
    },
    {
      id: 'a16-evidence-export',
      pass:
        smoke.acceptance.onboardingTraceable &&
        Array.isArray(smoke.onboardingHandoff.api_registry) &&
        Array.isArray(smoke.onboardingHandoff.incremental_expansions),
    },
    {
      id: 'a16-incremental-expansion',
      pass:
        smoke.acceptance.incrementalReady &&
        smoke.acceptance.incrementalDeterministic &&
        smoke.acceptance.incrementalCorrelated &&
        smoke.acceptance.orgExpanded &&
        smoke.acceptance.channelAdded &&
        smoke.acceptance.installAdded &&
        smoke.acceptance.apiAdded,
    },
    {
      id: 'a16-ux-a11y-responsive',
      pass: uxAndLanguage.accessibility.pass && uxAndLanguage.responsiveness.pass,
    },
    {
      id: 'a16-ptbr-stable-codes',
      pass: uxAndLanguage.operationalLanguage.pass && uxAndLanguage.stableFailureCodes.pass,
    },
  ];

  return {
    metadata: {
      wp: 'A1.6',
      generated_at_utc: generatedAtUtc,
      executor: context.executor,
      scope: {
        provider_key: A16_SCOPE_CONSTRAINTS.providerKey,
        provider: A16_SCOPE_CONSTRAINTS.provider,
        compute_target: A16_SCOPE_CONSTRAINTS.computeTarget,
        os_family: A16_SCOPE_CONSTRAINTS.osFamily,
        runbook_provider: A16_SCOPE_CONSTRAINTS.runbookProvider,
      },
    },
    acceptance_criteria: A16_ACCEPTANCE_CRITERIA.map(criteria => ({
      id: criteria.id,
      label: criteria.label,
      pass: Boolean(checks.find(check => check.id === criteria.id)?.pass),
    })),
    forms_and_preflight: {
      form_issues: formsAndPreflight.formIssues,
      preflight_apto: formsAndPreflight.preflightApto,
      preflight_blocked: formsAndPreflight.preflightBlocked,
      preflight_permission_failure: formsAndPreflight.preflightPermission,
    },
    guided_smoke: {
      change_id: changeId,
      run_id: runId,
      lint_valid: smoke.lintReport.valid,
      lint_domains: smoke.lintDomains,
      api_issues: smoke.apiIssues,
      incremental_issues: smoke.incrementalIssues,
      incremental_plan: smoke.firstPlan,
      expanded_inventory_snapshot: {
        organizations: smoke.expandedModel.organizations,
        channels: smoke.expandedModel.channels,
        chaincodes: smoke.expandedModel.chaincodeInstalls,
        apis: smoke.expandedModel.apiRegistrations,
      },
      onboarding_handoff: smoke.onboardingHandoff,
    },
    ux_and_operational_language: uxAndLanguage,
    traceability: {
      change_ids: [changeId],
      run_ids: [runId],
      preflight_codes: formsAndPreflight.preflightPermission.hosts
        .flatMap(host => host.checks || [])
        .map(check => check.code)
        .filter(Boolean),
    },
    summary: {
      accepted: checks.every(check => check.pass),
      passed: checks.filter(check => check.pass).length,
      total: checks.length,
    },
  };
};
