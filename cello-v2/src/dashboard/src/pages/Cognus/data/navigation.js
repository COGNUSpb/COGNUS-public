import { screenByKey } from './screens';
import {
  PROVISIONING_MODULE_ENTRY_PATH,
  PROVISIONING_MODULE_SCREEN_KEYS,
  PROVISIONING_MODULE_SCREEN_PATH_BY_KEY,
} from './provisioningContract';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';

const locale = resolveCognusLocale();
const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

export const navigationGroups = [
  {
    key: 'provisioningLifecycle',
    label: t('Automação de provisão e lifecycle', 'Provisioning automation and lifecycle'),
    objective: t(
      'Fluxo completo de Infra SSH (provedor externo) com execução guiada e continuidade operacional.',
      'Full SSH infra flow (external provider) with guided execution and operational continuity.'
    ),
    path: PROVISIONING_MODULE_ENTRY_PATH,
    screens: [...PROVISIONING_MODULE_SCREEN_KEYS, 'e3-lifecycle', 'e3-guardrails', 'e3-rollback'],
  },
  {
    key: 'governanceWorkflows',
    label: t(
      'Governança e workflows multi-organização',
      'Governance and multi-organization workflows'
    ),
    objective: t(
      'Formalização de mudanças interorganizacionais com aprovação e execução coordenada.',
      'Formalization of inter-organizational changes with coordinated approval and execution.'
    ),
    path: '/governance-workflows',
    screens: ['e2-cr', 'e2-aprovacoes', 'e2-timeline'],
  },
  {
    key: 'operationsProcesses',
    label: t('Operações e processos', 'Operations and processes'),
    objective: t(
      'Console do operador, topologia e catálogo operacional (BSO) com foco em repetibilidade.',
      'Operator console, topology, and operational catalog (BSO) focused on repeatability.'
    ),
    path: '/operations-processes',
    screens: ['e4-console', 'e4-topologia', 'e4-runbooks'],
  },
  {
    key: 'securityPrivacyAccess',
    label: t(
      'Segurança, privacidade e controle de acesso',
      'Security, privacy, and access control'
    ),
    objective: t(
      'Políticas, auditoria e evidências para operação segura de ponta a ponta.',
      'Policies, auditing, and evidence for secure end-to-end operations.'
    ),
    path: '/security-privacy',
    screens: ['e5-politicas', 'e5-auditoria', 'e5-evidencias'],
  },
  {
    key: 'resilienceObservability',
    label: t('Resiliência e observabilidade', 'Resilience and observability'),
    objective: t(
      'Dashboards, alertas, incidentes e correlação operacional para tolerância a falhas.',
      'Dashboards, alerts, incidents, and operational correlation for fault tolerance.'
    ),
    path: '/resilience-observability',
    screens: ['e6-observabilidade', 'e6-incidentes', 'e6-correlacao'],
  },
  {
    key: 'portabilityCostPerformance',
    label: t('Portabilidade, custo e desempenho', 'Portability, cost, and performance'),
    objective: t(
      'Catálogo de templates e análise de compatibilidade para decisões de infraestrutura.',
      'Template catalog and compatibility analysis for infrastructure decisions.'
    ),
    path: '/portability-performance',
    screens: ['e7-marketplace', 'e7-compatibilidade', 'e7-template-cr'],
  },
  {
    key: 'interoperabilityMigrationEcosystems',
    label: t(
      'Interoperabilidade, migração e desenho de ecossistemas',
      'Interoperability, migration, and ecosystem design'
    ),
    objective: t(
      'Views, migração e planejamento de evolução de ecossistemas distribuídos.',
      'Views, migration, and evolution planning for distributed ecosystems.'
    ),
    path: '/interop-ecosystem',
    screens: ['e8-readiness', 'e8-usabilidade', 'e8-documentacao'],
  },
];

export const screenPathByKey = {
  ...PROVISIONING_MODULE_SCREEN_PATH_BY_KEY,

  'e2-cr': '/changes/change-request',
  'e2-aprovacoes': '/changes/approvals',
  'e2-timeline': '/changes/timeline',

  'e3-lifecycle': '/chaincode-ops/lifecycle',
  'e3-guardrails': '/chaincode-ops/guardrails',
  'e3-rollback': '/chaincode-ops/rollback',

  'e4-console': '/operations/console',
  'e4-topologia': '/operations/topology',
  'e4-runbooks': '/operations/runbooks',

  'e5-politicas': '/governance/policies',
  'e5-auditoria': '/governance/access-audit',
  'e5-evidencias': '/governance/anchored-evidence',

  'e6-observabilidade': '/observability/dashboards',
  'e6-incidentes': '/observability/incidents',
  'e6-correlacao': '/observability/correlation',

  'e7-marketplace': '/catalog/templates',
  'e7-compatibilidade': '/catalog/compatibility',
  'e7-template-cr': '/catalog/template-link',

  'e8-readiness': '/readiness/ux',
  'e8-usabilidade': '/readiness/usability',
  'e8-documentacao': '/readiness/docs',
};

export const screenKeyByPath = Object.keys(screenPathByKey).reduce((accumulator, screenKey) => {
  accumulator[screenPathByKey[screenKey]] = screenKey;
  return accumulator;
}, {});

export const screenFlow = navigationGroups.reduce((accumulator, group) => {
  (group.screens || []).forEach(screenKey => accumulator.push(screenKey));
  return accumulator;
}, []);

export const getScreenPath = screenKey => screenPathByKey[screenKey] || '/overview';

export const getGroupByScreenKey = screenKey =>
  navigationGroups.find(group =>
    group.screens.some(groupScreenKey => groupScreenKey === screenKey)
  );

export const getFlowAdjacentScreens = screenKey => {
  const index = screenFlow.findIndex(current => current === screenKey);
  if (index < 0) {
    return { previous: null, next: null };
  }

  return {
    previous: index > 0 ? screenByKey[screenFlow[index - 1]] : null,
    next: index < screenFlow.length - 1 ? screenByKey[screenFlow[index + 1]] : null,
  };
};
