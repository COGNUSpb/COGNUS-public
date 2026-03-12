import { pickCognusText } from '../cognusI18n';

export const PROVISIONING_MODULE_ENTRY_PATH = '/automation-lifecycle';
export const PROVISIONING_ROOT_PATH = '/provisioning';
export const PROVISIONING_INFRA_SCREEN_KEY = 'e1-infra-ssh';
export const PROVISIONING_TECHNICAL_HUB_SCREEN_KEY = 'e1-tecnico';
export const PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY = 'e1-topologia-runtime-org';
export const PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY = 'e1-workspace-canal';

export const PROVISIONING_INFRA_ROUTE_PATH = '/provisioning/infrastructure';
export const PROVISIONING_TECHNICAL_HUB_ROUTE_PATH = '/provisioning/technical';
export const PROVISIONING_DEFAULT_ROUTE_PATH = PROVISIONING_INFRA_ROUTE_PATH;
export const PROVISIONING_TECHNICAL_HUB_VISIBLE = false;

const createLocalizedLabel = (ptBR, enUS) => ({ ptBR, enUS });

export const resolveProvisioningLabel = (labelCandidate, localeCandidate) => {
  if (
    labelCandidate &&
    typeof labelCandidate === 'object' &&
    typeof labelCandidate.ptBR === 'string' &&
    typeof labelCandidate.enUS === 'string'
  ) {
    return pickCognusText(labelCandidate.ptBR, labelCandidate.enUS, localeCandidate);
  }

  return String(labelCandidate || '').trim();
};

export const PROVISIONING_SCREEN_CONTRACTS = [
  {
    screenKey: 'e1-blueprint',
    routeName: 'blueprints',
    path: '/provisioning/blueprints',
    component: './Cognus/experiences/ProvisioningBlueprintPage',
    navLabel: createLocalizedLabel('Blueprint e versionamento', 'Blueprint and versioning'),
    hideInMenu: true,
  },
  {
    screenKey: 'e1-provisionamento',
    routeName: 'runbook',
    path: '/provisioning/runbook',
    component: './Cognus/experiences/ProvisioningRunbookPage',
    navLabel: createLocalizedLabel('Provisão assistida', 'Assisted provisioning'),
    hideInMenu: true,
  },
  {
    screenKey: 'e1-inventario',
    routeName: 'inventory',
    path: '/provisioning/inventory',
    component: './Cognus/experiences/ProvisioningInventoryPage',
    navLabel: createLocalizedLabel('Inventário inicial', 'Initial inventory'),
    hideInMenu: true,
  },
  {
    screenKey: PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
    routeName: 'runtime-topology',
    path: '/provisioning/runtime-topology',
    component: './Cognus/experiences/OrganizationRuntimeTopologyPage',
    navLabel: createLocalizedLabel(
      'Topologia runtime da organização',
      'Organization runtime topology'
    ),
    hideInMenu: true,
  },
  {
    screenKey: PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
    routeName: 'channel-workspace',
    path: '/provisioning/channel-workspace',
    component: './Cognus/experiences/ChannelWorkspacePage',
    navLabel: createLocalizedLabel(
      'Workspace operacional do canal',
      'Operational channel workspace'
    ),
    hideInMenu: true,
  },
];

export const PROVISIONING_ENTRY_CONTRACTS = [
  {
    screenKey: PROVISIONING_INFRA_SCREEN_KEY,
    routeName: 'infra',
    path: PROVISIONING_INFRA_ROUTE_PATH,
    component: './Cognus/experiences/ProvisioningInfrastructurePage',
    navLabel: createLocalizedLabel(
      'Provisionamento de Infra - Provedor externo via SSH',
      'Infrastructure Provisioning - External Provider via SSH'
    ),
  },
  {
    screenKey: PROVISIONING_TECHNICAL_HUB_SCREEN_KEY,
    routeName: 'technical',
    path: PROVISIONING_TECHNICAL_HUB_ROUTE_PATH,
    component: './Cognus/experiences/ProvisioningTechnicalHubPage',
    navLabel: createLocalizedLabel('Provisão técnica avançada', 'Advanced technical provisioning'),
    hideInMenu: !PROVISIONING_TECHNICAL_HUB_VISIBLE,
  },
];

export const PROVISIONING_MODULE_CONTRACTS = [
  ...PROVISIONING_ENTRY_CONTRACTS,
  ...PROVISIONING_SCREEN_CONTRACTS,
];

export const provisioningContractByScreenKey = PROVISIONING_MODULE_CONTRACTS.reduce(
  (accumulator, contract) => {
    accumulator[contract.screenKey] = contract;
    return accumulator;
  },
  {}
);

export const PROVISIONING_SCREEN_KEYS = PROVISIONING_SCREEN_CONTRACTS.map(
  contract => contract.screenKey
);

export const PROVISIONING_MODULE_SCREEN_KEYS = PROVISIONING_MODULE_CONTRACTS.map(
  contract => contract.screenKey
);

export const PROVISIONING_SCREEN_PATH_BY_KEY = PROVISIONING_SCREEN_CONTRACTS.reduce(
  (accumulator, contract) => {
    accumulator[contract.screenKey] = contract.path;
    return accumulator;
  },
  {}
);

export const PROVISIONING_MODULE_SCREEN_PATH_BY_KEY = PROVISIONING_MODULE_CONTRACTS.reduce(
  (accumulator, contract) => {
    accumulator[contract.screenKey] = contract.path;
    return accumulator;
  },
  {}
);

export const PROVISIONING_SCREEN_COMPONENT_BY_KEY = PROVISIONING_SCREEN_CONTRACTS.reduce(
  (accumulator, contract) => {
    accumulator[contract.screenKey] = contract.component;
    return accumulator;
  },
  {}
);

export const PROVISIONING_MODULE_SCREEN_COMPONENT_BY_KEY = PROVISIONING_MODULE_CONTRACTS.reduce(
  (accumulator, contract) => {
    accumulator[contract.screenKey] = contract.component;
    return accumulator;
  },
  {}
);

export const PROVISIONING_ROUTE_DEFINITIONS = PROVISIONING_SCREEN_CONTRACTS.map(contract => ({
  path: contract.path,
  name: contract.routeName,
  component: contract.component,
  hideInMenu: true,
}));

export const PROVISIONING_MODULE_ROUTE_DEFINITIONS = PROVISIONING_MODULE_CONTRACTS.map(
  contract => ({
    path: contract.path,
    name: contract.routeName,
    component: contract.component,
    hideInMenu: Boolean(contract.hideInMenu),
  })
);

export const PROVISIONING_NAV_ITEMS = PROVISIONING_ENTRY_CONTRACTS.filter(
  contract => !contract.hideInMenu
).map(contract => ({
  key: contract.screenKey,
  label: contract.navLabel,
  path: contract.path,
}));

export const getProvisioningContractByScreenKey = screenKey =>
  provisioningContractByScreenKey[screenKey] || null;
