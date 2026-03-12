import React from 'react';
import {
  CloudOutlined,
  DeploymentUnitOutlined,
  PlusOutlined,
  ToolOutlined,
} from '@ant-design/icons';
import { pickCognusText } from '../cognusI18n';
import {
  PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
  getProvisioningContractByScreenKey,
  PROVISIONING_INFRA_SCREEN_KEY,
  PROVISIONING_NAV_ITEMS,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
  PROVISIONING_SCREEN_KEYS,
  PROVISIONING_TECHNICAL_HUB_SCREEN_KEY,
  resolveProvisioningLabel,
} from '../data/provisioningContract';

const screenIconByKey = {
  [PROVISIONING_INFRA_SCREEN_KEY]: <ToolOutlined />,
  [PROVISIONING_TECHNICAL_HUB_SCREEN_KEY]: <CloudOutlined />,
  'e1-blueprint': <CloudOutlined />,
  'e1-provisionamento': <DeploymentUnitOutlined />,
  'e1-inventario': <PlusOutlined />,
  [PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY]: <DeploymentUnitOutlined />,
  [PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY]: <DeploymentUnitOutlined />,
  'e3-lifecycle': <DeploymentUnitOutlined />,
  'e3-guardrails': <CloudOutlined />,
  'e3-rollback': <ToolOutlined />,
};

export const PROVISIONING_SECTION_LABEL = 'COGNUS / Provisioning automation and lifecycle';

const provisioningTechnicalScreenKeySet = new Set([
  ...PROVISIONING_SCREEN_KEYS,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
  PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
  'e3-lifecycle',
  'e3-guardrails',
  'e3-rollback',
]);

const technicalDetailLabelByScreenKey = {
  'e3-lifecycle': {
    ptBR: 'Lifecycle de chaincode (Gateway API)',
    enUS: 'Chaincode lifecycle (Gateway API)',
  },
  'e3-guardrails': {
    ptBR: 'Guardrails de disponibilidade',
    enUS: 'Availability guardrails',
  },
  'e3-rollback': {
    ptBR: 'Versões e rollback',
    enUS: 'Versions and rollback',
  },
};

export const provisioningNavItems = PROVISIONING_NAV_ITEMS.map(item => ({
  ...item,
  icon: screenIconByKey[item.key] || <CloudOutlined />,
}));

const hasTechnicalHubPrimaryNav = provisioningNavItems.some(
  item => item.key === PROVISIONING_TECHNICAL_HUB_SCREEN_KEY
);

export const resolveProvisioningActiveNavKey = screenKey => {
  if (!hasTechnicalHubPrimaryNav) {
    return PROVISIONING_INFRA_SCREEN_KEY;
  }

  if (screenKey === PROVISIONING_INFRA_SCREEN_KEY) {
    return PROVISIONING_INFRA_SCREEN_KEY;
  }
  if (
    screenKey === PROVISIONING_TECHNICAL_HUB_SCREEN_KEY ||
    provisioningTechnicalScreenKeySet.has(screenKey)
  ) {
    return PROVISIONING_TECHNICAL_HUB_SCREEN_KEY;
  }
  return PROVISIONING_INFRA_SCREEN_KEY;
};

export const getProvisioningBreadcrumbs = (screenKey, localeCandidate) => {
  const activeNavKey = resolveProvisioningActiveNavKey(screenKey);
  const navItem = PROVISIONING_NAV_ITEMS.find(item => item.key === activeNavKey);
  const contract = getProvisioningContractByScreenKey(screenKey);
  const primaryLabel =
    (navItem && resolveProvisioningLabel(navItem.label, localeCandidate)) ||
    pickCognusText('Provisão base', 'Base provisioning', localeCandidate);
  const detailLabel =
    (contract &&
      resolveProvisioningLabel(contract.navLabel || contract.routeName, localeCandidate)) ||
    resolveProvisioningLabel(technicalDetailLabelByScreenKey[screenKey], localeCandidate) ||
    pickCognusText('Fluxo operacional', 'Operational flow', localeCandidate);
  const breadcrumbs = [
    pickCognusText(
      'Fluxo completo: Infra base + blueprint + multi-org (external provider + VM Linux)',
      'Complete flow: base infra + blueprint + multi-org (external provider + Linux VM)'
    ),
    primaryLabel,
  ];

  if (detailLabel !== primaryLabel) {
    breadcrumbs.push(detailLabel);
  }

  return breadcrumbs;
};
