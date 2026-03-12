/*
 SPDX-License-Identifier: Apache-2.0
*/
import {
  PROVISIONING_DEFAULT_ROUTE_PATH,
  PROVISIONING_MODULE_ENTRY_PATH,
  PROVISIONING_MODULE_ROUTE_DEFINITIONS,
  PROVISIONING_ROOT_PATH,
} from '../src/pages/Cognus/data/provisioningContract';

const provisioningStandaloneRoutes = PROVISIONING_MODULE_ROUTE_DEFINITIONS.map(route => ({
  ...route,
  hideInMenu: true,
}));

export default [
  // user
  {
    path: '/user',
    component: '../layouts/UserLayout',
    routes: [
      { path: '/user', redirect: '/user/login' },
      { path: '/user/login', name: 'login', component: './User/Login' },
      {
        component: '404',
      },
    ],
  },
  // app
  {
    path: '/',
    component: '../layouts/SecurityLayout',
    routes: [
      {
        path: '/',
        component: '../layouts/BasicLayout',
        authority: ['admin', 'member', 'operator'],
        routes: [
          { path: '/', redirect: '/overview' },
          {
            path: '/overview',
            name: 'overview',
            icon: 'dashboard',
            component: './Cognus/Overview',
          },
          {
            path: '/cello-core',
            name: 'celloCore',
            icon: 'team',
            menuOrder: 99,
            menuDividerBefore: true,
            hideInMenu: true,
            authority: ['admin', 'member'],
            routes: [
              { path: '/cello-core', redirect: '/organization' },
              {
                path: '/organization',
                authority: ['admin'],
                name: 'organization',
                icon: 'team',
                component: './Organization/Organization',
              },
              {
                path: '/agent',
                name: 'agent',
                icon: 'agent',
                component: './Agent/Agent',
              },
              {
                path: '/agent/newAgent',
                name: 'newAgent',
                component: './Agent/newAgent',
                hideInMenu: true,
              },
              {
                path: '/agent/editAgent',
                name: 'editAgent',
                component: './Agent/newAgent',
                hideInMenu: true,
              },
              {
                path: '/node',
                name: 'node',
                icon: 'node',
                component: './Node/index',
              },
              {
                path: '/node/new',
                name: 'newNode',
                hideInMenu: true,
                component: './Node/New/index',
                routes: [
                  {
                    path: '/node/new',
                    redirect: '/node/new/basic-info',
                  },
                  {
                    path: '/node/new/basic-info',
                    name: 'basicInfo',
                    component: './Node/New/basicInfo',
                  },
                  {
                    path: '/node/new/node-info',
                    name: 'nodeInfo',
                    component: './Node/New/nodeInfo',
                  },
                ],
              },
              {
                path: '/network',
                name: 'network',
                icon: 'network',
                component: './Network/Network',
              },
              {
                path: '/network/newNetwork',
                name: 'newNetwork',
                component: './Network/newNetwork',
                hideInMenu: true,
              },
              {
                path: '/channel',
                name: 'channel',
                icon: 'channel',
                component: './Channel/Channel',
              },
              {
                path: '/chaincode',
                name: 'chaincode',
                icon: 'chaincode',
                component: './ChainCode/ChainCode',
              },
              {
                path: '/userManagement',
                name: 'userManagement',
                icon: 'user',
                component: './UserManagement/UserManagement',
              },
            ],
          },
          {
            path: PROVISIONING_MODULE_ENTRY_PATH,
            name: 'provisioningLifecycle',
            icon: 'network',
            routes: [
              { path: PROVISIONING_MODULE_ENTRY_PATH, redirect: PROVISIONING_DEFAULT_ROUTE_PATH },
              { path: PROVISIONING_ROOT_PATH, redirect: PROVISIONING_DEFAULT_ROUTE_PATH },
              ...PROVISIONING_MODULE_ROUTE_DEFINITIONS,
              { path: '/chaincode-ops', redirect: '/chaincode-ops/lifecycle' },
              {
                path: '/chaincode-ops/lifecycle',
                name: 'lifecycle',
                hideInMenu: true,
                component: './Cognus/experiences/LifecycleStudioPage',
              },
              {
                path: '/chaincode-ops/guardrails',
                name: 'guardrails',
                hideInMenu: true,
                component: './Cognus/experiences/GuardrailsProposalPage',
              },
              {
                path: '/chaincode-ops/rollback',
                name: 'rollback',
                hideInMenu: true,
                component: './Cognus/ScreenPage',
              },
            ],
          },
          {
            path: '/governance-workflows',
            name: 'governanceWorkflows',
            icon: 'agent',
            menuDisabled: true,
            routes: [
              { path: '/governance-workflows', redirect: '/changes/change-request' },
              { path: '/changes', redirect: '/changes/change-request' },
              {
                path: '/changes/change-request',
                name: 'changeRequest',
                authority: ['admin', 'member'],
                component: './Cognus/experiences/ChangeRequestWorkbenchPage',
              },
              {
                path: '/changes/approvals',
                name: 'approvals',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/changes/timeline',
                name: 'timeline',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
            ],
          },
          {
            path: '/operations-processes',
            name: 'operationsProcesses',
            icon: 'node',
            menuDisabled: true,
            routes: [
              { path: '/operations-processes', redirect: '/operations/console' },
              { path: '/operations', redirect: '/operations/console' },
              {
                path: '/operations/console',
                name: 'console',
                authority: ['admin', 'member'],
                component: './Cognus/experiences/OperationsConsolePage',
              },
              {
                path: '/operations/topology',
                name: 'topology',
                authority: ['admin', 'member'],
                component: './Cognus/experiences/OperationsTopologyPage',
              },
              {
                path: '/operations/runbooks',
                name: 'runbooks',
                authority: ['admin', 'member'],
                component: './Cognus/experiences/ChannelWorkspacePage',
              },
            ],
          },
          {
            path: '/security-privacy',
            name: 'securityPrivacyAccess',
            icon: 'user',
            menuDisabled: true,
            routes: [
              { path: '/security-privacy', redirect: '/governance/policies' },
              { path: '/governance', redirect: '/governance/policies' },
              {
                path: '/governance/policies',
                name: 'policies',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/governance/access-audit',
                name: 'accessAudit',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/governance/anchored-evidence',
                name: 'anchoredEvidence',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
            ],
          },
          {
            path: '/resilience-observability',
            name: 'resilienceObservability',
            icon: 'dashboard',
            menuDisabled: true,
            routes: [
              { path: '/resilience-observability', redirect: '/observability/dashboards' },
              { path: '/observability', redirect: '/observability/dashboards' },
              {
                path: '/observability/dashboards',
                name: 'dashboards',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/observability/incidents',
                name: 'incidents',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/observability/correlation',
                name: 'correlation',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
            ],
          },
          {
            path: '/portability-performance',
            name: 'portabilityCostPerformance',
            icon: 'channel',
            menuDisabled: true,
            routes: [
              { path: '/portability-performance', redirect: '/catalog/templates' },
              { path: '/catalog', redirect: '/catalog/templates' },
              {
                path: '/catalog/templates',
                name: 'templates',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/catalog/compatibility',
                name: 'compatibility',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/catalog/template-link',
                name: 'templateLink',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
            ],
          },
          {
            path: '/interop-ecosystem',
            name: 'interoperabilityMigrationEcosystems',
            icon: 'team',
            menuDisabled: true,
            routes: [
              { path: '/interop-ecosystem', redirect: '/readiness/ux' },
              { path: '/readiness', redirect: '/readiness/ux' },
              {
                path: '/readiness/ux',
                name: 'readinessUx',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/readiness/usability',
                name: 'usability',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
              {
                path: '/readiness/docs',
                name: 'profileDocs',
                authority: ['admin', 'member'],
                component: './Cognus/ScreenPage',
              },
            ],
          },
          // Standalone routes keep pages accessible regardless of menu grouping.
          {
            path: '/organization',
            authority: ['admin'],
            hideInMenu: true,
            component: './Organization/Organization',
          },
          {
            path: '/agent',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Agent/Agent',
          },
          {
            path: '/agent/newAgent',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Agent/newAgent',
          },
          {
            path: '/agent/editAgent',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Agent/newAgent',
          },
          {
            path: '/node',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Node/index',
          },
          {
            path: '/node/new',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Node/New/index',
            routes: [
              {
                path: '/node/new',
                redirect: '/node/new/basic-info',
              },
              {
                path: '/node/new/basic-info',
                name: 'basicInfo',
                component: './Node/New/basicInfo',
              },
              {
                path: '/node/new/node-info',
                name: 'nodeInfo',
                component: './Node/New/nodeInfo',
              },
            ],
          },
          {
            path: '/network',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Network/Network',
          },
          {
            path: '/network/newNetwork',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Network/newNetwork',
          },
          {
            path: '/channel',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Channel/Channel',
          },
          {
            path: '/chaincode',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './ChainCode/ChainCode',
          },
          {
            path: '/userManagement',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './UserManagement/UserManagement',
          },
          {
            path: '/changes/change-request',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/experiences/ChangeRequestWorkbenchPage',
          },
          {
            path: '/changes/approvals',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/changes/timeline',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/operations/console',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/experiences/OperationsConsolePage',
          },
          {
            path: '/operations/topology',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/experiences/OperationsTopologyPage',
          },
          {
            path: '/operations/runbooks',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/experiences/ChannelWorkspacePage',
          },
          ...provisioningStandaloneRoutes,
          {
            path: '/chaincode-ops/lifecycle',
            hideInMenu: true,
            component: './Cognus/experiences/LifecycleStudioPage',
          },
          {
            path: '/chaincode-ops/guardrails',
            hideInMenu: true,
            component: './Cognus/experiences/GuardrailsProposalPage',
          },
          {
            path: '/chaincode-ops/rollback',
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/governance/policies',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/governance/access-audit',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/governance/anchored-evidence',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/observability/dashboards',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/observability/incidents',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/observability/correlation',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/catalog/templates',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/catalog/compatibility',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/catalog/template-link',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/readiness/ux',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/readiness/usability',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
          {
            path: '/readiness/docs',
            authority: ['admin', 'member'],
            hideInMenu: true,
            component: './Cognus/ScreenPage',
          },
        ],
      },
      {
        component: '404',
      },
    ],
  },
];
