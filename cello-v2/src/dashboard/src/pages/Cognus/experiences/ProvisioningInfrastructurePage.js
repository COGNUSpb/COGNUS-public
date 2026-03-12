import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Collapse,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Tag,
  Tooltip,
  Typography,
  Upload,
  message,
} from 'antd';
import {
  ArrowLeftOutlined,
  ArrowRightOutlined,
  DownloadOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import { history } from 'umi';
import NeoOpsLayout from '../components/NeoOpsLayout';
import styles from '../components/NeoOpsLayout.less';
import { getProvisioningActionReadiness } from './provisioningBackendReadiness';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import {
  PROVISIONING_INFRA_SCREEN_KEY,
  PROVISIONING_SCREEN_PATH_BY_KEY,
} from '../data/provisioningContract';
import {
  BLUEPRINT_LOCAL_MODE_ENABLED,
  lintBlueprint as lintBlueprintBackend,
  publishBlueprintVersion,
} from '@/services/blueprint';
import { uploadChainCode } from '@/services/chaincode';
import { runbookTechnicalPreflight } from '@/services/runbook';
import {
  inferBlueprintDocumentFormat,
  normalizeBackendLintReport,
  parseBlueprintDocument,
  serializeBlueprintDocument,
} from './provisioningBlueprintUtils';
import {
  DEFAULT_DOCKER_PORT,
  buildInfrastructureIssues,
  isSecureSecretReference,
  getMachineConnectionIssues,
  isValidPort,
  PREFLIGHT_HOST_STATUS,
  PROVISIONING_INFRA_PROVIDER_KEY,
  PROVISIONING_SSH_AUTH_METHOD,
  runInfrastructurePreflight,
} from './provisioningInfrastructureUtils';
import {
  buildBlueprintFromGuidedTopology,
  importGuidedTopologyFromBlueprint,
} from './provisioningTopologyWizardUtils';
import { deterministicRunId } from './provisioningRunbookUtils';
import {
  buildApiExposureTarget,
  buildApiManagementIssues,
  buildApiRegistryEntries,
  buildApiRoutePath,
} from './provisioningApiManagementUtils';
import {
  buildOnboardingRunbookHandoff,
  persistOnboardingRunbookHandoff,
} from './provisioningOnboardingHandoffUtils';
import {
  applyIncrementalExpansionToModel,
  buildIncrementalExpansionIssues,
  buildIncrementalNodeNamingPreview,
  buildIncrementalExpansionPlan,
  INCREMENTAL_NODE_OPERATION_OPTIONS,
  INCREMENTAL_NODE_OPERATION_TYPE,
  createIncrementalApiDraft,
  createIncrementalChannelDraft,
  createIncrementalInstallDraft,
  createIncrementalNodeExpansionDraft,
} from './provisioningIncrementalExpansionUtils';
import {
  OperationalWindowDialog,
  OperationalWindowManagerProvider,
} from '../components/OperationalWindowManager';
import {
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from '../cognusI18n';

const localizeInfrastructureText = (ptBR, enUS, values, localeCandidate) =>
  formatCognusTemplate(ptBR, enUS, values, localeCandidate || resolveCognusLocale());

const createMachineDraft = (index = 1) => ({
  id: `machine-${index}`,
  infraLabel: `machine${index}`,
  hostAddress: '',
  sshUser: '',
  sshPort: 22,
  authMethod: PROVISIONING_SSH_AUTH_METHOD,
  dockerPort: null,
  sshCredentialRef: '',
  sshCredentialPayload: '',
  sshCredentialFingerprint: '',
});

const normalizeUploadRawFile = fileCandidate => {
  if (!fileCandidate || typeof fileCandidate !== 'object') {
    return null;
  }
  return fileCandidate.originFileObj || fileCandidate;
};

const resolvePemUploadSelection = info => {
  const selectedFile =
    (info && info.file) || (info && Array.isArray(info.fileList) ? info.fileList[0] : null);
  if (!selectedFile) {
    return null;
  }

  const rawFile = normalizeUploadRawFile(selectedFile);
  const fileName = String(selectedFile.name || (rawFile && rawFile.name) || '').trim();
  if (!rawFile || !fileName) {
    return null;
  }

  return {
    fileName,
    rawFile,
  };
};

const resolveChaincodePackageSelection = info => {
  const selectedFile =
    (info && info.file) || (info && Array.isArray(info.fileList) ? info.fileList[0] : null);
  if (!selectedFile) {
    return null;
  }

  const rawFile = normalizeUploadRawFile(selectedFile);
  const fileName = String(selectedFile.name || (rawFile && rawFile.name) || '').trim();
  if (!rawFile || !fileName) {
    return null;
  }

  return {
    fileName,
    rawFile,
  };
};

const encodeBase64Buffer = arrayBuffer => {
  const uint8 = new Uint8Array(arrayBuffer);
  let binary = '';
  for (let index = 0; index < uint8.byteLength; index += 1) {
    binary += String.fromCharCode(uint8[index]);
  }
  return window.btoa(binary);
};

const fileToBase64 = file =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = event => {
      try {
        const payload = encodeBase64Buffer(event.target.result);
        resolve(payload);
      } catch (error) {
        reject(error);
      }
    };
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });

const fileToText = file =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = event => {
      resolve(String((event && event.target && event.target.result) || ''));
    };
    reader.onerror = reject;
    reader.readAsText(file);
  });

const computeFingerprintHex = async (content, base64Payload) => {
  if (typeof crypto !== 'undefined' && crypto.subtle && crypto.subtle.digest) {
    const fingerprintBuffer = await crypto.subtle.digest(
      'SHA-256',
      new TextEncoder().encode(content)
    );
    return Array.from(new Uint8Array(fingerprintBuffer))
      .map(byte => byte.toString(16).padStart(2, '0'))
      .join('');
  }
  return (base64Payload || '').slice(0, 32);
};

const DEFAULT_NETWORK_API_PORT = 31522;
const DEFAULT_CA_PORT = 7054;
const DEFAULT_CA_USER = 'ca-admin';
const DEFAULT_PEER_PORT_BASE = 7051;
const DEFAULT_ORDERER_PORT_BASE = 7050;
const DEFAULT_COUCH_PORT = 5984;
const DEFAULT_COUCH_USER = 'couchdb';
const DEFAULT_API_GATEWAY_PORT = 8443;
const DEFAULT_API_GATEWAY_ROUTE_PREFIX = '/api';
const DEFAULT_NETAPI_ROUTE_PREFIX = '/network';

const ORGANIZATION_SERVICE_KEYS = Object.freeze([
  'peer',
  'orderer',
  'ca',
  'couch',
  'apiGateway',
  'netapi',
]);

const SECURE_REFERENCE_LABELS = Object.freeze({
  caPasswordRef: 'CA Password Ref',
  couchAdminPasswordRef: 'Couch Admin Password Ref',
  apiGatewayAuthRef: 'API Gateway Auth Ref',
  netApiAccessRef: 'NetAPI Access Ref',
});

const createOrganizationDraft = (index = 1) => ({
  id: `org-${index}`,
  name: '',
  domain: '',
  label: `org-${index}`,
  networkApiHost: '',
  networkApiPort: DEFAULT_NETWORK_API_PORT,
  netApiHostRef: '',
  netApiRoutePrefix: DEFAULT_NETAPI_ROUTE_PREFIX,
  netApiAccessRef: '',
  apiGatewayHostRef: '',
  apiGatewayPort: DEFAULT_API_GATEWAY_PORT,
  apiGatewayRoutePrefix: DEFAULT_API_GATEWAY_ROUTE_PREFIX,
  apiGatewayAuthRef: '',
  couchHostRef: '',
  couchPort: DEFAULT_COUCH_PORT,
  couchDatabase: '',
  couchAdminUser: DEFAULT_COUCH_USER,
  couchAdminPasswordRef: '',
  caMode: 'internal',
  caName: `ca-org-${index}`,
  caHost: '',
  caHostRef: '',
  caPort: DEFAULT_CA_PORT,
  caUser: '',
  caPasswordRef: '',
  peerPortBase: DEFAULT_PEER_PORT_BASE,
  ordererPortBase: DEFAULT_ORDERER_PORT_BASE,
  peers: 1,
  orderers: 1,
  peerHostRef: '',
  ordererHostRef: '',
});

const createChannelDraft = (index = 1) => ({
  id: `channel-${index}`,
  name: '',
  memberOrgs: '',
});

const createChaincodeInstallDraft = (index = 1) => ({
  id: `install-${index}`,
  channel: '',
  chaincodeName: '',
  endpointPath: '',
  packageFileName: '',
  packagePattern: '',
  artifactRef: '',
  packageId: '',
  packageLabel: '',
  packageLanguage: '',
  uploadStatus: 'idle',
  uploadError: '',
});

const createApiRegistrationDraft = (index = 1) => ({
  id: `api-${index}`,
  organizationName: '',
  channel: '',
  chaincodeId: '*',
  routePath: '',
  exposureHost: '',
  exposurePort: null,
  status: 'active',
});

const buildEnvironmentProfileOptions = localeCandidate => [
  {
    value: 'dev-external-linux',
    label: pickCognusText(
      'Desenvolvimento (dev-external-linux)',
      'Development (dev-external-linux)',
      localeCandidate
    ),
  },
  {
    value: 'hml-external-linux',
    label: pickCognusText(
      'Homologação (hml-external-linux)',
      'Homologation (hml-external-linux)',
      localeCandidate
    ),
  },
  {
    value: 'prod-external-linux',
    label: pickCognusText(
      'Produção (prod-external-linux)',
      'Production (prod-external-linux)',
      localeCandidate
    ),
  },
];

const ENVIRONMENT_PROFILE_VALUE_SET = new Set(
  ['dev-external-linux', 'hml-external-linux', 'prod-external-linux']
);

const PEM_HELP_SCRIPT = localizeInfrastructureText(
  `#!/usr/bin/env bash
set -euo pipefail

# Parâmetros genéricos (edite)
KEY_FILE="<nome-da-chave>.pem"
USER_HOST="<usuario>@<ip-ou-host>"
PORT="<porta>"

# Gerar chave RSA 4096 em PEM, sem passphrase
ssh-keygen -t rsa -b 4096 -m PEM -N "" -f "$KEY_FILE"

# Permissões seguras para a chave privada
chmod 600 "$KEY_FILE"

# Cadastrar a chave pública no servidor (pode pedir senha uma vez)
ssh-copy-id -i "\${KEY_FILE}.pub" -p "$PORT" "$USER_HOST"

# Testar conexão usando somente chave (sem senha)
ssh -i "$KEY_FILE" -p "$PORT" \
  -o PreferredAuthentications=publickey \
  -o PasswordAuthentication=no \
  "$USER_HOST"`,
  `#!/usr/bin/env bash
set -euo pipefail

# Generic parameters (edit them)
KEY_FILE="<key-file-name>.pem"
USER_HOST="<user>@<ip-or-host>"
PORT="<port>"

# Generate a 4096-bit RSA key in PEM format, without passphrase
ssh-keygen -t rsa -b 4096 -m PEM -N "" -f "$KEY_FILE"

# Secure permissions for the private key
chmod 600 "$KEY_FILE"

# Register the public key on the server (it may ask for the password once)
ssh-copy-id -i "\${KEY_FILE}.pub" -p "$PORT" "$USER_HOST"

# Test the connection using only the key (no password)
ssh -i "$KEY_FILE" -p "$PORT" \
  -o PreferredAuthentications=publickey \
  -o PasswordAuthentication=no \
  "$USER_HOST"`
);

const blueprintExportFormatOptions = [
  { value: 'json', label: 'JSON' },
  { value: 'yaml', label: 'YAML' },
];

const INFRA_ONBOARDING_STORAGE_KEY = 'cognus.provisioning.infra.onboarding.v2';
const INFRA_ONBOARDING_STORAGE_KEY_LEGACY = 'cognus.provisioning.infra.onboarding.v1';
const RUNBOOK_AUDIT_HISTORY_STORAGE_KEY = 'cognus.provisioning.runbook.audit.history.v2';
const RUNBOOK_AUDIT_SELECTED_STORAGE_KEY = 'cognus.provisioning.runbook.audit.selected.v1';
const { Panel: CollapsePanel } = Collapse;

const getEnvironmentProfileHelp = profile => {
  const normalizedProfile = String(profile || '')
    .trim()
    .toLowerCase();
  if (normalizedProfile.startsWith('prod')) {
    return pickCognusText(
      'Produção: baseline mais rígido para execução operacional e evidências finais.',
      'Production: stricter baseline for operational execution and final evidence.'
    );
  }
  if (normalizedProfile.startsWith('hml')) {
    return pickCognusText(
      'Homologação: validação pré-produção com escopo external-linux e rastreabilidade completa.',
      'Homologation: pre-production validation with external-linux scope and full traceability.'
    );
  }
  return pickCognusText(
    'Desenvolvimento: fluxo guiado para modelagem e validação técnica inicial no escopo external-linux.',
    'Development: guided flow for modeling and initial technical validation in the external-linux scope.'
  );
};

const buildInfraWizardSteps = localeCandidate => [
  {
    key: 'infra',
    title: pickCognusText('Infra e preflight', 'Infra and preflight', localeCandidate),
  },
  { key: 'organizations', title: pickCognusText('Organizations', 'Organizations', localeCandidate) },
  {
    key: 'nodes',
    title: pickCognusText('Nodes (peers/orderers)', 'Nodes (peers/orderers)', localeCandidate),
  },
  {
    key: 'business-groups',
    title: pickCognusText('Business Groups', 'Business Groups', localeCandidate),
  },
  { key: 'channels', title: pickCognusText('Channels', 'Channels', localeCandidate) },
  {
    key: 'install',
    title: pickCognusText(
      'Install Chaincodes (.tar.gz)',
      'Install Chaincodes (.tar.gz)',
      localeCandidate
    ),
  },
];

const PREFLIGHT_STATUS_COLOR = {
  [PREFLIGHT_HOST_STATUS.apto]: 'green',
  [PREFLIGHT_HOST_STATUS.parcial]: 'gold',
  [PREFLIGHT_HOST_STATUS.bloqueado]: 'red',
};

const buildPreflightStatusLabel = localeCandidate => ({
  [PREFLIGHT_HOST_STATUS.apto]: pickCognusText('Apto', 'Ready', localeCandidate),
  [PREFLIGHT_HOST_STATUS.parcial]: pickCognusText('Parcial', 'Partial', localeCandidate),
  [PREFLIGHT_HOST_STATUS.bloqueado]: pickCognusText('Bloqueado', 'Blocked', localeCandidate),
});

const getRuntimeActiveContainers = row => {
  const runtimeSnapshot =
    row && typeof row.runtimeSnapshot === 'object' ? row.runtimeSnapshot : null;
  if (!runtimeSnapshot) {
    return [];
  }

  let rawContainers = [];
  if (Array.isArray(runtimeSnapshot.activeContainers)) {
    rawContainers = runtimeSnapshot.activeContainers;
  } else if (Array.isArray(runtimeSnapshot.active_containers)) {
    rawContainers = runtimeSnapshot.active_containers;
  }

  return rawContainers.map(containerName => String(containerName || '').trim()).filter(Boolean);
};

const hasContainerConflictSignal = row => {
  const activeContainers = getRuntimeActiveContainers(row);
  if (activeContainers.length > 0) {
    return true;
  }

  const causeText = `${String(row?.primaryCause || '').toLowerCase()} ${String(
    row?.primaryRecommendation || ''
  ).toLowerCase()}`;

  if (causeText.includes('sem container ativo') || causeText.includes('no active container')) {
    return false;
  }

  return (
    causeText.includes('container') &&
    (causeText.includes('ativo') || causeText.includes('active') || causeText.includes('running'))
  );
};

const buildAutoChangeId = (date = new Date()) => {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  return `cr-${year}${month}${day}-${hours}${minutes}${seconds}`;
};

const toText = value => String(value || '').trim();

const toArray = value => (Array.isArray(value) ? value : []);

const normalizeEnvironmentProfile = value => {
  const normalized = toText(value).toLowerCase();
  if (ENVIRONMENT_PROFILE_VALUE_SET.has(normalized)) {
    return normalized;
  }
  return 'dev-external-linux';
};

const parseCsv = value =>
  toText(value)
    .split(',')
    .map(token => toText(token))
    .filter(Boolean);

const isTarGzPackageName = value => {
  const packageName = toText(value).toLowerCase();
  return Boolean(packageName) && packageName.endsWith('.tar.gz');
};

const getBrowserLocalStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.localStorage || null;
  } catch (error) {
    return null;
  }
};

const getBrowserSessionStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.sessionStorage || null;
  } catch (error) {
    return null;
  }
};

const toSafePositiveInt = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizeAuditOrganizations = organizations =>
  (Array.isArray(organizations) ? organizations : [])
    .map(entry => ({
      orgId: String(entry && entry.orgId ? entry.orgId : '').trim(),
      orgName: String(entry && entry.orgName ? entry.orgName : '').trim(),
      nodeCount: toSafePositiveInt(entry && entry.nodeCount),
      peerCount: toSafePositiveInt(entry && entry.peerCount),
      ordererCount: toSafePositiveInt(entry && entry.ordererCount),
      caCount: toSafePositiveInt(entry && entry.caCount),
      apiCount: toSafePositiveInt(entry && entry.apiCount),
      channels: (Array.isArray(entry && entry.channels) ? entry.channels : [])
        .map(channelId => String(channelId || '').trim())
        .filter(Boolean),
      chaincodes: (Array.isArray(entry && entry.chaincodes) ? entry.chaincodes : [])
        .map(chaincodeId => String(chaincodeId || '').trim())
        .filter(Boolean),
    }))
    .filter(entry => entry.orgId || entry.orgName);

const localizeInfraOperationalHint = (text, localeCandidate) => {
  const normalizedText = toText(text);
  if (!normalizedText) {
    return '';
  }

  if (normalizedText === 'Sem pendências técnicas.' || normalizedText === 'Sem pendências técnicas (modo local).') {
    return pickCognusText(normalizedText, 'No technical issues.', localeCandidate);
  }

  if (normalizedText === 'Sem ação imediata.') {
    return pickCognusText('Sem ação imediata.', 'No immediate action.', localeCandidate);
  }

  if (normalizedText === 'Sem ação necessária.') {
    return pickCognusText('Sem ação necessária.', 'No action required.', localeCandidate);
  }

  return normalizedText;
};

const resolveAuditHistoryTone = (status, localeCandidate) => {
  const normalizedStatus = String(status || '')
    .trim()
    .toLowerCase();
  if (normalizedStatus === 'completed') {
    return { label: pickCognusText('consolidado', 'consolidated', localeCandidate), color: 'green' };
  }
  if (normalizedStatus === 'failed') {
    return { label: pickCognusText('falha', 'failed', localeCandidate), color: 'red' };
  }
  return { label: pickCognusText('parcial', 'partial', localeCandidate), color: 'orange' };
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

const downloadTextFile = (fileName, content, mimeType = 'text/plain;charset=utf-8') => {
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return;
  }

  const blob = new Blob([String(content || '')], { type: mimeType });
  const downloadUrl = window.URL.createObjectURL(blob);
  const anchor = document.createElement('a');

  anchor.href = downloadUrl;
  anchor.download = fileName;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  window.URL.revokeObjectURL(downloadUrl);
};

const toSafePort = (value, fallback) => {
  const parsed = Number(value);
  if (isValidPort(parsed)) {
    return parsed;
  }
  return fallback;
};

const toSlugToken = value =>
  toText(value)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

const buildDefaultChaincodeEndpointPath = (channel, chaincodeName) => {
  const channelToken = toSlugToken(channel) || 'channel';
  const chaincodeToken = toSlugToken(chaincodeName) || 'chaincode';
  return `/api/${channelToken}/${chaincodeToken}/query/getTx`;
};

const normalizeChaincodeUploadPayload = response => {
  const payload = response && typeof response.data === 'object' ? response.data : {};
  return {
    packageId: String(payload.package_id || '').trim(),
    packageLabel: String(payload.label || '').trim(),
    packageLanguage: String(payload.language || '').trim(),
    packageFileName: String(payload.file_name || '').trim(),
    artifactRef: String(payload.artifact_ref || '').trim(),
    alreadyExists: Boolean(payload.already_exists),
  };
};

const extractRequestErrorMessage = error => {
  if (!error) {
    return 'Falha inesperada.';
  }
  if (typeof error === 'string') {
    return error;
  }
  if (typeof error?.data?.msg === 'string' && error.data.msg.trim()) {
    return error.data.msg.trim();
  }
  if (typeof error?.message === 'string' && error.message.trim()) {
    return error.message.trim();
  }
  return 'Falha inesperada.';
};

const OFFICIAL_BLUEPRINT_ENDPOINTS_ENABLED = !BLUEPRINT_LOCAL_MODE_ENABLED;

const normalizeTechnicalPreflightStatus = value => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (
    normalized === PREFLIGHT_HOST_STATUS.apto ||
    normalized === PREFLIGHT_HOST_STATUS.parcial ||
    normalized === PREFLIGHT_HOST_STATUS.bloqueado
  ) {
    return normalized;
  }
  return PREFLIGHT_HOST_STATUS.bloqueado;
};

const normalizeTechnicalPreflightReport = ({ report, machines, changeId, executedAtUtc }) => {
  const safeReport = report && typeof report === 'object' ? report : {};
  const machineRegistry = (Array.isArray(machines) ? machines : []).reduce((registry, machine) => {
    const key = String(machine.infraLabel || '').trim();
    if (key) {
      return {
        ...registry,
        [key]: machine,
      };
    }
    return registry;
  }, {});

  const hosts = (Array.isArray(safeReport.hosts) ? safeReport.hosts : []).map((host, index) => {
    const hostRef = String(host.host_ref || host.infra_label || host.host_id || '').trim();
    const machine = machineRegistry[hostRef] || null;
    const diagnostics = host && typeof host.diagnostics === 'object' ? host.diagnostics : {};
    let rawRuntimeSnapshot = null;
    if (host && typeof host.runtime_snapshot === 'object') {
      rawRuntimeSnapshot = host.runtime_snapshot;
    } else if (host && typeof host.runtimeSnapshot === 'object') {
      rawRuntimeSnapshot = host.runtimeSnapshot;
    }

    let runtimeActiveContainers = [];
    if (rawRuntimeSnapshot && Array.isArray(rawRuntimeSnapshot.active_containers)) {
      runtimeActiveContainers = rawRuntimeSnapshot.active_containers;
    } else if (rawRuntimeSnapshot && Array.isArray(rawRuntimeSnapshot.activeContainers)) {
      runtimeActiveContainers = rawRuntimeSnapshot.activeContainers;
    }
    runtimeActiveContainers = runtimeActiveContainers
      .map(containerName => String(containerName || '').trim())
      .filter(Boolean);

    const normalizedRuntimeSnapshot = rawRuntimeSnapshot
      ? {
          ...rawRuntimeSnapshot,
          activeContainers: runtimeActiveContainers,
          activeContainersCount:
            Number(
              rawRuntimeSnapshot.active_containers_count ||
                rawRuntimeSnapshot.activeContainersCount ||
                runtimeActiveContainers.length
            ) || 0,
        }
      : null;

    return {
      id: String(host.host_id || hostRef || `machine-${index + 1}`).trim(),
      infraLabel: hostRef || String(machine?.infraLabel || `machine${index + 1}`).trim(),
      hostAddress: String(host.host_address || machine?.hostAddress || '').trim(),
      sshUser: String(host.ssh_user || machine?.sshUser || '').trim(),
      sshPort: Number(host.ssh_port || machine?.sshPort || 22) || 22,
      dockerPort: toSafePort(host?.docker_port || machine?.dockerPort, DEFAULT_DOCKER_PORT),
      status: normalizeTechnicalPreflightStatus(host.status),
      checkedAtUtc: String(host.checked_at_utc || executedAtUtc || '').trim(),
      checks: [],
      failures: [],
      warnings: [],
      primaryCause: String(host.primary_cause || '').trim(),
      primaryRecommendation: String(host.recommended_action || '').trim(),
      runtimeSnapshot: normalizedRuntimeSnapshot,
      diagnostics: {
        code: String(diagnostics.code || '').trim(),
        stderr: String(diagnostics.stderr || '').trim(),
        stdout: String(diagnostics.stdout || '').trim(),
        exitCode: Number(diagnostics.exit_code || 0),
      },
    };
  });

  const computedSummary = hosts.reduce(
    (summary, host) => {
      if (host.status === PREFLIGHT_HOST_STATUS.apto) {
        return {
          ...summary,
          apto: summary.apto + 1,
        };
      }

      if (host.status === PREFLIGHT_HOST_STATUS.parcial) {
        return {
          ...summary,
          parcial: summary.parcial + 1,
        };
      }
      return {
        ...summary,
        bloqueado: summary.bloqueado + 1,
      };
    },
    { apto: 0, parcial: 0, bloqueado: 0 }
  );

  return {
    changeId: String(safeReport.change_id || changeId || '').trim(),
    executedAtUtc: String(safeReport.executed_at_utc || executedAtUtc || '').trim(),
    overallStatus: normalizeTechnicalPreflightStatus(safeReport.overall_status),
    summary: {
      apto: Number(safeReport.summary?.apto ?? computedSummary.apto),
      parcial: Number(safeReport.summary?.parcial ?? computedSummary.parcial),
      bloqueado: Number(safeReport.summary?.bloqueado ?? computedSummary.bloqueado),
    },
    hosts,
  };
};

const normalizeRoutePrefix = (value, fallback) => {
  const normalized = toText(value);
  if (!normalized) {
    return toText(fallback);
  }
  const prefixed = normalized.startsWith('/') ? normalized : `/${normalized}`;
  return prefixed.replace(/\/{2,}/g, '/');
};

const isValidRoutePrefix = value => /^\/[a-z0-9/_-]*$/i.test(toText(value));

const resolveOrganizationServiceHostMapping = organization => ({
  peer: toText(organization && organization.peerHostRef),
  orderer: toText(organization && organization.ordererHostRef),
  ca: toText(organization && organization.caHostRef),
  couch: toText(organization && organization.couchHostRef),
  apiGateway: toText(organization && organization.apiGatewayHostRef),
  netapi: toText(organization && organization.netApiHostRef),
});

const resolveOrganizationServiceParameters = organization => ({
  peer: {
    count: Math.max(0, Number(organization && organization.peers) || 0),
    port_base: toSafePort(organization && organization.peerPortBase, DEFAULT_PEER_PORT_BASE),
  },
  orderer: {
    count: Math.max(0, Number(organization && organization.orderers) || 0),
    port_base: toSafePort(organization && organization.ordererPortBase, DEFAULT_ORDERER_PORT_BASE),
  },
  ca: {
    mode: toText(organization && organization.caMode) || 'internal',
    name: toText(organization && organization.caName),
    host: toText(organization && organization.caHost),
    port: toSafePort(organization && organization.caPort, DEFAULT_CA_PORT),
    user: toText(organization && organization.caUser),
    password_ref: toText(organization && organization.caPasswordRef),
  },
  couch: {
    host_ref: toText(organization && organization.couchHostRef),
    port: toSafePort(organization && organization.couchPort, DEFAULT_COUCH_PORT),
    database: toText(organization && organization.couchDatabase),
    admin_user: toText(organization && organization.couchAdminUser),
    admin_password_ref: toText(organization && organization.couchAdminPasswordRef),
  },
  apiGateway: {
    host_ref: toText(organization && organization.apiGatewayHostRef),
    port: toSafePort(organization && organization.apiGatewayPort, DEFAULT_API_GATEWAY_PORT),
    route_prefix: normalizeRoutePrefix(
      organization && organization.apiGatewayRoutePrefix,
      DEFAULT_API_GATEWAY_ROUTE_PREFIX
    ),
    auth_ref: toText(organization && organization.apiGatewayAuthRef),
  },
  netapi: {
    host: toText(organization && organization.networkApiHost),
    host_ref: toText(organization && organization.netApiHostRef),
    port: toSafePort(organization && organization.networkApiPort, DEFAULT_NETWORK_API_PORT),
    route_prefix: normalizeRoutePrefix(
      organization && organization.netApiRoutePrefix,
      DEFAULT_NETAPI_ROUTE_PREFIX
    ),
    access_ref: toText(organization && organization.netApiAccessRef),
  },
});

const resolveOrganizationServiceMatrixRows = organizations =>
  toArray(organizations).flatMap(organization => {
    const organizationName =
      toText(organization && organization.name) || toText(organization && organization.id);
    const serviceHostMapping = resolveOrganizationServiceHostMapping(organization);
    const serviceParameters = resolveOrganizationServiceParameters(organization);

    return ORGANIZATION_SERVICE_KEYS.map(serviceKey => ({
      key: `${organizationName || 'org'}-${serviceKey}`,
      organizationName: organizationName || '-',
      serviceKey,
      hostRef: serviceHostMapping[serviceKey] || '-',
      parameterSummary: JSON.stringify(serviceParameters[serviceKey] || {}),
    }));
  });
const isBackendBlueprintEndpointUnavailable = error => {
  const statusCode = Number(error?.response?.status || error?.status || 0);
  if ([404, 405, 500, 502, 503, 504].includes(statusCode)) {
    return true;
  }

  const errorMessage = String(error?.message || '').toLowerCase();
  return [
    'network',
    'timeout',
    'failed to fetch',
    'econnrefused',
    'service unavailable',
  ].some(token => errorMessage.includes(token));
};

const isBackendBlueprintResponseUnavailable = response => {
  const statusCode = Number(
    response?.response?.status || response?.statusCode || response?.status || 0
  );
  if ([404, 405, 500, 502, 503, 504].includes(statusCode)) {
    return true;
  }

  const serializedPayload = String(response?.data || response?.message || '').toLowerCase();
  return [
    '<!doctype html',
    '<html',
    'page not found',
    'didn’t match any',
    "didn't match any",
  ].some(token => serializedPayload.includes(token));
};

const tryParseJsonPayload = value => {
  if (typeof value !== 'string') {
    return null;
  }

  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (error) {
    return null;
  }
};

const toErrorDetailText = value => {
  if (Array.isArray(value)) {
    return value
      .map(item => toErrorDetailText(item))
      .filter(Boolean)
      .join(' | ');
  }

  if (!value && value !== 0) {
    return '';
  }

  if (typeof value === 'string') {
    return value.trim();
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  if (typeof value === 'object') {
    const detailText =
      toErrorDetailText(value.detail) ||
      toErrorDetailText(value.message) ||
      toErrorDetailText(value.msg);
    if (detailText) {
      return detailText;
    }

    try {
      return JSON.stringify(value);
    } catch (error) {
      return '';
    }
  }

  return '';
};

const resolvePublishIntegrationError = error => {
  const statusCode = Number(error?.response?.status || error?.status || 0);
  const normalizedMessage = toText(error?.message);
  const payloadCandidates = [
    error?.data,
    error?.response?.data,
    error?.info?.data,
    error?.info,
    tryParseJsonPayload(error?.message),
  ].filter(candidate => candidate && typeof candidate === 'object');

  let backendCode = '';
  let backendDetail = '';
  let tokenInvalid = statusCode === 401 || statusCode === 403;

  payloadCandidates.forEach(payload => {
    if (!backendCode) {
      backendCode =
        toText(payload.code) ||
        toText(payload.msg) ||
        toText(payload.error) ||
        toText(payload.error_code);
    }

    if (!backendDetail) {
      backendDetail =
        toErrorDetailText(payload.detail) ||
        toErrorDetailText(payload.message) ||
        toErrorDetailText(payload.messages) ||
        toErrorDetailText(payload.errors) ||
        toErrorDetailText(payload.msg);
    }

    const messageBundle = [
      toText(payload.code),
      toText(payload.detail),
      toText(payload.message),
      toText(payload.msg),
      toErrorDetailText(payload.messages),
      toErrorDetailText(payload.errors),
    ]
      .join(' ')
      .toLowerCase();

    if (
      messageBundle.includes('token') &&
      (messageBundle.includes('invalid') ||
        messageBundle.includes('expired') ||
        messageBundle.includes('not valid'))
    ) {
      tokenInvalid = true;
    }
  });

  const normalizedBackendDetail = toText(backendDetail);
  const normalizedBackendCode = toText(backendCode);
  const locale = resolveCognusLocale();
  const fallbackErrorMessage =
    normalizedMessage && normalizedMessage.toLowerCase() !== 'response error'
      ? normalizedMessage
      : pickCognusText(
          'erro inesperado durante a publicação do blueprint.',
          'unexpected error during blueprint publication.',
          locale
        );

  const userMessage = tokenInvalid
    ? pickCognusText(
        'Sessão expirada ou token inválido para publicar blueprint. Faça login novamente e repita a operação.',
        'Session expired or invalid token while publishing the blueprint. Log in again and retry the operation.',
        locale
      )
    : normalizedBackendDetail || fallbackErrorMessage;

  const diagnostics = [];
  if (statusCode > 0) {
    diagnostics.push(`HTTP ${statusCode}`);
  }
  if (normalizedBackendCode) {
    diagnostics.push(`codigo=${normalizedBackendCode}`);
  }
  if (normalizedBackendDetail && normalizedBackendDetail !== userMessage) {
    diagnostics.push(normalizedBackendDetail);
  }
  if (
    normalizedMessage &&
    normalizedMessage !== userMessage &&
    normalizedMessage.toLowerCase() !== 'response error'
  ) {
    diagnostics.push(normalizedMessage);
  }

  return {
    userMessage,
    statusCode,
    backendCode: normalizedBackendCode,
    backendDetail: normalizedBackendDetail,
    tokenInvalid,
    diagnostics: diagnostics.join(' | '),
  };
};

const normalizeMachineState = (machine, index) => {
  const fallback = createMachineDraft(index + 1);
  return {
    ...fallback,
    ...machine,
    id: toText(machine && machine.id) || fallback.id,
    infraLabel: toText(machine && machine.infraLabel) || fallback.infraLabel,
    hostAddress: toText(machine && machine.hostAddress),
    sshUser: toText(machine && machine.sshUser) || fallback.sshUser,
    sshPort: toSafePort(machine && machine.sshPort, fallback.sshPort),
    authMethod: PROVISIONING_SSH_AUTH_METHOD,
    dockerPort: toSafePort(machine && machine.dockerPort, fallback.dockerPort),
    sshCredentialRef: toText(machine && machine.sshCredentialRef),
    sshCredentialPayload: toText(machine && machine.sshCredentialPayload),
    sshCredentialFingerprint: toText(machine && machine.sshCredentialFingerprint),
  };
};

const normalizeOrganizationState = (organization, index) => {
  const fallback = createOrganizationDraft(index + 1);
  return {
    ...fallback,
    ...organization,
    id: toText(organization && organization.id) || fallback.id,
    name: toText(organization && organization.name),
    domain: toText(organization && organization.domain),
    label: toText(organization && organization.label) || fallback.label,
    networkApiHost: toText(organization && organization.networkApiHost),
    networkApiPort: toSafePort(
      organization && organization.networkApiPort,
      fallback.networkApiPort
    ),
    netApiHostRef: toText(organization && organization.netApiHostRef),
    netApiRoutePrefix: normalizeRoutePrefix(
      organization && organization.netApiRoutePrefix,
      fallback.netApiRoutePrefix
    ),
    netApiAccessRef: toText(organization && organization.netApiAccessRef),
    apiGatewayHostRef: toText(organization && organization.apiGatewayHostRef),
    apiGatewayPort: toSafePort(
      organization && organization.apiGatewayPort,
      fallback.apiGatewayPort
    ),
    apiGatewayRoutePrefix: normalizeRoutePrefix(
      organization && organization.apiGatewayRoutePrefix,
      fallback.apiGatewayRoutePrefix
    ),
    apiGatewayAuthRef: toText(organization && organization.apiGatewayAuthRef),
    couchHostRef: toText(organization && organization.couchHostRef),
    couchPort: toSafePort(organization && organization.couchPort, fallback.couchPort),
    couchDatabase: toText(organization && organization.couchDatabase),
    couchAdminUser: toText(organization && organization.couchAdminUser) || fallback.couchAdminUser,
    couchAdminPasswordRef: toText(organization && organization.couchAdminPasswordRef),
    caMode: toText(organization && organization.caMode) || fallback.caMode,
    caName: toText(organization && organization.caName) || fallback.caName,
    caHost: toText(organization && organization.caHost),
    caHostRef: toText(organization && organization.caHostRef),
    caPort: toSafePort(organization && organization.caPort, fallback.caPort),
    caUser: toText(organization && organization.caUser) || fallback.caUser || DEFAULT_CA_USER,
    caPasswordRef: toText(organization && organization.caPasswordRef),
    peerPortBase: toSafePort(organization && organization.peerPortBase, fallback.peerPortBase),
    ordererPortBase: toSafePort(
      organization && organization.ordererPortBase,
      fallback.ordererPortBase
    ),
    peers: Number.isInteger(Number(organization && organization.peers))
      ? Math.max(0, Number(organization.peers))
      : fallback.peers,
    orderers: Number.isInteger(Number(organization && organization.orderers))
      ? Math.max(0, Number(organization.orderers))
      : fallback.orderers,
    peerHostRef: toText(organization && organization.peerHostRef),
    ordererHostRef: toText(organization && organization.ordererHostRef),
  };
};

const normalizeSimpleRow = (row, fallbackFactory, index) => ({
  ...fallbackFactory(index + 1),
  ...(row || {}),
  id: toText(row && row.id) || fallbackFactory(index + 1).id,
});

const applyOrganizationAutoDefaults = (organization, fallbackHostRef = '') => {
  const name = toText(organization && organization.name);
  const domain = toText(organization && organization.domain).toLowerCase();
  const currentLabel = toText(organization && organization.label);
  const tokenBase =
    toSlugToken(currentLabel || name || toText(organization && organization.id)) || 'org';
  const next = { ...organization };

  if (!currentLabel && name) {
    next.label = `org-${toSlugToken(name) || tokenBase}`;
  }

  if (!toText(organization && organization.networkApiHost) && domain) {
    next.networkApiHost = `netapi.${domain}`;
  }

  if (!toText(organization && organization.caName)) {
    next.caName = `ca-${tokenBase}`;
  }

  if (!toText(organization && organization.caHost) && domain) {
    next.caHost = `ca.${domain}`;
  }

  if (!toText(organization && organization.caUser)) {
    next.caUser = DEFAULT_CA_USER;
  }

  if (!toText(organization && organization.caPasswordRef)) {
    next.caPasswordRef = `vault://ca/${tokenBase}`;
  }

  const peers = Number(organization && organization.peers);
  const orderers = Number(organization && organization.orderers);

  if (fallbackHostRef && peers > 0 && !toText(organization && organization.peerHostRef)) {
    next.peerHostRef = fallbackHostRef;
  }

  if (fallbackHostRef && orderers > 0 && !toText(organization && organization.ordererHostRef)) {
    next.ordererHostRef = fallbackHostRef;
  }

  if (fallbackHostRef && !toText(organization && organization.caHostRef)) {
    next.caHostRef = toText(organization && organization.peerHostRef) || fallbackHostRef;
  }

  if (fallbackHostRef && !toText(organization && organization.couchHostRef)) {
    next.couchHostRef = toText(organization && organization.peerHostRef) || fallbackHostRef;
  }

  if (fallbackHostRef && !toText(organization && organization.apiGatewayHostRef)) {
    next.apiGatewayHostRef = toText(organization && organization.peerHostRef) || fallbackHostRef;
  }

  if (fallbackHostRef && !toText(organization && organization.netApiHostRef)) {
    next.netApiHostRef =
      toText(organization && organization.apiGatewayHostRef) ||
      toText(organization && organization.peerHostRef) ||
      fallbackHostRef;
  }

  if (!toText(organization && organization.netApiRoutePrefix)) {
    next.netApiRoutePrefix = DEFAULT_NETAPI_ROUTE_PREFIX;
  }

  if (!toText(organization && organization.netApiAccessRef)) {
    next.netApiAccessRef = `vault://netapi/${tokenBase}/access`;
  }

  if (!toText(organization && organization.apiGatewayRoutePrefix)) {
    next.apiGatewayRoutePrefix = DEFAULT_API_GATEWAY_ROUTE_PREFIX;
  }

  if (!toText(organization && organization.apiGatewayAuthRef)) {
    next.apiGatewayAuthRef = `vault://apigateway/${tokenBase}/auth`;
  }

  if (!toText(organization && organization.couchDatabase)) {
    next.couchDatabase = `${tokenBase}_ledger`;
  }

  if (!toText(organization && organization.couchAdminUser)) {
    next.couchAdminUser = DEFAULT_COUCH_USER;
  }

  if (!toText(organization && organization.couchAdminPasswordRef)) {
    next.couchAdminPasswordRef = `vault://couch/${tokenBase}/admin`;
  }

  if (!isValidPort(Number(organization && organization.networkApiPort))) {
    next.networkApiPort = DEFAULT_NETWORK_API_PORT;
  }

  if (!isValidPort(Number(organization && organization.couchPort))) {
    next.couchPort = DEFAULT_COUCH_PORT;
  }

  if (!isValidPort(Number(organization && organization.apiGatewayPort))) {
    next.apiGatewayPort = DEFAULT_API_GATEWAY_PORT;
  }

  if (!isValidPort(Number(organization && organization.peerPortBase))) {
    next.peerPortBase = DEFAULT_PEER_PORT_BASE;
  }

  if (!isValidPort(Number(organization && organization.ordererPortBase))) {
    next.ordererPortBase = DEFAULT_ORDERER_PORT_BASE;
  }

  return next;
};

const buildBusinessGroupNetworkIdDefault = ({ businessGroupName, organizations, changeId }) => {
  const fromBusinessGroup = toSlugToken(businessGroupName);
  if (fromBusinessGroup) {
    return fromBusinessGroup;
  }

  const fromOrganization = toSlugToken(
    toArray(organizations)
      .map(organization => toText(organization && organization.name))
      .find(Boolean)
  );
  if (fromOrganization) {
    return `${fromOrganization}-network`;
  }

  const fromChangeId = toSlugToken(changeId);
  if (fromChangeId) {
    return `${fromChangeId}-network`;
  }

  return 'cognus-network';
};

const updateCollectionRow = (rows, rowId, field, value) =>
  rows.map(row => (row.id === rowId ? { ...row, [field]: value } : row));

const RUNBOOK_ROUTE_PATH = PROVISIONING_SCREEN_PATH_BY_KEY['e1-provisionamento'];

const ProvisioningInfrastructurePage = () => {
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale),
    [locale]
  );
  const environmentProfileOptions = useMemo(
    () => buildEnvironmentProfileOptions(locale),
    [locale]
  );
  const infraWizardSteps = useMemo(() => buildInfraWizardSteps(locale), [locale]);
  const preflightStatusLabel = useMemo(() => buildPreflightStatusLabel(locale), [locale]);
  const [changeId, setChangeId] = useState(() => buildAutoChangeId());
  const [environmentProfile, setEnvironmentProfile] = useState(() =>
    normalizeEnvironmentProfile('dev-external-linux')
  );
  const [showEnvironmentSelector, setShowEnvironmentSelector] = useState(false);
  const [showPemHelp, setShowPemHelp] = useState(false);
  const [machines, setMachines] = useState(() => [createMachineDraft(1)]);
  const [organizations, setOrganizations] = useState(() => [createOrganizationDraft(1)]);
  const [channels, setChannels] = useState([]);
  const [chaincodeInstalls, setChaincodeInstalls] = useState([]);
  const [apiRegistrations, setApiRegistrations] = useState([]);
  const [incrementalNodeExpansions, setIncrementalNodeExpansions] = useState([]);
  const [incrementalChannelAdditions, setIncrementalChannelAdditions] = useState([]);
  const [incrementalInstallAdditions, setIncrementalInstallAdditions] = useState([]);
  const [incrementalApiAdditions, setIncrementalApiAdditions] = useState([]);
  const [incrementalExpansionHistory, setIncrementalExpansionHistory] = useState([]);
  const [businessGroupName, setBusinessGroupName] = useState('');
  const [businessGroupDescription, setBusinessGroupDescription] = useState('');
  const [businessGroupNetworkId, setBusinessGroupNetworkId] = useState('');
  const [activeStepIndex, setActiveStepIndex] = useState(0);
  const [preflightApprovedSignature, setPreflightApprovedSignature] = useState('');
  const [preflightReport, setPreflightReport] = useState(null);
  const [isRunningPreflight, setIsRunningPreflight] = useState(false);
  const [blueprintExportFormat, setBlueprintExportFormat] = useState('json');
  const [isWizardLinting, setIsWizardLinting] = useState(false);
  const [wizardLintReport, setWizardLintReport] = useState(null);
  const [isPublishingAndOpeningRunbook, setIsPublishingAndOpeningRunbook] = useState(false);
  const [isInfraWizardDialogOpen, setIsInfraWizardDialogOpen] = useState(false);
  const [modelingAuditTrail, setModelingAuditTrail] = useState([]);
  const [provisioningAuditHistory, setProvisioningAuditHistory] = useState([]);
  const latestModelSignatureRef = useRef('');
  const hydrationDoneRef = useRef(false);
  const persistenceWarningShownRef = useRef(false);
  const lintBackendUnavailableRef = useRef(false);

  const validateActionReadiness = useMemo(
    () => getProvisioningActionReadiness(PROVISIONING_INFRA_SCREEN_KEY, 'validate_infra_form'),
    []
  );
  const exportActionReadiness = useMemo(
    () => getProvisioningActionReadiness(PROVISIONING_INFRA_SCREEN_KEY, 'export_infra_seed'),
    []
  );

  const organizationNames = useMemo(
    () => organizations.map(organization => String(organization.name || '').trim()).filter(Boolean),
    [organizations]
  );
  const organizationNameSet = useMemo(() => new Set(organizationNames), [organizationNames]);

  const channelNames = useMemo(
    () => channels.map(channel => String(channel.name || '').trim()).filter(Boolean),
    [channels]
  );
  const channelNameSet = useMemo(() => new Set(channelNames), [channelNames]);
  const activeChaincodeIds = useMemo(
    () =>
      Array.from(
        new Set(
          chaincodeInstalls
            .map(install => String(install.chaincodeName || '').trim())
            .filter(Boolean)
        )
      ),
    [chaincodeInstalls]
  );
  const activeChaincodeIdSet = useMemo(() => new Set(activeChaincodeIds), [activeChaincodeIds]);
  const hostRefOptions = useMemo(
    () =>
      machines
        .map(machine => {
          const label = String(machine.infraLabel || machine.id || '').trim();
          return {
            label,
            value: label,
          };
        })
        .filter(option => Boolean(option.value)),
    [machines]
  );
  const resolveIncrementalDefaultHostRef = useCallback(
    (organizationName, operationType) => {
      const normalizedOrganizationName = toText(organizationName);
      const organization = organizations.find(
        current => toText(current && current.name) === normalizedOrganizationName
      );
      const fallbackHostRef = toText(hostRefOptions[0] && hostRefOptions[0].value);

      if (!organization) {
        return fallbackHostRef;
      }

      if (operationType === INCREMENTAL_NODE_OPERATION_TYPE.addOrderer) {
        return toText(organization.ordererHostRef) || fallbackHostRef;
      }

      return toText(organization.peerHostRef) || fallbackHostRef;
    },
    [organizations, hostRefOptions]
  );
  const organizationServiceMatrixRows = useMemo(
    () => resolveOrganizationServiceMatrixRows(organizations),
    [organizations]
  );

  useEffect(() => {
    const fallbackHostRef = toText(hostRefOptions[0] && hostRefOptions[0].value);

    setOrganizations(current => {
      let hasChanges = false;

      const next = current.map(organization => {
        const withDefaults = applyOrganizationAutoDefaults(organization, fallbackHostRef);

        const changed =
          withDefaults.label !== organization.label ||
          withDefaults.networkApiHost !== organization.networkApiHost ||
          withDefaults.networkApiPort !== organization.networkApiPort ||
          withDefaults.netApiHostRef !== organization.netApiHostRef ||
          withDefaults.netApiRoutePrefix !== organization.netApiRoutePrefix ||
          withDefaults.netApiAccessRef !== organization.netApiAccessRef ||
          withDefaults.apiGatewayHostRef !== organization.apiGatewayHostRef ||
          withDefaults.apiGatewayPort !== organization.apiGatewayPort ||
          withDefaults.apiGatewayRoutePrefix !== organization.apiGatewayRoutePrefix ||
          withDefaults.apiGatewayAuthRef !== organization.apiGatewayAuthRef ||
          withDefaults.couchHostRef !== organization.couchHostRef ||
          withDefaults.couchPort !== organization.couchPort ||
          withDefaults.couchDatabase !== organization.couchDatabase ||
          withDefaults.couchAdminUser !== organization.couchAdminUser ||
          withDefaults.couchAdminPasswordRef !== organization.couchAdminPasswordRef ||
          withDefaults.caName !== organization.caName ||
          withDefaults.caHost !== organization.caHost ||
          withDefaults.caHostRef !== organization.caHostRef ||
          withDefaults.caUser !== organization.caUser ||
          withDefaults.caPasswordRef !== organization.caPasswordRef ||
          withDefaults.peerPortBase !== organization.peerPortBase ||
          withDefaults.ordererPortBase !== organization.ordererPortBase ||
          withDefaults.peerHostRef !== organization.peerHostRef ||
          withDefaults.ordererHostRef !== organization.ordererHostRef;

        if (changed) {
          hasChanges = true;
          return withDefaults;
        }

        return organization;
      });

      return hasChanges ? next : current;
    });
  }, [hostRefOptions, organizations]);

  useEffect(() => {
    if (channelNames.length === 0) {
      return;
    }

    const defaultChannel = channelNames[0];
    setChaincodeInstalls(current => {
      let hasChanges = false;

      const next = current.map(install => {
        const currentChannel = toText(install.channel);
        const resolvedChannel =
          !currentChannel || !channelNameSet.has(currentChannel) ? defaultChannel : currentChannel;
        const resolvedEndpoint =
          toText(install.endpointPath) ||
          buildDefaultChaincodeEndpointPath(resolvedChannel, install.chaincodeName);

        const channelChanged = resolvedChannel !== currentChannel;
        const endpointChanged = resolvedEndpoint !== toText(install.endpointPath);

        if (!channelChanged && !endpointChanged) {
          return install;
        }

        hasChanges = true;
        return {
          ...install,
          channel: resolvedChannel,
          endpointPath: resolvedEndpoint,
        };
      });

      return hasChanges ? next : current;
    });
  }, [channelNames, channelNameSet]);

  useEffect(() => {
    if (toText(businessGroupNetworkId)) {
      return;
    }

    const suggestedNetworkId = buildBusinessGroupNetworkIdDefault({
      businessGroupName,
      organizations,
      changeId,
    });

    if (!suggestedNetworkId) {
      return;
    }

    setBusinessGroupNetworkId(suggestedNetworkId);
  }, [businessGroupNetworkId, businessGroupName, organizations, changeId]);

  const machineCredentialBindings = useMemo(
    () =>
      machines.map(machine => ({
        machine_id: String(machine.infraLabel || machine.id || '').trim(),
        credential_ref: String(machine.sshCredentialRef || '').trim(),
        credential_payload: String(machine.sshCredentialPayload || '').trim(),
        credential_fingerprint: String(machine.sshCredentialFingerprint || '').trim(),
        reuse_confirmed: false,
      })),
    [machines]
  );

  const infrastructureIssues = useMemo(() => {
    return buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId,
      privateKeyRef: '',
      machines,
      machineCredentials: machineCredentialBindings,
    });
  }, [changeId, machines, machineCredentialBindings]);

  const organizationIssues = useMemo(() => {
    const issues = [];
    const machineLabelSet = new Set(
      machines.map(machine => String(machine.infraLabel || '').trim()).filter(Boolean)
    );
    const hasMachineOptions = machineLabelSet.size > 0;

    if (organizations.length === 0) {
      issues.push(
        t(
          'Ao menos uma organização deve ser cadastrada.',
          'At least one organization must be registered.'
        )
      );
    }

    organizations.forEach(organization => {
      const organizationLabel = organization.name || organization.id;
      const serviceHostMapping = resolveOrganizationServiceHostMapping(organization);
      const serviceParameters = resolveOrganizationServiceParameters(organization);

      if (!String(organization.name || '').trim()) {
        issues.push(
          t('Organization Name obrigatório ({id}).', 'Organization Name is required ({id}).', {
            id: organization.id,
          })
        );
      }
      if (!String(organization.domain || '').trim()) {
        issues.push(t('Domain obrigatório para {org}.', 'Domain is required for {org}.', { org: organizationLabel }));
      }
      if (!String(organization.label || '').trim()) {
        issues.push(t('Label obrigatório para {org}.', 'Label is required for {org}.', { org: organizationLabel }));
      }
      if (!String(organization.networkApiHost || '').trim()) {
        issues.push(
          t('Network API Host obrigatório para {org}.', 'Network API Host is required for {org}.', {
            org: organizationLabel,
          })
        );
      }
      if (!isValidPort(organization.networkApiPort)) {
        issues.push(
          t('Network API Port inválida para {org}.', 'Network API Port is invalid for {org}.', {
            org: organizationLabel,
          })
        );
      }
      if (!String(organization.caName || '').trim()) {
        issues.push(t('CA Name obrigatório para {org}.', 'CA Name is required for {org}.', { org: organizationLabel }));
      }
      if (!String(organization.caHost || '').trim()) {
        issues.push(t('CA Host obrigatório para {org}.', 'CA Host is required for {org}.', { org: organizationLabel }));
      }
      if (!isValidPort(organization.caPort)) {
        issues.push(t('CA Port inválida para {org}.', 'CA Port is invalid for {org}.', { org: organizationLabel }));
      }
      if (!String(organization.caUser || '').trim()) {
        issues.push(t('CA User obrigatório para {org}.', 'CA User is required for {org}.', { org: organizationLabel }));
      }
      if (!String(organization.caPasswordRef || '').trim()) {
        issues.push(
          t('CA Password Ref obrigatório para {org}.', 'CA Password Ref is required for {org}.', {
            org: organizationLabel,
          })
        );
      }

      Object.entries(SECURE_REFERENCE_LABELS).forEach(([fieldKey, label]) => {
        const value = toText(organization && organization[fieldKey]);
        if (!value) {
          issues.push(t('{label} obrigatório para {org}.', '{label} is required for {org}.', {
            label,
            org: organizationLabel,
          }));
          return;
        }
        if (!isSecureSecretReference(value)) {
          issues.push(
            t(
              '{label} inválido para {org}; use somente referência segura (ex.: vault://...).',
              '{label} is invalid for {org}; use only a secure reference (for example: vault://...).',
              {
                label,
                org: organizationLabel,
              }
            )
          );
        }
      });

      if (!isValidPort(serviceParameters.couch.port)) {
        issues.push(t('Couch Port inválida para {org}.', 'Couch Port is invalid for {org}.', { org: organizationLabel }));
      }
      if (!isValidPort(serviceParameters.apiGateway.port)) {
        issues.push(
          t('API Gateway Port inválida para {org}.', 'API Gateway Port is invalid for {org}.', {
            org: organizationLabel,
          })
        );
      }
      if (!isValidPort(serviceParameters.peer.port_base)) {
        issues.push(
          t('Peer Port Base inválida para {org}.', 'Peer Port Base is invalid for {org}.', {
            org: organizationLabel,
          })
        );
      }
      if (!isValidPort(serviceParameters.orderer.port_base)) {
        issues.push(
          t('Orderer Port Base inválida para {org}.', 'Orderer Port Base is invalid for {org}.', {
            org: organizationLabel,
          })
        );
      }

      if (!isValidRoutePrefix(serviceParameters.apiGateway.route_prefix)) {
        issues.push(
          t(
            'API Gateway Route Prefix inválido para {org}.',
            'API Gateway Route Prefix is invalid for {org}.',
            { org: organizationLabel }
          )
        );
      }
      if (!isValidRoutePrefix(serviceParameters.netapi.route_prefix)) {
        issues.push(
          t('NetAPI Route Prefix inválido para {org}.', 'NetAPI Route Prefix is invalid for {org}.', {
            org: organizationLabel,
          })
        );
      }

      if (!String(serviceParameters.couch.database || '').trim()) {
        issues.push(
          t('Couch Database obrigatório para {org}.', 'Couch Database is required for {org}.', {
            org: organizationLabel,
          })
        );
      }
      if (!String(serviceParameters.couch.admin_user || '').trim()) {
        issues.push(
          t('Couch Admin User obrigatório para {org}.', 'Couch Admin User is required for {org}.', {
            org: organizationLabel,
          })
        );
      }

      ORGANIZATION_SERVICE_KEYS.forEach(serviceKey => {
        const hostRef = toText(serviceHostMapping[serviceKey]);
        if (!hostRef) {
          issues.push(
            t('{service} host_ref obrigatório para {org}.', '{service} host_ref is required for {org}.', {
              service: serviceKey,
              org: organizationLabel,
            })
          );
          return;
        }
        if (!hasMachineOptions || !machineLabelSet.has(hostRef)) {
          issues.push(
            t(
              '{service} host_ref inválido para {org}; selecione host cadastrado na etapa de Infra.',
              '{service} host_ref is invalid for {org}; select a host registered in the Infra step.',
              {
                service: serviceKey,
                org: organizationLabel,
              }
            )
          );
        }
      });
    });

    const duplicatedOrganizationNames = organizationNames.filter(
      (name, index) => organizationNames.indexOf(name) !== index
    );

    if (duplicatedOrganizationNames.length > 0) {
      issues.push(
        t('Organizations duplicadas: {names}.', 'Duplicated organizations: {names}.', {
          names: duplicatedOrganizationNames.join(', '),
        })
      );
    }

    return issues;
  }, [machines, organizationNames, organizations, t]);

  const nodesIssues = useMemo(() => {
    const issues = [];

    if (organizationNames.length === 0) {
      issues.push(
        t(
          'Cadastre ao menos uma organização antes de definir nodes.',
          'Register at least one organization before defining nodes.'
        )
      );
      return issues;
    }

    organizations.forEach(organization => {
      const organizationLabel = organization.name || organization.id;
      const peers = Number(organization.peers || 0);
      const orderers = Number(organization.orderers || 0);

      if (!Number.isInteger(peers) || peers < 0) {
        issues.push(t('Quantidade de peers inválida para {org}.', 'Invalid peer count for {org}.', {
          org: organizationLabel,
        }));
      }
      if (!Number.isInteger(orderers) || orderers < 0) {
        issues.push(
          t('Quantidade de orderers inválida para {org}.', 'Invalid orderer count for {org}.', {
            org: organizationLabel,
          })
        );
      }
      if (peers + orderers <= 0) {
        issues.push(
          t(
            'A organização {org} precisa de ao menos um peer ou orderer.',
            'Organization {org} needs at least one peer or orderer.',
            { org: organizationLabel }
          )
        );
      }
    });

    return issues;
  }, [organizationNames, organizations, t]);

  const businessGroupIssues = useMemo(() => {
    const issues = [];

    if (!String(businessGroupName || '').trim()) {
      issues.push(
        t('Business Group Name é obrigatório.', 'Business Group Name is required.')
      );
    }

    return issues;
  }, [businessGroupName, t]);

  const channelIssues = useMemo(() => {
    const issues = [];

    if (channels.length === 0) {
      issues.push(t('Ao menos um channel deve ser criado.', 'At least one channel must be created.'));
    }

    channels.forEach(channel => {
      const channelLabel = channel.name || channel.id;
      const memberOrgs = parseCsv(channel.memberOrgs);

      if (!String(channel.name || '').trim()) {
        issues.push(t('Nome do channel obrigatório ({id}).', 'Channel name is required ({id}).', {
          id: channel.id,
        }));
      }
      memberOrgs.forEach(memberOrg => {
        if (!organizationNameSet.has(memberOrg)) {
          issues.push(
            t(
              "Channel {channel} referencia org '{org}' que não existe no cadastro atual.",
              "Channel {channel} references org '{org}' that does not exist in the current registry.",
              {
                channel: channelLabel,
                org: memberOrg,
              }
            )
          );
        }
      });
    });

    const duplicatedChannelNames = channelNames.filter(
      (name, index) => channelNames.indexOf(name) !== index
    );
    if (duplicatedChannelNames.length > 0) {
      issues.push(
        t('Channels duplicados: {names}.', 'Duplicated channels: {names}.', {
          names: duplicatedChannelNames.join(', '),
        })
      );
    }

    return issues;
  }, [channelNames, channels, organizationNameSet, t]);

  const installIssues = useMemo(() => {
    const issues = [];

    if (channelNames.length === 0) {
      issues.push(
        t(
          'Você precisa criar ao menos um channel antes do install de chaincode.',
          'You need to create at least one channel before installing chaincode.'
        )
      );
    }

    if (chaincodeInstalls.length === 0) {
      issues.push(
        t(
          'Ao menos uma instalação de chaincode deve ser informada.',
          'At least one chaincode installation must be provided.'
        )
      );
    }

    chaincodeInstalls.forEach(install => {
      const targetChannel = String(install.channel || '').trim();
      const packageFileName = String(install.packageFileName || '').trim();
      const chaincodeName = String(install.chaincodeName || '').trim();
      const artifactRef = String(install.artifactRef || '').trim();
      const uploadStatus = String(install.uploadStatus || '').trim();

      if (!targetChannel) {
        issues.push(t('Channel alvo obrigatório ({id}).', 'Target channel is required ({id}).', {
          id: install.id,
        }));
      } else if (!channelNameSet.has(targetChannel)) {
        issues.push(
          t(
            'Install {id} aponta para channel inexistente ({channel}).',
            'Install {id} points to a non-existing channel ({channel}).',
            {
              id: install.id,
              channel: targetChannel,
            }
          )
        );
      }

      if (!packageFileName) {
        issues.push(
          t('Envie o pacote .tar.gz para {id}.', 'Send the .tar.gz package for {id}.', {
            id: install.id,
          })
        );
      } else if (!isTarGzPackageName(packageFileName)) {
        issues.push(
          t(
            'Pacote inválido em {id}; somente arquivo .tar.gz é aceito.',
            'Invalid package in {id}; only .tar.gz files are accepted.',
            { id: install.id }
          )
        );
      }

      if (uploadStatus === 'uploading') {
        issues.push(
          t('Aguarde o upload do pacote para {id}.', 'Wait for the package upload for {id}.', {
            id: install.id,
          })
        );
      } else if (!artifactRef) {
        issues.push(
          t(
            'O pacote {id} ainda não foi carregado no orchestrator.',
            'Package {id} has not been uploaded to the orchestrator yet.',
            { id: install.id }
          )
        );
      }

      if (!chaincodeName) {
        issues.push(
          t('Informe o nome do chaincode para {id}.', 'Provide the chaincode name for {id}.', {
            id: install.id,
          })
        );
      }
    });

    return issues;
  }, [chaincodeInstalls, channelNameSet, channelNames, t]);

  const apiManagementIssues = useMemo(
    () =>
      buildApiManagementIssues({
        apiRegistrations,
        organizations,
        changeId,
      }),
    [apiRegistrations, organizations, changeId]
  );

  const apiRegistryEntries = useMemo(
    () =>
      buildApiRegistryEntries({
        apiRegistrations,
        changeId,
        executionContext: t(
          'A1.6.5 API onboarding por organização/canal/chaincode',
          'A1.6.5 API onboarding by organization/channel/chaincode'
        ),
        generatedAtUtc: new Date().toISOString(),
      }),
    [apiRegistrations, changeId, t]
  );

  const hasIncrementalDrafts =
    incrementalNodeExpansions.length > 0 ||
    incrementalChannelAdditions.length > 0 ||
    incrementalInstallAdditions.length > 0 ||
    incrementalApiAdditions.length > 0;

  const incrementalRunId = useMemo(
    () =>
      deterministicRunId({
        changeId: String(changeId || '').trim(),
        blueprintFingerprint: String(wizardLintReport?.fingerprint || '').trim(),
        resolvedSchemaVersion: String(wizardLintReport?.resolvedSchemaVersion || '1.0.0').trim(),
      }),
    [changeId, wizardLintReport]
  );

  const topologyModelSignature = useMemo(
    () =>
      JSON.stringify({
        changeId: String(changeId || '').trim(),
        environmentProfile,
        machines,
        organizations,
        businessGroupName: String(businessGroupName || '').trim(),
        businessGroupDescription: String(businessGroupDescription || '').trim(),
        businessGroupNetworkId: String(businessGroupNetworkId || '').trim(),
        channels,
        chaincodeInstalls,
        apiRegistrations,
        incrementalExpansionHistory,
      }),
    [
      changeId,
      environmentProfile,
      machines,
      organizations,
      businessGroupName,
      businessGroupDescription,
      businessGroupNetworkId,
      channels,
      chaincodeInstalls,
      apiRegistrations,
      incrementalExpansionHistory,
    ]
  );

  const appendAuditEvent = useCallback(
    (code, details = {}) => {
      setModelingAuditTrail(previous => [
        {
          id: `audit-${Date.now()}-${previous.length + 1}`,
          code,
          change_id: String(changeId || '').trim(),
          timestamp_utc: new Date().toISOString(),
          details,
        },
        ...previous,
      ]);
    },
    [changeId]
  );

  const handleChaincodePackageUpload = useCallback(
    async (installId, info) => {
      const selection = resolveChaincodePackageSelection(info);
      if (!selection) {
        return false;
      }

      const { fileName, rawFile } = selection;
      if (!isTarGzPackageName(fileName)) {
        setChaincodeInstalls(current =>
          current.map(install =>
            install.id === installId
              ? {
                  ...install,
                  packageFileName: fileName,
                  artifactRef: '',
                  packageId: '',
                  packageLabel: '',
                  packageLanguage: '',
                  uploadStatus: 'error',
                  uploadError: t(
                    'Somente arquivo .tar.gz é aceito.',
                    'Only .tar.gz files are accepted.'
                  ),
                }
              : install
          )
        );
        message.error(
          t(
            'Pacote inválido para {installId}; somente arquivo .tar.gz é aceito.',
            'Invalid package for {installId}; only .tar.gz files are accepted.',
            { installId }
          )
        );
        return false;
      }

      setChaincodeInstalls(current =>
        current.map(install =>
          install.id === installId
            ? {
                ...install,
                packageFileName: fileName,
                artifactRef: '',
                packageId: '',
                packageLabel: '',
                packageLanguage: '',
                uploadStatus: 'uploading',
                uploadError: '',
              }
            : install
        )
      );

      try {
        const formData = new FormData();
        formData.append('file', rawFile);
        formData.append('description', `Provisioning wizard upload (${installId})`);
        const response = await uploadChainCode(formData, {
          skipErrorHandler: true,
        });
        const uploadPayload = normalizeChaincodeUploadPayload(response);
        if (!uploadPayload.artifactRef) {
          throw new Error(
            t(
              'Resposta de upload sem artifact_ref.',
              'Upload response without artifact_ref.'
            )
          );
        }

        setChaincodeInstalls(current =>
          current.map(install =>
            install.id === installId
              ? {
                  ...install,
                  packageFileName: uploadPayload.packageFileName || fileName,
                  artifactRef: uploadPayload.artifactRef,
                  packageId: uploadPayload.packageId,
                  packageLabel: uploadPayload.packageLabel,
                  packageLanguage: uploadPayload.packageLanguage,
                  uploadStatus: 'uploaded',
                  uploadError: '',
                }
              : install
          )
        );

        appendAuditEvent('chaincode_package_uploaded', {
          install_id: installId,
          package_file_name: uploadPayload.packageFileName || fileName,
          package_id: uploadPayload.packageId,
          artifact_ref: uploadPayload.artifactRef,
          already_exists: uploadPayload.alreadyExists,
        });

        message.success(
          uploadPayload.alreadyExists
            ? t(
                'Pacote {fileName} reutilizado com sucesso.',
                'Package {fileName} reused successfully.',
                { fileName: uploadPayload.packageFileName || fileName }
              )
            : t(
                'Pacote {fileName} carregado com sucesso.',
                'Package {fileName} uploaded successfully.',
                { fileName: uploadPayload.packageFileName || fileName }
              )
        );
      } catch (error) {
        const errorMessage = extractRequestErrorMessage(error);
        setChaincodeInstalls(current =>
          current.map(install =>
            install.id === installId
              ? {
                  ...install,
                  artifactRef: '',
                  packageId: '',
                  packageLabel: '',
                  packageLanguage: '',
                  uploadStatus: 'error',
                  uploadError: errorMessage,
                }
              : install
          )
        );
        appendAuditEvent('chaincode_package_upload_failed', {
          install_id: installId,
          package_file_name: fileName,
          error: errorMessage,
        });
        message.error(
          t('Falha ao carregar {fileName}: {error}', 'Failed to upload {fileName}: {error}', {
            fileName,
            error: errorMessage,
          })
        );
      }

      return false;
    },
    [appendAuditEvent, t]
  );

  const loadProvisioningAuditHistory = useCallback(() => {
    const storage = getBrowserLocalStorage();
    if (!storage) {
      setProvisioningAuditHistory([]);
      return;
    }

    const rawHistoryPayload = storage.getItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
    if (!rawHistoryPayload) {
      setProvisioningAuditHistory([]);
      return;
    }

    try {
      const parsedHistory = JSON.parse(rawHistoryPayload);
      const normalizedHistory = (Array.isArray(parsedHistory) ? parsedHistory : [])
        .filter(entry => entry && typeof entry === 'object')
        .map(entry => {
          const context = entry.context && typeof entry.context === 'object' ? entry.context : {};
          const contextOrganizations = normalizeAuditOrganizations(context.organizations);
          const fallbackOrganizationCount =
            contextOrganizations.length > 0
              ? contextOrganizations.length
              : toSafePositiveInt(context.organizationCount);

          return {
            key: String(entry.key || '').trim(),
            runId: String(entry.runId || '').trim(),
            changeId: String(entry.changeId || '').trim(),
            status: String(entry.status || '')
              .trim()
              .toLowerCase(),
            startedAt: String(entry.startedAt || '').trim(),
            finishedAt: String(entry.finishedAt || '').trim(),
            capturedAt: String(entry.capturedAt || '').trim(),
            context: {
              providerKey: String(context.providerKey || '').trim(),
              environmentProfile: String(context.environmentProfile || '').trim(),
              hostCount: toSafePositiveInt(context.hostCount),
              organizationCount: fallbackOrganizationCount,
              nodeCount: toSafePositiveInt(context.nodeCount),
              apiCount: toSafePositiveInt(context.apiCount),
              incrementalCount: toSafePositiveInt(context.incrementalCount),
              organizations: contextOrganizations,
            },
          };
        })
        .filter(entry => entry.key && entry.runId);

      setProvisioningAuditHistory(normalizedHistory);
    } catch (error) {
      storage.removeItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
      setProvisioningAuditHistory([]);
    }
  }, []);

  useEffect(() => {
    loadProvisioningAuditHistory();
  }, [loadProvisioningAuditHistory]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }

    const refreshHistoryOnFocus = () => {
      loadProvisioningAuditHistory();
    };

    window.addEventListener('focus', refreshHistoryOnFocus);
    return () => {
      window.removeEventListener('focus', refreshHistoryOnFocus);
    };
  }, [loadProvisioningAuditHistory]);

  useEffect(() => {
    if (hydrationDoneRef.current) {
      return;
    }

    const storage = getBrowserLocalStorage();
    if (!storage) {
      hydrationDoneRef.current = true;
      return;
    }

    storage.removeItem(INFRA_ONBOARDING_STORAGE_KEY_LEGACY);

    const rawPayload = storage.getItem(INFRA_ONBOARDING_STORAGE_KEY);
    if (!rawPayload) {
      hydrationDoneRef.current = true;
      return;
    }

    try {
      const parsed = JSON.parse(rawPayload);
      if (!parsed || typeof parsed !== 'object') {
        storage.removeItem(INFRA_ONBOARDING_STORAGE_KEY);
        hydrationDoneRef.current = true;
        return;
      }

      setChangeId(toText(parsed.changeId) || buildAutoChangeId());
      setEnvironmentProfile(normalizeEnvironmentProfile(parsed.environmentProfile));

      const restoredMachines = toArray(parsed.machines).map((machine, index) =>
        normalizeMachineState(machine, index)
      );
      setMachines(restoredMachines.length > 0 ? restoredMachines : [createMachineDraft(1)]);

      const restoredOrganizations = toArray(parsed.organizations).map((organization, index) =>
        normalizeOrganizationState(organization, index)
      );
      setOrganizations(
        restoredOrganizations.length > 0 ? restoredOrganizations : [createOrganizationDraft(1)]
      );

      setChannels(
        toArray(parsed.channels).map((row, index) =>
          normalizeSimpleRow(row, createChannelDraft, index)
        )
      );
      setChaincodeInstalls(
        toArray(parsed.chaincodeInstalls).map((row, index) =>
          normalizeSimpleRow(row, createChaincodeInstallDraft, index)
        )
      );
      setApiRegistrations(
        toArray(parsed.apiRegistrations).map((row, index) =>
          normalizeSimpleRow(row, createApiRegistrationDraft, index)
        )
      );

      setIncrementalNodeExpansions(
        toArray(parsed.incrementalNodeExpansions).map((row, index) =>
          normalizeSimpleRow(row, createIncrementalNodeExpansionDraft, index)
        )
      );
      setIncrementalChannelAdditions(
        toArray(parsed.incrementalChannelAdditions).map((row, index) =>
          normalizeSimpleRow(row, createIncrementalChannelDraft, index)
        )
      );
      setIncrementalInstallAdditions(
        toArray(parsed.incrementalInstallAdditions).map((row, index) =>
          normalizeSimpleRow(row, createIncrementalInstallDraft, index)
        )
      );
      setIncrementalApiAdditions(
        toArray(parsed.incrementalApiAdditions).map((row, index) =>
          normalizeSimpleRow(row, createIncrementalApiDraft, index)
        )
      );

      setIncrementalExpansionHistory(toArray(parsed.incrementalExpansionHistory));
      setBusinessGroupName(toText(parsed.businessGroupName));
      setBusinessGroupDescription(toText(parsed.businessGroupDescription));
      setBusinessGroupNetworkId(toText(parsed.businessGroupNetworkId));

      const restoredStepIndex = Number(parsed.activeStepIndex);
      if (Number.isInteger(restoredStepIndex) && restoredStepIndex >= 0) {
        setActiveStepIndex(Math.min(5, restoredStepIndex));
      }

      setPreflightApprovedSignature(toText(parsed.preflightApprovedSignature));
      setPreflightReport(
        parsed.preflightReport && typeof parsed.preflightReport === 'object'
          ? parsed.preflightReport
          : null
      );

      setModelingAuditTrail(toArray(parsed.modelingAuditTrail));
      message.success(
        t(
          'Sessão de infraestrutura restaurada com sucesso.',
          'Infrastructure session restored successfully.'
        )
      );
    } catch (error) {
      storage.removeItem(INFRA_ONBOARDING_STORAGE_KEY);
      message.warning(
        t(
          'Estado local inválido foi descartado para evitar inconsistências.',
          'Invalid local state was discarded to avoid inconsistencies.'
        )
      );
    }

    hydrationDoneRef.current = true;
  }, [t]);
  const describeMissingRequirements = useCallback((issues, limit = 2) => {
    if (!Array.isArray(issues) || issues.length === 0) {
      return '';
    }

    const firstIssue = String(issues[0] || '').trim();
    if (issues.length === 1) {
      return firstIssue;
    }

    const extraIssues = Math.max(0, issues.length - 1);
    if (extraIssues <= limit) {
      return t('{firstIssue} (+{extraIssues} pendência(s)).', '{firstIssue} (+{extraIssues} pending item(s)).', {
        firstIssue,
        extraIssues,
      });
    }

    return t(
      '{firstIssue} (+{limit} de {extraIssues} pendência(s) adicionais).',
      '{firstIssue} (+{limit} of {extraIssues} additional pending item(s)).',
      {
        firstIssue,
        limit,
        extraIssues,
      }
    );
  }, [t]);

  useEffect(() => {
    if (!latestModelSignatureRef.current) {
      latestModelSignatureRef.current = topologyModelSignature;
      return;
    }

    if (latestModelSignatureRef.current === topologyModelSignature) {
      return;
    }

    appendAuditEvent('guided_topology_model_updated', {
      model_signature: topologyModelSignature,
    });
    latestModelSignatureRef.current = topologyModelSignature;
  }, [topologyModelSignature, appendAuditEvent]);

  useEffect(() => {
    if (!hydrationDoneRef.current) {
      return;
    }

    const storage = getBrowserLocalStorage();
    if (!storage) {
      return;
    }

    const payload = {
      changeId,
      environmentProfile,
      machines,
      organizations,
      channels,
      chaincodeInstalls,
      apiRegistrations,
      incrementalNodeExpansions,
      incrementalChannelAdditions,
      incrementalInstallAdditions,
      incrementalApiAdditions,
      incrementalExpansionHistory,
      businessGroupName,
      businessGroupDescription,
      businessGroupNetworkId,
      activeStepIndex,
      preflightApprovedSignature,
      preflightReport,
      modelingAuditTrail,
      persistedAtUtc: new Date().toISOString(),
    };

    try {
      storage.setItem(INFRA_ONBOARDING_STORAGE_KEY, JSON.stringify(payload));
      persistenceWarningShownRef.current = false;
    } catch (error) {
      try {
        storage.removeItem(INFRA_ONBOARDING_STORAGE_KEY);
      } catch (removeError) {
        // no-op
      }

      if (!persistenceWarningShownRef.current) {
        message.warning(
          t(
            'Não foi possível persistir a sessão local de infraestrutura. Continue o fluxo e, se necessário, exporte o seed antes de recarregar a página.',
            'Could not persist the local infrastructure session. Continue the flow and, if needed, export the seed before reloading the page.'
          )
        );
        persistenceWarningShownRef.current = true;
      }
    }
  }, [
    changeId,
    environmentProfile,
    machines,
    organizations,
    channels,
    chaincodeInstalls,
    apiRegistrations,
    incrementalNodeExpansions,
    incrementalChannelAdditions,
    incrementalInstallAdditions,
    incrementalApiAdditions,
    incrementalExpansionHistory,
    businessGroupName,
    businessGroupDescription,
    businessGroupNetworkId,
    activeStepIndex,
    preflightApprovedSignature,
    preflightReport,
    modelingAuditTrail,
    persistenceWarningShownRef,
    t,
  ]);

  const guidedBlueprintDraft = useMemo(
    () =>
      buildBlueprintFromGuidedTopology({
        changeId,
        environmentProfile,
        machines,
        organizations,
        businessGroup: {
          name: businessGroupName,
          description: businessGroupDescription,
          networkId: businessGroupNetworkId,
        },
        channels,
        chaincodeInstalls,
      }),
    [
      changeId,
      environmentProfile,
      machines,
      organizations,
      businessGroupName,
      businessGroupDescription,
      businessGroupNetworkId,
      channels,
      chaincodeInstalls,
    ]
  );

  const lintMatchesCurrentTopology =
    Boolean(wizardLintReport) && wizardLintReport.modelSignature === topologyModelSignature;
  const lintApprovedForCurrentModel =
    Boolean(wizardLintReport && wizardLintReport.valid) &&
    (lintMatchesCurrentTopology || wizardLintReport?.lintSource === 'degraded-local');

  const preflightSignature = useMemo(
    () =>
      JSON.stringify({
        providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
        machineCredentials: machineCredentialBindings,
        machines: machines.map(machine => ({
          id: machine.id,
          infraLabel: machine.infraLabel,
          hostAddress: String(machine.hostAddress || '').trim(),
          sshUser: String(machine.sshUser || '').trim(),
          sshPort: machine.sshPort,
          authMethod: machine.authMethod,
          dockerPort: toSafePort(machine.dockerPort, DEFAULT_DOCKER_PORT),
        })),
      }),
    [machineCredentialBindings, machines]
  );

  const latestPreflightMatchesCurrentInput =
    Boolean(preflightReport) && preflightReport.inputSignature === preflightSignature;

  const preflightRows = useMemo(() => {
    if (latestPreflightMatchesCurrentInput) {
      return preflightReport.hosts || [];
    }

    return machines.map((machine, index) => {
      const issues = getMachineConnectionIssues(machine, index);

      return {
        id: machine.id,
        infraLabel: machine.infraLabel || machine.id,
        hostAddress: String(machine.hostAddress || '').trim(),
        status: issues.length > 0 ? PREFLIGHT_HOST_STATUS.bloqueado : PREFLIGHT_HOST_STATUS.parcial,
        checks: [],
        failures:
          issues.length > 0
            ? [
                {
                  code: 'preflight_not_executed',
                  cause: issues[0],
                  recommendation: t(
                    'Corrigir os dados de conexão e executar preflight técnico.',
                    'Fix the connection data and run the technical preflight.'
                  ),
                },
              ]
            : [],
        warnings:
          issues.length === 0
            ? [
                {
                  code: 'preflight_not_executed',
                  cause: t(
                    'Preflight técnico ainda não executado para este host.',
                    'Technical preflight has not been run for this host yet.'
                  ),
                  recommendation: t(
                    'Executar preflight técnico para liberar a próxima etapa.',
                    'Run the technical preflight to unlock the next step.'
                  ),
                },
              ]
            : [],
        primaryCause:
          issues.length > 0
            ? issues[0]
            : t(
                'Preflight técnico ainda não executado para este host.',
                'Technical preflight has not been run for this host yet.'
              ),
        primaryRecommendation:
          issues.length > 0
            ? t(
                'Corrigir os dados mínimos de conexão e reexecutar o preflight técnico.',
                'Fix the minimum connection data and rerun the technical preflight.'
              )
            : t(
                'Executar preflight técnico para coletar diagnóstico de conectividade e capacidade.',
                'Run the technical preflight to collect connectivity and capacity diagnostics.'
              ),
        runtimeSnapshot: null,
      };
    });
  }, [latestPreflightMatchesCurrentInput, preflightReport, machines, t]);

  const preflightNeedsRefresh =
    Boolean(preflightApprovedSignature) && preflightApprovedSignature !== preflightSignature;

  const containerConflictRows = useMemo(
    () => preflightRows.filter(row => hasContainerConflictSignal(row)),
    [preflightRows]
  );
  const hasContainerConflict = containerConflictRows.length > 0;

  const preflightApproved =
    preflightApprovedSignature === preflightSignature &&
    preflightRows.length > 0 &&
    preflightRows.every(row => row.status === PREFLIGHT_HOST_STATUS.apto) &&
    !hasContainerConflict;

  const preflightSummary = useMemo(
    () =>
      (latestPreflightMatchesCurrentInput && preflightReport && preflightReport.summary) ||
      preflightRows.reduce(
        (accumulator, row) => {
          accumulator.total += 1;
          accumulator[row.status] += 1;
          return accumulator;
        },
        {
          total: 0,
          apto: 0,
          parcial: 0,
          bloqueado: 0,
        }
      ),
    [latestPreflightMatchesCurrentInput, preflightReport, preflightRows]
  );

  const incrementalNodePreview = useMemo(
    () =>
      buildIncrementalNodeNamingPreview({
        changeId,
        runId: incrementalRunId,
        organizations,
        machines,
        preflightReport,
        nodeExpansions: incrementalNodeExpansions,
      }),
    [
      changeId,
      incrementalRunId,
      organizations,
      machines,
      preflightReport,
      incrementalNodeExpansions,
    ]
  );

  const incrementalNodePreviewByDraftId = useMemo(
    () =>
      incrementalNodePreview.operations.reduce((accumulator, operation) => {
        const draftId = toText(operation && operation.source_draft_id);
        if (!draftId) {
          return accumulator;
        }

        if (!accumulator[draftId]) {
          accumulator[draftId] = [];
        }

        accumulator[draftId].push(operation);
        return accumulator;
      }, {}),
    [incrementalNodePreview]
  );

  const incrementalExpansionIssues = useMemo(() => {
    if (!hasIncrementalDrafts) {
      return [];
    }

    return buildIncrementalExpansionIssues({
      changeId,
      preflightApproved,
      lintApproved: lintApprovedForCurrentModel,
      organizations,
      channels,
      chaincodeInstalls,
      apiRegistrations,
      machines,
      preflightReport,
      nodeExpansions: incrementalNodeExpansions,
      channelAdditions: incrementalChannelAdditions,
      installAdditions: incrementalInstallAdditions,
      apiAdditions: incrementalApiAdditions,
    });
  }, [
    hasIncrementalDrafts,
    changeId,
    preflightApproved,
    lintApprovedForCurrentModel,
    organizations,
    channels,
    chaincodeInstalls,
    apiRegistrations,
    machines,
    preflightReport,
    incrementalNodeExpansions,
    incrementalChannelAdditions,
    incrementalInstallAdditions,
    incrementalApiAdditions,
  ]);

  const validationIssues = useMemo(
    () => [
      ...infrastructureIssues,
      ...organizationIssues,
      ...nodesIssues,
      ...businessGroupIssues,
      ...channelIssues,
      ...installIssues,
      ...apiManagementIssues,
      ...incrementalExpansionIssues,
    ],
    [
      infrastructureIssues,
      organizationIssues,
      nodesIssues,
      businessGroupIssues,
      channelIssues,
      installIssues,
      apiManagementIssues,
      incrementalExpansionIssues,
    ]
  );

  const infraStepReady = infrastructureIssues.length === 0 && preflightApproved;
  const organizationsStepReady = infraStepReady && organizationIssues.length === 0;
  const nodesStepReady = organizationsStepReady && nodesIssues.length === 0;
  const businessGroupsStepReady = nodesStepReady && businessGroupIssues.length === 0;
  const channelsStepReady = businessGroupsStepReady && channelIssues.length === 0;
  const installStepReady =
    channelsStepReady &&
    installIssues.length === 0 &&
    apiManagementIssues.length === 0 &&
    incrementalExpansionIssues.length === 0;
  const handoffToRunbookReady =
    preflightApproved && installStepReady && validationIssues.length === 0;

  const maxUnlockedStep = useMemo(() => {
    if (!infraStepReady) {
      return 0;
    }
    if (!organizationsStepReady) {
      return 1;
    }
    if (!nodesStepReady) {
      return 2;
    }
    if (!businessGroupsStepReady) {
      return 3;
    }
    if (!channelsStepReady) {
      return 4;
    }
    return 5;
  }, [
    infraStepReady,
    organizationsStepReady,
    nodesStepReady,
    businessGroupsStepReady,
    channelsStepReady,
  ]);

  useEffect(() => {
    if (activeStepIndex > maxUnlockedStep) {
      setActiveStepIndex(maxUnlockedStep);
    }
  }, [activeStepIndex, maxUnlockedStep]);

  const activeStepIssues = useMemo(() => {
      if (activeStepIndex === 0) {
        if (preflightApproved) {
          return infrastructureIssues;
        }
      return [
        ...infrastructureIssues,
        t(
          'Execute preflight e mantenha as VMs em status apto.',
          'Run preflight and keep the VMs in ready status.'
        ),
      ];
    }
    if (activeStepIndex === 1) {
      return organizationIssues;
    }
    if (activeStepIndex === 2) {
      return nodesIssues;
    }
    if (activeStepIndex === 3) {
      return businessGroupIssues;
    }
    if (activeStepIndex === 4) {
      return channelIssues;
    }
    return [...installIssues, ...apiManagementIssues, ...incrementalExpansionIssues];
  }, [
    activeStepIndex,
    infrastructureIssues,
    preflightApproved,
    organizationIssues,
    nodesIssues,
    businessGroupIssues,
    channelIssues,
    installIssues,
    apiManagementIssues,
    incrementalExpansionIssues,
    t,
  ]);

  const infrastructureSeed = useMemo(
    () => ({
      provider_key: PROVISIONING_INFRA_PROVIDER_KEY,
      provider_scope: 'vm-linux-via-ssh',
      change_id: String(changeId || '').trim(),
      environment_profile: environmentProfile,
      ssh: {
        auth_method: PROVISIONING_SSH_AUTH_METHOD,
        private_key_ref: '',
      },
      machine_credentials: machineCredentialBindings,
      machines: machines.map(machine => ({
        infra_label: String(machine.infraLabel || '').trim(),
        host_address: String(machine.hostAddress || '').trim(),
        ssh_user: String(machine.sshUser || '').trim(),
        ssh_port: machine.sshPort,
        auth_method: String(machine.authMethod || PROVISIONING_SSH_AUTH_METHOD),
        ssh_credential_ref: String(machine.sshCredentialRef || '').trim(),
        docker_port: toSafePort(machine.dockerPort, DEFAULT_DOCKER_PORT),
      })),
      network: {
        business_group: {
          name: String(businessGroupName || '').trim(),
          description: String(businessGroupDescription || '').trim(),
          network_id: String(businessGroupNetworkId || '').trim(),
        },
        organizations: organizations.map(organization => ({
          component_host_mapping: resolveOrganizationServiceHostMapping(organization),
          org_name: String(organization.name || '').trim(),
          org_domain: String(organization.domain || '').trim(),
          org_label: String(organization.label || '').trim(),
          network_api: {
            network_api_host: String(organization.networkApiHost || '').trim(),
            network_api_port: organization.networkApiPort,
          },
          services: resolveOrganizationServiceParameters(organization),
          certificate_authority: {
            ca_mode: organization.caMode,
            ca_name: String(organization.caName || '').trim(),
            ca_host: String(organization.caHost || '').trim(),
            ca_host_ref: String(organization.caHostRef || '').trim(),
            ca_port: organization.caPort,
            ca_user: String(organization.caUser || '').trim(),
            ca_password_ref: String(organization.caPasswordRef || '').trim(),
          },
          peers_count: organization.peers,
          orderers_count: organization.orderers,
          peer_port_base: organization.peerPortBase,
          orderer_port_base: organization.ordererPortBase,
          peer_host_ref: String(organization.peerHostRef || '').trim(),
          orderer_host_ref: String(organization.ordererHostRef || '').trim(),
        })),
        channels: channels.map(channel => ({
          name: String(channel.name || '').trim(),
          member_orgs: (() => {
            const members = parseCsv(channel.memberOrgs);
            return members.length > 0 ? members : organizationNames;
          })(),
        })),
        chaincodes: chaincodeInstalls.map(install => ({
          name: String(install.chaincodeName || '').trim(),
          channel: String(install.channel || '').trim(),
          package_pattern: String(install.packagePattern || '').trim(),
          package_file_name: String(install.packageFileName || '').trim(),
          artifact_ref: String(install.artifactRef || '').trim(),
          install_only: true,
          endpoint_path: String(install.endpointPath || '').trim(),
        })),
        apis: apiRegistryEntries,
        incremental_expansions: incrementalExpansionHistory,
      },
      generated_at_utc: new Date().toISOString(),
      guided_blueprint_draft: guidedBlueprintDraft,
      preflight: preflightReport
        ? {
            change_id: String(preflightReport.changeId || '').trim(),
            executed_at_utc: preflightReport.executedAtUtc,
            overall_status: preflightReport.overallStatus,
            summary: preflightReport.summary,
            hosts: (preflightReport.hosts || []).map(host => ({
              host_id: host.id,
              infra_label: host.infraLabel,
              host_address: host.hostAddress,
              status: host.status,
              primary_cause: host.primaryCause,
              recommended_action: host.primaryRecommendation,
            })),
          }
        : null,
    }),
    [
      businessGroupDescription,
      businessGroupNetworkId,
      businessGroupName,
      changeId,
      environmentProfile,
      guidedBlueprintDraft,
      machineCredentialBindings,
      machines,
      organizations,
      channels,
      chaincodeInstalls,
      apiRegistryEntries,
      incrementalExpansionHistory,
      preflightReport,
      organizationNames,
    ]
  );

  const sshPreviewLines = useMemo(() => {
    return machines
      .filter(machine => String(machine.hostAddress || '').trim())
      .map(machine => {
        const sshPort = machine.sshPort;
        const sshUser = String(machine.sshUser || '').trim() || t('usuario', 'user');
        const hostAddress = String(machine.hostAddress || '').trim();
        const credentialRef = String(machine.sshCredentialRef || '').trim();
        const isLocalPem = credentialRef.startsWith('local-file:');
        const pemFileName = isLocalPem ? credentialRef.replace('local-file:', '').trim() : '';

        if (pemFileName) {
          return `ssh -i ${pemFileName} -p ${sshPort} ${sshUser}@${hostAddress}`;
        }

        return `ssh -p ${sshPort} ${sshUser}@${hostAddress}`;
      });
  }, [machines, t]);

  const handleRunPreflight = async () => {
    if (!String(changeId || '').trim()) {
      message.error(
        t(
          'Preflight técnico bloqueado: change_id é obrigatório.',
          'Technical preflight blocked: change_id is required.'
        )
      );
      setPreflightApprovedSignature('');
      return;
    }

    if (infrastructureIssues.length > 0) {
      message.error(
        t(
          'Corrija os dados de conexão/VM antes do preflight ({count} pendências).',
          'Fix the connection/VM data before preflight ({count} pending issues).',
          { count: infrastructureIssues.length }
        )
      );
      setPreflightApprovedSignature('');
      return;
    }

    setIsRunningPreflight(true);

    try {
      const executedAtUtc = new Date().toISOString();
      let report = null;

      if (OFFICIAL_BLUEPRINT_ENDPOINTS_ENABLED) {
        const backendReport = await runbookTechnicalPreflight({
          change_id: String(changeId || '').trim(),
          provider_key: PROVISIONING_INFRA_PROVIDER_KEY,
          host_mapping: machines.map(machine => ({
            host_ref: String(machine.infraLabel || machine.id || '').trim(),
            host_address: String(machine.hostAddress || '').trim(),
            ssh_user: String(machine.sshUser || '').trim(),
            ssh_port: Number(machine.sshPort || 22) || 22,
          })),
          machine_credentials: machineCredentialBindings,
        });

        report = normalizeTechnicalPreflightReport({
          report: backendReport,
          machines,
          changeId,
          executedAtUtc,
        });
      } else {
        report = runInfrastructurePreflight({
          changeId,
          machines,
          machineCredentials: machineCredentialBindings,
          executedAtUtc,
        });
      }

      const reportWithSignature = {
        ...report,
        inputSignature: preflightSignature,
      };
      setPreflightReport(reportWithSignature);
      appendAuditEvent('preflight_executed', {
        preflight_status: reportWithSignature.overallStatus,
        host_summary: reportWithSignature.summary,
      });

      const blockedRows = reportWithSignature.hosts.filter(
        row => row.status === PREFLIGHT_HOST_STATUS.bloqueado
      );
      if (blockedRows.length > 0) {
        message.error(
          t(
            'Preflight bloqueado: {count} host(s) com falha crítica.',
            'Preflight blocked: {count} host(s) with critical failure.',
            { count: blockedRows.length }
          )
        );
        setPreflightApprovedSignature('');
        return;
      }

      const partialRows = reportWithSignature.hosts.filter(
        row => row.status === PREFLIGHT_HOST_STATUS.parcial
      );
      if (partialRows.length > 0) {
        const partialContainerConflictRows = partialRows.filter(row =>
          hasContainerConflictSignal(row)
        );
        if (partialContainerConflictRows.length > 0) {
          message.warning(
            t(
              'Preflight parcial: {count} host(s) com containers ativos. Limpe os containers antes de avançar para Organizations.',
              'Partial preflight: {count} host(s) with active containers. Clear the containers before advancing to Organizations.',
              { count: partialContainerConflictRows.length }
            )
          );
          setPreflightApprovedSignature('');
          return;
        }

        message.warning(
          t(
            'Preflight parcial: {count} host(s) com recomendações pendentes. Publicação permanece bloqueada até status apto.',
            'Partial preflight: {count} host(s) with pending recommendations. Publication remains blocked until ready status.',
            { count: partialRows.length }
          )
        );
        setPreflightApprovedSignature('');
        return;
      }

      setPreflightApprovedSignature(preflightSignature);
      message.success(
        t(
          'Preflight técnico concluído: hosts aptos para avançar para Organizations.',
          'Technical preflight completed: hosts are ready to advance to Organizations.'
        )
      );
    } catch (error) {
      setPreflightApprovedSignature('');
      appendAuditEvent('preflight_execution_failed', {
        reason: String(
          error?.message ||
            t(
              'Falha no preflight técnico oficial.',
              'Failure in the official technical preflight.'
            )
        ),
      });
      message.error(
        String(
          error?.message ||
            t('Falha no preflight técnico oficial.', 'Failure in the official technical preflight.')
        )
      );
    } finally {
      setIsRunningPreflight(false);
    }
  };

  const runWizardLint = useCallback(
    async ({ showSuccessMessage = true } = {}) => {
      if (!OFFICIAL_BLUEPRINT_ENDPOINTS_ENABLED) {
        const localReport = {
          valid: true,
          lintSource: 'degraded-local',
          contractValid: true,
          modelSignature: topologyModelSignature,
          errors: [],
          warnings: [
            {
              code: 'lint_backend_disabled_local_mode',
              message: t(
                'Endpoint oficial de lint indisponível neste ambiente. Validação local aplicada para continuidade controlada do onboarding.',
                'Official lint endpoint unavailable in this environment. Local validation applied for controlled onboarding continuity.'
              ),
            },
          ],
          hints: [],
        };

        setWizardLintReport(localReport);
        appendAuditEvent('guided_lint_degraded_local', {
          reason_code: 'lint_backend_disabled_local_mode',
        });

        if (showSuccessMessage) {
          message.success(
            t(
              'Lint A1.2 concluído em modo degradado local.',
              'A1.2 lint completed in degraded local mode.'
            )
          );
        }

        return {
          valid: true,
          report: localReport,
        };
      }

      if (lintBackendUnavailableRef.current) {
        const cachedFallbackReport = {
          valid: true,
          lintSource: 'degraded-local',
          contractValid: true,
          modelSignature: topologyModelSignature,
          errors: [],
          warnings: [
            {
              code: 'lint_backend_unavailable_cached_fallback_local',
              message: t(
                'Endpoint oficial de lint indisponível neste ambiente. Validação local aplicada para continuidade controlada do onboarding.',
                'Official lint endpoint unavailable in this environment. Local validation applied for controlled onboarding continuity.'
              ),
            },
          ],
          hints: [],
        };

        setWizardLintReport(cachedFallbackReport);
        appendAuditEvent('guided_lint_degraded_local', {
          reason_code: 'lint_backend_unavailable_cached_fallback_local',
        });

        if (showSuccessMessage) {
          message.success(
            t(
              'Lint A1.2 concluído em modo degradado local.',
              'A1.2 lint completed in degraded local mode.'
            )
          );
        }

        return {
          valid: true,
          report: cachedFallbackReport,
        };
      }

      const payload = {
        blueprint: guidedBlueprintDraft,
        allow_migration: false,
        change_id: String(changeId || '').trim(),
        execution_context: t(
          'Onboarding guiado A1.6 - modelagem de topologia',
          'Guided onboarding A1.6 - topology modeling'
        ),
      };

      try {
        const response = await lintBlueprintBackend(payload);
        const isSuccessEnvelope = response && response.status === 'successful' && response.data;

        if (!isSuccessEnvelope) {
          const backendUnavailable = isBackendBlueprintResponseUnavailable(response);
          if (backendUnavailable) {
            lintBackendUnavailableRef.current = true;
          }
          const fallbackReport = {
            valid: true,
            lintSource: 'degraded-local',
            contractValid: true,
            modelSignature: topologyModelSignature,
            errors: [],
            warnings: [
              {
                code: backendUnavailable
                  ? 'lint_backend_unavailable_response_fallback_local'
                  : 'lint_backend_invalid_response_fallback_local',
                message: backendUnavailable
                  ? t(
                      'Endpoint oficial de lint indisponível neste ambiente (rota ausente ou erro HTTP). Validação local aplicada para continuidade controlada do onboarding.',
                      'Official lint endpoint unavailable in this environment (missing route or HTTP error). Local validation applied for controlled onboarding continuity.'
                    )
                  : t(
                      'Backend de lint respondeu fora do contrato esperado. Validação local aplicada para continuidade controlada do onboarding.',
                      'Lint backend responded outside the expected contract. Local validation applied for controlled onboarding continuity.'
                    ),
              },
            ],
            hints: [],
          };
          setWizardLintReport(fallbackReport);
          appendAuditEvent('guided_lint_degraded_local', {
            reason_code: backendUnavailable
              ? 'lint_backend_unavailable_response_fallback_local'
              : 'lint_backend_invalid_response_fallback_local',
          });

          if (showSuccessMessage) {
            message.success(
              t(
                'Lint A1.2 concluído em modo degradado local.',
                'A1.2 lint completed in degraded local mode.'
              )
            );
          }

          return { valid: true, report: fallbackReport };
        }

        const normalizedLint = normalizeBackendLintReport(response.data);
        const report = {
          ...(normalizedLint.report || {}),
          lintSource: 'backend',
          contractValid: normalizedLint.contractValid,
          modelSignature: topologyModelSignature,
        };
        lintBackendUnavailableRef.current = false;
        setWizardLintReport(report);

        const lintPublishReady =
          report.valid && report.contractValid && report.lintSource === 'backend';

        appendAuditEvent(lintPublishReady ? 'guided_lint_passed' : 'guided_lint_blocked', {
          valid: lintPublishReady,
          errors: Array.isArray(report.errors) ? report.errors.length : 0,
          warnings: Array.isArray(report.warnings) ? report.warnings.length : 0,
          hints: Array.isArray(report.hints) ? report.hints.length : 0,
        });

        if (showSuccessMessage) {
          if (lintPublishReady) {
            message.success(
              t(
                'Lint A1.2 aprovado: blueprint guiado apto para publicação técnica.',
                'A1.2 lint approved: guided blueprint ready for technical publication.'
              )
            );
          } else {
            const errorsCount = Array.isArray(report.errors) ? report.errors.length : 0;
            message.error(
              t(
                'Lint bloqueante: {count} erro(s) no blueprint guiado.',
                'Blocking lint: {count} error(s) in the guided blueprint.',
                { count: errorsCount }
              )
            );
          }
        }

        return {
          valid: lintPublishReady,
          report,
        };
      } catch (error) {
        if (isBackendBlueprintEndpointUnavailable(error)) {
          lintBackendUnavailableRef.current = true;
          const fallbackReport = {
            valid: true,
            lintSource: 'degraded-local',
            contractValid: true,
            modelSignature: topologyModelSignature,
            errors: [],
            warnings: [
              {
                code: 'lint_backend_unavailable_fallback_local',
                message: t(
                  'Endpoint oficial de lint indisponível. Validação local aplicada para continuidade controlada do onboarding.',
                  'Official lint endpoint unavailable. Local validation applied for controlled onboarding continuity.'
                ),
              },
            ],
            hints: [],
          };

          setWizardLintReport(fallbackReport);
          appendAuditEvent('guided_lint_degraded_local', {
            reason_code: 'lint_backend_unavailable_fallback_local',
          });

          if (showSuccessMessage) {
            message.success(
              t(
                'Lint A1.2 concluído em modo degradado local.',
                'A1.2 lint completed in degraded local mode.'
              )
            );
          }

          return {
            valid: true,
            report: fallbackReport,
          };
        }

        const blockingReport = {
          valid: false,
          lintSource: 'backend',
          contractValid: false,
          modelSignature: topologyModelSignature,
          errors: [
            {
              code: 'lint_backend_unavailable',
              message: t(
                'Backend de lint indisponível: {message}',
                'Lint backend unavailable: {message}',
                {
                  message: error?.message || t('falha de conectividade.', 'connectivity failure.'),
                }
              ),
            },
          ],
          warnings: [],
          hints: [],
        };
        setWizardLintReport(blockingReport);
        appendAuditEvent('guided_lint_blocked', {
          reason_code: 'lint_backend_unavailable',
        });
        message.error(
          t(
            'Lint bloqueante: backend indisponível para validar blueprint guiado.',
            'Blocking lint: backend unavailable to validate the guided blueprint.'
          )
        );
        return {
          valid: false,
          report: blockingReport,
        };
      }
    },
    [appendAuditEvent, changeId, guidedBlueprintDraft, t, topologyModelSignature]
  );

  const syncApiDraft = useCallback(
    draft => {
      const nextDraft = {
        ...draft,
      };

      const currentChannel = String(nextDraft.channel || '').trim();
      const currentChaincodeId = String(nextDraft.chaincodeId || '').trim();

      nextDraft.channel =
        currentChannel && currentChannel !== '*' && channelNameSet.has(currentChannel)
          ? currentChannel
          : channelNames[0] || '';
      nextDraft.chaincodeId =
        currentChaincodeId &&
        currentChaincodeId !== '*' &&
        activeChaincodeIdSet.has(currentChaincodeId)
          ? currentChaincodeId
          : activeChaincodeIds[0] || '';

      const exposure = buildApiExposureTarget({
        organizationName: nextDraft.organizationName,
        organizations,
        machines,
      });

      if (exposure.host) {
        nextDraft.exposureHost = exposure.host;
      }
      if (exposure.port) {
        nextDraft.exposurePort = exposure.port;
      }

      nextDraft.routePath = buildApiRoutePath({
        channelId: nextDraft.channel,
        chaincodeId: nextDraft.chaincodeId,
      });

      if (nextDraft.routePath !== '/api/*/*') {
        nextDraft.routePath = `${nextDraft.routePath}/*/*`;
      }

      return nextDraft;
    },
    [
      organizations,
      machines,
      channelNameSet,
      channelNames,
      activeChaincodeIdSet,
      activeChaincodeIds,
    ]
  );

  const handleApiRegistrationFieldChange = useCallback(
    (registrationId, field, value) => {
      setApiRegistrations(currentRegistrations =>
        currentRegistrations.map(registration => {
          if (registration.id !== registrationId) {
            return registration;
          }
          return syncApiDraft({
            ...registration,
            [field]: value,
          });
        })
      );
      appendAuditEvent('api_registry_updated', {
        api_id: registrationId,
        changed_field: field,
      });
    },
    [appendAuditEvent, syncApiDraft]
  );

  useEffect(() => {
    setApiRegistrations(current => {
      let hasChanges = false;

      const next = current.map(registration => {
        const normalized = syncApiDraft(registration);

        const changed =
          normalized.channel !== registration.channel ||
          normalized.chaincodeId !== registration.chaincodeId ||
          normalized.routePath !== registration.routePath ||
          normalized.exposureHost !== registration.exposureHost ||
          normalized.exposurePort !== registration.exposurePort;

        if (changed) {
          hasChanges = true;
          return normalized;
        }

        return registration;
      });

      return hasChanges ? next : current;
    });
  }, [syncApiDraft]);

  const handleApplyIncrementalExpansion = () => {
    if (!hasIncrementalDrafts) {
      message.warning(
        t(
          'Adicione ao menos uma expansão incremental antes de aplicar mudanças.',
          'Add at least one incremental expansion before applying changes.'
        )
      );
      return;
    }

    if (incrementalExpansionIssues.length > 0) {
      message.error(
        t(
          'Expansão incremental bloqueada ({count} pendência(s)).',
          'Incremental expansion blocked ({count} pending item(s)).',
          { count: incrementalExpansionIssues.length }
        )
      );
      return;
    }

    const expansionPlan = buildIncrementalExpansionPlan({
      changeId,
      runId: incrementalRunId,
      executionContext: t(
        'A1.6.6 expansão incremental pós-criação da organização (idempotente + gate lint/preflight)',
        'A1.6.6 post-organization-creation incremental expansion (idempotent + lint/preflight gate)'
      ),
      generatedAtUtc: new Date().toISOString(),
      organizations,
      machines,
      preflightReport,
      nodeExpansions: incrementalNodeExpansions,
      channelAdditions: incrementalChannelAdditions,
      installAdditions: incrementalInstallAdditions,
      apiAdditions: incrementalApiAdditions,
    });

    if (expansionPlan.length === 0) {
      message.warning(
        t(
          'Nenhuma operação incremental efetiva foi identificada para aplicação.',
          'No effective incremental operation was identified for application.'
        )
      );
      return;
    }

    const nextModel = applyIncrementalExpansionToModel({
      changeId,
      runId: incrementalRunId,
      organizations,
      channels,
      chaincodeInstalls,
      apiRegistrations,
      machines,
      preflightReport,
      nodeExpansions: incrementalNodeExpansions,
      channelAdditions: incrementalChannelAdditions,
      installAdditions: incrementalInstallAdditions,
      apiAdditions: incrementalApiAdditions,
    });

    const operationTypes = Array.from(
      new Set(
        expansionPlan
          .map(operation => toText(operation && operation.operation_type))
          .filter(Boolean)
      )
    ).sort((left, right) => left.localeCompare(right));

    setOrganizations(nextModel.organizations);
    setChannels(nextModel.channels);
    setChaincodeInstalls(nextModel.chaincodeInstalls);
    setApiRegistrations(nextModel.apiRegistrations);

    setIncrementalExpansionHistory(current => [
      {
        expansion_id: `exp-${Date.now()}`,
        change_id: String(changeId || '').trim(),
        run_id: incrementalRunId,
        operations_total: expansionPlan.length,
        operation_types: operationTypes,
        generated_at_utc: new Date().toISOString(),
        operations: expansionPlan,
      },
      ...current,
    ]);

    appendAuditEvent('incremental_expansion_applied', {
      run_id: incrementalRunId,
      operations_total: expansionPlan.length,
      operation_types: operationTypes,
    });

    setIncrementalNodeExpansions([]);
    setIncrementalChannelAdditions([]);
    setIncrementalInstallAdditions([]);
    setIncrementalApiAdditions([]);
    setWizardLintReport(null);

    message.success(
      t(
        'Expansão incremental aplicada com {count} operação(ões). Reexecute o lint para publicação.',
        'Incremental expansion applied with {count} operation(s). Re-run lint before publication.',
        { count: expansionPlan.length }
      )
    );
  };

  const handleValidate = async () => {
    if (!preflightApproved) {
      message.error(
        t(
          'Validação bloqueada: execute preflight e mantenha VMs aptas.',
          'Validation blocked: run preflight and keep VMs ready.'
        )
      );
      return;
    }
    if (validationIssues.length > 0) {
      message.error(
        t(
          'Validação local bloqueada ({count} pendências). Falta implementar: {details}',
          'Local validation blocked ({count} pending items). Missing implementation: {details}',
          {
            count: validationIssues.length,
            details: describeMissingRequirements(validationIssues),
          }
        )
      );
      return;
    }

    setIsWizardLinting(true);
    await runWizardLint({ showSuccessMessage: true });
    setIsWizardLinting(false);
  };

  const exportGuidedBlueprint = format => {
    if (!installStepReady) {
      message.error(
        t(
          'Conclua todas as etapas do wizard antes de exportar blueprint.',
          'Complete all wizard steps before exporting the blueprint.'
        )
      );
      return;
    }

    const extension = format === 'yaml' ? 'yaml' : 'json';
    const serialized = serializeBlueprintDocument(guidedBlueprintDraft, extension);
    const safeChangeId = String(changeId || t('change-sem-id', 'change-no-id')).replace(
      /[^a-zA-Z0-9_-]+/g,
      '-'
    );
    const fileName = `guided-blueprint-${safeChangeId}.${extension}`;
    const mimeType =
      extension === 'yaml' ? 'text/yaml;charset=utf-8' : 'application/json;charset=utf-8';

    downloadTextFile(fileName, serialized, mimeType);
    appendAuditEvent('guided_blueprint_exported', {
      format: extension,
      file_name: fileName,
    });
    message.success(
      t(
        'Blueprint guiado exportado em {format}.',
        'Guided blueprint exported in {format}.',
        { format: extension.toUpperCase() }
      )
    );
  };

  const handleImportGuidedBlueprint = file => {
    const reader = new FileReader();

    reader.onload = event => {
      try {
        const filePayload = String((event && event.target && event.target.result) || '');
        const inferredFormat = inferBlueprintDocumentFormat(file.name);
        const parsedBlueprint = parseBlueprintDocument(filePayload, inferredFormat);
        const importedModel = importGuidedTopologyFromBlueprint(parsedBlueprint);

        if (importedModel.changeId) {
          setChangeId(importedModel.changeId);
        }
        setEnvironmentProfile(normalizeEnvironmentProfile(importedModel.environmentProfile));
        setMachines(importedModel.machines || [createMachineDraft(1)]);
        setOrganizations(importedModel.organizations || [createOrganizationDraft(1)]);
        setChannels(importedModel.channels || []);
        setChaincodeInstalls(importedModel.chaincodeInstalls || []);
        setApiRegistrations([]);
        setBusinessGroupName(importedModel.businessGroupName || '');
        setBusinessGroupDescription(importedModel.businessGroupDescription || '');
        setBusinessGroupNetworkId(importedModel.businessGroupNetworkId || '');
        setWizardLintReport(null);
        setPreflightApprovedSignature('');

        appendAuditEvent('guided_blueprint_imported', {
          file_name: file.name,
          inferred_format: inferredFormat,
        });
        message.success(
          t(
            "Blueprint '{name}' importado para o modo guiado.",
            "Blueprint '{name}' imported into guided mode.",
            { name: file.name }
          )
        );
      } catch (error) {
        appendAuditEvent('guided_blueprint_import_failed', {
          file_name: file.name,
        });
        message.error(
          t(
            "Falha ao importar '{name}': documento JSON/YAML inválido.",
            "Failed to import '{name}': invalid JSON/YAML document.",
            { name: file.name }
          )
        );
      }
    };

    reader.readAsText(file);
    return false;
  };

  const handleExport = async () => {
    if (!installStepReady) {
      message.error(
        t(
          'Conclua todas as etapas do wizard antes de exportar o seed técnico.',
          'Complete all wizard steps before exporting the technical seed.'
        )
      );
      return;
    }
    if (validationIssues.length > 0) {
      message.error(
        t(
          'Corrija as pendências antes de exportar o rascunho técnico.',
          'Fix pending issues before exporting the technical draft.'
        )
      );
      return;
    }

    let lintReady = lintApprovedForCurrentModel;
    if (!lintReady) {
      setIsWizardLinting(true);
      const lintOutcome = await runWizardLint({ showSuccessMessage: false });
      setIsWizardLinting(false);
      lintReady = lintOutcome.valid;
    }

    if (!lintReady) {
      message.error(
        t(
          'Exportação bloqueada: lint A1.2 deve estar aprovado para o modelo atual.',
          'Export blocked: A1.2 lint must be approved for the current model.'
        )
      );
      return;
    }

    const safeChangeId = String(changeId || t('change-sem-id', 'change-no-id')).replace(
      /[^a-zA-Z0-9_-]+/g,
      '-'
    );
    downloadJsonFile(`infra-seed-${safeChangeId}.json`, infrastructureSeed);
    appendAuditEvent('infra_seed_exported', {
      file_name: `infra-seed-${safeChangeId}.json`,
      lint_source: wizardLintReport?.lintSource || 'degraded-local',
    });
    message.success(
      t('Rascunho técnico exportado em JSON.', 'Technical draft exported as JSON.')
    );
  };

  const handlePublishAndOpenRunbook = async () => {
    if (!preflightApproved) {
      message.error(
        t(
          'Fluxo bloqueado: execute preflight e mantenha todos os hosts em status apto.',
          'Flow blocked: run preflight and keep all hosts in ready status.'
        )
      );
      return;
    }
    if (!installStepReady) {
      message.error(
        t(
          'Conclua todas as etapas do wizard antes de abrir o runbook.',
          'Complete all wizard steps before opening the runbook.'
        )
      );
      return;
    }
    if (validationIssues.length > 0) {
      message.error(
        t(
          'Fluxo bloqueado: há {count} pendência(s) de modelagem. Falta implementar: {details}',
          'Flow blocked: there are {count} modeling pending item(s). Missing implementation: {details}',
          {
            count: validationIssues.length,
            details: describeMissingRequirements(validationIssues),
          }
        )
      );
      return;
    }

    setIsPublishingAndOpeningRunbook(true);
    try {
      if (!OFFICIAL_BLUEPRINT_ENDPOINTS_ENABLED) {
        message.error(
          t(
            'Fluxo operacional bloqueado: publicação oficial de blueprint indisponível neste ambiente.',
            'Operational flow blocked: official blueprint publication is unavailable in this environment.'
          )
        );
        appendAuditEvent('onboarding_publish_blocked', {
          reason_code: 'publish_backend_disabled_operational_mode',
        });
        return;
      }

      let lintReady = lintApprovedForCurrentModel;
      if (!lintReady) {
        const lintOutcome = await runWizardLint({
          showSuccessMessage: false,
          requireOfficial: true,
        });
        lintReady = lintOutcome.valid;
      }

      if (!lintReady) {
        message.error(
          t(
            'Fluxo bloqueado: lint A1.2 deve estar aprovado para o modelo atual.',
            'Flow blocked: A1.2 lint must be approved for the current model.'
          )
        );
        return;
      }

      const executionContext = t(
        'Onboarding SSH -> blueprint -> runbook (A1.6.4) com host mapping resolvido [external-linux]',
        'SSH onboarding -> blueprint -> runbook (A1.6.4) with resolved host mapping [external-linux]'
      );
      const publishResponse = await publishBlueprintVersion({
        blueprint: guidedBlueprintDraft,
        change_id: String(changeId || '').trim(),
        execution_context: executionContext,
        allow_migration: false,
      });

      if (publishResponse && publishResponse.status === 'fail') {
        const backendFailureMessage =
          toErrorDetailText(publishResponse?.data?.detail) ||
          toErrorDetailText(publishResponse?.data?.message) ||
          toErrorDetailText(publishResponse?.message) ||
          toErrorDetailText(publishResponse?.msg) ||
          t(
            'Backend rejeitou a publicação do blueprint.',
            'Backend rejected the blueprint publication.'
          );
        const backendFailureCode =
          toText(publishResponse?.data?.code) ||
          toText(publishResponse?.code) ||
          toText(publishResponse?.msg);
        const backendFailureStatus =
          Number(
            publishResponse?.statusCode ||
              publishResponse?.status_code ||
              publishResponse?.http_status ||
              publishResponse?.response?.status ||
              400
          ) || 400;

        const backendFailureError = new Error(backendFailureMessage);
        backendFailureError.data = {
          ...(publishResponse?.data || {}),
          code: backendFailureCode,
          detail: backendFailureMessage,
          message: backendFailureMessage,
        };
        backendFailureError.response = {
          status: backendFailureStatus,
        };
        throw backendFailureError;
      }

      const publishedRecord =
        publishResponse && publishResponse.status === 'successful' && publishResponse.data
          ? publishResponse.data
          : null;

      if (!publishedRecord) {
        throw new Error(
          t(
            'Resposta de publicação de blueprint fora do contrato esperado.',
            'Blueprint publication response is outside the expected contract.'
          )
        );
      }

      const runId = deterministicRunId({
        changeId: String(changeId || '').trim(),
        blueprintFingerprint: String(publishedRecord.fingerprint_sha256 || '').trim(),
        resolvedSchemaVersion: String(publishedRecord.resolved_schema_version || '1.0.0').trim(),
      });

      const runbookHandoff = buildOnboardingRunbookHandoff({
        changeId,
        environmentProfile,
        runId,
        guidedBlueprint: guidedBlueprintDraft,
        machines,
        machineCredentials: machineCredentialBindings,
        organizations,
        channels,
        chaincodeInstalls,
        businessGroupName,
        businessGroupDescription,
        businessGroupNetworkId,
        publishedBlueprintRecord: publishedRecord,
        preflightReport,
        modelingAuditTrail,
        executionContext,
        apiRegistry: infrastructureSeed?.network?.apis || [],
        incrementalExpansions: infrastructureSeed?.network?.incremental_expansions || [],
        officialPublish: true,
      });
      persistOnboardingRunbookHandoff(runbookHandoff);

      appendAuditEvent('onboarding_blueprint_published', {
        blueprint_version: String(publishedRecord.blueprint_version || '').trim(),
        blueprint_fingerprint: String(publishedRecord.fingerprint_sha256 || '').trim(),
      });
      appendAuditEvent('onboarding_runbook_handoff_created', {
        run_id: runId,
        handoff_fingerprint: toText(runbookHandoff?.handoff_fingerprint),
        handoff_contract_version: toText(runbookHandoff?.handoff_contract_version),
        host_mapping_entries: Array.isArray(runbookHandoff.host_mapping)
          ? runbookHandoff.host_mapping.length
          : 0,
      });

      message.success(
        t(
          'Blueprint {version} publicado e execução {runId} preparada.',
          'Blueprint {version} published and execution {runId} prepared.',
          {
            version: String(publishedRecord.blueprint_version || '-'),
            runId,
          }
        )
      );
      history.push(RUNBOOK_ROUTE_PATH);
    } catch (error) {
      const publishFailure = resolvePublishIntegrationError(error);
      appendAuditEvent('onboarding_publish_failed', {
        reason_code:
          publishFailure.backendCode ||
          (publishFailure.tokenInvalid
            ? 'token_not_valid'
            : 'onboarding_publish_integration_failure'),
        http_status: publishFailure.statusCode || null,
        detail: publishFailure.backendDetail || publishFailure.userMessage,
      });
      message.error(
        t(
          'Falha na integração onboarding -> runbook: {message}',
          'Failure in onboarding -> runbook integration: {message}',
          { message: publishFailure.userMessage }
        )
      );

      if (publishFailure.diagnostics) {
        Modal.error({
          title: t(
            'Diagnóstico técnico da falha de publicação',
            'Technical diagnostics for publication failure'
          ),
          content: publishFailure.diagnostics,
          okText: t('Fechar', 'Close'),
        });
      }
    } finally {
      setIsPublishingAndOpeningRunbook(false);
    }
  };

  const buildReadinessTooltip = readiness => {
    if (!readiness.available) {
      return t('Ação bloqueada: {reason}', 'Action blocked: {reason}', {
        reason: readiness.reason,
      });
    }
    return t('Modo: {mode}. {reason}', 'Mode: {mode}. {reason}', {
      mode: readiness.modeLabel,
      reason: readiness.reason,
    });
  };

  const getBlockedStepReason = targetStepIndex => {
    if (targetStepIndex === 1 && !infraStepReady) {
      return t(
        'Conclua a etapa 1 (infra + preflight apto) antes de abrir Organizations.',
        'Complete step 1 (infra + ready preflight) before opening Organizations.'
      );
    }
    if (targetStepIndex === 2 && !organizationsStepReady) {
      return t('Conclua Organizations antes de configurar Nodes.', 'Complete Organizations before configuring Nodes.');
    }
    if (targetStepIndex === 3 && !nodesStepReady) {
      return t(
        'Conclua Nodes (peers/orderers) antes de configurar Business Groups.',
        'Complete Nodes (peers/orderers) before configuring Business Groups.'
      );
    }
    if (targetStepIndex === 4 && !businessGroupsStepReady) {
      return t(
        'Conclua Business Groups antes de configurar Channels.',
        'Complete Business Groups before configuring Channels.'
      );
    }
    if (targetStepIndex === 5 && !channelsStepReady) {
      return t(
        'Conclua Channels antes de configurar Install Chaincodes.',
        'Complete Channels before configuring Install Chaincodes.'
      );
    }
    return t(
      'Etapa bloqueada por dependência técnica anterior.',
      'Step blocked by a previous technical dependency.'
    );
  };

  const handleSelectStep = targetStepIndex => {
    if (targetStepIndex > maxUnlockedStep) {
      message.warning(getBlockedStepReason(targetStepIndex));
      return;
    }
    setActiveStepIndex(targetStepIndex);
  };

  const handleNextStep = () => {
    if (activeStepIndex === 0 && !infraStepReady) {
      message.warning(
        t(
          'Etapa 1 ainda não está apta: execute preflight e corrija pendências.',
          'Step 1 is not ready yet: run preflight and fix pending issues.'
        )
      );
      return;
    }
    if (activeStepIndex === 1 && !organizationsStepReady) {
      message.warning(
        t(
          'Etapa 2 ainda possui pendências de Organizations.',
          'Step 2 still has pending Organizations issues.'
        )
      );
      return;
    }
    if (activeStepIndex === 2 && !nodesStepReady) {
      message.warning(
        t(
          'Etapa 3 ainda possui pendências de Nodes (peers/orderers).',
          'Step 3 still has pending Nodes (peers/orderers) issues.'
        )
      );
      return;
    }
    if (activeStepIndex === 3 && !businessGroupsStepReady) {
      message.warning(
        t(
          'Etapa 4 ainda possui pendência de Business Group.',
          'Step 4 still has a pending Business Group issue.'
        )
      );
      return;
    }
    if (activeStepIndex === 4 && !channelsStepReady) {
      message.warning(
        t(
          'Etapa 5 ainda possui pendências de Channels.',
          'Step 5 still has pending Channels issues.'
        )
      );
      return;
    }
    setActiveStepIndex(current => Math.min(infraWizardSteps.length - 1, current + 1));
  };

  const handleOpenRunbookAuditFromHistory = useCallback(historyEntry => {
    if (!historyEntry || !historyEntry.runId) {
      return;
    }

    const sessionStorage = getBrowserSessionStorage();
    if (sessionStorage) {
      sessionStorage.setItem(
        RUNBOOK_AUDIT_SELECTED_STORAGE_KEY,
        JSON.stringify({
          key: historyEntry.key,
          runId: historyEntry.runId,
          capturedAt: historyEntry.capturedAt,
        })
      );
    }

    history.push(RUNBOOK_ROUTE_PATH);
  }, []);

  const activeStep = infraWizardSteps[activeStepIndex];
  const isFirstStep = activeStepIndex === 0;
  const isLastStep = activeStepIndex === infraWizardSteps.length - 1;
  const environmentProfileHelp = getEnvironmentProfileHelp(environmentProfile);
  const localizedScreenTitle = t(
    'Provisionamento de Infraestrutura via SSH',
    'Infrastructure Provisioning via SSH'
  );
  const localizedScreenSubtitle = t(
    'Cadastro operacional de VMs Linux por acesso SSH para preparar infraestrutura base sem edição manual de YAML/JSON.',
    'Operational registration of Linux VMs through SSH access to prepare the base infrastructure without manual YAML/JSON editing.'
  );
  const localizedScreenObjective = t(
    'Fluxo guiado com dependência técnica real',
    'Guided flow with real technical dependencies'
  );

  return (
    <OperationalWindowManagerProvider>
      <NeoOpsLayout
      screenKey={PROVISIONING_INFRA_SCREEN_KEY}
      sectionLabel={PROVISIONING_SECTION_LABEL}
      title={localizedScreenTitle}
      subtitle={localizedScreenSubtitle}
      navItems={provisioningNavItems}
      activeNavKey={resolveProvisioningActiveNavKey(PROVISIONING_INFRA_SCREEN_KEY)}
      breadcrumbs={getProvisioningBreadcrumbs(PROVISIONING_INFRA_SCREEN_KEY)}
      toolbar={
        <Button
          type="primary"
          icon={<ArrowRightOutlined />}
          onClick={() => setIsInfraWizardDialogOpen(true)}
        >
          {t('Abrir cockpit guiado de Infra SSH', 'Open guided Infra SSH cockpit')}
        </Button>
      }
    >
      <Alert
        showIcon
        type="info"
        message={localizedScreenObjective}
        description={t(
          'A ordem obrigatória desta tela é: Infra/VM apta -> Organizations -> Nodes -> Business Groups -> Channels -> Install Chaincodes (.tar.gz).',
          'The required order for this screen is: Infra/VM ready -> Organizations -> Nodes -> Business Groups -> Channels -> Install Chaincodes (.tar.gz).'
        )}
      />

      <div className={styles.neoCard}>
        <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
          <div>
            <Typography.Text className={styles.neoLabel}>
              {t('Assistente de etapa ativa', 'Active step assistant')}
            </Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Etapa {step}: {title}', 'Step {step}: {title}', {
                step: activeStepIndex + 1,
                title: activeStep.title,
              })}
            </Typography.Title>
            <Typography.Paragraph className={styles.neoLabel} style={{ marginBottom: 0 }}>
              {t(
                'Abra o cockpit guiado para executar preflight, validar topologia, exportar seed e publicar.',
                'Open the guided cockpit to run preflight, validate topology, export the seed, and publish.'
              )}
            </Typography.Paragraph>
          </div>
          <Button
            type="primary"
            icon={<ArrowRightOutlined />}
            onClick={() => setIsInfraWizardDialogOpen(true)}
          >
            {t('Abrir cockpit guiado', 'Open guided cockpit')}
          </Button>
        </Space>
      </div>

      <div className={styles.neoCard}>
        <Space align="start" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
          <div>
            <Typography.Text className={styles.neoLabel}>
              {t('Auditoria operacional', 'Operational audit')}
            </Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Histórico de provisionamentos', 'Provisioning history')}
            </Typography.Title>
            <Typography.Paragraph className={styles.neoLabel} style={{ marginBottom: 0 }}>
              {t(
                'Consulte execuções concluídas com resumo do contexto e acesso direto à auditoria detalhada no runbook.',
                'Review completed executions with a context summary and direct access to the detailed runbook audit.'
              )}
            </Typography.Paragraph>
          </div>
          <Button onClick={loadProvisioningAuditHistory}>
            {t('Atualizar histórico', 'Refresh history')}
          </Button>
        </Space>

        <div style={{ marginTop: 12 }}>
          {provisioningAuditHistory.length === 0 ? (
            <Alert
              showIcon
              type="info"
              message={t(
                'Nenhum provisionamento concluído até o momento',
                'No completed provisioning yet'
              )}
              description={t(
                'Quando um runbook concluir, a execução será exibida aqui para auditoria.',
                'Once a runbook completes, the execution will appear here for auditing.'
              )}
            />
          ) : (
            <Collapse ghost expandIconPosition="right">
              {provisioningAuditHistory.map(historyEntry => {
                const historyTone = resolveAuditHistoryTone(historyEntry.status, locale);
                const historyOrganizations = historyEntry.context.organizations;
                const historyTopologyKnown =
                  historyOrganizations.length > 0 ||
                  toSafePositiveInt(historyEntry.context.hostCount) > 0 ||
                  toSafePositiveInt(historyEntry.context.organizationCount) > 0 ||
                  toSafePositiveInt(historyEntry.context.nodeCount) > 0;
                return (
                  <CollapsePanel
                    key={historyEntry.key}
                    header={
                      <Space wrap>
                        <Typography.Text strong>{historyEntry.runId || '-'}</Typography.Text>
                        <Tag color={historyTone.color}>{historyTone.label}</Tag>
                        <Typography.Text type="secondary">
                          {t('status {status} | fim UTC {finishedAt}', 'status {status} | end UTC {finishedAt}', {
                            status: historyEntry.status || '-',
                            finishedAt: historyEntry.finishedAt || '-',
                          })}
                        </Typography.Text>
                      </Space>
                    }
                  >
                    <div style={{ display: 'grid', gap: 10 }}>
                      <div className={styles.chipRow}>
                        <span className={styles.chip}>{`change_id: ${historyEntry.changeId ||
                          '-'}`}</span>
                        <span className={styles.chip}>
                          {t('ambiente: {value}', 'environment: {value}', {
                            value: historyEntry.context.environmentProfile || '-',
                          })}
                        </span>
                        <span className={styles.chip}>
                          {`provider: ${historyEntry.context.providerKey || '-'}`}
                        </span>
                        <span className={styles.chip}>
                          {`hosts: ${
                            historyTopologyKnown ? historyEntry.context.hostCount : 'n/d'
                          }`}
                        </span>
                        <span className={styles.chip}>
                          {t('organizações: {value}', 'organizations: {value}', {
                            value: historyTopologyKnown
                              ? historyEntry.context.organizationCount
                              : t('n/d', 'n/a'),
                          })}
                        </span>
                        <span className={styles.chip}>
                          {`nodes: ${
                            historyTopologyKnown ? historyEntry.context.nodeCount : 'n/d'
                          }`}
                        </span>
                        <span className={styles.chip}>
                          {`APIs: ${historyEntry.context.apiCount}`}
                        </span>
                        <span className={styles.chip}>
                          {t('incrementais: {value}', 'incremental: {value}', {
                            value: historyEntry.context.incrementalCount,
                          })}
                        </span>
                      </div>
                      {historyOrganizations.length > 0 && (
                        <div className={styles.commandBlock}>
                          {historyOrganizations.map(organization => (
                            <div
                              key={`${historyEntry.key}-${organization.orgId ||
                                organization.orgName}`}
                            >
                              {`${organization.orgName || organization.orgId} | peers=${
                                organization.peerCount
                              }, orderers=${organization.ordererCount}, CA=${
                                organization.caCount
                              }, APIs=${organization.apiCount}`}
                            </div>
                          ))}
                        </div>
                      )}
                      <Space wrap>
                        <Button
                          type="primary"
                          size="small"
                          onClick={() => handleOpenRunbookAuditFromHistory(historyEntry)}
                        >
                          {t('Abrir auditoria no runbook', 'Open audit in runbook')}
                        </Button>
                      </Space>
                    </div>
                  </CollapsePanel>
                );
              })}
            </Collapse>
          )}
        </div>
      </div>

      <OperationalWindowDialog
        windowId="provisioning-infra-ssh-cockpit"
        title={t('Cockpit guiado de Provisionamento SSH', 'Guided SSH Provisioning cockpit')}
        eyebrow={t('Workspace de provisionamento', 'Provisioning workspace')}
        open={isInfraWizardDialogOpen}
        onClose={() => setIsInfraWizardDialogOpen(false)}
        preferredWidth="92vw"
        preferredHeight="82vh"
      >
        <div className={styles.windowWorkspace}>
          <div className={styles.windowWorkspaceBody}>
            <div className={styles.neoCard}>
          <Space align="start" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
            <div>
              <Typography.Text className={styles.neoLabel}>
                {t('Etapa ativa', 'Active step')}
              </Typography.Text>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Etapa {step}: {title}', 'Step {step}: {title}', {
                  step: activeStepIndex + 1,
                  title: activeStep.title,
                })}
              </Typography.Title>
            </div>
            <Space wrap size={8}>
              <Tag color="blue">
                {t('Passo {current}/{total}', 'Step {current}/{total}', {
                  current: activeStepIndex + 1,
                  total: infraWizardSteps.length,
                })}
              </Tag>
              <Tag color={activeStepIndex <= maxUnlockedStep ? 'green' : 'default'}>
                {activeStepIndex <= maxUnlockedStep
                  ? t('fluxo liberado', 'flow enabled')
                  : t('aguardando dependência', 'waiting for dependency')}
              </Tag>
            </Space>
          </Space>

          <div className={styles.steps} style={{ marginTop: 12 }}>
            {infraWizardSteps.map((step, index) => {
              const isStepUnlocked = index <= maxUnlockedStep;
              const isStepActive = index === activeStepIndex;

              return (
                <button
                  key={step.key}
                  type="button"
                  className={
                    isStepActive
                      ? `${styles.step} ${styles.stepActive} ${styles.stepButton}`
                      : `${styles.step} ${styles.stepButton}`
                  }
                  disabled={!isStepUnlocked}
                  style={!isStepUnlocked ? { opacity: 0.45, cursor: 'not-allowed' } : undefined}
                  onClick={() => handleSelectStep(index)}
                >
                  <Typography.Text className={styles.neoLabel}>
                    {t('Etapa {step}', 'Step {step}', { step: index + 1 })}
                  </Typography.Text>
                  <Typography.Text className={styles.neoValue}>{step.title}</Typography.Text>
                </button>
              );
            })}
          </div>
            </div>

            {activeStepIssues.length > 0 && (
          <Alert
            showIcon
            type="warning"
            message={t(
              'Pendências da etapa {step}: {count}',
              'Pending items for step {step}: {count}',
              { step: activeStepIndex + 1, count: activeStepIssues.length }
            )}
            description={activeStepIssues[0]}
          />
        )}

            {activeStepIndex === 0 && preflightApproved && (
          <Alert
            showIcon
            type="success"
            message={t('Infraestrutura apta', 'Infrastructure ready')}
            description={t(
              'Preflight aprovado com status apto para todas as VMs. A etapa de Organizations está liberada.',
              'Preflight approved with ready status for all VMs. The Organizations step is now enabled.'
            )}
          />
        )}

            {activeStepIndex === 0 && (
          <div className={styles.neoGrid2}>
            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Conexão SSH e VMs Linux', 'SSH connection and Linux VMs')}
              </Typography.Title>
              <Alert
                showIcon
                type="info"
                style={{ marginBottom: 10 }}
                message={t('Fluxo simplificado para operador', 'Simplified operator flow')}
                description={t(
                  'Escopo obrigatório: provider external-linux + VM Linux. Defina a chave SSH .pem por máquina, preencha SSH User, IP da máquina e SSH Port. Docker Port é opcional e usa 2376 por padrão. O change_id é gerado automaticamente.',
                  'Required scope: external-linux provider + Linux VM. Define the SSH .pem key per machine, fill SSH User, machine IP, and SSH Port. Docker Port is optional and defaults to 2376. The change_id is generated automatically.'
                )}
              />
              <Button
                size="small"
                icon={<QuestionCircleOutlined />}
                style={{ marginBottom: 12 }}
                onClick={() => setShowPemHelp(current => !current)}
              >
                {showPemHelp
                  ? t('Ocultar ajuda de geração .pem', 'Hide .pem generation help')
                  : t('Como gerar arquivo .pem', 'How to generate a .pem file')}
              </Button>
              {showPemHelp && (
                <Alert
                  showIcon
                  type="info"
                  message={t('Ajuda rápida: gerar chave .pem para SSH', 'Quick help: generate SSH .pem key')}
                  description={
                    <div>
                      <Typography.Paragraph style={{ marginBottom: 8 }}>
                        {t(
                          'Edite os parâmetros (KEY_FILE, USER_HOST, PORT) e execute o script abaixo em um terminal Linux/macOS.',
                          'Edit the parameters (KEY_FILE, USER_HOST, PORT) and run the script below in a Linux/macOS terminal.'
                        )}
                      </Typography.Paragraph>
                      <Typography.Paragraph style={{ marginBottom: 8 }}>
                        {t(
                          'Depois, envie o arquivo .pem no botão “Selecionar chave privada (.pem)”.',
                          'Then upload the .pem file using the “Select private key (.pem)” button.'
                        )}
                      </Typography.Paragraph>
                      <Typography.Paragraph style={{ marginBottom: 8 }}>
                        {t('Exemplo direto de acesso com chave local:', 'Direct access example with local key:')}{' '}
                        <strong>
                          ssh -i &lt;file.pem&gt; -p &lt;port&gt;
                          &lt;user&gt;@&lt;ip-or-host&gt;
                        </strong>
                      </Typography.Paragraph>
                      <Typography.Paragraph copyable style={{ marginBottom: 0 }}>
                        <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{PEM_HELP_SCRIPT}</pre>
                      </Typography.Paragraph>
                    </div>
                  }
                  style={{ marginBottom: 12 }}
                />
              )}
              <div className={styles.formGrid2}>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('ambiente (opcional)', 'environment (optional)')}
                  </Typography.Text>
                  <Space wrap>
                    <Tag color={environmentProfile === 'dev-external-linux' ? 'green' : 'blue'}>
                      {environmentProfile}
                    </Tag>
                    <Button
                      size="small"
                      onClick={() => setShowEnvironmentSelector(current => !current)}
                    >
                      {showEnvironmentSelector
                        ? t('Ocultar seleção de ambiente', 'Hide environment selector')
                        : t('Alterar ambiente (hml/prod)', 'Change environment (hml/prod)')}
                    </Button>
                    <Button size="small" onClick={() => setChangeId(buildAutoChangeId())}>
                      {t('Gerar novo change_id', 'Generate new change_id')}
                    </Button>
                  </Space>
                  {showEnvironmentSelector && (
                    <Select
                      value={environmentProfile}
                      options={environmentProfileOptions}
                      onChange={value => setEnvironmentProfile(normalizeEnvironmentProfile(value))}
                      style={{ marginTop: 8 }}
                    />
                  )}
                  <Typography.Text className={styles.neoLabel}>
                    {environmentProfileHelp}
                  </Typography.Text>
                </div>
              </div>

              <div className={styles.line} />

              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                {machines.map((machine, index) => (
                  <div key={machine.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>
                        {t('Máquina {index}', 'Machine {index}', { index: index + 1 })}
                      </Typography.Text>
                      <Button
                        danger
                        size="small"
                        disabled={machines.length === 1}
                        onClick={() =>
                          setMachines(current =>
                            current.filter(currentMachine => currentMachine.id !== machine.id)
                          )
                        }
                      >
                        {t('Remover', 'Remove')}
                      </Button>
                    </Space>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          host_address
                        </Typography.Text>
                        <Input
                          value={machine.hostAddress}
                          onChange={({ target: { value } }) =>
                            setMachines(current =>
                              updateCollectionRow(current, machine.id, 'hostAddress', value)
                            )
                          }
                          placeholder={t(
                            'ex.: 203.0.113.10 ou vm-exemplo.cognus.local',
                            'e.g. 203.0.113.10 or example-vm.cognus.local'
                          )}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          ssh_user
                        </Typography.Text>
                        <Input
                          value={machine.sshUser}
                          onChange={({ target: { value } }) =>
                            setMachines(current =>
                              updateCollectionRow(current, machine.id, 'sshUser', value)
                            )
                          }
                          placeholder={t('ex.: operador-linux', 'e.g. operator-linux')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          ssh_port
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={machine.sshPort}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setMachines(current =>
                              updateCollectionRow(
                                current,
                                machine.id,
                                'sshPort',
                                Number(value || 22)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t(
                            'docker_port (opcional; padrão 2376)',
                            'docker_port (optional; default 2376)'
                          )}
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={machine.dockerPort}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setMachines(current =>
                              updateCollectionRow(current, machine.id, 'dockerPort', value || null)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('chave SSH por máquina (.pem local)', 'SSH key per machine (local .pem)')}
                        </Typography.Text>
                        <Space direction="vertical" style={{ width: '100%' }} size={6}>
                          <Space wrap>
                            <Upload
                              beforeUpload={() => false}
                              showUploadList={false}
                              maxCount={1}
                              accept=".pem"
                              onChange={async info => {
                                const selection = resolvePemUploadSelection(info);
                                if (!selection) {
                                  message.error(
                                    t(
                                      'Não foi possível ler o arquivo .pem selecionado.',
                                      'Could not read the selected .pem file.'
                                    )
                                  );
                                  return;
                                }
                                const { fileName, rawFile } = selection;

                                if (!/\.pem$/i.test(fileName)) {
                                  message.error(
                                    t(
                                      'Formato inválido: selecione um arquivo .pem.',
                                      'Invalid format: select a .pem file.'
                                    )
                                  );
                                  return;
                                }

                                try {
                                  const base64Payload = await fileToBase64(rawFile);
                                  const content =
                                    typeof rawFile.text === 'function'
                                      ? await rawFile.text()
                                      : await fileToText(rawFile);
                                  const fingerprintHex = await computeFingerprintHex(
                                    content,
                                    base64Payload
                                  );

                                  setMachines(current =>
                                    current.map(currentMachine =>
                                      currentMachine.id === machine.id
                                        ? {
                                            ...currentMachine,
                                            sshCredentialRef: `local-file:${fileName}`,
                                            sshCredentialPayload: base64Payload,
                                            sshCredentialFingerprint: fingerprintHex,
                                          }
                                        : currentMachine
                                    )
                                  );
                                } catch (error) {
                                  message.error(
                                    t(
                                      'Falha ao ler o arquivo .pem local.',
                                      'Failed to read the local .pem file.'
                                    )
                                  );
                                }
                              }}
                            >
                              <Button size="small" icon={<UploadOutlined />}>
                                {t('Selecionar chave privada (.pem)', 'Select private key (.pem)')}
                              </Button>
                            </Upload>
                            {String(machine.sshCredentialRef || '').startsWith('local-file:') && (
                              <Button
                                size="small"
                                onClick={() =>
                                  setMachines(current =>
                                    current.map(currentMachine =>
                                      currentMachine.id === machine.id
                                        ? {
                                            ...currentMachine,
                                            sshCredentialRef: '',
                                            sshCredentialPayload: '',
                                            sshCredentialFingerprint: '',
                                          }
                                        : currentMachine
                                    )
                                  )
                                }
                              >
                                {t('Remover chave desta máquina', 'Remove key from this machine')}
                              </Button>
                            )}
                          </Space>
                        </Space>
                        <Typography.Text className={styles.neoLabel}>
                          {String(machine.sshCredentialRef || '').startsWith('local-file:')
                            ? t(
                                'Arquivo selecionado: {name}',
                                'Selected file: {name}',
                                {
                                  name: String(machine.sshCredentialRef || '')
                                    .replace(/^local-file:/, '')
                                    .trim(),
                                }
                              )
                            : t(
                                'Obrigatório por máquina. Sem arquivo .pem o preflight fica bloqueado.',
                                'Required per machine. Without a .pem file, preflight remains blocked.'
                              )}
                        </Typography.Text>
                      </div>
                    </div>
                  </div>
                ))}
                <Button
                  icon={<PlusOutlined />}
                  onClick={() =>
                    setMachines(current => [...current, createMachineDraft(current.length + 1)])
                  }
                >
                  {t('Adicionar máquina', 'Add machine')}
                </Button>
              </Space>
            </div>

            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Gate obrigatório da infraestrutura', 'Required infrastructure gate')}
              </Typography.Title>
              <Typography.Paragraph style={{ marginBottom: 8 }}>
                {t(
                  'Só é permitido avançar para Organizations quando todas as VMs estiverem aptas no preflight e sem risco de sobrescrita por containers ativos.',
                  'You can only advance to Organizations when all VMs are ready in preflight and there is no overwrite risk from active containers.'
                )}
              </Typography.Paragraph>

              <Space wrap size={8}>
                <Button type="primary" onClick={handleRunPreflight} loading={isRunningPreflight}>
                  {t('Executar preflight técnico', 'Run technical preflight')}
                </Button>
                <Tag color="green">{t('Aptas: {count}', 'Ready: {count}', { count: preflightSummary.apto })}</Tag>
                <Tag color="gold">{t('Parciais: {count}', 'Partial: {count}', { count: preflightSummary.parcial })}</Tag>
                <Tag color="red">{t('Bloqueadas: {count}', 'Blocked: {count}', { count: preflightSummary.bloqueado })}</Tag>
                <Tag color={preflightApproved ? 'green' : 'orange'}>
                  {preflightApproved ? t('Gate liberado', 'Gate enabled') : t('Gate bloqueado', 'Gate blocked')}
                </Tag>
              </Space>

              {preflightReport && (
                <Typography.Paragraph className={styles.neoLabel} style={{ marginTop: 8 }}>
                  {t(
                    'Último preflight UTC: {executedAt} | change_id: {changeId}',
                    'Latest preflight UTC: {executedAt} | change_id: {changeId}',
                    {
                      executedAt: preflightReport.executedAtUtc,
                      changeId: preflightReport.changeId,
                    }
                  )}
                </Typography.Paragraph>
              )}

              {preflightNeedsRefresh && (
                <Alert
                  showIcon
                  type="warning"
                  style={{ marginTop: 10 }}
                  message={t('Preflight desatualizado', 'Outdated preflight')}
                  description={t(
                    'Houve alteração em VM/SSH após a última execução. Reexecute o preflight para liberar o próximo passo.',
                    'VM/SSH data changed after the last execution. Re-run preflight to unlock the next step.'
                  )}
                />
              )}

              {hasContainerConflict && (
                <Alert
                  showIcon
                  type="warning"
                  style={{ marginTop: 10 }}
                  message={t(
                    'Risco operacional detectado: containers ativos nas VMs',
                    'Operational risk detected: active containers on the VMs'
                  )}
                  description={t(
                    'Existe(m) {count} host(s) com containers ativos potencialmente sobrescrevíveis. Audite e pare esses containers antes de avançar para Organizations.',
                    'There are {count} host(s) with active containers that may be overwritten. Audit and stop those containers before advancing to Organizations.',
                    { count: containerConflictRows.length }
                  )}
                />
              )}

              <div className={styles.commandBlock} style={{ marginTop: 10 }}>
                {preflightRows.map(row => (
                  <div key={row.id} style={{ marginBottom: 6 }}>
                    <Tag color={PREFLIGHT_STATUS_COLOR[row.status]}>
                      {preflightStatusLabel[row.status]}
                    </Tag>
                    {`${row.infraLabel || row.id} (${row.hostAddress || t('host não informado', 'host not informed')})`}
                    <div>
                      {t(
                        'Causa técnica: {cause}',
                        'Technical cause: {cause}',
                        {
                          cause: localizeInfraOperationalHint(
                            row.primaryCause || t('Sem pendências técnicas.', 'No technical issues.'),
                            locale
                          ),
                        }
                      )}
                    </div>
                    <div>
                      {t(
                        'Ação recomendada: {action}',
                        'Recommended action: {action}',
                        {
                          action: localizeInfraOperationalHint(
                            row.primaryRecommendation ||
                              t('Sem ação necessária.', 'No action required.'),
                            locale
                          ),
                        }
                      )}
                    </div>
                    {hasContainerConflictSignal(row) && (
                      <div>
                        {t(
                          'Containers ativos: {containers}',
                          'Active containers: {containers}',
                          {
                            containers:
                              getRuntimeActiveContainers(row).join(', ') ||
                              t('lista não informada pelo backend', 'list not informed by backend'),
                          }
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </div>

              <Typography.Paragraph style={{ marginTop: 12, marginBottom: 8 }}>
                {t('Comandos SSH de referência:', 'Reference SSH commands:')}
              </Typography.Paragraph>
              <div className={styles.commandBlock}>
                {sshPreviewLines.length > 0
                  ? sshPreviewLines.map(line => <div key={line}>{line}</div>)
                  : t('Nenhum host preenchido para montar comando ssh.', 'No host filled in to build the ssh command.')}
              </div>

              <div className={styles.line} />

              <Typography.Paragraph style={{ marginBottom: 8 }}>
                {t(
                  'Modo avançado (opcional): importação/exportação de blueprint JSON/YAML mantendo o wizard como fluxo padrão.',
                  'Advanced mode (optional): import/export JSON/YAML blueprint while keeping the wizard as the default flow.'
                )}
              </Typography.Paragraph>

              <Space wrap size={8}>
                <Select
                  value={blueprintExportFormat}
                  options={blueprintExportFormatOptions}
                  onChange={value => setBlueprintExportFormat(value)}
                  style={{ minWidth: 120 }}
                />
                <Button
                  icon={<DownloadOutlined />}
                  disabled={!installStepReady}
                  onClick={() => exportGuidedBlueprint(blueprintExportFormat)}
                >
                  {t('Exportar blueprint guiado', 'Export guided blueprint')}
                </Button>
                <Upload
                  beforeUpload={handleImportGuidedBlueprint}
                  showUploadList={false}
                  maxCount={1}
                >
                  <Button icon={<UploadOutlined />}>
                    {t('Importar blueprint JSON/YAML', 'Import JSON/YAML blueprint')}
                  </Button>
                </Upload>
              </Space>

              <Space wrap size={8} style={{ marginTop: 10 }}>
                <Tag color={lintApprovedForCurrentModel ? 'green' : 'red'}>
                  {lintApprovedForCurrentModel
                    ? t(
                        'Lint A1.2 aprovado para o modelo atual',
                        'A1.2 lint approved for the current model'
                      )
                    : t(
                        'Lint A1.2 pendente/bloqueado para o modelo atual',
                        'A1.2 lint pending/blocked for the current model'
                      )}
                </Tag>
                {wizardLintReport?.lintSource && (
                  <Tag color="blue">
                    {t('Fonte: {source}', 'Source: {source}', {
                      source: wizardLintReport.lintSource,
                    })}
                  </Tag>
                )}
                <Tag color="purple">
                  {t(
                    'Trilha de modelagem: {count} evento(s)',
                    'Modeling trail: {count} event(s)',
                    { count: modelingAuditTrail.length }
                  )}
                </Tag>
              </Space>
            </div>
          </div>
        )}
        {activeStepIndex === 1 && (
          <div className={styles.neoGrid2}>
            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                Organizations
              </Typography.Title>
              <Typography.Paragraph>
                {t(
                  'Defina dados de organização e CA. Sem organização com CA válida, o fluxo não libera Nodes e Business Groups.',
                  'Define organization and CA data. Without an organization with a valid CA, the flow does not enable Nodes or Business Groups.'
                )}
              </Typography.Paragraph>
              <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
                {t(
                  'Campos vazios recebem defaults automáticos (API/CA/refs) e podem ser ajustados depois.',
                  'Empty fields receive automatic defaults (API/CA/refs) and can be adjusted later.'
                )}
              </Typography.Paragraph>
              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                {organizations.map((organization, index) => (
                  <div key={organization.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{`Org ${index +
                        1}`}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        disabled={organizations.length === 1}
                        onClick={() =>
                          setOrganizations(current =>
                            current.filter(
                              currentOrganization => currentOrganization.id !== organization.id
                            )
                          )
                        }
                      >
                        {t('Remover', 'Remove')}
                      </Button>
                    </Space>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Nome da organização', 'Organization Name')}
                        </Typography.Text>
                        <Input
                          value={organization.name}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'name', value)
                            )
                          }
                          placeholder={t('ex.: ORG-ALFA', 'e.g. ORG-ALFA')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Domínio', 'Domain')}
                        </Typography.Text>
                        <Input
                          value={organization.domain}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'domain', value)
                            )
                          }
                          placeholder={t('ex.: org-alfa.cognus.local', 'e.g. org-alpha.cognus.local')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Label', 'Label')}
                        </Typography.Text>
                        <Input
                          value={organization.label}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'label', value)
                            )
                          }
                          placeholder={t('ex.: org-alfa', 'e.g. org-alpha')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Host da Network API', 'Network API Host')}
                        </Typography.Text>
                        <Input
                          value={organization.networkApiHost}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'networkApiHost', value)
                            )
                          }
                          placeholder={t(
                            'ex.: api.org-alfa.cognus.local',
                            'e.g. api.org-alpha.cognus.local'
                          )}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Porta da Network API', 'Network API Port')}
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={organization.networkApiPort}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'networkApiPort',
                                value || null
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Modo da CA', 'CA Mode')}
                        </Typography.Text>
                        <Select
                          value={organization.caMode}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'caMode', value)
                            )
                          }
                          options={[
                            { value: 'internal', label: 'internal' },
                            { value: 'external', label: 'external' },
                          ]}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Nome da CA', 'CA Name')}
                        </Typography.Text>
                        <Input
                          value={organization.caName}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'caName', value)
                            )
                          }
                          placeholder={t('ex.: ca-org-alfa', 'e.g. ca-org-alpha')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Host da CA', 'CA Host')}
                        </Typography.Text>
                        <Input
                          value={organization.caHost}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'caHost', value)
                            )
                          }
                          placeholder={t(
                            'ex.: ca.org-alfa.cognus.local',
                            'e.g. ca.org-alpha.cognus.local'
                          )}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Porta da CA', 'CA Port')}
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={organization.caPort}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'caPort',
                                Number(value || 7054)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Usuário da CA', 'CA User')}
                        </Typography.Text>
                        <Input
                          value={organization.caUser}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'caUser', value)
                            )
                          }
                          placeholder={t('ex.: ca-operator', 'e.g. ca-operator')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          CA Password Ref
                        </Typography.Text>
                        <Input
                          value={organization.caPasswordRef}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'caPasswordRef', value)
                            )
                          }
                          placeholder={t('ex.: vault://ca/org-alfa', 'e.g. vault://ca/org-alpha')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          CA Host Ref
                        </Typography.Text>
                        <Select
                          value={organization.caHostRef || undefined}
                          placeholder="Host para CA"
                          options={hostRefOptions}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'caHostRef', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Peer Host Ref
                        </Typography.Text>
                        <Select
                          value={organization.peerHostRef || undefined}
                          placeholder="Host para peers"
                          options={hostRefOptions}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'peerHostRef', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Peer Port Base
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={organization.peerPortBase}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'peerPortBase',
                                Number(value || DEFAULT_PEER_PORT_BASE)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Orderer Host Ref
                        </Typography.Text>
                        <Select
                          value={organization.ordererHostRef || undefined}
                          placeholder="Host para orderers"
                          options={hostRefOptions}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'ordererHostRef', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Orderer Port Base
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={organization.ordererPortBase}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'ordererPortBase',
                                Number(value || DEFAULT_ORDERER_PORT_BASE)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Couch Host Ref
                        </Typography.Text>
                        <Select
                          value={organization.couchHostRef || undefined}
                          placeholder="Host para CouchDB"
                          options={hostRefOptions}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'couchHostRef', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Couch Port
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={organization.couchPort}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'couchPort',
                                Number(value || DEFAULT_COUCH_PORT)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Couch Database
                        </Typography.Text>
                        <Input
                          value={organization.couchDatabase}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'couchDatabase', value)
                            )
                          }
                          placeholder={t('ex.: org-alfa_ledger', 'e.g. org-alpha_ledger')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Couch Admin User
                        </Typography.Text>
                        <Input
                          value={organization.couchAdminUser}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'couchAdminUser', value)
                            )
                          }
                          placeholder={t('ex.: couchdb', 'e.g. couchdb')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Couch Admin Password Ref
                        </Typography.Text>
                        <Input
                          value={organization.couchAdminPasswordRef}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'couchAdminPasswordRef',
                                value
                              )
                            )
                          }
                          placeholder={t(
                            'ex.: vault://couch/org-alfa/admin',
                            'e.g. vault://couch/org-alpha/admin'
                          )}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          API Gateway Host Ref
                        </Typography.Text>
                        <Select
                          value={organization.apiGatewayHostRef || undefined}
                          placeholder="Host para API Gateway"
                          options={hostRefOptions}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'apiGatewayHostRef',
                                value
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          API Gateway Port
                        </Typography.Text>
                        <InputNumber
                          min={1}
                          max={65535}
                          value={organization.apiGatewayPort}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'apiGatewayPort',
                                Number(value || DEFAULT_API_GATEWAY_PORT)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          API Gateway Route Prefix
                        </Typography.Text>
                        <Input
                          value={organization.apiGatewayRoutePrefix}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'apiGatewayRoutePrefix',
                                value
                              )
                            )
                          }
                          placeholder="/api"
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          API Gateway Auth Ref
                        </Typography.Text>
                        <Input
                          value={organization.apiGatewayAuthRef}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'apiGatewayAuthRef',
                                value
                              )
                            )
                          }
                          placeholder={t(
                            'ex.: vault://apigateway/org-alfa/auth',
                            'e.g. vault://apigateway/org-alpha/auth'
                          )}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          NetAPI Host Ref
                        </Typography.Text>
                        <Select
                          value={organization.netApiHostRef || undefined}
                          placeholder="Host para NetAPI"
                          options={hostRefOptions}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(current, organization.id, 'netApiHostRef', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          NetAPI Route Prefix
                        </Typography.Text>
                        <Input
                          value={organization.netApiRoutePrefix}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'netApiRoutePrefix',
                                value
                              )
                            )
                          }
                          placeholder="/network"
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          NetAPI Access Ref
                        </Typography.Text>
                        <Input
                          value={organization.netApiAccessRef}
                          onChange={({ target: { value } }) =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'netApiAccessRef',
                                value
                              )
                            )
                          }
                          placeholder={t(
                            'ex.: vault://netapi/org-alfa/access',
                            'e.g. vault://netapi/org-alpha/access'
                          )}
                        />
                      </div>
                    </div>
                    <Typography.Paragraph
                      type="secondary"
                      style={{ marginTop: 8, marginBottom: 0 }}
                    >
                      {t(
                        'Segredos devem ser informados apenas por referência segura (`vault://`, `secret://`, `ref://`), sem valor sensível em texto plano.',
                        'Secrets must be provided only through secure references (`vault://`, `secret://`, `ref://`), never as plaintext sensitive values.'
                      )}
                    </Typography.Paragraph>
                  </div>
                ))}
                <Button
                  icon={<PlusOutlined />}
                  onClick={() =>
                    setOrganizations(current => [
                      ...current,
                      createOrganizationDraft(current.length + 1),
                    ])
                  }
                >
                  {t('Adicionar organização', 'Add organization')}
                </Button>
              </Space>
            </div>

            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Gate de Organizations', 'Organizations gate')}
              </Typography.Title>
              <Space wrap size={8}>
                <Tag color={infraStepReady ? 'green' : 'red'}>
                  {infraStepReady ? t('Infra apta', 'Infra ready') : t('Infra pendente', 'Infra pending')}
                </Tag>
                <Tag color="cyan">
                  {t('Organizações válidas: {count}', 'Valid organizations: {count}', {
                    count: organizationNames.length,
                  })}
                </Tag>
              </Space>
              <Typography.Paragraph style={{ marginTop: 10 }}>
                {t(
                  'Não é permitido avançar para Nodes sem topology completa da organização e mapeamento componente -> host (`peer`, `orderer`, `ca`, `couch`, `apiGateway`, `netapi`).',
                  'You cannot advance to Nodes without a complete organization topology and component -> host mapping (`peer`, `orderer`, `ca`, `couch`, `apiGateway`, `netapi`).'
                )}
              </Typography.Paragraph>
              <div className={styles.commandBlock}>
                {organizationServiceMatrixRows.length > 0
                  ? organizationServiceMatrixRows.map(row => (
                      <div
                        key={row.key}
                      >{`${row.organizationName} | ${row.serviceKey} -> ${row.hostRef} | params=${row.parameterSummary}`}</div>
                    ))
                  : t('Nenhuma organização preenchida ainda.', 'No organization filled in yet.')}
              </div>
            </div>
          </div>
        )}

        {activeStepIndex === 2 && (
          <div className={styles.neoGrid2}>
            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Nodes (Peers e Orderers)', 'Nodes (Peers and Orderers)')}
              </Typography.Title>
              <Typography.Paragraph>
                {t(
                  'Defina a volumetria mínima de peers/orderers por organização.',
                  'Define the minimum peer/orderer volume per organization.'
                )}
              </Typography.Paragraph>
              <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
                {t(
                  'O mapeamento de host por componente e os parâmetros mínimos de serviço já foram declarados na etapa de Organizations; aqui você ajusta apenas quantidades.',
                  'The host mapping by component and the minimum service parameters were already declared in the Organizations step; here you only adjust quantities.'
                )}
              </Typography.Paragraph>
              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                {organizations.map(organization => (
                  <div key={organization.id} className={styles.neoCard}>
                    <Typography.Text className={styles.neoValue}>
                      {organization.name || organization.id}
                    </Typography.Text>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Peers', 'Peers')}
                        </Typography.Text>
                        <InputNumber
                          min={0}
                          max={20}
                          value={organization.peers}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'peers',
                                Number(value || 0)
                              )
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Orderers', 'Orderers')}
                        </Typography.Text>
                        <InputNumber
                          min={0}
                          max={20}
                          value={organization.orderers}
                          style={{ width: '100%' }}
                          onChange={value =>
                            setOrganizations(current =>
                              updateCollectionRow(
                                current,
                                organization.id,
                                'orderers',
                                Number(value || 0)
                              )
                            )
                          }
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </Space>
            </div>

            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Gate de Nodes', 'Nodes gate')}
              </Typography.Title>
              <Space wrap size={8}>
                <Tag color={organizationsStepReady ? 'green' : 'red'}>
                  {organizationsStepReady
                    ? t('Organizações aptas', 'Organizations ready')
                    : t('Organizações pendentes', 'Organizations pending')}
                </Tag>
                <Tag color="blue">
                  {t('Organizações: {count}', 'Organizations: {count}', {
                    count: organizations.length,
                  })}
                </Tag>
              </Space>
              <Typography.Paragraph style={{ marginTop: 10 }}>
                {t(
                  'Cada organização precisa de ao menos um node (peer ou orderer) para liberar Business Groups.',
                  'Each organization needs at least one node (peer or orderer) to enable Business Groups.'
                )}
              </Typography.Paragraph>
              <div className={styles.commandBlock}>
                {organizations.length > 0
                  ? organizations.map(organization => (
                      <div key={organization.id}>{`${organization.name || organization.id}: peers=${
                        organization.peers
                      }, orderers=${organization.orderers}`}</div>
                    ))
                  : t('Nenhuma organização disponível.', 'No organization available.')}
              </div>
            </div>
          </div>
        )}

        {activeStepIndex === 3 && (
          <div className={styles.neoGrid2}>
            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Detalhes do Business Group', 'Business Group details')}
              </Typography.Title>
              <Typography.Paragraph>
                {t(
                  'Este bloco representa o agrupamento operacional usado pelo COGNUS para channels e installs seguintes.',
                  'This block represents the operational grouping used by COGNUS for the next channel and install steps.'
                )}
              </Typography.Paragraph>
              <div className={styles.formGrid2}>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('Nome', 'Name')}
                  </Typography.Text>
                  <Input
                    value={businessGroupName}
                    onChange={event => setBusinessGroupName(event.target.value)}
                    placeholder={t('ex.: Business Group Alpha', 'e.g. Business Group Alpha')}
                  />
                </div>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('Network ID', 'Network ID')}
                  </Typography.Text>
                  <Input
                    value={businessGroupNetworkId}
                    onChange={event => setBusinessGroupNetworkId(event.target.value)}
                    placeholder={t(
                      'preenchido automaticamente (editável)',
                      'filled automatically (editable)'
                    )}
                  />
                </div>
                <div className={styles.formField}>
                  <Typography.Text className={styles.formFieldLabel}>
                    {t('Descrição', 'Description')}
                  </Typography.Text>
                  <Input.TextArea
                    value={businessGroupDescription}
                    onChange={event => setBusinessGroupDescription(event.target.value)}
                    rows={3}
                    placeholder={t(
                      'Descrição operacional do business group',
                      'Operational description of the business group'
                    )}
                  />
                </div>
              </div>
              <Typography.Paragraph type="secondary" style={{ marginTop: 8 }}>
                {t(
                  'Se vazio, o Network ID é sugerido automaticamente a partir do nome do Business Group.',
                  'If empty, the Network ID is suggested automatically from the Business Group name.'
                )}
              </Typography.Paragraph>
            </div>

            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Gate de Business Groups', 'Business Groups gate')}
              </Typography.Title>
              <Space wrap size={8}>
                <Tag color={nodesStepReady ? 'green' : 'red'}>
                  {nodesStepReady ? t('Nodes aptos', 'Nodes ready') : t('Nodes pendentes', 'Nodes pending')}
                </Tag>
                <Tag color={String(businessGroupName || '').trim() ? 'green' : 'orange'}>
                  {String(businessGroupName || '').trim()
                    ? t('Business Group definido', 'Business Group defined')
                    : t('Business Group pendente', 'Business Group pending')}
                </Tag>
              </Space>
              <Typography.Paragraph style={{ marginTop: 10 }}>
                {t(
                  'O fluxo não libera Channels enquanto o Business Group não estiver definido.',
                  'The flow does not enable Channels until the Business Group is defined.'
                )}
              </Typography.Paragraph>
              <div className={styles.commandBlock}>
                {String(businessGroupName || '').trim()
                  ? `Name: ${businessGroupName} | Network ID: ${businessGroupNetworkId || '-'}`
                  : t('Nenhum business group definido ainda.', 'No business group defined yet.')}
              </div>
            </div>
          </div>
        )}

        {activeStepIndex === 4 && (
          <div className={styles.neoGrid2}>
            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                Channels
              </Typography.Title>

              {channels.length === 0 && (
                <Alert
                  showIcon
                  type="info"
                  style={{ marginBottom: 12 }}
                  message={t('Sem channels', 'No channels')}
                  description={t(
                    'Crie ao menos um channel antes de instalar chaincodes.',
                    'Create at least one channel before installing chaincodes.'
                  )}
                />
              )}

              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                {channels.map(channel => (
                  <div key={channel.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{channel.id}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        onClick={() =>
                          setChannels(current =>
                            current.filter(currentChannel => currentChannel.id !== channel.id)
                          )
                        }
                      >
                        {t('Remover', 'Remove')}
                      </Button>
                    </Space>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Nome do canal', 'Channel name')}
                        </Typography.Text>
                        <Input
                          value={channel.name}
                          onChange={({ target: { value } }) =>
                            setChannels(current =>
                              updateCollectionRow(current, channel.id, 'name', value)
                            )
                          }
                          placeholder={t('ex.: channel-dev', 'e.g. channel-dev')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Organizations (csv, opcional)', 'Organizations (csv, optional)')}
                        </Typography.Text>
                        <Input
                          value={channel.memberOrgs}
                          onChange={({ target: { value } }) =>
                            setChannels(current =>
                              updateCollectionRow(current, channel.id, 'memberOrgs', value)
                            )
                          }
                          placeholder={t(
                            'vazio = usar orgs atuais; ex.: ORG-ALFA,ORG-BETA',
                            'empty = use current orgs; e.g. ORG-ALFA,ORG-BETA'
                          )}
                        />
                      </div>
                    </div>
                  </div>
                ))}

                <Button
                  icon={<PlusOutlined />}
                  onClick={() =>
                    setChannels(current => [...current, createChannelDraft(current.length + 1)])
                  }
                >
                  {t('Adicionar canal', 'Add channel')}
                </Button>
              </Space>
            </div>

            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Gate de Channels', 'Channels gate')}
              </Typography.Title>
              <Space wrap size={8}>
                <Tag color={businessGroupsStepReady ? 'green' : 'red'}>
                  {businessGroupsStepReady
                    ? t('Business Group apto', 'Business Group ready')
                    : t('Business Group pendente', 'Business Group pending')}
                </Tag>
                <Tag color="cyan">
                  {t('Canais válidos: {count}', 'Valid channels: {count}', {
                    count: channelNames.length,
                  })}
                </Tag>
              </Space>
              <Typography.Paragraph style={{ marginTop: 10 }}>
                {t(
                  'Associações de organizações são opcionais aqui; se vazio, o canal usa as orgs atuais e pode ser atualizado depois.',
                  'Organization associations are optional here; if empty, the channel uses the current orgs and can be updated later.'
                )}
              </Typography.Paragraph>
              <div className={styles.commandBlock}>
                {organizationNames.length > 0
                  ? organizationNames.map(org => (
                      <div key={org}>{t('Org disponível: {org}', 'Available org: {org}', { org })}</div>
                    ))
                  : t(
                      'Nenhuma organização disponível para associação ao canal.',
                      'No organization available for channel association.'
                    )}
              </div>
            </div>
          </div>
        )}

        {activeStepIndex === 5 && (
          <div className={styles.neoGrid2}>
            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Install Chaincodes (.tar.gz)', 'Install Chaincodes (.tar.gz)')}
              </Typography.Title>
              <Typography.Paragraph>
                {t(
                  'Nesta fase o fluxo usa somente o pacote `.tar.gz` anexado no wizard. Cada install publica um artefato explícito para o orchestrator reutilizar no runbook.',
                  'At this stage the flow uses only the `.tar.gz` package attached in the wizard. Each install publishes an explicit artifact for the orchestrator to reuse in the runbook.'
                )}
              </Typography.Paragraph>
              <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
                {t(
                  'Para instalar, anexe um `.tar.gz` por chaincode, informe o nome lógico do chaincode e selecione o channel alvo.',
                  'To install, attach one `.tar.gz` per chaincode, provide the logical chaincode name, and select the target channel.'
                )}
              </Typography.Paragraph>

              {channelNames.length === 0 && (
                <Alert
                  showIcon
                  type="warning"
                  style={{ marginBottom: 12 }}
                  message={t('Sem canais disponíveis', 'No channels available')}
                  description={t(
                    'Você precisa criar ao menos um channel para instalar chaincodes.',
                    'You need to create at least one channel to install chaincodes.'
                  )}
                />
              )}

              <Space direction="vertical" size={10} style={{ width: '100%' }}>
                {chaincodeInstalls.map(install => (
                  <div key={install.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{install.id}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        disabled={chaincodeInstalls.length === 1}
                        onClick={() =>
                          setChaincodeInstalls(current =>
                            current.filter(currentInstall => currentInstall.id !== install.id)
                          )
                        }
                      >
                        {t('Remover', 'Remove')}
                      </Button>
                    </Space>

                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Pacote de chaincode (.tar.gz)', 'Chaincode package (.tar.gz)')}
                        </Typography.Text>
                        <Upload
                          beforeUpload={() => false}
                          onChange={info => handleChaincodePackageUpload(install.id, info)}
                          showUploadList={false}
                          maxCount={1}
                          accept=".tar.gz"
                        >
                          <Button
                            icon={<UploadOutlined />}
                            loading={install.uploadStatus === 'uploading'}
                          >
                            {install.packageFileName
                              ? t('Pacote: {name}', 'Package: {name}', {
                                  name: install.packageFileName,
                                })
                              : t('Selecionar pacote .tar.gz', 'Select .tar.gz package')}
                          </Button>
                        </Upload>
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Nome do chaincode', 'Chaincode name')}
                        </Typography.Text>
                        <Input
                          value={install.chaincodeName}
                          onChange={({ target: { value } }) =>
                            setChaincodeInstalls(current =>
                              current.map(currentInstall => {
                                if (currentInstall.id !== install.id) {
                                  return currentInstall;
                                }

                                const nextChaincodeName = value;
                                const currentEndpoint = toText(currentInstall.endpointPath);
                                const previousDefaultEndpoint = buildDefaultChaincodeEndpointPath(
                                  currentInstall.channel,
                                  currentInstall.chaincodeName
                                );
                                const shouldSyncEndpoint =
                                  !currentEndpoint || currentEndpoint === previousDefaultEndpoint;

                                return {
                                  ...currentInstall,
                                  chaincodeName: nextChaincodeName,
                                  endpointPath: shouldSyncEndpoint
                                    ? buildDefaultChaincodeEndpointPath(
                                        currentInstall.channel,
                                        nextChaincodeName
                                      )
                                    : currentInstall.endpointPath,
                                };
                              })
                            )
                          }
                          placeholder={t('ex.: meu-chaincode', 'e.g. my-chaincode')}
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Canal alvo', 'Target channel')}
                        </Typography.Text>
                        <Select
                          value={install.channel || undefined}
                          placeholder={t('Selecione channel', 'Select channel')}
                          options={channelNames.map(name => ({ value: name, label: name }))}
                          disabled={channelNames.length === 0}
                          onChange={value =>
                            setChaincodeInstalls(current =>
                              current.map(currentInstall => {
                                if (currentInstall.id !== install.id) {
                                  return currentInstall;
                                }

                                const nextChannel = toText(value);
                                const currentEndpoint = toText(currentInstall.endpointPath);
                                const previousDefaultEndpoint = buildDefaultChaincodeEndpointPath(
                                  currentInstall.channel,
                                  currentInstall.chaincodeName
                                );
                                const shouldSyncEndpoint =
                                  !currentEndpoint || currentEndpoint === previousDefaultEndpoint;

                                return {
                                  ...currentInstall,
                                  channel: nextChannel,
                                  endpointPath: shouldSyncEndpoint
                                    ? buildDefaultChaincodeEndpointPath(
                                        nextChannel,
                                        currentInstall.chaincodeName
                                      )
                                    : currentInstall.endpointPath,
                                };
                              })
                            )
                          }
                        />
                      </div>
                    </div>

                    {(install.uploadError || install.artifactRef || install.packageLabel) && (
                      <div style={{ marginTop: 10 }}>
                        {install.uploadError ? (
                          <Alert
                            showIcon
                            type="error"
                            message={t(
                              'Upload inválido para {id}',
                              'Invalid upload for {id}',
                              { id: install.id }
                            )}
                            description={install.uploadError}
                          />
                        ) : (
                          <Alert
                            showIcon
                            type="success"
                            message={t(
                              'Artefato pronto para {id}',
                              'Artifact ready for {id}',
                              { id: install.id }
                            )}
                            description={`artifact_ref: ${install.artifactRef}${
                              install.packageLabel ? ` | label: ${install.packageLabel}` : ''
                            }`}
                          />
                        )}
                      </div>
                    )}
                  </div>
                ))}

                <Button
                  icon={<PlusOutlined />}
                  disabled={channelNames.length === 0}
                  onClick={() =>
                    setChaincodeInstalls(current => [
                      ...current,
                      createChaincodeInstallDraft(current.length + 1),
                    ])
                  }
                >
                  {t('Adicionar instalação', 'Add install')}
                </Button>

                <div className={styles.line} />

                <Typography.Title level={5} className={styles.neoCardTitle}>
                  {t(
                    'Gestão de APIs por organização/canal/chaincode',
                    'API management by organization/channel/chaincode'
                  )}
                </Typography.Title>
                <Typography.Paragraph>
                  {t('Cadastre APIs no padrão', 'Register APIs with the pattern')}{' '}
                  <Typography.Text code>
                    /api/{'{channel_id}'}/{'{chaincode_id}'}
                  </Typography.Text>
                  {t(
                    '. A mesma API base permanece estável para novas versões do chaincode.',
                    '. The same base API remains stable for new chaincode versions.'
                  )}
                </Typography.Paragraph>
                <Typography.Paragraph type="secondary" style={{ marginTop: -4 }}>
                  {t(
                    'Registro por organização: a mesma API atende qualquer channel/chaincode da org.',
                    'Registration by organization: the same API serves any channel/chaincode in the org.'
                  )}
                </Typography.Paragraph>

                {apiRegistrations.map(api => (
                  <div key={api.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{api.id}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        onClick={() => {
                          setApiRegistrations(current =>
                            current.filter(currentApi => currentApi.id !== api.id)
                          );
                          appendAuditEvent('api_registry_removed', {
                            api_id: api.id,
                          });
                        }}
                      >
                        {t('Remover API', 'Remove API')}
                      </Button>
                    </Space>

                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Organização', 'Organization')}
                        </Typography.Text>
                        <Select
                          value={api.organizationName || undefined}
                          placeholder={t('Selecione organization', 'Select organization')}
                          options={organizationNames.map(name => ({ value: name, label: name }))}
                          onChange={value =>
                            handleApiRegistrationFieldChange(api.id, 'organizationName', value)
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Escopo', 'Scope')}
                        </Typography.Text>
                        <Input
                          value={t(
                            'Todos channels/chaincodes da organização',
                            'All organization channels/chaincodes'
                          )}
                          disabled
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Caminho da rota', 'Route path')}
                        </Typography.Text>
                        <Input value={api.routePath} disabled />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Host de exposição', 'Exposure host')}
                        </Typography.Text>
                        <Input value={api.exposureHost} disabled />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Porta de exposição', 'Exposure port')}
                        </Typography.Text>
                        <InputNumber value={api.exposurePort} style={{ width: '100%' }} disabled />
                      </div>
                    </div>
                  </div>
                ))}

                <Button
                  icon={<PlusOutlined />}
                  disabled={
                    organizationNames.length === 0 ||
                    channelNames.length === 0 ||
                    activeChaincodeIds.length === 0
                  }
                  onClick={() => {
                    const nextIndex = apiRegistrations.length + 1;
                    const firstOrg = organizationNames[0] || '';
                    const firstChannel = channelNames[0] || '';
                    const firstChaincode = activeChaincodeIds[0] || '';
                    const exposure = buildApiExposureTarget({
                      organizationName: firstOrg,
                      organizations,
                      machines,
                    });
                    const nextDraft = {
                      ...createApiRegistrationDraft(nextIndex),
                      organizationName: firstOrg,
                      channel: firstChannel,
                      chaincodeId: firstChaincode,
                      exposureHost: exposure.host,
                      exposurePort: exposure.port || null,
                    };
                    const syncedDraft = syncApiDraft(nextDraft);
                    setApiRegistrations(current => [...current, syncedDraft]);
                    appendAuditEvent('api_registry_added', {
                      api_id: syncedDraft.id,
                      org_name: syncedDraft.organizationName,
                      channel_id: syncedDraft.channel,
                      chaincode_id: syncedDraft.chaincodeId,
                    });
                  }}
                >
                  {t('Adicionar registro de API', 'Add API registration')}
                </Button>

                <div className={styles.commandBlock}>
                  {apiRegistryEntries.length > 0
                    ? apiRegistryEntries.map(apiEntry => (
                        <div
                          key={apiEntry.api_id}
                        >{`${apiEntry.api_id}: ${apiEntry.route_path} -> ${apiEntry.exposure.host}:${apiEntry.exposure.port} | ${t(
                          'uso',
                          'usage'
                        )}: /api/{channel}/{chaincode}/...`}</div>
                      ))
                    : t('Nenhuma API cadastrada ainda.', 'No API registered yet.')}
                </div>

                <div className={styles.line} />

                <Typography.Title level={5} className={styles.neoCardTitle}>
                  {t('Expansão incremental pós-criação', 'Post-creation incremental expansion')}
                </Typography.Title>
                <Typography.Paragraph>
                  {t(
                    'Adicione peers/orderers, channels, installs e APIs sem recriar a organização, mantendo o mesmo gate de preflight/lint e correlação por',
                    'Add peers/orderers, channels, installs, and APIs without recreating the organization, keeping the same preflight/lint gate and correlation by'
                  )}{' '}
                  <Typography.Text code> change_id/run_id</Typography.Text>.
                </Typography.Paragraph>

                <Typography.Text className={styles.neoLabel}>
                  {t(
                    'Assistente add_peer/add_orderer por organização',
                    'add_peer/add_orderer assistant per organization'
                  )}
                </Typography.Text>
                {incrementalNodeExpansions.map(expansion => {
                  const draftPreviewRows = incrementalNodePreviewByDraftId[expansion.id] || [];

                  return (
                    <div key={expansion.id} className={styles.neoCard}>
                      <Space
                        align="center"
                        style={{ width: '100%', justifyContent: 'space-between' }}
                      >
                        <Typography.Text className={styles.neoValue}>
                          {expansion.id}
                        </Typography.Text>
                        <Button
                          danger
                          size="small"
                          onClick={() =>
                            setIncrementalNodeExpansions(current =>
                              current.filter(row => row.id !== expansion.id)
                            )
                          }
                        >
                          {t('Remover', 'Remove')}
                        </Button>
                      </Space>
                      <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                        <div className={styles.formField}>
                          <Typography.Text className={styles.formFieldLabel}>
                            Operation Type
                          </Typography.Text>
                          <Select
                            value={toText(expansion.operationType) || undefined}
                            placeholder={t('Selecione operação', 'Select operation')}
                            options={INCREMENTAL_NODE_OPERATION_OPTIONS}
                            onChange={value =>
                              setIncrementalNodeExpansions(current =>
                                current.map(row => {
                                  if (row.id !== expansion.id) {
                                    return row;
                                  }

                                  const nextOperationType = toText(value);
                                  const defaultHostRef = resolveIncrementalDefaultHostRef(
                                    row.organizationName,
                                    nextOperationType
                                  );

                                  return {
                                    ...row,
                                    operationType: nextOperationType,
                                    targetHostRef: toText(row.targetHostRef) || defaultHostRef,
                                    addPeers: 0,
                                    addOrderers: 0,
                                  };
                                })
                              )
                            }
                          />
                        </div>
                        <div className={styles.formField}>
                          <Typography.Text className={styles.formFieldLabel}>
                            {t('Organização', 'Organization')}
                          </Typography.Text>
                          <Select
                            value={expansion.organizationName || undefined}
                            placeholder={t('Selecione organization', 'Select organization')}
                            options={organizationNames.map(name => ({ value: name, label: name }))}
                            onChange={value =>
                              setIncrementalNodeExpansions(current =>
                                current.map(row => {
                                  if (row.id !== expansion.id) {
                                    return row;
                                  }

                                  const defaultHostRef = resolveIncrementalDefaultHostRef(
                                    value,
                                    row.operationType
                                  );

                                  return {
                                    ...row,
                                    organizationName: value,
                                    targetHostRef: toText(row.targetHostRef) || defaultHostRef,
                                  };
                                })
                              )
                            }
                          />
                        </div>
                        <div className={styles.formField}>
                          <Typography.Text className={styles.formFieldLabel}>
                            {t('Host alvo', 'Target host')}
                          </Typography.Text>
                          <Select
                            value={toText(expansion.targetHostRef) || undefined}
                            placeholder={t('Selecione host alvo', 'Select target host')}
                            options={hostRefOptions}
                            onChange={value =>
                              setIncrementalNodeExpansions(current =>
                                updateCollectionRow(current, expansion.id, 'targetHostRef', value)
                              )
                            }
                          />
                        </div>
                        <div className={styles.formField}>
                          <Typography.Text className={styles.formFieldLabel}>
                            Quantidade
                          </Typography.Text>
                          <InputNumber
                            min={1}
                            max={20}
                            value={Number(expansion.requestedCount || 0)}
                            style={{ width: '100%' }}
                            onChange={value =>
                              setIncrementalNodeExpansions(current =>
                                updateCollectionRow(
                                  current,
                                  expansion.id,
                                  'requestedCount',
                                  Number(value || 0)
                                )
                              )
                            }
                          />
                        </div>
                      </div>
                      <div className={styles.commandBlock} style={{ marginTop: 10 }}>
                        {draftPreviewRows.length > 0
                          ? draftPreviewRows.map(operation => (
                              <div key={operation.operation_id}>{`${operation.operation_type}: ${
                                operation.component_name
                              } | host=${operation.target_host_ref || t('n/d', 'n/a')} | port=${
                                operation.target_port
                              }`}</div>
                            ))
                          : t(
                              'Preview aguardando dados completos para gerar naming/porta.',
                              'Preview waiting for complete data to generate naming/port.'
                            )}
                      </div>
                    </div>
                  );
                })}
                <Button
                  icon={<PlusOutlined />}
                  disabled={organizationNames.length === 0}
                  onClick={() => {
                    const firstOrganization = organizationNames[0] || '';
                    const defaultHostRef = resolveIncrementalDefaultHostRef(
                      firstOrganization,
                      INCREMENTAL_NODE_OPERATION_TYPE.addPeer
                    );

                    setIncrementalNodeExpansions(current => [
                      ...current,
                      createIncrementalNodeExpansionDraft(current.length + 1, {
                        organizationName: firstOrganization,
                        targetHostRef: defaultHostRef,
                      }),
                    ]);
                  }}
                >
                  {t(
                    'Adicionar operação add_peer/add_orderer',
                    'Add add_peer/add_orderer operation'
                  )}
                </Button>

                <div className={styles.commandBlock}>
                  {incrementalNodePreview.operations.length > 0
                    ? incrementalNodePreview.operations.map(operation => (
                        <div key={operation.operation_id}>{`${operation.operation_type}: ${
                          operation.component_name
                        } | host=${operation.target_host_ref || t('n/d', 'n/a')} | port=${
                          operation.target_port
                        }`}</div>
                      ))
                    : t(
                        'Nenhuma operação add_peer/add_orderer planejada.',
                        'No add_peer/add_orderer operation planned yet.'
                      )}
                </div>

                <div className={styles.commandBlock}>
                  {incrementalNodePreview.hostCapacity.length > 0
                    ? incrementalNodePreview.hostCapacity.map(row => (
                        <div
                          key={row.host_key}
                        >{`${row.host_ref}: ${row.planned_operations} op(s), ${row.planned_units} ${t(
                          'unidade(s) planejadas',
                          'planned unit(s)'
                        )}, ${t('limite', 'limit')} ${row.available_units}, ${t(
                          'status preflight',
                          'preflight status'
                        )} ${row.status}.`}</div>
                      ))
                    : t(
                        'Capacidade por host será exibida após definição de operações incrementais.',
                        'Host capacity will be shown after incremental operations are defined.'
                      )}
                </div>

                <Typography.Text className={styles.neoLabel}>
                  {t('Novo channel incremental', 'New incremental channel')}
                </Typography.Text>
                {incrementalChannelAdditions.map(channel => (
                  <div key={channel.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{channel.id}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        onClick={() =>
                          setIncrementalChannelAdditions(current =>
                            current.filter(row => row.id !== channel.id)
                          )
                        }
                        >
                          {t('Remover', 'Remove')}
                        </Button>
                    </Space>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Nome do canal', 'Channel name')}
                        </Typography.Text>
                        <Input
                          value={channel.name}
                          onChange={({ target: { value } }) =>
                            setIncrementalChannelAdditions(current =>
                              updateCollectionRow(current, channel.id, 'name', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Organizações (csv)', 'Organizations (csv)')}
                        </Typography.Text>
                        <Input
                          value={channel.memberOrgs}
                          onChange={({ target: { value } }) =>
                            setIncrementalChannelAdditions(current =>
                              updateCollectionRow(current, channel.id, 'memberOrgs', value)
                            )
                          }
                        />
                      </div>
                    </div>
                  </div>
                ))}
                <Button
                  icon={<PlusOutlined />}
                  disabled={organizationNames.length === 0}
                  onClick={() =>
                    setIncrementalChannelAdditions(current => [
                      ...current,
                      createIncrementalChannelDraft(current.length + 1),
                    ])
                  }
                >
                  {t('Adicionar canal incremental', 'Add incremental channel')}
                </Button>

                <Typography.Text className={styles.neoLabel}>
                  {t('Novo install incremental', 'New incremental install')}
                </Typography.Text>
                {incrementalInstallAdditions.map(install => (
                  <div key={install.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{install.id}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        onClick={() =>
                          setIncrementalInstallAdditions(current =>
                            current.filter(row => row.id !== install.id)
                          )
                        }
                        >
                          {t('Remover', 'Remove')}
                        </Button>
                    </Space>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Canal alvo', 'Target channel')}
                        </Typography.Text>
                        <Select
                          value={install.channel || undefined}
                          placeholder={t('Selecione channel', 'Select channel')}
                          options={channelNames.map(name => ({ value: name, label: name }))}
                          onChange={value =>
                            setIncrementalInstallAdditions(current =>
                              updateCollectionRow(current, install.id, 'channel', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Caminho do endpoint', 'Endpoint path')}
                        </Typography.Text>
                        <Input
                          value={install.endpointPath}
                          onChange={({ target: { value } }) =>
                            setIncrementalInstallAdditions(current =>
                              updateCollectionRow(current, install.id, 'endpointPath', value)
                            )
                          }
                          placeholder="/api/new-channel/new-chaincode/query/getTx"
                        />
                      </div>
                    </div>
                  </div>
                ))}
                <Button
                  icon={<PlusOutlined />}
                  disabled={channelNames.length === 0}
                  onClick={() =>
                    setIncrementalInstallAdditions(current => [
                      ...current,
                      createIncrementalInstallDraft(current.length + 1),
                    ])
                  }
                >
                  {t('Adicionar instalação incremental', 'Add incremental install')}
                </Button>

                <Typography.Text className={styles.neoLabel}>
                  {t('Nova API incremental', 'New incremental API')}
                </Typography.Text>
                {incrementalApiAdditions.map(api => (
                  <div key={api.id} className={styles.neoCard}>
                    <Space
                      align="center"
                      style={{ width: '100%', justifyContent: 'space-between' }}
                    >
                      <Typography.Text className={styles.neoValue}>{api.id}</Typography.Text>
                      <Button
                        danger
                        size="small"
                        onClick={() =>
                          setIncrementalApiAdditions(current =>
                            current.filter(row => row.id !== api.id)
                          )
                        }
                        >
                          {t('Remover', 'Remove')}
                        </Button>
                    </Space>
                    <div className={styles.formGrid2} style={{ marginTop: 10 }}>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Organização', 'Organization')}
                        </Typography.Text>
                        <Select
                          value={api.organizationName || undefined}
                          placeholder={t('Selecione organization', 'Select organization')}
                          options={organizationNames.map(name => ({ value: name, label: name }))}
                          onChange={value =>
                            setIncrementalApiAdditions(current =>
                              updateCollectionRow(current, api.id, 'organizationName', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Canal', 'Channel')}
                        </Typography.Text>
                        <Select
                          value={api.channel || undefined}
                          placeholder={t('Selecione channel', 'Select channel')}
                          options={channelNames.map(name => ({ value: name, label: name }))}
                          onChange={value =>
                            setIncrementalApiAdditions(current =>
                              updateCollectionRow(current, api.id, 'channel', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          Chaincode
                        </Typography.Text>
                        <Select
                          value={api.chaincodeId || undefined}
                          placeholder={t('novo-chaincode', 'new-chaincode')}
                          options={activeChaincodeIds.map(name => ({ value: name, label: name }))}
                          onChange={value =>
                            setIncrementalApiAdditions(current =>
                              updateCollectionRow(current, api.id, 'chaincodeId', value)
                            )
                          }
                        />
                      </div>
                      <div className={styles.formField}>
                        <Typography.Text className={styles.formFieldLabel}>
                          {t('Caminho da rota (auto)', 'Route path (auto)')}
                        </Typography.Text>
                        <Input
                          value={buildApiRoutePath({
                            channelId: api.channel,
                            chaincodeId: api.chaincodeId,
                          })}
                          disabled
                        />
                      </div>
                    </div>
                  </div>
                ))}
                <Button
                  icon={<PlusOutlined />}
                  disabled={
                    organizationNames.length === 0 ||
                    channelNames.length === 0 ||
                    activeChaincodeIds.length === 0
                  }
                  onClick={() => {
                    const nextIndex = incrementalApiAdditions.length + 1;
                    const firstOrg = organizationNames[0] || '';
                    const firstChannel = channelNames[0] || '';
                    const firstChaincode = activeChaincodeIds[0] || 'chaincode';
                    const exposure = buildApiExposureTarget({
                      organizationName: firstOrg,
                      organizations,
                      machines,
                    });

                    setIncrementalApiAdditions(current => [
                      ...current,
                      {
                        ...createIncrementalApiDraft(nextIndex),
                        organizationName: firstOrg,
                        channel: firstChannel,
                        chaincodeId: firstChaincode,
                        exposureHost: exposure.host,
                        exposurePort: exposure.port || null,
                      },
                    ]);
                  }}
                >
                  {t('Adicionar API incremental', 'Add incremental API')}
                </Button>

                <Space wrap size={8}>
                  <Tag color={preflightApproved ? 'green' : 'red'}>
                    {preflightApproved
                      ? t('Preflight apto', 'Preflight ready')
                      : t('Preflight pendente', 'Preflight pending')}
                  </Tag>
                  <Tag color={lintApprovedForCurrentModel ? 'green' : 'red'}>
                    {lintApprovedForCurrentModel
                      ? t('Lint A1.2 aprovado', 'Lint A1.2 approved')
                      : t('Lint A1.2 pendente', 'Lint A1.2 pending')}
                  </Tag>
                  <Tag color="blue">{`run_id incremental: ${incrementalRunId}`}</Tag>
                </Space>

                {incrementalExpansionIssues.length > 0 && (
                  <Alert
                    showIcon
                    type="warning"
                    message={t(
                      'Expansão incremental bloqueada ({count} pendência(s))',
                      'Incremental expansion blocked ({count} pending item(s))',
                      { count: incrementalExpansionIssues.length }
                    )}
                    description={incrementalExpansionIssues[0]}
                  />
                )}

                <Button type="primary" onClick={handleApplyIncrementalExpansion}>
                  {t('Aplicar expansão incremental no modelo', 'Apply incremental expansion to the model')}
                </Button>

                <div className={styles.commandBlock}>
                  {incrementalExpansionHistory.length > 0
                    ? incrementalExpansionHistory.map(expansion => (
                        <div key={expansion.expansion_id}>{`${expansion.expansion_id}: ${
                          expansion.operations_total
                        } ${t('operação(ões)', 'operation(s)')} | change_id=${expansion.change_id} | run_id=${
                          expansion.run_id
                        } | operation_type=${(Array.isArray(expansion.operation_types)
                          ? expansion.operation_types
                          : []
                        )
                          .filter(Boolean)
                          .join(', ') || t('n/d', 'n/a')}`}</div>
                      ))
                    : t(
                        'Nenhuma expansão incremental aplicada ainda.',
                        'No incremental expansion applied yet.'
                      )}
                </div>
              </Space>
            </div>

            <div className={styles.neoCard}>
              <Typography.Title level={4} className={styles.neoCardTitle}>
                {t('Resumo final do fluxo', 'Final flow summary')}
              </Typography.Title>
              <Space wrap size={8}>
                <Tag color={infraStepReady ? 'green' : 'red'}>
                  {infraStepReady ? t('Infra ok', 'Infra ok') : t('Infra pendente', 'Infra pending')}
                </Tag>
                <Tag color={organizationsStepReady ? 'green' : 'red'}>
                  {organizationsStepReady
                    ? t('Organizações ok', 'Organizations ok')
                    : t('Organizações pendentes', 'Organizations pending')}
                </Tag>
                <Tag color={nodesStepReady ? 'green' : 'red'}>
                  {nodesStepReady ? t('Nodes ok', 'Nodes ok') : t('Nodes pendentes', 'Nodes pending')}
                </Tag>
                <Tag color={businessGroupsStepReady ? 'green' : 'red'}>
                  {businessGroupsStepReady
                    ? t('Business Groups ok', 'Business Groups ok')
                    : t('Business Groups pendentes', 'Business Groups pending')}
                </Tag>
                <Tag color={channelsStepReady ? 'green' : 'red'}>
                  {channelsStepReady
                    ? t('Channels ok', 'Channels ok')
                    : t('Channels pendentes', 'Channels pending')}
                </Tag>
                <Tag color={installStepReady ? 'green' : 'orange'}>
                  {installStepReady
                    ? t('Install pronto', 'Install ready')
                    : t('Install com pendências', 'Install with pending items')}
                </Tag>
                <Tag color={handoffToRunbookReady ? 'green' : 'orange'}>
                  {handoffToRunbookReady
                    ? t('Org completa pronta para handoff', 'Full org ready for handoff')
                    : t('Handoff técnico pendente', 'Technical handoff pending')}
                </Tag>
              </Space>

              <Typography.Paragraph style={{ marginTop: 12, marginBottom: 8 }}>
                {t('Seed técnico (resumo JSON):', 'Technical seed (JSON summary):')}
              </Typography.Paragraph>
              <div className={styles.commandBlock}>
                {JSON.stringify(infrastructureSeed, null, 2)}
              </div>
            </div>
          </div>
        )}

          </div>

          <div className={styles.windowWorkspaceFooter}>
            <Space wrap>
              <Button
                icon={<ArrowLeftOutlined />}
                disabled={isFirstStep}
                onClick={() => setActiveStepIndex(current => Math.max(0, current - 1))}
              >
                {t('Etapa anterior', 'Previous step')}
              </Button>
              <Button icon={<ArrowRightOutlined />} disabled={isLastStep} onClick={handleNextStep}>
                {t('Próxima etapa', 'Next step')}
              </Button>
            </Space>

            <div className={styles.windowWorkspaceFooterActions}>
              <Tooltip title={buildReadinessTooltip(validateActionReadiness)}>
                <Button
                  onClick={handleValidate}
                  loading={isWizardLinting}
                  disabled={!validateActionReadiness.available}
                >
                  {t('Validar topologia (lint A1.2)', 'Validate topology (lint A1.2)')}
                </Button>
              </Tooltip>
              <Tooltip title={buildReadinessTooltip(exportActionReadiness)}>
                <Button
                  type="primary"
                  icon={<DownloadOutlined />}
                  onClick={handleExport}
                  disabled={!exportActionReadiness.available || !installStepReady}
                >
                  {t('Exportar seed técnico', 'Export technical seed')}
                </Button>
              </Tooltip>
              <Tooltip
                title={t(
                  'Executa lint/publicação e abre o runbook com host mapping resolvido.',
                  'Runs lint/publication and opens the runbook with resolved host mapping.'
                )}
              >
                <Button
                  type="primary"
                  onClick={handlePublishAndOpenRunbook}
                  loading={isPublishingAndOpeningRunbook}
                  disabled={!installStepReady || isWizardLinting}
                >
                  {t('Publicar blueprint e abrir runbook', 'Publish blueprint and open runbook')}
                </Button>
              </Tooltip>
            </div>
          </div>
        </div>
      </OperationalWindowDialog>
    </NeoOpsLayout>
    </OperationalWindowManagerProvider>
  );
};

export default ProvisioningInfrastructurePage;
