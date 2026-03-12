import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Empty,
  Input,
  InputNumber,
  List,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
  Upload,
  message,
} from 'antd';
import { history } from 'umi';
import {
  CheckCircleOutlined,
  ClusterOutlined,
  LockOutlined,
  PlusOutlined,
  ReloadOutlined,
  RightOutlined,
  UploadOutlined,
  UnlockOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import PageHeaderWrapper from '@/components/PageHeaderWrapper';
import {
  getRunbookCatalog,
  getRunbookStatus,
  prewarmRunbookRuntimeInspectionCache,
  runbookTechnicalPreflight,
} from '@/services/runbook';
import {
  PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
  PROVISIONING_SCREEN_PATH_BY_KEY,
} from './data/provisioningContract';
import OfficialRuntimeInspectionDrawer from './experiences/OfficialRuntimeInspectionDrawer';
import { resolveOnboardingRunbookHandoffFromTrail } from './experiences/provisioningOnboardingHandoffUtils';
import {
  buildOfficialRunEvidenceMeta,
  buildOrganizationRuntimeTopologyRows,
  buildOrganizationWorkspaceReadModels,
} from './experiences/provisioningRuntimeTopologyUtils';
import {
  buildOverviewOperationalCard,
  buildOverviewOperationalFilterOptions,
  filterOverviewOperationalCards,
  OVERVIEW_EMPTY_CARD_FILTERS,
} from './overviewOperationalCards';
import {
  OperationalWindowDialog,
  OperationalWindowManagerProvider,
} from './components/OperationalWindowManager';
import {
  formatCognusDateTime,
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from './cognusI18n';
import useOfficialRuntimeInspection from './experiences/useOfficialRuntimeInspection';
import styles from './Overview.less';

const RUNBOOK_AUDIT_HISTORY_STORAGE_KEY = 'cognus.provisioning.runbook.audit.history.v2';
const RUNBOOK_AUDIT_SELECTED_STORAGE_KEY = 'cognus.provisioning.runbook.audit.selected.v1';
const ORGANIZATION_VM_ACCESS_STORAGE_KEY = 'cognus.overview.organization.vm-access.v2';
const RUNBOOK_ROUTE_PATH = PROVISIONING_SCREEN_PATH_BY_KEY['e1-provisionamento'];
const RUNTIME_TOPOLOGY_ROUTE_PATH =
  PROVISIONING_SCREEN_PATH_BY_KEY[PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY];
const CHANNEL_WORKSPACE_ROUTE_PATH =
  PROVISIONING_SCREEN_PATH_BY_KEY[PROVISIONING_CHANNEL_WORKSPACE_SCREEN_KEY];
const EMPTY_OFFICIAL_ORGANIZATION_STATE = {
  loading: false,
  run: null,
  gate: null,
  error: '',
};

const createEmptyWorkspaceWindowState = () => ({
  peers: false,
  orderers: false,
  services: false,
  channels: false,
  chaincodes: false,
  audit: false,
});

const resolveOverviewGovernedActionConfig = activeGovernedAction =>
  ({
    addPeer: {
      title: pickCognusText('Adicionar peer', 'Add peer'),
      summary: pickCognusText(
        'Fluxo governado para inclusão de peer nesta organização.',
        'Governed flow for adding a peer to this organization.'
      ),
    },
    addOrderer: {
      title: pickCognusText('Adicionar orderer', 'Add orderer'),
      summary: pickCognusText(
        'Fluxo governado para inclusão de orderer neste runtime oficial.',
        'Governed flow for adding an orderer to this official runtime.'
      ),
    },
    addChannel: {
      title: pickCognusText('Adicionar channel', 'Add channel'),
      summary: pickCognusText(
        'Fluxo governado para criação de novo channel associado ao business group.',
        'Governed flow for creating a new channel linked to the business group.'
      ),
    },
    addChaincode: {
      title: pickCognusText('Adicionar chaincode', 'Add chaincode'),
      summary: pickCognusText(
        'Fluxo governado para onboarding e publicação de chaincode neste ecossistema.',
        'Governed flow for chaincode onboarding and publication in this ecosystem.'
      ),
    },
  }[activeGovernedAction] || null);

const OVERVIEW_MESSAGE_TYPE_BY_ALERT_TYPE = Object.freeze({
  success: 'success',
  warning: 'warning',
  error: 'error',
  info: 'info',
});

const buildOperationalWorkspaceNotice = ({ organization, operationalCard }) => {
  if (!organization || !operationalCard) {
    return '';
  }

  const organizationName =
    organization.orgName ||
    organization.orgId ||
    pickCognusText('Organizacao', 'Organization');
  const statusPrefixByState = {
    implemented: pickCognusText(
      'Painel operacional pronto para uso.',
      'Operational panel ready for use.'
    ),
    partial: pickCognusText(
      'Painel operacional exige atencao.',
      'Operational panel requires attention.'
    ),
    blocked: pickCognusText('Painel operacional bloqueado.', 'Operational panel blocked.'),
    pending: pickCognusText(
      'Painel operacional em preparacao.',
      'Operational panel in preparation.'
    ),
  };

  return [
    `${organizationName}: ${
      statusPrefixByState[operationalCard.operationalStatus] ||
      pickCognusText('Painel operacional atualizado.', 'Operational panel updated.')
    }`,
    operationalCard.statusReason,
    operationalCard.baselineLabel,
    operationalCard.freshnessLabel,
    pickCognusText(
      `${operationalCard.pendingAlertsCount} alerta(s) pendente(s).`,
      `${operationalCard.pendingAlertsCount} pending alert(s).`
    ),
  ]
    .filter(Boolean)
    .join(' ');
};

const buildTelemetryWorkspaceNotice = telemetryEntry => {
  if (!telemetryEntry) {
    return { type: 'info', content: '' };
  }

  if (telemetryEntry.status === 'error') {
    return {
      type: 'warning',
      content: `${pickCognusText(
        'Nao foi possivel atualizar a telemetria em tempo real.',
        'Could not refresh live telemetry.'
      )} ${normalizeText(telemetryEntry.message)}`.trim(),
    };
  }

  if (telemetryEntry.status === 'audit_only') {
    return {
      type: 'info',
      content: `${pickCognusText(
        'Telemetria em tempo real desativada neste workspace.',
        'Live telemetry disabled in this workspace.'
      )} ${normalizeText(telemetryEntry.message)}`.trim(),
    };
  }

  if (telemetryEntry.status === 'unavailable') {
    return {
      type: 'info',
      content: `${pickCognusText(
        'Telemetria em tempo real indisponivel para esta organizacao.',
        'Live telemetry unavailable for this organization.'
      )} ${normalizeText(telemetryEntry.message)}`.trim(),
    };
  }

  return { type: 'info', content: '' };
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

const readOrganizationVmAccessRegistry = () => {
  const storage = getBrowserSessionStorage();
  if (!storage) {
    return {};
  }

  const rawPayload = storage.getItem(ORGANIZATION_VM_ACCESS_STORAGE_KEY);
  if (!rawPayload) {
    return {};
  }

  try {
    const parsedPayload = JSON.parse(rawPayload);
    return parsedPayload && typeof parsedPayload === 'object' && !Array.isArray(parsedPayload)
      ? parsedPayload
      : {};
  } catch (error) {
    storage.removeItem(ORGANIZATION_VM_ACCESS_STORAGE_KEY);
    return {};
  }
};

const persistOrganizationVmAccessRegistry = registry => {
  const storage = getBrowserSessionStorage();
  if (!storage) {
    return;
  }

  try {
    storage.setItem(
      ORGANIZATION_VM_ACCESS_STORAGE_KEY,
      JSON.stringify(registry && typeof registry === 'object' ? registry : {})
    );
  } catch (error) {
    // Session-only access gate is best-effort.
  }
};

function normalizeSessionVmAccessHostMapping(hostMapping) {
  return (Array.isArray(hostMapping) ? hostMapping : [])
    .map(entry => ({
      hostRef: normalizeText(entry && (entry.hostRef || entry.host_ref)),
      hostAddress: normalizeText(entry && (entry.hostAddress || entry.host_address)),
      sshUser: normalizeText(entry && (entry.sshUser || entry.ssh_user)),
      sshPort: toSafePositiveInt(entry && (entry.sshPort || entry.ssh_port)) || 22,
      dockerPort: toSafePositiveInt(entry && (entry.dockerPort || entry.docker_port)) || 2376,
    }))
    .filter(entry => entry.hostAddress && entry.sshUser);
}

function normalizeSessionVmAccessMachineCredentials(machineCredentials) {
  return (Array.isArray(machineCredentials) ? machineCredentials : [])
    .map(entry => ({
      machine_id: normalizeText(entry && entry.machine_id),
      credential_ref: normalizeText(entry && entry.credential_ref),
      credential_payload: normalizeText(entry && entry.credential_payload),
      credential_fingerprint: normalizeText(entry && entry.credential_fingerprint),
      reuse_confirmed: Boolean(entry && entry.reuse_confirmed),
    }))
    .filter(
      entry =>
        entry.machine_id &&
        (entry.credential_ref || entry.credential_fingerprint) &&
        (!entry.credential_ref.toLowerCase().startsWith('local-file:') || entry.credential_payload)
    );
}

const normalizeOrganizationVmAccessEntry = entry => {
  const safeEntry = entry && typeof entry === 'object' ? entry : {};
  return {
    grantedAtUtc: normalizeText(safeEntry.grantedAtUtc),
    validatedMachineCount: toSafePositiveInt(
      safeEntry.validatedMachineCount ||
        (Array.isArray(safeEntry.validatedMachines) ? safeEntry.validatedMachines.length : 0)
    ),
    validatedHostMapping: normalizeSessionVmAccessHostMapping(
      safeEntry.validatedHostMapping || safeEntry.validated_host_mapping
    ),
    validatedMachineCredentials: normalizeSessionVmAccessMachineCredentials(
      safeEntry.validatedMachineCredentials || safeEntry.validated_machine_credentials
    ),
  };
};

const hasOrganizationVmAccess = (registry, organizationKey) => {
  const normalizedOrganizationKey = normalizeText(organizationKey).toLowerCase();
  if (!normalizedOrganizationKey) {
    return false;
  }

  return Boolean(
    registry &&
      registry[normalizedOrganizationKey] &&
      normalizeText(registry[normalizedOrganizationKey].grantedAtUtc)
  );
};

const resolveProtectedValue = (value, accessGranted, hiddenLabel = 'protegido') => {
  const normalizedValue = normalizeText(value);
  if (!normalizedValue) {
    return '-';
  }
  return accessGranted ? normalizedValue : hiddenLabel;
};

const resolveProtectedHostMeta = (value, accessGranted) => {
  const normalizedValue = normalizeText(value);
  if (!normalizedValue) {
    return '';
  }
  return accessGranted
    ? normalizedValue
    : pickCognusText('dados do host protegidos', 'protected host data');
};

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
        resolve(encodeBase64Buffer(event.target.result));
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

const resolvePemFileNameFromCredentialRef = credentialRef => {
  const normalizedRef = normalizeText(credentialRef);
  if (!normalizedRef.toLowerCase().startsWith('local-file:')) {
    return '';
  }
  return normalizeText(normalizedRef.slice('local-file:'.length));
};

const buildOrganizationVmAccessTargets = ({ organization, machineCredentials }) => {
  const hostRows = resolveOrganizationTelemetryHostRows(organization);
  const credentialByMachineId = new Map(
    filterMachineCredentialsByHostRows(machineCredentials, hostRows).map(entry => [
      normalizeText(entry && entry.machine_id),
      entry,
    ])
  );

  const registry = new Map();
  hostRows.forEach((row, index) => {
    const machineId = normalizeText(row && (row.machine_id || row.machineId));
    const hostRef = normalizeText(row && (row.hostRef || row.host_ref));
    const hostAddress = normalizeText(row && (row.hostAddress || row.host_address));
    const key = machineId || hostRef || hostAddress || `vm-${index + 1}`;
    const current = registry.get(key) || {};
    const credential = credentialByMachineId.get(machineId) || {};

    registry.set(key, {
      key,
      label: `VM ${index + 1}`,
      machineId,
      hostRef: current.hostRef || hostRef,
      hostAddress: current.hostAddress || hostAddress,
      sshUser: current.sshUser || normalizeText(row && (row.sshUser || row.ssh_user)),
      sshPort: current.sshPort || toSafePositiveInt(row && (row.sshPort || row.ssh_port)) || 22,
      dockerPort:
        current.dockerPort || toSafePositiveInt(row && (row.dockerPort || row.docker_port)) || 2376,
      credentialRef: current.credentialRef || normalizeText(credential && credential.credential_ref),
      credentialFingerprint:
        current.credentialFingerprint ||
        normalizeText(credential && credential.credential_fingerprint),
    });
  });

  return Array.from(registry.values());
};

const createVmAccessDraftEntry = target => ({
  key: normalizeText(target && target.key),
  label: normalizeText(target && target.label) || 'VM',
  hostAddress: '',
  sshUser: '',
  sshPort: toSafePositiveInt(target && target.sshPort) || 22,
  dockerPort: toSafePositiveInt(target && target.dockerPort) || 2376,
  credentialRef:
    resolvePemFileNameFromCredentialRef(target && target.credentialRef)
      ? ''
      : normalizeText(target && target.credentialRef),
  pemFileName: '',
  pemPayload: '',
  pemFingerprint: '',
});

const validateVmAccessDraftEntries = ({ targetEntries, draftEntries }) => {
  const targets = Array.isArray(targetEntries) ? targetEntries : [];
  if (targets.length === 0) {
    return {
      ok: false,
      message: pickCognusText(
        'Esta organizacao ainda nao possui VMs catalogadas com dados suficientes para liberar acesso.',
        'This organization does not yet have cataloged VMs with enough data to grant access.'
      ),
    };
  }

  const draftByKey = new Map(
    (Array.isArray(draftEntries) ? draftEntries : []).map(entry => [normalizeText(entry && entry.key), entry])
  );

  for (let index = 0; index < targets.length; index += 1) {
    const target = targets[index];
    const draft = draftByKey.get(normalizeText(target && target.key));
    const label = normalizeText(target && target.label) || `VM ${index + 1}`;
    if (!draft) {
      return {
        ok: false,
        message: pickCognusText(
          `Preencha as credenciais completas de ${label}.`,
          `Fill in the full credentials for ${label}.`
        ),
      };
    }

    const hostAddress = normalizeText(draft.hostAddress);
    const sshUser = normalizeText(draft.sshUser);
    const sshPort = toSafePositiveInt(draft.sshPort);
    const pemFileName = normalizeText(draft.pemFileName);
    const pemFingerprint = normalizeText(draft.pemFingerprint);
    const credentialRef = normalizeText(draft.credentialRef);

    if (!hostAddress || !sshUser || !sshPort) {
      return {
        ok: false,
        message: pickCognusText(
          `${label}: preencha host_address, ssh_user e ssh_port para validar o acesso.`,
          `${label}: fill in host_address, ssh_user, and ssh_port to validate access.`
        ),
      };
    }

    if (normalizeText(target.hostAddress) && hostAddress !== normalizeText(target.hostAddress)) {
      return {
        ok: false,
        message: pickCognusText(
          `${label}: host_address nao confere com o provisionamento.`,
          `${label}: host_address does not match the provisioning data.`
        ),
      };
    }

    if (normalizeText(target.sshUser) && sshUser !== normalizeText(target.sshUser)) {
      return {
        ok: false,
        message: pickCognusText(
          `${label}: ssh_user nao confere com o provisionamento.`,
          `${label}: ssh_user does not match the provisioning data.`
        ),
      };
    }

    if (toSafePositiveInt(target.sshPort) && sshPort !== toSafePositiveInt(target.sshPort)) {
      return {
        ok: false,
        message: pickCognusText(
          `${label}: ssh_port nao confere com o provisionamento.`,
          `${label}: ssh_port does not match the provisioning data.`
        ),
      };
    }

    const expectedFingerprint = normalizeText(target.credentialFingerprint);
    const expectedCredentialRef = normalizeText(target.credentialRef);
    const expectedPemFileName = resolvePemFileNameFromCredentialRef(expectedCredentialRef);

    if (expectedFingerprint) {
      if (!pemFingerprint || pemFingerprint !== expectedFingerprint) {
        return {
          ok: false,
          message: pickCognusText(
            `${label}: selecione a chave .pem correta desta VM.`,
            `${label}: select the correct .pem key for this VM.`
          ),
        };
      }
    } else if (expectedPemFileName) {
      if (!pemFileName || pemFileName !== expectedPemFileName) {
        return {
          ok: false,
          message: pickCognusText(
            `${label}: o arquivo .pem nao corresponde ao provisionamento.`,
            `${label}: the .pem file does not match the provisioning data.`
          ),
        };
      }
    } else if (expectedCredentialRef) {
      if (!credentialRef || credentialRef !== expectedCredentialRef) {
        return {
          ok: false,
          message: pickCognusText(
            `${label}: credential_ref nao confere com o provisionamento.`,
            `${label}: credential_ref does not match the provisioning data.`
          ),
        };
      }
    } else if (!pemFileName && !credentialRef) {
      return {
        ok: false,
        message: pickCognusText(
          `${label}: informe a credencial SSH da VM para liberar o acesso.`,
          `${label}: provide the VM SSH credential to grant access.`
        ),
      };
    }
  }

  return {
    ok: true,
    validatedMachineCount: targets.length,
  };
};

const buildVmAccessGrantFromDraftEntries = ({ targetEntries, draftEntries }) => {
  const draftByKey = new Map(
    (Array.isArray(draftEntries) ? draftEntries : []).map(entry => [normalizeText(entry && entry.key), entry])
  );

  const validatedHostMapping = [];
  const validatedMachineCredentials = [];
  const reuseCountByKey = new Map();

  (Array.isArray(targetEntries) ? targetEntries : []).forEach(target => {
    const draft = draftByKey.get(normalizeText(target && target.key)) || {};
    const machineId = normalizeText(
      (target && (target.machineId || target.hostRef || target.hostAddress || target.key)) || ''
    );
    const hostAddress = normalizeText(draft.hostAddress || (target && target.hostAddress));
    const sshUser = normalizeText(draft.sshUser || (target && target.sshUser));
    const sshPort = toSafePositiveInt(draft.sshPort || (target && target.sshPort)) || 22;
    const dockerPort =
      toSafePositiveInt(draft.dockerPort || (target && target.dockerPort)) || 2376;
    const hostRef = normalizeText(target && target.hostRef);

    validatedHostMapping.push({
      hostRef,
      hostAddress,
      sshUser,
      sshPort,
      dockerPort,
    });

    const pemFileName = normalizeText(draft.pemFileName);
    const pemPayload = normalizeText(draft.pemPayload);
    const pemFingerprint = normalizeText(draft.pemFingerprint);
    const explicitCredentialRef = normalizeText(draft.credentialRef || (target && target.credentialRef));
    const credentialRef = pemFileName ? `local-file:${pemFileName}` : explicitCredentialRef;
    const credentialFingerprint =
      pemFingerprint || normalizeText(target && target.credentialFingerprint);

    validatedMachineCredentials.push({
      machine_id: machineId,
      credential_ref: credentialRef,
      credential_payload: pemPayload,
      credential_fingerprint: credentialFingerprint,
      reuse_confirmed: false,
    });

    const reuseKey = normalizeText(credentialRef || credentialFingerprint);
    if (reuseKey) {
      reuseCountByKey.set(reuseKey, (reuseCountByKey.get(reuseKey) || 0) + 1);
    }
  });

  return {
    validatedMachineCount: validatedMachineCredentials.length,
    validatedHostMapping,
    validatedMachineCredentials: validatedMachineCredentials.map(entry => {
      const reuseKey = normalizeText(entry.credential_ref || entry.credential_fingerprint);
      return {
        ...entry,
        reuse_confirmed: (reuseCountByKey.get(reuseKey) || 0) > 1,
      };
    }),
  };
};

const toSafePositiveInt = value => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const normalizeText = value => String(value || '').trim();

const normalizeToken = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');

const normalizeArrayOfText = value =>
  (Array.isArray(value) ? value : []).map(entry => String(entry || '').trim()).filter(Boolean);

const safeArray = value => (Array.isArray(value) ? value : []);

const normalizeAuditOrganizationSummary = organizations =>
  (Array.isArray(organizations) ? organizations : [])
    .map(entry => ({
      orgId: normalizeText(entry && entry.orgId),
      orgName: normalizeText(entry && entry.orgName),
      nodeCount: toSafePositiveInt(entry && entry.nodeCount),
      peerCount: toSafePositiveInt(entry && entry.peerCount),
      ordererCount: toSafePositiveInt(entry && entry.ordererCount),
      caCount: toSafePositiveInt(entry && entry.caCount),
      apiCount: toSafePositiveInt(entry && entry.apiCount),
      channels: normalizeArrayOfText(entry && entry.channels),
      chaincodes: normalizeArrayOfText(entry && entry.chaincodes),
    }))
    .filter(entry => entry.orgId || entry.orgName);

const normalizeTopologyHost = host => ({
  hostRef: normalizeText(
    host && (host.hostRef || host.host_ref || host.infraLabel || host.infra_label)
  ),
  hostAddress: normalizeText(host && (host.hostAddress || host.host_address)),
  sshUser: normalizeText(host && (host.sshUser || host.ssh_user)),
  sshPort: toSafePositiveInt(host && (host.sshPort || host.ssh_port)) || 22,
  dockerPort: toSafePositiveInt(host && (host.dockerPort || host.docker_port)) || 2376,
});

const normalizeTopologyNode = node => ({
  nodeId: normalizeText(node && (node.nodeId || node.node_id)),
  nodeType: normalizeText(node && (node.nodeType || node.node_type)).toLowerCase(),
  hostRef: normalizeText(node && (node.hostRef || node.host_ref)),
  hostAddress: normalizeText(node && (node.hostAddress || node.host_address)),
});

const normalizeTopologyOrganization = organization => ({
  orgId: normalizeText(
    organization && (organization.orgId || organization.org_id || organization.org_key)
  ),
  orgName: normalizeText(organization && (organization.orgName || organization.org_name)),
  domain: normalizeText(organization && organization.domain),
  peerHostRef: normalizeText(
    organization && (organization.peerHostRef || organization.peer_host_ref)
  ),
  ordererHostRef: normalizeText(
    organization && (organization.ordererHostRef || organization.orderer_host_ref)
  ),
  ca: {
    mode: normalizeText(organization && organization.ca && organization.ca.mode),
    name: normalizeText(organization && organization.ca && organization.ca.name),
    host: normalizeText(organization && organization.ca && organization.ca.host),
    port: toSafePositiveInt(organization && organization.ca && organization.ca.port),
    user: normalizeText(organization && organization.ca && organization.ca.user),
    hostRef: normalizeText(
      organization && organization.ca && (organization.ca.hostRef || organization.ca.host_ref)
    ),
  },
  networkApi: {
    host: normalizeText(
      (organization && organization.networkApi && organization.networkApi.host) ||
        (organization && organization.network_api && organization.network_api.host)
    ),
    port: toSafePositiveInt(
      (organization && organization.networkApi && organization.networkApi.port) ||
        (organization && organization.network_api && organization.network_api.port)
    ),
    exposureIp: normalizeText(
      (organization && organization.networkApi && organization.networkApi.exposureIp) ||
        (organization && organization.network_api && organization.network_api.exposure_ip)
    ),
  },
  peers: (Array.isArray(organization && organization.peers) ? organization.peers : [])
    .map(normalizeTopologyNode)
    .filter(node => node.nodeId || node.hostRef || node.hostAddress),
  orderers: (Array.isArray(organization && organization.orderers) ? organization.orderers : [])
    .map(normalizeTopologyNode)
    .filter(node => node.nodeId || node.hostRef || node.hostAddress),
  cas: (Array.isArray(organization && organization.cas) ? organization.cas : [])
    .map(normalizeTopologyNode)
    .filter(node => node.nodeId || node.hostRef || node.hostAddress),
  channels: normalizeArrayOfText(organization && organization.channels),
  chaincodes: normalizeArrayOfText(organization && organization.chaincodes),
  apis: (Array.isArray(organization && organization.apis) ? organization.apis : [])
    .map(api => ({
      apiId: normalizeText(api && (api.apiId || api.api_id)),
      channelId: normalizeText(api && (api.channelId || api.channel_id)),
      chaincodeId: normalizeText(api && (api.chaincodeId || api.chaincode_id)),
      routePath: normalizeText(api && (api.routePath || api.route_path)),
      exposureHost: normalizeText(api && api.exposure && api.exposure.host),
      exposurePort: toSafePositiveInt(api && api.exposure && api.exposure.port),
      statusHint: normalizeText(api && (api.statusHint || api.status)),
    }))
    .filter(api => api.apiId || api.routePath || api.exposureHost),
});

const normalizeTopologyBusinessGroup = businessGroup => ({
  groupId: normalizeText(
    businessGroup &&
      (businessGroup.groupId ||
        businessGroup.group_id ||
        businessGroup.networkId ||
        businessGroup.network_id)
  ),
  name: normalizeText(businessGroup && businessGroup.name),
  networkId: normalizeText(businessGroup && (businessGroup.networkId || businessGroup.network_id)),
  channels: (Array.isArray(businessGroup && businessGroup.channels) ? businessGroup.channels : [])
    .map(channel => ({
      channelId: normalizeText(channel && (channel.channelId || channel.channel_id)),
      memberOrgs: normalizeArrayOfText(channel && (channel.memberOrgs || channel.member_orgs)),
      chaincodes: normalizeArrayOfText(channel && channel.chaincodes),
    }))
    .filter(channel => channel.channelId),
});

const resolveTopologyBusinessGroups = safeTopology => {
  if (Array.isArray(safeTopology.businessGroups)) {
    return safeTopology.businessGroups;
  }
  if (Array.isArray(safeTopology.business_groups)) {
    return safeTopology.business_groups;
  }
  return [];
};

const normalizeAuditTopology = topology => {
  const safeTopology = topology && typeof topology === 'object' ? topology : {};
  return {
    hosts: (Array.isArray(safeTopology.hosts) ? safeTopology.hosts : [])
      .map(normalizeTopologyHost)
      .filter(host => host.hostRef || host.hostAddress),
    organizations: (Array.isArray(safeTopology.organizations) ? safeTopology.organizations : [])
      .map(normalizeTopologyOrganization)
      .filter(org => org.orgId || org.orgName),
    businessGroups: resolveTopologyBusinessGroups(safeTopology)
      .map(normalizeTopologyBusinessGroup)
      .filter(group => group.groupId || group.name || group.networkId),
  };
};

const normalizeAuditHostMapping = hostMapping =>
  (Array.isArray(hostMapping) ? hostMapping : [])
    .map(entry => ({
      orgId: normalizeText(entry && (entry.org_id || entry.orgId)),
      hostRef: normalizeText(entry && (entry.host_ref || entry.hostRef || entry.node_id || entry.nodeId)),
      hostAddress: normalizeText(entry && (entry.host_address || entry.hostAddress)),
      sshUser: normalizeText(entry && (entry.ssh_user || entry.sshUser)),
      sshPort: toSafePositiveInt(entry && (entry.ssh_port || entry.sshPort)) || 22,
      dockerPort: toSafePositiveInt(entry && (entry.docker_port || entry.dockerPort)) || 2376,
      nodeType: normalizeText(entry && (entry.node_type || entry.nodeType)).toLowerCase(),
    }))
    .filter(entry => entry.hostRef || entry.hostAddress);

const normalizeAuditMachineCredentials = machineCredentials =>
  (Array.isArray(machineCredentials) ? machineCredentials : [])
    .map(entry => ({
      machine_id: normalizeText(entry && entry.machine_id),
      credential_ref: normalizeText(entry && entry.credential_ref),
      credential_payload: normalizeText(entry && entry.credential_payload),
      credential_fingerprint: normalizeText(entry && entry.credential_fingerprint),
      reuse_confirmed: Boolean(entry && entry.reuse_confirmed),
    }))
    .filter(entry => entry.machine_id && (entry.credential_ref || entry.credential_fingerprint));

const readProvisioningAuditHistory = () => {
  const storage = getBrowserLocalStorage();
  if (!storage) {
    return [];
  }

  const rawHistoryPayload = storage.getItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
  if (!rawHistoryPayload) {
    return [];
  }

  try {
    const parsedHistory = JSON.parse(rawHistoryPayload);
    return (Array.isArray(parsedHistory) ? parsedHistory : [])
      .filter(entry => entry && typeof entry === 'object')
      .map(entry => {
        const context = entry.context && typeof entry.context === 'object' ? entry.context : {};
        const handoffFromTrail = resolveOnboardingRunbookHandoffFromTrail({
          runId: normalizeText(entry.runId),
          changeId: normalizeText(entry.changeId),
        });
        const organizations = normalizeAuditOrganizationSummary(context.organizations);
        const topology = normalizeAuditTopology(
          context.topology ||
            context.topology_catalog ||
            (handoffFromTrail && handoffFromTrail.topology_catalog)
        );
        const hostMapping = normalizeAuditHostMapping(
          context.host_mapping || (handoffFromTrail && handoffFromTrail.host_mapping)
        );
        const machineCredentials = normalizeAuditMachineCredentials(
          context.machine_credentials || (handoffFromTrail && handoffFromTrail.machine_credentials)
        );
        return {
          key: normalizeText(entry.key),
          runId: normalizeText(entry.runId),
          changeId: normalizeText(entry.changeId),
          status: normalizeText(entry.status).toLowerCase(),
          finishedAt: normalizeText(entry.finishedAt),
          capturedAt: normalizeText(entry.capturedAt),
          context: {
            providerKey: normalizeText(context.providerKey),
            environmentProfile: normalizeText(context.environmentProfile),
            hostCount: toSafePositiveInt(context.hostCount),
            organizationCount: toSafePositiveInt(context.organizationCount),
            nodeCount: toSafePositiveInt(context.nodeCount),
            apiCount: toSafePositiveInt(context.apiCount),
            incrementalCount: toSafePositiveInt(context.incrementalCount),
            organizations,
            topology,
            hostMapping,
            machineCredentials,
            handoffFingerprint: normalizeText(
              context.handoffFingerprint ||
                context.handoff_fingerprint ||
                (handoffFromTrail && handoffFromTrail.handoff_fingerprint)
            ),
          },
        };
      })
      .filter(entry => entry.key && entry.runId);
  } catch (error) {
    storage.removeItem(RUNBOOK_AUDIT_HISTORY_STORAGE_KEY);
    return [];
  }
};

const writeProvisioningAuditHistory = historyEntries => {
  const storage = getBrowserLocalStorage();
  if (!storage) {
    return;
  }

  try {
    storage.setItem(
      RUNBOOK_AUDIT_HISTORY_STORAGE_KEY,
      JSON.stringify(Array.isArray(historyEntries) ? historyEntries : [])
    );
  } catch (error) {
    // best-effort cache only
  }
};

const resolveExecutionTone = status => {
  if (status === 'completed') {
    return { label: pickCognusText('consolidado', 'consolidated'), color: 'green' };
  }
  if (status === 'failed') {
    return { label: pickCognusText('falha', 'failed'), color: 'red' };
  }
  return { label: pickCognusText('parcial', 'partial'), color: 'orange' };
};

const resolveA2AGateAlertType = gate => {
  const status = normalizeText(gate && gate.status).toLowerCase();
  if (status === 'implemented') {
    return 'success';
  }
  if (status === 'blocked') {
    return 'error';
  }
  return 'warning';
};

const resolveOperationalCriticalityColor = criticality => {
  if (criticality === 'critical') {
    return 'red';
  }
  if (criticality === 'warning') {
    return 'gold';
  }
  return 'green';
};

const resolveFreshnessTagColor = freshnessStatus => {
  if (freshnessStatus === 'fresh') {
    return 'green';
  }
  if (freshnessStatus === 'stale') {
    return 'orange';
  }
  return 'default';
};

const resolveRuntimeStatusTagColor = status => {
  if (status === 'running') {
    return 'green';
  }
  if (status === 'degraded') {
    return 'orange';
  }
  return 'default';
};

const createProjectedOrganization = ({ organizationKey, orgId, orgName }) => ({
  key: organizationKey,
  orgId,
  orgName,
  domain: '',
  peerCount: 0,
  ordererCount: 0,
  apiCount: 0,
  peers: [],
  orderers: [],
  cas: [],
  apis: [],
  channels: new Set(),
  chaincodes: new Set(),
  businessGroups: new Map(),
  hostTargets: new Map(),
  environments: new Set(),
  providers: new Set(),
  runHistory: [],
  latestRunId: '',
  latestEntryKey: '',
  latestStatus: '',
  latestFinishedAt: '',
  latestChangeId: '',
  latestHostMapping: [],
  latestMachineCredentials: [],
  latestHandoffFingerprint: '',
  ca: {
    mode: '',
    name: '',
    host: '',
    port: 0,
    user: '',
    hostRef: '',
  },
  networkApi: {
    host: '',
    port: 0,
    exposureIp: '',
  },
  isSynthetic: false,
  inferredOrganizationCount: 1,
});

const addHostTarget = (organization, host) => {
  if (!organization || !host) {
    return;
  }
  const hostRef = normalizeText(host.hostRef);
  const hostAddress = normalizeText(host.hostAddress);
  if (!hostRef && !hostAddress) {
    return;
  }

  const hostKey = hostRef || `addr:${hostAddress}`;
  if (!organization.hostTargets.has(hostKey)) {
    organization.hostTargets.set(hostKey, {
      hostRef,
      hostAddress,
      sshUser: normalizeText(host.sshUser),
      sshPort: toSafePositiveInt(host.sshPort) || 22,
      dockerPort: toSafePositiveInt(host.dockerPort) || 2376,
    });
  } else {
    const current = organization.hostTargets.get(hostKey);
    organization.hostTargets.set(hostKey, {
      ...current,
      hostAddress: current.hostAddress || hostAddress,
      sshUser: current.sshUser || normalizeText(host.sshUser),
      sshPort: current.sshPort || toSafePositiveInt(host.sshPort) || 22,
      dockerPort: current.dockerPort || toSafePositiveInt(host.dockerPort) || 2376,
    });
  }
};

const addBusinessGroup = (organization, group, channel) => {
  if (!organization || !group) {
    return;
  }

  const groupKey = normalizeText(group.groupId || group.name || group.networkId);
  if (!groupKey) {
    return;
  }

  if (!organization.businessGroups.has(groupKey)) {
    organization.businessGroups.set(groupKey, {
      groupId: normalizeText(group.groupId),
      name: normalizeText(group.name),
      networkId: normalizeText(group.networkId),
      channels: new Set(),
    });
  }

  if (channel) {
    const safeChannelId = normalizeText(channel.channelId || channel.channel_id || channel);
    if (safeChannelId) {
      organization.businessGroups.get(groupKey).channels.add(safeChannelId);
    }
  }
};

const buildOrganizationsDashboardProjection = historyEntries => {
  const sortedEntries = historyEntries
    .slice()
    .sort(
      (left, right) =>
        Date.parse(right.finishedAt || right.capturedAt || '') -
        Date.parse(left.finishedAt || left.capturedAt || '')
    );
  const registry = new Map();

  const ensureOrganization = ({ orgId, orgName }) => {
    const resolvedName = normalizeText(orgName);
    const resolvedId = normalizeText(orgId);
    const organizationKey = (resolvedId || resolvedName || '').toLowerCase();
    if (!organizationKey) {
      return null;
    }

    if (!registry.has(organizationKey)) {
      registry.set(
        organizationKey,
        createProjectedOrganization({
          organizationKey,
          orgId: resolvedId || resolvedName,
          orgName: resolvedName || resolvedId,
        })
      );
    }

    return registry.get(organizationKey);
  };

  sortedEntries.forEach(entry => {
    const context = entry.context || {};
    const topology = context.topology || { hosts: [], organizations: [], businessGroups: [] };
    const hostsByRef = new Map();
    const hostsByAddress = new Map();

    topology.hosts.forEach(host => {
      const normalizedHost = normalizeTopologyHost(host);
      if (normalizedHost.hostRef) {
        hostsByRef.set(normalizedHost.hostRef, normalizedHost);
      }
      if (normalizedHost.hostAddress) {
        hostsByAddress.set(normalizedHost.hostAddress, normalizedHost);
      }
    });

    let effectiveOrganizations = topology.organizations;

    if (effectiveOrganizations.length === 0) {
      const fallbackContextOrganizations = Array.isArray(context.organizations)
        ? context.organizations
        : [];

      effectiveOrganizations = fallbackContextOrganizations.map(organization => ({
        orgId: normalizeText(organization.orgId),
        orgName: normalizeText(organization.orgName),
        domain: '',
        peerCount: toSafePositiveInt(organization.peerCount),
        ordererCount: toSafePositiveInt(organization.ordererCount),
        apiCount: toSafePositiveInt(organization.apiCount),
        peers: [],
        orderers: [],
        cas: [],
        apis: [],
        channels: normalizeArrayOfText(organization.channels),
        chaincodes: normalizeArrayOfText(organization.chaincodes),
      }));

      if (effectiveOrganizations.length === 0) {
        const fallbackOrganizationCount = toSafePositiveInt(context.organizationCount);
        const shouldCreateSyntheticOrganization =
          fallbackOrganizationCount > 0 ||
          context.nodeCount > 0 ||
          context.apiCount > 0 ||
          entry.status === 'completed';

        if (shouldCreateSyntheticOrganization) {
          const fallbackName = entry.changeId
            ? pickCognusText(
                `Organizações do ${entry.changeId}`,
                `Organizations from ${entry.changeId}`
              )
            : pickCognusText(
                `Organizações do ${entry.runId}`,
                `Organizations from ${entry.runId}`
              );
          effectiveOrganizations = [
            {
              orgId: `legacy-${entry.runId}`,
              orgName: fallbackName,
              domain: '',
              peerCount: 0,
              ordererCount: 0,
              apiCount: toSafePositiveInt(context.apiCount),
              peers: [],
              orderers: [],
              cas: [],
              apis: [],
              channels: [],
              chaincodes: [],
              isSynthetic: true,
              inferredOrganizationCount: Math.max(1, fallbackOrganizationCount),
            },
          ];
        }
      }
    }

    effectiveOrganizations.forEach(org => {
      const projectedOrganization = ensureOrganization({ orgId: org.orgId, orgName: org.orgName });
      if (!projectedOrganization) {
        return;
      }

      projectedOrganization.orgId = projectedOrganization.orgId || normalizeText(org.orgId);
      projectedOrganization.orgName = projectedOrganization.orgName || normalizeText(org.orgName);
      projectedOrganization.domain = projectedOrganization.domain || normalizeText(org.domain);

      const incomingPeers = Array.isArray(org.peers)
        ? org.peers
            .map(normalizeTopologyNode)
            .filter(node => node.nodeId || node.hostRef || node.hostAddress)
        : [];
      const incomingOrderers = Array.isArray(org.orderers)
        ? org.orderers
            .map(normalizeTopologyNode)
            .filter(node => node.nodeId || node.hostRef || node.hostAddress)
        : [];
      const incomingCas = Array.isArray(org.cas)
        ? org.cas
            .map(normalizeTopologyNode)
            .filter(node => node.nodeId || node.hostRef || node.hostAddress)
        : [];

      if (incomingPeers.length > projectedOrganization.peers.length) {
        projectedOrganization.peers = incomingPeers;
      }
      if (incomingOrderers.length > projectedOrganization.orderers.length) {
        projectedOrganization.orderers = incomingOrderers;
      }
      if (incomingCas.length > projectedOrganization.cas.length) {
        projectedOrganization.cas = incomingCas;
      }

      const incomingCa = org.ca && typeof org.ca === 'object' ? org.ca : {};
      projectedOrganization.ca = {
        mode: projectedOrganization.ca.mode || normalizeText(incomingCa.mode),
        name: projectedOrganization.ca.name || normalizeText(incomingCa.name),
        host: projectedOrganization.ca.host || normalizeText(incomingCa.host),
        port: projectedOrganization.ca.port || toSafePositiveInt(incomingCa.port),
        user: projectedOrganization.ca.user || normalizeText(incomingCa.user),
        hostRef:
          projectedOrganization.ca.hostRef ||
          normalizeText(incomingCa.hostRef || incomingCa.host_ref),
      };

      const incomingNetworkApi =
        org.networkApi && typeof org.networkApi === 'object' ? org.networkApi : {};
      projectedOrganization.networkApi = {
        host: projectedOrganization.networkApi.host || normalizeText(incomingNetworkApi.host),
        port: projectedOrganization.networkApi.port || toSafePositiveInt(incomingNetworkApi.port),
        exposureIp:
          projectedOrganization.networkApi.exposureIp ||
          normalizeText(incomingNetworkApi.exposureIp),
      };

      const incomingApis = Array.isArray(org.apis) ? org.apis : [];
      if (incomingApis.length > projectedOrganization.apis.length) {
        projectedOrganization.apis = incomingApis;
      }

      normalizeArrayOfText(org.channels).forEach(channelId =>
        projectedOrganization.channels.add(channelId)
      );
      normalizeArrayOfText(org.chaincodes).forEach(chaincodeId =>
        projectedOrganization.chaincodes.add(chaincodeId)
      );

      const peerCountFromTopology = incomingPeers.length;
      const ordererCountFromTopology = incomingOrderers.length;
      const apiCountFromTopology = incomingApis.length;
      projectedOrganization.peerCount = Math.max(
        projectedOrganization.peerCount,
        peerCountFromTopology,
        toSafePositiveInt(org.peerCount)
      );
      projectedOrganization.ordererCount = Math.max(
        projectedOrganization.ordererCount,
        ordererCountFromTopology,
        toSafePositiveInt(org.ordererCount)
      );
      projectedOrganization.apiCount = Math.max(
        projectedOrganization.apiCount,
        apiCountFromTopology,
        toSafePositiveInt(org.apiCount)
      );

      projectedOrganization.isSynthetic =
        projectedOrganization.isSynthetic || Boolean(org.isSynthetic);
      projectedOrganization.inferredOrganizationCount = Math.max(
        projectedOrganization.inferredOrganizationCount,
        toSafePositiveInt(org.inferredOrganizationCount || 1)
      );

      if (context.environmentProfile) {
        projectedOrganization.environments.add(context.environmentProfile);
      }
      if (context.providerKey) {
        projectedOrganization.providers.add(context.providerKey);
      }

      const runHistoryEntry = {
        key: entry.key,
        runId: entry.runId,
        changeId: entry.changeId,
        status: entry.status,
        finishedAt: entry.finishedAt,
        capturedAt: entry.capturedAt,
      };

      if (!projectedOrganization.runHistory.find(run => run.key === runHistoryEntry.key)) {
        projectedOrganization.runHistory.push(runHistoryEntry);
      }

      if (!projectedOrganization.latestRunId) {
        projectedOrganization.latestRunId = entry.runId;
        projectedOrganization.latestEntryKey = entry.key;
        projectedOrganization.latestStatus = entry.status;
        projectedOrganization.latestFinishedAt = entry.finishedAt || entry.capturedAt;
        projectedOrganization.latestChangeId = entry.changeId;
        projectedOrganization.latestHostMapping = Array.isArray(context.hostMapping)
          ? context.hostMapping
          : [];
        projectedOrganization.latestMachineCredentials = Array.isArray(context.machineCredentials)
          ? context.machineCredentials
          : [];
        projectedOrganization.latestHandoffFingerprint = normalizeText(context.handoffFingerprint);
      }

      const declaredPeerHostRef = normalizeText(org.peerHostRef || org.peer_host_ref);
      const declaredOrdererHostRef = normalizeText(org.ordererHostRef || org.orderer_host_ref);
      const hostRefs = new Set(
        [
          declaredPeerHostRef,
          declaredOrdererHostRef,
          projectedOrganization.ca.hostRef,
          ...incomingPeers.map(node => node.hostRef),
          ...incomingOrderers.map(node => node.hostRef),
          ...incomingCas.map(node => node.hostRef),
        ].filter(Boolean)
      );

      hostRefs.forEach(hostRef => {
        const hostFromTopology = hostsByRef.get(hostRef);
        addHostTarget(projectedOrganization, {
          hostRef,
          hostAddress: hostFromTopology ? hostFromTopology.hostAddress : '',
          sshUser: hostFromTopology ? hostFromTopology.sshUser : '',
          sshPort: hostFromTopology ? hostFromTopology.sshPort : 22,
          dockerPort: hostFromTopology ? hostFromTopology.dockerPort : 2376,
        });
      });

      [...incomingPeers, ...incomingOrderers, ...incomingCas].forEach(node => {
        const hostFromAddress = node.hostAddress ? hostsByAddress.get(node.hostAddress) : null;
        addHostTarget(projectedOrganization, {
          hostRef: node.hostRef,
          hostAddress: node.hostAddress,
          sshUser: hostFromAddress ? hostFromAddress.sshUser : '',
          sshPort: hostFromAddress ? hostFromAddress.sshPort : 22,
          dockerPort: hostFromAddress ? hostFromAddress.dockerPort : 2376,
        });
      });

      if (projectedOrganization.ca.hostRef || projectedOrganization.ca.host) {
        const hostFromCaRef = projectedOrganization.ca.hostRef
          ? hostsByRef.get(projectedOrganization.ca.hostRef)
          : null;
        const hostFromCaAddress = projectedOrganization.ca.host
          ? hostsByAddress.get(projectedOrganization.ca.host)
          : null;
        addHostTarget(projectedOrganization, {
          hostRef: projectedOrganization.ca.hostRef,
          hostAddress:
            projectedOrganization.ca.host || (hostFromCaRef && hostFromCaRef.hostAddress),
          sshUser:
            (hostFromCaRef && hostFromCaRef.sshUser) ||
            (hostFromCaAddress && hostFromCaAddress.sshUser) ||
            '',
          sshPort:
            (hostFromCaRef && hostFromCaRef.sshPort) ||
            (hostFromCaAddress && hostFromCaAddress.sshPort) ||
            22,
          dockerPort:
            (hostFromCaRef && hostFromCaRef.dockerPort) ||
            (hostFromCaAddress && hostFromCaAddress.dockerPort) ||
            2376,
        });
      }

      incomingApis.forEach(api => {
        addHostTarget(projectedOrganization, {
          hostRef: '',
          hostAddress: normalizeText(api.exposureHost),
          sshUser: '',
          sshPort: 22,
          dockerPort: 2376,
        });
      });

      topology.businessGroups.forEach(group => {
        const channels = Array.isArray(group.channels) ? group.channels : [];
        channels.forEach(channel => {
          const orgTokens = normalizeArrayOfText(channel.memberOrgs).map(normalizeToken);
          const orgNameToken = normalizeToken(projectedOrganization.orgName);
          const orgIdToken = normalizeToken(projectedOrganization.orgId);
          const belongsToChannel =
            orgTokens.length === 0 ||
            orgTokens.includes(orgNameToken) ||
            orgTokens.includes(orgIdToken);
          if (!belongsToChannel) {
            return;
          }
          addBusinessGroup(projectedOrganization, group, channel.channelId);
          if (channel.channelId) {
            projectedOrganization.channels.add(channel.channelId);
          }
          normalizeArrayOfText(channel.chaincodes).forEach(chaincodeId =>
            projectedOrganization.chaincodes.add(chaincodeId)
          );
        });
      });
    });
  });

  return Array.from(registry.values())
    .map(entry => ({
      ...entry,
      channels: Array.from(entry.channels).sort(),
      chaincodes: Array.from(entry.chaincodes).sort(),
      environments: Array.from(entry.environments).sort(),
      providers: Array.from(entry.providers).sort(),
      hostTargets: Array.from(entry.hostTargets.values()),
      businessGroups: Array.from(entry.businessGroups.values()).map(group => ({
        groupId: group.groupId,
        name: group.name,
        networkId: group.networkId,
        channels: Array.from(group.channels).sort(),
      })),
      runHistory: entry.runHistory
        .slice()
        .sort(
          (left, right) =>
            Date.parse(right.finishedAt || right.capturedAt || '') -
            Date.parse(left.finishedAt || left.capturedAt || '')
        ),
    }))
    .sort(
      (left, right) =>
        Date.parse(right.latestFinishedAt || '') - Date.parse(left.latestFinishedAt || '')
    );
};

const resolveCachedStatusFallback = run => {
  const fallback =
    run && typeof run === 'object'
      ? run.cached_status_fallback || (run.snapshot && run.snapshot.cached_status_fallback)
      : null;

  if (!fallback || !fallback.active) {
    return null;
  }

  return {
    source: normalizeText(fallback.source) || 'official_status_cache',
    savedAtUtc: normalizeText(fallback.savedAtUtc || fallback.saved_at_utc),
    reasonCode: normalizeText(fallback.reasonCode || fallback.reason_code),
    reasonMessage: normalizeText(fallback.reasonMessage || fallback.reason_message),
  };
};

const buildOverviewTopologyDomains = (rows, localeCandidate) => {
  const safeRows = Array.isArray(rows) ? rows : [];
  const overviewTopologyDomains = [
    {
      key: 'identity',
      title: pickCognusText('Identidade e acesso', 'Identity and access', localeCandidate),
      summary: pickCognusText(
        'CA, gateway e pontos de entrada governados da organização.',
        'CA, gateway, and governed organization entry points.',
        localeCandidate
      ),
      componentTypes: ['ca', 'apiGateway', 'netapi'],
    },
    {
      key: 'fabric',
      title: pickCognusText('Malha Fabric', 'Fabric mesh', localeCandidate),
      summary: pickCognusText(
        'Peers, orderers e persistência do runtime operacional.',
        'Peers, orderers, and persistence in the operational runtime.',
        localeCandidate
      ),
      componentTypes: ['peer', 'orderer', 'couch'],
    },
    {
      key: 'chaincode',
      title: pickCognusText('Runtime de chaincode', 'Chaincode runtime', localeCandidate),
      summary: pickCognusText(
        'Serviços ativos e dependências de execução por canal.',
        'Active services and execution dependencies per channel.',
        localeCandidate
      ),
      componentTypes: ['chaincode_runtime'],
    },
  ];

  return overviewTopologyDomains.map(domain => {
    const items = safeRows
      .filter(row => domain.componentTypes.includes(normalizeText(row && row.componentType)))
      .sort((left, right) => {
        const leftKey = `${normalizeText(left && left.componentType)}:${normalizeText(
          left && left.componentName
        )}`;
        const rightKey = `${normalizeText(right && right.componentType)}:${normalizeText(
          right && right.componentName
        )}`;
        return leftKey.localeCompare(rightKey);
      });

    return {
      ...domain,
      items,
      criticalCount: items.filter(item => normalizeText(item && item.criticality) === 'critical')
        .length,
      degradedCount: items.filter(item => normalizeText(item && item.status) === 'degraded')
        .length,
    };
  }).filter(domain => domain.items.length > 0);
};

const sortOverviewRuntimeItems = items =>
  safeArray(items)
    .slice()
    .sort((left, right) => {
      const leftKey = `${normalizeText(left && (left.componentType || left.status))}:${normalizeText(
        left && (left.componentName || left.name || left.id)
      )}`;
      const rightKey = `${normalizeText(
        right && (right.componentType || right.status)
      )}:${normalizeText(right && (right.componentName || right.name || right.id))}`;
      return leftKey.localeCompare(rightKey);
    });

const selectOverviewTopologyItems = (rows, componentTypes) => {
  const allowedTypes = new Set(normalizeArrayOfText(componentTypes));
  return sortOverviewRuntimeItems(
    safeArray(rows).filter(row => allowedTypes.has(normalizeText(row && row.componentType)))
  );
};

const resolveWorkspaceProjectionId = value =>
  normalizeText(
    value &&
      (value.id ||
        value.name ||
        value.channelId ||
        value.channel_id ||
        value.chaincodeId ||
        value.chaincode_id ||
        value.componentId ||
        value.component_id)
  );

const resolveWorkspaceProjectionLookupKeys = value =>
  Array.from(
    new Set(
      [
        resolveWorkspaceProjectionId(value),
        normalizeText(value && value.id),
        normalizeText(value && value.name),
        normalizeText(value && value.channelId),
        normalizeText(value && value.chaincodeId),
      ]
        .map(entry => entry.toLowerCase())
        .filter(Boolean)
    )
  );

const buildBusinessScopeGroups = ({
  businessGroups,
  officialChannels,
  officialChaincodes,
  fallbackChannels,
}) => {
  const channelLookup = new Map();
  const chaincodeLookup = new Map();
  const chaincodesByChannel = new Map();
  const assignedChannelIds = new Set();

  const ensureChannelModel = channelId => {
    const safeChannelId = normalizeText(channelId);
    if (!safeChannelId) {
      return null;
    }

    if (!channelLookup.has(safeChannelId)) {
      channelLookup.set(safeChannelId, {
        key: safeChannelId,
        channelId: safeChannelId,
        name: safeChannelId,
        memberOrgs: [],
        chaincodeRefs: [],
        status: 'unknown',
        health: 'unknown',
        detailRef: null,
      });
    }

    return channelLookup.get(safeChannelId);
  };

  sortOverviewRuntimeItems(officialChannels).forEach(channel => {
    const channelId = resolveWorkspaceProjectionId(channel);
    if (!channelId) {
      return;
    }

    channelLookup.set(channelId, {
      key: channelId,
      channelId,
      name: normalizeText(channel && (channel.name || channel.id)) || channelId,
      memberOrgs: normalizeArrayOfText(channel && channel.memberOrgs),
      chaincodeRefs: normalizeArrayOfText(channel && channel.chaincodeRefs),
      status:
        normalizeText(channel && (channel.status || channel.health)).toLowerCase() || 'unknown',
      health:
        normalizeText(channel && (channel.health || channel.status)).toLowerCase() || 'unknown',
      detailRef: (channel && channel.detailRef) || null,
    });
  });

  normalizeArrayOfText(fallbackChannels).forEach(channelId => {
    ensureChannelModel(channelId);
  });

  sortOverviewRuntimeItems(officialChaincodes).forEach(chaincode => {
    const chaincodeId = resolveWorkspaceProjectionId(chaincode);
    if (!chaincodeId) {
      return;
    }

    const chaincodeModel = {
      key: chaincodeId,
      chaincodeId,
      name: normalizeText(chaincode && (chaincode.name || chaincode.id)) || chaincodeId,
      status:
        normalizeText(chaincode && (chaincode.status || chaincode.health)).toLowerCase() ||
        'unknown',
      health:
        normalizeText(chaincode && (chaincode.health || chaincode.status)).toLowerCase() ||
        'unknown',
      componentId: normalizeText(chaincode && chaincode.componentId),
      channelRefs: normalizeArrayOfText(chaincode && chaincode.channelRefs),
    };

    resolveWorkspaceProjectionLookupKeys(chaincode).forEach(lookupKey => {
      chaincodeLookup.set(lookupKey, chaincodeModel);
    });

    chaincodeModel.channelRefs.forEach(channelId => {
      const safeChannelId = normalizeText(channelId);
      if (!safeChannelId) {
        return;
      }

      if (!chaincodesByChannel.has(safeChannelId)) {
        chaincodesByChannel.set(safeChannelId, []);
      }

      chaincodesByChannel.get(safeChannelId).push(chaincodeModel);
    });
  });

  const buildChannelModel = channelId => {
    const safeChannelId = normalizeText(channelId);
    if (!safeChannelId) {
      return null;
    }

    const baseChannel = ensureChannelModel(safeChannelId);
    const channelChaincodes = new Map();

    safeArray(chaincodesByChannel.get(safeChannelId)).forEach(chaincode => {
      channelChaincodes.set(chaincode.key, chaincode);
    });

    normalizeArrayOfText(baseChannel && baseChannel.chaincodeRefs).forEach(chaincodeRef => {
      const lookupKey = normalizeText(chaincodeRef).toLowerCase();
      const resolvedChaincode = chaincodeLookup.get(lookupKey);

      if (resolvedChaincode) {
        channelChaincodes.set(resolvedChaincode.key, resolvedChaincode);
        return;
      }

      channelChaincodes.set(chaincodeRef, {
        key: chaincodeRef,
        chaincodeId: chaincodeRef,
        name: chaincodeRef,
        status: 'unknown',
        health: 'unknown',
        componentId: '',
        channelRefs: [safeChannelId],
      });
    });

    return {
      ...baseChannel,
      chaincodes: sortOverviewRuntimeItems(Array.from(channelChaincodes.values())),
    };
  };

  const explicitGroups = sortOverviewRuntimeItems(businessGroups)
    .map(group => {
      const groupChannelIds = normalizeArrayOfText(group && group.channels);
      const channels = groupChannelIds.map(buildChannelModel).filter(Boolean);

      channels.forEach(channel => {
        assignedChannelIds.add(channel.channelId);
      });

      return {
        key: normalizeText(group && (group.groupId || group.name || group.networkId)),
        groupId: normalizeText(group && group.groupId),
        name:
          normalizeText(group && (group.name || group.groupId || group.networkId)) ||
          'Business Group',
        networkId: normalizeText(group && group.networkId),
        channels,
      };
    })
    .filter(group => group.channels.length > 0 || group.name || group.groupId);

  const unassignedChannels = Array.from(channelLookup.keys())
    .filter(channelId => !assignedChannelIds.has(channelId))
    .map(buildChannelModel)
    .filter(Boolean);

  if (unassignedChannels.length > 0 || explicitGroups.length === 0) {
    explicitGroups.push({
      key: 'business-scope-official',
      groupId: 'business-scope-official',
      name: explicitGroups.length > 0 ? 'Escopo oficial adicional' : 'Escopo oficial',
      networkId: '',
      channels: unassignedChannels,
    });
  }

  return explicitGroups.filter(group => group.channels.length > 0);
};

const normalizePreflightStatus = value => {
  const normalized = normalizeText(value).toLowerCase();
  if (['apto', 'parcial', 'bloqueado'].includes(normalized)) {
    return normalized;
  }
  return 'unknown';
};

const resolveRequestErrorMessage = error => {
  const rawMessage = normalizeText(error && error.message);
  const backendCode = normalizeText(
    error && error.data && (error.data.code || error.data.msg || error.data.message)
  ).toLowerCase();

  if (rawMessage === 'timeout_telemetry_preflight') {
    return pickCognusText(
      'Tempo limite excedido ao consultar a VM para telemetria em tempo real.',
      'Timed out while querying the VM for live telemetry.'
    );
  }
  if (backendCode === 'runbook_machine_credentials_required') {
    return pickCognusText(
      'O backend oficial exige machine_credentials para telemetria ao vivo. Mantendo somente o snapshot auditável desta organização.',
      'The official backend requires machine_credentials for live telemetry. Keeping only the auditable snapshot for this organization.'
    );
  }
  if (backendCode === 'runbook_not_found') {
    return pickCognusText(
      'O backend oficial não localiza mais este run_id para coleta remota. Mantendo somente o snapshot auditável desta organização.',
      'The official backend can no longer find this run_id for remote collection. Keeping only the auditable snapshot for this organization.'
    );
  }

  const backendDetail =
    normalizeText(
      error && error.data && (error.data.detail || error.data.message || error.data.msg)
    ) ||
    normalizeText(error && error.response && error.response.data && error.response.data.detail) ||
    rawMessage;

  return (
    backendDetail ||
    pickCognusText(
      'falha de conectividade com o backend de telemetria.',
      'connectivity failure with the telemetry backend.'
    )
  );
};

const normalizeTelemetryHost = host => {
  const runtimeSnapshot =
    (host &&
      host.runtime_snapshot &&
      typeof host.runtime_snapshot === 'object' &&
      host.runtime_snapshot) ||
    (host &&
      host.runtimeSnapshot &&
      typeof host.runtimeSnapshot === 'object' &&
      host.runtimeSnapshot) ||
    null;

  const activeContainers =
    (runtimeSnapshot
      ? runtimeSnapshot.active_containers || runtimeSnapshot.activeContainers
      : []) || [];

  return {
    hostRef: normalizeText(host && (host.host_ref || host.infra_label || host.host_id)),
    hostAddress: normalizeText(host && host.host_address),
    status: normalizePreflightStatus(host && host.status),
    activeContainers: (Array.isArray(activeContainers) ? activeContainers : [])
      .map(containerName => normalizeText(containerName))
      .filter(Boolean),
    hasRuntimeSnapshot: Boolean(runtimeSnapshot),
  };
};

const runbookTechnicalPreflightWithTimeout = (payload, timeoutMs = 20000) =>
  Promise.race([
    runbookTechnicalPreflight(payload),
    new Promise((_, reject) => {
      setTimeout(() => {
        reject(new Error('timeout_telemetry_preflight'));
      }, timeoutMs);
    }),
  ]);

const buildTelemetryHostMappingPayload = hostTargets => {
  const normalizedTargets = (Array.isArray(hostTargets) ? hostTargets : []).map(target => ({
    hostRef: normalizeText(target.hostRef),
    hostAddress: normalizeText(target.hostAddress),
    sshUser: normalizeText(target.sshUser),
    sshPort: toSafePositiveInt(target.sshPort) || 22,
    dockerPort: toSafePositiveInt(target.dockerPort) || 2376,
  }));

  const deduplicatedTargets = normalizedTargets.reduce(
    (accumulator, target) => {
      const key = normalizeText(target.hostRef || target.hostAddress);
      if (!key) {
        return accumulator;
      }

      if (!accumulator.byKey.has(key)) {
        accumulator.byKey.set(key, target);
      } else {
        const current = accumulator.byKey.get(key);
        accumulator.byKey.set(key, {
          ...current,
          hostAddress: current.hostAddress || target.hostAddress,
          sshUser: current.sshUser || target.sshUser,
          sshPort: current.sshPort || target.sshPort || 22,
          dockerPort: current.dockerPort || target.dockerPort || 2376,
        });
      }

      if (target.sshUser && !accumulator.defaultSshUser) {
        accumulator.defaultSshUser = target.sshUser;
      }

      return accumulator;
    },
    {
      byKey: new Map(),
      defaultSshUser: '',
    }
  );

  const fallbackSshUser = normalizeText(deduplicatedTargets.defaultSshUser);

  return Array.from(deduplicatedTargets.byKey.values())
    .map(target => ({
      host_ref: normalizeText(target.hostRef),
      host_address: normalizeText(target.hostAddress),
      ssh_user: normalizeText(target.sshUser) || fallbackSshUser,
      ssh_port: toSafePositiveInt(target.sshPort) || 22,
      docker_port: toSafePositiveInt(target.dockerPort) || 2376,
      auth_method: 'key',
    }))
    .filter(target => target.host_address && target.ssh_user);
};

const normalizeMachineCredentialEntries = machineCredentials =>
  (Array.isArray(machineCredentials) ? machineCredentials : [])
    .map(entry => ({
      machine_id: normalizeText(entry && entry.machine_id),
      credential_ref: normalizeText(entry && entry.credential_ref),
      credential_payload: normalizeText(entry && entry.credential_payload),
      credential_fingerprint: normalizeText(entry && entry.credential_fingerprint),
      reuse_confirmed: Boolean(entry && entry.reuse_confirmed),
    }))
    .filter(
      entry => entry.machine_id && (entry.credential_ref || entry.credential_fingerprint)
    );

const extractMachineCredentialsFromPayload = payload => {
  const safePayload = payload && typeof payload === 'object' ? payload : {};
  if (
    Array.isArray(safePayload.machine_credentials) &&
    safePayload.machine_credentials.length > 0
  ) {
    return normalizeMachineCredentialEntries(safePayload.machine_credentials);
  }

  const handoffPayload =
    safePayload.handoff_payload && typeof safePayload.handoff_payload === 'object'
      ? safePayload.handoff_payload
      : {};
  if (
    Array.isArray(handoffPayload.machine_credentials) &&
    handoffPayload.machine_credentials.length > 0
  ) {
    return normalizeMachineCredentialEntries(handoffPayload.machine_credentials);
  }

  return [];
};

const resolveOrganizationTelemetryHostRows = organization => {
  const hostTargets = Array.isArray(organization && organization.hostTargets)
    ? organization.hostTargets
    : [];
  const targetRefs = new Set(
    hostTargets.map(target => normalizeText(target && target.hostRef)).filter(Boolean)
  );
  const targetAddresses = new Set(
    hostTargets.map(target => normalizeText(target && target.hostAddress)).filter(Boolean)
  );
  const organizationId = normalizeText(organization && organization.orgId);
  const latestHostMapping = Array.isArray(organization && organization.latestHostMapping)
    ? organization.latestHostMapping
    : [];

  const scopedHostRows = latestHostMapping.filter(entry => {
    const hostRef = normalizeText(entry && entry.hostRef);
    const hostAddress = normalizeText(entry && entry.hostAddress);
    const entryOrgId = normalizeText(entry && entry.orgId);
    return (
      (organizationId && entryOrgId && entryOrgId === organizationId) ||
      (hostRef && targetRefs.has(hostRef)) ||
      (hostAddress && targetAddresses.has(hostAddress))
    );
  });

  return scopedHostRows.length > 0 ? scopedHostRows : hostTargets;
};

const filterMachineCredentialsByHostRows = (machineCredentials, hostRows) => {
  const machineIds = new Set(
    (Array.isArray(hostRows) ? hostRows : [])
      .map(row => normalizeText((row && (row.machine_id || row.machineId || row.hostRef || row.hostAddress)) || ''))
      .filter(Boolean)
  );
  if (machineIds.size === 0) {
    return [];
  }

  return normalizeMachineCredentialEntries(machineCredentials).filter(entry =>
    machineIds.has(normalizeText(entry && entry.machine_id))
  );
};

const buildTelemetryIndex = telemetry => {
  const hosts = telemetry && Array.isArray(telemetry.hosts) ? telemetry.hosts : [];
  const byRef = new Map();
  const byAddress = new Map();

  hosts.forEach(host => {
    if (host.hostRef) {
      byRef.set(host.hostRef, host);
    }
    if (host.hostAddress) {
      byAddress.set(host.hostAddress, host);
    }
  });

  return {
    byRef,
    byAddress,
  };
};

const resolveNodeStatusTone = status => {
  if (status === 'running') {
    return { label: pickCognusText('ativo', 'active'), color: 'green' };
  }
  if (status === 'reachable') {
    return { label: pickCognusText('acessível', 'reachable'), color: 'blue' };
  }
  if (status === 'stopped') {
    return { label: pickCognusText('parado', 'stopped'), color: 'default' };
  }
  if (status === 'host_blocked') {
    return { label: pickCognusText('host bloqueado', 'host blocked'), color: 'red' };
  }
  return { label: pickCognusText('indisponível', 'unavailable'), color: 'orange' };
};

const resolveHostByNode = (node, telemetryIndex) => {
  if (!node) {
    return null;
  }

  if (node.hostRef && telemetryIndex.byRef.has(node.hostRef)) {
    return telemetryIndex.byRef.get(node.hostRef);
  }

  if (node.hostAddress && telemetryIndex.byAddress.has(node.hostAddress)) {
    return telemetryIndex.byAddress.get(node.hostAddress);
  }

  return null;
};

const containerMatchesNode = (containerName, nodeId) => {
  const normalizedContainerName = normalizeToken(containerName);
  const normalizedNodeId = normalizeToken(nodeId);
  if (!normalizedContainerName || !normalizedNodeId) {
    return false;
  }
  return normalizedContainerName.includes(normalizedNodeId);
};

const resolveNodeRoleToken = nodeType => {
  const normalizedType = normalizeText(nodeType).toLowerCase();
  if (normalizedType === 'peer') {
    return 'peer';
  }
  if (normalizedType === 'orderer') {
    return 'orderer';
  }
  if (normalizedType === 'ca') {
    return 'ca';
  }
  return '';
};

const buildNodeMatchTokens = node => {
  const normalizedNodeId = normalizeToken(node && node.nodeId);
  const roleToken = resolveNodeRoleToken(node && node.nodeType);
  const rawNodeId = normalizeText(node && node.nodeId).toLowerCase();
  const tokens = new Set();

  if (normalizedNodeId) {
    tokens.add(normalizedNodeId);
  }

  if (roleToken === 'peer') {
    const peerMatch = rawNodeId.match(/peer\d+/);
    if (peerMatch && peerMatch[0]) {
      tokens.add(normalizeToken(peerMatch[0]));
    }
  }

  if (roleToken === 'orderer') {
    const ordererMatch = rawNodeId.match(/orderer\d+/);
    if (ordererMatch && ordererMatch[0]) {
      tokens.add(normalizeToken(ordererMatch[0]));
    }
  }

  if (roleToken === 'ca') {
    tokens.add('ca');
    const caMatch = rawNodeId.match(/ca[-_]?[a-z0-9]*/);
    if (caMatch && caMatch[0]) {
      tokens.add(normalizeToken(caMatch[0]));
    }
  }

  return {
    roleToken,
    tokens: Array.from(tokens).filter(Boolean),
  };
};

const containerMatchesNodeHeuristics = (containerName, node) => {
  const normalizedContainerName = normalizeToken(containerName);
  if (!normalizedContainerName) {
    return false;
  }

  const { roleToken, tokens } = buildNodeMatchTokens(node);
  if (tokens.some(token => normalizedContainerName.includes(token))) {
    return true;
  }

  if (roleToken && normalizedContainerName.includes(roleToken)) {
    return true;
  }

  return false;
};

const buildRuntimeNode = (node, telemetryIndex) => {
  const host = resolveHostByNode(node, telemetryIndex);
  if (!host) {
    return {
      ...node,
      runtimeStatus: 'unknown',
    };
  }

  if (host.status === 'bloqueado') {
    return {
      ...node,
      runtimeStatus: 'host_blocked',
      hostStatus: host.status,
    };
  }

  const hasMatchingContainerByExactId = host.activeContainers.some(containerName =>
    containerMatchesNode(containerName, node.nodeId)
  );
  const hasMatchingContainerByHeuristic = host.activeContainers.some(containerName =>
    containerMatchesNodeHeuristics(containerName, node)
  );
  const hasMatchingContainer = hasMatchingContainerByExactId || hasMatchingContainerByHeuristic;

  if (hasMatchingContainer) {
    return {
      ...node,
      runtimeStatus: 'running',
      hostStatus: host.status,
    };
  }

  if (host.hasRuntimeSnapshot) {
    if (host.status === 'apto' || host.status === 'parcial') {
      return {
        ...node,
        runtimeStatus: 'reachable',
        hostStatus: host.status,
      };
    }
    return {
      ...node,
      runtimeStatus: 'stopped',
      hostStatus: host.status,
    };
  }

  return {
    ...node,
    runtimeStatus: host.status === 'apto' || host.status === 'parcial' ? 'reachable' : 'unknown',
    hostStatus: host.status,
  };
};

const resolveOrganizationPrimaryHostAddress = (organization, telemetryIndex) => {
  const networkApi = (organization && organization.networkApi) || {};
  const apis = safeArray(organization && organization.apis);
  const networkApiAddress =
    normalizeText(networkApi.exposureIp) ||
    normalizeText(networkApi.host) ||
    normalizeText(apis[0] && apis[0].exposureHost);
  if (networkApiAddress) {
    return networkApiAddress;
  }

  const hosts = Array.isArray(organization.hostTargets) ? organization.hostTargets : [];
  const hostAddressFromTopology = normalizeText(hosts[0] && hosts[0].hostAddress);
  if (hostAddressFromTopology) {
    return hostAddressFromTopology;
  }

  const telemetryHost =
    hosts
      .map(target => {
        const hostRef = normalizeText(target && target.hostRef);
        const hostAddress = normalizeText(target && target.hostAddress);
        if (hostRef && telemetryIndex.byRef.has(hostRef)) {
          return telemetryIndex.byRef.get(hostRef);
        }
        if (hostAddress && telemetryIndex.byAddress.has(hostAddress)) {
          return telemetryIndex.byAddress.get(hostAddress);
        }
        return null;
      })
      .find(Boolean) || null;

  return normalizeText(telemetryHost && telemetryHost.hostAddress);
};

const formatOverviewTimestamp = (value, locale) => {
  const safeValue = normalizeText(value);
  if (!safeValue) {
    return '';
  }

  const parsedDate = new Date(safeValue);
  if (Number.isNaN(parsedDate.getTime())) {
    return safeValue;
  }

  return formatCognusDateTime(parsedDate, locale, {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
};

const buildOverviewRuntimeSummary = (organization, telemetry) => {
  const telemetryIndex = buildTelemetryIndex(telemetry);
  const peers = safeArray(organization && organization.peers);
  const orderers = safeArray(organization && organization.orderers);
  const runtimePeers = peers.map(node => buildRuntimeNode(node, telemetryIndex));
  const runtimeOrderers = orderers.map(node => buildRuntimeNode(node, telemetryIndex));
  const telemetryReady = Boolean(telemetry && telemetry.status === 'ready');

  return {
    telemetryReady,
    peerTotalCount: peers.length,
    peerActiveCount: runtimePeers.filter(node => node.runtimeStatus === 'running').length,
    ordererTotalCount: orderers.length,
    ordererActiveCount: runtimeOrderers.filter(node => node.runtimeStatus === 'running').length,
  };
};

const buildOverviewRecentActivities = organizations => {
  const activityFeed = safeArray(organizations)
    .flatMap(organization =>
      safeArray(organization && organization.runHistory)
        .slice(0, 4)
        .map(entry => ({
          key: normalizeText(entry && entry.key) || `${organization.key}-${entry.runId}`,
          organizationName:
            normalizeText(organization && organization.orgName) ||
            normalizeText(organization && organization.orgId) ||
            pickCognusText('Organizacao', 'Organization'),
          title: normalizeText(entry && entry.changeId)
            ? pickCognusText(
                `Mudança ${normalizeText(entry && entry.changeId)}`,
                `Change ${normalizeText(entry && entry.changeId)}`
              )
            : `Run ${normalizeText(entry && entry.runId) || '-'}`,
          status: normalizeText(entry && entry.status).toLowerCase() || 'partial',
          timestamp: normalizeText(entry && (entry.finishedAt || entry.capturedAt)),
        }))
    )
    .sort(
      (left, right) =>
        Date.parse(right.timestamp || '') - Date.parse(left.timestamp || '')
    );

  return activityFeed.slice(0, 5);
};

const resolveActivityBulletToneClassName = (stylesRef, toneColor) => {
  if (toneColor === 'green') {
    return stylesRef.activityBulletSuccess;
  }
  if (toneColor === 'red') {
    return stylesRef.activityBulletDanger;
  }
  return stylesRef.activityBulletInfo;
};

const resolveTelemetryLiveLabel = telemetryEntry => {
  if (!telemetryEntry) {
    return '';
  }
  if (telemetryEntry.status === 'ready') {
    return pickCognusText('Telemetria ao vivo', 'Live telemetry');
  }
  if (telemetryEntry.status === 'loading') {
    return pickCognusText('Telemetria sincronizando', 'Telemetry syncing');
  }
  if (telemetryEntry.status === 'audit_only') {
    return pickCognusText('Somente auditoria', 'Audit only');
  }
  if (telemetryEntry.status === 'error') {
    return pickCognusText('Telemetria indisponível', 'Telemetry unavailable');
  }
  return '';
};

const buildNodePreviewEntries = ({ officialItems = [], runtimeItems = [] } = {}) => {
  if (officialItems.length > 0) {
    return sortOverviewRuntimeItems(officialItems)
      .slice(0, 3)
      .map(item => ({
        key: item.key,
        label: item.componentName || item.componentId || item.componentTypeLabel || 'componente',
        meta: [item.hostRef, item.channelId].filter(Boolean).join(' | '),
        toneColor: resolveRuntimeStatusTagColor(item.status),
        toneLabel: item.status || 'unknown',
      }));
  }

  return safeArray(runtimeItems)
    .slice(0, 3)
    .map(item => {
      const tone = resolveNodeStatusTone(item.runtimeStatus);
      return {
        key: item.nodeId || item.hostRef || item.hostAddress || 'node',
        label: item.nodeId || 'node',
        meta: [item.hostRef, item.hostAddress].filter(Boolean).join(' | '),
        toneColor: tone.color,
        toneLabel: tone.label,
      };
    });
};

const Overview = () => {
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale),
    [locale]
  );
  const [provisioningAuditHistory, setProvisioningAuditHistory] = useState([]);
  const [activeOrganizationKey, setActiveOrganizationKey] = useState('');
  const [workspaceWindows, setWorkspaceWindows] = useState(createEmptyWorkspaceWindowState);
  const [activeGovernedAction, setActiveGovernedAction] = useState('');
  const [overviewFilters, setOverviewFilters] = useState(OVERVIEW_EMPTY_CARD_FILTERS);
  const [telemetryByOrganization, setTelemetryByOrganization] = useState({});
  const [isRefreshingTelemetry, setIsRefreshingTelemetry] = useState(false);
  const [officialStateByOrganization, setOfficialStateByOrganization] = useState({});
  const [organizationVmAccessRegistry, setOrganizationVmAccessRegistry] = useState(
    readOrganizationVmAccessRegistry
  );
  const [vmAccessWindowOpen, setVmAccessWindowOpen] = useState(false);
  const [vmAccessDraft, setVmAccessDraft] = useState({
    machineEntries: [],
  });
  const workspaceOperationalNoticeRef = useRef('');
  const workspaceTelemetryNoticeRef = useRef('');

  useEffect(() => {
    persistOrganizationVmAccessRegistry(organizationVmAccessRegistry);
  }, [organizationVmAccessRegistry]);

  const organizationsDashboardProjection = useMemo(
    () => buildOrganizationsDashboardProjection(provisioningAuditHistory),
    [provisioningAuditHistory]
  );

  const activeOrganization = useMemo(
    () => organizationsDashboardProjection.find(org => org.key === activeOrganizationKey) || null,
    [activeOrganizationKey, organizationsDashboardProjection]
  );
  const activeOrganizationOfficialState = activeOrganization
    ? officialStateByOrganization[activeOrganization.key] || EMPTY_OFFICIAL_ORGANIZATION_STATE
    : EMPTY_OFFICIAL_ORGANIZATION_STATE;
  const activeOrganizationOfficialRun = activeOrganizationOfficialState.run || null;
  const activeOrganizationOfficialTopologyRows = useMemo(
    () => buildOrganizationRuntimeTopologyRows(activeOrganizationOfficialRun),
    [activeOrganizationOfficialRun]
  );
  const activeOrganizationWorkspaces = useMemo(
    () => buildOrganizationWorkspaceReadModels(activeOrganizationOfficialRun),
    [activeOrganizationOfficialRun]
  );
  const activeOrganizationVmAccessTargets = useMemo(() => {
    if (!activeOrganization) {
      return [];
    }

    const officialMachineCredentials = extractMachineCredentialsFromPayload(activeOrganizationOfficialRun);
    return buildOrganizationVmAccessTargets({
      organization: activeOrganization,
      machineCredentials:
        officialMachineCredentials.length > 0
          ? officialMachineCredentials
          : activeOrganization.latestMachineCredentials,
    });
  }, [activeOrganization, activeOrganizationOfficialRun]);
  const activeOrganizationOfficialWorkspace = useMemo(() => {
    if (!activeOrganization) {
      return null;
    }
    return (
      activeOrganizationWorkspaces.find(
        workspace =>
          workspace &&
          workspace.organization &&
          workspace.organization.orgId === activeOrganization.orgId
      ) || null
    );
  }, [activeOrganization, activeOrganizationWorkspaces]);
  const {
    inspectionState,
    inspectionScopeEntries,
    openInspectionByComponentId,
    openInspectionFromSnapshot,
    refreshInspection,
    closeInspection,
  } = useOfficialRuntimeInspection({
    runId: activeOrganization && activeOrganization.latestRunId,
    topologyRows: activeOrganizationOfficialTopologyRows,
  });
  const activeOrganizationFallbackMeta = useMemo(
    () => resolveCachedStatusFallback(activeOrganizationOfficialRun),
    [activeOrganizationOfficialRun]
  );
  const activeOrganizationTopologyDomains = useMemo(
    () => buildOverviewTopologyDomains(activeOrganizationOfficialTopologyRows, locale),
    [activeOrganizationOfficialTopologyRows, locale]
  );
  const activeOrganizationOfficialChannels = useMemo(
    () =>
      (activeOrganizationOfficialWorkspace &&
        activeOrganizationOfficialWorkspace.workspace &&
        activeOrganizationOfficialWorkspace.workspace.projections &&
        activeOrganizationOfficialWorkspace.workspace.projections.channels) ||
      [],
    [activeOrganizationOfficialWorkspace]
  );
  const activeOrganizationOfficialChaincodes = useMemo(
    () =>
      (activeOrganizationOfficialWorkspace &&
        activeOrganizationOfficialWorkspace.workspace &&
        activeOrganizationOfficialWorkspace.workspace.projections &&
        activeOrganizationOfficialWorkspace.workspace.projections.chaincodes) ||
      [],
    [activeOrganizationOfficialWorkspace]
  );
  const activeOrganizationOfficialPeers = useMemo(
    () => selectOverviewTopologyItems(activeOrganizationOfficialTopologyRows, ['peer']),
    [activeOrganizationOfficialTopologyRows]
  );
  const activeOrganizationOfficialOrderers = useMemo(
    () => selectOverviewTopologyItems(activeOrganizationOfficialTopologyRows, ['orderer']),
    [activeOrganizationOfficialTopologyRows]
  );
  const activeOrganizationOfficialControlPlane = useMemo(
    () =>
      selectOverviewTopologyItems(activeOrganizationOfficialTopologyRows, [
        'ca',
        'apiGateway',
        'netapi',
      ]),
    [activeOrganizationOfficialTopologyRows]
  );
  const activeOrganizationOfficialSupportServices = useMemo(
    () =>
      selectOverviewTopologyItems(activeOrganizationOfficialTopologyRows, [
        'couch',
        'chaincode_runtime',
      ]),
    [activeOrganizationOfficialTopologyRows]
  );
  const activeOrganizationOfficialServices = useMemo(
    () =>
      sortOverviewRuntimeItems([
        ...activeOrganizationOfficialControlPlane,
        ...activeOrganizationOfficialSupportServices,
      ]),
    [activeOrganizationOfficialControlPlane, activeOrganizationOfficialSupportServices]
  );
  const activeOrganizationArtifactOrigins = useMemo(
    () =>
      (activeOrganizationOfficialWorkspace && activeOrganizationOfficialWorkspace.artifactOrigins) ||
      [],
    [activeOrganizationOfficialWorkspace]
  );
  const activeOrganizationAvailableArtifactsCount = useMemo(
    () => activeOrganizationArtifactOrigins.filter(origin => origin && origin.available).length,
    [activeOrganizationArtifactOrigins]
  );
  const organizationOperationalCards = useMemo(
    () =>
      organizationsDashboardProjection.map(organization =>
        buildOverviewOperationalCard({
          organization,
          officialState:
            officialStateByOrganization[organization.key] || EMPTY_OFFICIAL_ORGANIZATION_STATE,
        })
      ),
    [officialStateByOrganization, organizationsDashboardProjection]
  );
  const overviewFilterOptions = useMemo(
    () => buildOverviewOperationalFilterOptions(organizationOperationalCards),
    [organizationOperationalCards]
  );
  const filteredOrganizationOperationalCards = useMemo(
    () => filterOverviewOperationalCards(organizationOperationalCards, overviewFilters),
    [organizationOperationalCards, overviewFilters]
  );
  const activeOrganizationOperationalCard = useMemo(
    () => organizationOperationalCards.find(card => card.key === activeOrganizationKey) || null,
    [activeOrganizationKey, organizationOperationalCards]
  );
  const activeOrganizationEnvironments = useMemo(
    () => normalizeArrayOfText(activeOrganization && activeOrganization.environments),
    [activeOrganization]
  );
  const activeOrganizationProviders = useMemo(
    () => normalizeArrayOfText(activeOrganization && activeOrganization.providers),
    [activeOrganization]
  );
  const activeOrganizationRunHistory = useMemo(
    () => safeArray(activeOrganization && activeOrganization.runHistory),
    [activeOrganization]
  );
  const activeOrganizationBusinessGroups = useMemo(
    () => safeArray(activeOrganization && activeOrganization.businessGroups),
    [activeOrganization]
  );
  const activeOrganizationBusinessScopeGroups = useMemo(
    () =>
      buildBusinessScopeGroups({
        businessGroups: activeOrganizationBusinessGroups,
        officialChannels: activeOrganizationOfficialChannels,
        officialChaincodes: activeOrganizationOfficialChaincodes,
        fallbackChannels: activeOrganization && activeOrganization.channels,
      }),
    [
      activeOrganization,
      activeOrganizationBusinessGroups,
      activeOrganizationOfficialChaincodes,
      activeOrganizationOfficialChannels,
    ]
  );
  const activeOrganizationProposalEntries = useMemo(
    () =>
      activeOrganizationOperationalCard
        ? activeOrganizationOperationalCard.proposals
        : activeOrganizationRunHistory.slice(0, 8),
    [activeOrganizationOperationalCard, activeOrganizationRunHistory]
  );
  const activeGovernedActionConfig = activeGovernedAction
    ? resolveOverviewGovernedActionConfig(activeGovernedAction)
    : null;
  const activeOrganizationPeers = useMemo(
    () => safeArray(activeOrganization && activeOrganization.peers),
    [activeOrganization]
  );
  const activeOrganizationOrderers = useMemo(
    () => safeArray(activeOrganization && activeOrganization.orderers),
    [activeOrganization]
  );
  const activeOrganizationVmAccessGrant = useMemo(
    () =>
      activeOrganization
        ? normalizeOrganizationVmAccessEntry(
            organizationVmAccessRegistry[normalizeText(activeOrganization.key).toLowerCase()]
          )
        : normalizeOrganizationVmAccessEntry(null),
    [activeOrganization, organizationVmAccessRegistry]
  );
  const activeOrganizationVmAccessGranted = Boolean(
    normalizeText(activeOrganizationVmAccessGrant.grantedAtUtc)
  );
  const overviewCounts = useMemo(
    () => ({
      total: organizationOperationalCards.length,
      implemented: organizationOperationalCards.filter(
        card => card.operationalStatus === 'implemented'
      ).length,
      partial: organizationOperationalCards.filter(card => card.operationalStatus === 'partial')
        .length,
      blocked: organizationOperationalCards.filter(card => card.operationalStatus === 'blocked')
        .length,
    }),
    [organizationOperationalCards]
  );
  const organizationRuntimeSummaryByKey = useMemo(
    () =>
      organizationsDashboardProjection.reduce((accumulator, organization) => {
        accumulator[organization.key] = buildOverviewRuntimeSummary(
          organization,
          telemetryByOrganization[organization.key] || null
        );
        return accumulator;
      }, {}),
    [organizationsDashboardProjection, telemetryByOrganization]
  );
  const organizationSnapshotByKey = useMemo(
    () =>
      organizationsDashboardProjection.reduce((accumulator, organization) => {
        const officialState =
          officialStateByOrganization[organization.key] || EMPTY_OFFICIAL_ORGANIZATION_STATE;
        const officialRun = officialState.run || null;
        const officialTopologyRows = buildOrganizationRuntimeTopologyRows(officialRun);
        const officialWorkspaces = buildOrganizationWorkspaceReadModels(officialRun);
        const officialWorkspace =
          officialWorkspaces.find(
            workspace =>
              workspace &&
              workspace.organization &&
              workspace.organization.orgId === organization.orgId
          ) || null;
        const telemetryEntry = telemetryByOrganization[organization.key] || null;
        const telemetryIndex = buildTelemetryIndex(telemetryEntry);
        const runtimePeers = safeArray(organization.peers).map(node =>
          buildRuntimeNode(node, telemetryIndex)
        );
        const runtimeOrderers = safeArray(organization.orderers).map(node =>
          buildRuntimeNode(node, telemetryIndex)
        );
        const officialPeers = selectOverviewTopologyItems(officialTopologyRows, ['peer']);
        const officialOrderers = selectOverviewTopologyItems(officialTopologyRows, ['orderer']);
        const officialChannels =
          (officialWorkspace &&
            officialWorkspace.workspace &&
            officialWorkspace.workspace.projections &&
            officialWorkspace.workspace.projections.channels) ||
          [];
        const officialChaincodes =
          (officialWorkspace &&
            officialWorkspace.workspace &&
            officialWorkspace.workspace.projections &&
            officialWorkspace.workspace.projections.chaincodes) ||
          [];

        accumulator[organization.key] = {
          fallbackMeta: resolveCachedStatusFallback(officialRun),
          peerPreviewEntries: buildNodePreviewEntries({
            officialItems: officialPeers,
            runtimeItems: runtimePeers,
          }),
          ordererPreviewEntries: buildNodePreviewEntries({
            officialItems: officialOrderers,
            runtimeItems: runtimeOrderers,
          }),
          peersCount: officialPeers.length || runtimePeers.length || organization.peerCount,
          orderersCount:
            officialOrderers.length || runtimeOrderers.length || organization.ordererCount,
          channelsCount: officialChannels.length || safeArray(organization.channels).length,
          chaincodesCount:
            officialChaincodes.length || safeArray(organization.chaincodes).length,
          primaryHostAddress: resolveOrganizationPrimaryHostAddress(organization, telemetryIndex),
        };
        return accumulator;
      }, {}),
    [organizationsDashboardProjection, officialStateByOrganization, telemetryByOrganization]
  );
  const overviewFreshCount = useMemo(
    () =>
      organizationOperationalCards.filter(card => card.freshnessStatus === 'fresh').length,
    [organizationOperationalCards]
  );
  const overviewPendingAlertsTotal = useMemo(
    () =>
      organizationOperationalCards.reduce(
        (accumulator, card) => accumulator + toSafePositiveInt(card.pendingAlertsCount),
        0
      ),
    [organizationOperationalCards]
  );
  const overviewLiveTelemetryCount = useMemo(
    () => Object.values(telemetryByOrganization).filter(entry => entry && entry.status === 'ready').length,
    [telemetryByOrganization]
  );
  const overviewSnapshotAuditOnlyCount = useMemo(
    () =>
      Object.values(organizationSnapshotByKey).filter(
        snapshot => snapshot && snapshot.fallbackMeta
      ).length,
    [organizationSnapshotByKey]
  );
  const overviewChannelsTotal = useMemo(
    () =>
      Object.values(organizationSnapshotByKey).reduce(
        (accumulator, snapshot) => accumulator + toSafePositiveInt(snapshot && snapshot.channelsCount),
        0
      ),
    [organizationSnapshotByKey]
  );
  const overviewChaincodesTotal = useMemo(
    () =>
      Object.values(organizationSnapshotByKey).reduce(
        (accumulator, snapshot) =>
          accumulator + toSafePositiveInt(snapshot && snapshot.chaincodesCount),
        0
      ),
    [organizationSnapshotByKey]
  );
  const overviewLastUpdatedLabel = useMemo(() => {
    const timestamps = organizationOperationalCards
      .map(card => card.officialWorkspaceObservedAt)
      .concat(organizationsDashboardProjection.map(organization => organization.latestFinishedAt))
      .filter(Boolean)
      .sort((left, right) => Date.parse(right || '') - Date.parse(left || ''));
    return formatOverviewTimestamp(timestamps[0], locale);
  }, [locale, organizationOperationalCards, organizationsDashboardProjection]);
  const overviewRecentActivities = useMemo(
    () => buildOverviewRecentActivities(organizationsDashboardProjection),
    [organizationsDashboardProjection]
  );
  const isRefreshingOverview = useMemo(
    () => Object.values(officialStateByOrganization).some(state => state && state.loading),
    [officialStateByOrganization]
  );

  const loadProvisioningAuditHistory = useCallback(async () => {
    const nextHistory = readProvisioningAuditHistory();
    if (nextHistory.length > 0) {
      setProvisioningAuditHistory(nextHistory);
      return nextHistory;
    }

    try {
      const recoveredHistory = await getRunbookCatalog();
      if (Array.isArray(recoveredHistory) && recoveredHistory.length > 0) {
        writeProvisioningAuditHistory(recoveredHistory);
        setProvisioningAuditHistory(recoveredHistory);
        return recoveredHistory;
      }
    } catch (error) {
      // Recovery is best-effort. Keep empty state if backend catalog is unavailable.
    }

    setProvisioningAuditHistory([]);
    return [];
  }, []);

  useEffect(() => {
    loadProvisioningAuditHistory().catch(() => {});
  }, [loadProvisioningAuditHistory]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }

    const refreshHistoryOnFocus = () => {
      loadProvisioningAuditHistory().catch(() => {});
    };

    window.addEventListener('focus', refreshHistoryOnFocus);
    return () => {
      window.removeEventListener('focus', refreshHistoryOnFocus);
    };
  }, [loadProvisioningAuditHistory]);

  const handleOpenRunbookAudit = runHistoryEntry => {
    if (!runHistoryEntry || !runHistoryEntry.runId) {
      return;
    }

    const sessionStorage = getBrowserSessionStorage();
    if (sessionStorage) {
      sessionStorage.setItem(
        RUNBOOK_AUDIT_SELECTED_STORAGE_KEY,
        JSON.stringify({
          key: runHistoryEntry.key,
          runId: runHistoryEntry.runId,
          capturedAt: runHistoryEntry.capturedAt,
        })
      );
    }

    history.push(RUNBOOK_ROUTE_PATH);
  };

  const refreshTelemetryForOrganizations = useCallback(async organizations => {
    const targets = Array.isArray(organizations) ? organizations : [];
    if (targets.length === 0) {
      setTelemetryByOrganization({});
      return;
    }

    setIsRefreshingTelemetry(true);

    await Promise.all(
      targets.map(async organization => {
        const officialState = officialStateByOrganization[organization.key] || EMPTY_OFFICIAL_ORGANIZATION_STATE;
        const fallbackMeta = resolveCachedStatusFallback(officialState.run);
        const sessionVmAccessGrant = normalizeOrganizationVmAccessEntry(
          organizationVmAccessRegistry[normalizeText(organization.key).toLowerCase()]
        );
        const telemetryHostRows =
          sessionVmAccessGrant.validatedHostMapping.length > 0
            ? sessionVmAccessGrant.validatedHostMapping
            : resolveOrganizationTelemetryHostRows(organization);
        const machineCredentials =
          sessionVmAccessGrant.validatedMachineCredentials.length > 0
            ? sessionVmAccessGrant.validatedMachineCredentials
            : filterMachineCredentialsByHostRows(
                extractMachineCredentialsFromPayload(officialState.run).length > 0
                  ? extractMachineCredentialsFromPayload(officialState.run)
                  : organization.latestMachineCredentials,
                telemetryHostRows
              );

        if (fallbackMeta) {
          setTelemetryByOrganization(current => ({
            ...current,
            [organization.key]: {
              status: 'audit_only',
              message:
                'Snapshot auditável disponível localmente. Telemetria ao vivo foi desativada porque o backend oficial não mantém mais este run_id.',
              updatedAt: fallbackMeta.savedAtUtc || new Date().toISOString(),
              hosts: [],
            },
          }));
          return;
        }

        const hostMappingPayload = buildTelemetryHostMappingPayload(telemetryHostRows);

        if (hostMappingPayload.length === 0) {
          setTelemetryByOrganization(current => ({
            ...current,
            [organization.key]: {
              status: 'unavailable',
              message:
                'Sem dados mínimos de conexão SSH para telemetria em tempo real desta organização.',
              updatedAt: new Date().toISOString(),
              hosts: [],
            },
          }));
          return;
        }

        if (machineCredentials.length === 0) {
          setTelemetryByOrganization(current => ({
            ...current,
            [organization.key]: {
              status: 'audit_only',
              message:
                'Run oficial sem credenciais SSH vinculadas por maquina. Exibindo somente o snapshot auditavel desta organizacao.',
              updatedAt: new Date().toISOString(),
              hosts: [],
            },
          }));
          return;
        }

        setTelemetryByOrganization(current => ({
          ...current,
          [organization.key]: {
            ...(current[organization.key] || {}),
            status: 'loading',
            message: '',
          },
        }));

        try {
          const preflightReport = await runbookTechnicalPreflightWithTimeout({
            change_id: organization.latestChangeId || organization.latestRunId || 'cr-telemetry',
            host_mapping: hostMappingPayload,
            machine_credentials: machineCredentials,
          });

          const normalizedHosts = (Array.isArray(preflightReport && preflightReport.hosts)
            ? preflightReport.hosts
            : []
          ).map(normalizeTelemetryHost);

          setTelemetryByOrganization(current => ({
            ...current,
            [organization.key]: {
              status: 'ready',
              message: '',
              updatedAt: new Date().toISOString(),
              hosts: normalizedHosts,
            },
          }));
        } catch (error) {
          const backendCode = normalizeText(
            error && error.data && (error.data.code || error.data.msg || error.data.message)
          ).toLowerCase();
          setTelemetryByOrganization(current => ({
            ...current,
            [organization.key]: {
              status:
                backendCode === 'runbook_machine_credentials_required' ||
                backendCode === 'runbook_not_found'
                  ? 'audit_only'
                  : 'error',
              message: resolveRequestErrorMessage(error),
              updatedAt: new Date().toISOString(),
              hosts: [],
            },
          }));
        }
      })
    );

    setIsRefreshingTelemetry(false);
  }, [officialStateByOrganization, organizationVmAccessRegistry]);

  useEffect(() => {
    if (
      !activeOrganization ||
      activeOrganizationOfficialState.loading ||
      !activeOrganizationVmAccessGranted
    ) {
      return;
    }
    refreshTelemetryForOrganizations([activeOrganization]);
  }, [
    activeOrganization,
    activeOrganizationOfficialState.loading,
    activeOrganizationVmAccessGranted,
    refreshTelemetryForOrganizations,
  ]);

  useEffect(() => {
    let cancelled = false;

    if (organizationsDashboardProjection.length === 0) {
      setOfficialStateByOrganization({});
      return undefined;
    }

    setOfficialStateByOrganization(currentState => {
      const nextState = {};
      organizationsDashboardProjection.forEach(organization => {
        if (!organization.latestRunId) {
          nextState[organization.key] = EMPTY_OFFICIAL_ORGANIZATION_STATE;
          return;
        }
        nextState[organization.key] = {
          ...(currentState[organization.key] || EMPTY_OFFICIAL_ORGANIZATION_STATE),
          loading: true,
          error: '',
        };
      });
      return nextState;
    });

    Promise.all(
      organizationsDashboardProjection.map(async organization => {
        if (!organization.latestRunId) {
          return [organization.key, EMPTY_OFFICIAL_ORGANIZATION_STATE];
        }

        try {
          const response = await getRunbookStatus(organization.latestRunId);
          const resolvedRun = response && response.run ? response.run : null;
          const evidenceMeta = buildOfficialRunEvidenceMeta(resolvedRun);
          if (resolvedRun && !resolveCachedStatusFallback(resolvedRun)) {
            prewarmRunbookRuntimeInspectionCache(
              organization.latestRunId,
              buildOrganizationRuntimeTopologyRows(resolvedRun)
            ).catch(() => {});
          }
          return [
            organization.key,
            {
              loading: false,
              run: resolvedRun,
              gate: evidenceMeta.a2aEntryGate || null,
              error: '',
            },
          ];
        } catch (error) {
          return [
            organization.key,
            {
              loading: false,
              run: null,
              gate: null,
              error:
                (error && error.message) ||
                t(
                  'Falha ao consultar o gate oficial de entrada do A2A para esta organizacao.',
                  'Failed to query the official A2A entry gate for this organization.'
                ),
            },
          ];
        }
      })
    ).then(entries => {
      if (cancelled) {
        return;
      }

      setOfficialStateByOrganization(
        entries.reduce((accumulator, [key, value]) => {
          accumulator[key] = value;
          return accumulator;
        }, {})
      );
    });

    return () => {
      cancelled = true;
    };
  }, [organizationsDashboardProjection]);

  const handleRefreshPanel = async () => {
    loadProvisioningAuditHistory();
    if (activeOrganization && activeOrganizationVmAccessGranted) {
      await refreshTelemetryForOrganizations([activeOrganization]);
    }
  };

  const handleOpenOrganizationCard = (organizationKey, drawerSection = 'overview') => {
    setActiveOrganizationKey(organizationKey);
    setWorkspaceWindows({
      ...createEmptyWorkspaceWindowState(),
      audit: drawerSection === 'proposals',
    });
    setActiveGovernedAction('');
  };

  const handleOpenVmAccessWindow = () => {
    setVmAccessDraft({
      machineEntries: activeOrganizationVmAccessTargets.map(createVmAccessDraftEntry),
    });
    setVmAccessWindowOpen(true);
  };

  const handleChangeVmAccessDraftEntry = (machineKey, patch) => {
    setVmAccessDraft(current => ({
      ...current,
      machineEntries: safeArray(current.machineEntries).map(entry =>
        normalizeText(entry && entry.key) === normalizeText(machineKey)
          ? {
              ...entry,
              ...patch,
            }
          : entry
      ),
    }));
  };

  const handleSelectVmAccessPem = async (machineKey, info) => {
    const selection = resolvePemUploadSelection(info);
    if (!selection) {
      message.error(t('Nao foi possivel ler o arquivo .pem selecionado.', 'Could not read the selected .pem file.'));
      return;
    }

    const { fileName, rawFile } = selection;
    if (!/\.pem$/i.test(fileName)) {
      message.error(t('Formato invalido: selecione um arquivo .pem.', 'Invalid format: select a .pem file.'));
      return;
    }

    try {
      const base64Payload = await fileToBase64(rawFile);
      const content =
        typeof rawFile.text === 'function' ? await rawFile.text() : await fileToText(rawFile);
      const fingerprintHex = await computeFingerprintHex(content, base64Payload);

      handleChangeVmAccessDraftEntry(machineKey, {
        pemFileName: fileName,
        pemPayload: base64Payload,
        pemFingerprint: fingerprintHex,
      });
    } catch (error) {
      message.error(t('Falha ao ler o arquivo .pem local.', 'Failed to read the local .pem file.'));
    }
  };

  const handleGrantVmAccess = () => {
    if (!activeOrganization) {
      return;
    }

    const validation = validateVmAccessDraftEntries({
      targetEntries: activeOrganizationVmAccessTargets,
      draftEntries: vmAccessDraft.machineEntries,
    });
    if (!validation.ok) {
      message.warning(validation.message);
      return;
    }

    const sessionGrant = buildVmAccessGrantFromDraftEntries({
      targetEntries: activeOrganizationVmAccessTargets,
      draftEntries: vmAccessDraft.machineEntries,
    });

    setOrganizationVmAccessRegistry(current => ({
      ...current,
      [normalizeText(activeOrganization.key).toLowerCase()]: {
        grantedAtUtc: new Date().toISOString(),
        validatedMachineCount: validation.validatedMachineCount,
        validatedHostMapping: sessionGrant.validatedHostMapping,
        validatedMachineCredentials: sessionGrant.validatedMachineCredentials,
      },
    }));
    setVmAccessWindowOpen(false);
    message.success(
      t(
        'Credenciais validadas com sucesso. O acesso às VMs foi liberado nesta sessao.',
        'Credentials validated successfully. VM access has been released in this session.'
      )
    );
  };

  const handleRevokeVmAccess = () => {
    if (!activeOrganization) {
      return;
    }

    setOrganizationVmAccessRegistry(current => {
      const nextState = { ...current };
      delete nextState[normalizeText(activeOrganization.key).toLowerCase()];
      return nextState;
    });
    message.info(
      t(
        'Acesso às VMs removido desta sessão para a organização selecionada.',
        'VM access removed from this session for the selected organization.'
      )
    );
    closeInspection();
  };

  const handleOrganizationCardKeyDown = (event, organizationKey) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      handleOpenOrganizationCard(organizationKey);
    }
  };

  const handleOpenWorkspaceWindow = windowKey => {
    setWorkspaceWindows(current => ({
      ...current,
      [windowKey]: true,
    }));
  };

  const handleCloseWorkspaceWindow = windowKey => {
    setWorkspaceWindows(current => ({
      ...current,
      [windowKey]: false,
    }));
  };

  const handleOpenGovernedAction = actionKey => {
    setActiveGovernedAction(actionKey);
  };

  const handleOpenOfficialInspection = item => {
    if (!activeOrganizationVmAccessGranted) {
      message.warning(
        t(
          'Libere o acesso às VMs para abrir inspeção runtime e dados sensíveis do host.',
          'Grant VM access before opening runtime inspection and sensitive host data.'
        )
      );
      return;
    }

    const safeItem = item || null;
    if (!safeItem) {
      return;
    }

    if (safeItem.componentId) {
      openInspectionByComponentId(safeItem.componentId);
      return;
    }

    if (activeOrganizationFallbackMeta) {
      openInspectionFromSnapshot(safeItem, {
        fallbackMeta: activeOrganizationFallbackMeta,
        correlation: {
          runId: activeOrganization && activeOrganization.latestRunId,
          changeId: activeOrganization && activeOrganization.latestChangeId,
        },
      });
      return;
    }

    if (!safeItem.componentId) {
      message.warning(
        t(
          'component_id oficial indisponível para abrir a inspeção deste componente.',
          'Official component_id unavailable to open inspection for this component.'
        )
      );
    }
  };

  const handleOpenChannelWorkspace = channelId => {
    if (!activeOrganization || !activeOrganization.latestRunId) {
      message.warning(
        t(
          'run_id oficial indisponível para abrir o workspace do canal.',
          'Official run_id unavailable to open the channel workspace.'
        )
      );
      return;
    }
    const normalizedChannelId = normalizeText(channelId);
    if (!normalizedChannelId) {
      return;
    }

    history.push(
      `${CHANNEL_WORKSPACE_ROUTE_PATH}?run_id=${encodeURIComponent(
        activeOrganization.latestRunId
      )}&channel_id=${encodeURIComponent(normalizedChannelId)}&org_id=${encodeURIComponent(
        activeOrganization.orgId || ''
      )}`
    );
  };

  const handleOpenOfficialRuntimeTopology = () => {
    if (!activeOrganizationVmAccessGranted) {
      message.warning(
        t(
          'Libere o acesso às VMs antes de abrir a topologia runtime desta organização.',
          'Grant VM access before opening this organization runtime topology.'
        )
      );
      return;
    }
    if (!activeOrganization || !activeOrganization.latestRunId) {
      message.warning(
        t(
          'run_id oficial indisponível para abrir a topologia runtime.',
          'Official run_id unavailable to open the runtime topology.'
        )
      );
      return;
    }
    history.push(
      `${RUNTIME_TOPOLOGY_ROUTE_PATH}?run_id=${encodeURIComponent(activeOrganization.latestRunId)}`
    );
  };

  const activeOrganizationTelemetry = activeOrganization
    ? telemetryByOrganization[activeOrganization.key] || null
    : null;
  const activeOrganizationA2AGate = activeOrganizationOfficialState.gate || null;
  const activeOrganizationA2AInteractive = Boolean(
    activeOrganizationA2AGate && activeOrganizationA2AGate.interactiveEnabled
  );
  const activeOrganizationA2ADashboardAllowed = Boolean(
    activeOrganizationA2AGate &&
      activeOrganizationA2AGate.actionAvailability &&
      activeOrganizationA2AGate.actionAvailability.openOperationalDashboard
  );
  const activeOrganizationTelemetryAuditOnly = Boolean(
    activeOrganizationTelemetry && activeOrganizationTelemetry.status === 'audit_only'
  );
  const activeOrganizationTelemetryIndex = useMemo(
    () => buildTelemetryIndex(activeOrganizationTelemetry),
    [activeOrganizationTelemetry]
  );
  useEffect(() => {
    if (!activeOrganization || !activeOrganizationOperationalCard) {
      workspaceOperationalNoticeRef.current = '';
      message.destroy('overview-organization-operational-status');
      return undefined;
    }

    const noticeSignature = [
      activeOrganization.key,
      activeOrganizationOperationalCard.operationalStatus,
      activeOrganizationOperationalCard.statusReason,
      activeOrganizationOperationalCard.baselineStatus,
      activeOrganizationOperationalCard.freshnessStatus,
      activeOrganizationOperationalCard.pendingAlertsCount,
    ].join('|');

    if (workspaceOperationalNoticeRef.current === noticeSignature) {
      return undefined;
    }

    workspaceOperationalNoticeRef.current = noticeSignature;
    message.open({
      key: 'overview-organization-operational-status',
      type:
        OVERVIEW_MESSAGE_TYPE_BY_ALERT_TYPE[
          activeOrganizationOperationalCard.operationalTone.alertType
        ] || 'info',
      content: buildOperationalWorkspaceNotice({
        organization: activeOrganization,
        operationalCard: activeOrganizationOperationalCard,
      }),
      duration: 5,
    });

    return undefined;
  }, [activeOrganization, activeOrganizationOperationalCard]);
  useEffect(() => {
    if (!activeOrganization || !activeOrganizationTelemetry) {
      workspaceTelemetryNoticeRef.current = '';
      message.destroy('overview-organization-telemetry-status');
      return undefined;
    }

    if (
      activeOrganizationTelemetry.status !== 'error' &&
      activeOrganizationTelemetry.status !== 'unavailable' &&
      activeOrganizationTelemetry.status !== 'audit_only'
    ) {
      workspaceTelemetryNoticeRef.current = '';
      message.destroy('overview-organization-telemetry-status');
      return undefined;
    }

    const noticeSignature = [
      activeOrganization.key,
      activeOrganizationTelemetry.status,
      normalizeText(activeOrganizationTelemetry.message),
    ].join('|');

    if (workspaceTelemetryNoticeRef.current === noticeSignature) {
      return undefined;
    }

    workspaceTelemetryNoticeRef.current = noticeSignature;
    const telemetryNotice = buildTelemetryWorkspaceNotice(activeOrganizationTelemetry);
    message.open({
      key: 'overview-organization-telemetry-status',
      type: telemetryNotice.type,
      content: telemetryNotice.content,
      duration: 5,
    });

    return undefined;
  }, [activeOrganization, activeOrganizationTelemetry]);
  const activeOrganizationRuntimePeers = useMemo(
    () =>
      activeOrganization
        ? activeOrganizationPeers.map(node => buildRuntimeNode(node, activeOrganizationTelemetryIndex))
        : [],
    [activeOrganization, activeOrganizationPeers, activeOrganizationTelemetryIndex]
  );
  const activeOrganizationPrimaryHostAddress = useMemo(
    () =>
      activeOrganization
        ? resolveOrganizationPrimaryHostAddress(
            activeOrganization,
            activeOrganizationTelemetryIndex
          )
        : '',
    [activeOrganization, activeOrganizationTelemetryIndex]
  );
  const activeOrganizationRuntimeOrderers = useMemo(
    () =>
      activeOrganization
        ? activeOrganizationOrderers.map(node =>
            buildRuntimeNode(node, activeOrganizationTelemetryIndex)
          )
        : [],
    [activeOrganization, activeOrganizationOrderers, activeOrganizationTelemetryIndex]
  );
  const workspaceNavigationCards = activeOrganization
    ? [
        {
          key: 'peers',
          eyebrow: t('Malha Fabric', 'Fabric mesh'),
          title: t('Peers', 'Peers'),
          metric: activeOrganizationOfficialPeers.length || activeOrganizationRuntimePeers.length,
          detail:
            activeOrganizationOfficialPeers.length > 0
              ? t(
                  'Abrir peers com status oficial e inspeção técnica por componente.',
                  'Open peers with official status and technical inspection by component.'
                )
              : t(
                  'Abrir peers legados conhecidos deste snapshot auditável.',
                  'Open legacy peers known from this auditable snapshot.'
                ),
        },
        {
          key: 'orderers',
          eyebrow: t('Malha Fabric', 'Fabric mesh'),
          title: t('Orderers', 'Orderers'),
          metric:
            activeOrganizationOfficialOrderers.length || activeOrganizationRuntimeOrderers.length,
          detail:
            activeOrganizationOfficialOrderers.length > 0
              ? t(
                  'Abrir orderers com consenso, disponibilidade e inspeção técnica.',
                  'Open orderers with consensus, availability, and technical inspection.'
                )
              : t(
                  'Abrir orderers legados conhecidos deste snapshot auditável.',
                  'Open legacy orderers known from this auditable snapshot.'
                ),
        },
        {
          key: 'services',
          eyebrow: t('Serviços operacionais', 'Operational services'),
          title: t('Control plane e suporte', 'Control plane and support'),
          metric: activeOrganizationOfficialServices.length,
          detail: t(
            'CA, gateway, netapi, couch e runtime de chaincode em uma janela dedicada.',
            'CA, gateway, netapi, couch, and chaincode runtime in a dedicated window.'
          ),
        },
        {
          key: 'channels',
          eyebrow: t('Escopo de negócio', 'Business scope'),
          title: t('Canais', 'Channels'),
          metric: activeOrganizationOfficialChannels.length,
          detail: t(
            'Abrir business groups, canais e vínculos oficiais desta organização.',
            'Open business groups, channels, and official links for this organization.'
          ),
        },
        {
          key: 'chaincodes',
          eyebrow: t('Escopo de negócio', 'Business scope'),
          title: t('Chaincodes', 'Chaincodes'),
          metric: activeOrganizationOfficialChaincodes.length,
          detail: t(
            'Abrir catálogo ativo de chaincodes e versões associadas aos canais.',
            'Open the active chaincode catalog and versions linked to the channels.'
          ),
        },
        {
          key: 'audit',
          eyebrow: t('Auditoria', 'Audit'),
          title: t('Evidências e propostas', 'Evidence and proposals'),
          metric: activeOrganizationProposalEntries.length,
          detail: t(
            'Abrir artefatos, trilha auditável e histórico recente de propostas operacionais.',
            'Open artifacts, the auditable trail, and recent history of operational proposals.'
          ),
        },
      ]
    : [];
  let organizationPanelContent = null;

  if (organizationsDashboardProjection.length === 0) {
    organizationPanelContent = (
      <Empty
        description={t(
          'Nenhuma organização auditável ainda. Conclua um provisionamento para habilitar o painel.',
          'No auditable organization yet. Complete a provisioning flow to enable the dashboard.'
        )}
        image={Empty.PRESENTED_IMAGE_SIMPLE}
      />
    );
  } else if (filteredOrganizationOperationalCards.length === 0) {
    organizationPanelContent = (
      <Empty
        description={t(
          'Nenhuma organização atende aos filtros operacionais atuais.',
          'No organization matches the current operational filters.'
        )}
        image={Empty.PRESENTED_IMAGE_SIMPLE}
      />
    );
  } else {
    organizationPanelContent = (
      <div className={styles.organizationGrid}>
        {filteredOrganizationOperationalCards.map(card => {
          const organization =
            organizationsDashboardProjection.find(entry => entry.key === card.key) || null;
          const executionTone = resolveExecutionTone(
            organization ? organization.latestStatus : 'partial'
          );
          const runtimeSummary =
            (organization && organizationRuntimeSummaryByKey[organization.key]) || null;
          const snapshotSummary =
            (organization && organizationSnapshotByKey[organization.key]) || null;
          let peersCount = 0;
          let orderersCount = 0;
          if (snapshotSummary) {
            peersCount = snapshotSummary.peersCount;
            orderersCount = snapshotSummary.orderersCount;
          } else if (organization) {
            peersCount = organization.peerCount;
            orderersCount = organization.ordererCount;
          }
          const telemetryEntry =
            (organization && telemetryByOrganization[organization.key]) || null;
          const telemetryLiveLabel = resolveTelemetryLiveLabel(telemetryEntry);
          const organizationVmAccessGranted = organization
            ? hasOrganizationVmAccess(organizationVmAccessRegistry, organization.key)
            : false;
          const organizationInitials =
            normalizeText(card.organizationName || card.organizationId)
              .split(/[\s-]+/)
              .map(token => token.slice(0, 1).toUpperCase())
              .join('')
              .slice(0, 2) || 'OR';

          return (
            <div
              key={card.key}
              className={styles.organizationTile}
              role="button"
              tabIndex={0}
              onClick={() => handleOpenOrganizationCard(card.key)}
              onKeyDown={event => handleOrganizationCardKeyDown(event, card.key)}
            >
              <div className={styles.organizationTileHeader}>
                <div className={styles.organizationIdentity}>
                  <div className={styles.organizationSeal}>{organizationInitials}</div>
                  <div>
                    <Typography.Text className={styles.organizationTileName}>
                      {card.organizationName || card.organizationId}
                    </Typography.Text>
                    <Typography.Text className={styles.organizationTileMeta}>
                      {card.domain || t('domain nao informado', 'domain not informed')}
                    </Typography.Text>
                    <Typography.Text className={styles.organizationTileMeta}>
                      {t('Último run: {runId}', 'Latest run: {runId}', {
                        runId: card.latestRunId || '-',
                      })}
                    </Typography.Text>
                  </div>
                </div>
                <div className={styles.organizationTileActions}>
                  <Space wrap>
                    <Tag color={card.operationalTone.color}>{card.operationalTone.label}</Tag>
                    <Tag color={resolveOperationalCriticalityColor(card.criticality)}>
                      {card.criticalityLabel}
                    </Tag>
                    <Tag color={executionTone.color}>{executionTone.label}</Tag>
                  </Space>
                  <Button
                    size="small"
                    type="primary"
                    onClick={event => {
                      event.stopPropagation();
                      handleOpenOrganizationCard(card.key, 'overview');
                    }}
                  >
                    {t('Ver detalhes', 'View details')}
                  </Button>
                </div>
              </div>

              <Typography.Paragraph className={styles.organizationStatusSummary}>
                {card.statusReason}
              </Typography.Paragraph>

              <div className={styles.organizationKpiGrid}>
                <div className={styles.organizationMetricTile}>
                  <Typography.Text className={styles.organizationMetricLabel}>
                    {t('Último snapshot', 'Latest snapshot')}
                  </Typography.Text>
                  <strong>
                    {formatOverviewTimestamp(
                      card.officialWorkspaceObservedAt || card.latestFinishedAt,
                      locale
                    ) || '-'}
                  </strong>
                  <span>{t('referência auditável mais recente', 'latest auditable reference')}</span>
                </div>
                <div className={styles.organizationMetricTile}>
                  <Typography.Text className={styles.organizationMetricLabel}>
                    {t('Host principal', 'Primary host')}
                  </Typography.Text>
                  <strong>
                    {resolveProtectedValue(
                      snapshotSummary && snapshotSummary.primaryHostAddress,
                      organizationVmAccessGranted,
                      t('oculto ate liberar acesso', 'hidden until access is granted')
                    )}
                  </strong>
                  <span>{t('endpoint operacional preferencial', 'preferred operational endpoint')}</span>
                </div>
                <div className={styles.organizationMetricTile}>
                  <Typography.Text className={styles.organizationMetricLabel}>
                    Peers
                  </Typography.Text>
                  <strong>{peersCount}</strong>
                  <span>
                    {runtimeSummary && runtimeSummary.telemetryReady
                      ? t('{count} with running status', '{count} with running status', {
                          count: runtimeSummary.peerActiveCount,
                        })
                      : t('componentes conhecidos no snapshot', 'components known in the snapshot')}
                  </span>
                </div>
                <div className={styles.organizationMetricTile}>
                  <Typography.Text className={styles.organizationMetricLabel}>
                    Orderers
                  </Typography.Text>
                  <strong>{orderersCount}</strong>
                  <span>
                    {runtimeSummary && runtimeSummary.telemetryReady
                      ? t('{count} with running status', '{count} with running status', {
                          count: runtimeSummary.ordererActiveCount,
                        })
                      : t('nós de consenso conhecidos no snapshot', 'consensus nodes known in the snapshot')}
                  </span>
                </div>
                <div className={styles.organizationMetricTile}>
                  <Typography.Text className={styles.organizationMetricLabel}>
                    {t('Canais', 'Channels')}
                  </Typography.Text>
                  <strong>{snapshotSummary ? snapshotSummary.channelsCount : card.channelsCount}</strong>
                  <span>{t('escopos oficiais vinculados', 'linked official scopes')}</span>
                </div>
                <div className={styles.organizationMetricTile}>
                  <Typography.Text className={styles.organizationMetricLabel}>
                    Chaincodes
                  </Typography.Text>
                  <strong>{snapshotSummary ? snapshotSummary.chaincodesCount : card.activeChaincodesCount}</strong>
                  <span>{t('catálogo conhecido', 'known catalog')}</span>
                </div>
              </div>

              <div className={styles.organizationComponentPreviewGrid}>
                <div className={styles.organizationComponentPreviewCard}>
                  <div className={styles.organizationComponentPreviewHeader}>
                    <Typography.Text className={styles.organizationMetricLabel}>
                      {t('Peers visíveis', 'Visible peers')}
                    </Typography.Text>
                    <span>
                      {t('{count} total', '{count} total', {
                        count: (snapshotSummary && snapshotSummary.peersCount) || 0,
                      })}
                    </span>
                  </div>
                  <div className={styles.organizationComponentPillList}>
                    {snapshotSummary && snapshotSummary.peerPreviewEntries.length > 0 ? (
                      snapshotSummary.peerPreviewEntries.map(entry => (
                        <div key={entry.key} className={styles.organizationComponentPill}>
                          <div>
                            <strong>{entry.label}</strong>
                            {entry.meta ? (
                              <span>
                                {resolveProtectedHostMeta(entry.meta, organizationVmAccessGranted)}
                              </span>
                            ) : null}
                          </div>
                          <Tag color={entry.toneColor}>{entry.toneLabel}</Tag>
                        </div>
                      ))
                    ) : (
                      <Typography.Text type="secondary">
                        Nenhum peer identificado neste snapshot.
                      </Typography.Text>
                    )}
                  </div>
                </div>

                <div className={styles.organizationComponentPreviewCard}>
                  <div className={styles.organizationComponentPreviewHeader}>
                    <Typography.Text className={styles.organizationMetricLabel}>
                      Visible Orderers
                    </Typography.Text>
                    <span>{`${(snapshotSummary && snapshotSummary.orderersCount) || 0} total`}</span>
                  </div>
                  <div className={styles.organizationComponentPillList}>
                    {snapshotSummary && snapshotSummary.ordererPreviewEntries.length > 0 ? (
                      snapshotSummary.ordererPreviewEntries.map(entry => (
                        <div key={entry.key} className={styles.organizationComponentPill}>
                          <div>
                            <strong>{entry.label}</strong>
                            {entry.meta ? (
                              <span>
                                {resolveProtectedHostMeta(entry.meta, organizationVmAccessGranted)}
                              </span>
                            ) : null}
                          </div>
                          <Tag color={entry.toneColor}>{entry.toneLabel}</Tag>
                        </div>
                      ))
                    ) : (
                      <Typography.Text type="secondary">
                        Nenhum orderer identificado neste snapshot.
                      </Typography.Text>
                    )}
                  </div>
                </div>
              </div>

              <div className={styles.organizationSignalRail}>
                <div className={styles.organizationSignalPill}>
                  <ReloadOutlined />
                  <span>{`Freshness ${card.freshnessStatus === 'fresh' ? '< 2 min' : card.freshnessStatus}`}</span>
                </div>
                <div className={styles.organizationSignalPill}>
                  {telemetryLiveLabel ? (
                    <>
                      <ClusterOutlined />
                      <span>{telemetryLiveLabel}</span>
                    </>
                  ) : (
                    <>
                      <ClusterOutlined />
                      <span>{t('Snapshot auditável', 'Auditable snapshot')}</span>
                    </>
                  )}
                </div>
                <div className={styles.organizationSignalPill}>
                  <WarningOutlined />
                  <span>
                    {t(
                      'Alertas {count} pendentes',
                      '{count} pending alerts',
                      { count: card.pendingAlertsCount }
                    )}
                  </span>
                </div>
                {snapshotSummary && snapshotSummary.fallbackMeta ? (
                  <div className={styles.organizationSignalPill}>
                    <CheckCircleOutlined />
                    <span>{t('Snapshot local em cache', 'Local snapshot cached')}</span>
                  </div>
                ) : null}
              </div>

              <div className={styles.organizationTileFooter}>
                <Typography.Text className={styles.organizationTileMeta}>
                  {card.hostLabels.length > 0
                    ? `Hosts: ${card.hostLabels.slice(0, 2).join(' | ')}`
                    : t('Host oficial nao identificado', 'Official host not identified')}
                </Typography.Text>
                <Space>
                  <Button
                    size="small"
                    type="link"
                    onClick={event => {
                      event.stopPropagation();
                      handleOpenOrganizationCard(card.key, 'proposals');
                    }}
                  >
                    {t('Propostas', 'Proposals')}
                  </Button>
                  <RightOutlined />
                </Space>
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  return (
    <PageHeaderWrapper>
      <OperationalWindowManagerProvider>
      <div className={styles.root}>
        <Card className={styles.heroCard}>
          <div className={styles.heroHeader}>
            <Space direction="vertical" size={8}>
              <Tag color="blue">COGNUS</Tag>
              <Typography.Title level={2} className={styles.title}>
                {t('Dashboard para Operador e Auditor', 'Dashboard for Operator and Auditor')}
              </Typography.Title>
              <Typography.Paragraph className={styles.subtitle}>
                {t(
                  'Triagem operacional por snapshot oficial, telemetria disponível e inventário auditável das organizações provisionadas.',
                  'Operational triage based on the official snapshot, available telemetry, and auditable inventory of provisioned organizations.'
                )}
              </Typography.Paragraph>
            </Space>
            <div className={styles.heroMeta}>
              <Button loading={isRefreshingOverview || isRefreshingTelemetry} onClick={handleRefreshPanel}>
                {t('Atualizar painel', 'Refresh dashboard')}
              </Button>
              <Typography.Text className={styles.heroMetaText}>
                {overviewLastUpdatedLabel
                  ? t('Última atualização: {value}', 'Last updated: {value}', {
                      value: overviewLastUpdatedLabel,
                    })
                  : t('Última atualização: -', 'Last updated: -')}
              </Typography.Text>
            </div>
          </div>
        </Card>

        <div className={styles.overviewSummaryStrip}>
          <Card size="small" className={styles.summaryCard}>
            <Typography.Text className={styles.summaryLabel}>
              {t('Organizações ativas', 'Active organizations')}
            </Typography.Text>
            <strong className={styles.summaryValue}>{overviewCounts.total}</strong>
            <span className={styles.summaryMeta}>
              {t('{count} consolidadas', '{count} consolidated', {
                count: overviewCounts.implemented,
              })}
            </span>
          </Card>
          <Card size="small" className={styles.summaryCard}>
            <Typography.Text className={styles.summaryLabel}>
              {t('Telemetria live', 'Live telemetry')}
            </Typography.Text>
            <strong className={styles.summaryValue}>{`${overviewLiveTelemetryCount}/${overviewCounts.total || 0}`}</strong>
            <span className={styles.summaryMeta}>
              {t('organizações com coleta ativa', 'organizations with active collection')}
            </span>
          </Card>
          <Card size="small" className={styles.summaryCard}>
            <Typography.Text className={styles.summaryLabel}>
              {t('Snapshots locais', 'Local snapshots')}
            </Typography.Text>
            <strong className={styles.summaryValue}>{overviewSnapshotAuditOnlyCount}</strong>
            <span className={styles.summaryMeta}>
              {t('reaproveitados do cache auditável', 'reused from the auditable cache')}
            </span>
          </Card>
          <Card size="small" className={styles.summaryCard}>
            <Typography.Text className={styles.summaryLabel}>
              {t('Recência', 'Freshness')}
            </Typography.Text>
            <strong className={styles.summaryValue}>{`${overviewFreshCount}/${overviewCounts.total || 0}`}</strong>
            <span className={styles.summaryMeta}>
              {t('Telemetria atualizada', 'Telemetry updated')}
            </span>
          </Card>
          <Card size="small" className={styles.summaryCard}>
            <Typography.Text className={styles.summaryLabel}>
              {t('Canais e chaincodes', 'Channels and chaincodes')}
            </Typography.Text>
            <strong className={styles.summaryValue}>{overviewChannelsTotal}</strong>
            <span className={styles.summaryMeta}>
              {t('{count} chaincodes conhecidos', '{count} known chaincodes', {
                count: overviewChaincodesTotal,
              })}
            </span>
          </Card>
          <Card size="small" className={styles.summaryCard}>
            <Typography.Text className={styles.summaryLabel}>
              {t('Alertas pendentes', 'Pending alerts')}
            </Typography.Text>
            <strong className={styles.summaryValueAlert}>{overviewPendingAlertsTotal}</strong>
            <span className={styles.summaryMeta}>{t('Requer atenção', 'Requires attention')}</span>
          </Card>
        </div>

        <Card className={styles.organizationCard}>
          <div className={styles.organizationHeader}>
            <div>
              <Typography.Text className={styles.organizationEyebrow}>
                {t('Panorama de organizações provisionadas', 'Provisioned organizations overview')}
              </Typography.Text>
              <Typography.Title level={3} className={styles.organizationTitle}>
                {t('Organizações ativas no ecossistema', 'Active organizations in the ecosystem')}
              </Typography.Title>
              <Typography.Paragraph className={styles.organizationDescription}>
                {t(
                  'Filtre por organização, status, criticidade, host e recência. Cada card mostra o snapshot auditável, peers, orderers, canais, chaincodes e alertas pendentes.',
                  'Filter by organization, status, criticality, host, and recency. Each card shows the auditable snapshot, peers, orderers, channels, chaincodes, and pending alerts.'
                )}
              </Typography.Paragraph>
            </div>
            <Space wrap>
              <Tag color="green">
                {t('{count} implementadas', '{count} implemented', {
                  count: overviewCounts.implemented,
                })}
              </Tag>
              <Tag color="orange">
                {t('{count} parciais', '{count} partial', { count: overviewCounts.partial })}
              </Tag>
              <Tag color="red">
                {t('{count} bloqueadas', '{count} blocked', { count: overviewCounts.blocked })}
              </Tag>
              <Button loading={isRefreshingOverview || isRefreshingTelemetry} onClick={handleRefreshPanel}>
                {t('Atualizar painel', 'Refresh dashboard')}
              </Button>
            </Space>
          </div>

          <div className={styles.organizationFilterBar}>
            <Input.Search
              allowClear
              placeholder={t('Filtrar organizacao', 'Filter organization')}
              value={overviewFilters.organization}
              onChange={event =>
                setOverviewFilters(current => ({
                  ...current,
                  organization: event.target.value,
                }))
              }
            />
            <Select
              value={overviewFilters.status}
              onChange={value =>
                setOverviewFilters(current => ({
                  ...current,
                  status: value,
                }))
              }
            >
              <Select.Option value="all">{t('Todos os status', 'All statuses')}</Select.Option>
              {overviewFilterOptions.statuses.map(status => (
                <Select.Option key={status} value={status}>
                  {status}
                </Select.Option>
              ))}
            </Select>
            <Select
              value={overviewFilters.criticality}
              onChange={value =>
                setOverviewFilters(current => ({
                  ...current,
                  criticality: value,
                }))
              }
            >
              <Select.Option value="all">
                {t('Toda criticidade', 'All criticalities')}
              </Select.Option>
              {overviewFilterOptions.criticalities.map(criticality => (
                <Select.Option key={criticality} value={criticality}>
                  {criticality}
                </Select.Option>
              ))}
            </Select>
            <Select
              value={overviewFilters.host}
              onChange={value =>
                setOverviewFilters(current => ({
                  ...current,
                  host: value,
                }))
              }
            >
              <Select.Option value="all">{t('Todos os hosts', 'All hosts')}</Select.Option>
              {overviewFilterOptions.hosts.map(host => (
                <Select.Option key={host} value={host}>
                  {host}
                </Select.Option>
              ))}
            </Select>
            <Select
              value={overviewFilters.freshness}
              onChange={value =>
                setOverviewFilters(current => ({
                  ...current,
                  freshness: value,
                }))
              }
            >
              <Select.Option value="all">{t('Toda recencia', 'All recency')}</Select.Option>
              {overviewFilterOptions.freshness.map(freshness => (
                <Select.Option key={freshness} value={freshness}>
                  {freshness}
                </Select.Option>
              ))}
            </Select>
            <Button onClick={() => setOverviewFilters(OVERVIEW_EMPTY_CARD_FILTERS)}>
              {t('Limpar filtros', 'Clear filters')}
            </Button>
          </div>

          {organizationPanelContent}
        </Card>

        <div className={styles.overviewLowerGrid}>
          <Card className={styles.workspaceSectionCard}>
            <div className={styles.workspaceSectionHeading}>
              <div>
                <Typography.Text className={styles.workspaceSectionEyebrow}>
                  {t('Atividades recentes', 'Recent activities')}
                </Typography.Text>
                <Typography.Title level={4} className={styles.workspaceSectionTitle}>
                  {t('Mudanças e eventos observados', 'Observed changes and events')}
                </Typography.Title>
              </div>
            </div>
            <div className={styles.activityList}>
              {overviewRecentActivities.length > 0 ? (
                overviewRecentActivities.map(activity => {
                  const tone = resolveExecutionTone(activity.status);
                  return (
                    <div key={activity.key} className={styles.activityItem}>
                      <div className={styles.activityBulletWrap}>
                        <span
                          className={`${styles.activityBullet} ${
                            resolveActivityBulletToneClassName(styles, tone.color)
                          }`}
                        />
                      </div>
                      <div className={styles.activityContent}>
                        <Typography.Text strong>
                          {`${activity.organizationName} • ${activity.title}`}
                        </Typography.Text>
                        <div className={styles.activityMetaRow}>
                          <Tag color={tone.color}>{tone.label}</Tag>
                          <Typography.Text type="secondary">
                            {formatOverviewTimestamp(activity.timestamp, locale) || '-'}
                          </Typography.Text>
                        </div>
                      </div>
                    </div>
                  );
                })
              ) : (
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description={t(
                    'Sem mudanças recentes materializadas no histórico local.',
                    'No recent changes materialized in the local history.'
                  )}
                />
              )}
            </div>
          </Card>

          <Card className={styles.workspaceSectionCard}>
            <div className={styles.workspaceSectionHeading}>
              <div>
                <Typography.Text className={styles.workspaceSectionEyebrow}>
                  {t('Leitura consolidada', 'Consolidated reading')}
                </Typography.Text>
                <Typography.Title level={4} className={styles.workspaceSectionTitle}>
                  {t('Sinais atuais do painel', 'Current panel signals')}
                </Typography.Title>
              </div>
            </div>
            <div className={styles.trendGrid}>
              <div className={styles.trendTile}>
                <div className={styles.trendTileHeader}>
                  <Typography.Text strong>{t('Telemetria live', 'Live telemetry')}</Typography.Text>
                  <Tag color="blue">{t('atual', 'current')}</Tag>
                </div>
                <strong className={styles.trendValue}>{`${overviewLiveTelemetryCount}/${overviewCounts.total || 0}`}</strong>
                <span className={styles.trendMeta}>
                  {t(
                    'organizações com coleta ativa neste momento',
                    'organizations with active collection right now'
                  )}
                </span>
                <div className={`${styles.trendSpark} ${styles.trendSparkBlue}`} />
              </div>
              <div className={styles.trendTile}>
                <div className={styles.trendTileHeader}>
                  <Typography.Text strong>{t('Snapshot local', 'Local snapshot')}</Typography.Text>
                  <Tag color="gold">{t('cache', 'cache')}</Tag>
                </div>
                <strong className={styles.trendValue}>{overviewSnapshotAuditOnlyCount}</strong>
                <span className={styles.trendMeta}>
                  {t(
                    'organizações navegando sem backend live',
                    'organizations navigating without a live backend'
                  )}
                </span>
                <div className={`${styles.trendSpark} ${styles.trendSparkGreen}`} />
              </div>
              <div className={styles.trendTile}>
                <div className={styles.trendTileHeader}>
                  <Typography.Text strong>
                    {t('Chaincodes oficiais', 'Official chaincodes')}
                  </Typography.Text>
                  <Tag color="purple">{t('catálogo', 'catalog')}</Tag>
                </div>
                <strong className={styles.trendValue}>{overviewChaincodesTotal}</strong>
                <span className={styles.trendMeta}>
                  {t(
                    'versões conhecidas nos snapshots oficiais',
                    'versions known in the official snapshots'
                  )}
                </span>
                <div className={`${styles.trendSpark} ${styles.trendSparkRed}`} />
              </div>
            </div>
          </Card>
        </div>

        <OperationalWindowDialog
          windowId="overview-organization-workspace"
          eyebrow={t('Workspace da organizacao', 'Organization workspace')}
          title={
            activeOrganization
              ? t('Organização: {name}', 'Organization: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Organização', 'Organization')
          }
          onClose={() => {
            setActiveOrganizationKey('');
            setWorkspaceWindows(createEmptyWorkspaceWindowState());
            setActiveGovernedAction('');
          }}
          open={Boolean(activeOrganization)}
          preferredWidth="88vw"
          preferredHeight="86vh"
        >
          {activeOrganization && (
            <div className={styles.organizationDrawerContent}>
              <div className={styles.workspaceCommandDeck}>
                <div className={styles.workspaceIdentityBlock}>
                  <div className={styles.workspaceIdentitySeal}>
                    {normalizeText(activeOrganization.orgName || activeOrganization.orgId)
                      .slice(0, 2)
                      .toUpperCase() || 'OR'}
                  </div>
                  <div className={styles.workspaceIdentityText}>
                    <Typography.Text className={styles.workspaceEyebrow}>
                      {t('Cabine operacional da organização', 'Organization operational cockpit')}
                    </Typography.Text>
                    <Typography.Title level={3} className={styles.workspaceTitle}>
                      {activeOrganization.orgName || activeOrganization.orgId}
                    </Typography.Title>
                    <div className={styles.workspaceMetaRow}>
                      <span>
                        {activeOrganization.domain || t('domain não informado', 'domain not informed')}
                      </span>
                      <span>{activeOrganizationEnvironments.join(' | ') || t('ambiente n/d', 'environment n/a')}</span>
                      <span>{activeOrganizationProviders.join(' | ') || t('provider n/d', 'provider n/a')}</span>
                    </div>
                    <div className={styles.workspaceCodeRow}>
                      <Typography.Text code>{`run_id ${activeOrganization.latestRunId || '-'}`}</Typography.Text>
                      <Typography.Text code>{`change_id ${activeOrganization.latestChangeId || '-'}`}</Typography.Text>
                      {activeOrganizationA2AGate && (
                        <Tag color={resolveA2AGateAlertType(activeOrganizationA2AGate)}>
                          {activeOrganizationA2AGate.status || 'pending'}
                        </Tag>
                      )}
                    </div>
                  </div>
                </div>

                <div className={styles.workspaceHeroActions}>
                  <Button
                    type="primary"
                    disabled={!activeOrganizationA2ADashboardAllowed || !activeOrganizationVmAccessGranted}
                    onClick={handleOpenOfficialRuntimeTopology}
                  >
                    {t('Abrir topologia runtime', 'Open runtime topology')}
                  </Button>
                  <Button
                    loading={Boolean(
                      activeOrganizationTelemetry && activeOrganizationTelemetry.status === 'loading'
                    )}
                    disabled={
                      !activeOrganizationVmAccessGranted ||
                      activeOrganizationTelemetryAuditOnly ||
                      Boolean(activeOrganizationFallbackMeta)
                    }
                    onClick={() => refreshTelemetryForOrganizations([activeOrganization])}
                  >
                    {t('Atualizar telemetria', 'Refresh telemetry')}
                  </Button>
                  {activeOrganizationVmAccessGranted ? (
                    <Button icon={<LockOutlined />} onClick={handleRevokeVmAccess}>
                      {t('Remover acesso às VMs', 'Remove VM access')}
                    </Button>
                  ) : (
                    <Button icon={<UnlockOutlined />} onClick={handleOpenVmAccessWindow}>
                      {t('Acessar VM(s)', 'Access VM(s)')}
                    </Button>
                  )}
                  <Button
                    onClick={() =>
                      activeOrganizationRunHistory[0] &&
                      handleOpenRunbookAudit(activeOrganizationRunHistory[0])
                    }
                  >
                    {t('Abrir auditoria', 'Open audit')}
                  </Button>
                </div>
              </div>

              {activeOrganizationFallbackMeta && (
                <Alert
                  showIcon
                  type="warning"
                  message={t('Modo auditoria por snapshot local', 'Local snapshot audit mode')}
                  description={`source=${activeOrganizationFallbackMeta.source || '-'} | ${
                    activeOrganizationFallbackMeta.reasonMessage ||
                    t(
                      'O backend oficial não mantém mais este run_id para leitura live. A organização continua disponível para auditoria com o último snapshot íntegro.',
                      'The official backend no longer keeps this run_id for live reads. The organization remains available for auditing with the latest intact snapshot.'
                    )
                  }`}
                />
              )}

              {activeOrganization.isSynthetic && (
                <Alert
                  showIcon
                  type="warning"
                  message={t(
                    'Metadados legados sem topologia completa',
                    'Legacy metadata without complete topology'
                  )}
                  description={t(
                    'Este histórico não possui cadastro detalhado de organização. Conclua novo provisionamento para habilitar visão técnica completa.',
                    'This history does not include detailed organization registration. Complete a new provisioning run to enable the full technical view.'
                  )}
                />
              )}

              {!activeOrganization.isSynthetic && activeOrganizationOfficialState.error && (
                <Alert
                  showIcon
                  type="warning"
                  message={t('Gate oficial do A2A indisponível', 'Official A2A gate unavailable')}
                  description={activeOrganizationOfficialState.error}
                />
              )}

              <Spin
                spinning={Boolean(
                  activeOrganizationTelemetry && activeOrganizationTelemetry.status === 'loading'
                )}
                tip={t('Atualizando telemetria da organização...', 'Refreshing organization telemetry...')}
              >
                <Card
                  size="small"
                  className={`${styles.workspaceSectionCard} ${styles.workspaceActionStripCard}`}
                >
                  <div className={styles.workspaceSectionHeading}>
                    <div>
                      <Typography.Text className={styles.workspaceSectionEyebrow}>
                        {t('Ações governadas', 'Governed actions')}
                      </Typography.Text>
                      <Typography.Title level={4} className={styles.workspaceSectionTitle}>
                        {t(
                          'Expansão operacional desta organização',
                          'Operational expansion for this organization'
                        )}
                      </Typography.Title>
                    </div>
                    <Tag color={activeOrganizationA2AInteractive ? 'green' : 'orange'}>
                      {activeOrganizationA2AInteractive
                        ? t('interativo', 'interactive')
                        : t('somente auditoria', 'audit only')}
                    </Tag>
                  </div>
                  <Typography.Paragraph className={styles.workspaceActionStripSummary}>
                    {t(
                      'Dispare as quatro ações incrementais oficiais diretamente do workspace da organização. Quando o gate A2A ainda não permitir mutação, os controles seguem visíveis para auditoria e contexto.',
                      'Trigger the four official incremental actions directly from the organization workspace. When the A2A gate still does not allow mutation, the controls remain visible for audit and context.'
                    )}
                  </Typography.Paragraph>
                  <div className={styles.actionRailButtons}>
                    <Button
                      icon={<PlusOutlined />}
                      disabled={!activeOrganizationA2AInteractive}
                      onClick={() => handleOpenGovernedAction('addPeer')}
                    >
                      {t('Adicionar peer', 'Add peer')}
                    </Button>
                    <Button
                      icon={<PlusOutlined />}
                      disabled={!activeOrganizationA2AInteractive}
                      onClick={() => handleOpenGovernedAction('addOrderer')}
                    >
                      {t('Adicionar orderer', 'Add orderer')}
                    </Button>
                    <Button
                      icon={<PlusOutlined />}
                      disabled={!activeOrganizationA2AInteractive}
                      onClick={() => handleOpenGovernedAction('addChannel')}
                    >
                      {t('Adicionar channel', 'Add channel')}
                    </Button>
                    <Button
                      icon={<PlusOutlined />}
                      disabled={!activeOrganizationA2AInteractive}
                      onClick={() => handleOpenGovernedAction('addChaincode')}
                    >
                      {t('Adicionar chaincode', 'Add chaincode')}
                    </Button>
                  </div>
                </Card>

                <div className={styles.workspaceHubLayout}>
                  <div className={styles.workspaceHubSidebar}>
                    <Card size="small" className={styles.workspaceSectionCard}>
                      <Typography.Text className={styles.workspaceSectionEyebrow}>
                        {t('Acesso às VMs', 'VM access')}
                      </Typography.Text>
                      <div className={styles.workspaceSignalList}>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Status', 'Status')}</span>
                          <Tag color={activeOrganizationVmAccessGranted ? 'green' : 'orange'}>
                            {activeOrganizationVmAccessGranted
                              ? t('liberado nesta sessao', 'released in this session')
                              : t('bloqueado por padrao', 'blocked by default')}
                          </Tag>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('VMs validadas', 'Validated VMs')}</span>
                          <strong>
                            {activeOrganizationVmAccessGranted
                              ? `${activeOrganizationVmAccessGrant.validatedMachineCount || 0}/${activeOrganizationVmAccessTargets.length}`
                              : `0/${activeOrganizationVmAccessTargets.length}`}
                          </strong>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Ultima liberacao', 'Latest release')}</span>
                          <strong>
                            {activeOrganizationVmAccessGranted
                              ? formatOverviewTimestamp(
                                  activeOrganizationVmAccessGrant.grantedAtUtc,
                                  locale
                                )
                              : t(
                                  'preencha as credenciais de todas as VMs',
                                  'fill in the credentials for all VMs'
                                )}
                          </strong>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Escopo', 'Scope')}</span>
                          <strong>
                            {t(
                              'IPs, host refs, telemetria live e inspeção runtime',
                              'IPs, host refs, live telemetry, and runtime inspection'
                            )}
                          </strong>
                        </div>
                      </div>
                    </Card>

                    <Card size="small" className={styles.workspaceSectionCard}>
                      <Typography.Text className={styles.workspaceSectionEyebrow}>
                        {t('Confiança operacional', 'Operational confidence')}
                      </Typography.Text>
                      <div className={styles.workspaceSignalList}>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Gate A2A', 'A2A gate')}</span>
                          <Tag color={resolveA2AGateAlertType(activeOrganizationA2AGate)}>
                            {(activeOrganizationA2AGate && activeOrganizationA2AGate.status) || 'pending'}
                          </Tag>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Baseline', 'Baseline')}</span>
                          <strong>
                            {(activeOrganizationOperationalCard &&
                              activeOrganizationOperationalCard.baselineLabel) || '-'}
                          </strong>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Recência', 'Freshness')}</span>
                          <Tag
                            color={resolveFreshnessTagColor(
                              activeOrganizationOperationalCard &&
                                activeOrganizationOperationalCard.freshnessStatus
                            )}
                          >
                            {(activeOrganizationOperationalCard &&
                              activeOrganizationOperationalCard.freshnessLabel) || '-'}
                          </Tag>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Host principal', 'Primary host')}</span>
                          <strong>
                            {resolveProtectedValue(
                              activeOrganizationPrimaryHostAddress,
                              activeOrganizationVmAccessGranted,
                              t('oculto ate liberar acesso', 'hidden until access is granted')
                            )}
                          </strong>
                        </div>
                      </div>
                    </Card>

                    <Card size="small" className={styles.workspaceSectionCard}>
                      <Typography.Text className={styles.workspaceSectionEyebrow}>
                        {t('Snapshot oficial', 'Official snapshot')}
                      </Typography.Text>
                      <div className={styles.workspaceSignalList}>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Read model', 'Read model')}</span>
                          <Typography.Text code>
                            {(activeOrganizationOfficialWorkspace &&
                              activeOrganizationOfficialWorkspace.readModelFingerprint) || '-'}
                          </Typography.Text>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Observed at', 'Observed at')}</span>
                          <strong>
                            {(activeOrganizationOfficialWorkspace &&
                              activeOrganizationOfficialWorkspace.observedAt) || '-'}
                          </strong>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Componentes oficiais', 'Official components')}</span>
                          <strong>{activeOrganizationOfficialTopologyRows.length}</strong>
                        </div>
                        <div className={styles.workspaceSignalRow}>
                          <span>{t('Artefatos disponíveis', 'Available artifacts')}</span>
                          <strong>{activeOrganizationAvailableArtifactsCount}</strong>
                        </div>
                      </div>
                    </Card>
                  </div>

                  <div className={styles.workspaceHubMain}>
                    <Card size="small" className={styles.workspaceSectionCard}>
                      <div className={styles.workspaceSectionHeading}>
                        <div>
                          <Typography.Text className={styles.workspaceSectionEyebrow}>
                            {t(
                              'Hub operacional da organização',
                              'Organization operational hub'
                            )}
                          </Typography.Text>
                          <Typography.Title level={4} className={styles.workspaceSectionTitle}>
                            {t('Blocos principais', 'Main blocks')}
                          </Typography.Title>
                        </div>
                        <Space wrap>
                          <Tag color="blue">
                            {t('{count} domínios oficiais', '{count} official domains', {
                              count: activeOrganizationTopologyDomains.length,
                            })}
                          </Tag>
                          <Tag color="cyan">{`${activeOrganizationBusinessScopeGroups.length} business groups`}</Tag>
                          {!activeOrganizationVmAccessGranted ? (
                            <Tag color="gold">
                              {t('dados de host protegidos', 'protected host data')}
                            </Tag>
                          ) : null}
                        </Space>
                      </div>

                      <div className={styles.workspaceHubGrid}>
                        {workspaceNavigationCards.map(card => (
                          <button
                            key={card.key}
                            type="button"
                            className={styles.workspaceNavCard}
                            onClick={() => handleOpenWorkspaceWindow(card.key)}
                          >
                            <div className={styles.workspaceNavCardHeader}>
                              <div>
                                <Typography.Text className={styles.workspaceSectionEyebrow}>
                                  {card.eyebrow}
                                </Typography.Text>
                                <Typography.Title level={5} className={styles.workspaceNavCardTitle}>
                                  {card.title}
                                </Typography.Title>
                              </div>
                              <strong className={styles.workspaceNavMetric}>{card.metric}</strong>
                            </div>
                            <Typography.Paragraph className={styles.workspaceNavDescription}>
                              {card.detail}
                            </Typography.Paragraph>
                            <span className={styles.workspaceNavLink}>
                              {t('Abrir janela', 'Open window')}
                            </span>
                          </button>
                        ))}
                      </div>
                    </Card>

                    <Card size="small" className={styles.workspaceSectionCard}>
                      <div className={styles.workspaceSectionHeading}>
                        <div>
                          <Typography.Text className={styles.workspaceSectionEyebrow}>
                            {t('Escopo consolidado', 'Consolidated scope')}
                          </Typography.Text>
                          <Typography.Title level={4} className={styles.workspaceSectionTitle}>
                            {t('Resumo operacional', 'Operational summary')}
                          </Typography.Title>
                        </div>
                      </div>

                      <div className={styles.workspaceSummaryGrid}>
                        <div className={styles.topologySummaryMetric}>
                          <Typography.Text className={styles.workspaceSectionEyebrow}>
                            Peers
                          </Typography.Text>
                          <strong>{activeOrganizationOfficialPeers.length || activeOrganizationRuntimePeers.length}</strong>
                          <span>
                            {t(
                              'instâncias conhecidas nesta organização',
                              'instances known in this organization'
                            )}
                          </span>
                        </div>
                        <div className={styles.topologySummaryMetric}>
                          <Typography.Text className={styles.workspaceSectionEyebrow}>
                            Orderers
                          </Typography.Text>
                          <strong>{activeOrganizationOfficialOrderers.length || activeOrganizationRuntimeOrderers.length}</strong>
                          <span>{t('nós de consenso disponíveis', 'available consensus nodes')}</span>
                        </div>
                        <div className={styles.topologySummaryMetric}>
                          <Typography.Text className={styles.workspaceSectionEyebrow}>
                            Channels
                          </Typography.Text>
                          <strong>{activeOrganizationOfficialChannels.length}</strong>
                          <span>
                            {t(
                              'canais oficiais vinculados ao business scope',
                              'official channels linked to the business scope'
                            )}
                          </span>
                        </div>
                        <div className={styles.topologySummaryMetric}>
                          <Typography.Text className={styles.workspaceSectionEyebrow}>
                            Chaincodes
                          </Typography.Text>
                          <strong>{activeOrganizationOfficialChaincodes.length}</strong>
                          <span>
                            {t(
                              'catálogo ativo associado aos canais',
                              'active catalog associated with the channels'
                            )}
                          </span>
                        </div>
                      </div>
                    </Card>
                  </div>
                </div>
              </Spin>
            </div>
          )}
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-peers-window"
          eyebrow={t('Malha Fabric', 'Fabric mesh')}
          title={
            activeOrganization
              ? t('Peers oficiais: {name}', 'Official peers: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Peers oficiais', 'Official peers')
          }
          onClose={() => handleCloseWorkspaceWindow('peers')}
          open={Boolean(activeOrganization && workspaceWindows.peers)}
          preferredWidth="58vw"
          preferredHeight="70vh"
        >
          {activeOrganizationOfficialPeers.length > 0 ? (
            <div className={styles.topologyNodeList}>
              {activeOrganizationOfficialPeers.map(item => (
                <button
                  key={item.key}
                  type="button"
                  className={styles.topologyNode}
                  disabled={
                    !activeOrganizationVmAccessGranted ||
                    (!activeOrganizationFallbackMeta && !item.componentId)
                  }
                  aria-disabled={!activeOrganizationVmAccessGranted || (!activeOrganizationFallbackMeta && !item.componentId)}
                  onClick={() => handleOpenOfficialInspection(item)}
                >
                  <div>
                    <Typography.Text strong>{item.componentName}</Typography.Text>
                    <Typography.Text className={styles.topologyNodeMeta}>
                      {`${resolveProtectedValue(
                        item.hostRef || t('host n/d', 'host n/a'),
                        activeOrganizationVmAccessGranted,
                        t('host protegido', 'protected host')
                      )} | ${item.channelId || t('sem channel', 'no channel')}`}
                    </Typography.Text>
                  </div>
                  <Space wrap size={6}>
                    <Tag color={resolveOperationalCriticalityColor(item.criticality)}>
                      {item.criticality || 'critical'}
                    </Tag>
                    <Tag color={resolveRuntimeStatusTagColor(item.status)}>
                      {item.status || 'unknown'}
                    </Tag>
                  </Space>
                </button>
              ))}
            </div>
          ) : (
            <List
              dataSource={activeOrganizationRuntimePeers}
              locale={{ emptyText: t('Sem peers registrados', 'No peers registered') }}
              renderItem={peer => {
                const tone = resolveNodeStatusTone(peer.runtimeStatus);
                return (
                  <List.Item>
                    <div className={styles.nodeRow}>
                      <Typography.Text>{peer.nodeId || 'peer'}</Typography.Text>
                      <Tag color={tone.color}>{tone.label}</Tag>
                    </div>
                  </List.Item>
                );
              }}
            />
          )}
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-orderers-window"
          eyebrow={t('Malha Fabric', 'Fabric mesh')}
          title={
            activeOrganization
              ? t('Orderers oficiais: {name}', 'Official orderers: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Orderers oficiais', 'Official orderers')
          }
          onClose={() => handleCloseWorkspaceWindow('orderers')}
          open={Boolean(activeOrganization && workspaceWindows.orderers)}
          preferredWidth="58vw"
          preferredHeight="70vh"
        >
          {activeOrganizationOfficialOrderers.length > 0 ? (
            <div className={styles.topologyNodeList}>
              {activeOrganizationOfficialOrderers.map(item => (
                <button
                  key={item.key}
                  type="button"
                  className={styles.topologyNode}
                  disabled={
                    !activeOrganizationVmAccessGranted ||
                    (!activeOrganizationFallbackMeta && !item.componentId)
                  }
                  aria-disabled={!activeOrganizationVmAccessGranted || (!activeOrganizationFallbackMeta && !item.componentId)}
                  onClick={() => handleOpenOfficialInspection(item)}
                >
                  <div>
                    <Typography.Text strong>{item.componentName}</Typography.Text>
                    <Typography.Text className={styles.topologyNodeMeta}>
                      {`${resolveProtectedValue(
                        item.hostRef || t('host n/d', 'host n/a'),
                        activeOrganizationVmAccessGranted,
                        t('host protegido', 'protected host')
                      )} | ${t('consenso oficial', 'official consensus')}`}
                    </Typography.Text>
                  </div>
                  <Space wrap size={6}>
                    <Tag color={resolveOperationalCriticalityColor(item.criticality)}>
                      {item.criticality || 'critical'}
                    </Tag>
                    <Tag color={resolveRuntimeStatusTagColor(item.status)}>
                      {item.status || 'unknown'}
                    </Tag>
                  </Space>
                </button>
              ))}
            </div>
          ) : (
            <List
              dataSource={activeOrganizationRuntimeOrderers}
              locale={{ emptyText: t('Sem orderers registrados', 'No orderers registered') }}
              renderItem={orderer => {
                const tone = resolveNodeStatusTone(orderer.runtimeStatus);
                return (
                  <List.Item>
                    <div className={styles.nodeRow}>
                      <Typography.Text>{orderer.nodeId || 'orderer'}</Typography.Text>
                      <Tag color={tone.color}>{tone.label}</Tag>
                    </div>
                  </List.Item>
                );
              }}
            />
          )}
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-services-window"
          eyebrow={t('Serviços operacionais', 'Operational services')}
          title={
            activeOrganization
              ? t('Control plane e suporte: {name}', 'Control plane and support: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Control plane e suporte', 'Control plane and support')
          }
          onClose={() => handleCloseWorkspaceWindow('services')}
          open={Boolean(activeOrganization && workspaceWindows.services)}
          preferredWidth="64vw"
          preferredHeight="72vh"
        >
          <div className={styles.workspaceDetailStack}>
            <Card size="small" className={styles.topologyFamilyCard}>
              <div className={styles.topologyFamilyHeader}>
                <div>
                  <Typography.Text className={styles.workspaceSectionEyebrow}>
                    {t('Control plane', 'Control plane')}
                  </Typography.Text>
                  <Typography.Title level={5} className={styles.topologyFamilyTitle}>
                    {t('Identidade, gateway e APIs', 'Identity, gateway, and APIs')}
                  </Typography.Title>
                </div>
                <Tag color="blue">
                  {t('{count} itens', '{count} items', {
                    count: activeOrganizationOfficialControlPlane.length,
                  })}
                </Tag>
              </div>
              <div className={styles.topologyNodeList}>
                {activeOrganizationOfficialControlPlane.map(item => (
                  <button
                    key={item.key}
                    type="button"
                    className={styles.topologyNode}
                    disabled={
                      !activeOrganizationVmAccessGranted ||
                      (!activeOrganizationFallbackMeta && !item.componentId)
                    }
                    aria-disabled={!activeOrganizationVmAccessGranted || (!activeOrganizationFallbackMeta && !item.componentId)}
                    onClick={() => handleOpenOfficialInspection(item)}
                  >
                    <div>
                      <Typography.Text strong>{item.componentName}</Typography.Text>
                      <Typography.Text className={styles.topologyNodeMeta}>
                        {`${item.componentTypeLabel} | ${resolveProtectedValue(
                          item.hostRef || t('host n/d', 'host n/a'),
                          activeOrganizationVmAccessGranted,
                          t('host protegido', 'protected host')
                        )}`}
                      </Typography.Text>
                    </div>
                    <Space wrap size={6}>
                      <Tag color={resolveOperationalCriticalityColor(item.criticality)}>
                        {item.criticality || 'supporting'}
                      </Tag>
                      <Tag color={resolveRuntimeStatusTagColor(item.status)}>
                        {item.status || 'unknown'}
                      </Tag>
                    </Space>
                  </button>
                ))}
              </div>
            </Card>

            <Card size="small" className={styles.topologyFamilyCard}>
              <div className={styles.topologyFamilyHeader}>
                <div>
                  <Typography.Text className={styles.workspaceSectionEyebrow}>
                    {t('Serviços de suporte', 'Support services')}
                  </Typography.Text>
                  <Typography.Title level={5} className={styles.topologyFamilyTitle}>
                    {t('Persistência e runtime', 'Persistence and runtime')}
                  </Typography.Title>
                </div>
                <Tag color="gold">
                  {t('{count} serviços', '{count} services', {
                    count: activeOrganizationOfficialSupportServices.length,
                  })}
                </Tag>
              </div>
              <div className={styles.topologyNodeList}>
                {activeOrganizationOfficialSupportServices.map(item => (
                  <button
                    key={item.key}
                    type="button"
                    className={styles.topologyNode}
                    disabled={
                      !activeOrganizationVmAccessGranted ||
                      (!activeOrganizationFallbackMeta && !item.componentId)
                    }
                    aria-disabled={!activeOrganizationVmAccessGranted || (!activeOrganizationFallbackMeta && !item.componentId)}
                    onClick={() => handleOpenOfficialInspection(item)}
                  >
                    <div>
                      <Typography.Text strong>{item.componentName}</Typography.Text>
                      <Typography.Text className={styles.topologyNodeMeta}>
                        {`${item.componentTypeLabel} | ${resolveProtectedValue(
                          item.hostRef || t('host n/d', 'host n/a'),
                          activeOrganizationVmAccessGranted,
                          t('host protegido', 'protected host')
                        )}`}
                      </Typography.Text>
                    </div>
                    <Space wrap size={6}>
                      <Tag color={resolveOperationalCriticalityColor(item.criticality)}>
                        {item.criticality || 'supporting'}
                      </Tag>
                      <Tag color={resolveRuntimeStatusTagColor(item.status)}>
                        {item.status || 'unknown'}
                      </Tag>
                    </Space>
                  </button>
                ))}
              </div>
            </Card>
          </div>
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-vm-access-window"
          eyebrow={t('Seguranca operacional', 'Operational security')}
          title={
            activeOrganization
              ? t('Acesso às VMs: {name}', 'VM access: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Acesso às VMs', 'VM access')
          }
          onClose={() => setVmAccessWindowOpen(false)}
          open={Boolean(activeOrganization && vmAccessWindowOpen)}
          preferredWidth="56vw"
          preferredHeight="74vh"
        >
          {activeOrganization ? (
            <div className={styles.vmAccessWindowBody}>
              <Alert
                showIcon
                type="info"
                message={t('Validação manual por VM', 'Manual validation per VM')}
                description={t(
                  'Para liberar esta organizacao, preencha e valide as credenciais de {count} VM(s). O acesso sensivel so abre quando todas coincidirem com o provisionamento.',
                  'To release this organization, fill in and validate the credentials for {count} VM(s). Sensitive access only opens when all of them match the provisioning data.',
                  { count: activeOrganizationVmAccessTargets.length }
                )}
              />
              <div className={styles.vmAccessSummary}>
                <div className={styles.vmAccessSummaryRow}>
                  <span>{t('Organização', 'Organization')}</span>
                  <strong>{activeOrganization.orgName || activeOrganization.orgId}</strong>
                </div>
                <div className={styles.vmAccessSummaryRow}>
                  <span>{t('VMs conhecidas', 'Known VMs')}</span>
                  <strong>{activeOrganizationVmAccessTargets.length}</strong>
                </div>
                <div className={styles.vmAccessSummaryRow}>
                  <span>{t('Regra de liberação', 'Release rule')}</span>
                  <strong>
                    {t(
                      'todas as VMs precisam ser validadas',
                      'all VMs must be validated'
                    )}
                  </strong>
                </div>
              </div>
              {activeOrganizationVmAccessTargets.length > 0 ? (
                <div className={styles.vmAccessMachineGrid}>
                  {activeOrganizationVmAccessTargets.map(target => {
                    const draftEntry =
                      safeArray(vmAccessDraft.machineEntries).find(
                        entry => normalizeText(entry && entry.key) === normalizeText(target.key)
                      ) || createVmAccessDraftEntry(target);
                    const requiresManagedReference =
                      Boolean(normalizeText(target.credentialRef)) &&
                      !resolvePemFileNameFromCredentialRef(target.credentialRef) &&
                      !normalizeText(target.credentialFingerprint);
                    const machineReady =
                      Boolean(normalizeText(draftEntry.hostAddress)) &&
                      Boolean(normalizeText(draftEntry.sshUser)) &&
                      Boolean(toSafePositiveInt(draftEntry.sshPort)) &&
                      Boolean(
                        normalizeText(draftEntry.pemFileName) ||
                          normalizeText(draftEntry.credentialRef)
                      );

                    return (
                      <Card key={target.key} size="small" className={styles.vmAccessMachineCard}>
                        <div className={styles.vmAccessMachineHeader}>
                          <div>
                            <Typography.Text className={styles.workspaceSectionEyebrow}>
                              {t('Credenciais obrigatorias', 'Required credentials')}
                            </Typography.Text>
                            <Typography.Title level={5} className={styles.vmAccessMachineTitle}>
                              {target.label}
                            </Typography.Title>
                          </div>
                          <Tag color={machineReady ? 'blue' : 'default'}>
                            {machineReady ? t('preenchida', 'filled') : t('pendente', 'pending')}
                          </Tag>
                        </div>
                        <div className={styles.vmAccessMachineFields}>
                          <div className={styles.vmAccessField}>
                            <Typography.Text strong>HOST_ADDRESS</Typography.Text>
                            <Input
                              value={draftEntry.hostAddress}
                              onChange={event =>
                                handleChangeVmAccessDraftEntry(target.key, {
                                  hostAddress: event.target.value,
                                })
                              }
                              placeholder="ex.: 203.0.113.10"
                            />
                          </div>
                          <div className={styles.vmAccessField}>
                            <Typography.Text strong>SSH_USER</Typography.Text>
                            <Input
                              value={draftEntry.sshUser}
                              onChange={event =>
                                handleChangeVmAccessDraftEntry(target.key, {
                                  sshUser: event.target.value,
                                })
                              }
                              placeholder="ex.: operador-linux"
                            />
                          </div>
                          <div className={styles.vmAccessField}>
                            <Typography.Text strong>SSH_PORT</Typography.Text>
                            <InputNumber
                              min={1}
                              max={65535}
                              value={draftEntry.sshPort}
                              style={{ width: '100%' }}
                              onChange={value =>
                                handleChangeVmAccessDraftEntry(target.key, {
                                  sshPort: value || 22,
                                })
                              }
                            />
                          </div>
                          <div className={styles.vmAccessField}>
                            <Typography.Text strong>
                              {t('DOCKER_PORT (opcional)', 'DOCKER_PORT (optional)')}
                            </Typography.Text>
                            <InputNumber
                              min={1}
                              max={65535}
                              value={draftEntry.dockerPort}
                              style={{ width: '100%' }}
                              onChange={value =>
                                handleChangeVmAccessDraftEntry(target.key, {
                                  dockerPort: value || 2376,
                                })
                              }
                            />
                          </div>
                          <div className={styles.vmAccessField}>
                            <Typography.Text strong>
                              {t(
                                'Chave SSH por maquina (.pem local)',
                                'SSH key per machine (local .pem)'
                              )}
                            </Typography.Text>
                            <Space direction="vertical" style={{ width: '100%' }} size={6}>
                              <Upload
                                beforeUpload={() => false}
                                showUploadList={false}
                                maxCount={1}
                                accept=".pem"
                                onChange={info => handleSelectVmAccessPem(target.key, info)}
                              >
                                <Button size="small" icon={<UploadOutlined />}>
                                  {t('Selecionar chave privada (.pem)', 'Select private key (.pem)')}
                                </Button>
                              </Upload>
                              <Typography.Text className={styles.vmAccessUploadHint}>
                                {draftEntry.pemFileName ||
                                  t(
                                    'Obrigatorio quando a VM usa chave local .pem.',
                                    'Required when the VM uses a local .pem key.'
                                  )}
                              </Typography.Text>
                            </Space>
                          </div>
                          {requiresManagedReference ? (
                            <div className={styles.vmAccessField}>
                              <Typography.Text strong>CREDENTIAL_REF</Typography.Text>
                              <Input
                                value={draftEntry.credentialRef}
                                onChange={event =>
                                  handleChangeVmAccessDraftEntry(target.key, {
                                    credentialRef: event.target.value,
                                  })
                                }
                                placeholder={t(
                                  'Informe o credential_ref gerenciado desta VM',
                                  'Enter the managed credential_ref for this VM'
                                )}
                              />
                            </div>
                          ) : null}
                        </div>
                      </Card>
                    );
                  })}
                </div>
              ) : (
                <Empty
                  description={t(
                    'Sem VMs catalogadas para esta organizacao',
                    'No VMs cataloged for this organization'
                  )}
                />
              )}
              <div className={styles.vmAccessActions}>
                <Button onClick={() => setVmAccessWindowOpen(false)}>
                  {t('Cancelar', 'Cancel')}
                </Button>
                <Button
                  type="primary"
                  icon={<UnlockOutlined />}
                  disabled={activeOrganizationVmAccessTargets.length === 0}
                  onClick={handleGrantVmAccess}
                >
                  {t('Validar e liberar acesso', 'Validate and grant access')}
                </Button>
              </div>
            </div>
          ) : null}
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-channels-window"
          eyebrow={t('Escopo de negócio', 'Business scope')}
          title={
            activeOrganization
              ? t('Channels e business groups: {name}', 'Channels and business groups: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Channels e business groups', 'Channels and business groups')
          }
          onClose={() => handleCloseWorkspaceWindow('channels')}
          open={Boolean(activeOrganization && workspaceWindows.channels)}
          preferredWidth="70vw"
          preferredHeight="74vh"
        >
          {activeOrganizationBusinessScopeGroups.length > 0 ? (
            <div className={styles.businessScopeGrid}>
              {activeOrganizationBusinessScopeGroups.map(group => (
                <Card key={group.key} size="small" className={styles.businessGroupCard}>
                  <div className={styles.businessGroupHeader}>
                    <div>
                      <Typography.Text className={styles.workspaceSectionEyebrow}>
                        {t('Grupo de negócio', 'Business group')}
                      </Typography.Text>
                      <Typography.Title level={5} className={styles.businessGroupTitle}>
                        {group.name}
                      </Typography.Title>
                    </div>
                    {group.networkId ? <Tag color="purple">{group.networkId}</Tag> : null}
                  </div>

                  <div className={styles.businessChannelStack}>
                    {group.channels.map(channel => (
                      <div key={`${group.key}-${channel.channelId}`} className={styles.businessChannelCard}>
                        <div className={styles.businessChannelHeader}>
                          <div>
                            <Typography.Text strong>
                              {channel.name || channel.channelId || t('canal', 'channel')}
                            </Typography.Text>
                            <Typography.Text className={styles.topologyNodeMeta}>
                              {channel.memberOrgs.join(' | ') || t('membros n/d', 'members n/a')}
                            </Typography.Text>
                          </div>
                          <Space wrap size={6}>
                            <Tag color={resolveRuntimeStatusTagColor(channel.status)}>
                              {channel.status || 'unknown'}
                            </Tag>
                            <Button
                              size="small"
                              type="link"
                              onClick={() => handleOpenChannelWorkspace(channel.channelId)}
                            >
                              {t('Abrir canal', 'Open channel')}
                            </Button>
                          </Space>
                        </div>

                        <div className={styles.channelChaincodeRail}>
                          {channel.chaincodes.length > 0 ? (
                            channel.chaincodes.map(chaincode => (
                              <div
                                key={`${channel.channelId}-${chaincode.key}`}
                                className={styles.businessChaincodePill}
                              >
                                <strong>{chaincode.name || chaincode.chaincodeId}</strong>
                                <span>{chaincode.health || chaincode.status || 'unknown'}</span>
                                <Tag color={resolveRuntimeStatusTagColor(chaincode.status)}>
                                  {chaincode.status || 'unknown'}
                                </Tag>
                              </div>
                            ))
                          ) : (
                            <Tag>{t('Sem chaincodes ativos', 'No active chaincodes')}</Tag>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </Card>
              ))}
            </div>
          ) : (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={t(
                'Sem business groups e channels oficiais materializados neste snapshot',
                'No official business groups or channels materialized in this snapshot'
              )}
            />
          )}
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-chaincodes-window"
          eyebrow={t('Escopo de negócio', 'Business scope')}
          title={
            activeOrganization
              ? t('Chaincodes ativos: {name}', 'Active chaincodes: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Chaincodes ativos', 'Active chaincodes')
          }
          onClose={() => handleCloseWorkspaceWindow('chaincodes')}
          open={Boolean(activeOrganization && workspaceWindows.chaincodes)}
          preferredWidth="60vw"
          preferredHeight="70vh"
        >
          {activeOrganizationOfficialChaincodes.length > 0 ? (
            <div className={styles.chaincodeGrid}>
              {activeOrganizationOfficialChaincodes.map(chaincode => (
                <Card
                  key={chaincode.id || chaincode.name}
                  size="small"
                  className={styles.chaincodeCard}
                >
                  <Typography.Text strong>
                    {chaincode.name || chaincode.id || 'chaincode'}
                  </Typography.Text>
                  <Typography.Text className={styles.topologyNodeMeta}>
                    {(chaincode.channelRefs || []).join(' | ') ||
                      t('sem channel associado', 'no associated channel')}
                  </Typography.Text>
                  <div className={styles.chaincodeTagRow}>
                    <Tag color={chaincode.status === 'running' ? 'green' : 'default'}>
                      {chaincode.status || 'unknown'}
                    </Tag>
                    <Tag>{chaincode.health || t('health n/d', 'health n/a')}</Tag>
                    {chaincode.componentId && <Tag color="purple">{chaincode.componentId}</Tag>}
                  </div>
                </Card>
              ))}
            </div>
          ) : (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={t(
                'Sem chaincodes oficiais ativos neste snapshot',
                'No official chaincodes active in this snapshot'
              )}
            />
          )}
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-audit-window"
          eyebrow={t('Auditoria operacional', 'Operational audit')}
          title={
            activeOrganization
              ? t('Evidências e propostas: {name}', 'Evidence and proposals: {name}', {
                  name: activeOrganization.orgName,
                })
              : t('Evidências e propostas', 'Evidence and proposals')
          }
          onClose={() => handleCloseWorkspaceWindow('audit')}
          open={Boolean(activeOrganization && workspaceWindows.audit)}
          preferredWidth="66vw"
          preferredHeight="74vh"
        >
          <div className={styles.workspaceDetailStack}>
            <Card size="small" className={styles.workspaceSectionCard}>
              <Typography.Text className={styles.workspaceSectionEyebrow}>
                {t('Origem dos artefatos', 'Artifact origin')}
              </Typography.Text>
              {activeOrganizationArtifactOrigins.length > 0 ? (
                <div className={styles.workspaceArtifactList}>
                  {activeOrganizationArtifactOrigins.map(origin => (
                    <div
                      key={`${origin.artifact}-${origin.primaryToken}`}
                      className={styles.workspaceArtifactRow}
                    >
                      <span>{origin.artifact}</span>
                      <Tag color={origin.available ? 'green' : 'orange'}>
                        {origin.available ? 'available' : 'missing'}
                      </Tag>
                    </div>
                  ))}
                </div>
              ) : (
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description={t(
                    'Sem trilha oficial de artefatos neste snapshot',
                    'No official artifact trail in this snapshot'
                  )}
                />
              )}
            </Card>

            <Card size="small" className={styles.workspaceSectionCard}>
              <Typography.Text className={styles.workspaceSectionEyebrow}>
                {t('Propostas operacionais', 'Operational proposals')}
              </Typography.Text>
              <Typography.Paragraph className={styles.organizationDescription}>
                {t(
                  'Fila auditável das mudanças e reentradas associadas a esta organização.',
                  'Auditable queue of changes and re-entries associated with this organization.'
                )}
              </Typography.Paragraph>
              <List
                dataSource={activeOrganizationProposalEntries}
                renderItem={runHistoryEntry => {
                  const runTone = resolveExecutionTone(runHistoryEntry.status);
                  return (
                    <List.Item
                      actions={[
                        <Button
                          key={`${runHistoryEntry.key}-open`}
                          type="link"
                          onClick={() => handleOpenRunbookAudit(runHistoryEntry)}
                        >
                          {t('Abrir auditoria', 'Open audit')}
                        </Button>,
                      ]}
                    >
                      <List.Item.Meta
                        title={
                          <Space wrap>
                            <Typography.Text strong>{runHistoryEntry.runId || '-'}</Typography.Text>
                            <Tag color={runTone.color}>{runTone.label}</Tag>
                          </Space>
                        }
                        description={t(
                          'change_id {changeId} | fim UTC {finishedAt}',
                          'change_id {changeId} | end UTC {finishedAt}',
                          {
                            changeId: runHistoryEntry.changeId || '-',
                            finishedAt: runHistoryEntry.finishedAt || '-',
                          }
                        )}
                      />
                    </List.Item>
                  );
                }}
              />
            </Card>
          </div>
        </OperationalWindowDialog>

        <OperationalWindowDialog
          windowId="overview-organization-governed-action-window"
          eyebrow={t('Ação governada', 'Governed action')}
          title={
            activeGovernedActionConfig && activeOrganization
              ? `${activeGovernedActionConfig.title}: ${activeOrganization.orgName}`
              : t('Ação governada', 'Governed action')
          }
          onClose={() => setActiveGovernedAction('')}
          open={Boolean(activeOrganization && activeGovernedActionConfig)}
          preferredWidth="42vw"
          preferredHeight="42vh"
        >
          {activeGovernedActionConfig && activeOrganization && (
            <div className={styles.workspaceDetailStack}>
              <Alert
                showIcon
                type="info"
                message={activeGovernedActionConfig.summary}
                description={t(
                  'Esta ação agora abre como janela operacional própria. O fluxo backend ainda não está concluído, então a tela funciona como ponto de entrada governado e não como wizard final de execução.',
                  'This action now opens in its own operational window. The backend flow is not finished yet, so this screen works as a governed entry point rather than as the final execution wizard.'
                )}
              />
              <Card size="small" className={styles.workspaceSectionCard}>
                <div className={styles.workspaceSignalList}>
                  <div className={styles.workspaceSignalRow}>
                    <span>{t('Organização', 'Organization')}</span>
                    <strong>{activeOrganization.orgName || activeOrganization.orgId}</strong>
                  </div>
                  <div className={styles.workspaceSignalRow}>
                    <span>{t('run_id oficial', 'official run_id')}</span>
                    <Typography.Text code>{activeOrganization.latestRunId || '-'}</Typography.Text>
                  </div>
                  <div className={styles.workspaceSignalRow}>
                    <span>change_id</span>
                    <Typography.Text code>{activeOrganization.latestChangeId || '-'}</Typography.Text>
                  </div>
                </div>
              </Card>
              <Space wrap>
                <Button
                  type="primary"
                  disabled={!activeOrganizationVmAccessGranted}
                  onClick={handleOpenOfficialRuntimeTopology}
                >
                  {t('Abrir topologia runtime', 'Open runtime topology')}
                </Button>
                <Button
                  onClick={() =>
                    activeOrganizationRunHistory[0] &&
                    handleOpenRunbookAudit(activeOrganizationRunHistory[0])
                  }
                >
                  {t('Abrir auditoria', 'Open audit')}
                </Button>
              </Space>
            </div>
          )}
        </OperationalWindowDialog>

        <OfficialRuntimeInspectionDrawer
          inspectionState={inspectionState}
          inspectionScopeEntries={inspectionScopeEntries}
          onClose={closeInspection}
          onRefresh={refreshInspection}
        />
      </div>
      </OperationalWindowManagerProvider>
    </PageHeaderWrapper>
  );
};

export default Overview;
