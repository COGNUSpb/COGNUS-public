import {
  formatCognusTemplate,
  pickCognusText,
  resolveCognusLocale,
} from '../cognusI18n';

export const PROVISIONING_INFRA_PROVIDER_KEY = 'external-linux';
export const PROVISIONING_SSH_AUTH_METHOD = 'key';
export const DEFAULT_DOCKER_PORT = 2376;
export const PREFLIGHT_HOST_STATUS = Object.freeze({
  apto: 'apto',
  parcial: 'parcial',
  bloqueado: 'bloqueado',
});

const IPV4_REGEX = /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$/;
const HOSTNAME_REGEX = /^(?=.{1,253}$)(?!-)[a-zA-Z0-9-]{1,63}(?<!-)(\.(?!-)[a-zA-Z0-9-]{1,63}(?<!-))*$/;
const SSH_USER_REGEX = /^[a-z_][a-z0-9_-]{0,31}$/;
const SECURE_REFERENCE_PREFIX_REGEX = /^(vault|secret|ref|ssm|kms|env|keyring):\/\/[^\s]+$/i;
const SECURE_REFERENCE_SHORT_REF_REGEX = /^ref:[a-z0-9][a-z0-9/_-]*$/i;
const LOCAL_FILE_REFERENCE_REGEX = /^local-file:[^\s]+$/i;
const REQUIRED_RUNTIME = Object.freeze({
  osFamily: 'linux',
  minCpuCores: 2,
  minMemoryMb: 4096,
  minDiskGb: 40,
});
const REQUIRED_TOOLS = Object.freeze({
  critical: ['bash', 'docker'],
  recommended: ['curl', 'jq'],
});

const localizeInfrastructureUtilsText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const formatInfrastructureUtilsText = (ptBR, enUS, values, localeCandidate) =>
  formatCognusTemplate(ptBR, enUS, values, localeCandidate || resolveCognusLocale());

const hashString = value => {
  const source = String(value || '');
  let hash = 0;

  for (let index = 0; index < source.length; index += 1) {
    hash = (hash * 31 + source.charCodeAt(index)) % 2147483647;
  }

  return Math.abs(hash);
};

const includesFlag = (hostAddress, token) =>
  String(hostAddress || '')
    .toLowerCase()
    .includes(String(token || '').toLowerCase());

const buildCheckResult = ({ code, status, observed, expected, cause, recommendation }) => ({
  code,
  status,
  observed,
  expected,
  cause,
  recommendation,
});

const resolveDockerPort = value => {
  const parsed = Number(value);
  if (Number.isInteger(parsed) && parsed > 0 && parsed <= 65535) {
    return parsed;
  }
  return DEFAULT_DOCKER_PORT;
};

const buildSimulatedActiveContainers = (hostAddress, seed) => {
  const normalizedHostAddress = String(hostAddress || '').toLowerCase();
  if (
    includesFlag(normalizedHostAddress, 'clean') ||
    includesFlag(normalizedHostAddress, 'empty')
  ) {
    return [];
  }

  const hasConflictFlag =
    includesFlag(normalizedHostAddress, 'busy') ||
    includesFlag(normalizedHostAddress, 'active') ||
    includesFlag(normalizedHostAddress, 'legacy') ||
    includesFlag(normalizedHostAddress, 'container');

  if (!hasConflictFlag) {
    return [];
  }

  const baselineContainers = ['peer0-legacy', 'orderer0-legacy', 'ca-legacy', 'ccapi-legacy'];
  const count = 1 + (seed % baselineContainers.length);
  return baselineContainers.slice(0, count);
};

const deriveRuntimeSnapshot = machine => {
  const hostAddress = String(machine.hostAddress || '')
    .trim()
    .toLowerCase();
  const resolvedDockerPort = resolveDockerPort(machine.dockerPort);
  const seed = hashString(
    `${hostAddress}|${machine.sshUser || ''}|${machine.sshPort || ''}|${resolvedDockerPort}`
  );

  const runtimeSnapshot = {
    osFamily: REQUIRED_RUNTIME.osFamily,
    cpuCores: 2 + (seed % 7),
    memoryMb: 4096 + (seed % 5) * 2048,
    diskGb: 80 + (seed % 9) * 20,
    availableTools: ['bash', 'docker', 'curl', 'jq'],
    portsReachable: {
      ssh: true,
      docker: true,
    },
    activeContainers: buildSimulatedActiveContainers(hostAddress, seed),
  };
  runtimeSnapshot.activeContainersCount = runtimeSnapshot.activeContainers.length;

  if (includesFlag(hostAddress, 'offline') || includesFlag(hostAddress, 'sshfail')) {
    runtimeSnapshot.portsReachable.ssh = false;
  }
  if (includesFlag(hostAddress, 'dockeroff') || includesFlag(hostAddress, 'dockerfail')) {
    runtimeSnapshot.portsReachable.docker = false;
  }
  if (includesFlag(hostAddress, 'lowcpu')) {
    runtimeSnapshot.cpuCores = 1;
  }
  if (includesFlag(hostAddress, 'lowmem')) {
    runtimeSnapshot.memoryMb = 2048;
  }
  if (includesFlag(hostAddress, 'lowdisk')) {
    runtimeSnapshot.diskGb = 20;
  }
  if (includesFlag(hostAddress, 'nojq')) {
    runtimeSnapshot.availableTools = runtimeSnapshot.availableTools.filter(tool => tool !== 'jq');
  }
  if (includesFlag(hostAddress, 'nocurl')) {
    runtimeSnapshot.availableTools = runtimeSnapshot.availableTools.filter(tool => tool !== 'curl');
  }
  if (includesFlag(hostAddress, 'nodocker')) {
    runtimeSnapshot.availableTools = runtimeSnapshot.availableTools.filter(
      tool => tool !== 'docker'
    );
  }
  if (includesFlag(hostAddress, 'nobash')) {
    runtimeSnapshot.availableTools = runtimeSnapshot.availableTools.filter(tool => tool !== 'bash');
  }

  return runtimeSnapshot;
};

const computeHostStatus = checks => {
  const hasFail = checks.some(check => check.status === 'fail');
  if (hasFail) {
    return PREFLIGHT_HOST_STATUS.bloqueado;
  }

  const hasWarn = checks.some(check => check.status === 'warn');
  if (hasWarn) {
    return PREFLIGHT_HOST_STATUS.parcial;
  }

  return PREFLIGHT_HOST_STATUS.apto;
};

const summarizeHostDiagnostics = checks => {
  const failures = checks.filter(check => check.status === 'fail');
  const warnings = checks.filter(check => check.status === 'warn');

  return {
    failures,
    warnings,
    primaryCause: failures.length > 0 ? failures[0].cause : warnings[0]?.cause || '',
    primaryRecommendation:
      failures.length > 0
        ? failures[0].recommendation
        : warnings[0]?.recommendation ||
          localizeInfrastructureUtilsText('Sem ação imediata.', 'No immediate action.'),
  };
};

export const isValidPort = value => Number.isInteger(value) && value > 0 && value <= 65535;

export const normalizeHostAddress = value =>
  String(value || '')
    .trim()
    .toLowerCase();

const normalizeCredentialValue = value => String(value || '').trim();
const resolveMachineTokens = machine => {
  const machineId = normalizeCredentialValue(machine && machine.id);
  const infraLabel = normalizeCredentialValue(machine && machine.infraLabel);
  return [machineId, infraLabel].filter(Boolean);
};

export const isValidHostAddress = value => {
  const normalized = normalizeHostAddress(value);
  if (!normalized) {
    return false;
  }

  return IPV4_REGEX.test(normalized) || HOSTNAME_REGEX.test(normalized);
};

export const isValidSshUser = value => SSH_USER_REGEX.test(String(value || '').trim());

export const containsPrivateKeyMaterial = value =>
  /-----BEGIN\s+[^-]*PRIVATE KEY-----/i.test(String(value || ''));

export const isSecureSecretReference = (value, { allowLocalFile = false } = {}) => {
  const normalized = String(value || '').trim();
  if (!normalized || /\s/.test(normalized) || containsPrivateKeyMaterial(normalized)) {
    return false;
  }

  if (allowLocalFile && LOCAL_FILE_REFERENCE_REGEX.test(normalized)) {
    return true;
  }

  return (
    SECURE_REFERENCE_PREFIX_REGEX.test(normalized) ||
    SECURE_REFERENCE_SHORT_REF_REGEX.test(normalized)
  );
};

export const getMachineConnectionIssues = (machine, index) => {
  const issues = [];
  const machineLabel = String(machine.infraLabel || machine.id || `machine-${index + 1}`).trim();
  const hostAddress = String(machine.hostAddress || '').trim();
  const sshUser = String(machine.sshUser || '').trim();
  const dockerPortRaw = machine ? machine.dockerPort : null;
  const dockerPortProvided =
    dockerPortRaw !== null && dockerPortRaw !== undefined && String(dockerPortRaw).trim() !== '';
  const authMethod = String(machine.authMethod || '')
    .trim()
    .toLowerCase();

  if (!hostAddress) {
    issues.push(
      formatInfrastructureUtilsText(
        'host_address obrigatório para {machineLabel}.',
        'host_address is required for {machineLabel}.',
        { machineLabel }
      )
    );
  } else if (!isValidHostAddress(hostAddress)) {
    issues.push(
      formatInfrastructureUtilsText(
        'host_address inválido para {machineLabel}. Use IPv4 ou FQDN válido.',
        'Invalid host_address for {machineLabel}. Use a valid IPv4 or FQDN.',
        { machineLabel }
      )
    );
  }

  if (!sshUser) {
    issues.push(
      formatInfrastructureUtilsText(
        'ssh_user obrigatório para {machineLabel}.',
        'ssh_user is required for {machineLabel}.',
        { machineLabel }
      )
    );
  } else if (!isValidSshUser(sshUser)) {
    issues.push(
      formatInfrastructureUtilsText(
        'ssh_user inválido para {machineLabel}. Use padrão Linux (ex.: operador, ubuntu, root).',
        'Invalid ssh_user for {machineLabel}. Use a Linux user pattern (for example: operator, ubuntu, root).',
        { machineLabel }
      )
    );
  }

  if (!isValidPort(machine.sshPort)) {
    issues.push(
      formatInfrastructureUtilsText(
        'ssh_port inválido para {machineLabel}.',
        'Invalid ssh_port for {machineLabel}.',
        { machineLabel }
      )
    );
  }

  if (authMethod !== PROVISIONING_SSH_AUTH_METHOD) {
    issues.push(
      formatInfrastructureUtilsText(
        "auth_method inválido para {machineLabel}. Apenas 'key' é permitido.",
        "Invalid auth_method for {machineLabel}. Only 'key' is allowed.",
        { machineLabel }
      )
    );
  }

  if (dockerPortProvided && !isValidPort(Number(dockerPortRaw))) {
    issues.push(
      formatInfrastructureUtilsText(
        'docker_port inválido para {machineLabel}.',
        'Invalid docker_port for {machineLabel}.',
        { machineLabel }
      )
    );
  }

  return issues;
};

export const buildInfrastructureIssues = ({
  providerKey,
  changeId,
  privateKeyRef,
  machines,
  machineCredentials,
}) => {
  const issues = [];
  const normalizedPrivateKeyRef = String(privateKeyRef || '').trim();
  const machineList = Array.isArray(machines) ? machines : [];
  const credentialRows = Array.isArray(machineCredentials) ? machineCredentials : [];

  if (providerKey !== PROVISIONING_INFRA_PROVIDER_KEY) {
    issues.push(
      localizeInfrastructureUtilsText(
        'Provider inválido. Apenas external-linux é permitido neste fluxo.',
        'Invalid provider. Only external-linux is allowed in this flow.'
      )
    );
  }

  if (!String(changeId || '').trim()) {
    issues.push(
      localizeInfrastructureUtilsText(
        'change_id é obrigatório para manter rastreabilidade operacional.',
        'change_id is required to keep operational traceability.'
      )
    );
  }

  if (containsPrivateKeyMaterial(privateKeyRef)) {
    issues.push(
      localizeInfrastructureUtilsText(
        'private_key_ref deve conter apenas referência segura (nome/URI), nunca chave privada em texto puro.',
        'private_key_ref must contain only a secure reference (name/URI), never a plaintext private key.'
      )
    );
  } else if (
    normalizedPrivateKeyRef &&
    !isSecureSecretReference(normalizedPrivateKeyRef, { allowLocalFile: true })
  ) {
    issues.push(
      localizeInfrastructureUtilsText(
        'private_key_ref fora do padrão de referência segura. Use vault://, secret://, ref:// ou local-file:.',
        'private_key_ref is outside the secure reference pattern. Use vault://, secret://, ref://, or local-file:.'
      )
    );
  }

  if (machineList.length === 0) {
    issues.push(
      localizeInfrastructureUtilsText(
        'Ao menos uma VM Linux deve ser cadastrada.',
        'At least one Linux VM must be registered.'
      )
    );
    return issues;
  }

  const knownMachineIds = new Set(machineList.flatMap(machine => resolveMachineTokens(machine)));
  const credentialByMachine = credentialRows.reduce((accumulator, row) => {
    const machineId = normalizeCredentialValue(row && row.machine_id);
    if (!machineId) {
      return accumulator;
    }

    accumulator[machineId] = {
      machineId,
      credentialRef: normalizeCredentialValue(row && row.credential_ref),
      credentialFingerprint: normalizeCredentialValue(row && row.credential_fingerprint),
      credentialPayload: normalizeCredentialValue(row && row.credential_payload),
      reuseConfirmed: Boolean(row && row.reuse_confirmed),
    };
    return accumulator;
  }, {});

  credentialRows.forEach((row, index) => {
    const machineId = normalizeCredentialValue(row && row.machine_id);
    if (!machineId) {
      issues.push(
        formatInfrastructureUtilsText(
          'machine_id obrigatório em machine_credentials[{index}].',
          'machine_id is required in machine_credentials[{index}].',
          { index }
        )
      );
      return;
    }
    if (!knownMachineIds.has(machineId)) {
      issues.push(
        formatInfrastructureUtilsText(
          'machine_credentials[{index}] referencia machine_id desconhecido: {machineId}.',
          'machine_credentials[{index}] references an unknown machine_id: {machineId}.',
          { index, machineId }
        )
      );
    }
  });

  const seenHostAddresses = new Set();

  machineList.forEach((machine, index) => {
    const machineIssues = getMachineConnectionIssues(machine, index);
    issues.push(...machineIssues);

    const machineLabel = String(machine.infraLabel || machine.id || `machine-${index + 1}`).trim();
    const machineTokens = resolveMachineTokens(machine);
    const credentialBinding = machineTokens
      .map(token => credentialByMachine[token])
      .find(Boolean) || {
      machineId: machineTokens[0] || '',
      credentialRef: '',
      credentialFingerprint: '',
      reuseConfirmed: false,
    };

    const bindingRef = credentialBinding.credentialRef;
    const bindingFingerprint = credentialBinding.credentialFingerprint;
    const bindingPayload = credentialBinding.credentialPayload;

    if (!bindingRef && !bindingFingerprint) {
      issues.push(
        formatInfrastructureUtilsText(
          'Credencial SSH obrigatória para {machineLabel}.',
          'SSH credential is required for {machineLabel}.',
          { machineLabel }
        )
      );
    }

    if (containsPrivateKeyMaterial(bindingRef)) {
      issues.push(
        formatInfrastructureUtilsText(
          'credential_ref inválido para {machineLabel}: nunca envie chave privada em texto puro.',
          'Invalid credential_ref for {machineLabel}: never send a plaintext private key.',
          { machineLabel }
        )
      );
    } else if (bindingRef && !isSecureSecretReference(bindingRef, { allowLocalFile: true })) {
      issues.push(
        formatInfrastructureUtilsText(
          'credential_ref fora do padrão seguro para {machineLabel}. Use vault://, secret://, ref:// ou local-file:.',
          'credential_ref is outside the secure pattern for {machineLabel}. Use vault://, secret://, ref://, or local-file:.',
          { machineLabel }
        )
      );
    } else if (
      String(bindingRef || '')
        .trim()
        .toLowerCase()
        .startsWith('local-file:') &&
      !bindingPayload
    ) {
      issues.push(
        formatInfrastructureUtilsText(
          'credential_payload obrigatório para {machineLabel} quando usar credential_ref local-file:.',
          'credential_payload is required for {machineLabel} when using a local-file: credential_ref.',
          { machineLabel }
        )
      );
    }

    const normalizedHostAddress = normalizeHostAddress(machine.hostAddress);
    if (normalizedHostAddress) {
      if (seenHostAddresses.has(normalizedHostAddress)) {
        issues.push(
          formatInfrastructureUtilsText(
            'host_address duplicado para {machineLabel}: {hostAddress}.',
            'Duplicated host_address for {machineLabel}: {hostAddress}.',
            {
              machineLabel,
              hostAddress: normalizedHostAddress,
            }
          )
        );
      }
      seenHostAddresses.add(normalizedHostAddress);
    }
  });

  return issues;
};

const buildInvalidConnectionPreflight = ({
  machine,
  index,
  changeId,
  checkedAtUtc,
  credentialBinding,
}) => {
  const connectionIssues = getMachineConnectionIssues(machine, index);
  const machineLabel = String(machine.infraLabel || machine.id || `machine-${index + 1}`).trim();
  const resolvedDockerPort = resolveDockerPort(machine.dockerPort);

  const credentialRef = normalizeCredentialValue(
    credentialBinding && credentialBinding.credential_ref
  );
  const credentialFingerprint = normalizeCredentialValue(
    credentialBinding && credentialBinding.credential_fingerprint
  );
  if (!credentialRef && !credentialFingerprint) {
    connectionIssues.push(
      formatInfrastructureUtilsText(
        'Credencial SSH ausente para {machineLabel}.',
        'SSH credential is missing for {machineLabel}.',
        { machineLabel }
      )
    );
  }

  const checks = connectionIssues.map((issue, issueIndex) =>
    buildCheckResult({
      code: `preflight_connection_data_invalid_${issueIndex + 1}`,
      status: 'fail',
      observed: issue,
      expected: localizeInfrastructureUtilsText(
        'Dados mínimos de conexão SSH válidos para execução de preflight.',
        'Valid minimum SSH connection data for preflight execution.'
      ),
      cause: issue,
      recommendation: formatInfrastructureUtilsText(
        'Corrigir os campos obrigatórios de conexão de {machineLabel} e reexecutar o preflight.',
        'Correct the required connection fields for {machineLabel} and rerun the preflight.',
        { machineLabel }
      ),
    })
  );

  const diagnostics = summarizeHostDiagnostics(checks);

  return {
    id: machine.id,
    infraLabel: machineLabel,
    hostAddress: String(machine.hostAddress || '').trim(),
    sshUser: String(machine.sshUser || '').trim(),
    sshPort: machine.sshPort,
    dockerPort: resolvedDockerPort,
    changeId,
    checkedAtUtc,
    status: PREFLIGHT_HOST_STATUS.bloqueado,
    checks,
    failures: diagnostics.failures,
    warnings: diagnostics.warnings,
    primaryCause: diagnostics.primaryCause,
    primaryRecommendation: diagnostics.primaryRecommendation,
    credentialBinding: credentialBinding || null,
    runtimeSnapshot: null,
  };
};

const buildHostPreflight = ({ machine, index, changeId, checkedAtUtc, credentialBinding }) => {
  const connectionIssues = getMachineConnectionIssues(machine, index);
  const credentialRef = normalizeCredentialValue(
    credentialBinding && credentialBinding.credential_ref
  );
  const credentialFingerprint = normalizeCredentialValue(
    credentialBinding && credentialBinding.credential_fingerprint
  );
  if (connectionIssues.length > 0 || (!credentialRef && !credentialFingerprint)) {
    return buildInvalidConnectionPreflight({
      machine,
      index,
      changeId,
      checkedAtUtc,
      credentialBinding,
    });
  }

  const resolvedDockerPort = resolveDockerPort(machine.dockerPort);
  const runtimeSnapshot = deriveRuntimeSnapshot(machine);
  const checks = [];

  const sshConnectivityPass = runtimeSnapshot.portsReachable.ssh;
  checks.push(
    buildCheckResult({
      code: 'ssh_connectivity',
      status: sshConnectivityPass ? 'pass' : 'fail',
      observed: sshConnectivityPass
        ? formatInfrastructureUtilsText(
            'Conexão SSH disponível em {hostAddress}:{sshPort}',
            'SSH connection available at {hostAddress}:{sshPort}',
            {
              hostAddress: machine.hostAddress,
              sshPort: machine.sshPort,
            }
          )
        : formatInfrastructureUtilsText(
            'Falha de conectividade SSH em {hostAddress}:{sshPort}',
            'SSH connectivity failure at {hostAddress}:{sshPort}',
            {
              hostAddress: machine.hostAddress,
              sshPort: machine.sshPort,
            }
          ),
      expected: localizeInfrastructureUtilsText(
        'Conectividade SSH válida por host.',
        'Valid SSH connectivity per host.'
      ),
      cause: sshConnectivityPass
        ? localizeInfrastructureUtilsText(
            'Conectividade SSH validada com sucesso.',
            'SSH connectivity validated successfully.'
          )
        : localizeInfrastructureUtilsText(
            'Host sem conectividade SSH no endpoint informado.',
            'Host has no SSH connectivity at the informed endpoint.'
          ),
      recommendation: sshConnectivityPass
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Verificar rota de rede, firewall e serviço sshd antes de prosseguir.',
            'Check network routing, firewall, and the sshd service before proceeding.'
          ),
    })
  );

  const osLinuxPass =
    String(runtimeSnapshot.osFamily || '').toLowerCase() === REQUIRED_RUNTIME.osFamily;
  checks.push(
    buildCheckResult({
      code: 'runtime_linux',
      status: osLinuxPass ? 'pass' : 'fail',
      observed: formatInfrastructureUtilsText(
        'Sistema detectado: {osFamily}',
        'Detected system: {osFamily}',
        { osFamily: runtimeSnapshot.osFamily }
      ),
      expected: formatInfrastructureUtilsText(
        'Sistema operacional {osFamily}.',
        'Operating system {osFamily}.',
        { osFamily: REQUIRED_RUNTIME.osFamily }
      ),
      cause: osLinuxPass
        ? localizeInfrastructureUtilsText('Runtime Linux compatível.', 'Compatible Linux runtime.')
        : localizeInfrastructureUtilsText(
            'Runtime não Linux detectado para o host.',
            'A non-Linux runtime was detected for the host.'
          ),
      recommendation: osLinuxPass
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Substituir o host por VM Linux compatível com external-linux.',
            'Replace the host with a Linux VM compatible with external-linux.'
          ),
    })
  );

  const cpuPass = runtimeSnapshot.cpuCores >= REQUIRED_RUNTIME.minCpuCores;
  checks.push(
    buildCheckResult({
      code: 'runtime_cpu_minimum',
      status: cpuPass ? 'pass' : 'fail',
      observed: `${runtimeSnapshot.cpuCores} vCPU`,
      expected: `>= ${REQUIRED_RUNTIME.minCpuCores} vCPU`,
      cause: cpuPass
        ? localizeInfrastructureUtilsText(
            'CPU suficiente para provisão inicial.',
            'CPU is sufficient for initial provisioning.'
          )
        : localizeInfrastructureUtilsText(
            'CPU abaixo do mínimo operacional.',
            'CPU is below the operational minimum.'
          ),
      recommendation: cpuPass
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Aumentar vCPU da VM ou selecionar host com capacidade compatível.',
            'Increase the VM vCPU or select a host with compatible capacity.'
          ),
    })
  );

  const memoryPass = runtimeSnapshot.memoryMb >= REQUIRED_RUNTIME.minMemoryMb;
  checks.push(
    buildCheckResult({
      code: 'runtime_memory_minimum',
      status: memoryPass ? 'pass' : 'fail',
      observed: `${runtimeSnapshot.memoryMb} MB`,
      expected: `>= ${REQUIRED_RUNTIME.minMemoryMb} MB`,
      cause: memoryPass
        ? localizeInfrastructureUtilsText(
            'Memória suficiente para execução do runtime.',
            'Memory is sufficient for runtime execution.'
          )
        : localizeInfrastructureUtilsText(
            'Memória abaixo do baseline mínimo.',
            'Memory is below the minimum baseline.'
          ),
      recommendation: memoryPass
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Aumentar memória da VM antes do pipeline prepare/provision.',
            'Increase the VM memory before the prepare/provision pipeline.'
          ),
    })
  );

  const diskPass = runtimeSnapshot.diskGb >= REQUIRED_RUNTIME.minDiskGb;
  checks.push(
    buildCheckResult({
      code: 'runtime_disk_minimum',
      status: diskPass ? 'pass' : 'fail',
      observed: `${runtimeSnapshot.diskGb} GB`,
      expected: `>= ${REQUIRED_RUNTIME.minDiskGb} GB`,
      cause: diskPass
        ? localizeInfrastructureUtilsText(
            'Disco suficiente para baseline de artefatos.',
            'Disk is sufficient for the artifact baseline.'
          )
        : localizeInfrastructureUtilsText(
            'Disco abaixo do mínimo operacional.',
            'Disk is below the operational minimum.'
          ),
      recommendation: diskPass
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Expandir disco ou realocar host antes da publicação para execução.',
            'Expand the disk or reallocate the host before publishing for execution.'
          ),
    })
  );

  const sshPortReachable = runtimeSnapshot.portsReachable.ssh;
  checks.push(
    buildCheckResult({
      code: 'runtime_port_ssh',
      status: sshPortReachable ? 'pass' : 'fail',
      observed: sshPortReachable
        ? formatInfrastructureUtilsText(
            'Porta SSH {sshPort} acessível.',
            'SSH port {sshPort} is reachable.',
            { sshPort: machine.sshPort }
          )
        : formatInfrastructureUtilsText(
            'Porta SSH {sshPort} inacessível.',
            'SSH port {sshPort} is unreachable.',
            { sshPort: machine.sshPort }
          ),
      expected: localizeInfrastructureUtilsText(
        'Porta SSH operacional para automação.',
        'Operational SSH port for automation.'
      ),
      cause: sshPortReachable
        ? localizeInfrastructureUtilsText('Porta SSH operacional.', 'Operational SSH port.')
        : localizeInfrastructureUtilsText(
            'Porta SSH sem resposta no host.',
            'SSH port has no response on the host.'
          ),
      recommendation: sshPortReachable
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Liberar a porta SSH no firewall/NAT e validar daemon sshd.',
            'Open the SSH port in the firewall/NAT and validate the sshd daemon.'
          ),
    })
  );

  const dockerPortReachable = runtimeSnapshot.portsReachable.docker;
  checks.push(
    buildCheckResult({
      code: 'runtime_port_docker',
      status: dockerPortReachable ? 'pass' : 'fail',
      observed: dockerPortReachable
        ? formatInfrastructureUtilsText(
            'Porta Docker {dockerPort} acessível.',
            'Docker port {dockerPort} is reachable.',
            { dockerPort: resolvedDockerPort }
          )
        : formatInfrastructureUtilsText(
            'Porta Docker {dockerPort} inacessível.',
            'Docker port {dockerPort} is unreachable.',
            { dockerPort: resolvedDockerPort }
          ),
      expected: localizeInfrastructureUtilsText(
        'Porta Docker operacional para provisão assistida.',
        'Operational Docker port for assisted provisioning.'
      ),
      cause: dockerPortReachable
        ? localizeInfrastructureUtilsText('Porta Docker operacional.', 'Operational Docker port.')
        : localizeInfrastructureUtilsText(
            'Porta Docker sem resposta para o runtime previsto.',
            'Docker port has no response for the expected runtime.'
          ),
      recommendation: dockerPortReachable
        ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
        : localizeInfrastructureUtilsText(
            'Revisar bind/daemon do runtime de contêiner e política de rede da VM.',
            'Review the container runtime bind/daemon and the VM network policy.'
          ),
    })
  );

  const activeContainers = Array.isArray(runtimeSnapshot.activeContainers)
    ? runtimeSnapshot.activeContainers
    : [];
  const hasActiveContainers = activeContainers.length > 0;
  checks.push(
    buildCheckResult({
      code: 'runtime_docker_containers_idle',
      status: hasActiveContainers ? 'warn' : 'pass',
      observed: hasActiveContainers
        ? formatInfrastructureUtilsText(
            'Containers ativos detectados: {containers}',
            'Active containers detected: {containers}',
            { containers: activeContainers.join(', ') }
          )
        : localizeInfrastructureUtilsText(
            'Nenhum container ativo detectado.',
            'No active containers detected.'
          ),
      expected: localizeInfrastructureUtilsText(
        'Host sem containers ativos potencialmente conflitantes antes de nova provisão.',
        'Host without potentially conflicting active containers before a new provisioning.'
      ),
      cause: hasActiveContainers
        ? localizeInfrastructureUtilsText(
            'Existem containers ativos que podem ser sobrescritos/apagados por uma nova provisão.',
            'There are active containers that may be overwritten/removed by a new provisioning.'
          )
        : localizeInfrastructureUtilsText(
            'Host sem risco de sobrescrita por containers ativos.',
            'Host has no overwrite risk from active containers.'
          ),
      recommendation: hasActiveContainers
        ? localizeInfrastructureUtilsText(
            'Auditar e encerrar containers ativos antes de avançar para Organizations.',
            'Audit and stop active containers before advancing to Organizations.'
          )
        : localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.'),
    })
  );

  REQUIRED_TOOLS.critical.forEach(tool => {
    const installed = runtimeSnapshot.availableTools.includes(tool);
    checks.push(
      buildCheckResult({
        code: `runtime_tool_${tool}`,
        status: installed ? 'pass' : 'fail',
        observed: installed
          ? formatInfrastructureUtilsText('{tool} disponível', '{tool} available', { tool })
          : formatInfrastructureUtilsText('{tool} ausente', '{tool} missing', { tool }),
        expected: formatInfrastructureUtilsText(
          '{tool} instalado no host',
          '{tool} installed on the host',
          { tool }
        ),
        cause: installed
          ? formatInfrastructureUtilsText('{tool} instalado.', '{tool} installed.', { tool })
          : formatInfrastructureUtilsText(
              '{tool} não encontrado no host.',
              '{tool} was not found on the host.',
              { tool }
            ),
        recommendation: installed
          ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
          : formatInfrastructureUtilsText(
              'Instalar {tool} antes de executar prepare -> provision -> reconcile -> verify.',
              'Install {tool} before running prepare -> provision -> reconcile -> verify.',
              { tool }
            ),
      })
    );
  });

  REQUIRED_TOOLS.recommended.forEach(tool => {
    const installed = runtimeSnapshot.availableTools.includes(tool);
    checks.push(
      buildCheckResult({
        code: `runtime_tool_${tool}`,
        status: installed ? 'pass' : 'warn',
        observed: installed
          ? formatInfrastructureUtilsText('{tool} disponível', '{tool} available', { tool })
          : formatInfrastructureUtilsText('{tool} ausente', '{tool} missing', { tool }),
        expected: formatInfrastructureUtilsText(
          '{tool} disponível para diagnóstico e operação',
          '{tool} available for diagnostics and operation',
          { tool }
        ),
        cause: installed
          ? formatInfrastructureUtilsText('{tool} instalado.', '{tool} installed.', { tool })
          : formatInfrastructureUtilsText(
              '{tool} não está instalado; operação pode ficar limitada.',
              '{tool} is not installed; operation may become limited.',
              { tool }
            ),
        recommendation: installed
          ? localizeInfrastructureUtilsText('Sem ação necessária.', 'No action required.')
          : formatInfrastructureUtilsText(
              'Instalar {tool} para reduzir risco operacional e facilitar troubleshooting.',
              'Install {tool} to reduce operational risk and simplify troubleshooting.',
              { tool }
            ),
      })
    );
  });

  const orderedChecks = [...checks].sort((left, right) => left.code.localeCompare(right.code));
  const hostStatus = computeHostStatus(orderedChecks);
  const diagnostics = summarizeHostDiagnostics(orderedChecks);

  return {
    id: machine.id,
    infraLabel: String(machine.infraLabel || machine.id || `machine-${index + 1}`).trim(),
    hostAddress: String(machine.hostAddress || '').trim(),
    sshUser: String(machine.sshUser || '').trim(),
    sshPort: machine.sshPort,
    dockerPort: resolvedDockerPort,
    changeId,
    checkedAtUtc,
    status: hostStatus,
    checks: orderedChecks,
    failures: diagnostics.failures,
    warnings: diagnostics.warnings,
    primaryCause: diagnostics.primaryCause,
    primaryRecommendation: diagnostics.primaryRecommendation,
    credentialBinding: credentialBinding || null,
    runtimeSnapshot,
  };
};

export const runInfrastructurePreflight = ({
  changeId,
  machines,
  machineCredentials,
  executedAtUtc,
}) => {
  const normalizedChangeId = String(changeId || '').trim();
  const timestampUtc = String(executedAtUtc || '').trim() || new Date().toISOString();
  const credentialByMachine = (Array.isArray(machineCredentials) ? machineCredentials : []).reduce(
    (accumulator, row) => {
      const machineId = normalizeCredentialValue(row && row.machine_id);
      if (!machineId) {
        return accumulator;
      }

      accumulator[machineId] = {
        machine_id: machineId,
        credential_ref: normalizeCredentialValue(row && row.credential_ref),
        credential_payload: normalizeCredentialValue(row && row.credential_payload),
        credential_fingerprint: normalizeCredentialValue(row && row.credential_fingerprint),
        reuse_confirmed: Boolean(row && row.reuse_confirmed),
      };
      return accumulator;
    },
    {}
  );

  const hosts = (Array.isArray(machines) ? machines : []).map((machine, index) =>
    buildHostPreflight({
      machine,
      index,
      changeId: normalizedChangeId,
      checkedAtUtc: timestampUtc,
      credentialBinding:
        resolveMachineTokens(machine)
          .map(token => credentialByMachine[token])
          .find(Boolean) || null,
    })
  );

  const summary = hosts.reduce(
    (accumulator, host) => {
      accumulator.total += 1;
      accumulator[host.status] += 1;
      return accumulator;
    },
    {
      total: 0,
      apto: 0,
      parcial: 0,
      bloqueado: 0,
    }
  );

  let overallStatus = PREFLIGHT_HOST_STATUS.apto;
  if (summary.bloqueado > 0) {
    overallStatus = PREFLIGHT_HOST_STATUS.bloqueado;
  } else if (summary.parcial > 0) {
    overallStatus = PREFLIGHT_HOST_STATUS.parcial;
  }

  return {
    changeId: normalizedChangeId,
    executedAtUtc: timestampUtc,
    overallStatus,
    summary,
    hosts,
  };
};
