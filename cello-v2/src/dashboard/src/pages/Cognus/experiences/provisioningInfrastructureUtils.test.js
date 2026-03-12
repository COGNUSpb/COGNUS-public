import {
  buildInfrastructureIssues,
  containsPrivateKeyMaterial,
  getMachineConnectionIssues,
  isSecureSecretReference,
  PREFLIGHT_HOST_STATUS,
  PROVISIONING_INFRA_PROVIDER_KEY,
  PROVISIONING_SSH_AUTH_METHOD,
  runInfrastructurePreflight,
} from './provisioningInfrastructureUtils';

const buildMachine = overrides => ({
  id: 'machine-1',
  infraLabel: 'host-dev-01',
  hostAddress: '10.10.10.11',
  sshUser: 'web3',
  sshPort: 22,
  authMethod: PROVISIONING_SSH_AUTH_METHOD,
  dockerPort: 2376,
  ...overrides,
});

const buildMachineCredential = (machine, overrides = {}) => ({
  machine_id: machine.id,
  credential_ref: 'vault://infra/keys/web3-ufg',
  credential_fingerprint: '',
  reuse_confirmed: false,
  ...overrides,
});

describe('provisioningInfrastructureUtils', () => {
  it('accepts a valid external-linux host payload', () => {
    const machine = buildMachine();
    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-001',
      privateKeyRef: 'vault://infra/keys/web3-ufg',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(issues).toHaveLength(0);
  });

  it('blocks provider outside external-linux', () => {
    const machine = buildMachine();
    const issues = buildInfrastructureIssues({
      providerKey: 'aws',
      changeId: 'cr-2026-02-16-ssh-002',
      privateKeyRef: 'vault://infra/keys/web3-ufg',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(issues).toContain('Provider inválido. Apenas external-linux é permitido neste fluxo.');
  });

  it('detects private key material in plain text and blocks it', () => {
    const machine = buildMachine();
    expect(
      containsPrivateKeyMaterial(
        '-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----'
      )
    ).toBe(true);

    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-003',
      privateKeyRef: '-----BEGIN PRIVATE KEY-----SENSITIVE',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(
      issues.some(issue => issue.includes('private_key_ref deve conter apenas referência segura'))
    ).toBe(true);
  });

  it('accepts only secure reference formats for secret-like fields', () => {
    expect(isSecureSecretReference('vault://ca/org-alpha')).toBe(true);
    expect(isSecureSecretReference('secret://apigateway/org-alpha/token')).toBe(true);
    expect(isSecureSecretReference('ref://netapi/org-alpha/access')).toBe(true);
    expect(isSecureSecretReference('ref:org-alpha/credential')).toBe(true);
    expect(isSecureSecretReference('local-file:key.pem')).toBe(false);
    expect(isSecureSecretReference('local-file:key.pem', { allowLocalFile: true })).toBe(true);
    expect(isSecureSecretReference('super-secret-password')).toBe(false);
  });

  it('blocks private_key_ref when it is not a secure reference token', () => {
    const machine = buildMachine();
    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-003b',
      privateKeyRef: 'plain-password-value',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(issues).toContain(
      'private_key_ref fora do padrão de referência segura. Use vault://, secret://, ref:// ou local-file:.'
    );
  });

  it('requires mandatory host ssh fields and key auth method', () => {
    const machineIssues = getMachineConnectionIssues(
      buildMachine({
        hostAddress: '',
        sshUser: 'Invalid User',
        sshPort: 70000,
        authMethod: 'password',
      }),
      0
    );

    expect(machineIssues.some(issue => issue.includes('host_address obrigatório'))).toBe(true);
    expect(machineIssues.some(issue => issue.includes('ssh_user inválido'))).toBe(true);
    expect(machineIssues.some(issue => issue.includes('ssh_port inválido'))).toBe(true);
    expect(machineIssues.some(issue => issue.includes("Apenas 'key' é permitido"))).toBe(true);
  });

  it('rejects duplicate host_address entries across machines', () => {
    const machineA = buildMachine({
      id: 'machine-1',
      infraLabel: 'host-a',
      hostAddress: '10.10.10.11',
    });
    const machineB = buildMachine({
      id: 'machine-2',
      infraLabel: 'host-b',
      hostAddress: '10.10.10.11',
    });
    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-004',
      privateKeyRef: 'vault://infra/keys/web3-ufg',
      machines: [machineA, machineB],
      machineCredentials: [buildMachineCredential(machineA), buildMachineCredential(machineB)],
    });

    expect(issues.some(issue => issue.includes('host_address duplicado'))).toBe(true);
  });

  it('allows credential reuse across machines without explicit confirmation', () => {
    const machineA = buildMachine({ id: 'machine-1', infraLabel: 'host-a' });
    const machineB = buildMachine({
      id: 'machine-2',
      infraLabel: 'host-b',
      hostAddress: '10.10.10.12',
    });

    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-004b',
      privateKeyRef: '',
      machines: [machineA, machineB],
      machineCredentials: [
        buildMachineCredential(machineA, {
          credential_ref: 'vault://infra/keys/shared-key',
        }),
        buildMachineCredential(machineB, {
          credential_ref: 'vault://infra/keys/shared-key',
        }),
      ],
    });

    expect(
      issues.some(issue => issue.includes('Reuso de credencial SSH exige confirmação explícita'))
    ).toBe(false);
  });

  it('does not fallback to private_key_ref when machine credential is missing', () => {
    const machine = buildMachine({ id: 'machine-1', infraLabel: 'host-a' });
    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-004c',
      privateKeyRef: 'local-file:shared-default.pem',
      machines: [machine],
      machineCredentials: [],
    });

    expect(issues).toContain('Credencial SSH obrigatória para host-a.');
  });

  it('accepts local-file credential_ref when payload and fingerprint are present', () => {
    const machine = buildMachine({ id: 'machine-1', infraLabel: 'host-a' });
    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-004d',
      privateKeyRef: '',
      machines: [machine],
      machineCredentials: [
        buildMachineCredential(machine, {
          credential_ref: 'local-file:host-a.pem',
          credential_payload: 'YmFzZTY0LWRhZG8tZGUtdGVzdGU=',
          credential_fingerprint: 'sha256-host-a',
        }),
      ],
    });

    expect(issues).toHaveLength(0);
  });

  it('blocks local-file credential_ref when payload is missing', () => {
    const machine = buildMachine({ id: 'machine-1', infraLabel: 'host-a' });
    const issues = buildInfrastructureIssues({
      providerKey: PROVISIONING_INFRA_PROVIDER_KEY,
      changeId: 'cr-2026-02-16-ssh-004e',
      privateKeyRef: '',
      machines: [machine],
      machineCredentials: [
        buildMachineCredential(machine, {
          credential_ref: 'local-file:host-a.pem',
          credential_payload: '',
          credential_fingerprint: 'sha256-host-a',
        }),
      ],
    });

    expect(issues).toContain(
      'credential_payload obrigatório para host-a quando usar credential_ref local-file:.'
    );
  });

  it('generates apto preflight report with change_id and UTC correlation', () => {
    const machine = buildMachine({ hostAddress: '10.10.10.33' });
    const report = runInfrastructurePreflight({
      changeId: 'cr-2026-02-16-ssh-100',
      executedAtUtc: '2026-02-16T18:30:00Z',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(report.changeId).toBe('cr-2026-02-16-ssh-100');
    expect(report.executedAtUtc).toBe('2026-02-16T18:30:00Z');
    expect(report.overallStatus).toBe(PREFLIGHT_HOST_STATUS.apto);
    expect(report.summary.apto).toBe(1);
    expect(report.summary.parcial).toBe(0);
    expect(report.summary.bloqueado).toBe(0);
    expect(report.hosts[0].status).toBe(PREFLIGHT_HOST_STATUS.apto);
  });

  it('marks host as parcial when recommended base tool is missing', () => {
    const machine = buildMachine({ hostAddress: 'vm-dev-nojq.inf.ufg.br' });
    const report = runInfrastructurePreflight({
      changeId: 'cr-2026-02-16-ssh-101',
      executedAtUtc: '2026-02-16T18:31:00Z',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(report.overallStatus).toBe(PREFLIGHT_HOST_STATUS.parcial);
    expect(report.summary.parcial).toBe(1);
    expect(report.hosts[0].status).toBe(PREFLIGHT_HOST_STATUS.parcial);
    expect(report.hosts[0].primaryCause).toContain('não está instalado');
    expect(report.hosts[0].primaryRecommendation).toContain('Instalar');
  });

  it('marks host as bloqueado with technical cause and recommendation', () => {
    const machine = buildMachine({ hostAddress: 'vm-offline.inf.ufg.br' });
    const report = runInfrastructurePreflight({
      changeId: 'cr-2026-02-16-ssh-102',
      executedAtUtc: '2026-02-16T18:32:00Z',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(report.overallStatus).toBe(PREFLIGHT_HOST_STATUS.bloqueado);
    expect(report.summary.bloqueado).toBe(1);
    expect(report.hosts[0].status).toBe(PREFLIGHT_HOST_STATUS.bloqueado);
    expect(report.hosts[0].primaryCause).toContain('SSH');
    expect(report.hosts[0].primaryRecommendation).toContain('firewall');
  });

  it('blocks preflight when connection data is invalid before runtime checks', () => {
    const machine = buildMachine({ hostAddress: '', sshUser: '', authMethod: 'password' });
    const report = runInfrastructurePreflight({
      changeId: 'cr-2026-02-16-ssh-103',
      executedAtUtc: '2026-02-16T18:33:00Z',
      machines: [machine],
      machineCredentials: [buildMachineCredential(machine)],
    });

    expect(report.overallStatus).toBe(PREFLIGHT_HOST_STATUS.bloqueado);
    expect(report.hosts[0].checks[0].code).toContain('preflight_connection_data_invalid');
    expect(report.hosts[0].primaryRecommendation).toContain('Corrigir os campos obrigatórios');
  });
});
