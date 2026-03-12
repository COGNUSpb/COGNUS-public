import {
  buildRedactedReference,
  sanitizeSensitiveText,
  sanitizeSensitiveValueByKey,
  sanitizeStructuredData,
} from './provisioningSecurityRedaction';

describe('provisioningSecurityRedaction', () => {
  it('redacts PEM blocks and bearer tokens from free text', () => {
    const sanitized = sanitizeSensitiveText(`
      BEGIN:
      -----BEGIN PRIVATE KEY-----
      abc123
      -----END PRIVATE KEY-----
      Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature
    `);

    expect(sanitized).not.toContain('abc123');
    expect(sanitized).not.toContain('payload.signature');
    expect(sanitized).toContain('[REDACTED_REF:pem:');
    expect(sanitized).toContain('[REDACTED_REF:bearer_token:');
  });

  it('redacts sensitive values by key while preserving fingerprints', () => {
    expect(sanitizeSensitiveValueByKey('password', 'admin123')).toMatch(
      /^\[REDACTED_REF:password:[0-9a-f]{12}\]$/
    );
    expect(sanitizeSensitiveValueByKey('private_key_ref', 'vault://ops/ssh-key')).toContain(
      'vault://[REDACTED_REF:private_key_ref:'
    );
    expect(sanitizeSensitiveValueByKey('manifest_fingerprint', 'ab'.repeat(32))).toBe(
      'ab'.repeat(32)
    );
  });

  it('sanitizes nested structures without dropping audit correlation fields', () => {
    const sanitized = sanitizeStructuredData({
      run_id: 'run-123',
      change_id: 'cr-123',
      machine_credentials: [
        {
          machine_id: 'machine-a',
          credential_payload: 'LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0t',
          credential_ref: 'secret://machine/a/key',
          credential_fingerprint: '12ab34cd56ef',
        },
      ],
      diagnostics: {
        message: 'password=admin123 token=abc123',
      },
    });

    expect(sanitized.run_id).toBe('run-123');
    expect(sanitized.change_id).toBe('cr-123');
    expect(typeof sanitized.machine_credentials).toBe('string');
    expect(sanitized.machine_credentials).toMatch(
      /^\[REDACTED_REF:machine_credentials:[0-9a-f]{12}\]$/
    );
    expect(sanitized.diagnostics.message).toContain('[REDACTED_REF:password:');
    expect(sanitized.diagnostics.message).toContain('[REDACTED_REF:token:');
  });

  it('builds deterministic redacted references', () => {
    const first = buildRedactedReference('vault://ops/key', 'credential_ref');
    const second = buildRedactedReference('vault://ops/key', 'credential_ref');
    expect(first).toBe(second);
  });
});
