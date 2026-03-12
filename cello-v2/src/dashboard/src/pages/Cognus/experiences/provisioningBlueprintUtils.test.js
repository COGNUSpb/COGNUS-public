import {
  buildBlockingLintReport,
  buildLintArtifacts,
  buildLintDiagnosticDomains,
  calculateCanonicalFingerprint,
  createBlueprintDraftTemplate,
  lintBlueprintDraft,
  normalizeBackendLintReport,
  parseBlueprintDocument,
  serializeBlueprintDocument,
  summarizeBlueprintVersionDiff,
  validateBackendLintContract,
} from './provisioningBlueprintUtils';

describe('provisioningBlueprintUtils', () => {
  it('parses yaml payload and keeps canonical blocks', () => {
    const draft = createBlueprintDraftTemplate();
    const yamlPayload = serializeBlueprintDocument(draft, 'yaml');

    const parsed = parseBlueprintDocument(yamlPayload, 'yaml');

    expect(parsed.network.network_id).toBe('consortium-dev');
    expect(Array.isArray(parsed.orgs)).toBe(true);
    expect(parsed.environment_profile.compute_target).toBe('vm_linux');
  });

  it('validates default draft as publishable', () => {
    const draft = createBlueprintDraftTemplate();

    const lintReport = lintBlueprintDraft(draft);

    expect(lintReport.valid).toBe(true);
    expect(lintReport.errors).toHaveLength(0);
    expect(lintReport.summary.orgs).toBeGreaterThan(0);
  });

  it('blocks unsupported provider scope for environment and nodes', () => {
    const invalidDraft = createBlueprintDraftTemplate({
      environment_profile: {
        profile_id: 'dev-cloud',
        stage: 'dev',
        provider: 'aws',
        compute_target: 'vm_linux',
        os_family: 'linux',
      },
    });

    invalidDraft.nodes = invalidDraft.nodes.map(node => ({
      ...node,
      provider: 'aws',
    }));

    const lintReport = lintBlueprintDraft(invalidDraft);

    expect(lintReport.valid).toBe(false);
    expect(lintReport.errors.some(issue => issue.code === 'scope_provider_not_supported')).toBe(
      true
    );
    expect(lintReport.errors.some(issue => issue.code === 'node_provider_not_supported')).toBe(
      true
    );
  });

  it('summarizes diff between two blueprint versions', () => {
    const baseBlueprint = createBlueprintDraftTemplate();
    const targetBlueprint = createBlueprintDraftTemplate({
      blueprint_version: '1.1.0',
      nodes: [
        ...baseBlueprint.nodes,
        {
          node_id: 'peer1-infufg',
          org_id: 'infufg',
          node_type: 'peer',
          host_ref: 'vm-dev-03',
          provider: 'external',
          os_family: 'linux',
          ports: [9051, 9052],
        },
      ],
    });

    const diff = summarizeBlueprintVersionDiff(baseBlueprint, targetBlueprint);

    expect(diff.find(item => item.key === 'blueprint_version').changed).toBe(true);
    expect(diff.find(item => item.key === 'nodes').changed).toBe(true);
    expect(diff.find(item => item.key === 'network').changed).toBe(false);
  });

  it('generates deterministic fingerprint independent of key order', async () => {
    const draft = createBlueprintDraftTemplate();
    const reorderedDraft = {
      environment_profile: draft.environment_profile,
      policies: draft.policies,
      nodes: draft.nodes,
      channels: draft.channels,
      orgs: draft.orgs,
      network: draft.network,
      updated_at: draft.updated_at,
      created_at: draft.created_at,
      blueprint_version: draft.blueprint_version,
      schema_version: draft.schema_version,
      schema_name: draft.schema_name,
    };

    const firstFingerprint = await calculateCanonicalFingerprint(draft);
    const secondFingerprint = await calculateCanonicalFingerprint(reorderedDraft);

    expect(firstFingerprint).toBe(secondFingerprint);
    expect(firstFingerprint).toHaveLength(64);
  });

  it('validates backend lint contract shape and normalizes report', () => {
    const backendReport = {
      valid: true,
      errors: [],
      warnings: [],
      hints: [],
      schema_name: 'cognus-blueprint',
      schema_version: '1.0.0',
      blueprint_version: '1.0.0',
      created_at: '2026-02-16T10:00:00Z',
      updated_at: '2026-02-16T10:05:00Z',
      schema_runtime: '1.0.0',
      resolved_schema_version: '1.0.0',
      fingerprint_sha256: 'a4c13f2dcf15c8d4f9a0b1e2c3d4f501a4c13f2dcf15c8d4f9a0b1e2c3d4f501',
      normalized_orgs: [{ org_id: 'infufg' }],
      normalized_channels: [{ channel_id: 'ops-channel' }],
      normalized_nodes: [{ node_id: 'peer0-infufg' }],
      normalized_policies: [{ policy_id: 'policy-channel-ops' }],
      normalized_environment_profile: { profile_id: 'dev-external-linux' },
      normalized_identity_baseline: { ca_profile: 'ca-infufg' },
    };

    const contractValidation = validateBackendLintContract(backendReport);
    const normalized = normalizeBackendLintReport(backendReport);

    expect(contractValidation.valid).toBe(true);
    expect(normalized.contractValid).toBe(true);
    expect(normalized.report.valid).toBe(true);
    expect(normalized.report.summary.nodes).toBe(1);
    expect(normalized.report.normalizedBlueprint.channels[0].channel_id).toBe('ops-channel');
  });

  it('forces blocking when backend returns malformed issue item in warning bucket', () => {
    const backendReport = {
      valid: true,
      errors: [],
      warnings: ['invalid-item'],
      hints: [],
      schema_name: 'cognus-blueprint',
      schema_version: '1.0.0',
      blueprint_version: '1.0.0',
      created_at: '2026-02-16T10:00:00Z',
      updated_at: '2026-02-16T10:05:00Z',
      schema_runtime: '1.0.0',
      resolved_schema_version: '1.0.0',
      fingerprint_sha256: 'a4c13f2dcf15c8d4f9a0b1e2c3d4f501a4c13f2dcf15c8d4f9a0b1e2c3d4f501',
      normalized_orgs: [{ org_id: 'infufg' }],
      normalized_channels: [{ channel_id: 'ops-channel' }],
      normalized_nodes: [{ node_id: 'peer0-infufg' }],
      normalized_policies: [{ policy_id: 'policy-channel-ops' }],
      normalized_environment_profile: { profile_id: 'dev-external-linux' },
      normalized_identity_baseline: { ca_profile: 'ca-infufg' },
    };

    const normalized = normalizeBackendLintReport(backendReport);

    expect(normalized.contractValid).toBe(true);
    expect(normalized.report.valid).toBe(false);
    expect(
      normalized.report.errors.some(issue => issue.code === 'invalid_lint_contract_issue_item')
    ).toBe(true);
  });

  it('creates blocking report when lint contract is unavailable', () => {
    const baseReport = lintBlueprintDraft(createBlueprintDraftTemplate());
    const blocked = buildBlockingLintReport(baseReport, {
      code: 'lint_backend_unavailable',
      message: 'Backend de lint indisponível.',
    });

    expect(blocked.valid).toBe(false);
    expect(blocked.contractValid).toBe(false);
    expect(blocked.errors.some(issue => issue.code === 'lint_backend_unavailable')).toBe(true);
  });

  it('builds diagnostic domains and artifacts for report download', () => {
    const base = buildBlockingLintReport(lintBlueprintDraft(createBlueprintDraftTemplate()), {
      code: 'lint_contract_invalid',
      message: 'Contrato inválido.',
    });

    const domains = buildLintDiagnosticDomains(base);
    const artifacts = buildLintArtifacts({
      ...base,
      schemaVersion: '1.0.0',
      resolvedSchemaVersion: '1.0.0',
      blueprintVersion: '1.0.0',
      fingerprint: 'b4c13f2dcf15c8d4f9a0b1e2c3d4f501b4c13f2dcf15c8d4f9a0b1e2c3d4f501',
      normalizedBlueprint: createBlueprintDraftTemplate(),
    });

    expect(domains.find(domain => domain.key === 'contract').count).toBeGreaterThan(0);
    expect(artifacts.reportPayload.contract_valid).toBe(false);
    expect(artifacts.normalizedBlueprintPayload.environment_profile).toBeTruthy();
  });
});
