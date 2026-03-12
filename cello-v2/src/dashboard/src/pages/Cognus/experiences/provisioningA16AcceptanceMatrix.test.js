import {
  A16_ACCEPTANCE_CRITERIA,
  A16_SCOPE_CONSTRAINTS,
  buildA16AcceptanceEvidencePackage,
} from './provisioningA16AcceptanceMatrix';

describe('provisioningA16AcceptanceMatrix', () => {
  it('keeps A1.6 scope constrained to external-linux flow', () => {
    expect(A16_SCOPE_CONSTRAINTS.providerKey).toBe('external-linux');
    expect(A16_SCOPE_CONSTRAINTS.provider).toBe('external');
    expect(A16_SCOPE_CONSTRAINTS.computeTarget).toBe('vm_linux');
    expect(A16_SCOPE_CONSTRAINTS.osFamily).toBe('linux');
    expect(A16_SCOPE_CONSTRAINTS.runbookProvider).toBe('external-linux');
  });

  it('exposes all expected acceptance criteria for item A1.6.7', () => {
    const criterionIds = A16_ACCEPTANCE_CRITERIA.map(item => item.id);
    expect(criterionIds).toEqual([
      'a16-forms-ssh-preflight',
      'a16-smoke-guided-flow',
      'a16-evidence-export',
      'a16-incremental-expansion',
      'a16-ux-a11y-responsive',
      'a16-ptbr-stable-codes',
    ]);
  });

  it('builds a passing acceptance package with preflight, smoke and incremental evidence', () => {
    const acceptance = buildA16AcceptanceEvidencePackage({
      generatedAtUtc: '2026-02-16T22:00:00Z',
      executor: 'qa.ops@ufg.br',
      changeIdPrefix: 'cr-a16-qa',
      runIdPrefix: 'run-a16-qa',
    });

    expect(acceptance.metadata.wp).toBe('A1.6');
    expect(acceptance.metadata.executor).toBe('qa.ops@ufg.br');
    expect(acceptance.metadata.generated_at_utc).toBe('2026-02-16T22:00:00Z');
    expect(acceptance.summary.accepted).toBe(true);
    expect(acceptance.summary.passed).toBe(acceptance.summary.total);

    expect(acceptance.guided_smoke.change_id).toBe('cr-a16-qa-acceptance');
    expect(acceptance.guided_smoke.run_id).toBe('run-a16-qa-acceptance');
    expect(acceptance.guided_smoke.lint_valid).toBe(true);
    expect(acceptance.guided_smoke.incremental_plan.length).toBeGreaterThan(0);
    expect(
      acceptance.guided_smoke.incremental_plan.every(
        operation =>
          operation.change_id === acceptance.guided_smoke.change_id &&
          operation.run_id === acceptance.guided_smoke.run_id
      )
    ).toBe(true);

    expect(acceptance.forms_and_preflight.preflight_apto.overallStatus).toBe('apto');
    expect(acceptance.forms_and_preflight.preflight_blocked.overallStatus).toBe('bloqueado');

    expect(
      acceptance.forms_and_preflight.preflight_permission_failure.hosts[0].checks[0].code
    ).toMatch(/^preflight_connection_data_invalid_/);

    expect(acceptance.guided_smoke.onboarding_handoff.preflight_approved).toBe(true);
    expect(acceptance.guided_smoke.onboarding_handoff.api_registry.length).toBeGreaterThan(0);
    expect(acceptance.guided_smoke.onboarding_handoff.incremental_expansions.length).toBe(1);

    expect(acceptance.ux_and_operational_language.accessibility.pass).toBe(true);
    expect(acceptance.ux_and_operational_language.responsiveness.pass).toBe(true);
    expect(acceptance.ux_and_operational_language.operationalLanguage.pass).toBe(true);
    expect(acceptance.ux_and_operational_language.stableFailureCodes.pass).toBe(true);
  });
});
