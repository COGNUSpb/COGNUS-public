import {
  A17_ACCEPTANCE_CRITERIA,
  buildA17AcceptanceEvidencePackage,
} from './provisioningA17AcceptanceMatrix';

describe('provisioningA17AcceptanceMatrix', () => {
  it('exposes required A1.7.9 acceptance criteria ids', () => {
    expect(A17_ACCEPTANCE_CRITERIA.map(item => item.id)).toEqual([
      'a17-api-contract-tests',
      'a17-e2e-official-flow',
      'a17-negative-endpoint-unavailable',
      'a17-negative-contract-preflight-lock-retry',
      'a17-no-local-success-operational',
      'a17-ci-contract-regression-gate',
    ]);
  });

  it('builds accepted package with official E2E flow and host activity proof', () => {
    const acceptance = buildA17AcceptanceEvidencePackage({
      generatedAtUtc: '2026-02-17T23:40:00Z',
      executor: 'qa.ops@ufg.br',
      changeId: 'cr-a17-qa-001',
      runId: 'run-a17-qa-001',
    });

    expect(acceptance.metadata.wp).toBe('A1.7');
    expect(acceptance.metadata.item).toBe('A1.7.9');
    expect(acceptance.summary.accepted).toBe(true);
    expect(acceptance.summary.passed).toBe(acceptance.summary.total);

    expect(acceptance.e2e_official_flow.flow).toEqual([
      'infra_ssh',
      'lint_oficial',
      'publish_oficial',
      'runbook_oficial',
      'inventario_oficial',
    ]);
    expect(acceptance.e2e_official_flow.host_activity_proof.ssh_probe_command).toContain('ssh -p');
    expect(acceptance.e2e_official_flow.host_activity_proof.remote_activity_marker).toBe(
      'COGNUS_PROVISION_RUNTIME_OK'
    );
    expect(acceptance.e2e_official_flow.host_activity_proof.expected_artifacts).toEqual(
      expect.arrayContaining([
        'pipeline-report.json',
        'inventory-final.json',
        'history.jsonl',
        'decision-trace.jsonl',
      ])
    );
  });

  it('covers mandatory negative scenarios and enforces non-local success events', () => {
    const acceptance = buildA17AcceptanceEvidencePackage();
    const negativeScenarioIds = acceptance.negative_scenarios.map(item => item.id);
    const negativeScenarioCodes = acceptance.negative_scenarios.map(item => item.expected_code);

    expect(negativeScenarioIds).toEqual(
      expect.arrayContaining([
        'endpoint_indisponivel',
        'contrato_invalido',
        'preflight_bloqueado',
        'lock_de_recurso',
        'retry_esgotado',
      ])
    );
    expect(negativeScenarioCodes).toEqual(
      expect.arrayContaining([
        'request_failed_or_unavailable',
        'runbook_invalid_transition',
        'runbook_runtime_check_failed',
        'runbook_resource_lock_conflict',
        'runbook_retry_no_pending_checkpoint',
      ])
    );

    const successEventCodes = acceptance.e2e_official_flow.official_events_success.map(
      event => event.code
    );
    expect(successEventCodes.some(code => String(code).includes('_local'))).toBe(false);
  });
});
