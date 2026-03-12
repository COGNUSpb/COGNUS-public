import {
  A18_ACCEPTANCE_CRITERIA,
  A18_OUT_OF_SCOPE_DESTINATIONS,
  A18_REQUIRED_OUT_OF_SCOPE,
  buildA18AcceptanceEvidencePackage,
} from './provisioningA18AcceptanceMatrix';

describe('provisioningA18AcceptanceMatrix', () => {
  it('exposes required acceptance criteria ids for A1.8 final closure', () => {
    expect(A18_ACCEPTANCE_CRITERIA.map(item => item.id)).toEqual([
      'a18-wp-closure-a1-1-a1-7',
      'a18-official-external-linux-baseline',
      'a18-deterministic-gates-contracts',
      'a18-final-acceptance-traceability',
      'a18-final-evidence-package',
      'a18-transition-checklist-a1-a2',
      'a18-cross-document-conformity',
      'a18-container-operational-matrix-executable',
      'a18-a1-operational-allocation-non-isolated',
      'a18-final-wp-a1-8-acceptance-criteria',
      'a18-out-of-scope-explicit',
    ]);
  });

  it('builds accepted final package with strict external-linux official baseline', () => {
    const acceptance = buildA18AcceptanceEvidencePackage({
      generatedAtUtc: '2026-02-18T01:10:00Z',
      executor: 'qa.ops@ufg.br',
      changeId: 'cr-a1-8-final-qa-001',
      runId: 'run-a1-8-final-qa-001',
      fingerprintSha256: 'fcdfde265321b607b77353a53bc40c41d3f24331362feba24182c6dd6bf1135d',
    });

    expect(acceptance.metadata.wp).toBe('A1.8');
    expect(acceptance.metadata.item).toBe('A1.8.final');
    expect(acceptance.summary.accepted).toBe(true);
    expect(acceptance.summary.passed).toBe(acceptance.summary.total);

    expect(acceptance.final_baseline.provider_key).toBe('external-linux');
    expect(acceptance.final_baseline.operational_mode).toBe('official');
    expect(acceptance.final_baseline.fallback_local_allowed).toBe(false);
    expect(acceptance.final_baseline.correlation.change_id).toBe('cr-a1-8-final-qa-001');
    expect(acceptance.final_baseline.correlation.run_id).toBe('run-a1-8-final-qa-001');
  });

  it('keeps out-of-scope list explicit and all Epic A1 WPs closed', () => {
    const acceptance = buildA18AcceptanceEvidencePackage();
    const closureRows = acceptance.wp_closure;
    const allocations = acceptance.scope.out_of_scope_allocations;
    const dossier = acceptance.closure_dossier;
    const transition = acceptance.transition_a1_to_a2;
    const conformity = acceptance.cross_document_conformity;
    const containerMatrix = acceptance.container_operational_matrix;
    const a1Allocation = acceptance.a1_operational_allocation;
    const finalAcceptance = acceptance.final_acceptance_criteria;

    expect(acceptance.scope.out_of_scope).toEqual(A18_REQUIRED_OUT_OF_SCOPE);
    expect(allocations).toEqual(A18_OUT_OF_SCOPE_DESTINATIONS);
    expect(allocations.every(item => item.target_epic)).toBe(true);
    expect(closureRows).toHaveLength(7);
    expect(closureRows.every(row => row.status === 'concluido')).toBe(true);
    expect(closureRows.map(row => row.wp)).toEqual([
      'A1.1',
      'A1.2',
      'A1.3',
      'A1.4',
      'A1.5',
      'A1.6',
      'A1.7',
    ]);

    expect(dossier.technical_acceptance_artifact.id).toBe('a1-delivery-1-closure-dossier');
    expect(dossier.test_trail.backend.length).toBeGreaterThan(0);
    expect(dossier.test_trail.frontend.length).toBeGreaterThan(0);
    expect(dossier.final_inventory_snapshot.operational.source).toBe('inventory-final.json');
    expect(dossier.final_inventory_snapshot.cryptographic.source).toBe(
      'configure/crypto-inventory.json'
    );
    expect(dossier.immutability.immutable_post_closure).toBe(true);
    expect(dossier.final_scope_conformity_checklist.signed).toBe(true);
    expect(dossier.final_scope_conformity_checklist.scope_conformity).toBe(true);

    expect(transition.target_epic).toBe('A2');
    expect(transition.consumed_prerequisites.length).toBeGreaterThanOrEqual(4);
    expect(transition.technical_dependencies.length).toBeGreaterThan(0);
    expect(transition.residual_risks.length).toBeGreaterThan(0);
    expect(transition.residual_risks.every(item => item.owner && item.mitigation)).toBe(true);
    expect(transition.handoff_boundary.scope_reopen_forbidden).toBe(true);
    expect(transition.handoff_boundary.reopened_scope_items).toEqual([]);

    expect(conformity.status).toBe('conform');
    expect(conformity.status_alignment.length).toBeGreaterThanOrEqual(3);
    expect(conformity.status_alignment.every(item => item.expected_status === 'encerrado')).toBe(
      true
    );
    expect(conformity.architecture_coherence.high_level_refs.length).toBeGreaterThanOrEqual(3);
    expect(conformity.architecture_coherence.low_level_refs.length).toBeGreaterThanOrEqual(1);
    expect(conformity.e1_operational_flow.unique_description_enforced).toBe(true);
    expect(conformity.e1_operational_flow.canonical_steps.length).toBeGreaterThanOrEqual(6);
    expect(conformity.containerization_by_epic.explicit_by_epic).toBe(true);
    expect(conformity.containerization_by_epic.epics).toEqual([
      'A1',
      'A2',
      'B1',
      'B2',
      'C1',
      'C2',
      'D1',
      'E1',
    ]);
    expect(conformity.blocking_policy.semantic_divergence_blocks_acceptance).toBe(true);

    expect(containerMatrix.status).toBe('executable');
    expect(containerMatrix.source_of_truth).toBe('docs/entregas/roadmap-epicos.md');
    expect(containerMatrix.blocking_policy.implicit_strategic_containers_forbidden).toBe(true);
    expect(containerMatrix.blocking_policy.ambiguous_responsibility_overlap_forbidden).toBe(true);
    expect(containerMatrix.blocking_policy.progressive_vm_convergence_required).toBe(true);
    expect(containerMatrix.epic_distribution.map(item => item.epic)).toEqual([
      'A2',
      'B1',
      'B2',
      'C1',
      'C2',
      'D1',
      'E1',
    ]);
    expect(
      containerMatrix.epic_distribution.every(item => item.orchestrator_containers.length > 0)
    ).toBe(true);
    expect(
      containerMatrix.epic_distribution.every(
        item => item.external_linux_host_containers.length > 0
      )
    ).toBe(true);
    expect(
      containerMatrix.epic_distribution.every(item => item.traceability.requirements.length > 0)
    ).toBe(true);
    expect(
      containerMatrix.epic_distribution.every(
        item => item.traceability.architecture_layers.length > 0
      )
    ).toBe(true);
    expect(
      containerMatrix.progressive_vm_convergence.wave_checkpoints.map(item => item.epic)
    ).toEqual(['A2', 'B1', 'B2', 'C1', 'C2', 'D1', 'E1']);
    expect(
      containerMatrix.progressive_vm_convergence.final_expected_components.length
    ).toBeGreaterThanOrEqual(10);

    expect(a1Allocation.status).toBe('enforced');
    expect(a1Allocation.convention.explicit_none_required_when_no_container_in_domain).toBe(true);
    expect(a1Allocation.convention.development_co_location_allowed_with_logical_separation).toBe(
      true
    );
    expect(a1Allocation.mandatory_a1_containers.orchestrator_system_services).toEqual([
      'runbook-api-engine',
      'provisioning-ssh-executor',
      'pipeline-evidence-store',
    ]);
    expect(a1Allocation.mandatory_a1_containers.external_linux_host_requirements).toEqual([
      'runtime-base-per-active-inventoried-host',
      'fabric-ca-minimum-one-per-org-with-ca-role',
      'fabric-orderer-minimum-one-per-org-with-orderer-role',
      'fabric-peer-minimum-one-per-channel-member-org',
      'couchdb-one-per-peer-when-external-state-db-required',
    ]);
    expect(a1Allocation.frontend_evolution.existing_incremented_screens).toEqual([
      'ProvisioningInfrastructurePage',
      'ProvisioningBlueprintPage',
      'ProvisioningRunbookPage',
      'ProvisioningInventoryPage',
    ]);
    expect(a1Allocation.frontend_evolution.new_screens_or_modules).toEqual([
      'ProvisioningTechnicalHubPage',
      'ProvisioningReadinessCard',
      'ScreenReadinessBanner',
    ]);
    expect(a1Allocation.minimum_container_acceptance_evidence).toEqual([
      'inventory-final-with-name-role-host-ports-status',
      'stage-reports-with-change-id-and-run-id-correlation',
      'verify-report-with-health-checks-per-critical-component',
    ]);

    expect(finalAcceptance.status).toBe('accepted');
    expect(
      finalAcceptance.minimum_coverage.functional_and_contractual_validation_official_flow_a1
    ).toBe(true);
    expect(
      finalAcceptance.minimum_coverage.minimum_evidence_and_auditable_decision_trace_validation
    ).toBe(true);
    expect(
      finalAcceptance.minimum_coverage.documentation_and_container_distribution_validation
    ).toBe(true);
    expect(finalAcceptance.mandatory_rules.contract_regression_breaks_ci).toBe(true);
    expect(finalAcceptance.mandatory_rules.no_local_or_degraded_success_path).toBe(true);
    expect(finalAcceptance.mandatory_rules.acceptance_requires_items_1_to_6_completed).toBe(true);
    expect(Object.values(finalAcceptance.required_items_1_to_6).every(Boolean)).toBe(true);
    expect(finalAcceptance.required_check_ids).toEqual(
      expect.arrayContaining([
        'wp_closure_a1_1_a1_7',
        'official_flow_no_local_fallback',
        'delivery_1_closure_dossier_complete',
        'cross_document_conformity',
        'container_operational_matrix_executable',
        'a1_operational_allocation_non_isolated',
      ])
    );
  });
});
