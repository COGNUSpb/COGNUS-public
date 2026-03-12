import fs from 'fs';
import path from 'path';
import {
  A15_CRITICAL_RENDER_STATES,
  A15_SCOPE_CONSTRAINTS,
  buildA15AcceptanceEvidencePackage,
  buildA15CriticalRenderStateMatrix,
  evaluateA15UxBaseline,
  runA15BlueprintRunbookInventoryIntegration,
  runA15ExternalLinuxE2ESmoke,
} from './provisioningA15AcceptanceMatrix';
import { PROVISIONING_SCREEN_CONTRACTS } from '../data/provisioningContract';

describe('provisioningA15AcceptanceMatrix', () => {
  it('covers all critical render states (vazio/parcial/bloqueado/erro/sucesso)', () => {
    const matrix = buildA15CriticalRenderStateMatrix({
      changeIdPrefix: 'cr-2026-02-16-matrix',
      runIdPrefix: 'run-2026-02-16-matrix',
      executor: 'ops.cognus@ufg.br',
    });

    expect(matrix.coverage.complete).toBe(true);
    expect(matrix.states).toHaveLength(A15_CRITICAL_RENDER_STATES.length);
    expect(matrix.coverage.coveredStates).toEqual(A15_CRITICAL_RENDER_STATES);

    const stateByKey = matrix.states.reduce((accumulator, state) => {
      accumulator[state.stateKey] = state;
      return accumulator;
    }, {});

    expect(stateByKey.vazio.runbook.status).toBe('idle');
    expect(stateByKey.vazio.inventory.syncPhase).toBe('unsynced');
    expect(stateByKey.vazio.evidences.gate.allowCloseExecution).toBe(false);

    expect(stateByKey.parcial.runbook.status).toBe('running');
    expect(stateByKey.parcial.inventory.syncPhase).toBe('partial');
    expect(stateByKey.parcial.evidences.gate.allowExport).toBe(false);

    expect(stateByKey.bloqueado.runbook.status).toBe('idle');
    expect(stateByKey.bloqueado.notes).toContain('runbook_provider_not_supported');
    expect(stateByKey.bloqueado.notes).toContain('runbook_backend_pending');
    expect(stateByKey.bloqueado.evidences.gate.allowCloseExecution).toBe(false);

    expect(stateByKey.erro.runbook.status).toBe('failed');
    expect(stateByKey.erro.runbook.latestFailure.code).toBe('prepare_ssh_access_failed');
    expect(stateByKey.erro.evidences.gate.allowCloseExecution).toBe(false);

    expect(stateByKey.sucesso.runbook.status).toBe('completed');
    expect(stateByKey.sucesso.inventory.syncPhase).toBe('full');
    expect(stateByKey.sucesso.evidences.gate.allowCloseExecution).toBe(true);
    expect(stateByKey.sucesso.evidences.immutable).toBe(true);
  });

  it('validates integration flow blueprint -> runbook -> inventario with contract simulation', () => {
    const integration = runA15BlueprintRunbookInventoryIntegration({
      changeIdPrefix: 'cr-2026-02-16-integration',
      runIdPrefix: 'run-2026-02-16-integration',
      executor: 'ops.cognus@ufg.br',
    });

    expect(integration.acceptance.validBlueprint).toBe(true);
    expect(integration.acceptance.runbookCompleted).toBe(true);
    expect(integration.acceptance.inventoryConsistent).toBe(true);
    expect(integration.acceptance.evidenceTraceable).toBe(true);
    expect(integration.acceptance.simulatedAndLegacyContracts).toBe(true);

    expect(integration.runbook.status).toBe('completed');
    expect(integration.runbook.officialSnapshot.coherence.isConsistent).toBe(true);
    expect(integration.inventory.syncPhase).toBe('full');
    expect(integration.inventory.exportPayload.metadata.provider_scope).toBe(
      A15_SCOPE_CONSTRAINTS.inventoryProvider
    );
    expect(integration.evidences.gate.allowCloseExecution).toBe(true);
    expect(integration.evidences.exportPayload.metadata.change_id).toBe(integration.changeId);
    expect(integration.evidences.exportPayload.metadata.run_id).toBe(integration.runId);
    expect(integration.contracts.availableContractKinds).toContain('official');
    expect(integration.contracts.availableContractKinds).toContain('legacy');
    expect(integration.contracts.availableContractKinds).toContain('simulated');
  });

  it('runs E2E smoke in external provider + VM Linux scope', () => {
    const e2e = runA15ExternalLinuxE2ESmoke({
      changeIdPrefix: 'cr-2026-02-16-e2e',
      runIdPrefix: 'run-2026-02-16-e2e',
      executor: 'ops.cognus@ufg.br',
    });

    expect(e2e.pass).toBe(true);
    e2e.checks.forEach(check => {
      expect(check.pass).toBe(true);
    });
    expect(e2e.evidences.gate.allowCloseExecution).toBe(true);
    expect(
      e2e.inventory.exportPayload.records.hosts.every(host => host.providerKey === 'external-linux')
    ).toBe(true);
    expect(
      e2e.inventory.exportPayload.records.nodes.every(node => node.providerKey === 'external-linux')
    ).toBe(true);
  });

  it('enforces UX baseline for accessibility and responsiveness on all E1 screens', () => {
    const ux = evaluateA15UxBaseline();

    expect(ux.accessibility.pass).toBe(true);
    expect(ux.accessibility.screenRows).toHaveLength(PROVISIONING_SCREEN_CONTRACTS.length);
    ux.accessibility.screenRows.forEach(row => {
      expect(row.pass).toBe(true);
      expect(row.hasScopeAlertContext).toBe(true);
      expect(row.actionsHaveOperationalReason).toBe(true);
    });

    expect(ux.responsiveness.pass).toBe(true);
    expect(ux.responsiveness.desktopMinWidth).toBe(1321);
    expect(ux.responsiveness.mobileMaxWidth).toBe(1320);

    const layoutStylesPath = path.resolve(__dirname, '../components/NeoOpsLayout.less');
    const layoutStyles = fs.readFileSync(layoutStylesPath, 'utf8');

    expect(layoutStyles).toContain('@media (max-width: 1320px)');
    expect(layoutStyles).toContain('grid-template-columns: 1fr;');
    ux.responsiveness.collapseToSingleColumnClasses.forEach(className => {
      expect(layoutStyles).toContain(`.${className}`);
    });
  });

  it('generates acceptance evidence package with criteria pass and traceability ids', () => {
    const evidencePackage = buildA15AcceptanceEvidencePackage({
      changeIdPrefix: 'cr-2026-02-16-aceite',
      runIdPrefix: 'run-2026-02-16-aceite',
      executor: 'ops.cognus@ufg.br',
      generatedAtUtc: '2026-02-16T14:00:00Z',
    });

    expect(evidencePackage.metadata.wp).toBe('A1.5');
    expect(evidencePackage.metadata.scope.provider).toBe('external');
    expect(evidencePackage.metadata.scope.compute_target).toBe('vm_linux');
    expect(evidencePackage.metadata.scope.runbook_provider).toBe('external-linux');
    expect(evidencePackage.summary.accepted).toBe(true);
    expect(evidencePackage.summary.passed).toBe(evidencePackage.summary.total);
    expect(evidencePackage.acceptance_criteria.every(criteria => criteria.pass)).toBe(true);

    expect(evidencePackage.traceability.change_ids.length).toBeGreaterThan(0);
    expect(evidencePackage.traceability.run_ids.length).toBeGreaterThan(0);
    expect(
      evidencePackage.traceability.change_ids.includes('cr-2026-02-16-aceite-integration')
    ).toBe(true);
    expect(evidencePackage.traceability.run_ids.includes('run-2026-02-16-aceite-integration')).toBe(
      true
    );
    expect(evidencePackage.traceability.evidence_gate.allowCloseExecution).toBe(true);
  });

  it('keeps delivery evidence artifact for WP A1.5 acceptance package', () => {
    const evidenceArtifactPath = path.resolve(
      __dirname,
      '../../../../../../../docs/entregas/evidencias/wp-a1.5-matriz-aceite.json'
    );
    const rawArtifact = fs.readFileSync(evidenceArtifactPath, 'utf8');
    const artifact = JSON.parse(rawArtifact);

    expect(artifact.metadata.wp).toBe('A1.5');
    expect(artifact.summary.accepted).toBe(true);
    expect(artifact.critical_render_matrix.coverage.complete).toBe(true);
    expect(artifact.e2e_smoke.pass).toBe(true);
    expect(artifact.acceptance_criteria.every(criteria => criteria.pass)).toBe(true);
  });
});
