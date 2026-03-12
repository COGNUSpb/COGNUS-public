import fs from 'fs';
import path from 'path';

import { buildA17AcceptanceEvidencePackage } from './provisioningA17AcceptanceMatrix';

const resolveExportPath = () => {
  const explicitPath = String(process.env.A17_ACCEPTANCE_EXPORT_PATH || '').trim();
  if (explicitPath) {
    return explicitPath;
  }

  return path.resolve(
    __dirname,
    '../../../../../../../docs/entregas/evidencias/wp-a1.7-criterios-aceite.json'
  );
};

describe('provisioningA17AcceptanceMatrix export', () => {
  it('exports acceptance evidence package for A1.7.9', () => {
    const exportEnabled = process.env.A17_EXPORT_ACCEPTANCE === '1';
    const exportPath = resolveExportPath();
    const payload = buildA17AcceptanceEvidencePackage({
      executor: 'qa.ops@ufg.br',
      generatedAtUtc: '2026-02-17T23:40:00Z',
      changeId: 'cr-a17-evidence-001',
      runId: 'run-a17-evidence-001',
    });

    if (exportEnabled) {
      fs.mkdirSync(path.dirname(exportPath), { recursive: true });
      fs.writeFileSync(exportPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    }

    expect(payload.metadata.wp).toBe('A1.7');
    expect(payload.metadata.item).toBe('A1.7.9');
    expect(payload.summary.accepted).toBe(true);
  });
});
