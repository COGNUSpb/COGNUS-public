import fs from 'fs';
import path from 'path';

import { buildA16AcceptanceEvidencePackage } from './provisioningA16AcceptanceMatrix';

const resolveExportPath = () => {
  const explicitPath = String(process.env.A16_ACCEPTANCE_EXPORT_PATH || '').trim();
  if (explicitPath) {
    return explicitPath;
  }

  return path.resolve(
    __dirname,
    '../../../../../../../docs/entregas/evidencias/wp-a1.6-criterios-aceite.json'
  );
};

describe('provisioningA16AcceptanceMatrix export', () => {
  it('exports acceptance evidence package for A1.6', () => {
    const exportEnabled = process.env.A16_EXPORT_ACCEPTANCE === '1';
    const exportPath = resolveExportPath();
    const payload = buildA16AcceptanceEvidencePackage({
      changeIdPrefix: 'cr-a16-evidence',
      runIdPrefix: 'run-a16-evidence',
      executor: 'qa.ops@ufg.br',
      generatedAtUtc: '2026-02-16T22:30:00Z',
    });

    if (exportEnabled) {
      fs.mkdirSync(path.dirname(exportPath), { recursive: true });
      fs.writeFileSync(exportPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    }

    expect(payload.metadata.wp).toBe('A1.6');
    expect(payload.summary.accepted).toBe(true);
    expect(payload.summary.passed).toBe(payload.summary.total);
  });
});
