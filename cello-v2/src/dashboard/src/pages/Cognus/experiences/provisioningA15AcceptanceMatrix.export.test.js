import fs from 'fs';
import path from 'path';
import { buildA15AcceptanceEvidencePackage } from './provisioningA15AcceptanceMatrix';

const resolveExportPath = () => {
  const explicitPath = String(process.env.A15_ACCEPTANCE_EXPORT_PATH || '').trim();
  if (explicitPath) {
    return explicitPath;
  }

  return path.resolve(
    __dirname,
    '../../../../../../../docs/entregas/evidencias/wp-a1.5-matriz-aceite.json'
  );
};

describe('provisioningA15AcceptanceMatrix export', () => {
  it('exports acceptance evidence package for CI job', () => {
    const exportEnabled = process.env.A15_ACCEPTANCE_EXPORT === '1';
    const exportPath = resolveExportPath();
    const payload = buildA15AcceptanceEvidencePackage({
      changeIdPrefix: 'cr-2026-02-16-aceite',
      runIdPrefix: 'run-2026-02-16-aceite',
      executor: 'ops.cognus@ufg.br',
      generatedAtUtc: '2026-02-16T14:00:00Z',
    });

    if (exportEnabled) {
      fs.mkdirSync(path.dirname(exportPath), { recursive: true });
      fs.writeFileSync(exportPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    }

    expect(payload.metadata.wp).toBe('A1.5');
    expect(payload.summary.accepted).toBe(true);
    expect(payload.summary.passed).toBe(payload.summary.total);
  });
});
