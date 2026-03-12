import fs from 'fs';
import path from 'path';

import { buildA18AcceptanceEvidencePackage } from './provisioningA18AcceptanceMatrix';

const resolveExportPath = () => {
  const explicitPath = String(process.env.A18_ACCEPTANCE_EXPORT_PATH || '').trim();
  if (explicitPath) {
    return explicitPath;
  }

  return path.resolve(
    __dirname,
    '../../../../../../../docs/entregas/evidencias/wp-a1.8-fechamento-entrega-1.json'
  );
};

describe('provisioningA18AcceptanceMatrix export', () => {
  it('exports final closure evidence package for A1.8', () => {
    const exportEnabled = process.env.A18_EXPORT_ACCEPTANCE === '1';
    const exportPath = resolveExportPath();
    const payload = buildA18AcceptanceEvidencePackage({
      generatedAtUtc: '2026-02-18T01:10:00Z',
      executor: 'qa.ops@ufg.br',
      changeId: 'cr-a1-8-final-evidence-001',
      runId: 'run-a1-8-final-evidence-001',
      fingerprintSha256: 'fcdfde265321b607b77353a53bc40c41d3f24331362feba24182c6dd6bf1135d',
    });

    if (exportEnabled) {
      fs.mkdirSync(path.dirname(exportPath), { recursive: true });
      fs.writeFileSync(exportPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    }

    expect(payload.metadata.wp).toBe('A1.8');
    expect(payload.metadata.item).toBe('A1.8.final');
    expect(payload.summary.accepted).toBe(true);
  });
});
