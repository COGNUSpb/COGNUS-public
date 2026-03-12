import {
  PROVISIONING_INFRA_SCREEN_KEY,
  PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY,
} from '../data/provisioningContract';
import {
  getProvisioningBreadcrumbs,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';

describe('provisioningNavigation', () => {
  it('keeps only infra as primary tab while technical hub is hidden', () => {
    expect(provisioningNavItems).toHaveLength(1);
    expect(provisioningNavItems[0].key).toBe(PROVISIONING_INFRA_SCREEN_KEY);
  });

  it('resolves technical screens into infra tab while technical hub is hidden', () => {
    expect(resolveProvisioningActiveNavKey('e1-blueprint')).toBe(PROVISIONING_INFRA_SCREEN_KEY);
    expect(resolveProvisioningActiveNavKey('e1-provisionamento')).toBe(
      PROVISIONING_INFRA_SCREEN_KEY
    );
    expect(resolveProvisioningActiveNavKey('e1-inventario')).toBe(PROVISIONING_INFRA_SCREEN_KEY);
    expect(resolveProvisioningActiveNavKey(PROVISIONING_ORG_RUNTIME_TOPOLOGY_SCREEN_KEY)).toBe(
      PROVISIONING_INFRA_SCREEN_KEY
    );
  });

  it('builds breadcrumbs preserving mandatory scope context', () => {
    const infraBreadcrumbs = getProvisioningBreadcrumbs(PROVISIONING_INFRA_SCREEN_KEY);
    const technicalBreadcrumbs = getProvisioningBreadcrumbs('e1-blueprint');

    expect(infraBreadcrumbs[0]).toContain('external provider + Linux VM');
    expect(technicalBreadcrumbs[0]).toContain('external provider + Linux VM');
    expect(technicalBreadcrumbs).toContain('Blueprint and versioning');
  });
});
