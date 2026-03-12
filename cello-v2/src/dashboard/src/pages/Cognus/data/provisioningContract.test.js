import { screenPathByKey } from './navigation';
import { screens } from './screens';
import {
  PROVISIONING_ENTRY_CONTRACTS,
  PROVISIONING_MODULE_ROUTE_DEFINITIONS,
  PROVISIONING_MODULE_SCREEN_PATH_BY_KEY,
  PROVISIONING_NAV_ITEMS,
  PROVISIONING_ROUTE_DEFINITIONS,
  PROVISIONING_SCREEN_COMPONENT_BY_KEY,
  PROVISIONING_SCREEN_CONTRACTS,
  PROVISIONING_SCREEN_PATH_BY_KEY,
} from './provisioningContract';

describe('provisioning route contract', () => {
  it('keeps unique keys and paths', () => {
    const keys = PROVISIONING_SCREEN_CONTRACTS.map(contract => contract.screenKey);
    const paths = PROVISIONING_SCREEN_CONTRACTS.map(contract => contract.path);

    expect(new Set(keys).size).toBe(keys.length);
    expect(new Set(paths).size).toBe(paths.length);
  });

  it('keeps unique keys and paths for module entry routes', () => {
    const keys = PROVISIONING_ENTRY_CONTRACTS.map(contract => contract.screenKey);
    const paths = PROVISIONING_ENTRY_CONTRACTS.map(contract => contract.path);

    expect(new Set(keys).size).toBe(keys.length);
    expect(new Set(paths).size).toBe(paths.length);
  });

  it('maps screen key to route path and component deterministically', () => {
    PROVISIONING_SCREEN_CONTRACTS.forEach(contract => {
      expect(PROVISIONING_SCREEN_PATH_BY_KEY[contract.screenKey]).toBe(contract.path);
      expect(PROVISIONING_SCREEN_COMPONENT_BY_KEY[contract.screenKey]).toBe(contract.component);
    });
  });

  it('stays synchronized with technical route definitions', () => {
    PROVISIONING_SCREEN_CONTRACTS.forEach(contract => {
      const route = PROVISIONING_ROUTE_DEFINITIONS.find(
        routeDefinition => routeDefinition.path === contract.path
      );
      expect(route).toBeTruthy();
      expect(route.name).toBe(contract.routeName);
      expect(route.component).toBe(contract.component);
    });
  });

  it('keeps module entry routes synchronized with primary navigation tabs', () => {
    PROVISIONING_ENTRY_CONTRACTS.forEach(contract => {
      const route = PROVISIONING_MODULE_ROUTE_DEFINITIONS.find(
        routeDefinition => routeDefinition.path === contract.path
      );
      const navItem = PROVISIONING_NAV_ITEMS.find(item => item.key === contract.screenKey);

      expect(route).toBeTruthy();
      expect(route.name).toBe(contract.routeName);
      expect(route.component).toBe(contract.component);
      if (contract.hideInMenu) {
        expect(navItem).toBeUndefined();
      } else {
        expect(navItem).toBeTruthy();
        expect(navItem.path).toBe(contract.path);
      }
    });
  });

  it('stays synchronized with screen catalog and global screen path map', () => {
    Object.keys(PROVISIONING_MODULE_SCREEN_PATH_BY_KEY).forEach(screenKey => {
      const contractPath = PROVISIONING_MODULE_SCREEN_PATH_BY_KEY[screenKey];
      const catalogScreen = screens.find(screen => screen.key === screenKey);

      expect(catalogScreen).toBeTruthy();
      expect(catalogScreen.path).toBe(contractPath);
      expect(screenPathByKey[screenKey]).toBe(contractPath);
    });
  });
});
