import { pathToRegexp } from 'path-to-regexp';
import { urlToList } from '../_utils/pathTools';

/**
 * Recursively flatten the data
 * [{path:string},{path:string}] => {path,path2}
 * @param  menus
 */
export const getFlatMenuKeys = menuData => {
  let keys = [];
  menuData.forEach(item => {
    keys.push(item.path);
    if (item.children) {
      keys = keys.concat(getFlatMenuKeys(item.children));
    }
  });
  return keys;
};

export const getMenuMatches = (flatMenuKeys, path) =>
  flatMenuKeys.filter(item => {
    if (item) {
      const { regexp } = pathToRegexp(item);
      return regexp.test(path);
    }
    return false;
  });

const getMenuPathByPathname = (menuData, pathname, parentKeys = []) => {
  if (!menuData) {
    return [];
  }

  for (let i = 0; i < menuData.length; i += 1) {
    const item = menuData[i];
    if (!item || !item.path) {
      // eslint-disable-next-line no-continue
      continue;
    }

    const currentKeys = [...parentKeys, item.path];
    if (item.children) {
      const childPath = getMenuPathByPathname(item.children, pathname, currentKeys);
      if (childPath.length > 0) {
        return childPath;
      }
    }

    const { regexp } = pathToRegexp(item.path);
    if (regexp.test(pathname)) {
      return currentKeys;
    }
  }

  return [];
};
/**
 * 获得菜单子节点
 * @memberof SiderMenu
 */
export const getDefaultCollapsedSubMenus = props => {
  const {
    location: { pathname },
    flatMenuKeys,
    menuData,
  } = props;
  const pathMenuKeys = getMenuPathByPathname(menuData, pathname);
  const urlMenuKeys = urlToList(pathname)
    .map(item => getMenuMatches(flatMenuKeys, item)[0])
    .filter(item => item);
  return Array.from(new Set(['/', ...pathMenuKeys, ...urlMenuKeys]));
};
