import React, { PureComponent } from 'react';
import classNames from 'classnames';
import { Menu } from 'antd';
import {
  EyeOutlined,
  ScheduleOutlined,
  TeamOutlined,
  DesktopOutlined,
  NodeIndexOutlined,
  ApartmentOutlined,
  DeploymentUnitOutlined,
  FunctionOutlined,
  UserOutlined,
} from '@ant-design/icons';
import { Link } from 'umi';
import { urlToList } from '../_utils/pathTools';
import { getMenuMatches } from './SiderMenuUtils';
// import { isUrl } from '@/utils/utils';
// import styles from './index.less';
// import IconFont from '@/components/IconFont';

const { SubMenu } = Menu;
const menus = {
  eye: <EyeOutlined />,
  dashboard: <ScheduleOutlined />,
  team: <TeamOutlined />,
  node: <NodeIndexOutlined />,
  network: <ApartmentOutlined />,
  channel: <DeploymentUnitOutlined />,
  chaincode: <FunctionOutlined />,
  user: <UserOutlined />,
  agent: <DesktopOutlined />,
};

// Allow menu.js config icon as string or ReactNode
//   icon: 'setting',
//   icon: 'icon-geren' #For Iconfont ,
//   icon: 'http://demo.com/icon.png',
//   icon: <Icon type="setting" />,
const getIcon = icon => {
  if (typeof icon === 'string') {
    // if (isUrl(icon)) {
    //   return <Icon component={() => <img src={icon} alt="icon" className={styles.icon} />} />;
    // }
    // if (icon.startsWith('icon-')) {
    //   return <IconFont type={icon} />;
    // }
    return menus[icon];
  }
  return icon;
};

export default class BaseMenu extends PureComponent {
  /**
   * 获得菜单子节点
   * @memberof SiderMenu
   */
  getNavMenuItems = (menusData, inheritedDisabled = false) => {
    if (!menusData) {
      return [];
    }

    return menusData
      .filter(item => item.name && !item.hideInMenu)
      .map((item, index) => ({ item, index }))
      .sort((left, right) => {
        const leftOrder = Number(left.item.menuOrder || 0);
        const rightOrder = Number(right.item.menuOrder || 0);
        if (leftOrder !== rightOrder) {
          return leftOrder - rightOrder;
        }
        return left.index - right.index;
      })
      .map(({ item, index }) => {
        const menuDisabled = inheritedDisabled || Boolean(item.menuDisabled || item.disabled);
        const normalizedItem = { ...item, menuDisabled };
        const menuNode = this.getSubMenuOrItem(normalizedItem, menuDisabled);
        if (!menuNode) {
          return null;
        }
        if (!normalizedItem.menuDividerBefore) {
          return [menuNode];
        }
        return [
          <Menu.Divider
            key={`divider-${normalizedItem.path || normalizedItem.name || index}`}
            className="sider-menu-divider"
          />,
          menuNode,
        ];
      })
      .reduce((accumulator, current) => {
        if (!current) {
          return accumulator;
        }
        return accumulator.concat(current);
      }, [])
      .filter(item => item);
  };

  // Get the currently selected menu
  getSelectedMenuKeys = pathname => {
    const { flatMenuKeys } = this.props;
    return urlToList(pathname).map(itemPath => getMenuMatches(flatMenuKeys, itemPath).pop());
  };

  /**
   * get SubMenu or Item
   */
  getSubMenuOrItem = (item, inheritedDisabled = false) => {
    const menuDisabled = inheritedDisabled || Boolean(item.menuDisabled || item.disabled);

    // doc: add hideChildrenInMenu
    if (item.children && !item.hideChildrenInMenu && item.children.some(child => child.name)) {
      const { name } = item;
      return (
        <SubMenu
          title={
            item.icon ? (
              <span>
                {getIcon(item.icon)}
                <span>{name}</span>
              </span>
            ) : (
              name
            )
          }
          key={item.path}
        >
          {this.getNavMenuItems(item.children, menuDisabled)}
        </SubMenu>
      );
    }
    return (
      <Menu.Item key={item.path} disabled={menuDisabled}>
        {this.getMenuItemPath(item, menuDisabled)}
      </Menu.Item>
    );
  };

  /**
   * 判断是否是http链接.返回 Link 或 a
   * Judge whether it is http link.return a or Link
   * @memberof SiderMenu
   */
  getMenuItemPath = (item, menuDisabled = false) => {
    const { name } = item;
    const itemPath = this.conversionPath(item.path);
    const icon = getIcon(item.icon);
    const { target } = item;
    if (menuDisabled) {
      return (
        <span>
          {icon}
          <span>{name}</span>
        </span>
      );
    }

    // Is it a http link
    if (/^https?:\/\//.test(itemPath)) {
      return (
        <a href={itemPath} target={target}>
          {icon}
          <span>{name}</span>
        </a>
      );
    }
    const { location, isMobile, onCollapse } = this.props;
    return (
      <Link
        to={itemPath}
        target={target}
        replace={itemPath === location.pathname}
        onClick={
          isMobile
            ? () => {
                onCollapse(true);
              }
            : undefined
        }
      >
        {icon}
        <span>{name}</span>
      </Link>
    );
  };

  conversionPath = path => {
    if (path && path.indexOf('http') === 0) {
      return path;
    }
    return `/${path || ''}`.replace(/\/+/g, '/');
  };

  getPopupContainer = (fixedHeader, layout) => {
    if (fixedHeader && layout === 'topmenu') {
      return this.wrap;
    }
    return document.body;
  };

  getRef = ref => {
    this.wrap = ref;
  };

  render() {
    const {
      openKeys,
      theme,
      mode,
      location: { pathname },
      className,
      collapsed,
      fixedHeader,
      layout,
    } = this.props;
    // if pathname can't match, use the nearest parent's key
    let selectedKeys = this.getSelectedMenuKeys(pathname);
    if (!selectedKeys.length && openKeys) {
      selectedKeys = [openKeys[openKeys.length - 1]];
    }
    let props = {};
    if (openKeys && !collapsed) {
      props = {
        openKeys: openKeys.length === 0 ? [...selectedKeys] : openKeys,
      };
    }
    const { handleOpenChange, style, menuData } = this.props;
    const cls = classNames(className, {
      'top-nav-menu': mode === 'horizontal',
    });

    return (
      <>
        <Menu
          key="Menu"
          mode={mode}
          theme={theme}
          onOpenChange={handleOpenChange}
          selectedKeys={selectedKeys}
          style={style}
          className={cls}
          {...props}
          getPopupContainer={() => this.getPopupContainer(fixedHeader, layout)}
        >
          {this.getNavMenuItems(menuData)}
        </Menu>
        <div ref={this.getRef} />
      </>
    );
  }
}
