import React, { PureComponent, Suspense } from 'react';
import { Layout } from 'antd';
import classNames from 'classnames';
import { Link, getLocale } from 'umi';
import styles from './index.less';
import PageLoading from '../PageLoading';
import { getDefaultCollapsedSubMenus } from './SiderMenuUtils';
import hyperledgerFabricLogo from '@/assets/Hyperledger-Fabric.png';
import hyperledgerFabricSymbol from '@/assets/menu_image.png';
import { pickCognusText } from '../../pages/Cognus/cognusI18n';

const BaseMenu = React.lazy(() => import('./BaseMenu'));
const { Sider } = Layout;
const SIDER_WIDTH = 312;
const SIDER_COLLAPSED_WIDTH = 88;

let firstMount = true;

export default class SiderMenu extends PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      openKeys: getDefaultCollapsedSubMenus(props),
    };
  }

  componentDidMount() {
    firstMount = false;
  }

  static getDerivedStateFromProps(props, state) {
    const { pathname, flatMenuKeysLen } = state;
    if (props.location.pathname !== pathname || props.flatMenuKeys.length !== flatMenuKeysLen) {
      return {
        pathname: props.location.pathname,
        flatMenuKeysLen: props.flatMenuKeys.length,
        openKeys: getDefaultCollapsedSubMenus(props),
      };
    }
    return null;
  }

  isMainMenu = key => {
    const { menuData } = this.props;
    return menuData.some(item => {
      if (key) {
        return item.key === key || item.path === key;
      }
      return false;
    });
  };

  handleOpenChange = openKeys => {
    const moreThanOne = openKeys.filter(openKey => this.isMainMenu(openKey)).length > 1;
    this.setState({
      openKeys: moreThanOne ? [openKeys.pop()] : [...openKeys],
    });
  };

  render() {
    const { logo, collapsed, onCollapse, fixSiderbar, theme, isMobile } = this.props;
    const { openKeys } = this.state;
    const locale = getLocale();
    const defaultProps = collapsed ? {} : { openKeys };
    const hideBrandText = isMobile || collapsed;

    const siderClassName = classNames(styles.sider, {
      [styles.fixSiderBar]: fixSiderbar,
      [styles.light]: theme === 'light',
    });
    const logoClassName = classNames(styles.logo, {
      [styles.logoCompact]: hideBrandText,
    });
    const logoMarkClassName = classNames(styles.logoMark, {
      [styles.logoMarkCompact]: hideBrandText,
    });
    return (
      <Sider
        trigger={null}
        collapsible
        collapsed={collapsed}
        collapsedWidth={SIDER_COLLAPSED_WIDTH}
        breakpoint="lg"
        onCollapse={collapse => {
          if (firstMount || !isMobile) {
            onCollapse(collapse);
          }
        }}
        width={SIDER_WIDTH}
        theme={theme}
        className={siderClassName}
      >
        <div className={logoClassName} id="logo">
          <Link to="/">
            <div className={styles.logoMain}>
              <img src={logo} alt="COGNUS" className={logoMarkClassName} />
              {!hideBrandText && (
                <div className={styles.brandBlock}>
                  <h1 className={styles.brandTitle}>COGNUS</h1>
                  <span className={styles.brandSubtitle}>
                    Consortium Orchestration &
                    <br />
                    Governance Network Unified System
                    <br />
                    (for permissioned blockchains)
                  </span>
                  <div className={styles.fabricBrand}>
                    <span className={styles.fabricBrandLabel}>
                      {pickCognusText(
                        'Blockchain permissionada orquestrada:',
                        'Orchestrated permissioned blockchain:',
                        locale
                      )}
                    </span>
                    <img
                      src={hyperledgerFabricLogo}
                      alt="Hyperledger Fabric"
                      className={styles.fabricBrandLogo}
                    />
                  </div>
                </div>
              )}
            </div>
            {hideBrandText && (
              <div className={styles.compactBrandBlock}>
                <span className={styles.compactBrandTitle}>COGNUS</span>
                <span className={styles.compactFabricSymbolWrap}>
                  <img
                    src={hyperledgerFabricSymbol}
                    alt="Hyperledger Fabric"
                    className={styles.compactFabricSymbol}
                  />
                </span>
              </div>
            )}
          </Link>
        </div>
        <Suspense fallback={<PageLoading />}>
          <BaseMenu
            {...this.props}
            mode="inline"
            handleOpenChange={this.handleOpenChange}
            onOpenChange={this.handleOpenChange}
            className={styles.baseMenu}
            style={{ padding: '16px 0', width: '100%' }}
            {...defaultProps}
          />
        </Suspense>
      </Sider>
    );
  }
}
