/*
 SPDX-License-Identifier: Apache-2.0
*/
import React, { Component } from 'react';
import { Layout } from 'antd';
import { Helmet } from 'react-helmet';
import { connect, setLocale, getLocale } from 'umi';
import { ContainerQuery } from 'react-container-query';
import classNames from 'classnames';
import Media from 'react-media';
import SiderMenu from '@/components/SiderMenu';
import getPageTitle from '@/utils/getPageTitle';
import logo from '../assets/cognus_C_icon.svg';
import Footer from './Footer';
import Header from './Header';
import Context from './MenuContext';
import { revokeAllVmAccessSessions } from '@/services/vmAccessSession';
import styles from './BasicLayout.less';

// lazy load SettingDrawer
const SettingDrawer = React.lazy(() => import('@/components/SettingDrawer'));

const { Content } = Layout;
const SIDER_WIDTH = 312;
const SIDER_COLLAPSED_WIDTH = 88;
const RUNBOOK_AUDIT_HISTORY_KEYS = [
  'cognus.provisioning.runbook.audit.history.v1',
  'cognus.provisioning.runbook.audit.history.v2',
];
const RUNBOOK_AUDIT_SELECTED_KEYS = [
  'cognus.provisioning.runbook.audit.selected.v1',
  'cognus.provisioning.runbook.audit.selected.v2',
];
const RUNBOOK_AUDIT_CLEAN_MARKER_APPLIED_KEY =
  'cognus.provisioning.runbook.audit.clean.marker.applied.v1';
const LOCALE_STORAGE_KEY = 'umi_locale';
const LOCALE_USER_SELECTED_KEY = 'umi_locale_user_selected';
const SUPPORTED_LOCALES = ['en-US', 'pt-BR'];
const DEFAULT_LOCALE = 'en-US';

const query = {
  'screen-xs': {
    maxWidth: 575,
  },
  'screen-sm': {
    minWidth: 576,
    maxWidth: 767,
  },
  'screen-md': {
    minWidth: 768,
    maxWidth: 991,
  },
  'screen-lg': {
    minWidth: 992,
    maxWidth: 1199,
  },
  'screen-xl': {
    minWidth: 1200,
    maxWidth: 1599,
  },
  'screen-xxl': {
    minWidth: 1600,
  },
};

class BasicLayout extends Component {
  componentDidMount() {
    const {
      dispatch,
      route: { routes, path, authority },
    } = this.props;
    dispatch({
      type: 'setting/getSetting',
    });
    dispatch({
      type: 'menu/getMenuData',
      payload: { routes, path, authority },
    });

    this.applyRunbookAuditCleanMarker();

    // Initialize language from localStorage or default to English
    const savedLocale = localStorage.getItem(LOCALE_STORAGE_KEY);
    const localeWasUserSelected =
      String(localStorage.getItem(LOCALE_USER_SELECTED_KEY) || '').trim() === 'true';
    const currentLocale = getLocale();
    const targetLocale = localeWasUserSelected && SUPPORTED_LOCALES.includes(savedLocale || '')
      ? savedLocale
      : DEFAULT_LOCALE;

    if (targetLocale !== currentLocale) {
      setLocale(targetLocale);
    }

    localStorage.setItem(LOCALE_STORAGE_KEY, targetLocale);
    window.addEventListener('storage', this.handleTokenStorageChange);
  }

  componentWillUnmount() {
    window.removeEventListener('storage', this.handleTokenStorageChange);
  }

  handleTokenStorageChange = event => {
    if (!event || event.key !== 'cello-token') {
      return;
    }

    const nextToken = String(event.newValue || '').trim();
    if (nextToken) {
      return;
    }

    revokeAllVmAccessSessions({
      clearToken: false,
      reason: 'token_removed',
    });
  };

  getContext() {
    const { location, breadcrumbNameMap } = this.props;
    return {
      location,
      breadcrumbNameMap,
    };
  }

  applyRunbookAuditCleanMarker = async () => {
    if (typeof window === 'undefined' || !window.localStorage || !window.fetch) {
      return;
    }

    try {
      const response = await window.fetch(`/cognus-clean-marker.json?t=${Date.now()}`, {
        cache: 'no-store',
      });
      if (!response.ok) {
        return;
      }

      const marker = await response.json();
      const markerValue = String(
        marker && marker.cleaned_at_utc ? marker.cleaned_at_utc : ''
      ).trim();
      if (!markerValue) {
        return;
      }

      const appliedMarker = String(
        window.localStorage.getItem(RUNBOOK_AUDIT_CLEAN_MARKER_APPLIED_KEY) || ''
      ).trim();
      if (appliedMarker === markerValue) {
        return;
      }

      [...RUNBOOK_AUDIT_HISTORY_KEYS, ...RUNBOOK_AUDIT_SELECTED_KEYS].forEach(storageKey => {
        window.localStorage.removeItem(storageKey);
      });
      window.localStorage.setItem(RUNBOOK_AUDIT_CLEAN_MARKER_APPLIED_KEY, markerValue);
    } catch (error) {
      // no-op: marker is best-effort and must not block layout bootstrap
    }
  };

  getLayoutStyle = () => {
    const { fixSiderbar, isMobile, collapsed, layout } = this.props;
    if (fixSiderbar && layout !== 'topmenu' && !isMobile) {
      return {
        paddingLeft: `${collapsed ? SIDER_COLLAPSED_WIDTH : SIDER_WIDTH}px`,
      };
    }
    return null;
  };

  handleMenuCollapse = collapsed => {
    const { dispatch } = this.props;
    dispatch({
      type: 'global/changeLayoutCollapsed',
      payload: collapsed,
    });
  };

  renderSettingDrawer = () => {
    // Do not render SettingDrawer in production
    // unless it is deployed in preview.pro.ant.design as demo
    // preview.pro.ant.design only do not use in your production ; preview.pro.ant.design 专用环境变量，请不要在你的项目中使用它。
    if (
      process.env.NODE_ENV === 'production' &&
      ANT_DESIGN_PRO_ONLY_DO_NOT_USE_IN_YOUR_PRODUCTION !== 'site'
    ) {
      return null;
    }
    return <SettingDrawer />;
  };

  render() {
    const {
      navTheme,
      layout: PropsLayout,
      children,
      location: { pathname },
      isMobile,
      menuData,
      breadcrumbNameMap,
      fixedHeader,
    } = this.props;

    const isTop = PropsLayout === 'topmenu';
    const contentStyle = !fixedHeader ? { paddingTop: 0 } : {};
    const layout = (
      <Layout>
        {isTop && !isMobile ? null : (
          <SiderMenu
            logo={logo}
            theme={navTheme}
            onCollapse={this.handleMenuCollapse}
            menuData={menuData}
            isMobile={isMobile}
            {...this.props}
          />
        )}
        <Layout
          style={{
            ...this.getLayoutStyle(),
            minHeight: '100vh',
          }}
        >
          <Header
            menuData={menuData}
            handleMenuCollapse={this.handleMenuCollapse}
            logo={logo}
            isMobile={isMobile}
            {...this.props}
          />
          <Content className={styles.content} style={contentStyle}>
            {children}
          </Content>
          <Footer />
        </Layout>
      </Layout>
    );
    return (
      <>
        <Helmet>
          <title>{getPageTitle(pathname, breadcrumbNameMap)}</title>
        </Helmet>

        <ContainerQuery query={query}>
          {params => (
            <Context.Provider value={this.getContext()}>
              <div className={classNames(params)}>{layout}</div>
            </Context.Provider>
          )}
        </ContainerQuery>
      </>
    );
  }
}

export default connect(({ global, setting, menu: menuModel }) => ({
  collapsed: global.collapsed,
  layout: setting.layout,
  menuData: menuModel.menuData,
  breadcrumbNameMap: menuModel.breadcrumbNameMap,
  ...setting,
}))(props => (
  <Media query="(max-width: 599px)">
    {isMobile => <BasicLayout {...props} isMobile={isMobile} />}
  </Media>
));
