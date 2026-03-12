import React, { PureComponent } from 'react';
import { injectIntl, setLocale, getLocale } from 'umi';
import { Menu } from 'antd';
import { GlobalOutlined } from '@ant-design/icons';
import classNames from 'classnames';
import HeaderDropdown from '../HeaderDropdown';
import styles from './index.less';

class SelectLang extends PureComponent {
  changeLang = ({ key }) => {
    setLocale(key);
    localStorage.setItem('umi_locale', key);
    localStorage.setItem('umi_locale_user_selected', 'true');
  };

  render() {
    const { className, intl } = this.props;
    const selectedLang = getLocale();
    const isPortugueseUi = String(selectedLang || '')
      .trim()
      .toLowerCase()
      .startsWith('pt');
    const locales = ['en-US', 'pt-BR'];
    const languageLabels = {
      'en-US': isPortugueseUi ? 'Inglês (EUA)' : 'English (US)',
      'pt-BR': isPortugueseUi ? 'Português (Brasil)' : 'Portuguese (Brazil)',
    };
    const languageIcons = {
      'en-US': '🇺🇸',
      'pt-BR': '🇧🇷',
    };
    const langMenu = (
      <Menu className={styles.menu} selectedKeys={[selectedLang]} onClick={this.changeLang}>
        {locales.map(locale => (
          <Menu.Item key={locale}>
            <span role="img" aria-label={languageLabels[locale]}>
              {languageIcons[locale]}
            </span>{' '}
            {languageLabels[locale]}
          </Menu.Item>
        ))}
      </Menu>
    );
    return (
      <HeaderDropdown overlay={langMenu} placement="bottomRight">
        <span className={classNames(styles.dropDown, className)}>
          <GlobalOutlined title={intl.formatMessage({ id: 'navBar.lang' })} />
        </span>
      </HeaderDropdown>
    );
  }
}

export default injectIntl(SelectLang);
