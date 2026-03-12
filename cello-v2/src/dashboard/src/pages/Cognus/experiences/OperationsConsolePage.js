import React from 'react';
import { Typography } from 'antd';
import { ScheduleOutlined, SettingOutlined } from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';
import styles from '../components/NeoOpsLayout.less';

const navItems = [
  { key: 'overview', label: { ptBR: 'Dashboard', enUS: 'Dashboard' }, icon: <ScheduleOutlined /> },
  { key: 'settings', label: { ptBR: 'Configurações', enUS: 'Settings' }, icon: <SettingOutlined /> },
];

const OperationsConsolePage = () => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

  return (
    <NeoOpsLayout
      screenKey="e4-console"
      sectionLabel={{ ptBR: 'Operações /', enUS: 'Operations /' }}
      title={{ ptBR: 'Organizações sob operação', enUS: 'Organizations under operation' }}
      subtitle={{
        ptBR: 'Acompanhamento diário de organizações, canais e grupos de negócio.',
        enUS: 'Daily monitoring of organizations, channels, and business groups.',
      }}
      navItems={navItems}
      activeNavKey="overview"
    >
      <div className={styles.content}>
        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Organização Alfa', 'Organization Alpha')}
            </Typography.Title>
            <Typography.Text className={styles.neoLabel}>
              {t('domínio alfa.local', 'domain alpha.local')}
            </Typography.Text>
            <div className={styles.line} />
            <Typography.Text className={styles.neoLabel}>Peers</Typography.Text>
            <div className={styles.chipRow}>
              <span className={`${styles.chip} ${styles.statusOn}`}>peer0.alfa.local</span>
            </div>
            <Typography.Text className={styles.neoLabel}>Orderers</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>orderer0.alfa.local</span>
            </div>
          </div>

          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Organização Beta', 'Organization Beta')}
            </Typography.Title>
            <Typography.Text className={styles.neoLabel}>
              {t('domínio beta.local', 'domain beta.local')}
            </Typography.Text>
            <div className={styles.line} />
            <Typography.Text className={styles.neoLabel}>Peers</Typography.Text>
            <div className={styles.chipRow}>
              <span className={`${styles.chip} ${styles.statusOn}`}>peer0.beta.local</span>
              <span className={`${styles.chip} ${styles.statusOn}`}>peer1.beta.local</span>
            </div>
            <Typography.Text className={styles.neoLabel}>Orderers</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>orderer0.beta.local</span>
            </div>
          </div>
        </div>

        <div className={styles.line} />

        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Grupo de Negócio A', 'Business Group A')}
            </Typography.Title>
            <Typography.Text className={styles.neoLabel}>{t('Canais', 'Channels')}</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>canal-core</span>
              <span className={styles.chip}>canal-ativos</span>
            </div>
            <Typography.Text className={styles.neoLabel}>
              {t('Organizações', 'Organizations')}
            </Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>{t('Organização Alfa', 'Organization Alpha')}</span>
            </div>
          </div>

          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Grupo de Negócio B', 'Business Group B')}
            </Typography.Title>
            <Typography.Text className={styles.neoLabel}>{t('Canais', 'Channels')}</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>canal-compliance</span>
              <span className={styles.chip}>canal-relatorios</span>
              <span className={styles.chip}>canal-operacoes</span>
              <span className={styles.chip}>canal-sandbox</span>
            </div>
            <Typography.Text className={styles.neoLabel}>
              {t('Organizações', 'Organizations')}
            </Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>{t('Organização Beta', 'Organization Beta')}</span>
            </div>
          </div>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default OperationsConsolePage;
