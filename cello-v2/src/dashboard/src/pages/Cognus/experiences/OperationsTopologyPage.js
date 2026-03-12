import React from 'react';
import { Button, Space, Typography } from 'antd';
import {
  ApiOutlined,
  ScheduleOutlined,
  DeploymentUnitOutlined,
  SafetyCertificateOutlined,
  SettingOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';
import styles from '../components/NeoOpsLayout.less';

const navItems = [
  { key: 'overview', label: { ptBR: 'Dashboard', enUS: 'Dashboard' }, icon: <ScheduleOutlined /> },
  {
    key: 'ca',
    label: { ptBR: 'Autoridade certificadora', enUS: 'Certificate authority' },
    icon: <SafetyCertificateOutlined />,
  },
  { key: 'peers', label: { ptBR: 'Peers', enUS: 'Peers' }, icon: <DeploymentUnitOutlined /> },
  { key: 'apis', label: { ptBR: 'APIs', enUS: 'APIs' }, icon: <ApiOutlined /> },
  { key: 'settings', label: { ptBR: 'Configurações', enUS: 'Settings' }, icon: <SettingOutlined /> },
];

const OperationsTopologyPage = () => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

  return (
    <NeoOpsLayout
      screenKey="e4-topologia"
      sectionLabel={{ ptBR: 'Topologia de Rede /', enUS: 'Network topology /' }}
      title={{ ptBR: 'Organização Alfa', enUS: 'Organization Alpha' }}
      subtitle={{
        ptBR: 'Visão consolidada de nós e serviços por organização e grupo de negócio.',
        enUS: 'Consolidated view of nodes and services by organization and business group.',
      }}
      navItems={navItems}
      activeNavKey="overview"
      breadcrumbs={[
        { ptBR: 'Consórcio Alpha', enUS: 'Consortium Alpha' },
        { ptBR: 'perfil-operacao', enUS: 'operations-profile' },
        { ptBR: 'v1.0', enUS: 'v1.0' },
      ]}
      toolbar={
        <Button type="primary" icon={<TeamOutlined />}>
          {t('Convidar membros', 'Invite members')}
        </Button>
      }
    >
      <div className={styles.content}>
        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Autoridade certificadora', 'Certificate authority')}
            </Typography.Title>
            <div className={styles.chipRow}>
              <span className={styles.chip}>ca-org-alfa</span>
              <span className={styles.chip}>10.30.0.21</span>
              <span className={`${styles.chip} ${styles.statusOn}`}>{t('Ativo', 'Active')}</span>
            </div>
          </div>
          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('API de Rede', 'Network API')}
            </Typography.Title>
            <div className={styles.chipRow}>
              <span className={styles.chip}>netapi-org-alfa</span>
              <span className={styles.chip}>10.30.0.22</span>
              <span className={`${styles.chip} ${styles.statusOn}`}>{t('Ativo', 'Active')}</span>
            </div>
          </div>
        </div>

        <div className={styles.neoGrid3}>
          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>Peers</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>peer0.org-alfa.local</span>
              <span className={styles.chip}>peer1.org-alfa.local</span>
            </div>
          </div>
          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>Orderers</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>orderer0.org-alfa.local</span>
            </div>
          </div>
          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>APIs</Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>ccapi-org-alfa</span>
              <span className={styles.chip}>10.30.0.23</span>
            </div>
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Grupos de negócio', 'Business groups')}
          </Typography.Title>
          <Space direction="vertical">
            <Typography.Text className={styles.neoValue}>
              {t('Grupo de Negócio A', 'Business Group A')}
            </Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>canal-core</span>
              <span className={styles.chip}>canal-ativos</span>
              <span className={styles.chip}>canal-relatorios</span>
              <span className={styles.chip}>canal-auditoria</span>
            </div>
          </Space>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default OperationsTopologyPage;
