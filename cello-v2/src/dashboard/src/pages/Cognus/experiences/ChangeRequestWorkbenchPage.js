import React from 'react';
import { Button, Space, Typography } from 'antd';
import { EditOutlined, TeamOutlined, UnorderedListOutlined } from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';
import styles from '../components/NeoOpsLayout.less';

const navItems = [
  {
    key: 'changeRequest',
    label: { ptBR: 'Abrir solicitação de mudança', enUS: 'Open change request' },
    icon: <EditOutlined />,
  },
  { key: 'approvals', label: { ptBR: 'Aprovações', enUS: 'Approvals' }, icon: <TeamOutlined /> },
  { key: 'timeline', label: { ptBR: 'Linha do tempo', enUS: 'Timeline' }, icon: <UnorderedListOutlined /> },
];

const ChangeRequestWorkbenchPage = () => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

  return (
    <NeoOpsLayout
      screenKey="e2-cr"
      sectionLabel={{ ptBR: 'Mudanças /', enUS: 'Changes /' }}
      title={{ ptBR: 'Nova Solicitação de Mudança', enUS: 'New change request' }}
      subtitle={{
        ptBR: 'Assistente de abertura com escopo técnico, evidências e governança multi-organização.',
        enUS: 'Opening assistant with technical scope, evidence, and multi-organization governance.',
      }}
      navItems={navItems}
      activeNavKey="changeRequest"
      breadcrumbs={[
        { ptBR: 'Consórcio Alpha', enUS: 'Consortium Alpha' },
        { ptBR: 'Plano operacional Q1', enUS: 'Operational plan Q1' },
      ]}
    >
      <div className={styles.content}>
        <div className={styles.neoCard}>
          <Typography.Text className={styles.neoLabel}>
            {t('Fases da criação de CR', 'CR creation phases')}
          </Typography.Text>
          <div className={styles.steps}>
            <div className={`${styles.step} ${styles.stepActive}`}>
              {t('1. Organizações', '1. Organizations')}
            </div>
            <div className={styles.step}>{t('2. Nós críticos', '2. Critical nodes')}</div>
            <div className={styles.step}>{t('3. Grupos de negócio', '3. Business groups')}</div>
            <div className={styles.step}>{t('4. Canais', '4. Channels')}</div>
            <div className={styles.step}>{t('5. Compilar chaincodes', '5. Build chaincodes')}</div>
            <div className={styles.step}>{t('6. Instalar chaincodes', '6. Install chaincodes')}</div>
          </div>
        </div>

        <div className={styles.neoGrid2}>
          <div className={styles.neoCard}>
            <Typography.Text className={styles.neoLabel}>
              {t('Organizações no plano', 'Organizations in the plan')}
            </Typography.Text>
            <div className={styles.selectCardSelected} style={{ marginTop: 10 }}>
              <div className={styles.selectCard}>
                <Typography.Text className={styles.neoValue}>
                  {t('Org Operadora 01', 'Operator Org 01')}
                </Typography.Text>
                <div className={styles.chipRow}>
                  <span className={styles.chip}>2 peers</span>
                  <span className={styles.chip}>1 orderer</span>
                  <span className={styles.chip}>1 API</span>
                </div>
              </div>
            </div>
            <div className={styles.selectCard} style={{ marginTop: 12, textAlign: 'center' }}>
              <Typography.Text className={styles.neoLabel}>
                {t('+ Adicionar organização ao CR', '+ Add organization to the CR')}
              </Typography.Text>
            </div>
          </div>

          <div className={styles.neoCard}>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Detalhes da Organização', 'Organization details')}
            </Typography.Title>
            <div className={styles.formGrid2}>
              <div className={styles.formField}>
                <span className={styles.formFieldLabel}>
                  {t('Nome da organização', 'Organization name')}
                </span>
                <div className={styles.inputLike}>{t('Org Operadora 01', 'Operator Org 01')}</div>
              </div>
              <div className={styles.formField}>
                <span className={styles.formFieldLabel}>{t('Domínio', 'Domain')}</span>
                <div className={styles.inputLike}>org01.local</div>
              </div>
              <div className={styles.formField}>
                <span className={styles.formFieldLabel}>
                  {t('Host da API de Rede', 'Network API host')}
                </span>
                <div className={styles.inputLike}>10.30.0.15</div>
              </div>
              <div className={styles.formField}>
                <span className={styles.formFieldLabel}>
                  {t('Tipo de certificação', 'Certification type')}
                </span>
                <div className={styles.inputLike}>
                  {t('Autoridade certificadora', 'Certificate authority')}
                </div>
              </div>
            </div>
            <div className={styles.line} />
            <Typography.Text className={styles.neoLabel}>
              {t('Configuração avançada', 'Advanced configuration')}
            </Typography.Text>
            <div className={styles.chipRow}>
              <span className={styles.chip}>{t('Host da CA: 0.0.0.0', 'CA host: 0.0.0.0')}</span>
              <span className={styles.chip}>{t('Usuário da CA: admin', 'CA user: admin')}</span>
              <span className={styles.chip}>{t('TLS obrigatório', 'TLS required')}</span>
            </div>
          </div>
        </div>

        <div className={styles.footerActions}>
          <Space>
            <Button>{t('Fechar', 'Close')}</Button>
            <Button type="primary">{t('Próximo', 'Next')}</Button>
          </Space>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default ChangeRequestWorkbenchPage;
