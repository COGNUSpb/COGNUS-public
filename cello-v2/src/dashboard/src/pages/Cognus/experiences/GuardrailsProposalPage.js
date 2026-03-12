import React from 'react';
import { Button, Typography } from 'antd';
import { SafetyOutlined, ClusterOutlined, RollbackOutlined } from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';
import styles from '../components/NeoOpsLayout.less';

const navItems = [
  { key: 'lifecycle', label: { ptBR: 'Lifecycle', enUS: 'Lifecycle' }, icon: <ClusterOutlined /> },
  { key: 'guardrails', label: { ptBR: 'Guardrails', enUS: 'Guardrails' }, icon: <SafetyOutlined /> },
  { key: 'rollback', label: { ptBR: 'Rollback', enUS: 'Rollback' }, icon: <RollbackOutlined /> },
];

const channels = [
  { key: 'core', name: 'canal-core', chaincodes: ['contrato-core'], selected: true },
  { key: 'integracao', name: 'canal-integracao', chaincodes: ['contrato-integra'] },
  { key: 'sandbox', name: 'canal-sandbox', chaincodes: ['contrato-sandbox'] },
  { key: 'ops', name: 'canal-operacoes', chaincodes: ['contrato-ops'] },
  { key: 'auditoria', name: 'canal-auditoria', chaincodes: ['contrato-audit'] },
  { key: 'dados', name: 'canal-dados', chaincodes: ['contrato-dados'] },
];

const GuardrailsProposalPage = () => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

  return (
    <NeoOpsLayout
      screenKey="e3-guardrails"
      sectionLabel={{
        ptBR: 'Governança de Chaincode /',
        enUS: 'Chaincode Governance /',
      }}
      title={{
        ptBR: 'Adicionar Chaincode com Guardrails',
        enUS: 'Add chaincode with guardrails',
      }}
      subtitle={{
        ptBR: 'Selecione canal, escopo de peers e política de endosso antes da submissão.',
        enUS: 'Select the channel, peer scope, and endorsement policy before submission.',
      }}
      navItems={navItems}
      activeNavKey="guardrails"
      breadcrumbs={[
        { ptBR: 'Consórcio Alpha', enUS: 'Consortium Alpha' },
        { ptBR: 'Grupo de Negócio A', enUS: 'Business Group A' },
      ]}
    >
      <div className={styles.content}>
        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Escolha um canal', 'Choose a channel')}
          </Typography.Title>
          <div className={styles.selectGrid}>
            {channels.map(channel => (
              <div
                key={channel.key}
                className={
                  channel.selected
                    ? `${styles.selectCard} ${styles.selectCardSelected}`
                    : styles.selectCard
                }
              >
                <Typography.Text className={styles.neoValue}>{channel.name}</Typography.Text>
                <div className={styles.chipRow}>
                  {channel.chaincodes.map(chaincode => (
                    <span key={chaincode} className={styles.chip}>
                      {chaincode}
                    </span>
                  ))}
                  <span className={styles.chip}>
                    {t('Org Operadora 01', 'Operator Org 01')}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Text className={styles.neoLabel}>
            {t('Chaincodes disponíveis no canal', 'Chaincodes available in the channel')}
          </Typography.Text>
          <div className={styles.chipRow}>
            <span className={styles.chip}>contrato-core (3)</span>
            <span className={styles.chip}>contrato-audit (2)</span>
            <span className={styles.chip}>contrato-ops (5)</span>
            <span className={styles.chip}>contrato-sandbox (1)</span>
          </div>
          <div className={styles.line} />
          <Typography.Text className={styles.neoLabel}>{t('Peers alvo', 'Target peers')}</Typography.Text>
          <div className={styles.chipRow}>
            <span className={styles.chip}>peer0.org01.local</span>
            <span className={styles.chip}>peer1.org01.local</span>
          </div>
          <div className={styles.line} />
          <Typography.Text className={styles.neoLabel}>
            {t('Definir política de endosso', 'Define endorsement policy')}
          </Typography.Text>
          <div className={styles.footerActions}>
            <Button>{t('Configurar endosso', 'Configure endorsement')}</Button>
            <Button type="primary">{t('Adicionar chaincode', 'Add chaincode')}</Button>
          </div>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default GuardrailsProposalPage;
