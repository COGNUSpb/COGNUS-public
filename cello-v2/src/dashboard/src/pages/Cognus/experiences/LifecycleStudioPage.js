import React from 'react';
import { Button, Typography } from 'antd';
import { ScheduleOutlined, FileAddOutlined, ShareAltOutlined } from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';
import styles from '../components/NeoOpsLayout.less';

const navItems = [
  { key: 'overview', label: { ptBR: 'Dashboard', enUS: 'Dashboard' }, icon: <ScheduleOutlined /> },
  {
    key: 'template',
    label: { ptBR: 'Template de chaincode', enUS: 'Chaincode template' },
    icon: <FileAddOutlined />,
  },
  { key: 'lifecycle', label: { ptBR: 'Enviar fonte', enUS: 'Upload source' }, icon: <ShareAltOutlined /> },
];

const LifecycleStudioPage = () => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

  return (
    <NeoOpsLayout
      screenKey="e3-lifecycle"
      sectionLabel={{ ptBR: 'Lifecycle de Chaincode /', enUS: 'Chaincode lifecycle /' }}
      title={{ ptBR: 'Publicar pacote de chaincode', enUS: 'Publish chaincode package' }}
      subtitle={{
        ptBR: 'Gerencie versão, empacotamento e envio de artefatos com trilha auditável.',
        enUS: 'Manage versioning, packaging, and artifact delivery with an auditable trail.',
      }}
      navItems={navItems}
      activeNavKey="lifecycle"
      breadcrumbs={[
        { ptBR: 'Lifecycle', enUS: 'Lifecycle' },
        { ptBR: 'Chaincodes', enUS: 'Chaincodes' },
        { ptBR: 'Envio de fonte', enUS: 'Source upload' },
      ]}
    >
      <div className={styles.content}>
        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Etapa 1 · Dados do chaincode', 'Step 1 · Chaincode data')}
          </Typography.Title>
          <div className={styles.formGrid2}>
            <div className={styles.formField}>
              <span className={styles.formFieldLabel}>{t('Nome do chaincode', 'Chaincode name')}</span>
              <div className={styles.inputLike}>contrato-exemplo</div>
            </div>
            <div className={styles.formField}>
              <span className={styles.formFieldLabel}>{t('Versão', 'Version')}</span>
              <div className={styles.inputLike}>1.0.0</div>
            </div>
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Etapa 2 · Gerar pacote', 'Step 2 · Generate package')}
          </Typography.Title>
          <Typography.Paragraph className={styles.neoLabel}>
            {t(
              'Comando recomendado para empacotamento (pode ser ajustado pelo operador).',
              'Recommended packaging command (can be adjusted by the operator).'
            )}
          </Typography.Paragraph>
          <div className={styles.commandBlock}>
            export FABRIC_CFG_PATH=fabric/config && peer lifecycle chaincode package chaincode.tar.gz
            --path chaincode --lang golang --label contrato-exemplo_1.0.0
          </div>
        </div>

        <div className={styles.neoCard}>
          <Typography.Title level={4} className={styles.neoCardTitle}>
            {t('Etapa 3 · Enviar arquivo', 'Step 3 · Upload file')}
          </Typography.Title>
          <div className={styles.formGrid2}>
            <div className={styles.formField}>
              <span className={styles.formFieldLabel}>
                {t('Arquivo base do chaincode', 'Base chaincode file')}
              </span>
              <div className={styles.inputLike}>{t('Enviar arquivo(s)', 'Upload file(s)')}</div>
            </div>
            <div className={styles.formField}>
              <span className={styles.formFieldLabel}>
                {t('Configuração avançada', 'Advanced configuration')}
              </span>
              <div className={styles.inputLike}>
                {t(
                  'Init required · Política de endosso · Collection config',
                  'Init required · Endorsement policy · Collection config'
                )}
              </div>
            </div>
          </div>
          <div className={styles.footerActions}>
            <Button type="primary">{t('Publicar chaincode', 'Publish chaincode')}</Button>
          </div>
        </div>
      </div>
    </NeoOpsLayout>
  );
};

export default LifecycleStudioPage;
