import React from 'react';
import { Alert, Space, Typography } from 'antd';
import { WarningTwoTone } from '@ant-design/icons';
import { getLocale } from 'umi';
import BackendStatusBadge from './BackendStatusBadge';
import { backendStatusMeta, screenByKey } from '../data/screens';
import styles from './ScreenReadinessBanner.less';
import { pickCognusText } from '../cognusI18n';

const ScreenReadinessBanner = ({ screenKey }) => {
  const locale = getLocale();
  const screen = screenByKey[screenKey];
  if (!screen) {
    return null;
  }

  const status = backendStatusMeta[screen.backendStatus] || backendStatusMeta.pending;
  const backendDescriptionByStatus = {
    ready: pickCognusText(
      'Esta tela já possui integração backend disponível.',
      'This screen already has backend integration available.',
      locale
    ),
    partial: pickCognusText(
      'A tela possui integração parcial e ainda depende de endpoints do orquestrador.',
      'This screen has partial integration and still depends on orchestrator endpoints.',
      locale
    ),
    pending: pickCognusText(
      'A integração backend desta tela ainda não foi implementada.',
      'Backend integration for this screen has not been implemented yet.',
      locale
    ),
  };
  const scopeAlertTitle = pickCognusText(
    'Escopo tecnico obrigatorio',
    'Mandatory technical scope',
    locale
  );
  const scopeAlertMessage = pickCognusText(
    'Fluxo E1 restrito a external provider + VM Linux. Integracoes fora desse recorte permanecem bloqueadas.',
    'E1 flow restricted to external provider + Linux VM. Integrations outside this scope remain blocked.',
    locale
  );

  return (
    <div className={styles.wrapper}>
      <div className={styles.summary}>
        <Space direction="vertical" size={2}>
          <Typography.Text className={styles.label}>
            {pickCognusText('Estado da integração backend', 'Backend integration status', locale)}
          </Typography.Text>
          <Typography.Text className={styles.description}>
            {backendDescriptionByStatus[screen.backendStatus] || backendDescriptionByStatus.pending}
          </Typography.Text>
        </Space>
        <BackendStatusBadge status={screen.backendStatus} />
      </div>
      {status.warning && (
        <Alert
          showIcon
          type="warning"
          icon={<WarningTwoTone twoToneColor="#D97706" />}
          message={pickCognusText(
            'Readiness parcial/pendente: degrade controlado ativo',
            'Partial/pending readiness: controlled degraded mode active',
            locale
          )}
          description={screen.backendNote || backendDescriptionByStatus[screen.backendStatus]}
          className={styles.alert}
        />
      )}
      <Alert
        showIcon
        type="info"
        message={scopeAlertTitle}
        description={scopeAlertMessage}
      />
    </div>
  );
};

export default ScreenReadinessBanner;
