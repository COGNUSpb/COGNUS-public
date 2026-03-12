import React from 'react';
import { Tag } from 'antd';
import { CheckCircleTwoTone, InfoCircleTwoTone, WarningTwoTone } from '@ant-design/icons';
import { getLocale } from 'umi';
import styles from './BackendStatusBadge.less';
import { pickCognusText } from '../cognusI18n';

const toneByStatus = {
  ready: '#16A34A',
  partial: '#0284C7',
  pending: '#D97706',
};

const iconByStatus = {
  ready: <CheckCircleTwoTone twoToneColor="#16A34A" />,
  partial: <InfoCircleTwoTone twoToneColor="#0284C7" />,
  pending: <WarningTwoTone twoToneColor="#D97706" />,
};

const BackendStatusBadge = ({ status }) => {
  const locale = getLocale();
  const labelByStatus = {
    ready: pickCognusText('Backend implementado', 'Backend implemented', locale),
    partial: pickCognusText('Backend parcial', 'Partial backend', locale),
    pending: pickCognusText('Backend pendente', 'Backend pending', locale),
  };
  const icon = iconByStatus[status] || iconByStatus.pending;
  const color = toneByStatus[status] || toneByStatus.pending;

  return (
    <Tag className={styles.badge} color={color}>
      <span className={styles.content}>
        {icon}
        <span>{labelByStatus[status] || labelByStatus.pending}</span>
      </span>
    </Tag>
  );
};

export default BackendStatusBadge;
