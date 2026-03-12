import React from 'react';
import { Space, Tag, Typography } from 'antd';
import { Link } from 'umi';
import PageHeaderWrapper from '@/components/PageHeaderWrapper';
import ScreenReadinessBanner from './ScreenReadinessBanner';
import { pickCognusText } from '../cognusI18n';
import styles from './NeoOpsLayout.less';

const resolveLayoutLabel = labelCandidate => {
  if (
    labelCandidate &&
    typeof labelCandidate === 'object' &&
    typeof labelCandidate.ptBR === 'string' &&
    typeof labelCandidate.enUS === 'string'
  ) {
    return pickCognusText(labelCandidate.ptBR, labelCandidate.enUS);
  }

  return labelCandidate;
};

const NeoOpsLayout = ({
  screenKey,
  sectionLabel,
  title,
  subtitle,
  navItems,
  activeNavKey,
  toolbar,
  breadcrumbs,
  children,
}) => (
  <PageHeaderWrapper>
    <div className={styles.root}>
      <div className={styles.surface}>
        <div className={styles.topBar}>
          <Space size={8} wrap>
            <Typography.Text className={styles.sectionLabel}>
              {resolveLayoutLabel(sectionLabel)}
            </Typography.Text>
            {!!breadcrumbs &&
              breadcrumbs.map(crumb => (
                <Tag key={crumb} className={styles.breadcrumbTag}>
                  {resolveLayoutLabel(crumb)}
                </Tag>
              ))}
          </Space>
        </div>

        <div className={styles.header}>
          <div>
            <Typography.Title level={2} className={styles.title}>
              {resolveLayoutLabel(title)}
            </Typography.Title>
            <Typography.Paragraph className={styles.subtitle}>
              {resolveLayoutLabel(subtitle)}
            </Typography.Paragraph>
          </div>
          <div>{toolbar}</div>
        </div>

        {!!navItems.length && (
          <div className={styles.navList}>
            {navItems.map(item => {
              const navClassName =
                item.key === activeNavKey
                  ? `${styles.navItem} ${styles.navItemActive}`
                  : styles.navItem;

              const content = (
                <>
                  <span className={styles.navIcon}>{item.icon}</span>
                  <span>{resolveLayoutLabel(item.label)}</span>
                </>
              );

              if (item.path) {
                return (
                  <Link
                    key={item.key}
                    to={item.path}
                    className={navClassName}
                    aria-current={item.key === activeNavKey ? 'page' : undefined}
                  >
                    {content}
                  </Link>
                );
              }

              return (
                <div key={item.key} className={navClassName}>
                  {content}
                </div>
              );
            })}
          </div>
        )}

        <ScreenReadinessBanner screenKey={screenKey} />

        <div className={styles.content}>{children}</div>
      </div>
    </div>
  </PageHeaderWrapper>
);

NeoOpsLayout.defaultProps = {
  navItems: [],
  breadcrumbs: null,
  toolbar: null,
};

export default NeoOpsLayout;
