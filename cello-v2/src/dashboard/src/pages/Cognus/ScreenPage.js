import React from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  List,
  Row,
  Space,
  Statistic,
  Table,
  Tag,
  Timeline,
  Typography,
} from 'antd';
import {
  ArrowLeftOutlined,
  ArrowRightOutlined,
  CheckCircleTwoTone,
  ClockCircleOutlined,
  ExclamationCircleTwoTone,
  WarningTwoTone,
} from '@ant-design/icons';
import { Link } from 'umi';
import PageHeaderWrapper from '@/components/PageHeaderWrapper';
import BackendStatusBadge from './components/BackendStatusBadge';
import { pickCognusText, resolveCognusLocale } from './cognusI18n';
import { backendStatusMeta, screenByKey } from './data/screens';
import {
  getFlowAdjacentScreens,
  getGroupByScreenKey,
  getScreenPath,
  screenKeyByPath,
} from './data/navigation';
import styles from './ScreenPage.less';

const normalizePath = pathname => pathname.replace(/\/$/, '') || '/';

const getTableColumns = locale => {
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);

  return [
    {
      title: t('Item', 'Item'),
      dataIndex: 'item',
      key: 'item',
    },
    {
      title: t('Escopo', 'Scope'),
      dataIndex: 'scope',
      key: 'scope',
    },
    {
      title: t('Estado da tela', 'Screen state'),
      dataIndex: 'state',
      key: 'state',
      render: value => <Tag color="blue">{value}</Tag>,
    },
    {
      title: t('Backend', 'Backend'),
      dataIndex: 'backendReady',
      key: 'backendReady',
      render: backendReady =>
        backendReady ? (
          <Tag color="green">{t('Disponível', 'Available')}</Tag>
        ) : (
          <Tag color="purple">{t('Pendente', 'Pending')}</Tag>
        ),
    },
  ];
};

const getStatusAlert = (screen, locale) => {
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);
  const meta = backendStatusMeta[screen.backendStatus] || backendStatusMeta.pending;
  if (!meta.warning) {
    return null;
  }

  return (
    <Alert
      className={styles.backendAlert}
      message={t('Atenção: backend ainda não finalizado', 'Attention: backend not finished yet')}
      description={screen.backendNote || meta.description}
      type="warning"
      showIcon
      icon={<WarningTwoTone twoToneColor="#D97706" />}
    />
  );
};

const ScreenPage = ({ location }) => {
  const locale = resolveCognusLocale();
  const t = (ptBR, enUS) => pickCognusText(ptBR, enUS, locale);
  const path = normalizePath(location.pathname);
  const screenKey = screenKeyByPath[path];
  const screen = screenByKey[screenKey];
  const tableColumns = getTableColumns(locale);

  if (!screen) {
    return (
      <PageHeaderWrapper>
        <Card>
          <Typography.Title level={4}>{t('Tela não mapeada', 'Unmapped screen')}</Typography.Title>
          <Typography.Paragraph>
            {t(
              'A rota acessada não está cadastrada no catálogo de telas do COGNUS.',
              'The accessed route is not registered in the COGNUS screen catalog.'
            )}
          </Typography.Paragraph>
          <Link to="/overview">
            <Button type="primary">{t('Voltar para visão integrada', 'Back to integrated view')}</Button>
          </Link>
        </Card>
      </PageHeaderWrapper>
    );
  }

  const group = getGroupByScreenKey(screen.key);
  const { previous, next } = getFlowAdjacentScreens(screen.key);

  return (
    <PageHeaderWrapper>
      <div className={styles.screenRoot}>
        <Card className={styles.heroCard}>
          <div className={styles.heroHeader}>
            <Space wrap>
              <Tag color="geekblue">{group ? group.label : t('Módulo', 'Module')}</Tag>
              <Tag color="default" className={styles.codeTag}>
                change-id: MOCK-{screen.key.toUpperCase()}
              </Tag>
            </Space>
            <BackendStatusBadge status={screen.backendStatus} />
          </div>
          <Typography.Title level={3} className={styles.heroTitle}>
            {screen.title}
          </Typography.Title>
          <Typography.Paragraph className={styles.heroSummary}>
            {screen.summary}
          </Typography.Paragraph>
          <Space wrap>
            <Tag color="blue">Org: org-alpha</Tag>
            <Tag color="cyan">{t('Canal: canal-principal', 'Channel: main-channel')}</Tag>
            <Tag color="purple">{t('Ambiente: hml', 'Environment: hml')}</Tag>
            <Tag color="purple">{t('Perfil: operador', 'Profile: operator')}</Tag>
          </Space>
        </Card>

        {getStatusAlert(screen, locale)}

        <>
          <Row gutter={[16, 16]}>
            {screen.metrics.map(metric => (
              <Col key={metric.label} xs={24} md={8}>
                <Card className={styles.metricCard}>
                  <Statistic title={metric.label} value={metric.value} />
                </Card>
              </Col>
            ))}
          </Row>

          <Row gutter={[16, 16]}>
            <Col xs={24} lg={12}>
              <Card
                title={t('Checkpoints da tela', 'Screen checkpoints')}
                className={styles.contentCard}
              >
                <Timeline>
                  {screen.checkpoints.map(checkpoint => (
                    <Timeline.Item key={checkpoint} dot={<ClockCircleOutlined />}>
                      {checkpoint}
                    </Timeline.Item>
                  ))}
                </Timeline>
              </Card>
            </Col>
            <Col xs={24} lg={12}>
              <Card
                title={t('Ações e integração backend', 'Actions and backend integration')}
                className={styles.contentCard}
              >
                <List
                  dataSource={screen.actions}
                  renderItem={action => (
                    <List.Item className={styles.actionRow}>
                      <Space>
                        {action.backendReady ? (
                          <CheckCircleTwoTone twoToneColor="#16A34A" />
                        ) : (
                          <ExclamationCircleTwoTone twoToneColor="#D97706" />
                        )}
                        <span>{action.label}</span>
                      </Space>
                      {action.backendReady ? (
                        <Tag color="green">{t('Integrado', 'Integrated')}</Tag>
                      ) : (
                        <Tag color="purple">{t('Sem backend', 'No backend')}</Tag>
                      )}
                    </List.Item>
                  )}
                />
              </Card>
            </Col>
          </Row>

          <Card
            title={t('Dados operacionais simulados', 'Simulated operational data')}
            className={styles.contentCard}
          >
            <Table
              columns={tableColumns}
              dataSource={screen.mockRows}
              pagination={false}
              rowKey="key"
            />
          </Card>

          <Card
            title={t('Evidências obrigatórias desta tela', 'Required evidence for this screen')}
            className={styles.contentCard}
          >
            <Space wrap>
              {screen.evidences.map(evidence => (
                <Tag key={evidence} color="blue" className={styles.evidenceTag}>
                  {evidence}
                </Tag>
              ))}
            </Space>
          </Card>
        </>

        <Card className={styles.navigationCard}>
          <div className={styles.navigationContent}>
            <Space>
              {previous ? (
                <Link to={getScreenPath(previous.key)}>
                  <Button icon={<ArrowLeftOutlined />}>{t('Tela anterior', 'Previous screen')}</Button>
                </Link>
              ) : (
                <Button icon={<ArrowLeftOutlined />} disabled>
                  {t('Tela anterior', 'Previous screen')}
                </Button>
              )}
              <Link to="/overview">
                <Button>{t('Visão integrada', 'Integrated view')}</Button>
              </Link>
            </Space>
            {next ? (
              <Link to={getScreenPath(next.key)}>
                <Button type="primary" icon={<ArrowRightOutlined />}>
                  {t('Próxima tela', 'Next screen')}
                </Button>
              </Link>
            ) : (
              <Button type="primary" icon={<ArrowRightOutlined />} disabled>
                {t('Próxima tela', 'Next screen')}
              </Button>
            )}
          </div>
        </Card>
      </div>
    </PageHeaderWrapper>
  );
};

export default ScreenPage;
