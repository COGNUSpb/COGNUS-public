import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Button, Progress, Space, Tag, Typography } from 'antd';
import { Link, useLocation } from 'umi';
import { ArrowLeftOutlined, ArrowRightOutlined, SendOutlined } from '@ant-design/icons';
import NeoOpsLayout from '../components/NeoOpsLayout';
import styles from '../components/NeoOpsLayout.less';
import { screenByKey } from '../data/screens';
import { formatCognusTemplate, resolveCognusLocale } from '../cognusI18n';
import ProvisioningReadinessCard from './ProvisioningReadinessCard';
import {
  getProvisioningBreadcrumbs,
  PROVISIONING_SECTION_LABEL,
  provisioningNavItems,
  resolveProvisioningActiveNavKey,
} from './provisioningNavigation';
import { PROVISIONING_TECHNICAL_HUB_SCREEN_KEY } from '../data/provisioningContract';

const technicalHubReadinessActionOrder = [
  'open_blueprint_screen',
  'open_runbook_screen',
  'open_inventory_screen',
  'open_lifecycle_screen',
  'open_guardrails_screen',
  'open_rollback_screen',
];
const ProvisioningTechnicalHubPage = () => {
  const location = useLocation();
  const [activeStepIndex, setActiveStepIndex] = useState(0);
  const locale = resolveCognusLocale();
  const t = useCallback(
    (ptBR, enUS, values) => formatCognusTemplate(ptBR, enUS, values, locale),
    [locale]
  );
  const screen = screenByKey[PROVISIONING_TECHNICAL_HUB_SCREEN_KEY];
  const advancedTechnicalSteps = useMemo(
    () => [
      {
        key: 'e1-blueprint',
        title: t('Blueprint e versionamento', 'Blueprint and versioning'),
        path: '/provisioning/blueprints',
        tone: 'blue',
        readinessLabel: t('backend parcial', 'partial backend'),
        summary: t(
          'Modelagem de topologia, lint técnico e publicação de versão rastreável por change_id.',
          'Topology modeling, technical lint, and publication of a version traceable by change_id.'
        ),
      },
      {
        key: 'e1-provisionamento',
        title: t('Provisão assistida', 'Assisted provisioning'),
        path: '/provisioning/runbook',
        tone: 'green',
        readinessLabel: t('backend implementado', 'backend implemented'),
        summary: t(
          'Execução guiada do runbook prepare -> provision -> reconcile -> verify com checkpoints.',
          'Guided execution of the prepare -> provision -> reconcile -> verify runbook with checkpoints.'
        ),
      },
      {
        key: 'e1-inventario',
        title: t('Inventário inicial', 'Initial inventory'),
        path: '/provisioning/inventory',
        tone: 'cyan',
        readinessLabel: t('backend híbrido', 'hybrid backend'),
        summary: t(
          'Consolidação de organizações, nós, canais e baseline de evidências do ciclo inicial.',
          'Consolidation of organizations, nodes, channels, and the evidence baseline for the initial cycle.'
        ),
      },
      {
        key: 'e3-lifecycle',
        title: t('Lifecycle de chaincode (Gateway API)', 'Chaincode lifecycle (Gateway API)'),
        path: '/chaincode-ops/lifecycle',
        tone: 'blue',
        readinessLabel: t('backend parcial', 'partial backend'),
        summary: t(
          'Orquestração de package/install/approve/commit com rastreabilidade operacional por canal.',
          'Orchestration of package/install/approve/commit with operational traceability per channel.'
        ),
      },
      {
        key: 'e3-guardrails',
        title: t('Guardrails de disponibilidade', 'Availability guardrails'),
        path: '/chaincode-ops/guardrails',
        tone: 'orange',
        readinessLabel: t('backend pendente', 'backend pending'),
        summary: t(
          'Pré-checagens e bloqueios preventivos para evitar mudanças com risco operacional elevado.',
          'Pre-checks and preventive blocks to avoid changes with high operational risk.'
        ),
      },
      {
        key: 'e3-rollback',
        title: t('Versões e rollback', 'Versions and rollback'),
        path: '/chaincode-ops/rollback',
        tone: 'blue',
        readinessLabel: t('backend parcial', 'partial backend'),
        summary: t(
          'Comparação de versões, seleção de ponto seguro e rollback assistido com justificativa técnica.',
          'Version comparison, safe point selection, and assisted rollback with technical justification.'
        ),
      },
    ],
    [t]
  );

  const locationQuery = useMemo(() => {
    const search = location && location.search ? location.search : '';
    return new URLSearchParams(search);
  }, [location]);

  const transitionSource = String(locationQuery.get('source') || '').trim();
  const transitionChangeId = String(locationQuery.get('change_id') || '').trim();

  useEffect(() => {
    const requestedStep = Number(locationQuery.get('step'));
    if (!Number.isInteger(requestedStep)) {
      return;
    }
    if (requestedStep < 0 || requestedStep >= advancedTechnicalSteps.length) {
      return;
    }
    setActiveStepIndex(requestedStep);
  }, [advancedTechnicalSteps.length, locationQuery]);

  const activeStep = advancedTechnicalSteps[activeStepIndex];
  const isFirstStep = activeStepIndex === 0;
  const isLastStep = activeStepIndex === advancedTechnicalSteps.length - 1;
  const progressPercent = useMemo(
    () => Math.round(((activeStepIndex + 1) / advancedTechnicalSteps.length) * 100),
    [activeStepIndex, advancedTechnicalSteps.length]
  );

  return (
    <NeoOpsLayout
      screenKey={PROVISIONING_TECHNICAL_HUB_SCREEN_KEY}
      sectionLabel={PROVISIONING_SECTION_LABEL}
      title={screen.title}
      subtitle={screen.summary}
      navItems={provisioningNavItems}
      activeNavKey={resolveProvisioningActiveNavKey(PROVISIONING_TECHNICAL_HUB_SCREEN_KEY)}
      breadcrumbs={getProvisioningBreadcrumbs(PROVISIONING_TECHNICAL_HUB_SCREEN_KEY)}
    >
      <Alert
        showIcon
        type="info"
        message={t('Jornada avançada em tela única', 'Advanced journey in a single screen')}
        description={t(
          'Execute as seis etapas em sequência usando Próximo/Anterior. Quando precisar operar, abra a etapa atual no detalhe técnico.',
          'Execute the six steps in sequence using Next/Previous. When you need to operate, open the current step in the technical detail.'
        )}
      />

      {transitionSource === 'infra-ssh' && (
        <Alert
          showIcon
          type="success"
          message={t(
            'Transição assistida recebida do onboarding SSH',
            'Assisted transition received from SSH onboarding'
          )}
          description={t(
            'Contexto transferido do subtópico de Infra SSH{suffix}. Continue na Etapa 1 e avance conforme ajustes técnicos necessários.',
            'Context transferred from the Infra SSH subtopic{suffix}. Continue at Step 1 and advance according to the required technical adjustments.',
            {
              suffix: transitionChangeId ? ` ${t('para', 'for')} change_id ${transitionChangeId}` : '',
            }
          )}
        />
      )}

      <div className={styles.neoCard}>
        <Space
          align="start"
          style={{ width: '100%', justifyContent: 'space-between', marginBottom: 8 }}
          wrap
        >
          <div>
            <Typography.Text className={styles.neoLabel}>
              {t('Etapa ativa', 'Active step')}
            </Typography.Text>
            <Typography.Title level={4} className={styles.neoCardTitle}>
              {t('Etapa {step} · {title}', 'Step {step} · {title}', {
                step: activeStepIndex + 1,
                title: activeStep.title,
              })}
            </Typography.Title>
          </div>
          <Space wrap size={8}>
            <Tag color="blue">
              {t('Passo {step}/{total}', 'Step {step}/{total}', {
                step: activeStepIndex + 1,
                total: advancedTechnicalSteps.length,
              })}
            </Tag>
            <Tag color={activeStep.tone}>{activeStep.readinessLabel}</Tag>
          </Space>
        </Space>

        <Typography.Paragraph style={{ marginBottom: 12 }}>
          {activeStep.summary}
        </Typography.Paragraph>

        <Progress percent={progressPercent} size="small" />

        <div className={styles.steps} style={{ marginTop: 12 }}>
          {advancedTechnicalSteps.map((step, index) => (
            <button
              key={step.key}
              type="button"
              className={
                index === activeStepIndex
                  ? `${styles.step} ${styles.stepActive} ${styles.stepButton}`
                  : `${styles.step} ${styles.stepButton}`
              }
              onClick={() => setActiveStepIndex(index)}
            >
              <Typography.Text className={styles.neoLabel}>
                {t('Etapa {step}', 'Step {step}', { step: index + 1 })}
              </Typography.Text>
              <Typography.Text className={styles.neoValue}>{step.title}</Typography.Text>
            </button>
          ))}
        </div>

        <div className={styles.footerActions}>
          <Button
            icon={<ArrowLeftOutlined />}
            disabled={isFirstStep}
            onClick={() => setActiveStepIndex(current => Math.max(0, current - 1))}
          >
            {t('Etapa anterior', 'Previous step')}
          </Button>
          <Button
            icon={<ArrowRightOutlined />}
            disabled={isLastStep}
            onClick={() =>
              setActiveStepIndex(current =>
                Math.min(advancedTechnicalSteps.length - 1, current + 1)
              )
            }
          >
            {t('Próxima etapa', 'Next step')}
          </Button>
          <Link to={activeStep.path}>
            <Button type="primary" icon={<SendOutlined />}>
              {t('Abrir etapa técnica', 'Open technical step')}
            </Button>
          </Link>
        </div>
      </div>

      <ProvisioningReadinessCard
        screenKey={PROVISIONING_TECHNICAL_HUB_SCREEN_KEY}
        actionOrder={technicalHubReadinessActionOrder}
      />
    </NeoOpsLayout>
  );
};

export default ProvisioningTechnicalHubPage;
