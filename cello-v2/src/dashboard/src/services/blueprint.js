import request from '../utils/request';
import {
  pickCognusText,
  resolveCognusLocale,
} from '../pages/Cognus/cognusI18n';
import {
  isProvisioningLocalModeBlockedByPolicy,
  isProvisioningLocalModePermitted,
  recordProvisioningFallbackUsage,
} from './provisioningEnvironmentPolicy';

const BLUEPRINT_LOCAL_MODE_FLAG = String(process.env.COGNUS_BLUEPRINT_LOCAL_MODE || '')
  .trim()
  .toLowerCase();

export const BLUEPRINT_LOCAL_MODE_FLAG_ENABLED =
  BLUEPRINT_LOCAL_MODE_FLAG === '1' || BLUEPRINT_LOCAL_MODE_FLAG === 'true';
export const BLUEPRINT_LOCAL_MODE_ENABLED = isProvisioningLocalModePermitted(
  BLUEPRINT_LOCAL_MODE_FLAG_ENABLED
);
export const BLUEPRINT_LOCAL_MODE_BLOCKED_BY_POLICY = isProvisioningLocalModeBlockedByPolicy(
  BLUEPRINT_LOCAL_MODE_FLAG_ENABLED
);
const BLUEPRINT_LOCAL_STORAGE_KEY = 'cognus.blueprints.local.versions.v1';

const SENSITIVE_CREDENTIAL_KEYS = new Set([
  'private_key',
  'private_key_pem',
  'ssh_private_key',
  'ssh_key',
  'password',
  'passphrase',
]);

const SENSITIVE_REFERENCE_KEYS = new Set([
  'private_key_ref',
  'vault_ref',
  'secret_ref',
  'credential_ref',
  'token_ref',
]);

const localizeBlueprintText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const sanitizeSensitivePayload = payload => {
  if (Array.isArray(payload)) {
    return payload.map(sanitizeSensitivePayload);
  }

  if (!payload || typeof payload !== 'object') {
    return payload;
  }

  return Object.entries(payload).reduce((accumulator, [key, value]) => {
    if (SENSITIVE_CREDENTIAL_KEYS.has(key)) {
      return accumulator;
    }

    if (SENSITIVE_REFERENCE_KEYS.has(key)) {
      accumulator[key] = '[PROTECTED_REF]';
      return accumulator;
    }

    accumulator[key] = sanitizeSensitivePayload(value);
    return accumulator;
  }, {});
};

const createLightweightFingerprint = blueprint => {
  const payload = JSON.stringify(blueprint || {});
  let hash = 0;
  for (let index = 0; index < payload.length; index += 1) {
    hash = (hash * 31 + payload.charCodeAt(index)) % 4294967291;
  }
  return `local-${Math.abs(hash)
    .toString(16)
    .padStart(8, '0')}`;
};

const getStorage = () => {
  if (typeof window === 'undefined' || !window.localStorage) {
    return null;
  }
  return window.localStorage;
};

const loadLocalBlueprintVersions = () => {
  const storage = getStorage();
  if (!storage) {
    return [];
  }

  try {
    const rawPayload = storage.getItem(BLUEPRINT_LOCAL_STORAGE_KEY);
    if (!rawPayload) {
      return [];
    }
    const parsed = JSON.parse(rawPayload);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
};

const persistLocalBlueprintVersions = versions => {
  const storage = getStorage();
  if (!storage) {
    return;
  }
  storage.setItem(
    BLUEPRINT_LOCAL_STORAGE_KEY,
    JSON.stringify(Array.isArray(versions) ? versions : [])
  );
};

const buildDefaultLocalVersionRecord = () => {
  const now = toIsoUtc();
  return {
    id: `local-${Date.now()}`,
    blueprint_version: '0.0.0-local-onboarding',
    schema_version: '1.0.0',
    resolved_schema_version: '1.0.0',
    fingerprint_sha256: 'local-seed',
    change_id: 'local-change',
    execution_context: 'Local blueprint service mode',
    published_at_utc: now,
    lint_generated_at_utc: now,
    valid: true,
    issues: [],
    warnings: [],
    hints: [],
    normalized_blueprint: {},
  };
};

const buildLocalLintPayload = params => {
  const now = toIsoUtc();
  const normalizedBlueprint =
    params && params.blueprint && typeof params.blueprint === 'object' ? params.blueprint : {};

  return {
    valid: true,
    errors: [],
    warnings: [
      {
        level: 'warning',
        code: 'lint_local_mode',
        path: '$backend',
        message: localizeBlueprintText(
          'Lint executado em modo local porque endpoint oficial não está disponível neste ambiente.',
          'Lint executed in local mode because the official endpoint is not available in this environment.'
        ),
      },
    ],
    hints: [],
    schema_version: String(normalizedBlueprint.schema_version || '1.0.0'),
    resolved_schema_version: String(normalizedBlueprint.schema_version || '1.0.0'),
    blueprint_version: String(normalizedBlueprint.blueprint_version || '0.0.0-local-onboarding'),
    created_at: String(normalizedBlueprint.created_at || now),
    updated_at: String(normalizedBlueprint.updated_at || now),
    schema_runtime: '1.0.0',
    current_schema_version: '1.0.0',
    fingerprint_sha256: createLightweightFingerprint(normalizedBlueprint),
    normalized_blueprint: normalizedBlueprint,
  };
};

const listLocalVersionsEnvelope = () => {
  const localVersions = loadLocalBlueprintVersions();
  const safeVersions =
    localVersions.length > 0 ? localVersions : [buildDefaultLocalVersionRecord()];
  return {
    status: 'successful',
    data: {
      data: safeVersions,
    },
  };
};

const publishLocalVersionEnvelope = params => {
  const now = toIsoUtc();
  const blueprint =
    params && params.blueprint && typeof params.blueprint === 'object' ? params.blueprint : {};
  const versionRecord = {
    id: `local-${Date.now()}`,
    blueprint_version: String(blueprint.blueprint_version || '0.0.0-local-onboarding'),
    schema_version: String(blueprint.schema_version || '1.0.0'),
    resolved_schema_version: String(blueprint.schema_version || '1.0.0'),
    fingerprint_sha256: createLightweightFingerprint(blueprint),
    change_id: String((params && params.change_id) || ''),
    execution_context: String((params && params.execution_context) || ''),
    published_at_utc: now,
    lint_generated_at_utc: now,
    valid: true,
    issues: [],
    warnings: [],
    hints: [],
    normalized_blueprint: blueprint,
  };

  const currentRows = loadLocalBlueprintVersions();
  persistLocalBlueprintVersions([versionRecord, ...currentRows]);

  return {
    status: 'successful',
    data: versionRecord,
  };
};

export async function lintBlueprint(params) {
  const safeParams = sanitizeSensitivePayload(params);

  if (BLUEPRINT_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'blueprint',
      action: 'lint',
      reasonCode: 'blueprint_local_mode_enabled_dev_only',
      details: localizeBlueprintText(
        'Lint executado em modo degradado por flag explícita de desenvolvimento.',
        'Lint executed in degraded mode due to an explicit development flag.'
      ),
    });
    return Promise.resolve({
      status: 'successful',
      data: buildLocalLintPayload(safeParams),
    });
  }

  return request('/api/v1/blueprints/lint', {
    method: 'POST',
    skipErrorHandler: true,
    data: safeParams,
  });
}

export async function publishBlueprintVersion(params) {
  const safeParams = sanitizeSensitivePayload(params);

  if (BLUEPRINT_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'blueprint',
      action: 'publish',
      reasonCode: 'blueprint_local_publish_dev_only',
      details: localizeBlueprintText(
        'Publicação local aplicada apenas para desenvolvimento explícito.',
        'Local publication applied only for explicit development.'
      ),
    });
    return Promise.resolve(publishLocalVersionEnvelope(safeParams));
  }

  return request('/api/v1/blueprints/publish', {
    method: 'POST',
    skipErrorHandler: true,
    data: safeParams,
  });
}

export async function listBlueprintVersions() {
  if (BLUEPRINT_LOCAL_MODE_ENABLED) {
    recordProvisioningFallbackUsage({
      domain: 'blueprint',
      action: 'versions',
      reasonCode: 'blueprint_local_versions_dev_only',
      details: localizeBlueprintText(
        'Histórico local de versões usado somente em modo degradado de desenvolvimento.',
        'Local version history used only in degraded development mode.'
      ),
    });
    return Promise.resolve(listLocalVersionsEnvelope());
  }

  return request('/api/v1/blueprints/versions', {
    skipErrorHandler: true,
  });
}

export default lintBlueprint;
