const ENV_PROFILE_DEV = 'dev';
const ENV_PROFILE_HML = 'hml';
const ENV_PROFILE_PROD = 'prod';

const FALLBACK_AUDIT_STORAGE_KEY = 'cognus.provisioning.fallback.audit.v1';
const FALLBACK_AUDIT_MAX_ENTRIES = 100;

const normalizeText = value =>
  String(value || '')
    .trim()
    .toLowerCase();

const resolveProvisioningEnvProfile = () => {
  const explicitProfile = normalizeText(
    process.env.COGNUS_ENV_PROFILE || process.env.COGNUS_RUNTIME_ENV
  );
  if (
    explicitProfile === ENV_PROFILE_DEV ||
    explicitProfile === ENV_PROFILE_HML ||
    explicitProfile === ENV_PROFILE_PROD
  ) {
    return explicitProfile;
  }

  const nodeEnv = normalizeText(process.env.NODE_ENV);
  if (nodeEnv === 'production') {
    return ENV_PROFILE_HML;
  }

  return ENV_PROFILE_DEV;
};

export const PROVISIONING_ENV_PROFILE = resolveProvisioningEnvProfile();

export const IS_PROVISIONING_OPERATIONAL_ENV =
  PROVISIONING_ENV_PROFILE === ENV_PROFILE_HML || PROVISIONING_ENV_PROFILE === ENV_PROFILE_PROD;

export const PROVISIONING_DEGRADED_MODE_ALLOWED = !IS_PROVISIONING_OPERATIONAL_ENV;

export const isProvisioningLocalModePermitted = flagEnabled =>
  Boolean(flagEnabled) && PROVISIONING_DEGRADED_MODE_ALLOWED;

export const isProvisioningLocalModeBlockedByPolicy = flagEnabled =>
  Boolean(flagEnabled) && !PROVISIONING_DEGRADED_MODE_ALLOWED;

const toIsoUtc = (date = new Date()) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const getSessionStorage = () => {
  if (typeof window === 'undefined' || !window.sessionStorage) {
    return null;
  }
  return window.sessionStorage;
};

const readFallbackAuditEntries = () => {
  const storage = getSessionStorage();
  if (!storage) {
    return [];
  }

  try {
    const rawPayload = storage.getItem(FALLBACK_AUDIT_STORAGE_KEY);
    if (!rawPayload) {
      return [];
    }
    const parsed = JSON.parse(rawPayload);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
};

const writeFallbackAuditEntries = entries => {
  const storage = getSessionStorage();
  if (!storage) {
    return;
  }

  const safeEntries = Array.isArray(entries) ? entries.slice(0, FALLBACK_AUDIT_MAX_ENTRIES) : [];
  storage.setItem(FALLBACK_AUDIT_STORAGE_KEY, JSON.stringify(safeEntries));
};

export const recordProvisioningFallbackUsage = ({
  domain = '',
  action = '',
  reasonCode = '',
  details = '',
} = {}) => {
  const entry = {
    timestamp_utc: toIsoUtc(),
    env_profile: PROVISIONING_ENV_PROFILE,
    operational_env: IS_PROVISIONING_OPERATIONAL_ENV,
    domain: String(domain || '').trim(),
    action: String(action || '').trim(),
    reason_code: String(reasonCode || '').trim(),
    details: String(details || '').trim(),
  };

  const currentEntries = readFallbackAuditEntries();
  writeFallbackAuditEntries([entry, ...currentEntries]);

  return entry;
};

export const readProvisioningFallbackAuditEntries = () => readFallbackAuditEntries();
