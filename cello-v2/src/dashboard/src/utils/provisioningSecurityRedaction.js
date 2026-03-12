const SENSITIVE_KEY_TOKEN_REGEX = /(password|passphrase|private[_-]?key|ssh[_-]?key|token|secret|credential(?:_payload)?|authorization|api[_-]?key|client[_-]?secret|ca[_-]?password|ca[_-]?credential|pem)/i;
const NON_SECRET_KEY_TOKEN_REGEX = /(fingerprint|digest|hash|checksum|signature)/i;
const REFERENCE_PREFIX_REGEX = /^(vault|secret|ref|ssm|kms|env|keyring):\/\/[^\s]+$/i;
const LOCAL_REFERENCE_PREFIX_REGEX = /^local-file:[^\s]+$/i;
const PEM_BLOCK_REGEX = /-----BEGIN [^-]+-----[\s\S]*?-----END [^-]+-----/g;
const BEARER_TOKEN_REGEX = /\bBearer\s+[A-Za-z0-9._~+/=-]{8,}\b/g;
const JSON_SENSITIVE_PAIR_REGEX = /(["'])([^"']*(?:password|passphrase|private[_-]?key|ssh[_-]?key|token|secret|credential(?:_payload)?|authorization|api[_-]?key|client[_-]?secret)[^"']*)\1\s*:\s*(["'])([^"']+)\3/gi;
const ASSIGNMENT_SENSITIVE_PAIR_REGEX = /\b(password|passphrase|private[_-]?key|ssh[_-]?key|token|secret|credential(?:_payload)?|authorization|api[_-]?key|client[_-]?secret)\b\s*[:=]\s*([^\s,;]+)/gi;
const EMBEDDED_REFERENCE_REGEX = /\b(?:vault|secret|ref|ssm|kms|env|keyring):\/\/[^\s'",)]+|\blocal-file:[^\s'",)]+/gi;

const normalizeText = value => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim();
};

const normalizeKeyLabel = value =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'ref';

const stableSortObject = value => {
  if (Array.isArray(value)) {
    return value.map(item => stableSortObject(item));
  }
  if (value && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce((accumulator, key) => {
        accumulator[key] = stableSortObject(value[key]);
        return accumulator;
      }, {});
  }
  return value;
};

const stringifyForDigest = value => {
  if (typeof value === 'string') {
    return value;
  }
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  try {
    return JSON.stringify(stableSortObject(value));
  } catch (error) {
    return String(value);
  }
};

const seededHashHex = (input, seed) => {
  const text = String(input || '');
  const modulus = 4294967291;
  let hash = Number(seed) % modulus;
  if (hash < 0) {
    hash += modulus;
  }

  for (let index = 0; index < text.length; index += 1) {
    hash = (Math.imul(hash, 1664525) + text.charCodeAt(index) + 1013904223) % modulus;
    if (hash < 0) {
      hash += modulus;
    }
  }

  return hash.toString(16).padStart(8, '0');
};

const buildDigestToken = value =>
  `${seededHashHex(stringifyForDigest(value), 0x811c9dc5)}${seededHashHex(
    stringifyForDigest(value),
    0x27d4eb2f
  )}`.slice(0, 12);

const isReferenceLikeValue = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return false;
  }
  return REFERENCE_PREFIX_REGEX.test(normalized) || LOCAL_REFERENCE_PREFIX_REGEX.test(normalized);
};

const isSensitiveKey = key => {
  const normalizedKey = normalizeText(key).toLowerCase();
  if (!normalizedKey) {
    return false;
  }
  if (NON_SECRET_KEY_TOKEN_REGEX.test(normalizedKey)) {
    return false;
  }
  if (normalizedKey.endsWith('_ref')) {
    return true;
  }
  return SENSITIVE_KEY_TOKEN_REGEX.test(normalizedKey);
};

export const buildRedactedReference = (value, label = 'ref') =>
  `[REDACTED_REF:${normalizeKeyLabel(label)}:${buildDigestToken(value)}]`;

const redactReferenceLikeString = (value, label = 'ref') => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return buildRedactedReference('', label);
  }
  if (normalized.includes('://')) {
    const prefix = normalized.split('://')[0];
    return `${prefix}://${buildRedactedReference(normalized, label)}`;
  }
  if (normalized.startsWith('local-file:')) {
    return `local-file:${buildRedactedReference(normalized, label)}`;
  }
  return buildRedactedReference(normalized, label);
};

export const sanitizeSensitiveText = value => {
  const normalized = normalizeText(value);
  if (!normalized) {
    return normalized;
  }

  let sanitized = normalized;
  sanitized = sanitized.replace(PEM_BLOCK_REGEX, match => buildRedactedReference(match, 'pem'));
  sanitized = sanitized.replace(BEARER_TOKEN_REGEX, match => {
    const token = match.replace(/^Bearer\s+/i, '');
    return `Bearer ${buildRedactedReference(token, 'bearer_token')}`;
  });
  sanitized = sanitized.replace(
    JSON_SENSITIVE_PAIR_REGEX,
    (fullMatch, quote, key, valueQuote, raw) => {
      const redacted = buildRedactedReference(raw, key);
      return `${quote}${key}${quote}:${valueQuote}${redacted}${valueQuote}`;
    }
  );
  sanitized = sanitized.replace(ASSIGNMENT_SENSITIVE_PAIR_REGEX, (fullMatch, key, raw) => {
    const redacted = buildRedactedReference(raw, key);
    return `${key}=${redacted}`;
  });
  sanitized = sanitized.replace(EMBEDDED_REFERENCE_REGEX, match =>
    redactReferenceLikeString(match, 'reference')
  );

  return sanitized;
};

export function sanitizeSensitiveValueByKey(key, value) {
  const normalizedKey = normalizeText(key);

  if (value === null || value === undefined) {
    return value;
  }

  if (!isSensitiveKey(normalizedKey)) {
    if (typeof value === 'string') {
      return sanitizeSensitiveText(value);
    }
    if (Array.isArray(value)) {
      return value.map(item => sanitizeSensitiveValueByKey('', item));
    }
    if (value && typeof value === 'object') {
      return Object.keys(value).reduce((accumulator, nestedKey) => {
        accumulator[nestedKey] = sanitizeSensitiveValueByKey(nestedKey, value[nestedKey]);
        return accumulator;
      }, {});
    }
    return value;
  }

  if (typeof value === 'string') {
    if (isReferenceLikeValue(value) || normalizedKey.endsWith('_ref')) {
      return redactReferenceLikeString(value, normalizedKey || 'reference');
    }
    return buildRedactedReference(value, normalizedKey || 'secret');
  }

  if (Array.isArray(value) || (value && typeof value === 'object')) {
    return buildRedactedReference(value, normalizedKey || 'secret');
  }

  return buildRedactedReference(String(value), normalizedKey || 'secret');
}

export function sanitizeStructuredData(value) {
  if (Array.isArray(value)) {
    return value.map(item => sanitizeStructuredData(item));
  }
  if (!value || typeof value !== 'object') {
    if (typeof value === 'string') {
      return sanitizeSensitiveText(value);
    }
    return value;
  }

  return Object.keys(value).reduce((accumulator, key) => {
    accumulator[key] = sanitizeSensitiveValueByKey(key, value[key]);
    return accumulator;
  }, {});
}

export const sanitizeStringList = values =>
  (Array.isArray(values) ? values : []).map(value => sanitizeSensitiveText(value));
