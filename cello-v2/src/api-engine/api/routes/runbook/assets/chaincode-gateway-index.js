#!/usr/bin/env node
/*
 * Generic Hyperledger Fabric chaincode REST gateway
 */

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const express = require('express');
const morgan = require('morgan');
const { Gateway, Wallets } = require('fabric-network');

const PORT = process.env.PORT || 3000;
const CCP_PATH = process.env.CCP_PATH;
const MSP_ID = process.env.MSP_ID;
const CERT_PATH = process.env.CERT_PATH;
const KEY_PATH = process.env.KEY_PATH;
const IDENTITY_LABEL = process.env.IDENTITY_LABEL || 'default-user';
const DISCOVERY_AS_LOCALHOST =
  (process.env.DISCOVERY_AS_LOCALHOST || 'false').toLowerCase() === 'true';
const DISCOVERY_ENABLED =
  (process.env.DISCOVERY_ENABLED || 'true').toLowerCase() !== 'false';
let DEFAULT_ORG = (process.env.DEFAULT_ORG || '').trim();
const DEFAULT_ORG_FROM_ENV = DEFAULT_ORG.length > 0;
const DEFAULT_CHANNEL = (process.env.DEFAULT_CHANNEL || '').trim() || undefined;
const DEFAULT_CHAINCODE = (process.env.DEFAULT_CHAINCODE || '').trim() || undefined;
const IDENTITY_CONFIG_PATH = process.env.IDENTITY_CONFIG;
const WALLET_PATH = process.env.WALLET_PATH || path.resolve(__dirname, '.gateway-wallet');

const defaultIdentityEntry =
  CCP_PATH && MSP_ID && CERT_PATH && KEY_PATH
    ? {
        ccpPath: CCP_PATH,
        mspId: MSP_ID,
        certPath: CERT_PATH,
        keyPath: KEY_PATH,
        identityLabel: IDENTITY_LABEL,
        discoveryAsLocalhost: DISCOVERY_AS_LOCALHOST,
      }
    : null;

let identityConfig = {};
let identitySelectionContract = {
  queryParam: 'org',
  header: 'x-fabric-org',
  requireExplicitOrgWhenAmbiguous: true,
};
try {
  identityConfig = loadIdentityConfigFromDisk();
} catch (err) {
  console.warn(
    `Failed to load identity configuration during startup from ${IDENTITY_CONFIG_PATH}:`,
    err
  );
  identityConfig = {};
}

resolveDefaultOrg();

if (!defaultIdentityEntry && Object.keys(identityConfig).length === 0) {
  console.warn(
    'No identity configuration found yet. Populate identities.json or set CCP_PATH/MSP/CERT/KEY before invoking transactions.'
  );
}

function loadFile(filePath) {
  const resolved = path.resolve(filePath);
  return fs.readFileSync(resolved).toString();
}

function parseJsonFile(filePath) {
  const resolved = path.resolve(filePath);
  const contents = fs.readFileSync(resolved, 'utf8');
  return JSON.parse(contents);
}

function loadIdentityConfigFromDisk() {
  if (!IDENTITY_CONFIG_PATH) {
    return {};
  }
  try {
    const rawConfig = parseJsonFile(IDENTITY_CONFIG_PATH);
    const normalized = normalizeIdentityConfigPayload(rawConfig);
    identitySelectionContract = {
      ...identitySelectionContract,
      ...(normalized.selectionContract || {}),
    };
    const config = normalized.organizations;
    const count = Object.keys(config || {}).length;
    console.info(
      `Loaded identity configuration from ${IDENTITY_CONFIG_PATH} (${count} entr${count === 1 ? 'y' : 'ies'})`
    );
    if (normalized.defaultOrg && !DEFAULT_ORG_FROM_ENV) {
      DEFAULT_ORG = normalized.defaultOrg;
    }
    return config;
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.warn(`Identity config file ${IDENTITY_CONFIG_PATH} not found yet. Continuing.`);
      return {};
    }
    throw err;
  }
}

function normalizeIdentityConfigPayload(rawPayload) {
  if (!rawPayload || typeof rawPayload !== 'object' || Array.isArray(rawPayload)) {
    return {
      organizations: {},
      defaultOrg: undefined,
      selectionContract: {},
    };
  }

  const payload = rawPayload;
  const hasWrappedOrganizations =
    payload.organizations &&
    typeof payload.organizations === 'object' &&
    !Array.isArray(payload.organizations);

  let organizations = {};
  if (hasWrappedOrganizations) {
    organizations = payload.organizations;
  } else {
    const reservedKeys = new Set(['default', 'defaultOrg', 'selectionContract', 'organizations']);
    organizations = Object.fromEntries(
      Object.entries(payload).filter(([key, value]) => {
        const normalizedKey = String(key || '').trim();
        return (
          !reservedKeys.has(normalizedKey) &&
          value &&
          typeof value === 'object' &&
          !Array.isArray(value)
        );
      })
    );
  }

  const normalizedOrganizations = Object.fromEntries(
    Object.entries(organizations).map(([key, value]) => [
      String(key || '').trim(),
      value,
    ])
  );

  const payloadDefaultOrg = extractString(payload.defaultOrg);
  const fallbackDefault = payload.default && typeof payload.default === 'object'
    ? Object.keys(normalizedOrganizations).find((key) => key.toLowerCase() === 'default')
    : undefined;

  return {
    organizations: normalizedOrganizations,
    defaultOrg: payloadDefaultOrg || fallbackDefault,
    selectionContract:
      payload.selectionContract && typeof payload.selectionContract === 'object'
        ? payload.selectionContract
        : {},
  };
}

function resolveDefaultOrg() {
  if (DEFAULT_ORG_FROM_ENV) {
    return;
  }
  const identityKeys = Object.keys(identityConfig || {});
  if (identityKeys.length === 1) {
    DEFAULT_ORG = identityKeys[0];
    console.info(
      `DEFAULT_ORG not provided; using '${DEFAULT_ORG}' from identities.json`
    );
    return;
  }
  if (identityKeys.length > 1) {
    if (!DEFAULT_ORG || !identityKeys.includes(DEFAULT_ORG)) {
      DEFAULT_ORG = undefined;
      console.warn(
        'DEFAULT_ORG não resolvida: múltiplas organizações válidas detectadas; o request deve informar query org ou header x-fabric-org.'
      );
    }
    return;
  }
  if (!DEFAULT_ORG || DEFAULT_ORG.trim().length === 0) {
    DEFAULT_ORG = undefined;
    console.warn('Nenhuma organização padrão definida. O fluxo requer especificação explícita de organização ou identities.json válido.');
  }
}

const ccpCache = new Map();
let wallet;
const registeredIdentities = new Map();
let identityReloadTimer = null;

function ensureIdentityConfigLoaded() {
  if (!IDENTITY_CONFIG_PATH) {
    return;
  }
  try {
    const fresh = loadIdentityConfigFromDisk();
    if (fresh && Object.keys(fresh).length > 0) {
      identityConfig = fresh;
      resolveDefaultOrg();
    }
  } catch (err) {
    console.warn('Lazy reload of identity configuration failed:', err);
  }
}

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function sendError(res, status, message) {
  res.status(status).json({ status, error: message });
}

function sendSuccess(res, payload) {
  if (payload === undefined) {
    res.status(200).json(null);
    return;
  }
  res.status(200).json(payload);
}

function respondWithError(res, error, defaultMessage, logLabel) {
  if (error instanceof HttpError && error.status) {
    if (error.status >= 500) {
      console.error(`${logLabel}:`, error);
    } else {
      console.warn(`${logLabel}:`, error.message);
    }
    sendError(res, error.status, error.message);
    return;
  }
  console.error(`${logLabel}:`, error);
  const message = error && error.message ? error.message : defaultMessage;
  sendError(res, 500, message || defaultMessage || 'Internal server error');
}

function parseJsonResult(buffer) {
  if (!buffer || buffer.length === 0) {
    return null;
  }
  const text = buffer.toString('utf8');
  try {
    return JSON.parse(text);
  } catch (err) {
    const parseError = new HttpError(500, 'Chaincode response is not valid JSON');
    parseError.cause = err;
    parseError.rawResponse = text;
    throw parseError;
  }
}

function extractString(value) {
  if (Array.isArray(value) && value.length > 0) {
    return extractString(value[0]);
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  }
  return undefined;
}

function stripTransientFields(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { cleaned: payload, transient: null };
  }

  const cleaned = {};
  const transient = {};
  let transientKeys = 0;

  for (const [key, value] of Object.entries(payload)) {
    if (typeof key === 'string' && key.startsWith('~')) {
      const trimmedKey = key.slice(1);
      if (trimmedKey.length > 0) {
        transient[trimmedKey] = value;
        transientKeys += 1;
      }
    } else {
      cleaned[key] = value;
    }
  }

  return { cleaned, transient: transientKeys > 0 ? transient : null };
}

function coerceArg(value) {
  if (typeof value === 'string') {
    return value;
  }
  if (value === undefined) {
    return '';
  }
  if (value === null) {
    return 'null';
  }
  return JSON.stringify(value);
}

function buildArgsFromPayload(payload) {
  if (payload === undefined || payload === null) {
    return [JSON.stringify({})];
  }

  if (Array.isArray(payload)) {
    return payload.map(coerceArg);
  }

  if (typeof payload !== 'object') {
    return [coerceArg(payload)];
  }

  const data = { ...payload };

  if (Array.isArray(data.args)) {
    const argsArray = data.args.map(coerceArg);
    delete data.args;
    delete data.arg;
    return argsArray;
  }

  if (data.arg !== undefined) {
    const argValue = data.arg;
    delete data.arg;
    delete data.args;
    return [coerceArg(argValue)];
  }

  delete data.args;
  delete data.arg;
  delete data.org;
  return [JSON.stringify(data)];
}

function toLowerCaseSet(value) {
  const result = new Set();
  if (!value) {
    return result;
  }

  const push = (item) => {
    if (typeof item === 'string') {
      const trimmed = item.trim();
      if (trimmed.length > 0) {
        result.add(trimmed.toLowerCase());
      }
    }
  };

  if (Array.isArray(value)) {
    value.forEach((item) => {
      if (typeof item === 'string') {
        push(item);
      } else if (item && typeof item === 'object') {
        Object.keys(item).forEach(push);
      }
    });
    return result;
  }

  if (typeof value === 'string') {
    push(value);
    return result;
  }

  if (value && typeof value === 'object') {
    Object.keys(value).forEach(push);
  }

  return result;
}

function buildArgsFromQuery(query) {
  if (!query) {
    return [];
  }

  const requestEncoded = extractString(query['@request']);
  if (requestEncoded) {
    try {
      return [Buffer.from(requestEncoded, 'base64').toString('utf8')];
    } catch (err) {
      throw new HttpError(
        400,
        'the @request query parameter must be a base64-encoded JSON string'
      );
    }
  }

  const request = extractString(query.request);
  if (request !== undefined) {
    return [request];
  }

  const arg = extractString(query.arg);
  if (arg !== undefined) {
    return [arg];
  }

  return [];
}

function buildTransientMap(transient) {
  if (!transient || Object.keys(transient).length === 0) {
    return null;
  }
  return {
    '@request': Buffer.from(JSON.stringify(transient), 'utf8'),
  };
}

function parseEndorsers(query) {
  if (!query) {
    return null;
  }

  let encoded = query['@endorsers'];
  if (Array.isArray(encoded)) {
    encoded = encoded[0];
  }
  if (!encoded) {
    return null;
  }

  try {
    const decoded = Buffer.from(String(encoded), 'base64').toString('utf8');
    const parsed = JSON.parse(decoded);
    if (
      !Array.isArray(parsed) ||
      parsed.some((item) => typeof item !== 'string' || item.trim().length === 0)
    ) {
      throw new Error('invalid');
    }
    return parsed.map((item) => item.trim());
  } catch (_err) {
    throw new HttpError(
      400,
      'the @endorsers query parameter must be a base64-encoded JSON array of strings'
    );
  }
}

function resolveChannelAndChaincode(req, options = {}) {
  const {
    channelParam = 'channel',
    chaincodeParam = 'chaincode',
    allowDefault = false,
    channel: fixedChannel,
    chaincode: fixedChaincode,
  } = options;

  let channelName =
    fixedChannel ||
    (channelParam ? extractString(req.params?.[channelParam]) : undefined);
  let chaincodeName =
    fixedChaincode ||
    (chaincodeParam ? extractString(req.params?.[chaincodeParam]) : undefined);

  if (!channelName && allowDefault) {
    channelName = DEFAULT_CHANNEL;
  }
  if (!chaincodeName && allowDefault) {
    chaincodeName = DEFAULT_CHAINCODE;
  }

  if (!channelName) {
    throw new HttpError(
      400,
      allowDefault
        ? 'Channel not provided and DEFAULT_CHANNEL is not configured'
        : 'Channel not provided'
    );
  }

  if (!chaincodeName) {
    throw new HttpError(
      400,
      allowDefault
        ? 'Chaincode not provided and DEFAULT_CHAINCODE is not configured'
        : 'Chaincode not provided'
    );
  }

  return { channelName, chaincodeName };
}

function extractChannelsFromEntry(entry) {
  if (!entry || typeof entry !== 'object') {
    return new Set();
  }

  const channels = new Set();
  const merge = (source) => {
    const subset = toLowerCaseSet(source);
    subset.forEach((val) => channels.add(val));
  };

  merge(entry.channels);
  merge(entry.channel);
  merge(entry.networkChannels);
  merge(entry.defaultChannels);

  if (entry.chaincodes && typeof entry.chaincodes === 'object') {
    Object.keys(entry.chaincodes).forEach((key) => {
      const trimmed = key.trim();
      if (trimmed.length > 0) {
        channels.add(trimmed.toLowerCase());
      }
    });
  }

  if (channels.size === 0 && entry.ccpPath) {
    try {
      const ccp = loadCcp(entry.ccpPath);
      if (ccp && ccp.channels && typeof ccp.channels === 'object') {
        Object.keys(ccp.channels).forEach((key) => {
          const trimmed = key.trim();
          if (trimmed.length > 0) {
            channels.add(trimmed.toLowerCase());
          }
        });
      }
    } catch (err) {
      console.warn(`Failed to read channels from CCP ${entry.ccpPath}:`, err.message);
    }
  }

  return channels;
}

function entryMatchesChannel(entry, channelName) {
  if (!channelName) {
    return false;
  }
  const target = channelName.toLowerCase();
  const baseChannels = extractChannelsFromEntry(entry);
  if (baseChannels.has(target)) {
    return true;
  }

  if (entry && typeof entry === 'object' && entry.users && typeof entry.users === 'object') {
    for (const userEntry of Object.values(entry.users)) {
      const userChannels = extractChannelsFromEntry(userEntry);
      if (userChannels.has(target)) {
        return true;
      }
    }
  }

  return false;
}

function resolveUserLabel(req) {
  const headerUser =
    extractString(req.headers?.user) ||
    extractString(req.headers?.['x-fabric-user']);
  if (headerUser) {
    return headerUser;
  }
  return extractString(req.query?.user);
}

function inferOrgFromChannel(channelName) {
  if (!channelName) {
    return undefined;
  }
  const normalized = channelName.toLowerCase();

  for (const [orgName, entry] of Object.entries(identityConfig)) {
    if (entryMatchesChannel(entry, normalized)) {
      return orgName;
    }
  }

  if (defaultIdentityEntry && entryMatchesChannel(defaultIdentityEntry, normalized)) {
    return DEFAULT_ORG;
  }

  return undefined;
}

function normalizeIdentityEntry(entry, userLabel) {
  if (!entry || typeof entry !== 'object') {
    return entry;
  }

  const base = { ...entry };
  const userKey = userLabel ? userLabel.trim() : undefined;

  if (base.users) {
    delete base.users;
  }

  if (userKey && entry.users && typeof entry.users === 'object') {
    if (entry.users[userKey]) {
      return { ...base, ...entry.users[userKey] };
    }
    const lowerKey = userKey.toLowerCase();
    if (entry.users[lowerKey]) {
      return { ...base, ...entry.users[lowerKey] };
    }
  }

  return base;
}

function resolveIdentityEntry(orgKey, userLabel, channelName) {
  ensureIdentityConfigLoaded();
  const explicitOrg = extractString(orgKey);
  let normalizedOrg = explicitOrg || inferOrgFromChannel(channelName);
  const identityKeys = Object.keys(identityConfig || {});

  if (!normalizedOrg && identityKeys.length === 1) {
    normalizedOrg = identityKeys[0];
  }

  if (!normalizedOrg) {
    normalizedOrg = DEFAULT_ORG;
  }

  const normalizedUser = userLabel ? userLabel.trim() : undefined;

  if (normalizedOrg && identityConfig[normalizedOrg]) {
    return {
      org: normalizedOrg,
      entry: normalizeIdentityEntry(identityConfig[normalizedOrg], normalizedUser),
    };
  }

  if (normalizedUser && identityConfig[normalizedUser]) {
    return {
      org: normalizedUser,
      entry: normalizeIdentityEntry(identityConfig[normalizedUser], normalizedUser),
    };
  }

  // If identities.json does not contain a matching org but a default identity
  // was provided via environment (CCP_PATH/MSP_ID/CERT_PATH/KEY_PATH), use it
  // as a fallback so callers don't need to explicitly provide org when the
  // container already has the correct identity mounted/configured.
  if (defaultIdentityEntry) {
    const fallbackOrg = normalizedOrg || DEFAULT_ORG || undefined;
    if (!fallbackOrg && identityKeys.length > 1) {
      throw new Error(
        `Organização ausente/ambígua. Informe explicitamente 'org' (query) ou '${identitySelectionContract.header || 'x-fabric-org'}' (header). Orgs válidas: ${identityKeys.sort().join(', ')}`
      );
    }
    return {
      org: fallbackOrg,
      entry: normalizeIdentityEntry(defaultIdentityEntry, normalizedUser),
    };
  }

  ensureIdentityConfigLoaded();
  const validOrgs = Object.keys(identityConfig || {}).sort();
  throw new Error(
    `Nenhuma configuração de identidade encontrada para org '${normalizedOrg || '(não especificada)'}'. Informe explicitamente 'org' (query) ou '${identitySelectionContract.header || 'x-fabric-org'}' (header). Orgs válidas: ${validOrgs.join(', ') || '(nenhuma)'}.`
  );
}

function toBooleanOrDefault(value, fallback) {
  if (value === undefined || value === null) {
    return fallback;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized.length === 0) {
      return fallback;
    }
    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
      return false;
    }
  }
  return fallback;
}

function shouldFallbackToStaticDiscovery(error) {
  const message = String(error && error.message ? error.message : '').toLowerCase();
  if (!message) {
    return false;
  }
  return (
    message.includes('discoveryservice') ||
    message.includes('no discovery results') ||
    message.includes('error: access denied') ||
    message.includes('failed to connect before the deadline on discoverer') ||
    message.includes('failed to connect before the deadline on endorser') ||
    message.includes('failed to connect before the deadline on committer') ||
    message.includes('waitforready - failed to connect to remote grpc server')
  );
}

function resolveChannelNameFromCcp(ccp, requestedChannelName) {
  const normalizedRequested = extractString(requestedChannelName);
  if (!normalizedRequested) {
    throw new HttpError(400, 'Channel not provided');
  }

  const channelsPayload =
    ccp && ccp.channels && typeof ccp.channels === 'object' && !Array.isArray(ccp.channels)
      ? ccp.channels
      : {};
  const availableChannels = Object.keys(channelsPayload)
    .map((channel) => String(channel || '').trim())
    .filter((channel) => channel.length > 0);

  if (availableChannels.includes(normalizedRequested)) {
    return normalizedRequested;
  }

  const caseInsensitiveMatch = availableChannels.find(
    (channel) => channel.toLowerCase() === normalizedRequested.toLowerCase()
  );
  if (caseInsensitiveMatch) {
    return caseInsensitiveMatch;
  }

  if (availableChannels.length === 1) {
    console.warn(
      `Requested channel '${normalizedRequested}' not found in connection profile. Falling back to '${availableChannels[0]}'.`
    );
    return availableChannels[0];
  }

  throw new HttpError(
    400,
    `Channel '${normalizedRequested}' not configured in connection profile. Available channels: ${availableChannels.sort().join(', ') || '(none)'}.`
  );
}

function loadCcp(ccpPath) {
  const resolved = path.resolve(ccpPath);
  if (!ccpCache.has(resolved)) {
    ccpCache.set(resolved, parseJsonFile(resolved));
  }
  return ccpCache.get(resolved);
}

function setupIdentityConfigWatcher() {
  if (!IDENTITY_CONFIG_PATH) {
    return;
  }
  const resolved = path.resolve(IDENTITY_CONFIG_PATH);
  const directory = path.dirname(resolved);
  try {
    fs.mkdirSync(directory, { recursive: true });
    fs.watch(directory, (eventType, filename) => {
      if (filename && path.resolve(directory, filename) !== resolved) {
        return;
      }
      scheduleIdentityReload(`file ${eventType || 'change'}`);
    });
    console.info(`Watching identity config directory ${directory}`);
  } catch (err) {
    console.warn('Unable to watch identity config for changes:', err);
  }
}

function scheduleIdentityReload(reason) {
  if (identityReloadTimer) {
    return;
  }
  identityReloadTimer = setTimeout(() => {
    identityReloadTimer = null;
    refreshIdentityConfig(reason).catch((err) => {
      console.error('Failed to refresh identity configuration:', err);
    });
  }, 500);
}

async function refreshIdentityConfig(reason) {
  try {
    identityConfig = loadIdentityConfigFromDisk();
    resolveDefaultOrg();
    ccpCache.clear();
    await resetWallet();
    console.info(`Identity configuration reloaded (${reason}).`);
  } catch (err) {
    console.warn(`Identity config reload failed (${reason}):`, err);
    identityReloadTimer = setTimeout(() => {
      identityReloadTimer = null;
      refreshIdentityConfig('retry after failure').catch((innerErr) => {
        console.error('Repeated failure reloading identities:', innerErr);
      });
    }, 1500);
  }
}

async function resetWallet() {
  registeredIdentities.clear();
  wallet = null;
  await fs.promises.rm(WALLET_PATH, { recursive: true, force: true });
  await initWallet(true);
}

async function ensureWalletIdentity(entry, orgKey, preferredLabel) {
  if (!wallet) {
    await fs.promises.mkdir(WALLET_PATH, { recursive: true });
    wallet = await Wallets.newFileSystemWallet(WALLET_PATH);
  }

  const normalizedOrg = orgKey ? orgKey.trim() : undefined;
  const normalizedUser = preferredLabel ? preferredLabel.trim() : undefined;
  const label =
    entry.identityLabel ||
    normalizedUser ||
    normalizedOrg ||
    IDENTITY_LABEL ||
    'default';

  let certPath = entry.certPath || CERT_PATH;
  let keyPath = entry.keyPath || KEY_PATH;
  let mspId = entry.mspId || MSP_ID;
  // If cert/key not provided but a cryptoPath is available (e.g. mounted /var/lib/cognus/msp/<org>),
  // try to auto-detect signcerts and keystore files there.
  if ((!certPath || !keyPath) && entry.cryptoPath) {
    try {
      const resolvedCrypto = path.resolve(entry.cryptoPath);
      const signcertsDir = path.join(resolvedCrypto, 'signcerts');
      const keystoreDir = path.join(resolvedCrypto, 'keystore');
      // detect cert
      if (!certPath) {
        try {
          const signFiles = fs.readdirSync(signcertsDir).filter((f) => f.toLowerCase().endsWith('.pem'));
          if (signFiles && signFiles.length > 0) {
            certPath = path.join(signcertsDir, signFiles[0]);
          }
        } catch (e) {
          // ignore
        }
      }
      // detect key
      if (!keyPath) {
        try {
          const keyFiles = fs.readdirSync(keystoreDir).filter((f) => f && f.length > 0);
          if (keyFiles && keyFiles.length > 0) {
            keyPath = path.join(keystoreDir, keyFiles[0]);
          }
        } catch (e) {
          // ignore
        }
      }
      // infer mspId from directory name if missing
      if (!mspId) {
        try {
          const dirName = path.basename(resolvedCrypto);
          if (dirName && dirName.length > 0) {
            mspId = dirName.endsWith('MSP') ? dirName : `${dirName}MSP`;
          }
        } catch (e) {}
      }
    } catch (err) {
      // non-fatal
    }
  }
  if (!certPath || !keyPath || !mspId) {
    throw new Error('Certificate, key or MSP ID missing for identity');
  }

  const registered = registeredIdentities.get(label);
  if (
    registered &&
    registered.certPath === certPath &&
    registered.keyPath === keyPath &&
    registered.mspId === mspId
  ) {
    return label;
  }

  if (registered) {
    try {
      await wallet.remove(label);
    } catch (err) {
      console.warn(`Failed to purge stale identity '${label}' from wallet:`, err);
    }
  }

  const cert = loadFile(certPath);
  const key = loadFile(keyPath);
  const identity = {
    credentials: {
      certificate: cert,
      privateKey: key,
    },
    mspId,
    type: 'X.509',
  };
  await wallet.put(label, identity);
  registeredIdentities.set(label, { certPath, keyPath, mspId });
  return label;
}

async function initWallet(force = false) {
  if (wallet && !force) {
    return;
  }
  await fs.promises.mkdir(WALLET_PATH, { recursive: true });
  wallet = await Wallets.newFileSystemWallet(WALLET_PATH);
  if (defaultIdentityEntry) {
    try {
      await ensureWalletIdentity(defaultIdentityEntry, DEFAULT_ORG);
    } catch (err) {
      if (err.code === 'ENOENT') {
        console.warn(
          'Default identity files not found yet. Waiting for identities.json update...'
        );
      } else {
        throw err;
      }
    }
  }
}

async function withContract(
  channelName,
  chaincodeName,
  orgKey,
  userLabel,
  callback
) {
  await initWallet();
  const { org: resolvedOrg, entry } = resolveIdentityEntry(orgKey, userLabel, channelName);
  if (!wallet) {
    await initWallet(true);
  }
  const ccpPath = entry.ccpPath || CCP_PATH;
  if (!ccpPath) {
    throw new Error('Connection profile (ccpPath) not configured');
  }
  const ccp = loadCcp(ccpPath);
  const label = await ensureWalletIdentity(entry, resolvedOrg, userLabel);
  const discoveryAsLocalhost =
    entry.discoveryAsLocalhost !== undefined
      ? entry.discoveryAsLocalhost
      : DISCOVERY_AS_LOCALHOST;
  const discoveryEnabled = toBooleanOrDefault(entry.discoveryEnabled, DISCOVERY_ENABLED);

  const gateway = new Gateway();

  const executeWithDiscovery = async (enabled) => {
    await gateway.connect(ccp, {
      wallet,
      identity: label,
      discovery: { enabled, asLocalhost: discoveryAsLocalhost },
    });
    const resolvedChannelName = resolveChannelNameFromCcp(ccp, channelName);
    const network = await gateway.getNetwork(resolvedChannelName);
    const contract = network.getContract(chaincodeName);
    return await callback(contract);
  };

  try {
    try {
      return await executeWithDiscovery(discoveryEnabled);
    } catch (err) {
      if (discoveryEnabled && shouldFallbackToStaticDiscovery(err)) {
        console.warn(
          `Discovery failed for channel '${channelName}' org '${resolvedOrg || 'default'}'. Retrying with discovery disabled.`
        );
        gateway.disconnect();
        return await executeWithDiscovery(false);
      }
      throw err;
    }
  } finally {
    gateway.disconnect();
  }
}

function resolveOrg(req, channelName) {
  // Prioridade: query > header > body > channel > identities.json
  const queryOrg = extractString(req.query?.org);
  if (queryOrg && identityConfig[queryOrg]) {
    return queryOrg;
  }
  const headerOrg = extractString(req.headers?.['x-fabric-org']);
  if (headerOrg && identityConfig[headerOrg]) {
    return headerOrg;
  }
  if (
    req.body &&
    typeof req.body === 'object' &&
    !Array.isArray(req.body) &&
    req.body.org && identityConfig[req.body.org]
  ) {
    return extractString(req.body.org);
  }
  const inferred = inferOrgFromChannel(channelName);
  if (inferred && identityConfig[inferred]) {
    return inferred;
  }
  if (DEFAULT_ORG && identityConfig[DEFAULT_ORG]) {
    return DEFAULT_ORG;
  }
  // Se só existe uma org, retorna ela
  const identityKeys = Object.keys(identityConfig || {});
  if (identityKeys.length === 1) {
    return identityKeys[0];
  }
  // Não retorna default, força erro se não encontrar
  return undefined;
}

async function handleInvoke(req, res, options = {}) {
  try {
    const { channelName, chaincodeName } = resolveChannelAndChaincode(req, options);
    const tx = extractString(req.params?.tx);
    if (!tx) {
      throw new HttpError(400, 'Transaction name not provided');
    }

    const orgKey = resolveOrg(req, channelName);
    if (!orgKey) {
      const validOrgs = Object.keys(identityConfig || {}).sort();
      throw new HttpError(
        400,
        `Organização ausente/ambígua. Informe query 'org' ou header 'x-fabric-org'. Orgs válidas: ${validOrgs.join(', ') || '(nenhuma)'}.`
      );
    }

    const userLabel = resolveUserLabel(req);

    if (
      req.body &&
      typeof req.body === 'object' &&
      !Array.isArray(req.body) &&
      'org' in req.body
    ) {
      delete req.body.org;
    }

    const { cleaned, transient } = stripTransientFields(req.body);
    const args = buildArgsFromPayload(cleaned);
    const endorsers = parseEndorsers(req.query);

    const result = await withContract(
      channelName,
      chaincodeName,
      orgKey,
      userLabel,
      async (contract) => {
        const transaction = contract.createTransaction(tx);

        const transientMap = buildTransientMap(transient);
        if (transientMap) {
          transaction.setTransient(transientMap);
        }

        if (endorsers && endorsers.length > 0) {
          transaction.setEndorsingOrganizations(...endorsers);
        }

        const buffer = await transaction.submit(...args);
        return parseJsonResult(buffer);
      }
    );

    sendSuccess(res, result);
  } catch (error) {
    respondWithError(res, error, 'Invoke failed', 'Invoke error');
  }
}

async function handleQuery(req, res, options = {}) {
  try {
    const { channelName, chaincodeName } = resolveChannelAndChaincode(req, options);
    const tx = extractString(req.params?.tx);
    if (!tx) {
      throw new HttpError(400, 'Transaction name not provided');
    }

    const orgKey = resolveOrg(req, channelName);
    if (!orgKey) {
      const validOrgs = Object.keys(identityConfig || {}).sort();
      throw new HttpError(
        400,
        `Organização ausente/ambígua. Informe query 'org' ou header 'x-fabric-org'. Orgs válidas: ${validOrgs.join(', ') || '(nenhuma)'}.`
      );
    }
    const userLabel = resolveUserLabel(req);

    if (
      req.method !== 'GET' &&
      req.body &&
      typeof req.body === 'object' &&
      !Array.isArray(req.body) &&
      'org' in req.body
    ) {
      delete req.body.org;
    }

    const args =
      req.method === 'GET'
        ? buildArgsFromQuery(req.query)
        : buildArgsFromPayload(stripTransientFields(req.body).cleaned);

    const result = await withContract(
      channelName,
      chaincodeName,
      orgKey,
      userLabel,
      async (contract) => {
        const buffer = await contract.evaluateTransaction(tx, ...args);
        return parseJsonResult(buffer);
      }
    );

    sendSuccess(res, result);
  } catch (error) {
    respondWithError(res, error, 'Query failed', 'Query error');
  }
}

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(morgan('combined'));

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', ts: new Date().toISOString() });
});

const invokeWithParams = (options) => (req, res) => handleInvoke(req, res, options);
const queryWithParams = (options) => (req, res) => handleQuery(req, res, options);

// CCAPI-compatible gateway routes
app.post(
  '/api/gateway/:channel/:chaincode/invoke/:tx',
  invokeWithParams()
);
app.put(
  '/api/gateway/:channel/:chaincode/invoke/:tx',
  invokeWithParams()
);
app.delete(
  '/api/gateway/:channel/:chaincode/invoke/:tx',
  invokeWithParams()
);

app.post(
  '/api/:channel/:chaincode/invoke/:tx',
  invokeWithParams()
);
app.put(
  '/api/:channel/:chaincode/invoke/:tx',
  invokeWithParams()
);
app.delete(
  '/api/:channel/:chaincode/invoke/:tx',
  invokeWithParams()
);

app.post('/api/gateway/invoke/:tx', invokeWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));
app.put('/api/gateway/invoke/:tx', invokeWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));
app.delete('/api/gateway/invoke/:tx', invokeWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));

app.post('/api/invoke/:tx', invokeWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));
app.put('/api/invoke/:tx', invokeWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));
app.delete('/api/invoke/:tx', invokeWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));

app.post(
  '/api/gateway/:channel/:chaincode/query/:tx',
  queryWithParams()
);
app.get(
  '/api/gateway/:channel/:chaincode/query/:tx',
  queryWithParams()
);

app.post(
  '/api/:channel/:chaincode/query/:tx',
  queryWithParams()
);
app.get(
  '/api/:channel/:chaincode/query/:tx',
  queryWithParams()
);

app.post('/api/gateway/query/:tx', queryWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));
app.get('/api/gateway/query/:tx', queryWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));

app.post('/api/query/:tx', queryWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));
app.get('/api/query/:tx', queryWithParams({ allowDefault: true, channelParam: null, chaincodeParam: null }));

(async () => {
  try {
    const needsInitialReload =
      Boolean(IDENTITY_CONFIG_PATH) &&
      (!identityConfig || Object.keys(identityConfig).length === 0);

    setupIdentityConfigWatcher();
    if (needsInitialReload) {
      scheduleIdentityReload('initial bootstrap');
    }
    await initWallet(true);
    app.listen(PORT, () => {
      console.log(`Chaincode gateway listening on port ${PORT}`);
    });
  } catch (err) {
    console.error('Failed to start gateway:', err);
    process.exit(1);
  }
})();
