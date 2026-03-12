const ORGANIZATION_VM_ACCESS_SESSION_KEYS = [
  'cognus.overview.organization.vm-access.v1',
  'cognus.overview.organization.vm-access.v2',
];
const AUTH_TOKEN_STORAGE_KEY = 'cello-token';

export const VM_ACCESS_SESSION_REVOKED_EVENT = 'cognus:vm-access-session-revoked';

const getLocalStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.localStorage || null;
  } catch (error) {
    return null;
  }
};

const getSessionStorage = () => {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.sessionStorage || null;
  } catch (error) {
    return null;
  }
};

const clearStorageKeys = (storage, keys) => {
  if (!storage) {
    return;
  }

  (Array.isArray(keys) ? keys : []).forEach(storageKey => {
    try {
      storage.removeItem(storageKey);
    } catch (error) {
      // best-effort cleanup only
    }
  });
};

export const revokeAllVmAccessSessions = (options = {}) => {
  const safeOptions = options && typeof options === 'object' ? options : {};
  const clearToken = safeOptions.clearToken !== false;
  const reason = String(safeOptions.reason || 'session_end').trim() || 'session_end';

  clearStorageKeys(getSessionStorage(), ORGANIZATION_VM_ACCESS_SESSION_KEYS);

  if (clearToken) {
    clearStorageKeys(getLocalStorage(), [AUTH_TOKEN_STORAGE_KEY]);
  }

  if (typeof window !== 'undefined' && typeof window.dispatchEvent === 'function') {
    try {
      window.dispatchEvent(
        new CustomEvent(VM_ACCESS_SESSION_REVOKED_EVENT, {
          detail: {
            reason,
          },
        })
      );
    } catch (error) {
      // no-op: UI notification is opportunistic
    }
  }
};