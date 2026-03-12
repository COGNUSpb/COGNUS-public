import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import ReactDOM from 'react-dom';
import {
  CloseOutlined,
  FullscreenExitOutlined,
  FullscreenOutlined,
  MinusOutlined,
  BorderOutlined,
} from '@ant-design/icons';
import { pickCognusText, resolveCognusLocale } from '../cognusI18n';
import styles from './OperationalWindowManager.less';

const WINDOW_STACK_BASE_Z_INDEX = 1410;
const OperationalWindowContext = createContext(null);
const localizeOperationalWindowText = (ptBR, enUS, localeCandidate) =>
  pickCognusText(ptBR, enUS, localeCandidate || resolveCognusLocale());

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const normalizeWindowOffset = (value, viewportSize) => {
  const parsed = Number(value);
  const safeValue = Number.isFinite(parsed) ? parsed : 0;
  const safeViewport = Number.isFinite(Number(viewportSize)) ? Number(viewportSize) : 1600;
  const boundary = Math.max(120, Math.floor(safeViewport * 0.42));
  return clamp(safeValue, boundary * -1, boundary);
};

const shallowEqualWindowMeta = (currentMeta, nextMeta) => {
  const current = currentMeta || {};
  const next = nextMeta || {};
  const currentKeys = Object.keys(current);
  const nextKeys = Object.keys(next);

  if (currentKeys.length !== nextKeys.length) {
    return false;
  }

  return nextKeys.every(key => current[key] === next[key]);
};

const moveWindowToTop = (currentOrder, windowId) => {
  const nextOrder = [...currentOrder.filter(id => id !== windowId), windowId];

  if (
    nextOrder.length === currentOrder.length &&
    nextOrder.every((value, index) => value === currentOrder[index])
  ) {
    return currentOrder;
  }

  return nextOrder;
};

const buildWindowMetrics = ({
  stackIndex,
  openCount,
  maximized,
  preferredWidth,
  preferredHeight,
  offsetX = 0,
  offsetY = 0,
}) => {
  if (maximized) {
    return {
      width: 'calc(100vw - 32px)',
      height: 'calc(100vh - 52px)',
      transform: 'translate(-50%, -50%)',
    };
  }

  const overlayDepth = Math.max(0, openCount - stackIndex - 1);
  const shrinkPx = clamp(overlayDepth * 54, 0, 160);
  const verticalOffsetPx = overlayDepth * 18;
  const horizontalOffsetPx = overlayDepth * 22;
  const width = `min(calc(${preferredWidth} - ${shrinkPx}px), calc(100vw - 44px))`;
  const height = `min(calc(${preferredHeight} - ${Math.round(shrinkPx * 0.55)}px), calc(100vh - 82px))`;

  return {
    width,
    height,
    transform: `translate(calc(-50% + ${horizontalOffsetPx + offsetX}px), calc(-50% + ${verticalOffsetPx + offsetY}px))`,
  };
};

export const OperationalWindowManagerProvider = ({ children }) => {
  const [windowsById, setWindowsById] = useState({});
  const [openOrder, setOpenOrder] = useState([]);
  const closeHandlersRef = useRef({});

  const registerWindow = useCallback((windowId, nextMeta, onClose) => {
    if (typeof onClose === 'function') {
      closeHandlersRef.current[windowId] = onClose;
    } else if (windowId in closeHandlersRef.current) {
      delete closeHandlersRef.current[windowId];
    }

    setWindowsById(current => {
      const mergedMeta = {
        ...(current[windowId] || {}),
        ...nextMeta,
      };

      if (shallowEqualWindowMeta(current[windowId], mergedMeta)) {
        return current;
      }

      return {
        ...current,
        [windowId]: mergedMeta,
      };
    });
  }, []);

  const unregisterWindow = useCallback(windowId => {
    setWindowsById(current => {
      if (!current[windowId]) {
        return current;
      }
      const next = { ...current };
      delete next[windowId];
      return next;
    });
    delete closeHandlersRef.current[windowId];
    setOpenOrder(current => current.filter(id => id !== windowId));
  }, []);

  const openWindow = useCallback(windowId => {
    setWindowsById(current => {
      const currentWindow = current[windowId] || {};
      if (currentWindow.minimized === false) {
        return current;
      }

      return {
        ...current,
        [windowId]: {
          ...currentWindow,
          minimized: false,
        },
      };
    });
    setOpenOrder(current => moveWindowToTop(current, windowId));
  }, []);

  const minimizeWindow = useCallback(windowId => {
    setWindowsById(current => {
      const currentWindow = current[windowId] || {};
      if (currentWindow.minimized) {
        return current;
      }

      return {
        ...current,
        [windowId]: {
          ...currentWindow,
          minimized: true,
        },
      };
    });
    setOpenOrder(current => current.filter(id => id !== windowId));
  }, []);

  const restoreWindow = useCallback(windowId => {
    setWindowsById(current => {
      const currentWindow = current[windowId] || {};
      if (currentWindow.minimized === false) {
        return current;
      }

      return {
        ...current,
        [windowId]: {
          ...currentWindow,
          minimized: false,
        },
      };
    });
    setOpenOrder(current => moveWindowToTop(current, windowId));
  }, []);

  const toggleMaximizeWindow = useCallback(windowId => {
    setWindowsById(current => {
      const currentWindow = current[windowId] || {};
      return {
        ...current,
        [windowId]: {
          ...currentWindow,
          maximized: !currentWindow.maximized,
        },
      };
    });
    setOpenOrder(current => moveWindowToTop(current, windowId));
  }, []);

  const bringToFront = useCallback(windowId => {
    setOpenOrder(current => {
      if (!current.includes(windowId)) {
        return current;
      }
      return moveWindowToTop(current, windowId);
    });
  }, []);

  const updateWindowPosition = useCallback((windowId, nextPosition) => {
    setWindowsById(current => {
      const currentWindow = current[windowId] || {};
      const viewportWidth = typeof window !== 'undefined' ? window.innerWidth : 1600;
      const viewportHeight = typeof window !== 'undefined' ? window.innerHeight : 1000;
      const offsetX = normalizeWindowOffset(nextPosition && nextPosition.offsetX, viewportWidth);
      const offsetY = normalizeWindowOffset(nextPosition && nextPosition.offsetY, viewportHeight);

      if (currentWindow.offsetX === offsetX && currentWindow.offsetY === offsetY) {
        return current;
      }

      return {
        ...current,
        [windowId]: {
          ...currentWindow,
          offsetX,
          offsetY,
        },
      };
    });
  }, []);

  const resetWindowPosition = useCallback(windowId => {
    setWindowsById(current => {
      const currentWindow = current[windowId] || {};
      if (!currentWindow.offsetX && !currentWindow.offsetY) {
        return current;
      }

      return {
        ...current,
        [windowId]: {
          ...currentWindow,
          offsetX: 0,
          offsetY: 0,
        },
      };
    });
  }, []);

  const contextValue = useMemo(
    () => ({
      windowsById,
      openOrder,
      registerWindow,
      unregisterWindow,
      openWindow,
      minimizeWindow,
      restoreWindow,
      toggleMaximizeWindow,
      bringToFront,
      updateWindowPosition,
      resetWindowPosition,
    }),
    [
      bringToFront,
      minimizeWindow,
      openOrder,
      openWindow,
      registerWindow,
      resetWindowPosition,
      restoreWindow,
      toggleMaximizeWindow,
      unregisterWindow,
      updateWindowPosition,
      windowsById,
    ]
  );

  const minimizedWindows = Object.entries(windowsById)
    .filter(([, value]) => value && value.minimized)
    .map(([windowId, value]) => ({ windowId, ...value }));
  const hasVisibleWindows = openOrder.length > 0;

  return (
    <OperationalWindowContext.Provider value={contextValue}>
      {children}
      {(hasVisibleWindows || minimizedWindows.length > 0) && (
        <div className={styles.windowLayer} aria-hidden>
          {hasVisibleWindows && <div className={styles.windowBackdrop} />}
        </div>
      )}
      {minimizedWindows.length > 0 && (
        <div className={styles.windowDock}>
          <div className={styles.windowDockLabel}>
            {localizeOperationalWindowText('Janelas minimizadas', 'Minimized windows')}
          </div>
          <div className={styles.windowDockItems}>
            {minimizedWindows.map(windowEntry => (
              <div key={windowEntry.windowId} className={styles.windowDockItem}>
                <span className={styles.windowDockItemLabel}>{windowEntry.title}</span>
                <button
                  type="button"
                  className={styles.windowDockButton}
                  onClick={() => restoreWindow(windowEntry.windowId)}
                >
                  {localizeOperationalWindowText('Restaurar', 'Restore')}
                </button>
                {closeHandlersRef.current[windowEntry.windowId] && (
                  <button
                    type="button"
                    className={styles.windowDockButton}
                    onClick={closeHandlersRef.current[windowEntry.windowId]}
                  >
                    Fechar
                  </button>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </OperationalWindowContext.Provider>
  );
};

const useOperationalWindowManager = () => useContext(OperationalWindowContext);

const useWindowRegistration = ({ windowId, title, open, onClose }) => {
  const manager = useOperationalWindowManager();
  const registerWindow = manager ? manager.registerWindow : null;
  const unregisterWindow = manager ? manager.unregisterWindow : null;
  const openWindow = manager ? manager.openWindow : null;

  useEffect(() => {
    if (!registerWindow || !unregisterWindow || !windowId) {
      return undefined;
    }

    registerWindow(
      windowId,
      {
        title,
        open: Boolean(open),
        minimized: false,
        maximized: false,
      },
      undefined
    );

    return () => {
      unregisterWindow(windowId);
    };
  }, [open, registerWindow, title, unregisterWindow, windowId]);

  useEffect(() => {
    if (!registerWindow || !windowId || !open) {
      return;
    }

    registerWindow(windowId, {}, onClose);
  }, [onClose, open, registerWindow, windowId]);

  useEffect(() => {
    if (!registerWindow || !unregisterWindow || !openWindow || !windowId) {
      return;
    }

    if (!open) {
      unregisterWindow(windowId);
      return;
    }

    registerWindow(
      windowId,
      {
        title,
        open: Boolean(open),
      },
      undefined
    );

    openWindow(windowId);
  }, [open, openWindow, registerWindow, title, unregisterWindow, windowId]);
};

export const OperationalWindowDialog = ({
  windowId,
  title,
  eyebrow = localizeOperationalWindowText('Workspace operacional', 'Operational workspace'),
  open,
  onClose,
  preferredWidth = '86vw',
  preferredHeight = '82vh',
  children,
}) => {
  const manager = useOperationalWindowManager();
  const dragStateRef = useRef(null);

  useWindowRegistration({ windowId, title, open, onClose });

  const windowMeta = manager && manager.windowsById ? manager.windowsById[windowId] : null;
  const stackIndex = manager ? manager.openOrder.indexOf(windowId) : 0;
  const openCount = manager ? manager.openOrder.length : 1;
  const isActive = stackIndex === openCount - 1;
  const isMaximized = Boolean(windowMeta && windowMeta.maximized);
  const isMinimized = Boolean(windowMeta && windowMeta.minimized);

  const handleDragPointerMove = useCallback(
    event => {
      if (!dragStateRef.current || !manager) {
        return;
      }

      const deltaX = event.clientX - dragStateRef.current.startX;
      const deltaY = event.clientY - dragStateRef.current.startY;
      manager.updateWindowPosition(windowId, {
        offsetX: dragStateRef.current.initialX + deltaX,
        offsetY: dragStateRef.current.initialY + deltaY,
      });
    },
    [manager, windowId]
  );

  const handleDragPointerUp = useCallback(() => {
    if (typeof window !== 'undefined') {
      window.removeEventListener('pointermove', handleDragPointerMove);
      window.removeEventListener('pointerup', handleDragPointerUp);
      window.removeEventListener('pointercancel', handleDragPointerUp);
    }
    dragStateRef.current = null;
  }, [handleDragPointerMove]);

  const handleHeaderPointerDown = useCallback(
    event => {
      if (!manager || isMaximized || event.button !== 0) {
        return;
      }

      if (event.target instanceof Element && event.target.closest('button')) {
        return;
      }

      event.preventDefault();
      manager.bringToFront(windowId);
      dragStateRef.current = {
        startX: event.clientX,
        startY: event.clientY,
        initialX:
          windowMeta && Number.isFinite(Number(windowMeta.offsetX)) ? Number(windowMeta.offsetX) : 0,
        initialY:
          windowMeta && Number.isFinite(Number(windowMeta.offsetY)) ? Number(windowMeta.offsetY) : 0,
      };

      if (typeof window !== 'undefined') {
        window.addEventListener('pointermove', handleDragPointerMove);
        window.addEventListener('pointerup', handleDragPointerUp);
        window.addEventListener('pointercancel', handleDragPointerUp);
      }
    },
    [handleDragPointerMove, handleDragPointerUp, isMaximized, manager, windowId, windowMeta]
  );

  useEffect(
    () => () => {
      handleDragPointerUp();
    },
    [handleDragPointerUp]
  );

  if (!open || isMinimized) {
    return null;
  }

  const metrics = buildWindowMetrics({
    stackIndex: stackIndex < 0 ? 0 : stackIndex,
    openCount,
    maximized: isMaximized,
    preferredWidth,
    preferredHeight,
    offsetX: windowMeta && Number.isFinite(Number(windowMeta.offsetX)) ? windowMeta.offsetX : 0,
    offsetY: windowMeta && Number.isFinite(Number(windowMeta.offsetY)) ? windowMeta.offsetY : 0,
  });

  const dialogNode = (
    <div
      role="dialog"
      aria-modal="false"
      className={`${styles.windowSurface} ${isActive ? styles.windowSurfaceActive : ''}`}
      style={{
        zIndex: WINDOW_STACK_BASE_Z_INDEX + (stackIndex < 0 ? 0 : stackIndex) * 10,
        width: metrics.width,
        height: metrics.height,
        transform: metrics.transform,
      }}
      onMouseDown={() => manager && manager.bringToFront(windowId)}
    >
      <div
        className={styles.windowHeader}
        onPointerDown={handleHeaderPointerDown}
        onDoubleClick={() => manager && manager.resetWindowPosition(windowId)}
      >
        <div className={styles.windowTitleBlock}>
          <span className={styles.windowEyebrow}>{eyebrow}</span>
          <span className={styles.windowTitle}>{title}</span>
        </div>
        <div className={styles.windowActions}>
          <button
            type="button"
            className={styles.windowActionButton}
            aria-label={localizeOperationalWindowText('Minimizar janela', 'Minimize window')}
            onClick={() => manager && manager.minimizeWindow(windowId)}
          >
            <MinusOutlined />
          </button>
          <button
            type="button"
            className={styles.windowActionButton}
            aria-label={localizeOperationalWindowText(
              isMaximized ? 'Restaurar tamanho da janela' : 'Maximizar janela',
              isMaximized ? 'Restore window size' : 'Maximize window'
            )}
            onClick={() => manager && manager.toggleMaximizeWindow(windowId)}
          >
            {isMaximized ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
          </button>
          <button
            type="button"
            className={styles.windowActionButton}
            aria-label={localizeOperationalWindowText('Fechar janela', 'Close window')}
            onClick={onClose}
          >
            <CloseOutlined />
          </button>
        </div>
      </div>
      <div className={styles.windowBody}>{children}</div>
    </div>
  );

  if (typeof document === 'undefined' || !document.body) {
    return dialogNode;
  }

  return ReactDOM.createPortal(dialogNode, document.body);
};

export const OperationalWindowDockHandle = BorderOutlined;
