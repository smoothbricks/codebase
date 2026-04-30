/// <reference types="bun" />

function isTruthyEnvFlag(value: unknown): boolean {
  return value === true || value === '1' || value === 'true';
}

export function isCleanupDebugEnabled(): boolean {
  const globalDebug = Reflect.get(globalThis, '__LMAO_TEST_CLEANUP_DEBUG__');
  if (isTruthyEnvFlag(globalDebug)) {
    return true;
  }

  return isTruthyEnvFlag(process.env.LMAO_TEST_CLEANUP_DEBUG);
}

export function cleanupDebug(label: string, details?: Record<string, unknown>): void {
  if (!isCleanupDebugEnabled()) {
    return;
  }

  const suffix = details ? ` ${JSON.stringify(details)}` : '';
  console.error(`[lmao/cleanup] ${label}${suffix}`);
}

export function cleanupDebugActiveHandles(label: string): void {
  if (!isCleanupDebugEnabled()) {
    return;
  }

  const getActiveHandles = Reflect.get(process, '_getActiveHandles');
  if (typeof getActiveHandles !== 'function') {
    cleanupDebug(label, { activeHandles: 'unavailable' });
    return;
  }

  const handles = getActiveHandles.call(process);
  if (!Array.isArray(handles)) {
    cleanupDebug(label, { activeHandles: 'unavailable' });
    return;
  }

  const names = handles.map((handle) => {
    if (typeof handle !== 'object' || handle === null) {
      return typeof handle;
    }
    const constructor = Reflect.get(handle, 'constructor');
    const name = typeof constructor === 'function' ? Reflect.get(constructor, 'name') : undefined;
    return typeof name === 'string' && name.length > 0 ? name : 'Object';
  });

  cleanupDebug(label, { activeHandleCount: handles.length, activeHandles: names });
}
