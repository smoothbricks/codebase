/**
 * Backend dependency injection for columine.
 *
 * Columine uses a backend to execute bytecode programs against columnar data.
 * Two usage patterns:
 *
 * 1. Standalone: getBackend() lazy-loads columine's own WASM binary
 *    that includes both reducer and RETE opcodes in a single dispatch loop
 */

import type { ColumineBackend } from './types.js';

let _backend: ColumineBackend | null = null;
let _lazyLoader: (() => Promise<ColumineBackend>) | null = null;

/**
 * Must be called before any columine operations if not using the default WASM backend.
 */
export function setBackend(backend: ColumineBackend): void {
  _backend = backend;
}

/**
 * Set a custom lazy loader for the backend.
 * Called by platform packages (e.g., columine WASM loader) to register themselves.
 */
export function setBackendLoader(loader: () => Promise<ColumineBackend>): void {
  _lazyLoader = loader;
}

/**
 * Get the current backend.
 * If no backend was injected via setBackend(), uses the registered lazy loader.
 *
 * @throws Error if no backend was injected and no loader is registered
 */
export async function getBackend(): Promise<ColumineBackend> {
  if (_backend) return _backend;

  if (_lazyLoader) {
    _backend = await _lazyLoader();
    return _backend;
  }

  throw new Error(
    'No columine backend available. Either call setBackend() to inject a backend, ' +
      'or call setBackendLoader() to register a lazy loader.',
  );
}

/**
 * Check if a backend has been injected or loaded.
 * Useful for diagnostics and testing.
 */
export function hasBackend(): boolean {
  return _backend !== null;
}

/**
 * Reset the backend to null. Primarily for testing.
 */
export function resetBackend(): void {
  _backend = null;
}
