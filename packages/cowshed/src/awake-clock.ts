/// <reference types="node" />

import { fileURLToPath } from 'node:url';
import { loadAddon } from './addon.js';

interface AwakeClockAddon {
  awakeTimeMs(): number;
}

function isAwakeClockAddon(module: unknown): module is AwakeClockAddon {
  return (
    typeof module === 'object' && module !== null && 'awakeTimeMs' in module && typeof module.awakeTimeMs === 'function'
  );
}

let addon: AwakeClockAddon | undefined;

/** Monotonic milliseconds since a process-local origin, excluding host suspension. */
export function awakeTimeMs(): number {
  addon ??= loadAddon(fileURLToPath(import.meta.url), (module) => {
    if (!isAwakeClockAddon(module)) {
      throw new Error('the addon exports no awakeTimeMs: it was built from an older tree than this entry');
    }
    return module;
  });
  return addon.awakeTimeMs();
}
