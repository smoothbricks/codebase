/// <reference types="node" />

import { fileURLToPath } from 'node:url';
import { loadAddon } from './addon.js';

/**
 * `@smoothbricks/cowshed/process-lineage` (07_api.md, "Process lineage"): the kernel's own record of
 * where processes descend from, read through the addon with nothing else of the package. It is
 * compiled on its own (`cowshed:lineage-js` into `dist/lineage`), so a consumer such as
 * bounded-exec's admission observer compiles against it without the package's whole build, and
 * the addon is loaded at the first call, never at import.
 */

/** Where a process descends from, as the kernel's own record of it says. */
export type ProcessLineage = {
  readonly pid: number;
  readonly ppid: number;
  readonly pgid: number;
  /** Exited, not yet reaped by its parent. */
  readonly zombie: boolean;
};

/** The addon as this entry uses it: the one export it calls. */
interface LineageAddon {
  processLineages(pids: readonly number[]): ProcessLineage[];
}

function isLineageAddon(module: unknown): module is LineageAddon {
  return (
    typeof module === 'object' &&
    module !== null &&
    'processLineages' in module &&
    typeof module.processLineages === 'function'
  );
}

function acceptLineageAddon(module: unknown): LineageAddon {
  if (!isLineageAddon(module)) {
    throw new Error('the addon exports no processLineages: it was built from an older tree than this entry');
  }
  return module;
}

let addon: LineageAddon | undefined;

/** The largest pid the addon's 32-bit argument carries. */
const PID_MAX = 0x7fff_ffff;

/**
 * The lineage of each listed pid a process holds, in the order asked; a pid none holds is left out,
 * as `ps -p` leaves it out. Each is the kernel's own record of that pid alone (`proc_pidinfo` on
 * Darwin, `/proc/<pid>/stat` on Linux): no process-table census, no spawned process, no wait on
 * another process's task, and no `cowshed host`. A number that is no 32-bit integer is a
 * programmer's error (`RangeError`); a pid that is not positive, or a record that cannot be read,
 * throws the addon's own error with its `code` (`usage`, `environment-missing`), message and hint.
 */
export function processLineages(pids: readonly number[]): ProcessLineage[] {
  for (const pid of pids) {
    if (!Number.isInteger(pid) || pid < -PID_MAX - 1 || pid > PID_MAX) {
      throw new RangeError(`${pid} is not a 32-bit integer process id`);
    }
  }
  addon ??= loadAddon(fileURLToPath(import.meta.url), acceptLineageAddon);
  return addon.processLineages(pids);
}
