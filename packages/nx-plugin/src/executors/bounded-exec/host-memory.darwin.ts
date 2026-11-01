import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { HostMemory, RunMemoryReading } from './host-memory.js';

/**
 * macOS's memory mechanisms. A run's memory is the host's, `hw.memsize`: macOS confines no job to a memory limit of
 * its own. Bun reports a leaf's `ru_maxrss` in bytes, as macOS does.
 */

const run = promisify(execFile);
const HW_MEMSIZE = 'hw.memsize';

async function readRunMemory(): Promise<RunMemoryReading> {
  let stdout: string;
  try {
    ({ stdout } = await run('sysctl', ['-n', HW_MEMSIZE]));
  } catch (error) {
    return {
      ok: false,
      error: {
        operation: 'read-run-memory',
        code: 'unreadable',
        subject: HW_MEMSIZE,
        cause: error instanceof Error ? error.message : String(error),
      },
    };
  }
  const value = stdout.trim();
  const bytes = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(bytes) || bytes === 0) {
    return {
      ok: false,
      error: {
        operation: 'read-run-memory',
        code: 'malformed',
        subject: HW_MEMSIZE,
        cause: `sysctl printed ${JSON.stringify(value)}, not a byte count`,
      },
    };
  }
  return { ok: true, memory: { bytes, source: HW_MEMSIZE } };
}

export const darwinMemory: HostMemory = { readRunMemory, peakBytes: (maxRss) => maxRss };
