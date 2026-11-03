import { totalmem } from 'node:os';
import type { HostMemory, RunMemoryReading } from './host-memory.js';

/**
 * macOS's memory mechanisms. A run's memory is the host's, `hw.memsize`: macOS confines no job to a memory limit of
 * its own. Bun reports a leaf's `ru_maxrss` in bytes, as macOS does.
 */

const HW_MEMSIZE = 'hw.memsize';

/**
 * `hw.memsize`, read in this process: `os.totalmem()` is that sysctl under Node (libuv) and Bun alike. A child `sysctl`
 * cost each run a process, and under `bun test` its exit sometimes never settled, so the pool a run serves never
 * started: eight concurrent runs of the pool-silence cases at load ~50 hung there 6 times out of 24.
 */
function readRunMemory(): Promise<RunMemoryReading> {
  const bytes = totalmem();
  if (!Number.isSafeInteger(bytes) || bytes <= 0) {
    return Promise.resolve({
      ok: false,
      error: {
        operation: 'read-run-memory',
        code: 'malformed',
        subject: HW_MEMSIZE,
        cause: `os.totalmem() answered ${String(bytes)}, not a byte count`,
      },
    });
  }
  return Promise.resolve({ ok: true, memory: { bytes, source: HW_MEMSIZE } });
}

/**
 * Bun's FFI, as the peak waiter reaches it. Node, which runs the executor, loads this module too and has no `bun:ffi`;
 * only the waiter, on Bun, reads a tree's footprint, so the FFI loads on its first read, by a specifier the compiler
 * does not resolve against Node's types.
 */
const BUN_FFI = 'bun:ffi';
const LIBPROC = '/usr/lib/libproc.dylib';
/** `proc_pid_rusage`'s oldest flavor, whose fields every later one starts with. */
const RUSAGE_INFO_V0 = 0;
/** `rusage_info_v0`: a 16-byte uuid, then ten u64 counters; `ri_phys_footprint` is the eighth, u64 index 9. */
const RUSAGE_INFO_V0_WORDS = 12;
const PHYS_FOOTPRINT = 9;

interface BunFfi {
  dlopen(path: string, symbols: Record<string, { args: string[]; returns: string }>): { symbols: unknown };
  ptr(view: ArrayBufferView): number;
}

/** The two libproc calls a tree's footprint takes, each pointer a Bun FFI address. */
interface LibprocSymbols {
  /** The pids whose parent is `pid`, written into `into` (`bytes` long): how many, or -1. */
  proc_listchildpids(pid: number, into: number, bytes: number): number;
  /** `pid`'s `rusage_info` of `flavor`, written into `into`: 0, or -1 once nothing it may read holds the pid. */
  proc_pid_rusage(pid: number, flavor: number, into: number): number;
}

function isBunFfi(module: unknown): module is BunFfi {
  return (
    typeof module === 'object' &&
    module !== null &&
    'dlopen' in module &&
    typeof module.dlopen === 'function' &&
    'ptr' in module &&
    typeof module.ptr === 'function'
  );
}

function isLibprocSymbols(symbols: unknown): symbols is LibprocSymbols {
  return (
    typeof symbols === 'object' &&
    symbols !== null &&
    'proc_listchildpids' in symbols &&
    typeof symbols.proc_listchildpids === 'function' &&
    'proc_pid_rusage' in symbols &&
    typeof symbols.proc_pid_rusage === 'function'
  );
}

/** One process's children and footprint, as libproc reads them. */
interface Footprints {
  children(pid: number): number[];
  /** What `pid` holds, or 0 once it is gone (or, a setuid program, unreadable to its owner's uid). */
  footprint(pid: number): number;
}

let footprints: Promise<Footprints> | undefined;

async function loadFootprints(): Promise<Footprints> {
  const ffi: unknown = await import(BUN_FFI);
  if (!isBunFfi(ffi)) {
    throw new Error(`${BUN_FFI} names no dlopen and ptr: a tree's footprint is read on Bun, by the peak waiter`);
  }
  const { symbols } = ffi.dlopen(LIBPROC, {
    proc_listchildpids: { args: ['i32', 'ptr', 'i32'], returns: 'i32' },
    proc_pid_rusage: { args: ['i32', 'i32', 'ptr'], returns: 'i32' },
  });
  if (!isLibprocSymbols(symbols)) {
    throw new Error(`${LIBPROC} opened without proc_listchildpids and proc_pid_rusage`);
  }
  const usage = new BigUint64Array(RUSAGE_INFO_V0_WORDS);
  const usageAt = ffi.ptr(usage);
  let pids = new Int32Array(256);
  return {
    children(pid) {
      // A full buffer may have cut the list short: it is read again into one twice the size.
      for (;;) {
        const count = symbols.proc_listchildpids(pid, ffi.ptr(pids), pids.byteLength);
        if (count < pids.length) return count <= 0 ? [] : Array.from(pids.subarray(0, count));
        pids = new Int32Array(pids.length * 2);
      }
    },
    footprint(pid) {
      return symbols.proc_pid_rusage(pid, RUSAGE_INFO_V0, usageAt) === 0 ? Number(usage[PHYS_FOOTPRINT]) : 0;
    },
  };
}

/** What the tree rooted at `root` holds: each member's `ri_phys_footprint`, the tree walked by `proc_listchildpids`. */
async function treeResidentBytes(root: number): Promise<number> {
  footprints ??= loadFootprints();
  const proc = await footprints;
  const seen = new Set<number>();
  const pending = [root];
  let total = 0;
  for (let pid = pending.pop(); pid !== undefined; pid = pending.pop()) {
    if (seen.has(pid)) continue;
    seen.add(pid);
    total += proc.footprint(pid);
    pending.push(...proc.children(pid));
  }
  return total;
}

export const darwinMemory: HostMemory = { readRunMemory, peakBytes: (maxRss) => maxRss, treeResidentBytes };
