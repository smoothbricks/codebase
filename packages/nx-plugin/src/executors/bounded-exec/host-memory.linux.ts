import { readFile } from 'node:fs/promises';
import { posix } from 'node:path';
import { childPids } from '../../process-children.linux.js';
import type { HostMemory, RunMemory, RunMemoryError, RunMemoryReading } from './host-memory.js';

/**
 * Linux's memory mechanisms. The run's memory is the least of the host's `MemTotal` and every finite `memory.max` of
 * the cgroup v2 the run's process is in and of its ancestors, up to the root of the mounted hierarchy: the kernel holds
 * a cgroup to its ancestors' limits too, so a job in cowshed's `background` class, whose own `memory.max` is `max`, has
 * the class's three quarters of the host. A host with no cgroup v2 hierarchy, or a hierarchy that sets no limit, gives
 * the run `MemTotal`.
 */

const MEMINFO = '/proc/meminfo';
const CGROUP = '/proc/self/cgroup';
const MOUNTINFO = '/proc/self/mountinfo';

/** A file's text, or the errno code and message its read failed with. */
export type FileRead =
  | { readonly ok: true; readonly text: string }
  | { readonly ok: false; readonly code: string; readonly message: string };

const failed = (code: RunMemoryError['code'], subject: string, cause: string): RunMemoryReading => ({
  ok: false,
  error: { operation: 'read-run-memory', code, subject, cause },
});
const unreadable = (subject: string, read: Extract<FileRead, { ok: false }>): RunMemoryReading =>
  failed('unreadable', subject, `${read.code}: ${read.message}`);

/** A mount point or root as mountinfo writes it, its space, tab, newline and backslash escaped in octal. */
const decodeOctal = (field: string): string =>
  field.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));

/** The cgroup v2 mounts mountinfo lists: the cgroup each shows (`root`) and where (`point`). */
function cgroup2Mounts(mountinfo: string): { root: string; point: string }[] {
  const mounts: { root: string; point: string }[] = [];
  for (const line of mountinfo.split('\n')) {
    const fields = line.split(' ');
    const separator = fields.indexOf('-');
    const [root, point] = [fields[3], fields[4]];
    if (separator !== -1 && fields[separator + 1] === 'cgroup2' && root !== undefined && point !== undefined) {
      mounts.push({ root: decodeOctal(root), point: decodeOctal(point) });
    }
  }
  return mounts;
}

/** The run's memory, reading every file through `read`. */
export async function linuxRunMemory(read: (path: string) => Promise<FileRead>): Promise<RunMemoryReading> {
  const meminfo = await read(MEMINFO);
  if (!meminfo.ok) return unreadable(MEMINFO, meminfo);
  const total = /^MemTotal:\s+(\d+) kB$/m.exec(meminfo.text)?.[1];
  if (total === undefined) return failed('malformed', MEMINFO, 'it names no MemTotal in kB');
  let least: RunMemory = { bytes: Number(total) * 1024, source: `${MEMINFO} MemTotal` };
  const cgroups = await read(CGROUP);
  if (!cgroups.ok) return unreadable(CGROUP, cgroups);
  const path = /^0::(\/.*)$/m.exec(cgroups.text)?.[1];
  if (path === undefined) {
    // No cgroup v2 hierarchy holds the run: the host's memory is its memory.
    return { ok: true, memory: least };
  }
  const mounts = await read(MOUNTINFO);
  if (!mounts.ok) return unreadable(MOUNTINFO, mounts);
  const mount = cgroup2Mounts(mounts.text).find(
    ({ root }) => root === '/' || path === root || path.startsWith(`${root}/`),
  );
  if (mount === undefined) {
    return failed('unmounted', path, `${MOUNTINFO} lists no cgroup2 mount that shows the run's cgroup`);
  }
  const top = posix.resolve(mount.point);
  const own = posix.resolve(top, `.${mount.root === '/' ? path : path.slice(mount.root.length)}`);
  if (own !== top && !own.startsWith(`${top}/`)) {
    return failed('unmounted', path, `the run's cgroup lies outside the cgroup2 mount at ${top}`);
  }
  for (let directory = own; ; ) {
    const file = posix.join(directory, 'memory.max');
    const limit = await read(file);
    if (limit.ok) {
      const value = limit.text.trim();
      if (value !== 'max') {
        if (!/^\d+$/.test(value)) {
          return failed('malformed', file, `it holds ${JSON.stringify(value)}, not max or bytes`);
        }
        // A limit above the host's memory (a near-2^63 one included) is no constraint, and is never made a number.
        if (BigInt(value) < BigInt(least.bytes)) least = { bytes: Number(value), source: file };
      }
    } else if (limit.code !== 'ENOENT') {
      // ENOENT: the hierarchy's root, or a cgroup whose parent enables no memory controller, has no limit of its own.
      return unreadable(file, limit);
    }
    if (directory === top) break;
    directory = posix.dirname(directory);
  }
  return { ok: true, memory: least };
}

async function readText(path: string): Promise<FileRead> {
  try {
    return { ok: true, text: await readFile(path, 'utf8') };
  } catch (error) {
    const code = error instanceof Error && 'code' in error && typeof error.code === 'string' ? error.code : 'EIO';
    return { ok: false, code, message: error instanceof Error ? error.message : String(error) };
  }
}

/** Bun before 1.4.0 reports Linux's `ru_maxrss` as the kernel does, in KiB; from 1.4.0 it reports bytes. */
function peakBytes(maxRss: number, bunVersion: string): number {
  const version = /^(\d+)\.(\d+)\./.exec(bunVersion);
  if (version === null) {
    throw new Error(`Bun names its version ${JSON.stringify(bunVersion)}, which is no semver`);
  }
  const [major, minor] = [Number(version[1]), Number(version[2])];
  return major < 1 || (major === 1 && minor < 4) ? maxRss * 1024 : maxRss;
}

/** What the tree rooted at `root` holds resident: each member's `VmRSS`, the tree walked by the kernel's child links. */
async function treeResidentBytes(root: number): Promise<number> {
  const seen = new Set<number>();
  let level = [root];
  let total = 0;
  while (level.length > 0) {
    const members = level.filter((pid) => !seen.has(pid));
    for (const pid of members) seen.add(pid);
    const [resident, children] = await Promise.all([Promise.all(members.map(residentBytes)), childPids(members)]);
    total += resident.reduce((sum, bytes) => sum + bytes, 0);
    level = children;
  }
  return total;
}

/**
 * `VmRSS` from `/proc/<pid>/status`: what the process holds resident, anonymous, file-backed and shared memory together.
 * A process that has exited (`ENOENT`, `ESRCH`) holds nothing, and so does a zombie, whose status names no `VmRSS`.
 */
async function residentBytes(pid: number): Promise<number> {
  let status: string;
  try {
    status = await readFile(`/proc/${pid}/status`, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ESRCH')) return 0;
    throw error;
  }
  const kib = /^VmRSS:\s+(\d+) kB$/m.exec(status)?.[1];
  return kib === undefined ? 0 : Number(kib) * 1024;
}

export const linuxMemory: HostMemory = { readRunMemory: () => linuxRunMemory(readText), peakBytes, treeResidentBytes };
