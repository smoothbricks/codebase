/// <reference types="node" />

/**
 * `@smoothbricks/procinfo` (specs/procinfo/01_procinfo.md): host process facts, each read from the kernel's own
 * record of one process or one group, never a spawned `ps` or `lsof` and never a census of the process table. Every
 * export loads the addon at its first call, never at import. A number that is no 32-bit integer is a programmer's
 * error (`RangeError`); an id that names no process throws the addon's error with `code: "usage"`, and a record that
 * cannot be read one with `code: "environment-missing"`, each with its message and `hint`.
 */

import { fileURLToPath } from 'node:url';
import { loadAddon } from './addon.js';

/** Where a process descends from, as the kernel's own record of it says. */
export type ProcessLineage = {
  readonly pid: number;
  readonly ppid: number;
  readonly pgid: number;
  /** Exited, not yet reaped by its parent. */
  readonly zombie: boolean;
};

/** What the kernel shows a process doing, as `ps` names its state. */
export type RunState =
  | { readonly state: 'running' | 'sleeping' | 'uninterruptible' | 'stopped' | 'zombie' | 'gone' }
  /** The kernel's answer could not be read; `reason` says why. */
  | { readonly state: 'unread'; readonly reason: string };

/** A process's executable path and its byte-exact argument vector, as the kernel records them. */
export type ProcessArguments = { readonly executable: Uint8Array; readonly argv: readonly Uint8Array[] };

/** One running process of a group: its pid, its command line and what the kernel shows it doing. */
export type GroupMember = { readonly pid: number; readonly command: string; readonly state: RunState };

/** What `lsof -F di` names a descriptor by. */
export type DescriptorName =
  /** lsof's `d` field: the kernel address of a pipe end or a socket (Darwin). */
  | { readonly kind: 'device'; readonly address: bigint }
  /** lsof's `i` field: the inode `fstat(2)` reports. */
  | { readonly kind: 'inode'; readonly inode: bigint }
  | { readonly kind: 'closed' }
  /** Open, of a Darwin kind neither field names: its `PROX_FDTYPE_*` number. */
  | { readonly kind: 'unnamed'; readonly fdType: number };

/** Another process's descriptors, or why they were not read. */
export type DescriptorNames =
  | { readonly descriptors: readonly { readonly fd: number; readonly name: DescriptorName }[] }
  /** `gone`: the process exited or no process has the pid; `withheld`: the kernel withholds them from this user. */
  | { readonly unread: 'gone' | 'withheld' };

/** `getloadavg(3)`: the average run-queue length over the last 1, 5 and 15 minutes. */
export type HostLoad = { readonly one: number; readonly five: number; readonly fifteen: number };

/** The run state as the addon answers it, before it is narrowed to {@link RunState}. */
interface NativeRunState {
  readonly state: string;
  readonly reason?: string | null;
}

/** The descriptor name as the addon answers it, before it is narrowed to {@link DescriptorName}. */
interface NativeDescriptorName {
  readonly kind: string;
  readonly value?: bigint | null;
  readonly fdType?: number | null;
}

/** The addon's exports (crates/procinfo-napi/src/lib.rs). */
interface ProcinfoAddon {
  awakeTimeMs(): number;
  processLineages(pids: readonly number[]): ProcessLineage[];
  running(pid: number): boolean;
  runState(pid: number): NativeRunState;
  processArguments(pid: number): ProcessArguments;
  readonly groupPids?: (pgid: number) => number[];
  readonly groupMembers?: (
    pgid: number,
  ) => { readonly pid: number; readonly command: string; readonly state: NativeRunState }[];
  readonly groupCpu?: (pgid: number) => bigint;
  residentBytes(pid: number): bigint | null | undefined;
  childPids(pid: number): number[];
  cpuTimeNs(pid: number): bigint | null | undefined;
  startedUs(pid: number): number | null | undefined;
  listeningPorts(pid: number): number[];
  descriptorName(fd: number): NativeDescriptorName;
  descriptorNames(
    pid: number,
    devices: boolean,
    inodes: boolean,
  ): {
    readonly descriptors: readonly { readonly fd: number; readonly name: NativeDescriptorName }[];
    readonly unread?: string | null;
  };
  sandboxed(pid: number): boolean;
  hostLoad(): HostLoad | null | undefined;
}

const ADDON_EXPORTS = [
  'awakeTimeMs',
  'processLineages',
  'running',
  'runState',
  'processArguments',
  'residentBytes',
  'listeningPorts',
  'childPids',
  'cpuTimeNs',
  'startedUs',
  'descriptorName',
  'descriptorNames',
  'sandboxed',
  'hostLoad',
] as const satisfies readonly (keyof ProcinfoAddon)[];

function isProcinfoAddon(module: unknown): module is ProcinfoAddon {
  return (
    typeof module === 'object' &&
    module !== null &&
    ADDON_EXPORTS.every((name) => typeof Reflect.get(module, name) === 'function')
  );
}

let loaded: ProcinfoAddon | undefined;

function addon(): ProcinfoAddon {
  loaded ??= loadAddon(fileURLToPath(import.meta.url), (module) => {
    if (!isProcinfoAddon(module)) {
      throw new Error('the procinfo addon lacks an export of this entry: it was built from an older tree');
    }
    return module;
  });
  return loaded;
}

/** The largest number the addon's 32-bit id argument carries. */
const ID_MAX = 0x7fff_ffff;

/** `id` unchanged, or a `RangeError` for a number that is no 32-bit integer. */
function int32(id: number, what: string): number {
  if (!Number.isInteger(id) || id < -ID_MAX - 1 || id > ID_MAX) {
    throw new RangeError(`${id} is not a 32-bit integer ${what}`);
  }
  return id;
}

function runState(read: NativeRunState): RunState {
  switch (read.state) {
    case 'running':
    case 'sleeping':
    case 'uninterruptible':
    case 'stopped':
    case 'zombie':
    case 'gone':
      return { state: read.state };
    case 'unread':
      return { state: 'unread', reason: read.reason ?? 'no reason given' };
    default:
      throw new Error(`the procinfo addon answered an unknown run state ${read.state}`);
  }
}

function descriptorName(read: NativeDescriptorName): DescriptorName {
  switch (read.kind) {
    case 'device':
    case 'inode': {
      if (typeof read.value !== 'bigint') throw new Error(`the procinfo addon named a ${read.kind} without its number`);
      return read.kind === 'device' ? { kind: 'device', address: read.value } : { kind: 'inode', inode: read.value };
    }
    case 'closed':
      return { kind: 'closed' };
    case 'unnamed': {
      if (typeof read.fdType !== 'number') throw new Error('the procinfo addon named an unnamed kind without its type');
      return { kind: 'unnamed', fdType: read.fdType };
    }
    default:
      throw new Error(`the procinfo addon answered an unknown descriptor kind ${read.kind}`);
  }
}

/**
 * Monotonic milliseconds from a process-local origin, excluding host suspension: `CLOCK_UPTIME_RAW` on Darwin,
 * `CLOCK_MONOTONIC` on Linux. Darwin's Node `performance.now()` counts sleep (libuv's `mach_continuous_time`).
 */
export function awakeTimeMs(): number {
  return addon().awakeTimeMs();
}

/**
 * The lineage of each listed pid a process holds, in the order asked; a pid none holds is left out, as `ps -p` leaves
 * it out. Each is the kernel's record of that pid alone (`proc_pidinfo` on Darwin, `/proc/<pid>/stat` on Linux).
 */
export function processLineages(pids: readonly number[]): ProcessLineage[] {
  for (const pid of pids) int32(pid, 'process id');
  return addon().processLineages(pids);
}

/** Whether the process `pid` has not exited. A zombie has; a process this one may not inspect still runs. */
export function running(pid: number): boolean {
  return addon().running(int32(pid, 'process id'));
}

/** The state the kernel shows the process `pid` in; `gone` when no process has the pid. */
export function processRunState(pid: number): RunState {
  return runState(addon().runState(int32(pid, 'process id')));
}

/** The executable and arguments of the process `pid`, byte for byte, as the kernel records them. */
export function processArguments(pid: number): ProcessArguments {
  return addon().processArguments(int32(pid, 'process id'));
}

/** A process group's facts, read where the kernel lists a group's members. */
export interface ProcessGroups {
  /** The pids holding the process group id `pgid`, exited processes included. */
  pids(pgid: number): number[];
  /** The running processes of group `pgid`, each named by its command line (else its executable) and its state. */
  members(pgid: number): GroupMember[];
  /**
   * The CPU the processes of group `pgid` have used, with their reaped children's, in Mach ticks: comparable only to
   * another reading of the same group.
   */
  cpu(pgid: number): bigint;
}

function missingGroups(): Error {
  return new Error('the procinfo addon lacks the process-group exports every Darwin build carries');
}

/**
 * Process-group facts on Darwin, where `proc_listpgrppids` lists a group's members; `null` on Linux, which lists a
 * group only to a census of `/proc`. On Linux a job's processes are its cgroup's, which cowshed's job runner reads.
 */
export const processGroups: ProcessGroups | null =
  process.platform === 'darwin'
    ? {
        pids(pgid) {
          const read = addon().groupPids;
          if (read === undefined) throw missingGroups();
          return read(int32(pgid, 'process group id'));
        },
        members(pgid) {
          const read = addon().groupMembers;
          if (read === undefined) throw missingGroups();
          return read(int32(pgid, 'process group id')).map((member) => ({
            pid: member.pid,
            command: member.command,
            state: runState(member.state),
          }));
        },
        cpu(pgid) {
          const read = addon().groupCpu;
          if (read === undefined) throw missingGroups();
          return read(int32(pgid, 'process group id'));
        },
      }
    : null;

/** The bytes the process `pid` holds resident now, or `null` once no such process runs. */
export function residentBytes(pid: number): bigint | null {
  return addon().residentBytes(int32(pid, 'process id')) ?? null;
}

/**
 * The pids whose parent is the process `pid`, as the kernel links them now (`proc_listchildpids` on Darwin, each
 * thread's `/proc/<pid>/task/<tid>/children` on Linux): none for a pid no process holds. A tree is walked from its root
 * one parent at a time, never by a census of the process table.
 */
export function childPids(pid: number): number[] {
  return addon().childPids(int32(pid, 'process id'));
}

/**
 * The CPU time the process `pid` has used, user and system together, in nanoseconds; `null` once it has exited, an
 * unreaped one included, or no process has the pid. Two readings of one process differ by the CPU it used between them.
 */
export function cpuTimeNs(pid: number): bigint | null {
  return addon().cpuTimeNs(int32(pid, 'process id')) ?? null;
}

/**
 * When the process `pid` started, in microseconds since the Unix epoch: fixed for its life, so with its pid it names one
 * process. `null` once no process has the pid. Linux counts it from a boot time given in whole seconds.
 */
export function startedUs(pid: number): number | null {
  return addon().startedUs(int32(pid, 'process id')) ?? null;
}

/** The TCP ports the process `pid` listens on, IPv4 and IPv6 alike, each once, ascending. */
export function listeningPorts(pid: number): number[] {
  return addon().listeningPorts(int32(pid, 'process id'));
}

/** The name of this process's own descriptor `fd`. */
export function ownDescriptorName(fd: number): DescriptorName {
  return descriptorName(addon().descriptorName(int32(fd, 'file descriptor')));
}

/**
 * The names of the open descriptors of the process `pid`: `devices` asks for pipes and sockets (named by address on
 * Darwin), `inodes` for everything named by an inode. Neither asked reads nothing.
 */
export function descriptorNames(
  pid: number,
  kinds: { readonly devices: boolean; readonly inodes: boolean },
): DescriptorNames {
  const read = addon().descriptorNames(int32(pid, 'process id'), kinds.devices, kinds.inodes);
  switch (read.unread) {
    case undefined:
    case null:
      return { descriptors: read.descriptors.map(({ fd, name }) => ({ fd, name: descriptorName(name) })) };
    case 'gone':
    case 'withheld':
      return { unread: read.unread };
    default:
      throw new Error(`the procinfo addon answered an unknown reason ${read.unread}`);
  }
}

/** Whether the process `pid` may run sandboxed; `false` proves a live, unsandboxed process. */
export function sandboxed(pid: number): boolean {
  return addon().sandboxed(int32(pid, 'process id'));
}

/** The host's load now, or `null` when the kernel will not report all three averages. */
export function hostLoad(): HostLoad | null {
  return addon().hostLoad() ?? null;
}
