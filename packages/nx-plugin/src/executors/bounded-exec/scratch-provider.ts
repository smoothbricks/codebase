import { realpathSync } from 'node:fs';
import { userInfo } from 'node:os';
import type { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import {
  COWSHED_EXECUTABLE_ENV,
  cowshedSubtreeOwner,
  SUPERVISOR_SOCKET_ENV,
  type SubtreeOwner,
} from './owned-subtree.js';
import {
  archiveDeletions,
  createHostCommands,
  describeRamTempError,
  PRODUCTION,
  RAM_TEMP_CAPACITY_BYTES,
  type RamTempAcquisition,
  type RamTempError,
  type RamTempLease,
  RamTempVolume,
  type Result,
  ramTempPaths,
} from './ram-temp.js';

/** The subset of `RamTempVolume` that the host-side scratch provider drives. */
export interface ScratchProviderVolume {
  acquireHostLease(subtrees: SubtreeOwner, workspace?: string): Promise<Result<RamTempAcquisition, RamTempError>>;
  settleHostLease(lease: RamTempLease, subtrees: SubtreeOwner): Promise<Result<string[], RamTempError>>;
}

export interface ScratchProviderIO {
  readonly platform?: NodeJS.Platform;
  readonly stdin: Readable;
  readonly stdout: { write(chunk: string): boolean };
  readonly stderr: { write(chunk: string): boolean };
  readonly workspace?: string;
  readonly subtrees: SubtreeOwner;
  readonly volume: ScratchProviderVolume;
}

/**
 * Cowshed's `[scratch] provider` host-side protocol:
 * 1. Acquire a host lease (`<mount>/h<5>` with `<mount>/h<5>.holder`) on the RAM temp volume.
 * 2. Write one JSON line on stdout: `{"directory":"/abs/path"}` or `{"unavailable":"<reason>"}`.
 * 3. After `directory`, stay alive holding the lease until stdin EOF, then archive failed task
 *    directories through the volume's retention, delete the rest and the sidecar, and exit.
 */
export async function runScratchProvider(io: ScratchProviderIO): Promise<number> {
  if ((io.platform ?? process.platform) !== 'darwin') {
    io.stdout.write(`${JSON.stringify({ unavailable: 'RAM temp volume is only supported on macOS' })}\n`);
    return 0;
  }
  let acquired: Result<RamTempAcquisition, RamTempError>;
  try {
    acquired = await io.volume.acquireHostLease(io.subtrees, io.workspace);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    io.stdout.write(`${JSON.stringify({ unavailable: detail })}\n`);
    return 0;
  }
  if (!acquired.ok) {
    io.stdout.write(`${JSON.stringify({ unavailable: describeRamTempError(acquired.error) })}\n`);
    return 0;
  }
  if (acquired.value.kind === 'unavailable') {
    io.stdout.write(`${JSON.stringify({ unavailable: `${acquired.value.reason}: ${acquired.value.detail}` })}\n`);
    return 0;
  }
  if (acquired.value.kind === 'sandboxed') {
    io.stdout.write(`${JSON.stringify({ unavailable: `sandboxed: ${acquired.value.detail}` })}\n`);
    return 0;
  }
  for (const held of acquired.value.held) {
    io.stderr.write(`RAM temp volume: dead lease ${held}\n`);
  }
  for (const reaped of acquired.value.reaped) {
    io.stderr.write(`RAM temp volume: the supervisor stopped ${reaped}, which a dead task's command left running\n`);
  }
  for (const gap of acquired.value.gaps) {
    io.stderr.write(`RAM temp volume: the supervisor's view of a dead task's command has a gap: ${gap}\n`);
  }
  for (const retained of acquired.value.retained) {
    io.stderr.write(`RAM temp volume: dead lease ${retained}\n`);
  }
  const { lease } = acquired.value;
  io.stdout.write(`${JSON.stringify({ directory: lease.directory })}\n`);
  await waitForEof(io.stdin);
  const settled = await io.volume.settleHostLease(lease, io.subtrees);
  if (settled.ok) {
    for (const line of settled.value) {
      io.stderr.write(`RAM temp volume: host lease ${line}\n`);
    }
  } else {
    io.stderr.write(`${describeRamTempError(settled.error)}\n`);
  }
  await archiveDeletions();
  return 0;
}

function waitForEof(stream: Readable): Promise<void> {
  if (stream.readableEnded || stream.destroyed) {
    return Promise.resolve();
  }
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the provider holds the host lease for the exec host's lifetime until cowshed's supervisor closes stdin on retirement (or exits, closing the pipe).
  return new Promise<void>((resolve) => {
    const done = (): void => {
      stream.off('end', done);
      stream.off('close', done);
      stream.off('error', done);
      resolve();
    };
    stream.on('end', done);
    stream.on('close', done);
    stream.on('error', done);
    stream.resume();
  });
}

function defaultSubtreeOwner(): SubtreeOwner {
  if (process.env[SUPERVISOR_SOCKET_ENV] !== undefined) {
    return cowshedSubtreeOwner(process.env[COWSHED_EXECUTABLE_ENV] ?? 'cowshed');
  }
  return {
    rootArgv() {
      throw new Error('the scratch provider spawns no bounded-exec command');
    },
    async retire() {
      return { kind: 'refused', reason: 'foldUnavailable' };
    },
  };
}

function isMainEntry(): boolean {
  const entry = process.argv[1];
  if (entry === undefined) {
    return false;
  }
  try {
    return realpathSync(entry) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
}

if (isMainEntry()) {
  const cowshed = process.env[COWSHED_EXECUTABLE_ENV] ?? 'cowshed';
  const volume = new RamTempVolume(
    ramTempPaths(userInfo(), PRODUCTION),
    RAM_TEMP_CAPACITY_BYTES,
    createHostCommands(GATEWAY_SOCKET, undefined, undefined, cowshed),
    process.pid,
    undefined,
    'optional',
  );
  const code = await runScratchProvider({
    stdin: process.stdin,
    stdout: process.stdout,
    stderr: process.stderr,
    workspace: process.env.COWSHED_SCRATCH_WORKSPACE,
    subtrees: defaultSubtreeOwner(),
    volume,
  });
  await Promise.all(
    [process.stdout, process.stderr].map((stream) => new Promise<void>((settle) => stream.write('', () => settle()))),
  );
  process.exitCode = code;
}
