import { describe, expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmdirSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { statfs } from 'node:fs/promises';
import { tmpdir, userInfo } from 'node:os';
import { basename, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { OpContextBinding } from '@smoothbricks/lmao';
import { useTestSpan } from '@smoothbricks/lmao/testing/bun';
import { whenChanged } from '../../__tests__/path-change.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { processTable, terminate } from '../../testing.js';
import {
  ARCHIVES_KEPT,
  archiveDeletions,
  type CommandOutput,
  createHostCommands,
  type HostCommands,
  leaseProcessesOn,
  mountsBelow,
  operationDisk,
  pidsInLease,
  type RamDisk,
  type RamTempError,
  type RamTempPaths,
  RamTempVolume,
  ramDisksIn,
} from './ram-temp.js';
import { registryDevice } from './ram-temp-fixtures.js';

const MIB = 1024 * 1024;

/**
 * A volume of its own for one test: a lock beside it, and a name and a size no other test
 * process uses, since the size and the user are what identify a RAM disk as this volume's.
 */
function scratchVolume(index: number): { parent: string; paths: RamTempPaths; capacity: number } {
  const parent = mkdtempSync(join(process.env.TMPDIR ?? '/tmp', 'rt-'));
  const name = `smoo-ram-test-${process.pid}-${index}`;
  return {
    parent,
    paths: {
      mountpoint: join('/Volumes', name),
      lockFile: join(parent, 'v.lock'),
      volumeName: name,
      staging: `/private/tmp/${name}-new`,
      user: userInfo().username,
    },
    capacity: 64 * MIB + (process.pid * 4 + index + 1) * 512,
  };
}

async function attachedDisks(paths: RamTempPaths, capacity: number): Promise<RamDisk[]> {
  const info = await createHostCommands().run('/usr/bin/hdiutil', ['info']);
  if (info.status !== 0) {
    throw new Error(`inventory RAM disks: exit ${info.status}: ${info.stderr}`);
  }
  return ramDisksIn(info.stdout).filter((disk) => disk.user === paths.user && disk.sectors === capacity / 512);
}

async function detachAll(paths: RamTempPaths, capacity: number): Promise<void> {
  const host = createHostCommands();
  for (const disk of await attachedDisks(paths, capacity)) {
    const output = await host.run('/usr/bin/hdiutil', ['detach', '-force', disk.physical]);
    if (output.status !== 0) {
      throw new Error(`hdiutil detach ${disk.physical}: exit ${output.status}: ${output.stderr}`);
    }
  }
}

/** A cowshed sandbox cannot attach images: creation fails at the host instead of leasing. */
function expectSandboxed(error: RamTempError): void {
  expect(error.kind).toBe('provision-failed');
  if (error.kind === 'provision-failed') {
    expect(['hdiutil info', 'hdiutil attach']).toContain(error.step);
  }
}

/**
 * The provisioning step a host call is: `mount_apfs` and `umount` of the private staging mount, or
 * the `diskutil mount` that places the volume. Null for every other call.
 */
function stepOf(file: string, args: readonly string[]): string | null {
  if (file === '/sbin/mount_apfs') {
    return 'mount_apfs';
  }
  if (file === '/sbin/umount') {
    return 'unmount';
  }
  return file === '/usr/sbin/diskutil' && args[0] === 'mount' ? 'mount' : null;
}

/** A readonly tree, two directories deep, with a file at each level. */
function readonlyTree(directory: string): void {
  const tree = join(directory, 'tree');
  const inner = join(tree, 'inner');
  mkdirSync(inner, { recursive: true });
  writeFileSync(join(tree, 'file'), 'in a readonly directory');
  writeFileSync(join(inner, 'deep'), 'in a nested readonly directory');
  chmodSync(inner, 0o500);
  chmodSync(tree, 0o555);
}

/** The directory a lease's link points at: it must keep its mode and contents, never be followed. */
function expectUntouched(outside: string): void {
  expect(statSync(outside).mode & 0o777).toBe(0o555);
  expect(readFileSync(join(outside, 'keep'), 'utf8')).toBe('outside the lease');
}

async function nativeRamVolume(): Promise<void> {
  const { parent, paths, capacity } = scratchVolume(1);
  const trace = useTestSpan<OpContextBinding>();
  const caseStartedAt = Date.now();
  process.stderr.write(`RAM lifecycle case started pid=${process.pid} wallAtMs=${caseStartedAt}\n`);
  const processes = leaseProcessesOn({
    async workingIn(directory) {
      const startedAt = Date.now();
      const starting = JSON.stringify({
        phase: 'lease-census-start',
        directory,
        pid: process.pid,
        wallAtMs: startedAt,
        caseElapsedMs: startedAt - caseStartedAt,
      });
      trace.log.info(starting);
      process.stderr.write(`${starting}\n`);
      try {
        const pids = await pidsInLease(directory);
        const endedAt = Date.now();
        const finished = JSON.stringify({
          phase: 'lease-census-end',
          directory,
          pid: process.pid,
          wallAtMs: endedAt,
          elapsedMs: endedAt - startedAt,
          caseElapsedMs: endedAt - caseStartedAt,
          pids,
        });
        trace.log.info(finished);
        process.stderr.write(`${finished}\n`);
        return pids;
      } catch (error) {
        const endedAt = Date.now();
        const details = typeof error === 'object' && error !== null ? error : {};
        const failed = JSON.stringify(
          {
            phase: 'lease-census-error',
            directory,
            pid: process.pid,
            wallAtMs: endedAt,
            elapsedMs: endedAt - startedAt,
            caseElapsedMs: endedAt - caseStartedAt,
            message: error instanceof Error ? error.message : String(error),
            code: 'code' in details ? details.code : null,
            signal: 'signal' in details ? details.signal : null,
            killed: 'killed' in details ? details.killed : null,
          },
          null,
          2,
        );
        trace.log.info(failed);
        process.stderr.write(`${failed}\n`);
        throw error;
      }
    },
    table: processTable,
    signal: terminate,
  });
  // A readonly directory outside every lease. Each lease links to it: removal must unlink the
  // link and never follow it, leaving this directory's mode and contents as they were.
  const outside = join(parent, 'outside');
  mkdirSync(outside);
  writeFileSync(join(outside, 'keep'), 'outside the lease');
  chmodSync(outside, 0o555);
  // The host's own commands, with one injectable image in the I/O Registry listing. The steps that
  // mount, unmount or place the volume are recorded in the order provisioning runs them.
  const delegate = createHostCommands();
  const host: HostCommands = {
    async run(file, args, deadlineMs) {
      const startedAt = Date.now();
      const starting = JSON.stringify({
        phase: 'native-host-command-start',
        file,
        args,
        deadlineMs: deadlineMs ?? null,
        wallAtMs: startedAt,
        caseElapsedMs: startedAt - caseStartedAt,
      });
      process.stderr.write(`${starting}\n`);
      trace.log.info(starting);
      const delegateStartedAt = Date.now();
      const output = await delegate.run(file, args, deadlineMs);
      const endedAt = Date.now();
      const finished = JSON.stringify({
        phase: 'native-host-command-end',
        file,
        args,
        status: output.status,
        stderr: output.stderr,
        wallAtMs: endedAt,
        elapsedMs: endedAt - startedAt,
        delegateStartedWallAtMs: delegateStartedAt,
        observationBeforeDelegateMs: delegateStartedAt - startedAt,
        delegateElapsedMs: endedAt - delegateStartedAt,
        caseElapsedMs: endedAt - caseStartedAt,
      });
      process.stderr.write(`${finished}\n`);
      trace.log.info(finished);
      return output;
    },
  };
  const attachedBelow: { image: string | null; failInventory: boolean } = { image: null, failInventory: false };
  const provisioning: string[] = [];
  const commands: HostCommands = {
    async run(file, args) {
      const step = stepOf(file, args);
      if (step !== null) {
        provisioning.push(step);
      }
      if (step === 'unmount') {
        // The volume leaves the staging directory only once its marker is in it.
        expect(readFileSync(join(paths.staging, '.smoo-ram'), 'utf8')).toBe(`${paths.volumeName}\n`);
      }
      if (file === '/usr/sbin/ioreg' && attachedBelow.failInventory) {
        attachedBelow.failInventory = false;
        return { status: 1, stdout: '', stderr: 'injected holder inventory failure' };
      }
      const output = await host.run(file, args);
      return file === '/usr/sbin/ioreg' && attachedBelow.image !== null
        ? {
            ...output,
            stdout: [output.stdout, ...registryDevice(`file://${attachedBelow.image}`, 'disk999')].join('\n'),
          }
        : output;
    },
  };
  const alive = new Set([101, 202]);
  const first = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid), processes);
  const second = new RamTempVolume(paths, capacity, commands, 202, (pid) => alive.has(pid), processes);
  const run = async (file: string, args: string[]): Promise<string> => {
    const output = await host.run(file, args);
    if (output.status !== 0) {
      throw new Error(`${file} ${args.join(' ')}: exit ${output.status}: ${output.stderr}`);
    }
    return output.stdout;
  };
  const volume = new RamTempVolume(paths, capacity, host, undefined, undefined, processes);
  try {
    // The debris: a volume mounted at the mountpoint by a creator that died before its marker
    // (the order this module used to create in), and a RAM disk whose deadlined attach finished
    // with nobody left to format it.
    const sectors = `ram://${capacity / 512}`;
    const orphan = await host.run('/usr/bin/hdiutil', ['attach', '-nomount', sectors]);
    if (orphan.status !== 0) {
      const acquired = await volume.acquire();
      expect(acquired.ok).toBe(false);
      if (!acquired.ok) {
        expectSandboxed(acquired.error);
      }
      return;
    }
    const physical = /^(\/dev\/disk\d+)\b/.exec(orphan.stdout)?.[1] ?? '';
    const container = operationDisk(await run('/usr/sbin/diskutil', ['apfs', 'createContainer', physical])) ?? '';
    const orphanVolume =
      operationDisk(
        await run('/usr/sbin/diskutil', ['apfs', 'addVolume', container, 'APFS', paths.volumeName, '-nomount']),
      ) ?? '';
    await run('/usr/sbin/diskutil', ['mount', '-mountOptions', 'nobrowse', orphanVolume]);
    await run('/usr/bin/hdiutil', ['attach', '-nomount', sectors]);
    expect((await attachedDisks(paths, capacity)).map((disk) => disk.mountpoints)).toEqual([[paths.mountpoint], []]);

    // An unmarked volume that carries a lease is someone's TMPDIR: it is named, not detached.
    const lease = join(paths.mountpoint, `${process.pid}-abcdef`);
    mkdirSync(lease);
    const refused = await volume.acquire();
    expect(refused).toEqual({
      ok: false,
      error: {
        kind: 'provision-failed',
        step: 'reclaim',
        detail: `${physical} at ${paths.mountpoint} has no marker but carries leases ${process.pid}-abcdef; not detached`,
      },
    });
    expect(existsSync(lease)).toBe(true);
    rmdirSync(lease);

    const a = await first.acquire();
    if (!a.ok || a.value.kind !== 'leased') {
      throw new Error(`lease over the debris failed: ${JSON.stringify(a)}`);
    }
    expect(a.value.kind).toBe('leased');
    // Mounted and marked at its staging directory, unmounted, then placed by DiskArbitration: never renamed.
    expect(provisioning).toEqual(['mount_apfs', 'unmount', 'mount']);
    expect(a.value.lease.mountpoint).toBe(paths.mountpoint);
    expect(readFileSync(join(paths.mountpoint, '.smoo-ram'), 'utf8')).toBe(`${paths.volumeName}\n`);
    expect(existsSync(`${paths.mountpoint} 1`)).toBe(false);
    expect((await attachedDisks(paths, capacity)).map((disk) => disk.mountpoints)).toEqual([[paths.mountpoint]]);

    const leaseA = a.value.lease;
    const b = await second.acquire();
    if (!b.ok || b.value.kind !== 'leased') {
      throw new Error(`second lease failed: ${JSON.stringify(b)}`);
    }
    const leaseB = b.value.lease;
    readonlyTree(leaseB.directory);
    symlinkSync(outside, join(leaseB.directory, 'outside-link'));
    expect(leaseA.mountpoint).toBe(paths.mountpoint);
    expect(leaseB.mountpoint).toBe(paths.mountpoint);
    expect(leaseA.directory).not.toBe(leaseB.directory);
    expect(statSync(leaseA.directory).dev).not.toBe(statSync(parent).dev);
    writeFileSync(join(leaseA.directory, 'scratch'), 'on RAM');
    readonlyTree(leaseA.directory);
    symlinkSync(outside, join(leaseA.directory, 'outside-link'));

    // A passing run's lease is deleted, not archived.
    expect(await first.release(leaseA, false)).toEqual({ ok: true, value: [] });
    expect(existsSync(leaseA.directory)).toBe(false);
    expectUntouched(outside);
    expect(statSync(leaseB.directory).dev).not.toBe(statSync(parent).dev);

    // The second task dies without releasing, with an image still attached from its lease: the
    // registry entry is injected, so the test spends no attach of its own. The next task keeps
    // the lease and names it instead of detaching another owner's image.
    alive.delete(202);
    const image = join(leaseB.directory, 'store', 'main.dmg');
    attachedBelow.image = image;
    const third = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid), processes);
    // A failed acquire must not leave its new live-pid lease or reclaim claim behind. Reuse
    // this mounted volume: no extra attach is needed to cover the failed-publication path.
    attachedBelow.failInventory = true;
    const failedAcquire = await third.acquire();
    expect(failedAcquire).toMatchObject({
      ok: false,
      error: { kind: 'provision-failed', step: 'ioreg (attached disk images)' },
    });
    expect(JSON.stringify(failedAcquire)).toContain('injected holder inventory failure');
    expect(readdirSync(paths.mountpoint).filter((name) => /^\d+-[A-Za-z0-9]{6}$/.test(name))).toEqual([
      leaseB.directory.slice(paths.mountpoint.length + 1),
    ]);
    expect(JSON.parse(readFileSync(`${paths.lockFile}.ledger`, 'utf8'))).toMatchObject({
      lifecycle: null,
      claims: [],
    });
    const c = await third.acquire();
    if (!c.ok || c.value.kind !== 'leased') {
      throw new Error(`third lease failed: ${JSON.stringify(c)}`);
    }
    expect(c.value.held).toEqual([
      `${leaseB.directory} is kept: ${image} (/dev/disk999) still attached from below it; detach through its owner`,
    ]);
    expect(existsSync(leaseB.directory)).toBe(true);
    // Held from below: the lease stays whole, its readonly tree and the link with it.
    expect(statSync(join(leaseB.directory, 'tree')).mode & 0o777).toBe(0o555);
    expect(statSync(join(leaseB.directory, 'tree', 'inner')).mode & 0o777).toBe(0o500);
    expect(existsSync(join(leaseB.directory, 'tree', 'inner', 'deep'))).toBe(true);
    expectUntouched(outside);
    expect(await third.release(c.value.lease, false)).toEqual({ ok: true, value: [] });
    expect(existsSync(c.value.lease.directory)).toBe(false);
    expect(statSync(leaseB.directory).dev).not.toBe(statSync(parent).dev);
    expect(statSync(join(leaseB.directory, 'tree')).mode & 0o777).toBe(0o555);

    // Once its owner has detached the image, the next task reclaims the lease, and its release,
    // the last, detaches the volume. The dead task left a writer behind, as a test's ttsc plugin
    // build is left: working elsewhere, writing into the lease through its TMPDIR, with a child.
    // The pid the lease is named after is dead; the writer is stopped before the lease is reclaimed,
    // so its archive never gains a file after the lease was judged dead. A second dead lease, that
    // nothing works in, is reclaimed beside it. Both are dead tasks' scratch, so both are archived.
    attachedBelow.image = null;
    const writer = spawn('sh', ['-c', 'sleep 600 & echo $!; i=0; while :; do i=$((i+1)); : > "$TMPDIR/w$i"; done'], {
      cwd: parent,
      env: { ...process.env, TMPDIR: leaseB.directory },
      detached: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const writerExited = once(writer, 'close');
    const writerChild = Number(String((await once(writer.stdout, 'data'))[0]).trim());
    const quiet = join(paths.mountpoint, '303-quiet1');
    mkdirSync(quiet);
    writeFileSync(join(quiet, 'left'), 'by a task nothing of which still runs');
    try {
      // Ends on the watch event that creates the writer's `w1` in the lease, not on a poll.
      await whenChanged(leaseB.directory, () => existsSync(join(leaseB.directory, 'w1')));
      const fourth = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid), processes);
      const d = await fourth.acquire();
      if (!d.ok || d.value.kind !== 'leased') {
        throw new Error(`fourth lease failed: ${JSON.stringify(d)}`);
      }
      expect(d.value.held).toEqual([]);
      expect(d.value.reaped.map((entry) => Number(entry.split(' ')[0])).sort((a, b) => a - b)).toEqual(
        [writer.pid ?? 0, writerChild].sort((a, b) => a - b),
      );
      expect(existsSync(leaseB.directory)).toBe(false);
      expect(existsSync(quiet)).toBe(false);
      expectUntouched(outside);

      // Each dead lease's scratch was renamed whole into the archive on the volume: its files, and its readonly tree
      // with its modes, are there to read.
      const archive = join(paths.mountpoint, '.archive');
      const archives = (): string[] =>
        readdirSync(archive)
          .filter((name) => name !== '.deleting')
          .sort();
      const archiveOf = (lease: string): string => {
        const name = archives().find((candidate) => candidate.endsWith(`-${basename(lease)}`));
        if (name === undefined) throw new Error(`no archive of ${lease} in ${archives().join(', ')}`);
        return name;
      };
      const archivedB = archiveOf(leaseB.directory);
      const archivedQuiet = archiveOf(quiet);
      expect(existsSync(join(archive, archivedB, 'w1'))).toBe(true);
      expect(statSync(join(archive, archivedB, 'tree')).mode & 0o777).toBe(0o555);
      expect(readFileSync(join(archive, archivedQuiet, 'left'), 'utf8')).toBe('by a task nothing of which still runs');
      expect(d.value.retained.map((line) => line.replace(/\d+ MiB/, 'N MiB')).sort()).toEqual(
        [
          [leaseB.directory, archivedB],
          [quiet, archivedQuiet],
        ]
          .map(
            ([lease, name]) =>
              `${lease} archived at ${join(archive, name)}: kept until ${ARCHIVES_KEPT} newer archives replace it, less than N MiB of the volume is free when one arrives, or the volume detaches with its last lease`,
          )
          .sort(),
      );

      // A third dead lease's archive rotates the oldest out at once, by rename; its deletion runs in the background.
      const oldest = [archivedB, archivedQuiet].sort()[0];
      const later = join(paths.mountpoint, '505-later1');
      mkdirSync(later);
      writeFileSync(join(later, 'left'), 'by a task that died after the others');
      const fifth = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid), processes);
      const e = await fifth.acquire();
      if (!e.ok || e.value.kind !== 'leased') {
        throw new Error(`fifth lease failed: ${JSON.stringify(e)}`);
      }
      expect(archives()).toHaveLength(ARCHIVES_KEPT);
      expect(archives()).not.toContain(oldest);
      expect(readFileSync(join(archive, archiveOf(later), 'left'), 'utf8')).toBe(
        'by a task that died after the others',
      );
      await archiveDeletions();
      expect(readdirSync(join(archive, '.deleting'))).toEqual([]);

      // A failed run's own scratch is archived too, unless less than half the volume is free when it arrives: then the
      // volume keeps no archive at all, since live leases need the space more than evidence does.
      const volumeNow = await statfs(paths.mountpoint);
      const fill = volumeNow.bavail * volumeNow.bsize - Math.floor((volumeNow.blocks * volumeNow.bsize) / 2) + MIB;
      writeFileSync(join(e.value.lease.directory, 'fill'), Buffer.alloc(fill));
      const crowded = await fifth.release(e.value.lease, true);
      expect(crowded.ok ? crowded.value.map((line) => line.replace(/\d+ MiB/g, 'N MiB')) : crowded).toEqual([
        `${e.value.lease.directory} deleted: the volume has N MiB free, below the N MiB under which it keeps no archive`,
      ]);
      expect(archives()).toEqual([]);
      await archiveDeletions();
      expect(readdirSync(join(archive, '.deleting'))).toEqual([]);
      expect(await fourth.release(d.value.lease, false)).toEqual({ ok: true, value: [] });
    } finally {
      terminate(writer.pid ?? 0, 'SIGKILL');
      terminate(writerChild, 'SIGKILL');
      await writerExited;
    }
    expect(existsSync(paths.mountpoint)).toBe(false);
    expect(await attachedDisks(paths, capacity)).toEqual([]);
  } finally {
    // Restore the owner write the outside directory was given, so the parent can be removed.
    chmodSync(outside, 0o755);
    await detachAll(paths, capacity);
    rmSync(parent, { recursive: true, force: true });
  }
}

/** The provisioning steps a test fails in turn, each with the step name its failure must report. */
const FAILING_STEPS = [
  { injected: 'mount_apfs', reported: 'mount_apfs (staging)' },
  { injected: 'unmount', reported: 'umount (staging)' },
  { injected: 'mount', reported: 'diskutil mount' },
] as const;

/**
 * A provisioning step that fails leaves nothing under the final name: the failure detaches the whole
 * disk, with the staging mount on it, and removes the staging directory the attempt made.
 */
async function aFailedStepLeavesNoNamedVolume(): Promise<void> {
  const trace = useTestSpan<OpContextBinding>();
  process.stderr.write(`RAM provisioning case started in ${process.pid}\n`);
  for (const { injected, reported } of FAILING_STEPS) {
    const { parent, paths, capacity } = scratchVolume(2);
    const host = createHostCommands();
    const calls: { file: string; args: readonly string[]; output: CommandOutput }[] = [];
    const commands: HostCommands = {
      async run(file, args, deadlineMs) {
        const record =
          stepOf(file, args) !== null ||
          (file === '/usr/bin/hdiutil' && ['attach', 'detach'].includes(args[0] ?? '')) ||
          (file === '/usr/sbin/diskutil' && args[0] === 'apfs');
        const startedAt = Date.now();
        const starting = JSON.stringify({ injected, phase: 'command-start', file, args });
        trace.log.info(starting);
        process.stderr.write(`${starting}\n`);
        const output =
          stepOf(file, args) === injected
            ? { status: 1, stdout: '', stderr: 'injected' }
            : await host.run(file, args, deadlineMs);
        if (record) {
          calls.push({ file, args, output });
        }
        const finished = JSON.stringify({
          injected,
          phase: 'command-end',
          file,
          args,
          output: record
            ? output
            : { status: output.status, stderr: output.stderr, stdoutBytes: Buffer.byteLength(output.stdout) },
          elapsedMs: Date.now() - startedAt,
        });
        trace.log.info(finished);
        process.stderr.write(`${finished}\n`);
        return output;
      },
    };
    try {
      const refused = await new RamTempVolume(paths, capacity, commands).acquire();
      const refusal = JSON.stringify({ injected, refused }, null, 2);
      trace.log.info(refusal);
      process.stderr.write(`${refusal}\n`);
      const failedDetach = calls.find(
        (call) => call.file === '/usr/bin/hdiutil' && call.args[0] === 'detach' && call.output.status !== 0,
      );
      if (failedDetach !== undefined) {
        // Diagnostic probes explain a failed detach: each waits for the host to answer, bounded by the leg backstop.
        const mountTable = await host.run('/sbin/mount', [], BOUNDED_TEST_IDLE_TIMEOUT_MS);
        const filesystemHolders = await host.run(
          '/usr/bin/sudo',
          ['-n', '/usr/sbin/lsof', '-nP', '+f', '--', paths.staging],
          BOUNDED_TEST_IDLE_TIMEOUT_MS,
        );
        const added = calls.find(
          (call) => call.file === '/usr/sbin/diskutil' && call.args[0] === 'apfs' && call.args[1] === 'addVolume',
        );
        const volume = added === undefined ? null : operationDisk(added.output.stdout);
        const devices = failedDetach.args.filter((argument) => argument.startsWith('/dev/disk'));
        if (volume !== null) devices.push(`/dev/${volume}`);
        if (devices.length === 0) throw new Error('rollback detach recorded no device for its diagnostic probe');
        const devicePaths: string[] = [];
        for (const device of devices) devicePaths.push(device, device.replace('/dev/', '/dev/r'));
        const deviceHolders = await host.run(
          '/usr/bin/sudo',
          ['-n', '/usr/sbin/lsof', '-nP', '--', ...devicePaths],
          BOUNDED_TEST_IDLE_TIMEOUT_MS,
        );
        const observations = JSON.stringify(
          {
            injected,
            phase: 'rollback-refused-observations',
            mountTable: {
              status: mountTable.status,
              stderr: mountTable.stderr,
              matching: mountTable.stdout.split('\n').filter((line) => line.includes(paths.staging)),
            },
            filesystemHolders,
            devicePaths,
            deviceHolders,
          },
          null,
          2,
        );
        trace.log.info(observations);
        process.stderr.write(`${observations}\n`);
      }
      // A cowshed sandbox cannot attach images, so provisioning stops before the injected step.
      if (!refused.ok && refused.error.kind === 'provision-failed' && refused.error.step !== reported) {
        expectSandboxed(refused.error);
        continue;
      }
      const detail = JSON.stringify({ injected, refused, calls }, null, 2);
      expect(refused, detail).toEqual({
        ok: false,
        error: { kind: 'provision-failed', step: reported, detail: 'exit 1: injected' },
      });
      expect(
        calls
          .filter((call) => call.file === '/usr/bin/hdiutil' && call.args[0] === 'detach')
          .map((call) => call.output.status),
        detail,
      ).toEqual([0]);
      const listed = await host.run('/usr/sbin/diskutil', ['list']);
      const remaining = listed.stdout.split('\n').filter((line) => line.includes(paths.volumeName));
      const inventory = JSON.stringify({ injected, inventory: { status: listed.status, remaining } }, null, 2);
      trace.log.info(inventory);
      process.stderr.write(`${inventory}\n`);
      expect(listed.status).toBe(0);
      expect(remaining, detail).toEqual([]);
      const table = await host.run('/sbin/mount', []);
      expect(table.status).toBe(0);
      expect(mountsBelow(table.stdout, paths.mountpoint)).toEqual([]);
      expect(mountsBelow(table.stdout, paths.staging)).toEqual([]);
      expect(existsSync(paths.staging)).toBe(false);
    } finally {
      trace.log.info(JSON.stringify({ injected, phase: 'test-teardown-start' }));
      await detachAll(paths, capacity);
      rmSync(parent, { recursive: true, force: true });
      trace.log.info(JSON.stringify({ injected, phase: 'test-teardown-done' }));
    }
  }
}

/** The non-Darwin executor owns no RAM disk: inherited and target-supplied temp directories stay with their owner. */
async function inheritedTempDirectory(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), 'rt-policy-'));
  const inherited = join(root, 'inherited');
  const supplied = join(root, 'supplied');
  const executor = fileURLToPath(new URL('../../../dist/executors/bounded-exec/executor.js', import.meta.url));
  try {
    for (const directory of [inherited, supplied]) {
      mkdirSync(directory);
      writeFileSync(join(directory, 'owned'), 'not a RAM lease');
    }
    const probe = join(root, 'probe.mjs');
    writeFileSync(
      probe,
      [
        "import { writeFileSync } from 'node:fs';",
        "import { tmpdir } from 'node:os';",
        "import { join } from 'node:path';",
        "writeFileSync(join(tmpdir(), 'proof'), process.env.TMPDIR);",
        '',
      ].join('\n'),
    );
    for (const directory of [inherited, supplied]) {
      const options = {
        command: `node ${JSON.stringify(probe)}`,
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        parallelism: 1,
        env: {
          // If the default runner incorrectly provisions an APFS volume here, it must fail,
          // not silently use this host's cowshed or disk tools.
          COWSHED_EXECUTABLE: join(root, 'no-disk-tools'),
          ...(directory === supplied ? { TMPDIR: supplied } : {}),
        },
      };
      const script = [
        `import { runDefaultBoundedExec } from ${JSON.stringify(executor)};`,
        `const result = await runDefaultBoundedExec(${JSON.stringify(options)}, ${JSON.stringify({ root })});`,
        'console.log(JSON.stringify(result));',
        'process.exitCode = result.success ? 0 : 1;',
      ].join('\n');
      const child = Bun.spawn(['node', '--input-type=module', '-e', script], {
        env: { ...process.env, TMPDIR: inherited },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
      ]);
      expect(exitCode, stdout + stderr).toBe(0);
      expect(readFileSync(join(directory, 'proof'), 'utf8')).toBe(directory);
      for (const owned of [inherited, supplied]) {
        expect(readFileSync(join(owned, 'owned'), 'utf8')).toBe('not a RAM lease');
      }
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

// Both platforms execute a host proof. APFS leasing is Darwin's contract; other hosts keep
// their inherited TMPDIR. Running Darwin's fixture there would test a feature the runner never uses.
describe('temporary-directory policy on this host', () => {
  if (process.platform === 'darwin') {
    it(
      'reclaims what a dead creator left, shares the volume between leases, and detaches it at the last release, never mounting beside it as "<name> 1"',
      nativeRamVolume,
    );
    it(
      'leaves no volume under its final name, and no staging directory, when any provisioning step fails',
      aFailedStepLeavesNoNamedVolume,
    );
  } else {
    it(
      'keeps inherited temporary storage, honors a target override, and never provisions an APFS disk',
      inheritedTempDirectory,
    );
  }
});
