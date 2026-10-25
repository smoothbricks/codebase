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
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { OpContextBinding } from '@smoothbricks/lmao';
import { useTestSpan } from '@smoothbricks/lmao/testing/bun';
import { whenChanged } from '../../__tests__/path-change.js';
import { standInCowshed } from '../../__tests__/stand-in-cowshed.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { pidsWorkingIn, terminate } from '../../testing.js';
import { cowshedSubtreeOwner, type SubtreeOwner, type SubtreeRetirement } from './owned-subtree.js';
import {
  ARCHIVES_KEPT,
  archiveDeletions,
  type CommandOutput,
  createHostCommands,
  type HostCommands,
  mountsBelow,
  operationDisk,
  type RamTempError,
  RamTempVolume,
} from './ram-temp.js';
import {
  attachedDisks,
  detachOwned,
  fixtureRamVolume,
  ownsRecordedDisk,
  recordedRamAttach,
  registryDevice,
  removeScratchVolume,
  scratchVolume,
  startFixtureGateway,
  stepOf,
} from './ram-temp-fixtures.js';

const MIB = 1024 * 1024;

/** The owner of a case that only provisions: it creates the volume, so it finds no dead lease to settle. */
const provisionOnly: SubtreeOwner = {
  rootArgv() {
    throw new Error('a provisioning case spawns no command');
  },
  async retire() {
    throw new Error('a provisioning case finds no dead lease to settle');
  },
};

/** A cowshed sandbox cannot attach images: creation fails at the host instead of leasing. */
function expectSandboxed(error: RamTempError): void {
  expect(error.kind).toBe('provision-failed');
  if (error.kind === 'provision-failed') {
    expect(['ioreg (RAM disks)', 'ram-lease']).toContain(error.step);
  }
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
  const fixture = scratchVolume(1);
  const { paths, capacity, owned, gatewaySocket } = fixture;
  const trace = useTestSpan<OpContextBinding>();
  const caseStartedAt = Date.now();
  process.stderr.write(`RAM lifecycle case started pid=${process.pid} wallAtMs=${caseStartedAt}\n`);
  // The supervisor every lease settles through: the stand-in cowshed's subtree verbs, whose tree is a process group
  // (`__tests__/stand-in-cowshed.ts`). A handle this test marks foreign is a live job of another task's, answered by
  // `foreignAnswers` in turn, as the supervisor answers a caller that is not that job's member.
  const standIn = cowshedSubtreeOwner(
    await standInCowshed(join(paths.root, 'supervisor'), { admission: 'admitted', retire: 'retire' }),
  );
  const foreignAnswers: SubtreeRetirement[] = [];
  const subtrees: SubtreeOwner = {
    rootArgv: (command) => standIn.rootArgv(command),
    async retire(handle, graceMs) {
      if (handle.foreign !== true) {
        return standIn.retire(handle, graceMs);
      }
      const answer = foreignAnswers.shift();
      if (answer === undefined) {
        throw new Error('a foreign handle was retired more often than this test answers for it');
      }
      return answer;
    },
  };
  // A readonly directory outside every lease. Each lease links to it: removal must unlink the
  // link and never follow it, leaving this directory's mode and contents as they were.
  const outside = join(paths.root, 'outside');
  mkdirSync(outside);
  writeFileSync(join(outside, 'keep'), 'outside the lease');
  chmodSync(outside, 0o555);
  // The host's own commands, with one injectable image in the I/O Registry listing. The steps that
  // mount, unmount or place the volume are recorded in the order provisioning runs them.
  const delegate = createHostCommands(gatewaySocket);
  const host: HostCommands = {
    detachRam: delegate.detachRam,
    ownsRamDisk: (disk) => ownsRecordedDisk(owned, disk),
    async run(file, args, deadlineMs, binding) {
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
      const output = await delegate.run(file, args, deadlineMs, binding);
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
    // Only the fixture gateway attaches this disk; no operation asks the user's gateway.
    attachRam: recordedRamAttach(
      delegate.attachRam,
      (file, args, deadlineMs, binding) => host.run(file, args, deadlineMs, binding),
      owned,
    ),
  };
  const attachedBelow: { image: string | null; failInventory: boolean } = { image: null, failInventory: false };
  const provisioning: string[] = [];
  const commands: HostCommands = {
    async run(file, args, deadlineMs, binding) {
      const step = stepOf(paths, file, args);
      if (step !== null) {
        provisioning.push(step);
      }
      if (step === 'unmount') {
        // The volume leaves the staging directory only once its marker is in it.
        expect(JSON.parse(readFileSync(join(paths.staging, '.smoo-ram'), 'utf8'))).toMatchObject({
          version: 1,
          volumeName: paths.volumeName,
          owner: 'gateway',
        });
      }
      if (file === '/usr/sbin/ioreg' && args.includes('AppleDiskImageDevice') && attachedBelow.failInventory) {
        attachedBelow.failInventory = false;
        return { status: 1, stdout: '', stderr: 'injected holder inventory failure' };
      }
      const output = await host.run(file, args, deadlineMs, binding);
      return file === '/usr/sbin/ioreg' && args.includes('AppleDiskImageDevice') && attachedBelow.image !== null
        ? {
            ...output,
            stdout: [output.stdout, ...registryDevice(`file://${attachedBelow.image}`, 'disk999')].join('\n'),
          }
        : output;
    },
    attachRam: (sectors) => host.attachRam(sectors),
    detachRam: host.detachRam,
    ownsRamDisk: host.ownsRamDisk,
  };
  const alive = new Set([101, 202]);
  const first = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
  const second = new RamTempVolume(paths, capacity, commands, 202, (pid) => alive.has(pid));
  const run = async (file: string, args: string[]): Promise<string> => {
    const output = await host.run(file, args);
    if (output.status !== 0) {
      throw new Error(`${file} ${args.join(' ')}: exit ${output.status}: ${output.stderr}`);
    }
    return output.stdout;
  };
  const stopGateway = await startFixtureGateway(fixture);
  const volume = new RamTempVolume(paths, capacity, host);
  try {
    // The debris: a volume mounted at the mountpoint by a creator that died before its marker
    // (the order this module used to create in), and a RAM disk whose deadlined attach finished
    // with nobody left to format it (what an attach left before the gateway held it under a lease).
    // Both are the test's own attaches, kept from their leases so they persist, and recorded by
    // registry identity for its teardown.
    const orphan = await host.attachRam(capacity / 512);
    if (!orphan.ok) {
      const acquired = await volume.acquire(subtrees);
      expect(acquired.ok).toBe(false);
      if (!acquired.ok) {
        expectSandboxed(acquired.error);
      }
      return;
    }
    const orphanKept = await orphan.value.keep();
    if (!orphanKept.ok) {
      throw new Error(`the orphan debris: ${orphanKept.error}`);
    }
    const physical = orphan.value.device;
    const container = operationDisk(await run('/usr/sbin/diskutil', ['apfs', 'createContainer', physical])) ?? '';
    const orphanVolume =
      operationDisk(
        await run('/usr/sbin/diskutil', ['apfs', 'addVolume', container, 'APFS', paths.volumeName, '-nomount']),
      ) ?? '';
    mkdirSync(paths.mountpoint, { mode: 0o700 });
    await run('/sbin/mount_apfs', ['-o', 'nobrowse,noowners', `/dev/${orphanVolume}`, paths.mountpoint]);
    const unformatted = await host.attachRam(capacity / 512);
    if (!unformatted.ok) {
      throw new Error(`the unformatted debris: ${unformatted.error}`);
    }
    const unformattedKept = await unformatted.value.keep();
    if (!unformattedKept.ok) {
      throw new Error(`the unformatted debris: ${unformattedKept.error}`);
    }
    expect((await attachedDisks(paths, capacity, gatewaySocket)).map((disk) => disk.mountpoints)).toEqual([
      [paths.mountpoint],
      [],
    ]);

    // An unmarked volume that carries a lease is someone's TMPDIR: it is named, not detached.
    const lease = join(paths.mountpoint, `${process.pid}-abcdef`);
    mkdirSync(lease);
    const refused = await volume.acquire(subtrees);
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

    const a = await first.acquire(subtrees);
    if (!a.ok || a.value.kind !== 'leased') {
      throw new Error(`lease over the debris failed: ${JSON.stringify(a)}`);
    }
    expect(a.value.kind).toBe('leased');
    // Mounted and marked at its staging directory, unmounted, then mounted at its mountpoint: never renamed.
    expect(provisioning).toEqual(['mount_apfs', 'unmount', 'mount']);
    expect(a.value.lease.mountpoint).toBe(paths.mountpoint);
    const markerPath = join(paths.mountpoint, '.smoo-ram');
    const originalMarker = readFileSync(markerPath, 'utf8');
    writeFileSync(
      markerPath,
      `${JSON.stringify({ version: 1, volumeName: paths.volumeName, owner: 'gateway', registryEntry: '0x1' })}\n`,
    );
    try {
      const mismatch = await new RamTempVolume(paths, capacity, commands).acquire(subtrees);
      expect(mismatch.ok).toBe(false);
      expect(JSON.stringify(mismatch)).toContain('no longer names this mounted volume');
      expect(existsSync(a.value.lease.directory)).toBe(true);
    } finally {
      writeFileSync(markerPath, originalMarker);
    }
    expect(JSON.parse(readFileSync(join(paths.mountpoint, '.smoo-ram'), 'utf8'))).toMatchObject({
      version: 1,
      volumeName: paths.volumeName,
      owner: 'gateway',
    });
    expect(existsSync(paths.staging)).toBe(false);
    expect((await attachedDisks(paths, capacity, gatewaySocket)).map((disk) => disk.mountpoints)).toEqual([
      [paths.mountpoint],
    ]);

    const leaseA = a.value.lease;
    const b = await second.acquire(subtrees);
    if (!b.ok || b.value.kind !== 'leased') {
      throw new Error(`second lease failed: ${JSON.stringify(b)}`);
    }
    const leaseB = b.value.lease;
    readonlyTree(leaseB.directory);
    symlinkSync(outside, join(leaseB.directory, 'outside-link'));
    expect(leaseA.mountpoint).toBe(paths.mountpoint);
    expect(leaseB.mountpoint).toBe(paths.mountpoint);
    expect(leaseA.directory).not.toBe(leaseB.directory);
    expect(statSync(leaseA.directory).dev).not.toBe(statSync(paths.root).dev);
    writeFileSync(join(leaseA.directory, 'scratch'), 'on RAM');
    readonlyTree(leaseA.directory);
    symlinkSync(outside, join(leaseA.directory, 'outside-link'));

    // A passing run's lease is deleted, not archived.
    expect(await first.release(leaseA, false, subtrees)).toEqual({ ok: true, value: [] });
    expect(existsSync(leaseA.directory)).toBe(false);
    expectUntouched(outside);
    expect(statSync(leaseB.directory).dev).not.toBe(statSync(paths.root).dev);

    // The second task dies without releasing, with an image still attached from its lease: the
    // registry entry is injected, so the test spends no attach of its own. The next task keeps
    // the lease and names it instead of detaching another owner's image.
    alive.delete(202);
    const image = join(leaseB.directory, 'store', 'main.dmg');
    attachedBelow.image = image;
    const third = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
    // A failed acquire must not leave its new live-pid lease or reclaim claim behind. Reuse
    // this mounted volume: no extra attach is needed to cover the failed-publication path.
    attachedBelow.failInventory = true;
    const failedAcquire = await third.acquire(subtrees);
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
    const c = await third.acquire(subtrees);
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
    expect(await third.release(c.value.lease, false, subtrees)).toEqual({ ok: true, value: [] });
    expect(existsSync(c.value.lease.directory)).toBe(false);
    expect(statSync(leaseB.directory).dev).not.toBe(statSync(paths.root).dev);
    expect(statSync(join(leaseB.directory, 'tree')).mode & 0o777).toBe(0o555);

    // Once its owner has detached the image, the next task reclaims the lease, and its release,
    // the last, detaches the volume. The dead task left a writer behind, as a test's ttsc plugin
    // build is left: working elsewhere, writing into the lease through its TMPDIR, with a child.
    // The task recorded its supervisor's handle on that subtree beside the lease before it died, so
    // the supervisor ends the writer and its child before the lease is reclaimed, and its archive
    // never gains a file after the lease was judged dead. A bystander that only works in the lease
    // is in no such subtree: the census that used to decide names it, and nothing signals it now.
    // A second dead lease records no handle, and nothing of its task still runs. Both are dead
    // tasks' scratch, so both are archived. A third records a handle of a job that is still another
    // task's: it is kept until that job ends.
    // The writer rewrites one file rather than making new ones: archived scratch stays on the volume, and a new file
    // per iteration filled this 64 MiB volume's metadata before the next acquire could make its lease (ENOSPC).
    attachedBelow.image = null;
    const writer = spawn('sh', ['-c', 'sleep 600 & echo $!; while :; do : > "$TMPDIR/w1"; done'], {
      cwd: paths.root,
      env: { ...process.env, TMPDIR: leaseB.directory },
      detached: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const writerExited = once(writer, 'close');
    const writerChild = Number(String((await once(writer.stdout, 'data'))[0]).trim());
    const bystander = spawn('sleep', ['600'], { cwd: leaseB.directory, detached: true, stdio: 'ignore' });
    const bystanderExited = once(bystander, 'exit');
    expect(
      await second.record(leaseB, {
        jobId: 1,
        index: writer.pid ?? 0,
        incarnation: '0123456789abcdef0123456789abcdef',
      }),
    ).toEqual({ ok: true, value: undefined });
    const quiet = join(paths.mountpoint, '303-quiet1');
    mkdirSync(quiet);
    writeFileSync(join(quiet, 'left'), 'by a task nothing of which still runs');
    const foreign = join(paths.mountpoint, '404-forgn1');
    mkdirSync(foreign);
    writeFileSync(`${foreign}.subtree`, `${JSON.stringify({ foreign: true })}\n`);
    foreignAnswers.push(
      { kind: 'refused', reason: 'foreignHandle' },
      { kind: 'refused', reason: 'foreignHandle' },
      { kind: 'refused', reason: 'jobEnded' },
    );
    try {
      // Ends on the watch event that creates the writer's `w1` in the lease, not on a poll.
      await whenChanged(leaseB.directory, () => existsSync(join(leaseB.directory, 'w1')));
      expect(await pidsWorkingIn(leaseB.directory)).toContain(bystander.pid ?? 0);
      const fourth = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
      const d = await fourth.acquire(subtrees);
      if (!d.ok || d.value.kind !== 'leased') {
        throw new Error(`fourth lease failed: ${JSON.stringify(d)}`);
      }
      expect(d.value.held).toEqual([
        `${foreign} is kept: its subtree is not this task's to retire (supervisor: foreignHandle); it goes once that subtree's job ends`,
      ]);
      expect(d.value.reaped.map((entry) => Number(entry.split(' ')[0])).sort((a, b) => a - b)).toEqual(
        [writer.pid ?? 0, writerChild].sort((a, b) => a - b),
      );
      expect(d.value.gaps).toEqual([]);
      expect(bystander.exitCode).toBeNull();
      expect(bystander.signalCode).toBeNull();
      expect(existsSync(leaseB.directory)).toBe(false);
      expect(existsSync(`${leaseB.directory}.subtree`)).toBe(false);
      expect(existsSync(quiet)).toBe(false);
      expect(existsSync(foreign)).toBe(true);
      expectUntouched(outside);
      // The bystander moved with the lease's directory into the archive; on the volume it would hold the volume from
      // detaching, so it is this test's to end, as it is its owner's on a real host.
      bystander.kill('SIGKILL');
      await bystanderExited;

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

      // A later dead lease's archive rotates the oldest out at once, by rename; its deletion runs in the background.
      // The foreign job is still live, so its lease is kept again.
      const oldest = [archivedB, archivedQuiet].sort()[0];
      const later = join(paths.mountpoint, '505-later1');
      mkdirSync(later);
      writeFileSync(join(later, 'left'), 'by a task that died after the others');
      const fifth = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
      const e = await fifth.acquire(subtrees);
      if (!e.ok || e.value.kind !== 'leased') {
        throw new Error(`fifth lease failed: ${JSON.stringify(e)}`);
      }
      expect(archives()).toHaveLength(ARCHIVES_KEPT);
      expect(archives()).not.toContain(oldest);
      expect(readFileSync(join(archive, archiveOf(later), 'left'), 'utf8')).toBe(
        'by a task that died after the others',
      );
      expect(existsSync(foreign)).toBe(true);
      await archiveDeletions();
      expect(readdirSync(join(archive, '.deleting'))).toEqual([]);

      // A failed run's own scratch is archived too, unless less than half the volume is free when it arrives: then the
      // volume keeps no archive at all, since live leases need the space more than evidence does. The foreign job has
      // ended by this pass, so its lease is reclaimed in it, and deleted with the rest.
      const volumeNow = await statfs(paths.mountpoint);
      const fill = volumeNow.bavail * volumeNow.bsize - Math.floor((volumeNow.blocks * volumeNow.bsize) / 2) + MIB;
      writeFileSync(join(e.value.lease.directory, 'fill'), Buffer.alloc(fill));
      const crowded = await fifth.release(e.value.lease, true, subtrees);
      expect(crowded.ok ? crowded.value.map((line) => line.replace(/\d+ MiB/g, 'N MiB')).sort() : crowded).toEqual(
        [e.value.lease.directory, foreign]
          .map(
            (lease) => `${lease} deleted: the volume has N MiB free, below the N MiB under which it keeps no archive`,
          )
          .sort(),
      );
      expect(archives()).toEqual([]);
      expect(existsSync(foreign)).toBe(false);
      await archiveDeletions();
      expect(readdirSync(join(archive, '.deleting'))).toEqual([]);
      expect(await fourth.release(d.value.lease, false, subtrees)).toEqual({ ok: true, value: [] });
    } finally {
      terminate(writer.pid ?? 0, 'SIGKILL');
      terminate(writerChild, 'SIGKILL');
      bystander.kill('SIGKILL');
      await Promise.all([writerExited, bystanderExited]);
    }
    expect(existsSync(paths.mountpoint)).toBe(false);
    expect(await attachedDisks(paths, capacity, gatewaySocket)).toEqual([]);
  } finally {
    // Restore the owner write the outside directory was given, so the root can be removed.
    chmodSync(outside, 0o755);
    try {
      await detachOwned(owned, paths.uid, gatewaySocket);
    } finally {
      await stopGateway();
    }
    removeScratchVolume(fixture);
  }
}

/**
 * The failing step the one real APFS cycle runs. The staging volume is still mounted, with its marker, when the
 * rollback detaches it: the kernel unwinds a mounted volume, and only a real disk shows what it leaves.
 */
const REAL_FAILING_STEP = { injected: 'unmount', reported: 'umount (staging)' } as const;

/**
 * A provisioning step that fails leaves nothing under the final name: the failure detaches the whole
 * disk, with the staging mount on it, and removes the staging directory and the mountpoint the attempt made.
 * This is the real disk's observation of that rule, for the one step REAL_FAILING_STEP names.
 */
async function aFailedStepLeavesNoNamedVolume(): Promise<void> {
  const trace = useTestSpan<OpContextBinding>();
  process.stderr.write(`RAM provisioning case started in ${process.pid}\n`);
  const { injected, reported } = REAL_FAILING_STEP;
  const fixture = scratchVolume(2);
  const { paths, capacity, owned, gatewaySocket } = fixture;
  const host = createHostCommands(gatewaySocket);
  const calls: { file: string; args: readonly string[]; output: CommandOutput }[] = [];
  const commands: HostCommands = {
    ownsRamDisk: (disk) => ownsRecordedDisk(owned, disk),
    async detachRam(device, entry, deadlineMs) {
      const output = await host.detachRam(device, entry, deadlineMs);
      calls.push({ file: '/usr/bin/hdiutil', args: ['detach', device], output });
      return output;
    },
    async run(file, args, deadlineMs, binding) {
      const record =
        stepOf(paths, file, args) !== null ||
        (file === '/usr/bin/hdiutil' && ['attach', 'detach'].includes(args[0] ?? '')) ||
        (file === '/usr/sbin/diskutil' && args[0] === 'apfs');
      const startedAt = Date.now();
      const starting = JSON.stringify({ injected, phase: 'command-start', file, args });
      trace.log.info(starting);
      process.stderr.write(`${starting}\n`);
      const output =
        stepOf(paths, file, args) === injected
          ? { status: 1, stdout: '', stderr: 'injected' }
          : await host.run(file, args, deadlineMs, binding);
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
    // Through `run`, so the injection and the record see the attach as every other step. The rollback's
    // release is recorded as the gateway detach it is.
    async attachRam(sectors) {
      const attached = await recordedRamAttach(
        host.attachRam,
        (file, args, deadlineMs, binding) => commands.run(file, args, deadlineMs, binding),
        owned,
      )(sectors);
      if (!attached.ok) return attached;
      const lease = attached.value;
      return {
        ok: true,
        value: {
          device: lease.device,
          registryEntry: lease.registryEntry,
          keep: () => lease.keep(),
          async release() {
            const released = await lease.release();
            if (released.ok) {
              calls.push({ file: '/usr/bin/hdiutil', args: ['detach', lease.device], output: released.value });
            }
            return released;
          },
        },
      };
    },
  };
  const stopGateway = await startFixtureGateway(fixture);
  try {
    const refused = await new RamTempVolume(paths, capacity, commands).acquire(provisionOnly);
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
      return;
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
    expect(existsSync(paths.mountpoint)).toBe(false);
  } finally {
    trace.log.info(JSON.stringify({ injected, phase: 'test-teardown-start' }));
    try {
      await detachOwned(owned, paths.uid, gatewaySocket);
    } finally {
      await stopGateway();
    }
    removeScratchVolume(fixture);
    trace.log.info(JSON.stringify({ injected, phase: 'test-teardown-done' }));
  }
}

/**
 * A lease whose volume goes while its task holds it is said lost, by the space an ENOSPC line reads and by the
 * release, never measured on the host's Data volume or released as though it were there. From a land gate: the
 * volume was detached under running shards, a write through `TMPDIR` made a lease directory again on the bare
 * mountpoint, and the shard's ENOSPC verdict read the Data volume's 2778060.4 MiB free "of 8192.0 MiB".
 */
async function aLeaseLostUnderItsTask(): Promise<void> {
  const fixture = scratchVolume(3);
  const { paths, capacity, owned, gatewaySocket } = fixture;
  const stopGateway = await startFixtureGateway(fixture);
  // The module attaches the disk through the fixture gateway and records it, so the teardown can detach it.
  const volume = fixtureRamVolume(fixture, undefined);
  try {
    const acquired = await volume.acquire(provisionOnly);
    if (!acquired.ok) {
      expectSandboxed(acquired.error);
      return;
    }
    if (acquired.value.kind !== 'leased') {
      throw new Error(`no lease: ${JSON.stringify(acquired)}`);
    }
    const lease = acquired.value.lease;
    const measured = await volume.space(lease);
    expect(measured).toMatchObject({ kind: 'measured', lease: lease.directory, mountpoint: paths.mountpoint });
    expect(measured.kind === 'measured' && measured.capacityBytes <= capacity).toBe(true);

    // Another owner detaches the volume, and a write through TMPDIR makes the lease directory again below the bare
    // mountpoint, on the Data volume.
    await detachOwned(owned, paths.uid, gatewaySocket);
    mkdirSync(join(lease.directory, 'node-compile-cache'), { recursive: true });
    const detail = `no marked volume is mounted at ${paths.mountpoint} any more: it was detached while the task held its lease`;
    expect(await volume.space(lease)).toEqual({
      kind: 'lost',
      lease: lease.directory,
      mountpoint: paths.mountpoint,
      detail,
    });
    expect(await volume.release(lease, true, provisionOnly)).toEqual({
      ok: false,
      error: { kind: 'lease-lost', lease: lease.directory, mountpoint: paths.mountpoint, detail },
    });
  } finally {
    try {
      await detachOwned(owned, paths.uid, gatewaySocket);
    } finally {
      await stopGateway();
    }
    // Only once nothing is mounted there: the bare mountpoint, with the lease directory the write made again.
    rmSync(paths.mountpoint, { recursive: true, force: true });
    removeScratchVolume(fixture);
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
      'reclaims what a dead creator left, shares the volume between leases, and detaches it at the last release, removing its mountpoint',
      nativeRamVolume,
    );
    it(
      'leaves no volume under its final name, and no staging directory, when the staging umount fails on a real disk',
      aFailedStepLeavesNoNamedVolume,
    );
    it(
      'says a lease lost when its volume is detached under its task, never measuring the host disk in its place',
      aLeaseLostUnderItsTask,
    );
  } else {
    it(
      'keeps inherited temporary storage, honors a target override, and never provisions an APFS disk',
      inheritedTempDirectory,
    );
  }
});
