import { describe, expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir, userInfo } from 'node:os';
import { join } from 'node:path';
import { pidsWorkingIn, processTable, terminate } from '../../testing.js';
import {
  createHostCommands,
  type HostCommands,
  hostLeaseProcesses,
  imagesBackedUnder,
  leaseProcessesOn,
  mountsBelow,
  operationDisk,
  type RamDisk,
  type RamTempError,
  type RamTempPaths,
  RamTempVolume,
  ramDisksIn,
  ramTempPaths,
} from './ram-temp.js';

const MIB = 1024 * 1024;

describe('RAM temp volume output parsing', () => {
  it('names the disk diskutil apfs reports it created', () => {
    const container =
      'Creating APFS Container\nCreated new APFS Container disk271\nDisk from APFS operation: disk271\nFinished APFS operation on disk270\n';
    expect(operationDisk(container)).toBe('disk271');
    expect(operationDisk('Exporting new APFS Volume\nDisk from APFS operation: disk271s1\n')).toBe('disk271s1');
    expect(operationDisk('Error: -69808\n')).toBeNull();
  });

  it('lists the RAM disks hdiutil info reports, with their user, size and mountpoints', () => {
    const info = [
      'framework       : 683.160.3',
      'driver          : 683.160.3',
      'images          : 3',
      '================================================',
      'image-path      : /private/cowshed/store/x/main.asif',
      'mounting user   : danny',
      'framework name  : DiskImages2',
      '/dev/disk10\t\t',
      '/dev/disk11\tEF57347C-0000-11AA-AA11-00306543ECAC\t',
      '/dev/disk11s1\t41504653-0000-11AA-AA11-00306543ECAC\t/Users/danny/Dev/x',
      '================================================',
      'image-path      : ram://16777216',
      'image-type      : read/write',
      'blockcount      : 16777216',
      'mounting user   : danny',
      'mounting mode   : <unknown>',
      'process ID      : 91006',
      'framework name  : DiskImages',
      '/dev/disk138\t\t',
      '/dev/disk139\tEF57347C-0000-11AA-AA11-00306543ECAC\t',
      '/dev/disk139s1\t41504653-0000-11AA-AA11-00306543ECAC\t/Volumes/smoo-ram-502 1',
      '================================================',
      'image-path      : ram://16777216',
      'mounting user   : root',
      '/dev/disk159\t\t',
      '',
    ].join('\n');
    expect(ramDisksIn(info)).toEqual([
      { physical: '/dev/disk138', sectors: 16777216, user: 'danny', mountpoints: ['/Volumes/smoo-ram-502 1'] },
      { physical: '/dev/disk159', sectors: 16777216, user: 'root', mountpoints: [] },
    ]);
  });

  it('finds only the images whose backing file is on the volume', () => {
    const registry = [
      ...registryDevice('file:///private/tmp/smoo-ram-501/501-a1b2c3/store/main%20copy.asif', 'disk280'),
      ...registryDevice('file:///private/tmp/smoo-ram-5010/x.asif', 'disk281'),
      ...registryDevice('ram://134217728', 'disk270'),
    ].join('\n');
    expect(imagesBackedUnder(registry, '/private/tmp/smoo-ram-501/')).toEqual([
      '/private/tmp/smoo-ram-501/501-a1b2c3/store/main copy.asif (/dev/disk280)',
    ]);
  });

  it('finds the mounts that would make removing a lease delete through them', () => {
    const table = [
      '/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)',
      '/dev/disk271s1 on /private/tmp/smoo-ram-501 (apfs, local, nodev, nosuid, journaled, noowners, nobrowse)',
      '/dev/disk290s1 on /private/tmp/smoo-ram-501/501-a1b2c3/checkout (apfs, local, nobrowse, mounted by danny)',
      '/dev/disk291s1 on /private/tmp/smoo-ram-501/501-a1b2c3x/other (apfs, local)',
    ].join('\n');
    expect(mountsBelow(table, '/private/tmp/smoo-ram-501/501-a1b2c3/')).toEqual([
      '/private/tmp/smoo-ram-501/501-a1b2c3/checkout',
    ]);
  });

  it('mounts where DiskArbitration puts it, short enough to hold sockets', () => {
    // sun_path is 104 bytes; a lease directory leaves room for nested fixture paths.
    expect(ramTempPaths({ uid: 501, username: 'ada' })).toEqual({
      mountpoint: '/Volumes/smoo-ram-501',
      lockFile: '/private/tmp/smoo-ram-501.lock',
      volumeName: 'smoo-ram-501',
      stagingName: 'smoo-ram-501-new',
      user: 'ada',
    });
  });
});

describe('RAM temp volume on this host', () => {
  it('shares one volume between leases and detaches it when the last one ends', async () => {
    const { parent, paths, capacity } = scratchVolume(0);
    // The host's own commands, with one injectable image in the I/O Registry listing.
    const host = createHostCommands();
    const attachedBelow: { image: string | null } = { image: null };
    const commands: HostCommands = {
      async run(file, args) {
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
    const first = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
    const second = new RamTempVolume(paths, capacity, commands, 202, (pid) => alive.has(pid));
    try {
      const a = await first.acquire();
      if (!a.ok) {
        expectSandboxed(a.error);
        return;
      }
      expect(a.value.kind).toBe('leased');
      const b = await second.acquire();
      if (a.value.kind !== 'leased' || !b.ok || b.value.kind !== 'leased') {
        throw new Error(`second lease failed: ${JSON.stringify(b)}`);
      }
      const leaseA = a.value.lease;
      const leaseB = b.value.lease;
      expect(leaseA.mountpoint).toBe(paths.mountpoint);
      expect(leaseB.mountpoint).toBe(paths.mountpoint);
      expect(leaseA.directory).not.toBe(leaseB.directory);
      expect(statSync(leaseA.directory).dev).not.toBe(statSync(parent).dev);
      writeFileSync(join(leaseA.directory, 'scratch'), 'on RAM');

      expect(await first.release(leaseA)).toEqual({ ok: true, value: undefined });
      expect(existsSync(leaseA.directory)).toBe(false);
      expect(statSync(leaseB.directory).dev).not.toBe(statSync(parent).dev);

      // The second task dies without releasing, with an image still attached from its lease: the
      // registry entry is injected, so the test spends no attach of its own. The next task keeps
      // the lease and names it instead of detaching another owner's image.
      alive.delete(202);
      const image = join(leaseB.directory, 'store', 'main.asif');
      attachedBelow.image = image;
      const third = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
      const c = await third.acquire();
      if (!c.ok || c.value.kind !== 'leased') {
        throw new Error(`third lease failed: ${JSON.stringify(c)}`);
      }
      expect(c.value.held).toEqual([
        `${leaseB.directory} is kept: ${image} (/dev/disk999) still attached from below it; detach through its owner`,
      ]);
      expect(existsSync(leaseB.directory)).toBe(true);
      expect(await third.release(c.value.lease)).toEqual({ ok: true, value: undefined });
      expect(existsSync(c.value.lease.directory)).toBe(false);
      expect(statSync(leaseB.directory).dev).not.toBe(statSync(parent).dev);

      // Once its owner has detached the image, the next task reclaims the lease, and its release,
      // the last, detaches the volume.
      attachedBelow.image = null;
      const fourth = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
      const d = await fourth.acquire();
      if (!d.ok || d.value.kind !== 'leased') {
        throw new Error(`fourth lease failed: ${JSON.stringify(d)}`);
      }
      expect(d.value.held).toEqual([]);
      expect(existsSync(leaseB.directory)).toBe(false);
      expect(await fourth.release(d.value.lease)).toEqual({ ok: true, value: undefined });
      expect(existsSync(paths.mountpoint)).toBe(false);
      expect(attachedDisks(paths, capacity)).toEqual([]);
    } finally {
      // A failed assertion must not leave a RAM disk attached.
      detachAll(paths, capacity);
      rmSync(parent, { recursive: true, force: true });
    }
    // A RAM volume's attach/format/mount/rename and its detach: ~3 s of DiskArbitration.
  }, 30_000);

  it('reclaims what a dead creator left, never mounting beside it as "<name> 1"', async () => {
    const { parent, paths, capacity } = scratchVolume(1);
    const host = createHostCommands();
    const run = async (file: string, args: string[]): Promise<string> => {
      const output = await host.run(file, args);
      if (output.status !== 0) {
        throw new Error(`${file} ${args.join(' ')}: exit ${output.status}: ${output.stderr}`);
      }
      return output.stdout;
    };
    const volume = new RamTempVolume(paths, capacity, host);
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
      expect(attachedDisks(paths, capacity).map((disk) => disk.mountpoints)).toEqual([[paths.mountpoint], []]);

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

      const acquired = await volume.acquire();
      if (!acquired.ok || acquired.value.kind !== 'leased') {
        throw new Error(`lease over the debris failed: ${JSON.stringify(acquired)}`);
      }
      expect(acquired.value.lease.mountpoint).toBe(paths.mountpoint);
      expect(readFileSync(join(paths.mountpoint, '.smoo-ram'), 'utf8')).toBe(`${paths.volumeName}\n`);
      expect(existsSync(`${paths.mountpoint} 1`)).toBe(false);
      expect(attachedDisks(paths, capacity).map((disk) => disk.mountpoints)).toEqual([[paths.mountpoint]]);

      expect(await volume.release(acquired.value.lease)).toEqual({ ok: true, value: undefined });
      expect(attachedDisks(paths, capacity)).toEqual([]);
    } finally {
      detachAll(paths, capacity);
      rmSync(parent, { recursive: true, force: true });
    }
  }, 60_000);

  it('kills a deadlined command together with everything it spawned', async () => {
    // `hdiutil attach` leaves the attach to a diskimages-helper it spawns; the deadline must take
    // that child down too, or it finishes the attach after the task gave up on it.
    const result = await createHostCommands(500).run('/bin/sh', ['-c', 'sleep 60 & echo $!; wait']);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toMatch(/^no answer within 500 ms; killed process group \d+$/);
    const spawned = result.stdout.trim();
    expect(spawned).toMatch(/^\d+$/);
    // Gone, or a zombie its new parent has yet to reap: either way no longer running.
    const state = await createHostCommands().run('/bin/ps', ['-o', 'stat=', '-p', spawned]);
    expect(state.status === 1 || state.stdout.trim().startsWith('Z')).toBe(true);
  });

  it('asks a gateway that predates disk leases once, not before every disk command', async () => {
    // Such a gateway answers only after 2 s of silence; asking it before each of a volume's
    // dozen hdiutil and diskutil calls once pushed a test shard past its 120 s bound.
    const socket = `/tmp/smoo-rt-${process.pid}.sock`;
    rmSync(socket, { force: true });
    let asked = 0;
    const server = createServer({ allowHalfOpen: true }, (client) => {
      asked += 1;
      client.resume();
      client.on('end', () =>
        client.end('{"ok":false,"code":"invalid-request","error":"unknown gateway control operation"}\n'),
      );
    });
    const { promise: listening, resolve: listened } = Promise.withResolvers<void>();
    server.listen(socket, listened);
    await listening;
    try {
      const host = createHostCommands(10_000, socket);
      // `umount` with no operand only prints its usage, and it is a disk tool that takes a lease.
      expect((await host.run('/sbin/umount', [])).status).not.toBe(0);
      expect((await host.run('/sbin/umount', [])).status).not.toBe(0);
      expect(asked).toBe(1);
    } finally {
      server.close();
      rmSync(socket, { force: true });
    }
  });
});

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
      stagingName: `${name}-new`,
      user: userInfo().username,
    },
    capacity: 64 * MIB + (process.pid * 4 + index + 1) * 512,
  };
}

function attachedDisks(paths: RamTempPaths, capacity: number): RamDisk[] {
  let info: string;
  try {
    info = execFileSync('/usr/bin/hdiutil', ['info'], { encoding: 'utf8' });
  } catch (error) {
    // A host without hdiutil has no RAM disk attached.
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
  return ramDisksIn(info).filter((disk) => disk.user === paths.user && disk.sectors === capacity / 512);
}

function detachAll(paths: RamTempPaths, capacity: number): void {
  for (const disk of attachedDisks(paths, capacity)) {
    execFileSync('/usr/bin/hdiutil', ['detach', '-force', disk.physical]);
  }
}

/** A cowshed sandbox cannot attach images: creation fails at the host instead of leasing. */
function expectSandboxed(error: RamTempError): void {
  expect(error.kind).toBe('provision-failed');
  if (error.kind === 'provision-failed') {
    expect(['hdiutil info', 'hdiutil attach']).toContain(error.step);
  }
}

/** One attached image as `ioreg -r -c AppleDiskImageDevice -l -w0` lists it. */
function registryDevice(url: string, disk: string): string[] {
  return [
    '+-o AppleDiskImageDevice@5d6  <class AppleDiskImageDevice, id 0x100659c38, registered, matched, active>',
    `  |   "DiskImageURL" = "${url}"`,
    '  +-o IOBlockStorageDriver  <class IOBlockStorageDriver, id 0x100659c3a, registered>',
    '    +-o Apple Disk Image Media  <class IOMedia, id 0x100659c3b, registered>',
    `      |   "BSD Name" = "${disk}"`,
    '        +-o Untitled 1@1  <class IOMedia, id 0x10037ce47, registered>',
    `          |   "BSD Name" = "${disk}s1"`,
  ];
}

describe('processes a task left working in its RAM lease', () => {
  const scratch = (): string => mkdtempSync(join(realpathSync(tmpdir()), 'lease-leftovers-'));

  /**
   * A fixture's Nx daemon, as Nx leaves it: its own session, working in the fixture, with a child of
   * its own (a plugin worker), so the command's process-group kill never reaches it. It starts the
   * child once a line can be read from `gate`, and prints the child's pid once it has.
   */
  function detachedDaemon(lease: string, gate: string) {
    const daemon = spawn('sh', ['-c', 'read go < "$1"; sleep 600 & echo $!; wait', 'daemon', gate], {
      cwd: lease,
      detached: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const pid = daemon.pid;
    if (pid === undefined) {
      throw new Error('sh did not start');
    }
    const childPid = once(daemon.stdout, 'data').then(([line]) => Number(String(line).trim()));
    const exited = once(daemon, 'close');
    return { daemon, pid, childPid, exited };
  }

  async function liveAmong(pids: readonly number[]): Promise<number[]> {
    const live = (await processTable()).filter((entry) => !entry.stat.startsWith('Z')).map((entry) => entry.pid);
    return pids.filter((pid) => live.includes(pid));
  }

  it('stops a detached process working in the lease and what it started, and names them', async () => {
    const lease = scratch();
    const gate = join(lease, 'gate');
    execFileSync('mkfifo', [gate]);
    const { pid, childPid, exited } = detachedDaemon(lease, gate);
    let child = 0;
    try {
      await writeFile(gate, 'go\n');
      child = await childPid;

      const stopped = await hostLeaseProcesses.stopWorkingIn(lease);

      expect(stopped.map((entry) => Number(entry.split(' ')[0])).sort((a, b) => a - b)).toEqual(
        [pid, child].sort((a, b) => a - b),
      );
      expect(await liveAmong([pid, child])).toEqual([]);
    } finally {
      terminate(pid, 'SIGKILL');
      if (child !== 0) {
        terminate(child, 'SIGKILL');
      }
      await exited;
      rmSync(lease, { recursive: true, force: true });
    }
  });

  it('stops what a process starts after the process table was read, before it was signalled', async () => {
    const lease = scratch();
    const gate = join(lease, 'gate');
    execFileSync('mkfifo', [gate]);
    const { pid, childPid, exited } = detachedDaemon(lease, gate);
    let child = 0;
    // The daemon starts its child right after the table is read, so the table cannot name it.
    let reads = 0;
    const processes = leaseProcessesOn({
      workingIn: pidsWorkingIn,
      async table() {
        const table = await processTable();
        reads += 1;
        if (reads === 1) {
          expect(table.some((entry) => entry.ppid === pid)).toBe(false);
          await writeFile(gate, 'go\n');
          child = await childPid;
        }
        return table;
      },
    });
    try {
      const stopped = await processes.stopWorkingIn(lease);

      expect(child).not.toBe(0);
      expect(await liveAmong([pid, child])).toEqual([]);
      expect(stopped.map((entry) => Number(entry.split(' ')[0])).sort((a, b) => a - b)).toEqual(
        [pid, child].sort((a, b) => a - b),
      );
    } finally {
      terminate(pid, 'SIGKILL');
      if (child !== 0) {
        terminate(child, 'SIGKILL');
      }
      await exited;
      rmSync(lease, { recursive: true, force: true });
    }
  });

  it('finds a child whose lease root exits before the process table is read', async () => {
    const lease = scratch();
    const gate = join(lease, 'gate');
    execFileSync('mkfifo', [gate]);
    const { daemon, pid, childPid, exited } = detachedDaemon(lease, gate);
    let child = 0;
    let reads = 0;
    const processes = leaseProcessesOn({
      workingIn: pidsWorkingIn,
      async table() {
        reads += 1;
        if (reads === 1) {
          await writeFile(gate, 'go\n');
          child = await childPid;
          const ended = once(daemon, 'exit');
          terminate(pid, 'SIGKILL');
          await ended;
        }
        return processTable();
      },
    });
    try {
      const stopped = await processes.stopWorkingIn(lease);
      expect(stopped.map((entry) => Number(entry.split(' ')[0]))).toEqual([child]);
      expect(await liveAmong([pid, child])).toEqual([]);
    } finally {
      terminate(pid, 'SIGKILL');
      if (child !== 0) {
        terminate(child, 'SIGKILL');
      }
      await exited;
      rmSync(lease, { recursive: true, force: true });
    }
  });

  it('resumes a process held during discovery when a host read fails', async () => {
    const lease = scratch();
    const gate = join(lease, 'gate');
    execFileSync('mkfifo', [gate]);
    const { pid, childPid, exited } = detachedDaemon(lease, gate);
    let child = 0;
    let observedStopped = false;
    const failed = new Error('process table unavailable after SIGSTOP');
    const processes = leaseProcessesOn({
      workingIn: pidsWorkingIn,
      async table() {
        const table = await processTable();
        if (table.some((entry) => entry.pid === pid && entry.stat.startsWith('T'))) {
          observedStopped = true;
          throw failed;
        }
        return table;
      },
    });
    try {
      await expect(processes.stopWorkingIn(lease)).rejects.toBe(failed);
      expect(observedStopped).toBe(true);
      // Reading the FIFO and starting the child requires the daemon to have resumed.
      await writeFile(gate, 'go\n');
      child = await childPid;
      expect(child).toBeGreaterThan(0);
    } finally {
      terminate(pid, 'SIGKILL');
      if (child !== 0) {
        terminate(child, 'SIGKILL');
      }
      await exited;
      rmSync(lease, { recursive: true, force: true });
    }
  });

  it('stops nothing in a lease no process works in, and nothing that works elsewhere', async () => {
    const lease = scratch();
    const elsewhere = scratch();
    const bystander = spawn('sleep', ['600'], { cwd: elsewhere, detached: true, stdio: 'ignore' });
    try {
      expect(await hostLeaseProcesses.stopWorkingIn(lease)).toEqual([]);
      expect(bystander.exitCode === null && bystander.signalCode === null).toBe(true);
    } finally {
      bystander.kill('SIGKILL');
      rmSync(lease, { recursive: true, force: true });
      rmSync(elsewhere, { recursive: true, force: true });
    }
  });
});
