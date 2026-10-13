import { describe, expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ownedSocketDirectory, pidsWorkingIn, processTable, terminate } from '../../testing.js';
import {
  createHostCommands,
  diskCommand,
  hostLeaseProcesses,
  imagesBackedUnder,
  leaseProcessesOn,
  mountsBelow,
  operationDisk,
  RAM_TEMP_CAPACITY_BYTES,
  ramDisksIn,
  ramTempPaths,
} from './ram-temp.js';
import { registryDevice } from './ram-temp-fixtures.js';

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
      'mounting user   : example',
      'framework name  : DiskImages2',
      '/dev/disk10\t\t',
      '/dev/disk11\tEF57347C-0000-11AA-AA11-00306543ECAC\t',
      '/dev/disk11s1\t41504653-0000-11AA-AA11-00306543ECAC\t/Users/example/checkout',
      '================================================',
      'image-path      : ram://16777216',
      'image-type      : read/write',
      'blockcount      : 16777216',
      'mounting user   : example',
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
      { physical: '/dev/disk138', sectors: 16777216, user: 'example', mountpoints: ['/Volumes/smoo-ram-502 1'] },
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
      '/dev/disk290s1 on /private/tmp/smoo-ram-501/501-a1b2c3/checkout (apfs, local, nobrowse, mounted by example)',
      '/dev/disk291s1 on /private/tmp/smoo-ram-501/501-a1b2c3x/other (apfs, local)',
    ].join('\n');
    expect(mountsBelow(table, '/private/tmp/smoo-ram-501/501-a1b2c3/')).toEqual([
      '/private/tmp/smoo-ram-501/501-a1b2c3/checkout',
    ]);
  });

  it('mounts where DiskArbitration puts it, short enough to hold sockets', () => {
    // sun_path is 104 bytes; a lease directory leaves room for nested fixture paths.
    expect(ramTempPaths({ uid: 501, username: 'ada' })).toEqual({
      mountpoint: '/Volumes/smoo-ram2-501',
      lockFile: '/private/tmp/smoo-ram2-501.lock',
      volumeName: 'smoo-ram2-501',
      stagingName: 'smoo-ram2-501-new',
      user: 'ada',
    });
  });

  it("never shares a volume or a RAM disk size with revision 1's checkouts", () => {
    // Revision 1 used /Volumes/smoo-ram-<uid> and ram://16777216; a creator's stray reclaim matches
    // user and size, so equal sizes would let either revision detach the other's attaching disk.
    expect(RAM_TEMP_CAPACITY_BYTES / 512).toBe(16777218);
  });
});

describe('RAM temp volume on this host', () => {
  it('kills a deadlined command together with everything it spawned', async () => {
    // Ordinary helpers remain deadline-bound; disk-image tools are owned by cowshed instead.
    const result = await createHostCommands(500).run('/bin/sh', ['-c', 'sleep 60 & echo $!; wait']);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toMatch(/^no answer within 500 ms; killed process group \d+$/);
    const spawned = result.stdout.trim();
    expect(spawned).toMatch(/^\d+$/);
    // Gone, or a zombie its new parent has yet to reap: either way no longer running.
    const state = await createHostCommands().run('/bin/ps', ['-o', 'stat=', '-p', spawned]);
    expect(state.status === 1 || state.stdout.trim().startsWith('Z')).toBe(true);
  });

  it('routes both disk tools through cowshed without shell interpolation', () => {
    for (const tool of ['hdiutil', 'diskutil']) {
      for (const file of [tool, `/usr/sbin/${tool}`]) {
        expect(diskCommand(file, ['attach', '/image with spaces; echo unsafe'])).toEqual([
          'disk-command',
          tool,
          '--',
          'attach',
          '/image with spaces; echo unsafe',
        ]);
      }
    }
    expect(diskCommand('/sbin/umount', ['/volume'])).toBeNull();
  });

  it('bounds its wait without killing the cowshed disk-command owner', async () => {
    const directory = await ownedSocketDirectory('rt-', 'fake-cowshed');
    const fake = join(directory, 'cowshed');
    await writeFile(fake, '#!/bin/sh\necho $$\nprintf "%s\\n" "$@"\nsleep 60\n', { mode: 0o755 });
    let pid = 0;
    try {
      const started = Date.now();
      const result = await createHostCommands(300, undefined, undefined, fake).run('/usr/bin/hdiutil', [
        'attach',
        '/image with spaces',
      ]);
      expect(Date.now() - started).toBeLessThan(10_000);
      expect(result.status).not.toBe(0);
      expect(result.stderr).toMatch(/^no answer within 300 ms; cowshed command \d+ left running, not killed/);
      const [processId, ...args] = result.stdout.trim().split('\n');
      pid = Number(processId);
      expect(args).toEqual(['disk-command', 'hdiutil', '--', 'attach', '/image with spaces']);
      expect(() => process.kill(pid, 0)).not.toThrow();
    } finally {
      if (pid > 0) process.kill(-pid, 'SIGKILL');
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('asks a gateway that predates disk leases once, not before every disk command', async () => {
    // Such a gateway answers only after 2 s of silence; asking it before each of a volume's
    // dozen hdiutil and diskutil calls once pushed a test shard past its 120 s bound.
    // A directory of its own in the socket namespace the sandbox admits, not a literal /tmp path.
    const directory = await ownedSocketDirectory('rt-', 'gw.sock');
    const socket = join(directory, 'gw.sock');
    let asked = 0;
    const server = createServer({ allowHalfOpen: true }, (client) => {
      asked += 1;
      client.resume();
      client.on('end', () =>
        client.end('{"ok":false,"code":"invalid-request","error":"unknown gateway control operation"}\n'),
      );
    });
    try {
      const { promise: listening, resolve: listened, reject: failed } = Promise.withResolvers<void>();
      server.once('error', failed);
      server.listen(socket, listened);
      await listening;
      server.off('error', failed);
      const host = createHostCommands(10_000, socket);
      // `umount` with no operand only prints its usage, and it is a disk tool that takes a lease.
      expect((await host.run('/sbin/umount', [])).status).not.toBe(0);
      expect((await host.run('/sbin/umount', [])).status).not.toBe(0);
      expect(asked).toBe(1);
    } finally {
      server.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('asks a gateway that refuses alone leases shared at once, and shared for a minute after', async () => {
    // A lease-scheduling gateway from before `share` refuses the field at once; the refused command
    // still runs leased, and later ones do not ask alone again.
    const directory = await ownedSocketDirectory('rt-', 'gw.sock');
    const socket = join(directory, 'gw.sock');
    const asked: string[] = [];
    const server = createServer({ allowHalfOpen: true }, (client) => {
      client.once('data', (chunk) => {
        const line = chunk.toString('utf8');
        asked.push(line.includes('"share":"alone"') ? 'alone' : 'shared');
        client.end(
          line.includes('"share"')
            ? '{"ok":false,"code":"invalid-request","error":"gateway control encoding failed: gateway control request contains an unknown field"}\n'
            : '{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted"}\n',
        );
      });
    });
    try {
      const { promise: listening, resolve: listened, reject: failed } = Promise.withResolvers<void>();
      server.once('error', failed);
      server.listen(socket, listened);
      await listening;
      server.off('error', failed);
      const host = createHostCommands(10_000, socket, 'alone');
      // `umount` with no operand only prints its usage, and it is a disk tool that takes a lease.
      expect((await host.run('/sbin/umount', [])).status).not.toBe(0);
      expect((await host.run('/sbin/umount', [])).status).not.toBe(0);
      expect(asked).toEqual(['alone', 'shared', 'shared']);
    } finally {
      server.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

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

      const owned = [pid, child];
      let cwdReads = 0;
      let consecutiveStopped = 0;
      const processes = leaseProcessesOn({
        workingIn: async (directory) => {
          if (cwdReads > 0) {
            expect(consecutiveStopped).toBeGreaterThanOrEqual(2);
          }
          cwdReads += 1;
          return pidsWorkingIn(directory);
        },
        signal: terminate,
        async table() {
          const table = await processTable();
          const rows = table.filter((entry) => owned.includes(entry.pid));
          const bothStopped = rows.length === owned.length && rows.every((entry) => /^[TtZ]/.test(entry.stat));
          consecutiveStopped = bothStopped ? consecutiveStopped + 1 : 0;
          return table;
        },
      });

      const stopped = await processes.stopWorkingIn(lease);

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
      signal: terminate,
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
      signal: terminate,
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
      signal: terminate,
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

  it('attempts every resume and preserves discovery failure when the first SIGCONT fails', async () => {
    const lease = scratch();
    const gate = join(lease, 'gate');
    execFileSync('mkfifo', [gate]);
    const { pid, childPid, exited } = detachedDaemon(lease, gate);
    let child = 0;
    const continued: number[] = [];
    const failedRead = new Error('process table unavailable after SIGSTOP');
    const failedResume = new Error('SIGCONT denied');
    const processes = leaseProcessesOn({
      workingIn: pidsWorkingIn,
      async table() {
        const table = await processTable();
        if (table.some((entry) => entry.pid === pid && entry.stat.startsWith('T'))) {
          throw failedRead;
        }
        return table;
      },
      signal(processId, signal) {
        if (signal === 'SIGCONT') {
          continued.push(processId);
          if (continued.length === 1) {
            throw failedResume;
          }
        }
        terminate(processId, signal);
      },
    });
    try {
      await writeFile(gate, 'go\n');
      child = await childPid;
      const cleanup = processes.stopWorkingIn(lease);
      await expect(cleanup).rejects.toBeInstanceOf(AggregateError);
      await expect(cleanup).rejects.toMatchObject({
        errors: [failedRead, expect.objectContaining({ cause: failedResume })],
      });
      expect(continued.slice().sort((a, b) => a - b)).toEqual([pid, child].sort((a, b) => a - b));
      await expect(cleanup).rejects.toThrow(`${continued[0]} (`);
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
