import { describe, expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { closeSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, rmSync } from 'node:fs';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { churnScript, openForProbe, writerRemains } from '../../__tests__/churn-group.js';
import { whenChanged } from '../../__tests__/path-change.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { awaitExit, ownedSocketDirectory, pidsWorkingIn, processTable, terminate } from '../../testing.js';
import {
  COWSHED_EXECUTABLE_ENV,
  createHostCommands,
  diskCommand,
  hostLeaseProcesses,
  imagesBackedUnder,
  leaseProcessesOn,
  mountsBelow,
  operationDisk,
  RAM_TEMP_CAPACITY_BYTES,
  RamTempVolume,
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
      staging: '/private/tmp/smoo-ram2-501-new',
      user: 'ada',
    });
  });

  it("never shares a volume or a RAM disk size with revision 1's checkouts", () => {
    // Revision 1 used /Volumes/smoo-ram-<uid> and ram://16777216; a creator's stray reclaim matches
    // user and size, so equal sizes would let either revision detach the other's attaching disk.
    expect(RAM_TEMP_CAPACITY_BYTES / 512).toBe(16777218);
  });
});

/** A real facade boundary: owns a detached stand-in, kills only SAFE children, and waits for reap.
 * The control socket gates facade exit after reap; protected output stays in owned files.
 */
async function ownedDiskFacadeFixture() {
  const directory = await ownedSocketDirectory('rt-', 'facade.sock');
  const socketPath = join(directory, 'facade.sock');
  const executable = join(directory, 'cowshed');
  const server = createServer();
  const connected = new Promise<Socket>((resolve) => server.once('connection', resolve));
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, resolve);
  });
  const session = connected.then((socket) => ({
    socket,
    lines: createInterface({ input: socket })[Symbol.asyncIterator](),
  }));
  let facadePid: number | undefined;
  await writeFile(
    executable,
    `#!${process.execPath}\n` +
      "import { spawn } from 'node:child_process';\nimport { once } from 'node:events';\nimport { closeSync, openSync } from 'node:fs';\nimport { createConnection } from 'node:net';\nimport { dirname, join } from 'node:path';\nimport { createInterface } from 'node:readline';\n\nconst startedAt = performance.now();\nconst argv = process.argv.slice(2);\nconst control = createConnection(argv.at(-1));\nawait once(control, 'connect');\nconst tool = argv[1];\nconst safe = argv[2] === '--deadline-ms';\nconst budget = safe ? Number(argv[3]) : undefined;\nconst native = argv.slice(safe ? 5 : 3);\nconst image = tool === 'hdiutil' || (tool === 'diskutil' && (native[0] === 'image' || native[0] === 'eject'));\nif (argv[0] !== 'disk-command' || (tool !== 'hdiutil' && tool !== 'diskutil') ||\n    argv[safe ? 4 : 2] !== '--' || safe === image ||\n    (safe && (!Number.isSafeInteger(budget) || budget <= 0))) {\n  control.end('invalid facade request\\n');\n  process.exitCode = 2;\n} else {\n  const mode = native[1];\n  const tail = mode === 'exit' ? 'exit 7' : mode === 'signal' ? 'kill -TERM $$' : 'exec sleep 600';\n  const files = image ? [openSync(join(dirname(argv.at(-1)), 'stdout'), 'w'), openSync(join(dirname(argv.at(-1)), 'stderr'), 'w')] : [];\n  const child = spawn('/bin/sh', ['-c', 'printf \"owned stdout\\\\n\"; printf \"owned stderr\\\\n\" >&2; printf ready >&3; ' + tail], {\n    detached: true,\n    stdio: ['ignore', files[0] ?? 'pipe', files[1] ?? 'pipe', 'pipe'],\n  });\n  for (const fd of files) closeSync(fd);\n  const ended = once(child, 'close');\n  if (!image) {\n    child.stdout.on('data', (chunk) => process.stdout.write(chunk));\n    child.stderr.on('data', (chunk) => process.stderr.write(chunk));\n  }\n  let release;\n  const finished = new Promise((resolve) => { release = resolve; });\n  let reaped = false;\n  const commands = createInterface({ input: control });\n  commands.on('line', (line) => {\n    if (line === 'cleanup') {\n      if (!reaped && child.exitCode === null && child.signalCode === null) process.kill(-child.pid, 'SIGKILL');\n      release();\n    } else if (line === 'finish') {\n      release();\n    } else if (line === 'inspect') {\n      process.kill(child.pid, 0);\n      control.write('custody ' + child.pid + '\\n');\n    }\n  });\n  let expired = false;\n  const timer = safe ? setTimeout(() => {\n    expired = true;\n    control.write('deadline ' + budget + '\\n');\n    process.kill(-child.pid, 'SIGKILL');\n  }, Math.max(0, budget - (performance.now() - startedAt))) : undefined;\n  const ready = await new Promise((resolve) => {\n    child.stdio[3].once('data', () => resolve(true));\n    child.stdio[3].once('end', () => resolve(false));\n    child.stdio[3].once('error', () => resolve(false));\n  });\n  if (!ready) throw new Error('owned child ended before readiness');\n  control.write('started ' + process.pid + ' ' + child.pid + '\\n');\n  const [code, signal] = await ended;\n  reaped = true;\n  clearTimeout(timer);\n  control.write('reaped ' + child.pid + ' ' + code + ' ' + signal + '\\n');\n  if (expired) process.stderr.write('owned child ' + child.pid + ' terminated by ' + signal + ' and reaped after ' + budget + ' ms deadline\\n');\n  await finished;\n  commands.close();\n  control.end();\n  await once(control, 'close');\n  if (mode === 'signal') process.kill(process.pid, 'SIGTERM');\n  else process.exitCode = expired ? 1 : code ?? 1;\n}\n",
    { mode: 0o755 },
  );
  async function event(): Promise<string> {
    const { lines } = await session;
    const next = await lines.next();
    if (next.done) throw new Error('owned disk facade closed before the expected event');
    return next.value;
  }
  return {
    directory,
    executable,
    socketPath,
    event,
    async started() {
      const line = await event();
      const match = /^started (\d+) (\d+)$/.exec(line);
      if (match === null) throw new Error(`owned disk facade did not start its child: ${line}`);
      facadePid = Number(match[1]);
      return Number(match[2]);
    },
    async send(command: 'finish' | 'inspect') {
      const { socket } = await session;
      socket.write(`${command}\n`);
    },
    async cleanup() {
      try {
        const { socket } = await session;
        if (!socket.destroyed) {
          const closed = once(socket, 'close');
          socket.write('cleanup\n');
          await closed;
        }
        if (facadePid !== undefined) {
          await awaitExit(
            [{ pid: facadePid, ppid: process.pid, stat: 'S', command: executable }],
            'owned disk facade cleanup',
          );
        }
      } finally {
        await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
        rmSync(directory, { recursive: true, force: true });
      }
    },
  };
}

describe('RAM temp volume on this host', () => {
  it('kills a deadlined command together with everything it spawned', async () => {
    // Ordinary helpers remain deadline-bound; disk-image tools are owned by cowshed instead. The command never answers:
    // its shell opens `lifetime`, hands it to a background `tail -f /dev/null` that never ends on its own, closes its
    // own copy and only then prints the tail's pid. So only the deadline ends the run, a printed pid means the FIFO
    // has a writer, and EOF on it is that descendant's death. The bound returns once it has signalled the group, not
    // once the kernel has torn it down, so a `ps` read straight after raced the kill (Linux CI saw it still running).
    const directory = mkdtempSync(join(realpathSync(tmpdir()), 'deadline-group-'));
    try {
      const lifetime = join(directory, 'lifetime.fifo');
      execFileSync('mkfifo', [lifetime]);
      const ended = readFile(lifetime, 'utf8');
      const result = await createHostCommands(500).run('/bin/sh', [
        '-c',
        'exec 3>"$1"; tail -f /dev/null & exec 3>&-; echo $!; wait',
        'sh',
        lifetime,
      ]);
      expect(result.status).not.toBe(0);
      // The group is called killed only when it was left empty; a group still listed at the end says so.
      expect(result.stderr).toMatch(
        /^no answer within 500 ms; (?:killed process group \d+|signalled its process group, but .*)$/,
      );
      expect(result.stdout.trim()).toMatch(/^\d+$/);
      expect(await ended).toBe('');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('leaves no member of a deadlined command behind when members fork as the kill goes out', async () => {
    // macOS does not hold a fork back for a group signal in flight, so a member forked as SIGKILL goes out is not
    // reached by it. The command is a group that forks as fast as it can. The deadline is the subject, and wherever it
    // lands in the group's start the same must hold: the bound returns only once the group lists no member or has
    // given up, so every member is gone when it does. The end of file of the FIFO every member holds is the proof, and
    // a kill that is not repeated leaves a member that only the backstop ends.
    const directory = mkdtempSync(join(realpathSync(tmpdir()), 'deadline-churn-'));
    const lifetime = join(directory, 'lifetime.fifo');
    execFileSync('mkfifo', [lifetime]);
    const reader = openForProbe(lifetime);
    try {
      const gone = readFile(lifetime, 'utf8');
      const result = await createHostCommands(500).run('/bin/sh', ['-c', churnScript(), 'sh', lifetime, '/dev/null']);
      expect(result.status).not.toBe(0);
      expect(result.stderr).toMatch(
        /^no answer within 500 ms; (?:killed process group \d+|signalled its process group, but .*)$/,
      );
      expect(writerRemains(reader), result.stderr).toBe(false);
      expect(await gone).toBe('');
    } finally {
      closeSync(reader);
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('routes disk tools and their lifetime together without shell interpolation', () => {
    const target = '/image with spaces; echo unsafe';
    for (const file of ['hdiutil', '/usr/bin/hdiutil']) {
      expect(diskCommand(file, ['attach', target], 300)).toEqual({
        args: ['disk-command', 'hdiutil', '--', 'attach', target],
        lifetime: 'stop-wait',
      });
    }
    for (const file of ['diskutil', '/usr/sbin/diskutil']) {
      for (const verb of ['image', 'eject']) {
        expect(diskCommand(file, [verb, target], 300)).toEqual({
          args: ['disk-command', 'diskutil', '--', verb, target],
          lifetime: 'stop-wait',
        });
      }
      for (const verb of ['info', 'apfs', 'mount', 'unmount', 'Image']) {
        expect(diskCommand(file, [verb, target], 300)).toEqual({
          args: ['disk-command', 'diskutil', '--deadline-ms', '300', '--', verb, target],
          lifetime: 'facade-owned',
        });
      }
    }
    expect(diskCommand('/sbin/umount', ['/volume'], 300)).toBeNull();
  });

  it('waits past the SAFE deadline for actual owned child reap and the facade terminal exit', async () => {
    const fixture = await ownedDiskFacadeFixture();
    let settled = false;
    const result = createHostCommands(undefined, undefined, undefined, fixture.executable)
      .run('/usr/sbin/diskutil', ['info', 'overrun', fixture.socketPath], 300)
      .then((output) => {
        settled = true;
        return output;
      });
    try {
      const pid = await fixture.started();
      expect(await fixture.event()).toBe('deadline 300');
      expect(await fixture.event()).toBe(`reaped ${pid} null SIGKILL`);
      expect(settled).toBe(false);
      expect(() => process.kill(pid, 0)).toThrow();
      await fixture.send('finish');
      expect(await result).toEqual({
        status: 1,
        stdout: 'owned stdout\n',
        stderr: `owned stderr\nowned child ${pid} terminated by SIGKILL and reaped after 300 ms deadline\n`,
      });
    } finally {
      await fixture.cleanup();
    }
  });

  it('stops waiting only for protected images while preserving their live owner and file output', async () => {
    for (const { tool, verb } of [
      { tool: 'hdiutil', verb: 'attach' },
      { tool: 'diskutil', verb: 'image' },
      { tool: 'diskutil', verb: 'eject' },
    ]) {
      const fixture = await ownedDiskFacadeFixture();
      const result = createHostCommands(300, undefined, undefined, fixture.executable).run(tool, [
        verb,
        fixture.socketPath,
      ]);
      let pid = 0;
      try {
        pid = await fixture.started();
        const output = await result;
        expect(output.status).toBe(1);
        expect(output.stderr).toMatch(/^no answer within 300 ms; cowshed command \d+ left running, not killed/);
        expect(() => process.kill(pid, 0)).not.toThrow();
        await fixture.send('inspect');
        expect(await fixture.event()).toBe(`custody ${pid}`);
        expect(await readFile(join(fixture.directory, 'stdout'), 'utf8')).toBe('owned stdout\n');
        expect(await readFile(join(fixture.directory, 'stderr'), 'utf8')).toBe('owned stderr\n');
      } finally {
        // The fixture alone owns cleanup authority; it kills its exact stand-in and awaits reap.
        await fixture.cleanup();
        if (pid > 0) expect(() => process.kill(pid, 0)).toThrow();
      }
    }
  });

  it('preserves facade nonzero exit, signal diagnostics and the environment-selected owner', async () => {
    const before = process.env[COWSHED_EXECUTABLE_ENV];
    try {
      for (const mode of ['exit', 'signal']) {
        const fixture = await ownedDiskFacadeFixture();
        process.env[COWSHED_EXECUTABLE_ENV] = fixture.executable;
        const result = createHostCommands().run('/usr/sbin/diskutil', ['info', mode, fixture.socketPath]);
        try {
          const pid = await fixture.started();
          expect(await fixture.event()).toBe(`reaped ${pid} ${mode === 'exit' ? '7 null' : 'null SIGTERM'}`);
          await fixture.send('finish');
          expect(await result).toEqual({
            status: mode === 'exit' ? 7 : 1,
            stdout: 'owned stdout\n',
            stderr: mode === 'exit' ? 'owned stderr\n' : 'owned stderr\n\nkilled by SIGTERM',
          });
        } finally {
          await fixture.cleanup();
        }
      }
    } finally {
      if (before === undefined) delete process.env[COWSHED_EXECUTABLE_ENV];
      else process.env[COWSHED_EXECUTABLE_ENV] = before;
    }
  });

  it('reports an unavailable facade as a process-start error, not a child signal', async () => {
    const directory = await ownedSocketDirectory('rt-', 'missing-cowshed');
    try {
      const executable = join(directory, 'missing-cowshed');
      // The bound is the leg backstop: a missing facade fails at spawn, so the run ends at once.
      const host = createHostCommands(BOUNDED_TEST_IDLE_TIMEOUT_MS, undefined, undefined, executable);
      const output = await host.run('/usr/sbin/diskutil', ['info']);
      expect(output.status).toBe(1);
      expect(output.stdout).toBe('');
      expect(output.stderr).toContain(executable);
      expect(output.stderr).toContain('ENOENT');
      expect(output.stderr).not.toContain('killed by');
    } finally {
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
      const host = createHostCommands(BOUNDED_TEST_IDLE_TIMEOUT_MS, socket);
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
      const host = createHostCommands(BOUNDED_TEST_IDLE_TIMEOUT_MS, socket, 'alone');
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
  it('skips only empty or absent lease directories and reports missing census tools as inspection failures', async () => {
    const directory = mkdtempSync(join(realpathSync(tmpdir()), 'lease-reap-errors-'));
    let scans = 0;
    let failure = Object.assign(new Error('spawn /usr/sbin/lsof ENOENT'), { code: 'ENOENT' });
    const volume = new RamTempVolume(
      ramTempPaths({ uid: 0, username: 'fixture' }),
      1,
      {
        async run() {
          throw new Error('reap must not provision a disk');
        },
      },
      undefined,
      undefined,
      {
        async stopWorkingIn() {
          scans += 1;
          throw failure;
        },
      },
    );
    const lease = { directory, mountpoint: directory, capacityBytes: 1 };
    try {
      expect(await volume.reap(lease)).toEqual({ ok: true, value: [] });
      await rm(directory, { recursive: true });
      expect(await volume.reap(lease)).toEqual({ ok: true, value: [] });
      expect(scans).toBe(0);
      await mkdir(directory);
      await writeFile(join(directory, 'owned-file'), 'still requires inspection');
      for (const code of ['ENOENT', 'EACCES']) {
        failure = Object.assign(new Error(`spawn /usr/sbin/lsof ${code}`), { code });
        expect(await volume.reap(lease)).toEqual({
          ok: false,
          error: { kind: 'provision-failed', step: 'reap', detail: failure.message },
        });
      }
      expect(scans).toBe(2);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

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

  it('stops what writes into the lease through its TMPDIR from elsewhere, so the lease can be removed', async () => {
    const lease = scratch();
    const elsewhere = scratch();
    // As a dead task's ttsc plugin build leaves it: working in its project, writing into `$TMPDIR`
    // as fast as it can (shell builtins only, so it forks nothing), with a child of its own. macOS
    // spells TMPDIR with a trailing slash.
    const writer = spawn('sh', ['-c', 'sleep 600 & echo $!; i=0; while :; do i=$((i+1)); : > "$TMPDIR/w$i"; done'], {
      cwd: elsewhere,
      env: { ...process.env, TMPDIR: `${lease}/` },
      detached: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    // Another, started on its own with a TMPDIR beneath the lease.
    mkdirSync(join(lease, 'nested'));
    const nested = spawn('sleep', ['600'], {
      cwd: elsewhere,
      env: { ...process.env, TMPDIR: join(lease, 'nested') },
      detached: true,
      stdio: 'ignore',
    });
    const exited = [once(writer, 'close'), once(nested, 'close')];
    const pids = [writer.pid ?? 0, nested.pid ?? 0];
    let child = 0;
    try {
      child = Number(String((await once(writer.stdout, 'data'))[0]).trim());
      // Ends on the watch event that brings the lease to two entries: `nested`, and the writer's first file.
      await whenChanged(lease, () => readdirSync(lease).length >= 2);
      // None of them works in the lease by its working directory.
      expect(await pidsWorkingIn(lease)).toEqual([]);

      const stopped = await hostLeaseProcesses.stopWorkingIn(lease);

      expect(stopped.map((entry) => Number(entry.split(' ')[0])).sort((a, b) => a - b)).toEqual(
        [...pids, child].sort((a, b) => a - b),
      );
      expect(await liveAmong([...pids, child])).toEqual([]);
      // Nothing writes there any more: the lease goes whole, with no ENOTEMPTY.
      await rm(lease, { recursive: true });
    } finally {
      for (const pid of [...pids, child]) {
        if (pid !== 0) {
          terminate(pid, 'SIGKILL');
        }
      }
      await Promise.all(exited);
      rmSync(lease, { recursive: true, force: true });
      rmSync(elsewhere, { recursive: true, force: true });
    }
  });

  it('stops nothing in a lease no process works in, and nothing that works elsewhere', async () => {
    const lease = scratch();
    const elsewhere = scratch();
    const bystander = spawn('sleep', ['600'], { cwd: elsewhere, detached: true, stdio: 'ignore' });
    // A TMPDIR that only starts with the lease's name is another directory.
    const sibling = spawn('sleep', ['600'], {
      cwd: elsewhere,
      env: { ...process.env, TMPDIR: `${lease}x` },
      detached: true,
      stdio: 'ignore',
    });
    try {
      expect(await hostLeaseProcesses.stopWorkingIn(lease)).toEqual([]);
      for (const left of [bystander, sibling]) {
        expect(left.exitCode === null && left.signalCode === null).toBe(true);
      }
    } finally {
      bystander.kill('SIGKILL');
      sibling.kill('SIGKILL');
      rmSync(lease, { recursive: true, force: true });
      rmSync(elsewhere, { recursive: true, force: true });
    }
  });
});
