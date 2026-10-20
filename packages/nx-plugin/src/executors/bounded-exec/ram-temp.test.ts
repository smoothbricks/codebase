import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { closeSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { churnScript, openForProbe, writerRemains } from '../../__tests__/churn-group.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { awaitExit, ownedSocketDirectory } from '../../testing.js';
import {
  type AttachedRamDisk,
  COWSHED_EXECUTABLE_ENV,
  type CommandOutput,
  createHostCommands,
  describeRamTempError,
  detachRamDisk,
  diskCommand,
  imagesBackedUnder,
  mountsBelow,
  operationDisk,
  RAM_TEMP_CAPACITY_BYTES,
  type RamTempError,
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

  it('lists the RAM disks the I/O Registry holds, with their owner, size and mounts', () => {
    // Captured 2026-10-22 on macOS 26.6 with `ioreg -r -c IOHDIXHDDriveOutKernel -l -w0`: another checkout's
    // revision-2 RAM disk, unmounted at that instant, and a 64 MiB disk whose volume mount_apfs placed below t/.
    const table = [
      '/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)',
      '/dev/disk543s1 on /private/var/folders/cowshed/502/t/smoo-ram-fixture-27416 (apfs, local, nodev, nosuid, journaled, noowners, nobrowse, mounted by danny)',
    ].join('\n');
    expect(ramDisksIn(capturedRegistry(), table)).toEqual({
      ok: true,
      value: [
        { entry: '0x100055578', physical: '/dev/disk539', sectors: 16777218, uid: 502, mountpoints: [] },
        {
          entry: '0x10005557b',
          physical: '/dev/disk540',
          sectors: 131072,
          uid: 502,
          mountpoints: ['/private/var/folders/cowshed/502/t/smoo-ram-fixture-27416'],
        },
      ],
    });
    expect(ramDisksIn('', table)).toEqual({ ok: true, value: [] });
  });

  it('refuses a registry read it cannot read whole, never reporting a disk absent', () => {
    const registry = capturedRegistry();
    // Cut inside the second disk's own properties, then inside its APFS volume: each leaves a property list open.
    for (const end of ['"image-path" = <"ram://131072">', '"BSD Name" = "disk543s1"']) {
      expect(ramDisksIn(registry.slice(0, registry.indexOf(end)), '')).toMatchObject({ ok: false });
    }
    expect(ramDisksIn('ioreg: no such class\n', '')).toMatchObject({ ok: false });
  });

  it('finds only the images whose backing file is on the volume', () => {
    const registry = [
      ...registryDevice('file:///private/var/folders/cowshed/501/ram3/501-a1b2c3/store/main%20copy.dmg', 'disk280'),
      ...registryDevice('file:///private/var/folders/cowshed/501/ram30/x.dmg', 'disk281'),
      ...registryDevice('ram://134217728', 'disk270'),
    ].join('\n');
    expect(imagesBackedUnder(registry, '/private/var/folders/cowshed/501/ram3/')).toEqual([
      '/private/var/folders/cowshed/501/ram3/501-a1b2c3/store/main copy.dmg (/dev/disk280)',
    ]);
  });

  it('finds the mounts that would make removing a lease delete through them', () => {
    const table = [
      '/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)',
      '/dev/disk271s1 on /private/var/folders/cowshed/501/ram3 (apfs, local, nodev, nosuid, journaled, noowners, nobrowse)',
      '/dev/disk290s1 on /private/var/folders/cowshed/501/ram3/501-a1b2c3/checkout (apfs, local, nobrowse, mounted by example)',
      '/dev/disk291s1 on /private/var/folders/cowshed/501/ram3/501-a1b2c3x/other (apfs, local)',
    ].join('\n');
    expect(mountsBelow(table, '/private/var/folders/cowshed/501/ram3/501-a1b2c3/')).toEqual([
      '/private/var/folders/cowshed/501/ram3/501-a1b2c3/checkout',
    ]);
  });

  it('mounts below the per-user cowshed root, where mds keeps no right for it, short enough to hold sockets', () => {
    // sun_path is 104 bytes; a lease directory leaves room for nested fixture paths. mds admits no
    // volume whose mount path contains /var/folders/ (specs/cowshed/01a_apfs.md, "Spotlight media references").
    expect(ramTempPaths({ uid: 501 })).toEqual({
      root: '/private/var/folders/cowshed/501',
      uid: 501,
      mountpoint: '/private/var/folders/cowshed/501/ram3',
      lockFile: '/private/tmp/smoo-ram3-501.lock',
      volumeName: 'smoo-ram3-501',
      staging: '/private/var/folders/cowshed/501/ram3-new',
    });
  });

  it('never shares a volume or a RAM disk size with the checkouts of revisions 1 and 2', () => {
    // Revision 1 used /Volumes/smoo-ram-<uid> and ram://16777216, revision 2 /Volumes/smoo-ram2-<uid> and
    // ram://16777218; a creator's stray reclaim matches uid and size, so equal sizes would let either
    // revision detach the other's attaching disk.
    expect(RAM_TEMP_CAPACITY_BYTES / 512).toBe(16777219);
  });
});

describe('the root the RAM temp volume mounts below', () => {
  it("refuses to create the volume, naming cowshed setup, until the root is the user's 0700 directory", async () => {
    const parent = mkdtempSync(join(realpathSync(tmpdir()), 'rt-root-'));
    const uid = process.getuid?.() ?? 0;
    const cases: { name: string; make: (root: string) => void; detail: string }[] = [
      { name: 'missing', make: () => {}, detail: 'is missing' },
      { name: 'open', make: (root) => mkdirSync(root, { mode: 0o755 }), detail: 'has mode 755, not 700' },
      { name: 'file', make: (root) => writeFileSync(root, ''), detail: 'is not a directory' },
    ];
    try {
      for (const { name, make, detail } of cases) {
        const root = join(parent, name);
        make(root);
        // The volume's paths, all below this root, so no case can find the host's own volume.
        const paths = {
          ...ramTempPaths({ uid }),
          root,
          mountpoint: join(root, 'ram'),
          staging: join(root, 'ram-new'),
          lockFile: join(parent, `${name}.lock`),
        };
        const volume = new RamTempVolume(paths, 512, {
          async run(file) {
            throw new Error(`${file} ran before the root was checked`);
          },
        });
        const error: RamTempError = { kind: 'root-unready', root, detail };
        expect(
          await volume.acquire({
            rootArgv() {
              throw new Error('root refusal must precede command admission');
            },
            async retire() {
              throw new Error('root refusal must precede subtree retirement');
            },
          }),
        ).toEqual({ ok: false, error });
        expect(describeRamTempError(error)).toBe(
          `RAM temp volume: ${root} ${detail}; run "cowshed setup", which makes it a directory of yours with mode 0700`,
        );
      }
    } finally {
      rmSync(parent, { recursive: true, force: true });
    }
  });
});

/** A real `ioreg -r -c IOHDIXHDDriveOutKernel -l -w0` read of two RAM disks on this host, kept beside this test. */
function capturedRegistry(): string {
  return readFileSync(new URL('./ram-temp-registry.txt', import.meta.url), 'utf8');
}
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

describe('detaching a RAM disk an inventory found', () => {
  // The captured registry's 64 MiB disk, as an inventory read it.
  const disk: AttachedRamDisk = {
    entry: '0x10005557b',
    physical: '/dev/disk540',
    sectors: 131072,
    uid: 502,
    mountpoints: [],
  };

  it('binds the detach to the registry entry the inventory read, never forced', () => {
    expect(diskCommand('/usr/bin/hdiutil', ['detach', '/dev/disk540'], 300, { registryEntry: '0x10005557b' })).toEqual({
      args: ['disk-command', 'hdiutil', '--registry-entry', '0x10005557b', '--', 'detach', '/dev/disk540'],
      lifetime: 'stop-wait',
    });
  });

  /** Detach `disk` with `output` as the host's answer, recording every call. */
  async function detachAnswered(output: CommandOutput) {
    const calls: unknown[] = [];
    const result = await detachRamDisk(
      {
        async run(file, args, deadlineMs, binding) {
          calls.push({ file, args, deadlineMs, binding });
          return output;
        },
      },
      disk,
      'hdiutil detach',
    );
    return { result, calls };
  }

  it('issues one bound detach and reports it detached', async () => {
    const { result, calls } = await detachAnswered({ status: 0, stdout: '"disk540" ejected.\n', stderr: '' });
    expect(result).toEqual({ ok: true, value: undefined });
    expect(calls).toEqual([
      {
        file: '/usr/bin/hdiutil',
        args: ['detach', '/dev/disk540'],
        deadlineMs: undefined,
        binding: { registryEntry: '0x10005557b' },
      },
    ]);
  });

  it("names cowshed's refusal as the entry no longer naming the disk, and tries nothing more", async () => {
    const stderr =
      'cowshed: refused executable "/usr/bin/hdiutil", argv ["detach", "/dev/disk540"]: registry entry 0x10005557b is now /dev/disk541, not /dev/disk540; nothing was detached\n';
    const { result, calls } = await detachAnswered({ status: 103, stdout: '', stderr });
    expect(result).toEqual({
      ok: false,
      error: {
        kind: 'provision-failed',
        step: 'hdiutil detach',
        detail: `/dev/disk540 not detached: its registry entry 0x10005557b no longer names it (exit 103: ${stderr.trim()})`,
      },
    });
    expect(calls).toHaveLength(1);
  });

  it("reports any other failure as hdiutil's own", async () => {
    const { result, calls } = await detachAnswered({
      status: 16,
      stdout: '',
      stderr: 'hdiutil: detach failed - Resource busy',
    });
    expect(result).toEqual({
      ok: false,
      error: {
        kind: 'provision-failed',
        step: 'hdiutil detach',
        detail: 'exit 16: hdiutil: detach failed - Resource busy',
      },
    });
    expect(calls).toHaveLength(1);
  });
});
