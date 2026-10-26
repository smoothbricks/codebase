import { describe, expect, it, jest } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import {
  closeSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  watch,
  writeFileSync,
} from 'node:fs';
import { open, readFile, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir, userInfo } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { churnScript, openForProbe, writerRemains } from '../../__tests__/churn-group.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { awaitExit, ownedSocketDirectory } from '../../testing.js';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import { COWSHED_EXECUTABLE_ENV } from './owned-subtree.js';
import { attachRamDisk } from './ram-attach.js';
import {
  type AttachedRamDisk,
  type CommandOutput,
  createHostCommands,
  describeRamTempError,
  detachRamDisk,
  diskCommand,
  type HostCommands,
  imagesBackedUnder,
  leaseDirectoryBytes,
  mountsBelow,
  operationDisk,
  PRODUCTION,
  RAM_TEMP_CAPACITY_BYTES,
  RAM_TMPDIR_MAX_BYTES,
  RAM_TMPDIR_SOCKET_BUDGET_BYTES,
  type RamDisk,
  type RamTempError,
  type RamTempPaths,
  RamTempVolume,
  type Result,
  ramDisksIn,
  ramTempPaths,
  testNamespace,
  volumeDisks,
} from './ram-temp.js';
import { registryDevice, stepOf } from './ram-temp-fixtures.js';

/**
 * The namespace the cases that run fake host commands name their volumes by. They make nothing below it: each case's
 * root, mountpoint, staging directory and lock are temporary directories of its own.
 */
const NAMESPACE = testNamespace(process.pid, 0);

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
    // revision-2 RAM disk, unmounted at that instant, and a 64 MiB disk whose volume mount_apfs placed at a test
    // namespace's ram3. Each disk's volume carries the name the registry gives it, which is whose it is.
    const table = [
      '/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)',
      '/dev/disk543s1 on /private/var/folders/cowshed/502/00l5k00/ram3 (apfs, local, nodev, nosuid, journaled, noowners, nobrowse, mounted by danny)',
    ].join('\n');
    expect(ramDisksIn(capturedRegistry(), table)).toEqual({
      ok: true,
      value: [
        {
          entry: '0x100055578',
          physical: '/dev/disk539',
          sectors: 16777218,
          uid: 502,
          mountpoints: [],
          volumes: [{ name: 'smoo-ram2-502', mountpoints: [] }],
        },
        {
          entry: '0x10005557b',
          physical: '/dev/disk540',
          sectors: 131072,
          uid: 502,
          mountpoints: ['/private/var/folders/cowshed/502/00l5k00/ram3'],
          volumes: [{ name: 'smoo-ram-fixture-27416', mountpoints: ['/private/var/folders/cowshed/502/00l5k00/ram3'] }],
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
    // A lease below it keeps the RAM TMPDIR rule (the budget cases below). mds admits no
    // volume whose mount path contains /var/folders/ (specs/cowshed/01a_apfs.md, "Spotlight media references").
    expect(ramTempPaths({ uid: 501 }, PRODUCTION)).toEqual({
      root: '/private/var/folders/cowshed/501',
      uid: 501,
      mountpoint: '/private/var/folders/cowshed/501/ram3',
      lockFile: '/private/tmp/smoo-ram3-501.lock',
      volumeName: 'smoo-ram3-501',
      staging: '/private/var/folders/cowshed/501/ram3-new',
      leaseBytesMax: 65,
    });
  });

  it('gives a test namespace its own root, volume, lock and staging directory below the per-user root', () => {
    expect(ramTempPaths({ uid: 501 }, testNamespace(94769, 3))).toEqual({
      root: '/private/var/folders/cowshed/501/0214h03',
      uid: 501,
      mountpoint: '/private/var/folders/cowshed/501/0214h03/ram3',
      lockFile: '/private/var/folders/cowshed/501/0214h03/ram3.lock',
      volumeName: 'smoo-ram3-501-0214h03',
      staging: '/private/var/folders/cowshed/501/0214h03/ram3-new',
      leaseBytesMax: 65,
    });
  });

  it('names a test namespace as cowshed_os does: the pid in five base-36 digits, the ordinal in two', () => {
    expect(testNamespace(94769, 3)).toEqual({ kind: 'test', name: '0214h03' });
    expect(testNamespace(36 ** 5 - 1, 36 ** 2 - 1).name).toBe('zzzzzzz');
    expect(() => testNamespace(36 ** 5, 0)).toThrow('pid 60466176 does not fit');
    expect(() => testNamespace(1, 36 ** 2)).toThrow('ordinal 1296 does not fit');
    for (const refused of [-1, 1.5, Number.NaN]) {
      expect(() => testNamespace(refused, 0)).toThrow('does not fit');
      expect(() => testNamespace(1, refused)).toThrow('does not fit');
    }
  });

  it('keeps every lease within the 65-byte RAM TMPDIR rule, leaving 38 bytes of sun_path below it', () => {
    // The layout's worst case: a u32 uid, macOS's largest pid (PID_MAX, 99999) and a test namespace, whose name is
    // always seven bytes (specs/cowshed/01_storage.md, "RAM temp path budget").
    const uid = 2 ** 32 - 1;
    const pid = 99_999;
    const worst = ramTempPaths({ uid }, testNamespace(36 ** 5 - 1, 36 ** 2 - 1));
    expect(leaseDirectoryBytes(worst.mountpoint, pid)).toBe(RAM_TMPDIR_MAX_BYTES);
    expect(Buffer.byteLength(`${worst.mountpoint}/${pid}-aBcDeF`)).toBe(65);
    expect(leaseDirectoryBytes(ramTempPaths({ uid }, PRODUCTION).mountpoint, pid)).toBe(57);
    expect(RAM_TMPDIR_SOCKET_BUDGET_BYTES).toBe(38);
  });

  it("counts a RAM disk as the volume's only by its exact name where mounted, or by an exact record", () => {
    const paths = ramTempPaths({ uid: 501 }, testNamespace(94769, 3));
    const sectors = 131_072;
    const disk = (entry: string, rest: Partial<RamDisk> = {}): RamDisk => ({
      entry,
      physical: '/dev/disk7',
      sectors,
      uid: 501,
      mountpoints: [],
      volumes: [],
      ...rest,
    });
    const volume = (name: string, ...mountpoints: string[]): Partial<RamDisk> => ({
      mountpoints,
      volumes: [{ name, mountpoints }],
    });
    // Its own by its exact name where it is mounted: at its mountpoint, or while it is staged.
    const own = [
      disk('0x1', volume(paths.volumeName, paths.mountpoint)),
      disk('0x2', volume(paths.volumeName, paths.staging)),
    ];
    expect(volumeDisks(own, paths, sectors, () => false)).toEqual(own);
    // Of its user and size, and still not its own: nothing mounted and nothing recorded (a test's disk, another
    // checkout's, its own interrupted attach), a volume of its name that is not mounted, another volume's name at its
    // mountpoint, its name mounted elsewhere too, another user's disk, another size.
    const strangers = [
      disk('0x3'),
      disk('0x4', { volumes: [{ name: paths.volumeName, mountpoints: [] }] }),
      disk('0x5', volume('smoo-ram3-501', paths.mountpoint)),
      disk('0x6', {
        mountpoints: [paths.mountpoint, '/Volumes/other'],
        volumes: [{ name: paths.volumeName, mountpoints: [paths.mountpoint] }],
      }),
      disk('0x7', { uid: 502, ...volume(paths.volumeName, paths.mountpoint) }),
      disk('0x8', { sectors: sectors + 1, ...volume(paths.volumeName, paths.mountpoint) }),
    ];
    expect(volumeDisks(strangers, paths, sectors, () => false)).toEqual([]);
    // An exact record makes a disk its own with nothing mounted, which covers an attach whose format never finished;
    // it makes no other user's disk, no other size's and none with a mount elsewhere its own.
    expect(volumeDisks(strangers, paths, sectors, (candidate) => candidate.entry === '0x3')).toEqual([strangers[0]]);
    expect(
      volumeDisks(strangers, paths, sectors, (candidate) => ['0x6', '0x7', '0x8'].includes(candidate.entry)),
    ).toEqual([]);
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
          ...ramTempPaths({ uid }, NAMESPACE),
          root,
          mountpoint: join(root, 'ram'),
          leaseBytesMax: Number.MAX_SAFE_INTEGER,
          staging: join(root, 'ram-new'),
          lockFile: join(parent, `${name}.lock`),
        };
        const volume = new RamTempVolume(paths, 512, {
          async run(file) {
            throw new Error(`${file} ran before the root was checked`);
          },
          async detachRam() {
            throw new Error('no disk exists');
          },
          async attachRam() {
            throw new Error('a RAM disk was attached before the root was checked');
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

describe('the RAM temp path budget', () => {
  it('refuses a lease longer than the rule before it checks the root or runs a command', async () => {
    // A mountpoint 40 bytes longer than the layout's, below a root this host's user does not have: only the budget
    // refusal can answer first.
    const paths = {
      ...ramTempPaths({ uid: 501 }, NAMESPACE),
      root: '/private/var/folders/cowshed/501',
      mountpoint: `/private/var/folders/cowshed/501/${'x'.repeat(40)}`,
    };
    const volume = new RamTempVolume(
      paths,
      512,
      {
        async run(file) {
          throw new Error(`${file} ran before the lease's length was checked`);
        },
        async detachRam() {
          throw new Error('no disk exists');
        },
        async attachRam() {
          throw new Error("a RAM disk was attached before the lease's length was checked");
        },
      },
      99_999,
    );
    const error: RamTempError = {
      kind: 'lease-path-budget',
      mountpoint: paths.mountpoint,
      pid: 99_999,
      bytes: 86,
      most: 65,
    };
    expect(
      await volume.acquire({
        rootArgv() {
          throw new Error('the budget refusal must precede command admission');
        },
        async retire() {
          throw new Error('the budget refusal must precede subtree retirement');
        },
      }),
    ).toEqual({ ok: false, error });
    expect(describeRamTempError(error)).toBe(
      `RAM temp volume: a lease below ${paths.mountpoint} for pid 99999 would be 86 bytes, past the 65 its layout admits, leaving 38 below it for a socket's names within sun_path (specs/cowshed/01_storage.md, "RAM temp path budget")`,
    );
  });
});

/** A real `ioreg -r -c IOHDIXHDDriveOutKernel -l -w0` read of two RAM disks on this host, kept beside this test. */
function capturedRegistry(): string {
  return readFileSync(new URL('./ram-temp-registry.txt', import.meta.url), 'utf8');
}

/** One RAM disk as `ioreg -r -c IOHDIXHDDriveOutKernel -l -w0` lists it, with its APFS volume when it has one. */
function registryRamDisk(disk: {
  readonly entry: string;
  readonly uid: number;
  readonly sectors: number;
  readonly device: string;
  readonly volume?: string;
}): string {
  return [
    `+-o IOHDIXHDDriveOutKernel@1  <class IOHDIXHDDriveOutKernel, id ${disk.entry}, registered, matched, active, busy 0, retain 1>`,
    '  | {',
    `  |   "image-path" = <"ram://${disk.sectors}">`,
    `  |   "owner-uid" = ${disk.uid}`,
    '  | }',
    '  +-o Apple read/write Media  <class IOMedia, id 0x100000a01, registered, matched, active, busy 0, retain 1>',
    '    | {',
    `    |   "BSD Name" = "${disk.device}"`,
    '    | }',
    ...(disk.volume === undefined
      ? []
      : [
          `    +-o ${disk.volume}@1  <class AppleAPFSVolume, id 0x100000a02, registered, matched, active, busy 0, retain 1>`,
          '      | {',
          `      |   "FullName" = "${disk.volume}"`,
          `      |   "BSD Name" = "${disk.device}s1"`,
          '      | }',
        ]),
    '',
  ].join('\n');
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
      "import { spawn } from 'node:child_process';\nimport { once } from 'node:events';\nimport { closeSync, openSync } from 'node:fs';\nimport { createConnection } from 'node:net';\nimport { dirname, join } from 'node:path';\nimport { createInterface } from 'node:readline';\n\nconst startedAt = performance.now();\nconst argv = process.argv.slice(2);\nif (argv[2] === '--gateway-socket') argv.splice(2, 2);\nconst control = createConnection(argv.at(-1));\nawait once(control, 'connect');\nconst tool = argv[1];\nconst safe = argv[2] === '--deadline-ms';\nconst budget = safe ? Number(argv[3]) : undefined;\nconst native = argv.slice(safe ? 5 : 3);\nconst image = tool === 'hdiutil' || (tool === 'diskutil' && (native[0] === 'image' || native[0] === 'eject'));\nif (argv[0] !== 'disk-command' || (tool !== 'hdiutil' && tool !== 'diskutil') ||\n    argv[safe ? 4 : 2] !== '--' || safe === image ||\n    (safe && (!Number.isSafeInteger(budget) || budget <= 0))) {\n  control.end('invalid facade request\\n');\n  process.exitCode = 2;\n} else {\n  const mode = native[1];\n  const tail = mode === 'exit' ? 'exit 7' : mode === 'signal' ? 'kill -TERM $$' : 'exec sleep 600';\n  const files = image ? [openSync(join(dirname(argv.at(-1)), 'stdout'), 'w'), openSync(join(dirname(argv.at(-1)), 'stderr'), 'w')] : [];\n  const child = spawn('/bin/sh', ['-c', 'printf \"owned stdout\\\\n\"; printf \"owned stderr\\\\n\" >&2; printf ready >&3; ' + tail], {\n    detached: true,\n    stdio: ['ignore', files[0] ?? 'pipe', files[1] ?? 'pipe', 'pipe'],\n  });\n  for (const fd of files) closeSync(fd);\n  const ended = once(child, 'close');\n  if (!image) {\n    child.stdout.on('data', (chunk) => process.stdout.write(chunk));\n    child.stderr.on('data', (chunk) => process.stderr.write(chunk));\n  }\n  let release;\n  const finished = new Promise((resolve) => { release = resolve; });\n  let reaped = false;\n  const commands = createInterface({ input: control });\n  commands.on('line', (line) => {\n    if (line === 'cleanup') {\n      if (!reaped && child.exitCode === null && child.signalCode === null) process.kill(-child.pid, 'SIGKILL');\n      release();\n    } else if (line === 'finish') {\n      release();\n    } else if (line === 'inspect') {\n      process.kill(child.pid, 0);\n      control.write('custody ' + child.pid + '\\n');\n    }\n  });\n  let expired = false;\n  const timer = safe ? setTimeout(() => {\n    expired = true;\n    control.write('deadline ' + budget + '\\n');\n    process.kill(-child.pid, 'SIGKILL');\n  }, Math.max(0, budget - (performance.now() - startedAt))) : undefined;\n  const ready = await new Promise((resolve) => {\n    child.stdio[3].once('data', () => resolve(true));\n    child.stdio[3].once('end', () => resolve(false));\n    child.stdio[3].once('error', () => resolve(false));\n  });\n  if (!ready) throw new Error('owned child ended before readiness');\n  control.write('started ' + process.pid + ' ' + child.pid + '\\n');\n  const [code, signal] = await ended;\n  reaped = true;\n  clearTimeout(timer);\n  control.write('reaped ' + child.pid + ' ' + code + ' ' + signal + '\\n');\n  if (expired) process.stderr.write('owned child ' + child.pid + ' terminated by ' + signal + ' and reaped after ' + budget + ' ms deadline\\n');\n  await finished;\n  commands.close();\n  control.end();\n  await once(control, 'close');\n  if (mode === 'signal') process.kill(process.pid, 'SIGTERM');\n  else process.exitCode = expired ? 1 : code ?? 1;\n}\n",
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
      const result = await createHostCommands(join(directory, 'unused-gateway.sock'), 500).run('/bin/sh', [
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
      const result = await createHostCommands(join(directory, 'unused-gateway.sock'), 500).run('/bin/sh', [
        '-c',
        churnScript(),
        'sh',
        lifetime,
        '/dev/null',
      ]);
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

  it("names its gateway to the disk facade unless it is the host's own, which the facade defaults to", async () => {
    const directory = mkdtempSync(join(realpathSync(tmpdir()), 'rt-facade-argv-'));
    try {
      // The stand-in prints the argv it is given and asks no gateway: only what the facade is told is under test.
      const facade = join(directory, 'cowshed');
      writeFileSync(facade, '#!/bin/sh\nprintf "%s\\n" "$@"\n', { mode: 0o755 });
      const fixtureGateway = join(directory, 'gateway.sock');
      const told = async (gateway: string): Promise<string[]> => {
        const output = await createHostCommands(gateway, 5_000, undefined, facade).run('/usr/sbin/diskutil', [
          'info',
          'x',
        ]);
        expect(output.status).toBe(0);
        return output.stdout.trim().split('\n');
      };
      expect(await told(GATEWAY_SOCKET)).toEqual([
        'disk-command',
        'diskutil',
        '--deadline-ms',
        '5000',
        '--',
        'info',
        'x',
      ]);
      expect(await told(fixtureGateway)).toEqual([
        'disk-command',
        'diskutil',
        '--gateway-socket',
        fixtureGateway,
        '--deadline-ms',
        '5000',
        '--',
        'info',
        'x',
      ]);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('waits past the SAFE deadline for actual owned child reap and the facade terminal exit', async () => {
    const fixture = await ownedDiskFacadeFixture();
    let settled = false;
    const result = createHostCommands(
      join(fixture.directory, 'unused-gateway.sock'),
      undefined,
      undefined,
      fixture.executable,
    )
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
      const result = createHostCommands(
        join(fixture.directory, 'unused-gateway.sock'),
        300,
        undefined,
        fixture.executable,
      ).run(tool, [verb, fixture.socketPath]);
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
        const result = createHostCommands(join(fixture.directory, 'unused-gateway.sock')).run('/usr/sbin/diskutil', [
          'info',
          mode,
          fixture.socketPath,
        ]);
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
      const host = createHostCommands(
        join(directory, 'unused-gateway.sock'),
        BOUNDED_TEST_IDLE_TIMEOUT_MS,
        undefined,
        executable,
      );
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
      const host = createHostCommands(socket, BOUNDED_TEST_IDLE_TIMEOUT_MS);
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
      const host = createHostCommands(socket, BOUNDED_TEST_IDLE_TIMEOUT_MS, 'alone');
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
    volumes: [],
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
        async detachRam(device, registryEntry) {
          calls.push({ device, registryEntry });
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
        device: '/dev/disk540',
        registryEntry: '0x10005557b',
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

/** What a fake gateway does with a line it read: answer it, or never answer and close at the client's EOF. */
type GatewayMove = { readonly answer: string } | 'unanswered';

/**
 * A gateway on an owned socket that serves one connection: it moves on each line the client writes as `move(line)` says,
 * and answers the client's EOF by closing. `body`'s result comes back with what the gateway saw, in order: each line,
 * and `EOF` where the client half-closed or went away.
 */
async function withGateway<T>(
  move: (line: string) => GatewayMove,
  body: (socket: string) => Promise<T>,
): Promise<{ result: T; seen: string[] }> {
  const directory = await ownedSocketDirectory('ra-', 'gw.sock');
  const socket = join(directory, 'gw.sock');
  const seen: string[] = [];
  const closed = Promise.withResolvers<void>();
  const server = createServer({ allowHalfOpen: true }, (client) => {
    let buffered = '';
    client.on('data', (chunk: Buffer) => {
      buffered += chunk.toString('utf8');
      for (let newline = buffered.indexOf('\n'); newline >= 0; newline = buffered.indexOf('\n')) {
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        seen.push(line);
        const next = move(line);
        if (next !== 'unanswered') client.write(next.answer);
      }
    });
    client.on('end', () => {
      seen.push('EOF');
      client.end();
    });
    client.on('error', (error) => seen.push(`error: ${error.message}`));
    client.on('close', () => closed.resolve());
  });
  try {
    const { promise: listening, resolve: listened, reject: failed } = Promise.withResolvers<void>();
    server.once('error', failed);
    server.listen(socket, listened);
    await listening;
    server.off('error', failed);
    const result = await body(socket);
    await closed.promise;
    return { result, seen };
  } finally {
    server.close();
    rmSync(directory, { recursive: true, force: true });
  }
}

const LEASE_REQUEST = '{"op":"ram-lease","sectors":131072}';
const GRANT = '{"ok":true,"ramDisk":{"device":"/dev/disk7","registryEntry":"0x1000b0ab1"}}\n';

describe("the gateway's ram-lease", () => {
  const detachAnswer = JSON.stringify({
    ok: true,
    ramDetach: {
      outcome: 'terminal',
      status: { exit: 0 },
      stdout: 'disk7 ejected\n',
      stderr: '',
      stdoutTruncated: false,
      stderrTruncated: false,
    },
  });
  const ends: {
    name: string;
    end: 'keep' | 'release';
    move: GatewayMove;
    outcome: Result<void, string> | Result<CommandOutput, string>;
    seen: string[];
  }[] = [
    {
      name: 'keeps the disk with one half-closed end line',
      end: 'keep',
      move: { answer: '{"ok":true}\n' },
      outcome: { ok: true, value: undefined },
      seen: [LEASE_REQUEST, '{"end":"keep"}', 'EOF'],
    },
    {
      name: "releases the disk with one half-closed end line and answers the gateway's detach",
      end: 'release',
      move: { answer: `${detachAnswer}\n` },
      outcome: { ok: true, value: { status: 0, stdout: 'disk7 ejected\n', stderr: '' } },
      seen: [LEASE_REQUEST, '{"end":"release"}', 'EOF'],
    },
    {
      name: 'says the lease lost when a restarted gateway never answers its keep',
      end: 'keep',
      move: 'unanswered',
      outcome: {
        ok: false,
        error: expect.stringContaining(
          'nothing is known: the gateway may have handed the disk over or left it to its next startup scan; detach it by its entry',
        ),
      },
      seen: [LEASE_REQUEST, '{"end":"keep"}', 'EOF'],
    },
    {
      name: 'says the lease lost when a restarted gateway never answers its release',
      end: 'release',
      move: 'unanswered',
      outcome: {
        ok: false,
        error: expect.stringContaining('the lease is lost, so nothing is known of a detach'),
      },
      seen: [LEASE_REQUEST, '{"end":"release"}', 'EOF'],
    },
  ];
  for (const row of ends) {
    it(`asks for one size without half-closing, holds the lease open, and ${row.name}`, async () => {
      const { result, seen } = await withGateway(
        (line) => (line === LEASE_REQUEST ? { answer: GRANT } : row.move),
        async (socket) => {
          const leased = await createHostCommands(socket, BOUNDED_TEST_IDLE_TIMEOUT_MS).attachRam(131_072);
          if (!leased.ok) throw new Error(`no lease: ${JSON.stringify(leased.error)}`);
          expect(leased.value).toMatchObject({ device: '/dev/disk7', registryEntry: '0x1000b0ab1' });
          return row.end === 'keep' ? await leased.value.keep() : await leased.value.release();
        },
      );
      expect(result).toEqual(row.outcome);
      // The request is never half-closed: the gateway reads EOF only after the end line, never before it.
      expect(seen).toEqual(row.seen);
    });
  }

  it('names a gateway that predates the operation, a refusal, and an answer without a whole disk and registry entry', async () => {
    const answers = [
      '{"ok":false,"code":"invalid-request","error":"gateway control encoding failed: unknown gateway control operation"}\n',
      '{"ok":false,"code":"rejected","error":"hdiutil attach -nomount ram://131072 ended exit 1: no memory"}\n',
      '{"ok":true,"ramDisk":{"device":"/dev/disk7s1","registryEntry":"0x1000b0ab1"}}\n',
      '{"ok":true,"ramDisk":{"device":"/dev/disk7"}}\n',
    ];
    const outcomes = [];
    for (const answer of answers) {
      const { result, seen } = await withGateway(
        () => ({ answer }),
        (socket) => attachRamDisk(socket, 131_072, BOUNDED_TEST_IDLE_TIMEOUT_MS),
      );
      expect(seen).toEqual([LEASE_REQUEST, 'EOF']);
      outcomes.push(result);
    }
    expect(outcomes).toEqual([
      {
        ok: false,
        error: {
          kind: 'ram-unavailable',
          reason: 'gateway-unsupported',
          detail:
            'gateway predates ram-lease (invalid-request: gateway control encoding failed: unknown gateway control operation)',
        },
      },
      {
        ok: false,
        error: 'the gateway refused ram-lease (rejected: hdiutil attach -nomount ram://131072 ended exit 1: no memory)',
      },
      {
        ok: false,
        error: `the gateway's ram-lease answer names no whole disk and registry entry: ${answers[2]?.trim()}`,
      },
      {
        ok: false,
        error: `the gateway's ram-lease answer names no whole disk and registry entry: ${answers[3]?.trim()}`,
      },
    ]);
  });

  it('closes the lease at its deadline without an end line, leaving the detach to the gateway', async () => {
    const { result, seen } = await withGateway(
      () => 'unanswered',
      (socket) => attachRamDisk(socket, 131_072, 50),
    );
    expect(result).toEqual({
      ok: false,
      error:
        'the gateway answered no ram-lease within 50 ms; this client closed the lease, so the gateway detaches any disk its attach still makes',
    });
    expect(seen).toEqual([LEASE_REQUEST, 'EOF']);
  });

  it('attaches nothing itself when no gateway answers', async () => {
    const directory = await ownedSocketDirectory('ra-', 'gw.sock');
    try {
      const socket = join(directory, 'gw.sock');
      const outcome = await attachRamDisk(socket, 131_072, BOUNDED_TEST_IDLE_TIMEOUT_MS);
      expect(outcome.ok).toBe(false);
      expect(outcome).toMatchObject({ ok: false, error: { kind: 'ram-unavailable', reason: 'gateway-absent' } });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

const noSubtrees = {
  rootArgv(): never {
    throw new Error('a creation that fails admits no command');
  },
  async retire(): Promise<never> {
    throw new Error('a creation that fails settles no dead lease');
  },
};

describe('creating the RAM temp volume', () => {
  it('asks its owner for the disk and never runs an attach of its own', async () => {
    // A root the creation accepts, and paths below it, so nothing here can find the user's volume.
    const parent = mkdtempSync(join(tmpdir(), 'ra-create-'));
    const root = join(parent, 'root');
    mkdirSync(root, { mode: 0o700 });
    const paths = {
      ...ramTempPaths(userInfo(), NAMESPACE),
      root,
      mountpoint: join(root, 'ram'),
      leaseBytesMax: Number.MAX_SAFE_INTEGER,
      staging: join(root, 'ram-new'),
      lockFile: join(parent, 'v.lock'),
    };
    const ran: string[] = [];
    const asked: number[] = [];
    const commands: HostCommands = {
      async run(file, args): Promise<CommandOutput> {
        ran.push([file, ...args].join(' '));
        // The stray inventory finds no RAM disk and nothing mounted; no other command may run before the attach.
        if (file === '/usr/sbin/ioreg' || file === '/sbin/mount') {
          return { status: 0, stdout: '', stderr: '' };
        }
        throw new Error(`${file} ${args.join(' ')} ran: the creation attached a disk of its own`);
      },
      async detachRam() {
        throw new Error('no disk exists');
      },
      async attachRam(sectors) {
        asked.push(sectors);
        return { ok: false, error: 'the owner refused' };
      },
    };
    try {
      const volume = new RamTempVolume(paths, 64 * 1024 * 1024, commands);
      const acquired = await volume.acquire({
        rootArgv() {
          throw new Error('a creation that failed admits no command');
        },
        async retire() {
          throw new Error('a creation that failed settles no dead lease');
        },
      });
      expect(acquired).toEqual({
        ok: false,
        error: { kind: 'provision-failed', step: 'ram-lease', detail: 'the owner refused' },
      });
      expect(asked).toEqual([131_072]);
      expect(ran).toEqual(['/usr/sbin/ioreg -r -c IOHDIXHDDriveOutKernel -l -w0', '/sbin/mount']);
    } finally {
      rmSync(parent, { recursive: true, force: true });
    }
  });

  it('ignores a RAM disk of its user and size that is not its own', async () => {
    const parent = mkdtempSync(join(tmpdir(), 'ra-ignore-'));
    const root = join(parent, 'root');
    mkdirSync(root, { mode: 0o700 });
    const paths = {
      ...ramTempPaths(userInfo(), NAMESPACE),
      root,
      mountpoint: join(root, 'ram'),
      leaseBytesMax: Number.MAX_SAFE_INTEGER,
      staging: join(root, 'ram-new'),
      lockFile: join(parent, 'v.lock'),
    };
    const capacity = 64 * 1024 * 1024;
    const disk = { entry: '0x100000def', uid: paths.uid, sectors: capacity / 512, device: 'disk9' };
    // Not the volume's, though of its user and size: nothing mounted, with a volume of its name on it, and another
    // volume's name where the mount table puts it at the mountpoint.
    const strangers = [
      { registry: registryRamDisk({ ...disk, volume: paths.volumeName }), table: '' },
      {
        registry: registryRamDisk({ ...disk, volume: 'smoo-ram3-0' }),
        table: `/dev/disk9s1 on ${paths.mountpoint} (apfs, local, nobrowse, noowners)`,
      },
    ];
    try {
      for (const { registry, table } of strangers) {
        const asked: number[] = [];
        const commands: HostCommands = {
          async run(file, args): Promise<CommandOutput> {
            if (file === '/usr/sbin/ioreg') return { status: 0, stdout: registry, stderr: '' };
            if (file === '/sbin/mount') return { status: 0, stdout: table, stderr: '' };
            throw new Error(`${file} ${args.join(' ')} ran: a disk that is not the volume's was acted on`);
          },
          async detachRam() {
            throw new Error("a disk that is not the volume's was detached");
          },
          async attachRam(sectors) {
            asked.push(sectors);
            return { ok: false, error: 'the owner refused' };
          },
        };
        expect(await new RamTempVolume(paths, capacity, commands).acquire(noSubtrees)).toEqual({
          ok: false,
          error: { kind: 'provision-failed', step: 'ram-lease', detail: 'the owner refused' },
        });
        expect(asked).toEqual([131_072]);
      }
    } finally {
      rmSync(parent, { recursive: true, force: true });
    }
  });

  it('names a disk mounted at its mountpoint under its exact name, and detaches it only with a receipt', async () => {
    const parent = mkdtempSync(join(tmpdir(), 'ra-named-'));
    const root = join(parent, 'root');
    mkdirSync(root, { mode: 0o700 });
    const paths = {
      ...ramTempPaths(userInfo(), NAMESPACE),
      root,
      mountpoint: join(root, 'ram'),
      leaseBytesMax: Number.MAX_SAFE_INTEGER,
      staging: join(root, 'ram-new'),
      lockFile: join(parent, 'v.lock'),
    };
    mkdirSync(paths.mountpoint);
    const capacity = 64 * 1024 * 1024;
    const registry = registryRamDisk({
      entry: '0x100000def',
      uid: paths.uid,
      sectors: capacity / 512,
      device: 'disk9',
      volume: paths.volumeName,
    });
    const commands: HostCommands = {
      async run(file, args): Promise<CommandOutput> {
        if (file === '/usr/sbin/ioreg') {
          return { status: 0, stdout: args.includes('AppleDiskImageDevice') ? '' : registry, stderr: '' };
        }
        if (file === '/sbin/mount') {
          return {
            status: 0,
            stdout: `/dev/disk9s1 on ${paths.mountpoint} (apfs, local, nobrowse, noowners)`,
            stderr: '',
          };
        }
        throw new Error(`${file} ${args.join(' ')} ran: a disk with no receipt was acted on`);
      },
      async detachRam() {
        throw new Error('a disk with no receipt was detached');
      },
      async attachRam() {
        throw new Error('a creation that stops at a disk it cannot prove its own attaches none');
      },
    };
    try {
      expect(await new RamTempVolume(paths, capacity, commands).acquire(noSubtrees)).toEqual({
        ok: false,
        error: {
          kind: 'ram-unavailable',
          reason: 'unproven-owner',
          detail: '/dev/disk9 has no exact gateway creation receipt; no reclaim or detach admitted',
        },
      });
    } finally {
      rmSync(parent, { recursive: true, force: true });
    }
  });
});

describe('observing the RAM temp volume', () => {
  it('takes a volume detached while its marker is read as no volume, and creates one, never failing the lease', async () => {
    // Another caller's last release detaches the volume outside the lock while this acquire observes it: the marker is
    // a FIFO, so the acquire's read ends only when the test closes its writer, after the marker is gone, which is what
    // the detach leaves between the marker's read and the identity's lstat.
    const parent = mkdtempSync(join(tmpdir(), 'ra-gone-'));
    const root = join(parent, 'root');
    mkdirSync(root, { mode: 0o700 });
    const paths = {
      ...ramTempPaths(userInfo(), NAMESPACE),
      root,
      mountpoint: join(root, 'ram'),
      leaseBytesMax: Number.MAX_SAFE_INTEGER,
      staging: join(root, 'ram-new'),
      lockFile: join(parent, 'v.lock'),
    };
    mkdirSync(paths.mountpoint);
    const marker = join(paths.mountpoint, '.smoo-ram');
    execFileSync('mkfifo', [marker]);
    const asked: number[] = [];
    const commands: HostCommands = {
      async run(file, args): Promise<CommandOutput> {
        if (file === '/usr/sbin/ioreg' || file === '/sbin/mount') {
          return { status: 0, stdout: '', stderr: '' };
        }
        throw new Error(`${[file, ...args].join(' ')} ran: only the creation inventory runs before the attach`);
      },
      async detachRam() {
        throw new Error('no disk exists');
      },
      async attachRam(sectors) {
        asked.push(sectors);
        return { ok: false, error: 'the owner refused' };
      },
    };
    try {
      const acquired = new RamTempVolume(paths, 64 * 1024 * 1024, commands).acquire(noSubtrees);
      // Opens once the acquire has opened the marker for its read.
      const writer = await open(marker, 'w');
      await writer.writeFile(
        JSON.stringify({ version: 1, volumeName: paths.volumeName, owner: 'gateway', registryEntry: '0x100000abc' }),
      );
      rmSync(marker);
      await writer.close();
      // Gone is no volume: the acquire creates one, which reaches the attach its owner refuses.
      expect(await acquired).toEqual({
        ok: false,
        error: { kind: 'provision-failed', step: 'ram-lease', detail: 'the owner refused' },
      });
      expect(asked).toEqual([131_072]);
    } finally {
      rmSync(parent, { recursive: true, force: true });
    }
  });
});

/** The registry entry the stand-in gives its attached RAM disk: the identity a detach of its own must name. */
const STAND_IN_REGISTRY_ENTRY = '0x1000b0ab1';

/** One RAM disk as ioreg -r -c IOHDIXHDDriveOutKernel -l -w0 lists it: its whole disk and its image. */
function ramDiskRecord(name: string, sectors: number, uid: number): string {
  return [
    `+-o IOHDIXHDDriveOutKernel <class IOHDIXHDDriveOutKernel, id ${STAND_IN_REGISTRY_ENTRY}, registered, matched, active, busy 0, retain 1>`,
    '{',
    `  "image-path" = <"ram://${sectors}">`,
    `  "owner-uid" = ${uid}`,
    `  "BSD Name" = "${name}"`,
    '}',
    '',
  ].join('\n');
}

/** How the stand-in's gateway answers the lease it grants, and whose disk its registry lists. */
interface StandInGateway {
  /** The uid the registry lists the attached disk under: any but the volume's fails the attach identity. */
  readonly owner: number;
  /** Its answer to the lease's keep. */
  readonly keep: Result<void, string>;
  /** Its release finds the lease lost to a gateway restart, so it answers no detach. */
  readonly lost: boolean;
}

/**
 * A recording HostCommands stand-in for one provisioning run. Its gateway attaches one RAM disk under a lease, and it
 * answers ioreg, mount, diskutil, mount_apfs and umount from the disk and the mounts it holds, so provisioning's own
 * checks run unchanged. Its mounts are no separate device, so the marker leaves with the staging mount and the
 * mountpoint never holds the marked volume: a provisioning that succeeds on it ends at the lease the creation then takes.
 * It fails the injected step by its exit or by a throw, records every step and every end of the lease in order, and
 * every detach the caller runs itself.
 */
function provisioningStandIn(
  paths: RamTempPaths,
  injected: { readonly step: string; readonly by: 'exit' | 'throw' } | null,
  gateway: StandInGateway,
) {
  const calls: { file: string; args: readonly string[] }[] = [];
  const steps: string[] = [];
  const detaches: { device: string; registryEntry: string }[] = [];
  const kernel: { disk: { name: string; sectors: number } | null; mounts: Map<string, string> } = {
    disk: null,
    mounts: new Map(),
  };
  const detach = (): CommandOutput => {
    kernel.disk = null;
    kernel.mounts.clear();
    return { status: 0, stdout: '', stderr: '' };
  };
  const commands: HostCommands = {
    async run(file, args) {
      calls.push({ file, args });
      const step = stepOf(paths, file, args);
      if (step !== null) steps.push(step);
      if (step !== null && step === injected?.step) {
        if (injected.by === 'throw') throw new Error('injected throw');
        return { status: 1, stdout: '', stderr: 'injected' };
      }
      if (file === '/usr/sbin/ioreg') {
        const disk = args.includes('IOHDIXHDDriveOutKernel') ? kernel.disk : null;
        return {
          status: 0,
          stdout: disk === null ? '' : ramDiskRecord(disk.name, disk.sectors, gateway.owner),
          stderr: '',
        };
      }
      if (file === '/sbin/mount') {
        const table = [...kernel.mounts].map(([point, volume]) => `/dev/${volume} on ${point} (apfs, local, nobrowse)`);
        return { status: 0, stdout: table.join('\n'), stderr: '' };
      }
      if (file === '/usr/sbin/diskutil' && args[1] === 'createContainer') {
        return { status: 0, stdout: 'Disk from APFS operation: disk902\n', stderr: '' };
      }
      if (file === '/usr/sbin/diskutil' && args[1] === 'addVolume') {
        return { status: 0, stdout: 'Disk from APFS operation: disk902s1\n', stderr: '' };
      }
      if (file === '/sbin/mount_apfs') {
        const [, , device = '', point = ''] = args;
        kernel.mounts.set(point, device.slice('/dev/'.length));
        return { status: 0, stdout: '', stderr: '' };
      }
      if (file === '/sbin/umount') {
        const [point = ''] = args;
        if (!kernel.mounts.delete(point)) return { status: 1, stdout: '', stderr: `${point} is not mounted` };
        // The marker leaves the directory with the volume, so the unmounted directory is bare again.
        rmSync(join(point, '.smoo-ram'), { force: true });
        return { status: 0, stdout: '', stderr: '' };
      }
      throw new Error(`${file} ${args.join(' ')} is not answered by the provisioning stand-in`);
    },
    async attachRam(sectors) {
      kernel.disk = { name: 'disk901', sectors };
      return {
        ok: true,
        value: {
          device: '/dev/disk901',
          registryEntry: STAND_IN_REGISTRY_ENTRY,
          async keep() {
            steps.push('keep');
            return gateway.keep;
          },
          async release() {
            steps.push('release');
            return gateway.lost
              ? { ok: false, error: 'the gateway answered no release: the lease is lost' }
              : { ok: true, value: detach() };
          },
        },
      };
    },
    async detachRam(device, registryEntry) {
      detaches.push({ device, registryEntry });
      return detach();
    },
  };
  return { commands, calls, steps, detaches };
}

describe('the lease on the RAM disk a creation attaches', () => {
  const uid = userInfo().uid;
  const granted: StandInGateway = { owner: uid, keep: { ok: true, value: undefined }, lost: false };
  const cases: {
    name: string;
    injected: { step: string; by: 'exit' | 'throw' } | null;
    gateway: StandInGateway;
    /** The creation's failure, given the volume's mountpoint. */
    error: (mountpoint: string) => { step: string; detail: string };
    steps: string[];
    detaches: { device: string; registryEntry: string }[];
  }[] = [
    {
      name: 'is released, and never detached by the creator, when the staging mount fails',
      injected: { step: 'mount_apfs', by: 'exit' },
      gateway: granted,
      error: () => ({ step: 'mount_apfs (staging)', detail: 'exit 1: injected' }),
      steps: ['mount_apfs', 'release'],
      detaches: [],
    },
    {
      name: 'is released, and never detached by the creator, when the mountpoint mount fails',
      injected: { step: 'mount', by: 'exit' },
      gateway: granted,
      error: () => ({ step: 'mount_apfs (mountpoint)', detail: 'exit 1: injected' }),
      steps: ['mount_apfs', 'unmount', 'mount', 'release'],
      detaches: [],
    },
    {
      name: 'is released once, and never detached by the creator, when a provisioning step throws',
      injected: { step: 'mount', by: 'throw' },
      gateway: granted,
      error: () => ({ step: 'provision', detail: 'injected throw' }),
      steps: ['mount_apfs', 'unmount', 'mount', 'release'],
      detaches: [],
    },
    {
      name: 'is released when the attached disk is not the unique registry identity of the attach',
      injected: null,
      gateway: { ...granted, owner: uid + 1 },
      error: () => ({
        step: 'attach identity',
        detail:
          '/dev/disk901 is not the unique registry identity of this gateway attach; no provisioning or guessed detach',
      }),
      steps: ['release'],
      detaches: [],
    },
    {
      name: 'is kept exactly once, after the volume is mounted at its mountpoint',
      injected: null,
      gateway: granted,
      // The stand-in's mounts are no separate device: the kept volume is no marked volume to the lease taken next.
      error: (mountpoint) => ({ step: 'create', detail: `${mountpoint} is not the marked volume once provisioned` }),
      steps: ['mount_apfs', 'unmount', 'mount', 'keep'],
      detaches: [],
    },
    {
      name: 'is detached by the creator by its exact identity when keep fails and its release finds the lease lost',
      injected: null,
      gateway: {
        owner: uid,
        keep: { ok: false, error: 'the gateway answered no keep: the lease is lost' },
        lost: true,
      },
      error: () => ({
        step: 'ram-lease keep',
        detail: 'the gateway answered no keep: the lease is lost; the gateway answered no release: the lease is lost',
      }),
      steps: ['mount_apfs', 'unmount', 'mount', 'keep', 'release'],
      detaches: [{ device: '/dev/disk901', registryEntry: STAND_IN_REGISTRY_ENTRY }],
    },
  ];
  for (const row of cases) {
    it(row.name, async () => {
      const parent = mkdtempSync(join(tmpdir(), 'ra-lease-'));
      try {
        const root = join(parent, 'root');
        mkdirSync(root, { mode: 0o700 });
        const paths: RamTempPaths = {
          ...ramTempPaths(userInfo(), NAMESPACE),
          root,
          mountpoint: join(root, 'ram'),
          leaseBytesMax: Number.MAX_SAFE_INTEGER,
          staging: join(root, 'ram-new'),
          lockFile: join(parent, 'v.lock'),
        };
        const standIn = provisioningStandIn(paths, row.injected, row.gateway);
        const acquired = await new RamTempVolume(paths, RAM_TEMP_CAPACITY_BYTES, standIn.commands).acquire(noSubtrees);
        const detail = JSON.stringify({ acquired, steps: standIn.steps, calls: standIn.calls }, null, 2);
        expect(acquired, detail).toEqual({
          ok: false,
          error: { kind: 'provision-failed', ...row.error(paths.mountpoint) },
        });
        expect(standIn.steps, detail).toEqual(row.steps);
        expect(standIn.detaches, detail).toEqual(row.detaches);
        expect(existsSync(paths.staging), detail).toBe(false);
        // Only a kept volume stays mounted; every ended attempt removes its mountpoint.
        expect(existsSync(paths.mountpoint), detail).toBe(row.steps.at(-1) === 'keep');
      } finally {
        rmSync(parent, { recursive: true, force: true });
      }
    });
  }
});

/** No native disk command runs: the only child is this owned facade, whose effect and exit have separate controls. */
async function detachCustodyFixture(mode: 'stray' | 'rollback' = 'stray', boundMs = 300) {
  const directory = await ownedSocketDirectory('rc-', 's.sock');
  const socketPath = join(directory, 's.sock');
  const registry = join(directory, 'registry');
  const executable = join(directory, 'cowshed');
  const root = join(directory, 'root');
  mkdirSync(root, { mode: 0o700 });
  const paths = {
    ...ramTempPaths(userInfo(), NAMESPACE),
    root,
    mountpoint: join(root, 'ram'),
    leaseBytesMax: Number.MAX_SAFE_INTEGER,
    staging: join(root, 'ram-new'),
    lockFile: join(directory, 'v.lock'),
  };
  const capacity = 131_072 * 512;
  const entry = '0x100000abc';
  const image = [
    `+-o IOHDIXHDDriveOutKernel <class IOHDIXHDDriveOutKernel, id ${entry}, registered, matched, active, busy 0, retain 1>`,
    '{',
    '  "image-path" = <"ram://131072">',
    `  "owner-uid" = ${paths.uid}`,
    '  "BSD Name" = "disk7"',
    '}',
    '',
  ].join('\n');
  await writeFile(registry, mode === 'stray' ? image : '');
  const connected = Promise.withResolvers<Socket>();
  let connection: Socket | undefined;
  const server = createServer((socket) => {
    connection = socket;
    connected.resolve(socket);
  });
  const listening = Promise.withResolvers<void>();
  server.once('error', listening.reject);
  server.listen(socketPath, listening.resolve);
  await listening.promise;
  server.off('error', listening.reject);
  const session = connected.promise.then((socket) => ({
    socket,
    lines: createInterface({ input: socket })[Symbol.asyncIterator](),
  }));
  const expected = ['disk-command', 'hdiutil', '--registry-entry', entry, '--', 'detach', '/dev/disk7'];
  await writeFile(
    executable,
    `#!${process.execPath}\n` +
      `import { once } from 'node:events';
import { writeFile } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { createInterface } from 'node:readline';
const args = process.argv.slice(2);
if (args[2] === '--gateway-socket') {
  if (args[3] !== ${JSON.stringify(join(directory, 'unused-gateway.sock'))}) throw new Error('foreign gateway route');
  args.splice(2, 2);
}
if (JSON.stringify(args) !== ${JSON.stringify(JSON.stringify(expected))}) throw new Error('unexpected disk argv: ' + JSON.stringify(args));
const socket = createConnection(${JSON.stringify(socketPath)});
await once(socket, 'connect');
const lines = createInterface({ input: socket });
socket.write('ready ' + process.pid + '\\n');
for await (const command of lines) {
  if (command === 'inspect') socket.write('alive\\n');
  else if (command === 'complete') {
    await writeFile(${JSON.stringify(registry)}, '');
    socket.write('effect\\n');
  } else if (command === 'finish' || command === 'cleanup' || command === 'wrapper-failure') {
    if (command === 'wrapper-failure') {
      process.stderr.write('owned wrapper wait failed; native terminal status unavailable\\n');
      process.exitCode = 100;
    }
    if (command === 'finish') {
      process.stdout.write('owned terminal stdout\\n');
      process.stderr.write('owned terminal stderr\\n');
    }
    socket.end('exiting\\n');
    break;
  } else if (command === 'signal') process.kill(process.pid, 'SIGTERM');
  else throw new Error('unexpected fixture control: ' + command);
}
lines.close();
socket.end();
`,
    { mode: 0o755 },
  );
  const host = createHostCommands(join(directory, 'unused-gateway.sock'), 300, 'shared', executable);
  const output = Promise.withResolvers<CommandOutput>();
  const calls: string[] = [];
  let attaches = 0;
  let recorded = mode === 'stray';
  const commands: HostCommands = {
    async run(file, args, _deadlineMs, binding) {
      calls.push([file, ...args].join(' '));
      if (file === '/usr/sbin/ioreg') return { status: 0, stdout: await readFile(registry, 'utf8'), stderr: '' };
      if (file === '/sbin/mount') return { status: 0, stdout: '', stderr: '' };
      if (mode === 'rollback' && file === '/usr/sbin/diskutil')
        return { status: 1, stdout: '', stderr: 'owned provisioning failure' };
      if (file !== '/usr/bin/hdiutil') throw new Error(`unexpected fixture command: ${file}`);
      // The production caller's bound is the subject; the scripted child cannot complete until commanded.
      const result = await host.run(file, args, boundMs, binding);
      output.resolve(result);
      return result;
    },
    ownsRamDisk: (disk) =>
      recorded && disk.entry === entry && disk.physical === '/dev/disk7' && disk.sectors === capacity / 512,
    detachRam: (device, registryEntry) =>
      commands.run('/usr/bin/hdiutil', ['detach', device], undefined, { registryEntry }),
    async attachRam() {
      attaches += 1;
      if (mode === 'rollback') {
        recorded = true;
        await writeFile(registry, image);
      }
      if (mode !== 'rollback') return { ok: false, error: 'owned attach admission reached' };
      // A gateway that restarted under the lease answers no release: the rollback is this caller's own detach.
      return {
        ok: true,
        value: {
          device: '/dev/disk7',
          registryEntry: entry,
          async keep() {
            throw new Error('a provisioning that failed keeps no lease');
          },
          async release() {
            return { ok: false, error: 'the gateway answered no release: the lease is lost' };
          },
        },
      };
    },
  };
  const subtrees = {
    rootArgv(): never {
      throw new Error('no command is admitted by this scripted volume');
    },
    async retire(): Promise<never> {
      throw new Error('no subtree exists on this scripted volume');
    },
  };
  let pid: number | undefined;
  async function event() {
    const { lines } = await session;
    const next = await lines.next();
    if (next.done) throw new Error('scripted facade closed before its event');
    return next.value;
  }
  return {
    directory,
    socketPath,
    executable,
    registry,
    paths,
    capacity,
    entry,
    image,
    commands,
    subtrees,
    calls,
    output: output.promise,
    attaches: () => attaches,
    volume: () => new RamTempVolume(paths, capacity, commands),
    ledger: () => JSON.parse(readFileSync(`${paths.lockFile}.ledger`, 'utf8')),
    event,
    async ready() {
      const match = /^ready (\d+)$/.exec(await event());
      if (match === null) throw new Error('scripted facade did not report readiness');
      pid = Number(match[1]);
      return pid;
    },
    async send(command: 'inspect' | 'complete' | 'finish' | 'signal' | 'wrapper-failure') {
      (await session).socket.write(`${command}\n`);
    },
    async cleanup() {
      try {
        if (connection !== undefined && !connection.destroyed) {
          const closed = once(connection, 'close');
          if (!connection.writableEnded) connection.write('cleanup\n');
          await closed;
        }
        if (pid !== undefined)
          await awaitExit([{ pid, ppid: process.pid, stat: 'S', command: executable }], 'scripted detach facade');
      } finally {
        const closed = Promise.withResolvers<void>();
        server.close((error) => (error ? closed.reject(error) : closed.resolve()));
        await closed.promise;
        rmSync(directory, { recursive: true, force: true });
      }
    },
  };
}

/** Publication is a rename; observe that event instead of sleeping for a late completion callback. */
function clearedDetachLedger(directory: string, ledgerPath: string) {
  const cleared = Promise.withResolvers<void>();
  const watcher = watch(directory, (_event, filename) => {
    if (filename !== 'v.lock.ledger') return;
    try {
      const ledger = JSON.parse(readFileSync(ledgerPath, 'utf8'));
      if (ledger.version === 1 && ledger.lifecycle === null) cleared.resolve();
    } catch (error) {
      cleared.reject(error);
    }
  });
  return { promise: cleared.promise, close: () => watcher.close() };
}

describe('RAM detach custody after a bounded wait', () => {
  it('fences the next lease while the scripted child lives, without fencing an unrelated volume', async () => {
    const fixture = await detachCustodyFixture();
    jest.useFakeTimers();
    try {
      const acquiring = fixture.volume().acquire(fixture.subtrees);
      const pid = await fixture.ready();
      jest.advanceTimersByTime(300);
      const failed = await acquiring;
      jest.useRealTimers();
      expect(failed.ok).toBe(false);
      expect(JSON.stringify(failed)).toContain('left running, not killed');
      expect(fixture.ledger()).toMatchObject({
        version: 2,
        lifecycle: {
          kind: 'create',
          pending: {
            physical: '/dev/disk7',
            registryEntry: fixture.entry,
            phase: 'reported',
            facadePid: pid,
          },
        },
        claims: [],
      });
      // Version 2 is the current fail-closed wire; historical-reader behavior is not inferred from this field.
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain('no lease admitted');
      expect(fixture.attaches()).toBe(0);
      await fixture.send('inspect');
      expect(await fixture.event()).toBe('alive');

      let unrelatedAdmitted = false;
      const unrelated = new RamTempVolume(
        {
          ...fixture.paths,
          lockFile: join(fixture.directory, 'other.lock'),
          mountpoint: join(fixture.paths.root, 'other'),
          staging: join(fixture.paths.root, 'other-new'),
        },
        fixture.capacity,
        {
          async run(file) {
            if (file !== '/usr/sbin/ioreg' && file !== '/sbin/mount')
              throw new Error(`unexpected unrelated command: ${file}`);
            return { status: 0, stdout: '', stderr: '' };
          },
          async detachRam() {
            throw new Error('no unrelated disk');
          },
          async attachRam() {
            unrelatedAdmitted = true;
            return { ok: false, error: 'unrelated admission reached' };
          },
        },
      );
      expect(JSON.stringify(await unrelated.acquire(fixture.subtrees))).toContain('unrelated admission reached');
      expect(unrelatedAdmitted).toBe(true);

      await writeFile(fixture.registry, 'incomplete registry');
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain(
        'not an IOHDIXHDDriveOutKernel entry',
      );
      expect(fixture.ledger().version).toBe(2);
      await writeFile(fixture.registry, fixture.image);
      const cleared = clearedDetachLedger(fixture.directory, `${fixture.paths.lockFile}.ledger`);
      try {
        await fixture.send('complete');
        expect(await fixture.event()).toBe('effect');
        await fixture.send('finish');
        expect(await fixture.event()).toBe('exiting');
        const output = await fixture.output;
        expect(output.pending).toBeDefined();
        expect(await output.pending?.completion).toEqual({
          status: 0,
          stdout: 'owned terminal stdout\n',
          stderr: 'owned terminal stderr\n',
        });
        await cleared.promise;
      } finally {
        cleared.close();
      }
      expect(fixture.ledger()).toEqual({ version: 1, lifecycle: null, claims: [], held: [] });
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain(
        'owned attach admission reached',
      );
      expect(fixture.attaches()).toBe(1);
    } finally {
      jest.useRealTimers();
      await fixture.cleanup();
    }
  });

  it('keeps a dead caller fenced until its bound registry incarnation disappears, even with its facade still alive', async () => {
    const fixture = await detachCustodyFixture();
    const script = join(fixture.directory, 'caller.mjs');
    await writeFile(
      script,
      `import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import { createHostCommands, RamTempVolume } from ${JSON.stringify(new URL('../../../dist/executors/bounded-exec/ram-temp.js', import.meta.url).href)};
const paths = ${JSON.stringify(fixture.paths)};
const host = createHostCommands(${JSON.stringify(join(fixture.directory, 'unused-gateway.sock'))}, 300, 'shared', ${JSON.stringify(fixture.executable)});
const commands = {
  async run(file, args, deadline, binding) {
    if (file === '/usr/sbin/ioreg') return { status: 0, stdout: await readFile(${JSON.stringify(fixture.registry)}, 'utf8'), stderr: '' };
    if (file === '/sbin/mount') return { status: 0, stdout: '', stderr: '' };
    if (file !== '/usr/bin/hdiutil') throw new Error('unexpected child command: ' + file);
    return host.run(file, args, 300, binding);
  },
  async attachRam() { throw new Error('the bounded detach must precede attach admission'); },
  ownsRamDisk: (disk) => disk.entry === ${JSON.stringify(fixture.entry)} && disk.physical === '/dev/disk7',
  detachRam: (device, registryEntry) => commands.run('/usr/bin/hdiutil', ['detach', device], undefined, { registryEntry }),
};
const volume = new RamTempVolume(paths, ${fixture.capacity}, commands);
const result = await volume.acquire({ rootArgv() { throw new Error('no subtree'); }, async retire() { throw new Error('no subtree'); } });
console.log(JSON.stringify(result));
process.stdin.resume();
await once(process.stdin, 'end');
`,
    );
    const caller = spawn('node', [script], { stdio: ['pipe', 'pipe', 'pipe'] });
    const ended = once(caller, 'close');
    const lines = createInterface({ input: caller.stdout });
    // The caller can publish its bounded result before the independent facade reports readiness.
    const receipt = lines[Symbol.asyncIterator]().next();
    const errors: Buffer[] = [];
    caller.stderr.on('data', (chunk: Buffer) => errors.push(chunk));
    try {
      const facade = await Promise.race([
        fixture.ready(),
        ended.then(([code, signal]) => {
          throw new Error(
            `owned caller exited before its facade became ready (${code}/${signal}): ${Buffer.concat(errors).toString()}`,
          );
        }),
      ]);
      const next = await receipt;
      if (next.done)
        throw new Error(`owned caller stdout ended without a receipt: ${Buffer.concat(errors).toString()}`);
      expect(next.value).toContain('left running, not killed');
      const before = fixture.ledger();
      expect(before.lifecycle.owner.pid).toBe(caller.pid);
      if (caller.pid === undefined) throw new Error('owned caller has no pid');
      caller.kill('SIGKILL');
      expect(await ended).toEqual([null, 'SIGKILL']);
      expect(Buffer.concat(errors).toString()).toBe('');
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain('no lease admitted');
      expect(fixture.ledger()).toEqual(before);
      expect(() => process.kill(facade, 0)).not.toThrow();
      expect(fixture.attaches()).toBe(0);
      await fixture.send('complete');
      expect(await fixture.event()).toBe('effect');
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain(
        'owned attach admission reached',
      );
      expect(fixture.attaches()).toBe(1);
      expect(fixture.ledger()).toEqual({ version: 1, lifecycle: null, claims: [], held: [] });
      // The registry witness permits admission while this exact old facade still waits for its exit control.
      await fixture.send('inspect');
      expect(await fixture.event()).toBe('alive');
    } finally {
      lines.close();
      if (caller.exitCode === null && caller.signalCode === null) caller.kill('SIGKILL');
      await ended;
      await fixture.cleanup();
    }
  });

  it('retains captured provisioning rollback custody until terminal receipt or exact entry absence', async () => {
    const fixture = await detachCustodyFixture('rollback');
    jest.useFakeTimers();
    try {
      const acquiring = fixture.volume().acquire(fixture.subtrees);
      await fixture.ready();
      jest.advanceTimersByTime(300);
      expect(JSON.stringify(await acquiring)).toContain('owned provisioning failure');
      jest.useRealTimers();
      expect(fixture.ledger()).toMatchObject({
        version: 2,
        lifecycle: { pending: { registryEntry: fixture.entry, phase: 'reported' } },
      });
      const afterDeath = new RamTempVolume(fixture.paths, fixture.capacity, fixture.commands, process.pid, () => false);
      expect(JSON.stringify(await afterDeath.acquire(fixture.subtrees))).toContain(
        'no lease admitted until terminal completion',
      );
      expect(fixture.attaches()).toBe(1);
      const cleared = clearedDetachLedger(fixture.directory, `${fixture.paths.lockFile}.ledger`);
      try {
        await fixture.send('finish');
        expect(await fixture.event()).toBe('exiting');
        expect((await (await fixture.output).pending?.completion)?.status).toBe(0);
        await cleared.promise;
      } finally {
        cleared.close();
      }
      expect(fixture.ledger()).toEqual({ version: 1, lifecycle: null, claims: [], held: [] });
      expect(fixture.calls.filter((call) => call.startsWith('/usr/bin/hdiutil'))).toEqual([
        '/usr/bin/hdiutil detach /dev/disk7',
      ]);
    } finally {
      jest.useRealTimers();
      await fixture.cleanup();
    }
  });

  it('never mistakes a signalled facade for a terminal disk-child receipt', async () => {
    const fixture = await detachCustodyFixture();
    try {
      const acquiring = fixture.volume().acquire(fixture.subtrees);
      await fixture.ready();
      await fixture.send('signal');
      expect((await acquiring).ok).toBe(false);
      expect(await (await fixture.output).pending?.completion).toBeNull();
      expect(fixture.ledger().version).toBe(2);
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain('no lease admitted');
      expect(fixture.attaches()).toBe(0);
    } finally {
      await fixture.cleanup();
    }
  });

  it('retains authority on wrapper failure both before and after the caller stops waiting', async () => {
    for (const bounded of [false, true]) {
      const fixture = await detachCustodyFixture();
      jest.useFakeTimers();
      try {
        const acquiring = fixture.volume().acquire(fixture.subtrees);
        await fixture.ready();
        if (bounded) {
          jest.advanceTimersByTime(300);
          expect((await acquiring).ok).toBe(false);
        }
        await fixture.send('wrapper-failure');
        expect(await fixture.event()).toBe('exiting');
        expect((await acquiring).ok).toBe(false);
        jest.useRealTimers();
        const output = await fixture.output;
        expect(output.status).toBe(bounded ? 1 : 100);
        expect(await output.pending?.completion).toBeNull();
        expect(fixture.ledger().version).toBe(2);
        expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain('no lease admitted');
        expect(fixture.attaches()).toBe(0);
      } finally {
        jest.useRealTimers();
        await fixture.cleanup();
      }
    }
  });

  it('preserves other claim owners when exact registry absence clears pending custody', async () => {
    const fixture = await detachCustodyFixture();
    const claim = {
      name: `${process.pid}-abc123`,
      identity: 'owned-lease-identity',
      owner: { pid: process.pid, token: 'another-live-operation' },
    };
    try {
      await writeFile(fixture.registry, '');
      await writeFile(
        `${fixture.paths.lockFile}.ledger`,
        JSON.stringify({
          version: 2,
          lifecycle: {
            kind: 'detach',
            owner: { pid: process.pid, token: 'original-detach' },
            pending: { physical: '/dev/disk7', registryEntry: fixture.entry, phase: 'reported', facadePid: null },
          },
          claims: [claim],
          held: [],
        }),
      );
      expect(JSON.stringify(await fixture.volume().acquire(fixture.subtrees))).toContain(
        'owned attach admission reached',
      );
      expect(fixture.ledger()).toEqual({ version: 1, lifecycle: null, claims: [claim], held: [] });
      expect(fixture.attaches()).toBe(1);
    } finally {
      await fixture.cleanup();
    }
  });

  it('refuses unknown versions and malformed pending custody without mutating their ledger', async () => {
    const fixture = await detachCustodyFixture();
    const owner = { pid: process.pid, token: 'owned-operation' };
    const pending = { physical: '/dev/disk7', registryEntry: fixture.entry, phase: 'reported', facadePid: process.pid };
    const invalid = [
      { version: 99, lifecycle: null },
      { version: 2, lifecycle: null },
      { version: 1, lifecycle: { kind: 'detach', owner, pending } },
      { version: 2, lifecycle: { kind: 'detach', owner, pending: { ...pending, registryEntry: 'unknown' } } },
      { version: 2, lifecycle: { kind: 'detach', owner, pending: { ...pending, facadePid: -1 } } },
      { version: 2, lifecycle: { kind: 'detach', owner, pending: { ...pending, phase: 'stopped' } } },
      {
        version: 2,
        lifecycle: {
          kind: 'detach',
          owner,
          pending: { physical: pending.physical, registryEntry: pending.registryEntry, facadePid: null },
        },
      },
    ];
    try {
      for (const header of invalid) {
        const text = JSON.stringify({ ...header, claims: [], held: [] });
        await writeFile(`${fixture.paths.lockFile}.ledger`, text);
        expect((await fixture.volume().acquire(fixture.subtrees)).ok).toBe(false);
        expect(readFileSync(`${fixture.paths.lockFile}.ledger`, 'utf8')).toBe(text);
      }
      expect(fixture.calls).toEqual([]);
      expect(fixture.attaches()).toBe(0);
    } finally {
      await fixture.cleanup();
    }
  });

  it('queues a concurrent caller behind a detach its live caller still awaits, then admits it after the terminal exit', async () => {
    // The caller's bound never fires here: the subject is the ordinary wait on a running detach.
    const fixture = await detachCustodyFixture('stray', 600_000);
    const lookedAgain = Promise.withResolvers<void>();
    let looks = 0;
    try {
      const first = fixture.volume().acquire(fixture.subtrees);
      await fixture.ready();
      expect(fixture.ledger()).toMatchObject({
        version: 2,
        lifecycle: {
          kind: 'create',
          owner: { pid: process.pid },
          pending: { physical: '/dev/disk7', registryEntry: fixture.entry, phase: 'waiting', facadePid: null },
        },
      });
      // Liveness only chooses queueing over reporting; a second ask proves the caller queued and looked again.
      const second = new RamTempVolume(fixture.paths, fixture.capacity, fixture.commands, process.pid, (pid) => {
        expect(pid).toBe(process.pid);
        looks += 1;
        if (looks === 2) lookedAgain.resolve();
        return true;
      }).acquire(fixture.subtrees);
      await lookedAgain.promise;
      expect(fixture.attaches()).toBe(0);
      await fixture.send('complete');
      expect(await fixture.event()).toBe('effect');
      await fixture.send('finish');
      expect(await fixture.event()).toBe('exiting');
      expect((await fixture.output).pending).toBeUndefined();
      expect(JSON.stringify(await first)).toContain('owned attach admission reached');
      expect(JSON.stringify(await second)).toContain('owned attach admission reached');
      expect(fixture.attaches()).toBe(2);
      expect(fixture.calls.filter((call) => call.startsWith('/usr/bin/hdiutil'))).toEqual([
        '/usr/bin/hdiutil detach /dev/disk7',
      ]);
      expect(fixture.ledger()).toEqual({ version: 1, lifecycle: null, claims: [], held: [] });
    } finally {
      await fixture.cleanup();
    }
  });

  it('reports custody when the detach run throws, refusing while its caller still lives', async () => {
    const fixture = await detachCustodyFixture();
    const commands: HostCommands = {
      async run(file, args, deadlineMs, binding) {
        if (file === '/usr/bin/hdiutil') throw new Error('owned run failure');
        return fixture.commands.run(file, args, deadlineMs, binding);
      },
      ownsRamDisk: fixture.commands.ownsRamDisk,
      async detachRam() {
        throw new Error('owned run failure');
      },
      attachRam: (sectors) => fixture.commands.attachRam(sectors),
    };
    const volume = () => new RamTempVolume(fixture.paths, fixture.capacity, commands);
    try {
      expect(JSON.stringify(await volume().acquire(fixture.subtrees))).toContain('owned run failure');
      expect(fixture.ledger()).toMatchObject({
        version: 2,
        lifecycle: {
          kind: 'create',
          owner: { pid: process.pid },
          pending: { physical: '/dev/disk7', registryEntry: fixture.entry, phase: 'reported', facadePid: null },
        },
      });
      // The caller is this live process: waiting custody would queue behind it forever, reported custody answers.
      expect(JSON.stringify(await volume().acquire(fixture.subtrees))).toContain('reported by pid');
      expect(fixture.attaches()).toBe(0);
      await writeFile(fixture.registry, '');
      expect(JSON.stringify(await volume().acquire(fixture.subtrees))).toContain('owned attach admission reached');
      expect(fixture.attaches()).toBe(1);
      expect(fixture.ledger()).toEqual({ version: 1, lifecycle: null, claims: [], held: [] });
    } finally {
      await fixture.cleanup();
    }
  });
});
