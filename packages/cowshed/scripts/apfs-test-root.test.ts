import { describe, expect, it } from 'bun:test';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  type Command,
  ensure,
  type Found,
  type Host,
  installCommand,
  judge,
  MOUNT_PREFIX,
  type Outcome,
  probe,
  type Requirement,
  requirements,
  systemHost,
} from './apfs-test-root.js';
import { sweepExecutable } from './apfs-test-sweep.js';

describe('the lane sweep executable in the test build', () => {
  const entry = { name: 'cowshed-test-sweep', kind: 'bin-exe', path: 'debug/cowshed-test-sweep' };
  const metadata = (entries: unknown[]) => ({
    'rust-build-meta': { 'non-test-binaries': { 'cowshed-apfs-package': entries } },
  });
  it("uses exactly the build's executable under this checkout's target directory", () => {
    expect(sweepExecutable('/target', metadata([entry]))).toBe('/target/debug/cowshed-test-sweep');
    expect(sweepExecutable('/relocated', metadata([{ ...entry, path: 'debug/build/sweep/out/helper' }]))).toBe(
      '/relocated/debug/build/sweep/out/helper',
    );
  });
  it('refuses missing or ambiguous cleanup executables instead of starting fixtures', () => {
    for (const value of [null, {}, metadata([]), metadata([entry, entry]), metadata([{ ...entry, kind: 'dylib' }])]) {
      expect(() => sweepExecutable('/target', value)).toThrow();
    }
  });
  it('cannot execute a path outside the target directory', () => {
    for (const path of ['/foreign/helper', '../foreign/helper', 'debug/../../foreign/helper', '.']) {
      expect(() => sweepExecutable('/target', metadata([{ ...entry, path }]))).toThrow();
    }
  });
});

const SUDO = '/usr/bin/sudo';
const INSTALL = '/usr/bin/install';
const PARENT = '/private/var/folders';
const ROOT = `${MOUNT_PREFIX}/501`;
const MNT = `${ROOT}/mnt`;
const SCRATCH = `${ROOT}/t`;

/** What making each directory runs: `sudo -n` only where only root can write the parent. */
const PREFIX_COMMAND: Command = [SUDO, '-n', INSTALL, '-d', '-o', '0', '-g', '0', '-m', '755', MOUNT_PREFIX];
const ROOT_COMMAND: Command = [SUDO, '-n', INSTALL, '-d', '-o', '501', '-g', '20', '-m', '700', ROOT];
const MNT_COMMAND: Command = [INSTALL, '-d', '-m', '700', MNT];

/** The three directories of uid 501's mount root, written out: what `cowshed setup` makes and who may make each. */
const prefix: Requirement = { path: MOUNT_PREFIX, uid: 0, gid: 0, mode: 0o755, authority: 'administrator' };
const root: Requirement = { path: ROOT, uid: 501, gid: 20, mode: 0o700, authority: 'administrator' };
const mnt: Requirement = { path: MNT, uid: 501, gid: 20, mode: 0o700, authority: 'user' };
const required = [prefix, root, mnt];

const directory = (uid: number, gid: number, mode: number): Found => ({ kind: 'directory', uid, gid, mode });
const rootOnly = directory(0, 0, 0o755);
const owned = directory(501, 20, 0o700);

/** A host `cowshed setup` has been through. */
const provisioned = (): Record<string, Found> => ({
  [PARENT]: rootOnly,
  [MOUNT_PREFIX]: rootOnly,
  [ROOT]: owned,
  [MNT]: owned,
});

/** A host whose only directory here is root's `/private/var/folders`. */
const fresh = (): Record<string, Found> => ({ [PARENT]: rootOnly });

/** What the fake does with a command: apply it, accept it and change nothing, or fail it. */
type Behaviour = (command: Command) => 'applies' | 'ignored' | { readonly failure: string };

const sudoDenied: Behaviour = ([program]) =>
  program === SUDO ? { failure: 'sudo: a password is required' } : 'applies';

/**
 * A host whose filesystem is `initial` and whose `install -d` does what the real one does: it makes the directory owned
 * by the `-o` and `-g` it is given, or by uid 501 and gid 20, the user running here, and sets the `-m` mode. It keeps
 * every path it was asked about and every command it was asked to run.
 */
function fakeHost(initial: Record<string, Found>, behaviour: Behaviour = () => 'applies') {
  const state = new Map(Object.entries(initial));
  const finds: string[] = [];
  const commands: Command[] = [];
  const host: Host = {
    find(path) {
      finds.push(path);
      return state.get(path) ?? { kind: 'absent' };
    },
    run(command) {
      commands.push(command);
      const what = behaviour(command);
      if (what === 'ignored') return undefined;
      if (typeof what === 'object') return what.failure;
      const install = command[0] === SUDO ? command.slice(2) : command;
      const flag = (name: string): string | undefined => {
        const at = install.indexOf(name);
        return at === -1 ? undefined : install[at + 1];
      };
      const path = install.at(-1);
      if (path === undefined) throw new Error('an install names the directory it makes');
      state.set(
        path,
        directory(Number(flag('-o') ?? 501), Number(flag('-g') ?? 20), Number.parseInt(flag('-m') ?? '', 8)),
      );
      return undefined;
    },
  };
  return { host, finds, commands };
}

function messageOf(outcome: Outcome): string {
  if (outcome.kind !== 'failed') throw new Error('expected the provisioning to fail');
  return outcome.message;
}

describe('the per-user mount root the real-APFS tests need', () => {
  it('is the three directories cowshed setup makes, parents first, each with the privilege it needs', () => {
    expect(requirements(MOUNT_PREFIX, 501, 20)).toEqual(required);
  });

  it("matches the paths and modes of cowshed's own mount root and scratch parent", () => {
    const crates = fileURLToPath(new URL('../crates/', import.meta.url));
    const read = (path: string): string => readFileSync(join(crates, path), 'utf8');
    expect(read('cowshed-core/src/storage/host_config.rs')).toContain(`MACOS_MOUNT_PREFIX: &str = "${MOUNT_PREFIX}"`);
    expect(read('cowshed-core/src/storage/host_config.rs')).toMatch(/fn macos_mount_root[^}]*\.join\("mnt"\)/);
    // The fixtures make `t` inside the per-user root this provisions, and refuse it when it is missing.
    expect(read('cowshed-os/src/scratch.rs')).toContain(`"${MOUNT_PREFIX}/{euid}/t"`);
    // setup's own operations: the prefix is root's 0755, the per-user root and `mnt` are the user's 0700, and the
    // build-cache volume's `shared` parent (the user's, 0750) is setup's alone: no test mounts over that volume.
    const operations = /fn mount_root_operations[\s\S]*?\n}\n/.exec(
      read('cowshed-core/src/storage/bootstrap/native/macos.rs'),
    )?.[0];
    expect(operations).toMatch(/uid: 0,\s+gid: 0,\s+mode: 0o755,/);
    expect(operations?.match(/mode: 0o700,/g)).toHaveLength(2);
    expect(
      operations?.match(/path: PathBuf::from\(BUILD_CACHE_PARENT\),\s+uid,\s+gid: shared_gid,\s+mode: 0o750,/g),
    ).toHaveLength(1);
    expect(operations?.match(/HostOperation::EnsureOwnedDirectory/g)).toHaveLength(4);
  });

  it('names the exact command for each directory, sudo only where only root can write the parent', () => {
    expect(installCommand(prefix)).toEqual(PREFIX_COMMAND);
    expect(installCommand(root)).toEqual(ROOT_COMMAND);
    expect(installCommand(mnt)).toEqual(MNT_COMMAND);
  });

  describe('judged where it stands', () => {
    const cases: readonly (readonly [string, Requirement, Found, string])[] = [
      ['absent needs creating', mnt, { kind: 'absent' }, 'needed'],
      ['as required needs nothing', mnt, owned, 'current'],
      ['a wrong mode needs repairing', mnt, directory(501, 20, 0o755), 'needed'],
      ['an extra setgid bit is a wrong mode', mnt, directory(501, 20, 0o2700), 'needed'],
      ['a foreign uid is refused', mnt, directory(0, 20, 0o700), 'refused'],
      ['a foreign gid is refused', mnt, directory(501, 0, 0o700), 'refused'],
      ['a link is refused', mnt, { kind: 'other', link: true }, 'refused'],
      ['a file is refused', mnt, { kind: 'other', link: false }, 'refused'],
      ['an unreadable path is refused', mnt, { kind: 'unreadable', why: 'EPERM' }, 'refused'],
      ["root's prefix as required needs nothing", prefix, rootOnly, 'current'],
      ["a user's own prefix is foreign", prefix, owned, 'refused'],
    ];
    for (const [name, requirement, found, expected] of cases) {
      it(name, () => {
        expect(judge(requirement, found).kind).toBe(expected);
      });
    }
  });

  describe('on a host setup has been through', () => {
    it('changes nothing, runs no command at all, sudo least of all, and looks at only the three directories', () => {
      const { host, finds, commands } = fakeHost(provisioned());
      expect(ensure(host, required)).toEqual({ kind: 'provisioned', changed: [] });
      expect(commands).toEqual([]);
      expect(finds).toEqual([MOUNT_PREFIX, ROOT, MNT]);
    });

    it('never looks at t, which the fixtures make and a cowshed exec sandbox denies', () => {
      const { host, finds } = fakeHost({ ...provisioned(), [SCRATCH]: { kind: 'unreadable', why: 'EPERM' } });
      expect(ensure(host, required)).toEqual({ kind: 'provisioned', changed: [] });
      expect(finds).not.toContain(SCRATCH);
    });
  });

  describe('on a host with nothing', () => {
    it('makes the prefix and the per-user root as administrator, then mnt as the user, in that order', () => {
      const { host, commands } = fakeHost(fresh());
      expect(probe(host, required, 'run 1')).toBe('unprovisioned run 1');
      expect(ensure(host, required)).toEqual({ kind: 'provisioned', changed: [MOUNT_PREFIX, ROOT, MNT] });
      expect(commands).toEqual([PREFIX_COMMAND, ROOT_COMMAND, MNT_COMMAND]);
      // A second run finds all of it as required and changes nothing.
      expect(ensure(host, required)).toEqual({ kind: 'provisioned', changed: [] });
      expect(commands).toHaveLength(3);
      expect(probe(host, required, 'run 2')).toBe('provisioned');
    });

    it('makes only what is missing', () => {
      const { host, commands } = fakeHost({ [PARENT]: rootOnly, [MOUNT_PREFIX]: rootOnly });
      expect(ensure(host, required)).toEqual({ kind: 'provisioned', changed: [ROOT, MNT] });
      expect(commands).toEqual([ROOT_COMMAND, MNT_COMMAND]);
    });

    it('repairs a wrong mode in place, by the authority that owns the directory', () => {
      const user = fakeHost({ ...provisioned(), [MNT]: directory(501, 20, 0o755) });
      expect(ensure(user.host, required)).toEqual({ kind: 'provisioned', changed: [MNT] });
      expect(user.commands).toEqual([MNT_COMMAND]);
      const administrator = fakeHost({ ...provisioned(), [MOUNT_PREFIX]: directory(0, 0, 0o775) });
      expect(ensure(administrator.host, required)).toEqual({ kind: 'provisioned', changed: [MOUNT_PREFIX] });
      expect(administrator.commands).toEqual([PREFIX_COMMAND]);
    });
  });

  describe('without passwordless sudo', () => {
    it('fails naming each administrator command to run by hand, after trying only the first', () => {
      const { host, commands } = fakeHost(fresh(), sudoDenied);
      const message = messageOf(ensure(host, required));
      expect(commands).toEqual([PREFIX_COMMAND]);
      expect(message).toContain(`cannot create ${MOUNT_PREFIX} without administrator rights`);
      expect(message).toContain('sudo: a password is required');
      expect(message).toContain('cowshed setup');
      expect(message.split('\n').slice(-2)).toEqual([
        `  ${SUDO} ${INSTALL} -d -o 0 -g 0 -m 755 ${MOUNT_PREFIX}`,
        `  ${SUDO} ${INSTALL} -d -o 501 -g 20 -m 700 ${ROOT}`,
      ]);
    });

    it('names only the command still needed when the prefix is already there', () => {
      const { host } = fakeHost({ [PARENT]: rootOnly, [MOUNT_PREFIX]: rootOnly }, sudoDenied);
      const message = messageOf(ensure(host, required));
      expect(message.split('\n').slice(2)).toEqual([`  ${SUDO} ${INSTALL} -d -o 501 -g 20 -m 700 ${ROOT}`]);
    });

    it('still succeeds on a host that needs no administrator at all', () => {
      const { host } = fakeHost(provisioned(), sudoDenied);
      expect(ensure(host, required)).toEqual({ kind: 'provisioned', changed: [] });
    });
  });

  describe('refuses to write through what it must not', () => {
    it('a link where the per-user root belongs, before running anything', () => {
      const { host, commands } = fakeHost({
        ...fresh(),
        [MOUNT_PREFIX]: rootOnly,
        [ROOT]: { kind: 'other', link: true },
      });
      expect(messageOf(ensure(host, required))).toBe(`refusing mount root path ${ROOT}: a symbolic link`);
      expect(commands).toEqual([]);
    });

    it('a file where the prefix belongs', () => {
      const { host, commands } = fakeHost({ ...fresh(), [MOUNT_PREFIX]: { kind: 'other', link: false } });
      expect(messageOf(ensure(host, required))).toBe(`refusing mount root path ${MOUNT_PREFIX}: not a plain directory`);
      expect(commands).toEqual([]);
    });

    it("another user's directory where the prefix belongs", () => {
      const { host, commands } = fakeHost({ ...fresh(), [MOUNT_PREFIX]: directory(501, 20, 0o755) });
      expect(messageOf(ensure(host, required))).toBe(
        `refusing foreign mount root directory ${MOUNT_PREFIX}: owned by 501:20, expected 0:0`,
      );
      expect(commands).toEqual([]);
    });

    it('a per-user root that is not the user and group running here', () => {
      const { host, commands } = fakeHost({ ...provisioned(), [ROOT]: directory(502, 20, 0o700) });
      expect(messageOf(ensure(host, required))).toContain('refusing foreign mount root directory');
      expect(commands).toEqual([]);
    });

    it('creating as administrator beneath a parent anyone but root can write', () => {
      for (const parent of [directory(501, 20, 0o755), directory(0, 0, 0o775), directory(0, 0, 0o757)]) {
        const { host, commands } = fakeHost({ [PARENT]: parent });
        expect(messageOf(ensure(host, required))).toContain(
          `refusing to create ${MOUNT_PREFIX} as an administrator: its parent ${PARENT} is`,
        );
        expect(commands).toEqual([]);
      }
    });

    it('creating as administrator beneath a parent that is absent', () => {
      const { host, commands } = fakeHost({});
      expect(messageOf(ensure(host, required))).toContain(`its parent ${PARENT} is absent`);
      expect(commands).toEqual([]);
    });
  });

  describe('attests every result', () => {
    it('failing when an install that succeeded left something else', () => {
      const { host } = fakeHost(fresh(), ([program]) => (program === SUDO ? 'ignored' : 'applies'));
      expect(messageOf(ensure(host, required))).toBe(
        `mount root path ${MOUNT_PREFIX} is absent after ${PREFIX_COMMAND.join(' ')}, expected 0:0 755`,
      );
    });

    it('failing when a user install is refused, with the command that was refused', () => {
      const { host, commands } = fakeHost({ ...provisioned(), [MNT]: { kind: 'absent' } }, () => ({
        failure: 'install: mkdir: Permission denied',
      }));
      expect(messageOf(ensure(host, required))).toBe(
        `cannot create ${MNT}: install: mkdir: Permission denied\n  (${MNT_COMMAND.join(' ')})`,
      );
      expect(commands).toEqual([MNT_COMMAND]);
    });
  });
});

describe('the system host', () => {
  it('tells absent, directory, link, file and unreadable apart, never following a link', () => {
    const temporary = mkdtempSync(join(tmpdir(), 'apfs-test-root-'));
    try {
      const directoryPath = join(temporary, 'directory');
      mkdirSync(directoryPath);
      chmodSync(directoryPath, 0o700);
      const filePath = join(temporary, 'file');
      writeFileSync(filePath, '');
      const linkPath = join(temporary, 'link');
      symlinkSync(directoryPath, linkPath);
      const { uid, gid } = statSync(directoryPath);
      expect(systemHost.find(join(temporary, 'absent'))).toEqual({ kind: 'absent' });
      expect(systemHost.find(directoryPath)).toEqual({ kind: 'directory', uid, gid, mode: 0o700 });
      expect(systemHost.find(linkPath)).toEqual({ kind: 'other', link: true });
      expect(systemHost.find(filePath)).toEqual({ kind: 'other', link: false });
      expect(systemHost.find(join(filePath, 'below')).kind).toBe('unreadable');
    } finally {
      rmSync(temporary, { recursive: true, force: true });
    }
  });

  it('reports how a command failed, and nothing when it succeeded', () => {
    const bun = process.execPath;
    expect(systemHost.run([bun, '-e', 'process.exit(0)'])).toBeUndefined();
    // Raw stderr: Nx sets FORCE_COLOR, under which console.error wraps its text in ANSI codes.
    const failing = "process.stderr.write('install: no space left\\n'); process.exit(3)";
    expect(systemHost.run([bun, '-e', failing])).toBe('install: no space left');
    expect(systemHost.run([bun, '-e', 'process.exit(3)'])).toBe('exited 3');
    expect(systemHost.run([join(tmpdir(), 'apfs-test-root-no-such-program')])).toContain('ENOENT');
  });

  it("provisions a user's directories with the real install, and finds a second run has nothing to do", () => {
    if (process.platform !== 'darwin') return;
    const temporary = mkdtempSync(join(tmpdir(), 'apfs-test-root-'));
    try {
      const { uid, gid } = statSync(temporary);
      const base = join(temporary, 'cowshed');
      const user = (path: string, mode: number): Requirement => ({ path, uid, gid, mode, authority: 'user' });
      const directories = [user(base, 0o755), user(join(base, 'mnt'), 0o700)];
      expect(ensure(systemHost, directories)).toEqual({
        kind: 'provisioned',
        changed: directories.map(({ path }) => path),
      });
      expect(systemHost.find(join(base, 'mnt'))).toEqual({ kind: 'directory', uid, gid, mode: 0o700 });
      expect(ensure(systemHost, directories)).toEqual({ kind: 'provisioned', changed: [] });
    } finally {
      rmSync(temporary, { recursive: true, force: true });
    }
  });
});
