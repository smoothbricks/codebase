/**
 * Provisions the per-user directories the real-APFS tests need, on a host `cowshed setup` has not run on.
 *
 * Every real-image fixture keeps its tree in the directory of its own test namespace,
 * `/private/var/folders/cowshed/<uid>/<ns>` (`HostNamespace::macos_user_root`), in the per-user root that also holds
 * the workspace mount root `mnt`, beside `t` (`cowshed_os::scratch::macos_scratch_root`), which holds only the state
 * every test run shares: the user's image template, the lane pools and the dead-run sweep's lock and record. A fixture
 * creates only its namespace directory and `t`: it refuses a missing or foreign per-user root and never makes the
 * root-owned prefix above it, because that is host state `cowshed setup` provisions (specs/cowshed/01a_apfs.md "Stable
 * workspace mount root", specs/cowshed/08_testing.md "Integration tests (real substrate)"). A development host has been
 * through setup, so there this changes nothing and costs a few lstat calls. A CI runner has not, and setup's
 * authorization prompt is interactive, so this applies setup's own three directories (`mount_root_operations` in
 * cowshed-core) with their owners and modes without it, split by the privilege each needs: `sudo -n` creates only the
 * prefix and the per-user root, each beneath a root-owned parent no unprivileged writer can plant a link in, and this
 * user creates `mnt` inside its own 0700 root. A link, a non-directory or a foreign owner in any path's place is
 * refused, never repaired over, and every result is attested by lstat. With the root missing and `sudo -n` refused, it
 * fails naming the exact commands. `t` and each namespace directory are the fixtures' to make and are never looked at
 * here: a `cowshed exec` sandbox, where the shards that depend on this run too, denies it.
 *
 * Every Nx target that runs a test creating a scratch root depends on `cowshed:apfs-test-root`, which runs this.
 *
 * Run with: `nx run cowshed:apfs-test-root`
 */

import { spawnSync } from 'node:child_process';
import { lstatSync } from 'node:fs';
import { dirname, join } from 'node:path';

/** cowshed-core's `MACOS_MOUNT_PREFIX`: the root-owned parent of every user's mount root. */
export const MOUNT_PREFIX = '/private/var/folders/cowshed';

const SUDO = '/usr/bin/sudo';
const INSTALL = '/usr/bin/install';

/**
 * Who creates a directory: `administrator` through `sudo -n`, where only root can write the parent, or `user`, this
 * process, inside a directory it owns.
 */
export type Authority = 'administrator' | 'user';

/** One directory the tests need, with the owner and mode `cowshed setup` gives it. */
export interface Requirement {
  readonly path: string;
  readonly uid: number;
  readonly gid: number;
  readonly mode: number;
  readonly authority: Authority;
}

/** A program and its arguments, run without a shell. */
export type Command = readonly [program: string, ...args: string[]];

/** What `lstat` finds at a path. A link is never followed. */
export type Found =
  | { readonly kind: 'absent' }
  | { readonly kind: 'directory'; readonly uid: number; readonly gid: number; readonly mode: number }
  | { readonly kind: 'other'; readonly link: boolean }
  | { readonly kind: 'unreadable'; readonly why: string };

/** What a path needs: nothing, creation or a mode repair, or the refusal to touch it. */
export type Verdict =
  | { readonly kind: 'current' }
  | { readonly kind: 'needed' }
  | { readonly kind: 'refused'; readonly why: string };

export type Outcome =
  | { readonly kind: 'provisioned'; readonly changed: readonly string[] }
  | { readonly kind: 'failed'; readonly message: string };

/** The host effects, so that every decision here is testable without one. */
export interface Host {
  find(path: string): Found;
  /** Runs `command` to completion: `undefined` when it exited 0, otherwise why it did not. */
  run(command: Command): string | undefined;
}

/** The three directories `cowshed setup` makes for one user, parents first, with its owners and modes. */
export function requirements(prefix: string, uid: number, gid: number): readonly Requirement[] {
  const root = join(prefix, String(uid));
  return [
    { path: prefix, uid: 0, gid: 0, mode: 0o755, authority: 'administrator' },
    { path: root, uid, gid, mode: 0o700, authority: 'administrator' },
    { path: join(root, 'mnt'), uid, gid, mode: 0o700, authority: 'user' },
  ];
}

export function judge(requirement: Requirement, found: Found): Verdict {
  const { path, uid, gid, mode } = requirement;
  switch (found.kind) {
    case 'absent':
      return { kind: 'needed' };
    case 'unreadable':
      return { kind: 'refused', why: `cannot inspect mount root path ${path}: ${found.why}` };
    case 'other':
      return {
        kind: 'refused',
        why: `refusing mount root path ${path}: ${found.link ? 'a symbolic link' : 'not a plain directory'}`,
      };
    case 'directory':
      if (found.uid !== uid || found.gid !== gid) {
        return {
          kind: 'refused',
          why: `refusing foreign mount root directory ${path}: owned by ${found.uid}:${found.gid}, expected ${uid}:${gid}`,
        };
      }
      return found.mode === mode ? { kind: 'current' } : { kind: 'needed' };
  }
}

/** The command that creates or repairs `requirement`. The administrator's is `sudo -n`, never a prompt. */
export function installCommand(requirement: Requirement): Command {
  const { path, uid, gid, mode, authority } = requirement;
  const octal = mode.toString(8);
  return authority === 'administrator'
    ? [SUDO, '-n', INSTALL, '-d', '-o', String(uid), '-g', String(gid), '-m', octal, path]
    : [INSTALL, '-d', '-m', octal, path];
}

/**
 * Provisions every directory `required` names, in order, creating or repairing only what is not already as required.
 * Each is judged where it stands, never through a link, and attested the same way after it is changed.
 */
export function ensure(host: Host, required: readonly Requirement[]): Outcome {
  const changed: string[] = [];
  for (const [position, requirement] of required.entries()) {
    const { path } = requirement;
    const verdict = judge(requirement, host.find(path));
    if (verdict.kind === 'refused') return failed(verdict.why);
    if (verdict.kind === 'current') continue;
    if (requirement.authority === 'administrator') {
      const unsafe = administratorParentRefusal(host, path);
      if (unsafe !== undefined) return failed(unsafe);
    }
    const command = installCommand(requirement);
    const failure = host.run(command);
    if (failure !== undefined) {
      return failed(
        requirement.authority === 'administrator'
          ? administratorUnavailable(host, requirement, required.slice(position + 1), command, failure)
          : `cannot create ${path}: ${failure}\n  (${render(command)})`,
      );
    }
    const found = host.find(path);
    const attested = judge(requirement, found);
    if (attested.kind === 'refused') return failed(attested.why);
    if (attested.kind === 'needed') {
      const { uid, gid, mode } = requirement;
      return failed(
        `mount root path ${path} is ${show(found)} after ${render(command)}, expected ${uid}:${gid} ${mode.toString(8)}`,
      );
    }
    changed.push(path);
  }
  return { kind: 'provisioned', changed };
}

/**
 * `sudo` acts on a path through its parent, which must therefore be a directory only root can write: anyone else could
 * swap a link in between the lstat that judged the path and the install that follows it.
 */
function administratorParentRefusal(host: Host, path: string): string | undefined {
  const parent = dirname(path);
  const found = host.find(parent);
  if (found.kind === 'directory' && found.uid === 0 && (found.mode & 0o022) === 0) return undefined;
  return `refusing to create ${path} as an administrator: its parent ${parent} is ${show(found)}, not a root-owned directory only root can write`;
}

/** Names what `sudo -n` refused, and every administrator command from there on that is still needed, to run by hand. */
function administratorUnavailable(
  host: Host,
  refusedFor: Requirement,
  later: readonly Requirement[],
  attempted: Command,
  failure: string,
): string {
  const stillNeeded = later.filter(
    (requirement) =>
      requirement.authority === 'administrator' && judge(requirement, host.find(requirement.path)).kind === 'needed',
  );
  const byHand = [refusedFor, ...stillNeeded].map(
    (requirement) => `  ${render(installCommand(requirement).filter((argument) => argument !== '-n'))}`,
  );
  return [
    `cannot create ${refusedFor.path} without administrator rights: \`${render(attempted)}\` failed: ${failure}`,
    'Run `cowshed setup` on this host, or run each of these once:',
    ...byHand,
  ].join('\n');
}

function failed(message: string): Outcome {
  return { kind: 'failed', message };
}

function render(command: readonly string[]): string {
  return command.join(' ');
}

function show(found: Found): string {
  switch (found.kind) {
    case 'absent':
      return 'absent';
    case 'unreadable':
      return `unreadable (${found.why})`;
    case 'other':
      return found.link ? 'a symbolic link' : 'not a directory';
    case 'directory':
      return `${found.uid}:${found.gid} ${found.mode.toString(8)}`;
  }
}

export const systemHost: Host = {
  find(path) {
    try {
      const stat = lstatSync(path, { throwIfNoEntry: false });
      if (stat === undefined) return { kind: 'absent' };
      if (stat.isDirectory()) return { kind: 'directory', uid: stat.uid, gid: stat.gid, mode: stat.mode & 0o7777 };
      return { kind: 'other', link: stat.isSymbolicLink() };
    } catch (error) {
      return { kind: 'unreadable', why: error instanceof Error ? error.message : String(error) };
    }
  },
  run([program, ...args]) {
    const result = spawnSync(program, args, { stdio: ['ignore', 'ignore', 'pipe'], encoding: 'utf8' });
    if (result.error !== undefined) return result.error.message;
    if (result.status === 0) return undefined;
    const detail = result.stderr.trim();
    if (detail !== '') return detail;
    return result.status === null ? `killed by ${result.signal}` : `exited ${result.status}`;
  },
};

function main(): number {
  if (process.platform !== 'darwin') {
    console.log(`apfs-test-root: ${process.platform} has no per-user APFS mount root; nothing to provision`);
    return 0;
  }
  const uid = process.getuid();
  const outcome = ensure(systemHost, requirements(MOUNT_PREFIX, uid, process.getgid()));
  if (outcome.kind === 'failed') {
    console.error(`apfs-test-root: ${outcome.message}`);
    return 1;
  }
  console.log(
    outcome.changed.length === 0
      ? `apfs-test-root: ${join(MOUNT_PREFIX, String(uid))} is provisioned; nothing to do`
      : `apfs-test-root: provisioned ${outcome.changed.join(', ')}`,
  );
  return 0;
}

if (import.meta.main) {
  process.exit(main());
}
