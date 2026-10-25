/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CowshedError, type ErrorCode, type Host, openHost } from '../dist/ts/index.js';

/** The taxonomy and a non-empty hint are the contract; message copy is not (see native.test.ts). */
function requireCowshedError(error: unknown, code: ErrorCode): CowshedError {
  expect(error).toBeInstanceOf(CowshedError);
  if (!(error instanceof CowshedError)) {
    // invariant throw: the assertion above proves this branch unreachable.
    throw new Error('expected CowshedError');
  }
  expect(error.code).toBe(code);
  expect(error.hint.length).toBeGreaterThan(0);
  return error;
}

const launcher = join(import.meta.dir, '..', 'bin', 'cowshed');
const recorded = { paths: [{ checkout: '.nx/cache', volume: 'nx/cache' }] };

/** A checkout whose build-volume link records one build-state path, as cowshed writes it. */
async function recordedCheckout(): Promise<{ root: string; checkout: string }> {
  const root = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-'));
  const checkout = join(root, 'checkout');
  const volume = join(root, 'volume');
  await mkdir(join(checkout, '.git'), { recursive: true });
  await mkdir(join(checkout, '.cowshed'), { recursive: true });
  await mkdir(volume);
  await writeFile(join(volume, 'cowshed-build-state.json'), JSON.stringify({ version: 1, paths: recorded.paths }));
  await symlink(volume, join(checkout, '.cowshed', 'build'));
  return { root, checkout };
}

/**
 * This process's `cowshed host` children: the launcher execs the binary, so each is our child. Only this process's own
 * children are read, never the host's process table: a process of the user caught in an uninterruptible kernel wait
 * holds a reader of that table there with it, which `ps -ax` was. `pgrep -P` names the children from the kernel's
 * parent links and `ps -p` reads those pids alone, both through PATH as nx-plugin's process-children.ts does (NixOS has
 * no /bin/ps); a cowshed test cannot import that module without every cowshed cache key following the plugin's sources.
 */
function hostChildren(): number[] {
  const children = list('pgrep', ['-P', String(process.pid)])
    .split('\n')
    .filter((line) => line !== '');
  if (children.length === 0) {
    return [];
  }
  return list('ps', ['-o', 'pid=', '-o', 'command=', '-p', children.join(',')])
    .split('\n')
    .flatMap((line) => {
      const [, pid, command] = /^\s*(\d+)\s+(.*)$/.exec(line) ?? [];
      return pid !== undefined && command?.endsWith('cowshed host') ? [Number(pid)] : [];
    });
}

/** The output of `file args` through PATH: `pgrep -P` and `ps -p` exit 1 with no output when no process matches. */
function list(file: string, args: readonly string[]): string {
  const ran = spawnSync(file, args, { encoding: 'utf8' });
  if (ran.status === 0) {
    return ran.stdout;
  }
  if (ran.status === 1 && ran.stdout === '' && ran.stderr === '') {
    return '';
  }
  throw new Error(`${file} ${args.join(' ')} failed (${ran.error ?? `status ${ran.status}`}): ${ran.stderr}`);
}

describe('openHost', () => {
  it('answers host.buildState with exactly what the build-state verb prints', async () => {
    const { root, checkout } = await recordedCheckout();
    const host = await openHost();
    try {
      const answered = await host.buildState(checkout);
      const verb = spawnSync(launcher, ['--json', '--project', checkout, 'build-state'], { encoding: 'utf8' });
      expect(verb.status).toBe(0);
      expect(answered).toEqual(JSON.parse(verb.stdout));
      expect(answered).toEqual(recorded);
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects with the verb own typed refusal', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-bare-'));
    const host = await openHost();
    try {
      await mkdir(join(root, '.git'));
      const refused = await host.buildState(root).then(
        () => undefined,
        (error: unknown) => error,
      );
      requireCowshedError(refused, 'environment-missing');
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('answers the holder censuses as lock-holders and tree-holders print them, and retirement as a typed value', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-holders-'));
    const lock = join(root, 'lock');
    await writeFile(lock, '');
    const host = await openHost();
    try {
      const observation = await host.lockHolders(lock);
      const printedLock = spawnSync(launcher, ['lock-holders', lock], { encoding: 'utf8' });
      expect(printedLock.status).toBe(0);
      expect(observation).toEqual(JSON.parse(printedLock.stdout));
      expect(observation.path).toEqual({ encoding: 'utf8', data: lock });

      expect(await host.treeHolders(root)).toEqual([]);

      // No pipe end or socket has the address 0: neither census names a holder of it.
      const fdHolders = await host.fdHolders({ devices: ['0x0'] });
      const printedFd = spawnSync(launcher, ['fd-holders', '--device', '0x0'], { encoding: 'utf8' });
      expect(printedFd.status).toBe(0);
      expect(fdHolders.holders).toEqual(JSON.parse(printedFd.stdout).holders);
      expect(fdHolders.holders).toEqual([]);
      expect(typeof fdHolders.uninspected).toBe('number');

      // A handle no supervisor issued: whatever supervisor this process runs under, if any, the
      // answer is the verb's typed refusal, never an error.
      const retirement = await host.retireDescendants(
        { jobId: 7, index: 2, incarnation: '0123456789abcdef0123456789abcdef' },
        0,
      );
      expect(retirement.kind).toBe('refused');
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('streams a disk command as its whole stdout, then its exit, the last event', async () => {
    const host = await openHost();
    try {
      const kinds: string[] = [];
      let exit: number | undefined;
      for await (const event of host.diskCommand({ tool: 'hdiutil', argv: [{ encoding: 'utf8', data: 'info' }] })) {
        expect(exit).toBeUndefined();
        kinds.push(event.kind);
        if (event.kind === 'exit') {
          exit = event.code;
        }
      }
      expect(exit).toBe(0);
      expect(kinds[0]).toBe('stdout');
    } finally {
      await host.close();
    }
  });

  it('rejects a relative disk gateway before running the host command', async () => {
    const host = await openHost();
    try {
      const refused = await host
        .diskCommand({
          tool: 'hdiutil',
          argv: [{ encoding: 'utf8', data: 'info' }],
          gatewaySocket: 'relative.sock',
        })
        [Symbol.asyncIterator]()
        .next()
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      requireCowshedError(refused, 'usage');
    } finally {
      await host.close();
    }
  });
  it('types every credential source, bytes only with an upload and an upload only with bytes', () => {
    // Compiled by typecheck-tests, never called: each line is a call shape the facade accepts or,
    // under @ts-expect-error, refuses.
    const shapes = (host: Host) => {
      const scope = { project: '/p', origin: 'https://registry.example.test', pathPrefixes: ['/acme/'] };
      const bytes = new TextEncoder().encode('token');
      return [
        host.credentialAdd({ ...scope, secret: { kind: 'env', name: 'TOKEN' } }),
        host.credentialAdd({ ...scope, secret: { kind: 'env', name: 'TOKEN', command: ['op', 'read'] } }),
        host.credentialAdd({ ...scope, secret: { kind: 'command', argv: ['op', 'read'] } }),
        host.credentialAdd({ ...scope, secret: { kind: 'upload' } }, bytes),
        // @ts-expect-error bytes beside a secret named by environment
        host.credentialAdd({ ...scope, secret: { kind: 'env', name: 'TOKEN' } }, bytes),
        // @ts-expect-error bytes beside a secret named by command
        host.credentialAdd({ ...scope, secret: { kind: 'command', argv: ['op'] } }, bytes),
        // @ts-expect-error an upload secret without its bytes
        host.credentialAdd({ ...scope, secret: { kind: 'upload' } }),
      ];
    };
    expect(shapes).toBeInstanceOf(Function);
  });

  it('sends an upload secret as the frame, reaching the verb own project discovery', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-credential-'));
    const host = await openHost();
    try {
      const refused = await host
        .credentialAdd(
          {
            project: root,
            origin: 'https://registry.example.test',
            pathPrefixes: ['/acme/'],
            secret: { kind: 'upload' },
          },
          new TextEncoder().encode('token'),
        )
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      requireCowshedError(refused, 'environment-missing');
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('installs the skill into the named project only, and refuses an unknown harness writing nothing', async () => {
    const project = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-skill-'));
    const host = await openHost();
    try {
      const refused = await host.skillInstall({ project, harnesses: ['no-such-harness'] }).then(
        () => undefined,
        (error: unknown) => error,
      );
      requireCowshedError(refused, 'usage');
      expect(await readdir(project)).toEqual([]);
      const report = await host.skillInstall({ project, harnesses: ['claude-code'] });
      expect(report.installs.length).toBeGreaterThan(0);
      for (const install of report.installs) {
        expect(install.path.startsWith(project)).toBe(true);
      }
    } finally {
      await host.close();
      await rm(project, { recursive: true, force: true });
    }
  });

  it('every concurrent close resolves once its cowshed host process has ended, and later calls reject', async () => {
    const { root, checkout } = await recordedCheckout();
    const before = hostChildren();
    const host = await openHost();
    try {
      expect(await host.buildState(checkout)).toEqual(recorded);
      const started = hostChildren().filter((pid) => !before.includes(pid));
      expect(started).toHaveLength(1);
      // Each close's own fulfillment is checked when it happens: a close that resolved before the
      // process ended would see the pid still listed.
      const ended = await Promise.all(
        [host.close(), host.close(), host.close()].map((close) =>
          close.then(() => hostChildren().includes(started[0])),
        ),
      );
      expect(ended).toEqual([false, false, false]);
      const after = await host.buildState(checkout).then(
        () => undefined,
        (error: unknown) => error,
      );
      requireCowshedError(after, 'environment-missing');
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });
});
