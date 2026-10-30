import { afterEach, describe, expect, it } from 'bun:test';
import { mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type AskRemote,
  type Clock,
  holdUntilServed,
  type Pin,
  type PinAnswer,
  readPins,
  remoteAsker,
  unitePins,
} from '../../../nx-plugin/managed/raw/tooling/git-hooks/pinned-revisions-served.ts';

/**
 * The managed judge of whether each git revision Cargo.lock pins is served by its remote, where CI fetches it. A land
 * builds a pin from the local Cargo git database, so only a push meets an unserved pin; the remotes here are real bare
 * repositories reached over file:// URLs, and the planted red is a revision committed in a clone and never pushed.
 */

const SCRIPT = new URL('../../../nx-plugin/managed/raw/tooling/git-hooks/pinned-revisions-served.ts', import.meta.url)
  .pathname;

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function scratch(): string {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'pinned-revisions-test-')));
  roots.push(root);
  return root;
}

function git(cwd: string, ...args: string[]): string {
  const run = Bun.spawnSync(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', ...args], { cwd });
  if (run.exitCode !== 0) throw new Error(`git ${args.join(' ')} failed in ${cwd}: ${run.stderr.toString()}`);
  return run.stdout.toString().trim();
}

/** A remote holding one pushed commit, and a clone holding that commit and one more it never pushed. */
function remoteWithUnpushed(): { root: string; url: string; clone: string; pushed: string; unpushed: string } {
  const root = scratch();
  git(root, 'init', '--quiet', '--bare', 'remote.git');
  git(root, 'clone', '--quiet', 'remote.git', 'clone');
  const clone = join(root, 'clone');
  git(clone, 'commit', '--quiet', '--allow-empty', '-m', 'pushed');
  git(clone, 'push', '--quiet', 'origin', 'HEAD:refs/heads/main');
  const pushed = git(clone, 'rev-parse', 'HEAD');
  git(clone, 'commit', '--quiet', '--allow-empty', '-m', 'unpushed');
  const unpushed = git(clone, 'rev-parse', 'HEAD');
  return { root, url: `file://${join(root, 'remote.git')}`, clone, pushed, unpushed };
}

function lock(...packages: readonly { name: string; source?: string }[]): string {
  return [
    'version = 4',
    ...packages.flatMap(({ name, source }) => [
      '',
      '[[package]]',
      `name = "${name}"`,
      'version = "0.1.0"',
      ...(source === undefined ? [] : [`source = "${source}"`]),
    ]),
  ].join('\n');
}

const A = 'a'.repeat(40);
const B = 'b'.repeat(40);

describe('readPins', () => {
  it('reads one pin per remote and revision, with the packages built from it', () => {
    const read = readPins(
      lock(
        { name: 'x', source: `git+https://example.test/r.git?rev=${A}#${A}` },
        { name: 'y', source: `git+https://example.test/r.git?rev=${A}#${A}` },
        { name: 'z', source: `git+https://example.test/r.git?branch=main#${B}` },
        { name: 'w', source: `git+https://example.test/other#${A}` },
        { name: 'serde', source: 'registry+https://github.com/rust-lang/crates.io-index' },
        { name: 'local' },
      ),
    );
    expect(read).toEqual({
      kind: 'pins',
      pins: [
        { url: 'https://example.test/r.git', rev: A, packages: ['x', 'y'] },
        { url: 'https://example.test/r.git', rev: B, packages: ['z'] },
        { url: 'https://example.test/other', rev: A, packages: ['w'] },
      ],
    });
  });

  it('refuses a git source that names no full revision', () => {
    expect(readPins(lock({ name: 'x', source: 'git+https://example.test/r.git?branch=main' }))).toEqual({
      kind: 'malformed',
      detail: "x's git source names no full revision: git+https://example.test/r.git?branch=main",
    });
  });

  it('reads no pins from a lock without packages', () => {
    expect(readPins('version = 4\n')).toEqual({ kind: 'pins', pins: [] });
  });

  it('unites the pins of several trees, each remote and revision once', () => {
    const url = 'https://example.test/r.git';
    expect(
      unitePins([
        [{ url, rev: A, packages: ['x'] }],
        [
          { url, rev: A, packages: ['x', 'y'] },
          { url, rev: B, packages: ['z'] },
        ],
      ]),
    ).toEqual([
      { url, rev: A, packages: ['x', 'y'] },
      { url, rev: B, packages: ['z'] },
    ]);
  });
});

describe('remoteAsker', () => {
  it('answers what the remote serves, and asks again after a push', async () => {
    const { url, clone, pushed, unpushed } = remoteWithUnpushed();
    const ask = remoteAsker(scratch());
    expect(await ask(url, pushed)).toEqual({ kind: 'served' });
    expect(await ask(url, unpushed)).toEqual({ kind: 'unserved' });
    git(clone, 'push', '--quiet', 'origin', 'HEAD:refs/heads/main');
    expect(await ask(url, unpushed)).toEqual({ kind: 'served' });
  });

  it('says a remote it cannot reach is unreachable, not unserved', async () => {
    const answer = await remoteAsker(scratch())(`file://${join(scratch(), 'missing.git')}`, A);
    expect(answer.kind).toBe('unreachable');
    expect(answer.kind === 'unreachable' && answer.detail).toContain('missing.git');
  });
});

describe('holdUntilServed', () => {
  const pin: Pin = { url: 'https://example.test/r.git', rev: A, packages: ['x'] };

  function manualClock(): Clock & { readonly slept: number[] } {
    let now = 0;
    const slept: number[] = [];
    return {
      slept,
      now: () => now,
      sleep: async (ms) => {
        slept.push(ms);
        now += ms;
      },
    };
  }

  function answering(...answers: readonly PinAnswer[]): AskRemote {
    let asked = 0;
    return async () => answers[Math.min(asked++, answers.length - 1)] ?? { kind: 'served' };
  }

  it('holds until the remote serves the pin, naming it while it waits', async () => {
    const clock = manualClock();
    const said: string[] = [];
    const outcome = await holdUntilServed(
      [pin],
      answering({ kind: 'unserved' }, { kind: 'unreachable', detail: 'offline' }, { kind: 'served' }),
      clock,
      900_000,
      30_000,
      (line) => said.push(line),
    );
    expect(outcome).toEqual({ kind: 'served', waitedMs: 60_000, rounds: 3 });
    expect(clock.slept).toEqual([30_000, 30_000]);
    expect(said).toEqual([
      `waiting (0 s of 900 s): https://example.test/r.git does not serve ${A} (Cargo.lock builds x from it)`,
      `waiting (30 s of 900 s): could not ask https://example.test/r.git for ${A} (Cargo.lock builds x from it): offline`,
    ]);
  });

  it('refuses at the bound with the pins still not served', async () => {
    const clock = manualClock();
    const outcome = await holdUntilServed([pin], answering({ kind: 'unserved' }), clock, 45_000, 30_000, () => {});
    expect(outcome).toEqual({ kind: 'refused', waitedMs: 45_000, outstanding: [[pin, { kind: 'unserved' }]] });
    expect(clock.slept).toEqual([30_000, 15_000]);
  });
});

describe('the command', () => {
  function workspacePinning(url: string, rev: string): { workspace: string; commit: string } {
    const workspace = scratch();
    git(workspace, 'init', '--quiet');
    writeFileSync(join(workspace, 'Cargo.lock'), lock({ name: 'pinned', source: `git+${url}?rev=${rev}#${rev}` }));
    git(workspace, 'add', 'Cargo.lock');
    git(workspace, 'commit', '--quiet', '-m', 'pin');
    return { workspace, commit: git(workspace, 'rev-parse', 'HEAD') };
  }

  function run(cwd: string, ...args: string[]): { exitCode: number; stdout: string; stderr: string } {
    const child = Bun.spawnSync([process.execPath, SCRIPT, ...args], { cwd });
    return { exitCode: child.exitCode, stdout: child.stdout.toString(), stderr: child.stderr.toString() };
  }

  it('reports an unserved pin and passes, for a land', () => {
    const { url, unpushed } = remoteWithUnpushed();
    const { workspace } = workspacePinning(url, unpushed);
    const reported = run(workspace, '--report');
    expect(reported.exitCode).toBe(0);
    expect(reported.stdout).toContain('PINNED-REVISIONS note: 1 pinned revision(s) not served by their remote');
    expect(reported.stdout).toContain(`${url} does not serve ${unpushed} (Cargo.lock builds pinned from it)`);
  });

  it('says every pin is served when the remote serves them', () => {
    const { url, pushed } = remoteWithUnpushed();
    const { workspace } = workspacePinning(url, pushed);
    expect(run(workspace, '--report').stdout).toContain(
      'PINNED-REVISIONS: every git revision Cargo.lock pins is served',
    );
  });

  it('refuses a push of a tree whose pin its remote never serves', () => {
    const { url, unpushed } = remoteWithUnpushed();
    const { workspace, commit } = workspacePinning(url, unpushed);
    const held = run(workspace, '--wait', '1', commit);
    expect(held.exitCode).toBe(1);
    expect(held.stderr).toContain(`pre-push: ${url} does not serve ${unpushed}`);
    expect(held.stderr).toContain('pre-push: refused: CI fetches every git revision Cargo.lock pins');
  });

  it('lets a push through once the pinned repository has pushed', () => {
    const { url, clone, unpushed } = remoteWithUnpushed();
    git(clone, 'push', '--quiet', 'origin', 'HEAD:refs/heads/main');
    const { workspace, commit } = workspacePinning(url, unpushed);
    const held = run(workspace, '--wait', '1', commit);
    expect(held).toEqual({ exitCode: 0, stdout: '', stderr: '' });
  });
});
