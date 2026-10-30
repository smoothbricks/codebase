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
/** Ample for a local file:// fetch; the budget only ends an ask that would otherwise hang. */
const BUDGET_MS = 60_000;
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
    expect(await ask(url, pushed, BUDGET_MS)).toEqual({ kind: 'served' });
    expect(await ask(url, unpushed, BUDGET_MS)).toEqual({ kind: 'unserved' });
    git(clone, 'push', '--quiet', 'origin', 'HEAD:refs/heads/main');
    expect(await ask(url, unpushed, BUDGET_MS)).toEqual({ kind: 'served' });
  });

  it('asks one remote for several revisions at once without the fetches contending for its scratch repository', async () => {
    const { clone, url, pushed, unpushed } = remoteWithUnpushed();
    git(clone, 'commit', '--quiet', '--allow-empty', '-m', 'third');
    const third = git(clone, 'rev-parse', 'HEAD');
    git(clone, 'push', '--quiet', 'origin', `${unpushed}:refs/heads/main`);
    const ask = remoteAsker(scratch());
    const answers = await Promise.all([pushed, unpushed, third, pushed].map((rev) => ask(url, rev, BUDGET_MS)));
    expect(answers).toEqual([{ kind: 'served' }, { kind: 'served' }, { kind: 'unserved' }, { kind: 'served' }]);
  });

  it('answers a remote that never answers as unreachable once its budget has passed', async () => {
    // git's ext transport runs a command as the remote; `read` waits on a request git never sends before the
    // remote's advertisement, so the fetch hangs until the asker's deadline kills it, and `read` ends with its pipe.
    const inherited = { ...process.env };
    Object.assign(process.env, {
      GIT_CONFIG_COUNT: '1',
      GIT_CONFIG_KEY_0: 'protocol.ext.allow',
      GIT_CONFIG_VALUE_0: 'always',
    });
    try {
      const started = performance.now();
      const answer = await remoteAsker(scratch())('ext::sh -c read', A, 1_000);
      expect(answer).toEqual({ kind: 'unreachable', detail: 'no answer within 1 s' });
      expect(performance.now() - started).toBeLessThan(30_000);
    } finally {
      for (const name of ['GIT_CONFIG_COUNT', 'GIT_CONFIG_KEY_0', 'GIT_CONFIG_VALUE_0'])
        if (inherited[name] === undefined) delete process.env[name];
        else process.env[name] = inherited[name];
    }
  });

  it('says a remote it cannot reach is unreachable, not unserved', async () => {
    const answer = await remoteAsker(scratch())(`file://${join(scratch(), 'missing.git')}`, A, BUDGET_MS);
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

  /** Answers in turn, the last one from then on, and records the budget each ask was given. */
  function answering(...answers: readonly PinAnswer[]): AskRemote & { readonly budgets: number[] } {
    const budgets: number[] = [];
    const ask = async (_url: string, _rev: string, budgetMs: number): Promise<PinAnswer> => {
      budgets.push(budgetMs);
      return answers[Math.min(budgets.length - 1, answers.length - 1)] ?? { kind: 'served' };
    };
    return Object.assign(ask, { budgets });
  }

  it('holds until the remote serves the pin, naming it while it waits', async () => {
    const clock = manualClock();
    const said: string[] = [];
    const ask = answering({ kind: 'unserved' }, { kind: 'unreachable', detail: 'offline' }, { kind: 'served' });
    const outcome = await holdUntilServed([pin], ask, clock, 900_000, 30_000, (line) => said.push(line));
    expect(outcome).toEqual({ kind: 'served', waitedMs: 60_000, rounds: 3 });
    expect(clock.slept).toEqual([30_000, 30_000]);
    expect(ask.budgets).toEqual([900_000, 870_000, 840_000]);
    expect(said).toEqual([
      `waiting (0 s of 900 s): https://example.test/r.git does not serve ${A} (Cargo.lock builds x from it)`,
      `waiting (30 s of 900 s): could not ask https://example.test/r.git for ${A} (Cargo.lock builds x from it): offline`,
    ]);
  });

  it('refuses after the last round that leaves no room for another before the bound', async () => {
    const clock = manualClock();
    const ask = answering({ kind: 'unserved' });
    const outcome = await holdUntilServed([pin], ask, clock, 45_000, 30_000, () => {});
    expect(outcome).toEqual({ kind: 'refused', waitedMs: 30_000, outstanding: [[pin, { kind: 'unserved' }]] });
    expect(clock.slept).toEqual([30_000]);
    // No ask is given more than what is left of the bound when it starts.
    expect(ask.budgets).toEqual([45_000, 15_000]);
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
    expect(reported.stdout).toContain('PINNED-REVISIONS note: 1 pinned revision(s) their remote does not serve');
    expect(reported.stdout).toContain(`${url} does not serve ${unpushed} (Cargo.lock builds pinned from it)`);
  });

  it('counts a remote that did not answer apart from one that does not serve', () => {
    const { workspace } = workspacePinning(`file://${join(scratch(), 'missing.git')}`, A);
    const reported = run(workspace, '--report');
    expect(reported.exitCode).toBe(0);
    expect(reported.stdout).toContain('PINNED-REVISIONS note: 1 whose remote did not answer, reported, not refused');
    expect(reported.stdout).not.toContain('does not serve');
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
