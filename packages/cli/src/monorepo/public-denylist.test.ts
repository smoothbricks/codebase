import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mergeEnv } from '../lib/run.js';
import { scanPublicDenylist } from './public-denylist.js';

/**
 * The environment the check and the fixture's own git commands run under: the fixture repository's
 * config is the only config, so a denylist in the developer's global or system config cannot decide
 * these results, and inherited `GIT_DIR`-style variables (set when the suite runs under a hook or
 * `rebase --exec`) cannot point git at the repository running the tests.
 */
function environment(denylist?: string): Record<string, string> {
  const env = mergeEnv({ GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_SYSTEM: '/dev/null' }, [
    'GIT_DIR',
    'GIT_WORK_TREE',
    'GIT_INDEX_FILE',
    'GIT_COMMON_DIR',
    'SMOOTHBRICKS_PUBLIC_DENYLIST',
  ]);
  if (denylist !== undefined) {
    env.SMOOTHBRICKS_PUBLIC_DENYLIST = denylist;
  }
  return env;
}

let root: string;

function git(...args: string[]): string {
  const child = Bun.spawnSync(['git', ...args], { cwd: root, env: environment() });
  if (child.exitCode !== 0) {
    throw new Error(`git ${args.join(' ')} failed: ${child.stderr.toString()}`);
  }
  return child.stdout.toString().trim();
}

async function commit(path: string, content: string): Promise<string> {
  await writeFile(join(root, path), content);
  git('add', path);
  git('commit', '--quiet', '-m', `add ${path}`);
  return git('rev-parse', 'HEAD');
}

function deny(pattern: string): void {
  git('config', '--add', 'smoothbricks.publicDenylist', pattern);
}

function commitMessage(...paragraphs: string[]): string {
  git('commit', '--quiet', '--allow-empty', ...paragraphs.flatMap((paragraph) => ['-m', paragraph]));
  return git('rev-parse', 'HEAD');
}

beforeEach(async () => {
  // Real path: git reports the resolved root, and macOS's temporary directory is a symlink.
  root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-public-denylist-')));
  git('init', '--quiet', '--initial-branch=main');
  git('config', 'user.email', 'denylist@example.test');
  git('config', 'user.name', 'Denylist Test');
  await commit('README.md', 'A neutral readme.\n');
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

describe('public denylist scan', () => {
  it('denies a tree that contains a configured word in any casing, naming revision, file and line', async () => {
    deny('secret-codename');
    deny('other-codename');
    await commit('notes.md', 'first line\nWe borrowed this from Secret-Codename.\n');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({
      outcome: 'denied',
      matches: 'HEAD:notes.md:2:We borrowed this from Secret-Codename.\n',
    });
  });

  it('passes a tree that contains none of the configured words', async () => {
    deny('secret-codename');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({ outcome: 'clean', patterns: 1 });
  });

  it('is unconfigured, not a match, when no denylist exists, whatever the tree contains', async () => {
    await commit('notes.md', 'secret-codename\n');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({ outcome: 'unconfigured' });
  });

  it('reads one pattern per line of SMOOTHBRICKS_PUBLIC_DENYLIST alongside the git config', async () => {
    deny('config-codename');
    await commit('notes.md', 'only the second-codename appears here\n');

    expect(await scanPublicDenylist(root, ['HEAD'], environment('first-codename\nsecond-codename\n'))).toEqual({
      outcome: 'denied',
      matches: 'HEAD:notes.md:1:only the second-codename appears here\n',
    });
    expect(await scanPublicDenylist(root, ['HEAD'], environment('first-codename\n\n'))).toEqual({
      outcome: 'clean',
      patterns: 2,
    });
  });

  it('judges the given revisions, not the working tree', async () => {
    deny('secret-codename');
    git('checkout', '--quiet', '-b', 'side');
    const side = await commit('notes.md', 'secret-codename\n');
    git('checkout', '--quiet', 'main');
    await writeFile(join(root, 'README.md'), 'an uncommitted secret-codename edit\n');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({ outcome: 'clean', patterns: 1 });
    expect(await scanPublicDenylist(root, ['HEAD', side], environment())).toEqual({
      outcome: 'denied',
      matches: `${side}:notes.md:1:secret-codename\n`,
    });
  });

  it('fails instead of passing when a configured pattern is not a valid regex', async () => {
    deny('(unclosed');

    const scan = await scanPublicDenylist(root, ['HEAD'], environment());

    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('missing closing parenthesis');
  });

  it('denies a commit message that contains a configured word, naming sha and line', async () => {
    deny('acme-secret');
    const sha = commitMessage('tidy the adapters', 'Moves the Acme-Secret adapter\nout of core.');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({
      outcome: 'denied',
      matches: `${sha}:message:3:Moves the Acme-Secret adapter\n`,
    });
  });

  it('passes commit messages that contain none of the configured words', async () => {
    deny('acme-secret');
    commitMessage('tidy the adapters', 'No private names here.');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({ outcome: 'clean', patterns: 1 });
  });

  it('judges a commit message with the same PCRE as a tree', async () => {
    // \h is PCRE's horizontal space; JavaScript reads it as a literal "h".
    deny('acme\\hsecret');
    const sha = commitMessage('rename Acme\tSecret');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({
      outcome: 'denied',
      matches: `${sha}:message:1:rename Acme\tSecret\n`,
    });
  });

  it('reports a commit message before the tree it published', async () => {
    deny('acme-secret');
    await writeFile(join(root, 'notes.md'), 'acme-secret\n');
    git('add', 'notes.md');
    const sha = commitMessage('document acme-secret');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({
      outcome: 'denied',
      matches: `${sha}:message:1:document acme-secret\nHEAD:notes.md:1:acme-secret\n`,
    });
  });

  it('judges every commit behind the revisions, not only the tip', async () => {
    deny('acme-secret');
    const old = commitMessage('mention acme-secret');
    commitMessage('a clean follow-up');

    expect(await scanPublicDenylist(root, ['HEAD'], environment())).toEqual({
      outcome: 'denied',
      matches: `${old}:message:1:mention acme-secret\n`,
    });
  });

  it('given a remote, judges only the commits that remote does not hold', async () => {
    deny('acme-secret');
    const published = commitMessage('mention acme-secret');
    git('update-ref', 'refs/remotes/origin/main', published);
    commitMessage('a clean follow-up');

    expect(await scanPublicDenylist(root, ['HEAD'], environment(), 'origin')).toEqual({
      outcome: 'clean',
      patterns: 1,
    });
    // A commit another remote holds is still unpublished to this one.
    expect(await scanPublicDenylist(root, ['HEAD'], environment(), 'private')).toEqual({
      outcome: 'denied',
      matches: `${published}:message:1:mention acme-secret\n`,
    });
  });

  it('judges an unpublished commit message even when the remote holds its parents', async () => {
    deny('acme-secret');
    git('update-ref', 'refs/remotes/origin/main', 'HEAD');
    const unpublished = commitMessage('mention acme-secret');

    expect(await scanPublicDenylist(root, ['HEAD'], environment(), 'origin')).toEqual({
      outcome: 'denied',
      matches: `${unpublished}:message:1:mention acme-secret\n`,
    });
  });

  it('refuses an empty remote name instead of reading it as every remote', async () => {
    deny('acme-secret');
    commitMessage('mention acme-secret');
    git('update-ref', 'refs/remotes/origin/main', 'HEAD');

    const scan = await scanPublicDenylist(root, ['HEAD'], environment(), '');

    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('empty remote name');
  });

  it('fails on a bad pattern even when the remote already holds every commit', async () => {
    deny('(unclosed');
    git('update-ref', 'refs/remotes/origin/main', 'HEAD');

    const scan = await scanPublicDenylist(root, ['HEAD'], environment(), 'origin');

    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('missing closing parenthesis');
  });

  it('fails instead of passing when a revision does not resolve', async () => {
    deny('acme-secret');

    const scan = await scanPublicDenylist(root, ['no-such-revision'], environment());

    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('no-such-revision');
  });
});
