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
});
