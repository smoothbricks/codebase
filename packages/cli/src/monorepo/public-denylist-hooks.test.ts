import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { chmod, copyFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { mergeEnv } from '../lib/run.js';

const SMOO = fileURLToPath(new URL('../../bin/smoo', import.meta.url));
const checkout = fileURLToPath(new URL('../../../../', import.meta.url));

let container: string;
let root: string;
let order: string;
let env: Record<string, string>;

function runGit(cwd: string, ...args: string[]) {
  const child = Bun.spawnSync(['git', ...args], { cwd, env });
  return { exitCode: child.exitCode, stdout: child.stdout.toString(), stderr: child.stderr.toString() };
}

function git(...args: string[]): string {
  const result = runGit(root, ...args);
  if (result.exitCode !== 0) throw new Error(`git ${args.join(' ')} failed: ${result.stderr}`);
  return result.stdout.trim();
}

async function executable(path: string, text: string): Promise<void> {
  await writeFile(path, text);
  await chmod(path, 0o755);
}

async function stage(text: string): Promise<void> {
  await writeFile(join(root, 'notes.txt'), text);
  git('add', 'notes.txt');
}

beforeEach(async () => {
  container = await realpath(await mkdtemp(join(tmpdir(), 'smoo-public-hooks-')));
  root = join(container, 'repository');
  order = join(container, 'hook-order.txt');
  await mkdir(root);
  await mkdir(join(container, 'home'));
  env = mergeEnv(
    {
      HOME: join(container, 'home'),
      GIT_CONFIG_GLOBAL: '/dev/null',
      GIT_CONFIG_SYSTEM: '/dev/null',
      SMOO_TEST_ORDER: order,
      SMOO_TEST_PATH: process.env.PATH ?? '',
      SMOO_TEST_BUN: process.execPath,
      SMOO_TEST_LAUNCHER: SMOO,
    },
    [
      'GIT_DIR',
      'GIT_WORK_TREE',
      'GIT_COMMON_DIR',
      'GIT_INDEX_FILE',
      'GIT_PREFIX',
      'SMOOTHBRICKS_PUBLIC_DENYLIST',
      'SMOO_REF_LOCK_HOLDER',
    ],
  );
  git('init', '--quiet', '--initial-branch=main');
  git('config', 'user.email', 'hooks@example.test');
  git('config', 'user.name', 'Hook Test');
  await writeFile(
    join(root, 'package.json'),
    JSON.stringify({ name: '@example/fixture', private: true, workspaces: [] }),
  );
  git('add', 'package.json');
  git('commit', '--quiet', '--no-verify', '-m', 'chore: initialize fixture');
  await mkdir(join(container, 'secret-consumer'));
  const sibling = runGit(join(container, 'secret-consumer'), 'init', '--quiet');
  if (sibling.exitCode !== 0) throw new Error(sibling.stderr);

  await mkdir(join(root, 'tooling', 'direnv'), { recursive: true });
  await mkdir(join(root, 'tooling', 'git-hooks'), { recursive: true });
  await executable(
    join(root, 'tooling', 'direnv', 'repo-path'),
    '#!/usr/bin/env bash\nprintf "%s\\n" "$PWD/tooling:$SMOO_TEST_PATH"\n',
  );
  await writeFile(
    join(root, 'tooling', 'direnv', 'ttsc-env.sh'),
    '# Fixture toolchain needs no extra compiler environment.\n',
  );
  // Only the unrelated formatter is a fixture. Every smoo invocation dispatches the real CLI.
  await executable(
    join(root, 'tooling', 'git-format-staged'),
    '#!/usr/bin/env bash\nprintf "format\\n" >> "$SMOO_TEST_ORDER"\n',
  );
  await executable(
    join(root, 'tooling', 'smoo'),
    '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$SMOO_TEST_ORDER"\nexec "$SMOO_TEST_BUN" "$SMOO_TEST_LAUNCHER" "$@"\n',
  );
  for (const hook of ['pre-commit', 'commit-msg']) {
    const target = join(root, 'tooling', 'git-hooks', `${hook}.sh`);
    await copyFile(join(managedAssetsRoot, 'raw', 'tooling', 'git-hooks', `${hook}.sh`), target);
    await chmod(target, 0o755);
    await executable(
      join(root, '.git', 'hooks', hook),
      `#!/usr/bin/env bash\nexec "$(git rev-parse --show-toplevel)/tooling/git-hooks/${hook}.sh" "$@"\n`,
    );
  }
});

afterEach(async () => {
  await rm(container, { recursive: true, force: true });
});

describe('managed public denylist commit hooks', () => {
  it('keeps generated hooks identical to the managed raw scripts', async () => {
    for (const hook of ['pre-commit', 'commit-msg']) {
      expect(await readFile(join(checkout, 'tooling', 'git-hooks', `${hook}.sh`), 'utf8')).toBe(
        await readFile(join(managedAssetsRoot, 'raw', 'tooling', 'git-hooks', `${hook}.sh`), 'utf8'),
      );
    }
  });

  it('blocks a real commit of a staged sibling name after the formatter, before validation', async () => {
    await stage('first line\nSecret-Consumer\n');
    const head = git('rev-parse', 'HEAD');
    const result = runGit(root, 'commit', '-m', 'fix: neutral summary');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('staged:notes.txt:2:Secret-Consumer');
    expect(git('rev-parse', 'HEAD')).toBe(head);
    expect((await readFile(order, 'utf8')).trim().split('\n')).toEqual([
      'format',
      'monorepo check-public-denylist --staged',
    ]);
  });

  it('blocks a real commit of the actual HOME prefix while neutral examples pass', async () => {
    await stage('/Users/example/Dev/project\n');
    expect(runGit(root, 'commit', '-m', 'fix: neutral example').exitCode).toBe(0);
    await stage(`/Users/example/Dev/project\n${env.HOME}/project\n`);
    const result = runGit(root, 'commit', '-m', 'fix: neutral summary');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain(`staged:notes.txt:2:${env.HOME}/project`);
  });

  it('blocks a real sibling-name message only after the conventional formatter has rewritten it', async () => {
    await stage('neutral content\n');
    const body = `${'A neutral explanation of the public change. '.repeat(4)}Secret-Consumer`;
    const head = git('rev-parse', 'HEAD');
    const result = runGit(root, 'commit', '-m', 'fix: neutral summary   ', '-m', body);
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toMatch(/message:\d+:.*Secret-Consumer/);
    expect(git('rev-parse', 'HEAD')).toBe(head);
    const formatted = await readFile(join(root, '.git', 'COMMIT_EDITMSG'), 'utf8');
    expect(formatted.startsWith('fix: neutral summary\n')).toBe(true);
    expect(formatted).not.toContain(body);
    const calls = (await readFile(order, 'utf8')).trim().split('\n');
    expect(calls[0]).toBe('format');
    expect(calls[1]).toBe('monorepo check-public-denylist --staged');
    expect(calls[2]).toBe('monorepo validate --fail-fast --only-if-new-workspace-package');
    expect(calls[3]).toContain('monorepo validate-commit-msg --fix ');
    expect(calls[4]).toContain('monorepo check-public-denylist --commit-message ');
  });

  it('blocks the actual HOME prefix in a real commit message', async () => {
    await stage('neutral content\n');
    const result = runGit(root, 'commit', '-m', 'fix: neutral summary', '-m', `${env.HOME}/project`);
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain(`message:3:${env.HOME}/project`);
  });

  it('uses the partial commit temporary index instead of the clean real index', async () => {
    await stage('neutral content\n');
    git('commit', '-m', 'fix: neutral content');
    await writeFile(join(root, 'notes.txt'), 'secret-consumer\n');
    const result = runGit(root, 'commit', '--only', '-m', 'fix: neutral summary', '--', 'notes.txt');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('staged:notes.txt:1:secret-consumer');
    expect(git('show', ':notes.txt')).toBe('neutral content');
  });

  it('blocks a private added line in the first commit with no HEAD', async () => {
    git('update-ref', '-d', 'refs/heads/main');
    await stage('secret-consumer\n');
    const result = runGit(root, 'commit', '-m', 'chore: initial content');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('staged:notes.txt:1:secret-consumer');
    expect(runGit(root, 'rev-parse', '--verify', 'HEAD').exitCode).not.toBe(0);
  });

  it('fails closed through the real hook for invalid PCRE even with neutral added text', async () => {
    git('config', '--add', 'smoothbricks.publicDenylist', '(unclosed');
    await stage('neutral content\n');
    const result = runGit(root, 'commit', '-m', 'fix: neutral summary');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('public denylist check did not run');
    expect(result.stderr).toContain('missing closing parenthesis');
  });

  describe('message-only patterns', () => {
    // Stand-ins for a repository's own list: a local store path, an absolute workspace path, and a
    // case-sensitive generated name (judged under `git grep -i`, so the pattern opts out with (?-i)).
    const patterns = [
      'private-store/',
      '/[.]cowshed/|/cowshed/store/',
      '(?-i)(?<![A-Za-z0-9_])Seat[A-Z][A-Za-z0-9]*(?![A-Za-z0-9_])',
    ];
    beforeEach(() => {
      for (const pattern of patterns) git('config', '--add', 'smoothbricks.commitMessageDenylist', pattern);
    });

    it('refuses a commit message that trips each pattern', async () => {
      await stage('neutral content\n');
      for (const line of ['see private-store/receipt.md', 'ran in /work/.cowshed/unit-a', 'asked SeatReviewer']) {
        const head = git('rev-parse', 'HEAD');
        const result = runGit(root, 'commit', '-m', 'fix: neutral summary', '-m', line);
        expect(result.exitCode).not.toBe(0);
        expect(result.stderr).toContain(`message:3:${line}`);
        expect(git('rev-parse', 'HEAD')).toBe(head);
      }
    });

    it('passes a relative package path and the same words outside the pattern shapes', async () => {
      await stage('neutral content\n');
      const result = runGit(
        root,
        'commit',
        '-m',
        'fix: neutral summary',
        '-m',
        'packages/cowshed/src/lib.rs and .cowshed/workspace.json; a seat reviewer read it',
      );
      expect(result.stderr).toBe('');
      expect(result.exitCode).toBe(0);
    });

    it('never judges added lines: staged text with all three shapes commits under a neutral message', async () => {
      await stage('private-store/receipt.md\n/work/.cowshed/unit-a\nSeatReviewer\n');
      const result = runGit(root, 'commit', '-m', 'fix: neutral summary');
      expect(result.stderr).toBe('');
      expect(result.exitCode).toBe(0);
      expect(git('show', 'HEAD:notes.txt')).toBe('private-store/receipt.md\n/work/.cowshed/unit-a\nSeatReviewer');
    });
  });

  describe('digits-only sibling directory', () => {
    // A stray data directory is a git root beside the checkout, but it is not a project.
    const hunkHeader = '@@ -379,36 +384,111 @@';

    beforeEach(async () => {
      await mkdir(join(container, '111'));
      const stray = runGit(join(container, '111'), 'init', '--quiet');
      if (stray.exitCode !== 0) throw new Error(stray.stderr);
    });

    it('commits a hunk header and a message that carry the number, through both real hooks', async () => {
      await stage(`${hunkHeader}\n`);
      const result = runGit(root, 'commit', '-m', 'fix: neutral summary', '-m', `Refresh the ${hunkHeader} hunk.`);
      expect(result.stderr).toBe('');
      expect(result.exitCode).toBe(0);
      expect(git('show', 'HEAD:notes.txt')).toBe(hunkHeader);
      const calls = (await readFile(order, 'utf8')).trim().split('\n');
      expect(calls).toContain('monorepo check-public-denylist --staged');
      expect(calls.some((call) => call.startsWith('monorepo check-public-denylist --commit-message '))).toBe(true);
    });

    it('still refuses a real sibling name staged beside the hunk header, reporting only that line', async () => {
      await stage(`${hunkHeader}\nSecret-Consumer\n`);
      const head = git('rev-parse', 'HEAD');
      const result = runGit(root, 'commit', '-m', 'fix: neutral summary');
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain('staged:notes.txt:2:Secret-Consumer');
      expect(result.stderr).not.toContain(hunkHeader);
      expect(git('rev-parse', 'HEAD')).toBe(head);
    });
  });
});

// The formatter fixture appends to `order`, so its absence proves the hook refused before rewriting the index.
describe('managed pre-commit main checkout guard', () => {
  /**
   * cowshed's in-image marker, which names the checkout's role (cowshed 01_storage.md). projectRoot is main's
   * checkout, here the fixture itself, beside which the public denylist discovers siblings.
   */
  async function role(value: 'main' | 'workspace'): Promise<void> {
    await mkdir(join(root, '.cowshed'), { recursive: true });
    const marker = { version: 1, projectRoot: root, workspace: value === 'main' ? 'main' : 'shed', role: value };
    await writeFile(join(root, '.cowshed', 'workspace.json'), `${JSON.stringify(marker, null, 2)}\n`);
  }

  /** Runs `script` in bash at the fixture root with LOCK naming the fixture's ref lock file. */
  function bash(script: string, lock: string) {
    const child = Bun.spawnSync(['bash', '-c', script], { cwd: root, env: { ...env, LOCK: lock } });
    return { exitCode: child.exitCode, stderr: child.stderr.toString() };
  }

  it('refuses a commit in main without a marker before the formatter rewrites the index', async () => {
    await role('main');
    await stage('neutral content\n');
    const head = git('rev-parse', 'HEAD');
    const result = runGit(root, 'commit', '-m', 'docs: bare commit in main');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('SMOO_REF_LOCK_HOLDER is not set');
    expect(result.stderr).toContain("~/Dev/.land-lock.sh <repo> git [-C <main>] commit -m '…' -- <paths>");
    expect(result.stderr).toContain('report that failure instead of committing here');
    expect(git('rev-parse', 'HEAD')).toBe(head);
    expect(await Bun.file(order).exists()).toBe(false);
  });

  it('commits in main under an ancestor that holds the named lock open', async () => {
    await role('main');
    await stage('neutral content\n');
    const head = git('rev-parse', 'HEAD');
    const lock = join(container, 'ref.lock');
    // bash holds the lock file open and stays git's parent: the trailing exit keeps it from exec'ing git.
    const result = bash(
      'exec 9>>"$LOCK"\nSMOO_REF_LOCK_HOLDER="$$:$LOCK" git commit -m "docs: commit under the ref lock"\nexit $?',
      lock,
    );
    expect(result).toMatchObject({ exitCode: 0 });
    expect(git('rev-parse', 'HEAD^')).toBe(head);
    expect(await Bun.file(order).exists()).toBe(true);
  });

  it.skipIf(process.platform !== 'darwin')(
    'commits in main under lockf, named by the shell that execs it',
    async () => {
      await role('main');
      await stage('neutral content\n');
      const head = git('rev-parse', 'HEAD');
      const lock = join(container, 'ref.lock');
      // `$$` expands before the exec, so it names the lockf process, which holds the lock while git runs.
      const result = bash(
        'exec lockf -k "$LOCK" env SMOO_REF_LOCK_HOLDER="$$:$LOCK" git commit -m "docs: commit under lockf"',
        lock,
      );
      expect(result).toMatchObject({ exitCode: 0 });
      expect(git('rev-parse', 'HEAD^')).toBe(head);
    },
  );

  it('commits without a marker in a shed and in a checkout cowshed never adopted', async () => {
    await stage('plain clone\n');
    expect(runGit(root, 'commit', '-m', 'docs: commit in a plain clone')).toMatchObject({ exitCode: 0 });
    await role('workspace');
    await stage('shed\n');
    const result = runGit(root, 'commit', '-m', 'docs: commit in a shed');
    expect(result).toMatchObject({ exitCode: 0 });
  });

  it('moves main without a marker by fast-forward and rebase, which run no pre-commit hook', async () => {
    await role('main');
    git('checkout', '--quiet', '-b', 'shed');
    await writeFile(join(root, 'landed.txt'), 'landed\n');
    git('add', 'landed.txt');
    git('commit', '--quiet', '--no-verify', '-m', 'docs: landed change');
    git('checkout', '--quiet', 'main');
    expect(runGit(root, 'merge', '--ff-only', 'shed')).toMatchObject({ exitCode: 0 });
    expect(git('rev-parse', 'HEAD')).toBe(git('rev-parse', 'shed'));

    git('checkout', '--quiet', 'shed');
    await writeFile(join(root, 'upstream.txt'), 'upstream\n');
    git('add', 'upstream.txt');
    git('commit', '--quiet', '--no-verify', '-m', 'docs: upstream change');
    git('checkout', '--quiet', 'main');
    await writeFile(join(root, 'local.txt'), 'local\n');
    git('add', 'local.txt');
    git('commit', '--quiet', '--no-verify', '-m', 'docs: local change');
    const rebase = runGit(root, 'rebase', 'shed');
    expect(rebase).toMatchObject({ exitCode: 0 });
    expect(git('rev-parse', 'HEAD^')).toBe(git('rev-parse', 'shed'));
    expect(await Bun.file(order).exists()).toBe(false);
  });

  it('refuses a marker whose holder is alive and holds the lock but is not an ancestor', async () => {
    await role('main');
    await stage('neutral content\n');
    const head = git('rev-parse', 'HEAD');
    const lock = join(container, 'ref.lock');
    const stale = Bun.spawn(['bash', '-c', 'exec 9>>"$LOCK"\necho ready\nexec sleep 60'], {
      env: { ...env, LOCK: lock },
      stdout: 'pipe',
    });
    try {
      const ready = await stale.stdout.getReader().read();
      expect(new TextDecoder().decode(ready.value)).toBe('ready\n');
      const result = bash(
        `SMOO_REF_LOCK_HOLDER="${stale.pid}:$LOCK" git commit -m "docs: stale marker"\nexit $?`,
        lock,
      );
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain(`names process ${stale.pid}, which is not an ancestor of this commit`);
      expect(git('rev-parse', 'HEAD')).toBe(head);
      expect(await Bun.file(order).exists()).toBe(false);
    } finally {
      stale.kill();
      await stale.exited;
    }
  });

  it('refuses a forged marker: malformed, a missing lock, or an ancestor that does not hold the lock', async () => {
    await role('main');
    await stage('neutral content\n');
    const head = git('rev-parse', 'HEAD');
    const lock = join(container, 'ref.lock');
    await writeFile(lock, '');

    const malformed = bash('SMOO_REF_LOCK_HOLDER="$LOCK" git commit -m "docs: malformed marker"\nexit $?', lock);
    expect(malformed.exitCode).not.toBe(0);
    expect(malformed.stderr).toContain('is not <pid>:<absolute lock file>');

    const missing = bash(
      `SMOO_REF_LOCK_HOLDER="$$:${join(container, 'absent.lock')}" git commit -m "docs: missing lock"\nexit $?`,
      lock,
    );
    expect(missing.exitCode).not.toBe(0);
    expect(missing.stderr).toContain('absent.lock, which does not exist');

    // The named bash is git's parent, but it never opened the lock: setting the marker by hand proves nothing.
    const unheld = bash('SMOO_REF_LOCK_HOLDER="$$:$LOCK" git commit -m "docs: hand-set marker"\nexit $?', lock);
    expect(unheld.exitCode).not.toBe(0);
    expect(unheld.stderr).toMatch(/process \d+ does not hold .*ref\.lock open/);

    expect(git('rev-parse', 'HEAD')).toBe(head);
    expect(await Bun.file(order).exists()).toBe(false);
  });
});
