import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { chmod, copyFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { mergeEnv } from '../lib/run.js';
import { scanPublicCommitDenylist } from './public-denylist.js';

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
    ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'SMOOTHBRICKS_PUBLIC_DENYLIST'],
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

  it('judges the formatted index, then the formatted message, refusing either before the commit', async () => {
    const head = git('rev-parse', 'HEAD');
    await stage('first line\nSecret-Consumer\n');
    const staged = runGit(root, 'commit', '-m', 'fix: neutral summary');
    expect(staged.exitCode).not.toBe(0);
    expect(staged.stderr).toContain('staged:notes.txt:2:Secret-Consumer');
    // pre-commit refused after the formatter and before validation, so commit-msg never ran.
    expect((await readFile(order, 'utf8')).trim().split('\n')).toEqual([
      'format',
      'monorepo check-public-denylist --staged',
    ]);

    await rm(order);
    await stage('neutral content\n');
    const body = `${'A neutral explanation of the public change. '.repeat(4)}Secret-Consumer`;
    const message = runGit(root, 'commit', '-m', 'fix: neutral summary   ', '-m', body);
    expect(message.exitCode).not.toBe(0);
    expect(message.stderr).toMatch(/message:\d+:.*Secret-Consumer/);
    expect(git('rev-parse', 'HEAD')).toBe(head);
    // The denylist judged the message the conventional formatter rewrote, not the one git was given.
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

  it('uses the partial commit temporary index instead of the clean real index', async () => {
    await stage('neutral content\n');
    git('commit', '--quiet', '--no-verify', '-m', 'fix: neutral content');
    await writeFile(join(root, 'notes.txt'), 'secret-consumer\n');
    const result = runGit(root, 'commit', '--only', '-m', 'fix: neutral summary', '--', 'notes.txt');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('staged:notes.txt:1:secret-consumer');
    expect(git('show', ':notes.txt')).toBe('neutral content');
  });

  it('exits 1 naming the reason when the scan cannot run, even for neutral added text', async () => {
    git('config', '--add', 'smoothbricks.publicDenylist', '(unclosed');
    await stage('neutral content\n');
    // A hook refuses on any nonzero smoo status (above); this is the status and reason a failed scan reports.
    const child = Bun.spawnSync([process.execPath, SMOO, 'monorepo', 'check-public-denylist', '--staged'], {
      cwd: root,
      env,
    });
    const stderr = child.stderr.toString();
    expect(child.exitCode).toBe(1);
    expect(stderr).toContain('public denylist check did not run');
    expect(stderr).toContain('missing closing parenthesis');
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

    /** Scans the message the commit-msg hook hands smoo once the conventional formatter has rewritten it. */
    async function message(text: string) {
      const path = join(root, '.git', 'COMMIT_EDITMSG');
      await writeFile(path, `fix: neutral summary\n\n${text}\n`);
      return scanPublicCommitDenylist(root, { kind: 'message', path }, env);
    }

    it('refuses a commit message that trips each pattern', async () => {
      for (const line of ['see private-store/receipt.md', 'ran in /work/.cowshed/unit-a', 'asked SeatReviewer']) {
        expect(await message(line)).toEqual({ outcome: 'denied', matches: `message:3:${line}\n` });
      }
    });

    it('passes a relative package path and the same words outside the pattern shapes', async () => {
      const text = 'packages/cowshed/src/lib.rs and .cowshed/workspace.json; a seat reviewer read it';
      expect(await message(text)).toEqual({ outcome: 'clean', patterns: 5 });
    });

    it('never judges added lines: staged text with all three shapes is clean', async () => {
      await stage('private-store/receipt.md\n/work/.cowshed/unit-a\nSeatReviewer\n');
      expect(await scanPublicCommitDenylist(root, { kind: 'staged' }, env)).toEqual({ outcome: 'clean', patterns: 2 });
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

    it('reports only the sibling name staged beside a hunk header that carries the number', async () => {
      await stage(`${hunkHeader}\nSecret-Consumer\n`);
      expect(await scanPublicCommitDenylist(root, { kind: 'staged' }, env)).toEqual({
        outcome: 'denied',
        matches: 'staged:notes.txt:2:Secret-Consumer\n',
      });
    });
  });
});
