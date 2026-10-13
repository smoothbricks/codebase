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
});
