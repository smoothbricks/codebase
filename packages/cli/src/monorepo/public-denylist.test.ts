import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mergeEnv } from '../lib/run.js';
import { PUBLIC_DENYLIST_ALLOWLIST_PATH, scanPublicCommitDenylist, scanPublicDenylist } from './public-denylist.js';

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
let container: string;

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
  container = await realpath(await mkdtemp(join(tmpdir(), 'smoo-public-denylist-')));
  root = join(container, 'repository');
  await mkdir(root);
  git('init', '--quiet', '--initial-branch=main');
  git('config', 'user.email', 'denylist@example.test');
  git('config', 'user.name', 'Denylist Test');
  await commit('README.md', 'A neutral readme.\n');
});

afterEach(async () => {
  await rm(container, { recursive: true, force: true });
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

function gitWithEnv(cwd: string, env: Record<string, string>, ...args: string[]): string {
  const child = Bun.spawnSync(['git', ...args], { cwd, env });
  if (child.exitCode !== 0) throw new Error(`git ${args.join(' ')} failed: ${child.stderr.toString()}`);
  return child.stdout.toString().trim();
}

async function sibling(name = 'secret-consumer'): Promise<string> {
  const path = join(container, name);
  await mkdir(path);
  gitWithEnv(path, environment(), 'init', '--quiet');
  return path;
}

async function stage(text: string, path = 'notes.txt'): Promise<void> {
  await writeFile(join(root, path), text);
  git('add', '--', path);
}

async function installAllowlist(text?: string): Promise<void> {
  const source = fileURLToPath(new URL('../../../../tooling/public-denylist-allowlist.tsv', import.meta.url));
  await mkdir(join(root, 'tooling'), { recursive: true });
  const fixture =
    text ?? (await readFile(source, 'utf8')).replace(/^package-scope\t[^\n]+$/m, 'package-scope\t@public-cms');
  await stage(fixture, PUBLIC_DENYLIST_ALLOWLIST_PATH);
}

async function staged(env = environment()) {
  return scanPublicCommitDenylist(root, { kind: 'staged' }, env);
}

describe('commit-time public denylist', () => {
  it('discovers sibling git roots, ignores plain dirs, dot dirs, files and its own basename', async () => {
    // An enclosing git repository must not turn ordinary sibling folders into projects.
    gitWithEnv(container, environment(), 'init', '--quiet');
    await mkdir(join(container, 'tooling'));
    await writeFile(join(container, 'packages'), 'ordinary file');
    await sibling('.secret-consumer');
    await stage('tooling packages repository .secret-consumer\n');
    expect((await staged()).outcome).toBe('clean');

    await sibling();
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:tooling packages repository .secret-consumer\n',
    });
  });

  it('discovers sibling Git worktrees with a .git file', async () => {
    git('worktree', 'add', '--quiet', '--detach', join(container, 'secret-consumer'), 'HEAD');
    await stage('secret-consumer\n');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
  });

  it('matches sibling names case-insensitively as whole words, never substrings', async () => {
    await sibling();
    await stage('prefixsecret-consumer secret-consumers secret-consumer_suffix\nSECRET-CONSUMER\n');
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:2:SECRET-CONSUMER\n',
    });
  });

  it('protects numeric project names without matching larger numeric literals', async () => {
    const project = '1'.repeat(3);
    await sibling(project);
    await stage(`${project}.0 0x${project} 1_${project} ${project}_0 .${project} ${project}.\n`);
    expect((await staged()).outcome).toBe('clean');
    await stage(`/Dev/${project}/x\n${project} project\n`);
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: `staged:notes.txt:1:/Dev/${project}/x\nstaged:notes.txt:2:${project} project\n`,
    });
    const path = join(root, '.git', 'COMMIT_EDITMSG');
    await writeFile(path, `fix: update /Dev/${project}/x\n`);
    expect(await scanPublicCommitDenylist(root, { kind: 'message', path }, environment())).toEqual({
      outcome: 'denied',
      matches: `message:1:fix: update /Dev/${project}/x\n`,
    });
  });

  it('regex-escapes sibling names instead of interpreting their punctuation', async () => {
    await sibling('secret-consumer+(1)');
    await stage('secret-consumer111\nsecret-consumer+(1)\n');
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:2:secret-consumer+(1)\n',
    });
  });

  it('uses the adopted projectRoot, never neighboring shed mount names', async () => {
    const adoptedParent = join(container, 'adopted');
    const adopted = join(adoptedParent, 'repository');
    await mkdir(adopted, { recursive: true });
    await mkdir(join(adoptedParent, 'secret-consumer'));
    gitWithEnv(join(adoptedParent, 'secret-consumer'), environment(), 'init', '--quiet');
    await sibling('other-workspace');
    await mkdir(join(root, '.cowshed'));
    await writeFile(
      join(root, '.cowshed', 'workspace.json'),
      JSON.stringify({ projectRoot: adopted, workspace: 'example' }),
    );
    await stage('other-workspace\nSecret-Consumer\n');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:2:Secret-Consumer\n' });
  });

  it('allows removal and unchanged context containing old private text', async () => {
    await commit('notes.txt', 'secret-consumer\nkeep secret-consumer as old context\nneutral\n');
    await sibling();
    await stage('keep secret-consumer as old context\nnew neutral line\n');
    expect((await staged()).outcome).toBe('clean');
    git('commit', '--quiet', '-m', 'record neutral edit');
    git('rm', '--quiet', 'notes.txt');
    expect((await staged()).outcome).toBe('clean');
  });

  it('judges the staged snapshot, not the working tree or untracked files', async () => {
    await sibling();
    await stage('neutral\n');
    await writeFile(join(root, 'notes.txt'), 'secret-consumer\n');
    await writeFile(join(root, 'untracked.txt'), 'secret-consumer\n');
    expect((await staged()).outcome).toBe('clean');

    git('add', 'notes.txt');
    await writeFile(join(root, 'notes.txt'), 'neutral again\n');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
  });

  it('does not exempt renamed copies of old private lines as unchanged content', async () => {
    await commit('old.txt', 'secret-consumer\n');
    await sibling();
    git('mv', 'old.txt', 'new.txt');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:new.txt:1:secret-consumer\n' });
  });

  it('reports real added line numbers across multiple diff hunks and literal pathspec names', async () => {
    const path = 'notes[1]: extra.txt';
    await commit(path, 'first\nsecond\nthird\nfourth\nfifth\n');
    await sibling();
    await stage('secret-consumer\nsecond\nthird\nfourth\nSecret-Consumer\n', path);
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: `staged:${path}:1:secret-consumer\nstaged:${path}:5:Secret-Consumer\n`,
    });
  });

  it('does not scan type-change patch headers or confuse a staged file named message with a commit message', async () => {
    const path = 'secret-consumer.txt';
    await symlink('neutral-target', join(root, path));
    git('add', path);
    git('commit', '--quiet', '-m', 'base content');
    await sibling();
    await rm(join(root, path));
    await stage('neutral text\n', path);
    expect((await staged()).outcome).toBe('clean');
    await stage('secret-consumer\n', 'message');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:message:1:secret-consumer\n' });
  });

  it('handles an initial commit with no HEAD', async () => {
    git('update-ref', '-d', 'refs/heads/main');
    await sibling();
    await stage('secret-consumer\n');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
  });

  it('respects GIT_INDEX_FILE without inspecting or changing the real index', async () => {
    await sibling();
    await stage('neutral\n');
    const env = { ...environment(), GIT_INDEX_FILE: join(container, 'alternate-index') };
    gitWithEnv(root, env, 'read-tree', 'HEAD');
    await writeFile(join(root, 'notes.txt'), 'secret-consumer\n');
    gitWithEnv(root, env, 'add', 'notes.txt');
    expect(await staged(env)).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
    expect((await staged()).outcome).toBe('clean');
    expect(git('show', ':notes.txt')).toBe('neutral');
  });

  it('isolates sibling probes and scratch grep from the committing repository pointers', async () => {
    await sibling();
    await stage('secret-consumer\n');
    const env = {
      ...environment(),
      GIT_DIR: join(root, '.git'),
      GIT_WORK_TREE: root,
      GIT_COMMON_DIR: join(root, '.git'),
      GIT_INDEX_FILE: join(root, '.git', 'index'),
    };
    expect(await staged(env)).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
    deny('secret-consumer');
    await commit('notes.txt', 'secret-consumer\n');
    expect((await scanPublicDenylist(root, ['HEAD'], env)).outcome).toBe('denied');
  });

  it('reads configured PCRE patterns from config and environment in staged mode', async () => {
    deny('secret\\hconsumer');
    await stage('Secret\tConsumer\nsecret-consumer\n');
    expect(await staged(environment('secret-consumer'))).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:Secret\tConsumer\nstaged:notes.txt:2:secret-consumer\n',
    });
  });

  it('checks commit message lines with the same PCRE and sibling discovery', async () => {
    await sibling();
    deny('secret\\hconsumer');
    const path = join(root, '.git', 'COMMIT_EDITMSG');
    await writeFile(path, 'fix: neutral summary\n\nSecret-Consumer\nsecret\tconsumer\n');
    expect(await scanPublicCommitDenylist(root, { kind: 'message', path }, environment())).toEqual({
      outcome: 'denied',
      matches: 'message:3:Secret-Consumer\nmessage:4:secret\tconsumer\n',
    });
  });

  it('rejects the actual HOME prefix, not neutral user examples or longer home basenames', async () => {
    const env = { ...environment(), HOME: join(container, 'home') };
    await stage(`/Users/example/Dev/project\n${env.HOME}-other/project\n${env.HOME}/project\n${env.HOME}\n`);
    expect(await staged(env)).toEqual({
      outcome: 'denied',
      matches: `staged:notes.txt:3:${env.HOME}/project\nstaged:notes.txt:4:${env.HOME}\n`,
    });
    const path = join(root, '.git', 'COMMIT_EDITMSG');
    await writeFile(path, `fix: neutral\n\n${env.HOME}/project\n`);
    expect(await scanPublicCommitDenylist(root, { kind: 'message', path }, env)).toEqual({
      outcome: 'denied',
      matches: `message:3:${env.HOME}/project\n`,
    });
  });

  it('allows public package and encoded patch references but still denies private prose on their lines', async () => {
    await sibling('public-cms');
    await sibling();
    await installAllowlist();
    await stage(
      'import { cmsIntegration } from "@public-cms/astro-cms/node";\npatches/@public-cms%2Fcontent-api.patch\n',
    );
    expect((await staged()).outcome).toBe('clean');
    await stage('@public-cms/content-api secret-consumer\n@public-cms/content-api public-cms prose\n');
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches:
        'staged:notes.txt:1:@public-cms/content-api secret-consumer\nstaged:notes.txt:2:@public-cms/content-api public-cms prose\n',
    });
  });

  it('keeps explicit PCRE authoritative over public references, including composites and zero-width alternatives', async () => {
    await sibling('public-cms');
    await installAllowlist();
    git('commit', '--quiet', '-m', 'base public references');
    await stage('@public-cms/content-api\n');
    expect((await staged(environment('\\h'))).outcome).toBe('clean');
    expect(await staged(environment('public-cms'))).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:@public-cms/content-api\n',
    });
    await stage('@public-cms/content-api secret-consumer\n');
    expect(await staged(environment('@public-cms/content-api.*secret-consumer'))).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:@public-cms/content-api secret-consumer\n',
    });
    expect(await staged(environment('(?=secret-consumer)'))).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:@public-cms/content-api secret-consumer\n',
    });
    expect(await staged(environment('public-cms|(?=secret-consumer)'))).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:@public-cms/content-api secret-consumer\n',
    });
    const path = join(root, '.git', 'COMMIT_EDITMSG');
    await writeFile(path, 'fix: update @public-cms/content-api\n');
    expect(await scanPublicCommitDenylist(root, { kind: 'message', path }, environment('(?=public-cms)'))).toEqual({
      outcome: 'denied',
      matches: 'message:1:fix: update @public-cms/content-api\n',
    });
  });

  it('compares UTF-8 PCRE columns with byte spans for public references', async () => {
    await sibling('public-cms');
    await sibling();
    await installAllowlist();
    await stage('é @public-cms/content-api\n');
    expect((await staged()).outcome).toBe('clean');
    await stage('é @public-cms/content-api secret-consumer\n');
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:é @public-cms/content-api secret-consumer\n',
    });
  });

  it('allows approved public repository tokens, not another sibling mentioned alongside them', async () => {
    await sibling('git-format-staged');
    await sibling('git-backup');
    await sibling();
    await installAllowlist();
    await stage('git-format-staged and git-backup\n');
    expect((await staged()).outcome).toBe('clean');
    await stage('git-format-staged secret-consumer\n');
    expect(await staged()).toEqual({
      outcome: 'denied',
      matches: 'staged:notes.txt:1:git-format-staged secret-consumer\n',
    });
  });

  it('permits the original attribution only at its exact file and exact text', async () => {
    await sibling();
    await installAllowlist();
    const allowlist = await readFile(join(root, PUBLIC_DENYLIST_ALLOWLIST_PATH), 'utf8');
    const declaration = allowlist.split('\n').find((line) => line.startsWith('line\t'));
    if (!declaration) throw new Error('missing attribution fixture');
    const [, path, attribution] = declaration.split('\t');
    if (!path || !attribution) throw new Error('invalid attribution fixture');
    const project = /\bfrom the (\S+) fork;/.exec(attribution)?.[1];
    if (!project) throw new Error('missing attribution source project');
    await sibling(project);
    await mkdir(join(root, dirname(path)), { recursive: true });
    await stage(`${attribution}\n`, path);
    expect((await staged()).outcome).toBe('clean');
    await stage(`${attribution} secret-consumer\n`, path);
    expect((await staged()).outcome).toBe('denied');
    await stage(`${attribution}\n`, path);
    await stage(`${attribution}\n`);
    expect(await staged()).toEqual({ outcome: 'denied', matches: `staged:notes.txt:1:${attribution}\n` });
  });

  it('applies public reference exemptions to messages without exempting private prose', async () => {
    await sibling('public-cms');
    await sibling();
    await installAllowlist();
    const path = join(root, '.git', 'COMMIT_EDITMSG');
    await writeFile(path, 'fix: update @public-cms/content-api\n');
    expect((await scanPublicCommitDenylist(root, { kind: 'message', path }, environment())).outcome).toBe('clean');
    await writeFile(path, 'fix: update @public-cms/content-api for Secret-Consumer\n');
    expect(await scanPublicCommitDenylist(root, { kind: 'message', path }, environment())).toEqual({
      outcome: 'denied',
      matches: 'message:1:fix: update @public-cms/content-api for Secret-Consumer\n',
    });
  });

  it('never exempts HOME because an allowed package or exact line contains it', async () => {
    const env = { ...environment(), HOME: join(container, 'home') };
    await installAllowlist(`package-scope\t@public-cms\nline\tnotes.txt\t${env.HOME}/project\n`);
    await stage(`${env.HOME}/project\n@public-cms/content-api ${env.HOME}/project\n`);
    const scan = await staged(env);
    expect(scan.outcome).toBe('denied');
    expect(scan.outcome === 'denied' && scan.matches).toContain(`staged:notes.txt:1:${env.HOME}/project`);
    expect(scan.outcome === 'denied' && scan.matches).toContain(
      `staged:notes.txt:2:@public-cms/content-api ${env.HOME}/project`,
    );
  });

  it('does not trust an untracked or unstaged allowlist edit', async () => {
    await sibling();
    await installAllowlist('repository\tgit-backup\n');
    git('commit', '--quiet', '-m', 'base allowlist');
    await writeFile(join(root, PUBLIC_DENYLIST_ALLOWLIST_PATH), 'repository\tsecret-consumer\n');
    await stage('secret-consumer\n');
    expect(await staged()).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
    git('rm', '--cached', '--quiet', PUBLIC_DENYLIST_ALLOWLIST_PATH);
    expect((await staged()).outcome).toBe('denied');
  });

  it('fails closed for invalid regex even when no lines are staged', async () => {
    deny('(unclosed');
    const scan = await staged();
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('missing closing parenthesis');
  });

  it('fails closed for invalid git configuration', async () => {
    const config = join(container, 'bad.gitconfig');
    await writeFile(config, '[unclosed\n');
    const scan = await staged({ ...environment(), GIT_CONFIG_GLOBAL: config });
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('git config');
  });

  it('fails closed for an unreadable or corrupt index', async () => {
    const index = join(container, 'invalid-index');
    await writeFile(index, 'not an index');
    const scan = await staged({ ...environment(), GIT_INDEX_FILE: index });
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('index');
  });

  it('fails closed for malformed workspace markers rather than using shed neighbors', async () => {
    await mkdir(join(root, '.cowshed'));
    const marker = join(root, '.cowshed', 'workspace.json');
    for (const text of ['{broken', '{"projectRoot":123}', '{"projectRoot":"relative"}']) {
      await writeFile(marker, text);
      const scan = await staged();
      expect(scan.outcome).toBe('failed');
      expect(scan.outcome === 'failed' && scan.reason).toContain(marker);
    }
  });

  it('fails closed for filesystem errors and missing adopted anchors', async () => {
    await mkdir(join(root, '.cowshed', 'workspace.json'), { recursive: true });
    let scan = await staged();
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('workspace.json');
    await rm(join(root, '.cowshed', 'workspace.json'), { recursive: true });
    await writeFile(
      join(root, '.cowshed', 'workspace.json'),
      JSON.stringify({ projectRoot: join(container, 'missing') }),
    );
    scan = await staged();
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('missing');
  });

  it('fails closed when a declared sibling git repository is broken', async () => {
    await mkdir(join(container, 'secret-consumer', '.git'), { recursive: true });
    const scan = await staged();
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain('secret-consumer');
    expect(scan.outcome === 'failed' && scan.reason).toContain('git rev-parse');
  });

  it('fails closed for a malformed staged allowlist, naming its line', async () => {
    await installAllowlist('repository\tgit-backup\nunknown\tvalue\n');
    const scan = await staged();
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain(`${PUBLIC_DENYLIST_ALLOWLIST_PATH}:2`);
  });

  it('fails closed for a missing message file, naming the file', async () => {
    const path = join(root, '.git', 'missing-message');
    const scan = await scanPublicCommitDenylist(root, { kind: 'message', path }, environment());
    expect(scan.outcome).toBe('failed');
    expect(scan.outcome === 'failed' && scan.reason).toContain(path);
  });

  it('refuses invalid HOME instead of silently disabling personal-path protection', async () => {
    for (const home of ['', '/', 'relative']) {
      const scan = await staged({ ...environment(), HOME: home });
      expect(scan.outcome).toBe('failed');
      expect(scan.outcome === 'failed' && scan.reason).toContain('HOME');
    }
  });
});
