import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readlinkSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, parse, relative, resolve } from 'node:path';
import { $ } from 'bun';
import { decode, run } from '../lib/run.js';

// post-commit is the one hook slot smoo shares. Tools that nudge a backup or a
// mirror after every commit (git-backup, git-auto-remote) install themselves by
// appending a fenced block to whatever hook file is already there, so a symlink
// is doubly wrong here: it would delete their block, and their next install
// would write through the link into the managed template. Own a fenced block
// that calls the managed script instead, which is the convention they document.
const POST_COMMIT_BEGIN = '# >>> smoo post-commit >>>';
const POST_COMMIT_END = '# <<< smoo post-commit <<<';
const POST_COMMIT_BLOCK = [
  POST_COMMIT_BEGIN,
  '# Restore the index for the paths the commit just wrote: a partial commit',
  '# (git commit --only -- <paths>) builds its tree from the worktree, so the',
  '# pre-commit formatter never reaches the real index. Mechanism in the script.',
  '"$(git rev-parse --show-toplevel)/tooling/git-hooks/post-commit.sh"',
  POST_COMMIT_END,
].join('\n');

/**
 * Wires the repository config and git hooks of the checkout at `root`. Shell entries managed
 * before the direnv setup stopped importing workspace packages call this as their last
 * bootstrap step, so a refused write — a sandbox that denies `.git/hooks`, a read-only checkout —
 * takes their shell down no more than an install does: it is reported, with the path and errno
 * it hit, and this returns.
 */
export async function applyWorkspaceGitConfig(root: string): Promise<void> {
  try {
    await wireWorkspaceGit(root);
  } catch (error) {
    console.error(`--- WARNING: applying the workspace git config in ${root} failed: ${error}`);
    console.error('Git hooks and repository config were not applied; the shell is loaded without them.\n---');
  }
}

async function wireWorkspaceGit(root: string): Promise<void> {
  // Located on disk, never through `git rev-parse`: a stale include or a denied config must not be
  // followed to find where the repository's config and hooks live.
  const config = repositoryConfig(root);
  const gitDir = dirname(config.file);
  const tooling = join(config.checkout, 'tooling');

  await keepRepositoryConfig(config);
  linkHook(gitDir, tooling, 'pre-commit');
  installPostCommitHook(gitDir, tooling);
  linkHook(gitDir, tooling, 'commit-msg');
  linkHook(gitDir, tooling, 'pre-push');
}

function linkHook(gitDir: string, tooling: string, name: string): void {
  const source = join(tooling, 'git-hooks', `${name}.sh`);
  if (!existsSync(source)) {
    throw new Error(`Missing ${name} hook source: ${source}`);
  }

  const target = join(gitDir, 'hooks', name);
  const relativeTarget = relative(dirname(target), source);
  if (readLinkOrNull(target) === relativeTarget) {
    return;
  }

  mkdirSync(dirname(target), { recursive: true });
  rmSync(target, { force: true });
  symlinkSync(relativeTarget, target);
}

function installPostCommitHook(gitDir: string, tooling: string): void {
  const source = join(tooling, 'git-hooks', 'post-commit.sh');
  if (!existsSync(source)) {
    throw new Error(`Missing post-commit hook source: ${source}`);
  }

  const target = join(gitDir, 'hooks', 'post-commit');
  const link = readLinkOrNull(target);
  if (link !== null) {
    // Never append through a symlink: that writes into whatever it points at.
    // Say which link went, so a hook belonging to something else can be put
    // back as a block beside ours instead of vanishing silently.
    console.warn(`Replaced symlinked post-commit hook (was ${link}) with a chainable block`);
  }

  const existing = link === null && existsSync(target) ? readFileSync(target, 'utf8') : '';
  const next = postCommitHookFile(existing);
  if (existing === next) {
    // A hook that is not executable is a hook git silently skips. A correct hook that is
    // write-denied is left as it is, so only a missing execute bit is repaired.
    if ((statSync(target).mode & 0o111) === 0) chmodSync(target, 0o755);
    return;
  }

  mkdirSync(dirname(target), { recursive: true });
  rmSync(target, { force: true });
  writeFileSync(target, next, { mode: 0o755 });
}

/**
 * The hook file to write: every foreign block preserved, ours replaced rather
 * than repeated, and a shebang when we are the ones creating the file.
 */
function postCommitHookFile(existing: string): string {
  const kept = stripBlock(existing, POST_COMMIT_BEGIN, POST_COMMIT_END).replace(/\s+$/, '');
  return `${kept === '' ? '#!/usr/bin/env bash' : kept}\n\n${POST_COMMIT_BLOCK}\n`;
}

function stripBlock(content: string, begin: string, end: string): string {
  const lines = content.split('\n');
  const start = lines.findIndex((line) => line.trim() === begin);
  if (start === -1) {
    return content;
  }

  // A block whose end marker was lost runs to the end of the file: it is ours
  // to replace either way, and leaving half of it behind would double it.
  const offset = lines.slice(start + 1).findIndex((line) => line.trim() === end);
  const resume = offset === -1 ? lines.length : start + offset + 2;
  return [...lines.slice(0, start), ...lines.slice(resume)].join('\n');
}

/** The config file whose include it owns: a standalone checkout's own, or a linked worktree's primary. */
interface RepositoryConfig {
  file: string;
  checkout: string;
}

/**
 * Locates the config on disk as the managed setup does: a `.git` directory, or a gitdir pointer
 * whose `commondir` names the primary repository. Never a git command, so a broken include or a
 * denied config is not followed to find it.
 */
function repositoryConfig(root: string): RepositoryConfig {
  const dotGit = join(root, '.git');
  if (statSync(dotGit).isDirectory()) {
    return { file: join(dotGit, 'config'), checkout: root };
  }
  const pointer = /^gitdir: (.+)$/m.exec(readFileSync(dotGit, 'utf8'));
  if (pointer?.[1] === undefined) {
    throw new Error(`${dotGit} is neither a git directory nor a gitdir pointer`);
  }
  const gitDir = resolve(root, pointer[1].trim());
  if (!existsSync(join(gitDir, 'commondir'))) {
    return { file: join(gitDir, 'config'), checkout: root };
  }
  const common = resolve(gitDir, readFileSync(join(gitDir, 'commondir'), 'utf8').trim());
  return { file: join(common, 'config'), checkout: dirname(common) };
}

function readLinkOrNull(path: string): string | null {
  try {
    return readlinkSync(path);
  } catch {
    return null;
  }
}

export const postCommitHookFileForTest = postCommitHookFile;

/**
 * Converges the checkout's repository config on the managed settings, as the managed direnv setup
 * does: the config file is read and written with `--file` from outside the repository, so no
 * stale or write-denied include is followed during repair. Only keys that differ are written;
 * `include.path` is spelled relative to the config's own directory, so a copy-on-write clone
 * names its own workspace file wherever it sits.
 */
async function keepRepositoryConfig(config: RepositoryConfig): Promise<void> {
  const configFile = config.file;
  const outside = parse(configFile).root;
  const settings: { key: string; value: string; replaces?: string }[] = [
    {
      key: 'include.path',
      value: relative(dirname(configFile), join(config.checkout, 'tooling', 'workspace.gitconfig')),
      replaces: '(^|/)tooling/workspace\\.gitconfig$',
    },
    { key: 'merge.smoo-newer-pins.name', value: 'keep the newer devenv/nvfetcher runtime pins' },
    { key: 'merge.smoo-newer-pins.driver', value: 'bash tooling/direnv/merge-newer-pins.sh %O %A %B %P' },
  ];
  const keys = `^(${settings.map((setting) => setting.key.replaceAll('.', '\\.')).join('|')})$`;
  const read = await $`git config --file ${configFile} --get-regexp ${keys}`.cwd(outside).quiet().nothrow();
  // Exit 1 is git's "no such key": nothing is set yet.
  if (read.exitCode !== 0 && read.exitCode !== 1) {
    throw new Error(
      `git config --file ${configFile} --get-regexp failed with exit code ${read.exitCode}: ${decode(read.stderr).trim()}`,
    );
  }
  const current = new Map<string, string[]>();
  for (const line of decode(read.stdout).split('\n')) {
    if (line === '') continue;
    const space = line.indexOf(' ');
    const key = space === -1 ? line : line.slice(0, space);
    current.set(key, [...(current.get(key) ?? []), space === -1 ? '' : line.slice(space + 1)]);
  }
  for (const setting of settings) {
    const replaced = setting.replaces === undefined ? null : new RegExp(setting.replaces);
    const owned = (current.get(setting.key) ?? []).filter((value) => replaced?.test(value) ?? true);
    if (owned.length === 1 && owned[0] === setting.value) continue;
    const pattern = setting.replaces === undefined ? [] : [setting.replaces];
    await run(
      'git',
      ['config', '--file', configFile, '--replace-all', setting.key, setting.value, ...pattern],
      outside,
    );
  }
}
