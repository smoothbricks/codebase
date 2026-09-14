import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readlinkSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
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

export async function applyWorkspaceGitConfig(root: string): Promise<void> {
  const gitDirResult = await $`git rev-parse --git-dir`.cwd(root).quiet().nothrow();
  if (gitDirResult.exitCode !== 0) {
    throw new Error(`git rev-parse --git-dir failed with exit code ${gitDirResult.exitCode}: not in a git repository`);
  }

  const gitDir = resolve(root, decode(gitDirResult.stdout).trim());
  const tooling = join(root, 'tooling');

  await run('git', ['config', '--local', 'include.path', join(tooling, 'workspace.gitconfig')], root);

  // Keep the newer runtime version pins on any merge (nvfetcher overlay +
  // devenv.lock) so a mirror sync's `git am --3way` never stalls on a version
  // conflict. Mapped by the managed .gitattributes (merge=smoo-newer-pins);
  // implemented in tooling/direnv/merge-newer-pins.sh. Runtime package.json
  // pin repair is explicit via `smoo monorepo init --runtime-only`.
  await run(
    'git',
    ['config', '--local', 'merge.smoo-newer-pins.name', 'keep the newer devenv/nvfetcher runtime pins'],
    root,
  );
  await run(
    'git',
    ['config', '--local', 'merge.smoo-newer-pins.driver', 'bash tooling/direnv/merge-newer-pins.sh %O %A %B %P'],
    root,
  );
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
  if (readLinkOrNull(target) === source) {
    return;
  }

  mkdirSync(dirname(target), { recursive: true });
  rmSync(target, { force: true });
  symlinkSync(source, target);
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
    // A hook that is not executable is a hook git silently skips.
    chmodSync(target, 0o755);
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

function readLinkOrNull(path: string): string | null {
  try {
    return readlinkSync(path);
  } catch {
    return null;
  }
}

export const postCommitHookFileForTest = postCommitHookFile;
