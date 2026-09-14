import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { printCommandOutput } from '../lib/run.js';

const hookScript = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  'managed/raw/tooling/git-hooks/post-commit.sh',
);

// Stand-in for git-format-staged, performing the same two writes: it rewrites
// the blobs in whichever index git handed the hook, and writes the formatted
// content to the working tree file. Formatting here is "strip trailing
// whitespace", so a reformat is visible in the committed bytes.
const FORMATTER = `#!/usr/bin/env bash
set -e -o pipefail
against=$(git rev-parse --verify -q HEAD || git hash-object -t tree /dev/null)
git diff-index --cached --name-only -z "$against" | while IFS= read -r -d '' path; do
  entry=$(git ls-files -s -- "$path")
  [ -n "$entry" ] || continue
  mode=$(echo "$entry" | awk '{print $1}')
  orig=$(echo "$entry" | awk '{print $2}')
  new=$(git cat-file -p "$orig" | sed 's/[[:space:]]*$//' | git hash-object -w --stdin)
  [ "$new" = "$orig" ] && continue
  git update-index --cacheinfo "$mode,$new,$path"
  git cat-file -p "$new" >"$path"
done
`;

// Shaped the way `smoo monorepo init` installs it: another tool's chainable
// block already owns the file, and ours calls the managed script.
const DISPATCHER = `#!/bin/sh
# >>> other-tool post-commit >>>
echo "other-tool ran" >&2
# <<< other-tool post-commit <<<
# >>> smoo post-commit >>>
"$(git rev-parse --show-toplevel)/tooling/git-hooks/post-commit.sh"
# <<< smoo post-commit <<<
`;

function git(cwd: string, ...args: string[]): string {
  const result = spawnSync('git', args, { cwd, encoding: 'utf8' });
  if (result.status !== 0) {
    printCommandOutput(result.stdout ?? '', result.stderr ?? '');
    throw new Error(`git ${args.join(' ')} failed with status ${result.status}`);
  }

  return result.stdout ?? '';
}

/** HEAD, index and working tree blob for one path, so divergence is nameable. */
function blobs(repo: string, file: string): { head: string; index: string; worktree: string } {
  return {
    head: git(repo, 'rev-parse', `HEAD:${file}`).trim(),
    index: git(repo, 'ls-files', '-s', '--', file).trim().split(/\s+/)[1] ?? '',
    worktree: git(repo, 'hash-object', '--', file).trim(),
  };
}

function withRepository(run: (repo: string) => void): void {
  const repo = mkdtempSync(join(tmpdir(), 'smoo-post-commit-'));
  try {
    git(repo, 'init', '-q', '--initial-branch=main');
    git(repo, 'config', 'user.email', 'hooks@example.test');
    git(repo, 'config', 'user.name', 'Hook Test');
    mkdirSync(join(repo, 'tooling', 'git-hooks'), { recursive: true });
    copyFileSync(hookScript, join(repo, 'tooling', 'git-hooks', 'post-commit.sh'));
    writeFileSync(join(repo, '.git', 'hooks', 'pre-commit'), FORMATTER, { mode: 0o755 });
    writeFileSync(join(repo, '.git', 'hooks', 'post-commit'), DISPATCHER, { mode: 0o755 });
    run(repo);
  } finally {
    rmSync(repo, { recursive: true, force: true });
  }
}

describe('post-commit hook', () => {
  it('leaves the index equal to the partial commit the formatter rewrote', () => {
    withRepository((repo) => {
      writeFileSync(join(repo, 'mine.txt'), 'base\n');
      git(repo, 'add', 'mine.txt');
      git(repo, 'commit', '-qm', 'base', '--no-verify');

      writeFileSync(join(repo, 'mine.txt'), 'mine needs format   \n');
      git(repo, 'add', 'mine.txt');
      git(repo, 'commit', '-m', 'partial', '--only', '--', 'mine.txt');

      // Formatted bytes reached the commit, and nothing stayed behind claiming
      // otherwise: the pre-format blob used to sit in the index here.
      expect(git(repo, 'cat-file', '-p', 'HEAD:mine.txt')).toBe('mine needs format\n');
      const { head, index, worktree } = blobs(repo, 'mine.txt');
      expect(index).toBe(head);
      expect(worktree).toBe(head);
      expect(git(repo, 'diff', '--cached', '--name-only', 'HEAD')).toBe('');
    });
  });

  it('repairs the first commit in a repository, which has no parent to diff against', () => {
    withRepository((repo) => {
      writeFileSync(join(repo, 'first.txt'), 'first needs format   \n');
      git(repo, 'add', 'first.txt');
      git(repo, 'commit', '-m', 'initial', '--only', '--', 'first.txt');

      expect(git(repo, 'cat-file', '-p', 'HEAD:first.txt')).toBe('first needs format\n');
      const { head, index } = blobs(repo, 'first.txt');
      expect(index).toBe(head);
    });
  });

  it('leaves staged work on paths outside the commit alone', () => {
    withRepository((repo) => {
      writeFileSync(join(repo, 'mine.txt'), 'base\n');
      writeFileSync(join(repo, 'peer.txt'), 'base\n');
      writeFileSync(join(repo, 'dropped.txt'), 'base\n');
      git(repo, 'add', '.');
      git(repo, 'commit', '-qm', 'base', '--no-verify');

      writeFileSync(join(repo, 'mine.txt'), 'mine needs format   \n');
      writeFileSync(join(repo, 'peer.txt'), 'peer work\n');
      git(repo, 'add', 'mine.txt', 'peer.txt');
      // A deliberate unstage is staged state too, and this commit did not
      // write dropped.txt, so the repair must not put it back.
      git(repo, 'rm', '--cached', '-q', 'dropped.txt');

      git(repo, 'commit', '-m', 'partial', '--only', '--', 'mine.txt');

      const peer = blobs(repo, 'peer.txt');
      expect(peer.index).toBe(peer.worktree);
      expect(peer.index).not.toBe(peer.head);
      expect(git(repo, 'ls-files', '--', 'dropped.txt')).toBe('');
    });
  });

  it('keeps a plain index-built commit consistent', () => {
    withRepository((repo) => {
      writeFileSync(join(repo, 'plain.txt'), 'base\n');
      git(repo, 'add', 'plain.txt');
      git(repo, 'commit', '-qm', 'base', '--no-verify');

      writeFileSync(join(repo, 'plain.txt'), 'plain needs format   \n');
      git(repo, 'add', 'plain.txt');
      git(repo, 'commit', '-m', 'plain');

      expect(git(repo, 'cat-file', '-p', 'HEAD:plain.txt')).toBe('plain needs format\n');
      const { head, index, worktree } = blobs(repo, 'plain.txt');
      expect(index).toBe(head);
      expect(worktree).toBe(head);
    });
  });
});
