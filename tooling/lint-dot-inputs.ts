#!/usr/bin/env bun
// Fails when a tracked dot path would not reach Nx's file index. The root
// `.nxignore` must re-include every dot-named prefix of the path with a `!/<prefix>`
// line after its last `.*` line, and Nx's own git-ignore checker must not exclude it.
// Takes no arguments; runs from any directory inside the checkout.
import { execFileSync } from 'node:child_process';
import { FsTree } from 'nx/src/generators/tree.js';
import { createGitIgnoreChecker } from 'nx/src/utils/ignore.js';

// `--full-name` keeps every path workspace-relative whatever directory this runs
// from; `-z` keeps names containing spaces or newlines intact.
const root = execFileSync('git', ['rev-parse', '--show-toplevel'], { encoding: 'utf8' }).trimEnd();
const tracked = execFileSync('git', ['ls-files', '-z', '--full-name'], { encoding: 'utf8' })
  .split('\0')
  .filter((path) => path !== '');
const dotPaths = tracked.filter((path) => path.split('/').some((part) => part.startsWith('.')));

const tree = new FsTree(root, false);
const nxignore = (tree.read('.nxignore', 'utf-8') ?? '').split('\n').map((line) => line.trimEnd());
const lastDotAll = nxignore.lastIndexOf('.*');
const failures: string[] = [];

if (lastDotAll === -1) failures.push('.nxignore has no `.*` line, so untracked dot paths are not ignored');

// A negation counts only when it follows the last `.*`: an earlier `!/<prefix>` is
// overridden by it. Every dot-named component of a path needs its own line, because
// a dot-named directory excluded by `.*` hides its children unless re-included.
const unreincluded = new Map<string, string[]>();
for (const path of dotPaths) {
  const parts = path.split('/');
  parts.forEach((part, index) => {
    if (!part.startsWith('.')) return;
    const prefix = parts.slice(0, index + 1).join('/');
    if (nxignore.lastIndexOf(`!/${prefix}`) <= lastDotAll) {
      unreincluded.set(prefix, [...(unreincluded.get(prefix) ?? []), path]);
    }
  });
}
for (const [prefix, paths] of unreincluded) {
  failures.push(`missing "!/${prefix}" after ".*" in .nxignore (tracked: ${paths.join(', ')})`);
}

// Nx's own checker: `.gitignore` and `.nxignore` cascaded from disk, plus its always-ignored list.
const checker = createGitIgnoreChecker(tree);
for (const path of dotPaths) {
  if (checker.isIgnoredFile(path)) failures.push(`${path}: excluded from Nx's file index by .gitignore or .nxignore`);
}

if (failures.length > 0) {
  for (const failure of failures) console.error(`lint-dot-inputs: ${failure}`);
  console.error(`lint-dot-inputs: ${failures.length} problem(s) across ${dotPaths.length} tracked dot paths`);
  process.exitCode = 1;
} else {
  console.log(`lint-dot-inputs: ${dotPaths.length} tracked dot paths are re-included and indexed`);
}
