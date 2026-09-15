import { randomUUID } from 'node:crypto';
import {
  chmodSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { basename, dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import {
  type FileResult,
  isManagedTarget,
  type ManagedFileSpec,
  type ManagedSnapshot,
  planManagedFiles,
} from './managed-plan.js';

/** Capture links with lstat: existsSync incorrectly treats dangling links as absent files. */
export function readManagedSnapshot(root: string, target: string): ManagedSnapshot {
  if (!isManagedTarget(target)) return { kind: 'blocked', reason: 'invalid managed path' };
  const parts = target.split('/');
  for (let index = 1; index < parts.length; index += 1) {
    const parent = lstatSync(resolve(root, ...parts.slice(0, index)), { throwIfNoEntry: false });
    if (parent === undefined) return { kind: 'missing' };
    if (!parent.isDirectory()) {
      return { kind: 'blocked', reason: 'parent path is not a regular directory (symlink parents are not managed)' };
    }
  }
  const path = resolve(root, target);
  const info = lstatSync(path, { throwIfNoEntry: false });
  if (info === undefined) return { kind: 'missing' };
  if (info.isSymbolicLink()) {
    let destination: string;
    try {
      destination = realpathSync(path);
    } catch {
      return { kind: 'blocked', reason: 'symlink is broken, cyclic or inaccessible; refusing to follow it' };
    }
    const fromRoot = relative(root, destination);
    if (isAbsolute(fromRoot) || fromRoot === '..' || fromRoot.startsWith(`..${sep}`)) {
      return { kind: 'blocked', reason: 'symlink resolves outside the workspace' };
    }
    const linked = statSync(destination);
    if (!linked.isFile()) return { kind: 'blocked', reason: 'symlink does not resolve to a regular file' };
    return {
      kind: 'symlink',
      destination,
      content: readFileSync(destination, 'utf8'),
      executable: (linked.mode & 0o100) !== 0,
    };
  }
  if (!info.isFile()) return { kind: 'blocked', reason: 'target is not a regular file or symlink' };
  return { kind: 'file', content: readFileSync(path, 'utf8'), executable: (info.mode & 0o100) !== 0 };
}

/**
 * Renderers are outside this shell. All snapshots and conflicts are evaluated
 * before writes, and each replacement is atomic. This is not a multi-file
 * transaction: an I/O failure during application can leave a completed prefix.
 * Re-running reconciles that prefix; no installed dependency or secret changes
 * are performed here.
 */
export function syncManagedFiles(
  workspaceRoot: string,
  specs: readonly ManagedFileSpec[],
  mode: 'update' | 'check' | 'diff',
): FileResult[] {
  const root = realpathSync(workspaceRoot);
  const inputs = specs.map((spec) => ({
    ...spec,
    current: spec.desired === null ? { kind: 'missing' as const } : readManagedSnapshot(root, spec.target),
  }));
  const plan = planManagedFiles(inputs);
  if (mode !== 'update') {
    return plan.results.map((result) =>
      result.action === 'created' || result.action === 'updated' ? { ...result, action: 'drifted' } : result,
    );
  }
  if (plan.conflicts.length > 0) {
    throw new Error(
      `Managed-file update refused before writing:\n${plan.conflicts.map(({ target, reason }) => `${target}: ${reason}`).join('\n')}`,
    );
  }
  for (const write of plan.writes) {
    const path = resolve(root, write.target);
    mkdirSync(dirname(path), { recursive: true });
    const temporary = resolve(dirname(path), `.${basename(path)}.smoo-${randomUUID()}.tmp`);
    try {
      const permissions = write.executable ? 0o755 : 0o644;
      writeFileSync(temporary, write.content, { flag: 'wx', mode: permissions });
      // Creation modes are filtered by umask; the managed executable contract is not.
      chmodSync(temporary, permissions);
      renameSync(temporary, path);
    } finally {
      rmSync(temporary, { force: true });
    }
  }
  return plan.results.map((result) =>
    result.action === 'ok-symlink' ? { ...result, action: 'skipped-symlink' } : result,
  );
}
