import { lstatSync, realpathSync, statSync } from 'node:fs';
import { isAbsolute, relative, resolve, sep } from 'node:path';
import { isManagedTarget, type ManagedPathInfo } from './tree.js';

/**
 * Small filesystem boundary for permissions and links, which Nx Tree cannot
 * read. Never reads content and never writes. In-memory generator tests supply
 * these facts directly; real-filesystem tests exercise this boundary separately.
 */
export function inspectManagedPaths(root: string, targets: readonly string[]): Map<string, ManagedPathInfo> {
  const canonicalRoot = realpathSync(root);
  return new Map(targets.map((target) => [target, inspectPath(canonicalRoot, target)]));
}

function inspectPath(root: string, target: string): ManagedPathInfo {
  if (!isManagedTarget(target)) return { error: 'invalid managed path' };
  const parts = target.split('/');
  for (let i = 1; i < parts.length; i++) {
    const parent = lstatSync(resolve(root, ...parts.slice(0, i)), { throwIfNoEntry: false });
    if (parent === undefined) return {};
    if (!parent.isDirectory())
      return { error: 'parent path is not a regular directory (symlink parents are not managed)' };
  }
  const path = resolve(root, target);
  const info = lstatSync(path, { throwIfNoEntry: false });
  if (info === undefined) return {};
  if (!info.isSymbolicLink()) {
    return info.isFile() ? { executable: (info.mode & 0o100) !== 0 } : { error: 'target is not a regular file' };
  }
  let destination: string;
  try {
    destination = realpathSync(path);
  } catch {
    return { error: 'symlink is broken, cyclic or inaccessible; refusing to follow it' };
  }
  const fromRoot = relative(root, destination);
  if (isAbsolute(fromRoot) || fromRoot === '..' || fromRoot.startsWith(`..${sep}`)) {
    return { error: 'symlink resolves outside the workspace' };
  }
  const linked = statSync(destination);
  return linked.isFile()
    ? { symlink: true, executable: (linked.mode & 0o100) !== 0 }
    : { error: 'symlink does not resolve to a regular file' };
}
