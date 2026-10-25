import { posix } from 'node:path';

import type { ScriptcNativeFfi } from './schema.js';

/** Whether `path` names something beneath the directory it is relative to: not empty, not absolute, never climbing out. */
function staysInside(path: string): boolean {
  const normalized = posix.normalize(path.replaceAll('\\', '/'));
  return normalized !== '.' && normalized !== '..' && !normalized.startsWith('../') && !posix.isAbsolute(normalized);
}

/**
 * What is wrong with where a package's FFI points, or undefined. The manifest, the declarations and the C sources are
 * read from the project, and a declaration is written beneath the run's staging directory. Nx hashes only what lies
 * beneath the project and the run removes only its staging, so a path that is absolute or climbs out of its root names a
 * file the cache key does not know, or one the run leaves behind.
 */
export function ffiPathProblem(ffi: ScriptcNativeFfi): string | undefined {
  const named = [
    ...[ffi.manifest, ...Object.keys(ffi.declarations), ...ffi.sources].map((path) => ({
      path,
      beneath: 'the project',
    })),
    ...Object.values(ffi.declarations).map((path) => ({ path, beneath: "the emitted JavaScript's root" })),
  ];
  const escaping = named.find(({ path }) => !staysInside(path));
  return escaping === undefined
    ? undefined
    : `names ${JSON.stringify(escaping.path)}, which is not a path beneath ${escaping.beneath}`;
}
