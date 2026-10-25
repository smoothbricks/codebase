import { join, posix } from 'node:path';
import type { CargoClosureSource } from '../cargo-closure-input.js';
import { CARGO_ANCESTOR_INPUTS, HASH_SKIPPED_DIRECTORIES } from '../cargo-source-hash.js';

/** Resolved metadata with the per-graph source scans already cached, as on later project visits. */
export function filesetSource(
  root: string,
  directories: readonly string[],
  files: readonly string[],
): CargoClosureSource {
  const local = directories.map((directory, index) => ({
    id: String(index),
    name: `package-${index}`,
    version: '1.0.0',
    directory: join(root, directory),
    manifest: join(root, directory, 'Cargo.toml'),
    sources: [join(root, directory, 'src/lib.rs'), ...files.map((file) => join(root, file))],
    governingManifest: join(root, 'Cargo.toml'),
  }));
  return {
    manifest: 'Cargo.toml',
    workspaceRoot: root,
    root,
    cargo: {
      root,
      members: new Set(local.map(({ id }) => id)),
      local,
      locked: new Map(),
      edges: new Map(local.map(({ id }) => [id, []])),
    },
    indexed: new Set(directories.map((directory) => posix.join(directory, 'Cargo.toml'))),
    compiledFilesCache: new Map(local.map(({ directory }) => [directory, []])),
  };
}

export interface ExpansionCopies {
  directoryEntries: number;
  siblingNames: number;
}

/** The pre-cutover fileset reduction, retained only as a differential oracle. */
export function expandedFilesets(
  packageDirectories: readonly string[],
  files: readonly string[],
  copies?: ExpansionCopies,
): string[] {
  const parent = (path: string): string | null => {
    if (path === '') return null;
    const directory = posix.dirname(path);
    return directory === '.' ? '' : directory;
  };
  const contains = (directory: string, path: string) =>
    directory === '' || path === directory || path.startsWith(`${directory}/`);
  const directories = [...new Set(packageDirectories)].sort().filter((directory, index, sorted) => {
    if (copies !== undefined) copies.directoryEntries += index;
    return !sorted.slice(0, index).some((outer) => contains(outer, directory));
  });
  const ancestors = new Set<string>();
  for (const directory of directories) {
    for (let ancestor = parent(directory); ancestor !== null; ancestor = parent(ancestor)) {
      const candidate = ancestor;
      if (!directories.some((outer) => contains(outer, candidate))) ancestors.add(candidate);
    }
  }
  const loose = new Set(
    files.filter(
      (file) =>
        !directories.some((directory) => contains(directory, file)) &&
        !(posix.basename(file) === 'Cargo.toml' && ancestors.has(parent(file) ?? '')),
    ),
  );
  const siblings = new Map<string, string[]>();
  for (const directory of directories) {
    const key = parent(directory);
    if (key === null) continue;
    if (copies !== undefined) copies.siblingNames += siblings.get(key)?.length ?? 0;
    siblings.set(key, [...(siblings.get(key) ?? []), posix.basename(directory)]);
  }
  const packageGlobs = directories.includes('')
    ? ['']
    : [...siblings].map(([under, names]) =>
        posix.join(under, names.length === 1 ? names.join('') : `{${names.join(',')}}`),
      );
  const anchored = (path: string, pattern = '') => posix.join('{workspaceRoot}', path, pattern);
  return [
    ...new Set([
      ...packageGlobs.map((glob) => anchored(glob, '**/{*.rs,Cargo.toml,.cargo/config,.cargo/config.toml}')),
      ...packageGlobs.map((glob) => `!${anchored(glob, `**/{${HASH_SKIPPED_DIRECTORIES.join(',')}}/**`)}`),
      ...[...ancestors].sort().map((ancestor) => anchored(ancestor, `{${CARGO_ANCESTOR_INPUTS.join(',')}}`)),
      ...[...loose].sort().map((file) => anchored(file)),
    ]),
  ];
}
