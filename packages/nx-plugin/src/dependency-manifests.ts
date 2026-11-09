import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';

/** One installed direct dependency's manifest: its package name, where it is, and its parsed JSON. */
export interface DependencyManifest {
  readonly name: string;
  readonly path: string;
  readonly json: unknown;
}

/**
 * The manifests of a project's direct dependencies, the ones ttsc discovers plugins on (`dependencies` and
 * `devDependencies`), once per name. Each is the manifest Node resolution reaches from the project, as ttsc reads it;
 * a dependency that is not installed has none. Read once per project, for every declaration inference reads from them.
 */
export function directDependencyManifests(
  packageJsonPath: string,
  dependencies: readonly string[],
): DependencyManifest[] {
  const lookup = createRequire(packageJsonPath);
  const manifests: DependencyManifest[] = [];
  for (const name of new Set(dependencies)) {
    const path = (lookup.resolve.paths(name) ?? [])
      .map((root) => join(root, ...name.split('/'), 'package.json'))
      .find((candidate) => existsSync(candidate));
    if (path !== undefined) manifests.push({ name, path, json: JSON.parse(readFileSync(path, 'utf-8')) });
  }
  return manifests;
}
