import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { type PackageJson, parsePackageJsonText } from '../lib/json.js';
import { getWorkspacePackages, workspaceDependencyFields } from '../lib/workspace.js';
import { prunePublishedExports } from './published-exports.js';

export { prunePublishedExports } from './published-exports.js';

/**
 * Run `fn` (a `bun pm pack`) with the package's on-disk manifest rewritten to
 * its published shape, restoring the original bytes afterwards.
 * A manifest transform (rather than a bun feature) because `bun pm pack`
 * offers no publish-time manifest hook.
 *
 * When `publishConfigRegistry` is set (private user-run release path), the
 * resolved literal registry is written into the packed publishConfig and the
 * workspace bytes are restored afterwards. Only a resolved literal is ever
 * written here, never an environment placeholder.
 *
 * When `workspaceRoot` is set (the release packer), each `workspace:*`
 * dependency on a package of that workspace is pinned at the version
 * {@link publishPackDependencyVersion} decides, so the tarball embeds smoo's
 * version rather than Bun's: `bun pm pack` 1.4.4 resolves `workspace:*` from
 * the dependency's own manifest, which holds an unpublished `-next` version
 * after a release, where 1.4.3 read the version from bun.lock.
 */
export async function withPublishManifest<T>(
  packageDir: string,
  fn: () => Promise<T>,
  options: { log?: boolean; publishConfigRegistry?: string; workspaceRoot?: string } = {},
): Promise<T> {
  const manifestPath = join(packageDir, 'package.json');
  const originalText = readFileSync(manifestPath, 'utf8');
  const parsed = parsePackageJsonText(originalText);
  if (!parsed) {
    // A manifest smoo cannot parse: pack anyway and let bun report it.
    return fn();
  }
  const { manifest: exported, adjustments } = prunePublishedExports(parsed);
  const { manifest, pins }: { manifest: PackageJson; pins: readonly string[] } =
    options.workspaceRoot === undefined
      ? { manifest: exported, pins: [] }
      : pinWorkspaceDependencies(options.workspaceRoot, exported);
  if (options.publishConfigRegistry) {
    manifest.publishConfig = { ...(manifest.publishConfig ?? {}), registry: options.publishConfigRegistry };
  }
  const registryChanged = options.publishConfigRegistry !== undefined;
  if (adjustments.length === 0 && pins.length === 0 && !registryChanged) {
    return fn();
  }
  if (options.log) {
    if (adjustments.length > 0) {
      console.log(`${manifest.name}: adjusting published exports for pack: ${adjustments.join(', ')}`);
    }
    if (pins.length > 0) {
      console.log(`${manifest.name}: pinning workspace dependencies for pack: ${pins.join(', ')}`);
    }
    if (registryChanged) {
      console.log(`${manifest.name}: setting publishConfig.registry for pack`);
    }
  }
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  try {
    return await fn();
  } finally {
    writeFileSync(manifestPath, originalText);
  }
}

/**
 * `manifest` with each `workspace:*` dependency on a package of the workspace at `root` pinned at the version
 * {@link publishPackDependencyVersion} decides, and one `<field>.<name> <version>` entry per pin. Any other range,
 * `workspace:^` included, stays as written for the packed-manifest check to judge.
 */
function pinWorkspaceDependencies(root: string, manifest: PackageJson): { manifest: PackageJson; pins: string[] } {
  const packages = getWorkspacePackages(root);
  const pinned: PackageJson = { ...manifest };
  const pins: string[] = [];
  for (const field of workspaceDependencyFields) {
    const dependencies = manifest[field];
    if (dependencies === undefined) continue;
    const exact = { ...dependencies };
    for (const [name, range] of Object.entries(dependencies)) {
      const dep = range === 'workspace:*' ? packages.find((pkg) => pkg.name === name) : undefined;
      if (dep === undefined) continue;
      const version = publishPackDependencyVersion(root, dep.projectName, dep.version);
      exact[name] = version;
      pins.push(`${field}.${name} ${version}`);
    }
    pinned[field] = exact;
  }
  return { manifest: pinned, pins };
}

/**
 * The version a published manifest pins for a workspace dependency on `projectName` at `packageVersion`: that
 * version, except that a prerelease (the `-next` version a package stays on after its stable release, never tagged or
 * published) pins the last stable `<projectName>@<version>` tag. With no such tag it stays the prerelease, which the
 * packed-manifest check refuses outside devDependencies.
 */
export function publishPackDependencyVersion(root: string, projectName: string, packageVersion: string): string {
  if (!packageVersion.includes('-')) return packageVersion;
  return latestStableTagVersion(root, projectName) ?? packageVersion;
}

function latestStableTagVersion(root: string, projectName: string): string | null {
  const args = ['tag', '--list', `${projectName}@*`, '--sort=-v:refname'];
  const command = `git ${args.join(' ')}`;
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git tag listing: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
  if (result.error) {
    throw new Error(`${command} in ${root} failed to start: ${result.error.message}`, { cause: result.error });
  }
  if (result.status !== 0) {
    throw new Error(`${command} in ${root} exited ${result.status ?? result.signal}: ${result.stderr.trim()}`);
  }
  const prefix = `${projectName}@`;
  for (const line of result.stdout.split('\n')) {
    const tag = line.trim();
    if (!tag.startsWith(prefix)) continue;
    const version = tag.slice(prefix.length);
    if (version && !version.includes('-')) {
      return version;
    }
  }
  return null;
}
