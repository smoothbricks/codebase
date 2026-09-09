import type { PackageJson } from '../lib/json.js';
import type { ReleasePackageInfo } from './core.js';
import { globToRegExp, releaseTag } from './core.js';

export interface AutoReleaseCandidateShell {
  gitRefExists(ref: string): Promise<boolean>;
  latestStableReleaseRef(projectName: string): Promise<string | null>;
  packageChangedFilesSince(ref: string, packagePath: string): Promise<string[]>;
  packageJsonAtRef(ref: string, packagePath: string): Promise<PackageJson | null>;
  currentPackageJson(packagePath: string): Promise<PackageJson | null>;
  packageBuildInputPatterns(projectName: string, packagePath: string): Promise<string[]>;
  packageHasHistory(packagePath: string): Promise<boolean>;
}

const releasableManifestKeys = [
  'author',
  'bin',
  'browser',
  'bugs',
  'cpu',
  'dependencies',
  'description',
  'engines',
  'exports',
  'files',
  'funding',
  'homepage',
  'imports',
  'keywords',
  'license',
  'main',
  'module',
  'name',
  'optionalDependencies',
  'os',
  'peerDependencies',
  'peerDependenciesMeta',
  'publishConfig',
  'repository',
  'sideEffects',
  'types',
  'typesVersions',
] as const;

export async function autoReleaseCandidatePackages<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  packages: Package[],
): Promise<Package[]> {
  const candidates: Package[] = [];
  for (const pkg of packages) {
    if (await isAutoReleaseCandidate(shell, pkg)) {
      candidates.push(pkg);
    }
  }
  return candidates;
}

export type ReleaseProjectSelection = { mode: 'changed' } | { mode: 'named'; projects: string[] } | { mode: 'all' };

/**
 * Parse the release project selector. Blank selects the package-local changed
 * set (the auto filter), whatever the bump mode is. `all` is the deliberate
 * whole-fleet opt-in. Anything else is a comma-separated Nx project list that
 * releases exactly those projects without change detection.
 */
export function parseReleaseProjectSelection(
  raw: string | undefined,
  knownProjects: string[],
): ReleaseProjectSelection {
  const value = (raw ?? '').trim();
  if (value === '') {
    return { mode: 'changed' };
  }
  if (value === 'all') {
    return { mode: 'all' };
  }
  const projects = value
    .split(',')
    .map((project) => project.trim())
    .filter(Boolean);
  const unknown = projects.filter((project) => !knownProjects.includes(project));
  if (unknown.length > 0) {
    throw new Error(
      `Unknown release project(s): ${unknown.join(', ')}. Owned release projects: ${knownProjects.join(', ')}.`,
    );
  }
  return { mode: 'named', projects };
}

/**
 * The bump specifier never widens the release set: an explicit patch/minor/
 * major/prerelease only forces the version step for whatever this selection
 * picks. Named projects and `all` bypass change detection on purpose.
 */
export async function releaseCandidatePackages<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  packages: Package[],
  selection: ReleaseProjectSelection,
): Promise<Package[]> {
  if (selection.mode === 'all') {
    return packages;
  }
  if (selection.mode === 'named') {
    return packages.filter((pkg) => selection.projects.includes(pkg.projectName));
  }
  return autoReleaseCandidatePackages(shell, packages);
}

async function isAutoReleaseCandidate<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  pkg: Package,
): Promise<boolean> {
  const tagRef = `refs/tags/${releaseTag(pkg)}`;
  if (await shell.gitRefExists(tagRef)) {
    return packageHasReleasableChangesSince(shell, tagRef, pkg);
  }
  if (pkg.version.includes('-')) {
    // Prerelease version (e.g. 1.0.1-next.0) without its own tag: check for
    // releasable changes since the last stable release. The prerelease bump is
    // added after every release to make it obvious during development that the
    // working version is unreleased — it should not suppress the next release.
    const stableRef = await shell.latestStableReleaseRef(pkg.projectName);
    if (stableRef) {
      return packageHasReleasableChangesSince(shell, stableRef, pkg);
    }
    // No prior stable release — first release must be intentional, not auto.
    return false;
  }
  return shell.packageHasHistory(pkg.path);
}

async function packageHasReleasableChangesSince<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  ref: string,
  pkg: Package,
): Promise<boolean> {
  const changedFiles = await shell.packageChangedFilesSince(ref, pkg.path);
  if (changedFiles.length === 0) {
    return false;
  }
  const currentManifest = await shell.currentPackageJson(pkg.path);
  const buildInputPatterns = await shell.packageBuildInputPatterns(pkg.projectName, pkg.path);
  for (const changedFile of changedFiles) {
    if (changedFile === 'package.json') {
      const previousManifest = await shell.packageJsonAtRef(ref, pkg.path);
      if (!previousManifest || !currentManifest || releasableManifestChanged(previousManifest, currentManifest)) {
        return true;
      }
      continue;
    }
    if (isReleasablePackagePath(changedFile, currentManifest, buildInputPatterns)) {
      return true;
    }
  }
  return false;
}

function releasableManifestChanged(previousManifest: PackageJson, currentManifest: PackageJson): boolean {
  for (const key of releasableManifestKeys) {
    if (!stableJsonEqual(Reflect.get(previousManifest, key), Reflect.get(currentManifest, key))) {
      return true;
    }
  }
  return false;
}

function isReleasablePackagePath(path: string, manifest: PackageJson | null, buildInputPatterns: string[]): boolean {
  return (
    isBuildInputPath(path, buildInputPatterns) || isPackageMetadataPath(path) || isManifestFilesPath(path, manifest)
  );
}

function isBuildInputPath(path: string, patterns: string[]): boolean {
  if (isReleaseIgnoredBuildInputPath(path)) {
    return false;
  }
  let matched = false;
  for (const pattern of patterns) {
    const excluded = pattern.startsWith('!');
    const rawPattern = excluded ? pattern.slice(1) : pattern;
    if (matchesBuildInputPattern(path, rawPattern)) {
      matched = !excluded;
    }
  }
  return matched;
}

function matchesBuildInputPattern(path: string, pattern: string): boolean {
  const normalized = pattern.replace(/^\.\//, '').replace(/\/$/, '');
  if (!normalized) {
    return false;
  }
  return globToRegExp(normalized).test(path);
}

function isReleaseIgnoredBuildInputPath(path: string): boolean {
  return (
    path === 'package.json' ||
    path === 'tsconfig.test.json' ||
    path.includes('/__tests__/') ||
    path.endsWith('.test.ts') ||
    path.endsWith('.test.tsx') ||
    path.endsWith('.spec.ts') ||
    path.endsWith('.spec.tsx')
  );
}

function isPackageMetadataPath(path: string): boolean {
  return /^(README|LICENSE|CHANGELOG)(\.|$)/.test(path);
}

function isManifestFilesPath(path: string, manifest: PackageJson | null): boolean {
  const files = manifest?.files;
  if (!Array.isArray(files)) {
    return false;
  }
  for (const entry of files) {
    if (entry.length === 0 || entry.startsWith('!')) {
      continue;
    }
    if (matchesManifestFilesEntry(path, entry)) {
      return true;
    }
  }
  return false;
}

function matchesManifestFilesEntry(path: string, entry: string): boolean {
  const normalized = entry.replace(/^\.\//, '').replace(/\/$/, '');
  if (!normalized || normalized.includes('*')) {
    return false;
  }
  return path === normalized || path.startsWith(`${normalized}/`);
}

function stableJsonEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(stableJson(left)) === JSON.stringify(stableJson(right));
}

function stableJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(stableJson);
  }
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    const normalized: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      normalized[key] = stableJson(Reflect.get(value, key));
    }
    return normalized;
  }
  return value;
}
