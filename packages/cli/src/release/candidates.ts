import { isRecord } from '../lib/json.js';
import type { ReleasePackageInfo } from './core.js';
import { releaseTag } from './core.js';

export interface AutoReleaseCandidateShell {
  gitRefExists(ref: string): Promise<boolean>;
  packageChangedFilesSince(ref: string, packagePath: string): Promise<string[]>;
  packageJsonAtRef(ref: string, packagePath: string): Promise<Record<string, unknown> | null>;
  currentPackageJson(packagePath: string): Promise<Record<string, unknown> | null>;
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

async function isAutoReleaseCandidate<Package extends ReleasePackageInfo>(
  shell: AutoReleaseCandidateShell,
  pkg: Package,
): Promise<boolean> {
  const tagRef = `refs/tags/${releaseTag(pkg)}`;
  if (await shell.gitRefExists(tagRef)) {
    return packageHasReleasableChangesSince(shell, tagRef, pkg);
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
  for (const changedFile of changedFiles) {
    if (changedFile === 'package.json') {
      const previousManifest = await shell.packageJsonAtRef(ref, pkg.path);
      if (!previousManifest || !currentManifest || releasableManifestChanged(previousManifest, currentManifest)) {
        return true;
      }
      continue;
    }
    if (isReleasablePackagePath(changedFile, currentManifest)) {
      return true;
    }
  }
  return false;
}

function releasableManifestChanged(
  previousManifest: Record<string, unknown>,
  currentManifest: Record<string, unknown>,
): boolean {
  for (const key of releasableManifestKeys) {
    if (!stableJsonEqual(previousManifest[key], currentManifest[key])) {
      return true;
    }
  }
  return false;
}

function isReleasablePackagePath(path: string, manifest: Record<string, unknown> | null): boolean {
  return (
    path.startsWith('src/') ||
    path.startsWith('bin/') ||
    path.startsWith('dist/') ||
    path.startsWith('managed/') ||
    isPackageMetadataPath(path) ||
    isManifestFilesPath(path, manifest)
  );
}

function isPackageMetadataPath(path: string): boolean {
  return /^(README|LICENSE|CHANGELOG)(\.|$)/.test(path);
}

function isManifestFilesPath(path: string, manifest: Record<string, unknown> | null): boolean {
  const files = manifest?.files;
  if (!Array.isArray(files)) {
    return false;
  }
  for (const entry of files) {
    if (typeof entry !== 'string' || entry.length === 0 || entry.startsWith('!')) {
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
  if (isRecord(value)) {
    const normalized: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      normalized[key] = stableJson(value[key]);
    }
    return normalized;
  }
  return value;
}
