import { readdirSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { hasOwn, hasOwnString, isRecord, readJson, readJsonObject, stringProperty } from './json.js';

export interface PackageInfo {
  name: string;
  version: string;
  private: boolean;
  tags: string[];
  path: string;
  packageJsonPath: string;
  json: Record<string, unknown>;
}

export interface RepositoryInfo {
  type: string;
  url: string;
}

export const workspaceDependencyFields = [
  'dependencies',
  'devDependencies',
  'peerDependencies',
  'optionalDependencies',
] as const;

export function listPublicPackages(root: string): PackageInfo[] {
  return getWorkspacePackages(root).filter((pkg) => !pkg.private && pkg.tags.includes('npm:public'));
}

export function listReleasePackages(
  root: string,
  rootPackage = readPackageJson(join(root, 'package.json')),
): PackageInfo[] {
  if (!rootPackage) {
    return [];
  }
  const rootRepository = repositoryInfo(rootPackage.json);
  if (!rootRepository) {
    return [];
  }
  return getWorkspacePackagesForPatterns(root, getWorkspacePatternsFromPackageJson(rootPackage.json)).filter(
    (pkg) => !pkg.private && pkg.tags.includes('npm:public') && isOwnedPackage(rootRepository, pkg),
  );
}

export function rootRepositoryInfo(root: string): RepositoryInfo | null {
  const rootPackage = readJsonObject(join(root, 'package.json'));
  return rootPackage ? repositoryInfo(rootPackage) : null;
}

export function packageRepositoryInfo(pkg: PackageInfo): RepositoryInfo | null {
  return repositoryInfo(pkg.json);
}

export function isOwnedPackage(rootRepository: RepositoryInfo, pkg: PackageInfo): boolean {
  const packageRepository = packageRepositoryInfo(pkg);
  return packageRepository !== null && packageRepository.url === rootRepository.url;
}

export function getWorkspacePackages(root: string): PackageInfo[] {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  if (!rootPackage) {
    throw new Error('package.json not found or invalid');
  }
  return getWorkspacePackagesForPatterns(root, getWorkspacePatternsFromPackageJson(rootPackage.json));
}

function getWorkspacePackagesForPatterns(root: string, workspacePatterns: string[]): PackageInfo[] {
  const packages: PackageInfo[] = [];
  for (const pattern of workspacePatterns) {
    if (!pattern.endsWith('/*')) {
      continue;
    }
    const parent = join(root, pattern.slice(0, -2));
    if (!statSync(parent, { throwIfNoEntry: false })?.isDirectory()) {
      continue;
    }
    for (const entry of readdirSync(parent)) {
      const pkgPath = join(parent, entry, 'package.json');
      const pkg = readPackageJson(pkgPath);
      if (!pkg?.name || !pkg.version) {
        continue;
      }
      packages.push({
        name: pkg.name,
        version: pkg.version,
        private: pkg.private,
        tags: pkg.tags,
        path: relative(root, dirname(pkgPath)),
        packageJsonPath: pkg.packageJsonPath,
        json: pkg.json,
      });
    }
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name));
}

export function listPackageJsonRecords(root: string): PackageInfo[] {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  if (!rootPackage) {
    throw new Error('package.json not found or invalid');
  }
  return [
    { ...rootPackage, path: '.' },
    ...getWorkspacePackagesForPatterns(root, getWorkspacePatternsFromPackageJson(rootPackage.json)),
  ];
}

export function getWorkspacePatterns(root: string): string[] {
  const raw = readJson(join(root, 'package.json'));
  return isRecord(raw) ? getWorkspacePatternsFromPackageJson(raw) : ['packages/*'];
}

function getWorkspacePatternsFromPackageJson(pkg: Record<string, unknown>): string[] {
  if (!hasOwn(pkg, 'workspaces')) {
    return ['packages/*'];
  }
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    return workspaces.filter((entry): entry is string => typeof entry === 'string');
  }
  if (isRecord(workspaces) && hasOwn(workspaces, 'packages') && Array.isArray(workspaces.packages)) {
    return workspaces.packages.filter((entry): entry is string => typeof entry === 'string');
  }
  return ['packages/*'];
}

export function readPackageJson(path: string): PackageInfo | null {
  const parsed = readJsonObject(path);
  if (!isRecord(parsed) || !hasOwnString(parsed, 'name') || !hasOwnString(parsed, 'version')) {
    return null;
  }
  const privateValue = hasOwn(parsed, 'private') && typeof parsed.private === 'boolean' ? parsed.private : false;
  const tags = getNxTags(parsed);
  return {
    name: parsed.name,
    version: parsed.version,
    private: privateValue,
    tags,
    path: dirname(path),
    packageJsonPath: path,
    json: parsed,
  };
}

export function getNxTags(pkg: Record<string, unknown>): string[] {
  if (!hasOwn(pkg, 'nx') || !isRecord(pkg.nx) || !hasOwn(pkg.nx, 'tags') || !Array.isArray(pkg.nx.tags)) {
    return [];
  }
  return pkg.nx.tags.filter((tag): tag is string => typeof tag === 'string');
}

export function repositoryInfo(pkg: Record<string, unknown>): RepositoryInfo | null {
  const repository = pkg.repository;
  if (typeof repository === 'string') {
    return { type: 'git', url: repository };
  }
  if (!isRecord(repository)) {
    return null;
  }
  const url = stringProperty(repository, 'url');
  if (!url) {
    return null;
  }
  return { type: stringProperty(repository, 'type') ?? 'git', url };
}

export function sameRepositoryAfterNormalization(left: string, right: string): boolean {
  return normalizedRepositoryUrl(left) === normalizedRepositoryUrl(right);
}

function normalizedRepositoryUrl(url: string): string {
  const trimmed = url
    .trim()
    .replace(/^git\+/, '')
    .replace(/#.*$/, '')
    .replace(/\/$/, '')
    .replace(/\.git$/, '');
  const https = /^https:\/\/github\.com\/([^/]+)\/([^/]+)$/i.exec(trimmed);
  if (https?.[1] && https[2]) {
    return githubRepositoryKey(https[1], https[2]);
  }
  const ssh = /^git@github\.com:([^/]+)\/([^/]+)$/i.exec(trimmed);
  if (ssh?.[1] && ssh[2]) {
    return githubRepositoryKey(ssh[1], ssh[2]);
  }
  const shorthand = /^github:([^/]+)\/([^/]+)$/i.exec(trimmed);
  if (shorthand?.[1] && shorthand[2]) {
    return githubRepositoryKey(shorthand[1], shorthand[2]);
  }
  return trimmed;
}

function githubRepositoryKey(owner: string, repo: string): string {
  return `github:${owner.toLowerCase()}/${repo.toLowerCase()}`;
}

export function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
