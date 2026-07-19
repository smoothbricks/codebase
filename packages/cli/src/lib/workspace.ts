import { readdirSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { isSmoothBricksCodebasePackageName } from './cli-package.js';
import {
  isPackageJson,
  type PackageJson,
  type PackageNxConfig,
  type PackageRepository,
  readJson,
  readJsonObject,
} from './json.js';

export type { PackageJson, PackageNxConfig, PackageRepository };

export interface PackageInfo {
  name: string;
  projectName: string;
  version: string;
  private: boolean;
  tags: string[];
  path: string;
  packageJsonPath: string;
  json: PackageJson;
}

export interface WorkspacePackageManifest {
  name: string;
  projectName: string;
  private: boolean;
  path: string;
  packageJsonPath: string;
  json: PackageJson;
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

export type WorkspaceDependencyField = (typeof workspaceDependencyFields)[number];

export function listPublicPackages(root: string): PackageInfo[] {
  return getWorkspacePackages(root).filter((pkg) => !pkg.private && pkg.tags.includes('npm:public'));
}

export function listReleasePackages(
  root: string,
  rootPackage = readPackageJson(join(root, 'package.json')),
): PackageInfo[] {
  const rootRepository = rootPackage ? repositoryInfo(rootPackage.json) : null;
  if (!rootRepository) {
    return [];
  }
  return listPublicPackages(root).filter((pkg) => isOwnedPackage(rootRepository, pkg));
}

export function rootRepositoryInfo(root: string): RepositoryInfo | null {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  return rootPackage ? repositoryInfo(rootPackage.json) : null;
}

export function rootPackageName(root: string): string | null {
  const rootPackage = readPackageJsonObject(join(root, 'package.json'));
  return rootPackage?.name ?? null;
}

export function isSmoothBricksCodebase(root: string): boolean {
  return isSmoothBricksCodebasePackageName(rootPackageName(root) ?? undefined);
}

export function packageRepositoryInfo(pkg: PackageInfo): RepositoryInfo | null {
  return repositoryInfo(pkg.json);
}

export function isOwnedPackage(rootRepository: RepositoryInfo, pkg: PackageInfo): boolean {
  const packageRepository = packageRepositoryInfo(pkg);
  return packageRepository !== null && packageRepository.url === rootRepository.url;
}

export function getWorkspacePackages(root: string): PackageInfo[] {
  return getWorkspacePackagesForPatterns(root, getWorkspacePatterns(root));
}

export function getWorkspacePackageManifests(root: string): WorkspacePackageManifest[] {
  return getWorkspacePackageManifestsForPatterns(root, getWorkspacePatterns(root));
}

function getWorkspacePackagesForPatterns(root: string, workspacePatterns: string[]): PackageInfo[] {
  const packages: PackageInfo[] = [];
  for (const path of listWorkspacePackageJsonPaths(root, workspacePatterns)) {
    const pkg = readPackageJson(path);
    if (!pkg) {
      continue;
    }
    packages.push({
      ...pkg,
      path: relative(root, dirname(path)),
    });
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name));
}

function getWorkspacePackageManifestsForPatterns(
  root: string,
  workspacePatterns: string[],
): WorkspacePackageManifest[] {
  const packages: WorkspacePackageManifest[] = [];
  for (const path of listWorkspacePackageJsonPaths(root, workspacePatterns)) {
    const pkg = readWorkspacePackageManifest(path);
    if (!pkg) {
      continue;
    }
    packages.push({
      ...pkg,
      path: relative(root, dirname(path)),
    });
  }
  return packages.sort((a, b) => a.name.localeCompare(b.name));
}

function listWorkspacePackageJsonPaths(root: string, workspacePatterns: string[]): string[] {
  const paths: string[] = [];
  for (const pattern of workspacePatterns) {
    if (!pattern.endsWith('/*')) {
      continue;
    }
    const parent = join(root, pattern.slice(0, -2));
    let entries: string[];
    try {
      entries = readdirSync(parent);
    } catch {
      continue;
    }
    for (const entry of entries) {
      const packageJsonPath = join(parent, entry, 'package.json');
      try {
        if (statSync(packageJsonPath).isFile()) {
          paths.push(packageJsonPath);
        }
      } catch {
        // skip missing package.json
      }
    }
  }
  return paths;
}

export function listPackageJsonRecords(root: string): PackageInfo[] {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  if (!rootPackage) {
    return getWorkspacePackages(root);
  }
  return [
    { ...rootPackage, path: '.' },
    ...getWorkspacePackagesForPatterns(root, getWorkspacePatternsFromPackageJson(rootPackage.json)),
  ];
}

export function getWorkspacePatterns(root: string): string[] {
  const raw = readJson(join(root, 'package.json'));
  const pkg = isPackageJson(raw) ? raw : null;
  return pkg ? getWorkspacePatternsFromPackageJson(pkg) : ['packages/*'];
}

function getWorkspacePatternsFromPackageJson(pkg: PackageJson): string[] {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    return workspaces.filter((entry): entry is string => typeof entry === 'string');
  }
  if (workspaces && typeof workspaces === 'object' && Array.isArray(workspaces.packages)) {
    return workspaces.packages.filter((entry): entry is string => typeof entry === 'string');
  }
  return ['packages/*'];
}

export function readPackageJson(path: string): PackageInfo | null {
  const parsed = readPackageJsonObject(path);
  if (!parsed?.name || !parsed.version) {
    return null;
  }
  return {
    name: parsed.name,
    projectName: packageNxProjectName(parsed),
    version: parsed.version,
    private: parsed.private === true,
    tags: getNxTags(parsed),
    path: dirname(path),
    packageJsonPath: path,
    json: parsed,
  };
}

export function readWorkspacePackageManifest(path: string): WorkspacePackageManifest | null {
  const parsed = readPackageJsonObject(path);
  if (!parsed?.name) {
    return null;
  }
  return {
    name: parsed.name,
    projectName: packageNxProjectName(parsed),
    private: parsed.private === true,
    path: dirname(path),
    packageJsonPath: path,
    json: parsed,
  };
}

function packageNxProjectName(pkg: PackageJson): string {
  return pkg.nx?.name ?? pkg.name ?? '';
}

export function readPackageJsonObject(path: string): PackageJson | null {
  return readJsonObject(path);
}

export function getNxTags(pkg: PackageJson): string[] {
  const tags = pkg.nx?.tags;
  if (!Array.isArray(tags)) {
    return [];
  }
  return tags.filter((tag): tag is string => typeof tag === 'string');
}

export function repositoryInfo(pkg: PackageJson): RepositoryInfo | null {
  const repository = pkg.repository;
  if (typeof repository === 'string') {
    return { type: 'git', url: repository };
  }
  if (!repository || typeof repository !== 'object') {
    return null;
  }
  const url = repository.url;
  if (!url) {
    return null;
  }
  return { type: repository.type ?? 'git', url };
}

export function sameRepositoryAfterNormalization(left: string, right: string): boolean {
  return normalizedRepositoryUrl(left) === normalizedRepositoryUrl(right);
}

function normalizedRepositoryUrl(url: string): string {
  const trimmed = url.trim().replace(/\.git$/i, '');
  const ssh = /^git@github\.com:(.+)$/i.exec(trimmed);
  if (ssh?.[1]) {
    const [owner, repo] = ssh[1].split('/');
    if (owner && repo) {
      return githubRepositoryKey(owner, repo);
    }
  }
  try {
    const parsed = new URL(trimmed.includes('://') ? trimmed : `https://${trimmed}`);
    if (parsed.hostname.replace(/^www\./i, '').toLowerCase() === 'github.com') {
      const [owner, repo] = parsed.pathname.replace(/^\//, '').split('/');
      if (owner && repo) {
        return githubRepositoryKey(owner, repo);
      }
    }
  } catch {
    // fall through
  }
  return trimmed.toLowerCase();
}

function githubRepositoryKey(owner: string, repo: string): string {
  return `github:${owner.toLowerCase()}/${repo.toLowerCase()}`;
}

export function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
