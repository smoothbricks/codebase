import type { PackageJson } from './workspace-manifest.js';

export interface RepositoryInfo {
  type: string;
  url: string;
}
export const smoothBricksCodebasePackageName = '@smoothbricks/codebase';
export function isSmoothBricksCodebasePackageName(name: string | undefined): boolean {
  return name === smoothBricksCodebasePackageName;
}

export function isPublishablePackage(pkg: { private: boolean; tags: string[] }): boolean {
  if (pkg.private) {
    return false;
  }
  return pkg.tags.includes('npm:public') !== pkg.tags.includes('npm:private');
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
