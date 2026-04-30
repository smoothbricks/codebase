// Promote a private package to npm publication. Invoked by `smoo g make-public <name>`.
// CLI wiring: packages/cli/src/generate/index.ts (variant registry)
import {
  getProjects,
  joinPathFragments,
  normalizePath,
  readJson,
  type Tree,
  updateJson,
} from 'nx/src/devkit-exports.js';

import type { MakePublicGeneratorSchema } from './schema.js';

export default async function generator(tree: Tree, schema: MakePublicGeneratorSchema): Promise<void> {
  const resolved = resolveProject(tree, schema.project);
  const packageJsonPath = joinPathFragments(resolved.root, 'package.json');

  if (!tree.exists(packageJsonPath)) {
    throw new Error(`Project ${resolved.name} is missing ${packageJsonPath}`);
  }

  const rootPkg = readJson<{ name?: string; license?: string; repository?: Record<string, unknown> }>(
    tree,
    'package.json',
  );

  updateJson(tree, packageJsonPath, (pkg: Record<string, unknown>) => {
    delete pkg.private;

    if (!pkg.license && rootPkg.license) {
      pkg.license = rootPkg.license;
    }

    pkg.publishConfig = { access: 'public' };

    if (rootPkg.repository) {
      pkg.repository = {
        ...(typeof rootPkg.repository === 'object' ? rootPkg.repository : {}),
        directory: resolved.root,
      };
    }

    const nx = (pkg.nx ?? {}) as Record<string, unknown>;
    const tags = Array.isArray(nx.tags) ? [...nx.tags] : [];
    if (!tags.includes('npm:public')) {
      tags.push('npm:public');
    }
    nx.tags = tags;
    pkg.nx = nx;

    if (pkg.version === '0.0.0') {
      pkg.version = '0.1.0';
    }

    return pkg;
  });
}

function resolveProject(tree: Tree, projectInput: string): { name: string; root: string } {
  const normalizedInput = normalizeLookupValue(projectInput);

  for (const [name, config] of getProjects(tree)) {
    if (normalizeLookupValue(name) === normalizedInput || normalizeLookupValue(config.root) === normalizedInput) {
      return { name, root: config.root };
    }

    const packageJsonPath = joinPathFragments(config.root, 'package.json');
    if (!tree.exists(packageJsonPath)) {
      continue;
    }

    const packageJson = readJson<{ name?: string }>(tree, packageJsonPath);
    if (packageJson.name && normalizeLookupValue(packageJson.name) === normalizedInput) {
      return { name, root: config.root };
    }
  }

  throw new Error(`Could not resolve project ${projectInput}`);
}

function normalizeLookupValue(value: string): string {
  return normalizePath(value.replace(/^\.\//, ''));
}
