import {
  getProjects,
  joinPathFragments,
  normalizePath,
  readJson,
  type Tree,
  updateJson,
} from 'nx/src/devkit-exports.js';

import { applyBoundedTestTargetPolicy, type BoundedTestPolicyPackageJson } from '../../bounded-test-policy.js';

interface BoundedTestTargetsGeneratorSchema {
  project: string;
}

export default async function generator(tree: Tree, schema: BoundedTestTargetsGeneratorSchema): Promise<void> {
  const resolved = resolveProject(tree, schema.project);
  const packageJsonPath = joinPathFragments(resolved.root, 'package.json');

  if (!tree.exists(packageJsonPath)) {
    throw new Error(`Project ${resolved.name} is missing ${packageJsonPath}`);
  }

  updateJson<BoundedTestPolicyPackageJson>(tree, packageJsonPath, (packageJson) => {
    applyBoundedTestTargetPolicy(packageJson, { projectName: resolved.name });
    return packageJson;
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
