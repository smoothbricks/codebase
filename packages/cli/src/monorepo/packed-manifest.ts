import { $ } from 'bun';
import { type PackageJson, parsePackageJsonText } from '../lib/json.js';
import { decode } from '../lib/run.js';
import type { PackageInfo } from '../lib/workspace.js';
import { getWorkspacePackages, workspaceDependencyFields } from '../lib/workspace.js';

export async function readPackedPackageJson(root: string, tarball: string, packageName: string): Promise<PackageJson> {
  const result = await $`tar -xOf ${tarball} package/package.json`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`${packageName}: unable to inspect packed package.json.`);
  }
  const parsed = parsePackageJsonText(decode(result.stdout));
  if (!parsed) {
    throw new Error(`${packageName}: packed package.json is invalid.`);
  }
  return parsed;
}

export function validatePackedWorkspaceDependencies(
  root: string,
  sourcePackage: PackageInfo,
  packedPackage: PackageJson,
): string[] {
  const workspacePackages = getWorkspacePackages(root);
  const workspaceVersions = new Map(workspacePackages.map((pkg) => [pkg.name, pkg.version]));
  const failures: string[] = [];
  for (const field of workspaceDependencyFields) {
    const sourceDependencies = sourcePackage.json[field];
    const packedDependencies = packedPackage[field];
    if (!sourceDependencies && !packedDependencies) {
      continue;
    }

    for (const [name, range] of Object.entries(packedDependencies ?? {})) {
      if (range.startsWith('workspace:')) {
        failures.push(`${sourcePackage.path}: packed ${field}.${name} must not contain ${range}`);
      }
    }

    if (!sourceDependencies) {
      continue;
    }
    for (const [name, sourceRange] of Object.entries(sourceDependencies)) {
      const workspaceVersion = workspaceVersions.get(name);
      if (!workspaceVersion) {
        continue;
      }
      const packedRange = packedDependencies?.[name];
      if (sourceRange !== 'workspace:*') {
        failures.push(`${sourcePackage.path}: source ${field}.${name} must use workspace:*`);
      }
      // Packed workspace deps must match the current package.json version.
      const expectedVersion = workspaceVersion;
      if (packedRange !== expectedVersion) {
        failures.push(
          `${sourcePackage.path}: packed ${field}.${name} must be ${expectedVersion}, got ${packedRange ?? '<missing>'}`,
        );
      }
    }
  }
  return failures;
}
