import { $ } from 'bun';
import { isRecord, recordProperty } from '../lib/json.js';
import { decode } from '../lib/run.js';
import type { PackageInfo } from '../lib/workspace.js';
import { getWorkspacePackages, workspaceDependencyFields } from '../lib/workspace.js';

export async function readPackedPackageJson(
  root: string,
  tarball: string,
  packageName: string,
): Promise<Record<string, unknown>> {
  const result = await $`tar -xOf ${tarball} package/package.json`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error(`${packageName}: unable to inspect packed package.json.`);
  }
  const parsed = JSON.parse(decode(result.stdout));
  if (!isRecord(parsed)) {
    throw new Error(`${packageName}: packed package.json is not an object.`);
  }
  return parsed;
}

export function validatePackedWorkspaceDependencies(
  root: string,
  sourcePackage: PackageInfo,
  packedPackage: Record<string, unknown>,
): string[] {
  const workspaceVersions = new Map(getWorkspacePackages(root).map((pkg) => [pkg.name, pkg.version]));
  const failures: string[] = [];
  for (const field of workspaceDependencyFields) {
    const sourceDependencies = recordProperty(sourcePackage.json, field);
    const packedDependencies = recordProperty(packedPackage, field);
    if (!sourceDependencies && !packedDependencies) {
      continue;
    }

    for (const [name, range] of Object.entries(packedDependencies ?? {})) {
      if (typeof range === 'string' && range.startsWith('workspace:')) {
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
      if (packedRange !== workspaceVersion) {
        failures.push(
          `${sourcePackage.path}: packed ${field}.${name} must be ${workspaceVersion}, got ${formatRange(packedRange)}`,
        );
      }
    }
  }
  return failures;
}

function formatRange(value: unknown): string {
  return typeof value === 'string' ? value : '<missing>';
}
