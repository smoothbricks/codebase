import { execSync } from 'node:child_process';
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
  const workspacePackages = getWorkspacePackages(root);
  const workspaceVersions = new Map(workspacePackages.map((pkg) => [pkg.name, pkg.version]));
  const projectNameByPackage = new Map(workspacePackages.map((pkg) => [pkg.name, pkg.projectName]));
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
      // When the workspace dep is at a prerelease version that was never
      // published, the lockfile sync rewrites it to the latest stable tag
      // version.  Accept that version as valid.
      const projectName = projectNameByPackage.get(name);
      const expectedVersion =
        workspaceVersion.includes('-') && projectName
          ? (latestStableTagVersion(root, projectName) ?? workspaceVersion)
          : workspaceVersion;
      if (packedRange !== expectedVersion) {
        failures.push(
          `${sourcePackage.path}: packed ${field}.${name} must be ${expectedVersion}, got ${formatRange(packedRange)}`,
        );
      }
    }
  }
  return failures;
}

function formatRange(value: unknown): string {
  return typeof value === 'string' ? value : '<missing>';
}

function latestStableTagVersion(root: string, projectName: string): string | null {
  try {
    const output = execSync(`git tag --list '${projectName}@*' --sort=-v:refname`, {
      cwd: root,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    const prefix = `${projectName}@`;
    for (const line of output.split('\n')) {
      const tag = line.trim();
      if (!tag.startsWith(prefix)) continue;
      const version = tag.slice(prefix.length);
      if (version && !version.includes('-')) {
        return version;
      }
    }
    return null;
  } catch {
    return null;
  }
}
