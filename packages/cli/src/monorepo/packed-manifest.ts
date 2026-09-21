import { type PackageExports, type PackageJson, parsePackageJsonText } from '../lib/json.js';
import { printCommandOutput, runResult } from '../lib/run.js';
import type { PackageInfo } from '../lib/workspace.js';
import { getWorkspacePackages, workspaceDependencyFields } from '../lib/workspace.js';
import { releaseCandidateShell } from '../release/candidate-shell.js';
import { packagePinFreshness } from '../release/candidates.js';
import { publishPackDependencyVersion } from './lockfile.js';

export async function readPackedPackageJson(root: string, tarball: string, packageName: string): Promise<PackageJson> {
  const result = await runResult('tar', ['-xOf', tarball, 'package/package.json'], root);
  if (result.exitCode !== 0) {
    printCommandOutput(result.stdout, result.stderr);
    throw new Error(
      `${packageName}: tar -xOf ${tarball} package/package.json failed with exit code ${result.exitCode}`,
    );
  }
  const parsed = parsePackageJsonText(result.stdout);
  if (!parsed) {
    throw new Error(`${packageName}: packed package.json is invalid.`);
  }
  return parsed;
}

export function validatePackedWorkspaceDependencies(
  root: string,
  sourcePackage: PackageInfo,
  packedPackage: PackageJson,
  options: { mode?: 'install' | 'publish' } = {},
): string[] {
  const mode = options.mode ?? 'publish';
  const workspacePackages = getWorkspacePackages(root);
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
      const dep = workspacePackages.find((pkg) => pkg.name === name);
      if (!dep) {
        continue;
      }
      const packedRange = packedDependencies?.[name];
      if (sourceRange !== 'workspace:*') {
        failures.push(`${sourcePackage.path}: source ${field}.${name} must use workspace:*`);
      }
      // install: what day-to-day pack embeds (package.json / lockfile aligned)
      // publish: what pre-publish sync embeds (unpublished -next → last stable tag)
      const expectedVersion =
        mode === 'publish' ? publishPackDependencyVersion(root, dep.projectName, dep.version) : dep.version;
      if (packedRange !== expectedVersion) {
        failures.push(
          `${sourcePackage.path}: packed ${field}.${name} must be ${expectedVersion}, got ${packedRange ?? '<missing>'}`,
        );
      }
    }
  }
  return failures;
}

/**
 * A published manifest may not advertise a `development` condition: it points
 * at source the tarball never ships, and every bundler that resolves with that
 * condition (Vite, vitest) then fails to load the package. The condition exists
 * for in-repo source resolution and must be stripped from what ships.
 */
export function validatePackedExportConditions(sourcePackage: PackageInfo, packedPackage: PackageJson): string[] {
  const failures: string[] = [];
  walkExportConditions(packedPackage.exports, '.', (path, condition) => {
    if (condition === 'development') {
      failures.push(
        `${sourcePackage.path}: packed exports${path} advertises a "development" condition; it targets unshipped source. Remove it from the published manifest.`,
      );
    }
  });
  return failures;
}

function walkExportConditions(
  exports: PackageExports,
  path: string,
  visit: (path: string, condition: string) => void,
): void {
  if (exports === null || exports === undefined || typeof exports === 'string') return;
  for (const [key, value] of Object.entries(exports)) {
    if (!key.startsWith('.')) visit(path, key);
    walkExportConditions(
      value,
      key.startsWith('.') ? `[${JSON.stringify(key)}]` : `${path}[${JSON.stringify(key)}]`,
      visit,
    );
  }
}

/**
 * A packed manifest pins each workspace dependency at a version; that version
 * must be what the package was built against. A dependency outside the release
 * set is pinned at a published version, and that publish must be the source at
 * HEAD: when the dependency carries releasable changes since the pin's tag, or
 * the pin names a version no release ever tagged, the release ships a build
 * nobody tested (`@axe.sc/axe` 0.0.8 pinned an untagged `axe-client-ts` 0.1.0
 * whose stale publish still advertised a `development` export). The fix is to
 * release the dependency too, never to ship the pin. Prerelease pins are
 * workspace-internal (`-next`) and never what ships, so they are not judged.
 */
export async function validatePackedDependencyFreshness(
  root: string,
  sourcePackage: PackageInfo,
  packedPackage: PackageJson,
  releasing: readonly string[] = [],
): Promise<string[]> {
  const workspacePackages = getWorkspacePackages(root);
  const shell = await releaseCandidateShell(root);
  const failures: string[] = [];
  for (const field of workspaceDependencyFields) {
    for (const [name, pinned] of Object.entries(packedPackage[field] ?? {})) {
      const dep = workspacePackages.find((pkg) => pkg.name === name);
      if (!dep || pinned.includes('-') || releasing.includes(dep.projectName)) continue;
      const verdict = await packagePinFreshness(shell, dep, pinned);
      if (verdict === 'fresh') continue;
      failures.push(
        verdict === 'untagged'
          ? `${sourcePackage.path}: packed ${field}.${name} pins ${pinned}, which no release tagged (${dep.projectName}@${pinned}); release ${dep.projectName} in the same run.`
          : `${sourcePackage.path}: packed ${field}.${name} pins ${pinned}, but ${dep.path} has releasable changes since ${dep.projectName}@${pinned}; the package was built against those. Release ${dep.projectName} in the same run.`,
      );
    }
  }
  return failures;
}
