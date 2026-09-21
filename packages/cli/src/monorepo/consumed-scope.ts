import {
  getWorkspacePackages,
  listPackageJsonRecords,
  readPackageJson,
  workspaceDependencyFields,
} from '../lib/workspace.js';
import { npmPackageExists, npmVersionExists } from '../release/index.js';

/**
 * Every dependency on the configured private scope that is not a workspace
 * package must exist in the registry at the version it names. A pin of a
 * version nobody published (only ever `bun link`ed) installs green on one
 * machine and fails on the first clean install. Exact versions are looked up
 * exactly; ranges only need
 * the package to exist, bun's lockfile pins the rest.
 */
export async function validateConsumedScopedDependenciesExist(root: string): Promise<number> {
  const scope = readPackageJson(`${root}/package.json`)?.json.smoo?.privateNpm?.scope;
  if (!scope) return 0;
  const workspaceNames = new Set(getWorkspacePackages(root).map((pkg) => pkg.name));
  const seen = new Map<string, Promise<boolean>>();
  let failures = 0;
  for (const pkg of listPackageJsonRecords(root)) {
    for (const field of workspaceDependencyFields) {
      for (const [name, range] of Object.entries(pkg.json[field] ?? {})) {
        if (!name.startsWith(`${scope}/`) || workspaceNames.has(name) || range.startsWith('workspace:')) continue;
        const exact = /^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(range);
        const key = exact ? `${name}@${range}` : name;
        let probe = seen.get(key);
        if (!probe) {
          probe = exact ? npmVersionExists(root, name, range) : npmPackageExists(root, name);
          seen.set(key, probe);
        }
        if (!(await probe)) {
          console.error(
            `${pkg.path}: ${field}.${name} is "${range}", which the registry does not hold; publish it (or pin a published version).`,
          );
          failures++;
        }
      }
    }
  }
  return failures;
}

const machineLocalSpecifier = /^(link|file|portal):/;

/**
 * A link:/file:/portal: specifier resolves on one machine and nowhere else:
 * CI and every deploy install nothing where it points. bun applies
 * `overrides`/`resolutions` to the whole install, so an entry there replaces a
 * registry package for every workspace. A `link:` override to an unpublished
 * package stays green locally and fails the first clean install.
 */
export function validateNoMachineLocalSpecifiers(root: string): number {
  let failures = 0;
  for (const pkg of listPackageJsonRecords(root)) {
    for (const field of [...workspaceDependencyFields, 'overrides', 'resolutions'] as const) {
      for (const [name, range] of Object.entries(pkg.json[field] ?? {})) {
        if (typeof range !== 'string' || !machineLocalSpecifier.test(range)) continue;
        console.error(
          `${pkg.path}: ${field}.${name} is "${range}", a machine-local specifier; CI and deploys resolve nothing there. Depend on a published version (or workspace:* for a workspace package).`,
        );
        failures++;
      }
    }
  }
  return failures;
}
