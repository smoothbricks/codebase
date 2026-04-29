import type { ReleasePackageInfo } from './core.js';

export const NPM_BOOTSTRAP_VERSION = '0.0.0-bootstrap.0';
export const NPM_BOOTSTRAP_DIST_TAG = 'bootstrap';

export interface BootstrapNpmPackagesOptions {
  dryRun: boolean;
  skipLogin: boolean;
  packages: string[];
}

export interface BootstrapNpmPackagesShell<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  listReleasePackages(): Package[];
  packageExists(name: string): Promise<boolean>;
  login(): Promise<void>;
  publishPlaceholder(pkg: Package): Promise<void>;
  log(message: string): void;
}

export async function bootstrapNpmPackages<Package extends ReleasePackageInfo>(
  shell: BootstrapNpmPackagesShell<Package>,
  options: BootstrapNpmPackagesOptions,
): Promise<Package[]> {
  const packages = selectedReleasePackages(shell.listReleasePackages(), options.packages);
  if (packages.length === 0) {
    throw new Error('No owned release packages found.');
  }

  const missing: Package[] = [];
  for (const pkg of packages) {
    if (await shell.packageExists(pkg.name)) {
      shell.log(`${pkg.name}: already exists on npm; skipping placeholder bootstrap.`);
    } else {
      missing.push(pkg);
    }
  }

  if (missing.length === 0) {
    shell.log('All selected owned release packages already exist on npm.');
    return [];
  }

  shell.log(
    `Bootstrap npm placeholders (${NPM_BOOTSTRAP_VERSION}, dist-tag ${NPM_BOOTSTRAP_DIST_TAG}): ${missing
      .map((pkg) => pkg.name)
      .join(', ')}`,
  );
  if (options.dryRun) {
    return missing;
  }

  if (!options.skipLogin) {
    await shell.login();
  }
  for (const pkg of missing) {
    shell.log(`${pkg.name}: publishing npm placeholder.`);
    await shell.publishPlaceholder(pkg);
  }
  shell.log('Bootstrap complete. Run smoo release trust-publisher before the first CI publish.');
  return missing;
}

function selectedReleasePackages<Package extends ReleasePackageInfo>(
  packages: Package[],
  selections: string[],
): Package[] {
  if (selections.length === 0) {
    return packages;
  }
  const byName = new Map(packages.map((pkg) => [pkg.name, pkg]));
  const selected: Package[] = [];
  const unknown: string[] = [];
  for (const name of selections) {
    const pkg = byName.get(name);
    if (pkg) {
      selected.push(pkg);
    } else {
      unknown.push(name);
    }
  }
  if (unknown.length > 0) {
    throw new Error(`Unknown owned release package selection: ${unknown.join(', ')}`);
  }
  return selected;
}
