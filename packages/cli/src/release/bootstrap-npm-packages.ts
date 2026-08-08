import type { ReleasePackageInfo } from './core.js';

export const NPM_BOOTSTRAP_VERSION = '0.0.0-bootstrap.0';
export const NPM_BOOTSTRAP_DIST_TAG = 'bootstrap';
export const NPM_BOOTSTRAP_VISIBILITY_TIMEOUT_MS = 120_000;

const NPM_BOOTSTRAP_POLL_DELAYS_MS = [0, 1_000, 2_000, 4_000, 8_000, 15_000, 30_000, 30_000, 30_000] as const;

export interface BootstrapNpmPackagesOptions {
  dryRun: boolean;
  skipLogin: boolean;
  packages: string[];
  otp?: string;
}

export interface BootstrapNpmPackagesShell<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  listReleasePackages(): Package[];
  packageExists(name: string): Promise<boolean>;
  packageVersionExists(name: string, version: string): Promise<boolean>;
  login(): Promise<void>;
  publishPlaceholder(pkg: Package, env?: Record<string, string>): Promise<void>;
  promptOtp(packageName: string): Promise<string>;
  wait(milliseconds: number): Promise<void>;
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
  // Upload every placeholder before waiting for npm registry propagation. Keeping the
  // phases separate prevents one slow package from blocking the remaining uploads.
  for (const pkg of missing) {
    shell.log(`${pkg.name}: publishing npm placeholder.`);
    const otp = options.otp ?? (await shell.promptOtp(pkg.name));
    await shell.publishPlaceholder(pkg, { NPM_CONFIG_OTP: otp });
  }

  await waitForPublishedPackages(shell, missing);
  shell.log('Bootstrap complete. Run smoo release trust-publisher before the first CI publish.');
  return missing;
}

async function waitForPublishedPackages<Package extends ReleasePackageInfo>(
  shell: BootstrapNpmPackagesShell<Package>,
  packages: readonly Package[],
): Promise<void> {
  let pending = [...packages];
  for (const delayMs of NPM_BOOTSTRAP_POLL_DELAYS_MS) {
    if (delayMs > 0) {
      await shell.wait(delayMs);
    }
    const visible = await Promise.all(
      pending.map((pkg) => shell.packageVersionExists(pkg.name, NPM_BOOTSTRAP_VERSION)),
    );
    pending = pending.filter((_, index) => !visible[index]);
    if (pending.length === 0) {
      return;
    }
  }

  throw new Error(
    `Bootstrap uploads succeeded, but npm did not expose these packages after ${NPM_BOOTSTRAP_VISIBILITY_TIMEOUT_MS / 1_000} seconds: ${pending
      .map((pkg) => pkg.name)
      .join(', ')}. Retry smoo release trust-publisher later; do not bootstrap again.`,
  );
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
