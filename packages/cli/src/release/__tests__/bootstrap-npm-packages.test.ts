import { describe, expect, it } from 'bun:test';
import {
  type BootstrapNpmPackagesShell,
  bootstrapNpmPackages,
  NPM_BOOTSTRAP_DIST_TAG,
  NPM_BOOTSTRAP_VERSION,
  NPM_BOOTSTRAP_VISIBILITY_TIMEOUT_MS,
} from '../bootstrap-npm-packages.js';
import type { ReleasePackageInfo } from '../core.js';

const stable: ReleasePackageInfo = {
  name: '@scope/stable',
  projectName: 'stable',
  path: 'packages/stable',
  version: '1.2.3',
};
const missing: ReleasePackageInfo = {
  name: '@scope/missing',
  projectName: 'missing',
  path: 'packages/missing',
  version: '2.0.0',
};

describe('bootstrap npm packages', () => {
  it('publishes placeholders only for selected packages missing from npm', async () => {
    const shell = new RecordingBootstrapShell({
      packages: [stable, missing],
      existing: [stable.name],
      otps: ['123456'],
    });

    const bootstrapped = await bootstrapNpmPackages(shell, {
      dryRun: false,
      skipLogin: false,
      packages: [missing.name],
    });

    expect(bootstrapped.map((pkg) => pkg.name)).toEqual([missing.name]);
    expect(shell.logins).toBe(1);
    expect(shell.published).toEqual([{ name: missing.name, otp: '123456' }]);
    expect(shell.logs.join('\n')).toContain(NPM_BOOTSTRAP_VERSION);
    expect(shell.logs.join('\n')).toContain(NPM_BOOTSTRAP_DIST_TAG);
  });

  it('dry-run reports missing packages without login or publish', async () => {
    const shell = new RecordingBootstrapShell({ packages: [stable, missing], existing: [stable.name] });

    const bootstrapped = await bootstrapNpmPackages(shell, { dryRun: true, skipLogin: false, packages: [] });

    expect(bootstrapped.map((pkg) => pkg.name)).toEqual([missing.name]);
    expect(shell.logins).toBe(0);
    expect(shell.published).toEqual([]);
    expect(shell.logs).toContain(`${stable.name}: already exists on npm; skipping placeholder bootstrap.`);
  });

  it('supports skipping npm login when an existing session is already authenticated', async () => {
    const shell = new RecordingBootstrapShell({ packages: [missing], existing: [], otps: ['654321'] });

    await bootstrapNpmPackages(shell, { dryRun: false, skipLogin: true, packages: [] });

    expect(shell.logins).toBe(0);
    expect(shell.published).toEqual([{ name: missing.name, otp: '654321' }]);
  });

  it('passes explicit OTP to placeholder publishes without prompting', async () => {
    const shell = new RecordingBootstrapShell({ packages: [missing], existing: [] });

    await bootstrapNpmPackages(shell, { dryRun: false, skipLogin: true, packages: [], otp: '111222' });

    expect(shell.published).toEqual([{ name: missing.name, otp: '111222' }]);
    expect(shell.prompts).toEqual([]);
  });

  it('uploads every placeholder before polling all pending packages with backoff', async () => {
    const shell = new RecordingBootstrapShell({
      packages: [stable, missing],
      existing: [],
      visibleAfterChecks: { [stable.name]: 2, [missing.name]: 3 },
    });

    await bootstrapNpmPackages(shell, { dryRun: false, skipLogin: true, packages: [], otp: '111222' });

    expect(shell.events).toEqual([
      `publish:${stable.name}`,
      `publish:${missing.name}`,
      `view:${stable.name}:${NPM_BOOTSTRAP_VERSION}`,
      `view:${missing.name}:${NPM_BOOTSTRAP_VERSION}`,
      'wait:1000',
      `view:${stable.name}:${NPM_BOOTSTRAP_VERSION}`,
      `view:${missing.name}:${NPM_BOOTSTRAP_VERSION}`,
      'wait:2000',
      `view:${missing.name}:${NPM_BOOTSTRAP_VERSION}`,
    ]);
  });

  it('times out after bounded backoff without suggesting another bootstrap', async () => {
    const shell = new RecordingBootstrapShell({
      packages: [missing],
      existing: [],
      visibleAfterChecks: { [missing.name]: Number.POSITIVE_INFINITY },
    });

    await expect(
      bootstrapNpmPackages(shell, { dryRun: false, skipLogin: true, packages: [], otp: '111222' }),
    ).rejects.toThrow('Retry smoo release trust-publisher later; do not bootstrap again.');

    const waited = shell.events
      .filter((event) => event.startsWith('wait:'))
      .reduce((total, event) => total + Number(event.slice('wait:'.length)), 0);
    expect(waited).toBe(NPM_BOOTSTRAP_VISIBILITY_TIMEOUT_MS);
  });

  it('rejects unknown package selections before npm login', async () => {
    const shell = new RecordingBootstrapShell({ packages: [stable], existing: [] });

    await expect(
      bootstrapNpmPackages(shell, { dryRun: false, skipLogin: false, packages: ['@scope/unknown'] }),
    ).rejects.toThrow('Unknown owned release package selection: @scope/unknown');
    expect(shell.logins).toBe(0);
    expect(shell.published).toEqual([]);
  });
});

class RecordingBootstrapShell implements BootstrapNpmPackagesShell<ReleasePackageInfo> {
  readonly events: string[] = [];
  readonly logs: string[] = [];
  readonly published: Array<{ name: string; otp: string }> = [];
  readonly prompts: string[] = [];
  logins = 0;
  private readonly packages: ReleasePackageInfo[];
  private readonly existing: Set<string>;
  private readonly otps: string[];
  private readonly visibilityChecks = new Map<string, number>();
  private readonly visibleAfterChecks: Readonly<Record<string, number>>;

  constructor(options: {
    packages: ReleasePackageInfo[];
    existing: string[];
    otps?: string[];
    visibleAfterChecks?: Record<string, number>;
  }) {
    this.packages = options.packages;
    this.existing = new Set(options.existing);
    this.otps = [...(options.otps ?? [])];
    this.visibleAfterChecks = options.visibleAfterChecks ?? {};
  }

  listReleasePackages(): ReleasePackageInfo[] {
    return this.packages;
  }

  async packageExists(name: string): Promise<boolean> {
    return this.existing.has(name);
  }

  async packageVersionExists(name: string, version: string): Promise<boolean> {
    this.events.push(`view:${name}:${version}`);
    const checks = (this.visibilityChecks.get(name) ?? 0) + 1;
    this.visibilityChecks.set(name, checks);
    return checks >= (this.visibleAfterChecks[name] ?? 1);
  }

  async login(): Promise<void> {
    this.logins += 1;
  }

  async publishPlaceholder(pkg: ReleasePackageInfo, env?: Record<string, string>): Promise<void> {
    this.events.push(`publish:${pkg.name}`);
    this.published.push({ name: pkg.name, otp: env?.NPM_CONFIG_OTP ?? '' });
  }

  async promptOtp(packageName: string): Promise<string> {
    this.prompts.push(packageName);
    const otp = this.otps.shift();
    if (!otp) {
      throw new Error(`unexpected OTP prompt for ${packageName}`);
    }
    return otp;
  }

  async wait(milliseconds: number): Promise<void> {
    this.events.push(`wait:${milliseconds}`);
  }

  log(message: string): void {
    this.logs.push(message);
  }
}
