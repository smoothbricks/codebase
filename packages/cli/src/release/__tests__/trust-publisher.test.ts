import { describe, expect, it } from 'bun:test';
import type { BootstrapNpmPackagesOptions } from '../bootstrap-npm-packages.js';
import type { ReleasePackageInfo } from '../core.js';
import { configureTrustedPublishers, type TrustPublisherShell } from '../index.js';

const stable: ReleasePackageInfo = { name: '@scope/stable', path: 'packages/stable', version: '1.2.3' };
const missing: ReleasePackageInfo = { name: '@scope/missing', path: 'packages/missing', version: '2.0.0' };

describe('trusted publisher setup', () => {
  it('bootstraps missing npm packages before configuring trust', async () => {
    const shell = new RecordingTrustPublisherShell({ packages: [stable, missing], existing: [stable.name] });

    await configureTrustedPublishers(shell, { bootstrap: true, skipLogin: false, otp: '123456' });

    expect(shell.events).toEqual([
      'bootstrap:false:false',
      `exists:${stable.name}`,
      `exists:${missing.name}`,
      `trust:${stable.name}:false:123456`,
      `trust:${missing.name}:false:123456`,
    ]);
    expect(shell.logins).toBe(0);
  });

  it('directs missing packages to trust-publisher --bootstrap', async () => {
    const shell = new RecordingTrustPublisherShell({ packages: [missing], existing: [] });

    await expect(configureTrustedPublishers(shell, { otp: '123456' })).rejects.toThrow(
      'Run smoo release trust-publisher --bootstrap locally',
    );
    expect(shell.events).toEqual([`exists:${missing.name}`]);
  });

  it('still logs in for trust when bootstrap finds no missing packages', async () => {
    const shell = new RecordingTrustPublisherShell({ packages: [stable], existing: [stable.name] });

    await configureTrustedPublishers(shell, { bootstrap: true, otp: '123456' });

    expect(shell.events).toEqual([
      'bootstrap:false:false',
      `exists:${stable.name}`,
      `trust:${stable.name}:false:123456`,
    ]);
    expect(shell.logins).toBe(1);
  });
});

class RecordingTrustPublisherShell implements TrustPublisherShell<ReleasePackageInfo> {
  readonly repository = 'scope/repo';
  readonly workflow = 'publish.yml';
  readonly events: string[] = [];
  readonly logs: string[] = [];
  readonly errors: string[] = [];
  logins = 0;
  private readonly packages: ReleasePackageInfo[];
  private readonly existing: Set<string>;

  constructor(options: { packages: ReleasePackageInfo[]; existing: string[] }) {
    this.packages = options.packages;
    this.existing = new Set(options.existing);
  }

  listReleasePackages(): ReleasePackageInfo[] {
    return this.packages;
  }

  async packageExists(name: string): Promise<boolean> {
    this.events.push(`exists:${name}`);
    return this.existing.has(name);
  }

  async bootstrapNpmPackages(options: BootstrapNpmPackagesOptions): Promise<ReleasePackageInfo[]> {
    this.events.push(`bootstrap:${options.dryRun}:${options.skipLogin}`);
    const missingPackages = this.packages.filter((pkg) => !this.existing.has(pkg.name));
    for (const pkg of missingPackages) {
      this.existing.add(pkg.name);
    }
    return missingPackages;
  }

  async login(): Promise<void> {
    this.logins += 1;
  }

  async trustPublisher(pkg: ReleasePackageInfo, dryRun: boolean, env?: Record<string, string>): Promise<void> {
    this.events.push(`trust:${pkg.name}:${dryRun}:${env?.NPM_CONFIG_OTP ?? ''}`);
  }

  async promptOtp(packageName: string): Promise<string> {
    throw new Error(`unexpected OTP prompt for ${packageName}`);
  }

  log(message: string): void {
    this.logs.push(message);
  }

  error(message: string): void {
    this.errors.push(message);
  }
}
