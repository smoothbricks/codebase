import { describe, expect, it } from 'bun:test';
import type { BootstrapNpmPackagesOptions } from '../bootstrap-npm-packages.js';
import type { ReleasePackageInfo } from '../core.js';
import {
  configureTrustedPublishers,
  parseTrustedPublishers,
  type TrustedPublisher,
  type TrustPublisherShell,
} from '../index.js';

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

describe('trusted publisher setup', () => {
  it('bootstraps missing npm packages before configuring trust', async () => {
    const shell = new RecordingTrustPublisherShell({ packages: [stable, missing], existing: [stable.name] });

    await configureTrustedPublishers(shell, { bootstrap: true, skipLogin: false });

    expect(shell.events).toEqual([
      'bootstrap:false:false',
      `exists:${stable.name}`,
      `exists:${missing.name}`,
      `list:${stable.name}`,
      `trust:${stable.name}:false`,
      `list:${missing.name}`,
      `trust:${missing.name}:false`,
    ]);
  });

  it('directs missing packages to trust-publisher --bootstrap', async () => {
    const shell = new RecordingTrustPublisherShell({ packages: [missing], existing: [] });

    await expect(configureTrustedPublishers(shell, {})).rejects.toThrow(
      'Run smoo release trust-publisher --bootstrap locally',
    );
    expect(shell.events).toEqual([`exists:${missing.name}`]);
  });

  it('does not pre-login before trust operations', async () => {
    const shell = new RecordingTrustPublisherShell({ packages: [stable], existing: [stable.name] });

    await configureTrustedPublishers(shell, { bootstrap: true });

    expect(shell.events).toEqual([
      'bootstrap:false:false',
      `exists:${stable.name}`,
      `list:${stable.name}`,
      `trust:${stable.name}:false`,
    ]);
  });

  it('skips packages with matching trusted publishers', async () => {
    const shell = new RecordingTrustPublisherShell({
      packages: [stable],
      existing: [stable.name],
      trustedPublishers: {
        [stable.name]: [{ id: 'trusted-1', type: 'github', file: 'publish.yml', repository: 'scope/repo' }],
      },
    });

    await configureTrustedPublishers(shell, { skipLogin: true });

    expect(shell.events).toEqual([`exists:${stable.name}`, `list:${stable.name}`]);
    expect(shell.logs).toContain(`${stable.name}: npm trusted publisher is already configured; skipping.`);
  });

  it('rejects mismatched trusted publishers', async () => {
    const shell = new RecordingTrustPublisherShell({
      packages: [stable],
      existing: [stable.name],
      trustedPublishers: {
        [stable.name]: [{ id: 'trusted-1', type: 'github', file: 'other.yml', repository: 'scope/repo' }],
      },
    });

    await expect(configureTrustedPublishers(shell, { skipLogin: true })).rejects.toThrow(
      'npm trusted publisher exists but does not match',
    );
    expect(shell.events).toEqual([`exists:${stable.name}`, `list:${stable.name}`]);
  });

  it('configures only selected packages', async () => {
    const shell = new RecordingTrustPublisherShell({
      packages: [stable, missing],
      existing: [stable.name, missing.name],
    });

    await configureTrustedPublishers(shell, { skipLogin: true, packages: [missing.name] });

    expect(shell.events).toEqual([`exists:${missing.name}`, `list:${missing.name}`, `trust:${missing.name}:false`]);
  });

  it('parses empty npm trust list output as no trusted publishers', () => {
    expect(parseTrustedPublishers('', stable.name)).toEqual([]);
    expect(parseTrustedPublishers('null', stable.name)).toEqual([]);
  });
});

class RecordingTrustPublisherShell implements TrustPublisherShell<ReleasePackageInfo> {
  readonly repository = 'scope/repo';
  readonly workflow = 'publish.yml';
  readonly events: string[] = [];
  readonly logs: string[] = [];
  readonly errors: string[] = [];
  private readonly packages: ReleasePackageInfo[];
  private readonly existing: Set<string>;
  private readonly trustedPublisherByPackage: Record<string, TrustedPublisher[]>;

  constructor(options: {
    packages: ReleasePackageInfo[];
    existing: string[];
    trustedPublishers?: Record<string, TrustedPublisher[]>;
  }) {
    this.packages = options.packages;
    this.existing = new Set(options.existing);
    this.trustedPublisherByPackage = options.trustedPublishers ?? {};
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

  async trustPublisher(pkg: ReleasePackageInfo, dryRun: boolean): Promise<'configured' | 'already-configured'> {
    this.events.push(`trust:${pkg.name}:${dryRun}`);
    return 'configured';
  }

  async trustedPublishers(pkg: ReleasePackageInfo): Promise<TrustedPublisher[]> {
    this.events.push(`list:${pkg.name}`);
    return this.trustedPublisherByPackage[pkg.name] ?? [];
  }

  log(message: string): void {
    this.logs.push(message);
  }

  error(message: string): void {
    this.errors.push(message);
  }
}
