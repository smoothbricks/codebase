import { describe, expect, it } from 'bun:test';
import type { BootstrapNpmPackagesOptions } from '../bootstrap-npm-packages.js';
import type { ReleasePackageInfo } from '../core.js';
import {
  configureTrustedPublishers,
  npmTrustGithubArgs,
  npmTrustListAccessDenied,
  parseTrustedPublishers,
  type TrustedPublisher,
  type TrustedPublisherLookup,
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

  it('reports package owners and opens npm login after trust access is denied', async () => {
    const shell = new RecordingTrustPublisherShell({
      packages: [stable],
      existing: [stable.name],
      trustedPublisherResponses: {
        [stable.name]: [
          {
            status: 'access-denied',
            identity: 'current-user',
            owners: 'package-owner <owner@example.test>',
          },
          [],
        ],
      },
    });

    await configureTrustedPublishers(shell, {});

    expect(shell.events).toEqual([
      `exists:${stable.name}`,
      `list:${stable.name}`,
      'login',
      `list:${stable.name}`,
      `trust:${stable.name}:false`,
    ]);
    expect(shell.errors.join('\n')).toContain('npm account "current-user"');
    expect(shell.errors.join('\n')).toContain('package-owner <owner@example.test>');
    expect(shell.logs).toContain(
      `${stable.name}: opening npm browser login. Sign in as a listed package owner, then return here.`,
    );
  });

  it('stops after one owner login when the selected account still lacks access', async () => {
    const shell = new RecordingTrustPublisherShell({
      packages: [stable],
      existing: [stable.name],
      trustedPublisherResponses: {
        [stable.name]: [
          {
            status: 'access-denied',
            identity: 'current-user',
            owners: 'package-owner <owner@example.test>',
          },
          {
            status: 'access-denied',
            identity: 'wrong-user',
            owners: 'package-owner <owner@example.test>',
          },
        ],
      },
    });

    await expect(configureTrustedPublishers(shell, {})).rejects.toThrow(
      'npm browser login completed, but the selected account still cannot manage this package',
    );
    expect(shell.events).toEqual([`exists:${stable.name}`, `list:${stable.name}`, 'login', `list:${stable.name}`]);
  });

  it('grants the trusted workflow permission to run npm publish', () => {
    expect(npmTrustGithubArgs(stable.name, 'scope/repo', 'publish.yml', false)).toEqual([
      'trust',
      'github',
      stable.name,
      '--file',
      'publish.yml',
      '--repo',
      'scope/repo',
      '--allow-publish',
      '--yes',
    ]);
    expect(npmTrustGithubArgs(stable.name, 'scope/repo', 'publish.yml', true).at(-1)).toBe('--dry-run');
  });

  it('recognizes npm trust access-denied output', () => {
    expect(npmTrustListAccessDenied('{"error":{"code":"E403"}}', '')).toBe(true);
    expect(npmTrustListAccessDenied('', 'npm error code E401')).toBe(true);
    expect(
      npmTrustListAccessDenied(
        '',
        'npm error 401 Unauthorized - GET https://registry.npmjs.org/-/package/@scope%2fpkg/trust - {"success":false,"error":"You must be logged in to publish packages."}',
      ),
    ).toBe(true);
    expect(npmTrustListAccessDenied('', 'npm error code EOTP')).toBe(false);
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
  private readonly trustedPublisherResponses: Record<string, TrustedPublisherLookup[]>;

  constructor(options: {
    packages: ReleasePackageInfo[];
    existing: string[];
    trustedPublishers?: Record<string, TrustedPublisher[]>;
    trustedPublisherResponses?: Record<string, TrustedPublisherLookup[]>;
  }) {
    this.packages = options.packages;
    this.existing = new Set(options.existing);
    this.trustedPublisherByPackage = options.trustedPublishers ?? {};
    this.trustedPublisherResponses = options.trustedPublisherResponses ?? {};
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
    this.events.push('login');
  }

  async trustPublisher(pkg: ReleasePackageInfo, dryRun: boolean): Promise<'configured' | 'already-configured'> {
    this.events.push(`trust:${pkg.name}:${dryRun}`);
    return 'configured';
  }

  async trustedPublishers(pkg: ReleasePackageInfo): Promise<TrustedPublisherLookup> {
    this.events.push(`list:${pkg.name}`);
    const response = this.trustedPublisherResponses[pkg.name]?.shift();
    return response ?? this.trustedPublisherByPackage[pkg.name] ?? [];
  }

  log(message: string): void {
    this.logs.push(message);
  }

  error(message: string): void {
    this.errors.push(message);
  }
}
