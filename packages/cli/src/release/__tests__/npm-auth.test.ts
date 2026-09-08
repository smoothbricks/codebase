import { describe, expect, it } from 'bun:test';
import type { ReleasePackageInfo } from '../core.js';
import {
  type NpmPublishDiagnosticShell,
  npmProvenanceRunnerRefusalMarkdown,
  npmProvenanceRunnerRefusalMessage,
  npmPublishAuthFailureMarkdown,
  npmPublishAuthFailureMessage,
  publishWithAuthDiagnostics,
  selfHostedGithubProvenanceRefusal,
} from '../npm-auth.js';
import { privateNpmPublishArgs, publishPrivateWithDiagnostics } from '../private-npm.js';

const pkg: Pick<ReleasePackageInfo, 'name' | 'version'> = { name: '@scope/pkg', version: '1.2.3' };

describe('npm publish auth diagnostics', () => {
  it('explains trusted publishing setup when an existing package publish is unauthenticated', () => {
    const message = npmPublishAuthFailureMessage(pkg, {
      tokenPresent: true,
      repository: 'smoothbricks/codebase',
    });
    const markdown = npmPublishAuthFailureMarkdown(pkg, {
      tokenPresent: true,
      repository: 'smoothbricks/codebase',
    });

    expect(message).toContain('🚨 npm publish authentication failed for @scope/pkg@1.2.3');
    expect(message).toContain('smoo release trust-publisher');
    expect(message).toContain('NODE_AUTH_TOKEN/NPM_TOKEN is set but unused');
    expect(message).toContain('repository: smoothbricks/codebase');
    expect(message).toContain('workflow: publish.yml');
    expect(markdown).toContain('## 🚨 npm Publish Authentication Failed');
    expect(markdown).toContain('Run locally: `smoo release trust-publisher`');
    expect(markdown).toContain('repository `smoothbricks/codebase` and workflow `publish.yml`');
  });

  it('points first package publishes to local placeholder bootstrap', () => {
    const message = npmPublishAuthFailureMessage(pkg, { tokenPresent: false });
    const markdown = npmPublishAuthFailureMarkdown(pkg, { tokenPresent: false });

    expect(message).toContain('smoo release trust-publisher --bootstrap');
    expect(markdown).toContain('smoo release trust-publisher --bootstrap');
  });

  it('reports trusted-publishing guidance and writes it to the publish summary after existing package auth failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true });

    await expect(
      publishWithAuthDiagnostics(pkg, shell, {
        tokenPresent: true,
        repository: 'smoothbricks/codebase',
      }),
    ).rejects.toThrow('@scope/pkg@1.2.3: npm publish authentication failed');

    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('🚨 npm publish authentication failed for @scope/pkg@1.2.3');
    expect(shell.errors[0]).toContain('NODE_AUTH_TOKEN/NPM_TOKEN is set but unused');
    expect(shell.errors[0]).toContain('smoo release trust-publisher');
    expect(shell.errors[0]).toContain('repository: smoothbricks/codebase');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('## 🚨 npm Publish Authentication Failed');
    expect(shell.summaries[0]).toContain('Run locally: `smoo release trust-publisher`');
    expect(shell.logs).toEqual([]);
  });

  it('reports bootstrap command guidance and writes it to the publish summary after first-publish auth failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true });

    await expect(publishWithAuthDiagnostics(pkg, shell, { tokenPresent: false })).rejects.toThrow(
      '@scope/pkg@1.2.3: npm publish authentication failed',
    );

    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('smoo release trust-publisher --bootstrap');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('smoo release trust-publisher --bootstrap');
    expect(shell.logs).toEqual([]);
  });

  it('continues without auth warning when the package version appears on npm after publish failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true, versionVisibleAfterFailure: true });

    await publishWithAuthDiagnostics(pkg, shell, { tokenPresent: true });

    expect(shell.errors).toEqual([]);
    expect(shell.summaries).toEqual([]);
    expect(shell.logs).toEqual(['@scope/pkg@1.2.3: publish result already visible on npm; continuing.']);
  });

  it('names runner policy and does not publish when GitHub Actions provenance is self-hosted', async () => {
    const shell = new RecordingPublishShell({ publishFails: false });
    const npmArgs = ['publish', 'pkg.tgz', '--access', 'public', '--tag', 'latest', '--provenance'];
    const env = { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'self-hosted' };

    expect(selfHostedGithubProvenanceRefusal(npmArgs, env)).toBe(true);
    expect(npmProvenanceRunnerRefusalMessage(pkg)).toContain('RUNNER_ENVIRONMENT=self-hosted');
    expect(npmProvenanceRunnerRefusalMessage(pkg)).toContain('github-hosted');
    expect(npmProvenanceRunnerRefusalMessage(pkg)).toContain('Do not run smoo release trust-publisher');
    expect(npmProvenanceRunnerRefusalMessage(pkg)).not.toContain('Run locally: smoo release trust-publisher');
    expect(npmProvenanceRunnerRefusalMessage(pkg)).not.toContain('smoo expected npm trusted publishing');
    expect(npmProvenanceRunnerRefusalMarkdown(pkg)).toContain('## npm provenance requires a GitHub-hosted runner');
    expect(npmProvenanceRunnerRefusalMarkdown(pkg)).toContain('Do not run `smoo release trust-publisher`');
    expect(npmProvenanceRunnerRefusalMarkdown(pkg)).not.toContain('Run locally: `smoo release trust-publisher`');

    await expect(
      publishWithAuthDiagnostics(pkg, shell, {
        tokenPresent: true,
        repository: 'smoothbricks/codebase',
        npmArgs,
        env,
      }),
    ).rejects.toThrow('npm provenance is refused on self-hosted GitHub Actions runners');

    expect(shell.publishCalls).toBe(0);
    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('RUNNER_ENVIRONMENT=self-hosted');
    expect(shell.errors[0]).toContain('github-hosted');
    expect(shell.errors[0]).toContain('Do not run smoo release trust-publisher');
    expect(shell.errors[0]).not.toContain('Run locally: smoo release trust-publisher');
    expect(shell.errors[0]).not.toContain('smoo expected npm trusted publishing');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('runner policy');
    expect(shell.summaries[0]).toContain('Do not run `smoo release trust-publisher`');
    expect(shell.summaries[0]).not.toContain('Run locally: `smoo release trust-publisher`');
    expect(shell.logs).toEqual([]);
  });

  it('publishes public provenance on a github-hosted GitHub Actions runner', async () => {
    const shell = new RecordingPublishShell({ publishFails: false });
    const npmArgs = ['publish', 'pkg.tgz', '--access', 'public', '--tag', 'latest', '--provenance'];

    await publishWithAuthDiagnostics(pkg, shell, {
      tokenPresent: false,
      npmArgs,
      env: { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'github-hosted' },
    });

    expect(
      selfHostedGithubProvenanceRefusal(npmArgs, { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'github-hosted' }),
    ).toBe(false);
    expect(shell.publishCalls).toBe(1);
    expect(shell.errors).toEqual([]);
    expect(shell.summaries).toEqual([]);
  });

  it('leaves local public provenance alone when GitHub Actions is unset', async () => {
    const shell = new RecordingPublishShell({ publishFails: false });
    const npmArgs = ['publish', 'pkg.tgz', '--access', 'public', '--tag', 'latest', '--provenance'];

    await publishWithAuthDiagnostics(pkg, shell, {
      tokenPresent: true,
      npmArgs,
      env: { RUNNER_ENVIRONMENT: 'self-hosted' },
    });

    expect(selfHostedGithubProvenanceRefusal(npmArgs, { RUNNER_ENVIRONMENT: 'self-hosted' })).toBe(false);
    expect(shell.publishCalls).toBe(1);
    expect(shell.errors).toEqual([]);
  });

  it('does not treat a self-hosted GitHub Actions publish without provenance as runner policy', async () => {
    const shell = new RecordingPublishShell({ publishFails: false });
    const npmArgs = ['publish', 'pkg.tgz', '--access', 'restricted', '--tag', 'latest'];

    await publishWithAuthDiagnostics(pkg, shell, {
      tokenPresent: false,
      npmArgs,
      env: { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'self-hosted' },
    });

    expect(
      selfHostedGithubProvenanceRefusal(npmArgs, { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'self-hosted' }),
    ).toBe(false);
    expect(shell.publishCalls).toBe(1);
    expect(shell.errors).toEqual([]);
  });

  it('still reports trusted-publishing guidance when github-hosted provenance auth fails', async () => {
    const shell = new RecordingPublishShell({ publishFails: true });

    await expect(
      publishWithAuthDiagnostics(pkg, shell, {
        tokenPresent: true,
        repository: 'smoothbricks/codebase',
        npmArgs: ['publish', 'pkg.tgz', '--access', 'public', '--tag', 'latest', '--provenance'],
        env: { GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'github-hosted' },
      }),
    ).rejects.toThrow('@scope/pkg@1.2.3: npm publish authentication failed');

    expect(shell.publishCalls).toBe(1);
    expect(shell.errors[0]).toContain('smoo release trust-publisher');
    expect(shell.errors[0]).not.toContain('RUNNER_ENVIRONMENT=self-hosted');
  });
});

describe('private npm publish diagnostics', () => {
  const registry = {
    scope: '@priv.test',
    registry: 'https://forgejo.example.test/api/packages/priv-owner/npm/',
    authKey: '//forgejo.example.test/api/packages/priv-owner/npm/:_authToken',
    readTokenEnv: 'PRIV_NPM_READ_TOKEN',
    publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
  };

  it('refuses with token env names and registry, never npmjs trusted-publisher repair', async () => {
    const shell = new RecordingPublishShell({ publishFails: true });

    await expect(publishPrivateWithDiagnostics(pkg, shell, registry)).rejects.toThrow(
      '@scope/pkg@1.2.3: private npm publish failed',
    );

    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('PRIV_NPM_READ_TOKEN');
    expect(shell.errors[0]).toContain('PRIV_NPM_PUBLISH_TOKEN');
    expect(shell.errors[0]).toContain('https://forgejo.example.test/api/packages/priv-owner/npm/');
    expect(shell.errors[0]).not.toContain('trust-publisher');
    expect(shell.errors[0]).not.toContain('provenance');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('## Private npm publish failed');
    expect(shell.logs).toEqual([]);
  });

  it('continues without auth warning when the version appears on the private registry after failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true, versionVisibleAfterFailure: true });

    await publishPrivateWithDiagnostics(pkg, shell, registry);

    expect(shell.errors).toEqual([]);
    expect(shell.summaries).toEqual([]);
    expect(shell.logs).toEqual([
      '@scope/pkg@1.2.3: publish result already visible on the private registry; continuing.',
    ]);
  });

  it('keeps the publish diagnostic when the follow-up status query also fails', async () => {
    const shell = new RecordingPublishShell({ publishFails: true, statusFails: '503 Service Unavailable' });

    // The probe is failure-aware and throws on 401/403/5xx. If that throw
    // escaped, the run would report a status problem for a failed publish and
    // write no operator summary at all.
    const failure = await publishPrivateWithDiagnostics(pkg, shell, registry).then(
      () => null,
      (error: unknown) => error,
    );

    if (!(failure instanceof Error)) {
      throw new Error('Expected publication to fail with an Error');
    }
    expect(failure.message).toContain('private npm publish failed; refusing');
    expect(failure.cause).toBe(shell.publishFailure);
    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('publication state is unknown');
    expect(shell.errors[0]).toContain('503 Service Unavailable');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('## Private npm publish failed');
    expect(shell.logs).toEqual([]);
  });

  it('builds restricted publish args against the resolved registry without provenance', () => {
    expect(privateNpmPublishArgs('/tmp/demo.tgz', 'latest', registry)).toEqual([
      'publish',
      '/tmp/demo.tgz',
      '--access',
      'restricted',
      '--tag',
      'latest',
      '--registry',
      'https://forgejo.example.test/api/packages/priv-owner/npm/',
    ]);
  });
});

class RecordingPublishShell implements NpmPublishDiagnosticShell {
  readonly logs: string[] = [];
  readonly errors: string[] = [];
  readonly summaries: string[] = [];
  readonly publishFailure = new Error('ENEEDAUTH');
  publishCalls = 0;
  private readonly publishFails: boolean;
  private readonly versionVisibleAfterFailure: boolean;
  private readonly statusFails: string | undefined;

  constructor(options: { publishFails: boolean; versionVisibleAfterFailure?: boolean; statusFails?: string }) {
    this.publishFails = options.publishFails;
    this.versionVisibleAfterFailure = options.versionVisibleAfterFailure === true;
    this.statusFails = options.statusFails;
  }

  async publish(): Promise<void> {
    this.publishCalls += 1;
    if (this.publishFails) {
      throw this.publishFailure;
    }
  }

  async versionExists(): Promise<boolean> {
    if (this.statusFails !== undefined) {
      throw new Error(this.statusFails);
    }
    return this.versionVisibleAfterFailure;
  }

  log(message: string): void {
    this.logs.push(message);
  }

  error(message: string): void {
    this.errors.push(message);
  }

  async appendSummary(markdown: string): Promise<void> {
    this.summaries.push(markdown);
  }
}
