import { describe, expect, it } from 'bun:test';
import type { ReleasePackageInfo } from '../core.js';
import {
  classifyNpmPublishFailure,
  NpmPublishCommandError,
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
const PUBLISH_ARGS = ['publish', 'pkg.tgz', '--access', 'public', '--tag', 'latest', '--provenance'];

/** npm's stderr from Publish run 39150196802, which smoo misreported as a trusted-publishing failure. */
const E413_STDERR = [
  'npm notice',
  'npm notice 📦  @smoothbricks/cowshed@0.2.4',
  'npm notice package size: 211.0 MB',
  'npm notice unpacked size: 598.4 MB',
  'npm notice total files: 41',
  'npm notice Publishing to https://registry.npmjs.org/ with tag latest and public access',
  'npm error code E413',
  'npm error 413 Payload Too Large - PUT https://registry.npmjs.org/@smoothbricks%2fcowshed',
  'npm error A complete log of this run can be found in: /home/runner/.npm/_logs/debug-0.log',
  '',
].join('\n');

function npmFailure(stderr: string): NpmPublishCommandError {
  return new NpmPublishCommandError(PUBLISH_ARGS, 1, stderr);
}

describe('npm publish failure classification', () => {
  it('classifies only npm auth codes as authentication failures', () => {
    for (const code of ['E401', 'E403', 'E404', 'ENEEDAUTH'] as const) {
      expect(classifyNpmPublishFailure(`npm error code ${code}\nnpm error something\n`)).toEqual({
        kind: 'auth',
        code,
      });
    }
    expect(classifyNpmPublishFailure('npm ERR! code ENEEDAUTH\n')).toEqual({ kind: 'auth', code: 'ENEEDAUTH' });
  });

  it('classifies E413 as too large with the packed size npm printed', () => {
    expect(classifyNpmPublishFailure(E413_STDERR)).toEqual({ kind: 'too-large', packageSize: '211.0 MB' });
    expect(classifyNpmPublishFailure('npm error code E413\n')).toEqual({ kind: 'too-large', packageSize: undefined });
  });

  it("keeps any other npm error verbatim, and npm's stderr tail when it printed no error line", () => {
    const stderr = 'npm notice package size: 1 kB\nnpm error code E500\nnpm error 500 Internal Server Error - PUT x\n';
    expect(classifyNpmPublishFailure(stderr)).toEqual({
      kind: 'npm',
      code: 'E500',
      detail: 'npm error code E500\nnpm error 500 Internal Server Error - PUT x',
    });
    expect(classifyNpmPublishFailure('Segmentation fault\n')).toEqual({
      kind: 'npm',
      code: undefined,
      detail: 'Segmentation fault',
    });
  });

  it('reports E413 as a too-large tarball with its size and npm ceiling, never trusted-publishing advice', async () => {
    const shell = new RecordingPublishShell({ publishFails: true, publishFailure: npmFailure(E413_STDERR) });

    await expect(
      publishWithAuthDiagnostics(pkg, shell, { tokenPresent: false, repository: 'smoothbricks/codebase' }),
    ).rejects.toThrow(
      "@scope/pkg@1.2.3: npm rejected the tarball as too large (E413 Payload Too Large, package size 211.0 MB; npm's ceiling is ~200 MB).",
    );

    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('tarball too large (E413)');
    expect(shell.errors[0]).toContain('Package size: 211.0 MB');
    expect(shell.errors[0]).toContain('roughly 200 MB');
    expect(shell.errors[0]).toContain('https://github.com/npm/documentation/issues/1691');
    expect(shell.errors[0]).not.toContain('trusted publishing is not configured');
    expect(shell.errors[0]).not.toContain('Run locally: smoo release trust-publisher');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('## npm rejected the package as too large');
    expect(shell.summaries[0]).toContain('Package size: 211.0 MB');
  });

  it("reports any other npm error with npm's own lines, never trusted-publishing advice", async () => {
    const stderr = 'npm error code EOTP\nnpm error This operation requires a one-time password.\n';
    const shell = new RecordingPublishShell({ publishFails: true, publishFailure: npmFailure(stderr) });

    await expect(publishWithAuthDiagnostics(pkg, shell, { tokenPresent: false })).rejects.toThrow(
      '@scope/pkg@1.2.3: npm publish failed (EOTP):\nnpm error code EOTP\nnpm error This operation requires a one-time password.',
    );

    expect(shell.errors[0]).toContain('npm error This operation requires a one-time password.');
    expect(shell.errors[0]).not.toContain('trust-publisher');
    expect(shell.summaries[0]).toContain('## npm publish failed');
    expect(shell.summaries[0]).toContain('npm error code EOTP');
  });

  it('rethrows a failure npm never reported, such as packing, unchanged and without a banner', async () => {
    const packFailure = new Error('bun pm pack failed with exit code 1');
    const shell = new RecordingPublishShell({ publishFails: true, publishFailure: packFailure });

    await expect(publishWithAuthDiagnostics(pkg, shell, { tokenPresent: false })).rejects.toBe(packFailure);

    expect(shell.errors).toEqual([]);
    expect(shell.summaries).toEqual([]);
  });
});

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
  readonly publishFailure: Error;
  publishCalls = 0;
  private readonly publishFails: boolean;
  private readonly versionVisibleAfterFailure: boolean;
  private readonly statusFails: string | undefined;

  constructor(options: {
    publishFails: boolean;
    publishFailure?: Error;
    versionVisibleAfterFailure?: boolean;
    statusFails?: string;
  }) {
    this.publishFails = options.publishFails;
    this.publishFailure =
      options.publishFailure ??
      npmFailure(
        'npm error code ENEEDAUTH\nnpm error need auth This command requires you to be logged in to https://registry.npmjs.org/\n',
      );
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
