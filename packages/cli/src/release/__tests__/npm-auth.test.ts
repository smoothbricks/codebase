import { describe, expect, it } from 'bun:test';
import type { ReleasePackageInfo } from '../core.js';
import {
  type NpmPublishDiagnosticShell,
  npmPublishAuthFailureMarkdown,
  npmPublishAuthFailureMessage,
  publishWithAuthDiagnostics,
} from '../npm-auth.js';

const pkg: Pick<ReleasePackageInfo, 'name' | 'version'> = { name: '@scope/pkg', version: '1.2.3' };

describe('npm publish auth diagnostics', () => {
  it('explains trusted publishing setup when an existing package publish is unauthenticated', () => {
    const message = npmPublishAuthFailureMessage(pkg, {
      useBootstrapToken: false,
      tokenPresent: true,
      repository: 'smoothbricks/codebase',
    });
    const markdown = npmPublishAuthFailureMarkdown(pkg, {
      useBootstrapToken: false,
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

  it('explains the bootstrap token path for first package publishes', () => {
    const message = npmPublishAuthFailureMessage(pkg, { useBootstrapToken: true, tokenPresent: false });
    const markdown = npmPublishAuthFailureMarkdown(pkg, { useBootstrapToken: true, tokenPresent: false });

    expect(message).toContain('temporary npm automation token');
    expect(message).toContain('NODE_AUTH_TOKEN/NPM_TOKEN is not set');
    expect(message).toContain('After the first successful publish, run:');
    expect(message).toContain('smoo release trust-publisher');
    expect(markdown).toContain('Add a temporary `NPM_TOKEN` repository secret');
    expect(markdown).toContain('After the first successful publish, run `smoo release trust-publisher`');
  });

  it('reports trusted-publishing guidance and writes it to the publish summary after existing package auth failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true });

    await expect(
      publishWithAuthDiagnostics(pkg, shell, {
        useBootstrapToken: false,
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

  it('reports bootstrap token guidance and writes it to the publish summary after first-publish auth failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true });

    await expect(
      publishWithAuthDiagnostics(pkg, shell, { useBootstrapToken: true, tokenPresent: false }),
    ).rejects.toThrow('@scope/pkg@1.2.3: npm publish authentication failed');

    expect(shell.errors).toHaveLength(1);
    expect(shell.errors[0]).toContain('temporary npm automation token');
    expect(shell.errors[0]).toContain('NODE_AUTH_TOKEN/NPM_TOKEN is not set');
    expect(shell.errors[0]).toContain('smoo release trust-publisher');
    expect(shell.summaries).toHaveLength(1);
    expect(shell.summaries[0]).toContain('Add a temporary `NPM_TOKEN` repository secret');
    expect(shell.logs).toEqual([]);
  });

  it('continues without auth warning when the package version appears on npm after publish failure', async () => {
    const shell = new RecordingPublishShell({ publishFails: true, versionVisibleAfterFailure: true });

    await publishWithAuthDiagnostics(pkg, shell, { useBootstrapToken: false, tokenPresent: true });

    expect(shell.errors).toEqual([]);
    expect(shell.summaries).toEqual([]);
    expect(shell.logs).toEqual(['@scope/pkg@1.2.3: publish result already visible on npm; continuing.']);
  });
});

class RecordingPublishShell implements NpmPublishDiagnosticShell {
  readonly logs: string[] = [];
  readonly errors: string[] = [];
  readonly summaries: string[] = [];
  private readonly publishFails: boolean;
  private readonly versionVisibleAfterFailure: boolean;

  constructor(options: { publishFails: boolean; versionVisibleAfterFailure?: boolean }) {
    this.publishFails = options.publishFails;
    this.versionVisibleAfterFailure = options.versionVisibleAfterFailure === true;
  }

  async publish(): Promise<void> {
    if (this.publishFails) {
      throw new Error('ENEEDAUTH');
    }
  }

  async versionExists(): Promise<boolean> {
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
