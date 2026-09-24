/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions verify emitted GitHub Actions expressions literally. */
import { describe, expect, it } from 'bun:test';
import { renderCiWorkflowYaml } from '@smoothbricks/nx-plugin/managed-files/ci-workflow';
import { renderPrPreviewCleanupWorkflowYaml } from '@smoothbricks/nx-plugin/managed-files/pr-preview-cleanup-workflow';
import { managedFileTargetsForTest } from '../managed-files.js';

const deployingCi = {
  deploy: true,
  deployProvider: 'cloudflare',
  browserTests: false,
  e2eDeployment: false,
  pushBranches: ['main'],
} as const satisfies Parameters<typeof renderCiWorkflowYaml>[0];

/** The step named `name`, from its `- name:` line to the next step, without blank or comment lines. */
function stepLines(workflow: string, name: string): string[] {
  const lines = workflow.split('\n');
  const start = lines.indexOf(`      - name: ${name}`);
  expect(start).toBeGreaterThan(-1);
  const end = lines.findIndex((line, index) => index > start && /^ {0,6}[-\w]/.test(line));
  return lines.slice(start, end === -1 ? undefined : end).filter((line) => line.trim() !== '' && !/^\s*#/.test(line));
}

/** The step env line that says which secret `name` reads. */
function secretLine(step: string[], name: string): string | undefined {
  return step.find((line) => line.trim().startsWith(`${name}:`))?.trim();
}

describe('PR preview cleanup workflow', () => {
  it('renders one close-only same-repository cleanup job from the PR number', () => {
    const rendered = renderPrPreviewCleanupWorkflowYaml({ runsOn: ['nixos-latest-x64', 'self-hosted'] });
    expect(managedFileTargetsForTest).toContainEqual({
      target: '.github/workflows/pr-preview-cleanup.yml',
      executable: undefined,
    });

    expect(rendered).toContain('pull_request:\n    types: [closed]');
    expect(rendered).not.toContain('opened');
    expect(rendered).not.toContain('synchronize');
    expect(rendered).toContain('if: github.event.pull_request.head.repo.full_name == github.repository');
    expect(rendered.match(/smoo wrangler cleanup-pr --pr/g)).toHaveLength(1);
    expect(rendered).toContain('smoo wrangler cleanup-pr --pr ${{ github.event.pull_request.number }}');
    expect(rendered).toContain('uses: ./.github/actions/setup-devenv');
    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
  });

  describe('in a repository that installs from a private npm registry', () => {
    const privateNpm = {
      scope: '@priv.test',
      readTokenEnv: 'PRIV_NPM_READ_TOKEN',
      publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN',
    };
    const readTokenLine = '      PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}\n';

    it('carries the read token at job level, so setup-devenv can install before the cleanup runs', () => {
      const rendered = renderPrPreviewCleanupWorkflowYaml({ runsOn: 'ubuntu-latest', privateNpm });

      // In CI, setup-devenv refuses a declared registry credential the job
      // environment lacks, so the token has to be in scope before that step.
      expect(rendered).toContain(`        working-directory: tooling/direnv\n    env:\n${readTokenLine}    steps:\n`);
      expect(rendered.indexOf(readTokenLine)).toBeLessThan(rendered.indexOf('uses: ./.github/actions/setup-devenv'));
    });

    it('spells the token exactly as the CI jobs that run setup-devenv do', () => {
      const ci = renderCiWorkflowYaml({
        deploy: true,
        deployProvider: 'cloudflare',
        browserTests: false,
        e2eDeployment: false,
        pushBranches: ['main'],
        privateNpm,
      });

      expect(ci).toContain(readTokenLine);
      expect(renderPrPreviewCleanupWorkflowYaml({ privateNpm })).toContain(readTokenLine);
    });

    it('adds the read token and nothing else: the publish token never reaches the cleanup job', () => {
      const withToken = renderPrPreviewCleanupWorkflowYaml({ runsOn: 'ubuntu-latest', privateNpm });

      expect(withToken).not.toContain('PRIV_NPM_PUBLISH_TOKEN');
      expect(withToken.replace(`    env:\n${readTokenLine}`, '')).toBe(
        renderPrPreviewCleanupWorkflowYaml({ runsOn: 'ubuntu-latest' }),
      );
    });
  });

  // The cleanup deletes what the stage deploy created, in the account the
  // deploy's CLOUDFLARE_ACCOUNT_ID names, so the job must resolve Cloudflare's
  // credentials exactly as that deploy does.
  describe('reads Cloudflare as the credential that deployed the stage', () => {
    const environments = { staging: 'preview-stages', production: 'live' };
    const deploySecrets = {
      CLOUDFLARE_API_TOKEN: 'CF_STAGE_TOKEN',
      CLOUDFLARE_ACCOUNT_ID: 'CF_STAGE_ACCOUNT',
      E2E_CONTROL_TOKEN: 'E2E_CONTROL_TOKEN',
    };

    it('under the staging environment and secret mapping of the stage deploy step', () => {
      const ci = renderCiWorkflowYaml({ ...deployingCi, environments, deploySecrets });
      const cleanup = renderPrPreviewCleanupWorkflowYaml({ environments, deploySecrets });
      const deployStep = stepLines(ci, '🚀 Deploy Stage');
      const cleanupStep = stepLines(cleanup, 'Cleanup PR environment');

      // Environment secrets shadow repository secrets of the same name, so the
      // environment decides which CLOUDFLARE_API_TOKEN a job reads.
      expect(ci).toContain('    environment: preview-stages\n');
      expect(cleanup).toContain('    environment: preview-stages\n');
      expect(cleanup).not.toContain('environment: live');
      for (const name of ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID']) {
        expect(secretLine(cleanupStep, name)).toBe(secretLine(deployStep, name));
      }
      expect(secretLine(cleanupStep, 'CLOUDFLARE_API_TOKEN')).toBe(
        'CLOUDFLARE_API_TOKEN: ${{ secrets.CF_STAGE_TOKEN }}',
      );
    });

    it('with the default secrets and no environment where the repository declares neither', () => {
      const deployStep = stepLines(renderCiWorkflowYaml(deployingCi), '🚀 Deploy Stage');
      const cleanup = renderPrPreviewCleanupWorkflowYaml({ environments: { production: 'live' }, deploySecrets: {} });
      const cleanupStep = stepLines(cleanup, 'Cleanup PR environment');

      expect(cleanup).not.toContain('environment:');
      expect(cleanup).toBe(renderPrPreviewCleanupWorkflowYaml());
      for (const name of ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID']) {
        expect(secretLine(cleanupStep, name)).toBe(secretLine(deployStep, name));
      }
    });

    it('carrying the two Cloudflare credentials and none of the other deploy secrets', () => {
      const cleanup = renderPrPreviewCleanupWorkflowYaml({ environments, deploySecrets });

      expect(cleanup).not.toContain('E2E_CONTROL_TOKEN');
      expect(cleanup.match(/\$\{\{ secrets\./g)).toHaveLength(2);
    });
  });

  describe('in a repository that builds against sibling sources', () => {
    const sourceCheckouts = [
      { path: '../shared', repository: 'https://git.example.test/shared.git', tokenEnv: 'SOURCE_READ_TOKEN' },
    ];

    it('clones them after checkout and before setup-devenv, whose shell resolves path inputs on them', () => {
      const rendered = renderPrPreviewCleanupWorkflowYaml({ sourceCheckouts });
      const clone = rendered.indexOf(
        'git clone --filter=blob:none https://git.example.test/shared.git "$root/../shared"',
      );

      expect(clone).toBeGreaterThan(rendered.indexOf('uses: actions/checkout'));
      expect(clone).toBeLessThan(rendered.indexOf('uses: ./.github/actions/setup-devenv'));
    });

    it('with the same gate, token and commands as the CI job that runs setup-devenv', () => {
      const ci = renderCiWorkflowYaml({ ...deployingCi, sourceCheckouts });
      const cleanup = renderPrPreviewCleanupWorkflowYaml({ sourceCheckouts });

      expect(stepLines(cleanup, 'Check out sibling sources').slice(1)).toEqual(
        stepLines(ci, '📦 Check out sibling sources').slice(1),
      );
      expect(cleanup).toContain('SOURCE_READ_TOKEN: ${{ secrets.SOURCE_READ_TOKEN }}');
    });

    it('and renders no clone step where none is declared', () => {
      expect(renderPrPreviewCleanupWorkflowYaml({ sourceCheckouts: [] })).toBe(renderPrPreviewCleanupWorkflowYaml());
      expect(renderPrPreviewCleanupWorkflowYaml()).not.toContain('sibling sources');
    });
  });

  describe('in a repository that fetches from private Cargo git origins', () => {
    const cargoCredentials = {
      gitOrigins: [
        { origin: 'https://git.example.net', tokenEnv: 'SOURCE_READ_TOKEN', internalMirror: 'http://10.89.0.1:3000' },
      ],
      registryTokenEnvs: ['CARGO_REGISTRIES_EXAMPLE_TOKEN'],
    };
    const sourceCheckouts = [{ path: '../shared', repository: 'https://git.example.net/org/shared.git' }];
    const cargoEnv = [
      '      CARGO_REGISTRIES_EXAMPLE_TOKEN: ${{ secrets.CARGO_REGISTRIES_EXAMPLE_TOKEN }}',
      '      SOURCE_READ_TOKEN: ${{ secrets.SOURCE_READ_TOKEN }}',
    ];

    /** The steps between checkout and setup-devenv in the first job, without names, blank or comment lines. */
    function preflightLines(workflow: string): string[] {
      const lines = workflow.split('\n');
      const checkout = lines.findIndex((line) => line.includes('uses: actions/checkout'));
      const firstStep = lines.findIndex((line, index) => index > checkout && line.startsWith('      - name:'));
      const setup = lines.findIndex((line) => /^ {6}- name: .*Setup Nix\/devenv$/.test(line));
      return lines
        .slice(firstStep, setup)
        .filter((line) => line.trim() !== '' && !/^\s*#/.test(line) && !line.startsWith('      - name:'));
    }

    /** The first job's job-level env block, one line per variable. */
    function jobEnvLines(workflow: string): string[] {
      const lines = workflow.split('\n');
      const start = lines.indexOf('    env:');
      return start === -1 ? [] : lines.slice(start + 1, lines.indexOf('    steps:', start));
    }

    it('runs the Cargo credential step before the sibling clone and setup-devenv, exactly as the CI jobs do', () => {
      const ci = renderCiWorkflowYaml({ ...deployingCi, cargoCredentials, sourceCheckouts });
      const cleanup = renderPrPreviewCleanupWorkflowYaml({ cargoCredentials, sourceCheckouts });
      const rewrite = cleanup.indexOf('url.http://10.89.0.1:3000/.insteadOf');

      // That step writes git configuration into GITHUB_ENV (a credential helper, the runner proxy and
      // the rewrite onto the internal mirror) which later git commands in the job read, so this sibling
      // clone, declared without tokenEnv, clones from the mirror on a runner that cannot reach the
      // public forge. A clone declared with tokenEnv sets GIT_CONFIG_COUNT=1 for its own auth header,
      // which replaces that configuration, so it gets no helper, proxy or rewrite.
      expect(preflightLines(cleanup)).toEqual(preflightLines(ci));
      expect(rewrite).toBeGreaterThan(-1);
      expect(rewrite).toBeLessThan(cleanup.indexOf('git clone --filter=blob:none https://git.example.net/org/shared'));
    });

    it('carries the Cargo tokens at job level as the CI jobs do, so setup-devenv can resolve them', () => {
      const ci = renderCiWorkflowYaml({ ...deployingCi, cargoCredentials });

      expect(jobEnvLines(ci)).toEqual(expect.arrayContaining(cargoEnv));
      expect(jobEnvLines(renderPrPreviewCleanupWorkflowYaml({ cargoCredentials }))).toEqual(cargoEnv);
    });

    it('beside the private npm read token when both are declared', () => {
      const privateNpm = { scope: '@priv.test', readTokenEnv: 'PRIV_NPM_READ_TOKEN' };

      expect(jobEnvLines(renderPrPreviewCleanupWorkflowYaml({ cargoCredentials, privateNpm }))).toEqual([
        ...cargoEnv,
        '      PRIV_NPM_READ_TOKEN: ${{ secrets.PRIV_NPM_READ_TOKEN }}',
      ]);
    });
  });

  it('renders no job env where the repository declares no private read token', () => {
    const publishOnly = renderPrPreviewCleanupWorkflowYaml({
      runsOn: 'ubuntu-latest',
      privateNpm: { scope: '@priv.test', publishTokenEnv: 'PRIV_NPM_PUBLISH_TOKEN' },
    });

    expect(publishOnly).toBe(renderPrPreviewCleanupWorkflowYaml({ runsOn: 'ubuntu-latest' }));
    expect(publishOnly).not.toMatch(/^ {4}env:/m);
  });
});
