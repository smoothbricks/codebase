/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { CiWorkflowStepKind, defineCiWorkflow, renderCiWorkflowYaml } from '../ci-workflow.js';

const nixosRunsOn = ['nixos-latest-x64', 'self-hosted'] as const;

describe('CI workflow definition', () => {
  it('renders the checked-in local CI workflow copy', async () => {
    const rendered = renderCiWorkflowYaml({
      deploy: false,
      pushBranches: ['main'],
      runsOn: [...nixosRunsOn],
    });
    const packageRoot = join(import.meta.dir, '..', '..', '..');

    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/ci.yml'), 'utf8')).resolves.toBe(rendered);
  });

  it('inserts deploy after tests and renumbers following deeplink steps', () => {
    const steps = defineCiWorkflow({ deploy: true, pushBranches: ['main'] });
    const rendered = renderCiWorkflowYaml({ deploy: true, pushBranches: ['main'] });

    expect(steps.map((step) => [step.kind, step.number])).toContainEqual([CiWorkflowStepKind.Deploy, 11]);
    expect(rendered.match(/- name: 🚀 Deploy Environment/g)).toHaveLength(1);
    expect(rendered).not.toContain('CLOUDFLARE_API_TOKEN');
    expect(rendered).not.toContain('CLOUDFLARE_ACCOUNT_ID');
    expect(rendered).toContain('smoo github-ci nx-deploy --mode run-many --name "Deploy Environment" --step 11');
    expect(rendered).toContain('github.event.pull_request.head.repo.full_name == github.repository');
    expect(rendered).toContain("github.ref == 'refs/heads/private'");
    expect(rendered).toContain("# Step 12\n      # Nx's database cache needs artifact files");
    expect(rendered).toContain('uses: ./.github/actions/setup-devenv');
    expect(rendered).toContain('id: setup');
    expect(rendered).not.toContain('Setup Nix/devenv (fork)');
    expect(rendered).not.toContain('Setup Nix/devenv (nixos)');
    expect(rendered).not.toContain('github-actions-bootstrap.sh');
    expect(rendered).toContain('uses: ./.github/actions/save-nix-devenv');
    expect(rendered).toContain('runs-on: ubuntu-latest');
  });

  it('adds Cloudflare credentials for Wrangler-backed deploys', () => {
    const rendered = renderCiWorkflowYaml({ deploy: true, deployProvider: 'cloudflare', pushBranches: ['main'] });

    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(rendered).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
    expect(rendered).toContain('GITHUB_CLIENT_SECRET: ${{ secrets.GITHUB_CLIENT_SECRET }}');
    expect(rendered).toContain('GITHUB_APP_PRIVATE_KEY: ${{ secrets.GITHUB_APP_PRIVATE_KEY }}');
    expect(rendered).toContain('GITHUB_APP_PRIVATE_KEY_PEM: ${{ secrets.GITHUB_APP_PRIVATE_KEY_PEM }}');
    expect(rendered).toContain('OAUTH_STATE_SIGNING_KEY: ${{ secrets.OAUTH_STATE_SIGNING_KEY }}');
    expect(rendered).toContain('MAIL_CAPTURE_CONTROL_TOKEN: ${{ secrets.MAIL_CAPTURE_CONTROL_TOKEN }}');
    expect(rendered).toContain('TOKEN_ENCRYPTION_KEY: ${{ secrets.TOKEN_ENCRYPTION_KEY }}');
  });

  it('uses the same architecture-scoped key to restore and save the Nx cache', async () => {
    const rendered = renderCiWorkflowYaml({ deploy: false, pushBranches: ['main'] });
    const packageRoot = join(import.meta.dir, '..', '..', '..');
    const restoreAction = await readFile(join(packageRoot, '..', '..', '.github/actions/cache-nx/action.yml'), 'utf8');
    const restoreKey = restoreAction.match(/^\s*key: (.+)$/m)?.[1];
    const saveKey = rendered.match(/^\s*key: (.+)$/m)?.[1];

    expect(restoreKey).toBe('${{ runner.os }}-${{ runner.arch }}-nx-db-v1-${{ github.sha }}');
    expect(saveKey).toBe(restoreKey);
  });

  it('nixos config: fork PRs on ubuntu; one setup-devenv (composite owns host path)', () => {
    const rendered = renderCiWorkflowYaml({
      deploy: false,
      pushBranches: ['main'],
      runsOn: [...nixosRunsOn],
    });

    expect(rendered).toContain(
      "runs-on:\n      ${{ (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository) &&\n      fromJSON('[\"nixos-latest-x64\",\"self-hosted\"]') || 'ubuntu-latest' }}",
    );
    expect(rendered).toContain('uses: ./.github/actions/setup-devenv');
    expect(rendered).toContain('id: setup');
    expect(rendered).not.toContain('Setup Nix/devenv (fork)');
    expect(rendered).not.toContain('Setup Nix/devenv (nixos)');
    expect(rendered).not.toContain('github-actions-bootstrap.sh');
    expect(rendered).not.toContain('determinate-nix-action');
    expect(rendered).toContain('if: always()');
    expect(rendered).toContain('uses: ./.github/actions/save-nix-devenv');
    // workflow yaml does not embed composite internals
    expect(rendered).not.toContain('cache-node-modules');
    expect(rendered).not.toContain('cache-ttsc-plugins');
  });
});
