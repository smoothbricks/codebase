/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  type CiWorkflowDefinitionOptions,
  CiWorkflowStepKind,
  defineCiWorkflow,
  renderCiWorkflowYaml,
} from '../ci-workflow.js';

const nixosRunsOn = ['nixos-latest-x64', 'self-hosted'] as const;

function options(overrides: Partial<CiWorkflowDefinitionOptions> = {}): CiWorkflowDefinitionOptions {
  return {
    deploy: false,
    browserTests: false,
    e2eDeployment: false,
    pushBranches: ['main'],
    ...overrides,
  };
}

describe('CI workflow definition', () => {
  it('renders the checked-in local CI workflow copy', async () => {
    const rendered = renderCiWorkflowYaml(options({ runsOn: [...nixosRunsOn] }));
    const packageRoot = join(import.meta.dir, '..', '..', '..');

    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/ci.yml'), 'utf8')).resolves.toBe(rendered);
  });

  it('deploys immediately after build and renumbers following steps', () => {
    const definition = options({ deploy: true, browserTests: true });
    const steps = defineCiWorkflow(definition);
    const rendered = renderCiWorkflowYaml(definition);

    expect(steps.map((step) => [step.kind, step.number])).toEqual([
      [CiWorkflowStepKind.Checkout, 2],
      [CiWorkflowStepKind.SetupDevenv, 3],
      [CiWorkflowStepKind.SetNxShas, 4],
      [CiWorkflowStepKind.RestoreNxCache, 5],
      [CiWorkflowStepKind.Build, 6],
      [CiWorkflowStepKind.Deploy, 7],
      [CiWorkflowStepKind.Lint, 8],
      [CiWorkflowStepKind.UnitTests, 9],
      [CiWorkflowStepKind.BrowserTests, 10],
      [CiWorkflowStepKind.ManagedFilesCheck, 11],
      [CiWorkflowStepKind.ManagedFilesDispatch, 12],
      [CiWorkflowStepKind.SaveNxCache, 13],
      [CiWorkflowStepKind.UploadTraceDbs, 14],
      [CiWorkflowStepKind.SaveNixDevenv, 15],
    ]);
    expect(rendered.match(/- name: 🚀 Deploy Stage/g)).toHaveLength(1);
    expect(rendered).toContain('id: deploy');
    expect(rendered).toContain('smoo github-ci nx-deploy --mode run-many --name "Deploy Stage" --step 7');
    expect(rendered).toContain('smoo github-ci nx-smart --target test-browser --name "Browser Tests" --step 10');
    expect(rendered).toContain('group: ${{ github.workflow }}-${{ github.ref }}');
    expect(rendered).toContain('cancel-in-progress: true');
    expect(rendered).toContain('github.event.pull_request.head.repo.full_name == github.repository');
    expect(rendered).toContain("github.ref == 'refs/heads/private'");
    expect(rendered).toContain("# Step 13\n      # Nx's database cache needs artifact files");
  });

  it('adds only generic Cloudflare credentials for Wrangler-backed deploys', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true, deployProvider: 'cloudflare' }));

    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(rendered).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
    expect(rendered.match(/^\s+[A-Z][A-Z0-9_]+: \${{ secrets\.[A-Z][A-Z0-9_]+ }}$/gm)).toEqual([
      '          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}',
      '          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}',
    ]);
  });

  it('renders deployment E2E as a dependent job with an independent stage input', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true, e2eDeployment: true, runsOn: [...nixosRunsOn] }));

    expect(rendered).toContain('deployment-stage: ${{ steps.deploy.outputs.stage }}');
    expect(rendered).toContain('  e2e-deployment:\n    name: Deployment E2E\n    needs: main');
    expect(rendered).not.toContain('\n\n\n  e2e-deployment:');
    expect(rendered).toContain(
      "if: ${{ needs.main.result == 'success' && needs.main.outputs.deployment-stage != '' }}",
    );
    expect(rendered).toContain('timeout-minutes: 15');
    expect(rendered).toContain(
      'smoo github-ci nx-smart --target e2e-deployment --mode run-many --stage "${{ needs.main.outputs.deployment-stage }}" --name "Deployment E2E" --step 4',
    );
    expect(rendered.match(/name: Deployment E2E/g)).toHaveLength(2);
  });

  it('omits optional browser and deployment-E2E lanes when disabled', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true }));

    expect(rendered).not.toContain('--target test-browser');
    expect(rendered).not.toContain('  e2e-deployment:');
    expect(rendered).not.toContain('deployment-stage:');
  });

  it('uses the same architecture-scoped key to restore and save the Nx cache', async () => {
    const rendered = renderCiWorkflowYaml(options());
    const packageRoot = join(import.meta.dir, '..', '..', '..');
    const restoreAction = await readFile(join(packageRoot, '..', '..', '.github/actions/cache-nx/action.yml'), 'utf8');
    const restoreKey = restoreAction.match(/^\s*key: (.+)$/m)?.[1];
    const saveKey = rendered.match(/^\s*key: (.+)$/m)?.[1];

    expect(restoreKey).toBe('${{ runner.os }}-${{ runner.arch }}-nx-db-v1-${{ github.sha }}');
    expect(saveKey).toBe(restoreKey);
  });

  it('nixos config gates both jobs away from private runners for fork PRs', () => {
    const rendered = renderCiWorkflowYaml(options({ deploy: true, e2eDeployment: true, runsOn: [...nixosRunsOn] }));
    const runnerExpression =
      "runs-on:\n      ${{ (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository) &&\n      fromJSON('[\"nixos-latest-x64\",\"self-hosted\"]') || 'ubuntu-latest' }}";

    expect(rendered.match(new RegExp(runnerExpression.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(2);
    expect(rendered).toContain('uses: ./.github/actions/setup-devenv');
    expect(rendered).not.toContain('github-actions-bootstrap.sh');
  });
});
