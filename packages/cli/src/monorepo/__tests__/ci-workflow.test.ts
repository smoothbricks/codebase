/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { CiWorkflowStepKind, defineCiWorkflow, renderCiWorkflowYaml } from '../ci-workflow.js';

describe('CI workflow definition', () => {
  it('renders the checked-in local CI workflow copy', async () => {
    const rendered = renderCiWorkflowYaml({ deploy: false, pushBranches: ['main'] });
    const packageRoot = join(import.meta.dir, '..', '..', '..');

    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/ci.yml'), 'utf8')).resolves.toBe(rendered);
  });

  it('inserts deploy after tests and renumbers following deeplink steps', () => {
    const steps = defineCiWorkflow({ deploy: true, pushBranches: ['main'] });
    const rendered = renderCiWorkflowYaml({ deploy: true, pushBranches: ['main'] });

    expect(steps.map((step) => [step.kind, step.number])).toContainEqual([CiWorkflowStepKind.Deploy, 9]);
    expect(rendered).toContain('- name: 🚀 Deploy Staging');
    expect(rendered).not.toContain('CLOUDFLARE_API_TOKEN');
    expect(rendered).not.toContain('CLOUDFLARE_ACCOUNT_ID');
    expect(rendered).toContain(
      'smoo github-ci nx-deploy --configuration staging --mode affected --name "Deploy Staging" --step 9',
    );
    expect(rendered).toContain("# Step 10\n      # Nx's database cache needs artifact files");
  });

  it('adds Cloudflare credentials for Wrangler-backed deploys', () => {
    const rendered = renderCiWorkflowYaml({ deploy: true, deployProvider: 'cloudflare', pushBranches: ['main'] });

    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(rendered).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
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
});
