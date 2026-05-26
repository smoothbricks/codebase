import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { defineCiWorkflow, renderCiWorkflowYaml } from '../ci-workflow.js';

describe('CI workflow definition', () => {
  it('renders the checked-in local CI workflow copy', async () => {
    const rendered = renderCiWorkflowYaml({ deploy: false, pushBranches: ['main'] });
    const packageRoot = join(import.meta.dir, '..', '..', '..');

    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/ci.yml'), 'utf8')).resolves.toBe(rendered);
  });

  it('inserts deploy after tests and renumbers following deeplink steps', () => {
    const steps = defineCiWorkflow({ deploy: true, pushBranches: ['main'] });
    const rendered = renderCiWorkflowYaml({ deploy: true, pushBranches: ['main'] });

    expect(steps.map((step) => [step.kind, step.number])).toContainEqual(['deploy', 9]);
    expect(rendered).toContain('- name: 🚀 Deploy Staging');
    expect(rendered).toContain(
      'smoo github-ci nx-deploy --configuration staging --mode affected --name "Deploy Staging" --step 9',
    );
    expect(rendered).toContain("# Step 10\n      # Nx's database cache needs artifact files");
  });
});
