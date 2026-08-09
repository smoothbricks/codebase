/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions verify emitted GitHub Actions expressions literally. */
import { describe, expect, it } from 'bun:test';
import { managedFileTargetsForTest } from '../managed-files.js';
import { renderPrPreviewCleanupWorkflowYaml } from '../pr-preview-cleanup-workflow.js';

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
});
