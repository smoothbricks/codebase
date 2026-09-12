/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

import { renderRunsOnLine } from './github-runs-on.js';

export interface PrPreviewCleanupWorkflowOptions {
  runsOn?: string | string[];
}

export function renderPrPreviewCleanupWorkflowYaml(options: PrPreviewCleanupWorkflowOptions = {}): string {
  return `name: PR Preview Cleanup

on:
  pull_request:
    types: [closed]

permissions:
  contents: read

jobs:
  cleanup:
    name: Cleanup PR environment
    if: github.event.pull_request.head.repo.full_name == github.repository
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 15
    defaults:
      run:
        working-directory: tooling/direnv
    steps:
      - name: Checkout
        uses: actions/checkout@v6.0.2
        with:
          filter: blob:none
          fetch-depth: 1

      - name: Setup Nix/devenv
        uses: ./.github/actions/setup-devenv

      - name: Cleanup PR environment
        env:
          CLOUDFLARE_API_TOKEN: \${{ secrets.CLOUDFLARE_API_TOKEN }}
          CLOUDFLARE_ACCOUNT_ID: \${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
        run: smoo wrangler cleanup-pr --pr \${{ github.event.pull_request.number }}
`;
}
