/**
 * Generate GitHub Actions workflow for automated dependency updates
 */

import { existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { DepUpdaterConfig } from '../config.js';
import { getRepoRoot } from '../git.js';
import type { GenerateWorkflowOptions } from '../types.js';
import { safeResolve } from '../utils/path-validation.js';

/**
 * Generate GitHub Actions workflow YAML content
 */
function generateWorkflowContent(options: { schedule: string; workflowName: string; useAI: boolean }): string {
  const { schedule, workflowName, useAI } = options;

  return `name: ${workflowName}

on:
  schedule:
    # Run daily at 2 AM UTC
    - cron: '${schedule}'
  workflow_dispatch: # Allow manual triggers

permissions:
  contents: write
  pull-requests: write

jobs:
  update-dependencies:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0 # Fetch all history for proper git operations

      - name: Setup Bun
        uses: oven-sh/setup-bun@v2
        with:
          bun-version: latest

      - name: Install dependencies
        run: bun install

      - name: Build dep-updater
        run: |
          cd packages/dep-updater
          bun run build

      - name: Configure git
        run: |
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          git config --global user.name "github-actions[bot]"

      - name: Run dependency updater
        env:
          GH_TOKEN: \${{ secrets.GH_PAT }}${useAI ? '\n          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}' : ''}
        run: |
          # Run the built CLI directly
          bun run packages/dep-updater/dist/cli.js update-deps --verbose${!useAI ? ' --skip-ai' : ''}
`;
}

/**
 * Generate GitHub Actions workflow file
 */
export async function generateWorkflow(config: DepUpdaterConfig, options: GenerateWorkflowOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  // Default values
  const schedule = options.schedule || '0 2 * * *'; // 2 AM UTC daily
  const workflowName = options.workflowName || 'Update Dependencies';
  const useAI = !options.skipAI && config.ai?.apiKey !== undefined;

  config.logger?.info('Generating GitHub Actions workflow...\n');
  config.logger?.info(`  Schedule: ${schedule}`);
  config.logger?.info(`  Workflow name: ${workflowName}`);
  config.logger?.info(`  AI analysis: ${useAI ? 'enabled' : 'disabled'}\n`);

  if (options.dryRun) {
    config.logger?.info('[DRY RUN] Would generate workflow file');
    config.logger?.info('\nWorkflow content:\n');
    config.logger?.info(generateWorkflowContent({ schedule, workflowName, useAI }));
    return;
  }

  // Create .github/workflows directory
  const workflowDir = safeResolve(repoRoot, '.github/workflows');
  if (!existsSync(workflowDir)) {
    await mkdir(workflowDir, { recursive: true });
    config.logger?.info('✓ Created .github/workflows directory');
  }

  // Generate workflow content
  const workflowContent = generateWorkflowContent({ schedule, workflowName, useAI });

  // Write workflow file
  const workflowPath = join(workflowDir, 'update-deps.yml');
  await writeFile(workflowPath, workflowContent, 'utf-8');

  config.logger?.info(`✓ Generated workflow file: ${workflowPath}\n`);

  // Print next steps
  config.logger?.info('Next steps:');
  config.logger?.info('  1. Create a fine-grained Personal Access Token:');
  config.logger?.info('     a. Go to https://github.com/settings/tokens?type=beta');
  config.logger?.info('     b. Click "Generate new token"');
  config.logger?.info('     c. Set token name (e.g., "dep-updater")');
  config.logger?.info("     d. Set expiration (max 1 year, you'll need to regenerate)");
  config.logger?.info('     e. Select repository access (only this repo or specific repos)');
  config.logger?.info('     f. Set permissions:');
  config.logger?.info('        - Contents: Read and write');
  config.logger?.info('        - Pull requests: Read and write');
  config.logger?.info('        - Workflows: Read and write');
  config.logger?.info('     g. Generate token and copy it');
  config.logger?.info('');
  config.logger?.info('  2. Add the token as a repository secret:');
  config.logger?.info('     a. Go to repository Settings → Secrets and variables → Actions');
  config.logger?.info('     b. Click "New repository secret"');
  config.logger?.info('     c. Name: GH_PAT');
  config.logger?.info('     d. Value: paste your token');
  config.logger?.info('     e. Click "Add secret"');
  if (useAI) {
    config.logger?.info('');
    config.logger?.info('  3. Add ANTHROPIC_API_KEY to GitHub Secrets (same steps as above)');
  }
  config.logger?.info('');
  config.logger?.info(`  ${useAI ? '4' : '3'}. Review the generated workflow file`);
  config.logger?.info(`  ${useAI ? '5' : '4'}. Commit and push the workflow file`);
  config.logger?.info(`  ${useAI ? '6' : '5'}. The workflow will run daily at 2 AM UTC`);
  config.logger?.info(`  ${useAI ? '7' : '6'}. You can also trigger it manually from GitHub Actions tab\n`);

  config.logger?.info('Why fine-grained PAT? It triggers CI workflows and has better security than classic tokens.');
  if (useAI) {
    config.logger?.info('Note: ANTHROPIC_API_KEY is required for AI-powered changelog analysis.');
  }
  config.logger?.info('\n// TODO: Add GitHub App authentication support for production use (not user-dependent)');
}
