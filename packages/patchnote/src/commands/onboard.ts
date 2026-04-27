/**
 * Onboarding command -- creates a "Configure Patchnote" PR on first CI run
 * when no config file exists. Mirrors the Renovate onboarding pattern.
 */

import { mkdir, writeFile } from 'node:fs/promises';
import * as p from '@clack/prompts';
import type { PatchnoteConfig } from '../config.js';
import { defaultConfig, loadConfig } from '../config.js';
import { commit, createBranch, getRepoRoot, pushWithUpstream, stageFiles, switchBranch } from '../git.js';
import type { CommandExecutor, OnboardOptions, PackageUpdate, ProjectSetup } from '../types.js';
import { safeResolve } from '../utils/path-validation.js';
import { detectProjectSetup } from '../utils/project-detection.js';
import { generateJSONConfig } from './init.js';
import { runAllUpdaters } from './update-deps.js';

export const ONBOARD_BRANCH = 'chore/configure-patchnote';
export const ONBOARD_PR_TITLE = 'Configure Patchnote';

const MAX_PREVIEW_ROWS = 50;

/**
 * Build the markdown body for the onboarding PR.
 */
export function buildOnboardingPRBody(setup: ProjectSetup, configContent: string, updates: PackageUpdate[]): string {
  const lines: string[] = [];

  // Welcome header
  lines.push('# Configure Patchnote');
  lines.push('');
  lines.push(
    'Welcome! [Patchnote](https://github.com/smoothbricks/smoothbricks/tree/main/packages/patchnote) automates dependency updates across npm, Expo, and Nix ecosystems with intelligent PR stacking and optional AI-powered changelog analysis.',
  );
  lines.push('');
  lines.push(
    'This PR proposes an initial configuration based on your project. Review it, merge to start receiving dependency update PRs, or edit `tooling/patchnote.json` on this branch to customize.',
  );
  lines.push('');

  // Detected setup
  lines.push('## Detected Project Setup');
  lines.push('');
  lines.push(`- **Package manager:** ${setup.packageManager}`);
  lines.push(`- **Expo:** ${setup.hasExpo ? 'Yes' : 'No'}`);
  lines.push(`- **Nix:** ${setup.hasNix ? 'Yes' : 'No'}`);
  lines.push(`- **Syncpack:** ${setup.hasSyncpack ? 'Yes' : 'No'}`);
  lines.push('');

  // Proposed config
  lines.push('## Proposed Configuration');
  lines.push('');
  lines.push('```json');
  lines.push(configContent.trimEnd());
  lines.push('```');
  lines.push('');

  // Dependency update preview
  lines.push('## Dependency Update Preview');
  lines.push('');

  if (updates.length === 0) {
    lines.push(
      'No outdated dependencies detected. Once merged, patchnote will check for updates on each scheduled run.',
    );
  } else {
    lines.push('If this configuration were active, the following updates would be created:');
    lines.push('');
    lines.push('| Package | From | To | Type | Ecosystem |');
    lines.push('| --- | --- | --- | --- | --- |');

    const displayUpdates = updates.slice(0, MAX_PREVIEW_ROWS);
    for (const u of displayUpdates) {
      lines.push(`| ${u.name} | ${u.fromVersion} | ${u.toVersion} | ${u.updateType} | ${u.ecosystem} |`);
    }

    if (updates.length > MAX_PREVIEW_ROWS) {
      lines.push('');
      lines.push(`*(and ${updates.length - MAX_PREVIEW_ROWS} more packages...)*`);
    }
  }

  lines.push('');

  // Next steps
  lines.push('## Next Steps');
  lines.push('');
  lines.push('1. Review the proposed configuration above.');
  lines.push('2. Merge this PR to start receiving dependency update PRs.');
  lines.push(
    '3. To customize, edit `tooling/patchnote.json` on this branch and the preview will update on the next scheduled run.',
  );
  lines.push('');

  return lines.join('\n');
}

/**
 * Create or update an onboarding PR with default configuration and dry-run preview.
 */
export async function onboard(
  config: PatchnoteConfig,
  options: OnboardOptions,
  executor?: CommandExecutor,
): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  p.intro('Configure Patchnote');

  // Create GitHubCLIClient
  const { GitHubCLIClient } = await import('../auth/github-client.js');
  const client = new GitHubCLIClient(executor);

  // Check for existing onboarding PR
  const existingPRSpinner = p.spinner();
  existingPRSpinner.start('Checking for existing onboarding PR');
  const existingPR = await client.findPRByHead(repoRoot, ONBOARD_BRANCH);
  existingPRSpinner.stop(existingPR ? `Found existing PR #${existingPR.number}` : 'No existing onboarding PR');

  // Detect project setup
  const setupSpinner = p.spinner();
  setupSpinner.start('Detecting project setup');
  const setup = await detectProjectSetup(repoRoot);
  setupSpinner.stop('Project setup detected');

  config.logger?.info(`  Package manager: ${setup.packageManager}`);
  config.logger?.info(`  Expo detected: ${setup.hasExpo ? 'yes' : 'no'}`);
  config.logger?.info(`  Nix detected: ${setup.hasNix ? 'yes' : 'no'}`);

  // Generate config content
  const configContent = generateJSONConfig({
    enableExpo: setup.hasExpo,
    enableNix: setup.hasNix,
    enableAI: false,
    enableStacking: true,
    maxStackDepth: 5,
  });

  // Prepare a temporary config for dry-run by merging defaults with generated values
  let dryRunConfig: PatchnoteConfig;
  if (existingPR) {
    // For existing PR, checkout the onboarding branch to load possibly-modified config
    try {
      await switchBranch(repoRoot, ONBOARD_BRANCH, executor);
      dryRunConfig = await loadConfig(repoRoot);
    } catch {
      // If branch switch fails, use defaults
      dryRunConfig = { ...defaultConfig, repoRoot };
    }
  } else {
    // For new PR, parse the generated config and merge with defaults
    try {
      const parsedConfig = JSON.parse(configContent);
      dryRunConfig = { ...defaultConfig, ...parsedConfig, repoRoot };
    } catch {
      dryRunConfig = { ...defaultConfig, repoRoot };
    }
  }

  // Run dry-run update preview
  let allUpdates: PackageUpdate[] = [];
  let previewError = false;
  try {
    const previewSpinner = p.spinner();
    previewSpinner.start('Running dry-run dependency check for preview');
    const result = await runAllUpdaters(dryRunConfig, repoRoot, {
      ...options,
      dryRun: true,
      skipGit: true,
    });
    allUpdates = result.allUpdates;
    previewSpinner.stop(`Found ${allUpdates.length} outdated dependencies`);
  } catch (error) {
    previewError = true;
    config.logger?.warn(`Could not generate update preview: ${error instanceof Error ? error.message : String(error)}`);
  }

  // Reset working tree after dry-run to avoid committing lock file artifacts
  try {
    const { execa } = await import('execa');
    await (executor || (execa as unknown as CommandExecutor))('git', ['checkout', '.'], { cwd: repoRoot });
  } catch {
    // Ignore reset errors
  }

  // Switch back to base branch if we were on the onboarding branch
  if (existingPR) {
    try {
      const baseBranch = config.git?.baseBranch || 'main';
      await switchBranch(repoRoot, baseBranch, executor);
    } catch {
      // Ignore switch errors
    }
  }

  // Build PR body
  const prBody = previewError
    ? buildOnboardingPRBody(setup, configContent, []).replace(
        'No outdated dependencies detected.',
        'Could not generate update preview. The preview will appear once the configuration is finalized.',
      )
    : buildOnboardingPRBody(setup, configContent, allUpdates);

  // If existing PR, just update the body
  if (existingPR) {
    const editSpinner = p.spinner();
    editSpinner.start(`Updating onboarding PR #${existingPR.number}`);
    await client.editPR(repoRoot, existingPR.number, { body: prBody });
    editSpinner.stop(`Re-rendered onboarding PR #${existingPR.number}`);
    p.outro(`Onboarding PR updated: ${existingPR.url}`);
    return;
  }

  // Create new onboarding PR
  const baseBranch = config.git?.baseBranch || 'main';

  // Write config file
  const toolingDir = safeResolve(repoRoot, 'tooling');
  const configPath = safeResolve(toolingDir, 'patchnote.json');

  const writeSpinner = p.spinner();
  writeSpinner.start('Writing configuration file');
  await mkdir(toolingDir, { recursive: true });
  await writeFile(configPath, configContent, 'utf-8');
  writeSpinner.stop('Configuration written to tooling/patchnote.json');

  // Create branch, stage, commit, push
  const branchSpinner = p.spinner();
  branchSpinner.start('Creating onboarding branch');
  await createBranch(repoRoot, ONBOARD_BRANCH, baseBranch, executor);
  await stageFiles(repoRoot, ['tooling/patchnote.json'], executor);
  await commit(repoRoot, 'chore: add patchnote configuration', undefined, executor);
  await pushWithUpstream(repoRoot, 'origin', ONBOARD_BRANCH, executor);
  branchSpinner.stop(`Pushed branch ${ONBOARD_BRANCH}`);

  // Create PR
  const prSpinner = p.spinner();
  prSpinner.start('Creating onboarding PR');
  const pr = await client.createPR(repoRoot, {
    title: ONBOARD_PR_TITLE,
    body: prBody,
    head: ONBOARD_BRANCH,
    base: baseBranch,
  });
  prSpinner.stop(`Created PR #${pr.number}`);

  p.outro(`Onboarding PR created: ${pr.url}`);
}
