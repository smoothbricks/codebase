/**
 * Update Expo SDK command
 */

import { generateCommitMessage } from '../changelog/analyzer.js';
import type { DepUpdaterConfig } from '../config.js';
import { checkForExpoUpdate } from '../expo/sdk-checker.js';
import { fetchExpoVersions } from '../expo/versions-fetcher.js';
import { createUpdateBranch, createUpdateCommit, getRepoRoot } from '../git.js';
import { createStackedPR } from '../pr/stacking.js';
import { updateSyncpackWithExpo } from '../syncpack/generator.js';
import type { UpdateOptions } from '../types.js';
import { updateBunDependencies } from '../updaters/bun.js';
import { safeResolve } from '../utils/path-validation.js';

/**
 * Check if new Expo SDK is available
 */
export async function checkExpoSDK(config: DepUpdaterConfig, options: UpdateOptions): Promise<void> {
  if (!config.expo?.enabled) {
    config.logger?.info('Expo SDK updates are disabled in config');
    return;
  }

  const repoRoot = config.repoRoot || (await getRepoRoot());
  const packageJsonPath = safeResolve(repoRoot, config.expo.packageJsonPath);

  config.logger?.info('Checking for Expo SDK updates...\n');

  const { hasUpdate, current, latest } = await checkForExpoUpdate(packageJsonPath);

  if (!current) {
    config.logger?.error('❌ No Expo SDK found in package.json');
    return;
  }

  config.logger?.info(`Current Expo SDK: ${current}`);
  config.logger?.info(`Latest Expo SDK: ${latest.version}`);

  if (hasUpdate) {
    config.logger?.info(`\n✨ New Expo SDK available: ${current} → ${latest.version}`);
    if (latest.changelogUrl) {
      config.logger?.info(`Changelog: ${latest.changelogUrl}`);
    }
  } else {
    config.logger?.info('\n✓ Already on latest Expo SDK');
  }
}

/**
 * Update Expo SDK and dependencies
 */
export async function updateExpo(config: DepUpdaterConfig, options: UpdateOptions): Promise<void> {
  if (!config.expo?.enabled) {
    config.logger?.info('Expo SDK updates are disabled in config');
    return;
  }

  const repoRoot = config.repoRoot || (await getRepoRoot());
  const packageJsonPath = safeResolve(repoRoot, config.expo.packageJsonPath);

  config.logger?.info('Checking for Expo SDK updates...\n');

  const { hasUpdate, current, latest } = await checkForExpoUpdate(packageJsonPath);

  if (!current) {
    config.logger?.error('❌ No Expo SDK found in package.json');
    return;
  }

  if (!hasUpdate) {
    config.logger?.info('✓ Already on latest Expo SDK');
    return;
  }

  config.logger?.info(`Updating Expo SDK: ${current} → ${latest.version}\n`);

  if (options.dryRun) {
    config.logger?.info('[DRY RUN] Would perform the following steps:');
    config.logger?.info('1. Fetch Expo recommended versions');
    config.logger?.info('2. Regenerate syncpack configuration');
    config.logger?.info('3. Update dependencies with Bun');
    config.logger?.info('4. Create git commit and PR');
    return;
  }

  // Step 1: Fetch Expo recommended versions
  config.logger?.info('Fetching Expo recommended versions...');
  const expoVersions = await fetchExpoVersions(latest.version);
  config.logger?.info(`✓ Fetched ${Object.keys(expoVersions.packages).length} package versions`);

  // Step 2: Regenerate syncpack config
  config.logger?.info('\nRegenerating syncpack configuration...');
  const syncpackPath = safeResolve(repoRoot, config.syncpack?.configPath || '.syncpackrc.json');
  await updateSyncpackWithExpo(
    expoVersions,
    syncpackPath,
    repoRoot,
    config.syncpack?.preserveCustomRules ?? true,
    config.logger,
  );

  // Step 3: Update dependencies
  config.logger?.info('\nUpdating dependencies...');
  const updateResult = await updateBunDependencies(repoRoot, {
    dryRun: false,
    recursive: true,
    logger: config.logger,
  });

  if (!updateResult.success) {
    config.logger?.error('❌ Failed to update dependencies:', updateResult.error);
    return;
  }

  config.logger?.info(`✓ Updated ${updateResult.updates.length} packages`);

  // Step 4: Generate commit message
  const { body } = await generateCommitMessage(updateResult.updates, config);
  const expoCommitTitle = `chore: update Expo SDK to ${latest.version}`;
  const expoCommitBody = `Updated Expo SDK from ${current} to ${latest.version}

Changelog: ${latest.changelogUrl || 'N/A'}

${body}`;

  // Step 5: Create commit (if not skipping git)
  if (!options.skipGit) {
    await createUpdateCommit(config, expoCommitTitle, expoCommitBody);

    // Step 6: Create branch and push
    const branchName = `${config.prStrategy.branchPrefix}-expo-${latest.version}`;
    await createUpdateBranch(config, branchName);

    // Step 7: Create PR
    const prTitle = `chore: update Expo SDK to ${latest.version}`;
    const prBody = expoCommitBody;

    const pr = await createStackedPR(config, repoRoot, {
      title: prTitle,
      body: prBody,
      headBranch: branchName,
    });

    config.logger?.info(`\n✓ Created PR #${pr.number}: ${pr.url}`);
  }

  config.logger?.info('\n✓ Expo SDK update complete!');
}
