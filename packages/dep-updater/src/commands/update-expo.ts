/**
 * Update Expo SDK command
 */

import { generateCommitMessage } from '../changelog/analyzer.js';
import { type DepUpdaterConfig, resolveExpoProjects } from '../config.js';
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
export async function checkExpoSDK(config: DepUpdaterConfig, _options: UpdateOptions): Promise<void> {
  if (!config.expo?.enabled) {
    config.logger?.info('Expo SDK updates are disabled in config');
    return;
  }

  const repoRoot = config.repoRoot || (await getRepoRoot());
  const projects = await resolveExpoProjects(config);

  if (projects.length === 0) {
    config.logger?.warn('No Expo projects found to check');
    return;
  }

  config.logger?.info(`Checking for Expo SDK updates in ${projects.length} project(s)...\n`);

  let hasAnyUpdate = false;
  for (const project of projects) {
    const packageJsonPath = safeResolve(repoRoot, project.packageJsonPath);
    config.logger?.info(`📦 ${project.name || project.packageJsonPath}`);

    const { hasUpdate, current, latest } = await checkForExpoUpdate(packageJsonPath);

    if (!current) {
      config.logger?.warn('   !  No Expo SDK found');
      continue;
    }

    config.logger?.info(`   Current: ${current}`);
    config.logger?.info(`   Latest:  ${latest.version}`);

    if (hasUpdate) {
      config.logger?.info(`   ✨ Update available: ${current} → ${latest.version}`);
      if (latest.changelogUrl) {
        config.logger?.info(`   Changelog: ${latest.changelogUrl}`);
      }
      hasAnyUpdate = true;
    } else {
      config.logger?.info('   ✓ Up to date');
    }
    config.logger?.info('');
  }

  if (hasAnyUpdate) {
    config.logger?.info('💡 Run `dep-updater update-expo` to update all projects');
  }
}

/**
 * Update Expo SDK and dependencies for all projects
 */
export async function updateExpo(config: DepUpdaterConfig, options: UpdateOptions): Promise<void> {
  if (!config.expo?.enabled) {
    config.logger?.info('Expo SDK updates are disabled in config');
    return;
  }

  const repoRoot = config.repoRoot || (await getRepoRoot());
  const projects = await resolveExpoProjects(config);

  if (projects.length === 0) {
    config.logger?.warn('No Expo projects found to update');
    return;
  }

  config.logger?.info(`Checking for Expo SDK updates in ${projects.length} project(s)...\n`);

  // Check all projects and collect update info
  const projectUpdates: Array<{ project: (typeof projects)[0]; current: string; latest: string }> = [];
  let targetVersion: string | null = null;

  for (const project of projects) {
    const packageJsonPath = safeResolve(repoRoot, project.packageJsonPath);
    const { hasUpdate, current, latest } = await checkForExpoUpdate(packageJsonPath);

    if (!current) {
      config.logger?.warn(`!  ${project.name || project.packageJsonPath}: No Expo SDK found`);
      continue;
    }

    config.logger?.info(`📦 ${project.name || project.packageJsonPath}: ${current}`);

    if (hasUpdate) {
      projectUpdates.push({ project, current, latest: latest.version });
      targetVersion = latest.version; // All projects should update to the same latest version
    }
  }

  if (projectUpdates.length === 0 || !targetVersion) {
    config.logger?.info('\n✓ All Expo projects are up to date');
    return;
  }

  config.logger?.info(`\n✨ ${projectUpdates.length} project(s) can be updated to Expo SDK ${targetVersion}\n`);

  if (options.dryRun) {
    config.logger?.info('[DRY RUN] Would perform the following steps:');
    config.logger?.info('1. Fetch Expo recommended versions');
    config.logger?.info('2. Regenerate syncpack configuration');
    config.logger?.info('3. Update dependencies with Bun');
    config.logger?.info('4. Create git commit and PR');
    return;
  }

  // Step 1: Fetch Expo recommended versions (targetVersion is guaranteed to be non-null here)
  config.logger?.info('Fetching Expo recommended versions...');
  const expoVersions = await fetchExpoVersions(targetVersion);
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
  const projectList = projectUpdates
    .map((u) => `  - ${u.project.name || u.project.packageJsonPath}: ${u.current} → ${u.latest}`)
    .join('\n');
  const expoCommitTitle = `chore: update Expo SDK to ${targetVersion}`;
  const expoCommitBody = `Updated Expo SDK to ${targetVersion}

Projects updated:
${projectList}

${body}`;

  // Step 5: Create commit (if not skipping git)
  if (!options.skipGit) {
    await createUpdateCommit(config, expoCommitTitle, expoCommitBody);

    // Step 6: Create branch and push
    const branchName = `${config.prStrategy.branchPrefix}-expo-${targetVersion}`;
    await createUpdateBranch(config, branchName);

    // Step 7: Create PR
    const prTitle = `chore: update Expo SDK to ${targetVersion}`;
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
