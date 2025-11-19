/**
 * Generate syncpack configuration command
 */

import type { DepUpdaterConfig } from '../config.js';
import { getCurrentExpoSDK } from '../expo/sdk-checker.js';
import { fetchExpoVersions } from '../expo/versions-fetcher.js';
import { getRepoRoot } from '../git.js';
import { updateSyncpackWithExpo } from '../syncpack/generator.js';
import type { UpdateOptions } from '../types.js';
import { safeResolve } from '../utils/path-validation.js';

interface GenerateSyncpackOptions extends UpdateOptions {
  expoSdkVersion?: string;
}

/**
 * Generate syncpack configuration from Expo SDK
 */
export async function generateSyncpack(config: DepUpdaterConfig, options: GenerateSyncpackOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());
  const packageJsonPath = safeResolve(repoRoot, config.expo?.packageJsonPath || './package.json');
  const syncpackPath = safeResolve(repoRoot, config.syncpack?.configPath || './.syncpackrc.json');

  let sdkVersion = options.expoSdkVersion;

  // If no version specified, get current from package.json
  if (!sdkVersion) {
    if (!config.expo?.enabled) {
      config.logger?.error('❌ Expo is not enabled and no SDK version specified');
      config.logger?.error('Usage: dep-updater generate-syncpack --expo-sdk 52');
      return;
    }

    const current = await getCurrentExpoSDK(packageJsonPath);
    if (!current) {
      config.logger?.error('❌ No Expo SDK found in package.json');
      config.logger?.error('Usage: dep-updater generate-syncpack --expo-sdk 52');
      return;
    }

    sdkVersion = current;
  }

  config.logger?.info(`Generating syncpack config for Expo SDK ${sdkVersion}...\n`);

  if (options.dryRun) {
    config.logger?.info('[DRY RUN] Would generate syncpack config for Expo SDK', sdkVersion);
    return;
  }

  // Fetch Expo versions
  config.logger?.info('Fetching Expo recommended versions...');
  const expoVersions = await fetchExpoVersions(sdkVersion);
  config.logger?.info(`✓ Fetched ${Object.keys(expoVersions.packages).length} package versions`);

  // Update syncpack config
  config.logger?.info('\nUpdating syncpack configuration...');
  await updateSyncpackWithExpo(
    expoVersions,
    syncpackPath,
    repoRoot,
    config.syncpack?.preserveCustomRules ?? true,
    config.logger,
  );

  config.logger?.info(`\n✓ Generated syncpack config at ${syncpackPath}`);
  config.logger?.info('\nNext steps:');
  config.logger?.info('  1. Review the generated config');
  config.logger?.info('  2. Run `bun run syncpack:fix` to align package versions');
  config.logger?.info('  3. Commit the changes');
}
