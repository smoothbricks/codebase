/**
 * Generate syncpack configuration from Expo SDK versions
 */

import { readFile, writeFile } from 'node:fs/promises';
import { filterCriticalVersions } from '../expo/versions-fetcher.js';
import type { ExpoPackageVersions } from '../types.js';
import { detectWorkspaceScopes } from '../utils/workspace-detector.js';

interface SyncpackConfig {
  versionGroups?: Array<{
    label?: string;
    dependencies?: string[];
    dependencyTypes?: string[];
    packages?: string[];
    pinVersion?: string;
    policy?: string;
  }>;
  semverGroups?: Array<{
    label?: string;
    dependencies?: string[];
    range?: string;
    packages?: string[];
  }>;
  [key: string]: unknown;
}

/**
 * Read existing syncpack configuration
 */
export async function readSyncpackConfig(configPath: string): Promise<SyncpackConfig> {
  try {
    const content = await readFile(configPath, 'utf-8');
    return JSON.parse(content);
  } catch (error) {
    // ENOENT is expected if file doesn't exist yet
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return {};
    }
    // Other errors should be logged
    console.warn(
      `Failed to read syncpack config from ${configPath}:`,
      error instanceof Error ? error.message : String(error),
    );
    return {};
  }
}

/**
 * Generate version groups for Expo packages
 */
export function generateExpoVersionGroups(expoVersions: ExpoPackageVersions): SyncpackConfig['versionGroups'] {
  const criticalVersions = filterCriticalVersions(expoVersions);
  const groups: NonNullable<SyncpackConfig['versionGroups']> = [];

  // Create version group for each critical package
  for (const [packageName, version] of Object.entries(criticalVersions)) {
    groups.push({
      label: `Pin ${packageName} to Expo SDK ${expoVersions.sdkVersion}`,
      dependencies: [packageName],
      dependencyTypes: ['prod', 'dev', 'peer'],
      packages: ['**'],
      pinVersion: version,
    });
  }

  return groups;
}

/**
 * Generate workspace dependency rules for multiple scopes
 */
export function generateWorkspaceRules(workspaceScopes: string[]): SyncpackConfig['versionGroups'] {
  if (workspaceScopes.length === 0) {
    return [];
  }

  // Generate glob patterns for all scopes
  const dependencies = workspaceScopes.map((scope) => `${scope}/*`);
  const scopeList = workspaceScopes.join(', ');

  return [
    {
      label: `Use workspace protocol for ${scopeList} packages`,
      dependencies,
      dependencyTypes: ['prod', 'dev', 'peer'],
      packages: ['**'],
      pinVersion: 'workspace:*',
    },
  ];
}

/**
 * Merge new version groups with existing ones, preserving custom rules
 */
export function mergeSyncpackConfig(
  existing: SyncpackConfig,
  expoGroups: SyncpackConfig['versionGroups'],
  preserveCustom: boolean,
): SyncpackConfig {
  const merged: SyncpackConfig = { ...existing };

  if (!preserveCustom) {
    // Replace all version groups with new ones
    merged.versionGroups = expoGroups;
    return merged;
  }

  // Preserve custom rules by removing only Expo-related groups
  const existingGroups = existing.versionGroups || [];
  const customGroups = existingGroups.filter(
    (group) =>
      !group.label?.includes('Expo SDK') &&
      !group.label?.includes('workspace protocol') &&
      !group.dependencies?.some((dep) => ['react', 'react-native', 'expo'].includes(dep)),
  );

  // Combine custom groups with new Expo groups
  merged.versionGroups = [...(expoGroups ?? []), ...(customGroups ?? [])];

  return merged;
}

/**
 * Generate complete syncpack config from Expo versions
 */
export async function generateSyncpackConfig(
  expoVersions: ExpoPackageVersions,
  configPath: string,
  options: {
    preserveCustomRules?: boolean;
    workspaceScopes?: string[];
  } = {},
): Promise<SyncpackConfig> {
  const { preserveCustomRules = true, workspaceScopes = [] } = options;

  // Read existing config
  const existing = await readSyncpackConfig(configPath);

  // Generate Expo version groups
  const expoGroups = generateExpoVersionGroups(expoVersions);

  // Add workspace rules if scopes are provided
  let allGroups = expoGroups;
  if (workspaceScopes.length > 0) {
    const workspaceRules = generateWorkspaceRules(workspaceScopes);
    allGroups = [...(expoGroups ?? []), ...(workspaceRules ?? [])];
  }

  // Merge with existing config
  const merged = mergeSyncpackConfig(existing, allGroups, preserveCustomRules);

  return merged;
}

/**
 * Write syncpack configuration to file
 */
export async function writeSyncpackConfig(config: SyncpackConfig, configPath: string): Promise<void> {
  const content = `${JSON.stringify(config, null, 2)}\n`;
  await writeFile(configPath, content, 'utf-8');
}

/**
 * Update syncpack config with Expo versions
 */
export async function updateSyncpackWithExpo(
  expoVersions: ExpoPackageVersions,
  configPath: string,
  repoRoot: string,
  preserveCustom = true,
  logger?: import('../logger.js').Logger,
): Promise<void> {
  // Auto-detect workspace scopes from package.json
  const workspaceScopes = await detectWorkspaceScopes(repoRoot);

  if (workspaceScopes.length > 0) {
    logger?.info(`✓ Detected workspace scopes: ${workspaceScopes.join(', ')}`);
  }

  const config = await generateSyncpackConfig(expoVersions, configPath, {
    preserveCustomRules: preserveCustom,
    workspaceScopes,
  });

  await writeSyncpackConfig(config, configPath);
  logger?.info(`✓ Updated syncpack config for Expo SDK ${expoVersions.sdkVersion}`);
}
