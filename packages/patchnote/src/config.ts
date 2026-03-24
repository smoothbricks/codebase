/**
 * Configuration for the patchnote tool
 */

import { existsSync, realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { getRepoRoot } from './git.js';
import { ConsoleLogger, type Logger, LogLevel } from './logger.js';
import { mergePartials, resolvePresets } from './presets.js';
import type {
  DeepPartial,
  ExpoProject,
  FilterConfig,
  GroupingConfig,
  MergeStrategy,
  PackageRule,
  SupportedProvider,
} from './types.js';
import { safeResolve, validatePathWithinBase } from './utils/path-validation.js';
import { detectExpoProjects } from './utils/workspace-detector.js';

export interface PatchnoteConfig {
  /** Expo SDK management configuration */
  expo?: {
    /** Enable Expo SDK updates */
    enabled: boolean;
    /** Auto-detect all Expo projects in the monorepo */
    autoDetect?: boolean;
    /** Explicit list of Expo projects to update */
    projects?: ExpoProject[];
  };

  /** Syncpack configuration */
  syncpack?: {
    /** Path to .syncpackrc.json */
    configPath: string;
    /**
     * Preserve custom syncpack rules when regenerating from Expo.
     *
     * When true, the tool filters out managed rules and keeps custom ones:
     * - **Managed (removed)**: Rules with labels containing "Expo SDK" or "workspace protocol",
     *   OR rules with dependencies including `react`, `react-native`, or `expo`
     * - **Custom (preserved)**: All other rules (e.g., pinning lodash, TypeScript version constraints)
     *
     * The preserved custom rules are merged with newly generated Expo rules.
     */
    preserveCustomRules: boolean;
    /** Script name to run syncpack fix (default: 'syncpack:fix') */
    fixScriptName: string;
  };

  /** Nix/devenv configuration */
  nix?: {
    /** Enable Nix ecosystem updates (devenv + nixpkgs overlay) */
    enabled: boolean;
    /** Path to devenv directory (default: './tooling/direnv') */
    devenvPath: string;
    /** Path to nixpkgs overlay directory (default: './tooling/direnv/nixpkgs-overlay') */
    nixpkgsOverlayPath: string;
  };

  /** Stacked PR strategy configuration */
  prStrategy: {
    /** Enable PR stacking (base new PRs on previous update PRs) */
    stackingEnabled: boolean;
    /** Maximum number of stacked PRs to keep open */
    maxStackDepth: number;
    /** Auto-close PRs older than maxStackDepth */
    autoCloseOldPRs: boolean;
    /** Reset stack (base on main) after any PR is merged */
    resetOnMerge: boolean;
    /** Don't create new PR if base branch has conflicts */
    stopOnConflicts: boolean;
    /** Branch name prefix for update PRs */
    branchPrefix: string;
    /** PR title prefix */
    prTitlePrefix: string;
    /** Labels to add to created PRs */
    labels?: string[];
    /** Users to assign to created PRs */
    assignees?: string[];
    /** Users to request review from */
    reviewers?: string[];
    /** Create PRs as draft */
    draft?: boolean;
  };

  /**
   * Auto-merge configuration
   * When enabled, uses `gh pr merge --auto` to let GitHub merge PRs
   * once branch protection requirements (status checks, reviews) are met.
   */
  autoMerge: {
    /** Enable auto-merge functionality */
    enabled: boolean;
    /** Auto-merge mode: none, patch only, or minor+patch */
    mode: 'none' | 'patch' | 'minor';
    /** Merge strategy to use (squash, rebase, or merge commit) */
    strategy: MergeStrategy;
    /** Require all tests to pass before auto-merge */
    requireTests: boolean;
    /** Minimum test coverage percentage (future use) */
    minCoverage?: number;
  };

  /** npm provenance downgrade detection */
  provenanceCheck?: {
    /** Behavior when provenance downgrade is detected.
     * - 'block': Add PR warning AND block auto-merge (default)
     * - 'warn': Add PR warning only, do not block auto-merge
     * - 'skip': Disable provenance checking entirely
     */
    mode: 'block' | 'warn' | 'skip';
  };

  /** Deprecated package detection */
  deprecationCheck?: {
    /** Behavior: 'warn' (PR warning), 'skip' (disable). Default: 'warn'. */
    mode: 'warn' | 'skip';
  };

  /** AI-powered changelog analysis */
  ai: {
    /** AI provider for changelog analysis */
    provider: SupportedProvider;
    /** Provider-specific model ID */
    model?: string;
    /** API key (falls back to env vars: ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.) */
    apiKey?: string;
    /**
     * Token budget for changelog analysis prompts.
     * When changelogs exceed this limit, they're summarized to fit.
     * Default: 64k (GLM-5-Turbo has 128k context window)
     */
    tokenBudget?: number;
  };

  /** Git configuration */
  git?: {
    /** Remote name (default: 'origin') */
    remote: string;
    /** Base branch (default: 'main') */
    baseBranch: string;
  };

  /** Semantic commit message configuration */
  semanticCommits?: {
    /** Enable semantic commit prefixes: true, false, or 'auto' (detect from repo history) */
    enabled: boolean | 'auto';
    /** Commit type for production dependency updates (default: 'chore(deps)') */
    prefix: string;
    /** Commit type for dev dependency updates (default: 'chore(dev-deps)') */
    devPrefix: string;
  };

  /**
   * Dependency filtering configuration.
   * Exclude takes precedence over include when both match a package.
   */
  filters?: FilterConfig;

  /**
   * Per-package update rules. Evaluated in order; later rules override earlier ones.
   * Runs after exclude/include filters for fine-grained per-package control.
   * @example
   * packageRules: [
   *   { match: '@types/*', automerge: true },
   *   { match: 'react', allowedVersions: '^18' },
   *   { match: '*', updateTypes: ['patch'], automerge: true },
   * ]
   */
  packageRules?: PackageRule[];

  /**
   * Update grouping/batching configuration.
   * When set, dependency updates are partitioned into separate PRs by semver level
   * and/or package name pattern. When undefined, all updates go into a single PR.
   */
  grouping?: GroupingConfig;

  /** Lock file maintenance configuration */
  lockFileMaintenance?: {
    /** Enable lock file maintenance mode */
    enabled: boolean;
    /** Branch prefix for lock file maintenance PRs */
    branchPrefix: string;
  };

  /** Repository root path */
  repoRoot?: string;

  /** Logger instance (not serialized in config files) */
  logger?: Logger;
}

/**
 * Input type for defineConfig and config files.
 * Extends DeepPartial<PatchnoteConfig> with an optional `extends` field
 * for referencing shareable preset configurations.
 *
 * The `extends` field is consumed at load time and does NOT appear
 * on the resolved PatchnoteConfig.
 */
export interface PatchnoteConfigInput extends DeepPartial<PatchnoteConfig> {
  /** Array of preset references to extend from (e.g., 'patchnote:recommended', '@myorg/patchnote-config') */
  extends?: string[];
}

/**
 * Default configuration
 */
export const defaultConfig: PatchnoteConfig = {
  expo: {
    enabled: false,
    autoDetect: true, // Auto-detect Expo projects by default
    projects: [],
  },
  syncpack: {
    configPath: './.syncpackrc.json',
    preserveCustomRules: true,
    fixScriptName: 'syncpack:fix',
  },
  nix: {
    enabled: false,
    devenvPath: './tooling/direnv',
    nixpkgsOverlayPath: './tooling/direnv/nixpkgs-overlay',
  },
  prStrategy: {
    stackingEnabled: true,
    maxStackDepth: 5,
    autoCloseOldPRs: true,
    resetOnMerge: true,
    stopOnConflicts: true,
    branchPrefix: 'chore/update-deps',
    prTitlePrefix: 'chore: update dependencies',
  },
  autoMerge: {
    enabled: false,
    mode: 'none',
    strategy: 'squash',
    requireTests: true,
  },
  provenanceCheck: {
    mode: 'block',
  },
  deprecationCheck: {
    mode: 'warn',
  },
  ai: {
    provider: 'zai',
    model: 'glm-5-turbo',
  },
  git: {
    remote: 'origin',
    baseBranch: 'main',
  },
  semanticCommits: {
    enabled: 'auto',
    prefix: 'chore(deps)',
    devPrefix: 'chore(dev-deps)',
  },
  filters: {
    exclude: [],
    include: [],
  },
  lockFileMaintenance: {
    enabled: false,
    branchPrefix: 'chore/lock-file-maintenance',
  },
  logger: new ConsoleLogger(LogLevel.INFO),
};

/**
 * Config file names to search for (in order of preference)
 * TypeScript configs (.ts) have priority over JSON
 */
const CONFIG_FILE_NAMES = ['tooling/patchnote.ts', 'tooling/patchnote.json'];

/**
 * Load configuration from file or return defaults
 *
 * Searches for config files in the current working directory and its parent directories
 * up to the git repository root.
 *
 * @param searchPath - Optional path to start searching from (defaults to process.cwd())
 * @returns Merged configuration with defaults
 *
 * @example
 * ```typescript
 * const config = await loadConfig();
 * console.log('Max stack depth:', config.prStrategy.maxStackDepth);
 * ```
 */
export async function loadConfig(searchPath?: string, explicitConfigPath?: string): Promise<PatchnoteConfig> {
  const startPath = searchPath || process.cwd();

  const configPath = explicitConfigPath
    ? await (async () => {
        let repoRoot = startPath;

        try {
          const gitRoot = await getRepoRoot(startPath);
          // Sanity check: startPath must be within gitRoot, otherwise the result
          // is unreliable (e.g. mocked in tests or stale git state)
          const realStart = existsSync(startPath) ? realpathSync(startPath) : startPath;
          const realGitRoot = existsSync(gitRoot) ? realpathSync(gitRoot) : gitRoot;
          if (realStart.startsWith(realGitRoot)) {
            repoRoot = realGitRoot;
          }
        } catch {
          // Fallback for non-git contexts such as isolated config tests.
        }

        if (isAbsolute(explicitConfigPath)) {
          const realConfig = existsSync(explicitConfigPath) ? realpathSync(explicitConfigPath) : explicitConfigPath;
          validatePathWithinBase(repoRoot, realConfig);
          return explicitConfigPath;
        }

        return safeResolve(repoRoot, explicitConfigPath);
      })()
    : await findConfigFile(startPath);

  if (!configPath) {
    // No config file found, return defaults
    return { ...defaultConfig };
  }

  try {
    let userConfig: DeepPartial<PatchnoteConfig>;

    // Load TypeScript config using dynamic import
    if (configPath.endsWith('.ts')) {
      const module = await import(pathToFileURL(configPath).href);
      userConfig = module.default || module;
    } else {
      // Load JSON config
      const fileContent = await readFile(configPath, 'utf-8');
      userConfig = JSON.parse(fileContent) as DeepPartial<PatchnoteConfig>;
    }

    // Validate basic structure
    if (typeof userConfig !== 'object' || userConfig === null || Array.isArray(userConfig)) {
      console.warn(
        `Invalid config file at ${configPath}: expected object, got ${Array.isArray(userConfig) ? 'array' : typeof userConfig}`,
      );
      return { ...defaultConfig };
    }

    // Extract extends field (not part of PatchnoteConfig)
    const { extends: presetRefs, ...localConfig } = userConfig as PatchnoteConfigInput;

    if (presetRefs && presetRefs.length > 0) {
      const resolvedPresets = await resolvePresets(presetRefs);
      const mergedWithPresets = mergePartials(resolvedPresets, localConfig);
      return mergeConfig(mergedWithPresets);
    }

    return mergeConfig(localConfig);
  } catch (error) {
    if (error instanceof SyntaxError) {
      console.warn(`Failed to parse config file at ${configPath}: ${error.message}`);
    } else {
      console.warn(
        `Failed to load config file at ${configPath}:`,
        error instanceof Error ? error.message : String(error),
      );
    }
    // Return defaults on error
    return { ...defaultConfig };
  }
}

/**
 * Find config file by searching up directory tree
 *
 * @param startPath - Path to start searching from
 * @returns Path to config file, or null if not found
 */
export async function findConfigFile(startPath: string): Promise<string | null> {
  let currentPath = startPath;
  const root = '/'; // Unix root; on Windows we'll hit the drive root

  // Search up to 10 levels to avoid infinite loops
  for (let i = 0; i < 10; i++) {
    // Try each config file name
    for (const fileName of CONFIG_FILE_NAMES) {
      const configPath = join(currentPath, fileName);
      if (existsSync(configPath)) {
        return configPath;
      }
    }

    // Move up one directory
    const parentPath = join(currentPath, '..');

    // Stop if we've reached the root or can't go up further
    if (parentPath === currentPath || currentPath === root) {
      break;
    }

    currentPath = parentPath;
  }

  return null;
}

/**
 * Merge user config with defaults
 */
export function mergeConfig(userConfig: DeepPartial<PatchnoteConfig>): PatchnoteConfig {
  return {
    ...defaultConfig,
    ...userConfig,
    expo: userConfig.expo ? { ...defaultConfig.expo, ...userConfig.expo } : defaultConfig.expo,
    syncpack: userConfig.syncpack ? { ...defaultConfig.syncpack, ...userConfig.syncpack } : defaultConfig.syncpack,
    nix: userConfig.nix ? { ...defaultConfig.nix, ...userConfig.nix } : defaultConfig.nix,
    prStrategy: { ...defaultConfig.prStrategy, ...(userConfig.prStrategy || {}) },
    autoMerge: { ...defaultConfig.autoMerge, ...(userConfig.autoMerge || {}) },
    provenanceCheck: userConfig.provenanceCheck
      ? { ...defaultConfig.provenanceCheck, ...userConfig.provenanceCheck }
      : defaultConfig.provenanceCheck,
    deprecationCheck: userConfig.deprecationCheck
      ? { ...defaultConfig.deprecationCheck, ...userConfig.deprecationCheck }
      : defaultConfig.deprecationCheck,
    ai: { ...defaultConfig.ai, ...(userConfig.ai || {}) },
    git: userConfig.git ? { ...defaultConfig.git, ...userConfig.git } : defaultConfig.git,
    semanticCommits: userConfig.semanticCommits
      ? { ...defaultConfig.semanticCommits, ...userConfig.semanticCommits }
      : defaultConfig.semanticCommits,
    filters: userConfig.filters ? { ...defaultConfig.filters, ...userConfig.filters } : defaultConfig.filters,
    grouping: userConfig.grouping ?? defaultConfig.grouping,
    packageRules: userConfig.packageRules ?? defaultConfig.packageRules,
    lockFileMaintenance: userConfig.lockFileMaintenance
      ? { ...defaultConfig.lockFileMaintenance, ...userConfig.lockFileMaintenance }
      : defaultConfig.lockFileMaintenance,
    // Logger is runtime-only, always use default logger (can be overridden later)
    logger: defaultConfig.logger,
  } as PatchnoteConfig;
}

/**
 * Sanitize configuration for safe logging
 * Removes sensitive information like API keys
 *
 * @param config - The configuration to sanitize
 * @returns Sanitized configuration safe for logging
 *
 * @example
 * ```typescript
 * const config = loadConfig();
 * console.log('Config:', sanitizeConfigForLogging(config));
 * // API key will be shown as '***REDACTED***'
 * ```
 */
export function sanitizeConfigForLogging(config: PatchnoteConfig): Partial<PatchnoteConfig> {
  return {
    ...config,
    ai: {
      ...config.ai,
      apiKey: config.ai.apiKey ? '***REDACTED***' : undefined,
    },
  };
}

/**
 * Resolve Expo projects from config
 * Handles auto-detection and explicit projects list
 *
 * @param config - The configuration containing expo settings
 * @returns Array of Expo projects to update
 *
 * @example
 * ```typescript
 * const config = await loadConfig();
 * const projects = await resolveExpoProjects(config);
 * for (const project of projects) {
 *   console.log(`Updating ${project.name} at ${project.packageJsonPath}`);
 * }
 * ```
 */
export async function resolveExpoProjects(config: PatchnoteConfig): Promise<ExpoProject[]> {
  if (!config.expo?.enabled) {
    return [];
  }

  const repoRoot = config.repoRoot || (await getRepoRoot());

  // Explicit projects list
  if (config.expo.projects && config.expo.projects.length > 0) {
    config.logger?.debug(`Using ${config.expo.projects.length} explicit project(s) from config`);
    return config.expo.projects;
  }

  // Auto-detect
  if (config.expo.autoDetect !== false) {
    config.logger?.debug('Auto-detecting Expo projects in monorepo...');
    const detected = await detectExpoProjects(repoRoot);
    config.logger?.debug(`Found ${detected.length} Expo project(s)`);
    return detected;
  }

  config.logger?.warn('No Expo projects configured and auto-detection is disabled');
  return [];
}

/**
 * Helper function for TypeScript config files
 * Provides type safety and autocomplete for config options
 *
 * @param config - Partial configuration object
 * @returns The same configuration object (for type inference)
 *
 * @example
 * ```typescript
 * // tooling/patchnote.ts
 * import { defineConfig } from '@smoothbricks/patchnote';
 *
 * export default defineConfig({
 *   expo: { enabled: true },
 *   prStrategy: {
 *     stackingEnabled: true,
 *     maxStackDepth: 3
 *   }
 * });
 * ```
 */
export function defineConfig(config: PatchnoteConfigInput): PatchnoteConfigInput {
  return config;
}

/**
 * Script mode function signature for executable config files
 * Allows custom update logic instead of declarative config
 *
 * @example
 * ```typescript
 * // tooling/patchnote.ts
 * import { updateBunDeps, type ConfigScript } from '@smoothbricks/patchnote';
 *
 * export default async function() {
 *   const updates = await updateBunDeps();
 *
 *   // Custom logic - skip React 19
 *   const filtered = updates.filter(u =>
 *     !(u.name === 'react' && u.toVersion.startsWith('19'))
 *   );
 *
 *   // Apply updates
 *   for (const update of filtered) {
 *     console.log(`Updating ${update.name}...`);
 *   }
 * }
 * ```
 */
export type ConfigScript = () => Promise<void> | void;

/**
 * Check if a config file path points to an executable script
 *
 * @param configPath - Path to config file
 * @returns True if the config exports a function (script mode)
 */
export async function isConfigScript(configPath: string): Promise<boolean> {
  if (!configPath.endsWith('.ts')) {
    return false;
  }

  try {
    const module = await import(pathToFileURL(configPath).href);
    return typeof module.default === 'function';
  } catch {
    return false;
  }
}

/**
 * Execute a config script file
 *
 * @param configPath - Path to the script file
 * @throws If script execution fails
 */
export async function executeConfigScript(configPath: string): Promise<void> {
  const module = await import(pathToFileURL(configPath).href);

  if (typeof module.default !== 'function') {
    throw new Error(`Config file at ${configPath} does not export a function`);
  }

  await module.default();
}
