/**
 * Shared types for dep-updater
 */

import type { Options } from 'execa';

/**
 * Simplified result interface for command execution
 * Contains only the fields we actually use from execa's Result type
 */
export interface ExecutorResult {
  /** Standard output */
  stdout: string;
  /** Standard error */
  stderr: string;
  /** Exit code */
  exitCode: number;
}

/**
 * Deep partial type - makes all properties and nested properties optional
 * Useful for partial configuration updates
 */
export type DeepPartial<T> = T extends object
  ? {
      [P in keyof T]?: DeepPartial<T[P]>;
    }
  : T;

/**
 * Update type classification
 */
export type UpdateType = 'major' | 'minor' | 'patch' | 'unknown';

/**
 * Dependency ecosystem
 */
export type DependencyEcosystem = 'npm' | 'nix' | 'nixpkgs' | 'expo';

/**
 * Information about a single package update
 */
export interface PackageUpdate {
  /** Package name */
  name: string;
  /** Version before update */
  fromVersion: string;
  /** Version after update */
  toVersion: string;
  /** Update type (major/minor/patch) */
  updateType: UpdateType;
  /** Ecosystem this package belongs to */
  ecosystem: DependencyEcosystem;
  /** Changelog URL or text */
  changelog?: string;
  /** Breaking changes detected */
  breakingChanges?: string[];
}

/**
 * Result of a dependency update operation
 */
export interface UpdateResult {
  /** Whether the update was successful */
  success: boolean;
  /** List of package updates */
  updates: PackageUpdate[];
  /** Error message if failed */
  error?: string;
  /** Ecosystem that was updated */
  ecosystem: DependencyEcosystem;
}

/**
 * Information about an open PR
 */
export interface OpenPR {
  /** PR number */
  number: number;
  /** PR title */
  title: string;
  /** Branch name */
  branch: string;
  /** Creation date */
  createdAt: Date;
  /** Whether PR has merge conflicts */
  hasConflicts: boolean;
  /** PR URL */
  url: string;
}

/**
 * Expo SDK version information
 */
export interface ExpoSDKVersion {
  /** SDK version number (e.g., "52.0.0") */
  version: string;
  /** Whether this is the latest stable version */
  isLatest: boolean;
  /** Changelog URL */
  changelogUrl?: string;
}

/**
 * Expo recommended package versions for an SDK
 */
export interface ExpoPackageVersions {
  /** SDK version these recommendations are for */
  sdkVersion: string;
  /** Map of package name to version */
  packages: Record<string, string>;
}

/**
 * Options for update commands
 */
export interface UpdateOptions {
  /** Dry run mode - don't make actual changes */
  dryRun: boolean;
  /** Skip git operations */
  skipGit: boolean;
  /** Skip AI changelog analysis */
  skipAI: boolean;
}

/**
 * Options for generate-workflow command
 */
export interface GenerateWorkflowOptions extends UpdateOptions {
  /** Cron schedule for workflow */
  schedule?: string;
  /** Name of the workflow */
  workflowName?: string;
}

/**
 * Options for init command
 */
export interface InitOptions extends UpdateOptions {
  /** Skip prompts and use defaults */
  yes?: boolean;
}

/**
 * Type for command executor (for testing git operations)
 * Matches the array-long form: execa(file, args, options)
 *
 * Uses simplified ExecutorResult instead of full execa Result to avoid
 * requiring mocks to implement all 15+ fields when we only use stdout/stderr/exitCode.
 * The real execa function returns a superset of this interface.
 */
export type CommandExecutor = (
  file: string | URL,
  args?: readonly string[],
  options?: Options,
) => Promise<ExecutorResult>;
