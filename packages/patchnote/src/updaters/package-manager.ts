/**
 * Package manager abstraction layer
 *
 * Provides a unified interface for package manager commands across
 * bun, npm, pnpm, and yarn.
 */

import type { ProjectSetup } from '../types.js';

/**
 * Unified interface for package manager commands
 */
export interface PackageManagerCommands {
  /** The executable name (bun, npm, pnpm, yarn) */
  cmd: string;
  /** Base args for update command (e.g. ['update'] for bun/npm/pnpm, ['upgrade'] for yarn) */
  updateArgs: string[];
  /** Flag to add for recursive workspace updates, or undefined if PM is workspace-aware by default */
  recursiveFlag: string | undefined;
  /** Args for basic install (sync lock file) */
  installArgs: string[];
  /** Args for force-refresh of lock file (re-resolve all transitive deps) */
  forceRefreshArgs: string[];
  /** Lock file name(s) to stage for git (e.g. ['bun.lock', 'bun.lockb']) */
  lockFileNames: string[];
  /** Generate args to run a package.json script (e.g. ['run', scriptName]) */
  runScriptArgs: (scriptName: string) => string[];
  /** Args for checking outdated packages */
  outdatedArgs: string[];
}

/**
 * Get the correct package manager commands for the detected package manager
 */
export function getPackageManagerCommands(pm: ProjectSetup['packageManager']): PackageManagerCommands {
  switch (pm) {
    case 'bun':
      return {
        cmd: 'bun',
        updateArgs: ['update'],
        recursiveFlag: '--recursive',
        installArgs: ['install'],
        forceRefreshArgs: ['install', '--force'],
        lockFileNames: ['bun.lock', 'bun.lockb'],
        runScriptArgs: (name) => ['run', name],
        outdatedArgs: ['outdated'],
      };
    case 'npm':
      return {
        cmd: 'npm',
        updateArgs: ['update'],
        recursiveFlag: undefined,
        installArgs: ['install'],
        forceRefreshArgs: ['install', '--package-lock-only'],
        lockFileNames: ['package-lock.json'],
        runScriptArgs: (name) => ['run', name],
        outdatedArgs: ['outdated', '--json'],
      };
    case 'pnpm':
      return {
        cmd: 'pnpm',
        updateArgs: ['update'],
        recursiveFlag: '--recursive',
        installArgs: ['install'],
        forceRefreshArgs: ['install', '--force'],
        lockFileNames: ['pnpm-lock.yaml'],
        runScriptArgs: (name) => ['run', name],
        outdatedArgs: ['outdated', '--format', 'json'],
      };
    case 'yarn':
      return {
        cmd: 'yarn',
        updateArgs: ['upgrade'],
        recursiveFlag: undefined,
        installArgs: ['install'],
        forceRefreshArgs: ['install', '--force'],
        lockFileNames: ['yarn.lock'],
        runScriptArgs: (name) => ['run', name],
        outdatedArgs: ['outdated', '--json'],
      };
    default: {
      const _exhaustive: never = pm;
      throw new Error(`Unsupported package manager: ${_exhaustive}`);
    }
  }
}
