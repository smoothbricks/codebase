/**
 * dep-updater
 *
 * Automated dependency update tool with:
 * - Expo SDK support
 * - Stacked PR strategy
 * - AI-powered changelog analysis
 * - Multiple dependency ecosystems (npm, Nix, nixpkgs)
 */

// Export changelog utilities
export * from './changelog/analyzer.js';
export * from './changelog/fetcher.js';
// Export commands
export * from './commands/generate-syncpack.js';
export * from './commands/update-deps.js';
export * from './commands/update-expo.js';
// Export configuration
export * from './config.js';
// Export Expo utilities
export * from './expo/sdk-checker.js';
export * from './expo/versions-fetcher.js';
// Export Git utilities
export * from './git.js';
// Export logging utilities
export * from './logger.js';
// Export PR utilities
export * from './pr/stacking.js';
// Export syncpack utilities
export * from './syncpack/generator.js';
// Export types
export * from './types.js';
// Export updaters
export * from './updaters/bun.js';
export * from './updaters/devenv.js';
export * from './updaters/nixpkgs.js';
