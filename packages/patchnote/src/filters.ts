import micromatch from 'micromatch';
import type { Logger } from './logger.js';
import type { FilterConfig, PackageUpdate } from './types.js';

/**
 * Filter package updates based on exclude/include patterns.
 * - Exclude takes precedence over include.
 * - Empty include means "all packages included".
 * - Uses micromatch for glob pattern matching.
 */
export function filterUpdates(
  updates: PackageUpdate[],
  filters: FilterConfig | undefined,
  logger?: Logger,
): PackageUpdate[] {
  if (!filters) return updates;
  const { exclude = [], include = [] } = filters;
  if (exclude.length === 0 && include.length === 0) return updates;

  return updates.filter((update) => {
    if (exclude.length > 0 && micromatch.isMatch(update.name, exclude)) {
      logger?.info(`Filtered out (excluded): ${update.name}`);
      return false;
    }
    if (include.length > 0 && !micromatch.isMatch(update.name, include)) {
      logger?.info(`Filtered out (not in include list): ${update.name}`);
      return false;
    }
    return true;
  });
}
