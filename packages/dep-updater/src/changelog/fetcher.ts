/**
 * Fetch changelogs for package updates
 */

import type { PackageUpdate } from '../types.js';

/**
 * Fetch changelog from npm registry
 */
async function fetchNpmChangelog(packageName: string, version: string): Promise<string | null> {
  try {
    // Try to get changelog from npm registry metadata
    const response = await fetch(`https://registry.npmjs.org/${packageName}`);
    if (!response.ok) return null;

    const rawData = await response.json();
    if (!rawData || typeof rawData !== 'object') {
      console.warn(`Invalid npm registry response for ${packageName}`);
      return null;
    }

    const data = rawData as { versions?: Record<string, { repository?: { url?: string } }> };
    if (!data.versions) {
      console.warn(`No versions found in npm registry response for ${packageName}`);
      return null;
    }
    const versionData = data.versions?.[version];

    // Check for repository URL
    const repository = versionData?.repository || (data as { repository?: { url?: string } }).repository;
    if (repository?.url) {
      const githubMatch = repository.url.match(/github\.com[/:]([^/]+\/[^/.]+)/);
      if (githubMatch) {
        const repo = githubMatch[1];
        return `https://github.com/${repo}/releases/tag/v${version}`;
      }
    }

    return null;
  } catch (error) {
    console.warn(
      `Failed to fetch npm changelog for ${packageName}@${version}:`,
      error instanceof Error ? error.message : String(error),
    );
    return null;
  }
}

/**
 * Fetch changelog from GitHub releases
 */
async function fetchGitHubChangelog(packageName: string, version: string): Promise<string | null> {
  try {
    // Common patterns for GitHub repositories
    const possibleRepos = [packageName.replace('@', '').replace('/', '/'), packageName.split('/').pop()];

    for (const repo of possibleRepos) {
      const url = `https://api.github.com/repos/${repo}/releases/tags/v${version}`;
      const response = await fetch(url);

      if (response.ok) {
        const data = (await response.json()) as { body?: string; html_url?: string };
        return data.body || data.html_url || null;
      }
    }

    return null;
  } catch (error) {
    console.warn(
      `Failed to fetch GitHub changelog for ${packageName}@${version}:`,
      error instanceof Error ? error.message : String(error),
    );
    return null;
  }
}

/**
 * Fetch changelog content from URL
 */
async function fetchChangelogContent(url: string): Promise<string | null> {
  try {
    const response = await fetch(url);
    if (!response.ok) return null;

    const contentType = response.headers.get('content-type');
    if (contentType?.includes('application/json')) {
      const data = (await response.json()) as { body?: string };
      return data.body || JSON.stringify(data, null, 2);
    }

    return await response.text();
  } catch (error) {
    console.warn(
      `Failed to fetch changelog content from ${url}:`,
      error instanceof Error ? error.message : String(error),
    );
    return null;
  }
}

/**
 * Fetch changelog for a package update
 * Returns an object with URL and content
 */
export async function fetchChangelog(
  update: PackageUpdate,
  logger?: import('../logger.js').Logger,
): Promise<{ url: string | null; content: string | null }> {
  logger?.info(`Fetching changelog for ${update.name}...`);

  // Try npm registry first
  let changelogUrl = await fetchNpmChangelog(update.name, update.toVersion);

  // If npm didn't work, try GitHub
  if (!changelogUrl) {
    changelogUrl = await fetchGitHubChangelog(update.name, update.toVersion);
  }

  // If we have a URL, fetch the content
  if (changelogUrl?.startsWith('http')) {
    const content = await fetchChangelogContent(changelogUrl);
    return { url: changelogUrl, content: content || changelogUrl };
  }

  return { url: changelogUrl, content: changelogUrl };
}

/**
 * Fetch changelogs for multiple package updates
 * Populates changelogUrl on update objects and returns content map
 */
export async function fetchChangelogs(
  updates: PackageUpdate[],
  maxConcurrent = 5,
  logger?: import('../logger.js').Logger,
): Promise<Map<string, string>> {
  const changelogs = new Map<string, string>();

  // Process in batches to avoid rate limiting
  for (let i = 0; i < updates.length; i += maxConcurrent) {
    const batch = updates.slice(i, i + maxConcurrent);
    const results = await Promise.all(
      batch.map(async (update) => {
        const result = await fetchChangelog(update, logger);
        return { update, result };
      }),
    );

    for (const { update, result } of results) {
      // Store URL in update object
      if (result.url) {
        update.changelogUrl = result.url;
      }
      // Store content in map
      if (result.content) {
        changelogs.set(update.name, result.content);
      }
    }
  }

  logger?.info(`✓ Fetched ${changelogs.size} changelogs`);
  return changelogs;
}

/**
 * Generate simple changelog summary without AI
 */
export function generateSimpleChangelog(updates: PackageUpdate[]): string {
  const sections = {
    major: [] as PackageUpdate[],
    minor: [] as PackageUpdate[],
    patch: [] as PackageUpdate[],
    unknown: [] as PackageUpdate[],
  };

  // Group by update type
  for (const update of updates) {
    sections[update.updateType].push(update);
  }

  const parts: string[] = [];

  // Format each section
  for (const [type, items] of Object.entries(sections)) {
    if (items.length === 0) continue;

    const title = type.charAt(0).toUpperCase() + type.slice(1);
    parts.push(`### ${title} Updates\n`);

    for (const item of items) {
      parts.push(`- ${item.name}: ${item.fromVersion} → ${item.toVersion}`);
    }

    parts.push('');
  }

  return parts.join('\n');
}
