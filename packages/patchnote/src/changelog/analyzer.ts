/**
 * AI-powered changelog analysis using Z.AI GLM-5-Turbo
 */

import { countTokens } from '../ai/token-counter.js';
import { sendPrompt } from '../ai/zai-client.js';
import { type PatchnoteConfig, sanitizeConfigForLogging } from '../config.js';
import type { PackageUpdate } from '../types.js';

/** Default token budget for GLM-5-Turbo (128k context window) */
const DEFAULT_TOKEN_BUDGET = 64000;

/**
 * Get the token budget for changelog analysis
 */
function getTokenBudget(config: PatchnoteConfig): number {
  return config.ai.tokenBudget || DEFAULT_TOKEN_BUDGET;
}

/**
 * Summarize a large changelog using a faster model
 */
async function summarizeChangelog(changelog: string, packageName: string, config: PatchnoteConfig): Promise<string> {
  try {
    const prompt = `Summarize this changelog for ${packageName} in 2-3 bullet points. Focus on breaking changes, security fixes, and important new features. Be concise.\n\n${changelog}`;

    const response = await sendPrompt(config, prompt);

    return response || changelog.substring(0, 1000);
  } catch (error) {
    config.logger?.warn(
      `Failed to summarize changelog for ${packageName}: ${error instanceof Error ? error.message : String(error)}`,
    );
    config.logger?.warn('Falling back to truncation (first 1000 chars)');
    return `${changelog.substring(0, 1000)}...`;
  }
}

/**
 * Analyze changelogs using Z.AI GLM-5-Turbo
 */
export async function analyzeChangelogs(
  updates: PackageUpdate[],
  changelogs: Map<string, string>,
  config: PatchnoteConfig,
  downgrades: PackageUpdate[] = [],
): Promise<string> {
  // Check for API key
  const apiKey = config.ai.apiKey || process.env.ZAI_API_KEY;
  if (!apiKey) {
    config.logger?.warn('No ZAI_API_KEY found, skipping AI analysis');
    config.logger?.warn('AI config:', JSON.stringify(sanitizeConfigForLogging(config).ai));
    return generateFallbackSummary(updates, downgrades);
  }

  try {
    // Build prompt and check token count, summarizing large changelogs if needed
    const prompt = await buildPromptWithinBudget(updates, changelogs, config);

    const response = await sendPrompt(config, prompt);

    if (response) {
      // Append nix updates and downgrades sections
      // AI may omit nix packages since they don't have changelogs
      let result = response;

      const nixSection = formatNixUpdatesSection(updates);
      if (nixSection) {
        result = `${result}\n\n${nixSection}`;
      }

      if (downgrades.length > 0) {
        result = `${result}\n\n${formatDowngradesSection(downgrades)}`;
      }

      // Append raw release notes after AI summary for reviewer reference
      const releaseNotes = renderReleaseNotesSection(updates, changelogs, 60000 - result.length);
      if (releaseNotes) {
        result = `${result}\n\n${releaseNotes}`;
      }

      return result;
    }

    return generateFallbackSummary(updates, downgrades, changelogs);
  } catch (error) {
    config.logger?.warn('AI analysis failed:', error instanceof Error ? error.message : String(error));
    if (process.env.VERBOSE) {
      config.logger?.warn('AI config:', JSON.stringify(sanitizeConfigForLogging(config).ai));
    }
    return generateFallbackSummary(updates, downgrades, changelogs);
  }
}

/**
 * Build the analysis prompt, summarizing changelogs if total tokens exceed budget
 */
async function buildPromptWithinBudget(
  updates: PackageUpdate[],
  changelogs: Map<string, string>,
  config: PatchnoteConfig,
): Promise<string> {
  // Get token budget (user override or provider default)
  const tokenBudget = getTokenBudget(config);

  // Start with original changelogs (copy to avoid mutating input)
  const workingChangelogs = new Map(changelogs);

  // Build initial prompt
  let changelogData = prepareChangelogData(updates, workingChangelogs);
  let prompt = buildPrompt(changelogData);

  // Count tokens using gpt-tokenizer
  let tokens = countTokens(prompt);
  config.logger?.debug?.(`Initial prompt tokens: ${tokens}`);

  // If under budget, we're done
  if (tokens <= tokenBudget) {
    return prompt;
  }

  // Over budget - find and summarize the largest changelogs
  config.logger?.info(`Prompt exceeds token budget (${tokens} > ${tokenBudget}), summarizing large changelogs`);

  // Sort changelogs by size (largest first)
  const sortedBySize = [...workingChangelogs.entries()]
    .filter(([_, content]) => content.length > 0)
    .sort((a, b) => b[1].length - a[1].length);

  // Summarize largest changelogs until under budget
  for (const [name, content] of sortedBySize) {
    if (tokens <= tokenBudget) break;

    config.logger?.info(`Summarizing changelog for ${name} (${content.length} chars)`);
    const summarized = await summarizeChangelog(content, name, config);
    workingChangelogs.set(name, summarized);

    // Rebuild prompt and re-count tokens
    changelogData = prepareChangelogData(updates, workingChangelogs);
    prompt = buildPrompt(changelogData);
    tokens = countTokens(prompt);
    config.logger?.debug?.(`After summarizing ${name}: ${tokens} tokens`);
  }

  return prompt;
}

/**
 * Build the full prompt with instructions
 */
function buildPrompt(changelogData: string): string {
  return `You are analyzing dependency updates for a software project. The updates are provided in XML format below.

<instructions>
1. Summarize the most important changes
2. Identify breaking changes and highlight them with !
3. Note security fixes with 🔒
4. Group related updates together
5. Include source links to changelogs where available
6. Keep it concise and actionable
</instructions>

${changelogData}

Please provide a markdown summary suitable for a Pull Request description. Include clickable links to changelog sources in the format: [changelog](url)`;
}

/**
 * Prepare changelog data for AI analysis using XML structure
 */
function prepareChangelogData(updates: PackageUpdate[], changelogs: Map<string, string>): string {
  const parts: string[] = ['<updates>'];

  for (const update of updates) {
    parts.push(`<package name="${update.name}">`);
    parts.push(`  <version from="${update.fromVersion}" to="${update.toVersion}" />`);
    parts.push(`  <type>${update.updateType}</type>`);
    parts.push(`  <ecosystem>${update.ecosystem}</ecosystem>`);

    if (update.changelogUrl) {
      parts.push(`  <source>${update.changelogUrl}</source>`);
    }

    const changelog = changelogs.get(update.name);
    if (changelog) {
      parts.push('  <changelog>');
      parts.push(`    ${changelog}`);
      parts.push('  </changelog>');
    }

    parts.push('</package>');
  }

  parts.push('</updates>');

  return parts.join('\n');
}

/**
 * Format a single update for display
 */
function formatUpdate(update: PackageUpdate): string {
  const version = `${update.fromVersion} → ${update.toVersion}`;
  if (update.changelogUrl) {
    return `${update.name}: ${version} ([changelog](${update.changelogUrl}))`;
  }
  return `${update.name}: ${version}`;
}

/**
 * Format downgrades/removals section for PR description
 */
function formatDowngradesSection(downgrades: PackageUpdate[]): string {
  if (downgrades.length === 0) return '';

  const parts: string[] = ['### i Downgrades & Removals (Informational)\n'];
  parts.push('> These changes are side effects of updating other packages and are shown for visibility.\n');

  for (const pkg of downgrades) {
    const ecosystemLabel = pkg.ecosystem !== 'npm' ? ` (${pkg.ecosystem})` : '';
    parts.push(`- ${formatUpdate(pkg)}${ecosystemLabel}`);
  }

  return parts.join('\n');
}

/**
 * Format nix updates section for PR description
 * Nix packages don't have changelogs fetched from npm, so ensure they're listed
 */
function formatNixUpdatesSection(updates: PackageUpdate[]): string {
  const nixUpdates = updates.filter((u) => u.ecosystem === 'nix' || u.ecosystem === 'nixpkgs');
  if (nixUpdates.length === 0) return '';

  const parts: string[] = ['### Nix Updates\n'];

  for (const pkg of nixUpdates) {
    parts.push(`- ${formatUpdate(pkg)} (${pkg.ecosystem})`);
  }

  return parts.join('\n');
}

/** Maximum characters for individual changelog content within a details block */
const MAX_INDIVIDUAL_CHANGELOG_CHARS = 2000;

/** Default character budget for total PR body */
const DEFAULT_CHAR_BUDGET = 55000;

/**
 * Render collapsible release notes section from changelogs
 * Each package with actual content (not just a URL) gets a <details> block.
 */
export function renderReleaseNotesSection(
  updates: PackageUpdate[],
  changelogs: Map<string, string>,
  charBudget: number = DEFAULT_CHAR_BUDGET,
): string {
  const blocks: string[] = [];
  let totalChars = 0;
  let omitted = false;

  for (const update of updates) {
    const content = changelogs.get(update.name);
    if (!content) continue;

    // Skip URL-only entries (not actual release notes)
    if (content.startsWith('http')) continue;

    // Check if we have room in the budget
    if (totalChars >= charBudget) {
      omitted = true;
      break;
    }

    // Truncate individual content if too large
    let displayContent = content;
    if (content.length > MAX_INDIVIDUAL_CHANGELOG_CHARS) {
      displayContent = `${content.substring(0, MAX_INDIVIDUAL_CHANGELOG_CHARS)}\n\n...(truncated)`;
    }

    const block = `<details><summary>${update.name} ${update.fromVersion} -> ${update.toVersion}</summary>\n\n${displayContent}\n\n</details>`;

    // Check if adding this block would exceed budget
    if (totalChars + block.length > charBudget) {
      omitted = true;
      break;
    }

    blocks.push(block);
    totalChars += block.length;
  }

  if (blocks.length === 0) return '';

  let result = `### Release Notes\n\n${blocks.join('\n\n')}`;

  if (omitted) {
    result += '\n\nAdditional release notes omitted for size.';
  }

  return result;
}

/**
 * Generate fallback summary without AI
 */
function generateFallbackSummary(
  updates: PackageUpdate[],
  downgrades: PackageUpdate[] = [],
  changelogs?: Map<string, string>,
): string {
  const sections = {
    major: [] as PackageUpdate[],
    minor: [] as PackageUpdate[],
    patch: [] as PackageUpdate[],
    other: [] as PackageUpdate[],
  };

  // Group by update type
  for (const update of updates) {
    if (update.updateType === 'major') {
      sections.major.push(update);
    } else if (update.updateType === 'minor') {
      sections.minor.push(update);
    } else if (update.updateType === 'patch') {
      sections.patch.push(update);
    } else {
      sections.other.push(update);
    }
  }

  const parts: string[] = ['## Dependency Updates\n'];

  if (sections.major.length > 0) {
    parts.push('### ! Major Updates\n');
    for (const update of sections.major) {
      parts.push(`- **${formatUpdate(update)}**`);
    }
    parts.push('');
  }

  if (sections.minor.length > 0) {
    parts.push('### Minor Updates\n');
    for (const update of sections.minor) {
      parts.push(`- ${formatUpdate(update)}`);
    }
    parts.push('');
  }

  if (sections.patch.length > 0) {
    parts.push('### Patch Updates\n');
    for (const update of sections.patch) {
      parts.push(`- ${formatUpdate(update)}`);
    }
    parts.push('');
  }

  if (sections.other.length > 0) {
    parts.push('### Other Updates\n');
    for (const update of sections.other) {
      const ecosystemLabel = update.ecosystem !== 'npm' ? ` (${update.ecosystem})` : '';
      parts.push(`- ${formatUpdate(update)}${ecosystemLabel}`);
    }
    parts.push('');
  }

  // Add downgrades section if any
  if (downgrades.length > 0) {
    parts.push(formatDowngradesSection(downgrades));
    parts.push('');
  }

  // Add release notes section if changelogs are available
  if (changelogs && changelogs.size > 0) {
    const currentLength = parts.join('\n').length;
    const releaseNotes = renderReleaseNotesSection(updates, changelogs, 60000 - currentLength);
    if (releaseNotes) {
      parts.push('');
      parts.push(releaseNotes);
    }
  }

  parts.push(`\nTotal updates: ${updates.length}`);

  return parts.join('\n');
}

/**
 * Generate commit message using AI
 */
export async function generateCommitMessage(
  updates: PackageUpdate[],
  _config: PatchnoteConfig,
  downgrades: PackageUpdate[] = [],
  changelogs?: Map<string, string>,
): Promise<{ title: string; body: string }> {
  const ecosystems = [...new Set(updates.map((u) => u.ecosystem))];
  const hasBreaking = updates.some((u) => u.updateType === 'major');

  // Simple commit message without AI for now
  const title = hasBreaking ? 'chore: update dependencies (includes breaking changes)' : 'chore: update dependencies';

  const body = `Updated ${updates.length} packages across ${ecosystems.join(', ')} ecosystems.

${generateFallbackSummary(updates, downgrades, changelogs)}`;

  return { title, body };
}
