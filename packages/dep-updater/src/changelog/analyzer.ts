/**
 * AI-powered changelog analysis using Claude
 */

import Anthropic from '@anthropic-ai/sdk';
import { type DepUpdaterConfig, sanitizeConfigForLogging } from '../config.js';
import type { PackageUpdate } from '../types.js';

/**
 * Analyze changelogs using Claude API
 */
export async function analyzeChangelogs(
  updates: PackageUpdate[],
  changelogs: Map<string, string>,
  config: DepUpdaterConfig,
): Promise<string> {
  const apiKey = config.ai.apiKey || process.env.ANTHROPIC_API_KEY;

  if (!apiKey) {
    config.logger?.warn('No Anthropic API key found, skipping AI analysis');
    config.logger?.warn('AI config:', JSON.stringify(sanitizeConfigForLogging(config).ai));
    return generateFallbackSummary(updates);
  }

  try {
    const client = new Anthropic({ apiKey });

    // Prepare changelog data for Claude
    const changelogData = prepareChangelogData(updates, changelogs);

    const prompt = `You are analyzing dependency updates for a software project. Below are package updates with their changelogs.

Your task:
1. Summarize the most important changes
2. Identify breaking changes and highlight them with !
3. Note security fixes with 🔒
4. Group related updates together
5. Include source links to changelogs where available
6. Keep it concise and actionable

Updates:
${changelogData}

Please provide a markdown summary suitable for a Pull Request description. Include clickable links to changelog sources in the format: [changelog](url)`;

    const message = await client.messages.create({
      model: config.ai.model || 'claude-sonnet-4-5-20250929',
      max_tokens: 2048,
      messages: [
        {
          role: 'user',
          content: prompt,
        },
      ],
    });

    const content = message.content[0];
    if (content.type === 'text') {
      return content.text;
    }

    return generateFallbackSummary(updates);
  } catch (error) {
    config.logger?.warn('Claude API analysis failed:', error instanceof Error ? error.message : String(error));
    if (process.env.VERBOSE) {
      config.logger?.warn('AI config:', JSON.stringify(sanitizeConfigForLogging(config).ai));
    }
    return generateFallbackSummary(updates);
  }
}

/**
 * Prepare changelog data for AI analysis
 */
function prepareChangelogData(updates: PackageUpdate[], changelogs: Map<string, string>): string {
  const parts: string[] = [];

  for (const update of updates) {
    parts.push(`## ${update.name}`);
    parts.push(`Version: ${update.fromVersion} → ${update.toVersion}`);
    parts.push(`Type: ${update.updateType}`);
    parts.push(`Ecosystem: ${update.ecosystem}`);

    // Include changelog URL if available
    if (update.changelogUrl) {
      parts.push(`Source: ${update.changelogUrl}`);
    }

    const changelog = changelogs.get(update.name);
    if (changelog) {
      // Truncate long changelogs
      const truncated = changelog.length > 1000 ? `${changelog.substring(0, 1000)}...` : changelog;
      parts.push(`\nChangelog:\n${truncated}`);
    } else {
      parts.push('\nNo changelog available');
    }

    parts.push('\n---\n');
  }

  return parts.join('\n');
}

/**
 * Generate fallback summary without AI
 */
function generateFallbackSummary(updates: PackageUpdate[]): string {
  const sections = {
    major: [] as PackageUpdate[],
    minor: [] as PackageUpdate[],
    patch: [] as PackageUpdate[],
  };

  // Group by update type
  for (const update of updates) {
    if (update.updateType === 'major') {
      sections.major.push(update);
    } else if (update.updateType === 'minor') {
      sections.minor.push(update);
    } else if (update.updateType === 'patch') {
      sections.patch.push(update);
    }
  }

  const formatUpdate = (update: PackageUpdate): string => {
    const version = `${update.fromVersion} → ${update.toVersion}`;
    if (update.changelogUrl) {
      return `${update.name}: ${version} ([changelog](${update.changelogUrl}))`;
    }
    return `${update.name}: ${version}`;
  };

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

  parts.push(`\nTotal updates: ${updates.length}`);

  return parts.join('\n');
}

/**
 * Generate commit message using AI
 */
export async function generateCommitMessage(
  updates: PackageUpdate[],
  _config: DepUpdaterConfig,
): Promise<{ title: string; body: string }> {
  const ecosystems = [...new Set(updates.map((u) => u.ecosystem))];
  const hasBreaking = updates.some((u) => u.updateType === 'major');

  // Simple commit message without AI for now
  const title = hasBreaking ? 'chore: update dependencies (includes breaking changes)' : 'chore: update dependencies';

  const body = `Updated ${updates.length} packages across ${ecosystems.join(', ')} ecosystems.

${generateFallbackSummary(updates)}`;

  return { title, body };
}
