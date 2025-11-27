/**
 * AI-powered changelog analysis using Claude
 *
 * TODO: Migrate to OpenCode SDK when available to support multiple AI providers
 */

import Anthropic from '@anthropic-ai/sdk';
import { type DepUpdaterConfig, sanitizeConfigForLogging } from '../config.js';
import type { PackageUpdate } from '../types.js';

/** Token budget for the analysis prompt */
const TOKEN_BUDGET = 8000;
/** Model to use for summarization (cheaper/faster) */
const SUMMARIZATION_MODEL = 'claude-3-5-haiku-latest';

/**
 * Count tokens for a prompt using the Anthropic API
 */
async function countTokens(
  client: Anthropic,
  model: string,
  content: string,
  config: DepUpdaterConfig,
): Promise<number> {
  try {
    const result = await client.beta.messages.countTokens({
      model,
      messages: [{ role: 'user', content }],
    });
    return result.input_tokens;
  } catch (error) {
    config.logger?.warn(`Token counting API failed: ${error instanceof Error ? error.message : String(error)}`);
    config.logger?.warn('Falling back to character estimate (~4 chars/token)');
    return Math.ceil(content.length / 4);
  }
}

/**
 * Summarize a large changelog using a faster model
 */
async function summarizeChangelog(
  client: Anthropic,
  changelog: string,
  packageName: string,
  config: DepUpdaterConfig,
): Promise<string> {
  try {
    const message = await client.messages.create({
      model: SUMMARIZATION_MODEL,
      max_tokens: 500,
      messages: [
        {
          role: 'user',
          content: `Summarize this changelog for ${packageName} in 2-3 bullet points. Focus on breaking changes, security fixes, and important new features. Be concise.\n\n${changelog}`,
        },
      ],
    });
    const content = message.content[0];
    return content.type === 'text' ? content.text : changelog.substring(0, 1000);
  } catch (error) {
    config.logger?.warn(
      `Failed to summarize changelog for ${packageName}: ${error instanceof Error ? error.message : String(error)}`,
    );
    config.logger?.warn('Falling back to truncation (first 1000 chars)');
    return `${changelog.substring(0, 1000)}...`;
  }
}

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
    const model = config.ai.model || 'claude-sonnet-4-5';

    // Build prompt and check token count, summarizing large changelogs if needed
    const prompt = await buildPromptWithinBudget(client, model, updates, changelogs, config);

    const message = await client.messages.create({
      model,
      max_tokens: 2048,
      messages: [{ role: 'user', content: prompt }],
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
 * Build the analysis prompt, summarizing changelogs if total tokens exceed budget
 */
async function buildPromptWithinBudget(
  client: Anthropic,
  model: string,
  updates: PackageUpdate[],
  changelogs: Map<string, string>,
  config: DepUpdaterConfig,
): Promise<string> {
  // Start with original changelogs (copy to avoid mutating input)
  const workingChangelogs = new Map(changelogs);

  // Build initial prompt
  let changelogData = prepareChangelogData(updates, workingChangelogs);
  let prompt = buildPrompt(changelogData);

  // Count tokens once for the full prompt
  let tokens = await countTokens(client, model, prompt, config);
  config.logger?.debug?.(`Initial prompt tokens: ${tokens}`);

  // If under budget, we're done
  if (tokens <= TOKEN_BUDGET) {
    return prompt;
  }

  // Over budget - find and summarize the largest changelogs
  config.logger?.info(`Prompt exceeds token budget (${tokens} > ${TOKEN_BUDGET}), summarizing large changelogs`);

  // Sort changelogs by size (largest first)
  const sortedBySize = [...workingChangelogs.entries()]
    .filter(([_, content]) => content.length > 0)
    .sort((a, b) => b[1].length - a[1].length);

  // Summarize largest changelogs until under budget
  for (const [name, content] of sortedBySize) {
    if (tokens <= TOKEN_BUDGET) break;

    config.logger?.info(`Summarizing changelog for ${name} (${content.length} chars)`);
    const summarized = await summarizeChangelog(client, content, name, config);
    workingChangelogs.set(name, summarized);

    // Rebuild prompt and recount
    changelogData = prepareChangelogData(updates, workingChangelogs);
    prompt = buildPrompt(changelogData);
    tokens = await countTokens(client, model, prompt, config);
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
