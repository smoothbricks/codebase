/**
 * Generate GitHub Actions workflow for automated dependency updates
 *
 * Uses a unified template that auto-detects auth type at runtime:
 * - If vars.DEP_UPDATER_APP_ID is set → GitHub App mode (priority)
 * - Otherwise → PAT mode (uses secrets.DEP_UPDATER_TOKEN)
 */

import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { PROVIDER_ENV_VARS } from '../ai/opencode-client.js';
import type { DepUpdaterConfig } from '../config.js';
import { getRepoRoot } from '../git.js';
import type { GenerateWorkflowOptions, SupportedProvider } from '../types.js';
import { safeResolve } from '../utils/path-validation.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Get the path to the templates directory
 */
function getTemplatesDir(): string {
  // When running from dist/, templates are at ../templates
  // When running from src/ (dev), templates are at ../../templates
  const distTemplates = join(__dirname, '..', 'templates', 'workflows');
  const srcTemplates = join(__dirname, '..', '..', 'templates', 'workflows');

  if (existsSync(distTemplates)) {
    return distTemplates;
  }
  if (existsSync(srcTemplates)) {
    return srcTemplates;
  }

  throw new Error('Could not find templates directory');
}

/**
 * Check if provider requires an API key in the workflow
 * OpenCode is free and doesn't require any secrets
 */
function providerRequiresSecret(provider: SupportedProvider): boolean {
  return PROVIDER_ENV_VARS[provider] !== '';
}

/**
 * Get the env var configuration for the workflow based on provider
 * Returns null for providers that don't require secrets (like opencode)
 */
function getAIEnvVarConfig(provider: SupportedProvider): { envVar: string; secretRef: string } | null {
  const envVar = PROVIDER_ENV_VARS[provider];
  if (!envVar) {
    return null; // No secret needed (e.g., opencode)
  }
  return {
    envVar,
    secretRef: `\n          ${envVar}: \${{ secrets.${envVar} }}`,
  };
}

/**
 * Process unified template placeholders
 * Only handles AI-related placeholders - auth detection is runtime
 */
function processUnifiedTemplate(template: string, useAI: boolean, provider: SupportedProvider): string {
  let result = template;

  if (useAI) {
    const aiConfig = getAIEnvVarConfig(provider);
    const needsSecret = aiConfig !== null;

    if (needsSecret) {
      // Paid provider - needs API key secret
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + AI Changelog Analysis');
      result = result.replace(
        '{{AI_SETUP_NOTE}}',
        `\n#   - Configured provider: ${provider} (requires ${aiConfig.envVar} secret)`,
      );
      result = result.replace('{{AI_STEP_SUFFIX}}', ' with AI changelog analysis');
      result = result.replace('{{AI_ENV_VAR}}', aiConfig.secretRef);
    } else {
      // Free tier (opencode) - no API key needed
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + Free AI Changelog Analysis');
      result = result.replace('{{AI_SETUP_NOTE}}', ''); // No extra note needed for free tier
      result = result.replace('{{AI_STEP_SUFFIX}}', ' with free AI changelog analysis');
      result = result.replace('{{AI_ENV_VAR}}', '');
    }
  } else {
    // Without AI
    result = result.replace('{{AI_HEADER_SUFFIX}}', '');
    result = result.replace('{{AI_SETUP_NOTE}}', '');
    result = result.replace('{{AI_STEP_SUFFIX}}', '');
    result = result.replace('{{AI_ENV_VAR}}', '');
  }

  return result;
}

/**
 * Generate workflow content from unified template
 */
async function generateWorkflowContentFromTemplate(options: {
  useAI: boolean;
  provider: SupportedProvider;
}): Promise<string> {
  const { useAI, provider } = options;

  const templatesDir = getTemplatesDir();
  const templatePath = join(templatesDir, 'unified.yml');

  const template = await readFile(templatePath, 'utf-8');
  return processUnifiedTemplate(template, useAI, provider);
}

/**
 * Generate GitHub Actions workflow file
 */
export async function generateWorkflow(config: DepUpdaterConfig, options: GenerateWorkflowOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  // AI is enabled by default for free tier (opencode), or when explicitly enabled, or when API key is configured
  const isFreeProvider = !providerRequiresSecret(config.ai.provider);
  const useAI = options.enableAI === true || (!options.skipAI && (isFreeProvider || config.ai?.apiKey !== undefined));

  const provider = config.ai.provider;

  config.logger?.info('Generating GitHub Actions workflow...\n');
  config.logger?.info('  Auth type: auto-detected at runtime (GitHub App > PAT)');
  config.logger?.info(`  AI analysis: ${useAI ? `enabled (${provider})` : 'disabled'}\n`);

  const workflowContent = await generateWorkflowContentFromTemplate({ useAI, provider });

  // Compute paths first so we can show them in dry-run
  const workflowDir = safeResolve(repoRoot, '.github/workflows');
  const workflowPath = join(workflowDir, 'update-deps.yml');

  if (options.dryRun) {
    config.logger?.info(`[DRY RUN] Would generate: ${workflowPath}`);
    config.logger?.info('\nWorkflow content:\n');
    config.logger?.info(workflowContent);
    return;
  }

  // Create .github/workflows directory
  if (!existsSync(workflowDir)) {
    await mkdir(workflowDir, { recursive: true });
    config.logger?.info('✓ Created .github/workflows directory');
  }

  // Write workflow file
  await writeFile(workflowPath, workflowContent, 'utf-8');

  config.logger?.info(`✓ Generated workflow file: ${workflowPath}\n`);
  config.logger?.info('Next steps:\n');
  config.logger?.info('  1. Choose your auth method and configure secrets/variables:');
  config.logger?.info('     • PAT: Add DEP_UPDATER_TOKEN secret');
  config.logger?.info('     • GitHub App: Add DEP_UPDATER_APP_ID variable + DEP_UPDATER_APP_PRIVATE_KEY secret\n');
  config.logger?.info('  2. Commit and push the workflow file\n');
  config.logger?.info('  3. Test: gh workflow run update-deps.yml\n');
}
