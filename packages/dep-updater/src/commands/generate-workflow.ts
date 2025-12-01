/**
 * Generate GitHub Actions workflow for automated dependency updates
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
 * Process template placeholders for PAT authentication
 */
function processPATTemplate(template: string, useAI: boolean, provider: SupportedProvider): string {
  let result = template;

  if (useAI) {
    const aiConfig = getAIEnvVarConfig(provider);
    const needsSecret = aiConfig !== null;

    // With AI (supports opencode free tier, or paid providers: anthropic, openai, google)
    if (needsSecret) {
      // Paid provider - needs API key secret
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + AI Changelog Analysis');
      result = result.replace(
        '{{AI_SETUP_STEP}}',
        `\n#   2. Get API key for your AI provider (configured: ${provider})`,
      );
      result = result.replace('{{STEP_SECRETS}}', '3');
      result = result.replace('{{AI_SECRETS_PLURAL}}', 's:');
      result = result.replace('{{AI_SECRET_COMMAND}}', `\n#      gh secret set ${aiConfig.envVar} --org YOUR_ORG`);
      result = result.replace('{{STEP_COPY}}', '4');
      result = result.replace('{{STEP_COMMIT}}', '5');
      result = result.replace('{{AI_STEP_SUFFIX}}', ' with AI changelog analysis');
      result = result.replace('{{AI_ENV_VAR}}', aiConfig.secretRef);
    } else {
      // Free tier (opencode) - no API key needed
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + Free AI Changelog Analysis');
      result = result.replace('{{AI_SETUP_STEP}}', ''); // No API key step needed
      result = result.replace('{{STEP_SECRETS}}', '2');
      result = result.replace('{{AI_SECRETS_PLURAL}}', ':');
      result = result.replace('{{AI_SECRET_COMMAND}}', '');
      result = result.replace('{{STEP_COPY}}', '3');
      result = result.replace('{{STEP_COMMIT}}', '4');
      result = result.replace('{{AI_STEP_SUFFIX}}', ' with free AI changelog analysis');
      result = result.replace('{{AI_ENV_VAR}}', '');
    }
  } else {
    // Without AI
    result = result.replace('{{AI_HEADER_SUFFIX}}', ' (Simple Setup)');
    result = result.replace('{{AI_SETUP_STEP}}', '');
    result = result.replace('{{STEP_SECRETS}}', '2');
    result = result.replace('{{AI_SECRETS_PLURAL}}', ':');
    result = result.replace('{{AI_SECRET_COMMAND}}', '');
    result = result.replace('{{STEP_COPY}}', '3');
    result = result.replace('{{STEP_COMMIT}}', '4');
    result = result.replace('{{AI_STEP_SUFFIX}}', '');
    result = result.replace('{{AI_ENV_VAR}}', '');
  }

  return result;
}

/**
 * Process template placeholders for GitHub App authentication
 */
function processGitHubAppTemplate(template: string, useAI: boolean, provider: SupportedProvider): string {
  let result = template;

  if (useAI) {
    const aiConfig = getAIEnvVarConfig(provider);
    const needsSecret = aiConfig !== null;

    // With AI (supports opencode free tier, or paid providers: anthropic, openai, google)
    if (needsSecret) {
      // Paid provider - needs API key secret
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + AI Changelog Analysis');
      result = result.replace(
        '{{AI_SETUP_STEP}}',
        `\n#   3. Get API key for your AI provider (configured: ${provider})`,
      );
      result = result.replace('{{STEP_VAR}}', '4');
      result = result.replace('{{STEP_SECRETS}}', '5');
      result = result.replace('{{AI_SECRETS_PLURAL}}', 's:');
      result = result.replace('{{AI_SECRET_LIST}}', `\n#      - ${aiConfig.envVar}`);
      result = result.replace('{{STEP_COPY}}', '6');
      result = result.replace('{{STEP_VALIDATE}}', '7');
      result = result.replace('{{AI_STEP_SUFFIX}}', ' with AI changelog analysis');
      result = result.replace('{{AI_ENV_VAR}}', aiConfig.secretRef);
    } else {
      // Free tier (opencode) - no API key needed
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + Free AI Changelog Analysis');
      result = result.replace('{{AI_SETUP_STEP}}', ''); // No API key step needed
      result = result.replace('{{STEP_VAR}}', '3');
      result = result.replace('{{STEP_SECRETS}}', '4');
      result = result.replace('{{AI_SECRETS_PLURAL}}', ':');
      result = result.replace('{{AI_SECRET_LIST}}', '');
      result = result.replace('{{STEP_COPY}}', '5');
      result = result.replace('{{STEP_VALIDATE}}', '6');
      result = result.replace('{{AI_STEP_SUFFIX}}', ' with free AI changelog analysis');
      result = result.replace('{{AI_ENV_VAR}}', '');
    }
  } else {
    // Without AI
    result = result.replace('{{AI_HEADER_SUFFIX}}', ' (Simple Setup)');
    result = result.replace('{{AI_SETUP_STEP}}', '');
    result = result.replace('{{STEP_VAR}}', '3');
    result = result.replace('{{STEP_SECRETS}}', '4');
    result = result.replace('{{AI_SECRETS_PLURAL}}', ':');
    result = result.replace('{{AI_SECRET_LIST}}', '');
    result = result.replace('{{STEP_COPY}}', '5');
    result = result.replace('{{STEP_VALIDATE}}', '6');
    result = result.replace('{{AI_STEP_SUFFIX}}', '');
    result = result.replace('{{AI_ENV_VAR}}', '');
  }

  return result;
}

/**
 * Generate workflow content from template
 */
async function generateWorkflowContentFromTemplate(options: {
  authType: 'pat' | 'github-app';
  useAI: boolean;
  provider: SupportedProvider;
}): Promise<string> {
  const { authType, useAI, provider } = options;

  const templatesDir = getTemplatesDir();
  const templateFile = authType === 'github-app' ? 'github-app.yml' : 'pat.yml';
  const templatePath = join(templatesDir, templateFile);

  const template = await readFile(templatePath, 'utf-8');

  if (authType === 'github-app') {
    return processGitHubAppTemplate(template, useAI, provider);
  }
  return processPATTemplate(template, useAI, provider);
}

/**
 * Generate GitHub Actions workflow file
 */
export async function generateWorkflow(config: DepUpdaterConfig, options: GenerateWorkflowOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  // Determine settings
  const authType = options.authType || 'pat';
  // AI is enabled by default for free tier (opencode), or when explicitly enabled, or when API key is configured
  const isFreeProvider = !providerRequiresSecret(config.ai.provider);
  const useAI = options.enableAI === true || (!options.skipAI && (isFreeProvider || config.ai?.apiKey !== undefined));

  const provider = config.ai.provider;

  config.logger?.info('Generating GitHub Actions workflow...\n');
  config.logger?.info(`  Auth type: ${authType}`);
  config.logger?.info(`  AI analysis: ${useAI ? `enabled (${provider})` : 'disabled'}\n`);

  const workflowContent = await generateWorkflowContentFromTemplate({ authType, useAI, provider });

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
  config.logger?.info('Follow the setup instructions in the workflow file header.\n');
}
