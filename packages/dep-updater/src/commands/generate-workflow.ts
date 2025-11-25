/**
 * Generate GitHub Actions workflow for automated dependency updates
 */

import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { DepUpdaterConfig } from '../config.js';
import { getRepoRoot } from '../git.js';
import type { GenerateWorkflowOptions } from '../types.js';
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
 * Process template placeholders for PAT authentication
 */
function processPATTemplate(template: string, useAI: boolean): string {
  let result = template;

  if (useAI) {
    // With AI
    result = result.replace('{{AI_HEADER_SUFFIX}}', ' + AI Changelog Analysis');
    result = result.replace(
      '{{AI_SETUP_STEP}}',
      '\n#   2. Generate Anthropic API key at https://console.anthropic.com/settings/keys',
    );
    result = result.replace('{{STEP_SECRETS}}', '3');
    result = result.replace('{{AI_SECRETS_PLURAL}}', 's:');
    result = result.replace('{{AI_SECRET_COMMAND}}', '\n#      gh secret set ANTHROPIC_API_KEY --org YOUR_ORG');
    result = result.replace('{{STEP_COPY}}', '4');
    result = result.replace('{{STEP_COMMIT}}', '5');
    result = result.replace('{{AI_STEP_SUFFIX}}', ' with AI changelog analysis');
    result = result.replace(
      '{{AI_ENV_VAR}}',
      '\n          ANTHROPIC_API_KEY: ' + '$' + '{{ secrets.ANTHROPIC_API_KEY }}',
    );
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
function processGitHubAppTemplate(template: string, useAI: boolean): string {
  let result = template;

  if (useAI) {
    // With AI
    result = result.replace('{{AI_HEADER_SUFFIX}}', ' + AI Changelog Analysis');
    result = result.replace(
      '{{AI_SETUP_STEP}}',
      '\n#   3. Generate Anthropic API key at https://console.anthropic.com/settings/keys',
    );
    result = result.replace('{{STEP_VAR}}', '4');
    result = result.replace('{{STEP_SECRETS}}', '5');
    result = result.replace('{{AI_SECRETS_PLURAL}}', 's:');
    result = result.replace('{{AI_SECRET_LIST}}', '\n#      - ANTHROPIC_API_KEY');
    result = result.replace('{{STEP_COPY}}', '6');
    result = result.replace('{{STEP_VALIDATE}}', '7');
    result = result.replace('{{AI_STEP_SUFFIX}}', ' with AI changelog analysis');
    result = result.replace(
      '{{AI_ENV_VAR}}',
      '\n          ANTHROPIC_API_KEY: ' + '$' + '{{ secrets.ANTHROPIC_API_KEY }}',
    );
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
}): Promise<string> {
  const { authType, useAI } = options;

  const templatesDir = getTemplatesDir();
  const templateFile = authType === 'github-app' ? 'github-app.yml' : 'pat.yml';
  const templatePath = join(templatesDir, templateFile);

  const template = await readFile(templatePath, 'utf-8');

  if (authType === 'github-app') {
    return processGitHubAppTemplate(template, useAI);
  }
  return processPATTemplate(template, useAI);
}

/**
 * Generate GitHub Actions workflow file
 */
export async function generateWorkflow(config: DepUpdaterConfig, options: GenerateWorkflowOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  // Determine settings
  const authType = options.authType || 'pat';
  const useAI = options.enableAI === true || (!options.skipAI && config.ai?.apiKey !== undefined);

  config.logger?.info('Generating GitHub Actions workflow...\n');
  config.logger?.info(`  Auth type: ${authType}`);
  config.logger?.info(`  AI analysis: ${useAI ? 'enabled' : 'disabled'}\n`);

  const workflowContent = await generateWorkflowContentFromTemplate({ authType, useAI });

  if (options.dryRun) {
    config.logger?.info('[DRY RUN] Would generate workflow file');
    config.logger?.info('\nWorkflow content:\n');
    config.logger?.info(workflowContent);
    return;
  }

  // Create .github/workflows directory
  const workflowDir = safeResolve(repoRoot, '.github/workflows');
  if (!existsSync(workflowDir)) {
    await mkdir(workflowDir, { recursive: true });
    config.logger?.info('✓ Created .github/workflows directory');
  }

  // Write workflow file
  const workflowPath = join(workflowDir, 'update-deps.yml');
  await writeFile(workflowPath, workflowContent, 'utf-8');

  config.logger?.info(`✓ Generated workflow file: ${workflowPath}\n`);
  config.logger?.info('Follow the setup instructions in the workflow file header.\n');
}
