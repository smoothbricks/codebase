/**
 * Generate GitHub Actions workflow for automated dependency updates
 *
 * Uses a unified template that auto-detects auth type at runtime:
 * - If vars.PATCHNOTE_APP_ID is set → GitHub App mode (priority)
 * - Otherwise → PAT mode (uses secrets.PATCHNOTE_TOKEN)
 */

import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { PatchnoteConfig } from '../config.js';
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

/** AI env var name for Z.AI */
const ZAI_ENV_VAR = 'ZAI_API_KEY';
const DEFAULT_SCHEDULE = '0 2 * * *';
const DEFAULT_WORKFLOW_NAME = 'Update Dependencies';

/**
 * Process unified template placeholders
 * Handles AI-related placeholders, schedule, and base branch - auth detection is runtime
 */
function processUnifiedTemplate(
  template: string,
  options: {
    useAI: boolean;
    provider: SupportedProvider;
    schedule?: string;
    workflowName?: string;
    configPath?: string;
    forceSkipAI?: boolean;
    baseBranch?: string;
  },
): string {
  const { useAI, schedule, workflowName, configPath, forceSkipAI, baseBranch } = options;
  let result = template;

  if (forceSkipAI) {
    result = result.replace('{{AI_HEADER_SUFFIX}}', '');
    result = result.replace('{{AI_SETUP_NOTE}}', '');
    result = result.replace('{{AI_ENV_VAR}}', '');
    result = result.replace('{{SKIP_AI_INPUT}}', 'true');
  } else {
    result = result.replace(
      '{{AI_ENV_VAR}}',
      `\n        env:\n          ${ZAI_ENV_VAR}: \${{ secrets.${ZAI_ENV_VAR} }}`,
    );
    result = result.replace('{{SKIP_AI_INPUT}}', `\${{ vars.PATCHNOTE_SKIP_AI == 'true' }}`);

    if (useAI) {
      result = result.replace('{{AI_HEADER_SUFFIX}}', ' + AI Changelog Analysis');
      result = result.replace(
        '{{AI_SETUP_NOTE}}',
        `\n#   - AI provider: Z.AI GLM-5-Turbo (requires ${ZAI_ENV_VAR} secret)`,
      );
    } else {
      result = result.replace('{{AI_HEADER_SUFFIX}}', '');
      result = result.replace('{{AI_SETUP_NOTE}}', '');
    }
  }

  result = result.replace('{{WORKFLOW_NAME}}', JSON.stringify(workflowName || DEFAULT_WORKFLOW_NAME));
  result = result.replace('{{SCHEDULE}}', schedule || DEFAULT_SCHEDULE);
  result = result.replace('{{BASE_BRANCH}}', baseBranch || 'main');
  result = result.replace(
    '{{CONFIG_PATH_BLOCK}}',
    configPath ? `\n          config-path: ${JSON.stringify(configPath)}` : '',
  );

  return result;
}

/**
 * Generate workflow content from unified template
 */
async function generateWorkflowContentFromTemplate(options: {
  useAI: boolean;
  provider: SupportedProvider;
  schedule?: string;
  workflowName?: string;
  configPath?: string;
  forceSkipAI?: boolean;
  baseBranch?: string;
}): Promise<string> {
  const templatesDir = getTemplatesDir();
  const templatePath = join(templatesDir, 'unified.yml');

  const template = await readFile(templatePath, 'utf-8');
  return processUnifiedTemplate(template, options);
}

/**
 * Generate GitHub Actions workflow file
 */
export async function generateWorkflow(config: PatchnoteConfig, options: GenerateWorkflowOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  // AI is enabled by default when API key is configured, or explicitly enabled
  const useAI =
    options.enableAI === true || (!options.skipAI && (config.ai?.apiKey !== undefined || !!process.env.ZAI_API_KEY));

  const provider = config.ai.provider;

  config.logger?.info('Generating GitHub Actions workflow...\n');
  config.logger?.info('  Auth type: auto-detected at runtime (GitHub App > PAT)');
  config.logger?.info(`  AI analysis: ${useAI ? `enabled (${provider})` : 'disabled'}\n`);

  const workflowContent = await generateWorkflowContentFromTemplate({
    useAI,
    provider,
    schedule: options.schedule,
    workflowName: options.workflowName,
    configPath: options.configPath,
    forceSkipAI: options.skipAI,
    baseBranch: config.git?.baseBranch || 'main',
  });

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
  config.logger?.info('     • PAT: Add PATCHNOTE_TOKEN secret');
  config.logger?.info('     • GitHub App: Add PATCHNOTE_APP_ID variable + PATCHNOTE_APP_PRIVATE_KEY secret\n');
  config.logger?.info('  2. Commit and push the workflow file\n');
  config.logger?.info('  3. Test: gh workflow run update-deps.yml\n');
}
