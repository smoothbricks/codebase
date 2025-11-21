import { Command } from 'commander';
import { loadConfig, sanitizeConfigForLogging } from './config.js';
import { ConsoleLogger, LogLevel } from './logger.js';
import type { UpdateOptions } from './types.js';

const program = new Command();

// Global options
program
  .option('--dry-run', 'Run without making changes')
  .option('--skip-git', 'Skip git operations')
  .option('--skip-ai', 'Skip AI-powered changelog analysis')
  .option('-v, --verbose', 'Enable verbose logging');

/**
 * Helper to setup config with logger based on CLI flags
 */
async function setupConfig() {
  const config = await loadConfig();
  const logger = new ConsoleLogger(program.opts().verbose ? LogLevel.DEBUG : LogLevel.INFO);
  config.logger = logger;

  if (program.opts().verbose) {
    logger.debug('Configuration:', JSON.stringify(sanitizeConfigForLogging(config), null, 2));
    logger.debug('');
  }

  return config;
}

/**
 * Helper to get update options from global flags
 */
function getUpdateOptions(): UpdateOptions {
  const opts = program.opts();
  return {
    dryRun: opts.dryRun ?? false,
    skipGit: opts.skipGit ?? false,
    skipAI: opts.skipAi ?? false,
  };
}

/**
 * Command: check-expo-sdk
 * Check for Expo SDK updates without applying them
 */
program
  .command('check-expo-sdk')
  .description('Check for Expo SDK updates without applying them')
  .action(async () => {
    const config = await setupConfig();

    const { checkExpoSDK } = await import('./commands/update-expo.js');

    await checkExpoSDK(config, getUpdateOptions());
  });

/**
 * Command: update-expo
 * Update Expo SDK and regenerate syncpack configuration
 */
program
  .command('update-expo')
  .description('Update Expo SDK and regenerate syncpack configuration')
  .action(async () => {
    const config = await setupConfig();

    const { updateExpo } = await import('./commands/update-expo.js');

    await updateExpo(config, getUpdateOptions());
  });

/**
 * Command: update-deps
 * Update all dependencies (npm, devenv, nixpkgs)
 */
program
  .command('update-deps')
  .description('Update all dependencies respecting syncpack constraints')
  .action(async () => {
    const config = await setupConfig();

    const { updateDeps } = await import('./commands/update-deps.js');

    await updateDeps(config, getUpdateOptions());
  });

/**
 * Command: generate-syncpack
 * Generate syncpack configuration from Expo SDK version
 */
program
  .command('generate-syncpack')
  .description('Generate syncpack configuration from Expo recommended versions')
  .option('--expo-sdk <version>', 'Expo SDK version to use')
  .action(async (options) => {
    const config = await setupConfig();

    const { generateSyncpack } = await import('./commands/generate-syncpack.js');

    await generateSyncpack(config, {
      ...getUpdateOptions(),
      expoSdkVersion: options.expoSdk,
    });
  });

/**
 * Command: generate-workflow
 * Generate GitHub Actions workflow for automated updates
 */
program
  .command('generate-workflow')
  .description('Generate GitHub Actions workflow for automated dependency updates')
  .option('--schedule <cron>', 'Cron schedule for workflow (default: "0 2 * * *")')
  .option('--workflow-name <name>', 'Name of the workflow (default: "Update Dependencies")')
  .action(async (options) => {
    const config = await setupConfig();

    const { generateWorkflow } = await import('./commands/generate-workflow.js');

    await generateWorkflow(config, {
      ...getUpdateOptions(),
      schedule: options.schedule,
      workflowName: options.workflowName,
    });
  });

/**
 * Command: init
 * Interactive setup wizard
 */
program
  .command('init')
  .description('Interactive setup wizard for dep-updater')
  .option('--yes', 'Skip prompts and use defaults')
  .action(async (options) => {
    const config = await setupConfig();

    const { init } = await import('./commands/init.js');

    await init(config, {
      ...getUpdateOptions(),
      yes: options.yes,
    });
  });

// Parse arguments and run
program.parse(process.argv);
