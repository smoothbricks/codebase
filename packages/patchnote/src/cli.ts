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
  .option('--config-path <path>', 'Path to patchnote config file')
  .option('-v, --verbose', 'Enable verbose logging');

/**
 * Helper to setup config with logger based on CLI flags
 */
async function setupConfig() {
  const opts = program.opts();
  const config = await loadConfig(process.cwd(), opts.configPath);
  const logger = new ConsoleLogger(opts.verbose ? LogLevel.DEBUG : LogLevel.INFO);
  config.logger = logger;

  if (opts.verbose) {
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
    configPath: opts.configPath,
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
  .option('--exclude <patterns...>', 'Exclude packages matching patterns (glob)')
  .option('--include <patterns...>', 'Only update packages matching patterns (allowlist)')
  .action(async (options) => {
    const config = await setupConfig();

    // Merge CLI filter patterns into config
    if (options.exclude) {
      config.filters = config.filters || { exclude: [], include: [] };
      config.filters.exclude = [...(config.filters.exclude || []), ...options.exclude];
    }
    if (options.include) {
      config.filters = config.filters || { exclude: [], include: [] };
      config.filters.include = [...(config.filters.include || []), ...options.include];
    }

    const { updateDeps } = await import('./commands/update-deps.js');

    await updateDeps(config, getUpdateOptions());
  });

/**
 * Command: lock-file-maintenance
 * Refresh lock file without changing package.json
 */
program
  .command('lock-file-maintenance')
  .description('Refresh lock file without changing package.json (updates transitive dependencies)')
  .action(async () => {
    const config = await setupConfig();
    const { lockFileMaintenance } = await import('./commands/lock-file-maintenance.js');
    await lockFileMaintenance(config, getUpdateOptions());
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
  .option('--enable-ai', 'Enable AI changelog analysis (overrides auto-detection)')
  .action(async (options) => {
    const config = await setupConfig();

    const { generateWorkflow } = await import('./commands/generate-workflow.js');

    await generateWorkflow(config, {
      ...getUpdateOptions(),
      schedule: options.schedule,
      workflowName: options.workflowName,
      enableAI: options.enableAi, // Note: commander converts --enable-ai to enableAi
    });
  });

/**
 * Command: init
 * Interactive setup wizard
 */
program
  .command('init')
  .description('Interactive setup wizard for patchnote')
  .option('--yes', 'Skip prompts and use defaults')
  .action(async (options) => {
    const config = await setupConfig();

    const { init } = await import('./commands/init.js');

    await init(config, {
      ...getUpdateOptions(),
      yes: options.yes,
    });
  });

/**
 * Command: validate-setup
 * Validate patchnote setup (GitHub CLI, auth, app installation, permissions)
 */
program
  .command('validate-setup')
  .description('Validate patchnote setup and configuration')
  .action(async () => {
    const logger = new ConsoleLogger(program.opts().verbose ? LogLevel.DEBUG : LogLevel.INFO);

    const { validateSetup } = await import('./commands/validate-setup.js');

    const exitCode = await validateSetup(logger);
    process.exit(exitCode);
  });

/**
 * Command: setup
 * Setup patchnote GitHub App authentication via manifest flow
 */
program
  .command('setup')
  .description('Setup patchnote GitHub App authentication')
  .option('--create-app', 'Create a new GitHub App via manifest flow')
  .option('--org <org>', 'GitHub organization name')
  .action(async (options) => {
    if (!options.createApp) {
      console.error('Please specify --create-app to create a new GitHub App');
      console.error('Usage: patchnote setup --create-app [--org <org>]');
      process.exit(1);
    }
    const config = await setupConfig();
    const { setup } = await import('./commands/setup.js');
    await setup(config, {
      ...getUpdateOptions(),
      createApp: options.createApp,
      org: options.org,
    });
  });

// Parse arguments and run
program.parseAsync(process.argv).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
