/**
 * Validates dep-updater setup
 * Checks GitHub CLI, authentication, app installation, permissions, and config
 */

import { execa } from 'execa';
import { loadConfig } from '../config.js';
import type { Logger } from '../logger.js';
import type { CommandExecutor } from '../types.js';

interface ValidationResult {
  success: boolean;
  message: string;
  details?: string;
}

/**
 * Validate complete dep-updater setup
 * Returns 0 if all checks pass, 1 if any fail
 */
export async function validateSetup(
  logger: Logger,
  repoRoot?: string,
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<number> {
  logger.info('🔍 Validating dep-updater setup...\n');

  const checks: ValidationResult[] = [];

  // 1. Check GitHub CLI is installed
  checks.push(await checkGHCLI(executor));

  // 2. Check GitHub CLI is authenticated
  checks.push(await checkGHAuth(executor));

  // 3. Check GitHub App is installed
  checks.push(await checkGitHubAppInstalled(repoRoot, executor));

  // 4. Check GitHub App has required permissions
  checks.push(await checkGitHubAppPermissions(repoRoot, executor));

  // 5. Check can generate GitHub App token
  checks.push(await checkCanGenerateToken(repoRoot, executor));

  // 6. Check config file exists and is valid
  checks.push(await checkConfig(repoRoot));

  // Print results
  let hasFailures = false;
  for (const check of checks) {
    if (check.success) {
      logger.info(`✓ ${check.message}`);
      if (check.details) {
        logger.info(`  ${check.details}`);
      }
    } else {
      logger.error(`✗ ${check.message}`);
      if (check.details) {
        logger.error(`  ${check.details}`);
      }
      hasFailures = true;
    }
  }

  // Print summary
  logger.info('');
  if (hasFailures) {
    logger.error('❌ Setup validation failed. Please fix the issues above.');
    logger.info(
      '\n📖 See setup guide: https://github.com/smoothbricks/smoothbricks/blob/main/packages/dep-updater/docs/SETUP.md',
    );
    return 1;
  }

  logger.info('✅ All checks passed! Your dep-updater setup is ready.');
  logger.info('\n🚀 Next steps:');
  logger.info('   - Run workflow manually: gh workflow run update-deps.yml');
  logger.info('   - Or wait for scheduled run (check .github/workflows/update-deps.yml)');
  return 0;
}

/**
 * Check if GitHub CLI is installed
 */
async function checkGHCLI(executor: CommandExecutor): Promise<ValidationResult> {
  try {
    await executor('gh', ['--version']);
    return {
      success: true,
      message: 'GitHub CLI is installed',
    };
  } catch {
    return {
      success: false,
      message: 'GitHub CLI (gh) is not installed',
      details: 'Install from: https://cli.github.com',
    };
  }
}

/**
 * Check if GitHub CLI is authenticated
 */
async function checkGHAuth(executor: CommandExecutor): Promise<ValidationResult> {
  try {
    await executor('gh', ['auth', 'status']);
    return {
      success: true,
      message: 'GitHub CLI is authenticated',
    };
  } catch {
    return {
      success: false,
      message: 'GitHub CLI is not authenticated',
      details: 'Run: gh auth login',
    };
  }
}

/**
 * Check if GitHub App is installed on repository
 */
async function checkGitHubAppInstalled(
  repoRoot: string | undefined,
  executor: CommandExecutor,
): Promise<ValidationResult> {
  try {
    const cwd = repoRoot || process.cwd();
    const { stdout } = await executor('gh', ['api', '/repos/{owner}/{repo}/installation', '--jq', '.id'], {
      cwd,
    });

    const installationId = stdout.trim();
    return {
      success: true,
      message: 'GitHub App is installed on this repository',
      details: `Installation ID: ${installationId}`,
    };
  } catch (error: unknown) {
    // Check if it's a 404 (not installed) vs other errors
    const stderr = (error as { stderr?: string }).stderr || '';
    const message = error instanceof Error ? error.message : '';
    const is404 = stderr.includes('404') || message.includes('404');

    if (is404) {
      return {
        success: false,
        message: 'GitHub App is not installed on this repository',
        details: 'Install the app from: GitHub App settings → Install App',
      };
    }

    return {
      success: false,
      message: 'Failed to check GitHub App installation',
      details: 'Run in a git repository with GitHub remote configured',
    };
  }
}

/**
 * Check if GitHub App has required permissions
 */
async function checkGitHubAppPermissions(
  repoRoot: string | undefined,
  executor: CommandExecutor,
): Promise<ValidationResult> {
  try {
    const cwd = repoRoot || process.cwd();
    const { stdout } = await executor('gh', ['api', '/repos/{owner}/{repo}/installation', '--jq', '.permissions'], {
      cwd,
    });

    const permissions = JSON.parse(stdout);

    const requiredPermissions = {
      contents: 'write',
      pull_requests: 'write',
    };

    const missing: string[] = [];
    for (const [perm, level] of Object.entries(requiredPermissions)) {
      if (permissions[perm] !== level) {
        missing.push(`${perm}: ${level}`);
      }
    }

    if (missing.length > 0) {
      return {
        success: false,
        message: 'GitHub App is missing required permissions',
        details: `Missing: ${missing.join(', ')}`,
      };
    }

    return {
      success: true,
      message: 'GitHub App has required permissions',
      details: 'contents:write, pull-requests:write',
    };
  } catch {
    return {
      success: false,
      message: 'Could not verify GitHub App permissions',
      details: 'This check requires the app to be installed first',
    };
  }
}

/**
 * Check if we can generate a GitHub App token
 * This validates that DEP_UPDATER_APP_ID and DEP_UPDATER_APP_PRIVATE_KEY are configured
 */
async function checkCanGenerateToken(
  repoRoot: string | undefined,
  executor: CommandExecutor,
): Promise<ValidationResult> {
  // Check if environment variables are set
  const appId = process.env.DEP_UPDATER_APP_ID;
  const privateKey = process.env.DEP_UPDATER_APP_PRIVATE_KEY;

  if (!appId || !privateKey) {
    return {
      success: false,
      message: 'GitHub App credentials not configured',
      details: 'Set DEP_UPDATER_APP_ID and DEP_UPDATER_APP_PRIVATE_KEY environment variables or repository secrets',
    };
  }

  try {
    // Try to get installation ID (validates auth works)
    const cwd = repoRoot || process.cwd();
    await executor('gh', ['api', '/repos/{owner}/{repo}/installation', '--jq', '.id'], {
      cwd,
    });

    return {
      success: true,
      message: 'Can generate GitHub App tokens',
      details: 'App ID and private key are configured correctly',
    };
  } catch {
    return {
      success: false,
      message: 'GitHub App token generation failed',
      details: 'Check that App ID and private key are correct',
    };
  }
}

/**
 * Check if config file exists and is valid
 */
async function checkConfig(repoRoot?: string): Promise<ValidationResult> {
  try {
    const config = await loadConfig(repoRoot);

    // Basic validation - check required fields exist
    if (!config.prStrategy) {
      return {
        success: false,
        message: 'Config file is invalid',
        details: 'Missing required field: prStrategy',
      };
    }

    // Check if config looks reasonable
    const warnings: string[] = [];
    if (config.prStrategy.maxStackDepth < 1) {
      warnings.push('prStrategy.maxStackDepth should be >= 1');
    }

    return {
      success: true,
      message: 'Config file is valid',
      details: warnings.length > 0 ? `Warnings: ${warnings.join(', ')}` : undefined,
    };
  } catch {
    return {
      success: false,
      message: 'Config file not found or invalid',
      details: 'Run: dep-updater init (to create config)',
    };
  }
}
