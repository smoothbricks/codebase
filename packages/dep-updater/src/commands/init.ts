/**
 * Initialize dep-updater in a project
 */

import { existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import * as p from '@clack/prompts';
import type { DepUpdaterConfig } from '../config.js';
import { getRepoRoot } from '../git.js';
import { ConsoleLogger, LogLevel } from '../logger.js';
import type { InitOptions } from '../types.js';
import { safeResolve } from '../utils/path-validation.js';
import { detectProjectSetup } from '../utils/project-detection.js';
import { generateWorkflow } from './generate-workflow.js';

/**
 * Generate JSON config file content
 */
function generateJSONConfig(options: {
  enableExpo: boolean;
  enableNix: boolean;
  enableAI: boolean;
  enableStacking: boolean;
  maxStackDepth: number;
}): string {
  const config: Partial<DepUpdaterConfig> = {
    expo: {
      enabled: options.enableExpo,
      autoDetect: true,
      projects: [],
    },
    nix: {
      enabled: options.enableNix,
      devenvPath: './tooling/direnv',
      nixpkgsOverlayPath: './tooling/direnv/nixpkgs-overlay',
    },
    prStrategy: {
      stackingEnabled: options.enableStacking,
      maxStackDepth: options.maxStackDepth,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    ai: {
      provider: 'opencode',
      model: 'big-pickle',
    },
  };

  return `${JSON.stringify(config, null, 2)}\n`;
}

/**
 * Generate TypeScript config file content
 */
function generateTypeScriptConfig(options: {
  enableExpo: boolean;
  enableNix: boolean;
  enableAI: boolean;
  enableStacking: boolean;
  maxStackDepth: number;
}): string {
  return `import { defineConfig } from 'dep-updater';

export default defineConfig({
  expo: {
    enabled: ${options.enableExpo},
    autoDetect: true,
    projects: [],
  },
  nix: {
    enabled: ${options.enableNix},
    devenvPath: './tooling/direnv',
    nixpkgsOverlayPath: './tooling/direnv/nixpkgs-overlay',
  },
  prStrategy: {
    stackingEnabled: ${options.enableStacking},
    maxStackDepth: ${options.maxStackDepth},
    autoCloseOldPRs: true,
    resetOnMerge: true,
    stopOnConflicts: true,
    branchPrefix: 'chore/update-deps',
    prTitlePrefix: 'chore: update dependencies',
  },
  ai: {
    provider: 'anthropic',
    model: 'claude-sonnet-4-5-20250929',
  },
});
`;
}

/**
 * Initialize dep-updater in a project
 */
export async function init(config: DepUpdaterConfig, options: InitOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  p.intro('🚀 Initializing dep-updater');

  // Check if already initialized
  const toolingDir = safeResolve(repoRoot, 'tooling');
  const configJSONPath = safeResolve(toolingDir, 'dep-updater.json');
  const configTSPath = safeResolve(toolingDir, 'dep-updater.ts');

  const existingConfig = [configTSPath, configJSONPath].find((path) => existsSync(path));

  if (existingConfig && !options.dryRun) {
    const overwrite =
      options.yes ||
      (await p.confirm({
        message: `Config already exists at ${existingConfig}. Overwrite?`,
        initialValue: false,
      }));

    if (p.isCancel(overwrite) || !overwrite) {
      p.cancel('Initialization cancelled.');
      return;
    }
  }

  // Detect project setup
  const s = p.spinner();
  s.start('Detecting project setup');
  const detected = await detectProjectSetup(repoRoot);
  s.stop('Project setup detected');

  config.logger?.info(`  Package manager: ${detected.packageManager}`);
  config.logger?.info(`  Expo detected: ${detected.hasExpo ? 'yes' : 'no'}`);
  config.logger?.info(`  Nix detected: ${detected.hasNix ? 'yes' : 'no'}`);
  config.logger?.info(`  Syncpack detected: ${detected.hasSyncpack ? 'yes' : 'no'}`);
  config.logger?.info('');

  if (detected.packageManager !== 'bun') {
    p.note(
      `Your project uses ${detected.packageManager}. Support for other package managers is coming soon.`,
      'Warning',
    );
  }

  let enableExpo = detected.hasExpo;
  let enableNix = detected.hasNix;
  let enableAI = false;
  let enableStacking = true;
  let maxStackDepth = 5;
  let generateWorkflowFile = true;
  let useTypeScript = false;
  let authType: 'pat' | 'github-app' = 'pat'; // Default to PAT (simpler)

  // Interactive prompts (unless --yes flag)
  if (!options.yes) {
    // Ask about authentication type first
    const authPrompt = await p.select({
      message: 'Choose authentication method for GitHub Actions:',
      options: [
        {
          value: 'pat',
          label: 'Personal Access Token (PAT)',
          hint: 'Simple 5-minute setup, good for small teams',
        },
        {
          value: 'github-app',
          label: 'GitHub App',
          hint: 'Advanced 15-minute setup, higher rate limits',
        },
      ],
      initialValue: 'pat',
    });
    if (p.isCancel(authPrompt)) {
      p.cancel('Operation cancelled.');
      return;
    }
    authType = authPrompt as 'pat' | 'github-app';

    // Show appropriate setup note based on auth type
    if (authType === 'github-app') {
      const hasGitHubApp = process.env.DEP_UPDATER_APP_ID && process.env.DEP_UPDATER_APP_PRIVATE_KEY;

      if (!hasGitHubApp) {
        p.note(
          'GitHub App authentication requires one-time setup (15-20 minutes):\n\n' +
            '  • Organization-level configuration (all repos inherit settings)\n' +
            '  • Higher rate limits (15,000 req/hour vs 5,000 for PAT)\n' +
            '  • PRs trigger CI workflows (unlike GITHUB_TOKEN)\n\n' +
            '📖 See docs/SETUP.md in the dep-updater package',
          'GitHub App Setup Required',
        );

        const hasCompletedSetup = await p.confirm({
          message: 'Have you completed the GitHub App setup?',
          initialValue: false,
        });

        if (p.isCancel(hasCompletedSetup) || !hasCompletedSetup) {
          p.outro(
            'Please complete the GitHub App setup first, then run this command again.\n\n' +
              '📖 Setup guide: https://github.com/smoothbricks/smoothbricks/blob/main/packages/dep-updater/docs/SETUP.md',
          );
          return;
        }
      }
    } else {
      // PAT setup note
      p.note(
        'Personal Access Token setup is quick and simple:\n\n' +
          '  1. Generate PAT: https://github.com/settings/tokens/new\n' +
          '  2. Add to org secrets: gh secret set DEP_UPDATER_TOKEN --org YOUR_ORG\n\n' +
          '📖 Quick start guide: https://github.com/smoothbricks/smoothbricks/blob/main/packages/dep-updater/docs/QUICK-START.md',
        'PAT Setup (5 minutes)',
      );
    }

    const formatPrompt = await p.select({
      message: 'Config file format?',
      options: [
        { value: 'json', label: 'JSON (tooling/dep-updater.json)', hint: 'Simple configuration' },
        {
          value: 'ts',
          label: 'TypeScript (tooling/dep-updater.ts)',
          hint: 'Advanced with type safety and custom logic',
        },
      ],
      initialValue: 'json',
    });
    if (p.isCancel(formatPrompt)) {
      p.cancel('Operation cancelled.');
      return;
    }
    useTypeScript = formatPrompt === 'ts';

    const expoPrompt = await p.confirm({
      message: 'Enable Expo SDK support?',
      initialValue: detected.hasExpo,
    });
    if (p.isCancel(expoPrompt)) {
      p.cancel('Operation cancelled.');
      return;
    }
    enableExpo = expoPrompt;

    if (detected.hasNix) {
      const nixPrompt = await p.confirm({
        message: 'Enable Nix (devenv/nixpkgs) updates?',
        initialValue: true,
      });
      if (p.isCancel(nixPrompt)) {
        p.cancel('Operation cancelled.');
        return;
      }
      enableNix = nixPrompt;
    }

    const aiPrompt = await p.confirm({
      message: 'Enable AI-powered changelog analysis?',
      initialValue: false,
    });
    if (p.isCancel(aiPrompt)) {
      p.cancel('Operation cancelled.');
      return;
    }
    enableAI = aiPrompt;

    if (enableAI) {
      p.note(
        'Requires an AI provider API key in GitHub Secrets.\n' +
          'Supported: ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY',
        'Note',
      );
    }

    if (enableExpo) {
      p.note(
        'Expo projects will be auto-detected by scanning for packages with "expo" dependency.\nTo manually specify projects, edit the "projects" array in the config file.',
        'Expo Auto-Detection',
      );
    }

    const stackingPrompt = await p.confirm({
      message: 'Enable PR stacking?',
      initialValue: true,
    });
    if (p.isCancel(stackingPrompt)) {
      p.cancel('Operation cancelled.');
      return;
    }
    enableStacking = stackingPrompt;

    if (enableStacking) {
      const maxDepthPrompt = await p.text({
        message: 'Max PRs to keep open (1-10)',
        initialValue: '5',
        validate: (value) => {
          const num = Number.parseInt(value, 10);
          if (Number.isNaN(num) || num < 1 || num > 10) {
            return 'Please enter a number between 1 and 10';
          }
          return undefined;
        },
      });
      if (p.isCancel(maxDepthPrompt)) {
        p.cancel('Operation cancelled.');
        return;
      }
      maxStackDepth = Number.parseInt(maxDepthPrompt, 10);
    }

    const workflowPrompt = await p.confirm({
      message: 'Generate GitHub Actions workflow?',
      initialValue: true,
    });
    if (p.isCancel(workflowPrompt)) {
      p.cancel('Operation cancelled.');
      return;
    }
    generateWorkflowFile = workflowPrompt;
  }

  const configFileName = useTypeScript ? 'dep-updater.ts' : 'dep-updater.json';
  const configPath = safeResolve(toolingDir, configFileName);

  if (options.dryRun) {
    config.logger?.info(`[DRY RUN] Would create tooling/${configFileName}:\n`);
    const content = useTypeScript
      ? generateTypeScriptConfig({
          enableExpo,
          enableNix,
          enableAI,
          enableStacking,
          maxStackDepth,
        })
      : generateJSONConfig({
          enableExpo,
          enableNix,
          enableAI,
          enableStacking,
          maxStackDepth,
        });
    config.logger?.info(content);

    if (generateWorkflowFile) {
      config.logger?.info('[DRY RUN] Would also generate .github/workflows/update-deps.yml');
    }
    return;
  }

  // Create tooling/ directory if it doesn't exist
  if (!existsSync(toolingDir)) {
    await mkdir(toolingDir, { recursive: true });
  }

  // Write config file
  const configContent = useTypeScript
    ? generateTypeScriptConfig({
        enableExpo,
        enableNix,
        enableAI,
        enableStacking,
        maxStackDepth,
      })
    : generateJSONConfig({
        enableExpo,
        enableNix,
        enableAI,
        enableStacking,
        maxStackDepth,
      });

  await writeFile(configPath, configContent, 'utf-8');

  // Generate workflow file if requested
  if (generateWorkflowFile) {
    const workflowConfig: DepUpdaterConfig = {
      ...config,
      repoRoot,
    };

    // Only include AI config if enabled
    if (!enableAI) {
      workflowConfig.ai = { ...config.ai, apiKey: undefined };
    }

    await generateWorkflow(workflowConfig, {
      dryRun: false,
      skipGit: false,
      skipAI: !enableAI,
      authType,
    });
  }

  // Optionally run validate-setup if GitHub App credentials are configured locally
  if (authType === 'github-app' && !options.yes) {
    const hasGitHubApp = process.env.DEP_UPDATER_APP_ID && process.env.DEP_UPDATER_APP_PRIVATE_KEY;

    if (hasGitHubApp) {
      const runValidation = await p.confirm({
        message: 'Run setup validation now?',
        initialValue: true,
      });

      if (!p.isCancel(runValidation) && runValidation) {
        p.log.step('Running setup validation...\n');

        const { validateSetup } = await import('./validate-setup.js');
        const logger = config.logger || new ConsoleLogger(LogLevel.INFO);
        const exitCode = await validateSetup(logger, repoRoot);

        if (exitCode === 0) {
          p.log.success('✅ Setup validation passed!\n');
        } else {
          p.log.warning('!  Some validation checks failed. Please review the output above.\n');
        }
      }
    }
  }

  // Show next steps
  let nextSteps = '';
  let stepNumber = 1;

  // Authentication-specific steps
  if (authType === 'pat') {
    // PAT authentication steps
    nextSteps += `${stepNumber}. Generate Personal Access Token:\n`;
    nextSteps += '   https://github.com/settings/tokens/new (scope: repo)\n\n';
    stepNumber++;

    nextSteps += `${stepNumber}. Add organization secret:\n`;
    nextSteps += '   gh secret set DEP_UPDATER_TOKEN --org YOUR_ORG\n\n';
    stepNumber++;
  } else {
    // GitHub App authentication steps
    nextSteps += `${stepNumber}. Validate your GitHub App setup:\n`;
    nextSteps += '   npx @smoothbricks/dep-updater validate-setup\n\n';
    stepNumber++;
  }

  // Review config
  nextSteps += `${stepNumber}. Review and customize tooling/${configFileName} if needed\n\n`;
  stepNumber++;

  // Add API key if AI enabled
  if (enableAI) {
    nextSteps += `${stepNumber}. Add your AI provider API key to GitHub organization secrets\n`;
    nextSteps += '   Supported: ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY\n';
    nextSteps += '   (Organization settings → Secrets and variables → Actions → New organization secret)\n\n';
    stepNumber++;
  }

  // Commit and push
  if (generateWorkflowFile) {
    nextSteps += `${stepNumber}. Commit the generated files:\n`;
    nextSteps += `   git add tooling/${configFileName} .github/workflows/update-deps.yml\n`;
    nextSteps += '   git commit -m "chore: add automated dependency updates"\n';
    nextSteps += '   git push\n\n';
    stepNumber++;
  } else {
    nextSteps += `${stepNumber}. Commit tooling/${configFileName}\n\n`;
    stepNumber++;
  }

  // Test or wait for scheduled run
  if (generateWorkflowFile) {
    nextSteps += `${stepNumber}. Test manually: gh workflow run update-deps.yml\n`;
    nextSteps += '   Or wait for scheduled run (daily at 2 AM UTC)\n';
  } else {
    nextSteps += `${stepNumber}. Test it now:\n`;
    nextSteps += '   bunx @smoothbricks/dep-updater update-deps --dry-run';
  }

  // Link to appropriate setup guide
  if (authType === 'pat') {
    nextSteps +=
      '\n\n📖 Quick start guide: https://github.com/smoothbricks/smoothbricks/blob/main/packages/dep-updater/docs/QUICK-START.md';
  } else {
    nextSteps +=
      '\n\n📖 Setup guide: https://github.com/smoothbricks/smoothbricks/blob/main/packages/dep-updater/docs/SETUP.md';
  }

  p.note(nextSteps, 'Next steps');
  p.outro('✓ Initialization complete!');
}
