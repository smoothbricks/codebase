/**
 * Initialize dep-updater in a project
 */

import { existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import * as p from '@clack/prompts';
import type { DepUpdaterConfig } from '../config.js';
import { getRepoRoot } from '../git.js';
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
      packageJsonPath: './package.json',
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
      provider: 'anthropic',
      model: 'claude-sonnet-4-5-20250929',
    },
  };

  return JSON.stringify(config, null, 2) + '\n';
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
    packageJsonPath: './package.json',
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

  // Interactive prompts (unless --yes flag)
  if (!options.yes) {
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
      p.note('Requires ANTHROPIC_API_KEY in GitHub Secrets', 'Note');
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
    });
  }

  // Show next steps
  let nextSteps = `1. Review and customize tooling/${configFileName} if needed\n`;

  if (enableAI) {
    nextSteps += '2. Add ANTHROPIC_API_KEY to GitHub Secrets\n';
    nextSteps += '   (Settings → Secrets and variables → Actions → New repository secret)\n';
  }

  if (generateWorkflowFile) {
    nextSteps += `${enableAI ? '3' : '2'}. Commit the generated files:\n`;
    nextSteps += `   git add tooling/${configFileName} .github/workflows/update-deps.yml\n`;
    nextSteps += '   git commit -m "chore: add automated dependency updates"\n';
    nextSteps += '   git push\n';
    nextSteps += `${enableAI ? '4' : '3'}. Workflow will run daily at 2 AM UTC\n`;
  } else {
    nextSteps += `${enableAI ? '3' : '2'}. Commit tooling/${configFileName}\n`;
  }

  nextSteps += '\nTest it now:\n';
  nextSteps += '  bunx @conloca/dep-updater update-deps --dry-run';

  p.note(nextSteps, 'Next steps');
  p.outro('✓ Initialization complete!');
}
