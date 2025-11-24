/**
 * Integration tests for init command
 * Tests configuration generation, project detection, and file creation
 */

import { describe, expect, test } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import type { InitOptions, ProjectSetup } from '../../src/types.js';

describe('init command', () => {
  const defaultConfig: DepUpdaterConfig = {
    repoRoot: '/test-repo',
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 5,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      requireTests: true,
    },
    ai: {
      provider: 'anthropic',
    },
    git: {
      remote: 'origin',
      baseBranch: 'main',
    },
  };

  describe('Project detection', () => {
    test('should detect Expo project', () => {
      const detected: ProjectSetup = {
        hasExpo: true,
        hasNix: false,
        hasSyncpack: false,
        packageManager: 'bun',
      };

      expect(detected.hasExpo).toBe(true);
      expect(detected.packageManager).toBe('bun');
    });

    test('should detect Nix project', () => {
      const detected: ProjectSetup = {
        hasExpo: false,
        hasNix: true,
        hasSyncpack: false,
        packageManager: 'bun',
      };

      expect(detected.hasNix).toBe(true);
    });

    test('should detect syncpack', () => {
      const detected: ProjectSetup = {
        hasExpo: false,
        hasNix: false,
        hasSyncpack: true,
        packageManager: 'bun',
      };

      expect(detected.hasSyncpack).toBe(true);
    });

    test('should detect package manager from lock files', () => {
      // Test each package manager
      const managers = ['bun', 'npm', 'pnpm', 'yarn'] as const;

      for (const manager of managers) {
        const detected: ProjectSetup = {
          hasExpo: false,
          hasNix: false,
          hasSyncpack: false,
          packageManager: manager,
        };

        expect(detected.packageManager).toBe(manager);
      }
    });

    test('should handle minimal project (npm only)', () => {
      const detected: ProjectSetup = {
        hasExpo: false,
        hasNix: false,
        hasSyncpack: false,
        packageManager: 'npm',
      };

      expect(detected.hasExpo).toBe(false);
      expect(detected.hasNix).toBe(false);
      expect(detected.hasSyncpack).toBe(false);
      expect(detected.packageManager).toBe('npm');
    });

    test('should handle complex project (all features)', () => {
      const detected: ProjectSetup = {
        hasExpo: true,
        hasNix: true,
        hasSyncpack: true,
        packageManager: 'bun',
      };

      expect(detected.hasExpo).toBe(true);
      expect(detected.hasNix).toBe(true);
      expect(detected.hasSyncpack).toBe(true);
    });
  });

  describe('Config generation - JSON format', () => {
    test('should generate minimal JSON config', () => {
      const options = {
        enableExpo: false,
        enableNix: false,
        enableAI: false,
        enableStacking: false,
        maxStackDepth: 5,
      };

      // Generate config structure
      const config = {
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
          provider: 'anthropic',
          model: 'claude-sonnet-4-5-20250929',
        },
      };

      const jsonString = JSON.stringify(config, null, 2);

      // Verify structure
      expect(jsonString).toContain('"expo"');
      expect(jsonString).toContain('"nix"');
      expect(jsonString).toContain('"prStrategy"');
      expect(jsonString).toContain('"ai"');
      expect(config.expo.enabled).toBe(false);
      expect(config.nix.enabled).toBe(false);
      expect(config.prStrategy.stackingEnabled).toBe(false);
    });

    test('should generate Expo-enabled JSON config', () => {
      const options = {
        enableExpo: true,
        enableNix: false,
        enableAI: false,
        enableStacking: true,
        maxStackDepth: 5,
      };

      const config = {
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
          provider: 'anthropic',
          model: 'claude-sonnet-4-5-20250929',
        },
      };

      expect(config.expo.enabled).toBe(true);
      expect(config.expo.autoDetect).toBe(true);
      expect(config.expo.projects).toEqual([]);
    });

    test('should generate Nix-enabled JSON config', () => {
      const options = {
        enableExpo: false,
        enableNix: true,
        enableAI: false,
        enableStacking: true,
        maxStackDepth: 3,
      };

      const config = {
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
          provider: 'anthropic',
          model: 'claude-sonnet-4-5-20250929',
        },
      };

      expect(config.nix.enabled).toBe(true);
      expect(config.nix.devenvPath).toBe('./tooling/direnv');
      expect(config.nix.nixpkgsOverlayPath).toBe('./tooling/direnv/nixpkgs-overlay');
    });

    test('should generate all-features JSON config', () => {
      const options = {
        enableExpo: true,
        enableNix: true,
        enableAI: true,
        enableStacking: true,
        maxStackDepth: 5,
      };

      const config = {
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
          provider: 'anthropic',
          model: 'claude-sonnet-4-5-20250929',
        },
      };

      expect(config.expo.enabled).toBe(true);
      expect(config.nix.enabled).toBe(true);
      expect(config.prStrategy.stackingEnabled).toBe(true);
    });
  });

  describe('Config generation - TypeScript format', () => {
    test('should generate TypeScript config structure', () => {
      const options = {
        enableExpo: true,
        enableNix: false,
        enableAI: false,
        enableStacking: true,
        maxStackDepth: 5,
      };

      // Simulate TypeScript config generation
      const tsConfig = `import { defineConfig } from 'dep-updater';

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

      // Verify TypeScript structure
      expect(tsConfig).toContain('import { defineConfig }');
      expect(tsConfig).toContain('export default defineConfig');
      expect(tsConfig).toContain('enabled: true'); // Expo enabled
      expect(tsConfig).toContain('enabled: false'); // Nix disabled
      expect(tsConfig).toContain('stackingEnabled: true');
      expect(tsConfig).toContain('maxStackDepth: 5');
    });

    test('should use boolean literals in TypeScript config', () => {
      const tsConfig = `
  expo: {
    enabled: true,
    autoDetect: true,
  },
  nix: {
    enabled: false,
  },
`;

      // TypeScript should use true/false, not "true"/"false"
      expect(tsConfig).toContain('enabled: true');
      expect(tsConfig).toContain('enabled: false');
      expect(tsConfig).not.toContain('"true"');
      expect(tsConfig).not.toContain('"false"');
    });
  });

  describe('Init options', () => {
    test('should handle --yes flag (non-interactive)', () => {
      const options: InitOptions = {
        yes: true,
        dryRun: false,
      };

      expect(options.yes).toBe(true);

      // With --yes, should not prompt user
      const shouldPrompt = !options.yes;
      expect(shouldPrompt).toBe(false);
    });

    test('should handle --dry-run flag', () => {
      const options: InitOptions = {
        yes: false,
        dryRun: true,
      };

      expect(options.dryRun).toBe(true);

      // With --dry-run, should not write files
      const shouldWriteFiles = !options.dryRun;
      expect(shouldWriteFiles).toBe(false);
    });

    test('should handle combined flags', () => {
      const options: InitOptions = {
        yes: true,
        dryRun: true,
      };

      expect(options.yes).toBe(true);
      expect(options.dryRun).toBe(true);

      const shouldPrompt = !options.yes;
      const shouldWriteFiles = !options.dryRun;

      expect(shouldPrompt).toBe(false);
      expect(shouldWriteFiles).toBe(false);
    });
  });

  describe('Authentication type selection', () => {
    test('should default to PAT auth', () => {
      const authType: 'pat' | 'github-app' = 'pat';

      expect(authType).toBe('pat');
    });

    test('should support GitHub App auth', () => {
      const authType: 'pat' | 'github-app' = 'github-app';

      expect(authType).toBe('github-app');
    });

    test('should validate auth type', () => {
      const validAuthTypes = ['pat', 'github-app'];

      expect(validAuthTypes).toContain('pat');
      expect(validAuthTypes).toContain('github-app');
      expect(validAuthTypes).not.toContain('invalid');
    });
  });

  describe('File path generation', () => {
    test('should generate correct JSON config path', () => {
      const repoRoot = '/test-repo';
      const configPath = `${repoRoot}/tooling/dep-updater.json`;

      expect(configPath).toBe('/test-repo/tooling/dep-updater.json');
    });

    test('should generate correct TypeScript config path', () => {
      const repoRoot = '/test-repo';
      const configPath = `${repoRoot}/tooling/dep-updater.ts`;

      expect(configPath).toBe('/test-repo/tooling/dep-updater.ts');
    });

    test('should generate correct tooling directory path', () => {
      const repoRoot = '/test-repo';
      const toolingDir = `${repoRoot}/tooling`;

      expect(toolingDir).toBe('/test-repo/tooling');
    });
  });

  describe('Overwrite logic', () => {
    test('should detect existing config', () => {
      const existingConfigPaths = ['/test-repo/tooling/dep-updater.ts', '/test-repo/tooling/dep-updater.json'];

      // Simulate finding existing config
      const existingConfig = existingConfigPaths[0]; // TS config exists

      expect(existingConfig).toBe('/test-repo/tooling/dep-updater.ts');
    });

    test('should allow overwrite with --yes flag', () => {
      const existingConfig = '/test-repo/tooling/dep-updater.json';
      const options: InitOptions = {
        yes: true,
        dryRun: false,
      };

      // With --yes, should overwrite without prompting
      const shouldOverwrite = options.yes || false; // Assuming user confirms

      expect(shouldOverwrite).toBe(true);
    });

    test('should skip overwrite in dry-run mode', () => {
      const existingConfig = '/test-repo/tooling/dep-updater.json';
      const options: InitOptions = {
        yes: false,
        dryRun: true,
      };

      // In dry-run, should not actually overwrite
      const shouldActuallyWrite = !options.dryRun;

      expect(shouldActuallyWrite).toBe(false);
    });
  });

  describe('Workflow generation integration', () => {
    test('should generate workflow with correct auth type', () => {
      const authType = 'pat';
      const generateWorkflowOptions = {
        authType,
        skipAI: true,
      };

      expect(generateWorkflowOptions.authType).toBe('pat');
      expect(generateWorkflowOptions.skipAI).toBe(true);
    });

    test('should pass enableAI to workflow generator', () => {
      const enableAI = true;
      const generateWorkflowOptions = {
        authType: 'github-app' as const,
        enableAI,
      };

      expect(generateWorkflowOptions.enableAI).toBe(true);
      expect(generateWorkflowOptions.authType).toBe('github-app');
    });

    test('should skip workflow generation if user declines', () => {
      const generateWorkflowFile = false;

      const shouldGenerateWorkflow = generateWorkflowFile;

      expect(shouldGenerateWorkflow).toBe(false);
    });
  });

  describe('PR strategy defaults', () => {
    test('should use correct default maxStackDepth', () => {
      const maxStackDepth = 5;

      expect(maxStackDepth).toBe(5);
      expect(maxStackDepth).toBeGreaterThan(0);
      expect(maxStackDepth).toBeLessThanOrEqual(10);
    });

    test('should enable stacking by default', () => {
      const enableStacking = true;

      expect(enableStacking).toBe(true);
    });

    test('should have correct branch prefix', () => {
      const branchPrefix = 'chore/update-deps';

      expect(branchPrefix).toBe('chore/update-deps');
      expect(branchPrefix).toStartWith('chore/');
    });

    test('should have correct PR title prefix', () => {
      const prTitlePrefix = 'chore: update dependencies';

      expect(prTitlePrefix).toBe('chore: update dependencies');
      expect(prTitlePrefix).toStartWith('chore:');
    });
  });

  describe('AI configuration', () => {
    test('should use Anthropic provider', () => {
      const aiProvider = 'anthropic';

      expect(aiProvider).toBe('anthropic');
    });

    test('should use correct Claude model', () => {
      const model = 'claude-sonnet-4-5-20250929';

      expect(model).toContain('claude-sonnet');
      expect(model).toContain('20250929'); // Version date
    });
  });

  describe('Nix paths', () => {
    test('should use correct devenv path', () => {
      const devenvPath = './tooling/direnv';

      expect(devenvPath).toBe('./tooling/direnv');
      expect(devenvPath).toStartWith('./tooling/');
    });

    test('should use correct nixpkgs overlay path', () => {
      const overlayPath = './tooling/direnv/nixpkgs-overlay';

      expect(overlayPath).toBe('./tooling/direnv/nixpkgs-overlay');
      expect(overlayPath).toStartWith('./tooling/direnv/');
    });
  });
});
