/**
 * Integration tests for configuration loading and merging
 * Tests realistic config scenarios and deep merging behavior
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  type DepUpdaterConfig,
  defaultConfig,
  executeConfigScript,
  isConfigScript,
  loadConfig,
  mergeConfig,
  sanitizeConfigForLogging,
} from '../../src/config.js';
import type { DeepPartial } from '../../src/types.js';

describe('Config Integration - loadConfig', () => {
  test('should load default configuration', async () => {
    const config = await loadConfig();

    expect(config).toEqual(defaultConfig);
  });

  test('should return a new object each time (not same reference)', async () => {
    const config1 = await loadConfig();
    const config2 = await loadConfig();

    expect(config1).toEqual(config2);
    expect(config1).not.toBe(config2); // Different object references
  });

  test('should have all required configuration sections', async () => {
    const config = await loadConfig();

    expect(config.expo).toBeDefined();
    expect(config.syncpack).toBeDefined();
    expect(config.nix).toBeDefined();
    expect(config.prStrategy).toBeDefined();
    expect(config.autoMerge).toBeDefined();
    expect(config.ai).toBeDefined();
    expect(config.git).toBeDefined();
  });

  test('should have sensible defaults', async () => {
    const config = await loadConfig();

    // PR stacking enabled by default
    expect(config.prStrategy.stackingEnabled).toBe(true);
    expect(config.prStrategy.maxStackDepth).toBe(5);

    // Auto-merge disabled by default
    expect(config.autoMerge.enabled).toBe(false);

    // Expo and Nix disabled by default
    expect(config.expo?.enabled).toBe(false);
    expect(config.nix?.enabled).toBe(false);

    // Git defaults
    expect(config.git?.remote).toBe('origin');
    expect(config.git?.baseBranch).toBe('main');

    // AI defaults
    expect(config.ai.provider).toBe('anthropic');
    expect(config.ai.model).toBe('claude-sonnet-4-5-20250929');
  });
});

describe('Config Integration - mergeConfig Deep Merging', () => {
  test('should merge empty config with defaults', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {};
    const merged = mergeConfig(userConfig);

    expect(merged).toEqual(defaultConfig);
  });

  test('should override top-level scalar values', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      repoRoot: '/custom/repo',
    };
    const merged = mergeConfig(userConfig);

    expect(merged.repoRoot).toBe('/custom/repo');
    expect(merged.prStrategy).toEqual(defaultConfig.prStrategy); // Other sections unchanged
  });

  test('should deep merge prStrategy section', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        stackingEnabled: false,
        maxStackDepth: 3,
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.prStrategy.stackingEnabled).toBe(false);
    expect(merged.prStrategy.maxStackDepth).toBe(3);
    // Other prStrategy fields should keep defaults
    expect(merged.prStrategy.autoCloseOldPRs).toBe(true);
    expect(merged.prStrategy.branchPrefix).toBe('chore/update-deps');
  });

  test('should deep merge git section', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      git: {
        baseBranch: 'develop',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.git?.baseBranch).toBe('develop');
    expect(merged.git?.remote).toBe('origin'); // Keeps default
  });

  test('should deep merge ai section', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      ai: {
        apiKey: 'sk-test-key',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.ai.apiKey).toBe('sk-test-key');
    expect(merged.ai.provider).toBe('anthropic'); // Keeps default
    expect(merged.ai.model).toBe('claude-sonnet-4-5-20250929'); // Keeps default
  });

  test('should deep merge expo section when enabled', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      expo: {
        enabled: true,
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.expo?.packageJsonPath).toBe('./package.json'); // Keeps default
  });

  test('should deep merge nix section with custom paths', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      nix: {
        enabled: true,
        devenvPath: './custom/devenv',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.nix?.enabled).toBe(true);
    expect(merged.nix?.devenvPath).toBe('./custom/devenv');
    expect(merged.nix?.nixpkgsOverlayPath).toBe('./tooling/direnv/nixpkgs-overlay'); // Keeps default
  });

  test('should handle undefined optional sections correctly', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        maxStackDepth: 10,
      },
    };
    const merged = mergeConfig(userConfig);

    // expo, nix, git should use defaults even though not in userConfig
    expect(merged.expo).toEqual(defaultConfig.expo);
    expect(merged.nix).toEqual(defaultConfig.nix);
    expect(merged.git).toEqual(defaultConfig.git);
  });
});

describe('Config Integration - Realistic Scenarios', () => {
  test('minimal config for simple project', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      ai: {
        apiKey: 'sk-ant-test',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.ai.apiKey).toBe('sk-ant-test');
    expect(merged.prStrategy.stackingEnabled).toBe(true); // Uses defaults
    expect(merged.expo?.enabled).toBe(false); // Uses defaults
  });

  test('config for Expo project with stacking disabled', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      expo: {
        enabled: true,
        packageJsonPath: './apps/mobile/package.json',
      },
      prStrategy: {
        stackingEnabled: false,
      },
      ai: {
        apiKey: process.env.ANTHROPIC_API_KEY,
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.expo?.packageJsonPath).toBe('./apps/mobile/package.json');
    expect(merged.prStrategy.stackingEnabled).toBe(false);
    expect(merged.prStrategy.maxStackDepth).toBe(5); // Keeps default
    expect(merged.syncpack).toEqual(defaultConfig.syncpack); // Uses defaults
  });

  test('config for monorepo with custom base branch and aggressive stacking', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      repoRoot: '/workspace/monorepo',
      git: {
        baseBranch: 'develop',
      },
      prStrategy: {
        stackingEnabled: true,
        maxStackDepth: 10,
        stopOnConflicts: false, // Continue stacking even with conflicts
        branchPrefix: 'deps/auto-update',
        prTitlePrefix: 'deps: automated update',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.repoRoot).toBe('/workspace/monorepo');
    expect(merged.git?.baseBranch).toBe('develop');
    expect(merged.git?.remote).toBe('origin'); // Keeps default
    expect(merged.prStrategy.maxStackDepth).toBe(10);
    expect(merged.prStrategy.stopOnConflicts).toBe(false);
    expect(merged.prStrategy.branchPrefix).toBe('deps/auto-update');
    expect(merged.prStrategy.autoCloseOldPRs).toBe(true); // Keeps default
  });

  test('config with all features enabled', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      expo: {
        enabled: true,
      },
      nix: {
        enabled: true,
      },
      prStrategy: {
        stackingEnabled: true,
        maxStackDepth: 3,
      },
      autoMerge: {
        enabled: true,
        mode: 'patch',
      },
      ai: {
        apiKey: 'sk-test',
        model: 'claude-3-5-sonnet-20241022',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.nix?.enabled).toBe(true);
    expect(merged.prStrategy.stackingEnabled).toBe(true);
    expect(merged.autoMerge.enabled).toBe(true);
    expect(merged.autoMerge.mode).toBe('patch');
    expect(merged.ai.model).toBe('claude-3-5-sonnet-20241022');
  });

  test('config overriding all default branch and prefix settings', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      git: {
        remote: 'upstream',
        baseBranch: 'trunk',
      },
      prStrategy: {
        branchPrefix: 'automated/updates',
        prTitlePrefix: 'build(deps): update',
      },
      syncpack: {
        configPath: './configs/.syncpackrc.json',
        fixScriptName: 'fix:versions',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.git?.remote).toBe('upstream');
    expect(merged.git?.baseBranch).toBe('trunk');
    expect(merged.prStrategy.branchPrefix).toBe('automated/updates');
    expect(merged.prStrategy.prTitlePrefix).toBe('build(deps): update');
    expect(merged.syncpack?.configPath).toBe('./configs/.syncpackrc.json');
    expect(merged.syncpack?.fixScriptName).toBe('fix:versions');
  });
});

describe('Config Integration - sanitizeConfigForLogging', () => {
  test('should redact API key', () => {
    const config: DepUpdaterConfig = {
      ...defaultConfig,
      ai: {
        provider: 'anthropic',
        apiKey: 'sk-ant-secret-key-123',
        model: 'claude-sonnet-4-5-20250929',
      },
    };

    const sanitized = sanitizeConfigForLogging(config);

    expect(sanitized.ai?.apiKey).toBe('***REDACTED***');
    expect(sanitized.ai?.provider).toBe('anthropic');
    expect(sanitized.ai?.model).toBe('claude-sonnet-4-5-20250929');
  });

  test('should handle missing API key', () => {
    const config: DepUpdaterConfig = {
      ...defaultConfig,
      ai: {
        provider: 'anthropic',
      },
    };

    const sanitized = sanitizeConfigForLogging(config);

    expect(sanitized.ai?.apiKey).toBeUndefined();
  });

  test('should preserve all non-sensitive configuration', () => {
    const config: DepUpdaterConfig = {
      ...defaultConfig,
      repoRoot: '/test/repo',
      ai: {
        provider: 'anthropic',
        apiKey: 'sk-secret',
      },
      prStrategy: {
        ...defaultConfig.prStrategy,
        maxStackDepth: 7,
      },
    };

    const sanitized = sanitizeConfigForLogging(config);

    expect(sanitized.repoRoot).toBe('/test/repo');
    expect(sanitized.prStrategy?.maxStackDepth).toBe(7);
    expect(sanitized.expo).toEqual(config.expo);
    expect(sanitized.git).toEqual(config.git);
  });

  test('should be safe to log sanitized config', () => {
    const config: DepUpdaterConfig = {
      ...defaultConfig,
      ai: {
        provider: 'anthropic',
        apiKey: 'sk-ant-very-secret-key-should-not-appear-in-logs',
      },
    };

    const sanitized = sanitizeConfigForLogging(config);
    const logOutput = JSON.stringify(sanitized);

    expect(logOutput).not.toContain('sk-ant-very-secret-key');
    expect(logOutput).toContain('***REDACTED***');
  });
});

describe('Config Integration - Type Safety', () => {
  test('should enforce required prStrategy fields', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        stackingEnabled: false,
      },
    };
    const merged = mergeConfig(userConfig);

    // TypeScript ensures all required fields are present
    expect(merged.prStrategy.stackingEnabled).toBeDefined();
    expect(merged.prStrategy.maxStackDepth).toBeDefined();
    expect(merged.prStrategy.autoCloseOldPRs).toBeDefined();
    expect(merged.prStrategy.resetOnMerge).toBeDefined();
    expect(merged.prStrategy.stopOnConflicts).toBeDefined();
    expect(merged.prStrategy.branchPrefix).toBeDefined();
    expect(merged.prStrategy.prTitlePrefix).toBeDefined();
  });

  test('should enforce required autoMerge fields', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      autoMerge: {
        enabled: true,
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.autoMerge.enabled).toBeDefined();
    expect(merged.autoMerge.mode).toBeDefined();
    expect(merged.autoMerge.requireTests).toBeDefined();
  });

  test('should enforce required ai fields', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      ai: {
        apiKey: 'test',
      },
    };
    const merged = mergeConfig(userConfig);

    expect(merged.ai.provider).toBeDefined();
    expect(merged.ai.apiKey).toBeDefined();
  });
});

describe('Config Integration - Script Mode', () => {
  const testDir = join(process.cwd(), 'test-temp-script-mode');

  beforeEach(async () => {
    // Create test directory
    if (!existsSync(testDir)) {
      await mkdir(testDir, { recursive: true });
    }
  });

  afterEach(async () => {
    // Clean up test directory
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
  });

  describe('isConfigScript', () => {
    test('should return true for .ts file that exports a function', async () => {
      const configPath = join(testDir, 'config-function.ts');
      await writeFile(
        configPath,
        `export default async function() {
          console.log('test');
        }`,
      );

      const result = await isConfigScript(configPath);
      expect(result).toBe(true);
    });

    test('should return true for .ts file that exports a sync function', async () => {
      const configPath = join(testDir, 'config-sync.ts');
      await writeFile(
        configPath,
        `export default function() {
          return 'test';
        }`,
      );

      const result = await isConfigScript(configPath);
      expect(result).toBe(true);
    });

    test('should return false for .ts file that exports an object', async () => {
      const configPath = join(testDir, 'config-object.ts');
      await writeFile(
        configPath,
        `import { defineConfig } from '../../src/config.js';
        export default defineConfig({
          expo: { enabled: true }
        });`,
      );

      const result = await isConfigScript(configPath);
      expect(result).toBe(false);
    });

    test('should return false for .json files', async () => {
      const configPath = join(testDir, 'config.json');
      await writeFile(configPath, JSON.stringify({ expo: { enabled: true } }));

      const result = await isConfigScript(configPath);
      expect(result).toBe(false);
    });

    test('should return false for non-.ts files', async () => {
      const configPath = join(testDir, 'config.js');
      await writeFile(
        configPath,
        `export default function() {
          console.log('test');
        }`,
      );

      const result = await isConfigScript(configPath);
      expect(result).toBe(false);
    });

    test('should return false for malformed .ts files', async () => {
      const configPath = join(testDir, 'config-malformed.ts');
      await writeFile(configPath, 'this is not valid typescript!!!');

      const result = await isConfigScript(configPath);
      expect(result).toBe(false);
    });

    test('should return false for .ts file with syntax errors', async () => {
      const configPath = join(testDir, 'config-syntax-error.ts');
      await writeFile(
        configPath,
        `export default function() {
          // Missing closing brace`,
      );

      const result = await isConfigScript(configPath);
      expect(result).toBe(false);
    });
  });

  describe('executeConfigScript', () => {
    test('should execute async function export', async () => {
      const configPath = join(testDir, 'script-async.ts');
      const outputPath = join(testDir, 'output.txt');

      await writeFile(
        configPath,
        `import { writeFile } from 'node:fs/promises';
        export default async function() {
          await writeFile('${outputPath}', 'async executed');
        }`,
      );

      await executeConfigScript(configPath);

      const output = await Bun.file(outputPath).text();
      expect(output).toBe('async executed');
    });

    test('should execute sync function export', async () => {
      const configPath = join(testDir, 'script-sync.ts');
      const outputPath = join(testDir, 'output-sync.txt');

      await writeFile(
        configPath,
        `import { writeFileSync } from 'node:fs';
        export default function() {
          writeFileSync('${outputPath}', 'sync executed');
        }`,
      );

      await executeConfigScript(configPath);

      const output = await Bun.file(outputPath).text();
      expect(output).toBe('sync executed');
    });

    test('should throw error if config does not export a function', async () => {
      const configPath = join(testDir, 'not-a-function.ts');
      await writeFile(
        configPath,
        `export default {
          expo: { enabled: true }
        };`,
      );

      await expect(executeConfigScript(configPath)).rejects.toThrow('Config file at');
      await expect(executeConfigScript(configPath)).rejects.toThrow('does not export a function');
    });

    test('should propagate errors thrown during script execution', async () => {
      const configPath = join(testDir, 'script-error.ts');
      await writeFile(
        configPath,
        `export default async function() {
          throw new Error('Script execution failed');
        }`,
      );

      await expect(executeConfigScript(configPath)).rejects.toThrow('Script execution failed');
    });

    test('should allow script to access node built-ins', async () => {
      const configPath = join(testDir, 'script-with-imports.ts');
      const outputPath = join(testDir, 'import-test.txt');

      await writeFile(
        configPath,
        `import { writeFile } from 'node:fs/promises';

        export default async function() {
          await writeFile('${outputPath}', 'node built-ins work');
        }`,
      );

      await executeConfigScript(configPath);

      const output = await Bun.file(outputPath).text();
      expect(output).toBe('node built-ins work');
    });

    test('should handle script that returns a value', async () => {
      const configPath = join(testDir, 'script-return.ts');
      await writeFile(
        configPath,
        `export default function() {
          return 'some value';
        }`,
      );

      // Should not throw even if function returns a value
      await expect(executeConfigScript(configPath)).resolves.toBeUndefined();
    });
  });

  describe('Script Mode Integration', () => {
    test('should verify example script file exists', async () => {
      const exampleScriptPath = join(process.cwd(), 'examples', 'script-mode.ts');

      // Verify the example file exists
      expect(existsSync(exampleScriptPath)).toBe(true);

      // Read the file to verify it exports a function
      const content = await Bun.file(exampleScriptPath).text();
      expect(content).toContain('export default async function');

      // Note: We can't actually test isConfigScript() on the example file
      // because it imports 'dep-updater' which can't be resolved in the test environment
      // In real usage, this works because the package is installed
    });

    test('should distinguish between declarative config and script mode', async () => {
      // Create declarative config
      const declarativeConfigPath = join(testDir, 'declarative.ts');
      await writeFile(
        declarativeConfigPath,
        `import { defineConfig } from '../../src/config.js';
        export default defineConfig({
          expo: { enabled: true },
          prStrategy: { maxStackDepth: 3 }
        });`,
      );

      // Create script mode config
      const scriptConfigPath = join(testDir, 'script.ts');
      await writeFile(
        scriptConfigPath,
        `export default async function() {
          console.log('Running custom logic');
        }`,
      );

      const isDeclarative = await isConfigScript(declarativeConfigPath);
      const isScript = await isConfigScript(scriptConfigPath);

      expect(isDeclarative).toBe(false);
      expect(isScript).toBe(true);
    });
  });
});
