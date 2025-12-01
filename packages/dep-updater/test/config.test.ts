/**
 * Tests for configuration utilities
 */

import { describe, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type DepUpdaterConfig,
  defaultConfig,
  loadConfig,
  mergeConfig,
  sanitizeConfigForLogging,
} from '../src/config.js';
import type { DeepPartial } from '../src/types.js';

describe('sanitizeConfigForLogging', () => {
  test('should redact API key when present', () => {
    const config: DepUpdaterConfig = {
      expo: { enabled: false, packageJsonPath: './package.json' },
      syncpack: { configPath: './.syncpackrc.json', preserveCustomRules: true, fixScriptName: 'syncpack:fix' },
      prStrategy: {
        stackingEnabled: true,
        maxStackDepth: 5,
        autoCloseOldPRs: true,
        resetOnMerge: true,
        stopOnConflicts: true,
        branchPrefix: 'chore/update-deps',
        prTitlePrefix: 'chore: update dependencies',
      },
      autoMerge: { enabled: false, mode: 'none', requireTests: true },
      ai: {
        provider: 'anthropic',
        model: 'claude-sonnet-4-5-20250929',
        apiKey: 'sk-ant-api-key-secret-12345',
      },
      git: { remote: 'origin', baseBranch: 'main' },
    };

    const sanitized = sanitizeConfigForLogging(config);

    expect(sanitized.ai?.apiKey).toBe('***REDACTED***');
    expect(sanitized.ai?.provider).toBe('anthropic');
    expect(sanitized.ai?.model).toBe('claude-sonnet-4-5-20250929');
  });

  test('should handle missing API key gracefully', () => {
    const config: DepUpdaterConfig = {
      expo: { enabled: false, packageJsonPath: './package.json' },
      syncpack: { configPath: './.syncpackrc.json', preserveCustomRules: true, fixScriptName: 'syncpack:fix' },
      prStrategy: {
        stackingEnabled: true,
        maxStackDepth: 5,
        autoCloseOldPRs: true,
        resetOnMerge: true,
        stopOnConflicts: true,
        branchPrefix: 'chore/update-deps',
        prTitlePrefix: 'chore: update dependencies',
      },
      autoMerge: { enabled: false, mode: 'none', requireTests: true },
      ai: {
        provider: 'anthropic',
        model: 'claude-sonnet-4-5-20250929',
      },
      git: { remote: 'origin', baseBranch: 'main' },
    };

    const sanitized = sanitizeConfigForLogging(config);

    expect(sanitized.ai?.apiKey).toBeUndefined();
    expect(sanitized.ai?.provider).toBe('anthropic');
  });

  test('should not modify original config', () => {
    const config: DepUpdaterConfig = {
      expo: { enabled: false, packageJsonPath: './package.json' },
      syncpack: { configPath: './.syncpackrc.json', preserveCustomRules: true, fixScriptName: 'syncpack:fix' },
      prStrategy: {
        stackingEnabled: true,
        maxStackDepth: 5,
        autoCloseOldPRs: true,
        resetOnMerge: true,
        stopOnConflicts: true,
        branchPrefix: 'chore/update-deps',
        prTitlePrefix: 'chore: update dependencies',
      },
      autoMerge: { enabled: false, mode: 'none', requireTests: true },
      ai: {
        provider: 'anthropic',
        model: 'claude-sonnet-4-5-20250929',
        apiKey: 'sk-ant-secret',
      },
      git: { remote: 'origin', baseBranch: 'main' },
    };

    const sanitized = sanitizeConfigForLogging(config);

    // Original config should be unchanged
    expect(config.ai.apiKey).toBe('sk-ant-secret');
    // Sanitized should be redacted
    expect(sanitized.ai?.apiKey).toBe('***REDACTED***');
  });
});

describe('mergeConfig', () => {
  test('should merge partial config with defaults', () => {
    const userConfig = {
      expo: {
        enabled: true,
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.expo?.autoDetect).toBe(true); // From defaults
    expect(merged.expo?.projects).toEqual([]); // From defaults
    expect(merged.prStrategy.stackingEnabled).toBe(true); // From defaults
  });

  test('should merge partial config with explicit projects', () => {
    const userConfig = {
      expo: {
        enabled: true,
        projects: [{ packageJsonPath: './apps/mobile/package.json' }],
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.expo?.projects).toHaveLength(1);
    expect(merged.expo?.projects?.[0]?.packageJsonPath).toBe('./apps/mobile/package.json');
    expect(merged.prStrategy.stackingEnabled).toBe(true); // from defaults
    expect(merged.ai.provider).toBe('opencode'); // from defaults (free tier)
  });

  test('should not introduce undefined in nested objects', () => {
    const userConfig = {
      prStrategy: {
        maxStackDepth: 3,
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.prStrategy.maxStackDepth).toBe(3);
    expect(merged.prStrategy.stackingEnabled).toBe(true); // Should have default
    expect(merged.prStrategy.branchPrefix).toBe('chore/update-deps');
  });

  test('should preserve default values when user config is empty', () => {
    const merged = mergeConfig({});

    expect(merged).toEqual(defaultConfig);
  });

  test('should allow overriding nested config values', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        maxStackDepth: 10,
        branchPrefix: 'update',
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.prStrategy.maxStackDepth).toBe(10);
    expect(merged.prStrategy.branchPrefix).toBe('update');
    expect(merged.prStrategy.stackingEnabled).toBe(true); // preserved from defaults
  });

  test('should allow disabling features', () => {
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        stackingEnabled: false,
        autoCloseOldPRs: false,
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.prStrategy.stackingEnabled).toBe(false);
    expect(merged.prStrategy.autoCloseOldPRs).toBe(false);
  });

  test('should merge multiple Expo projects', () => {
    const userConfig = {
      expo: {
        enabled: true,
        projects: [
          { name: 'mobile', packageJsonPath: './apps/mobile/package.json' },
          { name: 'tablet', packageJsonPath: './apps/tablet/package.json' },
        ],
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.expo?.projects).toHaveLength(2);
    expect(merged.expo?.projects?.[0]?.name).toBe('mobile');
    expect(merged.expo?.projects?.[1]?.packageJsonPath).toBe('./apps/tablet/package.json');
  });

  test('should allow disabling Expo auto-detection', () => {
    const userConfig = {
      expo: {
        enabled: true,
        autoDetect: false,
        projects: [{ packageJsonPath: './apps/mobile/package.json' }],
      },
    };

    const merged = mergeConfig(userConfig);

    expect(merged.expo?.enabled).toBe(true);
    expect(merged.expo?.autoDetect).toBe(false);
    expect(merged.expo?.projects).toHaveLength(1);
  });
});

describe('defaultConfig', () => {
  test('should have sensible defaults', () => {
    expect(defaultConfig.prStrategy.maxStackDepth).toBe(5);
    expect(defaultConfig.prStrategy.branchPrefix).toBe('chore/update-deps');
    expect(defaultConfig.ai.provider).toBe('opencode'); // Free tier by default
    expect(defaultConfig.autoMerge.enabled).toBe(false);
    expect(defaultConfig.git?.baseBranch).toBe('main');
  });
});

describe('loadConfig', () => {
  test('should return defaults when no config file exists', async () => {
    // Create a temporary directory with no config file
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));

    const config = await loadConfig(tmpDir);

    expect(config).toEqual(defaultConfig);
  });

  test('should load and merge config from tooling/dep-updater.json', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    mkdirSync(toolingDir, { recursive: true });

    // Create a config file
    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        maxStackDepth: 10,
        branchPrefix: 'feat/update-deps',
      },
      nix: {
        enabled: true,
      },
    };

    writeFileSync(join(toolingDir, 'dep-updater.json'), JSON.stringify(userConfig, null, 2));

    const config = await loadConfig(tmpDir);

    expect(config.prStrategy.maxStackDepth).toBe(10);
    expect(config.prStrategy.branchPrefix).toBe('feat/update-deps');
    expect(config.nix?.enabled).toBe(true);
    // Should still have defaults for unspecified values
    expect(config.prStrategy.stackingEnabled).toBe(true);
    expect(config.ai.provider).toBe('opencode'); // Free tier by default
  });

  test('should load config from tooling/dep-updater.json', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    mkdirSync(toolingDir, { recursive: true });

    const userConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: {
        maxStackDepth: 3,
      },
    };

    writeFileSync(join(toolingDir, 'dep-updater.json'), JSON.stringify(userConfig, null, 2));

    const config = await loadConfig(tmpDir);

    expect(config.prStrategy.maxStackDepth).toBe(3);
  });

  test('should prefer tooling/dep-updater.ts over tooling/dep-updater.json', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    mkdirSync(toolingDir, { recursive: true });

    const tsConfig = 'export default { prStrategy: { maxStackDepth: 10 } };';
    const jsonConfig: DeepPartial<DepUpdaterConfig> = {
      prStrategy: { maxStackDepth: 5 },
    };

    writeFileSync(join(toolingDir, 'dep-updater.ts'), tsConfig);
    writeFileSync(join(toolingDir, 'dep-updater.json'), JSON.stringify(jsonConfig, null, 2));

    const config = await loadConfig(tmpDir);

    expect(config.prStrategy.maxStackDepth).toBe(10);
  });

  test('should return defaults on invalid JSON', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    mkdirSync(toolingDir, { recursive: true });

    // Write invalid JSON
    writeFileSync(join(toolingDir, 'dep-updater.json'), '{ invalid json }');

    const config = await loadConfig(tmpDir);

    expect(config).toEqual(defaultConfig);
  });

  test('should return defaults on non-object JSON', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    mkdirSync(toolingDir, { recursive: true });

    // Write array instead of object
    writeFileSync(join(toolingDir, 'dep-updater.json'), '["not", "an", "object"]');

    const config = await loadConfig(tmpDir);

    expect(config).toEqual(defaultConfig);
  });

  test('should handle config with all optional fields set', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    mkdirSync(toolingDir, { recursive: true });

    const fullConfig: DeepPartial<DepUpdaterConfig> = {
      expo: {
        enabled: true,
        packageJsonPath: './apps/mobile/package.json',
      },
      syncpack: {
        configPath: './custom-syncpack.json',
        preserveCustomRules: false,
        fixScriptName: 'fix:versions',
      },
      nix: {
        enabled: true,
        devenvPath: './nix',
        nixpkgsOverlayPath: './nix/overlay',
      },
      prStrategy: {
        stackingEnabled: false,
        maxStackDepth: 3,
        autoCloseOldPRs: false,
        resetOnMerge: false,
        stopOnConflicts: false,
        branchPrefix: 'deps',
        prTitlePrefix: 'Update deps',
      },
      autoMerge: {
        enabled: true,
        mode: 'patch',
        requireTests: false,
        minCoverage: 80,
      },
      ai: {
        provider: 'anthropic',
        model: 'claude-opus-4-5-20250929',
        apiKey: 'test-key',
      },
      git: {
        remote: 'upstream',
        baseBranch: 'develop',
      },
      repoRoot: '/custom/path',
    };

    writeFileSync(join(toolingDir, 'dep-updater.json'), JSON.stringify(fullConfig, null, 2));

    const config = await loadConfig(tmpDir);

    expect(config.expo?.enabled).toBe(true);
    expect(config.expo?.packageJsonPath).toBe('./apps/mobile/package.json');
    expect(config.syncpack?.fixScriptName).toBe('fix:versions');
    expect(config.nix?.enabled).toBe(true);
    expect(config.prStrategy.stackingEnabled).toBe(false);
    expect(config.autoMerge.mode).toBe('patch');
    expect(config.ai.model).toBe('claude-opus-4-5-20250929');
    expect(config.git?.baseBranch).toBe('develop');
    expect(config.repoRoot).toBe('/custom/path');
  });

  test('should search parent directories for config file', async () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'dep-updater-test-'));
    const toolingDir = join(tmpDir, 'tooling');
    const subDir = join(tmpDir, 'sub', 'nested');

    // Create config in parent directory
    mkdirSync(toolingDir, { recursive: true });
    writeFileSync(join(toolingDir, 'dep-updater.json'), JSON.stringify({ prStrategy: { maxStackDepth: 7 } }));

    // Create nested subdirectory
    mkdirSync(subDir, { recursive: true });

    // Load from subdirectory - should find parent config
    const config = await loadConfig(subDir);

    expect(config.prStrategy.maxStackDepth).toBe(7);
  });
});
