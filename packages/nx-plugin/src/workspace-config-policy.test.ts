import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { readJson, writeJson } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import {
  applyWorkspaceConfig,
  applyWorkspaceConfigPolicy,
  applyWorkspaceConfigTree,
  BUILD_OUTPUT_DEPENDENCIES,
  checkWorkspaceConfig,
  checkWorkspaceConfigPolicy,
  checkWorkspaceConfigTree,
  LINUX_PLATFORM_TARGET_GLOBS,
  MACOS_PLATFORM_TARGET_GLOBS,
  PLATFORM_TARGET_GLOBS,
} from './workspace-config-policy.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function validNxJson(): Record<string, unknown> {
  return {
    plugins: ['@smoothbricks/nx-plugin'],
    targetDefaults: validTargetDefaults(),
    namedInputs: validNamedInputs(),
  };
}

function validTargetDefaults(): Record<string, unknown> {
  return {
    build: { cache: true, outputs: ['{projectRoot}/dist'] },
    clean: { executor: '@smoothbricks/nx-plugin:clean-outputs', cache: false },
  };
}

function validNamedInputs(): Record<string, unknown> {
  return {
    default: ['{projectRoot}/**/*', 'sharedGlobals'],
    sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
    production: [
      '{projectRoot}/src/**/*',
      '{projectRoot}/package.json',
      '!{projectRoot}/**/__tests__/**',
      '!{projectRoot}/**/*.test.*',
      '!{projectRoot}/**/*.spec.*',
    ],
  };
}

function validPlugins(): unknown[] {
  return ['@smoothbricks/nx-plugin'];
}

// ---------------------------------------------------------------------------
// Layer 1: Pure core function tests
// ---------------------------------------------------------------------------

describe('target family conventions', () => {
  it('separates platform-only families from ordinary build outputs', () => {
    expect(MACOS_PLATFORM_TARGET_GLOBS).toEqual(['*-macos', '*-ios']);
    expect(LINUX_PLATFORM_TARGET_GLOBS).toEqual(['*-linux']);
    expect(PLATFORM_TARGET_GLOBS).toEqual(['*-macos', '*-ios', '*-linux']);
    expect(BUILD_OUTPUT_DEPENDENCIES).toEqual([
      '*-js',
      '*-web',
      '*-html',
      '*-css',
      '*-android',
      '*-native',
      '*-napi',
      '*-bun',
      '*-wasm',
    ]);
  });
});

describe('pure core: checkWorkspaceConfig', () => {
  it('returns no issues for valid config', () => {
    const issues = checkWorkspaceConfig(validNxJson());
    expect(issues).toEqual([]);
  });

  it('uses nx.json as path in issues', () => {
    const issues = checkWorkspaceConfig({});
    for (const issue of issues) {
      expect(issue.path).toBe('nx.json');
    }
  });

  it('detects missing plugins', () => {
    const issues = checkWorkspaceConfig({
      plugins: [],
      targetDefaults: validTargetDefaults(),
      namedInputs: validNamedInputs(),
    });
    expect(issues.length).toBe(1);
    expect(issues.some((i) => i.message.includes('plugins must include @smoothbricks/nx-plugin'))).toBe(true);
  });

  it('detects colon target defaults', () => {
    const issues = checkWorkspaceConfig({
      ...validNxJson(),
      targetDefaults: {
        ...validTargetDefaults(),
        'build:wasm': { cache: true },
      },
    });
    expect(issues.some((i) => i.message.includes('build:wasm'))).toBe(true);
    expect(issues.some((i) => i.message.includes('must not use colon target names'))).toBe(true);
  });

  it('rejects static test typecheck defaults that override per-project inference', () => {
    const issues = checkWorkspaceConfig({
      ...validNxJson(),
      targetDefaults: {
        ...validTargetDefaults(),
        'typecheck-tests': {
          dependsOn: ['typecheck'],
          options: { command: 'ttsc -p tsconfig.test.json --noEmit' },
        },
      },
    });

    expect(
      issues.some((issue) => issue.message.includes('targetDefaults.typecheck-tests must not be configured')),
    ).toBe(true);
  });

  it('detects native TypeScript inference', () => {
    const issues = checkWorkspaceConfig({
      plugins: ['@nx/js/typescript', '@smoothbricks/nx-plugin'],
      targetDefaults: validTargetDefaults(),
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((issue) => issue.message.includes('plugins must not configure @nx/js/typescript'))).toBe(true);
  });

  it('detects imprecise production inputs', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: validTargetDefaults(),
      namedInputs: {
        ...validNamedInputs(),
        production: ['default'],
      },
    });
    expect(issues.some((i) => i.message.includes('enumerate precise production inputs'))).toBe(true);
  });

  it('detects missing build cache', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: {
        build: { cache: false },
        clean: { executor: '@smoothbricks/nx-plugin:clean-outputs', cache: false },
      },
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((i) => i.message.includes('build.cache must be true'))).toBe(true);
    expect(issues.some((i) => i.message.includes('build.outputs'))).toBe(true);
  });

  it('accepts additional project-root dist siblings in build outputs', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: {
        ...validTargetDefaults(),
        build: { cache: true, outputs: ['{projectRoot}/dist', '{projectRoot}/dist-node'] },
      },
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((i) => i.message.includes('build.outputs'))).toBe(false);
  });

  it('rejects build outputs missing the canonical dist tree', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: {
        ...validTargetDefaults(),
        build: { cache: true, outputs: ['{projectRoot}/dist-node'] },
      },
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((i) => i.message.includes('build.outputs'))).toBe(true);
  });

  it('rejects build outputs outside the project-root dist family', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: {
        ...validTargetDefaults(),
        build: { cache: true, outputs: ['{projectRoot}/dist', '{workspaceRoot}/out'] },
      },
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((i) => i.message.includes('build.outputs'))).toBe(true);
  });

  it('detects missing sharedGlobals', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: validTargetDefaults(),
      namedInputs: {
        ...validNamedInputs(),
        sharedGlobals: [],
      },
    });
    expect(issues.some((i) => i.message.includes('sharedGlobals'))).toBe(true);
  });
});

describe('pure core: applyWorkspaceConfig', () => {
  it('returns false for already-valid config', () => {
    const nxJson = validNxJson();
    expect(applyWorkspaceConfig(nxJson)).toBe(false);
  });

  it('preserves valid extra dist siblings in build outputs', () => {
    const nxJson = {
      ...validNxJson(),
      targetDefaults: {
        ...validTargetDefaults(),
        build: { cache: true, outputs: ['{projectRoot}/dist', '{projectRoot}/dist-node'] },
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(false);
    const build = expectRecord(expectRecord(nxJson.targetDefaults).build);
    expect(build.outputs).toEqual(['{projectRoot}/dist', '{projectRoot}/dist-node']);
  });

  it('resets invalid build outputs to the canonical dist tree', () => {
    const nxJson = {
      ...validNxJson(),
      targetDefaults: {
        ...validTargetDefaults(),
        build: { cache: true, outputs: ['{workspaceRoot}/out'] },
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    const build = expectRecord(expectRecord(nxJson.targetDefaults).build);
    expect(build.outputs).toEqual(['{projectRoot}/dist']);
  });

  it('fixes missing plugins', () => {
    const nxJson: Record<string, unknown> = {
      plugins: [],
      targetDefaults: validTargetDefaults(),
      namedInputs: validNamedInputs(),
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    const pluginNames = readPluginNames(nxJson.plugins);
    expect(pluginNames).not.toContain('@nx/js/typescript');
    expect(pluginNames).toContain('@smoothbricks/nx-plugin');
  });

  it('removes colon target defaults', () => {
    const nxJson = {
      ...validNxJson(),
      targetDefaults: {
        ...validTargetDefaults(),
        'build:wasm': { cache: true },
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    expect(expectRecord(nxJson.targetDefaults)['build:wasm']).toBeUndefined();
  });

  it('removes static test typecheck defaults so inference owns clean-output prerequisites', () => {
    const nxJson = {
      ...validNxJson(),
      targetDefaults: {
        ...validTargetDefaults(),
        'typecheck-tests': {
          dependsOn: ['typecheck'],
          options: { command: 'ttsc -p tsconfig.test.json --noEmit' },
        },
      },
    };

    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    expect(expectRecord(nxJson.targetDefaults)['typecheck-tests']).toBeUndefined();
  });

  it('fixes imprecise production inputs', () => {
    const nxJson: Record<string, unknown> = {
      plugins: validPlugins(),
      targetDefaults: validTargetDefaults(),
      namedInputs: {
        ...validNamedInputs(),
        production: ['default', '{projectRoot}/**/*'],
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    const namedInputs = expectRecord(nxJson.namedInputs);
    expect(namedInputs.production).toEqual([
      '{projectRoot}/src/**/*',
      '{projectRoot}/package.json',
      '!{projectRoot}/**/__tests__/**',
      '!{projectRoot}/**/*.test.*',
      '!{projectRoot}/**/*.spec.*',
    ]);
  });

  it('accepts custom precise production inputs unchanged', () => {
    const nxJson: Record<string, unknown> = {
      plugins: validPlugins(),
      targetDefaults: validTargetDefaults(),
      namedInputs: {
        ...validNamedInputs(),
        production: ['{projectRoot}/src/**/*.rs', '{projectRoot}/Cargo.toml', '!{projectRoot}/**/*.test.*'],
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Layer 2: Tree-based function tests
// ---------------------------------------------------------------------------

describe('Tree: checkWorkspaceConfigTree', () => {
  it('returns issue when nx.json missing', () => {
    const tree = createTreeWithEmptyWorkspace();
    // createTreeWithEmptyWorkspace creates an nx.json, remove it
    tree.delete('nx.json');
    const issues = checkWorkspaceConfigTree(tree);
    expect(issues).toHaveLength(1);
    expect(issues[0].message).toBe('nx.json not found');
  });

  it('detects missing plugins', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', {
      plugins: [],
      targetDefaults: validTargetDefaults(),
      namedInputs: validNamedInputs(),
    });

    const issues = checkWorkspaceConfigTree(tree);
    expect(issues.length).toBe(1);
    expect(issues.some((i) => i.message.includes('plugins must include @smoothbricks/nx-plugin'))).toBe(true);
  });

  it('returns no issues for valid config', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', validNxJson());

    expect(checkWorkspaceConfigTree(tree)).toEqual([]);
  });
});

describe('Tree: applyWorkspaceConfigTree', () => {
  it('returns false when nx.json missing', () => {
    const tree = createTreeWithEmptyWorkspace();
    tree.delete('nx.json');
    expect(applyWorkspaceConfigTree(tree)).toBe(false);
  });

  it('fixes missing plugins and writes back to tree', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', {
      plugins: [],
      targetDefaults: validTargetDefaults(),
      namedInputs: validNamedInputs(),
    });

    expect(applyWorkspaceConfigTree(tree)).toBe(true);

    const nxJson = readJson(tree, 'nx.json');
    const pluginNames = readPluginNames(nxJson.plugins);
    expect(pluginNames).not.toContain('@nx/js/typescript');
    expect(pluginNames).toContain('@smoothbricks/nx-plugin');

    // Tree version now passes check
    expect(checkWorkspaceConfigTree(tree)).toEqual([]);
  });

  it('returns false when config already valid', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', validNxJson());

    expect(applyWorkspaceConfigTree(tree)).toBe(false);
  });

  it('fixes build target defaults via tree', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', {
      plugins: validPlugins(),
      targetDefaults: { build: { cache: false } },
      namedInputs: validNamedInputs(),
    });

    expect(applyWorkspaceConfigTree(tree)).toBe(true);

    const nxJson = readJson(tree, 'nx.json');
    expect(expectRecord(nxJson.targetDefaults).build).toEqual({
      cache: true,
      outputs: ['{projectRoot}/dist'],
    });
    expect(expectRecord(nxJson.targetDefaults).clean).toEqual({
      executor: '@smoothbricks/nx-plugin:clean-outputs',
      cache: false,
    });
  });
});

// ---------------------------------------------------------------------------
// Layer 3: Filesystem wrapper integration test
// ---------------------------------------------------------------------------

describe('filesystem: checkWorkspaceConfigPolicy / applyWorkspaceConfigPolicy', () => {
  it('round-trips check/apply on real temp directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJsonFile(join(root, 'nx.json'), {
        plugins: [],
        targetDefaults: {
          build: { cache: false },
          'typecheck-tests': {
            dependsOn: ['typecheck'],
            options: { command: 'ttsc -p tsconfig.test.json --noEmit' },
          },
        },
        namedInputs: validNamedInputs(),
      });

      const issues = checkWorkspaceConfigPolicy(root);
      expect(issues.length).toBeGreaterThan(0);
      // Filesystem wrapper uses absolute paths
      expect(issues[0].path).toBe(join(root, 'nx.json'));

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = expectRecord(JSON.parse(await readFile(join(root, 'nx.json'), 'utf8')));
      const pluginNames = readPluginNames(nxJson.plugins);
      expect(pluginNames).not.toContain('@nx/js/typescript');
      expect(pluginNames).toContain('@smoothbricks/nx-plugin');
      expect(expectRecord(nxJson.targetDefaults)['typecheck-tests']).toBeUndefined();

      // No issues after fix
      expect(checkWorkspaceConfigPolicy(root)).toEqual([]);
      expect(applyWorkspaceConfigPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports issue when nx.json is missing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      const issues = checkWorkspaceConfigPolicy(root);
      expect(issues).toHaveLength(1);
      expect(issues[0].message).toBe('nx.json not found or invalid');

      expect(applyWorkspaceConfigPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Test-local file helper (for filesystem integration tests only)
// ---------------------------------------------------------------------------

async function writeJsonFile(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function expectRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error('expected object');
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function readPluginNames(value: unknown): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error('expected plugins array');
  }
  const plugins: unknown[] = value;
  return plugins.map((plugin) => (typeof plugin === 'string' ? plugin : expectRecord(plugin).plugin));
}
