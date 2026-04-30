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
  checkWorkspaceConfig,
  checkWorkspaceConfigPolicy,
  checkWorkspaceConfigTree,
} from './workspace-config-policy.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function validNxJson(): Record<string, unknown> {
  return {
    plugins: [
      {
        plugin: '@nx/js/typescript',
        options: {
          typecheck: { targetName: 'typecheck' },
          build: {
            targetName: 'tsc-js',
            configName: 'tsconfig.lib.json',
            buildDepsName: 'build-deps',
            watchDepsName: 'watch-deps',
          },
        },
      },
      '@smoothbricks/nx-plugin',
    ],
    targetDefaults: {
      build: { cache: true, outputs: ['{projectRoot}/dist'] },
    },
    namedInputs: validNamedInputs(),
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
  return [
    {
      plugin: '@nx/js/typescript',
      options: {
        typecheck: { targetName: 'typecheck' },
        build: {
          targetName: 'tsc-js',
          configName: 'tsconfig.lib.json',
          buildDepsName: 'build-deps',
          watchDepsName: 'watch-deps',
        },
      },
    },
    '@smoothbricks/nx-plugin',
  ];
}

// ---------------------------------------------------------------------------
// Layer 1: Pure core function tests
// ---------------------------------------------------------------------------

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
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
      namedInputs: validNamedInputs(),
    });
    expect(issues.length).toBe(2);
    expect(issues.some((i) => i.message.includes('Official Nx owns TypeScript library inference'))).toBe(true);
    expect(issues.some((i) => i.message.includes('plugins must include @smoothbricks/nx-plugin'))).toBe(true);
  });

  it('detects colon target defaults', () => {
    const issues = checkWorkspaceConfig({
      ...validNxJson(),
      targetDefaults: {
        build: { cache: true, outputs: ['{projectRoot}/dist'] },
        'build:wasm': { cache: true },
      },
    });
    expect(issues.some((i) => i.message.includes('build:wasm'))).toBe(true);
    expect(issues.some((i) => i.message.includes('must not use colon target names'))).toBe(true);
  });

  it('detects wrong tsc-js target name', () => {
    const issues = checkWorkspaceConfig({
      plugins: [
        { plugin: '@nx/js/typescript', options: { build: { targetName: 'build' } } },
        '@smoothbricks/nx-plugin',
      ],
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((i) => i.message.includes('build.targetName must be tsc-js'))).toBe(true);
  });

  it('detects imprecise production inputs', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
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
      targetDefaults: { build: { cache: false } },
      namedInputs: validNamedInputs(),
    });
    expect(issues.some((i) => i.message.includes('build.cache must be true'))).toBe(true);
    expect(issues.some((i) => i.message.includes('build.outputs'))).toBe(true);
  });

  it('detects missing sharedGlobals', () => {
    const issues = checkWorkspaceConfig({
      plugins: validPlugins(),
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
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

  it('fixes missing plugins', () => {
    const nxJson: Record<string, unknown> = {
      plugins: [],
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
      namedInputs: validNamedInputs(),
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    const pluginNames = (nxJson.plugins as unknown[]).map((p: unknown) =>
      typeof p === 'string' ? p : (p as Record<string, unknown>).plugin,
    );
    expect(pluginNames).toContain('@nx/js/typescript');
    expect(pluginNames).toContain('@smoothbricks/nx-plugin');
  });

  it('removes colon target defaults', () => {
    const nxJson = {
      ...validNxJson(),
      targetDefaults: {
        build: { cache: true, outputs: ['{projectRoot}/dist'] },
        'build:wasm': { cache: true },
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    expect((nxJson.targetDefaults as Record<string, unknown>)['build:wasm']).toBeUndefined();
  });

  it('fixes imprecise production inputs', () => {
    const nxJson: Record<string, unknown> = {
      plugins: validPlugins(),
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
      namedInputs: {
        ...validNamedInputs(),
        production: ['default', '{projectRoot}/**/*'],
      },
    };
    expect(applyWorkspaceConfig(nxJson)).toBe(true);
    const namedInputs = nxJson.namedInputs as Record<string, unknown>;
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
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
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
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
      namedInputs: validNamedInputs(),
    });

    const issues = checkWorkspaceConfigTree(tree);
    expect(issues.length).toBe(2);
    expect(issues.some((i) => i.message.includes('Official Nx owns TypeScript library inference'))).toBe(true);
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
      targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } },
      namedInputs: validNamedInputs(),
    });

    expect(applyWorkspaceConfigTree(tree)).toBe(true);

    const nxJson = readJson(tree, 'nx.json');
    const pluginNames = (nxJson.plugins as unknown[]).map((p: unknown) =>
      typeof p === 'string' ? p : (p as Record<string, unknown>).plugin,
    );
    expect(pluginNames).toContain('@nx/js/typescript');
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
    expect((nxJson.targetDefaults as Record<string, unknown>).build).toEqual({
      cache: true,
      outputs: ['{projectRoot}/dist'],
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
        targetDefaults: { build: { cache: false } },
        namedInputs: validNamedInputs(),
      });

      const issues = checkWorkspaceConfigPolicy(root);
      expect(issues.length).toBeGreaterThan(0);
      // Filesystem wrapper uses absolute paths
      expect(issues[0].path).toBe(join(root, 'nx.json'));

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      const pluginNames = (nxJson.plugins as unknown[]).map((p: unknown) =>
        typeof p === 'string' ? p : (p as Record<string, unknown>).plugin,
      );
      expect(pluginNames).toContain('@nx/js/typescript');
      expect(pluginNames).toContain('@smoothbricks/nx-plugin');

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
