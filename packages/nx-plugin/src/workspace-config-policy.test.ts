import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { applyWorkspaceConfigPolicy, checkWorkspaceConfigPolicy } from './workspace-config-policy.js';

describe('workspace config policy', () => {
  it('detects missing plugins and fixes them', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
        targetDefaults: {
          build: { cache: true, outputs: ['{projectRoot}/dist'] },
        },
        namedInputs: {
          default: ['{projectRoot}/**/*', 'sharedGlobals'],
          sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
          production: ['{projectRoot}/src/**/*', '{projectRoot}/package.json'],
        },
      });

      const issues = checkWorkspaceConfigPolicy(root);
      const messages = issues.map((i) => i.message);
      expect(messages).toContainEqual(expect.stringContaining('plugins must configure @nx/js/typescript'));
      expect(messages).toContainEqual(expect.stringContaining('plugins must include @smoothbricks/nx-plugin'));

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      const pluginNames = (nxJson.plugins as unknown[]).map((p: unknown) =>
        typeof p === 'string' ? p : (p as Record<string, unknown>).plugin,
      );
      expect(pluginNames).toContain('@nx/js/typescript');
      expect(pluginNames).toContain('@smoothbricks/nx-plugin');

      // Verify @nx/js/typescript has correct build.targetName
      const nxJsPlugin = (nxJson.plugins as unknown[]).find(
        (p: unknown) =>
          typeof p === 'object' && p !== null && (p as Record<string, unknown>).plugin === '@nx/js/typescript',
      ) as Record<string, unknown>;
      expect(nxJsPlugin).toBeDefined();
      const options = nxJsPlugin.options as Record<string, unknown>;
      const build = options.build as Record<string, unknown>;
      expect(build.targetName).toBe('tsc-js');

      // No issues after fix
      expect(checkWorkspaceConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects colon-style target defaults and removes them', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
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
          'build:wasm': { cache: true },
        },
        namedInputs: {
          default: ['{projectRoot}/**/*', 'sharedGlobals'],
          sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
          production: ['{projectRoot}/src/**/*', '{projectRoot}/package.json'],
        },
      });

      const issues = checkWorkspaceConfigPolicy(root);
      const colonIssue = issues.find((i) => i.message.includes('build:wasm'));
      expect(colonIssue).toBeDefined();
      expect(colonIssue!.message).toContain('must not use colon target names');

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      expect(nxJson.targetDefaults['build:wasm']).toBeUndefined();
      expect(nxJson.targetDefaults.build).toBeDefined();

      expect(checkWorkspaceConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('fixes build target default (cache: true, outputs)', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
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
          build: { cache: false },
        },
        namedInputs: {
          default: ['{projectRoot}/**/*', 'sharedGlobals'],
          sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
          production: ['{projectRoot}/src/**/*', '{projectRoot}/package.json'],
        },
      });

      const issues = checkWorkspaceConfigPolicy(root);
      expect(issues.some((i) => i.message.includes('build.cache must be true'))).toBe(true);
      expect(issues.some((i) => i.message.includes('build.outputs'))).toBe(true);

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      expect(nxJson.targetDefaults.build.cache).toBe(true);
      expect(nxJson.targetDefaults.build.outputs).toEqual(['{projectRoot}/dist']);

      expect(checkWorkspaceConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects broad production named inputs and fixes to precise defaults', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
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
        namedInputs: {
          default: ['{projectRoot}/**/*', 'sharedGlobals'],
          sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
          production: ['default', '{projectRoot}/**/*'],
        },
      });

      const issues = checkWorkspaceConfigPolicy(root);
      expect(issues.some((i) => i.message.includes('precise production inputs'))).toBe(true);

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      expect(nxJson.namedInputs.production).toEqual([
        '{projectRoot}/src/**/*',
        '{projectRoot}/package.json',
        '!{projectRoot}/**/__tests__/**',
        '!{projectRoot}/**/*.test.*',
        '!{projectRoot}/**/*.spec.*',
      ]);

      expect(checkWorkspaceConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts custom precise production inputs (Cargo.toml, *.rs paths)', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
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
        namedInputs: {
          default: ['{projectRoot}/**/*', 'sharedGlobals'],
          sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
          production: ['{projectRoot}/src/**/*.rs', '{projectRoot}/Cargo.toml', '!{projectRoot}/**/*.test.*'],
        },
      });

      const issues = checkWorkspaceConfigPolicy(root);
      // No production-related issues since these are precise
      expect(issues.some((i) => i.message.includes('production'))).toBe(false);

      // Apply should not change production
      expect(applyWorkspaceConfigPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('fixes sharedGlobals named input', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
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
        namedInputs: {
          default: ['{projectRoot}/**/*', 'sharedGlobals'],
          sharedGlobals: [],
          production: ['{projectRoot}/src/**/*', '{projectRoot}/package.json'],
        },
      });

      const issues = checkWorkspaceConfigPolicy(root);
      expect(issues.some((i) => i.message.includes('sharedGlobals'))).toBe(true);

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      expect(nxJson.namedInputs.sharedGlobals).toEqual(['{workspaceRoot}/.github/workflows/ci.yml']);

      expect(checkWorkspaceConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('returns no issues for a fully valid config', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-ws-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), validNxJson());

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
    namedInputs: {
      default: ['{projectRoot}/**/*', 'sharedGlobals'],
      sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
      production: [
        '{projectRoot}/src/**/*',
        '{projectRoot}/package.json',
        '!{projectRoot}/**/__tests__/**',
        '!{projectRoot}/**/*.test.*',
        '!{projectRoot}/**/*.spec.*',
      ],
    },
  };
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
