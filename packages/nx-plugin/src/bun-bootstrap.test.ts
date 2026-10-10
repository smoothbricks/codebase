import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { copyFile, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

describe('Bun bootstrap imports', () => {
  it('imports the main plugin entry point', async () => {
    // Module loading is the boundary under test; the public entry must select the transformed product.
    const plugin = await import('@smoothbricks/nx-plugin');
    expect(plugin.createNodesV2).toBeDefined();
    expect(Array.isArray(plugin.createNodesV2)).toBe(true);
    expect(typeof plugin.preTasksExecution).toBe('function');
    expect(typeof plugin.postTasksExecution).toBe('function');
    expect(typeof plugin.default.preTasksExecution).toBe('function');
    expect(typeof plugin.default.postTasksExecution).toBe('function');
  });

  it('refuses a cold missing bootstrap product instead of returning an empty graph', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-bootstrap-'));
    try {
      await writeFile(join(root, 'package.json'), '{"type":"module"}\n');
      await copyFile(join(import.meta.dir, '../loader.js'), join(root, 'loader.js'));
      const script = `await import(${JSON.stringify(join(root, 'loader.js'))})`;
      const child = spawnSync(process.execPath, ['--no-install', '-e', script], { cwd: root, encoding: 'utf8' });
      expect(child.status).not.toBe(0);
      expect(child.stderr).toContain('The Nx plugin bootstrap product is missing:');
      expect(child.stderr).toContain('ttsc -p packages/nx-plugin/tsconfig.lib.json --emit');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('loads the public policy caller closure without a transform preload', () => {
    const script = [
      'const { fileURLToPath } = await import("node:url");',
      'const { handleImport } = await import("nx/src/utils/handle-import.js");',
      'const root = await handleImport(fileURLToPath(import.meta.resolve("@smoothbricks/nx-plugin")));',
      'if (!Array.isArray(root.createNodesV2)) throw new Error("the compiled plugin entry is incomplete");',
      'const policy = await import("@smoothbricks/nx-plugin/workspace-config-policy");',
      'const bounded = await import("@smoothbricks/nx-plugin/bounded-test-policy");',
      'const targets = await import("@smoothbricks/nx-plugin/package-target-policy");',
      'const eslint = await import("@smoothbricks/nx-plugin/eslint-file-set");',
      'if (!policy.checkWorkspaceConfig({plugins:[{plugin:42}]}).some(issue => issue.message.startsWith("plugins is malformed:"))) throw new Error("the public policy did not validate");',
      'if (typeof bounded.applyBoundedTestTargetPolicy !== "function" || typeof targets.applyPackageTargetPolicy !== "function") throw new Error("the compiled caller closure is incomplete");',
      'if (!eslint.isEslintFileSet({command:"eslint src/fixture.ts",files:["src/fixture.ts"]}) || eslint.isEslintFileSet({command:42,files:[]})) throw new Error("the compiled ESLint file-set owner did not validate");',
    ].join('\n');
    for (const conditions of [[], ['--conditions=development']]) {
      const child = spawnSync(
        process.execPath,
        ['--config=/dev/null', '--no-env-file', '--no-install', ...conditions, '-e', script],
        { cwd: join(import.meta.dir, '../../cli'), encoding: 'utf8' },
      );
      expect(child.status, child.stdout + child.stderr).toBe(0);
    }
  });

  it('imports bounded-test-policy', async () => {
    const policy = await import('./bounded-test-policy.js');
    expect(typeof policy.applyWorkspaceBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.checkWorkspaceBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.applyBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.checkBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.boundedTestScriptAlias).toBe('function');
    expect(typeof policy.resolveTestCommand).toBe('function');
    expect(typeof policy.ensureBunTestTimeoutFlag).toBe('function');
    expect(typeof policy.BOUNDED_TEST_EXECUTOR).toBe('string');
    expect(typeof policy.BOUNDED_TEST_IDLE_TIMEOUT_MS).toBe('number');
    expect(typeof policy.BOUNDED_TEST_KILL_AFTER_MS).toBe('number');
    expect(typeof policy.BOUNDED_TEST_PER_TEST_TIMEOUT_MS).toBe('number');
  });

  it('imports workspace-config-policy', async () => {
    // Module loading is the boundary under test; no test preload may mask a raw public policy entry.
    const policy = await import('@smoothbricks/nx-plugin/workspace-config-policy');
    expect(typeof policy.checkWorkspaceConfigPolicy).toBe('function');
    expect(typeof policy.applyWorkspaceConfigPolicy).toBe('function');
    expect(Array.isArray(policy.BUILD_OUTPUT_DEPENDENCIES)).toBe(true);
    expect(
      policy
        .checkWorkspaceConfig({ plugins: [{ plugin: 42 }] })
        .some((issue) => issue.message.startsWith('plugins is malformed:')),
    ).toBe(true);
  });

  it('imports release-config-policy', async () => {
    const policy = await import('./release-config-policy.js');
    expect(typeof policy.checkReleaseConfigPolicy).toBe('function');
    expect(typeof policy.applyReleaseConfigPolicy).toBe('function');
    expect(typeof policy.SMOO_NX_VERSION_ACTIONS).toBe('string');
    expect(typeof policy.SMOO_NX_RELEASE_TAG_PATTERN).toBe('string');
  });

  it('imports package-target-policy', async () => {
    const policy = await import('./package-target-policy.js');
    expect(typeof policy.checkPackageTargetPolicy).toBe('function');
    expect(typeof policy.applyPackageTargetPolicy).toBe('function');
    expect(typeof policy.nxRunAlias).toBe('function');
    expect(typeof policy.packageNxProjectName).toBe('function');
    expect(Array.isArray(policy.BUILD_OUTPUT_DEPENDENCIES)).toBe(true);
  });
});
