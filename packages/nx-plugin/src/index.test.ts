import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import type { CreateNodesContextV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { createNodesV2 } from './index.js';

const [, inferTargets] = createNodesV2;

describe('@smoothbricks/nx-plugin inferred targets', () => {
  it('infers validation and aggregate build targets without owning TypeScript lib build', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/example/package.json',
        '{"name":"example","scripts":{"test":"bun test --pass-with-no-tests"}}\n',
      );
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(targets['tsc-js']).toBeUndefined();
      expect(targets.build?.executor).toBe('nx:noop');
      expect(targets.build?.cache).toBe(true);
      expect(targets.build?.dependsOn).toContain('^build');
      expect(targets.build?.dependsOn).toContain('tsc-js');

      expect(targets['typecheck-tests']?.executor).toBe('nx:run-commands');
      expect(targets['typecheck-tests']?.cache).toBe(true);
      expect(targets['typecheck-tests']?.dependsOn).toEqual(['typecheck']);
      expect(targets['typecheck-tests']?.options).toMatchObject({
        command: 'tsc --noEmit -p tsconfig.test.json',
        cwd: 'packages/example',
      });

      expect(targets['typecheck-tests:watch']?.executor).toBe('nx:run-commands');
      expect(targets['typecheck-tests:watch']?.continuous).toBe(true);
      expect(targets['typecheck-tests:watch']?.options).toMatchObject({
        command: 'tsc --noEmit -p tsconfig.test.json --watch',
        cwd: 'packages/example',
      });

      expect(targets['test:watch']?.executor).toBe('nx:run-commands');
      expect(targets['test:watch']?.continuous).toBe(true);
      expect(targets['test:watch']?.dependsOn).toEqual(['typecheck-tests']);
      expect(targets['test:watch']?.options).toMatchObject({
        command: 'bun test --watch --pass-with-no-tests',
        cwd: 'packages/example',
      });

      expect(targets.lint?.executor).toBe('nx:noop');
      expect(targets.lint?.cache).toBe(true);
      expect(targets.lint?.dependsOn).toEqual(['typecheck-tests']);
    } finally {
      await workspace.cleanup();
    }
  });

  it('infers vitest watch targets from explicit test commands', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/example/package.json',
        '{"name":"example","scripts":{"test":"vitest run --coverage"}}\n',
      );
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(targets['test:watch']?.continuous).toBe(true);
      expect(targets['test:watch']?.options).toMatchObject({
        command: 'vitest --coverage',
        cwd: 'packages/example',
      });
    } finally {
      await workspace.cleanup();
    }
  });

  it('infers zig step targets and excludes reserved non-build steps', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/ziggy/package.json', '{"name":"ziggy"}\n');
      await workspace.write(
        'packages/ziggy/build.zig',
        [
          'pub fn build(b: *std.Build) void {',
          '  _ = b.step("wasm", "Build wasm");',
          '  _ = b.step("native", "Build native");',
          '  _ = b.step("test", "Run tests");',
          '  _ = b.step("all", "Build everything");',
          '  _ = b.step("clean", "Clean outputs");',
          '  _ = b.step("install", "Install artifacts");',
          '}',
          '',
        ].join('\n'),
      );

      const targets = await inferProjectTargets(workspace, 'packages/ziggy/package.json');

      expect(Object.keys(targets).sort()).toEqual(['build', 'zig-native', 'zig-wasm']);
      expect(targets.build?.dependsOn).toEqual(['^build', 'zig-wasm', 'zig-native']);
      expect(targets.build?.cache).toBe(true);
      expect(targets['zig-wasm']?.cache).toBe(true);
      expect(targets['zig-native']?.cache).toBe(true);
      expect(targets['zig-wasm']?.options).toEqual({
        command: 'zig build wasm',
        cwd: 'packages/ziggy',
      });
      expect(targets['zig-native']?.options).toEqual({
        command: 'zig build native',
        cwd: 'packages/ziggy',
      });
    } finally {
      await workspace.cleanup();
    }
  });

  it('leaves build.zig without b.step declarations to smoo validation', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/broken/package.json', '{"name":"broken"}\n');
      await workspace.write('packages/broken/build.zig', 'pub fn build(_: *std.Build) void {}\n');

      const targets = await inferProjectTargets(workspace, 'packages/broken/package.json');

      expect(targets).toEqual({});
    } finally {
      await workspace.cleanup();
    }
  });
});

async function createWorkspace(): Promise<WorkspaceFixture> {
  const root = await mkdtemp(join(tmpdir(), 'smoothbricks-nx-plugin-'));

  return {
    context: {
      workspaceRoot: root,
      nxJsonConfiguration: {},
    },
    async write(filePath: string, contents: string): Promise<void> {
      const absolutePath = join(root, filePath);
      await mkdir(dirname(absolutePath), { recursive: true });
      await writeFile(absolutePath, contents);
    },
    async cleanup(): Promise<void> {
      await rm(root, { recursive: true, force: true });
    },
  };
}

interface WorkspaceFixture {
  context: CreateNodesContextV2;
  write(filePath: string, contents: string): Promise<void>;
  cleanup(): Promise<void>;
}

async function inferProjectTargets(
  workspace: WorkspaceFixture,
  packageJsonPath: string,
): Promise<Record<string, TargetConfiguration>> {
  const result = await inferTargets([packageJsonPath], undefined, workspace.context);
  const projectRoot = dirname(packageJsonPath);

  return result[0]?.[1].projects?.[projectRoot]?.targets ?? {};
}
