import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import type { CreateNodesContextV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { createNodesV2 } from './index.js';
import { BUILD_OUTPUT_DEPENDENCIES } from './workspace-config-policy.js';

const [, inferTargets] = createNodesV2;
const buildOutputDependencies = ['^build', ...BUILD_OUTPUT_DEPENDENCIES];

describe('@smoothbricks/nx-plugin inferred targets', () => {
  it('names standalone package projects from package metadata', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'specs/prototype/package.json',
        '{"name":"standalone-package","nx":{"name":"standalone-project"}}\n',
      );
      await workspace.write('specs/package-fallback/package.json', '{"name":"package-fallback"}\n');

      const explicit = await inferProject(workspace, 'specs/prototype/package.json');
      const fallback = await inferProject(workspace, 'specs/package-fallback/package.json');

      expect(explicit?.name).toBe('standalone-project');
      expect(explicit?.targets).toEqual({});
      expect(fallback?.name).toBe('package-fallback');
      expect(fallback?.targets).toEqual({});
    } finally {
      await workspace.cleanup();
    }
  });

  it('skips smoo managed raw package.json sources as projects', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/cli/managed/raw/tooling/typescript-api/package.json',
        '{"name":"@smoothbricks/typescript-api","private":true}\n',
      );
      await workspace.write(
        'tooling/typescript-api/package.json',
        '{"name":"@smoothbricks/typescript-api","private":true}\n',
      );

      const managed = await inferTargets(
        ['packages/cli/managed/raw/tooling/typescript-api/package.json'],
        undefined,
        workspace.context,
      );
      expect(managed[0]?.[1]).toEqual({});

      const live = await inferProject(workspace, 'tooling/typescript-api/package.json');
      expect(live?.name).toBe('@smoothbricks/typescript-api');
    } finally {
      await workspace.cleanup();
    }
  });

  it('overrides inferred TypeScript compiler commands with ttsc', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/example/package.json',
        '{"name":"example","scripts":{"test":"bun test --pass-with-no-tests"}}\n',
      );
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(targets['tsc-js']?.options).toMatchObject({
        command: 'ttsc -p tsconfig.lib.json --emit',
        cwd: 'packages/example',
      });
      const toolchainInputs = [
        '{workspaceRoot}/package.json',
        '{workspaceRoot}/bun.lock',
        '{workspaceRoot}/patches/**/*',
        '{workspaceRoot}/tsconfig.base.json',
      ];
      expect(targets['tsc-js']?.inputs).toEqual([
        'production',
        '^production',
        ...toolchainInputs,
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(targets['tsc-js']?.outputs).toEqual(['{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}']);
      expect(targets.typecheck?.options).toMatchObject({
        command: 'ttsc -p tsconfig.lib.json --noEmit',
        cwd: 'packages/example',
      });
      expect(targets.typecheck?.inputs).toEqual([
        'production',
        '^production',
        ...toolchainInputs,
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(targets.build?.executor).toBe('nx:noop');
      expect(targets.build?.cache).toBe(true);
      expect(targets.build?.dependsOn).toEqual(buildOutputDependencies);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(targets.clean?.cache).toBe(false);

      expect(targets['typecheck-tests']?.executor).toBe('nx:run-commands');
      expect(targets['typecheck-tests']?.cache).toBe(true);
      expect(targets['typecheck-tests']?.dependsOn).toEqual(['typecheck']);
      expect(targets['typecheck-tests']?.options).toMatchObject({
        command: 'ttsc -p tsconfig.test.json --noEmit',
        cwd: 'packages/example',
      });
      expect(targets['typecheck-tests']?.inputs).toEqual([
        'default',
        '^production',
        ...toolchainInputs,
        '{projectRoot}/tsconfig.test.json',
      ]);

      expect(targets['typecheck-tests:watch']?.executor).toBe('nx:run-commands');
      expect(targets['typecheck-tests:watch']?.continuous).toBe(true);
      expect(targets['typecheck-tests:watch']?.options).toMatchObject({
        command: 'ttsc -p tsconfig.test.json --noEmit --watch',
        cwd: 'packages/example',
      });

      expect(targets['test:watch']?.executor).toBe('nx:run-commands');
      expect(targets['test:watch']?.continuous).toBe(true);
      expect(targets['test:watch']?.dependsOn).toEqual(['typecheck-tests']);
      expect(targets['test:watch']?.options).toMatchObject({
        command: 'bun test --watch --pass-with-no-tests',
        cwd: 'packages/example',
      });

      expect(targets.lint?.executor).toBeUndefined();
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

  it('infers aggregate build for package-local output targets without owning them', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/tsdown/package.json',
        JSON.stringify({ name: 'tsdown', nx: { targets: { 'tsdown-js': { executor: 'nx:run-commands' } } } }),
      );

      const targets = await inferProjectTargets(workspace, 'packages/tsdown/package.json');

      expect(targets['tsc-js']).toBeUndefined();
      expect(targets['tsdown-js']).toBeUndefined();
      expect(targets.build?.executor).toBe('nx:noop');
      expect(targets.build?.dependsOn).toEqual(buildOutputDependencies);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps platform-only output families out of the ordinary aggregate build', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/platform/package.json',
        JSON.stringify({
          name: 'platform',
          nx: {
            targets: {
              'bundle-ios': { executor: 'nx:run-commands' },
              'bundle-macos': { executor: 'nx:run-commands' },
              'bundle-linux': { executor: 'nx:run-commands' },
            },
          },
        }),
      );

      const targets = await inferProjectTargets(workspace, 'packages/platform/package.json');

      expect(targets.build).toBeUndefined();
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(BUILD_OUTPUT_DEPENDENCIES).not.toContain('*-ios');
      expect(BUILD_OUTPUT_DEPENDENCIES).not.toContain('*-macos');
      expect(BUILD_OUTPUT_DEPENDENCIES).not.toContain('*-linux');
    } finally {
      await workspace.cleanup();
    }
  });

  //#region smoo!n/rust-output-target-inference
  it('infers generic cargo workspace targets without any rust output targets', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write(
        'packages/ferris/Cargo.toml',
        '[workspace]\nmembers = ["crates/ferris-core", "crates/ferris-wasm"]\n\n[profile.wasm-release]\ninherits = "release"\n',
      );
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');
      await workspace.write(
        'packages/ferris/crates/ferris-wasm/Cargo.toml',
        '[package]\nname = "ferris-wasm"\n\n[lib]\ncrate-type = ["cdylib", "rlib"]\n',
      );

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      // Even a cdylib crate named *-wasm infers no cargo-wasm: rust output
      // targets are declared package-locally, never derived from crate metadata.
      expect(Object.keys(targets).sort()).toEqual(['bench', 'cargo-lint', 'cargo-test', 'lint', 'mutation', 'test']);
      expect(targets['cargo-test']?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(targets['cargo-test']?.options).toMatchObject({
        command: 'cargo test --workspace',
        cwd: 'packages/ferris',
      });
      expect(targets['cargo-test']?.inputs).toEqual([
        '{projectRoot}/**/*.rs',
        '{projectRoot}/**/Cargo.toml',
        '{projectRoot}/Cargo.lock',
        '{projectRoot}/.cargo/config.toml',
        '!{projectRoot}/target/**',
      ]);
      expect(targets['cargo-lint']?.options).toMatchObject({
        commands: ['cargo fmt --all --check', 'cargo clippy --workspace --all-targets -- -D warnings'],
      });
      expect(targets.lint?.dependsOn).toEqual(['cargo-lint']);
      expect(targets.test?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(targets.test?.options).toMatchObject({
        command: 'cargo test --workspace',
        cwd: 'packages/ferris',
      });
      expect(targets.mutation?.cache).toBe(false);
      expect(targets.mutation?.options).toMatchObject({ command: 'cargo mutants --workspace' });
      expect(targets.bench?.options).toMatchObject({ command: 'cargo bench --workspace' });
    } finally {
      await workspace.cleanup();
    }
  });

  it('does not infer wasm targets for native N-API cdylib crates', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/cowshed/package.json', '{"name":"cowshed"}\n');
      await workspace.write('packages/cowshed/Cargo.toml', '[workspace]\nmembers = ["crates/cowshed-napi"]\n');
      await workspace.write(
        'packages/cowshed/crates/cowshed-napi/Cargo.toml',
        '[package]\nname = "cowshed-napi"\n\n[lib]\ncrate-type = ["cdylib"]\n',
      );

      const targets = await inferProjectTargets(workspace, 'packages/cowshed/package.json');

      expect(targets['cargo-wasm']).toBeUndefined();
      expect(targets.build).toBeUndefined();
      expect(targets.clean).toBeUndefined();
      expect(targets['cargo-test']).toBeDefined();
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps an explicit package-local cargo-wasm target singular and feeding the aggregate build', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/columine/package.json',
        JSON.stringify({
          name: 'columine',
          nx: {
            targets: {
              'cargo-wasm': { executor: 'nx:run-commands', outputs: ['{projectRoot}/dist/**/*.wasm'] },
            },
          },
        }),
      );
      await workspace.write(
        'packages/columine/Cargo.toml',
        '[workspace]\nmembers = ["crates/columine-wasm"]\n\n[profile.wasm-release]\ninherits = "release"\n',
      );
      await workspace.write(
        'packages/columine/crates/columine-wasm/Cargo.toml',
        '[package]\nname = "columine-wasm"\n\n[lib]\ncrate-type = ["cdylib", "rlib"]\n',
      );

      const targets = await inferProjectTargets(workspace, 'packages/columine/package.json');

      // The declared target stays the only cargo-wasm: inference never emits a
      // duplicate for Nx to merge, even with a cdylib *-wasm member crate present.
      expect(targets['cargo-wasm']).toBeUndefined();
      // Its *-wasm output-family name feeds the aggregate build and clean.
      expect(buildOutputDependencies).toContain('*-wasm');
      expect(targets.build?.executor).toBe('nx:noop');
      expect(targets.build?.dependsOn).toEqual(buildOutputDependencies);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
    } finally {
      await workspace.cleanup();
    }
  });

  it('lets explicit nx.targets suppress cargo inference and skips non-workspace Cargo.toml', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/custom/package.json',
        '{"name":"custom","nx":{"targets":{"cargo-wasm":{"options":{"command":"custom"}},"test":{}}}}\n',
      );
      await workspace.write('packages/custom/Cargo.toml', '[workspace]\nmembers = ["crates/x-wasm"]\n');
      await workspace.write(
        'packages/custom/crates/x-wasm/Cargo.toml',
        '[package]\nname = "x-wasm"\n\n[lib]\ncrate-type = ["cdylib"]\n',
      );

      const targets = await inferProjectTargets(workspace, 'packages/custom/package.json');
      expect(targets['cargo-wasm']).toBeUndefined();
      expect(targets.test).toBeUndefined();
      expect(targets['cargo-test']).toBeDefined();

      // A member crate's own Cargo.toml (no [workspace]) infers nothing.
      await workspace.write('packages/member/package.json', '{"name":"member"}\n');
      await workspace.write('packages/member/Cargo.toml', '[package]\nname = "member"\n');
      const memberTargets = await inferProjectTargets(workspace, 'packages/member/package.json');
      expect(memberTargets).toEqual({});
    } finally {
      await workspace.cleanup();
    }
  });
  //#endregion
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

async function inferProject(workspace: WorkspaceFixture, packageJsonPath: string) {
  const result = await inferTargets([packageJsonPath], undefined, workspace.context);
  return result[0]?.[1].projects?.[dirname(packageJsonPath)];
}

async function inferProjectTargets(
  workspace: WorkspaceFixture,
  packageJsonPath: string,
): Promise<Record<string, TargetConfiguration>> {
  return (await inferProject(workspace, packageJsonPath))?.targets ?? {};
}
