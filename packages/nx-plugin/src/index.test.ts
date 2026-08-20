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

  it('splits transformed JavaScript emit from native declarations and typechecking', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/example/package.json',
        '{"name":"example","scripts":{"test":"bun test --pass-with-no-tests"}}\n',
      );
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(targets['tsc-js']?.executor).toBe('@smoothbricks/nx-plugin:typescript-emit');
      expect(targets['tsc-js']?.options).toEqual({
        tsConfig: 'tsconfig.lib.json',
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
      expect(targets['tsc-js']?.outputs).toEqual([
        '{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}',
        '{projectRoot}/dist/**/*.tsbuildinfo',
      ]);
      expect(targets.typecheck?.options).toMatchObject({
        command: 'tsc -p tsconfig.lib.json --noEmit',
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
      expect(targets['typecheck-tests']?.dependsOn).toEqual(['tsc-js', 'typecheck']);
      expect(targets['typecheck-tests']?.options).toMatchObject({
        command: 'tsc -p tsconfig.test.json --noEmit',
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
        command: 'tsc -p tsconfig.test.json --noEmit --watch',
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

  it('rebuilds current library declarations before test typechecking after clean', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(targets['typecheck-tests']?.dependsOn).toEqual(['tsc-js', 'typecheck']);
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
        timeoutMs: 1_200_000,
      });
      expect(targets['cargo-test']?.inputs).toEqual([
        '{projectRoot}/**/*.rs',
        '{projectRoot}/**/Cargo.toml',
        '{projectRoot}/**/Cargo.lock',
        '{projectRoot}/**/.cargo/config.toml',
        '!{projectRoot}/**/target/**',
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

  it('infers a cached Cargo Wasm build directly from crate metadata', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/git-do/package.json', '{"name":"git-do"}\n');
      await workspace.write(
        'packages/git-do/crates/git-do/Cargo.toml',
        [
          '[package]',
          'name = "git-do"',
          '',
          '[lib]',
          'name = "gitoxide_engine"',
          'crate-type = ["cdylib", "rlib"]',
          '',
          '[package.metadata.smoothbricks.wasm-bindgen]',
          'targets = ["nodejs", "web"]',
          'out-dir = "generated/wasm"',
          '',
        ].join('\n'),
      );
      await workspace.write('packages/git-do/tsconfig.lib.json', '{"compilerOptions":{"outDir":"dist"}}\n');

      const targets = await inferProjectTargets(workspace, 'packages/git-do/package.json');

      expect(targets['cargo-wasm']).toMatchObject({
        executor: 'nx:run-commands',
        cache: true,
        dependsOn: ['^build'],
        inputs: [
          '{projectRoot}/**/*.rs',
          '{projectRoot}/**/Cargo.toml',
          '{projectRoot}/**/Cargo.lock',
          '{projectRoot}/**/.cargo/config.toml',
          '!{projectRoot}/**/target/**',
          '{projectRoot}/package.json',
          '{workspaceRoot}/bun.lock',
        ],
        outputs: ['{projectRoot}/generated/wasm'],
        options: {
          commands: [
            'cargo build --release --target wasm32-unknown-unknown --target-dir crates/git-do/target/cargo-wasm --manifest-path crates/git-do/Cargo.toml',
            'wasm-bindgen --target nodejs --out-dir generated/wasm/node crates/git-do/target/cargo-wasm/wasm32-unknown-unknown/release/gitoxide_engine.wasm',
            'wasm-bindgen --target web --out-dir generated/wasm/web crates/git-do/target/cargo-wasm/wasm32-unknown-unknown/release/gitoxide_engine.wasm',
          ],
          cwd: 'packages/git-do',
          parallel: false,
        },
      });
      expect(targets['tsc-js']?.dependsOn).toEqual(['^*-js', 'cargo-wasm']);
      expect(targets.typecheck?.dependsOn).toEqual(['^*-js', 'cargo-wasm']);
      expect(targets.build?.dependsOn).toEqual(buildOutputDependencies);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      // A nested crate manifest is enough for output inference, but not for
      // workspace-wide cargo-test/lint policy.
      expect(targets['cargo-test']).toBeUndefined();
      expect(targets['cargo-lint']).toBeUndefined();
    } finally {
      await workspace.cleanup();
    }
  });

  it('infers disjoint host and release targets from canonical N-API metadata', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/cowshed/package.json',
        JSON.stringify({
          name: 'cowshed',
          scripts: { test: 'nx run cowshed:test --outputStyle=stream' },
          napi: {
            binaryName: 'cowshed',
            targets: [
              'aarch64-apple-darwin',
              'x86_64-apple-darwin',
              'aarch64-unknown-linux-gnu',
              'x86_64-unknown-linux-gnu',
            ],
          },
        }),
      );
      await workspace.write('packages/cowshed/Cargo.toml', '[workspace]\nmembers = ["crates/cowshed-napi"]\n');
      await workspace.write(
        'packages/cowshed/crates/cowshed-napi/Cargo.toml',
        '[package]\nname = "cowshed-napi"\n\n[lib]\ncrate-type = ["cdylib"]\n',
      );
      await workspace.write(
        'packages/cowshed/tsconfig.lib.json',
        '{"compilerOptions":{"outDir":"dist/ts","tsBuildInfoFile":"dist/ts/tsconfig.lib.tsbuildinfo"}}\n',
      );
      await workspace.write('packages/cowshed/src/native.test.ts', 'export {};\n');

      const targets = await inferProjectTargets(workspace, 'packages/cowshed/package.json');

      expect(targets['cargo-wasm']).toBeUndefined();
      expect(targets['tsc-js']?.outputs).toEqual(['{projectRoot}/dist/ts']);
      expect(targets['cargo-napi']).toMatchObject({
        executor: 'nx:run-commands',
        cache: true,
        dependsOn: ['^build'],
        outputs: ['{projectRoot}/dist/native/host'],
        options: {
          cwd: 'packages/cowshed',
          command:
            'napi build --release --platform --no-js --dts cowshed.napi.d.ts --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --output-dir dist/native/host',
        },
      });
      expect(targets['napi-arm64-macos']?.outputs).toEqual(['{projectRoot}/dist/native/darwin-arm64']);
      expect(targets['napi-arm64-macos']?.options).toMatchObject({
        command:
          'napi build --release --platform --no-js --dts cowshed.darwin-arm64.d.ts --target aarch64-apple-darwin --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --output-dir dist/native/darwin-arm64',
      });
      expect(targets['napi-arm64-macos']?.options?.env).toBeUndefined();
      expect(targets['napi-x64-linux']?.outputs).toEqual(['{projectRoot}/dist/native/linux-x64-gnu']);
      expect(targets['napi-x64-linux']?.options).toMatchObject({
        command:
          'napi build --release --platform --no-js --dts cowshed.linux-x64-gnu.d.ts --target x86_64-unknown-linux-gnu --use-napi-cross --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --output-dir dist/native/linux-x64-gnu',
        env: { TARGET_CC: 'clang', TARGET_CXX: 'clang++' },
      });
      expect(targets.build?.dependsOn).toEqual(buildOutputDependencies);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(targets['napi-test']).toMatchObject({
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        cache: true,
        dependsOn: ['cargo-test', 'cargo-napi', 'tsc-js', '^build', 'build'],
        options: {
          command: 'bun test --config=../bunfig.toml --timeout=30000 native.test.ts',
          cwd: 'packages/cowshed/src',
          timeoutMs: 120000,
          killAfterMs: 10000,
        },
      });
      await workspace.write('packages/cowshed/bunfig.napi-test.toml', '[test]\n');
      const targetsWithDedicatedBunfig = await inferProjectTargets(workspace, 'packages/cowshed/package.json');
      expect(targetsWithDedicatedBunfig['napi-test']?.options?.command).toBe(
        'bun test --config=../bunfig.napi-test.toml --timeout=30000 native.test.ts',
      );
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
        '[package]\nname = "columine-wasm"\n\n[lib]\ncrate-type = ["cdylib", "rlib"]\n\n[package.metadata.smoothbricks.wasm-bindgen]\ntargets = ["web"]\n',
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
