import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import type { CreateNodesContextV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { mergeTargetConfigurations } from 'nx/src/project-graph/utils/project-configuration-utils.js';
import { CARGO_CROSS_LINT_COMMAND, CARGO_CROSS_LINT_TARGET, CARGO_LINT_CLIPPY_COMMAND } from './cross-check-policy.js';
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
      expect(Object.keys(targets).sort()).toEqual([
        'bench',
        'cargo-lint',
        'cargo-lint-cross',
        'cargo-sweep',
        'cargo-test',
        'cargo-test-compile',
        'cargo-test-ferris-core',
        'cargo-test-ferris-wasm',
        'lint',
        'mutation',
        'test',
      ]);
      expect(targets['cargo-sweep']?.options).toMatchObject({
        command: 'cargo sweep --time 7',
        cwd: 'packages/ferris',
      });
      expect(targets['cargo-sweep']?.cache).toBe(false);
      expect(targets['cargo-test-compile']?.executor).toBe('nx:run-commands');
      expect(targets['cargo-test-compile']?.options).toMatchObject({
        command: 'cargo --frozen test --workspace --no-run',
        cwd: 'packages/ferris',
      });
      expect(targets['cargo-test']?.executor).toBe('nx:noop');
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-ferris-core', 'cargo-test-ferris-wasm']);
      expect(targets['cargo-test-ferris-core']?.dependsOn).toEqual(['cargo-test-compile']);
      expect(targets['cargo-test-ferris-wasm']?.dependsOn).toEqual(['cargo-test-ferris-core']);
      expect(targets['cargo-test-ferris-core']?.options?.command).toMatch(
        /^cargo --frozen nextest run --package ferris-core --user-config-file none --config-file /,
      );
      expect(targets['cargo-test-ferris-core']?.inputs).toEqual([
        '{projectRoot}/Cargo.toml',
        '{projectRoot}/Cargo.lock',
        '{projectRoot}/crates/ferris-core/**/*.rs',
        '{projectRoot}/crates/ferris-core/Cargo.toml',
        '{projectRoot}/**/.cargo/config.toml',
        '{projectRoot}/scripts/*.sh',
        '!{projectRoot}/**/target/**',
      ]);
      expect(targets['cargo-lint']?.options).toMatchObject({
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
      });
      expect(targets.lint?.dependsOn).toEqual(['cargo-lint']);
      expect(targets[CARGO_CROSS_LINT_TARGET]?.options).toMatchObject({
        command: CARGO_CROSS_LINT_COMMAND,
        cwd: 'packages/ferris',
      });
      expect(targets[CARGO_CROSS_LINT_TARGET]?.cache).toBe(true);
      expect(targets.lint?.dependsOn).not.toContain(CARGO_CROSS_LINT_TARGET);
      expect(targets.test?.executor).toBe('nx:noop');
      expect(targets.test?.dependsOn).toEqual(['cargo-test']);
      expect(targets.mutation?.cache).toBe(false);
      expect(targets.mutation?.options).toMatchObject({ command: 'cargo --frozen mutants --workspace' });
      expect(targets.bench?.options).toMatchObject({ command: 'cargo --frozen bench --workspace' });
    } finally {
      await workspace.cleanup();
    }
  });

  it('scopes each per-crate cargo-test command to exactly one package', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write(
        'packages/ferris/Cargo.toml',
        '[workspace]\nmembers = ["crates/ferris-core", "crates/ferris-wasm"]\n',
      );
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');
      await workspace.write('packages/ferris/crates/ferris-wasm/Cargo.toml', '[package]\nname = "ferris-wasm"\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      expect(cargoPackageSelection(String(targets['cargo-test-ferris-core']?.options?.command ?? ''))).toEqual({
        workspace: false,
        packages: ['ferris-core'],
      });
      expect(cargoPackageSelection(String(targets['cargo-test-ferris-wasm']?.options?.command ?? ''))).toEqual({
        workspace: false,
        packages: ['ferris-wasm'],
      });
      expect(targets['cargo-test-ferris-core']?.dependsOn).toEqual(['cargo-test-compile']);
      expect(targets['cargo-test-ferris-wasm']?.dependsOn).toEqual(['cargo-test-ferris-core']);
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-ferris-core', 'cargo-test-ferris-wasm']);
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
          '{projectRoot}/scripts/*.sh',
          '!{projectRoot}/**/target/**',
          '{projectRoot}/package.json',
          '{workspaceRoot}/bun.lock',
        ],
        outputs: ['{projectRoot}/generated/wasm'],
        options: {
          commands: [
            'cargo --frozen build --release --target wasm32-unknown-unknown --target-dir crates/git-do/target/cargo-wasm --manifest-path crates/git-do/Cargo.toml',
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
          nx: {
            targets: {
              'cli-arm64-macos': { executor: 'nx:run-commands', options: { command: 'true' } },
              'cli-x64-linux': { executor: 'nx:run-commands', options: { command: 'true' } },
            },
          },
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
      // Match the production host suffix so both native-platform and dedicated
      // cargo-napi host-provider regimes are covered on every runner.
      const hostOs = process.platform === 'darwin' ? 'macos' : process.platform === 'linux' ? 'linux' : null;
      const hostArch = process.arch === 'arm64' ? 'arm64' : process.arch === 'x64' ? 'x64' : null;
      const hostSuffix = hostOs !== null && hostArch !== null ? `-${hostArch}-${hostOs}` : null;
      const hostPlatformNapiTarget = hostSuffix === null ? null : `napi${hostSuffix}`;
      const nativeHostProvider = hostPlatformNapiTarget?.endsWith('-macos') ? hostPlatformNapiTarget : null;
      const hostProvider = nativeHostProvider ?? 'cargo-napi';
      const hostProviders = ['cargo-napi', nativeHostProvider].filter(
        (name): name is string => name !== null && targets[name] !== undefined,
      );
      expect(hostProviders).toHaveLength(1);
      expect(hostProviders[0]).toBe(hostProvider);
      if (hostProvider === 'cargo-napi') {
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
      } else {
        expect(targets['cargo-napi']).toBeUndefined();
      }
      expect(targets['napi-arm64-macos']?.outputs).toEqual(['{projectRoot}/dist/native/darwin-arm64']);
      expect(targets['napi-arm64-macos']?.options).toMatchObject({
        command:
          'napi build --release --platform --no-js --dts cowshed.darwin-arm64.d.ts --target aarch64-apple-darwin --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --output-dir dist/native/darwin-arm64',
      });
      expect(targets['napi-arm64-macos']?.options?.env).toBeUndefined();
      expect(targets['napi-arm64-macos']?.dependsOn).toBeUndefined();
      expect(targets['napi-toolchain-arm64-linux']).toEqual({
        executor: '@smoothbricks/nx-plugin:napi-cross-toolchain',
        cache: false,
        options: { triple: 'aarch64-unknown-linux-gnu' },
      });
      expect(targets['napi-toolchain-x64-linux']).toEqual({
        executor: '@smoothbricks/nx-plugin:napi-cross-toolchain',
        cache: false,
        options: { triple: 'x86_64-unknown-linux-gnu' },
      });
      expect(targets['napi-x64-linux']?.dependsOn).toEqual(['napi-toolchain-x64-linux']);
      expect(targets['napi-arm64-linux']?.dependsOn).toEqual(['napi-toolchain-arm64-linux']);
      expect(targets['napi-toolchain-x64-macos']).toBeUndefined();
      expect(targets['napi-x64-linux']?.outputs).toEqual(['{projectRoot}/dist/native/linux-x64-gnu']);
      expect(targets['napi-x64-linux']?.options).toMatchObject({
        command:
          'napi build --release --platform --no-js --dts cowshed.linux-x64-gnu.d.ts --target x86_64-unknown-linux-gnu --use-napi-cross --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --output-dir dist/native/linux-x64-gnu',
        env: { TARGET_CC: 'clang', TARGET_CXX: 'clang++' },
      });
      // The aggregate build pulls in exactly the HOST's platform-suffixed
      // targets (publish still owns foreign platforms).
      const hostNapiTargets = [
        'cli-arm64-macos',
        'cli-x64-linux',
        'napi-arm64-macos',
        'napi-x64-macos',
        'napi-arm64-linux',
        'napi-x64-linux',
      ]
        .filter((name) => hostSuffix !== null && name.endsWith(hostSuffix))
        .sort();
      expect(targets.build?.dependsOn).toEqual([...buildOutputDependencies, ...hostNapiTargets]);
      const platformBuildDependencies = (targets.build?.dependsOn ?? []).filter(
        (dependency): dependency is string =>
          typeof dependency === 'string' && /-(?:arm64|x64)-(?:macos|linux)$/.test(dependency),
      );
      if (hostSuffix !== null) {
        for (const name of platformBuildDependencies) {
          expect(name.endsWith(hostSuffix)).toBe(true);
        }
      } else {
        expect(platformBuildDependencies).toEqual([]);
      }
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      // The test suite runs against the dev-profile addon, never a release
      // platform build: napi-debug shares target/debug with cargo-test, keeps
      // the release binaries off the test critical path, and lives outside
      // dist/ so the wholesale-packaged dist/ tree can never ship it.
      expect(targets['napi-debug']).toMatchObject({
        executor: 'nx:run-commands',
        cache: true,
        dependsOn: ['^build', 'cargo-test-compile'],
        outputs: ['{projectRoot}/.cache/native-debug'],
        options: {
          cwd: 'packages/cowshed',
          command:
            'napi build --platform --no-js --dts cowshed.napi.d.ts --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --output-dir .cache/native-debug',
        },
      });
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-cowshed-napi']);
      expect(targets['cargo-test-cowshed-napi']?.dependsOn).toEqual(['napi-debug']);
      expect(targets['cargo-test-cowshed-napi']?.options?.command).toMatch(
        /^cargo --frozen nextest run --package cowshed-napi --user-config-file none --config-file /,
      );
      expect(targets['napi-test']).toMatchObject({
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        cache: true,
        dependsOn: ['cargo-test', 'napi-debug', 'tsc-js', '^build'],
        options: {
          command: 'bun test --config=../bunfig.toml --timeout=30000 native.test.ts',
          cwd: 'packages/cowshed/src',
          env: { NAPI_DEBUG_ADDON: '1' },
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

  it('lets explicit nx.targets suppress output and aggregate inference, and skips non-workspace Cargo.toml', async () => {
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

  it('keeps inferring a cargo target the package partially declares', async () => {
    const workspace = await createWorkspace();
    const declared: Record<string, TargetConfiguration> = {
      // One added edge per target. Everything that makes each target do its job
      // — executor, options, inputs, cache, configurations — must survive the
      // declaration instead of being replaced wholesale by it.
      'cargo-test': { dependsOn: ['cargo-test-compile', 'cargo-wasm'] },
      'cargo-test-compile': { dependsOn: ['cargo-wasm'] },
      'cargo-lint': { inputs: ['{projectRoot}/clippy.toml'] },
      mutation: { options: { command: 'cargo mutants --in-diff' } },
      bench: { cache: true },
    };
    try {
      await workspace.write(
        'packages/ferris/package.json',
        JSON.stringify({ name: 'ferris', nx: { targets: declared } }),
      );
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = []\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      // Inference must still emit the base. Without one Nx has nothing to merge
      // the declaration onto and normalizes a bare `dependsOn` to `nx:noop` with
      // empty options — a cargo-test that passes having run no test binary.
      expect(targets['cargo-test']?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(targets['cargo-test-compile']?.executor).toBe('nx:run-commands');
      expect(targets['cargo-lint']?.options).toMatchObject({
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
      });

      const cargoTest = resolveDeclaredOverInferred(targets, declared, 'cargo-test');
      expect(cargoTest?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(cargoTest?.cache).toBe(true);
      expect(cargoTest?.dependsOn).toEqual(['cargo-test-compile', 'cargo-wasm']);
      expect(cargoTest?.options).toEqual({
        command: 'cargo --frozen test --workspace',
        cwd: 'packages/ferris',
        timeoutMs: 120000,
        killAfterMs: 10000,
      });
      expect(cargoTest?.configurations).toEqual({
        production: { command: 'cargo --frozen test --workspace --release' },
      });
      expect(cargoTest?.inputs).toContain('{projectRoot}/**/*.rs');

      const cargoTestCompile = resolveDeclaredOverInferred(targets, declared, 'cargo-test-compile');
      expect(cargoTestCompile?.executor).toBe('nx:run-commands');
      expect(cargoTestCompile?.dependsOn).toEqual(['cargo-wasm']);
      expect(cargoTestCompile?.options).toMatchObject({
        command: 'cargo --frozen test --workspace --no-run',
      });

      // A declared array replaces the inferred one; the command it guards stays.
      const cargoLint = resolveDeclaredOverInferred(targets, declared, 'cargo-lint');
      expect(cargoLint?.inputs).toEqual(['{projectRoot}/clippy.toml']);
      expect(cargoLint?.options).toMatchObject({
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
      });

      // `options` merges key by key, so a declared command overrides only itself.
      const mutation = resolveDeclaredOverInferred(targets, declared, 'mutation');
      expect(mutation?.options).toEqual({ command: 'cargo mutants --in-diff', cwd: 'packages/ferris' });
      expect(mutation?.cache).toBe(false);

      const bench = resolveDeclaredOverInferred(targets, declared, 'bench');
      expect(bench?.cache).toBe(true);
      expect(bench?.options).toMatchObject({ command: 'cargo --frozen bench --workspace' });
    } finally {
      await workspace.cleanup();
    }
  });

  it('lets a declared dependsOn spread expand the inferred cargo chain', async () => {
    const workspace = await createWorkspace();
    // Additive intent has Nx's own spelling: `'...'` expands the inferred list
    // at the token, so an added edge keeps the cargo serialization chain.
    const declared: Record<string, TargetConfiguration> = {
      'cargo-test': { dependsOn: ['...', 'cargo-wasm'] },
    };
    try {
      await workspace.write(
        'packages/ferris/package.json',
        JSON.stringify({ name: 'ferris', nx: { targets: declared } }),
      );
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = []\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      // The spread expands exactly once, against the single inferred base this
      // plugin emits — re-implementing the overlay here would double it.
      const cargoTest = resolveDeclaredOverInferred(targets, declared, 'cargo-test');
      expect(cargoTest?.dependsOn).toEqual(['cargo-test-compile', 'cargo-wasm']);
      expect(cargoTest?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
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

/**
 * The configuration Nx resolves for a target this plugin infers and the package
 * also declares. Nx merges a package-local `nx.targets` entry over an inference
 * plugin's target one layer later — the precedence order is specified plugins →
 * target defaults → default plugins, and the package.json reader is a default
 * plugin — so composing with the same merge function the graph uses makes these
 * assertions the resolved configuration rather than a guess about it.
 */
function resolveDeclaredOverInferred(
  targets: Record<string, TargetConfiguration>,
  declared: Record<string, TargetConfiguration>,
  name: string,
): TargetConfiguration | undefined {
  const inferred = targets[name];
  return inferred && mergeTargetConfigurations(declared[name] ?? {}, inferred);
}

/**
 * `--workspace` selects every member; `--package` names a crate. Both together
 * is the defect: nextest keeps the workspace set and the package flag does not
 * narrow it. A per-crate target is scoped iff it names exactly one package and
 * does not also select the workspace.
 */
function cargoPackageSelection(command: string): { workspace: boolean; packages: string[] } {
  const tokens = command.split(/\s+/);
  const packages: string[] = [];
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i] === '--package' || tokens[i] === '-p') {
      const name = tokens[i + 1];
      if (name !== undefined && name.length > 0) {
        packages.push(name);
      }
    }
  }
  return { workspace: tokens.includes('--workspace'), packages };
}
