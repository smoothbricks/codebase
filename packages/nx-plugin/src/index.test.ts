import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { CreateNodesContextV2, CreateNodesV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';
import { createTargetDefaultsResults } from 'nx/src/project-graph/utils/project-configuration/target-defaults.js';
import { mergeTargetConfigurations } from 'nx/src/project-graph/utils/project-configuration-utils.js';
import { BOUNDED_TEST_TIMEOUT_MS } from './bounded-test-policy.js';
import { exceptionalTestFilter } from './cargo-workspace.js';
import { CARGO_CROSS_LINT_COMMAND, CARGO_CROSS_LINT_TARGET, CARGO_LINT_CLIPPY_COMMAND } from './cross-check-policy.js';
import { createNodesV2, createNodesV2ForPlatform } from './index.js';
import { applyWorkspaceConfig } from './workspace-config-policy.js';

const [, inferTargets] = createNodesV2;

describe('@smoothbricks/nx-plugin inferred targets', () => {
  it('runs Rust-only lint without ESLint and checks JavaScript when sources appear', async () => {
    const workspace = await createWorkspace();
    const root = workspace.context.workspaceRoot;
    const repositoryRoot = fileURLToPath(new URL('../../../', import.meta.url));
    try {
      await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
      await workspace.write('package.json', '{"name":"lint-workspace","private":true,"workspaces":["packages/*"]}\n');
      await workspace.write(
        'nx.json',
        JSON.stringify({
          plugins: [fileURLToPath(new URL('../dist/index.js', import.meta.url))],
          namedInputs: { default: ['{projectRoot}/**/*'] },
          targetDefaults: { lint: { cache: true } },
        }),
      );
      await workspace.write(
        'biome.json',
        '{"formatter":{"enabled":false},"linter":{"rules":{"suspicious":{"noDebugger":"off"}}}}\n',
      );
      await workspace.write(
        'eslint.config.mjs',
        'export default [{ files: ["**/*.{js,ts}"], rules: { "no-debugger": "error" } }];\n',
      );
      await workspace.write('Cargo.toml', '[workspace]\nmembers = ["packages/rust"]\nresolver = "2"\n');
      await workspace.write('Cargo.lock', 'version = 4\n\n[[package]]\nname = "lint-crate"\nversion = "0.1.0"\n');
      await workspace.write('packages/rust/package.json', '{"name":"rust-fixture"}\n');
      await workspace.write(
        'packages/rust/Cargo.toml',
        '[package]\nname = "lint-crate"\nversion = "0.1.0"\nedition = "2021"\n',
      );
      await workspace.write('packages/rust/src/lib.rs', 'pub fn answer() -> u32 {\n    42\n}\n');
      const runLint = async () => {
        const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'run', 'rust-fixture:lint'], {
          cwd: root,
          env: {
            ...process.env,
            PATH: `${join(repositoryRoot, 'node_modules/.bin')}:${process.env.PATH ?? ''}`,
            NX_DAEMON: 'false',
            NX_ISOLATE_PLUGINS: 'false',
            NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx/workspace-data'),
            NX_CACHE_DIRECTORY: join(root, '.nx/cache'),
            RUSTC_WRAPPER: '',
            RUSTC_WORKSPACE_WRAPPER: '',
          },
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const [exitCode, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]);
        return { exitCode, output: stdout + stderr };
      };
      const rustOnly = await runLint();
      expect(rustOnly).toMatchObject({ exitCode: 0 });
      expect(rustOnly.output).toContain('cargo-lint');
      await workspace.write('packages/rust/src/check.js', 'debugger;\n');
      const withJavaScript = await runLint();
      expect(withJavaScript.exitCode).not.toBe(0);
      expect(withJavaScript.output).toContain('no-debugger');
      await workspace.write(
        'packages/rust/package.json',
        JSON.stringify({
          name: 'rust-fixture',
          nx: { targets: { lint: { executor: 'nx:run-commands', options: { command: 'printf explicit-lint' } } } },
        }),
      );
      const overridden = await runLint();
      expect(overridden).toMatchObject({ exitCode: 0 });
      expect(overridden.output).toContain('explicit-lint');
    } finally {
      await workspace.cleanup();
    }
  }, 120_000);

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

  it('never infers a project from a package.json a build wrote', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/cowshed/package.json', '{"name":"cowshed","private":true}\n');
      // napi's platform build, a dist tree, cargo's target dir, a package cache: every
      // one of these is written by a task, and a graph recomputed while it is being
      // written must not see a second `cowshed` and drop the real one.
      const outputs = [
        'packages/cowshed/.cache/native-debug/package.json',
        'packages/cowshed/dist/npm/linux-x64-gnu/package.json',
        'packages/cowshed/target/debug/build/x/out/package.json',
        'packages/cowshed/node_modules/dep/package.json',
      ];
      for (const path of outputs) {
        await workspace.write(path, '{"name":"cowshed","private":true}\n');
      }

      const inferred = await inferTargets(outputs, undefined, workspace.context);
      for (const [, result] of inferred) {
        expect(result).toEqual({});
      }
      const live = await inferProject(workspace, 'packages/cowshed/package.json');
      expect(live?.name).toBe('cowshed');
    } finally {
      await workspace.cleanup();
    }
  });

  it('splits transformed JavaScript emit from native declarations and typechecking', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/example/package.json',
        '{"name":"example","bin":{"example":"./dist/bin/example.js"},"scripts":{"test":"bun test --pass-with-no-tests"}}\n',
      );
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(targets['tsc-js']?.executor).toBe('@smoothbricks/nx-plugin:typescript-emit');
      expect(targets['tsc-js']?.options).toEqual({
        executableOutputs: ['./dist/bin/example.js'],
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
      expect(targets['tsc-js']?.outputs).toEqual(['{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}']);
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
      expect(targets.build?.dependsOn).toEqual(['^build', 'tsc-js']);
      // An `nx:noop` aggregate writes no file, so it must claim none: a
      // `{projectRoot}/dist` claim here would double-cache its children's bytes
      // and make `github-ci nx-run-many --collect-outputs` attribute every file
      // under dist to `build`, including the platform artifacts that collect
      // deliberately leaves to the platform step.
      expect(targets.build?.outputs).toBeUndefined();
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
      expect(targets.build?.dependsOn).toEqual(['^build', 'tsdown-js']);
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
      // No ordinary aggregate above is the whole assertion: a platform suffix
      // is not an output family, so `bundle-ios`/`-macos`/`-linux` give this
      // project nothing for `build` to aggregate — only a `clean` to own their
      // outputs.
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
      expect(targets['cargo-wasm']).toBeUndefined();
      expect(targets['cargo-napi']).toBeUndefined();
      expect(targets['cargo-sweep']?.options).toMatchObject({
        command: 'cargo sweep --time 7',
        cwd: 'packages/ferris',
      });
      expect(targets['cargo-sweep']?.cache).toBe(false);
      // Never cached: the download lands in CARGO_HOME, so a cache hit would
      // report success over a registry that still cannot resolve the lockfile.
      expect(targets['cargo-fetch']?.cache).toBe(false);
      expect(targets['cargo-fetch']?.options).toEqual({
        commands: ['cargo fetch --locked'],
        cwd: 'packages/ferris',
        parallel: false,
      });
      // Every frozen cargo command carries the edge, not just the head of the
      // serialization chain: offline cargo fails at resolution, so each one
      // needs the locked graph downloaded whether or not the chain runs first.
      for (const name of [
        'cargo-test-compile',
        'cargo-test-archive',
        'cargo-lint',
        CARGO_CROSS_LINT_TARGET,
        'mutation',
        'bench',
      ]) {
        expect(targets[name]?.dependsOn).toContain('cargo-fetch');
      }
      expect(targets['cargo-sweep']?.dependsOn).toBeUndefined();
      // The archive and the compile it stands for hash every crate's sources
      // AND the outputs of the targets they depend on: generated crate inputs
      // are gitignored, so a repository that had to restate `inputs` to add
      // them replaced the crate union with the root's `default` and shipped a
      // stale archive to every per-crate test.
      for (const name of ['cargo-test-compile', 'cargo-test-archive', 'cargo-lint', CARGO_CROSS_LINT_TARGET]) {
        const inputs = targets[name]?.inputs ?? [];
        expect(inputs).toContainEqual({ dependentTasksOutputFiles: '**/*', transitive: false });
        expect(inputs).toContain('{projectRoot}/crates/ferris-core/**/*');
      }
      expect(targets['cargo-test-compile']?.executor).toBe('nx:run-commands');
      expect(targets['cargo-test-compile']?.cache).toBe(false);
      expect(targets['cargo-test-compile']?.outputs).toEqual([]);
      expect(targets['cargo-test-compile']?.options).toMatchObject({
        command: 'cargo --frozen test --workspace --no-run',
        cwd: 'packages/ferris',
      });
      expect(targets['cargo-test']?.executor).toBe('nx:noop');
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-ferris-core', 'cargo-test-ferris-wasm']);
      // One workspace compile, packed once. Every per-crate runner reads that
      // archive, so the runners fan out instead of chaining: none of them
      // builds, re-resolves features, or writes cargo's flocked `target/`.
      expect(targets['cargo-test-archive']?.executor).toBe('nx:run-commands');
      expect(targets['cargo-test-archive']?.cache).toBe(true);
      expect(targets['cargo-test-archive']?.outputs).toEqual(['{projectRoot}/target/nextest/archive.tar.zst']);
      expect(targets['cargo-test-archive']?.options).toMatchObject({ cwd: 'packages/ferris', parallel: false });
      // nextest does not create the archive's parent directory and fails the
      // whole build if it is missing (measured: "error writing to archive").
      expect(targets['cargo-test-archive']?.options?.commands?.[0]).toBe('mkdir -p target/nextest');
      // `--tool-config-file`, never `--config-file`: the plugin's settings must
      // sit UNDER the repository's `.config/nextest.toml`, which is the only
      // place an `archive.include` for a cdylib or fixture can be declared.
      // `$PWD` because tool config paths must be absolute and an absolute path
      // in the command text would split one cache entry per checkout.
      expect(String(targets['cargo-test-archive']?.options?.commands?.[1])).toMatch(
        /^cargo --frozen nextest archive --workspace --archive-file target\/nextest\/archive\.tar\.zst --user-config-file none --tool-config-file "smoo:\$PWD\/.*nextest\.toml"$/,
      );
      expect(targets['cargo-test-archive']?.inputs).toContain('{projectRoot}/.config/nextest.toml');
      expect(String(targets['cargo-test-archive']?.configurations?.production?.commands?.[1])).toContain(
        'nextest archive --workspace --release --archive-file',
      );
      expect(targets['cargo-test-ferris-core']?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      expect(targets['cargo-test-ferris-wasm']?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      // The runner executes the archive's binaries and remaps the workspace
      // root: without `--workspace-remap`, a restored archive hands tests the
      // PRODUCING tree's absolute CARGO_MANIFEST_DIR (measured on a moved tree).
      // `--workspace` is not merely redundant here, nextest rejects it with
      // `--archive-file`.
      expect(targets['cargo-test-ferris-core']?.options?.command).toMatch(
        /^cargo --frozen nextest run --archive-file target\/nextest\/archive\.tar\.zst --workspace-remap \. -E 'package\(ferris-core\)' --no-tests=pass --user-config-file none --tool-config-file "smoo:\$PWD\/.*nextest\.toml"$/,
      );
      expect(targets['cargo-test-ferris-core']?.inputs).toContain('{projectRoot}/.config/nextest.toml');
      expect(targets['cargo-test-ferris-core']?.configurations?.production).toEqual({});
      expect(targets['cargo-test-ferris-core']?.inputs).toEqual(
        expect.arrayContaining([
          '{projectRoot}/Cargo.toml',
          '{projectRoot}/Cargo.lock',
          '{projectRoot}/crates/ferris-core/**/*',
        ]),
      );
      expect(targets['cargo-test-ferris-core']?.inputs).not.toContain('{projectRoot}/crates/ferris-wasm/**/*');
      // One clippy for the workspace in one target dir: a per-crate closure
      // compiled every shared dependency once per crate.
      expect(targets['cargo-lint']?.options).toMatchObject({
        cwd: 'packages/ferris',
        commands: [
          'cargo fmt --all --check',
          'cargo --frozen clippy --workspace --all-targets --target-dir target/cargo-lint -- -D warnings',
        ],
      });
      expect(targets['cargo-lint']?.cache).toBe(true);
      expect(targets['cargo-lint']?.outputs).toEqual([]);
      expect(targets['cargo-lint']?.inputs).toEqual(
        expect.arrayContaining(['{projectRoot}/crates/ferris-core/**/*', '{projectRoot}/crates/ferris-wasm/**/*']),
      );
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

  it('cleans every cargo directory its own commands write, including the ones caching refuses', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = ["crates/ferris-core"]\n');
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      // A clippy target dir is not an Nx output — restoring an 11 GiB build
      // tree from a cache is worse than rebuilding it — so `clean` cannot find
      // these through `outputs`. They are read back off the commands that
      // write them, which is why a new `--target-dir` cannot be forgotten here.
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(targets.clean?.cache).toBe(false);
      expect(targets.clean?.options?.outputs).toEqual([
        '{projectRoot}/dist',
        '{workspaceRoot}/packages/ferris/target/cargo-lint',
        '{workspaceRoot}/packages/ferris/target/cargo-lint-cross',
        '{workspaceRoot}/packages/ferris/target/nextest',
      ]);
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps Rust-only lint source-aware after migrating defaults without replacing mixed or custom lint', async () => {
    const workspace = await createWorkspace();
    const root = 'packages/ferris';
    const defaults = {
      targetDefaults: {
        lint: {
          executor: 'nx:run-commands',
          options: { commands: ['biome check {projectRoot}', 'eslint {projectRoot}/src'] },
        },
      },
    };
    applyWorkspaceConfig(defaults);
    const resolveLint = (targets: Record<string, TargetConfiguration>, declared: TargetConfiguration = {}) => {
      let lint = targets.lint ?? {};
      const synthesized = createTargetDefaultsResults(
        { [root]: { root, targets } },
        { [root]: { root, targets: { lint: declared } } },
        defaults,
      );
      for (const [, , result] of synthesized) {
        const target = result.projects?.[root]?.targets?.lint;
        if (target) lint = mergeTargetConfigurations(target, lint);
      }
      return mergeTargetConfigurations(declared, lint);
    };
    try {
      await workspace.write('biome.json', '{}\n');
      await workspace.write('eslint.config.mjs', 'export default [{ files: ["**/*.ts"] }];\n');
      await workspace.write(`${root}/package.json`, '{"name":"ferris"}\n');
      await workspace.write(`${root}/Cargo.toml`, '[workspace]\nmembers = ["crates/core"]\n');
      await workspace.write(`${root}/crates/core/Cargo.toml`, '[package]\nname = "ferris-core"\n');
      const rust = await inferProjectTargets(workspace, `${root}/package.json`);
      const rustLint = resolveLint(rust);
      expect(rustLint.executor).toBe('nx:run-commands');
      expect(rustLint.dependsOn).toEqual(['cargo-lint']);
      expect(rustLint.options?.commands).toEqual([`biome check --files-ignore-unknown=true '${root}'`]);

      const custom = { executor: 'nx:run-commands', options: { command: 'custom-linter package.json' } };
      await workspace.write(
        `${root}/package.json`,
        JSON.stringify({ name: 'ferris', nx: { targets: { lint: custom } } }),
      );
      const declared = await inferProjectTargets(workspace, `${root}/package.json`);
      expect(resolveLint(declared, custom).options?.command).toBe(custom.options.command);

      await workspace.write(`${root}/package.json`, '{"name":"ferris"}\n');
      await workspace.write(`${root}/tsconfig.lib.json`, '{"compilerOptions":{"outDir":"dist"}}\n');
      await workspace.write(`${root}/src/index.ts`, 'export const value = 1;\n');
      const mixed = await inferProjectTargets(workspace, `${root}/package.json`);
      expect(resolveLint(mixed).options?.commands).toEqual([
        `biome check --files-ignore-unknown=true '${root}'`,
        `eslint '${root}/src/index.ts'`,
      ]);
    } finally {
      await workspace.cleanup();
    }
  });

  it('attributes one repository-root Cargo workspace across package projects', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('package.json', '{"name":"@fixture/codebase"}\n');
      await workspace.write('packages/runtime/package.json', '{"name":"@fixture/runtime","nx":{"name":"runtime"}}\n');
      await workspace.write('packages/wasm/package.json', '{"name":"@fixture/wasm","nx":{"name":"wasm"}}\n');
      await workspace.write(
        'packages/native/package.json',
        JSON.stringify({
          name: '@fixture/native',
          nx: { name: 'native' },
          napi: {
            binaryName: 'native',
            targets: ['aarch64-apple-darwin'],
          },
        }),
      );
      await workspace.write(
        'Cargo.toml',
        [
          '[workspace]',
          'members = ["packages/runtime/crates/*", "packages/wasm", "packages/native/crates/*"]',
          '',
        ].join('\n'),
      );
      await workspace.write('Cargo.lock', 'version = 4\n');
      await workspace.write(
        'packages/runtime/crates/runtime-core/Cargo.toml',
        '[package]\nname = "runtime-core"\n\n[package.metadata.smoothbricks.test]\nshards = 2\n',
      );
      await workspace.write(
        'packages/wasm/Cargo.toml',
        [
          '[package]',
          'name = "runtime-wasm"',
          '',
          '[lib]',
          'crate-type = ["cdylib", "rlib"]',
          '',
          '[package.metadata.smoothbricks.wasm-bindgen]',
          'targets = ["web"]',
          '',
        ].join('\n'),
      );
      await workspace.write(
        'packages/native/crates/native-napi/Cargo.toml',
        '[package]\nname = "native-napi"\n\n[lib]\ncrate-type = ["cdylib"]\n',
      );

      const inferred = await inferTargets(
        ['package.json', 'packages/runtime/package.json', 'packages/wasm/package.json', 'packages/native/package.json'],
        undefined,
        workspace.context,
      );
      const projects = new Map(inferred.flatMap(([, result]) => Object.entries(result.projects ?? {})));
      const root = projects.get('.')?.targets ?? {};
      const runtime = projects.get('packages/runtime')?.targets ?? {};
      const wasm = projects.get('packages/wasm')?.targets ?? {};
      const native = projects.get('packages/native')?.targets ?? {};
      const rootProject = '@fixture/codebase';
      const rootFetch = { projects: [rootProject], target: 'cargo-fetch' };
      const rootCompile = { projects: [rootProject], target: 'cargo-test-compile' };
      const rootArchive = { projects: [rootProject], target: 'cargo-test-archive' };

      expect(
        [...projects]
          .filter(([, project]) => project.targets?.['cargo-test-compile'])
          .map(([projectRoot]) => projectRoot),
      ).toEqual(['.']);
      expect(root['cargo-fetch']?.options).toEqual({
        commands: ['cargo fetch --locked'],
        cwd: '.',
        parallel: false,
      });
      expect(root['cargo-test-compile']?.options).toMatchObject({
        command: 'cargo --frozen test --workspace --no-run',
        cwd: '.',
      });
      expect(root['cargo-lint']?.options?.commands).toEqual([
        'cargo fmt --all --check',
        'cargo --frozen clippy --workspace --all-targets --target-dir target/cargo-lint -- -D warnings',
      ]);
      expect(root['cargo-lint']?.dependsOn).toContain('cargo-fetch');
      expect(root['cargo-test']?.dependsOn).toHaveLength(5);

      // Root mode: every piece of every project hangs off the one root archive,
      // in any order. The crate is selected by filterset alone — `-p` would
      // re-resolve features, and there is nothing left to build anyway.
      expect(runtime['cargo-test-runtime-core-shard1']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-shard2']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-exceptions']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-shard1']?.options).toMatchObject({ cwd: '.' });
      expect(runtime['cargo-test-runtime-core-shard1']?.options?.command).toContain(
        "nextest run --archive-file target/nextest/archive.tar.zst --workspace-remap . -E 'package(runtime-core)",
      );
      expect(native['cargo-test-native-napi']?.options?.command).toContain('--no-tests=pass');
      expect(runtime['cargo-test']?.dependsOn).toEqual([
        'cargo-test-runtime-core-shard1',
        'cargo-test-runtime-core-shard2',
        'cargo-test-runtime-core-exceptions',
      ]);
      // A package owning crates validates through the root's one clippy verdict.
      expect(runtime['cargo-lint']?.dependsOn).toEqual([{ projects: [rootProject], target: 'cargo-lint' }]);
      expect(runtime.lint?.dependsOn).toEqual(['cargo-lint']);
      expect(runtime['cargo-lint-runtime-core']).toBeUndefined();
      expect(runtime.clean).toBeUndefined();

      expect(wasm['cargo-test-runtime-wasm']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(wasm['cargo-wasm']?.options).toMatchObject({ cwd: '.' });
      expect(wasm['cargo-wasm']?.options?.commands).toContain(
        'cargo --frozen build --release --target wasm32-unknown-unknown --target-dir target/cargo-wasm -p runtime-wasm',
      );
      expect(wasm['cargo-wasm']?.outputs).toEqual(['{projectRoot}/dist-wasm']);
      expect(wasm['cargo-wasm']?.dependsOn).toEqual([rootFetch, '^build']);
      expect(wasm.build?.dependsOn).toContainEqual(rootCompile);
      expect(wasm.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');

      // The crate whose project also builds the debug cdylib keeps that edge:
      // its tests load the artifact `napi-debug` writes into `target/debug`.
      expect(native['cargo-test-native-napi']?.dependsOn).toEqual([rootFetch, rootArchive, 'napi-debug']);
      expect(native['napi-debug']?.dependsOn).toContainEqual(rootCompile);
      // The addon depends on the package's napi block and the napi CLI's version,
      // never on the package version or the whole lockfile: a release bumps both
      // before it builds, and that must not recompile the native code.
      const napiInputs = native['napi-debug']?.inputs ?? [];
      expect(napiInputs).toContainEqual({ externalDependencies: ['@napi-rs/cli'] });
      expect(
        napiInputs.some((input) => typeof input === 'object' && 'runtime' in input && input.runtime.includes('.napi')),
      ).toBe(true);
      expect(napiInputs).not.toContain('{workspaceRoot}/bun.lock');
      expect(napiInputs.some((input) => typeof input === 'string' && input.endsWith('/package.json'))).toBe(false);
      expect(native['napi-debug']?.options).toMatchObject({
        cwd: '.',
        command:
          'packages/native/node_modules/.bin/napi build --platform --no-js --dts native.napi.d.ts --manifest-path packages/native/crates/native-napi/Cargo.toml --package native-napi --package-json-path packages/native/package.json --output-dir packages/native/.cache/native-debug',
      });
      expect(native['napi-arm64-macos']?.options).toMatchObject({
        cwd: '.',
        command:
          'packages/native/node_modules/.bin/napi build --release --platform --no-js --dts native.darwin-arm64.d.ts --target aarch64-apple-darwin --manifest-path packages/native/crates/native-napi/Cargo.toml --package native-napi --package-json-path packages/native/package.json --output-dir packages/native/dist/native/darwin-arm64',
      });
      expect(native.build?.dependsOn).toContainEqual(rootCompile);
      expect(native.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps root packages, nested workspaces and multiple owned crates in their own lint graph', async () => {
    const workspace = await createWorkspace();
    const declared: Record<string, TargetConfiguration> = {
      'cargo-lint': { dependsOn: ['...', 'prepare-check'] },
      lint: { dependsOn: ['cargo-lint', 'custom-check'] },
    };
    try {
      await workspace.write('package.json', '{"name":"outer-root"}\n');
      await workspace.write(
        'Cargo.toml',
        '[workspace]\nmembers = ["packages/group/crates/*"]\nexclude = ["nested"]\n\n[package]\nname = "host-core"\n',
      );
      await workspace.write(
        'packages/group/package.json',
        JSON.stringify({ name: 'group', nx: { targets: declared } }),
      );
      await workspace.write('packages/group/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/group/crates/alpha/Cargo.toml', '[package]\nname = "alpha-core"\n');
      await workspace.write('packages/group/crates/beta/Cargo.toml', '[package]\nname = "beta-wasm"\n');
      await workspace.write('nested/package.json', '{"name":"inner-root"}\n');
      await workspace.write(
        'nested/Cargo.toml',
        '[workspace]\nmembers = ["modules/leaf"]\n\n[package]\nname = "inner-host"\n',
      );
      await workspace.write('nested/modules/leaf/package.json', '{"name":"inner-leaf-project"}\n');
      await workspace.write('nested/modules/leaf/Cargo.toml', '[package]\nname = "inner-leaf"\n');
      const inferred = await inferTargets(
        ['package.json', 'packages/group/package.json', 'nested/package.json', 'nested/modules/leaf/package.json'],
        undefined,
        workspace.context,
      );
      const projects = new Map(inferred.flatMap(([, result]) => Object.entries(result.projects ?? {})));
      const outer = projects.get('.')?.targets ?? {};
      const group = projects.get('packages/group')?.targets ?? {};
      const inner = projects.get('nested')?.targets ?? {};
      const leaf = projects.get('nested/modules/leaf')?.targets ?? {};
      // Each cargo workspace root runs one clippy over all its members; the
      // packages owning crates validate through that verdict.
      expect(outer['cargo-lint']?.options).toMatchObject({
        cwd: '.',
        commands: [
          'cargo fmt --all --check',
          'cargo --frozen clippy --workspace --all-targets --target-dir target/cargo-lint -- -D warnings',
        ],
      });
      expect(outer['cargo-lint']?.inputs).toEqual(
        expect.arrayContaining([
          '{projectRoot}/packages/group/crates/alpha/**/*',
          '{projectRoot}/packages/group/crates/beta/**/*',
        ]),
      );
      expect(group['cargo-lint']?.dependsOn).toEqual([{ projects: ['outer-root'], target: 'cargo-lint' }]);
      expect(group.build?.dependsOn).toEqual([
        '^build',
        { projects: ['outer-root'], target: 'cargo-test-compile' },
        'tsc-js',
      ]);
      expect(outer['cargo-lint-host-core']).toBeUndefined();
      expect(inner['cargo-lint']?.options).toMatchObject({ cwd: 'nested' });
      expect(inner['cargo-lint']?.inputs).toContain('{projectRoot}/**/*');
      expect(inner['cargo-lint']?.inputs).not.toContain('{workspaceRoot}/packages/group/crates/alpha/**/*');
      expect(leaf['cargo-lint']?.dependsOn).toEqual([{ projects: ['inner-root'], target: 'cargo-lint' }]);
      expect(leaf['cargo-lint-inner-leaf']).toBeUndefined();
      expect(leaf['cargo-test-inner-leaf']?.options?.cwd).toBe('nested');
      expect(inner['cargo-test-compile']?.options?.cwd).toBe('nested');
      expect(leaf['cargo-test-inner-leaf']?.dependsOn).toEqual([
        { projects: ['inner-root'], target: 'cargo-fetch' },
        { projects: ['inner-root'], target: 'cargo-test-archive' },
      ]);
      // The profile lives in the archive, so the runner's production
      // configuration has nothing left to say.
      expect(leaf['cargo-test-inner-leaf']?.configurations?.production).toEqual({});
      expect(inner['cargo-test-archive']?.options?.cwd).toBe('nested');
      expect(resolveDeclaredOverInferred(group, declared, 'cargo-lint')).toMatchObject({
        executor: 'nx:noop',
        cache: true,
        outputs: [],
        dependsOn: [{ projects: ['outer-root'], target: 'cargo-lint' }, 'prepare-check'],
      });
      expect(resolveDeclaredOverInferred(group, declared, 'lint')?.dependsOn).toEqual(['cargo-lint', 'custom-check']);
    } finally {
      await workspace.cleanup();
    }
  });

  it('invalidates cached lint verdicts for Cargo target, profile and global configuration changes', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('package.json', '{"name":"cache-fixture"}\n');
      await workspace.write('Cargo.toml', '[workspace]\nmembers = ["crate"]\n');
      await workspace.write('crate/Cargo.toml', '[package]\nname = "cache-crate"\n');
      await workspace.write('cargo-home/config.toml', '[build]\njobs = 1\n');
      const targets = await inferProjectTargets(workspace, 'package.json');
      const runtimeCommands = (targets['cargo-lint']?.inputs ?? []).flatMap((input) =>
        typeof input !== 'string' && 'runtime' in input ? [input.runtime] : [],
      );
      const capture = async (extraEnv: Record<string, string>) =>
        Promise.all(
          runtimeCommands.map(async (command) => {
            const child = Bun.spawn(['sh', '-c', command], {
              cwd: workspace.context.workspaceRoot,
              env: { ...process.env, CARGO_HOME: join(workspace.context.workspaceRoot, 'cargo-home'), ...extraEnv },
              stdout: 'pipe',
              stderr: 'pipe',
            });
            const [exitCode, stdout, stderr] = await Promise.all([
              child.exited,
              new Response(child.stdout).text(),
              new Response(child.stderr).text(),
            ]);
            expect({ exitCode, stderr }).toMatchObject({ exitCode: 0 });
            return stdout;
          }),
        );
      const baseline = await capture({});
      expect(await capture({ CARGO_BUILD_TARGET: 'wasm32-unknown-unknown' })).not.toEqual(baseline);
      expect(await capture({ CARGO_PROFILE_DEV_OPT_LEVEL: '2' })).not.toEqual(baseline);
      expect(await capture({ UNRELATED_FIXTURE_VARIABLE: 'changed' })).toEqual(baseline);
      await workspace.write('cargo-home/config.toml', '[build]\njobs = 2\n');
      expect(await capture({})).not.toEqual(baseline);
    } finally {
      await workspace.cleanup();
    }
  }, 30_000);

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

      // Scoped by filterset over one archived workspace build: exactly one
      // crate runs, and no `-p` re-resolves its features.
      expect(cargoPackageSelection(String(targets['cargo-test-ferris-core']?.options?.command ?? ''))).toEqual({
        archiveRun: true,
        packageFlags: [],
        filtered: ['ferris-core'],
      });
      expect(cargoPackageSelection(String(targets['cargo-test-ferris-wasm']?.options?.command ?? ''))).toEqual({
        archiveRun: true,
        packageFlags: [],
        filtered: ['ferris-wasm'],
      });
      expect(targets['cargo-test-ferris-core']?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      expect(targets['cargo-test-ferris-wasm']?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
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

      // A release bumps package.json and the lockfile before it builds; neither
      // is part of a wasm artifact, so neither may be an input.
      expect(targets['cargo-wasm']?.inputs).not.toContain('{projectRoot}/package.json');
      expect(targets['cargo-wasm']?.inputs).not.toContain('{workspaceRoot}/bun.lock');
      expect(targets['cargo-wasm']).toMatchObject({
        executor: 'nx:run-commands',
        cache: true,
        dependsOn: ['cargo-fetch', '^build'],
        inputs: expect.arrayContaining(['{projectRoot}/**/*.rs', '{projectRoot}/**/Cargo.toml']),
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
      // A wasm-bindgen crate in a nested Cargo workspace still needs its locked
      // graph downloaded, and the project root is not a member of it, so the
      // fetch names the crate manifest instead of relying on cwd.
      expect(targets['cargo-fetch']?.options).toEqual({
        commands: ['cargo fetch --locked --manifest-path crates/git-do/Cargo.toml'],
        cwd: 'packages/git-do',
        parallel: false,
      });
      expect(targets['tsc-js']?.dependsOn).toEqual(['^*-js', 'cargo-wasm']);
      expect(targets.typecheck?.dependsOn).toEqual(['^*-js', 'cargo-wasm']);
      expect(targets.build?.dependsOn).toEqual(['^build', 'cargo-wasm', 'tsc-js']);
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
      // This fixture declares all four supported triples, so on EVERY supported
      // runner the host's own triple is present and native: the host provider is
      // that platform target and `cargo-napi` is never inferred. Tying the native
      // regime to `-macos` asserted a macOS-only model and failed on Linux with
      // zero host providers found. The dedicated `cargo-napi` regime is covered
      // below against a platform no declared triple can serve, which reaches it
      // on every runner instead of only on Linux.
      const hostOs = process.platform === 'darwin' ? 'macos' : process.platform === 'linux' ? 'linux' : null;
      const hostArch = process.arch === 'arm64' ? 'arm64' : process.arch === 'x64' ? 'x64' : null;
      const hostSuffix = hostOs !== null && hostArch !== null ? `-${hostArch}-${hostOs}` : null;
      expect(hostSuffix).not.toBeNull();
      expect(targets[`napi${hostSuffix}`]).toBeDefined();
      expect(targets['cargo-napi']).toBeUndefined();

      // A platform outside NAPI_TARGET_CONVENTIONS leaves no native host target,
      // which is precisely the condition that infers the dedicated host build.
      const unservedHostTargets = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('win32', 'x64'),
      );
      expect(unservedHostTargets['cargo-napi']).toMatchObject({
        executor: 'nx:run-commands',
        cache: true,
        // The dedicated host build is another cargo writer on the default
        // `target/`, and `build` lists it beside cargo-test-compile.
        dependsOn: ['^build', 'cargo-test-compile'],
        outputs: ['{projectRoot}/dist/native/host'],
        options: {
          cwd: 'packages/cowshed',
          command:
            'napi build --release --platform --no-js --dts cowshed.napi.d.ts --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir dist/native/host',
        },
      });
      expect(unservedHostTargets.build?.dependsOn).toEqual(['^build', 'cargo-test-compile', 'cargo-napi', 'tsc-js']);
      expect(targets['napi-arm64-macos']?.outputs).toEqual(['{projectRoot}/dist/native/darwin-arm64']);
      expect(targets['napi-arm64-macos']?.options).toMatchObject({
        command:
          'napi build --release --platform --no-js --dts cowshed.darwin-arm64.d.ts --target aarch64-apple-darwin --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir dist/native/darwin-arm64',
      });
      expect(targets['napi-arm64-macos']?.options?.env).toBeUndefined();
      // No cargo-test-compile edge here, on ANY host: a platform target's
      // dependency closure may only reach its own family
      // (`validatePlatformTargetDependencies`), so this pair serializes on
      // cargo's flock rather than on a graph edge.
      expect(targets['napi-arm64-macos']?.dependsOn).toBeUndefined();
      // A macOS triple never gets a cross toolchain: `usesNapiCross` is
      // `family === 'linux' && target !== host`, so the family decides this one
      // and no host can change it.
      expect(targets['napi-toolchain-x64-macos']).toBeUndefined();
      // Everything else about the cross regime depends on WHICH host is
      // inferring — on an x64 Linux runner `napi-x64-linux` is the native build,
      // with no `--use-napi-cross` and no toolchain prerequisite at all. Those
      // facts are asserted against forced platforms below instead of against
      // whichever machine happens to run the suite.

      // Exercise both host branches explicitly: this suite usually runs on
      // Darwin, but Linux's native compiler selection is the contract at risk.
      const linuxX64Targets = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('linux', 'x64'),
      );
      expect(linuxX64Targets['napi-x64-linux']?.options?.command).not.toContain('--use-napi-cross');
      expect(linuxX64Targets['napi-x64-linux']?.options?.env).toEqual({ CC: 'cc', CXX: 'c++' });
      expect(linuxX64Targets['napi-x64-linux']?.dependsOn).toBeUndefined();
      expect(linuxX64Targets['napi-toolchain-x64-linux']).toBeUndefined();
      expect(linuxX64Targets['napi-arm64-linux']?.options?.command).toContain('--use-napi-cross');
      expect(linuxX64Targets['napi-arm64-linux']?.options?.env).toEqual({
        TARGET_CC: 'clang',
        TARGET_CXX: 'clang++',
      });
      expect(linuxX64Targets['napi-debug']?.options?.env).toEqual({ CC: 'cc', CXX: 'c++' });

      const darwinArm64Targets = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('darwin', 'arm64'),
      );
      expect(darwinArm64Targets['napi-x64-linux']?.options?.command).toContain('--use-napi-cross');
      expect(darwinArm64Targets['napi-x64-linux']?.options?.env).toEqual({
        TARGET_CC: 'clang',
        TARGET_CXX: 'clang++',
      });
      expect(darwinArm64Targets['napi-x64-linux']?.dependsOn).toEqual(['napi-toolchain-x64-linux']);
      expect(darwinArm64Targets['napi-debug']?.options?.env).toBeUndefined();
      expect(darwinArm64Targets['napi-toolchain-arm64-linux']).toEqual({
        executor: '@smoothbricks/nx-plugin:napi-cross-toolchain',
        cache: false,
        options: { triple: 'aarch64-unknown-linux-gnu' },
      });
      expect(darwinArm64Targets['napi-toolchain-x64-linux']).toEqual({
        executor: '@smoothbricks/nx-plugin:napi-cross-toolchain',
        cache: false,
        options: { triple: 'x86_64-unknown-linux-gnu' },
      });
      expect(darwinArm64Targets['napi-arm64-linux']?.dependsOn).toEqual(['napi-toolchain-arm64-linux']);
      expect(darwinArm64Targets['napi-x64-linux']?.outputs).toEqual(['{projectRoot}/dist/native/linux-x64-gnu']);
      expect(darwinArm64Targets['napi-x64-linux']?.options?.command).toBe(
        'napi build --release --platform --no-js --dts cowshed.linux-x64-gnu.d.ts --target x86_64-unknown-linux-gnu --use-napi-cross --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir dist/native/linux-x64-gnu',
      );

      // The whole rule, stated once per forced host: a triple gets a cross
      // toolchain exactly when it is a linux triple that is not this host's own.
      // Asserting the pair by name is what broke on Linux, where
      // napi-toolchain-x64-linux legitimately does not exist.
      for (const [inferred, hostTriple] of [
        [linuxX64Targets, 'x64-linux'],
        [darwinArm64Targets, 'arm64-macos'],
      ] as const) {
        for (const suffix of ['arm64-linux', 'x64-linux', 'arm64-macos', 'x64-macos']) {
          const expectsToolchain = suffix.endsWith('-linux') && suffix !== hostTriple;
          expect(inferred[`napi-toolchain-${suffix}`] === undefined).toBe(!expectsToolchain);
          expect(inferred[`napi-${suffix}`]?.options?.command).toContain(
            expectsToolchain ? '--use-napi-cross' : '--target',
          );
        }
      }
      // The aggregate build pulls in exactly the inferring host's
      // platform-suffixed targets (publish still owns foreign platforms), and
      // the only cargo-test work it owns is compiling the executables — every
      // RUNNER is reached through `test`. Both hosts are asserted here so the
      // expectation does not depend on which machine runs the suite: the
      // previous form read `process.platform`, and the macOS answer was the only
      // one anyone checked.
      for (const [inferred, hostSuffix] of [
        [linuxX64Targets, '-x64-linux'],
        [darwinArm64Targets, '-arm64-macos'],
      ] as const) {
        const hostNapiTargets = [
          'cli-arm64-macos',
          'cli-x64-linux',
          'napi-arm64-macos',
          'napi-x64-macos',
          'napi-arm64-linux',
          'napi-x64-linux',
        ]
          .filter((name) => name.endsWith(hostSuffix))
          .sort();
        expect(hostNapiTargets.length).toBeGreaterThan(0);
        expect(inferred.build?.dependsOn).toEqual(['^build', 'cargo-test-compile', 'tsc-js', ...hostNapiTargets]);
        // Crate `cowshed-napi` names its bounded runner
        // `cargo-test-cowshed-napi`, which the retired `*-napi` output-family
        // glob matched on suffix alone. The runners are one serialized chain, so
        // that single edge put the whole cargo test suite inside
        // `nx run-many -t build`.
        expect(inferred['cargo-test-cowshed-napi']).toBeDefined();
        for (const dependency of inferred.build?.dependsOn ?? []) {
          expect(String(dependency).startsWith('cargo-test-')).toBe(dependency === 'cargo-test-compile');
        }
        for (const dependency of inferred.build?.dependsOn ?? []) {
          if (typeof dependency === 'string' && /-(?:arm64|x64)-(?:macos|linux)$/.test(dependency)) {
            expect(dependency.endsWith(hostSuffix)).toBe(true);
          }
        }
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
            'napi build --platform --no-js --dts cowshed.napi.d.ts --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir .cache/native-debug',
        },
      });
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-cowshed-napi']);
      // cargo-fetch survives the napi-debug edge: the per-crate runner is itself
      // a frozen cargo command, so its precondition is its own edge. It reads
      // the workspace archive and still waits for the debug cdylib its own
      // project builds, because its tests load that artifact from `target/`.
      expect(targets['cargo-test-cowshed-napi']?.dependsOn).toEqual([
        'cargo-fetch',
        'cargo-test-archive',
        'napi-debug',
      ]);
      expect(targets['cargo-test-cowshed-napi']?.options?.command).toMatch(
        /^cargo --frozen nextest run --archive-file target\/nextest\/archive\.tar\.zst --workspace-remap \. -E 'package\(cowshed-napi\)' --no-tests=pass --user-config-file none --tool-config-file "smoo:\$PWD\/.*nextest\.toml"$/,
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
      // Its declared *-wasm output-family name feeds the aggregate build and
      // clean by concrete name, and the cargo workspace's test-executable
      // compilation comes with it.
      expect(targets.build?.executor).toBe('nx:noop');
      expect(targets.build?.dependsOn).toEqual(['^build', 'cargo-test-compile', 'cargo-wasm']);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
    } finally {
      await workspace.cleanup();
    }
  });

  it('shards a crate around the tests nextest.toml singles out, not through them', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/rusty/package.json', '{"name":"rusty"}\n');
      await workspace.write('packages/rusty/Cargo.toml', '[workspace]\nmembers = ["crates/big", "crates/small"]\n');
      await workspace.write(
        'packages/rusty/crates/big/Cargo.toml',
        '[package]\nname = "big"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
      );
      await workspace.write('packages/rusty/crates/small/Cargo.toml', '[package]\nname = "small"\n');

      const targets = await inferProjectTargets(workspace, 'packages/rusty/package.json');

      // An unsharded crate keeps the bare name and takes no --partition, so
      // declaring nothing is exactly the old single-target behaviour.
      expect(targets['cargo-test-small']?.options?.command).toMatch(
        /nextest run --archive-file target\/nextest\/archive\.tar\.zst --workspace-remap \. -E 'package\(small\)' --no-tests=pass --user-config-file none/,
      );
      // The shards partition the crate MINUS the classes nextest.toml singles
      // out, i of N. Those are lifted out because a test-group only holds
      // within one nextest run, and because a test carrying a raised
      // slow-timeout costs what the suite does not — the compile-fail test
      // rustc's a fixture for 25.6s on a cold target dir against 1.8s warm.
      const exceptional = exceptionalTestFilter(fileURLToPath(new URL('../nextest.toml', import.meta.url)));
      if (exceptional === null) {
        throw new Error('nextest.toml declares no overrides; this test asserts the pin that protects them');
      }
      for (const index of [1, 2, 3]) {
        expect(targets[`cargo-test-big-shard${index}`]?.options?.command).toContain(
          `-E 'package(big) and not (${exceptional})' --partition hash:${index}/3 --no-tests=pass`,
        );
        expect(targets[`cargo-test-big-shard${index}`]?.options?.timeoutMs).toBe(BOUNDED_TEST_TIMEOUT_MS);
      }
      // Exact complement of the shards' filterset, so the union is the crate.
      expect(targets['cargo-test-big-exceptions']?.options?.command).toContain(
        `-E 'package(big) and (${exceptional})' --no-tests=pass`,
      );
      expect(targets['cargo-test-big-exceptions']?.options?.command).not.toContain('--partition');
      expect(targets['cargo-test-big-exceptions']?.options?.timeoutMs).toBe(BOUNDED_TEST_TIMEOUT_MS);
      // An unsharded crate needs no pin: its whole suite is already one run.
      expect(targets['cargo-test-small-exceptions']).toBeUndefined();
      expect(targets['cargo-test-big']).toBeUndefined();
      // Every piece reaches the aggregate, and they all hang off the one
      // archive: a run extracts binaries to its own temp directory and writes
      // nothing to cargo's flocked target/, so nothing is left to serialize.
      expect(targets['cargo-test']?.dependsOn).toEqual([
        'cargo-test-big-shard1',
        'cargo-test-big-shard2',
        'cargo-test-big-shard3',
        'cargo-test-big-exceptions',
        'cargo-test-small',
      ]);
      for (const name of [
        'cargo-test-big-shard1',
        'cargo-test-big-shard2',
        'cargo-test-big-shard3',
        'cargo-test-big-exceptions',
        'cargo-test-small',
      ]) {
        expect(targets[name]?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      }
      // Pieces of one crate share its inputs: they are one suite, split only to
      // fit the bound, so any change to the crate invalidates all of them.
      expect(targets['cargo-test-big-shard2']?.inputs).toEqual(targets['cargo-test-big-shard1']?.inputs);
      expect(targets['cargo-test-big-exceptions']?.inputs).toEqual(targets['cargo-test-big-shard1']?.inputs);
    } finally {
      await workspace.cleanup();
    }
  });

  it('rejects a shard count that cannot describe a partition', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/rusty/package.json', '{"name":"rusty"}\n');
      await workspace.write('packages/rusty/Cargo.toml', '[workspace]\nmembers = ["crates/big"]\n');
      await workspace.write(
        'packages/rusty/crates/big/Cargo.toml',
        '[package]\nname = "big"\n\n[package.metadata.smoothbricks.test]\nshards = 0\n',
      );

      // Nx wraps a createNodes throw in AggregateCreateNodesError, so the
      // reason a maintainer needs is in the nested error, not the top message.
      const failure = await inferProjectTargets(workspace, 'packages/rusty/package.json').then(
        () => undefined,
        (error: unknown) => error,
      );
      if (!(failure instanceof AggregateCreateNodesError)) {
        throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
      }
      expect(failure.errors[0]?.[1]?.message).toMatch(/smoothbricks\.test\.shards must be an integer >= 1, got 0/);
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
      expect(targets.test).toEqual({ dependsOn: ['^build', 'build'] });
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

  it('gives a declared TypeScript test the ^build/build dependsOn base targetDefaults must not supply', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/money/package.json',
        JSON.stringify({
          name: 'money',
          nx: {
            targets: {
              test: {
                executor: '@smoothbricks/nx-plugin:bounded-exec',
                options: { command: 'bun test', cwd: '{projectRoot}' },
              },
            },
          },
        }),
      );
      await workspace.write('packages/money/tsconfig.lib.json', '{}\n');

      const targets = await inferProjectTargets(workspace, 'packages/money/package.json');
      expect(targets.test).toEqual({ dependsOn: ['^build', 'build'] });
      expect(targets['cargo-test']).toBeUndefined();
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
      // A replacing array drops every inferred edge, cargo-fetch included: the
      // package now owns ordering the locked fetch ahead of its frozen compile.
      expect(cargoTestCompile?.dependsOn).toEqual(['cargo-wasm']);
      expect(targets['cargo-test-compile']?.dependsOn).toEqual(['cargo-fetch']);
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
      expect(cargoTest?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-compile', 'cargo-wasm']);
      expect(cargoTest?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
    } finally {
      await workspace.cleanup();
    }
  });

  it('never caches a declared deploy, and puts its build half in front of it', async () => {
    const workspace = await createWorkspace();
    const declared = {
      deploy: { executor: 'nx:run-commands', options: { command: 'smoo wrangler deploy-stage --stage={args.stage}' } },
      'deploy-build': { executor: 'nx:run-commands', options: { command: 'bun tooling/build-site.ts' } },
    };
    try {
      await workspace.write('packages/site/package.json', JSON.stringify({ name: 'site', nx: { targets: declared } }));
      const targets = await inferProjectTargets(workspace, 'packages/site/package.json');

      const deploy = resolveDeclaredOverInferred(targets, declared, 'deploy');
      // A cache hit on deploy would mean "we once uploaded this hash", which a rollback falsifies
      // silently; the step has to run and read live state instead.
      expect(deploy?.cache).toBe(false);
      expect(deploy?.dependsOn).toEqual(['deploy-build']);
      expect(resolveDeclaredOverInferred(targets, declared, 'deploy-build')?.cache).toBe(true);
    } finally {
      await workspace.cleanup();
    }
  });

  it('infers nothing for a project that declares no deploy', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/lib/package.json', '{"name":"plain-lib"}\n');
      const targets = await inferProjectTargets(workspace, 'packages/lib/package.json');

      expect(Object.keys(targets)).not.toContain('deploy');
      expect(Object.keys(targets)).not.toContain('deploy-build');
    } finally {
      await workspace.cleanup();
    }
  });

  it('orders a three-project deploy chain through one run-many', async () => {
    const workspace = await createWorkspace();
    const root = workspace.context.workspaceRoot;
    const repositoryRoot = fileURLToPath(new URL('../../../', import.meta.url));
    try {
      await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
      await workspace.write('package.json', '{"name":"chain-workspace","private":true,"workspaces":["packages/*"]}\n');
      await workspace.write(
        'nx.json',
        JSON.stringify({
          plugins: [fileURLToPath(new URL('../dist/index.js', import.meta.url))],
          namedInputs: { default: ['{projectRoot}/**/*'] },
        }),
      );
      // c is deepest: a's deploy calls into b's, b's calls into c's. Only the edges are declared;
      // nothing here says "round one" or "round two", and no project knows the chain's depth.
      const chain: Record<string, string[]> = { a: ['b:deploy'], b: ['c:deploy'], c: [] };
      for (const [name, dependsOn] of Object.entries(chain)) {
        await workspace.write(
          `packages/${name}/package.json`,
          JSON.stringify({
            name,
            nx: {
              targets: {
                deploy: {
                  executor: 'nx:run-commands',
                  ...(dependsOn.length > 0 ? { dependsOn: ['...', ...dependsOn] } : {}),
                  // A real deploy command consumes the stage through `{args.stage}`; this fixture
                  // records the order and nothing else, so it declines the forwarded flags.
                  options: {
                    command: `printf '%s\\n' ${name} >> ${join(root, 'deploy-order.txt')}`,
                    forwardAllArgs: false,
                  },
                },
              },
            },
          }),
        );
      }
      const child = Bun.spawn(
        [
          'bun',
          join(repositoryRoot, 'node_modules/.bin/nx'),
          'run-many',
          '-t',
          'deploy',
          '--projects=a,b,c',
          '--parallel=3',
          '--stage=staging',
        ],
        {
          cwd: root,
          env: {
            ...process.env,
            PATH: `${join(repositoryRoot, 'node_modules/.bin')}:${process.env.PATH ?? ''}`,
            NX_DAEMON: 'false',
            NX_ISOLATE_PLUGINS: 'false',
            NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx/workspace-data'),
            NX_CACHE_DIRECTORY: join(root, '.nx/cache'),
          },
          stdout: 'pipe',
          stderr: 'pipe',
        },
      );
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ exitCode, output: stdout + stderr }).toMatchObject({ exitCode: 0 });
      const order = (await readFile(join(root, 'deploy-order.txt'), 'utf8')).trim().split('\n');
      expect(order).toEqual(['c', 'b', 'a']);
    } finally {
      await workspace.cleanup();
    }
  }, 120_000);
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

async function inferProject(
  workspace: WorkspaceFixture,
  packageJsonPath: string,
  createNodes: CreateNodesV2 = createNodesV2,
) {
  const [, infer] = createNodes;
  const result = await infer([packageJsonPath], undefined, workspace.context);
  return result[0]?.[1].projects?.[dirname(packageJsonPath)];
}

async function inferProjectTargets(
  workspace: WorkspaceFixture,
  packageJsonPath: string,
  createNodes?: CreateNodesV2,
): Promise<Record<string, TargetConfiguration>> {
  return (await inferProject(workspace, packageJsonPath, createNodes))?.targets ?? {};
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
 * How a cargo test command is scoped to one crate, split into the three facts
 * that can independently go wrong.
 *
 * `--archive-file` must be present: the runner executes binaries one workspace
 * `cargo nextest archive --workspace` already built, so a per-crate run cannot
 * compile anything. Narrowing comes from the nextest filterset, which selects
 * what RUNS. `--package` narrows too, but it re-resolves that crate's features,
 * which is a build — 57.9s of a 120s bounded budget on a hosted 3-core macOS
 * runner when the runners still built — so its presence is a regression even
 * though it selects the same tests. nextest rejects it outright beside
 * `--archive-file`, and this assertion says so before cargo does.
 */
function cargoPackageSelection(command: string): {
  archiveRun: boolean;
  packageFlags: string[];
  filtered: string[];
} {
  const tokens = command.split(/\s+/);
  const packageFlags: string[] = [];
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i] === '--package' || tokens[i] === '-p') {
      const name = tokens[i + 1];
      if (name !== undefined && name.length > 0) {
        packageFlags.push(name);
      }
    }
  }
  return {
    archiveRun: tokens.includes('--archive-file'),
    packageFlags,
    filtered: [...command.matchAll(/\bpackage\(([^)]+)\)/g)].flatMap((match) =>
      match[1] === undefined ? [] : [match[1]],
    ),
  };
}
