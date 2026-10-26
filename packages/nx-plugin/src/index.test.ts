import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { NEXTEST_EXTRACTION_DIRECTORY } from '@smoothbricks/nextest-extract';
import { isEslintFileSet } from '@smoothbricks/nx-plugin/eslint-file-set';
import type {
  CreateNodesContextV2,
  CreateNodesV2,
  ProjectGraph,
  ProjectGraphProjectNode,
  TargetConfiguration,
} from 'nx/src/devkit-exports.js';
import { WorkspaceContext } from 'nx/src/native/index.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';
import { createTargetDefaultsResults } from 'nx/src/project-graph/utils/project-configuration/target-defaults.js';
import { mergeTargetConfigurations } from 'nx/src/project-graph/utils/project-configuration-utils.js';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';
import { getOutputsForTargetAndConfiguration } from 'nx/src/tasks-runner/utils.js';
import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { prepareCargoFixture } from './__tests__/cargo-fixture.js';
import { runtimeInputValue } from './__tests__/runtime-input.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS, BOUNDED_TEST_KILL_AFTER_MS } from './bounded-test-policy.js';
import { CARGO_CLOSURE_INPUT, isCargoLockInput } from './cargo-closure-input.js';
import { CARGO_PRODUCER_METADATA } from './cargo-retention-hook.js';
import { exceptionalTestFilter, packageNameFromCargoTestTarget } from './cargo-workspace.js';
import { CARGO_CROSS_LINT_COMMAND, CARGO_CROSS_LINT_TARGET, CARGO_LINT_CLIPPY_COMMAND } from './cross-check-policy.js';
import { ALL_VERDICTS_EXECUTOR } from './executors/all-verdicts/schema.js';
import { cargoCommandNpmPackages, createNodesV2, createNodesV2ForPlatform } from './index.js';
import { isRecord } from './type-guards.js';
import { applyWorkspaceConfig, RELEASE_CONFIGURATION } from './workspace-config-policy.js';

useFixtureCargoHome();

/** The inferring host's directory under a tool package's dist/bin, the one inferred commands run a tool from. */
const HOST_TOOLS = `${process.platform === 'darwin' ? 'darwin' : 'linux'}-${process.arch}`;

const [, rawInferTargets] = createNodesV2;
const inferTargets: typeof rawInferTargets = async (files, options, context) => {
  await prepareCargoFixture(context.workspaceRoot);
  return rawInferTargets(files, options, context);
};

describe('@smoothbricks/nx-plugin inferred targets', () => {
  it('never lets a cache hit on the build aggregate restore its children’s dist', async () => {
    const workspace = await createWorkspace();
    // The emitter keys on one input outside its project, as a transform or toolchain does. The
    // aggregate hashes only the project, so a toolchain change moves the emitter's key and not the
    // aggregate's: the aggregate replays from cache right after the emitter reran.
    const emit: TargetConfiguration = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['{projectRoot}/source.txt', '{workspaceRoot}/toolchain.txt'],
      outputs: ['{projectRoot}/dist/'],
      options: { command: 'mkdir -p dist && cat source.txt ../../toolchain.txt > dist/app.js', cwd: '{projectRoot}' },
    };
    const declared = { 'emit-js': emit };
    try {
      await workspace.write(
        'packages/app/package.json',
        JSON.stringify({ name: 'app', private: true, nx: { targets: declared } }),
      );
      const targets = await inferProjectTargets(workspace, 'packages/app/package.json');
      const build = resolveDeclaredOverInferred(targets, declared, 'build');
      expect(build?.dependsOn).toContain('emit-js');
      expect(build?.cache).toBe(true);
      if (build === undefined) throw new Error('app infers no build aggregate over emit-js');

      // A replay restores exactly what Nx resolves as the task's outputs. Left without `outputs`, a
      // `build` target gets Nx's legacy defaults, `{projectRoot}/dist` among them, and the replay
      // puts back the dist the emitter wrote on the run that last produced the aggregate's key.
      const app: ProjectGraphProjectNode = {
        name: 'app',
        type: 'lib',
        data: { root: 'packages/app', targets: { build, 'emit-js': emit } },
      };
      expect(getOutputsForTargetAndConfiguration({ project: 'app', target: 'build' }, {}, app)).toEqual([]);
      expect(getOutputsForTargetAndConfiguration({ project: 'app', target: 'emit-js' }, {}, app)).toEqual([
        'packages/app/dist',
      ]);
    } finally {
      await workspace.cleanup();
    }
  });

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
        'packages/nx-plugin/managed/raw/tooling/typescript-api/package.json',
        '{"name":"@smoothbricks/typescript-api","private":true}\n',
      );
      await workspace.write(
        'tooling/typescript-api/package.json',
        '{"name":"@smoothbricks/typescript-api","private":true}\n',
      );

      const managed = await inferTargets(
        ['packages/nx-plugin/managed/raw/tooling/typescript-api/package.json'],
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

  it('keys installed patch data and pins without keying patch documentation', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/example/package.json', '{"name":"example","scripts":{"test":"bun test"}}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');
      await workspace.write('patches/nx.patch', 'patch-one\n');
      await workspace.write('patches/compiler/PIN', 'revision-one\n');
      await workspace.write('patches/compiler/VERSION', 'version-one\n');
      await workspace.write('patches/README.md', 'documentation-one\n');
      await workspace.write('patches/compiler/notes.md', 'nested documentation-one\n');
      await workspace.write('packages/unrelated/source.ts', 'export const value = 1;\n');
      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');
      const context = new WorkspaceContext(
        workspace.context.workspaceRoot,
        join(workspace.context.workspaceRoot, '.nx/patch-input-context'),
      );
      for (const name of ['tsc-js', 'typecheck', 'typecheck-tests', 'lint']) {
        const inputs = targets[name]?.inputs ?? [];
        const patchInputs = inputs.filter(
          (input): input is string => typeof input === 'string' && input.includes('{workspaceRoot}/patches/'),
        );
        expect(patchInputs.length).toBeGreaterThan(0);
        const positive = patchInputs
          .filter((input) => !input.startsWith('!'))
          .map((input) => input.slice('{workspaceRoot}/'.length));
        const negative = patchInputs
          .filter((input) => input.startsWith('!'))
          .map((input) => input.slice('!{workspaceRoot}/'.length));
        expect(context.glob(positive, negative).sort()).toEqual([
          'patches/compiler/PIN',
          'patches/compiler/VERSION',
          'patches/nx.patch',
        ]);
        const before = context.hashFilesMatchingGlob(positive, negative);
        await workspace.write('patches/README.md', `${name} docs\n`);
        await workspace.write('patches/compiler/notes.md', `${name} nested docs\n`);
        await workspace.write('packages/unrelated/source.ts', `export const value = ${name.length};\n`);
        context.incrementalUpdate(
          ['patches/README.md', 'patches/compiler/notes.md', 'packages/unrelated/source.ts'],
          [],
        );
        expect(context.hashFilesMatchingGlob(positive, negative)).toBe(before);
        let previous = before;
        for (const file of ['patches/nx.patch', 'patches/compiler/PIN', 'patches/compiler/VERSION']) {
          await workspace.write(file, `${name} changed ${file}\n`);
          context.incrementalUpdate([file], []);
          const changed = context.hashFilesMatchingGlob(positive, negative);
          expect(changed).not.toBe(previous);
          previous = changed;
        }
      }
    } finally {
      await workspace.cleanup();
    }
  });

  it('uses the transformer-aware compiler for emit and validation', async () => {
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
        { externalDependencies: ['bun-lock:packages/example'] },
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
      ];
      // An emit is a shipped artifact: it hashes the raw manifests, so a
      // version bump reruns it rather than restoring a stale one.
      expect(targets['tsc-js']?.inputs).toEqual([
        'production',
        '^production',
        ...toolchainInputs,
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(targets['tsc-js']?.outputs).toEqual(['{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}']);
      // Typecheck runs the compiler devenv.smoo.nix pins (SMOO_TYPECHECK_TSC) itself, not through
      // ttsc, whose auto-loaded transform plugins would check with their own compiler; and it refuses
      // to fall back to another when the shell does not provide it.
      const typecheckCompiler =
        '"${SMOO_TYPECHECK_TSC:?is unset, enter the devenv shell (devenv.smoo.nix builds the typecheck compiler)}"';
      expect(targets.typecheck?.options).toMatchObject({
        command: `${typecheckCompiler} -p tsconfig.lib.json --noEmit`,
        cwd: 'packages/example',
      });
      const checkToolchainInputs = [
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
      ];
      // The compiler's store path names its pinned revision, so a compiler bump misses the cache.
      expect(targets.typecheck?.inputs).toEqual([
        'versionlessProduction',
        '^production',
        ...checkToolchainInputs,
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(targets.build?.executor).toBe('nx:noop');
      expect(targets.build?.cache).toBe(true);
      expect(targets.build?.dependsOn).toEqual(['^build', 'tsc-js']);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(targets.clean?.cache).toBe(false);

      expect(targets['typecheck-tests']?.executor).toBe('nx:run-commands');
      expect(targets['typecheck-tests']?.cache).toBe(true);
      expect(targets['typecheck-tests']?.dependsOn).toEqual(['tsc-js', 'typecheck']);
      expect(targets['typecheck-tests']?.options).toMatchObject({
        command: `${typecheckCompiler} -p tsconfig.test.json --noEmit`,
        cwd: 'packages/example',
      });
      expect(targets['typecheck-tests']?.inputs).toEqual([
        'versionlessDefault',
        '^production',
        ...checkToolchainInputs,
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.test.json',
      ]);

      expect(targets['typecheck-tests:watch']?.executor).toBe('nx:run-commands');
      expect(targets['typecheck-tests:watch']?.continuous).toBe(true);
      expect(targets['typecheck-tests:watch']?.options).toMatchObject({
        command: `${typecheckCompiler} -p tsconfig.test.json --noEmit --watch`,
        cwd: 'packages/example',
      });

      expect(targets['test-watch']?.executor).toBe('nx:run-commands');
      expect(targets['test-watch']?.continuous).toBe(true);
      expect(targets['test-watch']?.dependsOn).toEqual(['typecheck-tests']);
      expect(targets['test-watch']?.options).toMatchObject({
        command: 'bun test --watch --pass-with-no-tests',
        cwd: 'packages/example',
      });

      expect(targets.lint?.cache).toBe(true);
      expect(targets.lint?.dependsOn).toEqual(['typecheck-tests']);
      expect(targets.lint?.inputs).toContain('^production');
    } finally {
      await workspace.cleanup();
    }
  });

  it('compiles the bins of a scriptc package into dist/bin/<platform>, one target per platform', async () => {
    const workspace = await createWorkspace();
    try {
      const packs = {
        '@scriptc/runtime-darwin-arm64': '0.4.4',
        '@scriptc/runtime-darwin-x64': '0.4.4',
        '@scriptc/runtime-linux-arm64-musl': '0.4.4',
        '@scriptc/runtime-linux-x64-musl': '0.4.4',
      };
      const manifest = {
        name: '@scope/tool',
        bin: { 'smoo-nx-tool': './dist/ts/bin/smoo-nx-tool.js' },
        devDependencies: { ...packs, scriptc: '0.4.4' },
      };
      const libTsconfig = '{"compilerOptions":{"rootDir":"src","outDir":"dist/ts"}}\n';
      await workspace.write('packages/tool/package.json', JSON.stringify(manifest));
      await workspace.write('packages/tool/tsconfig.lib.json', libTsconfig);
      await workspace.write('packages/plain/package.json', JSON.stringify({ ...manifest, devDependencies: {} }));
      await workspace.write('packages/plain/tsconfig.lib.json', libTsconfig);

      const mac = await inferProjectTargets(
        workspace,
        'packages/tool/package.json',
        createNodesV2ForPlatform('darwin', 'arm64'),
      );
      const platforms = [
        ['scriptc-arm64-macos', 'aarch64-macos', 'darwin-arm64', '@scriptc/runtime-darwin-arm64'],
        ['scriptc-x64-macos', 'x86_64-macos', 'darwin-x64', '@scriptc/runtime-darwin-x64'],
        ['scriptc-arm64-linux', 'aarch64-linux-musl', 'linux-arm64', '@scriptc/runtime-linux-arm64-musl'],
        ['scriptc-x64-linux', 'x86_64-linux-musl', 'linux-x64', '@scriptc/runtime-linux-x64-musl'],
      ] as const;
      for (const [name, triple, directory, pack] of platforms) {
        expect(mac[name]).toMatchObject({
          executor: '@smoothbricks/nx-plugin:scriptc-native',
          cache: true,
          // A platform target's closure stays in its family (package-target-policy): no tsc-js sibling.
          dependsOn: ['^*-js'],
          outputs: [`{projectRoot}/dist/bin/${directory}/smoo-nx-tool`],
          options: {
            cwd: 'packages/tool',
            tsConfig: 'tsconfig.lib.json',
            triple,
            outputDirectory: `dist/bin/${directory}`,
            entries: { 'smoo-nx-tool': 'bin/smoo-nx-tool.js' },
          },
        });
        expect(mac[name]?.inputs).toContainEqual({ externalDependencies: ['scriptc', pack] });
      }
      expect(mac['tsc-js']?.outputs).toEqual(['{projectRoot}/dist/ts/']);
      // Only the host's own platform joins build; the publish legs build the rest by family.
      expect(mac.build?.dependsOn).toContain('scriptc-arm64-macos');
      expect(mac.build?.dependsOn).not.toContain('scriptc-x64-macos');
      const linux = await inferProjectTargets(
        workspace,
        'packages/tool/package.json',
        createNodesV2ForPlatform('linux', 'x64'),
      );
      // Every host infers the same four targets: a target's output does not depend on who built it.
      for (const [name] of platforms) expect(linux[name]).toEqual(mac[name]);
      expect(linux.build?.dependsOn).toContain('scriptc-x64-linux');
      expect(linux.build?.dependsOn).not.toContain('scriptc-arm64-macos');
      expect(Object.keys(await inferProjectTargets(workspace, 'packages/plain/package.json'))).not.toContainEqual(
        expect.stringMatching(/^scriptc-/),
      );

      const refusal = async (): Promise<string | undefined> => {
        const failure = await inferProjectTargets(workspace, 'packages/tool/package.json').then(
          () => undefined,
          (error: unknown) => error,
        );
        if (!(failure instanceof AggregateCreateNodesError)) {
          throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
        }
        return failure.errors[0]?.[1]?.message;
      };
      // A pack missing, or at another version than the compiler, is refused by name.
      const { '@scriptc/runtime-darwin-x64': _dropped, ...threePacks } = packs;
      await workspace.write(
        'packages/tool/package.json',
        JSON.stringify({
          ...manifest,
          devDependencies: { ...threePacks, '@scriptc/runtime-linux-x64-musl': '0.4.3', scriptc: '0.4.4' },
        }),
      );
      expect(await refusal()).toMatch(
        /list @scriptc\/runtime-darwin-x64@0\.4\.4, @scriptc\/runtime-linux-x64-musl@0\.4\.4 in devDependencies/,
      );
      // Emitting into dist/ itself would put the JavaScript and the executables in one output.
      await workspace.write('packages/tool/package.json', JSON.stringify(manifest));
      await workspace.write(
        'packages/tool/tsconfig.lib.json',
        '{"compilerOptions":{"rootDir":"src","outDir":"dist"}}\n',
      );
      expect(await refusal()).toMatch(/set tsconfig\.lib\.json compilerOptions\.outDir to "dist\/ts" \(it is "dist"\)/);
    } finally {
      await workspace.cleanup();
    }
  });

  it("hands a scriptc package's FFI to every platform target and hashes the files it is built from", async () => {
    const workspace = await createWorkspace();
    try {
      const packs = {
        '@scriptc/runtime-darwin-arm64': '0.4.4',
        '@scriptc/runtime-darwin-x64': '0.4.4',
        '@scriptc/runtime-linux-arm64-musl': '0.4.4',
        '@scriptc/runtime-linux-x64-musl': '0.4.4',
      };
      const ffi = { manifest: 'ffi.json', declarations: { 'src/ffi.d.ts': 'ffi.d.ts' }, sources: ['native/lock.c'] };
      const manifest = {
        name: '@scope/tool',
        bin: { 'smoo-nx-tool': './dist/ts/bin/smoo-nx-tool.js' },
        devDependencies: { ...packs, scriptc: '0.4.4' },
        scriptc: { ffi },
      };
      await workspace.write(
        'packages/tool/tsconfig.lib.json',
        '{"compilerOptions":{"rootDir":"src","outDir":"dist/ts"}}\n',
      );
      await workspace.write('packages/tool/package.json', JSON.stringify(manifest));
      const targets = await inferProjectTargets(workspace, 'packages/tool/package.json');
      const names = ['scriptc-arm64-macos', 'scriptc-x64-macos', 'scriptc-arm64-linux', 'scriptc-x64-linux'];
      for (const name of names) {
        expect(targets[name]?.options).toMatchObject({ ffi });
        // Change the manifest, a declaration or a C source and the executable is stale.
        for (const input of ['{projectRoot}/ffi.json', '{projectRoot}/src/ffi.d.ts', '{projectRoot}/native/lock.c']) {
          expect(targets[name]?.inputs).toContain(input);
        }
      }
      // A package that declares no FFI passes no option and no extra input.
      await workspace.write('packages/tool/package.json', JSON.stringify({ ...manifest, scriptc: undefined }));
      const plain = await inferProjectTargets(workspace, 'packages/tool/package.json');
      for (const name of names) {
        expect(plain[name]?.options).not.toHaveProperty('ffi');
        expect(plain[name]?.inputs).not.toContain('{projectRoot}/ffi.json');
      }
      const refusal = async (scriptc: unknown): Promise<string | undefined> => {
        await workspace.write('packages/tool/package.json', JSON.stringify({ ...manifest, scriptc }));
        const failure = await inferProjectTargets(workspace, 'packages/tool/package.json').then(
          () => undefined,
          (error: unknown) => error,
        );
        if (!(failure instanceof AggregateCreateNodesError)) {
          throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
        }
        return failure.errors[0]?.[1]?.message;
      };
      expect(await refusal({ ffi: { ...ffi, manifest: 7 } })).toMatch(/scriptc\.ffi needs a manifest path/);
      expect(await refusal({ ffi: { ...ffi, declarations: { 'src/ffi.d.ts': 7 } } })).toMatch(
        /scriptc\.ffi needs declarations mapping each source file to its staged path \(src\/ffi\.d\.ts is not\)/,
      );
      expect(await refusal({ ffi: { ...ffi, sources: 'native/lock.c' } })).toMatch(
        /scriptc\.ffi needs sources, a list of C files/,
      );
      expect(await refusal({ ffi: { ...ffi, sources: ['../outside.c'] } })).toMatch(
        /scriptc\.ffi names "\.\.\/outside\.c", which is not a path beneath the project/,
      );
      expect(await refusal({ ffi: { ...ffi, declarations: { 'src/ffi.d.ts': '/tmp/ffi.d.ts' } } })).toMatch(
        /scriptc\.ffi names "\/tmp\/ffi\.d\.ts", which is not a path beneath the emitted JavaScript's root/,
      );
      expect(await refusal({})).toMatch(/scriptc\.ffi must be an object/);
    } finally {
      await workspace.cleanup();
    }
  });
  it('hashes the manifests without their versions for checks, raw for artifacts', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{}\n');

      const project = await inferProjectWithNxJson(workspace, 'packages/example/package.json', {
        namedInputs: { versionlessProduction: ['production'] },
      });
      const targets = project?.targets ?? {};
      const checkToolchain = [
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        // Hashed by Nx itself: a `json` input costs no process, and neither
        // does the crate digest that replaces the manifests below.
        // The root manifest's scripts, smoo and nx sections are read by no check: an
        // edit to a script or a developer link must not re-key every lint and typecheck.
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
      ];

      // No crate manifest in this project, so no crate digest is declared at
      // all — nothing is hashed that nothing reads.
      expect(project?.namedInputs).toEqual({
        versionlessProduction: [
          'production',
          '!{projectRoot}/package.json',
          { json: '{projectRoot}/package.json', excludeFields: ['version'] },
        ],
        versionlessDefault: [
          'default',
          '!{projectRoot}/package.json',
          { json: '{projectRoot}/package.json', excludeFields: ['version'] },
        ],
      });
      expect(targets.typecheck?.inputs).toEqual([
        'versionlessProduction',
        { input: 'versionlessProduction', dependencies: true },
        ...checkToolchain,
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(targets['typecheck-tests']?.inputs).toEqual([
        'versionlessDefault',
        { input: 'versionlessProduction', dependencies: true },
        ...checkToolchain,
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.test.json',
      ]);
      expect(targets.lint?.inputs).toEqual([
        'versionlessDefault',
        { input: 'versionlessProduction', dependencies: true },
        ...checkToolchain,
        ...[
          'biome.json',
          'biome.jsonc',
          'eslint.config.js',
          'eslint.config.mjs',
          'eslint.config.cjs',
          'eslint.config.ts',
          'eslint.config.mts',
          'eslint.config.cts',
        ].flatMap((name) => [`{workspaceRoot}/${name}`, `{projectRoot}/${name}`]),
      ]);

      // The guard this whole treatment lives or dies on. A declaration emit is
      // a shipped artifact, and a crate compiled beside it embeds its version
      // through `env!("CARGO_PKG_VERSION")`, so an artifact target that stopped
      // hashing the raw manifests could hit a pre-bump entry and publish a
      // binary reporting the previous version.
      expect(targets['tsc-js']?.inputs).toEqual([
        'production',
        '^production',
        '{workspaceRoot}/package.json',
        { externalDependencies: ['bun-lock:packages/example'] },
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        '{projectRoot}/tsconfig.lib.json',
      ]);
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps the lockfiles out of the root versionless inputs, and names none in a member', async () => {
    const workspace = await createWorkspace();
    try {
      // The root's {projectRoot} is the whole workspace, so its versionless
      // inputs would reach the lockfiles the lock-scope nodes already key on.
      // A member's {projectRoot} holds no lockfile, so it names none.
      await workspace.write('package.json', '{"name":"root"}\n');
      await workspace.write('tsconfig.lib.json', '{}\n');
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');

      const root = await inferProjectWithNxJson(workspace, 'package.json', {});
      const member = await inferProjectWithNxJson(workspace, 'packages/example/package.json', {});
      const lockfiles = ['!{projectRoot}/bun.lock', '!{projectRoot}/bun.lockb', '!{projectRoot}/Cargo.lock'];
      expect(root?.targets?.typecheck?.inputs).toContain('versionlessProduction');
      for (const name of ['versionlessProduction', 'versionlessDefault']) {
        expect(root?.namedInputs?.[name]?.slice(-lockfiles.length)).toEqual(lockfiles);
        const memberInputs = member?.namedInputs?.[name];
        expect(memberInputs).toBeDefined();
        for (const lockfile of lockfiles) {
          expect(memberInputs).not.toContain(lockfile);
        }
      }
    } finally {
      await workspace.cleanup();
    }
  });

  it('hashes crate manifests into a digest, only where crates exist', async () => {
    const workspace = await createWorkspace();
    try {
      const manifest = (version: string) =>
        `[package]\nname = "rust-example"\nversion = "${version}"\n\n[dependencies]\nserde = { version = "1.0.200" }\n`;
      await workspace.write('packages/rust/package.json', '{"name":"rust-example"}\n');
      await workspace.write('packages/rust/tsconfig.lib.json', '{}\n');
      await workspace.write('packages/rust/Cargo.toml', manifest('0.1.0'));
      const production = async () =>
        (await inferProjectWithNxJson(workspace, 'packages/rust/package.json', {}))?.namedInputs?.versionlessProduction;

      const baseline = await production();

      // TOML has no `json` input, so this is the one manifest kind the plugin
      // hashes itself. There is no command behind it: the value IS the input,
      // and Nx hashes it through the project configuration.
      expect(baseline).toEqual([
        'production',
        '!{projectRoot}/package.json',
        { json: '{projectRoot}/package.json', excludeFields: ['version'] },
        '!{projectRoot}/**/Cargo.toml',
        expect.stringMatching(/^\{projectRoot\}\/\.versionless-crate-manifests\/[0-9a-f]{64}$/),
      ]);

      // What the digest exists for: the release moves the crate's own version
      // and nothing else, so the input a check hashes must not move with it.
      await workspace.write('packages/rust/Cargo.toml', manifest('9.9.9'));
      expect(await production()).toEqual(baseline);

      // And a dependency really changing must still reach the hash, or the
      // exclusion above would be hiding it.
      await workspace.write('packages/rust/Cargo.toml', manifest('0.1.0').replace('1.0.200', '1.0.201'));
      expect(await production()).not.toEqual(baseline);
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps the dependency half raw until the workspace defines the named input', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{}\n');

      // `{ input, dependencies: true }` resolves the name in each dependency's
      // own context and Nx throws when one has no definition for it. A
      // project.json project this plugin never inferred has none, so the
      // transitive half waits for the workspace-wide definition that covers it.
      const targets = (await inferProjectWithNxJson(workspace, 'packages/example/package.json', {}))?.targets ?? {};

      expect(targets.typecheck?.inputs).toEqual([
        'versionlessProduction',
        '^production',
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
    } finally {
      await workspace.cleanup();
    }
  });

  it('hashes every tsconfig the extends chain reaches, not just the one the command names', async () => {
    const workspace = await createWorkspace();
    try {
      // The shape that made this a silent cache hit: the options a check
      // resolves live one file ABOVE the config its command names.
      await workspace.write('tsconfig.base.json', '{"compilerOptions":{"strict":true}}\n');
      await workspace.write(
        'packages/tsconfig.base.json',
        '{"extends":"../tsconfig.base.json","compilerOptions":{"noUnusedLocals":true}}\n',
      );
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{"extends":"../tsconfig.base.json"}\n');
      // A deeper chain reached from the other config, named without the
      // extension TypeScript lets a specifier omit.
      await workspace.write('packages/example/tsconfig.strict.json', '{"extends":"../tsconfig.base"}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{"extends":"./tsconfig.strict.json"}\n');

      const targets = await inferProjectTargets(workspace, 'packages/example/package.json');

      // Every file in the chain, once, whichever config reached it.
      const configChain = [
        '{workspaceRoot}/tsconfig.base.json',
        '{workspaceRoot}/packages/tsconfig.base.json',
        '{workspaceRoot}/packages/example/tsconfig.strict.json',
      ];
      const checkToolchain = [
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        ...configChain,
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
      ];
      expect(targets.typecheck?.inputs).toEqual([
        'versionlessProduction',
        '^production',
        ...checkToolchain,
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(targets['typecheck-tests']?.inputs).toEqual([
        'versionlessDefault',
        '^production',
        ...checkToolchain,
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.test.json',
      ]);
      // An emit reads the same options, and a stale artifact is worse than a
      // stale verdict.
      expect(targets['tsc-js']?.inputs).toEqual([
        'production',
        '^production',
        '{workspaceRoot}/package.json',
        { externalDependencies: ['bun-lock:packages/example'] },
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        ...configChain,
        '{projectRoot}/tsconfig.lib.json',
      ]);
      // Typed lint runs the same program (`projectService`), so its verdict
      // moves with the same files.
      expect(targets.lint?.inputs).toEqual(expect.arrayContaining(configChain));
    } finally {
      await workspace.cleanup();
    }
  });

  it('refuses a project whose tsconfig extends a file that is not there', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{"extends":"../tsconfig.base.json"}\n');

      // Fail-closed, and the only safe direction: an ancestor left out of the
      // hash is a cached verdict nobody can invalidate. Nx wraps a createNodes
      // throw, so the reason lives in the nested error.
      const failure = await inferProjectTargets(workspace, 'packages/example/package.json').then(
        () => undefined,
        (error: unknown) => error,
      );
      if (!(failure instanceof AggregateCreateNodesError)) {
        throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
      }
      expect(failure.errors[0]?.[1]?.message).toMatch(/"extends": "\.\.\/tsconfig\.base\.json" names no file/);
    } finally {
      await workspace.cleanup();
    }
  });

  it('walks an extends cycle once, and leaves an installed config to the lockfile wherever it is stored', async () => {
    const workspace = await createWorkspace();
    // WHERE an installed config's bytes sit is the installer's business: bun
    // keeps them in a global store and points node_modules at it, so this one
    // is outside the workspace entirely.
    const store = await mkdtemp(join(tmpdir(), 'smoothbricks-nx-plugin-store-'));
    try {
      await workspace.write('tsconfig.base.json', '{"compilerOptions":{"strict":true}}\n');
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      // TypeScript rejects a cycle; this walk must terminate on one anyway,
      // with both files hashed.
      await workspace.write('packages/example/tsconfig.lib.json', '{"extends":"./tsconfig.cycle.json"}\n');
      await workspace.write('packages/example/tsconfig.cycle.json', '{"extends":"./tsconfig.lib.json"}\n');
      // Which bytes are in that store is pinned by the lockfile and the root
      // manifest, both already inputs; naming the path would hash nothing.
      await writeFile(join(store, 'package.json'), '{"name":"@fixture/tsconfig"}\n');
      await writeFile(join(store, 'tsconfig.json'), '{"compilerOptions":{"noUnusedLocals":true}}\n');
      await mkdir(join(workspace.context.workspaceRoot, 'node_modules/@fixture'), { recursive: true });
      await symlink(store, join(workspace.context.workspaceRoot, 'node_modules/@fixture/tsconfig'), 'dir');
      await workspace.write('packages/example/tsconfig.test.json', '{"extends":"@fixture/tsconfig/tsconfig.json"}\n');

      const { result: targets, stderr } = await withCapturedStderr(() =>
        inferProjectTargets(workspace, 'packages/example/package.json'),
      );
      const inputs = targets.typecheck?.inputs ?? [];

      expect(inputs).toEqual([
        'versionlessProduction',
        '^production',
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        '{workspaceRoot}/packages/example/tsconfig.cycle.json',
        '{workspaceRoot}/packages/example/tsconfig.lib.json',
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(JSON.stringify(inputs)).not.toContain('node_modules');
      // And silently, because nothing was lost. The noisy version of this fires
      // on every graph build in a workspace that installs one Expo package.
      expect(stderr).toBe('');
    } finally {
      await rm(store, { recursive: true, force: true });
      await workspace.cleanup();
    }
  });

  it("hashes a workspace member's config reached through its package name", async () => {
    const workspace = await createWorkspace();
    try {
      const root = workspace.context.workspaceRoot;
      await workspace.write('tsconfig.base.json', '{"compilerOptions":{"strict":true}}\n');
      // The other half of that judgement: a package name can resolve to
      // workspace SOURCE, which no lockfile pins. Dropping it would be the
      // original bug wearing a package name.
      await workspace.write('packages/tsconfig/package.json', '{"name":"@fixture/tsconfig"}\n');
      await workspace.write(
        'packages/tsconfig/base.json',
        '{"extends":"../../tsconfig.base.json","compilerOptions":{"noUnusedLocals":true}}\n',
      );
      await mkdir(join(root, 'node_modules/@fixture'), { recursive: true });
      await symlink(join(root, 'packages/tsconfig'), join(root, 'node_modules/@fixture/tsconfig'), 'dir');
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.lib.json', '{"extends":"@fixture/tsconfig/base.json"}\n');

      const { result: targets, stderr } = await withCapturedStderr(() =>
        inferProjectTargets(workspace, 'packages/example/package.json'),
      );

      expect(targets.typecheck?.inputs).toEqual([
        'versionlessProduction',
        '^production',
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        '{workspaceRoot}/packages/tsconfig/base.json',
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
      expect(stderr).toBe('');
    } finally {
      await workspace.cleanup();
    }
  });

  it('says so when an extends chain leaves the workspace, where nothing can hash it', async () => {
    const workspace = await createWorkspace();
    const outside = await mkdtemp(join(tmpdir(), 'smoothbricks-nx-plugin-outside-'));
    try {
      await writeFile(join(outside, 'tsconfig.base.json'), '{"compilerOptions":{"noUnusedLocals":true}}\n');
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write(
        'packages/example/tsconfig.lib.json',
        `${JSON.stringify({ extends: join(outside, 'tsconfig.base.json') })}\n`,
      );

      const { result: targets, stderr } = await withCapturedStderr(() =>
        inferProjectTargets(workspace, 'packages/example/package.json'),
      );

      // Nx hashes workspace files only, and no lockfile pins this one either,
      // so the verdict really can go stale. A silent drop here is the bug this
      // walk exists to close, so it is said out loud instead.
      expect(stderr).toContain(join(outside, 'tsconfig.base.json'));
      expect(stderr).toContain('outside the workspace');
      expect(targets.typecheck?.inputs).toEqual([
        'versionlessProduction',
        '^production',
        '{workspaceRoot}/patches/**/*',
        '!{workspaceRoot}/patches/**/*.md',
        '{workspaceRoot}/tsconfig.base.json',
        { json: '{workspaceRoot}/package.json', excludeFields: ['version', 'scripts', 'smoo', 'nx'] },
        { externalDependencies: ['bun-lock:packages/example'] },
        { env: 'SMOO_TYPECHECK_TSC' },
        '{projectRoot}/tsconfig.lib.json',
      ]);
    } finally {
      await rm(outside, { recursive: true, force: true });
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

      expect(targets['test-watch']?.continuous).toBe(true);
      expect(targets['test-watch']?.options).toMatchObject({
        command: 'vitest --coverage',
        cwd: 'packages/example',
      });
    } finally {
      await workspace.cleanup();
    }
  });

  it('makes lint check that the timing manifest names every file a timings-sharded Bun suite runs', async () => {
    const workspace = await createWorkspace();
    try {
      const shard = (n: number) => ({
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        options: { command: `bun test --timings=test-timings.json --shard=${n}/2`, cwd: '{projectRoot}' },
      });
      await workspace.write(
        'packages/sharded/package.json',
        JSON.stringify({ name: 'sharded', nx: { targets: { 'test-a': shard(1), 'test-b': shard(2) } } }),
      );
      await workspace.write(
        'packages/counted/package.json',
        JSON.stringify({
          name: 'counted',
          nx: { targets: { 'test-a': { options: { command: 'bun test --shard=1/1', cwd: '{projectRoot}' } } } },
        }),
      );

      const sharded = await inferProjectTargets(workspace, 'packages/sharded/package.json');
      const counted = await inferProjectTargets(workspace, 'packages/counted/package.json');

      expect(sharded['shard-timings']).toEqual({
        executor: '@smoothbricks/nx-plugin:shard-timings',
        cache: false,
        outputs: [],
        options: { cwd: 'packages/sharded', args: ['--timings=test-timings.json'] },
      });
      expect(sharded.lint?.dependsOn).toEqual(['shard-timings']);
      expect(counted['shard-timings']).toBeUndefined();
      expect(counted.lint).toBeUndefined();
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

  it('keeps a test runner out of the aggregate build however its name ends', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write(
        'packages/suites/package.json',
        JSON.stringify({
          name: 'suites',
          nx: {
            targets: {
              // A runner reading as "tool=test, output=bun". Suffix alone would
              // sweep it into `build`, which is a serialized test suite behind
              // every consumer's build.
              'test-bun': { executor: 'nx:run-commands' },
              'test-watch': { executor: 'nx:run-commands' },
              // A real emitter in the same family, to prove the exclusion is
              // the test-runner class and not the family itself.
              'bundle-bun': { executor: 'nx:run-commands' },
            },
          },
        }),
      );

      const targets = await inferProjectTargets(workspace, 'packages/suites/package.json');

      expect(targets.build?.dependsOn).toEqual(['^build', 'bundle-bun']);
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
  it("makes the retention preview wait for cargo-retain's host executable where the workspace builds it", async () => {
    const workspace = await createWorkspace();
    try {
      const root = workspace.context.workspaceRoot;
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = ["crates/ferris-core"]\n');
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');
      await workspace.write('packages/nx-plugin/package.json', '{"name":"@smoothbricks/nx-plugin"}\n');
      await workspace.write(
        'packages/cargo-retain/package.json',
        '{"name":"@smoothbricks/cargo-retain","nx":{"name":"cargo-retain"}}\n',
      );
      await mkdir(join(root, 'node_modules/@smoothbricks'), { recursive: true });
      await symlink(join(root, 'packages/nx-plugin'), join(root, 'node_modules/@smoothbricks/nx-plugin'), 'dir');
      await prepareCargoFixture(root);
      resetWorkspaceContext();
      const [, infer] = createNodesV2ForPlatform('darwin', 'arm64');
      const results = await infer(
        ['packages/ferris/package.json', 'packages/cargo-retain/package.json'],
        undefined,
        workspace.context,
      );
      const ferris = results.find(([file]) => file === 'packages/ferris/package.json')?.[1].projects?.[
        'packages/ferris'
      ];
      expect(ferris?.targets?.['cargo-retention']).toMatchObject({
        dependsOn: [{ projects: ['cargo-retain'], target: 'scriptc-arm64-macos' }],
        options: {
          command:
            '../../node_modules/@smoothbricks/nx-plugin/../cargo-retain/dist/bin/darwin-arm64/smoo-nx-cargo-retain',
          cwd: 'packages/ferris',
        },
      });
    } finally {
      await workspace.cleanup();
    }
  });

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
      expect(targets['cargo-sweep']).toBeUndefined();
      expect(targets['cargo-retention']?.options).toEqual({
        command: `../../node_modules/@smoothbricks/nx-plugin/../cargo-retain/dist/bin/${HOST_TOOLS}/smoo-nx-cargo-retain`,
        cwd: 'packages/ferris',
      });
      expect(targets['cargo-retention']?.cache).toBe(false);
      // Never cached: the download lands in CARGO_HOME, so a cache hit would
      // report success over a registry that still cannot resolve the lockfile.
      expect(targets['cargo-fetch']?.cache).toBe(false);
      expect(targets['cargo-fetch']?.options).toEqual({
        commands: ['cargo fetch --locked'],
        cwd: 'packages/ferris',
      });
      // It keys nothing (never cached), so its inputs are what a producer receipt binds it to. Left unnamed they are the
      // project's every file, and a receipt binds no symlink: a repository-root project's managed files are links.
      expect(targets['cargo-fetch']?.inputs).toEqual([
        '{projectRoot}/**/Cargo.toml',
        '{projectRoot}/**/Cargo.lock',
        '{projectRoot}/**/.cargo/config.toml',
        '!{projectRoot}/**/target/**',
      ]);
      // Every frozen cargo command carries the edge: offline cargo fails at
      // resolution, so each one needs the locked graph downloaded before it runs.
      for (const name of ['cargo-test-archive', 'cargo-lint', CARGO_CROSS_LINT_TARGET, 'mutation', 'bench']) {
        expect(targets[name]?.dependsOn).toContain('cargo-fetch');
      }
      expect(targets['cargo-retention']?.dependsOn).toBeUndefined();
      // Retention starts after the tasks that build into the target directory, named by this metadata, and no other.
      for (const name of ['cargo-test-archive', 'cargo-lint', CARGO_CROSS_LINT_TARGET, 'mutation', 'bench']) {
        expect(targets[name]?.metadata).toEqual(CARGO_PRODUCER_METADATA);
      }
      expect(targets['cargo-fetch']?.metadata).toBeUndefined();
      // The archive and the lints hash every crate's sources
      // The archive and the compile it stands for hash every crate's sources
      // AND the outputs of the targets they depend on: generated crate inputs
      // are gitignored, so a repository that had to restate `inputs` to add
      // them replaced the crate union with the root's `default` and shipped a
      // stale archive to every per-crate test.
      for (const name of ['cargo-test-archive', 'cargo-lint', CARGO_CROSS_LINT_TARGET]) {
        const inputs = targets[name]?.inputs ?? [];
        expect(inputs).toContainEqual({ dependentTasksOutputFiles: '**/*', transitive: false });
        expect(inputs).toContain('{workspaceRoot}/packages/ferris/crates/ferris-core/**/*');
      }
      // The archive is the one test compile; no uncached `--no-run` warm-up compiles the same units again.
      expect(targets['cargo-test-compile']).toBeUndefined();
      expect(targets['cargo-test']?.executor).toBe('nx:noop');
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-ferris-core', 'cargo-test-ferris-wasm']);
      // One workspace compile, packed once. Every per-crate runner reads that
      // archive, so the runners fan out instead of chaining: none of them
      // builds, re-resolves features, or writes cargo's flocked `target/`.
      expect(targets['cargo-test-archive']?.executor).toBe('nx:run-commands');
      expect(targets['cargo-test-archive']?.cache).toBe(true);
      expect(targets['cargo-test-archive']?.outputs).toEqual([
        '{projectRoot}/.cache/nextest/archive.tar.zst',
        '{projectRoot}/.cache/nextest/archive.tar.zst.key',
      ]);
      expect(targets['cargo-test-archive']?.options).toMatchObject({ cwd: 'packages/ferris' });
      // nextest does not create the archive's parent directory and fails the
      // whole build if it is missing (measured: "error writing to archive").
      // The directory and the archive are ONE command, in the order the archive needs.
      expect(targets['cargo-test-archive']?.options?.commands).toBeUndefined();
      // `--tool-config-file`, never `--config-file`: the plugin's settings must
      // sit UNDER the repository's `.config/nextest.toml`, which is the only
      // place an `archive.include` for a cdylib or fixture can be declared.
      // `$PWD` because tool config paths must be absolute and an absolute path
      // in the command text would split one cache entry per checkout; through
      // the root `node_modules` link because the plugin's real location is a
      // different number of `../` away from each checkout.
      expect(String(targets['cargo-test-archive']?.options?.command)).toBe(
        'mkdir -p .cache/nextest && cargo --frozen nextest archive --workspace --archive-file .cache/nextest/archive.tar.zst --user-config-file none --tool-config-file "smoo:$PWD/../../node_modules/@smoothbricks/nx-plugin/nextest.toml" && ' +
          'test -n "$NX_TASK_HASH" && printf \'%s\\n\' "$NX_TASK_HASH" > .cache/nextest/archive.tar.zst.key',
      );
      expect(targets['cargo-test-archive']?.inputs).toContain('{projectRoot}/.config/nextest.toml');
      // Tests compile the dev profile only. Nx forwards a run's configuration
      // down every edge to each target that has it, so a release configuration
      // here would make a release `build` compile test binaries nobody runs.
      for (const name of ['cargo-test-archive', 'cargo-test', 'cargo-test-ferris-core', 'test']) {
        expect(targets[name]).toBeDefined();
        expect(targets[name]?.configurations?.[RELEASE_CONFIGURATION]).toBeUndefined();
      }
      expect(targets['cargo-test-ferris-core']?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      expect(targets['cargo-test-ferris-wasm']?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      // The runner executes the archive's binaries, unpacked once for every
      // runner by the plugin's helper (through the root `node_modules` link,
      // so the hashed command names no machine path), and remaps the
      // workspace root: without `--workspace-remap`, a restored archive hands
      // tests the PRODUCING tree's absolute CARGO_MANIFEST_DIR (measured on a
      // moved tree). `--workspace` is not merely redundant here, nextest
      // rejects it beside a reused build.
      expect(targets['cargo-test-ferris-core']?.options?.command).toMatch(
        /^extracted="\$\(\.\.\/\.\.\/node_modules\/@smoothbricks\/nx-plugin\/\.\.\/nextest-extract\/dist\/bin\/[a-z0-9-]+\/smoo-nx-nextest-extract \.cache\/nextest\/archive\.tar\.zst\)" && cargo --frozen nextest run --binaries-metadata "\$extracted\/target\/nextest\/binaries-metadata\.json" --cargo-metadata "\$extracted\/target\/nextest\/cargo-metadata\.json" --target-dir-remap "\$extracted\/target" --workspace-remap \. -E 'package\(ferris-core\)' --no-fail-fast --no-tests=pass --user-config-file none --tool-config-file "smoo:\$PWD\/\.\.\/\.\.\/node_modules\/@smoothbricks\/nx-plugin\/nextest\.toml"$/,
      );
      expect(targets['cargo-test-ferris-core']?.inputs).toContain(
        '{workspaceRoot}/packages/ferris/.config/nextest.toml',
      );
      expect(targets['cargo-test-ferris-core']?.inputs).toEqual(
        expect.arrayContaining([
          '{workspaceRoot}/packages/ferris/Cargo.toml',
          { externalDependencies: ['cargo-lock:packages/ferris/crates/ferris-core'] },
          '{workspaceRoot}/packages/ferris/crates/ferris-core/**/*',
        ]),
      );
      expect(targets['cargo-test-ferris-core']?.inputs).not.toContain(
        '{workspaceRoot}/packages/ferris/crates/ferris-wasm/**/*',
      );
      expect(targets['cargo-test-ferris-core']?.inputs).not.toContain('{workspaceRoot}/packages/ferris/Cargo.lock');
      // One clippy for the workspace, in a target dir of its own: sharing the
      // dev build's `target/` queued the archive behind clippy's build lock.
      const [fmt, clippy] = targets['cargo-lint']?.options?.commands ?? [];
      expect(fmt).toBe('cargo fmt --all --check');
      expect(clippy).toBe(CARGO_LINT_CLIPPY_COMMAND);
      expect(clippy).toMatch(/^cargo --frozen clippy --workspace --all-targets /);
      expect(clippy).toContain(' --target-dir target/cargo-lint ');
      expect(targets['cargo-test-archive']?.options?.command).not.toContain('--target-dir');
      expect(targets['cargo-lint']?.options?.cwd).toBe(targets['cargo-test-archive']?.options?.cwd);
      expect(targets['cargo-lint']?.cache).toBe(true);
      expect(targets['cargo-lint']?.outputs).toEqual([]);
      expect(targets['cargo-lint']?.inputs).toEqual(
        expect.arrayContaining([
          '{workspaceRoot}/packages/ferris/crates/ferris-core/**/*',
          '{workspaceRoot}/packages/ferris/crates/ferris-wasm/**/*',
        ]),
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
      // On bounded-exec, so cargo-mutants' own per-core jobserver is sized to a CPU grant.
      expect(targets.mutation?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(targets.mutation?.options).toMatchObject({
        command: 'cargo --frozen mutants --workspace',
        timeoutMs: 6 * 60 * 60 * 1000,
      });
      expect(targets.bench?.options).toMatchObject({ command: 'cargo --frozen bench --workspace' });
      expectNoSchedulerThrottle(targets);
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

      // A clippy target dir is not an Nx output — restoring a multi-GiB build
      // tree from a cache is worse than rebuilding it — so `clean` cannot find
      // these through `outputs`. They are read back off the commands that
      // write them, which is why a new `--target-dir` cannot be forgotten here.
      // The workspace `target/` every dev build shares is not one of them.
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      expect(targets.clean?.cache).toBe(false);
      expect(targets.clean?.options?.outputs).toEqual([
        '{projectRoot}/dist',
        '{workspaceRoot}/packages/ferris/.cache/nextest',
        '{workspaceRoot}/packages/ferris/target/cargo-lint',
        '{workspaceRoot}/packages/ferris/target/cargo-lint-cross',
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
      const biome = `biome check --files-ignore-unknown=true '${root}'`;
      const rust = await inferProjectTargets(workspace, `${root}/package.json`);
      const rustLint = resolveLint(rust);
      expect(rustLint.executor).toBe(ALL_VERDICTS_EXECUTOR);
      expect(rustLint.dependsOn).toEqual(['cargo-lint']);
      expect(rustLint.options?.commands).toEqual([biome]);

      // A JavaScript source joins ESLint's file list the moment it appears, with no tsconfig to announce it.
      await workspace.write(`${root}/src/check.js`, 'debugger;\n');
      const withJavaScript = resolveLint(await inferProjectTargets(workspace, `${root}/package.json`));
      expect(withJavaScript.executor).toBe(ALL_VERDICTS_EXECUTOR);
      expect(withJavaScript.dependsOn).toEqual(['cargo-lint']);
      expect(withJavaScript.options?.commands).toEqual([biome, `eslint '${root}/src/check.js'`]);

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
        biome,
        `eslint '${root}/src/check.js' '${root}/src/index.ts'`,
      ]);
      expectNoSchedulerThrottle(mixed);
    } finally {
      await workspace.cleanup();
    }
  });

  it('lints the declared test program outside src and keys its sources and inherited declaration', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('eslint.config.mjs', 'export default [{ files: ["**/*.ts"] }];\n');
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write(
        'packages/test-base.json',
        JSON.stringify({
          include: ['\u0024{configDir}/tests/**/*.ts'],
          exclude: ['\u0024{configDir}/tests/excluded.ts'],
          files: ['contract/schema.d.ts'],
        }),
      );
      await workspace.write('packages/example/tsconfig.test.json', '{"extends":"../test-base.json"}\n');
      await workspace.write('packages/example/src/index.ts', 'export const value = 1;\n');
      await workspace.write('packages/example/tests/check.test.ts', 'export const check = 1;\n');
      await workspace.write('packages/example/tests/helper.ts', 'export const helper = 1;\n');
      await workspace.write('packages/example/tests/excluded.ts', 'export const excluded = 1;\n');
      await workspace.write('packages/example/other/stray.test.ts', 'export const stray = 1;\n');
      await workspace.write('packages/contract/schema.d.ts', 'export interface Contract { value: number }\n');

      const { lint } = await inferProjectTargets(workspace, 'packages/example/package.json');
      expect(lint?.options?.commands).toEqual([
        "eslint 'packages/contract/schema.d.ts' 'packages/example/src/index.ts' 'packages/example/tests/check.test.ts' 'packages/example/tests/helper.ts'",
      ]);
      expect(lint?.metadata?.eslintFileSet).toEqual({
        command: lint?.options?.commands?.[0],
        files: [
          'packages/contract/schema.d.ts',
          'packages/example/src/index.ts',
          'packages/example/tests/check.test.ts',
          'packages/example/tests/helper.ts',
        ],
      });
      expect(isEslintFileSet(lint?.metadata?.eslintFileSet)).toBe(true);
      expect(isEslintFileSet({ command: 'eslint invalid', files: [null] })).toBe(false);
      expect(lint?.inputs).toEqual(
        expect.arrayContaining([
          '{projectRoot}/tsconfig.test.json',
          '{workspaceRoot}/packages/test-base.json',
          '{workspaceRoot}/packages/contract/schema.d.ts',
          '{workspaceRoot}/packages/example/tests/check.test.ts',
          '{workspaceRoot}/packages/example/tests/helper.ts',
        ]),
      );
      expect(lint?.inputs).not.toContain('{workspaceRoot}/packages/example/tests/excluded.ts');
      expect(lint?.inputs).not.toContain('{workspaceRoot}/packages/example/other/stray.test.ts');
    } finally {
      await workspace.cleanup();
    }
  });

  it('lints and keys only the sources Nx indexes, never ignored files a build left on disk', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('eslint.config.mjs', 'export default [{ files: ["**/*.ts"] }];\n');
      // Generated outputs one build wrote and a fresh checkout lacks: a root ignore, a
      // project-level one, and an .nxignore, the three ways Nx keeps a file out of its index.
      await workspace.write('.gitignore', 'packages/example/src/generated/\n');
      await workspace.write('packages/example/.gitignore', '*.generated.ts\n');
      await workspace.write('.nxignore', 'packages/example/tests/nx-ignored.ts\n');
      await workspace.write('packages/example/package.json', '{"name":"example"}\n');
      await workspace.write('packages/example/tsconfig.test.json', '{"include":["tests/**/*.ts"]}\n');
      await workspace.write('packages/example/src/index.ts', 'export const value = 1;\n');
      await workspace.write('packages/example/src/generated/schemas.gen.ts', 'export const schemas = 1;\n');
      await workspace.write('packages/example/src/catalog.generated.ts', 'export const catalog = 1;\n');
      await workspace.write('packages/example/tests/check.test.ts', 'export const check = 1;\n');
      await workspace.write('packages/example/tests/helpers/catalog.generated.ts', 'export const c = 1;\n');
      await workspace.write('packages/example/tests/nx-ignored.ts', 'export const ignored = 1;\n');

      const { lint } = await inferProjectTargets(workspace, 'packages/example/package.json');
      const files = ['packages/example/src/index.ts', 'packages/example/tests/check.test.ts'];
      expect(lint?.metadata?.eslintFileSet).toEqual({
        command: `eslint ${files.map((file) => `'${file}'`).join(' ')}`,
        files,
      });
      expect(lint?.options?.commands).toEqual([lint?.metadata?.eslintFileSet.command]);
      const programInputs = (lint?.inputs ?? []).filter(
        (input): input is string => typeof input === 'string' && input.startsWith('{workspaceRoot}/packages/example/'),
      );
      expect(programInputs).toEqual(['{workspaceRoot}/packages/example/tests/check.test.ts']);
    } finally {
      await workspace.cleanup();
    }
  });

  it('keys exactly the workspace files jiti loads for a TypeScript ESLint config', async () => {
    const workspace = await createWorkspace();
    try {
      await writeConfigLoadFixture(workspace);
      await workspace.write(
        'eslint.config.ts',
        [
          "import { rule } from './tooling/checks/rule.js';",
          "import dirpkg from './tooling/checks/dirpkg';",
          "import review from '@review/rules';",
          // Erased: a declaration-only module, and a binding used only as a type.
          "import type { RuleName } from './tooling/checks/config-types';",
          "import { Helper } from './tooling/checks/helper';",
          "import { shared } from './tooling/shared';",
          'const helper: Helper | null = null;',
          "const lazy = await import('./tooling/checks/lazy');",
          'export default [rule, dirpkg, review, shared, lazy, helper] as RuleName[];',
          '',
        ].join('\n'),
      );
      await workspace.write('tooling/checks/config-types.d.ts', 'export type RuleName = unknown;\n');
      await workspace.write('tooling/checks/helper.ts', 'export interface Helper { readonly name: string }\n');
      await workspace.write('tooling/checks/lazy.ts', 'export const lazy = 4;\n');
      await workspace.write('tooling/shared.ts', 'export const shared = 1;\n');

      const { lint } = await inferProjectTargets(workspace, 'packages/example/package.json');
      const loaded = await loaderLoads(workspace, 'eslint.config.ts');
      expect(loaded).toEqual([
        'tooling/checks/dirpkg/actual.cjs',
        'tooling/checks/lazy.ts',
        'tooling/checks/rule.js',
        'tooling/checks/rules/index.cjs',
        'tooling/shared.ts',
      ]);
      expect(keyedTooling(lint)).toEqual(
        [
          ...loaded,
          'tooling/checks/dirpkg/package.json',
          'tooling/checks/package.json',
          'tooling/checks/rules/package.json',
        ].sort(),
      );
      expect(hashes(lint, 'eslint.config.ts')).toBe(true);
    } finally {
      await workspace.cleanup();
    }
  });

  it('hands the process back its stack-trace globals after jiti compiles a TypeScript ESLint config', async () => {
    const workspace = await createWorkspace();
    try {
      await writeConfigLoadFixture(workspace);
      await workspace.write(
        'eslint.config.ts',
        "import { shared } from './tooling/shared';\nexport default [shared];\n",
      );
      await workspace.write('tooling/shared.ts', 'export const shared = 1;\n');
      const { prepareStackTrace, stackTraceLimit } = Error;

      const { lint } = await inferProjectTargets(workspace, 'packages/example/package.json');

      expect(keyedTooling(lint)).toEqual(['tooling/shared.ts']);
      expect(Error.prepareStackTrace).toBe(prepareStackTrace);
      expect(Error.stackTraceLimit).toBe(stackTraceLimit);
      // follow-redirects' error types capture onto objects that are not Errors;
      // under Bun a Babel wrapper left installed makes this capture throw.
      const target: { stack?: string } = {};
      Error.captureStackTrace(target);
      expect(typeof target.stack).toBe('string');
    } finally {
      await workspace.cleanup();
    }
  });

  it('keys exactly the workspace files Node loads for a JavaScript ESLint config', async () => {
    const configs = {
      'eslint.config.cjs': {
        source: [
          "const { rule } = require('./tooling/checks/rule.js');",
          "const dirpkg = require('./tooling/checks/dirpkg');",
          "const review = require('@review/rules');",
          'module.exports = [rule, dirpkg, review];',
          '',
        ].join('\n'),
        manifests: [
          'tooling/checks/dirpkg/package.json',
          'tooling/checks/package.json',
          'tooling/checks/rules/package.json',
        ],
      },
      'eslint.config.mjs': {
        source: [
          "import { rule } from './tooling/checks/rule.js';",
          "import dirpkg from './tooling/checks/dirpkg/actual.cjs';",
          "import review from '@review/rules';",
          'export default [rule, dirpkg, review];',
          '',
        ].join('\n'),
        manifests: [
          'tooling/checks/dirpkg/package.json',
          'tooling/checks/package.json',
          'tooling/checks/rules/package.json',
        ],
      },
    };
    for (const [config, { source, manifests }] of Object.entries(configs)) {
      const workspace = await createWorkspace();
      try {
        await writeConfigLoadFixture(workspace);
        await workspace.write(config, source);

        const { lint } = await inferProjectTargets(workspace, 'packages/example/package.json');
        const loaded = await loaderLoads(workspace, config);
        expect(loaded).toEqual([
          'tooling/checks/dirpkg/actual.cjs',
          'tooling/checks/rule.js',
          'tooling/checks/rules/index.cjs',
        ]);
        expect(keyedTooling(lint)).toEqual([...loaded, ...manifests].sort());
      } finally {
        await workspace.cleanup();
      }
    }
  });

  it('refuses an ESLint config load it cannot key, naming the config, the module and the load', async () => {
    const refusals = {
      // A transitive import no file answers.
      'eslint.config.ts': {
        files: {
          'eslint.config.ts': "import { rule } from './tooling/checks/rule.ts';\nexport default [rule];\n",
          'tooling/checks/rule.ts': "export { rule } from './missing-helper';\n",
        },
        reason:
          /eslint\.config\.ts loads tooling\/checks\/rule\.ts, which imports '\.\/missing-helper', which names no file jiti resolves/,
      },
      // A require whose specifier is computed: Node loads it, nothing names it.
      'eslint.config.cjs': {
        files: {
          'eslint.config.cjs': "const name = './tooling/checks/rule.js';\nmodule.exports = require(name);\n",
        },
        reason: /eslint\.config\.cjs loads a module it computes at runtime \(require\(name\)\)/,
      },
      // A dynamic import jiti loads from a template.
      'eslint.config.mts': {
        files: {
          'eslint.config.mts':
            "const name = 'lazy';\nexport default await import('./tooling/checks/' + name + '.ts');\n",
        },
        reason: /eslint\.config\.mts loads a module it computes at runtime/,
      },
    };
    for (const { files, reason } of Object.values(refusals)) {
      const workspace = await createWorkspace();
      try {
        await writeConfigLoadFixture(workspace);
        for (const [path, contents] of Object.entries(files)) await workspace.write(path, contents);

        // Nx wraps a createNodes throw in AggregateCreateNodesError, so the reason is in the nested error.
        const failure = await inferProjectTargets(workspace, 'packages/example/package.json').then(
          () => undefined,
          (error: unknown) => error,
        );
        if (!(failure instanceof AggregateCreateNodesError)) {
          throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
        }
        expect(failure.errors[0]?.[1]?.message).toMatch(reason);
      } finally {
        await workspace.cleanup();
      }
    }
  });

  it('refuses every loader form Node loads through but the source does not spell as a call', async () => {
    // Each form loads tooling/checks/rule.js under real Node; none is a `require('…')` call.
    const forms = {
      dot: ["module.exports = module.require('./tooling/checks/rule.js');", '(module.require)'],
      bracket: ["module.exports = module['require']('./tooling/checks/rule.js');", "(module['require'])"],
      'computed bracket': [
        "const key = 'require';\nmodule.exports = module[key]('./tooling/checks/rule.js');",
        '(module[key])',
      ],
      call: ["module.exports = require.call(null, './tooling/checks/rule.js');", '(require.call)'],
      apply: ["module.exports = require.apply(null, ['./tooling/checks/rule.js']);", '(require.apply)'],
      bind: ["module.exports = require.bind(null)('./tooling/checks/rule.js');", '(require.bind)'],
      destructuring: [
        "const { require: load } = module;\nmodule.exports = load.call(module, './tooling/checks/rule.js');",
        '(require: load)',
      ],
      alias: ["const load = require;\nmodule.exports = load('./tooling/checks/rule.js');", '(load = require)'],
    };
    for (const [form, [source, load]] of Object.entries(forms)) {
      const workspace = await createWorkspace();
      try {
        await writeConfigLoadFixture(workspace);
        await workspace.write('eslint.config.cjs', `${source}\n`);
        expect({ form, loaded: await loaderLoads(workspace, 'eslint.config.cjs') }).toEqual({
          form,
          loaded: ['tooling/checks/rule.js'],
        });

        // Nx wraps a createNodes throw in AggregateCreateNodesError, so the reason is in the nested error.
        const failure = await inferProjectTargets(workspace, 'packages/example/package.json').then(
          () => undefined,
          (error: unknown) => error,
        );
        if (!(failure instanceof AggregateCreateNodesError)) {
          throw new Error(`${form}: expected AggregateCreateNodesError, got ${String(failure)}`);
        }
        expect({ form, message: failure.errors[0]?.[1]?.message }).toEqual({
          form,
          message: expect.stringContaining(`eslint.config.cjs loads a module it computes at runtime ${load}`),
        });
      } finally {
        await workspace.cleanup();
      }
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
      const rootArchive = { projects: [rootProject], target: 'cargo-test-archive' };

      expect(
        [...projects]
          .filter(([, project]) => project.targets?.['cargo-test-archive'])
          .map(([projectRoot]) => projectRoot),
      ).toEqual(['.']);
      expect(root['cargo-fetch']?.options).toEqual({
        commands: ['cargo fetch --locked'],
        cwd: '.',
      });
      // A repository-root workspace's members belong to other projects, so its fetch reads from the repository root.
      expect(root['cargo-fetch']?.inputs).toEqual([
        '{workspaceRoot}/**/Cargo.toml',
        '{workspaceRoot}/**/Cargo.lock',
        '{workspaceRoot}/**/.cargo/config.toml',
        '!{workspaceRoot}/**/target/**',
      ]);
      // The root owns the one workspace clippy, run from the workspace root.
      expect(String(root['cargo-lint']?.options?.commands?.[1])).toContain('clippy --workspace');
      expect(root['cargo-lint']?.options?.cwd).toBe(root['cargo-test-archive']?.options?.cwd);
      expect(root['cargo-lint']?.dependsOn).toContain('cargo-fetch');
      expect(root['cargo-test']?.dependsOn).toHaveLength(6);

      // Root mode: every piece of every project hangs off the one root archive,
      // in any order. The crate is selected by filterset alone — `-p` would
      // re-resolve features, and there is nothing left to build anyway.
      expect(runtime['cargo-test-runtime-core-shard1']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-shard2']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-exceptions-shard1']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-exceptions-shard2']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(runtime['cargo-test-runtime-core-shard1']?.options).toMatchObject({ cwd: '.' });
      // At the root the helper and the plugin's nextest config are the root's
      // own `node_modules` link, no `../`.
      expect(runtime['cargo-test-runtime-core-shard1']?.options?.command).toStartWith(
        `extracted="$(node_modules/@smoothbricks/nx-plugin/../nextest-extract/dist/bin/${HOST_TOOLS}/smoo-nx-nextest-extract .cache/nextest/archive.tar.zst)" && `,
      );
      expect(runtime['cargo-test-runtime-core-shard1']?.options?.command).toContain(
        `--target-dir-remap "$extracted/target" --workspace-remap . -E 'package(runtime-core)`,
      );
      expect(runtime['cargo-test-runtime-core-shard1']?.options?.command).toEndWith(
        '--tool-config-file "smoo:$PWD/node_modules/@smoothbricks/nx-plugin/nextest.toml"',
      );
      expect(native['cargo-test-native-napi']?.options?.command).toContain('--no-tests=pass');
      expect(runtime['cargo-test']?.dependsOn).toEqual([
        'cargo-test-runtime-core-shard1',
        'cargo-test-runtime-core-shard2',
        'cargo-test-runtime-core-exceptions-shard1',
        'cargo-test-runtime-core-exceptions-shard2',
      ]);
      // A package owning crates validates through the root's one clippy verdict.
      expect(runtime['cargo-lint']?.dependsOn).toEqual([{ projects: [rootProject], target: 'cargo-lint' }]);
      expect(runtime.lint?.dependsOn).toEqual(['cargo-lint']);
      expect(runtime['cargo-lint-runtime-core']).toBeUndefined();
      expect(runtime.clean).toBeUndefined();

      expect(wasm['cargo-test-runtime-wasm']?.dependsOn).toEqual([rootFetch, rootArchive]);
      expect(wasm['cargo-wasm']?.inputs).toContain(CARGO_CLOSURE_INPUT);
      expect(wasm['cargo-wasm']?.inputs).not.toContain('{workspaceRoot}/**/*.rs');
      expect(wasm['cargo-wasm']?.options).toMatchObject({ cwd: '.' });
      // Dev by default, in the workspace's own `target/`; only the release
      // configuration compiles, and binds, the release artifact.
      expect(wasm['cargo-wasm']?.options?.command).toBe(
        [
          'cargo --frozen build --target wasm32-unknown-unknown -p runtime-wasm',
          'wasm-bindgen --target web --out-dir packages/wasm/dist-wasm/web target/wasm32-unknown-unknown/debug/runtime_wasm.wasm',
        ].join(' && '),
      );
      expect(wasm['cargo-wasm']?.options?.commands).toBeUndefined();
      expect(wasm['cargo-wasm']?.configurations?.[RELEASE_CONFIGURATION]).toEqual({
        command: [
          'cargo --frozen build --release --target wasm32-unknown-unknown -p runtime-wasm',
          'wasm-bindgen --target web --out-dir packages/wasm/dist-wasm/web target/wasm32-unknown-unknown/release/runtime_wasm.wasm',
        ].join(' && '),
      });
      expect(wasm['cargo-wasm']?.outputs).toEqual(['{projectRoot}/dist-wasm/']);
      expect(wasm['cargo-wasm']?.dependsOn).toEqual([rootFetch, '^build']);
      expect(wasm.build?.dependsOn).toContainEqual(rootArchive);
      expect(wasm.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');

      // The crate whose project also builds the debug cdylib keeps that edge:
      // its tests load the artifact `napi-debug` writes into `target/debug`.
      expect(native['cargo-test-native-napi']?.dependsOn).toEqual([rootFetch, rootArchive, 'napi-debug']);
      // The addon build consumes only its dependencies' outputs; it is not
      // ordered behind the test compile.
      expect(native['napi-debug']?.dependsOn).toEqual(['^build']);
      // The addon depends on the package's napi block and the napi CLI's version,
      // never on the package version or the whole lockfile: a release bumps both
      // before it builds, and that must not recompile the native code.
      const napiInputs = native['napi-debug']?.inputs ?? [];
      expect(napiInputs).toContainEqual({ externalDependencies: ['@napi-rs/cli'] });
      expect(
        napiInputs.some((input) => typeof input === 'object' && 'runtime' in input && input.runtime.includes('.napi')),
      ).toBe(true);
      for (const [name, target] of Object.entries(native)) {
        if (!name.startsWith('napi-') || target.cache !== true) continue;
        expect(target.inputs).toContain(CARGO_CLOSURE_INPUT);
        expect(target.inputs).not.toContain('{workspaceRoot}/**/*.rs');
      }
      const nativeClosure = projects.get('packages/native')?.namedInputs?.[CARGO_CLOSURE_INPUT] ?? [];
      expect(nativeClosure).toContain(
        '{workspaceRoot}/packages/native/crates/native-napi/**/{*.rs,Cargo.toml,.cargo/config,.cargo/config.toml}',
      );
      expect(nativeClosure.some((input) => typeof input === 'string' && input.includes('packages/runtime'))).toBe(
        false,
      );
      expect(napiInputs).not.toContain('{workspaceRoot}/bun.lock');
      expect(napiInputs.some((input) => typeof input === 'string' && input.endsWith('/package.json'))).toBe(false);
      // ... and on the toolchain pin, which it must state itself: `napi build`
      // compiles with the pinned rustc but never spells `cargo --frozen`, so
      // the sweep that gives every other cargo target its identity skips it.
      expect(napiInputs).toContain('{workspaceRoot}/devenv.lock');
      expect(napiInputs).toContain('{workspaceRoot}/tooling/direnv/devenv.lock');
      expect(native['napi-debug']?.options).toMatchObject({
        cwd: '.',
        command:
          'packages/native/node_modules/.bin/napi build --platform --no-js --dts native.napi.d.ts --manifest-path packages/native/crates/native-napi/Cargo.toml --package native-napi --package-json-path packages/native/package.json --output-dir packages/native/.cache/native-debug',
      });
      const platformBuild = (release: string): string =>
        `packages/native/node_modules/.bin/napi build${release} --platform --no-js --dts native.darwin-arm64.d.ts --target aarch64-apple-darwin --manifest-path packages/native/crates/native-napi/Cargo.toml --package native-napi --package-json-path packages/native/package.json --output-dir packages/native/dist/native/darwin-arm64 && node_modules/@smoothbricks/nx-plugin/../build-inputs/dist/bin/${HOST_TOOLS}/smoo-nx-build-inputs --manifest-path packages/native/crates/native-napi/Cargo.toml --dep-info aarch64-apple-darwin/${release === '' ? 'debug' : 'release'}/libnative_napi.d --output packages/native/dist/native/darwin-arm64`;
      expect(native['napi-arm64-macos']?.options).toMatchObject({ cwd: '.', command: platformBuild('') });
      expect(native['napi-arm64-macos']?.configurations?.[RELEASE_CONFIGURATION]).toEqual({
        command: platformBuild(' --release'),
      });
      expect(native.build?.dependsOn).toContainEqual(rootArchive);
      expect(native.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      for (const inferred of [root, runtime, wasm, native]) {
        expectNoSchedulerThrottle(inferred);
      }
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
      expect(outer['cargo-lint']?.options?.cwd).toBe('.');
      expect(String(outer['cargo-lint']?.options?.commands?.[1])).toContain('clippy --workspace');
      // Workspace-anchored, because Nx hashes a `{projectRoot}` glob against
      // only the files its project owns, and the group project owns alpha and
      // beta: spelled from the root project, those globs hash nothing and the
      // archive replays stale binaries against edited crates.
      for (const name of ['cargo-lint', 'cargo-test-archive']) {
        expect(outer[name]?.inputs).toEqual(
          expect.arrayContaining([
            '{workspaceRoot}/packages/group/crates/alpha/**/*',
            '{workspaceRoot}/packages/group/crates/beta/**/*',
          ]),
        );
        expect(
          (outer[name]?.inputs ?? []).filter(
            (input) => typeof input === 'string' && input.startsWith('{projectRoot}/packages/'),
          ),
        ).toEqual([]);
      }
      expect(group['cargo-lint']?.dependsOn).toEqual([{ projects: ['outer-root'], target: 'cargo-lint' }]);
      expect(group.build?.dependsOn).toEqual([
        '^build',
        { projects: ['outer-root'], target: 'cargo-test-archive' },
        'tsc-js',
      ]);
      expect(outer['cargo-lint-host-core']).toBeUndefined();
      expect(inner['cargo-lint']?.options).toMatchObject({ cwd: 'nested' });
      expect(inner['cargo-lint']?.inputs).toContain('{workspaceRoot}/nested/**/*');
      expect(inner['cargo-lint']?.inputs).toContain('{workspaceRoot}/nested/modules/leaf/**/*');
      expect(inner['cargo-lint']?.inputs).not.toContain('{workspaceRoot}/packages/group/crates/alpha/**/*');
      expect(leaf['cargo-lint']?.dependsOn).toEqual([{ projects: ['inner-root'], target: 'cargo-lint' }]);
      expect(leaf['cargo-lint-inner-leaf']).toBeUndefined();
      expect(leaf['cargo-test-inner-leaf']?.options?.cwd).toBe('nested');
      expect(leaf['cargo-test-inner-leaf']?.dependsOn).toEqual([
        { projects: ['inner-root'], target: 'cargo-fetch' },
        { projects: ['inner-root'], target: 'cargo-test-archive' },
      ]);
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

  it('keys every cached cargo target on the declared toolchain pin', async () => {
    const workspace = await createWorkspace();
    try {
      await writeCargoIdentityFixture(workspace);
      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');
      const cached = cachedCargoTargets(targets);

      // Named, so a family that stops being cached — or a new one that starts —
      // shows up here rather than silently leaving the sweep below empty.
      expect(cached.map(([name]) => name).sort()).toEqual([
        'cargo-cross-test-archive-aarch64-apple-darwin',
        'cargo-lint',
        'cargo-lint-cross',
        'cargo-test-archive',
        'cargo-test-ferris-core',
      ]);
      // An SDK or toolchain change must invalidate every one of them. The pin
      // is the only thing in the identity that can see one, and it is declared
      // at both fleet locations because a fileset matching nothing is free.
      for (const [name, target] of cached) {
        const declared = (target.inputs ?? []).filter(
          (input) => typeof input === 'string' && input.endsWith('devenv.lock'),
        );
        expect([name, declared]).toEqual([
          name,
          ['{workspaceRoot}/devenv.lock', '{workspaceRoot}/tooling/direnv/devenv.lock'],
        ]);
      }
    } finally {
      await workspace.cleanup();
    }
  });

  // Without an `externalDependencies` input Nx keys a task whose executor is
  // not an `@nx/` one on every package in the lockfile, so a lockfile-only
  // commit re-ran every cargo verdict (measured in a consumer: every cargo-test-*
  // runner and the archive re-keyed on a one-integrity bun.lock edit). Here
  // the plugin is this repository's workspace project, which Nx refuses as an
  // external dependency, so each target names no npm package: its lockfile reach is one Cargo.lock scope node.
  it('keys every cached cargo target on the npm packages it runs, not the lockfile', async () => {
    const workspace = await createWorkspace();
    try {
      await writeCargoIdentityFixture(workspace);
      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');
      const cached = cachedCargoTargets(targets);
      expect(cached.length).toBeGreaterThan(0);
      for (const [name, target] of cached) {
        const named = (target.inputs ?? []).filter(
          (input) => typeof input === 'object' && 'externalDependencies' in input && !isCargoLockInput(input),
        );
        expect([name, named]).toEqual([name, [{ externalDependencies: [] }]]);
        // The lockfile reaches each target through exactly one input: the Cargo.lock
        // scope node it names, or the closure input that names it.
        const lockReaches = (target.inputs ?? []).filter(
          (input) => input === CARGO_CLOSURE_INPUT || isCargoLockInput(input),
        );
        expect([name, lockReaches.length]).toEqual([name, 1]);
      }
    } finally {
      await workspace.cleanup();
    }
  });

  it('names the installed plugin for a cargo target that runs its executor or files, and nothing for cargo alone', () => {
    const plugin = 'node_modules/.bun/@smoothbricks+nx-plugin@0.4.26/node_modules/@smoothbricks/nx-plugin';
    const runner = { executor: '@smoothbricks/nx-plugin:bounded-exec' };
    const archive = `cargo --frozen nextest archive --workspace --tool-config-file "smoo:$PWD/${plugin}/nextest.toml"`;
    const clippy = CARGO_LINT_CLIPPY_COMMAND;
    expect(cargoCommandNpmPackages(runner, 'cargo --frozen nextest run', true)).toEqual(['@smoothbricks/nx-plugin']);
    expect(cargoCommandNpmPackages({ executor: 'nx:run-commands' }, archive, true)).toEqual([
      '@smoothbricks/nx-plugin',
    ]);
    expect(cargoCommandNpmPackages({ executor: 'nx:run-commands' }, clippy, true)).toEqual([]);
    expect(cargoCommandNpmPackages(runner, archive, false)).toEqual([]);
  });

  // The property that decides whether Rust can ever use a shared cache. A cargo
  // target's identity used to include a dump of the ambient cargo environment,
  // whose values are absolute `/nix/store/…` and per-checkout devenv state
  // paths: no two machines agreed, and the same target hashed differently in a
  // bare shell and inside a devenv profile, which made the pre-push
  // cross-compile probe unsatisfiable by construction.
  //
  // The runtime value AND its command declaration must be portable: Nx also
  // hashes the owning project's target configuration.
  it('hashes no machine-local value into a cached cargo target', async () => {
    const workspace = await createWorkspace();
    try {
      await writeCargoIdentityFixture(workspace);
      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');
      // Installed the way a consumer links it, so the inputs find the wrapper they name. After
      // inference, whose fixture walk would follow the link.
      await mkdir(join(workspace.context.workspaceRoot, 'node_modules/@smoothbricks'), { recursive: true });
      await symlink(
        fileURLToPath(new URL('..', import.meta.url)),
        join(workspace.context.workspaceRoot, 'node_modules/@smoothbricks/nx-plugin'),
      );
      const offenders: string[] = [];
      for (const [name, target] of cachedCargoTargets(targets)) {
        for (const input of target.inputs ?? []) {
          if (typeof input === 'string') {
            if (!WORKSPACE_ANCHORED_INPUT.test(input)) offenders.push(`${name}: fileset ${input}`);
            continue;
          }
          if ('env' in input) offenders.push(`${name}: environment input ${String(input.env)}`);
          if (!('runtime' in input)) continue;
          if (input.runtime.includes(fileURLToPath(new URL('..', import.meta.url)))) {
            offenders.push(`${name}: runtime command names the plugin checkout`);
          }
          const value = await runtimeInputValue(input.runtime, workspace.context.workspaceRoot);
          const rooted = PATH_OUTSIDE_WORKSPACE.exec(value);
          if (rooted) offenders.push(`${name}: runtime input yields ${rooted[0].trim()} — from ${input.runtime}`);
        }
      }
      expect(offenders).toEqual([]);
    } finally {
      await workspace.cleanup();
    }
  });

  // A consumer links the plugin from a checkout of its own, outside the
  // workspace, and its forks sit at different depths. Nx hashes a project's
  // configuration into every task of that project and of its dependents, so
  // the configuration must not know how deep the workspace is: measured, a
  // `../` count in the tool config path re-keyed 118 tasks of a fork of a warm
  // main.
  it('infers byte-identical cargo targets for a workspace at any depth', async () => {
    const parent = await realpath(await mkdtemp(join(tmpdir(), 'smoothbricks-nx-plugin-depth-')));
    const plugin = fileURLToPath(new URL('..', import.meta.url));
    const pluginNextestConfig = await readFile(join(plugin, 'nextest.toml'), 'utf8');
    try {
      const inferred: string[] = [];
      for (const dir of [parent, join(parent, 'a/b/c')]) {
        await mkdir(dir, { recursive: true });
        const workspace = await createWorkspace(dir);
        const root = workspace.context.workspaceRoot;
        await writeCargoIdentityFixture(workspace);
        const project = await inferProject(workspace, 'packages/ferris/package.json');
        // Installed the way a consumer links it: a checkout outside the
        // workspace. After inference, whose fixture walk would follow it.
        await mkdir(join(root, 'node_modules/@smoothbricks'), { recursive: true });
        await symlink(plugin, join(root, 'node_modules/@smoothbricks/nx-plugin'));
        // Stable is not enough: from where each command runs, the spelling
        // must name the config the plugin ships.
        const configured: string[] = [];
        for (const [name, target] of cachedCargoTargets(project?.targets ?? {})) {
          const named = /--tool-config-file "smoo:\$PWD\/([^"]+)"/.exec(String(target.options?.command));
          if (!named?.[1]) continue;
          configured.push(name);
          const config = await readFile(join(root, String(target.options?.cwd), named[1]), 'utf8');
          expect([name, config]).toEqual([name, pluginNextestConfig]);
        }
        expect(configured.sort()).toEqual([
          'cargo-cross-test-archive-aarch64-apple-darwin',
          'cargo-test-archive',
          'cargo-test-ferris-core',
        ]);
        inferred.push(JSON.stringify(project, null, 2));
      }
      expect(inferred[1]).toBe(inferred[0]);
    } finally {
      await rm(parent, { recursive: true, force: true });
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

      // Scoped by filterset over one archived workspace build: exactly one
      // crate runs, and no `-p` re-resolves its features.
      expect(cargoPackageSelection(String(targets['cargo-test-ferris-core']?.options?.command ?? ''))).toEqual({
        prebuiltRun: true,
        packageFlags: [],
        filtered: ['ferris-core'],
      });
      expect(cargoPackageSelection(String(targets['cargo-test-ferris-wasm']?.options?.command ?? ''))).toEqual({
        prebuiltRun: true,
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

  it('extracts the test archive once for all per-crate runners, and again only when its task changes', async () => {
    const workspace = await createWorkspace();
    const root = workspace.context.workspaceRoot;
    const crates = ['ferris-a', 'ferris-b', 'ferris-c'];
    try {
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = ["crates/*"]\n');
      for (const crate of crates) {
        await workspace.write(`packages/ferris/crates/${crate}/Cargo.toml`, `[package]\nname = "${crate}"\n`);
      }
      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');
      // Installed the way a consumer has it, so each command finds the helper it names.
      await mkdir(join(root, 'node_modules/@smoothbricks'), { recursive: true });
      await symlink(fileURLToPath(new URL('..', import.meta.url)), join(root, 'node_modules/@smoothbricks/nx-plugin'));
      // A cargo that does what nextest does with an archive, and says so: an
      // `--archive-file` unpacks it (into `--extract-to`, or a private temp
      // dir when a run is given the archive itself), and a run reports which
      // archive's binaries it executed. `metadata` names the workspace's
      // target directory, where the extraction root lives.
      const log = join(root, 'nextest.log');
      await workspace.write(
        'bin/cargo',
        [
          '#!/bin/sh',
          'archive= dest= binaries=',
          'while [ $# -gt 0 ]; do',
          '  case "$1" in',
          '    metadata) printf \'{"target_directory":"%s/target"}\\n\' "$PWD"; exit 0 ;;',
          '    --archive-file) archive=$2; shift ;;',
          '    --extract-to) dest=$2; shift ;;',
          '    --binaries-metadata) binaries=$2; shift ;;',
          '  esac',
          '  shift',
          'done',
          'if [ -n "$archive" ]; then',
          '  echo "extract $(cat "$archive")" >> "$FAKE_NEXTEST_LOG"',
          '  if [ -z "$dest" ]; then echo "run $(cat "$archive")" >> "$FAKE_NEXTEST_LOG"; exit 0; fi',
          '  mkdir -p "$dest/target/nextest"',
          '  cat "$archive" > "$dest/target/nextest/binaries-metadata.json"',
          '  cat "$archive" > "$dest/target/nextest/cargo-metadata.json"',
          '  exit 0',
          'fi',
          'echo "run $(cat "$binaries")" >> "$FAKE_NEXTEST_LOG"',
          '',
        ].join('\n'),
      );
      await chmod(join(root, 'bin/cargo'), 0o755);
      // Every runner at once, through `sh -c` exactly as bounded-exec spawns it.
      const runAll = () =>
        Promise.all(
          crates.map(async (crate) => {
            const options = targets[`cargo-test-${crate}`]?.options;
            const child = Bun.spawn(['sh', '-c', String(options?.command)], {
              cwd: join(root, String(options?.cwd)),
              env: { ...process.env, PATH: `${join(root, 'bin')}:${process.env.PATH}`, FAKE_NEXTEST_LOG: log },
              stdout: 'pipe',
              stderr: 'pipe',
            });
            const [exitCode, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
            return { crate, exitCode, stderr };
          }),
        );

      await workspace.write('packages/ferris/.cache/nextest/archive.tar.zst', 'first archive');
      // The key its cargo-test-archive task writes beside it: that task's hash.
      await workspace.write('packages/ferris/.cache/nextest/archive.tar.zst.key', '1001\n');
      for (const run of await runAll()) {
        expect(run).toMatchObject({ exitCode: 0 });
      }
      expect((await readFile(log, 'utf8')).trim().split('\n').sort()).toEqual([
        'extract first archive',
        'run first archive',
        'run first archive',
        'run first archive',
      ]);

      // A rebuilt archive is another task's output at the same path: the runners
      // must not execute what the old one unpacked, and the old unpacking is garbage.
      await workspace.write('packages/ferris/.cache/nextest/archive.tar.zst', 'second archive');
      await workspace.write('packages/ferris/.cache/nextest/archive.tar.zst.key', '1002\n');
      for (const run of await runAll()) {
        expect(run).toMatchObject({ exitCode: 0 });
      }
      expect((await readFile(log, 'utf8')).trim().split('\n').sort()).toEqual([
        'extract first archive',
        'extract second archive',
        'run first archive',
        'run first archive',
        'run first archive',
        'run second archive',
        'run second archive',
        'run second archive',
      ]);
      // The old unpacking is gone from Cargo's build tree, and the archive's
      // directory holds only the archive and its key, the declared Nx outputs.
      const extracted = join(root, 'packages/ferris/target', NEXTEST_EXTRACTION_DIRECTORY);
      const unpacked = (await readdir(extracted, { recursive: true })).filter((path) =>
        path.endsWith('binaries-metadata.json'),
      );
      expect(await Promise.all(unpacked.map((path) => readFile(join(extracted, path), 'utf8')))).toEqual([
        'second archive',
      ]);
      expect((await readdir(join(root, 'packages/ferris/.cache/nextest'))).sort()).toEqual([
        'archive.tar.zst',
        'archive.tar.zst.key',
      ]);
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
        outputs: ['{projectRoot}/generated/wasm/'],
        options: {
          command: [
            'cargo --frozen build --target wasm32-unknown-unknown --manifest-path crates/git-do/Cargo.toml',
            'wasm-bindgen --target nodejs --out-dir generated/wasm/node crates/git-do/target/wasm32-unknown-unknown/debug/gitoxide_engine.wasm',
            'wasm-bindgen --target web --out-dir generated/wasm/web crates/git-do/target/wasm32-unknown-unknown/debug/gitoxide_engine.wasm',
          ].join(' && '),
          cwd: 'packages/git-do',
        },
      });
      // No workspace claims the crate, so cargo's default target dir is the one
      // beside its own manifest, and the release configuration reads the
      // release artifact from it.
      expect(targets['cargo-wasm']?.options?.commands).toBeUndefined();
      expect(targets['cargo-wasm']?.configurations?.[RELEASE_CONFIGURATION]).toEqual({
        command: [
          'cargo --frozen build --release --target wasm32-unknown-unknown --manifest-path crates/git-do/Cargo.toml',
          'wasm-bindgen --target nodejs --out-dir generated/wasm/node crates/git-do/target/wasm32-unknown-unknown/release/gitoxide_engine.wasm',
          'wasm-bindgen --target web --out-dir generated/wasm/web crates/git-do/target/wasm32-unknown-unknown/release/gitoxide_engine.wasm',
        ].join(' && '),
      });
      // A wasm-bindgen crate in a nested Cargo workspace still needs its locked
      // graph downloaded, and the project root is not a member of it, so the
      // fetch names the crate manifest instead of relying on cwd.
      expect(targets['cargo-fetch']?.options).toEqual({
        commands: ['cargo fetch --locked --manifest-path crates/git-do/Cargo.toml'],
        cwd: 'packages/git-do',
      });
      expect(targets['tsc-js']?.dependsOn).toEqual(['^*-js', 'cargo-wasm']);
      expect(targets.typecheck?.dependsOn).toEqual(['^*-js', 'cargo-wasm']);
      expect(targets.build?.dependsOn).toEqual(['^build', 'cargo-wasm', 'tsc-js']);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
      // A nested crate manifest is enough for output inference, but not for
      // workspace-wide cargo-test/lint policy.
      expect(targets['cargo-test']).toBeUndefined();
      expect(targets['cargo-lint']).toBeUndefined();
      expectNoSchedulerThrottle(targets);
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
      expect(targets['tsc-js']?.outputs).toEqual(['{projectRoot}/dist/ts/']);
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

      // A host no declared triple serves infers the dedicated host build. Here the host is darwin-arm64 and the
      // package declares Linux addons alone.
      const linuxOnly = await readFile(join(workspace.context.workspaceRoot, 'packages/cowshed/package.json'), 'utf8');
      const manifest: unknown = JSON.parse(linuxOnly);
      if (!isRecord(manifest) || !isRecord(manifest.napi))
        throw new Error('the cowshed fixture manifest declares no napi block');
      manifest.napi.targets = ['aarch64-unknown-linux-gnu', 'x86_64-unknown-linux-gnu'];
      await workspace.write('packages/cowshed/package.json', JSON.stringify(manifest));
      const unservedHostTargets = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('darwin', 'arm64'),
      );
      const buildInputs =
        '../../node_modules/@smoothbricks/nx-plugin/../build-inputs/dist/bin/darwin-arm64/smoo-nx-build-inputs';
      expect(unservedHostTargets['cargo-napi']).toMatchObject({
        executor: 'nx:run-commands',
        cache: true,
        // The dedicated host build consumes only its dependencies' outputs; it is
        // not ordered behind the test compile.
        dependsOn: ['^build'],
        outputs: ['{projectRoot}/dist/native/host/'],
        options: {
          cwd: 'packages/cowshed',
          command: `napi build --platform --no-js --dts cowshed.napi.d.ts --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir dist/native/host && ${buildInputs} --manifest-path crates/cowshed-napi/Cargo.toml --dep-info debug/libcowshed_napi.d --output dist/native/host`,
        },
        configurations: {
          [RELEASE_CONFIGURATION]: {
            command: `napi build --release --platform --no-js --dts cowshed.napi.d.ts --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir dist/native/host && ${buildInputs} --manifest-path crates/cowshed-napi/Cargo.toml --dep-info release/libcowshed_napi.d --output dist/native/host`,
          },
        },
      });
      expect(unservedHostTargets.build?.dependsOn).toEqual([
        '^build',
        'cargo-test-archive',
        'cargo-napi',
        'tsc-js',
        'cli-arm64-macos',
      ]);
      // A platform no scriptc platform covers has no build-inputs executable for the napi commands to run.
      const unservedFailure = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('win32', 'x64'),
      ).then(
        () => undefined,
        (error: unknown) => error,
      );
      if (!(unservedFailure instanceof AggregateCreateNodesError)) {
        throw new Error(`expected AggregateCreateNodesError, got ${String(unservedFailure)}`);
      }
      expect(unservedFailure.errors[0]?.[1]?.message).toMatch(
        /ships smoo-nx-build-inputs for darwin-arm64, darwin-x64, linux-arm64 and linux-x64 only/,
      );
      await workspace.write('packages/cowshed/package.json', linuxOnly);
      expect(targets['napi-arm64-macos']?.outputs).toEqual(['{projectRoot}/dist/native/darwin-arm64/']);
      // Every local build compiles dev; only the release configuration ships.
      const darwinArm64Build = (release: string): string =>
        `napi build${release} --platform --no-js --dts cowshed.darwin-arm64.d.ts --target aarch64-apple-darwin --manifest-path crates/cowshed-napi/Cargo.toml --package cowshed-napi --package-json-path package.json --output-dir dist/native/darwin-arm64 && ../../node_modules/@smoothbricks/nx-plugin/../build-inputs/dist/bin/${HOST_TOOLS}/smoo-nx-build-inputs --manifest-path crates/cowshed-napi/Cargo.toml --dep-info aarch64-apple-darwin/${release === '' ? 'debug' : 'release'}/libcowshed_napi.d --output dist/native/darwin-arm64`;
      expect(targets['napi-arm64-macos']?.options).toMatchObject({ command: darwinArm64Build('') });
      expect(targets['napi-arm64-macos']?.configurations?.[RELEASE_CONFIGURATION]).toEqual({
        command: darwinArm64Build(' --release'),
      });
      expect(targets['napi-arm64-macos']?.options?.env).toBeUndefined();
      // No cargo-test-archive edge here, on ANY host: the platform build consumes
      // nothing from it, and a platform target's dependency closure may only reach
      // its own family (`validatePlatformTargetDependencies`).
      expect(targets['napi-arm64-macos']?.dependsOn).toBeUndefined();
      // Every host infers the SAME Linux addon target: devenv's zig through the
      // cargo-zigbuild executor, glibc pinned by the triple suffix. There is no
      // native-versus-cross branch to diverge on, so forcing a Linux host and a
      // macOS host must yield byte-identical Linux targets, and neither may
      // carry a compiler environment or a toolchain prerequisite.
      const linuxX64Targets = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('linux', 'x64'),
      );
      const darwinArm64Targets = await inferProjectTargets(
        workspace,
        'packages/cowshed/package.json',
        createNodesV2ForPlatform('darwin', 'arm64'),
      );
      expect(darwinArm64Targets['napi-x64-linux']).toMatchObject({
        executor: '@smoothbricks/nx-plugin:cargo-zigbuild',
        cache: true,
        outputs: ['{projectRoot}/dist/native/linux-x64-gnu/'],
        options: {
          target: 'x86_64-unknown-linux-gnu.2.17',
          manifestPath: 'packages/cowshed/crates/cowshed-napi/Cargo.toml',
          package: 'cowshed-napi',
          output: '{projectRoot}/dist/native/linux-x64-gnu/cowshed.linux-x64-gnu.node',
        },
        configurations: { [RELEASE_CONFIGURATION]: { release: true } },
      });
      expect(darwinArm64Targets['napi-arm64-linux']?.options).toMatchObject({
        target: 'aarch64-unknown-linux-gnu.2.17',
        output: '{projectRoot}/dist/native/linux-arm64-gnu/cowshed.linux-arm64-gnu.node',
      });
      for (const suffix of ['arm64-linux', 'x64-linux']) {
        expect(linuxX64Targets[`napi-${suffix}`]).toEqual(darwinArm64Targets[`napi-${suffix}`]);
        expect(darwinArm64Targets[`napi-${suffix}`]?.dependsOn).toBeUndefined();
      }
      for (const inferred of [linuxX64Targets, darwinArm64Targets]) {
        expect(Object.keys(inferred).filter((name) => name.startsWith('napi-toolchain-'))).toEqual([]);
        expect(inferred['napi-debug']?.options?.env).toBeUndefined();
        for (const suffix of ['arm64-macos', 'x64-macos']) {
          expect(inferred[`napi-${suffix}`]?.options?.command).toContain('napi build');
          expect(inferred[`napi-${suffix}`]?.options?.env).toBeUndefined();
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
        expect(inferred.build?.dependsOn).toEqual(['^build', 'cargo-test-archive', 'tsc-js', ...hostNapiTargets]);
        // Crate `cowshed-napi` names its bounded runner `cargo-test-cowshed-napi`;
        // the retired `*-napi` output-family glob matched it on suffix alone and
        // put the whole cargo test suite inside `nx run-many -t build`.
        expect(inferred['cargo-test-cowshed-napi']).toBeDefined();
        for (const dependency of inferred.build?.dependsOn ?? []) {
          expect(String(dependency).startsWith('cargo-test-')).toBe(dependency === 'cargo-test-archive');
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
        dependsOn: ['^build'],
        outputs: ['{projectRoot}/.cache/native-debug/'],
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
        /^extracted="\$\(\.\.\/\.\.\/node_modules\/@smoothbricks\/nx-plugin\/\.\.\/nextest-extract\/dist\/bin\/[a-z0-9-]+\/smoo-nx-nextest-extract \.cache\/nextest\/archive\.tar\.zst\)" && cargo --frozen nextest run --binaries-metadata "\$extracted\/target\/nextest\/binaries-metadata\.json" --cargo-metadata "\$extracted\/target\/nextest\/cargo-metadata\.json" --target-dir-remap "\$extracted\/target" --workspace-remap \. -E 'package\(cowshed-napi\)' --no-fail-fast --no-tests=pass --user-config-file none --tool-config-file "smoo:\$PWD\/.*nextest\.toml"$/,
      );
      expect(targets['napi-test']).toMatchObject({
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        cache: true,
        dependsOn: ['cargo-test', 'napi-debug', 'tsc-js', '^build'],
        options: {
          command: 'bun test --config=../bunfig.toml --timeout=0 native.test.ts',
          cwd: 'packages/cowshed/src',
          env: { NAPI_DEBUG_ADDON: '1' },
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          killAfterMs: 10000,
        },
      });
      expectNoSchedulerThrottle(targets);
      await workspace.write('packages/cowshed/bunfig.napi-test.toml', '[test]\n');
      const targetsWithDedicatedBunfig = await inferProjectTargets(workspace, 'packages/cowshed/package.json');
      expect(targetsWithDedicatedBunfig['napi-test']?.options?.command).toBe(
        'bun test --config=../bunfig.napi-test.toml --timeout=0 native.test.ts',
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
      expect(targets.build?.dependsOn).toEqual(['^build', 'cargo-test-archive', 'cargo-wasm']);
      expect(targets.clean?.executor).toBe('@smoothbricks/nx-plugin:clean-outputs');
    } finally {
      await workspace.cleanup();
    }
  });

  it('binds the wasm a package workspace member writes into that workspace target dir', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/columine/package.json', '{"name":"columine"}\n');
      await workspace.write('packages/columine/Cargo.toml', '[workspace]\nmembers = ["crates/columine-wasm"]\n');
      await workspace.write(
        'packages/columine/crates/columine-wasm/Cargo.toml',
        '[package]\nname = "columine-wasm"\n\n[lib]\ncrate-type = ["cdylib", "rlib"]\n\n[package.metadata.smoothbricks.wasm-bindgen]\ntargets = ["web"]\n',
      );

      const targets = await inferProjectTargets(workspace, 'packages/columine/package.json');

      // Cargo writes a member's artifacts under its workspace root, not beside
      // the member's manifest, so the bindgen input is read from there.
      expect(targets['cargo-wasm']?.options?.command).toBe(
        [
          'cargo --frozen build --target wasm32-unknown-unknown --manifest-path crates/columine-wasm/Cargo.toml',
          'wasm-bindgen --target web --out-dir dist-wasm/web target/wasm32-unknown-unknown/debug/columine_wasm.wasm',
        ].join(' && '),
      );
    } finally {
      await workspace.cleanup();
    }
  });

  it('partitions ordinary, exceptional and measured tests into disjoint shards of the same declared count', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/rusty/package.json', '{"name":"rusty"}\n');
      await workspace.write(
        'packages/rusty/Cargo.toml',
        '[workspace]\nmembers = ["crates/big", "crates/small", "crates/timed"]\n',
      );
      await workspace.write(
        'packages/rusty/crates/big/Cargo.toml',
        '[package]\nname = "big"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
      );
      await workspace.write('packages/rusty/crates/small/Cargo.toml', '[package]\nname = "small"\n');
      await workspace.write(
        'packages/rusty/crates/timed/Cargo.toml',
        '[package]\nname = "timed"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
      );
      const timings = 'packages/rusty/.config/nextest-timings.json';
      await workspace.write(timings, '{"timed":{"slow::a":9000,"slow::b":5000,"slow::c":4000,"slow::d":3000}}\n');

      const targets = await inferProjectTargets(workspace, 'packages/rusty/package.json');

      // An unsharded crate keeps the bare name and takes no --partition, so
      // declaring nothing is exactly the old single-target behaviour.
      expect(targets['cargo-test-small']?.options?.command).toMatch(
        /--workspace-remap \. -E 'package\(small\)' --no-fail-fast --no-tests=pass --user-config-file none/,
      );
      // The shards partition the crate MINUS the classes nextest.toml singles
      // out, i of N. Those are lifted out because a test carrying a raised
      // slow-timeout costs what the suite does not — the compile-fail test
      // rustc's a fixture for 25.6s on a cold target dir against 1.8s warm.
      const exceptional = exceptionalTestFilter(fileURLToPath(new URL('../nextest.toml', import.meta.url)));
      if (exceptional === null) {
        throw new Error('nextest.toml declares no overrides; this test asserts the pin that protects them');
      }
      for (const index of [1, 2, 3]) {
        expect(targets[`cargo-test-big-shard${index}`]?.options?.command).toContain(
          `-E 'package(big) and not (${exceptional})' --partition hash:${index}/3 --no-fail-fast --no-tests=pass`,
        );
        expect(targets[`cargo-test-big-shard${index}`]?.options?.idleTimeoutMs).toBe(BOUNDED_TEST_IDLE_TIMEOUT_MS);
        expect(targets[`cargo-test-big-shard${index}`]?.options).not.toHaveProperty('timeoutMs');
      }
      // Exceptional partitions are the exact complement, hashed by the same
      // declared count; each keeps the full bounded window.
      for (const index of [1, 2, 3]) {
        const name = `cargo-test-big-exceptions-shard${index}`;
        expect(targets[name]?.options?.command).toContain(
          `-E 'package(big) and (${exceptional})' --partition hash:${index}/3 --no-fail-fast --no-tests=pass`,
        );
        expect(targets[name]?.options?.idleTimeoutMs).toBe(BOUNDED_TEST_IDLE_TIMEOUT_MS);
        expect(packageNameFromCargoTestTarget(name)).toBe('big');
      }
      expect(targets['cargo-test-big-exceptions']).toBeUndefined();
      // An unsharded crate needs no pin: its whole suite is already one run.
      expect(targets['cargo-test-small-exceptions']).toBeUndefined();
      expect(targets['cargo-test-big']).toBeUndefined();
      // Every piece reaches the aggregate, and they all hang off the one
      // archive: a run extracts binaries to its own temp directory and writes
      // nothing to cargo's target/, so no piece waits on another.
      expect(targets['cargo-test']?.dependsOn).toEqual([
        'cargo-test-big-shard1',
        'cargo-test-big-shard2',
        'cargo-test-big-shard3',
        'cargo-test-big-exceptions-shard1',
        'cargo-test-big-exceptions-shard2',
        'cargo-test-big-exceptions-shard3',
        'cargo-test-small',
        'cargo-test-timed-shard1',
        'cargo-test-timed-shard2',
        'cargo-test-timed-shard3',
        'cargo-test-timed-exceptions-shard1',
        'cargo-test-timed-exceptions-shard2',
        'cargo-test-timed-exceptions-shard3',
      ]);
      for (const name of [
        'cargo-test-big-shard1',
        'cargo-test-big-shard2',
        'cargo-test-big-shard3',
        'cargo-test-big-exceptions-shard1',
        'cargo-test-big-exceptions-shard2',
        'cargo-test-big-exceptions-shard3',
        'cargo-test-small',
      ]) {
        expect(targets[name]?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      }
      // Pieces of one crate share its inputs: they are one suite, split only to
      // fit the bound, so any change to the crate invalidates all of them.
      expect(targets['cargo-test-big-shard2']?.inputs).toEqual(targets['cargo-test-big-shard1']?.inputs);
      expect(targets['cargo-test-big-exceptions-shard1']?.inputs).toEqual(targets['cargo-test-big-shard1']?.inputs);
      // Exceptional pieces fan out like the ordinary ones: nothing is exclusive.
      expectNoSchedulerThrottle(targets);

      // A crate the timing manifest measures hashes only what it does not name; its exception shards
      // take the named tests longest first onto the least-loaded shard (a; b; then c and d, as c left
      // the third shard lightest), and the last also takes the exceptional tests it does not name.
      const named = 'test(=slow::a) or test(=slow::b) or test(=slow::c) or test(=slow::d)';
      for (const index of [1, 2, 3]) {
        expect(targets[`cargo-test-timed-shard${index}`]?.options?.command).toContain(
          `-E 'package(timed) and not ((${exceptional}) or (${named}))' --partition hash:${index}/3 --no-fail-fast`,
        );
      }
      for (const [index, own] of [
        [1, 'test(=slow::a)'],
        [2, 'test(=slow::b)'],
        [3, `test(=slow::c) or test(=slow::d)) or ((${exceptional}) and not (${named})`],
      ] as const) {
        expect(targets[`cargo-test-timed-exceptions-shard${index}`]?.options?.command).toContain(
          `-E 'package(timed) and ((${own}))' --no-fail-fast --no-tests=pass`,
        );
      }

      // The manifest may only time a crate that shards, by test paths a filter names literally.
      for (const [manifest, refusal] of [
        ['{"small":{"slow::a":9000}}', /times small, which is no member of this Cargo workspace that declares/],
        ['{"timed":{"slow(a)":9000}}', /timed names "slow\(a\)", which is not a Rust test path/],
      ] as const) {
        await workspace.write(timings, manifest);
        const failure = await inferProjectTargets(workspace, 'packages/rusty/package.json').then(
          () => undefined,
          (error: unknown) => error,
        );
        if (!(failure instanceof AggregateCreateNodesError)) {
          throw new Error(`${manifest}: expected AggregateCreateNodesError, got ${String(failure)}`);
        }
        expect(failure.errors[0]?.[1]?.message).toMatch(refusal);
      }
    } finally {
      await workspace.cleanup();
    }
  });

  it('runs the serial test group once, unpartitioned, and excludes its tests from every crate piece on macOS', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/rusty/package.json', '{"name":"rusty"}\n');
      await workspace.write('packages/rusty/Cargo.toml', '[workspace]\nmembers = ["crates/big", "crates/small"]\n');
      await workspace.write(
        'packages/rusty/crates/big/Cargo.toml',
        '[package]\nname = "big"\n\n[package.metadata.smoothbricks.test]\nshards = 3\n',
      );
      await workspace.write('packages/rusty/crates/small/Cargo.toml', '[package]\nname = "small"\n');
      await workspace.write(
        'packages/rusty/.config/nextest.toml',
        [
          '[test-groups.real-apfs]',
          'max-threads = 1',
          '',
          '[[profile.default.overrides]]',
          "filter = 'test(/real_apfs_/)'",
          `platform = 'cfg(target_os="macos")'`,
          "test-group = 'real-apfs'",
          '',
        ].join('\n'),
      );

      const macos = await inferProjectTargets(
        workspace,
        'packages/rusty/package.json',
        createNodesV2ForPlatform('darwin', 'arm64'),
      );
      const serial = '(test(/real_apfs_/))';
      const group = macos['cargo-test-group-real-apfs'];
      // One runner for the workspace, owned by the cargo root beside the archive it executes: a progress bound and no
      // ceiling like every test target, one CPU token, no partition and no thread flag. A filter that selects
      // nothing fails the run.
      expect(group?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(group?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      expect(String(group?.options?.command)).toContain('nextest run ');
      expect(String(group?.options?.command)).toContain(
        `-E '${serial}' --no-fail-fast --no-tests=fail --user-config-file none`,
      );
      expect(group?.options).not.toHaveProperty('timeoutMs');
      expect(group?.options?.idleTimeoutMs).toBe(BOUNDED_TEST_IDLE_TIMEOUT_MS);
      expect(group?.options?.killAfterMs).toBe(BOUNDED_TEST_KILL_AFTER_MS);
      expect(group?.options?.parallelism).toBe(1);
      expect(group?.inputs).toContain('{workspaceRoot}/packages/rusty/.config/nextest.toml');
      expect(JSON.stringify(group?.inputs)).toContain('nextest-code-bound.ts');
      expect(JSON.stringify(macos['cargo-test-small']?.inputs)).not.toContain('nextest-code-bound.ts');
      expect(String(group?.options?.command)).not.toMatch(/--partition|test-threads|max-fail/);

      // Every crate piece excludes the group's tests, so each test is selected by exactly one target.
      const exceptional = exceptionalTestFilter(fileURLToPath(new URL('../nextest.toml', import.meta.url)));
      if (exceptional === null) {
        throw new Error('nextest.toml declares no overrides; this test asserts the pin that protects them');
      }
      expect(macos['cargo-test-small']?.options?.command).toContain(
        `-E 'package(small) and not ((test(/real_apfs_/)))' --no-fail-fast --no-tests=pass`,
      );
      for (const index of [1, 2, 3]) {
        expect(macos[`cargo-test-big-shard${index}`]?.options?.command).toContain(
          `-E 'package(big) and not (${exceptional}) and not ((test(/real_apfs_/)))' --partition hash:${index}/3 --no-fail-fast --no-tests=pass`,
        );
        expect(macos[`cargo-test-big-exceptions-shard${index}`]?.options?.command).toContain(
          `-E 'package(big) and (${exceptional}) and not ((test(/real_apfs_/)))' --partition hash:${index}/3 --no-fail-fast --no-tests=pass`,
        );
      }
      // `cargo-test` and `test` are the ordinary suite alone: neither infers an edge to the group.
      expect(macos['cargo-test']?.dependsOn).not.toContain('cargo-test-group-real-apfs');
      expect(macos.test?.dependsOn).toEqual(['cargo-test']);

      // Off macOS nextest applies the group nowhere, so it is not inferred and no piece excludes its tests.
      const linux = await inferProjectTargets(
        workspace,
        'packages/rusty/package.json',
        createNodesV2ForPlatform('linux', 'x64'),
      );
      expect(linux['cargo-test-group-real-apfs']).toBeUndefined();
      expect(linux['cargo-test-small']?.options?.command).toContain(
        `-E 'package(small)' --no-fail-fast --no-tests=pass`,
      );
      expect(linux['cargo-test']?.dependsOn).not.toContain('cargo-test-group-real-apfs');
      expect(linux.test?.dependsOn).toEqual(['cargo-test']);
    } finally {
      await workspace.cleanup();
    }
  });

  it("scopes each serial group's runner inputs to its packages and their closure, and keeps an unscoped group workspace-wide", async () => {
    const workspace = await createWorkspace();
    try {
      await writeApfsSplitFixture(workspace, [
        ...APFS_SPLIT_GROUPS,
        '[test-groups.wide]',
        'max-threads = 1',
        '',
        ...serialOverride('test(/wide_/)', 'wide'),
      ]);
      const root = (await inferApfsSplit(workspace)).get('.') ?? {};
      const device = root['cargo-test-group-real-apfs'];
      const host = root['cargo-test-group-real-apfs-host'];
      const wide = root['cargo-test-group-wide'];

      // A group scoped to packages hashes those crates and every crate they link, and no other.
      expect(hashedCrates(device)).toEqual(['cowshed-apfs', 'cowshed-apfs-tests', 'cowshed-os']);
      expect(hashedCrates(host)).toEqual(['cowshed-apfs', 'cowshed-cli', 'cowshed-core', 'cowshed-os']);
      // A filter scoped to no package keeps every member's tree, as the archive hashes them.
      expect(hashedCrates(wide)).toEqual([
        'cowshed-apfs',
        'cowshed-apfs-tests',
        'cowshed-cli',
        'cowshed-core',
        'cowshed-os',
        'lmao-core',
      ]);

      // The proof as the three cases it makes. An unrelated crate reruns neither scoped group. A scenario
      // edit reruns the device group alone: the scenarios are a crate of their own that no host crate
      // depends on. A change to the production substrate, or to what it links, reruns both, because the
      // host crates link it too. Nothing here excludes a tests tree, and the test support every real-image
      // binary compiles in stays hashed for both groups.
      const hashedBy = (path: string) => ({
        device: hashes(device, path),
        host: hashes(host, path),
        wide: hashes(wide, path),
      });
      expect(hashedBy('packages/lmao/crates/lmao-core/src/lib.rs')).toEqual({ device: false, host: false, wide: true });
      expect(hashedBy('packages/cowshed/crates/cowshed-apfs-tests/src/tests.rs')).toEqual({
        device: true,
        host: false,
        wide: true,
      });
      expect(hashedBy('packages/cowshed/crates/cowshed-core/src/lib.rs')).toEqual({
        device: false,
        host: true,
        wide: true,
      });
      expect(hashedBy('packages/cowshed/crates/cowshed-cli/src/lib.rs')).toEqual({
        device: false,
        host: true,
        wide: true,
      });
      for (const shared of [
        'packages/cowshed/crates/cowshed-apfs/src/lib.rs',
        'packages/cowshed/crates/cowshed-apfs/tests/support/blank_image.rs',
        'packages/cowshed/crates/cowshed-os/src/lib.rs',
      ]) {
        expect(hashedBy(shared), shared).toEqual({ device: true, host: true, wide: true });
      }
      for (const group of [device, host]) {
        const exclusions = (group?.inputs ?? []).filter(
          (input): input is string => typeof input === 'string' && input.startsWith('!'),
        );
        expect(exclusions.filter((input) => /\btests?\b/.test(input))).toEqual([]);
      }

      // Every scoped group still hashes every member manifest, the repository's nextest config and the
      // run-wrapper its tests are bounded by: those decide a verdict whichever crates it selects.
      for (const group of [device, host, wide]) {
        expect(group?.inputs).toContain('{workspaceRoot}/packages/lmao/crates/lmao-core/Cargo.toml');
        expect(group?.inputs).toContain('{workspaceRoot}/.config/nextest.toml');
        expect(JSON.stringify(group?.inputs)).toContain('nextest-code-bound.ts');
      }
    } finally {
      await workspace.cleanup();
    }
  });

  // A runner keyed every closure crate's dev-dependencies, though Cargo builds a dependency for its library alone,
  // so a dependency's test helper re-keyed every crate above it (35 of 87 runners in one downstream workspace).
  it("keys a crate's runner on every crate its dependencies link, and on their dev-dependencies' manifests alone", async () => {
    const workspace = await createWorkspace();
    try {
      await writeApfsSplitFixture(workspace, APFS_SPLIT_GROUPS);
      // cowshed-core tests with lmao-core; cowshed-cli links cowshed-core.
      await workspace.write(
        'packages/cowshed/crates/cowshed-core/Cargo.toml',
        [
          '[package]',
          'name = "cowshed-core"',
          '',
          '[dependencies]',
          'cowshed-apfs = { path = "../cowshed-apfs" }',
          'cowshed-os = { path = "../cowshed-os" }',
          '',
          '[dev-dependencies]',
          'lmao-core = { path = "../../../lmao/crates/lmao-core" }',
          '',
        ].join('\n'),
      );
      const cowshed = (await inferApfsSplit(workspace)).get('packages/cowshed') ?? {};
      const cli = cowshed['cargo-test-cowshed-cli'];
      const core = cowshed['cargo-test-cowshed-core'];

      expect(hashedCrates(cli)).toEqual(['cowshed-apfs', 'cowshed-cli', 'cowshed-core', 'cowshed-os']);
      expect(hashedCrates(core)).toEqual(['cowshed-apfs', 'cowshed-core', 'cowshed-os', 'lmao-core']);
      const hashedBy = (path: string) => ({ cli: hashes(cli, path), core: hashes(core, path) });
      // An edit to the dependency's test helper reruns that dependency's tests alone.
      expect(hashedBy('packages/lmao/crates/lmao-core/src/lib.rs')).toEqual({ cli: false, core: true });
      // An edit to a crate the dependency links reruns both.
      expect(hashedBy('packages/cowshed/crates/cowshed-os/src/lib.rs')).toEqual({ cli: true, core: true });
      expect(hashedBy('packages/cowshed/crates/cowshed-core/src/lib.rs')).toEqual({ cli: true, core: true });
      // The helper's manifest stays in both keys: the workspace archive unifies its features into every binary.
      for (const runner of [cli, core]) {
        expect(runner?.inputs).toContain('{workspaceRoot}/packages/lmao/crates/lmao-core/Cargo.toml');
      }
    } finally {
      await workspace.cleanup();
    }
  });

  // A path package outside the Nx workspace that only some member's tests build is no fileset, yet the workspace
  // archive unifies its manifest's features into every test binary, and a runner's key does not follow the archive's
  // outputs. Every runner therefore names the repository's runtime hash of external sources.
  it('keys every runner on externalRustCrates while the workspace builds a path package outside Nx', async () => {
    const outside = await realpath(await mkdtemp(join(tmpdir(), 'smoothbricks-nx-plugin-outside-')));
    const workspace = await createWorkspace(outside);
    try {
      await writeApfsSplitFixture(workspace, APFS_SPLIT_GROUPS);
      await workspace.write('nx.json', JSON.stringify({ namedInputs: { externalRustCrates: [] } }));
      await mkdir(join(outside, 'external/src'), { recursive: true });
      await writeFile(join(outside, 'external/Cargo.toml'), '[package]\nname = "external"\n\n[workspace]\n');
      await writeFile(join(outside, 'external/src/lib.rs'), '');
      // Only cowshed-core's tests build it.
      await workspace.write(
        'packages/cowshed/crates/cowshed-core/Cargo.toml',
        [
          '[package]',
          'name = "cowshed-core"',
          '',
          '[dependencies]',
          'cowshed-apfs = { path = "../cowshed-apfs" }',
          'cowshed-os = { path = "../cowshed-os" }',
          '',
          '[dev-dependencies]',
          `external = { path = ${JSON.stringify(join(outside, 'external'))} }`,
          '',
        ].join('\n'),
      );
      const inferred = await inferApfsSplit(workspace);
      const cowshed = inferred.get('packages/cowshed') ?? {};
      const runners = {
        cli: cowshed['cargo-test-cowshed-cli'],
        core: cowshed['cargo-test-cowshed-core'],
        lmao: inferred.get('packages/lmao')?.['cargo-test-lmao-core'],
      };
      for (const [name, runner] of Object.entries(runners)) {
        expect(runner?.inputs, name).toContain('externalRustCrates');
      }
    } finally {
      await rm(outside, { recursive: true, force: true });
    }
  });
  it('refuses a serial group scoped to a package that is not a member of the Cargo workspace', async () => {
    const workspace = await createWorkspace();
    try {
      await writeApfsSplitFixture(workspace, [
        '[test-groups.real-apfs]',
        'max-threads = 1',
        '',
        ...serialOverride('package(cowshed-missing) and test(/real_apfs_/)', 'real-apfs'),
      ]);
      // Nx wraps a createNodes throw in AggregateCreateNodesError, so the reason is in the nested error.
      const failure = await inferApfsSplit(workspace).then(
        () => undefined,
        (error: unknown) => error,
      );
      if (!(failure instanceof AggregateCreateNodesError)) {
        throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
      }
      expect(failure.errors[0]?.[1]?.message).toMatch(
        /test-group "real-apfs" selects package\(cowshed-missing\), which is not a member of the Cargo workspace/,
      );
    } finally {
      await workspace.cleanup();
    }
  });

  it('keeps cargo-test and test to the ordinary suite, so no gate of the root or a member reaches a serial group', async () => {
    const workspace = await createWorkspace();
    try {
      await writeApfsSplitFixture(workspace, APFS_SPLIT_GROUPS);
      const macos = await inferApfsSplit(workspace);
      const root = macos.get('.') ?? {};
      const cowshed = macos.get('packages/cowshed') ?? {};
      const lmao = macos.get('packages/lmao') ?? {};

      // `cargo-test` is the ordinary suite alone, whichever project owns the aggregate.
      expect(root['cargo-test']?.dependsOn).toEqual([
        { projects: ['cowshed'], target: 'cargo-test-cowshed-apfs' },
        { projects: ['cowshed'], target: 'cargo-test-cowshed-apfs-tests' },
        { projects: ['cowshed'], target: 'cargo-test-cowshed-cli' },
        { projects: ['cowshed'], target: 'cargo-test-cowshed-core' },
        { projects: ['cowshed'], target: 'cargo-test-cowshed-os' },
        { projects: ['lmao'], target: 'cargo-test-lmao-core' },
      ]);
      expect(cowshed['cargo-test']?.dependsOn).toEqual([
        'cargo-test-cowshed-apfs',
        'cargo-test-cowshed-apfs-tests',
        'cargo-test-cowshed-cli',
        'cargo-test-cowshed-core',
        'cargo-test-cowshed-os',
      ]);
      expect(lmao['cargo-test']?.dependsOn).toEqual(['cargo-test-lmao-core']);

      // Ordinary test inference excludes serial groups at both the root and every member. Explicit repository
      // host aggregates, not inference, own those edges.
      for (const targets of [root, cowshed, lmao]) {
        expect(targets.test?.dependsOn).toEqual(['cargo-test']);
      }

      // Inference orders nothing between the groups: that edge is the repository's explicit config.
      for (const name of ['cargo-test-group-real-apfs', 'cargo-test-group-real-apfs-host']) {
        expect(root[name]?.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive']);
      }

      // Every crate piece, scoped to a package the groups hold or not, excludes both groups' tests.
      const exclusion =
        'not ((package(cowshed-apfs-tests) and test(/real_apfs_/)) or ' +
        '(package(cowshed-core) and (test(/real_apfs_/) or binary(apfs_native_macos))) or ' +
        '(package(cowshed-cli) and test(/real_apfs_/)))';
      const pieces: ReadonlyArray<readonly [TargetConfiguration | undefined, string]> = [
        [cowshed['cargo-test-cowshed-apfs'], 'cowshed-apfs'],
        [cowshed['cargo-test-cowshed-apfs-tests'], 'cowshed-apfs-tests'],
        [lmao['cargo-test-lmao-core'], 'lmao-core'],
      ];
      for (const [piece, crate] of pieces) {
        expect(piece?.options?.command, crate).toContain(
          `-E 'package(${crate}) and ${exclusion}' --no-fail-fast --no-tests=pass`,
        );
      }

      // Off macOS the groups are not inferred at all, and the gate is the same ordinary suite.
      const linux = await inferApfsSplit(workspace, createNodesV2ForPlatform('linux', 'x64'));
      expect(linux.get('.')?.test?.dependsOn).toEqual(['cargo-test']);
      expect(linux.get('packages/cowshed')?.test?.dependsOn).toEqual(['cargo-test']);
      expect(linux.get('.')?.['cargo-test-group-real-apfs-host']).toBeUndefined();
    } finally {
      await workspace.cleanup();
    }
  });

  it('runs the host group after the device group through the explicit targetDefaults edge, and no test gate runs either', async () => {
    const workspace = await createWorkspace();
    try {
      await writeApfsSplitFixture(workspace, APFS_SPLIT_GROUPS);
      const macos = await inferApfsSplit(workspace);
      const rootProject = '@fixture/codebase';
      const rootTargets = macos.get('.') ?? {};

      // The edge as the repository's nx.json states it. Nx expands `...` to the inferred list, so the
      // archive and the locked fetch stay ahead of the host group beside the device group.
      const nxJson = {
        targetDefaults: { 'cargo-test-group-real-apfs-host': { dependsOn: ['...', 'cargo-test-group-real-apfs'] } },
      };
      let host = rootTargets['cargo-test-group-real-apfs-host'] ?? {};
      for (const [, , result] of createTargetDefaultsResults(
        { '.': { root: '.', targets: rootTargets } },
        {},
        nxJson,
      )) {
        const target = result.projects?.['.']?.targets?.['cargo-test-group-real-apfs-host'];
        if (target) host = mergeTargetConfigurations(target, host);
      }
      expect(host.dependsOn).toEqual(['cargo-fetch', 'cargo-test-archive', 'cargo-test-group-real-apfs']);

      const nodes: Record<string, ProjectGraphProjectNode> = {
        [rootProject]: {
          name: rootProject,
          type: 'app',
          data: { root: '.', targets: { ...rootTargets, 'cargo-test-group-real-apfs-host': host } },
        },
        cowshed: {
          name: 'cowshed',
          type: 'lib',
          data: { root: 'packages/cowshed', targets: macos.get('packages/cowshed') ?? {} },
        },
        lmao: {
          name: 'lmao',
          type: 'lib',
          data: { root: 'packages/lmao', targets: macos.get('packages/lmao') ?? {} },
        },
      };
      const graph: ProjectGraph = { nodes, dependencies: { [rootProject]: [], cowshed: [], lmao: [] } };
      const task = (project: string, target: string): string => `${project}:${target}`;
      // Every task `from` waits on, directly or through the tasks it waits on.
      const reached = (dependencies: Record<string, string[]>, from: string): string[] => {
        const seen = new Set<string>();
        const pending = [from];
        while (pending.length > 0) {
          const next = pending.pop();
          if (next === undefined || seen.has(next)) continue;
          seen.add(next);
          pending.push(...(dependencies[next] ?? []));
        }
        return [...seen];
      };
      const groupsReached = (dependencies: Record<string, string[]>, from: string): string[] =>
        reached(dependencies, from).filter((id) => id.includes(':cargo-test-group-'));
      const device = task(rootProject, 'cargo-test-group-real-apfs');
      const hosted = task(rootProject, 'cargo-test-group-real-apfs-host');

      // The task graph `nx run-many -t test` runs reaches neither group, from the root or from a member.
      for (const project of [rootProject, 'cowshed']) {
        const gate = createTaskGraph(graph, {}, [project], ['test'], undefined, {});
        expect(groupsReached(gate.dependencies, task(project, 'test'))).toEqual([]);
      }

      // When a run selects both groups, the host starts only once the device group is done, so two real-image
      // runners never overlap, and the device group waits on no other group.
      const named = createTaskGraph(
        graph,
        {},
        [rootProject],
        ['test', 'cargo-test-group-real-apfs', 'cargo-test-group-real-apfs-host'],
        undefined,
        {},
      );
      expect(named.dependencies[hosted]).toContain(device);
      expect(groupsReached(named.dependencies, device)).toEqual([device]);
      expect(groupsReached(named.dependencies, task(rootProject, 'test'))).toEqual([]);
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
      // — executor, options, inputs, cache — must survive the declaration
      // instead of being replaced wholesale by it.
      'cargo-test': { dependsOn: ['cargo-test-ferris-core', 'cargo-wasm'] },
      'cargo-test-archive': { dependsOn: ['cargo-wasm'] },
      'cargo-lint': { inputs: ['{projectRoot}/clippy.toml'] },
      mutation: { options: { command: 'cargo mutants --in-diff' } },
      bench: { cache: true },
    };
    try {
      await workspace.write(
        'packages/ferris/package.json',
        JSON.stringify({ name: 'ferris', nx: { targets: declared } }),
      );
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = ["crates/ferris-core"]\n');
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      // The aggregate stays a no-op that reaches bounded crate runners. A
      // partial declaration must keep that inferred base, not erase the chain.
      expect(targets['cargo-test']?.executor).toBe('nx:noop');
      expect(targets['cargo-test-ferris-core']?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(targets['cargo-test-archive']?.executor).toBe('nx:run-commands');
      expect(targets['cargo-lint']?.options).toMatchObject({
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
      });

      const cargoTest = resolveDeclaredOverInferred(targets, declared, 'cargo-test');
      expect(cargoTest?.executor).toBe('nx:noop');
      expect(cargoTest?.cache).toBe(true);
      expect(cargoTest?.dependsOn).toEqual(['cargo-test-ferris-core', 'cargo-wasm']);
      expect(cargoTest?.options).toBeUndefined();
      expect(targets['cargo-test-ferris-core']?.options?.command).toContain("-E 'package(ferris-core)'");
      expect(targets['cargo-test-ferris-core']?.options?.idleTimeoutMs).toBe(BOUNDED_TEST_IDLE_TIMEOUT_MS);

      const cargoTestArchive = resolveDeclaredOverInferred(targets, declared, 'cargo-test-archive');
      expect(cargoTestArchive?.executor).toBe('nx:run-commands');
      // A replacing array drops every inferred edge, cargo-fetch included: the
      // package now owns ordering the locked fetch ahead of its frozen archive.
      expect(cargoTestArchive?.dependsOn).toEqual(['cargo-wasm']);
      expect(targets['cargo-test-archive']?.dependsOn).toEqual(['cargo-fetch']);
      expect(String(cargoTestArchive?.options?.command)).toContain('cargo --frozen nextest archive --workspace ');

      // A declared array replaces the inferred one; the command it guards stays.
      const cargoLint = resolveDeclaredOverInferred(targets, declared, 'cargo-lint');
      expect(cargoLint?.inputs).toEqual(['{projectRoot}/clippy.toml']);
      expect(cargoLint?.options).toMatchObject({
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
      });

      // `options` merges key by key, so a declared command overrides only itself.
      const mutation = resolveDeclaredOverInferred(targets, declared, 'mutation');
      expect(mutation?.options).toEqual({
        command: 'cargo mutants --in-diff',
        cwd: 'packages/ferris',
        timeoutMs: 6 * 60 * 60 * 1000,
      });
      expect(mutation?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
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
    // at the token, so an added edge keeps the inferred cargo edges.
    const declared: Record<string, TargetConfiguration> = {
      'cargo-test': { dependsOn: ['...', 'cargo-wasm'] },
    };
    try {
      await workspace.write(
        'packages/ferris/package.json',
        JSON.stringify({ name: 'ferris', nx: { targets: declared } }),
      );
      await workspace.write('packages/ferris/Cargo.toml', '[workspace]\nmembers = ["crates/ferris-core"]\n');
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');

      // The spread expands exactly once, against the single inferred base this
      // plugin emits — re-implementing the overlay here would double it.
      const cargoTest = resolveDeclaredOverInferred(targets, declared, 'cargo-test');
      expect(cargoTest?.dependsOn).toEqual(['cargo-test-ferris-core', 'cargo-wasm']);
      expect(cargoTest?.executor).toBe('nx:noop');
    } finally {
      await workspace.cleanup();
    }
  });

  it('gives a private wrangler project a deploy that runs the stage CLI, uncached, behind its build half', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/site/package.json', '{"name":"site","private":true}\n');
      await workspace.write('packages/site/wrangler.toml', 'name = "site"\n');
      const targets = await inferProjectTargets(workspace, 'packages/site/package.json');

      // One target taking a stage, never a configuration per stage: pull-request stages are
      // `prN` for unbounded N, so `--configuration` could not name the stage that matters most.
      expect(targets.deploy?.options).toEqual({
        command: 'smoo wrangler deploy-stage --stage {args.stage}',
        cwd: 'packages/site',
      });
      // A cache hit on deploy would mean "we once uploaded this hash", which a rollback falsifies
      // silently; the step has to run and read live state instead.
      expect(targets.deploy?.cache).toBe(false);
      expect(targets.deploy?.outputs).toEqual([]);
      // No build half of any kind here, so no edge is claimed: wrangler bundles its own
      // TypeScript, so a worker with neither `build` nor `deploy-build` is legitimate.
      expect(targets.deploy?.dependsOn).toBeUndefined();
    } finally {
      await workspace.cleanup();
    }
  });

  it('infers no deploy for a PUBLISHED package whose wrangler manifest documents a binding', async () => {
    const workspace = await createWorkspace();
    try {
      // The shape that regressed a consumer: a library published to npm that ships a wrangler
      // manifest so consumers can declare the Durable Object binding it implements. Structurally
      // identical to a worker's, so only the manifest's own `private` flag tells them apart.
      await workspace.write(
        'packages/acme-cloudflare/package.json',
        '{"name":"@acme/cloudflare","nx":{"tags":["npm:private"]}}\n',
      );
      await workspace.write(
        'packages/acme-cloudflare/wrangler.toml',
        '# Wrangler configuration for @acme/cloudflare Durable Object\nname = "acme-cloudflare"\nmain = "src/index.ts"\ncompatibility_date = "2025-01-01"\n',
      );
      const targets = await inferProjectTargets(workspace, 'packages/acme-cloudflare/package.json');

      expect(Object.keys(targets)).not.toContain('deploy');
    } finally {
      await workspace.cleanup();
    }
  });

  it('points an inferred deploy at the declared deploy-build, and caches that half', async () => {
    const workspace = await createWorkspace();
    const declared = {
      'deploy-build': { executor: 'nx:run-commands', options: { command: 'bun tooling/build-site.ts' } },
    };
    try {
      await workspace.write(
        'packages/site/package.json',
        JSON.stringify({ name: 'site', private: true, nx: { targets: declared } }),
      );
      await workspace.write('packages/site/wrangler.toml', 'name = "site"\n');
      const targets = await inferProjectTargets(workspace, 'packages/site/package.json');

      expect(targets.deploy?.dependsOn).toEqual(['deploy-build']);
      // Only the project knows what building it means, so the plugin supplies the policy for
      // that half and nothing else.
      expect(resolveDeclaredOverInferred(targets, declared, 'deploy-build')?.cache).toBe(true);
      expect(resolveDeclaredOverInferred(targets, declared, 'deploy-build')?.options).toEqual({
        command: 'bun tooling/build-site.ts',
      });
    } finally {
      await workspace.cleanup();
    }
  });

  it('falls back to the aggregate build when a wrangler project emits one', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/site/package.json', '{"name":"site","private":true}\n');
      await workspace.write('packages/site/wrangler.toml', 'name = "site"\n');
      // tsconfig.lib.json is what puts an emitting target, and so the `build` aggregate, on a project.
      await workspace.write('packages/site/tsconfig.lib.json', '{}\n');
      const targets = await inferProjectTargets(workspace, 'packages/site/package.json');

      expect(targets.build).toBeDefined();
      expect(targets.deploy?.dependsOn).toEqual(['build']);
    } finally {
      await workspace.cleanup();
    }
  });

  it('lets a dependsOn-only declaration add a cross-project edge without restating the command', async () => {
    const workspace = await createWorkspace();
    // The whole point of inferring: ordering is expressed in Nx's own vocabulary, and the
    // consumer never retypes the command, cwd, cache or outputs to get it.
    const declared: Record<string, TargetConfiguration> = {
      deploy: { dependsOn: ['...', { projects: ['backend'], target: 'deploy', params: 'forward' }] },
    };
    try {
      await workspace.write(
        'packages/site/package.json',
        JSON.stringify({ name: 'site', private: true, nx: { targets: declared } }),
      );
      await workspace.write('packages/site/wrangler.toml', 'name = "site"\n');
      await workspace.write('packages/site/tsconfig.lib.json', '{}\n');
      const targets = await inferProjectTargets(workspace, 'packages/site/package.json');

      const deploy = resolveDeclaredOverInferred(targets, declared, 'deploy');
      // `...` expands to the inferred list at its position, so the inferred build edge survives
      // beside the declared one.
      expect(deploy?.dependsOn).toEqual(['build', { projects: ['backend'], target: 'deploy', params: 'forward' }]);
      expect(deploy?.options).toEqual({
        command: 'smoo wrangler deploy-stage --stage {args.stage}',
        cwd: 'packages/site',
      });
      expect(deploy?.cache).toBe(false);
      expect(deploy?.outputs).toEqual([]);
    } finally {
      await workspace.cleanup();
    }
  });

  it('infers no deploy for a project with no wrangler manifest', async () => {
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

  it('orders a three-project chain of INFERRED deploys through one run-many', async () => {
    const workspace = await createWorkspace();
    // c is deepest: a's deploy calls into b's, b's calls into c's. No project declares a deploy
    // target — each only has a wrangler manifest and, where it needs ordering, the edge. Nothing
    // here says "round one" or "round two", and no project knows the depth.
    const chain: Record<string, string[]> = { a: ['b:deploy'], b: ['c:deploy'], c: [] };
    try {
      const nodes: Record<string, ProjectGraphProjectNode> = {};
      for (const [name, edges] of Object.entries(chain)) {
        const declared: Record<string, TargetConfiguration> =
          edges.length > 0 ? { deploy: { dependsOn: ['...', ...edges] } } : {};
        await workspace.write(`packages/${name}/wrangler.toml`, `name = "${name}"\n`);
        await workspace.write(
          `packages/${name}/package.json`,
          JSON.stringify({ name, private: true, nx: { targets: declared } }),
        );
        const targets = await inferProjectTargets(workspace, `packages/${name}/package.json`);
        const deploy = resolveDeclaredOverInferred(targets, declared, 'deploy');
        // The edge rides on the inferred target: its executor and command still carry the run.
        expect(deploy?.executor).toBe('nx:run-commands');
        expect(deploy?.options?.command).toBe('smoo wrangler deploy-stage --stage {args.stage}');
        if (deploy === undefined) throw new Error(`${name} infers no deploy`);
        nodes[name] = { name, type: 'app', data: { root: `packages/${name}`, targets: { ...targets, deploy } } };
      }
      const graph: ProjectGraph = { nodes, dependencies: { a: [], b: [], c: [] } };

      // The task graph `nx run-many -t deploy --projects=a,b,c` runs: Nx starts a task only once
      // every task it depends on has finished, so this is the order.
      const tasks = createTaskGraph(graph, {}, ['a', 'b', 'c'], ['deploy'], undefined, { stage: 'staging' });
      expect(tasks.dependencies).toEqual({ 'a:deploy': ['b:deploy'], 'b:deploy': ['c:deploy'], 'c:deploy': [] });
      expect(tasks.roots).toEqual(['c:deploy']);
    } finally {
      await workspace.cleanup();
    }
  });

  it('archives a declared foreign triple beside the host archive, and runs it without building', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write(
        'packages/ferris/Cargo.toml',
        '[workspace]\nmembers = ["crates/ferris-core"]\n\n' +
          '[workspace.metadata.smoothbricks.test]\n' +
          'cross-targets = [{ target = "aarch64-apple-darwin", cargo = "scripts/cargo-for-nextest.sh" }]\n',
      );
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');
      await workspace.write('packages/ferris/scripts/cargo-for-nextest.sh', '#!/bin/sh\nexec cargo "$@"\n');

      const targets = await inferProjectTargets(workspace, 'packages/ferris/package.json');
      const archive = targets['cargo-cross-test-archive-aarch64-apple-darwin'];
      const run = targets['cargo-cross-test-aarch64-apple-darwin'];

      // A distinct archive file, declared as this target's output: the host
      // archive must survive a cross build and each must be cached as itself.
      expect(archive?.outputs).toEqual(['{projectRoot}/.cache/nextest/archive-aarch64-apple-darwin.tar.zst']);
      expect(targets['cargo-test-archive']?.outputs).toEqual([
        '{projectRoot}/.cache/nextest/archive.tar.zst',
        '{projectRoot}/.cache/nextest/archive.tar.zst.key',
      ]);
      expect(archive?.cache).toBe(true);
      // Everything the host archive hashes, plus the declared driver: the
      // script that decides how the foreign link happens is an input of the
      // bytes it produces.
      for (const input of targets['cargo-test-archive']?.inputs ?? []) {
        expect(archive?.inputs).toContainEqual(input);
      }
      // The lock-scope node the host archive names: the archive builds against the same locked crates.
      expect(archive?.inputs).toContainEqual({ externalDependencies: ['cargo-lock:packages/ferris'] });
      expect(archive?.inputs).toContain('{projectRoot}/scripts/cargo-for-nextest.sh');
      expect(archive?.options?.env).toEqual({ CARGO: 'scripts/cargo-for-nextest.sh' });
      // The runner needs no driver: it never invokes cargo at all.
      expect(run?.options?.env).toBeUndefined();
      expect(archive?.options?.commands).toBeUndefined();
      // `cargo-nextest`, not `cargo nextest`: cargo overwrites CARGO for its
      // subcommands, and CARGO is the only seam a cross cargo driver has.
      expect(String(archive?.options?.command)).toMatch(
        /^mkdir -p \.cache\/nextest && cargo-nextest nextest archive --workspace --target aarch64-apple-darwin --frozen --archive-file \.cache\/nextest\/archive-aarch64-apple-darwin\.tar\.zst --user-config-file none --tool-config-file "smoo:\$PWD\/.*nextest\.toml"$/,
      );
      expect(archive?.configurations?.[RELEASE_CONFIGURATION]).toBeUndefined();
      expect(run?.configurations?.[RELEASE_CONFIGURATION]).toBeUndefined();
      // The archive BUILDS, so it needs the locked registry graph; the runner
      // never touches cargo, so it needs nothing and depends on nothing.
      expect(archive?.dependsOn).toEqual(['cargo-fetch']);
      expect(run?.dependsOn).toEqual([]);
      expect(String(run?.options?.command)).toMatch(
        /^cargo-nextest nextest run --archive-file \.cache\/nextest\/archive-aarch64-apple-darwin\.tar\.zst --workspace-remap \. --no-tests=pass --user-config-file none --tool-config-file "smoo:\$PWD\/.*nextest\.toml"$/,
      );
      expect(run?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      // Not a crate: the per-crate coverage policy reads `cargo-test-<name>`,
      // so a triple must not land in that namespace, and neither target may
      // join the test aggregate a host machine runs.
      expect(packageNameFromCargoTestTarget('cargo-cross-test-archive-aarch64-apple-darwin')).toBeNull();
      expect(packageNameFromCargoTestTarget('cargo-cross-test-aarch64-apple-darwin')).toBeNull();
      expect(targets['cargo-test']?.dependsOn).toEqual(['cargo-test-ferris-core']);
      expect(targets.build?.dependsOn ?? []).not.toContain('cargo-cross-test-archive-aarch64-apple-darwin');
      expectNoSchedulerThrottle(targets);
    } finally {
      await workspace.cleanup();
    }
  });

  it('refuses a cross-targets declaration that cannot name a target or a file', async () => {
    const workspace = await createWorkspace();
    try {
      await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
      await workspace.write(
        'packages/ferris/Cargo.toml',
        '[workspace]\nmembers = ["crates/ferris-core"]\n\n' +
          '[workspace.metadata.smoothbricks.test]\ncross-targets = ["aarch64 apple darwin; rm -rf /"]\n',
      );
      await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');

      const failure = await inferProjectTargets(workspace, 'packages/ferris/package.json').then(
        () => undefined,
        (error: unknown) => error,
      );
      if (!(failure instanceof AggregateCreateNodesError)) {
        throw new Error(`expected AggregateCreateNodesError, got ${String(failure)}`);
      }
      expect(failure.errors[0]?.[1]?.message).toMatch(/cross-targets entries must be target triples like/);
    } finally {
      await workspace.cleanup();
    }
  });
  //#endregion
});

async function createWorkspace(parent = tmpdir()): Promise<WorkspaceFixture> {
  // Canonical, because macOS puts the temp directory behind a /private symlink
  // and a daemon never sees an edit made under a root named through one.
  const root = await realpath(await mkdtemp(join(parent, 'smoothbricks-nx-plugin-')));

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

/**
 * What inference told the operator while it ran. The plugin degrades out loud
 * — a hash it cannot make complete is a cache verdict nobody can invalidate —
 * so the stream is part of its behavior, not decoration.
 */
async function withCapturedStderr<T>(run: () => Promise<T>): Promise<{ result: T; stderr: string }> {
  const original = process.stderr.write.bind(process.stderr);
  let stderr = '';
  process.stderr.write = (chunk: string | Uint8Array) => {
    stderr += typeof chunk === 'string' ? chunk : new TextDecoder().decode(chunk);
    return true;
  };
  try {
    return { result: await run(), stderr };
  } finally {
    process.stderr.write = original;
  }
}

/**
 * A fileset Nx resolves against the workspace, or a named input nx.json
 * declares. Anything else is a path this machine happens to have.
 */
const WORKSPACE_ANCHORED_INPUT = /^!?(?:\{(?:projectRoot|workspaceRoot)\}\/|\^?[A-Za-z][\w-]*$)/;

/** A path rooted outside the workspace, wherever it appears in a hashed value. */
const PATH_OUTSIDE_WORKSPACE = /(?:^|[^\w.\-~])(\/[\w.-]+\/[\w.-]+)/;

/**
 * One Cargo workspace producing every cached cargo target family inference can
 * make: the host archive, a per-crate run, both lint verdicts, and — through
 * the `cross-targets` declaration — the foreign-triple archive, which states
 * its own inputs instead of receiving them from the `cargo --frozen` sweep.
 */
async function writeCargoIdentityFixture(workspace: WorkspaceFixture): Promise<void> {
  await workspace.write('packages/ferris/package.json', '{"name":"ferris"}\n');
  await workspace.write(
    'packages/ferris/Cargo.toml',
    '[workspace]\nmembers = ["crates/ferris-core"]\n\n' +
      '[workspace.metadata.smoothbricks.test]\n' +
      'cross-targets = [{ target = "aarch64-apple-darwin", cargo = "scripts/cargo-for-nextest.sh" }]\n',
  );
  await workspace.write('packages/ferris/crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');
  await workspace.write('packages/ferris/scripts/cargo-for-nextest.sh', '#!/bin/sh\nexec cargo "$@"\n');
}

/**
 * The targets a cache verdict can be wrong about: cached, and running cargo.
 * Derived rather than listed, so a new cargo family joins these assertions the
 * day it is inferred instead of the day someone remembers to add it.
 */
function cachedCargoTargets(targets: Record<string, TargetConfiguration>): [string, TargetConfiguration][] {
  return Object.entries(targets).filter(([, target]) => {
    const command: unknown = target.options?.command;
    const commands: unknown = target.options?.commands;
    const text = [
      typeof command === 'string' ? command : '',
      ...(Array.isArray(commands) ? commands.filter((entry) => typeof entry === 'string') : []),
    ].join('\n');
    return target.cache === true && /\bcargo/.test(text);
  });
}

async function inferProject(
  workspace: WorkspaceFixture,
  packageJsonPath: string,
  createNodes: CreateNodesV2 = createNodesV2,
) {
  const [, infer] = createNodes;
  await prepareCargoFixture(workspace.context.workspaceRoot);
  // Each inference indexes the tree as it is now, as a fresh Nx process would: the
  // in-process workspace context otherwise keeps the files of the first inference.
  resetWorkspaceContext();
  const result = await infer([packageJsonPath], undefined, workspace.context);
  return result[0]?.[1].projects?.[dirname(packageJsonPath)];
}

/**
 * Inference against a chosen nx.json. The versionless manifest inputs reach as
 * far as the workspace's own `namedInputs` allow, so the workspace
 * configuration is an input to inference and not a constant of the fixture.
 */
async function inferProjectWithNxJson(
  workspace: WorkspaceFixture,
  packageJsonPath: string,
  nxJsonConfiguration: CreateNodesContextV2['nxJsonConfiguration'],
) {
  const [, infer] = createNodesV2;
  await prepareCargoFixture(workspace.context.workspaceRoot);
  resetWorkspaceContext();
  const result = await infer([packageJsonPath], undefined, {
    workspaceRoot: workspace.context.workspaceRoot,
    nxJsonConfiguration,
  });
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
 * `--binaries-metadata` must be present: the runner executes binaries one
 * workspace `cargo nextest archive --workspace` already built, so a per-crate
 * run cannot compile anything. Narrowing comes from the nextest filterset,
 * which selects what RUNS. `--package` narrows too, but it re-resolves that
 * crate's features, which is a build — 57.9s of a 120s bounded budget on a
 * hosted 3-core macOS runner when the runners still built — so its presence is
 * a regression even though it selects the same tests. nextest rejects it
 * outright beside a reused build, and this assertion says so before cargo does.
 */
function cargoPackageSelection(command: string): {
  prebuiltRun: boolean;
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
    prebuiltRun: tokens.includes('--binaries-metadata'),
    packageFlags,
    filtered: [...command.matchAll(/\bpackage\(([^)]+)\)/g)].flatMap((match) =>
      match[1] === undefined ? [] : [match[1]],
    ),
  };
}

/**
 * Inference never throttles the scheduler: no target is exclusive and no
 * run-commands target is run one command at a time. A pair that really is
 * producer and consumer is one `&&` command; everything else is left to Nx and
 * to the locks Cargo owns.
 */
function expectNoSchedulerThrottle(targets: Record<string, TargetConfiguration>): void {
  for (const [name, target] of Object.entries(targets)) {
    expect(target.parallelism, `${name} parallelism`).toBeUndefined();
    expect(target.options?.parallel, `${name} options.parallel`).toBeUndefined();
  }
}

/** A default-profile override that puts the tests `filter` selects in `group`, on the platform serial groups run on. */
function serialOverride(filter: string, group: string): string[] {
  return [
    '[[profile.default.overrides]]',
    `filter = '${filter}'`,
    `platform = 'cfg(target_os="macos")'`,
    `test-group = '${group}'`,
    '',
  ];
}

/** The real-image split in miniature: a device group over one crate, a host group over the two crates above it. */
const APFS_SPLIT_GROUPS: readonly string[] = [
  '[test-groups.real-apfs]',
  'max-threads = 1',
  '',
  '[test-groups.real-apfs-host]',
  'max-threads = 1',
  '',
  ...serialOverride('package(cowshed-apfs-tests) and test(/real_apfs_/)', 'real-apfs'),
  ...serialOverride('package(cowshed-core) and (test(/real_apfs_/) or binary(apfs_native_macos))', 'real-apfs-host'),
  ...serialOverride('package(cowshed-cli) and test(/real_apfs_/)', 'real-apfs-host'),
];

/**
 * A repository-root Cargo workspace with two projects: `cowshed` holds the production device layer (`cowshed-apfs`,
 * which links `cowshed-os` and carries the test support the crates above it compile in), the real-image scenarios in a
 * crate of their own (`cowshed-apfs-tests`, which nothing depends on) and the host crates, and `lmao` holds a crate no
 * group can reach. `nextest` is the repository's `.config/nextest.toml`.
 */
async function writeApfsSplitFixture(workspace: WorkspaceFixture, nextest: readonly string[]): Promise<void> {
  const crate = (
    project: string,
    name: string,
    dependencies: readonly string[] = [],
    section: 'dependencies' | 'dev-dependencies' = 'dependencies',
  ): Promise<void> =>
    workspace.write(
      `packages/${project}/crates/${name}/Cargo.toml`,
      [
        '[package]',
        `name = "${name}"`,
        ...(dependencies.length === 0
          ? []
          : ['', `[${section}]`, ...dependencies.map((dependency) => `${dependency} = { path = "../${dependency}" }`)]),
        '',
      ].join('\n'),
    );
  await workspace.write('package.json', '{"name":"@fixture/codebase"}\n');
  await workspace.write('packages/cowshed/package.json', '{"name":"@fixture/cowshed","nx":{"name":"cowshed"}}\n');
  await workspace.write('packages/lmao/package.json', '{"name":"@fixture/lmao","nx":{"name":"lmao"}}\n');
  await workspace.write('Cargo.toml', '[workspace]\nmembers = ["packages/*/crates/*"]\n');
  await crate('cowshed', 'cowshed-os');
  await crate('cowshed', 'cowshed-apfs', ['cowshed-os']);
  await crate('cowshed', 'cowshed-apfs-tests', ['cowshed-apfs', 'cowshed-os'], 'dev-dependencies');
  await crate('cowshed', 'cowshed-core', ['cowshed-apfs', 'cowshed-os']);
  await crate('cowshed', 'cowshed-cli', ['cowshed-core']);
  await crate('lmao', 'lmao-core');
  await workspace.write(
    'packages/cowshed/crates/cowshed-apfs/tests/support/blank_image.rs',
    '// shared test support\n',
  );
  await workspace.write('.config/nextest.toml', nextest.join('\n'));
}

/** Inference over the split fixture's three projects in the one call Nx makes: project root to inferred targets. */
async function inferApfsSplit(
  workspace: WorkspaceFixture,
  createNodes: CreateNodesV2 = createNodesV2ForPlatform('darwin', 'arm64'),
): Promise<Map<string, Record<string, TargetConfiguration>>> {
  const [, infer] = createNodes;
  await prepareCargoFixture(workspace.context.workspaceRoot);
  resetWorkspaceContext();
  const inferred = await infer(
    ['package.json', 'packages/cowshed/package.json', 'packages/lmao/package.json'],
    undefined,
    workspace.context,
  );
  return new Map(
    inferred.flatMap(([, result]) =>
      Object.entries(result.projects ?? {}).map(([root, project]): [string, Record<string, TargetConfiguration>] => [
        root,
        project.targets ?? {},
      ]),
    ),
  );
}

/** The crates whose whole tree a target hashes: one `{workspaceRoot}/…/crates/<crate>/**\/*` fileset each. */
function hashedCrates(target: TargetConfiguration | undefined): string[] {
  const crates = (target?.inputs ?? []).flatMap((input) =>
    typeof input === 'string'
      ? (/^\{workspaceRoot\}\/packages\/[^/]+\/crates\/([^/]+)\/\*\*\/\*$/.exec(input)?.[1] ?? [])
      : [],
  );
  return [...new Set(crates)].sort((left, right) => left.localeCompare(right));
}

/** Whether Nx would hash `path` (workspace-relative) under the `{workspaceRoot}` filesets of `target`'s inputs. */
function hashes(target: TargetConfiguration | undefined, path: string): boolean {
  const filesets = (target?.inputs ?? []).flatMap((input) =>
    typeof input === 'string' && input.includes('{workspaceRoot}') ? [input] : [],
  );
  const matches = (fileset: string): boolean => new Bun.Glob(fileset.replace('{workspaceRoot}/', '')).match(path);
  return (
    filesets.some((input) => !input.startsWith('!') && matches(input)) &&
    !filesets.some((input) => input.startsWith('!') && matches(input.slice(1)))
  );
}

/** This repository's ESLint, linked into config fixtures: its config loader brings the jiti it loads TypeScript with. */
const ESLINT_DIRECTORY = dirname(createRequire(import.meta.url).resolve('eslint/package.json'));

/**
 * A project ESLint lints, and the rule-module shapes an ESLint config can
 * reach beside files it must not key: a `.js` module with a TypeScript
 * sibling that imports a missing file, a directory entered through its
 * manifest's `main`, and a workspace member a package name reaches through
 * `node_modules`. One file beside the rule modules that no config loaded
 * re-keyed 44 lint targets of a downstream workspace in one land.
 */
async function writeConfigLoadFixture(workspace: WorkspaceFixture): Promise<void> {
  const root = workspace.context.workspaceRoot;
  await workspace.write('packages/example/package.json', '{"name":"example"}\n');
  await workspace.write('packages/example/src/index.ts', 'export const value = 1;\n');
  // Decides that rule.js and lazy.ts load as CommonJS; a `type` change changes what the config gets.
  await workspace.write('tooling/checks/package.json', '{"type":"commonjs"}\n');
  await workspace.write('tooling/checks/rule.js', 'exports.rule = 1;\n');
  await workspace.write('tooling/checks/rule.ts', "import './missing-unused';\nexport const rule = 0;\n");
  await workspace.write('tooling/checks/dirpkg/package.json', '{"name":"dirpkg","main":"actual.cjs"}\n');
  await workspace.write('tooling/checks/dirpkg/actual.cjs', 'module.exports = 2;\n');
  await workspace.write('tooling/checks/rules/package.json', '{"name":"@review/rules","main":"index.cjs"}\n');
  await workspace.write('tooling/checks/rules/index.cjs', 'module.exports = 3;\n');
  await workspace.write('tooling/checks/darwin-native-archive.fixtures.ts', 'export const fixture = 1;\n');
  await mkdir(join(root, 'node_modules/@review'), { recursive: true });
  await symlink('../../tooling/checks/rules', join(root, 'node_modules/@review/rules'));
  await symlink(ESLINT_DIRECTORY, join(root, 'node_modules/eslint'));
}

/**
 * Loads `config` the way ESLint 10 does under Node (jiti for TypeScript, a
 * native `import()` otherwise) and prints every file the loader read as a
 * module: jiti's module cache, or Node's load hook.
 */
const CONFIG_LOADER_ORACLE = `import { createRequire, registerHooks } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
const config = process.argv[1];
const loaded = new Set();
if (/\\.[mc]?ts$/.test(config)) {
  const require = createRequire(createRequire(config).resolve('eslint'));
  await require('jiti').createJiti(config, { moduleCache: true, fsCache: false }).import(config);
  for (const file of Object.keys(require.cache)) loaded.add(file);
} else {
  registerHooks({
    load(url, context, nextLoad) {
      if (url.startsWith('file:')) loaded.add(fileURLToPath(url));
      return nextLoad(url, context);
    },
  });
  await import(pathToFileURL(config).href);
}
console.log(JSON.stringify([...loaded]));`;

/** The workspace files outside `node_modules`, the config excluded, that the real loader loads for `config`. */
async function loaderLoads(workspace: WorkspaceFixture, config: string): Promise<string[]> {
  const root = workspace.context.workspaceRoot;
  const output = execFileSync('node', ['--input-type=module', '--eval', CONFIG_LOADER_ORACLE, join(root, config)], {
    encoding: 'utf8',
  });
  const files: unknown = JSON.parse(output);
  if (!Array.isArray(files)) throw new Error(`config loader oracle printed ${output}`);
  const loaded = await Promise.all(
    files.map(async (file) => (typeof file === 'string' ? relative(root, await realpath(file)) : String(file))),
  );
  return loaded
    .filter((file) => file !== config && !file.startsWith('../') && !file.split('/').includes('node_modules'))
    .sort();
}

/** The `tooling/` files `lint` keys by name. */
function keyedTooling(lint: TargetConfiguration | undefined): string[] {
  return (lint?.inputs ?? [])
    .flatMap((input) =>
      typeof input === 'string' && input.startsWith('{workspaceRoot}/tooling/')
        ? [input.slice('{workspaceRoot}/'.length)]
        : [],
    )
    .sort();
}
