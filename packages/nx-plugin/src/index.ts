import { existsSync } from 'node:fs';
import { readdir, readFile } from 'node:fs/promises';
import { dirname, join, posix } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  type CreateNodesResultV2,
  type CreateNodesV2,
  readJsonFile,
  type TargetConfiguration,
} from 'nx/src/devkit-exports.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';
import { parse as parseToml } from 'smol-toml';

import { BOUNDED_TEST_KILL_AFTER_MS, BOUNDED_TEST_TIMEOUT_MS } from './bounded-test-policy.js';
import {
  CARGO_TEST_COMPILE_TARGET,
  CARGO_TEST_TARGET,
  cargoPackageTestInputs,
  cargoTestPackageTargetName,
  listCargoWorkspacePackages,
  nextestConfigRelPath,
} from './cargo-workspace.js';
import {
  CARGO_CROSS_LINT_COMMAND,
  CARGO_CROSS_LINT_TARGET,
  CARGO_FETCH_COMMAND,
  CARGO_FETCH_TARGET,
  CARGO_FROZEN_PREFIX,
  CARGO_LINT_CLIPPY_COMMAND,
  cargoFrozen,
} from './cross-check-policy.js';
import { BUILD_OUTPUT_DEPENDENCIES, PLATFORM_TARGET_GLOBS } from './workspace-config-policy.js';

export { CARGO_TEST_COMPILE_TARGET };

const BUILD_OUTPUT_TARGET_PATTERN = /-(?:js|web|html|css|android|native|napi|bun|wasm)$/;
const TYPESCRIPT_TOOLCHAIN_INPUTS = [
  '{workspaceRoot}/package.json',
  '{workspaceRoot}/bun.lock',
  '{workspaceRoot}/patches/**/*',
  '{workspaceRoot}/tsconfig.base.json',
];

// The aggregate `build` pulls in platform-suffixed binary targets for THIS
// machine only: `<tool>-<arch>-<os>` names matching the host (for example
// cli-arm64-macos on Apple Silicon) build locally as part of `nx build`, while
// every foreign platform stays publish-workflow-only. nx.json targetDefaults
// cannot express "current platform" — its dependsOn lists are static — so the
// inference plugin owns this edge.
const HOST_PLATFORM_SUFFIX: string | null = (() => {
  const os = process.platform === 'darwin' ? 'macos' : process.platform === 'linux' ? 'linux' : null;
  const arch = process.arch === 'arm64' ? 'arm64' : process.arch === 'x64' ? 'x64' : null;
  return os !== null && arch !== null ? `-${arch}-${os}` : null;
})();

export function hostPlatformTargetNames(targetNames: Iterable<string>): string[] {
  if (HOST_PLATFORM_SUFFIX === null) return [];
  return [...new Set(targetNames)]
    .filter(
      // Binary targets only. A toolchain prerequisite carries the same platform
      // suffix but produces no artifact: the cross builds that need it depend
      // on it directly, so the aggregate stays a list of outputs.
      (name) => name.endsWith(HOST_PLATFORM_SUFFIX) && !name.startsWith(NAPI_TOOLCHAIN_TARGET_PREFIX),
    )
    .sort();
}

//#region smoo!n/rust-output-target-inference
// Cargo workspace inference: a package.json sitting next to a Cargo.toml that
// declares [workspace] gets direct cargo-test/test targets, cargo-lint feeding
// the lint aggregate, mutation (cargo-mutants), and bench. Cargo output families
// use tool-native metadata: N-API comes from package.json napi metadata; Wasm
// comes from [package.metadata.smoothbricks.wasm-bindgen] in a crate manifest.
const CARGO_WORKSPACE_PATTERN = /^\s*\[workspace\]/m;
//#endregion
const CARGO_INPUTS = [
  '{projectRoot}/**/*.rs',
  '{projectRoot}/**/Cargo.toml',
  '{projectRoot}/**/Cargo.lock',
  '{projectRoot}/**/.cargo/config.toml',
  // Shell scripts a `.cargo/config.toml` points at — linker, runner, and rustc wrappers — decide
  // what the compiler produces as surely as the config naming them does. Tracking the config
  // without its shims caches an artifact against a toolchain that has since changed. Only `*.sh`
  // is swept: `scripts/` also holds project tooling that has nothing to do with a cargo build.
  '{projectRoot}/scripts/*.sh',
  '!{projectRoot}/**/target/**',
];
const CARGO_OUTPUT_INPUTS = [...CARGO_INPUTS, '{projectRoot}/package.json', '{workspaceRoot}/bun.lock'];
const NAPI_INPUTS = CARGO_OUTPUT_INPUTS;

interface NapiTargetConvention {
  architecture: string;
  outputName: string;
  targetFamily: 'linux' | 'macos';
  useNapiCross: boolean;
}

const NAPI_TARGET_CONVENTIONS: Readonly<Record<string, NapiTargetConvention>> = {
  'aarch64-apple-darwin': {
    architecture: 'arm64',
    outputName: 'darwin-arm64',
    targetFamily: 'macos',
    useNapiCross: false,
  },
  'x86_64-apple-darwin': {
    architecture: 'x64',
    outputName: 'darwin-x64',
    targetFamily: 'macos',
    useNapiCross: false,
  },
  'aarch64-unknown-linux-gnu': {
    architecture: 'arm64',
    outputName: 'linux-arm64-gnu',
    targetFamily: 'linux',
    useNapiCross: true,
  },
  'x86_64-unknown-linux-gnu': {
    architecture: 'x64',
    outputName: 'linux-x64-gnu',
    targetFamily: 'linux',
    useNapiCross: true,
  },
};

const NAPI_TOOLCHAIN_TARGET_PREFIX = 'napi-toolchain-';

/**
 * Prerequisite target that extracts the cross toolchain one triple needs. The
 * platform suffix keeps it inside its own platform family, so it can be a
 * dependency of `napi-<arch>-linux` and package-local `cli-<arch>-linux`.
 */
export function napiToolchainTargetName(convention: NapiTargetConvention): string {
  return `${NAPI_TOOLCHAIN_TARGET_PREFIX}${convention.architecture}-${convention.targetFamily}`;
}

function createCargoWasmTarget(projectRoot: string, config: ResolvedCargoWasmConfig): TargetConfiguration {
  const cargoTargetDirectory = posix.join(posix.dirname(config.manifestPath), 'target/cargo-wasm');
  const wasmInput = `${cargoTargetDirectory}/wasm32-unknown-unknown/release/${config.libraryName}.wasm`;
  return {
    executor: 'nx:run-commands',
    cache: true,
    dependsOn: ['^build'],
    inputs: CARGO_OUTPUT_INPUTS,
    outputs: [`{projectRoot}/${config.outputDirectory}`],
    options: {
      commands: [
        cargoFrozen(
          `build --release --target wasm32-unknown-unknown --target-dir ${cargoTargetDirectory} --manifest-path ${config.manifestPath}`,
        ),
        ...config.targets.map(
          ({ bindgenTarget, outputName }) =>
            `wasm-bindgen --target ${bindgenTarget} --out-dir ${config.outputDirectory}/${outputName} ${wasmInput}`,
        ),
      ],
      cwd: projectRoot,
      parallel: false,
    },
  };
}

/**
 * Compilation is excluded from the bounded test window: a cold Cargo
 * workspace can take many minutes to compile while still making progress,
 * which is not a property of the tests. `cargo test --no-run` pays that cost
 * in its own unbounded, cacheable target; the bounded runner then re-invokes
 * cargo against a warm target directory, where only the suites' own runtime
 * counts against the standard bound.
 *
 * Cargo flocks one `target/` per invocation. Nx must not run two cargo
 * writers on that directory at once — that is a mutex, not a deadlock, and
 * the second process sits in "Blocking waiting for file lock" until a
 * timeout. Inference serializes writers that share the default target dir
 * (`napi-debug` after compile, `cargo-test` after `napi-debug`). Clippy uses
 * its own `--target-dir` so lint can overlap tests.
 */
function createCargoTestCompileTarget(projectRoot: string): TargetConfiguration {
  return {
    executor: 'nx:run-commands',
    cache: true,
    inputs: CARGO_INPUTS,
    options: {
      command: cargoFrozen('test --workspace --no-run'),
      cwd: projectRoot,
    },
    configurations: {
      production: { command: cargoFrozen('test --workspace --release --no-run') },
    },
  };
}

function createCargoTestTarget(projectRoot: string): TargetConfiguration {
  return {
    executor: '@smoothbricks/nx-plugin:bounded-exec',
    cache: true,
    inputs: CARGO_INPUTS,
    dependsOn: [CARGO_TEST_COMPILE_TARGET],
    options: {
      command: cargoFrozen('test --workspace'),
      cwd: projectRoot,
      timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
      killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
    },
    configurations: {
      production: { command: cargoFrozen('test --workspace --release') },
    },
  };
}

const PLUGIN_NEXTEST_CONFIG = fileURLToPath(new URL('../nextest.toml', import.meta.url));

/**
 * `--workspace -E 'package(X)'` rather than `--package X`. A filterset selects
 * what RUNS; `--package` also re-resolves FEATURES for that crate alone, which
 * fingerprints differently from the `cargo test --workspace --no-run` that
 * `cargo-test-compile` already paid for, so cargo rebuilds the divergent half
 * inside the bounded window. Measured on a hosted 3-core macOS runner that
 * rebuild was 57.9s of a 120s budget — the tests were then killed with 264 of
 * 836 still to run while nothing was wrong with any of them. The two forms
 * select the same tests; only the filterset reuses the compile target's
 * artifacts.
 */
async function addPerPackageCargoTestTargets(
  targets: Record<string, TargetConfiguration>,
  projectRoot: string,
  workspaceRoot: string,
  absoluteProjectRoot: string,
): Promise<string[]> {
  const packages = listCargoWorkspacePackages(absoluteProjectRoot);
  if (packages.length === 0) {
    return [];
  }
  const configFile = nextestConfigRelPath(workspaceRoot, projectRoot, PLUGIN_NEXTEST_CONFIG);
  const packageTargetNames: string[] = [];
  let previous = CARGO_TEST_COMPILE_TARGET;
  for (const pkg of packages) {
    const targetName = cargoTestPackageTargetName(pkg.name);
    packageTargetNames.push(targetName);
    targets[targetName] = {
      executor: '@smoothbricks/nx-plugin:bounded-exec',
      cache: true,
      inputs: await cargoPackageTestInputs(absoluteProjectRoot, pkg.dir),
      dependsOn: [previous],
      options: {
        command: cargoFrozen(
          `nextest run --workspace -E 'package(${pkg.name})' --user-config-file none --config-file ${configFile}`,
        ),
        cwd: projectRoot,
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    };
    previous = targetName;
  }
  return packageTargetNames;
}

export const createNodesV2: CreateNodesV2 = [
  '**/package.json',
  async (projectConfigurationFiles, _options, context) => {
    const results: CreateNodesResultV2 = [];
    const errors: Array<[file: string | null, error: Error]> = [];

    await Promise.all(
      projectConfigurationFiles.map(async (packageJsonPath) => {
        try {
          if (isManagedPackageJsonSource(packageJsonPath)) {
            // smoo monorepo managed raw/templates are source copies, not projects.
            // Dogfood trees symlink live paths here; discovering both doubles names.
            results.push([packageJsonPath, {}]);
            return;
          }
          results.push([packageJsonPath, await createProjectTargets(packageJsonPath, context.workspaceRoot)]);
        } catch (error) {
          errors.push([packageJsonPath, error instanceof Error ? error : new Error(String(error))]);
        }
      }),
    );

    if (errors.length > 0) {
      throw new AggregateCreateNodesError(errors, results);
    }

    return results;
  },
];

function isManagedPackageJsonSource(packageJsonPath: string): boolean {
  const normalized = packageJsonPath.replaceAll('\\', '/');
  return normalized.includes('/managed/raw/') || normalized.includes('/managed/templates/');
}

export default { createNodesV2 };

interface PackageJson {
  name?: string;
  napi?: Record<string, unknown>;
  scripts?: Record<string, unknown>;
  nx?: {
    name?: string;
    targets?: Record<string, unknown>;
  };
}

async function createProjectTargets(packageJsonPath: string, workspaceRoot: string) {
  const projectRoot = dirname(packageJsonPath);
  const absoluteProjectRoot = join(workspaceRoot, projectRoot);
  const packageJson = await readPackageJson(join(workspaceRoot, packageJsonPath));
  const targets: Record<string, TargetConfiguration> = {};
  const validationTargets: string[] = [];
  const libTsconfigPath = join(absoluteProjectRoot, 'tsconfig.lib.json');
  const hasLibTsconfig = existsSync(libTsconfigPath);
  const cargoTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  const isCargoWorkspace =
    existsSync(cargoTomlPath) && CARGO_WORKSPACE_PATTERN.test(await readFile(cargoTomlPath, 'utf-8'));
  const napiConfig = resolveNapiConfig(packageJson, packageJsonPath, absoluteProjectRoot, isCargoWorkspace);
  const declaredTargets = packageJson.nx?.targets ?? {};
  const cargoWasmConfig = await resolveCargoWasmConfig(absoluteProjectRoot, packageJsonPath);
  const inferCargoWasm = cargoWasmConfig !== null && !('cargo-wasm' in declaredTargets);
  const packageLocalBuildOutputs = classifyPackageLocalBuildOutputs(packageJson);
  const hasOrdinaryBuildOutputTarget =
    hasLibTsconfig || napiConfig !== null || cargoWasmConfig !== null || packageLocalBuildOutputs.ordinary;
  const hasAnyBuildOutputTarget = hasOrdinaryBuildOutputTarget || packageLocalBuildOutputs.platform;

  if (hasLibTsconfig) {
    // Dependency packages may publish bundled entries that raw tsc does not produce.
    // Build every JavaScript output lane before resolving package exports, without pulling
    // unrelated Wasm, N-API, native, or web outputs onto the compiler critical path.
    //
    // ttsc transforms the source program before declaration emit, so typia's
    // generated implementation identifiers are not valid declaration inputs.
    // The executor gives ttsc a JS-only overlay and emits declarations from the
    // original project with native tsc.
    targets['tsc-js'] = {
      executor: '@smoothbricks/nx-plugin:typescript-emit',
      cache: true,
      inputs: ['production', '^production', ...TYPESCRIPT_TOOLCHAIN_INPUTS, '{projectRoot}/tsconfig.lib.json'],
      outputs: inferTypescriptOutputs(libTsconfigPath, packageJsonPath),
      dependsOn: ['^*-js', ...(cargoWasmConfig ? ['cargo-wasm'] : [])],
      options: {
        tsConfig: 'tsconfig.lib.json',
        cwd: projectRoot,
      },
    };
    targets.typecheck = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['production', '^production', ...TYPESCRIPT_TOOLCHAIN_INPUTS, '{projectRoot}/tsconfig.lib.json'],
      outputs: [],
      dependsOn: ['^*-js', ...(cargoWasmConfig ? ['cargo-wasm'] : [])],
      options: {
        command: 'tsc -p tsconfig.lib.json --noEmit',
        cwd: projectRoot,
      },
    };
  }

  const hasTestTsconfig = existsSync(join(absoluteProjectRoot, 'tsconfig.test.json'));
  if (hasTestTsconfig) {
    targets['typecheck-tests'] = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['default', '^production', ...TYPESCRIPT_TOOLCHAIN_INPUTS, '{projectRoot}/tsconfig.test.json'],
      dependsOn: ['typecheck'],
      options: {
        command: 'tsc -p tsconfig.test.json --noEmit',
        cwd: projectRoot,
      },
    };
    targets['typecheck-tests:watch'] = {
      executor: 'nx:run-commands',
      continuous: true,
      options: {
        command: 'tsc -p tsconfig.test.json --noEmit --watch',
        cwd: projectRoot,
      },
    };
    const inferredTestWatchCommand = inferTestWatchCommand(packageJson);
    if (inferredTestWatchCommand) {
      targets['test:watch'] = {
        executor: 'nx:run-commands',
        continuous: true,
        dependsOn: ['typecheck-tests'],
        options: {
          command: inferredTestWatchCommand,
          cwd: projectRoot,
        },
      };
    }
    validationTargets.push('typecheck-tests');
  } else if (hasLibTsconfig) {
    validationTargets.push('typecheck');
  }

  if (inferCargoWasm && cargoWasmConfig) {
    targets['cargo-wasm'] = createCargoWasmTarget(projectRoot, cargoWasmConfig);
  }

  if (napiConfig) {
    Object.assign(targets, createNapiTargets(projectRoot, napiConfig));
    if (
      !('napi-test' in (packageJson.nx?.targets ?? {})) &&
      existsSync(join(absoluteProjectRoot, 'src/native.test.ts'))
    ) {
      targets['napi-test'] = createNapiTestTarget(
        projectRoot,
        existsSync(join(absoluteProjectRoot, 'bunfig.napi-test.toml')),
      );
    }
  }

  // Member crates get their targets from the workspace-root package.json,
  // never per-crate — one Nx project per Cargo workspace.
  //
  // Every cargo tool target below is inferred unconditionally, INCLUDING when
  // the package already declares that key in `nx.targets`. Nx merges a
  // package-local declaration over an inference plugin's target one layer
  // later — the precedence order is specified plugins → target defaults →
  // default plugins, and the package.json reader is a default plugin — so a
  // declaration only has to name the fields it changes:
  //
  //   - `executor`, `cache`, `inputs`, `outputs`, `dependsOn`: the declared
  //     value REPLACES the inferred one, and every field the declaration omits
  //     is inherited from here.
  //   - `options` and `configurations`: merged key by key with declared keys
  //     winning, unless the declaration names a different executor — then its
  //     options mean something else and replace them wholesale.
  //   - `"dependsOn": ["...", "extra"]` expands the inferred list at the
  //     token's position. That is how a package ADDS an edge.
  //
  // Skipping inference for a declared key — what this used to do — is what made
  // a one-line addition catastrophic: `nx.targets['cargo-test'] = { dependsOn:
  // [...] }` left nothing to merge onto, and Nx normalizes a bare `dependsOn`
  // to `executor: nx:noop` with empty options. That is the worst kind of green:
  // `cargo-test` reaches its dependencies and runs no test binary.
  // `checkWorkspaceCargoTestReachabilityPolicy` fails that shape, but the fix is
  // to leave a base for the declaration to land on.
  //
  // The overlay is deliberately NOT re-implemented here. Applying it in this
  // plugin as well would expand a `"..."` spread twice — once against the
  // inferred list here, once when Nx merges the same declaration over this
  // result — duplicating the very edges the spread exists to add.
  //
  // `dependsOn` REPLACES rather than unions. These lists are not a bag of
  // independent wishes; they are the serialization chain that keeps two cargo
  // writers off one flocked `target/` (see CARGO_TEST_COMPILE_TARGET), and their
  // ORDER carries the invariant. A package that must re-route the chain — build
  // its artifact ahead of the test run, or drop `cargo-test-compile` because it
  // compiles differently — can only say so by replacing the list; a union would
  // make every inferred edge unremovable and a corrected order unrepresentable.
  // Additive intent already has a spelling (`"..."` above), so a plugin-local
  // additive key would be a second convention beside a working one.
  //
  // The output families (`cargo-wasm`, `napi-*`) and the `test` aggregate stay
  // all-or-nothing, because for those the package decides whether the target
  // EXISTS — a packaging decision for output families, the bounded-test policy's
  // rewrite for `test` — so there is no inferred base to partially override.
  if (isCargoWorkspace) {
    targets[CARGO_TEST_COMPILE_TARGET] = createCargoTestCompileTarget(projectRoot);
    const packageTargetNames = await addPerPackageCargoTestTargets(
      targets,
      projectRoot,
      workspaceRoot,
      absoluteProjectRoot,
    );
    targets[CARGO_TEST_TARGET] =
      packageTargetNames.length > 0
        ? { executor: 'nx:noop', cache: true, dependsOn: packageTargetNames }
        : createCargoTestTarget(projectRoot);
    targets['cargo-lint'] = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: CARGO_INPUTS,
      options: {
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
        cwd: projectRoot,
        parallel: false,
      },
    };
    validationTargets.push('cargo-lint');
    // The Linux arm of `cargo-lint`, as its own target. Rationale for the name,
    // the command and the absent cross test leg lives in ./cross-check-policy.ts.
    //
    // Inferred rather than declared package-locally: the rule reserving
    // package-local declaration for output families (`cargo-wasm`, `cargo-napi`)
    // governs targets that PRODUCE artifacts, where "does this package ship this
    // artifact" is a packaging decision no crate manifest can answer. This target
    // emits nothing — clippy type-checks and discards — so it belongs with
    // cargo-lint and cargo-test on the inferred validation side.
    //
    // The discriminator for "has cross-platform Rust" is deliberately the same
    // `[workspace]` Cargo.toml that already grants cargo-lint, NOT a scan of
    // sources for `cfg` arms and NOT an opt-in flag. A source scan looks more
    // precise and is strictly worse: target-specific code arrives through
    // DEPENDENCIES as well as own source, which no scan of this repo's files can
    // see, so it would silently skip projects — today's bug in a new costume. An
    // opt-in flag has the same failure mode by construction. Every Rust project is
    // in scope because CI compiles every Rust project on Linux; a project that
    // genuinely cannot cross-compile overrides the target by declaring it with
    // `executor: nx:noop`, which is a visible edit to its package.json rather
    // than a silent absence.
    //
    // Deliberately NOT pushed to validationTargets: joining the lint aggregate
    // would make `bun run lint` — and CI's own Linux lint, already native — demand
    // the 0.4 GiB cross toolchain that the devenv profile exists to keep opt-in.
    targets[CARGO_CROSS_LINT_TARGET] = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: CARGO_INPUTS,
      options: {
        command: CARGO_CROSS_LINT_COMMAND,
        cwd: projectRoot,
      },
    };
    if (!targets.test && !('test' in declaredTargets) && typeof packageJson.scripts?.test !== 'string') {
      targets.test = {
        executor: 'nx:noop',
        cache: true,
        dependsOn: [CARGO_TEST_TARGET],
      };
    }
    // Mutation runs are minutes-to-hours: never cached, never part of build/lint.
    // CI runs these per-PR via `cargo mutants --in-diff` (see mutants.toml docs).
    targets.mutation = {
      executor: 'nx:run-commands',
      cache: false,
      options: { command: cargoFrozen('mutants --workspace'), cwd: projectRoot },
    };
    targets.bench = {
      executor: 'nx:run-commands',
      cache: false,
      options: { command: cargoFrozen('bench --workspace'), cwd: projectRoot },
    };
    // Target-dir GC. Cargo never removes superseded artifacts, so a busy
    // workspace grows tens of GB of stale variants no fingerprint references;
    // sweeping by age prunes exactly that junk while the warm current
    // surface — the gated asset — survives untouched. Never cached: the
    // verdict is about this machine's disk, not the commit.
    targets['cargo-sweep'] = {
      executor: 'nx:run-commands',
      cache: false,
      options: { command: 'cargo sweep --time 7', cwd: projectRoot },
    };
  }

  // Every `cargoFrozen` command needs a registry cache holding the whole locked
  // graph, and offline cargo checks that at RESOLUTION — so a dev-dependency no
  // build ever downloaded takes down `cargo --frozen build` just as surely as
  // `cargo --frozen test`. On an ephemeral runner the only thing that had been
  // populating that cache was a preceding NON-frozen `napi build`, which
  // downloads normal dependencies and never dev-dependencies; a workspace no
  // platform target reaches got nothing at all. `cargo-fetch` states the
  // precondition instead of inheriting it from another target's side effects.
  //
  // Derived from the command text rather than a curated list of target names:
  // the fact that makes a target need this is that it runs frozen cargo, so a
  // frozen target added later is covered without anyone remembering to. The edge
  // goes on EVERY frozen target, not just the head of the serialization chain,
  // so re-routing that chain cannot strip a surviving target's precondition.
  //
  // A package that REPLACES one of these `dependsOn` lists drops the edge with
  // it (see the merge rules above) and gets its cold-cache failure back; `"..."`
  // keeps it.
  const frozenCargoManifests = new Set<string>();
  if (isCargoWorkspace) {
    frozenCargoManifests.add('Cargo.toml');
  }
  if (inferCargoWasm && cargoWasmConfig) {
    // A wasm-bindgen crate can sit in a nested Cargo workspace the project root
    // is not a member of, so its graph is a second fetch, not the same one.
    frozenCargoManifests.add(cargoWasmConfig.manifestPath);
  }
  if (frozenCargoManifests.size > 0) {
    targets[CARGO_FETCH_TARGET] = {
      executor: 'nx:run-commands',
      // The download lands in CARGO_HOME, outside the workspace: a cache hit
      // would skip work a cold registry still needs — same reason
      // napi-toolchain-* is uncached. Nothing here is a workspace output, so
      // there is nothing for Nx to restore in its place.
      cache: false,
      options: {
        commands: [...frozenCargoManifests]
          .sort()
          .map((manifestPath) =>
            manifestPath === 'Cargo.toml'
              ? CARGO_FETCH_COMMAND
              : `${CARGO_FETCH_COMMAND} --manifest-path ${manifestPath}`,
          ),
        cwd: projectRoot,
        // Two fetches into one CARGO_HOME contend on its package-cache flock;
        // serializing here spends the wait in Nx instead of inside cargo.
        parallel: false,
      },
    };
    for (const [name, target] of Object.entries(targets)) {
      if (name === CARGO_FETCH_TARGET) {
        continue;
      }
      const command: unknown = target.options?.command;
      const commands: unknown = target.options?.commands;
      const text = [
        typeof command === 'string' ? command : '',
        ...(Array.isArray(commands) ? commands.filter((entry) => typeof entry === 'string') : []),
      ].join('\n');
      if (!text.includes(CARGO_FROZEN_PREFIX)) {
        continue;
      }
      const dependsOn = target.dependsOn ?? [];
      if (!dependsOn.includes(CARGO_FETCH_TARGET)) {
        target.dependsOn = [CARGO_FETCH_TARGET, ...dependsOn];
      }
    }
  }

  // Cargo flocks the package's default `target/`. Writers that share it must
  // not be Nx siblings. Clippy is not in this set: it has its own --target-dir.
  const napiDebug = targets['napi-debug'];
  if (napiDebug && targets[CARGO_TEST_COMPILE_TARGET]) {
    const dependsOn = napiDebug.dependsOn ?? [];
    if (!dependsOn.includes(CARGO_TEST_COMPILE_TARGET)) {
      napiDebug.dependsOn = [...dependsOn, CARGO_TEST_COMPILE_TARGET];
    }
  }
  if (napiDebug) {
    for (const [name, target] of Object.entries(targets)) {
      if (!name.startsWith('cargo-test-') || name === CARGO_TEST_COMPILE_TARGET) {
        continue;
      }
      const dependsOn = target.dependsOn ?? [];
      if (dependsOn.includes(CARGO_TEST_COMPILE_TARGET) && !dependsOn.includes('napi-debug')) {
        target.dependsOn = dependsOn.map((dep) => (dep === CARGO_TEST_COMPILE_TARGET ? 'napi-debug' : dep));
      }
    }
    const cargoTest = targets[CARGO_TEST_TARGET];
    if (
      cargoTest?.options &&
      typeof cargoTest.options.command === 'string' &&
      cargoTest.options.command.startsWith('cargo ')
    ) {
      const dependsOn = cargoTest.dependsOn ?? [];
      if (!dependsOn.includes('napi-debug')) {
        cargoTest.dependsOn = [...dependsOn, 'napi-debug'];
      }
    }
  }

  if (hasOrdinaryBuildOutputTarget) {
    targets.build = {
      executor: 'nx:noop',
      cache: true,
      dependsOn: [
        '^build',
        ...BUILD_OUTPUT_DEPENDENCIES,
        ...hostPlatformTargetNames([...Object.keys(declaredTargets), ...Object.keys(targets)]),
      ],
    };
  }
  if (hasAnyBuildOutputTarget) {
    targets.clean = {
      executor: '@smoothbricks/nx-plugin:clean-outputs',
      cache: false,
    };
  }

  if (targets['typecheck-tests']) {
    if (hasLibTsconfig) {
      // Test configs may resolve the package's own `./index.js` through dist.
      // Rebuild that current-project declaration output after clean, not only
      // dependency-project outputs reached through typecheck.
      targets['typecheck-tests'].dependsOn = ['tsc-js', 'typecheck'];
    } else if (targets.build) {
      targets['typecheck-tests'].dependsOn = ['build'];
    }
  }

  if (validationTargets.length > 0) {
    targets.lint = {
      cache: true,
      dependsOn: validationTargets,
    };
  }

  const projectName = packageJson.nx?.name ?? packageJson.name;
  if (typeof projectName !== 'string' || projectName.length === 0) {
    throw new Error(`${packageJsonPath} must declare a non-empty package or nx project name`);
  }

  return {
    projects: {
      [projectRoot]: { name: projectName, targets },
    },
  };
}

interface ResolvedCargoWasmConfig {
  libraryName: string;
  manifestPath: string;
  outputDirectory: string;
  targets: Array<{ bindgenTarget: string; outputName: string }>;
}

const WASM_BINDGEN_OUTPUT_NAMES: Readonly<Record<string, string>> = {
  bundler: 'bundler',
  deno: 'deno',
  module: 'module',
  'no-modules': 'no-modules',
  nodejs: 'node',
  web: 'web',
};

async function resolveCargoWasmConfig(
  absoluteProjectRoot: string,
  packageJsonPath: string,
): Promise<ResolvedCargoWasmConfig | null> {
  const manifestPaths = ['Cargo.toml'];
  const cratesDirectory = join(absoluteProjectRoot, 'crates');
  if (existsSync(cratesDirectory)) {
    const crateEntries = await readdir(cratesDirectory, { withFileTypes: true });
    for (const entry of crateEntries.sort((left, right) => left.name.localeCompare(right.name))) {
      if (entry.isDirectory()) {
        const manifestPath = posix.join('crates', entry.name, 'Cargo.toml');
        if (existsSync(join(absoluteProjectRoot, manifestPath))) {
          manifestPaths.push(manifestPath);
        }
      }
    }
  }

  const resolved: ResolvedCargoWasmConfig[] = [];
  for (const manifestPath of manifestPaths) {
    const absoluteManifestPath = join(absoluteProjectRoot, manifestPath);
    if (!existsSync(absoluteManifestPath)) {
      continue;
    }
    const manifest: unknown = parseToml(await readFile(absoluteManifestPath, 'utf-8'));
    const cargoPackage = isRecord(manifest) && isRecord(manifest.package) ? manifest.package : null;
    const metadata = cargoPackage && isRecord(cargoPackage.metadata) ? cargoPackage.metadata : null;
    const smoothbricks = metadata && isRecord(metadata.smoothbricks) ? metadata.smoothbricks : null;
    const rawConfig = smoothbricks && isRecord(smoothbricks['wasm-bindgen']) ? smoothbricks['wasm-bindgen'] : null;
    if (!rawConfig) {
      continue;
    }

    const packageName = cargoPackage?.name;
    if (typeof packageName !== 'string' || !/^[A-Za-z0-9_-]+$/.test(packageName)) {
      throw new Error(`${packageJsonPath}: ${manifestPath} must declare a valid package.name for Wasm inference`);
    }
    const lib = isRecord(manifest) && isRecord(manifest.lib) ? manifest.lib : null;
    const crateTypes = lib?.['crate-type'];
    if (!Array.isArray(crateTypes) || !crateTypes.includes('cdylib')) {
      throw new Error(`${packageJsonPath}: ${manifestPath} Wasm inference requires lib.crate-type to include cdylib`);
    }
    const rawLibraryName = lib?.name ?? packageName.replaceAll('-', '_');
    if (typeof rawLibraryName !== 'string' || !/^[A-Za-z0-9_]+$/.test(rawLibraryName)) {
      throw new Error(`${packageJsonPath}: ${manifestPath} must declare a valid Rust library name`);
    }

    const rawTargets = rawConfig.targets;
    if (
      !Array.isArray(rawTargets) ||
      rawTargets.length === 0 ||
      !rawTargets.every((target) => typeof target === 'string')
    ) {
      throw new Error(
        `${packageJsonPath}: ${manifestPath} smoothbricks.wasm-bindgen.targets must be a non-empty string array`,
      );
    }
    const bindgenTargets = [...new Set(rawTargets as string[])];
    const targets = bindgenTargets.map((bindgenTarget) => {
      const outputName = WASM_BINDGEN_OUTPUT_NAMES[bindgenTarget];
      if (!outputName) {
        throw new Error(`${packageJsonPath}: ${manifestPath} has unsupported wasm-bindgen target ${bindgenTarget}`);
      }
      return { bindgenTarget, outputName };
    });

    const rawOutputDirectory = rawConfig['out-dir'] ?? 'dist-wasm';
    if (typeof rawOutputDirectory !== 'string' || !/^[A-Za-z0-9._/-]+$/.test(rawOutputDirectory)) {
      throw new Error(
        `${packageJsonPath}: ${manifestPath} smoothbricks.wasm-bindgen.out-dir must be a safe relative path`,
      );
    }
    const outputDirectory = posix.normalize(rawOutputDirectory);
    if (
      outputDirectory === '.' ||
      outputDirectory === '..' ||
      outputDirectory.startsWith('../') ||
      outputDirectory.startsWith('/')
    ) {
      throw new Error(
        `${packageJsonPath}: ${manifestPath} smoothbricks.wasm-bindgen.out-dir must stay inside the project`,
      );
    }

    resolved.push({ libraryName: rawLibraryName, manifestPath, outputDirectory, targets });
  }

  if (resolved.length > 1) {
    throw new Error(`${packageJsonPath}: only one Cargo crate may declare smoothbricks.wasm-bindgen metadata`);
  }
  return resolved[0] ?? null;
}

interface ResolvedNapiConfig {
  binaryName: string;
  cargoPackage: string;
  manifestPath: string;
  targets: string[];
}

function inferTypescriptOutputs(tsconfigPath: string, packageJsonPath: string): string[] {
  const tsconfig: unknown = readJsonFile(tsconfigPath);
  const compilerOptions = isRecord(tsconfig) && isRecord(tsconfig.compilerOptions) ? tsconfig.compilerOptions : null;
  const outDir = compilerOptions?.outDir;
  if (typeof outDir !== 'string' || outDir.length === 0) {
    return ['{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}', '{projectRoot}/dist/**/*.tsbuildinfo'];
  }

  const normalized = posix.normalize(outDir.replaceAll('\\', '/'));
  if (normalized === '.' || normalized === '..' || normalized.startsWith('../') || normalized.startsWith('/')) {
    throw new Error(`${packageJsonPath}: tsconfig.lib.json compilerOptions.outDir must stay inside the project`);
  }
  return [`{projectRoot}/${normalized}`];
}

function resolveNapiConfig(
  packageJson: PackageJson,
  packageJsonPath: string,
  absoluteProjectRoot: string,
  isCargoWorkspace: boolean,
): ResolvedNapiConfig | null {
  if (!packageJson.napi) {
    return null;
  }
  if (!isCargoWorkspace) {
    throw new Error(`${packageJsonPath}: napi target inference requires a Cargo workspace beside package.json`);
  }

  const binaryName = packageJson.napi.binaryName;
  if (typeof binaryName !== 'string' || !/^[A-Za-z0-9_-]+$/.test(binaryName)) {
    throw new Error(`${packageJsonPath}: napi.binaryName must contain only letters, numbers, hyphens, or underscores`);
  }
  const rawTargets = packageJson.napi.targets;
  if (
    !Array.isArray(rawTargets) ||
    rawTargets.length === 0 ||
    !rawTargets.every((target) => typeof target === 'string')
  ) {
    throw new Error(`${packageJsonPath}: napi.targets must be a non-empty array of target triples`);
  }
  const targets = [...new Set(rawTargets as string[])];
  for (const target of targets) {
    if (!NAPI_TARGET_CONVENTIONS[target]) {
      throw new Error(`${packageJsonPath}: unsupported inferred napi target ${target}`);
    }
  }

  const cargoPackage = `${binaryName}-napi`;
  const manifestPath = `crates/${cargoPackage}/Cargo.toml`;
  if (!existsSync(join(absoluteProjectRoot, manifestPath))) {
    throw new Error(`${packageJsonPath}: inferred N-API crate is missing at ${manifestPath}`);
  }
  return { binaryName, cargoPackage, manifestPath, targets };
}

function createNapiTargets(projectRoot: string, config: ResolvedNapiConfig): Record<string, TargetConfiguration> {
  const commonCommand = `--manifest-path ${config.manifestPath} --package ${config.cargoPackage}`;
  const hostPlatformTargetName = HOST_PLATFORM_SUFFIX === null ? null : `napi${HOST_PLATFORM_SUFFIX}`;
  let nativeHostTargetName: string | null = null;
  if (hostPlatformTargetName !== null) {
    for (const triple of config.targets) {
      const convention = NAPI_TARGET_CONVENTIONS[triple];
      const targetName = convention ? `napi-${convention.architecture}-${convention.targetFamily}` : null;
      if (targetName === hostPlatformTargetName && !convention?.useNapiCross) {
        nativeHostTargetName = targetName;
        break;
      }
    }
  }
  const targets: Record<string, TargetConfiguration> = {};

  // The addon the test suite loads. A dev-profile build shares target/debug
  // with cargo-test's dependency graph, so after the tests compile this is
  // nearly free — where the release platform build it replaces cost minutes
  // and sat alone on the test critical path. The output lives OUTSIDE dist/
  // because packages publish `files: ["dist"]` wholesale: a debug addon under
  // dist/ would ship. native.ts resolves this directory only when the
  // napi-test target sets NAPI_DEBUG_ADDON=1, so production loads never see it.
  targets['napi-debug'] = {
    executor: 'nx:run-commands',
    cache: true,
    dependsOn: ['^build'],
    inputs: NAPI_INPUTS,
    outputs: ['{projectRoot}/.cache/native-debug'],
    options: {
      cwd: projectRoot,
      command: `napi build --platform --no-js --dts ${config.binaryName}.napi.d.ts ${commonCommand} --output-dir .cache/native-debug`,
    },
  };

  if (nativeHostTargetName === null) {
    // The platform target for this host triple is the same compilation, so a
    // second cargo-napi invocation only buys a duplicate dependency graph in
    // target/release; native.ts already resolves the identical platform-suffixed
    // filename from dist/native/<platform-dir> after dist/native/host.
    targets['cargo-napi'] = {
      executor: 'nx:run-commands',
      cache: true,
      dependsOn: ['^build'],
      inputs: NAPI_INPUTS,
      outputs: ['{projectRoot}/dist/native/host'],
      options: {
        cwd: projectRoot,
        command: `napi build --release --platform --no-js --dts ${config.binaryName}.napi.d.ts ${commonCommand} --output-dir dist/native/host`,
      },
    };
  }

  for (const triple of config.targets) {
    const convention = NAPI_TARGET_CONVENTIONS[triple];
    if (!convention?.useNapiCross) {
      continue;
    }
    // One toolchain target per triple, shared by every cross build that needs
    // it: the inferred napi-<arch>-linux plus any package-local cli-<arch>-linux.
    // Nx runs a shared dependency once, which is what stops those builds from
    // racing inside the Bun store. See the executor for the failure it removes.
    // The name carries the platform suffix so a Linux platform target's
    // dependency closure stays Linux-only (package-target-policy).
    targets[napiToolchainTargetName(convention)] = {
      executor: '@smoothbricks/nx-plugin:napi-cross-toolchain',
      // Extraction lands in ~/.napi-rs, outside the workspace: a cache hit
      // would skip work a cold home directory still needs.
      cache: false,
      options: { triple },
    };
  }

  for (const triple of config.targets) {
    const convention = NAPI_TARGET_CONVENTIONS[triple];
    if (!convention) {
      throw new Error(`Missing N-API target convention for ${triple}`);
    }
    const targetName = `napi-${convention.architecture}-${convention.targetFamily}`;
    const outputDirectory = `dist/native/${convention.outputName}`;
    const crossFlag = convention.useNapiCross ? ' --use-napi-cross' : '';
    targets[targetName] = {
      executor: 'nx:run-commands',
      cache: true,
      ...(convention.useNapiCross ? { dependsOn: [napiToolchainTargetName(convention)] } : {}),
      inputs: NAPI_INPUTS,
      outputs: [`{projectRoot}/${outputDirectory}`],
      options: {
        cwd: projectRoot,
        command: `napi build --release --platform --no-js --dts ${config.binaryName}.${convention.outputName}.d.ts --target ${triple}${crossFlag} ${commonCommand} --output-dir ${outputDirectory}`,
        // @napi-rs recognizes Clang and supplies its downloaded GNU sysroot
        // and toolchain flags. This keeps sccache enabled while avoiding the
        // old bundled GCC, which rejects sccache's diagnostics-color flag.
        ...(convention.useNapiCross ? { env: { TARGET_CC: 'clang', TARGET_CXX: 'clang++' } } : {}),
      },
    };
  }
  return targets;
}

function createNapiTestTarget(projectRoot: string, hasDedicatedBunfig: boolean): TargetConfiguration {
  const configFlag = hasDedicatedBunfig ? ' --config=../bunfig.napi-test.toml' : ' --config=../bunfig.toml';
  return {
    executor: '@smoothbricks/nx-plugin:bounded-exec',
    cache: true,
    // Deliberately NOT `build`: the aggregate drags the host-platform release
    // binaries onto the test critical path, and the tests assert behavior, not
    // optimization level. tsc-js supplies dist/ts for the suite's own imports;
    // napi-debug supplies the addon.
    dependsOn: ['cargo-test', 'napi-debug', 'tsc-js', '^build'],
    options: {
      // cwd is src/, not the package root: `bun test <arg>` treats the arg as a
      // FILTER and scans the whole cwd tree for test files, and a Rust
      // package's cargo target/ directory turns that scan into tens of
      // seconds. bun only auto-loads bunfig from the cwd, so the package
      // config is passed explicitly.
      command: `bun test${configFlag} --timeout=30000 native.test.ts`,
      cwd: `${projectRoot}/src`,
      // Routes the package's native loader to the napi-debug artifact under
      // .cache/native-debug instead of the packaged dist/native tree.
      env: { NAPI_DEBUG_ADDON: '1' },
      timeoutMs: 120000,
      killAfterMs: 10000,
    },
  };
}

async function readPackageJson(packageJsonPath: string): Promise<PackageJson> {
  const parsed: unknown = JSON.parse(await readFile(packageJsonPath, 'utf-8'));
  if (!isRecord(parsed)) {
    throw new Error(`${packageJsonPath} must contain a JSON object`);
  }
  const rawNx = isRecord(parsed.nx) ? parsed.nx : undefined;
  return {
    ...(typeof parsed.name === 'string' ? { name: parsed.name } : {}),
    ...(isRecord(parsed.napi) ? { napi: parsed.napi } : {}),
    ...(isRecord(parsed.scripts) ? { scripts: parsed.scripts } : {}),
    ...(rawNx
      ? {
          nx: {
            ...(typeof rawNx.name === 'string' ? { name: rawNx.name } : {}),
            ...(isRecord(rawNx.targets) ? { targets: rawNx.targets } : {}),
          },
        }
      : {}),
  };
}

function inferTestWatchCommand(packageJson: PackageJson): string | null {
  const scriptCommand = packageJson.scripts?.test;
  if (typeof scriptCommand === 'string') {
    const watchCommand = watchCommandFromTestCommand(scriptCommand);
    if (watchCommand) {
      return watchCommand;
    }
  }

  const target = packageJson.nx?.targets?.test;
  if (!isRecord(target)) {
    return null;
  }

  const options = target.options;
  if (!isRecord(options) || typeof options.command !== 'string') {
    return null;
  }

  return watchCommandFromTestCommand(options.command);
}

function watchCommandFromTestCommand(command: string): string | null {
  const parsed = parseEnvPrefixedCommand(command);
  const trimmed = parsed.command.trim();

  if (/^bun\s+test(?:\s|$)/.test(trimmed)) {
    const suffix = trimmed.slice(trimmed.indexOf('test') + 'test'.length).trim();
    return `${parsed.envPrefix}bun test --watch${suffix ? ` ${suffix}` : ''}`;
  }

  if (/^vitest(?:\s+run|\s+--run)?(?:\s|$)/.test(trimmed)) {
    const suffix = trimmed.replace(/^vitest(?:\s+run|\s+--run)?/, '').trim();
    return `${parsed.envPrefix}vitest${suffix ? ` ${suffix}` : ''}`;
  }

  return null;
}

function parseEnvPrefixedCommand(command: string): { command: string; envPrefix: string } {
  const match = /^(?:\s*[A-Za-z_][A-Za-z0-9_]*=(?:"[^"]*"|'[^']*'|\S+)\s+)+/.exec(command);
  if (!match?.[0]) {
    return { command, envPrefix: '' };
  }
  return { command: command.slice(match[0].length), envPrefix: match[0] };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}

function classifyPackageLocalBuildOutputs(packageJson: PackageJson): { ordinary: boolean; platform: boolean } {
  const targets = packageJson.nx?.targets;
  const targetNames = isRecord(targets) ? Object.keys(targets) : [];
  return {
    ordinary: targetNames.some((targetName) => BUILD_OUTPUT_TARGET_PATTERN.test(targetName)),
    platform: targetNames.some((targetName) =>
      PLATFORM_TARGET_GLOBS.some((glob) => targetName.endsWith(glob.slice(1))),
    ),
  };
}
