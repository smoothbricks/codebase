import { existsSync } from 'node:fs';
import { readdir, readFile } from 'node:fs/promises';
import { dirname, join, posix, relative } from 'node:path';
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
  type AttributedCargoWorkspacePackage,
  attributeCargoWorkspacePackages,
  CARGO_TEST_COMPILE_TARGET,
  CARGO_TEST_EXCEPTIONS_SUFFIX,
  CARGO_TEST_TARGET,
  cargoPackageTestInputs,
  cargoTestPackageTargetName,
  exceptionalTestFilter,
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
import { PLATFORM_TARGET_GLOBS } from './workspace-config-policy.js';

export { CARGO_TEST_COMPILE_TARGET };

const BUILD_OUTPUT_TARGET_PATTERN = /-(?:js|web|html|css|android|native|napi|bun|wasm)$/;
const TYPESCRIPT_TOOLCHAIN_INPUTS = [
  '{workspaceRoot}/package.json',
  '{workspaceRoot}/bun.lock',
  '{workspaceRoot}/patches/**/*',
  '{workspaceRoot}/tsconfig.base.json',
];

type NapiArchitecture = 'arm64' | 'x64';
type NapiTargetFamily = 'linux' | 'macos';

interface NapiPlatform {
  architecture: NapiArchitecture;
  targetFamily: NapiTargetFamily;
}

function napiPlatform(platform: NodeJS.Platform, architecture: string): NapiPlatform | null {
  const targetFamily = platform === 'darwin' ? 'macos' : platform === 'linux' ? 'linux' : null;
  const targetArchitecture = architecture === 'arm64' ? 'arm64' : architecture === 'x64' ? 'x64' : null;
  return targetFamily !== null && targetArchitecture !== null
    ? { architecture: targetArchitecture, targetFamily }
    : null;
}

function sameNapiPlatform(left: NapiPlatform, right: NapiPlatform | null): boolean {
  return left.architecture === right?.architecture && left.targetFamily === right.targetFamily;
}

function hostPlatformTargetNames(targetNames: Iterable<string>, hostPlatform: NapiPlatform | null): string[] {
  if (hostPlatform === null) return [];
  const suffix = `-${hostPlatform.architecture}-${hostPlatform.targetFamily}`;
  return [...new Set(targetNames)]
    .filter(
      // Binary targets only. A toolchain prerequisite carries the same platform
      // suffix but produces no artifact: the cross builds that need it depend
      // on it directly, so the aggregate stays a list of outputs.
      (name) => name.endsWith(suffix) && !name.startsWith(NAPI_TOOLCHAIN_TARGET_PREFIX),
    )
    .sort();
}

/**
 * The reserved suffixes name targets that EMIT an artifact, and the `build`
 * aggregate is a list of those. Handing Nx the raw `*-<family>` globs let it
 * match on suffix alone, and one namespace collides by construction: a
 * per-crate cargo test runner is named `cargo-test-<crate>`, so any crate whose
 * name ends in an output family — `*-napi`, `*-wasm`, `*-native`, `*-js` — put
 * its RUNNER in the aggregate. The runners are one serialized chain, so a
 * single such crate made `build` pull an entire cargo test suite. Expanding the
 * families here instead of delegating to Nx's matcher keeps the plugin's own
 * `cargo-test-` namespace out of the aggregate however a crate is named.
 */
function buildOutputTargetNames(targetNames: Iterable<string>): string[] {
  return [...new Set(targetNames)]
    .filter((name) => BUILD_OUTPUT_TARGET_PATTERN.test(name) && !name.startsWith(`${CARGO_TEST_TARGET}-`))
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
const REPO_ROOT_CARGO_OUTPUT_INPUTS = [
  '{workspaceRoot}/**/*.rs',
  '{workspaceRoot}/**/Cargo.toml',
  '{workspaceRoot}/Cargo.lock',
  '{workspaceRoot}/**/.cargo/config.toml',
  '{workspaceRoot}/scripts/*.sh',
  '!{workspaceRoot}/**/target/**',
  '{projectRoot}/package.json',
  '{workspaceRoot}/bun.lock',
];
const NAPI_INPUTS = CARGO_OUTPUT_INPUTS;

interface NapiTargetConvention extends NapiPlatform {
  outputName: string;
}

const NAPI_TARGET_CONVENTIONS: Readonly<Record<string, NapiTargetConvention>> = {
  'aarch64-apple-darwin': {
    architecture: 'arm64',
    outputName: 'darwin-arm64',
    targetFamily: 'macos',
  },
  'x86_64-apple-darwin': {
    architecture: 'x64',
    outputName: 'darwin-x64',
    targetFamily: 'macos',
  },
  'aarch64-unknown-linux-gnu': {
    architecture: 'arm64',
    outputName: 'linux-arm64-gnu',
    targetFamily: 'linux',
  },
  'x86_64-unknown-linux-gnu': {
    architecture: 'x64',
    outputName: 'linux-x64-gnu',
    targetFamily: 'linux',
  },
};

const NATIVE_LINUX_COMPILER_ENV = Object.freeze({ CC: 'cc', CXX: 'c++' });
const CROSS_LINUX_COMPILER_ENV = Object.freeze({ TARGET_CC: 'clang', TARGET_CXX: 'clang++' });

function usesNapiCross(target: NapiTargetConvention, hostPlatform: NapiPlatform | null): boolean {
  return target.targetFamily === 'linux' && !sameNapiPlatform(target, hostPlatform);
}

function napiCompilerEnv(
  target: NapiPlatform | null,
  hostPlatform: NapiPlatform | null,
): Readonly<Record<string, string>> | undefined {
  if (target?.targetFamily !== 'linux') return undefined;
  return sameNapiPlatform(target, hostPlatform) ? NATIVE_LINUX_COMPILER_ENV : CROSS_LINUX_COMPILER_ENV;
}

const NAPI_TOOLCHAIN_TARGET_PREFIX = 'napi-toolchain-';

/**
 * Prerequisite target that extracts the cross toolchain one triple needs. The
 * platform suffix keeps it inside its own platform family, so it can be a
 * dependency of `napi-<arch>-linux` and package-local `cli-<arch>-linux`.
 */
export function napiToolchainTargetName(convention: NapiTargetConvention): string {
  return `${NAPI_TOOLCHAIN_TARGET_PREFIX}${convention.architecture}-${convention.targetFamily}`;
}

function createCargoWasmTarget(
  projectRoot: string,
  config: ResolvedCargoWasmConfig,
  repoRooted: boolean,
): TargetConfiguration {
  const cargoTargetDirectory = repoRooted
    ? 'target/cargo-wasm'
    : posix.join(posix.dirname(config.manifestPath), 'target/cargo-wasm');
  const wasmInput = `${cargoTargetDirectory}/wasm32-unknown-unknown/release/${config.libraryName}.wasm`;
  const outputDirectory = repoRooted ? posix.join(projectRoot, config.outputDirectory) : config.outputDirectory;
  const cargoSelection = repoRooted ? `-p ${config.cargoPackage}` : `--manifest-path ${config.manifestPath}`;
  return {
    executor: 'nx:run-commands',
    cache: true,
    dependsOn: ['^build'],
    inputs: repoRooted ? REPO_ROOT_CARGO_OUTPUT_INPUTS : CARGO_OUTPUT_INPUTS,
    outputs: [`{projectRoot}/${config.outputDirectory}`],
    options: {
      commands: [
        cargoFrozen(
          `build --release --target wasm32-unknown-unknown --target-dir ${cargoTargetDirectory} ${cargoSelection}`,
        ),
        ...config.targets.map(
          ({ bindgenTarget, outputName }) =>
            `wasm-bindgen --target ${bindgenTarget} --out-dir ${outputDirectory}/${outputName} ${wasmInput}`,
        ),
      ],
      cwd: repoRooted ? '.' : projectRoot,
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
 * One bounded target per crate; a crate that declares `smoothbricks.test.shards`
 * gets one per shard plus one for the tests nextest.toml singles out.
 *
 * `--workspace -E 'package(X)'` rather than `--package X`. A filterset selects
 * what RUNS; `--package` also re-resolves FEATURES for that crate alone, which
 * fingerprints differently from the `cargo test --workspace --no-run` that
 * `cargo-test-compile` already paid for, so cargo rebuilds the divergent half
 * inside the bounded window. Measured on a hosted 3-core macOS runner that
 * rebuild was 57.9s of a 120s budget — the tests were killed with 264 still to
 * run while nothing was wrong with them. The two forms select the same tests;
 * only the filterset reuses the compile target's artifacts.
 *
 * nextest.toml singles some tests out with an override, and each such class
 * breaks a shard in its own way — a `test-group` is scoped to one nextest RUN
 * so the hash would dissolve it, and a raised `slow-timeout` marks a test whose
 * cost is not the suite's. Those are lifted OUT of the hash into one target and
 * the shards run the exact complement, so which tests a shard holds stops
 * depending on how the hash happened to fall.
 *
 * The two filtersets are exact complements, so their union is the crate whatever
 * either one matches. Every piece uses `--no-tests=pass`: a valid workspace
 * member may expose no nextest tests, a small suite can leave a declared hash
 * shard empty, and platform-specific exceptions may not exist on this host.
 * Manifest discovery proves the package exists and generates the selector from
 * its Cargo name, so accepting an empty run cannot hide a misspelled package.
 *
 * The trade is explicit: a generated target cannot distinguish an intentionally
 * test-less crate from one whose entire suite stopped matching, so both pass.
 * Detecting that regression requires a separate coverage policy rather than
 * making valid empty crates and shards fail execution.
 *
 * Pieces chain rather than fan out, like the crates do: cargo flocks one
 * `target/`, and chaining keeps even the pinned group from overlapping the
 * shards on a machine-wide resource.
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
  const exceptional = exceptionalTestFilter(PLUGIN_NEXTEST_CONFIG);
  const packageTargetNames: string[] = [];
  let previous = CARGO_TEST_COMPILE_TARGET;
  for (const pkg of packages) {
    const inputs = await cargoPackageTestInputs({ workspaceRoot, absoluteProjectRoot, memberDir: pkg.dir });
    const sharded = pkg.testShards > 1;
    const pin = sharded && exceptional !== null ? exceptional : null;
    const addTarget = (piece: string | undefined, selector: string, extra: string) => {
      const targetName = cargoTestPackageTargetName(pkg.name, piece);
      packageTargetNames.push(targetName);
      targets[targetName] = {
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        cache: true,
        inputs,
        dependsOn: [previous],
        options: {
          command: cargoFrozen(
            `nextest run --workspace -E '${selector}'${extra} --no-tests=pass --user-config-file none --config-file ${configFile}`,
          ),
          cwd: projectRoot,
          timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
          killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
        },
      };
      previous = targetName;
    };
    const shardable = pin === null ? `package(${pkg.name})` : `package(${pkg.name}) and not (${pin})`;
    for (let index = 1; index <= pkg.testShards; index += 1) {
      // nextest hashes the test name, so a shard holds the same tests whatever
      // else the filterset selects and whatever tests are added later.
      addTarget(
        sharded ? `shard${index}` : undefined,
        shardable,
        sharded ? ` --partition hash:${index}/${pkg.testShards}` : '',
      );
    }
    if (pin !== null) {
      addTarget(CARGO_TEST_EXCEPTIONS_SUFFIX, `package(${pkg.name}) and (${pin})`, '');
    }
  }
  return packageTargetNames;
}

type CreateNodesHandler = CreateNodesV2[1];

function createNodesHandler(hostPlatform: NapiPlatform | null): CreateNodesHandler {
  return async (projectConfigurationFiles, _options, context) => {
    const results: CreateNodesResultV2 = [];
    const errors: Array<[file: string | null, error: Error]> = [];
    const repoRootCargoWorkspace = await resolveRepoRootCargoWorkspace(
      projectConfigurationFiles,
      context.workspaceRoot,
    );

    await Promise.all(
      projectConfigurationFiles.map(async (packageJsonPath) => {
        try {
          if (isManagedPackageJsonSource(packageJsonPath) || isBuildOutputPackageJson(packageJsonPath)) {
            // smoo monorepo managed raw/templates are source copies, not projects.
            // Dogfood trees symlink live paths here; discovering both doubles names.
            // A package.json a build wrote (napi's platform dirs, dist, cargo's
            // target, a package cache) is an artifact, never a project: inferring
            // one races every task that rebuilds the graph against the build that
            // is writing it, and a name collision drops the real project.
            results.push([packageJsonPath, {}]);
            return;
          }
          results.push([
            packageJsonPath,
            await createProjectTargets(packageJsonPath, context.workspaceRoot, hostPlatform, repoRootCargoWorkspace),
          ]);
        } catch (error) {
          errors.push([packageJsonPath, error instanceof Error ? error : new Error(String(error))]);
        }
      }),
    );

    if (errors.length > 0) {
      throw new AggregateCreateNodesError(errors, results);
    }

    return results;
  };
}

export function createNodesV2ForPlatform(platform: NodeJS.Platform, architecture: string): CreateNodesV2 {
  return ['**/package.json', createNodesHandler(napiPlatform(platform, architecture))];
}

export const createNodesV2: CreateNodesV2 = [
  '**/package.json',
  (...args) => createNodesHandler(napiPlatform(process.platform, process.arch))(...args),
];

function isManagedPackageJsonSource(packageJsonPath: string): boolean {
  const normalized = packageJsonPath.replaceAll('\\', '/');
  return normalized.includes('/managed/raw/') || normalized.includes('/managed/templates/');
}

/** Directories only builds write into; a package.json found under one is an output. */
const BUILD_OUTPUT_SEGMENTS = ['/node_modules/', '/dist/', '/target/', '/.cache/', '/.nx/'];

function isBuildOutputPackageJson(packageJsonPath: string): boolean {
  const normalized = `/${packageJsonPath.replaceAll('\\', '/')}`;
  return BUILD_OUTPUT_SEGMENTS.some((segment) => normalized.includes(segment));
}

export default { createNodesV2 };

interface PackageJson {
  bin?: unknown;
  name?: string;
  napi?: Record<string, unknown>;
  scripts?: Record<string, unknown>;
  nx?: {
    name?: string;
    targets?: Record<string, unknown>;
  };
}

function packageBinOutputs(packageJson: PackageJson, packageJsonPath: string): string[] {
  if (packageJson.bin === undefined) {
    return [];
  }
  if (typeof packageJson.bin === 'string') {
    if (packageJson.bin.length === 0) {
      throw new Error(`${packageJsonPath}: bin must not be empty`);
    }
    return [packageJson.bin];
  }
  if (!isRecord(packageJson.bin)) {
    throw new Error(`${packageJsonPath}: bin must be a string or an object of executable paths`);
  }
  const outputs = new Set<string>();
  for (const [name, output] of Object.entries(packageJson.bin)) {
    if (typeof output !== 'string' || output.length === 0) {
      throw new Error(`${packageJsonPath}: bin.${name} must be a non-empty executable path`);
    }
    outputs.add(output);
  }
  return [...outputs];
}

async function createProjectTargets(
  packageJsonPath: string,
  workspaceRoot: string,
  hostPlatform: NapiPlatform | null,
  repoRootCargoWorkspace: RepoRootCargoWorkspace | null,
) {
  const projectRoot = dirname(packageJsonPath);
  const absoluteProjectRoot = join(workspaceRoot, projectRoot);
  const packageJson = await readPackageJson(join(workspaceRoot, packageJsonPath));
  const projectName = packageJson.nx?.name ?? packageJson.name;
  if (typeof projectName !== 'string' || projectName.length === 0) {
    throw new Error(`${packageJsonPath} must declare a non-empty package or nx project name`);
  }
  const targets: Record<string, TargetConfiguration> = {};
  const validationTargets: string[] = [];
  const libTsconfigPath = join(absoluteProjectRoot, 'tsconfig.lib.json');
  const hasLibTsconfig = existsSync(libTsconfigPath);
  const cargoTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  const isCargoWorkspace =
    existsSync(cargoTomlPath) && CARGO_WORKSPACE_PATTERN.test(await readFile(cargoTomlPath, 'utf-8'));
  const repoRootPackagePlans =
    repoRootCargoWorkspace?.packages.filter((plan) => plan.package.projectRoot === projectRoot) ?? [];
  const isRepoRootWorkspaceRoot = repoRootCargoWorkspace !== null && projectRoot === '.';
  const isRepoRootCargoProject = isRepoRootWorkspaceRoot || repoRootPackagePlans.length > 0;
  const repoRootPackages = repoRootPackagePlans.map((plan) => plan.package);
  const isRepoRootedCargoProject = isRepoRootCargoProject && (!isCargoWorkspace || isRepoRootWorkspaceRoot);
  const napiConfig = resolveNapiConfig(
    packageJson,
    packageJsonPath,
    absoluteProjectRoot,
    isCargoWorkspace,
    repoRootPackages,
  );
  const declaredTargets = packageJson.nx?.targets ?? {};
  const cargoWasmConfig = await resolveCargoWasmConfig(
    absoluteProjectRoot,
    packageJsonPath,
    workspaceRoot,
    repoRootPackages,
  );
  const inferCargoWasm = cargoWasmConfig !== null && !('cargo-wasm' in declaredTargets);
  const packageLocalBuildOutputs = classifyPackageLocalBuildOutputs(packageJson);
  const hasOrdinaryBuildOutputTarget =
    hasLibTsconfig || napiConfig !== null || cargoWasmConfig !== null || packageLocalBuildOutputs.ordinary;
  const hasAnyBuildOutputTarget = hasOrdinaryBuildOutputTarget || packageLocalBuildOutputs.platform;

  if (hasLibTsconfig) {
    const executableOutputs = packageBinOutputs(packageJson, packageJsonPath);
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
        ...(executableOutputs.length > 0 ? { executableOutputs } : {}),
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
    targets['cargo-wasm'] = createCargoWasmTarget(projectRoot, cargoWasmConfig, isRepoRootedCargoProject);
  }

  if (napiConfig) {
    Object.assign(targets, createNapiTargets(projectRoot, napiConfig, hostPlatform, isRepoRootedCargoProject));
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
  const ownsCargoWorkspaceTargets = isCargoWorkspace || isRepoRootWorkspaceRoot;
  if (ownsCargoWorkspaceTargets) {
    const cargoWorkspaceRoot = isRepoRootWorkspaceRoot ? '.' : projectRoot;
    targets[CARGO_TEST_COMPILE_TARGET] = createCargoTestCompileTarget(cargoWorkspaceRoot);
    const packageTargetNames = isRepoRootWorkspaceRoot
      ? await addRepoRootCargoTestTargets(targets, projectName, projectRoot, workspaceRoot, repoRootCargoWorkspace)
      : await addPerPackageCargoTestTargets(targets, projectRoot, workspaceRoot, absoluteProjectRoot);
    const aggregateDependencies = isRepoRootWorkspaceRoot
      ? repoRootCargoWorkspace.packages.flatMap((plan) =>
          plan.pieces.map((piece) =>
            cargoTargetDependency(projectName, {
              projectName: plan.package.projectName,
              targetName: piece.targetName,
            }),
          ),
        )
      : packageTargetNames;
    targets[CARGO_TEST_TARGET] =
      aggregateDependencies.length > 0
        ? { executor: 'nx:noop', cache: true, dependsOn: aggregateDependencies }
        : createCargoTestTarget(cargoWorkspaceRoot);
    targets['cargo-lint'] = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: CARGO_INPUTS,
      options: {
        commands: ['cargo fmt --all --check', CARGO_LINT_CLIPPY_COMMAND],
        cwd: cargoWorkspaceRoot,
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
    // cargo-lint and cargo-test on the inferred validation side. Repository-root
    // workspaces own it once at the root because `--workspace` already checks
    // every member.
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
        cwd: cargoWorkspaceRoot,
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
      options: { command: cargoFrozen('mutants --workspace'), cwd: cargoWorkspaceRoot },
    };
    targets.bench = {
      executor: 'nx:run-commands',
      cache: false,
      options: { command: cargoFrozen('bench --workspace'), cwd: cargoWorkspaceRoot },
    };
    // Target-dir GC. Cargo never removes superseded artifacts, so a busy
    // workspace grows tens of GB of stale variants no fingerprint references;
    // sweeping by age prunes exactly that junk while the warm current
    // surface — the gated asset — survives untouched. Never cached: the
    // verdict is about this machine's disk, not the commit.
    targets['cargo-sweep'] = {
      executor: 'nx:run-commands',
      cache: false,
      options: { command: 'cargo sweep --time 7', cwd: cargoWorkspaceRoot },
    };
  }

  if (!isCargoWorkspace && !isRepoRootWorkspaceRoot && repoRootPackagePlans.length > 0 && repoRootCargoWorkspace) {
    const packageTargetNames = await addRepoRootCargoTestTargets(
      targets,
      projectName,
      projectRoot,
      workspaceRoot,
      repoRootCargoWorkspace,
    );
    targets[CARGO_TEST_TARGET] = {
      executor: 'nx:noop',
      cache: true,
      dependsOn: packageTargetNames,
    };
    if (!targets.test && !('test' in declaredTargets) && typeof packageJson.scripts?.test !== 'string') {
      targets.test = {
        executor: 'nx:noop',
        cache: true,
        dependsOn: [CARGO_TEST_TARGET],
      };
    }
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
  // Package-rooted workspaces fetch beside their package.json; members of a
  // repository-root workspace all depend on the root project's one fetch.
  //
  // A package that REPLACES one of these `dependsOn` lists drops the edge with
  // it (see the merge rules above) and gets its cold-cache failure back; `"..."`
  // keeps it.
  const frozenCargoManifests = new Set<string>();
  if (isCargoWorkspace) {
    frozenCargoManifests.add('Cargo.toml');
  }
  if (inferCargoWasm && cargoWasmConfig && !isRepoRootedCargoProject) {
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
        cwd: isRepoRootWorkspaceRoot ? '.' : projectRoot,
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
      if (text.includes(CARGO_FROZEN_PREFIX)) {
        target.dependsOn = [CARGO_FETCH_TARGET, ...(target.dependsOn ?? [])];
      }
    }
  } else if (isRepoRootedCargoProject && repoRootCargoWorkspace) {
    const rootFetch = cargoTargetDependency(projectName, {
      projectName: repoRootCargoWorkspace.rootProjectName,
      targetName: CARGO_FETCH_TARGET,
    });
    for (const target of Object.values(targets)) {
      const command: unknown = target.options?.command;
      const commands: unknown = target.options?.commands;
      const text = [
        typeof command === 'string' ? command : '',
        ...(Array.isArray(commands) ? commands.filter((entry) => typeof entry === 'string') : []),
      ].join('\n');
      if (text.includes(CARGO_FROZEN_PREFIX)) {
        target.dependsOn = [rootFetch, ...(target.dependsOn ?? [])];
      }
    }
  }

  // Cargo flocks the workspace's default target/. Keep every writer out of the
  // bounded test window by placing N-API debug builds on the same serialized
  // chain as the root compile and per-crate runners.
  const cargoTestCompileDependency: CargoTargetDependency | null = targets[CARGO_TEST_COMPILE_TARGET]
    ? CARGO_TEST_COMPILE_TARGET
    : isRepoRootCargoProject && repoRootCargoWorkspace
      ? cargoTargetDependency(projectName, {
          projectName: repoRootCargoWorkspace.rootProjectName,
          targetName: CARGO_TEST_COMPILE_TARGET,
        })
      : null;
  const napiDebug = targets['napi-debug'];
  if (napiDebug && cargoTestCompileDependency) {
    const dependsOn = napiDebug.dependsOn ?? [];
    if (!hasCargoTargetDependency(dependsOn, cargoTestCompileDependency)) {
      napiDebug.dependsOn = [...dependsOn, cargoTestCompileDependency];
    }
  }
  if (napiDebug && isRepoRootedCargoProject) {
    const firstCargoTest = repoRootPackagePlans.flatMap((plan) => plan.pieces)[0];
    const firstTarget = firstCargoTest ? targets[firstCargoTest.targetName] : undefined;
    if (firstCargoTest && firstTarget) {
      const previous = cargoTargetDependency(projectName, firstCargoTest.previous);
      const debugDependencies = napiDebug.dependsOn ?? [];
      if (!hasCargoTargetDependency(debugDependencies, previous)) {
        napiDebug.dependsOn = [...debugDependencies, previous];
      }
      firstTarget.dependsOn = (firstTarget.dependsOn ?? []).map((dependency) =>
        hasCargoTargetDependency([dependency], previous) ? 'napi-debug' : dependency,
      );
    }
  } else if (napiDebug) {
    for (const [name, target] of Object.entries(targets)) {
      if (!name.startsWith('cargo-test-') || name === CARGO_TEST_COMPILE_TARGET) {
        continue;
      }
      const dependsOn = target.dependsOn ?? [];
      if (dependsOn.includes(CARGO_TEST_COMPILE_TARGET) && !dependsOn.includes('napi-debug')) {
        target.dependsOn = dependsOn.map((dependency) =>
          dependency === CARGO_TEST_COMPILE_TARGET ? 'napi-debug' : dependency,
        );
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

  // `build` lists cargo-test-compile beside the host's platform binaries, and
  // every one of those is a `napi build` — another cargo writer on the
  // workspace's default `target/`, which cargo flocks.
  //
  // Only `cargo-napi` gets the ordering edge. A platform-suffixed target may NOT
  // have one: `validatePlatformTargetDependencies` (package-target-policy.ts:674)
  // walks each platform target's dependency closure and refuses any member of a
  // different family, because the publish flow builds and collects one platform
  // at a time — a `*-macos` collect that reached a familyless sibling would drag
  // in work the macOS runner has no business doing. `cargo-napi` carries no
  // platform suffix, so it is not scanned and the edge is legal there.
  //
  // The platform binaries therefore stay Nx siblings of cargo-test-compile and
  // serialize on cargo's flock instead of on a graph edge. That is the same
  // arrangement `cli-<arch>-<os>` and `napi-<arch>-<os>` already have with each
  // other — both `napi build --release` on one target dir — so this adds a third
  // participant to an existing wait, not a new hazard. The flock makes them wait;
  // it does not corrupt anything, and these are unbounded `nx:run-commands`
  // targets, so the wait is not charged against a test budget.
  const cargoWriterNames = hostPlatformTargetNames(
    [...Object.keys(declaredTargets), ...Object.keys(targets)],
    hostPlatform,
  );
  const cargoNapi = targets['cargo-napi'];
  if (cargoNapi && cargoTestCompileDependency) {
    const dependsOn = cargoNapi.dependsOn ?? [];
    if (!hasCargoTargetDependency(dependsOn, cargoTestCompileDependency)) {
      cargoNapi.dependsOn = [...dependsOn, cargoTestCompileDependency];
    }
  }

  // No `outputs`, deliberately: this aggregate runs no command, so every file
  // under dist belongs to the concrete target that emitted it. Claiming
  // `{projectRoot}/dist` here would cache the children's bytes a second time
  // under the aggregate's hash, and would make `smoo github-ci nx-run-many
  // --collect-outputs` attribute to `build` whatever sits under dist — including
  // platform-target artifacts the collect excludes on purpose and stale
  // artifacts of targets that never ran. Two collected trees then claim one
  // binary and `apply-outputs` rejects the overlap. `lint` and `test` are
  // aggregates on the same terms. Same reason nx.json must not put `outputs` in
  // targetDefaults.build (see validateBuildTargetDefault).
  if (hasOrdinaryBuildOutputTarget) {
    targets.build = {
      executor: 'nx:noop',
      cache: true,
      // `cargo-napi` is both an output family and a serialized cargo writer, so
      // the two sources overlap; the Set keeps one edge per target while holding
      // the order the families are listed in.
      dependsOn: [
        ...new Set([
          '^build',
          // Compiling the test executables is build work: it is unbounded and
          // cacheable, while every RUNNER is a bounded-exec target reached only
          // through the `test` aggregate. That split is what keeps a cold cargo
          // workspace's compile time out of the bounded test window.
          ...(cargoTestCompileDependency ? [cargoTestCompileDependency] : []),
          ...buildOutputTargetNames([...Object.keys(declaredTargets), ...Object.keys(targets)]),
          ...cargoWriterNames,
        ]),
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

  return {
    projects: {
      [projectRoot]: { name: projectName, targets },
    },
  };
}

interface ResolvedCargoWasmConfig {
  cargoPackage: string;
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
  workspaceRoot: string,
  repoRootPackages: readonly AttributedCargoWorkspacePackage[],
): Promise<ResolvedCargoWasmConfig | null> {
  const manifestPaths = repoRootPackages.map((pkg) => posix.join(pkg.dir, 'Cargo.toml'));
  if (manifestPaths.length === 0) {
    manifestPaths.push('Cargo.toml');
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
  }

  const resolved: ResolvedCargoWasmConfig[] = [];
  for (const manifestPath of manifestPaths) {
    const absoluteManifestPath =
      repoRootPackages.length > 0 ? join(workspaceRoot, manifestPath) : join(absoluteProjectRoot, manifestPath);
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

    resolved.push({
      cargoPackage: packageName,
      libraryName: rawLibraryName,
      manifestPath,
      outputDirectory,
      targets,
    });
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
  repoRootPackages: readonly AttributedCargoWorkspacePackage[],
): ResolvedNapiConfig | null {
  if (!packageJson.napi) {
    return null;
  }
  if (!isCargoWorkspace && repoRootPackages.length === 0) {
    throw new Error(`${packageJsonPath}: napi target inference requires a Cargo workspace containing this project`);
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
  const rootMember = repoRootPackages.find((pkg) => pkg.name === cargoPackage);
  const manifestPath = rootMember ? posix.join(rootMember.dir, 'Cargo.toml') : `crates/${cargoPackage}/Cargo.toml`;
  const absoluteManifestPath = rootMember
    ? join(absoluteProjectRoot, relative(dirname(packageJsonPath), manifestPath))
    : join(absoluteProjectRoot, manifestPath);
  if (!existsSync(absoluteManifestPath)) {
    throw new Error(`${packageJsonPath}: inferred N-API crate is missing at ${manifestPath}`);
  }
  return { binaryName, cargoPackage, manifestPath, targets };
}

function createNapiTargets(
  projectRoot: string,
  config: ResolvedNapiConfig,
  hostPlatform: NapiPlatform | null,
  repoRooted: boolean,
): Record<string, TargetConfiguration> {
  // napi derives the addon filename ([name]) from the nearest package.json `napi`
  // field starting at the invocation cwd. A repository-root invocation runs where
  // no such field exists, so without an explicit path every repo-rooted build
  // emits index.*.node while the loader resolves binaryName.*.node — a mismatch
  // that stale local artifacts can mask for months.
  const packageJsonPath = repoRooted ? posix.join(projectRoot, 'package.json') : 'package.json';
  const commonCommand = `--manifest-path ${config.manifestPath} --package ${config.cargoPackage} --package-json-path ${packageJsonPath}`;
  const cargoInputs = repoRooted ? REPO_ROOT_CARGO_OUTPUT_INPUTS : NAPI_INPUTS;
  const cargoCwd = repoRooted ? '.' : projectRoot;
  // A repository-root Cargo invocation runs outside the owning npm package, so
  // Nx's root-only PATH cannot resolve that package's napi CLI.
  const napiCommand = repoRooted ? posix.join(projectRoot, 'node_modules/.bin/napi') : 'napi';
  const outputPath = (projectOutput: string) => (repoRooted ? posix.join(projectRoot, projectOutput) : projectOutput);
  const hostPlatformTargetName =
    hostPlatform === null ? null : `napi-${hostPlatform.architecture}-${hostPlatform.targetFamily}`;
  let nativeHostTargetName: string | null = null;
  if (hostPlatformTargetName !== null) {
    for (const triple of config.targets) {
      const convention = NAPI_TARGET_CONVENTIONS[triple];
      const targetName = convention ? `napi-${convention.architecture}-${convention.targetFamily}` : null;
      if (targetName === hostPlatformTargetName && convention && !usesNapiCross(convention, hostPlatform)) {
        nativeHostTargetName = targetName;
        break;
      }
    }
  }
  const targets: Record<string, TargetConfiguration> = {};
  const hostCompilerEnv = napiCompilerEnv(hostPlatform, hostPlatform);

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
    inputs: cargoInputs,
    outputs: ['{projectRoot}/.cache/native-debug'],
    options: {
      cwd: cargoCwd,
      command: `${napiCommand} build --platform --no-js --dts ${config.binaryName}.napi.d.ts ${commonCommand} --output-dir ${outputPath('.cache/native-debug')}`,
      ...(hostCompilerEnv ? { env: hostCompilerEnv } : {}),
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
      inputs: cargoInputs,
      outputs: ['{projectRoot}/dist/native/host'],
      options: {
        cwd: cargoCwd,
        command: `${napiCommand} build --release --platform --no-js --dts ${config.binaryName}.napi.d.ts ${commonCommand} --output-dir ${outputPath('dist/native/host')}`,
        ...(hostCompilerEnv ? { env: hostCompilerEnv } : {}),
      },
    };
  }

  for (const triple of config.targets) {
    const convention = NAPI_TARGET_CONVENTIONS[triple];
    if (!convention || !usesNapiCross(convention, hostPlatform)) {
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
    const useNapiCross = usesNapiCross(convention, hostPlatform);
    const crossFlag = useNapiCross ? ' --use-napi-cross' : '';
    const compilerEnv = napiCompilerEnv(convention, hostPlatform);
    targets[targetName] = {
      executor: 'nx:run-commands',
      cache: true,
      ...(useNapiCross ? { dependsOn: [napiToolchainTargetName(convention)] } : {}),
      inputs: cargoInputs,
      outputs: [`{projectRoot}/${outputDirectory}`],
      options: {
        cwd: cargoCwd,
        command: `${napiCommand} build --release --platform --no-js --dts ${config.binaryName}.${convention.outputName}.d.ts --target ${triple}${crossFlag} ${commonCommand} --output-dir ${outputPath(outputDirectory)}`,
        // Genuine Linux cross-compiles use Clang plus napi-rs's downloaded
        // GNU sysroot. A native Linux target uses Nix's cc wrapper so C build
        // scripts resolve the host libc headers instead.
        ...(compilerEnv ? { env: compilerEnv } : {}),
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
    ...('bin' in parsed ? { bin: parsed.bin } : {}),
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

interface RepoRootCargoTargetRef {
  projectName: string;
  targetName: string;
}

interface RepoRootCargoTestPiece {
  extra: string;
  previous: RepoRootCargoTargetRef;
  selector: string;
  targetName: string;
}

interface RepoRootCargoPackagePlan {
  package: AttributedCargoWorkspacePackage;
  pieces: RepoRootCargoTestPiece[];
}

interface RepoRootCargoWorkspace {
  packages: RepoRootCargoPackagePlan[];
  rootProjectName: string;
}

type CargoTargetDependency = NonNullable<TargetConfiguration['dependsOn']>[number];

function cargoTargetDependency(currentProjectName: string, target: RepoRootCargoTargetRef): CargoTargetDependency {
  return target.projectName === currentProjectName
    ? target.targetName
    : { projects: [target.projectName], target: target.targetName };
}

async function resolveRepoRootCargoWorkspace(
  packageJsonPaths: readonly string[],
  workspaceRoot: string,
): Promise<RepoRootCargoWorkspace | null> {
  const cargoTomlPath = join(workspaceRoot, 'Cargo.toml');
  if (!existsSync(cargoTomlPath) || !CARGO_WORKSPACE_PATTERN.test(await readFile(cargoTomlPath, 'utf-8'))) {
    return null;
  }

  const projects: Array<{ name: string; root: string }> = [];
  for (const packageJsonPath of packageJsonPaths) {
    if (isManagedPackageJsonSource(packageJsonPath) || isBuildOutputPackageJson(packageJsonPath)) {
      continue;
    }
    try {
      const packageJson = await readPackageJson(join(workspaceRoot, packageJsonPath));
      const name = packageJson.nx?.name ?? packageJson.name;
      if (typeof name === 'string' && name.length > 0) {
        projects.push({ name, root: dirname(packageJsonPath) });
      }
    } catch {
      // createProjectTargets records the path-specific parse failure below.
    }
  }
  const rootProject = projects.find((project) => project.root === '.');
  if (!rootProject) {
    return null;
  }

  const attributed = attributeCargoWorkspacePackages(listCargoWorkspacePackages(workspaceRoot), projects);
  const packages: RepoRootCargoPackagePlan[] = [];
  let previous: RepoRootCargoTargetRef = {
    projectName: rootProject.name,
    targetName: CARGO_TEST_COMPILE_TARGET,
  };
  const exceptional = exceptionalTestFilter(PLUGIN_NEXTEST_CONFIG);
  for (const pkg of attributed) {
    const pieces: RepoRootCargoTestPiece[] = [];
    const sharded = pkg.testShards > 1;
    const pin = sharded && exceptional !== null ? exceptional : null;
    const shardable = pin === null ? `package(${pkg.name})` : `package(${pkg.name}) and not (${pin})`;
    for (let index = 1; index <= pkg.testShards; index += 1) {
      const targetName = cargoTestPackageTargetName(pkg.name, sharded ? `shard${index}` : undefined);
      pieces.push({
        extra: sharded ? ` --partition hash:${index}/${pkg.testShards}` : '',
        previous,
        selector: shardable,
        targetName,
      });
      previous = { projectName: pkg.projectName, targetName };
    }
    if (pin !== null) {
      const targetName = cargoTestPackageTargetName(pkg.name, CARGO_TEST_EXCEPTIONS_SUFFIX);
      pieces.push({
        extra: '',
        previous,
        selector: `package(${pkg.name}) and (${pin})`,
        targetName,
      });
      previous = { projectName: pkg.projectName, targetName };
    }
    packages.push({ package: pkg, pieces });
  }
  return { packages, rootProjectName: rootProject.name };
}

/**
 * Root mode keeps `--workspace` and adds `-p` only to make the owning crate
 * explicit in the command Nx attributes to its project. Cargo treats
 * `--workspace -p X` as the full workspace selection, so `-p` is inert for
 * feature resolution; the nextest filterset is what narrows the tests that run.
 * Never drop `--workspace`: `-p X` alone re-resolves features for one package
 * and defeats the compile target's shared artifacts.
 */
async function addRepoRootCargoTestTargets(
  targets: Record<string, TargetConfiguration>,
  currentProjectName: string,
  projectRoot: string,
  workspaceRoot: string,
  workspace: RepoRootCargoWorkspace,
): Promise<string[]> {
  const targetNames: string[] = [];
  const configFile = nextestConfigRelPath(workspaceRoot, '.', PLUGIN_NEXTEST_CONFIG);
  for (const plan of workspace.packages) {
    if (plan.package.projectRoot !== projectRoot) {
      continue;
    }
    const inputs = await cargoPackageTestInputs({
      workspaceRoot,
      absoluteProjectRoot: workspaceRoot,
      memberDir: plan.package.dir,
      inputRoot: '{workspaceRoot}',
    });
    for (const piece of plan.pieces) {
      targetNames.push(piece.targetName);
      targets[piece.targetName] = {
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        cache: true,
        inputs,
        dependsOn: [cargoTargetDependency(currentProjectName, piece.previous)],
        options: {
          command: cargoFrozen(
            `nextest run --workspace -p ${plan.package.name} -E '${piece.selector}'${piece.extra} --no-tests=pass --user-config-file none --config-file ${configFile}`,
          ),
          cwd: '.',
          timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
          killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
        },
      };
    }
  }
  return targetNames;
}

function hasCargoTargetDependency(
  dependencies: readonly CargoTargetDependency[],
  expected: CargoTargetDependency,
): boolean {
  if (typeof expected === 'string') {
    return dependencies.includes(expected);
  }
  return dependencies.some(
    (dependency) =>
      typeof dependency !== 'string' &&
      dependency.target === expected.target &&
      Array.isArray(dependency.projects) &&
      Array.isArray(expected.projects) &&
      dependency.projects.length === expected.projects.length &&
      dependency.projects.every((project, index) => project === expected.projects?.[index]),
  );
}
