import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, join, posix } from 'node:path';

import {
  type CreateNodesResultV2,
  type CreateNodesV2,
  readJsonFile,
  type TargetConfiguration,
} from 'nx/src/devkit-exports.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';

import { BUILD_OUTPUT_DEPENDENCIES, PLATFORM_TARGET_GLOBS } from './workspace-config-policy.js';

const BUILD_OUTPUT_TARGET_PATTERN = /-(?:js|web|html|css|android|native|napi|bun|wasm)$/;
const TYPESCRIPT_TOOLCHAIN_INPUTS = [
  '{workspaceRoot}/package.json',
  '{workspaceRoot}/bun.lock',
  '{workspaceRoot}/patches/**/*',
  '{workspaceRoot}/tsconfig.base.json',
];

//#region smoo!n/rust-output-target-inference
// Cargo workspace inference: a package.json sitting next to a Cargo.toml that
// declares [workspace] gets direct cargo-test/test targets, cargo-lint feeding
// the lint aggregate, mutation (cargo-mutants), and bench. Rust output targets
// are not guessed from crate metadata: N-API targets require package.json napi
// metadata plus the conventional crates/<binaryName>-napi workspace member.
// Other output families remain explicit package targets.
const CARGO_WORKSPACE_PATTERN = /^\s*\[workspace\]/m;
//#endregion
const CARGO_INPUTS = [
  '{projectRoot}/**/*.rs',
  '{projectRoot}/**/Cargo.toml',
  '{projectRoot}/Cargo.lock',
  '{projectRoot}/.cargo/config.toml',
  '!{projectRoot}/target/**',
];
const NAPI_INPUTS = [...CARGO_INPUTS, '{projectRoot}/package.json', '{workspaceRoot}/bun.lock'];

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

function createCargoTestTarget(projectRoot: string): TargetConfiguration {
  return {
    executor: '@smoothbricks/nx-plugin:bounded-exec',
    cache: true,
    inputs: CARGO_INPUTS,
    options: {
      command: 'cargo test --workspace',
      cwd: projectRoot,
      timeoutMs: 600000,
      killAfterMs: 10000,
    },
    configurations: {
      production: { command: 'cargo test --workspace --release' },
    },
  };
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
  const packageLocalBuildOutputs = classifyPackageLocalBuildOutputs(packageJson);
  const hasOrdinaryBuildOutputTarget = hasLibTsconfig || napiConfig !== null || packageLocalBuildOutputs.ordinary;
  const hasAnyBuildOutputTarget = hasOrdinaryBuildOutputTarget || packageLocalBuildOutputs.platform;

  if (hasLibTsconfig) {
    // ttsc transforms the source program before declaration emit, so typia's
    // generated implementation identifiers are not valid declaration inputs.
    // The executor gives ttsc a JS-only overlay and emits declarations from the
    // original project with native tsc.
    targets['tsc-js'] = {
      executor: '@smoothbricks/nx-plugin:typescript-emit',
      cache: true,
      inputs: ['production', '^production', ...TYPESCRIPT_TOOLCHAIN_INPUTS, '{projectRoot}/tsconfig.lib.json'],
      outputs: inferTypescriptOutputs(libTsconfigPath, packageJsonPath),
      dependsOn: ['^tsc-js'],
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
      dependsOn: ['^tsc-js'],
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

  if (napiConfig) {
    Object.assign(targets, createNapiTargets(projectRoot, napiConfig));
    if (
      !('napi-test' in (packageJson.nx?.targets ?? {})) &&
      existsSync(join(absoluteProjectRoot, 'src/native.test.ts'))
    ) {
      targets['napi-test'] = createNapiTestTarget(projectRoot);
    }
  }

  // Member crates get their targets from the workspace-root package.json,
  // never per-crate — one Nx project per Cargo workspace.
  if (isCargoWorkspace) {
    const declared = packageJson.nx?.targets ?? {};
    if (!('cargo-test' in declared)) {
      targets['cargo-test'] = createCargoTestTarget(projectRoot);
    }
    if (!('cargo-lint' in declared)) {
      targets['cargo-lint'] = {
        executor: 'nx:run-commands',
        cache: true,
        inputs: CARGO_INPUTS,
        options: {
          commands: ['cargo fmt --all --check', 'cargo clippy --workspace --all-targets -- -D warnings'],
          cwd: projectRoot,
          parallel: false,
        },
      };
    }
    validationTargets.push('cargo-lint');
    if (!targets.test && !('test' in declared) && typeof packageJson.scripts?.test !== 'string') {
      // Execute Cargo directly: workspace targetDefaults may replace test.dependsOn.
      targets.test = createCargoTestTarget(projectRoot);
    }
    if (!('mutation' in declared)) {
      // Mutation runs are minutes-to-hours: never cached, never part of build/lint.
      // CI runs these per-PR via `cargo mutants --in-diff` (see mutants.toml docs).
      targets.mutation = {
        executor: 'nx:run-commands',
        cache: false,
        options: { command: 'cargo mutants --workspace', cwd: projectRoot },
      };
    }
    if (!('bench' in declared)) {
      targets.bench = {
        executor: 'nx:run-commands',
        cache: false,
        options: { command: 'cargo bench --workspace', cwd: projectRoot },
      };
    }
  }

  if (hasOrdinaryBuildOutputTarget) {
    targets.build = {
      executor: 'nx:noop',
      cache: true,
      dependsOn: ['^build', ...BUILD_OUTPUT_DEPENDENCIES],
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
  const targets: Record<string, TargetConfiguration> = {
    'cargo-napi': {
      executor: 'nx:run-commands',
      cache: true,
      dependsOn: ['^build'],
      inputs: NAPI_INPUTS,
      outputs: ['{projectRoot}/dist/native/host'],
      options: {
        cwd: projectRoot,
        command: `napi build --release --platform --no-js --dts ${config.binaryName}.napi.d.ts ${commonCommand} --output-dir dist/native/host`,
      },
    },
  };

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
      inputs: NAPI_INPUTS,
      outputs: [`{projectRoot}/${outputDirectory}`],
      options: {
        cwd: projectRoot,
        command: `napi build --release --platform --no-js --dts ${config.binaryName}.${convention.outputName}.d.ts --target ${triple}${crossFlag} ${commonCommand} --output-dir ${outputDirectory}`,
      },
    };
  }
  return targets;
}

function createNapiTestTarget(projectRoot: string): TargetConfiguration {
  return {
    executor: '@smoothbricks/nx-plugin:bounded-exec',
    cache: true,
    dependsOn: ['cargo-test', 'cargo-napi', 'tsc-js', '^build', 'build'],
    options: {
      command: 'bun test --timeout=30000 src/native.test.ts',
      cwd: projectRoot,
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
