import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import type { CreateNodesResultV2, CreateNodesV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
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
// (cargo-wasm, cargo-napi, ...) are never inferred from crate metadata such as
// cdylib crate-types — a native N-API cdylib is not a wasm build. Packages
// declare output targets in package.json nx.targets, and those *-wasm/*-napi/
// *-native output-family names feed the aggregate build and clean targets.
// Explicit nx.targets entries always win over inference.
const CARGO_WORKSPACE_PATTERN = /^\s*\[workspace\]/m;
//#endregion
const CARGO_INPUTS = [
  '{projectRoot}/**/*.rs',
  '{projectRoot}/**/Cargo.toml',
  '{projectRoot}/Cargo.lock',
  '{projectRoot}/.cargo/config.toml',
  '!{projectRoot}/target/**',
];

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

export default { createNodesV2 };

interface PackageJson {
  name?: string;
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
  const hasLibTsconfig = existsSync(join(absoluteProjectRoot, 'tsconfig.lib.json'));
  const packageLocalBuildOutputs = classifyPackageLocalBuildOutputs(packageJson);
  const hasOrdinaryBuildOutputTarget = hasLibTsconfig || packageLocalBuildOutputs.ordinary;
  const hasAnyBuildOutputTarget = hasOrdinaryBuildOutputTarget || packageLocalBuildOutputs.platform;

  if (hasLibTsconfig) {
    // Official Nx target inference is disabled because its compiler surface only
    // supports tsc/tsgo. Smoo owns the complete transformer-aware ttsc targets.
    targets['tsc-js'] = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['production', '^production', ...TYPESCRIPT_TOOLCHAIN_INPUTS, '{projectRoot}/tsconfig.lib.json'],
      outputs: ['{projectRoot}/dist/**/*.{js,cjs,mjs,jsx,d.ts,d.cts,d.mts}{,.map}'],
      dependsOn: ['^tsc-js'],
      options: {
        command: 'ttsc -p tsconfig.lib.json --emit',
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
        command: 'ttsc -p tsconfig.lib.json --noEmit',
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
        command: 'ttsc -p tsconfig.test.json --noEmit',
        cwd: projectRoot,
      },
    };
    targets['typecheck-tests:watch'] = {
      executor: 'nx:run-commands',
      continuous: true,
      options: {
        command: 'ttsc -p tsconfig.test.json --noEmit --watch',
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

  // Member crates get their targets from the workspace-root package.json,
  // never per-crate — one Nx project per Cargo workspace.
  const cargoTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  if (existsSync(cargoTomlPath) && CARGO_WORKSPACE_PATTERN.test(await readFile(cargoTomlPath, 'utf-8'))) {
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
    if (!('test' in declared) && typeof packageJson.scripts?.test !== 'string') {
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
      targets['typecheck-tests'].dependsOn = ['typecheck'];
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

async function readPackageJson(packageJsonPath: string): Promise<PackageJson> {
  const parsed: unknown = JSON.parse(await readFile(packageJsonPath, 'utf-8'));
  if (!isRecord(parsed)) {
    throw new Error(`${packageJsonPath} must contain a JSON object`);
  }
  const rawNx = isRecord(parsed.nx) ? parsed.nx : undefined;
  return {
    ...(typeof parsed.name === 'string' ? { name: parsed.name } : {}),
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
