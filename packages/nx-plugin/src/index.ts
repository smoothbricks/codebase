import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import type { CreateNodesResultV2, CreateNodesV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';

import { BUILD_OUTPUT_DEPENDENCIES } from './workspace-config-policy.js';

const RESERVED_ZIG_STEPS = new Set(['all', 'clean', 'install', 'test']);
const ZIG_STEP_PATTERN = /\bb\.step\(\s*["']([^"']+)["']\s*,/g;
const VALID_ZIG_STEP_NAME = /^[A-Za-z0-9_-]+$/;
const BUILD_OUTPUT_TARGET_PATTERN = /-(?:js|web|html|css|ios|android|native|napi|bun|wasm)$/;

// Cargo workspace inference: a package.json sitting next to a Cargo.toml that
// declares [workspace] gets direct cargo-test/test targets, cargo-lint feeding
// the lint aggregate, mutation (cargo-mutants), bench, and — per cdylib member
// crate — a cargo-wasm build producing dist/<crate>.wasm. Explicit nx.targets
// entries in the package.json always win over inference.
const CARGO_WORKSPACE_PATTERN = /^\s*\[workspace\]/m;
const CARGO_MEMBERS_PATTERN = /^\s*members\s*=\s*\[([^\]]*)\]/m;
const CARGO_MEMBER_ENTRY_PATTERN = /["']([^"']+)["']/g;
const CARGO_PACKAGE_NAME_PATTERN = /^\s*name\s*=\s*["']([^"']+)["']/m;
const CARGO_CDYLIB_PATTERN = /^\s*crate-type\s*=\s*\[[^\]]*["']cdylib["']/m;
const CARGO_WASM_RELEASE_PROFILE_PATTERN = /^\s*\[profile\.wasm-release\]/m;
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
  scripts?: Record<string, unknown>;
  nx?: {
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
  let hasBuildOutputTarget = hasLibTsconfig || hasPackageLocalBuildOutputTarget(packageJson);

  const hasTestTsconfig = existsSync(join(absoluteProjectRoot, 'tsconfig.test.json'));
  if (hasTestTsconfig) {
    targets['typecheck-tests'] = {
      executor: 'nx:run-commands',
      cache: true,
      options: {
        command: 'tsc --noEmit -p tsconfig.test.json',
        cwd: projectRoot,
      },
    };
    targets['typecheck-tests:watch'] = {
      executor: 'nx:run-commands',
      continuous: true,
      options: {
        command: 'tsc --noEmit -p tsconfig.test.json --watch',
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

  const cargo = await readCargoWorkspace(absoluteProjectRoot);
  if (cargo) {
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
    if (cargo.cdylibCrates.length > 0 && !('cargo-wasm' in declared)) {
      const profile = cargo.hasWasmReleaseProfile ? 'wasm-release' : 'release';
      const buildAndCopy = (crate: string, profileName: string | null) => {
        const artifact = crate.replace(/-/g, '_');
        const profileFlag = profileName === null ? '' : ` --profile ${profileName}`;
        const outputDirectory = profileName ?? 'debug';
        return `cargo build${profileFlag} --target wasm32-unknown-unknown -p ${crate} && mkdir -p dist && cp target/wasm32-unknown-unknown/${outputDirectory}/${artifact}.wasm dist/${artifact}.wasm`;
      };
      targets['cargo-wasm'] = {
        executor: 'nx:run-commands',
        cache: true,
        inputs: CARGO_INPUTS,
        outputs: ['{projectRoot}/dist/**/*.wasm'],
        options: {
          commands: cargo.cdylibCrates.map((crate) => buildAndCopy(crate, profile)),
          cwd: projectRoot,
          parallel: false,
        },
        configurations: {
          development: {
            commands: cargo.cdylibCrates.map((crate) => buildAndCopy(crate, null)),
          },
        },
      };
      hasBuildOutputTarget = true;
    }
  }

  const zigSteps = await readZigSteps(absoluteProjectRoot, projectRoot);
  for (const step of zigSteps) {
    const targetName = `zig-${step}`;
    targets[targetName] = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['{projectRoot}/src/**/*.zig', '{projectRoot}/build.zig', '{projectRoot}/build.zig.zon'],
      outputs: [
        '{projectRoot}/dist/**/*.wasm',
        '{projectRoot}/dist/**/*.node',
        '{projectRoot}/dist/**/*.dylib',
        '{projectRoot}/dist/**/*.so',
        '{projectRoot}/dist/**/*.dll',
        '{projectRoot}/dist/**/*.a',
      ],
      options: {
        command: `zig build ${step}`,
        cwd: projectRoot,
      },
    };
    hasBuildOutputTarget = true;
  }

  if (hasBuildOutputTarget) {
    targets.build = {
      executor: 'nx:noop',
      cache: true,
      dependsOn: ['^build', ...BUILD_OUTPUT_DEPENDENCIES],
    };
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
      executor: 'nx:noop',
      cache: true,
      dependsOn: validationTargets,
    };
  }

  return {
    projects: {
      [projectRoot]: { targets },
    },
  };
}

async function readPackageJson(packageJsonPath: string): Promise<PackageJson> {
  return JSON.parse(await readFile(packageJsonPath, 'utf-8')) as PackageJson;
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

function hasPackageLocalBuildOutputTarget(packageJson: PackageJson): boolean {
  const targets = packageJson.nx?.targets;
  if (!isRecord(targets)) {
    return false;
  }

  return Object.keys(targets).some((targetName) => BUILD_OUTPUT_TARGET_PATTERN.test(targetName));
}

interface CargoWorkspace {
  cdylibCrates: string[];
  hasWasmReleaseProfile: boolean;
}

async function readCargoWorkspace(absoluteProjectRoot: string): Promise<CargoWorkspace | null> {
  const cargoTomlPath = join(absoluteProjectRoot, 'Cargo.toml');
  if (!existsSync(cargoTomlPath)) {
    return null;
  }

  const cargoToml = await readFile(cargoTomlPath, 'utf-8');
  if (!CARGO_WORKSPACE_PATTERN.test(cargoToml)) {
    // Member crates get their targets from the workspace-root package.json,
    // never per-crate — one Nx project per Cargo workspace.
    return null;
  }

  const cdylibCrates: string[] = [];
  const membersList = CARGO_MEMBERS_PATTERN.exec(cargoToml)?.[1] ?? '';
  for (const match of membersList.matchAll(CARGO_MEMBER_ENTRY_PATTERN)) {
    const memberTomlPath = join(absoluteProjectRoot, match[1], 'Cargo.toml');
    if (!existsSync(memberTomlPath)) {
      continue;
    }
    const memberToml = await readFile(memberTomlPath, 'utf-8');
    if (CARGO_CDYLIB_PATTERN.test(memberToml)) {
      const name = CARGO_PACKAGE_NAME_PATTERN.exec(memberToml)?.[1];
      if (name && !cdylibCrates.includes(name)) {
        cdylibCrates.push(name);
      }
    }
  }

  return { cdylibCrates, hasWasmReleaseProfile: CARGO_WASM_RELEASE_PROFILE_PATTERN.test(cargoToml) };
}

async function readZigSteps(absoluteProjectRoot: string, projectRoot: string): Promise<string[]> {
  const buildZigPath = join(absoluteProjectRoot, 'build.zig');

  if (!existsSync(buildZigPath)) {
    return [];
  }

  const buildZig = await readFile(buildZigPath, 'utf-8');
  const declaredSteps = Array.from(buildZig.matchAll(ZIG_STEP_PATTERN), (match) => match[1]);

  if (declaredSteps.length === 0) {
    return [];
  }

  const steps: string[] = [];
  for (const step of declaredSteps) {
    if (RESERVED_ZIG_STEPS.has(step)) {
      continue;
    }

    if (!VALID_ZIG_STEP_NAME.test(step)) {
      throw new Error(
        `${projectRoot}/build.zig declares unsupported Zig step "${step}"; inferred target names cannot contain ':' or other special characters`,
      );
    }

    if (!steps.includes(step)) {
      steps.push(step);
    }
  }

  return steps;
}
