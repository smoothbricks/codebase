import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import type { CreateNodesResultV2, CreateNodesV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';

const RESERVED_ZIG_STEPS = new Set(['all', 'clean', 'install', 'test']);
const ZIG_STEP_PATTERN = /\bb\.step\(\s*["']([^"']+)["']\s*,/g;
const VALID_ZIG_STEP_NAME = /^[A-Za-z0-9_-]+$/;
const BUILD_OUTPUT_DEPENDENCIES = [
  '*-js',
  '*-web',
  '*-html',
  '*-css',
  '*-ios',
  '*-android',
  '*-native',
  '*-napi',
  '*-bun',
  '*-wasm',
];
const BUILD_OUTPUT_TARGET_PATTERN = /-(?:js|web|html|css|ios|android|native|napi|bun|wasm)$/;

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
