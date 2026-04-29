import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import type { CreateNodesResultV2, CreateNodesV2, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';

const RESERVED_ZIG_STEPS = new Set(['all', 'clean', 'install', 'test']);
const ZIG_STEP_PATTERN = /\bb\.step\(\s*["']([^"']+)["']\s*,/g;
const VALID_ZIG_STEP_NAME = /^[A-Za-z0-9_-]+$/;

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

async function createProjectTargets(packageJsonPath: string, workspaceRoot: string) {
  const projectRoot = dirname(packageJsonPath);
  const absoluteProjectRoot = join(workspaceRoot, projectRoot);
  const targets: Record<string, TargetConfiguration> = {};
  const buildComponents: string[] = [];
  const validationTargets: string[] = [];

  if (existsSync(join(absoluteProjectRoot, 'tsconfig.lib.json'))) {
    buildComponents.push('tsc-js');
    validationTargets.push('typecheck');
  }

  if (existsSync(join(absoluteProjectRoot, 'tsconfig.test.json'))) {
    targets['typecheck-tests'] = {
      executor: 'nx:run-commands',
      cache: true,
      options: {
        command: 'tsc --noEmit -p tsconfig.test.json',
        cwd: projectRoot,
      },
    };
    validationTargets.push('typecheck-tests');
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
    buildComponents.push(targetName);
  }

  if (buildComponents.length > 0) {
    targets.build = {
      executor: 'nx:noop',
      cache: true,
      dependsOn: ['^build', ...buildComponents],
    };
  }

  if (targets['typecheck-tests'] && targets.build) {
    targets['typecheck-tests'].dependsOn = ['build'];
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
