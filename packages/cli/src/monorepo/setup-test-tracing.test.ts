import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type SetupTestTracingShell, setupTestTracing } from './index.js';

describe('LMAO test tracing setup', () => {
  it('runs the Bun test tracing generator for every workspace package', async () => {
    await withWorkspace(async (root) => {
      await writeWorkspacePackage(root, 'packages/a', '@scope/a', 'a');
      await writeWorkspacePackage(root, 'packages/b', '@scope/b', 'b');
      const shell = new RecordingSetupShell();

      await setupTestTracing(root, { all: true }, shell);

      expect(shell.runs).toEqual([generatorRun(root, 'a', '@scope/a'), generatorRun(root, 'b', '@scope/b')]);
    });
  });

  it('selects packages by project name, package name, or package root', async () => {
    await withWorkspace(async (root) => {
      await writeWorkspacePackage(root, 'packages/a', '@scope/a', 'a');
      await writeWorkspacePackage(root, 'packages/b', '@scope/b', 'b');
      await writeWorkspacePackage(root, 'packages/c', '@scope/c', 'c');
      const shell = new RecordingSetupShell();

      await setupTestTracing(root, { projects: 'a,@scope/b,packages/c', opContextExport: 'customContext' }, shell);

      expect(shell.runs).toEqual([
        generatorRun(root, 'a', '@scope/a', 'customContext'),
        generatorRun(root, 'b', '@scope/b', 'customContext'),
        generatorRun(root, 'c', '@scope/c', 'customContext'),
      ]);
    });
  });

  it('prints generator invocations in dry-run mode without running Nx', async () => {
    await withWorkspace(async (root) => {
      await writeWorkspacePackage(root, 'packages/a', '@scope/a', 'a');
      const shell = new RecordingSetupShell();

      await setupTestTracing(root, { all: true, dryRun: true }, shell);

      expect(shell.runs).toEqual([]);
      expect(shell.logs).toEqual([
        'would run      nx g @smoothbricks/nx-plugin:bun-test-tracing --project a --opContextModule @scope/a --opContextExport opContext --tracerModule @smoothbricks/lmao/testing/bun',
      ]);
    });
  });
});

class RecordingSetupShell implements SetupTestTracingShell {
  readonly runs: { command: string; args: string[]; cwd: string }[] = [];
  readonly logs: string[] = [];

  async run(command: string, args: string[], cwd: string): Promise<void> {
    this.runs.push({ command, args, cwd });
  }

  log(message: string): void {
    this.logs.push(message);
  }
}

async function withWorkspace(run: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-test-tracing-'));
  try {
    await writeJson(join(root, 'package.json'), {
      name: '@scope/workspace',
      version: '0.0.0',
      private: true,
      workspaces: ['packages/*'],
    });
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function writeWorkspacePackage(
  root: string,
  packagePath: string,
  name: string,
  projectName: string,
): Promise<void> {
  await mkdir(join(root, packagePath), { recursive: true });
  await writeJson(join(root, packagePath, 'package.json'), {
    name,
    version: '1.0.0',
    nx: { name: projectName },
  });
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function generatorRun(
  root: string,
  project: string,
  opContextModule: string,
  opContextExport = 'opContext',
): { command: string; args: string[]; cwd: string } {
  return {
    command: 'nx',
    args: [
      'g',
      '@smoothbricks/nx-plugin:bun-test-tracing',
      '--project',
      project,
      '--opContextModule',
      opContextModule,
      '--opContextExport',
      opContextExport,
      '--tracerModule',
      '@smoothbricks/lmao/testing/bun',
    ],
    cwd: root,
  };
}
