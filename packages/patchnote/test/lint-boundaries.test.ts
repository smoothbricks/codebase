import { afterEach, describe, expect, test } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { GitHubCLIClient } from '../src/auth/github-client.js';
import { defaultConfig, loadConfig, mergeConfig } from '../src/config.js';
import { executeCommand } from '../src/executor.js';
import type { CommandExecutor } from '../src/types.js';
import { createMockExeca } from './helpers/mock-execa.js';

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
describe('validated command and configuration boundaries', () => {
  test('the real text executor satisfies the injectable contract without assertions', async () => {
    const execute: CommandExecutor = executeCommand;
    const result = await execute(process.execPath, [
      '-e',
      'process.stdout.write("text"); process.stderr.write("diagnostic")',
    ]);
    expect(result.stdout).toBe('text');
    expect(result.stderr).toBe('diagnostic');
    expect(result.exitCode).toBe(0);
  });
  test('rejects malformed members, not only a non-array PR response', async () => {
    for (const value of [
      [null],
      [{}],
      [{ number: '123', title: 'bad', headRefName: 'branch', createdAt: '2026-01-01', url: 'url' }],
    ]) {
      const client = new GitHubCLIClient(
        createMockExeca({
          'gh pr list --json number,title,headRefName,baseRefName,createdAt,url --state open': JSON.stringify(value),
        }),
      );
      await expect(client.listUpdatePRs('/repo')).rejects.toThrow('Expected array');
    }
  });
  test('refuses undeclared mergeability values', async () => {
    const client = new GitHubCLIClient(
      createMockExeca({
        'gh pr view 123 --json mergeable': JSON.stringify({ mergeable: 'invented' }),
      }),
    );
    await expect(client.checkPRConflicts('/repo', 123)).rejects.toThrow('mergeable');
  });
  test('enhancing an operational rejection cannot crash on null or a primitive', async () => {
    for (const failure of [null, undefined, 'offline']) {
      const executor: CommandExecutor = async () => {
        throw failure;
      };
      await expect(new GitHubCLIClient(executor).listUpdatePRs('/repo')).rejects.toThrow('Failed to list PRs');
    }
  });
  test('invalid nested config falls back instead of leaking an invalid runtime type', async () => {
    const root = await mkdtemp(join(tmpdir(), 'patchnote-config-boundary-'));
    roots.push(root);
    await mkdir(join(root, 'tooling'));
    await writeFile(join(root, 'tooling/patchnote.json'), JSON.stringify({ prStrategy: { maxStackDepth: 'many' } }));
    expect((await loadConfig(root, 'tooling/patchnote.json')).prStrategy.maxStackDepth).toBe(
      defaultConfig.prStrategy.maxStackDepth,
    );
  });
  test('configuration merges replace complete list elements and retain required defaults', () => {
    const config = mergeConfig({
      expo: { projects: [{ packageJsonPath: 'apps/mobile/package.json' }] },
      packageRules: [{ match: 'react' }],
    });
    expect(config.expo?.projects).toEqual([{ packageJsonPath: 'apps/mobile/package.json' }]);
    expect(config.expo?.enabled).toBe(defaultConfig.expo?.enabled);
    expect(config.packageRules).toEqual([{ match: 'react' }]);
  });
});

describe('text executor output modes', () => {
  test('represents uncaptured streams as empty text without changing execution', async () => {
    const result = await executeCommand(process.execPath, ['-e', 'process.stdout.write("ignored")'], {
      stdio: 'ignore',
    });
    expect(result).toEqual({ stdout: '', stderr: '', exitCode: 0 });
  });
  test('retains a nonzero exit code when the caller requests non-throwing execution', async () => {
    const result = await executeCommand(process.execPath, ['-e', 'process.exit(7)'], { reject: false });
    expect(result.exitCode).toBe(7);
  });
  test('rejects known non-text modes before even resolving the executable', async () => {
    for (const options of [{ encoding: 'buffer' }, { lines: true }] as const) {
      await expect(executeCommand('patchnote-test-executable-that-does-not-exist', [], options)).rejects.toThrow(
        'requires text output',
      );
    }
  });
  test('an unsupported provider cannot admit the rest of an invalid configuration', async () => {
    const root = await mkdtemp(join(tmpdir(), 'patchnote-provider-boundary-'));
    roots.push(root);
    await mkdir(join(root, 'tooling'));
    await writeFile(
      join(root, 'tooling/patchnote.json'),
      JSON.stringify({ ai: { provider: 'unsupported' }, prStrategy: { maxStackDepth: 99 } }),
    );
    const config = await loadConfig(root, 'tooling/patchnote.json');
    expect(config.ai.provider).toBe(defaultConfig.ai.provider);
    expect(config.prStrategy.maxStackDepth).toBe(defaultConfig.prStrategy.maxStackDepth);
  });
});
