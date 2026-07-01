import { afterEach, beforeEach, describe, expect, it, spyOn } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type PrResolveShell, resolvePrConflicts } from './index.js';

let gitDir: string;

beforeEach(() => {
  gitDir = mkdtempSync(join(tmpdir(), 'smoo-pr-'));
  spyOn(console, 'log').mockImplementation(() => {});
  spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  rmSync(gitDir, { recursive: true, force: true });
});

const PR_JSON = JSON.stringify({
  number: 40,
  url: 'https://github.com/conloca/private/pull/40',
  headRefName: 'gar-sync/private-to-public',
  baseRefName: 'public-mirror',
  isCrossRepository: false,
});
const MARKERED = '{\n<<<<<<< HEAD\n  "a": 1\n=======\n  "a": 2\n>>>>>>> feat\n}\n';
const CLEAN = '{\n  "a": 1\n}\n';

const statePath = () => join(gitDir, 'smoo', 'pr-resolve.json');

describe('smoo pr resolve — phase A (start)', () => {
  it('reports clean and writes no state when the PR has no markers', async () => {
    const shell = scriptedShell([
      ok(gitDir), // rev-parse --absolute-git-dir
      ok(''), // status --porcelain (clean tree)
      ok(PR_JSON), // gh pr view
      ok('a.json\n'), // diff --name-only base...head
      ok(CLEAN), // show head:a.json
    ]);
    const exit = await resolvePrConflicts('/repo', '40', { remote: 'origin' }, shell.shell);
    expect(exit).toBe(0);
    expect(existsSync(statePath())).toBe(false);
    expect(shell.runCommands).not.toContainEqual(expect.stringContaining('checkout'));
  });

  it('checks out the branch, saves state, and returns 2 when markers exist', async () => {
    const shell = scriptedShell([
      ok(gitDir),
      ok(''),
      ok(PR_JSON),
      ok('a.json\n'),
      ok(MARKERED),
      ok('main\n'), // symbolic-ref (current branch)
      ok('deadbeefcafe\n'), // rev-parse HEAD
    ]);
    const exit = await resolvePrConflicts('/repo', '40', { remote: 'origin' }, shell.shell);
    expect(exit).toBe(2);
    const state = JSON.parse(readFileSync(statePath(), 'utf8'));
    expect(state.pr).toBe(40);
    expect(state.headBranch).toBe('gar-sync/private-to-public');
    expect(state.originalBranch).toBe('main');
    expect(state.remote).toBe('origin');
    expect(shell.runCommands).toContainEqual(
      'git checkout -B gar-sync/private-to-public origin/gar-sync/private-to-public',
    );
  });

  it('refuses to start on a dirty working tree', async () => {
    const shell = scriptedShell([ok(gitDir), ok(' M src/x.ts\n')]);
    const exit = await resolvePrConflicts('/repo', '40', { remote: 'origin' }, shell.shell);
    expect(exit).toBe(1);
    expect(existsSync(statePath())).toBe(false);
  });
});

describe('smoo pr resolve — phase B (finish)', () => {
  it('pushes and restores the original branch when markers are resolved', async () => {
    writeState({ originalBranch: 'main' });
    const shell = scriptedShell([
      ok(gitDir),
      ok('gar-sync/private-to-public\n'), // symbolic-ref (on the PR branch)
      ok(''), // status --porcelain (committed, clean)
      ok('a.json\n'), // diff --name-only base...HEAD
      ok(CLEAN), // show HEAD:a.json (resolved)
      ok(''), // git push (fast-forward)
    ]);
    const exit = await resolvePrConflicts('/repo', undefined, {}, shell.shell);
    expect(exit).toBe(0);
    expect(existsSync(statePath())).toBe(false);
    expect(shell.runResultCommands).toContainEqual('git push origin HEAD:refs/heads/gar-sync/private-to-public');
    expect(shell.runCommands).toContainEqual('git checkout main');
  });

  it('returns 2 and keeps state when markers still remain', async () => {
    writeState({ originalBranch: 'main' });
    const shell = scriptedShell([
      ok(gitDir),
      ok('gar-sync/private-to-public\n'),
      ok(''),
      ok('a.json\n'),
      ok(MARKERED), // still markered
    ]);
    const exit = await resolvePrConflicts('/repo', undefined, {}, shell.shell);
    expect(exit).toBe(2);
    expect(existsSync(statePath())).toBe(true);
    expect(shell.runResultCommands).not.toContainEqual(expect.stringContaining('push'));
  });

  it('returns 2 when the resolution is not committed', async () => {
    writeState({ originalBranch: 'main' });
    const shell = scriptedShell([ok(gitDir), ok('gar-sync/private-to-public\n'), ok(' M a.json\n')]);
    const exit = await resolvePrConflicts('/repo', undefined, {}, shell.shell);
    expect(exit).toBe(2);
    expect(existsSync(statePath())).toBe(true);
  });

  it('errors when HEAD is not on the PR branch', async () => {
    writeState({ originalBranch: 'main' });
    const shell = scriptedShell([ok(gitDir), ok('some-other-branch\n')]);
    const exit = await resolvePrConflicts('/repo', undefined, {}, shell.shell);
    expect(exit).toBe(1);
    expect(existsSync(statePath())).toBe(true);
  });

  it('errors when pointed at a different PR mid-resolution', async () => {
    writeState({ originalBranch: 'main' });
    const shell = scriptedShell([ok(gitDir)]);
    const exit = await resolvePrConflicts('/repo', '99', {}, shell.shell);
    expect(exit).toBe(1);
  });
});

describe('smoo pr resolve --abort', () => {
  it('restores the original branch and clears state', async () => {
    writeState({ originalBranch: 'main' });
    const shell = scriptedShell([ok(gitDir)]);
    const exit = await resolvePrConflicts('/repo', undefined, { abort: true }, shell.shell);
    expect(exit).toBe(0);
    expect(existsSync(statePath())).toBe(false);
    expect(shell.runCommands).toContainEqual('git checkout main');
  });
});

function writeState(overrides: Record<string, unknown>): void {
  mkdirSync(join(gitDir, 'smoo'), { recursive: true });
  const state = {
    pr: 40,
    url: 'https://github.com/conloca/private/pull/40',
    headBranch: 'gar-sync/private-to-public',
    baseBranch: 'public-mirror',
    remote: 'origin',
    crossRepo: false,
    originalBranch: 'main',
    originalSha: 'deadbeefcafe',
    startedAt: '2026-07-01T00:00:00Z',
    ...overrides,
  };
  writeFileSync(statePath(), `${JSON.stringify(state, null, 2)}\n`);
}

function ok(stdout: string): { exitCode: number; stdout: string; stderr: string } {
  return { exitCode: 0, stdout, stderr: '' };
}

function scriptedShell(responses: { exitCode: number; stdout: string; stderr: string }[]): {
  shell: PrResolveShell;
  runCommands: string[];
  runResultCommands: string[];
} {
  const runCommands: string[] = [];
  const runResultCommands: string[] = [];
  let index = 0;
  const shell: PrResolveShell = {
    async runResult(command, args, _cwd) {
      runResultCommands.push(`${command} ${args.join(' ')}`);
      const response = responses[index++];
      if (!response) {
        throw new Error(`unexpected runResult call: ${command} ${args.join(' ')}`);
      }
      return response;
    },
    async run(command, args, _cwd) {
      runCommands.push(`${command} ${args.join(' ')}`);
    },
  };
  return { shell, runCommands, runResultCommands };
}
