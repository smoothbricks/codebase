import { describe, expect, mock, test } from 'bun:test';
import { type LockFileMaintenanceDeps, lockFileMaintenance } from '../../src/commands/lock-file-maintenance.js';
import type { PatchnoteConfig } from '../../src/config.js';
import type { UpdateOptions } from '../../src/types.js';

// Mock @clack/prompts to suppress UI output in tests
mock.module('@clack/prompts', () => ({
  intro: () => {},
  outro: () => {},
  note: () => {},
  log: { info: () => {}, warn: () => {}, error: () => {}, step: () => {} },
  spinner: () => ({ start: () => {}, stop: () => {} }),
}));

function makeDeps(overrides: Partial<LockFileMaintenanceDeps> = {}): LockFileMaintenanceDeps {
  return {
    getRepoRoot: mock(() => Promise.resolve('/repo')),
    createBranch: mock(() => Promise.resolve()),
    stageFiles: mock(() => Promise.resolve()),
    commit: mock(() => Promise.resolve()),
    pushWithUpstream: mock(() => Promise.resolve()),
    deleteRemoteBranch: mock(() => Promise.resolve()),
    createPR: mock(() => Promise.resolve({ number: 42, url: 'https://github.com/test/repo/pull/42' })),
    resolveSemanticPrefix: mock(() => Promise.resolve('chore(deps)')),
    refreshLockFile: mock(() => Promise.resolve({ changed: true })),
    ...overrides,
  } as LockFileMaintenanceDeps;
}

function makeConfig(overrides: Partial<PatchnoteConfig> = {}): PatchnoteConfig {
  return {
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 5,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      strategy: 'squash',
      requireTests: true,
    },
    ai: { provider: 'zai' },
    git: { remote: 'origin', baseBranch: 'main' },
    lockFileMaintenance: {
      enabled: true,
      branchPrefix: 'chore/lock-file-maintenance',
    },
    ...overrides,
  } as PatchnoteConfig;
}

function makeOptions(overrides: Partial<UpdateOptions> = {}): UpdateOptions {
  return {
    dryRun: false,
    skipGit: false,
    skipAI: false,
    ...overrides,
  };
}

describe('lockFileMaintenance command', () => {
  test('exits early with message when no lock file changes detected', async () => {
    const deps = makeDeps({
      refreshLockFile: mock(() => Promise.resolve({ changed: false })),
    });

    await lockFileMaintenance(makeConfig(), makeOptions(), deps);

    expect(deps.createBranch).not.toHaveBeenCalled();
    expect(deps.stageFiles).not.toHaveBeenCalled();
    expect(deps.commit).not.toHaveBeenCalled();
    expect(deps.pushWithUpstream).not.toHaveBeenCalled();
    expect(deps.createPR).not.toHaveBeenCalled();
  });

  test('in dry-run mode reports status without creating PR', async () => {
    const deps = makeDeps({
      refreshLockFile: mock(() => Promise.resolve({ changed: false })),
    });

    await lockFileMaintenance(makeConfig(), makeOptions({ dryRun: true }), deps);

    expect(deps.refreshLockFile).toHaveBeenCalled();
    const callArgs = (deps.refreshLockFile as ReturnType<typeof mock>).mock.calls[0]!;
    expect(callArgs[1]).toMatchObject({ dryRun: true });

    expect(deps.createBranch).not.toHaveBeenCalled();
    expect(deps.createPR).not.toHaveBeenCalled();
  });

  test('stages only bun.lock and bun.lockb, not all changes', async () => {
    const deps = makeDeps();

    await lockFileMaintenance(makeConfig(), makeOptions(), deps);

    expect(deps.stageFiles).toHaveBeenCalledTimes(1);
    const stageArgs = (deps.stageFiles as ReturnType<typeof mock>).mock.calls[0]!;
    expect(stageArgs[0]).toBe('/repo');
    expect(stageArgs[1]).toEqual(['bun.lock', 'bun.lockb']);
  });

  test('creates branch with correct prefix format', async () => {
    const deps = makeDeps();

    await lockFileMaintenance(makeConfig(), makeOptions(), deps);

    expect(deps.createBranch).toHaveBeenCalledTimes(1);
    const branchName = (deps.createBranch as ReturnType<typeof mock>).mock.calls[0]![1] as string;
    expect(branchName).toMatch(/^chore\/lock-file-maintenance-\d{4}-\d{2}-\d{2}-\d{4}$/);
  });

  test('creates PR targeting base branch (main) directly, not stacked', async () => {
    const deps = makeDeps();

    await lockFileMaintenance(makeConfig(), makeOptions(), deps);

    expect(deps.createPR).toHaveBeenCalledTimes(1);
    const createPRArgs = (deps.createPR as ReturnType<typeof mock>).mock.calls[0]!;
    expect(createPRArgs[2]).toMatchObject({
      baseBranch: 'main',
    });
  });

  test('skips git operations when skipGit is true', async () => {
    const deps = makeDeps();

    await lockFileMaintenance(makeConfig(), makeOptions({ skipGit: true }), deps);

    expect(deps.createBranch).not.toHaveBeenCalled();
    expect(deps.stageFiles).not.toHaveBeenCalled();
    expect(deps.commit).not.toHaveBeenCalled();
    expect(deps.pushWithUpstream).not.toHaveBeenCalled();
    expect(deps.createPR).not.toHaveBeenCalled();
  });

  test('handles PR creation failure by cleaning up remote branch', async () => {
    const deps = makeDeps({
      createPR: mock(() => {
        throw new Error('PR creation failed: conflict');
      }),
    });

    await lockFileMaintenance(makeConfig(), makeOptions(), deps);

    expect(deps.deleteRemoteBranch).toHaveBeenCalledTimes(1);
    const deleteArgs = (deps.deleteRemoteBranch as ReturnType<typeof mock>).mock.calls[0]!;
    expect(deleteArgs[0]).toBe('/repo');
    expect(deleteArgs[1]).toBe('origin');
    expect(deleteArgs[2]).toMatch(/^chore\/lock-file-maintenance-/);
  });
});
