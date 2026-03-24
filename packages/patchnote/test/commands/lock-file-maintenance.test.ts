import { describe, expect, test, beforeEach, mock } from 'bun:test'

// Mock @clack/prompts to suppress UI output in tests
mock.module('@clack/prompts', () => ({
  intro: () => {},
  outro: () => {},
  note: () => {},
  log: { info: () => {}, warn: () => {}, error: () => {}, step: () => {} },
  spinner: () => ({ start: () => {}, stop: () => {} }),
}))

// Mock git operations
const mockGetRepoRoot = mock(() => Promise.resolve('/repo'))
const mockCreateBranch = mock(() => Promise.resolve())
const mockCreateUpdateCommit = mock(() => Promise.resolve())
const mockPushWithUpstream = mock(() => Promise.resolve())
const mockDeleteRemoteBranch = mock(() => Promise.resolve())
const mockFetch = mock(() => Promise.resolve())
const mockSwitchBranch = mock(() => Promise.resolve())

mock.module('../../src/git.js', () => ({
  getRepoRoot: mockGetRepoRoot,
  createBranch: mockCreateBranch,
  createUpdateCommit: mockCreateUpdateCommit,
  pushWithUpstream: mockPushWithUpstream,
  deleteRemoteBranch: mockDeleteRemoteBranch,
  fetch: mockFetch,
  switchBranch: mockSwitchBranch,
}))

// Mock refreshLockFile
const mockRefreshLockFile = mock(() => Promise.resolve({ changed: true }))
mock.module('../../src/updaters/bun.js', () => ({
  refreshLockFile: mockRefreshLockFile,
}))

// Mock createPR
const mockCreatePR = mock(() => Promise.resolve({ number: 42, url: 'https://github.com/test/repo/pull/42' }))
mock.module('../../src/pr/stacking.js', () => ({
  createPR: mockCreatePR,
}))

// Mock resolveSemanticPrefix
const mockResolveSemanticPrefix = mock(() => Promise.resolve('chore(deps)'))
mock.module('../../src/semantic.js', () => ({
  resolveSemanticPrefix: mockResolveSemanticPrefix,
}))

import { lockFileMaintenance } from '../../src/commands/lock-file-maintenance.js'
import type { PatchnoteConfig } from '../../src/config.js'
import type { UpdateOptions } from '../../src/types.js'

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
  } as PatchnoteConfig
}

function makeOptions(overrides: Partial<UpdateOptions> = {}): UpdateOptions {
  return {
    dryRun: false,
    skipGit: false,
    skipAI: false,
    ...overrides,
  }
}

describe('lockFileMaintenance command', () => {
  beforeEach(() => {
    mockGetRepoRoot.mockReset()
    mockGetRepoRoot.mockImplementation(() => Promise.resolve('/repo'))
    mockCreateBranch.mockReset()
    mockCreateBranch.mockImplementation(() => Promise.resolve())
    mockCreateUpdateCommit.mockReset()
    mockCreateUpdateCommit.mockImplementation(() => Promise.resolve())
    mockPushWithUpstream.mockReset()
    mockPushWithUpstream.mockImplementation(() => Promise.resolve())
    mockDeleteRemoteBranch.mockReset()
    mockDeleteRemoteBranch.mockImplementation(() => Promise.resolve())
    mockFetch.mockReset()
    mockFetch.mockImplementation(() => Promise.resolve())
    mockSwitchBranch.mockReset()
    mockSwitchBranch.mockImplementation(() => Promise.resolve())
    mockRefreshLockFile.mockReset()
    mockRefreshLockFile.mockImplementation(() => Promise.resolve({ changed: true }))
    mockCreatePR.mockReset()
    mockCreatePR.mockImplementation(() =>
      Promise.resolve({ number: 42, url: 'https://github.com/test/repo/pull/42' }),
    )
    mockResolveSemanticPrefix.mockReset()
    mockResolveSemanticPrefix.mockImplementation(() => Promise.resolve('chore(deps)'))
  })

  test('exits early with message when no lock file changes detected', async () => {
    mockRefreshLockFile.mockImplementation(() => Promise.resolve({ changed: false }))

    await lockFileMaintenance(makeConfig(), makeOptions())

    // Should not attempt git operations
    expect(mockCreateBranch).not.toHaveBeenCalled()
    expect(mockCreateUpdateCommit).not.toHaveBeenCalled()
    expect(mockPushWithUpstream).not.toHaveBeenCalled()
    expect(mockCreatePR).not.toHaveBeenCalled()
  })

  test('in dry-run mode reports status without creating PR', async () => {
    // In dry-run mode, refreshLockFile returns { changed: false }
    mockRefreshLockFile.mockImplementation(() => Promise.resolve({ changed: false }))

    await lockFileMaintenance(makeConfig(), makeOptions({ dryRun: true }))

    // refreshLockFile should be called with dryRun: true
    expect(mockRefreshLockFile).toHaveBeenCalled()
    const callArgs = mockRefreshLockFile.mock.calls[0]!
    expect(callArgs[1]).toMatchObject({ dryRun: true })

    // Should not create PR
    expect(mockCreateBranch).not.toHaveBeenCalled()
    expect(mockCreatePR).not.toHaveBeenCalled()
  })

  test('creates branch with correct prefix format', async () => {
    await lockFileMaintenance(makeConfig(), makeOptions())

    expect(mockCreateBranch).toHaveBeenCalledTimes(1)
    const branchName = mockCreateBranch.mock.calls[0]![1] as string
    // Branch should start with configured prefix and end with date-time suffix
    expect(branchName).toMatch(/^chore\/lock-file-maintenance-\d{4}-\d{2}-\d{2}-\d{4}$/)
  })

  test('creates PR targeting base branch (main) directly, not stacked', async () => {
    await lockFileMaintenance(makeConfig(), makeOptions())

    expect(mockCreatePR).toHaveBeenCalledTimes(1)
    const createPRArgs = mockCreatePR.mock.calls[0]!
    // Third argument is the options object with baseBranch
    expect(createPRArgs[2]).toMatchObject({
      baseBranch: 'main',
    })
  })

  test('skips git operations when skipGit is true', async () => {
    await lockFileMaintenance(makeConfig(), makeOptions({ skipGit: true }))

    expect(mockCreateBranch).not.toHaveBeenCalled()
    expect(mockCreateUpdateCommit).not.toHaveBeenCalled()
    expect(mockPushWithUpstream).not.toHaveBeenCalled()
    expect(mockCreatePR).not.toHaveBeenCalled()
  })

  test('handles PR creation failure by cleaning up remote branch', async () => {
    mockCreatePR.mockImplementation(() => {
      throw new Error('PR creation failed: conflict')
    })

    // Should not throw - the command handles the error
    await lockFileMaintenance(makeConfig(), makeOptions())

    // Verify deleteRemoteBranch was called to clean up
    expect(mockDeleteRemoteBranch).toHaveBeenCalledTimes(1)
    const deleteArgs = mockDeleteRemoteBranch.mock.calls[0]!
    expect(deleteArgs[0]).toBe('/repo')
    expect(deleteArgs[1]).toBe('origin')
    // Branch name should match the created branch
    expect(deleteArgs[2]).toMatch(/^chore\/lock-file-maintenance-/)
  })
})
