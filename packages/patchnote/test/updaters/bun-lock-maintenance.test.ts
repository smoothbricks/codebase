import { describe, expect, test } from 'bun:test';
import { refreshLockFile } from '../../src/updaters/bun.js';
import { createErrorExeca, createExecaSpy } from '../helpers/mock-execa.js';

describe('refreshLockFile', () => {
  test('runs bun install --force and returns changed: true when bun.lock changed', async () => {
    const spy = createExecaSpy({
      'bun install --force': '',
      'git diff --name-only -- bun.lock bun.lockb': 'bun.lock\n',
    });

    const result = await refreshLockFile('/repo', { executor: spy.mock });

    expect(result).toEqual({ changed: true });
    expect(spy.calls).toHaveLength(2);
    expect(spy.calls[0]![1]).toEqual(['install', '--force']);
    expect(spy.calls[1]![1]).toEqual(['diff', '--name-only', '--', 'bun.lock', 'bun.lockb']);
  });

  test('returns changed: true when bun.lockb changed (binary lock format)', async () => {
    const spy = createExecaSpy({
      'bun install --force': '',
      'git diff --name-only -- bun.lock bun.lockb': 'bun.lockb\n',
    });

    const result = await refreshLockFile('/repo', { executor: spy.mock });

    expect(result).toEqual({ changed: true });
  });

  test('returns changed: false when git diff shows no lock file changes', async () => {
    const spy = createExecaSpy({
      'bun install --force': '',
      'git diff --name-only -- bun.lock bun.lockb': '',
    });

    const result = await refreshLockFile('/repo', { executor: spy.mock });

    expect(result).toEqual({ changed: false });
  });

  test('in dry-run mode does NOT run bun install --force', async () => {
    const spy = createExecaSpy({});

    const result = await refreshLockFile('/repo', { dryRun: true, executor: spy.mock });

    expect(result).toEqual({ changed: false });
    expect(spy.calls).toHaveLength(0);
  });

  test('passes correct cwd to execa', async () => {
    const spy = createExecaSpy({
      'bun install --force': '',
      'git diff --name-only -- bun.lock bun.lockb': 'bun.lock\n',
    });

    await refreshLockFile('/my/custom/repo', { executor: spy.mock });

    expect(spy.calls[0]![2]).toEqual({ cwd: '/my/custom/repo' });
    expect(spy.calls[1]![2]).toEqual({ cwd: '/my/custom/repo' });
  });

  test('catches errors from bun install --force and returns changed: false with error', async () => {
    const errorExeca = createErrorExeca('bun install failed: network error');

    const result = await refreshLockFile('/repo', { executor: errorExeca });

    expect(result.changed).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.error).toContain('bun install failed: network error');
  });
});
