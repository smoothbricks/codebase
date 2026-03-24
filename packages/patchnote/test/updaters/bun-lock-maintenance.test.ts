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

  test('with packageManager: pnpm runs pnpm install --force and checks pnpm-lock.yaml', async () => {
    const spy = createExecaSpy({
      'pnpm install --force': '',
      'git diff --name-only -- pnpm-lock.yaml': 'pnpm-lock.yaml\n',
    });

    const result = await refreshLockFile('/repo', { executor: spy.mock, packageManager: 'pnpm' });

    expect(result).toEqual({ changed: true });
    expect(spy.calls).toHaveLength(2);
    expect(spy.calls[0]![0]).toBe('pnpm');
    expect(spy.calls[0]![1]).toEqual(['install', '--force']);
    expect(spy.calls[1]![1]).toEqual(['diff', '--name-only', '--', 'pnpm-lock.yaml']);
  });

  test('with packageManager: npm runs npm install and checks package-lock.json', async () => {
    const spy = createExecaSpy({
      'npm install': '',
      'git diff --name-only -- package-lock.json': 'package-lock.json\n',
    });

    const result = await refreshLockFile('/repo', { executor: spy.mock, packageManager: 'npm' });

    expect(result).toEqual({ changed: true });
    expect(spy.calls).toHaveLength(2);
    expect(spy.calls[0]![0]).toBe('npm');
    expect(spy.calls[0]![1]).toEqual(['install']);
    expect(spy.calls[1]![1]).toEqual(['diff', '--name-only', '--', 'package-lock.json']);
  });

  test('with packageManager: yarn runs yarn install --force and checks yarn.lock', async () => {
    const spy = createExecaSpy({
      'yarn install --force': '',
      'git diff --name-only -- yarn.lock': '',
    });

    const result = await refreshLockFile('/repo', { executor: spy.mock, packageManager: 'yarn' });

    expect(result).toEqual({ changed: false });
    expect(spy.calls[0]![0]).toBe('yarn');
    expect(spy.calls[0]![1]).toEqual(['install', '--force']);
    expect(spy.calls[1]![1]).toEqual(['diff', '--name-only', '--', 'yarn.lock']);
  });
});
