import { describe, expect, test } from 'bun:test';
import { getPackageManagerCommands } from '../../src/updaters/package-manager.js';

describe('getPackageManagerCommands', () => {
  test('bun returns correct command structure', () => {
    const pm = getPackageManagerCommands('bun');

    expect(pm.cmd).toBe('bun');
    expect(pm.updateArgs).toEqual(['update']);
    expect(pm.recursiveFlag).toBe('--recursive');
    expect(pm.installArgs).toEqual(['install']);
    expect(pm.forceRefreshArgs).toEqual(['install', '--force']);
    expect(pm.lockFileNames).toEqual(['bun.lock', 'bun.lockb']);
    expect(pm.outdatedArgs).toEqual(['outdated']);
  });

  test('npm returns correct command structure', () => {
    const pm = getPackageManagerCommands('npm');

    expect(pm.cmd).toBe('npm');
    expect(pm.updateArgs).toEqual(['update']);
    expect(pm.recursiveFlag).toBeUndefined();
    expect(pm.installArgs).toEqual(['install']);
    expect(pm.forceRefreshArgs).toEqual(['install']);
    expect(pm.lockFileNames).toEqual(['package-lock.json']);
    expect(pm.outdatedArgs).toEqual(['outdated', '--json']);
  });

  test('pnpm returns correct command structure', () => {
    const pm = getPackageManagerCommands('pnpm');

    expect(pm.cmd).toBe('pnpm');
    expect(pm.updateArgs).toEqual(['update']);
    expect(pm.recursiveFlag).toBe('--recursive');
    expect(pm.installArgs).toEqual(['install']);
    expect(pm.forceRefreshArgs).toEqual(['install', '--force']);
    expect(pm.lockFileNames).toEqual(['pnpm-lock.yaml']);
    expect(pm.outdatedArgs).toEqual(['outdated', '--format', 'json']);
  });

  test('yarn returns correct command structure', () => {
    const pm = getPackageManagerCommands('yarn');

    expect(pm.cmd).toBe('yarn');
    expect(pm.updateArgs).toEqual(['upgrade']);
    expect(pm.recursiveFlag).toBeUndefined();
    expect(pm.installArgs).toEqual(['install']);
    expect(pm.forceRefreshArgs).toEqual(['install', '--force']);
    expect(pm.lockFileNames).toEqual(['yarn.lock']);
    expect(pm.outdatedArgs).toEqual(['outdated', '--json']);
  });

  describe('runScriptArgs', () => {
    test('bun returns correct run script args', () => {
      const pm = getPackageManagerCommands('bun');
      expect(pm.runScriptArgs('syncpack:fix')).toEqual(['run', 'syncpack:fix']);
    });

    test('npm returns correct run script args', () => {
      const pm = getPackageManagerCommands('npm');
      expect(pm.runScriptArgs('syncpack:fix')).toEqual(['run', 'syncpack:fix']);
    });

    test('pnpm returns correct run script args', () => {
      const pm = getPackageManagerCommands('pnpm');
      expect(pm.runScriptArgs('test')).toEqual(['run', 'test']);
    });

    test('yarn returns correct run script args', () => {
      const pm = getPackageManagerCommands('yarn');
      expect(pm.runScriptArgs('build')).toEqual(['run', 'build']);
    });
  });
});
