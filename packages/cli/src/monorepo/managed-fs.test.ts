import { afterEach, describe, expect, it } from 'bun:test';
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { INLINE_LOCAL_BEGIN, INLINE_LOCAL_END } from './managed-content.js';
import { syncManagedFiles } from './managed-fs.js';
import type { ManagedFileSpec } from './managed-plan.js';

const directories: string[] = [];
function temporaryWorkspace(): string {
  const directory = mkdtempSync(join(tmpdir(), 'smoo-managed-'));
  directories.push(directory);
  return directory;
}
const spec: ManagedFileSpec = { target: 'config', desired: { content: 'new\n', executable: false } };
afterEach(() => {
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe('managed filesystem shell', () => {
  it.each(['check', 'diff'] as const)('%s is read-only, including missing directories', (mode) => {
    const root = temporaryWorkspace();
    expect(syncManagedFiles(root, [{ ...spec, target: 'nested/config' }], mode)).toEqual([
      { target: 'nested/config', action: 'drifted' },
    ]);
    expect(readdirSync(root)).toEqual([]);
  });

  it('updates then checks without drift and leaves no temporary files', () => {
    const root = temporaryWorkspace();
    expect(syncManagedFiles(root, [spec], 'update')[0]?.action).toBe('created');
    expect(readFileSync(join(root, spec.target), 'utf8')).toBe('new\n');
    expect(syncManagedFiles(root, [spec], 'check')[0]?.action).toBe('unchanged');
    expect(readdirSync(root)).toEqual(['config']);
  });

  it.each([0o644, 0o645])('reports and repairs owner-execute drift (mode %i)', (mode) => {
    const root = temporaryWorkspace();
    const script = { ...spec, desired: { content: '#!/bin/sh\n', executable: true } };
    writeFileSync(join(root, spec.target), script.desired.content);
    chmodSync(join(root, spec.target), mode);
    expect(syncManagedFiles(root, [script], 'check')[0]?.action).toBe('drifted');
    syncManagedFiles(root, [script], 'update');
    expect(statSync(join(root, spec.target)).mode & 0o777).toBe(0o755);
    expect(syncManagedFiles(root, [script], 'check')[0]?.action).toBe('unchanged');
  });

  it('does not write earlier files if a later target is a directory', () => {
    const root = temporaryWorkspace();
    writeFileSync(join(root, 'first'), 'old');
    mkdirSync(join(root, 'config'));
    expect(() => syncManagedFiles(root, [{ ...spec, target: 'first' }, spec], 'update')).toThrow('before writing');
    expect(readFileSync(join(root, 'first'), 'utf8')).toBe('old');
  });

  it('preflights an inline ownership conflict in both check and update', () => {
    const root = temporaryWorkspace();
    const current = ['removed anchor', INLINE_LOCAL_BEGIN, 'local', INLINE_LOCAL_END].join('\n');
    writeFileSync(join(root, 'config'), current);
    const specs = [{ ...spec, target: 'first' }, spec];
    expect(syncManagedFiles(root, specs, 'check')[1]?.reason).toContain('matches no line');
    expect(() => syncManagedFiles(root, specs, 'update')).toThrow('before writing');
    expect(existsSync(join(root, 'first'))).toBe(false);
    expect(readFileSync(join(root, 'config'), 'utf8')).toBe(current);
  });

  it('does not follow a dangling symlink to create its destination', () => {
    const root = temporaryWorkspace();
    symlinkSync('missing', join(root, 'config'));
    expect(syncManagedFiles(root, [spec], 'check')[0]?.action).toBe('drifted');
    expect(() => syncManagedFiles(root, [spec], 'update')).toThrow('symlink');
    expect(existsSync(join(root, 'missing'))).toBe(false);
    expect(lstatSync(join(root, 'config')).isSymbolicLink()).toBe(true);
  });

  it('preserves a matching in-workspace source link', () => {
    const root = temporaryWorkspace();
    writeFileSync(join(root, 'source'), 'new\n');
    chmodSync(join(root, 'source'), 0o644);
    symlinkSync('source', join(root, 'config'));
    expect(syncManagedFiles(root, [spec], 'check')[0]?.action).toBe('ok-symlink');
    expect(syncManagedFiles(root, [spec], 'update')[0]?.action).toBe('skipped-symlink');
    expect(lstatSync(join(root, 'config')).isSymbolicLink()).toBe(true);
  });

  it('refuses an out-of-workspace link even when its contents match', () => {
    const root = temporaryWorkspace();
    const outside = temporaryWorkspace();
    writeFileSync(join(outside, 'source'), 'new\n');
    symlinkSync(join(outside, 'source'), join(root, 'config'));
    expect(() => syncManagedFiles(root, [spec], 'update')).toThrow('outside the workspace');
    expect(readFileSync(join(outside, 'source'), 'utf8')).toBe('new\n');
  });

  it('refuses symlinked parent directories without changing their contents', () => {
    const root = temporaryWorkspace();
    const outside = temporaryWorkspace();
    symlinkSync(outside, join(root, 'nested'));
    expect(() => syncManagedFiles(root, [{ ...spec, target: 'nested/config' }], 'update')).toThrow('parent path');
    expect(readdirSync(outside)).toEqual([]);
  });

  it('leaves a capability-disabled file and its customizations untouched', () => {
    const root = temporaryWorkspace();
    writeFileSync(join(root, 'config'), 'repo customization');
    expect(syncManagedFiles(root, [{ ...spec, desired: null }], 'update')[0]?.action).toBe('skipped');
    expect(readFileSync(join(root, 'config'), 'utf8')).toBe('repo customization');
  });
});
