import { afterAll, describe, expect, it } from 'bun:test';
import { execSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import fc from 'fast-check';
import { syncBunLockfileVersions, validateBunLockfileVersions } from './lockfile.js';

const cleanupRoots: string[] = [];
afterAll(() => {
  for (const root of cleanupRoots) {
    rmSync(root, { recursive: true, force: true });
  }
});

function git(root: string, args: string): string {
  return execSync(`git ${args}`, {
    cwd: root,
    encoding: 'utf8',
    stdio: ['pipe', 'pipe', 'pipe'],
  });
}

interface FixtureOptions {
  packageVersion: string;
  lockVersion: string;
}

function makeFixtureRepo(options: FixtureOptions): string {
  const root = mkdtempSync(join(tmpdir(), 'smoo-lockfile-'));
  cleanupRoots.push(root);
  git(root, 'init -q');
  git(root, 'commit -q --allow-empty -m init');
  writeFileSync(
    join(root, 'package.json'),
    JSON.stringify({ name: 'fixture-root', version: '0.0.0', private: true, workspaces: ['packages/*'] }),
  );
  mkdirSync(join(root, 'packages', 'foo'), { recursive: true });
  writeFixturePackage(root, options.packageVersion);
  writeFixtureLock(root, options.lockVersion);
  return root;
}

function writeFixturePackage(root: string, version: string): void {
  writeFileSync(
    join(root, 'packages/foo/package.json'),
    `${JSON.stringify({ name: '@fixture/foo', version, private: false, nx: { name: 'foo' } }, null, 2)}\n`,
  );
}

function writeFixtureLock(root: string, version: string): void {
  writeFileSync(
    join(root, 'bun.lock'),
    [
      '{',
      '  "lockfileVersion": 1,',
      '  "workspaces": {',
      '    "": { "name": "fixture-root" },',
      '    "packages/foo": {',
      '      "name": "@fixture/foo",',
      `      "version": "${version}"`,
      '    }',
      '  }',
      '}',
      '',
    ].join('\n'),
  );
}

function lockVersionIn(root: string): string {
  const match = readFileSync(join(root, 'bun.lock'), 'utf8').match(/"packages\/foo":\s*\{[^}]*"version":\s*"([^"]+)"/);
  if (!match) throw new Error('fixture lockfile lost its workspace entry');
  return match[1] as string;
}

describe('bun.lock workspace version sync', () => {
  it('a stale lockfile version fails validation against package.json', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.2',
    });
    expect(validateBunLockfileVersions(root)).toBe(1);
  });

  it('sync restores the package.json version and validation passes', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.2',
    });
    expect(syncBunLockfileVersions(root, { log: false })).toBe(1);
    expect(lockVersionIn(root)).toBe('0.1.3-next.0');
    expect(validateBunLockfileVersions(root)).toBe(0);
  });

  it('a prerelease package keeps its manifest version in the lockfile', () => {
    const root = makeFixtureRepo({ packageVersion: '0.2.0-next.0', lockVersion: '0.2.0-next.0' });
    expect(validateBunLockfileVersions(root)).toBe(0);
    expect(syncBunLockfileVersions(root, { log: false })).toBe(0);
  });

  it('a stable manifest version is required verbatim in the lockfile', () => {
    const root = makeFixtureRepo({ packageVersion: '0.1.0', lockVersion: '0.0.9' });
    expect(validateBunLockfileVersions(root)).toBe(1);
    expect(syncBunLockfileVersions(root, { log: false })).toBe(1);
    expect(lockVersionIn(root)).toBe('0.1.0');
  });

  it('stage: true stages the healed bun.lock', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.2',
    });
    expect(syncBunLockfileVersions(root, { log: false, stage: true })).toBe(1);
    expect(git(root, 'diff --cached --name-only')).toContain('bun.lock');
  });

  it('stage: true leaves a clean lockfile unstaged', () => {
    const root = makeFixtureRepo({ packageVersion: '0.1.2', lockVersion: '0.1.2' });
    expect(syncBunLockfileVersions(root, { log: false, stage: true })).toBe(0);
    expect(git(root, 'diff --cached --name-only').trim()).toBe('');
  });

  it('property: after sync, validation always passes', () => {
    const version = fc
      .tuple(fc.nat({ max: 20 }), fc.nat({ max: 20 }), fc.nat({ max: 20 }), fc.option(fc.nat({ max: 5 })))
      .map(([major, minor, patch, next]) => `${major}.${minor}.${patch}${next === null ? '' : `-next.${next}`}`);
    const root = makeFixtureRepo({ packageVersion: '0.1.2', lockVersion: '0.1.2' });
    fc.assert(
      fc.property(version, version, (packageVersion, lockVersion) => {
        writeFixturePackage(root, packageVersion);
        writeFixtureLock(root, lockVersion);
        syncBunLockfileVersions(root, { log: false });
        return validateBunLockfileVersions(root) === 0;
      }),
      { numRuns: 25 },
    );
  });
});
