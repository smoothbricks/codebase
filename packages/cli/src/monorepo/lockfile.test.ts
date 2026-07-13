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
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: '/dev/null',
      GIT_CONFIG_SYSTEM: '/dev/null',
      GIT_AUTHOR_NAME: 'fixture',
      GIT_AUTHOR_EMAIL: 'fixture@invalid',
      GIT_COMMITTER_NAME: 'fixture',
      GIT_COMMITTER_EMAIL: 'fixture@invalid',
    },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
}

interface FixtureOptions {
  packageVersion: string;
  lockVersion: string;
  stableTag?: string;
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
  if (options.stableTag) {
    git(root, `tag ${options.stableTag}`);
  }
  return root;
}

function writeFixturePackage(root: string, version: string): void {
  writeFileSync(
    join(root, 'packages', 'foo', 'package.json'),
    JSON.stringify({ name: '@fixture/foo', version, nx: { name: 'foo' } }),
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
      `      "version": "${version}",`,
      '    },',
      '  },',
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
  it('a bun-install revert of the release-synced version fails validation', () => {
    // The b3298fc4c shape: package.json holds the next prerelease, a stable tag
    // exists, and `bun install` rewrote the lockfile back to the prerelease.
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.3-next.0',
      stableTag: 'foo@0.1.2',
    });
    expect(validateBunLockfileVersions(root)).toBe(1);
  });

  it('sync restores the latest stable tag version and validation passes', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.3-next.0',
      stableTag: 'foo@0.1.2',
    });
    expect(syncBunLockfileVersions(root, { log: false })).toBe(1);
    expect(lockVersionIn(root)).toBe('0.1.2');
    expect(validateBunLockfileVersions(root)).toBe(0);
  });

  it('a never-published prerelease package falls back to its manifest version', () => {
    // The new-workspace-package shape (e.g. cowshed): no stable tag exists yet.
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
      lockVersion: '0.1.3-next.0',
      stableTag: 'foo@0.1.2',
    });
    expect(syncBunLockfileVersions(root, { log: false, stage: true })).toBe(1);
    expect(git(root, 'diff --cached --name-only')).toContain('bun.lock');
  });

  it('stage: true leaves a clean lockfile unstaged', () => {
    const root = makeFixtureRepo({ packageVersion: '0.1.2', lockVersion: '0.1.2', stableTag: 'foo@0.1.2' });
    expect(syncBunLockfileVersions(root, { log: false, stage: true })).toBe(0);
    expect(git(root, 'diff --cached --name-only').trim()).toBe('');
  });

  it('property: after sync, validation always passes', () => {
    const version = fc
      .tuple(fc.nat({ max: 20 }), fc.nat({ max: 20 }), fc.nat({ max: 20 }), fc.option(fc.nat({ max: 5 })))
      .map(([major, minor, patch, next]) => `${major}.${minor}.${patch}${next === null ? '' : `-next.${next}`}`);
    const root = makeFixtureRepo({ packageVersion: '0.1.2', lockVersion: '0.1.2', stableTag: 'foo@0.1.2' });
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
