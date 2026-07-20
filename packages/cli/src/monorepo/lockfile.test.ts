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
  stableTag?: string;
}

function makeFixtureRepo(options: FixtureOptions): string {
  const root = mkdtempSync(join(tmpdir(), 'smoo-lockfile-'));
  cleanupRoots.push(root);
  git(root, 'init -q');
  // Local identity only — CI runners have no global git user.
  git(root, 'config user.name Test');
  git(root, 'config user.email test@example.com');
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
  it('install validate requires lockfile ≡ package.json including -next', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.2',
      stableTag: 'foo@0.1.2',
    });
    expect(validateBunLockfileVersions(root)).toBe(1);
  });

  it('install mode sync restores package.json -next for frozen CI', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.2',
      stableTag: 'foo@0.1.2',
    });
    expect(syncBunLockfileVersions(root, { log: false, mode: 'install' })).toBe(1);
    expect(lockVersionIn(root)).toBe('0.1.3-next.0');
    expect(validateBunLockfileVersions(root)).toBe(0);
  });

  it('publish mode rewrites unpublished -next to last stable tag for pack', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.3-next.0',
      stableTag: 'foo@0.1.2',
    });
    // Install/CI is happy with -next matching package.json.
    expect(validateBunLockfileVersions(root)).toBe(0);
    // Pre-publish rewrite embeds last stable for bun pm pack.
    expect(syncBunLockfileVersions(root, { log: false, mode: 'publish' })).toBe(1);
    expect(lockVersionIn(root)).toBe('0.1.2');
  });

  it('never-published prerelease keeps package.json version in both modes', () => {
    const root = makeFixtureRepo({ packageVersion: '0.2.0-next.0', lockVersion: '0.2.0-next.0' });
    expect(validateBunLockfileVersions(root)).toBe(0);
    expect(syncBunLockfileVersions(root, { log: false, mode: 'install' })).toBe(0);
    expect(syncBunLockfileVersions(root, { log: false, mode: 'publish' })).toBe(0);
  });

  it('stable manifest version is required verbatim', () => {
    const root = makeFixtureRepo({ packageVersion: '0.1.0', lockVersion: '0.0.9' });
    expect(validateBunLockfileVersions(root)).toBe(1);
    expect(syncBunLockfileVersions(root, { log: false, mode: 'install' })).toBe(1);
    expect(lockVersionIn(root)).toBe('0.1.0');
  });

  it('stage: true stages the healed bun.lock', () => {
    const root = makeFixtureRepo({
      packageVersion: '0.1.3-next.0',
      lockVersion: '0.1.2',
      stableTag: 'foo@0.1.2',
    });
    expect(syncBunLockfileVersions(root, { log: false, mode: 'install', stage: true })).toBe(1);
    expect(git(root, 'diff --cached --name-only')).toContain('bun.lock');
  });

  it('property: after install-mode sync, validation always passes', () => {
    const version = fc
      .tuple(fc.nat({ max: 20 }), fc.nat({ max: 20 }), fc.nat({ max: 20 }), fc.option(fc.nat({ max: 5 })))
      .map(([major, minor, patch, next]) => `${major}.${minor}.${patch}${next === null ? '' : `-next.${next}`}`);
    const root = makeFixtureRepo({ packageVersion: '0.1.2', lockVersion: '0.1.2', stableTag: 'foo@0.1.2' });
    fc.assert(
      fc.property(version, version, (packageVersion, lockVersion) => {
        writeFixturePackage(root, packageVersion);
        writeFixtureLock(root, lockVersion);
        syncBunLockfileVersions(root, { log: false, mode: 'install' });
        return validateBunLockfileVersions(root) === 0;
      }),
      { numRuns: 25 },
    );
  });
});
