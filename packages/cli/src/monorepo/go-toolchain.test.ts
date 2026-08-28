import { describe, expect, test } from 'bun:test';
import { mkdirSync, writeFileSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  normaliseGoVersion,
  readGoToolchainPair,
  ttscPlatformPackage,
  validateGoToolchainPair,
  vendoredVersionCandidates,
} from './go-toolchain.js';

describe('normaliseGoVersion', () => {
  test('reduces `go version` output and a bare VERSION line to the same form', () => {
    expect(normaliseGoVersion('go version go1.26.5 darwin/arm64')).toBe('go1.26.5');
    expect(normaliseGoVersion('go1.26.5\ntime 2026-07-01T21:24:27Z\n')).toBe('go1.26.5');
  });

  test('keeps prerelease qualifiers distinct so a release candidate never matches a release', () => {
    expect(normaliseGoVersion('go version go1.27rc3 linux/amd64')).toBe('go1.27rc3');
    expect(normaliseGoVersion('go1.27rc3')).not.toBe(normaliseGoVersion('go1.27'));
  });

  test('reports absence rather than inventing a version', () => {
    expect(normaliseGoVersion('command not found')).toBeNull();
  });
});

describe('ttscPlatformPackage', () => {
  test('names the published package for supported platforms', () => {
    expect(ttscPlatformPackage('darwin', 'arm64')).toBe('darwin-arm64');
    expect(ttscPlatformPackage('linux', 'x64')).toBe('linux-x64');
    expect(ttscPlatformPackage('win32', 'arm64')).toBe('win32-arm64');
  });

  test('publishes 32-bit arm for linux only, matching ttsc optionalDependencies', () => {
    expect(ttscPlatformPackage('linux', 'arm')).toBe('linux-arm');
    expect(ttscPlatformPackage('darwin', 'arm')).toBeNull();
  });

  test('returns null for platforms ttsc does not publish', () => {
    expect(ttscPlatformPackage('freebsd', 'x64')).toBeNull();
    expect(ttscPlatformPackage('linux', 'riscv64')).toBeNull();
  });
});

describe('validateGoToolchainPair', () => {
  test('agreement passes', () => {
    expect(
      validateGoToolchainPair({
        devenv: 'go1.26.7',
        vendored: 'go1.26.7',
        ttscVersion: '0.28.3',
        vendoredPath: '/x/VERSION',
      }),
    ).toBe(0);
  });

  test('a patch-level divergence fails — this is the case that produced the build error', () => {
    expect(
      validateGoToolchainPair({
        devenv: 'go1.26.5',
        vendored: 'go1.26.6',
        ttscVersion: '0.28.2',
        vendoredPath: '/x/VERSION',
      }),
    ).toBe(1);
  });
});

describe('readGoToolchainPair', () => {
  test('no ttsc dependency means no invariant to check', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-go-toolchain-'));
    expect(readGoToolchainPair(root, 'go', 'go version go1.26.7 darwin/arm64')).toBeNull();
  });

  test('an installed ttsc whose vendored SDK is missing is an error, never a silent pass', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-go-toolchain-'));
    mkdirSync(join(root, 'node_modules', 'ttsc'), { recursive: true });
    writeFileSync(join(root, 'node_modules', 'ttsc', 'package.json'), JSON.stringify({ version: '0.28.3' }));
    const result = readGoToolchainPair(root, 'go', 'go version go1.26.7 darwin/arm64');
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toContain('vendored Go SDK VERSION file was not found');
  });

  test('reads the pair from the bun store layout, which is where a real install puts it', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-go-toolchain-'));
    mkdirSync(join(root, 'node_modules', 'ttsc'), { recursive: true });
    writeFileSync(join(root, 'node_modules', 'ttsc', 'package.json'), JSON.stringify({ version: '0.28.3' }));
    const platformPackage = ttscPlatformPackage(process.platform, process.arch);
    expect(platformPackage).not.toBeNull();
    const storePath = vendoredVersionCandidates(root, platformPackage as string, '0.28.3')[1] as string;
    mkdirSync(join(storePath, '..'), { recursive: true });
    writeFileSync(storePath, 'go1.26.7\ntime 2026-08-18T21:44:21Z\n');

    const result = readGoToolchainPair(root, 'go', 'go version go1.26.7 darwin/arm64');
    expect(result).not.toBeInstanceOf(Error);
    expect(result).toMatchObject({ devenv: 'go1.26.7', vendored: 'go1.26.7', ttscVersion: '0.28.3' });
    expect(validateGoToolchainPair(result as never)).toBe(0);
  });

  test('an unreadable devenv go version is an error rather than a pass', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-go-toolchain-'));
    mkdirSync(join(root, 'node_modules', 'ttsc'), { recursive: true });
    writeFileSync(join(root, 'node_modules', 'ttsc', 'package.json'), JSON.stringify({ version: '0.28.3' }));
    const platformPackage = ttscPlatformPackage(process.platform, process.arch) as string;
    const storePath = vendoredVersionCandidates(root, platformPackage, '0.28.3')[1] as string;
    mkdirSync(join(storePath, '..'), { recursive: true });
    writeFileSync(storePath, 'go1.26.7\n');

    const result = readGoToolchainPair(root, 'go', 'bash: go: command not found');
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toContain('no recognisable Go version');
  });
});
