import { describe, expect, it, spyOn } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { validateSccachePatches } from './sccache-patches.js';

async function withFixture<T>(files: Record<string, string>, callback: (root: string) => T): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-sccache-patches-'));
  try {
    for (const [path, content] of Object.entries(files)) {
      const target = join(root, path);
      await mkdir(dirname(target), { recursive: true });
      await writeFile(target, content);
    }
    return callback(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function check(files: Record<string, string>): Promise<{ failures: number; messages: string[] }> {
  return withFixture(files, (root) => {
    const captured = captureErrors();
    try {
      return { failures: validateSccachePatches(root), messages: captured.messages };
    } finally {
      captured.restore();
    }
  });
}

function captureErrors(): { messages: string[]; restore: () => void } {
  const messages: string[] = [];
  const error = spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    messages.push(args.join(' '));
  });
  return { messages, restore: () => error.mockRestore() };
}

function md5(content: string): string {
  return createHash('md5').update(content).digest('hex');
}

describe('sccache patch identity', () => {
  it('returns zero when both directories hold the same named bytes', async () => {
    const result = await check({
      'packages/cowshed/patches/sccache-singleflight.patch': 'body\n',
      'tooling/direnv/nixpkgs-overlay/sccache-singleflight.patch': 'body\n',
      'tooling/direnv/nixpkgs-overlay/flake.nix': '{}\n',
    });
    expect(result.failures).toBe(0);
    expect(result.messages).toEqual([]);
  });

  it('reports both paths and both md5 hashes when a counterpart differs', async () => {
    const authoritative = 'alpha\n';
    const overlay = 'beta\n';
    const result = await check({
      'packages/cowshed/patches/sccache-singleflight.patch': authoritative,
      'tooling/direnv/nixpkgs-overlay/sccache-singleflight.patch': overlay,
    });
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      `packages/cowshed/patches/sccache-singleflight.patch: md5 ${md5(authoritative)} != tooling/direnv/nixpkgs-overlay/sccache-singleflight.patch md5 ${md5(overlay)}`,
    ]);
  });

  it('reports a patch present only in the authoritative directory', async () => {
    const result = await check({
      'packages/cowshed/patches/sccache-singleflight.patch': 'body\n',
    });
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      'packages/cowshed/patches/sccache-singleflight.patch: no counterpart at tooling/direnv/nixpkgs-overlay/sccache-singleflight.patch',
    ]);
  });

  it('reports a patch present only in the overlay', async () => {
    const result = await check({
      'tooling/direnv/nixpkgs-overlay/sccache-singleflight.patch': 'body\n',
    });
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      'tooling/direnv/nixpkgs-overlay/sccache-singleflight.patch: no counterpart at packages/cowshed/patches/sccache-singleflight.patch',
    ]);
  });
});
