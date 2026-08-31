import { describe, expect, it, spyOn } from 'bun:test';
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

const FLAKE = 'packages/cowshed/nix/sccache/flake.nix';
const PATCH = 'packages/cowshed/nix/sccache/sccache-singleflight.patch';

/** A flake body shaped like the real one: a `patches` list of nix relative paths. */
function flakeReferencing(...names: string[]): string {
  return `{\n  patches = [\n${names.map((name) => `    ./${name}\n`).join('')}  ];\n}\n`;
}

describe('sccache patch references', () => {
  it('returns zero when the directory and the flake name each other exactly', async () => {
    const result = await check({
      [PATCH]: 'body\n',
      [FLAKE]: flakeReferencing('sccache-singleflight.patch'),
    });
    expect(result.failures).toBe(0);
    expect(result.messages).toEqual([]);
  });

  // The quiet failure: nix ignores a `.patch` nobody references, so the file reads as applied
  // while the built binary does not carry it.
  it('reports a patch the flake does not reference', async () => {
    const result = await check({
      [PATCH]: 'body\n',
      'packages/cowshed/nix/sccache/sccache-orphan.patch': 'body\n',
      [FLAKE]: flakeReferencing('sccache-singleflight.patch'),
    });
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      'packages/cowshed/nix/sccache/sccache-orphan.patch: not referenced by flake.nix; nix would ignore it and the built sccache would not carry it',
    ]);
  });

  it('reports a reference to a patch that is not there', async () => {
    const result = await check({
      [FLAKE]: flakeReferencing('sccache-singleflight.patch'),
    });
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      'packages/cowshed/nix/sccache/flake.nix: references ./sccache-singleflight.patch, which is not there',
    ]);
  });

  // A flake applying nothing still builds a binary calling itself `-cowshed`, so every client
  // would trust a version string promising behaviour the binary does not have.
  it('reports a flake that applies no patches at all', async () => {
    const result = await check({ [FLAKE]: '{}\n' });
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      'packages/cowshed/nix/sccache/flake.nix: references no .patch files; the patched sccache is what this flake is for',
    ]);
  });

  it('reports a missing flake rather than passing on an empty directory', async () => {
    const result = await check({});
    expect(result.failures).toBe(1);
    expect(result.messages).toEqual([
      'packages/cowshed/nix/sccache/flake.nix: missing; the sccache flake is not there',
    ]);
  });
});
