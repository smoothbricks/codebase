import { expect, it } from 'bun:test';
import { realpath } from 'node:fs/promises';
import { posix } from 'node:path';
import { expandedFilesets, filesetSource } from './__tests__/cargo-closure-filesets.js';
import { cargoClosureInputs } from './cargo-closure-input.js';

const root = await realpath(import.meta.dir);

async function equivalent(directories: readonly string[], files: readonly string[]): Promise<void> {
  const expected = expandedFilesets(directories, [
    'Cargo.toml',
    ...directories.map((directory) => posix.join(directory, 'src/lib.rs')),
    ...files,
  ]);
  expect(await cargoClosureInputs('', [filesetSource(root, directories, files)])).toEqual(expected);
}

it('preserves ordered inputs for roots, nested packages, prefix siblings, and loose targets', async () => {
  for (const directories of [
    [''],
    ['', 'crates/a', 'crates/a/nested'],
    ['crates/a', 'crates/a-', 'crates/a/nested', 'crates/a./child', 'crates/a0', 'crates/ab'],
    ['crates/z/deep/member', 'crates/a/member', 'crates/z/other', 'crates/a/member'],
    ['a/b', 'a/b/c', 'a/b/c/d', 'a/b-', 'a/b-/d', 'a/b0'],
    ['é/a', 'é/a-b', 'é/a/b', 'a b/c', 'a b/c/d'],
  ]) {
    await equivalent(directories, [
      'Cargo.toml',
      'Cargo.lock',
      'shared.rs',
      'crates/Cargo.toml',
      'crates/a/src/lib.rs',
      'crates/a-/outside.rs',
      'crates/a0/src/lib.rs',
      'crates/a/loose.rs',
      'elsewhere/Cargo.toml',
    ]);
  }
});

it('matches the expansion oracle over generated directory trees and repeated source ownership', async () => {
  let state = 0x9e3779b9;
  const next = (bound: number): number => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) % bound;
  };
  const names = ['a', 'a-', 'a.', 'a0', 'b', 'z'];
  for (let trial = 0; trial < 256; trial += 1) {
    const path = (): string => Array.from({ length: next(4) }, () => names[next(names.length)]).join('/');
    const directories = Array.from({ length: 1 + next(32) }, path);
    const files = Array.from({ length: 1 + next(8) }, () =>
      posix.join(path(), next(2) === 0 ? 'Cargo.toml' : 'target.rs'),
    );
    await equivalent(directories, files);
  }
});

it('reduces a large sibling closure to the same few ordered filesets', async () => {
  const directories = Array.from({ length: 1024 }, (_, index) => `crates/package-${index}`);
  await equivalent(directories, ['shared.rs']);
});
