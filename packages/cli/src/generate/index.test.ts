import { describe, expect, it } from 'bun:test';

import { variants } from './index.js';

describe('package generator variants', () => {
  it('routes TypeScript and Rust scaffolds through the convention-owned generator', () => {
    expect(variants['ts-lib']).toMatchObject({
      generator: 'create-package',
      description: 'Create a TypeScript library package',
    });
    expect(variants['ts-lib']?.args('duration')).toEqual(['--name', 'duration', '--variant', 'ts-lib']);

    expect(variants['rust-crate']).toMatchObject({
      generator: 'create-package',
      description: 'Create a Rust workspace package',
    });
    expect(variants['rust-crate']?.args('ferris')).toEqual(['--name', 'ferris', '--variant', 'rust-crate']);
    expect(variants['rust-crate']?.options).toEqual([
      { flag: '--wasm', description: 'add wasm-bindgen web and Node.js output families' },
    ]);
  });
});
