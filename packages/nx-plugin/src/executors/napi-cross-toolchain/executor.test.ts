import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { prewarmCrossToolchain } from './executor.js';

// The pins that make extraction possible live on the consumer, so the real
// extraction runs against that project: `cowshed` declares the
// `@napi-rs/cross-toolchain-*-target-*` optionalDependencies its cross builds
// need. aarch64 is the one triple pinned for both an arm64 and an x64 host, so
// this covers developer machines and CI with the same archive.
const consumerProjectRoot = join(import.meta.dir, '../../../../cowshed');
const triple = 'aarch64-unknown-linux-gnu';
const temporaryDirs: string[] = [];

function temporaryDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  temporaryDirs.push(dir);
  return dir;
}

let firstHome = '';
let firstOutcome = '';
let version = '';

beforeAll(() => {
  firstHome = temporaryDir('napi-cross-toolchain-');
  firstOutcome = prewarmCrossToolchain({
    projectRoot: consumerProjectRoot,
    triple,
    platform: 'linux',
    home: firstHome,
  });
  // The directory napi probes is keyed by the resolved @napi-rs/cross-toolchain
  // version, so read it back instead of restating the pinned version here.
  const versions = readdirSync(join(firstHome, '.napi-rs', 'cross-toolchain'));
  expect(versions).toHaveLength(1);
  version = versions[0] ?? '';
});

afterAll(() => {
  for (const dir of temporaryDirs) {
    rmSync(dir, { force: true, recursive: true });
  }
});

describe('napi-cross-toolchain executor', () => {
  it('extracts the pinned toolchain once and then reports it ready', () => {
    expect(firstOutcome).toBe('extracted');

    // napi derives the compiler, sysroot and PATH entries from this layout and
    // skips extraction on the package.json marker alone: a marker without the
    // toolchain turns every later build into a missing-linker failure.
    const toolchain = join(firstHome, '.napi-rs', 'cross-toolchain', version, triple);
    expect(existsSync(join(toolchain, 'package.json'))).toBe(true);
    expect(existsSync(join(toolchain, 'bin', `${triple}-gcc`))).toBe(true);
    expect(existsSync(join(toolchain, triple, 'sysroot'))).toBe(true);

    expect(
      prewarmCrossToolchain({ projectRoot: consumerProjectRoot, triple, platform: 'linux', home: firstHome }),
    ).toBe('ready');
  });

  it('lands the toolchain in the empty directory napi creates before probing', () => {
    const home = temporaryDir('napi-cross-toolchain-');
    const destination = join(home, '.napi-rs', 'cross-toolchain', version, triple);
    mkdirSync(destination, { recursive: true });

    expect(prewarmCrossToolchain({ projectRoot: consumerProjectRoot, triple, platform: 'linux', home })).toBe(
      'extracted',
    );
    expect(existsSync(join(destination, 'package.json'))).toBe(true);
  });

  it('leaves hosts that cannot run the toolchain to the build itself', () => {
    expect(
      prewarmCrossToolchain({
        projectRoot: consumerProjectRoot,
        triple,
        platform: 'darwin',
        home: temporaryDir('napi-cross-toolchain-'),
      }),
    ).toBe('unsupported-host');
  });

  it('rejects triples that have no cross toolchain package', () => {
    expect(() =>
      prewarmCrossToolchain({
        projectRoot: consumerProjectRoot,
        triple: 'armv7-unknown-linux-gnueabihf',
        platform: 'linux',
        home: temporaryDir('napi-cross-toolchain-'),
      }),
    ).toThrow('Unsupported --use-napi-cross triple armv7-unknown-linux-gnueabihf');
  });

  it('names the optional dependency a project is missing', () => {
    const projectRoot = temporaryDir('napi-cross-consumer-');
    writeFileSync(join(projectRoot, 'package.json'), '{"name":"unpinned"}\n');

    expect(() =>
      prewarmCrossToolchain({
        projectRoot,
        triple,
        platform: 'linux',
        hostArch: 'x64',
        home: temporaryDir('napi-cross-toolchain-'),
      }),
    ).toThrow('@napi-rs/cross-toolchain-x64-target-aarch64 is not installed');
  });
});
