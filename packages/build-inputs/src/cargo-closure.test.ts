import { describe, expect, it } from 'bun:test';
import {
  buildClosure,
  type CargoEdge,
  cargoLockResolve,
  localPackagesUnder,
  lockClosure,
  parseCargoMetadata,
} from './cargo-closure.js';

const REGISTRY = 'registry+https://github.com/rust-lang/crates.io-index';
const GIT = 'git+https://example.test/pinned?rev=1f2e3d#1f2e3d4c5b6a79881f2e3d4c5b6a79881f2e3d4c';

const edge = (pkg: string, devOnly = false): CargoEdge => ({ pkg, devOnly });

describe('buildClosure', () => {
  // app builds lib and tests with harness; lib builds serde and tests with proptest.
  const edges = new Map([
    ['app', [edge('lib'), edge('harness', true)]],
    ['lib', [edge('serde'), edge('proptest', true)]],
    ['harness', []],
    ['serde', []],
    ['proptest', []],
  ]);

  it('reaches every dependency kind of a selected package, and below it only what Cargo builds', () => {
    expect(buildClosure({ edges }, ['app'])).toEqual(
      new Map([
        ['app', ['lib', 'harness']],
        ['harness', []],
        ['lib', ['serde']],
        ['serde', []],
      ]),
    );
  });

  it('reaches no dev-dependency of a package that was only built', () => {
    expect(buildClosure({ edges }, [], ['app'])).toEqual(
      new Map([
        ['app', ['lib']],
        ['lib', ['serde']],
        ['serde', []],
      ]),
    );
  });

  it('refuses a resolve without edges', () => {
    expect(() => buildClosure({ edges: null }, ['app'])).toThrow('cargo metadata reported no dependency resolution');
  });
});

describe('localPackagesUnder', () => {
  it('selects the packages at or under the directory, and none beside it', () => {
    const local = ['/w/crates/a', '/w/crates/a/nested', '/w/crates/ab', '/w'].map((directory) => ({
      manifest: `${directory}/Cargo.toml`,
    }));
    expect(localPackagesUnder(local, '/w/crates/a').map((pkg) => pkg.manifest)).toEqual([
      '/w/crates/a/Cargo.toml',
      '/w/crates/a/nested/Cargo.toml',
    ]);
  });
});

describe('lockClosure', () => {
  // The document `cargo metadata --format-version 1` prints, trimmed to the fields read.
  const pkg = (id: string, name: string, version: string, source: string | null) => ({
    id,
    name,
    version,
    source,
    manifest_path: `/w/${name}/Cargo.toml`,
    targets: [],
  });
  const node = (id: string, deps: readonly [string, null | 'dev' | 'build'][]) => ({
    id,
    deps: deps.map(([dependency, kind]) => ({ pkg: dependency, dep_kinds: [{ kind }] })),
  });
  const metadata = JSON.stringify({
    packages: [
      pkg('path+file:///w/app#0.1.0', 'app', '0.1.0', null),
      pkg(`${REGISTRY}#serde@1.0.219`, 'serde', '1.0.219', REGISTRY),
      pkg(`${REGISTRY}#syn@1.0.109`, 'syn', '1.0.109', REGISTRY),
      pkg(`${REGISTRY}#syn@2.0.100`, 'syn', '2.0.100', REGISTRY),
      pkg('git+https://example.test/pinned?rev=1f2e3d#0.3.0', 'pinned', '0.3.0', GIT),
    ],
    resolve: {
      nodes: [
        node('path+file:///w/app#0.1.0', [
          [`${REGISTRY}#serde@1.0.219`, null],
          ['git+https://example.test/pinned?rev=1f2e3d#0.3.0', 'build'],
          [`${REGISTRY}#syn@1.0.109`, 'dev'],
        ]),
        node(`${REGISTRY}#serde@1.0.219`, [[`${REGISTRY}#syn@2.0.100`, null]]),
        node(`${REGISTRY}#syn@1.0.109`, []),
        node(`${REGISTRY}#syn@2.0.100`, []),
        node('git+https://example.test/pinned?rev=1f2e3d#0.3.0', []),
      ],
    },
    workspace_members: ['path+file:///w/app#0.1.0'],
    workspace_root: '/w',
  });

  it('spells each package of the closure as Cargo.lock names a dependency in full, a git one by its pinned commit', () => {
    const cargo = cargoLockResolve(parseCargoMetadata(metadata));
    expect(lockClosure(cargo, buildClosure(cargo, [], ['path+file:///w/app#0.1.0']))).toEqual({
      'app 0.1.0': [`pinned 0.3.0 (${GIT})`, `serde 1.0.219 (${REGISTRY})`],
      [`pinned 0.3.0 (${GIT})`]: [],
      [`serde 1.0.219 (${REGISTRY})`]: [`syn 2.0.100 (${REGISTRY})`],
      [`syn 2.0.100 (${REGISTRY})`]: [],
    });
  });
});
