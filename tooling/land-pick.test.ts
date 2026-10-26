import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  changedLockEntries,
  configMeaning,
  dirtyMembers,
  lockEntries,
  type Member,
  owningMember,
  parseCargoMetadata,
  pick,
  previewRefusal,
  previewSeed,
  price,
  readUnitTimes,
  registryReach,
  testOnlyUnit,
  type UnitTime,
} from './land-pick.ts';

// The pick's decisions as plain calls (land-host.test.ts keeps the one real run through the script). The fixture is a
// three-member chain: app depends on core, core on os, and os reaches the registry package ring@0.17.14.

const os: Member = { name: 'os', dir: 'crates/os', dependents: ['core'], registry: new Set(['ring@0.17.14']) };
const core: Member = { name: 'core', dir: 'crates/core', dependents: ['app'], registry: new Set(['ring@0.17.14']) };
const app: Member = { name: 'app', dir: 'crates/app', dependents: [], registry: new Set(['ring@0.17.14']) };
const nested: Member = { name: 'nested', dir: 'crates/core/nested', dependents: [], registry: new Set() };
const MEMBERS = [os, core, app];
const unchangedConfig = (): boolean => false;
const changedConfig = (): boolean => true;

/** Each dirty member as 'package' or its sorted test-only units, for a readable comparison. */
function reach(paths: readonly string[], registry: readonly string[] = []): Record<string, string | string[]> {
  const dirty = dirtyMembers(MEMBERS, paths, new Set(registry), unchangedConfig);
  if (dirty.kind !== 'members') throw new Error(`expected members, got ${dirty.kind}`);
  return Object.fromEntries(
    [...dirty.members].map(([name, extent]) => [
      name,
      extent.kind === 'package' ? 'package' : [...extent.units].sort(),
    ]),
  );
}

describe('owningMember', () => {
  it('gives a path to the member with the deepest manifest directory', () => {
    expect(owningMember([core, nested], 'crates/core/nested/src/lib.rs')?.name).toBe('nested');
    expect(owningMember([core, nested], 'crates/core/src/lib.rs')?.name).toBe('core');
  });

  it('owns nothing outside every member, and a directory prefix is not containment', () => {
    expect(owningMember(MEMBERS, 'docs/readme.md')).toBeUndefined();
    expect(owningMember(MEMBERS, 'crates/core-extra/src/lib.rs')).toBeUndefined();
  });
});

describe('dirtyMembers', () => {
  it('dirties a changed library and every member above it', () => {
    expect(reach(['crates/os/src/lib.rs'])).toEqual({ os: 'package', core: 'package', app: 'package' });
  });

  it('dirties only the test targets of a member whose tests changed', () => {
    expect(reach(['crates/core/tests/rekey.rs', 'crates/core/benches/load/main.rs'])).toEqual({
      core: ['test-bench-load', 'test-integration-test-rekey'],
    });
    expect(reach(['crates/core/tests/common/mod.rs'])).toEqual({ core: ['test-integration-test-*'] });
  });

  it('upgrades a member reached by both a test change and a library change below it', () => {
    expect(reach(['crates/app/tests/smoke.rs', 'crates/core/src/lib.rs'])).toEqual({
      core: 'package',
      app: 'package',
    });
  });

  it('dirties each member reaching a registry package whose locked entry changed', () => {
    expect(reach(['Cargo.lock'], ['ring@0.17.14'])).toEqual({ os: 'package', core: 'package', app: 'package' });
  });

  it('ignores paths no member owns', () => {
    expect(reach(['docs/readme.md', 'packages/site/index.ts'])).toEqual({});
  });

  it('dirties every member when Cargo configuration changed in meaning', () => {
    expect(dirtyMembers(MEMBERS, ['.cargo/config.toml'], new Set(), changedConfig)).toEqual({
      kind: 'all',
      because: '.cargo/config.toml',
    });
    expect(dirtyMembers(MEMBERS, ['packages/x/.cargo/config.toml'], new Set(), changedConfig).kind).toBe('all');
    expect(dirtyMembers(MEMBERS, ['rust-toolchain.toml'], new Set(), changedConfig).kind).toBe('all');
  });

  it('keeps a configuration change that only touched comments out of the price', () => {
    expect(dirtyMembers(MEMBERS, ['.cargo/config.toml'], new Set(), unchangedConfig)).toEqual({
      kind: 'members',
      members: new Map(),
    });
  });

  it('dirties no member for a comment-only change to configuration a member owns', () => {
    const root: Member = { name: 'root', dir: '', dependents: [], registry: new Set() };
    expect(
      dirtyMembers([root, ...MEMBERS], ['Cargo.toml', 'crates/core/.cargo/config.toml'], new Set(), unchangedConfig),
    ).toEqual({ kind: 'members', members: new Map() });
  });
});

describe('registryReach', () => {
  const a = 'path+file:///w/crates/a#a@0.1.0';
  const b = 'path+file:///w/crates/b#b@0.1.0';
  const syn = 'registry+https://github.com/rust-lang/crates.io-index#syn@2.0.1';
  const quote = 'registry+https://github.com/rust-lang/crates.io-index#quote@1.0.40';

  it('gives both members of a dev-dependency cycle every registry package either reaches', () => {
    const depsOf = new Map([
      [a, [b, syn]],
      [b, [a, quote]],
      [syn, [quote]],
    ]);
    const reach = registryReach(new Set([a, b]), depsOf);
    expect([...(reach.get(a) ?? [])].sort()).toEqual(['quote@1.0.40', 'syn@2.0.1']);
    expect([...(reach.get(b) ?? [])].sort()).toEqual(['quote@1.0.40', 'syn@2.0.1']);
  });
});

describe('testOnlyUnit', () => {
  it('names the target a test-only file is, or every target of its kind for a shared file', () => {
    expect(testOnlyUnit('tests/rekey.rs')).toBe('test-integration-test-rekey');
    expect(testOnlyUnit('tests/rekey/main.rs')).toBe('test-integration-test-rekey');
    expect(testOnlyUnit('tests/rekey/helpers.rs')).toBe('test-integration-test-*');
    expect(testOnlyUnit('examples/demo.rs')).toBe('example-demo');
    expect(testOnlyUnit('tests/fixtures/data.json')).toBe('test-integration-test-*');
  });

  it('leaves a path outside the test-only directories to the library', () => {
    expect(testOnlyUnit('src/lib.rs')).toBeUndefined();
    expect(testOnlyUnit('tests')).toBeUndefined();
    expect(testOnlyUnit('constructor/x.rs')).toBeUndefined();
  });
});

describe('configMeaning', () => {
  it('drops blank lines and whole-line comments, and keeps every setting', () => {
    expect(configMeaning('# why\n[unstable]\n\n  # more\nmtime-on-use = true\n')).toBe(
      '[unstable]\nmtime-on-use = true',
    );
    expect(configMeaning(undefined)).toBeUndefined();
  });
});

describe('lock entries', () => {
  const before =
    '[[package]]\nname = "ring"\nversion = "0.17.14"\nsource = "registry"\n\n[[package]]\nname = "cc"\nversion = "1.0.0"\n';
  it('keys each entry by name@version', () => {
    expect([...lockEntries(before).keys()]).toEqual(['ring@0.17.14', 'cc@1.0.0']);
  });

  it('names the entries the gate tree has that the built tree lacked or locked otherwise', () => {
    const after = before
      .replace('version = "1.0.0"', 'version = "1.1.0"')
      .replace('"registry"', '"registry"\ndependencies = ["cc"]');
    expect([...changedLockEntries(lockEntries(before), lockEntries(after))].sort()).toEqual([
      'cc@1.1.0',
      'ring@0.17.14',
    ]);
    expect(changedLockEntries(lockEntries(before), lockEntries(before)).size).toBe(0);
  });
});

describe('price', () => {
  const times = new Map<string, readonly UnitTime[]>([
    [
      'core',
      [
        { unit: 'lib-core', seconds: 50 },
        { unit: 'test-lib-core', seconds: 80 },
        { unit: 'test-integration-test-rekey', seconds: 20 },
      ],
    ],
    ['app', [{ unit: 'bin-app', seconds: 5 }]],
  ]);

  it('sums every unit of a package-dirty member and only test units of a tests-dirty one', () => {
    const dirty = dirtyMembers(
      MEMBERS,
      ['crates/core/tests/rekey.rs', 'crates/app/src/main.rs'],
      new Set(),
      unchangedConfig,
    );
    expect(price(dirty, MEMBERS, times)).toEqual({ seconds: 25, units: 2, dirty: ['app', 'core'], unpriced: [] });
  });

  it('prices every unit of a kind for a shared test file, and nothing else of the member', () => {
    const dirty = dirtyMembers(MEMBERS, ['crates/core/tests/common/mod.rs'], new Set(), unchangedConfig);
    expect(price(dirty, MEMBERS, times)).toEqual({ seconds: 20, units: 1, dirty: ['core'], unpriced: [] });
  });

  it('names a dirty member no fingerprint priced instead of pricing it free', () => {
    const dirty = dirtyMembers(MEMBERS, ['crates/os/src/lib.rs'], new Set(), unchangedConfig);
    expect(price(dirty, MEMBERS, times)).toEqual({
      seconds: 155,
      units: 4,
      dirty: ['app', 'core', 'os'],
      unpriced: ['os'],
    });
  });

  it('prices every member when the configuration changed', () => {
    expect(price({ kind: 'all', because: 'Cargo.toml' }, MEMBERS, times).seconds).toBe(155);
  });
});

describe('pick', () => {
  const priced = (seconds: number, unpriced: readonly string[] = []) => ({ seconds, units: 1, dirty: [], unpriced });

  it('builds on the cheaper image, and keeps the own volume on a tie', () => {
    expect(pick(priced(100), priced(10), 4)).toEqual({ pick: 'seed', basis: 'cargo' });
    expect(pick(priced(10), priced(100), 4)).toEqual({ pick: 'own', basis: 'cargo' });
    expect(pick(priced(10), priced(10), 4)).toEqual({ pick: 'own', basis: 'cargo' });
  });

  it('lets space decide for main seed when no unit was timed', () => {
    expect(pick(priced(0), priced(0), 0)).toEqual({ pick: 'seed', basis: 'space' });
  });

  it('keeps the own volume when the seed leaves a member unpriced that the own volume does not', () => {
    expect(pick(priced(100), priced(10, ['os']), 4)).toEqual({ pick: 'own', basis: 'unpriced', members: ['os'] });
    expect(pick(priced(100, ['app']), priced(10, ['app', 'os']), 4)).toEqual({
      pick: 'own',
      basis: 'unpriced',
      members: ['os'],
    });
  });

  it('compares seconds when the seed leaves no member unpriced that the own volume prices', () => {
    expect(pick(priced(100, ['os']), priced(10, ['os']), 4)).toEqual({ pick: 'seed', basis: 'cargo' });
    expect(pick(priced(100, ['os']), priced(10), 4)).toEqual({ pick: 'seed', basis: 'cargo' });
  });
});

describe('previewSeed', () => {
  const SEED = 'a895a1ce6c80439484c0edd59a646063';
  it('reads the seed id of a reseed preview envelope', () => {
    expect(previewSeed(JSON.stringify({ ok: true, result: { seed: SEED, dryRun: true } }))).toBe(SEED);
  });

  it('refuses an envelope without a well-formed seed id', () => {
    expect(previewSeed('not json')).toBeUndefined();
    expect(previewSeed(JSON.stringify({ ok: false, error: { code: 'notFound' } }))).toBeUndefined();
    expect(previewSeed(JSON.stringify({ ok: true, result: { seed: '../x' } }))).toBeUndefined();
  });
});

describe('previewRefusal', () => {
  it('reads the error code of a refused preview, and nothing from an answer or other text', () => {
    expect(previewRefusal(JSON.stringify({ ok: false, error: { code: 'not-found', message: 'no seed' } }))).toBe(
      'not-found',
    );
    expect(previewRefusal(JSON.stringify({ ok: false, error: { code: 'conflict' } }))).toBe('conflict');
    expect(previewRefusal(JSON.stringify({ ok: true, result: { seed: 'a895a1ce6c80439484c0edd59a646063' } }))).toBe(
      undefined,
    );
    expect(previewRefusal('')).toBeUndefined();
    expect(previewRefusal(JSON.stringify({ ok: false, error: { code: 7 } }))).toBeUndefined();
  });
});

describe('parseCargoMetadata', () => {
  const document = {
    version: 1,
    workspace_root: '/w',
    workspace_members: ['a'],
    packages: [{ id: 'a', name: 'a', manifest_path: '/w/a/Cargo.toml', source: null }],
    resolve: { nodes: [{ id: 'a', deps: [{ name: 'b', pkg: 'b', dep_kinds: [] }] }], root: null },
    target_directory: '/w/target',
  } as const;
  it('reads the fields the pick uses and keeps what else the document holds', () => {
    expect(parseCargoMetadata(JSON.stringify(document))).toEqual(document);
  });

  it.each([
    ['text that is not JSON', 'cargo: not json'],
    ['no output', ''],
    ['another format version', JSON.stringify({ ...document, version: 2 })],
    ['no resolve graph', JSON.stringify({ ...document, resolve: null })],
    [
      'a package without a name',
      JSON.stringify({ ...document, packages: [{ id: 'a', manifest_path: '/w/a/Cargo.toml' }] }),
    ],
    ['a member list holding a number', JSON.stringify({ ...document, workspace_members: [1] })],
    [
      'a dependency without its package',
      JSON.stringify({ ...document, resolve: { nodes: [{ id: 'a', deps: [{ name: 'b' }] }] } }),
    ],
  ])('refuses %s', (_name, text) => {
    expect(parseCargoMetadata(text)).toBeUndefined();
  });
});
describe('readUnitTimes', () => {
  const T = 1_700_000_000;
  /** Writes a fingerprint generation's files, each at its own modification time in seconds. */
  function generation(checkout: string, name: string, files: Record<string, number>): void {
    for (const [file, seconds] of Object.entries(files)) {
      const path = join(checkout, 'target/debug/build/core', name, file);
      mkdirSync(join(path, '..'), { recursive: true });
      writeFileSync(path, '');
      utimesSync(path, seconds, seconds);
    }
  }

  it('times a compile from fingerprint/invoked.timestamp and a build-script run from run/invoked.timestamp', () => {
    const checkout = mkdtempSync(join(tmpdir(), 'land-pick-'));
    try {
      generation(checkout, 'compiled', {
        'fingerprint/invoked.timestamp': T,
        'fingerprint/lib-core.json': T + 12,
        'fingerprint/lib-core': T + 100,
      });
      generation(checkout, 'ran', {
        'run/invoked.timestamp': T + 50,
        'fingerprint/run-build-script-build-script-build.json': T + 53,
        'fingerprint/run-build-script-build-script-build': T + 60,
      });
      // Used last, but with no stamp it has no compile to time, so it neither prices nor displaces lib-core.
      generation(checkout, 'unstamped', { 'fingerprint/lib-core.json': T + 500, 'fingerprint/lib-core': T + 1000 });
      const read = readUnitTimes(checkout, [core]);
      if (read.kind !== 'read') throw new Error(`expected unit times, got ${read.kind}`);
      const times = [...(read.times.get('core') ?? [])].sort((a, b) => a.unit.localeCompare(b.unit));
      expect(times).toEqual([
        { unit: 'lib-core', seconds: 12 },
        { unit: 'run-build-script-build-script-build', seconds: 3 },
      ]);
    } finally {
      rmSync(checkout, { recursive: true, force: true });
    }
  });

  it('reads a member with no build directory as untimed, and any other unreadable directory as a failure', () => {
    const checkout = mkdtempSync(join(tmpdir(), 'land-pick-'));
    try {
      const absent = readUnitTimes(checkout, [core]);
      expect(absent.kind === 'read' ? [...absent.times] : absent).toEqual([]);
      mkdirSync(join(checkout, 'target/debug/build'), { recursive: true });
      writeFileSync(join(checkout, 'target/debug/build/core'), '');
      const unreadable = readUnitTimes(checkout, [core]);
      expect(unreadable.kind).toBe('unreadable');
      expect(unreadable.kind === 'unreadable' ? unreadable.path : '').toBe(join(checkout, 'target/debug/build/core'));
    } finally {
      rmSync(checkout, { recursive: true, force: true });
    }
  });
});
