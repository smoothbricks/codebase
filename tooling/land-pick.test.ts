import { describe, expect, it } from 'bun:test';
import {
  changedLockEntries,
  configMeaning,
  dirtyMembers,
  lockEntries,
  type Member,
  owningMember,
  pick,
  previewSeed,
  price,
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
  const priced = (seconds: number) => ({ seconds, units: 1, dirty: [], unpriced: [] });

  it('builds on the cheaper image, and keeps the own volume on a tie', () => {
    expect(pick(priced(100), priced(10), 4)).toEqual({ pick: 'seed', basis: 'cargo' });
    expect(pick(priced(10), priced(100), 4)).toEqual({ pick: 'own', basis: 'cargo' });
    expect(pick(priced(10), priced(10), 4)).toEqual({ pick: 'own', basis: 'cargo' });
  });

  it('lets space decide for main seed when no unit was timed', () => {
    expect(pick(priced(0), priced(0), 0)).toEqual({ pick: 'seed', basis: 'space' });
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
