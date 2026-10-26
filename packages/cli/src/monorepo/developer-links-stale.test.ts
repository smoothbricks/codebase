import { afterAll, afterEach, beforeAll, describe, expect, it, spyOn } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  rmSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  applyDeclaredLinks,
  decodeBuildInputRecord,
  planDeclaredLinks,
  selectedBuildInputProducer,
} from '../../../nx-plugin/managed/raw/tooling/direnv/developer-links.ts';

/**
 * A live developer link runs whatever its checkout last built. The managed script has no top-level effect, so the cases
 * import it and run its postinstall (plan the declared links, then apply them) here: a refusal is the error it throws.
 * The provider is a real git workspace whose build units carry fixed modification times.
 */
const HOST = `${process.platform}-${process.arch}`;
const OTHER_HOST = process.arch === 'arm64' ? `${process.platform}-x64` : `${process.platform}-arm64`;
const STORE = '.bun/linked-api@1.0.0/node_modules/linked-api';

const SOURCES = 1_700_000_000;
const BUILT = SOURCES + 100;
const LATER = BUILT + 100;

const created: string[] = [];
afterEach(() => {
  for (const directory of created.splice(0)) rmSync(directory, { recursive: true, force: true });
});

function write(file: string, content: string, seconds: number): void {
  mkdirSync(join(file, '..'), { recursive: true });
  writeFileSync(file, content);
  utimesSync(file, seconds, seconds);
}

function touch(file: string, seconds: number): void {
  utimesSync(file, seconds, seconds);
}

function git(root: string, ...args: string[]): void {
  const run = spawnSync('git', ['-C', root, ...args], { encoding: 'utf8' });
  if (run.status !== 0) throw new Error(`git ${args.join(' ')} failed: ${run.stderr}`);
}

interface Fixture {
  readonly source: string;
  readonly consumer: string;
  readonly alias: string;
}

/**
 * A CLI build for one platform as a package manifest declares it: a run without a configuration writes the developer
 * directory, which no `files` entry publishes, and `-c production` the shipping directory.
 */
const cliTarget = (platform: string) => ({
  outputs: ['{projectRoot}/{options.outputDirectory}/'],
  options: { outputDirectory: `.cache/bin/${platform}` },
  defaultConfiguration: 'development',
  configurations: { development: {}, production: { outputDirectory: `dist/ship/${platform}` } },
});

/**
 * A provider package with a TypeScript build (stamped by its build info) and a declared per-host native target, its
 * sources older than its outputs, and a consumer whose alias still links the store version, built under `base`.
 */
function build(base: string): void {
  const provider = join(base, 'provider');
  const source = join(provider, 'packages/api');
  write(
    join(provider, 'package.json'),
    JSON.stringify({ name: 'root', private: true, workspaces: ['packages/*'] }),
    SOURCES,
  );
  write(join(provider, '.gitignore'), 'dist\nnode_modules\n', SOURCES);
  write(
    join(source, 'package.json'),
    JSON.stringify({
      name: 'linked-api',
      version: '1.0.0',
      files: ['dist', 'src'],
      nx: {
        name: 'api',
        targets: {
          'native-host': {
            outputs: [`{projectRoot}/dist/bin/${HOST}`],
            inputs: ['{projectRoot}/**/*.rs', '!{projectRoot}/**/target/**', 'cargoToolchain'],
          },
          [`cli-${HOST}`]: cliTarget(HOST),
          [`cli-${OTHER_HOST}`]: cliTarget(OTHER_HOST),
        },
      },
    }),
    SOURCES,
  );
  write(
    join(source, 'tsconfig.lib.json'),
    JSON.stringify({
      compilerOptions: { outDir: 'dist/js', tsBuildInfoFile: 'dist/js/lib.tsbuildinfo' },
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.test.ts'],
    }),
    SOURCES,
  );
  write(join(source, 'src/index.ts'), 'export const value = 1;\n', SOURCES);
  write(join(source, 'src/index.test.ts'), 'export {};\n', SOURCES);
  write(join(source, 'README.md'), '# api\n', SOURCES);
  write(join(source, 'skills/SKILL.md'), '# api skill\n', SOURCES);
  write(join(source, 'crates/api/src/lib.rs'), 'pub fn value() -> u8 { 1 }\n', SOURCES);
  write(join(source, 'crates/addon/src/lib.rs'), 'pub fn addon() -> u8 { 1 }\n', SOURCES);
  git(provider, 'init', '-q');
  git(provider, 'add', '-A');
  git(
    provider,
    '-c',
    'user.name=fixture',
    '-c',
    'user.email=fixture@example.test',
    '-c',
    'maintenance.auto=false',
    'commit',
    '-qm',
    'fixture',
  );
  write(join(source, 'dist/js/index.js'), 'export const value = 1;\n', BUILT);
  // A leftover build info no build rewrites (the emit is not incremental): older than the sources, and never judged.
  write(join(source, 'dist/js/lib.tsbuildinfo'), '{}', SOURCES - 100);
  write(join(source, `dist/bin/${HOST}/api`), 'native', BUILT);
  // Built for another host long ago: nothing here runs it, so it is never judged.
  write(join(source, `dist/bin/${OTHER_HOST}/api`), 'native', SOURCES - 100);

  const consumer = join(base, 'consumer');
  write(
    join(consumer, 'package.json'),
    JSON.stringify({
      name: 'consumer',
      private: true,
      smoo: { developerLinks: { 'linked-api': '../provider/packages/api' } },
    }),
    SOURCES,
  );
  write(
    join(consumer, 'node_modules', STORE, 'package.json'),
    JSON.stringify({ name: 'linked-api', version: '1.0.0' }),
    SOURCES,
  );
  const alias = join(consumer, 'node_modules/linked-api');
  symlinkSync(STORE, alias);
}

/** The checkout every fixture starts from, built once: each case changes a copy of it. */
let template: string | undefined;

beforeAll(() => {
  template = realpathSync(mkdtempSync(join(tmpdir(), 'developer-links-stale-template-')));
  build(template);
});

afterAll(() => {
  if (template !== undefined) rmSync(template, { recursive: true, force: true });
});

function fixture(): Fixture {
  if (template === undefined) throw new Error('the checkout template is built before the first case runs');
  const base = realpathSync(mkdtempSync(join(tmpdir(), 'developer-links-stale-')));
  created.push(base);
  // Copied with their modification times: every case is a judgment of them.
  for (const part of ['provider', 'consumer']) {
    cpSync(join(template, part), join(base, part), {
      recursive: true,
      verbatimSymlinks: true,
      preserveTimestamps: true,
    });
  }
  const consumer = join(base, 'consumer');
  return { source: join(base, 'provider/packages/api'), consumer, alias: join(consumer, 'node_modules/linked-api') };
}

/** What the root postinstall says on stderr as it links `fixture`'s declaration: its plan, then its application. */
async function link({ consumer }: Fixture): Promise<string> {
  const lines: string[] = [];
  const error = spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    lines.push(args.map(String).join(' '));
  });
  try {
    applyDeclaredLinks(consumer, await planDeclaredLinks(consumer));
    return lines.join('\n');
  } finally {
    error.mockRestore();
  }
}

/** The refusal the postinstall dies of, which is the error `planDeclaredLinks` throws; linking instead fails the case. */
async function refusal(fixture: Fixture): Promise<string> {
  const failure: unknown = await link(fixture).then(
    () => undefined,
    (error: unknown) => error,
  );
  if (!(failure instanceof Error)) throw new Error('the postinstall linked a checkout it was expected to refuse');
  return failure.message;
}

const iso = (seconds: number) => new Date(seconds * 1000).toISOString();

describe('a live developer link whose build is older than its sources', () => {
  it('links a checkout whose every build unit is newer than its inputs', async () => {
    const fresh = fixture();
    expect(await link(fresh)).not.toContain('refusing');
    expect(realpathSync(fresh.alias)).toBe(fresh.source);
  });

  it('refuses when a TypeScript source is newer than the build info, naming both times and the cached rebuild', async () => {
    const stale = fixture();
    touch(join(stale.source, 'src/index.ts'), LATER);
    const refused = await refusal(stale);
    expect(refused).toContain(
      `packages/api/tsconfig.lib.json (outDir dist/js) was built at ${iso(BUILT)} (packages/api/dist/js/index.js), but packages/api/src/index.ts changed at ${iso(LATER)}`,
    );
    expect(refused).toContain('nx run api:build');
    expect(refused).not.toContain('--skip-nx-cache');
    // Without a record, the refusal says what modification time cannot tell apart.
    expect(refused).toContain(
      'no .build-inputs.json, so judged by modification time, where a file rewritten with its own bytes also counts',
    );
    // Refused, not fallen back: the alias is exactly what it was.
    expect(readlinkSync(stale.alias)).toBe(STORE);
  });

  it('refuses when a declared target input is newer than that target output, and only for that unit', async () => {
    const stale = fixture();
    touch(join(stale.source, 'crates/api/src/lib.rs'), LATER);
    const refused = await refusal(stale);
    expect(refused).toContain(
      `nx target native-host was built at ${iso(BUILT)} (packages/api/dist/bin/${HOST}/api), but packages/api/crates/api/src/lib.rs changed at ${iso(LATER)}`,
    );
    expect(refused).not.toContain('tsconfig.lib.json (outDir');
    expect(readlinkSync(stale.alias)).toBe(STORE);
  });

  it('refuses a published build that was never built', async () => {
    const unbuilt = fixture();
    rmSync(join(unbuilt.source, 'dist/js'), { recursive: true });
    expect(await refusal(unbuilt)).toContain('packages/api/tsconfig.lib.json (outDir dist/js) was never built');
    expect(readlinkSync(unbuilt.alias)).toBe(STORE);
  });

  it('ignores documentation, tests, the manifest and builds for another host', async () => {
    const touched = fixture();
    for (const file of ['README.md', 'src/index.test.ts', 'package.json']) touch(join(touched.source, file), LATER);
    expect(await link(touched)).not.toContain('refusing');
    expect(realpathSync(touched.alias)).toBe(touched.source);
  });

  it('refuses a checkout outside Git that declares a build, and links one that declares none', async () => {
    const untracked = fixture();
    rmSync(join(untracked.source, '../../.git'), { recursive: true });
    expect(await refusal(untracked)).toContain(`cannot read the build state of ${untracked.source}`);
    expect(readlinkSync(untracked.alias)).toBe(STORE);
    for (const file of ['tsconfig.lib.json', 'package.json']) rmSync(join(untracked.source, file));
    write(join(untracked.source, 'package.json'), JSON.stringify({ name: 'linked-api', version: '1.0.0' }), SOURCES);
    await link(untracked);
    expect(realpathSync(untracked.alias)).toBe(untracked.source);
  });

  it('links again once the rebuild rewrote the outputs', async () => {
    const rebuilt = fixture();
    touch(join(rebuilt.source, 'src/index.ts'), LATER);
    await refusal(rebuilt);
    touch(join(rebuilt.source, 'dist/js/index.js'), LATER + 1);
    await link(rebuilt);
    expect(realpathSync(rebuilt.alias)).toBe(rebuilt.source);
  });
});

const sha256 = (file: string) => createHash('sha256').update(readFileSync(file)).digest('hex');

/** The record a producer writes (nx-plugin build-inputs.ts), here for the TypeScript emit and the native build. */
function record(fixture: Fixture): void {
  const provider = join(fixture.source, '../..');
  const files = (paths: readonly string[]) =>
    Object.fromEntries(paths.map((file) => [file, sha256(join(provider, file))]));
  writeFileSync(
    join(fixture.source, 'dist/js/.build-inputs.json'),
    JSON.stringify({
      version: 1,
      files: files(['packages/api/src/index.ts', 'packages/api/tsconfig.lib.json']),
      complete: [':(glob)packages/api/src/**/*.ts', ':(exclude,glob)packages/api/src/**/*.test.ts'],
    }),
  );
  writeFileSync(
    join(fixture.source, `dist/bin/${HOST}/.build-inputs.json`),
    JSON.stringify({
      version: 1,
      producer: { project: 'api', target: 'native-host' },
      files: files(['packages/api/crates/api/src/lib.rs']),
    }),
  );
}

describe('a live developer link whose build recorded its inputs', () => {
  it('links when sources were rewritten with the bytes the build read', async () => {
    const touched = fixture();
    record(touched);
    for (const file of ['src/index.ts', 'tsconfig.lib.json', 'crates/api/src/lib.rs'])
      touch(join(touched.source, file), LATER);
    expect(await link(touched)).not.toContain('refusing');
    expect(realpathSync(touched.alias)).toBe(touched.source);
  });

  it('refuses an input with other bytes, naming it and the cached rebuild, and only for its unit', async () => {
    const changed = fixture();
    record(changed);
    write(join(changed.source, 'crates/api/src/lib.rs'), 'pub fn value() -> u8 { 2 }\n', BUILT - 50);
    const refused = await refusal(changed);
    expect(refused).toContain(
      `packages/api/dist/bin/${HOST} is stale: input packages/api/crates/api/src/lib.rs has other bytes than the build read`,
    );
    expect(refused).not.toContain('packages/api/dist/js is stale');
    expect(refused).toContain('nx run api:native-host');
    expect(readlinkSync(changed.alias)).toBe(STORE);
  });

  it('refuses a source added under the recorded pathspecs and one removed since the build', async () => {
    const added = fixture();
    record(added);
    write(join(added.source, 'src/extra.ts'), 'export {};\n', SOURCES);
    write(join(added.source, 'src/extra.test.ts'), 'export {};\n', SOURCES);
    expect(await refusal(added)).toContain(
      'packages/api/dist/js is stale: input packages/api/src/extra.ts was added since the build',
    );
    expect(readlinkSync(added.alias)).toBe(STORE);

    const removed = fixture();
    record(removed);
    rmSync(join(removed.source, 'src/index.ts'));
    expect(await refusal(removed)).toContain(
      'packages/api/dist/js is stale: input packages/api/src/index.ts was removed since the build',
    );

    // Deleted before the build (still in Git's index): no build read it, so it is neither recorded nor added.
    const deleted = fixture();
    rmSync(join(deleted.source, 'src/index.ts'));
    writeFileSync(
      join(deleted.source, 'dist/js/.build-inputs.json'),
      JSON.stringify({
        version: 1,
        files: { 'packages/api/tsconfig.lib.json': sha256(join(deleted.source, 'tsconfig.lib.json')) },
        complete: [':(glob)packages/api/src/**/*.ts', ':(exclude,glob)packages/api/src/**/*.test.ts'],
      }),
    );
    expect(await link(deleted)).not.toContain('refusing');
  });

  it('links again once a build restores outputs whose record names the current bytes', async () => {
    const restored = fixture();
    record(restored);
    write(join(restored.source, 'src/index.ts'), 'export const value = 2;\n', LATER);
    await refusal(restored);
    // What a cache hit does: the outputs and their record come back together, older than the edit.
    record(restored);
    touch(join(restored.source, 'dist/js/.build-inputs.json'), BUILT);
    expect(await link(restored)).not.toContain('refusing');
    expect(realpathSync(restored.alias)).toBe(restored.source);
  });

  it('refuses a record it cannot read rather than trusting it', async () => {
    const broken = fixture();
    record(broken);
    writeFileSync(join(broken.source, 'dist/js/.build-inputs.json'), JSON.stringify({ version: 2, files: {} }));
    expect(await refusal(broken)).toContain('dist/js/.build-inputs.json is not a version 1 build input record');
    expect(readlinkSync(broken.alias)).toBe(STORE);
  });
});

/** Actual default tasks leave the CLI and inferred addon's receipts beside their source-owned development outputs. */
function recordDevelopment(fixture: Fixture): void {
  write(join(fixture.source, `.cache/bin/${HOST}/api`), 'native', BUILT);
  writeFileSync(
    join(fixture.source, `.cache/bin/${HOST}/.build-inputs.json`),
    JSON.stringify({
      version: 1,
      producer: { project: 'api', target: `cli-${HOST}`, configuration: 'development' },
      files: { 'packages/api/skills/SKILL.md': sha256(join(fixture.source, 'skills/SKILL.md')) },
    }),
  );
  write(join(fixture.source, `.cache/native-dev/${HOST}/api.node`), 'addon', BUILT);
  writeFileSync(
    join(fixture.source, `.cache/native-dev/${HOST}/.build-inputs.json`),
    JSON.stringify({
      version: 1,
      producer: { project: 'api', target: `napi-${HOST}` },
      files: { 'packages/api/crates/addon/src/lib.rs': sha256(join(fixture.source, 'crates/addon/src/lib.rs')) },
    }),
  );
}

const INVALID_PRODUCERS: readonly unknown[] = [
  null,
  'api:native',
  [],
  { target: 'native' },
  { project: 'api' },
  { project: '', target: 'native' },
  { project: 'api', target: '' },
  { project: 'api', target: 1 },
  { project: 'api', target: 'native', configuration: '' },
  { project: 'api', target: 'native', configuration: 1 },
];

describe('a live developer link whose developer build lies outside the published files', () => {
  it('judges the record beside each output the checkout runs, by content, and none of a build it does not run', async () => {
    const dev = fixture();
    const skill = join(dev.source, 'skills/SKILL.md');
    const original = readFileSync(skill, 'utf8');
    const addon = join(dev.source, 'crates/addon/src/lib.rs');
    const originalAddon = readFileSync(addon, 'utf8');
    record(dev);
    recordDevelopment(dev);

    // Current: both sources are newer than the outputs but retain the bytes the actual tasks read.
    for (const file of [skill, addon]) touch(file, LATER);
    expect((await planDeclaredLinks(dev.consumer)).changes.map((change) => change.path)).toEqual([
      'node_modules/linked-api',
    ]);

    // Both declared CLI and inferred addon refuse by content, naming their actual tasks, not the tsc-only build.
    write(skill, '# api skill, edited\n', LATER);
    write(addon, 'pub fn addon() -> u8 { 2 }\n', LATER);
    const refused = await refusal(dev);
    expect(refused).toContain(
      `packages/api/.cache/bin/${HOST} is stale: input packages/api/skills/SKILL.md has other bytes than the build read`,
    );
    expect(refused).toContain(
      `packages/api/.cache/native-dev/${HOST} is stale: input packages/api/crates/addon/src/lib.rs has other bytes than the build read`,
    );
    expect(refused).toContain(`nx run api:cli-${HOST}:development`);
    expect(refused).toContain(`nx run api:napi-${HOST}`);
    expect(refused).not.toContain('nx run api:build');
    expect(refused).not.toContain('modification time');
    expect(readlinkSync(dev.alias)).toBe(STORE);

    // Restored: the bytes the build read are current again with no rebuild, though the file is newer than the binary.
    write(skill, original, LATER + 1);
    write(addon, originalAddon, LATER + 1);
    expect(await link(dev)).not.toContain('refusing');
    expect(realpathSync(dev.alias)).toBe(dev.source);

    // Foreign hosts, projects, nondefault/production tasks and obsolete native records do not own this checkout's
    // outputs. Their stale surrounding trees must not reappear as timestamp-fallback units either.
    const staleInputs = { 'packages/api/skills/SKILL.md': '0'.repeat(64) };
    const outdated = JSON.stringify({ version: 1, files: staleInputs });
    const ignored = [
      { output: `.cache/bin/${OTHER_HOST}`, producer: { project: 'api', target: `cli-${OTHER_HOST}` } },
      {
        output: `.cache/production/${HOST}`,
        producer: { project: 'api', target: `napi-${HOST}`, configuration: 'production' },
      },
      { output: `.cache/foreign/${HOST}`, producer: { project: 'foreign', target: `napi-${HOST}` } },
      { output: `dist/ship/${HOST}`, producer: { project: 'api', target: `cli-${HOST}`, configuration: 'production' } },
      { output: `dist/foreign/${HOST}`, producer: { project: 'foreign', target: `napi-${HOST}` } },
      { output: `dist/native/${HOST}`, producer: undefined },
    ];
    for (const { output, producer } of ignored) {
      write(
        join(dev.source, output, '.build-inputs.json'),
        JSON.stringify({ version: 1, producer, files: staleInputs }),
        SOURCES - 100,
      );
      write(join(dev.source, output, 'api.node'), 'old native', SOURCES - 100);
    }
    // A scan may not enter build-state directories or follow a link, even if they contain unreadable receipts.
    for (const excluded of ['target', 'node_modules'])
      write(join(dev.source, '.cache', excluded, '.build-inputs.json'), 'not JSON', BUILT);
    write(join(dev.source, 'external-state/.build-inputs.json'), 'not JSON', BUILT);
    symlinkSync('../external-state', join(dev.source, '.cache/redirect'), 'dir');
    expect(await link(dev)).not.toContain('refusing');

    // The standalone TypeScript receipt has no producer and still owns the nested emit independently of an old dist
    // receipt. The same boundary also preserves the missing-record timestamp fallback.
    write(join(dev.source, 'dist/.build-inputs.json'), outdated, BUILT);
    expect(await link(dev)).not.toContain('refusing');
    write(join(dev.source, 'src/index.ts'), 'export const value = 2;\n', LATER);
    const emitted = await refusal(dev);
    expect(emitted).toContain(
      'packages/api/dist/js is stale: input packages/api/src/index.ts has other bytes than the build read',
    );
    expect(emitted).toContain('nx run api:build');
    expect(emitted).not.toContain('packages/api/dist is stale');
    rmSync(join(dev.source, 'dist/js/.build-inputs.json'));
    const fallback = await refusal(dev);
    expect(fallback).toContain('tsconfig.lib.json (outDir dist/js) was built at');
    expect(fallback).toContain('no .build-inputs.json, so judged by modification time');
    expect(fallback).not.toContain('packages/api/dist is stale');

    // A present malformed identity is corrupt data, not an unselected producer that can hide its stale tree.
    writeFileSync(
      join(dev.source, '.cache/bin', HOST, '.build-inputs.json'),
      JSON.stringify({
        version: 1,
        files: {},
        producer: { project: 'api', target: 1 },
      }),
    );
    await expect(planDeclaredLinks(dev.consumer)).rejects.toThrow('is not a version 1 build input record: producer');
  });
});

interface ProducerCase {
  readonly label: string;
  readonly producer: Parameters<typeof selectedBuildInputProducer>[0];
  readonly targets?: unknown;
  readonly expected?: NonNullable<ReturnType<typeof selectedBuildInputProducer>>;
}

const PRODUCER_CASES: readonly ProducerCase[] = [
  {
    label: 'a declared default task',
    producer: { project: 'api', target: 'cli-host' },
    targets: { 'cli-host': {} },
    expected: { project: 'api', target: 'cli-host' },
  },
  {
    label: 'an inferred task absent from the manifest',
    producer: { project: 'api', target: 'napi-host' },
    expected: { project: 'api', target: 'napi-host' },
  },
  {
    label: 'the manifest target names its default configuration',
    producer: { project: 'api', target: 'native', configuration: 'development' },
    targets: { native: { defaultConfiguration: 'development' } },
    expected: { project: 'api', target: 'native', configuration: 'development' },
  },
  {
    label: 'an unconfigured receipt remains selected with a named default',
    producer: { project: 'api', target: 'native' },
    targets: { native: { defaultConfiguration: 'development' } },
    expected: { project: 'api', target: 'native' },
  },
  { label: 'another project', producer: { project: 'foreign', target: 'native' } },
  { label: 'a production task', producer: { project: 'api', target: 'native', configuration: 'production' } },
  {
    label: 'production is never a developer configuration',
    producer: { project: 'api', target: 'native', configuration: 'production' },
    targets: { native: { defaultConfiguration: 'production' } },
  },
  { label: 'a nondefault named configuration', producer: { project: 'api', target: 'native', configuration: 'debug' } },
  {
    label: 'a different named configuration than the default',
    producer: { project: 'api', target: 'native', configuration: 'debug' },
    targets: { native: { defaultConfiguration: 'development' } },
  },
  { label: 'an obsolete native receipt without provenance', producer: undefined },
];

describe('the actual producer of a developer build receipt', () => {
  it('decodes absent provenance and rejects malformed present provenance without I/O', () => {
    expect(decodeBuildInputRecord({ version: 1, files: {} }, 'record').producer).toBeUndefined();
    for (const producer of INVALID_PRODUCERS) {
      expect(() => decodeBuildInputRecord({ version: 1, files: {}, producer }, 'record')).toThrow(
        'record is not a version 1 build input record: producer',
      );
    }
  });
  it('selects only this project and its default development task without reading files or starting Nx', () => {
    for (const { label, producer, targets, expected } of PRODUCER_CASES) {
      expect({ label, producer: selectedBuildInputProducer(producer, 'api', targets) }).toEqual({
        label,
        producer: expected,
      });
    }
  });
});
