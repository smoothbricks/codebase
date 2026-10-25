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
  planDeclaredLinks,
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
  write(join(source, 'crates/api/src/lib.rs'), 'pub fn value() -> u8 { 1 }\n', SOURCES);
  git(provider, 'init', '-q');
  git(provider, 'add', '-A');
  git(provider, '-c', 'user.name=fixture', '-c', 'user.email=fixture@example.test', 'commit', '-qm', 'fixture');
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
    JSON.stringify({ version: 1, files: files(['packages/api/crates/api/src/lib.rs']) }),
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
    expect(refused).toContain('nx run api:build');
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
