import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
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
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';

/**
 * A live developer link runs whatever its checkout last built. The managed script is loaded the way shell entry loads
 * it, in a plain `bun -e` child; the provider is a real git workspace whose build units carry fixed modification times.
 */
const LINKS = join(managedAssetsRoot, 'raw/tooling/direnv/developer-links.ts');
const HOST = `${process.platform}-${process.arch}`;
const OTHER_HOST = process.arch === 'arm64' ? `${process.platform}-x64` : `${process.platform}-arm64`;
const STORE = '.bun/linked-api@1.0.0/node_modules/linked-api';

const PLAN = `
const links = await import(Bun.argv[1]);
const root = Bun.argv[2];
links.applyDeclaredLinks(root, await links.planDeclaredLinks(root));
`;

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
 * sources older than its outputs, and a consumer whose alias still links the store version.
 */
function fixture(): Fixture {
  const base = realpathSync(mkdtempSync(join(tmpdir(), 'developer-links-stale-')));
  created.push(base);
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
  return { source, consumer, alias };
}

function plan({ consumer }: Fixture) {
  const run = spawnSync(process.execPath, ['-e', PLAN, LINKS, consumer], { encoding: 'utf8' });
  return { status: run.status, stderr: run.stderr };
}

const iso = (seconds: number) => new Date(seconds * 1000).toISOString();

describe('a live developer link whose build is older than its sources', () => {
  it('links a checkout whose every build unit is newer than its inputs', () => {
    const fresh = fixture();
    const result = plan(fresh);
    expect(result.stderr).not.toContain('refusing');
    expect(result.status).toBe(0);
    expect(realpathSync(fresh.alias)).toBe(fresh.source);
  });

  it('refuses when a TypeScript source is newer than the build info, naming both times and the cached rebuild', () => {
    const stale = fixture();
    touch(join(stale.source, 'src/index.ts'), LATER);
    const result = plan(stale);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain(
      `packages/api/tsconfig.lib.json (outDir dist/js) was built at ${iso(BUILT)} (packages/api/dist/js/index.js), but packages/api/src/index.ts changed at ${iso(LATER)}`,
    );
    expect(result.stderr).toContain('nx run api:build');
    expect(result.stderr).not.toContain('--skip-nx-cache');
    // Without a record, the refusal says what modification time cannot tell apart.
    expect(result.stderr).toContain(
      'no .build-inputs.json, so judged by modification time, where a file rewritten with its own bytes also counts',
    );
    // Refused, not fallen back: the alias is exactly what it was.
    expect(readlinkSync(stale.alias)).toBe(STORE);
  });

  it('refuses when a declared target input is newer than that target output, and only for that unit', () => {
    const stale = fixture();
    touch(join(stale.source, 'crates/api/src/lib.rs'), LATER);
    const result = plan(stale);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain(
      `nx target native-host was built at ${iso(BUILT)} (packages/api/dist/bin/${HOST}/api), but packages/api/crates/api/src/lib.rs changed at ${iso(LATER)}`,
    );
    expect(result.stderr).not.toContain('tsconfig.lib.json (outDir');
    expect(readlinkSync(stale.alias)).toBe(STORE);
  });

  it('refuses a published build that was never built', () => {
    const unbuilt = fixture();
    rmSync(join(unbuilt.source, 'dist/js'), { recursive: true });
    const result = plan(unbuilt);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('packages/api/tsconfig.lib.json (outDir dist/js) was never built');
    expect(readlinkSync(unbuilt.alias)).toBe(STORE);
  });

  it('ignores documentation, tests, the manifest and builds for another host', () => {
    const touched = fixture();
    for (const file of ['README.md', 'src/index.test.ts', 'package.json']) touch(join(touched.source, file), LATER);
    const result = plan(touched);
    expect(result.stderr).not.toContain('refusing');
    expect(result.status).toBe(0);
    expect(realpathSync(touched.alias)).toBe(touched.source);
  });

  it('refuses a checkout outside Git that declares a build, and links one that declares none', () => {
    const untracked = fixture();
    rmSync(join(untracked.source, '../../.git'), { recursive: true });
    const refused = plan(untracked);
    expect(refused.status).not.toBe(0);
    expect(refused.stderr).toContain(`cannot read the build state of ${untracked.source}`);
    expect(readlinkSync(untracked.alias)).toBe(STORE);
    for (const file of ['tsconfig.lib.json', 'package.json']) rmSync(join(untracked.source, file));
    write(join(untracked.source, 'package.json'), JSON.stringify({ name: 'linked-api', version: '1.0.0' }), SOURCES);
    const linked = plan(untracked);
    expect(linked.status).toBe(0);
    expect(realpathSync(untracked.alias)).toBe(untracked.source);
  });

  it('links again once the rebuild rewrote the outputs', () => {
    const rebuilt = fixture();
    touch(join(rebuilt.source, 'src/index.ts'), LATER);
    expect(plan(rebuilt).status).not.toBe(0);
    touch(join(rebuilt.source, 'dist/js/index.js'), LATER + 1);
    const result = plan(rebuilt);
    expect(result.status).toBe(0);
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
  it('links when sources were rewritten with the bytes the build read', () => {
    const touched = fixture();
    record(touched);
    for (const file of ['src/index.ts', 'tsconfig.lib.json', 'crates/api/src/lib.rs'])
      touch(join(touched.source, file), LATER);
    const result = plan(touched);
    expect(result.stderr).not.toContain('refusing');
    expect(result.status).toBe(0);
    expect(realpathSync(touched.alias)).toBe(touched.source);
  });

  it('refuses an input with other bytes, naming it and the cached rebuild, and only for its unit', () => {
    const changed = fixture();
    record(changed);
    write(join(changed.source, 'crates/api/src/lib.rs'), 'pub fn value() -> u8 { 2 }\n', BUILT - 50);
    const result = plan(changed);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain(
      `packages/api/dist/bin/${HOST} is stale: input packages/api/crates/api/src/lib.rs has other bytes than the build read`,
    );
    expect(result.stderr).not.toContain('packages/api/dist/js is stale');
    expect(result.stderr).toContain('nx run api:build');
    expect(readlinkSync(changed.alias)).toBe(STORE);
  });

  it('refuses a source added under the recorded pathspecs and one removed since the build', () => {
    const added = fixture();
    record(added);
    write(join(added.source, 'src/extra.ts'), 'export {};\n', SOURCES);
    write(join(added.source, 'src/extra.test.ts'), 'export {};\n', SOURCES);
    const addedResult = plan(added);
    expect(addedResult.status).not.toBe(0);
    expect(addedResult.stderr).toContain(
      'packages/api/dist/js is stale: input packages/api/src/extra.ts was added since the build',
    );
    expect(readlinkSync(added.alias)).toBe(STORE);

    const removed = fixture();
    record(removed);
    rmSync(join(removed.source, 'src/index.ts'));
    expect(plan(removed).stderr).toContain(
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
    const deletedResult = plan(deleted);
    expect(deletedResult.stderr).not.toContain('refusing');
    expect(deletedResult.status).toBe(0);
  });

  it('links again once a build restores outputs whose record names the current bytes', () => {
    const restored = fixture();
    record(restored);
    write(join(restored.source, 'src/index.ts'), 'export const value = 2;\n', LATER);
    expect(plan(restored).status).not.toBe(0);
    // What a cache hit does: the outputs and their record come back together, older than the edit.
    record(restored);
    touch(join(restored.source, 'dist/js/.build-inputs.json'), BUILT);
    const result = plan(restored);
    expect(result.stderr).not.toContain('refusing');
    expect(result.status).toBe(0);
    expect(realpathSync(restored.alias)).toBe(restored.source);
  });

  it('refuses a record it cannot read rather than trusting it', () => {
    const broken = fixture();
    record(broken);
    writeFileSync(join(broken.source, 'dist/js/.build-inputs.json'), JSON.stringify({ version: 2, files: {} }));
    const result = plan(broken);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('dist/js/.build-inputs.json is not a version 1 build input record');
    expect(readlinkSync(broken.alias)).toBe(STORE);
  });
});
