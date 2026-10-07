import { describe, expect, it } from 'bun:test';
import { includeGlobsFor, type TestProgram, testProgramFault } from './test-program.js';

type Configs = Record<string, Record<string, unknown>>;

function program(files: string[], configs: Configs = {}): TestProgram {
  return { projectRoot: 'packages/app', files, readConfig: (path) => configs[path] ?? null };
}

/** The fault of a project whose tsconfig.test.json is `config`, with `configs` readable beside it. */
function faultOf(files: string[], config: Record<string, unknown>, configs: Configs = {}) {
  return testProgramFault(program(files, configs), config);
}

const SRC_TESTS = ['src/a.test.ts'];

describe('which files a test program selects', () => {
  it('selects the tests its include names', () => {
    expect(faultOf(SRC_TESTS, { include: ['src/**/*.test.ts'] })).toBeNull();
  });

  it('names the tests an include leaves out', () => {
    expect(faultOf(SRC_TESTS, { include: ['lib/**/*.ts'] })).toEqual({ kind: 'tests-unselected', tests: SRC_TESTS });
  });

  it('reports a project with no tests when no file is selected, and none when some is', () => {
    expect(faultOf(['src/index.ts'], { include: ['src/**/*.test.ts'] })).toEqual({ kind: 'no-tests' });
    expect(faultOf(['src/index.ts'], { include: ['src/**/*.ts'] })).toBeNull();
  });

  it('reads a last segment with no wildcard and no extension as a directory', () => {
    expect(faultOf(SRC_TESTS, { include: ['src'] })).toBeNull();
    expect(faultOf(['lib/a.test.ts'], { include: ['src'] })).toEqual({
      kind: 'tests-unselected',
      tests: ['lib/a.test.ts'],
    });
  });

  it('reads `?` as one character and `*` as one segment', () => {
    expect(faultOf(['src/a.test.ts'], { include: ['src/?.test.ts'] })).toBeNull();
    expect(faultOf(['src/deep/a.test.ts'], { include: ['src/*.test.ts'] })).not.toBeNull();
  });

  it('applies the default include, every file, when neither include nor files is declared', () => {
    expect(faultOf(SRC_TESTS, {})).toBeNull();
  });

  it('selects only the named files when `files` is declared without an include', () => {
    expect(faultOf(['src/a.test.ts', 'src/index.ts'], { files: ['src/a.test.ts'] })).toBeNull();
    expect(faultOf(['src/a.test.ts', 'src/index.ts'], { files: ['src/index.ts'] })).toEqual({
      kind: 'tests-unselected',
      tests: ['src/a.test.ts'],
    });
  });

  it('removes what an exclude names, and everything beneath it', () => {
    expect(faultOf(SRC_TESTS, { include: ['src/**/*.ts'], exclude: ['src'] })).not.toBeNull();
    expect(faultOf(SRC_TESTS, { include: ['src/**/*.ts'], exclude: ['src/**'] })).not.toBeNull();
    expect(faultOf(SRC_TESTS, { include: ['src/**/*.ts'], exclude: ['src/**/__tests__/**'] })).toBeNull();
  });

  it('ends an exclude at a segment boundary, so a prefix pattern drops the directory it matches', () => {
    // TypeScript terminates an exclude at `$` or `/`: `src/foo*` removes src/foobar/ whole.
    expect(faultOf(['src/foobar/a.test.ts'], { include: ['src/**/*.ts'], exclude: ['src/foo*'] })).not.toBeNull();
    expect(faultOf(['src/bar/a.test.ts'], { include: ['src/**/*.ts'], exclude: ['src/foo*'] })).toBeNull();
  });

  it('keeps node_modules out when no exclude is declared', () => {
    expect(faultOf(['node_modules/x/a.test.ts'], { include: ['**/*.ts'] })).toEqual({
      kind: 'tests-unselected',
      tests: ['node_modules/x/a.test.ts'],
    });
  });
});

describe('where a test program inherits its selection from', () => {
  const base = (include: string[]): Record<string, unknown> => ({ include });

  it('takes an include from the base it extends, and exclude from the same chain', () => {
    const configs: Configs = {
      'packages/app/tsconfig.json': { include: ['src/**/*.ts'], exclude: ['src/**/*.test.ts'] },
    };
    expect(faultOf(SRC_TESTS, { extends: './tsconfig.json' }, configs)).toEqual({
      kind: 'tests-unselected',
      tests: SRC_TESTS,
    });
  });

  it('lets a later base win over an earlier one', () => {
    const configs: Configs = { 'packages/app/a.json': base(['lib/**/*']), 'packages/app/b.json': base(['src/**/*']) };

    expect(faultOf(SRC_TESTS, { extends: ['./a.json', './b.json'] }, configs)).toBeNull();
    expect(faultOf(SRC_TESTS, { extends: ['./b.json', './a.json'] }, configs)).not.toBeNull();
  });

  it('lets the file win over every base', () => {
    const configs: Configs = { 'packages/app/a.json': base(['lib/**/*']) };

    expect(faultOf(SRC_TESTS, { extends: './a.json', include: ['src/**/*'] }, configs)).toBeNull();
  });

  it('writes an inherited pattern against the config that declared it', () => {
    const configs: Configs = { 'packages/app/base/tsconfig.json': base(['../src/**/*.ts']) };

    expect(faultOf(SRC_TESTS, { extends: './base/tsconfig.json' }, configs)).toBeNull();
  });

  it('selects nothing for an include ending in a bare **, which tsc itself rejects (TS5010)', () => {
    expect(faultOf(SRC_TESTS, { include: ['src/**'] })).toEqual({ kind: 'tests-unselected', tests: SRC_TESTS });
  });

  it('never selects a hidden file with a wildcard, as TypeScript does not', () => {
    expect(faultOf(['src/.a.test.ts'], { include: ['src/**/*.test.ts'] })).toEqual({
      kind: 'tests-unselected',
      tests: ['src/.a.test.ts'],
    });
  });

  it('lets an exclude remove hidden paths, though an include never selects them with a wildcard', () => {
    const hidden = ['src/.hid/a.test.ts'];

    expect(faultOf(hidden, { include: ['src/.hid/*.test.ts'] })).toBeNull();
    expect(faultOf(hidden, { include: ['src/.hid/*.test.ts'], exclude: ['src/*'] })).toEqual({
      kind: 'tests-unselected',
      tests: hidden,
    });
  });

  it('counts only what the tool treats as a test: a fixture is not one', () => {
    expect(faultOf(['fixtures/a.test.ts', 'src/index.ts'], { include: ['src/**/*.test.ts'] })).toEqual({
      kind: 'no-tests',
    });
  });

  it('writes the configDir template against the config being resolved, wherever it was declared', () => {
    const configs: Configs = { 'packages/app/base/tsconfig.json': base(['${configDir}/src']) };

    expect(faultOf(SRC_TESTS, { extends: './base/tsconfig.json' }, configs)).toBeNull();
  });

  it('cannot say when a base is a package, or missing', () => {
    expect(faultOf([], { extends: '@acme/tsconfig/bun.json' })).toBeNull();
    expect(faultOf([], { extends: './missing.json' })).toBeNull();
  });

  it('cannot say for a pattern outside the project, an absolute one, or one for files it does not list', () => {
    expect(faultOf([], { include: ['../other/**/*.ts'] })).toBeNull();
    expect(faultOf([], { include: ['/abs/src/**/*.ts'] })).toBeNull();
    expect(faultOf(SRC_TESTS, { include: ['src/**/*.test.js'] })).toBeNull();
  });
});

describe('where a test program looks for tests', () => {
  const canonical = (files: string[], isNew = true) => includeGlobsFor(program(files), {}, isNew);

  it('is src, with the tracer, for a project whose tests live there', () => {
    expect(canonical(SRC_TESTS)).toEqual([
      'src/**/*.test.ts',
      'src/**/*.spec.ts',
      'src/**/__tests__/**/*.ts',
      'src/**/__tests__/**/*.tsx',
      'src/test-suite-tracer.ts',
    ]);
  });

  it('is src for a project with no tests yet', () => {
    expect(canonical(['src/index.ts'])).toContain('src/**/*.test.ts');
  });

  it('adds the .tsx globs only where .tsx tests exist, so a project without them is unchanged', () => {
    expect(canonical(['src/App.test.tsx'])).toEqual([
      'src/**/*.test.ts',
      'src/**/*.spec.ts',
      'src/**/*.test.tsx',
      'src/**/*.spec.tsx',
      'src/**/__tests__/**/*.ts',
      'src/**/__tests__/**/*.tsx',
      'src/test-suite-tracer.ts',
    ]);
    const selected = program(['src/App.test.tsx']);
    expect(testProgramFault(selected, { include: canonical(['src/App.test.tsx']) })).toBeNull();
  });

  it('is the directory itself for a top-level __tests__, not a second __tests__ beneath it', () => {
    const files = ['__tests__/a.ts'];

    expect(canonical(files)).toEqual(['__tests__/**/*.ts', '__tests__/**/*.tsx']);
    expect(testProgramFault(program(files), { include: canonical(files) })).toBeNull();
  });

  it('is the project root for a test beside the manifest', () => {
    expect(canonical(['a.test.ts'])).toEqual(['*.test.ts', '*.spec.ts']);
  });

  it('is every directory a src-less project keeps tests in, and never its fixtures', () => {
    expect(canonical(['direnv/a.test.ts', 'scripts/b.spec.ts', 'fixtures/c.test.ts'])).toEqual([
      'direnv/**/*.test.ts',
      'direnv/**/*.spec.ts',
      'direnv/**/__tests__/**/*.ts',
      'direnv/**/__tests__/**/*.tsx',
      'scripts/**/*.test.ts',
      'scripts/**/*.spec.ts',
      'scripts/**/__tests__/**/*.ts',
      'scripts/**/__tests__/**/*.tsx',
    ]);
  });

  it('stays on src when tests also live elsewhere: those are strays, not a reason to widen', () => {
    expect(canonical(['src/a.test.ts', 'scripts/b.test.ts'])).not.toContain('scripts/**/*.test.ts');
  });

  it('leaves a declared include alone for tests it already selects', () => {
    const files = ['direnv/a.test.ts'];

    expect(includeGlobsFor(program(files), { include: ['direnv/**/*'] }, false)).toEqual([]);
    expect(includeGlobsFor(program(files), { include: ['lib/**/*'] }, false)).toContain('direnv/**/*.test.ts');
  });
});
