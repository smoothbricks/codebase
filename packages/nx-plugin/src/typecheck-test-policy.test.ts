import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { addProjectConfiguration, readJson, type Tree, writeJson } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';
import { FsTree, flushChanges } from 'nx/src/generators/tree.js';
import { inspectManagedPaths } from './managed-files/paths.js';
import { assertNoManagedConflicts, stageManagedFiles } from './managed-files/tree.js';
import type { TestProgram } from './test-program.js';

import {
  applyTypecheckTestDefaults,
  checkTsconfigTestReference,
  checkTypecheckTestConfig,
  checkTypecheckTestPolicy,
  checkTypecheckTestPolicyTree,
  detectPackageTestRunners,
  removeTsconfigTestReference,
  renderTypecheckTestFiles,
} from './typecheck-test-policy.js';

// ---------------------------------------------------------------------------
// Helpers for filesystem tests
// ---------------------------------------------------------------------------

async function writeJsonFs(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function applyTypecheckTestPolicyTree(tree: Tree): boolean {
  const results = stageManagedFiles(tree, renderTypecheckTestFiles(tree));
  assertNoManagedConflicts(results);
  return results.some((result) => result.action === 'created' || result.action === 'updated');
}

function applyTypecheckTestPolicy(root: string): boolean {
  const tree = new FsTree(root, false);
  const files = renderTypecheckTestFiles(tree);
  const results = stageManagedFiles(
    tree,
    files,
    inspectManagedPaths(
      root,
      files.map((file) => file.target),
    ),
  );
  assertNoManagedConflicts(results);
  const changes = tree.listChanges();
  flushChanges(root, changes);
  return changes.length > 0;
}

async function readJsonFs(path: string): Promise<unknown> {
  return JSON.parse(await readFile(path, 'utf8'));
}

// ---------------------------------------------------------------------------
// Layer 1: Pure core function tests
// ---------------------------------------------------------------------------

describe('detectPackageTestRunners', () => {
  it('detects bun from scripts', () => {
    const runners = detectPackageTestRunners({ scripts: { test: 'bun test' } });
    expect(runners.has('bun')).toBe(true);
    expect(runners.size).toBe(1);
  });

  it('detects vitest from scripts', () => {
    const runners = detectPackageTestRunners({ scripts: { test: 'vitest run' } });
    expect(runners.has('vitest')).toBe(true);
    expect(runners.size).toBe(1);
  });

  it('detects bun from nx targets in package.json', () => {
    const runners = detectPackageTestRunners({
      nx: { targets: { test: { options: { command: 'bun test --coverage' } } } },
    });
    expect(runners.has('bun')).toBe(true);
  });

  it('detects bun from project targets', () => {
    const runners = detectPackageTestRunners({}, { test: { options: { command: 'bun test' } } });
    expect(runners.has('bun')).toBe(true);
    expect(runners.size).toBe(1);
  });

  it('detects vitest from project targets', () => {
    const runners = detectPackageTestRunners({}, { test: { options: { command: 'vitest run' } } });
    expect(runners.has('vitest')).toBe(true);
  });

  it('detects bun behind env prefix', () => {
    const runners = detectPackageTestRunners({ scripts: { test: 'NODE_ENV=test bun test' } });
    expect(runners.has('bun')).toBe(true);
  });

  it('returns empty set for non-test runners', () => {
    const runners = detectPackageTestRunners({ scripts: { test: 'node --test' } });
    expect(runners.size).toBe(0);
  });

  it('returns empty set for no scripts or targets', () => {
    const runners = detectPackageTestRunners({});
    expect(runners.size).toBe(0);
  });

  it('merges runners from scripts and project targets', () => {
    const runners = detectPackageTestRunners(
      { scripts: { test: 'bun test' } },
      { 'test-vitest': { options: { command: 'vitest run' } } },
    );
    expect(runners.has('bun')).toBe(true);
    expect(runners.has('vitest')).toBe(true);
    expect(runners.size).toBe(2);
  });
});

describe('checkTypecheckTestConfig', () => {
  it('returns no issues for valid config', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, composite: false } }, 'packages/app');
    expect(issues).toEqual([]);
  });

  it('reports missing noEmit', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { composite: false } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('noEmit must be true');
  });

  it('reports composite = true', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, composite: true } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('composite');
  });

  it('reports declaration = true', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, declaration: true } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('declaration = true');
  });

  it('reports declarationMap = true', () => {
    const issues = checkTypecheckTestConfig(
      { compilerOptions: { noEmit: true, declarationMap: true } },
      'packages/app',
    );
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('declarationMap');
  });

  it('reports dist-test outDir', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, outDir: 'dist-test' } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('dist-test');
  });

  it('reports dist-test tsBuildInfoFile', () => {
    const issues = checkTypecheckTestConfig(
      { compilerOptions: { noEmit: true, tsBuildInfoFile: 'dist-test/tsconfig.test.tsbuildinfo' } },
      'packages/app',
    );
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('dist-test');
  });

  it('returns empty for null input', () => {
    const issues = checkTypecheckTestConfig(null, 'packages/app');
    expect(issues).toEqual([]);
  });

  it('uses packagePath in issue path', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: {} }, 'packages/mylib');
    expect(issues[0]?.path).toContain('packages/mylib');
    expect(issues[0]?.path).toContain('tsconfig.test.json');
  });
});

describe('checkTsconfigTestReference', () => {
  it('returns no issues when no test reference', () => {
    const issues = checkTsconfigTestReference({ references: [{ path: './tsconfig.lib.json' }] }, 'packages/app');
    expect(issues).toEqual([]);
  });

  it('reports test reference', () => {
    const issues = checkTsconfigTestReference(
      { references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }] },
      'packages/app',
    );
    expect(issues.length).toBe(1);
    expect(issues[0]?.message).toContain('must not reference ./tsconfig.test.json');
  });

  it('returns empty for null input', () => {
    const issues = checkTsconfigTestReference(null, 'packages/app');
    expect(issues).toEqual([]);
  });
});

describe('applyTypecheckTestDefaults', () => {
  const program: TestProgram = { projectRoot: 'packages/app', files: ['src/app.test.ts'], readConfig: () => null };

  it('applies all defaults to empty object', () => {
    const tsconfigTest: Record<string, unknown> = {};
    const changed = applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: ['./tsconfig.lib.json'],
      program,
      isNew: true,
    });
    expect(changed).toBe(true);
    expect(tsconfigTest.extends).toBe('../../tsconfig.base.json');

    const compilerOptions = expectRecord(tsconfigTest.compilerOptions);
    expect(compilerOptions.noEmit).toBe(true);
    expect(compilerOptions.composite).toBe(false);
    expect(compilerOptions.declaration).toBe(false);
    expect(compilerOptions.declarationMap).toBe(false);
    expect(compilerOptions.emitDeclarationOnly).toBe(false);
    expect(compilerOptions.types).toContain('bun');

    const include = expectStringArray(tsconfigTest.include);
    expect(include).toContain('src/**/*.test.ts');
    expect(include).toContain('src/**/*.spec.ts');

    const references = expectReferences(tsconfigTest.references);
    expect(references).toContainEqual({ path: './tsconfig.lib.json' });
  });

  it('leaves an inherited include alone: minting one narrows the program to nothing', () => {
    // A package keeping its suites in `test/` inherits src + test + tooling
    // from its tsconfig.json. Writing `include` with src-test globs replaced
    // that with a program of zero files, which passed the gate by compiling
    // nothing (the false green the policy exists to prevent).
    const declared: Record<string, unknown> = { extends: './tsconfig.json', compilerOptions: { noEmit: true } };
    applyTypecheckTestDefaults(declared, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: [],
      program,
    });
    expect(declared.extends).toBe('./tsconfig.json');
    expect(Object.hasOwn(declared, 'include')).toBe(false);
    // Same for `types`: declaring it would drop the workers types the program inherits.
    expect(Object.hasOwn(expectRecord(declared.compilerOptions), 'types')).toBe(false);

    const explicit: Record<string, unknown> = { extends: './tsconfig.json', include: ['test/**/*.ts'] };
    applyTypecheckTestDefaults(explicit, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: [],
      program,
    });
    expect(expectStringArray(explicit.include)).toEqual(expect.arrayContaining(['test/**/*.ts', 'src/**/*.test.ts']));
  });

  it('fills compiler options the test program left out and keeps the ones it declared', () => {
    // A Bun-only test program declaring es2023 says something the library cannot
    // know: its runtime has change-array-by-copy while the library's output ships
    // to node, workerd and browsers. Overwriting it made `toSorted` a type error
    // in a suite that calls it, and every `smoo monorepo update` reintroduced it.
    const declared: Record<string, unknown> = {
      compilerOptions: { lib: ['es2023'], types: ['bun'], noEmit: true },
    };
    applyTypecheckTestDefaults(declared, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: [],
      program,
      libCompilerOptions: { lib: ['es2022'], module: 'preserve' },
    });

    const declaredOptions = expectRecord(declared.compilerOptions);
    expect(declaredOptions.lib).toEqual(['es2023']);
    expect(declaredOptions.module).toBe('preserve');

    const silent: Record<string, unknown> = { compilerOptions: {} };
    applyTypecheckTestDefaults(silent, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: [],
      program,
      libCompilerOptions: { lib: ['es2022'] },
    });
    expect(expectRecord(silent.compilerOptions).lib).toEqual(['es2022']);
  });

  it('extends what it is given when the file names nothing', () => {
    const tsconfigTest: Record<string, unknown> = {};
    applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../tsconfig.custom.json',
      referencePaths: [],
      program,
    });
    expect(tsconfigTest.extends).toBe('../tsconfig.custom.json');
  });

  it('copies lib compiler options', () => {
    const tsconfigTest: Record<string, unknown> = {};
    applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      libCompilerOptions: { baseUrl: '.', module: 'esnext', jsx: 'react-jsx' },
      referencePaths: [],
      program,
    });
    const compilerOptions = expectRecord(tsconfigTest.compilerOptions);
    expect(compilerOptions.baseUrl).toBe('.');
    expect(compilerOptions.module).toBe('esnext');
    expect(compilerOptions.jsx).toBe('react-jsx');
  });

  it('does not add bun types for vitest-only', () => {
    const tsconfigTest: Record<string, unknown> = {};
    applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['vitest'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: [],
      program,
    });
    const compilerOptions = expectRecord(tsconfigTest.compilerOptions);
    expect(compilerOptions.types).toBeUndefined();
  });

  it('removes outDir and tsBuildInfoFile', () => {
    const tsconfigTest: Record<string, unknown> = {
      compilerOptions: { outDir: 'dist-test', tsBuildInfoFile: 'dist-test/tsconfig.tsbuildinfo' },
    };
    const changed = applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      referencePaths: [],
      program,
    });
    expect(changed).toBe(true);
    const compilerOptions = expectRecord(tsconfigTest.compilerOptions);
    expect('outDir' in compilerOptions).toBe(false);
    expect('tsBuildInfoFile' in compilerOptions).toBe(false);
  });

  it('is idempotent', () => {
    const tsconfigTest: Record<string, unknown> = {};
    const opts = {
      testRunners: new Set(['bun'] as const),
      testExtends: '../../tsconfig.base.json',
      libCompilerOptions: { lib: ['es2024', 'webworker'] },
      referencePaths: ['./tsconfig.lib.json'],
      program,
    };
    applyTypecheckTestDefaults(tsconfigTest, opts);
    const secondChanged = applyTypecheckTestDefaults(tsconfigTest, opts);
    expect(secondChanged).toBe(false);
  });
});

describe('removeTsconfigTestReference', () => {
  it('removes test reference', () => {
    const tsconfig: Record<string, unknown> = {
      references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
    };
    expect(removeTsconfigTestReference(tsconfig)).toBe(true);
    expect(tsconfig.references).toEqual([{ path: './tsconfig.lib.json' }]);
  });

  it('returns false when no test reference', () => {
    const tsconfig: Record<string, unknown> = {
      references: [{ path: './tsconfig.lib.json' }],
    };
    expect(removeTsconfigTestReference(tsconfig)).toBe(false);
  });

  it('returns false when no references', () => {
    const tsconfig: Record<string, unknown> = {};
    expect(removeTsconfigTestReference(tsconfig)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Layer 2: Tree-based tests
// ---------------------------------------------------------------------------

describe('typecheck test policy (Tree)', () => {
  it('creates tsconfig.test.json for bun test package', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'app', { root: 'packages/app', targets: {} });
    // Remove auto-created project.json to test package.json-only detection
    if (tree.exists('packages/app/project.json')) tree.delete('packages/app/project.json');
    writeJson(tree, 'packages/app/package.json', {
      name: '@scope/app',
      scripts: { test: 'bun test' },
      nx: { name: 'app' },
    });
    writeJson(tree, 'packages/app/tsconfig.lib.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: { composite: true, baseUrl: '.', rootDir: 'src', outDir: 'dist' },
    });

    tree.write('packages/app/src/app.test.ts', 'export {};\n');
    const changed = applyTypecheckTestPolicyTree(tree);
    expect(changed).toBe(true);

    const tsconfig = readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.test.json');
    const compilerOptions = expectRecord(tsconfig.compilerOptions);
    expect(compilerOptions.noEmit).toBe(true);
    expect(compilerOptions.composite).toBe(false);
    expect(compilerOptions.types).toContain('bun');
    expect(compilerOptions.baseUrl).toBe('.');
    expect(tsconfig.extends).toBe('../../tsconfig.base.json');

    const include = expectStringArray(tsconfig.include);
    expect(include).toContain('src/**/*.test.ts');
    expect(include).toContain('src/**/*.spec.ts');

    const references = expectReferences(tsconfig.references);
    expect(references).toContainEqual({ path: './tsconfig.lib.json' });
  });

  it('extends the workspace base from the project root when there is no lib program', () => {
    // `tooling` sits one level below the workspace root. A fixed
    // '../../tsconfig.base.json' named a file above the repository.
    const tree = createTreeWithEmptyWorkspace();
    for (const [name, root] of [
      ['tooling', 'tooling'],
      ['app', 'packages/app'],
    ] as const) {
      addProjectConfiguration(tree, name, { root, targets: {} });
      if (tree.exists(`${root}/project.json`)) tree.delete(`${root}/project.json`);
      writeJson(tree, `${root}/package.json`, { name: `@scope/${name}`, scripts: { test: 'bun test' }, nx: { name } });
    }

    tree.write('tooling/src/tooling.test.ts', 'export {};\n');
    tree.write('packages/app/src/app.test.ts', 'export {};\n');
    applyTypecheckTestPolicyTree(tree);
    expect(readJson<Record<string, unknown>>(tree, 'tooling/tsconfig.test.json').extends).toBe('../tsconfig.base.json');
    expect(readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.test.json').extends).toBe(
      '../../tsconfig.base.json',
    );
  });

  it('references only composite lib programs (TS6306 otherwise), resolving composite through extends', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'tsconfig.composite.json', { compilerOptions: { composite: true } });
    for (const [name, composite] of [
      ['app', 'no'],
      ['lib', 'own'],
      ['inherits', 'base'],
    ] as const) {
      addProjectConfiguration(tree, name, { root: `packages/${name}`, targets: {} });
      if (tree.exists(`packages/${name}/project.json`)) tree.delete(`packages/${name}/project.json`);
      writeJson(tree, `packages/${name}/package.json`, {
        name: `@scope/${name}`,
        scripts: { test: 'bun test' },
        nx: { name },
        ...(name === 'app' ? { dependencies: { '@scope/lib': 'workspace:*', '@scope/inherits': 'workspace:*' } } : {}),
      });
      writeJson(tree, `packages/${name}/tsconfig.lib.json`, {
        extends:
          composite === 'base' ? ['../../tsconfig.base.json', '../../tsconfig.composite'] : '../../tsconfig.base.json',
        compilerOptions: { rootDir: 'src', outDir: 'dist', ...(composite === 'own' ? { composite: true } : {}) },
      });
    }

    for (const name of ['app', 'lib', 'inherits']) {
      tree.write(`packages/${name}/src/${name}.test.ts`, 'export {};\n');
    }
    applyTypecheckTestPolicyTree(tree);
    const references = expectReferences(
      readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.test.json').references,
    );
    // The app's own emit program is not composite: its sources are in the
    // test program by glob, not by reference. The composite dependencies are —
    // whether composite is declared on the file or inherited from a base.
    expect(references).toEqual([{ path: '../lib/tsconfig.lib.json' }, { path: '../inherits/tsconfig.lib.json' }]);
  });

  it('detects bun test in project.json targets', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'app', {
      root: 'packages/app',
      targets: { test: { executor: 'nx:run-commands', options: { command: 'bun test' } } },
    });
    writeJson(tree, 'packages/app/package.json', { name: '@scope/app' });

    const issues = checkTypecheckTestPolicyTree(tree);
    // Should detect bun test from project.json and require tsconfig.test.json
    expect(issues.some((i) => i.message.includes('requires tsconfig.test.json'))).toBe(true);
    expect(issues.some((i) => i.message.includes('bun test'))).toBe(true);
  });

  it('detects vitest in project.json targets', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'web', {
      root: 'packages/web',
      targets: { test: { executor: 'nx:run-commands', options: { command: 'vitest run' } } },
    });
    writeJson(tree, 'packages/web/package.json', { name: '@scope/web' });

    const issues = checkTypecheckTestPolicyTree(tree);
    expect(issues.some((i) => i.message.includes('vitest'))).toBe(true);
  });

  it('reports issues for bad tsconfig.test.json contents', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'bad', { root: 'packages/bad', targets: {} });
    writeJson(tree, 'packages/bad/package.json', {
      name: '@scope/bad',
      scripts: { test: 'bun test' },
      nx: { name: 'bad' },
    });
    writeJson(tree, 'packages/bad/tsconfig.test.json', {
      compilerOptions: {
        composite: true,
        declaration: true,
        outDir: 'dist-test',
      },
    });

    const issues = checkTypecheckTestPolicyTree(tree);
    const messages = issues.map((i) => i.message);
    expect(messages).toContainEqual(expect.stringContaining('noEmit must be true'));
    expect(messages).toContainEqual(expect.stringContaining('composite'));
    expect(messages).toContainEqual(expect.stringContaining('declaration = true'));
    expect(messages).toContainEqual(expect.stringContaining('dist-test'));
  });

  it('reports tsconfig.json test reference', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'app', { root: 'packages/app', targets: {} });
    writeJson(tree, 'packages/app/package.json', {
      name: '@scope/app',
      scripts: { test: 'bun test' },
      nx: { name: 'app' },
    });
    writeJson(tree, 'packages/app/tsconfig.test.json', {
      compilerOptions: { noEmit: true },
    });
    writeJson(tree, 'packages/app/tsconfig.json', {
      references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
    });

    const issues = checkTypecheckTestPolicyTree(tree);
    expect(issues.some((i) => i.message.includes('must not reference'))).toBe(true);
  });

  it('removes tsconfig.json test reference on apply', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'app', { root: 'packages/app', targets: {} });
    writeJson(tree, 'packages/app/package.json', {
      name: '@scope/app',
      scripts: { test: 'bun test' },
      nx: { name: 'app' },
    });
    writeJson(tree, 'packages/app/tsconfig.test.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: {
        noEmit: true,
        composite: false,
        declaration: false,
        declarationMap: false,
        emitDeclarationOnly: false,
        types: ['bun'],
      },
      include: [
        'src/**/*.test.ts',
        'src/**/*.spec.ts',
        'src/**/__tests__/**/*.ts',
        'src/**/__tests__/**/*.tsx',
        'src/test-suite-tracer.ts',
      ],
    });
    writeJson(tree, 'packages/app/tsconfig.json', {
      references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
    });

    tree.write('packages/app/src/app.test.ts', 'export {};\n');
    const changed = applyTypecheckTestPolicyTree(tree);
    expect(changed).toBe(true);

    const tsconfig = readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.json');
    const references = expectReferences(tsconfig.references);
    expect(references).toEqual([{ path: './tsconfig.lib.json' }]);
  });

  it('skips root project', () => {
    const tree = createTreeWithEmptyWorkspace();
    // Root project with root "." should be skipped
    addProjectConfiguration(tree, 'root', { root: '.', targets: {} });
    writeJson(tree, 'package.json', {
      name: '@scope/root',
      scripts: { test: 'bun test' },
    });

    const issues = checkTypecheckTestPolicyTree(tree);
    expect(issues).toEqual([]);
  });

  it('skips packages without test runners', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'utils', { root: 'packages/utils', targets: {} });
    writeJson(tree, 'packages/utils/package.json', {
      name: '@scope/utils',
      scripts: { build: 'tsc' },
    });

    const issues = checkTypecheckTestPolicyTree(tree);
    expect(issues).toEqual([]);
    expect(applyTypecheckTestPolicyTree(tree)).toBe(false);
  });

  it('adds workspace dependency references', () => {
    const tree = createTreeWithEmptyWorkspace();

    // Library dependency
    addProjectConfiguration(tree, 'lib', { root: 'packages/lib', targets: {} });
    writeJson(tree, 'packages/lib/package.json', { name: '@scope/lib' });
    writeJson(tree, 'packages/lib/tsconfig.lib.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: { composite: true },
    });

    // App that depends on lib
    addProjectConfiguration(tree, 'app', { root: 'packages/app', targets: {} });
    writeJson(tree, 'packages/app/package.json', {
      name: '@scope/app',
      scripts: { test: 'bun test' },
      dependencies: { '@scope/lib': 'workspace:*' },
      nx: { name: 'app' },
    });
    writeJson(tree, 'packages/app/tsconfig.lib.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: { composite: true },
    });

    tree.write('packages/app/src/app.test.ts', 'export {};\n');
    expect(applyTypecheckTestPolicyTree(tree)).toBe(true);

    const tsconfig = readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.test.json');
    const references = expectReferences(tsconfig.references);
    expect(references).toContainEqual({ path: './tsconfig.lib.json' });
    expect(references).toContainEqual({ path: '../lib/tsconfig.lib.json' });
  });
});

// ---------------------------------------------------------------------------
// Layer 3: Filesystem-based tests (existing)
// ---------------------------------------------------------------------------

describe('typecheck test policy', () => {
  it('reads a documented tsconfig.test.json instead of treating it as absent', async () => {
    // tsconfig files are JSONC: TypeScript permits comments. A plain
    // JSON.parse failed, the reader answered "absent", and the policy
    // regenerated the file — deleting a declared `lib`, an `exclude`, extra
    // `include` globs and every comment explaining them. That is how a
    // Bun-only test program loses es2023 and `toSorted` becomes a type error.
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-jsonc-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await mkdir(join(root, 'packages/app'), { recursive: true });
      const documented = [
        '{',
        '  "extends": "../../tsconfig.base.json",',
        '  // WHY es2023: this program is Bun-only and JSC has change-array-by-copy.',
        '  "compilerOptions": { "lib": ["es2023"], "types": ["bun"], "noEmit": true, "composite": false,',
        '    "declaration": false, "declarationMap": false, "emitDeclarationOnly": false },',
        '  "include": ["src/**/*.test.ts", "src/**/*.spec.ts", "src/**/__tests__/**/*.ts",',
        '    "src/**/__tests__/**/*.tsx", "src/test-suite-tracer.ts", "tests/**/*.ts"],',
        '  "exclude": ["src/lib/ts-plugin.ts"]',
        '}',
        '',
      ].join('\n');
      await writeFile(join(root, 'packages/app/tsconfig.test.json'), documented);

      // Nothing the policy wants is missing, so it must not rewrite the file.
      expect(applyTypecheckTestPolicy(root)).toBe(false);
      expect(await readFile(join(root, 'packages/app/tsconfig.test.json'), 'utf8')).toBe(documented);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('creates tsconfig.test.json for bun test package', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });

      // check should report missing tsconfig.test.json
      const issues = checkTypecheckTestPolicy(root);
      expect(issues.length).toBe(1);
      expect(issues[0]?.message).toContain('bun test');
      expect(issues[0]?.message).toContain('tsconfig.test.json');

      // apply should create it
      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/app/tsconfig.test.json')));
      const compilerOptions = expectRecord(tsconfig.compilerOptions);
      expect(compilerOptions.noEmit).toBe(true);
      expect(compilerOptions.composite).toBe(false);
      expect(compilerOptions.declaration).toBe(false);
      expect(compilerOptions.declarationMap).toBe(false);
      expect(compilerOptions.emitDeclarationOnly).toBe(false);
      expect(compilerOptions.types).toContain('bun');
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');

      const include = expectStringArray(tsconfig.include);
      expect(include).toContain('src/**/*.test.ts');
      expect(include).toContain('src/**/*.spec.ts');
      expect(include).toContain('src/**/__tests__/**/*.ts');
      expect(include).toContain('src/test-suite-tracer.ts');

      // second apply should be idempotent
      expect(applyTypecheckTestPolicy(root)).toBe(false);

      // check should now pass
      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('creates tsconfig.test.json for vitest package', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/web/package.json'), {
        name: '@scope/web',
        scripts: { test: 'vitest run' },
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/web/tsconfig.test.json')));
      const compilerOptions = expectRecord(tsconfig.compilerOptions);
      expect(compilerOptions.noEmit).toBe(true);
      // vitest should NOT add bun types
      expect(compilerOptions.types).toBeUndefined();

      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('detects bun test in Nx targets', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        nx: {
          targets: {
            test: {
              options: { command: 'bun test --coverage' },
            },
          },
        },
      });

      const issues = checkTypecheckTestPolicy(root);
      expect(issues.length).toBe(1);
      expect(issues[0]?.message).toContain('bun test');

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/lib/tsconfig.test.json')));
      const compilerOptions = expectRecord(tsconfig.compilerOptions);
      expect(compilerOptions.noEmit).toBe(true);
      expect(compilerOptions.types).toContain('bun');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects composite/declaration/dist-test in tsconfig.test.json', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/bad/package.json'), {
        name: '@scope/bad',
        scripts: { test: 'bun test' },
      });
      await writeSource(join(root, 'packages/bad/src/bad.test.ts'));
      await writeJsonFs(join(root, 'packages/bad/tsconfig.test.json'), {
        compilerOptions: {
          composite: true,
          declaration: true,
          declarationMap: true,
          outDir: 'dist-test',
          tsBuildInfoFile: 'dist-test/tsconfig.test.tsbuildinfo',
        },
      });

      const issues = checkTypecheckTestPolicy(root);
      // Should report: noEmit missing, composite, declaration, declarationMap, outDir dist-test, tsBuildInfoFile dist-test
      expect(issues.length).toBe(6);
      const messages = issues.map((issue) => issue.message);
      expect(messages).toContainEqual(expect.stringContaining('noEmit must be true'));
      expect(messages).toContainEqual(expect.stringContaining('composite'));
      expect(messages).toContainEqual(expect.stringContaining('declaration = true'));
      expect(messages).toContainEqual(expect.stringContaining('declarationMap'));
      expect(messages).toContainEqual(expect.stringContaining('dist-test'));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('removes tsconfig.json reference to ./tsconfig.test.json', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await writeJsonFs(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          noEmit: true,
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
        },
        include: ['src/**/*.test.ts'],
      });
      await writeJsonFs(join(root, 'packages/app/tsconfig.json'), {
        references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
      });

      // check should report the bad reference
      const issues = checkTypecheckTestPolicy(root);
      const referenceIssues = issues.filter((i) => i.message.includes('must not reference'));
      expect(referenceIssues.length).toBe(1);

      // apply should remove it
      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/app/tsconfig.json')));
      const references = expectReferences(tsconfig.references);
      expect(references).toEqual([{ path: './tsconfig.lib.json' }]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('copies lib compiler options', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await writeJsonFs(join(root, 'packages/app/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          composite: true,
          baseUrl: '.',
          module: 'esnext',
          moduleResolution: 'bundler',
          jsx: 'react-jsx',
          lib: ['ES2023', 'DOM'],
        },
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/app/tsconfig.test.json')));
      const compilerOptions = expectRecord(tsconfig.compilerOptions);
      expect(compilerOptions.baseUrl).toBe('.');
      expect(compilerOptions.module).toBe('esnext');
      expect(compilerOptions.moduleResolution).toBe('bundler');
      expect(compilerOptions.jsx).toBe('react-jsx');
      expect(compilerOptions.lib).toEqual(['ES2023', 'DOM']);
      // extends should come from tsconfig.lib.json
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');

      // references should include ./tsconfig.lib.json
      const references = expectReferences(tsconfig.references);
      expect(references).toContainEqual({ path: './tsconfig.lib.json' });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('adds workspace dependency references', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
      });
      await writeJsonFs(join(root, 'packages/lib/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: true },
      });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
        dependencies: { '@scope/lib': 'workspace:*' },
      });
      await writeJsonFs(join(root, 'packages/app/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: true },
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/app/tsconfig.test.json')));
      const references = expectReferences(tsconfig.references);
      expect(references).toContainEqual({ path: './tsconfig.lib.json' });
      expect(references).toContainEqual({ path: '../lib/tsconfig.lib.json' });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('does not require tsconfig.test.json for non-bun/vitest runners', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'node --test' },
      });

      expect(checkTypecheckTestPolicy(root)).toEqual([]);
      expect(applyTypecheckTestPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts valid noEmit test tsconfig', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await writeJsonFs(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: [
          'src/**/*.test.ts',
          'src/**/*.spec.ts',
          'src/**/__tests__/**/*.ts',
          'src/**/__tests__/**/*.tsx',
          'src/test-suite-tracer.ts',
        ],
      });

      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('detects bun test behind env prefix', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'NODE_ENV=test bun test' },
      });

      const issues = checkTypecheckTestPolicy(root);
      expect(issues.length).toBe(1);
      expect(issues[0]?.message).toContain('bun test');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// The test program's include comes from where the tests live, and a program
// that selects no file is refused
// ---------------------------------------------------------------------------

const SRC_GLOBS = [
  'src/**/*.test.ts',
  'src/**/*.spec.ts',
  'src/**/__tests__/**/*.ts',
  'src/**/__tests__/**/*.tsx',
  'src/test-suite-tracer.ts',
];

function globsFor(directory: string): string[] {
  return [
    `${directory}/**/*.test.ts`,
    `${directory}/**/*.spec.ts`,
    `${directory}/**/__tests__/**/*.ts`,
    `${directory}/**/__tests__/**/*.tsx`,
  ];
}

async function writeSource(path: string, text = 'export {};\n'): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, text);
}

/** The shape of this repository's `tooling` project: tests beside the shell scripts, no src/. */
async function toolingShapedWorkspace(root: string): Promise<void> {
  await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*', 'tooling'] });
  await writeJsonFs(join(root, 'tsconfig.base.json'), { compilerOptions: {} });
  await writeJsonFs(join(root, 'tooling/package.json'), {
    name: '@scope/tooling',
    private: true,
    nx: {
      name: 'tooling',
      targets: {
        test: {
          executor: '@smoothbricks/nx-plugin:bounded-exec',
          options: { command: 'bun test --timeout=30000 direnv', cwd: '{projectRoot}' },
        },
      },
    },
  });
  await writeSource(join(root, 'tooling/direnv/enter-shell.test.ts'));
}

describe('where the test program looks for tests', () => {
  it('includes the directory a src-less project keeps its tests in, not src', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await toolingShapedWorkspace(root);

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'tooling/tsconfig.test.json')));
      expect(tsconfig.include).toEqual(globsFor('direnv'));
      expect(applyTypecheckTestPolicy(root)).toBe(false);
      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps the src convention for a package whose tests live in src', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
      await writeJsonFs(join(root, 'packages/app/package.json'), { name: '@scope/app', scripts: { test: 'bun test' } });
      await writeSource(join(root, 'packages/app/src/app.test.ts'));
      // A stray outside src stays the stray-test policy's to report; it must not widen the program.
      await writeSource(join(root, 'packages/app/scripts/helper.test.ts'));

      applyTypecheckTestPolicy(root);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'packages/app/tsconfig.test.json')));
      expect(tsconfig.include).toEqual(SRC_GLOBS);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('does not add a glob for tests an include the project wrote already selects', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await toolingShapedWorkspace(root);
      await writeJsonFs(join(root, 'tooling/tsconfig.test.json'), {
        extends: '../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: ['direnv/**/*'],
      });

      expect(applyTypecheckTestPolicy(root)).toBe(false);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'tooling/tsconfig.test.json')));
      expect(tsconfig.include).toEqual(['direnv/**/*']);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('adds the directory to an include the project wrote that selects none of its tests', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await toolingShapedWorkspace(root);
      await writeJsonFs(join(root, 'tooling/tsconfig.test.json'), {
        extends: '../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: ['src/**/*.test.ts'],
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = expectRecord(await readJsonFs(join(root, 'tooling/tsconfig.test.json')));
      expect(tsconfig.include).toEqual(['src/**/*.test.ts', ...globsFor('direnv')]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('a test program that selects none of its tests', () => {
  async function packageWithoutTests(root: string): Promise<void> {
    await writeJsonFs(join(root, 'package.json'), { workspaces: ['packages/*'] });
    await writeJsonFs(join(root, 'tsconfig.base.json'), { compilerOptions: {} });
    await writeJsonFs(join(root, 'packages/app/package.json'), { name: '@scope/app', scripts: { test: 'bun test' } });
    await writeSource(join(root, 'packages/app/src/index.ts'));
  }

  it('is reported by the generator without writing the file, naming the project and why', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await packageWithoutTests(root);
      const tree = new FsTree(root, false);

      const results = stageManagedFiles(tree, renderTypecheckTestFiles(tree));
      const skipped = results.find((result) => result.target === 'packages/app/tsconfig.test.json');

      expect(skipped?.action).toBe('skipped');
      expect(skipped?.reason).toContain('matches no file');
      expect(skipped?.reason).toContain('packages/app');
      expect(tree.exists('packages/app/tsconfig.test.json')).toBe(false);
      expect(() => assertNoManagedConflicts(results)).not.toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is reported by the policy check', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await packageWithoutTests(root);
      await writeJsonFs(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: SRC_GLOBS,
      });

      const issues = checkTypecheckTestPolicy(root);

      expect(issues.map((issue) => issue.path)).toEqual([join(root, 'packages/app/tsconfig.test.json')]);
      expect(issues[0]?.message).toContain('matches no file');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is refused when its exclude removes every test', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await packageWithoutTests(root);
      await writeSource(join(root, 'packages/app/src/app.test.ts'));
      await writeJsonFs(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: SRC_GLOBS,
        exclude: ['src'],
      });

      const issues = checkTypecheckTestPolicy(root);

      expect(issues.map((issue) => issue.message).join('\n')).toContain('matches no file');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is not refused while an inherited include the plugin cannot read may select files', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await packageWithoutTests(root);
      await writeJsonFs(join(root, 'packages/app/tsconfig.test.json'), {
        extends: '@acme/tsconfig/bun.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
      });

      expect(checkTypecheckTestPolicy(root).some((issue) => issue.message.includes('matches no file'))).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is reported for a workspace member named by path, which the packages/* listing never walked', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await toolingShapedWorkspace(root);
      await writeJsonFs(join(root, 'tooling/tsconfig.test.json'), {
        extends: '../tsconfig.base.json',
        compilerOptions: {
          composite: false,
          declaration: false,
          declarationMap: false,
          emitDeclarationOnly: false,
          noEmit: true,
          types: ['bun'],
        },
        include: ['src/**/*.test.ts'],
      });

      const issues = checkTypecheckTestPolicy(root);

      expect(issues.map((issue) => issue.path)).toEqual([
        join(root, 'tooling/tsconfig.test.json'),
        join(root, 'tooling/tsconfig.test.json'),
      ]);
      expect(issues[0]?.message).toContain('matches no file of its tests');
      expect(issues[1]?.message).toContain('canonical');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is reported by the Tree check too', () => {
    const tree = createTreeWithEmptyWorkspace();
    addProjectConfiguration(tree, 'app', { root: 'packages/app', targets: {} });
    writeJson(tree, 'packages/app/package.json', {
      name: '@scope/app',
      scripts: { test: 'bun test' },
      nx: { name: 'app' },
    });
    writeJson(tree, 'tsconfig.base.json', { compilerOptions: {} });
    tree.write('packages/app/src/index.ts', 'export {};\n');
    writeJson(tree, 'packages/app/tsconfig.test.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: { noEmit: true, composite: false, declaration: false, declarationMap: false, types: ['bun'] },
      include: SRC_GLOBS,
    });

    expect(checkTypecheckTestPolicyTree(tree).map((issue) => issue.message)).toEqual([
      expect.stringContaining('matches no file (the project has no test file)'),
    ]);
  });

  it('leaves an existing config it cannot honor as it was, and still repairs the project tsconfig.json', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await packageWithoutTests(root);
      await writeJsonFs(join(root, 'packages/app/tsconfig.json'), {
        references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
      });

      const tree = new FsTree(root, false);
      const results = stageManagedFiles(tree, renderTypecheckTestFiles(tree));

      expect(results.find((result) => result.target === 'packages/app/tsconfig.json')?.action).toBe('updated');
      expect(results.find((result) => result.target === 'packages/app/tsconfig.test.json')?.action).toBe('skipped');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('does not stumble on a dangling link beside the sources', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-typecheck-test-policy-'));
    try {
      await packageWithoutTests(root);
      await writeSource(join(root, 'packages/app/src/app.test.ts'));
      await symlink(join(root, 'nowhere'), join(root, 'packages/app/result'));

      expect(applyTypecheckTestPolicy(root)).toBe(true);
      expect(checkTypecheckTestPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

interface TypeScriptReference {
  path: string;
}

function expectRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error('expected object');
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function expectStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    throw new Error('expected array');
  }
  const values: unknown[] = value;
  if (!values.every((entry): entry is string => typeof entry === 'string')) {
    throw new Error('expected string array');
  }
  return values;
}

function expectReferences(value: unknown): TypeScriptReference[] {
  if (!Array.isArray(value)) {
    throw new Error('expected array');
  }
  const values: unknown[] = value;
  if (
    !values.every(
      (entry): entry is TypeScriptReference =>
        entry !== null &&
        typeof entry === 'object' &&
        !Array.isArray(entry) &&
        'path' in entry &&
        typeof entry.path === 'string',
    )
  ) {
    throw new Error('expected TypeScript references');
  }
  return values;
}
