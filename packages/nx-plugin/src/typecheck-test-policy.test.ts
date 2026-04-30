import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { addProjectConfiguration, readJson, writeJson } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import {
  applyTypecheckTestDefaults,
  applyTypecheckTestPolicy,
  applyTypecheckTestPolicyTree,
  checkTsconfigTestReference,
  checkTypecheckTestConfig,
  checkTypecheckTestPolicy,
  checkTypecheckTestPolicyTree,
  detectPackageTestRunners,
  removeTsconfigTestReference,
} from './typecheck-test-policy.js';

// ---------------------------------------------------------------------------
// Helpers for filesystem tests
// ---------------------------------------------------------------------------

async function writeJsonFs(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
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
    expect(issues[0]!.message).toContain('noEmit must be true');
  });

  it('reports composite = true', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, composite: true } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]!.message).toContain('composite');
  });

  it('reports declaration = true', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, declaration: true } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]!.message).toContain('declaration = true');
  });

  it('reports declarationMap = true', () => {
    const issues = checkTypecheckTestConfig(
      { compilerOptions: { noEmit: true, declarationMap: true } },
      'packages/app',
    );
    expect(issues.length).toBe(1);
    expect(issues[0]!.message).toContain('declarationMap');
  });

  it('reports dist-test outDir', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: { noEmit: true, outDir: 'dist-test' } }, 'packages/app');
    expect(issues.length).toBe(1);
    expect(issues[0]!.message).toContain('dist-test');
  });

  it('reports dist-test tsBuildInfoFile', () => {
    const issues = checkTypecheckTestConfig(
      { compilerOptions: { noEmit: true, tsBuildInfoFile: 'dist-test/tsconfig.test.tsbuildinfo' } },
      'packages/app',
    );
    expect(issues.length).toBe(1);
    expect(issues[0]!.message).toContain('dist-test');
  });

  it('returns empty for null input', () => {
    const issues = checkTypecheckTestConfig(null, 'packages/app');
    expect(issues).toEqual([]);
  });

  it('uses packagePath in issue path', () => {
    const issues = checkTypecheckTestConfig({ compilerOptions: {} }, 'packages/mylib');
    expect(issues[0]!.path).toContain('packages/mylib');
    expect(issues[0]!.path).toContain('tsconfig.test.json');
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
    expect(issues[0]!.message).toContain('must not reference ./tsconfig.test.json');
  });

  it('returns empty for null input', () => {
    const issues = checkTsconfigTestReference(null, 'packages/app');
    expect(issues).toEqual([]);
  });
});

describe('applyTypecheckTestDefaults', () => {
  it('applies all defaults to empty object', () => {
    const tsconfigTest: Record<string, unknown> = {};
    const changed = applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      referencePaths: ['./tsconfig.lib.json'],
    });
    expect(changed).toBe(true);
    expect(tsconfigTest.extends).toBe('../../tsconfig.base.json');

    const compilerOptions = tsconfigTest.compilerOptions as Record<string, unknown>;
    expect(compilerOptions.noEmit).toBe(true);
    expect(compilerOptions.composite).toBe(false);
    expect(compilerOptions.declaration).toBe(false);
    expect(compilerOptions.declarationMap).toBe(false);
    expect(compilerOptions.emitDeclarationOnly).toBe(false);
    expect(compilerOptions.types).toContain('bun');

    const include = tsconfigTest.include as string[];
    expect(include).toContain('src/**/*.test.ts');
    expect(include).toContain('src/**/*.spec.ts');

    const references = tsconfigTest.references as Array<{ path: string }>;
    expect(references).toContainEqual({ path: './tsconfig.lib.json' });
  });

  it('uses custom extends from lib tsconfig', () => {
    const tsconfigTest: Record<string, unknown> = {};
    applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      tsconfigLibExtends: '../tsconfig.custom.json',
      referencePaths: [],
    });
    expect(tsconfigTest.extends).toBe('../tsconfig.custom.json');
  });

  it('copies lib compiler options', () => {
    const tsconfigTest: Record<string, unknown> = {};
    applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      libCompilerOptions: { baseUrl: '.', module: 'esnext', jsx: 'react-jsx' },
      referencePaths: [],
    });
    const compilerOptions = tsconfigTest.compilerOptions as Record<string, unknown>;
    expect(compilerOptions.baseUrl).toBe('.');
    expect(compilerOptions.module).toBe('esnext');
    expect(compilerOptions.jsx).toBe('react-jsx');
  });

  it('does not add bun types for vitest-only', () => {
    const tsconfigTest: Record<string, unknown> = {};
    applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['vitest'] as const),
      referencePaths: [],
    });
    const compilerOptions = tsconfigTest.compilerOptions as Record<string, unknown>;
    expect(compilerOptions.types).toBeUndefined();
  });

  it('removes outDir and tsBuildInfoFile', () => {
    const tsconfigTest: Record<string, unknown> = {
      compilerOptions: { outDir: 'dist-test', tsBuildInfoFile: 'dist-test/tsconfig.tsbuildinfo' },
    };
    const changed = applyTypecheckTestDefaults(tsconfigTest, {
      testRunners: new Set(['bun'] as const),
      referencePaths: [],
    });
    expect(changed).toBe(true);
    const compilerOptions = tsconfigTest.compilerOptions as Record<string, unknown>;
    expect('outDir' in compilerOptions).toBe(false);
    expect('tsBuildInfoFile' in compilerOptions).toBe(false);
  });

  it('is idempotent', () => {
    const tsconfigTest: Record<string, unknown> = {};
    const opts = {
      testRunners: new Set(['bun'] as const),
      referencePaths: ['./tsconfig.lib.json'],
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
      compilerOptions: { baseUrl: '.', rootDir: 'src', outDir: 'dist' },
    });

    const changed = applyTypecheckTestPolicyTree(tree);
    expect(changed).toBe(true);

    const tsconfig = readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.test.json');
    const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
    expect(compilerOptions.noEmit).toBe(true);
    expect(compilerOptions.composite).toBe(false);
    expect(compilerOptions.types).toContain('bun');
    expect(compilerOptions.baseUrl).toBe('.');
    expect(tsconfig.extends).toBe('../../tsconfig.base.json');

    const include = tsconfig.include as string[];
    expect(include).toContain('src/**/*.test.ts');
    expect(include).toContain('src/**/*.spec.ts');

    const references = tsconfig.references as Array<{ path: string }>;
    expect(references).toContainEqual({ path: './tsconfig.lib.json' });
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

    const changed = applyTypecheckTestPolicyTree(tree);
    expect(changed).toBe(true);

    const tsconfig = readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.json');
    const references = tsconfig.references as Array<{ path: string }>;
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
      compilerOptions: {},
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
      compilerOptions: {},
    });

    expect(applyTypecheckTestPolicyTree(tree)).toBe(true);

    const tsconfig = readJson<Record<string, unknown>>(tree, 'packages/app/tsconfig.test.json');
    const references = tsconfig.references as Array<{ path: string }>;
    expect(references).toContainEqual({ path: './tsconfig.lib.json' });
    expect(references).toContainEqual({ path: '../lib/tsconfig.lib.json' });
  });
});

// ---------------------------------------------------------------------------
// Layer 3: Filesystem-based tests (existing)
// ---------------------------------------------------------------------------

describe('typecheck test policy', () => {
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
      expect(issues[0]!.message).toContain('bun test');
      expect(issues[0]!.message).toContain('tsconfig.test.json');

      // apply should create it
      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJsonFs(join(root, 'packages/app/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
      expect(compilerOptions.noEmit).toBe(true);
      expect(compilerOptions.composite).toBe(false);
      expect(compilerOptions.declaration).toBe(false);
      expect(compilerOptions.declarationMap).toBe(false);
      expect(compilerOptions.emitDeclarationOnly).toBe(false);
      expect(compilerOptions.types).toContain('bun');
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');

      const include = tsconfig.include as string[];
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

      const tsconfig = (await readJsonFs(join(root, 'packages/web/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
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
      expect(issues[0]!.message).toContain('bun test');

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJsonFs(join(root, 'packages/lib/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
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

      const tsconfig = (await readJsonFs(join(root, 'packages/app/tsconfig.json'))) as Record<string, unknown>;
      const references = tsconfig.references as Array<{ path: string }>;
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
          baseUrl: '.',
          module: 'esnext',
          moduleResolution: 'bundler',
          jsx: 'react-jsx',
          lib: ['ES2023', 'DOM'],
        },
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJsonFs(join(root, 'packages/app/tsconfig.test.json'))) as Record<string, unknown>;
      const compilerOptions = tsconfig.compilerOptions as Record<string, unknown>;
      expect(compilerOptions.baseUrl).toBe('.');
      expect(compilerOptions.module).toBe('esnext');
      expect(compilerOptions.moduleResolution).toBe('bundler');
      expect(compilerOptions.jsx).toBe('react-jsx');
      expect(compilerOptions.lib).toEqual(['ES2023', 'DOM']);
      // extends should come from tsconfig.lib.json
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');

      // references should include ./tsconfig.lib.json
      const references = tsconfig.references as Array<{ path: string }>;
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
        compilerOptions: {},
      });
      await writeJsonFs(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
        dependencies: { '@scope/lib': 'workspace:*' },
      });
      await writeJsonFs(join(root, 'packages/app/tsconfig.lib.json'), {
        extends: '../../tsconfig.base.json',
        compilerOptions: {},
      });

      expect(applyTypecheckTestPolicy(root)).toBe(true);

      const tsconfig = (await readJsonFs(join(root, 'packages/app/tsconfig.test.json'))) as Record<string, unknown>;
      const references = tsconfig.references as Array<{ path: string }>;
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
      expect(issues[0]!.message).toContain('bun test');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
