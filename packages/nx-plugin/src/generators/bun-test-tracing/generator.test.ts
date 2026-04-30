import { beforeEach, describe, expect, it } from 'bun:test';
import { addProjectConfiguration, readJson, type Tree } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import { BOUNDED_TEST_KILL_AFTER_MS, BOUNDED_TEST_TIMEOUT_MS } from '../../bounded-test-policy.js';
import generator from './generator.js';

function referencePaths(references: Array<{ path: string }> | undefined): string[] {
  return (references ?? []).map((ref) => ref.path);
}

function readStringArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.filter((item): item is string => typeof item === 'string');
  }
  return [];
}

describe('bun-test-tracing generator', () => {
  let tree: Tree;

  beforeEach(() => {
    tree = createTreeWithEmptyWorkspace();
  });

  it('should generate files and update configs for a project', async () => {
    addWorkspacePackage(tree, '@smoothbricks/lmao', 'packages/lmao');
    addWorkspacePackage(tree, 'example', 'packages/example');

    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      type: 'module',
      dependencies: {
        '@smoothbricks/lmao': 'workspace:*',
      },
    });
    writeJsonFile(tree, 'packages/example/tsconfig.json', {
      extends: '../../tsconfig.base.json',
      files: [],
      include: [],
      references: [{ path: './tsconfig.lib.json' }],
    });
    writeJsonFile(tree, 'packages/example/tsconfig.lib.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: {
        baseUrl: '.',
        rootDir: 'src',
      },
      include: ['src/**/*.ts'],
    });

    await generator(tree, {
      project: 'example',
      opContextModule: '@smoothbricks/lmao',
    });

    expect(tree.read('packages/example/bunfig.toml', 'utf-8')).toContain(
      'preload = ["@smoothbricks/lmao/bun/preload", "@smoothbricks/lmao/bun/trace-preload"]',
    );
    expect(tree.read('packages/example/bunfig.toml', 'utf-8')).toContain('timeout = 30000');

    const tracerFile = tree.read('packages/example/src/test-suite-tracer.ts', 'utf-8');
    expect(tracerFile).toContain("import { opContext } from '@smoothbricks/lmao';");
    expect(tracerFile).toContain("import { defineTestTracer } from '@smoothbricks/lmao/testing/bun';");
    expect(tracerFile).toContain(
      'export const { useTestSpan, opContext, extraTestColumns } = defineTestTracer(opContext);',
    );

    const packageJson = readJson(tree, 'packages/example/package.json');
    expect(packageJson.scripts?.test).toBe('nx run example:test --tui=false --outputStyle=stream');
    expect(packageJson.nx?.targets?.test).toEqual({
      executor: '@smoothbricks/nx-plugin:bounded-exec',
      dependsOn: ['typecheck-tests', '^build'],
      options: {
        command: 'bun test',
        cwd: '{projectRoot}',
        timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
        killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
      },
    });
    expect(
      packageJson.dependencies?.['@smoothbricks/lmao'] ?? packageJson.devDependencies?.['@smoothbricks/lmao'],
    ).toBe('workspace:*');

    const tsconfigTest = readJson(tree, 'packages/example/tsconfig.test.json') as {
      extends?: string;
      compilerOptions?: {
        types?: string[];
        composite?: boolean;
        declaration?: boolean;
        declarationMap?: boolean;
        emitDeclarationOnly?: boolean;
        noEmit?: boolean;
        outDir?: string;
        tsBuildInfoFile?: string;
      };
      include?: string[];
      references?: Array<{ path: string }>;
    };
    expect(tsconfigTest.extends).toBe('../../tsconfig.base.json');
    expect(tsconfigTest.compilerOptions?.types).toEqual(['bun']);
    expect(tsconfigTest.compilerOptions?.composite).toBe(false);
    expect(tsconfigTest.compilerOptions?.declaration).toBe(false);
    expect(tsconfigTest.compilerOptions?.declarationMap).toBe(false);
    expect(tsconfigTest.compilerOptions?.emitDeclarationOnly).toBe(false);
    expect(tsconfigTest.compilerOptions?.noEmit).toBe(true);
    expect(tsconfigTest.compilerOptions?.outDir).toBeUndefined();
    expect(tsconfigTest.compilerOptions?.tsBuildInfoFile).toBeUndefined();
    expect(tsconfigTest.include).toContain('src/test-suite-tracer.ts');
    expect(referencePaths(tsconfigTest.references)).toContain('./tsconfig.lib.json');

    const projectTsconfig = readJson(tree, 'packages/example/tsconfig.json') as {
      references?: Array<{ path: string }>;
    };
    expect(referencePaths(projectTsconfig.references)).not.toContain('./tsconfig.test.json');
  });

  it('should add bunfig.toml if missing', async () => {
    addWorkspacePackage(tree, '@smoothbricks/lmao', 'packages/lmao');
    addWorkspacePackage(tree, 'example', 'packages/example');

    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      type: 'module',
    });
    writeJsonFile(tree, 'packages/example/tsconfig.json', {
      extends: '../../tsconfig.base.json',
      files: [],
      include: [],
      references: [{ path: './tsconfig.lib.json' }],
    });
    writeJsonFile(tree, 'packages/example/tsconfig.lib.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: { baseUrl: '.', rootDir: 'src' },
      include: ['src/**/*.ts'],
    });

    await generator(tree, {
      project: 'example',
      opContextModule: '@smoothbricks/lmao',
    });

    const bunfig = tree.read('packages/example/bunfig.toml', 'utf-8') ?? '';
    expect(bunfig).toContain('[test]');
    expect(bunfig).toContain('timeout = 30000');
    expect(bunfig).toContain('preload');
  });

  it('should update existing bunfig.toml without duplicating preload', async () => {
    addWorkspacePackage(tree, '@smoothbricks/lmao', 'packages/lmao');
    addWorkspacePackage(tree, 'example', 'packages/example');

    writeJsonFile(tree, 'packages/example/package.json', {
      name: '@scope/example',
      type: 'module',
      scripts: { test: 'bun test' },
      dependencies: {
        '@smoothbricks/lmao': 'workspace:*',
      },
      nx: {
        targets: {
          test: {
            executor: '@smoothbricks/nx-plugin:bounded-exec',
            dependsOn: ['typecheck-tests', '^build'],
            options: {
              command: 'bun test',
              cwd: '{projectRoot}',
              timeoutMs: 600000,
              killAfterMs: 10000,
            },
          },
          lint: {},
        },
      },
    });
    writeJsonFile(tree, 'packages/example/tsconfig.json', {
      extends: '../../tsconfig.base.json',
      compilerOptions: {
        rootDir: 'src',
      },
      include: ['src/**/*'],
      references: [{ path: './tsconfig.lib.json' }],
    });
    writeJsonFile(tree, 'packages/example/tsconfig.lib.json', {
      extends: './tsconfig.json',
      compilerOptions: { baseUrl: '.', rootDir: 'src' },
    });
    tree.write('packages/example/bunfig.toml', '[test]\ntimeout = 10\npreload = ["./custom.ts"]\n');

    await generator(tree, {
      project: 'example',
      opContextModule: '@smoothbricks/lmao',
    });

    const bunfig = tree.read('packages/example/bunfig.toml', 'utf-8') ?? '';
    expect(bunfig).toContain('timeout = 10');
    expect(bunfig).toContain('@smoothbricks/lmao/bun/preload');
    expect(bunfig).toContain('@smoothbricks/lmao/bun/trace-preload');

    const packageJson = readJson(tree, 'packages/example/package.json');
    expect(packageJson.scripts?.test).toBe('nx run example:test --tui=false --outputStyle=stream');

    const tsconfigTest = readJson(tree, 'packages/example/tsconfig.test.json') as {
      extends?: string;
      compilerOptions?: {
        types?: string[];
        composite?: boolean;
        declaration?: boolean;
        declarationMap?: boolean;
        emitDeclarationOnly?: boolean;
        noEmit?: boolean;
        outDir?: string;
      };
      include?: string[];
      references?: Array<{ path: string }>;
    };
    expect(tsconfigTest.extends).toBe('./tsconfig.json');
    expect(readStringArray(tsconfigTest.compilerOptions?.types)).toEqual(['bun']);
    expect(tsconfigTest.compilerOptions?.composite).toBe(false);
    expect(tsconfigTest.compilerOptions?.declaration).toBe(false);
    expect(tsconfigTest.compilerOptions?.declarationMap).toBe(false);
    expect(tsconfigTest.compilerOptions?.emitDeclarationOnly).toBe(false);
    expect(tsconfigTest.compilerOptions?.noEmit).toBe(true);
    expect(tsconfigTest.compilerOptions?.outDir).toBeUndefined();
    expect(tsconfigTest.include).toContain('src/test-suite-tracer.ts');
    expect(referencePaths(tsconfigTest.references)).toContain('./tsconfig.lib.json');
  });
});

function addWorkspacePackage(tree: Tree, name: string, root: string): void {
  addProjectConfiguration(tree, name, {
    root,
    sourceRoot: `${root}/src`,
    projectType: 'library',
    targets: {},
  });
  writeJsonFile(tree, `${root}/package.json`, { name, type: 'module' });
  writeJsonFile(tree, `${root}/tsconfig.lib.json`, { extends: '../../tsconfig.base.json' });
}

function writeJsonFile(tree: Tree, filePath: string, value: unknown): void {
  tree.write(filePath, `${JSON.stringify(value, null, 2)}\n`);
}
