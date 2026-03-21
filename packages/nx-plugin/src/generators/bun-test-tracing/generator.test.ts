import { createTreeWithEmptyWorkspace } from '@nx/devkit/testing';
import { readJson, Tree } from '@nx/devkit';
import generator from './generator';

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
    tree.write(
      'packages/example/package.json',
      JSON.stringify({ name: '@scope/example' }),
    );
    tree.write(
      'packages/example/tsconfig.json',
      JSON.stringify({ compilerOptions: {} }),
    );
    tree.write(
      'packages/example/tsconfig.lib.json',
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { baseUrl: '.', rootDir: 'src' },
        include: ['src/**/*.ts'],
      }),
    );

    await generator(tree, {
      project: 'example',
      opContextModule: '@smoothbricks/lmao',
      spanContextModule: '@smoothbricks/lmao',
    });

    expect(tree.read('packages/example/bunfig.toml', 'utf-8')).toContain('preload = [\"../../test-trace-preload.ts\"]');
    expect(tree.exists('packages/example/test-trace-setup.ts')).toBe(false);

    const tracerFile = tree.read('packages/example/src/test-suite-tracer.ts', 'utf-8');
    expect(tracerFile).toContain("import { type TraceContext, opContext } from '@smoothbricks/lmao';");
    expect(tracerFile).toContain('makeBunTestSuiteTracer(opContext');

    const packageJson = readJson(tree, 'packages/example/package.json');
    expect(packageJson.scripts?.test).toBe('bun test');
    expect(packageJson.devDependencies?.['@smoothbricks/lmao']).toBe('workspace:*');

    const tsconfigTest = readJson(tree, 'packages/example/tsconfig.test.json') as {
      extends?: string;
      compilerOptions?: { types?: string[]; outDir?: string; tsBuildInfoFile?: string };
      include?: string[];
      references?: Array<{ path: string }>;
    };
    expect(tsconfigTest.extends).toBe('./tsconfig.json');
    expect(tsconfigTest.compilerOptions?.types).toEqual(['bun']);
    expect(tsconfigTest.compilerOptions?.outDir).toBe('dist-test');
    expect(tsconfigTest.compilerOptions?.tsBuildInfoFile).toBe('dist-test/tsconfig.test.tsbuildinfo');
    expect(tsconfigTest.include).toContain('src/test-suite-tracer.ts');
    expect(referencePaths(tsconfigTest.references)).toEqual([
      './tsconfig.lib.json',
    ]);
  });

  it('should add bunfig.toml if missing', async () => {
    tree.write(
      'packages/example/package.json',
      JSON.stringify({ name: '@scope/example' }),
    );
    tree.write(
      'packages/example/tsconfig.json',
      JSON.stringify({ compilerOptions: {} }),
    );
    tree.write(
      'packages/example/tsconfig.lib.json',
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { baseUrl: '.', rootDir: 'src' },
        include: ['src/**/*.ts'],
      }),
    );

    await generator(tree, {
      project: 'example',
      opContextModule: '@smoothbricks/lmao',
      spanContextModule: '@smoothbricks/lmao',
    });

    const bunfig = tree.read('packages/example/bunfig.toml', 'utf-8') ?? '';
    expect(bunfig).toContain('[test]');
    expect(bunfig).toContain('preload');
  });

  it('should update existing bunfig.toml without duplicating preload', async () => {
    tree.write(
      'packages/example/package.json',
      JSON.stringify({ name: '@scope/example' }),
    );
    tree.write(
      'packages/example/tsconfig.json',
      JSON.stringify({ compilerOptions: {} }),
    );
    tree.write(
      'packages/example/tsconfig.lib.json',
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { baseUrl: '.', rootDir: 'src' },
        include: ['src/**/*.ts'],
      }),
    );
    tree.write(
      'packages/example/bunfig.toml',
      '[test]\ntimeout = 10\npreload = ["./custom.ts"]\n',
    );

    await generator(tree, {
      project: 'example',
      opContextModule: '@smoothbricks/lmao',
      spanContextModule: '@smoothbricks/lmao',
    });

    const bunfig = tree.read('packages/example/bunfig.toml', 'utf-8') ?? '';
    expect(bunfig).toContain('timeout = 10');
    expect(bunfig).toContain('"./custom.ts"');
    expect(bunfig).toContain('\"../../test-trace-preload.ts\"');
    expect(bunfig.match(/test-trace-preload\.ts/g)?.length ?? 0).toBe(1);

    const tsconfigTest = readJson(tree, 'packages/example/tsconfig.test.json') as {
      extends?: string;
      compilerOptions?: { types?: string[] };
      include?: string[];
      references?: Array<{ path: string }>;
    };
    expect(tsconfigTest.extends).toBe('./tsconfig.json');
    expect(readStringArray(tsconfigTest.compilerOptions?.types)).toEqual(['bun']);
    expect(tsconfigTest.include).toContain('src/test-suite-tracer.ts');
    expect(referencePaths(tsconfigTest.references)).toEqual([
      './tsconfig.lib.json',
    ]);
  });
});
