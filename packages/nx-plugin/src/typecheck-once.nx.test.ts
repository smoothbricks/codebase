import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

const repositoryRoot = join(import.meta.dir, '../../..');

it('fails typecheck through tsc-js on lib errors, typecheck-tests on test errors, and real typecheck on emit-less projects', async () => {
  await withNxFixture('nx-typecheck-once-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(
      join(workspace, 'package.json'),
      JSON.stringify({ name: 'typecheck-once-fixture', private: true, workspaces: ['packages/*'] }),
    );
    await writeFile(
      join(workspace, 'nx.json'),
      JSON.stringify({
        plugins: ['@smoothbricks/nx-plugin'],
        namedInputs: {
          default: ['{projectRoot}/**/*'],
          production: ['default', '!{projectRoot}/**/*.test.ts'],
        },
      }),
    );
    await writeFile(
      join(workspace, 'tsconfig.base.json'),
      JSON.stringify({
        compilerOptions: {
          target: 'ES2022',
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          strict: true,
          skipLibCheck: true,
          composite: true,
        },
      }),
    );

    const runNx = (...args: string[]) => {
      const child = spawnSync('bun', [join(repositoryRoot, 'node_modules/.bin/nx'), ...args], {
        cwd: workspace,
        env: fixtureNxEnv(workspace, socketDirectory),
        encoding: 'utf8',
      });
      if (child.error) throw child.error;
      return { status: child.status, output: child.stdout + child.stderr };
    };

    // 1. Standard lib+test project: `typecheck` delegates to `tsc-js`; test files are checked by `typecheck-tests`.
    await mkdir(join(workspace, 'packages/lib-pkg/src'), { recursive: true });
    await writeFile(
      join(workspace, 'packages/lib-pkg/package.json'),
      JSON.stringify({ name: 'lib-pkg', type: 'module' }),
    );
    await writeFile(
      join(workspace, 'packages/lib-pkg/tsconfig.lib.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { rootDir: 'src', outDir: 'dist', types: [] },
        include: ['src/**/*.ts'],
        exclude: ['src/**/*.test.ts', 'src/test-suite-tracer.ts'],
      }),
    );
    await writeFile(
      join(workspace, 'packages/lib-pkg/tsconfig.test.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: false, declaration: false, declarationMap: false, noEmit: true, types: [] },
        include: ['src/**/*.test.ts', 'src/test-suite-tracer.ts'],
        references: [{ path: './tsconfig.lib.json' }],
      }),
    );
    await writeFile(join(workspace, 'packages/lib-pkg/src/index.ts'), "export const broken: number = 'bad';\n");
    await writeFile(
      join(workspace, 'packages/lib-pkg/src/index.test.ts'),
      "import { broken } from './index.js';\nexport const tested: number = broken;\n",
    );
    await writeFile(join(workspace, 'packages/lib-pkg/src/test-suite-tracer.ts'), 'export const tracer = 1;\n');

    // 2. Emit-less project (no tsconfig.lib.json, declares its own typecheck) and custom-emit project (declares tsc-js).
    await mkdir(join(workspace, 'packages/emitless/src'), { recursive: true });
    await writeFile(
      join(workspace, 'packages/emitless/package.json'),
      JSON.stringify({
        name: 'emitless',
        type: 'module',
        nx: {
          targets: {
            typecheck: {
              executor: 'nx:run-commands',
              options: {
                command: '"$SMOO_TYPECHECK_TSC" -p tsconfig.json --noEmit',
                cwd: '{projectRoot}',
              },
            },
          },
        },
      }),
    );
    await writeFile(
      join(workspace, 'packages/emitless/tsconfig.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: false, noEmit: true, types: [] },
        include: ['src/**/*.ts'],
        exclude: ['src/**/*.test.ts'],
      }),
    );
    await writeFile(
      join(workspace, 'packages/emitless/tsconfig.test.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: false, declaration: false, declarationMap: false, noEmit: true, types: [] },
        include: ['src/**/*.test.ts'],
      }),
    );
    await writeFile(join(workspace, 'packages/emitless/src/index.ts'), "export const bad: number = 'nope';\n");
    await writeFile(join(workspace, 'packages/emitless/src/index.test.ts'), 'export const ok = 1;\n');

    await mkdir(join(workspace, 'packages/custom-emit/src'), { recursive: true });
    await writeFile(
      join(workspace, 'packages/custom-emit/package.json'),
      JSON.stringify({
        name: 'custom-emit',
        type: 'module',
        nx: {
          targets: {
            'tsc-js': { executor: 'nx:noop' },
          },
        },
      }),
    );
    await writeFile(
      join(workspace, 'packages/custom-emit/tsconfig.lib.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { rootDir: 'src', outDir: 'dist', types: [] },
        include: ['src/**/*.ts'],
      }),
    );
    await writeFile(join(workspace, 'packages/custom-emit/src/index.ts'), "export const bad: number = 'nope';\n");

    // Lib type error fails `nx run lib-pkg:typecheck` through `lib-pkg:tsc-js`.
    const libError = runNx('run', 'lib-pkg:typecheck');
    expect(libError.status, libError.output).not.toBe(0);
    expect(libError.output).toContain('lib-pkg:tsc-js');
    expect(libError.output).toContain('TS2322');

    // Fix lib error and put a type error in the test file: `typecheck` passes, `typecheck-tests` fails.
    await writeFile(join(workspace, 'packages/lib-pkg/src/index.ts'), 'export const broken: number = 1;\n');
    await writeFile(
      join(workspace, 'packages/lib-pkg/src/index.test.ts'),
      "import { broken } from './index.js';\nexport const tested: string = broken;\n",
    );
    const libFixed = runNx('run', 'lib-pkg:typecheck');
    expect(libFixed.status, libFixed.output).toBe(0);

    const testError = runNx('run', 'lib-pkg:typecheck-tests');
    expect(testError.status, testError.output).not.toBe(0);
    expect(testError.output).toContain('TS2322');

    // Emit-less project's type error fails its `typecheck`.
    const emitlessError = runNx('run', 'emitless:typecheck');
    expect(emitlessError.status, emitlessError.output).not.toBe(0);
    expect(emitlessError.output).toContain('TS2322');

    // Project with declared `tsc-js` still runs the real inferred `typecheck` and fails on lib type errors.
    const customError = runNx('run', 'custom-emit:typecheck');
    expect(customError.status, customError.output).not.toBe(0);
    expect(customError.output).toContain('TS2322');
  });
});
