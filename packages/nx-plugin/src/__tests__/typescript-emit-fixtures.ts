/**
 * The TypeScript emit executor's fixture projects (`executors/typescript-emit/executor.test.ts`), one per ttsc plugin
 * host its cases load, and the list of those hosts the `ttsc-plugin-hosts` target prepares.
 */
import { mkdir, mkdtemp, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

/** A project whose `typia` dependency puts it on ttsc's typia host, with no linked plugin, compiling `source`. */
export async function fixture(source: string) {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules'));
  await symlink(
    dirname(createRequire(import.meta.url).resolve('typia/package.json')),
    join(root, 'node_modules/typia'),
    'dir',
  );
  await writeFile(join(root, 'package.json'), JSON.stringify({ type: 'module', dependencies: { typia: '*' } }));
  await writeFile(
    join(root, 'tsconfig.lib.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        rootDir: 'src',
        outDir: 'dist',
        declaration: true,
        declarationMap: true,
        emitDeclarationOnly: true,
        types: [],
        skipLibCheck: true,
      },
      include: ['src/**/*.ts'],
    }),
  );
  await writeFile(join(root, 'src/index.ts'), source);
  return root;
}

/**
 * A project whose transform runs in ttsc's utility-plugin lane: lmao-ttsc's descriptor names a linked Go library, which
 * ttsc links into its utility host (a project with typia links it into typia's host instead). The lane is what this
 * fixture is for, not lmao's rewrite; the shards' inputs name that descriptor.
 */
export async function utilityPluginFixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-utility-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules/@smoothbricks'), { recursive: true });
  await symlink(join(import.meta.dir, '../../../lmao-ttsc'), join(root, 'node_modules/@smoothbricks/lmao-ttsc'), 'dir');
  await writeFile(join(root, 'package.json'), JSON.stringify({ type: 'module' }));
  await writeFile(
    join(root, 'tsconfig.lib.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        rootDir: 'src',
        outDir: 'dist',
        types: [],
        skipLibCheck: true,
        plugins: [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }],
      },
      include: ['src/**/*.ts'],
    }),
  );
  await writeFile(join(root, 'src/index.ts'), 'export const index = 1;\n');
  return root;
}

/**
 * A project whose linked plugin (`src/__tests__/side-output-plugin`) writes `side-output.json` into outDir, beside the
 * compiler's outputs, as a census-writing lint plugin writes its census. ttsc links the plugin into its utility host,
 * or into typia's host when the project uses typia.
 */
export async function sideOutputFixture(host: 'utility' | 'typia'): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-side-output-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules'));
  await symlink(join(import.meta.dir, 'side-output-plugin'), join(root, 'node_modules/side-output-plugin'), 'dir');
  await writeFile(
    join(root, 'package.json'),
    JSON.stringify(host === 'typia' ? { type: 'module', dependencies: { typia: '*' } } : { type: 'module' }),
  );
  if (host === 'typia') {
    await symlink(
      dirname(createRequire(import.meta.url).resolve('typia/package.json')),
      join(root, 'node_modules/typia'),
      'dir',
    );
  }
  await writeFile(
    join(root, 'tsconfig.lib.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        rootDir: 'src',
        outDir: 'dist',
        types: [],
        skipLibCheck: true,
        plugins: [{ transform: 'side-output-plugin' }],
      },
      include: ['src/**/*.ts'],
    }),
  );
  await writeFile(
    join(root, 'src/index.ts'),
    host === 'typia'
      ? "import typia from 'typia';\nexport const validate = typia.createValidate<{ value: number }>();\n"
      : 'export const index = 1;\n',
  );
  return root;
}

/**
 * A project whose linked plugin (`src/__tests__/dialect-plugin`) admits `for await (… of lowered)` in a generator and
 * lowers it to the sync loop. The checker refuses every `for await` in a generator (TS1103); the plugin withdraws the
 * refusal of its own dialect through the driver's DiagnosticPlugin seam (the managed ttsc patch), on ttsc's utility
 * host, or on typia's when the project uses typia.
 */
export async function dialectFixture(host: 'utility' | 'typia', iterated: 'lowered' | 'kept'): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-dialect-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules'));
  await symlink(join(import.meta.dir, 'dialect-plugin'), join(root, 'node_modules/dialect-plugin'), 'dir');
  await writeFile(
    join(root, 'package.json'),
    JSON.stringify(host === 'typia' ? { type: 'module', dependencies: { typia: '*' } } : { type: 'module' }),
  );
  if (host === 'typia') {
    await symlink(
      dirname(createRequire(import.meta.url).resolve('typia/package.json')),
      join(root, 'node_modules/typia'),
      'dir',
    );
  }
  await writeFile(
    join(root, 'tsconfig.lib.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        rootDir: 'src',
        outDir: 'dist',
        types: [],
        skipLibCheck: true,
        plugins: [{ transform: 'dialect-plugin' }],
      },
      include: ['src/**/*.ts'],
    }),
  );
  await writeFile(
    join(root, 'src/index.ts'),
    `declare const ${iterated}: Iterable<number>;\nexport function* values() {\n  for await (const value of ${iterated}) yield value;\n}\n`,
  );
  return root;
}

/**
 * Every ttsc plugin host the executor's cases load, as the fixture that loads it. ttsc names a host by a hash of what it
 * is built from (its plugins' sources, ttsc, tsgo and the Go toolchain), never by the project, so preparing these
 * fixtures builds exactly the binaries the cases then only load.
 */
export const PLUGIN_HOST_FIXTURES = {
  typia: () => fixture('export const index = 1;\n'),
  'lmao-ttsc on the utility host': utilityPluginFixture,
  'side-output-plugin on the utility host': () => sideOutputFixture('utility'),
  'side-output-plugin on the typia host': () => sideOutputFixture('typia'),
  'dialect-plugin on the utility host': () => dialectFixture('utility', 'lowered'),
  'dialect-plugin on the typia host': () => dialectFixture('typia', 'lowered'),
} as const satisfies Record<string, () => Promise<string>>;
