import { describe, expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { BuildInputRecord } from '@smoothbricks/build-inputs';
import { EMIT_OVERRIDES } from './emit-overrides.js';
import typescriptEmitExecutor, {
  type CompilerInvocation,
  type CompilerOutcome,
  readVerboseEmit,
  runTypeScriptEmit,
} from './executor.js';

async function fixture(source: string) {
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
async function utilityPluginFixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-utility-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules/@smoothbricks'), { recursive: true });
  await symlink(
    join(import.meta.dir, '../../../../lmao-ttsc'),
    join(root, 'node_modules/@smoothbricks/lmao-ttsc'),
    'dir',
  );
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
async function sideOutputFixture(host: 'utility' | 'typia'): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-side-output-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules'));
  await symlink(
    join(import.meta.dir, '../../__tests__/side-output-plugin'),
    join(root, 'node_modules/side-output-plugin'),
    'dir',
  );
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

/** Every file under `directory` with what a write changes: its inode, modification time and status-change time. */
async function fileIdentities(directory: string): Promise<Record<string, readonly bigint[]>> {
  const identities: Record<string, readonly bigint[]> = {};
  for (const name of await readdir(directory, { recursive: true })) {
    const stats = await stat(join(directory, name), { bigint: true });
    if (stats.isFile()) identities[name] = [stats.ino, stats.mtimeNs, stats.ctimeNs];
  }
  return identities;
}

/** What a compiler that emitted nothing answers. */
const EMITTED_NOTHING: CompilerOutcome = { success: true, emitted: new Set() };

describe('@smoothbricks/nx-plugin TypeScript emit executor', () => {
  it('emits executable transformed validation and declarations from the same program, and nothing an earlier build left', async () => {
    const root = await fixture(
      "#!/usr/bin/env node\nimport typia from 'typia';\nexport const validate = typia.createValidate<{ value: number }>();\n",
    );
    try {
      // What an earlier build left: a bin since moved to another package, and the output of a since-deleted module.
      await mkdir(join(root, 'dist/bin'), { recursive: true });
      await writeFile(join(root, 'dist/bin/moved.js'), 'export {};\n');
      await writeFile(join(root, 'dist/deleted.js'), 'export {};\n');
      expect(
        await typescriptEmitExecutor(
          { cwd: root, tsConfig: 'tsconfig.lib.json', executableOutputs: ['dist/index.js'] },
          { root },
        ),
      ).toEqual({ success: true });
      expect((await stat(join(root, 'dist/index.js'))).mode & 0o111).toBe(0o111);
      expect((await readdir(join(root, 'dist'))).sort()).toEqual(['index.d.ts', 'index.d.ts.map', 'index.js']);
      expect((await readdir(root)).sort()).toEqual([
        'dist',
        'node_modules',
        'package.json',
        'src',
        'tsconfig.lib.json',
      ]);
      const child = Bun.spawn(
        [
          'bun',
          '-e',
          "import { validate } from './dist/index.js'; process.stdout.write(JSON.stringify([validate({value: 7}).success, validate({value: 'bad'}).success]))",
        ],
        { cwd: root, stdout: 'pipe', stderr: 'pipe' },
      );
      const [text, errors, exit] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
      ]);
      expect(exit, errors).toBe(0);
      expect(text).toBe('[true,false]');
      // A second emit of the same source: ttsc writes no output that already holds its bytes and the bin keeps its
      // execute bits, so every output keeps its file (inode, mtime and ctime), and the removal keeps them all.
      const emitted = await fileIdentities(join(root, 'dist'));
      expect(
        await typescriptEmitExecutor(
          { cwd: root, tsConfig: 'tsconfig.lib.json', executableOutputs: ['dist/index.js'] },
          { root },
        ),
      ).toEqual({ success: true });
      expect(await fileIdentities(join(root, 'dist'))).toEqual(emitted);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("removes a deleted module's outputs and leaves every other output's file as it was", async () => {
    const root = await fixture(
      "import typia from 'typia';\nexport const validate = typia.createValidate<{ value: number }>();\n",
    );
    try {
      await writeFile(join(root, 'src/gone.ts'), 'export const gone = 1;\n');
      const emit = () => typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.lib.json' }, { root });
      expect(await emit()).toEqual({ success: true });
      const before = await fileIdentities(join(root, 'dist'));
      expect(Object.keys(before).sort()).toEqual([
        'gone.d.ts',
        'gone.d.ts.map',
        'gone.js',
        'index.d.ts',
        'index.d.ts.map',
        'index.js',
      ]);
      await rm(join(root, 'src/gone.ts'));
      expect(await emit()).toEqual({ success: true });
      expect(await fileIdentities(join(root, 'dist'))).toEqual({
        'index.d.ts': before['index.d.ts'],
        'index.d.ts.map': before['index.d.ts.map'],
        'index.js': before['index.js'],
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it.each(['utility', 'typia'] as const)(
    'keeps a side output a linked plugin writes into outDir across emits, in the %s host',
    async (host) => {
      const root = await sideOutputFixture(host);
      try {
        const emit = () => typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.lib.json' }, { root });
        for (const run of [1, 2, 3]) {
          expect(await emit(), `emit ${String(run)}`).toEqual({ success: true });
          expect(
            await readFile(join(root, 'dist/side-output.json'), 'utf8').catch(() => 'absent'),
            `emit ${String(run)}`,
          ).toBe('{"side":true}\n');
        }
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    },
  );
  it("reads the emitted files of ttsc's utility-plugin lane, keeps its unchanged outputs and prunes a deleted module's", async () => {
    const root = await utilityPluginFixture();
    try {
      await writeFile(join(root, 'src/gone.ts'), 'export const gone = 1;\n');
      // The lane this case holds: ttsc's utility host prints the --verbose summary under its own label.
      const lane = spawnSync('ttsc', ['-p', 'tsconfig.lib.json', '--emit', '--verbose', ...EMIT_OVERRIDES], {
        cwd: root,
        encoding: 'utf8',
      });
      expect(lane.status, lane.stderr).toBe(0);
      expect(lane.stdout).toContain('// ttsc utility: ');
      const emit = () => typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.lib.json' }, { root });
      expect(await emit()).toEqual({ success: true });
      const before = await fileIdentities(join(root, 'dist'));
      expect(Object.keys(before).sort()).toEqual([
        'gone.d.ts',
        'gone.d.ts.map',
        'gone.js',
        'index.d.ts',
        'index.d.ts.map',
        'index.js',
      ]);
      await rm(join(root, 'src/gone.ts'));
      expect(await emit()).toEqual({ success: true });
      expect(await fileIdentities(join(root, 'dist'))).toEqual({
        'index.d.ts': before['index.d.ts'],
        'index.d.ts.map': before['index.d.ts.map'],
        'index.js': before['index.js'],
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps every output of an executor that prunes by file identity, which does not opt ttsc into write-if-changed', async () => {
    const root = await fixture(
      "import typia from 'typia';\nexport const validate = typia.createValidate<{ value: number }>();\n",
    );
    try {
      // The published executor's rule (nx-plugin 0.7.9 and earlier, still in any checkout whose dist predates this one):
      // emit with plain ttsc, then remove each outDir file whose inode, ctime and mtime the emit left as they were.
      const identityPruningEmit = async (): Promise<void> => {
        const before = new Map<string, readonly bigint[]>();
        for (const name of await readdir(join(root, 'dist'), { recursive: true })) {
          const stats = await lstat(join(root, 'dist', name), { bigint: true });
          if (stats.isFile()) before.set(name, [stats.ino, stats.ctimeNs, stats.mtimeNs]);
        }
        const { TTSC_WRITE_IF_CHANGED: _optIn, ...inherited } = process.env;
        const ttsc = spawnSync('ttsc', ['-p', 'tsconfig.lib.json', '--emit', ...EMIT_OVERRIDES], {
          cwd: root,
          env: inherited,
          encoding: 'utf8',
        });
        expect(ttsc.status, ttsc.stderr).toBe(0);
        for (const [name, identity] of before) {
          const now = await lstat(join(root, 'dist', name), { bigint: true });
          if (now.ino === identity[0] && now.ctimeNs === identity[1] && now.mtimeNs === identity[2]) {
            await rm(join(root, 'dist', name));
          }
        }
      };
      await mkdir(join(root, 'dist'));
      await identityPruningEmit();
      await identityPruningEmit();
      expect((await readdir(join(root, 'dist'))).sort()).toEqual(['index.d.ts', 'index.d.ts.map', 'index.js']);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports real type errors and removes nothing from the previous output instead of claiming a successful build', async () => {
    const root = await fixture("export const amount: number = 'not a number';\n");
    try {
      await mkdir(join(root, 'dist'));
      await writeFile(join(root, 'dist/previous.js'), 'export const amount = 1;\n');
      expect(await typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.lib.json' }, { root })).toEqual({
        success: false,
      });
      expect(await readdir(join(root, 'dist'))).toContain('previous.js');
      expect((await readdir(root)).sort()).toEqual([
        'dist',
        'node_modules',
        'package.json',
        'src',
        'tsconfig.lib.json',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

const GIT_IDENTITY = ['-c', 'user.name=fixture', '-c', 'user.email=fixture@example.test'];

/** A workspace's `packages/lib`: a config that emits its `src` less the tests, a source and a test. */
const LIB = {
  'packages/lib/tsconfig.lib.json': JSON.stringify({
    compilerOptions: { outDir: 'dist' },
    include: ['src'],
    exclude: ['src/**/*.test.ts'],
  }),
  'packages/lib/src/index.ts': 'export const index = 1;\n',
  'packages/lib/src/index.test.ts': "import { index } from './index.js';\n\nexport const checked = index;\n",
};

function git(root: string, ...args: string[]): void {
  execFileSync('git', ['-C', root, ...GIT_IDENTITY, ...args], { stdio: 'pipe' });
}

function sha256(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}

/** Runs `body` in a Git work tree of `LIB`, committed, in a new physical temp directory removed however it ends. */
async function inLibWorkspace(body: (root: string) => unknown): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-inputs-')));
  try {
    for (const [path, text] of Object.entries(LIB)) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), text);
    }
    git(root, 'init', '-q');
    git(root, 'add', '.');
    git(root, 'commit', '-qm', 'fixture');
    await body(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

describe('@smoothbricks/nx-plugin TypeScript emit build input record', () => {
  it('records the sources and config the emit reads, and the pathspecs that complete them', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      expect(await runTypeScriptEmit(options, { root }, async () => EMITTED_NOTHING)).toEqual({ success: true });
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(record.version).toBe(1);
      expect('producer' in record).toBe(false);
      expect(record.files).toEqual({
        'packages/lib/src/index.ts': sha256(LIB['packages/lib/src/index.ts']),
        'packages/lib/tsconfig.lib.json': sha256(LIB['packages/lib/tsconfig.lib.json']),
      });
      expect(record.complete).toContain(':(glob)packages/lib/src/**/*.ts');
      expect(record.complete).toContain(':(exclude,glob)packages/lib/src/**/*.test.ts');
    });
  });

  it('records the bytes the emit began with when a source is edited while it runs', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      const edited = 'export const index = 2;\n';
      const editDuringEmit = async () => {
        await writeFile(join(lib, 'src/index.ts'), edited);
        return EMITTED_NOTHING;
      };
      const producer = { project: 'lib', target: 'tsc-js', configuration: 'development' };
      expect(await runTypeScriptEmit(options, { root }, editDuringEmit, producer)).toEqual({ success: true });
      expect(await readFile(join(lib, 'src/index.ts'), 'utf8')).toBe(edited);
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(record.files['packages/lib/src/index.ts']).toBe(sha256(LIB['packages/lib/src/index.ts']));
      expect(record.producer).toEqual(producer);
    });
  });

  it('records the files that exist when a tracked source was deleted without staging it', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      await writeFile(join(lib, 'src/old.ts'), 'export const old = 1;\n');
      git(root, 'add', '.');
      git(root, 'commit', '-qm', 'old');
      // What a plain `rm` leaves: gone from the work tree, still in the index.
      await rm(join(lib, 'src/old.ts'));
      expect(await runTypeScriptEmit(options, { root }, async () => EMITTED_NOTHING)).toEqual({ success: true });
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(Object.keys(record.files)).toEqual(['packages/lib/src/index.ts', 'packages/lib/tsconfig.lib.json']);
    });
  });

  it('writes nothing beside the tsconfig while the compiler runs, and hands ttsc that tsconfig itself', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const before = (await readdir(lib)).sort();
      let during: string[] = [];
      let invocation: CompilerInvocation | undefined;
      const observe = async (call: CompilerInvocation) => {
        invocation = call;
        during = (await readdir(lib)).sort();
        return EMITTED_NOTHING;
      };
      expect(await runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, observe)).toEqual({
        success: true,
      });
      // Another project's ttsc transform proves this directory's listing while it compiles.
      expect(during).toEqual(before);
      expect(invocation?.args.slice(0, 3)).toEqual(['-p', join(lib, 'tsconfig.lib.json'), '--emit']);
      // ttsc leaves an output that already holds its bytes alone only for a caller that prunes by its emitted-file list.
      expect(invocation?.env).toEqual({ TTSC_WRITE_IF_CHANGED: '1' });
    });
  });
  it('refuses to prune, and fails, when ttsc names an output that does not exist', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      await mkdir(join(lib, 'dist'), { recursive: true });
      await writeFile(join(lib, 'dist/index.js'), 'export const index = 1;\n');
      // A list that names a file the emit did not leave cannot tell an output from a file an earlier build left.
      const emit = async (): Promise<CompilerOutcome> => ({
        success: true,
        emitted: new Set([join(lib, 'dist/missing.js')]),
      });
      expect(await runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, emit)).toEqual({
        success: false,
      });
      expect(await readdir(join(lib, 'dist'))).toEqual(['index.js']);
    });
  });
  it('removes only the outputs the emit no longer produces, and the directories that leaves empty', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      await mkdir(join(lib, 'dist/bin'), { recursive: true });
      await writeFile(join(lib, 'dist/bin/moved.js'), 'export {};\n');
      await writeFile(join(lib, 'dist/index.js'), 'export const index = 1;\n');
      await writeFile(join(lib, 'dist/deleted.js'), 'export {};\n');
      const index = await stat(join(lib, 'dist/index.js'), { bigint: true });
      // The compiler leaves an output whose bytes are unchanged as it is, writes what it newly produces, and names both.
      const emit = async (): Promise<CompilerOutcome> => {
        await writeFile(join(lib, 'dist/added.js'), 'export {};\n');
        return { success: true, emitted: new Set([join(lib, 'dist/index.js'), join(lib, 'dist/added.js')]) };
      };
      expect(await runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, emit)).toEqual({
        success: true,
      });
      expect((await readdir(join(lib, 'dist'))).sort()).toEqual(['.build-inputs.json', 'added.js', 'index.js']);
      const kept = await stat(join(lib, 'dist/index.js'), { bigint: true });
      expect([kept.ino, kept.mtimeNs]).toEqual([index.ino, index.mtimeNs]);
    });
  });

  it('keeps the input record file when a rebuild records the same inputs', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      expect(await runTypeScriptEmit(options, { root }, async () => EMITTED_NOTHING)).toEqual({ success: true });
      const record = await stat(join(lib, 'dist/.build-inputs.json'), { bigint: true });
      expect(await runTypeScriptEmit(options, { root }, async () => EMITTED_NOTHING)).toEqual({ success: true });
      const again = await stat(join(lib, 'dist/.build-inputs.json'), { bigint: true });
      expect([again.ino, again.mtimeNs]).toEqual([record.ino, record.mtimeNs]);
    });
  });

  it('refuses an outDir that holds a TypeScript source before the compiler runs', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      await mkdir(join(lib, 'dist'));
      await writeFile(join(lib, 'dist/kept.ts'), 'export const kept = 1;\n');
      let compiled = false;
      const emit = async () => {
        compiled = true;
        return EMITTED_NOTHING;
      };
      await expect(runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, emit)).rejects.toThrow(
        `holds the TypeScript source ${join(lib, 'dist/kept.ts')}`,
      );
      expect(compiled).toBe(false);
      expect(await readdir(join(lib, 'dist'))).toEqual(['kept.ts']);
    });
  });

  it('writes no record when the compiler fails', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      expect(await runTypeScriptEmit(options, { root }, async () => ({ success: false }))).toEqual({ success: false });
      await expect(stat(join(lib, 'dist/.build-inputs.json'))).rejects.toMatchObject({ code: 'ENOENT' });
    });
  });
});

describe('ttsc --verbose summary', () => {
  it('names the emitted files from the summary cwd of any host, keeps the other output, and refuses a list it cannot trust', () => {
    const summary = [
      'checked',
      '// ttsc: tsconfig=/w/p/tsconfig.lib.json cwd=/w/p sites=0 emit=true',
      '// ttsc: emitted=2 files',
      '  + dist/a.js',
      '  + dist/a.d.ts',
      '',
    ].join('\n');
    const read = readVerboseEmit(summary);
    expect([...read.emitted].sort()).toEqual(['/w/p/dist/a.d.ts', '/w/p/dist/a.js']);
    expect(read.output).toBe(
      'checked\n// ttsc: tsconfig=/w/p/tsconfig.lib.json cwd=/w/p sites=0 emit=true\n// ttsc: emitted=2 files\n',
    );
    expect(() => readVerboseEmit(summary.replace('emitted=2', 'emitted=3'))).toThrow('ttsc --verbose');
    expect(() => readVerboseEmit('checked\n')).toThrow('ttsc --verbose');
    const typia = readVerboseEmit(
      '// ttsc-typia build: tsconfig=/w/q/tsconfig.json cwd=/w/q emit=true rewrite=typia\n// ttsc-typia build: emitted=1 files\n  + dist/index.js\n',
    );
    expect([...typia.emitted]).toEqual(['/w/q/dist/index.js']);
  });
});
