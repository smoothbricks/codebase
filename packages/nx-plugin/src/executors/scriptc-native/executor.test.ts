import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  type ArchiveInvocation,
  type EmitInvocation,
  runScriptcNative,
  type ScriptcInvocation,
  type ScriptcRunner,
  scriptcBuildArguments,
  untransformedTypiaCalls,
  zigArchiveCommands,
} from './executor.js';
import { ffiPathProblem } from './ffi-paths.js';

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function workspace(): { root: string; project: string } {
  const root = mkdtempSync(join(tmpdir(), 'scriptc-native-'));
  roots.push(root);
  const project = join(root, 'packages/tool');
  mkdirSync(project, { recursive: true });
  writeFileSync(join(project, 'tsconfig.lib.json'), '{"compilerOptions":{"rootDir":"src","outDir":"dist/ts"}}\n');
  return { root, project };
}

function write(directory: string, files: Record<string, string>): void {
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(directory, path)), { recursive: true });
    writeFileSync(join(directory, path), text);
  }
}

const options = {
  cwd: 'packages/tool',
  tsConfig: 'tsconfig.lib.json',
  triple: 'aarch64-linux-musl',
  outputDirectory: 'dist/bin/linux-arm64',
  entries: { 'smoo-nx-tool': 'bin/smoo-nx-tool.js' },
};

/** A runner whose emit writes `emitted` into the staging directory, recording what each step saw. */
function fake(
  emitted: Record<string, string>,
  builds: (count: number) => boolean,
  archived: (count: number) => boolean = () => true,
) {
  const emits: Array<EmitInvocation & { overlay: unknown }> = [];
  const archives: ArchiveInvocation[] = [];
  const runs: Array<ScriptcInvocation & { staged: string[]; manifest?: unknown; declaration?: string | undefined }> =
    [];
  const runner: ScriptcRunner = {
    emit: async (invocation) => {
      emits.push({ ...invocation, overlay: JSON.parse(readFileSync(invocation.tsconfig, 'utf8')) });
      write(invocation.outDir, emitted);
      return true;
    },
    archive: async (invocation) => {
      archives.push(invocation);
      const compiled = archived(archives.length);
      if (compiled) writeFileSync(invocation.archive, '');
      return compiled;
    },
    build: async (invocation) => {
      const staging = dirname(dirname(invocation.entry));
      runs.push({
        ...invocation,
        staged: readdirSync(staging).sort(),
        // What scriptc is handed, read while the run's staging still exists.
        manifest: invocation.ffi === undefined ? undefined : JSON.parse(readFileSync(invocation.ffi, 'utf8')),
        declaration: existsSync(join(staging, 'ffi.d.ts'))
          ? readFileSync(join(staging, 'ffi.d.ts'), 'utf8')
          : undefined,
      });
      const built = builds(runs.length);
      if (built) writeFileSync(invocation.output, '');
      return built;
    },
  };
  return { runner, emits, archives, runs };
}

describe('scriptc-native', () => {
  it('names every call through a typia binding that the transform left behind, and no runtime member read', () => {
    const { project } = workspace();
    write(join(project, 'staged'), {
      'bin/smoo-nx-tool.js': "import { run } from '../tool.js';\nrun();\n",
      'tool.js': [
        "import typia, { TypeGuardError } from 'typia';",
        'export const isEntry = typia.createIs();',
        'export function run(raw) {',
        '  return typia.json.assertParse(raw) instanceof TypeGuardError;',
        '  if (raw instanceof typia.TypeGuardError) throw new typia.TypeGuardError(raw);',
        '}',
        '',
      ].join('\n'),
      'namespace.js': "import * as t from 'typia/lib/index.js';\nexport const check = t['assert'](input);\n",
      'named.js': [
        "import { createIs, TypeGuardError } from 'typia';",
        'export const isEntry = createIs();',
        'export const isError = (error) => error instanceof TypeGuardError;',
        '',
      ].join('\n'),
      'destructured.js': [
        "import typia from 'typia';",
        'const { assertParse } = typia.json;',
        'export const parse = (raw) => assertParse(raw);',
        '',
      ].join('\n'),
      // ttsc keeps the import after replacing every call; a kept import is not a call.
      'transformed.js': "import typia from 'typia';\nexport const is = (input) => typeof input === 'string';\n",
      // The transform's output calls typia's internal helpers; those calls are the transform working.
      'helpers.js': [
        "import * as _validateReport_1 from 'typia/lib/internal/_validateReport';",
        "import { _accessExpressionAsString } from 'typia/lib/internal/_accessExpressionAsString.js';",
        'export const report = _validateReport_1._validateReport([]);',
        "export const path = _accessExpressionAsString('key');",
        '',
      ].join('\n'),
      // A name the module never imported from typia, and the text of a call, are not calls.
      'unrelated.js':
        "const typia = { createIs: () => true };\nexport const note = 'typia.createIs()' + typia.createIs();\n",
    });
    expect(
      untransformedTypiaCalls(join(project, 'staged')).map(
        (call) => `${call.file.slice(project.length + 1)}:${call.line}:${call.column} ${call.text}`,
      ),
    ).toEqual([
      'staged/destructured.js:3:31 assertParse',
      'staged/named.js:2:24 createIs',
      "staged/namespace.js:2:22 t['assert']",
      'staged/tool.js:2:24 typia.createIs',
      'staged/tool.js:4:10 typia.json.assertParse',
    ]);
  });

  it('compiles nothing while an untransformed typia call remains, and leaves no staging behind', async () => {
    const { root, project } = workspace();
    const { runner, runs } = fake(
      { 'bin/smoo-nx-tool.js': "import typia from 'typia';\nconsole.log(typia.is(process.argv[2]));\n" },
      () => true,
    );
    expect(await runScriptcNative(options, { root }, runner)).toEqual({ success: false });
    expect(runs).toEqual([]);
    expect(existsSync(join(project, 'dist/bin'))).toBe(false);
    expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    expect(readdirSync(project).sort()).toEqual(['.cache', 'tsconfig.lib.json']);
  });

  it("emits into its own staging, then builds each bin there for the target's triple, stopping at a failed build", async () => {
    const { root, project } = workspace();
    const three = { ...options, entries: { ...options.entries, other: 'bin/other.js', third: 'bin/third.js' } };
    const { runner, emits, runs } = fake(
      { 'bin/smoo-nx-tool.js': 'console.log(1);\n', 'bin/other.js': '' },
      (count) => count === 1,
    );
    expect(await runScriptcNative(three, { root }, runner)).toEqual({ success: false });
    const staging = emits[0]?.outDir ?? '';
    expect(dirname(staging)).toBe(join(project, '.cache/scriptc'));
    expect(emits).toEqual([
      {
        tsconfig: expect.stringMatching(/\/packages\/tool\/\.cache\/scriptc\/aarch64-linux-musl-[^/]+\.json$/),
        outDir: staging,
        cwd: project,
        // An overlay of the package's own emit config, writing only JavaScript, into staging.
        overlay: {
          extends: '../../tsconfig.lib.json',
          compilerOptions: {
            outDir: staging,
            composite: false,
            declaration: false,
            declarationMap: false,
            emitDeclarationOnly: false,
            incremental: false,
            sourceMap: false,
          },
        },
      },
    ]);
    // scriptc type-checks the staged JavaScript under the strict config and module format written beside it.
    const staged = ['bin', 'package.json', 'tsconfig.json'];
    expect(runs).toEqual([
      {
        entry: join(staging, 'bin/smoo-nx-tool.js'),
        output: join(project, 'dist/bin/linux-arm64/smoo-nx-tool'),
        cwd: project,
        triple: 'aarch64-linux-musl',
        staged,
      },
      {
        entry: join(staging, 'bin/other.js'),
        output: join(project, 'dist/bin/linux-arm64/other'),
        cwd: project,
        triple: 'aarch64-linux-musl',
        staged,
      },
    ]);
    // The overlay and the staging are gone, and so is the executable the first bin wrote: the run failed.
    expect(readdirSync(join(project, 'dist/bin/linux-arm64'))).toEqual([]);
    expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    expect(readdirSync(project).sort()).toEqual(['.cache', 'dist', 'tsconfig.lib.json']);
  });

  describe("a package that calls C through scriptc's FFI", () => {
    const ffi = { manifest: 'ffi.json', declarations: { 'src/ffi.d.ts': 'ffi.d.ts' }, sources: ['native/lock.c'] };
    const manifest = {
      ffi_format: 6,
      functions: [{ name: 'tryLockFd', symbol: 'try_lock', params: ['i32'], returns: 'i32' }],
      libraries: [],
      system_libraries: [],
    };
    const files = {
      'ffi.json': JSON.stringify(manifest),
      'src/ffi.d.ts': 'declare function tryLockFd(fd: number): number;\n',
      'native/lock.c': 'int try_lock(int fd) { return fd; }\n',
    };

    it('passes the staged manifest to scriptc as --ffi, and nothing for a package without C', () => {
      const build = {
        entry: '/stage/bin/tool.js',
        output: '/p/dist/bin/tool',
        cwd: '/p',
        triple: 'aarch64-linux-musl',
      };
      const base = ['build', '/stage/bin/tool.js', '-o', '/p/dist/bin/tool', '--strip', '--no-keep-llvm'];
      expect(scriptcBuildArguments(build)).toEqual(base);
      expect(scriptcBuildArguments({ ...build, ffi: '/stage/ffi.json' })).toEqual([
        ...base,
        '--ffi',
        '/stage/ffi.json',
      ]);
    });

    it('compiles each C source for the triple with zig, then archives the objects in one step', () => {
      expect(
        zigArchiveCommands({
          sources: ['/p/native/a.c', '/p/native/b.c'],
          archive: '/stage/native/ffi.a',
          cwd: '/p',
          triple: 'aarch64-linux-musl',
        }),
      ).toEqual([
        [
          'cc',
          '-target',
          'aarch64-linux-musl',
          '-O2',
          '-fno-sanitize=undefined',
          '-c',
          '/p/native/a.c',
          '-o',
          '/stage/native/ffi.a.0.o',
        ],
        [
          'cc',
          '-target',
          'aarch64-linux-musl',
          '-O2',
          '-fno-sanitize=undefined',
          '-c',
          '/p/native/b.c',
          '-o',
          '/stage/native/ffi.a.1.o',
        ],
        ['ar', 'rcs', '/stage/native/ffi.a', '/stage/native/ffi.a.0.o', '/stage/native/ffi.a.1.o'],
      ]);
    });

    it('stages the declarations, compiles the C once for the triple, and hands scriptc a manifest that links the archive', async () => {
      const { root, project } = workspace();
      write(project, files);
      const { runner, archives, runs } = fake({ 'bin/smoo-nx-tool.js': 'console.log(1);\n' }, () => true);
      expect(await runScriptcNative({ ...options, ffi }, { root }, runner)).toEqual({ success: true });
      const staging = dirname(dirname(runs[0]?.entry ?? ''));
      expect(archives).toEqual([
        {
          sources: [join(project, 'native/lock.c')],
          archive: join(staging, 'native/ffi.a'),
          cwd: project,
          triple: 'aarch64-linux-musl',
        },
      ]);
      // The declaration sits beside the emitted JavaScript, where the module that references it looks.
      expect(runs[0]?.staged).toEqual(['bin', 'ffi.d.ts', 'ffi.json', 'native', 'package.json', 'tsconfig.json']);
      expect(runs[0]?.declaration).toBe(files['src/ffi.d.ts']);
      // scriptc gets a copy of the manifest: every field kept, and the archive this run built linked as its only
      // library.
      expect(runs[0]?.ffi).toBe(join(staging, 'ffi.json'));
      expect(runs[0]?.manifest).toEqual({
        ...manifest,
        libraries: [join(staging, 'native/ffi.a')],
      });
      expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    });

    it('compiles each bin against that one manifest, and compiles no C for a package that has none', async () => {
      const { root, project } = workspace();
      write(project, files);
      const two = { ...options, entries: { ...options.entries, other: 'bin/other.js' } };
      const { runner, archives, runs } = fake({ 'bin/smoo-nx-tool.js': '', 'bin/other.js': '' }, () => true);
      expect(await runScriptcNative({ ...two, ffi }, { root }, runner)).toEqual({ success: true });
      expect(archives).toHaveLength(1);
      expect(runs.map((run) => run.ffi)).toEqual([runs[0]?.ffi, runs[0]?.ffi]);
      const plain = fake({ 'bin/smoo-nx-tool.js': '' }, () => true);
      expect(await runScriptcNative(options, { root }, plain.runner)).toEqual({ success: true });
      expect(plain.archives).toEqual([]);
      expect(plain.runs[0]?.ffi).toBeUndefined();
    });

    it('builds nothing and writes no executable when the C does not compile', async () => {
      const { root, project } = workspace();
      write(project, files);
      const { runner, archives, runs } = fake(
        { 'bin/smoo-nx-tool.js': '' },
        () => true,
        () => false,
      );
      expect(await runScriptcNative({ ...options, ffi }, { root }, runner)).toEqual({ success: false });
      expect(archives).toHaveLength(1);
      expect(runs).toEqual([]);
      expect(existsSync(join(project, 'dist'))).toBe(false);
      expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    });

    it('refuses a manifest that names a prebuilt library: every platform builds the one manifest', async () => {
      const { root, project } = workspace();
      write(project, { ...files, 'ffi.json': JSON.stringify({ ...manifest, libraries: ['./vendor/libextra.a'] }) });
      const { runner, archives, runs } = fake({ 'bin/smoo-nx-tool.js': '' }, () => true);
      await expect(runScriptcNative({ ...options, ffi }, { root }, runner)).rejects.toThrow(
        `${join(project, 'ffi.json')}: lists libraries (./vendor/libextra.a)`,
      );
      expect(archives).toEqual([]);
      expect(runs).toEqual([]);
      expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    });

    it('names the first path that leaves its root, and accepts one that winds but stays beneath it', () => {
      expect(ffiPathProblem(ffi)).toBeUndefined();
      expect(ffiPathProblem({ ...ffi, sources: ['./native/../native/lock.c', '..lock.c'] })).toBeUndefined();
      for (const path of ['', '.', '..', '../lock.c', 'native/../../lock.c', '/tmp/lock.c', '..\\lock.c']) {
        expect(ffiPathProblem({ ...ffi, sources: [path] })).toBe(
          `names ${JSON.stringify(path)}, which is not a path beneath the project`,
        );
      }
      expect(ffiPathProblem({ ...ffi, manifest: '../ffi.json' })).toBe(
        'names "../ffi.json", which is not a path beneath the project',
      );
      expect(ffiPathProblem({ ...ffi, declarations: { '../ffi.d.ts': 'ffi.d.ts' } })).toBe(
        'names "../ffi.d.ts", which is not a path beneath the project',
      );
      expect(ffiPathProblem({ ...ffi, declarations: { 'src/ffi.d.ts': '../../escape.d.ts' } })).toBe(
        'names "../../escape.d.ts", which is not a path beneath the emitted JavaScript\'s root',
      );
    });

    it('refuses a path that leaves its root before it creates or copies anything', async () => {
      const { root, project } = workspace();
      write(project, files);
      const { runner, archives } = fake({ 'bin/smoo-nx-tool.js': '' }, () => true);
      await expect(
        runScriptcNative(
          { ...options, ffi: { ...ffi, declarations: { 'src/ffi.d.ts': '../../escape.d.ts' } } },
          { root },
          runner,
        ),
      ).rejects.toThrow(
        'scriptc-native: ffi names "../../escape.d.ts", which is not a path beneath the emitted JavaScript\'s root',
      );
      await expect(
        runScriptcNative({ ...options, ffi: { ...ffi, sources: ['/tmp/lock.c'] } }, { root }, runner),
      ).rejects.toThrow('scriptc-native: ffi names "/tmp/lock.c", which is not a path beneath the project');
      expect(archives).toEqual([]);
      // No staging, no overlay, and nothing beside them for the run's cleanup to miss.
      expect(existsSync(join(project, '.cache'))).toBe(false);
    });
    it('refuses a manifest that lists no libraries, naming the file, and leaves no staging behind', async () => {
      const { root, project } = workspace();
      write(project, { ...files, 'ffi.json': '{"ffi_format":6}' });
      const { runner, archives } = fake({ 'bin/smoo-nx-tool.js': '' }, () => true);
      await expect(runScriptcNative({ ...options, ffi }, { root }, runner)).rejects.toThrow(
        `${join(project, 'ffi.json')}: an FFI manifest lists its libraries`,
      );
      expect(archives).toEqual([]);
      expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    });
  });
});
