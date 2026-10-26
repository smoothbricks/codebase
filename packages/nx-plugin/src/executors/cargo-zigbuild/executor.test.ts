import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import { BUILD_INPUTS_FILE, type BuildInputRecord, cargoBuildInputs } from '@smoothbricks/build-inputs';
import { outsideTreesTmpdir } from '@smoothbricks/cargo-retain/testing/outside-trees';

import {
  artifactFile,
  buildInputsOf,
  cargoTriple,
  depInfoPath,
  publishArtifact,
  type ZigbuildArtifact,
  zigbuildArgs,
  zigbuildEnv,
} from './executor.js';

const MANIFEST = '/repo/packages/cowshed/crates/cowshed-cli/Cargo.toml';

/** One `compiler-artifact` line in the shape cargo prints it, trimmed to stable fields. */
function artifactLine(fields: {
  manifest: string;
  kind: string[];
  name: string;
  filenames: string[];
  executable: string | null;
}): string {
  return JSON.stringify({
    reason: 'compiler-artifact',
    package_id: 'path+file:///repo/packages/cowshed/crates/cowshed-cli#0.0.1',
    manifest_path: fields.manifest,
    target: { kind: fields.kind, crate_types: fields.kind, name: fields.name, src_path: '/repo/src/main.rs' },
    profile: { opt_level: '3', debuginfo: 0, debug_assertions: false, overflow_checks: false, test: false },
    features: [],
    filenames: fields.filenames,
    executable: fields.executable,
    fresh: false,
  });
}

/** Runs `body` in a new physical directory outside any Git work tree, removed however `body` ends. */
async function inTemporaryDirectory(body: (root: string) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'smoo-cargo-zigbuild-')));
  try {
    await body(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

describe('cargo-zigbuild executor', () => {
  it('runs cargo-zigbuild directly, locked, with the pinned glibc suffix on the target', () => {
    expect(
      zigbuildArgs(
        { target: 'x86_64-unknown-linux-gnu.2.17', manifestPath: 'm', package: 'cowshed-napi', output: 'o' },
        MANIFEST,
      ),
    ).toEqual([
      'zigbuild',
      '--locked',
      '--message-format=json-render-diagnostics',
      '--target',
      'x86_64-unknown-linux-gnu.2.17',
      '--manifest-path',
      MANIFEST,
      '--package',
      'cowshed-napi',
      '--lib',
    ]);
  });

  it('selects the named binary and the release profile when asked', () => {
    const args = zigbuildArgs(
      {
        target: 'aarch64-unknown-linux-musl',
        manifestPath: 'm',
        package: 'cowshed-cli',
        bin: 'cowshed',
        output: 'o',
        release: true,
      },
      MANIFEST,
    );
    expect(args.slice(-3)).toEqual(['--release', '--bin', 'cowshed']);
    expect(args).not.toContain('--lib');
  });

  it('names the bare triple cargo sees under a glibc suffix', () => {
    expect(cargoTriple('x86_64-unknown-linux-gnu.2.17')).toBe('x86_64-unknown-linux-gnu');
    expect(cargoTriple('aarch64-unknown-linux-musl')).toBe('aarch64-unknown-linux-musl');
  });

  it("drops the shell's compilers for the artifact's triple and keeps everything else", () => {
    const env = zigbuildEnv(
      {
        CC_x86_64_unknown_linux_gnu: '/nix/store/cc-wrapper/bin/cc',
        'CC_x86_64-unknown-linux-gnu': '/nix/store/cc-wrapper/bin/cc',
        CXX_x86_64_unknown_linux_gnu: '/nix/store/cc-wrapper/bin/c++',
        AR_x86_64_unknown_linux_gnu: 'ar',
        RANLIB_x86_64_unknown_linux_gnu: 'ranlib',
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER: 'cc',
        CC_aarch64_unknown_linux_gnu: 'kept-other-triple',
        CC: 'kept-generic',
        PATH: '/bin',
      },
      'x86_64-unknown-linux-gnu.2.17',
    );
    expect(env).toEqual({ CC_aarch64_unknown_linux_gnu: 'kept-other-triple', CC: 'kept-generic', PATH: '/bin' });
  });

  it("reads the binary's executable from its artifact message", () => {
    const line = artifactLine({
      manifest: MANIFEST,
      kind: ['bin'],
      name: 'cowshed',
      filenames: ['/repo/target/x86_64-unknown-linux-musl/release/cowshed'],
      executable: '/repo/target/x86_64-unknown-linux-musl/release/cowshed',
    });
    expect(artifactFile(line, MANIFEST, { kind: 'bin', name: 'cowshed' })).toBe(
      '/repo/target/x86_64-unknown-linux-musl/release/cowshed',
    );
    expect(artifactFile(line, MANIFEST, { kind: 'bin', name: 'other' })).toBeNull();
    expect(artifactFile(line, '/repo/elsewhere/Cargo.toml', { kind: 'bin', name: 'cowshed' })).toBeNull();
  });

  it("reads the cdylib's shared object, not the rlib beside it", () => {
    const line = artifactLine({
      manifest: MANIFEST,
      kind: ['cdylib', 'rlib'],
      name: 'cowshed_napi',
      filenames: [
        '/repo/target/x86_64-unknown-linux-gnu/debug/libcowshed_napi.rlib',
        '/repo/target/x86_64-unknown-linux-gnu/debug/libcowshed_napi.so',
      ],
      executable: null,
    });
    expect(artifactFile(line, MANIFEST, { kind: 'cdylib' })).toBe(
      '/repo/target/x86_64-unknown-linux-gnu/debug/libcowshed_napi.so',
    );
  });

  it('ignores the other messages cargo prints', () => {
    expect(artifactFile('{"reason":"build-finished","success":true}', MANIFEST, { kind: 'cdylib' })).toBeNull();
    expect(artifactFile('', MANIFEST, { kind: 'cdylib' })).toBeNull();
  });

  it('refuses an artifact message that breaks the contract instead of skipping it', () => {
    expect(() =>
      artifactFile('{"reason":"compiler-artifact","manifest_path":7}', MANIFEST, { kind: 'cdylib' }),
    ).toThrow('does not match the artifact contract');
  });

  it('finds the dep-info cargo uplifts beside an artifact: its file name with any extension replaced by .d', () => {
    const uplifted: [artifact: string, depInfo: string][] = [
      // The static CLI has no extension; the addon's shared object and a Windows executable lose theirs.
      [
        '/repo/target/x86_64-unknown-linux-musl/release/cowshed',
        '/repo/target/x86_64-unknown-linux-musl/release/cowshed.d',
      ],
      [
        '/repo/target/x86_64-unknown-linux-gnu/debug/libcowshed_napi.so',
        '/repo/target/x86_64-unknown-linux-gnu/debug/libcowshed_napi.d',
      ],
      [
        '/repo/target/x86_64-pc-windows-gnu/release/cowshed.exe',
        '/repo/target/x86_64-pc-windows-gnu/release/cowshed.d',
      ],
      // A dot in a directory is no extension of the file.
      ['/repo/v1.2/target/release/cowshed', '/repo/v1.2/target/release/cowshed.d'],
    ];
    for (const [artifact, depInfo] of uplifted) expect(depInfoPath(artifact)).toBe(depInfo);
  });

  it('names the inputs of a build from the dep-info beside its artifact, or why it cannot', async () => {
    await inTemporaryDirectory(async (root) => {
      const manifest = join(root, 'crates/cli/Cargo.toml');
      const built = join(root, 'target/x86_64-unknown-linux-musl/release/cowshed');
      const depInfo = join(root, 'target/x86_64-unknown-linux-musl/release/cowshed.d');
      await mkdir(dirname(manifest), { recursive: true });
      // Outside a Git work tree there is nothing to name an input relative to.
      expect(buildInputsOf(built, manifest, root)).toEqual({
        ok: false,
        error: `${dirname(manifest)} is not in a Git work tree, so no input can be named`,
      });
      execFileSync('git', ['init', '-q', root]);
      expect(buildInputsOf(built, manifest, root)).toEqual({
        ok: false,
        error: `${depInfo} does not exist: cargo wrote no dep-info beside ${built}`,
      });
      const files = {
        '.gitignore': 'target/\n',
        'Cargo.toml': '[workspace]\nmembers = ["crates/cli"]\n',
        'Cargo.lock': '# lock\n',
        'crates/cli/Cargo.toml': '[package]\nname = "cli"\n',
        'crates/cli/src/main.rs': 'fn main() {}\n',
      };
      for (const [file, text] of Object.entries(files)) {
        await mkdir(dirname(join(root, file)), { recursive: true });
        await writeFile(join(root, file), text);
      }
      await mkdir(dirname(depInfo), { recursive: true });
      // A registry file whose tail is no tracked path is no input of this checkout.
      await writeFile(depInfo, `${built}: /registry/src/foo-1.0/src/lib.rs\n`);
      expect(buildInputsOf(built, manifest, root)).toEqual({ ok: false, error: `${depInfo} names no file of ${root}` });
      // Compiled in a sibling clone, as a shed's cloned target directory records it.
      const depInfoText = `${built}: /elsewhere/clone/crates/cli/src/main.rs\n`;
      await writeFile(depInfo, depInfoText);
      const producer = { project: 'cli', target: 'cli-x64-linux' };
      const expected = cargoBuildInputs(root, depInfoText, root, producer);
      if (expected === null) throw new Error('The fixture dep-info must name its tracked source');
      expect(buildInputsOf(built, manifest, root, producer)).toEqual({
        ok: true,
        value: expected,
      });
    });
  });

  it('publishes the artifact and its input record into the output directory and leaves nothing partial', async () => {
    await inTemporaryDirectory(async (root) => {
      const built = join(root, 'target/release/artifact');
      await mkdir(dirname(built), { recursive: true });
      await writeFile(built, 'artifact bytes');
      const record: BuildInputRecord = {
        version: 1,
        files: { 'crates/cli/src/main.rs': '0'.repeat(64) },
        producer: { project: 'cli', target: 'cli-x64-linux' },
      };
      const published: [ZigbuildArtifact, string][] = [
        [{ kind: 'bin', name: 'cowshed' }, join(root, '.cache/bin/linux-x64/cowshed')],
        [{ kind: 'cdylib' }, join(root, 'dist/native/linux-x64-gnu/cowshed.linux-x64-gnu.node')],
      ];
      for (const [artifact, output] of published) {
        await publishArtifact(built, output, artifact, record);
        expect(await readFile(output, 'utf8')).toBe('artifact bytes');
        if (artifact.kind === 'bin') expect((await stat(output)).mode & 0o111).toBe(0o111);
        const directory = dirname(output);
        expect(JSON.parse(await readFile(join(directory, BUILD_INPUTS_FILE), 'utf8'))).toEqual(record);
        expect((await readdir(directory)).sort()).toEqual([BUILD_INPUTS_FILE, basename(output)]);
      }
    });
  });
});
