import { describe, expect, it } from 'bun:test';

import { artifactFile, cargoTriple, zigbuildArgs, zigbuildEnv } from './executor.js';

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
});
