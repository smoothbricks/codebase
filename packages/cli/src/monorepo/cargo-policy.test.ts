import { describe, expect, it, spyOn } from 'bun:test';
import { readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { applyCargoFeatureUnification, type CargoHakariShell, validateCargoCachePolicy } from './cargo-policy.js';

async function withFixture<T>(files: Record<string, string>, callback: (root: string) => T): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-cargo-policy-'));
  try {
    for (const [path, content] of Object.entries(files)) {
      const target = join(root, path);
      await mkdir(dirname(target), { recursive: true });
      await writeFile(target, content);
    }
    return callback(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function check(
  files: Record<string, string>,
  shell?: CargoHakariShell,
): Promise<{ failures: number; messages: string[] }> {
  return withFixture(files, (root) => {
    const captured = captureErrors();
    try {
      return { failures: validateCargoCachePolicy(root, { shell }), messages: captured.messages };
    } finally {
      captured.restore();
    }
  });
}

/** Records what update would run, and answers `verify` however the test needs. */
function recordingHakari(
  verify: { code: number; output?: string; missing?: boolean } = { code: 0 },
): CargoHakariShell & {
  calls: string[][];
} {
  const calls: string[][] = [];
  return {
    calls,
    run(_directory, args) {
      calls.push([...args]);
      return args[0] === 'verify'
        ? { code: verify.code, output: verify.output ?? '', missing: verify.missing ?? false }
        : { code: 0, output: '', missing: false };
    },
  };
}

const NIGHTLY_DEVENV = 'languages.rust = {\n  channel = "nightly";\n};\n';
const UNIFIED_CONFIG = '[unstable]\nfeature-unification = true\n\n[resolver]\nfeature-unification = "workspace"\n';
const TWO_CRATE_WORKSPACE = {
  'Cargo.toml': '[workspace]\nmembers = ["crates/*"]\n\n[profile.test]\nincremental = false\ndebug = 0\n',
  'crates/alpha/Cargo.toml': '[package]\nname = "alpha"\n',
  'crates/beta/Cargo.toml': '[package]\nname = "beta"\n',
};

function captureErrors(): { messages: string[]; restore: () => void } {
  const messages: string[] = [];
  const error = spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    messages.push(args.join(' '));
  });
  return { messages, restore: () => error.mockRestore() };
}

describe('Cargo cache policy', () => {
  it('returns zero for a clean repository', async () => {
    const result = await check({});
    expect(result.failures).toBe(0);
    expect(result.messages).toEqual([]);
  });

  it('requires a non-incremental test lane at each workspace root', async () => {
    const missing = await check({ 'Cargo.toml': '[workspace]\nmembers = []\n' });
    expect(missing.failures).toBe(1);
    expect(missing.messages[0]).toContain('[profile.test] incremental = false');

    const present = await check({
      'Cargo.toml': '[workspace]\nmembers = []\n\n[profile.test]\nincremental = false\ndebug = 0\n',
    });
    expect(present.failures).toBe(0);
  });

  it('requires the same lane for a standalone package root', async () => {
    const missing = await check({ 'Cargo.toml': '[package]\nname = "standalone"\n' });
    expect(missing.failures).toBe(1);
    expect(missing.messages[0]).toContain('effective Cargo workspace root');

    const present = await check({
      'Cargo.toml': '[package]\nname = "standalone"\n\n[profile.test]\nincremental = false\ndebug = 0\n',
    });
    expect(present.failures).toBe(0);
  });

  it('flags an explicitly cacheable profile that carries debuginfo', async () => {
    const result = await check({
      'Cargo.toml':
        '[workspace]\nmembers = []\n\n[profile.test]\nincremental = false\ndebug = 0\n\n[profile.cache]\nincremental = false\ndebug = 1\n',
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('profile cache is cacheable');
    expect(result.messages[0]).toContain('absolute source paths');
  });

  it('resolves a non-incremental inherited profile before checking debuginfo', async () => {
    const result = await check({
      'Cargo.toml':
        '[workspace]\nmembers = []\n\n[profile.test]\nincremental = false\ndebug = 0\n\n[profile.cache]\ninherits = "release"\ndebug = 1\n',
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('profile cache');
  });

  it('allows path-neutral and incremental profiles', async () => {
    const result = await check({
      'Cargo.toml':
        '[workspace]\nmembers = []\n\n[profile.test]\nincremental = false\ndebug = 0\n\n[profile.no-debug]\nincremental = false\ndebug = 0\n\n[profile.dev-symbols]\nincremental = true\ndebug = 2\n',
    });
    expect(result.failures).toBe(0);
  });

  it('rejects CARGO_INCREMENTAL in a Justfile', async () => {
    const result = await check({ Justfile: 'set export CARGO_INCREMENTAL := "1"\n' });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('CARGO_INCREMENTAL=1 hard-fails');
    expect(result.messages[0]).toContain('leave CARGO_INCREMENTAL unset');
  });

  it('rejects CARGO_INCREMENTAL in Cargo config env', async () => {
    const result = await check({ '.cargo/config.toml': '[env]\nCARGO_INCREMENTAL = "0"\n' });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('.cargo/config.toml');
    expect(result.messages[0]).toContain('CARGO_INCREMENTAL=0 surrenders');
  });

  it('flags profile tables in workspace members but not standalone package roots', async () => {
    const result = await check({
      'Cargo.toml': '[workspace]\nmembers = ["crates/*"]\n\n[profile.test]\nincremental = false\ndebug = 0\n',
      'crates/member/Cargo.toml': '[package]\nname = "member"\n\n[profile.dev]\nincremental = true\n',
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('Cargo ignores profile tables');
  });

  it('flags a target directory outside the repository root', async () => {
    const result = await check({ '.cargo/config.toml': '[build]\ntarget-dir = "/tmp/smoo-target"\n' });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('build.target-dir');
    expect(result.messages[0]).toContain('repository-relative');
  });

  it('flags an absolute foreign linker', async () => {
    const result = await check({
      '.cargo/config.toml': '[target.aarch64-apple-darwin]\nlinker = "/opt/toolchain/clang"\n',
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('linker is an absolute path outside');
    expect(result.messages[0]).toContain('repository-relative linker');
  });

  it('reports CARGO_MANIFEST_DIR as informational without failing', async () => {
    const result = await check({
      'src/lib.rs': 'const ROOT: &str = env!("CARGO_MANIFEST_DIR");\n',
      'src/lib_test.rs': 'const TEST_ROOT: &str = env!("CARGO_MANIFEST_DIR");\n',
      'tests/integration.rs': 'const TEST_ROOT: &str = env!("CARGO_MANIFEST_DIR");\n',
    });
    expect(result.failures).toBe(0);
    expect(result.messages.join('\n')).toContain('Cargo cache policy advisories (informational only');
    expect(result.messages.join('\n')).toContain('src/lib.rs:1');
    expect(result.messages.join('\n')).toContain('patched sccache never normalises env-dep values');
    expect(result.messages.join('\n')).not.toContain('src/lib_test.rs');
    expect(result.messages.join('\n')).not.toContain('tests/integration.rs');
  });

  it('ignores the macro named in a comment but still reports it in code', async () => {
    const result = await check({
      'src/documented.rs': [
        '/// A crate compiling env!("CARGO_MANIFEST_DIR") fails closed across paths.',
        '// So does env!("CARGO_MANIFEST_DIR") in a plain comment.',
        '/* and env!("CARGO_MANIFEST_DIR") in a block */',
        'const SEPARATOR: &str = "// not a comment";',
        'const RAW: &str = r#"env!("CARGO_MANIFEST_DIR") inside a raw string"#;',
        'const REAL: &str = env!("CARGO_MANIFEST_DIR");',
        '',
      ].join('\n'),
    });
    expect(result.failures).toBe(0);
    const messages = result.messages.join('\n');
    expect(messages).toContain('src/documented.rs:6');
    expect(messages).not.toContain('src/documented.rs:1');
    expect(messages).not.toContain('src/documented.rs:2');
    expect(messages).not.toContain('src/documented.rs:3');
  });

  it('honours an explicit ignore marker for a non-hidden subtree', async () => {
    const result = await check({
      'vendor/Cargo.toml': '# smoo-cargo-policy: ignore\n[workspace]\nmembers = []\n',
      'vendor/crate/Cargo.toml': '[package]\nname = "ignored"\n',
      'vendor/.cargo/config.toml': '[build]\ntarget-dir = "/tmp/ignored"\n',
      'vendor/Justfile': 'export CARGO_INCREMENTAL := "1"\n',
    });
    expect(result.failures).toBe(0);
    expect(result.messages.join('\n')).toContain('skips this manifest and its subtree');
    expect(result.messages.join('\n')).toContain('vendor/Cargo.toml');
  });

  it('skips manifests below hidden vendored directories', async () => {
    const result = await check({
      '.bun-pin/Cargo.toml': '[workspace]\nmembers = []\n',
    });
    expect(result.failures).toBe(0);
    expect(result.messages).toEqual([]);
  });
});

describe('Cargo workspace feature unification', () => {
  it('leaves a single-crate workspace alone', async () => {
    // Nothing to unify: `feature-unification = "workspace"` and a workspace-hack
    // both exist to stop ONE dependency being built twice with different
    // features for two members, which needs two members.
    const result = await check({
      'Cargo.toml': '[workspace]\nmembers = ["crates/only"]\n\n[profile.test]\nincremental = false\ndebug = 0\n',
      'crates/only/Cargo.toml': '[package]\nname = "only"\n',
      'tooling/direnv/devenv.smoo.nix': NIGHTLY_DEVENV,
    });
    expect(result.failures).toBe(0);
  });

  it('requires a mechanism once a workspace holds more than one crate', async () => {
    const result = await check({ ...TWO_CRATE_WORKSPACE, 'tooling/direnv/devenv.smoo.nix': NIGHTLY_DEVENV });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('feature-unification');
    expect(result.messages[0]).toContain('smoo monorepo update');
  });

  it('accepts the nightly resolver configuration and rejects it without the unstable flag', async () => {
    const configured = await check({
      ...TWO_CRATE_WORKSPACE,
      'tooling/direnv/devenv.smoo.nix': NIGHTLY_DEVENV,
      '.cargo/config.toml': UNIFIED_CONFIG,
    });
    expect(configured.failures).toBe(0);

    // Cargo silently ignores [resolver] feature-unification without the
    // unstable opt-in, so the half-configured repository believes it is unified
    // while every crate still re-resolves features.
    const inert = await check({
      ...TWO_CRATE_WORKSPACE,
      'tooling/direnv/devenv.smoo.nix': NIGHTLY_DEVENV,
      '.cargo/config.toml': '[resolver]\nfeature-unification = "workspace"\n',
    });
    expect(inert.failures).toBe(1);
    expect(inert.messages[0]).toContain('[unstable] feature-unification = true');
  });

  it('reads the channel from the managed devenv module, not from a decorative rust-toolchain file', async () => {
    // devenv resolves the toolchain through rust-overlay and ignores
    // rust-toolchain.toml unless languages.rust.toolchainFile names it, so the
    // module's channel is the compiler that actually runs these commands.
    const result = await check({
      ...TWO_CRATE_WORKSPACE,
      'tooling/direnv/devenv.smoo.nix': NIGHTLY_DEVENV,
      'rust-toolchain.toml': '[toolchain]\nchannel = "stable"\n',
      '.cargo/config.toml': UNIFIED_CONFIG,
    });
    expect(result.failures).toBe(0);
  });

  it('refuses the nightly-only resolver key on a stable toolchain', async () => {
    const result = await check({
      ...TWO_CRATE_WORKSPACE,
      'rust-toolchain.toml': '[toolchain]\nchannel = "stable"\n',
      '.cargo/config.toml': UNIFIED_CONFIG,
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('cargo-hakari');
    expect(result.messages[0]).toContain('stable');
  });

  it('accepts a hakari-managed workspace-hack on a stable toolchain', async () => {
    const hakari = recordingHakari();
    const result = await check(
      {
        'Cargo.toml':
          '[workspace]\nmembers = ["crates/*", "workspace-hack"]\n\n[profile.test]\nincremental = false\ndebug = 0\n',
        'crates/alpha/Cargo.toml':
          '[package]\nname = "alpha"\n\n[dependencies]\nworkspace-hack = { path = "../../workspace-hack" }\n',
        'crates/beta/Cargo.toml':
          '[package]\nname = "beta"\n\n[dependencies]\nworkspace-hack = { path = "../../workspace-hack" }\n',
        'workspace-hack/Cargo.toml': '[package]\nname = "workspace-hack"\n',
        '.config/hakari.toml': 'hakari-package = "workspace-hack"\nresolver = "2"\n',
        'rust-toolchain.toml': '[toolchain]\nchannel = "stable"\n',
      },
      hakari,
    );
    expect(result.failures).toBe(0);
    expect(hakari.calls).toEqual([['verify']]);
  });

  it('flags a crate that does not depend on the workspace-hack', async () => {
    const result = await check(
      {
        'Cargo.toml':
          '[workspace]\nmembers = ["crates/*", "workspace-hack"]\n\n[profile.test]\nincremental = false\ndebug = 0\n',
        'crates/alpha/Cargo.toml':
          '[package]\nname = "alpha"\n\n[dependencies]\nworkspace-hack = { path = "../../workspace-hack" }\n',
        'crates/beta/Cargo.toml': '[package]\nname = "beta"\n',
        'workspace-hack/Cargo.toml': '[package]\nname = "workspace-hack"\n',
        '.config/hakari.toml': 'hakari-package = "workspace-hack"\n',
        'rust-toolchain.toml': '[toolchain]\nchannel = "stable"\n',
      },
      recordingHakari(),
    );
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('beta');
    expect(result.messages[0]).toContain('smoo monorepo update');
  });

  it('surfaces a stale workspace-hack that cargo hakari verify rejects', async () => {
    const result = await check(
      {
        'Cargo.toml':
          '[workspace]\nmembers = ["crates/*", "workspace-hack"]\n\n[profile.test]\nincremental = false\ndebug = 0\n',
        'crates/alpha/Cargo.toml':
          '[package]\nname = "alpha"\n\n[dependencies]\nworkspace-hack = { path = "../../workspace-hack" }\n',
        'crates/beta/Cargo.toml':
          '[package]\nname = "beta"\n\n[dependencies]\nworkspace-hack = { path = "../../workspace-hack" }\n',
        'workspace-hack/Cargo.toml': '[package]\nname = "workspace-hack"\n',
        '.config/hakari.toml': 'hakari-package = "workspace-hack"\n',
        'rust-toolchain.toml': '[toolchain]\nchannel = "stable"\n',
      },
      recordingHakari({ code: 1, output: 'workspace-hack is not up-to-date' }),
    );
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('cargo hakari verify');
    expect(result.messages[0]).toContain('workspace-hack is not up-to-date');
  });
});

describe('Cargo feature unification update', () => {
  it('writes the nightly resolver configuration once', async () => {
    await withFixture({ ...TWO_CRATE_WORKSPACE, 'tooling/direnv/devenv.smoo.nix': NIGHTLY_DEVENV }, (root) => {
      const configPath = join(root, '.cargo/config.toml');
      const hakari = recordingHakari();
      applyCargoFeatureUnification(root, { shell: hakari });
      const written = readFileSync(configPath, 'utf8');
      expect(written).toContain('[unstable]\nfeature-unification = true');
      expect(written).toContain('[resolver]\nfeature-unification = "workspace"');
      expect(hakari.calls).toEqual([]);

      applyCargoFeatureUnification(root, { shell: hakari });
      expect(readFileSync(configPath, 'utf8')).toBe(written);
      const captured = captureErrors();
      try {
        expect(validateCargoCachePolicy(root, { shell: hakari })).toBe(0);
      } finally {
        captured.restore();
      }
    });
  });

  it('generates and wires a workspace-hack on a stable toolchain', async () => {
    await withFixture(
      { ...TWO_CRATE_WORKSPACE, 'rust-toolchain.toml': '[toolchain]\nchannel = "1.89.0"\n' },
      (root) => {
        const hakari = recordingHakari();
        applyCargoFeatureUnification(root, { shell: hakari });
        expect(hakari.calls).toEqual([['init', 'workspace-hack'], ['generate'], ['manage-deps', '--yes']]);
      },
    );
  });

  it('skips hakari init when the workspace already declares one', async () => {
    await withFixture(
      {
        ...TWO_CRATE_WORKSPACE,
        'rust-toolchain.toml': '[toolchain]\nchannel = "1.89.0"\n',
        '.config/hakari.toml': 'hakari-package = "workspace-hack"\n',
      },
      (root) => {
        const hakari = recordingHakari();
        applyCargoFeatureUnification(root, { shell: hakari });
        expect(hakari.calls).toEqual([['generate'], ['manage-deps', '--yes']]);
      },
    );
  });
});
