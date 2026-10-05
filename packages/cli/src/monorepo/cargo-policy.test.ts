import { describe, expect, it, spyOn } from 'bun:test';
import { readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { renderCargoDevProfiles } from '@smoothbricks/nx-plugin/cargo-dev-profile';
import type { ProjectTargets } from '../nx/index.js';
import {
  applyCargoFeatureUnification,
  type CargoHakariShell,
  validateCargoCachePolicy,
  validateCargoToolchainInputs,
} from './cargo-policy.js';

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

async function checkToolchain(
  files: Record<string, string>,
  projects: ProjectTargets[],
): Promise<{ failures: number; messages: string[] }> {
  return withFixture(files, (root) => {
    const captured = captureErrors();
    try {
      return { failures: validateCargoToolchainInputs(root, projects), messages: captured.messages };
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

/**
 * The dev and debugging profiles, spelled by hand rather than rendered, so the
 * policy is checked against the documented TOML and not against itself.
 */
const DEV_PROFILE_TABLES: ReadonlyArray<readonly [string, readonly string[]]> = [
  ['profile.dev', ['debug = "line-tables-only"', 'split-debuginfo = "unpacked"']],
  ['profile.dev.package."*"', ['debug = 0']],
  ['profile.dev.build-override', ['debug = 0']],
  ['profile.debugging', ['inherits = "dev"', 'debug = 2']],
  ['profile.debugging.package."*"', ['debug = 2']],
  ['profile.debugging.build-override', ['debug = 2']],
];

function devProfiles(edit: (table: string, lines: readonly string[]) => readonly string[] = (_, lines) => lines) {
  return DEV_PROFILE_TABLES.map(([table, lines]) => [`[${table}]`, ...edit(table, lines), ''].join('\n')).join('\n');
}

const DEV_PROFILES = devProfiles();
const WORKSPACE_ROOT = `[workspace]\nmembers = ["crates/*"]\n\n${DEV_PROFILES}`;
const TWO_CRATE_WORKSPACE = {
  'Cargo.toml': WORKSPACE_ROOT,
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

  it('lets the test profile inherit dev at every effective root', async () => {
    const workspace = await check({ 'Cargo.toml': `[workspace]\nmembers = []\n\n${DEV_PROFILES}` });
    expect(workspace.messages).toEqual([]);

    const standalone = await check({ 'Cargo.toml': '[package]\nname = "standalone"\n' });
    expect(standalone.messages).toEqual([]);
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
      'Cargo.toml': WORKSPACE_ROOT,
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

  // Cargo does not fingerprint the checkout path, so a cowshed workspace runs the test binaries it
  // inherited from main; a test that compiled the path in reads main's files. Tests are the usual
  // offenders, so they are not exempt.
  it('refuses a compiled CARGO_MANIFEST_DIR in any Rust source, tests included', async () => {
    const result = await check({
      'src/lib.rs': 'const ROOT: &str = env!("CARGO_MANIFEST_DIR");\n',
      'src/lib_test.rs': 'fn test_root() -> Option<&\'static str> { option_env!("CARGO_MANIFEST_DIR") }\n',
      'tests/integration.rs': 'const TEST_ROOT: &str = concat!(env!( "CARGO_MANIFEST_DIR" ), "/fixture");\n',
    });
    expect(result.failures).toBe(3);
    const messages = result.messages.join('\n');
    expect(messages).toContain('src/lib.rs:1');
    expect(messages).toContain('src/lib_test.rs:1');
    expect(messages).toContain('tests/integration.rs:1');
    expect(messages).toContain('reads the files of the checkout that compiled it');
  });

  it('ignores the macro named in a comment but refuses it in code', async () => {
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
    expect(result.failures).toBe(1);
    const messages = result.messages.join('\n');
    expect(messages).toContain('src/documented.rs:6');
    expect(messages).not.toContain('src/documented.rs:1');
    expect(messages).not.toContain('src/documented.rs:2');
    expect(messages).not.toContain('src/documented.rs:3');
  });

  it('lets a test read CARGO_MANIFEST_DIR at run time', async () => {
    const result = await check({
      'tests/integration.rs': 'fn root() -> String { std::env::var("CARGO_MANIFEST_DIR").unwrap() }\n',
    });
    expect(result.failures).toBe(0);
    expect(result.messages).toEqual([]);
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

describe('Cargo dev profile policy', () => {
  it('accepts a workspace root carrying the dev and debugging profiles', async () => {
    const result = await check({ 'Cargo.toml': WORKSPACE_ROOT });
    expect(result.failures).toBe(0);
    expect(result.messages).toEqual([]);
  });

  it('accepts what the package generator writes', async () => {
    const result = await check({ 'Cargo.toml': `[workspace]\nmembers = []\n\n${renderCargoDevProfiles()}\n` });
    expect(result.messages).toEqual([]);
  });

  it('compares debug by level, not by spelling', async () => {
    const result = await check({
      'Cargo.toml': `[workspace]\nmembers = []\n\n${devProfiles((table, lines) =>
        lines.map((line) =>
          line === 'debug = 0' ? (table.endsWith('"*"') ? 'debug = false' : 'debug = "none"') : line,
        ),
      )}`,
    });
    expect(result.messages).toEqual([]);
  });

  const elements = DEV_PROFILE_TABLES.flatMap(([table, lines]) => lines.map((line) => [table, line] as const));
  it.each(elements)('refuses a root missing [%s] %s and names the TOML to add', async (table, missing) => {
    const result = await check({
      'Cargo.toml': `[workspace]\nmembers = []\n\n${devProfiles((current, lines) =>
        current === table ? lines.filter((line) => line !== missing) : lines,
      )}`,
    });
    const key = missing.slice(0, missing.indexOf(' ='));
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('Cargo.toml: ');
    expect(result.messages[0]).toContain(`[${table}] ${key} is missing.`);
    expect(result.messages[0]).toEndWith(`Fix it with:\n[${table}]\n${missing}`);
  });

  it('refuses a root with no profiles once per requirement', async () => {
    const result = await check({ 'Cargo.toml': '[workspace]\nmembers = []\n' });
    expect(result.failures).toBe(elements.length);
  });

  it('refuses a build-override whose debug level differs from the dependencies', async () => {
    const result = await check({
      'Cargo.toml': `[workspace]\nmembers = []\n\n${devProfiles((table, lines) =>
        table === 'profile.dev.build-override' ? ['debug = "line-tables-only"'] : lines,
      )}`,
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('[profile.dev.build-override] debug is "line-tables-only".');
    expect(result.messages[0]).toContain('It must equal [profile.dev.package."*"] debug.');
    expect(result.messages[0]).toContain('compiles it, and everything above it, twice');
    expect(result.messages[0]).toEndWith('Fix it with:\n[profile.dev.build-override]\ndebug = 0');
  });

  it('refuses a dev profile that turns incremental compilation off', async () => {
    const result = await check({
      'Cargo.toml': `[workspace]\nmembers = []\n\n${devProfiles((table, lines) =>
        table === 'profile.dev' ? [...lines, 'incremental = false'] : lines,
      )}`,
    });
    expect(result.failures).toBe(1);
    expect(result.messages[0]).toContain('[profile.dev] sets incremental = false');
    expect(result.messages[0]).toContain('Fix it by deleting incremental = false');
  });

  it('leaves standalone package roots and ignored subtrees alone', async () => {
    const result = await check({
      'Cargo.toml': '[package]\nname = "standalone"\n',
      'vendor/Cargo.toml': '# smoo-cargo-policy: ignore\n[workspace]\nmembers = []\n',
    });
    expect(result.failures).toBe(0);
  });
});

describe('Cargo workspace feature unification', () => {
  it('leaves a single-crate workspace alone', async () => {
    // Nothing to unify: `feature-unification = "workspace"` and a workspace-hack
    // both exist to stop ONE dependency being built twice with different
    // features for two members, which needs two members.
    const result = await check({
      'Cargo.toml': `[workspace]\nmembers = ["crates/only"]\n\n${DEV_PROFILES}`,
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
        'Cargo.toml': `[workspace]\nmembers = ["crates/*", "workspace-hack"]\n\n${DEV_PROFILES}`,
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
        'Cargo.toml': `[workspace]\nmembers = ["crates/*", "workspace-hack"]\n\n${DEV_PROFILES}`,
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
        'Cargo.toml': `[workspace]\nmembers = ["crates/*", "workspace-hack"]\n\n${DEV_PROFILES}`,
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

describe('cargo toolchain identity policy', () => {
  const cargoLint = (inputs: string[]): ProjectTargets => ({
    project: 'codebase',
    root: '.',
    targets: ['cargo-lint'],
    targetCache: new Map([['cargo-lint', true]]),
    targetInputs: new Map([['cargo-lint', inputs]]),
    targetOptions: new Map([['cargo-lint', { command: 'cargo --frozen clippy --workspace -- -D warnings' }]]),
  });

  // The failure this policy exists for: a hand-written input list replaces the
  // inferred one, so the target stops hashing the pin and a toolchain bump
  // leaves its cached verdict standing.
  it('refuses a cached cargo target whose declared inputs drop the pin', async () => {
    const { failures, messages } = await checkToolchain({ 'nx.json': '{}\n' }, [cargoLint(['rustWorkspace'])]);
    expect(failures).toBe(1);
    expect(messages).toEqual([
      'codebase:cargo-lint: cached cargo target hashes no toolchain pin, so a toolchain bump cannot invalidate it. ' +
        'Add "cargoToolchain" to its inputs, or to the named input it uses.',
    ]);
  });

  it('accepts the pin reached directly, through a named input, or by the name inference defines', async () => {
    const viaNamedInput = JSON.stringify({
      namedInputs: { rustWorkspace: ['{workspaceRoot}/Cargo.toml', '{workspaceRoot}/tooling/direnv/devenv.lock'] },
    });
    expect(await checkToolchain({ 'nx.json': viaNamedInput }, [cargoLint(['rustWorkspace'])])).toMatchObject({
      failures: 0,
    });
    expect(
      await checkToolchain({ 'nx.json': '{}\n' }, [cargoLint(['{workspaceRoot}/tooling/direnv/devenv.lock'])]),
    ).toMatchObject({ failures: 0 });
    expect(await checkToolchain({ 'nx.json': '{}\n' }, [cargoLint(['cargoToolchain'])])).toMatchObject({
      failures: 0,
    });
  });

  // A negated fileset REMOVES a path from the hash. Reading one as the pin
  // would accept the exact declaration that guarantees the bug.
  it('does not accept an excluded lock as the pin', async () => {
    expect(
      await checkToolchain({ 'nx.json': '{}\n' }, [cargoLint(['!{workspaceRoot}/tooling/direnv/devenv.lock'])]),
    ).toMatchObject({ failures: 1 });
  });

  // Only what Nx can restore, and only what runs cargo: an uncached warm-up or
  // a TypeScript build has no stale artifact for a toolchain bump to strand.
  it('governs cached cargo targets only', async () => {
    const uncached: ProjectTargets = {
      ...cargoLint([]),
      targetCache: new Map([['cargo-lint', false]]),
    };
    const notCargo: ProjectTargets = {
      ...cargoLint([]),
      targets: ['tsc-js'],
      targetCache: new Map([['tsc-js', true]]),
      targetInputs: new Map([['tsc-js', ['default']]]),
      targetOptions: new Map([['tsc-js', { command: 'ttsc --build' }]]),
    };
    expect(await checkToolchain({ 'nx.json': '{}\n' }, [uncached, notCargo])).toMatchObject({ failures: 0 });
  });

  // A cyclic named input is a repository mistake, not a reason to hang the
  // whole validation run before it can report anything at all.
  it('terminates on a named input that references itself', async () => {
    const cyclic = JSON.stringify({ namedInputs: { rustWorkspace: ['rustWorkspace'] } });
    expect(await checkToolchain({ 'nx.json': cyclic }, [cargoLint(['rustWorkspace'])])).toMatchObject({ failures: 1 });
  });
});
