import { describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { validateCargoCachePolicy } from './cargo-policy.js';

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

async function check(files: Record<string, string>): Promise<{ failures: number; messages: string[] }> {
  return withFixture(files, (root) => {
    const captured = captureErrors();
    try {
      return { failures: validateCargoCachePolicy(root), messages: captured.messages };
    } finally {
      captured.restore();
    }
  });
}

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
