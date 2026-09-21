import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { hashCargoPathInputs } from './cargo-source-hash.js';

it.each(['app', '.'])('invalidates transitive and inherited Cargo inputs with Nx rooted at %s', async (nxDirectory) => {
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-'));
  const app = join(root, 'app');
  const manifest = join(app, 'Cargo.toml');
  const nxRoot = join(root, nxDirectory);
  async function put(path: string, text: string): Promise<void> {
    const file = join(root, path);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, text);
  }
  try {
    await put(
      'app/Cargo.toml',
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../external/bridge"}\n[workspace]\n',
    );
    await put('app/src/lib.rs', 'pub fn run() {}\n');
    await put(
      'external/Cargo.toml',
      '[workspace]\nmembers=["bridge","leaf"]\nresolver="2"\n[workspace.package]\nedition="2021"\n',
    );
    await put(
      'external/bridge/Cargo.toml',
      '[package]\nname="bridge"\nversion="0.1.0"\nedition.workspace=true\n[dependencies]\nleaf={path="../leaf"}\n',
    );
    await put('external/bridge/src/lib.rs', 'pub fn bridge() {}\n');
    await put('external/leaf/Cargo.toml', '[package]\nname="leaf"\nversion="0.1.0"\nedition.workspace=true\n');
    await put('external/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 1 }\n');
    execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe' });
    const initial = await hashCargoPathInputs(manifest, nxRoot);
    await put('external/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 2 }\n');
    const sourceChanged = await hashCargoPathInputs(manifest, nxRoot);
    expect(sourceChanged).not.toBe(initial);
    await put(
      'external/Cargo.toml',
      '[workspace]\nmembers=["bridge","leaf"]\nresolver="2"\n[workspace.package]\nedition="2024"\n',
    );
    const inheritedChanged = await hashCargoPathInputs(manifest, nxRoot);
    expect(inheritedChanged).not.toBe(sourceChanged);
    await put('external/leaf/target/noise.rs', 'uncompiled build output');
    expect(await hashCargoPathInputs(manifest, nxRoot)).toBe(inheritedChanged);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('the runtime input is byte-identical under cargo lock contention: nothing on stderr', async () => {
  // Nx hashes a runtime input's stdout and stderr together, and hashes once per
  // distinct task env, so the bin runs many times at once at the start of a
  // run. Cargo reports package-cache lock contention on stderr; a private
  // CARGO_HOME makes that contention real here.
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-'));
  const manifest = join(root, 'Cargo.toml');
  try {
    await mkdir(join(root, 'src'), { recursive: true });
    await writeFile(manifest, '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[workspace]\n');
    await writeFile(join(root, 'src', 'lib.rs'), 'pub fn run() {}\n');
    const env = { ...process.env, CARGO_HOME: join(root, '.cargo-home') };
    execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe', env });
    // The built bin under node, exactly as nx.json's runtime input invokes it (test depends on build).
    const bin = join(import.meta.dir, '..', 'dist', 'bin', 'smoo-nx-cargo-hash.js');
    const runs = await Promise.all(
      Array.from({ length: 8 }, async () => {
        const proc = Bun.spawn(['node', bin, manifest], {
          cwd: root,
          env,
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const [stdout, stderr, exitCode] = await Promise.all([
          new Response(proc.stdout).text(),
          new Response(proc.stderr).text(),
          proc.exited,
        ]);
        return { stdout, stderr, exitCode };
      }),
    );
    for (const run of runs) {
      expect(run.exitCode).toBe(0);
      expect(run.stderr).toBe('');
    }
    expect(new Set(runs.map((run) => run.stdout)).size).toBe(1);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
