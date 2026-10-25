import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

/**
 * Target-inference fixtures declare real Cargo workspaces. Supply their empty
 * Rust library and initial lock before inference, rather than relying on a
 * rejected Cargo query. Explicit sources and locks, including planted failures,
 * remain untouched.
 */
export async function prepareCargoFixture(root: string): Promise<void> {
  const manifests = (await readdir(root, { recursive: true }))
    .filter((path) => path === 'Cargo.toml' || path.endsWith('/Cargo.toml'))
    .filter((path) => !path.split('/').some((part) => ['node_modules', 'target', '.nx'].includes(part)));
  const workspaces: string[] = [];
  for (const manifest of manifests) {
    const path = join(root, manifest);
    const contents = await readFile(path, 'utf8');
    if (/^\[package\]\s*$/m.test(contents)) {
      const source = join(dirname(path), 'src/lib.rs');
      if (!(await Bun.file(source).exists())) {
        await mkdir(dirname(source), { recursive: true });
        await writeFile(source, '// Empty Cargo fixture library.\n');
      }
    }
    if (/^\[workspace\]\s*$/m.test(contents)) workspaces.push(path);
  }
  for (const manifest of workspaces) {
    if (await Bun.file(join(dirname(manifest), 'Cargo.lock')).exists()) continue;
    const child = Bun.spawn(['cargo', 'generate-lockfile', '--offline', '--manifest-path', manifest], {
      env: process.env,
      cwd: root,
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    if (code !== 0) throw new Error(`Cargo fixture ${manifest} could not lock:\n${stdout}${stderr}`);
  }
}
