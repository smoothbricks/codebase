import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { statSync } from 'node:fs';
import { lstat, readdir, readFile, readlink, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import typia from 'typia';

interface CargoMetadata {
  packages: {
    source: string | null;
    manifest_path: string;
    targets: { src_path: string }[];
  }[];
  workspace_root: string;
}

const parseMetadata = typia.json.createAssertParse<CargoMetadata>();
const ignoredDirectories = new Set([
  'target',
  '.git',
  'node_modules',
  '.cache',
  '.cowshed',
  '.fseventsd',
  '.TemporaryItems',
  '.Trashes',
  '.Spotlight-V100',
  '.DocumentRevisions-V100',
]);
const ancestorInputs = ['Cargo.toml', '.cargo/config', '.cargo/config.toml'];

/**
 * Cargo owns resolution, including workspace inheritance, target dependencies,
 * patches, and transitive path dependencies. A locked offline query observes
 * that graph without fetching dependencies or changing the lockfile. Git and
 * registry packages are immutable inputs already identified by Cargo.lock.
 *
 * This covers Rust sources and Cargo configuration, matching the plugin's
 * in-workspace Rust inputs. Build scripts' other data/environment inputs remain
 * explicit Nx inputs, just as they are for in-workspace crates.
 */
export async function hashCargoPathInputs(manifestPath: string, workspaceRoot: string): Promise<string> {
  const root = await realpath(workspaceRoot);
  const metadata = parseMetadata(
    execFileSync(
      'cargo',
      ['metadata', '--format-version', '1', '--locked', '--offline', '--manifest-path', resolve(manifestPath)],
      { cwd: root, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'inherit'] },
    ),
  );
  const cargoRoot = await realpath(metadata.workspace_root);
  const directories = new Set<string>();
  const files = new Set<string>();
  const ancestors = new Set<string>();
  const links = new Map<string, string>();
  for (const pkg of metadata.packages) {
    if (pkg.source !== null) continue;
    const manifest = await realpath(pkg.manifest_path);
    const directory = dirname(manifest);
    const fromRoot = relative(root, directory);
    const fromCargo = relative(cargoRoot, directory);
    if (
      fromRoot !== '..' &&
      !fromRoot.startsWith(`..${sep}`) &&
      !isAbsolute(fromRoot) &&
      fromCargo !== '..' &&
      !fromCargo.startsWith(`..${sep}`) &&
      !isAbsolute(fromCargo) &&
      !fromRoot.split(sep).includes('node_modules')
    )
      continue;
    directories.add(directory);
    files.add(manifest);
    for (const target of pkg.targets) files.add(await realpath(target.src_path));
    // An external member can inherit edition, lint policy, dependencies and
    // profiles from a workspace above its package directory. Those manifests
    // are not descendants of the source roots returned by Cargo metadata.
    for (let ancestor = dirname(directory); !ancestors.has(ancestor); ancestor = dirname(ancestor)) {
      ancestors.add(ancestor);
      for (const name of ancestorInputs) {
        const file = join(ancestor, name);
        if (statSync(file, { throwIfNoEntry: false })?.isFile()) files.add(await realpath(file));
      }
    }
  }

  // Overlapping packages (a facade root plus its child crates) share one walk.
  const visited = new Set<string>();
  async function walk(directory: string): Promise<void> {
    const canonical = await realpath(directory);
    if (visited.has(canonical)) return;
    visited.add(canonical);
    for (const entry of await readdir(canonical, { withFileTypes: true })) {
      if (ignoredDirectories.has(entry.name)) continue;
      const path = join(canonical, entry.name);
      if (entry.isSymbolicLink()) links.set(path, await readlink(path));
      const info = entry.isSymbolicLink() ? await lstat(await realpath(path)) : entry;
      if (info.isDirectory()) {
        await walk(path);
      } else if (
        info.isFile() &&
        (entry.name.endsWith('.rs') ||
          entry.name === 'Cargo.toml' ||
          (canonical.endsWith(`${sep}.cargo`) && (entry.name === 'config' || entry.name === 'config.toml')))
      ) {
        files.add(await realpath(path));
      }
    }
  }
  for (const directory of directories) await walk(directory);
  const hash = createHash('sha256');
  hash.update('cargo-path-inputs-v1\0');
  for (const [path, target] of [...links].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))) {
    hash.update(relative(root, path));
    hash.update('\0symlink\0');
    hash.update(target);
    hash.update('\0');
  }
  for (const file of [...files].sort()) {
    const bytes = await readFile(file);
    // Both names and bytes matter: moving a module can change resolution even
    // when its contents are unchanged. Length framing makes boundaries unique.
    hash.update(relative(root, file));
    hash.update('\0');
    hash.update(String(bytes.length));
    hash.update('\0');
    hash.update(bytes);
  }
  return hash.digest('hex');
}
