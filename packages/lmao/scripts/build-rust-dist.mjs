import { spawnSync } from 'node:child_process';
import { cpSync, lstatSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';

const packageRoot = resolve(import.meta.dirname, '..');
const workspaceRoot = resolve(packageRoot, '..', '..');
const sourceCrates = join(packageRoot, 'crates');
const outputRoot = join(packageRoot, 'dist', 'rust');
// Every file this copy reads: the input record (packages/build-inputs) beside the copy names exactly these.
const inputs = [import.meta.filename, join(workspaceRoot, 'Cargo.toml'), join(workspaceRoot, 'Cargo.lock')];
const recordFile = (source) => {
  if (lstatSync(source).isFile()) inputs.push(source);
  return true;
};
const rootManifest = Bun.TOML.parse(readFileSync(join(workspaceRoot, 'Cargo.toml'), 'utf8'));

const inheritedDependencies = new Set();
for (const entry of readdirSync(sourceCrates, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const crateManifest = Bun.TOML.parse(readFileSync(join(sourceCrates, entry.name, 'Cargo.toml'), 'utf8'));
  for (const sectionName of ['dependencies', 'dev-dependencies', 'build-dependencies']) {
    const section = crateManifest[sectionName];
    if (section === undefined) continue;
    for (const [name, dependency] of Object.entries(section)) {
      if (typeof dependency === 'object' && dependency !== null && dependency.workspace === true) {
        inheritedDependencies.add(name);
      }
    }
  }
}

const dependencies = {};
for (const name of [...inheritedDependencies].sort()) {
  const dependency = rootManifest.workspace.dependencies[name];
  if (dependency === undefined) {
    throw new Error(`root Cargo workspace does not define inherited dependency ${name}`);
  }
  if (typeof dependency === 'object' && dependency !== null && typeof dependency.path === 'string') {
    dependencies[name] = {
      ...dependency,
      path: dependency.path.replace(/^packages\/lmao\//, ''),
    };
  } else {
    dependencies[name] = dependency;
  }
}

const manifest = {
  workspace: {
    resolver: rootManifest.workspace.resolver,
    members: ['crates/*'],
    package: rootManifest.workspace.package,
    dependencies,
  },
  profile: {
    release: rootManifest.profile.release,
  },
};

rmSync(outputRoot, { recursive: true, force: true });
mkdirSync(outputRoot, { recursive: true });
cpSync(sourceCrates, join(outputRoot, 'crates'), {
  recursive: true,
  filter: (source) => basename(source) !== 'target' && recordFile(source),
});
mkdirSync(join(outputRoot, 'src', 'lib', 'schema'), { recursive: true });
for (const file of [join('lib', 'capacityTuning.ts'), join('lib', 'schema', 'systemSchema.ts')]) {
  recordFile(join(packageRoot, 'src', file));
  cpSync(join(packageRoot, 'src', file), join(outputRoot, 'src', file));
}
cpSync(join(packageRoot, '.cargo'), join(outputRoot, '.cargo'), { recursive: true, filter: recordFile });
cpSync(join(workspaceRoot, 'Cargo.lock'), join(outputRoot, 'Cargo.lock'));
writeFileSync(join(outputRoot, 'Cargo.toml'), Bun.TOML.stringify(manifest));
const metadata = spawnSync(
  'cargo',
  ['metadata', '--manifest-path', join(outputRoot, 'Cargo.toml'), '--offline', '--format-version', '1'],
  { stdio: ['ignore', 'ignore', 'inherit'] },
);
if (metadata.status !== 0) {
  throw new Error(`could not derive the standalone Rust workspace lockfile (cargo exited ${metadata.status})`);
}

// The copy's input record, written through build-inputs' `smoo-nx-build-inputs --input` into dist/rust, the target's
// declared output: a cached run restores it with the copy, so a developer link judges the copy by the bytes it read
// (specs/cli/01_workspace_reconciliation.md, "Declared developer link pins"). Run under Nx, it names this task.
const recorded = spawnSync(
  process.execPath,
  [
    join(workspaceRoot, 'packages', 'build-inputs', 'dist', 'ts', 'bin', 'smoo-nx-build-inputs.js'),
    ...inputs.flatMap((file) => ['--input', file]),
    '--output',
    outputRoot,
  ],
  { stdio: ['ignore', 'inherit', 'inherit'] },
);
if (recorded.status !== 0) {
  throw new Error(`could not record the inputs of ${outputRoot} (smoo-nx-build-inputs exited ${recorded.status})`);
}
