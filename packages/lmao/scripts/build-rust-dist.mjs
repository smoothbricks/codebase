import { cpSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';

const packageRoot = resolve(import.meta.dirname, '..');
const workspaceRoot = resolve(packageRoot, '..', '..');
const sourceCrates = join(packageRoot, 'crates');
const outputRoot = join(packageRoot, 'dist', 'rust');
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
    test: rootManifest.profile.test,
  },
};

rmSync(outputRoot, { recursive: true, force: true });
mkdirSync(outputRoot, { recursive: true });
cpSync(sourceCrates, join(outputRoot, 'crates'), {
  recursive: true,
  filter: (source) => basename(source) !== 'target',
});
cpSync(join(packageRoot, '.cargo'), join(outputRoot, '.cargo'), { recursive: true });
cpSync(join(workspaceRoot, 'Cargo.lock'), join(outputRoot, 'Cargo.lock'));
writeFileSync(join(outputRoot, 'Cargo.toml'), Bun.TOML.stringify(manifest));
