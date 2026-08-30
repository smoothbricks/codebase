import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join, normalize, posix, relative, sep } from 'node:path';
import { parse as parseToml } from 'smol-toml';

export interface CargoWorkspacePackage {
  name: string;
  dir: string;
}

export async function listCargoWorkspacePackages(absoluteProjectRoot: string): Promise<CargoWorkspacePackage[]> {
  const workspaceToml = await readFile(join(absoluteProjectRoot, 'Cargo.toml'), 'utf-8');
  const parsed: unknown = parseToml(workspaceToml);
  if (!isRecord(parsed) || !isRecord(parsed.workspace) || !Array.isArray(parsed.workspace.members)) {
    return [];
  }
  const packages: CargoWorkspacePackage[] = [];
  for (const member of parsed.workspace.members) {
    if (typeof member !== 'string' || member.length === 0) {
      continue;
    }
    const crateTomlPath = join(absoluteProjectRoot, member, 'Cargo.toml');
    if (!existsSync(crateTomlPath)) {
      continue;
    }
    const crateParsed: unknown = parseToml(await readFile(crateTomlPath, 'utf-8'));
    if (!isRecord(crateParsed) || !isRecord(crateParsed.package) || typeof crateParsed.package.name !== 'string') {
      continue;
    }
    packages.push({ name: crateParsed.package.name, dir: member.split('\\').join('/') });
  }
  packages.sort((left, right) => left.name.localeCompare(right.name));
  return packages;
}

export async function cargoPackageTestInputs(absoluteProjectRoot: string, memberDir: string): Promise<string[]> {
  const workspacePathDeps = await workspacePathDependencies(absoluteProjectRoot);
  const dirs = new Set<string>([memberDir.split('\\').join('/')]);
  const crateParsed: unknown = parseToml(await readFile(join(absoluteProjectRoot, memberDir, 'Cargo.toml'), 'utf-8'));
  if (isRecord(crateParsed)) {
    for (const tableName of ['dependencies', 'dev-dependencies', 'build-dependencies'] as const) {
      const table = crateParsed[tableName];
      if (!isRecord(table)) {
        continue;
      }
      for (const [depName, spec] of Object.entries(table)) {
        const pathDep = pathDependencyDir(memberDir, depName, spec, workspacePathDeps);
        if (pathDep !== null) {
          dirs.add(pathDep);
        }
      }
    }
  }
  const inputs: string[] = ['{projectRoot}/Cargo.toml', '{projectRoot}/Cargo.lock'];
  for (const dir of [...dirs].sort()) {
    inputs.push(`{projectRoot}/${dir}/**/*.rs`, `{projectRoot}/${dir}/Cargo.toml`);
  }
  inputs.push('{projectRoot}/**/.cargo/config.toml', '{projectRoot}/scripts/*.sh', '!{projectRoot}/**/target/**');
  return inputs;
}

async function workspacePathDependencies(absoluteProjectRoot: string): Promise<Map<string, string>> {
  const parsed: unknown = parseToml(await readFile(join(absoluteProjectRoot, 'Cargo.toml'), 'utf-8'));
  const deps = new Map<string, string>();
  if (!isRecord(parsed) || !isRecord(parsed.workspace) || !isRecord(parsed.workspace.dependencies)) {
    return deps;
  }
  for (const [name, spec] of Object.entries(parsed.workspace.dependencies)) {
    if (isRecord(spec) && typeof spec.path === 'string') {
      deps.set(name, spec.path.split('\\').join('/'));
    }
  }
  return deps;
}

function pathDependencyDir(
  memberDir: string,
  depName: string,
  spec: unknown,
  workspacePathDeps: Map<string, string>,
): string | null {
  if (spec === undefined || spec === null) {
    return null;
  }
  if (typeof spec === 'string') {
    return null;
  }
  if (!isRecord(spec)) {
    return null;
  }
  if (spec.workspace === true) {
    return workspacePathDeps.get(depName) ?? null;
  }
  if (typeof spec.path !== 'string') {
    return null;
  }
  const resolved = normalize(join(memberDir, spec.path));
  if (resolved.startsWith('..')) {
    return null;
  }
  return resolved.split(sep).join(posix.sep);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function cargoTestPackageTargetName(packageName: string): string {
  return `cargo-test-${packageName}`;
}

export function nextestConfigRelPath(workspaceRoot: string, projectRoot: string, configAbs: string): string {
  const rel = relative(join(workspaceRoot, projectRoot), configAbs);
  return rel.length === 0 ? configAbs : rel.split(sep).join('/');
}
