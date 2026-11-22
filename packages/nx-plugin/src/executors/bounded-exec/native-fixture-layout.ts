/** Select a private Cargo final target only when this native test recipe can promote an executable or library.
 * Runs inside the bounded fixture segment, before any test or custom consumer. The caller runs Cargo and nextest
 * with the same configuration: only they write the metadata describing which artifacts were actually built. */
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';

function field(value: unknown, key: string): unknown {
  return typeof value === 'object' && value !== null && key in value ? Reflect.get(value, key) : undefined;
}

function onlySharedTargets(pkg: unknown): boolean {
  const targets: unknown = field(pkg, 'targets');
  return (
    Array.isArray(targets) &&
    targets.every((target: unknown) => {
      const kinds: unknown = field(target, 'kind');
      return (
        Array.isArray(kinds) &&
        kinds.every(
          (kind: unknown) => kind === 'lib' || kind === 'test' || kind === 'custom-build' || kind === 'proc-macro',
        )
      );
    })
  );
}

const [fixture, ...args] = process.argv.slice(2);
if (fixture === undefined) throw new Error('native fixture layout requires its owned fixture directory');
const metadata: unknown = JSON.parse(readFileSync(join(fixture, 'cargo-metadata.json'), 'utf8'));
const packages = field(metadata, 'packages');
const members = field(metadata, 'workspace_members');
const name = args.length === 2 && args[0] === '-p' ? args[1] : undefined;
const selected =
  name === undefined || !Array.isArray(packages) || !Array.isArray(members)
    ? []
    : packages.filter((pkg: unknown) => field(pkg, 'name') === name && members.includes(field(pkg, 'id')));
if (selected.length === 1 && onlySharedTargets(selected[0])) {
  process.stdout.write('shared');
} else {
  const target = field(metadata, 'target_directory');
  const build = field(metadata, 'build_directory');
  if (typeof target !== 'string' || !isAbsolute(target) || typeof build !== 'string' || !isAbsolute(build)) {
    throw new Error(
      'private native test exports require Cargo metadata with absolute target_directory and build_directory',
    );
  }
  // Neither the cold placement build nor the consumer receives these overrides. No step creates the shared target
  // before mbx can place it. A shared build view which is relinked or collected never owns the final executable.
  writeFileSync(
    join(fixture, 'layout.toml'),
    `[build]\nbuild-dir = ${JSON.stringify(build)}\ntarget-dir = ${JSON.stringify(join(fixture, 'final'))}\n`,
  );
  process.stdout.write(existsSync(target) ? 'private' : 'cold');
}
