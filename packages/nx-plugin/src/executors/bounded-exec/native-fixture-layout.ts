/** Obtain the workspace's Cargo metadata from the content-keyed fixture store, write this invocation's private
 * `cargo-metadata.json`, and select a private Cargo final target only when this native test recipe can promote an
 * executable or library. Runs inside the bounded fixture segment, before any test or custom consumer: shared mode
 * writes Cargo's answer verbatim; private mode writes `layout.toml` and Cargo's answer with `target_directory` set
 * to `<fixture>/final`. */
import { writeFileSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';
import { fixtureCargoMetadata } from './native-fixture-metadata.js';
import { FIXTURE_STORE_ENV } from './native-test.js';

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
const store = process.env[FIXTURE_STORE_ENV];
if (store === undefined || store === '') {
  throw new Error(`native fixture layout requires ${FIXTURE_STORE_ENV} to be set`);
}
const text = fixtureCargoMetadata(process.cwd(), store);
const metadata: unknown = JSON.parse(text);
if (typeof metadata !== 'object' || metadata === null || Array.isArray(metadata)) {
  throw new Error('Cargo metadata did not parse as a JSON object');
}
const packages = field(metadata, 'packages');
const members = field(metadata, 'workspace_members');
const name = args.length === 2 && args[0] === '-p' ? args[1] : undefined;
const selected =
  name === undefined || !Array.isArray(packages) || !Array.isArray(members)
    ? []
    : packages.filter((pkg: unknown) => field(pkg, 'name') === name && members.includes(field(pkg, 'id')));
if (selected.length === 1 && onlySharedTargets(selected[0])) {
  writeFileSync(join(fixture, 'cargo-metadata.json'), text);
  process.stdout.write('shared');
} else {
  const build = field(metadata, 'build_directory');
  if (typeof build !== 'string' || !isAbsolute(build)) {
    throw new Error('private native test exports require Cargo metadata with an absolute build_directory');
  }
  const finalDirectory = join(fixture, 'final');
  writeFileSync(
    join(fixture, 'layout.toml'),
    `[build]\nbuild-dir = ${JSON.stringify(build)}\ntarget-dir = ${JSON.stringify(finalDirectory)}\n`,
  );
  writeFileSync(
    join(fixture, 'cargo-metadata.json'),
    JSON.stringify({ ...metadata, target_directory: finalDirectory }),
  );
  process.stdout.write('private');
}
