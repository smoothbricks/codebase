import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  accessSync,
  constants,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { basename, delimiter, dirname, isAbsolute, join, resolve } from 'node:path';

const SCHEMA_TAG = 'native-fixture-metadata-v1';
const CARGO_ARGS = ['--frozen', 'metadata', '--format-version', '1', '--all-features'] as const;
const MAX_BUFFER_BYTES = 256 * 1024 * 1024;
const KEY_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const CONFIG_INCLUDE_PATTERN = /^\s*include\s*=/m;
const IGNORED_CARGO_ENV_KEYS = new Set(['CARGO_MAKEFLAGS', 'CARGO_BUILD_JOBS']);
const EXTRA_ENV_KEYS = new Set([
  'RUSTUP_TOOLCHAIN',
  'RUSTUP_HOME',
  'COWSHED_CARGO_ROUTE',
  'MBX_TARGET_VIEWS',
  'MBX_TARGET_ROOT',
  'MBX_DISABLE',
  'MBX_CACHE_DIR',
]);

interface MetadataInputs {
  readonly files: readonly string[];
  readonly listings: readonly string[];
  readonly exists: readonly string[];
}

interface StoredAnswer {
  readonly key: string;
  readonly inputs: MetadataInputs;
  readonly text: string;
}

interface FlightLock {
  unlock(): void;
}

function compareCodeUnits(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function field(value: unknown, key: string): unknown {
  return typeof value === 'object' && value !== null && key in value ? Reflect.get(value, key) : undefined;
}

function errorCode(error: unknown): string | undefined {
  if (!(error instanceof Error) || !('code' in error)) return undefined;
  return typeof error.code === 'string' ? error.code : undefined;
}

function isAbsent(error: unknown): boolean {
  const code = errorCode(error);
  return code === 'ENOENT' || code === 'ENOTDIR';
}

function isAccessMiss(error: unknown): boolean {
  const code = errorCode(error);
  return code === 'ENOENT' || code === 'EACCES' || code === 'ENOTDIR';
}

function resolveCargoHome(): string {
  const configured = process.env.CARGO_HOME;
  return resolve(configured !== undefined && configured !== '' ? configured : join(homedir(), '.cargo'));
}

function resolveRustupHome(): string {
  const configured = process.env.RUSTUP_HOME;
  return resolve(configured !== undefined && configured !== '' ? configured : join(homedir(), '.rustup'));
}

function isCargoConfigFile(file: string, cargoHome: string): boolean {
  const name = basename(file);
  if (name !== 'config' && name !== 'config.toml') return false;
  const parent = dirname(file);
  return parent === cargoHome || basename(parent) === '.cargo';
}

function cargoIdentity(realCwd: string): string[] {
  const binary = process.platform === 'win32' ? 'cargo.exe' : 'cargo';
  const rawRoute = process.env.COWSHED_CARGO_ROUTE;
  let routeDir: string | null = null;
  if (rawRoute !== undefined && rawRoute !== '') {
    try {
      routeDir = realpathSync.native(rawRoute);
    } catch (error) {
      if (!isAbsent(error)) throw error;
      routeDir = resolve(rawRoute);
    }
  }
  const found: string[] = [];
  for (const entry of (process.env.PATH ?? '').split(delimiter)) {
    const dir = entry === '' ? realCwd : resolve(realCwd, entry);
    const candidate = join(dir, binary);
    let real: string;
    let realDir: string;
    try {
      const info = statSync(candidate);
      if (!info.isFile()) continue;
      accessSync(candidate, constants.X_OK);
      real = realpathSync.native(candidate);
      realDir = realpathSync.native(dir);
    } catch (error) {
      if (isAccessMiss(error)) continue;
      throw error;
    }
    if (found.length === 0) {
      found.push(real);
      if (routeDir === null || realDir !== routeDir) return found;
    } else if (realDir !== routeDir) {
      found.push(real);
      return found;
    }
  }
  if (found.length === 0) found.push('<cargo-not-on-PATH>');
  return found;
}

function isAbsoluteStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every((item: unknown) => typeof item === 'string' && isAbsolute(item));
}

function parseInputs(value: unknown): MetadataInputs {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('inputs line is not a JSON object');
  }
  if (
    !Object.hasOwn(value, 'files') ||
    !Object.hasOwn(value, 'listings') ||
    !Object.hasOwn(value, 'exists') ||
    Object.keys(value).length !== 3
  ) {
    throw new Error('inputs object must contain exactly files, listings and exists');
  }
  const files = field(value, 'files');
  const listings = field(value, 'listings');
  const exists = field(value, 'exists');
  if (!isAbsoluteStringArray(files) || !isAbsoluteStringArray(listings) || !isAbsoluteStringArray(exists)) {
    throw new Error('inputs arrays must contain absolute path strings');
  }
  return { files, listings, exists };
}

function readStoredAnswer(file: string, warn: (message: string) => void): StoredAnswer | null {
  let content: string;
  try {
    content = readFileSync(file, 'utf8');
  } catch (error) {
    if (isAbsent(error)) return null;
    throw error;
  }
  try {
    const firstNewline = content.indexOf('\n');
    if (firstNewline === -1) throw new Error('missing key header');
    const key = content.slice(0, firstNewline);
    if (!KEY_PATTERN.test(key)) throw new Error('invalid content-key header');
    const secondNewline = content.indexOf('\n', firstNewline + 1);
    if (secondNewline === -1) throw new Error('missing inputs line');
    const inputs = parseInputs(JSON.parse(content.slice(firstNewline + 1, secondNewline)));
    const text = content.slice(secondNewline + 1);
    if (text.length === 0) throw new Error('empty metadata payload');
    return { key, inputs, text };
  } catch (error) {
    warn(
      `@smoothbricks/nx-plugin: discarding corrupt fixture Cargo metadata cache ${file}: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return null;
  }
}

function deriveInputs(text: string, realCwd: string): MetadataInputs {
  const parsed: unknown = JSON.parse(text);
  const rawWorkspaceRoot = field(parsed, 'workspace_root');
  if (typeof rawWorkspaceRoot !== 'string' || !isAbsolute(rawWorkspaceRoot)) {
    throw new Error('Cargo metadata did not report an absolute workspace_root');
  }
  const workspaceRoot = resolve(rawWorkspaceRoot);
  const rawPackages = field(parsed, 'packages');
  if (!Array.isArray(rawPackages)) {
    throw new Error('Cargo metadata did not report a packages array');
  }
  const files = new Set<string>([join(workspaceRoot, 'Cargo.lock')]);
  const listings = new Set<string>();
  const exists = new Set<string>();
  const ancestorStarts = new Set<string>([realCwd, workspaceRoot]);

  for (const pkg of rawPackages) {
    // Registry and git packages are pinned by Cargo.lock and immutable in Cargo's home: not keyed.
    if (field(pkg, 'source') !== null) continue;
    const rawManifest = field(pkg, 'manifest_path');
    if (typeof rawManifest !== 'string' || !isAbsolute(rawManifest)) {
      throw new Error('Cargo metadata path-source package has no absolute manifest_path');
    }
    const pkgDir = dirname(resolve(rawManifest));
    ancestorStarts.add(pkgDir);
    for (const subdir of ['src/bin', 'tests', 'examples', 'benches']) {
      listings.add(join(pkgDir, subdir));
    }
    for (const candidate of ['src/lib.rs', 'src/main.rs', 'build.rs']) {
      exists.add(join(pkgDir, candidate));
    }
    const rawTargets = field(pkg, 'targets');
    if (!Array.isArray(rawTargets)) {
      throw new Error('Cargo metadata path-source package has no targets array');
    }
    for (const target of rawTargets) {
      const rawSrc = field(target, 'src_path');
      if (typeof rawSrc !== 'string' || !isAbsolute(rawSrc)) {
        throw new Error('Cargo metadata target has no absolute src_path');
      }
      exists.add(resolve(rawSrc));
    }
  }

  const visited = new Set<string>();
  for (const start of ancestorStarts) {
    for (let dir = start; !visited.has(dir); dir = dirname(dir)) {
      visited.add(dir);
      files.add(join(dir, 'Cargo.toml'));
      files.add(join(dir, '.cargo', 'config'));
      files.add(join(dir, '.cargo', 'config.toml'));
      files.add(join(dir, 'rust-toolchain'));
      files.add(join(dir, 'rust-toolchain.toml'));
    }
  }

  const cargoHome = resolveCargoHome();
  files.add(join(cargoHome, 'config'));
  files.add(join(cargoHome, 'config.toml'));
  const rustupHome = resolveRustupHome();
  files.add(join(rustupHome, 'settings.toml'));

  return {
    files: [...files].sort(compareCodeUnits),
    listings: [...listings].sort(compareCodeUnits),
    exists: [...exists].sort(compareCodeUnits),
  };
}

function computeKey(
  realCwd: string,
  inputs: MetadataInputs,
): { readonly key: string; readonly hasConfigInclude: boolean } {
  const hash = createHash('sha256');
  const put = (value: string): void => {
    hash.update(value).update('\0');
  };

  put(SCHEMA_TAG);
  put(realCwd);
  for (const arg of CARGO_ARGS) put(arg);

  for (const executable of cargoIdentity(realCwd)) put(executable);
  put('');

  const envNames = Object.keys(process.env)
    .filter(
      (name) =>
        process.env[name] !== undefined &&
        ((name.startsWith('CARGO_') && !IGNORED_CARGO_ENV_KEYS.has(name)) || EXTRA_ENV_KEYS.has(name)),
    )
    .sort(compareCodeUnits);
  for (const name of envNames) put(`${name}=${process.env[name] ?? ''}`);
  put('');

  const cargoHome = resolveCargoHome();
  let hasConfigInclude = false;
  for (const file of inputs.files) {
    put(file);
    try {
      const bytes = readFileSync(file);
      if (
        !hasConfigInclude &&
        isCargoConfigFile(file, cargoHome) &&
        CONFIG_INCLUDE_PATTERN.test(bytes.toString('utf8'))
      ) {
        hasConfigInclude = true;
      }
      put(String(bytes.length));
      hash.update(bytes).update('\0');
    } catch (error) {
      if (!isAbsent(error)) throw error;
      put('absent');
    }
  }
  put('');

  for (const dir of inputs.listings) {
    put(dir);
    let names: string[];
    try {
      names = readdirSync(dir);
    } catch (error) {
      if (!isAbsent(error)) throw error;
      put('absent');
      continue;
    }
    names.sort(compareCodeUnits);
    put(String(names.length));
    for (const name of names) {
      const entryPath = join(dir, name);
      let hasMain = false;
      try {
        if (statSync(entryPath).isDirectory()) {
          try {
            hasMain = statSync(join(entryPath, 'main.rs')).isFile();
          } catch (error) {
            if (!isAbsent(error)) throw error;
          }
        }
      } catch (error) {
        if (!isAbsent(error)) throw error;
      }
      put(hasMain ? `${name}/main.rs` : name);
    }
  }
  put('');

  for (const file of inputs.exists) {
    put(file);
    let present = false;
    try {
      statSync(file);
      present = true;
    } catch (error) {
      if (!isAbsent(error)) throw error;
    }
    put(present ? '1' : '0');
  }

  return { key: hash.digest('base64url'), hasConfigInclude };
}

function allOlderThan(inputs: MetadataInputs, startedNs: bigint): boolean {
  for (const path of inputs.files) {
    try {
      if (statSync(path, { bigint: true }).mtimeNs >= startedNs) return false;
    } catch (error) {
      if (!isAbsent(error)) throw error;
    }
  }
  const checkedDirs = new Set<string>();
  for (const path of inputs.listings) {
    checkedDirs.add(path);
    try {
      if (statSync(path, { bigint: true }).mtimeNs >= startedNs) return false;
    } catch (error) {
      if (!isAbsent(error)) throw error;
    }
  }
  // Creating or removing an `exists` entry moves its parent directory's mtime.
  for (const path of inputs.exists) {
    const parent = dirname(path);
    if (checkedDirs.has(parent)) continue;
    checkedDirs.add(parent);
    try {
      if (statSync(parent, { bigint: true }).mtimeNs >= startedNs) return false;
    } catch (error) {
      if (!isAbsent(error)) throw error;
    }
  }
  return true;
}

function acquireFlightLock(lockFile: string): FlightLock {
  const native: unknown = createRequire(import.meta.url)('nx/src/native/index.js');
  if (field(native, 'IS_WASM') === true) {
    throw new Error('fixture Cargo metadata requires the native Nx file lock; a WASM Nx host cannot serialize flights');
  }
  const FileLockCtor = field(native, 'FileLock');
  if (typeof FileLockCtor !== 'function') {
    throw new Error('nx/src/native/index.js did not export a FileLock constructor');
  }
  const instance: unknown = Reflect.construct(FileLockCtor, [lockFile]);
  const lockMethod = field(instance, 'lock');
  const unlockMethod = field(instance, 'unlock');
  if (typeof lockMethod !== 'function' || typeof unlockMethod !== 'function') {
    throw new Error('Nx FileLock instance is missing lock/unlock methods');
  }
  Reflect.apply(lockMethod, instance, []);
  return {
    unlock: () => {
      Reflect.apply(unlockMethod, instance, []);
    },
  };
}

/**
 * Return the exact stdout text of `cargo --frozen metadata --format-version 1 --all-features` in `cwd`,
 * reusing a stored answer under `store` when every keyed input matches and serializing cold flights across processes.
 */
export function fixtureCargoMetadata(cwd: string, store: string): string {
  const realCwd = realpathSync.native(cwd);
  const directory = join(store, encodeURIComponent(realCwd));
  const storeFile = join(directory, 'answer');
  let warnedCorrupt = false;
  const warnCorrupt = (message: string): void => {
    if (warnedCorrupt) return;
    warnedCorrupt = true;
    process.stderr.write(message);
  };

  const warm = readStoredAnswer(storeFile, warnCorrupt);
  if (warm !== null && warm.key === computeKey(realCwd, warm.inputs).key) {
    return warm.text;
  }

  mkdirSync(directory, { recursive: true });
  const lock = acquireFlightLock(join(directory, 'flight.lock'));
  let pendingFile: string | null = null;
  try {
    const reread = readStoredAnswer(storeFile, warnCorrupt);
    if (reread !== null && reread.key === computeKey(realCwd, reread.inputs).key) {
      return reread.text;
    }

    mkdirSync(directory, { recursive: true });
    pendingFile = join(directory, `answer.${process.pid}.tmp`);
    writeFileSync(pendingFile, '');
    const startedNs = statSync(pendingFile, { bigint: true }).mtimeNs;

    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the fixture segment's own ceiling, which starts at spawn, ends a hung cargo
    const result = spawnSync('cargo', [...CARGO_ARGS], {
      cwd: realCwd,
      stdio: ['ignore', 'pipe', 'inherit'],
      maxBuffer: MAX_BUFFER_BYTES,
      encoding: 'utf8',
    });
    const command = `cargo ${CARGO_ARGS.join(' ')}`;
    if (result.error !== undefined) {
      throw new Error(`${command} failed to start in ${realCwd}: ${result.error.message}`, { cause: result.error });
    }
    if (result.signal !== null) {
      throw new Error(`${command} in ${realCwd} terminated by signal ${result.signal}`);
    }
    if (result.status !== 0) {
      throw new Error(`${command} in ${realCwd} exited with status ${result.status ?? 'unknown'}`);
    }

    const inputs = deriveInputs(result.stdout, realCwd);
    const { key, hasConfigInclude } = computeKey(realCwd, inputs);
    if (!hasConfigInclude && allOlderThan(inputs, startedNs)) {
      writeFileSync(pendingFile, `${key}\n${JSON.stringify(inputs)}\n${result.stdout}`);
      renameSync(pendingFile, storeFile);
    }
    return result.stdout;
  } finally {
    try {
      if (pendingFile !== null) rmSync(pendingFile, { force: true });
    } finally {
      lock.unlock();
    }
  }
}
