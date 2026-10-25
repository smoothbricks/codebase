/**
 * Which environment variables a Cargo target's build-script runs recorded with a value other than the one Cargo would
 * see now (specs/cowshed/16_build_volumes.md, "One Cargo environment on every path").
 *
 * `cargo:rerun-if-env-changed=VAR` makes VAR's value part of a build-script run's identity. Cargo records the value it
 * saw in the run's fingerprint (`fingerprint/run-build-script-*.json`, `local[].RerunIfEnvChanged`) and, on the next
 * build, reruns the script when the value it sees then differs, and rebuilds every unit that depends on the script's
 * package. The value Cargo sees is its `[env]` configuration's when that entry is forced or its own environment lacks
 * the variable, and its own environment's otherwise.
 */
import { readdirSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { type Result, readCargoFingerprint } from '@smoothbricks/cargo-retain';
import { parse as parseToml } from 'smol-toml';
import typia from 'typia';

/** One variable whose recorded value differs from the value Cargo would see now, and the runs that recorded it. */
export interface EnvironmentDrift {
  readonly variable: string;
  /** What the runs recorded; `null` is unset. */
  readonly recorded: string | null;
  /** What Cargo would see now; `null` is unset. */
  readonly current: string | null;
  /** The packages whose build scripts recorded it, sorted, each once. */
  readonly packages: readonly string[];
  /** How many build-script runs recorded it: a package's script runs once per profile, target and feature set. */
  readonly runs: number;
}

/** The value Cargo would give a variable now; `null` is unset. */
export type CargoSees = (variable: string) => string | null;

type CargoEnvEntry = string | { readonly value: string; readonly force?: boolean; readonly relative?: boolean };
interface CargoConfig {
  readonly env?: Readonly<Record<string, CargoEnvEntry>>;
}
const validateConfig = typia.createValidate<CargoConfig>();

/**
 * Variables Cargo itself sets for the processes it runs (build scripts, rustc and test binaries): Cargo's own
 * namespace and the build-script names it documents
 * (https://doc.rust-lang.org/cargo/reference/environment-variables.html). A run that recorded one with a value was
 * started by a `cargo` that a Cargo-run process spawned (a test that runs `cargo` passes on its own
 * `CARGO_MANIFEST_DIR`), and no shell gives or takes it.
 */
const SET_BY_CARGO: Readonly<Record<string, true>> = {
  CARGO: true,
  OUT_DIR: true,
  TARGET: true,
  HOST: true,
  NUM_JOBS: true,
  OPT_LEVEL: true,
  DEBUG: true,
  PROFILE: true,
  RUSTC: true,
  RUSTDOC: true,
  RUSTC_LINKER: true,
  RUSTC_WRAPPER: true,
  RUSTC_WORKSPACE_WRAPPER: true,
};

function describeCause(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorCode(error: unknown): string | undefined {
  return error instanceof Error && 'code' in error && typeof error.code === 'string' ? error.code : undefined;
}

/** The entries of `directory`, or none when it is absent or not a directory. */
function entries(directory: string): string[] {
  try {
    return readdirSync(directory);
  } catch (error) {
    // Nothing there, or a stray file where Cargo keeps a directory: no runs to read.
    if (errorCode(error) === 'ENOENT' || errorCode(error) === 'ENOTDIR') return [];
    throw error;
  }
}

/** Every build-script run fingerprint under one profile directory, with its package, in either Cargo layout. */
function buildScriptRuns(profile: string): { readonly pkg: string; readonly file: string }[] {
  const runs: { pkg: string; file: string }[] = [];
  const collect = (pkg: string, fingerprint: string): void => {
    for (const name of entries(fingerprint)) {
      if (name.startsWith('run-build-script-') && name.endsWith('.json'))
        runs.push({ pkg, file: join(fingerprint, name) });
    }
  };
  // `build/<package>/<hash>/fingerprint/`: the layout this repository's Cargo writes.
  const build = join(profile, 'build');
  for (const pkg of entries(build)) {
    for (const hash of entries(join(build, pkg))) collect(pkg, join(build, pkg, hash, 'fingerprint'));
  }
  // `.fingerprint/<package>-<hash>/`: the layout before it.
  const legacy = join(profile, '.fingerprint');
  for (const unit of entries(legacy)) collect(unit.replace(/-[0-9a-f]{16}$/, ''), join(legacy, unit));
  return runs;
}

/**
 * The drift between the build-script runs recorded under `profile` (a target directory's `debug`, say) and what
 * `sees` gives each recorded variable, one entry per variable and recorded value, sorted by variable. Variables Cargo
 * sets for the processes it runs are not compared. A missing profile directory has nothing to compare.
 */
export function buildScriptEnvironmentDrift(profile: string, sees: CargoSees): Result<EnvironmentDrift[], string> {
  const drift = new Map<
    string,
    { variable: string; recorded: string | null; current: string | null; packages: Set<string>; runs: number }
  >();
  let runs: { readonly pkg: string; readonly file: string }[];
  try {
    runs = buildScriptRuns(profile);
  } catch (error) {
    return { ok: false, error: `cannot list the build-script runs under ${profile}: ${describeCause(error)}` };
  }
  for (const { pkg, file } of runs) {
    let text: string;
    try {
      text = readFileSync(file, 'utf8');
    } catch (error) {
      return { ok: false, error: `cannot read ${file}: ${describeCause(error)}` };
    }
    const fingerprint = readCargoFingerprint(text);
    if (!fingerprint.ok) return { ok: false, error: `${file}: ${fingerprint.error}` };
    for (const local of fingerprint.value.local ?? []) {
      const recorded = local.RerunIfEnvChanged;
      if (recorded === undefined || recorded.var.startsWith('CARGO_') || SET_BY_CARGO[recorded.var] === true) continue;
      const current = sees(recorded.var);
      if (current === recorded.val) continue;
      const key = `${recorded.var}\u0000${recorded.val ?? '\u0000'}`;
      const entry = drift.get(key) ?? {
        variable: recorded.var,
        recorded: recorded.val,
        current,
        packages: new Set<string>(),
        runs: 0,
      };
      entry.packages.add(pkg);
      entry.runs += 1;
      drift.set(key, entry);
    }
  }
  return {
    ok: true,
    value: [...drift.values()]
      .map(({ packages, ...entry }) => ({ ...entry, packages: [...packages].sort() }))
      .sort((a, b) => (a.variable < b.variable ? -1 : a.variable > b.variable ? 1 : 0)),
  };
}

/** One Cargo configuration file: its text, and the directory its `relative` paths resolve against. */
export interface CargoConfigFile {
  readonly path: string;
  /** The parent of the `.cargo` directory holding the file (for `$CARGO_HOME/config.toml`, `$CARGO_HOME`'s parent). */
  readonly root: string;
  readonly text: string;
}

/**
 * The configuration files a Cargo invoked in `directory` reads, deepest first: each ancestor's `.cargo/config.toml`
 * (or `.cargo/config`, which Cargo prefers when both exist) up to `/`, then `$CARGO_HOME/config.toml` unless the walk
 * already read it.
 */
export function cargoConfigFiles(directory: string, cargoHome: string): Result<CargoConfigFile[], string> {
  const files: CargoConfigFile[] = [];
  const seen = new Set<string>();
  const read = (candidates: readonly string[], root: string): string | null => {
    for (const path of candidates) {
      let text: string;
      try {
        text = readFileSync(path, 'utf8');
      } catch (error) {
        if (errorCode(error) === 'ENOENT' || errorCode(error) === 'ENOTDIR') continue;
        return `cannot read ${path}: ${describeCause(error)}`;
      }
      const real = realpathSync(path);
      if (!seen.has(real)) files.push({ path, root, text });
      seen.add(real);
      return null;
    }
    return null;
  };
  for (let at = resolve(directory); ; at = dirname(at)) {
    const failed = read([join(at, '.cargo', 'config'), join(at, '.cargo', 'config.toml')], at);
    if (failed !== null) return { ok: false, error: failed };
    if (dirname(at) === at) break;
  }
  const home = resolve(cargoHome);
  const failed = read([join(home, 'config'), join(home, 'config.toml')], dirname(home));
  return failed === null ? { ok: true, value: files } : { ok: false, error: failed };
}

/**
 * What Cargo sees for a variable under `files` (deepest first, as `cargoConfigFiles` lists them) with `environment`
 * its own: the deepest `[env]` entry for it decides; a forced entry wins, else the environment's value, else the
 * entry's; a `relative` entry's value is a path under its file's root.
 */
export function cargoSees(
  files: readonly CargoConfigFile[],
  environment: Readonly<Record<string, string | undefined>>,
): Result<CargoSees, string> {
  const entries = new Map<string, { readonly entry: CargoEnvEntry; readonly root: string }>();
  for (const file of [...files].reverse()) {
    let parsed: unknown;
    try {
      parsed = parseToml(file.text);
    } catch (error) {
      return { ok: false, error: `${file.path} is not TOML: ${describeCause(error)}` };
    }
    const checked = validateConfig(parsed);
    if (!checked.success) {
      return {
        ok: false,
        error: `${file.path} [env]: ${checked.errors.map((e) => `${e.path} expected ${e.expected}`).join('; ')}`,
      };
    }
    for (const [variable, entry] of Object.entries(checked.data.env ?? {}))
      entries.set(variable, { entry, root: file.root });
  }
  return {
    ok: true,
    value: (variable) => {
      const own = environment[variable] ?? null;
      const configured = entries.get(variable);
      if (configured === undefined) return own;
      const { entry, root } = configured;
      const { value, force = false, relative = false } = typeof entry === 'string' ? { value: entry } : entry;
      if (own !== null && !force) return own;
      return relative ? join(root, value) : value;
    },
  };
}

/** One line for a drift: the variable, both values, and the runs Cargo reruns for it. */
export function describeDrift(drift: EnvironmentDrift): string {
  const [recorded, current] = [drift.recorded, drift.current].map((value) =>
    value === null ? 'unset' : JSON.stringify(value),
  );
  return `${drift.variable}: ${drift.runs} build-script run(s) of ${drift.packages.join(', ')} recorded ${recorded}; Cargo now sees ${current}, so it reruns them and rebuilds what depends on them`;
}

/**
 * `<checkout>`: print each drift between `<checkout>/target/debug`'s build-script runs and what a Cargo invoked in
 * `<checkout>` with this process's environment would see, one line each. Exits 0 when it could compare, drift or not;
 * 1 when a record or a configuration file cannot be read; 64 on a usage error.
 */
export function runBuildScriptEnvironmentDrift(args: readonly string[]): number {
  const [checkout] = args;
  if (checkout === undefined || args.length !== 1) {
    process.stderr.write('usage: cargo-build-script-env <checkout>\n');
    return 64;
  }
  const home = process.env.CARGO_HOME ?? join(homedir(), '.cargo');
  const files = cargoConfigFiles(checkout, home);
  const sees = files.ok ? cargoSees(files.value, process.env) : files;
  const drift = sees.ok ? buildScriptEnvironmentDrift(join(checkout, 'target', 'debug'), sees.value) : sees;
  if (!drift.ok) {
    process.stderr.write(`cargo-build-script-env: ${drift.error}\n`);
    return 1;
  }
  for (const entry of drift.value) process.stdout.write(`cargo-build-script-env: ${describeDrift(entry)}\n`);
  return 0;
}
