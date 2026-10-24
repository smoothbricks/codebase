/**
 * Retention of unused Cargo build outputs: the superseded unit generations under a Cargo
 * target directory's new build-dir layout, `<profile>/build/<package>/<META>/`.
 *
 * Cargo never deletes a generation a later build superseded (rust-lang/cargo#13062: "we
 * aren't garbage collecting build-dirs yet"), so a busy checkout's target directory grows
 * without bound. Age alone cannot tell a superseded generation from a current one, and this
 * volume is mounted `noatime`. The selector therefore reads Cargo's own records only:
 *
 * - Every `fingerprint/<kind>.json` names its unit's identity (every field but `deps` and
 *   `local`, unknown fields included) and its dependency edges (`deps[i][3]`, the dependency's
 *   fingerprint hash). Integers are kept exact: `JSON.parse` hands each number's source text
 *   to the reviver, so a u64 is never rounded through a double.
 * - The hash file beside it (`fingerprint/<kind>`) holds that unit's own fingerprint hash as
 *   the 16 hex digits of the u64's little-endian bytes; an edge resolves to every unit whose
 *   hash file holds it. Cargo stamps the hash file's mtime on every use when
 *   `-Zmtime-on-use` is on, and `invoked.timestamp` marks the unit's last compile.
 * - Roots are the newest generation of each identity of a workspace member package, and
 *   every unit used within {@link RECENT_USE_MS} of now. Kept is the transitive dependency
 *   closure of the roots across every profile of the target directory: a cross build's
 *   build scripts and proc macros live in the host profile. An edge whose hash no unit holds
 *   keeps every unit whose kind could carry the dependency's name. A generation is reclaimed
 *   only when none of its units is kept.
 *
 * No fingerprint graph is read or changed unless every profile's Cargo locks are held: an
 * exclusive non-blocking flock(2) on each profile's `.cargo-lock` (every Cargo build holds it
 * shared), `.cargo-build-lock` and, where Cargo created it, `.cargo-artifact-lock`. A held
 * lock makes the pass `busy` and leaves the directory as it was; the pass never waits behind
 * a build. Under that one hold the pass plans and renames each reclaimed generation into the
 * profile's `.smoo-retention-trash/<pass>/`, listing its device and inode in a manifest
 * first; it unlinks the trash after letting go of the Cargo locks, under the target's own
 * retention lock, and deletes only manifest entries whose inode matches. Anything this
 * selector cannot read (a legacy layout, an unreadable fingerprint, a profile without
 * `build/` or without Cargo's locks) refuses the whole pass: its edges are unknowable.
 *
 * Not reclaimed here: `incremental/`, nested target directories, and the split-debuginfo
 * objects rustc keeps inside current units (rust-lang/rust#161824).
 */
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { lstatSync, readdirSync, readFileSync } from 'node:fs';
import { lstat, mkdir, readdir, readFile, realpath, rename, rmdir, unlink, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, normalize, relative, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline';
import { logger, type ProjectGraph, readCachedProjectGraph } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import typia, { type tags } from 'typia';
import { CARGO_ANCESTOR_INPUTS } from './cargo-source-hash.js';

export type Result<T, E> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: E };

export type RetentionError =
  | { readonly kind: 'cargo-metadata'; readonly cwd: string; readonly detail: string }
  | { readonly kind: 'target-not-cargo'; readonly targetDir: string; readonly detail: string }
  | { readonly kind: 'lossless-json-unavailable'; readonly detail: string }
  | { readonly kind: 'lock-helper'; readonly detail: string }
  | { readonly kind: 'io'; readonly operation: string; readonly path: string; readonly detail: string };

export type ReclaimReason = 'superseded-generation';
export type KeepReason =
  | 'newest-of-identity'
  | 'recently-used'
  | 'dependency-of-kept'
  | 'dangling-dependency'
  | 'no-fingerprint'
  | 'not-a-unit'
  | 'incomplete-fingerprint'
  | 'unproven-trash-entry';

/** A reclaimed (or, planned, reclaimable) generation: `bytes` free with it, `linkedBytes` stay linked elsewhere. */
export interface Reclaimable {
  readonly path: string;
  readonly bytes: number;
  readonly linkedBytes: number;
  readonly reason: ReclaimReason;
}
export interface Retained {
  readonly path: string;
  readonly reason: KeepReason;
  readonly detail?: string;
}
export type RetentionOutcome =
  | {
      readonly kind: 'planned' | 'reclaimed';
      readonly reclaim: readonly Reclaimable[];
      readonly reclaimedBytes: number;
      readonly linkedBytes: number;
      readonly retained: readonly Retained[];
    }
  | { readonly kind: 'not-due'; readonly nextDueAt: number }
  | { readonly kind: 'busy'; readonly lock: string }
  | { readonly kind: 'refused'; readonly profile: string; readonly reason: string };
export interface RetentionReport {
  readonly workspaceRoot: string;
  readonly targetDir: string;
  readonly nestedRoots: readonly string[];
  readonly outcome: RetentionOutcome;
  /** Scheduling evidence only, never a cached selection or proof about later outputs. */
  readonly completed?: { readonly at: number; readonly configuration: string };
}
export interface RetentionOptions {
  readonly apply: boolean;
  /** Only the automatic hook opts into the successful receipt's maintenance interval. */
  readonly automatic?: boolean;
  /** The clock {@link RECENT_USE_MS} counts back from. */
  readonly now?: Date;
}

/** A unit used this recently is a root whatever its generation: one working day of alternating commands. */
export const RECENT_USE_MS = 24 * 60 * 60 * 1000;
export const AUTOMATIC_INTERVAL_MS = 24 * 60 * 60 * 1000;
const SCHEDULE_VERSION = 1;
const validateRetentionReport = typia.createValidate<RetentionReport>();
export const RETENTION_RECEIPT = '.smoo-cargo-retention.json';
/** The installed retention CLI, relative to the workspace root (`smoo-nx-cargo-retain`). */
export const CARGO_RETAIN_BIN = 'node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-cargo-retain.js';
const RETENTION_LOCK = '.smoo-cargo-retention.lock';
const TRASH = '.smoo-retention-trash';
/** Cargo's optional backup tag; not created when the target directory already exists (`paths::create_dir_all_excluded_from_backups_atomic`). */
const CACHEDIR_SIGNATURE = 'Signature: 8a477f597d28d172789f06886806bc55';
/** Cargo holds `.cargo-lock` shared and `.cargo-build-lock` exclusively for every build of a profile. */
const REQUIRED_LOCKS = ['.cargo-lock', '.cargo-build-lock'] as const;
/** Created only by builds that take it (a `cargo check` without `--timings` never does). */
const OPTIONAL_LOCKS = ['.cargo-artifact-lock'] as const;
const LEGACY_MARKERS = ['.fingerprint', 'deps'] as const;
const META_NAME = /^[0-9a-f]{16}$/;
const HASH_TEXT = /^[0-9a-f]{16}$/;
const INTEGER = /^-?(?:0|[1-9]\d*)$/;
const MANIFEST_NAME = /^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.json$/;
const SECTOR = 512n;

type U64 = bigint & tags.Type<'uint64'>;
/** One `fingerprint/<kind>.json`: Cargo's `Fingerprint`, integers exact. Additive: unknown fields enter the identity. */
export interface CargoFingerprint {
  readonly deps: readonly (readonly [U64, string, boolean, U64])[];
  readonly local?: readonly {
    readonly CheckDepInfo?: { readonly dep_info: string };
    readonly RerunIfChanged?: { readonly output: string };
  }[];
  readonly [field: string]: unknown;
}
const validateFingerprint = typia.createValidate<CargoFingerprint>();

interface CargoMetadata {
  readonly target_directory: string;
  readonly workspace_root: string;
  readonly packages: readonly { readonly name: string }[];
}
const validateMetadata = typia.createValidate<CargoMetadata>();

interface TrashManifest {
  readonly version: 1;
  readonly entries: readonly { readonly from: string; readonly dev: string; readonly ino: string }[];
}
const validateManifest = typia.createValidate<TrashManifest>();

interface JsonSourceContext {
  readonly source?: string;
}

/** Every integer as a bigint from its source digits; the reviver throws rather than round. */
function exactIntegers(_key: string, value: unknown, context?: JsonSourceContext): unknown {
  if (typeof value !== 'number') return value;
  const source = context?.source;
  if (source === undefined) throw new Error('JSON.parse gave the reviver no number source text');
  if (!INTEGER.test(source)) throw new Error(`non-integer number ${source}`);
  return BigInt(source);
}

/** Whether this runtime hands a reviver each number's source text (the JSON.parse source text access proposal). */
function losslessJsonAvailable(): boolean {
  const probe: unknown = JSON.parse(
    '[18446744073709551615]',
    (_key: string, value: unknown, context?: JsonSourceContext) =>
      typeof value === 'number' ? context?.source : value,
  );
  return Array.isArray(probe) && probe[0] === '18446744073709551615';
}

export function readCargoFingerprint(text: string): Result<CargoFingerprint, string> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text, exactIntegers);
  } catch (error) {
    return {
      ok: false,
      error: `unreadable fingerprint JSON: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  const checked = validateFingerprint(parsed);
  if (!checked.success) {
    return {
      ok: false,
      error: `unexpected fingerprint shape: ${checked.errors.map((e) => `${e.path} expected ${e.expected}`).join('; ')}`,
    };
  }
  return { ok: true, value: checked.data };
}

/** Tagged canonical text: a string, an integer and a boolean never spell the same. */
function canonical(value: unknown): string {
  switch (typeof value) {
    case 'bigint':
      return `n${value}`;
    case 'string':
      return `s${JSON.stringify(value)}`;
    case 'boolean':
      return `b${value}`;
    case 'object':
      if (value === null) return 'z';
      if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
      return `{${Object.keys(value)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${canonical(Reflect.get(value, key))}`)
        .join(',')}}`;
    default:
      throw new Error(`a JSON value cannot be ${typeof value}`);
  }
}

/** Package, kind and every fingerprint field but its edges (`deps`) and source state (`local`). */
export function fingerprintIdentity(pkg: string, kind: string, fingerprint: CargoFingerprint): string {
  const fields = Object.keys(fingerprint)
    .filter((field) => field !== 'deps' && field !== 'local')
    .sort()
    .map((field) => `${JSON.stringify(field)}=${canonical(fingerprint[field])}`);
  return [JSON.stringify(pkg), JSON.stringify(kind), ...fields].join('\u0000');
}

/** The hash file's spelling of a fingerprint hash: its little-endian bytes in hex (Cargo's `util::to_hex`). */
function hashText(hash: bigint): string {
  let text = '';
  for (let byte = 0n; byte < 8n; byte += 1n) text += ((hash >> (8n * byte)) & 0xffn).toString(16).padStart(2, '0');
  return text;
}

function describeCause(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorCode(error: unknown): string | undefined {
  return error instanceof Error && 'code' in error && typeof error.code === 'string' ? error.code : undefined;
}

async function lstatOrUndefined(path: string) {
  try {
    return await lstat(path, { bigint: true });
  } catch (error) {
    if (errorCode(error) === 'ENOENT') return undefined;
    throw error;
  }
}

function ioError(operation: string, path: string, error: unknown): RetentionError {
  return { kind: 'io', operation, path, detail: describeCause(error) };
}

// ---- Cargo's locks, held by a perl child: its flock(2) is the one Cargo takes, it lives as
// long as its stdin pipe, and the kernel lets go of it however the holder ends. ----

const LOCK_HELPER = [
  'use strict; use Fcntl qw(:flock O_RDONLY O_CREAT); $| = 1;',
  'my $mode = shift @ARGV; my @held;',
  'for my $path (@ARGV) {',
  '  my $flags = $mode eq "create" ? O_RDONLY | O_CREAT : O_RDONLY;',
  '  sysopen(my $fh, $path, $flags, 0644) or do { print "error\\t$path\\t$!\\n"; exit 0 };',
  '  if (!flock($fh, LOCK_EX | LOCK_NB)) { print(($!{EWOULDBLOCK} ? "busy\\t$path\\n" : "error\\t$path\\t$!\\n")); exit 0 }',
  '  push @held, $fh;',
  '}',
  'print "held\\n";',
  '1 while defined(my $line = <STDIN>);',
].join('\n');

interface Hold {
  release(): Promise<void>;
}
type HoldAttempt = { readonly kind: 'held'; readonly hold: Hold } | { readonly kind: 'busy'; readonly lock: string };

/** Every lock in `paths` exclusively and without waiting, in order; the first held one makes it `busy`. */
async function tryHold(
  paths: readonly string[],
  mode: 'existing' | 'create',
): Promise<Result<HoldAttempt, RetentionError>> {
  const child = spawn('perl', ['-e', LOCK_HELPER, mode, ...paths], { stdio: ['pipe', 'pipe', 'pipe'] });
  const exited = new Promise<void>((resolve) => child.once('close', () => resolve()));
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', (chunk: string) => {
    stderr += chunk;
  });
  const lines = createInterface({ input: child.stdout });
  const first = await new Promise<Result<string, string>>((resolve) => {
    child.once('error', (error) => resolve({ ok: false, error: `cannot run perl: ${error.message}` }));
    lines.once('line', (line) => resolve({ ok: true, value: line }));
    lines.once('close', () => resolve({ ok: false, error: `perl ended before answering: ${stderr.trim()}` }));
  });
  if (!first.ok) {
    child.stdin.destroy();
    return { ok: false, error: { kind: 'lock-helper', detail: first.error } };
  }
  const [verdict, path, detail] = first.value.split('\t');
  if (verdict === 'held') {
    return {
      ok: true,
      value: {
        kind: 'held',
        hold: {
          async release() {
            child.stdin.end();
            await exited;
          },
        },
      },
    };
  }
  child.stdin.end();
  await exited;
  if (verdict === 'busy' && path !== undefined) return { ok: true, value: { kind: 'busy', lock: path } };
  return {
    ok: false,
    error: {
      kind: 'lock-helper',
      detail: verdict === 'error' ? `${path}: ${detail}` : `unexpected answer ${first.value}`,
    },
  };
}

// ---- The target directory's profiles ----

interface TargetLayout {
  readonly profiles: readonly string[];
}

async function isFile(path: string): Promise<boolean> {
  return (await lstatOrUndefined(path))?.isFile() === true;
}

async function subdirectories(path: string): Promise<string[]> {
  const found: string[] = [];
  for (const name of (await readdir(path)).sort()) {
    if ((await lstatOrUndefined(join(path, name)))?.isDirectory() === true) found.push(join(path, name));
  }
  return found;
}

/**
 * Candidate profiles hold Cargo's `.cargo-lock` at depth one or two. A nested target
 * can have the same shape as a cross profile, and rustc's cache file is optional.
 * The locked scan proves their root from Cargo's own root-relative local paths.
 */
async function discoverLayout(targetDir: string): Promise<Result<TargetLayout, RetentionError>> {
  try {
    const tag = join(targetDir, 'CACHEDIR.TAG');
    const tagStats = await lstatOrUndefined(tag);
    if (
      tagStats !== undefined &&
      (!tagStats.isFile() || !(await readFile(tag, 'utf8')).startsWith(CACHEDIR_SIGNATURE))
    ) {
      return { ok: false, error: { kind: 'target-not-cargo', targetDir, detail: `${tag} is not Cargo's cache tag` } };
    }
    const profiles: string[] = [];
    for (const directory of await subdirectories(targetDir)) {
      if (await isFile(join(directory, '.cargo-lock'))) {
        profiles.push(directory);
      } else {
        for (const inner of await subdirectories(directory)) {
          if (await isFile(join(inner, '.cargo-lock'))) profiles.push(inner);
        }
      }
    }
    if (profiles.length === 0) {
      return { ok: false, error: { kind: 'target-not-cargo', targetDir, detail: 'no Cargo profile locks' } };
    }
    return { ok: true, value: { profiles: profiles.sort() } };
  } catch (error) {
    return { ok: false, error: ioError('discover profiles', targetDir, error) };
  }
}

// ---- Units ----

interface Unit {
  readonly meta: Meta;
  readonly kind: string;
  /** Absent while a compile of the unit is unfinished: Cargo empties the hash file before compiling a dirty unit. */
  readonly hash: string | undefined;
  readonly identity: string;
  readonly deps: readonly { readonly hash: string; readonly name: string }[];
  readonly lastUse: number;
}
interface Meta {
  readonly profile: string;
  readonly pkg: string;
  readonly path: string;
  readonly units: Unit[];
}
interface ProfileScan {
  readonly root: string | undefined;
  readonly metas: readonly Meta[];
  readonly anomalies: readonly Retained[];
}

function mtimeMs(path: string): number | undefined {
  const stats = lstatSync(path, { bigint: true, throwIfNoEntry: false });
  return stats?.isFile() === true ? Number(stats.mtimeMs) : undefined;
}

/** Directory types already returned by the kernel; never follow a symbolic link to classify an entry. */
function directoryEntries(path: string) {
  return readdirSync(path, { withFileTypes: true }).sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

/**
 * A refusal names why; nothing in the pass is touched. Small metadata reads are synchronous:
 * sequential libuv dispatch cost more than the reads. The enclosing pass already holds Cargo's
 * locks, and every invocation still reads the current fingerprints, including in-place changes.
 */
function scanProfile(profile: string, targetDir: string): Result<ProfileScan, string> {
  let root: string | undefined;
  const ownPrefix = relative(targetDir, join(profile, 'build')) + sep;
  const nestedRoot = dirname(profile);
  const nestedPrefix = relative(nestedRoot, join(profile, 'build')) + sep;
  const profileEntries = directoryEntries(profile);
  for (const marker of LEGACY_MARKERS) {
    if (profileEntries.some((entry) => entry.name === marker)) {
      return { ok: false, error: `legacy target layout: ${marker} is present` };
    }
  }
  const build = join(profile, 'build');
  if (!profileEntries.some((entry) => entry.name === 'build' && entry.isDirectory())) {
    return { ok: false, error: 'no build directory' };
  }
  const metas: Meta[] = [];
  const anomalies: Retained[] = [];
  for (const pkgEntry of directoryEntries(build)) {
    const pkg = pkgEntry.name;
    const pkgPath = join(build, pkg);
    if (!pkgEntry.isDirectory()) {
      anomalies.push({ path: pkgPath, reason: 'not-a-unit', detail: 'not a directory' });
      continue;
    }
    for (const entry of directoryEntries(pkgPath)) {
      const name = entry.name;
      const path = join(pkgPath, name);
      if (!entry.isDirectory()) {
        anomalies.push({
          path,
          reason: 'not-a-unit',
          detail: entry.isSymbolicLink() ? 'symbolic link' : 'not a directory',
        });
        continue;
      }
      if (!META_NAME.test(name)) {
        anomalies.push({ path, reason: 'not-a-unit', detail: 'not a 16-hex unit directory' });
        continue;
      }
      const contents = directoryEntries(path);
      const fingerprintDir = join(path, 'fingerprint');
      const fingerprintEntry = contents.find((entry) => entry.name === 'fingerprint');
      if (fingerprintEntry === undefined) {
        anomalies.push({ path, reason: 'no-fingerprint' });
        continue;
      }
      if (!fingerprintEntry.isDirectory()) {
        anomalies.push({ path, reason: 'not-a-unit', detail: 'fingerprint is not a directory' });
        continue;
      }
      const files = directoryEntries(fingerprintDir);
      const jsonFiles = files.filter((file) => file.name.endsWith('.json'));
      if (jsonFiles.length === 0) {
        anomalies.push({ path, reason: 'no-fingerprint' });
        continue;
      }
      const invoked = Math.max(
        files.some((file) => file.name === 'invoked.timestamp' && file.isFile())
          ? (mtimeMs(join(fingerprintDir, 'invoked.timestamp')) ?? 0)
          : 0,
        contents.some((entry) => entry.name === 'run' && entry.isDirectory())
          ? (mtimeMs(join(path, 'run', 'invoked.timestamp')) ?? 0)
          : 0,
      );
      const meta: Meta = { profile, pkg, path, units: [] };
      for (const jsonFile of jsonFiles) {
        const kind = jsonFile.name.slice(0, -'.json'.length);
        const jsonPath = join(fingerprintDir, jsonFile.name);
        const hashPath = join(fingerprintDir, kind);
        if (!jsonFile.isFile() || !files.some((file) => file.name === kind && file.isFile())) {
          return { ok: false, error: `unreadable fingerprint ${jsonPath}: it or its hash file is not a regular file` };
        }
        const hash = readFileSync(hashPath, 'utf8');
        if (hash !== '' && !HASH_TEXT.test(hash))
          return { ok: false, error: `unreadable fingerprint hash ${hashPath}` };
        const fingerprint = readCargoFingerprint(readFileSync(jsonPath, 'utf8'));
        if (!fingerprint.ok) return { ok: false, error: `${jsonPath}: ${fingerprint.error}` };
        // LocalFingerprint paths are anchored at Cargo's target_root, not the package
        // (cargo/src/compiler/fingerprint/mod.rs). Check every witness under the lock.
        for (const local of fingerprint.value.local ?? []) {
          for (const name of [local.CheckDepInfo?.dep_info, local.RerunIfChanged?.output]) {
            if (name === undefined) continue;
            const path = normalize(name);
            const observed = path.startsWith(ownPrefix)
              ? targetDir
              : nestedRoot !== targetDir && path.startsWith(nestedPrefix)
                ? nestedRoot
                : undefined;
            if (observed === undefined || (root !== undefined && root !== observed)) {
              return { ok: false, error: `unproven or mixed build root in ${jsonPath}: ${name}` };
            }
            root = observed;
          }
        }
        meta.units.push({
          meta,
          kind,
          hash: hash === '' ? undefined : hash,
          identity: fingerprintIdentity(pkg, kind, fingerprint.value),
          deps: fingerprint.value.deps.map(([, depName, , depHash]) => ({ hash: hashText(depHash), name: depName })),
          lastUse: Math.max(mtimeMs(hashPath) ?? 0, invoked),
        });
      }
      metas.push(meta);
    }
  }
  if (root === undefined && nestedRoot !== targetDir) {
    return { ok: false, error: 'no fingerprint local path proves this depth-two profile belongs to the target' };
  }
  return { ok: true, value: { root, metas, anomalies } };
}

// ---- Selection ----

const KEEP_PRIORITY: readonly KeepReason[] = [
  'newest-of-identity',
  'recently-used',
  'dependency-of-kept',
  'dangling-dependency',
];

interface Selection {
  readonly reclaim: readonly Meta[];
  readonly kept: readonly Retained[];
}

function select(metas: readonly Meta[], members: ReadonlySet<string>, now: number): Selection {
  const units = metas.flatMap((meta) => meta.units);
  const byHash = new Map<string, Unit[]>();
  const byIdentity = new Map<string, Unit[]>();
  for (const unit of units) {
    if (unit.hash !== undefined) {
      const sameHash = byHash.get(unit.hash);
      if (sameHash === undefined) byHash.set(unit.hash, [unit]);
      else sameHash.push(unit);
    }
    if (!members.has(unit.meta.pkg)) continue;
    const generations = byIdentity.get(unit.identity);
    if (generations === undefined) byIdentity.set(unit.identity, [unit]);
    else generations.push(unit);
  }
  const kept = new Map<Unit, KeepReason>();
  const pending: Unit[] = [];
  const keep = (unit: Unit, reason: KeepReason) => {
    const prior = kept.get(unit);
    if (prior === undefined) pending.push(unit);
    if (prior === undefined || KEEP_PRIORITY.indexOf(reason) < KEEP_PRIORITY.indexOf(prior)) kept.set(unit, reason);
  };
  for (const generations of byIdentity.values()) {
    const newest = Math.max(...generations.map((unit) => unit.lastUse));
    for (const unit of generations) if (unit.lastUse === newest) keep(unit, 'newest-of-identity');
  }
  for (const unit of units) if (now - unit.lastUse < RECENT_USE_MS) keep(unit, 'recently-used');
  const danglingCache = new Map<string, readonly Unit[]>();
  const carriersOf = (name: string): readonly Unit[] => {
    const suffix = `_${name.replaceAll('-', '_')}`;
    let found = danglingCache.get(suffix);
    if (found === undefined) {
      found = units.filter((unit) => unit.kind.replaceAll('-', '_').endsWith(suffix));
      danglingCache.set(suffix, found);
    }
    return found;
  };
  for (let unit = pending.pop(); unit !== undefined; unit = pending.pop()) {
    for (const dep of unit.deps) {
      const resolved = byHash.get(dep.hash);
      if (resolved === undefined) for (const carrier of carriersOf(dep.name)) keep(carrier, 'dangling-dependency');
      else for (const target of resolved) keep(target, 'dependency-of-kept');
    }
  }
  const reclaim: Meta[] = [];
  const keptMetas: Retained[] = [];
  for (const meta of metas) {
    const reasons = meta.units.flatMap((unit) => kept.get(unit) ?? []);
    const reason =
      KEEP_PRIORITY.find((candidate) => reasons.includes(candidate)) ??
      (meta.units.some((unit) => unit.hash === undefined) ? 'incomplete-fingerprint' : undefined);
    if (reason === undefined) reclaim.push(meta);
    else keptMetas.push({ path: meta.path, reason });
  }
  return { reclaim, kept: keptMetas };
}

// ---- Bytes ----

interface Bytes {
  bytes: number;
  linkedBytes: number;
}

/** Allocated bytes under `path` (never following a link): those whose every link is inside it, and the rest. */
async function measureTree(path: string): Promise<Bytes> {
  const inodes = new Map<string, { seen: bigint; nlink: bigint; blocks: bigint }>();
  const walk = async (entry: string): Promise<void> => {
    const stats = await lstat(entry, { bigint: true });
    if (stats.isDirectory()) {
      for (const name of await readdir(entry)) await walk(join(entry, name));
      return;
    }
    const key = `${stats.dev}:${stats.ino}`;
    const known = inodes.get(key);
    if (known === undefined) inodes.set(key, { seen: 1n, nlink: stats.nlink, blocks: stats.blocks });
    else known.seen += 1n;
  };
  await walk(path);
  let bytes = 0n;
  let linkedBytes = 0n;
  for (const inode of inodes.values()) {
    if (inode.seen >= inode.nlink) bytes += inode.blocks * SECTOR;
    else linkedBytes += inode.blocks * SECTOR;
  }
  return { bytes: Number(bytes), linkedBytes: Number(linkedBytes) };
}

/**
 * Unlink everything under `path`, never following a link. `bytes` are freed by these unlinks;
 * `linkedBytes` were still linked elsewhere when unlinked. `linked` carries the inodes still
 * linked across the pass, so a later unlink that frees one takes it out again.
 */
async function removeTree(path: string, linked: Map<string, bigint>): Promise<Bytes> {
  let freed = 0n;
  let stillLinked = 0n;
  const remove = async (entry: string): Promise<void> => {
    const stats = await lstat(entry, { bigint: true });
    if (stats.isDirectory()) {
      for (const name of await readdir(entry)) await remove(join(entry, name));
      await rmdir(entry);
      return;
    }
    const key = `${stats.dev}:${stats.ino}`;
    await unlink(entry);
    if (stats.nlink === 1n) {
      freed += stats.blocks * SECTOR;
      linked.delete(key);
    } else {
      linked.set(key, stats.blocks * SECTOR);
      stillLinked += stats.blocks * SECTOR;
    }
  };
  await remove(path);
  return { bytes: Number(freed), linkedBytes: Number(stillLinked) };
}

// ---- Trash ----

interface Moved {
  readonly from: string;
  readonly dev: string;
  readonly ino: string;
}

/** Under the Cargo hold: manifest first, then each rename, so every trash entry has a proven origin. */
async function moveToTrash(profile: string, victims: readonly Meta[]): Promise<Result<void, RetentionError>> {
  const trash = join(profile, TRASH);
  try {
    const trashStats = await lstatOrUndefined(trash);
    if (trashStats === undefined) await mkdir(trash);
    else if (!trashStats.isDirectory()) {
      return { ok: false, error: { kind: 'io', operation: 'use trash', path: trash, detail: 'not a directory' } };
    }
    const pass = randomUUID();
    const entries: Moved[] = [];
    for (const victim of victims) {
      const stats = await lstat(victim.path, { bigint: true });
      entries.push({ from: victim.path, dev: stats.dev.toString(), ino: stats.ino.toString() });
    }
    const manifest: TrashManifest = { version: 1, entries };
    await writeFile(join(trash, `${pass}.json`), JSON.stringify(manifest), { flag: 'wx' });
    await mkdir(join(trash, pass));
    for (const [index, entry] of entries.entries()) await rename(entry.from, join(trash, pass, String(index)));
    return { ok: true, value: undefined };
  } catch (error) {
    return { ok: false, error: ioError('move superseded generations to trash', trash, error) };
  }
}

interface Purged {
  readonly reclaimed: readonly Reclaimable[];
  readonly retained: readonly Retained[];
}

/**
 * Under the retention lock: delete every trash entry a manifest proves this selector moved
 * (a real directory with the manifest's device and inode), whichever pass moved it. Anything
 * else in the trash is retained and reported, never deleted.
 */
async function purgeTrash(profile: string, linked: Map<string, bigint>): Promise<Result<Purged, RetentionError>> {
  const trash = join(profile, TRASH);
  const reclaimed: Reclaimable[] = [];
  const retained: Retained[] = [];
  try {
    const trashStats = await lstatOrUndefined(trash);
    if (trashStats === undefined) return { ok: true, value: { reclaimed, retained } };
    if (!trashStats.isDirectory()) {
      return { ok: true, value: { reclaimed, retained: [{ path: trash, reason: 'unproven-trash-entry' }] } };
    }
    const names = await readdir(trash);
    const proven = new Set<string>();
    for (const name of names) {
      const id = MANIFEST_NAME.exec(name)?.[1];
      if (id === undefined) continue;
      const manifestPath = join(trash, name);
      const passDir = join(trash, id);
      if (!(await isFile(manifestPath)) || (await lstatOrUndefined(passDir))?.isDirectory() !== true) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(await readFile(manifestPath, 'utf8'));
      } catch {
        continue;
      }
      const manifest = validateManifest(parsed);
      if (!manifest.success) continue;
      proven.add(name).add(id);
      for (const [index, entry] of manifest.data.entries.entries()) {
        const moved = join(passDir, String(index));
        const stats = await lstatOrUndefined(moved);
        if (stats === undefined) continue;
        if (!stats.isDirectory() || stats.dev.toString() !== entry.dev || stats.ino.toString() !== entry.ino) {
          retained.push({
            path: moved,
            reason: 'unproven-trash-entry',
            detail: `not the generation moved from ${entry.from}`,
          });
          continue;
        }
        reclaimed.push({ path: entry.from, ...(await removeTree(moved, linked)), reason: 'superseded-generation' });
      }
      const left = await readdir(passDir);
      for (const name of left) {
        const path = join(passDir, name);
        if (!retained.some((entry) => entry.path === path)) {
          retained.push({ path, reason: 'unproven-trash-entry', detail: 'not in its manifest' });
        }
      }
      if (left.length === 0) {
        await rmdir(passDir);
        await unlink(manifestPath);
      }
    }
    for (const name of names) {
      if (!proven.has(name)) retained.push({ path: join(trash, name), reason: 'unproven-trash-entry' });
    }
    if ((await readdir(trash)).length === 0) await rmdir(trash);
    return { ok: true, value: { reclaimed, retained } };
  } catch (error) {
    return { ok: false, error: ioError('purge trash', trash, error) };
  }
}

// ---- The pass ----

/** Cargo's own answer for the workspace in `cwd`: its target directory and member packages. */
async function cargoWorkspace(
  cwd: string,
): Promise<Result<{ workspaceRoot: string; targetDir: string; members: ReadonlySet<string> }, RetentionError>> {
  const args = ['--frozen', 'metadata', '--no-deps', '--format-version', '1'];
  const child = spawn('cargo', args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
  const status = await new Promise<Result<number | null, string>>((resolve) => {
    child.once('error', (error) => resolve({ ok: false, error: error.message }));
    child.once('close', (code) => resolve({ ok: true, value: code }));
  });
  if (!status.ok || status.value !== 0) {
    const detail = status.ok
      ? `exited ${String(status.value)}: ${Buffer.concat(stderr).toString('utf8').trim()}`
      : status.error;
    return { ok: false, error: { kind: 'cargo-metadata', cwd, detail: `cargo ${args.join(' ')} ${detail}` } };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.concat(stdout).toString('utf8'));
  } catch (error) {
    return { ok: false, error: { kind: 'cargo-metadata', cwd, detail: describeCause(error) } };
  }
  const metadata = validateMetadata(parsed);
  if (!metadata.success) {
    const detail = metadata.errors.map((e) => `${e.path} expected ${e.expected}`).join('; ');
    return { ok: false, error: { kind: 'cargo-metadata', cwd, detail } };
  }
  try {
    return {
      ok: true,
      value: {
        workspaceRoot: metadata.data.workspace_root,
        targetDir: await realpath(metadata.data.target_directory),
        members: new Set(metadata.data.packages.map((pkg) => pkg.name)),
      },
    };
  } catch (error) {
    return { ok: false, error: ioError('resolve target directory', metadata.data.target_directory, error) };
  }
}

/** The lock files of every profile, in order, or the profile that lacks one Cargo always creates. */
async function profileLocks(
  profiles: readonly string[],
): Promise<Result<string[], { profile: string; reason: string }>> {
  const locks: string[] = [];
  for (const profile of profiles) {
    for (const name of REQUIRED_LOCKS) {
      if (!(await isFile(join(profile, name)))) return { ok: false, error: { profile, reason: `no ${name}` } };
      locks.push(join(profile, name));
    }
    for (const name of OPTIONAL_LOCKS) if (await isFile(join(profile, name))) locks.push(join(profile, name));
  }
  return { ok: true, value: locks };
}

/** Identity of the scheduling policy, Cargo configuration, workspace and actual target directory. */
function schedulingConfiguration(
  workspaceRoot: string,
  targetDir: string,
  members: ReadonlySet<string>,
): Result<string, RetentionError> {
  try {
    const target = lstatSync(targetDir, { bigint: true });
    const hash = createHash('sha256').update(
      JSON.stringify({
        version: SCHEDULE_VERSION,
        interval: AUTOMATIC_INTERVAL_MS,
        recentUse: RECENT_USE_MS,
        workspaceRoot,
        targetDir,
        dev: target.dev.toString(),
        ino: target.ino.toString(),
        members: [...members].sort(),
      }),
    );
    const files = new Set<string>([join(workspaceRoot, 'Cargo.lock'), join(targetDir, 'CACHEDIR.TAG')]);
    for (let directory = workspaceRoot; ; directory = dirname(directory)) {
      for (const input of [...CARGO_ANCESTOR_INPUTS, 'rust-toolchain', 'rust-toolchain.toml'])
        files.add(join(directory, input));
      if (dirname(directory) === directory) break;
    }
    const cargoHome = resolve(process.env.CARGO_HOME ?? join(homedir(), '.cargo'));
    files.add(join(cargoHome, 'config'));
    files.add(join(cargoHome, 'config.toml'));
    for (const file of [...files].sort()) {
      hash.update(file).update('\0');
      try {
        const bytes = readFileSync(file);
        hash.update(String(bytes.length)).update('\0').update(bytes);
      } catch (error) {
        if (errorCode(error) !== 'ENOENT') throw error;
        hash.update('missing');
      }
      hash.update('\0');
    }
    for (const key of [
      'CARGO_BUILD_TARGET',
      'CARGO_BUILD_DIR',
      'CARGO_TARGET_DIR',
      'CARGO_ENCODED_RUSTFLAGS',
      'RUSTFLAGS',
      'RUSTC',
      'RUSTC_WRAPPER',
      'RUSTC_WORKSPACE_WRAPPER',
      'RUSTUP_TOOLCHAIN',
    ]) {
      hash
        .update(key)
        .update('\0')
        .update(process.env[key] ?? '')
        .update('\0');
    }
    return { ok: true, value: hash.digest('hex') };
  } catch (error) {
    return { ok: false, error: ioError('read scheduling configuration', workspaceRoot, error) };
  }
}

/** An invalid, foreign, busy, refused or backward-clock receipt never postpones maintenance. */
function nextAutomaticPass(
  workspaceRoot: string,
  targetDir: string,
  configuration: string,
  now: number,
): number | undefined {
  try {
    const receipt = join(targetDir, RETENTION_RECEIPT);
    if (lstatSync(receipt, { throwIfNoEntry: false })?.isFile() !== true) return undefined;
    const parsed = validateRetentionReport(JSON.parse(readFileSync(receipt, 'utf8')));
    if (!parsed.success) return undefined;
    const previous = parsed.data;
    const completed = previous.completed;
    if (
      previous.workspaceRoot !== workspaceRoot ||
      previous.targetDir !== targetDir ||
      previous.outcome.kind !== 'reclaimed' ||
      completed?.configuration !== configuration
    )
      return undefined;
    if (!Number.isFinite(completed.at) || completed.at < 0 || now < completed.at) return undefined;
    const due = completed.at + AUTOMATIC_INTERVAL_MS;
    return now < due ? due : undefined;
  } catch {
    // A missing or unreadable receipt is not evidence of a completed pass; the due path reads current Cargo state.
    return undefined;
  }
}
export async function retainCargoWorkspace(
  cargoWorkspaceRoot: string,
  options: RetentionOptions,
): Promise<Result<RetentionReport, RetentionError>> {
  if (options.now !== undefined && !Number.isFinite(options.now.getTime())) {
    return {
      ok: false,
      error: {
        kind: 'io',
        operation: 'read retention clock',
        path: cargoWorkspaceRoot,
        detail: 'clock is not a finite instant',
      },
    };
  }
  if (!losslessJsonAvailable()) {
    return {
      ok: false,
      error: {
        kind: 'lossless-json-unavailable',
        detail: 'JSON.parse does not hand revivers the source text of numbers',
      },
    };
  }
  const metadataStarted = performance.now();
  const workspace = await cargoWorkspace(cargoWorkspaceRoot);
  performance.measure('cargoRetention:metadata', { start: metadataStarted });
  if (!workspace.ok) return workspace;
  const { workspaceRoot, targetDir, members } = workspace.value;
  const configuration = schedulingConfiguration(workspaceRoot, targetDir, members);
  if (!configuration.ok) return configuration;
  if (options.apply && options.automatic) {
    const due = nextAutomaticPass(workspaceRoot, targetDir, configuration.value, (options.now ?? new Date()).getTime());
    if (due !== undefined)
      return {
        ok: true,
        value: { workspaceRoot, targetDir, nestedRoots: [], outcome: { kind: 'not-due', nextDueAt: due } },
      };
  }
  const layoutStarted = performance.now();
  const layout = await discoverLayout(targetDir);
  performance.measure('cargoRetention:layout', { start: layoutStarted });
  if (!layout.ok) return layout;
  const report = (outcome: RetentionOutcome): RetentionReport => ({
    workspaceRoot,
    targetDir,
    nestedRoots: [],
    outcome,
  });
  let retention: Hold | undefined;
  if (options.apply) {
    const attempt = await tryHold([join(targetDir, RETENTION_LOCK)], 'create');
    if (!attempt.ok) return attempt;
    if (attempt.value.kind === 'busy') return { ok: true, value: report({ kind: 'busy', lock: attempt.value.lock }) };
    retention = attempt.value.hold;
  }
  try {
    // Another automatic caller may have completed between the first check and our lock acquisition.
    if (options.apply && options.automatic) {
      const due = nextAutomaticPass(
        workspaceRoot,
        targetDir,
        configuration.value,
        (options.now ?? new Date()).getTime(),
      );
      if (due !== undefined) return { ok: true, value: report({ kind: 'not-due', nextDueAt: due }) };
    }
    const result = await pass(targetDir, layout.value.profiles, members, options);
    if (!result.ok) return result;
    const done: RetentionReport = {
      workspaceRoot,
      targetDir,
      ...result.value,
      ...(options.apply && result.value.outcome.kind === 'reclaimed'
        ? { completed: { at: (options.now ?? new Date()).getTime(), configuration: configuration.value } }
        : {}),
    };
    if (options.apply) {
      const receipt = join(targetDir, RETENTION_RECEIPT);
      const staging = `${receipt}.${randomUUID()}.tmp`;
      try {
        await writeFile(staging, `${JSON.stringify(done, null, 2)}\n`, { flag: 'wx' });
        await rename(staging, receipt);
      } catch (error) {
        return { ok: false, error: ioError('write retention receipt', receipt, error) };
      }
    }
    return { ok: true, value: done };
  } finally {
    await retention?.release();
  }
}

interface PassReport {
  readonly outcome: RetentionOutcome;
  readonly nestedRoots: readonly string[];
}
async function pass(
  targetDir: string,
  profiles: readonly string[],
  members: ReadonlySet<string>,
  options: RetentionOptions,
): Promise<Result<PassReport, RetentionError>> {
  const nestedRoots = new Set<string>();
  const complete = (outcome: RetentionOutcome): Result<PassReport, RetentionError> => ({
    ok: true,
    value: { outcome, nestedRoots: [...nestedRoots].sort() },
  });
  const ownedProfiles: string[] = [];
  const containers = new Map<string, string>();
  const locks = await profileLocks(profiles);
  if (!locks.ok) return complete({ kind: 'refused', ...locks.error });
  const lockStarted = performance.now();
  const attempt = await tryHold(locks.value, 'existing');
  performance.measure('cargoRetention:locks', { start: lockStarted });
  if (!attempt.ok) return attempt;
  if (attempt.value.kind === 'busy') return complete({ kind: 'busy', lock: attempt.value.lock });
  const cargo = attempt.value.hold;
  const metas: Meta[] = [];
  const anomalies: Retained[] = [];
  let selection: Selection;
  try {
    const scanStarted = performance.now();
    for (const profile of profiles) {
      let scanned: Result<ProfileScan, string>;
      try {
        scanned = scanProfile(profile, targetDir);
      } catch (error) {
        scanned = { ok: false, error: `cannot read it: ${describeCause(error)}` };
      }
      if (!scanned.ok) return complete({ kind: 'refused', profile, reason: scanned.error });
      const root = scanned.value.root ?? targetDir;
      const container = dirname(profile);
      const previous = containers.get(container);
      if (previous !== undefined && previous !== root) {
        return complete({
          kind: 'refused',
          profile,
          reason: 'profiles in this directory disagree about their build root',
        });
      }
      containers.set(container, root);
      if (root !== targetDir) {
        nestedRoots.add(root);
        continue;
      }
      ownedProfiles.push(profile);
      metas.push(...scanned.value.metas);
      anomalies.push(...scanned.value.anomalies);
    }
    performance.measure('cargoRetention:scan', { start: scanStarted });
    const selectionStarted = performance.now();
    selection = select(metas, members, (options.now ?? new Date()).getTime());
    performance.measure('cargoRetention:select', { start: selectionStarted });
    if (options.apply) {
      for (const profile of ownedProfiles) {
        const victims = selection.reclaim.filter((meta) => meta.profile === profile);
        if (victims.length === 0) continue;
        const moved = await moveToTrash(profile, victims);
        if (!moved.ok) return moved;
      }
    }
  } finally {
    await cargo.release();
  }
  if (!options.apply) {
    const reclaim: Reclaimable[] = [];
    for (const meta of selection.reclaim) {
      try {
        reclaim.push({ path: meta.path, ...(await measureTree(meta.path)), reason: 'superseded-generation' });
      } catch (error) {
        return { ok: false, error: ioError('measure', meta.path, error) };
      }
    }
    const linkedBytes = reclaim.reduce((sum, entry) => sum + entry.linkedBytes, 0);
    return complete(outcome('planned', reclaim, linkedBytes, [...selection.kept, ...anomalies]));
  }
  const linked = new Map<string, bigint>();
  const reclaim: Reclaimable[] = [];
  const retained: Retained[] = [...selection.kept, ...anomalies];
  for (const profile of ownedProfiles) {
    const purged = await purgeTrash(profile, linked);
    if (!purged.ok) return purged;
    reclaim.push(...purged.value.reclaimed);
    retained.push(...purged.value.retained);
  }
  let linkedBytes = 0n;
  for (const blocks of linked.values()) linkedBytes += blocks;
  return complete(outcome('reclaimed', reclaim, Number(linkedBytes), retained));
}

/** `linkedBytes`: planned, the candidates' bytes linked outside them; reclaimed, the inodes still linked after the pass. */
function outcome(
  kind: 'planned' | 'reclaimed',
  reclaim: readonly Reclaimable[],
  linkedBytes: number,
  retained: readonly Retained[],
): RetentionOutcome {
  return { kind, reclaim, reclaimedBytes: reclaim.reduce((sum, entry) => sum + entry.bytes, 0), linkedBytes, retained };
}

// ---- Reporting and the Nx hook ----

function gib(bytes: number): string {
  return `${(bytes / 2 ** 30).toFixed(2)} GiB`;
}

export function describeRetention(report: RetentionReport): string {
  const subject = `Cargo retention ${report.targetDir}`;
  const outcome = report.outcome;
  switch (outcome.kind) {
    case 'planned':
    case 'reclaimed': {
      const verb = outcome.kind === 'planned' ? 'would reclaim' : 'reclaimed';
      return (
        `${subject}: ${verb} ${gib(outcome.reclaimedBytes)} in ${outcome.reclaim.length} superseded unit generations ` +
        `(${gib(outcome.linkedBytes)} more stays linked elsewhere); ${outcome.retained.length} retained`
      );
    }
    case 'not-due':
      return `${subject}: automatic maintenance not due until ${new Date(outcome.nextDueAt).toISOString()}; no scan or cleanup ran`;
    case 'busy':
      return `${subject}: skipped, ${outcome.lock} is held`;
    case 'refused':
      return `${subject}: refused, ${outcome.profile}: ${outcome.reason}; nothing was changed`;
  }
}

export function describeRetentionError(error: RetentionError): string {
  switch (error.kind) {
    case 'cargo-metadata':
      return `cargo metadata in ${error.cwd} failed: ${error.detail}`;
    case 'target-not-cargo':
      return `${error.targetDir} is not a Cargo target directory: ${error.detail}`;
    case 'lossless-json-unavailable':
      return `cannot read Cargo fingerprints exactly: ${error.detail}`;
    case 'lock-helper':
      return `cannot take Cargo's locks: ${error.detail}`;
    case 'io':
      return `cannot ${error.operation} ${error.path}: ${error.detail}`;
  }
}

type TaskStatus = PostTasksExecutionContext['taskResults'][string]['status'];

/** Whether a task with this status ran its command, and so may have built into a Cargo target directory. */
function ran(status: TaskStatus): boolean {
  switch (status) {
    case 'success':
    case 'failure':
    case 'stopped':
      return true;
    case 'skipped':
    case 'local-cache-kept-existing':
    case 'local-cache':
    case 'remote-cache':
      return false;
  }
}

/**
 * The metadata the plugin puts on every target it infers that builds, checks or tests into a Cargo
 * target directory. Only an executed task carrying it starts a retention pass: a fetch, a lint of
 * another language or a cache-restored replay wrote nothing there.
 */
export const CARGO_PRODUCER_METADATA = { cargoProducer: true } as const;

/**
 * After a run that executed a Cargo producer task ({@link CARGO_PRODUCER_METADATA}), check whether
 * each Cargo workspace is due for its daily maintenance. A successful receipt schedules only;
 * every due pass reads the current graph under Cargo's locks and is awaited by this Nx process.
 */
export async function postTasksExecution(_options: unknown, context: PostTasksExecutionContext): Promise<void> {
  const executed = Object.values(context.taskResults).filter(({ status }) => ran(status));
  if (executed.length === 0) return;
  let graph: ProjectGraph;
  try {
    graph = readCachedProjectGraph();
  } catch (error) {
    logger.warn(
      `Cargo retention: cannot read the project graph that names Cargo producer tasks: ${describeCause(error)}`,
    );
    return;
  }
  const roots = new Set<string>();
  for (const { task } of executed) {
    const project = graph.nodes[task.target.project];
    if (project?.data.targets?.[task.target.target]?.metadata?.cargoProducer !== true) continue;
    roots.add(join(context.workspaceRoot, task.projectRoot ?? project.data.root));
  }
  const done: string[] = [];
  for (const root of [...roots].sort()) {
    if (done.some((workspaceRoot) => !relative(workspaceRoot, root).startsWith('..'))) continue;
    const started = performance.now();
    const cpu = process.cpuUsage();
    const result = await retainCargoWorkspace(root, { apply: true, automatic: true });
    performance.measure('cargoRetention:total', { start: started });
    const used = process.cpuUsage(cpu);
    performance.measure('cargoRetention:cpu', { start: 0, duration: (used.user + used.system) / 1000 });
    if (!result.ok) {
      logger.warn(`Cargo retention for ${root}: ${describeRetentionError(result.error)}`);
      continue;
    }
    done.push(result.value.workspaceRoot);
    const line = describeRetention(result.value);
    if (result.value.outcome.kind === 'refused') logger.warn(line);
    else logger.info(line);
  }
}
