/**
 * Retention of unused Cargo build outputs: the superseded unit generations under a Cargo
 * target directory's new build-dir layout, `<profile>/build/<package>/<META>/`.
 *
 * Cargo never deletes a generation a later build superseded (rust-lang/cargo#13062: "we
 * aren't garbage collecting build-dirs yet"), so a busy checkout's target directory grows
 * without bound. Age alone cannot tell a superseded generation from a current one, and this
 * volume is mounted `noatime`. The selector therefore reads Cargo's own records only:
 *
 * - Every `fingerprint/<kind>.json` names its unit's identity (every field but `deps`, `local` and the
 *   toolchain, which is `rustc` and the `--sysroot` flag; unknown fields included) and its dependency edges
 *   (`deps[i][3]`, the dependency's fingerprint hash). Integers are kept exact: `JSON.parse` hands each number's
 *   source text to the reviver, so a u64 is never rounded through a double.
 * - The hash file beside it (`fingerprint/<kind>`) holds that unit's own fingerprint hash as
 *   the 16 hex digits of the u64's little-endian bytes; an edge resolves to every unit whose
 *   hash file holds it. Cargo stamps the hash file's mtime on every use when `-Zmtime-on-use` is on (cowshed
 *   exports it), and `invoked.timestamp` marks the unit's last compile. A hash file newer than its compile by
 *   more than {@link RECENT_USE_MS} proves the stamping; a root whose units show none reports `recency-unknown`,
 *   because a unit's last use is then only its last compile.
 * - Roots are the newest generation of each identity of a workspace member package, whichever toolchain built
 *   it (a newer toolchain supersedes an older one, ordered by last use), and every unit used within
 *   {@link RECENT_USE_MS} of now. Kept is the transitive dependency
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
 * selector cannot read (an unreadable fingerprint, a profile whose only unit records are a legacy layout's,
 * a profile without `build/` or without Cargo's locks) refuses the whole pass: its edges are unknowable. So does
 * a lock that cannot be attempted at all (nothing that could hold it starts here, its file will
 * not open): nothing was read or changed, and the reason is a value, because a maintenance pass
 * never fails the run it follows. A profile holding a legacy layout beside the current one is read for the
 * current one alone: the legacy entries are reported retained as `legacy-layout`, and nothing is reclaimed from them.
 *
 * A Cargo target root nested inside the target directory (`target/cargo-lint`, `target/cargo-lint-cross`, each
 * built with its own `--target-dir`) is a target directory of its own; units never depend across roots. The
 * target directory's scan finds it: it locks and reads the nested root's depth-two profile like
 * its own, and that profile's root-relative fingerprint paths prove the root. A pass busy or
 * refused there reaches no nested root. Each nested root then gets this same pass over its own
 * profiles, host and cross, under only their Cargo locks and the target's one retention lock, so
 * one that is busy or refused is left as it was and holds up no other. Only generations are
 * reclaimed, never a root or a profile, and a nested root holding no unit of a workspace member
 * has nothing proving it ours: it is kept whole, as `foreign-root`.
 *
 * Not reclaimed here: `incremental/` and the split-debuginfo objects rustc keeps inside current
 * units (rust-lang/rust#161824).
 */
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, constants, lstatSync, openSync, readdirSync, readFileSync, writeSync } from 'node:fs';
import {
  type FileHandle,
  lstat,
  mkdir,
  open,
  readdir,
  readFile,
  realpath,
  rename,
  rmdir,
  unlink,
} from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, normalize, relative, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline';
import typia, { type tags } from 'typia';
import { CARGO_ANCESTOR_INPUTS } from './cargo-ancestor-inputs.js';
import { O_EXLOCK } from './open-flags.js';

export type Result<T, E> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: E };

export type RetentionError =
  | { readonly kind: 'cargo-metadata'; readonly cwd: string; readonly detail: string }
  | { readonly kind: 'target-not-cargo'; readonly targetDir: string; readonly detail: string }
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
  | 'unproven-trash-entry'
  | 'foreign-root'
  | 'legacy-layout';

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
/**
 * Whether Cargo recorded the use of a root's units. `-Zmtime-on-use` stamps a unit's hash file on every use, so a
 * hash file newer than its compile by more than {@link RECENT_USE_MS} proves it; without any, a unit's last use is
 * its last compile, and "recently used" only means "recently compiled".
 */
export type Recency =
  | { readonly kind: 'use-stamped' }
  | { readonly kind: 'recency-unknown'; readonly reason: 'mtime-on-use off' }
  | { readonly kind: 'no-units' };

/** What one root's pass did: planned or reclaimed generations, a skip behind a held lock, or a refusal. */
export type PassOutcome =
  | {
      readonly kind: 'planned' | 'reclaimed';
      readonly reclaim: readonly Reclaimable[];
      readonly reclaimedBytes: number;
      readonly linkedBytes: number;
      readonly retained: readonly Retained[];
      readonly recency: Recency;
    }
  | { readonly kind: 'busy'; readonly lock: string }
  | { readonly kind: 'refused'; readonly profile: string; readonly reason: string };
export type RetentionOutcome = PassOutcome | { readonly kind: 'not-due'; readonly nextDueAt: number };
/** A Cargo target root nested inside the target directory, passed under its own profiles' Cargo locks. */
export interface NestedRoot {
  readonly root: string;
  readonly outcome: PassOutcome;
}
export interface RetentionReport {
  readonly workspaceRoot: string;
  readonly targetDir: string;
  readonly nestedRoots: readonly NestedRoot[];
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
/** The old layout's build script directory: `build/<crate>-<hash>`. */
const LEGACY_SCRIPT_NAME = /-[0-9a-f]{16}$/;
const HASH_TEXT = /^[0-9a-f]{16}$/;
const INTEGER = /^-?(?:0|[1-9]\d*)$/;
const MANIFEST_NAME = /^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.json$/;
const SECTOR = 512;

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

// ---- Exact JSON ----
//
// Cargo writes fingerprint hashes as bare u64 integers, which a double cannot hold, and the native build of this
// selector has no `JSON.parse` reviver source text to recover their digits from. So fingerprints are read by this
// parser: every integer is a bigint from its digits, and any other number is refused rather than rounded. What it
// returns is checked afterwards by the generated validator, never by hand.

interface JsonReader {
  readonly text: string;
  at: number;
}

const MAX_JSON_DEPTH = 64;

function jsonError(reader: JsonReader, what: string): Error {
  return new Error(`${what} at offset ${reader.at}`);
}

function skipJsonSpace(reader: JsonReader): void {
  while (reader.at < reader.text.length) {
    const code = reader.text.charCodeAt(reader.at);
    if (code !== 0x20 && code !== 0x09 && code !== 0x0a && code !== 0x0d) return;
    reader.at += 1;
  }
}

/** The string at the cursor: found by its closing quote, decoded by `JSON.parse`, which also refuses bad escapes. */
function readJsonString(reader: JsonReader): string {
  const start = reader.at;
  reader.at += 1;
  while (reader.at < reader.text.length) {
    const code = reader.text.charCodeAt(reader.at);
    if (code === 0x22) {
      reader.at += 1;
      const decoded: unknown = JSON.parse(reader.text.slice(start, reader.at));
      if (typeof decoded !== 'string') throw jsonError(reader, 'a string was expected');
      return decoded;
    }
    reader.at += code === 0x5c ? 2 : 1;
  }
  throw jsonError(reader, 'a string is not closed');
}

function readJsonInteger(reader: JsonReader): bigint {
  const start = reader.at;
  if (reader.text.charCodeAt(reader.at) === 0x2d) reader.at += 1;
  while (reader.at < reader.text.length) {
    const code = reader.text.charCodeAt(reader.at);
    if (code < 0x30 || code > 0x39) break;
    reader.at += 1;
  }
  const next = reader.text.charCodeAt(reader.at);
  if (next === 0x2e || next === 0x65 || next === 0x45) throw jsonError(reader, 'a non-integer number');
  const digits = reader.text.slice(start, reader.at);
  if (!INTEGER.test(digits)) throw jsonError(reader, `a malformed number ${digits}`);
  return BigInt(digits);
}

function readJsonLiteral(reader: JsonReader, word: string, value: boolean | null): boolean | null {
  if (!reader.text.startsWith(word, reader.at)) throw jsonError(reader, 'an unexpected character');
  reader.at += word.length;
  return value;
}

function readJsonArray(reader: JsonReader, depth: number): unknown[] {
  reader.at += 1;
  const items: unknown[] = [];
  skipJsonSpace(reader);
  if (reader.text.charCodeAt(reader.at) === 0x5d) {
    reader.at += 1;
    return items;
  }
  for (;;) {
    items.push(readJsonValue(reader, depth + 1));
    skipJsonSpace(reader);
    const next = reader.text.charCodeAt(reader.at);
    reader.at += 1;
    if (next === 0x5d) return items;
    if (next !== 0x2c) throw jsonError(reader, 'a comma or a closing bracket was expected');
  }
}

/** A later duplicate of a name wins, as in `JSON.parse`; `Object.fromEntries` also keeps `__proto__` an own property. */
function readJsonObject(reader: JsonReader, depth: number): unknown {
  reader.at += 1;
  const entries: [string, unknown][] = [];
  skipJsonSpace(reader);
  if (reader.text.charCodeAt(reader.at) === 0x7d) {
    reader.at += 1;
    return Object.fromEntries(entries);
  }
  for (;;) {
    skipJsonSpace(reader);
    if (reader.text.charCodeAt(reader.at) !== 0x22) throw jsonError(reader, 'a property name was expected');
    const name = readJsonString(reader);
    skipJsonSpace(reader);
    if (reader.text.charCodeAt(reader.at) !== 0x3a) throw jsonError(reader, 'a colon was expected');
    reader.at += 1;
    entries.push([name, readJsonValue(reader, depth + 1)]);
    skipJsonSpace(reader);
    const next = reader.text.charCodeAt(reader.at);
    reader.at += 1;
    if (next === 0x7d) return Object.fromEntries(entries);
    if (next !== 0x2c) throw jsonError(reader, 'a comma or a closing brace was expected');
  }
}

function readJsonValue(reader: JsonReader, depth: number): unknown {
  if (depth >= MAX_JSON_DEPTH) throw jsonError(reader, 'nesting is too deep');
  skipJsonSpace(reader);
  const code = reader.text.charCodeAt(reader.at);
  switch (code) {
    case 0x7b:
      return readJsonObject(reader, depth);
    case 0x5b:
      return readJsonArray(reader, depth);
    case 0x22:
      return readJsonString(reader);
    case 0x74:
      return readJsonLiteral(reader, 'true', true);
    case 0x66:
      return readJsonLiteral(reader, 'false', false);
    case 0x6e:
      return readJsonLiteral(reader, 'null', null);
    default:
      if (code === 0x2d || (code >= 0x30 && code <= 0x39)) return readJsonInteger(reader);
      throw jsonError(reader, 'an unexpected character');
  }
}

/** The JSON document in `text` with every number a bigint; a non-integer number or any malformed text is an error. */
export function parseExactJson(text: string): Result<unknown, string> {
  const reader: JsonReader = { text, at: 0 };
  try {
    const value = readJsonValue(reader, 0);
    skipJsonSpace(reader);
    if (reader.at !== text.length) throw jsonError(reader, 'unexpected text after the document');
    return { ok: true, value };
  } catch (error) {
    return { ok: false, error: describeCause(error) };
  }
}

export function readCargoFingerprint(text: string): Result<CargoFingerprint, string> {
  const parsed = parseExactJson(text);
  if (!parsed.ok) return { ok: false, error: `unreadable fingerprint JSON: ${parsed.error}` };
  const checked = validateFingerprint(parsed.value);
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

/** Fingerprint fields that are not a unit's identity: its edges, its source state and the compiler that built it. */
const NOT_IDENTITY: ReadonlySet<string> = new Set(['deps', 'local', 'rustc']);

/** `rustflags` without the `--sysroot <path>` (or `--sysroot=<path>`) that names the toolchain it ran against. */
function withoutSysroot(flags: readonly unknown[]): unknown[] {
  const kept: unknown[] = [];
  for (let index = 0; index < flags.length; index += 1) {
    const flag = flags[index];
    if (flag === '--sysroot') index += 1;
    else if (typeof flag !== 'string' || !flag.startsWith('--sysroot=')) kept.push(flag);
  }
  return kept;
}

/**
 * Package, kind and every fingerprint field but its edges (`deps`), its source state (`local`) and its toolchain
 * (`rustc`, and the sysroot in `rustflags`): the same unit built by another toolchain is another generation of it,
 * and the newest generation, by last use, is the one kept.
 */
export function fingerprintIdentity(pkg: string, kind: string, fingerprint: CargoFingerprint): string {
  const fields = Object.keys(fingerprint)
    .filter((field) => !NOT_IDENTITY.has(field))
    .sort()
    .map((field) => {
      const value = fingerprint[field];
      const flags = field === 'rustflags' && Array.isArray(value) ? withoutSysroot(value) : value;
      return `${JSON.stringify(field)}=${canonical(flags)}`;
    });
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

/** Create `path`, failing if it exists, and write `text` into it. */
function writeNew(path: string, text: string): void {
  const fd = openSync(path, 'wx');
  try {
    writeSync(fd, text);
  } finally {
    closeSync(fd);
  }
}
async function lstatOrUndefined(path: string) {
  try {
    return await lstat(path);
  } catch (error) {
    if (errorCode(error) === 'ENOENT') return undefined;
    throw error;
  }
}

function ioError(operation: string, path: string, error: unknown): RetentionError {
  return { kind: 'io', operation, path, detail: describeCause(error) };
}

// ---- Cargo's locks ----
//
// Cargo takes flock(2) on its lock files, so a pass takes the same lock: exclusive, never waiting. The kernel lets go
// of it when the descriptor holding it closes, however its process ends. macOS holds it in-process: `open(2)` with
// O_EXLOCK | O_NONBLOCK is `flock(LOCK_EX | LOCK_NB)` made with the open, so a sandbox that refuses to exec an
// interpreter costs the pass nothing. Node has no flock(2) anywhere else, so a perl child holds it there, for as long
// as its stdin pipe lives. Whichever holds it, an attempt that cannot be made is a value, never an exception.

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

type LockMode = 'existing' | 'create';

interface Hold {
  release(): Promise<void>;
}
type HoldAttempt =
  | { readonly kind: 'held'; readonly hold: Hold }
  | { readonly kind: 'busy'; readonly lock: string }
  /** The attempt could not be made at all: nothing is held, and nobody is known to hold the lock. */
  | { readonly kind: 'unavailable'; readonly reason: string };

/** Every lock in `paths` exclusively and without waiting, in order; the first held one makes it `busy`. */
function tryHold(paths: readonly string[], mode: LockMode): Promise<HoldAttempt> {
  return process.platform === 'darwin' ? holdInProcess(paths, mode) : holdThroughPerl(paths, mode);
}

/** `open(2)` takes each flock(2) itself: contention fails the open with EAGAIN, and closing the descriptor lets go. */
async function holdInProcess(paths: readonly string[], mode: LockMode): Promise<HoldAttempt> {
  const flags =
    (mode === 'create' ? constants.O_RDONLY | constants.O_CREAT : constants.O_RDONLY) | constants.O_NONBLOCK | O_EXLOCK;
  const held: FileHandle[] = [];
  const letGo = async (): Promise<void> => {
    await Promise.all(held.map((handle) => handle.close()));
  };
  for (const path of paths) {
    try {
      held.push(await open(path, flags, 0o644));
    } catch (error) {
      await letGo();
      return errorCode(error) === 'EAGAIN'
        ? { kind: 'busy', lock: path }
        : { kind: 'unavailable', reason: describeCause(error) };
    }
  }
  return { kind: 'held', hold: { release: letGo } };
}

function startHelper(mode: LockMode, paths: readonly string[]) {
  return spawn('perl', ['-e', LOCK_HELPER, mode, ...paths], { stdio: ['pipe', 'pipe', 'pipe'] });
}

/** A perl child takes each flock(2) on its own descriptor and holds it for as long as its stdin pipe lives. */
async function holdThroughPerl(paths: readonly string[], mode: LockMode): Promise<HoldAttempt> {
  let child: ReturnType<typeof startHelper>;
  try {
    child = startHelper(mode, paths);
  } catch (error) {
    // Node throws, not emits 'error', for exec failures it does not class as run-time, such as EPERM from a sandbox.
    return { kind: 'unavailable', reason: `cannot run perl: ${describeCause(error)}` };
  }
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
    return { kind: 'unavailable', reason: first.error };
  }
  const [verdict, path, detail] = first.value.split('\t');
  if (verdict === 'held') {
    return {
      kind: 'held',
      hold: {
        async release() {
          child.stdin.end();
          await exited;
        },
      },
    };
  }
  child.stdin.end();
  await exited;
  if (verdict === 'busy' && path !== undefined) return { kind: 'busy', lock: path };
  return {
    kind: 'unavailable',
    reason: verdict === 'error' ? `${path}: ${detail}` : `unexpected answer ${first.value}`,
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
  /** Units whose hash file is newer than their compile by more than {@link RECENT_USE_MS}: Cargo stamped a use. */
  readonly stamped: number;
}

function mtimeMs(path: string): number | undefined {
  const stats = lstatSync(path, { throwIfNoEntry: false });
  return stats?.isFile() === true ? Math.floor(stats.mtimeMs) : undefined;
}

/** Directory types already returned by the kernel; never follow a symbolic link to classify an entry. */
function directoryEntries(path: string) {
  return readdirSync(path, { withFileTypes: true }).sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

/** A profile whose only unit records are a legacy layout's: this selector cannot read their edges. */
function legacyOnly(markers: readonly string[]): string {
  return `legacy target layout: ${markers.join(', ')} hold the only unit records`;
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
  const legacy = LEGACY_MARKERS.filter((marker) => profileEntries.some((entry) => entry.name === marker));
  const build = join(profile, 'build');
  if (!profileEntries.some((entry) => entry.name === 'build' && entry.isDirectory())) {
    return { ok: false, error: legacy.length > 0 ? legacyOnly(legacy) : 'no build directory' };
  }
  const metas: Meta[] = [];
  let stamped = 0;
  const anomalies: Retained[] = [];
  for (const pkgEntry of directoryEntries(build)) {
    const pkg = pkgEntry.name;
    const pkgPath = join(build, pkg);
    if (!pkgEntry.isDirectory()) {
      anomalies.push({ path: pkgPath, reason: 'not-a-unit', detail: 'not a directory' });
      continue;
    }
    const children = directoryEntries(pkgPath);
    if (LEGACY_SCRIPT_NAME.test(pkg) && !children.some((child) => META_NAME.test(child.name))) {
      anomalies.push({ path: pkgPath, reason: 'legacy-layout', detail: 'an old-layout build script directory' });
      continue;
    }
    for (const entry of children) {
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
        const hashTime = mtimeMs(hashPath) ?? 0;
        if (invoked > 0 && hashTime - invoked > RECENT_USE_MS) stamped += 1;
        meta.units.push({
          meta,
          kind,
          hash: hash === '' ? undefined : hash,
          identity: fingerprintIdentity(pkg, kind, fingerprint.value),
          deps: fingerprint.value.deps.map(([, depName, , depHash]) => ({ hash: hashText(depHash), name: depName })),
          lastUse: Math.max(hashTime, invoked),
        });
      }
      metas.push(meta);
    }
  }
  if (root === undefined && nestedRoot !== targetDir) {
    return { ok: false, error: 'no fingerprint local path proves this depth-two profile belongs to the target' };
  }
  if (legacy.length > 0 && metas.length === 0) return { ok: false, error: legacyOnly(legacy) };
  for (const marker of legacy) {
    anomalies.push({
      path: join(profile, marker),
      reason: 'legacy-layout',
      detail: 'old-layout unit records; this selector reads the current layout only',
    });
  }
  return { ok: true, value: { root, metas, anomalies, stamped } };
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
  const inodes = new Map<string, { seen: number; nlink: number; blocks: number }>();
  const walk = async (entry: string): Promise<void> => {
    const stats = await lstat(entry);
    if (stats.isDirectory()) {
      for (const name of await readdir(entry)) await walk(join(entry, name));
      return;
    }
    const key = `${stats.dev}:${stats.ino}`;
    const known = inodes.get(key);
    if (known === undefined) inodes.set(key, { seen: 1, nlink: stats.nlink, blocks: stats.blocks });
    else known.seen += 1;
  };
  await walk(path);
  let bytes = 0;
  let linkedBytes = 0;
  for (const inode of inodes.values()) {
    if (inode.seen >= inode.nlink) bytes += inode.blocks * SECTOR;
    else linkedBytes += inode.blocks * SECTOR;
  }
  return { bytes, linkedBytes };
}

/**
 * Unlink everything under `path`, never following a link. `bytes` are freed by these unlinks;
 * `linkedBytes` were still linked elsewhere when unlinked. `linked` carries the inodes still
 * linked across the pass, so a later unlink that frees one takes it out again.
 */
async function removeTree(path: string, linked: Map<string, number>): Promise<Bytes> {
  let freed = 0;
  let stillLinked = 0;
  const remove = async (entry: string): Promise<void> => {
    const stats = await lstat(entry);
    if (stats.isDirectory()) {
      for (const name of await readdir(entry)) await remove(join(entry, name));
      await rmdir(entry);
      return;
    }
    const key = `${stats.dev}:${stats.ino}`;
    await unlink(entry);
    if (stats.nlink === 1) {
      freed += stats.blocks * SECTOR;
      linked.delete(key);
    } else {
      linked.set(key, stats.blocks * SECTOR);
      stillLinked += stats.blocks * SECTOR;
    }
  };
  await remove(path);
  return { bytes: freed, linkedBytes: stillLinked };
}

// ---- Trash ----

/**
 * A generation's device and inode as the trash manifest spells them, or `undefined` when a double cannot hold either
 * exactly: the manifest proves a generation by those digits, so a rounded number proves nothing.
 */
export function exactIdentity(stats: {
  readonly dev: number;
  readonly ino: number;
}): { readonly dev: string; readonly ino: string } | undefined {
  return Number.isSafeInteger(stats.dev) && Number.isSafeInteger(stats.ino)
    ? { dev: String(stats.dev), ino: String(stats.ino) }
    : undefined;
}

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
      const identity = exactIdentity(await lstat(victim.path));
      if (identity === undefined) {
        const detail = 'its device or inode number is not an exact integer';
        return {
          ok: false,
          error: { kind: 'io', operation: 'identify superseded generation', path: victim.path, detail },
        };
      }
      entries.push({ from: victim.path, ...identity });
    }
    const manifest: TrashManifest = { version: 1, entries };
    writeNew(join(trash, `${pass}.json`), JSON.stringify(manifest));
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
async function purgeTrash(profile: string, linked: Map<string, number>): Promise<Result<Purged, RetentionError>> {
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
        const identity = exactIdentity(stats);
        if (!stats.isDirectory() || identity?.dev !== entry.dev || identity?.ino !== entry.ino) {
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
    const target = lstatSync(targetDir);
    const hash = createHash('sha256').update(
      JSON.stringify({
        version: SCHEDULE_VERSION,
        interval: AUTOMATIC_INTERVAL_MS,
        recentUse: RECENT_USE_MS,
        workspaceRoot,
        targetDir,
        dev: String(target.dev),
        ino: String(target.ino),
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

/** Whether any root's pass refused: nothing was changed there, and the report names its profile and reason. */
export function refusedAnywhere(report: RetentionReport): boolean {
  return report.outcome.kind === 'refused' || report.nestedRoots.some((nested) => nested.outcome.kind === 'refused');
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
    if (attempt.kind === 'busy') return { ok: true, value: report({ kind: 'busy', lock: attempt.lock }) };
    if (attempt.kind === 'unavailable') {
      return { ok: true, value: report(lockRefusal(targetDir, 'the retention lock', attempt.reason)) };
    }
    retention = attempt.hold;
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
    const result = await passRoots(targetDir, layout.value.profiles, members, options);
    if (!result.ok) return result;
    const done: RetentionReport = {
      workspaceRoot,
      targetDir,
      ...result.value,
      // A receipt schedules only a pass that reclaimed in every root: a busy or refused one is retried next run.
      ...(options.apply &&
      result.value.outcome.kind === 'reclaimed' &&
      result.value.nestedRoots.every((nested) => nested.outcome.kind === 'reclaimed')
        ? { completed: { at: (options.now ?? new Date()).getTime(), configuration: configuration.value } }
        : {}),
    };
    if (options.apply) {
      const receipt = join(targetDir, RETENTION_RECEIPT);
      const staging = `${receipt}.${randomUUID()}.tmp`;
      try {
        writeNew(staging, `${JSON.stringify(done, null, 2)}\n`);
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

/** A nested root's pass; a root whose tag or profile locks cannot be read refuses, as one of its profiles would. */
async function passNested(
  root: string,
  members: ReadonlySet<string>,
  options: RetentionOptions,
): Promise<Result<PassReport, RetentionError>> {
  const layout = await discoverLayout(root);
  if (layout.ok) return pass(root, layout.value.profiles, members, options, true);
  if (layout.error.kind !== 'target-not-cargo') return layout;
  return {
    ok: true,
    value: { outcome: { kind: 'refused', profile: root, reason: layout.error.detail }, nestedRoots: [] },
  };
}

/**
 * The target directory's pass, then the pass of each Cargo root it found nested, and of theirs, one
 * after another: each root holds only its own profiles' Cargo locks, so a busy nested root waits on
 * no other and holds none up. A target directory pass that is busy or refused found no nested roots.
 */
async function passRoots(
  targetDir: string,
  profiles: readonly string[],
  members: ReadonlySet<string>,
  options: RetentionOptions,
): Promise<Result<{ readonly outcome: PassOutcome; readonly nestedRoots: readonly NestedRoot[] }, RetentionError>> {
  const own = await pass(targetDir, profiles, members, options, false);
  if (!own.ok) return own;
  const { outcome } = own.value;
  const nestedRoots: NestedRoot[] = [];
  if (outcome.kind === 'planned' || outcome.kind === 'reclaimed') {
    const pending = [...own.value.nestedRoots];
    for (let root = pending.pop(); root !== undefined; root = pending.pop()) {
      const passed = await passNested(root, members, options);
      if (!passed.ok) return passed;
      nestedRoots.push({ root, outcome: passed.value.outcome });
      pending.push(...passed.value.nestedRoots);
    }
  }
  nestedRoots.sort((a, b) => (a.root < b.root ? -1 : a.root > b.root ? 1 : 0));
  return { ok: true, value: { outcome, nestedRoots } };
}

interface PassReport {
  readonly outcome: PassOutcome;
  /** The roots nested in this one, proven by its depth-two profiles' fingerprints and passed after it. */
  readonly nestedRoots: readonly string[];
}
/** One Cargo target root's pass: the target directory's own, or (`nested`) a root found nested inside it. */
async function pass(
  targetDir: string,
  profiles: readonly string[],
  members: ReadonlySet<string>,
  options: RetentionOptions,
  nested: boolean,
): Promise<Result<PassReport, RetentionError>> {
  const nestedRoots = new Set<string>();
  const complete = (outcome: PassOutcome): Result<PassReport, RetentionError> => ({
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
  if (attempt.kind === 'busy') return complete({ kind: 'busy', lock: attempt.lock });
  if (attempt.kind === 'unavailable') return complete(lockRefusal(targetDir, "Cargo's locks", attempt.reason));
  const cargo = attempt.hold;
  const metas: Meta[] = [];
  let stamped = 0;
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
      stamped += scanned.value.stamped;
    }
    performance.measure('cargoRetention:scan', { start: scanStarted });
    const selectionStarted = performance.now();
    // Cargo's own metadata makes the target directory the workspace's; a nested root is ours only by
    // holding a member's unit. Without one the selection has no roots, and would reclaim everything.
    selection =
      nested && metas.length > 0 && !metas.some((meta) => members.has(meta.pkg))
        ? { reclaim: [], kept: [{ path: targetDir, reason: 'foreign-root', detail: 'no unit of a workspace member' }] }
        : select(metas, members, (options.now ?? new Date()).getTime());
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
    return complete(
      outcome('planned', reclaim, linkedBytes, [...selection.kept, ...anomalies], recencyOf(metas, stamped)),
    );
  }
  const linked = new Map<string, number>();
  const reclaim: Reclaimable[] = [];
  const retained: Retained[] = [...selection.kept, ...anomalies];
  for (const profile of ownedProfiles) {
    const purged = await purgeTrash(profile, linked);
    if (!purged.ok) return purged;
    reclaim.push(...purged.value.reclaimed);
    retained.push(...purged.value.retained);
  }
  let linkedBytes = 0;
  for (const blocks of linked.values()) linkedBytes += blocks;
  return complete(outcome('reclaimed', reclaim, linkedBytes, retained, recencyOf(metas, stamped)));
}

/** A pass that could not even attempt a lock changed nothing: it is refused, naming why on one line. */
function lockRefusal(targetDir: string, lock: string, reason: string): PassOutcome {
  return {
    kind: 'refused',
    profile: targetDir,
    reason: `cannot attempt ${lock}: ${reason.replace(/\s+/g, ' ').trim()}`,
  };
}

/** What a root's units say of Cargo's recording of use. */
function recencyOf(metas: readonly Meta[], stamped: number): Recency {
  if (metas.length === 0) return { kind: 'no-units' };
  return stamped > 0 ? { kind: 'use-stamped' } : { kind: 'recency-unknown', reason: 'mtime-on-use off' };
}

/** `linkedBytes`: planned, the candidates' bytes linked outside them; reclaimed, the inodes still linked after the pass. */
function outcome(
  kind: 'planned' | 'reclaimed',
  reclaim: readonly Reclaimable[],
  linkedBytes: number,
  retained: readonly Retained[],
  recency: Recency,
): PassOutcome {
  return {
    kind,
    reclaim,
    reclaimedBytes: reclaim.reduce((sum, entry) => sum + entry.bytes, 0),
    linkedBytes,
    retained,
    recency,
  };
}

// ---- Reporting ----

function gib(bytes: number): string {
  return `${(bytes / 2 ** 30).toFixed(2)} GiB`;
}

/** Empty when Cargo's stamping of use is evident, or there is nothing to judge it by. */
function describeRecency(recency: Recency): string {
  switch (recency.kind) {
    case 'use-stamped':
    case 'no-units':
      return '';
    case 'recency-unknown':
      return `; recency unknown: ${recency.reason}, so a unit's last use is its last compile`;
  }
}

function describeOutcome(subject: string, outcome: RetentionOutcome): string {
  switch (outcome.kind) {
    case 'planned':
    case 'reclaimed': {
      const verb = outcome.kind === 'planned' ? 'would reclaim' : 'reclaimed';
      return (
        `${subject}: ${verb} ${gib(outcome.reclaimedBytes)} in ${outcome.reclaim.length} superseded unit generations ` +
        `(${gib(outcome.linkedBytes)} more stays linked elsewhere); ${outcome.retained.length} retained` +
        describeRecency(outcome.recency)
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

/** One line: the target directory's outcome, then each nested root's. */
export function describeRetention(report: RetentionReport): string {
  return [
    describeOutcome(`Cargo retention ${report.targetDir}`, report.outcome),
    ...report.nestedRoots.map((nested) => describeOutcome(`nested ${nested.root}`, nested.outcome)),
  ].join('; ');
}

export function describeRetentionError(error: RetentionError): string {
  switch (error.kind) {
    case 'cargo-metadata':
      return `cargo metadata in ${error.cwd} failed: ${error.detail}`;
    case 'target-not-cargo':
      return `${error.targetDir} is not a Cargo target directory: ${error.detail}`;
    case 'io':
      return `cannot ${error.operation} ${error.path}: ${error.detail}`;
  }
}
