import { createHash, type Hash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { join, sep } from 'node:path';

import { isNonSourceDirectory } from './source-directories.js';

/**
 * A release rewrites `version` in every manifest it publishes. Those manifests
 * are inputs of the validation targets, so a publish run misses the cache for
 * the whole gate set over a change that alters no code.
 *
 * Nx hashes JSON manifests with the version removed natively, through a `json`
 * input with `excludeFields` — no process, and the file hashing stays in the
 * native hasher. TOML has no such input, so crate manifests are the one
 * remaining case the plugin computes itself, and this module is only that
 * case. It runs inside `createNodes`, which already reads these manifests, and
 * the digest it returns rides into the project's named inputs as a literal —
 * Nx hashes a project's `namedInputs` definitions as part of the project
 * configuration, so no process and no `runtime` carrier is needed to get the
 * value into the task hash.
 *
 * This treatment belongs to checks that READ source, never to a target that
 * produces a shipped artifact. A crate embeds its version at compile time
 * through `env!("CARGO_PKG_VERSION")`, so a version-insensitive build hash
 * would let a post-bump build hit a pre-bump artifact and ship a binary
 * reporting the previous version — silent, and worse than the cache miss it
 * saves.
 *
 * Every fallback here over-invalidates rather than under-invalidates. A
 * manifest excluded from a fileset with no digest replacing it serves stale
 * results in silence, so nothing in this module drops bytes it is unsure
 * about: what it cannot read line by line, it hashes verbatim.
 */
const DIGEST_TAG = 'versionless-crate-manifests-v2';

const CARGO_MANIFEST = 'Cargo.toml';

/** Frames the parts of the digest, so no path can spell out another part's content. */
const SEPARATOR = Buffer.of(0);

/** The ASCII the scanner steers by. Cargo manifests are UTF-8, whose non-ASCII bytes are all >= 0x80. */
const TAB = 0x09;
const LINE_FEED = 0x0a;
const CARRIAGE_RETURN = 0x0d;
const SPACE = 0x20;
const QUOTATION_MARK = 0x22;
const NUMBER_SIGN = 0x23;
const HYPHEN = 0x2d;
const FULL_STOP = 0x2e;
const DIGIT_ZERO = 0x30;
const DIGIT_NINE = 0x39;
const EQUALS_SIGN = 0x3d;
const UPPER_A = 0x41;
const UPPER_Z = 0x5a;
const LEFT_SQUARE_BRACKET = 0x5b;
const REVERSE_SOLIDUS = 0x5c;
const RIGHT_SQUARE_BRACKET = 0x5d;
const LOW_LINE = 0x5f;
const LOWER_A = 0x61;
const LOWER_Z = 0x7a;
const LEFT_CURLY_BRACKET = 0x7b;
const RIGHT_CURLY_BRACKET = 0x7d;
const APOSTROPHE = 0x27;

const PACKAGE = 'package';
const WORKSPACE = 'workspace';
const VERSION = 'version';

/**
 * Feed one crate manifest to `hash` without the versions the crate declares
 * for itself.
 *
 * A forward scan over the bytes that hands the runs it keeps straight to the
 * hasher as views: the manifest is never parsed into objects, never
 * re-serialised, and never copied. `[package].version` and the
 * `[workspace.package].version` that members inherit are the crate's own and
 * go whole — trailing comment and newline included. A `version` under a
 * dependency table, a `[target.'cfg(…)'.dependencies]` table or an inline
 * table names a DIFFERENT crate and stays: dropping it would let a genuinely
 * different dependency serve a stale result.
 *
 * Some of TOML is out of reach of a line-oriented scan. A multi-line basic or
 * literal string can hold anything at all, a `[package]` header and a
 * `version = "…"` line included, and no scan that has not lexed the whole
 * document can tell that text from the document. So from the first `"""` or
 * `'''`, from an unterminated value, and from any line it cannot classify,
 * this elides nothing more and hashes the rest of the manifest verbatim. That
 * direction is the safe one: hashing too much costs a cache miss on release,
 * hashing too little serves a wrong result.
 */
export function updateVersionlessCrateManifest(hash: Hash, manifest: Buffer): void {
  const end = manifest.length;
  // Bytes scanned but not yet handed over. Only an elision advances this past
  // a line, so a manifest with no version at all is fed as one whole view.
  let kept = 0;
  // Whether the keys being read belong to a table whose `version` is the
  // crate's own rather than some dependency's.
  let ownVersionTable = false;
  // Brackets left open by a value that spans lines: `keywords = [` and the
  // lines under it are value, not statements.
  let depth = 0;
  let line = 0;
  while (line < end) {
    const lineFeed = manifest.indexOf(LINE_FEED, line);
    const lineEnd = lineFeed < 0 ? end : lineFeed;
    const nextLine = lineFeed < 0 ? end : lineFeed + 1;
    if (depth > 0) {
      depth = readDepth(manifest, line, lineEnd, depth);
      if (depth < 0) break;
      line = nextLine;
      continue;
    }
    const at = skipBlanks(manifest, line, lineEnd);
    if (at === lineEnd || manifest[at] === NUMBER_SIGN) {
      line = nextLine;
      continue;
    }
    if (manifest[at] === LEFT_SQUARE_BRACKET) {
      // Only a comment may follow a header, so the rest of the line is not
      // scanned: a `"""` inside that comment is not a multi-line string.
      ownVersionTable = isOwnVersionTable(manifest, at + 1, lineEnd);
      line = nextLine;
      continue;
    }
    const equals = readKeyEnd(manifest, at, lineEnd);
    if (equals < 0) break;
    const own = ownVersionTable && isOwnVersionKey(manifest, at, equals);
    depth = readDepth(manifest, equals + 1, lineEnd, 0);
    if (depth < 0) break;
    if (own && depth === 0) {
      hash.update(manifest.subarray(kept, line));
      kept = nextLine;
    }
    line = nextLine;
  }
  hash.update(manifest.subarray(kept, end));
}

/**
 * The digest of every crate manifest under a project, with the versions the
 * crates declare for themselves removed.
 *
 * This is exactly what the validation targets exclude from their filesets.
 * Paths are part of the digest, so moving a crate is a change even when no
 * manifest body differs. A project with no crate manifest never declares this
 * input at all, so the empty digest is not a case that reaches Nx.
 *
 * A crate that pins a workspace sibling by `path` + `version` still carries
 * that sibling's version, so a release does invalidate it. That is the honest
 * answer: the dependency the digest describes really did change.
 */
export async function hashVersionlessCrateManifests(projectRoot: string, workspaceRoot: string): Promise<string> {
  const root = join(workspaceRoot, projectRoot);
  const paths: string[] = [];
  await collectCrateManifests(root, '', paths);
  paths.sort();
  const manifests = await Promise.all(paths.map((path) => readFile(join(root, path))));
  const hash = createHash('sha256');
  hash.update(DIGEST_TAG);
  hash.update(SEPARATOR);
  for (let index = 0; index < paths.length; index++) {
    hash.update(paths[index] as string);
    hash.update(SEPARATOR);
    updateVersionlessCrateManifest(hash, manifests[index] as Buffer);
    hash.update(SEPARATOR);
  }
  return hash.digest('hex');
}

/** Collects manifest paths relative to the project root, `/`-separated so the digest is platform-independent. */
async function collectCrateManifests(directory: string, prefix: string, found: string[]): Promise<void> {
  // No `catch`: an unreadable directory would otherwise hash as an empty
  // manifest set, which is the silent-staleness failure this module exists to
  // avoid. The caller degrades to the raw manifests instead. Symlinked
  // directories are not entries `isDirectory` reports, so the walk cannot loop
  // and matches what Nx keeps in its file map.
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      // The exclusions every source walk in this plugin uses, so the digest
      // covers the same files Nx keeps in its file map.
      if (!isNonSourceDirectory(entry.name)) {
        await collectCrateManifests(`${directory}${sep}${entry.name}`, `${prefix}${entry.name}/`, found);
      }
    } else if (entry.name === CARGO_MANIFEST) {
      found.push(`${prefix}${entry.name}`);
    }
  }
}

/** `from` is just past the `[`. */
function isOwnVersionTable(manifest: Buffer, from: number, to: number): boolean {
  // `[[bin]]` and every other array of tables: a crate's own version is never
  // declared in one.
  if (manifest[from] === LEFT_SQUARE_BRACKET) return false;
  let after = readWord(manifest, from, to, PACKAGE);
  if (after < 0) {
    const afterWorkspace = readWord(manifest, from, to, WORKSPACE);
    if (afterWorkspace < 0) return false;
    const dot = skipBlanks(manifest, afterWorkspace, to);
    if (dot >= to || manifest[dot] !== FULL_STOP) return false;
    after = readWord(manifest, dot + 1, to, PACKAGE);
    if (after < 0) return false;
  }
  // The path has to END here: `[package.metadata]` is a different table, and
  // the version under it is not the crate's.
  const close = skipBlanks(manifest, after, to);
  return close < to && manifest[close] === RIGHT_SQUARE_BRACKET;
}

/** `to` is the offset of the `=`, so the key path must end exactly there. */
function isOwnVersionKey(manifest: Buffer, from: number, to: number): boolean {
  const afterVersion = readWord(manifest, from, to, VERSION);
  if (afterVersion < 0) return false;
  const dot = skipBlanks(manifest, afterVersion, to);
  if (dot === to) return true;
  if (manifest[dot] !== FULL_STOP) return false;
  // `version.workspace = true` inherits the version, so it is the same field.
  const afterWorkspace = readWord(manifest, dot + 1, to, WORKSPACE);
  return afterWorkspace >= 0 && skipBlanks(manifest, afterWorkspace, to) === to;
}

/**
 * Offset just past `word` written bare, `"quoted"` or `'quoted'`, or -1.
 *
 * A quoted key that spells the word with an escape (`"pack\u0061ge"`) does not
 * match: decoding it would mean carrying a decoder, and not matching only
 * keeps bytes the digest could have dropped.
 */
function readWord(manifest: Buffer, from: number, to: number, word: string): number {
  const at = skipBlanks(manifest, from, to);
  const quote = manifest[at] as number;
  const quoted = quote === QUOTATION_MARK || quote === APOSTROPHE;
  const start = quoted ? at + 1 : at;
  const stop = start + word.length;
  if (stop > to) return -1;
  for (let index = 0; index < word.length; index++) {
    if (manifest[start + index] !== word.charCodeAt(index)) return -1;
  }
  if (quoted) return manifest[stop] === quote ? stop + 1 : -1;
  // A bare key runs on: `versioning` is not `version`.
  return stop === to || !isBareKeyByte(manifest[stop] as number) ? stop : -1;
}

/** Offset of the `=` that closes this line's key path, or -1 when the line is not one. */
function readKeyEnd(manifest: Buffer, from: number, to: number): number {
  let at = from;
  for (;;) {
    at = readKeySegment(manifest, at, to);
    if (at < 0) return -1;
    at = skipBlanks(manifest, at, to);
    if (at >= to) return -1;
    const byte = manifest[at];
    if (byte === EQUALS_SIGN) return at;
    if (byte !== FULL_STOP) return -1;
    at++;
  }
}

/** Offset just past one key segment — bare, `"quoted"` or `'quoted'` — or -1. */
function readKeySegment(manifest: Buffer, from: number, to: number): number {
  const at = skipBlanks(manifest, from, to);
  if (at >= to) return -1;
  const byte = manifest[at];
  if (byte === QUOTATION_MARK || byte === APOSTROPHE) return readStringEnd(manifest, at, to);
  let stop = at;
  while (stop < to && isBareKeyByte(manifest[stop] as number)) stop++;
  return stop === at ? -1 : stop;
}

/**
 * Bracket depth still open at the end of a value or of the line continuing
 * one, or -1 when the scan must stop: a multi-line string opens, a string
 * never closes, or a bracket closes one that never opened.
 */
function readDepth(manifest: Buffer, from: number, to: number, depth: number): number {
  let open = depth;
  let at = from;
  while (at < to) {
    const byte = manifest[at] as number;
    // A comment runs to the end of the line and cannot open anything.
    if (byte === NUMBER_SIGN) return open;
    if (byte === QUOTATION_MARK || byte === APOSTROPHE) {
      if (manifest[at + 1] === byte && manifest[at + 2] === byte) return -1;
      at = readStringEnd(manifest, at, to);
      if (at < 0) return -1;
      continue;
    }
    if (byte === LEFT_SQUARE_BRACKET || byte === LEFT_CURLY_BRACKET) {
      open++;
    } else if (byte === RIGHT_SQUARE_BRACKET || byte === RIGHT_CURLY_BRACKET) {
      open--;
      if (open < 0) return -1;
    }
    at++;
  }
  return open;
}

/** `from` is the opening quote. Offset just past the closing one, or -1 when the line ends first. */
function readStringEnd(manifest: Buffer, from: number, to: number): number {
  const quote = manifest[from];
  let at = from + 1;
  while (at < to) {
    const byte = manifest[at];
    // A literal string has no escapes: `'a\'` closes at that quote.
    if (byte === REVERSE_SOLIDUS && quote === QUOTATION_MARK) {
      at += 2;
      continue;
    }
    if (byte === quote) return at + 1;
    at++;
  }
  return -1;
}

/** Whitespace between tokens. A `\r` only ever precedes the `\n` a line was split on. */
function skipBlanks(manifest: Buffer, from: number, to: number): number {
  let at = from;
  while (at < to) {
    const byte = manifest[at];
    if (byte !== SPACE && byte !== TAB && byte !== CARRIAGE_RETURN) break;
    at++;
  }
  return at;
}

function isBareKeyByte(byte: number): boolean {
  return (
    (byte >= LOWER_A && byte <= LOWER_Z) ||
    (byte >= UPPER_A && byte <= UPPER_Z) ||
    (byte >= DIGIT_ZERO && byte <= DIGIT_NINE) ||
    byte === LOW_LINE ||
    byte === HYPHEN
  );
}
