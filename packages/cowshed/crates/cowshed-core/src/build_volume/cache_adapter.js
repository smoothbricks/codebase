// One version-sensitive boundary for the checkout's own Nx cache (05_gateway.md, "Publication").
// DbCache and its native cache field are INTERNAL Nx APIs, not a public stable adapter: each Nx version
// it was read against is declared in openNxCache with the layout it relies on, and any other is refused.
// biome-ignore lint/correctness/noUnusedVariables: invoked by the bodies cache_adapter::script prefixes (cache_adapter.rs), not from this file.
function openNxCache(root) {
  const path = require('node:path');
  const { createRequire } = require('node:module');
  if (!root || !path.isAbsolute(root)) {
    throw new Error(`Nx cache adapter requires an absolute checkout root, got ${JSON.stringify(root)}`);
  }
  // What each qualified Nx version fixes for this adapter, read in nrwl/nx at that tag: the checkout's
  // task-database and cache directories (`utils/cache-directory.ts`). In both, the task database is
  // `<machine>-v<DB_VERSION>.db` there (v3 in 23.2.1, v6 in 24.0.3: `native/db/mod.rs`, `initialize.rs`),
  // reached only through Nx's own `NxCache.get`; an entry is `<cache>/<hash>/` with its terminal output in
  // `<cache>/terminalOutputs/<hash>` (`native/cache/cache.rs`); `CachedResult` is `{ code, terminalOutput,
  // outputsPath, size }`; and `HttpRemoteCache` stores and restores an entry as the archive nxArchive (below) writes
  // (`native/cache/http_remote_cache.rs`, the same source at both tags, built with tar 0.4.44 and flate2 1.1.9 in each
  // tag's Cargo.lock). 23.2.1's `get` answers a row whose entry directory is gone; 24.0.3's does not.
  const layouts = new Map([
    ['23.2.1', { workspaceData: '.nx/workspace-data', cache: '.nx/cache' }],
    ['24.0.3', { workspaceData: '.nx/workspace-data', cache: '.nx/cache' }],
  ]);
  const requireNx = createRequire(path.join(root, 'package.json'));
  const version = requireNx('nx/package.json').version;
  const layout = layouts.get(version);
  if (!layout) {
    throw new Error(
      `Nx cache adapter supports exactly Nx ${[...layouts.keys()].join(' or ')}, not ${version}; requalify the adapter against it`,
    );
  }
  const workspaceData = path.join(root, layout.workspaceData);
  const cacheDir = path.join(root, layout.cache);
  // Nx fixes these paths at module load. Every root is handled in its own existing job process.
  process.env.NX_WORKSPACE_ROOT_PATH = root;
  process.env.NX_WORKSPACE_DATA_DIRECTORY = workspaceData;
  process.env.NX_CACHE_DIRECTORY = cacheDir;
  if (requireNx('nx/src/native').IS_WASM) {
    throw new Error('The Nx cache adapter requires the pinned native database cache, not WASM');
  }
  // Nx resolves its workspace-data directory itself; prove it is this checkout's before any database is opened.
  const { sharedDataDirectory } = requireNx('nx/src/utils/cache-directory');
  const actualWorkspaceData = sharedDataDirectory(root, 'workspace-data');
  if (path.resolve(actualWorkspaceData) !== path.resolve(workspaceData)) {
    throw new Error(
      `Nx ${version} workspace-data directory changed: expected ${workspaceData}, actual ${actualWorkspaceData}`,
    );
  }
  const { DbCache } = requireNx('nx/src/tasks-runner/cache');
  // Construction, not init(): Nx owns all schema/metadata setup; no remote loader is contacted.
  const cache = new DbCache({ nxCloudRemoteCache: null, skipRemoteCache: true });
  const native = cache.cache;
  if (!native || typeof native.get !== 'function' || typeof native.cacheDirectory !== 'string') {
    throw new Error(`Nx ${version} DbCache native cache surface changed; requalify the adapter`);
  }
  if (path.resolve(native.cacheDirectory) !== path.resolve(cacheDir)) {
    throw new Error(`Nx ${version} cache directory changed: expected ${cacheDir}, actual ${native.cacheDirectory}`);
  }
  return {
    requireNx,
    cache,
    cacheDir: native.cacheDirectory,
    getLocal: (hash) => native.get(hash),
    archive: (hash, terminalOutput, code) => nxArchive(path.join(native.cacheDirectory, hash), terminalOutput, code),
  };
}

// The archive of `<cache>/<hash>` Nx's native `HttpRemoteCache.store` sends and its `retrieve` unpacks in any checkout,
// written here so a land archives only a key its store lacks, and at gzip level 1 rather than Nx's default 6 (retrieve
// gunzips any level). With tar 0.4.44's `Builder::new()` (complete header mode) and symlinks not followed, `store` writes:
//  - `append_dir_all("", <cache>/<hash>)`: a stack walk from the entry directory, which pushes each directory's
//    `read_dir` entries in readdir order and takes the last pushed first, writing a directory before its children. Each
//    entry has a GNU header (magic "ustar ", version " \0") filled from its metadata: the mtime in whole seconds, uid,
//    gid, the whole st_mode, the size (0 for a directory or symlink), device fields of zero, and a symlink's target. A
//    path or target over 100 bytes is first written whole and NUL-terminated in a `././@LongLink` entry ('L' or 'K');
//  - `terminalOutput` (its UTF-8 bytes) and `code` (four big-endian bytes) under old-style headers;
//  - two zero blocks.
// Numbers are tar 0.4.44's: octal, NUL-terminated and zero-padded, or big-endian binary with the high bit set past what
// the field holds. These are Nx's own bytes (the publisher's tests compare them), except that a sparse file, which tar
// 0.4.44 records as GNU sparse on Linux only, is written whole; `retrieve` restores the same file either way.
const TAR_BLOCK = 512;
/** gzip is fed writes of at least 1 MiB, not one per 512-byte header. */
const TAR_WRITE = 1 << 20;
const TAR_TYPES = new Map([
  [0o100000n, '0'],
  [0o120000n, '2'],
  [0o040000n, '5'],
]);

/**
 * Nx's archive of the entry directory `entry` (above), gzipped at level 1: the body of one PUT as its compressed chunks
 * and their total length, sent as they are rather than joined into a second copy.
 */
async function nxArchive(entry, terminalOutput, code) {
  const { createGzip } = require('node:zlib');
  const { pipeline } = require('node:stream/promises');
  const compressed = [];
  await pipeline(tarWrites(tarStream(entry, terminalOutput, code)), createGzip({ level: 1 }), async (gzip) => {
    for await (const chunk of gzip) compressed.push(chunk);
  });
  return { chunks: compressed, length: compressed.reduce((sum, chunk) => sum + chunk.length, 0) };
}

/** Joins successive byte strings into writes of at least TAR_WRITE bytes. */
function* tarWrites(parts) {
  let held = [];
  let size = 0;
  for (const part of parts) {
    held.push(part);
    size += part.length;
    if (size >= TAR_WRITE) {
      yield held.length === 1 ? held[0] : Buffer.concat(held, size);
      held = [];
      size = 0;
    }
  }
  if (size > 0) yield Buffer.concat(held, size);
}

/** The tar stream `HttpRemoteCache.store` builds for `entry`, as successive byte strings. */
function* tarStream(entry, terminalOutput, code) {
  const fs = require('node:fs');
  // append_dir_all's stack: [source path, archive path (null for the entry itself), is a directory].
  const stack = [[Buffer.from(entry), null, true]];
  while (stack.length > 0) {
    const [source, name, isDirectory] = stack.pop();
    if (isDirectory) {
      const directory = fs.opendirSync(source, { encoding: 'buffer' });
      try {
        for (let child = directory.readSync(); child !== null; child = directory.readSync()) {
          const archived = name === null ? child.name : Buffer.concat([name, TAR_SLASH, child.name]);
          stack.push([Buffer.concat([source, TAR_SLASH, child.name]), archived, child.isDirectory()]);
        }
      } finally {
        directory.closeSync();
      }
      if (name !== null) yield* tarEntry(name, fs.statSync(source, { bigint: true }), null);
      continue;
    }
    const stat = fs.lstatSync(source, { bigint: true });
    if (stat.isSymbolicLink()) {
      yield* tarEntry(name, stat, fs.readlinkSync(source, { encoding: 'buffer' }));
    } else if (stat.isFile()) {
      yield* tarFile(fs, source, name);
    } else {
      throw new Error(`${source} is neither a directory, a file nor a symlink, which no Nx cache entry holds`);
    }
  }
  yield* tarOld('terminalOutput', Buffer.from(terminalOutput, 'utf8'));
  const exitCode = Buffer.alloc(4);
  exitCode.writeUInt32BE(code);
  yield* tarOld('code', exitCode);
  yield Buffer.alloc(2 * TAR_BLOCK);
}

/** A regular file's header and bytes, its length the one its header names: a file that changes meanwhile is refused. */
function* tarFile(fs, source, name) {
  const fd = fs.openSync(source, 'r');
  try {
    const stat = fs.fstatSync(fd, { bigint: true });
    yield* tarEntry(name, stat, null);
    let left = Number(stat.size);
    while (left > 0) {
      const chunk = Buffer.allocUnsafe(Math.min(left, TAR_WRITE));
      const read = fs.readSync(fd, chunk, 0, chunk.length, null);
      if (read === 0) throw new Error(`${source} shrank while it was archived`);
      left -= read;
      yield read === chunk.length ? chunk : chunk.subarray(0, read);
    }
    if (fs.readSync(fd, Buffer.alloc(1), 0, 1, null) !== 0) throw new Error(`${source} grew while it was archived`);
    yield* tarPadding(stat.size);
  } finally {
    fs.closeSync(fd);
  }
}

/** tar 0.4.44's `append_fs`/`append_file` header for one walked entry, after any long-name entries it needs. */
function* tarEntry(name, stat, link) {
  const header = tarGnuHeader();
  yield* tarPath(header, name);
  tarNumber(header, 136, 12, tarSeconds(stat.mtimeNs));
  tarNumber(header, 108, 8, stat.uid);
  tarNumber(header, 116, 8, stat.gid);
  tarOctal(header, 100, 8, stat.mode);
  header.write(TAR_TYPES.get(stat.mode & 0o170000n), 156, 'latin1');
  tarNumber(header, 124, 12, stat.isFile() ? stat.size : 0n);
  tarOctal(header, 329, 8, 0n);
  tarOctal(header, 337, 8, 0n);
  if (link !== null) yield* tarLink(header, link);
  tarChecksum(header);
  yield header;
}

/** `append_data` under `Header::new_old()`: no magic, mode, owner or type, only the name, size and a zero mtime. */
function* tarOld(name, data) {
  const header = Buffer.alloc(TAR_BLOCK);
  tarNumber(header, 136, 12, 0n);
  tarNumber(header, 124, 12, BigInt(data.length));
  tarCopyPath(header, 0, 100, Buffer.from(name), false, false);
  tarChecksum(header);
  yield header;
  if (data.length > 0) yield data;
  yield* tarPadding(BigInt(data.length));
}

function tarGnuHeader() {
  const header = Buffer.alloc(TAR_BLOCK);
  header.write('ustar ', 257, 'latin1');
  header.write(' \0', 263, 'latin1');
  tarNumber(header, 136, 12, 0n);
  return header;
}

/** `prepare_header_path`: the name field, or a GNU long-name entry and the name cut to its valid UTF-8 within 100 bytes. */
function* tarPath(header, name) {
  try {
    tarCopyPath(header, 0, 100, name, false, false);
  } catch (error) {
    if (name.length < 100) throw error;
    yield* tarLongLink(name, 'L');
    const { isUtf8 } = require('node:buffer');
    let cut = 100;
    while (cut > 0 && !isUtf8(name.subarray(0, cut))) cut -= 1;
    tarCopyPath(header, 0, 100, name.subarray(0, cut), false, true);
  }
}

/** `prepare_header_link`: the link-name field, or a GNU long-link entry before the header. */
function* tarLink(header, link) {
  try {
    tarCopyPath(header, 157, 100, link, true, false);
  } catch (error) {
    if (link.length < 100) throw error;
    yield* tarLongLink(link, 'K');
  }
}

/** tar 0.4.44's `prepare_header`: a `././@LongLink` entry holding `data` NUL-terminated. */
function* tarLongLink(data, type) {
  const header = tarGnuHeader();
  header.write('././@LongLink', 0, 'latin1');
  tarOctal(header, 100, 8, 0o644n);
  tarNumber(header, 108, 8, 0n);
  tarNumber(header, 116, 8, 0n);
  tarNumber(header, 124, 12, BigInt(data.length + 1));
  header.write(type, 156, 'latin1');
  tarChecksum(header);
  yield header;
  yield Buffer.concat([data, Buffer.alloc(1)]);
  yield* tarPadding(BigInt(data.length + 1));
}

const TAR_SLASH = Buffer.from('/');

/**
 * tar 0.4.44's `copy_path_into_inner`: `path`'s components (std's `Path::components`) joined by '/' into the field,
 * each copy NUL-terminated while room remains. Like it, a component that does not fit throws after the ones before it
 * were written, and those stay in the header.
 */
function tarCopyPath(header, offset, length, path, isLinkName, isTruncatedGnuLongPath) {
  const end = offset + length;
  let at = offset;
  const copy = (bytes) => {
    if (bytes.length > end - at) throw new Error(`${path} is too long for a ${length}-byte tar field`);
    if (bytes.includes(0)) throw new Error(`${path} contains a NUL byte`);
    bytes.copy(header, at);
    if (at + bytes.length < end) header[at + bytes.length] = 0;
    at += bytes.length;
  };
  const components = tarComponents(path);
  let needsSlash = false;
  let emitted = false;
  for (const [index, { kind, bytes }] of components.entries()) {
    if (!isLinkName) {
      if (kind === 'root') throw new Error(`${path}: paths in archives must be relative`);
      if (kind === 'parent' && (!isTruncatedGnuLongPath || index < components.length - 1)) {
        throw new Error(`${path}: paths in archives must not have '..'`);
      }
      if (kind === 'current' && components.length !== 1) continue;
    }
    if (needsSlash) copy(TAR_SLASH);
    copy(bytes);
    if (kind !== 'root') needsSlash = true;
    emitted = true;
  }
  if (!emitted) throw new Error(`${path}: paths in archives must have at least one component`);
  if (path.length > 0 && path[path.length - 1] === 0x2f) copy(TAR_SLASH);
}

/** std's Unix `Path::components`: a leading root, a leading '.', then the '/'-separated names past empty and '.' ones. */
function tarComponents(path) {
  const components = [];
  if (path[0] === 0x2f) components.push({ kind: 'root', bytes: TAR_SLASH });
  else if (path[0] === 0x2e && (path.length === 1 || path[1] === 0x2f)) {
    components.push({ kind: 'current', bytes: Buffer.from('.') });
  }
  let start = 0;
  for (let index = 0; index <= path.length; index++) {
    if (index < path.length && path[index] !== 0x2f) continue;
    const bytes = path.subarray(start, index);
    start = index + 1;
    if (bytes.length === 0 || (bytes.length === 1 && bytes[0] === 0x2e)) continue;
    const parent = bytes.length === 2 && bytes[0] === 0x2e && bytes[1] === 0x2e;
    components.push({ kind: parent ? 'parent' : 'normal', bytes });
  }
  return components;
}

/** tar 0.4.44's `octal_into`: the low digits that fit, zero-padded, NUL last. */
function tarOctal(header, offset, length, value) {
  const digits = value.toString(8);
  header[offset + length - 1] = 0;
  for (let place = 1; place < length; place++) {
    const digit = digits.length - place;
    header[offset + length - 1 - place] = digit >= 0 ? digits.charCodeAt(digit) : 0x30;
  }
}

/** tar 0.4.44's `num_field_wrapper_into`: octal, or big-endian binary with the high bit set past 2^33 (2^21 in 8 bytes). */
function tarNumber(header, offset, length, value) {
  if (value >= 1n << 33n || (value >= 1n << 21n && length === 8)) {
    header.fill(0, offset, offset + length - 8);
    header.writeBigUInt64BE(value, offset + length - 8);
    header[offset] |= 0x80;
  } else {
    tarOctal(header, offset, length, value);
  }
}

/** `set_cksum`: the byte sum with the checksum field read as spaces, in its octal form. */
function tarChecksum(header) {
  let sum = 8 * 0x20;
  for (let index = 0; index < TAR_BLOCK; index++) {
    if (index < 148 || index >= 156) sum += header[index];
  }
  tarOctal(header, 148, 8, BigInt(sum));
}

/** st_mtime as `MetadataExt::mtime() as u64`: whole seconds, floored, two's complement when negative. */
function tarSeconds(nanoseconds) {
  const seconds = nanoseconds / 1_000_000_000n;
  const floored = nanoseconds < 0n && seconds * 1_000_000_000n !== nanoseconds ? seconds - 1n : seconds;
  return BigInt.asUintN(64, floored);
}

function* tarPadding(size) {
  const rest = Number(size % BigInt(TAR_BLOCK));
  if (rest !== 0) yield Buffer.alloc(TAR_BLOCK - rest);
}
