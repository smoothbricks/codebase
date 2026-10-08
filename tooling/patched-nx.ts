#!/usr/bin/env bun
/**
 * Builds the patched Nx this repository and its consumers install: the registry `nx` tarball with
 * `patches/nx@<version>.patch` applied, repacked byte for byte the same on every machine.
 *
 * Why not `patchedDependencies`: Bun never puts a patched package in its global store (a patch could
 * diverge from a shared copy), so a patched `nx` installs project-local and writable under
 * `node_modules/.bun/`. A tarball dependency is immutable to Bun, so the patched Nx lands in
 * `~/.bun/install/cache/links` like every other package, where no sandbox can write it. The root
 * `package.json` keeps `nx` at its registry version and `overrides.nx` names this tarball's release URL;
 * `bun.lock` records its sha512 and Bun refuses an asset that differs.
 *
 * The release is content-addressed: its tag carries the uncompressed tar's sha256, so the same patch
 * always names the same release and a changed patch or packing names a new one. The tar is written
 * here (sorted regular files, fixed owner, mode and mtime) and gzip-framed here (no timestamp, OS
 * "unknown"), so nothing of the build host leaks into the bytes.
 *
 *   bun tooling/patched-nx.ts build <out-dir>    write <out-dir>/<asset>, its extracted package/, and release-notes.md;
 *                                                print the release as JSON
 *   bun tooling/patched-nx.ts verify <asset>     refuse unless the asset unpacks to the tar built now;
 *                                                print the release with the asset's own integrity
 *   bun tooling/patched-nx.ts lock <integrity>   refuse unless bun.lock resolves nx to this release's URL
 *                                                with exactly this integrity
 */

import { lstat, mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import { $ } from 'bun';

const NX_VERSION = '23.2.1';
/** The registry's own integrity for the tarball the patch applies to (`npm view nx@23.2.1 dist.integrity`). */
const NX_REGISTRY_INTEGRITY =
  'sha512-QOgM7EcJ6gvLKZ3Azno7wdoXXhRhZHbRDN79gCmGXsBxH9WBXCeSy3DyGdee/d5Fa6GnxVOgwKTCx8l0eA+vHw==';
const NX_REGISTRY_TARBALL = `https://registry.npmjs.org/nx/-/nx-${NX_VERSION}.tgz`;
const REPOSITORY = 'smoothbricks/codebase';
const ASSET = `nx-${NX_VERSION}.tgz`;
/** The upstream pull requests whose fixes the patch carries. */
const UPSTREAM = [
  ['nrwl/nx#37268', 'record task history on the client\u2019s own database connection'],
  ['nrwl/nx#37269', 'size the default cache bound from the cache\u2019s own filesystem'],
  ['nrwl/nx#37271', 'keep daemon plugin workers with graph hooks alive between graphs'],
  ['nrwl/nx#37272', 'resolve typescript and release version actions from the workspace root'],
] as const;
/** Repairs the patch carries that no upstream pull request proposes yet. */
const NOT_YET_UPSTREAM = [
  ['task graph', 'give a task the same dependencies whichever other tasks the run asks for (`findCycles`)'],
  [
    'task graph',
    'retain requested tasks and their real regular or continuous dependencies after dummy-cycle normalization',
  ],
  [
    'cache restore',
    'stamp each restored output with the time of the restore, not the time its cache entry was written',
  ],
  ['file hashes', 'hash every file again when the archive may hold a hash taken in the second its file was rewritten'],
  ['exit status', 'exit 1, not 130, when a failure skips tasks or bails; preserve 130 for a real interruption'],
  [
    'daemon claim',
    'start at most one daemon per workspace, and never close a socket another daemon bound (`startServer`)',
  ],
  [
    'daemon workspace identity',
    'ignore redundant trailing root separators without weakening foreign-workspace refusal or losing POSIX/drive/UNC roots',
  ],
  [
    'foreign environment',
    'refuse any command whose `NX_*` root, data, cache or socket directory names another workspace than the cwd\u2019s',
  ],
  [
    'run summary',
    'report success only when every task ended, and name the tasks a run that threw never finished (`endCommand`)',
  ],
  ['ignore files', 'stop the daemon for a `.gitignore` or `.nxignore` only when its watcher may hold other bytes'],
] as const;
/** npm's own fixed tar mtime (1985-10-26T08:15:00Z), so the packed entries match `npm pack`'s. */
const TAR_MTIME = 499162500;

const root = join(import.meta.dir, '..');
const patchPath = join(root, 'patches', `nx@${NX_VERSION}.patch`);

interface Release {
  readonly tag: string;
  readonly url: string;
  readonly asset: string;
  /** SRI sha512 of the gzip asset, the value bun.lock records. */
  readonly integrity: string;
  /** sha256 of the uncompressed tar, the content the tag is named for. */
  readonly tarSha256: string;
}

function sri(bytes: Uint8Array): string {
  return `sha512-${new Bun.CryptoHasher('sha512').update(bytes).digest('base64')}`;
}

function sha256(bytes: Uint8Array): string {
  return new Bun.CryptoHasher('sha256').update(bytes).digest('hex');
}

/** A field of `width` bytes holding `value` as zero-padded octal and a NUL. */
function octal(value: number, width: number): string {
  const digits = value.toString(8);
  if (digits.length > width - 1) throw new Error(`${value} does not fit a ${width}-byte tar field`);
  return `${digits.padStart(width - 1, '0')}\0`;
}

/** The ustar name and prefix for `path`, or a refusal naming the path that fits neither. */
function splitPath(path: Uint8Array, display: string): { name: Uint8Array; prefix: Uint8Array } {
  if (path.length <= 100) return { name: path, prefix: new Uint8Array() };
  for (let slash = path.lastIndexOf(0x2f, 155); slash > 0; slash = path.lastIndexOf(0x2f, slash - 1)) {
    if (path.length - slash - 1 <= 100) return { name: path.subarray(slash + 1), prefix: path.subarray(0, slash) };
  }
  throw new Error(`${display} is too long for a ustar name and prefix`);
}

function header(path: string, size: number, executable: boolean): Uint8Array {
  const block = new Uint8Array(512);
  const encoder = new TextEncoder();
  const { name, prefix } = splitPath(encoder.encode(path), path);
  const put = (offset: number, bytes: Uint8Array | string) =>
    block.set(typeof bytes === 'string' ? encoder.encode(bytes) : bytes, offset);
  put(0, name);
  put(100, octal(executable ? 0o755 : 0o644, 8));
  put(108, octal(0, 8));
  put(116, octal(0, 8));
  put(124, octal(size, 12));
  put(136, octal(TAR_MTIME, 12));
  put(148, '        ');
  put(156, '0');
  put(257, 'ustar\0');
  put(263, '00');
  put(345, prefix);
  const checksum = block.reduce((sum, byte) => sum + byte, 0);
  put(148, `${checksum.toString(8).padStart(6, '0')}\0 `);
  return block;
}

/** Every regular file under `directory`, as `[path relative to it, absolute path]`, in byte order. */
async function files(directory: string): Promise<[string, string][]> {
  const found: [string, string][] = [];
  for (const entry of await readdir(directory, { recursive: true, withFileTypes: true })) {
    const absolute = join(entry.parentPath, entry.name);
    if (entry.isDirectory()) continue;
    if (!entry.isFile()) throw new Error(`${absolute} is not a regular file; the packed Nx holds only files`);
    found.push([relative(directory, absolute), absolute]);
  }
  const encoder = new TextEncoder();
  return found.sort(([left], [right]) => Buffer.compare(encoder.encode(left), encoder.encode(right)));
}

/** The tar of `directory`'s files under `package/`, as npm packs a package. */
async function tar(directory: string): Promise<Uint8Array> {
  const blocks: Uint8Array[] = [];
  for (const [path, absolute] of await files(directory)) {
    const bytes = await readFile(absolute);
    const executable = ((await lstat(absolute)).mode & 0o111) !== 0;
    blocks.push(header(`package/${path}`, bytes.length, executable), bytes);
    const padding = (512 - (bytes.length % 512)) % 512;
    if (padding > 0) blocks.push(new Uint8Array(padding));
  }
  blocks.push(new Uint8Array(1024));
  return Buffer.concat(blocks);
}

/** RFC 1952 framing around raw DEFLATE: no name, no timestamp, OS 255, so the host never shows. */
function gzip(data: Uint8Array): Uint8Array {
  const trailer = new Uint8Array(8);
  const view = new DataView(trailer.buffer);
  view.setUint32(0, Bun.hash.crc32(data), true);
  view.setUint32(4, data.length >>> 0, true);
  return Buffer.concat([
    Uint8Array.of(0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 2, 0xff),
    Bun.deflateSync(data, { level: 9 }),
    trailer,
  ]);
}

/** The registry tarball with the patch applied, packed as the uncompressed tar. */
async function patchedTar(): Promise<Uint8Array> {
  const response = await fetch(NX_REGISTRY_TARBALL);
  if (!response.ok) throw new Error(`GET ${NX_REGISTRY_TARBALL}: ${response.status} ${response.statusText}`);
  const registry = new Uint8Array(await response.arrayBuffer());
  if (sri(registry) !== NX_REGISTRY_INTEGRITY) {
    throw new Error(`${NX_REGISTRY_TARBALL} is ${sri(registry)}, not the registry's ${NX_REGISTRY_INTEGRITY}`);
  }
  const work = await mkdtemp(join(tmpdir(), 'patched-nx-'));
  try {
    await writeFile(join(work, 'registry.tgz'), registry);
    await $`tar -xzf ${join(work, 'registry.tgz')} -C ${work}`.quiet();
    const extracted = join(work, 'package');
    await $`git apply --whitespace=nowarn ${patchPath}`.cwd(extracted).quiet();
    return await tar(extracted);
  } finally {
    await rm(work, { recursive: true, force: true });
  }
}

function release(tarBytes: Uint8Array, asset: Uint8Array): Release {
  const tarSha256 = sha256(tarBytes);
  const tag = `nx-${NX_VERSION}-patched-${tarSha256.slice(0, 12)}`;
  return {
    tag,
    url: `https://github.com/${REPOSITORY}/releases/download/${tag}/${ASSET}`,
    asset: ASSET,
    integrity: sri(asset),
    tarSha256,
  };
}

function notes(built: Release): string {
  return [
    `Nx ${NX_VERSION} from the npm registry (${NX_REGISTRY_INTEGRITY}) with \`patches/nx@${NX_VERSION}.patch\` applied,`,
    'packed reproducibly by `tooling/patched-nx.ts`.',
    '',
    'It carries these upstream fixes until Nx releases them:',
    '',
    ...UPSTREAM.map(([pr, what]) => `- https://github.com/${pr.replace('#', '/pull/')} (${pr}): ${what}`),
    '',
    'and these repairs, which no upstream pull request proposes yet:',
    '',
    ...NOT_YET_UPSTREAM.map(([area, what]) => `- ${area}: ${what}`),
    '',
    'A consumer keeps `nx` at its registry version and sets `overrides.nx` to this asset\u2019s URL; bun.lock pins:',
    '',
    `- ${built.asset}: \`${built.integrity}\``,
    `- uncompressed tar sha256: \`${built.tarSha256}\``,
    '',
    'Drop the override, and this release with it, once the installed Nx release contains all of these.',
    '',
  ].join('\n');
}

/**
 * Refuse unless bun.lock resolves `nx` to `built.url` with integrity `integrity`. Bun writes one line per
 * package, `"nx": ["nx@<resolution>", …, "<integrity>"],`, so the line is checked as Bun wrote it; Bun
 * itself refuses an asset whose bytes differ from that integrity at install.
 */
async function checkLock(built: Release, integrity: string): Promise<void> {
  const lock = await readFile(join(root, 'bun.lock'), 'utf8');
  const entry = lock.split('\n').find((line) => line.startsWith('    "nx": ['));
  if (entry === undefined) throw new Error('bun.lock has no packages entry for nx');
  if (!entry.startsWith(`    "nx": ["nx@${built.url}", `) || !entry.endsWith(`"${integrity}"],`)) {
    throw new Error(
      `bun.lock resolves nx as ${entry.trim().slice(0, 120)}…, not nx@${built.url} with ${integrity}; ` +
        'run `bun install` against the published release',
    );
  }
}

async function main([command, argument]: string[]): Promise<void> {
  if (command === 'build' && argument !== undefined) {
    const tarBytes = await patchedTar();
    const asset = gzip(tarBytes);
    const built = release(tarBytes, asset);
    await mkdir(argument, { recursive: true });
    await writeFile(join(argument, built.asset), asset);
    await writeFile(join(argument, 'release-notes.md'), notes(built));
    await rm(join(argument, 'package'), { recursive: true, force: true });
    await $`tar -xzf ${join(argument, built.asset)} -C ${argument}`.quiet();
    // The test package borrows this checkout's locked dependencies, not its installed Nx implementation.
    await symlink(
      relative(resolve(argument, 'package'), join(root, 'node_modules', '.bun', 'node_modules')),
      join(argument, 'package', 'node_modules'),
      'dir',
    );
    console.log(JSON.stringify(built));
    return;
  }
  if (command === 'verify' && argument !== undefined) {
    const asset = await readFile(argument);
    const tarBytes = await patchedTar();
    if (Buffer.compare(Bun.gunzipSync(asset), tarBytes) !== 0) {
      throw new Error(`${argument} does not unpack to the patched Nx tar built from patches/nx@${NX_VERSION}.patch`);
    }
    console.log(JSON.stringify(release(tarBytes, asset)));
    return;
  }
  if (command === 'lock' && argument !== undefined) {
    const tarBytes = await patchedTar();
    await checkLock(release(tarBytes, gzip(tarBytes)), argument);
    return;
  }
  throw new Error('usage: bun tooling/patched-nx.ts build <out-dir> | verify <asset> | lock <integrity>');
}

await main(Bun.argv.slice(2));
