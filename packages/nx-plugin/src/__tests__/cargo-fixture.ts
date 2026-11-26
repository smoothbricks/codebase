import { createHash } from 'node:crypto';
import { mkdir, readdir, readFile, realpath, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { type CargoMetadata, parseCargoMetadata } from '@smoothbricks/build-inputs/cargo-closure';
import { cargoFromMetadata, cargoResolveCache, resolutionKey } from '../cargo-source-hash.js';
import { isRecord } from '../type-guards.js';

const RECORDINGS_DIR = fileURLToPath(new URL('./cargo-fixture-recordings', import.meta.url));
const RECORDING_SCHEMA = 'cargo-fixture-recording-v1';
const WORKSPACE_ROOT_TOKEN = '<WORKSPACE_ROOT>';
const WORKSPACE_PARENT_TOKEN = '<WORKSPACE_PARENT>';
const RECORD_COMMAND =
  'cd packages/nx-plugin && RECORD_CARGO_FIXTURES=1 bun test --timeout=0 src/index.test.ts src/cargo-workspace.test.ts src/native-test-cache.test.ts';

export interface CargoFixtureOptions {
  /** Run real `cargo generate-lockfile` and leave the resolve cache unseeded so inference calls real `cargo metadata`. */
  readonly liveCargo?: boolean;
}

interface RecordedWorkspace {
  readonly lockfile: string;
  readonly metadata: unknown;
}

interface CargoFixtureRecording {
  readonly manifests: readonly string[];
  readonly workspaces: Readonly<Record<string, RecordedWorkspace>>;
}

function isNotFound(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT';
}

function posixPath(path: string): string {
  return path.replaceAll('\\', '/');
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function normalizeText(text: string, rootAliases: readonly string[], parentAliases: readonly string[]): string {
  let out = text;
  for (const alias of rootAliases) out = out.replaceAll(alias, WORKSPACE_ROOT_TOKEN);
  for (const alias of parentAliases) out = out.replaceAll(alias, WORKSPACE_PARENT_TOKEN);
  return out;
}

function hydrateText(text: string, canonicalRoot: string): string {
  return text
    .replaceAll(WORKSPACE_ROOT_TOKEN, canonicalRoot)
    .replaceAll(WORKSPACE_PARENT_TOKEN, dirname(canonicalRoot));
}

async function runCargo(args: readonly string[], cwd: string, label: string): Promise<string> {
  const child = Bun.spawn(['cargo', ...args], {
    env: process.env,
    cwd,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code !== 0) throw new Error(`${label}:\n${stdout}${stderr}`);
  return stdout;
}

async function recordWorkspace(
  canonicalRoot: string,
  relManifest: string,
  rootAliases: readonly string[],
  parentAliases: readonly string[],
): Promise<RecordedWorkspace> {
  const manifest = join(canonicalRoot, relManifest);
  const lockPath = join(dirname(manifest), 'Cargo.lock');
  if (!(await Bun.file(lockPath).exists())) {
    await runCargo(
      ['generate-lockfile', '--offline', '--manifest-path', manifest],
      canonicalRoot,
      `Cargo fixture ${manifest} could not lock`,
    );
  }
  const lockfile = await readFile(lockPath, 'utf8');
  const stdout = await runCargo(
    ['metadata', '--format-version', '1', '--locked', '--offline', '--manifest-path', manifest],
    canonicalRoot,
    `Cargo fixture ${manifest} could not resolve metadata`,
  );
  const metadata: CargoMetadata = parseCargoMetadata(stdout);
  metadata.workspace_root = await realpath(metadata.workspace_root);
  const members = new Set(metadata.workspace_members);
  for (const pkg of metadata.packages) {
    if (pkg.source !== null) continue;
    pkg.manifest_path = await realpath(pkg.manifest_path);
    for (const target of pkg.targets) target.src_path = await realpath(target.src_path);
    pkg.governing_manifest_path = members.has(pkg.id)
      ? join(metadata.workspace_root, 'Cargo.toml')
      : await realpath(
          (
            await runCargo(
              ['locate-project', '--workspace', '--manifest-path', pkg.manifest_path, '--message-format', 'plain'],
              canonicalRoot,
              `Cargo fixture ${pkg.manifest_path} could not locate governing workspace`,
            )
          ).trim(),
        );
  }
  const normalizedMetadata: unknown = JSON.parse(normalizeText(JSON.stringify(metadata), rootAliases, parentAliases));
  return { lockfile, metadata: normalizedMetadata };
}

function parseRecording(raw: string, file: string): CargoFixtureRecording {
  const parsed: unknown = JSON.parse(raw);
  if (!isRecord(parsed) || !Array.isArray(parsed.manifests) || !isRecord(parsed.workspaces)) {
    throw new Error(`Malformed Cargo fixture recording ${file}`);
  }
  const manifests: string[] = [];
  for (const item of parsed.manifests) {
    if (typeof item !== 'string') throw new Error(`Malformed Cargo fixture recording ${file}: non-string manifest`);
    manifests.push(item);
  }
  const workspaces: Record<string, RecordedWorkspace> = {};
  for (const [key, value] of Object.entries(parsed.workspaces)) {
    if (!isRecord(value) || typeof value.lockfile !== 'string' || !('metadata' in value)) {
      throw new Error(`Malformed Cargo fixture recording ${file}: invalid workspace entry ${key}`);
    }
    workspaces[key] = { lockfile: value.lockfile, metadata: value.metadata };
  }
  return { manifests, workspaces };
}

async function ensureRecordedLockfile(
  canonicalRoot: string,
  relManifest: string,
  recorded: RecordedWorkspace,
): Promise<boolean> {
  const manifest = join(canonicalRoot, relManifest);
  const lockPath = join(dirname(manifest), 'Cargo.lock');
  if (await Bun.file(lockPath).exists()) {
    // Explicit locks (including planted failures) remain untouched; only seed when the lock matches the recording.
    return (await readFile(lockPath, 'utf8')) === recorded.lockfile;
  }
  await writeFile(lockPath, recorded.lockfile);
  return true;
}

async function seedResolveCache(
  canonicalRoot: string,
  relManifest: string,
  indexed: ReadonlySet<string>,
  recorded: RecordedWorkspace,
): Promise<void> {
  const manifest = join(canonicalRoot, relManifest);
  const metadataJson = hydrateText(JSON.stringify(recorded.metadata), canonicalRoot);
  const metadata = parseCargoMetadata(metadataJson);
  const cargo = cargoFromMetadata(metadata);
  const key = resolutionKey(canonicalRoot, manifest, cargo, indexed);
  const cacheDirectory = cargoResolveCache(canonicalRoot, manifest).directory;
  await mkdir(cacheDirectory, { recursive: true });
  await writeFile(join(cacheDirectory, 'entry.cache'), `${key}\n${metadataJson}\n`);
}

/**
 * Target-inference fixtures declare real Cargo workspaces. Supply their empty Rust library and initial lock, and seed
 * `readCargoResolve`'s production cache from the committed recording keyed by the fixture's manifest set. Explicit
 * sources and locks, including planted failures, remain untouched. Pass `{ liveCargo: true }` to exercise real Cargo
 * directly (used by the single real-Cargo smoke test).
 */
export async function prepareCargoFixture(root: string, options?: CargoFixtureOptions): Promise<void> {
  const canonicalRoot = await realpath(root);
  const entries = (await readdir(canonicalRoot, { recursive: true }))
    .map(posixPath)
    .filter((path) => !path.split('/').some((part) => ['node_modules', 'target', '.nx'].includes(part)));
  const manifests = entries
    .filter((path) => path === 'Cargo.toml' || path.endsWith('/Cargo.toml'))
    .sort(compareStrings);
  const cargoConfigs = entries
    .filter(
      (path) =>
        path === '.cargo/config' ||
        path === '.cargo/config.toml' ||
        path.endsWith('/.cargo/config') ||
        path.endsWith('/.cargo/config.toml'),
    )
    .sort(compareStrings);

  const workspaces: string[] = [];
  const manifestContents = new Map<string, string>();
  for (const manifest of manifests) {
    const path = join(canonicalRoot, manifest);
    const contents = await readFile(path, 'utf8');
    manifestContents.set(manifest, contents);
    if (/^\[package\]\s*$/m.test(contents)) {
      const source = join(dirname(path), 'src/lib.rs');
      if (!(await Bun.file(source).exists())) {
        await mkdir(dirname(source), { recursive: true });
        await writeFile(source, '// Empty Cargo fixture library.\n');
      }
    }
    if (/^\[workspace\]\s*$/m.test(contents)) workspaces.push(manifest);
  }
  if (workspaces.length === 0) return;

  if (options?.liveCargo === true) {
    for (const relManifest of workspaces) {
      const manifest = join(canonicalRoot, relManifest);
      if (await Bun.file(join(dirname(manifest), 'Cargo.lock')).exists()) continue;
      await runCargo(
        ['generate-lockfile', '--offline', '--manifest-path', manifest],
        canonicalRoot,
        `Cargo fixture ${manifest} could not lock`,
      );
    }
    return;
  }

  const rawRoot = resolve(root);
  const rootAliases = [...new Set([canonicalRoot, rawRoot])].sort((left, right) => right.length - left.length);
  const parentAliases = [...new Set([dirname(canonicalRoot), dirname(rawRoot)])].sort(
    (left, right) => right.length - left.length,
  );

  const digest = createHash('sha256').update(`${RECORDING_SCHEMA}\0`);
  for (const manifest of manifests) {
    const normalized = normalizeText(manifestContents.get(manifest) ?? '', rootAliases, parentAliases);
    digest.update(`${manifest}\0${normalized}\0`);
  }
  for (const config of cargoConfigs) {
    const normalized = normalizeText(await readFile(join(canonicalRoot, config), 'utf8'), rootAliases, parentAliases);
    digest.update(`${config}\0${normalized}\0`);
  }
  const fixtureKey = digest.digest('hex').slice(0, 16);
  const recordingPath = join(RECORDINGS_DIR, `${fixtureKey}.json`);
  const indexed = new Set(manifests);

  let recording: CargoFixtureRecording | null = null;
  try {
    recording = parseRecording(await readFile(recordingPath, 'utf8'), recordingPath);
  } catch (error) {
    if (!isNotFound(error)) throw error;
  }

  if (process.env.RECORD_CARGO_FIXTURES === '1') {
    const recordedWorkspaces: Record<string, RecordedWorkspace> = {};
    for (const relManifest of workspaces) {
      recordedWorkspaces[relManifest] = await recordWorkspace(canonicalRoot, relManifest, rootAliases, parentAliases);
    }
    const fresh: CargoFixtureRecording = { manifests, workspaces: recordedWorkspaces };
    await mkdir(RECORDINGS_DIR, { recursive: true });
    await writeFile(recordingPath, `${JSON.stringify(fresh, null, 2)}\n`);
    recording = fresh;
  } else if (recording === null) {
    throw new Error(
      `Missing recorded Cargo fixture metadata: ${recordingPath}\n` +
        `Manifests: ${manifests.join(', ')}\n` +
        `Record or update fixtures with:\n  ${RECORD_COMMAND}`,
    );
  }

  // Write every workspace's Cargo.lock before computing any resolutionKey, because resolutionFiles hashes every
  // indexed manifest directory's Cargo.lock across nested workspaces.
  const seedable: readonly (readonly [string, RecordedWorkspace])[] = await Promise.all(
    workspaces.map(async (relManifest) => {
      const recorded = recording.workspaces[relManifest];
      if (recorded === undefined) {
        throw new Error(
          `Cargo fixture recording ${recordingPath} has no entry for ${relManifest}.\n` +
            `Record or update fixtures with:\n  ${RECORD_COMMAND}`,
        );
      }
      const matchesLock = await ensureRecordedLockfile(canonicalRoot, relManifest, recorded);
      return matchesLock ? ([relManifest, recorded] as const) : null;
    }),
  ).then((entries) => entries.filter((entry): entry is readonly [string, RecordedWorkspace] => entry !== null));

  for (const [relManifest, recorded] of seedable) {
    await seedResolveCache(canonicalRoot, relManifest, indexed, recorded);
  }
}
