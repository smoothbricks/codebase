import { createHash } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, isAbsolute, join, posix, resolve } from 'node:path';
import { publint } from 'publint';
import { formatMessage } from 'publint/utils';
import typia from 'typia';
import { githubCiNxRunMany } from '../github-ci/index.js';
import type { PackageExports } from '../lib/json.js';
import { run, runResult } from '../lib/run.js';
import {
  getWorkspacePackages,
  isOwnedPackage,
  listPublishablePackages,
  type PackageInfo,
  type RepositoryInfo,
  readPackageJson,
  repositoryInfo,
  workspaceDependencyFields,
} from '../lib/workspace.js';
import { syncBunLockfileVersions } from '../monorepo/lockfile.js';
import { readPackedPackageJson, validatePackedWorkspaceDependencies } from '../monorepo/packed-manifest.js';
import { withPublishManifest } from '../monorepo/publish-manifest.js';
import { loadNxProjects } from '../nx/index.js';

const parseReleasePackManifestText = typia.json.createIsParse<ReleasePackManifest>();
/** Runtime dependency fields expanded when computing the artifact closure. */
const runtimeDependencyFields = ['dependencies', 'optionalDependencies', 'peerDependencies'] as const;

export interface ReleasePackOptions {
  projects: string;
  output: string;
}

export interface ReleasePackPackageEntry {
  projectName: string;
  name: string;
  version: string;
  /** POSIX relative path inside the output directory. */
  tarball: string;
  /** Lowercase hex SHA-256 of the tarball bytes. */
  sha256: string;
  runtimeDependencies: string[];
}

export interface ReleasePackManifest {
  schemaVersion: 1;
  packages: ReleasePackPackageEntry[];
}

export function safeTarballPrefix(name: string): string {
  return name.replace(/^@/, '').replace(/[^a-zA-Z0-9._-]+/g, '-');
}

function tarballFileName(pkg: Pick<PackageInfo, 'name' | 'version'>): string {
  return `${safeTarballPrefix(pkg.name)}-${pkg.version}.tgz`;
}

/**
 * Shared artifact pack helper extracted from the publish path: normalize
 * workspace versions, pack with lifecycle scripts disabled, and assert the
 * packed manifest carries exact versions. Returns the temp tarball plus a
 * cleanup the caller runs in a finally. Both `release publish` and
 * `release pack` pack through here; there is no second exporter.
 *
 * Artifact-only means artifact-only: bun.lock is snapshotted byte-for-byte
 * before the publish-mode sync and restored on every path (success or
 * failure), never "normalized" to install mode, so the source tree and lock
 * are unchanged after packing.
 */
export async function packReleaseTarball(
  root: string,
  pkg: PackageInfo,
  options: { publishConfigRegistry?: string } = {},
): Promise<{ tarball: string; cleanup: () => Promise<void> }> {
  const lockSnapshot = await snapshotBunLockfile(root);
  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-pack-'));
  const tarball = join(tempDir, tarballFileName(pkg));
  const cleanup = async (): Promise<void> => {
    await rm(tempDir, { recursive: true, force: true });
  };
  // bun pm pack resolves workspace:* from bun.lock. Temporarily rewrite
  // unpublished -next entries to the last stable tag so the tarball embeds
  // installable versions, then restore the exact original bytes.
  try {
    syncBunLockfileVersions(root, { mode: 'publish', log: true });
    await runReleaseCheckGate(root, pkg);
    console.log(`${pkg.name}@${pkg.version}: packing with bun pm pack`);
    await withPublishManifest(
      join(root, pkg.path),
      () => run('bun', ['pm', 'pack', '--filename', tarball, '--ignore-scripts', '--quiet'], join(root, pkg.path)),
      { log: true, ...(options.publishConfigRegistry ? { publishConfigRegistry: options.publishConfigRegistry } : {}) },
    );
    const failures = validatePackedWorkspaceDependencies(
      root,
      pkg,
      await readPackedPackageJson(root, tarball, pkg.name),
      { mode: 'publish' },
    );
    if (failures.length > 0) {
      throw new Error(failures.join('\n'));
    }
    return { tarball, cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  } finally {
    await restoreBunLockfile(lockSnapshot);
  }
}
/**
 * Per-package release gate: when the project declares an Nx `release-check`
 * target, run it after prebuilt outputs merge and before the tarball exists.
 * The target itself owns what it asserts; this owner enforces the two
 * properties that make the assertion mean anything, reading the *resolved*
 * graph configuration so `targetDefaults` cannot smuggle either one back in:
 *
 * - Empty `dependsOn`: the gate runs in the publishing job, where the only
 *   complete artifact tree is the union of this job's build and the foreign
 *   platform outputs applied over it. A dependency would rebuild and replace
 *   that tree, so the gate would verify (or wipe) something other than what
 *   gets packed.
 * - `cache: false`: a cached gate is replayed, not run. Its inputs cannot
 *   describe artifacts merged in from another job, so a cache hit would
 *   silently skip verification of the very bytes being published.
 *
 * Undeclared packages pack exactly as before.
 */
async function runReleaseCheckGate(root: string, pkg: PackageInfo): Promise<void> {
  const gate = (await loadNxProjects(root))[pkg.projectName]?.targets?.['release-check'];
  if (!gate) {
    return;
  }
  const dependsOn = gate.dependsOn ?? [];
  if (dependsOn.length > 0) {
    throw new Error(
      `${pkg.projectName}: the release-check target must declare an empty dependsOn so the gate cannot rebuild merged prebuilt outputs; it resolves to ${JSON.stringify(dependsOn)}.`,
    );
  }
  if (gate.cache !== false) {
    throw new Error(
      `${pkg.projectName}: the release-check target must declare cache: false so the gate cannot be replayed from cache instead of verifying the packed artifacts.`,
    );
  }
  console.log(`${pkg.name}@${pkg.version}: running Nx release-check gate`);
  try {
    await githubCiNxRunMany(root, { targets: 'release-check', projects: pkg.projectName });
  } catch (error) {
    throw new Error(`${pkg.name}@${pkg.version}: release-check gate refused the release`, { cause: error });
  }
}

interface BunLockSnapshot {
  path: string;
  bytes: Uint8Array;
}

async function snapshotBunLockfile(root: string): Promise<BunLockSnapshot | null> {
  const path = join(root, 'bun.lock');
  try {
    return { path, bytes: await readFile(path) };
  } catch {
    return null;
  }
}

async function restoreBunLockfile(snapshot: BunLockSnapshot | null): Promise<void> {
  if (snapshot) {
    await writeFile(snapshot.path, snapshot.bytes);
  }
}

function parseProjectSelection(projects: string): string[] {
  const names = projects
    .split(',')
    .map((name) => name.trim())
    .filter((name) => name.length > 0);
  if (names.length === 0) {
    throw new Error('release pack requires --projects with at least one Nx project name.');
  }
  return [...new Set(names)];
}

/**
 * Resolve the selected projects plus their runtime closure. All refusals here
 * happen before any Nx build or pack: unknown projects, unpublishable
 * packages, foreign-repository packages, conflicting tags, unpublishable
 * internal runtime edges, and public-to-private runtime edges.
 */
export function resolvePackClosure(root: string, projects: string): PackageInfo[] {
  const names = parseProjectSelection(projects);
  const workspace = getWorkspacePackages(root);
  const byProject = new Map(workspace.map((pkg) => [pkg.projectName, pkg]));
  const byName = new Map(workspace.map((pkg) => [pkg.name, pkg]));
  const publishableNames = new Set(listPublishablePackages(root).map((pkg) => pkg.name));
  const rootManifest = readPackageJson(join(root, 'package.json'));
  const rootRepository = rootManifest ? repositoryInfo(rootManifest.json) : null;
  const selected: PackageInfo[] = [];
  for (const name of names) {
    const pkg = byProject.get(name);
    if (!pkg) {
      throw new Error(`${name}: unknown Nx project; no workspace package maps to it.`);
    }
    if (!publishableNames.has(pkg.name)) {
      throw new Error(
        `${name}: not a publishable package. Publishable packages are non-private with exactly one of the nx tags npm:public, npm:private.`,
      );
    }
    assertOwnedPackage(rootRepository, pkg);
    selected.push(pkg);
  }
  // Expand normal/optional/peer runtime dependencies over publishable packages.
  const closure = new Map<string, PackageInfo>();
  const queue = [...selected];
  for (const pkg of selected) {
    closure.set(pkg.name, pkg);
  }
  let head = queue.pop();
  while (head !== undefined) {
    const pkg = head;
    const isPublic = pkg.tags.includes('npm:public');
    for (const field of runtimeDependencyFields) {
      const deps = pkg.json[field] ?? {};
      for (const depName of Object.keys(deps)) {
        const dep = byName.get(depName);
        if (!dep) {
          continue;
        }
        if (!publishableNames.has(dep.name)) {
          throw new Error(
            `${pkg.name}: unpublishable internal runtime edge ${field}.${depName} (${dep.projectName} is not publishable).`,
          );
        }
        assertOwnedPackage(
          rootRepository,
          dep,
          `${pkg.name}: foreign-repository runtime edge ${field}.${depName} is not allowed.`,
        );
        if (isPublic && dep.tags.includes('npm:private')) {
          throw new Error(`${pkg.name}: public-to-private runtime edge ${field}.${depName} is not allowed.`);
        }
        if (!closure.has(dep.name)) {
          closure.set(dep.name, dep);
          queue.push(dep);
        }
      }
    }
    head = queue.pop();
  }
  return [...closure.values()].sort((a, b) => a.name.localeCompare(b.name));
}

function assertOwnedPackage(rootRepository: RepositoryInfo | null, pkg: PackageInfo, message?: string): void {
  if (!rootRepository || !isOwnedPackage(rootRepository, pkg)) {
    throw new Error(
      message ??
        `${pkg.projectName}: package repository does not match the root repository; refusing to pack foreign packages.`,
    );
  }
}

/**
 * Post-pack checks per tarball: no unresolved edges, no registry placeholder,
 * declared types present, every exported Wasm/JS asset present. Export
 * resolution reuses the existing publint packed-export validation; the Wasm
 * walk names the missing asset directly.
 */
export async function assertPackedArtifact(root: string, tarball: string, pkg: PackageInfo): Promise<void> {
  const manifest = await readPackedPackageJson(root, tarball, pkg.name);
  const failures: string[] = [];
  for (const field of workspaceDependencyFields) {
    for (const [name, range] of Object.entries(manifest[field] ?? {})) {
      if (/^(workspace:|link:|file:)/.test(range)) {
        failures.push(`${pkg.path}: packed ${field}.${name} must not contain ${range}`);
      }
    }
  }
  const registry = manifest.publishConfig?.registry;
  if (registry?.includes('$')) {
    failures.push(
      `${pkg.path}: packed publishConfig.registry must be a real registry URL, never an environment placeholder.`,
    );
  }
  const files = await listTarballFiles(root, tarball);
  const typesEntry = manifest.types ?? manifest.typings;
  if (typeof typesEntry === 'string' && typesEntry.length > 0 && !files.has(posix.join('package', typesEntry))) {
    failures.push(
      `${pkg.path}: declared types entry ${typesEntry} is missing from the packed tarball (check the files allowlist).`,
    );
  }
  for (const target of collectExportTargets(manifest.exports)) {
    if (target.endsWith('.wasm') && !files.has(posix.join('package', target))) {
      failures.push(`${pkg.path}: exported Wasm asset ${target} is missing from the packed tarball.`);
    }
  }
  const tarballBytes = await readFile(tarball);
  const lint = await publint({
    pack: { tarball: Uint8Array.from(tarballBytes).buffer as ArrayBuffer },
    level: 'error',
  });
  for (const message of lint.messages) {
    failures.push(`${pkg.path}: publint ${message.type} ${message.code}: ${formatMessage(message, lint.pkg)}`);
  }
  if (failures.length > 0) {
    throw new Error(failures.join('\n'));
  }
}

/** Every file target the exports map offers runtimes, without `./` prefixes. */
function collectExportTargets(exports: PackageExports | undefined): string[] {
  if (typeof exports === 'string') {
    return [normalizeExportTarget(exports)];
  }
  if (exports === null || exports === undefined || typeof exports !== 'object') {
    return [];
  }
  return Object.values(exports).flatMap((value) => collectExportTargets(value ?? undefined));
}

function normalizeExportTarget(target: string): string {
  return target.startsWith('./') ? target.slice(2) : target;
}

async function listTarballFiles(root: string, tarball: string): Promise<Set<string>> {
  const result = await runResult('tar', ['-tzf', tarball], root);
  if (result.exitCode !== 0) {
    throw new Error(`tar -tzf ${tarball} failed with exit code ${result.exitCode}`);
  }
  return new Set(
    result.stdout
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.length > 0),
  );
}

/**
 * Artifact-only pack: build via Nx, expand and validate the runtime closure,
 * pack with lifecycle scripts disabled, and write immutable tarballs plus a
 * manifest of projectName/name/version/relative tarball/SHA-256 and runtime
 * dependency names. Never calls registry status, changes Git refs/versions,
 * publishes, or pushes. Requires no registry credentials.
 */
export async function releasePack(root: string, options: ReleasePackOptions): Promise<void> {
  const closure = resolvePackClosure(root, options.projects);
  const outputDir = isAbsolute(options.output) ? options.output : resolve(root, options.output);
  await mkdir(outputDir, { recursive: true });
  const existing = await readdir(outputDir);
  if (existing.length > 0) {
    throw new Error(`Refusing to pack into nonempty output directory ${outputDir}; use an empty directory.`);
  }
  await run('nx', ['run-many', '-t', 'build', `--projects=${closure.map((pkg) => pkg.projectName).join(',')}`], root);
  const entries: ReleasePackPackageEntry[] = [];
  for (const pkg of closure) {
    const { tarball, cleanup } = await packReleaseTarball(root, pkg);
    try {
      await assertPackedArtifact(root, tarball, pkg);
      const fileName = tarballFileName(pkg);
      await copyFile(tarball, join(outputDir, fileName));
      const sha256 = createHash('sha256')
        .update(await readFile(join(outputDir, fileName)))
        .digest('hex');
      entries.push({
        projectName: pkg.projectName,
        name: pkg.name,
        version: pkg.version,
        tarball: fileName,
        sha256,
        runtimeDependencies: runtimeDependencyNames(pkg).sort(),
      });
    } finally {
      await cleanup();
    }
  }
  entries.sort((a, b) => a.name.localeCompare(b.name));
  const manifest: ReleasePackManifest = { schemaVersion: 1, packages: entries };
  await writeFile(join(outputDir, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
  // Self-verify digests on write so a corrupt artifact directory never leaves here.
  await verifyReleasePackManifest(outputDir);
}

function runtimeDependencyNames(pkg: PackageInfo): string[] {
  const names = new Set<string>();
  for (const field of runtimeDependencyFields) {
    for (const name of Object.keys(pkg.json[field] ?? {})) {
      names.add(name);
    }
  }
  return [...names];
}

/**
 * Verify an artifact directory against its manifest, in the order a consumer
 * needs: shape, then the name/tarball binding, then bytes. Path shape is
 * checked before any read so a hostile entry cannot name a file outside the
 * directory, duplicate names are refused because the manifest is the
 * name/version map a consumer pins against, and an unbound file is refused
 * because `release pack` writes into an empty directory -- anything else in
 * there is a mixed or tampered release.
 */
export async function verifyReleasePackManifest(outputDir: string): Promise<ReleasePackManifest> {
  const manifestPath = join(outputDir, 'manifest.json');
  const manifest = parseReleasePackManifestText(await readFile(manifestPath, 'utf8'));
  if (!manifest) {
    throw new Error(`${manifestPath}: invalid release pack manifest (expected schemaVersion 1 with typed entries).`);
  }
  const boundNames = new Set<string>();
  const boundTarballs = new Set<string>();
  for (const entry of manifest.packages) {
    if (entry.tarball !== basename(entry.tarball) || isAbsolute(entry.tarball) || entry.tarball.includes('..')) {
      throw new Error(`${entry.name}: tarball path must be a bare file name relative to the output directory.`);
    }
    if (boundNames.has(entry.name)) {
      throw new Error(`${entry.name}: appears twice in the manifest; one entry must bind one package name.`);
    }
    if (boundTarballs.has(entry.tarball)) {
      throw new Error(`${entry.name}: tarball ${entry.tarball} is claimed by more than one manifest entry.`);
    }
    boundNames.add(entry.name);
    boundTarballs.add(entry.tarball);
  }
  const unbound = (await readdir(outputDir)).filter((file) => file !== 'manifest.json' && !boundTarballs.has(file));
  if (unbound.length > 0) {
    throw new Error(
      `${outputDir}: holds ${unbound.sort().join(', ')}, which the manifest does not bind; a release artifact directory holds exactly its manifest and the tarballs it names.`,
    );
  }
  for (const entry of manifest.packages) {
    const digest = createHash('sha256')
      .update(await readFile(join(outputDir, entry.tarball)))
      .digest('hex');
    if (digest !== entry.sha256) {
      throw new Error(`${entry.name}: SHA-256 mismatch for ${entry.tarball} (manifest corrupt or tarball replaced).`);
    }
  }
  return manifest;
}
