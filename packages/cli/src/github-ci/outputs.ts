import { createHash } from 'node:crypto';
import { chmodSync, createReadStream, type Dirent, type Stats } from 'node:fs';
import { copyFile, lstat, mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import typia from 'typia';
import { listPublishablePackages, type PackageInfo } from '../lib/workspace.js';
import { type ProjectTargets, readProjectTargets } from '../nx/index.js';

export interface CollectedOutputFile {
  project: string;
  target: string;
  output: string;
  path: string;
  size: number;
  mode: number;
  sha256: string;
}

export interface CollectedOutputsManifest {
  version: 3;
  sourceSha: string;
  /** Independently versioned platform jobs must agree on the full npm closure. */
  packageVersions: Record<string, string>;
  files: CollectedOutputFile[];
}

export interface NxTargetRun {
  target: string;
  projects: ProjectTargets[];
}

/** One file `lstat` observed under a declared output. Symlinks are reported as facts, never followed. */
export interface OutputFileFact {
  path: string;
  kind: 'file' | 'symlink' | 'other';
  size: number;
  mode: number;
}

/** One declared output of one target, with the files it matched on disk. */
export interface DeclaredOutputMatch {
  declaredOutput: string;
  files: readonly OutputFileFact[];
}

/** One project target and the observed matches for each of its declared outputs. */
export interface OutputClaimant {
  project: ProjectTargets;
  target: string;
  outputs: readonly DeclaredOutputMatch[];
}

export type OutputClaim = Omit<CollectedOutputFile, 'sha256'>;

/**
 * A planner verdict. Refusals are values so the decision is testable without a filesystem; only the shell turns a
 * refusal into a thrown Error.
 */
export type Planned<T> = { readonly ok: true; readonly value: T } | ({ readonly ok: false } & Refusal);

/**
 * A refusal as a value. `cause` is the error the shell observed: the reason quotes it, and the value travels with the
 * refusal so the thrown Error keeps `{ cause }`.
 */
export interface Refusal {
  readonly reason: string;
  readonly cause?: unknown;
}

/** What the shell found at an output file, staged or applied. Bytes are hashed only when it is a regular file. */
export type StagedObservation =
  | { readonly kind: 'missing'; readonly cause: unknown }
  | { readonly kind: 'not-regular' }
  | { readonly kind: 'regular'; readonly size: number; readonly sha256: string };

/** What the shell found at a collected tree's staged `workspace` directory. */
export type StagedWorkspaceObservation =
  | { readonly kind: 'absent'; readonly cause: unknown }
  | { readonly kind: 'unreadable'; readonly cause: unknown }
  | { readonly kind: 'not-directory' }
  | { readonly kind: 'directory'; readonly regularFiles: readonly string[] };

export interface StagedOutputFile {
  /** The manifest entry, with its path already validated by the shell. */
  entry: CollectedOutputFile;
  /** Absolute staged location, resolved by the shell so the planner never builds paths. */
  source: string;
  observed: StagedObservation;
}

export interface CollectedTree {
  manifestPath: string;
  manifest: Pick<CollectedOutputsManifest, 'sourceSha' | 'packageVersions'>;
  workspacePath: string;
  /** In manifest order. */
  files: readonly StagedOutputFile[];
  workspace: StagedWorkspaceObservation;
}

export interface StagedOverlay {
  source: string;
  destination: string;
  mode: number;
}

interface PendingOutputFile extends CollectedOutputFile {
  source: string;
}

const parseManifest = typia.json.createAssertParse<CollectedOutputsManifest>();
const assertExactManifest = typia.createAssertEquals<CollectedOutputsManifest>();
const GLOB_MAGIC = /[*?{[]/;
const GIT_SHA = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/i;

export async function collectNxOutputs(
  root: string,
  destination: string,
  runs: NxTargetRun[],
  sourceSha: string,
): Promise<CollectedOutputsManifest> {
  assertGitSha(sourceSha, 'Collected output source SHA');
  const pending = await inspectNxOutputs(root, runs);

  await requireEmptyDestination(destination);
  const workspace = resolve(destination, 'workspace');
  try {
    await mkdir(workspace);
  } catch (error) {
    throw new Error(`Unable to create output workspace ${workspace}: ${describeError(error)}`, { cause: error });
  }
  for (const file of pending) {
    const staged = await prepareSafeOutputPath(workspace, file.path, 'staged output file');
    try {
      await copyFile(file.source, staged);
    } catch (error) {
      throw new Error(`Unable to stage output ${file.path} at ${staged}: ${describeError(error)}`, { cause: error });
    }
    const stagedHash = await checksumFile(staged, 'Unable to checksum staged output');
    if (stagedHash !== file.sha256) {
      throw new Error(
        `SHA-256 mismatch for staged output ${file.path}: expected ${file.sha256}, received ${stagedHash}.`,
      );
    }
  }

  const manifest: CollectedOutputsManifest = {
    version: 3,
    sourceSha,
    packageVersions: Object.fromEntries(listPublishablePackages(root).map((pkg) => [pkg.name, pkg.version])),
    files: pending.map(({ source: _source, ...file }) => file),
  };
  const manifestPath = resolve(destination, 'manifest.json');
  try {
    await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  } catch (error) {
    throw new Error(
      `Unable to write collected output manifest ${manifestPath} for source SHA ${sourceSha}: ${describeError(error)}`,
      { cause: error },
    );
  }
  return manifest;
}

/** Every refusal is a value; only the shell decides whether it becomes a thrown Error. */
function refuse(reason: string, cause?: unknown): { readonly ok: false } & Refusal {
  return { ok: false, reason, cause };
}

async function inspectNxOutputs(root: string, runs: NxTargetRun[]): Promise<PendingOutputFile[]> {
  const claimants: OutputClaimant[] = [];
  for (const run of runs) {
    for (const project of run.projects) {
      const outputs: DeclaredOutputMatch[] = [];
      for (const declaredOutput of project.targetOutputs?.get(run.target) ?? []) {
        const output = resolveDeclaredOutput(declaredOutput, project);
        outputs.push({ declaredOutput, files: await observeOutputFiles(root, output) });
      }
      claimants.push({ project, target: run.target, outputs });
    }
  }
  const plan = planOutputClaims(claimants);
  if (!plan.ok) {
    throw new Error(plan.reason);
  }
  const pending: PendingOutputFile[] = [];
  for (const claim of plan.value) {
    const source = resolveWorkspacePath(root, claim.path, 'output file');
    pending.push({ ...claim, sha256: await checksumFile(source, 'Unable to checksum output file'), source });
  }
  return pending;
}

/**
 * Decides which target claims each matched file. Pure over observed facts: the shell gathers `OutputFileFact`s and
 * this function alone decides empty, missing, colliding and non-regular outputs.
 */
export function planOutputClaims(claimants: readonly OutputClaimant[]): Planned<readonly OutputClaim[]> {
  const claimedBy = new Map<string, { project: string; target: string }>();
  const claims: OutputClaim[] = [];
  for (const { project, target, outputs } of claimants) {
    if (outputs.length === 0) {
      return refuse(`Nx target ${project.project}:${target} declares no outputs.`);
    }
    for (const { declaredOutput, files } of outputs) {
      if (files.length === 0) {
        return refuse(`Declared output ${declaredOutput} for ${project.project}:${target} is missing.`);
      }
      for (const file of files) {
        const claimed = claimedBy.get(file.path);
        if (claimed) {
          // A dependency's output reached again through its aggregate is one artifact, not a second producer.
          if (
            claimed.project === project.project &&
            targetTransitivelyDependsOn(project, target, claimed.target, new Set())
          ) {
            continue;
          }
          return refuse(`Output collision: ${file.path} is declared by more than one project target or output.`);
        }
        claimedBy.set(file.path, { project: project.project, target });
        if (file.kind !== 'file') {
          return refuse(`Output file must be a regular file: ${file.path}`);
        }
        claims.push({
          project: project.project,
          target,
          output: declaredOutput,
          path: file.path,
          size: file.size,
          mode: file.mode,
        });
      }
    }
  }
  claims.sort((left, right) => left.path.localeCompare(right.path));
  return { ok: true, value: claims };
}

async function observeOutputFiles(root: string, output: string): Promise<OutputFileFact[]> {
  const facts: OutputFileFact[] = [];
  for (const path of await filesMatchingOutput(root, output)) {
    const source = resolveWorkspacePath(root, path, 'output file');
    let stat: Stats;
    try {
      stat = await lstat(source);
    } catch (error) {
      throw new Error(`Output file is missing: ${path}: ${describeError(error)}`, { cause: error });
    }
    facts.push({ path, kind: fileKind(stat), size: stat.size, mode: stat.mode & 0o7777 });
  }
  return facts;
}

function fileKind(stat: Stats): OutputFileFact['kind'] {
  if (stat.isSymbolicLink()) {
    return 'symlink';
  }
  return stat.isFile() ? 'file' : 'other';
}

export async function applyCollectedOutputs(
  root: string,
  directories: string[],
  expectedSourceSha: string,
  projects?: ProjectTargets[],
): Promise<void> {
  if (directories.length === 0) {
    throw new Error('At least one collected output directory is required.');
  }
  assertGitSha(expectedSourceSha, 'Expected source SHA');
  const resolvedProjects = projects ?? (await readProjectTargets(root));

  const trees: CollectedTree[] = [];
  for (const directory of directories) {
    const { manifest, manifestPath } = await readCollectedOutputsManifest(directory);
    trees.push(await observeCollectedTree(resolve(directory, 'workspace'), manifestPath, manifest));
  }
  const plan = planOverlays(trees, expectedSourceSha, resolvedProjects, listPublishablePackages(root));
  if (!plan.ok) {
    throw new Error(plan.reason, { cause: plan.cause });
  }

  for (const overlay of plan.value) {
    const destination = await prepareSafeOutputPath(root, overlay.destination, 'workspace output file');
    try {
      await copyFile(overlay.source, destination);
      chmodSync(destination, overlay.mode);
    } catch (error) {
      throw new Error(
        `Unable to apply staged output ${overlay.destination} at ${destination}: ${describeError(error)}`,
        { cause: error },
      );
    }
  }
}

/**
 * Gathers what one collected tree holds on disk. Staged bytes are hashed before planning so the planner can judge
 * them: a refused tree pays that hashing, and a valid tree costs what it did before.
 */
async function observeCollectedTree(
  workspace: string,
  manifestPath: string,
  manifest: CollectedOutputsManifest,
): Promise<CollectedTree> {
  const files: StagedOutputFile[] = [];
  for (const file of manifest.files) {
    const path = validateWorkspaceRelativePath(file.path, 'manifest file path');
    const source = resolveWorkspacePath(workspace, path, 'staged output file');
    files.push({ entry: { ...file, path }, source, observed: await observeOutputFile(source, 'staged') });
  }
  return {
    manifestPath,
    manifest: { sourceSha: manifest.sourceSha, packageVersions: manifest.packageVersions },
    workspacePath: workspace,
    files,
    workspace: await observeStagedWorkspace(workspace),
  };
}

async function observeOutputFile(source: string, stage: OutputStage['stage']): Promise<StagedObservation> {
  let stat: Stats;
  try {
    stat = await lstat(source);
  } catch (error) {
    return { kind: 'missing', cause: error };
  }
  if (!stat.isFile() || stat.isSymbolicLink()) {
    return { kind: 'not-regular' };
  }
  const sha256 = await checksumFile(source, `Unable to checksum ${stage} output`);
  return { kind: 'regular', size: stat.size, sha256 };
}

async function observeStagedWorkspace(workspace: string): Promise<StagedWorkspaceObservation> {
  let stat: Stats;
  try {
    stat = await lstat(workspace);
  } catch (error) {
    return isMissingPathError(error) ? { kind: 'absent', cause: error } : { kind: 'unreadable', cause: error };
  }
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    return { kind: 'not-directory' };
  }
  const regularFiles = (await listRegularFiles(workspace)).map((file) =>
    workspaceRelativePath(workspace, file, 'staged output file'),
  );
  return { kind: 'directory', regularFiles };
}

/**
 * Which boundary an output file is judged at. Only `applied` carries a manifest path, the promise a refusal names.
 * The stage changes the wording of a refusal, never the decision.
 */
export type OutputStage = { readonly stage: 'staged' } | { readonly stage: 'applied'; readonly manifestPath: string };

/**
 * The one judgement of an observed output file against the manifest entry that promised it. Staging and the
 * post-apply check both call it, so the two boundaries cannot disagree about which bytes the manifest recorded.
 * Returns the refusal, or `undefined` when the file matches.
 */
export function judgeOutputFile(
  stage: OutputStage,
  path: string,
  expected: Pick<CollectedOutputFile, 'size' | 'sha256'>,
  observed: StagedObservation,
): Refusal | undefined {
  const label = stage.stage === 'staged' ? 'Staged output' : 'Applied output';
  if (observed.kind === 'missing') {
    return { reason: `${label} file is missing: ${path}: ${describeError(observed.cause)}`, cause: observed.cause };
  }
  if (observed.kind === 'not-regular') {
    return { reason: `${label} must be a regular file: ${path}` };
  }
  if (observed.size !== expected.size) {
    return {
      reason: `Size mismatch for ${stage.stage} output ${path}: expected ${expected.size}, received ${observed.size}.`,
    };
  }
  if (observed.sha256 !== expected.sha256) {
    const from = stage.stage === 'applied' ? ` from ${stage.manifestPath}` : '';
    return {
      reason: `SHA-256 mismatch for ${stage.stage} output ${path}${from}: expected ${expected.sha256}, received ${observed.sha256}.`,
    };
  }
  return undefined;
}

/**
 * Decides which staged files may overlay the workspace. Trees are judged in order and each path is claimed by one
 * tree only, so the first refusal is the first in manifest order. Pure: every fact arrives through `trees`.
 */
export function planOverlays(
  trees: readonly CollectedTree[],
  expectedSourceSha: string,
  projects: readonly ProjectTargets[],
  packages: readonly Pick<PackageInfo, 'name' | 'version'>[],
): Planned<readonly StagedOverlay[]> {
  const projectsByName = new Map(projects.map((project) => [project.project, project]));
  const overlays: StagedOverlay[] = [];
  const claimedPaths = new Set<string>();
  for (const tree of trees) {
    const { manifestPath, manifest, workspacePath, files, workspace } = tree;
    if (manifest.sourceSha !== expectedSourceSha) {
      return refuse(
        `Source SHA mismatch in ${manifestPath}: expected ${expectedSourceSha}, received ${manifest.sourceSha}.`,
      );
    }
    const packageMismatch = findPackageVersionMismatch(packages, manifest, manifestPath);
    if (packageMismatch !== undefined) {
      return refuse(packageMismatch);
    }

    const declaredPaths = new Set<string>();
    for (const { entry: file, source, observed } of files) {
      const path = file.path;
      if (file.project === '' || GLOB_MAGIC.test(file.project)) {
        return refuse(`Manifest project in ${manifestPath} must be an exact Nx name: ${file.project}`);
      }
      if (file.target === '' || GLOB_MAGIC.test(file.target)) {
        return refuse(`Manifest target in ${manifestPath} must be an exact Nx name: ${file.target}`);
      }
      const project = projectsByName.get(file.project);
      if (!project) {
        return refuse(`Manifest ${manifestPath} references unknown Nx project ${file.project} for ${path}.`);
      }
      if (!project.targets.includes(file.target)) {
        return refuse(
          `Manifest ${manifestPath} references unknown Nx target ${file.project}:${file.target} for ${path}.`,
        );
      }
      const declaredOutputs = project.targetOutputs?.get(file.target) ?? [];
      if (!declaredOutputs.includes(file.output)) {
        return refuse(
          `Manifest output ${file.output} for ${path} is not declared by Nx target ${file.project}:${file.target}.`,
        );
      }
      const output = resolveDeclaredOutput(file.output, project);
      if (!outputContainsPath(output, path)) {
        return refuse(`Manifest file ${path} is not contained by declared output ${file.output}.`);
      }
      if (declaredPaths.has(path)) {
        return refuse(`Manifest ${manifestPath} contains a duplicate output file: ${path}`);
      }
      if (claimedPaths.has(path)) {
        return refuse(`Output collision across collected trees in ${manifestPath}: ${path}`);
      }
      declaredPaths.add(path);
      claimedPaths.add(path);

      const refusal = judgeOutputFile({ stage: 'staged' }, path, file, observed);
      if (refusal !== undefined) {
        return refuse(refusal.reason, refusal.cause);
      }
      overlays.push({ source, destination: path, mode: file.mode });
    }

    if (workspace.kind === 'absent' && files.length === 0) {
      // Artifact transports omit empty directories, so an empty manifest is the complete transferable contract.
      continue;
    }
    if (workspace.kind === 'absent' || workspace.kind === 'unreadable') {
      return refuse(
        `Collected output workspace is missing: ${workspacePath}: ${describeError(workspace.cause)}`,
        workspace.cause,
      );
    }
    if (workspace.kind === 'not-directory') {
      return refuse(`Collected output workspace must be a real directory: ${workspacePath}`);
    }
    for (const path of workspace.regularFiles) {
      if (!declaredPaths.has(path)) {
        return refuse(`Undeclared staged output file in ${manifestPath}: ${path}`);
      }
    }
  }
  return { ok: true, value: overlays };
}

export async function assertCollectedOutputsApplied(
  root: string,
  directories: string[],
  projectNames: string[],
): Promise<void> {
  if (directories.length === 0) {
    throw new Error('At least one collected output directory is required.');
  }
  const requestedProjects = new Set(projectNames);
  const foundProjects = new Set<string>();
  const claimedPaths = new Set<string>();
  const verifiedParents = new Set<string>();
  const packages = listPublishablePackages(root);
  for (const directory of directories) {
    const { manifest, manifestPath } = await readCollectedOutputsManifest(directory);
    const packageMismatch = findPackageVersionMismatch(packages, manifest, manifestPath);
    if (packageMismatch !== undefined) {
      throw new Error(packageMismatch);
    }
    for (const file of manifest.files) {
      const path = validateWorkspaceRelativePath(file.path, 'manifest file path');
      claimedPaths.add(path);
      if (!requestedProjects.has(file.project)) {
        continue;
      }
      foundProjects.add(file.project);
      verifiedParents.add(dirname(path));
      const destination = resolveWorkspacePath(root, path, 'workspace output file');
      const observed = await observeOutputFile(destination, 'applied');
      const refusal = judgeOutputFile({ stage: 'applied', manifestPath }, path, file, observed);
      if (refusal !== undefined) {
        throw new Error(refusal.reason, { cause: refusal.cause });
      }
    }
  }
  for (const project of requestedProjects) {
    if (!foundProjects.has(project)) {
      throw new Error(`Collected outputs contain no verified files for Nx project ${project}.`);
    }
  }
  for (const parent of verifiedParents) {
    if (!parent || parent === '.') {
      continue;
    }
    for (const file of await listRegularFiles(resolveWorkspacePath(root, parent, 'workspace output directory'))) {
      const path = workspaceRelativePath(root, file, 'workspace output file');
      if (!claimedPaths.has(path)) {
        throw new Error(`Undeclared workspace output file at merge boundary: ${path}`);
      }
    }
  }
}

/**
 * Returns the first way a collected tree's package versions disagree with the workspace's publishable set, or
 * `undefined`. Returned rather than thrown so the planner can refuse with it as a value.
 */
function findPackageVersionMismatch(
  packages: readonly Pick<PackageInfo, 'name' | 'version'>[],
  manifest: Pick<CollectedOutputsManifest, 'packageVersions'>,
  manifestPath: string,
): string | undefined {
  for (const pkg of packages) {
    if (manifest.packageVersions[pkg.name] !== pkg.version) {
      return `Package version mismatch in ${manifestPath} for ${pkg.name}: expected ${pkg.version}, received ${manifest.packageVersions[pkg.name] ?? 'missing'}.`;
    }
  }
  if (Object.keys(manifest.packageVersions).length !== packages.length) {
    return `Publishable package set mismatch in ${manifestPath}.`;
  }
  return undefined;
}

async function readCollectedOutputsManifest(
  directory: string,
): Promise<{ manifest: CollectedOutputsManifest; manifestPath: string }> {
  const manifestPath = resolve(directory, 'manifest.json');
  let manifest: CollectedOutputsManifest;
  try {
    manifest = assertExactManifest(parseManifest(await readFile(manifestPath, 'utf8')));
  } catch (error) {
    throw new Error(`Invalid collected output manifest ${manifestPath}: ${describeError(error)}`, { cause: error });
  }
  assertGitSha(manifest.sourceSha, `Manifest source SHA in ${manifestPath}`);
  return { manifest, manifestPath };
}

export function resolveDeclaredOutput(declaredOutput: string, project: ProjectTargets): string {
  if (!project.root) {
    throw new Error(`Nx project ${project.project} is missing its resolved root.`);
  }
  const substituted = declaredOutput
    .replaceAll('{workspaceRoot}/', '')
    .replaceAll('{workspaceRoot}', '.')
    .replaceAll('{projectRoot}/', `${project.root}/`)
    .replaceAll('{projectRoot}', project.root);
  if (hasUnsupportedOutputBraces(substituted)) {
    throw new Error(`Unsupported Nx output placeholder in ${project.project}:${declaredOutput}`);
  }
  // A trailing `/` declares a directory output (nx-plugin build-state-output-policy); the path it names has none.
  return validateWorkspaceRelativePattern(substituted.replace(/\/+$/, ''), `output for ${project.project}`);
}

function hasUnsupportedOutputBraces(value: string): boolean {
  let cursor = 0;
  while (cursor < value.length) {
    const character = value.charCodeAt(cursor);
    if (character === 0x7d) {
      return true;
    }
    if (character !== 0x7b) {
      cursor += 1;
      continue;
    }

    let hasAlternative = false;
    cursor += 1;
    while (cursor < value.length && value.charCodeAt(cursor) !== 0x7d) {
      const inner = value.charCodeAt(cursor);
      if (inner === 0x7b) {
        return true;
      }
      hasAlternative ||= inner === 0x2c;
      cursor += 1;
    }
    if (cursor === value.length || !hasAlternative) {
      return true;
    }
    cursor += 1;
  }
  return false;
}

function targetTransitivelyDependsOn(
  project: ProjectTargets,
  target: string,
  candidate: string,
  visited: Set<string>,
): boolean {
  if (visited.has(target)) {
    return false;
  }
  visited.add(target);
  for (const dependency of project.targetDependencies?.get(target) ?? []) {
    if (dependency.startsWith('^')) {
      continue;
    }
    const targets = GLOB_MAGIC.test(dependency)
      ? project.targets.filter((entry) => new Bun.Glob(dependency).match(entry))
      : [dependency];
    if (
      targets.includes(candidate) ||
      targets.some((dependencyTarget) => targetTransitivelyDependsOn(project, dependencyTarget, candidate, visited))
    ) {
      return true;
    }
  }
  return false;
}

async function filesMatchingOutput(root: string, output: string): Promise<string[]> {
  if (!GLOB_MAGIC.test(output)) {
    const path = resolveWorkspacePath(root, output, 'declared output');
    try {
      await assertNoSymlinkTraversal(root, output, 'declared output');
      const stat = await lstat(path);
      if (stat.isSymbolicLink()) {
        throw new Error(`Declared output must not be a symbolic link: ${output}`);
      }
      if (stat.isFile()) {
        return [output];
      }
      if (stat.isDirectory()) {
        return (await listRegularFiles(path)).map((file) => workspaceRelativePath(root, file, 'output file')).sort();
      }
      return [];
    } catch (error) {
      if (isMissingPathError(error)) {
        return [];
      }
      throw error;
    }
  }

  const globBase = output.slice(0, output.search(GLOB_MAGIC)).replace(/\/+$/, '');
  if (globBase) {
    try {
      await assertNoSymlinkTraversal(root, globBase, 'declared output');
    } catch (error) {
      if (isMissingPathError(error)) {
        return [];
      }
      throw error;
    }
  }

  const files = new Set<string>();
  for await (const match of new Bun.Glob(output).scan({
    cwd: root,
    dot: true,
    followSymlinks: false,
    onlyFiles: false,
  })) {
    const path = resolveWorkspacePath(root, match, 'globbed output');
    await assertNoSymlinkTraversal(root, match, 'globbed output');
    const stat = await lstat(path);
    if (stat.isSymbolicLink()) {
      throw new Error(`Declared output must not contain a symbolic link: ${match}`);
    }
    if (stat.isFile()) {
      files.add(validateWorkspaceRelativePath(match, 'globbed output'));
    } else if (stat.isDirectory()) {
      for (const file of await listRegularFiles(path)) {
        files.add(workspaceRelativePath(root, file, 'output file'));
      }
    }
  }
  return [...files].sort((left, right) => left.localeCompare(right));
}

async function listRegularFiles(directory: string): Promise<string[]> {
  let entries: Dirent[];
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (error) {
    if (isMissingPathError(error)) {
      return [];
    }
    throw error;
  }
  entries.sort((left, right) => left.name.localeCompare(right.name));
  const files: string[] = [];
  for (const entry of entries) {
    const path = resolve(directory, entry.name);
    if (entry.isSymbolicLink()) {
      throw new Error(`Collected outputs must not contain symbolic links: ${path}`);
    }
    if (entry.isDirectory()) {
      files.push(...(await listRegularFiles(path)));
    } else if (entry.isFile()) {
      files.push(path);
    } else {
      throw new Error(`Collected outputs must contain only regular files: ${path}`);
    }
  }
  return files;
}

function validateWorkspaceRelativePattern(value: string, description: string): string {
  const normalized = value
    .replaceAll('\\', '/')
    .replace(/^\.\//, '')
    .replace(/\/{2,}/g, '/');
  if (!normalized || normalized === '.' || isAbsolute(normalized) || normalized.split('/').includes('..')) {
    throw new Error(`${description} escapes the workspace: ${value}`);
  }
  return normalized;
}

function validateWorkspaceRelativePath(value: string, description: string): string {
  const normalized = validateWorkspaceRelativePattern(value, description);
  if (GLOB_MAGIC.test(normalized)) {
    throw new Error(`${description} must not contain glob syntax: ${value}`);
  }
  return normalized;
}

function resolveWorkspacePath(root: string, path: string, description: string): string {
  const validated = validateWorkspaceRelativePath(path, description);
  const resolvedRoot = resolve(root);
  const resolvedPath = resolve(resolvedRoot, ...validated.split('/'));
  const fromRoot = relative(resolvedRoot, resolvedPath);
  if (fromRoot === '..' || fromRoot.startsWith(`..${sep}`) || isAbsolute(fromRoot)) {
    throw new Error(`${description} escapes the workspace: ${path}`);
  }
  return resolvedPath;
}

function workspaceRelativePath(root: string, path: string, description: string): string {
  return validateWorkspaceRelativePath(relative(resolve(root), resolve(path)).replaceAll(sep, '/'), description);
}

function outputContainsPath(output: string, path: string): boolean {
  return GLOB_MAGIC.test(output) ? new Bun.Glob(output).match(path) : path === output || path.startsWith(`${output}/`);
}

async function requireEmptyDestination(destination: string): Promise<void> {
  try {
    const stat = await lstat(destination);
    if (stat.isSymbolicLink() || !stat.isDirectory()) {
      throw new Error(`Output collection path must be a real directory: ${destination}`);
    }
    const entries = await readdir(destination);
    if (entries.length > 0) {
      throw new Error(`Output collection directory must be empty: ${destination}`);
    }
  } catch (error) {
    if (!isMissingPathError(error)) {
      throw error;
    }
    await mkdir(destination, { recursive: true });
  }
}

async function assertNoSymlinkTraversal(root: string, path: string, description: string): Promise<void> {
  const validated = validateWorkspaceRelativePath(path, description);
  let current = resolve(root);
  const rootStat = await lstat(current);
  if (rootStat.isSymbolicLink() || !rootStat.isDirectory()) {
    throw new Error(`${description} root must be a real directory: ${current}`);
  }
  for (const segment of validated.split('/')) {
    current = resolve(current, segment);
    const stat = await lstat(current);
    if (stat.isSymbolicLink()) {
      throw new Error(`${description} must not traverse a symbolic link: ${path}`);
    }
  }
}

async function prepareSafeOutputPath(root: string, path: string, description: string): Promise<string> {
  const validated = validateWorkspaceRelativePath(path, description);
  const resolvedRoot = resolve(root);
  const rootStat = await lstat(resolvedRoot);
  if (rootStat.isSymbolicLink() || !rootStat.isDirectory()) {
    throw new Error(`${description} root must be a real directory: ${resolvedRoot}`);
  }

  let current = resolvedRoot;
  const segments = validated.split('/');
  for (const [index, segment] of segments.entries()) {
    current = resolve(current, segment);
    const isDestination = index === segments.length - 1;
    try {
      const stat = await lstat(current);
      if (stat.isSymbolicLink()) {
        throw new Error(`${description} must not traverse a symbolic link: ${path}`);
      }
      if (isDestination ? !stat.isFile() : !stat.isDirectory()) {
        throw new Error(`${description} has an invalid existing path component: ${path}`);
      }
    } catch (error) {
      if (!isMissingPathError(error)) {
        throw error;
      }
      if (!isDestination) {
        await mkdir(current);
      }
    }
  }
  return current;
}

function assertGitSha(value: string, description: string): void {
  if (!GIT_SHA.test(value)) {
    throw new Error(`${description} is invalid: ${value}. Expected a 40- or 64-character hexadecimal Git SHA.`);
  }
}

function isMissingPathError(error: unknown): boolean {
  return error instanceof Error && 'code' in error && error.code === 'ENOENT';
}

function sha256File(path: string): Promise<string> {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a read of a local file to its end; no other process is on the far side, and the stream's 'error' settles it as well
  return new Promise((resolveHash, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(path);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolveHash(hash.digest('hex')));
  });
}

async function checksumFile(path: string, description: string): Promise<string> {
  try {
    return await sha256File(path);
  } catch (error) {
    throw new Error(`${description} ${path}: ${describeError(error)}`, { cause: error });
  }
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
