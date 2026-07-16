import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { copyFile, lstat, mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { isAbsolute, relative, resolve, sep } from 'node:path';
import { isRecord } from '@smoothbricks/validation';
import typia from 'typia';
import type { ProjectTargets } from '../nx/index.js';

export interface CollectedOutputFile {
  project: string;
  target: string;
  output: string;
  path: string;
  size: number;
  sha256: string;
}

export interface CollectedOutputsManifest {
  version: 1;
  sourceSha: string;
  files: CollectedOutputFile[];
}

export interface NxTargetRun {
  target: string;
  projects: ProjectTargets[];
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

  const pending: PendingOutputFile[] = [];
  const claimedPaths = new Set<string>();
  for (const run of runs) {
    for (const project of run.projects) {
      const outputs = project.targetOutputs?.get(run.target);
      if (!outputs || outputs.length === 0) {
        throw new Error(`Nx target ${project.project}:${run.target} declares no outputs.`);
      }
      for (const declaredOutput of outputs) {
        const output = resolveDeclaredOutput(declaredOutput, project);
        const matchedFiles = await filesMatchingOutput(root, output);
        if (matchedFiles.length === 0) {
          throw new Error(`Declared output ${declaredOutput} for ${project.project}:${run.target} is missing.`);
        }
        for (const path of matchedFiles) {
          if (claimedPaths.has(path)) {
            throw new Error(`Output collision: ${path} is declared by more than one project target or output.`);
          }
          claimedPaths.add(path);
          const source = resolveWorkspacePath(root, path, 'output file');
          const stat = await lstat(source);
          if (!stat.isFile() || stat.isSymbolicLink()) {
            throw new Error(`Output file must be a regular file: ${path}`);
          }
          pending.push({
            project: project.project,
            target: run.target,
            output,
            path,
            size: stat.size,
            sha256: await sha256File(source),
            source,
          });
        }
      }
    }
  }

  pending.sort((left, right) => left.path.localeCompare(right.path));
  await requireEmptyDestination(destination);
  const workspace = resolve(destination, 'workspace');
  await mkdir(workspace);
  for (const file of pending) {
    const staged = await prepareSafeOutputPath(workspace, file.path, 'staged output file');
    await copyFile(file.source, staged);
  }

  const manifest: CollectedOutputsManifest = {
    version: 1,
    sourceSha,
    files: pending.map(({ source: _source, ...file }) => file),
  };
  await writeFile(resolve(destination, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  return manifest;
}

export async function applyCollectedOutputs(
  root: string,
  directories: string[],
  expectedSourceSha: string,
): Promise<void> {
  if (directories.length === 0) {
    throw new Error('At least one collected output directory is required.');
  }
  assertGitSha(expectedSourceSha, 'Expected source SHA');

  const overlays: Array<{ source: string; destination: string }> = [];
  const claimedPaths = new Set<string>();
  for (const directory of directories) {
    const manifestPath = resolve(directory, 'manifest.json');
    let manifest: CollectedOutputsManifest;
    try {
      manifest = assertExactManifest(parseManifest(await readFile(manifestPath, 'utf8')));
    } catch (error) {
      throw new Error(`Invalid collected output manifest ${manifestPath}.`, { cause: error });
    }
    assertGitSha(manifest.sourceSha, `Manifest source SHA in ${manifestPath}`);
    if (manifest.sourceSha !== expectedSourceSha) {
      throw new Error(
        `Source SHA mismatch in ${manifestPath}: expected ${expectedSourceSha}, received ${manifest.sourceSha}.`,
      );
    }

    const workspace = resolve(directory, 'workspace');
    const declaredPaths = new Set<string>();
    for (const file of manifest.files) {
      const path = validateWorkspaceRelativePath(file.path, 'manifest file path');
      const output = validateWorkspaceRelativePattern(file.output, 'manifest output');
      if (!outputContainsPath(output, path)) {
        throw new Error(`Manifest file ${path} is not contained by declared output ${output}.`);
      }
      if (declaredPaths.has(path)) {
        throw new Error(`Manifest contains a duplicate output file: ${path}`);
      }
      if (claimedPaths.has(path)) {
        throw new Error(`Output collision across collected trees: ${path}`);
      }
      declaredPaths.add(path);
      claimedPaths.add(path);

      const source = resolveWorkspacePath(workspace, path, 'staged output file');
      let stat;
      try {
        stat = await lstat(source);
      } catch (error) {
        throw new Error(`Staged output file is missing: ${path}`, { cause: error });
      }
      if (!stat.isFile() || stat.isSymbolicLink()) {
        throw new Error(`Staged output must be a regular file: ${path}`);
      }
      if (stat.size !== file.size) {
        throw new Error(`Size mismatch for staged output ${path}: expected ${file.size}, received ${stat.size}.`);
      }
      const checksum = await sha256File(source);
      if (checksum !== file.sha256) {
        throw new Error(`SHA-256 mismatch for staged output ${path}.`);
      }
      overlays.push({ source, destination: path });
    }

    const workspaceStat = await lstat(workspace);
    if (workspaceStat.isSymbolicLink() || !workspaceStat.isDirectory()) {
      throw new Error(`Collected output workspace must be a real directory: ${workspace}`);
    }
    const stagedFiles = await listRegularFiles(workspace);
    for (const stagedFile of stagedFiles) {
      const path = workspaceRelativePath(workspace, stagedFile, 'staged output file');
      if (!declaredPaths.has(path)) {
        throw new Error(`Undeclared staged output file: ${path}`);
      }
    }
  }

  for (const overlay of overlays) {
    const destination = await prepareSafeOutputPath(root, overlay.destination, 'workspace output file');
    await copyFile(overlay.source, destination);
  }
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
  if (substituted.includes('{') || substituted.includes('}')) {
    throw new Error(`Unsupported Nx output placeholder in ${project.project}:${declaredOutput}`);
  }
  return validateWorkspaceRelativePattern(substituted, `output for ${project.project}`);
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
  let entries;
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
    throw new Error(`${description} must be a 40- or 64-character hexadecimal Git SHA.`);
  }
}

function isMissingPathError(error: unknown): boolean {
  return isRecord(error) && error.code === 'ENOENT';
}

function sha256File(path: string): Promise<string> {
  return new Promise((resolveHash, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(path);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolveHash(hash.digest('hex')));
  });
}
