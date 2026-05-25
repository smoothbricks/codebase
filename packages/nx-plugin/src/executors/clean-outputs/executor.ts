import { execFileSync } from 'node:child_process';
import { rm } from 'node:fs/promises';
import path from 'node:path';

import type { CleanOutputsOptions } from './schema.js';

interface CleanOutputsContext {
  root: string;
  projectName?: string;
  projectsConfigurations?: {
    projects: Record<string, NxProject>;
  };
}

interface CleanOutputsResult {
  success: boolean;
}

interface NxProject {
  root?: string;
  targets?: Record<string, { outputs?: string[] }>;
}

export default async function cleanOutputsExecutor(
  options: CleanOutputsOptions,
  context: CleanOutputsContext,
): Promise<CleanOutputsResult> {
  const projectName = context.projectName;
  if (!projectName) {
    throw new Error('@smoothbricks/nx-plugin:clean-outputs requires a project context.');
  }

  const workspaceRoot = context.root;
  const project = context.projectsConfigurations?.projects[projectName];
  if (!project) {
    throw new Error(`Project ${projectName} was not found in the Nx project graph.`);
  }

  const outputDirs = resolveOutputDirs({
    outputs: options.outputs ?? project.targets?.build?.outputs ?? ['{projectRoot}/dist'],
    projectName,
    projectRoot: project.root ?? '.',
    workspaceRoot,
  });

  for (const outputDir of outputDirs) {
    assertSafeOutputDir(outputDir, project.root ?? '.', workspaceRoot);
    if (containsTrackedFiles(outputDir, workspaceRoot)) {
      console.log(`skipped ${path.relative(workspaceRoot, outputDir)} (contains tracked files)`);
      continue;
    }
    await rm(outputDir, { force: true, recursive: true });
    console.log(`removed ${path.relative(workspaceRoot, outputDir)}`);
  }

  return { success: true };
}

export function resolveOutputDirs(options: {
  outputs: string[];
  projectName: string;
  projectRoot: string;
  workspaceRoot: string;
}): string[] {
  const concreteOutputDirs = options.outputs.map((output) => outputDir(output, options));
  const dedupedOutputDirs = [...new Set(concreteOutputDirs)].sort((a, b) => a.length - b.length);
  return dedupedOutputDirs.filter(
    (candidate, index) =>
      !dedupedOutputDirs.some((other, otherIndex) => otherIndex < index && isWithin(candidate, other)),
  );
}

function outputDir(
  output: string,
  options: { projectName: string; projectRoot: string; workspaceRoot: string },
): string {
  const interpolated = output
    .replaceAll('{workspaceRoot}', '.')
    .replaceAll('{projectRoot}', options.projectRoot)
    .replaceAll('{projectName}', options.projectName);

  const segments = interpolated.split(/[\\/]/);
  const globIndex = segments.findIndex((segment) => segment.includes('*'));
  const dirSegments = globIndex === -1 ? segments : segments.slice(0, globIndex);
  const outputPath = dirSegments.join(path.sep);

  return path.resolve(options.workspaceRoot, outputPath);
}

function assertSafeOutputDir(outputDir: string, projectRoot: string, workspaceRoot: string): void {
  const absoluteWorkspaceRoot = path.resolve(workspaceRoot);
  const absoluteProjectRoot = path.resolve(workspaceRoot, projectRoot);

  if (!isWithin(outputDir, absoluteWorkspaceRoot)) {
    throw new Error(`Refusing to remove output outside workspace: ${outputDir}`);
  }

  if (outputDir === absoluteWorkspaceRoot || outputDir === absoluteProjectRoot) {
    throw new Error(`Refusing to remove unsafe output directory: ${path.relative(workspaceRoot, outputDir) || '.'}`);
  }
}

function isWithin(candidate: string, parent: string): boolean {
  const relative = path.relative(parent, candidate);
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function containsTrackedFiles(outputDir: string, workspaceRoot: string): boolean {
  const relativeOutputDir = path.relative(workspaceRoot, outputDir);
  const trackedFiles = execFileSync('git', ['ls-files', '--', relativeOutputDir], {
    cwd: workspaceRoot,
    encoding: 'utf8',
  });

  return trackedFiles.trim().length > 0;
}
