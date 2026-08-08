import { existsSync } from 'node:fs';
import { lstat, rm } from 'node:fs/promises';
import { join } from 'node:path';
// Nx package exports are CLI-oriented; this is the module the CLI uses for graph + daemon IPC.
import { createProjectGraphAsync } from 'nx/src/project-graph/project-graph.js';
import { workspaceRoot as primaryWorkspaceRoot } from 'nx/src/utils/workspace-root.js';
import type { NxDependsOn, NxProjectJson, NxTargetConfig, NxTargetOptions } from '../lib/json.js';
import { run, runResult } from '../lib/run.js';

export interface ProjectTargets {
  project: string;
  root?: string;
  targets: string[];
  buildDependsOn?: string[];
  targetDependencies?: Map<string, string[]>;
  targetExecutors?: Map<string, string>;
  targetOptions?: Map<string, NxTargetOptions>;
  targetOutputs?: Map<string, string[]>;
  targetScripts?: Map<string, string>;
}

export interface CommandInvocation {
  command: string;
  args: string[];
}

export function nxResetCommand(): CommandInvocation {
  return { command: 'nx', args: ['reset'] };
}

export function nxCacheDirectories(root: string): string[] {
  return [join(root, '.nx/cache'), join(root, '.nx/workspace-data'), join(root, 'node_modules/.cache/nx')];
}

export function targetNamesFromNxProjectJson(value: NxProjectJson | null | undefined): string[] {
  const targets = value?.targets;
  return targets ? Object.keys(targets).sort((a, b) => a.localeCompare(b)) : [];
}

export function projectRootFromNxProjectJson(value: NxProjectJson | null | undefined): string | undefined {
  return typeof value?.root === 'string' ? value.root : undefined;
}

export function targetDependenciesFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, string[]> {
  const targets = value?.targets;
  const dependencies = new Map<string, string[]>();
  if (!targets) {
    return dependencies;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (!target.dependsOn) {
      continue;
    }
    const entries: string[] = [];
    for (const dependency of target.dependsOn) {
      const local = localDependsOnTarget(dependency);
      if (local === undefined) {
        if (isInvalidDependsOn(dependency)) {
          throw new Error(`Nx target ${targetName} has an invalid dependsOn entry.`);
        }
        continue;
      }
      entries.push(local);
    }
    dependencies.set(targetName, entries);
  }
  return dependencies;
}

function localDependsOnTarget(dependency: NxDependsOn): string | undefined {
  if (typeof dependency === 'string') {
    return dependency;
  }
  if (typeof dependency.target !== 'string') {
    return undefined;
  }
  if (dependency.projects !== undefined && dependency.projects !== 'self') {
    // Target closures are intentionally project-local. Nx schedules explicit
    // cross-project prerequisites itself; treating them as local targets
    // would collect or verify outputs from the wrong project.
    return undefined;
  }
  return dependency.target;
}

function isInvalidDependsOn(dependency: NxDependsOn): boolean {
  return typeof dependency !== 'string' && typeof dependency.target !== 'string';
}

export function targetExecutorsFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, string> {
  const targets = value?.targets;
  const executors = new Map<string, string>();
  if (!targets) {
    return executors;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (typeof target.executor === 'string') {
      executors.set(targetName, target.executor);
    }
  }
  return executors;
}

export function targetOptionsFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, NxTargetOptions> {
  const targets = value?.targets;
  const options = new Map<string, NxTargetOptions>();
  if (!targets) {
    return options;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (target.options) {
      options.set(targetName, target.options);
    }
  }
  return options;
}

export function targetOutputsFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, string[]> {
  return targetStringArraysFromNxProjectJson(value, 'outputs');
}

export function targetScriptsFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, string> {
  const targets = value?.targets;
  const scripts = new Map<string, string>();
  if (!targets) {
    return scripts;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (typeof target.options?.script === 'string') {
      scripts.set(targetName, target.options.script);
    }
  }
  return scripts;
}

function targetStringArraysFromNxProjectJson(
  value: NxProjectJson | null | undefined,
  property: 'outputs' | 'inputs',
): Map<string, string[]> {
  const targets = value?.targets;
  const values = new Map<string, string[]>();
  if (!targets) {
    return values;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    const entries = target[property];
    if (Array.isArray(entries)) {
      const strings: string[] = [];
      for (const entry of entries) {
        if (typeof entry === 'string') {
          strings.push(entry);
        }
      }
      values.set(targetName, strings);
    }
  }
  return values;
}

export function formatProjectTargetLines(projects: ProjectTargets[]): string {
  return projects
    .flatMap((project) => project.targets.map((target) => `${project.project}:${target}`))
    .sort((a, b) => a.localeCompare(b))
    .join('\n');
}

export function projectNamesWithTarget(projects: ProjectTargets[], target: string): string[] {
  return projects
    .filter((project) => project.targets.includes(target))
    .map((project) => project.project)
    .sort((a, b) => a.localeCompare(b));
}

export async function listTargets(root: string): Promise<void> {
  const output = formatProjectTargetLines(await readProjectTargets(root));
  if (output) {
    console.log(output);
  }
}

export async function listProjects(root: string, options: { withTarget?: string }): Promise<void> {
  if (!options.withTarget) {
    throw new Error('smoo nx list-projects requires --with-target <target>');
  }
  const projects = projectNamesWithTarget(await readProjectTargets(root), options.withTarget).join('\n');
  if (projects) {
    console.log(projects);
  }
}

export async function resetCache(root: string): Promise<void> {
  const command = nxResetCommand();
  await run(command.command, command.args, root);
}

export async function cleanCache(root: string): Promise<void> {
  for (const path of nxCacheDirectories(root)) {
    if (!existsSync(path)) {
      continue;
    }
    const stat = await lstat(path);
    if (!stat.isDirectory() && !stat.isSymbolicLink()) {
      console.warn(`Skipping non-directory Nx cache path: ${path}`);
      continue;
    }
    await rm(path, { recursive: true, force: true });
    console.log(`Removed ${path}`);
  }
}

export type NxProjects = Readonly<Record<string, NxProjectJson>>;

/**
 * Load the resolved project graph through Nx's API. The current workspace uses the
 * daemon in-process; foreign roots use an isolated process because Nx snapshots its
 * workspace cache paths when the module loads.
 */
let graphLoadTail: Promise<void> = Promise.resolve();

export function loadNxProjects(root: string): Promise<NxProjects> {
  const result = graphLoadTail.then(() => loadNxProjectsNow(root));
  graphLoadTail = result.then(
    () => undefined,
    () => undefined,
  );
  return result;
}

async function loadNxProjectsNow(root: string): Promise<NxProjects> {
  if (root !== primaryWorkspaceRoot) {
    return loadForeignNxProjects(root);
  }
  const graph = await createProjectGraphAsync({ exitOnError: false });
  return nxProjectsFromNodes(graph.nodes);
}

const FOREIGN_GRAPH_SCRIPT = `
import('nx/src/project-graph/project-graph.js')
  .then(async ({ createProjectGraphAsync }) => {
    const graph = await createProjectGraphAsync({ exitOnError: false });
    const projects = Object.fromEntries(
      Object.entries(graph.nodes).map(([name, node]) => [
        name,
        { ...node.data, name: node.data?.name ?? name, targets: node.data?.targets ?? {} },
      ]),
    );
    process.stdout.write(JSON.stringify(projects));
  })
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
`;

async function loadForeignNxProjects(root: string): Promise<NxProjects> {
  const result = await runResult(process.execPath, ['--eval', FOREIGN_GRAPH_SCRIPT], root, {
    NX_DAEMON: 'false',
    NX_ISOLATE_PLUGINS: 'false',
    NX_WORKSPACE_ROOT_PATH: root,
  });
  if (result.exitCode !== 0) {
    throw new Error(`Failed to load Nx project graph for ${root}: ${result.stderr.trim()}`);
  }
  const parsed: unknown = JSON.parse(result.stdout);
  if (!isNxProjects(parsed)) {
    throw new Error(`Nx returned an invalid project graph for ${root}`);
  }
  return parsed;
}

function nxProjectsFromNodes(nodes: Readonly<Record<string, { data: NxProjectJson }>>): NxProjects {
  const projects: Record<string, NxProjectJson> = {};
  for (const [name, node] of Object.entries(nodes)) {
    projects[name] = {
      ...node.data,
      name: node.data.name ?? name,
      targets: node.data.targets ?? {},
    };
  }
  return projects;
}

function isNxProjects(value: unknown): value is NxProjects {
  if (!isRecord(value)) return false;
  return Object.values(value).every(
    (project) =>
      isRecord(project) &&
      (project.name === undefined || typeof project.name === 'string') &&
      (project.root === undefined || typeof project.root === 'string') &&
      (project.targets === undefined || (isRecord(project.targets) && Object.values(project.targets).every(isRecord))),
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function targetNamesFromProjects(projects: NxProjects): string[] {
  const names = new Set<string>();
  for (const project of Object.values(projects)) {
    for (const targetName of targetNamesFromNxProjectJson(project)) {
      names.add(targetName);
    }
  }
  return [...names];
}

export function projectTargetsFromNxProjects(projects: NxProjects): ProjectTargets[] {
  return Object.entries(projects)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([project, metadata]) => {
      const targetDependencies = targetDependenciesFromNxProjectJson(metadata);
      return {
        project,
        root: projectRootFromNxProjectJson(metadata),
        targets: targetNamesFromNxProjectJson(metadata),
        buildDependsOn: targetDependencies.get('build'),
        targetDependencies,
        targetExecutors: targetExecutorsFromNxProjectJson(metadata),
        targetOptions: targetOptionsFromNxProjectJson(metadata),
        targetOutputs: targetOutputsFromNxProjectJson(metadata),
        targetScripts: targetScriptsFromNxProjectJson(metadata),
      };
    });
}

export async function readProjectTargets(root: string): Promise<ProjectTargets[]> {
  return projectTargetsFromNxProjects(await loadNxProjects(root));
}

export type { NxProjectJson, NxTargetConfig };
