import { existsSync } from 'node:fs';
import { lstat, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import {
  type NxDependsOn,
  type NxProjectJson,
  type NxTargetConfig,
  parseNxProjectJsonText,
  parseStringArrayText,
} from '../lib/json.js';
import { decode, run } from '../lib/run.js';

export interface ProjectTargets {
  project: string;
  root?: string;
  targets: string[];
  buildDependsOn?: string[];
  targetDependencies?: Map<string, string[]>;
  targetExecutors?: Map<string, string>;
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

export function nxShowProjectCommand(project: string): CommandInvocation {
  return { command: 'nx', args: ['show', 'project', project, '--json'] };
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

export function buildDependsOnFromNxProjectJson(value: NxProjectJson | null | undefined): string[] | undefined {
  return targetDependenciesFromNxProjectJson(value).get('build');
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
      values.set(
        targetName,
        entries.filter((entry): entry is string => typeof entry === 'string'),
      );
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

export function projectNamesFromNxShowProjectsOutput(output: string): string[] {
  const trimmed = output.trim();
  if (!trimmed) {
    return [];
  }

  if (trimmed.startsWith('[')) {
    try {
      const parsed = parseStringArrayText(trimmed);
      if (parsed) {
        return parsed.sort((a, b) => a.localeCompare(b));
      }
    } catch {
      // Fall through to legacy newline parsing for older Nx output shapes.
    }
  }

  return trimmed
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
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

export async function readProjectTargets(root: string): Promise<ProjectTargets[]> {
  const projects = await readNxProjectNames(root);
  return Promise.all(projects.map((project) => readProjectTarget(root, project)));
}

async function readNxProjectNames(root: string): Promise<string[]> {
  const result = await $`nx show projects`.cwd(root).quiet();
  return projectNamesFromNxShowProjectsOutput(decode(result.stdout));
}

async function readProjectTarget(root: string, project: string): Promise<ProjectTargets> {
  const command = nxShowProjectCommand(project);
  const result = await $`${command.command} ${command.args}`.cwd(root).quiet();
  const parsed = parseNxProjectJsonText(decode(result.stdout));
  if (!parsed) {
    throw new Error(`Unable to inspect Nx project ${project}.`);
  }
  const targetDependencies = targetDependenciesFromNxProjectJson(parsed);
  return {
    project,
    root: projectRootFromNxProjectJson(parsed),
    targets: targetNamesFromNxProjectJson(parsed),
    buildDependsOn: targetDependencies.get('build'),
    targetDependencies,
    targetExecutors: targetExecutorsFromNxProjectJson(parsed),
    targetOutputs: targetOutputsFromNxProjectJson(parsed),
    targetScripts: targetScriptsFromNxProjectJson(parsed),
  };
}

export type { NxProjectJson, NxTargetConfig };
