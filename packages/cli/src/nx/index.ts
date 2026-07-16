import { existsSync } from 'node:fs';
import { lstat, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import { isRecord, recordProperty } from '../lib/json.js';
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

export function targetNamesFromNxProjectJson(value: unknown): string[] {
  const targets = isRecord(value) ? recordProperty(value, 'targets') : null;
  return targets ? Object.keys(targets).sort((a, b) => a.localeCompare(b)) : [];
}

export function projectRootFromNxProjectJson(value: unknown): string | undefined {
  return isRecord(value) && typeof value.root === 'string' ? value.root : undefined;
}

export function buildDependsOnFromNxProjectJson(value: unknown): string[] | undefined {
  return targetDependenciesFromNxProjectJson(value).get('build');
}

export function targetDependenciesFromNxProjectJson(value: unknown): Map<string, string[]> {
  const targets = isRecord(value) ? recordProperty(value, 'targets') : null;
  const dependencies = new Map<string, string[]>();
  if (!targets) {
    return dependencies;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (!isRecord(target) || !Array.isArray(target.dependsOn)) {
      continue;
    }
    const entries: string[] = [];
    for (const dependency of target.dependsOn) {
      if (typeof dependency === 'string') {
        entries.push(dependency);
        continue;
      }
      if (!isRecord(dependency) || typeof dependency.target !== 'string') {
        throw new Error(`Nx target ${targetName} has an invalid dependsOn entry.`);
      }
      if (dependency.projects !== undefined && dependency.projects !== 'self') {
        throw new Error(`Nx target ${targetName} uses unsupported cross-project dependsOn for ${dependency.target}.`);
      }
      entries.push(dependency.target);
    }
    dependencies.set(targetName, entries);
  }
  return dependencies;
}

export function targetExecutorsFromNxProjectJson(value: unknown): Map<string, string> {
  const targets = isRecord(value) ? recordProperty(value, 'targets') : null;
  const executors = new Map<string, string>();
  if (!targets) {
    return executors;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (isRecord(target) && typeof target.executor === 'string') {
      executors.set(targetName, target.executor);
    }
  }
  return executors;
}

export function targetOutputsFromNxProjectJson(value: unknown): Map<string, string[]> {
  return targetStringArraysFromNxProjectJson(value, 'outputs');
}

export function targetScriptsFromNxProjectJson(value: unknown): Map<string, string> {
  const targets = isRecord(value) ? recordProperty(value, 'targets') : null;
  const scripts = new Map<string, string>();
  if (!targets) {
    return scripts;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (!isRecord(target)) {
      continue;
    }
    const options = recordProperty(target, 'options');
    if (typeof options?.script === 'string') {
      scripts.set(targetName, options.script);
    }
  }
  return scripts;
}

function targetStringArraysFromNxProjectJson(value: unknown, property: string): Map<string, string[]> {
  const targets = isRecord(value) ? recordProperty(value, 'targets') : null;
  const values = new Map<string, string[]>();
  if (!targets) {
    return values;
  }
  for (const [targetName, target] of Object.entries(targets)) {
    if (!isRecord(target)) {
      continue;
    }
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
      const parsed: unknown = JSON.parse(trimmed);
      if (Array.isArray(parsed) && parsed.every((entry) => typeof entry === 'string')) {
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
  const parsed: unknown = JSON.parse(decode(result.stdout));
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
