import { existsSync } from 'node:fs';
import { lstat, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import { isRecord, recordProperty } from '../lib/json.js';
import { decode, run } from '../lib/run.js';

export interface ProjectTargets {
  project: string;
  targets: string[];
  buildDependsOn?: string[];
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

export function buildDependsOnFromNxProjectJson(value: unknown): string[] | undefined {
  const targets = isRecord(value) ? recordProperty(value, 'targets') : null;
  const build = targets ? recordProperty(targets, 'build') : null;
  if (!Array.isArray(build?.dependsOn) || !build.dependsOn.every((entry) => typeof entry === 'string')) {
    return undefined;
  }
  return build.dependsOn;
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

export async function readProjectTargets(root: string): Promise<ProjectTargets[]> {
  const projects = await readNxProjectNames(root);
  return Promise.all(projects.map((project) => readProjectTarget(root, project)));
}

async function readNxProjectNames(root: string): Promise<string[]> {
  const result = await $`nx show projects`.cwd(root).quiet();
  return decode(result.stdout)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

async function readProjectTarget(root: string, project: string): Promise<ProjectTargets> {
  const command = nxShowProjectCommand(project);
  const result = await $`${command.command} ${command.args}`.cwd(root).quiet();
  const parsed: unknown = JSON.parse(decode(result.stdout));
  return {
    project,
    targets: targetNamesFromNxProjectJson(parsed),
    buildDependsOn: buildDependsOnFromNxProjectJson(parsed),
  };
}
