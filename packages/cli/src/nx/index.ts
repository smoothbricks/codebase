import { join, posix } from 'node:path';
import { taskConfigurations } from '@smoothbricks/nx-plugin/build-state-output-policy';
import {
  refreshTestTimingsCommand,
  timingsCoverage,
  timingsShardedSuite,
  untimedTestFilesReport,
} from '@smoothbricks/nx-plugin/test-timings';
// Nx package exports are CLI-oriented; this is the module the CLI uses for graph + daemon IPC.
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph.js';
import type { TargetConfiguration } from 'nx/src/config/workspace-json-project-json.js';
import { createProjectGraphAsync } from 'nx/src/project-graph/project-graph.js';
import { ProcessTasks } from 'nx/src/tasks-runner/create-task-graph.js';
import { interpolate, normalizeDependencyConfigDefinition } from 'nx/src/tasks-runner/utils.js';
import { workspaceRoot as primaryWorkspaceRoot } from 'nx/src/utils/workspace-root.js';
import typia from 'typia';
import type { NxDependsOn, NxProjectJson, NxTargetConfig, NxTargetOptions } from '../lib/json.js';
import { printCommandOutput, runResult, runStatus } from '../lib/run.js';

export interface ProjectTargets {
  project: string;
  root?: string;
  targets: string[];
  /** Per-target `cache`, for policies that only govern what Nx can restore. */
  targetCache?: Map<string, boolean>;
  /** Per-target `configurations`: each named configuration's overrides of the target's `options`. */
  targetConfigurations?: Map<string, Record<string, NxTargetOptions>>;
  /** Per-target `defaultConfiguration`: the configuration Nx runs a request naming none under. */
  targetDefaultConfigurations?: Map<string, string>;
  targetDependencies?: Map<string, string[]>;
  /**
   * Per target, the targets of this project its `dependsOn` schedules, resolved by Nx's own normalization: `^` and
   * `dependencies: true` edges and other projects' targets are not listed; `project:target`, `projects: [self]` and
   * patterns over target names are.
   */
  localTargetDependencies?: Map<string, string[]>;
  targetExecutors?: Map<string, string>;
  /** Per-target inputs, including runtime definitions needed by identity policies. */
  targetInputs?: Map<string, NonNullable<NxTargetConfig['inputs']>>;
  /** Project-local named inputs override workspace definitions. */
  namedInputs?: NxProjectJson['namedInputs'];
  targetOptions?: Map<string, NxTargetOptions>;
  targetOutputs?: Map<string, string[]>;
  targetScripts?: Map<string, string>;
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
  // The object form of `^target`: the dependencies' target, never this project's.
  return dependency.dependencies === true ? `^${dependency.target}` : dependency.target;
}

/** The graph Nx's dependsOn normalization reads: each project's root, tags and targets' edges. */
function dependencyGraph(projects: NxProjects): ProjectGraph {
  const nodes: Record<string, ProjectGraphProjectNode> = {};
  for (const [name, project] of Object.entries(projects)) {
    const targets: Record<string, TargetConfiguration> = {};
    for (const [target, config] of Object.entries(project.targets ?? {})) {
      targets[target] = config.dependsOn === undefined ? {} : { dependsOn: config.dependsOn };
    }
    nodes[name] = { name, type: 'lib', data: { root: project.root ?? '', tags: project.tags ?? [], targets } };
  }
  return { nodes, dependencies: {} };
}

/**
 * Per target of `project`, the local targets Nx schedules from its `dependsOn`, through the normalization its task
 * graph uses (`tasks-runner/utils.js`, `getDependencyConfigs`). Nx rewrites the definitions it is given, so each
 * object is copied first.
 */
function localDependenciesOf(
  project: string,
  metadata: NxProjectJson,
  graph: ProjectGraph,
  allTargetNames: string[],
): Map<string, string[]> {
  const own = new Set(targetNamesFromNxProjectJson(metadata));
  const local = new Map<string, string[]>();
  for (const [target, config] of Object.entries(metadata.targets ?? {})) {
    const members = (config.dependsOn ?? []).flatMap((definition) =>
      normalizeDependencyConfigDefinition(
        typeof definition === 'string' ? definition : { ...definition },
        project,
        graph,
        allTargetNames,
      )
        .filter((normalized) => normalized.dependencies !== true && normalized.projects.includes(project))
        .map((normalized) => normalized.target)
        .filter((member) => own.has(member)),
    );
    local.set(target, [...new Set(members)]);
  }
  return local;
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

export function targetConfigurationsFromNxProjectJson(
  value: NxProjectJson | null | undefined,
): Map<string, Record<string, NxTargetOptions>> {
  const configurations = new Map<string, Record<string, NxTargetOptions>>();
  for (const [targetName, target] of Object.entries(value?.targets ?? {})) {
    if (target.configurations) {
      configurations.set(targetName, target.configurations);
    }
  }
  return configurations;
}

export function targetDefaultConfigurationsFromNxProjectJson(
  value: NxProjectJson | null | undefined,
): Map<string, string> {
  const defaults = new Map<string, string>();
  for (const [targetName, target] of Object.entries(value?.targets ?? {})) {
    if (target.defaultConfiguration) {
      defaults.set(targetName, target.defaultConfiguration);
    }
  }
  return defaults;
}

export function targetOutputsFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, string[]> {
  return targetStringArraysFromNxProjectJson(value, 'outputs');
}

export function targetInputsFromNxProjectJson(
  value: NxProjectJson | null | undefined,
): Map<string, NonNullable<NxTargetConfig['inputs']>> {
  const inputs = new Map<string, NonNullable<NxTargetConfig['inputs']>>();
  for (const [targetName, target] of Object.entries(value?.targets ?? {})) {
    if (target.inputs !== undefined) inputs.set(targetName, target.inputs);
  }
  return inputs;
}

export function targetCacheFromNxProjectJson(value: NxProjectJson | null | undefined): Map<string, boolean> {
  const cache = new Map<string, boolean>();
  for (const [targetName, target] of Object.entries(value?.targets ?? {})) {
    if (typeof target.cache === 'boolean') {
      cache.set(targetName, target.cache);
    }
  }
  return cache;
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

/**
 * Runs the project's timings-sharded Bun suite once, unsharded, with `--update-timings`, so Bun rewrites the first
 * `--timings` manifest with every file's measured duration, then proves the manifest names every file the shards run.
 */
export async function refreshTestTimings(root: string, projectName: string): Promise<void> {
  const projects = await loadNxProjects(root);
  const project = projects[projectName];
  if (project === undefined) {
    throw new Error(`No Nx project is named ${projectName}`);
  }
  const found = timingsShardedSuite(project.root ?? '.', project.targets ?? {});
  if (!found.ok) {
    throw new Error(`${projectName}: ${found.error}`);
  }
  if (found.suite === null) {
    throw new Error(`${projectName} has no bun test target with both --shard and --timings`);
  }
  const status = await runStatus('bun', ['test', ...found.suite.args, '--update-timings'], join(root, found.suite.cwd));
  if (status !== 0) {
    throw new Error(
      `bun test exited ${status}; Bun still wrote this failing run's durations, so fix the suite and run ${refreshTestTimingsCommand(projectName)} again`,
    );
  }
  const checked = await timingsCoverage(root, found.suite);
  if (!checked.ok) {
    throw new Error(checked.error);
  }
  if (checked.coverage.untimed.length > 0) {
    throw new Error(untimedTestFilesReport(projectName, found.suite.cwd, checked.coverage));
  }
  console.log(
    `${posix.join(found.suite.cwd, checked.coverage.manifests[0] ?? '')} now times all ${checked.coverage.files.length} test files of ${projectName}`,
  );
}

export type NxProjects = Readonly<Record<string, NxProjectJson>>;

/** Parse the foreign graph script's output. Invalid JSON throws; wrong shape returns null. */
const parseNxProjects = typia.json.createIsParse<NxProjects>();

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

/**
 * Nx's overrides for where one workspace keeps its daemon socket, daemon record,
 * task database and cache. This process's environment names the primary
 * workspace's; a foreign root's own Nx, started with them, would listen on the
 * primary's socket and share its database.
 */
const PRIMARY_WORKSPACE_STATE_ENV = [
  'NX_SOCKET_DIR',
  'NX_DAEMON_SOCKET_DIR',
  'NX_WORKSPACE_DATA_DIRECTORY',
  'NX_CACHE_DIRECTORY',
] as const;

async function loadForeignNxProjects(root: string): Promise<NxProjects> {
  const result = await runResult(
    process.execPath,
    ['--eval', FOREIGN_GRAPH_SCRIPT],
    root,
    { NX_ISOLATE_PLUGINS: 'false', NX_WORKSPACE_ROOT_PATH: root },
    PRIMARY_WORKSPACE_STATE_ENV,
  );
  if (result.exitCode !== 0) {
    printCommandOutput(result.stdout, result.stderr);
    throw new Error(
      `Failed to load Nx project graph for ${root}: ${process.execPath} --eval <foreign graph script> failed with exit code ${result.exitCode}`,
    );
  }
  const parsed = parseNxProjects(result.stdout);
  if (!parsed) {
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
  const graph = dependencyGraph(projects);
  const allTargetNames = targetNamesFromProjects(projects);
  return Object.entries(projects)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([project, metadata]) => {
      const targetDependencies = targetDependenciesFromNxProjectJson(metadata);
      return {
        project,
        root: projectRootFromNxProjectJson(metadata),
        targets: targetNamesFromNxProjectJson(metadata),
        targetCache: targetCacheFromNxProjectJson(metadata),
        targetConfigurations: targetConfigurationsFromNxProjectJson(metadata),
        targetDefaultConfigurations: targetDefaultConfigurationsFromNxProjectJson(metadata),
        targetDependencies,
        localTargetDependencies: localDependenciesOf(project, metadata, graph, allTargetNames),
        targetInputs: targetInputsFromNxProjectJson(metadata),
        namedInputs: metadata.namedInputs,
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

/**
 * The configuration Nx runs `target` of `project` under for a run that requests `configuration`, by Nx's own task graph
 * resolution: a request the target has no configuration for, and a request naming none, run it under its
 * `defaultConfiguration`. `undefined` runs the target under its own options alone.
 */
export function resolveTaskConfiguration(
  project: ProjectTargets,
  target: string,
  configuration: string | undefined,
): string | undefined {
  const node: ProjectGraphProjectNode = {
    name: project.project,
    type: 'lib',
    data: {
      root: project.root ?? '',
      targets: {
        [target]: {
          configurations: project.targetConfigurations?.get(target),
          defaultConfiguration: project.targetDefaultConfigurations?.get(target),
        },
      },
    },
  };
  const graph: ProjectGraph = { nodes: { [project.project]: node }, dependencies: {} };
  return new ProcessTasks({}, graph).resolveConfiguration(node, target, configuration);
}

/**
 * Every configuration a task of `target` runs under: each named one, and the run naming none, which is the target's
 * own options or its `defaultConfiguration`. The nx-plugin's output policy reads the same tasks.
 */
export function targetTaskConfigurations(project: ProjectTargets, target: string): (string | undefined)[] {
  return taskConfigurations({
    defaultConfiguration: project.targetDefaultConfigurations?.get(target),
    configurations: project.targetConfigurations?.get(target),
  });
}

/**
 * `declaredOutput` of `target` as Nx interpolates it for the task a run requesting `configuration` makes of it:
 * `{projectRoot}`, `{projectName}`, a leading `{workspaceRoot}/` and `{options.*}` resolve, the options being the
 * target's under the configuration the run resolves to. Nx normalizes the result as it does every output, so the `/`
 * that names a directory and any `..` segment are gone, and a placeholder it cannot resolve stays in the text.
 */
export function interpolateTaskOutput(
  declaredOutput: string,
  project: ProjectTargets,
  target: string,
  configuration: string | undefined,
): string {
  if (project.root === undefined) {
    throw new Error(`Nx project ${project.project} is missing its resolved root.`);
  }
  const task = resolveTaskConfiguration(project, target, configuration);
  const overrides = task === undefined ? undefined : project.targetConfigurations?.get(target)?.[task];
  try {
    return interpolate(declaredOutput, {
      projectRoot: project.root,
      projectName: project.project,
      options: { ...project.targetOptions?.get(target), ...overrides },
    });
  } catch (error) {
    // Nx tests a template against a module-level global regex before it throws, leaving that regex's lastIndex past
    // the first placeholder; the next interpolation in this process would then skip its placeholders. One
    // interpolation rewinds it: a test that fails resets lastIndex.
    interpolate('', {});
    throw new Error(`${project.project}:${target}: ${error instanceof Error ? error.message : String(error)}`, {
      cause: error,
    });
  }
}

export type { NxProjectJson, NxTargetConfig };
