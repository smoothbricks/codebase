// The Nx cache keys the tree at the Nx root `process.argv[2]` looks up (16_build_volumes.md,
// "Rebase carry"): the hash of every cacheable task, computed by the root's own stock Nx the way
// `nx run-many` computes it before it reads the cache. Run as `node - <root>` with this script on
// stdin; answers one JSON object on stdout, `{"hashes": ["<hash>", ...]}`, and fails with a
// non-zero exit and its reason on stderr for anything it cannot hash. It writes what a run writes
// before its first task: the root's task database, and a `task_details` row per hash.

// Nx prints with `process.stdout.write`; the answer is the only thing on stdout.
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);

const path = require('node:path');
const { createRequire } = require('node:module');

const root = process.argv[2];
if (!root || !path.isAbsolute(root)) {
  throw new Error(`usage: node - <absolute Nx root>, given ${JSON.stringify(root)}`);
}
// The environment the Nx capability gives a job of the project at `root`
// (capabilities/nx.rs), set before Nx loads: Nx fixes its workspace root at module load. Whether
// a daemon serves the graph stays Nx's own decision, as for any run.
process.env.NX_WORKSPACE_ROOT_PATH = root;
process.env.NX_WORKSPACE_DATA_DIRECTORY = path.join(root, '.nx', 'workspace-data');
process.env.NX_CACHE_DIRECTORY = path.join(root, '.nx', 'cache');

// The root's own Nx: a hash is only a hit for the Nx that wrote it.
const requireNx = createRequire(path.join(root, 'package.json'));
const { readNxJson } = requireNx('nx/src/config/nx-json');
const { createProjectGraphAsync } = requireNx('nx/src/project-graph/project-graph');
const { splitArgsIntoNxArgsAndOverrides } = requireNx('nx/src/utils/command-line-utils');
const { getRunnerOptions, setEnvVarsBasedOnArgs } = requireNx('nx/src/tasks-runner/run-command');
const { createTaskGraph } = requireNx('nx/src/tasks-runner/create-task-graph');
const { findCycle, makeAcyclic } = requireNx('nx/src/tasks-runner/task-graph-utils');
const { createTaskHasher } = requireNx('nx/src/hasher/create-task-hasher');
const { getTaskDetails, hashTasks } = requireNx('nx/src/hasher/hash-task');
const { getTaskSpecificEnv } = requireNx('nx/src/tasks-runner/task-env');
const { projectHasTarget } = requireNx('nx/src/utils/project-graph-utils');
const hooks = requireNx('nx/src/project-graph/plugins/tasks-execution-hooks');
const { daemonClient } = requireNx('nx/src/daemon/client/client');

async function main() {
  const nxJson = readNxJson();
  // Stock Nx's own connection opens the root's task database, creating it in a tree that never ran
  // Nx: the carry indexes into it, and never copies the target's history to make one.
  const taskDetails = getTaskDetails();
  if (!taskDetails) {
    throw new Error('this Nx keeps no task database (WASM build), so it has no cache to carry into');
  }
  // A running daemon's file map trails what the rebase just wrote until its watcher catches up,
  // and a graph from it would hash yesterday's files. It is stopped, as `nx daemon --stop` stops
  // it; the graph below then comes from a daemon started on the files as they are now.
  if (daemonClient.enabled()) {
    await daemonClient.stop();
  }
  const projectGraph = await createProjectGraphAsync({ exitOnError: false, resetDaemonClient: false });
  const projects = Object.values(projectGraph.nodes);
  const targets = [...new Set(projects.flatMap((project) => Object.keys(project.data.targets ?? {})))].sort();
  if (targets.length === 0) {
    return [];
  }
  // A run names at most one configuration; each target without it runs its default. Every name
  // any target declares is a run some gate may make, and its graph hashes differently: a task's
  // hash covers its dependencies', resolved under the run's configuration.
  const configurations = [
    undefined,
    ...[
      ...new Set(
        projects.flatMap((project) =>
          Object.values(project.data.targets ?? {}).flatMap((target) => Object.keys(target.configurations ?? {})),
        ),
      ),
    ].sort(),
  ];
  // `nx run-many --targets=<every target>` under each configuration, through Nx's own argument
  // normalization: task overrides are part of the hash.
  const runs = configurations.map((configuration) =>
    splitArgsIntoNxArgsAndOverrides({ targets, configuration }, 'run-many', { printWarnings: false }, nxJson),
  );
  const loadDotEnvFiles = process.env.NX_LOAD_DOT_ENV_FILES !== 'false';
  // Every run sets the same variables: the arguments differ only in configuration.
  setEnvVarsBasedOnArgs(runs[0].nxArgs, loadDotEnvFiles);
  // Plugins' `preTasksExecution` hooks set variables that declared `env` inputs hash. The hook is
  // paired with `postTasksExecution`, as around a run, here one that ran no task.
  const run = {
    id: `cowshed-carry-${process.pid}`,
    workspaceRoot: root,
    nxJsonConfiguration: nxJson,
    argv: process.argv,
  };
  const startTime = Date.now();
  await hooks.runPreTasksExecution(run);
  const hasher = createTaskHasher(projectGraph, nxJson, getRunnerOptions('default', nxJson, runs[0].nxArgs, false));
  const runnable = projects.filter((project) => targets.some((target) => projectHasTarget(project, target)));
  const hashes = new Set();
  for (const { nxArgs, overrides } of runs) {
    const taskGraph = createTaskGraph(
      projectGraph,
      {},
      runnable.map((project) => project.name),
      nxArgs.targets,
      nxArgs.configuration,
      overrides,
      false,
    );
    const cycle = findCycle(taskGraph);
    if (cycle) {
      // As `nx run-many` does: a cycle runs nothing unless Nx is told to ignore it.
      if (process.env.NX_IGNORE_CYCLES !== 'true') {
        throw new Error(`the task graph has a circular dependency: ${cycle.join(' --> ')}`);
      }
      makeAcyclic(taskGraph);
    }
    // Only a cacheable task's hash is ever looked up.
    const tasks = Object.values(taskGraph.tasks).filter((task) => task.cache && !task.continuous);
    const perTaskEnvs = {};
    for (const task of tasks) {
      perTaskEnvs[task.id] = getTaskSpecificEnv(task, projectGraph);
    }
    // Recorded as a run records them, through the connection above.
    await hashTasks(hasher, projectGraph, taskGraph, perTaskEnvs, taskDetails, tasks);
    for (const task of tasks) {
      if (!task.hash) {
        throw new Error(`Nx answered no hash for ${task.id}`);
      }
      hashes.add(task.hash);
    }
  }
  await hooks.runPostTasksExecution({ ...run, taskResults: {}, startTime, endTime: Date.now() });
  return [...hashes].sort();
}

main().then(
  (hashes) => answer(`${JSON.stringify({ hashes })}\n`),
  (error) => {
    process.stderr.write(`${error?.stack ?? String(error)}\n`);
    process.exitCode = 1;
  },
);
