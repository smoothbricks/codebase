// What the tree at the Nx root `process.argv[2]` looks up in the Nx cache (16_build_volumes.md,
// "Rebase carry"), computed by the root's own stock Nx the way `nx run-many` computes it before it
// reads the cache. A cacheable task Nx hashes up front is named by its hash. A deferred one, which
// stock Nx hashes only after its dependencies ran, is named by its identity instead: its hash
// covers its dependencies' outputs, and those on disk in a rebased checkout are stale. Run as
// `node - <root>` with this script on stdin; answers one JSON object on stdout,
// `{"hashes": ["<hash>", ...], "deferred": [{"project", "target", "configuration"}, ...]}`
// (`configuration` null for none, as `task_details` records it), and fails with a non-zero exit
// and its reason on stderr for anything it cannot hash. It writes what a run writes before its
// first task: the root's task database, and a `task_details` row per hash.

// Nx prints with `process.stdout.write`; the answer is the only thing on stdout.
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);

const root = process.argv[2];
// The one pinned adapter opens Nx's own complete cache schema in this same hashing job.
const { requireNx } = openNxCache(root);
const { readNxJson } = requireNx('nx/src/config/nx-json');
const { createProjectGraphAsync, readProjectsConfigurationFromProjectGraph } = requireNx(
  'nx/src/project-graph/project-graph',
);
const { splitArgsIntoNxArgsAndOverrides } = requireNx('nx/src/utils/command-line-utils');
const { getRunnerOptions, setEnvVarsBasedOnArgs } = requireNx('nx/src/tasks-runner/run-command');
const { createTaskGraph } = requireNx('nx/src/tasks-runner/create-task-graph');
const { findCycle, makeAcyclic } = requireNx('nx/src/tasks-runner/task-graph-utils');
const { createTaskHasher } = requireNx('nx/src/hasher/create-task-hasher');
const { getInputs } = requireNx('nx/src/hasher/task-hasher');
const { getTaskDetails, hashTasks } = requireNx('nx/src/hasher/hash-task');
const { getTaskSpecificEnv } = requireNx('nx/src/tasks-runner/task-env');
const { getCustomHasher } = requireNx('nx/src/tasks-runner/utils');
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
    return { hashes: [], deferred: [] };
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
  // Plugins' `preTasksExecution` hooks set variables that declared `env` inputs hash. Each hook
  // that ran is paired with `postTasksExecution`, here around a run that ran no task, whether or
  // not the hashing succeeds: a hook's resources are released either way.
  const run = {
    id: `cowshed-carry-${process.pid}`,
    workspaceRoot: root,
    nxJsonConfiguration: nxJson,
    argv: process.argv,
  };
  const startTime = Date.now();
  await hooks.runPreTasksExecution(run);
  let selection;
  // `{ error }` only on failure: a hasher may throw any value, `undefined` included.
  let failed = null;
  try {
    selection = await hashRuns(nxJson, projectGraph, projects, targets, runs, taskDetails);
  } catch (error) {
    failed = { error };
  }
  try {
    await hooks.runPostTasksExecution({ ...run, taskResults: {}, startTime, endTime: Date.now() });
  } catch (error) {
    // Both failures are named: the hashing's is the cause, the hook's what it left behind.
    throw failed === null
      ? error
      : new AggregateError([failed.error, error], 'hashing failed, and so did postTasksExecution after it');
  }
  if (failed !== null) {
    throw failed.error;
  }
  return selection;
}

/**
 * Over `runs`, each run's graph read as Nx's own: the sorted union of the hashes of every cacheable
 * task Nx hashes up front, and the sorted union of the identities of every cacheable task it defers.
 */
async function hashRuns(nxJson, projectGraph, projects, targets, runs, taskDetails) {
  const hasher = createTaskHasher(projectGraph, nxJson, getRunnerOptions('default', nxJson, runs[0].nxArgs, false));
  const projectConfigurations = readProjectsConfigurationFromProjectGraph(projectGraph).projects;
  const runnable = projects.filter((project) => targets.some((target) => projectHasTarget(project, target)));
  const hashes = new Set();
  // Keyed by the identity's JSON: one entry per task, whichever runs defer it.
  const deferred = new Map();
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
    // Nx's own rule, `hashTasksThatDoNotDependOnOutputsOfOtherTasks`: a custom hasher may read
    // other tasks' outputs, and a `dependentTasksOutputFiles` input of a task with dependencies
    // does.
    const upFront = [];
    for (const task of tasks) {
      const defers =
        getCustomHasher(task, projectConfigurations) ||
        (taskGraph.dependencies[task.id].length > 0 && getInputs(task, projectGraph, nxJson).depsOutputs.length > 0);
      if (defers) {
        const identity = {
          project: task.target.project,
          target: task.target.target,
          configuration: task.target.configuration ?? null,
        };
        deferred.set(JSON.stringify(identity), identity);
      } else {
        upFront.push(task);
      }
    }
    const perTaskEnvs = {};
    for (const task of upFront) {
      perTaskEnvs[task.id] = getTaskSpecificEnv(task, projectGraph);
    }
    // Recorded as a run records them, through the task database's connection.
    await hashTasks(hasher, projectGraph, taskGraph, perTaskEnvs, taskDetails, upFront);
    for (const task of upFront) {
      if (!task.hash) {
        throw new Error(`Nx answered no hash for ${task.id}`);
      }
      hashes.add(task.hash);
    }
  }
  return {
    hashes: [...hashes].sort(),
    deferred: [...deferred.keys()].sort().map((key) => deferred.get(key)),
  };
}

main().then(
  (selection) => answer(`${JSON.stringify(selection)}\n`),
  (error) => {
    // Node's own rendering: an AggregateError's `errors` and any `cause` are printed with it.
    console.error(error);
    process.exitCode = 1;
  },
);
