// Land step 7 (16_build_volumes.md): hash/restore only the captured check's Nx task graph.
// No task runner, executor, check shell or execution hook is called, even on a miss.
// Usage: node - <absolute Nx root> <CheckedRun JSON>, with this program on stdin.
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);
const path = require('node:path');
const { createRequire } = require('node:module');
const root = process.argv[2];
const checked = JSON.parse(process.argv[3]);
process.env.NX_WORKSPACE_ROOT_PATH = root;
process.env.NX_WORKSPACE_DATA_DIRECTORY = path.join(root, '.nx', 'workspace-data');
process.env.NX_CACHE_DIRECTORY = path.join(root, '.nx', 'cache');
const requireNx = createRequire(path.join(root, 'package.json'));
const yargs = createRequire(requireNx.resolve('nx/package.json'))('yargs');
const { readNxJson } = requireNx('nx/src/config/nx-json');
const { createProjectGraphAsync } = requireNx('nx/src/project-graph/project-graph');
const { withOverrides } = requireNx('nx/src/command-line/yargs-utils/shared-options');
const { splitArgsIntoNxArgsAndOverrides } = requireNx('nx/src/utils/command-line-utils');
const { getRunnerOptions, setEnvVarsBasedOnArgs } = requireNx('nx/src/tasks-runner/run-command');
const { createTaskGraph } = requireNx('nx/src/tasks-runner/create-task-graph');
const { findCycle } = requireNx('nx/src/tasks-runner/task-graph-utils');
const { createTaskHasher } = requireNx('nx/src/hasher/create-task-hasher');
const { getTaskDetails, hashTasks } = requireNx('nx/src/hasher/hash-task');
const { getTaskSpecificEnv } = requireNx('nx/src/tasks-runner/task-env');
const { getCache } = requireNx('nx/src/tasks-runner/cache');
const { daemonClient } = requireNx('nx/src/daemon/client/client');

async function main() {
  const nxJson = readNxJson();
  // The graph must describe the delivered tree, not a daemon's pre-delivery file map.
  if (daemonClient.enabled()) await daemonClient.stop();
  const projectGraph = await createProjectGraphAsync({ exitOnError: false, resetDaemonClient: false });
  // Attribute accepts only a command spelled by the check. Stock Nx's run summary joins argv
  // with spaces; quoted/ambiguous check arguments fail attribution rather than being guessed.
  const argv = checked.command.split(/\s+/).slice(1);
  const mode = argv[0];
  const command =
    mode === 'run-many'
      ? requireNx('nx/src/command-line/run-many/command-object').yargsRunManyCommand
      : requireNx('nx/src/command-line/run/command-object').yargsRunCommand;
  if (mode !== 'run-many' && mode !== 'run') throw new Error(`unsupported checked Nx command: ${checked.command}`);
  const parsedArgv =
    mode === 'run-many'
      ? requireNx('nx/bin/init-local').rewriteTargetsAndProjects([process.execPath, 'nx', ...argv])
      : argv;
  // Reuse the CLI's declaration and parser, but deliberately never register its handler.
  const parsed = yargs()
    .exitProcess(false)
    .help(false)
    .version(false)
    .command(command.command, false, command.builder)
    .parseSync(parsedArgv);
  const args = withOverrides(parsed);
  let projects;
  let split;
  if (mode === 'run-many') {
    split = splitArgsIntoNxArgsAndOverrides(args, 'run-many', { printWarnings: false }, nxJson);
    projects = requireNx('nx/src/command-line/run-many/run-many')
      .projectsToRun(split.nxArgs, projectGraph)
      .map((project) => project.name);
  } else {
    const opts = requireNx('nx/src/command-line/run/run-one').parseRunOneOptions(root, args, projectGraph, nxJson);
    split = splitArgsIntoNxArgsAndOverrides(
      { ...opts.parsedArgs, configuration: opts.configuration, targets: [opts.target] },
      'run-one',
      { printWarnings: false },
      nxJson,
    );
    projects = requireNx('nx/src/utils/find-matching-projects').findMatchingProjects(
      [opts.project],
      projectGraph.nodes,
    );
  }
  const { nxArgs, overrides } = split;
  setEnvVarsBasedOnArgs(nxArgs, process.env.NX_LOAD_DOT_ENV_FILES !== 'false');
  const graph = createTaskGraph(
    projectGraph,
    {},
    projects,
    nxArgs.targets,
    nxArgs.configuration,
    overrides,
    nxArgs.excludeTaskDependencies,
  );
  const allowed = new Set(Object.keys(checked.tasks));
  const actual = Object.keys(graph.tasks);
  if (actual.length !== allowed.size || actual.some((id) => !allowed.has(id))) {
    throw new Error(
      `checked task graph changed: validated ${JSON.stringify(checked.tasks)}, now ${JSON.stringify(actual)}`,
    );
  }
  const cycle = findCycle(graph);
  if (cycle) throw new Error(`cache-only restoration has a dependency cycle: ${cycle.join(' -> ')}`);
  const options = getRunnerOptions('default', nxJson, nxArgs, false);
  const hasher = createTaskHasher(projectGraph, nxJson, options);
  // Not init()ed: only the adopted local cache is evidence, never a remote download or task.
  const cache = getCache(options);
  const done = new Map();
  const misses = [];
  let hits = 0;
  async function restore(id) {
    if (done.has(id)) return done.get(id);
    const task = graph.tasks[id];
    let dependenciesRestored = true;
    for (const dependency of [...graph.dependencies[id], ...(graph.continuousDependencies[id] ?? [])]) {
      if (!(await restore(dependency))) dependenciesRestored = false;
    }
    let reason;
    if (!dependenciesRestored) reason = 'a dependency has no restorable result; task was not executed';
    else if (!task.cache || task.continuous || nxArgs.skipNxCache)
      reason = 'task is not cacheable; task was not executed';
    else {
      const env = getTaskSpecificEnv(task, projectGraph);
      await hashTasks(hasher, projectGraph, graph, { [id]: env }, getTaskDetails(), [task]);
      if (task.hash !== checked.tasks[id]) {
        reason = `target hash ${task.hash} differs from checked hash ${checked.tasks[id]}; not restored`;
      } else {
        const cached = await cache.get(task);
        if (!cached || cached.code !== 0) reason = 'no successful local cache result; task was not executed';
        else {
          await cache.copyFilesFromCache(task.hash, cached, task.outputs);
          hits += 1;
          done.set(id, true);
          return true;
        }
      }
    }
    misses.push({ task: id, hash: checked.tasks[id], reason });
    done.set(id, false);
    return false;
  }
  for (const id of actual) await restore(id);
  return { hits, misses };
}
main().then(
  (result) => answer(`${JSON.stringify(result)}\n`),
  (error) => {
    console.error(error);
    process.exitCode = 1;
  },
);
