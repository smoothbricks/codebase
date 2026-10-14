// The active land plan's stock-Nx API bridge (16_build_volumes.md, Accepted-project Nx cache).
// Prefixed once by cache_adapter::script; inspect is a protected job, publish a controller child.
// No mode executes a task, scans historical rows, or constructs its own archive.
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);
const path = require('node:path');
const fs = require('node:fs');
const { performance } = require('node:perf_hooks');
const [root, mode, inputJson] = process.argv.slice(2);
if (!root || !path.isAbsolute(root) || !['inspect', 'publish'].includes(mode)) {
  throw new Error('usage: node - <absolute Nx root> <inspect|publish> <selection JSON>');
}
const input = JSON.parse(inputJson);
const { requireNx, cacheDir, getLocal } = openNxCache(root);
const requireFromNx = require('node:module').createRequire(requireNx.resolve('nx/package.json'));
const { HttpRemoteCache } = requireNx('nx/src/native');
const SUCCESS = new Set(['success', 'local-cache', 'local-cache-kept-existing', 'remote-cache']);

function localResult(hash) {
  if (typeof hash !== 'string' || !/^\d{1,20}$/.test(hash)) {
    throw new Error('the qualified receipt contains an invalid Nx task hash');
  }
  const result = getLocal(hash);
  if (result?.code !== 0 || !fs.statSync(result.outputsPath, { throwIfNoEntry: false })?.isDirectory()) {
    throw new Error(`qualified task ${hash} has no successful complete local cache artifact`);
  }
  return result;
}

async function normalizedRun(requireNx, argv, graph, nxJson) {
  if (!Array.isArray(argv) || argv.length < 3 || argv.some((argument) => typeof argument !== 'string')) {
    throw new Error('the task-results hint contains no Nx invocation argv');
  }
  const yargs = requireFromNx('yargs');
  const { withOverrides } = requireNx('nx/src/command-line/yargs-utils/shared-options');
  const { splitArgsIntoNxArgsAndOverrides } = requireNx('nx/src/utils/command-line-utils');
  const arguments_ = argv.slice(2);
  const parser = yargs()
    .exitProcess(false)
    .help(false)
    .version(false)
    .fail((message, error) => {
      throw error ?? new Error(message);
    });
  let module;
  let runMode;
  let commandLevel;
  if (arguments_[0] === 'affected') {
    module = requireNx('nx/src/command-line/affected/command-object').yargsAffectedCommand;
    runMode = 'affected';
    commandLevel = 1;
  } else if (arguments_[0] === 'run-many') {
    module = requireNx('nx/src/command-line/run-many/command-object').yargsRunManyCommand;
    runMode = 'run-many';
    commandLevel = 1;
  } else {
    const commands = requireNx('nx/src/command-line/run/command-object');
    module = arguments_[0] === 'run' ? commands.yargsRunCommand : commands.yargsNxInfixCommand;
    runMode = 'run-one';
    commandLevel = arguments_[0] === 'run' ? 1 : 0;
  }
  // Register only stock argument definitions, never its handler or a task runner.
  parser.command({ command: module.command, describe: module.describe, builder: module.builder });
  const parsed = withOverrides(parser.parseSync(arguments_), commandLevel);
  let normalized;
  let projects;
  if (runMode === 'affected') {
    normalized = splitArgsIntoNxArgsAndOverrides(parsed, runMode, { printWarnings: false }, nxJson);
    const { getAffectedGraphNodes } = requireNx('nx/src/command-line/affected/affected');
    const affected = await getAffectedGraphNodes(normalized.nxArgs, graph);
    projects = affected
      .filter((node) => normalized.nxArgs.targets?.some((target) => node.data.targets?.[target]))
      .map((node) => node.name);
  } else if (runMode === 'run-many') {
    normalized = splitArgsIntoNxArgsAndOverrides(parsed, runMode, { printWarnings: false }, nxJson);
    projects = requireNx('nx/src/command-line/run-many/run-many')
      .projectsToRun(normalized.nxArgs, graph)
      .map((project) => project.name);
  } else {
    const { parseRunOneOptions } = requireNx('nx/src/command-line/run/run-one');
    const options = parseRunOneOptions(root, parsed, graph, nxJson);
    normalized = splitArgsIntoNxArgsAndOverrides(
      { ...options.parsedArgs, configuration: options.configuration, targets: [options.target] },
      runMode,
      { printWarnings: false },
      nxJson,
    );
    const { findMatchingProjects } = requireNx('nx/src/utils/find-matching-projects');
    projects = findMatchingProjects([options.project], graph.nodes);
    if (projects.length !== 1) throw new Error('the checked Nx run-one invocation does not select one project');
  }
  return { ...normalized, projects, command: [path.basename(argv[1]).replace(/\.js$/, ''), ...arguments_].join(' ') };
}

async function inspect() {
  if (
    !input ||
    typeof input.requestId !== 'string' ||
    !/^[0-9a-f]{32}$/.test(input.requestId) ||
    !Number.isSafeInteger(input.startMs) ||
    !Number.isSafeInteger(input.endMs) ||
    input.endMs < input.startMs
  ) {
    throw new Error('the active land plan supplied no bounded check-window selection');
  }
  const directory = path.join(cacheDir, '.cowshed-task-results', input.requestId);
  const names = fs.readdirSync(directory);
  if (names.length === 0 || names.length > 256)
    throw new Error('the successful check left no bounded task-results hints');
  const { readNxJson } = requireNx('nx/src/config/nx-json');
  const { createProjectGraphAsync } = requireNx('nx/src/project-graph/project-graph');
  const { createTaskGraph } = requireNx('nx/src/tasks-runner/create-task-graph');
  const { createTaskHasher } = requireNx('nx/src/hasher/create-task-hasher');
  const { getTaskDetails, hashTasks } = requireNx('nx/src/hasher/hash-task');
  const { getTaskSpecificEnv } = requireNx('nx/src/tasks-runner/task-env');
  const { getRunnerOptions, setEnvVarsBasedOnArgs } = requireNx('nx/src/tasks-runner/run-command');
  const hooks = requireNx('nx/src/project-graph/plugins/tasks-execution-hooks');
  const nxJson = readNxJson();
  const graph = await createProjectGraphAsync({ exitOnError: false });
  const details = getTaskDetails();
  if (!details) throw new Error('the checked Nx client has no native task database');
  const verified = new Map();
  const commands = [];
  for (const name of names.sort()) {
    if (!/^[0-9a-f]{64}\.json$/.test(name)) throw new Error('the task-results hint has an invalid invocation name');
    const file = path.join(directory, name);
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.size === 0 || stat.size > 1024 * 1024)
      throw new Error('the task-results hint is not a bounded regular file');
    const hint = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (
      hint.version !== 1 ||
      hint.requestId !== input.requestId ||
      hint.workspaceRoot !== root ||
      !hint.run ||
      !Number.isSafeInteger(hint.run.startTime) ||
      !Number.isSafeInteger(hint.run.endTime) ||
      hint.run.startTime < input.startMs - 1 ||
      hint.run.endTime > input.endMs + 1 ||
      hint.run.endTime < hint.run.startTime ||
      !Array.isArray(hint.tasks)
    ) {
      throw new Error('the task-results hint does not belong to the successful protected check window');
    }
    const run = await normalizedRun(requireNx, hint.run.argv, graph, nxJson);
    setEnvVarsBasedOnArgs(run.nxArgs, process.env.NX_LOAD_DOT_ENV_FILES !== 'false');
    const taskGraph = createTaskGraph(
      graph,
      {},
      run.projects,
      run.nxArgs.targets,
      run.nxArgs.configuration,
      run.overrides,
      run.nxArgs.excludeTaskDependencies ?? false,
    );
    const tasks = Object.values(taskGraph.tasks);
    const results = new Map(hint.tasks.map((result) => [result.task.id, result]));
    if (results.size !== hint.tasks.length || tasks.length !== results.size) {
      throw new Error('the task-results hint does not cover the exact checked task graph');
    }
    for (const task of tasks) {
      const result = results.get(task.id);
      if (result?.code !== 0 || !SUCCESS.has(result.status)) {
        throw new Error(`the checked task ${task.id} has no successful result`);
      }
    }
    const context = {
      id: `cowshed-inspect-${process.pid}-${commands.length}`,
      workspaceRoot: root,
      nxJsonConfiguration: nxJson,
      argv: hint.run.argv,
    };
    const startTime = Date.now();
    await hooks.runPreTasksExecution(context);
    try {
      const cacheable = tasks.filter((task) => task.cache && !task.continuous);
      const environments = {};
      for (const task of cacheable) environments[task.id] = getTaskSpecificEnv(task, graph);
      const hasher = createTaskHasher(graph, nxJson, getRunnerOptions('default', nxJson, run.nxArgs, false));
      await hashTasks(hasher, graph, taskGraph, environments, details, cacheable);
      for (const task of cacheable) {
        const hinted = results.get(task.id);
        if (!task.hash || task.hash !== hinted.task.hash) {
          throw new Error(`the current hash of checked task ${task.id} differs from its result hint`);
        }
        const local = localResult(task.hash);
        verified.set(task.hash, {
          task: task.id,
          hash: task.hash,
          code: local.code,
          size: local.size,
          cacheStatus: hinted.status,
        });
      }
    } finally {
      await hooks.runPostTasksExecution({ ...context, taskResults: {}, startTime, endTime: Date.now() });
    }
    commands.push(run.command);
  }
  if (verified.size === 0) throw new Error('the successful check qualified no cacheable Nx task');
  for (const name of names) fs.unlinkSync(path.join(directory, name));
  fs.rmdirSync(directory);
  return { node: process.execPath, commands, entries: [...verified.values()] };
}

async function publish() {
  if (!Array.isArray(input) || input.length === 0 || new Set(input).size !== input.length) {
    throw new Error('the private active land plan must select distinct qualified hashes');
  }
  if (!process.env.NX_SELF_HOSTED_REMOTE_CACHE_SERVER || !process.env.NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN) {
    throw new Error('only the active land transaction may supply publication authority');
  }
  const remote = new HttpRemoteCache();
  let published = 0;
  let existing = 0;
  const started = performance.now();
  for (const hash of input) {
    const result = localResult(hash);
    if (await remote.store(hash, cacheDir, result.terminalOutput ?? '', 0)) {
      published += 1;
    } else {
      // Native false means 409 or 403. Prove an existing entry before counting it.
      const response = await fetch(`${process.env.NX_SELF_HOSTED_REMOTE_CACHE_SERVER}/v1/cache/${hash}`, {
        headers: { Authorization: `Bearer ${process.env.NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN}` },
      });
      if (response.status !== 200) {
        await response.body?.cancel();
        throw new Error(`qualified task ${hash} was not published and remote GET returned ${response.status}`);
      }
      await response.body?.cancel();
      existing += 1;
    }
  }
  return { published, existing, elapsedMs: Math.ceil(performance.now() - started) };
}
(mode === 'inspect' ? inspect() : publish()).then(
  (result) => answer(`${JSON.stringify(result)}\n`),
  (error) => {
    console.error(error);
    process.exitCode = 1;
  },
);
