#!/usr/bin/env node
'use strict';

// specs/cowshed/16_build_volumes.md, "Read-only cache-reuse advisor".
// This is deliberately separate from task_hashes.js: carry opens the task database,
// stops the daemon and records details. None of those operations belongs in a probe.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createRequire } = require('node:module');
const { spawnSync } = require('node:child_process');
const { performance } = require('node:perf_hooks');

const usage = `Usage: node cache-advisor.cjs --workspace <absolute checkout> --main <absolute checkout>
  [--runtime=deny|read-only] -- [bun] nx run-many <gate arguments...>
macOS and Node >=22.13 required. Emits one JSON report; never runs targets.
Runtime inputs are denied by default. read-only permits them under the probe's
kernel-enforced write/network denial and reports their exact commands.
`;

function parse(argv) {
  const split = argv.indexOf('--');
  if (split < 0) throw new Error(usage);
  const options = { runtime: 'deny' };
  for (let i = 0; i < split; i++) {
    const arg = argv[i];
    if (arg.startsWith('--runtime=')) options.runtime = arg.slice('--runtime='.length);
    else if (arg === '--workspace' || arg === '--main') options[arg.slice(2)] = argv[++i];
    else throw new Error(`unknown advisor argument ${arg}\n${usage}`);
  }
  for (const key of ['workspace', 'main']) {
    if (!options[key] || !path.isAbsolute(options[key]))
      throw new Error(`${key} must be an absolute checkout path\n${usage}`);
    options[key] = fs.realpathSync(options[key]);
  }
  if (!['deny', 'read-only'].includes(options.runtime)) throw new Error(`unknown runtime policy ${options.runtime}`);
  const command = argv.slice(split + 1);
  if (command[0] === 'bun') command.shift();
  if (command.shift() !== 'nx' || command.shift() !== 'run-many') {
    throw new Error(`only an explicit nx run-many command is supported; shell scripts are not interpreted\n${usage}`);
  }
  options.command = command;
  return options;
}

function sandboxProfile(scratch) {
  // All descendants inherit this restriction, including graph plugins and runtime
  // shells. The existing sccache socket is metadata/toolchain IPC, not a target.
  const socket = process.env.SCCACHE_SERVER_UDS;
  const sccache = socket
    ? `(allow network-outbound (remote unix-socket (path-literal ${JSON.stringify(socket)})))`
    : '';
  return `(version 1)(allow default)(deny file-write*)(deny network*)(deny signal)
    (allow file-write* (subpath ${JSON.stringify(scratch)}) (literal "/dev/null"))${sccache}`;
}

function run(options) {
  if (process.platform !== 'darwin')
    throw new Error('cache advisor requires macOS sandbox-exec; refusing an unconfined probe');
  const [major, minor] = process.versions.node.split('.').map(Number);
  if (major < 22 || (major === 22 && minor < 13))
    throw new Error(`cache advisor requires Node >=22.13, got ${process.versions.node}`);
  const start = performance.now();
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cowshed-advisor-')));
  try {
    const child = spawnSync(
      '/usr/bin/sandbox-exec',
      ['-p', sandboxProfile(scratch), process.execPath, __filename, '--probe', scratch, JSON.stringify(options)],
      {
        cwd: options.workspace,
        env: { ...process.env, TMPDIR: `${scratch}/` },
        encoding: 'utf8',
        maxBuffer: 32 * 1024 * 1024,
      },
    );
    if (child.stderr) process.stderr.write(child.stderr);
    if (child.error)
      throw new Error(`starting confined advisor for ${options.workspace}: ${child.error.message}`, {
        cause: child.error,
      });
    if (child.status !== 0)
      throw new Error(`confined advisor for ${options.workspace} exited ${child.status ?? child.signal}`);
    const report = JSON.parse(child.stdout);
    report.wallSeconds = (performance.now() - start) / 1000;
    return report;
  } finally {
    fs.rmSync(scratch, { recursive: true, force: true });
  }
}

function identity(task) {
  return JSON.stringify([task.project, task.target, task.configuration ?? null]);
}

function fingerprint(file) {
  try {
    const stat = fs.statSync(file, { bigint: true });
    return [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs];
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

function assertUnchanged(file, before) {
  const after = fingerprint(file);
  if ((before === null) !== (after === null) || before?.some((value, i) => value !== after[i])) {
    throw new Error(`busy: candidate file changed during snapshot: ${file}; no time decision available`);
  }
}

function copyDatabase(filename, destination) {
  const files = ['', '-wal', '-journal'].map((suffix) => ({
    source: filename + suffix,
    destination: destination + suffix,
    before: fingerprint(filename + suffix),
  }));
  if (!files[0].before)
    throw new Error(`busy: candidate database disappeared before snapshot: ${filename}; no time decision available`);
  let bytes = 0;
  try {
    for (const file of files) {
      if (file.before) {
        fs.copyFileSync(file.source, file.destination);
        bytes += Number(file.before[2]);
      }
    }
  } catch (error) {
    for (const file of files) assertUnchanged(file.source, file.before);
    throw new Error(`copying candidate database ${filename}: ${error.message}`, { cause: error });
  }
  // Check the ENTIRE set after all copies, including previously absent sidecars.
  // A writer invalidates the observation; never retry or ignore the live WAL.
  for (const file of files) assertUnchanged(file.source, file.before);
  return bytes;
}

function candidate(root, scratch) {
  const { DatabaseSync } = require('node:sqlite');
  const started = performance.now();
  const dataLink = path.join(root, '.nx', 'workspace-data');
  const cacheLink = path.join(root, '.nx', 'cache');
  const data = fs.existsSync(dataLink) ? fs.realpathSync(dataLink) : dataLink;
  const cache = fs.existsSync(cacheLink) ? fs.realpathSync(cacheLink) : cacheLink;
  const databaseNames = () =>
    fs.existsSync(data)
      ? fs
          .readdirSync(data)
          .filter((name) => name.endsWith('.db'))
          .sort()
      : [];
  const databases = databaseNames();
  fs.mkdirSync(scratch, { recursive: true });
  const hashes = new Set();
  const cachedIdentities = new Map();
  let identityInventoryComplete = true;
  const samples = [];
  let snapshotBytes = 0;
  for (const name of databases) {
    const filename = path.join(data, name);
    const privateFile = path.join(scratch, name);
    snapshotBytes += copyDatabase(filename, privateFile);
    // Only the private copy is opened by SQLite. Writable scratch permits private
    // WAL/journal recovery and SHM creation; no live database is opened at all.
    const db = new DatabaseSync(privateFile);
    try {
      db.exec('PRAGMA query_only=ON; BEGIN');
      const tables = new Set(
        db
          .prepare("SELECT name FROM sqlite_master WHERE type='table'")
          .all()
          .map((row) => row.name),
      );
      if (tables.has('cache_outputs')) {
        const query = tables.has('task_details')
          ? `SELECT c.hash, d.project, d.target, d.configuration FROM cache_outputs c
             LEFT JOIN task_details d ON d.hash=c.hash WHERE c.code=0`
          : 'SELECT hash FROM cache_outputs WHERE code=0';
        for (const row of db.prepare(query).all()) {
          if (!/^[0-9]+$/.test(row.hash)) throw new Error(`invalid Nx hash ${JSON.stringify(row.hash)} in ${filename}`);
          if (!fs.existsSync(path.join(cache, row.hash))) continue;
          hashes.add(row.hash);
          if (typeof row.project !== 'string' || typeof row.target !== 'string') {
            identityInventoryComplete = false;
            continue;
          }
          const key = identity(row);
          const entries = cachedIdentities.get(key) ?? new Set();
          entries.add(row.hash);
          cachedIdentities.set(key, entries);
        }
      }
      if (tables.has('task_history') && tables.has('task_details')) {
        samples.push(
          ...db
            .prepare(`SELECT d.project, d.target, d.configuration, h.hash, h.start, h.end
          FROM task_history h JOIN task_details d ON d.hash=h.hash
          WHERE h.status='success' AND h.code=0 AND h.end>=h.start`)
            .all(),
        );
      }
      db.exec('ROLLBACK');
    } catch (error) {
      throw new Error(`reading candidate database ${filename}: ${error.message}`, { cause: error });
    } finally {
      db.close();
    }
  }
  if (JSON.stringify(databaseNames()) !== JSON.stringify(databases))
    throw new Error(`busy: database set changed in ${data}; no time decision available`);
  for (const [link, resolved] of [
    [dataLink, data],
    [cacheLink, cache],
  ]) {
    if (fs.existsSync(link) && fs.realpathSync(link) !== resolved)
      throw new Error(`busy: candidate link changed: ${link}; no time decision available`);
  }
  return {
    root,
    databases,
    hashes,
    cachedIdentities,
    identityInventoryComplete,
    samples,
    snapshotBytes,
    snapshotSeconds: (performance.now() - started) / 1000,
  };
}

function assess(task, observation) {
  if (observation.hashes === null) {
    return { status: 'UNAVAILABLE', basis: 'unavailable-candidate', reason: observation.error };
  }
  if (task.hash) {
    return { status: observation.hashes.has(task.hash) ? 'HIT' : 'MISS', basis: 'current-key', reason: null };
  }
  const cachedEntries = observation.cachedIdentities.get(identity(task))?.size ?? 0;
  if (task.hashMode === 'custom') {
    return {
      status: 'UNKNOWN',
      basis: 'custom-key-unavailable',
      cachedEntries,
      reason: 'a custom hasher can return a key unrelated to task identity; identity absence cannot prove a miss',
    };
  }
  if (cachedEntries === 0 && observation.identityInventoryComplete) {
    return {
      status: 'MISS',
      basis: 'identity-absence',
      cachedEntries,
      reason: 'no successful cached entry for this identity; any current key would miss',
    };
  }
  return {
    status: 'UNKNOWN',
    basis: 'key-unavailable',
    cachedEntries,
    reason: cachedEntries
      ? 'historical entries exist, but their keys do not prove a current hit without the current dependency outputs or other omitted hash inputs'
      : 'cached entries lack identity metadata, so absence of a matching identity cannot be proved',
  };
}

function summarize(selection, own, main) {
  // The same tuple is the SQL UNION key used by the measured duration query.
  // Do not count a historical run twice just because its database was cloned.
  const seen = new Set();
  const durations = new Map();
  for (const sample of [...own.samples, ...main.samples]) {
    const key = identity(sample);
    const event = JSON.stringify([key, sample.hash, sample.start, sample.end]);
    if (seen.has(event)) continue;
    seen.add(event);
    const seconds = (sample.end - sample.start) / 1000;
    const value = durations.get(key) ?? { samples: 0, total: 0, minSeconds: seconds, maxSeconds: seconds };
    value.samples++;
    value.total += seconds;
    value.minSeconds = Math.min(value.minSeconds, seconds);
    value.maxSeconds = Math.max(value.maxSeconds, seconds);
    durations.set(key, value);
  }
  const candidatesUsable = own.hashes !== null && main.hashes !== null;
  const hasNxCache = candidatesUsable && (own.hashes.size > 0 || main.hashes.size > 0);
  const classes = { HIT_BOTH: 0, HIT_OWN: 0, HIT_MAIN: 0, MISS_BOTH: 0, UNRESOLVED: 0 };
  const totals = {
    ownMissWorkSeconds: 0,
    mainMissWorkSeconds: 0,
    ownMeasuredMissWorkSeconds: 0,
    mainMeasuredMissWorkSeconds: 0,
    ownAssumedMissWorkSeconds: 0,
    mainAssumedMissWorkSeconds: 0,
    assumedDifferenceSeconds: 0,
    keylessWorstCaseSeconds: 0,
    weightedKeylessTasks: 0,
    ownUnweightedMisses: 0,
    mainUnweightedMisses: 0,
  };
  let ownAssessedTasks = 0;
  let mainAssessedTasks = 0;
  let assumedTasks = 0;
  let withDuration = 0;
  let resolvedWithDuration = 0;
  const tasks = selection.tasks.map((task) => {
    const history = durations.get(identity(task));
    const weight = history
      ? {
          meanSeconds: history.total / history.samples,
          samples: history.samples,
          minSeconds: history.minSeconds,
          maxSeconds: history.maxSeconds,
        }
      : null;
    if (weight) withDuration++;
    const ownAssessment = assess(task, own);
    const mainAssessment = assess(task, main);
    const ownUnknown = ownAssessment.status === 'UNKNOWN';
    const mainUnknown = mainAssessment.status === 'UNKNOWN';
    if (ownAssessment.status === 'HIT' || ownAssessment.status === 'MISS') ownAssessedTasks++;
    if (mainAssessment.status === 'HIT' || mainAssessment.status === 'MISS') mainAssessedTasks++;
    const classification =
      !task.hash || !candidatesUsable
        ? 'UNRESOLVED'
        : ownAssessment.status === 'HIT'
          ? mainAssessment.status === 'HIT'
            ? 'HIT_BOTH'
            : 'HIT_OWN'
          : mainAssessment.status === 'HIT'
            ? 'HIT_MAIN'
            : 'MISS_BOTH';
    classes[classification]++;
    if (classification !== 'UNRESOLVED' && weight) resolvedWithDuration++;
    if (ownUnknown || mainUnknown) assumedTasks++;
    if (ownUnknown && mainUnknown && weight) {
      totals.keylessWorstCaseSeconds += weight.meanSeconds;
      totals.weightedKeylessTasks++;
    }
    if (candidatesUsable) {
      if (weight) totals.assumedDifferenceSeconds += (Number(ownUnknown) - Number(mainUnknown)) * weight.meanSeconds;
      for (const [side, assessment] of [
        ['own', ownAssessment],
        ['main', mainAssessment],
      ]) {
        if (assessment.status === 'MISS' || assessment.status === 'UNKNOWN') {
          if (weight) {
            totals[`${side}MissWorkSeconds`] += weight.meanSeconds;
            const component = assessment.status === 'UNKNOWN' ? 'Assumed' : 'Measured';
            totals[`${side}${component}MissWorkSeconds`] += weight.meanSeconds;
          } else totals[`${side}UnweightedMisses`]++;
        }
      }
    }
    return {
      ...task,
      reason: task.reason ?? own.error ?? main.error ?? null,
      own: ownUnknown ? 'ASSUMED_MISS' : ownAssessment.status === 'UNAVAILABLE' ? 'UNRESOLVED' : ownAssessment.status,
      main: mainUnknown
        ? 'ASSUMED_MISS'
        : mainAssessment.status === 'UNAVAILABLE'
          ? 'UNRESOLVED'
          : mainAssessment.status,
      assessment: { own: ownAssessment, main: mainAssessment },
      classification,
      weight,
      cost: {
        ownAssumed: ownUnknown,
        mainAssumed: mainUnknown,
        label:
          ownUnknown && mainUnknown
            ? 'unpriced key, worst case: misses in both'
            : ownUnknown
              ? 'unpriced key in own, worst case: miss in own'
              : mainUnknown
                ? 'unpriced key in main, worst case: miss in main'
                : null,
        ownSeconds:
          ownAssessment.status === 'UNAVAILABLE'
            ? null
            : ownAssessment.status === 'HIT'
              ? 0
              : (weight?.meanSeconds ?? null),
        mainSeconds:
          mainAssessment.status === 'UNAVAILABLE'
            ? null
            : mainAssessment.status === 'HIT'
              ? 0
              : (weight?.meanSeconds ?? null),
      },
    };
  });
  const unpriced = tasks
    .filter((task) => !candidatesUsable || !task.weight)
    .map((task) => ({
      id: task.id,
      reason: !candidatesUsable
        ? (own.error ?? main.error)
        : 'no successful uncached duration sample for this task identity',
    }));
  const timeAvailable = hasNxCache && withDuration > 0;
  const determinedDifference = totals.ownMeasuredMissWorkSeconds - totals.mainMeasuredMissWorkSeconds;
  const difference = determinedDifference + totals.assumedDifferenceSeconds;
  const differenceRange = timeAvailable
    ? {
        minSeconds: determinedDifference - totals.mainAssumedMissWorkSeconds,
        maxSeconds: determinedDifference + totals.ownAssumedMissWorkSeconds,
        downwardSwingSeconds: totals.ownAssumedMissWorkSeconds,
        upwardSwingSeconds: totals.mainAssumedMissWorkSeconds,
        maxSwingSeconds: Math.max(totals.ownAssumedMissWorkSeconds, totals.mainAssumedMissWorkSeconds),
        scope: unpriced.length === 0 ? 'all selected tasks' : 'weighted tasks only; unpriced tasks excluded',
      }
    : null;
  const observation = ({ root, databases, snapshotBytes, snapshotSeconds, error }) => ({
    root,
    databases,
    snapshotBytes,
    snapshotSeconds,
    error,
  });
  return {
    ...selection,
    candidates: { own: observation(own), main: observation(main) },
    tasks,
    classes,
    totals: {
      ...totals,
      ownMinusMainSeconds: difference,
      differenceRange,
      complete: timeAvailable && unpriced.length === 0,
    },
    decision: {
      basis: timeAvailable ? 'time' : 'space',
      timeDecisionAvailable: timeAvailable,
      workScoreSeconds: timeAvailable ? difference : null,
      reason: !candidatesUsable
        ? 'candidate observation unavailable; caller must fall back to space'
        : !hasNxCache
          ? 'neither candidate has a usable Nx cache; caller must fall back to space'
          : !withDuration
            ? 'no selected task has a duration sample; caller must fall back to space'
            : 'per-candidate historical mean rebuild work is usable; unknown cache outcomes are separately bounded',
    },
    unpriced,
    coverage: {
      cacheableTasks: tasks.length,
      withDuration,
      resolvedTasks: tasks.length - classes.UNRESOLVED,
      resolvedWithDuration,
      ownAssessedTasks,
      mainAssessedTasks,
      assumedTasks,
    },
    unresolved: tasks
      .filter((task) => task.classification === 'UNRESOLVED')
      .map(({ id, reason, assessment }) => ({
        id,
        reason,
        own: assessment.own.status,
        main: assessment.main.status,
      })),
    caveat:
      'Historical means estimate serial work, not gate elapsed time. Unknown cache outcomes use separately reported worst-case misses; the difference interval holds these mean weights fixed and excludes unpriced tasks and runtime variance.',
  };
}

async function probe(options, scratch) {
  // Same on-disk ownership contract as cowshed-os::scratch. Publish after the child starts:
  // a parent killed during spawn leaves an unknown owner, never false evidence of death.
  fs.writeFileSync(path.join(scratch, '.cowshed-owner'), `cowshed-scratch-v1\n${process.ppid}\n${process.pid}\nend\n`, {
    flag: 'wx',
    mode: 0o600,
  });
  const start = performance.now();
  const root = options.workspace;
  process.chdir(root);
  Object.assign(process.env, {
    NX_WORKSPACE_ROOT_PATH: root,
    NX_WORKSPACE_DATA_DIRECTORY: path.join(scratch, 'workspace-data'),
    NX_CACHE_DIRECTORY: path.join(scratch, 'cache'),
    NX_PROJECT_GRAPH_CACHE_DIRECTORY: path.join(scratch, 'workspace-data'),
    NX_NATIVE_FILE_CACHE_DIRECTORY: path.join(scratch, 'native'),
    NX_DAEMON: 'false',
    NX_ISOLATE_PLUGINS: 'false',
  });
  // Reuse only disposable resolution caches, not file hashes or a possibly stale
  // project graph. Nx walks this checkout and builds its graph afresh exactly once.
  const data = process.env.NX_WORKSPACE_DATA_DIRECTORY;
  fs.mkdirSync(data, { recursive: true });
  const sourceData = path.join(root, '.nx', 'workspace-data');
  for (const name of [
    'cargo-resolve',
    'parsed-lock-file.dependencies.json',
    'parsed-lock-file.nodes.json',
    'lockfile-dependencies.hash',
    'lockfile-nodes.hash',
  ]) {
    if (fs.existsSync(path.join(sourceData, name)))
      fs.cpSync(path.join(sourceData, name), path.join(data, name), { recursive: true });
  }
  const requireNx = createRequire(path.join(root, 'package.json'));
  const { readNxJson } = requireNx('nx/src/config/nx-json');
  const { createProjectGraphAsync, readProjectsConfigurationFromProjectGraph } = requireNx(
    'nx/src/project-graph/project-graph',
  );
  const { withOverrides } = requireNx('nx/src/command-line/yargs-utils/shared-options');
  const { yargsRunManyCommand } = requireNx('nx/src/command-line/run-many/command-object');
  const { splitArgsIntoNxArgsAndOverrides } = requireNx('nx/src/utils/command-line-utils');
  const { setEnvVarsBasedOnArgs, getRunnerOptions } = requireNx('nx/src/tasks-runner/run-command');
  const nxJson = readNxJson();
  const yargs = createRequire(requireNx.resolve('nx/package.json'))('yargs');
  const parsed = await yargsRunManyCommand
    .builder(yargs(options.command).exitProcess(false).help(false).version(false))
    .parse();
  const { nxArgs, overrides } = splitArgsIntoNxArgsAndOverrides(
    withOverrides(parsed, 0),
    'run-many',
    { printWarnings: false },
    nxJson,
  );
  if (!nxArgs.targets?.length) throw new Error('the gate command must name at least one target');
  if (nxArgs.skipNxCache || nxArgs.graph || nxArgs.watch)
    throw new Error('cache bypass, graph and watch commands are not advisory gate commands');
  setEnvVarsBasedOnArgs(nxArgs, process.env.NX_LOAD_DOT_ENV_FILES !== 'false');
  const projectGraph = await createProjectGraphAsync({ exitOnError: false, resetDaemonClient: false });
  const { projectsToRun } = requireNx('nx/src/command-line/run-many/run-many');
  const { createTaskGraph } = requireNx('nx/src/tasks-runner/create-task-graph');
  const { findCycle, makeAcyclic } = requireNx('nx/src/tasks-runner/task-graph-utils');
  const taskGraph = createTaskGraph(
    projectGraph,
    {},
    projectsToRun(nxArgs, projectGraph).map((project) => project.name),
    nxArgs.targets,
    nxArgs.configuration,
    overrides,
    nxArgs.excludeTaskDependencies,
  );
  const cycle = findCycle(taskGraph);
  if (cycle) {
    if (!nxArgs.nxIgnoreCycles && process.env.NX_IGNORE_CYCLES !== 'true')
      throw new Error(`circular task graph: ${cycle.join(' --> ')}`);
    makeAcyclic(taskGraph);
  }
  const { getPlugins, cleanupPlugins } = requireNx('nx/src/project-graph/plugins/get-plugins');
  const plugins = await getPlugins(nxJson, root);
  const hooks = plugins
    .filter((plugin) => plugin.preTasksExecution || plugin.postTasksExecution)
    .map((plugin) => plugin.name);
  const { getInputs } = requireNx('nx/src/hasher/task-hasher');
  const { getCustomHasher } = requireNx('nx/src/tasks-runner/utils');
  const { createTaskHasher } = requireNx('nx/src/hasher/create-task-hasher');
  const { hashTasks } = requireNx('nx/src/hasher/hash-task');
  const { getTaskSpecificEnv } = requireNx('nx/src/tasks-runner/task-env');
  const { HashPlanInspector } = requireNx('nx/src/hasher/hash-plan-inspector');
  const configurations = readProjectsConfigurationFromProjectGraph(projectGraph).projects;
  const inspector = new HashPlanInspector(projectGraph, root, nxJson);
  await inspector.init();
  const plans = inspector.inspector.inspectInputs(
    inspector.planner.getPlansReference(Object.keys(taskGraph.tasks), taskGraph),
  );
  const cacheable = Object.values(taskGraph.tasks).filter((task) => task.cache && !task.continuous);
  const rows = [];
  const upFront = [];
  for (const task of cacheable) {
    const runtimeCommands = plans[task.id]?.runtime ?? [];
    let reason = null;
    const customHasher = getCustomHasher(task, configurations);
    if (customHasher) reason = 'custom hasher may consume dependency outputs';
    else if (taskGraph.dependencies[task.id].length && getInputs(task, projectGraph, nxJson).depsOutputs.length)
      reason = 'deferred: dependency outputs are not current until dependencies execute';
    else if (hooks.length) reason = `execution hooks are not run by the advisor: ${hooks.join(', ')}`;
    else if (runtimeCommands.length && options.runtime === 'deny')
      reason = 'runtime inputs denied; opt in with --runtime=read-only after auditing the reported commands';
    // Pinned Nx ignores a runtime command's exit status. The repository wrapper
    // makes failures invalid UTF-8, which Nx rejects rather than hashing an error.
    else if (
      runtimeCommands.some(
        (command) => !command.startsWith('sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh '),
      )
    ) {
      reason = 'runtime input lacks the fail-closed nx-plugin/runtime-input.sh wrapper';
    }
    if (!reason) upFront.push(task);
    rows.push({
      id: task.id,
      project: task.target.project,
      target: task.target.target,
      hashMode: customHasher ? 'custom' : 'stock',
      configuration: task.target.configuration ?? null,
      hash: null,
      reason,
      runtimeCommands,
      runtimeInputsAdmitted: !reason && runtimeCommands.length > 0,
    });
  }
  const hasher = createTaskHasher(projectGraph, nxJson, getRunnerOptions(nxArgs.runner, nxJson, nxArgs, false));
  const envs = Object.fromEntries(upFront.map((task) => [task.id, getTaskSpecificEnv(task, projectGraph)]));
  let hashFailure = null;
  try {
    // Exactly one hash pass, and deliberately NO TaskDetails argument/connection.
    await hashTasks(hasher, projectGraph, taskGraph, envs, undefined, upFront);
  } catch (error) {
    hashFailure = `hash pass failed under read-only policy: ${error.stack ?? error}`;
  } finally {
    cleanupPlugins();
  }
  for (const row of rows) {
    if (row.reason) continue;
    const task = taskGraph.tasks[row.id];
    if (hashFailure || !/^[0-9]+$/.test(task.hash ?? '')) row.reason = hashFailure ?? 'Nx returned no decimal hash';
    else row.hash = task.hash;
  }
  const selection = {
    command: ['nx', 'run-many', ...options.command],
    tasks: rows.sort((a, b) => a.id.localeCompare(b.id)),
    noncacheableTasks: Object.values(taskGraph.tasks)
      .filter((task) => !task.cache || task.continuous)
      .map((task) => task.id)
      .sort(),
    audit: {
      runtimePolicy: options.runtime,
      hooksNotExecuted: hooks,
      targetExecutions: 0,
      sccacheSocket: process.env.SCCACHE_SERVER_UDS ?? null,
      confinement:
        'persistent writes only to scratch; /dev/null allowed; signals denied; network only to configured sccache Unix metadata socket; Nx daemon bypassed in-process',
    },
    hashPassSeconds: (performance.now() - start) / 1000,
  };
  function observe(candidateRoot, label) {
    try {
      return candidate(candidateRoot, path.join(scratch, label));
    } catch (error) {
      const reason = `candidate ${label} (${candidateRoot}) unavailable: ${error.message}`;
      console.error(reason);
      return { root: candidateRoot, databases: [], hashes: null, samples: [], error: reason };
    }
  }
  return summarize(selection, observe(root, 'own'), observe(options.main, 'main'));
}

module.exports = { parse, run, summarize, candidate, sandboxProfile, fingerprint, assertUnchanged };
if (require.main === module) {
  if (process.argv[2] === '--probe') {
    const answer = process.stdout.write.bind(process.stdout);
    process.stdout.write = process.stderr.write.bind(process.stderr);
    probe(JSON.parse(process.argv[4]), process.argv[3]).then(
      (report) => answer(`${JSON.stringify(report)}\n`),
      (error) => {
        console.error(error);
        process.exitCode = 1;
      },
    );
  } else {
    try {
      if (process.argv.includes('--help')) process.stdout.write(usage);
      else process.stdout.write(`${JSON.stringify(run(parse(process.argv.slice(2))))}\n`);
    } catch (error) {
      console.error(error);
      process.stdout.write(
        `${JSON.stringify({
          decision: { basis: 'space', timeDecisionAvailable: false, workScoreSeconds: null, reason: error.message },
          error: error.message,
        })}\n`,
      );
      process.exitCode = 1;
    }
  }
}
