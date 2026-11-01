import { expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, stopFixtureNxDaemon, withNxFixture } from './__tests__/fixture-nx-env.js';
import { whenFound } from './__tests__/path-change.js';
import { awaitExit, isRunning, processesOf, terminate } from './testing.js';

/**
 * Guards the `NX_DAEMON_START=never` hunks of `patches/nx@24.0.3.patch`. A stock client that finds no live daemon
 * starts one, detached into a session of its own, and the record shares it with every client of the checkout. Cowshed
 * ends a job's whole process tree when the job ends (`specs/cowshed/11_shell.md`), so a daemon one job's client
 * started would end with that job while another job's `nx run` still used it. Every cowshed job therefore runs with
 * `NX_DAEMON_START=never` (`specs/cowshed/04_sandbox.md`): its client uses the daemon the record names while that
 * runs, otherwise computes the command without one, and never starts a daemon. A daemon that stops under such a
 * client turns the daemon off for that process alone, and the command continues without it: its successor is its
 * starter's to start, and the stop is no reason to disable the checkout's daemon for anyone else.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const nxVersion: string = createRequire(join(repositoryRoot, 'package.json'))('nx/package.json').version;

/** The original cases this file walks. Every assertion's message starts with the name of the case it guards. */
const cases = {
  never: 'a client that may never start a daemon uses a running one and otherwise runs without',
  gone: 'a client that may never start a daemon continues without one that stopped under it, and disables nothing',
  explicit: 'refuses an explicit daemon start under the job rule, and starts one without it',
  descriptors: 'a restarting daemon closes the log descriptors it hands its successor',
  midrun: 'a run whose daemon stops under it continues without one, and disables nothing',
  restart: 'a run whose daemon restarts itself under it finishes',
  fresh: 'a run that continues without its daemon hashes over the graph it builds, not the planner of its daemon run',
  requests: 'a plugin-capabilities request and an affected-task selection whose daemon stops continue in this process',
} as const;

// The child owns Nx's workspace singletons. It hands the daemon-based hasher a planning context built over a graph
// whose `p:build` runs another command, as the daemon run's planner was, then hashes `p:build` with the in-process
// hasher the run continues with and with one built fresh over the same graph.
const FRESH_PROBE = String.raw`
const { join } = require('node:path');
const nx = process.argv[2];
const { daemonClient } = require(join(nx, 'dist/src/daemon/client/client.js'));
const { createTaskHasher } = require(join(nx, 'dist/src/hasher/create-task-hasher.js'));
const { createProjectGraphAsync } = require(join(nx, 'dist/src/project-graph/project-graph.js'));
const { createTaskGraph } = require(join(nx, 'dist/src/tasks-runner/create-task-graph.js'));
const { readNxJson } = require(join(nx, 'dist/src/config/nx-json.js'));
const { transformProjectGraphForRust } = require(join(nx, 'dist/src/native/transform-objects.js'));
const { HashPlanner } = require(join(nx, 'dist/src/native/index.js'));
(async () => {
  const nxJson = readNxJson();
  const graph = await createProjectGraphAsync({ exitOnError: false });
  const planned = structuredClone(graph);
  planned.nodes.p.data.targets.build.options.command = 'echo planned';
  const projectGraphRef = transformProjectGraphForRust(planned);
  const planningContext = { projectGraphRef, planner: new HashPlanner(nxJson, projectGraphRef) };
  daemonClient.enabled = () => true;
  const daemonBased = createTaskHasher(graph, nxJson, {}, undefined, planningContext);
  // The stopped daemon turns the daemon off for this process before the run continues.
  daemonClient.enabled = () => false;
  const continued = await daemonBased.withoutDaemon();
  const fresh = createTaskHasher(graph, nxJson, {}, undefined, undefined);
  const taskGraph = createTaskGraph(graph, {}, ['p'], ['build'], undefined, {});
  const task = taskGraph.tasks['p:build'];
  const env = { [task.id]: process.env };
  const [fromContinued] = await continued.hashTasks([{ ...task }], taskGraph, env);
  const [fromFresh] = await fresh.hashTasks([{ ...task }], taskGraph, env);
  process.stdout.write(JSON.stringify({ continued: fromContinued.value, fresh: fromFresh.value }) + '\n');
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;

// The child owns Nx's daemon client. It finds a running daemon (`enabled()` already answered true), whose request
// for `argv[3]` then fails as a daemon that stopped under a client that may never start one fails it. For the affected
// selection the daemon still answers plugin capabilities, which the in-process selection asks for too: only the
// selection's own request may turn the daemon off.
const REQUEST_PROBE = String.raw`
const { join } = require('node:path');
const nx = process.argv[2];
const request = process.argv[3];
const { daemonClient } = require(join(nx, 'dist/src/daemon/client/client.js'));
const { capabilitiesOfConfiguredPlugins } = require(join(nx, 'dist/src/project-graph/plugins/get-plugins.js'));
const { computeAffectedTasks } = require(join(nx, 'dist/src/project-graph/affected/affected-tasks.js'));
const { createProjectGraphAsync } = require(join(nx, 'dist/src/project-graph/project-graph.js'));
const { readNxJson } = require(join(nx, 'dist/src/config/nx-json.js'));
const stopped = async () => {
  throw Object.assign(new Error('The daemon stopped under this run.'), { daemonStartForbidden: true });
};
(async () => {
  const nxJson = readNxJson();
  const projectGraph = await createProjectGraphAsync({ exitOnError: false });
  daemonClient._enabled = true;
  daemonClient.getPluginCapabilities = request === 'capabilities' ? stopped : async () => [];
  daemonClient.selectAffectedTasks = stopped;
  let answered;
  if (request === 'capabilities') {
    answered = (await capabilitiesOfConfiguredPlugins(nxJson)).length > 0;
  } else {
    const selection = await computeAffectedTasks({
      targets: ['build'],
      touchedFiles: [{ file: 'p/src.txt', getChanges: () => [] }],
      nxJson,
      projectGraph,
    });
    answered = selection.affectedTaskIds.has('p:build');
  }
  process.stdout.write(JSON.stringify({ answered, daemonOn: daemonClient.enabled() }) + '\n');
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;

const record = (workspace: string) => join(workspace, '.nx/workspace-data/d/server-process.json');
const daemonLog = (workspace: string) => join(workspace, '.nx/workspace-data/d/daemon.log');
const taskOutput = (project: string) => `${project}/dist/${project}.txt`;
const build = (command: string) => ({ command, outputs: ['{projectRoot}/dist/'], cache: true });

async function recordedPid(workspace: string): Promise<number | null> {
  const text = await readFile(record(workspace), 'utf8').catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  });
  if (text === null) return null;
  const parsed: unknown = JSON.parse(text);
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('processId' in parsed) ||
    typeof parsed.processId !== 'number'
  ) {
    throw new Error(`the daemon record names no process: ${text}`);
  }
  return parsed.processId;
}

/**
 * The pid a whole record names. Where link(2) is refused (a cowshed sandbox), a daemon creates its record exclusively
 * and then writes it, and removes its staged copy only after that (`patches/nx@24.0.3.patch`): a record that does not
 * parse yet is still being written, and that removal brings the next judgment.
 */
async function wholeRecordPid(workspace: string): Promise<number | null> {
  try {
    return await recordedPid(workspace);
  } catch (error) {
    if (error instanceof SyntaxError) return null;
    throw error;
  }
}

/**
 * The pid the record names, once it is whole. A kernel watch on its directory judges the record once armed and after
 * every entry change there (specs/cowshed/08_testing.md, "Fixtures"), so a record published before the watch arms is
 * still found. Bun's fs.watch arms after it returns, and saw such a record only through a later write to the log.
 */
function recordWritten(workspace: string): Promise<number> {
  return whenFound(join(workspace, '.nx/workspace-data/d'), () => wholeRecordPid(workspace));
}

async function fixtureWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-start-never', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
}

/** Not disabled for the checkout: the marker only `nx reset` clears would follow it into every terminal. */
async function expectNotDisabled(workspace: string, label: string): Promise<void> {
  await expect(readFile(join(workspace, '.nx/workspace-data/d/disabled'), 'utf8'), label).rejects.toMatchObject({
    code: 'ENOENT',
  });
}

/**
 * Run `nx show projects --json`, require it to list the fixture's one project, and return everything it printed. The
 * format is asked for, never inferred: unasked, Nx prints JSON only when it detects an AI agent in the environment
 * (`show/command-object.js`, `isAiAgent`) and a plain name per line otherwise. The list is stdout's last line: a note
 * Nx prints about its daemon goes to stdout before it.
 */
function showProjects(workspace: string, env: Record<string, string>, label: string): string {
  const ran = spawnSync('bun', [nxEntry, 'show', 'projects', '--json'], { cwd: workspace, env, encoding: 'utf8' });
  const output = `${ran.stdout}${ran.stderr}`;
  expect(ran.status, `${label}: ${output}`).toBe(0);
  expect(JSON.parse(ran.stdout.trimEnd().split('\n').at(-1) ?? ''), `${label}: ${output}`).toEqual(['p']);
  return output;
}

/** Start the fixture's daemon with a client allowed to, under `runtime`, and return the pid its record names. */
async function startDaemon(
  workspace: string,
  env: Record<string, string>,
  runtime: string,
  label: string,
): Promise<number> {
  const ran = spawnSync(runtime, [nxEntry, 'show', 'projects', '--json'], { cwd: workspace, env, encoding: 'utf8' });
  expect(ran.status, `${label}: ${ran.stdout}${ran.stderr}`).toBe(0);
  const daemon = await recordedPid(workspace);
  if (daemon === null) throw new Error(`${label}: a client allowed to start a daemon left no record`);
  return daemon;
}

/** `nx run-many -t build` under the job rule; requires it to succeed with `q:build` built from `p:build`. */
async function runBothUnderNever(
  workspace: string,
  env: Record<string, string>,
  runtime: string,
  label: string,
): Promise<string> {
  const run = spawnSync(runtime, [nxEntry, 'run-many', '-t', 'build', '--outputStyle=static'], {
    cwd: workspace,
    env: { ...env, NX_DAEMON_START: 'never' },
    encoding: 'utf8',
  });
  const printed = `${run.stdout}${run.stderr}`;
  expect(run.status, `${label}: ${printed}`).toBe(0);
  expect(await readFile(join(workspace, taskOutput('q')), 'utf8'), `${label}: q:build built from p:build`).toBe('p\n');
  await expectNotDisabled(workspace, label);
  return printed;
}

/**
 * `p:build` runs `first` and writes its output. Each step writes its own command: the command is part of the task's
 * hash, so the step's run misses the cache and runs the task.
 */
async function writeBuildOfP(workspace: string, first: string): Promise<void> {
  await writeFile(
    join(workspace, 'p', 'project.json'),
    JSON.stringify({
      name: 'p',
      targets: { build: build(`${first}; mkdir -p p/dist && echo p > ${taskOutput('p')}`) },
    }),
  );
}

/**
 * Two cacheable tasks: `p:build` (set by each step, see `writeBuildOfP`) writes its output, and `q:build` copies it.
 * `q:build`'s inputs are `p:build`'s outputs, so it is hashed only once `p:build` has finished, and whatever the step's
 * command did to the daemon is behind every daemon request after it: that hash, recording outputs, the post-run hooks.
 */
async function twoTaskWorkspace(workspace: string): Promise<void> {
  await fixtureWorkspace(workspace);
  await mkdir(join(workspace, 'q'));
  await writeFile(
    join(workspace, 'q', 'project.json'),
    JSON.stringify({
      name: 'q',
      implicitDependencies: ['p'],
      targets: {
        build: {
          ...build(`mkdir -p q/dist && cp ${taskOutput('p')} ${taskOutput('q')}`),
          dependsOn: ['^build'],
          inputs: ['default', { dependentTasksOutputFiles: '**/*.txt' }],
        },
      },
    }),
  );
}

/**
 * One fixture, one walk over the daemon states of a client that may never start one. The record and the daemon table
 * are the walk's state: each step reads what the steps before it left, so the restart comes first, from no daemon and
 * no record.
 */
it('the NX_DAEMON_START=never daemon states walk one fixture', async () => {
  await withNxFixture('nx-daemon-start-never', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    const never = { ...env, NX_DAEMON_START: 'never' };

    // A restarting daemon opens the log twice for its successor's stdout and stderr. Left to garbage collection, Node
    // >=26 turns each collected `FileHandle` into a fatal `ERR_INVALID_STATE`, which ended a restarting daemon in a job
    // with the socket its clients were still writing to. Collected deliberately here, right after the restart's spawn,
    // so the descriptors' lifetime decides the exit rather than when a collection happens to run. The successor is the
    // fixture's to stop: its record is awaited, then it is stopped, before any step that reads the record.
    const shutdownUtils = join(repositoryRoot, 'node_modules/nx/dist/src/daemon/server/shutdown-utils.js');
    const restart = `const { setTimeout: sleep } = require('node:timers/promises');
    require(${JSON.stringify(shutdownUtils)}).startNewDaemonInBackground().then(async () => {
      for (let round = 0; round < 3; round++) { gc(); await sleep(50); }
    })`;
    const restarted = spawnSync('node', ['--expose-gc', '-e', restart], { cwd: workspace, env, encoding: 'utf8' });
    expect(restarted.status, `${cases.descriptors}: ${restarted.stdout}${restarted.stderr}`).toBe(0);
    expect(restarted.stderr, `${cases.descriptors}: no ERR_INVALID_STATE`).not.toContain('ERR_INVALID_STATE');
    // The restarted daemon's record, within the 60 s the poll this replaces allowed.
    await guardEvent(
      recordWritten(workspace),
      `${cases.descriptors}: the restarted daemon writes its record`,
      () => readFile(daemonLog(workspace), 'utf8'),
      undefined,
      60_000,
    );
    await stopFixtureNxDaemon(workspace, env);

    // No daemon runs: the command is computed in the client, and no daemon is started.
    showProjects(workspace, never, `${cases.never}: no daemon`);
    expect(await recordedPid(workspace), `${cases.never}: no daemon is started`).toBeNull();

    // An explicit `nx daemon --start` calls the daemon start directly, past the implicit-start check above, so under
    // the job's rule it is refused as policy: a non-zero exit naming `NX_DAEMON_START=never` and the fixture escape,
    // and no daemon.
    const refused = spawnSync('bun', [nxEntry, 'daemon', '--start'], {
      cwd: workspace,
      env: never,
      encoding: 'utf8',
    });
    const said = `${refused.stdout}${refused.stderr}`;
    expect(refused.status, `${cases.explicit}: ${said}`).toBe(1);
    expect(said, `${cases.explicit}: names the rule`).toContain(
      'NX_DAEMON_START=never forbids this client starting a daemon',
    );
    expect(said, `${cases.explicit}: names the fixture escape`).toContain('FIXTURE_STARTS_ITS_OWN_DAEMON');
    expect(await recordedPid(workspace), `${cases.explicit}: no daemon record`).toBeNull();

    // Control: the same client without the variable starts one, so the absence above is the variable's doing.
    showProjects(workspace, env, `${cases.never}: control`);
    const daemon = await recordedPid(workspace);
    if (daemon === null) throw new Error('a client allowed to start a daemon left no record');
    expect(isRunning(daemon), `${cases.never}: control daemon runs`).toBe(true);

    // A daemon runs: the client uses it, and it is still the one the record names.
    const before = (await readFile(daemonLog(workspace), 'utf8')).length;
    showProjects(workspace, never, `${cases.never}: uses the running daemon`);
    expect(await recordedPid(workspace), `${cases.never}: same daemon`).toBe(daemon);
    expect((await readFile(daemonLog(workspace), 'utf8')).length, `${cases.never}: daemon log grew`).toBeGreaterThan(
      before,
    );

    // A record whose daemon has gone (killed, so it never cleared its record) is no daemon to use and no reason to
    // start one.
    const killed = await processesOf([daemon]);
    terminate(daemon, 'SIGKILL');
    await awaitExit(killed, 'the killed fixture daemon');
    showProjects(workspace, never, `${cases.never}: dead record`);
    expect(await recordedPid(workspace), `${cases.never}: dead record kept, none started`).toBe(daemon);

    // A record naming a live process that serves no socket: a daemon that stopped answering after the client chose to
    // use it. The checkout's daemon is not disabled for it.
    const holder = spawn('sleep', ['300'], { stdio: 'ignore' });
    const held = once(holder, 'exit');
    try {
      const pid = holder.pid;
      if (pid === undefined) throw new Error('sleep did not start');
      await writeFile(
        record(workspace),
        JSON.stringify({ processId: pid, socketPath: join(socketDirectory, 'gone.sock'), nxVersion }),
      );
      const continued = showProjects(workspace, never, `${cases.gone}: continues`);
      expect(continued, `${cases.gone}: names the rule`).toContain(
        'NX_DAEMON_START=never: this client never starts a daemon',
      );
      expect(await recordedPid(workspace), `${cases.gone}: record names the stopped daemon`).toBe(pid);
      await expectNotDisabled(workspace, cases.gone);
    } finally {
      holder.kill('SIGKILL');
    }
    await held;

    // An explicit start without the variable, which is what a test's named fixture escape runs, starts one.
    const started = spawnSync('bun', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, `${cases.explicit}: started: ${started.stdout}${started.stderr}`).toBe(0);
    const explicit = await recordedPid(workspace);
    if (explicit === null) throw new Error('an explicit start without the variable left no daemon record');
    expect(isRunning(explicit), `${cases.explicit}: the started daemon runs`).toBe(true);
  });
});

/**
 * One fixture, two runs of the same two tasks under the job rule, one after the other: the daemon stops under the first
 * and restarts under the second. The second run's daemon is one its own step started, after the first step left the
 * record naming a daemon that is gone.
 */
it('a run whose daemon stops under it, then one whose daemon restarts under it, walk one fixture', async () => {
  await withNxFixture('nx-daemon-start-run', async ({ workspace, socketDirectory }) => {
    await twoTaskWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };

    // The daemon stops while a run uses it, after the graph came from it: `p:build` kills it, and every daemon request
    // after that goes to a daemon that is gone. Each continues in the client, the run finishes, and nothing is disabled
    // or started.
    await writeBuildOfP(workspace, 'kill -9 $STOP_DAEMON; while kill -0 $STOP_DAEMON 2>/dev/null; do sleep 0.05; done');
    const daemon = await startDaemon(workspace, env, 'bun', cases.midrun);
    const printed = await runBothUnderNever(workspace, { ...env, STOP_DAEMON: String(daemon) }, 'bun', cases.midrun);
    expect(isRunning(daemon), `${cases.midrun}: the daemon stopped`).toBe(false);
    expect(printed, `${cases.midrun}: continued in the client`).toContain(
      'NX_DAEMON_START=never: this client never starts a daemon. Continuing without the daemon.',
    );
    // No successor: the record still names the daemon the task killed.
    expect(await recordedPid(workspace), `${cases.midrun}: no successor`).toBe(daemon);

    // The daemon restarts itself while a run uses it: `p:build` writes a lockfile, so the next request finds the daemon
    // outdated, and it starts its successor and ends the request's socket. The client waits for that successor and
    // resumes on it, as the job rule allows: its starter is the restarting daemon, not the client. The run finishes.
    // A lockfile Nx parses: an unparsable one fails the successor's graph, which is a different failure.
    const lockfile = JSON.stringify({
      name: 'daemon-start-never',
      lockfileVersion: 3,
      requires: true,
      packages: { '': { name: 'daemon-start-never' } },
    });
    await writeBuildOfP(workspace, `echo '${lockfile}' > package-lock.json`);
    await startDaemon(workspace, env, 'node', cases.restart);
    await runBothUnderNever(workspace, env, 'node', cases.restart);
    expect(await readFile(daemonLog(workspace), 'utf8'), `${cases.restart}: the successor restarted`).toContain(
      'Restarting daemon',
    );
  });
});

it(cases.fresh, async () => {
  await withNxFixture('nx-start-never-fresh-', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    await writeFile(
      join(workspace, 'p', 'project.json'),
      JSON.stringify({ name: 'p', targets: { build: build('echo built') } }),
    );
    await writeFile(join(workspace, 'probe.cjs'), FRESH_PROBE);
    const env = fixtureNxEnv(workspace, socketDirectory);
    const ran = spawnSync('node', ['probe.cjs', join(repositoryRoot, 'node_modules/nx')], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    const output = `${ran.stdout}${ran.stderr}`;
    expect(ran.status, `${cases.fresh}: ${output}`).toBe(0);
    const hashes = JSON.parse(ran.stdout.trimEnd().split('\n').at(-1) ?? '');
    expect(hashes.continued, `${cases.fresh}: ${output}`).toBe(hashes.fresh);
  });
});

it(cases.requests, async () => {
  await withNxFixture('nx-start-never-requests-', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    await writeFile(
      join(workspace, 'p', 'project.json'),
      JSON.stringify({
        name: 'p',
        targets: { build: { ...build('cat p/src.txt'), inputs: ['{projectRoot}/src.txt'] } },
      }),
    );
    await writeFile(join(workspace, 'p', 'src.txt'), 'one\n');
    await writeFile(join(workspace, 'probe.cjs'), REQUEST_PROBE);
    const env = fixtureNxEnv(workspace, socketDirectory);
    for (const request of ['capabilities', 'affected']) {
      const ran = spawnSync('node', ['probe.cjs', join(repositoryRoot, 'node_modules/nx'), request], {
        cwd: workspace,
        env,
        encoding: 'utf8',
      });
      const output = `${ran.stdout}${ran.stderr}`;
      const label = `${cases.requests}: ${request}`;
      expect(ran.status, `${label}: ${output}`).toBe(0);
      // The client says it continues without the daemon, and the daemon stays off for the rest of its process.
      expect(output, label).toContain('The daemon stopped under this run. Continuing without the daemon.');
      expect(JSON.parse(ran.stdout.trimEnd().split('\n').at(-1) ?? ''), `${label}: ${output}`).toEqual({
        answered: true,
        daemonOn: false,
      });
    }
  });
});
