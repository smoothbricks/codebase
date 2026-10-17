#!/usr/bin/env bun
/** Uncached host proof: run its declared Nx target from the checkout root, outside the bounded Bun shards. */
import { mkdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { connect, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import type { DaemonProcessJson } from 'nx/src/daemon/cache';
import typia from 'typia';
import { findNxWorkspaceRoot } from '../packages/nx-plugin/src/ensure-built.js';
import {
  awaitExit,
  isRunning,
  NX_LONGEST_SOCKET_NAME,
  ownedFixtureRoot,
  ownedSocketDirectory,
  processTable,
  stopNxDaemon,
  withDescendants,
} from '../packages/nx-plugin/src/testing.js';

const root = findNxWorkspaceRoot(process.cwd());
if (root === null) throw new Error('run the host proof from its owning Nx checkout');
const nx = join(root, 'node_modules', '.bin', 'nx');
const require = createRequire(import.meta.url);
const installedPolicy = typia.assert<{ SERVER_INACTIVITY_TIMEOUT_MS: number }>(
  require('nx/src/daemon/server/shutdown-utils.js'),
);
const timeoutMs = installedPolicy.SERVER_INACTIVITY_TIMEOUT_MS;
if (timeoutMs !== 300_000) throw new Error(`the installed Nx inactivity policy is ${timeoutMs}, not 300000ms`);
const implementation = await realpath(dirname(require.resolve('nx/package.json')));
const parseRecord = typia.json.createAssertParse<DaemonProcessJson>();
const workspace = await ownedFixtureRoot(tmpdir(), 'nx-daemon-idle-proof', 'connected-');
const sockets = await ownedSocketDirectory('ni-', join('r', 'nx', NX_LONGEST_SOCKET_NAME));
const socketDirectory = join(sockets, 'r', 'nx');
const env = {
  ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_'))),
  NX_WORKSPACE_ROOT_PATH: workspace,
  NX_WORKSPACE_DATA_DIRECTORY: join(workspace, '.nx', 'workspace-data'),
  NX_CACHE_DIRECTORY: join(workspace, '.nx', 'cache'),
  NX_SOCKET_DIR: socketDirectory,
  NX_DAEMON: 'true',
  NX_USE_LOCAL: 'true',
};
const recordPath = join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json');
const logPath = join(workspace, '.nx', 'workspace-data', 'd', 'daemon.log');
let connection: Socket | undefined;
let departed = false;

// A graph plugin that asks its daemon for files, as globIndexedFiles does. The daemon keeps its isolated worker for as
// long as it lives, and the worker's daemon client is a connection to that daemon while it waits on a reply.
const DAEMON_GLOB_PLUGIN = `const { globWithWorkspaceContext } = require('nx/src/utils/workspace-context');
const { workspaceRoot } = require('nx/src/utils/workspace-root');
exports.createNodesV2 = ['**/project.json', async () => {
  await globWithWorkspaceContext(workspaceRoot, ['**/project.json']);
  return [];
}];
`;

async function showProjects(): Promise<void> {
  const child = Bun.spawn(['bun', nx, 'show', 'projects', '--json'], {
    cwd: workspace,
    env,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code !== 0 || stdout.trim() !== '["p"]') throw new Error(`nx show projects exited ${code}: ${stdout}${stderr}`);
}

async function daemonCommand(command: '--start' | '--stop'): Promise<void> {
  const child = Bun.spawn(['bun', nx, 'daemon', command], { cwd: workspace, env, stdout: 'pipe', stderr: 'pipe' });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code !== 0) throw new Error(`nx daemon ${command} exited ${code}: ${stdout}${stderr}`);
}

try {
  await writeFile(join(workspace, 'nx.json'), '{"plugins":["./daemon-glob-plugin.js"]}\n');
  await writeFile(join(workspace, 'daemon-glob-plugin.js'), DAEMON_GLOB_PLUGIN);
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), '{"name":"p"}\n');
  await writeFile(join(workspace, 'package.json'), '{"name":"connected-idle-proof","private":true}\n');
  await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\n');
  await symlink(join(root, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await mkdir(join(workspace, '.nx', 'run', 'nx'), { recursive: true });
  await symlink(join(workspace, '.nx', 'run'), join(sockets, 'r'), 'dir');
  const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
  if (git.exitCode !== 0) throw new Error(`git init exited ${git.exitCode}: ${git.stderr.toString()}`);
  await daemonCommand('--start');
  const record = parseRecord(await readFile(recordPath, 'utf8'));
  if (record.workspaceRoot !== workspace || !isRunning(record.processId)) {
    throw new Error(`the daemon record does not name the live owned workspace: ${JSON.stringify(record)}`);
  }
  const processes = (await processTable()).filter(({ pid }) => pid === record.processId);
  if (processes.length !== 1) throw new Error(`the kernel does not name daemon ${record.processId}`);
  await showProjects();
  const workers = withDescendants(await processTable(), [record.processId]).filter(({ command }) =>
    command.includes('plugin-worker'),
  );
  if (workers.length === 0) throw new Error(`daemon ${record.processId} keeps no plugin worker after a graph request`);
  const client = connect(record.socketPath);
  connection = client;
  await new Promise<void>((resolve, reject) => {
    client.once('connect', resolve);
    client.once('error', reject);
  });
  const connectedAt = performance.now();
  console.log(
    JSON.stringify({ phase: 'connected', workspace, implementation, record, timeoutMs, kernel: processes, workers }),
  );
  await sleep(timeoutMs + 10_000);
  const still = parseRecord(await readFile(recordPath, 'utf8'));
  const alive = (await processTable()).find(({ pid }) => pid === record.processId);
  if (client.destroyed || still.processId !== record.processId || alive === undefined || alive.stat.startsWith('Z')) {
    throw new Error(`the silent connected client lost daemon ${record.processId}: ${JSON.stringify({ still, alive })}`);
  }
  console.log(
    JSON.stringify({ phase: 'silent-connected-alive', elapsedMs: performance.now() - connectedAt, kernel: alive }),
  );
  const disconnectedAt = performance.now();
  await new Promise<void>((resolve) => {
    client.once('close', resolve);
    client.destroy();
  });
  connection = undefined;
  await sleep(timeoutMs - 10_000);
  const beforeDeadline = (await processTable()).find(({ pid }) => pid === record.processId);
  if (performance.now() - disconnectedAt >= timeoutMs) {
    throw new Error('the host delayed the before-deadline kernel observation past the idle boundary');
  }
  if (beforeDeadline === undefined || beforeDeadline.stat.startsWith('Z')) {
    throw new Error(`daemon ${record.processId} retired before five minutes after the last client disconnected`);
  }
  await awaitExit([...processes, ...workers], `owned idle daemon ${record.processId} and its plugin workers`, 15_000);
  const elapsedMs = performance.now() - disconnectedAt;
  const log = await readFile(logPath, 'utf8');
  const stopped = log
    .split('\n')
    .filter((line) => line.includes('Server stopped because:'))
    .at(-1);
  if (stopped === undefined || !stopped.includes('300000ms of inactivity') || elapsedMs > timeoutMs + 15_000) {
    throw new Error(`idle departure was not the expected five-minute boundary: ${elapsedMs}ms; ${stopped}`);
  }
  departed = true;
  console.log(
    JSON.stringify({
      phase: 'disconnected-idle-exited',
      elapsedMs,
      stopped,
      kernel: await processTable().then((table) => table.filter(({ pid }) => pid === record.processId)),
    }),
  );
} finally {
  connection?.destroy();
  if (!departed) await stopNxDaemon(workspace, () => daemonCommand('--stop'));
  await rm(sockets, { recursive: true });
  await rm(workspace, { recursive: true });
}
