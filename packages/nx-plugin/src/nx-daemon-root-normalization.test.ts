import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { join } from 'node:path';
import typia from 'typia';
import {
  assertNotForeignWorkspaceMessage,
  isForeignWorkspaceMessage,
  normalizeRootString,
} from '../../../.cache/patched-nx/package/dist/src/daemon/message-types/daemon-message.js';
import { sendMessage } from '../../../.cache/patched-nx/package/dist/src/daemon/socket-utils.js';
import {
  consumeMessagesFromSocket,
  parseMessage,
} from '../../../.cache/patched-nx/package/dist/src/utils/consume-messages-from-socket.js';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

const repositoryRoot = join(import.meta.dir, '../../..');
const artifactNxEntry = join(repositoryRoot, '.cache/patched-nx/package/dist/bin/nx.js');

interface DaemonRecord {
  processId: number;
  socketPath: string;
}

it('removes only redundant POSIX separators and preserves root, case and backslash components', () => {
  for (const [input, expected] of [
    ['/', '/'],
    ['///', '/'],
    ['/repo/', '/repo'],
    ['/repo/../repo//', '/repo'],
    ['/repo\\/', '/repo\\'],
  ]) {
    expect(normalizeRootString(input, 'linux')).toBe(expected);
  }
  expect(normalizeRootString('/repo', 'linux')).not.toBe(normalizeRootString('/REPO', 'linux'));
});

it('preserves absolute Windows drive and UNC roots while normalizing equivalent spellings', () => {
  for (const [input, expected] of [
    ['C:\\', 'c:\\'],
    ['C:/Repo//', 'c:\\repo'],
    ['C:\\Repo\\', 'c:\\repo'],
    ['\\\\SERVER\\SHARE', '\\\\server\\share\\'],
    ['\\\\SERVER\\SHARE\\', '\\\\server\\share\\'],
    ['\\\\SERVER\\SHARE\\Repo\\', '\\\\server\\share\\repo'],
    ['\\', '\\'],
  ]) {
    expect(normalizeRootString(input, 'win32')).toBe(expected);
  }
  expect(normalizeRootString('C:\\Repo', 'win32')).not.toBe(normalizeRootString('D:\\Repo', 'win32'));
  expect(normalizeRootString('\\\\one\\share\\', 'win32')).not.toBe(normalizeRootString('\\\\two\\share\\', 'win32'));
});

it('compares both roots canonically without admitting a genuinely foreign workspace', () => {
  expect(isForeignWorkspaceMessage({ type: 'PING', workspaceRoot: '/repo/' }, '/repo')).toBe(false);
  expect(isForeignWorkspaceMessage({ type: 'PING', workspaceRoot: '/repo' }, '/repo/')).toBe(false);
  expect(isForeignWorkspaceMessage({ type: 'PING', workspaceRoot: '/foreign/' }, '/repo')).toBe(true);
  expect(() => assertNotForeignWorkspaceMessage({ type: 'PING', workspaceRoot: '/foreign/' }, '/repo')).toThrow();
  expect(isForeignWorkspaceMessage({ type: 'PING' }, '/repo')).toBe(false);
});

async function ping(socketPath: string, workspaceRoot: string): Promise<unknown> {
  const socket = createConnection(socketPath);
  const { promise, resolve, reject } = Promise.withResolvers<unknown>();
  socket.once('error', reject);
  socket.once('close', () => reject(new Error('fixture daemon closed before answering')));
  socket.on(
    'data',
    consumeMessagesFromSocket((message) => resolve(parseMessage(message)), reject),
  );
  socket.once('connect', () => sendMessage(socket, { type: 'PING', workspaceRoot }, 'json'));
  try {
    return await guardEvent(
      promise,
      'the fixture daemon root RPC',
      () => `socket ${socketPath}; root ${workspaceRoot}`,
    );
  } finally {
    socket.destroy();
  }
}

it('accepts its real daemon RPC with a trailing root separator and refuses a genuinely foreign root', async () => {
  await withNxFixture(
    'nx-root-rpc-',
    async ({ root, workspace }) => {
      await mkdir(workspace);
      await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, 'nx.json'), '{}\n');
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'root-rpc', private: true }));
      const foreign = join(root, 'foreign');
      await mkdir(foreign);
      await writeFile(join(foreign, 'nx.json'), '{}\n');
      const started = spawnSync('node', [artifactNxEntry, 'daemon', '--start'], {
        cwd: workspace,
        env: { ...fixtureNxEnv(workspace), NX_DAEMON: 'true' },
        encoding: 'utf8',
      });
      expect(started.status, started.stdout + started.stderr).toBe(0);
      const recordPath = join(workspace, '.nx/workspace-data/d/server-process.json');
      const record = typia.json.assertParse<DaemonRecord>(await readFile(recordPath, 'utf8'));
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(await ping(record.socketPath, `${workspace}/`)).toBe(true);
      expect(await ping(record.socketPath, foreign)).toMatchObject({ error: expect.anything() });
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(typia.json.assertParse<DaemonRecord>(await readFile(recordPath, 'utf8'))).toEqual(record);
    },
    'workspace',
  );
});
