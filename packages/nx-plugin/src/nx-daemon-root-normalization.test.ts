import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, lstatSync, readdirSync, readFileSync, readlinkSync, realpathSync } from 'node:fs';
import { copyFile, mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { dirname, join, relative } from 'node:path';
import typia from 'typia';
import type { DaemonProcessJson } from '../../../.cache/patched-nx/node_modules/nx/dist/src/daemon/cache.js';
import {
  assertNotForeignWorkspaceMessage,
  isForeignWorkspaceMessage,
  normalizeRootString,
} from '../../../.cache/patched-nx/node_modules/nx/dist/src/daemon/message-types/daemon-message.js';
import { sendMessage } from '../../../.cache/patched-nx/node_modules/nx/dist/src/daemon/socket-utils.js';
import {
  consumeMessagesFromSocket,
  parseMessage,
} from '../../../.cache/patched-nx/node_modules/nx/dist/src/utils/consume-messages-from-socket.js';
import { activatePackageProduct } from '../managed/raw/tooling/direnv/developer-links.js';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import type { PortablePackageProduct } from './managed-assets.js';

const repositoryRoot = join(import.meta.dir, '../../..');
// The stable installed namespace is activation-owned; the cached producer writes only tar/notes/product.json.
const artifactNxEntry = join(repositoryRoot, '.cache/patched-nx/node_modules/nx/dist/bin/nx.js');

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

/** Snapshot package bytes and link text without following immutable-store dependency links. */
function generationSnapshot(root: string) {
  const entries = new Map<string, { inode: number; modified: number; bytes: Buffer | string | null }>();
  const state = lstatSync(root);
  entries.set('.', { inode: state.ino, modified: state.mtimeMs, bytes: null });
  const visit = (directory: string) => {
    for (const name of readdirSync(directory).sort()) {
      const at = join(directory, name);
      const state = lstatSync(at);
      entries.set(relative(root, at), {
        inode: state.ino,
        modified: state.mtimeMs,
        bytes: state.isFile() ? readFileSync(at) : state.isSymbolicLink() ? readlinkSync(at) : null,
      });
      if (state.isDirectory()) visit(at);
    }
  };
  visit(root);
  return entries;
}

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
      const record = typia.json.assertParse<DaemonProcessJson>(await readFile(recordPath, 'utf8'));
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(await ping(record.socketPath, `${workspace}/`)).toBe(true);
      expect(await ping(record.socketPath, foreign)).toMatchObject({ error: expect.anything() });
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(typia.json.assertParse<DaemonProcessJson>(await readFile(recordPath, 'utf8'))).toEqual(record);
    },
    'workspace',
  );
});

it('keeps the old product generation daemon serving while a complete new generation loads its CLI and native closure', async () => {
  await withNxFixture(
    'nx-product-generation-',
    async ({ workspace }) => {
      await mkdir(join(workspace, 'node_modules'), { recursive: true });
      await writeFile(join(workspace, 'nx.json'), '{}\n');
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'generation-daemon', private: true }));
      const producer = join(repositoryRoot, '.cache/patched-nx');
      const source = typia.json.assertParse<PortablePackageProduct>(
        await readFile(join(producer, 'product.json'), 'utf8'),
      );
      expect(source.closure.packageName).toBe('nx');
      expect(source.closure.platform).toBe(process.platform);
      expect(source.closure.arch).toBe(process.arch);
      const native = source.closure.dependencies.find(
        (dependency) => dependency.direct && dependency.name.startsWith(`@nx/nx-${process.platform}-${process.arch}`),
      );
      if (!native) {
        throw new Error("the declared Nx producer closure must contain this install's real native optional package");
      }
      const directory = join(workspace, '.cache/patched-nx');
      await mkdir(directory, { recursive: true });
      await copyFile(join(producer, source.asset), join(directory, source.asset));
      const first: PortablePackageProduct = { ...source, identity: `${source.identity}-daemon-old` };
      await writeFile(join(directory, 'product.json'), JSON.stringify(first));
      expect(activatePackageProduct(workspace, directory)).toBe(true);
      const alias = join(directory, 'node_modules/nx');
      const oldGenerationRoot = join(directory, 'generations', first.identity);
      const oldGeneration = join(oldGenerationRoot, 'node_modules/nx');
      expect(realpathSync(alias)).toBe(oldGeneration);
      await symlink(alias, join(workspace, 'node_modules/nx'), 'dir');
      const entry = join(alias, 'dist/bin/nx.js');
      const env = { ...fixtureNxEnv(workspace), NX_DAEMON: 'true' };
      const started = spawnSync('node', [entry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
      expect(started.status, started.stdout + started.stderr).toBe(0);
      const recordPath = join(workspace, '.nx/workspace-data/d/server-process.json');
      const recordText = await readFile(recordPath, 'utf8');
      const record = typia.json.assertParse<DaemonProcessJson>(recordText);
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8')).toContain(
        `New daemon __dirname: ${join(oldGeneration, 'dist/src/daemon/server')}`,
      );
      const oldBytes = generationSnapshot(oldGenerationRoot);
      const oldAlias = { inode: lstatSync(alias).ino, target: readlinkSync(alias) };
      const refused: PortablePackageProduct = {
        ...source,
        identity: `${source.identity}-daemon-refused`,
        closure: {
          ...source.closure,
          dependencies: source.closure.dependencies.map((dependency) =>
            dependency === native ? { ...dependency, storePath: 'uninstalled-fixture-native-package' } : dependency,
          ),
        },
      };
      await writeFile(join(directory, 'product.json'), JSON.stringify(refused));
      expect(() => activatePackageProduct(workspace, directory)).toThrow();
      expect(existsSync(join(directory, 'generations', refused.identity))).toBe(false);
      expect({ inode: lstatSync(alias).ino, target: readlinkSync(alias) }).toEqual(oldAlias);
      expect(generationSnapshot(oldGenerationRoot)).toEqual(oldBytes);
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(await readFile(recordPath, 'utf8')).toBe(recordText);

      const next: PortablePackageProduct = { ...source, identity: `${source.identity}-daemon-new` };
      await writeFile(join(directory, 'product.json'), JSON.stringify(next));
      expect(activatePackageProduct(workspace, directory, true)).toBe(false);
      expect(realpathSync(alias)).toBe(oldGeneration);
      expect(activatePackageProduct(workspace, directory)).toBe(true);
      const newGenerationRoot = join(directory, 'generations', next.identity);
      const newGeneration = join(newGenerationRoot, 'node_modules/nx');
      expect(realpathSync(alias)).toBe(newGeneration);
      expect(readFileSync(join(newGenerationRoot, 'product.json'), 'utf8')).toBe(JSON.stringify(next));
      expect(readFileSync(join(newGeneration, 'dependency-closure.json'), 'utf8')).toBe(JSON.stringify(next.closure));
      const cache = Bun.spawnSync(['bun', 'pm', 'cache'], { cwd: workspace, stdout: 'pipe', stderr: 'pipe' });
      expect(cache.exitCode, new TextDecoder().decode(cache.stderr)).toBe(0);
      const store = realpathSync(new TextDecoder().decode(cache.stdout).trim());
      for (const dependency of next.closure.dependencies) {
        if (dependency.direct) {
          expect(realpathSync(join(newGeneration, 'node_modules', dependency.name))).toBe(
            join(store, dependency.storePath),
          );
        }
      }
      const loaded = spawnSync('node', [entry, '--version'], { cwd: workspace, env, encoding: 'utf8' });
      expect(loaded.status, loaded.stdout + loaded.stderr).toBe(0);
      expect(loaded.stdout).toContain('23.2.1');
      expect(realpathSync(entry)).toBe(join(newGeneration, 'dist/bin/nx.js'));
      const nativeLoaded = spawnSync(
        'node',
        [
          '-e',
          [
            "const { createRequire } = require('node:module');",
            'const fromGeneration = createRequire(process.argv[1]);',
            "const native = fromGeneration('./dist/src/native/index.js');",
            'const direct = fromGeneration(process.argv[2]);',
            "const hash = direct.hashArray(['generation-proof']);",
            "if (native.hashArray(['generation-proof']) !== hash) throw new Error('native binding mismatch');",
            "process.stdout.write(fromGeneration.resolve(process.argv[2]) + '\\n' + hash);",
          ].join('\n'),
          join(newGeneration, 'package.json'),
          native.name,
        ],
        { cwd: workspace, env, encoding: 'utf8' },
      );
      expect(nativeLoaded.status, nativeLoaded.stdout + nativeLoaded.stderr).toBe(0);
      const [nativePath, nativeHash] = nativeLoaded.stdout.split('\n');
      if (!nativePath || !nativeHash) {
        throw new Error('the actual new generation native load returned no package path or hash');
      }
      expect(realpathSync(dirname(nativePath))).toBe(join(store, native.storePath));
      expect(activatePackageProduct(workspace, directory, true)).toBe(true);
      expect(generationSnapshot(oldGenerationRoot)).toEqual(oldBytes);
      expect(await ping(record.socketPath, workspace)).toBe(true);
      expect(await ping(record.socketPath, `${workspace}/`)).toBe(true);
      expect(await readFile(recordPath, 'utf8')).toBe(recordText);
      expect(typia.json.assertParse<DaemonProcessJson>(await readFile(recordPath, 'utf8'))).toEqual(record);
    },
    'workspace',
  );
});
