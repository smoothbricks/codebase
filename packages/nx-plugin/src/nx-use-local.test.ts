import { expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { chmod, mkdir, readFile, symlink, unlink, writeFile } from 'node:fs/promises';
import { delimiter, join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the `latest-nx.js` and `bin/nx.js` hunks of `patches/nx@23.2.1.patch`. Stock Nx 23.2.1 answers the Nx
 * Console and AI-agent status every `nx run` asks its daemon for by installing `nx@latest` from the registry into a
 * temporary directory (`npm install`), unless the environment the client sent the daemon says `NX_USE_LOCAL=true`.
 * Agents, cowshed jobs and test fixtures run Nx without that variable, and the daemon took it from whichever client
 * spoke last. The patched daemon resolves its own installed Nx instead, whatever a client sends, and the patched
 * `nx` entry defaults the variable for every command it runs.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const PACKAGE_MANAGERS = ['npm', 'npx', 'pnpm', 'yarn'];

async function run(
  command: string,
  args: string[],
  cwd: string,
  env: Record<string, string>,
  expectedCode = 0,
): Promise<string> {
  const child = spawn(command, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  child.stdout?.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  child.stderr?.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  // The test waits for what it needs, the command's close, however long a loaded host makes the real Nx take: a
  // deadline here measured the host, not the daemon's behaviour (a 15 s guard failed SB land gates at load 3 and 80
  // with `oracle-control` still running). A command that never closes is the runner's idle-hang backstop's to report.
  const code = await new Promise<number | null>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });
  expect(code, output).toBe(expectedCode);
  return output;
}

it('starts no package manager from a daemon whose clients do not set NX_USE_LOCAL, through nx or the library', async () => {
  await withNxFixture('nx-use-local', async ({ root, workspace, socketDirectory }) => {
    // Every package manager Nx could install `nx@latest` with records that it ran, and fails.
    const fakes = join(root, 'package-managers');
    const marker = join(root, 'package-manager-ran');
    await mkdir(fakes);
    for (const name of PACKAGE_MANAGERS) {
      await writeFile(join(fakes, name), `#!/bin/sh\necho "${name} $*" >> '${marker}'\nexit 1\n`);
      await chmod(join(fakes, name), 0o755);
    }
    await mkdir(join(root, 'home'));
    await mkdir(join(workspace, 'app'));
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\nhome\npackage-managers\npackage-manager-ran\n');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'nx-use-local-fixture', private: true }));
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await writeFile(
      join(workspace, 'app/project.json'),
      JSON.stringify({
        name: 'app',
        targets: { build: { executor: 'nx:run-commands', options: { command: 'true' } } },
      }),
    );
    const { NX_USE_LOCAL: _local, ...withoutLocal } = fixtureNxEnv(workspace, socketDirectory);
    const env = {
      ...withoutLocal,
      HOME: join(root, 'home'),
      NX_DAEMON: 'true',
      PATH: `${fakes}${delimiter}${process.env.PATH ?? ''}`,
    };

    const controls: string[] = [];
    for (const name of PACKAGE_MANAGERS) {
      await run(name, ['oracle-control'], workspace, env, 1);
      controls.push(`${name} oracle-control`);
    }
    expect(await readFile(marker, 'utf8')).toBe(`${controls.join('\n')}\n`);
    await unlink(marker);

    // A library client (smoo, cowshed's task hashing, ensureBuilt) without the variable starts the daemon, which
    // computes Nx Console's and the AI agents' status in the environment it started with.
    const library = `
      const client = require.resolve('nx/src/daemon/client/client.js', { paths: [${JSON.stringify(repositoryRoot)}] });
      const { daemonClient } = require(client);
      (async () => {
        await daemonClient.getNxConsoleStatus();
        await daemonClient.getConfigureAiAgentsStatus();
        process.exit(0);
      })().catch((error) => { console.error(error); process.exit(1); });
    `;
    await run('node', ['-e', library], workspace, env);
    // The bare entry, as `node_modules/.bin/nx` runs it, asks the same daemon again at the end of a run.
    await run('node', [join(repositoryRoot, 'node_modules/.bin/nx'), 'run', 'app:build'], workspace, env);

    const log = await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8');
    // The daemon took the branch that pulled `nx@latest` from the registry; it now resolves the installed Nx.
    expect(log).toContain('[NX-CONSOLE]: Console status check completed');
    expect(log).not.toContain('[LATEST-NX]: Pulling');
    await expect(readFile(marker, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
  });
});
