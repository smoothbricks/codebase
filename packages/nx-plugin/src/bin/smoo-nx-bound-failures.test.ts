import { expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { nxFixtureRoot } from '../__tests__/fixture-nx-env.js';
import { publishFixtureSocketDirectory, retireFixtureSocketDirectory } from '../testing.js';

/**
 * StoreRunInformationLifeCycle writes `run.json` to Nx's `cacheDir`. A checkout
 * that shares a cache does not have that file under `cacheDirectoryForWorkspace`,
 * so a reporter that reads the checkout-local path exits 2 with ENOENT and never
 * names the failed dependency. This runs a real `app:test` whose `lib:build`
 * dependency fails, then asks the reporter.
 */

const repositoryRoot = join(import.meta.dir, '../../../..');
const reporter = join(import.meta.dir, '../../dist/bin/smoo-nx-bound-failures.js');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const DEPENDENCY = 'lib:build';

it('names the failed dependency Nx recorded in cacheDir, not the checkout-local run.json', async () => {
  const root = await nxFixtureRoot('nx-bound-failures-cachedir-');
  const workspace = join(root, 'ws');
  const home = join(root, 'home');
  let published = false;
  try {
    await mkdir(join(workspace, 'lib'), { recursive: true });
    await mkdir(join(workspace, 'app'), { recursive: true });
    await mkdir(home, { recursive: true });
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await writeFile(
      join(workspace, 'package.json'),
      `${JSON.stringify({ name: 'bound-failures-fixture', private: true })}\n`,
    );
    await writeFile(
      join(workspace, 'lib', 'project.json'),
      `${JSON.stringify({
        name: 'lib',
        targets: { build: { executor: 'nx:run-commands', options: { command: 'false' } } },
      })}\n`,
    );
    await writeFile(
      join(workspace, 'app', 'project.json'),
      `${JSON.stringify({
        name: 'app',
        targets: {
          test: {
            executor: 'nx:run-commands',
            dependsOn: [DEPENDENCY],
            options: { command: 'true' },
          },
        },
      })}\n`,
    );
    execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
    execFileSync(
      'git',
      [
        '-C',
        workspace,
        '-c',
        'core.hooksPath=/dev/null',
        '-c',
        'commit.gpgsign=false',
        '-c',
        'user.email=fixture@example.com',
        '-c',
        'user.name=fixture',
        'commit',
        '--allow-empty',
        '-m',
        'fixture',
      ],
      { stdio: 'pipe' },
    );

    const socketDirectory = await publishFixtureSocketDirectory(workspace);
    published = true;
    const env = reporterEnv(workspace, home, socketDirectory);
    const cacheDir = nxPath("process.stdout.write(require('nx/src/utils/cache-directory.js').cacheDir)", env);
    const local = nxPath(
      [
        "const cache = require('nx/src/utils/cache-directory.js');",
        "const root = require('nx/src/utils/workspace-root.js').workspaceRoot;",
        'process.stdout.write(cache.cacheDirectoryForWorkspace(root));',
      ].join('\n'),
      env,
    );
    expect(cacheDir).not.toBe(local);
    const ran = spawnSync('bun', [nxEntry, 'run', 'app:test', '--outputStyle=static'], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    const nxOutput = `${ran.stdout}${ran.stderr}`;
    expect(ran.status, nxOutput).not.toBe(0);
    expect(existsSync(join(cacheDir, 'run.json')), `${cacheDir}\n${nxOutput}`).toBe(true);
    expect(existsSync(join(local, 'run.json'))).toBe(false);

    const reported = spawnSync('bun', [reporter], { cwd: workspace, env, encoding: 'utf8' });
    expect(reported.stdout, reported.stderr).toContain(DEPENDENCY);
    expect(reported.status, reported.stderr).toBe(1);
  } finally {
    if (published) await retireFixtureSocketDirectory(workspace);
    await rm(root, { recursive: true, force: true });
  }
});

function nxPath(script: string, env: NodeJS.ProcessEnv): string {
  const printed = spawnSync('node', ['-e', script], { cwd: repositoryRoot, env, encoding: 'utf8' });
  expect(printed.status, printed.stderr).toBe(0);
  return printed.stdout;
}

function reporterEnv(workspace: string, home: string, socketDirectory: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !key.startsWith('NX_')) env[key] = value;
  }
  env.HOME = home;
  env.NX_DAEMON = 'false';
  env.NX_SOCKET_DIR = socketDirectory;
  env.NX_WORKSPACE_ROOT_PATH = workspace;
  return env;
}
