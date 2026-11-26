import { afterAll, describe, expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { RETENTION_RECEIPT } from '@smoothbricks/cargo-retain';
import {
  backdate,
  cargoEnv,
  churnFixture,
  churnGenerations,
  detachedPassPreload,
  exists,
  expectFresh,
  future,
  inFixture,
  parseReport,
  planned,
  put,
  reclaimPaths,
  retain,
  scopedTo,
  withEnv,
} from '@smoothbricks/cargo-retain/testing/cargo-fixture';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { testFileInstallation } from './__tests__/owned-installation.js';

useFixtureCargoHome();

/** The Nx and compiled plugin under test, one copy this file's Nx fixture shares. */
const installation = testFileInstallation();
afterAll(() => installation.retire());

/**
 * Smoke that the retention hook is wired into Nx's postTasksExecution and starts the real detached pass; trigger and
 * outcome tables live in cargo-retention-hook.test.ts and selector/cadence/lock cases in @smoothbricks/cargo-retain.
 */
describe('cargo retention after real Nx runs', () => {
  it('an executed Cargo producer task in a real Nx run starts the detached pass and reclaims superseded generations', async () => {
    await withNxFixture('cargo-retention-hook', async ({ workspace, socketDirectory }) => {
      await withEnv(
        {
          CARGO_TARGET_DIR: undefined,
          CARGO_BUILD_DIR: undefined,
          CARGO_BUILD_TARGET: undefined,
          RUSTC_WRAPPER: '',
          RUSTC_WORKSPACE_WRAPPER: '',
        },
        async () => {
          const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
          expect(git.exitCode, git.stderr.toString()).toBe(0);
          await symlink(await installation.nodeModules(), join(workspace, 'node_modules'), 'dir');
          await put(
            join(workspace, 'nx.json'),
            JSON.stringify({ cacheDirectory: '.nx/cache', plugins: ['@smoothbricks/nx-plugin'] }),
          );
          const fx = await churnFixture(workspace, false);
          await put(
            join(fx.ws, 'project.json'),
            JSON.stringify({
              name: 'ws',
              targets: {
                'build-cargo-fixture': {
                  executor: 'nx:run-commands',
                  cache: true,
                  metadata: { cargoProducer: true },
                  inputs: ['{projectRoot}/marker.txt'],
                  outputs: ['{projectRoot}/result.txt'],
                  options: {
                    cwd: 'ws',
                    command: 'cargo build --workspace --locked --offline && cp marker.txt result.txt',
                  },
                },
              },
            }),
          );
          await put(join(fx.ws, 'marker.txt'), 'one\n');
          const gen = await churnGenerations(fx);
          await backdate(gen.superseded);
          const env = { ...fixtureNxEnv(workspace, socketDirectory), PATH: cargoEnv().PATH ?? '' };
          // A due pass runs detached, after the run has exited; the preload, which the Node it runs in loads through
          // NODE_OPTIONS, says that process's exit into this FIFO, so the receipt is read once the pass has written it.
          const passExited = join(workspace, 'pass-exited.fifo');
          execFileSync('mkfifo', [passExited]);
          const passPreload = join(workspace, 'detached-pass.mjs');
          await put(passPreload, detachedPassPreload(passExited));
          const ended = readFile(passExited, 'utf8');
          const nx = join(workspace, 'node_modules', '.bin', 'nx');
          const child = Bun.spawn([process.execPath, nx, 'run', 'ws:build-cargo-fixture', '--outputStyle=static'], {
            cwd: workspace,
            env: { ...env, NODE_OPTIONS: `--import=${pathToFileURL(passPreload).href}` },
            stdout: 'pipe',
            stderr: 'pipe',
          });
          const [code, stdout, stderr] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
          ]);
          const log = stdout + stderr;
          let pass: string;
          if (log.includes('its pass runs detached in pid')) {
            pass = (await ended).trim();
          } else {
            // No pass started, so nothing will write the FIFO: this end lets the pending read go.
            await writeFile(passExited, '');
            await ended;
            pass = 'none started';
          }
          expect({ code, pass }, log).toEqual({ code: 0, pass: 'exit 0' });
          const receiptPath = join(fx.target, RETENTION_RECEIPT);
          const receipt = parseReport(await readFile(receiptPath, 'utf8'));
          expect(reclaimPaths(planned(receipt))).toEqual(gen.superseded);
          for (const path of gen.superseded) expect(await exists(path)).toBe(false);
          await expectFresh(fx.ws);
        },
      );
    });
  });

  it('the test build still runs in place after retention reclaims superseded generations', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        // The fixture's binaries stay in Cargo's target directory and the runner consumes the local metadata.
        const listed = spawnSync(
          'cargo',
          [
            '--frozen',
            'nextest',
            'list',
            '--workspace',
            '--list-type',
            'binaries-only',
            '--message-format',
            'json',
            '--user-config-file',
            'none',
          ],
          {
            cwd: fx.ws,
            env: cargoEnv(),
            encoding: 'utf8',
          },
        );
        expect({ status: listed.status, stderr: listed.status === 0 ? '' : listed.stderr }).toEqual({
          status: 0,
          stderr: '',
        });
        const binaries = join(root, 'binaries-metadata.json');
        await writeFile(binaries, listed.stdout);
        const workspace = spawnSync('cargo', ['--frozen', 'metadata', '--format-version', '1', '--all-features'], {
          cwd: fx.ws,
          env: cargoEnv(),
          encoding: 'utf8',
        });
        expect(workspace.status, workspace.stderr).toBe(0);
        const cargoMetadata = join(root, 'cargo-metadata.json');
        await writeFile(cargoMetadata, workspace.stdout);
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(expect.arrayContaining(gen.superseded));
        const ran = spawnSync(
          'cargo',
          [
            '--frozen',
            'nextest',
            'run',
            '--binaries-metadata',
            binaries,
            '--cargo-metadata',
            cargoMetadata,
            '--target-dir-remap',
            fx.target,
            '--workspace-remap',
            '.',
            '--user-config-file',
            'none',
            '--color',
            'never',
          ],
          { cwd: fx.ws, env: cargoEnv(), encoding: 'utf8' },
        );
        expect({ status: ran.status, passed: (ran.stderr + ran.stdout).includes('1 passed') }).toEqual({
          status: 0,
          passed: true,
        });
      });
    });
  });
});
