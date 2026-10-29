import { afterAll, describe, expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { cp, readFile, symlink, writeFile } from 'node:fs/promises';
import { constants } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { AUTOMATIC_INTERVAL_MS, RETENTION_RECEIPT } from '@smoothbricks/cargo-retain';
import {
  backdate,
  build,
  cargoEnv,
  churnFixture,
  churnGenerations,
  denyPreload,
  detachedPassPreload,
  exists,
  expectFresh,
  future,
  inFixture,
  metasOf,
  parseReport,
  planned,
  put,
  RETENTION_LOCK,
  reclaimPaths,
  relock,
  retain,
  scopedTo,
  withEnv,
  writeMid,
} from '@smoothbricks/cargo-retain/testing/cargo-fixture';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import {
  EXTRACTED_BINARIES_METADATA,
  EXTRACTED_CARGO_METADATA,
  EXTRACTED_TARGET_DIR,
  NEXTEST_ARCHIVE_KEY_SUFFIX,
} from '@smoothbricks/nextest-extract';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { testFileInstallation } from './__tests__/owned-installation.js';

useFixtureCargoHome();

/** The Nx and compiled plugin under test, one copy this file's Nx fixture shares. */
const installation = testFileInstallation();
afterAll(() => installation.retire());

/** The retention hook as Nx runs it; the selector's own cases live in @smoothbricks/cargo-retain. */
describe('cargo retention after real Nx runs', () => {
  it('a real Nx run reclaims after executed Cargo producer tasks, not after a noop, a fetch or a cache replay', async () => {
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
                'noop-only': { executor: 'nx:noop', cache: false },
                'fetch-only': {
                  executor: 'nx:run-commands',
                  cache: false,
                  options: { cwd: 'ws', command: 'cargo fetch --locked --offline' },
                },
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
                'build-fails': {
                  executor: 'nx:run-commands',
                  cache: false,
                  metadata: { cargoProducer: true },
                  options: { cwd: 'ws', command: 'cargo build --workspace --locked --offline && exit 3' },
                },
                'build-uncached': {
                  executor: 'nx:run-commands',
                  cache: false,
                  metadata: { cargoProducer: true },
                  options: { cwd: 'ws', command: 'cargo build --workspace --locked --offline' },
                },
              },
            }),
          );
          await put(join(fx.ws, 'marker.txt'), 'one\n');
          const gen = await churnGenerations(fx);
          await backdate(gen.superseded);
          const env = fixtureNxEnv(workspace, socketDirectory);
          const runNx = async (target: string, preload?: string, extraEnv: Record<string, string> = {}) => {
            const nx = join(workspace, 'node_modules', '.bin', 'nx');
            // A preload is a Node `--import`: it patches the process the hook runs in, which here is Node itself.
            const argv =
              preload === undefined
                ? [process.execPath, nx, 'run', target, '--outputStyle=static']
                : ['node', '--import', pathToFileURL(preload).href, nx, 'run', target, '--outputStyle=static'];
            const child = Bun.spawn(argv, {
              cwd: workspace,
              env: { ...env, ...extraEnv },
              stdout: 'pipe',
              stderr: 'pipe',
            });
            const [code, stdout, stderr] = await Promise.all([
              child.exited,
              new Response(child.stdout).text(),
              new Response(child.stderr).text(),
            ]);
            return { code, log: stdout + stderr };
          };
          // A due pass runs detached, after the run has exited; the preload, which the Node it runs in loads through
          // NODE_OPTIONS, says that process's exit into this FIFO, so the receipt is read once the pass has written it.
          const passExited = join(workspace, 'pass-exited.fifo');
          execFileSync('mkfifo', [passExited]);
          const passPreload = join(workspace, 'detached-pass.mjs');
          await put(passPreload, detachedPassPreload(passExited));
          const runNxAndPass = async (target: string) => {
            const ended = readFile(passExited, 'utf8');
            const run = await runNx(target, undefined, { NODE_OPTIONS: `--import=${pathToFileURL(passPreload).href}` });
            if (run.log.includes('its pass runs detached in pid')) return { ...run, pass: (await ended).trim() };
            // No pass started, so nothing will write the FIFO: this end lets the pending read go.
            await writeFile(passExited, '');
            await ended;
            return { ...run, pass: 'none started' };
          };
          const receiptPath = join(fx.target, RETENTION_RECEIPT);
          const noop = await runNx('ws:noop-only');
          expect(noop.code, noop.log).toBe(0);
          expect(await exists(receiptPath)).toBe(false);
          const fetched = await runNx('ws:fetch-only');
          expect(fetched.code, fetched.log).toBe(0);
          expect(await exists(receiptPath)).toBe(false);
          for (const path of gen.superseded) expect(await exists(path)).toBe(true);
          const built = await runNxAndPass('ws:build-cargo-fixture');
          expect({ code: built.code, pass: built.pass }, built.log).toEqual({ code: 0, pass: 'exit 0' });
          const receipt = parseReport(await readFile(receiptPath, 'utf8'));
          expect(reclaimPaths(planned(receipt))).toEqual(gen.superseded);
          for (const path of gen.superseded) expect(await exists(path)).toBe(false);
          await expectFresh(fx.ws);
          // A second toggle leaves backdated superseded generations again; a cache replay must not touch them.
          const current = [...(await metasOf(fx.target, 'ret-mid')), ...(await metasOf(fx.target, 'ret-app'))].sort();
          await writeMid(fx.ws, false, '0.1');
          relock(fx.ws);
          await build(fx.ws);
          if (current.length === 0) throw new Error('the second toggle left no generation to supersede');
          await backdate(current);
          const receiptBefore = await readFile(receiptPath, 'utf8');
          const replayed = await runNx('ws:build-cargo-fixture');
          expect(replayed.code, replayed.log).toBe(0);
          // The replay keeps the untouched outputs in place (the Nx patch's outputs stamps).
          expect(replayed.log).toContain('existing outputs match the cache');
          expect(await readFile(receiptPath, 'utf8')).toBe(receiptBefore);
          for (const path of current) expect(await exists(path)).toBe(true);
          const notDue = await runNx('ws:build-fails');
          expect(notDue.code, notDue.log).not.toBe(0);
          expect(notDue.log).toContain('automatic maintenance not due');
          expect(await readFile(receiptPath, 'utf8')).toBe(receiptBefore);
          for (const path of current) expect(await exists(path)).toBe(true);
          if (receipt.completed === undefined) throw new Error('successful hook did not record completion');
          // Advance eligibility through the receipt clock, without sleeping or changing Cargo's stamps.
          await writeFile(
            receiptPath,
            JSON.stringify({ ...receipt, completed: { ...receipt.completed, at: Date.now() - AUTOMATIC_INTERVAL_MS } }),
          );
          const failed = await runNxAndPass('ws:build-fails');
          expect({ failed: failed.code !== 0, pass: failed.pass }, failed.log).toEqual({
            failed: true,
            pass: 'exit 0',
          });
          const receipt2 = parseReport(await readFile(receiptPath, 'utf8'));
          expect(reclaimPaths(planned(receipt2))).toEqual(current);
          for (const path of current) expect(await exists(path)).toBe(false);
          await expectFresh(fx.ws);
          // A sandbox that refuses the lock by every route its platform has: the run still succeeds, and the hook says
          // why it skipped retention. The EPERM is thrown in a real Node, which is what throws it from spawn().
          if (receipt2.completed === undefined) throw new Error('second successful hook did not record completion');
          const preload = join(workspace, 'deny-locks.mjs');
          await put(preload, denyPreload(['perl'], true));
          await writeFile(
            receiptPath,
            JSON.stringify({
              ...receipt2,
              completed: { ...receipt2.completed, at: Date.now() - AUTOMATIC_INTERVAL_MS },
            }),
          );
          const dueReceipt = await readFile(receiptPath, 'utf8');
          const skipped = await runNx('ws:build-uncached', preload);
          expect(skipped.code, skipped.log).toBe(0);
          const refusal =
            process.platform === 'darwin'
              ? `open '${join(fx.target, RETENTION_LOCK)}': errno ${constants.errno.EPERM}: operation not permitted`
              : 'cannot run perl: spawn EPERM';
          expect(skipped.log).toContain(
            `Cargo retention ${fx.target}: refused, ${fx.target}: cannot attempt the retention lock: ${refusal}; nothing was changed`,
          );
          expect(await readFile(receiptPath, 'utf8')).toBe(dueReceipt);
          // Whatever else escapes a pass is reported as well, never rethrown: here cargo itself cannot be spawned.
          await put(preload, denyPreload(['cargo'], false));
          const escaped = await runNx('ws:build-uncached', preload);
          expect(escaped.code, escaped.log).toBe(0);
          expect(escaped.log).toMatch(/Cargo retention for .*: unexpected failure: .*EPERM/s);
          expect(await readFile(receiptPath, 'utf8')).toBe(dueReceipt);
        },
      );
    });
  });

  it('a cache-restored nextest archive still runs green after retention reclaims generations', async () => {
    await inFixture(async (root) => {
      const fx = await churnFixture(root, false);
      await scopedTo(fx.target, async () => {
        const gen = await churnGenerations(fx);
        const archive = join(fx.ws, 'archive.tar.zst');
        const archived = spawnSync(
          'cargo',
          ['--frozen', 'nextest', 'archive', '--archive-file', archive, '--workspace', '--user-config-file', 'none'],
          { cwd: fx.ws, env: cargoEnv(), encoding: 'utf8' },
        );
        expect({ status: archived.status, stderr: archived.status === 0 ? '' : archived.stderr }).toEqual({
          status: 0,
          stderr: '',
        });
        // The outputs an Nx cache restore would hand back, in a fresh place: the archive and its task's key.
        const restored = join(root, 'restored.tar.zst');
        await cp(archive, restored);
        await writeFile(`${restored}${NEXTEST_ARCHIVE_KEY_SUFFIX}`, '424242\n');
        const applied = planned(await retain(fx.ws, { apply: true, now: future() }));
        expect(reclaimPaths(applied)).toEqual(expect.arrayContaining(gen.superseded));
        const extracted = spawnSync(
          'node',
          [join(import.meta.dir, '../../nextest-extract/dist/ts/bin/smoo-nx-nextest-extract.js'), restored],
          { cwd: fx.ws, env: cargoEnv(), encoding: 'utf8' },
        );
        expect(extracted.status, extracted.stderr).toBe(0);
        const extractionDirectory = extracted.stdout.trim();
        const ran = spawnSync(
          'cargo',
          [
            '--frozen',
            'nextest',
            'run',
            '--binaries-metadata',
            join(extractionDirectory, EXTRACTED_BINARIES_METADATA),
            '--cargo-metadata',
            join(extractionDirectory, EXTRACTED_CARGO_METADATA),
            '--target-dir-remap',
            join(extractionDirectory, EXTRACTED_TARGET_DIR),
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
