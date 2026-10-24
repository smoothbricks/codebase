/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync } from 'node:fs';
import { chmod, cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import {
  GIT_HTTP_LOW_SPEED_LIMIT,
  GIT_HTTP_LOW_SPEED_TIME_S,
} from '../../../nx-plugin/managed/raw/tooling/direnv/cargo-fetch-bound.ts';
import { edit, HEALTHY, repository, withManagedRepository } from './__tests__/helpers/shell-entry.js';

describe('what shell entry fetches for a Cargo workspace', () => {
  // The Nx plugin resolves a Cargo workspace wherever a project's Cargo.toml declares
  // `[workspace]`: here the root and one member. Another member's plain package and the
  // root workspace's crate are not workspaces of their own.
  const CARGO_PROJECT = {
    'Cargo.toml': '[workspace]\nmembers = ["crates/*"]\n',
    'Cargo.lock': 'version = 4\n',
    'crates/core/Cargo.toml': '[package]\nname = "core"\n',
    'packages/native/package.json': JSON.stringify({ name: 'native', version: '0.0.0' }),
    'packages/native/Cargo.toml': '[workspace]\nmembers = ["."]\n\n[package]\nname = "native"\n',
    'packages/native/Cargo.lock': 'version = 4\n',
    'packages/plain/package.json': JSON.stringify({ name: 'plain', version: '0.0.0' }),
    'packages/plain/Cargo.toml': '[package]\nname = "plain"\n',
  };
  const fetch = (manifest: string) => ['fetch', '--locked', '--manifest-path', manifest];
  /** What Cargo itself writes at the root of CARGO_HOME: cowshed's STATE_FILES, the exact files it grants a sandbox. */
  const CARGO_STATE_FILES = ['.package-cache', '.package-cache-mutate', '.global-cache', '.global-cache-journal'];
  /** The stamps shell entry left in the private XDG cache: smoo's only record of a fetch. */
  const stamps = (xdgCache: string) => {
    const directory = join(xdgCache, 'smoo', 'cargo-fetched');
    return existsSync(directory) ? readdirSync(directory) : [];
  };

  // Shell entry's installs never fetch: devenv's smoo:cargo-fetch task runs `--cargo`, gated by `--cargo --check`.
  const CARGO = { cargo: true } as const;

  it('installs without fetching; the cargo task fetches each workspace once per change to what the fetch reads', async () => {
    await withManagedRepository(
      { workspaces: ['packages/*'], files: CARGO_PROJECT },
      async ({ root, xdgCache, enterShell: enter, cargoRuns }) => {
        expect(await enter()).toEqual(HEALTHY);
        expect(cargoRuns()).toEqual([]);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns()).toEqual([fetch('Cargo.toml'), fetch('packages/native/Cargo.toml')]);
        expect(stamps(xdgCache)).toHaveLength(2);

        await edit(join(root, 'Cargo.lock'), 'version = 4\n\n[[package]]\nname = "arrow-ipc"\n');
        expect(await enter(CARGO)).toEqual(HEALTHY);
        // Adding Cargo configuration changes what the fetch reads as much as editing it does.
        await mkdir(join(root, 'packages/native/.cargo'));
        await edit(join(root, 'packages/native/.cargo/config.toml'), '[net]\noffline = false\n');
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns().slice(2)).toEqual([fetch('Cargo.toml'), fetch('packages/native/Cargo.toml')]);
      },
    );
  });

  it('reports through --check whether CARGO_HOME holds every workspace, so a fresh one fetches', async () => {
    await withManagedRepository({ files: CARGO_PROJECT }, async (original) => {
      expect((await original.enterShell({ cargo: true, check: true })).exitCode).toBe(1);
      expect(await original.enterShell(CARGO)).toEqual(HEALTHY);
      expect(await original.enterShell({ cargo: true, check: true })).toEqual(HEALTHY);

      // A copy of the checkout sharing CARGO_HOME is fetched already.
      const clone = join(dirname(original.root), 'clone');
      await cp(original.root, clone, { recursive: true, verbatimSymlinks: true });
      const copied = repository(clone, join(dirname(original.root), 'ledgers'), join(dirname(original.root), 'bin'));
      expect(await copied.enterShell({ cargo: true, check: true })).toEqual(HEALTHY);

      await rm(original.cargoHome, { recursive: true, force: true });
      expect((await original.enterShell({ cargo: true, check: true })).exitCode).toBe(1);
      expect(await original.enterShell(CARGO)).toEqual(HEALTHY);
      expect(original.cargoRuns()).toEqual([fetch('Cargo.toml'), fetch('Cargo.toml')]);
    });
  });

  it('fetches again into a recreated CARGO_HOME and into another one, each keeping its own stamp', async () => {
    await withManagedRepository({ files: CARGO_PROJECT }, async ({ cargoHome, enterShell: enter, cargoRuns }) => {
      expect(await enter(CARGO)).toEqual(HEALTHY);
      // Another CARGO_HOME holds none of what the first one fetched.
      expect(await enter({ ...CARGO, cargoHome: `${cargoHome}-other` })).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(2);
      // Neither fetch spoiled the other's stamp.
      expect(await enter(CARGO)).toEqual(HEALTHY);
      expect(await enter({ ...CARGO, cargoHome: `${cargoHome}-other` })).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(2);

      // The same path, deleted and created again: a stamp outside CARGO_HOME survives it, so what
      // it promises must not.
      await rm(cargoHome, { recursive: true });
      await mkdir(cargoHome);
      expect(await enter(CARGO)).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(3);
      expect(await enter(CARGO)).toEqual(HEALTHY);
      expect(cargoRuns()).toHaveLength(3);
    });
  });

  it('keeps its stamp in the private XDG cache, entering a CARGO_HOME that is writable only where Cargo writes', async () => {
    await withManagedRepository(
      { files: CARGO_PROJECT },
      async ({ cargoHome, xdgCache, enterShell: enter, cargoRuns }) => {
        // CARGO_HOME as a cowshed sandbox grants it: Cargo's state files and its `registry` and
        // `git` caches are writable, and nothing else — no entry may be created at its root.
        await mkdir(join(cargoHome, 'registry'), { recursive: true });
        await mkdir(join(cargoHome, 'git'));
        for (const file of CARGO_STATE_FILES) {
          await writeFile(join(cargoHome, file), '');
        }
        await chmod(cargoHome, 0o555);
        try {
          // The fixture is only a model of the grant when the root really refuses a new entry.
          await expect(writeFile(join(cargoHome, 'smoo-fetched'), '')).rejects.toThrow('EACCES');

          expect(await enter(CARGO)).toEqual(HEALTHY);
          expect(await enter(CARGO)).toEqual(HEALTHY);
          expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
          expect(readdirSync(cargoHome).sort()).toEqual([...CARGO_STATE_FILES, 'git', 'registry'].sort());
          expect(stamps(xdgCache)).toHaveLength(1);
        } finally {
          await chmod(cargoHome, 0o755);
        }
      },
    );
  });

  it('falls back to ~/.cache/smoo without XDG_CACHE_HOME, leaving ~/.cargo to Cargo', async () => {
    await withManagedRepository({ files: CARGO_PROJECT }, async ({ root, enterShell: enter, cargoRuns }) => {
      const home = join(dirname(root), 'home');
      await mkdir(home);
      expect(await enter({ ...CARGO, home })).toEqual(HEALTHY);
      expect(await enter({ ...CARGO, home })).toEqual(HEALTHY);
      expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
      expect(readdirSync(join(home, '.cargo')).sort()).toEqual(['.package-cache', 'registry']);
      expect(readdirSync(join(home, '.cache', 'smoo'))).toEqual(['cargo-fetched']);
      expect(readdirSync(join(home, '.cache', 'smoo', 'cargo-fetched'))).toHaveLength(1);
    });
  });

  it("loads the shell when the fetch fails, naming Cargo's cause, and fetches again on the next entry", async () => {
    await withManagedRepository(
      { workspaces: ['packages/*'], files: { ...CARGO_PROJECT, 'cargo-fails': '' } },
      async ({ root, xdgCache, enterShell: enter, cargoRuns }) => {
        const failed = await enter(CARGO);
        expect(failed.exitCode).toBe(0);
        expect(failed.stderr).toContain('cargo fetch --locked --manifest-path Cargo.toml');
        expect(failed.stderr).toContain('failed to download from `https://index.crates.io/config.json`');
        expect((await enter(CARGO)).exitCode).toBe(0);
        // Two workspaces, and each fetch fails at once, so the ledger is written in event order: the step stopped at the
        // first failed fetch and never began the second.
        expect(cargoRuns()).toEqual([fetch('Cargo.toml'), fetch('Cargo.toml')]);
        // A fetch that failed promised nothing: no stamp, not even the directory for one.
        expect(existsSync(join(xdgCache, 'smoo'))).toBe(false);

        await rm(join(root, 'cargo-fails'));
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        // Both workspaces fetch and stamp once nothing fails.
        expect(cargoRuns()).toHaveLength(4);
        expect(stamps(xdgCache)).toHaveLength(2);
      },
    );
  });

  it('fails a CI fetch that fails', async () => {
    await withManagedRepository(
      { workspaces: ['packages/*'], files: { ...CARGO_PROJECT, 'cargo-fails': '' } },
      async ({ xdgCache, enterShell: enter, cargoRuns }) => {
        const entry = await enter({
          cargo: true,
          ciSecrets: { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' },
        });
        expect(entry.exitCode).toBe(1);
        expect(entry.stderr).toContain('failed to download from `https://index.crates.io/config.json`');
        // The step stopped at the failed fetch: the second workspace was never begun.
        expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
        expect(existsSync(join(xdgCache, 'smoo'))).toBe(false);
      },
    );
  });

  // A fetch whose git child stalls holds Cargo's package-cache lock until something ends it, so the step bounds it. The
  // bound is the case's subject: the fixture's cargo never answers, so only the deadline ends it. A bound that starts at
  // spawn races the stand-in's first instruction and no event orders that ahead of a clock, so under load the stand-in
  // can start after whatever deadline a case gives it. Nothing asserted here therefore depends on how far the stand-in
  // got before the kill, the deadline is the least there is, and the stand-in is one process that can leave nothing
  // behind wherever the kill lands. The case asserts what holds then: the entry returned (the bounded exit), said which
  // fetch it ended, past which deadline and holding which lock, and stamped nothing. That a live process tree dies with
  // its fetch is proved where events order it: with the clock held by hand and readiness through a pipe
  // (cargo-fetch-run.test.ts), and through this script by the signal cases below.
  const STALL_DEADLINE_MS = 1;
  for (const [mode, entry, exitCode] of [
    ['local', CARGO, 0],
    ['CI', { cargo: true, ciSecrets: { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' } }, 1],
  ] as const) {
    it(`ends a fetch that outlives its deadline and stamps nothing, in a ${mode} entry`, async () => {
      await withManagedRepository(
        { files: { ...CARGO_PROJECT, 'cargo-hangs': '' } },
        async ({ cargoHome, xdgCache, enterShell: enter }) => {
          const entered = await enter({ ...entry, deadlineMs: STALL_DEADLINE_MS });
          expect(entered.exitCode).toBe(exitCode);
          for (const fact of [
            'cargo fetch --locked --manifest-path Cargo.toml',
            `past its ${STALL_DEADLINE_MS} ms deadline`,
            join(cargoHome, '.package-cache'),
          ]) {
            expect(entered.stderr).toContain(fact);
          }
          expect(existsSync(join(xdgCache, 'smoo'))).toBe(false);
        },
      );
    });
  }

  // Cargo runs `git fetch` for git dependencies, and git reads its low-speed limit from these variables ahead of its
  // configuration: the fetch gets the step's limits whatever the entry carried, which is what ends a stalled transfer.
  // The recording cargo shows what reached it.
  it("gives each fetch the step's git low-speed limits, over any the entry carried", async () => {
    const limits = [String(GIT_HTTP_LOW_SPEED_LIMIT), String(GIT_HTTP_LOW_SPEED_TIME_S)];
    await withManagedRepository(
      { workspaces: ['packages/*'], files: CARGO_PROJECT },
      async ({ root, enterShell: enter, cargoLowSpeed }) => {
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoLowSpeed()).toEqual([limits, limits]);

        // The lockfile changed, so the root workspace fetches again, from an entry that carries other limits.
        await edit(join(root, 'Cargo.lock'), 'version = 4\n\n[[package]]\nname = "arrow-ipc"\n');
        const carried = { GIT_HTTP_LOW_SPEED_LIMIT: '1000000', GIT_HTTP_LOW_SPEED_TIME: '1' };
        expect(await enter({ ...CARGO, env: carried })).toEqual(HEALTHY);
        expect(cargoLowSpeed()).toEqual([limits, limits, limits]);
      },
    );
  });

  // The fetch leads a process group of its own, so the signal that ends the step no longer reaches it the way a
  // terminal's does: the step ends it, and what it started, as it ends.
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) {
    it(`takes the fetch and what the fetch started along when ${signal} ends the step`, async () => {
      await withManagedRepository(
        { files: { ...CARGO_PROJECT, 'cargo-stalls': '' } },
        async ({ ledgers, startShell }) => {
          const lifetime = join(ledgers, 'stall-lifetime');
          const up = join(ledgers, 'stall-up');
          execFileSync('mkfifo', [lifetime, up]);
          // Each end of a pipe opens only once the other does: read both before the fixture's cargo writes.
          const gone = readFile(lifetime, 'utf8');
          const said = readFile(up, 'utf8');
          const entry = startShell(CARGO);
          expect(await said).toBe('up\n');
          entry.kill(signal);
          await entry.exited;
          expect(entry.signalCode).toBe(signal);
          expect(await gone).toBe('');
        },
      );
    });
  }
});
