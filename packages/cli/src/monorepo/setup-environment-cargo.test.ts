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

  // Shell entry's installs never fetch: devenv's smoo:cargo-fetch task runs --cargo, gated by --cargo --check.
  const CARGO = { cargo: true } as const;
  const CHECK = { cargo: true, check: true } as const;
  /** An entry on a CI runner, with its secrets injected: a fetch that fails there fails the entry. */
  const CI = { cargo: true, ciSecrets: { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' } } as const;
  /** What Cargo says when its fetch can reach no registry. */
  const REFUSAL = 'failed to download from `https://index.crates.io/config.json`';

  // One repository, entered in sequence: each step starts from what the one before it left, and asserts only what it
  // adds to the ledger of runs, so the order is the case.
  it('fetches each workspace once per change to what the fetch reads, in each CARGO_HOME, with its stamps outside them', async () => {
    const limits = [String(GIT_HTTP_LOW_SPEED_LIMIT), String(GIT_HTTP_LOW_SPEED_TIME_S)];
    await withManagedRepository(
      { workspaces: ['packages/*'], files: CARGO_PROJECT },
      async ({ root, ledgers, cargoHome, xdgCache, enterShell: enter, cargoRuns, cargoLowSpeed }) => {
        const bothWorkspaces = [fetch('Cargo.toml'), fetch('packages/native/Cargo.toml')];

        // An install fetches nothing, and neither does the status of the cargo task, which says nothing is fetched yet.
        expect(await enter()).toEqual(HEALTHY);
        expect((await enter(CHECK)).exitCode).toBe(1);
        expect(cargoRuns()).toEqual([]);

        // The cargo task fetches the root workspace and the member that is one, gives each fetch the step's git
        // low-speed limits, and stamps it.
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns()).toEqual(bothWorkspaces);
        expect(cargoLowSpeed()).toEqual([limits, limits]);
        expect(stamps(xdgCache)).toHaveLength(2);

        // The status of the task says a copy of the checkout that shares CARGO_HOME is fetched already.
        const clone = join(dirname(root), 'clone');
        await cp(root, clone, { recursive: true, verbatimSymlinks: true });
        const copied = repository(clone, ledgers, join(dirname(root), 'bin'));
        expect(await copied.enterShell(CHECK)).toEqual(HEALTHY);

        // The lockfile changed, so the root workspace fetches again and the member does not, from an entry that carries
        // other git limits: the fetch gets the step's, whatever the entry carried.
        await edit(join(root, 'Cargo.lock'), 'version = 4\n\n[[package]]\nname = "arrow-ipc"\n');
        const carried = { GIT_HTTP_LOW_SPEED_LIMIT: '1000000', GIT_HTTP_LOW_SPEED_TIME: '1' };
        expect(await enter({ ...CARGO, env: carried })).toEqual(HEALTHY);
        expect(cargoRuns().slice(2)).toEqual([fetch('Cargo.toml')]);
        expect(cargoLowSpeed()).toEqual([limits, limits, limits]);

        // Adding Cargo configuration changes what the fetch reads as much as editing it does.
        await mkdir(join(root, 'packages/native/.cargo'));
        await edit(join(root, 'packages/native/.cargo/config.toml'), '[net]\noffline = false\n');
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns().slice(3)).toEqual([fetch('packages/native/Cargo.toml')]);

        // Another CARGO_HOME holds none of what the first one fetched. This one is as a cowshed sandbox grants it:
        // Cargo's state files and its registry and git caches are writable, and nothing else, so no entry may be
        // created at its root and a stamp kept there would fail the entry.
        const sandboxHome = join(dirname(cargoHome), 'cargo-home-sandbox');
        await mkdir(join(sandboxHome, 'registry'), { recursive: true });
        await mkdir(join(sandboxHome, 'git'));
        for (const file of CARGO_STATE_FILES) {
          await writeFile(join(sandboxHome, file), '');
        }
        await chmod(sandboxHome, 0o555);
        try {
          // The fixture is only a model of the grant when the root really refuses a new entry.
          await expect(writeFile(join(sandboxHome, 'smoo-fetched'), '')).rejects.toThrow('EACCES');
          const stamped = stamps(xdgCache).length;
          expect(await enter({ ...CARGO, cargoHome: sandboxHome })).toEqual(HEALTHY);
          expect(cargoRuns().slice(4)).toEqual(bothWorkspaces);
          // Neither fetch spoiled the other's stamp: the first CARGO_HOME has nothing left to fetch either.
          expect(await enter(CARGO)).toEqual(HEALTHY);
          expect(await enter({ ...CARGO, cargoHome: sandboxHome })).toEqual(HEALTHY);
          expect(cargoRuns()).toHaveLength(6);
          // The sandbox's stamps are in the private XDG cache, and nothing was created at the root of its CARGO_HOME.
          expect(stamps(xdgCache)).toHaveLength(stamped + 2);
          expect(readdirSync(sandboxHome).sort()).toEqual([...CARGO_STATE_FILES, 'git', 'registry'].sort());
        } finally {
          await chmod(sandboxHome, 0o755);
        }

        // A stamp outside CARGO_HOME survives the deletion of CARGO_HOME, so what it promises must not: a CARGO_HOME
        // that is gone holds nothing, and one created again at the same path is another directory, fetched into again.
        await rm(cargoHome, { recursive: true });
        expect((await enter(CHECK)).exitCode).toBe(1);
        await mkdir(cargoHome);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns().slice(6)).toEqual(bothWorkspaces);
        expect(await enter(CARGO)).toEqual(HEALTHY);
        expect(cargoRuns()).toHaveLength(8);
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
  // (cargo-fetch-run.test.ts), and through this script by the signal rounds below.
  const STALL_DEADLINE_MS = 1;

  // One repository again, whose fetches never finish: the fixture's cargo is told how to fail by a file in the
  // checkout, so each step swaps that file and enters. The private XDG cache stays empty until the last step, and
  // every step before it says so.
  it('loads the shell when a fetch fails or outlives its deadline, fails a CI one, and ends a fetch along with the step that a signal ends', async () => {
    await withManagedRepository(
      { workspaces: ['packages/*'], files: { ...CARGO_PROJECT, 'cargo-fails': '' } },
      async ({ root, ledgers, cargoHome, xdgCache, enterShell: enter, startShell, cargoRuns }) => {
        // A fetch that did not finish promised nothing: no stamp, not even the directory for one.
        const unstamped = () => expect(existsSync(join(xdgCache, 'smoo'))).toBe(false);

        // A fetch Cargo refuses loads the shell, naming Cargo's cause. Two workspaces, and the fetch fails at once, so
        // the ledger is written in event order: the step stopped at the failed fetch and never began the second.
        const failed = await enter(CARGO);
        expect(failed.exitCode).toBe(0);
        expect(failed.stderr).toContain('cargo fetch --locked --manifest-path Cargo.toml');
        expect(failed.stderr).toContain(REFUSAL);
        expect(cargoRuns()).toEqual([fetch('Cargo.toml')]);
        unstamped();

        // A CI entry fails with it, and stops at the failed fetch as well.
        const refused = await enter(CI);
        expect(refused.exitCode).toBe(1);
        expect(refused.stderr).toContain(REFUSAL);
        expect(cargoRuns().slice(1)).toEqual([fetch('Cargo.toml')]);
        unstamped();

        // A fetch that outlives its deadline is ended, and the entry says which fetch, past which deadline and holding
        // which lock: a local entry loads the shell, a CI one fails. Each begins its fetch, and says so: the failures
        // before left no stamp that would let a local entry skip it.
        await rm(join(root, 'cargo-fails'));
        await writeFile(join(root, 'cargo-hangs'), '');
        for (const [mode, options, exitCode] of [
          ['local', CARGO, 0],
          ['CI', CI, 1],
        ] as const) {
          const entered = await enter({ ...options, deadlineMs: STALL_DEADLINE_MS });
          expect({ mode, exitCode: entered.exitCode }).toEqual({ mode, exitCode });
          for (const fact of [
            'cargo fetch --locked --manifest-path Cargo.toml',
            `past its ${STALL_DEADLINE_MS} ms deadline`,
            join(cargoHome, '.package-cache'),
          ]) {
            expect(entered.stderr).toContain(fact);
          }
          unstamped();
        }

        // The fetch leads a process group of its own, so the signal that ends the step no longer reaches it the way a
        // terminal's does: the step ends it, and what it started, as it ends.
        await rm(join(root, 'cargo-hangs'));
        await writeFile(join(root, 'cargo-stalls'), '');
        const lifetime = join(ledgers, 'stall-lifetime');
        const up = join(ledgers, 'stall-up');
        execFileSync('mkfifo', [lifetime, up]);
        for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) {
          // Each end of a pipe opens only once the other does: read both before the fixture's cargo writes.
          const gone = readFile(lifetime, 'utf8');
          const said = readFile(up, 'utf8');
          const entry = startShell(CARGO);
          expect(await said).toBe('up\n');
          entry.kill(signal);
          await entry.exited;
          expect({ signal, ended: entry.signalCode }).toEqual({ signal, ended: signal });
          expect(await gone).toBe('');
          unstamped();
        }

        // Once nothing fails, both workspaces fetch and stamp. This entry has neither CARGO_HOME nor XDG_CACHE_HOME, as
        // on a developer machine: its stamps go to ~/.cache/smoo, and ~/.cargo is left to Cargo.
        await rm(join(root, 'cargo-stalls'));
        const home = join(dirname(root), 'home');
        await mkdir(home);
        const before = cargoRuns().length;
        expect(await enter({ ...CARGO, home })).toEqual(HEALTHY);
        expect(cargoRuns().slice(before)).toEqual([fetch('Cargo.toml'), fetch('packages/native/Cargo.toml')]);
        expect(readdirSync(join(home, '.cargo')).sort()).toEqual(['.package-cache', 'registry']);
        expect(readdirSync(join(home, '.cache', 'smoo'))).toEqual(['cargo-fetched']);
        expect(readdirSync(join(home, '.cache', 'smoo', 'cargo-fetched'))).toHaveLength(2);
      },
    );
  });
});
