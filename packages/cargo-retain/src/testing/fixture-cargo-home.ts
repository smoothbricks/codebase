import { afterAll, beforeAll } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/**
 * Give the calling test file its own empty `CARGO_HOME` from its first test to
 * its last.
 *
 * Use it in files whose Cargo fixtures are offline and path-only. Their Cargo
 * needs nothing from the developer's home, but it still takes that home's
 * package-cache lock. Another Cargo command can hold that lock while it runs;
 * fixture setup, the code under test and its Nx children would otherwise wait
 * on unrelated work.
 *
 * The home goes into `process.env`, which is how a caller hands Cargo its home:
 * node's `child_process` and the plugin's own Cargo children read it at spawn,
 * and `fixtureNxEnv` copies it. `Bun.spawn` does not. Its default environment is
 * the one the process started with, so a `Bun.spawn` that starts Cargo must
 * pass `env` explicitly. A test that needs a particular home (its config.toml,
 * or contention on it) still sets its own `CARGO_HOME` over this one.
 */
export function useFixtureCargoHome(): void {
  let inherited: string | undefined;
  let home = '';
  beforeAll(async () => {
    inherited = process.env.CARGO_HOME;
    home = await mkdtemp(join(tmpdir(), 'fixture-cargo-home-'));
    process.env.CARGO_HOME = home;
  });
  afterAll(async () => {
    if (inherited === undefined) delete process.env.CARGO_HOME;
    else process.env.CARGO_HOME = inherited;
    await rm(home, { recursive: true, force: true });
  });
}
