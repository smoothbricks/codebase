import { CARGO_FETCH_COMMAND } from './cross-check-policy.js';
import { runtimeInput, shellWord } from './runtime-input.js';

/** The locked fetch of one manifest, relative to the project it runs in. */
export function cargoFetchCommand(manifestPath: string): string {
  return manifestPath === 'Cargo.toml' ? CARGO_FETCH_COMMAND : `${CARGO_FETCH_COMMAND} --manifest-path ${manifestPath}`;
}

/** The managed shell's question whether one workspace's locked graph is fetched into this CARGO_HOME. */
const FETCH_STAMP = 'bun tooling/direnv/setup-environment.ts --cargo --check --workspace "$0"';

/**
 * Whether the registry holds every manifest's locked graph, as the runtime input that brings CARGO_HOME into
 * `cargo-fetch`'s key: the download lands outside the workspace, so no output can stand for it. `fetched` when an
 * offline fetch of each manifest succeeds, which asks no network and fails on any crate the registry lacks. A registry
 * that lacks one gives a value of its own run (the shell's pid and the time), which no cached run matches, so the fetch
 * runs; a registry that holds them all keys an unchanged lock to its cached run.
 *
 * An offline fetch still takes Cargo's package-cache lock, one per CARGO_HOME, so every checkout's hash queued behind
 * every other cargo on the machine (measured: 4.6 s alone, 95 s under load, minutes in a queue of 24). The project's
 * own workspace manifest is therefore answered first by the managed shell's fetch stamp, which shell entry writes for
 * the same manifest, lock, configuration and CARGO_HOME: while it is current no cargo starts. A stamp that is not
 * current, a repository without the managed shell, and every manifest the shell stamps nothing for (a package's
 * separate Wasm manifest) keep the offline fetch.
 */
export function cargoFetchedInput(projectRoot: string, manifests: readonly string[]): { runtime: string } {
  const offline = (asked: readonly string[]): string =>
    `( cd "$0"${asked.map((manifest) => ` && ${cargoFetchCommand(manifest)} --offline --quiet`).join('')} )`;
  const unstamped = manifests.filter((manifest) => manifest !== 'Cargo.toml');
  const fetched =
    unstamped.length === manifests.length
      ? offline(manifests)
      : `${FETCH_STAMP} && ${offline(unstamped)} || ${offline(manifests)}`;
  return runtimeInput(
    `sh -c ${shellWord(`if { ${fetched}; } >/dev/null 2>&1; then echo fetched; else echo "unfetched $$ $(date +%s)"; fi`)} ${shellWord(projectRoot)}`,
  );
}
