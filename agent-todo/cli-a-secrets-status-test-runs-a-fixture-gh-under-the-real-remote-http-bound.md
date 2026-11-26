# A `smoo secrets status` test runs a fixture `gh` under the real remote-http bound

`packages/cli/src/secrets/commands.test.ts`, "a real `smoo secrets status` run > writes one document to stdout and
nothing else, and still refuses" (line 145). The test puts a `#!/bin/sh` stand-in `gh` first on `PATH` and calls
`secretsStatus`. Each `gh` call goes through `readRepositorySecrets` (`secrets/commands.ts:105`), which waits on
`remoteChild` with its default `SYSTEM_TIMERS` (`lib/remote-http.ts:122-125`). That wait is the real `remote-http`
bound, `REMOTE_HTTP_BOUND_MS = 17_000` (`lib/remote-http.ts:16`).

On 2026-11-26 the gate of land `bun-devenv` (log `/private/tmp/land-bun-devenv.out`, 17:28) failed this test after
17,364.75 ms with `out` empty: one stand-in `gh` gave no whole answer within 17 s, so `secretsStatus` printed the
`TIMEOUT` line on stderr and nothing on stdout. Every spawning test of cli shards 1-5 was 10-600× slower than in main's
gate that day. For example:

- `toolchain-stamp.test.ts`: 46,627 ms against 75 ms.
- `devenv.test.ts` "runs commands on the toolchain PATH": 12,606 ms against 110 ms.
- `index.test.ts` "holds the devenv profile Go": 9,023 ms against 42 ms.
- this file's two sibling tests: 4,458 ms and 6,012 ms against 172 ms and 249 ms.

Run alone in the same shed afterwards, `nx run cli:test-shard-5` passed, with this test at 52.9 ms and its siblings at
50.8 ms and 48.5 ms. Nothing in the land changed `secrets/` or `lib/remote-http.ts`.

A local `/bin/sh` script is not a remote service. The bound exists so that a stalled GitHub request ends (cowshed
05_gateway.md, "Deadlines"). Here it times process start-up under host load. The test exists to check the command's
output (one document, still exit 1). It does not check the deadline, which `lib/remote-http.test.ts` already covers with
injected timers.

To do: give `secretsStatus` and `gatherSecretsStatus` the `RemoteHttpTimers` that `readRepositorySecrets` passes to
`remoteChild`, defaulting to `SYSTEM_TIMERS` as `remoteRequest` and `remoteChild` already do. The test then passes
timers whose `after` never fires. Do not raise or skip the bound. Red: a stand-in `gh` that sleeps longer than an
injected timer's span fails today's test the same way the gate did. With timers that never fire, it passes.
