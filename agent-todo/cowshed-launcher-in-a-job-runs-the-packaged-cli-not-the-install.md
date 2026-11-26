# Inside a cowshed job, the npm launcher runs the package's stale cowshed instead of the installed one

Found 2026-11-26 when consumer test targets failed after all their tests had passed. Owner: cowshed.

`cowshed retire-descendants`, which bounded-exec runs after a test leaf, exited 7:

```
workspace supervisor protocol: malformed frame: unknown field `gateActive`, expected `turns` or `busy`
```

The supervisor was current; the CLI that read its frame was old.

- The supervisor's heartbeat names `gateActive` only while a land's gate runs (`ActorPulse`, `supervisor_socket.rs`), so
  the failure appeared only then.
- `bun nx` puts `node_modules/.bin` first on `PATH`, so bounded-exec's `cowshed` is the npm package's launcher
  (`bin/cowshed`).
- The launcher runs the installed copy when `${HOME}/Library/Application Support/dev.cowshed/bin/cowshed` exists. Inside
  a cowshed job, `HOME` is the shed's `.cowshed/home` (04_sandbox.md), where no install exists.
- The launcher then runs the package's own `dist/bin/<platform>/cowshed`. In a consumer workspace on 2026-11-26 that was
  `@smoothbricks/cowshed` 0.5.4, built from 40cf9c5e5a, before the supervisor gained `gateActive`.

Every running supervisor and the installed CLI were the same build. Only the launcher's choice differed.

06_cli.md, "Launcher": "when the host has an installed copy (05_gateway.md "The installed cowshed"), every verb except
`setup` and `gateway` runs it". The installed copy lives under the account's home (05_gateway.md:133-134), not under
whatever `HOME` a job is given. The installed `cowshed` is already granted to jobs (15_capabilities.md, the `tools/bin`
bootstrap entry), but the launcher does not find it.

## What must hold

- A consumer launcher run inside a cowshed job runs the host's installed cowshed, as it does outside one.
- A refused frame never leaves descendants unretired: the caller is told which binary answered and which build served.

## Red

Inside a cowshed job of a workspace whose `@smoothbricks/cowshed` package ships an older binary than the install,
`node_modules/.bin/cowshed --version` and `retire-descendants` run the installed build.
