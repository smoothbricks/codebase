# cowshed-gateway

A localhost daemon that is the only path from a sandboxed workspace to the network. It is protocol-aware where that
removes the need for TLS interception (registry mirroring) and a plain allowlisted tunnel everywhere else. Secrets exist
only here — loaded from the macOS Keychain, never written into any workspace. Git never crosses the gateway as a
protocol: workspace git speaks only local filesystem remotes (02_workspaces.md); the gateway's git role is executing
mirror fetches on the control plane (below).

## Placement and identity

Two planes, strictly separated:

- **Control plane — host only.** `127.0.0.1:7644` (override `COWSHED_GATEWAY_PORT`; 7644 sits next to jcode's gateway
  at 7643) plus `~/.cowshed/gateway.sock` (status, audit tail, control verbs). The control plane serves cowshed-core,
  `cowshed doctor`, and coordinators. **7644 never appears in workspace-facing configuration**, and the sandbox baseline
  denies reaching it (04_sandbox.md).
- **Data plane — one port block per workspace.** Every workspace, main included, gets a contiguous block of **16 ports**
  from a reserved range (default **40960–49151** — 512 blocks, sitting just below macOS's ephemeral range 49152–65535):
  allocated at new/fork (at adopt, for main), preserved across restore, recorded as `portBlock: {base, size: 16}` in the
  workspace's grant file (04_sandbox.md). `base` is the gateway's per-workspace data-plane listener, **bound host-side
  by the gateway**; `base+1 … base+15` are the workspace's own bindable service ports (dev servers — vite/astro/metro,
  `devenv up` — run sandboxed and are reachable from the host browser, container-style, without sibling collisions). All
  in-image tool configuration (bunfig registry, cargo source replacement) is written against `base`.

Workspace identity is the pair, in that order:

- **Port — primary.** The destination listener (`base`) names the workspace. The Seatbelt baseline confines the
  workspace's binds and connects to its own block plus the shared ephemeral range, denying the rest of the reserved
  range (04_sandbox.md) — a workspace can neither reach a sibling's `base` listener nor squat one while the gateway is
  down. Port identity is kernel-enforced, not asserted.
- **Token — mandatory second factor.** The in-volume `.cowshed/token` (minted at adopt/new/fork, rewritten on restore)
  accompanies every request. A request on a workspace's `base` without that workspace's token ⇒ 401. Port+token resolve
  to the workspace → its grant file → its egress policy; a policy miss ⇒ 403 with a machine-parsable body naming the
  exact `cowshed grant … --egress <host>` that would allow it.

Main is a first-class data-plane client: `cowshed adopt` mints main's token, allocates main's port block, and creates
`main.asif.grants.json` — main uses identical wiring, so an interactive `bun install` warms the same mirror agents read
from.

The gateway starts under launchd (installed at adopt); see "Availability and offline behavior" for what its absence
means.

## Authorization tiers: baseline registries vs granted egress

Two tiers, never mixed:

- **Registry endpoints are baseline broker policy.** The configured upstreams — `registry.npmjs.org`, crates.io, and
  scoped registries declared in `gateway/config.json` — are available to every workspace with **zero grants**: a closed
  workspace installs packages out of the box. Port+token still identify and audit every request.
- **Everything else requires an explicit egress grant.** CONNECT and any non-registry destination are checked per
  request against the workspace's grant file. Generic CONNECT never inherits registry defaults.

## Endpoints (data plane, per-workspace port)

### `/npm/*` — npm registry mirror

Speaks the npm registry protocol (packument metadata + tarball downloads). Upstream is `registry.npmjs.org` by default;
scoped registries (e.g. a private `@smoothbricks` registry) map via `gateway/config.json`. Metadata is cached briefly (5
min TTL); tarballs are immutable and cached forever, content-addressed by their integrity hash on the caches volume
(`~/.cowshed/caches/mirror/sha512/<hh>/<hash>`). Upstream credentials (npm tokens) are injected from Keychain per
upstream host. Workspaces point at it via `bunfig.toml` `install.registry` written against the workspace's own
data-plane port — no CA tricks, plain HTTP on loopback.

### `/cargo/` — cargo sparse registry mirror

Serves the sparse index protocol (`config.json`, index files) and crate downloads with the same immutable-artifact
caching. Workspaces use a source replacement of crates.io in the in-image cargo config →
`sparse+http://127.0.0.1:<base>/cargo/` (03_caches.md).

### `CONNECT <host>:<port>` — allowlisted tunnel

The long tail (arbitrary HTTPS APIs, model downloads). Opaque byte tunnel — **no TLS MITM**: the gateway forwards
encrypted traffic and can neither read nor inject. Authorization: host:port must match the workspace's egress grants.
Tools reach it via `HTTPS_PROXY` pointing at the workspace's own `base` (part of exec env wiring when the workspace has
any egress grants).

## Git: control-plane mirrors, never a data-plane protocol

There is deliberately no git endpoint. Workspace git speaks only filesystem remotes — the `host` remote (main's mount)
and read-only bare mirrors — so no git traffic, and no git credential, ever exists on a path a sandbox can reach.

- **`repo mirror <url>`** (control-plane verb; CLI `cowshed repo mirror`, with `cowshed repo clone` as sugar): the
  gateway checks the requesting workspace's repo-scoped grants, then executes `git fetch` itself — credentials from
  Keychain (`systemd-creds` on Linux runner hosts, 10_ci.md) — into a bare mirror at
  `~/.cowshed/caches/git/<host>/<path>.git`, appends one audit line, and returns the mirror path. Mirrors are created
  and configured by the gateway and are never sandbox-writable (03_caches.md); the fetch runs against gateway-owned git
  config, so no agent-writable config or hook is ever in the loop. Workspaces then clone or fetch from the mirror path
  locally.
- **Pushes to real remotes are coordinator work**, executed host-side with the operator's normal credentials after
  `cowshed land`/review (02_workspaces.md). A sandbox has no push path even in principle: no git endpoint exists, and a
  CONNECT grant is useless for pushing without credentials.
- **GitHub API operations (PRs, issues) are out of scope for v1** — coordinator-side, like pushes. If in-sandbox API
  access is ever needed, the shape is another protocol-aware endpoint exposing a curated operation set with
  gateway-injected auth; generic header injection would require TLS MITM, which stays rejected.

## Control plane (unix socket + 7644)

`GET /v1/status` (uptime, cache stats, per-workspace request counts), `GET /v1/audit?follow` (NDJSON tail),
`POST /v1/repo/mirror` (the verb above). Used by `cowshed gateway status`, `cowshed doctor`, and `cowshed repo …`.

## Credentials

- Storage: macOS Keychain generic passwords, service `dev.cowshed.gateway`, account `<protocol>:<host>` — e.g.
  `npm:registry.npmjs.org`, `git:github.com`, `cargo:crates.internal.example`. The protocol prefix is deliberate: npm
  and git credentials for the same host can differ. Managed with standard tooling (`security add-generic-password …` or
  Keychain Access); cowshed intentionally ships no secret-entry CLI so secrets never transit argv or shell history.
- The gateway process runs unsandboxed as the user; Keychain items are ACL'd to it.
- Rotation is a Keychain update; the gateway re-reads on next use, no restart.

## Availability and offline behavior

Two distinct situations, not to be conflated:

- **Gateway absent** (daemon not running): there is no local process to serve anything — registry requests fail until it
  starts. `cowshed ensure` warns and `cowshed doctor` exits 5 with the kickstart command in the `next:` hint. Builds
  that need no new packages still work: the in-image bun cache covers locked, previously-installed dependencies.
- **Gateway up, upstream offline**: mirror cache hits are served without upstream — anything previously seen installs on
  a plane. Misses fail fast with a `cowshed:` note distinguishing "offline" from "denied" so agents don't request grants
  to fix a network outage.

## Audit log

Append-only NDJSON at `~/.cowshed/store/gateway/audit.ndjson` (rotated by size) — on the store volume, denied to every
sandbox (04_sandbox.md), because it is the authoritative egress record. Operational daemon logs (startup, errors) go to
`~/.cowshed/store/logs/`.

```json
{"ts":"2026-07-11T12:34:56.789Z","ws":"raven","port":40976,"rev":7,"kind":"npm","name":"react","status":200,"bytes":31245,"cache":"hit"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"connect","host":"api.anthropic.com:443","status":"denied"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"repo-mirror","url":"https://github.com/tinylibs/tinybench","status":200,"bytes":184201}
```

Every denial names the grant that would permit it — the audit log doubles as the debugging tool for "why can't my agent
reach X".

## Tradeoffs

**TLS MITM rejected.** A local CA trusted inside every workspace would let the gateway inject credentials into arbitrary
HTTPS — and create a single key whose compromise silently breaks all transport security, plus per-tool CA-trust shims
forever. Infisical's [agent-vault](https://github.com/Infisical/agent-vault) is that road taken (a MITM CA substituting
placeholder tokens at the boundary); protocol-aware mirrors are the no-MITM alternative covering the cases that actually
need credential injection, and the tunnel handles the rest without touching plaintext.

**Generic forward proxy only (no mirrors) rejected.** Plain CONNECT everywhere would be simpler but loses download
deduplication (layer 1 of the cache architecture) and loses credential injection entirely — private registries would
force tokens back into workspaces, breaking the no-secrets invariant that makes main safe to clone.

**Git as a gateway protocol rejected.** Host-granularity egress cannot express git policy: Anthropic's
[sandbox-runtime](https://github.com/anthropic-experimental/sandbox-runtime) docs state the failure plainly (allowing
github.com lets a process push to any repository), and [yolo-cage](https://github.com/borenstein/yolo-cage) exists to
compensate with an entire git-command classifier. Local-only workspace git plus gateway-executed mirror fetches makes
the deny structural instead of policied — and deletes the hairiest endpoint (a faithful smart-HTTP broker) outright.

**A shared fixed data port rejected.** Token-only identity on one port gave Seatbelt nothing to pin: any workspace could
reach the shared listener, and with the gateway down could bind it and impersonate the gateway to siblings (harvesting
tokens, serving unpinned packages). Per-workspace port blocks make workspace identity kernel-enforced; the cost is one
block allocator, and 512 blocks is not a real concurrency ceiling.

**A single port per workspace (vs a block) rejected.** One data port would identify the workspace but leave dev servers
to fight over the shared loopback. A 16-port block gives each workspace its own bindable service ports (`base+1 …`), so
`devenv up` and dev servers run inside the sandbox and are reachable from the host browser without per-project port
arbitration — the block _is_ the port-collision fix, not a workaround for it.
