# cowshed-gateway

A localhost daemon that is the only path from a sandboxed workspace to the network. For a **granted** egress host it
**terminates TLS with the workspace's own CA** — reading and re-originating the request so it can inject upstream
credentials and trace context — and caches registry traffic behind protocol-aware **mirrors**. The residual (clients
that pin certificates, hosts an operator marks incompatible) falls back to an **opaque** allowlisted CONNECT tunnel.
Secrets exist only here — loaded from the macOS Keychain, never written into any workspace; the workspace holds only
public trust anchors. Git never crosses the gateway as a protocol: workspace git speaks only local filesystem remotes
(02_workspaces.md); the gateway's git role is executing mirror fetches on the control plane (below).

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

## Egress modes

Three tiers, never mixed:

- **Registry mirrors are baseline broker policy.** The configured upstreams — `registry.npmjs.org`, crates.io,
  `proxy.golang.org`, and scoped/private registries declared in `gateway/config.json` — are available to every workspace
  with **zero grants**: a closed workspace installs packages out of the box. Port+token still identify and audit every
  request. Mirrors terminate the protocol (not just TLS) because their value is caching and dedupe (below), not only
  credential injection.
- **Granted hosts are intercepted by default.** `cowshed grant <ws> --egress <host>` sets `mode: "intercept"`: the
  gateway mints a per-host leaf under the workspace's CA, terminates the request, injects the Keychain credential for
  `<host>` (if one exists) and the trace context, forwards it, and audits it at request granularity (verb, path, status,
  bytes). This is what gives an in-sandbox agent authenticated access to an arbitrary API without ever holding the
  secret.
- **`--opaque` hosts tunnel.** For a cert-pinning client or a host interception breaks,
  `cowshed grant <ws> --egress <host> --opaque` reverts that host to an opaque CONNECT byte tunnel: allowlisted,
  host-only audit, no injection. This is the fallback CONNECT role, not the default.

`--impersonate <profile>` (per host, off by default) composes with interception on the **outbound** leg only — see
"Interception engine". A destination with no matching grant is denied: 403 with a machine-parsable body naming the exact
`cowshed grant … --egress <host>` that would allow it.

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

### `/go/` — Go module proxy mirror

Speaks the [GOPROXY protocol](https://go.dev/ref/mod#goproxy-protocol) (`<module>/@v/list`, `.info`, `.mod`, `.zip`,
`<module>/@latest`) with the same immutable-artifact caching: module zips and `.mod` files are content-addressed on the
caches volume and cached forever; `@latest`/`list` metadata is cached briefly (5 min TTL). Upstream is
`proxy.golang.org` by default; **private module hosts** map via `gateway/config.json` with Keychain credentials injected
upstream — this is the private-module path, and the reason `/go` matters beyond caching: without a proxy, `go` falls
back to VCS (`git`) for private modules, and workspace git is deliberately local-only (below), so the gateway proxy _is_
how private Go modules reach a sandbox. The endpoint also passes through the **checksum database** per the GOPROXY spec
(`/sumdb/sum.golang.org/…` proxied verbatim, cached), so `go.sum` verification keeps working against `sum.golang.org`
with zero workspace configuration; projects with private modules set `GOPRIVATE` in the in-image go env file
(03_caches.md) to skip sumdb for those paths, exactly as they would without cowshed. Workspaces point at it via
`GOPROXY=http://127.0.0.1:<base>/go` in the in-image go env file — plain HTTP on loopback, no CA tricks, no `,direct`
fallback (a miss fails at the gateway with the offline/denied distinction rather than punching through to VCS).

### `/sim/` — personal-session simulator broker (posture B)

The bridge that lets a workspace drive the **human's** simulator (14_nix.md: the personal-session simulator is an
artifact host; dev-side headless simulators are reached directly and need no gateway). Requests arrive on the
workspace's own data-plane port from the in-image `xcrun` wrapper (03_caches.md) — first-party code, so every call
carries `traceparent` (tier-1 attribution, 13_telemetry.md). The gateway checks the workspace's `sim` grants
(04_sandbox.md), forwards allowed verbs to the **session broker**, and appends one `kind: "sim"` audit event per
operation.

The session broker is a small daemon in the personal user's GUI session (launchd agent) — under posture B, **the one
cowshed component that runs as the personal user**, because CoreSimulatorService is per-user and the human's simulator
lives in the human's session. It accepts a whitelisted verb set and nothing else:

- `list` / `boot` — enumerate and boot the personal device set (how personal devices appear as explicitly-named remote
  targets in the wrapper's merged device list);
- `openurl` — restricted to the project's registered URL schemes (deep links, dev-client reconnects);
- `install` — drop-dir artifacts only (`/Users/Shared/cowshed-drop/…`, 14_nix.md), subject to the human-gating rule.

The gateway↔broker channel is authenticated with a mutual secret provisioned at posture-B setup (mechanism is a kickoff
verification item). Graceful degradation: a tool that hardcodes `/usr/bin/xcrun` bypasses the wrapper and reaches only
dev-local CoreSimulator — the safe default; the personal session is unreachable except through this endpoint. Broker
absent (not logged in, agent not loaded) ⇒ exit 5 with a `next:` hint, distinguished from a denial.

### Granted egress — intercept (default) or `--opaque`

The long tail (arbitrary HTTPS APIs, model downloads, private hosts). Tools reach it via `HTTPS_PROXY` pointing at the
workspace's own `base` (part of exec env wiring when the workspace has any egress grants).

- **Intercept (default).** The gateway receives the CONNECT, mints a per-host leaf under the workspace CA, completes the
  TLS handshake _as_ the host, then reads the HTTP request: it injects the Keychain credential for the host (if one
  exists) and the trace context, forwards over a fresh upstream TLS connection, and records the request. h1 and h2 both
  supported, SSE/streaming preserved (LLM APIs). The workspace trusts the leaf because the workspace CA cert is an
  in-image trust anchor (04_sandbox.md); the CA **private key** never leaves the gateway.
- **Opaque (`--opaque`).** The gateway forwards the encrypted bytes and can neither read nor inject — for cert-pinning
  clients and hosts interception breaks. Authorization is still per-workspace; audit is host-only (no path/verb).

## Interception engine (normative)

The engine is `hyper` + `rustls` + `rcgen` + `tokio`. These requirements are the production lessons of a Bun/Node MITM
proxy (Hero's `@ulixee/unblocked-agent-mitm` Bun adapter) ported as **design, not code** — the JS bug classes below are
designed out, not reproduced:

- **One listener, dynamic SNI.** A single rustls acceptor per workspace serves every hostname via a `ResolvesServerCert`
  that mints/looks up the right leaf at SNI time; the CONNECT handler peeks the first byte to route TLS vs plaintext.
  The rejected shape is one `https.Server` (its own TLS context) **per (session, hostname)**: it leaks LISTEN fds under
  LRU eviction while keep-alive/looped-back sockets keep the listener bound (observed in the JS original: ~100 lingering
  servers within minutes). rustls's single-listener SNI selection has no such object to leak.
- **h1 + h2 with correct ALPN on both legs.** The Bun original was forced to HTTP/1.1 by an ALPN limitation; hyper
  negotiates h1/h2 properly. First-class requirement, not a nicety — SSE and streaming LLM responses depend on it.
- **In-process leaf minting.** `rcgen` signs a per-host leaf under the workspace CA in microseconds; the original
  spawned `openssl` per host. Cache the **leaves** (LRU), never servers.
- **Upstream-health gate at the single outbound choke point.** Every outbound connect funnels through one path; a
  cached-probe gate there fails a dead upstream **immediately** with a classifiable error instead of a per-request
  connect timeout. This _is_ the implementation of the availability contract below — gateway-absent, upstream-offline,
  and denied are three distinct, distinguishable outcomes.
- **Socket teardown discipline.** Unpipe-before-destroy and explicit connection close-tracking; generous request
  header-size tolerance (the JS original hit 16 KiB header overflow on real traffic). Both are encoded as
  integration-test assertions (08_testing.md), not rediscovered.
- **Port-based session identity.** The engine keys the session by the inbound port — the same trick cowshed already uses
  to identify a workspace by its `base` port, so it is load-bearing regardless.

### Outbound connectors

The outbound leg is an `OutboundConnector` trait. The default connector is hyper/rustls. An optional `impersonate` cargo
feature links `libcurl-impersonate` (via the `curl` crate), runtime-detected and truly optional; a host granted
`--impersonate <profile>` uses it to present a browser-shaped TLS/h2 fingerprint. Pure-Rust alternatives
(`wreq`/`rquest`, BoringSSL-based) remain possible behind the same trait. Impersonation is the outbound leg only — the
workspace-facing (interception) leg is unchanged.

## Trace context

The gateway is a span boundary. On an intercepted request or a mirror fetch it **adopts** an inbound `traceparent` if
the client sent one, else roots a new span at the workspace-attributed request; on the **upstream** leg it injects
`traceparent` (with that span as parent) exactly parallel to credential injection (13_telemetry.md). The upstream span —
latency, cache fill, retry — is recorded regardless of whether upstream participates.

One carve-out: **an impersonated connection suppresses header injection.** Modern fingerprints (JA4-family) include h2
header order and presence, so an injected `traceparent` can defeat the impersonation the grant asked for. Injection is
therefore **per-connector** — on by default, suppressed on `--impersonate` connections — and the trace is still recorded
gateway-side either way. An opaque (`--opaque`) tunnel likewise carries no injected header (the gateway never sees the
plaintext).

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
- **GitHub API operations (PRs, issues) stay coordinator-side by policy in v1** — but the mechanism now exists without a
  bespoke endpoint: a granted, intercepted `api.github.com` injects the Keychain `git:github.com` credential on each
  request (Egress modes). Whether to open that to workers is a coordinator policy choice, not a missing capability;
  publishing to a real origin remains host-side (02_workspaces.md).

## Control plane (unix socket + 7644)

`GET /v1/status` (uptime, cache stats, per-workspace request counts), `GET /v1/audit?follow` (live audit tail, NDJSON
**as wire encoding on the socket** — storage is Arrow, 13_telemetry.md), `POST /v1/repo/mirror` (the verb above). Used
by `cowshed gateway status`, `cowshed doctor`, `cowshed audit --follow`, and `cowshed repo …`.

## Credentials

- Storage: macOS Keychain generic passwords, service `dev.cowshed.gateway`, account `<protocol>:<host>` — e.g.
  `npm:registry.npmjs.org`, `git:github.com`, `cargo:crates.internal.example`. The protocol prefix is deliberate: npm
  and git credentials for the same host can differ. Managed with standard tooling (`security add-generic-password …` or
  Keychain Access); cowshed intentionally ships no secret-entry CLI so secrets never transit argv or shell history.
- Injection points: the mirror upstreams (npm/cargo tokens) and **every intercepted request** (the `<protocol>:<host>`
  credential for the granted host). An `--opaque` tunnel gets none — the gateway can't see the request. A workspace with
  no Keychain entry for a host it reaches simply sends no credential; interception still gives request-level audit and
  trace injection.
- **Per-workspace CA key.** The gateway holds each workspace's CA private key next to its grant file on the store volume
  (controller-owned, 0600, never inside any mount — 04_sandbox.md). It is minted at new/fork and destroyed with the
  workspace (02_workspaces.md); the matching CA **cert** ships in-image as a trust anchor. The blast radius of a leaked
  CA key is one ephemeral workspace's traffic — and it lives with the gateway, which already holds every upstream
  credential, so it is no new trust root.
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

## Audit events

One audit event per decision, written as **Arrow segments** under `~/.cowshed/telemetry/` (schema, flush policy, and
durability window in 13_telemetry.md) — on the store volume, denied to every sandbox (04_sandbox.md), because this is
the authoritative egress record. There is no separate audit file and no separate gateway log file: audit events and
gateway operational events are rows in the same telemetry store, distinguished by `kind`.

Read it with `cowshed audit` (06_cli.md) — human tables by default, `--json`/`--ndjson` to pipe. The same events,
rendered:

```
$ cowshed audit --ws raven --ndjson | head -5
{"ts":"2026-07-11T12:34:56.789Z","ws":"raven","port":40976,"rev":7,"kind":"npm","name":"react","status":200,"bytes":31245,"cache":"hit","traceId":"4bf92f…"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"intercept","host":"api.example.com","method":"POST","path":"/v1/run","status":200,"bytes":8123,"traceId":"4bf92f…"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"opaque","host":"pinned.example.com:443","status":200,"bytes":51200}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"connect","host":"api.anthropic.com:443","status":"denied"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"repo-mirror","url":"https://github.com/tinylibs/tinybench","status":200,"bytes":184201}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"sim","verb":"openurl","target":"booted","status":"ok","traceId":"4bf92f…"}
```

Intercepted requests carry request-granular fields (`method`, `path`); an `--opaque` tunnel is host-only by
construction. `traceId`/`spanId` columns tie each event to the trace that caused it (13_telemetry.md). Every denial
names the grant that would permit it — the audit store doubles as the debugging tool for "why can't my agent reach X".

## Tradeoffs

**Per-workspace CA interception (an earlier draft rejected all TLS MITM).** The prior spec rejected interception
outright — but that reasoning was written against a **single machine-wide CA**: one key whose compromise silently breaks
all transport security. A **per-workspace** CA changes the calculus. The key's blast radius is one ephemeral workspace,
it dies with the workspace, and it lives in the gateway — which already holds every upstream credential, so it adds no
new trust root. That earns interception as the **default** for granted hosts, which in turn supersedes the "one
protocol-aware endpoint per authenticated service" roadmap (a bespoke `/github`, `/anthropic`, … each): a granted host
gets generic credential + trace injection with no new endpoint. Infisical's
[agent-vault](https://github.com/Infisical/agent-vault) is the machine-wide-CA version of this idea. What interception
costs, stated plainly: **trust-store wiring per tool** (04_sandbox.md — env anchors cover most tools, a JVM truststore
is generated, macOS-native-TLS tools are a documented gap), a **`--opaque` fallback** for cert-pinning clients, and the
gateway **seeing granted-host plaintext** (which is the point — it is how credentials, audit, and trace attach).

**Registry mirrors kept (not folded into interception).** Interception could inject a registry token too, so why keep
protocol-aware `/npm` and `/cargo`? Because mirrors do what interception cannot: content-address and **cache** immutable
artifacts once per host (layer 1 of the cache architecture, 03_caches.md) and serve them offline. Dropping them for
plain intercepted CONNECT would lose fleet-wide download dedupe and offline installs. Injection is a side benefit of the
mirror, not its reason to exist.

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
