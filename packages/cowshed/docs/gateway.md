# cowshed-gateway

The gateway is a localhost daemon that is the **only network path out of a sandboxed workspace**. For a **granted**
egress host it terminates TLS with the workspace's own CA — reading and re-originating the request so it injects
upstream credentials and trace context — caches registry traffic behind protocol-aware mirrors, and mirrors git
repositories on your behalf. Secrets live only here (macOS Keychain); a workspace holds only public trust anchors, never
a token.

Ports: `127.0.0.1:7644` is the **host control plane only** (status, audit) alongside `~/.cowshed/gateway.sock` — it
never appears in workspace configuration. Each workspace (main included) owns a **16-port block** from a reserved range
(default `40960–49151`), allocated at `new`/`fork`/`adopt` and recorded in the grant sidecar. The block's **base port is
the workspace's gateway data-plane listener**: the port is the workspace's identity at the gateway, with its token
(`.cowshed/token`) as the mandatory second factor, and each sandbox may reach only its own block — one workspace can
never impersonate another. Ports base+1 through base+15 belong to the workspace itself, for its own dev servers (see
cli.md).

Five jobs:

1. **Registry mirrors.** Speaks the npm, cargo (sparse), and Go module proxy protocols at `/npm`, `/cargo/`, and `/go/`
   on every workspace listener. Immutable artifacts (tarballs, `.crate` files, module zips) are cached once on
   `~/.cowshed/caches` and served to every workspace and to main — duplicate downloads are eliminated fleet-wide.
   Upstream credentials (private registries, GitHub Packages, private Go module hosts) are injected by the gateway;
   workspaces never see a token. `/go` also passes the checksum database through, so `go.sum` verification works
   unchanged — and it is how private Go modules reach a sandbox at all, since workspace git is local-only and `go`'s VCS
   fallback has nothing to talk to. Registry access is **baseline policy**: it works in a fully closed workspace with
   zero grants.
2. **Repo mirrors.** Workspace git is local-paths-only — the `host` remote plus read-only bare mirrors under
   `~/.cowshed/caches/git`. `cowshed repo mirror <url>` asks the gateway to fetch a repository upstream (Keychain
   credentials, repo-scoped grants, one audit event) into the shared mirror; the workspace then clones from the mirror
   path locally. There is no git protocol endpoint, no credential helper, and no `insteadOf` rewiring inside workspaces
   — remote pushes are coordinator work, host-side.
3. **Intercepted egress (the default for a granted host).** `cowshed grant <ws> --egress <host>` terminates TLS under
   the workspace's CA, injects the Keychain credential for that host (if one exists) and the trace context, forwards
   over a fresh upstream connection, and audits at request granularity. This is what gives an in-workspace agent
   authenticated access to an arbitrary HTTPS API **without the workspace ever holding the secret** — the workspace
   trusts the gateway's per-host leaf because the workspace's CA **certificate** (public) is an in-image trust anchor;
   the CA **private key** never leaves the gateway.
4. **Opaque tunnels (`--opaque`).** For a cert-pinning client or a host interception breaks,
   `cowshed grant <ws> --egress <host> --opaque` reverts that host to a plain allowlisted CONNECT byte tunnel: no
   injection, host-only audit. This is the fallback, not the default.
5. **Simulator broker (`/sim/`, posture B — see ios.md).** Routes whitelisted `simctl` verbs from the in-image `xcrun`
   wrapper to the personal session's simulator, under the `--sim` grant axis (`openurl` freely grantable; `install`
   drop-dir-bound and human-gated). Dev-side headless simulators never touch the gateway.

Every decision is auditable: each request becomes one event (workspace, grant revision, kind, host/name, status, bytes,
cache hit/miss, trace id) in the telemetry store at `~/.cowshed/telemetry/` — Arrow segments, same substrate as all
cowshed telemetry. Read them with `cowshed audit`; there is no separate log file to find.

## Start at login (launchd)

`cowshed gateway run` runs in the foreground. On a nix host, prefer the declarative options:
`programs.cowshed.gateway.launchd` (home-manager owns the LaunchAgent — see the specs' 14_nix.md), or under the
dedicated-dev-uid posture, `services.cowshed.gateway` (nix-darwin LaunchDaemon with `UserName = dev`, boot-time, no
login session needed). The hand-written equivalent:

```xml
<!-- ~/Library/LaunchAgents/dev.cowshed.gateway.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>dev.cowshed.gateway</string>
  <key>ProgramArguments</key>
  <array><string>/Users/danny/.cargo-target/release/cowshed</string>
         <string>gateway</string><string>run</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>/Users/danny/.cowshed/telemetry/daemon-stderr.log</string>
</dict></plist>
```

```sh
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/dev.cowshed.gateway.plist
cowshed gateway status        # -> running 4821 control 7644, 3 workspace listeners
```

`cowshed doctor` and `cowshed ensure` both check gateway liveness; a dead gateway is exit 5 with the kickstart command
in the `next:` hint.

## Credentials (Keychain-held, never in workspaces)

The gateway reads secrets exclusively from the login Keychain, service-prefixed `dev.cowshed.gateway`:

```sh
# npm publish/install token for a private scope
security add-generic-password -s dev.cowshed.gateway -a npm:registry.npmjs.org -w "$NPM_TOKEN"

# GitHub token, used by the gateway when mirroring private repos (cowshed repo mirror)
security add-generic-password -s dev.cowshed.gateway -a git:github.com -w "$GITHUB_TOKEN"

# private cargo registry
security add-generic-password -s dev.cowshed.gateway -a cargo:crates.internal.example -w "$TOKEN"
```

The account field is `<protocol>:<host>`; the gateway matches on it when forwarding upstream. Rotation is a Keychain
update — nothing inside any workspace changes, because nothing inside any workspace ever held the credential. This is
what makes **main's image safe to clone for agents**: there is no `.env` to scrub, no token to leak. Keep it that way —
if a tool demands a secret in a file, that's a gateway feature request, not a workspace exception.

## Client wiring (files, not hand-written env)

Wiring is written into the image at adopt/new and re-validated by `ensure` — it travels with every clone and covers
processes cowshed never spawned:

- bun/npm: `bunfig.toml` points `install.registry` at the workspace's own gateway base port (e.g.
  `http://127.0.0.1:40960/npm` for a workspace whose block starts at 40960) and sets the in-image cache dir.
- cargo: a source-replacement stanza in the in-image `.cargo/config.toml` pointing `crates-io` at
  `sparse+http://127.0.0.1:<block-base>/cargo/`.
- go: an in-image go env file (`.cowshed/cache/go/env`, reached via the `GOENV` export from `cowshed ensure --envrc`)
  sets `GOPROXY=http://127.0.0.1:<block-base>/go` (no `,direct` fallback), the shared `GOMODCACHE`/`GOCACHE` on
  `~/.cowshed/caches/go`, in-image `GOPATH`/`GOBIN`, and `GOTOOLCHAIN=local` — **`~/go` is never created**, and the
  sandbox denies it as a tripwire so a mis-wired go invocation fails loudly instead of silently regrowing it.
- generic: `HTTP_PROXY`/`HTTPS_PROXY` at the workspace's base port for proxy-aware tools; egress is still subject to
  grants.
- TLS trust anchor: the workspace's public CA cert ships in-image and cowshed points each tool family at it —
  `SSL_CERT_FILE` (curl/OpenSSL), `NODE_EXTRA_CA_CERTS` (Node/Bun — a verification item), `GIT_SSL_CAINFO`,
  `REQUESTS_CA_BUNDLE`, `http.cainfo` in cargo config, a generated PKCS12 truststore for the JVM (04_sandbox.md). This
  is a public trust anchor, not a secret. macOS-native TLS (Security.framework — Swift/Xcode) ignores these and stays a
  documented gap.
- identity: the bearer token lives at `<mount>/.cowshed/token`; `cowshed ensure --envrc` exports `COWSHED_GATEWAY_TOKEN`
  only until token-via-config verification lands.

Main gets identical wiring on its own port — your interactive `bun install` warms the same mirror cache agents read
from.

## Egress allowlisting

Three tiers, checked in order:

1. **Baseline broker policy**: the configured registry upstreams (npm, crates.io, and any scoped registries in
   `gateway/config.json`). Available to every workspace, including fully closed ones — installs need zero grants.
2. **Repo grants** (`cowshed grant <ws> --repo github.com/org/*`): which repositories the gateway will mirror on this
   workspace's behalf.
3. **Egress grants** (`cowshed grant <ws> --egress <host>`): everything else. Intercepted by default (credential + trace
   injection, request-level audit); `--opaque` for a pass-through tunnel, `--impersonate <profile>` for a browser-shaped
   outbound fingerprint (which suppresses injection). Stored in the workspace's grants file, enforced by the gateway
   _immediately_ (no re-exec needed), revoked with `cowshed revoke`. Egress never inherits the registry defaults.

Denials are visible in three places: exit 6 with a `next:` grant hint at the CLI, a denied event in the audit store, and
`cowshed doctor`'s recent-denials summary. Sandboxed processes cannot reach the network except through their own gateway
listener, so the allowlist is the complete egress policy — there is no second path to audit.

## Why per-workspace CA interception is safe

Interception normally means "one machine-wide CA whose compromise breaks all TLS everywhere" — which is why a naive MITM
proxy is a bad idea. cowshed's CA is **per workspace**: minted at `new`/`fork`, its private key kept gateway-side next
to the grant file (never in any mount), destroyed with the workspace. A leaked key is one ephemeral workspace's traffic,
and it lives with the gateway, which already holds every upstream credential — so it is no new trust root. That is what
earns interception as the _default_: a granted host gets credential + trace injection with no bespoke per-service
endpoint. (Infisical's [agent-vault](https://github.com/Infisical/agent-vault) is the machine-wide-CA version of the
same idea; the per-workspace scope is the difference.) The interception engine (`hyper` + `rustls` + `rcgen`, dynamic
SNI, h1+h2, streaming preserved for LLM APIs) is specified normatively in 05_gateway.md.

## Reading the audit events

One event per decision. `cowshed audit` renders human tables; `--ndjson` gives you lines to pipe (the storage itself is
Arrow — NDJSON only ever exists on the pipe):

```sh
$ cowshed audit --ws raven --ndjson | head -2
{"ts":"2026-07-11T14:22:09Z","ws":"raven","rev":7,"kind":"npm",
 "name":"react-native","status":200,"cache":"hit","bytes":812443}
{"ts":"2026-07-11T14:23:41Z","ws":"raven","rev":7,"kind":"connect",
 "host":"fixtures.internal.example:443","status":"denied","reason":"no egress grant"}
```

Useful one-liners:

```sh
# what did agents talk to today, and how often?
cowshed audit --ndjson --since 1d | jq -r 'select(.status!="denied") | .host // .name' | sort | uniq -c | sort -rn
# recent denials with the workspace that triggered them
cowshed audit --denied --ndjson | jq -rc '[.ts,.ws,.host] | @tsv' | tail
# live tail while an agent works
cowshed audit --follow
```

## Offline behavior

Two different situations, kept distinct on purpose:

- **Gateway up, upstream offline**: mirror cache hits are served without upstream — installs of anything previously seen
  work on a plane. Misses fail fast with a `cowshed:` note distinguishing "offline" from "denied" so agents don't
  request grants to fix a network outage.
- **Gateway not running**: there is no local process to serve even cached artifacts, so registry requests fail until it
  starts. `cowshed ensure` warns (and `doctor` exits 5 with the kickstart hint); builds that touch no registry keep
  working.
