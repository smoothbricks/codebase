# cowshed-gateway

The gateway is a localhost daemon that is the **only network path out of a sandboxed workspace**. It is protocol-aware
rather than a generic MITM proxy: registry traffic terminates at real mirror endpoints (no CA tricks), git repositories
are mirrored on your behalf, and everything else needs an explicit egress grant.

Ports: `127.0.0.1:7644` is the **host control plane only** (status, audit) alongside `~/.cowshed/gateway.sock` — it
never appears in workspace configuration. Each workspace (main included) owns a **16-port block** from a reserved range
(default `40960–49151`), allocated at `new`/`fork`/`adopt` and recorded in the grant sidecar. The block's **base port is
the workspace's gateway data-plane listener**: the port is the workspace's identity at the gateway, with its token
(`.cowshed/token`) as the mandatory second factor, and each sandbox may reach only its own block — one workspace can
never impersonate another. Ports base+1 through base+15 belong to the workspace itself, for its own dev servers (see
cli.md).

Three jobs:

1. **Registry mirrors.** Speaks the npm and cargo (sparse) registry protocols at `/npm` and `/cargo/` on every workspace
   listener. Immutable artifacts (tarballs, `.crate` files) are cached once on `~/.cowshed/caches` and served to every
   workspace and to main — duplicate downloads are eliminated fleet-wide. Upstream credentials (private registries,
   GitHub Packages) are injected by the gateway; workspaces never see a token. Registry access is **baseline policy**:
   it works in a fully closed workspace with zero grants.
2. **Repo mirrors.** Workspace git is local-paths-only — the `host` remote plus read-only bare mirrors under
   `~/.cowshed/caches/git`. `cowshed repo mirror <url>` asks the gateway to fetch a repository upstream (Keychain
   credentials, repo-scoped grants, one audit line) into the shared mirror; the workspace then clones from the mirror
   path locally. There is no git protocol endpoint, no credential helper, and no `insteadOf` rewiring inside workspaces
   — remote pushes are coordinator work, host-side.
3. **Allowlisted CONNECT.** The long tail (a scoped API host an agent legitimately needs) tunnels through `CONNECT` when
   — and only when — the workspace holds an egress grant for that host. No header injection on tunnels; it's transport,
   not impersonation.

Every decision is auditable: `~/.cowshed/store/gateway/audit.ndjson` gets one line per request (workspace, grant
revision, kind, host/name, status, bytes, cache hit/miss). Operational daemon logs — not the audit trail — go to
`~/.cowshed/store/logs/`.

## Start at login (launchd)

`cowshed gateway run` runs in the foreground. For login-time start, install a LaunchAgent:

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
  <key>StandardErrorPath</key><string>/Users/danny/.cowshed/store/logs/gateway.log</string>
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
- generic: `HTTP_PROXY`/`HTTPS_PROXY` at the workspace's base port for proxy-aware tools; CONNECT is still subject to
  egress grants.
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
3. **Egress grants** (`cowshed grant <ws> --egress <host>`): CONNECT tunnels to everything else. Stored in the
   workspace's grants file, enforced by the gateway _immediately_ (no re-exec needed), revoked with `cowshed revoke`.
   CONNECT never inherits the registry defaults.

Denials are visible in three places: exit 6 with a `next:` grant hint at the CLI, a denied line in the audit log, and
`cowshed doctor`'s recent-denials summary. Sandboxed processes cannot reach the network except through their own gateway
listener, so the allowlist is the complete egress policy — there is no second path to audit.

## Reading the audit log

One JSON line per decision; the fields you'll actually grep:

```json
{"ts":"2026-07-11T14:22:09Z","ws":"raven","rev":7,"kind":"npm",
 "name":"react-native","status":200,"cache":"hit","bytes":812443}
{"ts":"2026-07-11T14:23:41Z","ws":"raven","rev":7,"kind":"connect",
 "host":"fixtures.internal.example:443","status":"denied","reason":"no egress grant"}
```

Useful one-liners (against `~/.cowshed/store/gateway/audit.ndjson`):

```sh
# what did agents talk to today, and how often?
jq -r 'select(.status!="denied") | .host // .name' audit.ndjson | sort | uniq -c | sort -rn
# recent denials with the workspace that triggered them
jq -rc 'select(.status=="denied") | [.ts,.ws,.host] | @tsv' audit.ndjson | tail
```

## Offline behavior

Two different situations, kept distinct on purpose:

- **Gateway up, upstream offline**: mirror cache hits are served without upstream — installs of anything previously seen
  work on a plane. Misses fail fast with a `cowshed:` note distinguishing "offline" from "denied" so agents don't
  request grants to fix a network outage.
- **Gateway not running**: there is no local process to serve even cached artifacts, so registry requests fail until it
  starts. `cowshed ensure` warns (and `doctor` exits 5 with the kickstart hint); builds that touch no registry keep
  working.
