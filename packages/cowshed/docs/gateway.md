# cowshed-gateway

The gateway is the **only external-network path out of a sandboxed workspace**. For granted intercepted egress it
presents a leaf signed by that workspace's CA, reads the HTTP request, then opens and verifies a separate upstream TLS
connection. This lets it add narrowly scoped credentials and trace context without putting secrets in a workspace. The
in-image CA certificate trusts the gateway as a server only; it is not a client identity and is never sent upstream.
Protocol mirrors cache registry artifacts, and certificate-pinning clients can use an allowlisted opaque tunnel.

The host control plane is host-netns `127.0.0.1:7644` plus `~/.cowshed/gateway.sock`; neither host endpoint is reachable
from a workspace. Linux reuses the numeric loopback address inside each private netns for a distinct data-plane
connector; it is not the host control listener. Data-plane identity depends on the OS:

- **macOS:** each workspace has a 16-port block from `40960–49151`. The gateway listener is `base`; service ports are
  `base+1 … base+15`. Seatbelt lets the workspace connect only to its own block, so the destination listener identifies
  the workspace.
- **Linux:** there is no port block. A controller-owned socket at `~/.cowshed/run/gateway/<workspaceIncarnation>.sock`
  is mounted as `/run/cowshed/gateway.sock` inside that workspace's private loopback-only network namespace. The
  controller launches exactly one trusted minimal connector in the netns, under a controller-owned identity and
  dedicated cgroup that workspace processes cannot signal, ptrace, inspect, or join. It binds only IPv4
  `127.0.0.1:7644`, rejects non-loopback traffic, and copies bytes unchanged only to the mounted socket. It owns no
  policy, token, CA key, registry credential, or upstream-network authority. There is no veth, route, DNAT, host
  listener, or sibling-reachable loopback; socket inode plus netns, not port 7644, identifies the workspace.

Every request also sends exactly `Proxy-Authorization: Bearer <opaque-token>`. The endpoint selects the workspace; the
32-byte unpadded-base64url token is defense in depth. It is never accepted in a URL, cookie, alternate header, or query
parameter, is compared in constant time, and is stripped before upstream forwarding. Restore rotates the token and CA,
drains and kills the old Linux connector cgroup, closes all old gateway connections, recreates the Linux socket/netns
connector, and atomically rewrites client wiring; a preserved macOS port therefore does not preserve authority. Detach
drains and removes the connector/socket before releasing the netns; attach creates exactly one before admitting execs.

Five jobs:

1. **Registry mirrors.** `/npm`, `/cargo/`, and `/go/` serve anonymous public npm, crates.io, the public Go proxy, and
   checksum database as zero-grant baseline policy. Private/scoped/credentialed registries and module namespaces work
   only when trusted policy for the current `repo_id` admits their exact origin and package/module prefix. Repository
   config may request or further restrict a route, but cannot admit itself. Artifacts are digest-checked and cached
   once.
2. **Repo mirrors.** Git is local-paths-only inside workspaces. Mirror fetch is a coordinator-only control-plane action,
   bound to the current project and its trusted repository admission. A sandbox token cannot invoke it; admission or a
   mirror for one project grants nothing to another. Fetches use gateway-owned config and credentials, and resulting
   bare mirrors are sandbox-readable but never sandbox-writable. Pushes remain host-side coordinator work.
3. **Intercepted egress.** `cowshed grant <ws> --egress <host>` defaults to interception. CONNECT authority, port, TLS
   SNI, and HTTP authority must agree before the gateway opens an upstream connection. The gateway verifies upstream
   TLS, strips client authorization, injects only a credential whose exact origin, project, method, and normalized path
   scope match, and audits at request granularity.
4. **Opaque tunnels (`--opaque`).** After the same endpoint, token, CONNECT-authority/SNI, port, and grant checks, the
   gateway forwards encrypted bytes without credentials, trace injection, or path visibility. This is for pinned or
   incompatible clients, not the default.
5. **Simulator broker (`/sim/`).** The only personal-session grant verbs are `openurl` (project-registered schemes) and
   `install` (drop-directory artifact, human-gated). Personal-device `list` and `boot` remain dev-side controller
   actions; dev-side headless simulators never use the gateway.

Every decision is auditable in Arrow telemetry under `~/.cowshed/telemetry/`; read it with `cowshed audit`.

## Start at login (launchd)

On macOS, run:

```sh
cowshed gateway start
cowshed gateway status --json
```

`start` atomically installs `~/Library/LaunchAgents/dev.cowshed.gateway.plist` at mode 0600, with the absolute current
executable followed by the fixed `gateway run` argv. The agent has `RunAtLoad` and `KeepAlive`; early startup failures
go only to `~/.cowshed/telemetry/daemon-stderr.log`. The CLI uses fixed `/bin/launchctl bootstrap`, `kickstart -k`,
`bootout`, and `print` argv—never shell text—and maps already-loaded/not-loaded states idempotently. It waits for the
authenticated Unix control socket before returning success. `cowshed gateway stop` boots out the agent and removes its
plist.

The internal `cowshed gateway run` entrypoint refuses to provision storage: it first validates the existing mounted APFS
host store, constructs production cache/telemetry/control configuration with TCP control disabled, starts the gateway,
and restores all canonical attached sessions from repository bindings, mount/incarnation facts, grants, and validated
workspace credentials. Detached and retired workspaces are never installed. SIGTERM and SIGINT stop admissions and drain
the gateway before exit.

Every ordinary `exec`, `ensure`, and `doctor` invocation reconciles the current project before use. Attach, detach,
restore, removal, and other lifecycle publication paths reconcile again before success is printed, replacing changed
revisions/tokens and removing stale project sessions. Gateway absence is exit 5 with:

```text
next: launchctl kickstart -k gui/<uid>/dev.cowshed.gateway
```

## Credentials (credential-store-held, never in workspaces)

On macOS, secrets are Keychain generic passwords under service `dev.cowshed.gateway`; Linux runner storage follows the
CI platform configuration. A valid credential binding includes protocol, exact HTTPS origin (scheme, normalized host,
explicit port), allowed methods, normalized path/package/module prefixes, and project `repo_id` where applicable. A bare
host-only credential is rejected.

Credential lookup happens only after workspace endpoint and token checks, project admission, CONNECT authority/SNI and
port agreement, method validation, and path normalization. Client `Authorization`, `Proxy-Authorization`, cookies, and
protocol token headers are stripped first, so workspace input cannot select or override the credential. Values never
appear in URLs, logs, telemetry, cache keys, or responses.

Redirects are not trusted continuations. Each location is normalized and re-authorized; all credentials are stripped and
only an independently matching credential may be injected. Cross-origin redirects, TLS downgrade, method rewriting, or
leaving the admitted prefix are returned unfollowed (mirror fills fail closed). At most five same-origin redirects are
followed. Credential rotation is observed on next use; nothing inside a workspace changes.

## Client wiring (files, not hand-written env)

Wiring is written at adopt/new/fork and revalidated by `ensure`. `portBlock` is optional and macOS-only. The exact
registry URLs are:

| Client                | macOS                                         | Linux                                 |
| --------------------- | --------------------------------------------- | ------------------------------------- |
| Bun/npm               | `http://127.0.0.1:<block-base>/npm`           | `http://127.0.0.1:7644/npm`           |
| Cargo sparse source   | `sparse+http://127.0.0.1:<block-base>/cargo/` | `sparse+http://127.0.0.1:7644/cargo/` |
| Go `GOPROXY`          | `http://127.0.0.1:<block-base>/go`            | `http://127.0.0.1:7644/go`            |
| Generic HTTP(S) proxy | `http://127.0.0.1:<block-base>`               | `http://127.0.0.1:7644`               |

`HTTP_PROXY`, `HTTPS_PROXY`, and their lowercase forms use the generic base. Linux clients do **not** speak HTTP over
the Unix socket: only the connector opens `/run/cowshed/gateway.sock`. No direct fallback is configured.

All requests load the token from mode-0600 configuration and send it only in `Proxy-Authorization`; no token is exported
in a URL or proxy variable. The compatibility listener provides reachability, not identity or authority: endpoint/socket
selection plus the token still authenticate, and gateway policy still authorizes.

The workspace's public CA certificate is installed as a server trust anchor for supported tool families. Its private key
never enters a mount. macOS-native TLS clients that ignore configured anchors must fail or use an explicitly granted
opaque tunnel.

Main gets identical platform wiring and warms the same validated artifact cache.

## Egress and filesystem policy

Policy is monotonic. `repo_id` is stable lowercase `owner/repo`, normalized from a chosen remote URL; its binding
records and validates that remote. Multiple identities may be bound with exactly one primary. Local-only repositories
require an explicit identifier, and discovery may propose but never silently mint one. Effective filesystem denies are
the canonical-path union of built-ins, trusted operator policy at `~/.cowshed/<owner>/<repo>/policy.json`, and
repository-added denies; repository config can add protection but cannot remove or carve back earlier entries. The
trusted path is formed from separately validated `owner` and `repo` segments—never by accepting separators, `.`, `..`,
encoded separators, or a repository-relative path. Malformed trusted policy fails closed.

A read grant opens read plus metadata/listing access only within a configured **closed-baseline external root**. Such
roots are intentionally grantable and are distinct from immutable denies. No grant—ancestor or exact—can re-open a
built-in, trusted-policy, or repository-added deny.

Network decisions are checked in order:

1. Anonymous public registry baseline.
2. Trusted project admission for private/scoped/credentialed registry routes and coordinator-only repo mirrors.
3. Workspace egress grants for all other destinations; intercepted by default, with explicit opaque or impersonated
   mode.

## Gateway safety limits

Defaults are deliberately bounded: 32 active and 64 queued requests per workspace, 256 active and 512 queued globally,
and 8 upstream connections per workspace+origin. Each stream direction buffers at most 1 MiB and applies backpressure;
queue overflow returns 429. Header/read, connect, TLS, upstream-header, and idle timeouts are 10 s, 5 s, 10 s, 60 s, and
120 s; ordinary requests stop at 15 minutes and opaque or detected streaming connections at 60 minutes.

Requests are limited to an 8 KiB target, 100 headers, 16 KiB per field, 64 KiB total headers, and a 64 MiB generic body.
Mirror artifacts may stream to 2 GiB only with declared length and digest. Ambiguous HTTP framing is rejected: no
obs-fold, duplicate authority, repeated/conflicting content length, content-length plus transfer-encoding, invalid
transfer coding, CONNECT body, authority mismatch, or h2 connection-specific fields. Parsed requests are reserialized,
never relayed with client framing.

The leaf LRU holds 256 entries per workspace and 4096 globally, expires entries after 24 hours, and validates hostname,
workspace CA, validity, and remaining lifetime on use. The artifact cache evicts inactive LRU objects from a 20 GiB high
water mark to 16 GiB; active readers/fills are pinned. Metadata is conditionally revalidated after 5 minutes. Immutable
objects are length- and digest-verified on fill and every read, atomically committed, and deleted on corruption.

## Why per-workspace CA interception is bounded

The CA authenticates only the gateway's client-facing TLS server and is scoped to one workspace. The private key stays
controller-owned outside mounts and rotates on restore; the public certificate is only a trust anchor. This limits blast
radius without turning the CA into workspace identity or upstream authentication. The explicit costs are tool-specific
trust wiring, opaque fallback for pinned clients, and gateway visibility into intercepted plaintext.

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
