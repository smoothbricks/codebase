# cowshed-gateway

The gateway is the **only external-network path out of a sandboxed workspace**. For granted intercepted egress it
presents a leaf signed by that workspace's CA, reads the HTTP request, then opens and verifies a separate upstream TLS
connection. This lets it add narrowly scoped credentials and trace context without putting secrets in a workspace. The
in-image CA certificate trusts the gateway as a server only; it is not a client identity and is never sent upstream.
Protocol mirrors cache registry artifacts, and certificate-pinning clients can use an allowlisted opaque tunnel.

The host control plane is host-netns `127.0.0.1:7644` plus `/private/cowshed/store/gateway.sock`; neither host endpoint
is reachable from a workspace. Linux reuses the numeric loopback address inside each private netns for a distinct
data-plane connector; it is not the host control listener. Data-plane identity depends on the OS:

- **macOS:** each workspace has a port block from `40960–49151`, recorded as `{base, size}`: 64 ports for a new
  workspace, grown on request by `cowshed grant <ws> --ports <N>` (at least N service ports, gateway excluded; never
  shrunk). The gateway listener is `base`; service ports are `base+1 … base+size-1`. Seatbelt lets the workspace connect
  only to its own blocks, so the destination listener identifies the workspace. A block grows into a larger block
  containing it when one is free and otherwise moves; a block it moved from stays reserved to the workspace (and
  connectable by its jobs) until the workspace is removed or a later block contains it, but carries no gateway listener.
  Growth is refused while any of the workspace's jobs runs, and the next exec reaches the gateway at the current block's
  base.
- **Linux:** there is no port block. A controller-owned socket at
  `/private/cowshed/store/run/gateway/<workspaceIncarnation>.sock` is mounted as `/run/cowshed/gateway.sock` inside that
  workspace's private loopback-only network namespace. The controller launches exactly one trusted minimal connector in
  the netns, under a controller-owned identity and dedicated cgroup that workspace processes cannot signal, ptrace,
  inspect, or join. It binds only IPv4 `127.0.0.1:7644`, rejects non-loopback traffic, and copies bytes unchanged only
  to the mounted socket. It owns no policy, token, CA key, registry credential, or upstream-network authority. There is
  no veth, route, DNAT, host listener, or sibling-reachable loopback; socket inode plus netns, not port 7644, identifies
  the workspace.

Every request also carries the workspace's 32-byte unpadded-base64url token in `Proxy-Authorization`, in one of two
accepted spellings: `Bearer <token>` for anything that can set a header, and `Basic <base64(cowshed:<token>)>` for the
standard clients that cannot. The second is why the exported proxy variables carry the token as userinfo
(`http://cowshed:<token>@127.0.0.1:<base>`): curl, libcurl — so cargo — reqwest, and Go all turn proxy userinfo into a
`Basic` credential on the first `CONNECT`, and none of them can be told to send `Bearer`. The username is a fixed label
and is not compared. Both spellings reach the same constant-time comparison, and the token is stripped before upstream
forwarding either way; it is still never accepted in a request URL, cookie, alternate header, or query parameter.

The endpoint selects the workspace; the token is defense in depth. A missing or wrong credential is `407` with
`Proxy-Authenticate: Basic realm="cowshed"` — one terminal round trip, so a client either answers with the credential it
holds or aborts instead of retrying a tunnel that will never open. Restore rotates the token and CA, drains and kills
the old Linux connector cgroup, closes all old gateway connections, recreates the Linux socket/netns connector, and
atomically rewrites client wiring; a preserved macOS port therefore does not preserve authority. Detach drains and
removes the connector/socket before releasing the netns; attach creates exactly one before admitting execs.

Five jobs:

1. **Registry mirror (npm).** A read-only package request to an admitted npm registry uses the verified artifact cache.
   Every workspace gets an anonymous `registry.npmjs.org:443` intercept-read default; an explicit matching grant
   replaces it, including an opaque grant. Private, scoped, or credentialed registries require trusted policy for the
   current `repo_id` admitting their exact origin and package prefix. Repository config cannot admit itself. Tarballs
   are digest-checked and cached once. Exact packuments requested as full JSON or install-v1 JSON have separate
   representation caches; admin and version endpoints, queries, or requests carrying `Authorization` remain generic.
   Cargo and Go are not mirrored: crates.io is intercepted egress and the Go module proxy an opaque tunnel.
2. **Repo mirrors.** Mirror fetch is a coordinator-only control-plane action, bound to the current project and its
   trusted repository admission. A sandbox token cannot invoke it; admission or a mirror for one project grants nothing
   to another. Fetches use gateway-owned config and credentials, and resulting bare mirrors are sandbox-readable but
   never sandbox-writable. Pushes remain host-side coordinator work.
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

Every decision is auditable in Arrow telemetry under `/private/cowshed/store/telemetry/gateway/`
([telemetry.md](telemetry.md)).

### Registry metadata streams

npm install metadata has no whole-document buffer or byte cap. Original response bytes, status, and headers (including
ETag and Content-Length) stream to the client while the same chunks fill the cache. The JSON scanner retains bounded
parser state and relevant tokens, version-key hashes for duplicate detection, and a tarball-path-to-integrity-and-size
index. Memory grows with version/index cardinality, not ignored document bytes; it never constructs a JSON DOM. Later
tarball requests look up that index rather than opening or parsing the packument again. If a lockfile install requests a
tarball before its packument is cached, the gateway fills and indexes that packument once. Missing published integrity
is a typed refusal, not an unverified download.

Full `application/json` packuments (including npm installs and `npm view`) and compact
`application/vnd.npm.install-v1+json` packuments retain their own representations, response headers, and cache
identities. Either representation's published index can verify a tarball; the gateway does not fetch a second packument
just because the install client requested full metadata.

A client's `If-None-Match` or `If-Modified-Since` is answered by the gateway and never forwarded: the registry's `304`
answers the validator it was sent, which would leave the gateway nothing to serve or cache. `If-None-Match` is compared
weakly (`W/"x"` matches `"x"`) and rules out `If-Modified-Since`, which is compared with `Last-Modified`. Whether the
representation comes from a cache hit, a revalidation, or a fill, a client that already holds it gets a `304` repeating
its `ETag`, `Last-Modified`, `Cache-Control`, `Expires`, `Vary`, `Date`, and `Content-Location`; any other client gets
the `200`. A fill still publishes before the `304`, so a client with a warm manifest cache (Bun, npm) fills the gateway
cache instead of failing against it. Only the gateway's own cached entry validates upstream.

The index is persisted as a checksummed appendix to the cache entry, after the unchanged metadata bytes. Cache format
version 3 discards version 2 entries once on upgrade, so the first install after upgrading is cold. Malformed JSON or
unsafe, ambiguous integrity metadata aborts the stream and publishes neither cache entry nor index. Unsupported
integrity algorithms are not indexed. The gateway never rewrites tarball URLs or adds integrity query parameters. Stream
timeouts remain: 120 s idle between body bytes and 15 minutes in total for an ordinary request.

Clients use the ordinary `HTTP_PROXY`/`HTTPS_PROXY` and gateway CA wiring. TLS clients that omit ALPN use HTTP/1.1;
clients negotiating HTTP/2 or HTTP/1.1 use that protocol. No local `/npm`, `/cargo`, or `/go` registry endpoint remains.

## Start at login (launchd)

On macOS, run:

```sh
cowshed gateway start
cowshed gateway status --json
```

`start` installs the agent's binary with the same install `cowshed setup` makes: the running build becomes the installed
cowshed under `~/Library/Application Support/dev.cowshed/bin`, on the volume that carries `~/Library/LaunchAgents`
itself, so launchd can still reach the program after a reboot. The build is copied to `cowshed-<sha256>`, named for the
SHA-256 of its bytes, mode 0755, streamed into an exclusive temporary file and renamed, and never rewritten once there;
the stable name `cowshed` beside it is a symbolic link to that copy, moved from one build to the next in one rename. The
plist names only the stable name, so it never changes with the build. The source may be anywhere — the store tree, a
mounted workspace image, the nix store, a global npm prefix — because the plist never names it. That is what stops the
agent exiting 78 in a `KeepAlive` loop when the volume the build came from is not mounted at boot. `stop` and `status`
derive the same path rather than the running executable.

The `cowshed` on your `PATH` is the same artifact: `cowshed setup` points the first `cowshed` on `PATH` at the stable
name when that entry is a symbolic link, so the daemon and the `cowshed` you type run the same bytes and only an install
changes them. `start` installs without touching that entry. The previous build's copy is kept for one more install,
because a workspace supervisor of that build still draining its jobs may start shells from its path; older copies are
removed once the new build's agent is active. If activation fails, the stable name is pointed back at the previous copy
and the agent is loaded again. A release build records its commit beside its copy (`cowshed-<sha256>.build`), and
`start` and `setup` refuse to replace the installed cowshed with a build of an older commit, or one that records none,
naming the binary and both builds; `cowshed setup --downgrade` installs it anyway. The full rules are in the spec,
`specs/cowshed/05_gateway.md` "The installed cowshed".

`start` atomically installs `~/Library/LaunchAgents/dev.cowshed.gateway.plist` at mode 0600, with the stable name
followed by the fixed `gateway run` argv. The agent has `RunAtLoad` and `KeepAlive`; early startup failures go only to
`~/Library/Logs/cowshed/daemon-stderr.log`, never under the `/private/cowshed/store` mountpoint. The CLI uses fixed
`/bin/launchctl bootstrap`, `kickstart -k`, `bootout`, and `print` argv—never shell text—and maps
already-loaded/not-loaded states idempotently. A plist this run rewrote is booted out and bootstrapped again rather than
kickstarted: launchd keeps the definition it loaded, so a kickstart alone would restart the old program. It waits until
the daemon reports itself healthy — answering, not draining, and done starting — saying every few seconds what it is
waiting on in the daemon's own count. `cowshed gateway stop` boots out the agent and removes its plist; the installed
cowshed stays, as host state rather than agent state. `cowshed gateway stop --purge` also deletes the stable name, every
stored copy and its build record, and the `cowshed` links on your `PATH` that name the stable name.

The internal `cowshed gateway run` entrypoint first remounts already-created host volumes if macOS auto-mounted them at
`/Volumes` or if a leftover launchd stub occupies `/private/cowshed/store`. It never creates volumes or opens an
authorization prompt. After the store is mounted at the canonical path it answers its control socket at once, then
mounts every adopted project (mains first) and restores all canonical attached sessions from repository bindings,
mount/incarnation facts, grants, and validated workspace credentials. Until that pass ends, `cowshed gateway status`
says the gateway is still starting and how many projects it still mounts (`healing` in `--json`), and every command that
needs a workspace is refused with that progress and a retry hint rather than reported as a missing gateway. Each step of
the pass is logged as a `startup-heal` span. Detached and retired workspaces are never installed. SIGTERM and SIGINT
stop admissions and drain the gateway before exit.

The workspace supervisors still running from before the daemon started — those of another cowshed build are asked to
drain — are recovered all at once in the background while the daemon already serves. Until each is recovered,
`cowshed gateway status` reports how many are left (`recovering: { supervisors }` in `--json`), and a command for one of
their workspaces is refused with that count and a retry hint rather than reported as a missing gateway; every other
workspace is served. Each drain is logged as a `supervisor-recovery drain <socket>` span naming the pid that answered.

An audit failure closes the gateway: it cuts in-flight streams, refuses every new session, reports `draining` with the
failure as its cause, and exits once its drain completes, so `KeepAlive` restarts it instead of leaving a daemon that
answers its control socket while serving nothing. `cowshed gateway status` and `doctor` call the gateway healthy only
when the daemon answers, is not draining, and runs the same bytes as the CLI asking. Builds of one version all report
the same package version, so the daemon reports the SHA-256 of its own executable instead, and a mismatch names the
remedy `cowshed setup`, run from the cowshed you mean to use: it installs that build and restarts a gateway that runs
other bytes. `start` restarts a daemon it finds running other bytes once, and refuses with that remedy if the mismatch
survives the restart. A daemon of another build also refuses every command that would reach a workspace supervisor
(`exec`, `rm`, `new`, `land`, …): the CLI asks it before the command changes anything, so the refusal — "the cowshed
daemon is build X; this cowshed is build Y" — leaves the host as it was, and its hint is the same `cowshed setup`.

Every ordinary `exec`, `attach`, and `doctor` invocation reconciles the current project before use. Attach, detach,
restore, removal, and other lifecycle publication paths reconcile again before success is printed, replacing changed
revisions/tokens and removing stale project sessions. The gateway's session table is a cache of host inventory, never an
authority of its own: a session left behind by a project deleted out of band keeps its port block in the gateway while
the host-global allocator hands that block to the next workspace, so reconcile also evicts a foreign-project session
that holds an endpoint the current project's inventory assigns — but only after confirming no live workspace anywhere
still claims that identity; two live workspaces on one block is an integrity refusal pointing at
`cowshed doctor --json`, never a silent eviction. Installs are independent: one workspace that cannot be installed is
reported, it does not abandon the rest of the project. A kernel `AddrInUse` during installation is different from an
inventory collision: the owning controller chooses a new free block, atomically publishes its next grant revision,
re-reads the session from the store, and retries. Each refused base is excluded from that reconciliation's remaining
candidates. The allocator binds every port in a candidate block until its image publication is complete; a process
claiming a port between publication and installation still triggers the retry rather than stranding the workspace. The
CLI and embedded controller pass their already-validated host roots to `gateway_service::reconcile_native_project`; they
do not resolve `HOME` again. The shared `cowshed-core` `reconcile_project_with_reallocator` drives the control and
inventory boundaries. Gateway absence is exit 5 with:

```text
next: launchctl kickstart -k gui/<uid>/dev.cowshed.gateway
```

## Credentials (credential-store-held, never in workspaces)

On macOS, secrets are Keychain generic passwords under service `dev.cowshed.gateway`; Linux runner storage follows the
CI platform configuration. A valid credential binding includes protocol, exact HTTPS origin (scheme, normalized host,
explicit port), allowed methods, normalized path/package/module prefixes, and project `repo_id` where applicable. A bare
host-only credential is rejected.

`cowshed credential add|ls|status|rm` is the operator's side of that store, and the only supported way to write one: the
secret is named (`--secret-env`, `--secret-command`, `--secret-stdin`) rather than typed, travels in a zeroizing buffer,
and never enters argv, a file, or any diagnostic. Enrolment applies the same validation the gateway applies when reading
a record, so a binding that could never match is refused while the operator is looking instead of silently never
attaching. The variable NAME a credential was enrolled from is recorded in host state and withheld from every child of
that project — a credential the gateway holds has no reason to also reach a sandbox. See
[cli.md](cli.md#cowshed-credential-addlsstatusrm).

Credential lookup happens only after workspace endpoint and token checks, project admission, CONNECT authority/SNI and
port agreement, method validation, and path normalization. Client `Authorization`, `Proxy-Authorization`, cookies, and
protocol token headers are stripped first, so workspace input cannot select or override the credential. Values never
appear in URLs, logs, telemetry, cache keys, or responses.

Redirects are not trusted continuations. Each location is normalized and re-authorized; all credentials are stripped and
only an independently matching credential may be injected. Cross-origin redirects, TLS downgrade, method rewriting, or
leaving the admitted prefix are returned unfollowed (mirror fills fail closed). At most five same-origin redirects are
followed. Credential rotation is observed on next use; nothing inside a workspace changes.

## Client wiring (files, not hand-written env)

Wiring is written at adopt/new/fork and revalidated by `attach`. `portBlock` is optional and macOS-only. Every client
reaches the gateway at one URL:

| Client                | macOS                                           | Linux                                   |
| --------------------- | ----------------------------------------------- | --------------------------------------- |
| Generic HTTP(S) proxy | `http://cowshed:<token>@127.0.0.1:<block-base>` | `http://cowshed:<token>@127.0.0.1:7644` |

`HTTP_PROXY`, `HTTPS_PROXY`, and their lowercase forms use that base. No client is pointed at a registry mirror route.
Bun reads its registry from the repository's own configuration, cargo has no registry configuration, and both reach
their public registries as intercepted hosts that trust the workspace CA; the gateway mirrors an eligible npm request in
flight. Go reaches `proxy.golang.org` and `sum.golang.org` through opaque tunnels, because `cmd/go` sends credentials
only over HTTPS and never trusts the workspace CA on macOS. Linux clients do **not** speak HTTP over the Unix socket:
only the connector opens `/run/cowshed/gateway.sock`. No direct fallback is configured. Every exec and shell also owns
`NODE_USE_ENV_PROXY=1`: Node 24.5+ native HTTP and `fetch` opt in to the same proxy variables, including during package
postinstalls. The workspace CA remains an additive `NODE_EXTRA_CA_CERTS` trust anchor; no TLS check is disabled and no
package-specific proxy agent is required.

No wiring file carries the token. The proxy variables carry it as userinfo, which the client turns into
`Proxy-Authorization: Basic` itself: a generic proxy client has no cowshed configuration file and no way to add a
header. That exports no authority into the sandbox that `COWSHED_WORKSPACE_TOKEN` does not already give it, and the
token still authenticates against nothing but that workspace's own endpoint. The compatibility listener provides
reachability, not identity or authority: endpoint/socket selection plus the token still authenticate, and gateway policy
still authorizes.

The workspace's public CA certificate is installed as a server trust anchor for supported tool families. Its private key
never enters a mount. macOS-native TLS clients that ignore configured anchors must fail or use an explicitly granted
opaque tunnel.

Main gets identical platform wiring and warms the same validated artifact cache.

## Egress and filesystem policy

Policy is monotonic. `repo_id` is stable lowercase `owner/repo`, normalized from a chosen remote URL; its binding
records and validates that remote. Multiple identities may be bound with exactly one primary. Local-only repositories
require an explicit identifier, and discovery may propose but never silently mint one. Effective filesystem denies are
the canonical-path union of built-ins, trusted operator policy at `/private/cowshed/store/<owner>/<repo>/policy.json`,
and repository-added denies; repository config can add protection but cannot remove or carve back earlier entries. The
trusted path is formed from separately validated `owner` and `repo` segments—never by accepting separators, `.`, `..`,
encoded separators, or a repository-relative path. Malformed trusted policy fails closed.

A read grant opens read plus metadata/listing access only within a configured **closed-baseline external root**. Such
roots are intentionally grantable and are distinct from immutable denies. No grant—ancestor or exact—can re-open a
built-in, trusted-policy, or repository-added deny.

Network decisions are checked in order:

1. Workspace egress grants: public npm HTTPS reads have an anonymous default grant; other destinations need explicit
   grants. An explicit matching registry grant overrides the default, including an opaque grant.
2. Registry mirror resolution for an admitted intercepted npm request: the public registry anonymously, and a private,
   scoped, or credentialed registry only under trusted project admission.
3. Coordinator-only repo mirrors, bound to trusted repository admission.

## Gateway safety limits

Defaults are deliberately bounded: 32 active and 64 queued requests per workspace, 256 active and 512 queued globally,
and 8 upstream HTTP/opaque connections per workspace+origin. Intercepted CONNECTs are downstream TLS transports, with a
separate bounded pool of 32 per workspace and 256 globally; they do not occupy their contained HTTP requests' slots.
Both pools retain lifetime audits and participate in drain/rotation. Status reports their combined active count. Each
stream direction buffers at most 1 MiB and applies backpressure; queue overflow returns 429. Header/read, connect, TLS,
upstream-header, and idle timeouts are 10 s, 5 s, 10 s, 60 s, and 120 s; ordinary requests stop at 15 minutes and opaque
or detected streaming connections at 60 minutes.

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

One event per decision, written as Arrow segments under `/private/cowshed/store/telemetry/gateway/<yyyy-mm-dd>/`.
cowshed has no reader verb for it: each segment is an Arrow IPC stream, readable with any Arrow library, for example:

```sh
python3 -c 'import glob, pyarrow as pa, pyarrow.ipc as ipc
day = sorted(glob.glob("/private/cowshed/store/telemetry/gateway/2026-09-30/*.arrow"))
t = pa.concat_tables(ipc.open_stream(f).read_all() for f in day)
print(t.slice(max(t.num_rows - 5, 0)))'
```

## Offline behavior

Two different situations, kept distinct on purpose:

- **Gateway up, upstream offline**: mirror cache hits are served without upstream — installs of anything previously seen
  work on a plane. Misses fail fast with a `cowshed:` note distinguishing "offline" from "denied" so agents don't
  request grants to fix a network outage.
- **Gateway not running**: there is no local process to serve even cached artifacts, so registry requests fail until it
  starts. `cowshed attach` warns (and `doctor` exits 5 with the kickstart hint); builds that touch no registry keep
  working.
