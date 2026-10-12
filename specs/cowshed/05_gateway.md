# cowshed-gateway

A host daemon that is the only path from a sandboxed workspace to external networks. For a **granted** egress host it
terminates client TLS with a leaf signed by the workspace CA, then establishes and verifies a separate upstream TLS
connection so it can inject narrowly scoped credentials and trace context. Protocol-aware mirrors cache registry
traffic; cert-pinning or explicitly incompatible clients may use an **opaque** allowlisted CONNECT tunnel. Secrets exist
only in the gateway's platform credential store, never in a workspace. The workspace receives only a public CA
certificate that trusts the gateway-as-server; it receives no client identity and no upstream credential. Git crosses
the data plane only as an intercepted fetch (Egress modes, below).

## Placement and identity

On macOS the gateway runs as the `dev.cowshed.gateway` LaunchAgent at launchd `ProcessType` **Interactive**, the one
type that runs at parity with the host's own processes (PRI 31). The type is the QoS band of the agent and every
descendant, and a descendant cannot rise above it; the daemon's supervisor manager starts every workspace supervisor,
which starts every shell host and job. Background runs them at priority 4 and Standard at priority 20 (the utility
band), and both throttle their I/O behind the host's. Under Background on a host at load 80 a fresh workspace's
supervisor took 115 s to answer and its first `cowshed exec` spent 537 s inside `direnv export json`; under the utility
clamp at load 10–47 the first `codegraph status` over a fresh workspace's cold 1 GB SQLite index took 34.5 s and 54.5 s
where the same cold run unclamped took 2.4 s and 2.7 s. The sccache LaunchAgent is Interactive for the same reason
(launchd.rs `PROCESS_TYPE`).

The gateway's generated plist sets `SoftResourceLimits.NumberOfFiles` to the finite kernel-sized value `245760` and
`HardResourceLimits.NumberOfFiles` to Darwin's `RLIM_INFINITY`. A finite soft limit keeps `sysconf(_SC_OPEN_MAX)` useful
to tools that close inherited descriptors. Workspace supervisors, warm shell hosts, and jobs inherit these limits
naturally; child-spawn code does not change them. Kernel-wide descriptor capacity still applies. `cowshed setup`
reconciles the generated plist as well as the installed cowshed (below), including on hosts whose installed copy already
matches.

The host-only control plane is host-netns `127.0.0.1:7644` (override `COWSHED_GATEWAY_PORT`) plus
`/private/cowshed/store/gateway.sock` for status, audit tail, and coordinator verbs. Neither host endpoint is reachable
from a workspace, and the sandbox baseline denies both. Linux separately reuses the numeric address `127.0.0.1:7644`
inside each private netns for its data-plane connector; namespace separation makes it a different listener. Data-plane
topology is platform-specific:

- **macOS — port block.** Every workspace, main included, gets a contiguous block of ports from 32768–49151, recorded as
  `portBlock {base, size}` (04_sandbox.md: a power-of-two size, a base aligned to it; new workspaces get 64 ports, and a
  live block grows only through a `service_ports` grant; one that moves keeps its old block reserved as
  `retainedPortBlocks`, which carries no gateway listener). `portBlock` is allocated at new/fork (adopt for main),
  preserved across restore, and present only in macOS grant files. The gateway binds `base`; `base+1 … base+size-1` are
  workspace service ports. A session carries its block's recorded size, and the gateway validates the endpoint against
  it. Seatbelt permits a workspace to connect only to its current and retained blocks. The destination `base` listener
  is the primary, kernel-enforced workspace identity.
- **Linux — Unix socket, private netns, and trusted connector.** No `portBlock` is allocated. The controller creates
  `/private/cowshed/store/run/gateway/<workspaceIncarnation>.sock` under a 0700 directory with mode 0600 and bind-mounts
  that one socket as `/run/cowshed/gateway.sock` inside the workspace's private network namespace. The namespace has
  loopback up but no veth, routed interface, or default route. For ordinary package/proxy clients, the controller
  launches exactly one trusted minimal connector in that netns under a controller-owned process identity and dedicated
  cgroup that workspace processes cannot signal, ptrace, inspect, or join. It binds only IPv4 `127.0.0.1:7644`, accepts
  no non-loopback traffic, and forwards bytes bidirectionally and unchanged only to `/run/cowshed/gateway.sock`. It
  parses no protocol, owns no policy, token, CA key, registry credential, or upstream-network authority, and cannot
  select a different Unix socket. The socket inode plus private netns is the primary workspace identity;
  `127.0.0.1:7644` is only a namespace-local compatibility endpoint, so fixed service ports neither collide nor reach
  siblings.

The allocator's publication marker excludes other cowshed creators while it binds all ports in the candidate block.
Those listeners are released only after the image and grant are published. A separate host process can still claim a
port before the daemon installs the session: `AddrInUse` is a typed control refusal, not a stale-grant success. The
project controller chooses a new unassigned block under the same reservation and inventory check, publishes a higher
grant revision, rereads the authoritative session, and retries. A refused base is excluded for that reconciliation, so a
retry cannot alternate between occupied ports. Other installation errors remain errors.

Every data-plane request additionally carries exactly `Proxy-Authorization: Bearer <opaque-token>`. The token is 32
random bytes encoded as unpadded base64url, lives at `.cowshed/token` mode 0600, and is defense in depth rather than the
workspace selector: the already-selected macOS listener or Linux socket chooses the workspace before comparison. A proxy
client may present it as `Proxy-Authorization: Basic` with the token as password (what curl, libcurl, reqwest and Go
send for proxy-URL userinfo). No other header carries it: a request's `Authorization` is the client's own and
authenticates nothing. The gateway accepts no cookie, query parameter, URL userinfo, or path token; it strips
`Proxy-Authorization` and `Authorization` before any upstream request. It decodes the presented value to bytes, rejects
malformed or wrong-length values, and compares all 32 bytes in constant time. Missing or mismatched token is 401.

Create and fork mint a token. Restore stops admissions, drains the Linux connector and gateway connections, kills the
connector cgroup, rotates the token, unlinks/recreates the Linux socket and namespace-local connector when applicable,
and atomically rewrites the in-image token before new execution is admitted. Detach performs the same drain, cgroup
kill, and socket unlink without rotating persistent authority; attach creates the socket and connector before admitting
execs. The old token and every pre-restore keep-alive or tunnel are therefore invalid immediately; a preserved macOS
port does not preserve authority. A policy miss after successful endpoint and token authentication is 403 with a
machine-parsable grant hint naming both remedies, the standing one first:
`cowshed grant --project-wide --egress <host:port>` (every workspace of the project) or
`cowshed grant <ws> --egress <host:port>` (that workspace).

Main is a first-class data-plane client with identical platform wiring and policy. Gateway startup and absence are
covered under "Availability and offline behavior".

## The installed cowshed

The gateway daemon and the `cowshed` an operator types are one artifact: the installed cowshed, under
`~/Library/Application Support/dev.cowshed/bin`, on the volume that carries `~/Library/LaunchAgents` itself, so launchd
reaches it after a reboot whatever volume the build came from (a workspace image, the nix store, a checkout).

| Name in `bin`            | What it is                                                                                     |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| `cowshed`                | The stable name: a symbolic link saying `cowshed-<sha256>`, relative to `bin`.                 |
| `cowshed-<sha256>`       | A stored copy, named for the SHA-256 of its bytes (64 lowercase hex digits), mode 0755.        |
| `cowshed-<sha256>.build` | That copy's build record, JSON `{"commit","commitTime"}`, written by a build that records one. |

`cowshed-source`, one level up beside `bin`, records the path the current install came from and its package version.

**Content-addressed copies, one moving name.** A copy is written once: the build's bytes go into an exclusive temporary
file in `bin`, are synced, and are renamed to the copy's name. A copy whose bytes and mode already match is never
rewritten, so whatever runs from it runs exactly that build for as long as it exists. The stable name moves from one
whole build to the next in one rename of a temporary link, so launchd, which restarts a `KeepAlive` service the moment
it exits, never finds a half-written binary or a missing name there. The gateway plist names the stable name, so the
plist never changes with the build; an install moves the link and restarts the agent onto it. Installing a build the
stable name already runs changes nothing.

**The `PATH` entry.** `setup` points the first `cowshed` on the operator's `PATH` at the stable name: the first absolute
`PATH` directory holding a symbolic link or an executable file of that name. A symbolic link — what `bun link`,
`bun add --global` or a link made by hand leaves — is replaced by a link saying the stable path, unless it already
resolves to it (judged by where both end, so the operator's own chain of links to it is left alone). A program that is
not a link is not cowshed's to replace: setup reports it, with `ln -sf '<stable path>' '<entry>'` as the hint, and the
same hint follows a link it could not replace. With no `cowshed` on `PATH` the hint is
`ln -s '<stable path>' <a directory on your PATH>/cowshed`.

**One artifact.** The daemon runs the stable name and the typed `cowshed` reaches it, so they are the same bytes, and
only an install changes them: `setup`, or `gateway start`, which installs the running build the same way but leaves the
`PATH` entry alone. A rebuild in a checkout changes nothing on `PATH` until a setup runs from that build. This exists
because `~/.bun/bin/cowshed` was once a hand-made link to a checkout's `packages/cowshed/bin/cowshed`, the launcher that
runs that checkout's build (06_cli.md "Launcher"): every rebuild there changed the CLI every agent used while the daemon
kept the build setup had installed, the daemon then refused the CLI's verbs ("the cowshed daemon is build X; this
cowshed is build Y", 11_shell.md), and a removal refused half-way failed its own state restoration too.

`cowshed setup` typed at a shell therefore runs the installed copy itself, and installs nothing: it says so, and names
the remedy. To install another build, run that build's own binary — a checkout's `packages/cowshed/bin/cowshed setup`,
or its `dist/bin/<platform>/cowshed setup`. Such a run still repairs everything else, the plist included, and restarts a
gateway that answers while running other bytes than the build it leaves installed (status `executableSha256`): launchd
keeps a running process whatever the stable name now says, so this is what ends the disagreement the build refusal
names.

**Install order and rollback.** An install that replaces the running build refuses a debug build and a downgrade (below)
before anything is installed. With the gateway agent installed it then writes the plist if it changed, and retains the
previous build: when the stable name is a link, the copy it names is already stored; when it is the plain binary the
layout before content-addressed copies left, that binary is first stored under its own digest through the same copy and
rename. It stores the new copy, moves the stable name, records the source and the build record, and activates the agent.
A failed activation points the stable name back at the retained copy, records `cowshed-source` as restored, and loads
the agent again; the error says what failed and whether the restore and the reload did. A first install has nothing to
roll back to and keeps its copy. A setup on a host with no gateway agent installs the same way without the activation.
Setup installs only after its storage repair succeeded.

**Pruning.** After an install succeeds (for the gateway, after its activation), every stored copy and build record but
the new build's and the one it replaced is removed. The replaced build stays one install longer: a workspace supervisor
of that build, still draining its jobs (11_shell.md "Draining a supervisor of another build"), starts the shells those
jobs need from the path it started from. A failed prune is reported on stderr and never fails the install.

**Build records and downgrades.** A release build records the commit `HEAD` names and that commit's committer time
(`build.rs`: `COWSHED_BUILD_COMMIT`, `COWSHED_BUILD_COMMIT_TIME`); the time is read from that one commit, so a shallow
clone records it too. A debug build, and a build made where git cannot answer (a source archive, a nix build), records
nothing. The installing build writes its record beside its stored copy, so the installed build's record is read from a
file and nothing is executed to learn it; a record that cannot be written is said on stderr and does not fail the
install. An install that would replace the installed build is refused when the installed build has a record and the
candidate's commit time is older, or the candidate records nothing (it cannot show it is not older). Equal commit times
are not ordered, so neither is older; the same bytes are never a downgrade of themselves; an installed build with no
record — installed before records existed, or the plain binary of the old layout — is never an obstacle. The refusal is
`Conflict` naming the binary it would have installed and both builds (`commit <12 hex>, committed <RFC 3339 UTC>`), with
the hint "run `cowshed setup` from a build of a newer commit; `cowshed setup --downgrade` from this build installs it
anyway". `setup` reports it once for the gateway and the CLI, repairs everything else, and exits non-zero with that
`Conflict`; `gateway start` refuses before it installs anything. `setup --downgrade` installs the invoking build anyway,
and is valid only for a run that installs: not with `--uninstall`, `--force` or `--mount-root`. An unreadable installed
record is `Integrity`. This exists because twice a `cowshed setup` run through a launcher that resolved to a stale
checkout build put back a gateway without a landed feature, and nothing said so.

**Uninstall.** `setup --uninstall` and `gateway stop --purge` remove, after the agent is gone, the stable name first (so
nothing launchd could start dangles), then every stored copy with its `.build` record, and every `cowshed` on `PATH`
that is a link saying exactly the stable path — the entries setup made, and no other link that merely arrives there. A
plain `gateway stop` keeps all of it: it is host state rather than agent state, and the next `start` is then a plist
write.

## Egress modes

Three tiers, never mixed:

- **Public npm reads are anonymous by default.** Every workspace's derived policy includes an intercepted
  `registry.npmjs.org:443` read grant, including fresh workspaces with no project-standing grants. An explicit grant
  matching that host and port replaces the default, including a matching wildcard or opaque grant. Eligible package
  requests use the verified mirror cache without credentials. No HTTP port, alternate registry, private namespace, or
  registry write is implicitly admitted. crates.io, `proxy.golang.org`, and the public checksum database remain
  explicitly granted hosts, intercepted and opaque respectively, never mirrored.
- **Admitted private/credentialed registry routes.** A private upstream would be usable only when a trusted admission
  for the project's stable `repo_id` named the exact registry origin and package scope. The trusted project policy has
  no admission field, so no workspace holds a registry route beyond the anonymous public baseline; repository config
  cannot admit one either. The gateway rejects an unadmitted route before credential lookup.
- **Granted hosts.** `cowshed grant <ws> --egress <host>` defaults to `mode: "intercept"`, which admits `GET` and `HEAD`
  on the whole origin plus `POST` to a path ending in `/git-upload-pack` — git's smart-HTTP fetch, a read git can only
  express as a POST (protocol v2 posts even the ref listing); `git-receive-pack` and every other write are refused.
  `cowshed grant <ws> --egress <host> --opaque` grants the hosts of that invocation as a byte tunnel with host-only
  audit and no injection; a host holds one rule, so granting it again restates its mode. A project's standing egress
  grants (`cowshed grant --project-wide --egress <host> [--opaque]`, 04_sandbox.md) join every workspace's own.
  Unmatched destinations are denied.

## Endpoints (data plane, per-workspace endpoint)

The HTTP URL base is `http://127.0.0.1:<portBlock.base>` on macOS and exactly `http://127.0.0.1:7644` on Linux. No
workspace client is configured for a mirror route: bun (its repository's registry), cargo and Go reach their public
registries through the generic `HTTP_PROXY`/`HTTPS_PROXY` variables (and lowercase equivalents) on `<base>`
(03_caches.md) — intercepted for npm and crates.io, an opaque tunnel for Go, which sends no credential to plain HTTP. On
Linux the trusted connector carries these byte streams to the mounted Unix socket; clients never speak HTTP over Unix
sockets. Endpoint selection and the token still authenticate every request.

### npm registry mirror — eligible requests to an admitted registry

A read-only package request that an egress grant admits is served through the mirror; nothing else is. Eligible requests
carry no `Authorization` and no query, and have the exact shape of a packument (`/<name>` or `/@scope%2fname`, asked for
as `application/json` or `application/vnd.npm.install-v1+json`) or a tarball (`/<name>/-/<file>.tgz`). Full and compact
packuments have distinct representation cache identities; either index can verify later tarballs without another
metadata fetch. Admin/version endpoints, queries, unsupported/ambiguous Accept, and credentialed requests remain
generic. Anonymous `registry.npmjs.org` is the baseline. Scoped or private origins require a trusted project-policy
admission binding an exact origin to allowed package scopes; only then may the gateway select the matching credential,
and only the longest admitted prefix on that exact origin decides — a scope never crosses origins, and a private origin
has no public baseline behind it. Metadata TTL is 5 minutes. A packument is streamed to the client byte for byte — the
gateway rewrites no tarball URL, buffers none of it, and caps its size nowhere — while the same chunks fill the cache. A
streaming recognizer reads them in that one pass and records, for every `versions.*.dist`, the `dist.integrity` and
`dist.size` of the same-origin tarball path that object names — one path and digest per version, never the document; an
ambiguous document (a duplicate `versions`, `dist`, `tarball`, `integrity` or `size` key, or two expectations for one
path) is refused. Tarballs are content-addressed by that published digest, verified while filling and again on every
cache read, and committed by atomic rename. A tarball request — a lockfile install builds its URL without reading the
packument — takes its expectation from the index of its package's packument, fetched through the same metadata path,
instead of parsing it again. A version the packument does not publish with a SHA-512 or SHA-256 integrity is refused,
never served unverified.

### Cargo and Go — not mirrored

crates.io and the Go module proxy are ordinary granted hosts: cargo reaches `index.crates.io` and `static.crates.io`
through intercepted egress, Go reaches `proxy.golang.org` and `sum.golang.org` through opaque tunnels. Neither is served
from the mirror cache or rewritten, and neither could be a mirror client. Every sandbox builds with one literal
`CARGO_HOME` whose registry is shared host-wide (03_caches.md), cargo names that registry by its index URL (a
per-workspace endpoint would split it), and cargo has no client-side setting for the `dl` of a registry. `cmd/go` sends
credentials only over HTTPS, so it cannot authenticate to a plain-HTTP endpoint. The clients verify what they download
themselves: cargo checks every crate against the index's own `cksum`, and Go checks `.mod` and `.zip` against `go.sum`
or the checksum database and verifies the database's signed tree.

### `/sim/` — personal-session simulator broker (posture B)

The sandbox-visible grant enum contains only `openurl` and `install`. `openurl` is restricted to URL schemes registered
for the project; `install` accepts drop-directory artifacts only and remains human-gated. Personal-device `list` and
`boot` are dev-side controller operations and are not gateway grant verbs. Dev-side headless simulators are reached
directly. Unknown verbs are rejected before broker forwarding, and every decision is audited.

### Granted egress — intercept (default) or `--opaque`

Proxy-aware clients use their platform endpoint. On CONNECT, the gateway canonicalizes the authority as DNS-name plus
explicit port, resolves the workspace grant, and requires TLS SNI to equal that DNS name after IDNA A-label and
lowercase normalization. IP CONNECT is allowed only by an exact IP grant and must not carry a conflicting DNS SNI.
Missing SNI for a DNS CONNECT, authority/SNI mismatch, port mismatch, wildcard crossing more than one label, or a
request whose HTTP `:authority`/`Host` differs from the CONNECT authority is denied before upstream connect. The gateway
never resolves an unvalidated secondary authority.

Intercept mode presents a workspace-CA leaf to the client and separately verifies upstream certificate name, chain,
validity, and revocation policy against system roots. The workspace CA certificate authenticates only the gateway's
server side; it is not sent upstream and cannot authenticate a workspace. Opaque mode forwards encrypted bytes only
after the same endpoint, token, authority, and grant checks and never injects credentials or trace headers.

## Interception engine (normative)

The engine is `hyper` + `rustls` + `rcgen` + `tokio`:

- One acceptor per workspace endpoint uses dynamic SNI; never create a listener per host. h1 and h2 ALPN are supported
  on both legs, with SSE and streaming preserved.
- Leaves are minted in process and cached, not listeners. The leaf LRU is capped at 256 entries per workspace and 4096
  globally; entries expire after 24 hours and are usable only when hostname, workspace CA fingerprint, validity window,
  and at least 5 minutes of remaining lifetime all match. Rotation of a workspace CA drops that workspace's entries.
- Limits are 32 active HTTP/opaque requests and 64 queued admissions per workspace, 256 active HTTP/opaque requests and
  512 queued admissions globally, and 8 active upstream connections per workspace+origin. Intercepted CONNECT transports
  use a separate bounded pool (32 per workspace, 256 globally), not their contained requests' slots or upstream-origin
  slots; lifetime audit leases, rotation, cancellation, and drain cover both pools. Active status is their combined
  count. Queue overflow returns 429 without reading a body. Each direction gets a 1 MiB bounded streaming buffer;
  producers pause when it fills, and cancellation closes both legs.
- Timeouts are 10 s for request headers, 5 s for TCP connect, 10 s for each TLS handshake, 60 s to upstream response
  headers, 120 s idle between body bytes, 15 min total for ordinary requests, and 60 min total for opaque tunnels or
  explicitly detected streaming responses. Timeouts close both legs and emit a classified audit event.
- Parsed request limits are an 8 KiB request target, 100 header fields, 16 KiB per field, 64 KiB aggregate headers, and
  64 MiB request body for intercepted generic egress. Mirror artifacts may stream to 2 GiB only when protocol metadata
  declares an expected digest/length; larger or indeterminate artifacts are rejected without caching.
- Request-smuggling defenses are fail-closed before forwarding: reject obs-fold, invalid header names/values, whitespace
  before `:`, duplicate `Host`/`:authority`, conflicting or repeated `Content-Length`, any `Transfer-Encoding` with
  `Content-Length`, transfer codings other than a single terminal `chunked`, absolute-form URI/authority disagreement,
  CONNECT with a body, and h2 connection-specific headers. The gateway reserializes parsed requests rather than relaying
  client framing and never downgrades ambiguous h2 requests into h1.
- Cache storage has a 20 GiB high-water mark and evicts least-recently-used inactive objects to 16 GiB. Active readers
  and in-progress fills are pinned. Metadata older than 5 minutes is conditionally revalidated with ETag/Last-Modified;
  304 refreshes metadata, 200 replaces atomically. Immutable objects require expected length plus digest verification on
  fill and every read; corruption deletes the entry and becomes a miss. Fills use a same-filesystem temporary file,
  fsync, atomic rename, and parent fsync; concurrent misses coalesce by cache key.

### Credential and redirect boundary

Client-supplied `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie`, and protocol-specific token headers are
stripped before forwarding; a sandbox cannot choose or override an upstream credential. A credential record binds one
protocol, HTTPS origin (scheme, canonical host, explicit port), allowed methods, and normalized path/package/module
prefixes. Injection occurs only after all fields match and only on the freshly serialized upstream request. The default
credential policy permits `GET`/`HEAD` for mirrors; write methods require an explicit trusted-policy admission. Path
normalization rejects encoded separators, dot segments, backslashes, NUL, and ambiguous double decoding before prefix
comparison.

Redirects are never followed implicitly. For each 3xx, the gateway resolves and normalizes `Location`, strips every
credential and client authorization value, then re-runs project admission, egress mode, origin, method, and path checks.
It may reinject only a credential independently bound to the new exact origin and scope. Cross-origin redirects,
HTTPS-to-HTTP downgrade, method rewriting (including 301/302/303 POST-to-GET), or a redirect outside the admitted prefix
are returned to the client without following; mirror fetches fail closed instead. At most 5 same-origin redirects are
followed. DNS is resolved after authorization and each connection is pinned to the authorized resolution; redirects and
retries repeat resolution and private/link-local/loopback targets require an explicit exact policy entry.

## Git: coordinator-only project mirrors, never a data-plane protocol

There is no git data-plane endpoint. Only the host-side coordinator may request a mirror; a workspace, sandbox token, or
sandbox process cannot invoke the control verb. The coordinator supplies the bound project identity and canonical remote
URL. The gateway verifies the stable `repo_id`, requires the trusted project policy to admit that exact repository or
namespace, and stores/fetches only within that project's mirror scope. A mirror admitted for one project is not thereby
visible or authorized for another project, even when content storage deduplicates identical objects.

The gateway performs `git fetch` with gateway-owned config, hooks disabled, protocol restricted to HTTPS, redirects and
credential scope checked as above, and credentials selected only after exact project+origin+repository-path admission.
The resulting bare mirror is sandbox-readable and never sandbox-writable. Pushes and all real-origin mutation remain
coordinator-side; the data plane supplies neither git credentials nor a push path.

## Control plane (Unix socket + optional 7644)

The control plane provides status and audit to host tools, a project-bound repo-mirror operation to coordinators, and
the host's disk-lifecycle lease and CPU budget to every process that runs a disk tool or a parallel runner (below). Peer
credentials on the Unix socket (and equivalent local authentication on TCP) must identify an authorized host process;
the data-plane token is never accepted on the control plane.

Workspace sessions are installed and removed over the control plane, and the gateway's session table is a cache of host
inventory, never an authority. Each session carries its workspace's effective grant revision, which every grant change
and every attach advances. An install must exceed every revision that workspace identity has held — a removal leaves its
revision behind as a tombstone — and a removal must name the installed revision; either refusal is `revision-fence`, and
a removal of a session that is already gone is `not-installed`.

Every controller that needs the gateway reconciles its project first, so reconciles of one project run concurrently.
Each reads gateway status before its inventory snapshot: every installed session came from an older snapshot, so a
removal never revokes a newer decision, which would leave a tombstone refusing the workspace's own revision until its
next attach. A write refused with `revision-fence` or `not-installed` lost a race to a reconcile that read a newer
snapshot; that decision stands, and the write counts as superseded rather than failing the command that ran the
reconcile.

Every request is one line. A one-shot request is followed by the client's EOF and answered with one line; a `disk-lease`
or `cpu-tokens` request is held for as long as its client keeps the connection open.

## Disk-lifecycle lease

Disk-changing tools cowshed runs on the host — the CLI's, a supervisor's, the gateway's own startup pass, and every
test's — run under a host-wide lease the gateway schedules. One code path takes it: `SystemCommandRunner` reads the
command's class from its program and arguments and holds that lease around the child; setup uses the same client. An
attach spends most of its time in StorageKit's `syncAllDisks`, after its device already exists, and that sync does not
finish while the mount table keeps changing (01_storage.md, "How the APFS host degrades"): eight attach loops beside two
mount loops made 8 attaches in 42 s, each taking 41.7 s, while keeping the two apart made 106 attaches at 0.94 s p50 and
1.78 s p95 under load 167–212. Attaches among themselves only queue on `storagekitd`; so the lease is two classes that
exclude each other rather than N interchangeable tokens.

- **Storage**: `diskutil` except `renameVolume`, `hdiutil`, and `newfs_apfs`. **Namespace**: `mount_apfs` and `umount`.
  Read-only operations (`fsck_apfs`, IORegistry reads) and cosmetic `diskutil renameVolume` take no lease.
- **A label does not change the disk lifecycle.** On a mounted scratch APFS volume, a rename kept the same mount-table
  position (58/58) and st_dev (16777292). Five attach probes without renames took 0.648, 0.683, 0.953, 0.896 and 0.572
  s; with 19 renames on another volume they took 1.056, 0.596, 0.461, 0.590 and 0.477 s. Five mount/unmount probes took
  1.123, 0.862, 0.991, 0.928 and 0.889 s; with 13 concurrent renames, 0.841, 0.556, 0.450, 0.750 and 0.567 s. Renames
  neither starved attaches nor delayed namespace work. A background cosmetic rename therefore must not hold either phase
  and make an attach wait behind it.
- Members of one class share the running phase, up to 8 at once. A request of the running class with room in the phase
  and nobody waiting enters at once; an idle gateway starts a phase for it.
- Once a member of the other class waits, the running phase admits nobody new, so a steady stream of one class cannot
  starve the other. When the phase drains, the next goes to the other class, whose oldest waiters enter it in arrival
  order up to the cap; with both classes waiting, phases alternate.
- **A stuck holder is bounded.** A holder that keeps its phase 10 s after its grant while the other class waits is
  evicted from the phase: it no longer counts, the phase hands over once its other holders leave, and the gateway's
  stderr names it — `disk-lease evicted a <class> holder after <held> while <class> waited`, with its pid and the
  command line it asked for. Its command may still be running beside the next phase; a hung `umount` costs that one
  overlap, not every attach on the host. A client's own deadline still kills its child at 120 s, except a disk-image
  command (`diskutil image …`, `hdiutil`), which it warns about from 120 s every 30 s and kills only at 15 minutes,
  because a killed one can leak its device until reboot (01_storage.md, "How the APFS host degrades" §2).
- **Wire.** The client writes `{"op":"disk-lease","class":"storage"|"namespace","command":"<argv>"}` and does not
  half-close. The gateway answers `{"ok":true,"lease":"queued"}` at once and `{"ok":true,"lease":"granted"}` when the
  command may run; the lease lasts until the client closes the connection, so a client that dies holding one releases it
  with its socket, and one that closes while queued leaves the queue.
- **The lease spaces commands out; it never decides whether one runs.** The command runs unleased when the control
  socket does not answer (said once per process: the gateway is down or being set up), when the gateway refuses, when no
  grant comes within 120 s, and when the gateway predates leases. An older gateway says nothing until it reads EOF, so a
  client that hears no `queued` within 2 s half-closes, reads that gateway's `invalid-request` refusal, and stops asking
  it for a minute.
- **Spans.** The client's lifecycle spans are `disk-lease <class> wait`, from the request to the grant (`status=err`,
  with an `unleased=<reason>` line, when the command runs unleased), and `disk-lease <class> phase`, around the command
  the grant admitted. With `COWSHED_TIMING` set, the gateway prints each phase as it drains: its class, how long it ran,
  its members, and how many it evicted.

The regression is `cowshed-gateway`'s `contention_attaches_stay_fast_beside_mount_churn_only_under_the_lease`, run by
name with `--ignored`: eight real attach/detach loops beside two real mount loops for 40 s, first through a real
gateway's lease and then unleased. Measured at load 14–50: leased, 125 attaches at 1.24 s p50 and 1.93 s p95 (2.09 s and
3.39 s with the lease wait); unleased, 8 attaches, each taking 53.5 s.

## Host CPU budget

Every gate on the host sizes its runners to the whole machine. Nx runs one task per core, and each nextest run under it
runs one test thread per core again; five to ten concurrent gates on an 18-core host kept the load average at 80–170,
and tests that do no disk work timed out waiting for a CPU (37 of 65 timeouts in one acceptance run). Running anything
serially would trade those timeouts for idle cores. The gateway instead holds one budget of CPU tokens for the host: a
runner takes tokens before it starts and sizes its own parallelism to its grant, every gate keeps running its tasks in
parallel, and the total runnable work stays near the core count.

- **The budget.** N tokens, one per core the host reports (`std::thread::available_parallelism`); one token is one
  runnable thread or process. The ledger keeps `free + Σ held = N`; it never infers a holder from a process scan.
- **Who asks.** The `@smoothbricks/nx-plugin` `bounded-exec` executor, before it starts any command — the inferred
  nextest runners, `bun test` targets, cargo commands it runs. Its `want` is the command's runner's own parallelism: a
  `nextest run` asks for its `--test-threads` or one per core; `bun test --parallel=N` for N workers, a bare
  `--parallel` for one per core; a cargo build (`cargo build`/`test`/`clippy`/…, `nextest archive`, `napi build`) for
  one job per core; anything else, one `bun test` process included, for one. A target's `parallelism` option outranks
  all of these. `test.concurrent` inside one process is not another CPU.
- **Sized to the grant.** A grant is between 1 and `want`, and `want` is capped at half the host (below). The executor
  sets `NEXTEST_TEST_THREADS` for nextest, rewrites a `--test-threads=`/`--parallel=` count the command names (a flag
  outranks the environment), sets `CARGO_BUILD_JOBS` and `RUST_TEST_THREADS` for cargo, and gives every command
  `BOUNDED_EXEC_CPU_TOKENS`, so a script that starts a runner itself can size it.
- **Nested runners use a parent-held sub-ledger.** A granted executor serves its T tokens on a private socket and
  publishes that endpoint as `BOUNDED_EXEC_CPU_POOL`, beside its `BOUNDED_EXEC_CPU_TOKENS` count. A default nested
  executor connects to its immediate parent's live pool, requests at most `min(want, inherited, ceil(N/2))`, and holds
  one complete local grant on that connection. The pool accounts for every holding sibling, so their sum never exceeds
  T. It serves its own FIFO queue, granting the head `min(requested, free)` once at least one token is free; that count
  is final, not a partial holding awaiting more tokens. Queued siblings wait only for that pool's own holders. Host
  checkout fairness remains the upstream ledger's concern. An admitted child serves its granted count as its own pool,
  and grandchildren draw only from that pool: no holder waits for another grant from an ancestor or the host. The parent
  remains the owner of its upstream grant. A child returns only its local allocation by closing its connection; it never
  releases the parent's connection. Upstream loss revokes the downstream pool and its connections, so a dead parent
  cannot grant tokens to a descendant. Losing a borrowed grant ends its still-running command through the normal
  TERM/grace/KILL path and reports an ordinary CPU-grant-loss failure, never a timeout. If a bound already started
  terminating the command, the loss still makes its verdict an ordinary failure; the existing termination owns the
  signals and grace, so losing the grant starts no second teardown. A bare or invalid count proves no allocation; an
  absent endpoint follows normal host admission. Once connected to the parent, refusal, silence or protocol failure is
  an ordinary admission failure, never a second host take while that parent may still hold its grant. A stale descriptor
  whose endpoint no longer connects is treated as absent. Only the executor publishes its actual live pool;
  `options.env` cannot inject either marker into a command. The span and `cpu.inherited: true` record identify a
  parent-pool grant and its actual local wait. Pool setup or cleanup failure is an ordinary failure, never a successful
  unaccounted run. Real-process default-runner proofs live in `executor-host.test.ts` under `test-host`, because the
  default macOS runner acquires a real RAM volume. Node peers import the built executor artifact named by
  `executors.json`; the host target's existing build dependency produces it, rather than compiling TypeScript inside a
  test's guard. Pure pool and executor unit cases never attach images. **Known admission ambiguity:** a connect error is
  currently classified as an absent endpoint. A full listen backlog or local file-descriptor exhaustion can also prevent
  connection to a live pool, so this classification does not yet prove that its parent has released the upstream grant.
  Distinguishing those cases before any host fallback remains an admission follow-up; connected refusals and protocol
  errors already fail without a second take.
- **Fair between checkouts.** The checkout is the Nx workspace root the request names. A checkout's share is N divided
  among the checkouts holding or waiting (at least 1). The next grant goes to the waiting checkout that holds the fewest
  tokens (the older head request on a tie), to its oldest request. While another checkout waits, that grant is
  `min(want, max(1, share − held))`; with nobody else waiting it is `min(want, free)`. A head request whose least grant
  is not free yet waits for it, and so does everyone behind it, so a large request is never starved by a stream of small
  ones. A gate with sixty runners queued cannot crowd out one with three.
- **All at once.** A request is granted once, in full, and never holds part of a grant while waiting for more, so no two
  requests can deadlock on each other's tokens.
- **No grant exceeds half the host**, `ceil(N/2)`. Fairness acts only when a grant is made, and a running nextest or
  cargo cannot hand tokens back. Uncapped, one checkout's single runner once held 17 of 18 tokens for its whole run, and
  a second checkout's gate held 1 and waited it out. With the cap, a checkout arriving beside one runner finds the other
  half, or the next runner's tokens. A checkout alone still fills the host, because each gate runs several runners.
  - The cost: one runner alone on the host runs at most N/2 threads. Measured on 18 cores at host load 83–99 from other
    work, with `cowshed-core`'s 903 non-APFS unit tests: 26 s at 18 threads (warm) against 31 s and 44 s at 9. A
    newcomer behind an uncapped runner instead waits out that runner's whole run.
  - Rejected alternative: reserving slots for a newcomer. It idles cores whenever no newcomer comes, and it still lets
    two runners of one checkout hold everything else.
- **Crash safety by connection.** The grant is tied to the connection: on exit, `SIGKILL` included, the kernel closes
  the socket and the gateway returns the tokens at once. A request that closes while queued leaves the queue. A gateway
  restart forgets every grant, so runners started under the old one overshoot until they finish.
- **The bounds start at the grant.** The executor's `timeoutMs` and `idleTimeoutMs` start when the command starts, after
  the grant: a wait for the host is not the command's time.
- **Wire.** The client writes `{"op":"cpu-tokens","want":<n>,"checkout":"<workspace root>","command":"<command>"}` and
  does not half-close. The gateway answers `{"ok":true,"lease":"queued"}` at once and
  `{"ok":true,"lease":"granted","tokens":<g>}` when the runner may start. `want` 0, a missing or empty checkout, or one
  over 4096 bytes is refused as `invalid-request`. `{"op":"cpu-budget"}` is one-shot and answers the ledger as
  `cpuBudget`: `total`, `held`, and per checkout `held`, `running` and `waiting`.
- **The budget spaces runners out; it never decides whether one runs.** With no gateway listening (any host without
  cowshed, CI included) the command runs exactly as configured and nothing is said. A gateway that predates the budget
  refuses the unknown operation at once (an older one, silent until EOF, is half-closed after 2 s), and the command runs
  unbudgeted with that reason on its stderr. A grant has no deadline: queued, the client knows the gateway is alive.
- **Spans.** The executor prints
  `cowshed: cpu-tokens wait done elapsed=<ms>ms status=ok tokens=<g>/<want> runner=<kind>` before the command's own
  output (`status=err … runs unbudgeted: <reason>` without a grant), so a slow or failing task says how long it waited
  and how many threads it ran with. With `COWSHED_TIMING` set, the gateway prints each grant: tokens, want, the wait,
  the checkout, and the client's pid and command.
- **Not budgeted.** Processes outside `bounded-exec` — Nx's own `nx:run-commands` builds, a host shell's `cargo` — run
  as before. The unbuilt Workload classes and priority section admits those managed root runners through the same ledger
  and leaves Cargo's internal jobserver under that root's grant; unmanaged host commands remain outside it.

Tests: `cowshed-gateway`'s `cpu_budget` ledger unit tests (fairness, share, starvation, leave) and `tests/cpu_budget.rs`
over a real socket (`SIGKILL` of a holding client returns its tokens to the waiter at once; checkouts share the host;
refusals); the plugin's `cpu-tokens.test.ts` (each runner's `want` and sizing, a killed holder closing its grant) and
`executor.test.ts` (a granted run sized and released, a refused one run as asked).

## Workload classes and priority

> **Implementation status: unbuilt.** This section specifies the gate-first extension to the shipped Host CPU budget,
> not measured performance of that extension. Its implementation and acceptance are in
> `agent-todo/cowshed-workload-classes.md`. Package docs continue to describe shipped behaviour.

The **current landing gate wins the host** over speculative implementation builds and suites. Class alone is not enough:
a high numeric priority among background jobs still competes with already-running background jobs, and a reserve for
small checks does not accelerate a gate incorrectly classified as bulk. One host-wide land turn identifies the current
gate; the existing CPU ledger gives its work first admission, and macOS demotes competing bulk processes.

- **`feedback`** is watched work: an incremental compile, a focused test, and the active land turn's rebase, gate and
  land checks. Only descendants of the actual land-lock holder have the **gate role**. `--class feedback` or a high
  priority does not manufacture that role. A queued land has no running gate.
- **`bulk`** is speculative or unattended work: implementation builds, full writer suites, mutation sweeps and release
  builds. Unclassified requests are bulk at priority 0. Bulk runs in the background band from its first instruction, not
  only when a later feedback request oversubscribes the host.
- **Priority** is a caller-supplied unsigned 32-bit integer, higher first, default 0. It orders queued lands and queued
  bulk within their own queues; no value outranks the active gate or buys the feedback reserve.

### Gate-first admission

There is one CPU ledger, not a second gate semaphore or a second resource scheduler. For `N >= 2` cores,
`R = min(ceil(N/8), N - 1)` tokens belong to ordinary feedback and `G = N - R` to the gate or bulk window. For one core,
gate and ordinary feedback share one token, FIFO between their runnable requests; bulk uses it only without a land turn
or waiting feedback. The one-core case never creates a zero-capacity queue.

1. **Without a land turn**, ordinary feedback uses at most R tokens, bulk at most G. Feedback never waits behind bulk
   but can wait behind other feedback; there is no unlimited one-token oversubscription per request. Bulk keeps the
   existing half-host grant cap and checkout fairness within one priority. A bulk request is aged after `2N` bulk grants
   pass it; aged requests go first by ticket, then higher priority, then fewer held tokens and ticket. Aging guarantees
   service only when grants continue and older work terminates, not a wall-time bound or eventual finish under an
   infinite sequence of land turns.
2. **During the land turn**, no fresh bulk resource grant is made, even between the gate's tasks and even to aged or
   urgent bulk. The gate's lineage can hold up to G tokens in total; an individual gate runner can use all G when its
   demand allows, rather than inheriting the half-host cap. Other feedback remains bounded by R. The next gate grant
   precedes any eligible bulk grant; FIFO within the gate and within ordinary feedback prevents a sibling from being
   repeatedly passed. Queued bulk retains its ticket and age. It resumes when the turn ends.
3. **Already-running bulk is demoted, not erased.** Its grant stays held until its command tree ends. Gate grants may
   overlap those old background grants, so the gate does not wait out a long suite before starting. The ledger counts
   the actual grants: `over = max(0, held - N)`, `free = max(0, N - held)`, `free + held = N + over`. This is bounded
   transition debt: gate plus ordinary feedback holds at most N; fresh bulk cannot increase the debt. No token is
   declared returned merely because a process was demoted. After the turn, no bulk is admitted until the actual holdings
   fit the ordinary budget again.

This is scheduler preemption through the OS band, not `SIGSTOP`, cancellation, command replay or live runner resizing.
No lowest-priority completion guarantee is inferred from a nice value. Existing resource ownership and cancellation
remain authoritative. QoS does **not** free resident memory or cap RSS: old bulk jobs retain RAM while they drain. The
loaded acceptance below must expose swap pressure rather than call an unsuccessful policy a latency repair.

### Bands

**macOS.** The gateway, supervisors, activation (`direnv`), Nx daemon and shared cache service keep their Interactive
launchd band (Placement and identity). Only work processes are demoted: before the bulk command executes, its child sets
`PRIO_DARWIN_BG` with `setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG)`, equivalent to `taskpolicy -b`. Feedback and
the active gate run in the normal inherited band. There is no irreversible `taskpolicy -c background` clamp and no
Background or Standard daemon `ProcessType`: demoting infrastructure would throttle the gate's own activation, hashing
and cache service.

A live change uses the same-user process API, equivalent to `taskpolicy -b -p <pid>` or `taskpolicy -B -p <pid>`. It
covers every existing member and subsequently observed joiner using the complete process-tree identities already used
for accounting and termination (11_shell.md), not a name-based host process scan. An exiting member is reconciled as an
exit; permission, identity or application failure is a named error and no successful band-change receipt. A spawned
child inherits its parent's band; clearing a parent does not clear children already forked, so every member must be
covered. The acceptance reads scheduled priority and behaviour for live changes: self `getpriority` alone is not a
reliable witness of an externally applied background policy.

**Shared compilers.** A bulk compiler spawned by the shared sccache daemon must not escape demotion merely because its
parent is infrastructure. Its compile request carries the requesting work's band through the existing single-flight
owner, and the compiler starts in that band. A gate or feedback client joining that same compile lifts that compiler
until the last such waiter leaves. This is a real blocking dependency, not promotion of every job sharing a cache or
build volume. The service itself stays interactive.

**Linux.** Every job cgroup is a direct child of the controller's one delegated cgroup, irrespective of workspace
(07_api.md, Complete job accounting). Feedback uses `cpu.weight` 100 and `io.weight` 100; bulk uses 1 and 1. Changing
weights is reversible without process migration or privileged nice restoration. Weights compare sibling job cgroups, not
arbitrary operator processes outside their delegated parent; `io.weight` is effective only where the device's I/O
controller supports it, including enabled `io.cost`. No Linux I/O or outside-cgroup guarantee is inferred from writing a
weight ([cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html), Weights and IO). The macOS loaded acceptance is
not evidence of Linux performance.

### QoS leases

A supervisor holds a `qos-lease` connection for each managed job, with the existing job identity, checkout, command,
`want` and either its own `class`/`priority` or a `parent` lease id (07_api.md `JobQos`, `QosSource`). Queued and
granted replies carry the stable lease id; a resource-consuming command starts only after its grant. A parent carries
its class, priority and verified gate lineage to `cowshed exec`, `cowshed land` checks and `bounded-exec`. The CLI reads
`COWSHED_QOS_LEASE`; the supervisor validates the lineage rather than trusting environment text as ownership.

`want` is the positive root runner demand, sized to its actual grant. Zero is band and lineage only for idle services
and orchestration: the holder, a shell, the Nx daemon or an Nx coordinator. It authorizes no unbudgeted heavy work. An
orchestrator's heavy children take individual grants; a root that holds tokens never asks the host again for nested
work. The admission key binds source kind, own class, want, stable parent id or land target as applicable, **not**
mutable numeric priority. A repeated Start finds the same job after a priority change; a different parent is a conflict.

`job-qos` changes an own lease's class and priority, or a land holder's queued priority, and applies the band to its
work. Parent followers answer `follows-parent`. A running grant is not resized or retroactively placed in a smaller
window: a class raise whose holdings cannot fit waits for its command to finish before the new admission class applies;
a priority-only change can reorder pending requests immediately. No class change creates gate authority.

**Restart.** Lease ids use a random gateway incarnation plus a counter. Supervisors retain the job, band, allocation and
id on a gateway disconnect and reconnect with `resume` and the actual held count. Duplicate live ids answer
`lease-live`; a queued request resumes with a new ticket and age zero. The gateway's recovery accounts surviving grants
before admitting fresh bulk; an unavailable owner is a named recovering state, not proof that its tokens are free. A
request whose parent has not resumed is recorded as bulk 0 with one stderr explanation; it follows the parent once the
parent resumes, but cannot receive the gate role until the durable turn owner is verified. A resumed grant may create
`over` and remains counted until its command ends. Connection loss never creates a duplicate job.

### The land lock

There is **one host-wide land turn across all repositories and targets**. It serializes rebase, merged-tip gate and
land, not editing or already-running bulk work. The existing `land-lock` queue uses target identity, priority and
ticket. A waiter ages after two completed turns; aged waiters precede other priorities by ticket. Thus a newly urgent
land waits behind older aged waiters; changing priority never interrupts the current gate.

The reply names the lease id and the single controller-owned `<host-store>/land.lock` file (01_storage.md). The gateway
orders; every holder takes an exclusive `flock` on that same file before activation. The gateway marks the verified
file-owning turn active before any gate child receives its role. Waiting for the grant or file holds no CPU, RAM-temp or
disk-lifecycle lease. A caller under a live CPU pool or other CPU grant is refused as `Usage`, preventing the
lock-waiter/token-holder cycle. Host turn → target lifecycle locks → CPU admission → temporary resources is the order;
cleanup releases resources without re-entering admission.

**Durable custody, including the verb.** `QosSource::LandLock { target, priority }` admits a keyed, want-0 holder job.
Its existing workspace supervisor owns the file, not the executor or CLI. `cowshed land-lock <target> -- <cmd…>` uses
the same holder and runs the command with `COWSHED_QOS_LEASE` and `COWSHED_LAND_LOCK` naming its canonical target.
Same-target descendants reuse it only after their parent lineage is verified. A marker for another target is a `Usage`
refusal, not recursive acquisition of the host file; a different land must be submitted outside that turn. With no
gateway the same supervisor/file path still excludes, but reports that priority ordering and gate-first admission are
unavailable; it never claims accelerated execution without a live ledger.

The durable `land.turn` record beside the stable lock inode names the holder and each follower's existing job identity,
registered **before spawn** through the existing atomic, synchronized record-commit mechanism. The locked inode is never
replaced to update custody. A failed record commit starts no child and releases no unfinished turn. Normal completion or
cancellation closes admission to new followers, ends or waits for the current followers through the existing termination
path, and releases the turn only after they are terminal. A gateway restart does not release the supervisor's file. An
executor restart reattaches by admission key. If the supervisor dies and the kernel releases the file, the successor
that acquires it reconciles the unfinished turn's recorded jobs before activating a new one; unprovable liveness is
`Conflict` with `admission: { reason: "turnRecovering", unproven }` (07_api.md), never an empty host. This also covers
the CLI dying during a detached gate: ending a connection is not evidence that its descendants ended. Target ref
compare-and-swap remains unchanged.

Land checks inside the turn use `--no-retire` (`LandOptions.retire = false`), and a retire request there is `Usage`
before target mutation (02_workspaces.md). After land and follower termination release the turn, its caller may retire
the workspace normally. The land cannot wait on retirement of the same supervisor and holder waiting for it to finish.

### `bounded-exec` under a class

Reuse its shipped runner sizing, parent-held CPU pool, RAM-temp leases and the gateway's disk-lifecycle leases. A
positive-want root's nested executors draw only from its existing pool, whose sibling allocations sum to its grant.
Cargo/make use their normal jobserver inside that one sized root; there is no new per-job FIFO in which every
independent executor invents an extra implicit token.

A want-0 orchestrator's supervisor exposes a CPU-only admission endpoint, using the existing `cpu-tokens`/pool wire and
an exact sandbox path grant. It forwards child requests with the supervisor-owned lineage and retains each host grant on
that child's connection; it owns no root grant while doing so. This is not access to the gateway's other control
operations. A nested executor under a positive grant cannot fall through to this endpoint and take twice. Grant loss and
cleanup use the existing pool failure path.

All resource-consuming tasks beneath an orchestrator use this admission path. Inferred and authored gate builds still
using `nx:run-commands` migrate to `bounded-exec` or an explicitly sized managed root; a class marker on an Nx parent
alone does not cover those tasks. The gate task graph is checked for escaping compute runners as part of acceptance.

The class defaults to bulk; a verified parent wins over a target's own class option. CPU admission precedes RAM-temp
acquisition, so a queued bulk task holds neither a RAM-temp volume nor a disk phase while the gate runs. A temporary
resource failure returns the CPU grant exactly once; cancellation and exit close the child pool before returning its
upstream grant. The RAM-temp lease is temporary storage, **not a host RSS budget**. Existing wall-clock bounds, per-test
timeouts, verdicts and bound-only re-runs stay unchanged. No run clock or run estimate is required or claimed.

### The live view and loaded acceptance

`cpu-budget` adds the active gate's target and lease, G/R windows, actual held tokens and `over`, queue/admission
reasons and pending band/recovery errors to its existing live view. Existing job CPU windows and transition spans remain
the resource evidence. There is no sampled-metrics log or second monitoring daemon. Each gate's transition record
reports request-to-terminal wall time (including turn wait), active-turn-to-terminal wall time, command wall time and
CPU/RAM admission waits, cache hits and timeout outcomes; a queue wait is not hidden by starting the clock after
admission.

Acceptance is a real macOS run, not a scheduler simulation or a claim based on `ps`. On the same commit, command,
toolchain, fixed task set and equivalently warm sheds, record at least three solo gates and three alternating matched
policy-disabled/policy-enabled pairs. Start the gate only after 20 real parallel build/test workers are live; retain a
pending backlog and enough real cache-miss work to overlap the gate, rather than replacing it with CPU spinners. Both
arms retain the existing safety leases. Record all raw durations, workload/cache identity, bulk completions, feedback
latency, timeouts, core count and start/end load, swap and resident-memory snapshots in the acceptance artifact. The
small-sample p95 uses nearest rank (and is labelled with its sample count), not a population-confidence claim.

The enabled loaded median active-turn wall time is at most twice the solo median and at most half the disabled loaded
median, with no new gate timeout or failure. Ordinary feedback latency is reported beside its solo control; bulk resumes
after release. Request-to-terminal and turn wait are reported independently so serialization cannot disguise starvation.
Every failed arm is retained. Until that run passes, this is an unbuilt policy, not evidence that a 15–60-minute gate or
swap pressure has been repaired. Unmanaged host processes are identified as outside coverage, not silently demoted by
PID guesswork or omitted from the result.

## Credentials

- Secrets use macOS Keychain generic passwords under service `dev.cowshed.gateway`; Linux runner credentials use the
  platform mechanism specified in 10_ci.md. Records carry protocol, exact HTTPS origin, admitted methods, normalized
  path/package/module scopes, and project `repo_id` where applicable; a bare host-only secret record is invalid.
- Credential lookup happens only after endpoint identity, token, project admission, CONNECT authority/SNI, method, and
  normalized path checks. Rotation is observed on next use. Values are never logged, cached in telemetry, placed in
  URLs, or returned to clients.
- Each workspace CA private key is controller-owned mode 0600 outside every mount. The matching public certificate is an
  in-image server trust anchor only. Create/fork mint a key; destroy removes it; restore rotates it and closes existing
  intercepted connections before admitting execution.
- The gateway process is host-side and credential-store access is restricted to that executable identity.

## Startup contract: eager heal, always

The gateway is `RunAtLoad`, so it is the first cowshed process alive after a reboot, and its startup pass is where
mounts are restored. In order:

1. Validate the host store — both dedicated volumes present, mounted, marked, and with canonical flags (01_storage.md).
   A store that fails validation stops here and reports; nothing below can be meaningful without it.
2. Count the recorded projects and list the workspace supervisors still serving from before this daemon, then serve: the
   control socket and the supervisor manager answer from the first moment. Status answers throughout, and so do the
   audit, mirror, and simulator requests, none of which reads a workspace mount.
3. Eagerly heal every recorded project's mounts: every project's main attached and mounted at its checkout path first,
   then every other workspace under the mount root (02_workspaces.md). Each step is a lifecycle span of its own —
   `startup-heal discover`, `startup-heal open <repo>`, `startup-heal mount <repo>/<workspace>` — so a slow pass reads
   as the step that spent the time.
4. Restore every attached workspace's session into the gateway from what step 3 mounted.

Until step 4 ends, status carries `healing`: `{ mounting: { projects: N } }` with the projects not yet mounted (never 0;
a project counts once its sessions are mounted), then `restoringSessions`; it is absent once the pass is over, and
`cowshed gateway status`, `doctor` (`gateway-starting`), and `gateway start`'s wait say so in those words. Everything
that depends on what the pass restores is refused by type meanwhile, never reported as an absent gateway: a session
`install` or `remove` on the control socket with failure code `healing`, every command's reconcile with a `Conflict`
carrying `healing` and the `cowshed gateway status` hint, and every supervisor ensure likewise. Sessions are restored
from the attachment facts the pass is still changing, so a session installed or removed before it ends would race that
restore; a supervisor would start in a workspace the pass may not have mounted yet. The supervisors listed in step 2 are
recovered once the control socket is bound, all at once and in the background, alongside steps 3 and 4 (11_shell.md
"Supervisor recovery"): while any is still being recovered, status carries `recovering: { supervisors: N }` (never 0;
absent once none is left), and only a command for one of those workspaces is refused, by the typed recovering refusal.

Serving before the pass ends does not weaken it: the guarantee below is about the checkout path, which no request to the
gateway makes appear any sooner or later, and answering at once is what lets every client tell "still starting" from
"not running".

Eager, not heal-on-contact. Adoption's guarantee is that the checkout path is never absent and never dangling, and a
reboot is the one window that guarantee has to survive as much as the publication transaction does. Without a startup
pass the window is real and user-visible: the checkout is an empty directory showing the self-healing stub — in the
user's editor, in their shell, and in Finder — until first contact heals it. "First contact" is not a moment cowshed
controls, and a user reaching a broken path and then watching it repair is not the same product as the path simply
working.

Heal-on-contact remains, as the fallback for everything that appears after startup: an image published while the gateway
is already running, a gateway restart mid-session, a workspace detached and reattached by hand. The startup pass closes
the reboot window; the stub closes everything else.

Failures are per-project and never block the gateway from serving healthy ones. A project whose store, image, or mount
cannot be healed is recorded as a finding, surfaced by `cowshed doctor` with its `next:` hint, and skipped; the
remaining projects are healed and the gateway serves. A single unhealable project taking the daemon down with it would
convert one broken checkout into a machine with no gateway at all.

**autofs/automount is declined.** Delegating mount-on-access to the platform would replace the startup pass with a
kernel-driven one, and is rejected for three reasons: its configuration surface is root-owned, which puts a
per-user-project tool's mount table under administrative install rather than the user's own control; macOS 26 adds
attestation friction to automount that cowshed cannot satisfy without further privileged setup; and it introduces a
second mount pathway that every existing invariant — kernel mount facts, marker validation, flag canonicalization,
crash-window classification — would have to be reconciled against, for mounts cowshed did not perform and cannot fence.
One mount pathway cowshed owns end to end is worth more than the eager pass it would save.

## Availability and offline behavior

Two distinct situations, not to be conflated:

- **Gateway absent** (daemon not running): there is no local process to serve anything — registry requests fail until it
  starts. `cowshed doctor` exits 5 with the kickstart command in the `next:` hint. Builds that need no new packages
  still work: the shared bun cache covers locked, previously-installed dependencies.
- **Gateway up, upstream offline**: mirror cache hits are served without upstream — anything previously seen installs on
  a plane. Misses fail fast with a `cowshed:` note distinguishing "offline" from "denied" so agents don't request grants
  to fix a network outage.

## Audit events

One audit event per decision, written as **Arrow segments** under `/private/cowshed/store/telemetry/` (schema, flush
policy, and durability window in 13_telemetry.md) — on the store volume, denied to every sandbox (04_sandbox.md),
because this is the authoritative egress record. There is no separate audit file and no separate gateway log file: audit
events and gateway operational events are rows in the same telemetry store, distinguished by `kind`.

Read it with `cowshed audit` (06_cli.md) — human tables by default, `--json`/`--ndjson` to pipe. The same events,
rendered:

```
$ cowshed audit --ws raven --ndjson | head -5
{"ts":"2026-07-11T12:34:56.789Z","ws":"raven","port":40960,"rev":7,"kind":"npm","name":"react","status":200,"bytes":31245,"cache":"hit","traceId":"4bf92f…"}
{"ts":"…","ws":"raven","port":40960,"rev":7,"kind":"intercept","host":"api.example.com","method":"POST","path":"/v1/run","status":200,"bytes":8123,"traceId":"4bf92f…"}
{"ts":"…","ws":"raven","port":40960,"rev":7,"kind":"opaque","host":"pinned.example.com:443","status":200,"bytes":51200}
{"ts":"…","ws":"raven","port":40960,"rev":7,"kind":"connect","host":"api.example.net:443","status":"denied"}
{"ts":"…","ws":"raven","port":40960,"rev":7,"kind":"repo-mirror","url":"https://github.com/tinylibs/tinybench","status":200,"bytes":184201}
{"ts":"…","ws":"raven","port":40960,"rev":7,"kind":"sim","verb":"openurl","target":"booted","status":"ok","traceId":"4bf92f…"}
```

Intercepted requests carry request-granular fields (`method`, `path`); an `--opaque` tunnel is host-only by
construction. `traceId`/`spanId` columns tie each event to the trace that caused it (13_telemetry.md). Every denial
names the grant that would permit it — the audit store doubles as the debugging tool for "why can't my agent reach X".

## Tradeoffs

**Per-workspace CA interception.** A workspace-scoped CA limits a signing-key compromise to one workspace and lives with
the gateway, which already mediates upstream credentials. The explicit costs are per-tool trust-anchor wiring, opaque
fallback for pinned clients, and gateway visibility into intercepted plaintext.

**Registry mirrors retained.** Protocol-aware mirrors provide digest validation, fleet-wide deduplication, bounded cache
storage, and offline reads that generic interception cannot.

**Git data-plane protocol rejected.** Host-granularity egress cannot constrain repository mutation. Coordinator-only,
project-scoped mirror fetches make the no-push boundary structural.

**Platform-specific data-plane identity.** macOS uses a port block because it lacks per-process network namespaces;
Linux uses a private netns, one per-incarnation Unix socket, and its namespace-local compatibility connector, allocating
no port block. A host-shared listener or token-only identity is rejected because endpoint isolation, not a bearer
secret, must select the workspace.
