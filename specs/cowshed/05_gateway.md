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
descendant, and a descendant cannot leave it; the daemon's supervisor manager starts every workspace supervisor, which
starts every shell host and job. Background runs them at priority 4 and Standard at priority 20 (the utility band), and
both throttle their I/O behind the host's. Under either clamp a loaded host delays a fresh workspace's supervisor, its
first `direnv export json` and cold index reads many times over their unclamped cost. The sccache LaunchAgent is
Interactive for the same reason (launchd.rs `PROCESS_TYPE`).

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
  `retainedPortBlocks`, which carries no gateway listener). `portBlock` is allocated at fork (adopt for main), preserved
  across restore, and present only in macOS grant files. The gateway binds `base`; `base+1 … base+size-1` are workspace
  service ports. A session carries its block's recorded size, and the gateway validates the endpoint against it.
  Seatbelt permits a workspace to connect only to its current and retained blocks. The destination `base` listener is
  the primary, kernel-enforced workspace identity. Of two sessions whose blocks share a port, the gateway serves the
  older workspace's and not the newer one's, at startup and in every per-project reconcile, with
  `next: cowshed doctor --repair`, serving every other session (04_sandbox.md, `portBlock` collisions); neither startup
  nor a reconcile fails over a collision.
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

Port-block inventory is macOS data even when a pure inventory test runs on another host. Its session conversion
validates the recorded macOS block; actual gateway installation always validates the current host's topology and rejects
TCP sessions on Linux. A namespace-local connector is never a TCP gateway-session endpoint.

The allocator's publication marker excludes other cowshed creators while it binds all ports in the candidate block.
Those listeners are released only after the image and grant are published. A separate host process can still claim a
port before the daemon installs the session: `AddrInUse` is a typed control refusal, not a stale-grant success.
Workspace job reconciliation returns that refusal with the maintenance remedy; it never reads host-wide grants to find a
new block. Only project lifecycle/maintenance reconciliation chooses a new unassigned block under the same reservation
and inventory check, publishes a higher grant revision, rereads the authoritative session, and retries. A refused base
is excluded for that reconciliation, so a retry cannot alternate between occupied ports. Other installation errors
remain errors.

Every data-plane request additionally carries exactly `Proxy-Authorization: Bearer <opaque-token>`. The token is 32
random bytes encoded as unpadded base64url, lives at `.cowshed/token` mode 0600, and is defense in depth rather than the
workspace selector: the already-selected macOS listener or Linux socket chooses the workspace before comparison. A proxy
client may present it as `Proxy-Authorization: Basic` with the token as password (what curl, libcurl, reqwest and Go
send for proxy-URL userinfo). The Basic username authenticates nothing: `job-<job_id>` (a job's proxy URL, 03_caches.md)
is recorded as the request's job attribution (13_telemetry.md "Attribution tiers"), and any other username, `cowshed`
included, leaves the request to the query-time interval join. No other header carries the token: a request's
`Authorization` is the client's own and authenticates nothing. The one non-proxy exception is the local Nx cache route
below: stock Nx sends its bearer in `Authorization`, so only that exact route accepts that header, after endpoint
selection. The gateway accepts no cookie, query parameter, URL userinfo, or path token; it strips `Proxy-Authorization`
and `Authorization` before any upstream request. It decodes the presented value to bytes, rejects malformed or
wrong-length values, and compares all 32 bytes in constant time. Missing or mismatched token is 401; a local Nx
authentication refusal is 401 with exactly `Content-Type: text/plain`, and a proxy refusal uses the proxy's 407
challenge.

Create and fork mint a token. Restore stops admissions, drains the Linux connector and gateway connections, kills the
connector cgroup, rotates the token, unlinks/recreates the Linux socket and namespace-local connector when applicable,
and atomically rewrites the in-image token before new execution is admitted. Detach performs the same drain, cgroup
kill, and socket unlink without rotating persistent authority; attach creates the socket and connector before admitting
execs. The old token and every pre-restore keep-alive or tunnel are therefore invalid immediately; a preserved macOS
port does not preserve authority. A policy miss after successful endpoint and token authentication is 403 with a
machine-parsable grant hint naming both remedies, the standing one first: `cowshed grant --project-wide --egress <host>`
(every workspace of the project) or `cowshed grant <ws> --egress <host>` (that workspace). The hint names the bare
canonical DNS name or IP address, never a CONNECT authority: the grant host grammar is shared with grant admission and
exec (04_sandbox.md).

Main is a first-class data-plane client with identical platform wiring and policy. Gateway startup and absence are
covered under "Availability and offline behavior".

## The installed cowshed

The gateway daemon and the `cowshed` an operator types are one artifact: the installed cowshed, under
`~/Library/Application Support/dev.cowshed/bin`, on the volume that carries `~/Library/LaunchAgents` itself, so launchd
reaches it after a reboot whatever volume the build came from (a workspace image, the nix store, a checkout).

| Name in `bin`            | What it is                                                                                     |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| `cowshed`                | The stable name: a symbolic link saying `cowshed-<sha256>`, relative to `bin`.                 |
| `cargo`                  | The jobserver launcher: a relative link saying `cowshed`, so install and rollback switch both. |
| `cowshed-<sha256>`       | A stored copy, named for the SHA-256 of its bytes (64 lowercase hex digits), mode 0755.        |
| `cowshed-<sha256>.build` | That copy's build record, JSON `{"commit","commitTime"}`, written by a build that records one. |
| `nx-cache-server`        | The Nx cache server's stable name, a link to its own stored copy `nx-cache-server-<sha256>`.   |

`cowshed-source`, one level up beside `bin`, records the path the current install came from and its package version.
`install.lock`, beside it, is the install slot's lock (below); it is never removed.

**The Nx cache server is JavaScript run by the shell's `bun`** (tg#7504, tg#6789). Neither `@smoothbricks/cowshed` nor
`@smoothbricks/nx-cache-server` ships a compiled server or an interpreter. An install copies the build's server bundle
into `bin` the same content-addressed way: for a distribution (`<package>/dist/bin/<platform>/cowshed`) it is
`node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js`, resolved as Node resolves the dependency
from the package and then each ancestor (an isolated Bun store, a hoisted install and a checkout's workspace link
alike); for a build the slot already holds it is the slot's own `nx-cache-server`. A build with neither is refused by
name before anything changes. The gateway agent's plist sets `PATH` to the directory of the first `bun` on the
installing shell's `PATH`, ahead of launchd's default `/usr/bin:/bin:/usr/sbin:/sbin`, **named as a profile nix or
devenv keeps, never as a `/nix/store` path** (tg#7504, tg#7551). A plist outlives the shell that wrote it and nothing
roots a store path for it, so after a devenv update and a nix GC a recorded store path is gone and every backend start
fails. The entry is kept as `PATH` spells it, links unresolved — a nix profile's `bin` such as
`/etc/profiles/per-user/<user>/bin`, or a devenv `.devenv/profile/bin` — so the agent follows the profile when it is
re-pointed. A devenv shell puts its own profile on `PATH` as the immutable store path it evaluated (`DEVENV_PROFILE`);
that entry is recorded as `$DEVENV_DOTFILE/profile/bin`, the link devenv re-points on each activation and keeps alive
through the `.devenv/gc` root it registers. Any other `/nix/store` entry is refused, as is a shell with no `bun`; both
refusals name the setup step, and nothing beside cowshed stands in. Setup runs from main's devenv shell: a shed's
profile link disappears with the shed. The daemon starts each project's backend as
`<bun on its PATH> <bin/nx-cache-server> --exit-on-stdin-eof`; a recorded profile that no longer holds `bun` (a removed
checkout, a main not yet mounted) fails that start by name, an outage until the next start finds it.

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

**Install order and rollback.** Every install holds the install slot (below). An install that replaces the running build
refuses a debug build and a build that does not contain the installed one (below) before anything is installed. With the
gateway agent installed it then writes the plist if it changed, and retains the previous build: when the stable name is
a link, the copy it names is already stored; when it is the plain binary the layout before content-addressed copies
left, that binary is first stored under its own digest through the same copy and rename. It stores the new copy, moves
the stable name, records the source and the build record, and activates the agent. A failed activation points the stable
name back at the retained copy, records `cowshed-source` as restored, and loads the agent again; the error says what
failed and whether the restore and the reload did. A first install has nothing to roll back to and keeps its copy. A
setup on a host with no gateway agent installs the same way without the activation. Setup installs only after its
storage repair succeeded.

**An install waits for the gates in flight.** Activating the agent restarts a gateway that runs, and so does the host
move inside `setup`; a restart closes every lease connection ("Crash safety by connection"), a land's gate-job pool with
them, and every task that gate admits afterwards is refused. Measured 2026-10-18: an install restarted the gateway at
17:00 while a land's gate ran, its next tasks failed with
`cpu-tokens parent pool refused: the gateway closed the lease connection`, and the land failed. So an install — holding
the slot, after its refusals and before the activation or host move that would restart a gateway that answers `status` —
first waits until that gateway's `cpu-budget` ledger shows no gate-class work holding or wanting tokens (`gateJobs` and
`gateQueue` both empty, "Host CPU budget", Wire). It reads those two fields alone, each entry's `checkout` and `job`,
and nothing else of the ledger: the gateway it is about to replace is of another build, and a ledger field one build has
and the other lacks must not turn the wait into a restart that cuts a gate off. It reads the ledger once a second, with
no deadline, as no admission has one ("No admission deadline"): the gates end, and the install goes on. It says on
stderr which gate jobs it waits for (`gate job <n> at <checkout>`), once and again whenever they change, and when none
is left. A gateway that does not answer `cpu-budget` is not waited on, and the install says so. An install that restarts
nothing (no daemon answers, or the refresh finds the agent current) does not wait. The restart and its refusals
themselves are unchanged.

**Pruning.** After an install succeeds (for the gateway, after its activation), every stored copy and build record but
the new build's and the one it replaced is removed. The replaced build stays one install longer: a workspace supervisor
of that build, still draining its jobs (11_shell.md "Draining a supervisor of another build"), starts the shells those
jobs need from the path it started from. A failed prune is reported on stderr and never fails the install.

**The install slot.** The installed cowshed is one slot, so one install fills it at a time. `setup` (repair and
`--uninstall` alike), `gateway start` and `gateway stop --purge` each take an exclusive `flock(2)` on
`dev.cowshed/install.lock` before they read what is installed, and hold it until the command exits; the kernel drops it
when the process exits or dies, so no crash leaves the slot held. It is the lock `lockf(1)` takes, the shape of
`~/Dev/.land-lock.sh`: a second installer waits in line, says on stderr that it is waiting and for which file, and then
judges its build against whatever the install it waited for left behind, because the judgement below runs after the lock
is taken. A host move inside `setup` restarts the gateway under the slot `setup` already holds. This exists because two
workers each built and installed a cowshed within an hour, neither aware of the other's commits, and nothing serialized
who filled the slot.

**A pending install is visible.** `cowshed gateway status` says when an install holds the slot, without taking it from
the install: it tries a shared `flock(2)` on `install.lock` without blocking, which fails exactly while an install holds
it, and lets go at once when it succeeds (an installer arriving in that instant waits for it as for any install, and
says so). While it waits for gates ("An install waits for the gates in flight"), the holder records the gate jobs it
waits for in `install.wait` beside the lock, written whole and renamed into place on every change and removed when the
wait ends; an installer removes a record a dead holder left when it takes the slot, so while the slot is held the record
is the holder's. Status reports `pendingInstall: { waitingFor: [{ job, checkout }] }` (empty while the holder waits for
no gate), absent while the slot is free; `gateway start`'s own report, which holds the slot, carries none.

**Build records and the basis.** A release build records the commit `HEAD` names and that commit's committer time
(`build.rs`: `COWSHED_BUILD_COMMIT`, `COWSHED_BUILD_COMMIT_TIME`); the time is read from that one commit, so a shallow
clone records it too. Its Git HEAD/reflog watch paths are emitted relative to the build-script manifest directory, so a
warm Cargo target clone follows its own repository rather than retaining an absolute watch on its donor. The owner
script directive control and a real warm Cargo clone pin this boundary. A debug build, and a build made where git cannot
answer (a source archive, a nix build), records nothing. The installing build writes its record beside its stored copy,
so the installed build's record is read from a file and nothing is executed to learn it; a record that cannot be written
is said on stderr and does not fail the install. The time describes a build to a person and never orders two builds: two
sibling commits are ordered by clock, and the later one carries none of the earlier one's fixes.

An install that would replace the installed build is judged by containment. Nothing is judged, and the install proceeds,
when nothing is installed, when the stable name is the plain binary of the old layout, when the installed copy has no
`.build` record (installed before records existed, or by `--downgrade` from a build that records nothing — it names no
commit to keep), when the bytes are the installed bytes, or when the candidate records the installed commit. An
installed record that exists but cannot be read is `Integrity` naming the file, never read as absent: it says a commit
was installed and will not say which. Otherwise the candidate must record a commit, and that commit must be on the
`main` branch of an adopted project's own checkout (the adopted-project registry, read live). That main judges it: every
commit of `<candidate>..<installed>` must be held by the candidate, by ancestry or by patch identity — exactly the
containment `cowshed rm` requires (`GitRepository::commits_not_held`, the list form of `commits_already_held`:
`git cherry` equivalence for ordinary commits that change something, and merges that author nothing). Main is the
authority because it outlives every workspace: a workspace's commit may be rebased away when it lands and the workspace
removed, while main only moves forward. A rebased or cherry-picked commit, or a squash of one commit, is held by patch
identity; a squash of several commits matches none of them, and is refused naming each.

Every refusal is `Conflict` naming the binary it would have installed and both builds
(`commit <12 hex>, committed <RFC 3339 UTC>`). A candidate that lacks installed commits names each one as
`<12 hex> <subject>`, the first 20 and then `… and <n> more: git log <candidate>..<installed>`. A candidate that records
nothing, one whose commit is on no adopted main (each main and why is named), and one whose comparison git cannot make
(the installed commit absent from that main) are refused too: not knowing is not permission. The hint is "rebase this
build's checkout onto `<installed 12 hex>` (or merge it), rebuild, and run that build's `cowshed setup`;
`cowshed setup --downgrade` from this build installs it anyway and removes what it lacks". `setup` reports it once for
the gateway and the CLI, repairs everything else, and exits non-zero with that `Conflict`; `gateway start` refuses
before it installs anything. `setup --downgrade` installs the invoking build without judging it, and is valid only for a
run that installs: not with `--uninstall` or `--force`. This exists because a guard that compared commit times would
have let a release train's build of a sibling commit replace an installed build carrying a gc fix it lacked, and said
nothing; before that, twice a `cowshed setup` run through a launcher that resolved to a stale checkout build put back a
gateway without a landed feature.

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

### `/nx/v1/cache/{hash}` — accepted-project Nx artifacts

**This cache is an optimization for cross-shed reuse and never blocks a build.** Every refusal or failure on this route
reaches the client as a miss (`GET` 404) or not-stored (`PUT` 403). The task then runs, or keeps the result it already
produced, exactly as it would with no cache. The only exception is a credential that is not this workspace's, which is
401 (Placement and identity). Stock Nx 23.2.1 fails a task on any other status (Busy gateway, below), so the gateway
never answers or forwards one: drain, a closed audit sink, a stopped actor, a queue bound, a body timeout and a backend
response head that Nx cannot read are all the method's 404/403. The audit and the log record the cause.

Every checkout may read its project's accepted task artifacts, during ordinary iteration as well as adoption. Nx keeps
its local artifact cache and task database private to that checkout (16_build_volumes.md); this route shares neither
path nor live database. The controller wires stock Nx's `NX_SELF_HOSTED_REMOTE_CACHE_SERVER` to `<base>/nx`, without a
trailing slash, and its access token to that endpoint's read authority. The backend's bearer credentials remain
host-side. No scratch cache receives WIP artifacts.

**Trust domain: one user, unverified producers.** This store is one host directory owned by one user. Every writer is
that user's process, so the write boundary does not defend against a malicious writer, and CREEP's multi-tenant attacker
(CVE-2025-36852) is not this deployment's threat. It defends against an unverified producer instead. A shed can build
under an input its task does not declare, such as another toolchain resolved through a shed-private `tools/bin`. The
result is stored under a hash that is correct by the declared inputs. The store is first-writer-wins: `link(2)` answers
409 to every later writer, so that artifact becomes permanent. Every later reader would restore it, including a land
gate and main's adoption checks, which must never be fed an artifact nobody verified. Only a land publishes, because the
land gate verified its hashes. A shed holds read authority only. Its `PUT` 403 is an authority refusal, not a
reachability failure: the backend is a local process. That 403 costs the shed the archive work below and the benefit of
publishing, never the task.

The listener or Linux socket selects the workspace, its session selects the project, and only the exact, query-free
`/nx/v1/cache/{hash}` route accepts Nx's `Authorization: Bearer` credential. A hash is one validated Nx task key: 1-128
characters of `[A-Za-z0-9_-]`, the backend's own key charset. Nx uses a task's hash verbatim as its cache key, so this
admits stock Nx's decimal hash and any hex, decimal or base64url value an executor's custom hasher returns; it excludes
`/`, `\`, `.` and `%`, so a key is never a filesystem path or an escaped one. A final segment outside it does not name
this route and is a 400 `invalid-target`. The gateway forwards `GET` to the selected project's backend as
`GET /v1/cache/{hash}`. The publication receipt and the publication capability validate hashes by this same rule. An
ordinary workspace credential has read authority only: `PUT` is 403 before any artifact is staged or forwarded. A
sibling token, an expired transaction token, an ambiguous path, or a different project never acquires write authority.

Publication is private to an active, authorized land plan. The authorized initiation accepts the caller's exact expected
source head; external source review remains the caller's workflow, not a gateway interpretation of a model's ACK. Before
issuing temporary write authority, the controller requires a protected successful-check receipt. That receipt is not yet
specified (16_build_volumes.md has no section for it), so no controller path issues write authority and no land
publishes. When the receipt is specified, it binds the project, source incarnation, clean accepted head, successful
check/job identities, and exact successful current task hashes. Only those hashes may be forwarded as `PUT` under that
transaction. The ordinary worker cannot create or replace the receipt, request a publication operation, or obtain
backend write credentials. A changed head or failed, forged, missing, or foreign receipt refuses publication. Land
without qualification reports it explicitly and does not publish.

The public publication-control lane remains removed. The actor accepts only a hash-scoped capability installed by an
in-process caller; it does not verify a land plan or protected receipt. Publication authority remains controller-owned,
never a public workspace operation or a standing worker grant.

The controller revokes the temporary authority on completion, refusal, cancellation, or interrupted land. It is not
written to `.cowshed/env`, shell configuration, source, or a later job's environment. Main's adoption checks and every
ordinary exec remain read-only. A gateway restart restores ordinary read sessions from inventory, never a writer from an
audit trail or an unfinished transaction.

The existing artifact server is a controller-owned foreground child. The gateway passes `--exit-on-stdin-eof` and
retains the write end of its stdin pipe; EOF drains the server and releases its store lock. A gateway SIGKILL closes
that descriptor in the kernel, so `kill_on_drop` is not the parent-death mechanism. Ordinary standalone server
invocation omits the flag and retains its signal-driven lifecycle. A real kill/reopen proof and a no-flag orphan control
pin this lease.

Before reclaiming staging or serving requests, the backend holds an exclusive nonblocking kernel `flock` on a
persistent, never-unlinked `.owner.lock`. One small `bun:ffi` module calls libc's frozen `flock(int, int)` ABI
(`libc.dylib` on macOS, `libc.so.6` on Linux). The backend owns the `FileHandle` through request drain and cleanup;
SIGKILL releases it in the kernel. `EWOULDBLOCK` is a typed store-locked refusal, other errno is a loud startup failure,
and unavailable FFI has no fallback. A second process must refuse before staging reclamation; kill/reopen and stdin-EOF
proofs pin exclusion and lifetime. Report native macOS proof separately from Linux compile/typecheck coverage.

The gateway preserves the existing self-hosted cache protocol and artifact bytes. `GET` returns 200 for a complete
artifact or 404 for a miss; `PUT` returns 200 for a publication, 409 for an existing key, or 403 for a read-only
refusal. An incomplete, interrupted, or evicted artifact is never a successful empty restore. Project backend lifecycle
and bounded eviction belong to 03_caches.md.

**Backend outage.** Before committing a response head, the gateway maps a project's backend startup, health, connect,
handshake or response-head failure, and any upstream status Nx cannot read (5xx, or anything but `GET` 200/404 and `PUT`
200/403/409), to a miss (404) for `GET` and a refusal (403) for `PUT`. It audits each as `failed` with classification
`nx-backend-unavailable` and logs the cause loudly, naming the upstream status. A startup or connection refusal forwards
no artifact, but an upstream status may arrive after request bytes have already been sent. An outage is never a
successful read: its 404 is not evidence that a hash is absent and never counts as a hit.

**Busy gateway.** Nx requests wait under the gateway's queued-request bounds whether their backend is starting or
`Ready`. A full queue is an audited `limited` `nx-queue-limited` 404/403 and an expired queue wait an audited
`timed-out` `queue-timeout` 404/403, never a 429 or 504. Stock Nx 23.2.1 retries no `GET`, ignores `Retry-After`, and
reads every status but `GET` 200/404 and `PUT` 200/403/409 as a fatal misconfiguration (`native/cache/errors.rs`
`convert_response_to_error`), so a busy gateway answers what a cache may always answer: not served now. Like an outage,
that 404 is not evidence of absence and never counts as a hit. The price is work, not failure: a lookup wave wider than
the queue re-runs its refused tasks, even ones the cache holds, and a refused publication is not stored. That extra work
is the designed cost of a busy gateway, not a regression. Drain and audit fail-closed keep their own audit status and
classification, not a backend outage's, but answer the same 404/403. Unsupported Nx methods are denied before starting
or waiting for a backend.

**Backend lifecycle isolation.** Each repository's backend is supervised by a per-repository lifecycle state
(`Starting`, `Ready`, or `Failed { retry_after }`) held by the actor; backend startup and readiness waiting run outside
it. Concurrent requests for one repository share one start (single-flight). A failed start records
`Failed { retry_after }` with `retry_after` taken from `GatewayTimeouts.connect` (default 5 s); until it elapses,
requests for that repository are refused as in the outage above, without starting another child, and other repositories
are unaffected. If the child exits, the next request replaces it immediately within the same call. A startup worker's
task failure is reported through the same failed-start transition; it cannot leave an unreported `Starting` state.
Cancelling the reporting task also aborts its owned worker. A host-configured backend cannot replace a gateway-managed
live child. Readiness logs the project, address and reclaimed staging-file count, never credentials. A backend lives
with its project's sessions: when the last session of a repository leaves the gateway, the gateway stops the child it
started for it (a start in flight is aborted) and forgets its state, so the next session's first request starts a new
one; a host-configured backend owns no child and stays. Measured 2026-10-19: seven of the gateway's nine servers served
scratch projects that tests had adopted and discarded hours earlier, each running until the gateway restarted.

**Outage before and after the response head.** The statuses above apply only before the response head. Once the upstream
commits a success head, the gateway keeps `200` and cannot change it. A body that then fails, times out, or is dropped
ends early, and its audit event is `failed` with classification `nx-transfer-failed`, never a hit or a completed read. A
client must treat a short body as a failed read; the gateway stages and buffers nothing to hide the cut.

**Read-only is an authority guarantee, not a client-cost claim.** Stock Nx 23.2.1's `native/cache/http_remote_cache.rs`
has no independent read-only/skip-upload option. Its `store` first builds an in-memory gzip tar archive, then attempts
`PUT`; 403 returns false without failing the task. `skipRemoteCache` disables reads too. Do not patch Nx or hide the
attempts: measure archive construction, denied request bytes/time and backend publication separately (13_telemetry.md).
A read-only WIP run may attempt uploads but publishes zero artifacts.

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
  count. Queue overflow returns 429 without reading a body, except an Nx cache request's (§Busy gateway above). Each
  direction gets a 1 MiB bounded streaming buffer; producers pause when it fills, and cancellation closes both legs.
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

Core's project runtime owns workspace gateway preparation: after the decoded route's `AdmissionSource::admit` and live
workspace-incarnation validation, and before exec, session opening or checked-land work, it reconciles only the selected
workspace. Both `worker.exec` and trusted `coordinator.exec` use this boundary; connection role never substitutes for
route admission. The scoped inventory revalidates the held incarnation after reading gateway status. A missing or
recreated workspace refuses before gateway installation or removal; it never retargets the request. Jobs read no sibling
session or grants record, evict no foreign session, and never allocate a replacement port block. Job observation, stdin,
logs and status do not prepare gateway sessions. Project-wide reconciliation and port repair belong to lifecycle and
maintenance verbs.

The runtime owns one control client constructed from its validated host storage through the existing `GatewayControl`
trait. The CLI supplies the transport at construction; core does not link the daemon or select an ambient control
socket, and no default or no-op client can silently bypass preparation. Workspace operations borrow that client.

Concurrent reconciles read gateway status before their scoped inventory snapshot: every installed session came from an
older snapshot, so a removal never revokes a newer decision, which would leave a tombstone refusing the workspace's own
revision until its next attach. A write refused with `revision-fence` or `not-installed` lost a race to a reconcile that
read a newer snapshot; that decision stands, and the write counts as superseded rather than failing the command that ran
the reconcile.

Every request is one line. A one-shot request is followed by the client's EOF and answered with one line; a
`disk-lease`, `cpu-tokens`, `jobserver` or `resident-pool` request is held for as long as its client keeps the
connection open.

## Disk-lifecycle lease

Disk-changing tools and descriptor-native unmounts cowshed runs on the host — the CLI's, a supervisor's, the gateway's
own startup pass, and every test's — run under a host-wide lease the gateway schedules. One client path takes it:
`SystemCommandRunner` admits disk children by command class and native `funmount` by namespace class through the same
lease client. Setup uses that client too. Mount and unmount retain a registry-proven raw device pin across their lease
wait and effect. Native unmount resolves and proves its current held root after admission, including when running
unleased. Unpinned detach commands recheck positive exact-image/device registry ownership after the lease wait,
immediately before spawning. An attach spends most of its time in StorageKit's `syncAllDisks`, after its device already
exists, and that sync does not finish while the mount table keeps changing (01a_apfs.md, "How the APFS host degrades"),
so attaches stall beside mount churn and stay fast once the two are kept apart. Attaches among themselves only queue on
`storagekitd`; so the lease is two classes that exclude each other rather than N interchangeable tokens.

- **Storage**: `diskutil`, `hdiutil`, and `newfs_apfs`. **Namespace**: `mount_apfs`, `umount`, and owned-root native
  `funmount`. Cowshed's image unmount is the native namespace effect, not a DA command. An operator's `diskutil` facade
  remains storage. Read-only operations (`fsck_apfs`, IORegistry and kernel mount reads) take no lease.
- **A label does not change the disk lifecycle.** No disk tool renames a volume: the runner refuses `diskutil rename`
  and `renameVolume` outright, and a volume is named through its held mount root, a kernel attribute write (01a_apfs.md,
  "Ownership, identity, and the volume label"). A name keeps a mounted volume's mount-table position and st_dev, so
  naming takes neither phase and makes no attach wait behind it.
- Members of one class share the running phase, up to 8 at once. A request of the running class with room in the phase
  and nobody waiting enters at once; an idle gateway starts a phase for it. A request asked `alone` takes all 8 places:
  it enters only a phase that has no other member and runs with none beside it.
- Once a member of the other class waits, the running phase admits nobody new, so a steady stream of one class cannot
  starve the other. When the phase drains, the next goes to the other class, whose oldest waiters enter it in arrival
  order up to the cap, none overtaking an older one the phase has no room for; with both classes waiting, phases
  alternate.
- **A stuck holder is bounded.** A holder that keeps its phase 10 s after its grant while the other class waits is
  evicted from the phase: it no longer counts, the phase hands over once its other holders leave, and the gateway's
  stderr names it — `disk-lease evicted a <class> holder after <held> while <class> waited`, with its pid and the
  command line it asked for. Its command may still be running beside the next phase; a hung `umount` costs that one
  overlap, not every attach on the host. A SAFE command (every disk child except a protected disk-image command) owns
  one monotonic caller execution budget for admission and child execution; the gateway adds no command deadline.
  Control-socket establishment uses one nonblocking connect and any pending-connect poll is clipped to that same
  remaining budget; refusal (including a full-backlog EAGAIN) is an immediate unleased fallback, not a timeout or
  authority to retry. Descriptor creation and close-on-exec/nonblocking flag installation alone run under the fork lock,
  never connect or poll. Every failed connection closes its owned descriptor; no connector thread remains alive. Each
  queued acknowledgement, grant and after-half-close wait is clipped to its remaining budget, including old-gateway and
  alone-to-shared retries. The runner checks remaining time before spawn and inside its existing pre-exec closure, after
  any fork-lock wait: exhausted admission never executes the native tool. A failed pre-exec fork stub is reaped by the
  spawn operation; it performs no disk-tool effect. An overrun signals the owned process group and waits to reap the
  child, retaining its actual exit or signal status. A kill failure retains the kill error, exact child pid and a
  nonblocking observation of exited, still running or reap failure; it neither waits without bound nor joins live output
  pipes. A reap failure is distinct and never claims the child was reaped. A protected disk-image command
  (`diskutil image …`, `diskutil eject`, every `hdiutil` verb) instead warns at the caller's configured cadence and is
  never killed, because interruption can leak its device until reboot (01a_apfs.md, "How the APFS host degrades" §2).
  Its detached session, file-backed output and caller-bounded stop-wait policy are unchanged.
- **Wire.** The client writes `{"op":"disk-lease","class":"storage"|"namespace","command":"<argv>"}`, with
  `"share":"alone"` when it asks for the whole phase, and does not half-close. The gateway answers
  `{"ok":true,"lease":"queued"}` at once and `{"ok":true,"lease":"granted"}` when the command may run; the lease lasts
  until the client closes the connection, so a client that dies holding one releases it with its socket, and one that
  closes while queued leaves the queue.
- **The lease spaces commands out; it never decides whether one runs.** The command runs unleased when the control
  socket does not answer (said once per process: the gateway is down or being set up), when the gateway refuses, when
  the caller's admission budget expires before a grant, and when the gateway predates leases. A queue's age alone is not
  an execution timeout, and the gateway adds no fixed grant-wait deadline. An older gateway says nothing until it reads
  EOF, so a client that hears no `queued` within 2 s half-closes, reads that gateway's `invalid-request` refusal, and
  stops asking it for a minute. A gateway that predates `alone` refuses the field as `invalid-request` at once; the
  client asks it shared instead, from that command on, for a minute. These are lease fallbacks, not authority to restart
  a SAFE command's total budget: every fallback and retry keeps the original deadline, and an exhausted budget prevents
  child spawn even when no lease is available.
- **Spans.** The client's lifecycle spans are `disk-lease <class> wait`, from the request to the grant (`status=err`,
  with an `unleased=<reason>` line, when the command runs unleased), and `disk-lease <class> phase`, around the command
  the grant admitted. With `COWSHED_TIMING` set, the gateway prints each phase as it drains: its class, how long it ran,
  its members, and how many it evicted.
- **A test's waits are recorded.** When `COWSHED_LEASE_WAIT_RECORD` names a file, every client also appends each wait's
  start before it asks and its end once the wait is over, so the real-APFS lane can bound a test's code time without the
  host's queue (08_testing.md, "Integration tests (real substrate)"). Nothing on the wire changes.
- **A run's commands are recorded.** When `COWSHED_DISK_COMMAND_RECORD` names a file, cowshed's `leased` also appends
  every disk command as `<class> <argv>` before it runs, leased or not, so a test run counts the images it attached and
  the volumes it mounted (08_testing.md, "Integration tests (real substrate)").

The regression is `cowshed-gateway`'s `contention_attaches_stay_fast_beside_mount_churn_only_under_the_lease`, run by
name with `--ignored`: eight real attach/detach loops beside two real mount loops for 40 s, first through a real
gateway's lease and then unleased: leased attaches stay fast, unleased ones stall behind the mount churn.

## Host CPU budget

Every gate on the host sizes its runners to the whole machine. Nx runs one task per core, and each nextest run under it
runs one test thread per core again; several concurrent gates oversubscribe the host many times over, and tests that do
no disk work time out waiting for a CPU. Running anything serially would trade those timeouts for idle cores. The
gateway instead holds one budget of CPU tokens for the host: a runner takes tokens before it starts and sizes its own
parallelism to its grant, every gate keeps running its tasks in parallel, and the total runnable work stays near the
core count.

- **The budget.** N tokens, one per core the host reports (`std::thread::available_parallelism`); one token is one
  runnable thread or process. The ledger keeps `free + Σ held = N`; it never infers a holder from a process scan.
- **Who asks.** The `@smoothbricks/nx-plugin` `bounded-exec` executor, before it starts any command — the inferred
  nextest runners, `bun test` targets, cargo commands it runs. Its requested parallelism is the command's runner's own:
  a `nextest run` names `--test-threads` or defaults to one per core; `bun test --parallel=N` names N workers, a bare
  `--parallel` defaults to one per core; a cargo build (`cargo build`/`test`/`clippy`/…, `nextest archive`,
  `napi build`) defaults to one job per core; anything else, one `bun test` process included, defaults to one. A
  target's `parallelism` option selects that request, but positive inherited `NEXTEST_TEST_THREADS` and
  `CARGO_BUILD_JOBS` are ceilings on their respective runners. A configured environment count may lower an inherited
  ceiling, never raise it. `want` is the lesser of the request and its ceiling. `test.concurrent` inside one process is
  not another CPU.
- **Sized to the grant.** A grant is between 1 and `want`; runner concurrency is at most `min(want, granted tokens)`,
  never raised above an inherited ceiling. The executor sets `NEXTEST_TEST_THREADS` and rewrites nextest's named
  `--test-threads=` count to that limit, rewrites Bun's `--parallel=` count, and sets `CARGO_BUILD_JOBS` for cargo.
  Cargo's `RUST_TEST_THREADS` also retains its independent inherited/configured ceiling. Every command receives the
  actual held grant as `BOUNDED_EXEC_CPU_TOKENS`; the sub-ledger and telemetry report held tokens, not a fabricated
  runner count.
- **Nested runners use a parent-held sub-ledger.** A granted executor serves its T tokens on a private socket and
  publishes that endpoint as `BOUNDED_EXEC_CPU_POOL`, beside its `BOUNDED_EXEC_CPU_TOKENS` count. A default nested
  executor connects to its immediate parent's live pool, requests its own demand (the count is not a bound on the ask:
  the pool grants at most what it can), and holds one complete local grant on that connection. A background pool
  accounts for every holding sibling, so their sum never exceeds T. It serves its own FIFO queue, granting the head
  `min(requested, free)` once at least one token is free; that count is final, not a partial holding awaiting more
  tokens. Queued siblings wait only for that pool's own holders. Host checkout fairness remains the upstream ledger's
  concern. A gate job's pool is the exception: it lends nothing of its own and relays each child's ask to the host
  ledger as a gate-class request for the gate's checkout ("Gate admission" below). An admitted child serves its granted
  count as its own pool, and grandchildren draw only from that pool: no holder waits for another grant from an ancestor
  or the host. The parent remains the owner of its upstream grant. A child returns only its local allocation by closing
  its connection; it never releases the parent's connection. Upstream loss revokes the downstream pool and its
  connections, so a dead parent cannot grant tokens to a descendant. Revocation ends accounting, never work: a command
  whose borrowed or host grant closes while it runs — the gateway restarting, which every install does, or the lending
  parent exiting — runs on over budget to its own exit, says so on stderr
  (`cowshed: cpu-tokens grant ended: … it runs on over budget until it ends`), and is judged by that exit alone, as
  "Crash safety by connection" below says of every runner across a gateway restart. A connection is a ledger entry, not
  the work's owner; only a bound, a signal or the process that started the command stops it. A grant loss is never a
  failure and starts no teardown. A bare or invalid count proves no allocation; an absent endpoint follows normal host
  admission. Once connected to the parent, refusal, silence or protocol failure is an ordinary admission failure, never
  a second host take while that parent may still hold its grant. A stale descriptor whose endpoint no longer connects is
  treated as absent. Only the executor publishes its actual live pool; `options.env` cannot inject either marker into a
  command. The span and `cpu.inherited: true` record identify a parent-pool grant and its actual local wait. Pool setup
  or cleanup failure is an ordinary failure, never a successful unaccounted run. Real-process default-runner proofs live
  in `executor-host.test.ts` under `test-host`, because the default macOS runner acquires a real RAM volume. Node peers
  import the built executor artifact named by `executors.json`; the host target's existing build dependency produces it,
  rather than compiling TypeScript inside a test's guard. Pure pool and executor unit cases never attach images. **Known
  admission ambiguity:** a connect error is currently classified as an absent endpoint. A full listen backlog or local
  file-descriptor exhaustion can also prevent connection to a live pool, so this classification does not yet prove that
  its parent has released the upstream grant. Distinguishing those cases before any host fallback remains an admission
  follow-up; connected refusals and protocol errors already fail without a second take.
- **Queued priority** (2026-10-11). The coordinator supplies each job's priority, an unsigned 32-bit integer, higher
  first, default 0. It orders queued bulk work and queued lands within their own queues; no value outranks the active
  gate or manufactures gate authority. A priority change re-keys pending work, never interrupts, resizes or restarts an
  already-running grant. Priority and the OS band are independent.
- **Fair between checkouts within one priority.** The checkout is the Nx workspace root the request names. A checkout's
  share is the background limit `⌊N/2⌋` ("Room for a gate" below) divided among the checkouts holding or waiting (at
  least 1). The next grant goes to the highest-priority eligible waiting request; within that priority, to the waiting
  checkout that holds the fewest tokens (the older head request on a tie), to its oldest request. While another checkout
  waits, that grant is `min(want, max(1, share − held))`; with nobody else waiting it is `min(want, free)`, and a
  background grant draws only on its class's room ("Room for a gate" below). A head request whose least grant is not
  free yet waits for it, and so does everyone behind it within the same priority. A large request is never starved by a
  stream of smaller requests at that priority; a gate with sixty runners queued cannot crowd out one with three at the
  same priority.
- **All at once.** A request is granted once and never holds part of a grant while waiting for more, so no two requests
  can deadlock on each other's tokens. A compile-only Cargo build may grow after it starts ("A short build grows"
  below), but it runs from its first grant on and never waits for a growth.

- **Gate admission: oldest gate job first, ahead of background, never starving it** (2026-10-17, Danny tg#6693: "The
  gate class is served FIFO at highest priority over all other execs … When gate runs it needs more tickets than
  background but not fully starve background"; tg#7019, of two concurrent lands: "first should be full priority and
  second almost background until first completes"). A jobserver root request carries a typed
  `class: "background" | "gate"`; omission is background, and any other class is `invalid-request` (a resident asks with
  `resident-pool`, below). Gate-class work is ranked by the gate job it serves, by that job's own admission: a gate root
  request is its own job, and every task its pool relays ranks by the root's admission, not by when the task asked.
  While any gate-class work wants tokens, the oldest gate job's next want is served once a token is free, wherever its
  requests sit in their checkouts' queues: a waiting task takes `min(want, free)`, in the order that job's tasks asked,
  before a running task of that job grows ("A short build grows" below). All of it comes ahead of every queued
  background request (both jobserver and `cpu-tokens`). The work behind the gate job being served — background and every
  younger gate job — is not starved meanwhile: a background checkout, or a younger gate job, that waits holding nothing
  is granted one token first, oldest request first, as long as that work as a whole then still holds less than half of
  N, so the job being served and the older ones hold the larger part. A younger land is thereby "almost background"
  (tg#7019): its orchestrator starts on its floor token, and its tasks queue behind the older land's until the older
  land stops wanting. Every grant is whole, so two younger gate jobs on their floor tokens wait on nothing the other
  holds. Measured without that floor: a younger land asking for its orchestrator's one token while the older land held
  13 of 18 tokens waited out the older land and failed. A running gate grant blocks nobody: with no gate-class work
  wanting tokens, ordinary checkout fairness serves background from the free tokens within background's limit ("Room for
  a gate" below). An in-flight background grant stays exactly as it is until its child exits or its connection closes.
  This is priority on genuinely returned tokens and on the half of the host background never holds, not a budget
  increase or stolen allocation. Conservation remains `free + Σ held = N`.
- **A land is a gate job, and its tasks are gate-class requests.** The selecting authority is the land queue holder:
  `~/Dev/.land-lock.sh` runs main's `tooling/land-host.sh`, the only path that gates a tree before `cowshed land`
  fast-forwards to it, and it runs that gate as `cowshed exec <workspace> --class gate --host -- …`. The trusted route
  alone grants the class (class-not-permitted for a worker). The job's own grant is the one token its orchestrator uses;
  its pool relays every task's ask, from the host or from inside the sandbox, as a gate-class request for the
  workspace's checkout, ranked by the job's own admission. A land therefore takes returned tokens at every task, in the
  order its tasks ask, and is never frozen at the width of its first grant. Same-repo lands are already serialized by
  the per-repository land lock. Lands of two repositories are served oldest gate job first: the younger land's
  orchestrator starts on its floor token, the older land's tasks take every returned token they want, the younger land's
  tasks take what the older one does not ask for, and the younger takes the older one's place when it completes. Neither
  is preempted: a running task of either keeps its grant. The jobs the land's own cowshed steps start are gate-class
  work too, since the land lock waits on each of them: the rebase carry's task-hash computation, which the lander asks
  for with `cowshed rebase --gate`, and `cowshed land`'s Nx cache initialization (16_build_volumes.md, "Carry" and
  "Rebase carry"). Measured 2026-10-19 with both at background class: a land's task-hash job waited 21 s in the
  background queue with no other gate running, and in one repository the same computation took 48 s on one land and 297
  s on the next, while that repository's whole land queue waited behind it.
- **A job lives with its requester** (2026-10-18, Danny tg#7386: "cowshed gateway is supposed to background every
  landing queued and not cause load that slows down the head of the queue"). A foreground job's only consumer is the
  process that asked for it, so when that process is gone the job stops and its tokens return. A gate job's verdict
  moves main only through the `cowshed land` its lander runs next. A gate whose lander is gone — killed, or stopped
  under the one-lander rule — authorizes nothing, yet it keeps its admission, so it stays the oldest gate job and takes
  every returned token ahead of the land that holds a land lock. Measured 2026-10-18: a land of another repository
  stopped while gating left `nx run-many --parallel=18` running at gate class with no lander and that repository's land
  lock free; it headed `gateQueue` and held 13 of 18 tokens against the locked smoothbricks land. The same holds for
  background work. Measured 2026-10-19: an agent's tool timeout killed three `cowshed exec` clients, and their sandboxed
  jobs ran on under the supervisor for 1h04, 61 and 33 minutes, holding a quarter of the host and reading as work. So
  every foreground `cowshed exec`, whatever its class, holds its job for as long as the exec process lives (11_shell.md,
  `hold`): when that process ends before its job — `SIGKILL` included, since the kernel closes its connection — the
  supervisor kills the job, and its tokens (and a gate's place in the gate order) pass on at once. `--timeout` releases
  the hold before it leaves the job running, and `--background` takes none: a caller who asks for an unattended job
  keeps it. A job that cannot be held is killed and its exec fails, rather than run for nobody.
- **A short build grows** (2026-10-17, Danny chose option C of tg#6761). A grant taken at the first returned token can
  be far below its `want`: measured, a land task's nextest asked for 18 tokens on a host another shed held whole, waited
  32 s and ran its whole suite with 1 (`tokens=1/18`). A runner whose parallelism is a make jobserver can use tokens
  handed to it later; one whose test threads were sized at its start cannot. So only a Cargo invocation that compiles
  and runs nothing it built — `build`, `check`, `clippy`, `doc`, `rustc`, `rustdoc`, `fix`, `test`/`bench` with
  `--no-run`, `nextest archive` — asks with `"grow": true`; test runners keep their first grant, so no token sits idle
  in a fifo nobody reads. A growing holder that holds less than its `want` takes `min(want − held, free)` more from
  returned tokens that no waiter ahead of it takes: a gate-class build grows once no task of its own gate job or of an
  older one waits, ahead of a younger gate job's tasks and of background, under the same floor; a background build grows
  only while no background request waits, and only within background's limit ("Room for a gate" below). It never passes
  its own `want`, so one task cannot starve the rest of its own gate. Conservation is unchanged: growth moves tokens
  from `free` to that holder. The holder hears each new total as `{"ok":true,"lease":"grown","tokens":<total>}` on its
  connection, and the launcher hands the difference to Cargo's jobserver fifo and says
  `cowshed: cargo jobserver grew tokens=<from>/<want> -> <to>/<want>` on stderr, beside its
  `cowshed: cargo jobserver granted tokens=<g>/<want>`. Closing the connection still returns everything held.
- **Room for a gate, since no grant shrinks** (2026-10-19, Danny: "avoid dumb waiting"). A running grant cannot be
  shrunk: Cargo's implicit make token and libtest/nextest threads sized from the original grant do not return FIFO
  bytes. The gateway does not resize a running FIFO, kill peer work, or use `SIGSTOP`/`SIGCONT`: wall and idle bounds
  would keep advancing during a freeze, and a killed nextest run cannot resume, so its owner would rerun it whole and
  read a failure it did not cause. Fairness that acts only when a grant is made cannot reach a grant made while nobody
  waited, so the grant itself leaves the room: **background-class work as a whole never holds more than `⌊N/2⌋`
  tokens**, and at least one (`background_limit`, 9 of 18). A background request takes only its room, the free tokens
  within that limit; a short background build grows only within it; and the floor token "Gate admission" grants
  background already keeps it under half. This is the host ledger's rule: a parent's lent pool ("Nested runners") serves
  a grant the host already made, no gate asks it, and its children may hold all of it. The other `⌈N/2⌉` host tokens are
  therefore free whenever no gate-class work holds them, so a gate's first request is granted at least
  `min(want, ⌈N/2⌉)` in the ledger transition of its own ask: it waits for no departure, whatever background holds and
  for however long. Its later wants take returned tokens ahead of background as above. Measured 2026-10-19: a background
  `cargo nextest run` granted 17 of 18 tokens while it was the only waiter sat at 0% CPU for 50 minutes, and the land
  gate that arrived after it ran on its one token for over 35 minutes against a usual 10 to 20. A cap on a lone grant
  alone would not close this, since two background grants made one after another fill the host again; a floor smaller
  than half would leave the gate running on that floor for the whole of the longest background run. Once gate-class work
  — the gate's own tasks or an older land's — holds the gate half, a gate request waits for a returned token like any
  other; gate priority then promises admission order, not a fixed test duration. The supervisor's separate nice
  placement below ("Bands") orders the classes by lowering nice, never by raising one above default.
- **No admission deadline, for any class** (Danny tg#6679, tg#6693). A gate is the highest-priority work there is, so it
  is never failed for waiting in the order that serves it: a gate jobserver request, like a background one, waits until
  it is granted or its client closes the connection, which leaves the queue. The operator request is
  `cowshed exec <workspace> --class gate --host --want N -- COMMAND [ARGS...]`, and it sends
  `{"op":"jobserver","class":"gate","want":<n>,"checkout":"<root>","command":"…"}`. A waiting request holds nothing, so
  its wait costs nobody; who holds the host while it waits is the live `cpu-budget` ledger below, not a refusal.
  Measured with the 120000 ms bound this replaced: a younger land's gate was refused after 120003 ms while an older land
  held 13 of 18 tokens, killing the land for waiting in the very order the ledger serves. An unknown class is an invalid
  request, and so is a `waitMs` field: it is no longer part of the request. Running task wall/idle bounds still start at
  their grant.
- **The task server owns the live grant and nice.** Work enters the existing `cowshed exec` / workspace supervisor, not
  a gate-host or taskpolicy wrapper. Its typed request carries class, want and scope. Only a trusted
  coordinator/operator may request Gate or HostGate; a worker request is refused with typed class-not-permitted before
  job or spawn effects. Ordinary commands default Background, want1 and Workspace scope; producers that need parallel
  work supply their actual demand; a requester that awaits the result asks Interactive ("Bands"), which a worker may ask
  for, since it buys a band and no token; a resident (below) is Resident, which a worker may ask for, and names no want.
  The supervisor records Queued, asks the gateway off its actor path and spawns only after a real grant. CPU admission
  uses the control socket of the controller's validated host storage, the same store/gateway.sock authority its gateway
  reconciliation uses, never an unrelated ambient store. A queued kill withdraws the request without spawning.
  Missing/refused admission never causes an unbudgeted server spawn. The supervisor retains the allocation until the
  owned job concludes. The gateway's existing CpuBudget ledger serves that exact allocation on a private
  workspace-scoped CPU-pool endpoint; Nx bounded-exec and Cargo descendants borrow only from it, with sibling sums
  bounded by its actual count, or, for a gate job, through it as gate-class host requests. The root FIFO is not offered
  alongside the pool: that would spend the same tokens twice. Its canonical endpoint is
  `<store>/run/cpu/<base64url-incarnation>/<opaque-endpoint>`: the unpadded 22-byte incarnation component losslessly
  encodes all 128 bits of the validated 32-lowercase-hex workspace incarnation, never a digest or truncated prefix. The
  opaque endpoint is an independent 22-byte unpadded base64url UUID. The gateway, sandbox builder and grant validator
  derive this one layout through the same helper; there is no runtime alias or fallback. This adds 54 bytes to the store
  prefix, giving a 98-byte socket path for the existing 44-byte fixture store, within Darwin's 103-byte usable limit.
  Arbitrarily long store paths do not acquire a new short-link lifecycle. A bare count/class is no gate role. The owned
  command is placed at its class's nice ("Bands"), never the shared warm host or supervisor. No class is raised above
  default, which requires root; the order lowers the other classes' nice instead. Nice is applied and read back before
  command execution, using the held-child fence on macOS and a private report pipe on Linux. Refusal is typed
  PriorityFailure, not an unobserved run. Class, scope, actual tokens and observed nice are recorded; nice remains
  absent when never observed. A HostGate is a coordinator-only argv one-shot on the host, outside ExecutedChild,
  preserving the real-APFS gate's host authority; no ordinary workspace sandbox is loosened. Controller discovery uses
  only `cargo locate-project` and `cargo metadata --no-deps --offline`, which retain their noncompiling exemption: no
  grant, class nice or job record is claimed for that discovery.
- **A resident holds no token** (2026-10-19). A token is a runnable thread the holder is using now, so a process that
  stays up to serve requests and is idle between them — the Nx daemon a shed's supervisor keeps (04_sandbox.md), a
  worker that runs each request it is sent — must not hold one for its lifetime: starting a resident must not reduce its
  checkout's usable concurrency. Measured 2026-10-19: every shed's Nx daemon held its shed's one background token for
  its whole life (46 minutes in `batch-7`, 3 jobs waiting behind it), and a test's scratch Nx worker held its
  workspace's token for 28 minutes while that test's worktree snapshot waited for it; and because the next grant goes to
  the waiting checkout that holds the fewest tokens, a checkout whose daemon held one token was served after every
  checkout that held none. A resident is admitted as class Resident: its supervisor writes
  `{"op":"resident-pool","checkout":"<workspace root>","command":"<command>","job":{…}}`, which names no `want`; the
  gateway takes no ledger place for it and answers `queued` and then at once `granted` with `cpuPool` and no `tokens` or
  `fifo`. That pool lends nothing of its own: each child that asks it asks the host ledger itself, at background class
  for the resident's checkout, exactly as a root background request does, so a resident's request takes its tokens when
  it starts and returns them when it ends. Its command receives that pool and `BOUNDED_EXEC_CPU_TOKENS=0`; a pool with a
  count of 0 is a valid descriptor (a bare 0 without a pool still proves nothing), and Nx bounded-exec and the Cargo
  launcher ask it as any parent pool. The resident runs at background nice. The connection is the pool's life: the
  resident's exit closes it. A different token class would leave the resident in the ledger holding something; exempting
  it from the budget would leave the requests it runs unbudgeted; acquiring per request through a relay pool is the gate
  job's existing mechanism and budgets exactly the work.
- **Work-conserving but for the gate's room.** No grant is capped below `want` for fairness' sake: with nobody else
  waiting, a gate-class grant is `min(want, free)`, so a lone gate runner may hold all N tokens, because a gate's wall
  time is what the budget buys. A background grant is `min(want, room)`, the room being the free tokens within the
  background limit: that limit is the one cap, and it is the trade. Up to `⌈N/2⌉` tokens (9 of 18) sit idle exactly
  while no gate-class work holds or wants them and background work wants more than `⌊N/2⌋`; a gate that starts uses them
  at once, and with no more than `⌊N/2⌋` wanted by background nothing idles. The idle half buys a gate that starts wide
  at once instead of waiting out the longest background run, which nothing can shorten once granted. A lone waiter also
  takes what its room holds when that is below its fair share: on an 18-token host, a background runner holding 6 does
  not make the sole background newcomer wait instead of taking the 3 left of the limit. Ordinary checkout fairness (the
  share rule above) still bounds a grant while another ordinary checkout waits. Gate admission orders who is served
  first; it never leaves a token idle.
- **Bands** (2026-10-17, Danny tg#6693: "The gate class is served FIFO at highest priority over all other execs. The
  interactive class has priority over gate and regular exec. Priority is handled by OS niceness. When gate runs it needs
  more tickets than background but not fully starve background"). A job's class fixes, at the job's birth, both its
  place in the token order and the band the host schedules it in. This table is the one statement of both; every other
  section names it.

  | Class         | What runs in it                                                                                  | CPU tokens                                                                                             | Band           | Nice |
  | ------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ | -------------- | ---- |
  | `interactive` | work a person or model is waiting on, declared by the requester that awaits its result           | behind gate, as background: checkout fairness and the floor token                                      | **foreground** | 0    |
  | `gate`        | a land's gate job, every task its pool relays and the land turn's holder; the trusted route only | first: the oldest gate job FIFO, ahead of every other class ("Gate admission")                         | **normal**     | +5   |
  | `background`  | every other exec, the default, and each request a resident's pool relays                         | checkout fairness behind gate within half the host ("Room for a gate"), never starved: the floor token | **background** | +10  |

  Tokens decide which work runs at all; nice decides whose runnable threads win a core when more are runnable than there
  are cores. Those are the ruling's two priorities: gate work takes returned tokens first and, while it runs, more of
  them than the rest without starving it, and on a contended core an interactive thread runs before a gate thread and a
  gate thread before a background one. Nice is never negative, because raising a priority above the supervisor's own
  takes a privilege no supervisor holds, so the order is made by lowering it: interactive at the supervisor's own nice,
  gate five below it, background ten below it. A band never admits, queues or sizes a job — the token ledger alone does
  that — and an idle host leaves every band's work at full speed. A class is fixed at the job's birth and its band with
  it: no live transition moves a running job between bands. An interactive job asks the gateway's ledger as background,
  whose tokens it shares; `interactive` is the supervisor's placement and the task server's class, never a third ledger
  class.

- **Crash safety by connection.** The grant is tied to the connection: on exit, `SIGKILL` included, the kernel closes
  the socket and the gateway returns the tokens at once. A request that closes while queued leaves the queue. A gateway
  restart forgets every grant, so runners started under the old one overshoot until they finish. The gateway does not
  drain held grants before it exits: its drain (`Gateway::drain`) stops the control socket first, aborting every held
  lease connection, and a crash closes them the same way. Holders therefore read a closed grant as the end of its
  accounting and keep running; no holder stops work because its connection closed. A request still queued when the
  gateway goes is refused, as any premature close is, and its runner never starts.
- **The bounds start at the grant.** The executor's `timeoutMs` and `idleTimeoutMs` start when the command starts, after
  the grant: a wait for the host is not the command's time.
- **A queued descendant is not idle** (2026-10-18, Danny: a job waiting for a CPU token is queued, not idle; "idle"
  means nothing is happening while nothing is expected to happen). Work a command starts can queue for host tokens of
  its own — a test's scratch project, whose cowshed jobs ask the host under that project's checkouts, holds no token of
  the command's pool. A descendant that knows its work waits tells the run on the pool it inherited: it writes
  `{"op":"host-wait"}` on `BOUNDED_EXEC_CPU_POOL`, is answered `{"ok":true,"wait":"held"}` at once, and holds the
  connection for as long as the work waits. Every pool of the run, a sub-pool it served Cargo's launcher included,
  counts to the run. While at least one is held the run's idle clock is stopped: the time left of its no-progress
  interval is kept, nothing is spent, and stderr says `bounded-exec: idle clock stopped`. When the last closes — the
  token arrived, the work was withdrawn, or the descendant died, which closes its socket — the clock runs on from what
  was left and stderr says `bounded-exec: idle clock restarted` with the queued time. A finished test still sets the
  interval back to `idleTimeoutMs`. An expiry names the queued time it did not count. Only a held wait stops the clock:
  a run whose descendants say nothing keeps the bound exactly as before. `timeoutMs`, a ceiling, is never stopped. A
  wait holds no token, so it changes neither the pool's count nor any grant.
- **Wire.** The client writes
  `{"op":"cpu-tokens","want":<n>,"priority":<u32>,"checkout":"<workspace root>","command":"<command>"}` and does not
  half-close. An omitted priority is 0. A named job also carries its existing
  `job: { repoId, workspaceIncarnation, jobId }` identity, so its queued admission can be re-keyed without selecting a
  different job. The gateway answers `{"ok":true,"lease":"queued","id":"<lease id>"}` at once and
  `{"ok":true,"lease":"granted","id":"<lease id>","tokens":<g>}` when the runner may start. The same lease id identifies
  that admission until its connection closes. `want` 0, a missing or empty checkout, or one over 4096 bytes is refused
  as `invalid-request`. `{"op":"cpu-budget"}` is one-shot and answers the ledger as `cpuBudget`: `total`, `held`, per
  checkout `held`, `running` and `waiting` (every class together); `gateJobs`: the gate jobs whose runners hold tokens,
  oldest job first — the first is the head of the gate queue — each with its `checkout`, its gate `job` (the job's
  admission number; the smaller is the older job), the tokens it `held` and its `holders`; `gateQueue`: the waiting
  gate-class requests in the order returned tokens go to them, each with its `checkout`, gate `job`, the client's `pid`,
  the `command` it named, its `want` and `waitedMs`, the milliseconds since it asked; and `background`: each checkout's
  background-class stake alone (`held`, `holders`, and its waiting requests as `waiters`, each with `pid`, `command`,
  `want` and `waitedMs`), for the checkouts that have one. Every waiting entry is one request: a gate job whose tasks
  wait is one entry per task — the job identity is the gate `job` number and each entry is a distinct connection — so
  the queue is never collapsed per job; its pid, command and age tell the entries apart (measured 2026-10-19: fifteen
  `batch-7 gate job 603 want 1` entries were fifteen of that gate's tasks, and read as duplicates because they named
  nothing else). `holders` are the granted leases, one per runner holding tokens, oldest grant first, each with the
  client's `pid` (the socket's peer; absent when the socket names none — a pool-relayed task is named by its own
  connection to the pool), the `command` it named, the `tokens` it holds now, its `want`, and `heldMs`, the milliseconds
  since its grant. The age is the lease's, never the process's: a lease is one connection granted once, a growth does
  not renew it, and a process that asks again holds a new lease with its own age; the ledger never scans processes, and
  `ps -p <pid>` gives the process's age. Measured 2026-10-18: a ledger that named only
  `land-test-hang: held 15, running 1` read as an orphan, and a live 34-minute `cargo test …` another unit was waiting
  on was killed by hand; its holder line would have said what it was and that it was live. `cowshed gateway status`
  shows this ledger to the operator in the order it serves — its count with the background limit
  (`gateway cpu budget: held <h>/<N> tokens, background at most <⌊N/2⌋>`), the head gate job, the gate jobs behind it,
  the gate queue, then background, each running gate job and background stake followed by its leases — in its human
  lines, and verbatim as `cpuBudget` in `--json` (06_cli.md; 07_api.md `GatewayStatus`), whose limit is `⌊total/2⌋`, at
  least one, so whether queued lands wait behind the head of the queue without loading it, why background waits beside
  free tokens, and who holds every token since when, is visible rather than inferred; a daemon that does not answer
  `cpu-budget` is reported as such, never as an empty ledger.
- **The hold limit** (2026-10-19). A lease held past 30 minutes (`HOLD_LIMIT`) is named on the lines right after the
  ledger's count, oldest first, by checkout, pid, tokens, want, age and command
  (`gateway cpu hold past 30m: <checkout>: pid <pid>, <g>/<want> tokens, granted <age> ago: <command>`), so the first
  lines of `cowshed gateway status` answer who holds the host and since when. Measured 2026-10-19: a deployed-proof
  `cargo test … --ignored` integration test held 12 of 18 tokens for 48 minutes while a land's gate waited with 2, and
  its holder was found with `ps`. A background lease now holds at most `⌊N/2⌋` ("Room for a gate"), so a long hold costs
  background its half, never a gate its start; what the line names is a run that may hold tokens without working, as the
  50-minute nextest at 0% CPU of the same night did. The limit is reported, never enforced: a lease is a ledger entry,
  not its work's owner ("Crash safety by connection"), so revoking it would return tokens while the work runs on over
  budget. What ends a runaway is its own bound — bounded-exec's `timeoutMs`, a test harness's ceiling — and a wait for
  tokens is bounded the same way: a queued run is not idle, but a ceiling no wait stops still ends it as starved.
- **Priority updates.** `{"op":"job-qos","job":{…}|"lease":"<lease id>","priority":<u32>}` is one-shot and answers
  `{"ok":true}`, or `not-found` for an admission that is not live. It re-keys the selected admission only while queued;
  a granted job keeps its grant and band. The caller supplies the same coordinator priority on later admissions. This
  control changes neither the token budget nor the band rules ("Bands") and creates no separate resource ledger.
- **Managed Cargo admission is fail-closed.** For a root request, the CLI launcher scans cwd ancestors with lstat for
  the in-image `.cowshed/workspace.json` marker. Its presence only tightens admission and grants no authority; an
  inspection error refuses. A managed checkout starts no payload without a live grant: absent gateway, refusal, unknown
  operation, malformed protocol and premature close all fail with their actual reason. Only a positively nonmanaged
  host/CI checkout may run unbudgeted only when NotFound proves gateway absence. ConnectionRefused can come from a live
  saturated listener, so it and every other connect error refuse. A connected refusal or protocol error is never
  absence, in any checkout. Noncompiling discovery and inherited grants keep their existing precedence. No grant, gate
  or background, has an admission deadline.
- **Spans.** The executor prints
  `cowshed: cpu-tokens wait done elapsed=<ms>ms status=ok tokens=<g>/<want> runner=<kind>` before the command's own
  output (`status=err … runs unbudgeted: <reason>` without a grant), so a slow or failing task says how long it waited
  and how many threads it ran with. With `COWSHED_TIMING` set, the gateway prints each grant: tokens, want, the wait,
  the checkout, and the client's pid and command.
- **Outside `bounded-exec`.** Processes outside `bounded-exec` — Nx's own `nx:run-commands` builds, a host shell's
  `cargo` — take the budget through Cargo's make-protocol jobserver, one fifo per client process. The managed
  environment's `cargo` is cowshed's jobserver launcher (03_caches.md, "The `cargo` jobserver launcher"). It reads its
  environment and its subcommand, the first argument after cargo's global options and a `+toolchain`, in this order:
  - **An inherited jobserver** (`CARGO_MAKEFLAGS` names a live one; for a `fifo:` one, its path exists), as for a
    `cargo` a build script starts: it runs `cargo` as asked, since that jobserver already counts it.

  - **A live parent CPU pool** (`BOUNDED_EXEC_CPU_POOL` plus its count, positive or a resident's 0): a compiling Cargo
    asks that pool for one token per core, as a root build does, never a second host grant. It requests
    `{"op":"cpu-pool","want":<n>,"children":true,"grow":<compiles only>}`; the existing pool accounts for this holder
    and serves its actual grant as a private child pool, named by `pool` in the granted reply. Cargo's test variables
    and inherited count are set to the actual local grant, and descendants receive that child pool, not the occupied
    ancestor. A growing build runs under a make jobserver fifo the launcher creates in its `TMPDIR` and holds open, with
    `g − 1` token bytes, instead of `CARGO_BUILD_JOBS`; each `grown` line from the pool adds its difference to that
    fifo, and the launcher unlinks it when Cargo ends. A gateway pool grows a child by the rules above; the
    `bounded-exec` pool accepts the field and never grows. Any other line after the grant is a protocol failure and ends
    the pool's accounting as its close does. Cargo's own inherited make jobserver still comes first. Noncompiling
    subcommands still run directly. A connected parent's refusal, connection failure or malformed grant fails the
    command. A valid named pool never falls through to a second root or an unbudgeted run. An absent or malformed
    descriptor alone follows normal host admission. The launcher owns its unreaped Cargo process group. Its pool's close
    (a gateway restart, or the lending parent's exit) is said on stderr and stops nothing: Cargo runs on over budget and
    the launcher exits with Cargo's status. Only a signal sent to the launcher ends the group, through the normal
    TERM/grace/KILL path (10 seconds grace), so a launcher killed by its own parent never leaves Cargo's separate group
    behind. It releases its local grant after its command ends; it never freezes a test.
  - **A `bounded-exec` grant** (`BOUNDED_EXEC_CPU_TOKENS` set): it takes no second grant and runs `cargo` with
    `CARGO_BUILD_JOBS`, `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` set to `BOUNDED_EXEC_CPU_TOKENS` where they are
    unset. `bounded-exec` sets the cargo variables only for a cargo runner, so a `cargo` that a `process` runner's
    script starts under a one-token grant would otherwise run one job per core.
  - **A subcommand that compiles nothing** — `metadata`, `fmt`, `locate-project`, `tree`, `pkgid`, `read-manifest`,
    `verify-project`, `version`, `help`, `config`, `search`, `info`, `new`, `init`, `add`, `remove`, `update`, `fetch`,
    `vendor`, `generate-lockfile`, `clean`, `login`, `logout`, `owner`, `yank`, `report`, or none at all (`--version`,
    `--list`), or any subcommand whose whole argument list is one query flag (`-V`, `--version`, `-h`, `--help`), so
    `cargo nextest --version` and `cargo clippy -V` need no entry: it runs `cargo` directly without asking. These runs
    are short and nearly idle, and a queue would make rust-analyzer's frequent `cargo metadata` wait behind gates
    whenever the host is saturated. A sandboxed Nx daemon's toolchain runtime input asks for versions and cannot reach
    the gateway, so a managed checkout would otherwise refuse it. A query flag beside other arguments is not a query:
    `cargo run -- --version` builds the program it passes the flag to. The launcher does not expand aliases, so a query
    against an alias whose expansion ends in `--` (lmao's `proptest-heavy`) builds unbudgeted; that is accepted, since
    such a query is not a version probe and nothing issues one.
  - **Any other subcommand**, aliases and third-party `cargo-*` subcommands included, is a build. The launcher asks the
    gateway on the control socket
    (`{"op":"jobserver","want":<n>,"checkout":"<workspace root>","command":"<command>","grow":<compiles only>}`) with
    `want` one per core, as for a cargo build under `bounded-exec`. The gateway queues and grants it like a `cpu-tokens`
    request (fairness, all at once), refuses the same malformed requests and a growing request that also asks for a
    `pool` (a pool lends a fixed count), and answers `{"ok":true,"lease":"granted","tokens":<g>,"fifo":"<path>"}`. A
    growing grant's gateway writes each grown token into that fifo and then answers `grown` with the new total.
- **The jobserver fifo.** On the grant the gateway creates that fifo in its private runtime directory, mode `0600` and
  owned by the client's user (from the socket's peer credentials). It opens the fifo `O_RDWR` and keeps that descriptor
  until the drain, so the bytes survive while no client has it open, and writes `g − 1` token bytes into it: the
  client's implicit make token is the first. At startup the gateway removes every fifo left in that directory, since a
  restart forgets every grant.
- **The jobserver run.** The launcher spawns the real `cargo` with `CARGO_MAKEFLAGS` and `MAKEFLAGS` set to
  `-j --jobserver-auth=fifo:<path>`, and with `RUST_TEST_THREADS`, `NEXTEST_TEST_THREADS` and `BOUNDED_EXEC_CPU_TOKENS`
  set to `g` where they are unset: libtest and nextest test threads take no jobserver token, so without them a
  `cargo test` or `cargo nextest run` holding `g` tokens still runs one test thread per core. A `--test-threads` count
  the command names outranks them, as a flag outranks the environment. The launcher waits for `cargo` and exits with its
  status, and keeps the connection open, close-on-exec, for as long as it lives. When the connection closes — the build
  ended, or either process was killed, `SIGKILL` included — the gateway drains the bytes left in the fifo, unlinks it,
  and returns all `g` tokens to the ledger, so tokens a killed process read and never wrote back are credited too. A
  `cargo` that outlives a killed launcher keeps its open fifo and runs over budget until it finishes, as a runner does
  across a gateway restart.
- **Where the launcher cannot budget.** It refuses a failed root admission in a managed checkout and never starts Cargo.
  Gateway absence permits unbudgeted execution only in a positively nonmanaged host/CI checkout, not merely because a
  sandbox cannot reach the socket. Marker inspection failures also refuse. The failure diagnostic names the cause and
  says it is refusing to run without a grant. Existing inherited jobserver/pool and noncompiling paths above are
  unchanged; fixed test/nextest grants are never opted into cooperative FIFO reclaim. An inherited `CARGO_MAKEFLAGS`
  whose fifo is gone counts as no jobserver.

Tests: `cowshed-gateway`'s `cpu_budget` ledger unit tests (fairness, share, starvation, leave, the background floor, the
background limit — a lone background request granted half the host, a second one finding no room, and a gate arriving 50
minutes later granted the other half with a zero wait, in the ledger transition of its own ask — an older land served
before a younger land that asked first and both before a feature build that asked before either, an older land's build
growing before a younger land's task starts, a short build growing to and never past its `want`, a new land task
starting before a running one of the same land grows) and `tests/cpu_budget.rs` over a real socket (`SIGKILL` of a
holding client returns its tokens to the waiter at once; checkouts share background's half; a gate job and its task
granted the free half while a lone background grant still holds its own; refusals); the plugin's `cpu-tokens.test.ts`
(each runner's `want` and sizing, a killed holder closing its grant) and `executor.test.ts` (a granted run sized and
released, a refused one run as asked); a jobserver test in which a `SIGKILL`ed `cargo` mid-build leaves the ledger whole
and its fifo unlinked; the launcher's tests (each of its four cases, the `want` it sends, and the variables it sets or
leaves on each path); and the band tests: each class runs in its own band at its own nice whatever the host's state —
interactive above gate above background — and gate work is served returned tokens ahead of interactive and background
work.

## The task server schedules work units

Work units admit to the gateway with the facts of their plan — the units they depend on, the owner that runs them, the
CPU they would use, the serialization windows they hold — and a deterministic scheduler alone decides which ready unit
runs next. The order derives from the logged facts and the host observations; no operator assignment and no model output
chooses which ready unit acquires a lease. Strong models propose task boundaries and interfaces; they do not schedule.

- **The order.** Aged waiters first: a unit that has waited the configured age bound outranks fresh priority and runs
  among the aged in the order they waited, so a stream of fresh urgent work can never starve a queued unit. Then the
  class's token order ("Bands") — gate-class work first, interactive and background work behind it while the host is
  busy (gate work live or no free CPU token) and at parity when it is not — then the coordinator's `urgent` override,
  the unit's leverage (the distinct units transitively depending on it), the cache-domain deadline (known deadlines
  first, soonest first; an unknown deadline sorts behind known ones and is never invented), resource fit (the predicted
  cost against the free CPU tokens, unknown last), and finally the stable unit id. The schedule is recomputed at every
  event and at the oldest waiter's age bound; the same facts always produce the same order.
- **Ownership.** A unit keeps exactly one live attempt: its admission connection is the attempt, held like a CPU-token
  stake. A second admission of a live unit is refused. A connection that closes without a terminal outcome abandons its
  attempt, and a delayed holder of a settled attempt can never advance a newer one.
- **Serialization classes.** A granted run holds its declared classes from its selection until its terminal fact:
  `cowshed-new` (one `cowshed new` on the host at a time), `image` (a workspace-image mutation window), and
  `land-publication` (the checked land-authority transition). `land-publication` is a publication fence — it serializes
  publication only, never a whole-host CPU grant: gate coordinator groups run concurrently in one CPU class and order.
  Lands queue for the fence in one order — the first claimant in admission order, behind older aged waiters — so an
  urgent land waits its turn and never overtakes an older claimant. These classes bound the windows the operator keeps
  by hand today; the disk-lifecycle lease above still spaces every disk command, and the CPU budget below still bounds
  every runner. A lease-blocked unit holds up nothing beside it.
- **Caps and bands.** A unit may declare its own parallelism ceiling — the caps its run kind applies to itself
  (`nx parallel`, `CARGO_BUILD_JOBS`, `--test-threads`) — and the grant reports that ceiling clamped to the CPU tokens
  it holds, so a run's own parallelism never exceeds its grant. The grant is the only allocation authority and leaf
  controls derive from it, never from a static profile. The grant also names the run's band, its class's row of "Bands":
  interactive work foreground, gate-class work normal — the land turn's holder included — and background work
  background, whether or not the host is busy. Placement is server-owned: the band follows the class alone, and no other
  caller field raises a band or widens a ceiling.
- **The CPU budget stays the only allocator.** On selection the scheduler submits the unit's CPU ask to the budget
  above, in selection order, and the grant reports the tokens the budget gave it. The scheduler holds no token counter:
  its capacity view folds the budget's own ledger, a capacity observation never mints a grant, and what cannot be
  observed stays unknown rather than becoming headroom. `TaskClass` is the work-unit ordering and placement axis, not
  the root-admission `JobserverClass` axis. Selected work-unit asks use `JobserverClass::Background`, including units in
  the `TaskClass::Gate` band; a task band must not create an exclusive gate root per group. The trusted execution
  boundary owns the `JobserverClass::Gate` grant; its coordinator groups ask through that grant's relay pool
  concurrently.
- **Wire.** `task-admit` is held exactly like `cpu-tokens` —
  `{"op":"task-admit","unit":…,"owner":…,"incarnation": …,"deps":[…],"leases":[…],"want":<n>,"priority":<u32>,"urgent":true,"class":"background","cost":<u64>,"deadline": <epoch-ms>,"command":…}`
  — answered `queued` at once and a granted `taskGrant` (`unit`, `band`, `caps`, `cpuTokens`) when the scheduler selects
  it. `task-terminal` is one-shot — `{"op":"task-terminal","unit":…, "outcome":"landed"|"failed"|"abandoned"}` — the
  fact every dependent reads and the release of the unit's classes and CPU grant; its decision log names the owner and
  workspace incarnation the lifecycle receipts bind to. `task-queue` is one-shot and answers `taskQueue`: the ready
  units in selection order (with any lease class they find full and who holds it), the waiting units with each
  dependency's outcome, the running units with their leases, granted tokens and caps, each class's places and holders,
  and the CPU ledger as an observation.
- **Decisions are logged facts.** Every selection and terminal fact is written to the `task-server` timing stream, and
  `task-queue` reports the live fold; nothing in the scheduler is a second authority over the plan.

Tests: the scheduler's own regressions pin the policy without an agent in the loop — the exact order over a crafted fact
set, the same order from the same facts however they arrive, a low-priority unit selected at its age bound under a
stream of fresher urgent arrivals, the serial classes with the urgent land behind a turn in progress, one live attempt
per unit, the caps applied and clamped to the grant, and every class in its own band on a quiet host and a busy one with
gate work ordered first — plus `tests/task_server.rs` over a real control socket: units admitted and advanced by their
terminal facts alone, with the caps and the queue view read off the wire. Gate-band units with spare CPU capacity retain
concurrent grants; a work-unit class never creates exclusive root-gate admission.

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

1. Validate the host store — both dedicated volumes present, mounted, marked, and with canonical flags (01a_apfs.md). A
   store that fails validation stops here and reports; nothing below can be meaningful without it.
2. Count the recorded projects and list the workspace supervisors still serving from before this daemon, then serve: the
   control socket and the supervisor manager answer from the first moment. Status answers throughout, and so do the
   audit, mirror, and simulator requests, none of which reads a workspace mount.
3. Eagerly heal every recorded project's mounts: every project's main attached and mounted at its checkout path first,
   then every other workspace under the mount root (02_workspaces.md). Each step is a lifecycle span of its own —
   `startup-heal discover`, `startup-heal open <repo>`, `startup-heal mount <repo>/<workspace>` — so a slow pass reads
   as the step that spent the time.
4. Restore every attached workspace's session into the gateway from what step 3 mounted.

Positively authorized pending project retirements are excluded from the ordinary heal set. After the control socket and
supervisor manager are serving, startup resumes those records through the same owned drain, unforced detach and
preservation transaction as `cowshed forget <repo> --abandon` (02_workspaces.md). It does not infer abandonment from a
missing checkout, marker or process; a failed or busy retirement remains fenced and is reported without cancelling
healthy projects' mount restoration.

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
fallback for pinned clients, and gateway visibility into intercepted plaintext. A single machine-wide CA trusted inside
every workspace is rejected: one key whose compromise silently breaks all transport security, shared by every workspace.
A MITM broker that substitutes placeholder tokens at that shared boundary is the same design and is rejected for the
same reason.

**Registry mirrors retained.** Protocol-aware mirrors provide digest validation, fleet-wide deduplication, bounded cache
storage, and offline reads that generic interception cannot.

**Git data-plane protocol rejected.** Host-granularity egress cannot constrain repository mutation. Coordinator-only,
project-scoped mirror fetches make the no-push boundary structural.

**Platform-specific data-plane identity.** macOS uses a port block because it lacks per-process network namespaces;
Linux uses a private netns, one per-incarnation Unix socket, and its namespace-local compatibility connector, allocating
no port block. A host-shared listener or token-only identity is rejected because endpoint isolation, not a bearer
secret, must select the workspace.
