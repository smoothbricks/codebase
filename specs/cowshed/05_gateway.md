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

**What the token proves, and to whom.** The token is per workspace and authenticates only at that workspace's own
endpoint: the selected listener compares the presented value with its own session's token alone, so a token is worth
nothing at any other endpoint. It is not secret from the workspace's siblings. Seatbelt does not mediate
`kern.procargs2` (04_sandbox.md), so a sandboxed sibling can read another workspace's proxy variables, token included.
What holds is that the read token is unusable: the sibling's profile refuses a connection to another workspace's block
(`EPERM`), and the sibling's own endpoint answers another workspace's token with 407. Another local uid sees no start
environment of this user's processes (04_sandbox.md), and reaches a loopback listener only to get 407 without this
workspace's token. An unsandboxed process of the owning uid can read `.cowshed/token` and reach the listener: that is
the owner's own authority, not an attack, and is out of scope. If that ever stops being acceptable, the remedy is a
separate uid for the gateway and its workspaces. The token is never exported bare: no job environment and no
`.cowshed/env` carries `COWSHED_WORKSPACE_TOKEN` (03_caches.md), and `.cowshed/workspace.json` is what marks a
workspace.

Create and fork mint a token. Restore stops admissions, drains the Linux connector and gateway connections, kills the
connector cgroup, rotates the token, unlinks/recreates the Linux socket and namespace-local connector when applicable,
and atomically rewrites the in-image token before new execution is admitted. Detach performs the same drain, cgroup
kill, and socket unlink without rotating persistent authority; attach creates the socket and connector before admitting
execs. The old token and every pre-restore keep-alive or tunnel are therefore invalid immediately; a preserved macOS
port does not preserve authority. Remove uninstalls the session with its listener, so the removed workspace's token
authenticates nowhere, including at a later workspace given the freed block, which mints its own. A policy miss after
successful endpoint and token authentication is 403 with a machine-parsable grant hint naming both remedies, the
standing one first: `cowshed grant --project-wide --egress <host>` (every workspace of the project) or
`cowshed grant <ws> --egress <host>` (that workspace). The hint names the bare canonical DNS name or IP address, never a
CONNECT authority: the grant host grammar is shared with grant admission and exec (04_sandbox.md).

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
| `nx-cache-server`        | The Nx cache server's stable name, a link to its own stored copy `nx-cache-server-<sha256>`.   |

`cowshed-source`, one level up beside `bin`, records the path the current install came from and its package version.
`install.lock`, beside it, is the install slot's lock (below); it is never removed. Nothing in `bin` answers to `cargo`
("Cargo reads its holder's jobserver"). Earlier installs planted a `cargo` link there, saying the stable name or a
stored copy; every install removes it before it changes anything else, so `cargo` on a `PATH` that leads with `bin` is
the toolchain's own. A `cargo` there that no install planted — a file, a foreign owner's, a link saying anything else —
is refused by name and left untouched.

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
failed and whether the restore and the reload did. The reload takes the one step what launchd holds allows: an agent it
does not hold is bootstrapped from its plist, since `kickstart` answers it "Could not find service" (exit 113); one it
holds is kickstarted; one whose bootout went overdue (below) is left alone and named, since a kickstart of a job still
in teardown is launchctl 37, "operation already in progress". A first install has nothing to roll back to and keeps its
copy. A setup on a host with no gateway agent installs the same way without the activation. Setup installs only after
its storage repair succeeded.

**A restart waits for the old gateway's exit, within launchd's own bound.** An activation that boots the agent out (a
changed plist, a daemon running other bytes) and `gateway stop` wait until the old daemon has exited — the kernel's
`NOTE_EXIT`, watched from before the bootout signals it, which it posts once the process has released every descriptor,
its listeners included — and launchd no longer holds the job. launchd SIGKILLs a process its bootout's SIGTERM has not
ended at the agent's exit timeout, so that is the only bound: the exit timeout of the definition launchd loaded, read
from `launchctl print`, plus 1 s its unload may trail its kill by. Past it the bootout is overdue, an
`environment-missing` error naming what is left — the pid that has not exited, or the job launchd still holds — with the
bootstrap that loads the agent once that has gone. The gateway plist states `ExitTimeOut` 30 s (`GATEWAY_EXIT_TIMEOUT`),
and the daemon's drain force-stops what it accepted after 20 s (`GATEWAY_DRAIN_WITHIN`), so the drain, its force stop
and the audit flush finish before launchd's kill. Measured 2026-10-22: launchd's default exit timeout is 5 s; the plist
stated none, so a bootout SIGKILLed a draining gateway 5.03 s after its SIGTERM, while the install's own wait, a fixed 5
s, gave up 0.03 s earlier and reported the bootout stuck; its restore then kickstarted the job still in teardown
(launchctl 37), and launchd dropped it a moment later, leaving the host with no gateway.

**An install refuses while a land or a gate runs.** Activating the agent restarts a gateway that runs, and so does the
host move inside `setup`; a restart closes every lease connection ("Crash safety by connection"), a land's gate-job pool
with them, and every task that gate admits afterwards is refused. Measured 2026-10-18: an install restarted the gateway
at 17:00 while a land's gate ran, its next tasks failed with
`cpu-tokens parent pool refused: the gateway closed the lease connection`, and the land failed. Between a land's
commands nothing of it shows in the ledger, and a restart there refuses its next command's connection ("A land step
waits for an install in progress"). So an install — holding the slot, after its build refusals and before it installs
anything that would restart a gateway that answers `status` — refuses with `Conflict`, naming each blocker, while a land
holds its repository's turn (the first live ticket under `dev.cowshed/land-turn/<repo>/`, "Land turns": its workspace,
ticket and pid) or that gateway's `cpu-budget` ledger shows gate-class work holding or wanting tokens (`gateJobs` and
`gateQueue`, each entry's `checkout` and `job`: `gate job <n> at <checkout>`). It never waits for them, and the refusal
leaves the host as it was; the hint is to rerun once they have ended, and `cowshed gateway status` lists both. A land
queued behind the turn's holder runs nothing yet and is not a blocker: its first command waits for the slot. Land turns
that cannot be read refuse too, naming the directory: not knowing whether a land runs is no permission. The install
reads only those two ledger fields: the gateway it is about to replace is of another build, and a field one build has
and the other lacks must not turn the check into a restart that cuts a gate off. A gateway that does not answer
`cpu-budget` names no gate job, and the install says so. An install that restarts nothing (no daemon answers, or the
refresh finds the agent current) checks nothing. This replaces a wait with no deadline for the gates in flight: measured
2026-10-21, it held an install behind a gate of 2 h 26 min until the gate was killed, and operators took to granting
installs a window by hand.

**A land step waits for an install in progress.** The ledger shows a land only while one of its gates runs, and a land
is several cowshed commands with none running between them: measured 2026-10-20, an install found the ledger empty as a
land's first gate ended and restarted the gateway as its host gate started, and that gate's `cowshed exec` failed with
the daemon's socket refusing the connection, failing the land. So every command a land runs — `exec --class gate`,
`rebase` and `land` — before it asks the daemon anything, waits for an install that holds the slot ("The install slot",
below), saying on stderr that it waits and for which file, and then holds nothing. A step that waited is still the build
it started as, and the daemon that install left refuses another build. Measured 2026-10-21: a land's gate passed in
1,503 s, its next step waited for an install, and the daemon refused it by build (exit 4), throwing the green gate away.
So a step that waited and is then refused as another build goes on as the installed cowshed: it execs, in place, the
stored copy the stable name says once the install is done, with its own arguments, keeping its pid, working directory,
environment and descriptors, and says so on stderr. A step that did not wait, or a refusal of any other kind, is
returned as it is. A daemon connection refused later in the step, while an install holds the slot, is that install
restarting the daemon — a positive signal — so it waits for the install and connects once more to the daemon the install
started; a refusal with no install holding the slot is returned as it is. An install that restarted the gateway —
`gateway start` and `setup`'s refresh alike — holds the slot until that gateway serves: its control socket answers from
the installed bytes, its startup pass is over and it is not draining, the wait `gateway start` reports. Releasing the
slot at the restart is not enough: measured 2026-10-20, `setup` restarted the gateway and released the slot at once, and
the land step that had waited on it found the daemon's socket refusing the connection, failing a land whose gate had
passed. A land step never holds the slot for its run: it did once, and an install then waited on every land step in
flight, between its gates as well as during them. An install refuses a gate that holds or wants tokens ("An install
refuses while a land or a gate runs"), because a gate's CPU pool is gateway state a restart ends and is never carried
across one. A job in flight lives in its workspace supervisor, which a gateway restart leaves running; what a restart
breaks is a new connection to the daemon, and that is what waits. The ledger is never the guard against a restart
between two of a land's commands, since no process of that land holds anything the gateway records there; its land turn
is. Status reads the slot without taking it.

**An install never refuses a client connection.** A restart closes every workspace listener until the next gateway's
startup pass restores its session (5 s and more), and a stock client cannot wait for that: measured 2026-10-20, an
install restarted the gateway under a shed's Nx build, and Nx read the refused cache connection
(`Failed to send request …/nx/v1/cache/…`) as fatal and failed the build. No wait can prevent it. Nx sends a cache
lookup holding no CPU token, so between two lookups no ledger, lease or slot shows that it runs, and waiting for all
running work would hold installs back on any busy host. A typed "restarting" error is no answer either, since Nx reads
only HTTP statuses. So the listening sockets outlive the restart. On macOS an install that restarts the gateway —
`gateway start` and `setup`'s refresh alike, not a `--downgrade`, whose older gateway might not take them — serves
`listener-handoff.sock` beside the control socket from before the restart until the restarted gateway serves. The old
gateway, as its drain begins and while it still accepts, gives the install a duplicate of every TCP workspace listener
(`SCM_RIGHTS`), and its drain then answers what it already accepted as before (the Nx route's 404/403). The install's
copies keep each port listening, so the kernel queues a connection made while no gateway runs. The next gateway takes
them before it binds anything ("Startup contract" step 2) and serves each on its restored session (step 4), queued
connections included. Only this user's processes may give or take, and the store deny keeps every sandbox off the
socket. Every failure is what a restart was without the handoff: no install serving the socket, an old gateway that
predates the handoff, an install that dies (the kernel closes its copies), or a failed take, after which the install
closes its copies before the new gateway binds. A gateway that drains with workspace listeners and finds no install
serving the handoff — `gateway stop`, then a fresh start — says on stderr how many it did not carry and why, so the
refused connections that follow read as designed, not as a failed handoff. Linux binds no TCP workspace listener, so it
carries none. An exec client of the replaced build reads its job's end from its own drained supervisor (11_shell.md
"Draining a supervisor of another build"); a queued CPU request is cut off, and told by which install ("A queued CPU ask
outlives an install").

**A queued CPU ask outlives an install.** A restart closes every lease connection ("Crash safety by connection"). A
holder's grant ends with it and its command runs on over budget, but a queued ask failed: measured 2026-10-18, a land's
gate tasks failed with `cpu-tokens parent pool refused: the gateway closed the lease connection`. So the install that
restarts a gateway names itself on `listener-handoff.sock` — its pid, its binary, and how long it gives the gateway it
starts to serve (`INSTALL_SERVES_WITHIN`, 180 s, the bound `gateway start` waits). Linux serves no handoff socket, so
there a restart cuts the queue off unnamed, as before. A gateway whose control socket stops while an install serves that
socket first cuts its CPU queue off: every queued ask — a background `cpu-tokens` ask, a gate job's admission, a gate
pool's child — and every ask after is answered
`{"ok":false,"code":"install-restart","error":…,"install":{"pid":…,"binary":…,"servesWithinMs":…}}` before its
connection closes, and the stop waits up to 2 s for every queued ask to be told. A client so answered asks once more, of
the gateway that install starts: every 100 ms it asks again while the control socket refuses or the answer is the same
install's cut-off, and the first other answer is its one retry, a grant or a refusal returned as it is. The bound covers
reaching that gateway, not the admission: once it answers `queued`, the wait has no deadline ("No admission deadline").
Past the bound the ask fails, naming the install's pid and binary and how long it waited, and is never asked again; a
refusal of any other code, or a connection closed with no answer, is never retried. Nothing of the old ledger is carried
across: the retry is a fresh ask. A gate pool's child asks the host, since its pool ended with the old gateway, and says
that it asks as background work; so does an ask that finds its pool absent after a restart. With no install serving the
socket, or one of an older build, a restart cuts the queue off unnamed, as before. An install still refuses while a gate
runs ("An install refuses while a land or a gate runs"): a gate job's own grant ends with the restart, and its tasks'
later asks lose its class.

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
says so). Status reports `pendingInstall: true` while the slot is held, absent while it is free; `gateway start`'s own
report, which holds the slot, carries none.

**Land turns.** A repository lands one land at a time, in the order the lands asked (tg#7385 keeps the per-repository
serialization; FIFO, no priority). A bare `flock(2)` on one lock file serialized lands but never ordered them: the
kernel wakes every waiter when the holder releases and the first to retry wins, so a land asking at the moment of a
release barges ahead of one that has waited for minutes. Measured 2026-10-21: a 72-commit train queued behind one land,
and when it released, a land that had asked after the train took the turn and held it for 36 minutes; the train's only
output meanwhile was watchdog `QUEUED` records. So `cowshed land-turn <repo> --workspace <ws> -- <cmd…>` waits for the
repository's turn, runs the command holding it, and exits with the command's status (128 plus the signal for a signalled
one; SIGINT, SIGTERM and SIGHUP are passed on to the command, and the turn is held until the command really ends). Each
land takes a numbered ticket in `dev.cowshed/land-turn/<repo>/`: a file locked exclusively by the waiting process for as
long as it waits or holds, so the kernel drops it with that process however it ends. Numbers are given under the
directory's `.seq` lock, each above every ticket present, and a ticket is locked under a temporary name before it is
renamed into place, so no reader ever sees an unlocked live ticket. A ticket whose lock is free is dead and is removed
by whoever finds it. The lowest live ticket holds the turn; each other land waits on the lock of the live ticket just
ahead of its own and, when that ends, looks again, so a killed holder or a killed waiter blocks nobody and only the next
in line is woken. A waiting land says on stderr how many lands are ahead and who holds the turn — its workspace, pid and
when it asked — when it starts waiting, whenever the land ahead changes, and every 90 s between (the land watchdog's
cadence), and how long it waited once the turn is its own. `--outside-lock <file>` names the lock file lands took before
every caller asked for turns: the head takes it too, exclusively and keeping the file, so it excludes a land started
that way. A process outside the queue holding that file records nothing, so while the head waits for it the head names
it: when it starts waiting and every 90 s after, one census of the processes with the file open
(`build_volume::nx::holders`; Darwin names no `flock(2)` owner, so an open descriptor is the evidence) gives each one's
pid and command, the head's own descriptor left out. A census that finds no other process says so (the holder let go as
it looked, or is not this user's to see), and one that cannot be taken says why. Measured 2026-10-21: a land waited 1002
s for the outside lock having said once that "a land that did not ask for a turn" held it, naming no process. The census
decides nothing: the head waits for the kernel's lock alone, and it is the one holder census 02_workspaces.md admits
outside a destructive operation ("Holders cowshed does not run"). The wait starts before the first census, on a thread
of its own, so no census delays taking the lock. Measured 2026-10-21 on one fixture (a `lockf -k` holder for 1.5 s,
debug builds, a 709-process host, 20 interleaved runs each): without the census the first report came 18 ms after the
wait began and the turn 20 ms after the holder let go (medians); with it, 213 ms and 17 ms, at about 0.11 s of CPU per
census. A land's turn covers rebase, gate, fast-forward and adoption; the push to a remote runs after the turn ends,
since a remote is never needed to land (tg#6988). A turn is only as durable as the process that holds it: a land that
shares its caller's session ends with that session. Measured 2026-10-21: one lander's lands died with its tool session
three times, twice while queued and once mid-gate. So a caller that may go away first starts the land in a session of
its own (`setsid`, output to a file, not its child) and waits on the land's pid; `land-turn` itself never detaches,
since the caller that stays reads its output and its exit status.

**A ticket's life is recorded.** Measured 2026-10-21: a land queued for 635 s vanished with its agent's session, its log
stopping mid-wait, and nothing recorded it, so it read as running for half an hour while it did not exist. Each
repository's `turns.log`, beside its tickets, gets one JSON line per event, appended and never rewritten:
`{ atUnixMs, event, ticket, outcome? }` with `event` one of `queued` (the land took its place), `taken` (it holds the
turn), `finished` (its command ended; `outcome` says how — `exited <code>`, `killed by signal <n>`, or that the turn
ended before its command did because cowshed failed) and `abandoned` (its process ended without finishing — killed,
crashed, its session gone — found by whoever read the queue next, waiter or status, before removing its dead ticket).
How long a land waited and held is the gap between its lines. A line that cannot be written is said on stderr and never
stops a land. `cowshed gateway status` lists each repository's live tickets, the holder first, and the tickets found
abandoned in the last day, as
`landTurns: [{ repo, tickets: [{ number, pid, workspace, askedUnixMs, command }], abandoned?: [<turn log line>] }]`,
read without taking a turn, the way it reads the install slot.

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
commit of `<candidate>..<installed>` must be held by the candidate, by ancestry or by patch identity
(`GitRepository::commits_not_held`: `git cherry` equivalence for ordinary commits that change something, and merges that
author nothing). This is an identity question, deliberately not `cowshed rm`'s content one (02_workspaces.md, `rm` step
1): a build that lacks a commit lacks it whatever its tree says, while a workspace whose change main holds under another
commit loses nothing when removed. Main is the authority because it outlives every workspace: a workspace's commit may
be rebased away when it lands and the workspace removed, while main only moves forward. A rebased or cherry-picked
commit, or a squash of one commit, is held by patch identity; a squash of several commits matches none of them, and is
refused naming each.

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
nothing launchd could start dangles), then every stored copy with its `.build` record, a `cargo` link an earlier install
planted, and every `cowshed` on `PATH` that is a link saying exactly the stable path — the entries setup made, and no
other link that merely arrives there; a `cargo` no install planted is left as it is. A plain `gateway stop` keeps all of
it: it is host state rather than agent state, and the next `start` is then a plist write.

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
(`Starting`, `Ready`, `Failed { retry_after }`, or `Clearing`) held by the actor; backend startup, readiness waiting and
a store clear run outside it. Concurrent requests for one repository share one start (single-flight). A failed start
records `Failed { retry_after }` with `retry_after` taken from `GatewayTimeouts.connect` (default 5 s); until it
elapses, requests for that repository are refused as in the outage above, without starting another child, and other
repositories are unaffected. If the child exits, the next request replaces it immediately within the same call. A
startup worker's task failure is reported through the same failed-start transition; it cannot leave an unreported
`Starting` state. Cancelling the reporting task also aborts its owned worker. A host-configured backend cannot replace a
gateway-managed live child. Readiness logs the project, address and reclaimed staging-file count, never credentials. A
backend lives with its project's sessions: when the last session of a repository leaves the gateway, the gateway stops
the child it started for it (a start in flight is aborted) and forgets its state, so the next session's first request
starts a new one; a host-configured backend owns no child and stays. Measured 2026-10-19: seven of the gateway's nine
servers served scratch projects that tests had adopted and discarded hours earlier, each running until the gateway
restarted.

**Clearing the store.** `nx-cache-clear` (Control plane, below) moves an idle repository whose backend the gateway
started, failed to start or never started to `Clearing`: the backend stops, the store is cleared under its own lease
(03_caches.md, "Nx accepted-task artifact service"), and the state is forgotten when the clear reports, so the next
request starts a fresh backend. Every request meanwhile is refused as in the outage above, and no start begins. A
repository with an admitted or queued Nx request or a publication is refused, naming the work by workspace; one that is
`Starting` or already `Clearing`, or whose backend the host configured, is refused too.

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
the data-plane token is never accepted on the control plane. The Unix accept loop records listener failure, unreadable
peer credentials, unauthorized UID, connection-capacity refusal, connection-task failure and shutdown; an admission
refusal is never a silent return.

Workspace sessions are installed and removed over the control plane, and the gateway's session table is a cache of host
inventory, never an authority. Each session carries its workspace's effective grant revision, which every grant change
and every attach advances. An install must exceed every revision that workspace identity has held — a removal leaves its
revision behind as a tombstone — and a removal must name the installed revision; either refusal is `revision-fence`, and
a removal of a session that is already gone is `not-installed`.

A resident exec checks only its own installed revision with
`{"op":"session-current","workspaceId":"<identity>","revision":<u64>}`. The authenticated host control plane answers
`{"ok":true,"current":true|false}` directly from the installed session table: false for an absent identity or a
different revision, including a removed session whose revision remains only as a tombstone. This is the same
instantaneous observation as filtering status, not a reservation against a later rotation. It neither builds a host-wide
census nor keeps a second revision inventory. Status remains the host's full diagnostic view; a failed single-session
query leaves resident admission to the existing controller path.

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

`{"op":"nx-cache-clear","repoId":"<owner/repo>"}` is one-shot and clears the project's Nx accepted-task store (the
`/nx/v1/cache/{hash}` route's "Clearing the store"). It answers `nxCacheCleared: { repoId, entries }` with the archives
deleted. Nx work of the project in flight, a backend starting or a clear already running is the failure code `busy`,
whose message names that work; any other refusal is `rejected`.

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
  alone-to-shared retries. Answer reads wait on descriptor readability under those original deadlines and drain bytes
  already received, including a complete refusal followed by peer close. Closing after writing a refusal is valid: the
  client does not require the peer to remain connected while it consumes the answer, nor set receive-timeout socket
  options after the peer can close. The runner checks remaining time before spawn and inside its existing pre-exec
  closure, after any fork-lock wait: exhausted admission never executes the native tool. A failed pre-exec fork stub is
  reaped by the spawn operation; it performs no disk-tool effect. An overrun signals the owned process group and waits
  to reap the child, retaining its actual exit or signal status. A kill failure retains the kill error, exact child pid
  and a nonblocking observation of exited, still running or reap failure; it neither waits without bound nor joins live
  output pipes. A reap failure is distinct and never claims the child was reaped. A protected disk-image command
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
  nextest runners, `bun test` targets, cargo commands it runs. Its requested parallelism is the command's runner's own,
  read from the runner's own flag rather than assumed from the core count, since a runner's flag and its actual
  concurrency can disagree: a `nextest run` names `--test-threads` or defaults to one per core, and one with
  `--no-capture` (`--nocapture` past `--`) runs its tests serially whatever count it names, so it asks for one;
  `bun test --parallel=N` names N workers, a bare `--parallel` defaults to one per core; `stryker run` names
  `--concurrency`/`-c` N or N% of the cores and defaults to one worker per core less one past four cores (31 on a
  32-core host); `vitest` names `--maxWorkers` N or N% and defaults to one per core less one, one with file parallelism
  off; `go test` names `-p` and defaults to `GOMAXPROCS`, normally one per core; `cargo mutants` asks for its own
  jobserver's tasks, `--jobserver-tasks` or one per core by default — its `--jobs` (default one) counts the cargo
  processes that share those tasks, so it lowers nothing, and asks for more only when it exceeds them, since each
  process runs at least one thread; a cargo build (`cargo build`/`test`/`clippy`/…, `nextest archive`, `napi build`)
  defaults to one job per core; anything else, one `bun test` process included, defaults to one. A runner's flag counts
  only in its own invocation's arguments, up to the next `;`, `&`, `|` or parenthesis outside quotes and `$(…)`, so
  `ulimit -c 0 && bun stryker run` asks for Stryker's default; a value that is no count (`-p="$(nproc)"`) leaves the
  default. Measured in nightly 477, `nx run-many -t mutation` under `NX_PARALLEL=100%` on a CI host without a gateway:
  Stryker started 31 workers per package and cargo-mutants a jobserver of one task per core, side by side, and Stryker
  for three packages and a fourth target were SIGKILLed within 20 seconds. A target's `parallelism` option selects that
  request, but positive inherited `NEXTEST_TEST_THREADS` and `CARGO_BUILD_JOBS` are ceilings on their respective
  runners. A configured environment count may lower an inherited ceiling, never raise it. `want` is the lesser of the
  request and its ceiling. `test.concurrent` inside one process, and a Go package's `t.Parallel` tests inside one test
  binary, are not another CPU.
- **Sized to the grant.** A grant is between 1 and `want`; runner concurrency is at most `min(want, granted tokens)`,
  never raised above an inherited ceiling. A runner that reads a make jobserver needs no count of its own: Cargo's rustc
  count is the grant's fifo ("Cargo reads its holder's jobserver" below), so no `CARGO_BUILD_JOBS` is set from a grant.
  Every other runner's flag is rewritten from the count, since none of them reads a jobserver. The executor sets
  `NEXTEST_TEST_THREADS` and rewrites nextest's named `--test-threads=` count to that limit, rewrites Bun's
  `--parallel=` count, and sets `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` for a cargo runner and for any other
  command, each under its inherited/configured ceiling, so a script's `cargo test` is sized too. Stryker's
  `--concurrency=`, Vitest's `--maxWorkers=` and go's `-p=` are rewritten where named and otherwise added right after
  the invocation, so a config file's count cannot outrank the grant. cargo-mutants makes a jobserver of its own and
  hands it to every cargo it starts in place of the holder's, so it gets `--jobserver-tasks=` that limit, its named
  `--jobs` and `CARGO_MUTANTS_JOBS` lowered to at most it, and each of its cargo processes `⌊limit / jobs⌋`, at least
  one, as `CARGO_BUILD_JOBS`, `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS`; `--jobs` is never added, since
  cargo-mutants refuses it beside `--in-place`. Every command receives the actual held grant as
  `BOUNDED_EXEC_CPU_TOKENS`; the sub-ledger and telemetry report held tokens, not a fabricated runner count.
- **Nested runners use a parent-held sub-ledger.** A granted executor serves its T tokens on a private socket and
  publishes that endpoint as `BOUNDED_EXEC_CPU_POOL`, beside its `BOUNDED_EXEC_CPU_TOKENS` count and the pool's make
  jobserver fifo ("Cargo reads its holder's jobserver" below). A default nested executor connects to its immediate
  parent's live pool, requests its own demand (the count is not a bound on the ask: the pool grants at most what it
  can), and holds one complete local grant on that connection. A background pool accounts for every holding sibling, so
  their sum never exceeds T. It serves its own FIFO queue, granting the head `min(requested, free)` once at least one
  token is free; that count is final, not a partial holding awaiting more tokens. Queued siblings wait only for that
  pool's own holders. Host checkout fairness remains the upstream ledger's concern. A gate job's pool is the exception:
  it lends nothing of its own and relays each child's ask to the host ledger as a gate-class request for the gate's
  checkout ("Gate admission" below). An admitted child serves its granted count as its own pool, and grandchildren draw
  only from that pool: no holder waits for another grant from an ancestor or the host. The parent remains the owner of
  its upstream grant. A child returns only its local allocation by closing its connection; it never releases the
  parent's connection. Upstream loss revokes the downstream pool and its connections, so a dead parent cannot grant
  tokens to a descendant. Revocation ends accounting, never work: a command whose borrowed or host grant closes while it
  runs — the gateway restarting, which every install does, or the lending parent exiting — runs on over budget to its
  own exit, says so on stderr (`cowshed: cpu-tokens grant ended: … it runs on over budget until it ends`), and is judged
  by that exit alone, as "Crash safety by connection" below says of every runner across a gateway restart. A connection
  is a ledger entry, not the work's owner; only a bound, a signal or the process that started the command stops it. A
  grant loss is never a failure and starts no teardown. A bare or invalid count proves no allocation; an absent endpoint
  follows normal host admission. Once connected to the parent, refusal, silence or protocol failure is an ordinary
  admission failure, never a second host take while that parent may still hold its grant. A stale descriptor whose
  endpoint no longer connects is treated as absent. Only the executor publishes its actual live pool; `options.env`
  cannot inject either marker into a command. The span and `cpu.inherited: true` record identify a parent-pool grant and
  its actual local wait. Pool setup or cleanup failure is an ordinary failure, never a successful unaccounted run.
  Real-process default-runner proofs live in `executor-host.test.ts` under `test-host`, because the default macOS runner
  acquires a real RAM volume. Node peers import the built executor artifact named by `executors.json`; the host target's
  existing build dependency produces it, rather than compiling TypeScript inside a test's guard. Pure pool and executor
  unit cases never attach images. **Known admission ambiguity:** a connect error is currently classified as an absent
  endpoint. A full listen backlog or local file-descriptor exhaustion can also prevent connection to a live pool, so
  this classification does not yet prove that its parent has released the upstream grant. Distinguishing those cases
  before any host fallback remains an admission follow-up; connected refusals and protocol errors already fail without a
  second take.
- **Work a holder starts draws on its grant** (2026-10-19; first grant wins, 2026-10-20). The rule above, that no holder
  waits for another grant from an ancestor or the host, covers every way work starts under a holder, not only a nested
  runner. A holder exports its grant through two doors, its fifo and its pool socket, and work its command starts takes
  the holder's tokens through either, the holder's own token last ("Cargo reads its holder's jobserver" below): a Cargo
  reads the fifo and asks nobody, and a nested runner or a `cowshed exec` asks the pool socket. The controller runs in
  the requester's process (in-process for the CLI and every embedder; `cowshed controller` inherits its embedder's
  environment), so an exec's supervisor names the requester's descriptor as the jobserver request's `parentPool`. An
  exec from a holder then waits on the pool and on the host ledger at once: the gateway asks the pool for the job's
  `want` (`{"op":"cpu-pool","want":<n>}`) and the host ledger for the request's checkout at its class's rank. The first
  grant serves the job, its fifo and its own lent pool in the job's scope, as any grant is served. The other ask is
  withdrawn: the pool connection closes, which leaves the pool's queue, or the host stake drops, which leaves the
  ledger's. The gateway says so on stderr
  (``cowshed: `<job>` was granted <n> token(s) by <ledger>; its ask of <ledger> was withdrawn after <ms> ms``) and as a
  `COWSHED_TIMING` `jobserver` event naming the job, the ledger that granted and the wait. One ask is never granted
  twice, and each grant is real and accounted by the ledger that made it: a host grant in the host ledger like any root
  grant, a pool grant in the pool alone. Nothing hangs on a pool: a pool whose tokens its own work holds leaves the job
  to the host, which grants it when it has room. A pool that cannot be reached is absent, and the job asks the host
  alone (said on stderr); a pool that answers and refuses, falls silent or breaks protocol before the host grants
  refuses the job. A pool grant's end ends the job's accounting, never its work: its own pool lends nothing more and the
  job runs on to its own exit. A resident holds no token, so it is admitted through its own relay pool as before. A
  `cargo` started from a bare shell, under no holder, holds nothing its descendants could wait behind: they ask the host
  as any root request does.

  Measured 2026-10-19, the case it closes: a test case's own scratch job — a cowshed job of the scratch project's
  supervisor, which the gateway starts and so is no descendant of the test — asked the host as background for its own
  checkout and waited 26 minutes behind the test's runner, which held background's whole room, 9 of 18 tokens: the floor
  grants only below that room. The same shape deadlocks at any demand: runners that together hold background's room
  while each waits on a child queued at the host leave no child grantable, whether one nextest run of nine integration
  cases at nine threads or nine one-token cases. Before "Room for a gate" capped background at half the host, that child
  took the 18th token. A runner kind cannot close it; drawing on the holder's grant does.

- **A run without a gateway budgets itself** (2026-10-19, Danny: "Runner should be able to handle whatever is being
  executed"). A host without a gateway — a CI runner, a machine without cowshed — had no budget at all: `bounded-exec`
  found the socket absent and ran every command exactly as configured, so under `NX_PARALLEL=100%` Nx started every
  target side by side and each runner sized itself to the whole host (nightly 477, "Who asks"). Leaving the count to
  each command is the absence of a mechanism, not a policy. So the plugin's `preTasksExecution` serves the Nx run its
  own CPU pool when nothing else budgets it: the run inherits no `BOUNDED_EXEC_CPU_POOL`, and the gateway's control
  socket path does not exist. Only `ENOENT` proves the gateway absent; a socket that exists but does not answer may be a
  live, saturated gateway, and a run budgeting itself beside it would count the host twice. The pool is the sub-ledger
  above with one token per core (`availableParallelism`), published as `BOUNDED_EXEC_CPU_POOL`,
  `BOUNDED_EXEC_CPU_TOKENS` and its fifo as `CARGO_MAKEFLAGS`/`MAKEFLAGS` in the environment every task inherits, so
  `bounded-exec` takes from it as from any parent pool — FIFO, the head taking `min(want, free)` once one token is free,
  every grant whole — and an `nx:run-commands` Cargo reads the same fifo. Nx still starts the run's targets side by
  side; their runners now queue for the run's cores instead of each taking the host, and a queued runner's bounds start
  at its grant ("The bounds start at the grant"). The plugin's `run-cpu-pool.test.ts` holds that shape over a real
  socket: two sweeps each asking 31 of a 4-token run pool, the first granted 4 and the second queued until the first
  returned them. The run's `postTasksExecution` closes the pool; a run that dies first takes it with its process. Its
  scope is one Nx run, not the host: two concurrent runs on one host without a gateway hold one budget each. A runner
  that reads no jobserver and asks nobody — an `nx:run-commands` Stryker — is not sized by it, which is why such runners
  run under `bounded-exec` ("A runner no jobserver sizes"). A pool that cannot be made is said on stderr and leaves the
  run unbudgeted, as before.
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
  can deadlock on each other's tokens. A grant never grows (2026-10-19, Danny: "Ok delete"): it runs with the count it
  was granted until its connection closes, and a returned token goes only to a waiting request.

- **Gate admission: oldest gate job first, ahead of background, never starving it** (2026-10-17, Danny tg#6693: "The
  gate class is served FIFO at highest priority over all other execs … When gate runs it needs more tickets than
  background but not fully starve background"; tg#7019, of two concurrent lands: "first should be full priority and
  second almost background until first completes"). A jobserver root request carries a typed
  `class: "background" | "gate"`; omission is background, and any other class is `invalid-request` (a resident asks with
  `resident-pool`, below). A gate also carries its trusted `job.repoId`, workspace incarnation and job id, and requests
  its private pool. Anonymous gate requests are refused. The ledger ranks the root and every task its pool relays by
  both repository and the root's admission ticket, not the child's arrival. **Gates are serialized per repository and
  parallel across repositories** (tg#7385): while a gate job or any of its relayed tasks holds tokens, no other gate job
  for that repository may hold any, including a floor token. With no holder, the oldest queued job of that repository is
  its head. Cancellation removes a queued claimant; a root ending does not release the repository until its last held
  child releases too. An earlier ticket enqueued late cannot preempt a running job. These heads are derived from the
  existing holders and waiters, not a second admission registry. Among eligible repository heads, the oldest job's next
  request takes `min(want, free)`, in that job's request order, ahead of queued background. A background checkout or a
  younger head **of another repository** holding nothing gets one floor token first, oldest request first, while work
  behind the job being served still holds less than half of N. The younger repository's orchestrator can therefore start
  while the older one's tasks take every returned token they want. A behind-head gate in the same repository stays
  queued even beside free tokens; it cannot enter through the background selector. Background still uses its own free
  room when no eligible gate wants tokens. Running grants are never preempted or resized. Conservation remains
  `free + Σ held = N`.
- **A land is a gate job, and its tasks are gate-class requests.** The selecting authority is the land queue holder:
  `~/Dev/.land-lock.sh` runs main's `tooling/land-host.sh`, the only path that gates a tree before `cowshed land`
  fast-forwards to it, and it runs that gate as `cowshed exec <workspace> --class gate --host -- …`. The trusted route
  alone grants the class (class-not-permitted for a worker). The job's own grant is the one token its orchestrator uses;
  its pool relays every task's ask, from the host or from inside the sandbox, as a gate-class request for the
  workspace's checkout, ranked by the job's own admission. A land therefore takes returned tokens at every task, in the
  order its tasks ask, and is never frozen at the width of its first grant. The repository's land turn ("Land turns")
  protects the landing turn; the gateway independently enforces one token-holding gate job per repository. Lands of two
  repositories are served oldest gate job first: the younger land's orchestrator starts on its floor token, the older
  land's tasks take every returned token they want, the younger land's tasks take what the older one does not ask for,
  and the younger takes the older one's place when it completes. Neither is preempted: a running task of either keeps
  its grant. The jobs the land's own cowshed steps start are gate-class work too, since the land waits on each of them:
  the rebase carry's task-hash computation, which the lander asks for with `cowshed rebase --gate`, `cowshed land`'s Nx
  cache initialization (16_build_volumes.md, "Carry" and "Rebase carry"), and each `cowshed land --check` command
  (02_workspaces.md, "`cowshed land`" step 2). Measured 2026-10-19 with the first two at background class: a land's
  task-hash job waited 21 s in the background queue with no other gate running, and in one repository the same
  computation took 48 s on one land and 297 s on the next, while that repository's whole land queue waited behind it.
  Measured 2026-10-20 with `--check` at background class, on a three-token ledger (the macOS arm64 CI runner), whose
  background room is one token ("Room for a gate"): one long-lived ordinary background job held that token, and the
  land's check waited at `check-wait` for the job to end, while the job was waiting for the land, so the land never
  reached its adoption. That is the starvation the room exists to remove, at the smallest host: on a host of three
  tokens or fewer, one ordinary long-lived background job keeps every other background exec waiting until it ends. A
  process that stays up idle between work is a resident ("A resident holds no token") and holds none; a land's own jobs
  never contest the background room at all.
- **A job lives with its requester** (2026-10-18, Danny tg#7386: "cowshed gateway is supposed to background every
  landing queued and not cause load that slows down the head of the queue"). A foreground job's only consumer is the
  process that asked for it, so when that process is gone the job stops and its tokens return. A gate job's verdict
  moves main only through the `cowshed land` its lander runs next. A gate whose lander is gone — killed, or stopped
  under the one-lander rule — authorizes nothing, yet it keeps its admission, so it stays the oldest gate job and takes
  every returned token ahead of the land that holds the land turn. Measured 2026-10-18: a land of another repository
  stopped while gating left `nx run-many --parallel=18` running at gate class with no lander and that repository's land
  lock free; it headed `gateQueue` and held 13 of 18 tokens against the locked smoothbricks land. The same holds for
  background work. Measured 2026-10-19: an agent's tool timeout killed three `cowshed exec` clients, and their sandboxed
  jobs ran on under the supervisor for 1h04, 61 and 33 minutes, holding a quarter of the host and reading as work. So
  every foreground `cowshed exec`, whatever its class, holds its job for as long as the exec process lives (11_shell.md,
  `hold`): when that process ends before its job — `SIGKILL` included, since the kernel closes its connection — the
  supervisor kills the job, and its tokens (and a gate's place in the gate order) pass on at once. `--timeout` releases
  the hold before it leaves the job running, and `--background` takes none: a caller who asks for an unattended job
  keeps it. A job that cannot be held is killed and its exec fails, rather than run for nobody.
- **Room for a gate, since no grant shrinks** (2026-10-19, Danny: "avoid dumb waiting"). A running grant cannot be
  shrunk: Cargo's implicit make token and libtest/nextest threads sized from the original grant do not return FIFO
  bytes. The gateway does not resize a running FIFO, kill peer work, or use `SIGSTOP`/`SIGCONT`: wall and idle bounds
  would keep advancing during a freeze, and a killed nextest run cannot resume, so its owner would rerun it whole and
  read a failure it did not cause. Fairness that acts only when a grant is made cannot reach a grant made while nobody
  waited, so the grant itself leaves the room: **background-class work as a whole never holds more than `⌊N/2⌋`
  tokens**, and at least one (`background_limit`, 9 of 18). A background request takes only its room, the free tokens
  within that limit, and the floor token "Gate admission" grants background already keeps it under half. This is the
  host ledger's rule: a parent's lent pool ("Nested runners") serves a grant the host already made, no gate asks it, and
  its children may hold all of it. The other `⌈N/2⌉` host tokens are therefore free whenever no gate-class work holds
  them, so a gate's first request is granted at least `min(want, ⌈N/2⌉)` in the ledger transition of its own ask: it
  waits for no departure, whatever background holds and for however long. Its later wants take returned tokens ahead of
  background as above. Measured 2026-10-19: a background `cargo nextest run` granted 17 of 18 tokens while it was the
  only waiter sat at 0% CPU for 50 minutes, and the land gate that arrived after it ran on its one token for over 35
  minutes against a usual 10 to 20. A cap on a lone grant alone would not close this, since two background grants made
  one after another fill the host again; a floor smaller than half would leave the gate running on that floor for the
  whole of the longest background run. Once gate-class work — the gate's own tasks or an older land's — holds the gate
  half, a gate request waits for a returned token like any other; gate priority then promises admission order, not a
  fixed test duration. The supervisor's separate nice placement below ("Bands") orders the classes by lowering nice,
  never by raising one above default.
- **No admission deadline, for any class** (Danny tg#6679, tg#6693). A gate is the highest-priority work there is, so it
  is never failed for waiting in the order that serves it: a gate jobserver request, like a background one, waits until
  it is granted or its client closes the connection, which leaves the queue. The operator request is
  `cowshed exec <workspace> --class gate --host --want N -- COMMAND [ARGS...]`, and it sends
  `{"op":"jobserver","class":"gate","want":<n>,"checkout":"<root>","command":"…","pool":true,"job":{"repoId":"<owner/repo>","workspaceIncarnation":"<incarnation>","jobId":<id>}}`.
  A gate asks only the host repository queue: `parentPool` is refused for gates, and the supervisor does not forward an
  inherited pool on gate admission. Otherwise a parent's token could start a second gate outside the repository order. A
  waiting request holds nothing, so its wait costs nobody; the live `cpu-budget` ledger names what it waits behind.
  Measured with the 120000 ms bound this replaced: a younger land's gate was refused after 120003 ms while an older land
  held 13 of 18 tokens, killing the land for waiting in the very order the ledger serves. An unknown class is an invalid
  request, and so is a `waitMs` field: it is no longer part of the request. Running task wall/idle bounds still start at
  their grant.
- **The task server owns the live grant and nice.** Work enters the existing `cowshed exec` / workspace supervisor, not
  a gate-host or taskpolicy wrapper. Its typed request carries class, want and scope. Only a trusted
  coordinator/operator may request Gate or HostGate; a worker request is refused with typed class-not-permitted before
  job or spawn effects. Ordinary commands default Background and Workspace scope. A request that names no want asks for
  its class's own: background and interactive work the background share `⌊N/2⌋` ("Room for a gate"), all that their
  class holds at once, a gate the one token its orchestrator uses ("A land is a gate job"), a resident none; a named
  want is taken as named. An ordinary command's caller cannot know how many runners it starts, and what it starts draws
  on its grant ("Work a holder starts draws on its grant"). Measured 2026-10-21 with a default of one token: an
  `nx run-many` under `cowshed exec` handed that one token to its bounded-exec runners in turn, which waited 261, 473,
  664 and 887 s for it, and the run took 21m19s against 5m54s on the host route. `JobScheduling::requested` resolves the
  want, the validation both surfaces share (07_api.md, "CLI and API exec parity"), from the share the supervisor's CPU
  admission reports; a keyed request binds its want as named, so an unnamed want is the same request whatever share each
  supervisor resolves it to (07_api.md, "Keyed admission and restart attachment"); a requester that awaits the result
  asks Interactive ("Bands"), which a worker may ask for, since it buys a band and no token; a resident (below) is
  Resident or InteractiveResident, which a worker may ask for, and names no want. The supervisor records Queued, asks
  the gateway off its actor path and spawns only after a real grant. **The exec answers at that admission, never at the
  grant** (11_shell.md, "Server-owned CPU admission and nice placement"): a job waiting in the queue has its id, reads
  `queued` with its class and no `cpuTokens`, and is cancellable, since a request that waits for no deadline must stay
  reachable for as long as it waits. Measured 2026-10-19 with the answer at the grant: a `cowshed exec --background` on
  a loaded host returned 25 s later, so the executor's sequential admission of 100 held jobs stopped at the first
  waiting one, which had no id to report or cancel. CPU admission uses the control socket of the controller's validated
  host storage, the same store/gateway.sock authority its gateway reconciliation uses, never an unrelated ambient store.
  A queued kill withdraws the request without spawning. Missing/refused admission never causes an unbudgeted server
  spawn. The supervisor retains the allocation until the owned job concludes. The gateway's existing CpuBudget ledger
  serves that exact allocation on a private workspace-scoped CPU-pool endpoint; Nx bounded-exec and Cargo descendants
  borrow only from it, with sibling sums bounded by its actual count, or, for a gate job, through it as gate-class host
  requests. The root FIFO is not offered alongside the pool: that would spend the same tokens twice. Its canonical
  endpoint is `<store>/run/cpu/<base64url-incarnation>/<opaque-endpoint>`: the unpadded 22-byte incarnation component
  losslessly encodes all 128 bits of the validated 32-lowercase-hex workspace incarnation, never a digest or truncated
  prefix. The opaque endpoint is an independent 22-byte unpadded base64url UUID. The gateway, sandbox builder and grant
  validator derive this one layout through the same helper; there is no runtime alias or fallback. This adds 54 bytes to
  the store prefix, giving a 98-byte socket path for the existing 44-byte fixture store, within Darwin's 103-byte usable
  limit. Arbitrarily long store paths do not acquire a new short-link lifecycle. A bare count/class is no gate role. The
  owned command is placed at its class's nice ("Bands"), never the shared warm host or supervisor. No class is raised
  above default, which requires root; the order lowers the other classes' nice instead. Nice is applied and read back
  before command execution, using the held-child fence on macOS and a private report pipe on Linux. Refusal is typed
  PriorityFailure, not an unobserved run. Class, scope, actual tokens and observed nice are recorded; nice remains
  absent when never observed. A HostGate is a coordinator-only argv one-shot on the host, outside ExecutedChild,
  preserving the host authority real-APFS work needs; it asks the ledger as its own class, so host work that is not a
  gate never enters the gate order; no ordinary workspace sandbox is loosened. Controller discovery uses only
  `cargo locate-project` and `cargo metadata --no-deps --offline`, which retain their noncompiling exemption: no grant,
  class nice or job record is claimed for that discovery.
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
  it starts and returns them when it ends. Its command receives that pool and `BOUNDED_EXEC_CPU_TOKENS=0`, and no fifo:
  it holds no token for a Cargo to read, so a Cargo it starts runs unconfigured. A pool with a count of 0 is a valid
  descriptor (a bare 0 without a pool still proves nothing), and Nx bounded-exec asks it as any parent pool. The
  resident runs at background nice. **An interactive resident** (2026-10-19, Danny tg#6693 with the paragraph above) is
  a resident whose requests a person or model waits on — an embedding executor's feedback Nx worker, whose focused
  checks a session awaits: it is admitted as class InteractiveResident and differs from a resident in its nice alone,
  running at nice 0, the interactive band. It writes the same `resident-pool` request, holds no token, names no want,
  and its pool relays each child's request at background class exactly as a resident's does, so neither residency buys
  tokens with its band; a task program it starts inherits nice 0, as a resident's inherits +10. The connection is the
  pool's life: the resident's exit closes it. A different token class would leave the resident in the ledger holding
  something; exempting it from the budget would leave the requests it runs unbudgeted; acquiring per request through a
  relay pool is the gate job's existing mechanism and budgets exactly the work.
  - **A resident's start holds one token** (2026-10-20). A resident is idle between requests once it runs, but its start
    is not: under a workspace `.envrc` its shell activates first, and a cold devenv evaluation is minutes of CPU
    (measured 2026-10-20: a shed's Nx daemon keeper evaluated its shell for 197 s while holding nothing, beside other
    work the budget had admitted, so the budget did not bound it). Before its `resident-pool` request, a resident's
    supervisor therefore asks one background token for the resident's checkout with an activation stake,
    `{"op":"jobserver","want":1,"checkout":"<workspace root>","command":"<command>","class":"background","pool":false}`,
    which names no job and asks for no pool: it waits as any background request does, and the gateway answers
    `tokens: 1` and a fifo nobody reads. The resident asks its pool only once the stake is held, holds the stake while
    its shell activates, and closes it when its command starts, however its spawn reports that: a warm host's command
    with the job's `Started` event once the host has activated, a one-shot command when its spawn returns its process.
    From then on it holds no token, as above. A one-shot child that enters a workspace shell runs that activation inside
    its own process, after the spawn has returned, so its activation is not under the stake; only a supervisor without
    an exec host program spawns one so. A stake that is refused fails the job as a refused admission does; a job that
    ends before its command starts returns the stake with its grant. The same stake bounds a shell pool's spare
    (11_shell.md, "One spare and a SIEVE cache"). The ledger knows no partial return, so the stake is its own one-token
    grant rather than a count the resident's pool would later give back.
  - **A kept daemon a land waits on starts as the land's gate task** (2026-10-21). A start under a holder names the
    holder's pool as the stake's `parentPool`, and the stake waits on that pool and the host ledger at once ("Work a
    holder starts draws on its grant"). The keeper's start (04_sandbox.md) has such a holder while its shed's supervisor
    runs a gate job, the oldest one if several: a land's gate, or its rebase's task-hash job. A land resets the shed's
    daemon and waits for its successor before its first gate, so that start is work the land waits on, and a land's own
    work never contests the background room ("A land is a gate job"). The stake drawn on the gate's pool is asked of the
    host at the gate's rank, as the gate's tasks are. A shed running no gate job asks the host alone, as background.
    Measured 2026-10-21: a land's rebase ran its task-hash job in an idle shed, and the keeper asked its start at once.
    Its background stake waited 324 s behind another repository's land and background tests, while the land waited 60 s
    for the daemon and gave up. Over the same gateway log, 361 of 510 kept daemons' stakes waited 0 s, the median was 0
    s, p90 34 s, p99 208 s, the longest 852 s, and 31 (6.1%) waited more than 60 s: a background stake waits as long as
    background's room stays full, so no fixed wait bounds it. A land that runs its daemon reset and that wait as one
    gate job waits for the daemon with no deadline.
  - **CPU pool retirement.** Pool retirement unlinks its endpoint before closing its listener. It drains the kernel
    accept queue, not merely the runtime's readiness cache, and answers queued and already accepted connections with
    `{"ok":false,"code":"pool-retired","error":"the CPU pool owner's lease ended"}` followed by EOF. A new connection
    after unlink fails at path lookup; an already queued connection completes and hears the named refusal, rather than
    being abandoned during its handshake. The retirement log names the pool and counts queued refusals and accepted
    connections ended. Explicit close and owner drop use this same drain; neither aborts the listener task.
- **Work-conserving within repository admission and the gate's room.** Among eligible requests, with nobody else
  waiting, a gate-class grant is `min(want, free)`, so a lone gate runner may hold all N tokens, because a gate's wall
  time is what the budget buys. A background grant is `min(want, room)`, the room being the free tokens within the
  background limit: that limit is the one cap, and it is the trade. Up to `⌈N/2⌉` tokens (9 of 18) sit idle exactly
  while no gate-class work holds or wants them and background work wants more than `⌊N/2⌋`; a gate that starts uses them
  at once, and with no more than `⌊N/2⌋` wanted by background nothing idles. The idle half buys a gate that starts wide
  at once instead of waiting out the longest background run, which nothing can shorten once granted. A lone waiter also
  takes what its room holds when that is below its fair share: on an 18-token host, a background runner holding 6 does
  not make the sole background newcomer wait instead of taking the 3 left of the limit. Ordinary checkout fairness (the
  share rule above) still bounds a grant while another ordinary checkout waits. Repository serialization may leave
  tokens free when only a behind-head gate wants them; starting that gate is not useful concurrency.
- **Bands** (2026-10-17, Danny tg#6693: "The gate class is served FIFO at highest priority over all other execs. The
  interactive class has priority over gate and regular exec. Priority is handled by OS niceness. When gate runs it needs
  more tickets than background but not fully starve background"). A job's class fixes, at the job's birth, both its
  place in the token order and the band the host schedules it in. This table is the one statement of both; every other
  section names it.

  | Class         | What runs in it                                                                                                                                   | CPU tokens                                                                                                                                    | Band           | Nice |
  | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ---- |
  | `interactive` | work a person or model is waiting on, declared by the requester that awaits its result, and an interactive resident ("A resident holds no token") | behind gate, as background: checkout fairness and the floor token; an interactive resident holds none and its pool relays at background class | **foreground** | 0    |
  | `gate`        | a land's gate job, every task its pool relays and the land turn's holder; the trusted route only                                                  | first: the oldest gate job FIFO, ahead of every other class ("Gate admission")                                                                | **normal**     | +5   |
  | `background`  | every other exec, the default, a resident, and each request either resident's pool relays                                                         | checkout fairness behind gate within half the host ("Room for a gate"), never starved: the floor token; a resident holds none                 | **background** | +10  |

  Tokens decide which work runs at all; nice decides whose runnable threads win a core when more are runnable than there
  are cores. Those are the ruling's two priorities: gate work takes returned tokens first and, while it runs, more of
  them than the rest without starving it, and on a contended core an interactive thread runs before a gate thread and a
  gate thread before a background one. Nice is never negative, because raising a priority above the supervisor's own
  takes a privilege no supervisor holds, so the order is made by lowering it: interactive at the supervisor's own nice,
  gate five below it, background ten below it. A band never admits, queues or sizes a job — the token ledger alone does
  that — and an idle host leaves every band's work at full speed. A class is fixed at the job's birth and its band with
  it: no live transition moves a running job between bands. An interactive job asks the gateway's ledger as background,
  whose tokens it shares; `interactive` is the supervisor's placement and the task server's class, never a third ledger
  class. A requester names its job's class in the exec request itself — `ExecClass`, one of `background`, `interactive`,
  `gate`, `resident` or `interactive-resident` (a resident, "A resident holds no token" above, holds no token and runs
  at background nice; an interactive resident is the same resident at nice 0) — and the CLI's `cowshed exec --class` and
  the API's `class` field are that one typed value (07_api.md, "CLI and API exec parity"), so an embedding executor
  declares a class exactly as the CLI does. The ledger's wire class (`JobserverClass`) is `background` or `gate` only:
  the supervisor sends interactive as background and either resident as `resident-pool`, so neither reaches the ledger
  as a class of its own.

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
  means nothing is happening while nothing is expected to happen). Work a command starts draws on the command's pool
  ("Work a holder starts draws on its grant"): a cowshed exec whose requester holds a live `BOUNDED_EXEC_CPU_POOL` waits
  on that pool and on the host at once, and the first grant serves it. A descendant can still wait — for its requester's
  pool and the host together, or at the host alone when its requester holds no pool. A descendant that knows its work
  waits tells the run on the pool it inherited: it writes `{"op":"host-wait"}` on `BOUNDED_EXEC_CPU_POOL`, is answered
  `{"ok":true,"wait":"held"}` at once, and holds the connection for as long as the work waits. Every pool of the run,
  its sub-pools included, counts to the run. While at least one is held the run's idle clock is stopped: the time left
  of its no-progress interval is kept, nothing is spent, and stderr says `bounded-exec: idle clock stopped`. When the
  last closes — the token arrived, the work was withdrawn, or the descendant died, which closes its socket — the clock
  runs on from what was left and stderr says `bounded-exec: idle clock restarted` with the queued time. A finished test
  still sets the interval back to `idleTimeoutMs`. An expiry names the queued time it did not count. Only a held wait
  stops the clock: a run whose descendants say nothing keeps the bound exactly as before. `timeoutMs`, a ceiling, is
  never stopped. A wait holds no token, so it changes neither the pool's count nor any grant.
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
  since its grant. The age is the lease's, never the process's: a lease is one connection granted once, and a process
  that asks again holds a new lease with its own age; the ledger never scans processes, and `ps -p <pid>` gives the
  process's age. Measured 2026-10-18: a ledger that named only `land-test-hang: held 15, running 1` read as an orphan,
  and a live 34-minute `cargo test …` another unit was waiting on was killed by hand; its holder line would have said
  what it was and that it was live. `cowshed gateway status` shows this ledger to the operator in the order it serves —
  its count with the background limit (`gateway cpu budget: held <h>/<N> tokens, background at most <⌊N/2⌋>`), the head
  gate job, the gate jobs behind it, the gate queue, then background, each running gate job and background stake
  followed by its leases — in its human lines, and verbatim as `cpuBudget` in `--json` (06_cli.md; 07_api.md
  `GatewayStatus`), whose limit is `⌊total/2⌋`, at least one, so whether queued lands wait behind the head of the queue
  without loading it, why background waits beside free tokens, and who holds every token since when, is visible rather
  than inferred; a daemon that does not answer `cpu-budget` is reported as such, never as an empty ledger.
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
- **Spans.** The executor prints
  `cowshed: cpu-tokens wait done elapsed=<ms>ms status=ok tokens=<g>/<want> runner=<kind>` before the command's own
  output (`status=err … runs unbudgeted: <reason>` without a grant), so a slow or failing task says how long it waited
  and how many threads it ran with. Gateway grant and scheduling records are unconditional stderr diagnostics, including
  in the installed LaunchAgent: no `COWSHED_TIMING` switch is needed. Each line starts `cowshed: timing`:
  - `lease-grant scheduler=<name> elapsed_ns=<n>` measures from the held request's queued-answer boundary through the
    successful granted-response write, including the wait and jobserver FIFO/pool preparation. It excludes connection
    establishment and request parsing; client-observed latency is a separate measurement.
  - `cpu-tokens grant ticket=<id> tokens=<g> want=<n> waited_ns=<n>` records the ledger's admission-to-selection wait,
    with checkout, peer pid and command. An immediately selected request has zero ledger wait, not zero wire latency.
    Gate grants also name `gate_repo`, `gate_job`, `head_repo` and `head_job`: the granted job and the global oldest
    admitted gate, including holders with no task currently queued. The two repositories may differ; two jobs of the
    same repository may not hold tokens together. Background grants have no gate identity.
  - `cpu-tokens queue-step cause=ask|release ticket=<id> elapsed_ns=<n> grants=<n>` measures one actor transition
    through grant dispatch. `cpu-tokens handoff ticket=<released-id> elapsed_ns=<n> grants=<n>` measures stake release
    through dispatch when that release selects waiters, including time waiting in the actor's inbox. Neither includes
    its own diagnostic write. All grants of a transition are dispatched before its records are written. Numeric
    durations are nanoseconds, so p50/p99 require no parsing of human duration suffixes. Status reads emit no scheduling
    step. The existing opt-in spans remain available for other lifecycle detail.
- **Cargo reads its holder's jobserver** (2026-10-19, Danny: why need cargo wrappers, rather than configure it properly;
  tg#6787, tg#6998, tg#7002). A CPU grant reaches the processes its holder starts as the configuration Cargo and its
  test runners already read; no `cargo` on `PATH` intercepts anything, and nothing installed answers to the name ("The
  installed cowshed"). Every CPU pool — the gateway's for a `cowshed exec` job and each child pool it serves,
  `bounded-exec`'s, the Nx run's — is a GNU make jobserver fifo beside its control socket:
  - **The fifo is the pool's free count.** A pool of `g` holds `g − 1` token bytes in its fifo for the grant's whole
    life; the holder's own command holds the implicit first token, as make's protocol has it. A grant never grows: a
    completed command returns its tokens to waiters, and nothing else adds to the FIFO after the grant. A child asking
    the pool's socket takes its grant from that same count: the fifo's bytes first, then the implicit token while no
    child holds it, FIFO among the socket's waiters, the head taking `min(want, free)` once one is free, every grant
    whole; its close writes the bytes back and frees the implicit token. A Cargo reading the fifo and a nested runner
    holding a grant therefore never count one token twice. Lending the implicit token is what lets a one-token pool
    admit the nested job its command waits on rather than deadlock on an empty fifo. A gate's relay pool lends nothing:
    its fifo holds the gate's own tokens for the gate's own command, and each child asks the host ("Gate admission"). A
    resident holds no token and has no fifo ("A resident holds no token").
  - **The holder exports it.** The command a holder starts receives `CARGO_MAKEFLAGS` and `MAKEFLAGS` set to
    `-j --jobserver-auth=fifo:<path>`, beside `BOUNDED_EXEC_CPU_POOL` and `BOUNDED_EXEC_CPU_TOKENS=g`, and
    `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` set to `g` where it named neither: libtest and nextest test threads
    take no jobserver token, so without them a `cargo test` holding `g` tokens still runs one test thread per core. A
    `--test-threads` count the command names outranks them, as a flag outranks the environment. The supervisor sets
    these for a job (`CpuPlacement`), over a caller's own `CARGO_MAKEFLAGS`, `MAKEFLAGS` and `MFLAGS`, which it never
    passes; `bounded-exec` sets them for every runner it starts ("Sized to the grant"), and a target's `options.env`
    cannot name them. Cargo, every build script and every nested `cargo` a test starts inherit them and run rustc only
    on tokens read from that fifo; `cargo metadata` and every query read none, so nothing needs to tell a build from a
    query.
  - **No holder, no configuration.** A Cargo no holder started — an agent's in a shed, a host shell's, rust-analyzer's,
    a resident's — receives none of these and runs exactly as Cargo does unconfigured, at the host's full width: the
    approved behaviour since the `cargo` launcher's deletion, unchanged by the deletion of growth. A parent make's own
    jobserver still applies, by Cargo's own rule. Such a Cargo holds no grant, so nothing it starts can wait behind one.
  - **A pool's end.** A pool's close drains its fifo and unlinks it. A Cargo that outlives it keeps its open descriptor
    and runs over budget until it ends, as a runner does across a gateway restart; a Cargo started after it finds no
    fifo and runs unconfigured.
  - **A fifo the command cannot open is reported, never silent.** A command that cannot open its grant's fifo — a
    sandbox profile that does not admit it, a fifo gone — runs, and every Cargo it starts runs without the jobserver,
    outside the CPU budget. Cargo says so itself on stderr,
    `failed to connect to jobserver from environment variable CARGO_MAKEFLAGS=… (os error N)`, naming the fifo and the
    errno, and nothing of cowshed's filters that line. A warm exec host, the first process inside the sandbox, opens the
    fifo read-write and non-blocking once before each command and on failure writes
    `cowshed: the job's CPU jobserver fifo <path> cannot be opened in the sandbox (<error>); Cargo will run without it, outside the CPU budget`
    on the job's stderr; the command still runs. A one-shot child (a workspace without an exec host) runs no cowshed
    code inside the sandbox before its command, so Cargo's own line is its report. Until 2026-10-20 every sandboxed
    Cargo met this: the profile emitted the pool directory's rules before the `/private/cowshed` deny, which re-denied
    the fifo's open with EPERM (04_sandbox.md).

  Held over the gateway's real socket with no Cargo (`cowshed-gateway`'s `tests/jobserver_fifo.rs`): a job grant of `g`
  names a fifo beside its pool socket holding `g − 1` bytes; its socket lends the fifo's bytes first and the holder's
  token last, a child waits while a reader standing in for Cargo holds the bytes and is granted when they are written
  back, a closed child's tokens reach the next, and the host ledger holds `g` alone throughout. What a job's command
  receives is `CpuPlacement::environment`'s unit test. That the sandbox opens the fifo and the pool socket and nothing
  else in the pool directory is `cowshed-core`'s `devenv_runtime_dir.rs` host-controller probe under the real profile,
  and that a fifo it refuses is reported while the command runs is `cowshed-shell`'s `shell_pool.rs`. How Cargo reads a
  jobserver is Cargo's own to test; an end-to-end proof with real Cargo belongs in a nightly, not the gate.

- **A runner no jobserver sizes.** A grant bounds what reads its fifo and its test variables, nothing else.
  `cargo mutants` hands every cargo it starts a jobserver of its own, `--jobserver-tasks` tokens, one per core by
  default, in place of its holder's fifo; Stryker, Vitest and `go test` start their own workers and read no make
  jobserver at all. Such a runner therefore runs under `bounded-exec`, whose runner kinds read and rewrite its own flag
  from the count ("Who asks", "Sized to the grant"): the plugin's inferred `mutation` target runs `cargo mutants` there,
  and a repository's declared Stryker, Vitest and `go test` targets do the same.
- **The jobserver fifo.** A job's fifo lives beside its pool socket in the workspace's pool directory, which the sandbox
  opens for reading and writing fifos and nothing else (04_sandbox.md); a `jobserver` grant without a pool has its fifo
  in the gateway's private runtime directory. Either is mode `0600` and owned by the client's user. The gateway opens it
  `O_RDWR` and keeps that descriptor until the drain, so the bytes survive while no client has it open, and writes
  `g − 1` token bytes into it. At startup the gateway removes every fifo left in its runtime directory, since a restart
  forgets every grant. When the grant's connection closes — the job ended, or its client was killed, `SIGKILL` included
  — the gateway closes the pool, drains the bytes left in the fifo, unlinks it, and returns all `g` tokens to the
  ledger, so tokens a killed process read and never wrote back are credited too.

Tests: `cowshed-gateway`'s `cpu_budget` ledger unit tests (fairness, share, starvation, leave, the background floor, the
background limit — a lone background request granted half the host, a second one finding no room, and a gate arriving 50
minutes later granted the other half with a zero wait, in the ledger transition of its own ask — an older land served
before a younger land that asked first and both before a feature build that asked before either) and
`tests/cpu_budget.rs` over a real socket (`SIGKILL` of a holding client returns its tokens to the waiter at once;
checkouts share background's half; a gate job and its task granted the free half while a lone background grant still
holds its own; refusals, among them a `jobserver` request and a `cpu-pool` ask naming `grow`, each an `invalid-request`;
a holder at background's whole room whose child, queued at the saturated host, is granted at once when asked with the
holder's pool, its host ask withdrawn and the host ledger unchanged, while a pool that answers and refuses refuses its
child and an unreachable one leaves it to the host; a one-token holder whose token a sibling holds has its exec granted
by the host when the host has room, and the pool's next child is lent the token at once; with the host full and the pool
busy, the exec waits in the ledger and is granted by whichever frees first, a pool grant leaving the ledger's queue);
the plugin's `cpu-tokens.test.ts` (each runner's `want` and sizing, a killed holder closing its grant) and
`executor.test.ts` (a granted run sized and released, a refused one run as asked, a target's jobserver variables ignored
for the run's own fifo); `cpu-pool.test.ts` (a request naming `grow` refused like any other unknown field) and the
gateway's lent-pool test (a pool's fifo holds `g − 1` bytes, a child is lent the implicit token while a reader holds the
bytes, the next child waits until the reader writes them back, a child's close puts them back);
`tests/jobserver_fifo.rs` (above); and the band tests: each class runs in its own band at its own nice whatever the
host's state — interactive above gate above background — and gate work is served returned tokens ahead of interactive
and background work.

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
- **Decisions are logged facts.** Every selection and terminal fact is written unconditionally to stderr as
  `cowshed: timing task-server select|terminal`, with unit, owner and incarnation. A selection includes its ticket,
  attempt, numeric `waited_ns` from admission to selection, band, want and leases. `task-queue` reports the live fold;
  nothing in the scheduler is a second authority over the plan.

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
2. Take the workspace listeners an install carried across the restart ("An install never refuses a client connection"),
   before anything binds. Then count the recorded projects and list the workspace supervisors still serving from before
   this daemon, then serve: the control socket and the supervisor manager answer from the first moment. Status answers
   throughout, and so do the audit, mirror, and simulator requests, none of which reads a workspace mount.
3. Eagerly heal every recorded project's mounts: every project's main attached and mounted at its checkout path first,
   then every other workspace under the mount root (02_workspaces.md). Each step is a lifecycle span of its own —
   `startup-heal discover`, `startup-heal open <repo>`, `startup-heal mount <repo>/<workspace>` — so a slow pass reads
   as the step that spent the time.
4. Restore every attached workspace's session into the gateway from what step 3 mounted. A session whose endpoint an
   install carried serves on that listener, connections queued on it included; any other binds its endpoint. Once every
   session is restored, each carried listener no session claimed is closed, so its queued connections are reset rather
   than left waiting for nobody.

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
