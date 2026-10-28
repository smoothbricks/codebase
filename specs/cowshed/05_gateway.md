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
first `direnv export json` and cold index reads many times over their unclamped cost.

The gateway's generated plist sets `SoftResourceLimits.NumberOfFiles` to the finite kernel-sized value `245760` and
`HardResourceLimits.NumberOfFiles` to Darwin's `RLIM_INFINITY`. A finite soft limit keeps `sysconf(_SC_OPEN_MAX)` useful
to tools that close inherited descriptors. Workspace supervisors, warm shell hosts, and jobs inherit these limits
naturally; child-spawn code does not change them. Kernel-wide descriptor capacity still applies. `cowshed setup`
reconciles the generated plist as well as the installed cowshed (below), including on hosts whose installed copy already
matches.

The host-only control plane is host-netns `127.0.0.1:7644` (override `COWSHED_GATEWAY_PORT`) plus the store's
`<store>/gateway.sock`, production's `/private/cowshed/store/gateway.sock`, for status, audit tail, and coordinator
verbs; every client derives the socket from the store it was given, so a test namespace's gateway binds and answers only
its own (01_storage.md, "Host namespace"). Neither host endpoint is reachable from a workspace, and the sandbox baseline
denies both. Linux separately reuses the numeric address `127.0.0.1:7644` inside each private netns for its data-plane
connector; namespace separation makes it a different listener. Data-plane topology is platform-specific:

- **macOS — port block.** Every workspace, main included, gets a contiguous block of ports from the reserved range
  32768–49151, the union of the host partition production allocates from and the fixture partition test namespaces
  allocate from, recorded as `portBlock {base, size}` (04_sandbox.md: a power-of-two size, a base aligned to it; new
  workspaces get 64 ports, and a live block grows only through a `service_ports` grant; one that moves keeps its old
  block reserved as `retainedPortBlocks`, which carries no gateway listener). `portBlock` is allocated at fork (adopt
  for main), and present only in macOS grant files. The gateway binds `base`; `base+1 … base+size-1` are workspace
  service ports. A session carries its block's recorded size, and the gateway validates the endpoint against it. A store
  allocates from its namespace's partition: production from the host partition (32768–45055), a test namespace's fixture
  store from the fixture partition (45056–49151), so no test block is ever one production holds. Seatbelt permits a
  workspace to connect only to its current and retained blocks. The destination `base` listener is the primary,
  kernel-enforced workspace identity. Of two sessions whose blocks share a port, the gateway serves the older
  workspace's and not the newer one's, at startup and in every per-project reconcile, with
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
`Authorization` is the client's own and authenticates nothing. The local Nx cache route below takes no credential at
all: endpoint selection alone names its workspace. The gateway accepts no cookie, query parameter, URL userinfo, or path
token; it strips `Proxy-Authorization` and `Authorization` before any upstream request. It decodes the presented value
to bytes, rejects malformed or wrong-length values, and compares all 32 bytes in constant time. A missing or mismatched
token is the proxy's 407 challenge.

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

Create and fork mint a token. Detach drains the Linux connector and gateway connections, kills the connector cgroup and
unlinks the socket without rotating persistent authority; attach creates the socket and connector before admitting
execs. Remove uninstalls the session with its listener, so the removed workspace's token authenticates nowhere,
including at a later workspace given the freed block, which mints its own. A policy miss after successful endpoint and
token authentication is 403 with a machine-parsable grant hint naming both remedies, the standing one first:
`cowshed grant --project-wide --egress <host>` (every workspace of the project) or `cowshed grant <ws> --egress <host>`
(that workspace). The hint names the bare canonical DNS name or IP address, never a CONNECT authority: the grant host
grammar is shared with grant admission and exec (04_sandbox.md).

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
| `cowshed-gateway-host`   | The program launchd runs for the gateway: a copy of the build, signed ("The gateway host").    |

`cowshed-source`, one level up beside `bin`, records the path the current install came from and its package version.
`install.lock`, beside it, is the install slot's lock (below); it is never removed. Nothing in cowshed's installed `bin`
answers to `cargo`: the project shell owns Cargo routing (11_shell.md, "Canonical mbx environment"). An install removes
only a `cargo` link owned by cowshed, one naming the stable cowshed or a stored cowshed copy. A foreign file or link
there is refused by name and left untouched.

**The Nx cache server is JavaScript run by the shell's `bun`** (the owner's rulings, 2026-10-17 and 2026-10-18). Neither
`@smoothbricks/cowshed` nor `@smoothbricks/nx-cache-server` ships a compiled server or an interpreter. An install copies
the build's server bundle into `bin` the same content-addressed way: for a distribution
(`<package>/dist/bin/<platform>/cowshed`) it is
`node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js`, resolved as Node resolves the dependency
from the package and then each ancestor (an isolated Bun store, a hoisted install and a checkout's workspace link
alike); for a build the slot already holds it is the slot's own `nx-cache-server`. A build with neither is refused by
name before anything changes. The gateway agent's plist sets `PATH` to the directory of the first `bun` on the
installing shell's `PATH`, ahead of launchd's default `/usr/bin:/bin:/usr/sbin:/sbin`, **named as a profile nix or
devenv keeps, never as a `/nix/store` path** (the owner's rulings, 2026-10-18 and 2026-10-19). A plist outlives the
shell that wrote it and nothing roots a store path for it, so after a devenv update and a nix GC a recorded store path
is gone and every backend start fails. The entry is kept as `PATH` spells it, links unresolved — a nix profile's `bin`
such as `/etc/profiles/per-user/<user>/bin`, or a devenv `.devenv/profile/bin` — so the agent follows the profile when
it is re-pointed. A devenv shell puts its own profile on `PATH` as the immutable store path it evaluated
(`DEVENV_PROFILE`); that entry is recorded as `$DEVENV_DOTFILE/profile/bin`, the link devenv re-points on each
activation and keeps alive through the `.devenv/gc` root it registers. Any other `/nix/store` entry is refused, as is a
shell with no `bun`; both refusals name the setup step, and nothing beside cowshed stands in. Setup runs from main's
devenv shell: a shed's profile link disappears with the shed. The daemon starts each project's backend as
`<bun on its PATH> <bin/nx-cache-server> --exit-on-stdin-eof`; a recorded profile that no longer holds `bun` (a removed
checkout, a main not yet mounted) fails that start by name, an outage until the next start finds it.

**Content-addressed copies, one moving name.** A copy is written once: the build's bytes go into an exclusive temporary
file in `bin`, are synced, and are renamed to the copy's name. A copy whose bytes and mode already match is never
rewritten, so whatever runs from it runs exactly that build for as long as it exists. The stable name moves from one
whole build to the next in one rename of a temporary link, so launchd, which restarts a `KeepAlive` service the moment
it exits, never finds a half-written binary or a missing name there. The gateway plist names the gateway host and, as
its child, the stable name, so the plist never changes with the build; an install moves the link and restarts the agent
onto it. Installing a build the stable name already runs changes nothing.

**The gateway host.** launchd runs `bin/cowshed-gateway-host __gateway-host <bin/cowshed> gateway run`. The host spawns
its arguments as its child, passes the child SIGTERM, SIGINT, SIGHUP and SIGQUIT, and exits as the child did — its exit
code, or death by its signal — and does nothing else: it keeps no state and restarts nothing, since launchd's
`KeepAlive` restarts the pair. The child stays in the host's process group, so launchd's teardown of the job reaches it
even when the host itself is killed. The host spawns and never execs, because after an exec the process would carry the
new image's path and signature.

It exists for macOS's first-launch scan: macOS assesses each new executable on its first launch and charges the wait to
the run's responsible process. A launchd job is its own responsible process and everything it starts inherits that, so
the host is the responsible process of the daemon, every workspace supervisor and every workspace job, `--host` gates
included. A Developer Tools entry exempts a responsible process and its descendants, and tccd keys a non-bundle client's
entry to its resolved path and its designated requirement, not to a signing identity alone
(anthropics/claude-code#85586: "tccd records the grant by path, not by code signature"; abhaymettu/tcc-helper: "macOS
keys a TCC grant to the resolved binary path plus its code signature"). The daemon's own path is its content-addressed
copy, new with every build, and an ad-hoc signature's requirement is its cdhash, so neither can hold the entry. The host
holds both still. Its path is fixed, and every install signs it with the user's cowshed code-signing identity: a
self-signed P-256 certificate named `cowshed gateway host` (keyUsage digitalSignature, extended key usage codeSigning)
in cowshed's own keychain `~/Library/Keychains/cowshed-signing.keychain`, made by the first install that finds none,
with no authorization and no prompt. That keychain has the empty password, never locks by time or sleep, and is appended
to the user's keychain search list, the only place `codesign --sign` looks; setup unlocks it with that password before
signing. The key is imported with an access list naming `codesign` and its partition list set with the keychain's
password, so signing raises no dialog either. The login keychain is never written: a key imported there carries
`security`'s partition, `codesign` then raises a keychain dialog (observed on macOS 26.6), and setting its partition
list needs the login password. The empty password protects nothing a same-user process could not already do through
`codesign`. The certificate is never trusted for code signing: trusting it needs an authorization, and neither
`codesign` nor a `certificate leaf` requirement needs trust. The host's designated requirement is
`identifier "dev.cowshed.gateway-host" and certificate leaf = H"<certificate sha1>"`, the same for every build. Two
identities of that name refuse the install by name rather than choose; uninstall leaves the identity in the keychain,
since the Developer Tools entry names it.

An install that moves the stable name to another build rewrites the host: the build is copied into a temporary file in
`bin`, signed there, synced and renamed over the host, so the host is always one whole signed file. Any install or setup
that finds the host absent, or not satisfying that requirement, rewrites it the same way, without a restart: launchd
runs the rewritten file at its next start. Otherwise the host is left as it is. A running host keeps the file it started
from. The one step that completes the exemption is the operator's, one administrator authorization once per host: System
Settings → Privacy & Security → Developer Tools → + →
`~/Library/Application Support/dev.cowshed/bin/cowshed-gateway-host` (⇧⌘G takes the path). The entry applies only to a
gateway host started after it was added: macOS judges a process's exemption when it starts, so the running host, and
every job under it, stays charged until the gateway restarts — `launchctl kickstart -k gui/<uid>/dev.cowshed.gateway`,
or `cowshed gateway start`, which restarts the agent onto the installed build and refuses while a land or gate runs. No
install can take the step, so none raises it; `cowshed doctor` reports whether it holds (06_cli.md,
`first-launch-scan`).

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
the remedy. To install another build, run that build's own binary by its path — a release build's
`dist/bin/<platform>/cowshed setup`; a checkout's `packages/cowshed/bin/cowshed` runs its development build, which setup
refuses (06_cli.md "Launcher"). Such a run still repairs everything else, the plist included, and restarts a gateway
that answers while running other bytes than the build it leaves installed (status `executableSha256`): launchd keeps a
running process whatever the stable name now says, so this is what ends the disagreement the build refusal names.

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
its listeners included — and launchd no longer holds the job. The pid launchd holds is the gateway host's, which exits
only after the daemon, its child, has, so the host's `NOTE_EXIT` is the daemon's. launchd SIGKILLs a process its
bootout's SIGTERM has not ended at the agent's exit timeout, so that is the only bound: the exit timeout of the
definition launchd loaded, read from `launchctl print`, plus 1 s its unload may trail its kill by. Past it the bootout
is overdue, whatever `launchctl bootout` itself answered (it can exit non-zero while launchd tears the job down; its
answer is kept as context): an `environment-missing` error naming what is left — the pid that has not exited, or the job
launchd still holds — with the bootstrap that loads the agent once that has gone, and a restore never kickstarts it. The
gateway plist states `ExitTimeOut` 30 s (`GATEWAY_EXIT_TIMEOUT`), and the daemon's drain force-stops what it accepted
after 20 s (`GATEWAY_DRAIN_WITHIN`), so the drain, its force stop, its reap of the Nx backends ("Deadlines") and the
audit flush finish before launchd's kill. Measured 2026-10-22: launchd's default exit timeout is 5 s; the plist stated
none, so a bootout SIGKILLed a draining gateway 5.03 s after its SIGTERM, while the install's own wait, a fixed 5 s,
gave up 0.03 s earlier and reported the bootout stuck; its restore then kickstarted the job still in teardown (launchctl
37), and launchd dropped it a moment later, leaving the host with no gateway.

**An install refuses while a land runs.** Activating the agent restarts a gateway that runs, and so does the host move
inside `setup`, and a restart refuses a land's next command its connection to the daemon ("A land step waits for an
install in progress"). So an install — holding the slot, after its build refusals and before it installs anything that
would restart a gateway that answers `status` — refuses with `Conflict`, naming each blocker, while a land holds its
repository's turn (the first ticket of its queue as that gateway lists it, "Land turns": its workspace, ticket and pid).
It never waits for them. The refused install copies, moves and restarts nothing, so the installed cowshed and the
running gateway stay as they were; `setup`'s storage repair, which runs before its install (setup_service order:
volumes, owner markers, mains, git identity, then the service refresh), stays done. The hint is to rerun once the land
has ended, and `cowshed gateway status` lists the turns. A land queued behind the turn's holder runs nothing yet and is
not a blocker: its first command waits for the slot. The land turns are that gateway's own state, gone with it: one that
does not answer `land-turns`, a build from before them, names no land turn, and the install says so. A gate job that is
no land's — a train's proof, an operator's run — is no blocker: it lives in its workspace supervisor, which a gateway
restart leaves running. An install that restarts nothing (no daemon answers, or the refresh finds the agent current)
checks nothing. This replaces a wait with no deadline for the gates in flight: measured 2026-10-21, it held an install
behind a gate of 2 h 26 min until the gate was killed, and operators took to granting installs a window by hand.

**A land step waits for an install in progress.** A land is several cowshed commands with none running between them:
measured 2026-10-20, an install found no gate running as a land's first gate ended and restarted the gateway as its host
gate started, and that gate's `cowshed exec` failed with the daemon's socket refusing the connection, failing the land.
So every command a land runs — `exec --class gate`, `rebase` and `land` without `--preflight` — before it asks the
daemon anything, waits for an install that holds the slot ("The install slot", below), saying on stderr that it waits
and for which file, and then holds nothing. A step that waited is still the build it started as, and the daemon that
install left refuses another build. Measured 2026-10-21: a land's gate passed in 1,503 s, its next step waited for an
install, and the daemon refused it by build (exit 4), throwing the green gate away. So a step that waited and is then
refused as another build goes on as the installed cowshed: it execs, in place, the stored copy the stable name says once
the install is done, with its own arguments, keeping its pid, working directory, environment and descriptors, and says
so on stderr. A step that did not wait, or a refusal of any other kind, is returned as it is. A daemon connection
refused later in the step, while an install holds the slot, is that install restarting the daemon — a positive signal —
so it waits for the install and connects once more to the daemon the install started; a refusal with no install holding
the slot is returned as it is. An install that restarted the gateway — `gateway start` and `setup`'s refresh alike —
holds the slot until that gateway serves: its control socket answers from the installed bytes, its startup pass is over
and it is not draining, the wait `gateway start` reports. Releasing the slot at the restart is not enough: measured
2026-10-20, `setup` restarted the gateway and released the slot at once, and the land step that had waited on it found
the daemon's socket refusing the connection, failing a land whose gate had passed. A land step never holds the slot for
its run: it did once, and an install then waited on every land step in flight, between its gates as well as during them.
A job in flight lives in its workspace supervisor, which a gateway restart leaves running; what a restart breaks is a
new connection to the daemon, and that is what waits, and the guard against a restart between two of a land's commands
is the land's turn. Status reads the slot without taking it.

`land --preflight` only reads the pre-check fences (02_workspaces.md): it is not a land step, waits for no install,
requires no matching daemon build and never re-execs the installed cowshed.

**A land installs its own cowshed after its turn.** A land that changed cowshed, or a project it depends on (Nx's
affected set over the land's commits, and the root `Cargo.lock`, which Nx attributes to the root project alone), leaves
the host running the build before it, and cannot install that build from inside its own turn: an install refuses while
any land holds its turn, the installing land's included. `tooling/land-install.sh <repo> <before> <head>` therefore runs
once the turn has ended. It builds the release CLI (`cli-arm64-macos` or `cli-x64-macos`, `production`) and the Nx cache
server bundle as a land turn of its own (`cowshed land-turn <repo>`), so main stands still and no land's fast-forward or
build races it, and stages both in `.cache/cowshed-release/<head>/` laid out as a distribution
(`<package>/dist/bin/<platform>/cowshed`, the server under `<package>/node_modules`, where setup looks for it): the
debug build every land replays into `dist/bin` replaces the release binary there, and a stage is the copy whose path an
operator is told to run until three newer stages exist, each newer install superseding it. With no turn held it asks the
staged build's `doctor` whether `setup` would raise the administrator authorization (its host-storage findings come from
the plan setup executes, and it errs toward asking) and `gateway status` for what an install refuses under or waits for:
a land turn's holder or another install holding the install slot. It runs the staged `setup` once, in main's devenv
(setup installs the PATH it runs under, and its discovery runs cargo), only when neither says otherwise, and a refusal
`setup` still answers is classified by a fresh status. Every outcome is one line on stdout,
`<UTC time> <commit> install attended|unverified|deferred|installed|failed|skipped`, and the first three carry the exact
command, which enters that devenv too: to run by hand (`attended`, `unverified`) or to rerun once the named work has
ended (`deferred`). An install that cannot be taken is deferred, never waited for; the operator's land-status record
holds it for whoever watches that record.

**An install never refuses a client connection.** A restart closes every workspace listener until the next gateway's
startup pass restores its session (5 s and more), and a stock client cannot wait for that: measured 2026-10-20, an
install restarted the gateway under a shed's Nx build, and Nx read the refused cache connection
(`Failed to send request …/nx/v1/cache/…`) as fatal and failed the build. The carried Nx patch now reads it as a miss
("The client half" under `/nx/v1/cache/{hash}`), which still costs every lookup of that run. No wait can prevent it. Nx
holds nothing between two cache lookups, so no lease or slot shows that it runs, and waiting for all running work would
hold installs back on any busy host. A typed "restarting" error is no answer either, since Nx reads only HTTP statuses.
So the listening sockets outlive the restart. On macOS an install that restarts the gateway — `gateway start` and
`setup`'s refresh alike, not a `--downgrade`, whose older gateway might not take them — serves `listener-handoff.sock`
beside the control socket from before the restart until the restarted gateway serves. The old gateway, as its drain
begins and while it still accepts, gives the install a duplicate of every TCP workspace listener (`SCM_RIGHTS`), and its
drain then answers what it already accepted as before (the Nx route's 404/403). The install's copies keep each port
listening, so the kernel queues a connection made while no gateway runs. The next gateway takes them before it binds
anything ("Startup contract" step 2) and serves each on its restored session (step 4), queued connections included. Only
this user's processes may give or take, and the store deny keeps every sandbox off the socket. Every failure is what a
restart was without the handoff: no install serving the socket, an old gateway that predates the handoff, an install
that dies (the kernel closes its copies), or a failed take, after which the install closes its copies before the new
gateway binds. A gateway that drains with workspace listeners and finds no install serving the handoff — `gateway stop`,
then a fresh start — says on stderr how many it did not carry and why, so the refused connections that follow read as
designed, not as a failed handoff. Linux binds no TCP workspace listener, so it carries none. An exec client of the
replaced build reads its job's end from its own drained supervisor (11_shell.md "Draining a supervisor of another
build").

**Pruning.** After an install succeeds (for the gateway, after its activation), every stored copy and build record but
the new build's and the one it replaced is removed. The replaced build stays one install longer: a workspace supervisor
of that build, still draining its jobs (11_shell.md "Draining a supervisor of another build"), starts the shells those
jobs need from the path it started from. A failed prune is reported on stderr and never fails the install.

**The install slot.** The installed cowshed is one slot, so one install fills it at a time. `setup` (repair and
`--uninstall` alike), `gateway start` and `gateway stop --purge` each take an exclusive `flock(2)` on
`dev.cowshed/install.lock` before they read what is installed, and hold it until the command exits; the kernel drops it
when the process exits or dies, so no crash leaves the slot held. It is the lock `lockf(1)` takes: a second installer
waits in line, says on stderr that it is waiting and for which file, and then judges its build against whatever the
install it waited for left behind, because the judgement below runs after the lock is taken. A host move inside `setup`
restarts the gateway under the slot `setup` already holds. This exists because two workers each built and installed a
cowshed within an hour, neither aware of the other's commits, and nothing serialized who filled the slot.

**A pending install is visible.** `cowshed gateway status` says when an install holds the slot, without taking it from
the install: it tries a shared `flock(2)` on `install.lock` without blocking, which fails exactly while an install holds
it, and lets go at once when it succeeds (an installer arriving in that instant waits for it as for any install, and
says so). Status reports `pendingInstall: true` while the slot is held, absent while it is free; `gateway start`'s own
report, which holds the slot, carries none.

**Land turns.** A repository lands one land at a time, in the order the lands asked (the owner's ruling, 2026-10-18,
keeps the per-repository serialization; FIFO, no priority). A bare `flock(2)` on one lock file serialized lands but
never ordered them: the kernel wakes every waiter when the holder releases and the first to retry wins, so a land asking
at the moment of a release barges ahead of one that has waited for minutes. Measured 2026-10-21: a 72-commit train
queued behind one land, and when it released, a land that had asked after the train took the turn and held it for 36
minutes; the train's only output meanwhile was watchdog `QUEUED` records. So
`cowshed land-turn <repo> --workspace <ws> -- <cmd…>` waits for the repository's turn, runs the command holding it, and
exits with the command's status (128 plus the signal for a signalled one; SIGINT, SIGTERM and SIGHUP are passed on to
the command, and the turn is held until the command really ends).

The queues are the gateway's state, never files (2026-10-26). Measured 2026-10-25 and 2026-10-26: with each ticket a
file held through an inherited `flock(2)` description, the image cleanup's deletion children inherited a land's ticket
and kept its turn held after the land had ended, `gateway status` listed tickets whose pids had exited, still locked by
whatever had inherited them, and a second land waited 19 s for a `sleep 20` the first land's command had left running.
So a land asks the gateway over a connection of its own to the control socket,
`{"op":"land-turn","repo","workspace","command"}`. One actor keeps every repository's queue: it names the asker by the
kernel's identity of the connection's peer, and numbers each ticket itself as the ask reaches it, so a ticket's number
is its place and no ask that arrived later takes the turn ahead of one that arrived first. The first ticket of a
repository holds its turn. A waiting land is answered `{"turn":"queued","ticket","holder","ahead"}` when it asks and
whenever its queue changes, and `{"turn":"held","ticket"}` once the turn is its own. A turn is held by processes, never
by descriptors: by its asker until the asker ends its hold (`{"step":"end","outcome"}`), closes the connection or exits,
and by each command bound to it (`{"step":"bind","pid"}`, answered `{"turn":"bound","pid"}`) until that command exits. A
command binds itself: its asker arms it so that, between its fork and its exec, the child writes the bind step naming
its own pid on the asker's connection, which it shares until its exec closes it, and the asker reads the answer once the
spawn has returned. The gateway reads that step before the asker's end, which follows it on the connection, and after
the asker's exit it reads the connection on to its end of file, which comes once every copy is closed: so a land killed
at any moment after the fork leaves its command holding the turn, and one killed before it started none (measured
2026-10-26 in review: a land killed between spawning its gate and binding it left the gate running outside the turn).
The gateway watches every one of those exits (`ExitWatch`: `NOTE_EXIT`, a pidfd), never polls, and follows a bound
command across its execs by the kernel's identity of the process (Darwin's unique id, which an exec keeps while it
renews the pid version). A watch that fails is armed again once, on that same life, never on its bare pid, so a later
process given the pid holds nothing; one that fails again leaves the command holding the turn until the gateway
restarts, said on stderr, since a command that may still run is never taken for exited. The turn passes on once the last
holder has ended: the next ticket is told at once that it holds the turn, each other how many are ahead and who holds
it. Nothing a bound command starts holds the turn, whatever descriptors it inherits. A waiter that leaves, closed or
killed, leaves the queue, and nobody behind it waits for it. A turn is asked only over the Unix control socket, whose
peer the kernel names; the gateway refuses one asked over TCP. A waiting land says on stderr how many lands are ahead
and who holds the turn — its workspace, its pid, the pids of the commands bound to it and when it asked — when it starts
waiting, whenever the land ahead changes, and every 90 s between (the land watchdog's cadence), from the gateway's
answers alone, and how long it waited once the turn is its own. `--gateway-socket <path>` names another gateway's queue
than the host's, as for `disk-command`.

The queues live as long as the gateway: an install refuses while a turn is held (below), so only a crash loses them. A
land whose gateway connection closes while it holds the turn has lost it, since not knowing whether another land runs is
no permission to run beside it. Every holder waits on that loss beside each command bound to it: `land-turn`, a declared
land's gate, `cowshed doc-commit`'s commands and the ref fence's git are sent SIGTERM and reaped within the reap bound
("Deadlines"), and the step fails with a wrapper exit code, saying whether the signal reached the command and how it
ended. A declared land whose turn is lost starts no further rebase or gate, stops once a rebase during which it was lost
has ended, and one that finds it lost before its fast-forward never fast-forwards.

`--outside-lock <file>` names the lock file lands took before every caller asked for turns: the head takes it too, as a
held lock (02_workspaces.md "Held locks"), keeping the file, so it excludes a land started that way. A legacy process
outside the queue holding that file may record nothing, so while the head waits it names the processes with the file
open: when it starts waiting and every 90 s after, one census (`build_volume::nx::holders`) gives each one's pid and
command, the head's own descriptor left out. These are open process descriptors, not a proof of lock custody or
authority to signal. A census that finds no other process says so (the holder let go as it looked, or is not this user's
to see), and one that cannot be taken says why. Measured 2026-10-21: a land waited 1002 s for the outside lock having
said once that "a land that did not ask for a turn" held it, naming no process. The census decides nothing: the head
waits for the kernel's lock alone. The wait starts before the first census, on a thread of its own, so no census delays
taking the lock. Measured 2026-10-21 on one fixture (a `lockf -k` holder for 1.5 s, debug builds, a 709-process host, 20
interleaved runs each): without the census the first report came 18 ms after the wait began and the turn 20 ms after the
holder let go (medians); with it, 213 ms and 17 ms, at about 0.11 s of CPU per census.

`land-turn` arms its command to bind itself to the turn as it starts (above), and the command inherits the outside
lock's descriptor, so the kernel keeps that lock until the command, and every process it hands it to, has closed it:
`land-turn` killed alone never lets the next land run beside its command, and the next land takes the turn when the
command exits. A command the gateway refuses to bind is sent SIGTERM and the land fails, so no command runs outside its
turn. The launch boundaries and the git and Nx policies that keep cowshed's own daemons and git's automatic maintenance
out of a held command are specified in 02_workspaces.md "Held locks". A land's turn covers rebase, gate, fast-forward
and adoption, and a declared land (02_workspaces.md "A declared land") takes it in its own process for that span and the
retire that ends it, binding its gate to the turn; the push to a remote runs after the turn ends, since a remote is
never needed to land (the owner's ruling, 2026-10-17). A turn is only as durable as the processes that hold it: a land
that shares its caller's session ends with that session. Measured 2026-10-21: one lander's lands died with its tool
session three times, twice while queued and once mid-gate. So a caller that may go away first starts the land in a
session of its own (`setsid`, output to a file, not its child) and waits on the land's pid; `land-turn` itself never
detaches, since the caller that stays reads its output and its exit status.

**A ref turn.** A repository whose main also moves outside its lands (an operator's Markdown commit straight into main)
keeps a second queue beside its land turn, by convention `<repo>.ref`: every writer that moves main's ref outside
cowshed takes it for that move alone (`cowshed land-turn <repo>.ref --workspace <label> -- <commit>`), and a land names
it to `cowshed land --ref-turn <repo>.ref`, which takes it only from re-reading the target head through the fast-forward
(02_workspaces.md "`cowshed land`" step 3). It is a land-turn queue like any other: first asked, first served, the
holder named, held by the land and by the fast-forward's git, which it binds to the turn. A Markdown commit therefore
waits for one fast-forward, never for a land's gate, and never barges. A land takes its land turn first and the ref turn
inside it; a writer that holds the ref turn never asks for the land turn, so the two never deadlock. A flock(2) ref lock
(`--ref-lock`) orders no waiters: the kernel wakes them all and the first to retry wins, and it names no holder in
order.

**A ticket's life is said.** Measured 2026-10-21: a land queued for 635 s vanished with its agent's session, its log
stopping mid-wait, and nothing recorded it, so it read as running for half an hour while it did not exist. The gateway
says each ticket's life on its stderr log, one line per event: asked (with the workspace, pid and command), held (after
how long), each command bound, and its end: `finished: <outcome>` once its asker ended its hold with how the command
ended (`exited <code>`, `killed by signal <n>`, `did not start: <reason>`) and every bound command has exited, or
`abandoned` when its asker went without an end: `it left the queue before its turn`, or
`it held the turn, and its asker went without ending it` (cowshed failed, was killed or crashed, its session gone). The
gateway keeps the last 256 abandonments. `cowshed gateway status` asks it, `{"op":"land-turns","abandonedSinceUnixMs"}`,
and lists each repository's tickets, the holder first, and the tickets abandoned in the last day, as
`landTurns: [{ repo, tickets: [{ number, pid, workspace, askedUnixMs, command, bound? }], abandoned?: [{ atUnixMs, ticket, reason }] }]`,
read without taking a turn; a gateway that does not answer `land-turns` lists none, and status says why
(`landTurnsError`). A gateway that is not running holds no turn.

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
author nothing). This is an identity question, deliberately not `cowshed rm`'s, which also holds a commit by content
(02_workspaces.md, `rm` step 1): a build that lacks a commit lacks it whatever its tree says, while a workspace whose
change main holds under another commit loses nothing when removed. Main is the authority because it outlives every
workspace: a workspace's commit may be rebased away when it lands and the workspace removed, while main only moves
forward. A rebased or cherry-picked commit, or a squash of one commit, is held by patch identity; a squash of several
commits matches none of them, and is refused naming each.

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

**Uninstall.** `setup --uninstall` and `gateway stop --purge` remove, after the agent is gone, the gateway host and then
the stable name (so nothing launchd could start dangles), then every stored copy with its `.build` record, a `cargo`
link an earlier install planted, and every `cowshed` on `PATH` that is a link saying exactly the stable path — the
entries setup made, and no other link that merely arrives there; a `cargo` no install planted is left as it is. A plain
`gateway stop` keeps all of it: it is host state rather than agent state, and the next `start` is then a plist write.

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
produced, exactly as it would with no cache. Stock Nx 23.2.1 fails a task on any other status (Busy gateway, below), so
the gateway never answers or forwards one: drain, a closed audit sink, a stopped actor, a queue bound, a body timeout
and a backend response head that Nx cannot read are all the method's 404/403. The audit and the log record the cause.

**The client half: an unreachable cache is a miss.** A gateway that cannot answer at all (a stopped listener, a
host-wide stall) answers nothing the route can map. The carried Nx patch (`patches/README.md`, "A remote cache outage is
a miss") makes the client read a refused or dropped connection, a 5xx and no result within 90 s as an outage: the lookup
misses, the store is skipped, one line names the server and the cause, and that process asks the cache nothing more. The
90 s exceed this route's own outage answer (`GatewayTimeouts` connect 5 s plus response head 60 s), so the gateway's
audited miss decides whenever the gateway runs. Every other status Nx reads as misconfiguration still fails the run.

**No credential: one user, localhost** (the owner's ruling, 2026-10-25). The store is one host directory of one user,
served on loopback. A hash names a task's inputs, so a result stored under it by any shed is as trustworthy as that
shed's own local cache: a task that builds under an input it does not declare poisons the local cache exactly as it
would this one, and the remedy for both is declaring the input (16_build_volumes.md). So the route authenticates
nothing, and no token, principal, transaction or receipt guards it. What it keeps is what correctness needs: project
namespacing, the key charset, atomic first-writer-wins writes, and the eviction bound (03_caches.md, "Nx accepted-task
artifact service").

**Sheds read; a land writes** (the owner's rulings, 2026-10-10). Every checkout reads its project's results, during
ordinary iteration as well as adoption. Nx keeps its local artifact cache and task database private to that checkout
(16_build_volumes.md); this route shares neither path nor live database. The controller wires stock Nx's
`NX_SELF_HOSTED_REMOTE_CACHE_SERVER` to `<base>/nx`, without a trailing slash, beside
`NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true`, in `.cowshed/env` and in every job's environment (03_caches.md), and no
access token. The read-only flag is configuration, not authority: the carried Nx patch makes a read-only client answer
every store `false` before it builds an archive (patches/README.md, "A shed reads the remote cache and never stores"),
so work in progress is never stored and never costs an upload. The gateway itself would store a shed's `PUT`; nothing
configured sends one. Every supervisor start rewrites `.cowshed/env` whole, so an existing shed's older file, which
exported the retired bearer and no flag, carries the flag from the first supervisor start of a build that sets it, and
direnv reloads it in the shed's host shells.

The listener or Linux socket selects the workspace, its session selects the project, and only the exact, query-free
`/nx/v1/cache/{hash}` route reaches that project's backend, so a hash in one project never names another project's
archive. A hash is one validated Nx task key: 1-128 characters of `[A-Za-z0-9_-]`, the backend's own key charset. Nx
uses a task's hash verbatim as its cache key, so this admits stock Nx's decimal hash and any hex, decimal or base64url
value an executor's custom hasher returns; it excludes `/`, `\`, `.` and `%`, so a key is never a filesystem path or an
escaped one. A final segment outside it does not name this route and is a 400 `invalid-target`. The gateway forwards
`GET` and `PUT` to the selected project's backend as `GET`/`PUT /v1/cache/{hash}`; any other method is denied before a
backend starts or is waited for.

**Publication.** A land publishes after its gate passes. `cowshed land` attributes each passed check's Nx run from the
run summary the check's Nx wrote (16_build_volumes.md, "Land step 7 source capture"), then runs `node - <root> <hashes>`
with `build_volume/nx_publish.js` on stdin as a gate-class job of the landing workspace. The job stores every attributed
task result from that checkout's own Nx cache through the workspace's own route, with plain `PUT`s from the stock native
`HttpRemoteCache` itself, which the read-only flag the job also carries does not wrap: local hits as well as executed
tasks, since stock Nx stores only what it executed. A task Nx does not cache, or one that failed, is skipped; a key
already stored is counted, never replaced. The cache never blocks a land: a failed publish is said on stderr with its
cause, and the land goes on. A declared land's gate (02_workspaces.md "A declared land") publishes its run with the same
script (SmoothBricks: `tooling/land-host.sh --gate` through `tooling/land-publish.ts`, after its host gate and Linux
cross check pass).

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
artifact or 404 for a miss; `PUT` returns 200 for a stored archive, 409 for an existing key, or 403 for a refusal (the
backend's full staging budget, or an outage below). An incomplete, interrupted, or evicted artifact is never a
successful empty restore. Project backend lifecycle and bounded eviction belong to 03_caches.md.

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
`convert_response_to_error`). The carried patch reads only a 5xx among them as the client's own outage ("The client
half", above); every other status stays fatal. So a busy gateway answers what a cache may always answer, not served now,
and records why in its audit. Like an outage, that 404 is not evidence of absence and never counts as a hit. The price
is work, not failure: a lookup wave wider than the queue re-runs its refused tasks, even ones the cache holds, and a
refused `PUT` is not stored. That extra work is the designed cost of a busy gateway, not a regression. Drain and audit
fail-closed keep their own audit status and classification, not a backend outage's, but answer the same 404/403.
Unsupported Nx methods are denied before starting or waiting for a backend.

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
repository with an admitted or queued Nx request is refused, naming the work by workspace; one that is `Starting` or
already `Clearing`, or whose backend the host configured, is refused too.

**Outage before and after the response head.** The statuses above apply only before the response head. Once the upstream
commits a success head, the gateway keeps `200` and cannot change it. A body that then fails, times out, or is dropped
ends early, and its audit event is `failed` with classification `nx-transfer-failed`, never a hit or a completed read. A
client must treat a short body as a failed read; the gateway stages and buffers nothing to hide the cut.

**A shed never uploads.** Stock Nx 23.2.1's `native/cache/http_remote_cache.rs` has no read-only option: its `store`
first builds an in-memory gzip tar archive, then attempts `PUT`, and `skipRemoteCache` disables reads too. The carried
patch's read-only client (Sheds read; a land writes, above) answers every store before that archive exists, so a shed's
run costs its project cache lookups only. Measure lookups and a land's publication separately (13_telemetry.md).

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
  - A leaf carries the RFC 5280 end-entity profile: a subjectAltName for the host, an Authority Key Identifier equal to
    the issuing workspace CA's Subject Key Identifier, critical keyUsage digitalSignature (the leaf key is P-256 ECDSA),
    and extendedKeyUsage serverAuth. The workspace CA carries a Subject Key Identifier, critical basicConstraints
    CA:TRUE, and critical keyUsage keyCertSign and cRLSign; being self-signed, it needs no Authority Key Identifier.
    Strict X.509 verifiers accept the chain: OpenSSL with `X509_V_FLAG_X509_STRICT`, which Python 3.13+ sets in its
    default context, refuses a leaf without the Authority Key Identifier ("Missing Authority Key Identifier"). A change
    to the leaf profile needs no CA migration: leaves exist only in the in-process LRU, so a gateway restart mints every
    leaf afresh under the existing CAs.
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

The control plane provides status and audit to host tools, a project-bound repo-mirror operation to coordinators, the
host's disk-lifecycle lease to every process that runs a disk tool (below), and the nudge of the released-image cleanup
to every process that releases an image ("Deleting released images"). Peer credentials on the Unix socket (and
equivalent local authentication on TCP) must identify an authorized host process; the data-plane token is never accepted
on the control plane. The Unix accept loop records listener failure, unreadable peer credentials, unauthorized UID,
connection-capacity refusal, connection-task failure and shutdown; an admission refusal is never a silent return.

Workspace sessions are installed and removed over the control plane, and the gateway's session table is a cache of host
inventory, never an authority. Each session carries its workspace's effective grant revision, which every grant change
and every attach advances. An install must exceed every revision that workspace identity has held — a removal leaves its
revision behind as a tombstone — and a removal must name the installed revision; either refusal is `revision-fence`, and
a removal of a session that is already gone is `not-installed`.

A workspace whose mount is unproven (01a_apfs.md, "Ownership, identity, and the volume label") is held by the inventory
that found it: reconciliation installs no session for it and removes none of its sessions, at any incarnation. A held
workspace's session holding an endpoint another project claims counts as live, so that conflict is reported rather than
evicted.

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

`{"op":"ram-lease","sectors":<n>}` attaches one RAM disk of `n` 512-byte sectors and holds it for the connection that
asked ("RAM disks", below). It answers `ramDisk: { device: "/dev/diskN", registryEntry: "0x<hex>" }`, the whole disk
`hdiutil` printed and the I/O Registry entry the gateway read it as; the client then writes one `{"end":"keep"}` or
`{"end":"release"}` line and half-closes. `{"op":"first-launch"}` is one-shot and measures a fresh executable's first
launch from the daemon's own process ("The gateway host"): a copy of the daemon's executable, signed ad hoc again under
an identifier no launch has used, launched twice with `--version`, each timed from spawn to exit, then removed. It
answers `firstLaunch: { firstMicros, secondMicros }`; a daemon that supplies no probe, or a probe that fails, is
`rejected` with the probe's own words. `{"op":"ram-detach","device":"/dev/diskN","registryEntry":"0x<hex>"}` is one-shot
and detaches the RAM disk that I/O Registry entry registers as that whole disk. It answers `ramDetach`, whose `outcome`
is `terminal`, `not-started` or `unknown` (same section).

Every request is one line. A one-shot request is followed by the client's EOF and answered with one line; a `disk-lease`
or `task-admit` request is held for as long as its client keeps the connection open, and a `ram-lease` until its
client's end line or the connection's end.

## Deadlines

Every wait one cowshed process has on another has a bound sized to its latency class. On expiry the wait ends and
answers a typed failure that names the op, the peer (its socket and pid), the bound, the time waited and the last
progress the waiter saw. The expiry ends the wait, never the peer. Nothing retries around the failure and nothing raises
the bound to pass it: the failure is the diagnosis. The values are `lmao-core`'s (`Deadline`, `Idle`, `Expired`,
`Silent`, `RetryClass`; specs/lmao/01r_transient_errors_and_retry.md, "Rust"). The shell that maps a connection's reads
and writes and the timer onto them is `cowshed-os::deadline` (`PeerStream`, `within`, `establish`). `PeerSocket` is
`PeerStream`'s blocking twin for a client that runs no async runtime: one nonblocking connect, then a poll before each
read or write clipped to the wait's bound, and no socket timeout option, since Darwin refuses `setsockopt` once the peer
has shut down and a kernel timeout restarts on every partial read. Both name the peer by the pid the kernel reports for
the connection (`peer_pid`: `LOCAL_PEERPID` on Darwin, `SO_PEERCRED` on Linux), never by who owns the socket path.

| class       | covers                                                                                                                                                                                             | bound |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| control     | connecting to a running daemon and sending it one request                                                                                                                                          | 5 s   |
| handshake   | connecting to a peer that should be running, and its hello                                                                                                                                         | 10 s  |
| reap        | a child's exit once its pipes closed or it was killed                                                                                                                                              | 5 s   |
| ffi         | the image lease a disk-image command holds for its run                                                                                                                                             | 120 s |
| ffi         | one launchctl print or bootstrap                                                                                                                                                                   | 10 s  |
| ffi         | a launchctl bootout of an agent whose exit timeout is ET                                                                                                                                           | ET+1  |
| ffi         | a launchctl kickstart -k of that agent                                                                                                                                                             | ET+11 |
| ffi         | one codesign signature or verification of the host binary                                                                                                                                          | 10 s  |
| remote-http | one request to GitHub, Forgejo or the npm registry, or one `gh` command that makes one, through its whole answer; a stream read as it arrives waits at most this long for each read, with no total | 17 s  |

The remote-http bound is measured, not chosen: the slowest kind's p99 × 20, rounded up to the second. Measured
2026-10-25 23:10–23:14 UTC from the macOS development host (18 CPUs, load average 15–28 while other gates ran), each
sample a fresh connection read to the end of its body (`tooling/measure/remote-http-p99.sh 100`; the `gh` kinds with 60
samples): GitHub REST `GET /repos/…/releases` p99 0.59 s, npm registry abbreviated packument p99 0.06 s, Forgejo (the
project's own instance) `GET /api/v1/repos/search` p99 0.22 s, `gh api` one request p99 0.78 s, and `gh secret list` p99
0.82 s, the slowest, which gives 16.4 s. Only reads were measured; a write (a release's create or update, a workflow
dispatch, `gh secret set`'s key read and write) is held to the same bound. A new bound needs a new measurement here.

A control or handshake bound covers establishing the connection and sending the request, which the kernel completes
whatever the peer's load. The answer itself is bounded by the heartbeat silence below, never by a total, so a saturated
peer that is still working is never failed for being slow. Hello is the exception: the whole hello exchange is inside
the handshake bound.

- **Codes.** `TIMEOUT` is a request past its class's bound. `SILENT` is a long wait whose holder showed no progress for
  its silence bound. `PEER_LOST` means the peer ended the connection before answering, so the outcome is unknown and the
  caller reads state again. These three are terminal. `BLOCKED` means nothing listens at the socket (missing, or
  refusing with nothing attached); it is lmao's `Blocked(service)`, and the caller states its own fallback. A socket
  that refuses while still attached is a live peer that is not accepting (a full queue), never `BLOCKED`.
- **A long wait has no total ceiling; its holder heartbeats.** Some answers take as long as their work: a job's
  terminal, a log follow, a commitments read, an advance, a process event. While such an answer is not ready, the
  workspace supervisor writes `{"progress":{"seq":n,"job":{jobId,state,members}|null}}` on the call's connection every 5
  s. The job field is present for a call about a job. The client connects and sends within the control bound, then reads
  past the heartbeats under a silence bound of 15 s (three heartbeats). Each heartbeat is a round trip through the
  supervisor's actor under the authority the supervisor serves now: it reads that authority, and the job for a call
  about one. A grant advance during the wait therefore changes nothing. A supervisor that is alive but silent, or whose
  actor stopped answering, writes no heartbeat and fails `SILENT`, naming its socket and pid. A heartbeat about a job
  that is running or draining with no process of its group left is not progress. Only a silence the waiter was there to
  see is judged: a waiter whose own turn comes later than the silence bound plus one heartbeat after its last one (its
  thread was blocked, its process stopped, as an in-process controller and its client stall together) did not run to
  read what the holder sent meanwhile, so it gives the holder one heartbeat more from then before it judges. That is a
  liveness rule, not a wider bound: a waiter that runs in time judges the holder at the bound, and a SILENT still names
  the holder's whole silence since its last progress. If that state persists for the silence bound, the wait fails
  `SILENT`, naming the job as stuck between its last process and its end. A queued job is progress: resource admission
  has no total queue-age deadline ("Host scheduling").
- **Cowshed CPU-pool admission has its own policy when that pool is used.** This is the existing non-mbx protocol, not a
  protocol imposed on mbx. Connecting and writing the complete request share the 5 s control bound. After the write
  completes, the client keeps its connection open and reads the queue acknowledgement and grant under the 15 s
  heartbeat-silence bound, never a total queue-age bound. A valid queued answer arriving after 2 s is still accepted:
  the disk lease's 2 s half-close probes a gateway predating that protocol and has no place in CPU admission. While
  requests wait for tokens, the pool actor writes `{"ok":true,"progress":n}` every 5 s. Only an advancing positive
  sequence renews silence, as does the queued acknowledgement, `{"ok":true,"lease":"queued","server":pid}`, which names
  the process the pool runs in. The heartbeat runs on the same event loop that admits and grants, not an independent
  liveness thread, and that loop is the pool's own. An Nx run's pool runs on a worker thread of the process that serves
  the run: that process (the Nx daemon when installed plugins load in it, the Nx CLI without one) hashes tasks and
  builds the project graph in synchronous calls on its main thread, and a pool on that thread fell silent behind a 14.2
  s `hashPlans` while it still worked. A granted `bounded-exec`'s sub-pool runs on its executor's thread, in the task
  process Nx forks for that executor alone, which blocks on nothing longer than milliseconds. A queue that keeps
  heartbeating waits until a token returns; a silent actor fails admission as `SILENT`, naming the socket, the pool's
  pid, bound, elapsed wait and last progress, and a refused `bounded-exec` keeps that failure as the typed `cpuWait` row
  of its verdict record. Expiry ends only this client's wait and releases its connection; it neither runs the command
  unbudgeted nor signals the pool.
- **The gateway control plane heartbeats the same way.** A one-shot request is connected, written and half-closed within
  the control bound. While its answer is not ready, and while an audit-tail follow waits for events, the gateway writes
  `{"ok":true,"progress":n}` every 5 s, once its actor has answered a ping. A heartbeat line, once begun, is written
  whole before the answer. The client reads past those lines. Until the first one arrives it allows the gateway 60 s of
  silence, because a gateway of an earlier build, which a client meets during an install, never heartbeats, and its slow
  answers (`first-launch`, `ram-detach`) still arrive within the minute doctor always gave it. Once a heartbeat has
  arrived, the 15 s silence bound applies. An answer that takes as long as its work (`nx-cache-clear`, `ram-detach`,
  `first-launch`) is therefore never cut off, and a gateway whose actor stopped turning is named `SILENT`. A held
  request (`disk-lease`, `task-admit`, `ram-lease`) keeps its own protocol above.
  - **A child's exit and its output are reap-class waits.** A child cowshed has signalled to end is waited for within
    the reap bound (`cowshed-os::deadline::reap`), its exit watched (`ExitWatch`: `NOTE_EXIT`, a pidfd), never polled.
    Past the bound the wait fails `TIMEOUT` naming the child's pid and the state the kernel shows it in
    (`cowshed-os::process::run_state`), so a child stuck in an uninterruptible kernel call, which no signal ends, is
    named as such. Once a child has exited, its piped stdout and stderr are read to their end within the reap bound
    (`OutputPumps`): a pipe still open then is held by a process the child started, and the run fails `TIMEOUT` naming
    the child and the stream. A failed read of either stream is a typed failure, never empty output. In both cases the
    child, and whatever holds its pipe, is left as it is. Every disk child's run takes both waits
    (`SystemCommandRunner`). A Linux job's retirement after `cgroup.kill` is the same wait over its cgroup: the
    empty-membership notification and each signalled member's pidfd within one reap bound, past which it fails `TIMEOUT`
    naming every member still alive with its pid, image and kernel state (07_api.md, "Complete job accounting and
    observation reconciliation").
    - **A child whose output or CPU is its progress has a silence bound, not a total.** A long-running command that
      shows its work runs under `cowshed-os::deadline::run_by_progress` (`run_to_log` appends both streams to a log,
      `output_by_progress` collects them), its child a member of a source its caller names, which is authoritative for
      every member wherever it was reparented: on Darwin the child leads a process group of its own, which the kernel
      lists (`proc_listpgrppids`); on Linux a cgroup v2 leaf of this run alone that the child enters before it executes:
      a controller run's (07_api.md "Complete job accounting and observation reconciliation"), or one an after-land job
      makes for each command inside its own job cgroup, so nothing an earlier command left behind is counted as the next
      one's or takes its `SIGTERM`. A failure to enter the leaf names it and its `cgroup.procs`. The members are never
      read by a census of the host. Its progress is its output or CPU advancing among its members: each chunk restarts
      its silence bound, which its caller names, and once the silence is spent the members' CPU (each one's own and its
      reaped children's on Darwin, the cgroup's `usage_usec` on Linux) is read and compared with the last reading, so a
      hook computing in silence keeps it going. A child its caller names as waiting in line behind a live holder is
      queued, not silent: it keeps waiting, said on stderr. A child that shows none of these for its silence fails
      `SILENT`, naming its pid, the state the kernel shows it in, the bytes it wrote and every running member by pid,
      command and state; the members are sent `SIGTERM`, the child reaped within the reap bound and collected later past
      it. A child that ends its streams is reaped within the reap bound and its status answers. Its callers: a
      repository mirror's transfers (`clone --mirror` and the `fetch --prune` that updates it), with `--progress`, their
      output in the mirror's transfer log; workspace git that runs repository-selected merge drivers, filters and hooks
      (both Darwin's); a project's tool the controller runs outside any job (the land's nx daemon --stop), in a
      controller run's leaf on Linux; and a declared land's push and after commands, inside their after-land job
      (02_workspaces.md "A declared land" step 6). Discovery's Cargo instead runs in its canonical workspace job and
      applies the same heartbeat silence (15 s, HEARTBEAT_SILENCE) only after its command starts; waiting on shared
      shell activation does not spend it (11_shell.md, "One spare and a SIEVE cache"). Neither query waits on Cargo's
      package-cache lock, measured, so no holder extends its command silence. nx daemon --stop has the 10 s a stopped
      daemon gets after SIGTERM (DAEMON_EXIT_GRACE); push and after commands have 180 s.
  - **The gateway's drain reaps its Nx backends within one reap bound.** Once nothing is in flight, the drain sends
    `SIGTERM` to every Nx cache backend it started and waits for all of them within one reap bound
    (`cowshed-os::deadline::reap_async`). The actor is therefore parked at most that long, and the drain ends inside
    launchd's kill: the force stop at 20 s (`GATEWAY_DRAIN_WITHIN`), this bound and the audit flush fit in the 30 s exit
    timeout (`GATEWAY_EXIT_TIMEOUT`, "The installed cowshed"). A backend still running past the bound fails the drain
    `TIMEOUT`, naming its pid and the state the kernel shows it in, said on stderr for each such backend; the sessions
    still stop and the audit is still flushed first. The expiry ends the wait, never the backend: its stdin closes,
    which asks it to drain and exit (`--exit-on-stdin-eof`, Nx route), and a task owns it until it exits and collects
    its exit (`cowshed-os::deadline::collect_later_async`), since dropping it would kill it (`kill_on_drop`). If the
    gateway process ends first, its runtime drops that task and the backend's `kill_on_drop` kills it: the process
    ending kills it, not the wait. `drain_names_an_nx_backend_that_ignores_sigterm` pins the bound, the named pid and
    the backend left running.
- **A short lock is a request.** A lock taken for one short critical section is waited for within its class's bound. The
  job records lock (11_shell.md, "Job control") is held for an open's recovery of the records and its scan of the job
  directories that stay, a refresh, one record-sequence allocation and its append. An open refuses records past the
  retained recovery budget (64 MiB) and its scan reads one directory entry per job that stays, so each hold is short and
  its wait is control class. A free lock is taken without blocking. A held one is waited for on a helper thread that
  blocks in `flock(2)` and hands the locked descriptor back, so nothing polls. At the bound the wait fails `TIMEOUT`,
  naming the lock path and its holders as the held-lock census reads them then (02_workspaces.md, "Held locks"); every
  caller (the supervisor, doctor's repair, a workspace's lineage read) answers it as a wait on another process, never as
  damage to the store. The holder is never signalled, and a lock the helper takes after its waiter left is released at
  once. A checkout's activation-record fence (one read and write) and the lifecycle intent journal's lock (one read and
  write) are waited for within the control bound the same way. The image lease is held for one disk-image command's run,
  so its wait is class `ffi` bounded by that command's own deadline, 120 s (`DISK_CHILD_DEADLINE`), and it names the pid
  its holder recorded. The shell is `cowshed-os::flock::exclusive`.
- **A lock held across its holder's work is queued, not silent.** A waiter behind a holder that keeps the lock for as
  long as its work (an image's lifecycle lock, a project's retirement lock, a build volume's exclusive claim, the
  install slot, the ref lock) has no ceiling: a bound would fail a waiter that is only queued. It keeps its place in the
  kernel's queue on a helper thread and names the holder, as the held-lock census reads it, once per report interval: 10
  s (`HOLDER_REPORT`), and for the image lifecycle lock once per `DISK_CHILD_DEADLINE`, the bound of each disk command
  its holder runs. The holder's release at its work's end, or the kernel's at its exit, ends the wait; nothing signals
  the holder. The shell is `cowshed-os::flock::queued`. A land or ref turn is no lock: it is the gateway's state, waited
  for on its answers ("Land turns"), and the wait names the holder at the same 10 s cadence.
- **A launchctl or codesign call is an ffi call.** Every `launchctl` call cowshed runs as the user (the CLI's
  `LaunchctlCommand` and setup's mount-service probe) and every `codesign` of the gateway host runs to its end within
  its row's bound (`cowshed-os::deadline::output_within`). The calls setup makes through its privileged session
  (`AuthorizationExecuteWithPrivileges`: the mount service's bootout, enable, bootstrap and kickstart) are read from
  that session's pipe, which no bound covers yet. Past its bound a call fails `TIMEOUT` naming the tool's pid and the
  state the kernel shows it in, and the tool is left as it is. A failed call is a failure, never an answer: a probe
  whose launchctl did not answer never reads as "not loaded". The bases, measured 2026-10-26 on an M5 Max (18 cores) at
  load average 17: `print` and `bootstrap` answer from launchd's own state in at most 0.02 s (five `launchctl print`
  runs of the gateway agent and one `bootstrap` of a test agent), so their bound is the 10 s starting value of the
  class. A `bootout` returned in 0.01 s while launchd went on tearing the agent down, but launchctl may wait for the
  process, so its bound is launchd's own deadline for the bootout: ET, the exit timeout `launchctl print` reports for
  the loaded agent, plus the 1 s its unload may trail the kill (`agent_cycle::bootout_deadline`). A `kickstart -k`
  returns only once the old process has exited and the new one has started: 9.03 s at ET 2 s and at ET 6 s (launchd's 10
  s respawn throttle, counted from the old start) and 12.02 s at ET 12 s, a test agent ignoring SIGTERM. So it is
  bounded as that bootout and then one launchctl call. `codesign` signed a copy of the 48 MB gateway host in at most
  0.15 s and verified it in at most 0.06 s (three runs each).
- **A controller connection heartbeats from handshake version 2.** While a call on a version 2 connection is unanswered
  (an open stream counts), the controller writes `{"progress":n,"busy":{method,subject?,forMs}|null}` on it every 5 s.
  `busy` is the op the project's router actor serves now: its method, the workspace it names, and how long it has run.
  The actor publishes it as each route starts and clears it as the route ends; a heartbeat reads it without a lock, and
  it is `null` while the actor serves nothing. The router is serial, so a call waits behind whatever op it is busy with,
  and a call has no total bound, which would fail a call that is only queued. The client reads past the heartbeats under
  the 15 s silence bound, measured from when the controller last owed it nothing. A controller silent that long fails
  every waiting call `SILENT`, naming it as the socketpair peer with the pid its hello reported (the socketpair's
  credentials name the process that made the pair, not the controller) and the busy op of its last heartbeat. SILENT
  ends those waits, never by itself the connection: a `cowshed controller` exits once its peer closes its end
  (06_cli.md), and every other op of the coordinator shares that connection. The client keeps it, closes each stream
  whose demand failed, and sends the next call on the same connection. A failed call stays owed: an answer or step the
  controller sends for it late — a real one included — is read and dropped, and the call holds its in-flight slot until
  then. Silence is measured on while any such call is owed, and its clock restarts each time it expires, so a call
  queued behind slots the abandoned calls hold fails `SILENT` too, never waits without a bound. A controller silent for
  a second window in a row with every slot held by abandoned calls is lost: the client fails every waiting call with
  that loss, naming the controller's pid and the sample of its last recorded silence, and closes the connection, which
  ends a `cowshed controller` serving it. A heartbeat proves that the controller process and its connection serve, and
  names the op the actor is busy with. It does not prove the actor is not deadlocked: an actor stuck inside one op keeps
  the heartbeats coming, and their `forMs` keeps rising. A version 1 connection, an older client's, hears none
  (07_api.md, "Controller handshake").
- **Spans.** A failure reaches the boundary that owns the wait as a value. A `TIMEOUT`, `SILENT` or `PEER_LOST` failure
  carries its facts (`WaitFailure`: code, op, peer and its pid, class, bound, time waited, last progress) on the
  caller's error, inside the process only. An owner that has a span ends it `span-err` with the failure's message. The
  CLI has none: a command that ends in an expired wait seals it, at its one error boundary, as a one-span `cowshed.wait`
  segment in the trace it adopted from `TRACEPARENT` (13_telemetry.md, "Wait spans"). The expiries that carry it come
  from every workspace-supervisor call and from the gateway control requests that reconciliation and the CLI make
  (`status`, session install and removal, `session-current`, `nx-cache-clear`). A `BLOCKED` peer and a failed I/O call
  are not expiries and write no span. Neither does an answered wait, whose time the command owns, nor an expiry the
  caller takes as an answer (doctor's unanswered gateway, a probe of whether a gateway runs). The stderr line stays: the
  command's own report names the failure, and a seal that fails is said there and never changes the outcome. A CLI
  foreground relay that fails SILENT on a workspace supervisor captures that peer's stacks before releasing its job
  hold. On macOS it invokes unprivileged /usr/bin/sample for 2 seconds, within one 10 s ffi bound, without signalling
  the supervisor. It retains the report as a private 0600 wait-<uuid>.sample.txt beside the wait span, names it in the
  error, and records its path as wait_stack_sample (or the capture failure as wait_stack_error). A client whose call on
  a process-external controller connection fails SILENT samples that controller the same way and seals the
  `cowshed.wait` span itself under the host's telemetry root: the controller's own boundaries never see a wait its peer
  gave up on. It samples the pid the hello reported only while that pid's process started at the instant it read at the
  handshake; a pid that ended or was recycled since is not sampled, and why is its wait_stack_error. The sample and the
  seal run off the client's actor, which keeps serving calls, answers and stream closes meanwhile; the waiting calls are
  told once the silence is recorded, and their error names the sample (`cowshed controller stacks: …`) and carries the
  receipt, so an embedder's span names it and a later boundary records nothing again. Other command failures use the
  same final error boundary. The local receipt is reused there: one capture and one seal attempt per failure, even when
  either failed. The wait's expiry timestamp, elapsed time, bound and exit code are fixed before sampling; diagnostics
  neither retry the operation nor renew its silence. An in-process controller socketpair retains each typed wait cause
  in that pair alone, under its response id, before encoding the unchanged error frame. Its reader takes and reattaches
  the cause before delivering Err. The client's pending-call cap and the connection's open-stream cap bound retained
  causes. Process-external connections share no custody and encode no private cause. Host exec and a declined resident
  path therefore retain the same diagnostic facts as a direct resident call, without extending the public error wire.
- **Enforcement.** TypeScript: the `deadline/no-unbounded-wait` ESLint rule (`tooling/checks/no-unbounded-wait.ts`,
  wired in `eslint.config.ts` over `packages/*/src`) refuses `fetch` without a `signal`, a socket connect (`net`/`tls`
  `connect`, `createConnection`, `Socket#connect`, `Bun.connect`), a Bun child's `exited`, `events.once` on a child,
  socket or stream without a `signal`, a promise settled only from a child's, socket's or stream's callback, and a
  child_process or `Bun.spawnSync` call that waits for the child without `timeout`. Inside the body of lmao's `deadline`
  or `idle` (specs/lmao/01r_transient_errors_and_retry.md, "TypeScript: deadlines") only a `fetch` without the wait's
  signal and a synchronous child are refused. A kept site carries
  `// eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: <what ends the wait>`, and
  `deadline/unbounded-wait-why` refuses a disable of the rule without that reason. The root `no-unbounded-wait` target
  runs the rule's planted fixtures. Rust: each cowshed crate's `clippy.toml` disallows every wait on another process (a
  child's `wait`/`wait_with_output`, tokio `Child::kill`, `Command::output`/`status`, `fork_lock`'s
  `output_locked`/`status_locked`, a unix-socket `connect`, `libc::flock` and `File::lock`/`lock_shared`,
  `waitpid`/`waitid`/`wait4`), each reason naming its bounded form in `cowshed_os::deadline` (`reap`, `reap_async`,
  `output_within`, `run_by_progress` and its `run_to_log` and `output_by_progress`, `connect_unix`) or
  `cowshed_os::flock` (`exclusive`, `queued`). A kept site allows `clippy::disallowed_methods` for its one item or
  statement beside `// WHY unbounded: <what ends the wait>`, and `cowshed:wait-why` (WAIT-WHY,
  `packages/nx-plugin/src/wait-why-policy.ts`, a dependency of `cowshed:lint`) refuses the allow without it, and refuses
  a reason that records `debt:`: that names a stall the site leaves, not an end, so the site is bounded instead. Tests
  wait through `cowshed_os::test_backstop`'s helpers, bounded by the no-progress backstop; a test keeps a raw wait, with
  its WHY, only where a helper thread would change what it measures.

## Disk-lifecycle lease

Disk-changing tools and descriptor-native unmounts cowshed runs on the host — the CLI's, a supervisor's, the gateway's
own startup pass, and a test's — run under a lease the gateway of their host namespace schedules (01_storage.md, "Host
namespace"): production's gateway leases production's disk work and nothing else, and a test namespace's disk work
leases from the gateway at its root, `<ns root>/gateway.sock`, the RAM fixture gateway where one runs, and otherwise
runs unleased. A test therefore never queues on, holds or spaces out production's disk phases. `storagekitd` is a
physical host resource every attach shares: a test bounds its effect on it by attaching only where an image's behavior
is what the test proves (08_testing.md), never by holding production's lease. One client path takes the lease:
`SystemCommandRunner`, built from a namespace, admits disk children by command class and native `funmount` by namespace
class through the same lease client. Setup uses that client too. Mount and unmount retain a registry-proven raw device
pin across their lease wait and effect. Native unmount resolves and proves its current held root after admission,
including when running unleased. Unpinned detach commands recheck positive exact-image/device registry ownership after
the lease wait, immediately before spawning. An attach spends most of its time in StorageKit's `syncAllDisks`, after its
device already exists, and that sync does not finish while the mount table keeps changing (01a_apfs.md, "How the APFS
host degrades"), so attaches stall beside mount churn and stay fast once the two are kept apart. Attaches among
themselves only queue on `storagekitd`; so the lease is two classes that exclude each other rather than N
interchangeable tokens.

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

## RAM disks

A legacy DiskImages RAM disk lives in the `diskimages-helper` process that `hdiutil attach ram://<n>` forks; the helper
calls `setsid` and serves the disk until it is detached. A job's conclusion retires every member that outlives its
leader, `setsid` lives included (11_shell.md, "Process identity and signal ownership"), so a helper a job forked dies
with that job, and the disk with it, under every other job's use of the disk: open files on it read EIO, and its paths
ENOENT.

The helper of a RAM disk a host-shared volume lives on belongs to the gateway, which no job contains; a job's retirement
must never reach it. `ram-lease` forks `hdiutil attach -nomount ram://<n>` from the gateway, through
`SystemCommandRunner` under the storage class of the gateway's own disk-lifecycle lease, as a protected disk-image
command that is never killed. The request names a size and nothing else; no argv, path or option reaches `hdiutil` from
the wire. `sectors` must be a positive integer whose bytes fit in the host's physical memory (`hw.memsize`): a RAM disk
larger than memory is swap. A request with another field, or a size outside that range, is `invalid-request`; an attach
that fails, or a gateway whose daemon supplied no disk runner, is `rejected` with its reason, `hdiutil`'s exit and
stderr included. A gateway on Linux attaches none.

A volume may be reused only with gateway-creation provenance bound to its exact live IORegistry entry. A mounted legacy
volume is not proof of gateway ownership and is never adopted by the new plugin. The production RAM optimization may
report explicit capability or provenance unavailability and run on owned ordinary temporary storage; an I/O failure,
malformed provenance or unreadable pending authority is still an error. Required RAM boundary fixtures never substitute
another medium.

The attach is a lease the requesting connection holds, and every way it ends settles the disk, so no disk outlives every
owner. The gateway keeps its records in `ram-leases/` beside its control socket (mode 0700); each file is written whole
under a scratch name and renamed into place. Before `hdiutil` spawns, the gateway records the attach's intent,
`<pid>-<n>.intent` holding the sectors asked. Right after `hdiutil` answers, on the blocking thread that ran it, the
gateway reads the disk's I/O Registry entry and records the lease, a file named by that entry and holding its whole
disk, then removes the intent; only then does it answer `ramDisk`. An intent that cannot be recorded refuses the attach
before any spawn. A disk whose lease cannot be recorded is detached by its entry. A disk whose entry cannot be read is
never detached by its name, which proves nothing once the registry could not say whose it is; its intent stays. An
attach that failed keeps its intent too, since it may still have made a disk; one refused before its spawn removes it.
Each of these is `rejected`. The client keeps the connection open and writes one end line, which decides the lease
(`cowshed-gateway`'s `ram_lease` core):

- `{"end":"keep"}`: the disk is the caller's from then on. The gateway removes the record and answers `ok: true`. A
  record it cannot remove would make the next startup scan detach a disk its owner uses, so the gateway then detaches
  the disk and refuses the keep as `rejected`.
- `{"end":"release"}`: the gateway detaches the disk and answers `ramDetach` exactly as `ram-detach` does.
- Any other line: the gateway detaches the disk and answers `invalid-request` naming the detach's outcome.
- No line: the connection ended or broke first, its owner dead, killed or gone past its deadline. The gateway detaches
  the disk; nobody is left to answer.

Each detach is the registry-bound one `ram-detach` runs, on a blocking thread no connection owns. The record goes only
once the entry is no longer registered right after the detach: an entry id is never reused, so only its absence proves
the disk gone. An entry still registered, with or without its whole disk (busy, so the unforced detach failed, or not
started), or a registry that cannot be read, keeps its record. A gateway that stops while it holds a lease, its
connections aborted with it, leaves the record; one that stops mid-attach, aborted, killed or past its drain while
`hdiutil` still runs, leaves the intent. The gateway's startup scan reads every record and intent before its control
socket serves, so each is a previous gateway's, and reads then, for each intent's size, the RAM disks of that size this
user attached that nothing mounts. On a blocking thread it detaches each recorded lease's disk, removing the record by
the same rule, and each such disk no record names; an intent goes once every disk of its size it found has left the
registry. A scratch file a writer died before renaming is removed, and a file that names no lease or intent is said and
left in place. The scan is the only retry: a record or intent waits for the next gateway start, never for a timer. The
gateway says every lease it detaches on its stderr, as
`cowshed: ram-lease <device> entry=<entry> <why>: detach outcome=<outcome>; <record>`.
`cowshed: ram-lease <device> entry=<entry> <why>: detach outcome=<outcome>; <record>`.

A caller reads a `ram-detach` answer past its heartbeats under the silence rule of "Deadlines" (60 s before the first
heartbeat, 15 s after each one) and under no total ceiling, so a creator waiting on a stray's detach waits as long as
the detach runs. A silent gateway ends that wait `SILENT`, naming the socket, the bound, the time waited and the last
progress; the detach's custody stays in the volume's ledger until its late terminal answer or its entry's absence.

A kept disk's caller owns the volume on it and detaches the disk only through `ram-detach` naming its entry and whole
disk. It never detaches one through `disk-command`, and never with `-force`. The helper's own session keeps it out of
the gateway's process group, so a gateway restart leaves a kept disk attached. A volume counts as its own only the disks
of its uid and size that carry its exact volume name at its mountpoint or staging, or whose registry entry its ledger or
a receipt records, the ledger's record of the disk a creator kept included (written before the keep, so a volume
unmounted from under its marker leaves a disk the record still names); it reclaims those strays and ignores every other
disk, so a disk of another namespace, a test's, is never taken for production's.

`ram-detach` is the gateway's other RAM disk mutation and has the kept disk's owner. The gateway forks
`hdiutil detach <device>` through `SystemCommandRunner`'s registry-bound detach under the storage class of its own
disk-lifecycle lease, as a protected disk-image command that is never killed. Inside the grant, immediately before the
spawn, the runner reads `registryEntry` again and spawns only while that entry still registers `device` as its whole
disk, so a name recycled while the request queued is never detached. The request names that pair and nothing else:
`device` is a whole disk, `/dev/disk<N>`, and `registryEntry` a nonzero entry id written `0x` and lowercase hex without
leading zeros. Any other field or form is `invalid-request`, which dispatched nothing. There is no force: a disk still
in use stays attached, and its caller keeps the staging it owns.

Once dispatched, the detach runs on a gateway blocking thread that no connection owns: the requester's disconnect, its
death and its job's retirement never cancel or signal it, and the gateway reaps the exact `hdiutil` child before it
answers. The answer is `ok: true` with `ramDetach`, whose `outcome` says what is known:

- `terminal`: `hdiutil` ran and was reaped. `status` is `{"exit":<code>}` or `{"signal":<number>}`; `stdout` and
  `stderr` are its output as lossy UTF-8, each cut to the 64 KiB inline output frame (`MAX_INLINE_OUTPUT_BYTES`,
  07_api.md) with `stdoutTruncated` and `stderrTruncated` saying whether it was. Only exit 0 detached the disk.
- `not-started` with `reason`: refused before any spawn, so nothing was detached. The entry no longer registers `device`
  or could not be read; Disk Arbitration would put the release behind an administrator prompt or cannot read the disk's
  owner; the spawn failed; the gateway has no disk runner (a gateway on Linux detaches none); or the blocking thread was
  cancelled before it ran.
- `unknown` with `reason`: `hdiutil` may have run, but its end was not observed: a wait or reap failure, an end with
  neither exit status nor signal, or a blocking thread that panicked. The caller fences the disk as possibly detached
  and settles it by the entry's presence in the registry, never by assuming either end.

A requester that receives no answer knows no more than `unknown`. The gateway says every outcome on its stderr, as
`cowshed: ram-detach <device> entry=<entry> outcome=<outcome>` followed by the status or the cause.

The regression is `cowshed-gateway`'s `a_ram_disk_the_gateway_attaches_outlives_the_job_that_asked_for_it`: a fixture
gateway on its own test namespace's socket attaches a disk of a size no other process uses for a job the test's process
monitor owns. When the job concludes, its retirement signals no `diskimages-helper` and the disk stays registered; the
same attach forked inside the job is retired with it and the disk leaves the registry. Only the fixture's own disks are
detached. The detach's are `cowshed-gateway`'s `ram_detach` tests, each on a fixture gateway of its own with disks only
it attached: `a_ram_detach_outlives_the_requester_that_asked_for_it` kills a job's requester and retires its job while
the detach waits behind a namespace phase the test holds; the retirement signals no `hdiutil`, and the disk leaves the
registry once the phase drains. `a_ram_detach_refuses_an_entry_that_does_not_register_the_device` swaps the entries of
two fixture disks, answers `not-started` for both and leaves both registered, then detaches one by its own entry while
the other stays registered under the same whole disk. The lease's decisions are `ram_lease`'s table tests, and its
connection boundary is `control`'s `ram_lease` tests on in-memory disks: an owner gone, kept, released or garbled, and a
startup scan of a previous gateway's record. The actual runs are `cowshed-gateway`'s `ram_lease` tests on real fixture
disks: `a_ram_lease_whose_owner_ends_without_keeping_it_leaves_the_registry` ends a lease with no line and finds the
disk gone once the gateway closes the connection; `the_next_gateway_detaches_a_lease_a_stopped_gateway_held` drains a
gateway holding a lease, finds the disk still registered, and starts the namespace's next gateway, whose startup scan
detaches it.

## Host scheduling

The kernel schedules runnable threads and their I/O. Resource admission has one authority: with mbx present on the host,
mbx's machine-wide pool is the CPU and measured-memory ledger; without it, cowshed's existing mechanism is unchanged:
per-job jobservers and the Nx/bounded-exec cpu-pool, not a host token ledger. Class placement still tells the kernel
whose runnable threads take precedence ("Classes"). The owner's ruling, 2026-10-27, selects this boundary.

### Pool selection and revision

**Present on the host** means the mbx installed with the running cowshed build is available at that build's pinned git
revision (14_nix.md, "mbx"). An arbitrary `mbx` on the operator's PATH does not select the host scheduler. An installed
mbx that cannot be executed or whose revision cannot be proven is an admission failure, not absence. With no installed
host mbx, the non-mbx behavior below applies unchanged.

A Cargo project whose canonical environment resolves mbx while the host installation is absent refuses job admission as
`mbx-host-missing`, naming that resolved executable and the remedy `cowshed setup`. Doctor reports the same finding.
Running that Cargo pool beside the non-mbx mechanism would violate the single-ledger boundary.

The one pool is the pool of the shared store (03_caches.md, "mbx"). All Cargo sessions and external reservations use the
same cache directory and the same host CPU/memory settings, not per-project or per-job copies. mbx owns that pool's
lifecycle; cowshed starts no pool daemon and keeps no second resource ledger. One pool preserves single-flight:
identical compilations in different sheds wait for one producer and restore its result, rather than compile twice. mbx
documents both the pool and in-flight registry as cache-directory scoped
([Machine-wide scheduling](https://mr-boxington.jdx.dev/how-it-works#machine-wide-scheduling)).

The installed mbx's host-global settings retain memory scheduling. `scheduler.memory = "none"` is the typed host finding
`mbx-memory-unscheduled`: doctor reports it and mbx-mode admission refuses with the remedy to set a memory budget. It
never selects a CPU-only pool or cowshed fallback, because that would discard the measured-memory ledger.

A Cargo project's mbx capability resolves its executable in the canonical job environment (15_capabilities.md, "mbx
detection"). The proof of the pin is executable identity: its realpath must equal the realpath of the mbx shipped with
the installed cowshed build. Both are derived at detection, never stored; on Nix the resolved store path identifies the
pinned build. Equal `mbx --version` release strings alone prove nothing. A mismatch refuses admission, naming the host's
expected revision, both executable paths, both `mbx --version` outputs and the remedy: align the project's devenv pin.
Any actual revision established by the project's lock is reported too, never inferred from a release version. A pin
whose revision cannot be established from that identity is the same skew finding as a mismatch, and refuses admission
the same way: detection never fails open, so an unprovable pin yields no capability answer that external reservations
could admit either. Admission never selects another pool or falls back to cowshed's cpu-pool for that project. A project
without the capability does not acquire it merely because the host has mbx; its work uses external reservations under
"Parallelism inside a job".

**Enforced by:** the host-selection, revision-skew and one-pool tests under "Required scheduling evidence"; those tests
must exist for Phase 2.

### Classes

A requester names its job's class in the exec request: `ExecClass`, one of `interactive`, `gate`, `background`,
`resident` or `interactive-resident`. The CLI's `cowshed exec --class` and the API's `class` field are that one typed
value (07_api.md, "CLI and API exec parity"), so an embedding executor declares a class exactly as the CLI does. Only
the trusted coordinator/operator route may ask `gate`; a worker that asks it is refused with typed class-not-permitted
before any job or spawn effect. Ordinary commands default to `background`. A class is fixed at the job's birth; no live
transition moves a running job between classes. The table owns kernel placement and the non-mbx pressure policy. With
mbx selected, all classes use its admission and pressure policy ("Memory pressure"); the land gate has
`MBX_SCHEDULER_PRIORITY=normal`, and every other class has `low`, including interactive and resident work. The same
mapping supplies `mbx reserve --priority`. Kernel nice and cgroup placement do not create another pool.

| Class                  | What runs in it                                                                                                       | Nice | Linux cgroup                                                                 | Without mbx: waits on memory pressure | Without mbx: ended at macOS Critical |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------- | ---- | ---------------------------------------------------------------------------- | ------------------------------------- | ------------------------------------ |
| `interactive`          | work a person or model waits on, declared by the requester that awaits its result                                     | 0    | `interactive`                                                                | never                                 | never                                |
| `gate`                 | a land's gate job, a train's proof and the jobs a land's own steps start ("A land is a gate job"); trusted route only | +5   | `gate`: `memory.low` max                                                     | never                                 | never                                |
| `background`           | every other exec, the default                                                                                         | +10  | `background`: `memory.high` ½ host, `memory.max` ¾ host, `memory.swap.max` 0 | yes                                   | youngest first                       |
| `resident`             | a process idle between the requests it serves: a shed's Nx daemon, a worker that runs each request it is sent         | +10  | `background`                                                                 | never                                 | youngest first                       |
| `interactive-resident` | a resident whose requests a person or model waits on, such as an embedder's feedback Nx worker                        | 0    | `interactive`                                                                | never                                 | never                                |

Nice decides whose runnable threads win a core when more are runnable than there are cores: an interactive thread before
a gate thread, and a gate thread before a background one. On an idle host every class runs at full speed. Nice is never
negative. On macOS, where it is each process's priority, raising one above the supervisor's own takes a privilege no
supervisor holds, so the order is made by lowering it: interactive at the supervisor's own nice, gate five below it,
background ten below it. On Linux the table's value itself is the class cgroup's `cpu.weight.nice`, a weight: one step
is about 55:45, so five steps are about 3:1, and interactive, gate and background share contended cores about 9:3:1. A
resident costs no runnable CPU while idle; with mbx selected, its external reservation still lasts for its admitted
work.

**Linux.** Every job runs in its own cgroup v2, `<class>/job-<incarnation>-<id>` under the supervisor's delegated root
(07_api.md, "Process-tree observations"); the incarnation is in the name because job ids repeat across workspaces. There
is one class cgroup per nice of the table — `gate`, `interactive` (with interactive residents) and `background` (with
residents) — beside the controller's own `controller` leaf, made and written when the supervisor opens its delegation,
and shared by every workspace. The class sits directly under the root because a weight compares siblings only: there the
gate's weight meets all background work together, about 3:1 however many background jobs run, where a level per
workspace would rank a gate in one shed only against its own shed's jobs. The supervisor enables the `cpu`, `memory`,
`io` and `pids` controllers in the `cgroup.subtree_control` of the root and of each class cgroup (`io` for the `io.stat`
accounting of 07_api.md) and writes:

- on each class cgroup, the class's nice as `cpu.weight.nice`; a job writes no weight of its own, so the jobs of one
  class share its weight equally;
- on the `gate` class cgroup and on each gate job, `memory.low` `max`: the gate's pages are reclaimed last, and the
  protection holds exactly the pages the gate holds, nothing idle. Protection flows down only through cgroups that hold
  it (unless cgroup2 is mounted `memory_recursiveprot`), so each job carries it too, and it reaches only as far as the
  delegated root's own `memory.low` (systemd `MemoryLow=`) lets it;
- on the `background` class cgroup, `memory.high` at half the host's physical memory, `memory.max` at three quarters of
  it and `memory.swap.max` 0, bounding background and resident work as a whole. Past `memory.high` the kernel throttles
  and reclaims it and never kills it, so background work does not grow past half the host unchecked; `memory.max` is the
  kernel's kill backstop, inside the background class only, before the host and the gate run out; and background work
  never swaps the host;
- on every job but a gate's, `pids.max` 16384, far above a cargo or nextest tree on a large host, so a fork bomb fails
  with `EAGAIN` inside its own job; a gate is never limited (08_testing.md, "The landing gate is never CPU-limited").

`cpu.max` is never written: a quota strands idle cycles exactly as tokens did. No `io.weight` is written: proportional
I/O control needs `io.cost.qos`, which only root may configure. A controller the delegation lacks, or a file the
supervisor cannot write, refuses the job's admission with a typed error naming it and removes the job's empty cgroup: a
job never runs unplaced (07_api.md). No per-process nice is placed on Linux: inside a cgroup it would order only threads
within that group, and the class cgroup's weight is what the host sees.

**macOS.** macOS has no cgroups, so the class is the job's nice, `setpriority(PRIO_PROCESS)` alone, placed on the job's
first process and read back before the command runs (11_shell.md, "Job admission and class placement"). The Clutch
scheduler runs threads of a higher base priority first and bounds a lower bucket's lateness, so a background thread at
+10 is late but never starved, and +10 (base priority 21) stays in the non-UI band, eligible for performance cores.

**On macOS nothing cowshed starts is throttled for I/O.** No process cowshed spawns that opens a path under the store or
a workspace runs under `PRIO_DARWIN_BG`, a QoS clamp (`taskpolicy -b`, `taskpolicy -c utility|background`) or a disk I/O
policy below IMPORTANT, and every cowshed launchd agent is `ProcessType=Interactive`. Those mechanisms throttle the
process's disk I/O along with its CPU, and a throttled thread sleeps while it holds work other writers wait behind:

- 2026-10-22 (57b1c81bc0): the image-freeing thread under `IOPOL_THROTTLE` slept on 1,213 of 1,963 metadata reads at a
  20.1 ms median; an unlink took 53.1 s against 7.6 s at normal policy, and a probe writer's p50 rose from 0.2 ms to
  10.6 ms (01a_apfs.md, "Freeing an image").
- A launchd `Background` process type made a 0.7 s shell loop take 1 to 30 s and `direnv export json` take 537 s at load
  80; a `utility` clamp made a cold SQLite open take 34.5 to 54.5 s against 2.4 to 2.7 s.
- 2026-10-16: the landing gate took 40 minutes under `taskpolicy -b` with `--parallel=2` against 4m44s at default
  priority (08_testing.md, "The landing gate is never CPU-limited").

### Memory pressure

**With mbx, mbx owns memory admission.** Compilers charge CPU and predicted memory independently, using mbx's measured
peak history; those are scheduling estimates, not OS memory limits. Live pressure control holds further admissions, not
running processes. Cowshed adds no pressure queue or Critical-pressure kill loop in this mode. Existing kernel placement
and memory backstops remain as specified under "Classes". External-work charges are specified under "Parallelism inside
a job". The signals and prediction policy belong to
[mbx's scheduling contract](https://mr-boxington.jdx.dev/scheduling#machine-wide-compile-scheduling), not a second
cowshed implementation.

**Without mbx, a background start waits while the host reports memory pressure**: on Linux while `/proc/pressure/memory`
reads `some avg10` above 10.00%, on macOS while `kern.memorystatus_vm_pressure_level` reads Warning or Critical. The
supervisor is told the pressure by the kernel, never polls it ("Admission"): it keeps the job `queued` until the
kernel's report lets it start. The wait has no deadline and is never a refusal: it ends when a report drops below the
threshold or the requester kills the job. The job's stderr says so, with the reading, as
`cowshed: jobserver wait start queue=memory pressure=<reading>` and then
`cowshed: jobserver wait done elapsed=<waited> status=ok|err`. A reading that cannot be taken fails the job (`failed`,
failure `cpuAdmission`, naming the file or sysctl and its error): a host whose pressure cannot be read is not shown to
have room. Gate, interactive and either resident never wait; a resident's start is small and its requests are what a
person or the land waits on.

**Without mbx, at Critical, macOS ends background work.** macOS charges no job tree to a group the kernel could reclaim
or kill as one, so the supervisor does it. When the kernel reports Critical, each workspace supervisor ends the youngest
of its own running `background` and `resident` jobs through the ordinary kill, reported `failed` with failure `memory`,
its stderr naming the pressure level and the job's resident sum from its latest sample. A supervisor ends one job at a
time: while a job it ended for memory has not concluded, its grace still running, it ends no other, and once that job
has concluded while the last report is still Critical, it ends the next youngest, so each step sees what the last ending
freed. Gate, interactive and interactive-resident jobs are never ended. Linux runs no such loop: its kernel throttles
the background class at `memory.high`, kills inside it at `memory.max`, and `memory.swap.max` 0 keeps it from swapping
the host.

Measured 2026-10-26: six acceptance runs of one repository started together reached load 23 on 18 cores and put 5.9 of 7
GB in swap; a CPU-only budget did not account for that memory. The non-mbx pressure policy above remains the fallback,
while mbx supplies measured-memory admission when present. An ended run is not retried automatically.

### Admission

A workspace supervisor's admission is one state machine, `cowshed-core`'s `runtime::job_admission`: pure, with no clock,
file or kernel of its own. The supervisor feeds it events, each stamped with the instant it observed, and carries out
the effects it answers, in order.

- **States.** A job is `queued` until the host's pressure is known, `waiting` while memory pressure holds it, `admitted`
  once it may start, `running` from its first process's dispatch, `ending for memory` once Critical pressure chose it,
  and concluded when it ends, which takes it out of the ledger.
- **Events.** A start request (job, class, and whether the project has the mbx capability), a pressure report, a spawn,
  a cancel and a job's end. A cancel removes a `queued` or `waiting` job at once; an admitted one leaves at its end.
- **Effects.** Admit (make the jobserver and spawn), the wait's start and end lines, refuse (`cpuAdmission`, an
  unreadable host) and end for memory.

The policy it holds:

- **Gate before background.** A gate, interactive work and residents are admitted at once whatever the host reports, and
  are never ended for memory; only a background start waits. With mbx every class is admitted at once, and mbx's pool
  orders the work by priority: `normal` for a gate, `low` for everything else ("Classes").
- **Fairness between checkouts.** Each supervisor's ledger holds its own checkout's jobs; no job waits on another job's
  tokens, so there is no cross-checkout queue to order. A drop in pressure releases every waiter of every supervisor
  that hears it, oldest first within each. Contended CPU is shared by the kernel's class weights; with mbx, across
  checkouts, by mbx's one pool, which admits each compile and test.
- **Long holders.** A job's jobserver holds its own constant count `g`, never a share of a host ledger, so a grant made
  on an idle host cannot hold a later start, however long its holder runs. This closes the 2026-10-19 hole, in which a
  grant made while the host was idle never shrank, for cowshed's ledger in both modes (enforced by
  `a_long_holder_admitted_on_an_idle_host_never_holds_a_later_start`). Inside an mbx project, mbx admits each rustc and,
  with `MBX_SCHEDULER_TESTS=1`, each test process, so a long `cargo nextest` gives its grant back as each one exits. A
  non-Cargo leaf's `mbx reserve` holds its `--cpus g` until the leaf concludes ("Tradeoff" under "Status"): the hole
  stays open there, bounded by one leaf and its own `g`.

**The kernel tells the supervisor the pressure.** Each supervisor opens one feed (`host_pressure::Feed`) when it starts:
one reading, then a new reading each time the kernel notifies a change. On macOS the notification is libdispatch's
memory-pressure source (`DISPATCH_SOURCE_TYPE_MEMORYPRESSURE`, every level), and each notification reads
`kern.memorystatus_vm_pressure_level` again. On Linux it is a PSI trigger on `/proc/pressure/memory`,
`some 1000000 10000000` (1 s of stall in a 10 s window, the 10% threshold above). PSI notifies a rise only, so while the
last reading holds a start the feed reads again at each window's end, the first moment a drop can show; an unpressured
host is read again only when the trigger fires. A kernel that refuses the subscription leaves a feed whose one reading
names why, which refuses every waiting start. Nothing samples the pressure on a timer.

Enforced by the table tests in `runtime::job_admission`, one per rule above, and
`a_background_start_waits_for_the_reported_drop_and_a_gate_does_not`, which drives a supervisor through a test's
pressure feed.

### Parallelism inside a job

`g = min(want, cores)`, with `cores` from `std::thread::available_parallelism`, remains cowshed's sizing rule. `want` is
the job's `--want`, or `cores` when absent; every class uses the same rule and a brief command may request fewer.

**With mbx, Cargo and non-Cargo leaves enter the same pool once.** A Cargo leaf runs through mbx under its job's
priority, with `CARGO_BUILD_JOBS=g` as that build's CPU ceiling, not an enclosing resource reservation. It takes no
cowshed host grant; mbx admits the actual compiler and supported test work. It keeps its job's jobserver fifo: the fifo
bounds one build tree's own fan-out at the same `g`, never host capacity, so it is no second ledger, and Cargo reads
`MAKEFLAGS` as well as `CARGO_MAKEFLAGS`, so withholding one would bound nothing. A non-Cargo leaf runs under
`mbx reserve --cpus g --priority <job-priority> -- <command>`, the host's installed mbx invoked through its link beside
the installed cowshed (14_nix.md, "mbx"), `g` being the job's own count from its `--want`, never a host figure. The
grant is the job's admission: its command stays queued until mbx grants it, then its existing jobserver, runner flags
and thread variables are bounded by that grant. The memory request is the same task identity's last recorded resource
peak from existing LMAO job/process spans (13_telemetry.md, "Job resource peaks"), passed as `--memory <bytes>`; no
cowshed peak ledger or prediction table exists. [INFERENCE] The task-identity key needed to select that span is not
verified for Nx leaves; until the span carries it, or when no prior peak exists, `--memory` is omitted. mbx defines an
omitted memory request as a zero-byte charge, not a measurement or a memory guarantee. This open keying question does
not authorize a second store (AGENTS.md, rule 8).

The boundary is the actual leaf runner (`bounded-exec` and equivalent direct runners), not an enclosing orchestration
command. An Nx run mixing Cargo and non-Cargo leaves holds no reservation of its own: Cargo leaves use mbx, non-Cargo
leaves reserve, and local fan-out consumes only the admitted leaf's grant. `mbx reserve` does not discover Cargo
children. Reserving around an orchestrator and then letting its Cargo children acquire again would double-charge and can
deadlock, so that path is not used. Nested non-Cargo work shares its enclosing grant instead of reserving twice. The
seven unscheduled Cargo test-runner cases are an explicit exception with job-local bounds, not another hidden pool
(11_shell.md, "Cargo test-runner boundaries" and "Open questions"). The external-work interface belongs to
[mbx reserve](https://mr-boxington.jdx.dev/scheduling#reserve-capacity-for-external-work).

**Without mbx, every job receives its own GNU make jobserver fifo**, served by its supervisor at the constant count `g`.
The fifo bounds the compiler and test threads one build tree starts, not host-wide capacity. Two jobs may each hold
`cores` tokens, and the kernel weighs their runnable threads by class. This non-mbx mechanism remains unchanged. The
remaining bullets specify that mechanism and the local sizing inside an external reservation; they do not add another
host grant around mbx Cargo work.

Measured 2026-10-21 under a default of one token: an `nx run-many` under `cowshed exec` handed that one token to its
runners in turn, which waited 261, 473, 664 and 887 s for it, and the run took 21m19s against 5m54s on the host route.

- **Without mbx, Cargo reads its holder's jobserver.** A job's count reaches its children as configuration Cargo and its
  test runners already read; cowshed installs no Cargo interceptor ("The installed cowshed"). The command receives
  `CARGO_MAKEFLAGS` and `MAKEFLAGS` set to `-j --jobserver-auth=fifo:<path>`, `BOUNDED_EXEC_CPU_TOKENS=g`, and
  `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` set to `g` where it named neither: libtest and nextest test threads
  take no jobserver token, so without them a `cargo test` still runs one test thread per core. A `--test-threads` count
  the command names outranks them, as a flag outranks the environment. The supervisor sets these over a caller's own
  `CARGO_MAKEFLAGS`, `MAKEFLAGS`, `MFLAGS` and `BOUNDED_EXEC_CPU_*`, which it never passes. It also exports
  `NX_PARALLEL` as `g` (a caller's lower count kept, a percentage replaced) and `COWSHED_NX_PARALLEL`, which
  `.cowshed/env` restores after a devenv shell sets its own, so Nx's task fan-out is the job's too; an explicit
  `--parallel=N` stays the caller's. Measured 2026-10-26: `cowshed exec <ws> -- nx run-many` on 2 tokens ran 11 task
  processes at once (16 GB RSS, host load 39), since the devenv shell's `NX_PARALLEL=100%` let Nx start a task per core.
  Cargo, every build script and every nested `cargo` a test starts inherit these and run rustc only on tokens read from
  that fifo; `cargo metadata` and every query read none, so nothing needs to tell a build from a query.
- **The fifo is the job's free count.** The supervisor creates it as
  `<store>/run/cpu/<base64url-incarnation>/job-<id>.fifo`, mode `0600`, in the workspace's pool directory, which the
  sandbox opens for reading and writing fifos and nothing else (04_sandbox.md). It opens it `O_RDWR`, keeps that
  descriptor for the job's life, so the bytes survive while no reader has it open, and writes `g − 1` token bytes: the
  command holds the implicit first token, as make's protocol has it. At the job's end it closes and unlinks the fifo,
  and a supervisor's start removes every fifo left in its pool directory. A Cargo that outlives the fifo keeps its open
  descriptor and runs on unbounded until it ends; one started after finds no fifo and runs unconfigured. The incarnation
  component is the unpadded 22-byte base64url encoding of all 128 bits of the validated workspace incarnation, never a
  digest or prefix; the sandbox builder and the supervisor derive this one layout through the same helper.
- **A nested exec shares its parent's fifo.** A `cowshed exec` from inside a job whose inherited `CARGO_MAKEFLAGS` fifo
  lies in the new job's own workspace pool directory reuses that fifo and its `BOUNDED_EXEC_CPU_TOKENS` count, so a
  test's child build reads the test run's tokens and waits only for a sibling's thread, which is the jobserver's
  intended behaviour inside one tree. An exec into another workspace, or with no inherited fifo, gets its own.
- **No holder, no cowshed jobserver.** A Cargo no job started — a host shell's or rust-analyzer's — receives no job
  count. With the mbx capability it still uses the canonical mbx environment and one shared pool (11_shell.md,
  "Canonical mbx environment"); without it, Cargo's own or a parent make's jobserver behavior applies.
- **A fifo the command cannot open is reported, never silent.** A command that cannot open its fifo — a sandbox profile
  that does not admit it, a fifo gone — runs, and every Cargo it starts runs without the jobserver. Cargo says so itself
  on stderr, `failed to connect to jobserver from environment variable CARGO_MAKEFLAGS=… (os error N)`, naming the fifo
  and the errno, and nothing of cowshed's filters that line. A warm exec host, the first process inside the sandbox,
  opens the fifo read-write and non-blocking once before each command and on failure writes
  `cowshed: the job's CPU jobserver fifo <path> cannot be opened in the sandbox (<error>); Cargo will run without it` on
  the job's stderr; the command still runs. A one-shot child runs no cowshed code inside the sandbox before its command,
  so Cargo's own line is its report.
- **Without mbx, every Nx run budgets itself.** The `@smoothbricks/nx-plugin`'s `preTasksExecution` serves every Nx run
  that inherits no `BOUNDED_EXEC_CPU_POOL` a CPU pool of its own, `min(inherited BOUNDED_EXEC_CPU_TOKENS, cores)`
  tokens, `cores` when it inherits no count: a socket published as `BOUNDED_EXEC_CPU_POOL` beside
  `BOUNDED_EXEC_CPU_TOKENS`, and a make jobserver fifo published as `CARGO_MAKEFLAGS`/`MAKEFLAGS`, in the environment
  every task inherits. Nx still starts the run's targets side by side; their runners queue for the run's tokens instead
  of each taking the host. Measured in nightly 477, `nx run-many -t mutation` under `NX_PARALLEL=100%` with nothing
  budgeting the run: Stryker started 31 workers per package and cargo-mutants a jobserver of one task per core, side by
  side, and Stryker for three packages and a fourth target were SIGKILLed within 20 seconds. The pool serves its own
  FIFO queue, granting the head `min(want, free)` once one token is free, every grant whole: no request holds part of a
  grant while it waits for more, so no two deadlock on each other's tokens. Its fifo holds the free count: a socket
  child takes the fifo's bytes first and the implicit token last, and its close writes them back, so a Cargo reading the
  fifo and a nested runner holding a grant never count one token twice. A granted `bounded-exec` serves its grant the
  same way, as a pool its own nested runners take from; no runner asks the gateway. A pool's end ends accounting, never
  work: a command whose lending parent exits runs on to its own exit and is judged by that exit alone. The run's pool
  runs on a thread of its own ("Deadlines"); the run's `postTasksExecution` closes it, and a run that dies first takes
  it with its process. The plugin's `run-cpu-pool.test.ts` holds that shape over a real socket: two sweeps each asking
  31 of a 4-token run pool, the first granted 4 and the second queued until the first returned them. Its
  `cpu-pool.silence.ts`, which the `test-pool-silence` target runs, queues a runner in another process on a 1-token run
  pool and blocks the thread that served the run for the silence bound plus a heartbeat (20 s): the runner stays queued
  and is granted once the holder returns its token.
- **Runners are sized to the count.** The `bounded-exec` executor reads each runner's requested parallelism from the
  runner's own flag rather than assuming the core count, since a runner's flag and its actual concurrency can disagree:
  a `nextest run` names `--test-threads` or defaults to one per core, and one with `--no-capture` (`--nocapture` past
  `--`) runs its tests serially whatever count it names, so it asks for one; `bun test --parallel=N` names N workers, a
  bare `--parallel` defaults to one per core; `stryker run` names `--concurrency`/`-c` N or N% of the cores and defaults
  to one worker per core less one past four cores; `vitest` names `--maxWorkers` N or N% and defaults to one per core
  less one, one with file parallelism off; `go test` names `-p` and defaults to `GOMAXPROCS`; `cargo mutants` asks for
  its own jobserver's tasks, `--jobserver-tasks` or one per core; a cargo build (`cargo build`/`test`/`clippy`/…,
  `nextest archive`, `napi build`) defaults to one job per core; anything else defaults to one. A runner's flag counts
  only in its own invocation's arguments, up to the next `;`, `&`, `|` or parenthesis outside quotes and `$(…)`; a value
  that is no count (`-p="$(nproc)"`) leaves the default. Positive inherited `NEXTEST_TEST_THREADS` and
  `CARGO_BUILD_JOBS` are ceilings on their runners, and `want` is the lesser of the request and its ceiling. The
  executor asks its inherited pool for `want`, and runs its command at `min(want, granted)`: it sets
  `NEXTEST_TEST_THREADS` and rewrites nextest's named `--test-threads=`, rewrites Bun's `--parallel=`, sets
  `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` for a cargo runner and any other command under its ceilings, and
  rewrites Stryker's `--concurrency=`, Vitest's `--maxWorkers=` and go's `-p=` where named or adds them right after the
  invocation, so a config file's count cannot outrank it. A Cargo needs no count of its own: it reads the pool's fifo,
  so no `CARGO_BUILD_JOBS` is set. cargo-mutants makes a jobserver of its own and hands it to every cargo it starts, so
  it gets `--jobserver-tasks=` that limit, its named `--jobs` and `CARGO_MUTANTS_JOBS` lowered to at most it, and each
  of its cargo processes `⌊limit / jobs⌋`, at least one, as `CARGO_BUILD_JOBS`, `RUST_TEST_THREADS` and
  `NEXTEST_TEST_THREADS`; `--jobs` is never added, since cargo-mutants refuses it beside `--in-place`. Every command
  receives its count as `BOUNDED_EXEC_CPU_TOKENS`; a target's `options.env` cannot name these variables. The executor's
  `timeoutMs` and `idleTimeoutMs` start when its command starts, after its pool grant: a wait for the pool is not the
  command's time.
- **A runner no jobserver sizes.** A count bounds what reads its fifo and its test variables, nothing else.
  `cargo mutants` hands every cargo it starts a jobserver of its own in place of its holder's fifo; Stryker, Vitest and
  `go test` start their own workers and read no make jobserver at all. Such a runner therefore runs under
  `bounded-exec`, whose runner kinds rewrite its own flag from the count: the plugin's inferred `mutation` target runs
  `cargo mutants` there, and a repository's declared Stryker, Vitest and `go test` targets do the same.

### Gates and lands

- **A land is a gate job.** The land turn's holder, a declared land (02_workspaces.md "A declared land"), runs its
  repository's `[land] gate` (`tooling/land-host.sh --gate <ws>` in SmoothBricks), the only path that gates a tree
  before main fast-forwards to it, and the gate runs as `cowshed exec <workspace> --class gate --host -- …`. The trusted
  route alone grants the class (class-not-permitted for a worker). The jobs a land's own cowshed steps start are
  gate-class work too, since the land waits on each of them: each `cowshed land --check` command (02_workspaces.md,
  "`cowshed land`" step 2).
- **One land per repository is the land turn's.** A repository's lands are serialized by its land turn ("Land turns"),
  which a land holds from before its gate to its fast-forward; the gateway keeps no gate order of its own. A
  `gate`-class job is not a lock: kernel placement and mbx priority follow "Classes", and only the land turn decides
  which land moves main. Resource admission does not serialize lands of different repositories.
- **A train's proof is gate-class work.** A train is the repository's next land: its owner proves the train's tree
  before asking for the land turn, so the land's own gate replays that proof from the Nx cache. That proof is the land's
  gate run early, and it runs as one: `cowshed exec <train> --class gate [--host] -- <gate command>`, from the trusted
  route that alone grants the class. It takes no land turn, since nothing of it moves main. Ordinary unit work of a shed
  that is no train stays background.
- **Nothing else yields to gates.** No fork, attach, rebase, removal, release cleanup or other job waits for a gate to
  finish. With mbx, low-priority work continues through the same pool while normal-priority gate work waits or runs; the
  admission policy belongs to mbx and never preempts an admitted reservation. Without mbx, the existing class placement
  applies. A gate has no quiet window and creates no additional hold.
- **A job lives with its requester.** A foreground job's only consumer is the process that asked for it, so when that
  process is gone the job stops. A gate job's verdict moves main only through the `cowshed land` its lander runs next,
  so a gate whose lander is gone authorizes nothing and only loads the host. Measured 2026-10-18: a land of another
  repository stopped while gating left `nx run-many --parallel=18` running at gate class with no lander. The same holds
  for background work. Measured 2026-10-19: an agent's tool timeout killed three `cowshed exec` clients, and their
  sandboxed jobs ran on under the supervisor for 1h04, 61 and 33 minutes, loading a quarter of the host and reading as
  work. So every foreground `cowshed exec`, whatever its class, holds its job for as long as the exec process lives
  (11_shell.md, `hold`): when that process ends before its job — `SIGKILL` included, since the kernel closes its
  connection — the supervisor kills the job. `--timeout` releases the hold before it leaves the job running, and
  `--background` takes none: a caller who asks for an unattended job keeps it. A job that cannot be held is killed and
  its exec fails, rather than run for nobody.

### Status

`cowshed gateway status` identifies the active mechanism and reads its authority, never reconstructing a second ledger.
With mbx, it reports the host's expected pinned revision, the shared cache directory, and the pool's live leases,
waiters and CPU/memory charges through mbx's published inspection interface (`mbx stats --scheduler --json`). Unknown or
unavailable observations stay unknown and name the error; the gateway neither converts them to zero nor admits work
through a fallback pool. Scheduler estimates and activity remain mbx-owned
([Machine-wide compile scheduling](https://mr-boxington.jdx.dev/scheduling#machine-wide-compile-scheduling)).

In either mode the host reports the 1-, 5- and 15-minute load averages (`getloadavg`), memory pressure (macOS
`kern.memorystatus_vm_pressure_level`; Linux `/proc/pressure/memory` `some avg10`), swap in use against its total (macOS
`vm.swapusage`; Linux `/proc/meminfo`), and jobs by class and state from each live supervisor. It also reports cleanup
entries from their authoritative `.deleting` directories ("Deleting released images"; 16_build_volumes.md, "Garbage
collection") and land turns ("Land turns"). A failed reading names its error, and a supervisor that does not answer is
named with its jobs excluded from counts. `--json` carries the projection as `scheduling` beside `landTurns` (07_api.md,
`GatewayStatus`).

**Tradeoff.** A non-Cargo reservation holds its stated capacity until the owned command concludes, even while it sleeps.
Its last observed peak is not a prediction or an OS limit, and a first or unidentified run has the zero-byte charge
specified above. Cargo's fine-grained measured admissions and single-flight are why cowshed delegates that work instead
of reserving one job-sized envelope around a build. Without mbx, the existing kernel-priority and pressure policy
provides no host-wide CPU reservation.

### Required scheduling evidence

For Phase 2, the following cowshed-owned tests must exist:

- **`host_mbx_selection_uses_the_installed_pin`** distinguishes no host installation, the matching installed build, a
  broken installation and an unrelated ambient PATH program; absence keeps non-mbx projects unchanged but refuses a
  project with canonical mbx as `mbx-host-missing`, with its executable and `cowshed setup` remedy.
- **`mbx_mode_requires_a_memory_budget`** maps host-global `scheduler.memory = "none"` to `mbx-memory-unscheduled`, the
  set-memory-budget remedy and no spawned work, CPU-only pool or fallback admission.
- **`mbx_revision_skew_refuses_admission`** checks realpath identity even when release versions match, both executable
  paths and version outputs, the expected pin and align-devenv remedy, with no second pool, fallback or spawned command.
- **`mbx_priority_follows_the_job_class`** table-checks the Cargo environment and reserve priority against "Classes".
- **`external_work_reserves_its_existing_want`** checks `--want` capping, exactly one `--cpus g` reservation, the last
  matching span's peak as `--memory` or its explicit absence, the bounded jobserver and runner count, and release only
  after the owned command tree concludes. The peak selection reads existing spans and writes no second store.
- **`mixed_nx_leaves_take_one_pool_grant_each`** drives one Cargo leaf and one non-Cargo leaf through the real cowshed
  admission/runner boundary against mbx's published interface; the orchestrator reserves nothing, Cargo uses the
  compiler pool, and the external leaf reserves once. Nested work never reacquires an enclosing external grant.
- **`mbx_status_reads_the_pool_authority`** maps published mbx observations and inspection errors into status without
  gateway-maintained capacity, queue or lease counters.
- **`no_mbx_keeps_existing_jobserver_and_pressure_behavior`** preserves `g`, FIFO inheritance, runner sizing, and the
  existing pure pressure decisions: threshold crossings and youngest eligible Critical-pressure victim.
- **`gate_priority_never_holds_other_lifecycle_work`** interleaves a gate with fork, attach and release cleanup and sees
  no gate-induced admission hold.

Exactly one real mbx wiring smoke, **`canonical_cargo_route_reaches_the_pinned_mbx`**, uses two shed roots, the upstream
Cargo shim and the one shared cache directory: it records both sessions' pool identity, priority and published evidence
of shared in-flight work. It proves the common pool wiring, not mbx's deduplication, prediction or OS permit recovery.
Kernel nice/cgroup behavior and mbx's internal scheduling policy remain their owners' tests.

### Open questions

- **External-work peak identity.** The inference under "Parallelism inside a job" is whether existing job/process spans
  identify the same Nx task across runs. Inspect that correlation before enabling the span-derived memory request;
  absent or unprovable identity uses the specified zero-byte charge, never a shadow table.
- The known unscheduled Cargo test-runner gap and its closing measurement belong to 11_shell.md, "Open questions".

## The task server schedules work units

Work units admit to the gateway with the facts of their plan — the units they depend on, the owner that runs them, the
serialization windows they hold — and a deterministic scheduler alone decides which ready unit runs next. The order
derives from the logged facts and the host's memory pressure; no operator assignment and no model output chooses which
ready unit acquires a lease. Strong models propose task boundaries and interfaces; they do not schedule.

- **The order.** Aged waiters first: a unit that has waited the configured age bound outranks fresh priority and runs
  among the aged in the order they waited, so a stream of fresh urgent work can never starve a queued unit. Then the
  class order — gate-class work first, interactive and background work behind it, the same on any host — then the
  coordinator's `urgent` override, the unit's leverage (the distinct units transitively depending on it), the
  cache-domain deadline (known deadlines first, soonest first; an unknown deadline sorts behind known ones and is never
  invented), and finally the stable unit id. The schedule is recomputed at every event and at the oldest waiter's age
  bound; the same facts always produce the same order. Within one unchanged-graph operation, selection, ready order and
  queue status share one leverage observation; an actual post-selection abandonment before the queue reply is the sole
  invalidation, and the reply then takes one fresh leverage pass over the abandoned graph.
- **Ownership.** A unit keeps exactly one live attempt: its admission connection is the attempt, held like a disk lease.
  A second admission of a live unit is refused. A connection that closes without a terminal outcome abandons its
  attempt, and a delayed holder of a settled attempt can never advance a newer one.
- **Serialization classes.** A granted run holds its declared classes from its selection until its terminal fact:
  `cowshed-new` (one `cowshed new` on the host at a time), `image` (a workspace-image mutation window), and
  `land-publication` (the checked land-authority transition). `land-publication` is a publication fence — it serializes
  publication only: gate coordinator groups run concurrently in one class. Lands queue for the fence in one order — the
  first claimant in admission order, behind older aged waiters — so an urgent land waits its turn and never overtakes an
  older claimant. These classes bound the windows the operator keeps by hand today; the disk-lifecycle lease above still
  spaces every disk command; leaf resource admission follows "Host scheduling". A lease-blocked unit holds up nothing
  beside it.
- **Caps and bands.** A unit may declare its own parallelism ceiling — the caps its run kind applies to itself
  (`nx parallel`, `CARGO_BUILD_JOBS`, `--test-threads`) — and the grant reports that ceiling as declared; leaf controls
  derive from it, never from a static profile. The grant also names the run's band, its class's nice in "Classes":
  interactive work foreground (0), gate-class work normal (+5) — the land turn's holder included — and background work
  background (+10). Placement is server-owned: the band follows the class alone, and no other caller field raises a band
  or widens a ceiling.
- **Memory admission has the same owner as jobs.** With mbx selected, the task server adds no pressure gate before
  selecting a ready unit; its leaves admit through mbx. Without mbx, a background unit is not selected while the host's
  memory pressure holds a background start ("Memory pressure"), and the scheduler reads pressure again on the same 2 s
  interval; every other class is selected regardless. A failed reading holds a background unit and names why rather than
  failing a unit that is not a job yet. The scheduler holds no CPU counter and mints no CPU grant: selected units' leaf
  runners use "Host scheduling" and "Parallelism inside a job", not another task-server resource ledger. `TaskClass` is
  the work-unit ordering and placement axis; a task band never creates an exclusive gate root per group.
- **Wire.** `task-admit` is held like a disk lease —
  `{"op":"task-admit","unit":…,"owner":…,"incarnation": …,"deps":[…],"leases":[…],"priority":<u32>,"urgent":true,"class":"background","deadline": <epoch-ms>,"command":…}`
  — answered `queued` at once and a granted `taskGrant` (`unit`, `band`, `caps`) when the scheduler selects it.
  `task-terminal` is one-shot — `{"op":"task-terminal","unit":…, "outcome":"landed"|"failed"|"abandoned"}` — the fact
  every dependent reads and the release of the unit's classes; its decision log names the owner and workspace
  incarnation the lifecycle receipts bind to. `task-queue` is one-shot and answers `taskQueue`: the ready units in
  selection order (with any lease class they find full and who holds it, and `memoryWait` when memory holds it:
  `{"kind":"read","pressure":…}`, the reading that held it, or `{"kind":"unread","error":…}`), the waiting units with
  each dependency's outcome, the running units with their leases and caps, and each class's places and holders.
- **Decisions are logged facts.** Every selection and terminal fact is written unconditionally to stderr as
  `cowshed: timing task-server select|terminal`, with unit, owner and incarnation. A selection includes its ticket,
  attempt, numeric `waited_ns` from admission to selection, band, leases, and the admitting client's `command` and
  `pid`. `task-queue` reports the live fold; nothing in the scheduler is a second authority over the plan.

Tests: the scheduler's own regressions pin the policy without an agent in the loop — the exact order over a crafted fact
set, the same order from the same facts however they arrive, a low-priority unit selected at its age bound under a
stream of fresher urgent arrivals, the serial classes with the urgent land behind a turn in progress, one live attempt
per unit, the caps reported as declared, and gate work ordered first — plus `tests/task_server.rs` over a real control
socket: units admitted and advanced by their terminal facts alone, with the caps and the queue view read off the wire.
Gate-band units are granted concurrently; a work-unit class never creates exclusive root-gate admission.

## Credentials

- Secrets use macOS Keychain generic passwords under service `dev.cowshed.gateway`; Linux runner credentials use the
  platform mechanism specified in 10_ci.md. Records carry protocol, exact HTTPS origin, admitted methods, normalized
  path/package/module scopes, and project `repo_id` where applicable; a bare host-only secret record is invalid.
- Credential lookup happens only after endpoint identity, token, project admission, CONNECT authority/SNI, method, and
  normalized path checks. Rotation is observed on next use. Values are never logged, cached in telemetry, placed in
  URLs, or returned to clients.
- Each workspace CA private key is controller-owned mode 0600 outside every mount. The matching public certificate is an
  in-image server trust anchor only. Create/fork mint a key; destroy removes it.
- The gateway process is host-side and credential-store access is restricted to that executable identity.

## Startup contract: mains eagerly, sessions on use

The gateway is `RunAtLoad`, so it is the first cowshed process alive after a reboot, and its startup pass is where
mounts are restored. In order:

1. Validate the host store — both dedicated volumes present, mounted, marked, and with canonical flags (01a_apfs.md). A
   store that fails validation stops here and reports; nothing below can be meaningful without it.
2. Take the workspace listeners an install carried across the restart ("An install never refuses a client connection"),
   before anything binds. Then count the recorded projects and list the workspace supervisors still serving from before
   this daemon, then serve: the control socket and the supervisor manager answer from the first moment. Status answers
   throughout, and so do the audit, mirror, and simulator requests, none of which reads a workspace mount. Once it
   serves, the daemon starts the `image-cleanup` thread, which lists the released images a restart or a crash left, then
   frees them one at a time, and lists again at every release's nudge for as long as the gateway runs ("Deleting
   released images", below). It reads no mount and waits for neither step 3 nor step 4.
3. Eagerly heal every recorded project's main: attached and mounted at its checkout path. Each step is a lifecycle span
   of its own — `startup-heal discover`, `startup-heal open <repo>`, `startup-heal mount <repo>/main` — so a slow pass
   reads as the step that spent the time. Session workspaces are not attached by the pass: a session the kernel still
   holds (a gateway restart without a reboot) stays attached, and every other session stays detached until a verb uses
   it. The pass takes each main's image lock without waiting. A held lock is a live lifecycle operation's, a fork of
   main or another command's, whose process may itself be waiting on this starting gateway, so a wait would hold the
   start until that process ended. A main that already stands mounted at its checkout is served as it stands under the
   held lock; one that needs healing is reported as left to the holder, and the pass goes on. A background task heals it
   in the shape of startup recovery's wait (01_storage.md, "Lock order"): it waits for that lock alone, holding no
   lease, then takes the project's reader lease, refuses a project whose retirement owns it by then, and mounts main
   without waiting, letting go of the lease to wait again if another process took the lock meanwhile.
4. Restore every attached workspace's session into the gateway. A session whose endpoint an install carried serves on
   that listener, connections queued on it included; any other binds its endpoint. Once every session is restored, each
   carried listener no session claimed is closed, so its queued connections are reset rather than left waiting for
   nobody.

Positively authorized pending project retirements are excluded from the ordinary heal set. After the control socket and
supervisor manager are serving, startup resumes those records through the same owned drain, unforced detach and
preservation transaction as `cowshed forget <repo> --abandon` (02_workspaces.md). It does not infer abandonment from a
missing checkout, marker or process; a failed or busy retirement remains fenced and is reported without cancelling
healthy projects' mount restoration.

Until step 4 ends, status carries `healing`: `{ mounting: { projects: N } }` with the projects not yet mounted (never 0;
a project counts once its main is mounted or left to its lock's holder), then `restoringSessions`; it is absent once the
pass is over, and `cowshed gateway status`, `doctor` (`gateway-starting`), and `gateway start`'s wait say so in those
words. Everything that depends on what the pass restores is refused by type meanwhile, never reported as an absent
gateway: a session `install` or `remove` on the control socket with failure code `healing`, every command's reconcile
with a `Conflict` carrying `healing` and the `cowshed gateway status` hint, and every supervisor ensure likewise.
Sessions are restored from the attachment facts the pass is still changing, so a session installed or removed before it
ends would race that restore; a supervisor would start in a workspace the pass may not have mounted yet. The supervisors
listed in step 2 are recovered once the control socket is bound, all at once and in the background, alongside steps 3
and 4 (11_shell.md "Supervisor recovery"): while any is still being recovered, status carries
`recovering: { supervisors: N }` (never 0; absent once none is left), and only a command for one of those workspaces is
refused, by the typed recovering refusal.

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

Sessions, not mains, heal on contact. A session's path is under the mount root, not a checkout the user's shell, editor
or Finder shows, and every verb that reaches a session (`path`, `exec`, `attach`, a lifecycle verb) opens it, which
attaches a detached one (06_cli.md, "Resident workspaces"). Attaching every session at startup instead cost each shed
two images and six registered IOMedia whether or not anyone used it: on 2026-10-22 the pass re-attached about 154 sheds,
308 images and 931 IOMedia, most of them idle or already landed, which put the host within a hundred of IOKit's 1024
match-notification cap (01a_apfs.md, "The IOMedia match-notification ceiling") before any work began. A session attached
on use costs its media only while it is used, and making room (02_workspaces.md) detaches it again when it idles.

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

## Deleting released images

A released image is freed by the gateway. Every image a cowshed operation removes is released, never unlinked by the
verb: a retirement (`rm`), the volume and seed a land replaces, a compaction move's previous volume, a superseded volume
a job's end releases, a fork's seed refresh, the staged clone a failed creation leaves, a staged copy a re-adoption
leaves, and what `gc` collects (a retired workspace's trash, an orphan session or staging image ). The operation commits
the release with one `rename(2)` of the detached image, never replacing a file, into a `.deleting` directory beside it:
`build/.deleting/<id>.dmg` for a build volume, and for any other image of the project `sessions/.deleting/`, a removed
workspace's as `<ws>-<incarnation>.dmg` and any other as `reclaimed-<fresh 32-hex>.dmg`. It syncs the directories it
touched (16_build_volumes.md, "Garbage collection"; 02_workspaces.md, `rm` step 5). The commit never opens the image, so
it reads none of its bytes and needs no access to it. No verb frees the bytes itself, because a verb that frees holds
its turn and its locks, a land its land turn and an `rm` its workspace's locks, for as long as the free takes. No
releasing process ever truncates an image, and none starts a process to do it: lands, forks and rebases never wait for a
deletion. Measured 2026-10-25: two deletion children of a land, started under its land turn, held the repository's land
turn after the land exited, until their frees ended, and nine children truncating released images at once saturated the
store's container, with lsof, git, forks and execs in uninterruptible waits behind them.

- **The rename is the commit, and the `.deleting` directories are the queue** (the owner's ruling, 2026-10-26). That is
  what makes a release crash-safe with eventual cleanup: whatever stops between the rename and the free, the next
  listing finds the entry. Any separate record of what `.deleting` holds — a hand-over message's contents, an extent
  count in a sidecar, an in-memory queue taken as truth — can diverge from the directories across a crash or a restart,
  so there is none. The gateway's only inputs are a listing of the directories and a nudge on each release, and the
  nudge carries nothing the listing does not.
- **Every release ends with a nudge, sent once its rename is durable.** The releasing process holds no image lock while
  it renames and syncs, and returns at once. It writes one one-shot control request, `{"op":"released"}`, to the gateway
  of its host namespace (01_storage.md, "Host namespace") and half-closes; the gateway answers `{"ok":true}` and lists
  its store again. If the gateway is absent, does not answer within the 2 s every admission's acknowledgement has
  ("Disk-lifecycle lease"), or refuses, the releaser says so once on stderr, and the entry waits for the gateway's next
  listing: its rename is the release, so the verb still answers. A nudge that arrives before the cleanup has started is
  refused as `rejected`; the cleanup's first listing finds the entry.
- **One thread, `image-cleanup`, frees what the directories hold.** The gateway starts it when it starts serving
  ("Startup contract", step 2), so neither a listing nor a free delays its readiness. Each pass lists every entry in the
  store: each project's `build/.deleting` and `sessions/.deleting`, the same two directories of every archived project
  (`.retired-projects/<operation-id>/`, where `forget` renamed the project's tree with its released images in it,
  02_workspaces.md; the receipt files beside an archive are skipped), and the store's own `<store>/.deleting`. A project
  is `<store>/<owner>/<repo>` with both named as encoded components (`repository::is_encoded_component`), which never
  start with `.`, so the volume's own system directories (`.Spotlight-V100`, `.Trashes`) are never read. A stray or a
  non-regular file is named on stderr at the gateway's start and left untouched; a directory that cannot be read is
  named at every pass that finds it so. The listing takes each entry's type from the directory read (`d_type`) and never
  stats or opens an entry, so neither it nor `cowshed gateway status`, which counts the same listing, waits on the entry
  a cut holds (the first cut of a clone still sharing its extent map measured 32 to 81 s in one call, 2026-10-26). It
  looks nowhere else and does no sync: an archive's released images are no evidence, they were released before the
  archive's rename, and nothing else in an archive is opened. The thread frees every entry the pass listed, then waits
  for a nudge; the nudges heard during a pass start one more pass. It has no timer and carries nothing from one pass to
  the next.
- **Oldest first.** A pass frees its entries by the change time (`st_ctime`) their release's rename set, then by path;
  an entry `stat` cannot read goes last, where its free says why. A cut changes an entry's change time, so one a
  previous gateway cut short goes after the rest. Ordering by an image's cost or yield would need what a listing and
  `stat` do not give, its extents and the bytes it shares with its source, so nothing orders by them.
- **One cut at a time, beside gate work too** (2026-10-26, the owner's ruling: the queue serializes cuts, and never
  waits for gates). The thread frees one entry at a time, so one release cut runs per gateway, and therefore per
  container on a host. It cuts the entry it reaches at once: nothing in it asks for CPU or waits for a gate; the kernel
  schedules it. While it frees an entry it holds only that entry's own `flock`, never an image lock (`main.dmg.lock`,
  `sessions/<ws>.dmg.lock`; 01_storage.md, "Lock order").
- **One entry's free.** The thread `flock`s the entry without waiting. An entry another process holds is left, and said
  on stderr: its holder is another cleanup or deleter still cutting it. A busy entry, like one whose free fails, waits
  for the next pass, which the next release's nudge or the next gateway start begins. Before the first cut the thread
  syncs the entry's directory and that directory's parent, because a releaser may have died between its rename and the
  rename's syncs. It then cuts from the end of the file, each entry starting at 1,024 physical runs per `ftruncate` and
  each next cut sized from the rate the one before it took, toward 250 ms (01a_apfs.md, "Freeing an image"), at normal
  disk priority (16_build_volumes.md, "Garbage collection"; `IOPOL_THROTTLE` stays rejected, 01a_apfs.md, "Freeing an
  image"), unlinks the empty file and syncs the directory. The file's length is the journal: a cut ends on a run
  boundary, so an entry cut short is finished from its length. The thread says on stderr each entry it frees, with its
  bytes and cuts. An entry that is gone when its turn comes (freed already, or moved by a final unbinding) needs
  nothing; one whose free fails is said with its cause and stays in `.deleting` for the next pass.
- **Under pressure, one unlink per entry** (the owner's ruling, 2026-10-28: when space runs out, progress stalls
  anyway). Before each entry the thread reads the store's free space and capacity (`statfs`); when free space is under
  the store's floor, 10% of capacity, plus a 5% margin, and an entry waits, it frees that entry by one `unlink` instead
  of cuts (`release::drain_pace`, a pure function of free space, capacity and the entries left). Per-extent cost does
  not shrink with the cut size, and two frees at once only slow each other on one container, so skipping the cuts is the
  one faster path: one unlink freed the 131,072-extent clone in 7.6 to 29 s against 35 to 62 s in cuts, while other
  writers stalled for seconds (01a_apfs.md, "Freeing an image"). It says on stderr each change between paced and
  pressured draining. The floor is one constant cowshed owns (`release::STORE_FLOOR_PERCENT`): `cowshed gateway status`
  reports it with the store's free share and the pace (`imageCleanup.floorPercent`, `freePercent`, `pressure`), and a
  watchdog that judges the store reads the floor there rather than keeping a copy.
- **A restart finishes what it cut.** An install's restart stops the thread mid-entry, and the next gateway's first
  listing finishes what it left: so a gateway restart or a reboot (the gateway is `RunAtLoad`) frees every entry left,
  an image cut short mid-free included.
- **What is never done.** No verb or other releasing process truncates an image, starts a process to free one, or holds
  an image lock through a free. The gateway keeps no record of an entry and reads no count from a releaser, applies no
  `IOPOL_THROTTLE`, and no environment variable drives the cleanup.

Space lags. Until the cleanup frees an entry the store's container still holds its allocation, so free-space figures and
extent counts lag the commit by the entry's wait for its pass and the free's own duration, and an entry no gateway was
nudged for waits for the next listing. Nothing references an entry: no record, link, hold or mount names it, and nothing
mounts, forks or reads it.

Two cases nudge nothing. A store whose configuration names no gateway (a fixture store no test gave one) says so on
stderr once per process, and its entries wait in `.deleting` for a gateway's start. A directory store, the test
substrate, only renames: its release moves the payload into `<project>/sessions/.deleting/<name>` (a build volume into
`build/.deleting`), the verb deletes nothing and nudges nothing (said once per process), and a released payload stays in
`.deleting` until its test store goes, because a tree can be freed only with the directory host's holder census, which
runs in-process; `DirectoryExecutionHost::finish_build_releases` can finish build-volume trees, leaving one a process
still holds (16_build_volumes.md, "Garbage collection").

A project's final unbinding (`rm main --restore`, 02_workspaces.md, "Checkout layouts") frees nothing either. Its store
tree goes with it, so before it plans terminal storage it moves each released image of its project, from
`build/.deleting` and `sessions/.deleting`, into `<store>/.deleting/<fresh id>.dmg` and nudges like any release. A cut
the gateway is making moves with its entry, which the cleanup holds open, and the next pass lists it under its new name;
a stray left in those directories still refuses the plan.

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
