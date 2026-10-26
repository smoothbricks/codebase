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
  for main), preserved across restore, and present only in macOS grant files. The gateway binds `base`;
  `base+1 … base+size-1` are workspace service ports. A session carries its block's recorded size, and the gateway
  validates the endpoint against it. A store allocates from its namespace's partition: production from the host
  partition (32768–45055), a test namespace's fixture store from the fixture partition (45056–49151), so no test block
  is ever one production holds. Seatbelt permits a workspace to connect only to its current and retained blocks. The
  destination `base` listener is the primary, kernel-enforced workspace identity. Of two sessions whose blocks share a
  port, the gateway serves the older workspace's and not the newer one's, at startup and in every per-project reconcile,
  with `next: cowshed doctor --repair`, serving every other session (04_sandbox.md, `portBlock` collisions); neither
  startup nor a reconcile fails over a collision.
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
| `cowshed-gateway-host`   | The program launchd runs for the gateway: a copy of the build, signed ("The gateway host").    |

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
`gateQueue`, each entry's `checkout` and `job`: `gate job <n> at <checkout>`). It never waits for them. The refused
install copies, moves and restarts nothing, so the installed cowshed and the running gateway stay as they were;
`setup`'s storage repair, which runs before its install (setup_service order: volumes, owner markers, mains, git
identity, sccache client, then the service refresh), stays done. The hint is to rerun once they have ended, and
`cowshed gateway status` lists both. A land queued behind the turn's holder runs nothing yet and is not a blocker: its
first command waits for the slot. Land turns that cannot be read refuse too, naming the directory: not knowing whether a
land runs is no permission. The install reads only those two ledger fields: the gateway it is about to replace is of
another build, and a field one build has and the other lacks must not turn the check into a restart that cuts a gate
off. A gateway that does not answer `cpu-budget` names no gate job, and the install says so. An install that restarts
nothing (no daemon answers, or the refresh finds the agent current) checks nothing. This replaces a wait with no
deadline for the gates in flight: measured 2026-10-21, it held an install behind a gate of 2 h 26 min until the gate was
killed, and operators took to granting installs a window by hand.

**A land step waits for an install in progress.** The ledger shows a land only while one of its gates runs, and a land
is several cowshed commands with none running between them: measured 2026-10-20, an install found the ledger empty as a
land's first gate ended and restarted the gateway as its host gate started, and that gate's `cowshed exec` failed with
the daemon's socket refusing the connection, failing the land. So every command a land runs — `exec --class gate`,
`rebase` and `land` without `--preflight` — before it asks the daemon anything, waits for an install that holds the slot
("The install slot", below), saying on stderr that it waits and for which file, and then holds nothing. A step that
waited is still the build it started as, and the daemon that install left refuses another build. Measured 2026-10-21: a
land's gate passed in 1,503 s, its next step waited for an install, and the daemon refused it by build (exit 4),
throwing the green gate away. So a step that waited and is then refused as another build goes on as the installed
cowshed: it execs, in place, the stored copy the stable name says once the install is done, with its own arguments,
keeping its pid, working directory, environment and descriptors, and says so on stderr. A step that did not wait, or a
refusal of any other kind, is returned as it is. A daemon connection refused later in the step, while an install holds
the slot, is that install restarting the daemon — a positive signal — so it waits for the install and connects once more
to the daemon the install started; a refusal with no install holding the slot is returned as it is. An install that
restarted the gateway — `gateway start` and `setup`'s refresh alike — holds the slot until that gateway serves: its
control socket answers from the installed bytes, its startup pass is over and it is not draining, the wait
`gateway start` reports. Releasing the slot at the restart is not enough: measured 2026-10-20, `setup` restarted the
gateway and released the slot at once, and the land step that had waited on it found the daemon's socket refusing the
connection, failing a land whose gate had passed. A land step never holds the slot for its run: it did once, and an
install then waited on every land step in flight, between its gates as well as during them. An install refuses a gate
that holds or wants tokens ("An install refuses while a land or a gate runs"), because a gate's CPU pool is gateway
state a restart ends and is never carried across one. A job in flight lives in its workspace supervisor, which a gateway
restart leaves running; what a restart breaks is a new connection to the daemon, and that is what waits. The ledger is
never the guard against a restart between two of a land's commands, since no process of that land holds anything the
gateway records there; its land turn is. Status reads the slot without taking it.

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
a land turn's holder, a gate job holding or wanting tokens, another install holding the install slot. It runs the staged
`setup` once, in main's devenv (setup installs the PATH it runs under, and its discovery runs cargo), only when neither
says otherwise, and a refusal `setup` still answers is classified by a fresh status. Every outcome is one line on
stdout, `<UTC time> <commit> install attended|unverified|deferred|installed|failed|skipped`, and the first three carry
the exact command, which enters that devenv too: to run by hand (`attended`, `unverified`) or to rerun once the named
work has ended (`deferred`). An install that cannot be taken is deferred, never waited for; the operator's land-status
record holds it for whoever watches that record.

**An install never refuses a client connection.** A restart closes every workspace listener until the next gateway's
startup pass restores its session (5 s and more), and a stock client cannot wait for that: measured 2026-10-20, an
install restarted the gateway under a shed's Nx build, and Nx read the refused cache connection
(`Failed to send request …/nx/v1/cache/…`) as fatal and failed the build. The carried Nx patch now reads it as a miss
("The client half" under `/nx/v1/cache/{hash}`), which still costs every lookup of that run. No wait can prevent it. Nx
sends a cache lookup holding no CPU token, so between two lookups no ledger, lease or slot shows that it runs, and
waiting for all running work would hold installs back on any busy host. A typed "restarting" error is no answer either,
since Nx reads only HTTP statuses. So the listening sockets outlive the restart. On macOS an install that restarts the
gateway — `gateway start` and `setup`'s refresh alike, not a `--downgrade`, whose older gateway might not take them —
serves `listener-handoff.sock` beside the control socket from before the restart until the restarted gateway serves. The
old gateway, as its drain begins and while it still accepts, gives the install a duplicate of every TCP workspace
listener (`SCM_RIGHTS`), and its drain then answers what it already accepted as before (the Nx route's 404/403). The
install's copies keep each port listening, so the kernel queues a connection made while no gateway runs. The next
gateway takes them before it binds anything ("Startup contract" step 2) and serves each on its restored session (step
4), queued connections included. Only this user's processes may give or take, and the store deny keeps every sandbox off
the socket. Every failure is what a restart was without the handoff: no install serving the socket, an old gateway that
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
land takes a numbered ticket in `dev.cowshed/land-turn/<repo>/`: a file locked exclusively through one open file
description for as long as the land waits or holds. The command inherits that description, so the kernel releases the
ticket only once the land and every process holding the description have closed it, however each ends. Numbers are given
under the directory's `.seq` lock, each above every ticket present, and a ticket is locked under a temporary name before
it is renamed into place, so no reader ever sees an unlocked live ticket. No ticket is removed while its lock is held:
neither the land's own end nor its drop unlinks it, since a descendant may still hold the description and a ticket no
path names would let the next land run beside it. A queue scan (a waiting land's, or status's) removes a ticket only
once it has proven it dead by taking its lock without waiting; a ticket still locked is live, whatever its pid says. The
lowest live ticket holds the turn; each other land waits on the lock of the live ticket just ahead of its own and, when
that ends, looks again, so a killed holder or a killed waiter blocks nobody and only the next in line is woken. A
waiting land says on stderr how many lands are ahead and who holds the turn — its workspace, pid and when it asked —
when it starts waiting, whenever the land ahead changes, and every 90 s between (the land watchdog's cadence), and how
long it waited once the turn is its own. `--outside-lock <file>` names the lock file lands took before every caller
asked for turns: the head takes it too, as a held lock (02_workspaces.md "Held locks"), keeping the file, so it excludes
a land started that way. A legacy process outside the queue holding that file may record nothing, so while the head
waits it names the processes with the file open: when it starts waiting and every 90 s after, one census
(`build_volume::nx::holders`) gives each one's pid and command, the head's own descriptor left out. These are open
process descriptors, not a proof of lock custody or authority to signal. A census that finds no other process says so
(the holder let go as it looked, or is not this user's to see), and one that cannot be taken says why. Measured
2026-10-21: a land waited 1002 s for the outside lock having said once that "a land that did not ask for a turn" held
it, naming no process. The census decides nothing: the head waits for the kernel's lock alone. The wait starts before
the first census, on a thread of its own, so no census delays taking the lock. Measured 2026-10-21 on one fixture (a
`lockf -k` holder for 1.5 s, debug builds, a 709-process host, 20 interleaved runs each): without the census the first
report came 18 ms after the wait began and the turn 20 ms after the holder let go (medians); with it, 213 ms and 17 ms,
at about 0.11 s of CPU per census.

The command inherits the descriptors of its ticket and of the outside lock, so the kernel keeps the turn and that lock
until the command, and every process it hands them to, has closed them: `land-turn` killed alone never lets the next
land run beside its command, and the next land takes the turn when the last of them ends. A descendant that detaches
holds the turn by design until it closes the descriptor or exits; it is never unlocked around. The launch boundaries and
the git and Nx policies that keep cowshed's own daemons and git's automatic maintenance out of a held command are
specified in 02_workspaces.md "Held locks". A land's turn covers rebase, gate, fast-forward and adoption; the push to a
remote runs after the turn ends, since a remote is never needed to land (tg#6988). A turn is only as durable as the
processes that hold it: a land that shares its caller's session ends with that session. Measured 2026-10-21: one
lander's lands died with its tool session three times, twice while queued and once mid-gate. So a caller that may go
away first starts the land in a session of its own (`setsid`, output to a file, not its child) and waits on the land's
pid; `land-turn` itself never detaches, since the caller that stays reads its output and its exit status.

**A ref turn.** A repository whose main also moves outside its lands (an operator's Markdown commit straight into main)
keeps a second queue beside its land turn, by convention `<repo>.ref`: every writer that moves main's ref outside
cowshed takes it for that move alone (`cowshed land-turn <repo>.ref --workspace <label> -- <commit>`), and a land names
it to `cowshed land --ref-turn <repo>.ref`, which takes it only from re-reading the target head through the fast-forward
(02_workspaces.md "`cowshed land`" step 3). It is a land-turn queue like any other: first asked, first served, the
holder named, held through its ticket's descriptor, which the fast-forward's git inherits. A Markdown commit therefore
waits for one fast-forward, never for a land's gate, and never barges. A land takes its land turn first and the ref turn
inside it; a writer that holds the ref turn never asks for the land turn, so the two never deadlock. A flock(2) ref lock
(`--ref-lock`) orders no waiters: the kernel wakes them all and the first to retry wins, and it names no holder in
order.

**A ticket's life is recorded.** Measured 2026-10-21: a land queued for 635 s vanished with its agent's session, its log
stopping mid-wait, and nothing recorded it, so it read as running for half an hour while it did not exist. Each
repository's `turns.log`, beside its tickets, gets one JSON line per event, appended and never rewritten:
`{ atUnixMs, event, ticket, outcome? }` with `event` one of `queued` (the land took its place), `taken` (it holds the
turn), `finished` and `abandoned`. `finished` is written only by a land that ends its turn explicitly once its command
has ended (`exited <code>` or `killed by signal <n>`) or could not start (`did not start: <reason>`). It first marks its
ticket finished by appending a final newline to the ticket's record in place (the inode is never replaced), then writes
the line. A turn dropped without that end (cowshed failed, was killed or crashed, its session gone) writes nothing; the
ticket stays, locked while any descendant holds its description, and whoever next scans the queue and proves it
unlocked, waiter or status, writes `abandoned` for it before removing it. A scan that finds a proven-unlocked ticket
marked finished removes it and writes nothing, so a land that ended normally has exactly one `finished` and no
`abandoned`. A marker that cannot be appended is said on stderr with the ticket number, its directory and the error, and
the known `finished` is still written; that ticket, left unmarked, may then also be logged `abandoned` by the scan that
removes it. How long a land waited and held is the gap between its lines. A line that cannot be written is said on
stderr and never stops a land. `cowshed gateway status` lists each repository's live tickets, the holder first, and the
tickets found abandoned in the last day, as
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

**No credential: one user, localhost** (Danny tg#10456). The store is one host directory of one user, served on
loopback. A hash names a task's inputs, so a result stored under it by any shed is as trustworthy as that shed's own
local cache: a task that builds under an input it does not declare poisons the local cache exactly as it would this one,
and the remedy for both is declaring the input (16_build_volumes.md). So the route authenticates nothing, and no token,
principal, transaction or receipt guards it. What it keeps is what correctness needs: project namespacing, the key
charset, atomic first-writer-wins writes, and the eviction bound (03_caches.md, "Nx accepted-task artifact service").

**Sheds read; a land writes** (tg#1780, tg#1782). Every checkout reads its project's results, during ordinary iteration
as well as adoption. Nx keeps its local artifact cache and task database private to that checkout (16_build_volumes.md);
this route shares neither path nor live database. The controller wires stock Nx's `NX_SELF_HOSTED_REMOTE_CACHE_SERVER`
to `<base>/nx`, without a trailing slash, beside `NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true`, in `.cowshed/env` and in
every job's environment (03_caches.md), and no access token. The read-only flag is configuration, not authority: the
carried Nx patch makes a read-only client answer every store `false` before it builds an archive (patches/README.md, "A
shed reads the remote cache and never stores"), so work in progress is never stored and never costs an upload. The
gateway itself would store a shed's `PUT`; nothing configured sends one. Every supervisor start rewrites `.cowshed/env`
whole, so an existing shed's older file, which exported the retired bearer and no flag, carries the flag from the first
supervisor start of a build that sets it, and direnv reloads it in the shed's host shells.

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
cause, and the land goes on. A repository whose land gates outside `cowshed land` publishes its gate's run with the same
script (SmoothBricks: `tooling/land-host.sh` through `tooling/land-publish.ts`, after its host gate and Linux cross
check pass).

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
host's disk-lifecycle lease and CPU budget to every process that runs a disk tool or a parallel runner (below), and the
hand-over of a released image to every process that releases one ("Deleting released images"). Peer credentials on the
Unix socket (and equivalent local authentication on TCP) must identify an authorized host process; the data-plane token
is never accepted on the control plane. The Unix accept loop records listener failure, unreadable peer credentials,
unauthorized UID, connection-capacity refusal, connection-task failure and shutdown; an admission refusal is never a
silent return.

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

Every request is one line. A one-shot request is followed by the client's EOF and answered with one line; a
`disk-lease`, `cpu-tokens`, `jobserver` or `resident-pool` request is held for as long as its client keeps the
connection open, and a `ram-lease` until its client's end line or the connection's end.

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
  that is running or draining with no process of its group left is not progress. If that state persists for the silence
  bound, the wait fails `SILENT`, naming the job as stuck between its last process and its end. A queued job is
  progress, because CPU tokens have no admission deadline (Host CPU budget, "No admission deadline, for any class").
- **The gateway control plane heartbeats the same way.** A one-shot request is connected, written and half-closed within
  the control bound. While its answer is not ready, and while an audit-tail follow waits for events, the gateway writes
  `{"ok":true,"progress":n}` every 5 s, once its actor has answered a ping. A heartbeat line, once begun, is written
  whole before the answer. The client reads past those lines. Until the first one arrives it allows the gateway 60 s of
  silence, because a gateway of an earlier build, which a client meets during an install, never heartbeats, and its slow
  answers (`first-launch`, `ram-detach`) still arrive within the minute doctor always gave it. Once a heartbeat has
  arrived, the 15 s silence bound applies. An answer that takes as long as its work (`nx-cache-clear`, `ram-detach`,
  `first-launch`) is therefore never cut off, and a gateway whose actor stopped turning is named `SILENT`. `cpu-budget`
  is never heartbeated: an install reads it from a client of another build, whose reader ignores unknown fields and
  would take a heartbeat line for a ledger with no gate jobs, and it answers from the ledger at once. A held request
  (`disk-lease`, `cpu-tokens`, `jobserver`, `resident-pool`, `ram-lease`) keeps its own protocol above.
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
  directories that stay, a refresh, one record-sequence allocation and its append, and a checkpoint's hash of the
  records and its manifest append. An open refuses records past the retained recovery budget (64 MiB) and its scan reads
  one directory entry per job that stays, so each hold is short and its wait is control class. A free lock is taken
  without blocking. A held one is waited for on a helper thread that blocks in `flock(2)` and hands the locked
  descriptor back, so nothing polls. At the bound the wait fails `TIMEOUT`, naming the lock path and its holders as the
  held-lock census reads them then (02_workspaces.md, "Held locks"); every caller (the supervisor, doctor's repair, a
  workspace's lineage read) answers it as a wait on another process, never as damage to the store. The holder is never
  signalled, and a lock the helper takes after its waiter left is released at once. The image lease is held for one
  disk-image command's run, so its wait is class `ffi` bounded by that command's own deadline, 120 s
  (`DISK_CHILD_DEADLINE`), and it names the pid its holder recorded. The shell is `cowshed-os::flock::exclusive`.
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
  credentials name the process that made the pair, not the controller) and the busy op of its last heartbeat. A
  heartbeat proves that the controller process and its connection serve, and names the op the actor is busy with. It
  does not prove the actor is not deadlocked: an actor stuck inside one op keeps the heartbeats coming, and their
  `forMs` keeps rising. A version 1 connection, an older client's, hears none (07_api.md, "Controller handshake").
- **Spans.** A failure reaches the boundary that owns the wait as a value. A `TIMEOUT`, `SILENT` or `PEER_LOST` failure
  carries its facts (`WaitFailure`: code, op, peer and its pid, class, bound, time waited, last progress) on the
  caller's error, inside the process only. An owner that has a span ends it `span-err` with the failure's message. The
  CLI has none: a command that ends in an expired wait seals it, at its one error boundary, as a one-span `cowshed.wait`
  segment in the trace it adopted from `TRACEPARENT` (13_telemetry.md, "Wait spans"). The expiries that carry it come
  from every workspace-supervisor call and from the gateway control requests that reconciliation and the CLI make
  (`status`, session install and removal, `session-current`, `nx-cache-clear`). A `BLOCKED` peer and a failed I/O call
  are not expiries and write no span. Neither does an answered wait, whose time the command owns, nor an expiry the
  caller takes as an answer (doctor's unanswered gateway, a probe of whether a gateway runs). The stderr line stays: the
  command's own report names the failure, and a seal that fails is said there and never changes the outcome.
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
  child's `wait`/`wait_with_output`, tokio `Child::kill`, `fork_lock`'s `output_locked`/`status_locked`, a unix-socket
  `connect`, `libc::flock` and `File::lock`/`lock_shared`, `waitpid`/`waitid`/`wait4`), each reason naming its bounded
  form in `cowshed_os::deadline` (`reap`, `reap_async`, `output_within`, `connect_unix`). A kept site allows
  `clippy::disallowed_methods` for its one item or statement beside `// WHY unbounded: <what ends the wait>`, and
  `cowshed:wait-why` (WAIT-WHY, `packages/nx-plugin/src/wait-why-policy.ts`, a dependency of `cowshed:lint`) refuses the
  allow without it. Tests wait through `cowshed_os::test_backstop`'s helpers, bounded by the no-progress backstop; a
  test keeps a raw wait, with its WHY, only where a helper thread would change what it measures.

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
a receipt records; it reclaims those strays and ignores every other disk, so a disk of another namespace, a test's, is
never taken for production's.

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
  `BOUNDED_EXEC_CPU_TOKENS`; the sub-ledger and telemetry report held tokens, not a fabricated runner count. Nx's task
  fan-out is the grant's too. Measured 2026-10-26: `cowshed exec <ws> -- nx run-many` on 2 tokens ran 11 task processes
  at once (16 GB RSS, host load 39), since the devenv shell's `NX_PARALLEL=100%` let Nx start a task per core. A job
  granted outside a gate therefore exports `NX_PARALLEL` as its count (a caller's lower count kept, a percentage
  replaced) and `COWSHED_NX_PARALLEL`, which `.cowshed/env` restores after a devenv shell sets its own; an explicit
  `--parallel=N` stays the caller's. A gate exports neither: its tasks ask the host one by one ("Gate admission").
- **Nested runners use a parent-held sub-ledger.** A granted executor serves its T tokens on a private socket and
  publishes that endpoint as `BOUNDED_EXEC_CPU_POOL`, beside its `BOUNDED_EXEC_CPU_TOKENS` count and the pool's make
  jobserver fifo ("Cargo reads its holder's jobserver" below). A default nested executor connects to its immediate
  parent's live pool, requests its own demand (the count is not a bound on the ask: the pool grants at most what it
  can), and holds one complete local grant on that connection. A background pool accounts for every holding sibling, so
  their sum never exceeds T. It serves its own FIFO queue, granting the head `min(requested, free)` once at least one
  token is free; that count is not a partial holding awaiting more tokens, and only a grant from the host ledger tops up
  ("All at once"). Queued siblings wait only for that pool's own holders. Host checkout fairness remains the upstream
  ledger's concern. A gate job's pool is the exception: it lends nothing of its own and relays each child's ask to the
  host ledger as a gate-class request for the gate's checkout ("Gate admission" below). An admitted child serves its
  granted count as its own pool, and grandchildren draw only from that pool: no holder waits for another grant from an
  ancestor or the host. The parent remains the owner of its upstream grant. A child returns only its local allocation by
  closing its connection; it never releases the parent's connection. Upstream loss revokes the downstream pool and its
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
  share is the host's N divided among the checkouts holding or waiting (at least 1). The next grant goes to the
  highest-priority eligible waiting request; within that priority, to the waiting checkout that holds the fewest tokens
  (the older head request on a tie), to its oldest request. While another checkout waits, that grant is
  `min(want, max(1, share − held))`; with nobody else waiting it is `min(want, free)`. A head request whose least grant
  is not free yet waits for it, and so does everyone behind it within the same priority. A large request is never
  starved by a stream of smaller requests at that priority; a gate with sixty runners queued cannot crowd out one with
  three at the same priority.
- **No background request is passed over without bound** (2026-10-22). Fewest-held order alone starves a checkout whose
  grant stays held while checkouts holding nothing keep asking: `held` counts every class the checkout holds, so neither
  the next grant nor the floor token ("Gate admission" below) ever reached it. Measured 2026-10-22 on gateway 89443:
  scratch checkout `ax-resources-18390-1` held one token for an idle FIFO reader, and its next one-token ask, ticket 95,
  waited 2,478 s while 51 later background asks (and 8 gate-class ones) were granted; in the same log the kept-daemon
  start stake of shed `icm-no-python`, whose checkout held its land's gate token, waited 2,214 s while 75 later
  background asks were granted, and the first token returned after the gate's own token left was its. So a checkout's
  waiting background head counts the background grants made to requests that asked after it (by admission ticket) while
  it is that head; once `⌊N/2⌋` of them (the background limit, 9 of 18) have passed it, it is overdue. Overdue heads are
  served before fewest-held order, the oldest first, at the same least grant, and an overdue head takes the floor token
  as a checkout holding nothing does. **The guarantee: a background request at its checkout's head is passed by at most
  `⌊N/2⌋` later background grants**, and one with k requests of its checkout queued ahead of it by at most
  `(k + 1)·⌊N/2⌋`. Gate-class grants outrank background and do not count. The bound counts grants, never time, so no
  clock decides an order. A heavy checkout loses little fairness by it: an overdue head takes its least grant, and the
  next head of that checkout starts counting from zero.
- **All at once, then topped up from tokens nobody waits for** (top-up 2026-10-26, Danny tg#10924: "where are the
  timeouts"). A request is granted once and never holds part of a grant while waiting for more, so no two requests can
  deadlock on each other's tokens. A grant below its `want` then tops up its fifo. Measured 2026-10-26: a land's task
  granted 1 of the 18 it asked while its siblings held the host ran its Cargo on that one token in jobserver acquire for
  minutes after they ended, the ledger 17 tokens free and nothing queued. So a lent pool whose grant came from the host
  ledger below its `want` looks at its fifo every second; while the fifo holds no token it asks the ledger for one more,
  at its own rank, one token at a time up to its `want`, and at once when a child waits on its socket. The ledger gives
  it only a token no request waits for; otherwise it refuses at once. A token the pool added that sits in its fifo goes
  straight back, as any returned token does, first to a waiter; an offer that sat idle backs the next one off, doubling
  to 30 s, and one a reader took is followed at once. The deadlock argument holds: a top-up is never waited for, granted
  or refused at once with no queue place, and it never takes a token a waiting request could have. A refused ask is
  asked again every second, and a pool the ledger refuses for 3 s, its fifo empty throughout, says so once a spell,
  naming the pool, the job and the ledger:
  `cowshed: cpu-pool STARVED pool=… job=… held=… want=… refused_ms=… socket_waiters=… cause=full|waiting ledger_free=… ledger_total=… ledger_queued=…`.
  A reader blocked on the fifo is invisible to the pool, so the empty fifo stands for it; `socket_waiters` counts the
  children queued on the pool's socket. The pool's own back-off is not a refusal. A grant a pool lends its own children
  ("Nested runners") does not top up: only the host ledger tops up. A runner sized when it starts keeps that size; only
  a jobserver reader, Cargo, draws on the tokens a top-up adds.
  - **A one-token job starts on probation** (2026-10-25). A short command's cost is its wait, not its work: measured
    2026-10-25 on an 18-token host whose tokens other sheds' gates held, a coder drive's sandboxed
    `git ls-files -z -- specs` (6 ms of work) waited 177 s for its token, `git show` (30 ms) 62 s and `codegraph status`
    (1 s) 290 s, about 1,200 s of a 1,660 s preparation. So the budget has one token beyond its N, the **probation
    token**, which the ledger lends to a waiting request whose job may start before its grant: a background-class
    request (interactive asks as background) for one token, `want` 1, that names no `parentPool` and whose requester can
    stop the job it starts, which it says with `"probation":true`. The workspace supervisor says so for every background
    and interactive job, since it started the job's processes and holds their group. A request that stays queued after
    its admission transition takes the probation token in that transition when it is free; otherwise the oldest such
    waiter, by admission ticket, takes it when it returns. The gateway answers
    `{"ok":true,"lease":"probation","tokens":1,"cpuPool":"…","fifo":"…"}` after `queued`, and the job starts at once,
    sized exactly as its one-token grant: one token's environment, a fifo of no bytes, and a pool that lends nothing
    until the grant, since the holder's own token is the probation token and no child may take it. The request keeps its
    place: when the ledger's order reaches it, it is granted its one token as any request is (`granted`, naming the same
    pool and fifo), its pool lends that token from then on, and the probation token passes on. Its grant does not grow:
    the job runs on one token on probation and on one after it.

  A probation that is not granted ends once the job has used 1 s of CPU or run 2 s since its first process, whichever
  comes first, as the supervisor reads it at its sampling boundary (one second, 07_api.md "Job monitoring"). The
  supervisor then stops the job's processes — `SIGSTOP` to its process group on macOS, `cgroup.freeze` on Linux — and
  writes `{"op":"probation-over"}` on the connection; the probation token passes on, and the job's ask waits where it
  was. Its grant continues it (`SIGCONT`, the cgroup thawed). A stopped job that is killed is continued as its kill
  begins, so it can act on `SIGTERM`. The job's stderr says each step: `cowshed: jobserver probation start` with its
  queue snapshot, `cowshed: jobserver probation over after <wall> ms, <cpu> ms CPU; stopped until its token is granted`,
  and `cowshed: jobserver wait done elapsed=<waited> status=ok` at the grant. So nothing runs outside the budget for
  more than one probation: the host runs at most N + 1 tokens' work, the extra one for at most a sampling period past 1
  s of CPU or 2 s of wall. A job whose work is brief finishes on the probation token, and one whose work is not waits
  for its token as it did before, having lost nothing of its place. A stopped probation is not peer work frozen to make
  room ("Room for a gate"): it is a job that was never granted, stopped by the process that started it, and its runners'
  bounds start at their own grants, which its pool makes only after its own ("The bounds start at the grant").

  Only a one-token request is lent it. A larger ask's command sizes its runners when it starts — test threads,
  `BOUNDED_EXEC_CPU_TOKENS` and the ceilings its children inherit — and keeps that size ("All at once"), so a job
  started on one token would run on one token for its life, which is the 21-minute `nx run-many` of "The task server
  owns the live grant and nice". A requester that knows its command is brief asks one token, as a coordinator's
  sandboxed reads do. A gate asks only its repository's queue and is never lent the token; neither is a request that
  draws on a requester's pool, an activation stake, or a task a relay pool asks for, none of which a supervisor stops.

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
  younger head **of another repository** holding nothing, and an overdue background head ("No background request is
  passed over without bound" above), gets one floor token first, oldest request first, while work behind the job being
  served still holds less than half of N. The younger repository's orchestrator can therefore start while the older
  one's tasks take every returned token they want. A behind-head gate in the same repository stays queued even beside
  free tokens; it cannot enter through the background selector. Background still uses its own free room when no eligible
  gate wants tokens. Running grants are never preempted or resized. Conservation remains `free + Σ held = N`.
- **One gate per repository, and a second says whom it waits for** (2026-10-25, PreventionAudit class #10, from Danny
  tg#10536: a second full gate run of one repository beside its land, and one repository's lands raced by two agents).
  The serialization above admits one gate-class job per repository at a time; a second one, a land's or an operator's
  `cowshed exec --class gate … nx run-many`, queues behind it and is never granted beside it. Its queued acknowledgement
  says so as a typed `cause` beside `queue`,
  `{"kind":"repository-gate","gate":{"repository":…,"class":"gate","checkout":…,"job":<n>,"command":…}}`, naming the
  gate job that heads its repository: that job's root request, or its oldest task once the root has left while its tasks
  still hold. Any other queued request, background included, waits for nothing but tokens in its order and says
  `{"kind":"tokens"}`. The client prints the cause on its `jobserver wait start` line ("Admission waits say what they
  wait for"), so a duplicate gate hears which gate holds its repository instead of reading a list of every grantee. The
  cause rides in the answer's envelope, not inside `queue`, whose shape a supervisor of an older build reads strictly.
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
  background room was one token under the half cap of that day: one long-lived ordinary background job held that token,
  and the land's check waited at `check-wait` for the job to end, while the job was waiting for the land, so the land
  never reached its adoption. At gate class the check is served ahead of background from every returned token. A process
  that stays up idle between work is a resident ("A resident holds no token") and holds none.
- **A train's proof is gate-class work** (2026-10-25, from a measurement: another repository's train's pre-land proof,
  run as background work, held 3 of the 9 tokens it wanted for 33 minutes while a non-critical shed held 9 and 15 other
  checkouts queued). A train is the repository's next land: its owner proves the train's tree before asking for the land
  turn, so the land's own gate replays that proof from the Nx cache. That proof is the land's gate run early, and it
  runs as one: `cowshed exec <train> --class gate [--host] -- <gate command>`, from the trusted route that alone grants
  the class. It takes no land turn, since nothing of it moves main, and it is held to everything a gate is: it is its
  repository's one gate job while it runs ("One gate per repository"), so a land's gate of the same repository waits
  behind it, oldest job first, its queued answer naming the proof as its cause; and its asks are served ahead of
  background from every returned token ("Room for a gate"). Ordinary unit work of a shed that is no train stays
  background: a gate-class run of it would hold its repository's gate order against the lands that need it.
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
- **Room for a gate: gate-class asks are served first, and nothing else yields to them** (2026-10-19, Danny: "avoid dumb
  waiting"; work-conserving since 2026-10-22, Danny tg#9527; ordering, not throttling, since 2026-10-26, Danny tg#11027,
  tg#11029: "I never said yield for gates"). A running grant cannot be shrunk: Cargo's implicit make token and
  libtest/nextest threads sized from the original grant do not return FIFO bytes. The gateway does not resize a running
  FIFO, kill peer work, or use `SIGSTOP`/`SIGCONT`: wall and idle bounds would keep advancing during a freeze, and a
  killed nextest run cannot resume, so its owner would rerun it whole and read a failure it did not cause. So a gate's
  room is order, not a cap: **every free or returned token goes first to a waiting gate-class request that may run
  ("Gate admission"), and background is limited only by the free tokens: while no such request waits, a background grant
  is `min(want, free)` under checkout fairness, whether or not gate work holds tokens.** The one-token floor of "Gate
  admission" still grants a starved background checkout, or a younger repository's head, one token while the work behind
  the oldest gate job holds less than half the host, and at least one token on a host of two or more
  (`behind + 1 ≤ max(1, ⌊(N − 1)/2⌋)` for N ≥ 2, `behind_limit`): it guarantees progress and limits nothing. A grant
  below its want tops up only from tokens no request waits for, and gives back a topped-up token it holds idle ("All at
  once"), so background that took free tokens beside a gate returns what it does not use to the gate's next ask.
  Measured 2026-10-22: no gate job ran, background was capped at 9 of 18 tokens, `leak-prevent` held all 9, and three
  other sheds waited while the other 9 idled. Measured 2026-10-26 on the train-6 install, with background held to one
  token a grant beside gate work: one gate job holding 1 token and asking nothing more left 17 background asks queued
  while 9 of 18 tokens idled. A cap beside a gate leaves the host idle, so there is none.

  **Nothing else yields to gates** (Danny tg#11040: "No gate needs a quiet window ever. The only legit reason for quiet
  is absolute precision in benchmarks."). Forks, attaches and the gateway's released-image cleanup never ask whether a
  gate works and never wait for one: a fork or attach runs when its controller routes it, and the cleanup cuts as soon
  as its queue picks an entry ("Deleting released images"). The gateway keeps no "quiet" state and has no operation that
  waits for gate work to end. A gate's precedence is the token order above and its nice band ("Bands"), nothing more. A
  quiet host belongs to a benchmark that needs absolute precision, arranged by whoever runs it, never to a gate. The
  waits this rules out answered no measured harm to a gate: the one-token background clamp and the fork and attach wait
  (3607bc754e) wrote down an agents' "quiet window" convention whose cited cost was load 23 on 18 cores beside an SB
  gate, and the clamp then left 9 of 18 tokens idle beside a gate asking nothing (9752d00c7d); the cleanup's wait before
  each cut (28385f7929, with a disk-pressure escape in d14d75b54c) rode beside the fix that mattered, one cut at a time
  after 55 concurrent truncations saturated the container, and was itself measured holding 86 released images for 679 s
  while free space fell below the pressure floor ("Deleting released images").

  This is the host ledger's rule: a parent's lent pool ("Nested runners") serves a grant the host already made, no gate
  asks it, and its children may hold all of it. **The trade:** a grant made while no gate ask waits is not shrunk when a
  gate arrives, so a gate that arrives while background holds the host is not granted in the transition of its own ask.
  It takes back its share at its next admission points: every returned token goes to gate-class work first, but for the
  floor token ("Gate admission") that a younger gate or a starved background checkout holding nothing is granted while
  the work behind the oldest gate then holds under half the host; background is granted only what no waiting gate ask
  takes. Nothing is killed, frozen or timed to make room, and the gate waits in admission order with no deadline ("No
  admission deadline"). The cost was measured 2026-10-19, before any cap: a background `cargo nextest run` granted 17 of
  18 tokens while it was the only waiter sat at 0% CPU for 50 minutes, and the land gate that arrived after it ran on
  its one token for over 35 minutes against a usual 10 to 20. A background grant made while no gate ask waits can do
  that again; Danny's ruling takes that wait over an idle host. Once gate-class work — the gate's own tasks or an older
  land's — holds the gate's part, a gate request waits for a returned token like any other; gate priority then promises
  admission order, not a fixed test duration. The supervisor's separate nice placement below ("Bands") orders the
  classes by lowering nice, never by raising one above default.

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
  - **Admission waits say what they wait for.** A jobserver request that remains queued after its admission transition
    receives that transition's queue snapshot in its queued acknowledgement, before waiting for any token. The client
    writes `cowshed: jobserver wait start` to stderr with repository (or explicitly unknown), class, workspace, its
    one-based waiting position, all current token grantees, the queued entries ahead, naming each workspace, admission
    job number and command, and the typed cause: the gate job heading its repository, or tokens alone ("One gate per
    repository"). Gate position is in its repository's gate order; background position is in its checkout's FIFO on the
    host's checkout-fair ledger, not an invented repository FIFO or a promised global grant order. Holders are separate
    from queued positions. The snapshot is the broker's decision, not a client-side status poll. A request granted
    immediately prints no wait. A request lent the probation token ends the phase with
    `cowshed: jobserver probation start elapsed=<waited>` and its grant later with `wait done` ("A one-token job starts
    on probation"). Grant or refusal ends the phase with `cowshed: jobserver wait done elapsed=<waited> status=ok|err`;
    transport EOF is an error, never a grant. Supervisor admission forwards these lines through the job's live stderr,
    including before any command exists, so the waiting exec hears them rather than only the daemon log. Cancellation
    still closes the owning connection and leaves the queue. This adds no admission deadline and changes no grant order.
    `cowshed gateway status` reads this same ledger without changing it: its existing `cpuBudget` projection shows token
    grantees, gate requests and background waiters, and every `gateJobs` and `gateQueue` entry names its `repository`,
    including a repository whose land queue is empty. `--json` preserves that projection in the normal success envelope;
    text names the repository alongside each gate job, lease and queued request.
- **The task server owns the live grant and nice.** Work enters the existing `cowshed exec` / workspace supervisor, not
  a gate-host or taskpolicy wrapper. Its typed request carries class, want and scope. Only a trusted
  coordinator/operator may request Gate or HostGate; a worker request is refused with typed class-not-permitted before
  job or spawn effects. Ordinary commands default Background and Workspace scope. A request that names no want asks for
  its class's own: background and interactive work the background share `⌊N/2⌋`, a gate the one token its orchestrator
  uses ("A land is a gate job"), a resident none; a named want is taken as named. An ordinary command's caller cannot
  know how many runners it starts, and what it starts draws on its grant ("Work a holder starts draws on its grant").
  Measured 2026-10-21 with a default of one token: an `nx run-many` under `cowshed exec` handed that one token to its
  bounded-exec runners in turn, which waited 261, 473, 664 and 887 s for it, and the run took 21m19s against 5m54s on
  the host route. `JobScheduling::requested` resolves the want, the validation both surfaces share (07_api.md, "CLI and
  API exec parity"), from the share the supervisor's CPU admission reports; a keyed request binds its want as named, so
  an unnamed want is the same request whatever share each supervisor resolves it to (07_api.md, "Keyed admission and
  restart attachment"); a requester that awaits the result asks Interactive ("Bands"), which a worker may ask for, since
  it buys a band and no token; a resident (below) is Resident or InteractiveResident, which a worker may ask for, and
  names no want. The supervisor records Queued, asks the gateway off its actor path and spawns only after a real grant.
  **The exec answers at that admission, never at the grant** (11_shell.md, "Server-owned CPU admission and nice
  placement"): a job waiting in the queue has its id, reads `queued` with its class and no `cpuTokens`, and is
  cancellable, since a request that waits for no deadline must stay reachable for as long as it waits. Measured
  2026-10-19 with the answer at the grant: a `cowshed exec --background` on a loaded host returned 25 s later, so the
  executor's sequential admission of 100 held jobs stopped at the first waiting one, which had no id to report or
  cancel. CPU admission uses the control socket of the controller's validated host storage, the same store/gateway.sock
  authority its gateway reconciliation uses, never an unrelated ambient store. A queued kill withdraws the request
  without spawning. Missing/refused admission never causes an unbudgeted server spawn. The supervisor retains the
  allocation until the owned job concludes. The gateway's existing CpuBudget ledger serves that exact allocation on a
  private workspace-scoped CPU-pool endpoint; Nx bounded-exec and Cargo descendants borrow only from it, with sibling
  sums bounded by its actual count, or, for a gate job, through it as gate-class host requests. The root FIFO is not
  offered alongside the pool: that would spend the same tokens twice. Its canonical endpoint is
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
    work the budget had admitted, so the budget did not bound it). What a start activates is judged when the resident is
    admitted, from its exec hosts' activation state, as one of three (2026-10-23): **bare**, the command's directory
    selects no `.envrc`, so no shell activates, one-shot or pooled; **activated**, an exec host of that shell has
    already activated and waits, idle or as the spare of its pool's current generation; **activates**, the shell
    activates before the command: no activated host waits, or a one-shot child enters the shell itself (a supervisor
    without an exec host program, or the host boundary's command). A resident whose exec host has already activated asks
    no stake; its start costs nothing the stake bounds, and neither does a bare start: both go straight to their
    `resident-pool` request. Measured 2026-10-23 (`b20-resource-snapshots-71706`): every resident start asked the stake,
    so with about 17 long background `sh` jobs holding the background room, the next Start's resident (a bare one-shot
    worktree-basis read) waited for a token none of them would return, and the Start never finished. Before the
    `resident-pool` request of a start that activates, a resident's supervisor asks one background token for the
    resident's checkout with an activation stake,
    `{"op":"jobserver","want":1,"checkout":"<workspace root>","command":"<command>","class":"background","pool":false}`,
    which names no job and asks for no pool: it waits as any background request does, and the gateway answers
    `tokens: 1` and a fifo nobody reads. The resident asks its pool only once the stake is held, holds the stake while
    its shell activates, and closes it when its command starts, however its spawn reports that: a warm host's command
    with the job's `Started` event once the host has activated, a one-shot command when its spawn returns its process.
    From then on it holds no token, as above. The judgment reserves no host: a resident judged activated whose spawn
    must activate after all (another command took the host, or an input changed, since its admission) asks the same
    stake then, on the same `parentPool`, and holds it while the shell activates; a kill while it waits ends the job as
    it would have ended the activation. A one-shot child that enters a workspace shell runs that activation inside its
    own process, after the spawn has returned, so its activation is not under the stake; only a supervisor without an
    exec host program spawns one so. A stake that is refused fails the job as a refused admission does; a job that ends
    before its command starts returns the stake with its grant. The same stake bounds a shell pool's spare (11_shell.md,
    "One spare and a SIEVE cache"). The ledger knows no partial return, so the stake is its own one-token grant rather
    than a count the resident's pool would later give back.
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
    gate job waits for the daemon with no deadline. A start still waiting at the host when a gate job begins is that
    gate's work too (2026-10-22): once a gate job of the shed holds its grant, the supervisor withdraws the keeper's
    waiting background stake, which leaves the host queue, and asks it again with the oldest running gate job's pool as
    its `parentPool`, as if it had started under that gate; a start already drawing on a pool keeps it, and the answer
    of a withdrawn ask is dropped, never spawned. Measured 2026-10-22 in shed `icm-no-python`: the rebase's task-hash
    gate ended at 04:39:32, the keeper's start queued as background at 04:39:35, and the land's gate began at 04:39:46
    and waited on that daemon; the supervisor chose the stake's holder once, at the start, so the stake stayed a
    background ask for 2,214 s.
  - **CPU pool retirement.** Pool retirement unlinks its endpoint before closing its listener. It drains the kernel
    accept queue, not merely the runtime's readiness cache, and answers queued and already accepted connections with
    `{"ok":false,"code":"pool-retired","error":"the CPU pool owner's lease ended"}` followed by EOF. A new connection
    after unlink fails at path lookup; an already queued connection completes and hears the named refusal, rather than
    being abandoned during its handshake. The retirement log names the pool and counts queued refusals and accepted
    connections ended. Explicit close and owner drop use this same drain; neither aborts the listener task.
- **Work-conserving within repository admission.** Among eligible requests, with nobody else waiting, a gate-class grant
  is `min(want, free)`, so a lone gate runner may hold all N tokens, because a gate's wall time is what the budget buys.
  While no eligible gate-class request waits, a background grant is `min(want, free)`, gate work holding tokens or not,
  so no token idles while only background wants it ("Room for a gate"). Ordinary checkout fairness (the share rule
  above) still bounds a grant while another ordinary checkout waits. Repository serialization may leave tokens free when
  only a behind-head gate wants them; starting that gate is not useful concurrency.
- **Bands** (2026-10-17, Danny tg#6693: "The gate class is served FIFO at highest priority over all other execs. The
  interactive class has priority over gate and regular exec. Priority is handled by OS niceness. When gate runs it needs
  more tickets than background but not fully starve background"). A job's class fixes, at the job's birth, both its
  place in the token order and the band the host schedules it in. This table is the one statement of both; every other
  section names it.

  | Class         | What runs in it                                                                                                                                   | CPU tokens                                                                                                                                    | Band           | Nice |
  | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ---- |
  | `interactive` | work a person or model is waiting on, declared by the requester that awaits its result, and an interactive resident ("A resident holds no token") | behind gate, as background: checkout fairness and the floor token; an interactive resident holds none and its pool relays at background class | **foreground** | 0    |
  | `gate`        | a land's gate job or a train's proof, every task its pool relays and the land turn's holder; the trusted route only                               | first: the oldest gate job FIFO, ahead of every other class ("Gate admission")                                                                | **normal**     | +5   |
  | `background`  | every other exec, the default, a resident, and each request either resident's pool relays                                                         | checkout fairness over the free tokens, behind every waiting gate ask ("Room for a gate"); a resident holds none                              | **background** | +10  |

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
  oldest job first — the first is the head of the gate queue — each with its `repository`, `checkout`, its gate `job`
  (the job's admission number; the smaller is the older job), the tokens it `held` and its `holders`; `gateQueue`: the
  waiting gate-class requests in the order returned tokens go to them, each with its `repository`, `checkout`, gate
  `job`, the client's `pid`, the `command` it named, its `want` and `waitedMs`, the milliseconds since it asked; and
  `background`: each checkout's background-class stake alone (`held`, `holders`, and its waiting requests as `waiters`,
  each with `pid`, `command`, `want` and `waitedMs`), for the checkouts that have one; and `probation`, while the
  probation token is lent, naming its request's `checkout`, `pid`, `command` and `lentMs`, the milliseconds since it was
  lent, a request that still waits in `background` for its own token. Every waiting entry is one request: a gate job
  whose tasks wait is one entry per task — the job identity is the gate `job` number and each entry is a distinct
  connection — so the queue is never collapsed per job; its pid, command and age tell the entries apart (measured
  2026-10-19: fifteen `batch-7 gate job 603 want 1` entries were fifteen of that gate's tasks, and read as duplicates
  because they named nothing else). `holders` are the granted leases, one per runner holding tokens, oldest grant first,
  each with the client's `pid` (the socket's peer; absent when the socket names none — a pool-relayed task is named by
  its own connection to the pool), the `command` it named, the `tokens` it holds now, its `want`, and `heldMs`, the
  milliseconds since its grant. The age is the lease's, never the process's: a lease is one connection granted once, and
  a process that asks again holds a new lease with its own age; the ledger never scans processes, and `ps -p <pid>`
  gives the process's age. Measured 2026-10-18: a ledger that named only `land-test-hang: held 15, running 1` read as an
  orphan, and a live 34-minute `cargo test …` another unit was waiting on was killed by hand; its holder line would have
  said what it was and that it was live. `cowshed gateway status` shows this ledger to the operator in the order it
  serves — its count with what background holds (`gateway cpu budget: held <h>/<N> tokens, background holds <b>`), the
  request on the probation token (`gateway cpu probation: <checkout>: pid <pid>, lent <age> ago: <command>`), the head
  gate job, the gate jobs behind it, the gate queue, then background, each running gate job and background stake
  followed by its leases — in its human lines, and verbatim as `cpuBudget` in `--json` (06_cli.md; 07_api.md
  `GatewayStatus`), so whether queued lands wait behind the head of the queue without loading it, why background waits,
  and who holds every token since when, is visible rather than inferred; a daemon that does not answer `cpu-budget` is
  reported as such, never as an empty ledger.
- **The hold limit** (2026-10-19). A lease held past 30 minutes (`HOLD_LIMIT`) is named on the lines right after the
  ledger's count, oldest first, by checkout, pid, tokens, want, age and command
  (`gateway cpu hold past 30m: <checkout>: pid <pid>, <g>/<want> tokens, granted <age> ago: <command>`), so the first
  lines of `cowshed gateway status` answer who holds the host and since when. Measured 2026-10-19: a deployed-proof
  `cargo test … --ignored` integration test held 12 of 18 tokens for 48 minutes while a land's gate waited with 2, and
  its holder was found with `ps`. A background lease may hold the whole host, and a gate that arrives then waits for its
  returned tokens ("Room for a gate"). What the line names is a run that may hold tokens without working, as the
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
  - `cpu-tokens probation ticket=<id>` names the ask a transition lent the probation token, with checkout, peer pid and
    command; `lease-grant … lease=Probation` measures its answer, and `lease-grant … after=probation` its grant.
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
    `--test-threads` count the command names outranks them, as a flag outranks the environment. A gate's relay pool
    exports neither: its command is the orchestrator, every runner it starts asks the host and sizes its test threads to
    its own grant, and `bounded-exec` reads an inherited count as a ceiling, so the gate's own one-token grant would run
    every Rust suite under it on one thread (measured 2026-10-22: `tokens=1/1` in every gate runner since 10-20, and
    `cowshed-cli`'s 487 tests summing to their 1225 s wall). The supervisor sets these for a job (`CpuPlacement`), over
    a caller's own `CARGO_MAKEFLAGS`, `MAKEFLAGS` and `MFLAGS`, which it never passes; `bounded-exec` sets them for
    every runner it starts ("Sized to the grant"), and a target's `options.env` cannot name them. Cargo, every build
    script and every nested `cargo` a test starts inherit them and run rustc only on tokens read from that fifo;
    `cargo metadata` and every query read none, so nothing needs to tell a build from a query.
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
work-conserving cap — a lone background request granted the whole host with no gate work, a newcomer beside a land
waiting while background holds its half, and a gate arriving 50 minutes after background filled the host granted every
returned token but the floor token of the background checkout holding nothing — an older land served before a younger
land that asked first and both before a feature build that asked before either) and `tests/cpu_budget.rs` over a real
socket (`SIGKILL` of a holding client returns its tokens to the waiter at once; checkouts share the host; a gate job
arriving after a lone background grant filled the host queued, then with its task granted every returned token but one
background floor token; refusals, among them a `jobserver` request and a `cpu-pool` ask naming `grow`, each an
`invalid-request`; a holder at background's whole room whose child, queued at the saturated host, is granted at once
when asked with the holder's pool, its host ask withdrawn and the host ledger unchanged, while a pool that answers and
refuses refuses its child and an unreachable one leaves it to the host; a one-token holder whose token a sibling holds
has its exec granted by the host when the host has room, and the pool's next child is lent the token at once; with the
host full and the pool busy, the exec waits in the ledger and is granted by whichever frees first, a pool grant leaving
the ledger's queue; with every token held, a one-token job's ask answered `probation` at once with a pool that lends
nothing, a two-token ask never lent it, and a job whose probation ended passing the token on, keeping its place and
granted one token on the same pool and fifo, its pool then lending it); the ledger's probation unit test (lent at once,
returned by a leave or a probation's end, the probation's ask granted one token in its place, never more than the total
held); `cowshed-shell`'s `shell_pool.rs` through a real gateway, supervisor and sandbox (a one-token `git ls-files`
ending on the probation token beside a holder of every token, and a CPU-heavy one stopped at its bound, the token passed
on, then continued to its end by its grant); the plugin's `cpu-tokens.test.ts` (each runner's `want` and sizing, a
killed holder closing its grant) and `executor.test.ts` (a granted run sized and released, a refused one run as asked, a
target's jobserver variables ignored for the run's own fifo); `cpu-pool.test.ts` (a request naming `grow` refused like
any other unknown field) and the gateway's lent-pool test (a pool's fifo holds `g − 1` bytes, a child is lent the
implicit token while a reader holds the bytes, the next child waits until the reader writes them back, a child's close
puts them back); `tests/jobserver_fifo.rs` (above); and the band tests: each class runs in its own band at its own nice
whatever the host's state — interactive above gate above background — and gate work is served returned tokens ahead of
interactive and background work.

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
   frees them and every entry handed over after, one at a time, for as long as the gateway runs ("Deleting released
   images", below). It reads no mount and waits for neither step 3 nor step 4.
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

A released image is handed to the gateway, which frees it. Every image a cowshed operation removes is released, never
unlinked by the verb: a retirement (`rm`), the volume and seed a land or a `reseed` replaces, a superseded volume a
job's end releases, a fork's seed refresh, the staged clone a failed creation or restore leaves, a staged copy a
re-adoption leaves, and what `gc` collects (a retired workspace's trash, an orphan session or staging image, an expired
checkpoint). The operation commits the release with one `rename(2)` of the detached image, never replacing a file, into
a `.deleting` directory beside it: `build/.deleting/<id>.dmg` for a build volume, and for any other image of the project
`sessions/.deleting/`, a removed workspace's as `<ws>-<incarnation>.dmg` and any other as
`reclaimed-<fresh 32-hex>.dmg`. It syncs the directories it touched (16_build_volumes.md, "Garbage collection";
02_workspaces.md, `rm` step 5). The commit never opens the image, so it reads none of its bytes and needs no access to
it. No verb frees the bytes itself, because a verb that frees holds its turn and its locks, a land its land turn and an
`rm` its workspace's locks, for as long as the free takes. No releasing process ever truncates an image, and none starts
a process to do it: lands, forks and rebases never wait for a deletion. Measured 2026-10-25: two deletion children of a
land, started under its land turn, held the repository's land turn after the land exited, until their frees ended, and
nine children truncating released images at once saturated the store's container, with lsof, git, forks and execs in
uninterruptible waits behind them.

- **Every release is a hand-off, made once its rename is durable.** The releasing process holds no image lock while it
  renames and syncs, and returns at once. It hands the entry to the gateway of its host namespace (01_storage.md, "Host
  namespace") with one one-shot control request,
  `{"op":"released","entry":"<absolute path>","uniqueBytes":<n>?,"extents":<n>?}`, answered `{"ok":true}` once the
  gateway has queued the entry. The two counts are the releaser's record of the image: `uniqueBytes`, the bytes it does
  not share with its source, which freeing it returns, and `extents`, its physical runs, which freeing it costs. Each is
  present only when the releaser knows it: a seed's release sends `extents`, the count its publication proof read and
  its record keeps (at most 7141, where that proof stops; 16_build_volumes.md, "Targets and seeds"), and a release that
  walks no extent map (13_telemetry.md, "Native image observations") knows neither. If the gateway is absent, does not
  answer within the 2 s every admission's acknowledgement has ("Disk-lifecycle lease"), or refuses, the releaser says so
  once on stderr, and the entry waits for the gateway's next start: its rename is the release, so the verb still
  answers.
- **The gateway validates what it is handed.** An entry is an absolute path, with no `..` component, inside the
  gateway's store and in a `.deleting` directory; anything else is refused as `invalid-request` and queued nowhere.
- **One thread, `image-cleanup`, owns the queue.** The gateway starts it when it starts serving ("Startup contract",
  step 2), so neither the listing nor a free delays its readiness; until the thread has started, a hand-over is refused
  as `rejected`, and the listing the thread makes first queues that entry, which is already in its `.deleting`
  directory. That first step lists every leftover entry in the store: each project's `build/.deleting` and
  `sessions/.deleting`, the same two directories of every archived project (`.retired-projects/<operation-id>/`, where
  `forget` renamed the project's tree with its released images in it, 02_workspaces.md; the receipt files beside an
  archive are skipped), and the store's own `<store>/.deleting`. Leftovers go into the queue as entries with no record,
  oldest listed first. A stray, a non-regular file and a directory that cannot be read are named on stderr and left
  untouched. The listing looks nowhere else and does no sync: an archive's released images are no evidence, they were
  released before the archive's rename, and nothing else in an archive is opened. Entries handed over later join the
  queue with the record their releaser sent.
- **The next entry is a pure function of the queue and the pressure.** `cleanup_order::next(pending, pressure)`
  (`storage/apfs/cleanup_order.rs`) picks it, and the thread reads the pressure before each pick.
  - With no pressure, the entry with the fewest extents goes first: a cut's time grows with its extents, so each burst
    of container work stays short.
  - Under pressure, the entry with the most unique bytes per extent goes first: the most space for the least work.
    Unique bytes are the bytes the image does not share with its source: a clone's shared extents return nothing when it
    is freed, so its allocated or sparse size is never used. Ties go to the fewer extents.
  - An entry with no record goes last, oldest first, in both modes, and under pressure so does one without both counts.
    Entries equal in every other respect go oldest first.
  - Pressure is the store container's free space below 1/8 of its capacity, measured with `statvfs`:
    `f_bavail·f_frsize < f_blocks·f_frsize / 8`. One constant, `PRESSURE_FREE_FRACTION = 8`, holds the fraction. A
    container whose space cannot be read is under no pressure, and the thread says so on stderr.
- **One cut at a time, as soon as the queue picks it** (2026-10-26, Danny tg#11027, tg#11029: the queue serializes cuts,
  and never waits for gates). The thread frees one entry at a time, so one release cut runs per gateway, and therefore
  per container on a host, in the order above. It cuts the entry it picks at once, beside gate work too: nothing in the
  cleanup asks the CPU ledger or waits for a gate ("Room for a gate"). While it frees an entry it holds only that
  entry's own `flock`, never an image lock (`main.dmg.lock`, `sessions/<ws>.dmg.lock`; 01_storage.md, "Lock order") and
  never a token. Measured 2026-10-26 on the train-6 install, while each cut waited for gate work: lands ran nearly back
  to back, the wait reached 679 s with 86 released images in `.deleting`, and free space fell to 895 GB, below the
  pressure floor of about 931 GB.
- **One entry's free.** The thread `flock`s the entry without waiting. An entry another process holds is left, and said
  on stderr: its holder is a releaser still syncing its own rename, and that releaser hands it over afterwards. Before
  the first cut the thread syncs the entry's directory and that directory's parent, because a releaser may have died
  between its rename and the rename's syncs. It then cuts from the end of the file, at most 1,024 physical runs per
  `ftruncate`, at normal disk priority (16_build_volumes.md, "Garbage collection"; `IOPOL_THROTTLE` stays rejected,
  01a_apfs.md, "Freeing an image"), unlinks the empty file and syncs the directory. The file's length is the journal: a
  cut ends on a run boundary, so an entry cut short is finished from its length. The thread says on stderr each entry it
  frees, with its bytes and cuts. An entry that is gone when its turn comes (freed already, or moved by a final
  unbinding) needs nothing; one whose free fails is said with its cause and stays in `.deleting` for the gateway's next
  start.
- **A restart finishes what it cut.** An install's restart stops the thread mid-entry, and the next gateway's startup
  listing finishes what it left: so a gateway restart or a reboot (the gateway is `RunAtLoad`) frees every entry left,
  an image cut short mid-free included.
- **What is never done.** No verb or other releasing process truncates an image, starts a process to free one, or holds
  an image lock through a free. The gateway applies no `IOPOL_THROTTLE`, and no environment variable drives the queue:
  the thread waits only on its hand-overs, and it has no timer.

Space lags. Until the cleanup frees an entry the store's container still holds its allocation, so free-space figures and
extent counts lag the commit by the entry's wait in the queue and the free's own duration, and an entry the gateway was
never handed waits for its next start. Nothing references an entry: no record, link, hold or mount names it, and nothing
mounts, forks or reads it.

Two cases hand nothing over. A store whose configuration names no gateway (a fixture store no test gave one) says so on
stderr once per process, and its entries wait in `.deleting` for a gateway's start. A directory store, the test
substrate, only renames: its release moves the payload into `<project>/sessions/.deleting/<name>` (a build volume into
`build/.deleting`), the verb deletes nothing and hands nothing over (said once per process), and a released payload
stays in `.deleting` until its test store goes, because a tree can be freed only with the directory host's holder
census, which runs in-process; `DirectoryExecutionHost::finish_build_releases` can finish build-volume trees, leaving
one a process still holds (16_build_volumes.md, "Garbage collection").

A project's final unbinding (`rm main --restore`, 02_workspaces.md, "Checkout layouts") frees nothing either. Its store
tree goes with it, so before it plans terminal storage it moves each released image of its project, from
`build/.deleting` and `sessions/.deleting`, into `<store>/.deleting/<fresh id>.dmg` and hands each one over like any
release. A cut the gateway is making moves with its entry, which the cleanup holds open, and the entry's new name is
handed over with the rest; a stray left in those directories still refuses the plan.

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
