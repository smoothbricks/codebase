# Warm Shell Layer

`cowshed-shell` is the process-management layer between `cowshed-core` (which owns _what may run where_ — substrate,
mounts, sandbox profiles, grants) and every client that runs commands (CLI, MCP, CI, NAPI). It provides one long-lived
supervisor per workspace holding warm exec hosts with the workspace shell already activated, a framed stdio protocol
over a Unix socket, job control, and the single exec-record capture that all clients consume.

## Shell activation and process reuse

A fresh workspace is a CoW clone of its origin (main, or the workspace it was forked from): the same inputs, already
installed. What a fresh host lacks is the activated environment, and activation dominates command latency: **a warm
`direnv exec` on a devenv repo costs seconds per invocation, and a cold devenv evaluation at a new path costs a minute
or more** (74.6 s measured on a devenv monorepo), dwarfing `sandbox-exec` startup and process creation. Cowshed
therefore activates once per lineage: a host evaluates only when no verified export exists for its checkout, and reuses
the activated process for every command until the bytes of an input direnv recorded for that activation change.

**Exec hosts.** A command whose cwd lies under a workspace-contained `.envrc` runs in a warm exec host: a small cowshed
process started under the executed-child profile, in its own process group, with the sandbox environment
(04_sandbox.md). The host approves the `.envrc` in the workspace's private trust store and evaluates it once with
`direnv export json`, applying the exported diff to its own environment; the repository's entry hooks run exactly as
they do under `direnv exec`. For each command it receives the job's stdin, stdout and stderr descriptors over its
control socket (`SCM_RIGHTS`), forks the argv into a new process group with the requested cwd and the caller's
environment laid over the activated one, and reports the raw `waitpid` status. Nothing a command does — `cd`, `export`,
a changed umask — reaches the host or a later command: every command is a fresh process from the same activated
environment. The host program is staged under `.cowshed/shell-host/`, which every profile denies writes to, so no job
can replace the parent of later commands. A workspace with no `.envrc` above the command's cwd gets hosts that skip
activation and hold the sandbox environment itself; its argv jobs keep one-shot spawns, its script jobs run in those
hosts. Cowshed has no other shell backend: a devenv project activates through `use devenv` in its own `.envrc`.

**A host never spins once its mount is gone.** A host runs its program from the workspace mount, so once that mount is
gone (a forced unmount, DiskArbitration restarting) the next page-in of code it has not yet run fails with SIGBUS. The
host restores SIGBUS to its default disposition before it serves, so that fault ends it. The Rust runtime's
stack-overflow handler would otherwise catch the signal, and its own code lies on the same unreadable pages, so the
fault repeats forever: on 2026-10-22, after DiskArbitration restarted and dropped the shed mounts, 37 of 62 hosts spun
in R state with high system time (host load 48) until killed, and a host woken by its controller's end of stream after a
forced unmount reproduces it. So a host whose mount is gone ends one of three ways, never by spinning: an idle host
keeps blocking in its control read; a host that runs only code already resident exits as it would have; any other dies
by SIGBUS at its first page-in after the mount went. No job waits on a dropped host, so its supervisor writes how it
ended to its log unless it exited 0, naming a SIGBUS as a page the host's program could not be given, and says so too
when it cannot collect the host at all.

**Inherited shells.** Every activation whose inputs stayed put while it ran (below) leaves its export in
`.cowshed/shell-host/activation.json`, written by the supervisor, so the APFS clone carries it to every workspace forked
from this one, and a fork of a fork carries it again. A host that starts with no live generation adopts that export
instead of evaluating when four checks pass, and evaluates otherwise (the checkout the record was written in needs only
the first two: nothing moves):

- **The inputs are the same bytes.** The record keeps the activation's watch list, each input relative to the checkout
  root, and its key: SHA-256 over every input, ordered by relative path, as (path, whether it exists, its bytes). The
  adopting supervisor certifies the inputs at its own checkout — identities, bytes, identities again, refused if
  anything moved between the readings — and recomputes the key. The cowshed-owned inputs whose bytes differ per
  workspace by construction, `.cowshed/env` and direnv's trust files under `.cowshed/data/direnv/`, are not in the key;
  their effect is read at each checkout (below).
- **The baseline is the same.** direnv's export lists, for each variable the activation changed, its complete new value
  (or a removal) relative to the host's pre-activation environment, so it is valid only over an equivalent one. The
  record keeps the baseline's key: SHA-256 over that environment with the checkout's places (below) replaced by
  placeholders, every withheld name (below) and cowshed's per-workspace git identity (`GIT_CONFIG_GLOBAL`) left out.
- **Nothing names the origin after the move.** Every value has each of the origin's per-workspace places — its root, its
  runtime link (`XDG_RUNTIME_DIR`), its `TMPDIR` and its `HOME`, root first — replaced by the adopting checkout's
  wherever a path starts there and ends a component (`/x/repo` moves `/x/repo/tooling`, `-I/x/repo/include` and
  `-Wl,-rpath,/x/repo/lib`, never `/x/repo-worktrees` or `/w/x/repo`); devenv's `DEVENV_RUNTIME`, a hash of the dotfile
  path, is recomputed as devenv computes it. The check after the move is stricter than the move: a value in which an
  origin place still ends a component anywhere, or that holds a number inside the origin's port block, refuses adoption,
  since a false refusal costs one evaluation and a false pass would hand the clone the origin's tree.
- **The producer is certified.** No check of the values can see one derived from a hash of the checkout path (an
  `.envrc` exporting a hash of `$PWD`) or from a fact cowshed does not publish, so a record moves only once its input
  generation's producer has been measured at two roots. A clone whose inherited record is uncertified evaluates, as
  every clone used to; when its own export, for the same input bytes over an equivalent baseline, equals the moved one
  variable for variable (withheld names by name), the supervisor writes its record with the proof — the two roots — and
  gives the origin's record the same proof if that record is still the one the clone read (compared by its bytes'
  SHA-256). Clones of either then adopt. Any disagreement leaves both uncertified, names the variable on the
  supervisor's stderr, and every later clone of that generation evaluates; a hash of `$PWD` therefore never reaches a
  clone. The cost is one evaluation per input generation, by its first clone. The two-root fixture (08_testing.md) runs
  this end to end, the `$PWD` hash included, and a devenv monorepo's export was measured against a fresh evaluation of a
  fork (of its 71 variables, 51 equal as recorded, 16 equal after the move, `DEVENV_RUNTIME` recomputed,
  `DIRENV_WATCHES` rebuilt, `COWSHED_RUNTIME_LINK` read from the tail, and `DEVENV_TASK_FILE` dropped), the comparison a
  certification makes.

The record never carries direnv's own `DIRENV_WATCHES` and `DIRENV_DIFF` (the host rebuilds both for its checkout),
`DEVENV_TASK_FILE` (the parent's task file runs scripts built with the parent's root; tasks and processes are not tools)
or a `shellHook`, nor any variable the secret convention names — cowshed's workspace and cache tokens, proxy URLs,
`*_TOKEN`, `*_SECRET`, `*_KEY`, values of the 02_workspaces.md token shapes, URLs with userinfo — nor anything
`.cowshed/env` publishes per workspace. Without that filter every fork would re-copy every ancestor's secrets,
multiplied by the number of workspaces and the depth of their lineage. Those values are not lost: the record names which
ones the export set, and the adopting host reads them from its own checkout's tail files — `.cowshed/env` and
`.envrc-local`, those of the two the activation's watch list names, in its order — through direnv's own bash, stdlib,
`source_env_if_exists` and `direnv dump json`, exactly as the activation read them. The withheld names start unset for
that reading, so a value the host already holds never stands in for one the activation exported, and a tail that does
not supply one of them refuses the adoption, the host's environment untouched, and the host evaluates. So adopted and
evaluated shells obtain those values one way. This is non-propagation, not isolation: the files they come from are in
the image either way, and moving them out is the gateway vault's.

Adoption runs no direnv evaluation, no devenv, no entry task and no approval; only the tail files run, and their output
reaches the job that waited on the adoption as an activation's would. A miss says why on that job's stderr
(`cowshed: evaluating the workspace shell: …`) and evaluates as before. A lost or unreadable record is a miss, never an
error, and a record that cannot be written is reported on the supervisor's stderr and only means clones evaluate.

**Script jobs.** Besides a byte-exact argv, a job can be a script: bash-compatible shell text as a template of literal
`parts` and `values` (`ExecCommand::Script`, 07_api.md). At admission the supervisor binds each value to a shell
variable of its own and puts a reference to it where the value stood, quoted for that position — `"${v}"` in an unquoted
word, `"${v[@]}"` when a list is a whole bare word (one word per element), `${v}` inside `"…"`, `'"${v}"'` inside `'…'`;
`$(…)`, backquotes and process substitution start a fresh unquoted context. A value therefore never reaches the shell as
text: no quoting in a value can end its word, start a command or expand anything. A value inside a comment, inside
`$'…'` or directly after an odd run of backslashes is refused before admission. Rendering does not detect the one
context where the shell reads a variable's content as more than data: arithmetic (`$((…))`, `((…))`, `let`, an array
subscript, an integer variable, an arithmetic `[[ … -eq … ]]` operand), where bash and brush evaluate the content as an
expression, so a value placed there can assign shell variables. Clients refuse those placements before submitting. The
host parses the text with an upstream `brush` parser, so a script that does not parse fails before anything runs: the
job ends `failed` with `failure: scriptSyntax`, status 2 as bash gives, and the parser's diagnostic on its stderr. A
script that parses runs in a child the host forks without exec, the way bash runs a subshell: the child leads the job's
process group and is the only process that creates it (two creations of one group race, and macOS refuses the loser with
`EPERM`), and it reports once the group exists, so the host gives the supervisor the pid only then and the supervisor
never signals a group not yet made. A child that cannot lead its group says why on the job's stderr and exits 126. The
child resets every signal the host handles to its default as an exec would, holds the job's descriptors as 0, 1 and 2,
builds a brush interpreter from the already-activated environment (no activation, no rc files) with job control off,
runs the program and exits with its status. External commands inherit the job's group, so a group signal never reaches
the host. A descendant can leave that group with `setsid` or `setpgid`; group membership alone is therefore not a
descendant-ownership proof. A killed script dies by the signal and reports it exactly; `cd`, `umask`, `ulimit`, `trap`
and `exec` end with the child. The interpreter lives only in the host binary (the `cowshed-shell` crate), not in the
supervisor library or its Node addon.

Retirement means every retained running member reaches an observed exit; it does not mean the caller may reap processes
whose parent is another process. An exited, unreaped zombie still answers `kill(pid, 0)`, so a null signal is not
evidence that a killed descendant survived. Consumers retain a kernel identity-fenced exit watch while the member is
running, then wait for that member's actual exit event. The actual parent alone reaps its children; the command leader
stays unreaped until release, and the independent host must remain usable.

Leader exit and both output EOFs begin nonterminal `draining`, not terminal publication. The actor retains the process,
CPU and volume claims while its native membership operation snapshots the outliving processes, sends TERM, waits the
existing grace, ends survivors and joins their exits. Only a successful retirement reply permits sealing the terminal
record and releasing the leader. A refusal keeps the claims and the nonterminal job, and reaches waiters as an error; an
explicit cancellation or retirement can retry it. The durable `containment`, `outlived` and `outlivedIncomplete` receipt
names the proof, the live members retirement signalled (including a live leader on cancellation), and whether native
retirement may have ended members it could not name. Rows preserve each known observed-tree index, pid, image and last
signal; a successful receipt leaves no scoped survivors. The marker defaults false and is omitted on the wire when
false. Empty rows prove no member was ended only when the marker is false. A never-born job has no containment, empty
rows and a false marker. Kernel-proven emptiness is not an exhaustive metadata census.

On Darwin membership is the held leader's group plus the existing observer's retained running lives, including lives
that called `setsid`. The receipt is always `observed`, never `kernel`: `NOTE_FORK` can coalesce, and a forked child can
leave before observation attaches it. `lost` records detected attachment/parent-read gaps; an empty `lost` is not a
completeness claim. Age, quiet output and zero CPU are not authority to retire a running job. A keeper-owned foreground
Resident is a separate declared job and survives another job's conclusion; its own leader exit uses the same draining
rule. Client detachment neither creates OS-process ownership nor transfers it.

**A command that cannot start is named.** A failed `execvp` names the program it was given, whatever was missing:
macOS's `sandbox-exec` prints `execvp() of 'tooling/codegraph' failed: No such file or directory` and exits 71 both when
that script is absent and when it exists and is executable but its `#!` interpreter is not there. So before spawning an
argv command, in a one-shot child, a warm host or a host gate, the supervisor follows the links the kernel will: the
program (relative to the cwd, or looked up on the job's PATH), the interpreter its `#!` line names, and the name an
`env` interpreter looks up on that PATH. The first one proven absent (`ENOENT` or `ENOTDIR`) refuses the spawn as
not-found (wrapper exit 102), naming it on the job's stderr, and the job fails with `failure: unlaunched`. Only absence
is judged: a path the supervisor cannot inspect, an unreadable script, `env` with options, and any name looked up on a
PATH that shell activation sets inside the sandbox are the kernel's to decide, so this never becomes a second sandbox.
Nothing rewrites the command: the kernel still runs the interpreter, and `$0` and `BASH_SOURCE` stay the script's.

**Process identity and signal ownership.** The process that creates a command's group observes its leader's immutable
birth identity before anything can reap it, including a leader that exits immediately. One-shot commands and activation
hosts are direct children held by a parent-owned reap/signal fence. A job's leader is observed exiting **without
reaping** (`WNOWAIT`) and remains held until the complete job concludes: descendants can outlive the leader, with or
without keeping its output pipes open. A later `kill`, quota stop, or retirement therefore still owns their exact group
and retained observed lives. Only after both pipes end and membership retirement succeeds does the parent release and
reap the leader. Signals and reaping serialize on the fence, without holding a blocking lock across an await; a
released/reaped id grants no future signal rights. Birth-observation failure never becomes a ledger claim, but the
actual parent's unreaped child remains its own. A stop is not an exit: Darwin reports a stopped child to a `waitid` that
asked only for exits (measured on Darwin 25.6), so the exit read takes that report with a wait that cannot reap
(`WSTOPPED | WCONTINUED`, never `WEXITED`) and reads again; a leader that stops its own group stays held and signalled.
A warm command's signals are framed `REQUEST_SIGNAL`s to its actual parent host. The host reports the observed birth
with `REPLY_STARTED`, and its exact typed exit with `REPLY_EXITED`, while keeping the leader unreaped. It continues
serving signals until the supervisor concludes the job and sends `REQUEST_RELEASE`; `REPLY_RELEASED` acknowledges actual
reaping, and only then may that host serve another command. Failed signal delivery is `REPLY_SIGNAL_FAILED`, not silent
success. Signal/release while no command is held, malformed frames, and unknown tags fail closed.

Parent-side output/diagnostic writers are dropped as soon as the command starts, not kept until release: otherwise EOF
would wait for release while release waited for EOF. Every post-fork failure and controller EOF ends the host's own held
command group with TERM, the existing grace, KILL, and reap. The supervisor never force-kills the host before that
command retirement can run, and no stale start reply can authorize signalling another process. Retirement is a fallible
actual-parent operation. A failed TERM, membership read, KILL, or reap retains its operation and native cause; later
cleanup steps are still attempted. A controller EOF invokes that same operation explicitly, and the host reports failure
rather than acknowledging successful retirement. The host reports the preserved native failure before attempting a reply
that may fail on the closed controller socket; a refused reply retains its native cause too. Destructor fallback
attempts fallible stderr/stdout reporting without panicking or hiding the original caller error; if both channels are
unavailable, delivery cannot be guaranteed. Consumers never clean up an old descendant PID or group ID after an exit or
release: they close the actual parent-control capability, which still owns any held command.

**Held leaders: `cowshed hold`.** The fence above is one public cowshed-core capability,
`runtime::child_fence::ChildFence` (07_api.md "Held leaders"), and
`cowshed hold --control-fd <n> --state-fd <m> -- <cmd…>` gives it to a caller that is not cowshed: a watchdog ending a
step's group, for one, never signals a pid or a group id itself. `hold` spawns the command as the leader of a new
session, and so of a new process group, that it stays outside of. A stop or kill of that group never reaches `hold`, so
it always answers: a custodian inside the group is stopped by the very whole-group `SIGSTOP` it would have to answer.
The command's argv, environment, working directory, standard streams and every inherited descriptor but `n` and `m` are
the caller's, byte for byte. `n` and `m` are adopted close-on-exec before any runtime exists and are never the
command's, and once the command runs `hold` keeps no copy of its standard streams, so they end with its group.

- **State.** `m` carries one `HoldEvent` per line, as JSON, read with the generated validator of that type.
  `started {pid, birth, output}` names the leader, its birth identity and the descriptors it was handed as 1 and 2, in
  the form `lsof -F di` prints them: the pipe or socket address (`d`) on Darwin, the inode (`i`) otherwise, read
  natively from `hold`'s own copies before it lets them go, so a caller names their later holders from one host-wide
  `lsof -F pfdi` instead of a probe per step. `exited {status}` is the leader's exit, observed without reaping it, in
  the `ExitStatus` form every job's exit has. `signalled {signal}` and `notSignalled {signal, reason}` answer each
  signal. `failed {reason}` says no command runs and is the last line. `commandFailed {pid, reason}` says `hold` failed
  once its leader ran -- before `started`, when it could not give up its standard streams, or after it. No `release` can
  arrive any more, so `hold` ends the group as at the end of `n` before a release, answering each signal on `m`, then
  releases the leader, answers each signal it receives, and ends with `exited` once the leader is reaped. When ending
  the group, that release or that wait fails, a last `commandFailed` says what failed.
- **Control.** `n` carries one request per line. `signal <SIG>` sends `kill(-pgid, SIG)` only while the leader is held,
  and is answered `notSignalled` with the reason once the leader was reaped, when its birth was never observed, or when
  the kernel refuses. `release` reaps the leader once it has exited; signals are still served until the reap. A release
  is explicit: the end of `n` before one is a controller gone, and `hold` ends the group as a warm host ends its held
  command's group on controller EOF (`job_groups::end_held_group`: SIGTERM, the grace while any process of the group
  still runs, SIGKILL), answering each as `signalled` or `notSignalled`, then reaps the leader once it exits. A caller
  that dies therefore leaves no process of the group behind; one that wants the group to outlive it sends `release`
  first, after which the end of `n` changes nothing. A line that is no request is `commandFailed`.
- **Exit.** Once the leader is reaped, `hold` exits with its status as a shell reports it: the code, or 128 plus the
  signal. A failure of `hold` itself uses the wrapper codes `exec` uses. SIGINT, SIGHUP and SIGTERM that `hold` receives
  are sent to the group as a `signal` request is, and answered on `m`.

Under a lock it composes as `cowshed lock <lock> --state-fd 3 -- cowshed hold --control-fd 4 --state-fd 5 -- <cmd…>`:
the waiter, the caller's direct child, keeps its `SIGUSR1` request path, and the step's group is the hold's. Each path
has a real-boundary test: a leader that stops its whole group, ended through `hold`; an exited leader held as a zombie
while `signal TERM` still ends a descendant holding its output; the end of `n` before a release ending the whole group,
a member that ignores SIGTERM included; an explicit release letting a running leader outlive its controller; a signal
after a release; the exact argv, environment, working directory and descriptors; `started.output` against `lsof` (or
procfs) of a descendant; a malformed request, which ends the group; a command that cannot start; a signal `hold`
receives; and `hold` under `lock`.

**Freshness is direnv's own.** direnv records every input an evaluation depended on — the `.envrc`, its approval files,
each `source_up`, `use devenv` and `watch_file` target — as `DIRENV_WATCHES` (base64url of zlib-deflated JSON
`[{path, modtime, exists}]`) and reloads when any of them changes. The supervisor decodes that list from each
activation, subscribes the listed paths with the kernel (kqueue `EVFILT_VNODE` on macOS, inotify on Linux; the nearest
existing ancestor for an input recorded as absent), and keeps the exact identity — inode, size, mtime and ctime at the
filesystem's timestamp resolution — each had when the activation finished. The device number is not part of an identity:
it numbers the mount, not the file, and a clone of a volume keeps every file's inode, size, mtime and ctime under
another device, so comparing it reads an unchanged clone as changed. direnv's whole-second `modtime` is never compared.
Where timestamps are coarse (a scheduler tick on Linux without multigrain timestamps, a second on HFS+) two same-size
writes inside one tick share an identity, and the kernel subscription is what reports them. When an activation finishes
the supervisor also certifies the inputs' bytes: identities, SHA-256 of each input (of its sorted entry names for a
directory, which `watch_dir` records beside every file under it), identities again; an input that moved between the
readings leaves the activation unstable. **Bytes decide; identities and events only ask.** A kernel event, or an
in-process stat of the listed paths that finds an identity changed (the backstop for coalesced events), makes the pool
certify the inputs again at the next decision point, never at the event: bytes equal to the generation's mean the
rewrite changed nothing the shell read (a checkout, a stash, an editor's save of the same text), and the generation
takes the new identities, resubscribes (a replaced file is a new vnode) and stays warm; any other bytes, or bytes that
cannot be certified, set the pool's dirty flag. The next command acts on the flag: every idle host and the spare are
retired and a fresh host activates for that command (adopting first, which misses because the key moved, then
evaluating); a host still executing finishes its command and is dropped instead of returned. An activation whose own
inputs moved while it ran — an input the previous generation listed changed identity across the evaluation, or a newly
listed input's ctime is later than the evaluation's start — serves the command that paid for it and is never reused. The
start is read off the workspace filesystem's own clocks, never the process clock, which runs up to a tick ahead of the
stamps a coarse clock gives. One filesystem need not stamp every change from one clock: ZFS on Linux stamps writes from
the kernel's coarse tick clock, and attribute changes (creations too, under POSIX ACLs) from the VFS clock, which on a
kernel with multigrain timestamps (6.13 and later) a fine-grained stamp on any other filesystem of the host moves up to
a tick ahead of it. So the supervisor probes the protected host directory — it creates a file, writes to it and changes
its mode, reading the ctime after each — and probes again until a probe's earliest stamp is later than the first probe's
latest. That latest stamp is the start: every change before it, the approval's included, stamps no later, and every
change after the probing stamps no earlier than the crossing probe's earliest — as long as the clocks run forward. A
ctime between the two readings is not proven older than the start; a change made while the supervisor probes has no
causal order against it. A wall clock stepped back while the evaluation runs (a VM's time sync, an NTP step) stamps a
change made during it earlier than the start, so the supervisor reads the slowest clock again once the evaluation's
inputs are snapshotted: an end reading earlier than the crossing proves the clock ran backwards, no newly listed input
counts as older than the evaluation, and the activation is never reused. An end reading at or after the crossing does
not rule a step back out: a clock stepped back and forward again past the crossing reads exactly like one that ran
forward, was slewed or stamps coarsely, and the filesystem's stamps are the only clock there is to read (a monotonic
clock beside them drifts from the wall clock by slewing alone). A newly listed input written while the clock was stepped
back can still count as older, and the shell serves until that input changes again. An input on another filesystem,
which may stamp in whole seconds, must predate the start's second. A change to a path not on the list costs nothing. The
repository decides what else counts as shell input with `watch_file`: lockfiles and devenv inputs whose change must
rerun shell entry. devenv leaves directory inputs off the list it hands direnv (only files can be `watch_file`d), so a
directory a devenv module reads, such as a package's build recipe a source filter is rooted at, is named by the
repository with direnv's `watch_dir`, which records every file beneath it. An input missing from the list is the one way
a warm or inherited shell can serve stale: neither the identity check nor the content key can see it.

**One spare and a SIEVE cache.** Hosts are pooled per shell identity: effective sandbox mode, `.envrc` directory, grant
revision, the exact sandbox environment, and the host program. A read-only command never runs in a read-write host, and
no host activated under one grant revision serves another. Each pool keeps one activated spare ahead of demand and a
bounded cache (four hosts) of hosts that are executing or were just returned; a full cache evicts by SIEVE. A command
takes the most recently returned idle host, else the spare — whose replacement starts activating in the background —
else activates a host of its own. Commands never queue for a host. One actor owns each pool's state; nothing locks. A
supervisor that lives only as long as a short-lived controller process keeps no spare, which would end unused with it. A
spare's activation runs for no job, so no job's grant bounds it: it first asks one background token of the workspace's
checkout with an activation stake (05_gateway.md, "A resident's start holds one token") and holds it until its
activation ends, whatever ends it. A stake that is refused, or a gateway that cannot be reached, is a failed activation
like any other: there is no spare, and the next command activates for itself.

**Activation belongs to the job that waited on it.** A command that activates a host receives the activation's output on
its own stdout and stderr, and until its command starts that host's process group is the job's: a kill reaches the
activation, and a failed activation fails the job with the activation's status and discards the host. A warm command's
streams carry only its own output. A spare's activation output is discarded; its failure only means there is no spare.
An activation whose supervisor goes away — dropped the host, or its process exited — ends with its whole process group,
since nothing can use its result. A host that cannot serve a request — it cannot run `direnv`, say — answers with the
reason and exits; any host that breaks its protocol fails the job waiting on it as a launch that failed
(`failure: unlaunched`), with the reason on the job's stderr, since its command never ran. The one exception is a host
that died by a signal while it activated for the job: a kill of the job reaches its activation, and a crash is the job's
to see, so that death is the job's status (the reason still goes to its stderr).

The layer's other roles:

- **Sessions** — a named session carries a cwd and an environment overlay across calls; each of its commands runs in a
  warm host like any other and sees the session's overlay, never another command's exports.
- **The framed stdio protocol** — multiplexed, backpressured, language-neutral I/O for concurrent CLI, NAPI, MCP, and CI
  clients, instead of each reinventing pipe plumbing.
- **Job control** — waiting, caller timeouts, backgrounding, re-attach, structured capture.

The CoW clone and warm build caches are distinct from shell reuse. Neither justifies bypassing repository activation or
replaying a supervisor-owned environment in place of its shell entry hooks.

## Supervisor

One supervisor per attached workspace, the workspace's sole job allocator, running as a process of its own:
`cowshed __workspace-supervisor <project-root> <workspace>`, which serves it on the workspace's socket (Protocol,
below). The gateway daemon owns these processes through its supervisor manager, listening at `<store>/run/manager.sock`.
A controller asks the manager to ensure the workspace's supervisor under the incarnation and grant revision it read, and
the manager answers with the socket once a supervisor serves it there or at a newer grant revision of the same
incarnation. Grant revisions of one incarnation only grow, so a newer one is a grant change — another process's gateway
reconcile moving the port block, a `grant` — published between the controller's read and the supervisor's own; the
controller's calls then name the authority the supervisor reports, and it never runs under grants older than the ones it
read. A supervisor serving an older revision re-reads the grants before it answers. It starts one with the daemon's own
binary, in a session of its own, when nothing answers; ensures of one workspace run one at a time, so two controllers
never start two allocators. A supervisor therefore outlives the command that first needed it, and the daemon's restarts:
the manager watches every supervisor it started and, when it starts, finds the ones still serving. A supervisor
answering under another incarnation is refused with `Conflict` naming its pid. A supervisor that ends before it serves
says why on a report pipe the manager hands it (named by `COWSHED_SUPERVISOR_REPORT_FD`, close-on-exec in the supervisor
so no job inherits it), and the ensure — so the command that needed the workspace — fails with that error and its code,
not with a pointer to the daemon's log. The supervisor itself never evaluates `.envrc`, sources shell startup, or runs
repository hooks; it reads only the watch list an activation reports. Before a spawn it refreshes the convention-gated
capability snapshot (15_capabilities.md) and compiles the matching child profile if that snapshot changed. Every exec
host, one-shot command and descendant starts beneath that restriction, in its job's own process group. The child profile
denies writes beneath `.cowshed/job/**`; ReadOnly narrows the selected authority and puts tool state in the private
exec-temp roots. Every job's Node and Bun load cowshed's port-0 preload, named in `NODE_OPTIONS` and `BUN_OPTIONS` after
the caller's own options and rewritten into the private environment before each spawn, so a port-0 listener and a
child's `--inspect…:0` land on a port of the workspace block the job can connect to (04_sandbox.md, "A sandboxed job
binds its listeners in its block"; 03_caches.md lists the variables).

- Holds the warm exec hosts above. Host startup and activation run inside the child sandbox; no repository-controlled
  startup runs in the supervisor.
- **Named sessions** (`--session <name>`, `WorkspaceHandle::shell(Some(name))`) keep a cwd, an environment overlay, and
  the set of their background jobs until explicitly closed or the supervisor stops. They hold no process of their own.
  This is how a coordinator gives one subagent a stable working directory and environment for a multi-step task.

Execution cwd has one representation across `ExecRequest`, supervisor/session state, `JobInfo`, JSON, and Arrow:
`Option<WorkspacePath>`. `None` denotes the workspace mount root; `Some(path)` denotes exactly one validated, normalized
workspace-relative path. Wire `cwd` is required and encodes `None` as JSON `null`; omission rejects, and neither an
empty path nor `.` is admitted as a root sentinel. Execution argv likewise has one byte-exact representation.
`ExecRequest.command` and `JobInfo`'s command are one `ExecCommand`: an argv or a script (above), flattened on the wire
as exactly one of `argv` and `script`. An argv is `Vec<CommandArg>`, where each immutable element owns an `OsString`. On
Unix the shared serde codec emits exactly `{encoding:"utf8",data}` iff the bytes validate as UTF-8 and otherwise
canonical standard base64. It denies unknown fields/encodings and rejects malformed/non-canonical base64, base64 for
valid UTF-8, decoded NUL, arguments above 128 KiB, aggregate argv above 1 MiB, and empty argv/`argv[0]`. A script's
parts and values are UTF-8 strings without NUL, bounded together at the same 1 MiB, with exactly one more part than
values. Validation precedes RPC, job/artifact effects, process allocation, and spawn. The supervisor consumes arguments
into `OsString` for `plan_exec`; no `String`, lossy rendering, or alternate supervisor wire shape exists.

- It retires itself after 30 minutes with no named session open and no job running, freeing its warm hosts; the next
  command starts another. A shed's supervisor also waits for its Nx daemon to be gone, which its keeper stops five
  minutes after the shed was last in use (04_sandbox.md). `cowshed detach`/`cowshed rm` retire it before the substrate
  changes (teardown below). Every retirement, and a project runtime's shutdown, which retires each supervisor that
  process serves, stops and waits for all of the supervisor's jobs, including the foreground keeper maintenance job,
  through the ordinary job lifecycle (16_build_volumes.md, "The Nx daemon stays on"). No daemon or plugin worker
  outlives the supervisor or the runtime.
- Admission closes only on the way to retirement, and nothing reopens it: a supervisor that refuses work is one that
  retires, whoever asked and whether or not they are still there. So a caller in another process asks one request,
  `retire`, saying when — now (running jobs stopped), after the running jobs end, or only if no job runs (a `Conflict`,
  changing nothing, while one does) — and the supervisor carries it out on its own. Closing admission without retiring
  (quiesce) is for the supervisor's own process alone, which retires it afterwards itself; the wire has no such request.
  Measured 2026-10-21: a supervisor whose 17 jobs had all ended refused every exec as "quiescing or retired" for hours.
  A caller had quiesced it and then failed to send the retire that was a second request; `attach` changed nothing, and
  its idle retirement never fired either, because the Nx keeper probes only while the supervisor runs, so "idle", which
  waits for the keeper to settle, never became true. Every caller in another process that stops a supervisor goes
  through this one request: `detach`, `rm`, `grant` growing the port block, `doctor --repair` moving a colliding port
  block, and re-adopting main.
- Its opening waits for no image lock (07_api.md, "Authority model"): it finishes nothing. While a live lifecycle
  operation holds the workspace's lock (a fork of it, `main`'s included, its `rm` or its defragmentation), an already
  settled mount is served as it stands, and one that needs settling waits for the lock after saying once on its stderr,
  the daemon's log, which processes hold it by PID and command. Nothing but the manager's start bound ends that wait.

## Protocol

Each supervisor listens on a Unix socket at `<store>/run/<digest>.sock`, where `digest` is the first 32 hex digits of
the SHA-256 of `repo_id`, a NUL, and the workspace name. Owner, repository and workspace names together can exceed the
104 bytes a socket path may hold on macOS; the digest keeps every path at 64 and no two workspaces share one. The
serving process creates `<store>/run` mode `0700`, so no other user reaches a socket in it, binds the socket there and
sets it `0600`, and verifies each connecting peer's uid before reading a byte. A persistent no-follow file lease at
`<digest>.sock.lock` serializes binders and stale-socket replacement. The bound listener and lease are one owned value,
held throughout serving or a workspace mutation. A supervisor of another build must finish the other-build drain before
a new build binds; ensure refuses the old build while it still serves, and the lifecycle stop waits for its exit. Stream
refusal is not absence: a live Darwin listener can refuse when its backlog is full. Recovery probes the exact filesystem
socket with a datagram connection; wrong-type or connected means a socket remains attached to that vnode, including one
inherited after its creator exited. Only a proven unattached socket is removed under the lease. New listeners bind
privately in mode `0600` and publish with one exclusive rename. Cleanup removes only the bound listener's own inode
while holding the lease. Retirement releases the listener and lease **before** acknowledging success.

The same serving actor also binds `<digest>.members.sock` under its own listener lease, with the same framing and uid
check but a separate closed request grammar: exactly `subtreeHandle` and `retireDescendants`. Unknown operations are
refused by name. Only this membership endpoint is granted to job sandboxes and exported as `COWSHED_SUPERVISOR_SOCKET`;
the privileged coordinator endpoint remains inaccessible. Caller-provided values cannot redirect the exported endpoint.
Initial policy and grant advancement use the same endpoint grant. Retirement closes both listeners before acknowledging
the supervisor's retirement.

A spawn copies every descriptor of the spawning process into the child, which holds the copies until its `exec` closes
the close-on-exec ones; a listener or lease closed meanwhile by another thread is not released — the socket stays
attached (a client connects, then reads end of stream) and the lease stays held (a peer sees `Conflict`). One
process-wide fork lock (`cowshed_core::fork_lock`, Go's `syscall.ForkLock`) closes that window. Every child
cowshed-core, the CLI and the gateway spawn is spawned holding it shared across the spawn call only, which returns once
the child has run `exec` or failed to, never across a wait; spawns stay concurrent. Every descriptor whose release a
peer observes — the supervisor listener and its `.sock.lock` lease, the gateway's workspace session and control
listeners, the APFS image lease and raw-device pin, image lifecycle locks, the intent lease and journal lock, the job
records lock, cargo's cache locks — is fenced from the moment it is opened: its close takes the lock exclusive, waiting
only for spawns in flight. A socket, which Darwin cannot create close-on-exec in one system call, is also created
exclusive, or a child forked between `socket` and `fcntl` would keep it for life. A job's pipe pair is close-on-exec
from birth too: `pipe2(O_CLOEXEC)` on Linux; on Darwin, which has no `pipe2`, it is created and marked under the
exclusive lock (`fork_lock::create`), so no unrelated spawn holds a job's end of stream open. cowshed-core's clippy
configuration refuses unlocked `spawn`/`output`/`status`. This includes test fixture children, such as the pinned Nx
cache adapter's version-refusal probe. Gateway listener binds and accepted-socket creation use that same exclusive
creation step, including close-on-exec publication; asynchronous readiness waits hold no fork-lock guard.

The supervisor retains the job's diagnostics stderr before it acquires a warm host. If cloning that descriptor fails,
the job fails with `EnvironmentMissing`, naming the clone operation and its syscall cause (including `EMFILE` for
descriptor exhaustion); no command starts. The reason is written through the original stderr descriptor. Retained
diagnostics are written through a borrowed file, never another descriptor clone, and their writer closes as soon as the
command starts so it cannot hold output EOF open.

Each non-streaming call is one connection: the client writes one JSON request frame, then the raw bytes the call carries
(inline stdin, a stdin chunk) as one more frame; the supervisor answers with one JSON response frame, then the raw bytes
the answer carries (a log chunk), and both sides close. A frame is a big-endian `u32` length and that many bytes. A long
call — `wait`, a following log read — therefore holds only its own connection: no call queues behind another, nothing is
multiplexed, and a client that disconnects abandons only its own call, never a job — except `hold`, below.

`processEvents` holds its own connection. The supervisor validates the caller's authority and opens the retained native
subscription before acknowledging it. The client then sends the stream-lane demands defined in
[07_api.md, “A stream-lane call answers each demand”](07_api.md#napi-streaming-frozen). Each `next` receives one
canonical process event, an end, or an error; `close` remains readable while a demand awaits an event. Disconnecting or
closing drops only that subscription, never the job. No public cursor or long-poll operation is exposed.

`hold` holds its own connection too, and is the one call whose disconnection reaches a job (05_gateway.md, "A job lives
with its requester"). The client reads the served authority from `hello` and refuses a supervisor serving another
incarnation than its job's before anything is held, so a grant advance between admission and hold changes nothing. The
supervisor validates the authority and the job and acknowledges with `null`. It then answers the job's terminal
`JobInfo` when the job ends, or `null` again when the client sends the demand `release`, which lets the job outlive the
connection. A connection that closes before either has left a running job with nobody waiting for it: the supervisor
kills it under the authority it serves then, and says so on its stderr.

- **hello** — the supervisor answers with its build, the authority it serves (repository, workspace, incarnation, grant
  and lifecycle revisions) and its pid. A client reads the build first and refuses any other by name (`Conflict`), so
  two cowshed builds never exchange a call either cannot decode. A build is the Mach-O `LC_UUID` the linker derives from
  the binary's contents (the SHA-256 of the executable on Linux): no number anybody has to remember to bump, so a change
  nobody announced still counts. The manager refuses an ensure from another build the same way, and its refusal carries
  both builds as data — `otherBuild: { daemon, caller }` on the `Conflict` — so a client tells a controller the daemon
  will no longer serve from every other conflict without reading the sentence. Its sentence is "the cowshed daemon is
  build X; this cowshed is build Y", and its hint "run `cowshed setup` from the cowshed you mean to use: it installs
  that build as both the daemon and the `cowshed` on PATH" (05_gateway.md "The installed cowshed"). A refusal at the
  first ensure comes after a verb's first step — a removal has by then stopped its workspace's supervisor and journaled
  its intent, and the refusal comes again on the path that would put that back, "state restoration also failed" — so
  every CLI verb that needs a supervisor first sends the manager `sameBuild`, a request carrying nothing but the
  caller's build, before it opens the project (06_cli.md "Onboarding and repair" lists the verbs). The manager checks
  the build before it reads the rest of any request, answers `sameBuild` before any other work, and refuses another
  build's with the same `Conflict`, so the verb is refused before its first step and the host is left as it was. No
  manager answering is not a disagreement: there is no other build to refuse the caller, and a verb that needs the
  daemon says so itself. The controller client notes the first other-build refusal on its connection
  (`Coordinator::other_build`, 07_api.md); a supervisor of the controller's own build that still drains its jobs keeps
  answering the calls that reach it. A program that links cowshed as a library is another build whatever revision it
  links, so it opens no project in-process: it runs its controller as `cowshed controller` (06_cli.md), a process of the
  host's own build, and speaks the controller protocol to it over the socket it hands that process; after an install has
  started the daemon of a new build, it starts that verb again for its new work.

  Only a hello proving that nothing serves the socket starts a supervisor: no socket file, or one no socket is attached
  to any more, whether the connection was refused or the peer ended it unanswered (a supervisor exiting closes its
  listener with a hello in its queue). A hello that times out, a socket still attached that refuses or ends the
  exchange, or an answer that cannot be read is no proof either way, and an ensure returns it as itself, naming what
  happened, and starts nothing. A 10-second hello timeout on a saturated drive is indistinguishable from a dead
  supervisor; measured 2026-10-21, an ensure that read it as a vacancy started a second supervisor beside one that had
  served the socket since it bound (the socket file's modification time is when its listener bound), and the second
  failed as "a supervisor or lifecycle operation owns" the socket, costing its caller a unit's time on a misleading
  sentence.

- **advance** — the supervisor re-reads its workspace's grants and serves under their revision from then on
  (Grant-change propagation, below); answered with the authority it serves afterwards.
- **drain** — the supervisor admits nothing more, lets its running jobs finish, then retires; answered at once with its
  pid. This one request keeps its shape across builds (Supervisor recovery, below).
- **commitments** — the controller commitments the supervisor recorded after a cursor, waiting up to 30 seconds for one
  when there is none; **acknowledgeCommitments** forgets every one through a cursor. Every commitment goes to the
  supervisor process's own sink, the host's default; a controller whose sink is its own reads them here, records them
  into it, and acknowledges them. The supervisor keeps up to 65,536 unacknowledged commitments and then drops the
  oldest, which the next read reports.
- **calls** — `openSession`, `sessionSnapshot`, `closeSession`, `exec`, `stdinWrite`, `stdinClose`, `streamChunk`,
  `streamEnd`, `info`, `sealed`, `jobByKey`, `list`, `kill`, `wait`, `logRead`, `checkpoint`, `retire`: one per
  supervisor operation, each naming the authority the caller holds. The supervisor fences every call by it exactly as it
  fences an in-process one, so a caller holding a stale incarnation or grant revision is refused, not served under the
  wrong profile. A first `exec` allocates its numeric `jobId` before process creation; a keyed repeat answers the
  durable existing id and spawns nothing. A spawn failure is therefore a terminal job, not a response with no identity.
  `jobByKey` reads the exact incarnation's admission records after a lost reply or supervisor restart; only proven
  absence in that current authority returns `None` (wire `null`). Missing workspace/job, stale authority, transport,
  storage and observation failures remain errors; unreadable keyed history is an explicit unprovable refusal, never
  absence. `info`, `list`, `kill` and `wait` answer the supervisor's own jobs; `sealed` answers a job's terminal record
  from the workspace's records — state, exit, failure, duration, output limit and both streams — for any job of the
  incarnation that has one, including a job an earlier supervisor ran and sealed, and `logRead` reads such a job's
  sealed streams from any offset as it reads its own terminal jobs'.
- **stdin** — empty, inline bytes (the request's raw frame), a workspace-relative regular file opened inside the
  sandbox, a reader-backed stream, or explicit `open` with no reader or automatic EOF. A reader's `streamChunk` calls
  enter its bounded source channel in order and `streamEnd` names clean EOF or the source error. Attachment `stdinWrite`
  carries a byte offset and answers only after its frame is fully written to the child's pipe; `stdinClose` waits for
  all accepted writes, then sends one EOF. Admission to a source channel is not the delivered cursor. Bytes are never
  interpolated into shell text.
- **stdout/stderr** — `logRead` returns the bytes of one stream from an offset, following (waiting for the next bytes)
  when asked. Capture begins in a bounded in-memory buffer while SHA-256 and the combined quota advance over the exact
  admitted bytes; a stream promotes lazily to a protected file when it exceeds the inline bound or when backgrounding,
  checkpointing, or replay requires filesystem-resident bytes. A slow or absent reader never blocks capture, and a
  reader resumes from the representation-transparent offset whether the protected artifact is terminal inline Arrow
  Binary or a file.

The protocol is transport for every client — the controller behind the CLI (06_cli.md), NAPI, and the MCP server
(12_mcp.md). JSON is bounded control/result transport: it may carry a tagged, bounded inline artifact, but never an
unbounded stdout/stderr stream. Controller commitments never carry output payload.

Monitoring operations and their request/result/event records come from the same canonical API declarations as the N-API
bindings (07_api.md, Job monitoring). Clients consume the supervisor's samples and journal accounting; no adapter shells
out to `ps`, scans the checkout, or recaptures output. A progress subscription begins with the latest sample, advances
while the job runs even when it writes nothing, and ends with the same final sample that is sealed in its terminal
result. Slow subscribers may coalesce intermediate samples, never the final sample or journal bytes. Status, tails,
cancel, and other job admissions remain serviceable while another client follows progress or waits.

## Job control

Every exec submission is a job. At admission the supervisor allocates a positive `u64`-backed `jobId` in `1..=2^53-1`,
monotonically increasing within that workspace and never reused; exhaustion is typed `Conflict`. Decimal rendering is
canonical, with no prefix or zero padding. The protected record stream is `.cowshed/job/records.arrow`; optional spill
files, created only on promotion, are:

```
.cowshed/job/<jobId>/out
.cowshed/job/<jobId>/err
```

Stdout and stderr are separate opaque byte streams admitted under one combined quota: no UTF-8 assumption, line
rewriting, redaction, merging, or summary substitution. The supervisor begins each stream in bounded memory and creates
neither per-job stream path eagerly. When promotion is required it exclusively creates the job directory and selected
file without following links, writes the buffered prefix, then appends future admitted bytes. Executed shells receive a
child profile that denies every write mutation beneath `.cowshed/job/**`; no writable protected-artifact descriptor is
inheritable and no protected inode may be hardlinked into a workspace-writable path. Completion drains, hashes, fsyncs,
closes, and seals file artifacts, or writes bounded inline bytes into the terminal Job Arrow batch, before publishing
`ControllerCommitment::Terminal(TerminalCommitment)`.

Allocation is exclusive and crash-safe without an eager job directory. One supervisor is the sole allocator for an
attached workspace. Admission appends a complete `ProtectedRecord::Job(JobArtifactRecord)` batch and publishes
`ControllerCommitment::Admission(AdmissionCommitment)` before process creation, so spawn failure retains durable
identity. A record's sequence comes from the `records.sequence` counter, published by atomic rename before the record is
appended under the same lock, so a crash may leave a gap but never hands a sequence out twice. At startup the supervisor
takes the maximum canonical `job_id` across valid complete in-volume allocation batches, controller commitments for the
active lineage, canonical spill directories and the checked `records.job-floor` counter, then chooses `max + 1` (or `1`
when none exist). Any disagreement or duplicate durable key is `Integrity`, not a reason to reuse a number.
Open/recovery requires every record's `repo_id` to equal the workspace's bound repository. A record incarnation may
differ from the current marker only when the marker's fork/restore lineage names it as an ancestor or controller
fork/restore/checkpoint lineage and commitments admit it as inherited history; an unknown historical incarnation, or any
new allocation under a non-current incarnation, is `Integrity`. Thus copied histories are intentional but cannot smuggle
a foreign repository/timeline. Supervisor replacement and attach otherwise discard only an incomplete trailing Arrow
batch and never rewrite a complete batch or sealed artifact. Fork/checkpoint/restore copies start above every inherited
allocation, including the maximum id retained by `records.job-floor` for a store set aside unread. That floor record
never decodes or retrofits the earlier store's rows.

**Durability of a job's own records.** A job's records — its admission and terminal batches, its spill files (made
durable when it backgrounds, sealed when it ends), the output copies it publishes, and its admission and terminal
commitments — are written for every exec and survive the death of the process that wrote them, not power loss: each
takes exactly one `fsync(2)` of the file it appends or creates, and no directory sync, before the job is admitted,
backgrounded, answered as ended, or its copy reported published; commitments queued together share one segment and its
one `fsync(2)` (13_telemetry.md, "Group commit"); the sequence counter's rename is not synced at all. `F_FULLFSYNC` is
what survives power loss on macOS, and on a disk image it flushes every dirty block of the whole image — tens of
milliseconds to seconds right after a build — so it stays on lifecycle and authority state only: workspace creation and
removal, landing, checkpoint manifests, grants and policy revisions, and every lifecycle commitment. A checkpoint's
manifest batch is appended after it makes every running prefix durable, so its one `F_FULLFSYNC` carries those prefixes
past power loss with it. Power loss ends every job anyway, and may take any suffix of the records written since: a torn
trailing batch is discarded as above; a counter rolled back behind the log is advanced to the log's highest sequence at
the next open, before anything allocates; and when the next supervisor takes the workspace's socket — which no other
supervisor then holds — it seals every job of the incarnation that has an admission and no terminal record `failed` with
`supervisorLost`, after ending the process groups its predecessor's ledger still names, and seals a spill file the power
took as a stream with no bytes. A job whose terminal record was lost is therefore reported lost, never as having
succeeded.

Each admission and terminal job batch also records its immutable command in a required Arrow `List<Binary>` `argv`
column. An argv job stores its arguments; a script job stores exactly two elements, `\0script` and the script's JSON — a
first element no argv can have. Recovery requires the canonical schema and non-null Binary elements; an argv needs a
non-empty first element, no NUL, and the same per-element and aggregate byte bounds before reconstructing `CommandArg`,
and a script needs its JSON to decode to a valid script. This preserves non-UTF-8 Unix argv across crash recovery and
rejects malformed complete batches as `Integrity`; protected storage never downgrades argv to Arrow Utf8.

Record layouts grow only by trailing columns, and each build reads exactly its own layout. A store that holds a
complete, intact record in an earlier layout (one row whose leading `record_kind` and `record_version` columns declare a
version below this build's) is set aside whole the first time this build opens it. Under the records lock, recovery
reads only the common allocation header of each intact frame, the `record_kind` and the `job_id` every layout keeps in
its fifth column: a job's id is required and checked, and only a checkpoint manifest has none. A frame whose header is
missing, malformed or mislabeled refuses the set-aside as `Integrity` before anything is written or moved. The highest
id the store handed out raises the checked `records.job-floor` counter beside the records: an inline job leaves no job
directory, so without that floor the empty store would hand its id out again under the same incarnation. Then
`records.sequence` and every `records.barrier.*` move into a fresh `.cowshed/job/set-aside/layout-<v>/`
(`layout-<v>.<n>` when that name already holds records), and `records.arrow` moves last: an open interrupted mid-move
meets the same earlier layout again and finishes into the first such directory that holds no records yet. Recovery
reports the set-aside store and says so on stderr, an empty store starts, and new job ids are allocated above the floor,
the job directories that stay, and every frame. The checkpoint manifests in those records go with them: a checkpoint
taken before the set-aside no longer attests to anything in the new store. Nothing else of an earlier layout is read,
and nothing is projected from one. A complete, intact batch that begins with every column of this build's layout and has
more (a job record in it declares a version above this build's) was written by a newer cowshed: recovery refuses it as
`Conflict`, naming the record's layout and the newest this build reads, and never truncates, rewrites or seals it. A
torn trailing batch is cut as above only in a store that stays; a store being set aside moves with its bytes as they
are, torn suffix included. A batch in no layout at all, an earlier layout claiming this build's version, this build's
layout claiming another version, or a batch holding other than one row is `Integrity`.

A set-aside keeps and never decodes: the earlier store's bytes are moved whole into `.cowshed/job/set-aside/`, never
deleted, and nothing is projected from them, so no cwd, clock or other canonical evidence is retrofitted onto an older
row. Its admission keys are not inferred absent either: while the set-aside store may have served the current
incarnation, a key the new store lacks is typed `Unprovable` and a keyed exec under it is refused, never run a second
time.

Numeric sealed lookup follows [07_api.md, “Captured-content authority”](07_api.md#captured-content-authority).

Checkpoint manifests derive from the same authoritative snapshot, not from which rows earlier readers happened to ask
about. A controller-minted immutable `workspaceIncarnation` disambiguates histories copied by fork/checkpoint/restore.
Each create, fork destination, and restore result receives a fresh incarnation; inherited records retain the incarnation
that produced them, and the new allocator starts above the inherited maximum. Thus the durable job key is
`(repoId, workspaceIncarnation, jobId)`: `jobId` is the familiar workspace-local handle, while the full tuple remains
unique across checkpoint copies and recycled workspace names.

**Detached birth, client attachment.** A job is owned by its supervisor from admission, not by an open tool call. A
monitoring executor requests detached admission immediately; a long-running command never moves to another execution
mechanism because a caller stops waiting. Foreground CLI presentation is an attached view of that same job. A caller
disconnect, dropped handle, soft deadline, or progress-subscription close does not terminate it. Explicit cancellation,
quota enforcement, and workspace retirement terminate owned membership; a soft timeout does not. An embedding executor
persists its operation-to-`(repoId, workspaceIncarnation, jobId)` binding before acknowledging admission and reopens
that job by its number after it restarts; reopening never executes the command again. The admission contract closes the
spawn-before-reply crash window as specified in 07_api.md.

- **Foreground commands wait.** A foreground command's output streams to the caller while it runs, and the caller waits
  for its end however long it runs; there is no default timeout. A caller that sets one (`--timeout`) reads the job's
  status when it passes: a job still running is detached and the caller is told it still runs, never given a success
  that stands in for the command's; a job that ended meanwhile is not detached — its output is drained to EOF and its
  exit is the command's, because a finished job has nothing to reattach to. Before acknowledging detachment, the
  supervisor promotes each memory-resident stream prefix to its protected file; the files then keep growing and
  `job-backgrounded` fires. The client already has the job id and can poll or reattach through the API. A later
  checkpoint uses the barrier/manifest protocol below.
- **`--background`** detaches and promotes immediately, and the caller gets the job id.
- **Hard timeout** (`[shell] hard_timeout`, unset by default, set by CI — 10_ci.md) sends SIGTERM, then SIGKILL after a
  grace, drains both pipes to EOF, and marks the job `killed:timeout`. It is distinct from the CLI's `--timeout`, which
  bounds how long the caller waits before the job is backgrounded and never kills.
- **Combined output quota.** Each job has one configurable quota across stdout and stderr, default **1 GiB**. Accounting
  includes protected bytes plus bytes read from either child pipe but still buffered/in flight. The first read whose
  inclusion would cross the quota atomically trips the limit: the supervisor admits no payload beyond the exact
  boundary, sends SIGTERM to the complete process group, waits the configured grace, SIGKILLs stragglers, and drains
  both pipes to EOF without retaining post-boundary payload. After drain and artifact sealing, the authoritative
  terminal state is `output-limit`, with configured limit and observed crossing recorded. Summary truncation remains an
  independent bounded projection.
- **Re-attach**: `Job::attach` re-opens stdio to a running job from the client's last acknowledged stream offsets,
  representation-transparently across memory/file promotion.
- **Logs**: `Job::logs` resolves `StreamInfo.storage.artifact`; callers never need to distinguish inline Arrow Binary
  from a protected file and no path is promised.

### Job resource and journal facts

Every live sample and terminal result carries the canonical `JobResourceSample` defined in 07_api.md. CPU user/sys time
covers the job's complete process group, including exited members; CPU share is normalized to one core, so a parallel
build can exceed 100%. Current RSS is the group's resident-byte sum, and peak RSS is the maximum observed group sum, not
a sum of unrelated per-process peaks. A leader that exits before descendants does not end accounting: the same ownership
fence that retains the group for cancellation retains it for sampling through terminal drain.

On macOS each sample carries the job's CPU share over the supervisor's sampling clock window, its leader/children rusage
totals reconciled against a polled census of the group, and the census's CPU-winning leaf.

Each sample names its native `jobId`, including a standalone progress event or a completed-output projection. The start
boundary is the first job-owned process: the activation group on a cold host, otherwise the command. Activation receives
real progress while it runs, and a failure preserves its cost. When the command starts, current leader/membership move
to its group, but wall time, host-start, and volume baselines stay at that first spawn; CPU accumulates activation and
command, never idle time inherited from a reused warm host.

### Complete process tree, compact observations

The supervisor observes fork, exec and exit, not just membership at a poll. macOS uses kqueue
`NOTE_FORK`/`NOTE_EXEC`/`NOTE_EXIT` on each member and reads a forking member's children with `proc_listchildpids`,
since kqueue gives no child PID: its per-process coverage is best-effort with a coverage gap. Its job accounting retains
the declared leader-own plus reaped-children rusage source and its limits (07_api.md, "Complete job accounting and
observation reconciliation"). Linux combines pidfd identity and proc metrics with an event-capable source, chosen by
measuring proc connector `CN_PROC` through the privileged helper against ptrace fork/exec/exit tracing on the same
fork-heavy workload. Polling alone cannot claim complete short-lived descendants. Missing events are explicit coverage
gaps, never a shortened tree presented as complete.

`JobHandle.processes` and `process_events` expose the canonical records in 07_api.md through both controller and N-API,
generated from the same declaration. The records carry per-process CPU user/sys microseconds, the share of one core over
the last sample window with that window, current/peak RSS, read/write I/O, busy/idle state, observed blocker kind and
evidence, and exact exit state. A lock can name its holder PID and job; absence of blocker evidence never means `none`.
No shell flavor or argv classification sets an expectation for these observations.

The supervisor emits one `process.run` span per process under the job's trace, with parentage following the process tree
and birth/exit as its boundaries. Each unchanged live process has one heartbeat a minute, independent of any API
sampling/subscriber interval; changed-state and terminal rows are immediate. These rows use the fixed shared columns in
13_telemetry.md; job-level host load and volume deltas stay on the job span. There is no per-process, per-metric, or
per-sample column proliferation, and no JSON string payload.

Linux job totals come from the job's cgroup v2: CPU `cpu.stat`, charged memory `memory.current`/`memory.peak`, and
storage I/O `io.stat`; those totals include short-lived descendants the process observer missed. Charged memory includes
cache/kernel charges and remains distinct from RSS. macOS reconciles against the leader's own/children rusage totals.
Per-process rows and independent totals are compared in named units; differences produce explicit unattributed CPU/I/O
rows on the job span. A coverage gap does not turn totals into a guessed live-process sum. The leaf is the observed
process with the most own CPU, named only when the observed processes cover a declared fraction (initially 90%) of the
job's exact CPU total; otherwise the sample says why none is named, and it cannot seed a leaf-keyed baseline.

Workspace and build-volume used-byte deltas are separate volume statistics, never a tree scan or a claim about
per-process write syscalls. They compare usage at spawn with usage at the sample or terminal boundary and may be
negative after deletion. Concurrent writers on the same volume contribute to its delta; this fact remains explicit. No
unrelated shared-store/container free-space counter may substitute for usage of the owned volume.

The supervisor counts bytes and newline-delimited lines as it admits each stream, retaining partial-line state across
reads. The journal cursors are independent stdout/stderr byte offsets and survive inline/file promotion. A bounded tail
resumes from those cursors without replaying an already-read prefix; omission requests the latest bounded tail. A final
unterminated non-empty line counts as a line. Tails preserve raw bytes and expose their next cursors and truncation,
never substitute a summary or merge stdout and stderr.

An unavailable kernel sample reports a typed operational error with its cause; missing CPU, RSS, membership, load, or
volume evidence never becomes invented zeroes. Job output and the actual exit status remain preserved.

### Scoped descendant retirement capabilities

A bounded command inside an admitted job does not acquire authority from a pid, cwd, temporary directory, process age,
or CPU silence. `cowshed subtree-root --handle-fd N -- <argv...>` asks the membership endpoint about its own live
connection peer. Darwin's audit peer token fences that read; the monitor must already retain the exact unique life in
this job's immutable ancestry. The wrapper writes a newline-delimited admitted/refused JSON value to the inherited
writable descriptor, closes it, then execs the command without changing pid or life. Operational failures are errors; a
typed coverage refusal still runs the command without a RAM lease.

An admitted value carries an opaque `{jobId,index,incarnation}` capability. The random root-issued nonce is retained on
that fold life, not inferred from its index or supplied birth data. `cowshed retire-descendants --grace-ms N` reads only
the inner handle JSON from stdin and asks from its own live connection. That peer must belong to the same admitted job
even if it possesses another job's capability. The requested grace may shorten, never exceed, the supervisor's
termination grace. A successful retirement spends the capability; replay is typed `SpentHandle`, unknown or forged
handles are `ForeignHandle`, and an operational failure retains the capability for explicit retry.

The existing monitor writer retires the root and its retained descendants, including children reparented after the root
exited. It never expands a subtree into the whole job group: sibling commands stay alive. A naturally ended subtree's
first retirement succeeds with no signal rows and then spends its capability. TERM/grace/KILL joins native exits before
success. The receipt is `Observed { lost }`, never a Darwin completeness claim; its `outlived` rows name the lives
signalled, not survivors. Those rows also remain in the whole job's later terminal receipt. The same
`outlivedIncomplete` vocabulary distinguishes incomplete native kill metadata from membership proof.

Linux currently returns typed `FoldUnavailable`: whole-job cgroup membership is not retained per-command ancestry. The
caller takes no RAM lease and reports that fact, just as on a no-supervisor runner. A cwd/TMPDIR census remains a
failure-only diagnostic; it cannot signal processes, refuse a gate, or become a fallback ownership authority.

#### Separate unit: Linux per-command cgroup admission (LINUX-COMMAND-CGROUP)

This independently scoped unit admits a controller-owned child cgroup for a bounded command before its wrapper execs,
under the existing job's cgroup owner. It must issue a job-bound unforgeable capability, preserve membership through
fork, setsid and double-fork, retire only that child cgroup, and join kernel emptiness before releasing the RAM lease.
Acceptance requires a sibling command and an unrelated same-cwd process to survive, foreign/spent capabilities to be
refused, and cancellation/refused-start cleanup to leave no members. It must reuse the existing cgroup owner and ledger;
live parent-chain reconstruction and whole-job kill used as subtree retirement are forbidden. Until that unit is
implemented and verified on Linux, `FoldUnavailable` and the explicit no-lease path remain the supported behavior.

### Readiness and attachment stdin

`JobHandle.listening_ports` reports the current TCP LISTEN ports whose sockets belong to identity-proven members of this
job's process group. It includes listening children, supports IPv4 and IPv6, and never mistakes a port owned by an
unrelated host process for readiness. The supervisor reads kernel socket ownership through the same birth-identity fence
as group sampling; a failed ownership read is a typed error, not an empty census. The controller and N-API reach this
query through the operation bindings generated from the one API declaration (07_api.md, "DTO freeze (single source of
truth)").

Explicit Open admission keeps stdin available without starting a reader. `JobStdin::write` / `close` in Rust and the
generated N-API `JobAttachment.write` / `end` preserve arbitrary binary views, at most 64 KiB per wire frame, with
delivery backpressure. The cursor is the known fully-written prefix, never merely queued bytes; its accepted end is
retained once, not recalculated by scanning the lane. Closing waits even when the pump owns an unfinished write and the
channel slot is free. EOF is sent once, repeated EOF is idempotent, and later writes refuse.

Offset replay delivers no bytes twice: equal retained ranges are acknowledged, unequal ranges are typed conflicts, and a
range outside retained evidence is explicitly unprovable. If the pipe fails partway, `DeliveryUnknown` names the last
known cursor and is not safe to retry; every later input operation preserves that refusal. Closing stdin, detaching a
view and cancelling the process group remain distinct. Neither EOF nor detachment silently kills the job.

### Exec records, stream storage, and tiered authority

Every job emits protected lifecycle records to `.cowshed/job/records.arrow`. The stream travels with checkpoints and
complete batches written by the trusted supervisor are authoritative for captured content within their recorded origin
`workspaceIncarnation` and checkpoint-manifest boundary. Executed jobs may read but cannot write, truncate, rename,
unlink, link, or replace protected records or artifacts. Terminal completion is a record-batch boundary. After an
unclean supervisor exit, recovery may discard only an incomplete trailing batch or uncommitted bytes beyond a checkpoint
manifest; malformed or mismatched complete data is `Integrity`, never child-authored input to reinterpret.

Every job's record carries `envHash`, the hash of the environment the job was spawned with, beside its `grantRevision`:
a consumer sees the same `grantRevision` and `envHash` needed to reproduce a run.

The shared DTO is exact:

```rust
enum ProtectedOutput {
    Inline { data: BinaryData },
    File { path: WorkspacePath },
}
enum OutputStorage {
    Captured { artifact: ProtectedOutput },
    Redirect { source: WorkspacePath, artifact: ProtectedOutput },
}
struct StreamInfo {
    storage: OutputStorage,
    bytes: u64,
    sha256: Sha256Digest,
    summary: OutputSummary,
}
```

`BinaryData` is a bounded byte newtype. Arrow stores Binary; JSON/NAPI uses the exact wire union
`{encoding:"utf8",data:"…"} | {encoding:"base64",data:"…"}`, choosing `utf8` iff the bytes validate and bounding both
branches by decoded length. Ordinary `JobInfo` may contain this bounded value; controller commitments never do.
`File.path` is a protected workspace-relative path and exists only after lazy promotion. `Captured.artifact` is the
canonical full-fidelity content. `Redirect.source` names an AST-proven real-shell `>`/`2>` workspace destination written
during execution; it is mutable caller-visible state and never authority. Its `artifact` is an independent protected
post-terminal snapshot of the exact admitted bytes, inline when small or clone/reflink/copied to a sealed protected file
when large.

For queued/running jobs, `StreamInfo` is a bounded current view and its artifact is not sealed content authority yet.
Inline data may be the supervisor's current bounded buffer; background acknowledgement and checkpointing force it to a
protected file, and terminal publication freezes the final union/count/hash. Only complete protected batches and sealed
files carry the scoped content authority described here. Representation-transparent reads always resolve `artifact`,
never `Redirect.source`.

Redirect classification is permitted only for a real shell AST whose simple literal redirection semantics are proven and
only when the supervisor controls the actual writable descriptor and applies the same exact combined-quota boundary as
ordinary captured pipes. Polling, tailing, or reopening the destination path after execution is forbidden: it cannot
prove byte admission or defeat replacement/truncation races. If interposition is unavailable or any eligibility check
fails, cowshed runs ordinary shell semantics and `StreamInfo` describes only bytes that actually reached the captured
pipe; it makes no claim over bytes redirected away by the shell.

After terminal sealing, the publication API remains exactly `ExecRequest.stdout_copy` /
`stderr_copy: Option<OutputPublication>`, `OutputPublication { path, policy }`, and
`PublicationPolicy::{CreateNew, Replace}` (camelCase `stdoutCopy`/`stderrCopy`, `createNew`/`replace` in JSON/NAPI). The
supervisor clonefiles/reflinks or extent-copies into a temporary destination, fsyncs, and atomically renames.
Publication never changes `StreamInfo.storage`; reads and authority never resolve through it. Redirect snapshots and
publication forbid hardlinks.

Controller commitments, the audit telemetry of job and lineage events, use this exact tagged/versioned union:

```rust
enum ControllerCommitment {
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
}
```

Every event struct has `version, order, repo_id`. Admission adds `workspace_incarnation, job_id, grant_revision`;
Terminal adds
`workspace_incarnation, job_id, state, grant_revision, stdout_bytes, stdout_sha256, stderr_bytes, stderr_sha256, batch_sha256`;
Checkpoint adds `origin_incarnation, checkpoint_id, barrier_id, manifest_batch_sha256`; Fork adds
`source_incarnation, destination_incarnation`; Restore adds
`source_checkpoint, source_incarnation, destination_incarnation`.

At launch the controller supplies a dedicated write-only IPC capability that is non-inheritable before any workspace
process starts. `order` is the writing controller's own monotone sequence. The records are audit telemetry: each is
validated on its own row and written to the configured sink (sealed Arrow segments by default, `off`, or a host's own
sink), and nothing replays them for a decision. No variant contains inline bytes, protected paths, redirect sources,
summaries, or raw payload.

Protected in-volume records and artifacts are the authoritative evidence of what a workspace ran; the controller
commitments above are telemetry about it and decide nothing. The protected tier carries its own checks. Every complete
frame keeps its `batch_sha256`, so a mutated frame is caught. Each terminal job record holds its artifacts' byte counts
and SHA-256 digests, and opening the store verifies them, as it does the files a checkpoint manifest's visible jobs
name, so a missing or altered artifact is caught. A checkpoint manifest binds the record prefix it covers through
`records_sha256`. The checked counters beside the records only move forward: `records.sequence` and each incarnation's
`records.barrier.*` are published before the records they number, and `records.job-floor` holds the highest job id a
set-aside store handed out. At open, a sequence counter behind the log is advanced to it ("Durability of a job's own
records", above), and a barrier counter that trails a durable checkpoint barrier is detected as a rollback of the
counter. Nothing in the volume detects a rollback of the whole image, records and counters together: the commitments
record what happened for whoever reads the audit trail, and cowshed does not read them for a decision. Any detected
fault, a duplicate durable key, or an incarnation outside the marker lineage returns typed `Integrity`, stops
status/restore/export from presenting the record as valid, and preserves the evidence for diagnosis; cowshed never
overwrites it or chooses whichever copy appears newer. Grant authority remains in controller-owned grant files and
gateway decisions remain in controller-owned audit segments.

Each `StreamInfo.summary` is a deterministic, bounded, versioned **diagnostic summary**, inspired by RTK/ContextCrawler:
retain a small ordered context window around compiler/test/error signatures plus deterministic head/tail context,
normalize only summary text, redact configured secret/token/path patterns before emission, and apply fixed byte, line,
and match budgets. The summary object is `{version, text, truncated}`. Summaries are convenience evidence only: they
never establish denial, exit/signal, output-limit, policy/audit, or build/test success. Full-fidelity admitted bytes are
resolved through the protected artifact; quota crossing remains explicit terminal state.

The protected record stream uses exactly:

```rust
enum ProtectedRecord {
    Job(JobArtifactRecord),
    CheckpointManifest(CheckpointManifestRecord),
}
```

`barrier_id` is positive/monotonic within the origin. `records_sha256` hashes the complete protected-record prefix
before the manifest batch. Each `VisibleJobCommitment` is `{workspace_incarnation,job_id,state,stdout,stderr}`; a stream
is `{storage_kind,bytes,sha256,protected_path}` with storage kind exactly
`captured-inline|captured-file|redirect-inline|redirect-file` and path present iff file. The barrier pauses mutation,
promotes running prefixes, fsyncs files, writes the complete manifest batch, and clones while held. Recovery frames
retain `batch_sha256`; only an incomplete trailing frame may be discarded/reported.

The workspace-file stdin source opens after allocation with a read-only no-follow component walk rooted at the workspace
mount and must resolve to a regular file beneath it. EOF closes child stdin exactly once. Cancellation records
incomplete delivery without implicitly killing the job. `JobInfo.stdin` and job/trace metadata carry stdin kind, bytes
delivered, completion, and normalized relative source where applicable; inline stdin contents never enter telemetry.

Inspect with `WorkspaceHandle::list_jobs`, `cowshed exec --json` (the final `JobInfo`), or `lmao-inspect`
(13_telemetry.md). This is the only capture implementation. CLI, MCP, NAPI, and CI consume the same protected evidence
and controller commitments; no client recaptures output or derives authority from summary text.

## Grant-change propagation

When a coordinator applies an effective filesystem or `--loopback-any` `grant`/`revoke` (04_sandbox.md, 07_api.md), the
revision a command needs moves past the one its workspace's supervisor serves. The supervisor runs no workspace code, so
it needs no process per revision: each exec host and one-shot command starts under the profile compiled for the revision
the supervisor serves at that moment. So the next command's ensure finds the supervisor serving the older revision of
the same incarnation and tells it to advance; it re-reads the workspace's and the project's grants, compiles the profile
for them, and from then on:

1. starts every new exec host and command under that profile; exec hosts, keyed by grant revision, are never reused
   across revisions;
2. reports the new enforced `grant_revision` on subsequent jobs;
3. leaves running jobs under the profile they started with until they end or are killed. A coordinator needing a hard
   cut kills them.

No-op mutations and egress-only or simulator-only mutations leave the revision, and the supervisor, untouched.

**Named sessions refuse to cross revisions.** A named session is pinned to the supervisor profile it was opened under.
Once a filesystem grant/revoke advances the revision, a later `run` targeting that stale session is rejected with
`Conflict` naming the enforced and current revisions; it is never silently migrated or resumed. The caller opens a new
named session after the advance. Exec hosts need no migration rule: a host is keyed by the grant revision it activated
under, so no host of the old revision serves a command of the new one.

## Supervisor recovery

A supervisor that ends without retiring — killed, crashed — leaves its jobs' processes running and their records
admitted but never sealed. Two things put that right:

- **The group ledger.** A served supervisor keeps `<store>/run/<digest>.groups` beside its socket: each job's process
  group and the leader's immutable birth identity observed by its parent before reaping, plus the existing Darwin
  observer's retained running lives. Each life carries its pid and `p_uniqueid` under `kern.bootsessionuuid`: a reboot
  ends those lives, and a reused pid under another unique id is never signalled. The existing actor remains the sole
  ledger writer; observer membership changes wake it without a periodic sweep. Every rewrite carries original
  identities, never a fresh pid observation used to renew an old claim. The ledger names the supervisor that wrote it. A
  watcher acts only on its own lost supervisor's ledger, never one a newer supervisor has written. New writers record
  the leader's start time as `sysctl(KERN_PROC_PID)` reports it on macOS (`/proc/<pid>/stat` on Linux), which the kernel
  answers for every process of any owner, running or exited and unreaped, so a pid another user's process now holds is
  compared and found to be another process. Older protected ledgers with a recorded `proc_pid_rusage` birth remain
  readable, but the kernel reads that birth back only for the caller's own processes: one whose pid another owner now
  holds cannot be compared and stays a refusal, whatever process now holds that pid.

  Group recovery proves that the recorded leader held its id throughout the membership snapshot. Those proven members,
  and the separately retained exact lives from this boot, are signalled through kernel identities that cannot target a
  later process: the pid/version audit token on macOS, a pidfd on Linux. It sends TERM, waits the existing grace, proves
  ownership again, then sends KILL to still-owned members. A TERM that reaches none of a group's snapshot members is no
  proof the group ended -- a macOS member that execs after the snapshot runs on under a new pid version -- so every
  group the snapshot found running gets the KILL pass. No ledger operation signals a numeric pid or group after an
  ownership check. A reused live leader is left alone. A reaped or never-identified leader with surviving unrecorded
  group members is unresolved, never signalled or silently dropped. A retained life remains owned after `setsid`, leader
  reap, reparenting or exec; recovery refreshes its pid version only while the same unique id still names that life,
  registers its exit watch before signalling, and joins kernel exit before discarding its claim. A full membership
  buffer is not absence evidence; inspection and signal failures retain the ledger and report the operational error.
  KILL is followed by bounded membership verification; live survivors or inspection errors retain the ledger rather than
  claiming release. A sealed failed-wait record is not process-release proof, so its still-unaccounted-for group remains
  recorded. Failed inspection of an inherited group likewise retains that entry while recording new groups. A macOS
  audit-token signal permission denial retains the native cause and reports the expected pid/version, caller EUID, and a
  separately labeled post-failure kernel UID/flags/current version or inspection error. This cold observation never
  renews signal authority or proves exit. The error explicitly says no exit watch was held at that signal boundary; it
  never registers a late watch and presents it as prior exit evidence.

  Every replacement supervisor inherits unresolved entries with their original job, group, and lost supervisor. Each
  subsequent ledger rewrite carries them while the group remains unaccounted for, and drops only entries proven
  released. A prior ended marker does not authorize force or erase that evidence. Unresolved groups are diagnosed
  through `doctor`; they do not prevent a new supervisor from serving safe commands.

  Publication borrows the current job-group iterator and the inspected inherited entries directly into the existing JSON
  field order; it does not materialize a second group inventory. A claim's observed member lives are shared from the
  job's published process observation, not copied, and the boot session that fences them is read once per supervisor.
  The actor prepares the destination and temporary paths once. Ordinary publications without inherited groups allocate
  only their JSON output buffer; inherited recovery still owns the carried entries and inspection errors it returns.
  This does not add an active-job frontier or change which unsealed and failed-wait groups remain recorded.

  Runtime teardown uses the same native retirement, not a legacy group signal followed by unconditional claim release.
  The actor transfers all parent handles and resource claims to one retirement worker and joins for a bounded interval.
  A slow retirement keeps running with those handles retained; a refusal refreshes the same ledger with the known member
  identities before handing it to recovery. Failure to write that handoff retains the native claims and names the
  failure. Neither destructor completion nor an earlier terminal/sealed record is membership proof.

- **Sealing.** The next supervisor binds the workspace socket first, reconciles the predecessor's groups, then seals
  every admitted but unterminated record of the current incarnation as `failed` with `failure: supervisorLost` and a
  terminal commitment. It preserves the bytes already spilled to protected files, not bytes held only in lost memory.
  The record allocator and recovery use the protected records, not a ledger's completeness: power loss may have taken an
  entry. Unresolved process evidence remains in the inherited ledger independently of the job's terminal record.

**Draining a supervisor of another build.** The manager of a newly started daemon asks every supervisor of another build
— including one that names no build at all — to drain: it admits nothing more, lets its running jobs finish, and
retires, and the next command for its workspace gets a supervisor of the new build. `drain` is the one request whose
shape never changes across builds, so any later daemon can retire any earlier supervisor. A supervisor of the daemon's
own build keeps serving across a daemon restart and retires on its own once its ledger has held nothing for its idle
bound; it reads no process table to decide (02_workspaces.md, "Holders cowshed does not run").

A job a draining supervisor serves is owed to the processes that asked for or about it: the one whose connection
admitted it (its requester) and the peer of every `hold`, `wait`, `sealed` and `logRead` of it, each held by the
identity the kernel gave its connection (an audit token on macOS, a pidfd on Linux), never by a pid alone. The drain
registers an exit watch on each of them when it begins (`NOTE_EXIT`, a readable pidfd) and on each later one when it
asks; the watch is an event, never a poll of the process table (04_sandbox.md, "a probe runs inside the supervisor's
actor"). A process that had already exited when the drain looked is named as such. The drain lists its jobs before it
closes admission, so a job admitted in between is owed and waited for like a listed one; whether its requester is
recorded before or after the drain begins is decided under one lock, so it is never lost between the two, and the drain
settles only once every exec in flight on its socket has recorded its requester or been refused.

A resident (`Resident` or `InteractiveResident`, 05_gateway.md "A resident holds no token") never finishes on its own:
it runs each request its requester sends it, and nothing else reaches it. So a draining supervisor ends a running
resident once its requester has exited, with the ordinary retirement (TERM, the grace, KILL), and the job ends `killed`.
The drain report, the stderr line and the retirement's settlement name each such job, its requester's pid and how that
exit was observed. Any other job a requester leaves — a `--background` exec, a released hold — still runs to its own
end. Measured 2026-10-22: after an install, a drained supervisor of the replaced build served an embedder's feedback Nx
worker whose controller had died with the replaced gateway, and refused the next fleet's commands for as long as the
worker lived.

A draining supervisor serves the ends of the jobs it ran before it retires. Their clients are of its own build, and the
workspace's next supervisor is of another, whose protocol they cannot speak: measured 2026-10-20, an exec client of the
replaced build came back for its job's end after the job had ended, found its supervisor retired, and was refused by the
new build's daemon, so a job that succeeded reported failure. So once its last job has ended, a draining supervisor
keeps answering its clients; it admits nothing, since `drain` closed admission. Each job running when it was asked to
drain is owed until one of the processes it is owed to has read its end (a `wait`, `hold` or `sealed` answer carried it)
and read to EOF every stream it was reading through `logRead`, or until every one of them has exited: a reader that is
gone reads nothing, and a drain never waits on it. The supervisor retires once every owed job is read or owed to nobody,
or once 30 minutes pass with no read of an owed job (the idle retirement's bound), so a live reader that never reads, or
a job no process was seen asking about, cannot keep it serving. Only a read of an owed job starts that bound over: a
`hello`, a `drain`, a refused command of another build or membership traffic reads nothing. Measured 2026-10-22: a
drained supervisor whose job had ended with its client gone answered the `hello` of every command of the new build it
refused, each of which started its bound over, and it served until it was killed. Its retirement says on stderr which
jobs' ends were read, which were owed to processes that all exited, and which went unread. This holds only when the
supervisor being replaced already contains it: a supervisor of a build from before it still retires the moment its last
job ends, so the first install of a build with this rule is not covered by it, and its clients are refused as before.

A draining supervisor says what it waits for. At each step it publishes a report beside its socket (`<socket>.drain`,
naming its pid): the jobs still running, then the ends no client has read, with each resident it ended for its
requester's exit, or the step of its retirement that failed. A failed step is said on stderr as well, and the supervisor
then serves until it is killed; it never stops retiring in silence. The refusal below quotes the report of the pid that
answered the `hello`. A report another pid wrote is a killed supervisor's and is not quoted, and one that does not
decode is named as such. Measured 2026-10-21: after an install, drained supervisors of the replaced build refused
commands for up to 15 minutes, and neither the refusal nor the log said whether a job, an unread end, or a failed
retirement held them.

Until a draining supervisor retires, a command of the new build that reaches it is refused by name: `exec` and every
other verb that would run work there, because they cannot speak its protocol — except a land step (`exec --class gate`,
`rebase`, `land`, the commands a land is made of), which waits for it. An install mid-train leaves the old build's
supervisor serving whatever job ran then, and a land that failed on it would fail on a job that has nothing to do with
the land: measured 2026-10-20, a land's `rebase` met main's supervisor of the replaced build serving a 16-minute job.
The land step asks it to `drain` (already draining when this build's daemon recovered it), says on stderr which socket
and pid it waits for, and waits on the exit watch registered before that `drain`, never a poll. The wait has one ceiling
per step, an hour, which bounds only the step: past it the step refuses naming the pid, and the supervisor and its jobs
run on. Neither the wait nor its refusal ends a job. The wait includes the supervisor's read-out after its last job
(above), at most the 30-minute bound when a job's end goes unread. A lifecycle verb that must change the workspace's
substrate — `detach`, `rm`, `restore`, `resize`, `mv` — does not wait for it, since a drain lasts as long as its longest
job. A lifecycle verb stops admission with `drain` and identifies the serving process from that exact connection's
kernel peer credential, cross-checking the reply's pid. TERM and, after the existing grace, KILL go only through that
non-reusable identity: a macOS audit token or Linux peer pidfd (kernel 6.5 or newer), never a later process that reused
the pid. A host lacking that identity primitive gets a typed refusal, not a raw-pid fallback.

The owner exit watch is registered before sending `drain` or a termination signal. On macOS, registration is followed by
an actual kernel pid-version match, never a guessed numeric-pid or signal-zero probe; on Linux the watch duplicates the
already pinned peer pidfd. A watch registered after the process starts exiting may fail while its descriptors are still
open, so that failure is not death proof. Termination waits on the pre-established watch within the existing grace, then
retains the exclusive replacement listener continuously through the caller's mutation. A listener inherited by another
live process remains attached and is refused even after its original creator dies.

Once the supervisor exits, the verb reconciles its group ledger as above and unlinks the socket unless something serves
it again; unresolved group evidence is retained. The jobs' records remain unterminated until the next supervisor seals
them. `exec` does not replace another build as a side effect: its refusal names the serving pid and says its jobs end
when it exits, so `kill <pid>` is the immediate cutover; `cowshed detach` is no general answer, since main refuses it.

**Recovering at daemon start.** The manager lists every supervisor socket under `<store>/run` before the daemon's
control socket binds, and recovers them once it is bound: asks each who serves it, drains each of another build, watches
each of its own build, and ends the jobs a positively absent one left (the group ledger, above). Each recovery reaches
only its own socket and ledger, so they all run at once, each in a task of its own, with nothing bounding them but the
sockets there are; one wedged supervisor holds back its own workspace, never the host's others. Each drain is a
lifecycle span, `supervisor-recovery drain <socket>`, that reports the answering `pid=` (or `error=`) and ends
`status=ok|err`; the whole pass is `supervisor-recovery startup`, reporting `supervisors=` up front. The manager takes
ensures from the daemon's first moment. Until the daemon's startup pass has mounted every project and restored its
sessions, every ensure is refused as a `Conflict` carrying `healing` (05_gateway.md "Startup contract"), because the
supervisor would run in a workspace that pass may not have mounted yet. After it, an ensure of a workspace whose
supervisor is still being recovered is refused as a `Conflict` carrying `recovering: { supervisors }` (the count left),
because a supervisor started then would race the recovery for the socket and the ledger; every other ensure is served.
Gateway status reports both until they are over.

## Teardown ordering

`cowshed rm` / `cowshed detach` must stop the supervisor **before** unmounting or destroying the substrate, because live
children hold the mount busy and would force `-force` detaches or `zfs destroy` retries. The daemon/long-lived-child
population is large and project-specific — Nx daemon, Gradle/Kotlin daemons, watchman, Metro, `workerd`/miniflare,
Verdaccio, DynamoDB Local, MinIO, Jupyter kernel gateways, LSP servers, DAP adapters, `devenv up` service trees, and
anything a job double-forks — so the supervisor never enumerates by name; it tracks its **process group** and tears down
the whole descendant tree regardless of what those processes are. Order:

1. supervisor SIGTERMs its session/job tree, waits the grace, SIGKILLs stragglers;
2. supervisor exits; only then is its persistent socket unlinked;
3. only then does `cowshed-core` detach/destroy (02_workspaces.md, 09_substrates.md).

The supervisor tracks its descendants (process group) precisely so this is deterministic — another reason the shell
layer is not optional glue but core to correct lifecycle.

Lost-supervisor groups that cannot be safely identified do not block ordinary supervisor startup or new admission. They
do block destroying or replacing the canonical image: `rm`, checkpoint restore, and adopted-main restore reconcile the
ledger **after** stopping the supervisor while holding the exclusive socket/listener lease across the mutation. Unknown
hello, retirement, or exit outcomes are errors, not proof of absence; no group takeover runs beside a live supervisor.
The adopted-main refusal guard runs before recording any mutation intent, so a clean refusal cannot lock later safe
admission out through intent replay. Neither `--force` nor an ended marker authorizes detaching beneath an
unaccounted-for writer. Ordinary detach and resize use idle-only unmounting without a force fallback. Grant changes
retain their existing contract above: running jobs keep their original profiles; a revision change never claims every
old-authority process died.

## doctor awareness

`cowshed doctor` reports a detached workspace that still has a supervisor (`mount-supervisor`), with the detach and
attach that clear it. It also reports each unresolved lost-job group (`unresolved-job-group`) with its workspace, job,
group id, lost supervisor, reason, and retained ledger path. A readable live unresolved group is a warning, not a
fabricated healthy or dead group; an unreadable ledger is an error. The next ledger writer drops evidence only once the
recorded group is proven released. This read-only diagnosis grants no permission to signal a stale numeric pid,
force-detach a busy image, or erase a ledger.

## Tradeoffs

**Full per-command authority regeneration rejected.** Rebuilding the complete workspace profile and environment for
every command would pay the expensive setup cost and cannot hold persistent shell state. Launch-time supervisor
sandboxing pays that cost once per grant revision. A small deterministic inner profile is nevertheless mandatory for
every shell: it removes `.cowshed/job/**` write authority and applies request-specific narrowing without regenerating or
widening the outer profile. A one-shot exec uses the same trusted-parent/narrow-child split.

**Re-entering the shell per command rejected.** `direnv exec` per job evaluates the whole `.envrc` — for a devenv
project, a Nix evaluation and every entry hook — on every command, which is where a sandboxed command's seconds went.
Whether that evaluation is still valid is a question direnv already answers from its own recorded watch list; asking it
once per activation and watching the answer's inputs costs a few dozen `stat`s per command instead.

**Replaying an unverified export rejected; inheriting a verified one adopted.** Capturing `direnv export` or
`print-dev-env` output and applying it to later processes, keyed by nothing, has no truthful invalidation and skips the
entry hooks. A clone's inherited export is different on each count. Its invalidation is the content key over the
activation's own watch list plus the baseline key, recomputed at the adopting checkout (above): the inputs direnv says
the evaluation read, as bytes, so a changed input misses and a rewrite of the same bytes does not. Its entry hooks
already ran on these same bytes at the origin — installs and generated files are in the image the clone copied, and a
task that would run again finds nothing to do (on that monorepo every entry task reported cached on a fresh fork's
evaluation, and `devenv:files:cleanup` spent 10.1 s doing nothing). What does differ per checkout is read at the
checkout: the tail files, sourced through direnv. A record whose checks fail is never applied.

**Making devenv's own evaluation cache answer a clone rejected.** devenv keys its cache on the absolute root, dotfile,
`TMPDIR`, runtime directory and git root, and records `HOME` among its inputs, so a clone's copy of its origin's cache
never hits; and the shell's root-dependent derivations (there, the files-cleanup and service scripts embed the root, and
seven derivations rebuild at a new path) would hand the clone the origin's paths even on a hit. Root-free derivations
plus a root-free key would need a devenv fork carried across releases, for a result the inherited export already gives
without evaluating at all.

**A live bash as the warm shell rejected.** A job owns anonymous pipes whose other ends only the supervisor holds, a
process group the supervisor signals, and an exact wait status. A long-lived bash can accept none of these per command:
it cannot receive descriptors, named FIFOs are reachable by sibling jobs, and its `wait` folds a signal death into
`128+N` and drops the core-dump flag. The exec host does exactly what bash cannot and nothing more; shell text belongs
to a shell interpreter, not to the process that keeps the activation.

**Scripts interpreted inside the warm host rejected.** Running a script in the host's own brush interpreter would save a
fork but break the job contract: with job control off every external command joins the interpreter's own process group
(so a job kill would kill the host), with it on each pipeline gets a group of its own (so `a; b &` escapes a kill), a
command's signal death is folded into exit `128+N` with no way to report the signal, and `umask`, `ulimit`, `trap` and
`exec` act on the host process that later commands share. A fork of the warm host costs about a millisecond and inherits
the activated environment, so a per-script child keeps every property of an argv job without changing the interpreter.

**A cross-workspace shared supervisor rejected.** One supervisor serving many workspaces would straddle sandbox
boundaries and couple unrelated workspaces' lifecycles. One supervisor per workspace keeps the boundary and teardown
clean while its persistent socket still supports multiple concurrent clients.

**Text-log capture rejected.** Scraping merged terminal text is what forces agents into brittle `grep` parsing.
Structured exec records with separated raw streams and the grant/env context make a failed run reproducible instead of
merely readable (10_ci.md). Records are Arrow, not a line log, for the same reason all cowshed telemetry is
(13_telemetry.md): one substrate, queryable and joinable against the store-side spans — "diff the job histories of these
two forks" is a query, not a diff of text files.

## Shell text, redirection, and sealed output publication

Shell text is interpreted by the selected shell. Cowshed does not use regexes or output sniffing to reinterpret `>`,
`2>`, append, pipelines, expansions, or other syntax. The optional real-AST `Redirect` classification above is an
evidence-preserving capture path, not the former hardlink optimization: it is available only with a
supervisor-controlled writable descriptor and exact quota accounting, and snapshots independently after terminal state.
Otherwise ordinary shell semantics apply and bytes redirected away from captured pipes are not claimed by the job
handle.

Callers that want a workspace-visible copy independent of shell syntax request `stdout_copy` and/or `stderr_copy` in
`ExecRequest` (07_api.md). Only after terminal state and canonical artifact sealing does the supervisor materialize a
temporary destination from that artifact, fsync it, and atomically rename it according to create/replace policy. APFS
uses `clonefile`, ZFS uses block cloning when available, and other substrates use an extent copy. Hardlinks are
forbidden in every case.

Redirect sources and publication paths are normalized workspace-relative regular-file paths opened with no-follow
component walks. Absolute paths, traversal, symlink ancestors, directories, devices, sockets, and replacement races fail
closed. Publication disposition (`clone`, `reflink`, or `copy`) and failure are structured metadata. Publication failure
does not change the established process/quota state or `StreamInfo`; it is a separate typed operational outcome. The
destination is never promised while the command runs, never used for job reads, and never authoritative.

Once the protected terminal record is sealed, a refused output copy cannot leave the live job `running`: terminal state,
exit, artifacts, commitment, and session removal advance before the original copy error reaches an early or late
`wait`/`kill` caller; normal process-group bookkeeping still concludes. A refused terminal commitment likewise fails its
callers without replacing sealed state, stream counts/hashes, or quota metadata with the admission's empty values. The
completed in-memory outcome is either a sealed artifact with separate commitment/publication errors, or a genuine
sealing failure; it cannot claim both sealed and unsealed.

A true sealing failure answers `info`, `list`, `wait`, and `kill` with that error rather than presenting a fake empty
success. The captured bytes remain readable through `logRead`; `sealed` reports no terminal record. Every caller is
answered without hanging, and retirement may proceed after the child has ended. The next supervisor uses the existing
lost-job recovery for records that genuinely remained unterminated, preserving the distinction between a child's exit, a
protected terminal record, and its controller commitment.

## Server-owned CPU admission and nice placement

The supervisor's exec admission records Queued durably, acquires the gateway jobserver off the actor path, and spawns
only under the actual grant (05_gateway.md, Host CPU budget). Status/cancel remain responsive while queued. **Every exec
answers at admission**, foreground, `--background` and API caller alike (07_api.md, "CLI and API exec parity"): the
answer is the job id while the job is `queued` with class and scope set and `cpuTokens`/`nice` absent, so its caller
reads the grant from the job's status (`running`, `cpuTokens`, `nice`) and cancels it with kill while it waits. A
foreground CLI exec therefore holds its job (05_gateway.md, "A job lives with its requester") from admission, not from
spawn, so a requester killed while its job is queued withdraws it. A refusal after admission — the gateway's, or the
class nice that cannot be placed — fails the job: `failed`, failure `cpuAdmission` or `priority`, exit the refusal's
wrapper code, and its cause on the job's stderr; the exec call itself succeeded. Measured 2026-10-19 before this: a
`cowshed exec --background` on a loaded host answered at its grant, 25 s later, so a queued job had no id to read or
cancel. Killing a queued job withdraws its request and seals Killed without a command. Detaching (`--background`,
`--timeout`, `JobHandle::detach`) promotes the captured prefixes to their protected files before it answers. The grant
stays held until the owned job concludes. Caller MAKEFLAGS/CARGO_MAKEFLAGS/MFLAGS and BOUNDED_EXEC_CPU_* never
substitute for that allocation; actual pool/count override caller env after filtering.

Gate is trusted coordinator/operator only; workers default Background / Workspace, may ask Interactive, Resident or
InteractiveResident, and cannot request Gate or HostGate. The owned command's nice is its class's row of 05_gateway.md
"Bands" — Interactive and InteractiveResident 0 / Gate +5 / Background and Resident +10 — not the reusable host's or
supervisor's. An interactive job asks the gateway as background; its class reaches the ledger only as that. On macOS
placement/read-back occurs while the existing observation fence holds the child. On Linux the child reports placement
before exec or group reporting on a private CLOEXEC pipe: a 20-byte stack record of outcome, actual pid, requested,
observed and errno. No allocation occurs in the forked report path. REPLY_NICE_REFUSED (tag11) carries the typed failure
in the warm-host exchange; absence is explicit, not a guessed nice. A script that exits before reporting its group
answers EXITED, never STARTED. Failed placement is priority, and observed nice stays absent until placement succeeds.

HostGate is an explicitly authorized argv host one-shot of any class on this same server, without ExecutedChild or
activation; it preserves the real-APFS host boundary and refuses scripts, read-only mode and sessions, never custom
environment names. Its record-only public environment projection and full authored-env commitment are defined in
07_api.md, Task server scheduling and trusted host gates; execution and canonical activation retain all ordinary project
variables. No ordinary workspace sandbox is widened. The single sandbox builder allows only connection and file metadata
in the gateway-derived workspace CPU-pool scope, not directory data, file writes, socket binding, the global control
socket or another workspace's pool.

Job records use layout14: layout10's cpu_class, cpu_want, cpu_wait_ms, cpu_tokens, cpu_nice and cpu_scope columns remain
an exact prefix, followed by layout11's admission_env_sha256 for the complete non-withheld authored environment,
layout12's admission_want, the want a keyed request named (07_api.md, "Keyed admission and restart attachment"),
layout13's nullable retirement_containment and retirement_outlived wire-JSON columns, and layout14's nullable boolean
retirement_outlived_incomplete. Those three are all null without a receipt or all present with one; any partial receipt
is damaged. An empty list with a true marker remains an honest receipt of proven-empty membership with incomplete
metadata. Layout13 and earlier stores are set aside whole, never decoded with an invented false marker. cpu_want is the
want its admission resolved. cpu_wait_ms is retired since gate admission lost its bound (05_gateway.md, "No admission
deadline, for any class"): layouts grow only by trailing columns, so it stays, written null; an earlier record's
positive bound still reads, a zero one is damage, and no bound is read into the record. JobAdmission.env is the public
plaintext projection only for every scope; keyed Env comparison uses the digest, including after restart. Records retain
typed cpuAdmission/priority failures. Earlier histories are retained untouched and set aside. Per 07_api.md Keyed
admission, existing incarnations must be renewed before new keyed exec: unverifiable historical keys are refused as
unprovable, never migrated into an attestation they did not record. Unkeyed operator CLI exec remains available.
Controller-only discovery runs locate-project/metadata --no-deps --offline under its ordinary child sandbox outside job
admission, retaining the explicit noncompiling exemption rather than claiming a grant or recorded nice.
