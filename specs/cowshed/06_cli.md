# CLI Contract

The `cowshed` binary is self-driving: an agent that has never seen it can operate it from its own output. Three rules
make that possible and they are absolute:

1. **stdout is machine-readable only.** A bare value (usually a path) or, with `--json`, one JSON envelope. Nothing else
   — ever. `cowshed path raven | xargs ls` and `cowshed ls | grep …` must never see prose.
2. **Everything for humans and agents goes to stderr**: progress, warnings, and — after every command — actionable
   hints. Guidance can never contaminate pipes or grep. Guidance lines are prefixed `cowshed:`; trailing hint lines are
   prefixed `next:`.
3. **A command that may trigger an authorization prompt says so first.** Any operation that can escalate — volume
   provisioning, a remount macOS classifies as privileged, plist installation — emits a `cowshed:` line naming the
   action and the prompt before the dialog appears
   (`cowshed: one administrator authorization will remount cowshed.store at /private/cowshed/store`). A silent
   SecurityAgent dialog behind an unrelated read-only verb is a contract violation, not a platform quirk to tolerate.
   Where the platform permits (fstab-pinned mounts), cowshed prefers designs in which no prompt can occur at all
   (01a_apfs.md).
4. **Diagnosis is canonical and self-contained.** The same host state yields the same verdict from any directory,
   regardless of unrelated details (a stray file in a mountpoint dir must change the wording, not the verdict). Every
   finding names the observed evidence (which volume, mounted where, expected where, which stub files) and a `next:`
   command that exists in the parser. Hinted verbs are contract-tested against the parser so guidance can never
   reference a command that does not exist.

## Launcher

The npm package's `cowshed` bin is `bin/cowshed`, a POSIX shell script that execs the native binary for the host. A
published package, and a checkout's launcher reached through a link from outside that checkout (another repository's
`node_modules/.bin/cowshed`, which a developer link points at the checkout's package), are consumers: when the host has
an installed copy (05_gateway.md "The installed cowshed"), every verb except `setup` and `gateway` runs it. Another
build cannot read the install's daemon and records. `setup` and `gateway` always run the invoking package's own build,
since setup and gateway start install that build and gateway status compares it with the daemon. Global flags and the
value of `--project` are not verbs; the original argv is passed unchanged.

Without an install, or for `setup` and `gateway`, the package's source authority picks its own build
(packages/nx-plugin/README.md, "Cargo profiles"). A checkout holds the tracked entry of the CLI's own crate,
`crates/cowshed-cli/src/main.rs`, which a published package never ships, and runs its development build,
`.cache/bin/<platform>/cowshed`; a published package runs its release build, `dist/bin/<platform>/cowshed`, restoring an
execute bit a publish dropped. A launcher invoked from within its own checkout always keeps that development authority,
even with an install present. Neither source authority substitutes the other build or a cargo `target/release/cowshed`:
a checkout without its selected development build exits 5 naming the path and `nx run cowshed:cli-native`, a package
without its selected binary exits 5 naming its path, and a host the package ships no binary for exits 5 naming the host.
It is a shell script rather than Node because it runs before every command, and Node's own start (~30 ms) costs more
than the fastest verbs take in total; `exec` hands the binary the command's signals, exit status and standard streams
unchanged. The library's `runCli`, `openHost` and the typed launchers (`spawnLock`, `spawnHold`, `spawnSubtreeRoot`,
`spawnLandTurn`; 07_api.md, "Launchers") run the CLI through the same script, so one rule picks the binary.

The launcher is how a build that is not installed yet is run, and not the `cowshed` on `PATH`. After `setup`, the
`cowshed` on `PATH` is the installed copy (05_gateway.md "The installed cowshed"): setup repoints the first `cowshed` on
`PATH` at it when that entry is a symbolic link, which is what `bun link` of the package or `bun add --global` leaves. A
`PATH` entry left on the launcher would run whatever a checkout last built, under a daemon still running the build setup
installed. `setup` installs only a release build and refuses a debug one as the supervised binary, and a checkout's
launcher runs its development build, so a checkout's release build installs itself by its own path,
`dist/bin/<platform>/cowshed setup`.

The development build is the default configuration of the platform's `cli-<arch>-<os>` target (`cli-native` is the
host's) and the shipped one its `production` configuration, each writing its own tree with the `.build-inputs.json`
receipt of what it read beside the binary. The launcher only tests that the file exists and carries no per-exec
currentness check: building is the target's job, and the builds of a developer-linked checkout are judged once, at shell
entry, by the receipt beside each binary (specs/cli/01_workspace_reconciliation.md, "Declared developer link pins").

The shipped trees are judged where they are assembled. The package's `release-check` target
(`scripts/verify-packaged-bin-bits.ts`) fails on a missing or empty `dist/bin`, on any binary in it without its execute
bit, on this host's N-API addon missing at `dist/native/<directory>/cowshed.<directory>.node`, and on any family's addon
there that is empty or unreadable (one directory per `NATIVE_TARGETS` row in `src/platform.ts`, the only tree an
installed package loads; the publishing job has every family's, the land's shipping seam this host's). It never reads
`.cache/bin` or `.cache/native-dev`. `smoo release pack` runs it in the publishing job, after every platform's prebuilt
outputs are applied and before the tarball is packed, and refuses a gate that declares dependencies or is not cached on
a runtime digest of the packed bytes (`verify-packaged-bin-bits.ts --digest`: this host's family and every file of
`dist/bin` and `dist/native` with its mode and bytes), so the gate judges the bytes that get packed, never a rebuild,
and a replay stands for exactly those bytes.

### Linux artifacts

Linux artifacts reference no build-host path. The CLI is a static musl executable (no PT_INTERP, no DT_NEEDED). N-API
addons link glibc 2.17 symbol versions and carry no RUNPATH. Both are built by devenv's zig through cargo-zigbuild, with
the same command on every host.

So the CLI runs on any Linux userland of its architecture — glibc or musl, FHS or NixOS without nix-ld — and lives in
`dist/bin/linux-<arch>/`, a directory without a libc suffix. The addon is glibc because Node is: it lives in
`dist/native/linux-<arch>-gnu/cowshed.linux-<arch>-gnu.node`, loads on glibc ≥ 2.17 (below Node ≥ 18's own 2.28), needs
no `libgcc_s`, and is not built for musl Node. macOS artifacts are built by `@napi-rs/cli` with the host's own
toolchain, since they need Apple's SDK.

## Onboarding and repair

Two verbs own the host story, both runnable from any directory:

- **`doctor`** is the universal diagnostician: version and install source, per-volume state (present/absent, current
  versus canonical mountpoint, marker validity), service status, workspace inventory summary, project checks when an
  adopted checkout resolves. Pending host setup actions are findings, not a gate on inventory or project checks; each
  reader validates the storage it needs and reports a failure instead of silently skipping those checks. It never
  mutates: its project open finishes no unfinished lifecycle operation, interrupted publication, retired-image
  reclamation, identity change or binding heal, and reports each as a finding for the next opening command to finish.
  `doctor --repair` opens the way every other verb does. While the retired caches volume still exists it reports the
  `caches-volume` warning, whose hint is `cowshed setup --retire-caches-volume` once only the volume's marker remains
  and `cowshed setup, then cowshed setup --retire-caches-volume` before that.

  Doctor says where it is as it goes, on stderr, in the lifecycle-span form every slow verb uses: every probe is a
  `doctor <probe>` step whose `cowshed: doctor <probe> start` line always prints, before the probe asks anything, and
  whose `done elapsed=<duration> status=ok|err` line prints only once the probe took 2 s (`SLOW_PROBE`). On a loaded
  host every ordinary probe answered in under a second while the ones a slow doctor's time went to — the gateway's
  answer, the store's metadata walk — took 2 s and more, so below that a `done` line would only say, hundreds of times
  per project, that a probe was fast. A probe that waits for another party names it on the line after its `start`:
  `cowshed: doctor <probe> waits-on=<party>` (launchd, securityd, the gateway at its socket, the commitment publisher).
  The host probes are `host-storage`, `caches-volume`, `gateway-status`, `vnode-table`, `disk-image-helpers`,
  `attach-budget`, `inventory` and `retired-mount-layout`, then `first-launch` while the gateway serves; the project's
  are `project-root`, `artifact-repair` (`--repair` only), `project-open`, `git-identity`, `project` and
  `project-shutdown`, and `project` runs one step per project check, per workspace as `<workspace>/<check>`
  (`main/marker`), ending in one summary line,
  `cowshed: doctor workspace-checks done elapsed=<d> workspaces=<n> slowest=<workspace> <d>`. A doctor that does not
  finish therefore names, in its last `start` line, the probe it waits in, and in that probe's `waits-on=` whose answer
  is missing. `status=err` means the probe's own query failed; the failure is also its finding. A party that is absent
  or refuses is a typed finding (`gateway-down`, `gateway-status`, `project-checks-skipped`, …). A daemon that took
  doctor's request and stays silent is reported as `gateway-unanswered` (error):
  `<peer> accepted doctor's request at <socket> and gave no answer in <bound>`. The gateway heartbeats every answer that
  is not ready yet (05_gateway.md, "Deadlines"), so a live, saturated gateway that answers `status` late (4.6–4.9 s
  measured on a loaded host) is never reported; one that says nothing, not even a heartbeat, for the 15 s silence bound
  is. The `gateway-unanswered` hint looks before it restarts, since a restart cuts off every gate the gateway serves. No
  probe reports an answer it did not get. A caller that asked for its steps (07_api.md) hears every step start and end,
  however short; `--json` changes stdout only.

  `first-launch` reads whether workspace jobs pay macOS's first-launch scan (05_gateway.md "The gateway host"). The TCC
  database is unreadable to the user, so it reads behavior: doctor mints a fresh copy of its own executable, signed ad
  hoc under an unused identifier, and launches it twice in its own process, the host shell's; then the gateway
  (`first-launch` control op) does the same in the daemon's process, whose responsible process every job has. The scan
  is what a first launch costs beyond its second. A charged launch of a program cowshed's size waits hundreds of
  milliseconds to seconds (2078 ms and 5258.7 ms measured under an unexempt gateway), while an exempt gateway on a
  loaded host measured 23.8 ms against the shell's 8.7 ms. So the gateway is charged only when its excess passes both
  100 ms (`CHARGED_EXCESS_MICROS`) and what the host shell, whose terminal holds the exemption, pays at this load: its
  own excess plus one host-shell first launch, which keeps a host so loaded that the shell itself is slow from reading
  as charged. The finding names all four times: `first-launch-exempt` (info) when jobs launch at host-shell speed;
  `first-launch-scan` (warning) when they wait, naming the gateway host's path, the one step that ends it (System
  Settings → Privacy & Security → Developer Tools → +) and the restart that step needs, since an entry applies only to a
  gateway host started after it was added (`launchctl kickstart -k gui/<uid>/dev.cowshed.gateway`, or
  `cowshed gateway start`, which refuses while a land or gate runs), with the hint
  `open 'x-apple.systempreferences:com.apple.preference.security?Privacy_DevTools'; then launchctl kickstart -k gui/<uid>/dev.cowshed.gateway`;
  `first-launch-unmeasured` (warning) when either side measured nothing, or when doctor itself runs under the gateway
  host (inside a workspace job, as its responsible process says) and so has no host-shell launch to compare; its hint is
  `cowshed setup` for a gateway of a build without the op, and running doctor from a terminal for the latter. A scan is
  a cost, not a broken host, so no `first-launch-*` finding makes doctor unhealthy.

  Doctor also judges the host's mbx, for every Cargo project, on a host that has installed one (15_capabilities.md;
  05_gateway.md, "Pool selection and revision" owns the rules, doctor only reports them). A host link that exists and
  does not resolve is a broken installation that doctor and `cowshed setup` report, naming the link and the cause, with
  the remedy `cowshed setup`; no job is refused for it, because a project then gets no mbx and runs on the non-mbx
  mechanism. An installed mbx that cannot be executed is `mbx-host-missing`, and one with no provisioned build-cache
  volume for its store is `mbx-build-cache-missing`, each with the remedy `cowshed setup`; job admission refuses with
  the same findings, and admission never selects another pool or falls back. A host with no installed mbx, or a link
  whose shim directory `cowshed setup` has not yet provisioned, is no finding: its projects get no mbx and cowshed's own
  ledger runs (05_gateway.md, "Pool selection and revision"). A project's own mbx pin is no finding either: the
  installed mbx is the one its Cargo runs, and nothing compares the two. An installed mbx whose host-global
  `scheduler.memory` is `"none"` is `mbx-memory-unscheduled`: doctor reports it and mbx-mode job admission refuses with
  the remedy to set a memory budget; there is no CPU-only fallback. Each is derived at every detection from the host
  link, never stored.

- **`setup`** is idempotent host repair: provision absent volumes, repair detached or mis-mounted ones,
  FileVault-encrypt unencrypted ones in place and store passphrases in System.keychain, validate markers precisely, pin
  `/etc/fstab`, and install the `dev.cowshed.storage` system LaunchDaemon that unlocks and mounts before login
  (`01a_apfs.md`, Dedicated store volume). The build-cache volume is provisioned by this same transaction — created or
  adopted by UUID at its mountpoint, encrypted, pinned and boot-mounted by the same daemon, script and keychain scheme,
  and never deleted (`01a_apfs.md`, Dedicated build-cache volume). Privileged commands report their actual child exit
  status. A newly persisted canonical passphrase must read back identically before encryption; the UUID-keyed APFS
  discovery credential is prepared and verified in the same authorization session, including repair for retained
  encrypted volumes. The boot script checks credential retrieval before unlocking, and its root daemon retains the
  refusing step/source line in `/var/log/cowshed-storage.log`. It announces an authorization prompt before raising one,
  then performs everything that can require elevation inside that single session. Once storage repair succeeds, it
  installs the invoking build as the installed cowshed — the gateway's binary and the `cowshed` on `PATH` alike, with
  the gateway host signed by the user's cowshed code-signing identity, made in cowshed's own keychain on first need
  without authorization or a dialog — restarting a gateway that runs other bytes, and points the first `cowshed` on
  `PATH` at it when that entry is a symbolic link (05_gateway.md "The installed cowshed"). Run from the installed copy
  it installs nothing and says so; another build is installed by running that build's own `setup`. It holds the install
  slot for its whole run (05_gateway.md "The install slot") and refuses to replace the installed build with one that
  does not contain it — one whose commit is on no adopted `main`, or which lacks an installed commit by both ancestry
  and patch identity — or one that records no commit, naming the binary, both builds and each lost commit; it repairs
  everything else and exits non-zero; `setup --downgrade` installs it anyway. It then refreshes every adopted main's
  build state through the same runtime path jobs use (16_build_volumes.md). Migration is rebuild-only: it announces the
  discard of contributed incremental directories before acting, protects tracked source files, and links an empty build
  volume for the next build to repopulate. No build state is copied. A project refusal is reported on stderr without
  hiding later projects; the command exits with the first typed failure and the number of failed projects, never a
  success JSON envelope first. Uninstall and a failed storage repair never run migration. A fully migrated healthy host
  changes nothing and says so. The non-destructive storage promise applies to the host store volume, layer-3 caches and
  source data, not to rebuildable incremental state. On a host that still has the retired caches volume, setup moves
  what the volume holds to the host HOME without authorization, and `setup --retire-caches-volume` deletes the emptied
  volume, its `/etc/fstab` pin and its mount-service entry inside that same single session, refusing while anything but
  the marker is left (03_caches.md, "Wiring").

The verbs that change workspace or lifecycle state or run a job through a workspace supervisor — `adopt`, `fork`, `mv`,
`path`, `exec`, `grant`, `rm`, `forget`, `attach`, `detach`, `resize`, `defrag`, `cache clear`, `rekey`, `gc`, `push`,
`rebase`, `land` — first ask the host daemon's supervisor manager whether it runs this CLI's build (`sameBuild`,
11_shell.md), before the project is opened, so not even recovery mutates anything. A daemon of another build is refused
up front with "the cowshed daemon is build X; this cowshed is build Y" and the hint "run `cowshed setup` from the
cowshed you mean to use: it installs that build as both the daemon and the `cowshed` on PATH"; no daemon answering is
not a disagreement. `ls`, `doctor`, `build-state`, `mount`, `setup`, `gateway`, `controller`, `credential`, `identity`,
`skill`, `--version` and `--help` are not asked: `doctor` diagnoses the disagreement, and `setup` and `gateway` end it.
`gateway status` and `doctor` (`gateway-stale-binary`) name `cowshed setup` as the remedy for a daemon running other
bytes than the CLI.

For exec in host scope, project discovery first establishes the caller's store authority through the controller Git
profile it already writes. A sandboxed CLI therefore gets the store's sandbox-denied refusal (exit 6, naming that path)
before an install-slot wait, a manager build check or a resident connection can replace it with a control-plane failure.
The discovered root is carried into coordinator dispatch and is not discovered twice. The build check still precedes
opening the project or replaying lifecycle work; other commands keep their existing preflight order.

The stranded-user journey this contract exists for: after a reboot with locked or unmounted volumes, `cowshed doctor`
explains the exact divergence (volume present but locked, or at macOS's default `/Volumes/<name>` instead of its
canonical path, stubs listed by name, service status) and `cowshed setup` fixes it in one step.

```
$ cowshed fork raven
<home>/.cowshed/mnt/acme/widget/raven             ← stdout (bare mount path)
cowshed: created workspace raven from main @ 8f31c2d (612ms)     ← stderr
next: cd "$(cowshed path raven)"                                 ← stderr
next: cowshed exec raven -- bun install                          ← stderr
next: cowshed grant raven --egress registry.npmjs.org            ← stderr (if gateway saw no grants)
```

## JSON envelope (frozen)

`--json` on any command emits **exactly this discriminated envelope**, one line on stdout:

```json
{"ok":true,"result":{"workspace":"raven","mount":"…","baseCommit":"8f31c2d"}}
{"ok":false,"error":{"code":"conflict","message":"workspace raven already exists","hint":"cowshed ls"}}
```

`ok` is the discriminant; success carries `result`, failure carries `error` with `code` (taxonomy name), `message`, and
`hint`. No other top-level keys — `cmd`, `detail`, and a numeric `code` are **not** part of the envelope. This is the
single frozen shape; contract goldens (08_testing.md) enforce it, and any package doc showing a different arrangement is
stale, not authoritative. Long operations additionally emit NDJSON progress events on stderr with `--json`
(`{"event":"attach","ms":233}`) — NDJSON here is a **wire encoding on a pipe to a live consumer**, the same role it
plays for `--ndjson` export flags; cowshed never writes NDJSON to disk (telemetry storage is Arrow, 13_telemetry.md).
The CLI output module serializes `cowshed_core::api::JsonEnvelope<T>` and `CowshedError` directly. It has no local JSON
value tree, error record, or envelope encoder. The sealed `ResultBody` bound rejects `()` and anonymous adapter maps;
empty success is exactly the named `EmptyResult {}` body, never `null`.

### JSON result bodies (frozen)

Every command uses a named `cowshed-core` DTO as its `result`; adapters never assemble anonymous maps. `adopt`, `fork`,
`attach`, and `path` return `MountResult { workspace, mount, baseCommit? }`; lifecycle commands fill `baseCommit`, while
query/attachment paths may omit it when no marker snapshot was requested. `detach`, `rm`, job detach/kill, and
successful policy mutations with no additional observation return the literal empty object `{}` through `EmptyResult`
(never `null`, `true`, or a message string). `doctor` returns `DoctorReport { healthy, findings }`; each `Finding` has
`code`, `severity`, `message`, `hint`, and optional `path`. `ls` returns `WorkspaceInfo[]` and `ls --all` returns
`ProjectWorkspaces[]` (07_api.md); `attach` returns `MountResult` for one workspace and `WorkspaceInfo[]` for several;
`gc` returns `GcReport`; `identity add` returns `IdentityReport { repoId, added, identity, identities }`; `forget`
returns `ProjectRetirementReport { repoId, archive, images }`. Exec and job commands return the frozen job DTOs in
07_api.md. Commands whose normal stdout is a scalar use a named one-field body ( `RevisionResult { oid }`,
`SlotResult { slot }`) so JSON never changes shape when another field is added.

The controller commitment transport never carries output payload or artifact paths. Ordinary `JobInfo` JSON describes
protected content through the frozen `StreamInfo` union and may include a bounded inline artifact as the exact
`BinaryData` wire union `{encoding:"utf8",data:"…"} | {encoding:"base64",data:"…"}`. The serializer selects `utf8`
exactly when the bytes are valid UTF-8; both forms are bounded by decoded byte length. Detached workspaces still omit
unavailable marker fields rather than emitting `null` or attaching as a side effect.

### Exec and job JSON (frozen)

Child command arguments never pass through `String`. The CLI collects each post-`--` value as an `OsString` and moves it
into the canonical `CommandArg`; the controller request and `JobInfo.argv` serialize every element as exactly
`{encoding:"utf8",data}` when its Unix bytes are valid UTF-8 or `{encoding:"base64",data}` otherwise. Base64 is strict
and canonical; unknown fields/encodings, malformed data, NUL, an argument above 128 KiB, total decoded argv above 1 MiB,
an empty argv or `argv[0]`, and non-representable platform bytes fail as usage errors before RPC, job-id/artifact
effects, or spawn. The supervisor consumes each `CommandArg` into `OsString`, and protected Arrow records argv as
`List<Binary>`; neither the CLI nor runtime uses lossy text conversion or a second wire shape.

Every `cowshed exec` submission, foreground or background, atomically allocates a positive, workspace-local,
monotonically increasing numeric `JobId`; ids are never reused. In normal mode captured child stdout remains CLI stdout,
so the control-plane job id and state are reported on stderr (`--background` prints the bare numeric id on stdout
instead). With `--json`, stdout contains only the final JSON envelope: `result` carries the numeric `jobId`, `JobInfo`
lifecycle/result metadata, hashes, bounded summaries, and—only when the canonical artifact is inline—the bounded
`BinaryData` `utf8|base64` tagged union. Live progress on stderr remains control-only NDJSON. Unbounded child output
never enters JSON, and controller commitments never receive either inline encoding.

The CLI writes its stdout and stderr with a blocking write's semantics whatever flags the inherited descriptors carry: a
foreground job's relayed bytes and cowshed's own `cowshed:` diagnostic lines alike. `O_NONBLOCK` belongs to the open
file description, which a parent that drives the same pipe from an event loop, such as Node, may set; a write the kernel
then refuses with `EAGAIN` waits in `poll(2)` for `POLLOUT` on that same descriptor, with no deadline, and is made
again. Cowshed never clears the flag: the description is the parent's too. Only a real failure ends the relay with an
error: `EPIPE` once the reader is gone, `EIO`, or a closed descriptor. Observed before this rule: a foreground exec
relaying a 22-minute gate exited 5 with `could not write child output: Resource temporarily unavailable (os error 35)`
while its consumer was still reading.

`JobInfo` also carries structured stdin metadata (`kind`, delivered `bytes`, `complete`, and an optional normalized
workspace-relative `path`) and the explicit `output-limit` terminal state. The configurable combined stdout+stderr
capture quota defaults to 1 GiB. Crossing counts protected plus in-flight bytes, then TERM/grace/KILLs the process
group, drains both pipes without retaining bytes beyond the exact boundary, and publishes `output-limit`; cowshed never
silently truncates output while a job continues.

Each stream is `StreamInfo { storage, bytes, sha256, summary }`. `storage` is `{kind:"captured",artifact}` or
`{kind:"redirect",source,artifact}`; the protected artifact is `{kind:"inline",data}` or `{kind:"file",path}`. A small
terminal stream is inline Arrow Binary and has no path. A protected `.cowshed/job/<id>/out|err` file appears only after
lazy promotion. `Redirect.source` is an AST-proven live shell destination and never authority;
representation-transparent logs always read its independent protected `artifact`. `summary` remains deterministic,
bounded, versioned, redacted diagnostic text and drives no denial, exit, quota, policy, or build-success decision.

Protected records and content artifacts are the authoritative evidence of a job; controller commitments are audit
telemetry and decide nothing (11_shell.md, `ControllerCommitment`). A protected record or artifact that fails its own
digest, count, counter or lineage check is typed `integrity`; cowshed never silently overwrites it.

## Exit codes (stable API)

Two mappings. **cowshed's own outcome** uses 0–7; a child process run under `cowshed exec` has its exit code passed
through unchanged. Wrapper failures that occur while trying to exec use **100–106**. Because a real child may also exit
100–106, shell status alone is intentionally ambiguous; structured job/error output identifies whether cowshed failed
before exec or the child itself returned that status.

| Code | Meaning                       | Example                                                                     |
| ---- | ----------------------------- | --------------------------------------------------------------------------- |
| 0    | success                       |                                                                             |
| 1    | internal error (bug — report) | panic, unexpected substrate failure                                         |
| 2    | usage                         | unknown flag, bad workspace name                                            |
| 3    | not found                     | no such workspace/project                                                   |
| 4    | conflict / busy               | name taken, mount busy, CAS moved                                           |
| 5    | environment missing           | not adopted, gateway down when required, no supported substrate             |
| 6    | sandbox-denied                | denial proven pre-spawn, by kernel evidence, or by gateway                  |
| 7    | integrity                     | missing/altered committed artifact, invalid complete batch, digest mismatch |

**`cowshed exec` exit semantics (frozen):**

- The child's exit code passes through exactly; a child that exits 6 or 7 reports its own status, not a cowshed wrapper
  denial/integrity result.
- Wrapper failures are **100** internal, **101** usage, **102** not-found, **103** conflict, **104** env-missing,
  **105** sandbox-denied-pre-spawn, and **106** integrity.
- Exit/typed **6** is emitted only on authoritative denial evidence (04_sandbox.md), never output string scanning.
- Exit/typed **7** covers a missing or altered protected artifact, invalid complete Arrow batches, a count/hash/batch
  digest mismatch, and an incarnation outside the marker lineage. Discarding an incomplete trailing batch is successful
  recovery with a structured recovery report, not exit 7.
- NAPI/MCP surface the same taxonomy as structured errors/outcomes (07_api.md, 12_mcp.md).

## Commands

Global flags: `--json`, `--project <git-root>` (default: cwd's repo), `-q`/`--quiet` (aliases; suppress stderr guidance,
never errors).

| Command                                       | stdout                                                | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cowshed adopt [path]`                        | mount path                                            | Convert checkout → main workspace (02). `--capacity <size>`. Default is idempotent; `--re-adopt` explicitly replaces an adopted main through the verified, retained-source transaction (02 §Re-adoption), for guest-space reclamation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed ls`                                  | one name per line (`--json`: full records)            | Includes `main`, mount state, branch, base commit, age. Detached rows degrade — see below.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `cowshed path <ws>`                           | mount path                                            | Exit 3 if unknown; attaches if detached (unless `--no-attach`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed exec <ws> -- <cmd…>`                 | child's stdout                                        | Sandboxed byte-exact argv exec (04). Post-`--` arguments remain `OsString`; each is capped at 128 KiB and their decoded total at 1 MiB. Every submission creates a numeric `JobId`; structured binary stdin is separate from shell text. `--stdout-copy <rel>` / `--stderr-copy <rel>` request independent post-terminal publication; default policy is `CreateNew`, and `--replace-output` changes all requested copies to `Replace`. `--ro`, `--cwd <rel>`. Child exit passes through unchanged; wrapper errors use 100–106.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed shell <ws>`                          | — (interactive)                                       | Sandboxed login shell inside the mount.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `cowshed repo mirror <url>`                   | mirror path                                           | Gateway fetches `<url>` into a read-only bare mirror in cowshed's own cache directory (02/05). Repo-scoped egress grant required.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed repo clone <url> [dir]`              | clone path                                            | `repo mirror` then a local `git clone --dissociate` into the workspace (default dir: repo basename).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `cowshed sim export <ws> [artifact]`          | drop path                                             | Copy a built iOS `.app` to the one-way drop dir for the personal-session simulator (02/14). Default: newest built app.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed app export <ws> [artifact]`          | drop path                                             | Mac-target sibling of `sim export`: copy a built macOS `.app` to the drop dir (02/14).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed attach [ws] [--all]`                 | mount path(s)                                         | `<ws>` one; cwd in a project = that project's sessions; `--all` store-wide. Mains always mounted (05).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `cowshed detach [ws] [--all]`                 | — (no stdout)                                         | `<ws>` one from store readdir (sidecar identity, no cwd/git); `--project` overrides; `--all` store-wide attached sessions. Mains never targets.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed mount main [--repo-id <owner/repo>]` | mount path                                            | Mount a project's main from store records — the repository binding and the checkout-path record — rather than a live git checkout, so a stub directory left by a broken workspace still mounts; a volume mounted with other flags is remounted with the canonical ones.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `cowshed grant <ws> …`                        | new grant revision                                    | `--read/--write <path>`, `--egress <host> [--opaque]`, `--repo <host/org[/repo]>`, `--sim <verb>`, `--preset simulator` (04/05); `--ports <N>` grows a macOS port block to at least N service ports, gateway excluded (04).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `cowshed grant --project-wide …`              | new standing-grant revision                           | `--read <path>`, `--egress <host> [--opaque]`: the project's standing grants every workspace runs under (04); bare prints them. Takes no `<ws>` and no `--write`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed identity add <remote>`               | the project's bound identities                        | Bind a remote main's checkout configures as a non-primary repository identity: from the next exec every workspace that can read the checkout fetches that URL (and its SSH form without the login) from the local clone (02). Refused when another adopted project binds the URL; an already-bound remote reports the binding unchanged.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed revoke <ws> …`                       | new grant revision                                    | Same selectors + `--all`, except `--ports`: a port block never shrinks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `cowshed revoke --project-wide …`             | standing revision (`--json`: standing-grant envelope) | `--read <path>`, `--deny-write <relative-path>`, `--deny <relative-path>`, `--egress <host>`, or `--all` alone; no workspace or write allow. Uses `Coordinator::revoke_project`. See the [revoke rules](04_sandbox.md#widening-and-narrowing).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed push <ws>`                           | preserved ref, source sha                             | `--branch <name>` plus optional incarnation/source/destination CAS expectations. Preserves under `refs/cowshed/<ws>/…`; never advances an integration branch or checkout (02).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed rebase <ws>`                         | new head sha                                          | `git rebase` onto `--onto <branch-or-ref>` (default `main`), or onto the lane base with `--into <lane>` (02, "Lanes"): git leaves out a commit whose patch the destination holds; a conflict stops in progress (`--continue`, `--abort`); `--fresh` sheds divergence. Optional incarnation/source/base CAS expectations (02). `--json`: `RebaseReport` (`rebased`, `aborted`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed land <ws>`                           | target branch, landed sha, checkout state             | Refuse a dirty tree, validate, ff `--target <branch>` (default `main`), retire the named source, then idle peers whose non-empty own commits since their recorded fork base overlap the landed range and are all held by the result; live supervisors, running or queued jobs, attached shells and dirty trees keep peers, each reported in `folded` (02, `land` step 5). A gated land never rebases: a target main has moved past is refused (exit 4, `next: cowshed rebase <ws>`). The target must be the branch main's checkout has checked out, whose HEAD/index/tree it advances; any other target is refused. `--check <cmd>`, `--no-retire`, and optional incarnation/source/target CAS expectations (02). `--into <lane> --ungated` folds a unit into its lane base by Git alone, rebasing it onto the lane base's branch first (nothing when already based; a conflict refuses, naming its paths): no check, seed freeze or adoption; main refuses it (02, "Lanes"; 16, "Ungated folds"). `--ref-lock <file>` names the lock file the target branch's other writers hold; land holds it only for the head fence and fast-forward (02, step 3). `--push-only` validates and preserves ref/head without FF, adoption or retirement; its JSON report has `kind: delivered`, versus `kind: landed` for an actual land. `--preflight` runs only land's fences before its check, mutating nothing: exit 4 with land's own refusal, or `kind: preflighted` with the incarnation, source head and target head the land can pin (02). |
| `cowshed fork [<src>] <ws>`                   | mount path                                            | Create a session from `<src>` (default main), carrying uncommitted state warm; minted with the `main` remote and branch `cowshed/<ws>` (02). `--clean`, `--ref <rev>` (implies clean), `--browse`, `--slot <n>`, `--register`, `--git-worktree`; `--markdown-only` is refused by the controller (a pure-Markdown edit belongs in place). `new` is an unknown command, not an alias.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cowshed rm <ws>`                             | — (no stdout)                                         | Perceived-instant (02). Requires live main to hold every change HEAD and its branches carry, judged by its merge into main changing nothing, by content or by patch identity, or kept by the ref `cowshed push` wrote for it (HEAD's checked-out branch, or another branch's own: 02, `rm` step 1); `--force` does not waive that check. `--abandon` keeps unlanded work in a verified bundle in the trash, its only copy; nothing is written into main's repository. Named removal also handles unpublished clones without activating them. `<ws>` comes from cwd's project or `--project`; outside any project it is resolved from the store readdir as `detach <ws>` is, and refused when more than one project holds the name. An explicit `rm` also refuses a workspace younger than ten minutes; `--force` overrides that, and the deletion log names who forced it (02).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed forget <owner/repo> --abandon`       | the preserved archive path                            | Retire a project registration the ordinary verbs can no longer open, from store records alone: no checkout, no `--project`. `--abandon` is the operator's explicit authorization, never inferred from a missing checkout, marker, lease or PID. Fences new opens, drains supervisors, detaches each image without force and renames the whole project tree to `.retired-projects/<operation-id>`; nothing is deleted. A busy holder refuses and the same command resumes (Project retirement). `--json`: `ProjectRetirementReport { repoId, archive, images }`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `cowshed mv <ws> <new-name>`                  | new mount path                                        | Rename a workspace, or move the project checkout with `cowshed mv main <new-path>` — the source decides whether the destination reads as a name or a path. Owns the unmount/republish/remount cycle, symmetric with `rm` (02). The rename carries uncommitted work and refuses an in-progress Git operation, a running job, or an untracked nested repository (`conflict` naming its directory, with commit, move-out, exclude and retry as the way out); the checkout move refuses a relative, occupied, parentless, or storage-overlapping destination — each naming the command that clears it (02).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `cowshed du [ws]`                             | `--json`: written/referenced per ws                   | CoW-aware usage (01).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `cowshed logs`                                | human table (`--json`/`--ndjson`: events)             | Controller telemetry (13). `--ws`, `--kind`, `--since`, `--follow`. Wraps `lmao-inspect` over the store segments.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed audit`                               | human table (`--json`/`--ndjson`: events)             | Gateway audit events (05/13). `--denied`, `--host`, `--ws`, `--follow` (live tail via the control plane).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed trace <trace-id>`                    | human waterfall (`--json`: span tree)                 | Terminal waterfall of a lifecycle op, exec, or land (13).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed mcp serve`                           | — (stdio/socket server)                               | Coordinator authority arrives only on an inherited FD/socketpair; it is never printed or placed in argv/environment (12).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed controller`                          | — (serves stdin)                                      | One coordinator controller connection for an embedding process, on standard input, which must be a Unix socket (else exit 2 before the project is resolved); exits 0 when the peer closes its end. A program that links cowshed is another build, and the daemon starts supervisors only for its own (11), so it runs its controller as this verb. Reconciles the project's gateway sessions before each exec, shell and checked land, as `exec` and `land --check` do (05).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `cowshed host`                                | — (serves stdin)                                      | One host connection on standard input for an embedding process: the host's own verbs as typed operations (07_api.md, Host operations). N-API's `openHost` starts it through the launcher on a socketpair. Never writes stdout.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `cowshed gateway run`                         | — (foreground daemon)                                 | launchd runs this as the gateway host's child (05_gateway.md "The gateway host").                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `cowshed gateway status`                      | `--json` status                                       | Health and versions; an install holding the install slot (05, "A pending install is visible"); then the host's scheduling, read on the host (05, "Host scheduling", Status): load averages, memory pressure, swap in use, jobs per class and state from each workspace supervisor, the image-cleanup entries waiting, and the land turns. A reading that fails is stated with its error, never as zero. `--json`: `GatewayStatus` with `pendingInstall?`, `scheduling?` and `landTurns`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | { unanswered }`.                                                                                                                                                                                                                                                                                            |
| `cowshed resize <ws\\                         | main> <size>`                                         | new capacity                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Grow the image (01a, "Detached growth"); with `--build <size>` instead, grow the workspace's build volume and its seed (16, "Substrate"). The supervisor stops for the resize and restarts after; a held volume refuses before anything changes. `--json`: `ResizeResult { workspace, volume: "workspace"\\ | "build", previousCapacity, capacity }`. |
| `cowshed defrag <ws>`                         | extent count after the rewrite                        | Rewrite the image contiguously (01a, "Clone cost follows extents, not size"): detached like `resize`, refused while busy or short of free space, copied without cloning, renamed over, verified, remounted. `--json`: `DefragmentResult { workspace, previousExtents, extents, bytes }`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `cowshed cache clear [<ws>]`                  | one line per cache cleared, with its entry count      | Delete the workspace's Nx state on its build volume (every entry in `.nx/cache` and `.nx/workspace-data`) and the project's gateway Nx store, each refused by name while running work holds it ("Cache clear", below). `nx reset` is no substitute: it removes the build links and leaves every entry. `--json`: `CacheClearReport { workspace, store }`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `cowshed rekey <ws\\                          | main>`                                                | workspace name (`--json`: `RekeyResult`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Rebuild the CA identity of a workspace whose companion key is missing or invalid: republish its quarantined sidecar beside the still-in-place image (revision + 1), mint fresh credentials into the live mount, consume the quarantine entry. Invalidates in-flight job certificates.                       |
| `cowshed gc [--abandoned]`                    | released bytes                                        | Two-phase exact candidate plan (01); `--dry-run` lists typed candidates and sums bytes with zero mutation. A plain sweep also releases the trash images earlier builds kept as recovery images, and drops the abandon bundles main holds every commit of by patch identity (`heldBundle`); only explicit `--abandoned` reclaims the rest of the recovery bundles `rm --abandon` kept in `sessions/.trash`, the only copy of what they hold (01); `--dry-run --abandoned` lists them without deleting them.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `cowshed doctor`                              | `--json` findings                                     | Invariant checks; each finding carries a `fix:` hint.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

`cowshed disk-command <diskutil|hdiutil> [--deadline-ms <positive integer>] -- <native args…>` runs one native argv
through the same `SystemCommandRunner` and disk-lifecycle lease as cowshed's own disk tools (05, "Disk-lifecycle
lease"). The option is parsed only before `--`; every later argument remains the tool's byte-exact argument, even
`--deadline-ms`. Standard input is inherited, and terminal stdout/stderr and a normal child's exit code or
`128 + signal` pass through unchanged. The command needs no project; `--json` is refused because its output belongs to
the tool. A Disk Arbitration release of a disk-image disk another user owns, or one whose disk cannot be named, is
refused before it spawns as a conflict (`ReleaseNeedsAuthorization`, 01a_apfs.md "Release authority"), never left
waiting on an administrator prompt.

`--gateway-socket <absolute path>`, before `--`, selects the control socket for this command's disk-lifecycle admission
instead of the host socket. It uses the same peer authentication, admission and child runner; it changes neither release
authority nor registry-bound revalidation inside admission. No environment variable selects this route. A relative path
is refused.

For SAFE commands, the facade starts one monotonic total admission-and-child budget before handing work to the blocking
runner: 120000 ms by default, or the supplied positive integer milliseconds. Exhausted admission never executes the
native tool, including when the fork lock spends the remaining budget: the final pre-exec check refuses it and the spawn
operation reaps any failed fork stub. Such a stub performs no disk-tool effect. A running overrun is killed through its
owned process group and actually waited/reaped before a timeout is reported. The facade's timeout is wrapper exit 103,
not a fabricated child exit or signal: stderr separately retains the pid of the process group it signalled and the
child's observed terminal status that reaping it returned, or says that no child started. Kill and reap failures are
wrapper failures (exit 100), with their causes and any still-running child pid retained; they never assert successful
termination. A SAFE caller passes its budget and waits for the facade's actual terminal close; it does not time out and
abandon or kill the facade, unref it, or destroy its pipes.

Protected image commands (any `hdiutil`, `diskutil image …`, `diskutil eject`) retain their 120 s warning point and 30 s
warnings thereafter, detached session, file-backed output and never-kill custody (01a_apfs.md, "How the APFS host
degrades" §2). `--deadline-ms` does not grant authority to cancel them or change that warning policy. Their caller may
stop waiting with a bounded, named still-running report, leaving the operation and its leases alive.

`cowshed disk-command hdiutil --registry-entry <id> -- detach <device>` binds a detach to the IORegistry entry ID the
caller inventoried the device as (`0x…` or decimal; any other argv with the option is a usage error). Inside the
disk-lifecycle admission, immediately before the spawn, the runner reads that entry again: the detach runs only while it
is still registered with `<device>` as its whole disk. Otherwise nothing is detached, and the command fails with a
wrapper exit and `refused … nothing was detached`. A device name that was recycled while the command queued for
admission is never detached. The window that remains, from that read to `hdiutil`'s own open of the name, is the one
every cowshed release has. A caller that detaches a device it did not just attach itself uses this, never `-force`
(`packages/nx-plugin/README.md`, RAM temp).

During an interrupted re-adoption (02 §Re-adoption), `adopt <checkout> --re-adopt` and
`mount main --repo-id <owner/repo>` resolve from the existing controller checkout-path record only when a strict
unfinished journal owns the exact repository and checkout. No Git checkout is required at that boundary. They finish the
recorded replacement incarnation, never a second reclaim; a checkout-path record without that journal is not recovery
authority.

When main's image is missing, `adopt <checkout> --re-adopt` re-adopts it from the plain Git checkout at its path (02
§Missing main image); a checkout whose remotes do not name the project's repository is refused (exit 4).

`gc` never lets a candidate-local failure prevent later candidates from being tried; it reports each deferred path and
diagnostic on stderr and in the structured result, alongside the allocated bytes of the images it released for deletion.
`doctor` diagnoses sidecarless session images by name rather than treating them as live workspaces or failing the
inventory. Store-wide `ls --all` and attach/detach skip an unreadable project with a named warning and still process
other projects; identity collisions remain errors. Resolving which project a checkout belongs to does the same: an
unreadable project is skipped with a named warning while a readable one claims the root, and fails only a root no
readable project claims, because the unreadable one may own it. A retired session's `<ws>-<incarnation>.dmg` is never
read as a retired main, whatever its name begins with. `doctor` retains a finding for each unreadable project while
inspecting the others.

Store-wide attach and detach likewise try the other workspaces after one target or project fails and name each skipped
target; successful mount changes still reconcile with the gateway.

### Cache clear

`cache clear [<ws>]` deletes both Nx caches cowshed keeps: the workspace's own Nx state on its build volume
(16_build_volumes.md, "Clearing a checkout's Nx state") and the project's accepted-task store the gateway serves
(03_caches.md, "Nx accepted-task artifact service"). It acts on the workspace in place, so it infers `<ws>` from the
working directory. The store is per project, so clearing it for one workspace clears it for all of them; every other
checkout's Nx state is its own and is cleared by naming that checkout. `nx reset` is no substitute: in a cowshed
checkout `.nx/cache` and `.nx/workspace-data` are links into the build volume, and `nx reset` removes the links and
leaves every entry on the volume, where the next refresh links them back.

The workspace is cleared first, through `coordinator.clearNxState` (07_api.md), then the store, through the gateway's
`nx-cache-clear` control operation (05_gateway.md). Each refuses as a `conflict` naming what is running, and a refusal
clears nothing of that cache. The workspace refuses while a job or land holds its build volume, a lifecycle operation
holds its image lock, or a process other than its Nx daemon has a task database open or is opening one. The store
refuses while one of the project's Nx requests is admitted or queued, or its backend is starting. These are the existing
records of running work — the build volume's job-admission hold, Nx's own database descriptors and open locks, the
gateway's admission table — and no second registry is kept. A store refusal after the workspace was cleared says the
workspace was cleared; clearing is idempotent, so the same command clears the store once the named work ends. Nothing is
stopped except the checkout's Nx daemon (as `nx daemon --stop` stops it) and the project's Nx backend, and each starts
again on its next request.

Bare stdout is one line per cache,
`cleared workspace <ws> Nx state: <n> entries in .nx/cache, <m> in .nx/workspace-data, build volume <id>` and
`cleared project <owner/repo> Nx store: <k> entries`. An entry is one name directly in such a directory, deleted with
whatever it holds; a store entry is one archive. `--json` returns
`CacheClearReport { workspace: NxStateClearResult { workspace, volume, cacheEntries, workspaceDataEntries }, store: NxStoreClearResult { repoId, entries } }`.

### Build-volume names and reclamation

Every mounted live build volume is named `[cowshed] <project> — build <checkout>` (`<project>` is `<owner> · <repo>`),
never only `[cowshed]`. The supervisor names new volumes off the provisioning path, reconciles existing names when the
supervisor starts, and renames a volume when land or adopt moves it to another checkout. Seeds are never mounted and are
never named: a mount just to rename one would advance its image mtime, the seed-freshness clock (16, "Substrate").

`rm` and `land`, including `land --no-retire`, collect build volumes synchronously: the moment no checkout links a
volume and it is no target's latest seed, it is detached and released, unless a running cowshed job's shared hold or an
unfinished create/fork in the lifecycle intent journal proves an owner. A release commits the image with one rename into
the project's `build/.deleting`, nudges the gateway, whose `image-cleanup` thread frees it a bounded step at a time
(05_gateway.md, "Deleting released images"), and frees no byte itself, so space lags the command by the free. A nudge
the gateway does not take (absent, silent for 2 s or refusing) is said on stderr and fails nothing: the release stands,
and the entry waits for the gateway's next listing. `gc` collects build volumes the same way, committing and never
freeing. A non-forced unmount gets a bounded grace; if it still refuses and no owner remains, release forces the
unmount. It names each initial holder's pid and command on stderr and says whether the volume was released within the
grace or forced afterwards. Force revokes those processes' volume access rather than killing them. A job admitted before
an adoption retains its old volume until its hold is dropped. Failed forks and superseded seeds use the same release
rule (16, "Garbage collection").

Both explicit `gc` and opportunistic collection explain deferrals: job holds, unfinished create/fork work, unreadable
detached checkouts or records, and release failures name the volume and reason; opportunistic collection counts routine
detached/still-forming deferrals on one line. A project open likewise says the unfinished lifecycle operations it leaves
to other verbs as one count, labelled `info` and explicitly non-blocking for this command; `doctor` names each. This
context is not a refusal and changes neither the exit code nor the JSON error's message or hint. A refusal on one volume
does not hide later candidates. `gc --dry-run` reports candidates and reasons without changing mounts, labels or images.

### Project retirement

`cowshed forget <owner/repo> --abandon` retires a project registration that the ordinary verbs can no longer open: its
checkout is gone, or its in-image marker, resource lease or repository record is corrupt. It resolves the project from
the store by `<owner/repo>` alone and needs no checkout, working directory or project: `--project` is a usage error,
project discovery never runs, and the host's existing validated storage is used without opening or remounting the
project. Like every verb that changes lifecycle state it first asks the daemon whether it runs this CLI's build, because
only a daemon that implements the fence enforces it.

`--abandon` is the operator's explicit authorization, and the only one. Without it the command is a usage error (exit 2)
whose hint is the completed command line. Nothing else stands in for it: a missing checkout, marker or lease, a dead
PID, a path prefix or an age never authorizes a retirement, and nothing retires a project automatically. Before any side
effect the authorization is persisted as an immutable record outside the project tree,
`.retirements/<owner>/<repo>.json` in the store, and from then on no open, heal or adoption of the project is admitted.
The retirement then advances through recorded phases (02_workspaces.md; the record's layout is 01_storage.md): `fenced`,
`drained`, `detached`, `archived`. It drains every running supervisor of the project and requires each one's positive
receipt, detaches each of the project's backing images without force, and renames the whole project tree — the Git
repositories inside the images, grants and resource evidence — unchanged to `.retired-projects/<operation-id>` in the
store. Nothing is deleted, rewritten or copied, and the retained image bytes are not freed: the archive is the
preservation artifact. An archive is reserved host storage that no verb discovers, heals or reclaims, and one that
already exists is never overwritten. Only a released image already waiting in the project's deleting directories is
freed there, by the gateway's cleanup, whose startup listing finds it in the archive (05_gateway.md, "Deleting released
images"); it is no evidence.

A supervisor that has not finished draining, an owner that still holds the project and a holder that keeps an image busy
each refuse their step as a `conflict` (exit 4), and nothing is forced: the project stays fenced, and the same command
run again resumes from the recorded phase. The gateway daemon finishes a recorded retirement the same way at its next
start, before it heals any project; it resumes only a record that already holds the explicit authorization and never
writes one. Run again after it finished, the command reports the archive the finished retirement made.

stdout is the archive path. `--json` returns `ProjectRetirementReport { repoId, archive, images }` through the ordinary
envelope, where `images` counts the backing images the preserved tree carries, all of them detached. Before its first
side effect the command states on stderr the `--abandon` that confirmed it and what will happen; afterwards it names the
repository, the detached image count and the archive, says the tree is preserved — never that bytes were deleted — and
hints `next: cowshed ls --all`.

### `cowshed ls` detached rows

`ls` must never attach an image to read it (that would blow the ≤50 ms budget and mutate mount state), but the base
commit, branch, and age live in the in-image marker. So for a **detached** workspace those fields come from the snapshot
every grants sidecar carries (01_storage.md); a sidecar without one is unreadable, not a row with empty columns. `state`
and `name` are always accurate because they derive from readdir + getmntinfo alone.

### `cowshed ls` unproven workspaces

A workspace whose mount does not prove itself the workspace's (01a_apfs.md, "Ownership, identity, and the volume label")
is neither attached nor detached, so `ls` gives it no row. `ls` lists every other workspace, names that one on stderr
with its refusal and `next: cowshed doctor`, and exits 0. `--json` keeps `WorkspaceInfo[]` on stdout; `ls --all --json`
carries the workspace in its project's `unproven` (07_api.md, `UnprovenWorkspace`). A shed whose marker is unreadable
during its own detach costs its own row, never its project's listing.

### Egress grant modes

`--egress <host>` grants an intercepted host by default: the gateway terminates TLS under the workspace CA and injects
the Keychain credential + trace context (05_gateway.md). `--opaque` grants every `--egress` host of the same invocation
as an opaque CONNECT tunnel instead (pinned clients, Go on macOS; no injection), and is a usage error without one. A
host holds one rule, so granting a host again restates its mode — `--egress <host>` alone turns an opaque host back to
intercepted. Per-port narrowing is a further field of that rule, set through the coordinator API's `EgressRule`; the CLI
takes no flag for it. A bare `cowshed grant <ws>` (no flags) prints the current set with a `mode` column on the egress
rows.

`cowshed grant --project-wide` addresses the project's standing policy: read paths, egress hosts, and workspace-relative
`--deny-write` and `--deny` paths every workspace (including main) runs under. The policy lives outside the workspaces
(04_sandbox.md, "Project-standing grants"), so a workspace grant cannot remove a project deny. The project comes from
ordinary discovery — the cwd, or `--project <git-root>`. `--write` and a `<ws>` alongside `--project-wide` are usage
errors; a write allow remains a per-workspace decision. A bare `cowshed grant --project-wide` prints the standing set,
and `--json` includes `{ revision, read, denyWrite, deny, egress }`.

`--sim <verb>` grants personal-session simulator broker verbs (`openurl`, `install` — 04/05/14); dev-side headless
simulators need the `--preset simulator` profile class instead (CoreSimulator IPC), not a `--sim` grant. `install` is
additionally bound to drop-dir artifacts and the human-gating rule (14_nix.md).

`cowshed grant <ws> --ports <N>` (macOS) is the workspace's port-capacity request: the block must hold at least N
service ports, the gateway port at its base excluded, so it becomes the smallest aligned power-of-two block with N+1
ports (`--ports 80` turns the initial 64-port block into a 128-port one). It sets `GrantDelta.servicePorts` (07_api.md);
growth is monotone, so a count the block already holds leaves the block, the grant revision, and the supervisor
unchanged, and `revoke` takes no `--ports`. `--ports 0`, a malformed count, and `--project-wide --ports` are usage
errors; Linux, whose workspaces each own a private loopback, refuses the grant rather than recording a block. A larger
block keeps its base when it can grow in place and otherwise moves to a disjoint one; the block it left stays reserved
to the workspace as `retainedPortBlocks` until the workspace is removed (04_sandbox.md). Either way growth needs an idle
workspace: while any of its jobs is active the grant is refused immediately — it never waits or queues — so services
that need the ports are launched after the grant. The next exec starts a fresh supervisor whose jobs get the new block's
Seatbelt profile and `COWSHED_PORT_BASE`/`COWSHED_PORT_BLOCK_SIZE` (04_sandbox.md). The reserved range is a finite host
resource shared by every workspace: a block that has no free aligned place is refused.

`cowshed grant <ws> --loopback-any` (macOS) lets every job of the workspace connect to any loopback port, the control
plane's and every sibling's included, for a tool whose port-0 listener the Node and Bun preload cannot move
(04_sandbox.md). It sets `GrantDelta.loopbackAny` (07_api.md), prints a warning naming that reach, lists as its own
`loopback` row, and advances the grant revision; `cowshed revoke <ws> --loopback-any` withdraws it.
`--project-wide --loopback-any` is a usage error on both verbs, and Linux refuses the grant.

`cowshed exec` and `cowshed shell` accept `--session <name>` to bind to a named session in the workspace supervisor,
whose cwd and environment overlay apply to each of its commands; with or without it, the command runs in a warm exec
host of the workspace shell (11_shell.md). `cowshed exec` waits for its command however long it runs and exits with the
command's status. `--timeout <dur>` stops waiting after that long: a command still running then keeps running under the
supervisor, and `cowshed exec` exits **103** with an error naming its job and how to reach it, because it has no output
or status of the command's to give. `--background` asks not to wait at all: the numeric `JobId` is stdout, the exit is
0, and stderr names how to reach the job. The CLI has no verb that reattaches to a job; the API does
(`WorkspaceHandle::job`, 07_api.md). Leaving a job running changes attachment only. It forces memory-only prefixes into
lazy protected files so later reads are durable; it does not imply every terminal job has stream paths. Every foreground
command is held for as long as `cowshed exec` runs (11_shell.md, `hold`): an exec process that ends first, however it
ends — a caller's timeout killing it included — leaves a job nobody waits for, and the supervisor kills it
(05_gateway.md, "A job lives with its requester"). `--timeout` releases the hold before it leaves the job running;
`--background` takes none. A `--class resident` command — a daemon or worker that stays up and is idle between requests
— starts at once and costs nothing while idle (05_gateway.md, "Classes"). Every exec gets a make jobserver of its own,
`min(--want, cores)` tokens and `cores` when it names no `--want`, which bounds how many compiler and test threads its
command starts (05_gateway.md, "Parallelism inside a job"); `--want <n>` asks fewer. A `--class interactive` command is
one its caller waits on: it runs at nice 0, above a gate at +5 and background at +10 (05_gateway.md, "Classes"); a
workspace may ask for it. A background exec waits to start while the host reports memory pressure (05_gateway.md,
"Memory pressure"). Every `exec` flag that asks the scheduler for something is a typed field of the API's exec request
(07_api.md, "CLI and API exec parity"), so an embedding process can ask for everything this command can.

`--host` names where the command runs, not how it is scheduled. The argv runs once at the host boundary, outside the
workspace sandbox and without shell activation, from an empty environment filled with the operator's own (07_api.md,
"Task server scheduling and trusted host gates"). It composes with every `--class` and defaults to background like any
exec: host unit work — a test that needs real APFS images or the host controller — runs at background nice +10 and waits
out memory pressure like any background start (05_gateway.md, "Classes", "Memory pressure"), holds no install slot
(05_gateway.md, "The install slot") and leaves the gate class to its lander. A land's gate asks for that class as
`--class gate --host` (05_gateway.md, "A land is a gate job"). Unit work never passes `--class gate`.

`--host` once required `--class gate`, because the host route was built for the land's real-APFS gate, and the tie made
two things one. What keeps an unsandboxed command safe is its route, not its class: only the trusted coordinator route
may name the host scope, and a workspace's own capability is refused with the typed class-not-permitted error whatever
class it asks (05_gateway.md, "Classes"). That, and the host scope's own rules — an argv only, read-write, no session,
an environment the operator supplies — hold at every class. What the tie added was scheduling alone: every host command
entered its repository's gate order and held the install slot, so host unit work serialized with, and queued ahead of,
the lands of its repository (measured 2026-10-21: a lander waited 61 minutes behind host unit work admitted as gates),
or left cowshed for an unbudgeted host shell. Nothing about the host boundary requires the gate class.

The CLI is that coordinator route (07_api.md, "CLI and API exec parity"): every exec it admits travels as
`coordinator.exec`, whatever its class, so who asks decides the route and the class asked for never does. What keeps a
workspace's own process from asking is that it cannot reach that authority. On macOS a job may run the installed CLI,
but its sandbox denies cowshed's store, which holds every project's records, the daemon's socket and every workspace
supervisor's socket, and it admits no controller socket. So the CLI fails at its first write into the store, as
sandbox-denied (exit 6) naming the path, before any route is asked. On Linux there is no sandbox yet (04_sandbox.md,
"Linux enforcement"): a workspace command already runs with the invoking user's full authority, store included, so the
host scope gives it nothing it lacks, and this boundary arrives with the Linux sandbox.

`--register` (on `cowshed fork`) additionally adds a `cowshed/<ws>` remote in the **main** workspace pointing at the new
workspace's canonical mount, so a human in main can fetch and review the workspace's branch in place. Off by default
because it is host-side state that accumulates; `cowshed rm` and `cowshed land` drop it at retirement (02).

`--git-worktree` (on `cowshed fork`) mints a git-worktree workspace: a registered linked worktree of main's repository
sharing its object store and refs, instead of the default standalone clone. Requires main mounted, then and thereafter;
gets no `main` remote (nothing to fetch from).

`--slot <n>` (on `cowshed fork`) mounts the workspace at a stable, recycled path
(`<mount-root>/<owner>/<repo>/slot@<n>`) instead of a name-derived one. The host root is fixed, derived and never
configured: `/private/var/folders/cowshed/<uid>/mnt` on macOS and `~/.cowshed/mnt` on Linux (01a_apfs.md, "Stable
workspace mount root"). `owner` and `repo` are the separately validated and encoded components of the primary `repo_id`,
never an unsplit path value. Successive workspaces in the same slot inherit each other's **path-keyed** cache warmth
(Xcode DerivedData, and whatever a build records by absolute path; cargo's own fingerprints are package-relative and
need no slot) — opt-in, because it trades workspace-path uniqueness for warmth and only one workspace may hold a slot at
a time (exit 4 if occupied).

## Self-driving conventions

- Every failure's stderr includes the exact command that would resolve it
  (`cowshed: workspace not mounted — run: cowshed attach raven`), and `--json` errors carry the same in `hint`. Agents
  recover without documentation.
- `cowshed` with no args prints a compact command map to stderr and exits 2 — safe for probing. The map groups the verbs
  under headings by what they are for, in this order: workspace (fork, ls, path, exec, attach, detach, mv, rm),
  integration (rebase, land, push, doc-commit), grant, setup and repair, storage and cache, and last the verbs tools and
  embedding processes call (`build-state`, `disk-command`, `land-turn`, the lock and holder probes, `controller`,
  `host`). The groups are the parser's one command registry, so a verb cannot dispatch and stay off the map. Grammars
  and command summaries wrap within 96 columns, with one summary column across every group. A grammar that exceeds the
  summary column puts its summary on the next line rather than widening the map.
- Destructive operations (`rm`) state on stderr precisely what will be destroyed and which flag confirmed it; there are
  no interactive prompts, ever (agents can't answer them). Missing confirmation flags are exit 2 with the completed
  command line in the hint.
- Output stability: bare-stdout shapes and JSON keys are covered by CLI contract tests (08_testing.md); changing them is
  a breaking change.

### Inferring `<ws>` from the working directory

A command run inside a mounted workspace may omit that workspace's name when the verb infers from cwd. Resolution is
containment of the canonical cwd in exactly one currently mounted workspace (01a_apfs.md), which is exact because mount
identity is keyed off the in-image marker, and which refuses an ambiguous match rather than picking one. An explicit
argument always wins; inference never overrides what the caller named. Outside any workspace those verbs refuse and name
both ways out: name one, or run the command from inside one.

The split is by what the verb does to the workspace, not by convenience:

| Infers from cwd                         | Requires the name          |
| --------------------------------------- | -------------------------- |
| `rebase`, `push`, `path`, `cache clear` | `rm`, `land`, `mv`, `exec` |

Verbs that act on a workspace **in place** infer it. Verbs that **retire it** (`rm`, `land`) or **rename or move it**
(`mv`) require it to be named, so that losing the workspace you are standing in is always something you asked for by
name rather than something the working directory decided for you. `exec` is excluded for a second reason as well: its
workspace argument is positionally ambiguous with the command it runs.

`attach` and `detach` change mount state but do not infer a single workspace from cwd. `attach [ws] [--all]`: `<ws>` one
session; no name while cwd is in a project checkout or session = that project's detached sessions; `--all` every
detached session store-wide. Mains are always mounted and are never attach targets. `detach [ws] [--all]`: `<ws>` one
session resolved from the store readdir (`<owner>/<repo>/sessions/<ws>.image` plus sidecar/marker identity) without cwd
or git discovery; `--project` still selects the project; `--all` every attached session store-wide. Mains are never
detach targets. A bare `detach` with neither a name nor `--all` is usage.

Before anything stops, detach refuses while a process cowshed does not run holds the mount, naming each holder's pid,
command and what it holds where (02_workspaces.md, "Holders cowshed does not run"). Then it stops the workspace's
supervisor, then stops the Nx daemon the checkout's own `.nx/workspace-data` record names, as a land's quiesce does
(`nx daemon --stop`'s `SIGTERM`): the daemon's cwd is the checkout, whichever shell started it, and Nx starts a fresh
one on the next run. A volume the kernel still refuses to unmount is a `conflict` naming every holder, pid and argv (an
open file, a working directory or an executable on the volume), with stopping them as the next move. It is not a storage
failure. When no process of this user holds it, the refusal says so and names what is mounted inside it.

A verb that may attach an image first makes room under IOKit's media match-notification ceiling (02_workspaces.md,
"Making room for an attach"): idle workspaces of any project detach, least recently used first, each said on stderr.
When nothing idle frees enough, a warning on stderr names the attached workspaces and the IOMedia each holds, and the
verb attaches anyway.

### Resident workspaces

`path <ws>` and `exec <ws> -- …` name one workspace. When that workspace is mounted, every fact `path` depends on is
live, and `exec`'s are too once its daemon-owned supervisor (11_shell.md) already serves its current authority — or a
newer grant revision of it, published since the records were read. They are answered from that state without opening the
project controller. The answer reads, fresh on every call, only the records that decide this workspace: the in-image
marker at the invocation's Git root (which names the project), the named workspace's marker at its mount and its active
sidecar, slot records (which place the mount), and the lifecycle-intent journal; `exec` also reads the project's policy
(whose revision is part of the effective grant revision). `path` asks the host one thing, whether a filesystem is
mounted exactly at the mount path, and never asks the supervisor: it runs no work there, so it answers while a
supervisor of another build drains its jobs (11_shell.md, "Draining a supervisor of another build"), as the controller
does, whose attach of a mounted workspace attaches nothing (02_workspaces.md, "`cowshed attach` / `cowshed detach`").
`exec` also asks which authority the supervisor's socket reports, and whether the gateway has the workspace's session
installed at that revision — exactly the case in which the controller's pre-exec reconcile would install nothing for it.
And `exec` admits a job only onto build state the controller's refresh (16_build_volumes.md, "One link per checkout")
would leave as it is: every fixed link the checkout's build volume records is in place and reaches its volume directory
(a declared path nothing occupies needs none), and the tracked build inputs still have the fingerprint that volume
records, computed as the refresh computes it (one `git ls-files`, one more per declared pattern, and a hash of the
tracked manifests). The job then runs through the supervisor's socket, relayed as the controller path relays it.

Any disagreement opens the controller exactly as before, which is what does the work: a workspace that is detached; for
`exec`, one unserved, served under an older grant or another incarnation, a gateway session not yet at the served
revision, build inputs whose fingerprint moved (a `.cowshed.toml` `[build] state` edit, a new manifest), a checkout that
links no build volume or whose volume records no fingerprint (only discovery says what build state it has), a
build-state link a tool displaced or removed, a linked worktree, or unfinished lifecycle work on the workspace, on
`main`, or on the project identity. Git discovery steered by `GIT_DIR`, `GIT_WORK_TREE`, `GIT_COMMON_DIR`,
`GIT_CEILING_DIRECTORIES` or `GIT_DISCOVERY_ACROSS_FILESYSTEM`, an omitted `<ws>`, `path --slot`, and `exec --session`
always go through the controller. No directory is listed: a project with a thousand retired sessions answers as fast as
one with a single workspace. Once the supervisor admits the job, every later failure is that job's and is reported,
never retried through the controller. A discard an earlier refresh left pending is resumed by the next refresh that
opens the controller, or by `cowshed gc`.

What a resident answer does not re-check is what the serving supervisor proved when it opened: the project binding
against Git's remotes. The recorded binding changes only through cowshed verbs, which change the records read above; a
remote edited by hand is reconciled by the next verb that opens the controller. `COWSHED_TIMING=1` prints each step of
either path, including why a resident answer declined, on stderr (13_telemetry.md).

## Configuration

None required. `cowshed adopt` through daily use works with zero files. Optional `.cowshed.toml` at the repo root, all
keys optional:

```toml
capacity = "100g"              # image cap
[build]
capacity = "100g"              # build volume cap for a volume created from nothing; clones inherit theirs
state = ["vendor/upstream"]    # declared build state no capability detects (16_build_volumes.md)
[gateway]
port = 7644                    # macOS host control plane; Linux private-netns connector; never a service port
[cache]                        # extend (never replace) the convention table
extra_workspace_dirs = ["build-out"]
[shell]
output_limit = "1GiB"         # combined stdout+stderr per job; explicit output-limit terminal state
[nx]
daemon_idle = "5m"             # a shed unused this long has its kept Nx daemon stopped (04_sandbox.md); main's copy only
[land]                         # main's copy only: the declared land route (02_workspaces.md "A declared land")
turn = "acme"                  # the land-turn queue; the ref turn is "acme.ref"
gate = ["sh", "tooling/land-host.sh", "--gate", "{workspace}"]  # the gate; {workspace} is the landing workspace
push = "origin"                # the remote a landed main is pushed to; omitted, nothing is pushed
after = [["sh", "tooling/land-install.sh", "{before}", "{head}"]]  # run in order after the push, detached
[doc]                          # main's copy only: `cowshed doc-commit` (02_workspaces.md)
paths = ["*.md"]               # documentation: `*` matches any run of characters, `/` included; `?` one
check = ["prettier", "--write"]  # optional; run with the admitted paths appended
```

`[land]` and `[doc]` are read from main's checkout alone, as `[sandbox]` is: a workspace's copy is the agent's to edit.
`turn` is a land-turn queue name (1 to 60 of `A-Z a-z 0-9 . _ -`, not starting with `.` or ending in `.ref`, so
`<turn>.ref` is one too). `gate` and each `after` command are argv lists whose program is literal: a word spelled `{…}`
must be one of that command's slots (`{workspace}` for the gate; `{before}` and `{head}` for an after command), and any
other word is passed as it is. `[doc]` needs `[land]`, whose turn names the ref turn it holds.

macOS port allocation is not a configuration setting. Fixture stores allocate from a reserved fixture partition of the
validation range; the host never assigns there. The partition bounds are defined once in code. Allocation, growth, and
explicit slot assignment use the store's partition, while persisted blocks validate against the full validation range
(04_sandbox.md/05_gateway.md).

Workspace environment lives inside the image as `.cowshed/env`: on macOS it exports `COWSHED_PORT_BASE` and
`COWSHED_PORT_BLOCK_SIZE` (= the authoritative detached-metadata block base and size) so
`vite`/`astro`/`metro`/`devenv up` select ports inside the workspace's own block instead of colliding with siblings
(04_sandbox.md). It never exports the bare workspace token (02_workspaces.md). Block size is not configuration: a
block's size is persisted with it, new workspaces get 64 ports, and `cowshed grant <ws> --ports <N>` grows it
(04_sandbox.md). Linux allocates no port block and emits no port sentinel: dev servers bind its private loopback, while
ordinary package tools retain their configured `http://127.0.0.1:7644/…` proxy and registry URLs through exactly one
trusted minimal connector launched inside that namespace. Cowshed publishes `.cowshed/env` whenever it mints credentials
and whenever it starts the workspace's supervisor; `.envrc` sources it (see 02_workspaces.md). No CLI verb prints these
values on demand.

## Tradeoffs

**Prose-on-stdout rejected.** Mixed streams are the root cause of agents parsing with `grep` and breaking on wording
changes. The stdout/stderr split costs nothing for humans (terminals merge the streams visually) and makes every cowshed
invocation composable. `-q` exists for callers that want silence, not a different contract.

**Interactive prompts rejected.** A confirmation prompt is an API that only humans can call. Explicit flags (`--force`)
plus precise stderr statements provide the same safety with a uniform caller model.

### Structured stdin

`cowshed exec` accepts exactly one stdin source. With no stdin flag it inherits interactive stdin as usual; `--stdin`
explicitly streams opaque bytes from the invoking process, `--stdin-base64 <data>` decodes inline bytes strictly before
submission, and `--stdin-file <rel>` asks the supervisor to open a workspace-relative regular file. These flags populate
`ExecRequest.stdin`; they never rewrite or text-normalize byte-exact `CommandArg` argv, interpolate shell text, or
synthesize `< file`. File paths must remain beneath the workspace and are opened read-only with no-follow traversal;
absolute paths, symlinks, devices, sockets, directories, and races fail closed. Backpressure reaches the caller/file
reader, EOF closes child stdin once, and interrupted input records incomplete delivery in `JobInfo` without implicitly
killing the job.

### Shell redirection and explicit publication

Cowshed never uses a shell AST to create a hardlink alias. An optional real-AST path may classify a proven literal
`>`/`2>` destination as `OutputStorage::Redirect` only when the supervisor controls the actual writable descriptor and
accounts admitted bytes at the identical combined-quota boundary. It snapshots the terminal source into an independent
protected inline or clone/reflink/copied file artifact. Polling/tailing or reopening a path is forbidden. If those
conditions are unavailable, ordinary shell semantics run and `StreamInfo` claims only bytes that reached the captured
pipe.

`--stdout-copy <rel>` / `--stderr-copy <rel>` are separate explicit post-terminal publication requests. Each defaults to
`PublicationPolicy::CreateNew`; one `--replace-output` changes every requested copy in that invocation to
`PublicationPolicy::Replace` and is a usage error unless at least one copy option is present. Publication materializes
from the sealed canonical protected artifact using clone/reflink/copy plus fsync and atomic rename. It never hardlinks,
changes `StreamInfo.storage`, becomes a read/authority source, or promises a destination while the command runs.
