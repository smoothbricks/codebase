# Project capabilities

Cowshed's core owns sandboxing, copy-on-write workspaces, leases, the gateway and land. Project tooling is optional. A
plain Git repository needs neither `.envrc` nor Nix, and cowshed does not create, rewrite or require repository shell
hooks.

## Detection inputs

Detection reads convention markers inside the workspace, never an enclosing checkout or the operator's shell
environment. Its input is the workspace mount, command cwd, host home, main's `[caches] home` entries (below), the
host's program directories, private environment root, short runtime directory, and optional public gateway trust bundle.
Paths derive from those inputs; secrets and ambient PATH are not detection inputs.

The program directories are the host's conventional installation directories, in search order, where core and every
detector resolve an individual tool ("Bootstrap executables"): the user's and the system's Nix profiles, Homebrew's,
`/usr/local/bin`, `/usr/bin` and `/bin`. The controller resolves them once, from HOME and its effective user, when it
validates host storage, and every workspace sandbox it builds carries that value. Detection searches only the
directories it is given, plus a detector's own tool convention (`~/.bun/bin`, Go's installer directory), and never asks
the host who it is; a test passes its own directories. **Why**: detection that resolved the effective user itself
inspected `/etc/profiles/per-user/<user>/bin` from inside any test that built a workspace sandbox, and a test running in
a workspace sandbox, whose profile denies that path, failed with `EPERM` before its subject ran.

Project-scoped detectors inspect the workspace root, or the contained relative directory selected by that capability's
override. A tool invoked through a task runner gets the same cache and daemon authority as one invoked directly. There
is no recursive walk through dependencies or build trees. Direnv alone inspects command ancestors inside the workspace
and selects the nearest `.envrc`; an explicit directory override selects only that directory. Spawn admission refreshes
the convention snapshot, so adding or removing a convention takes effect without restarting the supervisor. An unchanged
snapshot reuses its rendered policy; a changed snapshot renders a matched sandbox/profile pair before applying job-mode
narrowing. Warm-host identity includes the resulting profile, environment and shell directory. At mint the same
detectors inspect the minted tree. A detector may examine file contents to disambiguate a convention; it never executes
project code during discovery. A convention that resolves outside the workspace is rejected, except for a provisioned
directory marker's fixed link through `.cowshed/build`. Missing markers mean absence; other filesystem errors report the
path and failure.

| Detector  | Convention                                                                                  | Contribution                                                                                                        |
| --------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| direnv    | `.envrc`                                                                                    | Contained shell activation, private approval state, read-only host `source_url` store, bootstrap executable         |
| Nx        | `nx.json`                                                                                   | The checkout's one `.nx` and daemon for every job; short shared socket namespace; discard inherited daemon records  |
| Codegraph | `.codegraph/` at the selected project root                                                  | The project-relative index on the build volume, cloned and adopted behind its fixed relative link                   |
| cargo     | Tracked `Cargo.toml` files                                                                  | Shared registry/git caches, exact cache-state files, Git and trust; target build state only without mbx (below)     |
| Go        | `go.mod` or `go.work` at the selected root, or any tracked `go.mod` below it                | Shared module/build caches; no generated GOENV or toolchain/proxy policy                                            |
| Bun       | `package.json` and `bun.lock` or `bun.lockb`                                                | Bun install cache and JavaScript trust/proxy settings; each installed package's `node_modules/.cache`               |
| npm       | `package.json` and `package-lock.json` or `npm-shrinkwrap.json`                             | npm content cache and JavaScript trust/proxy settings; each installed package's `node_modules/.cache`               |
| pnpm      | `package.json` and `pnpm-lock.yaml`                                                         | pnpm store and JavaScript trust/proxy settings; each installed package's `node_modules/.cache`                      |
| uv        | `pyproject.toml` or `uv.lock`                                                               | uv cache and platform certificate opt-in                                                                            |
| Zig       | `build.zig`                                                                                 | Zig global cache                                                                                                    |
| Gradle    | `settings.gradle`, `settings.gradle.kts`, `build.gradle` or `build.gradle.kts`              | Gradle cache, not host credentials or configuration                                                                 |
| Nix       | `flake.nix` or `devenv.nix`, or an `.envrc` chain reaching Nix (below)                      | Nix client caches, immutable tool/store reads, canonical live daemon socket, TLS settings and bootstrap executables |
| mbx       | Cargo convention and `mbx` resolved at the pinned revision in the canonical job environment | Shared mbx store access and canonical Cargo routing; no build-state path (below)                                    |

A Nix/devenv convention does not activate a shell. Projects that want devenv activation use their own `.envrc` with
direnv's `use devenv`. There is no built-in devenv shell backend or `[devenv]` configuration. No detector recognizes a
repository name, a managed-repository marker or a project-specific compiler cache.

Cargo build-state discovery enumerates git-tracked `Cargo.toml` files, excluding vendor trees, and asks Cargo for each
distinct workspace root rather than treating members as separate workspaces. A repository can contain several
independent Cargo workspaces, so a root-only detector misses build state.

Go likewise detects any git-tracked `go.mod` in the selected tree; its directory override narrows that snapshot.
**Why**: monorepos keep Go modules nested under `packages/`. A root-only detector missed those modules and broke
sandboxed Go checks because the sandbox could not write Go's shared module cache. Cargo, Go and mbx share one lazy
tracked-manifest snapshot per detector loop, with no recursive walk and no extra Git query.

Cargo runs `cargo metadata --no-deps --offline --format-version 1` once per root exactly as a Cargo job of the workspace
runs: sandboxed in the executed-child profile, from the job environment, after the workspace shell's activation inside
the sandbox. A checkout whose toolchain comes from its dev environment has none on the bootstrap PATH, or a different
one, so discovery from the pre-activation environment asked a cargo no job ever runs. One job per selected workspace
shell answers a whole batch of lookups, so discovery pays one activation per shell, not one per manifest. An in-checkout
`target_directory` contributes its checkout-relative path only when the mbx capability is absent. A disabled cargo
capability skips discovery and mbx detection; its directory override narrows the tracked manifest scan. Offline lookup
failures, and an activation that ends before Cargo answers, are typed findings and skip that workspace, not the whole
mint. So is a job whose PATH holds no `cargo`, because no shell selects its directory or the one that does activates
none: cowshed links no toolchain (Bootstrap executables, below), so that batch is one finding naming its shell, or that
there is none, and every manifest it skips, never one Cargo failure per manifest. Discovery runs at mint and when
tracked Cargo input contents change. One `git ls-files` query supplies every tracked `Cargo.toml`, `.cargo/config*` and
`go.mod` path; BLAKE3 hashes their working-tree bytes, so unstaged edits refresh the build-state paths before the next
job. The fingerprint also folds in a discovery revision, so a cowshed that asks differently rediscovers instead of
keeping an earlier discovery's answer, findings included. Caller environment and untracked config are not fingerprint
inputs.

The job that answers a batch is one `bash` script. `bash`, and the `cargo`, `mktemp`, `rm` and `cat` the script runs,
are each the first executable of their name on the PATH the job holds, never a fixed `/bin` or `/usr/bin` path: NixOS
keeps only `/bin/sh` and `/usr/bin/env`, and a job whose shell supplies none of them reaches `bash`, `cat`, `mktemp` and
`rm` through core's bootstrap links (Bootstrap executables, below); `cargo` is the repository's shell's.

Cargo's contribution drops caller `CARGO_TARGET_DIR` so where a job writes depends on the tracked project files, not an
unrelated shell's override. The remaining unit-identity environment belongs to 16_build_volumes.md, "One Cargo
environment on every path". Forks inherit the snapshot and fixed build-state links without rediscovery. Nx contributes
the paths specified by 03_caches.md, "Nx cache"; Codegraph contributes each detected project's `.codegraph/` index as
build state through the fixed links of 16_build_volumes.md, "One link per checkout".

Bun also contributes metadata-only lookup beneath the host's `~/.bun/install/global/node_modules`, where `bun link`
registers developer packages. Resolving a workspace's package alias through that registration does not read or list the
registry, write its links, or grant the provider's contents: the resolved provider still requires its own explicit read
grant. This applies to ordinary jobs and Nx cache-carry probes alike. A real Seatbelt regression reads a granted
provider through its registration while refusing ungranted providers, registry payloads/listings and provider writes. A
detected JavaScript package manager (Bun, npm or pnpm) contributes the `node_modules/.cache` of every package it
installed: a tracked `package.json` beside a real `node_modules` directory, below the capability's selected directory
and not itself inside a `node_modules`. `node_modules/.cache/<tool>` of the package a tool runs in is the JavaScript
convention for per-tree tool state (find-cache-dir: babel, webpack, ava, stryker), so the capability names the
convention and never a tool. A tracked manifest nothing installed, such as a fixture's, gets nothing, so discovery never
creates a `node_modules` in a fixture. The fingerprint records each tracked `package.json` path with whether its package
is installed, never its bytes: a dependency edit does not change where any tool writes, and must not rediscover Cargo.
Why: such state is rewritten in place on every run, which in the source image fragments an image every session clone
shares (measured: lmao's trace sink, a SQLite database of 716 MB in one package of a consumer repository, now in its own
`.cache/lmao` declared as build state).

A project may keep its Nix files away from its root and reach them from its `.envrc`, for example with
`cd tooling/shell` and then `. envrc.sh`. The Nix convention therefore also holds when the `.envrc` chain reaches Nix.
That means a direnv `use flake`, `use nix` or `use devenv` (or the `use_*` function) in any file the chain sources, or a
`flake.nix`, `devenv.nix` or `devenv.yaml` in a directory the chain `cd`s into or sources from. The chain is read and
never run. Detection follows only literal `cd`, `.`, `source`, `source_env` and `source_env_if_exists` arguments, reads
each file once and at most 16 files in all, and never follows a path out of the workspace.

### mbx detection

mbx is a Cargo-only capability, not a host-installed-tool convention. The same activated canonical job environment that
answers Cargo metadata resolves `mbx`; the executable-identity proof, revision-skew refusal and `mbx-host-missing`
admission refusal are specified by 05_gateway.md, "Pool selection and revision". Discovery never borrows the
controller's PATH or a host installation to enable the capability. A non-Rust project gets no mbx environment, grants,
bootstrap executable or build-state change, even on a host whose scheduler uses mbx.

With mbx detected, the Cargo capability contributes no `BuildStatePath`: mbx owns target placement. The lifecycle of its
managed-target link, including fork and refresh, belongs to 16_build_volumes.md, "One link per checkout"; shared store
access belongs to 03_caches.md, "mbx". Without mbx, Cargo contributes each discovered in-checkout target path as build
state, including the ordinary `target`, through the existing contribution contract. Removing mbx therefore changes the
Cargo contribution rather than leaving two owners of the same path.

**Content freshness keeps mbx off.** A Cargo project whose tracked Cargo config sets
`[unstable] checksum-freshness = true` gets no mbx capability, even where mbx resolves. Cargo then adds
`-Zchecksum-hash-algorithm` to every rustc invocation, and the pinned mbx bypasses every invocation that carries it. A
managed target would trade the build image's seeded target for a store that never hits: measured 2026-10-27, 16 of 16
compilations bypassed with 0 store hits, and with the flag absent the same checkout restored 9 of 9. Detection reports a
finding that names the config file and key, never a refusal, and Cargo contributes its target paths as it does without
mbx. The capability turns on at the first discovery after the project drops content freshness; discovery hashes the
tracked `.cargo/config*` bytes, so that edit rediscovers.

mbx's managed placement requires no target override. A detected mbx project whose tracked Cargo config sets
`build.target-dir` or `build.build-dir`, or whose activated canonical environment exports `CARGO_TARGET_DIR`, produces a
typed finding and refuses job admission. The finding names the override and the remedy: remove it or disable the mbx
capability. The canonical export is inspected before the contribution drops caller target variables; a caller overlay
does not stand in for that export. A per-command `--target-dir` remains the caller's Cargo choice: that build writes
outside managed placement, only as far as its sandbox grants allow. Cowshed promises no managed target there. These are
mbx's documented placement exclusions
([When mbx leaves a target alone](https://mr-boxington.jdx.dev/managed-targets#when-mbx-leaves-a-target-alone)), not
paths for the Cargo capability to claim alongside mbx.

Detection refreshes with the canonical activation's input identity (11_shell.md, "Shell activation and process reuse")
as well as Cargo's discovery inputs, so a changed devenv pin or resolved mbx build cannot retain an earlier capability
answer. A resolved mbx whose pinned revision cannot be established refuses admission as the skew finding, exactly as a
mismatched pin does (05_gateway.md, "Pool selection and revision"); detection never fails open into a successful answer.
Detection does not run `mbx setup`, change shell configuration, create a target or start a build.

**Where the answer comes from.** The canonical job environment that resolves `mbx` is build-state discovery's own Cargo
job (16_build_volumes.md, "Refresh"): after the workspace shell's activation, before its first Cargo query, the job
names the `mbx` its PATH resolves when its `cargo` is mbx's Cargo shim (11_shell.md, "Canonical mbx environment"; an
`mbx` beside a plain `cargo` names nothing, or plain Cargo would write a real `target/` once the volume released it),
and discovery keeps that executable's realpath. The checkout's build volume records it beside the discovered paths
(`cowshed-build-state.json`, `mbx`), and the discovery fingerprint covers the canonical activation's key (11_shell.md,
"Inherited shells") as well as the tracked Cargo inputs, so a changed pin rediscovers. The mbx capability is active
exactly where that record names an mbx; it is a cache keyed by those inputs, never a second authority. Admission is
derived at every `exec` admission, after that refresh, from the recorded realpath and the host link's (05_gateway.md,
"Pool selection and revision"), so discovery itself, which must run to replace a stale answer, is never refused by one.

**Enforced by:** `cowshed-core`'s
`mbx_is_detected_from_the_recorded_resolution_and_contributes_the_canonical_environment`,
`a_project_mbx_is_admitted_only_as_the_host_pin_itself`, `the_job_names_the_mbx_its_path_resolves_before_its_answers`,
`the_job_names_mbx_only_when_its_cargo_is_mbx_s_shim`,
`tracked_independent_workspaces_contribute_once_and_honor_their_own_target_config` (its mbx arm) and
`a_target_mbx_owns_leaves_the_state_and_the_mbx_is_recorded`. The content-freshness, target-override and wrapper
refusals above, and the remaining tests listed under "Required evidence", must exist for Phase 2.

## One contribution contract

Each detector returns data through one `CapabilityContribution`:

- **Environment:** named actions `Own(value)`, `Default(value)`, `Append(line)` or `Unset`. Owned and unset variables
  cannot be changed by a caller overlay. Defaults preserve an explicit caller value; appended lines follow that value.
  Project activation still executes inside the child sandbox, not in the controller. A served workspace's host
  environment and supervisor children publish the same capability-owned values through this contribution (11_shell.md,
  "Canonical mbx environment"); shell entry never selects another Cargo environment.
- **Filesystem grants:** exact paths or subtrees, with read or read/write access. Grants pass through the same
  protected-path validation as the core sandbox. Detection never grants host credentials, binaries' parent homes, a
  sibling workspace or cowshed controller state.
- **Shared caches:** host cache directories (`SharedCache`), each with an access and an optional private-environment
  link. A detector names its tool's cache once, as a `SharedToolHome`: the variable that points a child at it, for a
  tool that reads one; the tool's own default beneath HOME (03_caches.md); and whether that whole directory is the cache
  or only named subdirectories are, with the root state files the tool writes beside them. `add_shared_tool_home`
  derives the owned variable, the cache directories, the split root's literal read and the state files' read-write
  literals from it; no second table owns cache permissions. The supervisor creates every cache directory before a child
  runs and links it from its private path when the tool finds it there rather than through a variable (Nix's XDG cache
  and state, Gradle's `caches` under a private `GRADLE_USER_HOME`, direnv's `source_url` store); the link replaces an
  empty private directory and never one that holds anything. The sandbox grants each as a subtree with its access and
  metadata-only ancestors, and refuses one that is HOME itself, lies outside HOME, or intersects a protected path or
  cowshed controller state. This HOME-relative validation applies to `SharedToolHome`, not to mbx's provisioned
  build-cache-volume grant (03_caches.md, "mbx"). A cache is read-write unless a host process executes what it holds:
  direnv's `$XDG_CACHE_HOME/direnv/cas`, which `source_url` trusts by name without rehashing and a host shell sources,
  is shared read-only, so a sandbox reuses what the host fetched and never plants what the host runs. Only detected
  capabilities' caches and main's `[caches] home` entries are shared.
- **Build state:** `BuildStatePath` pairs a normalized checkout-relative tool path with its normalized volume-relative
  destination (16_build_volumes.md). Overlapping contributions fail; identical ones coalesce. The storage implementation
  applies fixed relative links through the checkout's one `.cowshed/build` link. Besides the detectors, the repository
  contributes through the same contract: `.cowshed.toml` `[build] state` declares checkout paths, or patterns such as
  `packages/*/.cache/lmao` expanded over the tracked package layout, whose state no capability can detect, each held on
  the volume at `declared/<path>` (16_build_volumes.md, "Declared build state"). **Why**: some tool state has no
  convention a detector could read — a patch-development checkout of an upstream project and its multi-GiB incremental
  build tree is just an ignored directory, a per-package trace store just a cache directory — yet it is exactly what
  belongs on the build volume. A declaration overlapping a capability's build state, holding tracked source, or reaching
  outside the checkout is refused with its remedy.
- **Daemon isolation:** private directories to create and workspace-relative inherited state to discard at mint. The
  detector owns the convention-specific paths; the clone implementation only applies the returned paths.
- **Unix sockets:** canonical, individually admitted host service sockets. No wildcard socket grants or in-sandbox
  host-daemon fallback.
- **Bootstrap executables:** command-name-to-resolved-executable entries needed by a detected capability. Preparation
  installs exact private `.cowshed/tools/bin/<name>` links (read-only jobs use their exec-temp `tools/bin`) so an
  executable such as `npm-cli.js` remains reachable as `npm`. The link set is reconciled at preparation; removed
  capabilities leave no stale program links. The separate `.cowshed/bin` remains the workspace's shim directory. The
  core adds both bins and platform system directories; it does not borrow a workspace `.devenv/profile`, an entire login
  PATH or an entire Nix profile. A detector may resolve an individual bootstrap tool through its installation
  conventions. Store-resolved executables receive immutable Nix-store reads; that alone does not grant Nix caches or
  daemon access. The same read-only store grant follows any program link in the mode's `tools/bin` or the workspace's
  `.cowshed/bin` that targets the store, with or without a Nix project; a host with no store gets none. Independently of
  detected project capabilities, core contributes the installed `cowshed` executable as the `cowshed` command in the
  mode's private `tools/bin`. This bootstrap entry admits executables, never controller state: only the stable program
  link and its resolved executable receive literal read grants; ancestors receive metadata-only lookup. It permits no
  directory listing, adjacent controller data, write authority or controller-socket access, and does not relax the
  filesystem-grant prohibition above. Consuming-repository lint can therefore run `cowshed build-state --json` against
  its checkout's build-volume record without duplicating its reader. The contribution merge records core ownership of
  the program name; a conflicting detector contribution returns a typed conflict naming core and the detector, never a
  missing-owner panic.

  Core also contributes one `bash` the same way a detector resolves its tool: the first executable `bash` in the host's
  conventional installation directories (the user's and system Nix profiles, `/run/current-system/sw/bin`, Homebrew,
  `/usr/local/bin`, `/usr/bin`, `/bin`), linked as `bash` in the mode's private `tools/bin`, with literal reads of the
  link lookups and the resolved executable and, for a store-resolved bash, the immutable store read above. Scripts name
  their shell `#!/usr/bin/env bash`, and a job without shell activation runs on the bootstrap PATH alone, whose fixed
  system directories hold no bash on NixOS; with this link `env` finds one on every host. The directory it was found in
  never joins PATH. The runtime never parses a script's interpreter line or rewrites argv0: the kernel runs the
  interpreter, so `$0` and `BASH_SOURCE` stay the script's. Core resolves `cat`, `mktemp` and `rm` the same way, for the
  Cargo discovery driver (Detection inputs): without them, a shell that supplies no coreutils on a host whose fixed
  system directories hold none ends the job before `cargo` starts.

  Reconciliation reads the owned-name manifest and checks every requested link; it never scans unrelated entries. A
  clean manifest and matching links require no write or durability barrier. Before repairing a set, preparation durably
  publishes the union of old and requested names as pending, then repairs the links, syncs their directory, and only
  then durably publishes the clean requested set. An interrupted repair therefore remains pending even when every link
  already matches on the next call; that call completes the barrier before declaring the set clean. Existing unmarked
  manifests take this repair path once. The private NUL-separated manifest uses one leading empty field for pending and
  two for clean; no job can write these bootstrap directories. Link replacement remains atomic and anchored to the held
  parent, and all mutations retain their power-loss barriers.

  When the registered `cowshed` multicall executable owns the exec host, detected `bun`, `npm` and `pnpm` entries use
  that same staged executable as their install wrapper, linked in the mode's private `tools/manager-bin` rather than
  `tools/bin`. Their actual capability-resolved executables stay under the reconciled `tools/real-bin`; no route is
  writable by job code. The bootstrap PATH lists `tools/manager-bin` just ahead of `tools/bin`, and after shell
  activation `tools/manager-bin` alone leads PATH; every other bootstrap link keeps its bootstrap position behind the
  activated shell (04_sandbox.md, "direnv trust is path-keyed"). The toolchain is not among what leads, nor among what
  is linked at all: the repository declares its toolchain through its own shell (Cargo build-state discovery above), so
  the cargo capability links no `cargo`, `rustc`, `rustdoc` or `rustup`, and the toolchain a repository's shell declares
  is the one its jobs and its Nx daemon run. The package managers follow the same rule: every invocation, install or
  not, runs the first executable of its name on the PATH it was given that is not the wrapper itself, recognized by file
  identity so no spelling of its route makes it run itself. After activation that is the shell's own, the program the
  host route runs; only when PATH names none, as before activation or in a job without a shell, does it run the
  capability-resolved program under `tools/real-bin` beside the PATH directory that reached the wrapper. **Why**: a
  bootstrap program is whatever the host's profile pinned. Measured 2026-10-21: the wrapper ran the home-manager
  profile's Bun 1.4.2 in a repository whose `packageManager` and devenv shell are Bun 1.4.3, and 1.4.2 truncates the
  stdout of a `console.log` followed by exit at 768 KiB where 1.4.3 delivers it whole, so a sandboxed test did not test
  what the gate tests.

  The wrapper knows itself and its workspace from its own executable, never from the environment: every
  `tools/manager-bin` link names the exec host the supervisor staged at `<workspace>/.cowshed/shell-host/<file>`, which
  no job can write, so the staged file's identity is the wrapper's and its location names the workspace. A job's
  environment is the job's to replace. **Why**: a test that spawns `bun` with only `PATH` and `HOME` still finds the
  wrapper first on that PATH; measured 2026-10-21, when the wrapper read its route and workspace from variables, every
  such child exited 126 ("managed package manager has no wrapper route"), 91 in one nx-plugin suite, where the host
  route runs the same command.

  When the root `package.json` declares `packageManager` as `<name>@<version>` for the invoked manager (an integrity
  suffix after `+` is not part of the version), the wrapper compares the version that program prints for `--version`
  before running anything, and refuses a different one with exit 126, naming the manifest, the declaration, and the
  program with the version it reported. A manifest that declares another manager, or none, is not compared; one that
  cannot be read or parsed, or a declaration that is not `<name>@<version>`, refuses. The exec host asks the program its
  activated PATH names once per activation and exports the answer as `COWSHED_PACKAGE_MANAGER_REPORTED`,
  `<version> <dev>:<ino>` of the file that answered; the wrapper takes that answer for the same file and asks any other
  file itself, so the commands of a warm shell start no extra process, and a child that dropped the answer is asked
  again.

  A first-argument install verb is covered immediately: Bun `install`, `i`, `add`, `remove`, `update`; npm `install`,
  `i`, `ci`, `add`, `uninstall`, `remove`, `update`; pnpm `install`, `i`, `add`, `remove`, `update`. Other invocations
  execute that program directly, without guessing another CLI or changing explicit relinks. The wrapper snapshots live
  developer package links and their executable aliases before the install, and restores them before returning its actual
  exit or signal, so `bun install && nx ...` uses those links in one job. A link the declaration made and no longer
  names, one whose text still matches its `node_modules/.smoo-declared-links` receipt while the root manifest's
  `smoo.developerLinks` no longer names its package, is the declaration's and not a developer link
  (packages/cli/README.md, "Declared Links"): no snapshot captures it, so neither the wrapper nor terminal cleanup
  restores it, the install leaves the version it installs, and the wrapper names it on stderr.

  The supervisor uses the same snapshot/restore implementation at job admission and terminal cleanup, including failed
  or killed installs. Terminal cleanup has two distinct failures, each labelled in the sealed job's stderr: a link
  restoration that fails is `developer-link restoration failed`, and a marker that cannot be removed is
  `install observation cleanup failed`. Either one, or both together, leaves the job in the failed state, and the
  waiters receive one error that names every cause that occurred. A blocking cleanup task failure is instead labelled
  `terminal cleanup task failed`: it proves neither filesystem cause. These failures are not findings: only a link
  absent with no observed install is one. Explicit absolute host/devenv executable paths, invocations whose first
  argument is not one of those verbs, and unregistered library-only one-shot hosts bypass immediate wrapping and get
  terminal cleanup only; chained-command preservation is not promised for those bypasses. No snapshot taken by a job or
  wrapper grants a new target. Only the mint captures canonical sibling read grants from its source. Terminal cleanup
  repairs developer-link registry replacements, and a link an observed install removed, are restored to their snapshot.
  The wrapper records an install in a per-job marker the supervisor creates before launch and removes at cleanup. A
  different live developer binding is preserved, with no new grant or attestation. A link absent with no observed
  install produces a `developer-link-ambiguous` finding in the job's stderr and stays absent. Restoration plans every
  leaf before changing any link. Adjacent siblings in the captured path order share one no-follow parent-directory
  capability during that planning pass; a new parent is validated and opened anew, and no directory observation survives
  the call. This reuses the snapshot's order, not a second inventory of links. Mutation still opens its parents
  independently and syncs each changed parent once.

- **HOME probes:** the sandboxed supervisor repeats detection beneath the HOME-wide read deny (04_sandbox.md), so every
  HOME path a detector inspects is also one it grants read: each bootstrap candidate beneath HOME as its literal program
  path and rustup's settings file. The sandboxed search then sees what the host's saw. These grants are never HOME
  itself and never a directory listing; their ancestors beneath HOME are metadata-only.
- **Shell activation:** an optional contained direnv directory. Absence leaves the ordinary sandbox environment and
  supports argv and script jobs. The shell entry owns its project root: the checkout's own git top level, derived from
  the working directory or the script's directory, never `DEVENV_ROOT`. A `DEVENV_ROOT` naming another root is refused
  with a typed finding that names both roots and a hint; it grants nothing and selects nothing. A checkout without its
  repository manifest is typed unavailable before any directory change.

HOME-relative shared caches stay at each tool's own default, so host checkouts and clones reach the same bytes through
identical path spellings with no host-side relocation: a sandboxed `bun install` extracts into `~/.bun/install/cache`,
the path every checkout's `node_modules/.bun` links name, as Cargo's registry and Go's `GOCACHE` land at their own
defaults. mbx's provisioned shared store is specified in 03_caches.md, "mbx". Cache declarations live with their
detectors; host preparation does not enable a capability in a repository.

A shared cache's ancestors get metadata-only access after the HOME read deny. Cargo/libgit2 canonicalizes a newly
fetched Git cache path; a writable cache subtree is insufficient when its parents cannot be inspected. This authority
comes from the shared cache itself, never an unrelated daemon socket, and does not allow listing or reading HOME.

## Ordering and conflicts

The registry has a stable order: direnv, Nx, cargo, mbx, Go, Bun, npm, pnpm, uv, Zig, Gradle, Nix. Convention detection
is side-effect free. Contributions are merged before any directory is prepared or child is launched.

Cowshed core reserves HOME, XDG roots, TMPDIR, PATH, workspace token/port variables, gateway routing, isolated Git
identity and controller-owned Git configuration. A detector attempting to own a reserved variable fails. Two detectors
contributing different actions or values to the same variable fail with both capability names and the variable;
byte-identical contributions coalesce. Identical grants, shared caches, bootstrap entries and isolation paths coalesce.
A read/write subtree subsumes a read grant only within the same validated path. Conflicting link targets fail rather
than depending on order. Capability grants never override immutable denies. Read-only jobs keep source files read-only
while sharing the checkout's writable build volume and Nx daemon/socket, not a private second Nx state.

## Explicit overrides

`.cowshed.toml` retains storage and land configuration. `[capabilities.<detector>]` may set `disabled = true` or
`directory = "relative/project"`. A directory is workspace-relative, normalized, contained, and inspected for that
detector's convention. An override never activates an absent convention and never substitutes repository identity for
detection. Unknown capability names, keys and invalid paths fail explicitly. No configuration is required for the
conventions above or for a plain repository. `[build] state` is not an override: it adds build state beside what the
detectors find and never changes a detector's contribution.

Directory overrides also root repository-relative contribution paths at that directory: for example Nx's workspace root
and inherited `.nx` rendezvous directory follow the selected Nx directory. Private environment state remains scoped to
the whole sandbox. Credential and controller-state denies remain unconditional core policy even when the corresponding
tool capability is absent.

`[caches] home = ["<path relative to HOME>", …]` in main's `.cowshed.toml` declares caches a repository's own tooling
places beneath HOME that no detector names, such as a compiler plugin cache. It is trusted only from main, exactly like
`[sandbox] deny`; a workspace's copy is the agent's to edit and is never read for it. Each entry is a non-empty path of
plain components and joins the merged contribution as a shared cache at `<host home>/<path>`, linked from
`<private HOME>/<path>`, so a tool resolving `$HOME/<path>` reaches the same bytes on the host and in every sandbox. An
entry that reaches a protected path is refused like any shared cache. A repository that declares none shares nothing
beyond its detectors' caches.

## Required evidence

Every detector has a filesystem-backed test proving its convention enables its contributions and removing that
convention removes them. Composite tests cover conflicts, containment, absent shared caches and overrides that cannot
enable missing conventions. The generic-repository integration exercises adopt, new, exec and land in a plain Git
repository without `.envrc`, Nix conventions or managed-repository files. Release dogfood runs the installed release
build against distinct adopted repositories; host setup is broadcast before restarting the gateway and never raises
unattended authorization prompts.

For Phase 2, the following cowshed-owned tests must exist:

- **`mbx_detection_uses_the_canonical_cargo_environment`** resolves the pinned mbx after the same shell activation as
  Cargo metadata, with a conflicting host PATH unable to enable or replace it; an unprovable revision is a finding.
- **`mbx_detection_refreshes_with_the_devenv_pin`** changes activation inputs, adds/removes the canonical mbx and
  changes its revision without changing a manifest, and observes a new contribution rather than a stale snapshot.
- **`mbx_changes_only_cargos_build_state_contribution`** checks that a Cargo project without mbx contributes its
  discovered target paths, while one with mbx contributes none; registry/git/trust contributions survive both cases.
  Independent Cargo workspace roots and directory overrides remain covered by the existing discovery boundary.
- **`mbx_target_overrides_are_explicit`** covers tracked `build.target-dir` and `build.build-dir`, an activated
  `CARGO_TARGET_DIR`, the typed finding/admission refusal and both remedies; it distinguishes caller overlays and
  per-command `--target-dir` from canonical activation, without claiming the explicit target is managed.
- **`non_rust_projects_never_receive_mbx`** supplies a host mbx and a non-Cargo project environment containing mbx, and
  observes no mbx capability, environment, grants or target paths.
- **`mbx_uses_the_one_contribution_contract`** checks environment ownership, exact shared-store grants, conflict and
  containment refusals, and the identical host-file and child projection; it adds no separate detection or grant table.

These tests prove cowshed's detection and mapping, not mbx's cache or scheduler internals. The real wiring smoke and
revision-skew admission tests belong to 05_gateway.md, "Required scheduling evidence", and 11_shell.md, "Canonical mbx
environment".
