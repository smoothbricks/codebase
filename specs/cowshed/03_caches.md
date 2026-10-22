# Cache Architecture

Caches are classified by **what the tool does with the cache**, not by tool and not by concurrency safety. The
classification decides where bytes live, what is shared, and what each sandbox may write. Main and every session use
identical wiring — there is one cache reality, carried by files and host-level paths (not environment variables; see
Wiring).

## The discriminator

Nearly every cache in scope is content-addressed with immutable entries — bun's install cache is the same class of
object as cargo's registry. Concurrency safety therefore discriminates nothing; what matters is the cache's role at use
time:

- **Read-at-build caches** — cargo registry, Go module + build caches, uv, sccache, zig global cache, gradle. The tool
  reads sources or artifacts from the cache and writes its output somewhere else. The cache is only ever read at build
  time, so sharing it costs nothing. These stay where the tool keeps them in the host user's HOME.
- **Link-target caches** — bun with its isolated linker (`[install] linker = "isolated"`). `bun install` extracts each
  package once into `<cache>/links/<name>@<version>-<hash>` and writes `node_modules/.bun/<name>@<version>` as an
  absolute symlink to it, so the cache's path is part of every checkout's `node_modules`. These stay in the host HOME
  too, and every checkout reaches them through **one literal path**: the tool's own default (Wiring, tool homes).
- **Clone-materializing caches** — a tool that materializes the workspace by _cloning out of its cache_: the cache is
  the reflink source. APFS clonefile is strictly same-volume, so cache placement decides whether install runs at
  clonefile speed or copyfile speed. Such a cache must live **reflink-reachable from the workspace** — on APFS, inside
  the workspace image.

Why placement decides it for a clone-materializing cache (measured with bun's clonefile backend, so it is not
re-litigated — bun 1.3.14, warm caches, lockfile pinned, 83 packages / 1,579 files / 59 MB `node_modules`,
`rm -rf node_modules` + reinstall ×3): **in-volume cache: 0.03 s and 480 KiB of volume space consumed** (99.2% of blocks
shared with the cache — clonefile confirmed); **cross-volume cache: 0.18 s (6×) and a full 59 MB copy, zero sharing**.
For delta installs the two placements are nearly a wash — the gateway mirror already dedupes the download. The deciding
case is **full materialization**: a wiped `node_modules`, a big lockfile churn, a fresh adopt — at large-repository
scale (~90k objects, GBs) the measured 6× ratio lands in the seconds-vs-tens-of-seconds band.

Why the link-target cache is shared rather than in-image: the links in `node_modules/.bun` name the cache by absolute
path, so they are byte-identical in main and every clone only if every checkout's bun uses the same cache path. A
per-image cache cannot give that. Main's image and each clone mount at their own paths, so a clone's inherited links
name main's cache — a sibling mount the sandbox denies — until its own `bun install` relinks every package into a
private copy of the cache the clone carries; `node_modules` then differs from main's in every workspace. Linking needs
no reflink, so the cache's volume never enters an install's cost.

## The three layers

| Layer                                   | Contents                                                                                                                                                                | Location                                                                                                                     | Sharing                                   |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| 1. Gateway mirrors                      | npm metadata and tarballs, bare repository mirrors                                                                                                                      | cowshed's own user cache directory: `~/Library/Caches/dev.cowshed/{mirror,repo-mirrors}` (Linux `$XDG_CACHE_HOME/cowshed/…`) | Global, written only by cowshed-gateway   |
| 2. Clone-materializing caches           | caches a tool reflinks out of into the workspace — **none in scope**: bun's isolated linker links into its cache instead                                                | Inside each workspace image                                                                                                  | Inherited from main via CoW at clone time |
| 3. Read-at-build and link-target caches | Cargo registry/git extraction caches, bun/npm/pnpm install caches, uv cache, Go module + build caches, sccache, zig global cache, gradle, Nix fetcher and profile state | Each tool's own default location in the host user's HOME, shared into the sandboxes of projects that use the tool            | Shared writable by detecting workspaces   |

Layer 1 removes duplicate _downloads_ (and stores compressed bytes once, ever). Bare repository mirrors live only at
`<cowshed cache dir>/repo-mirrors/<host>/<path>.git`; they are written by the gateway's `repo mirror` control-plane verb
and, like the rest of cowshed's cache directory, are controller state no sandbox reads or writes. Cargo's `~/.cargo/git`
is **not** that mirror tree: it is cargo's own shared writable extraction/index cache. The two have distinct ownership,
permissions, and paths, so a Cargo process can never mutate gateway repository mirrors. Layer 2 is one special case, not
a category: it exists exactly where a tool reflinks out of its cache and the substrate cannot reflink across volume
boundaries, and no cache in scope does that. Layer 3 caches are read at build time or linked to, and write nowhere near
the workspace, so sharing them is free.

**ZFS may widen layer 2**: OpenZFS 2.2 block cloning (BRT) works across datasets within a pool — something APFS
clonefile cannot do across volumes. A clone-materializing cache whose copy path goes through `copy_file_range` (which
BRT intercepts) would get reflink speed from a pool-shared location, so on ZFS layer 2 could share with zero duplication
and no speed penalty. Bun's Linux _default_ backend is hardlink, which cannot cross datasets; its isolated linker needs
neither.

**Workspace-keyed state** — `target/`, materialized `node_modules`, `DerivedData`, `.nx`, `.zig-cache`, Metro/Expo
caches — is not shareable concurrently under any mechanism. A build tool's per-tree incremental state that capability
detection names (Cargo target directories, Nx's cache and task database) lives on the workspace's build volume, cloned
from main's seed at fork and adopted by main at land (16_build_volumes.md). The rest stays in the workspace image, warm
because the image was cloned from main.

For a managed Bun checkout, the successful installer records each present `bun.lock`/`bun.lockb` SHA-256 in its existing
`node_modules/.smoo-install` receipt, beside the installer-input digest. The receipt is written only after success and
travels with `node_modules`; a receipt without lock digests is not current. `cowshed doctor` reads the checkout's locks
and receipt and reports `node-modules-install-missing` or `node-modules-lock-drift`, without installing or changing
files.

Before managed setup records a successful Bun installation, it checks the installed isolated-store links and their
immediate package aliases, including transitive aliases inside shared cache entries. Every observed package alias must
reach a directory with a readable package manifest. A refusal names the actual alias path, its expected target and the
metadata operation and cause; no new successful installation receipt is written. The check neither mutates shared cache
entries nor retries, purges or forces an install. Local setup retains its documented degraded shell; CI fails. Both
receipt-writing setup paths use this check. The CLI update command installs without writing this receipt and is not
covered by this receipt-admission check.

This is link and package-manifest admission at the installation boundary, not verification of every published payload
file or a guarantee against later modification of a shared cache entry. Registry tarball integrity remains the package
manager's responsibility.

**Open question — reuse after installation.** A matching install stamp and devenv's task status do not revalidate the
shared dependency targets. Extending admission to that path requires a separate contract decision and comparable
measurements of unchanged shell-entry cost and validation coverage; it is not part of receipt-writing admission.

## Explicit missing-payload repair

`bun tooling/direnv/setup-environment.ts --repair-missing-payload <installed-path>` is an explicit transaction in the
same canonical installer and under the same `withInstallLock`. It is not normal shell-entry validation, a global payload
scan, a second installer, or Bun's upstream cache repair. The ordinary link/manifest admission and its
matching-stamp/status boundary above remain unchanged.

The request names a physically missing published file or directory through this checkout's installed package alias. The
installed package must be physically owned by the checkout; workspace/developer targets outside it are not replacement
candidates. Its manifest name/version must have one registry integrity binding in `bun.lock`. Bun reads the checkout's
registry configuration to resolve that exact version's `dist` metadata; the downloaded tar must match the locked
integrity and contain the same package name/version. An unreadable or authenticated tar download reports the actual
transport refusal; it does not fall back to another install route.

Require a valid prior successful-install witness whose lock hashes still match. This transaction proves one package, not
a new full-install input set: publish that **same witness**, preserving its bytes and input digest, only after the
payload proof. A stale input witness stays stale for ordinary installation to resolve; an absent, invalid or
lock-drifted prior witness refuses rather than minting a false successful installation.

Compare only that package's published payload. Any existing byte, kind or symlink-target mismatch refuses: this mode
never overwrites a differing existing published file. Build a sibling staging directory with only missing published
entries, then archive-copy the existing package over it to preserve its extra files, symlinks, modes, ownership and
modification times. Verify preservation and published completeness, and require unchanged present/absent lockfiles.
Publish with the native atomic directory exchange (`cowshed-os::fsio::rename_exchange`), never a missing or partially
populated live path. Verify the live postconditions and unchanged lock bytes before the existing successful-install
receipt is written. A repeated healthy request is idempotent and performs no install, force, purge or retry.

Repair failures **before the success receipt** preserve or restore the original live package. Keep the old directory
until that receipt commits, so failed postconditions or receipt writes can roll back the exchange. After commit,
reclamation is separate: a failure is reported loudly as `repair committed; retained staging path ...`, with no false
rollback or host-health claim. A failed rollback retains both directories and reports both causes. No new authority
ledger is created. The explicit command reports the real published-file/restored-file counts and its completeness and
lock-preservation proof; ordinary shell entry does not acquire this cost.

## The reflink-reachability rule

A clone-materializing cache must live where the substrate can reflink from it into the workspace — on APFS, the same
volume; on ZFS, the same pool (pending the BRT verification). Any placement that breaks reachability silently downgrades
materialization from clonefile to copyfile. This replaces the earlier, broader "no cache dependency outside the volume"
rule: read-at-build and link-target caches may live off-image freely, because nothing reflinks or hardlinks out of them.

## Wiring

Wiring is carried primarily by **files and host paths**. Files travel with the image through clone/fork/checkpoint and
cover processes cowshed never spawned (IDE terminals, launchd jobs, CI runner steps). The exception is standard generic
HTTP proxy variables, which are emitted by the workspace environment wiring because ordinary proxy-aware tools consume
them there; they contain the endpoint URL, whose userinfo is the workspace token (below).

- **Private-environment config files**, written when the workspace is minted (adopt/fork, and rekey/restore's fresh CA)
  and republished before every exec, so a moved environment is never served stale. They live in the workspace's private
  environment — `.cowshed/{home,config,cache}`, which a cowshed-spawned child gets as `HOME`, `XDG_CONFIG_HOME` and
  `XDG_CACHE_HOME` — never in a tracked file, so a rewrite never dirties `git status`. A read-only exec gets the same
  files under its exec-temp environment. Define `GATEWAY_HTTP` as `http://127.0.0.1:<portBlock.base>` on macOS and
  exactly `http://127.0.0.1:7644` on Linux. The Linux address is served by the trusted connector inside that workspace's
  private netns; package clients do not speak Unix sockets. No Linux `portBlock` or synthetic base exists.
  - **bun: no registry configuration at all.** Bun reads its registry from the repository's own configuration
    (`bunfig.toml`, `.npmrc`; the public registry by default) and reaches it like every other client: through the proxy
    variables (below), trusting the workspace CA through `NODE_EXTRA_CA_CERTS` (04_sandbox.md). Public npm HTTPS reads
    have an anonymous default grant (05_gateway.md "Egress modes"). No `$XDG_CONFIG_HOME/.bunfig.toml` is written: it
    would put the workspace token and a loopback registry URL in a file, and an earlier wiring did — so every publish
    removes that entry itself, never a link target. No bunfig names a cache directory either: bun's global install cache
    is the shared link-target cache every checkout reaches through the host's literal path (tool homes below), and an
    `[install.cache] dir` inside the checkout would write each checkout's own path into its `node_modules/.bun` links.
  - **cargo: no registry configuration at all.** Every sandbox of a cargo project (`Cargo.toml`, 15_capabilities.md)
    builds with one literal `CARGO_HOME` — the host's `~/.cargo`, whose `registry` and `git` are shared into the sandbox
    where they are (below) — because cargo fingerprints a registry or git dependency by its absolute source path under
    `CARGO_HOME`: the same crate reached through another path, even a symlink to the same bytes, recompiles it and
    everything above it. So there is no per-workspace `$CARGO_HOME/config.toml` to carry a crates.io source replacement,
    and `~/.cargo/config.toml` stays host-owned and denied. Cargo reaches crates.io through the proxy variables (below),
    intercepted, trusting the workspace trust bundle through `CARGO_HTTP_CAINFO` (04_sandbox.md): rustup's cargo on
    macOS verifies through SecureTransport, which ignores `SSL_CERT_FILE`, so cargo needs its own variable.
    `index.crates.io` and `static.crates.io` are project-standing egress grants. Downloads land once per host in the
    shared registry. Git dependencies resolve through the fetch mappings to local clones when their checkouts are
    granted, and otherwise through intercepted `github.com`. Cowshed links no toolchain: the repository's shell declares
    it (15_capabilities.md, "Bootstrap executables"). A host whose toolchain is rustup's lends it read-only to a shell
    that puts it on PATH: the proxies in `~/.cargo/bin`, and `~/.rustup`'s settings and toolchains through an owned
    `RUSTUP_HOME`; nothing a sandbox runs installs or updates a toolchain there.
  - No git remote config or credential helper is written into the image: git reaches the network through the proxy
    variables like every other client, fetch-only (05_gateway.md "Egress modes"), and the fetch routes rewrite a bound
    repository's URLs onto its local clone (02_workspaces.md "Remote code ingress").
  - **Go: no configuration file.** A Go project (`go.mod` or `go.work`) gets `GOMODCACHE` and `GOCACHE` naming the
    host's own Go defaults — `<host home>/go/pkg/mod` and the host user cache directory's `go-build`
    (`~/Library/Caches/go-build` on macOS, `~/.cache/go-build` on Linux) — shared, layer 3: a module downloads once per
    host, and the host's own `go` uses the same two directories without any configuration. `GOPATH`, and with it
    `go install`'s binaries, stays at Go's default under the private `HOME`, in the image and per workspace, never in
    the host HOME. The Go capability exports `GOPROXY=https://proxy.golang.org` with no `direct` fallback, so a module
    the public proxy does not serve fails the fetch and nothing connects to a VCS host directly;
    `GOSUMDB=sum.golang.org`; and `GOTOOLCHAIN=local`, so a `go.mod` `toolchain` line cannot make `go` download a
    toolchain. They are environment actions, not a GOENV file. Go reaches `proxy.golang.org` and `sum.golang.org`
    through the proxy variables (below) as **opaque** tunnels: Go on macOS verifies TLS with the platform verifier and
    never trusts the workspace CA. No Go client presents the workspace token to a gateway route: `cmd/go` attaches
    credentials (netrc, `GOAUTH`, URL userinfo) only to HTTPS URLs.
- **Generic proxy variables.** Workspace env wiring sets `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`, and `https_proxy` to
  `<GATEWAY_HTTP>` with the workspace token as its userinfo password (`http://cowshed:<token>@…`), and configures
  `NO_PROXY`/`no_proxy` only for the workspace's own local services. A job's environment names the job instead of
  `cowshed` as the userinfo username (`http://job-<job_id>:<token>@…`), which the gateway records for job-exact
  attribution (13_telemetry.md "Attribution tiers"); the username is attribution only and authenticates nothing. On
  Linux these variables therefore resolve to `http://127.0.0.1:7644`; on macOS they resolve to the workspace block base.
  Userinfo is the one channel standard clients (curl, libcurl so cargo, reqwest, Go) turn into
  `Proxy-Authorization: Basic` on the first CONNECT; the token authenticates against nothing but this workspace's own
  endpoint.
- **Trust bundle.** `.cowshed/ca-bundle.pem` in the private environment holds the platform roots followed by the
  workspace CA. The core points `GIT_SSL_CAINFO` and `SSL_CERT_FILE` at it; each detected capability adds its tool's own
  variable, such as cargo's `CARGO_HTTP_CAINFO` (04_sandbox.md).
- **In-image tool shims** at `.cowshed/bin/`, PATH-prepended by the same `.envrc` wiring (so they travel with every
  clone and cover every process spawned in the workspace, IDE terminals included). There is one shim: the **`xcrun`
  wrapper** — pure `exec /usr/bin/xcrun "$@"` passthrough for everything except the simulator-control verbs (`simctl`,
  `devicectl`), so toolchain calls (`xcrun clang`, `--show-sdk-path`) stay native-speed and unbreakable. Simulator verbs
  resolve their device target: **dev-local CoreSimulator is the default** (agents and automation never accidentally
  reach the personal session); personal-session devices appear as explicitly-named remote targets and route through the
  gateway's `/sim/` endpoint (05_gateway.md) under the `sim` grant axis (04_sandbox.md). Tools that hardcode
  `/usr/bin/xcrun` bypass the shim and degrade to dev-local simulators — the safe default.
- **Cargo reads its budget from its holder, never from a `cargo` of cowshed's** (2026-10-19, Danny: why need cargo
  wrappers, rather than configure it properly; tg#6787). `cargo` in the managed environment is the project toolchain's
  own; nothing installed answers to the name, and an install removes the `cargo` link earlier installs planted
  (05_gateway.md, "The installed cowshed"). A CPU grant reaches Cargo as the variables Cargo already reads, exported by
  the process that holds the grant: `CARGO_MAKEFLAGS` and `MAKEFLAGS` naming the grant's make jobserver fifo, and
  `RUST_TEST_THREADS` and `NEXTEST_TEST_THREADS` set to its count where unset (05_gateway.md, "Cargo reads its holder's
  jobserver"). A Cargo no holder started — a host shell's, rust-analyzer's — runs exactly as Cargo does unconfigured.
- **Tool homes stay where the tool keeps them.** Every layer-3 cache lives at its tool's own default location in the
  host user's HOME, the one place the host's own tools already use without configuration, and cowshed shares that exact
  path into the sandbox of every project whose detector names the tool (15_capabilities.md). There is no cache volume,
  no relocation and no link: the bytes a sandbox writes are the bytes the host's own `cargo`, `bun` or `go` reads next.

  | Detector | Shared host paths (macOS; Linux in parentheses)                                            |
  | -------- | ------------------------------------------------------------------------------------------ |
  | cargo    | `~/.cargo/registry`, `~/.cargo/git`                                                        |
  | Bun      | `~/.bun/install/cache`                                                                     |
  | npm      | `~/.npm`                                                                                   |
  | pnpm     | `~/Library/pnpm/store` (`~/.local/share/pnpm/store`)                                       |
  | uv       | `~/.cache/uv`                                                                              |
  | Zig      | `~/.cache/zig`                                                                             |
  | Gradle   | `~/.gradle/caches`                                                                         |
  | Nix      | `~/.cache/nix`, `~/.local/state/nix`                                                       |
  | Go       | `~/go/pkg/mod`, `~/Library/Caches/go-build` (`~/.cache/go-build`)                          |
  | sccache  | none: `~/Library/Caches/Mozilla.sccache` (`~/.cache/sccache`) is daemon-write-only (below) |
  | direnv   | `~/.cache/direnv/cas`, read-only: `source_url`'s store, which host shells execute          |

  The HOME read deny (04_sandbox.md) stays the default; each detector's contribution carves back exactly its tool's
  paths after it: the cache directories read-write, the tool's root and that root's ancestors as literal reads with
  metadata-only ancestors, and cargo's root state files (the package-cache locks and the `.global-cache` usage database
  with its journal) as read-write literals. Nothing else in a host tool home is granted: configuration, credentials and
  binaries there stay denied. The supervisor creates every contributed cache directory before a child runs, because a
  child granted writes inside one cannot create its parent. Each child is pointed at the host path itself —
  `CARGO_HOME=<host home>/.cargo`, `BUN_INSTALL_CACHE_DIR=<host home>/.bun/install/cache`,
  `UV_CACHE_DIR=<host home>/.cache/uv`, `GOMODCACHE`, `GOCACHE` and the rest — and a caller's own value for these
  variables never reaches it.

  Restoration classifies a developer's package links against these same roots, and grants nothing for them: the
  controller derives each root from the host home (`BUN_HOME` and `PNPM_HOME` through `host_path`), and a job's wrapper
  reads the variables the sandbox was given. A link into a root is the installer's own entry, and a link to a real
  directory outside every root, such as a sibling checkout's package at `~/Dev/pnpm/store/pkg`, stays a developer
  binding. An unset variable leaves its root inactive, and a present root that is empty or relative is refused.

  **Repository-placed caches.** A cache that no detector names — one a repository's own tooling places, such as a
  TypeScript compiler plugin cache or a prebuilt runtime store — is declared by the repository in main's
  `.cowshed.toml`, trusted only from main exactly like `[sandbox] deny`:
  `[caches] home = ["<path relative to HOME>", …]`. Each entry shares `<host home>/<path>` read-write into every sandbox
  of the project and links `<private HOME>/<path>` to it, so a tool that resolves `$HOME/<path>` reaches the same bytes
  on the host and in every sandbox. Entries are plain relative paths of normal components, created by the supervisor
  like a detector's, and refused when they reach into a hard deny (credentials, cowshed's controller state, another
  workspace). Nothing is shared for a repository that declares none.

  **Retiring the caches volume.** Earlier releases kept these caches on a dedicated `cowshed.caches` volume (a
  `<pool>/cowshed/caches` dataset on ZFS) at `/private/cowshed/caches` and linked the host's tool paths into it.
  `cowshed setup` retires it, idempotently, and `doctor` reports `caches-volume` — naming the command that finishes the
  retirement — until it is gone. A plain `cowshed setup` runs steps 1–3, which need no authorization, and never
  escalates for the retirement; the volume is deleted only inside an attended run,
  `cowshed setup --retire-caches-volume`, which runs step 4:

  1. With the gateway and the sccache daemon stopped, each host link into the volume is replaced by the tool's own
     directory: the linked bytes move back to the host path — a copy that keeps symlinks, modes, times and hard links
     (`ditto` on macOS, `cp -a` on Linux) into a staging directory beside the host path, published with one rename, then
     removed from the volume. Cargo's two move under cargo's own package-cache locks; a cargo process holding them
     refuses the run. bun and uv offer no host-wide lock, so their caches move only while no install runs.
  2. A cache that exists in both places is merged, never kept twice: for a content-addressed cache (cargo, bun, npm,
     pnpm, uv, Zig, Gradle, Go, sccache) every entry the host lacks moves in and every entry it already holds is dropped
     from the volume, because the same key names the same bytes. Nix's fetcher and state caches are not
     content-addressed; the host's copy wins for the old canonical client cache. The historically nested `nix/nix` tree
     can hold unique mutable SQLite state and Git packs: it is never host-wins-dropped or merged into the active client
     cache. Its entire tree is retained under `~/.cache/nix/cowshed-retired/nix-nix`, with its moved bytes and retained
     destination reported. An existing retained copy is reconciled only by byte equality; different bytes refuse without
     overwriting or dropping either copy.
  3. Directories the volume held for no detector — the gateway's `mirror/` and `repo-mirrors/`, sccache's store,
     repository-placed caches such as `ttsc/` — move to their HOME location the same way: the gateway's to cowshed's own
     cache directory, sccache's to its default directory, a repository's to the `[caches] home` path that repository
     declares. Earlier Nix XDG-root wiring also placed Bun's install cache at `nix/.bun/install/cache` and Bun's runtime
     transpiler cache at `nix/bun`: those move to `~/.bun/install/cache` and `~/.cache/bun`, respectively, not into the
     Nix client cache.

     A legacy toolchain cache whose current owner instead declares checkout build state is placed with that existing
     build volume, never made into a new shared HOME cache merely to retire this volume. For `ttsc/`, the managed shell
     writes `TTSC_CACHE_DIR=<checkout>/.cache/ttsc` and `TTSC_GO_CACHE_DIR` beneath it; main already declares
     `.cache/ttsc` in `[build] state` (16_build_volumes.md, "Declared build state"). The operator explicitly selects
     that main and witnesses its existing fixed link before moving the legacy tree with the retirement executor. If
     several mains declare the same toolchain path, this is a deliberate choice of the main whose build volume will
     retain the old state, not a global name match: the other mains keep their independently declared caches. No
     detector, HOME-sharing declaration or configuration knob is added. A legacy tree with divergent mutable lock or
     usage bookkeeping is preserved whole at `<checkout>/.cache/ttsc/cowshed-retired/ttsc`, within that existing
     declared build state, rather than merged with the current cache; the retained bytes are reported. Writers of the
     source and destination must be quiet; bytes are preserved, never discarded as a substitute for placement.

  4. Under `--retire-caches-volume`, and only once steps 1–3 left the volume holding nothing but its marker, setup
     deletes the volume (or destroys the dataset), removes its `/etc/fstab` pin and drops it from the storage mount
     service, inside the one administrator authorization setup already uses. The flag refuses while anything else is
     left on the volume: whatever setup cannot place is named, with its size, so nothing is deleted that was not moved.

  Setup reports the bytes it moved and the bytes it dropped as duplicates, per cache. While anything is left it names
  each leftover with its size and states that `cowshed setup --retire-caches-volume` cannot run until those are placed;
  once only the marker remains it names that exact command as the attended next step. On home-manager, NixOS and
  nix-darwin hosts a module that still declares a link into the volume is a conflict naming that module option; setup
  never rewrites module-owned paths.

  **Sandboxed shells evaluate devenv in place, once per lineage.** devenv evaluates a project at its path, and the
  result really differs per path. The absolute root, its `.devenv`, `TMPDIR` and runtime directory are inputs to its
  evaluation-cache key, and its exported hook may embed each path. A clone's copy of its origin's `.devenv` therefore
  never answers for the clone, and no shared writable cache, however content-addressed, serves executable shell exports.
  What a clone does take is its origin's export carried in its own image (`.cowshed/shell-host/activation.json`,
  11_shell.md "Inherited shells"), and only within this boundary: keyed by the bytes of the activation's recorded inputs
  and by its baseline environment, both recomputed at the clone; taken only once its producer is certified, two
  checkouts' own evaluations of those bytes having agreed after the move; moved to the clone's root, runtime link and
  dotfile, with any value still naming the origin refused; never carrying `DEVENV_TASK_FILE`, a shell hook, a variable
  the secret convention names, or what `.cowshed/env` publishes per workspace, whose values the clone reads from its own
  `.cowshed/env` and `.envrc-local`; recomputable by evaluation and deletable at any time, a lost record costing one
  evaluation. It is a byproduct of an activation that already ran, in the workspace it ran in, not a second store.

  **Every checkout reaches a shared tool home through the host's literal path.** Cargo fingerprints a registry or git
  dependency by the absolute path of its source under `$CARGO_HOME` (measured: the same registry reached through a
  different `$CARGO_HOME` path, even a symlink to the same bytes, recompiles the dependency and everything built on it),
  and bun's isolated linker writes its cache's path into every `node_modules/.bun` link. A sandbox whose tools followed
  its private `HOME` would rebuild every dependency a clone's copied `target/` already holds and relink `node_modules`
  into a cache no other checkout has. That is why every sandboxed child of a detecting project is pointed at the host's
  own default path, with exactly the grants listed above.

  **A clone is warm only for the units its origin built.** Cargo keys a unit on its profile, and whether it compiles
  incrementally is part of that key. A `test` profile that differs from `dev` makes `cargo test` and `cargo build` two
  builds of every dependency, and a main warmed by one hands its clones nothing for the other; so `test` inherits `dev`,
  and `smoo monorepo check` asks nothing of it. Dependencies are never incremental and, through the one `$CARGO_HOME`,
  are path-identical in every checkout, so the compile cache shares them as they are; workspace crates stay incremental.
  For the same reason no cowshed-launched child — `land --check` included — sets `CARGO_INCREMENTAL`. Cargo itself turns
  incremental off for workspace crates whenever `CI` is set: its precedence is `CARGO_INCREMENTAL`, then
  `build.incremental`, then `CI`, and only then the profile's `incremental`, so no profile key outranks `CI`. A host
  shell exporting `CI` therefore builds different units for those crates than a sandbox child, which never inherits it,
  and main is warmed through `cowshed exec main --`.

  **The parent config directories stay denied.** `~/.cargo/config.toml`, `~/.cargo/config`, `~/.cargo/credentials.toml`,
  `~/.cargo/credentials` and `~/.gradle/gradle.properties` sit beside the shared caches and are on the secret deny list
  (04_sandbox.md); a sandbox building against the host `$CARGO_HOME` cannot read or write any of them. `~/.cargo/bin`
  holds rustup's proxies and no credentials: a cargo project reads it, never writes it (above). A Gradle project's
  `GRADLE_USER_HOME` is private (below), never the host's `~/.gradle`; `SCCACHE_DIR` reaches host shells and jobs
  through the same capability contribution, naming the daemon's own cache directory, which the sandbox can neither read
  nor write (below). Profile generation canonicalizes symlinked paths when emitting write grants (the `/var` →
  `/private/var` handling generalizes).

- **Environment variables.** Two sets, and neither is wiring a file could carry instead:
  - The in-image `.cowshed/env`, sourced by the workspace's `.envrc`, exports what processes cowshed never spawned need
    and no file can give them: on macOS the dev-server port conventions `COWSHED_PORT_BASE` and
    `COWSHED_PORT_BLOCK_SIZE` (04_sandbox.md, cooperative-sandboxing caveat), and in an Nx checkout the local Nx cache
    route's server and bearer, the bearer being the workspace token, because stock Nx takes it from nowhere else. It
    never exports the bare token as `COWSHED_WORKSPACE_TOKEN`. A served mount also publishes the sccache capability's
    client environment below, and Nx's `NX_MAX_CACHE_SIZE` default; explicit caller cache-size overrides are preserved.
    Linux exports no port values: services use private loopback and package/proxy wiring uses `http://127.0.0.1:7644`.
    No workspace-identity variable is exported; anything that needs identity derives it from cwd via
    `.cowshed/workspace.json` or asks the CLI.
  - The supervisor sets, for every job it spawns (`runtime/supervisor.rs` `sandbox_environment`):
    - the private environment: `HOME`, `XDG_{CONFIG,CACHE,DATA,STATE}_HOME`, `DIRENV_CONFIG`, `TMPDIR` (the workspace's
      temp dir in the project store, 04_sandbox.md) and `XDG_RUNTIME_DIR`;
    - git isolation: `GIT_CONFIG_GLOBAL`, `GIT_CONFIG_NOSYSTEM`, `GIT_ATTR_NOSYSTEM`, and the fetch-route include as
      `GIT_CONFIG_COUNT`/`KEY`/`VALUE` (02_workspaces.md);
    - each detected capability's contribution (15_capabilities.md): for an Nx project `NX_SOCKET_DIR`,
      `NX_WORKSPACE_DATA_DIRECTORY` and `NX_CACHE_DIRECTORY` (the checkout's own `.nx` for both read-write and read-only
      jobs; source remains read-only while the build volume stays writable); `NX_MAX_CACHE_SIZE` defaults to one tenth
      of configured `[build] capacity` (100 GiB by default), preserving explicit caller overrides; a caller's
      `NX_DAEMON` is withheld so Nx's own default decides (04_sandbox.md), and `NX_WORKSPACE_ROOT_PATH`; for a cargo
      project `CARGO_HOME` (above, naming the host's own default path only to undo the private `HOME`),
      `CARGO_NET_GIT_FETCH_WITH_CLI=true` and on a rustup host `RUSTUP_HOME`; for a Go project `GOMODCACHE` and
      `GOCACHE`; the other shared tool homes of detected package managers and toolchains (`BUN_INSTALL_CACHE_DIR`,
      `NPM_CONFIG_CACHE`, `PNPM_CONFIG_STORE_DIR`, `UV_CACHE_DIR`, `ZIG_GLOBAL_CACHE_DIR`), each naming the host's own
      path; and for a Gradle project a private `GRADLE_USER_HOME` under the private cache whose `caches` links to the
      host's `~/.gradle/caches`, so the daemon, wrapper distributions, native libraries and JDKs stay private and no
      other part of the host `~/.gradle` is granted;
    - the `.cowshed/env` set again (the port pair, and for an Nx project the cache route pair), `SCCACHE_SERVER_UDS` and
      `SCCACHE_DIR` (the host sccache daemon, below), and `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` in both cases, carrying
      the token as proxy userinfo. A caller's `COWSHED_WORKSPACE_TOKEN` is withheld, not forwarded;
    - the build wiring, also emitted into `.cowshed/env` from the same sccache capability: `SCCACHE_BASEDIR_CWD=1`,
      `SCCACHE_BASEDIR=<workspace root>`, and `RUSTC_WRAPPER` naming `bin/sccache` inside the store path the host's
      sccache GC root pins, the program the daemon itself runs (below). It is read through the root before every spawn
      and names the program rather than a `PATH` entry: shell activation owns `PATH`, and a repository shell that ships
      no sccache left a bare `sccache` unresolvable, failing every cargo at its version probe. A host that pinned none —
      sccache is opt-in — or whose pinned store path was collected gets no wrapper at all; no value is the caller's;
    - trust anchors as defaults a caller may override: the core's `GIT_SSL_CAINFO` and `SSL_CERT_FILE`, and each
      detected capability's own — `CARGO_HTTP_CAINFO`, `NODE_EXTRA_CA_CERTS`, `NIX_SSL_CERT_FILE` with an
      `ssl-cert-file` line appended to `NIX_CONFIG`, `UV_SYSTEM_CERTS=true` (04_sandbox.md);
    - the port-0 preload's loader options, each appended after the caller's own options with a space:
      `--require=<private environment>/port-zero.cjs` to `NODE_OPTIONS` and `--preload=<…>` to `BUN_OPTIONS`
      (04_sandbox.md, "A sandboxed job binds its listeners in its block");
    - the bootstrap `PATH` (04_sandbox.md).

    That list is the whole job contract; everything else a job sees is what the workspace's own `.envrc`/devenv exports
    and what the request's explicit `env` names. Nothing is read from the environment of the process that runs the
    supervisor: no locale, terminal, Xcode selection, login `PATH`, `NX_*`, `VIRTUAL_ENV` or `CARGO_HOME` from another
    shell reaches a job, and a tool a build needs from the host user's home is declared in the workspace devenv instead.

    No registry client receives the workspace token from a file: it travels only as proxy userinfo, and to Nx as the
    cache route's bearer. No job environment and no `.cowshed/env` carries it bare, and no file under the private
    `home`, `config` or `cache` carries it. Within one workspace a child inherits its parent's proxy variables, token
    included, by design; a fork's inherited shell export leaves the proxy variables and every token-bearing name out,
    and the fork's own supervisor sets its own. The cargo `[env]` guidance above mirrors `SCCACHE_SERVER_UDS` for cargo
    builds cowshed never spawned.

    **Paths under the checkout but outside the cwd.** sccache hashes every `CARGO_*` value and every env-dep value into
    a Rust key, and `SCCACHE_BASEDIR_CWD=1` normalizes only the request's cwd prefix. cargo runs a registry crate's
    rustc from its package directory, so the OUT_DIR its build script fills (`<checkout>/target/.../out`) is outside
    that cwd, and a crate that includes from it — serde and serde_core do — keyed and recorded the checkout, as did
    every crate built on it. A nested Cargo workspace (a vendored upstream checkout) has the same problem with
    `CARGO_HTTP_CAINFO` (`<checkout>/.cowshed/ca-bundle.pem`) and the devenv's `CARGO_INSTALL_ROOT`
    (`<checkout>/tooling/direnv/.devenv/state/cargo-install`): 0/192 cross-workspace hits measured. Neither path can
    take a stable spelling: the bundle's content is per workspace (the platform roots plus the workspace's own CA,
    04_sandbox.md), and a path spelled the same in every workspace but holding each workspace's content would need a
    per-process filesystem namespace, which macOS does not have. So every job also gets
    `SCCACHE_BASEDIR=<workspace root>`, the bundled sccache's per-request base (both hashers): it strips and remaps the
    workspace root as the cwd is, and at a workspace member's cwd it keys exactly as `SCCACHE_BASEDIR_CWD=1` alone. A
    C/C++ request under it runs with direct mode off. A build whose outputs are relative to a deeper root (cmake and
    ninja below a checkout) sets its own.

### The sccache daemon

sccache is served by a **host-owned daemon**: the `dev.cowshed.sccache` LaunchAgent runs the sccache binary itself as a
foreground unix-socket server outside every sandbox — `SCCACHE_START_SERVER=1` selects server mode,
`SCCACHE_NO_DAEMON=1` keeps it in the foreground under launchd supervision, `SCCACHE_IDLE_TIMEOUT=0` disables idle exit,
and its environment pins `SCCACHE_SERVER_UDS=/private/cowshed/store/sccache.sock` and `SCCACHE_DIR` to sccache's own
default cache directory — `~/Library/Caches/Mozilla.sccache` on macOS, `~/.cache/sccache` on Linux — (all
source-verified against sccache 0.16, which reads `SCCACHE_SERVER_UDS` in both client and server ahead of
`SCCACHE_SERVER_PORT`; the TCP port is the fallback wiring only on a platform without unix sockets, and then the
Seatbelt loopback-allow class in 04_sandbox.md applies). `cowshed sccache start|stop|status` install, remove, and probe
the agent; start is healthy when the socket answers. Every disk-cache read and write happens inside the daemon
(source-verified: sccache instantiates its disk cache only in the server process), so no sandbox is granted the store at
all — it is **daemon-only**, and the HOME read deny covers it. `cowshed setup` writes sccache's own client config with
the same directory and the daemon's size cap, so a store-less client that starts a server of its own neither forks a
second cache nor evicts the shared one down to sccache's 10 GiB default.

Host ownership is the boundary: a server a workspace spawned would inherit that workspace's Seatbelt profile and apply
it to every other client, while launchd starts this one outside every sandbox, so it enforces no workspace's boundary
and can serve them all. No client-side mode avoids a server: `SCCACHE_NO_DAEMON=1` only stops an auto-spawned server
from detaching, and a client still spawns one on connect failure (source-verified, sccache 0.16). Because no sccache
0.16 client flag suppresses that auto-spawn fallback, the sandbox provides the fail-fast: binding
`/private/cowshed/store/sccache.sock` needs write-create under `/private/cowshed/store`, which no workspace holds, so a
client whose daemon is down fails its compile promptly with a bind error instead of wedging — and can never stand up a
wrong-boundary server for siblings. The daemon is a trusted mediator in the nix-daemon sense, with its confused-deputy
surface named explicitly in 04_sandbox.md: the unsandboxed daemon reads sources and executes a client-named compiler at
a sandboxed client's request, accepted under the same threat model that already concedes layer-3 poisoning (below)
because it adds immediacy, not new reach, while the store's write surface strictly narrows.

### Nx accepted-task artifact service

**This cache is an optimization for cross-shed reuse and never blocks a build.** A refusal is a miss (`GET` 404) or
not-stored (`PUT` 403), and the task runs or keeps its own result (05_gateway.md "`/nx/v1/cache/{hash}`"). A cache that
cannot answer at all is a miss on the client too (05_gateway.md "The client half"). A shed's `PUT` is refused by
authority, not reachability. The store is single-user, so its write boundary guards against an unverified producer, not
a malicious writer: only a land whose gate verified the hashes publishes (05_gateway.md "Trust domain").

Nx's live database and local cache remain private to each checkout/build volume. Accepted task archives are a separate
host-owned store, not a shared `.nx` directory, blob CAS, or Cargo warm volume. Cowshed reuses the public
`@smoothbricks/nx-cache-server` Bun server and its stock `GET`/`PUT /v1/cache/{hash}` protocol. Each project has one
backend listener and one flat entries directory under cowshed's host cache directory; the gateway maps project identity
to that backend. A hash in one project cannot name another project's archive.

The gateway service owns the backend lifecycle, outside every workspace sandbox, using an installed copy of the
package's server bundle rather than a workspace's source files. The package ships that bundle as one
platform-independent JavaScript file, `dist/server/nx-cache-server.js`, and no compiled executables; cowshed bundles no
interpreter (tg#7504). Setup installs a content-addressed copy of the bundle and records where its invoker's `PATH`
finds `bun`, named as a devenv or nix profile and never as a store path (05_gateway.md), so that `bun` is the one the
gateway runs. One process exclusively owns each entries directory. Its configuration and labelled read/write bearer
principals are host-owned; no repository, workspace child environment, or sandbox receives a backend secret or
filesystem grant. The host-owned backend necessarily holds its own host credentials. Clients receive only their own
gateway authority (05_gateway.md). Normal iteration always reads accepted project results with read-only authority. Only
an accepted landing may publish its successful current task hashes, including existing local hits without rebuilding,
under temporary gateway write authority; exact-head/task-hash eligibility belongs to 05_gateway.md and
16_build_volumes.md. There is no scratch store. Read-only authority denies publication, not necessarily archive creation
or an attempted PUT by the stock client: measure that cost separately, and never describe a server-side 403 as zero
upload overhead.

The installed gateway starts the backend as `bun <installed bundle> --exit-on-stdin-eof`, `bun` resolved on that
recorded `PATH` at each start (none found is a start failure said as one), and retains the write end of its stdin pipe
for the child's lifetime. On hard parent death the kernel closes that pipe, and EOF drains and stops the backend. Normal
standalone invocation without the flag keeps signal supervision. EOF changes lifecycle only; it is not the
store-exclusion mechanism.

The backend opens `<project>/entries/.owner.lock` and takes a nonblocking exclusive kernel `flock` (`LOCK_EX | LOCK_NB`)
through a minimal Bun FFI binding to libc, before any staging reclamation. The lock file is persistent and is never
unlinked; no stale-PID or exclusive-create substitute exists. The backend holds the lease through listener, handler, and
publication drain until stop; hard process death releases it in the kernel. A busy lock is a typed store-locked startup
refusal before listening. Any other errno, or unavailable FFI, fails startup loudly with no fallback. Normal standalone
mode takes the same lease. A replacement backend cannot open or reclaim the store until its predecessor exits and
releases the lock.

The server treats archives as opaque binary bytes in Nx's native format: no unpacking, repacking, database import, or
whole-history copy. Publication streams into `.staging` on the same filesystem, verifies the declared positive byte
length, then atomically hard-links to the final task key without replacement. Concurrent same-key writers produce one
200 and conflicts produce 409, preserving the winner's whole archive. Different keys proceed independently. Read-only
PUT is 403; missing/evicted GET is 404; GET success is 200 with `application/octet-stream` as a standard chunked
complete binary body; a pinned GET stream uses chunked HTTP framing with archive bytes unchanged. Authentication failure
is 401 with exactly `text/plain`. Successful PUT requires a positive `Content-Length` and returns 200 (the native Nx
client and the current official Nx self-hosted caching OpenAPI both use 200). Failed or interrupted uploads never become
cache hits. Startup removes abandoned staging files and reports the count; graceful stop drains requests, and hard death
leaves only reclaimable staging.

Retention reuses filesystem SIEVE: insertion order is mtime, `atime > mtime` is the visited bit, clearing restores atime
to mtime, and a sibling `sieve.hand` persists the cursor. Mount access-clock behavior is not relied on; a successful GET
explicitly marks the opened inode's visited bit when clear so SIEVE does not degrade to FIFO, preserving its
modification-time cursor through `utimes`; it never retimes a path that could name a replacement inode. No shared live
cache database is introduced. Streaming stages proceed in parallel. Complete staged values enter one serialized
publication section: never-replace hard link, then SIEVE sweep, then successful completion. No publication interleaves a
sweep's listing and cap enforcement. A sweep also runs before listening. Each successful commit restores the configured
`capBytes` sum of published archive lengths before replying 200. `maxBytes` independently bounds one upload and cannot
exceed `capBytes`.

The sweep never evicts the archive its own commit just published. The protected just-linked key remains in used-byte
accounting, while other keys undergo SIEVE. Each PUT 200 still has its own archive immediately retrievable. There is no
capacity enlargement or bypass.

Before reading an authenticated writer's body, the server reserves its positive declared length from an independent
staging budget, also `capBytes`. If outstanding reservations would exceed that budget it refuses 403 before streaming;
there is no admission queue or retry service. The reservation covers streaming and completed uploads awaiting commit,
and is released after cleanup. Live archives and in-flight staging each have their own logical bound, not one combined
disk cap; filesystem allocation and metadata are measured separately. Startup reclaims abandoned staging. Eviction and
reclamation report bytes/counts and failures, never silent success. A failed staging cleanup retains its reservation and
reports the path/reason; it is not reusable budget. Restart reclaims those files before accepting requests and fails
closed if reclamation fails. This is ownership of retained bytes, not a background retry path. An evicted archive
remains an honest miss, and the gateway's post-adoption check requires all accepted cacheable hashes to hit
(16_build_volumes.md).

**Clearing.** `cowshed cache clear` (06_cli.md, "Cache clear") empties a project's store through the gateway, which owns
its backend. It refuses while the project's Nx work is in flight — an admitted or queued request, a land holding
publication authority, or a backend still starting — naming each workspace, and refuses a host-configured backend's
store, which is not the gateway's. Otherwise the gateway stops the backend it started (`SIGTERM`, as a drain does; one
still running after 10 s is killed and reaped), takes the store's lease itself (the same `flock` on
`entries/.owner.lock`, never unlinked) and deletes every archive (each file in `entries/` whose name is a task key) and
`sieve.hand`. Staging is left to the next backend's startup reclamation. A lease another process holds is a typed
store-locked refusal. Until the clear reports, the project's requests get the outage's miss or refusal and no backend
starts; the next request after it starts a fresh one. The store holds only what lands publish, so a cleared entry
returns only through another land's publication.

The package follows the repository's public-package release conventions (`npm:public`, generated conventional-commit
versions, normal build/release targets). Each consumer migration adopts the published dependency and deletes the
superseded source in the same change.

## What a land re-runs

A cached task's key is every input it declares: its files, its dependencies' files, its environment, runtime and
toolchain inputs. With a warm cache holding a valid entry for each key, a land gate runs only the tasks whose key the
landed commits (or a changed runtime or toolchain input) changed, plus the targets that state `"cache": false`, each for
a reason its configuration names; a re-gate of an unchanged tree runs those uncached targets alone. A cold, evicted or
unreachable entry runs its task again, as a cache degrade does (Nx accepted-task artifact service, above). Five rules
keep the key no wider than what a task observes, each learned from a measured miss (2026-10-22, tg#9675: twenty minutes
of gate for a one-line change).

- **A target split from a cached target states its own cache.** Nx caches a target only when its own configuration or a
  `targetDefaults` entry for its exact name says so; `targetDefaults.test` keys the target named `test` and no other.
  When nx-plugin's and cli's Bun suites became `test` aggregates over `test-shardN`, `test-host` and `test-shell-entry`
  (2026-10-02), those targets stated nothing and never cached again: 0 hits in 8-9 gates each, `nx-plugin:test-shard2`
  up to 849 s, on every land whatever it changed. Each states `"cache": true` with the inputs the unsharded `test` had,
  and `smoo monorepo validate` refuses any target a cached `nx:noop` aggregate runs from its own project that states no
  `cache`. It reads the aggregate's members through Nx's own `dependsOn` normalization, so `project:target`,
  `projects: [self]` and target patterns count and `^` edges do not.
- **Dependents key on a dependency's files without its tests.** `^default` is all of a dependency's files, its tests
  included: one nx-plugin test file (297acb6d6) re-ran six statebus suites and tooling's, which reach nx-plugin only
  through cli and cannot observe it. The named input `dependencyDefault` is `default` without `__tests__/`, `*.test.*`,
  `*.spec.*` and the Bun shard manifest `test-timings.json`; build, test and cargo-test name `^dependencyDefault`. A
  dependency's exported testing helpers (`src/testing.ts`) and runtime scripts still key its dependents.
- **TypeScript tasks key on Rust only through its outputs.** `production` names a project's TypeScript sources and no
  crate source: a TypeScript consumer of a Rust artifact depends on the producer target and keys on its outputs through
  `dependentTasksOutputFiles`, a target that reads crate source as text names it in its own inputs, and cargo targets
  key on `cargoClosure`. `smoo monorepo validate` refuses a `production` entry beneath `{projectRoot}/crates`, ending in
  `.rs` or naming a `Cargo.toml`, and `smoo monorepo update` removes it. While a consumer workspace's `production` named
  `{projectRoot}/crates/**/*`, one Rust integration test (`crates/<crate>/tests/*.rs`) re-keyed 237 TypeScript tasks,
  the lint, typecheck, test and tsc-js of about 31 projects, in one land (2026-10-23, tg#9767).
- **A gate's runners size their own test threads.** The gate's orchestrator holds one CPU token and its pool relays
  every runner's ask to the host, so it exports no `RUST_TEST_THREADS`/`NEXTEST_TEST_THREADS` (05_gateway.md, "The
  holder exports it"). While it exported 1, every Rust suite in every gate ran on one thread: `tokens=1/1` in each
  runner from 2026-10-20, and cowshed-cli's 487 tests summed to their 1225 s wall, the tail of a 24m55s gate for a
  three-file Rust change.
- **A suite that outlasts the rest of the gate is sharded.** A runner's grant never grows, so one long runner finishes
  on the tokens it started with while the host idles. cowshed-cli runs in eight hash partitions
  (`[package.metadata.smoothbricks.test] shards = 8`), as cowshed-core does: 492 tests, the longest runner 95 s.

Cargo's own reuse is not where the time goes: a TypeScript-only land compiled no Rust unit, and a cowshed-core change
compiled exactly the seven cowshed crates downstream of it. The targets that state `"cache": false` are `cargo-fetch`
and `cargo-test-compile` (the shared Cargo directories are no Nx output; nx-plugin README, "Cargo cache boundaries"),
`shard-timings`, `tooling:lint-disk-commands`, the ttsc and capability-probe warmups (their caches live outside Nx
outputs) and the packaging checks; they are what a re-gate re-runs.

## Convention table

Cache classification is a built-in table keyed by well-known names — never configuration:

| Name (at any depth, gitignored)                       | Class                         |
| ----------------------------------------------------- | ----------------------------- |
| `node_modules`                                        | workspace-keyed, in-image     |
| `target` (with `Cargo.toml` sibling)                  | workspace-keyed, build volume |
| `.nx`                                                 | workspace-keyed, build volume |
| `.turbo`, `.next`, `.expo`, `.gradle` (project-local) | workspace-keyed, in-image     |
| `.zig-cache`, `zig-cache`, `zig-out`                  | workspace-keyed, in-image     |
| `DerivedData` (project-local)                         | workspace-keyed, in-image     |
| `Pods`                                                | workspace-keyed, in-image     |
| `vendor` (with `go.mod` sibling)                      | workspace-keyed, in-image     |

The table exists so `cowshed doctor` can report cache health and so the Seatbelt baseline can include project-local
build dirs as writable without per-project config. Unknown build dirs are simply in-image files — nothing breaks; the
table is advisory metadata, not a gate.

## New-package flow

1. A workspace's `bun install` (or cargo, or `go mod download`) asks its registry — through the gateway's proxy endpoint
   on the workspace's own data-plane port (05_gateway.md) — for a package it lacks. Every registry host needs an egress
   grant (a project's standing grants name the registries its builds use); the port+token still identify and audit every
   request.
2. For npm, the gateway serves an eligible package request through its verified cache: it fetches the tarball upstream
   once (credentials injected where trusted policy admits them), stores it content-addressed in the gateway mirror, and
   serves it over loopback, so no WAN duplication across workspaces. Cargo and Go fetch from their registries as
   intercepted and opaque egress; the shared cargo registry and `GOMODCACHE` below are their deduplication.
3. bun extracts into the shared global cache and links `node_modules/.bun` to the extracted package; cargo extracts into
   the shared registry, and go extracts into the shared `GOMODCACHE` (0444 entries, internally locked, built for exactly
   this cross-consumer sharing), where every workspace and main read them thereafter.
4. Extracted bytes land once, in the tool's own cache in the host HOME: a package any checkout installed is already
   extracted for every other, and for the host's own tools.

## Known limitations

- **Path-keyed warm state.** Cargo keys a workspace crate on its package-relative path, so a session at a per-name mount
  finds main's units fresh; what stays path-keyed is whatever a build records absolutely — a dependency's `$CARGO_HOME`
  path (one literal path, above) and a build script's watched path outside its package — and Xcode DerivedData. Slot
  mounts (`fork --slot`) recycle stable paths for those. A compiled-in `env!("CARGO_MANIFEST_DIR")` is worse than
  path-keyed: cargo does not fingerprint the checkout path, so the inherited unit stays fresh and reads its origin's
  files; the path is read at run time instead, where cargo and nextest set it. cowshed does not force compiler flags.
- **Poisoning model.** Lockfile integrity hashes protect _downloads_ (layer 1 is verified by the package managers
  themselves), not cache _reuse_: layers 2–3 are trusted once written. State this plainly: the layer-3 write scope a
  sandbox holds includes cargo's `registry/src`, bun's global cache (whose `links/` main's `node_modules` resolves
  into), uv's cache and the Go caches — caches that _main itself compiles from_, so a poisoned entry can influence
  main's next build. That is an accepted risk under the confinement threat model (semi-trusted agents running the user's
  own code), bounded by write scope — a sandboxed workspace can write only the designated layer-3 subtrees, never the
  gateway mirror or `repo-mirrors` (layer 1, gateway-only) and never the Cargo/Gradle _config_ beside the shared caches
  (deny-listed, above). Keeping the caches in the host HOME widens nothing: the host's tools read the same directories
  the volume's links led them to before. The sccache store left that direct write scope entirely (Wiring): it is
  daemon-write-only, so poisoning it means going through the daemon's compile path rather than writing entries — the
  same trust class with one fewer direct write surface, traded against the daemon's named confused-deputy surface
  (04_sandbox.md). Go's posture within that scope is notably stronger than cargo's: module downloads verify against
  `go.sum` plus the checksum database, extraction is ziphash-verified, and `GOMODCACHE` entries land read-only (0444) —
  tampering requires an explicit chmod, which the escape suite exercises (04_sandbox.md); `GOCACHE` is the
  sccache-analog and shares its trust level.
- **Proxy-unaware tools.** Tools that hardcode registries need per-tool shims in the wiring step; the shim list grows by
  experience and lives in cowshed-core, not user config.
- **Simulator and Xcode state.** CoreSimulator device sets (`~/Library/Developer/CoreSimulator`) are **dev-uid host
  state** — shared mutable infrastructure like the nix store, never inside images (booting a per-workspace simulator
  would cost minutes and gigabytes for nothing; devices are reset with `simctl erase`, not cloned). `DerivedData` stays
  workspace-keyed in-image (convention table). Xcode.app itself is the one unavoidable `/Applications` global — Apple's
  licensing and packaging make it un-nixable; versions are managed with the `xcodes` CLI, and `DEVELOPER_DIR` selects
  per-project when needed.

## Tradeoffs

**"Concurrent-safe → shared" as the discriminator rejected.** An earlier framing placed caches by sharing safety, which
classified cargo's registry alongside bun's cache (both content-addressed, both immutable-entry, both effectively
concurrent-safe) and hid the property that actually matters. Safety is table stakes; _use_ is the discriminator. Cargo
never reflinks from its registry into `target/` — sharing it costs nothing. A cache that is a reflink source makes its
placement a speed decision, and a cache that is a link target makes its path a correctness decision. The spec says so
plainly to keep the next redesign from rediscovering it.

**Bun cache inside each image rejected.** A per-image cache keeps full materialization at clonefile speed only for a
tool that clones out of its cache (the measurements above). Under the isolated linker it buys no speed and breaks
correctness: `node_modules/.bun` links name the cache path, so a per-image cache makes them differ between main and
every clone, a clone's inherited links point into main's image (a sibling the sandbox denies), and every clone carries a
full copy of the cache (measured: a 5.5 GB copy inherited by every clone of one large repository).

**Environment variables as the wiring rejected.** Environment survives only processes cowshed spawns; files and host
paths survive everything. So identity is the marker file, the gateway URL lives in the config files that consume it, and
cache locations are the tools' own default directories — none of them an export. What is exported is what no file can
carry (the list above): Go's env file path, the token and port pair a dev server or proxy client needs, the sccache
endpoint for compile paths with no config file, and the per-job private environment and trust anchors only the
supervisor can set.

**Gateway-proxied sccache rejected.** Routing sccache through cowshed-gateway would mean translating sccache's own
client-server protocol for zero policy gain — the gateway mediates _egress_, and the sccache daemon never leaves the
host. cowshed adds lifecycle and scoped socket access, not protocol translation.

**Cross-session cache harvest rejected.** Actively copying new cache entries between live sessions adds a mutable side
channel between sandboxes and machinery (staging, folding, scheduling) whose entire benefit is bytes that
land-then-clone convergence already delivers a few hours later.

**A dedicated caches volume rejected.** Earlier releases put layer 3 on its own volume and linked the host's tool paths
into it, so that a sandbox's write scope was one subtree outside HOME. It bought nothing the HOME read deny and exact
per-tool carve-backs do not give, and it cost a second copy of nearly every cache: a tool the host ran before setup, or
a cache setup refused to move because both sides held one, kept filling its own default while sandboxes filled the
volume. Measured on one host before retirement: bun's install cache 20 GB in HOME beside the volume's, uv's 18 GB, Nix's
fetcher cache 14 GB in both places. It also needed an administrator authorization to provision, an fstab pin, a boot
mount service and a migration step whenever a tool was added. The tools' own defaults need none of that, and are the
paths the host's own builds already use.
