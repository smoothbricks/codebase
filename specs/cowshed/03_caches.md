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
| 2. Clone-materializing caches           | caches a tool reflinks out of into the workspace — **none today**: bun's isolated linker links into its cache instead                                                   | Inside each workspace image                                                                                                  | Inherited from main via CoW at clone time |
| 3. Read-at-build and link-target caches | Cargo registry/git extraction caches, bun/npm/pnpm install caches, uv cache, Go module + build caches, sccache, zig global cache, gradle, Nix fetcher and profile state | Each tool's own default location in the host user's HOME, shared into the sandboxes of projects that use the tool            | Shared writable by detecting workspaces   |

Layer 1 removes duplicate _downloads_ (and stores compressed bytes once, ever). Bare repository mirrors live only at
`<cowshed cache dir>/repo-mirrors/<host>/<path>.git`; they are written by the gateway's `repo mirror` control-plane verb
and, like the rest of cowshed's cache directory, are controller state no sandbox reads or writes. Cargo's `~/.cargo/git`
is **not** that mirror tree: it is cargo's own shared writable extraction/index cache. The two have distinct ownership,
permissions, and paths, so a Cargo process can never mutate gateway repository mirrors. Layer 2 is one special case, not
a category: it exists exactly where a tool reflinks out of its cache and the substrate cannot reflink across volume
boundaries, and no cache in scope does that today. Layer 3 caches are read at build time or linked to, and write nowhere
near the workspace, so sharing them is free.

**ZFS may widen layer 2** (verify item, not a promise): OpenZFS 2.2 block cloning (BRT) works across datasets within a
pool — something APFS clonefile cannot do across volumes. A clone-materializing cache whose copy path goes through
`copy_file_range` (which BRT intercepts) would get reflink speed from a pool-shared location, so on ZFS layer 2 could
share with zero duplication and no speed penalty. Bun's Linux _default_ backend is hardlink, which cannot cross
datasets; its isolated linker needs neither.

**Workspace-keyed state** — `target/`, materialized `node_modules`, `DerivedData`, `.nx`, `.zig-cache`, Metro/Expo
caches — is not shareable concurrently under any mechanism. A build tool's per-tree incremental state that capability
detection names (Cargo target directories, Nx's cache and task database) lives on the workspace's build volume, cloned
from main's seed at fork and adopted by main at land (16_build_volumes.md). The rest stays in the workspace image, warm
because the image was cloned from main.

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

- **Private-environment config files**, written when the workspace is minted (adopt/new/fork, and rekey/restore's fresh
  CA) and republished before every exec, so a moved environment is never served stale. They live in the workspace's
  private environment — `.cowshed/{home,config,cache}`, which a cowshed-spawned child gets as `HOME`, `XDG_CONFIG_HOME`
  and `XDG_CACHE_HOME` — never in a tracked file, so a rewrite never dirties `git status`. A read-only exec gets the
  same files under its exec-temp environment. Define `GATEWAY_HTTP` as `http://127.0.0.1:<portBlock.base>` on macOS and
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
    intercepted, trusting the workspace trust bundle through `CARGO_HTTP_CAINFO` (04_sandbox.md); `index.crates.io` and
    `static.crates.io` are project-standing egress grants. Downloads land once per host in the shared registry. Git
    dependencies resolve through the fetch mappings to local clones when their checkouts are granted, and otherwise
    through intercepted `github.com`. A host whose toolchain is rustup's lends it read-only: the proxies in
    `~/.cargo/bin`, and `~/.rustup`'s settings and toolchains through an owned `RUSTUP_HOME`; nothing a sandbox runs
    installs or updates a toolchain there.
  - No git remote config or credential helper is written into the image: git reaches the network through the proxy
    variables like every other client, fetch-only (05_gateway.md "Egress modes"), and the fetch routes rewrite a bound
    repository's URLs onto its local clone (02_workspaces.md "Remote code ingress").
  - **Go: no configuration file.** A Go project (`go.mod` or `go.work`) gets `GOMODCACHE` and `GOCACHE` naming the
    host's own Go defaults — `<host home>/go/pkg/mod` and the host user cache directory's `go-build`
    (`~/Library/Caches/go-build` on macOS, `~/.cache/go-build` on Linux) — shared, layer 3: a module downloads once per
    host, and the host's own `go` uses the same two directories without any configuration. `GOPATH`, and with it
    `go install`'s binaries, stays at Go's default under the private `HOME`, in the image and per workspace, never in
    the host HOME. `GOPROXY`, `GOSUMDB` and `GOTOOLCHAIN` are Go's defaults or the project's own setting. Go reaches
    `proxy.golang.org` and `sum.golang.org` through the proxy variables (below) as **opaque** tunnels: Go on macOS
    verifies TLS with the platform verifier and never trusts the workspace CA. No Go client presents the workspace token
    to a gateway route: `cmd/go` attaches credentials (netrc, `GOAUTH`, URL userinfo) only to HTTPS URLs.
- **Generic proxy variables.** Workspace env wiring sets `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`, and `https_proxy` to
  `<GATEWAY_HTTP>` with the workspace token as its userinfo (`http://cowshed:<token>@…`), and configures
  `NO_PROXY`/`no_proxy` only for the workspace's own local services. On Linux these variables therefore resolve to
  `http://127.0.0.1:7644`; on macOS they resolve to the workspace block base. Userinfo is the one channel standard
  clients (curl, libcurl so cargo, reqwest, Go) turn into `Proxy-Authorization: Basic` on the first CONNECT; the token
  authenticates against nothing but this workspace's own endpoint.
- **Trust bundle.** `.cowshed/ca-bundle.pem` in the private environment holds the platform roots followed by the
  workspace CA. The core points `GIT_SSL_CAINFO` and `SSL_CERT_FILE` at it; each detected capability adds its tool's own
  variable, such as cargo's `CARGO_HTTP_CAINFO` (04_sandbox.md).
- **In-image tool shims** at `.cowshed/bin/`, PATH-prepended by the same `.envrc` wiring (so they travel with every
  clone and cover every process spawned in the workspace, IDE terminals included). Today that is one shim: the **`xcrun`
  wrapper** — pure `exec /usr/bin/xcrun "$@"` passthrough for everything except the simulator-control verbs (`simctl`,
  `devicectl`), so toolchain calls (`xcrun clang`, `--show-sdk-path`) stay native-speed and unbreakable. Simulator verbs
  resolve their device target: **dev-local CoreSimulator is the default** (agents and automation never accidentally
  reach the personal session); personal-session devices appear as explicitly-named remote targets and route through the
  gateway's `/sim/` endpoint (05_gateway.md) under the `sim` grant axis (04_sandbox.md). Tools that hardcode
  `/usr/bin/xcrun` bypass the shim and degrade to dev-local simulators — the safe default.
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
     content-addressed; the host's copy wins and the volume's is deleted.
  3. Directories the volume held for no detector — the gateway's `mirror/` and `repo-mirrors/`, sccache's store,
     repository-placed caches such as `ttsc/` — move to their HOME location the same way: the gateway's to cowshed's own
     cache directory, sccache's to its default directory, a repository's to the `[caches] home` path that repository
     declares.
  4. Under `--retire-caches-volume`, and only once steps 1–3 left the volume holding nothing but its marker, setup
     deletes the volume (or destroys the dataset), removes its `/etc/fstab` pin and drops it from the storage mount
     service, inside the one administrator authorization setup already uses. The flag refuses while anything else is
     left on the volume: whatever setup cannot place is named, with its size, so nothing is deleted that was not moved.

  Setup reports the bytes it moved and the bytes it dropped as duplicates, per cache. While anything is left it names
  each leftover with its size and states that `cowshed setup --retire-caches-volume` cannot run until those are placed;
  once only the marker remains it names that exact command as the attended next step. On home-manager, NixOS and
  nix-darwin hosts a module that still declares a link into the volume is a conflict naming that module option; setup
  never rewrites module-owned paths.

  **Sandboxed shells evaluate devenv in place.** devenv evaluates a project at its path, and the result really differs
  per path. The absolute root, its `.devenv`, `TMPDIR` and runtime directory are inputs to its evaluation-cache key, and
  its exported hook may embed each path. A clone's copy of its origin's `.devenv` therefore never answers for the clone:
  each checkout evaluates its own shell, and no shared writable cache, however content-addressed, serves executable
  shell exports.

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
  `GRADLE_USER_HOME` is private (below), never the host's `~/.gradle`; `SCCACHE_DIR` reaches a job only as the
  supervisor sets it, naming the daemon's own cache directory, which the sandbox can neither read nor write (below).
  Profile generation canonicalizes symlinked paths when emitting write grants (the `/var` → `/private/var` handling
  generalizes).

- **Environment variables.** Two sets, and neither is wiring a file could carry instead:
  - The in-image `.cowshed/env`, sourced by the workspace's `.envrc`, exports what processes cowshed never spawned need
    and no file can give them: `COWSHED_WORKSPACE_TOKEN`, and on macOS the dev-server port conventions
    `COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE` (04_sandbox.md, cooperative-sandboxing caveat). Linux exports no
    port values: services use private loopback and package/proxy wiring uses the fixed `http://127.0.0.1:7644`. No
    workspace-identity variable is exported; anything that needs identity derives it from cwd via
    `.cowshed/workspace.json` or asks the CLI.
  - The supervisor sets, for every job it spawns (`runtime/supervisor.rs` `sandbox_environment`):
    - the private environment: `HOME`, `XDG_{CONFIG,CACHE,DATA,STATE}_HOME`, `DIRENV_CONFIG`, `TMPDIR` (the workspace's
      temp dir in the project store, 04_sandbox.md) and `XDG_RUNTIME_DIR`;
    - git isolation: `GIT_CONFIG_GLOBAL`, `GIT_CONFIG_NOSYSTEM`, `GIT_ATTR_NOSYSTEM`, and the fetch-route include as
      `GIT_CONFIG_COUNT`/`KEY`/`VALUE` (02_workspaces.md);
    - each detected capability's contribution (15_capabilities.md): for an Nx project `NX_SOCKET_DIR`,
      `NX_WORKSPACE_DATA_DIRECTORY` and `NX_CACHE_DIRECTORY` (for a read-write job the checkout's own `.nx`, shared with
      the checkout's host shells; for a read-only job its exec temp dir; a caller's `NX_DAEMON` withheld so Nx's own
      default decides, 04_sandbox.md) and `NX_WORKSPACE_ROOT_PATH`; for a cargo project `CARGO_HOME` (above, naming the
      host's own default path only to undo the private `HOME`), `CARGO_NET_GIT_FETCH_WITH_CLI=true` and on a rustup host
      `RUSTUP_HOME`; for a Go project `GOMODCACHE` and `GOCACHE`; the other shared tool homes of detected package
      managers and toolchains (`BUN_INSTALL_CACHE_DIR`, `NPM_CONFIG_CACHE`, `PNPM_CONFIG_STORE_DIR`, `UV_CACHE_DIR`,
      `ZIG_GLOBAL_CACHE_DIR`), each naming the host's own path; and for a Gradle project a private `GRADLE_USER_HOME`
      under the private cache whose `caches` links to the host's `~/.gradle/caches`, so the daemon, wrapper
      distributions, native libraries and JDKs stay private and no other part of the host `~/.gradle` is granted;
    - the `.cowshed/env` set again (the token, the port pair), `SCCACHE_SERVER_UDS` and `SCCACHE_DIR` (the host sccache
      daemon, below), and `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` in both cases, carrying the token as proxy userinfo;
    - the build wiring: `SCCACHE_BASEDIR_CWD=1`, and `RUSTC_WRAPPER` naming `bin/sccache` inside the store path the
      host's sccache GC root pins, the program the daemon itself runs (below). It is read through the root before every
      spawn and names the program rather than a `PATH` entry: shell activation owns `PATH`, and a repository shell that
      ships no sccache left a bare `sccache` unresolvable, failing every cargo at its version probe. A host that pinned
      none — sccache is opt-in — or whose pinned store path was collected gets no wrapper at all; neither value is the
      caller's;
    - trust anchors as defaults a caller may override: the core's `GIT_SSL_CAINFO` and `SSL_CERT_FILE`, and each
      detected capability's own — `CARGO_HTTP_CAINFO`, `NODE_EXTRA_CA_CERTS`, `NIX_SSL_CERT_FILE` with an
      `ssl-cert-file` line appended to `NIX_CONFIG`, `UV_SYSTEM_CERTS=true` (04_sandbox.md);
    - the bootstrap `PATH` (04_sandbox.md).

    That list is the whole job contract; everything else a job sees is what the workspace's own `.envrc`/devenv exports
    and what the request's explicit `env` names. Nothing is read from the environment of the process that runs the
    supervisor: no locale, terminal, Xcode selection, login `PATH`, `NX_*`, `VIRTUAL_ENV` or `CARGO_HOME` from another
    shell reaches a job, and a tool a build needs from the host user's home is declared in the workspace devenv instead.

    No registry client receives the workspace token from a file: it travels only as proxy userinfo and as
    `COWSHED_WORKSPACE_TOKEN`, and no file under the private `home`, `config` or `cache` carries it. The cargo `[env]`
    guidance above mirrors `SCCACHE_SERVER_UDS` for cargo builds cowshed never spawned.

    **Limitation: per-workspace `CARGO_*` paths key nested-cwd cargo runs per workspace.** sccache hashes every
    `CARGO_*` value into a Rust key, and `SCCACHE_BASEDIR_CWD=1` normalizes only the request's cwd prefix. Two of those
    values are paths under the checkout: `CARGO_HTTP_CAINFO` (`<checkout>/.cowshed/ca-bundle.pem`) and the devenv's
    `CARGO_INSTALL_ROOT` (`<checkout>/tooling/direnv/.devenv/state/cargo-install`, exported by devenv's own rust module
    in `enterShell`, after every capability's env). A cargo run from the checkout root strips both. A run whose cwd is
    below the root (a nested Cargo workspace such as a vendored upstream checkout) keys them verbatim, so its Rust
    entries are per workspace: 0/192 cross-workspace hits measured. Neither path can take a stable spelling. The
    bundle's content is per workspace, since it is the platform roots plus the workspace's own CA (04_sandbox.md), so a
    content-addressed path still differs per workspace. A path spelled the same in every workspace but holding each
    workspace's content would need a per-process filesystem namespace, which macOS does not have. Cargo still needs the
    bundle: rustup's cargo verifies through SecureTransport, which ignores `SSL_CERT_FILE`. The bundled sccache instead
    takes a per-request `SCCACHE_BASEDIR=<absolute dir>` (both hashers; the C/C++ one for cmake and ninja builds below a
    checkout root). A nested build that should share entries across workspaces names the checkout root there, which
    strips both paths; cowshed does not export it, because only the build knows which root its outputs may be relative
    to.

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

## Convention table

Cache classification is a built-in table keyed by well-known names — never configuration:

| Name (at any depth, gitignored)                              | Class                     |
| ------------------------------------------------------------ | ------------------------- |
| `node_modules`                                               | workspace-keyed, in-image |
| `target` (with `Cargo.toml` sibling)                         | workspace-keyed, in-image |
| `.nx`, `.turbo`, `.next`, `.expo`, `.gradle` (project-local) | workspace-keyed, in-image |
| `.zig-cache`, `zig-cache`, `zig-out`                         | workspace-keyed, in-image |
| `DerivedData` (project-local)                                | workspace-keyed, in-image |
| `Pods`                                                       | workspace-keyed, in-image |
| `vendor` (with `go.mod` sibling)                             | workspace-keyed, in-image |

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
  mounts (`new --slot`) recycle stable paths for those. A compiled-in `env!("CARGO_MANIFEST_DIR")` is worse than
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
