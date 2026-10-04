# Cache Architecture

Caches are classified by **what the tool does with the cache**, not by tool and not by concurrency safety. The
classification decides where bytes live, what is shared, and what each sandbox may write. Main and every session use
identical wiring — there is one cache reality, carried by files and host-level paths (not environment variables; see
Wiring).

## The discriminator

Nearly every cache in scope is content-addressed with immutable entries — bun's install cache is the same class of
object as cargo's registry. Concurrency safety therefore discriminates nothing; what matters is the cache's role at use
time:

- **Read-at-build caches** — cargo registry, Go module + build caches, uv, ttsc plugins, sccache, zig global cache,
  gradle. The tool reads sources or artifacts from the cache and writes its output somewhere else. The cache is only
  ever read at build time, so sharing it costs nothing. These live on the shared cache volume.
- **Link-target caches** — bun with its isolated linker (`[install] linker = "isolated"`). `bun install` extracts each
  package once into `<cache>/links/<name>@<version>-<hash>` and writes `node_modules/.bun/<name>@<version>` as an
  absolute symlink to it, so the cache's path is part of every checkout's `node_modules`. These live on the shared cache
  volume too, and every checkout reaches them through **one literal path** (Wiring, host-level relocation).
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

| Layer                                   | Contents                                                                                                                                                                                | Location                                                                                                                   | Sharing                                   |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| 1. Gateway mirrors                      | npm metadata and tarballs, bare repository mirrors                                                                                                                                      | `/private/cowshed/caches/mirror` and `/private/cowshed/caches/repo-mirrors` (gateway-owned, sandbox-read-only)             | Global, written only by cowshed-gateway   |
| 2. Clone-materializing caches           | caches a tool reflinks out of into the workspace — **none today**: bun's isolated linker links into its cache instead                                                                   | Inside each workspace image                                                                                                | Inherited from main via CoW at clone time |
| 3. Read-at-build and link-target caches | Cargo registry/git extraction caches, bun global install cache, uv cache, Go module + build caches, ttsc plugins, sccache, zig global cache, gradle, Nix eval/fetcher and profile state | Dedicated writable roots under `/private/cowshed/caches/` reached through the host's literal default path or direct config | Shared writable by all workspaces         |

Layer 1 removes duplicate _downloads_ (and stores compressed bytes once, ever). Bare repository mirrors live only at
`/private/cowshed/caches/repo-mirrors/<host>/<path>.git`; they are written by the gateway's `repo mirror` control-plane
verb and are read-only to workspaces and main. Cargo's `~/.cargo/git` is **not** that mirror tree: it is a shared
writable Cargo extraction/index cache at `/private/cowshed/caches/cargo/git`. The two have distinct ownership,
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
caches — is not shareable concurrently under any mechanism. It stays in-image, warm because the image was cloned from
main.

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
    is the shared link-target cache every checkout reaches through the host's literal path (host-level relocation
    below), and an `[install.cache] dir` inside the checkout would write each checkout's own path into its
    `node_modules/.bun` links.
  - **cargo: no registry configuration at all.** Every sandbox builds with one literal `CARGO_HOME` — the host's
    `~/.cargo`, whose `registry` and `git` are relocated to the caches volume (below) — because cargo fingerprints a
    registry or git dependency by its absolute source path under `CARGO_HOME`: the same crate reached through another
    path, even a symlink to the same bytes, recompiles it and everything above it. So there is no per-workspace
    `$CARGO_HOME/config.toml` to carry a crates.io source replacement, and `~/.cargo/config.toml` stays host-owned and
    denied. Cargo reaches crates.io through the proxy variables (below), intercepted, trusting the workspace trust
    bundle through `CARGO_HTTP_CAINFO` (04_sandbox.md); `index.crates.io` and `static.crates.io` are project-standing
    egress grants. Downloads land once per host in the shared registry. Git dependencies resolve through the fetch
    mappings to local clones when their checkouts are granted, and otherwise through intercepted `github.com`.
  - No git remote config or credential helper is written into the image: git reaches the network through the proxy
    variables like every other client, fetch-only (05_gateway.md "Egress modes"), and the fetch routes rewrite a bound
    repository's URLs onto its local clone (02_workspaces.md "Remote code ingress").
  - **Go env file** at `$XDG_CACHE_HOME/go/env` (the `go env -w` format), reached via a `GOENV` export (below). Go is
    the one toolchain with **no project-level config file** — settings live in a single user-global env file
    (`os.UserConfigDir()/go/env`, measured default `~/Library/Application Support/go/env`) overridable only by `GOENV` —
    and its settings are per-workspace. The file pins: `GOPROXY=https://proxy.golang.org` (no `,direct` fallback — a
    miss fails rather than cloning a VCS repository) and `GOSUMDB=sum.golang.org`, both reached through the proxy
    variables (below) as **opaque** tunnels — Go on macOS verifies TLS with the platform verifier and never trusts the
    workspace CA, so both hosts are project-standing `--opaque` egress grants. Go does not use the loopback mirror:
    `cmd/go` attaches credentials (netrc, `GOAUTH`, URL userinfo) only to HTTPS URLs, and the mirror is plain HTTP, so
    no Go client can present the workspace token to it. It also pins `GOMODCACHE=/private/cowshed/caches/go/mod` and
    `GOCACHE=/private/cowshed/caches/go/build` (shared, layer 3 — a module downloads once per host),
    `GOPATH=<mount>/.cowshed/cache/go/path` and `GOBIN=<mount>/.cowshed/cache/go/bin` (in-image, workspace-keyed —
    `go install` binaries are the `~/.cargo/bin` persistence-escape hazard and must never land on the shared volume).
    Net effect: **`~/go` is never created** (measured on this host: the devenv-provided go 1.26.3 had already grown a
    1.1 GB `~/go/pkg/mod` under the defaults); 04_sandbox.md turns any regression into a loud tripwire. cowshed also
    writes **`GOTOOLCHAIN=local`**: the toolchain is nix/devenv-provided and pinned, and `auto` silently downloading Go
    toolchains contradicts the declarative environment — a project that deliberately overrides to `auto` gets its
    downloads in `GOMODCACHE`, i.e. on the caches volume, never in `$HOME`. A host-global `go env -w` file instead of
    `GOENV` is rejected: `GOPATH` and `GOBIN` are per-workspace, and a global file would share one workspace's with
    every other. The same file serves a host process that loads the workspace's `.envrc`: it names no endpoint and
    carries no token, so it works outside the sandbox too.
- **Generic proxy variables.** Workspace env wiring sets `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`, and `https_proxy` to
  `<GATEWAY_HTTP>` with the workspace token as its userinfo (`http://cowshed:<token>@…`), and configures
  `NO_PROXY`/`no_proxy` only for the workspace's own local services. On Linux these variables therefore resolve to
  `http://127.0.0.1:7644`; on macOS they resolve to the workspace block base. Userinfo is the one channel standard
  clients (curl, libcurl so cargo, reqwest, Go) turn into `Proxy-Authorization: Basic` on the first CONNECT; the token
  authenticates against nothing but this workspace's own endpoint.
- **Trust bundle.** `.cowshed/ca-bundle.pem` in the private environment holds the platform roots followed by the
  workspace CA, and is what `GIT_SSL_CAINFO`, `CARGO_HTTP_CAINFO` and nix read (04_sandbox.md).
- **In-image tool shims** at `.cowshed/bin/`, PATH-prepended by the same `.envrc` wiring (so they travel with every
  clone and cover every process spawned in the workspace, IDE terminals included). Today that is one shim: the **`xcrun`
  wrapper** — pure `exec /usr/bin/xcrun "$@"` passthrough for everything except the simulator-control verbs (`simctl`,
  `devicectl`), so toolchain calls (`xcrun clang`, `--show-sdk-path`) stay native-speed and unbreakable. Simulator verbs
  resolve their device target: **dev-local CoreSimulator is the default** (agents and automation never accidentally
  reach the personal session); personal-session devices appear as explicitly-named remote targets and route through the
  gateway's `/sim/` endpoint (05_gateway.md) under the `sim` grant axis (04_sandbox.md). Tools that hardcode
  `/usr/bin/xcrun` bypass the shim and degrade to dev-local simulators — the safe default.
- **Host-level relocation, once — cache subtrees only**: `cowshed setup --imperative-host-setup` (idempotent, re-checked
  by `doctor`) makes the shared tools' _cache_ directories resolve to these exact dedicated roots:

  | Tool default           | cowshed.caches target                       |
  | ---------------------- | ------------------------------------------- |
  | `~/.cargo/registry`    | `/private/cowshed/caches/cargo/registry`    |
  | `~/.cargo/git`         | `/private/cowshed/caches/cargo/git`         |
  | `~/.bun/install/cache` | `/private/cowshed/caches/bun/install/cache` |
  | `~/.cache/uv`          | `/private/cowshed/caches/uv`                |
  | `~/.cache/zig`         | `/private/cowshed/caches/zig`               |
  | `~/.gradle/caches`     | `/private/cowshed/caches/gradle/caches`     |
  | `~/.cache/nix`         | `/private/cowshed/caches/nix/cache`         |
  | `~/.local/state/nix`   | `/private/cowshed/caches/nix/state`         |

  Each host path becomes a symlink to its target. An absent host path is linked; a real directory is moved first — a
  copy across volumes into a staging directory beside the target, published with one rename, and only then is the
  original removed — provided the target is missing or empty. The copy keeps symlinks, modes, times and hard links
  (`ditto` on macOS, where `cp -a` splits hard links; `cp -a` on Linux): cargo's git checkouts share inodes with its
  databases, and a copy that split them would grow the cache several-fold. A host path and a target that both hold a
  cache, or a host path that already links elsewhere, is a conflict: setup leaves both exactly as they are, names them,
  and exits non-zero. Cargo's two move while setup holds cargo's own `.package-cache` and `.package-cache-mutate` locks,
  so no cargo process reads or writes them mid-copy; a cargo process holding a lock refuses the run. bun and uv offer no
  host-wide lock to take, so their caches move only while no install runs. sccache's platform cache is not linked: the
  store is daemon-write-only, and `cowshed setup` instead writes sccache's own config so a store-less client caches in
  `/private/cowshed/caches/sccache` (below). Go and ttsc remain direct-configured:
  `/private/cowshed/caches/go/{mod,build}` through Go's env file and `/private/cowshed/caches/ttsc` through
  `TTSC_CACHE_DIR`; the supervisor creates these three directories before a child runs, because a child granted writes
  inside one cannot create its parent. A smoo-managed repository shell (`tooling/direnv/shared-caches.sh`) exports
  `TTSC_CACHE_DIR`, `GOCACHE` and `GOMODCACHE` naming those same directories whenever `/private/cowshed/caches` exists,
  on the host and in every sandbox alike, so one path reaches each cache from every checkout. Gateway artifacts remain
  outside every writable tool root at `mirror/` and `repo-mirrors/`.

  **Sandboxed shells may inherit devenv evaluations privately.** devenv evaluates a project at its path, and the result
  really differs per path. The absolute root, its `.devenv`, `TMPDIR` and runtime directory are inputs to its
  evaluation-cache key, and its exported hook may embed each path. The origin checkout's `.devenv` lives inside the
  workspace image and travels to a new workspace through cowshed's copy-on-write clone. A smoo-managed shell
  (`tooling/direnv/inherited-devenv.ts`) records a real `devenv direnv-export` in that private `.devenv` with a verified
  disabled merged hook. Its evaluator receives exactly what a workspace child is handed and evaluation needs: the
  bootstrap `PATH`, the private `HOME`, `TMPDIR` and XDG roots (Nix's client fetcher cache is the shared
  `$XDG_CACHE_HOME/nix`), and the workspace's live routing — the proxy variables (token as userinfo), `NO_PROXY`, the CA
  files and the `ssl-cert-file` line of `NIX_CONFIG`. The evaluator disables Nix's shared evaluation cache so it cannot
  supply executable shell code, and receives no login identity, locale, terminal, Xcode selection, other credential or
  `NIX_CONFIG` line. An export that embeds a proxy value or its userinfo is never published, so a clone always routes
  through its own gateway. Before relocating it in a clone, it checks exact source and local path-input contents,
  devenv's watched input paths, the toolchain, and the emitted task graph. The graph may not contain writing
  prerequisites for `devenv:enterShell`. The original merged enterShell hook is not executed during verification; direnv
  imports and runs it in the destination checkout. Missing, changed or unsafe evidence falls back to an in-place
  evaluation. No sibling can update another's private artifact: a shared writable cache, however content-addressed,
  cannot safely serve executable shell exports.

  **Every checkout reaches a shared tool home through the host's literal path.** Cargo fingerprints a registry or git
  dependency by the absolute path of its source under `$CARGO_HOME` (measured: the same registry reached through a
  different `$CARGO_HOME` path, even a symlink to the same bytes, recompiles the dependency and everything built on it),
  and bun's isolated linker writes its cache's path into every `node_modules/.bun` link. A sandbox whose tools followed
  its private `HOME` would rebuild every dependency a clone's copied `target/` already holds and relink `node_modules`
  into a cache no other checkout has. Once all of a tool's caches are relocated, every sandboxed child is pointed at the
  host's own default path — `CARGO_HOME=<host home>/.cargo`, `BUN_INSTALL_CACHE_DIR=<host home>/.bun/install/cache`,
  `UV_CACHE_DIR=<host home>/.cache/uv` — and its profile admits exactly: the shared directories read-write; literal
  reads of the host path, its ancestors, and cargo's `registry` and `git` links; and read-write literals for the files
  cargo writes at its root (the package-cache locks and the `.global-cache` usage database with its journal). Nothing
  else in a host tool home is granted. A caller's own value for these variables never reaches the child. Until a tool's
  caches are relocated the sandbox keeps the tool's private default under its private `HOME`, and `doctor` reports each
  unshared cache; an unshared bun cache stays readable, never writable, so the links a clone inherited from main's
  `node_modules` keep resolving.

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

  **The parent config directories stay on the host.** `~/.cargo/config.toml`, `~/.cargo/config`,
  `~/.cargo/credentials.toml`, `~/.cargo/credentials`, `~/.cargo/bin` (on PATH), and `~/.gradle/gradle.properties` are
  _not_ relocated and are on the secret deny list (04_sandbox.md) — relocating them wholesale would put user config,
  credentials, and PATH-resolved binaries on a sandbox-writable volume, a persistence-escape surface; a sandbox building
  against the host `$CARGO_HOME` still cannot read or write any of them. No `ZIG_GLOBAL_CACHE_DIR` or `GRADLE_USER_HOME`
  exports exist in workspaces; `SCCACHE_DIR` reaches a job only as the supervisor sets it, naming the daemon's own cache
  directory, which the sandbox can read and never write (below). Go needs no symlink at all — `GOMODCACHE`/`GOCACHE` are
  directly configurable in its env file (above), which is strictly cleaner than relocating a default path. Profile
  generation canonicalizes symlinked paths when emitting write grants (the `/var` → `/private/var` handling
  generalizes).

  On home-manager/NixOS/nix-darwin hosts this relocation is **declarative and mandatory**: the module creates the exact
  links/bindings above as generation-managed artifacts, including the two Nix subdirectories, and `setup` only
  validates. They never mutate module-owned paths: a host path that links into `/nix/store` anywhere but its target is a
  conflict naming the module. The sole exception is an explicitly imperative, non-declarative host: when no supported
  declarative manager owns the paths, `cowshed setup --imperative-host-setup` creates the same links; the flag is the
  confirmation, and a default `setup` never moves anything out of the user's home. There is no automatic fallback from
  failed declarative validation; mixed ownership is a conflict and `doctor` points to the declarative option that must
  be fixed.

- **Environment variables.** Two sets, and neither is wiring a file could carry instead:
  - The in-image `.cowshed/env`, sourced by the workspace's `.envrc`, exports what processes cowshed never spawned need
    and no file can give them: `GOENV=<mount>/.cowshed/cache/go/env` (Go has no directory-scoped config, so its in-image
    env file is reachable only through this export; IDE-spawned tools such as gopls get it via the editor's direnv
    integration), `COWSHED_WORKSPACE_TOKEN`, and on macOS the dev-server port conventions `COWSHED_PORT_BASE` and
    `COWSHED_PORT_BLOCK_SIZE` (04_sandbox.md, cooperative-sandboxing caveat). Linux exports no port values: services use
    private loopback and package/proxy wiring uses the fixed `http://127.0.0.1:7644`. No workspace-identity variable is
    exported; anything that needs identity derives it from cwd via `.cowshed/workspace.json` or asks the CLI.
  - The supervisor sets, for every job it spawns (`runtime/supervisor.rs` `sandbox_environment`):
    - the private environment: `HOME`, `XDG_{CONFIG,CACHE,DATA,STATE}_HOME`, `DIRENV_CONFIG`, `TMPDIR` (the workspace's
      temp dir in the project store, 04_sandbox.md), `XDG_RUNTIME_DIR`, `NX_SOCKET_DIR`, `NX_WORKSPACE_DATA_DIRECTORY`
      and `NX_CACHE_DIRECTORY` (the sandbox's own Nx daemon record, task database and cache under the private
      `XDG_CACHE_HOME`, with a caller's `NX_DAEMON` withheld so Nx's own default decides, 04_sandbox.md), and the shared
      tool homes (`CARGO_HOME`, `BUN_INSTALL_CACHE_DIR`, `UV_CACHE_DIR`, above), which name the host's own default paths
      only to undo the private `HOME`;
    - git isolation: `GIT_CONFIG_GLOBAL`, `GIT_CONFIG_NOSYSTEM`, `GIT_ATTR_NOSYSTEM`, the fetch-route include as
      `GIT_CONFIG_COUNT`/`KEY`/`VALUE` (02_workspaces.md), and `CARGO_NET_GIT_FETCH_WITH_CLI`;
    - the `.cowshed/env` set again (`GOENV`, the token, the port pair), `SCCACHE_SERVER_UDS` and `SCCACHE_DIR` (the host
      sccache daemon, below), and `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` in both cases, carrying the token as proxy
      userinfo;
    - the build wiring: `SCCACHE_BASEDIR_CWD=1`, and `RUSTC_WRAPPER` naming `bin/sccache` inside the store path the
      host's sccache GC root pins, the program the daemon itself runs (below). It is read through the root before every
      spawn and names the program rather than a `PATH` entry: shell activation owns `PATH`, and a repository shell that
      ships no sccache left a bare `sccache` unresolvable, failing every cargo at its version probe. A host that pinned
      none — sccache is opt-in — or whose pinned store path was collected gets no wrapper at all; neither value is the
      caller's;
    - inherited devenv evaluation artifacts in the checkout's private `.devenv` (no shared cache variable);
    - trust anchors as defaults a caller may override: `NODE_EXTRA_CA_CERTS`, `GIT_SSL_CAINFO`, `CARGO_HTTP_CAINFO`,
      `NIX_SSL_CERT_FILE`, `SSL_CERT_FILE`, `UV_SYSTEM_CERTS=true`, and an `ssl-cert-file` line appended to `NIX_CONFIG`
      (04_sandbox.md);
    - the bootstrap `PATH` (04_sandbox.md).

    That list is the whole job contract; everything else a job sees is what the workspace's own `.envrc`/devenv exports
    and what the request's explicit `env` names. Nothing is read from the environment of the process that runs the
    supervisor: no locale, terminal, Xcode selection, login `PATH`, `NX_*`, `VIRTUAL_ENV` or `CARGO_HOME` from another
    shell reaches a job, and a tool a build needs from the host user's home is declared in the workspace devenv instead.

    No registry client receives the workspace token from a file: it travels only as proxy userinfo and as
    `COWSHED_WORKSPACE_TOKEN`, and no file under the private `home`, `config` or `cache` carries it. The cargo `[env]`
    guidance above mirrors `SCCACHE_SERVER_UDS` for cargo builds cowshed never spawned.

### The sccache daemon

sccache is served by a **host-owned daemon**: the `dev.cowshed.sccache` LaunchAgent runs the sccache binary itself as a
foreground unix-socket server outside every sandbox — `SCCACHE_START_SERVER=1` selects server mode,
`SCCACHE_NO_DAEMON=1` keeps it in the foreground under launchd supervision, `SCCACHE_IDLE_TIMEOUT=0` disables idle exit,
and its environment pins `SCCACHE_SERVER_UDS=/private/cowshed/store/sccache.sock` and
`SCCACHE_DIR=/private/cowshed/caches/sccache` (all source-verified against sccache 0.16, which reads
`SCCACHE_SERVER_UDS` in both client and server ahead of `SCCACHE_SERVER_PORT`; the TCP port is the fallback wiring only
on a platform without unix sockets, and then the Seatbelt loopback-allow class in 04_sandbox.md applies).
`cowshed sccache start|stop|status` install, remove, and probe the agent; start is healthy when the socket answers.
Every disk-cache read and write happens inside the daemon (source-verified: sccache instantiates its disk cache only in
the server process), so the Seatbelt write carve-back for `/private/cowshed/caches/sccache` is gone — the store is
**daemon-write-only** and sandboxes keep only the caches-wide read.

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
   once (credentials injected where trusted policy admits them), stores it content-addressed on the caches volume, and
   serves it over loopback, so no WAN duplication across workspaces. Cargo and Go fetch from their registries as
   intercepted and opaque egress; the shared cargo registry and `GOMODCACHE` below are their deduplication.
3. bun extracts into the shared global cache and links `node_modules/.bun` to the extracted package; cargo extracts into
   the shared registry, and go extracts into the shared `GOMODCACHE` (0444 entries, internally locked, built for exactly
   this cross-consumer sharing), where every workspace and main read them thereafter.
4. Extracted bytes land once on the caches volume: a package any checkout installed is already extracted for every
   other.

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
  gateway mirror or `repo-mirrors` (layer 1, gateway-only) and never relocated Cargo/Gradle _config_ (host-side,
  deny-listed — see relocation above). The sccache store left that direct write scope entirely (Wiring): it is
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
cache locations are the tools' own default directories relocated once — none of them an export. What is exported is what
no file can carry (the list above): Go's env file path, the token and port pair a dev server or proxy client needs, the
sccache endpoint for compile paths with no config file, and the per-job private environment and trust anchors only the
supervisor can set.

**Gateway-proxied sccache rejected.** Routing sccache through cowshed-gateway would mean translating sccache's own
client-server protocol for zero policy gain — the gateway mediates _egress_, and the sccache daemon never leaves the
host. cowshed adds lifecycle and scoped socket access, not protocol translation.

**Cross-session cache harvest rejected.** Actively copying new cache entries between live sessions adds a mutable side
channel between sandboxes and machinery (staging, folding, scheduling) whose entire benefit is bytes that
land-then-clone convergence already delivers a few hours later.
