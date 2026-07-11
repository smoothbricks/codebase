# Cache Architecture

Caches are classified by **what the tool does with the cache**, not by tool and not by concurrency safety. The
classification decides where bytes live, what is shared, and what each sandbox may write. Main and every session use
identical wiring — there is one cache reality, carried by files and host-level paths (not environment variables; see
Wiring).

## The discriminator

Nearly every cache in scope is content-addressed with immutable entries — bun's install cache is the same class of
object as cargo's registry. Concurrency safety therefore discriminates nothing; what matters is the cache's role at use
time:

- **Read-at-build caches** — cargo registry, Go module + build caches, sccache, zig global cache, gradle. The tool reads
  sources or artifacts from the cache and writes its output somewhere else. The cache is only ever read at build time,
  so sharing it costs nothing. These live on the shared cache volume.
- **Clone-materializing caches** — bun. `bun install` materializes `node_modules` by _cloning from the cache_: the cache
  is the reflink source. APFS clonefile is strictly same-volume, so cache placement decides whether install runs at
  clonefile speed or copyfile speed. These must live **reflink-reachable from the workspace** — on APFS, inside the
  workspace image.

Why placement decides it (measured, so it is not re-litigated — bun 1.3.14, warm caches, lockfile pinned, 83 packages /
1,579 files / 59 MB `node_modules`, `rm -rf node_modules` + reinstall ×3): **in-volume cache: 0.03 s and 480 KiB of
volume space consumed** (99.2% of blocks shared with the cache — clonefile confirmed); **cross-volume cache: 0.18 s (6×)
and a full 59 MB copy, zero sharing**. For delta installs the two placements are nearly a wash — the gateway mirror
already dedupes the download. The deciding case is **full materialization**: a wiped `node_modules`, a big lockfile
churn, a fresh adopt — at Conloca scale (~90k objects, GBs) the measured 6× ratio lands in the
seconds-vs-tens-of-seconds band (the project benchmark's 100k-file copy took ~7.5 s for 256-byte files; real file sizes
are worse). The measured 0.8% marginal disk also proves the "cache and `node_modules` share blocks in-image" claim:
carrying the cache in the image is nearly free. The cost is sibling workspaces duplicating entries fetched post-clone —
bounded, ephemeral, reclaimed at rm/land.

## The three layers

| Layer                         | Contents                                                                                 | Location                                                                                                                       | Sharing                                   |
| ----------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------- |
| 1. Mirror artifacts           | npm tarballs, crate files, registry metadata, bare git mirrors                           | `~/.cowshed/caches/{mirror,git}` (the `cowshed.caches` volume, 01_storage.md), content-addressed                               | Global, written only by cowshed-gateway   |
| 2. Clone-materializing caches | bun install cache — **today: bun, on APFS; possibly nothing at all on ZFS**              | Inside each workspace image under `.cowshed/cache/bun`                                                                         | Inherited from main via CoW at clone time |
| 3. Read-at-build caches       | cargo registry (`~/.cargo`), Go module + build caches, sccache, zig global cache, gradle | Shared cache volume, reached through the tools' **default paths** (relocated once) or **direct path config** (Go — see Wiring) | Shared writable by all workspaces         |

Layer 1 removes duplicate _downloads_ (and stores compressed bytes once, ever); its git form is the bare-mirror tree
(`~/.cowshed/caches/git/<host>/<path>.git`), written only by the gateway's `repo mirror` verb (05_gateway.md) and
locally cloned/fetched from by workspaces and main — workspace git never speaks to a network remote. Layer 2 is one
special case, not a category: it exists exactly where a tool reflinks out of its cache and the substrate cannot reflink
across volume boundaries. Layer 3 caches are read at build time and write nowhere near the workspace, so sharing them is
free.

**ZFS may empty layer 2** (verify item, not a promise): OpenZFS 2.2 block cloning (BRT) works across datasets within a
pool — something APFS clonefile cannot do across volumes. If bun's Linux copy path goes through `copy_file_range` (which
BRT intercepts), a pool-shared bun cache gets reflink speed anyway, and on ZFS every cache is shared with zero
duplication and no speed penalty. Bun's Linux _default_ backend is hardlink, which cannot cross datasets either; the
open question is specifically what its copyfile backend calls. Until verified, the ZFS substrate keeps the bun cache
in-dataset like APFS does.

**Workspace-keyed state** — `target/`, materialized `node_modules`, `DerivedData`, `.nx`, `.zig-cache`, Metro/Expo
caches — is not shareable concurrently under any mechanism. It stays in-image, warm because the image was cloned from
main.

## The reflink-reachability rule

A clone-materializing cache must live where the substrate can reflink from it into the workspace — on APFS, the same
volume; on ZFS, the same pool (pending the BRT verification). Any placement that breaks reachability silently downgrades
materialization from clonefile to copyfile. This replaces the earlier, broader "no cache dependency outside the volume"
rule: read-at-build caches may live off-image freely, because nothing ever reflinks or hardlinks out of them.

## Wiring

Wiring is carried by **files and host paths, not environment variables**. Files travel with the image through
clone/fork/checkpoint and cover processes cowshed never spawned (IDE terminals, launchd jobs, CI runner steps);
environment dies at the first unwrapped spawn.

- **In-image config files**, written at adopt/new/fork (per-workspace values: the gateway data port) and re-validated by
  `ensure`. cowshed lists the paths it owns in the workspace repo's `.git/info/exclude` — repo-local ignore that travels
  with every clone — so per-workspace rewrites never dirty `git status`:
  - `bunfig.toml` at the workspace root: `install.registry` pointing at the workspace's own gateway data-plane port
    (`http://127.0.0.1:<base>/npm` — the workspace's `portBlock.base`, 05_gateway.md; a file, still no variable) and
    `[install.cache] dir = ".cowshed/cache/bun"` — **verified (bun 1.3.14): the relative path is honored and resolves
    against the project root, not the invocation cwd** — one committed line, identical for main, workspaces, and CI.
    Caution, same verification: the `[install] cacheDir` spelling is _silently ignored_ (falls back to the global cache
    with no error); `cowshed doctor` checks for that misspelling.
  - cargo config: source replacement of crates.io with `sparse+http://127.0.0.1:<base>/cargo/`,
    `[build] rustc-wrapper = "sccache"` when sccache is present, and `[env]` setting `SCCACHE_NO_DAEMON = "1"` —
    **verified (cargo 1.97): `[env]` values reach every rustc-wrapper invocation; no environment fallback is needed.**
    Host-global settings live in the host-owned `~/.cargo/config.toml` (never on the cache volume — see relocation
    below); per-workspace ones live in the in-image `.cargo/config.toml`. The port is the primary workspace identity, so
    the URL no longer needs to carry the token; the token second factor rides the registry auth mechanism
    (05_gateway.md).
  - No git remote/proxy config is written: workspace git speaks only local filesystem remotes (the `host` remote and
    gateway-owned bare mirrors — 05_gateway.md), so there is nothing to route through the gateway and no credential
    helper inside the image.
  - **Go env file** at `.cowshed/cache/go/env` (the `go env -w` format), reached via a `GOENV` export (below). Go is the
    one toolchain with **no project-level config file** — settings live in a single user-global env file
    (`os.UserConfigDir()/go/env`, measured default `~/Library/Application Support/go/env`) overridable only by `GOENV` —
    and `GOPROXY` must carry the workspace's own data-plane port, so a host-global file cannot express it. The in-image
    file pins: `GOPROXY=http://127.0.0.1:<base>/go` (no `,direct` fallback — misses fail at the gateway with the
    offline/denied distinction, 05_gateway.md), `GOSUMDB=sum.golang.org` (verification rides the proxy's sumdb
    passthrough), `GOMODCACHE=~/.cowshed/caches/go/mod` and `GOCACHE=~/.cowshed/caches/go/build` (shared, layer 3),
    `GOPATH=<mount>/.cowshed/cache/go/path` and `GOBIN=<mount>/.cowshed/cache/go/bin` (in-image, workspace-keyed —
    `go install` binaries are the `~/.cargo/bin` persistence-escape hazard and must never land on the shared volume).
    Net effect: **`~/go` is never created** (measured on this host: the devenv-provided go 1.26.3 had already grown a
    1.1 GB `~/go/pkg/mod` under the defaults); 04_sandbox.md turns any regression into a loud tripwire. cowshed also
    writes **`GOTOOLCHAIN=local`**: the toolchain is nix/devenv-provided and pinned, and `auto` silently downloading Go
    toolchains contradicts the declarative environment — a project that deliberately overrides to `auto` gets its
    downloads in `GOMODCACHE`, i.e. on the caches volume, never in `$HOME`. A host-global `go env -w` file instead of
    `GOENV` is rejected: `GOPROXY` is per-workspace identity, and a global file would leak one workspace's port to all.
- **In-image tool shims** at `.cowshed/bin/`, PATH-prepended by the same `.envrc` wiring (so they travel with every
  clone and cover every process spawned in the workspace, IDE terminals included). Today that is one shim: the **`xcrun`
  wrapper** — pure `exec /usr/bin/xcrun "$@"` passthrough for everything except the simulator-control verbs (`simctl`,
  `devicectl`), so toolchain calls (`xcrun clang`, `--show-sdk-path`) stay native-speed and unbreakable. Simulator verbs
  resolve their device target: **dev-local CoreSimulator is the default** (agents and automation never accidentally
  reach the personal session); personal-session devices appear as explicitly-named remote targets and route through the
  gateway's `/sim/` endpoint (05_gateway.md) under the `sim` grant axis (04_sandbox.md). Tools that hardcode
  `/usr/bin/xcrun` bypass the shim and degrade to dev-local simulators — the safe default.
- **Host-level relocation, once — cache subtrees only**: at first adopt on a host (idempotent, re-checked by `doctor`),
  the read-at-build tools' _cache_ directories become symlinks onto the cache volume: `~/.cargo/registry`,
  `~/.cargo/git`, `~/.cache/zig`, `~/.gradle/caches` (and the other gradle cache subdirs), sccache's platform default.
  **The parent config directories stay on the host.** `~/.cargo/config.toml`, `~/.cargo/credentials.toml`,
  `~/.cargo/bin` (on PATH), and `~/.gradle/gradle.properties` are _not_ relocated and are on the secret deny list
  (04_sandbox.md) — relocating them wholesale would put user config, credentials, and PATH-resolved binaries on a
  sandbox-writable volume, a persistence-escape surface. No `CARGO_HOME`, `ZIG_GLOBAL_CACHE_DIR`, `GRADLE_USER_HOME`, or
  `SCCACHE_DIR` exports exist. Go needs no symlink at all — `GOMODCACHE`/`GOCACHE` are directly configurable in its env
  file (above), which is strictly cleaner than relocating a default path. Profile generation canonicalizes symlinked
  paths when emitting write grants (the `/var` → `/private/var` handling generalizes). The relocation runs in one of two
  modes (14_nix.md): **declarative** — `programs.cowshed` (home-manager) owns the symlinks as generation-managed
  artifacts, and `adopt`/`ensure` VALIDATE without mutating (detection: the symlinks resolve into `/nix/store`; `doctor`
  findings point at the declarative fix, e.g. `next: enable programs.cowshed.relocations`); **imperative** — the non-nix
  fallback, where `adopt` writes the symlinks itself, exactly as above. Same end state either way; the difference is who
  owns it.
- **Environment variables: at most two load-bearing.**
  - `BUN_INSTALL_CACHE_DIR` is **retired** — its verification passed (relative `[install.cache] dir` works, above); the
    committed bunfig line is the wiring and no export exists.
  - `COWSHED_GATEWAY_TOKEN` is one candidate: it dies if bun accepts the token via the registry auth mechanism against
    the in-image bunfig (the port already identifies the workspace — 05_gateway.md). If not, this is a load-bearing
    export. (There is no git credential helper to consider — git is local-only.)
  - `GOENV=<mount>/.cowshed/cache/go/env` is the other: Go has no directory-scoped config, so the in-image env file is
    reachable only through this export. It rides the in-image `.envrc`/direnv like the rest of the wiring —
    `cowshed exec`'s fail-closed `direnv export` (04_sandbox.md) carries it, and IDE-spawned tools (gopls) get it via
    the editor's direnv integration. Verification item (kickoff): coverage across go invocations including gopls, and
    whether any file-based mechanism exists that kills the export.
  - `cowshed ensure --envrc` additionally emits **port conventions for dev servers** —
    `COWSHED_PORT_BASE=<portBlock.base>` and `PORT=<base+1>` — so devenv/dev servers bind inside the workspace's own
    block (04_sandbox.md, cooperative-sandboxing caveat); and **optional prompt conveniences — explicitly
    non-load-bearing** — `COWSHED_WORKSPACE` / `COWSHED_PROJECT` / `COWSHED_LAYER` / `COWSHED_MOUNT`. Anything that
    needs identity derives it from cwd via `.cowshed/workspace.json` or asks the CLI.

`SCCACHE_NO_DAEMON=1` remains mandatory wiring wherever it is carried: a shared sccache server process would inherit the
sandbox of whichever workspace spawned it and enforce the wrong boundary for every other client. In-process mode trades
a small per-invocation cost for a per-exec-correct sandbox.

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

1. A workspace's `bun install` (or cargo, or `go mod download`) asks the gateway mirror — on the workspace's own
   data-plane port (05_gateway.md) — for a package it lacks. Registry endpoints are **baseline broker policy**: a closed
   workspace installs with zero grants; the port+token still identify and audit every request.
2. The gateway fetches it upstream once (credentials injected), stores it content-addressed on the caches volume, and
   serves it — over loopback, so no WAN duplication across workspaces.
3. bun extracts into the workspace's in-image cache and materializes into `node_modules` via clonefile — new inodes land
   inside the image, never on the host volume. cargo extracts into the shared registry, and go extracts into the shared
   `GOMODCACHE` (0444 entries, internally locked, built for exactly this cross-consumer sharing), where every workspace
   and main read them thereafter.
4. Extracted bun-cache bytes duplicate only across sessions that independently adopt the same new package, and only
   until those branches land and later workspaces clone a main that includes them — bounded, ephemeral duplication.

## Known limitations

- **Path-keyed warm state.** Cargo incremental fingerprints and DerivedData embed absolute paths. Main (fixed
  mountpoint) keeps its warm state forever; sessions mount at per-name paths and take a partial cold hit on first build.
  Mitigations: shared sccache (layer 3) absorbs most rustc recompilation, slot mounts (`new --slot`) recycle stable
  paths, and projects may opt into `trim-paths`/`--remap-path-prefix`. cowshed does not force compiler flags.
- **Poisoning model.** Lockfile integrity hashes protect _downloads_ (layer 1 is verified by the package managers
  themselves), not cache _reuse_: layers 2–3 are trusted once written. State this plainly: the layer-3 write scope a
  sandbox holds includes cargo's `registry/src`, the Go caches, and the sccache store — caches that _main itself
  compiles from_, so a poisoned entry can influence main's next build. That is an accepted risk under the confinement
  threat model (semi-trusted agents running the user's own code), bounded by write scope — a sandboxed workspace can
  write only the designated layer-3 subtrees, never the mirror (layer 1, gateway-only) and never the relocated
  cargo/gradle _config_ (host-side, deny-listed — see relocation above). Go's posture within that scope is notably
  stronger than cargo's: module downloads verify against `go.sum` plus the checksum database, extraction is
  ziphash-verified, and `GOMODCACHE` entries land read-only (0444) — tampering requires an explicit chmod, which the
  escape suite exercises (04_sandbox.md); `GOCACHE` is the sccache-analog and shares its trust level.
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
never reflinks from its registry into `target/` — sharing it costs nothing. Bun's cache is a reflink source — its
placement is a speed decision. The spec says so plainly to keep the next redesign from rediscovering it.

**Bun cache on the shared volume (on APFS) rejected.** It would deduplicate extracted bytes globally but moves the
reflink source across a volume boundary, downgrading every full materialization from seconds (clonefile) to tens of
seconds (copyfile) — pessimizing the hot path to optimize a bounded, ephemeral duplication the mirror has already made
cheap. On ZFS this tradeoff may not exist at all (BRT crosses datasets); that is the verification's point.

**Environment-variable wiring (the original twelve exports) rejected.** Identity vars duplicated the marker file; the
gateway URL duplicated the config files that actually consume it; four cache paths duplicated what a one-time relocation
of the tools' default directories does more robustly; `SCCACHE_NO_DAEMON` belongs to cargo's `[env]` (verified to reach
wrapper invocations). Environment survives only processes cowshed spawns; files and host paths survive everything. What
remains is at most two exports (the gateway token, pending its own verification, and `GOENV` — Go's lack of any
directory-scoped config makes it the one toolchain where a file cannot carry per-workspace wiring) — the bun cache
export already died to its verification.

**Cross-session cache harvest rejected.** Actively copying new cache entries between live sessions adds a mutable side
channel between sandboxes and machinery (staging, folding, scheduling) whose entire benefit is bytes that
land-then-clone convergence already delivers a few hours later.
